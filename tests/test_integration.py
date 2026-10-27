"""Black-box tests against the compiled proxy and actual loopback TCP sockets."""
import asyncio
import concurrent.futures
import contextlib
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from lab import BINARY, Lab, wait_until
from traffic import benchmark


def receive_exact(sock, size):
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(min(65536, size - len(result)))
        if not chunk:
            raise AssertionError(f'early EOF at {len(result)}/{size} bytes')
        result.extend(chunk)
    return bytes(result)


def framed(sock, payload=b'test\x00\xff\n'):
    sock.sendall(struct.pack('!I', len(payload)) + payload)
    length, = struct.unpack('!I', receive_exact(sock, 4))
    response = receive_exact(sock, length)
    name_len = response[0]
    if response[name_len + 1:] != payload:
        raise AssertionError('payload corruption')
    return response[1:1 + name_len].decode()


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='test-', dir=ROOT / 'build')
        self.lab = None

    def start(self, **kwargs):
        self.lab = Lab(self.temp.name, **kwargs).start()
        return self.lab

    def connect(self):
        return socket.create_connection(('127.0.0.1', self.lab.proxy_port), timeout=4)

    def tearDown(self):
        if self.lab:
            self.lab.close()
            self.assertEqual(self.lab.proxy.returncode, 0, (Path(self.temp.name) / 'proxy.log').read_text())
            log = (Path(self.temp.name) / 'proxy.log').read_text()
            self.assertNotIn('ERROR: AddressSanitizer', log)
            self.assertNotIn('runtime error:', log)
        self.temp.cleanup()

    def test_round_robin_concurrency_and_binary_integrity(self):
        lab = self.start()
        result = asyncio.run(benchmark(port=lab.proxy_port, clients=36, duration=.3, payload_size=1024))
        self.assertEqual(result['failed_operations'], 0, result)
        self.assertGreater(result['successful_messages'], 36)
        self.assertEqual(set(result['messages_by_backend']), {'backend-1', 'backend-2', 'backend-3'})
        self.assertEqual(list(result['connections_by_backend'].values()), [12, 12, 12])
        wait_until(lambda: lab.snapshot()['active'] == 0)

    def test_fragmented_frame(self):
        self.start()
        payload = bytes(range(256)) * 8
        request = struct.pack('!I', len(payload)) + payload
        with self.connect() as sock:
            for i in range(0, len(request), 7):
                sock.sendall(request[i:i+7])
            size, = struct.unpack('!I', receive_exact(sock, 4))
            response = receive_exact(sock, size)
            self.assertEqual(response[1 + response[0]:], payload)

    def test_large_binary_stream_and_half_close(self):
        self.start(mode='echo')
        payload = os.urandom(2 * 1024 * 1024)
        with self.connect() as sock, concurrent.futures.ThreadPoolExecutor(1) as pool:
            def send():
                sock.sendall(payload)
                sock.shutdown(socket.SHUT_WR)
            sender = pool.submit(send)
            self.assertEqual(receive_exact(sock, len(payload)), payload)
            self.assertEqual(sock.recv(1), b'')
            sender.result(timeout=4)
        wait_until(lambda: self.lab.snapshot()['active'] == 0)

    def test_slow_receiver_backpressure_is_lossless(self):
        lab = self.start(mode='echo', backend_args=['--chunk-size', '1024'])
        payload = os.urandom(4 * 1024 * 1024)
        with self.connect() as sock, concurrent.futures.ThreadPoolExecutor(1) as pool:
            def send():
                sock.sendall(payload)
                sock.shutdown(socket.SHUT_WR)
            sender = pool.submit(send)
            time.sleep(.15)
            self.assertEqual(receive_exact(sock, len(payload)), payload)
            self.assertEqual(sock.recv(1), b'')
            sender.result(timeout=4)
        metrics = lab.snapshot()
        self.assertGreater(metrics['backpressure_events'], 0)
        self.assertLessEqual(metrics['buffer_capacity_bytes'], metrics['max_connections'] * 32768)

    def test_backend_failure_and_recovery(self):
        lab = self.start()
        lab.stop_backend(1)
        lab.wait_health(1, False)
        for _ in range(12):
            with self.connect() as sock:
                self.assertNotEqual(framed(sock), 'backend-2')
        lab.start_backend(1)
        lab.wait_health(1, True)
        names = set()
        for _ in range(9):
            with self.connect() as sock:
                names.add(framed(sock))
        self.assertIn('backend-2', names)
        state = lab.snapshot()['backends'][1]
        self.assertGreaterEqual(state['up_transitions'], 2)
        self.assertGreaterEqual(state['down_transitions'], 1)

    def test_existing_connection_is_not_migrated(self):
        lab = self.start(backend_count=1)
        with self.connect() as sock:
            self.assertEqual(framed(sock), 'backend-1')
            lab.stop_backend(0)
            lab.wait_health(0, False)
            try:
                self.assertEqual(sock.recv(1), b'')
            except ConnectionResetError:
                pass
        wait_until(lambda: lab.snapshot()['active'] == 0)

    def test_all_backends_down_rejects_and_readiness_fails(self):
        lab = self.start(backend_count=1)
        lab.stop_backend(0)
        lab.wait_health(0, False)
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(f'http://127.0.0.1:{lab.admin_port}/ready', timeout=2)
        self.assertEqual(raised.exception.code, 503)
        raised.exception.close()
        with self.connect() as sock:
            try:
                self.assertEqual(sock.recv(1), b'')
            except ConnectionResetError:
                pass
        self.assertGreaterEqual(lab.snapshot()['rejected'], 1)

    def test_connection_admission_limit_and_cleanup(self):
        lab = self.start(proxy_args=['--max-connections', '2'])
        first, second = self.connect(), self.connect()
        try:
            framed(first)
            framed(second)
            wait_until(lambda: lab.snapshot()['active'] == 2)
            with self.connect() as extra:
                try:
                    self.assertEqual(extra.recv(1), b'')
                except ConnectionResetError:
                    pass
            self.assertGreaterEqual(lab.snapshot()['rejected'], 1)
        finally:
            first.close()
            second.close()
        wait_until(lambda: lab.snapshot()['active'] == 0)
        with self.connect() as sock:
            framed(sock)

    def test_idle_timeout(self):
        lab = self.start(proxy_args=['--idle-timeout-ms', '150'])
        with self.connect() as sock:
            framed(sock)
            self.assertEqual(sock.recv(1), b'')
        self.assertGreaterEqual(lab.snapshot()['idle_timeouts'], 1)

    def test_least_connections_prefers_freed_backend(self):
        lab = self.start(proxy_args=['--algorithm', 'leastconn'])
        sockets = [self.connect() for _ in range(3)]
        try:
            names = [framed(sock) for sock in sockets]
            self.assertEqual(len(set(names)), 3)
            freed = names[1]
            sockets[1].close()
            wait_until(lambda: lab.snapshot()['active'] == 2)
            with self.connect() as sock:
                self.assertEqual(framed(sock), freed)
        finally:
            for sock in sockets:
                sock.close()

    def test_graceful_shutdown_finishes_existing_session(self):
        lab = self.start(mode='echo')
        with self.connect() as sock:
            sock.sendall(b'before')
            self.assertEqual(receive_exact(sock, 6), b'before')
            lab.proxy.send_signal(signal.SIGTERM)
            wait_until(lambda: lab.snapshot()['draining'])
            self.assertIsNone(lab.proxy.poll())
            sock.sendall(b'after\x00\xff')
            sock.shutdown(socket.SHUT_WR)
            self.assertEqual(receive_exact(sock, 7), b'after\x00\xff')
            self.assertEqual(sock.recv(1), b'')
        lab.proxy.wait(timeout=3)
        self.assertEqual(lab.proxy.returncode, 0)

    def test_shutdown_deadline_closes_stalled_sessions(self):
        lab = self.start(proxy_args=['--drain-timeout-ms', '200'])
        with self.connect() as sock:
            framed(sock)
            started = time.monotonic()
            lab.proxy.send_signal(signal.SIGTERM)
            lab.proxy.wait(timeout=3)
            self.assertLess(time.monotonic() - started, 2)
        lab.close()
        self.assertEqual(lab.final_metrics['forced_shutdowns'], 1)

    def test_abrupt_client_resets_do_not_crash_proxy(self):
        lab = self.start()
        for _ in range(60):
            sock = self.connect()
            sock.sendall(struct.pack('!I', 100000) + b'x' * 300)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            sock.close()
        with self.connect() as sock:
            framed(sock)
        wait_until(lambda: lab.snapshot()['active'] == 0)
        self.assertIsNone(lab.proxy.poll())

    def test_rapid_connection_churn_does_not_reuse_stale_events(self):
        lab = self.start(proxy_args=['--max-connections', '24'])
        def worker(_):
            for _ in range(30):
                with self.connect() as sock:
                    framed(sock, os.urandom(300))
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            list(pool.map(worker, range(8)))
        wait_until(lambda: lab.snapshot()['active'] == 0)
        self.assertEqual(lab.snapshot()['rejected'], 0)

    def test_admin_handles_fragmented_requests_and_unknown_path(self):
        lab = self.start()
        with socket.create_connection(('127.0.0.1', lab.admin_port), timeout=3) as sock:
            sock.sendall(b'GET /met')
            time.sleep(.02)
            sock.sendall(b'rics HTTP/1.1\r\nHost: localhost\r\n\r\n')
            response = bytearray()
            while data := sock.recv(65536):
                response.extend(data)
            header, body = bytes(response).split(b'\r\n\r\n', 1)
            self.assertIn(b'200 OK', header)
            self.assertIn(json.loads(body)['engine'], ['poll', 'epoll'])
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(f'http://127.0.0.1:{lab.admin_port}/missing', timeout=2)
        self.assertEqual(raised.exception.code, 404)
        raised.exception.close()

    def test_admin_slow_clients_expire_without_blocking_data(self):
        lab = self.start()
        slow = [socket.create_connection(('127.0.0.1', lab.admin_port), timeout=3) for _ in range(16)]
        try:
            with self.connect() as sock:
                framed(sock)
            time.sleep(2.15)
            for sock in slow:
                self.assertEqual(sock.recv(1), b'')
        finally:
            for sock in slow:
                sock.close()
        self.assertGreater(lab.snapshot()['accepted'], 0)

    def test_cli_rejects_invalid_configuration(self):
        for arguments in [[], ['--wat', '5'], ['--max-connections', '0'],
                          ['--backend', 'bad'], ['--backend', '127.0.0.1:9001', '--algorithm', 'random'],
                          ['--backend', '127.0.0.1:9001', '--max-connections', '20000'],
                          ['--backend', '127.0.0.1:9001', '--health-threshold', '-1']]:
            with self.subTest(arguments=arguments):
                result = subprocess.run([str(BINARY), *arguments], capture_output=True, timeout=3)
                self.assertNotEqual(result.returncode, 0)
        result = subprocess.run([str(BINARY), '--help'], capture_output=True, timeout=3)
        self.assertEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()
