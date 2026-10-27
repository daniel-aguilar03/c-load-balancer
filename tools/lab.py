#!/usr/bin/env python3
"""Launch a self-contained local lab, failure demonstration, or benchmark."""
import argparse
import asyncio
import contextlib
import datetime
import html
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from traffic import benchmark, exchange

ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(os.environ.get('STREAMGATE_BIN', str(ROOT / 'build/streamgate')))


def unused_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def wait_until(predicate, timeout=8, description='condition'):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, urllib.error.URLError):
            pass
        time.sleep(.04)
    raise TimeoutError(f'timed out waiting for {description}')


class Lab:
    def __init__(self, directory=None, backend_count=3, mode='framed', proxy_args=None,
                 proxy_port=None, admin_port=None, backend_args=None):
        self.directory = Path(directory) if directory else Path(tempfile.mkdtemp(prefix='streamgate-'))
        self.directory.mkdir(parents=True, exist_ok=True)
        ports = unused_ports(backend_count + 2)
        self.proxy_port = proxy_port or ports[0]
        self.admin_port = admin_port or ports[1]
        self.backend_ports = ports[2:]
        self.backends = [None] * backend_count
        self.logs = []
        self.proxy = None
        self.mode = mode
        self.proxy_args = proxy_args or []
        self.backend_args = backend_args or []
        self.final_metrics = None

    def start_backend(self, index, extra=None):
        assert self.backends[index] is None or self.backends[index].poll() is not None
        log = (self.directory / f'backend-{index + 1}.log').open('a')
        self.logs.append(log)
        args = [sys.executable, str(ROOT / 'tools/backend.py'), '--port', str(self.backend_ports[index]),
                '--name', f'backend-{index + 1}', '--mode', self.mode, *self.backend_args, *(extra or [])]
        self.backends[index] = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        def listening():
            if self.backends[index].poll() is not None:
                raise RuntimeError(f'backend exited: {log.name}')
            with socket.create_connection(('127.0.0.1', self.backend_ports[index]), timeout=.1):
                return True
        wait_until(listening, description=f'backend-{index + 1} to listen')

    def stop_backend(self, index):
        process = self.backends[index]
        if process and process.poll() is None:
            process.kill()  # Crash injection deliberately interrupts existing sessions.
            process.wait(timeout=3)

    def start(self):
        try:
            for i in range(len(self.backends)):
                self.start_backend(i)
            log = (self.directory / 'proxy.log').open('w')
            self.logs.append(log)
            command = [str(BINARY), '--listen', f'127.0.0.1:{self.proxy_port}',
                       '--admin', f'127.0.0.1:{self.admin_port}',
                       '--health-interval-ms', '100', '--health-timeout-ms', '100']
            for port in self.backend_ports:
                command.extend(['--backend', f'127.0.0.1:{port}'])
            self.proxy = subprocess.Popen([*command, *self.proxy_args], stdout=log, stderr=subprocess.STDOUT)
            def ready():
                if self.proxy.poll() is not None:
                    raise RuntimeError(f'proxy exited: {(self.directory / "proxy.log").read_text()}')
                return self.snapshot() if all(b['healthy'] for b in self.snapshot()['backends']) else False
            wait_until(ready, description='all backends to pass health checks')
            return self
        except BaseException:
            self.close()
            raise

    def snapshot(self):
        with urllib.request.urlopen(f'http://127.0.0.1:{self.admin_port}/metrics', timeout=2) as response:
            return json.load(response)

    def wait_health(self, index, healthy):
        return wait_until(lambda: self.snapshot()['backends'][index]['healthy'] == healthy,
                          description=f'backend-{index + 1} healthy={healthy}')

    def close(self):
        if self.proxy and self.proxy.poll() is None:
            self.proxy.send_signal(signal.SIGTERM)
            try:
                self.proxy.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.proxy.kill()
                self.proxy.wait(timeout=3)
        for i in range(len(self.backends)):
            self.stop_backend(i)
        for log in self.logs:
            log.close()
        self.logs.clear()
        proxy_log = self.directory / 'proxy.log'
        if proxy_log.exists():
            for line in proxy_log.read_text().splitlines():
                with contextlib.suppress(ValueError):
                    entry = json.loads(line)
                    if 'accepted' in entry:
                        self.final_metrics = entry

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()


def write_report(directory, result):
    directory = Path(directory)
    (directory / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
    stages = result.get('stages', [dict(name='benchmark', result=result['benchmark'])] if 'benchmark' in result else [])
    rows = []
    for stage in stages:
        r = stage['result']
        latency = r['latency_ms']
        rows.append('<tr>' + ''.join(f'<td>{html.escape(str(v))}</td>' for v in [
            stage['name'], r['clients'], r['successful_messages'], r['failed_operations'],
            r['messages_per_second'], latency['p50'], latency['p95'], latency['p99']]) + '</tr>')
    backend_rows = []
    for stage in stages:
        for name, latency in stage['result']['latency_by_backend_ms'].items():
            backend_rows.append('<tr>' + ''.join(f'<td>{html.escape(str(v))}</td>' for v in [
                stage['name'], name, latency['p50'], latency['p95'], latency['p99']]) + '</tr>')
    events = ''.join(f'<li>{html.escape(str(e))}</li>' for e in result.get('events', []))
    engine = result.get('metrics', {}).get('engine', 'unknown')
    page = '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>StreamGate experiment report</title><style>
:root{color-scheme:dark}body{font:16px/1.6 system-ui,sans-serif;background:#101820;color:#e9f1f8;max-width:1100px;margin:48px auto;padding:0 24px}
h1{font-size:36px;line-height:1.2}p{color:#b6c9d6}table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:12px;text-align:left;border-bottom:1px solid #344453}
th{color:#74e2c4}pre{white-space:pre-wrap;background:#172631;padding:20px;border-radius:12px;overflow-wrap:anywhere}.scroll{overflow:auto}a{color:#74e2c4}
</style><h1>StreamGate / experiment report</h1>'''
    page += f'<p>Generated {html.escape(result["generated_at"])} · event engine: {html.escape(engine)}</p>'
    page += '<p>Real TCP traffic through the C proxy. Clients verify every binary response. Latencies are application round trips on persistent connections.</p>'
    page += '<div class="scroll"><table><thead><tr>' + ''.join(f'<th>{h}</th>' for h in ['Scenario', 'Clients', 'Messages', 'Errors', 'Msg/s', 'p50 ms', 'p95 ms', 'p99 ms']) + '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>'
    page += '<h2>Latency by backend</h2><p>Global percentiles can hide a slow backend in a closed-loop workload because slow clients complete fewer messages.</p><div class="scroll"><table><thead><tr><th>Scenario</th><th>Backend</th><th>p50 ms</th><th>p95 ms</th><th>p99 ms</th></tr></thead><tbody>' + ''.join(backend_rows) + '</tbody></table></div>'
    page += '<h2>Experiment events</h2><ul>' + (events or '<li>No failure injection in this run.</li>') + '</ul>'
    page += '<h2>Measurement limits</h2><p>Closed-loop clients run on the same machine as the proxy and backends. Results include Python, scheduling, and local networking costs. These are reproducible observations, not isolated proxy capacity or production guarantees. TCP probes establish connectivity only. Existing sessions are not migrated after backend failure.</p>'
    page += '<h2>Full results</h2><pre>' + html.escape(json.dumps(result, indent=2)) + '</pre></html>'
    (directory / 'report.html').write_text(page)


async def failure_demo(lab, clients, duration):
    stages, events = [], []

    async def stage(name):
        result = await benchmark(port=lab.proxy_port, clients=clients, duration=duration)
        if result['failed_operations'] or not result['successful_messages']:
            raise AssertionError(f'{name}: unexpected traffic failures: {result}')
        stages.append({'name': name, 'result': result, 'metrics': lab.snapshot()})
        print(f"{name:24} {result['successful_messages']:7} verified messages | "
              f"{result['messages_per_second']:8.1f} msg/s | p99 {result['latency_ms']['p99']} ms", flush=True)
        return result

    await stage('all backends healthy')
    reader = writer = None
    # Keep a known session on backend-2 open while injecting its crash.
    for _ in range(10):
        reader, writer = await asyncio.open_connection('127.0.0.1', lab.proxy_port)
        identity = await asyncio.wait_for(exchange(reader, writer, b'session-before-crash'), 2)
        if identity == 'backend-2':
            break
        writer.close()
        await writer.wait_closed()
    else:
        raise AssertionError('could not select backend-2 for interrupted-session demo')
    try:
        before = time.perf_counter()
        lab.stop_backend(1)
        lab.wait_health(1, False)
        detection = round((time.perf_counter() - before) * 1000, 2)
        interrupted = False
        try:
            await asyncio.wait_for(exchange(reader, writer, b'session-after-crash'), 2)
        except (ConnectionError, OSError, asyncio.IncompleteReadError, asyncio.TimeoutError):
            interrupted = True
        if not interrupted:
            raise AssertionError('existing session unexpectedly survived backend process death')
        events.append(f'Killed backend-2; TCP health detection observed after {detection} ms. Existing session interrupted as expected.')
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()
    survivors = await stage('backend-2 crashed')
    assert 'backend-2' not in survivors['messages_by_backend']
    reader, writer = await asyncio.open_connection('127.0.0.1', lab.proxy_port)
    try:
        target = await asyncio.wait_for(exchange(reader, writer, b'retry-after-reconnect'), 2)
        assert target != 'backend-2'
        events.append(f'Application reconnect succeeded on {target}; proxy does not replay requests.')
    finally:
        writer.close()
        await writer.wait_closed()
    lab.start_backend(1)
    lab.wait_health(1, True)
    recovered = await stage('backend-2 recovered')
    assert 'backend-2' in recovered['messages_by_backend']
    events.append('Restarted backend-2; health checks returned it to rotation.')
    lab.stop_backend(2)
    lab.wait_health(2, False)
    lab.start_backend(2, ['--delay-ms', '20'])
    lab.wait_health(2, True)
    await stage('backend-3 delayed 20ms')
    events.append('A backend with 20 ms application delay remains TCP-healthy; latency metrics expose the degradation.')
    return stages, events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['serve', 'demo', 'benchmark'])
    parser.add_argument('--clients', type=int, default=60)
    parser.add_argument('--duration', type=float, default=2)
    parser.add_argument('--payload-size', type=int, default=256)
    parser.add_argument('--algorithm', choices=['roundrobin', 'leastconn'], default='roundrobin')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--port', type=int, default=8080, help='serve mode only')
    parser.add_argument('--admin-port', type=int, default=9090, help='serve mode only')
    args = parser.parse_args()
    if not 3 <= args.clients <= 10000 or not 0 < args.duration <= 300 or not 16 <= args.payload_size <= 1048576:
        parser.error('clients must be 3..10000, duration >0 and <=300, payload size 16..1048576')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    directory = args.output or ROOT / 'reports' / f'{args.command}-{stamp}'
    if not BINARY.exists():
        parser.error('build the proxy first: make')
    result = {'generated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'algorithm': args.algorithm}
    lab = Lab(directory, proxy_args=['--algorithm', args.algorithm],
              proxy_port=args.port if args.command == 'serve' else None,
              admin_port=args.admin_port if args.command == 'serve' else None)
    try:
        with lab:
            print(f'Proxy 127.0.0.1:{lab.proxy_port} | metrics http://127.0.0.1:{lab.admin_port}/metrics', flush=True)
            for i, process in enumerate(lab.backends):
                print(f'backend-{i + 1}: 127.0.0.1:{lab.backend_ports[i]} PID={process.pid}', flush=True)
            if args.command == 'serve':
                print('Ctrl+C drains connections and stops all lab processes.', flush=True)
                while True:
                    time.sleep(.5)
                    if lab.proxy.poll() is not None:
                        raise RuntimeError('proxy exited unexpectedly')
            elif args.command == 'demo':
                result['stages'], result['events'] = asyncio.run(failure_demo(lab, args.clients, args.duration))
            else:
                result['benchmark'] = asyncio.run(benchmark(port=lab.proxy_port, clients=args.clients,
                                                           duration=args.duration, payload_size=args.payload_size))
                print(json.dumps(result['benchmark'], indent=2))
                if result['benchmark']['failed_operations'] or not result['benchmark']['successful_messages']:
                    raise AssertionError('benchmark traffic failed')
            result['metrics'] = lab.snapshot()
    except KeyboardInterrupt:
        print('\nLab stopped.', flush=True)
        return
    result['final_metrics'] = lab.final_metrics
    if lab.proxy.returncode != 0:
        raise RuntimeError(f'proxy exited with {lab.proxy.returncode}; inspect {directory / "proxy.log"}')
    write_report(directory, result)
    print(f'Report: {directory / "report.html"}', flush=True)


if __name__ == '__main__':
    main()
