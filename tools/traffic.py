#!/usr/bin/env python3
"""Concurrent, persistent TCP clients with binary integrity checks and latency data."""
import argparse
import asyncio
import collections
import contextlib
import json
import math
import os
import platform
import struct
import time
import urllib.request
import urllib.error
from pathlib import Path

MAX_FRAME = 1024 * 1024


async def exchange(reader, writer, payload):
    writer.write(struct.pack('!I', len(payload)) + payload)
    await writer.drain()
    header = await reader.readexactly(4)
    size, = struct.unpack('!I', header)
    if not 1 <= size <= MAX_FRAME + 256:
        raise ValueError(f'invalid response size: {size}')
    data = await reader.readexactly(size)
    identity_length = data[0]
    if len(data) < identity_length + 1 or data[identity_length + 1:] != payload:
        raise ValueError('response failed binary integrity check')
    return data[1:identity_length + 1].decode('ascii')


def percentile(sorted_samples, fraction):
    if not sorted_samples:
        return None
    return round(sorted_samples[max(0, math.ceil(len(sorted_samples) * fraction) - 1)], 3)


async def benchmark(host='127.0.0.1', port=8080, clients=100, duration=5,
                    payload_size=256, timeout=3, request_limit=200000):
    """Latency is per application message, excluding initial connection setup.

    Each client has one request in flight. Results describe a closed-loop workload.
    A global request cap bounds memory used to retain every successful latency.
    """
    latencies, errors = [], collections.Counter()
    backend_latencies = collections.defaultdict(list)
    distribution, connection_distribution = collections.Counter(), collections.Counter()
    attempts = 0
    start = time.perf_counter()
    deadline = start + duration

    async def client(index):
        nonlocal attempts
        reader = writer = None
        sequence = 0
        random_body = os.urandom(payload_size - 16)
        try:
            while time.perf_counter() < deadline and attempts < request_limit:
                try:
                    if writer is None:
                        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
                        first_request = True
                    if attempts >= request_limit or time.perf_counter() >= deadline:
                        break
                    attempts += 1
                    sequence += 1
                    payload = struct.pack('!QQ', index, sequence) + random_body
                    before = time.perf_counter()
                    name = await asyncio.wait_for(exchange(reader, writer, payload), timeout)
                    latency = (time.perf_counter() - before) * 1000
                    latencies.append(latency)
                    backend_latencies[name].append(latency)
                    distribution[name] += 1
                    if first_request:
                        connection_distribution[name] += 1
                        first_request = False
                except (ConnectionError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError) as exc:
                    errors[type(exc).__name__] += 1
                    if writer:
                        writer.close()
                        with contextlib.suppress(ConnectionError, OSError):
                            await writer.wait_closed()
                    reader = writer = None
                    await asyncio.sleep(0.02)
        finally:
            if writer:
                writer.close()
                with contextlib.suppress(ConnectionError, OSError):
                    await writer.wait_closed()

    await asyncio.gather(*(client(i) for i in range(clients)))
    elapsed = time.perf_counter() - start
    latencies.sort()
    return {
        'clients': clients, 'requested_duration_s': duration, 'elapsed_s': round(elapsed, 3),
        'payload_bytes': payload_size, 'attempts': attempts, 'successful_messages': len(latencies),
        'failed_operations': sum(errors.values()), 'errors': dict(errors),
        'messages_per_second': round(len(latencies) / elapsed, 1),
        'payload_mib_per_second_bidirectional': round(len(latencies) * payload_size * 2 / elapsed / 1048576, 3),
        'latency_ms': {'p50': percentile(latencies, .50), 'p95': percentile(latencies, .95),
                       'p99': percentile(latencies, .99), 'max': round(latencies[-1], 3) if latencies else None},
        'messages_by_backend': dict(distribution), 'connections_by_backend': dict(connection_distribution),
        'latency_by_backend_ms': {name: {'p50': percentile(sorted(samples), .50),
                                        'p95': percentile(sorted(samples), .95),
                                        'p99': percentile(sorted(samples), .99)}
                                  for name, samples in backend_latencies.items()},
        'request_cap': request_limit, 'cap_reached': attempts >= request_limit,
        'environment': {'system': platform.platform(), 'python': platform.python_version()},
        'workload': 'closed-loop; one outstanding binary echo request per persistent connection; reconnect on failure',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--clients', type=int, default=100)
    parser.add_argument('--duration', type=float, default=5)
    parser.add_argument('--payload-size', type=int, default=256)
    parser.add_argument('--request-limit', type=int, default=200000)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--wait-ready', help='wait up to 15 seconds for an HTTP readiness URL')
    args = parser.parse_args()
    if not 1 <= args.clients <= 10000 or not 0 < args.duration <= 300:
        parser.error('clients must be 1..10000 and duration must be >0 and <=300')
    if not 16 <= args.payload_size <= MAX_FRAME or not 1 <= args.request_limit <= 2000000:
        parser.error('payload size must be 16..1048576 and request limit 1..2000000')
    if args.wait_ready:
        deadline = time.monotonic() + 15
        while True:
            try:
                with urllib.request.urlopen(args.wait_ready, timeout=1) as response:
                    if response.status == 200:
                        break
            except (OSError, urllib.error.URLError):
                pass
            if time.monotonic() >= deadline:
                parser.error('readiness deadline exceeded')
            time.sleep(.1)
    result = asyncio.run(benchmark(args.host, args.port, args.clients, args.duration,
                                  args.payload_size, request_limit=args.request_limit))
    rendered = json.dumps(result, indent=2) + '\n'
    print(rendered, end='')
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    if not result['successful_messages'] or result['failed_operations']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
