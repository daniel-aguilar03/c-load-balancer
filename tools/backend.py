#!/usr/bin/env python3
"""Demo backends. Framed mode identifies the backend and echoes binary payloads."""
import argparse
import asyncio
import contextlib
import struct

MAX_FRAME = 1024 * 1024


async def handle(reader, writer, args):
    try:
        while True:
            if args.read_delay_ms:
                await asyncio.sleep(args.read_delay_ms / 1000)
            if args.mode == 'framed':
                header = await reader.readexactly(4)
                size, = struct.unpack('!I', header)
                if size > MAX_FRAME:
                    break
                payload = await reader.readexactly(size)
                identity = args.name.encode('ascii')
                response = bytes([len(identity)]) + identity + payload
                if args.delay_ms:
                    await asyncio.sleep(args.delay_ms / 1000)
                writer.write(struct.pack('!I', len(response)) + response)
            else:
                payload = await reader.read(args.chunk_size)
                if not payload:
                    break
                if args.mode == 'sink':
                    continue
                if args.delay_ms:
                    await asyncio.sleep(args.delay_ms / 1000)
                writer.write(payload)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass  # Empty health probes and disconnected clients are expected.
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()


async def run(args):
    server = await asyncio.start_server(lambda r, w: handle(r, w, args), args.host, args.port)
    print(f'{args.name}: {args.host}:{args.port} ({args.mode})', flush=True)
    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--name', default='backend-a')
    parser.add_argument('--mode', choices=['framed', 'echo', 'sink'], default='framed')
    parser.add_argument('--delay-ms', type=float, default=0)
    parser.add_argument('--read-delay-ms', type=float, default=0)
    parser.add_argument('--chunk-size', type=int, default=65536)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or not 1 <= len(args.name) <= 63 or not args.name.isascii():
        parser.error('port must be 1..65535 and name must be 1..63 ASCII characters')
    if min(args.delay_ms, args.read_delay_ms) < 0 or not 1 <= args.chunk_size <= MAX_FRAME:
        parser.error('delays must be nonnegative and chunk size must be 1..1048576')
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(args))


if __name__ == '__main__':
    main()
