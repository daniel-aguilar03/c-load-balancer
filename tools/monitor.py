#!/usr/bin/env python3
"""Print live proxy metrics without third-party dependencies."""
import argparse
import contextlib
import json
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:9090/metrics')
    parser.add_argument('--interval', type=float, default=1)
    parser.add_argument('--samples', type=int, default=0, help='0 means until Ctrl+C')
    args = parser.parse_args()
    if args.interval <= 0 or args.samples < 0:
        parser.error('interval must be positive and samples nonnegative')
    previous = None
    count = 0
    with contextlib.suppress(KeyboardInterrupt):
        while args.samples == 0 or count < args.samples:
            try:
                with urllib.request.urlopen(args.url, timeout=2) as response:
                    data = json.load(response)
                now = time.monotonic()
                delta = now - previous[0] if previous else 1
                byte_delta = data['client_bytes_read'] - previous[1] if previous else 0
                states = '  '.join(f"{b['address']}={'UP' if b['healthy'] else 'DOWN'}({b['active']})" for b in data['backends'])
                print(f"{data['engine']} active={data['active']}/{data['max_connections']} "
                      f"accepted={data['accepted']} rejected={data['rejected']} "
                      f"ingress={byte_delta / delta / 1048576:.2f}MiB/s "
                      f"peakRSS={data['max_rss_bytes']/1048576:.1f}MiB cpu={data['cpu_seconds']:.2f}s | {states}", flush=True)
                previous = (now, data['client_bytes_read'])
            except (OSError, ValueError, urllib.error.URLError) as exc:
                print(f'metrics unavailable: {exc}', flush=True)
                previous = None
            count += 1
            if args.samples == 0 or count < args.samples:
                time.sleep(args.interval)


if __name__ == '__main__':
    main()
