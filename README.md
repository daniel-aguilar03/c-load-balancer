# StreamGate

**A TCP load balancer in C, with a reproducible traffic and failure laboratory.**

StreamGate accepts real TCP connections and forwards arbitrary byte streams to
healthy backends. The accompanying Python simulator makes the proxy's correctness,
resource use, and failure behavior observable. It is a systems engineering portfolio
project, not a production availability guarantee.

```text
                                     ┌─ backend-1 ─┐
Python clients ── TCP ── C proxy ──────┼─ backend-2 ─┼── binary echo responses
                         │           └─ backend-3 ─┘
                         ├─ asynchronous TCP health probes
                         └─ local HTTP JSON metrics
                                  │
                           experiment report
```

## Run the complete demonstration

Requirements: Linux or macOS, a C11 compiler, Make, and Python 3.10+. Python tools
use only the standard library. Windows users can use Docker or WSL2.

```sh
make
make demo
```

The demo starts a proxy and three backends on available loopback ports, generates
verified binary traffic, crashes a backend, proves an existing session is interrupted,
reconnects through a healthy backend, restores the failed backend, and introduces a
20 ms response delay. Processes are stopped automatically when it finishes.

Open the printed `reports/demo-*/report.html` path in a browser. The report is a
self-contained local file with scenario summaries, global and per-backend latency,
connection distribution, resource metrics, failure observations, and the raw results.
`results.json` and process logs are saved beside it. Each traffic phase must deliver
messages without errors; the explicitly demonstrated interrupted session is expected.

```sh
# Keep the lab running for manual exploration (Ctrl+C stops it)
python3 tools/lab.py serve

# In another terminal
curl http://127.0.0.1:9090/metrics
python3 tools/traffic.py --clients 100 --duration 5
python3 tools/monitor.py
```

The interactive lab binds the proxy to `127.0.0.1:8080` and metrics to
`127.0.0.1:9090`. It prints backend ports and PIDs for manual failure experiments.
Use `--port` and `--admin-port` if those ports are occupied.

## Linux containers

```sh
docker compose up --build -d
docker compose run --rm --no-deps traffic
curl http://127.0.0.1:9090/metrics

# Observe health changes and run fresh traffic while one backend is down
docker compose stop backend-b
sleep 2
docker compose run --rm --no-deps traffic
docker compose start backend-b

# Stop and remove the lab
docker compose down
```

The proxy runs as an unprivileged user. Published ports bind to host loopback. The
container runtime uses `epoll`; the native macOS implementation uses `poll`.
Docker service names resolve at startup, so recreate the proxy if a backend's
container IP changes. TCP health checks do not re-resolve DNS.

To run the Linux integration tests and sanitizers in an isolated build:

```sh
docker build --target test -t streamgate:test .
docker run --rm streamgate:test
```

## Features

- Level-triggered Linux `epoll` with nonblocking connect/read/write; portable `poll` fallback.
- Round-robin and least-active-connections policies, applied once per accepted connection.
- Two fixed 16 KiB buffers per connection, partial writes, backpressure, and TCP half-closes.
- Asynchronous TCP health checks with consecutive-result thresholds and automatic recovery.
- Connect/idle deadlines, admission limits, bounded admin connections, and SIGPIPE protection.
- SIGINT/SIGTERM stop new admissions and drain active sessions up to a configured deadline.
- JSON event logs, readiness/liveness endpoints, byte/error counters, peak RSS and CPU time.
- Persistent binary traffic clients, reconnect behavior, and integrity verification.
- Automated integration tests, AddressSanitizer/UBSan runs, and Linux/macOS CI.

## Use it with your own TCP service

```sh
./build/streamgate \
  --listen 127.0.0.1:8080 \
  --admin 127.0.0.1:9090 \
  --backend 127.0.0.1:9001 \
  --backend 127.0.0.1:9002 \
  --algorithm leastconn \
  --max-connections 2048
```

The proxy is protocol agnostic. The demo backends use length-prefixed binary
messages, but HTTP, TLS pass-through, or another TCP application can be forwarded
without changing the proxy. Backends must offer compatible application behavior.
IPv6 endpoints use brackets, for example `[::1]:9001`. See `--help` for all options.

| Option | Default | Meaning |
|---|---:|---|
| `--algorithm` | `roundrobin` | `roundrobin` or `leastconn` |
| `--max-connections` | 2048 | Concurrent client sessions; maximum 16384 |
| `--connect-timeout-ms` | 1000 | Backend connection deadline |
| `--idle-timeout-ms` | 30000 | Maximum time without successful data I/O |
| `--health-interval-ms` | 500 | Delay between completed probes |
| `--health-timeout-ms` | 300 | TCP probe connection deadline |
| `--health-threshold` | 2 | Consecutive successes/failures to change probe health |
| `--drain-timeout-ms` | 5000 | Maximum graceful shutdown wait |

A failed application connection immediately removes that backend from selection;
health probes subsequently control recovery. Backends start unhealthy. Deadlines are
checked at each loop iteration with an idle wait of at most 50 ms; load and scheduling
can increase detection time. The lab overrides probe interval/timeout to 100 ms.

## Observe and measure

| Local endpoint | Behavior |
|---|---|
| `GET /health` | 200 while the event loop is serving admin requests |
| `GET /ready` | 200 when at least one backend is healthy and the proxy is accepting; otherwise 503 |
| `GET /metrics` | JSON snapshot of connections, I/O, resources, and backend state |

The admin API is unauthenticated and intended for a trusted local/private network.
A maximum of 16 admin clients and a 2-second request/response deadline bound its use.

```sh
make test
make sanitize  # Linux: ASan + UBSan; macOS: UBSan
make benchmark
python3 tools/lab.py benchmark --clients 200 --duration 10 --algorithm leastconn
python3 tools/lab.py demo --clients 90 --duration 3 --output reports/my-demo
```

Benchmark reports include payload size, client count, duration, errors, per-backend
request and connection distribution, and p50/p95/p99 latency. The workload is
closed-loop with one message in flight per persistent connection. A default 200,000
request cap bounds sample storage; reaching it ends the workload early. All successful
responses are checked byte-for-byte. See [measurement guidance](docs/performance.md)
before interpreting throughput or comparing machines.

## Project map

| Path | Purpose |
|---|---|
| `src/main.c` | Proxy state machine, probes, timers, balancing, admin API |
| `src/event.c` | epoll/poll adapter and stale-event protection |
| `tools/backend.py` | Framed echo, raw echo, and sink backends |
| `tools/traffic.py` | Traffic generator and latency measurements |
| `tools/lab.py` | Process lifecycle, failure injection, report generation |
| `tools/monitor.py` | Live terminal metrics |
| `tests/test_integration.py` | Real-socket black-box integration tests |
| `docs/architecture.md` | State transitions, memory bounds, design tradeoffs |
| `docs/debugging.md` | GDB/core dump, sanitizer, and failure investigation workflows |
| `docs/performance.md` | Repeatable benchmarking and profiling |

## Deliberate limits

Existing TCP sessions are **not migrated** when a backend fails. Clients must decide
whether to reconnect or retry; application retries require appropriate idempotency.
TCP probes prove connectivity, not application correctness. A slow service can remain
healthy. Round-robin balances connections, not messages; leastconn also cannot infer
backend CPU load. There is no TLS termination, HTTP parsing, HTTP request retry, QUIC,
persistence, hot reload, multi-proxy failover, or hard real-time guarantee. One proxy
process remains a single point of failure.

The implementation uses a single event-loop thread and scans configured connection
slots for deadlines. Larger capacities may benefit from timer queues and multiple
workers; make those changes only after profiling. Fixed buffers bound application
buffer storage to 32 KiB per active session, excluding kernel socket buffers and process
metadata. Kernel and Python simulator memory are separate from the reported proxy
buffer capacity. Peak RSS is a high-water mark, not current resident memory.

The repository intentionally avoids unmeasured throughput or production-readiness
claims. Generated runs are ignored by Git; a documented example run is kept under
`docs/example-run/`.

## References

- [Linux epoll semantics](https://man7.org/linux/man-pages/man7/epoll.7.html)
- [Nonblocking connect and SO_ERROR](https://man7.org/linux/man-pages/man2/connect.2.html)
- [TCP shutdown and half-closes](https://man7.org/linux/man-pages/man2/shutdown.2.html)
- [GDB core-file debugging](https://sourceware.org/gdb/current/onlinedocs/gdb.html/Core-File-Generation.html)

Licensed under MIT; see [LICENSE](LICENSE).
