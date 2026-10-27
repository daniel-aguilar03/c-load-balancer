# StreamGate

An event-driven TCP load balancer in C, built to explore Linux networking,
bounded resource use, availability, and performance through reproducible experiments.

The proxy uses level-triggered `epoll` on Linux and `poll` on macOS. It forwards
arbitrary TCP byte streams; the demo protocol is separate from the proxy.

```sh
make
./build/streamgate --backend 127.0.0.1:9001 --backend 127.0.0.1:9002
curl http://127.0.0.1:9090/metrics
```

Run `./build/streamgate --help` for configuration options. Backends must be running
and pass two consecutive TCP health checks before receiving traffic.

Features: round-robin or least-connections selection, nonblocking connection setup,
16 KiB buffers per direction, backpressure, TCP half-closes, idle/connect timeouts,
connection admission limits, asynchronous health probes, JSON metrics, and bounded
shutdown draining on SIGINT/SIGTERM.

This is a portfolio and learning project. Backend failover applies to new connections;
existing sessions are not migrated. The proxy itself is a single point of failure.
