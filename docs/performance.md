# Reproducible performance experiments

Build with the same compiler/optimization flags for each comparison. Record the
commit SHA, OS/kernel, CPU, number of cores, memory, container/VM limits, and whether
client, proxy, and backends share a host. Record the proxy algorithm, maximum
connections, timeouts, payload size, client count, and test duration.

## A simple scaling experiment

```sh
make
python3 tools/lab.py benchmark --clients 10 --duration 5 --output reports/c10
python3 tools/lab.py benchmark --clients 100 --duration 5 --output reports/c100
python3 tools/lab.py benchmark --clients 500 --duration 5 --output reports/c500
```

Repeat important comparisons several times, alternating versions to reduce systematic
warmup or background-load effects. The generator has a default 200,000-request cap;
check `cap_reached` and `elapsed_s`. For a longer uncapped-by-default-cap run, use
`tools/traffic.py --request-limit 2000000` against `tools/lab.py serve`. Verify the
request cap is still not reached before comparing nominal durations.

The proxy performs application reads/writes in a single thread. A separate Python
backend process handles each backend. The Python load generator also has a single
asyncio thread. The machine may therefore bottleneck in the generator, backend,
scheduler, socket buffers, or proxy. These are end-to-end benchmark observations.
Do not report a same-host result as the proxy's isolated maximum capacity.

Latency is measured from writing a complete request until reading and verifying its
complete response on an established connection. It excludes initial connection
setup. Each client has one message in flight, producing a closed-loop workload.
Slow requests reduce offered load, so this cannot characterize latency under a fixed
external arrival rate. A future open-loop generator can explore queueing effects.

Round-robin distributes connections. It does not guarantee equal message counts when
one backend responds more slowly. The global p99 can also hide slow backends because
fast backends complete more messages: inspect `latency_by_backend_ms` and the report's
per-backend table. The delayed-backend scenario deliberately demonstrates this.

## CPU and memory

`/metrics` exposes cumulative CPU seconds and peak RSS for the proxy only. To compare
CPU usage over an interval, subtract CPU seconds and divide by elapsed wall time.
A ratio of 1.0 represents one core of CPU time. Peak RSS cannot show memory returning
to the OS; it only increases. Read current RSS externally when investigating leaks.
Kernel socket buffers, clients, and backend processes are outside this metric.

The memory bound applies to the proxy's two forwarding buffers, not all allocations.
For N admitted sessions the buffer allocation is 32768*N bytes. Connection metadata,
event-loop tables, admin clients, allocator overhead, and kernel buffers add to this.

## Linux profiling

Use a Linux host with `perf` available and permission to sample your own process:

```sh
perf stat -p PROXY_PID -- sleep 10
perf record -g -p PROXY_PID -- sleep 10
perf report
```

While sampling, generate traffic from another terminal or machine. Use a release build
with debug symbols (`make` defaults to `-O2 -g`). Investigate syscall rate, buffer-copy
cost, and the O(max-connections) timeout scan before adding worker threads or timers.
For connection churn, a generator that repeatedly opens/closes sockets measures a
different workload from the supplied persistent-connection benchmark.

Use `tc netem` only in an isolated Linux lab/network namespace if introducing network
latency, loss, or bandwidth limits. The supplied demo's 20 ms application delay is
not network packet loss and should not be described as such.
