# Recorded Linux demonstration

This is an actual run of the failure demonstration, not a target or projected result.
The source commit, environment, duration, request cap, error counts, and per-backend
latencies are recorded in `results.json`. `report.html` is a self-contained rendering.

Reproduce from the repository root:

```sh
docker build --target test -t streamgate:test .
docker run --rm -v "$PWD/reports:/app/reports" streamgate:test \
  python3 tools/lab.py demo --clients 90 --duration 1.5 --output reports/linux-example
```

This was measured in a Linux container on Docker Desktop on Apple Silicon.
The same container runs the clients, proxy, and backends. Results include all those
components and host scheduling. They are not a production service-level objective.

The crash exercise explicitly verifies an interrupted existing session. The zero
traffic-error counts in the table apply to phases run after the health state settles.
A newly reconnecting application reaches a healthy backend. Requests are not replayed.

The deliberately delayed backend stays TCP-healthy, and its per-backend latency
shows the added delay. Global percentiles can obscure that observation.
