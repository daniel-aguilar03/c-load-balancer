# Architecture and invariants

## Event loop

The proxy owns all session state in one thread. This avoids mutexes and makes state
transitions explicit. Linux uses level-triggered epoll. macOS rebuilds a pollfd array
before each wait. The Linux adapter only updates registrations when interests change.
Sockets are nonblocking; a ready descriptor can still return EAGAIN. Read and write
handlers each perform at most four system calls, and admissions are capped per tick,
to keep individual ready descriptors from monopolizing the loop.

Each registration has a slot and generation number. Returned events carry both.
Closing or suspending a registration invalidates its generation before a slot or
numeric file descriptor can be reused. Dispatch validates generations again because
a previous event in the same batch can close/reuse a session. Fully backpressured
sockets with no useful interest are temporarily removed, avoiding repeated EOF events.

Each iteration checks connection and admin deadlines and starts/finishes health
probes. The epoll/poll wait is at most 50 ms. Timeout scans are O(configured maximum
connections), an explicit simplicity/performance tradeoff.

## Session lifecycle

1. Accept a client and select a currently healthy backend.
2. Reject immediately if no backend is healthy or the connection limit is reached.
3. Allocate two 16 KiB buffers and begin a nonblocking backend connect.
4. On connect readiness, inspect SO_ERROR. On failure, close the session and mark
   the backend down. No bytes are replayed and no transparent application retry occurs.
5. Relay bytes while respecting buffer space and partial writes.
6. Propagate EOF with shutdown(SHUT_WR) after the corresponding buffer drains.
   Continue forwarding in the opposite direction until its EOF.
7. Close both descriptors and free buffers when both directions finish, on a fatal
   socket error, on idle/connect deadline, or on forced shutdown.

The selected backend never changes during a session. Selection counters count
admitted connection attempts, including attempts that fail to connect. Active counts
include backend connection setup until the whole session closes.

## Buffer ownership and backpressure

`buffer[0]` holds bytes read from the client awaiting the backend.
`buffer[1]` holds bytes read from the backend awaiting the client.

A read interest is enabled only while its source buffer has free space. A write
interest is enabled only while the opposite buffer contains pending data (or while
connecting). Sent prefixes are compacted before further reads. EOF does not discard
queued bytes. This bounds user-space forwarding buffers to 32 KiB per active session.
The `backpressure_events` metric counts occasions when a successful read fills a
buffer; it is not elapsed time under backpressure.

## Health state

Each backend has one independent asynchronous TCP connect probe. A probe completes
with success, connection error, or timeout. Consecutive successes/failures trigger
health transitions; opposite results reset the streak. Backends start down.
A real session's backend connection error immediately marks the backend down.
Probes continue even while it is down and bring it back after the success threshold.

This only checks TCP acceptance. A service can accept sockets while returning invalid
data or responding slowly. An application-layer health checker would be a separate
feature with protocol-specific semantics.

## Administration and shutdown

The admin listener supports three small HTTP GET endpoints. It has 16 bounded client
slots, a 2047-byte request limit, a bounded JSON response, and a 2-second lifetime.
Responses are written nonblockingly. HTTP keep-alive and general HTTP parsing are not
implemented. Keep the listener on loopback or a trusted container network.

The signal handler only sets a sig_atomic_t flag. The main loop closes the data
listener, marks readiness false, and continues existing sessions. Once all finish or
the drain deadline expires, it closes remaining descriptors and emits a final metrics
snapshot to stderr. The exit status is zero for both a complete and deadline-limited
drain; inspect `forced_shutdowns` to distinguish them.

## Metrics interpretation

- `accepted`: sockets accepted at the data listener, including later rejections.
- `rejected`: no healthy backend, admission limit, or accepted-socket setup failure.
- `completed`: admitted sessions closed for any reason, not necessarily successful.
- `client_bytes_read` / `backend_bytes_read`: bytes received into proxy buffers,
  not proof the final application consumed them.
- `buffer_capacity_bytes`: active sessions multiplied by 32768; not total process memory.
- `max_rss_bytes`: peak RSS, normalized to bytes across Linux and macOS.
- `cpu_seconds`: cumulative user + system CPU in this proxy process.
- backend `selected`: sessions assigned to it; health checks do not increment this.

## Simulation protocol

A request is a network-byte-order unsigned 32-bit length followed by up to 1 MiB of
binary payload. A response has the same length prefix, then one byte containing the
backend-name length, ASCII backend name, and the original payload. Each traffic client
sends its ID, a monotonically increasing sequence, and random bytes. It verifies the
response exactly. The proxy has no knowledge of this framing.
