# Investigating failures

## Start with repeatable evidence

```sh
make test
make sanitize
python3 tools/lab.py demo --output reports/investigation
```

Each lab writes proxy JSON logs, backend logs, and measured results. Match a client
failure to health transitions, connect/I/O error counters, and timeout counters.
`completed` counts all closed sessions; it does not mean every request succeeded.
Keep the exact compiler flags and commit with any reported incident.

The integration suite covers binary transfer, fragmentation, half-closes, slow
receivers, crash/recovery, connection limits, idle expiry, resets, descriptor reuse,
leastconn selection, admin behavior, and graceful/deadline-limited shutdown. Tests
use real loopback sockets; they do not model every kernel or network failure mode.

## AddressSanitizer and UndefinedBehaviorSanitizer

`make sanitize` uses ASan + UBSan on Linux and UBSan on macOS. It builds a separate
instrumented executable and runs the same socket
suite. The optimized normal executable remains available. Sanitizers can catch buffer
errors and use-after-free that don't visibly corrupt a small demonstration. Tests
also inspect the proxy logs for sanitizer diagnostics.

### Observed macOS sanitizer-runtime startup hang

During development on macOS 26.5 with the installed Apple compiler runtime, an
ASan-instrumented `--help` invocation stalled before reaching `main`. The normal
executable and Linux ASan/UBSan socket suites succeeded. A one-second native `sample`
showed this path (abbreviated):

```text
libSystem_initializer -> __malloc_init -> AsanInitInternal
-> InitializeShadowMemory -> MemoryRangeIsAvailable -> get_dyld_hdr
-> _Block_copy -> __sanitizer_mz_malloc -> AsanInitFromRtl
-> StaticSpinMutex::LockSlow
```

This supports a runtime initialization deadlock rather than a proxy event-loop hang:
the proxy's own code had not begun executing. macOS therefore defaults to UBSan;
Linux retains ASan + UBSan. To try ASan on a compatible macOS toolchain, explicitly
run `make sanitize SANITIZERS=address,undefined`. This is a documented environment
limitation, not a claim that native macOS ASan validation passed.

## GDB on Linux

Install GDB through your Linux distribution. Run a demo backend in one terminal:

```sh
python3 tools/backend.py --port 9001 --mode echo --name debug-backend
```

Start the debug-symbol build under GDB in another:

```sh
make
ulimit -c unlimited
gdb --args ./build/streamgate --backend 127.0.0.1:9001 --max-connections 128
```

Inside GDB:

```text
run
# Ctrl+C pauses the process after it is running
bt
info locals
p stats
p backends[0]
p connections[0]
generate-core-file build/streamgate.snapshot.core
continue
```

GDB-generated snapshots let you practice postmortem inspection without adding an
intentional fault to the real proxy. A snapshot of a healthy process is not evidence
that a crash occurred. Generate traffic in another terminal before pausing if you
want active-session state to inspect.

For a real crash, preserve the matching executable and core file. On Linux the
location depends on `/proc/sys/kernel/core_pattern`; systems with systemd may require
`coredumpctl`. Avoid changing host-wide core settings just for this lab.

```sh
gdb ./build/streamgate build/streamgate.snapshot.core
```

Useful postmortem commands:

```text
info threads
thread apply all bt full
frame 0
info locals
p stats.active
p connections[0].buffer[0]
```

If symbols don't match the executable that crashed, the analysis is unreliable.
Container debugging may require explicitly granting ptrace permissions to a disposable
debug container; they are not required by the normal runtime. On macOS, use LLDB
(`lldb -- ./build/streamgate ...`) or the sanitizer target instead.

## Concrete failure exercises

1. Stop one backend. Observe a backend-down event and new connection distribution.
   Existing connections to it fail; the proxy never silently migrates their state.
2. Restart it. Check the success threshold before it returns to selection.
3. Delay application responses. TCP health remains up; per-backend latency rises.
4. Open idle sessions with a short idle timeout. Verify timeout counters and cleanup.
5. Send SIGTERM during traffic. Readiness becomes false; current sessions can finish
   until the drain deadline. Inspect the final `forced_shutdowns` count.

Record symptoms, hypotheses, observations, root cause, and the verification after a
fix. Distinguish a reproduced incident from an exercise or a proposed investigation.
