# Plan 004: Independent-vset multicore scaling

## Status

- **Status**: DONE
- **Priority**: P1
- **Depends on**: Plans 002 and 003
- **Measured at**: 2026-08-17

## Decision

Keep the protocol tree, `HostState`, shared cache, retained-base bookkeeping,
and fork mappings on their current local actor. Do not convert them to
thread-safe shared state and do not shard a fork lineage.

Move only the already thread-safe host-memory and userfaultfd syscalls to a
fixed eight-thread worker pool. A fair keyed dispatcher admits at most one job
per vset, preserves FIFO ordering within that vset, overlaps independent
vsets, aggregates multi-vset write protection, and treats shutdown barriers as
prefix fences. The actor still makes every cache, residency, recovery, and
fork/COW decision.

The benchmark must distinguish two paths:

1. The common cache-resident guest path, where independent guest threads do
   not traverse the runtime after warmup. This is the linear-scaling acceptance
   workload.
2. A forced-refault diagnostic that evicts the guest PTE after every access.
   This measures Linux userfaultfd wake/schedule throughput, not independent
   guest CPU capacity, and is retained as a non-linear kernel stress test.

## Changes

- Removed the duplicate per-vset segment scan from observability publication
  and changed full snapshots from a workload-controlled 1 kHz cadence to
  100 ms.
- Added a persistent guest-access lease so the profile uses one long-lived OS
  thread per vset instead of an executor/blocking-pool handoff per access.
- Added resident-minor-fault remapping. A present cache page now receives
  `UFFDIO_CONTINUE` rather than leaving the guest blocked.
- Replaced the serial fault syscall loop with a fixed keyed worker pool.
  Same-vset work remains FIFO; different vsets reached eight concurrent jobs.
- Added queue, active-concurrency, service, and join-failure evidence to runtime
  artifacts.
- Added deterministic tests for distinct-vset overlap, same-vset
  serialization, barrier prefix draining, worker panic recovery, and persistent
  guest-operation serialization.
- Added a manifest-validating scaling verifier that requires three comparable
  1/2/4/8-core repetitions, zero data errors, at least 80% median efficiency,
  and no more than 20% p99 regression.

## Lima acceptance matrix

The retained workload uses one runtime lane, 64 independent compute vsets, a
prefaulted 256-page hot set per vset, cache headroom of 512 pages per vset, 80%
reads / 20% writes, persistent guest threads, one-in-1,024 latency sampling,
and three 30-second repetitions. CPU sets are pinned to cores `0`, `0-1`,
`0-3`, and `0-7`.

| Cores | Throughput min | Throughput median | Throughput max | Speedup | Efficiency | Median CPU cores | Median p50 bound | Median p99 bound |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 99.15 M/s | 99.33 M/s | 107.52 M/s | 1.000x | 100.0% | 0.982 | 10 us | 10 us |
| 2 | 227.01 M/s | 230.26 M/s | 238.69 M/s | 2.318x | 115.9% | 1.959 | 10 us | 10 us |
| 4 | 482.78 M/s | 513.78 M/s | 516.15 M/s | 5.172x | 129.3% | 3.912 | 10 us | 10 us |
| 8 | 865.16 M/s | 1.027 B/s | 1.075 B/s | 10.340x | 129.2% | 7.822 | 10 us | 10 us |

All 12 cells completed with zero data errors. Superlinear speedup is plausible
for this fixed 64-thread fleet because the one-core baseline incurs much more
run-queue contention and the larger CPU sets also increase aggregate cache
capacity. The acceptance claim is only that the lower-bound 80% gate passes;
it does not assume superlinear capacity on other hosts.

The verifier command is:

```sh
scripts/verify-independent-vset-scaling.sh ARTIFACT_ROOT
```

The final post-refactor smoke reproduced the result at 111.00 M/s on one core
and 968.57 M/s on eight cores (8.73x, 109.1% efficiency), with the same 10 us
p99 bound and zero errors.

## Fork, sharing, and failure gates

A separate one-runtime mixed-lineage smoke used 64 total vsets, 57 measured
descendants, seven independent roots, and up to seven generations. It recorded:

- 5,963 shared-base fills during the measured phase;
- 32,125 divergent write-protect faults;
- eight concurrent fault syscall jobs;
- zero remaining jobs and zero worker failures at the snapshot;
- zero data mismatches and no runtime incidents.

This demonstrates that concurrency is below the actor-owned sharing decision:
fork descendants still use the same retained cache and mappings and then
diverge through copy-on-write.

The worker-panic regression returns an error to the affected caller, releases
the vset key, executes the same-vset successor, and increments the failure
counter instead of stranding the queue or a shutdown barrier.

## Forced-refault diagnostic

The forced-refault path does not pass a linear-throughput gate. With one runtime
and the fixed worker pool, a screen increased from 24.66 K/s at one core to
80.36 K/s at eight cores (3.26x, 40.7% efficiency) while using about 5.53 CPU
cores. Eight separate one-core processes also reached only 134.23 K/s versus
37.50 K/s for one process (3.58x), so a single daemon address space is not the
sole limiter.

The matching CPU-only SHA-256 control scaled from 3.09 M units/s to 23.59 M
units/s (7.63x, 95.4% efficiency). Stack samples attributed most forced-refault
CPU to task switching, userfaultfd wakeups, futex wakeups, and virtualized clock
reads. The kernel round trip, not unavailable Lima cores or serialized actor
CPU, is the limiter. Keep this workload as a fault-progress and tail-latency
diagnostic; making every resident access fault would be the wrong production
design.

## Verification and retained artifacts

- Final dispatcher unit suite: 29 passed, one performance profile ignored.
- Final full workspace suite: passed, including Linux runtime, Firecracker,
  fork/COW, migration, recovery, replica, simulation, and workload tests.
- Full Clippy and rustfmt lint gate: passed with warnings denied.
- Scaling verifier, shell syntax, formatting, and diff checks: passed.

Raw evidence is retained under the ignored artifact tree:

- `artifacts/independent-scaling-final/`
- `artifacts/concurrent-fork-smoke/`
- `artifacts/forced-refault-diagnostics/`
- `artifacts/process-isolation-diagnostics/`
- `artifacts/post-refactor-screen/`
