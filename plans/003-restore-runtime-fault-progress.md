# Plan 003: Restore fork-write correctness and runtime fault progress

> **Executor instructions**: Fix correctness and deterministic progress before
> changing fault-work concurrency. Preserve the single local protocol actor,
> host-wide cache policy, shared-base residency, and shared mappings. Regression
> tests must encode successful intended behavior; never add a green test whose
> expectation is the abort or hang.

## Status

- **Status**: DONE
- **Priority**: P0
- **Effort**: M
- **Risk**: HIGH — incorrect userfault classification can corrupt a fork or
  leave a faulting guest blocked forever
- **Depends on**: Plan 002 Lima evidence
- **Category**: runtime correctness and progress

## Objective

Make all of the following repeatably correct on Linux before resuming performance
work:

1. A forked child can read a shared-base page, write it, diverge from its parent,
   and checkpoint without aborting or modifying the parent's bytes.
2. One, two, and four concurrent cold-faulting vsets complete a bounded phase
   repeatedly without leaving a guest blocked in userfaultfd.
3. Eviction/refault integration completes deterministically.

This plan does not parallelize `FaultWork`, shard state, or make local actor
futures `Send`.

## Step 1: Add failing intended-behavior regressions

1. Add a Linux integration regression that creates a base and child, reads the
   inherited page, writes the child, then verifies distinct parent/child bytes
   across checkpoint and refault. It must expect success and should fail before
   the fix.
2. Add a bounded cold-fault stress regression at one and four vsets. Use guest
   threads separate from the local actor thread and fail on a deadline rather
   than hanging the suite.
3. Keep any process-abort repro in a child-process test or outside the green
   suite until the expectation is successful completion.
4. Run the existing eviction/refault integration test under the same deadline.

## Step 2: Preserve kernel fault classification

1. Extend the internal `GuestFault` value with the kernel `wp` and `minor`
   classification already available in `FaultEvent`. This is an internal source
   change, not a wire or storage format.
2. Update every simulator/model constructor explicitly so tests state which
   fault kind they represent.
3. Route a write-protect fault separately from a missing/minor fault. A shared
   page already mapped read-only must acquire the correct per-vset dirty/cache
   accounting and then use `unprotect`; it must not call `UFFDIO_CONTINUE`.
4. Preserve copy-on-fault capture ordering, cache reservation, mutation sequence,
   pressure wakeups, and parent/child byte isolation.
5. Add core model tests for shared read→write, direct write-first divergence,
   and write during capture.

## Step 3: Find and remove the progress loss

1. Add bounded counters for UFFD events read, events injected, fault actors
   started/completed, responses delivered, and reader exits by reason. Do not
   add per-event logs or unbounded request state.
2. Make the readiness reader explicitly drain until `WouldBlock`; distinguish a
   transient empty read from a terminal descriptor error and record terminal
   errors before exiting.
3. At a timed-out regression, assert which boundary owns the unresolved fault:
   kernel queue, injector, core fault task, `FaultWork`, or response delivery.
4. Check notification and cancellation races in `AsyncFd`, the critical
   injector, `TaskSet` completion accounting, and shutdown. Fix the narrow
   boundary demonstrated by the counters rather than adding polling.
5. Prove that a fault reader cannot exit silently while its vset remains live.

## Step 4: Verification gate

Run on Linux:

```text
cargo test -p blockd-hostmem --test uffd_linux -- --test-threads=1
cargo test -p blockd-runtime --test replica_e2e_linux durable_eviction_bounds_residency_and_refaults_exact_disk_bytes -- --exact --test-threads=1
cargo test -p blockd-runtime --test <new-fork-write-regression> -- --test-threads=1
cargo test -p blockd-runtime --test <new-fault-progress-regression> -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Repeat the new progress regression at least 100 times in release mode. Then
rerun the alternating detailed-metrics control and the 1/2/3/4-vset cold matrix.
Do not resume Plan 001 unless all repetitions complete and Plan 002's hotspot
and instrumentation gates are satisfied.

## STOP conditions

- Stop if a proposed fix weakens parent/child byte isolation, capture ordering,
  or durable checkpoint correctness.
- Stop if it requires sharding the host cache or shared-base residency.
- Stop if progress depends on periodic polling, sleeps, or a larger timeout.
- Stop if any low-level UFFD or existing runtime integration regression appears.

## Result

Completed on 2026-08-17 without sharding actor state, cache state, base
residency, or mappings.

- Kernel `wp` and `minor` classification now reaches the core fault actor.
  Writing a mapped shared-base page promotes that vset to private dirty state
  and clears write protection instead of attempting another continue.
- Synthetic guest accesses leave the local actor thread before touching a
  userfaultfd mapping. The non-`Send` protocol tree now runs on one dedicated
  current-thread runtime, so external fault delivery cannot depend on a nested
  caller-owned `LocalSet` being repolled.
- Blocking mapping/ioctl work remains serialized on one dedicated worker. This
  is a progress correction, not fault-work parallelism.
- Fault readers explicitly drain readiness, retain their task handles, report
  read/injection/exit counters, and are stopped during shutdown.

Lima verification:

- The fork read→write→checkpoint→refault regression completed 100/100 bounded
  release-mode repetitions.
- The four-vset concurrent cold-fault regression completed 100/100 bounded
  release-mode repetitions with balanced read/injection counters.
- Ten consecutive realistic four-vset, 65,536-page, five-second cold profiles
  completed; the same cell previously stalled intermittently.
- A two-vset fork-star mixed profile completed its 30-second measured phase.
- All 13 Linux host-memory tests passed, including all 11 direct UFFD contract
  tests.
- Workspace tests and clippy passed after the correctness changes.

The separate replica-release follow-up is resolved. Recovery now retains the
exact passive cleanup target, startup preserves it, release acknowledgements
survive assignment-CAS races, and the rare acknowledgement reconnects instead
of using a socket to the dead primary instance. All seven Linux replica
end-to-end cases pass together in Lima.

The final workspace-wide Linux run passes completely in Lima. Follow-up fixes
made the migration assertion distinguish primary residue from a legitimate
passive spool, moved nested deterministic simulation work off the runtime
thread, isolated primary NVMe-pressure accounting from passive capacity,
established a complete new memory baseline for checkpoints after cold boot,
and sized the simulated TCP backlog for the transport's bounded peer fan-in.
