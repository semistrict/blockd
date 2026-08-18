# Plan 001: Parallelize independent production fault work without sharding actor state

> **SUPERSEDED — DO NOT EXECUTE.** Plan 002 measures realistic large-host
> workloads before selecting an optimization. This plan is retained to preserve
> plan history and numbered identifiers; its proposed implementation has not
> been validated as the actual bottleneck.

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report; do not improvise. When done, update the status row for this plan in
> `plans/README.md`, unless a reviewer dispatched you and told you they maintain
> the index.
>
> **Drift check (run first)**:
> `git diff --stat 802717e..HEAD -- crates/runtime/src/actor_host.rs crates/runtime/tests/loop_interference_linux.rs scripts/run-fault-multicore-profile.sh TESTING.md`
> If either in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding. A semantic
> mismatch is a STOP condition.

## Status

- **Status**: REJECTED — superseded by benchmark-first Plan 002
- **Priority**: P1
- **Effort**: M (approximately one to two focused days, including Linux tests)
- **Risk**: HIGH — userfaultfd ordering mistakes can corrupt captures or leave
  guest faults blocked indefinitely
- **Depends on**: none
- **Category**: performance and concurrency correctness
- **Planned at**: commit `802717e`, 2026-08-17

## Why this matters

The protocol core intentionally owns host-wide cache, fork-lineage, and vset
state on one current-thread actor tree. That decision should remain unchanged:
forks share base residency and physical memory mappings, and pressure policy
needs one authoritative cache view. However, production currently sends as many
as 64 concurrent core fault actors into a world-side worker that executes only
one blocking mapping or userfaultfd operation at a time. The Linux interference
profile records severe noisy-neighbor collapse at 48 busy vsets.

This plan removes that narrower serialization point. It keeps `HostState`,
`Cache`, `ProductionWorld`, and the actor tree local and non-`Send`; only
thread-safe `Arc<VsetHost>` mapping/kernel operations execute concurrently.
Ordering remains FIFO for each vset, write-protect calls spanning several vsets
complete only after every subgroup completes, and graceful shutdown retains a
prefix-draining barrier.

## Current state

### Design constraints that must not change

- `DESIGN.md:122-136` defines one current-thread protocol actor tree in both
  simulation and production. Production places the non-`Send` tree on one
  `LocalSet` and sends network, blocking, and timer work through Tokio services.
- `crates/core/src/world.rs:9-12` requires guest-memory operations to be applied
  in call order per page. This is the minimum ordering contract. The new worker
  may provide the stronger and simpler guarantee of FIFO ordering per vset.
- `crates/core/src/world.rs:25-26` states that dropping a world-method future
  cancels only the wait; already-submitted I/O may still land. Do not attempt to
  make blocking syscalls cancellable in this plan.
- `crates/core/src/cache.rs:82-106` stores host-wide residency, dirty/unstable
  tracking, eviction order, and shared base residency in one `Cache`. Do not
  partition or lock this structure.
- `crates/hostmem/src/linux.rs:121-134` makes `HostRegion` explicitly `Send` and
  `Sync`; `GuestView` has the same property at lines 243-251. Moving operations
  on these objects to Tokio's blocking pool is an intended boundary.
- `crates/runtime/src/fc.rs:444-518` owns the separate Firecracker fork-sharing
  path. Its `ShmemServer` owns one shared-memory file per sharing domain and
  already serves connections on multi-thread Tokio tasks. Do not alter it.

### The serialization point

`crates/runtime/src/actor_host.rs:283-318` currently has one receiver and awaits
each blocking job before receiving the next:

```rust
enum FaultWork {
    Fill { /* one page */ },
    Unprotect { /* one page */ },
    Evict { /* one page */ },
    WriteProtect { /* groups from several vsets */ },
    Barrier { /* graceful shutdown */ },
}

async fn fault_work_loop(mut work: UnboundedReceiver<FaultWork>) {
    while let Some(item) = work.recv().await {
        if tokio::task::spawn_blocking(move || execute_fault_work(item))
            .await
            .is_err()
        {
            return;
        }
    }
}
```

`crates/core/src/engine/host.rs:471-498` independently admits up to
`FAULT_CONCURRENCY` (64) protocol fault actors. Those actors can overlap object
reads and state-machine waits, but every final fill, unprotect, eviction, and
write-protect syscall funnels through the single loop above.

`crates/runtime/src/actor_host.rs:624-747` establishes the top-level call
shapes:

- `arm_write_protect` groups pages by `VsetId`, submits one multi-vset
  `WriteProtect`, and awaits one aggregate reply.
- `fill`, `unprotect`, and `evict` each target exactly one page and therefore one
  vset.
- `fill_shared` clones immutable source bytes and targets one `VsetHost`; the
  actor-owned base-residency decision has already happened before this method.

`crates/runtime/src/actor_host.rs:1035-1049` uses `FaultWork::Barrier` during
graceful shutdown. The barrier must mean: all work accepted before the barrier
has completed. Work submitted after it must not delay that reply.

### Existing performance evidence

`crates/runtime/tests/loop_interference_linux.rs:189-225` exercises real
userfaultfd and disk I/O at 0, 4, 16, and 48 noisy vsets. Its current comment
records about 56 probe operations in three seconds and about 50 ms per operation
at 48 noisy vsets. The test only verifies that traffic flowed; it does not prove
that production fault work overlapped.

### Applicable repository conventions

- Use `BTreeMap`, `BTreeSet`, and `VecDeque`; `clippy.toml` rejects hash-based
  collections because deterministic ordering is a project convention.
- Keep blocking syscalls behind `tokio::task::spawn_blocking`; see
  `crates/runtime/src/world.rs:34-63`.
- Fixed per-host worker tasks are acceptable. Per-vset permanent tasks are not:
  `REQUIREMENTS.md:R1.3` requires 10,000 mostly idle live guests to have small
  fixed overhead.
- Tests must assert intended behavior. Do not add a test that passes merely
  because the current serialization or latency collapse reproduces.
- Do not change wire, storage, or public API formats.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Confirm Linux | `test "$(uname -s)" = Linux` | exit 0 |
| Focused unit tests | `cargo test -p blockd-runtime --lib fault_work -- --nocapture` | all matching tests pass; at least four tests run |
| Interference profile | `cargo test --release -p blockd-runtime --test loop_interference_linux -- --test-threads=1 --nocapture` | one profile test passes and prints rows for 0, 4, 16, and 48 noisy vsets |
| Multicore profile | `scripts/run-fault-multicore-profile.sh artifacts/fault-multicore` | serial-control and parallel rows are recorded for each usable core count |
| Runtime tests | `cargo test -p blockd-runtime` | all tests pass |
| Portable workspace | `cargo test --workspace` | all tests pass on the current platform |
| Linux kernel suite | `scripts/run-linux-kernel-tests.sh artifacts/linux-kernel` | all listed targets pass |
| Lint | `./lint.sh` | Clippy and rustfmt exit 0 with no warnings |

Linux is mandatory for the focused scheduler tests and the performance profile
because `actor_host` is compiled only on Linux. If Linux is unavailable, do not
claim this plan is complete; use the STOP condition below.

## Scope

**In scope — the only source files to modify:**

- `crates/runtime/src/actor_host.rs`
- `crates/runtime/tests/loop_interference_linux.rs`
- `scripts/run-fault-multicore-profile.sh` (create)
- `TESTING.md` (document the new ignored profile)
- `plans/README.md` only for the final status update

**Out of scope — do not modify:**

- `crates/core/**` — actor state, cache policy, fault state machine, and world
  traits remain unchanged.
- `crates/exec/**` — do not replace `LocalSet`, `spawn_local`, or the task
  collection.
- `crates/runtime/src/fc.rs` and `crates/hostmem/**` — shared Firecracker maps
  and unsafe `Send`/`Sync` implementations are not part of this change.
- `crates/sim/**` — simulation has no production blocking worker and must not
  gain one.
- Any sharding of `HostState`, `Cache`, vsets, fork lineages, or shared-memory
  backing files.
- Any dependency, configuration-format, wire-format, or storage-format change.

## Git workflow

- Branch: `semistrict/parallel-fault-work`
- Use imperative commit messages matching recent history, for example:
  `Parallelize independent fault work`.
- Do not push or open a pull request unless instructed separately.

## Target design

Implement a bounded, fair, keyed dispatcher inside
`crates/runtime/src/actor_host.rs` with these invariants:

1. `ProductionWorld` remains local and non-`Send`. `HostState`, `Cache`, and
   `shared_pages` are never moved to worker threads.
2. A fixed `FAULT_WORK_CONCURRENCY` limits active blocking jobs. Start with 8,
   matching the existing bounded file and part-fetch worker counts. Do not make
   it configurable in this plan.
3. At most one job for a given `VsetId` is active. Jobs for that vset execute in
   submission order. Jobs for distinct vsets may overlap up to the global
   limit.
4. No permanent per-vset worker or queue task exists. Queue state is created
   only while work is pending and removed after the key becomes idle.
5. `WriteProtect` is split into one job per vset. Its one caller reply is sent
   only after every subgroup completes; any subgroup error makes the aggregate
   result an error.
6. When the dispatcher receives `Barrier`, it temporarily stops admitting
   later requests, drains all already-admitted queued and active work, replies
   to the barrier, then resumes receiving. This makes the barrier a prefix
   fence without requiring sequence-number bookkeeping.
7. A blocking-task join error completes that job with `Err(())`; it must not
   strand its direct caller, a write-protect aggregate, or a shutdown barrier.
   The protocol root will convert the returned guest-memory error into its
   existing failure behavior.
8. Dropping the runtime may still detach an already-running blocking syscall,
   matching the existing world contract. Graceful `Runtime::shutdown` must
   drain through the barrier.
9. Queue selection is round-robin across ready vset keys. A continuously busy
   key may not keep another ready key from starting.

Use a private structure equivalent to the repository's existing keyed-queue
pattern in `crates/core/src/engine/keyed_queue.rs`, but keep it local to
`actor_host.rs` so this change does not expose or relocate core internals. It
should maintain:

- `BTreeMap<VsetId, VecDeque<QueuedFaultJob>>` for pending FIFO queues;
- `VecDeque<VsetId>` for fair ready-key rotation;
- `BTreeSet<VsetId>` for active keys;
- a total active count bounded by `FAULT_WORK_CONCURRENCY`;
- local write-protect batch state keyed by a monotonically allocated internal
  identifier.

Refactor the blocking function so it accepts one per-vset job and returns
`Result<(), ()>`. Reply routing belongs to the dispatcher, not to the blocking
function. This keeps aggregation, failures, and barriers on the local task.

## Steps

### Step 1: Record the Linux baseline before editing

On a Linux host with userfaultfd enabled, run the release interference profile
and retain its four output rows in the implementation notes or commit message.
Record probe operation count, p50/p90/p99, noisy operations per second, loop
occupancy, and fill-dispatch time for each fleet size. This baseline is for
comparison only; do not encode the current collapse as an accepted test result.

**Verify**:
`cargo test --release -p blockd-runtime --test loop_interference_linux -- --test-threads=1 --nocapture`
→ exits 0 and prints one row each for 0, 4, 16, and 48 noisy vsets.

### Step 2: Add intended-behavior scheduler tests first

In the existing `#[cfg(test)]` module of
`crates/runtime/src/actor_host.rs`, add test-only fault jobs that can signal
when a blocking job begins and wait on a test-controlled release primitive.
Use bounded timeouts so failures report instead of hanging CI. Add these tests:

1. `fault_work_overlaps_distinct_vsets`: two jobs with different `VsetId`s both
   reach their blocking section before either is released. This must fail under
   the original one-at-a-time loop and pass under the target dispatcher.
2. `fault_work_serializes_one_vset`: the second job for one `VsetId` cannot
   enter until the first completes, while a job for a different vset can enter.
3. `fault_work_barrier_drains_only_its_prefix`: a blocked job submitted before
   the barrier delays the barrier; a blocked job submitted after the barrier
   does not delay the barrier reply and begins only after barrier admission
   resumes.
4. `fault_work_batch_waits_for_every_vset_and_combines_errors`: a synthetic
   multi-vset batch does not reply early and returns `Err(())` when any subgroup
   fails.
5. `fault_work_join_failure_does_not_strand_dispatch`: a deliberately panicking
   test job yields an error reply, later independent work completes, and a
   subsequent barrier completes.
6. `fault_work_reaps_idle_keys`: after jobs finish, no pending or active
   per-vset queue state remains.

Test-only variants must be under `#[cfg(test)]`; production callers must not be
able to construct them. These tests encode the intended correct behavior and
must not be weakened to observe the current bug.

**Verify before implementation**:
`cargo test -p blockd-runtime --lib fault_work_overlaps_distinct_vsets -- --nocapture`
→ fails because the second independent job cannot enter. Do not commit a green
test that accepts serialization.

### Step 3: Make every production job explicitly keyed

Refactor `FaultWork` into an ingress request and a smaller per-vset blocking
job shape.

- `Fill`, `Unprotect`, and `Evict` derive their key from
  `page.volume.vset`.
- Change the internal `WriteProtect` groups from `(Arc<VsetHost>, Vec<usize>)`
  to `(VsetId, Arc<VsetHost>, Vec<usize>)`; the grouping map in
  `arm_write_protect` already has the vset ID.
- `Barrier` remains an ingress-only control request and never becomes a
  blocking job.
- Keep the exact existing syscall bodies for fill, unprotect, evict, and
  write-protect. Only move reply handling out of `execute_fault_work` and have
  the blocking job return `Result<(), ()>`.

Do not key by a hash, pointer value, or page number. FIFO by `VsetId` is a
deliberately stronger guarantee than the per-page world contract and avoids
ordering bugs among fill, write-protect, unprotect, and eviction for one guest.
The separate `ShmemServer` sharing domain does not traverse this queue.

**Verify**:
`cargo check -p blockd-runtime --tests`
→ exits 0 on Linux; every `FaultWork` construction has an explicit derivable
vset key, and no public API changed.

### Step 4: Implement the bounded fair dispatcher

Replace the one-at-a-time `fault_work_loop` with a loop that concurrently
selects between new ingress and blocking-job completions.

- Admit ready keys while active count is below `FAULT_WORK_CONCURRENCY`.
- Spawn each admitted job with `tokio::task::spawn_blocking` and deliver its
  join result to a local completion channel.
- Mark the key inactive on every completion path, including join errors, then
  rotate it to the back of the ready queue if more work remains.
- Route a direct job result to its original `Injector<Result<(), ()>>`.
- Update a write-protect batch's remaining count and accumulated failure; send
  its original reply exactly once when remaining reaches zero.
- If a write-protect request has no compute-vset groups, reply `Ok(())`
  immediately.
- On a barrier, stop polling the ingress receiver until pending and active work
  are both empty, send the barrier reply, then resume ingress.
- If ingress closes, drain already-admitted work and exit. Do not abandon
  callers already represented in the queue.

Add maximum-in-flight observation within `actor_host.rs` using atomics in the
existing `Shared` structure, and expose it through a small read-only `Runtime`
method used by the Linux profile. Track current and maximum blocking jobs; the
current count must return to zero after graceful shutdown. Do not add a public
configuration field or modify `DaemonStats` wire/storage representations.

**Verify**:
`cargo test -p blockd-runtime --lib fault_work -- --nocapture`
→ all six new tests pass, at least two different-vset jobs are observed active
at once, same-vset FIFO holds, batch and barrier tests finish, and idle-key state
is empty.

### Step 5: Turn the interference profile into a concurrency regression gate

Update `crates/runtime/tests/loop_interference_linux.rs`:

- Remove the comment that says latency collapse itself is "the finding."
- Record the runtime's maximum world-side fault-work concurrency in
  `PhaseResult` and print it with each profile row.
- Keep the existing traffic-shape assertions.
- For every phase with at least two noisy vsets, assert that maximum
  world-side fault-work concurrency is at least 2. This is a
  machine-independent functional property and directly prevents restoration
  of the one-at-a-time worker.
- Do not invent an absolute microsecond threshold. `TESTING.md` intentionally
  treats machine latency values as profiles, and the functional overlap tests
  provide the deterministic correctness gate.

Run the release profile on the same host and build profile used for Step 1.
Compare the four rows. Include the before/after table in the handoff. Treat
worse 16- or 48-vset probe throughput or tail latency as an investigation
signal, not something to hide by weakening assertions.

**Verify**:
`cargo test --release -p blockd-runtime --test loop_interference_linux -- --test-threads=1 --nocapture`
→ exits 0, prints all four rows plus maximum fault-work concurrency, and the 4,
16, and 48 noisy-vset phases report a maximum of at least 2.

### Step 6: Verify kernel ordering and shutdown behavior

Run the entire Linux kernel suite because the modified queue covers fills,
write protection, unprotection, eviction, and shutdown across recovery,
replication, migration, and real I/O tests. Pay particular attention to hangs:
a timeout usually indicates a lost reply, an incomplete batch, or a barrier
that cannot drain.

After the suite, run lint and the workspace tests. Do not change timeouts merely
to make a hang pass.

**Verify**:

1. `scripts/run-linux-kernel-tests.sh artifacts/linux-kernel`
   → all eight listed targets pass, including `loop_interference_linux`.
2. `cargo test -p blockd-runtime`
   → all runtime tests pass.
3. `cargo test --workspace`
   → all portable workspace tests pass.
4. `./lint.sh`
   → exits 0 with no Clippy or formatting warnings.

### Step 7: Audit the implementation diff and defer the status update

Confirm that only the two in-scope source files and the plan status changed.
Confirm there are no new `Arc<Mutex<HostState>>`, `tokio::spawn` calls in core,
per-vset permanent tasks, or configuration fields.

**Verify**:

- `git diff --check` → no whitespace errors.
- `git status --short` → only
  `crates/runtime/src/actor_host.rs`,
  `crates/runtime/tests/loop_interference_linux.rs`, and
  the benchmark/documentation files explicitly listed in Scope are modified by
  the executor.
- `rg -n "Arc<Mutex<HostState|FAULT_WORK_CONCURRENCY" crates/core crates/runtime/src/actor_host.rs`
  → no `Arc<Mutex<HostState` match; exactly the intended production
  concurrency constant and its uses are present in `actor_host.rs`.

Do not update Plan 001 to `DONE` yet. Step 8's multicore profile is also a
required gate.

### Step 8: Add multicore profiles that isolate dispatcher scaling

Add two ignored, Linux-only release profiles to the existing private test
module in `crates/runtime/src/actor_host.rs`. They must exercise the real
production dispatcher and real `HostRegion`/`GuestView`/userfaultfd operations;
they must not replace blocking jobs with sleeps. Keeping the profiles beside
the private implementation lets them select a test-only dispatcher width
without widening the runtime's public API.

The profile must contain two workloads:

1. **Independent fill throughput**: create enough distinct compute vsets and
   missing pages to keep all selected workers busy. Submit real per-page fill
   jobs spread round-robin across vsets. Report completed fills, elapsed time,
   fills/second, and observed maximum in-flight jobs.
2. **Mixed fault work**: across distinct vsets, execute a repeatable mixture of
   fill, write-protect, unprotect, and eviction operations. Preserve valid
   operation order within each vset. Report total operations/second and counts
   by operation. This catches a design that scales byte copies but serializes
   the relevant ioctls.

The profiles must accept a test-only environment variable
`BLOCKD_PROFILE_FAULT_WORKERS` in the range 1 through
`FAULT_WORK_CONCURRENCY`. This value selects dispatcher width for the profile
only; production continues to use the constant. Invalid or missing values must
respectively fail loudly or default to `FAULT_WORK_CONCURRENCY`.

Create `scripts/run-fault-multicore-profile.sh` to generate the comparison
matrix reproducibly:

- Read the process's allowed CPU list from `/proc/self/status` rather than
  assuming CPUs begin at zero; expand comma-separated ranges safely.
- Test core counts 1, 2, 4, and 8, skipping counts larger than the allowed set.
- For each core count greater than one, run both:
  - a **serial control** with `BLOCKD_PROFILE_FAULT_WORKERS=1`; and
  - a **parallel candidate** with worker count equal to the core count, capped
    at `FAULT_WORK_CONCURRENCY`.
- Pin each run to the same first N allowed CPUs with `taskset --cpu-list`.
- Run the exact same release test binary and workload sizes for control and
  candidate rows.
- Write the full command, revision, kernel, Rust version, allowed CPU list,
  worker count, and profile output to the requested artifact directory.
- Refuse to overwrite an existing non-empty result directory. A caller must
  choose a new directory for every run.

This produces the minimum useful post-change matrix. The one-worker rows run
the exact new queue with concurrency limited to one, modeling the old global
serialization while holding queue code, workload, CPU affinity, and host
constant. Step 1's interference profile is the separate pre-change end-to-end
baseline.

| Allowed cores | Workers | Role | Expected result after this plan |
|---------------|---------|------|---------------------------------|
| 1 | 1 | single-core baseline | establishes non-parallel overhead |
| 2 | 1 | serial control | approximately flat versus the 1-core baseline |
| 2 | 2 | parallel candidate | higher independent-fill and mixed-work throughput |
| 4 | 1 | serial control | approximately flat versus other serial controls |
| 4 | 4 | parallel candidate | improves again unless memory/syscall limits dominate |
| 8 | 1 | serial control | models the old global worker on a larger host |
| 8 | 8 | production-width candidate | best or plateaued throughput, never required to scale linearly |

The profiles are expected to improve independent-vset throughput with 2 and 4
workers. Eight workers may plateau because page copies, userfaultfd ioctls,
memory bandwidth, or the kernel become limiting. Do not require linear scaling
and do not encode a machine-specific throughput threshold. Correctness
assertions must still require all submitted jobs to complete, per-vset order to
hold, and observed maximum in-flight jobs to reach at least 2 in every parallel
candidate.

Use each same-core one-worker row as the controlled multicore comparison.
Include a summary table with these columns in the handoff:

`revision, cores, workers, workload, operations, seconds, operations_per_second, max_in_flight, speedup_vs_same_core_serial`.

Update `TESTING.md` under Performance profiles with the script command, Linux
and userfaultfd requirements, the one-worker-control interpretation, and the
rule that results are comparable only on the same host and build profile.

**Verify**:

1. `cargo test --release -p blockd-runtime --lib fault_work_multicore_profile -- --ignored --nocapture --test-threads=1`
   → both workloads complete and print one machine-readable result row each.
2. `scripts/run-fault-multicore-profile.sh artifacts/fault-multicore`
   → exits 0; records the 1-core baseline and every supported serial/parallel
   pair up to 8 cores; parallel rows report maximum in-flight of at least 2.
3. Run the script again with the same output directory
   → exits nonzero before running a benchmark and preserves the prior results.
4. Compare same-core control/candidate rows
   → 2- and 4-core independent-fill candidates have throughput greater than
   their one-worker controls. If either does not, trigger the STOP condition
   below rather than describing the change as a multicore improvement.

After every Step 8 verification succeeds, update Plan 001 in
`plans/README.md` from `TODO` to `DONE`.

## Test plan

### New focused tests

Add all six scheduler tests named in Step 2 to the existing Linux-only test
module in `crates/runtime/src/actor_host.rs`. They must cover:

- independent-vset overlap;
- same-vset FIFO ordering;
- fair progress for a second key;
- multi-vset write-protect aggregation and failure;
- barrier prefix semantics;
- blocking-task panic/join failure;
- cleanup of idle keyed state.

Use bounded Tokio timeouts and synchronization primitives, never sleeps as the
only ordering proof. The independent-overlap test must require both jobs to
enter before either is released, making accidental serialization fail
deterministically.

### Existing tests to retain

- `crates/runtime/tests/loop_interference_linux.rs` remains the real-kernel
  concurrency profile and gains a functional maximum-concurrency assertion.
- `crates/hostmem/tests/uffd_linux.rs` continues to validate shared physical
  pages and write-protect ordering; it is exercised by the Linux kernel suite.
- Migration, replica, part-fetch, and workload targets in
  `scripts/run-linux-kernel-tests.sh` cover lost-reply and shutdown regressions.

### New ignored multicore profiles

- The ignored private tests in `crates/runtime/src/actor_host.rs` measure actual
  independent fills and a valid mixed ioctl workload through the production
  dispatcher.
- `scripts/run-fault-multicore-profile.sh` supplies same-core one-worker
  controls and pinned 1/2/4/8-core candidates. It is a measurement harness, not
  a portable correctness gate.

### Explicit non-tests

- Do not add an absolute latency threshold that depends on a particular host.
- Do not leave a test that passes when all fault work is serialized.
- Do not simulate success by mocking away the keyed dispatcher; the focused
  tests must exercise the same queue and completion logic production uses.

## Done criteria

All items must hold:

- [ ] `HostState`, `Cache`, `ProductionWorld`, and `shared_pages` remain local
      and non-`Send`.
- [ ] No vset, cache, fork-lineage, or memory-backing sharding was introduced.
- [ ] At most `FAULT_WORK_CONCURRENCY` blocking jobs run concurrently.
- [ ] At most one job per `VsetId` is active, with FIFO order.
- [ ] Distinct vsets demonstrably overlap in the focused test.
- [ ] Multi-vset write protection replies once, after all subgroups, and
      combines errors.
- [ ] A blocking-task panic cannot strand a direct caller, aggregate, or
      graceful shutdown barrier.
- [ ] Barrier semantics drain the accepted prefix without waiting for later
      requests.
- [ ] Idle vsets retain no worker task or keyed queue entry.
- [ ] The Linux profile reports maximum fault-work concurrency of at least 2
      under multi-vset load.
- [ ] The before/after release-profile table is included in the handoff.
- [ ] The multicore artifact contains same-core serial controls and parallel
      candidates for every supported core count through 8.
- [ ] Real independent-fill throughput improves at 2 and 4 cores versus the
      same-core one-worker controls, or the plan is reported blocked rather
      than claimed complete.
- [ ] The mixed workload completes with correct per-vset operation order and
      demonstrates more than one active worker in parallel candidates.
- [ ] The Linux kernel suite, runtime suite, workspace suite, and lint all pass.
- [ ] No source file outside the explicit scope changed.
- [ ] `plans/README.md` status is `DONE`.

## STOP conditions

Stop and report; do not improvise if any condition occurs:

- Either in-scope source file has semantically drifted from the excerpts above.
- The target production path now lets two `VsetHost` values reference the same
  mutable `HostRegion` backing, or the Firecracker `ShmemServer` path has been
  routed through `FaultWork`. `VsetId` is then no longer a sufficient ordering
  key; the design must be revised around an explicit backing-domain identity.
- Correct ordering requires holding a `RefCell` borrow or mutex guard across an
  `.await`.
- A solution appears to require changing `crates/core`, `crates/exec`,
  `crates/hostmem`, `crates/runtime/src/fc.rs`, or a public configuration/API
  type.
- Linux with working userfaultfd is unavailable for final verification.
- Any focused scheduler test remains flaky or relies on sleep timing rather
  than synchronization.
- The Linux kernel suite hangs or fails twice after one reasonable correction.
- Release-profile maximum concurrency stays at 1 under multi-vset load.
- The 2-core or 4-core real-fill candidate fails to outperform its same-core
  one-worker control in two clean, pinned runs. The serialized worker was then
  not the expected multicore limiter; retain the measurements and investigate
  before landing.
- The multicore profile can run only by exposing production-private scheduler
  or mapping types as stable public API. Keep the API private and report the
  harness-design blocker instead.
- The allowed CPU set cannot provide at least two CPUs. Record the environment
  limitation and rerun on a suitable Linux host; do not claim multicore
  validation.
- The new implementation worsens both probe throughput and p99 latency at 16
  or 48 noisy vsets on the same host/build profile. Report the measurements and
  investigate before landing.

## Maintenance notes

- Reviewers should scrutinize reply ownership. Every ingress request must have
  exactly one terminal path: direct reply, aggregate reply, barrier reply, or
  receiver closure that wakes the waiter with an error.
- Reviewers should also trace FIFO ordering for `Fill -> WriteProtect`,
  `WriteProtect -> Unprotect`, and `Evict -> Fill` on one vset.
- `FAULT_WORK_CONCURRENCY = 8` is an initial bounded production value, not a
  universal optimum. A future configuration change requires its own measured
  plan and must account for Tokio blocking-pool saturation.
- Preserve the same-core one-worker rows in performance artifacts. Comparing a
  parallel run on a larger CPU set only against a one-core run confounds CPU
  availability with dispatcher behavior.
- If production later unifies `VsetHost` regions across forks, add an explicit
  stable backing-domain identifier and key ordering by that domain (and page
  where safe), rather than by vset or pointer address.
- If the local actor loop remains saturated after this plan, profile poll cost
  and batching next. Do not jump directly to sharding the host cache; preserve
  the shared-residency invariant and quantify the remaining on-loop work first.
