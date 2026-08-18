# Plan 002: Profile realistic large-host hotspots before changing concurrency

> **Executor instructions**: Follow this plan in order. Preserve production
> semantics while adding observability and benchmark-only orchestration. Do not
> implement fault-worker concurrency, shard host state, or replace the local
> actor tree while executing this plan. Run every stated verification and retain
> the raw artifacts. If a STOP condition occurs, record it and stop the affected
> run rather than improvising around it. When the evidence package is complete,
> update this plan's row in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat 802717e..HEAD -- crates/runtime/src/actor_host.rs crates/runtime/src/loopstats.rs crates/runtime/src/metrics.rs crates/runtime/tests/loop_interference_linux.rs crates/runtime/tests/fc_perf_linux.rs crates/runtime/tests/support crates/workload/specs scripts TESTING.md`
> Compare any changed in-scope code with the assumptions below. A semantic
> mismatch in fault dispatch, loop statistics, shared mappings, or workload
> execution is a STOP condition until this plan is reconciled.

## Status

- **Status**: IN PROGRESS — retained Lima runtime/UFFD core scaling,
  measured-phase CPU, shared-base, 256-volume, hotspot, and instrumentation gates
  are complete; long-duration, real-VM, hardware-counter, NUMA, pressure, and
  lifecycle tiers remain
- **Priority**: P1
- **Effort**: L (instrumentation plus at least one full day of controlled host
  runs; longer if the workload must be stabilized)
- **Risk**: MEDIUM — instrumentation can perturb timing, and poorly controlled
  large-host runs can produce persuasive but false conclusions
- **Depends on**: none
- **Category**: performance investigation
- **Planned at**: commit `802717e`, 2026-08-17
- **Suggested branch**: `semistrict/large-host-profile`

## Objective

Determine what limits multicore throughput and tail latency under realistic
large-host operation before choosing an optimization. The result must separate,
as far as practical:

- current-thread actor-loop saturation and long polls;
- time waiting in the production `FaultWork` queue;
- blocking-pool dispatch delay and actual mapping/ioctl service time;
- local blob, peer, and object-store I/O;
- writeback, checkpoint, and eviction interference;
- kernel scheduling, lock, and userfaultfd costs;
- storage latency, last-level-cache pressure, NUMA effects, and memory bandwidth.

The benchmark must include forked guests that retain shared cache residency and
shared memory mappings. The investigation is complete only when it produces a
reproducible ranked hotspot report and a decision about the next implementation
plan. It is acceptable for the decision to be “do not parallelize fault work.”

Two independent parameters are mandatory throughout the harness and artifact
schema:

- `volume_count`: the total live volumes in the measured phase;
- `fork_provenance`: a reproducible lineage description containing topology,
  root count, maximum generation, branching parameters, and the seed or explicit
  parent relation used to derive every volume.

Do not infer provenance from creation order during analysis. Record the parent
of each fork so results can be regrouped by root, generation, sibling cohort,
and degree of shared ancestry.

## Current state and constraints

- `DESIGN.md:122-136` intentionally keeps protocol actors on one production
  `LocalSet`; network, process, timer, and blocking work are delegated to Tokio.
- `crates/core/src/engine/state.rs:24` stores shared host state as
  `Rc<RefCell<HostState>>`. This plan does not replace it with cross-thread
  locking.
- `crates/core/src/cache.rs` maintains host-wide residency, pressure, dirty and
  unstable pages, eviction ordering, and shared base residency. Do not shard it.
- `crates/hostmem/src/linux.rs` implements `Send + Sync` shared host regions and
  guest views backed by shared mappings. Benchmark their current behavior; do
  not replace it.
- `crates/runtime/src/fc.rs:444-518` serves fork sharing through one
  shared-memory file per sharing domain. Fork benchmarks must verify that this
  physical sharing remains active.
- `crates/core/src/engine/host.rs:471-498` admits up to 64 concurrent protocol
  fault actors, while `crates/runtime/src/actor_host.rs:310-318` currently awaits
  one blocking `FaultWork` item before receiving the next.
- `crates/runtime/tests/loop_interference_linux.rs` demonstrates severe
  interference at its largest noisy-volume scale, but it cannot identify the
  responsible resource. Treat it as a symptom and regression profile, not proof
  of a particular fix.
- `crates/runtime/src/loopstats.rs` records aggregate poll and world-operation
  time. Those aggregates do not currently distinguish queue delay from service
  time and must not be interpreted as an additive end-to-end breakdown.

## Non-goals

- No fault-worker parallelism, batching, or dedicated worker pool.
- No actor-state or cache sharding.
- No `Send` conversion of local protocol futures.
- No wire, storage, API, checkpoint, or memory-sharing format changes.
- No production scheduling-policy change hidden inside instrumentation.
- No machine-specific pass/fail performance threshold in the normal test suite.
- No cloud resource creation as part of the repository changes. If an executor
  separately receives authorization to create a benchmark host, every resource
  must be removed and verified absent after the runs.

## Evidence package

Every benchmark invocation writes to a new, caller-supplied artifact directory
and refuses to overwrite it. The package must contain:

1. `manifest.json`: revision, dirty-tree state, exact command, workload seed,
   runtime configuration, `volume_count`, serialized `fork_provenance`,
   repetition, start/end time, and artifact schema.
2. `machine.json`: CPU model and topology, NUMA layout, RAM, kernel, Rust
   toolchain, CPU governor, allowed CPUs, swap state, filesystem and mount
   options, block-device model, Firecracker version, and relevant limits.
3. `runtime.jsonl` or equivalent bounded snapshots: cumulative counts and
   histograms sampled at a documented cadence, with monotonic timestamps.
4. `system/`: `perf stat` output, per-thread CPU and context-switch samples,
   pressure-stall information, block-device statistics, and thermal/frequency
   evidence available on the host.
5. `correctness.json`: workload verification, fork-sharing checks, unexpected
   task exits, I/O errors, guest crashes, and cleanup results.
6. `summary.csv`: one row per scenario/scale/repetition containing throughput,
   p50/p90/p99/p99.9/max latency where sample size permits, stage delays, CPU,
   queue depth/age, I/O, correctness status, volume count, root count, maximum
   generation, and provenance topology.
7. `report.md`: plots or compact tables, ranked hotspots, limitations, and the
   decision gate outcome.

Raw samples are authoritative. Summaries must be regenerable without rerunning
the workload.

## Metrics model

### Fault and world-work measurements

Add low-overhead counts, gauges, maxima, and cumulative histograms for:

- faults observed and completed, split by source and operation where available;
- end-to-end fault latency already visible at the runtime boundary;
- `FaultWork` queue depth, maximum depth, enqueue-to-dequeue wait, and oldest
  queued age;
- blocking task submission-to-start delay;
- blocking service time, split among fill, unprotect, evict, and write-protect;
- failures, panics/join failures, cancellations, and shutdown/barrier drain;
- local blob, peer, store, shared-page, writeback, checkpoint, and eviction
  counts and latency at boundaries that already exist.

Do not report these distributions as components that sum exactly to end-to-end
latency: asynchronous stages overlap, and independent requests interleave. Use
them to identify queue growth and service demand. If exact correlation would
require an unbounded request table or a new protocol identifier, omit it and
document the limitation.

### Actor-loop measurements

Extend loop snapshots to expose:

- poll count, total poll duration, maximum poll duration, and a poll-duration
  histogram;
- time between successive loop polls and evidence of runtime starvation;
- world-operation count, total duration, maximum duration, and histogram;
- actor ingress/runnable depth and oldest age wherever the existing channel
  boundary exposes them without changing scheduling;
- snapshot deltas so a benchmark phase can exclude warmup without resetting
  live counters.

### System measurements

The runner captures external telemetry without making it a normal-test
dependency:

- `perf stat` for task-clock, CPUs utilized, context switches, migrations,
  cycles, instructions, branches, and cache events supported by the host;
- per-thread CPU so the local actor thread, Tokio workers, blocking workers,
  Firecracker processes, and storage helpers can be distinguished;
- run-queue, PSI, block-device latency/throughput, major/minor faults, and swap;
- NUMA placement and remote-memory counters when supported;
- memory-controller bandwidth counters when supported;
- sampled call stacks or flamegraphs for the hottest phase when permissions and
  installed tools allow them.

Unsupported optional counters must be recorded as unavailable, not treated as a
failed benchmark. `perf stat`, correctness, runtime metrics, CPU topology, and
block statistics are required.

## Workload matrix

Use a staged ramp. Start at 64 guests/volumes, then 256 and 1,024. Attempt 5,000
and 10,000 only when memory, file-descriptor, process, thermal, and storage
headroom remain safe. If full Firecracker process scale is not practical at a
tier, keep two results separate: a real-VM tier at the largest stable scale and
a runtime-only userfaultfd tier at the target logical-volume scale. Never present
the latter as real-VM capacity.

Each retained measurement has a warmup phase, a 15-minute minimum steady phase,
a drain/verification phase, and three repetitions with fixed seeds. Longer
30-minute steady phases are required for checkpoint, eviction, and mixed
lifecycle scenarios that cycle slowly.

At every practical scale, run these provenance shapes as a separate matrix axis:

- `independent`: every volume has its own root and no shared ancestry;
- `star`: one root with all other volumes forked directly from it;
- `balanced`: fixed branching factor and enough generations to reach the target
  count, recording the partially filled final generation;
- `chain`: one descendant per generation, capped at a safe supported lineage
  depth, with additional independent chains if required for the target count;
- `mixed`: a fixed-seed distribution of root sizes and generations intended to
  model a long-lived fleet.

The real-VM tier may omit a shape that exceeds a documented product or kernel
limit, but the omission must be explicit. Hold access volume and hot-set size
constant both per-volume and per-root in separate sub-runs: per-volume normalization
shows total load scaling, while per-root normalization isolates the effect of
increased sharing without silently multiplying work.

### Scenario A: steady shared fleet

- Many mostly idle guests/volumes, with a controlled hot subset.
- Zipf-like or trace-derived hot/cold page locality instead of uniform random
  access.
- Read/write proportions based on an explicit target deployment assumption.
- Purpose: measure actor overhead, baseline fault capacity, and idle-fleet
  interference as managed scale grows.

### Scenario B: noisy neighbors and paced probes

- A small saturated subset continuously faults while independent probe guests
  issue paced reads and writes.
- Sweep noisy counts and total managed counts independently.
- Preserve the existing `loop_interference_linux` profile as a short comparison,
  but run the realistic long profile separately.
- Purpose: determine whether probe p99 tracks actor occupancy, `FaultWork` queue
  age, storage, or scheduler contention.

### Scenario C: fork storm with shared base

- Create a base, fork many descendants, and access a large common read working
  set followed by small divergent writes per descendant.
- Include both simultaneous startup and rate-limited fork arrival.
- Verify shared-page identity/residency through existing runtime-visible
  evidence plus process-level proportional/resident memory measurements.
- Purpose: stress the exact case that prevents naive cache or memory-map
  sharding while measuring copy, mapping, NUMA, and userfaultfd costs.
- Run all supported provenance shapes at matching `volume_count`; this is the
  primary comparison for whether lineage shape changes CPU hotspots.

### Scenario D: cold restore and refault

- Restore guests from durable state with cold local cache and separately with a
  warm local/shared cache.
- Record time to first useful guest work, initial burst p99, and steady refault
  throughput.
- Purpose: distinguish source/store latency from final fill and kernel costs.

### Scenario E: writeback and checkpoint interference

- Fault hot guests while another subset dirties memory, synchronizes, and takes
  checkpoints at realistic intervals.
- Sweep checkpoint concurrency without changing correctness ordering.
- Purpose: expose write-protect, dirty capture, blob I/O, and actor-loop
  interference.

### Scenario F: memory pressure, eviction, and refault

- Grow the combined working set beyond the configured cache target while
  retaining safe host headroom and no swap.
- Cycle hot sets so evictions and refaults reach a repeatable steady state.
- Purpose: identify whether host-wide cache decisions, hole punching, storage,
  or memory bandwidth dominate under pressure.

### Scenario G: mixed lifecycle

- Use the workload runner to combine create, read, write, sync, checkpoint,
  fork, restore, migrate where configured, crash, and verify operations.
- Publish the operation distribution and seeds.
- Purpose: validate that conclusions from focused scenarios survive realistic
  control-plane and data-plane overlap.

## CPU scaling experiment

After establishing the stable whole-host baseline, rerun Scenarios B, C, and E
with the runtime restricted to 1, 2, 4, 8, 16, and all available physical cores,
skipping counts above host capacity. Keep guest/process CPU placement and actor
thread placement documented and consistent. Run two placement variants:

1. production-like scheduler placement;
2. diagnostic placement with the local actor thread isolated from guest vCPUs
   and blocking workers.

The purpose is diagnostic, not to claim pinning as a fix. Report throughput
scaling efficiency, p99 change, actor-thread utilization, blocking-worker
utilization, queue growth, LLC misses, and memory bandwidth. A flat curve with a
saturated actor thread suggests actor serialization; rising queue delay with
idle worker/core capacity suggests dispatch serialization; saturated bandwidth
or storage with low queue wait argues against adding CPU concurrency.

For each CPU-count result, compare hotspot stacks and counter shares across both
`volume_count` and `fork_provenance`. Report stacks whose share changes by at least
five percentage points, newly appearing stacks, and stacks that stay constant
while throughput changes. This separates costs driven by the number of actors
from costs driven by shared ancestry, cache reuse, mapping reuse, or divergent
writes.

## Implementation steps

### Step 1: Add snapshot-safe runtime metrics

**Files**:

- `crates/runtime/src/metrics.rs`
- `crates/runtime/src/loopstats.rs`
- focused unit tests in the same modules or existing test modules

**Actions**:

1. Add cumulative histogram/max/gauge primitives needed above, reusing existing
   atomic metric style and fixed buckets.
2. Add subtraction/delta helpers that tolerate concurrent snapshots and counter
   wrap rules explicitly.
3. Extend loop snapshots with poll and world-operation distributions and maxima.
4. Ensure steady-state recording performs no heap allocation, blocking, locks,
   or per-event logging.
5. Add intended-behavior tests for bucket boundaries, maxima, gauges, and phase
   deltas.

**Verification**:

- `cargo test -p blockd-runtime --lib metrics`
- `cargo test -p blockd-runtime --lib loopstats`

### Step 2: Separate fault queue delay from blocking service

**Files**:

- `crates/runtime/src/actor_host.rs`
- focused Linux-gated tests in the existing module

**Actions**:

1. Timestamp `FaultWork` enqueue without changing item ordering or ownership.
2. Record queue depth/maximum/oldest-age data at enqueue and dequeue.
3. Record blocking submission-to-start delay and service duration per operation.
4. Record completion, failure, cancellation, and shutdown drain counts.
5. Expose read-only snapshots to the benchmark harness without adding a public
   wire/API format.
6. Use controlled barriers in tests to prove queue wait is excluded from service
   duration and that the existing serial execution order is unchanged.

**Verification**:

- On Linux: `cargo test -p blockd-runtime --lib actor_host::tests`

### Step 3: Add profiling boundaries for other plausible bottlenecks

**Files**:

- `crates/runtime/src/actor_host.rs`
- existing runtime modules that own local blob, peer/store, checkpoint,
  writeback, and eviction boundaries
- focused unit tests adjacent to changed code

**Actions**:

1. Inventory existing latency metrics before adding new ones; avoid duplicate
   names measuring different boundaries.
2. Add only the counts and phase timers required to distinguish the sources in
   the Metrics model.
3. Label operations with fixed enums, not dynamic strings or unbounded maps.
4. Document each timer's start/end boundary and whether it includes downstream
   queueing.

**Verification**:

- Run focused unit tests for every changed module.
- `cargo clippy --workspace --all-targets -- -D warnings`

### Step 4: Build the large-host profile harness

**Files**:

- new `crates/runtime/tests/large_host_profile_linux.rs`
- new `crates/runtime/tests/large_host_fc_profile_linux.rs`
- `crates/runtime/tests/support/workload.rs`
- new workload specs under `crates/workload/specs/` with descriptive filenames
- new `scripts/run-large-host-profile.sh`
- `TESTING.md`

**Actions**:

1. Reuse the real userfaultfd/disk setup from
   `loop_interference_linux.rs`, Firecracker setup from `fc_perf_linux.rs`, and
   the shared workload runner; do not create a second runtime implementation.
   Keep the actor-runtime tier and Firecracker shared-mapping tier in distinct
   test binaries and artifacts so their CPU profiles cannot be conflated.
2. Represent scenarios and scale tiers in explicit configuration recorded in
   the manifest.
3. Require `volume_count` and a validated `fork_provenance` configuration. Build
   and persist the explicit volume-to-parent relation before the measured phase;
   reject cycles, missing parents, duplicate IDs, count mismatches, and lineage
   deeper than configured safety limits.
4. Add fixed seeds, warmup/steady/drain phases, correctness verification, and
   bounded metric sampling.
5. Make the runner validate prerequisites and refuse unsafe or ambiguous runs:
   Linux, userfaultfd, KVM for VM tiers, dedicated scratch on supported XFS,
   swap disabled, sufficient file descriptors, writable artifact path, and a
   new empty artifact directory.
6. Capture child PIDs/TIDs so system telemetry can distinguish actor, worker,
   blocking, guest, and helper CPU use.
7. Always terminate guests/helpers and verify mounts, processes, and temporary
   resources are gone, including on failure.
8. Document exact commands, expected duration, artifact layout, and how to run
   a short smoke profile versus the retained full matrix.

**Verification**:

- On Linux, short smoke tier:
  `cargo test -p blockd-runtime --test large_host_profile_linux -- --ignored --nocapture`
- Validate that every declared artifact exists and that summary regeneration
  from raw data is deterministic.

### Step 5: Measure instrumentation overhead

**Actions**:

1. Run an identical fixed-seed short profile with new detailed histograms and
   fault-work accounting disabled and enabled on the same otherwise-idle host.
   The disabled path preserves the pre-existing aggregate loop counters while
   bypassing the new loop histograms, fault-work queue lock, and fault phase
   timers. Treat the always-on maximum added to existing latency histograms as
   a residual limitation of this control.
2. Alternate disabled/enabled order for at least five repetitions of each mode,
   then repeat with the opposite starting order if run-order drift is visible.
3. Compare throughput, p50/p99 bounds, task-clock, cycles, instructions, and
   context switches. Retain the raw observations, not only their means.
4. If median throughput or latency perturbation exceeds 5%, reduce the
   instrumentation cost or use a benchmark-only opt-in path and repeat. Do not
   continue to the full matrix with intrusive measurements.

### Step 6: Run the staged workload and CPU matrix

**Actions**:

1. Record the host manifest before workload setup.
2. Run the smoke tier, inspect correctness and pressure limits, then ramp one
   scale at a time.
3. Retain all three repetitions; do not discard outliers without a documented
   external cause.
4. Capture sampled stacks during the hottest stable interval of each focused
   scenario when supported.
5. Stop ramping a scenario at the first safety threshold while preserving the
   last valid tier.
6. Repeat the diagnostic CPU-count and placement matrix for B, C, and E.
7. Verify cleanup after each run and once more at the end.

### Step 7: Analyze and choose the next optimization

**Actions**:

1. Generate per-scenario scaling curves and latency/queue/CPU tables from raw
   artifacts.
2. Produce two hotspot comparisons: scaling `volume_count` at fixed provenance,
   and changing provenance at fixed `volume_count` and normalized work. Attribute
   differences to root count, generation, sibling cohort, and shared-ancestry
   depth only when the recorded parent graph supports that attribution.
3. Rank hotspots by their association with target-load p99 and by consumed CPU,
   blocked time, queue age, bandwidth, or I/O demand.
4. Require the observed relationship to recur in at least two of three retained
   repetitions and across at least two relevant scenarios before calling it the
   primary hotspot.
5. Record counterevidence and measurement limitations.
6. Apply the decision gates below and write exactly one recommended next plan,
   or recommend no architecture change if evidence is inconclusive.

## Decision gates

An implementation plan requires a reproducible hotspot that accounts for at
least 20% of target-load CPU/blocked time or whose queue growth strongly tracks
at least 20% of end-to-end p99. Treat 20% as a planning threshold, not a product
performance promise.

### Write a fault-work concurrency plan only if

- enqueue-to-dequeue or blocking submission delay grows materially with load;
- blocking service remains parallelizable by operation/volume semantics;
- actor-thread utilization, source I/O, storage, and memory bandwidth do not
  already explain the plateau;
- multiple physical cores remain available at the target load; and
- the fork-sharing scenario exhibits the same bottleneck without requiring
  cache or mapping sharding.

The new plan must name which CPU work would overlap and preserve per-page/volume
ordering, multi-volume write-protect completion, barriers, cancellation semantics,
and shared mappings.

### Prefer actor-loop work if

- one actor thread is saturated, polls are long, or runnable/ingress age grows
  while the fault-work queue is short;
- profiles identify specific polls, batching, scans, or decision logic; and
- isolating the actor thread materially changes p99.

Do not infer that making all actors `Send` is necessary. First plan the smallest
measured source of long polls or excessive work per event.

### Prefer dispatch/batching work if

- `spawn_blocking` submission delay or per-job overhead is large relative to
  syscall/copy service, but the actor loop and external resources have headroom.

### Prefer storage, NUMA, or memory-layout work if

- device latency/queueing, remote NUMA traffic, LLC misses, or bandwidth
  saturation tracks the plateau while software queues remain controlled.

Adding more concurrent workers in this case is explicitly rejected unless a
separate experiment shows it improves useful throughput without worsening p99.

### Report inconclusive if

- retained repetitions disagree, instrumentation perturbation remains too high,
  the workload cannot reproduce target behavior, or no source crosses the
  planning threshold.

## Safety thresholds and STOP conditions

Stop the affected run and preserve artifacts when any of the following occurs:

- correctness verification, checkpoint restore, shared-page verification, or
  cleanup fails;
- swap becomes active, the OOM killer fires, PSI full stalls remain unsafe, or
  free-memory headroom falls below the benchmark's predeclared limit;
- sustained thermal throttling, CPU-frequency collapse, another tenant, or host
  maintenance invalidates isolation;
- filesystem/device prerequisites differ from the recorded scenario;
- instrumentation overhead exceeds 5% after one reduction attempt;
- a proposed metric needs unbounded allocation, per-event logs, a global
  request table, or a protocol/storage/API identifier change;
- result coefficient of variation exceeds 15% after controlled repetitions and
  no external cause can be isolated;
- the next scale tier would exceed process, descriptor, KVM, disk, or memory
  safety headroom;
- benchmark orchestration would delete or overwrite a caller-provided path;
- separately authorized cloud resources cannot be verified as removed.

STOP means do not claim a performance conclusion from that run. It does not
erase valid lower-scale evidence.

## Final verification

After repository changes and before large-host conclusions:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --doc --workspace
scripts/run-linux-kernel-tests.sh
```

Run the full profile through `scripts/run-large-host-profile.sh` using a new
artifact directory. Confirm that correctness is green, cleanup is complete, raw
artifacts regenerate the published summary, and the report applies one of the
decision gates without assuming the result.

## Completion criteria

- Production behavior is unchanged except for bounded observability.
- Instrumentation overhead is measured and at or below 5%.
- Scenarios A through G have at least one valid realistic-host tier; B, C, and E
  include the CPU scaling matrix.
- Results are parameterized by both volume count and recorded fork provenance, and
  the report compares CPU hotspots along each axis while holding the other fixed.
- The largest attempted tier and the reason for stopping are reported honestly.
- Fork/cache/mapping sharing is verified rather than assumed.
- Raw data, manifest, correctness, summaries, and report are complete and
  reproducible.
- The report identifies a reproducible primary hotspot and a narrow next plan,
  or explicitly concludes that evidence is insufficient.
- No benchmark guest, helper, mount, temporary resource, or separately
  authorized cloud resource remains after completion.
