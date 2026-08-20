# Testing

Run the portable suite with nextest:

```sh
cargo nextest run
cargo test --doc --workspace
```

If nextest is unavailable, use `cargo test --workspace`.

The required merge checks are the macOS portable workflow's lint, workspace
tests, workspace documentation tests, deterministic dynamic-membership sweep,
and dependency-policy job, plus the Linux-kernel and simulation jobs. The
Linux-kernel job also runs the runtime library, production daemon, and HostId
lifecycle regressions that are compiled out on macOS. Dependency exceptions
follow `DEPENDENCY_POLICY.md`.

## Linux integration

The Linux suite covers userfaultfd, durable replica recovery, peer transport,
migration, part fetching, and event-loop interference:

```sh
scripts/run-linux-kernel-tests.sh artifacts/linux-kernel
```

Firecracker tests require patched binaries and KVM, so they are not included
in this script.

## Simulation

Simulation scenarios run on Turmoil's deterministic, paused Tokio runtime.
The scenario seed controls both Turmoil and the domain fault models.

Run one deterministic simulation shard with:

```sh
scripts/run-sim-ensemble.sh migration 0 250 artifacts/simulation/local
```

The checked scenarios are in `crates/sim/scenarios`. Failed runs write their
seed, resolved configuration, trace hashes, and replay command to the artifact
directory.

Run the same scenario with both supported test page sizes when changing page
layout or accounting:

```sh
BLOCKD_TEST_PAGE_SIZE=4096 scripts/run-sim-ensemble.sh chaos 0 1000 artifacts/sim-4k
BLOCKD_TEST_PAGE_SIZE=16384 scripts/run-sim-ensemble.sh chaos 0 1000 artifacts/sim-16k
```

## Performance profiles

Profiles report measurements without machine-specific pass thresholds. Run
them in release mode with output enabled:

```sh
cargo test --release -p blockd-core --test perf_crc -- --ignored --nocapture
cargo test --release -p blockd-core --test perf_replication -- --ignored --nocapture
cargo test --release -p blockd-runtime --test perf_actor -- --nocapture
cargo test --release -p blockd-runtime --test loop_interference_linux -- --nocapture
cargo test --release -p blockd-runtime --test fc_perf_linux -- --ignored --nocapture
```

Compare performance results only on the same machine, build profile, CPU
governor, and background load.

### Large-host lineage profile

The ignored large-host runtime profile is parameterized independently by live
volume count and fork provenance. It creates the recorded lineage through real
checkpoint, retained-base, and fork operations, then captures actor-loop,
fault-work queue/dispatch/service, per-volume fault, local I/O, and `perf stat`
measurements during a fixed-duration read/write phase.

Run one retained configuration on an otherwise-idle Linux host with swap off
and dedicated XFS scratch at `/var/tmp/blockd-scratch`:

```sh
scripts/run-large-host-profile.sh \
  artifacts/large-host/star-256-r1 \
  256 \
  star \
  900 \
  runtime
```

The provenance argument is one of:

- `independent`
- `star`
- `balanced:N`, where `N` is the branching factor
- `chain:N`, where `N` is the maximum generation before a new root
- `mixed:SEED:ROOT_PPM:MAX_DEPTH`

Use a new artifact directory for every repetition. Compare volume scaling while
holding provenance fixed, then compare provenance at the same volume count and
seed. Set `BLOCKD_PROFILE_CPU_LIST=0-7` to restrict the measured process tree
with `taskset` for the diagnostic CPU-count sweep; record topology-aware CPU
lists rather than assuming adjacent logical CPUs are physical cores. The runtime
tier exercises the protocol actor, host-wide base cache,
userfaultfd, and host mappings.

The default prefaults the configured hot set before measurement. For a cold
first-touch/refault phase, set `BLOCKD_PROFILE_PREFAULT_HOTSET=0`, choose
`BLOCKD_PROFILE_PAGES_PER_VOLUME` and `BLOCKD_PROFILE_HOT_PAGES` large enough
that the phase cannot warm the entire set, and record
`BLOCKD_PROFILE_CACHE_PAGES_PER_VOLUME`. Do not undersize the cache merely to
force churn unless pressure/eviction progress is itself the scenario under
test. Set `BLOCKD_PROFILE_TIMEOUT_SECS` to a positive outer guard for smoke and
diagnostic runs; a timeout invalidates the cell and must not be treated as a
latency observation.

For a fork storm that exercises actual retained-base sharing rather than just
recording fork ancestry, set `BLOCKD_PROFILE_SEED_SHARED_HOTSET=1`. Each root
then receives deterministic contents across the whole hot set before its first
checkpoint and retained-base creation. Set `BLOCKD_PROFILE_MEASURE_ROOTS=0` to
keep those seed roots idle during measurement so a directly mapped root cannot
dominate aggregate descendant throughput. This combination requires a
provenance topology with at least one fork and records `active_volume_count`, both
switches, and the total live-volume count in the manifest. Shared-fill and
write-protect fault deltas in `runtime.json` are the correctness evidence that
the measured descendants reused and then diverged from the retained base.

On Linux, `summary.json` also records `process_cpu` from per-thread
`/proc/self/task/*/schedstat` snapshots immediately around the measured phase.
`average_cores` is process CPU-running nanoseconds divided by measured elapsed
nanoseconds; `runqueue_wait_ns` and `schedules` are phase deltas. Prefer these
fields over outer `perf stat` task-clock when setup or teardown is substantial,
because the external counters deliberately cover the complete process
lifecycle.

The runtime profile keeps one persistent guest thread and one guest-operation
lease per measured volume. This matches a VM vCPU touching its mapping directly;
it avoids measuring a fresh executor handoff on every access. Latency sampling
defaults to one operation in 1,024 and is recorded as
`latency_sample_rate` in the manifest. Override it with
`BLOCKD_PROFILE_LATENCY_SAMPLE_RATE`, but do not compare runs with different
sampling rates.

Set `BLOCKD_PROFILE_RUNTIME_SHARDS=0` to assign each independent root lineage
to one of the available CPU-count lanes, or set an explicit positive lane
count. Every descendant remains on its root's lane, so retained-base sharing
and copy-on-write stay inside one runtime. Multiple lanes are a diagnostic of
actor-level partitioning, not the production default; use one lane for runtime
acceptance results unless the deployed runtime uses the same topology.

`BLOCKD_PROFILE_REFAULT_EACH_ACCESS=1` evicts the guest PTE after every
operation and forces a minor userfaultfd round trip on the next access. This is
a kernel wakeup and fault-progress stress test. It is intentionally not a
linear-scaling acceptance workload: normal resident guest accesses do not
traverse the runtime, and Linux userfaultfd wake/schedule costs can dominate
this mode even when volumes and userfaultfd contexts are isolated.

For independent-volume multicore acceptance, use a fixed fleet, prefault the hot
set, disable forced refaults, and retain a realistic read/write mix. Run at
least three repetitions on topology-aware 1/2/4/8-core CPU lists, report the
median throughput speedup and efficiency, and keep the forced-refault matrix
beside it as a separate diagnostic. The cache-resident phase verifies the
common path; the fork-sharing smoke described above verifies that cold shared
fills, write protection, and divergent writes remain correct.

After collecting at least three comparable repetitions per core count, enforce
the 80% median parallel-efficiency gate and the 20% p99-regression guard with:

```sh
scripts/verify-independent-volume-scaling.sh ARTIFACT_ROOT
```

The verifier rejects mixed workload manifests, non-independent provenance,
multiple diagnostic runtime lanes, forced-refault runs, data errors, or missing
1/2/4/8-core repetitions before calculating the result.

Set `BLOCKD_PROFILE_PERF_EVENTS` to a comma-separated event list when the host
exposes model-specific memory-controller or NUMA events. The runner records the
resolved event list and samples node `numastat`, pressure, memory, and disk
counters. Unsupported model-specific counters must be omitted explicitly; do
not silently substitute cache misses for memory bandwidth.

Run the separate Firecracker tier with patched binaries in `BLOCKD_FC_DIR` and
read/write access to `/dev/kvm`:

```sh
scripts/run-large-host-profile.sh \
  artifacts/large-host/fc-star-64-r1 \
  64 \
  star \
  900 \
  firecracker
```

In that tier, each recorded parent with children owns one snapshot fill server;
its children map the same shmem file privately and diverge through copy-on-write.
Keep its results separate from the runtime tier, then compare how volume count and
the same provenance shapes change CPU stacks, fill deduplication, Rss/Pss,
refault latency, and throughput.

Run a controlled matrix by explicitly choosing counts and provenance shapes:

```sh
BLOCKD_PROFILE_COUNTS=64,256,1024 \
BLOCKD_PROFILE_PROVENANCES=independent,star,balanced:4,chain:8,mixed:17:100000:8 \
BLOCKD_PROFILE_REPETITIONS=3 \
BLOCKD_PROFILE_CPU_LISTS='all;0-7;0-15' \
scripts/run-large-host-matrix.sh artifacts/large-host/runtime-matrix runtime
```

CPU-list values are separated with semicolons because an individual `taskset`
list may contain commas. The first repetition of each cell captures sampled
stacks by default; set `BLOCKD_PROFILE_STACKS_FIRST=0` for counter-only runs.
Compilation completes before counters and stacks start. Do not compare a
stack-sampled repetition directly with a counter-only repetition when estimating
instrumentation overhead.

Measure the added detailed histograms and fault-work queue accounting before
using their results to make an optimization decision:

```sh
scripts/measure-large-host-instrumentation.sh \
  artifacts/large-host/instrumentation-star-256 \
  256 \
  star \
  300
```

This runs ten counter-only repetitions, alternating detailed metrics off and on
with the same release build and workload. `results.tsv` retains every
observation and `effect.tsv` reports mean throughput, cycles/operation, and
instructions/operation changes. Use
`BLOCKD_PROFILE_INSTRUMENTATION_REPETITIONS` to select a larger even count (at
least ten). Set `BLOCKD_PROFILE_DETAILED_FIRST=1` for a second matrix with the
opposite starting order if drift is visible. A retained profile run defaults to
detailed metrics enabled; set `BLOCKD_PROFILE_DETAILED_METRICS=0` only for this
overhead control.

After the matrix completes, generate one throughput/latency table and one
normalized sampled-stack table:

```sh
scripts/summarize-large-host-matrix.sh artifacts/large-host/runtime-matrix
```

Compare `summary.tsv` at fixed provenance while increasing volume count, then at
fixed volume count while changing provenance. Use `hotspots.tsv` to identify CPU
symbols whose overhead share appears, disappears, or shifts materially along
either axis. Confirm candidate explanations against fault-work queue delay,
blocking dispatch/service, poll-only actor occupancy, storage counters, Pss/Rss,
and memory-bandwidth/NUMA counters available on the host; sampled-stack share
alone is not a causal attribution.

Compare any two stack-sampled cells directly (for example, fixed provenance at
64 versus 256 volumes, or fixed volume count with independent versus star roots):

```sh
scripts/compare-large-host-hotspots.sh \
  artifacts/large-host/runtime-matrix/v64-star-cpuall-r1 \
  artifacts/large-host/runtime-matrix/v256-star-cpuall-r1
```

The comparison is sorted by absolute change in percentage points and prints the
recorded count, topology, roots, generations, and CPU placement for both runs.
