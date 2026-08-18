# Plan 002 Lima baseline report

## Decision

**Do not shard host state, the shared cache, or shared mappings, and do not make
fault-worker parallelism the first optimization.** The retained Lima baseline
does show useful overlap between the actor, fault worker, and guest/runtime
threads, but the curve stops improving after four cores while the primary actor
remains about 88% occupied. The largest growing user-space hotspot is a 1 kHz
observability publication that scans per-vset segment state twice.

The next focused implementation/evaluation should make observability snapshots
incremental or substantially less frequent, and remove the duplicate segment
scan. It should then rerun this exact baseline before considering more fault
workers. Scheduler wakeups and the serial fault queue remain secondary
candidates: at eight cores, mean fill queue wait is still about 94 us versus
about 19 us of fill service, but removing that wait cannot bypass the saturated
actor path.

This report completes the reproducible runtime/UFFD Lima baseline requested
before architecture changes. It does not complete every long-duration scenario
in Plan 002: Firecracker, hardware PMU, NUMA, memory-bandwidth, eviction,
checkpoint-interference, mixed-lifecycle, and 15-minute soak tiers remain future
work on a suitable host.

## Host and retained workload

- Lima VM: 8 aarch64 vCPUs, 15 GiB usable RAM, one NUMA node, Linux
  7.0.0-29-generic.
- Swap disabled. Scratch was a fresh 32 GiB XFS loop filesystem mounted with
  `noatime,inode64,logbufs=8,logbsize=32k,noquota`.
- Runtime/UFFD tier only. Firecracker artifacts and `/dev/kvm` were not
  available.
- Hardware cycles, instructions, branch, cache, LLC, memory-controller, and
  NUMA PMU events were not exposed by Lima. Software task-clock, scheduler,
  fault, pressure, block, and stack sampling were available.
- 64 total star-lineage vsets: one idle seed root and 63 active descendants.
- The root owns a deterministic 16,384-page (64 MiB) hot set that is
  checkpointed and retained before forking. Descendants first-touch the same
  retained pages and diverge on writes. The measured root is disabled so its
  already-mapped pages cannot dominate aggregate throughput.
- Cold first-touch, uniform random 64 MiB virtual hot range per descendant,
  80% reads / 20% writes, no pacing, fixed seed, cache headroom large enough to
  avoid intentional eviction.
- Three counter-only 30-second repetitions at 1, 2, 4, and 8 allowed cores.
  Matching stack samples used 20-second phases. A separate 256-vset control used
  one idle root plus 255 active descendants on all eight cores.

The shared-base smoke gate produced 1,272 shared fills and about 21,600
write-protect work items in five seconds with zero mismatches. Across the final
retained, CPU-confirmation, hotspot, instrumentation, and scale-control runs,
every measured phase completed with zero data errors and no runtime incidents.

## Multicore scaling

The min/median/max values below are the three retained observations. Efficiency
is speedup divided by allowed cores.

| Allowed cores | Throughput min | Throughput median | Throughput max | Speedup | Efficiency | Median p50 bound | Median p99 bound |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,126 | 1,238 ops/s | 1,272 | 1.00x | 100.0% | 50 ms | 250 ms |
| 2 | 1,854 | 1,870 ops/s | 1,999 | 1.51x | 75.5% | 50 ms | 250 ms |
| 4 | 2,035 | 2,268 ops/s | 2,339 | 1.83x | 45.8% | 25 ms | 100 ms |
| 8 | 2,063 | 2,192 ops/s | 2,228 | 1.77x | 22.1% | 25 ms | 100 ms |

Two cores materially help and four cores improve throughput and p99. Eight do
not move the throughput needle: median throughput is 3.4% below four cores and
the retained ranges overlap.

## Measured-phase CPU and fault pipeline

Outer `perf stat` includes setup and teardown, so phase CPU comes from
`/proc/self/task/*/schedstat` snapshots taken immediately before and after the
workers. These four confirmation cells reproduce the retained curve.

| Allowed cores | Throughput | Average CPU cores used | Allowed CPU utilization | Total run-queue wait | Schedules |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,236 ops/s | 0.97 | 97% | 49.0 s | 1.33 M |
| 2 | 2,050 ops/s | 1.53 | 76% | 73.9 s | 1.33 M |
| 4 | 2,348 ops/s | 1.89 | 47% | 38.2 s | 1.25 M |
| 8 | 2,160 ops/s | 2.26 | 28% | 20.3 s | 1.09 M |

The process uses only 2.26 cores in the eight-core envelope. More cores reduce
run-queue and fault-queue delay, but there is not enough runnable independent
CPU work to occupy the machine.

| Allowed cores | Primary actor occupancy | Max fault queue | Fill count | Mean fill queue wait | Mean fill service | Shared fills | Write-protect work |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 88.3% | 41 | 37,209 | 417 us | 50 us | 30,238 | 3,138 |
| 2 | 89.5% | 52 | 61,600 | 318 us | 43 us | 50,067 | 3,907 |
| 4 | 88.4% | 45 | 70,580 | 147 us | 20 us | 57,256 | 3,319 |
| 8 | 87.6% | 39 | 64,945 | 94 us | 19 us | 52,646 | 3,172 |

The actor stays nearly saturated at every core count. Fill service and queue
wait improve through eight cores, yet throughput peaks at four, so the one
fault worker is not the only ceiling.

## What CPU work overlaps

At high core count, these paths run concurrently:

- the primary current-thread actor performs fault protocol decisions, shared
  cache/state updates, background capture decisions, and observability scans;
- one dedicated active fault worker performs host-memory mapping, copy,
  write-protect, and userfaultfd ioctl service;
- Tokio and blocking workers execute guest touches, local/store I/O, peer work,
  timers, and wakeups;
- the two passive runtime hosts have their own mostly idle actor and fault
  threads.

Endpoint process samples show the primary actor near 87% CPU in the eight-core
profile, the active fault worker around 6-7%, and the two passive actors around
5-6% each. The remaining CPU is distributed across guest/runtime workers and
kernel scheduling. By contrast, work inside the primary actor cannot overlap
with other actor work, and fill/ioctl service for different faults cannot
overlap inside the single fault worker.

## Ranked hotspots

Function-level `perf` samples were collected at 99 Hz. The Lima PMU does not
expose hardware counters, and `arch_counter_get_cntpct` is partly a
virtualization/sampling clock cost, so runtime counters and scaling results are
authoritative.

| Symbol | 1 core / 64 vsets | 8 cores / 64 vsets | 8 cores / 256 vsets |
|---|---:|---:|---:|
| `try_to_wake_up` | 9.53% | 18.46% | 15.65% |
| `finish_task_switch` | 17.22% | 15.29% | 8.96% |
| `VsetState::live_segment_bytes` | 6.99% | 7.66% | 11.54% |
| `HostState::seg_space` | 4.60% | 5.00% | 10.80% |
| `HostState::stats` | 0.94% | 1.68% | 2.69% |
| `eventfd_write` | 4.79% | 2.52% | 2.05% |
| `__wake_userfault` | 0.84% | 1.68% | 1.56% |
| `arch_counter_get_cntpct` | 30.13% | 23.12% | 15.01% |

The runtime publishes observability at
`writeback_interval.clamp(1, 1_000_000)` nanoseconds, at most a 1 ms period.
Every publication calls `HostState::stats`; that calls
`VsetState::live_segment_bytes` once per vset and then calls `seg_space`, which
scans the same per-vset segment locations again. At 256 vsets, those three
symbols rise from 14.34% to 25.03% of sampled CPU. That fleet-size response is
much stronger evidence than the flat eight-core curve alone.

The 256-vset retained control reached a median 3,712 ops/s, only 1.69x the
64-vset eight-core result for 4x as many active descendants. Median p50 worsened
from 25 ms to 100 ms and p99 from 100 ms to 250 ms; mean fill queue wait roughly
doubled from 94 us to 187 us, while the actor remained about 88% occupied and
the process used only about 2.0 CPU cores.

## Instrumentation overhead

Two ten-run counter-only series alternated detailed runtime metrics off/on, with
opposite starting order:

| Ordering | Metrics-off mean | Metrics-on mean | Change |
|---|---:|---:|---:|
| off first | 2,679 ops/s | 2,616 ops/s | -2.36% |
| on first | 2,695 ops/s | 2,673 ops/s | -0.83% |
| combined | 2,687 ops/s | 2,644 ops/s | -1.59% |

Individual pairs were noisy and the ten-second phases drifted, but both orderings
remain under the 5% gate and have the same sign. Cycles and instructions per
operation are unavailable in Lima.

## Discarded envelope and limitations

A 256 MiB seeded base was attempted first. Synthetic sequential first-touch
combined with the test runtime's 5 ms writeback period generated 4,000-16,000
blob writes during setup. One 4-core repetition rejected base retention; an
exact diagnostic rerun passed. Disk usage was 3%, memory pressure was absent,
and the failure occurred before measurement. That fragmented envelope is
discarded rather than mixed into the retained matrix. The stable 64 MiB base
still verifies real shared fills and divergent write-protect work.

Other limits:

- 30-second phases establish a baseline but are not the 15-minute soak required
  for slow lifecycle scenarios in the full plan.
- Uniform random access is a focused fault-pressure workload, not a production
  trace or Zipf distribution.
- The runtime/UFFD tier is not a real-VM capacity claim.
- No hardware cache, bandwidth, cycles, instructions, or NUMA counter claims
  can be made from this VM.
- The 64-vset independent-root control was noisy and ordered; it is retained as
  exploratory evidence only, not a controlled provenance conclusion.

## Reproduction and artifacts

The authoritative ignored artifact package is
`artifacts/lima-shared-base-baseline-2026-08-17-structured/`. It contains the
three-repetition core matrix, measured-phase CPU confirmation, 64- and 256-vset
hotspot profiles, both instrumentation orderings, the shared-base smoke gate,
machine metadata, raw runtime snapshots, `perf stat`, stack samples, pressure,
disk, NUMA, and memory observations.

Core matrix command (inside the prepared Lima VM):

```sh
BLOCKD_PROFILE_PREFAULT_HOTSET=0 \
BLOCKD_PROFILE_SEED_SHARED_HOTSET=1 \
BLOCKD_PROFILE_MEASURE_ROOTS=0 \
BLOCKD_PROFILE_PAGES_PER_VOLUME=16384 \
BLOCKD_PROFILE_HOT_PAGES=16384 \
BLOCKD_PROFILE_CACHE_PAGES_PER_VSET=16384 \
BLOCKD_PROFILE_COUNTS=64 \
BLOCKD_PROFILE_PROVENANCES=star \
BLOCKD_PROFILE_REPETITIONS=3 \
BLOCKD_PROFILE_DURATION_SECS=30 \
BLOCKD_PROFILE_CPU_LISTS='0;0-1;0-3;0-7' \
BLOCKD_PROFILE_STACKS_FIRST=0 \
scripts/run-large-host-matrix.sh ARTIFACT_ROOT runtime
```

The 256-vset control changes `BLOCKD_PROFILE_COUNTS=256` and
`BLOCKD_PROFILE_CPU_LISTS=0-7`. Endpoint stack profiles use
`BLOCKD_PROFILE_STACKS=1` with `scripts/run-large-host-profile.sh`.
