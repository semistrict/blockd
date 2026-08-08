# Testing

The normal suite is:

```sh
cargo test --workspace
```

Linux-only tests exercise userfaultfd, real disk behavior, peer transport,
loop interference, migration, and Firecracker. They compile to empty test
binaries on other operating systems. The live GCS round trip is ignored by
default because it requires a configured bucket; the in-process GCS contract
tests remain in the normal suite.

## Performance profiles

Performance profiles print measurements rather than pinning machine-specific
latency thresholds. Run them in release mode with output enabled:

```sh
# Hardware/SIMD CRC-32C versus the previous bytewise table loop.
cargo test --release -p blockd-core --test perf_crc -- --ignored --nocapture

# One MiB database request: total work, slice count, and worst loop step.
cargo test --release -p blockd-core profile_one_mib_database_write_slices -- --ignored --nocapture

# 300k-page capture: total throughput and the worst bounded continuation.
cargo test --release -p blockd-runtime --test perf_decider profile_huge_vset_capture_stall -- --nocapture

# 300k-page migration hydration: one bounded map slice and fetch batch.
cargo test --release -p blockd-core profile_300k_page_hydration_tick -- --ignored --nocapture

# Replica artifact preparation: old inline-equivalent time versus bounded
# peer-I/O queue submission and worker completion.
cargo test --release -p blockd-core --test perf_replica -- --ignored --nocapture

# Directory-backed store: 8192 bounded 4 KiB reads from one 32 MiB object.
cargo test --release -p blockd-runtime profile_bounded_directory_range_reads -- --ignored --nocapture

# Startup scan: eight sparse 64 MiB segments, reporting logical versus loaded bytes.
cargo test --release -p blockd-runtime profile_recovery_scan_skips_large_segment_payloads -- --ignored --nocapture
```

On Linux, the real-userfaultfd noisy-neighbor profile adds end-to-end probe
latency, event-loop occupancy, and mean on-loop fill dispatch time:

```sh
cargo test --release -p blockd-runtime --test loop_interference_linux \
  profile_probe_latency_under_noisy_neighbors -- --nocapture

# 128 MiB snapshot upload: elapsed time and maximum bytes held by concurrent
# upload futures. This profile does not require Firecracker artifacts.
cargo test --release -p blockd-runtime --test fc_perf_linux \
  profile_streaming_snapshot_upload -- --ignored --nocapture

# vsetfs metadata request count plus clean and one-page-dirty 8 MiB DAX fsync.
cargo test --release -p blockd-vsetfs \
  profile_metadata_and_dax_writeback_shape -- --ignored --nocapture

# Real guest comparison: one active request worker versus four request queues
# and four independent database vsets. Requires BLOCKD_FC_DIR artifacts.
cargo test --release -p blockd-runtime --test fc_virtiofs_sqlite_e2e_linux \
  profile_parallel_virtiofs_request_queues -- --ignored --nocapture
```

The demo migration response also reports `snapshot_write_ms`, `publish_ms`,
`handoff_ms`, total `migration_ms`, and `overlap_ms`. Use those fields to
compare the same 128 MiB guest and store-latency configuration across builds;
`overlap_ms` is the serial blackout removed by concurrent publication and
handoff.

Keep the same machine, build profile, CPU governor, and background load when
comparing revisions. The printed shape assertions ensure the intended path ran;
the measurements themselves are evidence, not portable pass/fail gates.

## Deterministic simulation ensembles

Three shared presets continuously search beyond the permanent regression seed
corpora:

- `chaos`: single-host crashes, bit rot, and a store outage.
- `cluster`: host loss followed by racing restore claims.
- `migration`: concurrent migration, crashes, lossy and duplicating peer
  traffic, and a store outage.

Run an automation-equivalent shard locally with:

```sh
scripts/run-sim-ensemble.sh migration 0 250 artifacts/simulation/local
```

The runner builds the optimized simulator, records the revision and seed
range, and requires aggregate evidence that the preset's distinguishing faults
actually occurred. The sweep itself retains its original lightweight command:

```sh
cargo run --release -p blockd-sim --bin sweep -- migration 0 250
```

Set `BLOCKD_SWEEP_REQUIRE_COVERAGE=1` to enable aggregate coverage gates and
`BLOCKD_SWEEP_ARTIFACT_DIR=<path>` to retain failure evidence without using the
wrapper.

Every invariant or liveness failure is immediately replayed with the same seed.
Its artifact contains both reports, both trace hashes, the full preset
configuration, whether replay was identical, and an exact replay command.

## Automation policy

Changes and main-branch updates run 1,000 seeds per preset in four independent
shards. The nightly job starts from a range derived from its run identifier and
runs 16,000 new seeds per preset. Failed shards retain their evidence for 30
days.

Any seed that exposes a defect must be added to the matching permanent corpus
before the fix merges. The regression test must assert the intended correct
behavior; it must not pass merely because the defect reproduces.

## Linux kernel integration

The Linux lane runs separately from portable tests and simulation:

```sh
scripts/run-linux-kernel-tests.sh artifacts/linux-kernel
```

It first compiles static assertions against the host's
`<linux/userfaultfd.h>`, pinning the ioctl numbers and feature bits used by the
hand-written Rust ABI. It then runs the userfaultfd, memfd fleet, real disk,
runtime lifecycle, TCP peer, live migration, part-fetch, and loop-interference
integration binaries serially. Serial execution keeps physical-memory and
latency assertions isolated from neighboring tests within the job.

The runner records the host page size. One native size is used end to end for
memory, cache entries, captures, segment storage, transfers, direct I/O, and
simulation. Journal and segment headers carry that size and reject data from
an incompatible host rather than interpreting it with the wrong granularity.

The lane uses `UFFD_USER_MODE_ONLY`, so it does not enable privileged
userfaultfd globally. Firecracker tests are deliberately excluded because they
need a patched binary, kernel, initramfs, and KVM; they belong to their own
nightly lane.
