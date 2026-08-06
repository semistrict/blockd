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
