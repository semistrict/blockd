# Testing

Run the portable suite with nextest:

```sh
cargo nextest run
cargo test --doc --workspace
```

If nextest is unavailable, use `cargo test --workspace`.

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
cargo test --release -p blockd-core --test perf_replica -- --ignored --nocapture
cargo test --release -p blockd-runtime --test perf_decider -- --nocapture
cargo test --release -p blockd-runtime --test loop_interference_linux -- --nocapture
cargo test --release -p blockd-runtime --test fc_perf_linux -- --ignored --nocapture
```

Compare performance results only on the same machine, build profile, CPU
governor, and background load.
