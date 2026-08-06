//! Seed sweep over the corpus chaos presets: the corpora pin ~30
//! hand-promoted seeds forever, but a bug reachable only from seed 132+
//! is never searched for unless something searches. This binary runs one
//! preset over a seed range with violation-only checks (plus a liveness
//! floor), one process per range so a driver can fan ranges across cores:
//!
//! ```sh
//! cargo build --release -p blockd-sim --bin sweep
//! for i in $(seq 0 7); do target/release/sweep chaos $((i*250)) 250 & done; wait
//! ```
//!
//! Any failing seed reproduces exactly under the matching corpus test
//! (same preset, by construction) — promote it into the corpus list.

use std::process::ExitCode;

use blockd_sim::{cluster, harness, presets};

fn usage() -> ExitCode {
    eprintln!("usage: sweep <chaos|cluster|migration> <start-seed> <count>");
    ExitCode::from(2)
}

/// One run: (violations, completed guest ops).
fn run_one(preset: &str, seed: u64) -> (Vec<String>, u64) {
    match preset {
        "chaos" => {
            let report = harness::run(seed, presets::single_host_chaos());
            (report.violations, report.completed_ops)
        }
        "cluster" => {
            let report = cluster::run(seed, presets::cluster_kill_race());
            (report.violations, report.completed_ops)
        }
        "migration" => {
            let report = cluster::run(seed, presets::migration_chaos());
            (report.violations, report.completed_ops)
        }
        _ => unreachable!("preset validated in main"),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, preset, start, count] = args.as_slice() else {
        return usage();
    };
    if !matches!(preset.as_str(), "chaos" | "cluster" | "migration") {
        return usage();
    }
    let (Ok(start), Ok(count)) = (start.parse::<u64>(), count.parse::<u64>()) else {
        return usage();
    };

    let mut failed = 0u64;
    for seed in start..start + count {
        let (violations, completed_ops) = run_one(preset, seed);
        for violation in &violations {
            println!("seed {seed}: VIOLATION: {violation}");
        }
        // Liveness floor: a run where no guest op completed found a wedge
        // even if no invariant fired.
        if completed_ops == 0 {
            println!("seed {seed}: LIVENESS: zero guest ops completed");
        }
        if !violations.is_empty() || completed_ops == 0 {
            failed += 1;
        }
    }
    println!(
        "sweep {preset} seeds {start}..{}: {failed} failing of {count}",
        start + count
    );
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
