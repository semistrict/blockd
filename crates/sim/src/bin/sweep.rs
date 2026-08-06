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

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use blockd_sim::{cluster, harness, presets};

fn usage() -> ExitCode {
    eprintln!(
        "usage: sweep <chaos|cluster|migration> <start-seed> <count>\n\
         optional environment:\n\
         BLOCKD_SWEEP_ARTIFACT_DIR=<path>  retain replay data for failures\n\
         BLOCKD_SWEEP_REQUIRE_COVERAGE=1   fail if the range misses preset faults"
    );
    ExitCode::from(2)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Coverage {
    runs: u64,
    completed_ops: u64,
    crashes: u64,
    bitflips: u64,
    store_retries: u64,
    restores: u64,
    claims_lost: u64,
    recoveries: u64,
    migrations: u64,
    peer_drops: u64,
    peer_dups: u64,
}

impl Coverage {
    fn merge(&mut self, other: &Coverage) {
        self.runs += other.runs;
        self.completed_ops += other.completed_ops;
        self.crashes += other.crashes;
        self.bitflips += other.bitflips;
        self.store_retries += other.store_retries;
        self.restores += other.restores;
        self.claims_lost += other.claims_lost;
        self.recoveries += other.recoveries;
        self.migrations += other.migrations;
        self.peer_drops += other.peer_drops;
        self.peer_dups += other.peer_dups;
    }

    fn missing_for(&self, preset: &str) -> Vec<&'static str> {
        let required: Vec<(u64, &'static str)> = match preset {
            "chaos" => vec![
                (self.crashes, "daemon crash"),
                (self.bitflips, "bit flip"),
                (self.store_retries, "store outage retry"),
            ],
            "cluster" => vec![
                (self.restores, "orphan restore"),
                (self.claims_lost, "restore claim race"),
            ],
            "migration" => vec![
                (self.recoveries, "host recovery"),
                (self.migrations, "completed migration"),
                (self.peer_drops, "peer message drop"),
                (self.peer_dups, "peer message duplicate"),
            ],
            _ => unreachable!("preset validated in main"),
        };
        required
            .iter()
            .filter_map(|&(hits, name)| (hits == 0).then_some(name))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Outcome {
    trace_hash: u64,
    violations: Vec<String>,
    completed_ops: u64,
    coverage: Coverage,
    config: String,
    report: String,
}

impl Outcome {
    fn failed(&self) -> bool {
        !self.violations.is_empty() || self.completed_ops == 0
    }
}

/// Run one seed and retain enough state to diagnose and replay a failure.
fn run_one(preset: &str, seed: u64) -> Outcome {
    match preset {
        "chaos" => {
            let config = presets::single_host_chaos();
            let config_debug = format!("{config:#?}");
            let report = harness::run(seed, config);
            Outcome {
                trace_hash: report.trace_hash,
                violations: report.violations.clone(),
                completed_ops: report.completed_ops,
                coverage: Coverage {
                    runs: 1,
                    completed_ops: report.completed_ops,
                    crashes: report.crashes,
                    bitflips: report.bitflips,
                    store_retries: report.counters.store_retries,
                    restores: report.restores,
                    ..Coverage::default()
                },
                config: config_debug,
                report: format!("{report:#?}"),
            }
        }
        "cluster" => {
            let config = presets::cluster_kill_race();
            let config_debug = format!("{config:#?}");
            let report = cluster::run(seed, config);
            Outcome {
                trace_hash: report.trace_hash,
                violations: report.violations.clone(),
                completed_ops: report.completed_ops,
                coverage: Coverage {
                    runs: 1,
                    completed_ops: report.completed_ops,
                    restores: report.restores,
                    claims_lost: report.claims_lost,
                    recoveries: report.recoveries,
                    ..Coverage::default()
                },
                config: config_debug,
                report: format!("{report:#?}"),
            }
        }
        "migration" => {
            let config = presets::migration_chaos();
            let config_debug = format!("{config:#?}");
            let report = cluster::run(seed, config);
            Outcome {
                trace_hash: report.trace_hash,
                violations: report.violations.clone(),
                completed_ops: report.completed_ops,
                coverage: Coverage {
                    runs: 1,
                    completed_ops: report.completed_ops,
                    restores: report.restores,
                    claims_lost: report.claims_lost,
                    recoveries: report.recoveries,
                    migrations: report.migrations,
                    peer_drops: report.peer_drops,
                    peer_dups: report.peer_dups,
                    ..Coverage::default()
                },
                config: config_debug,
                report: format!("{report:#?}"),
            }
        }
        _ => unreachable!("preset validated in main"),
    }
}

fn write_failure_artifact(
    directory: &Path,
    preset: &str,
    seed: u64,
    first: &Outcome,
    replay: &Outcome,
) -> std::io::Result<()> {
    fs::create_dir_all(directory)?;
    let mut artifact = String::new();
    writeln!(artifact, "preset: {preset}").expect("write string");
    writeln!(artifact, "seed: {seed}").expect("write string");
    writeln!(
        artifact,
        "replay command: target/release/sweep {preset} {seed} 1"
    )
    .expect("write string");
    writeln!(artifact, "replay identical: {}", first == replay).expect("write string");
    writeln!(artifact, "first trace hash: {:#018x}", first.trace_hash).expect("write string");
    writeln!(artifact, "replay trace hash: {:#018x}", replay.trace_hash).expect("write string");
    writeln!(artifact, "\nconfiguration:\n{}", first.config).expect("write string");
    writeln!(artifact, "\nfirst report:\n{}", first.report).expect("write string");
    writeln!(artifact, "\nreplay report:\n{}", replay.report).expect("write string");
    fs::write(
        directory.join(format!("{preset}-seed-{seed}.txt")),
        artifact,
    )
}

fn write_coverage_artifact(
    directory: &Path,
    preset: &str,
    start: u64,
    end: u64,
    coverage: &Coverage,
    missing: &[&str],
) -> std::io::Result<()> {
    fs::create_dir_all(directory)?;
    fs::write(
        directory.join(format!("{preset}-coverage.txt")),
        format!(
            "preset: {preset}\nseeds: {start}..{end}\nmissing: {missing:?}\ncoverage: {coverage:#?}\n"
        ),
    )
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
    let Some(end) = start.checked_add(count) else {
        eprintln!("seed range overflows u64");
        return ExitCode::from(2);
    };
    if count == 0 {
        eprintln!("count must be greater than zero");
        return ExitCode::from(2);
    }

    let artifact_dir = std::env::var_os("BLOCKD_SWEEP_ARTIFACT_DIR").map(PathBuf::from);
    let require_coverage = std::env::var_os("BLOCKD_SWEEP_REQUIRE_COVERAGE").is_some();

    let mut failed = 0u64;
    let mut artifact_errors = 0u64;
    let mut coverage = Coverage::default();
    for seed in start..end {
        let first = run_one(preset, seed);
        coverage.merge(&first.coverage);
        for violation in &first.violations {
            println!("seed {seed}: VIOLATION: {violation}");
        }
        // Liveness floor: a run where no guest op completed found a wedge
        // even if no invariant fired.
        if first.completed_ops == 0 {
            println!("seed {seed}: LIVENESS: zero guest ops completed");
        }
        if first.failed() {
            failed += 1;
            let replay = run_one(preset, seed);
            if first != replay {
                println!(
                    "seed {seed}: NONDETERMINISM: first trace {:#018x}, replay {:#018x}",
                    first.trace_hash, replay.trace_hash
                );
            }
            if let Some(directory) = &artifact_dir
                && let Err(error) = write_failure_artifact(directory, preset, seed, &first, &replay)
            {
                artifact_errors += 1;
                eprintln!("seed {seed}: failed to write replay artifact: {error}");
            }
        }
    }
    println!("coverage {preset}: {coverage:?}");

    let missing = coverage.missing_for(preset);
    if require_coverage && !missing.is_empty() {
        failed += 1;
        println!("COVERAGE: preset {preset} missed {}", missing.join(", "));
        if let Some(directory) = &artifact_dir
            && let Err(error) =
                write_coverage_artifact(directory, preset, start, end, &coverage, &missing)
        {
            artifact_errors += 1;
            eprintln!("failed to write coverage artifact: {error}");
        }
    }
    println!(
        "sweep {preset} seeds {start}..{end}: {failed} failing of {count}; \
         {artifact_errors} artifact errors"
    );
    if failed == 0 && artifact_errors == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::Coverage;

    #[test]
    fn each_preset_requires_its_distinguishing_faults() {
        assert_eq!(
            Coverage::default().missing_for("chaos"),
            ["daemon crash", "bit flip", "store outage retry"]
        );
        assert_eq!(
            Coverage::default().missing_for("cluster"),
            ["orphan restore", "restore claim race"]
        );
        assert_eq!(
            Coverage::default().missing_for("migration"),
            [
                "host recovery",
                "completed migration",
                "peer message drop",
                "peer message duplicate"
            ]
        );
    }

    #[test]
    fn merging_coverage_preserves_every_observation() {
        let mut total = Coverage {
            runs: 1,
            crashes: 2,
            migrations: 3,
            ..Coverage::default()
        };
        total.merge(&Coverage {
            runs: 4,
            crashes: 5,
            migrations: 6,
            ..Coverage::default()
        });
        assert_eq!(total.runs, 5);
        assert_eq!(total.crashes, 7);
        assert_eq!(total.migrations, 9);
    }
}
