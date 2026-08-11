//! Seed sweeps over checked-in simulation scenarios.
//!
//! Each seed first realizes bounded scenario distributions from an independent
//! RNG stream and then runs the deterministic simulator. Failure artifacts keep
//! the composed specification, realized configuration, report, and replay
//! comparison together.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use blockd_sim::cluster::FaultPoint;
use blockd_sim::scenario::{
    self, CoverageMetric, CoverageRequirement, FaultPointSpec, OutcomeRequirement,
    RealizedScenario, Scenario,
};
use blockd_sim::{cluster, harness};

fn usage() -> ExitCode {
    eprintln!(
        "usage: sweep <{}> <start-seed> <count>\n\
         optional environment:\n\
         BLOCKD_SWEEP_ARTIFACT_DIR=<path>  retain replay data for failures\n\
         BLOCKD_SWEEP_REQUIRE_COVERAGE=1   fail if the range misses scenario faults\n\
         BLOCKD_SWEEP_REQUIRE_REPLAY=1     replay and compare every seed\n\
         BLOCKD_SWEEP_REQUIRE_DISTINCT=1   fail on a trace-hash collision",
        scenario::SWEEP_SCENARIOS.join("|")
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
    peer_faults: u64,
    replica_commits: u64,
    store_unavailable: u64,
    nvme_reclaims: u64,
    nvme_stalls: u64,
    records_written: u64,
    pages_flushed: u64,
    guest_deaths: u64,
    nemesis_drops: u64,
    wedged_guests: u64,
    wedged_hydration: u64,
    wedged_outbound: u64,
    releases: u64,
    leaf_fills: u64,
    prefetch_fills: u64,
    parked_end: u64,
    hydrating_end: u64,
    space_amplification_ppm: u64,
    fault_hits: BTreeMap<FaultPoint, u64>,
}

impl Coverage {
    fn merge(&mut self, other: &Self) {
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
        self.peer_faults += other.peer_faults;
        self.replica_commits += other.replica_commits;
        self.store_unavailable += other.store_unavailable;
        self.nvme_reclaims += other.nvme_reclaims;
        self.nvme_stalls += other.nvme_stalls;
        self.records_written += other.records_written;
        self.pages_flushed += other.pages_flushed;
        self.guest_deaths += other.guest_deaths;
        self.nemesis_drops += other.nemesis_drops;
        self.wedged_guests += other.wedged_guests;
        self.wedged_hydration += other.wedged_hydration;
        self.wedged_outbound += other.wedged_outbound;
        self.releases += other.releases;
        self.leaf_fills += other.leaf_fills;
        self.prefetch_fills += other.prefetch_fills;
        self.parked_end += other.parked_end;
        self.hydrating_end += other.hydrating_end;
        self.space_amplification_ppm = self
            .space_amplification_ppm
            .max(other.space_amplification_ppm);
        for (point, hits) in &other.fault_hits {
            *self.fault_hits.entry(*point).or_default() += hits;
        }
    }

    fn hits(&self, metric: CoverageMetric, fault_point: Option<FaultPointSpec>) -> u64 {
        match metric {
            CoverageMetric::DaemonCrash => self.crashes,
            CoverageMetric::BitFlip => self.bitflips,
            CoverageMetric::StoreRetry => self.store_retries,
            CoverageMetric::OrphanRestore => self.restores,
            CoverageMetric::RestoreClaimRace => self.claims_lost,
            CoverageMetric::HostRecovery => self.recoveries,
            CoverageMetric::CompletedMigration => self.migrations,
            CoverageMetric::PeerDrop => self.peer_drops,
            CoverageMetric::PeerDuplicate => self.peer_dups,
            CoverageMetric::PeerFault => fault_point.map_or(self.peer_faults, |point| {
                self.fault_hits
                    .get(&point.to_fault_point())
                    .copied()
                    .unwrap_or(0)
            }),
            CoverageMetric::ReplicaCommit => self.replica_commits,
            CoverageMetric::StoreUnavailable => self.store_unavailable,
            CoverageMetric::NvmeReclaim => self.nvme_reclaims,
            CoverageMetric::NvmeStall => self.nvme_stalls,
            CoverageMetric::RecordsWritten => self.records_written,
            CoverageMetric::PagesFlushed => self.pages_flushed,
            CoverageMetric::GuestDeath => self.guest_deaths,
            CoverageMetric::NemesisDrop => self.nemesis_drops,
            CoverageMetric::Wedge => {
                self.wedged_guests + self.wedged_hydration + self.wedged_outbound
            }
            CoverageMetric::WedgeGuest => self.wedged_guests,
            CoverageMetric::WedgeHydration => self.wedged_hydration,
            CoverageMetric::WedgeOutbound => self.wedged_outbound,
            CoverageMetric::Release => self.releases,
            CoverageMetric::LeafFill => self.leaf_fills,
            CoverageMetric::PrefetchFill => self.prefetch_fills,
            CoverageMetric::ParkedEnd => self.parked_end,
            CoverageMetric::HydratingEnd => self.hydrating_end,
            CoverageMetric::SpaceAmplificationPpm => self.space_amplification_ppm,
        }
    }

    fn missing_for<'a>(&self, requirements: &'a [CoverageRequirement]) -> Vec<&'a str> {
        requirements
            .iter()
            .filter_map(|requirement| {
                (self.hits(requirement.metric, requirement.fault_point) == 0)
                    .then_some(requirement.label.as_str())
            })
            .collect()
    }

    fn outcome_violations(&self, requirements: &[OutcomeRequirement]) -> Vec<String> {
        let mut violations = Vec::new();
        for requirement in requirements {
            let hits = self.hits(requirement.metric, requirement.fault_point);
            if let Some(min) = requirement.min
                && hits < min
            {
                violations.push(format!(
                    "scenario outcome {} observed {hits}, below minimum {min}",
                    requirement.label
                ));
            }
            if let Some(max) = requirement.max
                && hits > max
            {
                violations.push(format!(
                    "scenario outcome {} observed {hits}, above maximum {max}",
                    requirement.label
                ));
            }
        }
        violations
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Outcome {
    trace_hash: u64,
    violations: Vec<String>,
    completed_ops: u64,
    coverage: Coverage,
    scenario_sources: Vec<String>,
    scenario_specification: String,
    config: String,
    report: String,
}

impl Outcome {
    fn failed(&self) -> bool {
        !self.violations.is_empty() || self.completed_ops == 0
    }
}

/// Run one seed and retain enough state to diagnose and replay a failure.
fn run_one(scenario: &Scenario, seed: u64) -> Outcome {
    let sources = scenario.sources().to_vec();
    let specification = scenario.resolved_specification().to_owned();
    match scenario
        .realize(seed)
        .unwrap_or_else(|error| panic!("cannot realize scenario {}: {error}", scenario.name()))
    {
        RealizedScenario::SingleHost(config) => {
            let config_debug = format!("{config:#?}");
            let report = harness::run(seed, config);
            let coverage = Coverage {
                runs: 1,
                completed_ops: report.completed_ops,
                crashes: report.crashes,
                bitflips: report.bitflips,
                store_retries: report.counters.store_retries,
                restores: report.restores,
                nvme_reclaims: report.counters.nvme_reclaims,
                nvme_stalls: report.counters.nvme_stalls,
                records_written: report.counters.records_written,
                pages_flushed: report.counters.pages_flushed,
                guest_deaths: report.guest_deaths,
                parked_end: u64::try_from(report.parked_end).expect("parked count fits u64"),
                space_amplification_ppm: ratio_ppm(report.seg_bytes_end, report.seg_live_bytes_end),
                ..Coverage::default()
            };
            let mut violations = report.violations.clone();
            violations.extend(coverage.outcome_violations(scenario.outcomes()));
            Outcome {
                trace_hash: report.trace_hash,
                violations,
                completed_ops: report.completed_ops,
                coverage,
                scenario_sources: sources,
                scenario_specification: specification,
                config: config_debug,
                report: format!("{report:#?}"),
            }
        }
        RealizedScenario::Cluster(config) => {
            let config_debug = format!("{config:#?}");
            let report = cluster::run(seed, config);
            let coverage = Coverage {
                runs: 1,
                completed_ops: report.completed_ops,
                crashes: report.host_crashes,
                store_retries: report.store_retries,
                restores: report.restores,
                claims_lost: report.claims_lost,
                recoveries: report.recoveries,
                migrations: report.migrations,
                peer_drops: report.peer_drops,
                peer_dups: report.peer_dups,
                peer_faults: report.fault_coverage.values().sum(),
                replica_commits: report.replica_commits,
                store_unavailable: report.store_unavailable,
                guest_deaths: report.guest_deaths,
                nemesis_drops: report.nemesis_drops,
                wedged_guests: report.wedged_guests,
                wedged_hydration: report.wedged_hydration,
                wedged_outbound: report.wedged_outbound,
                releases: report.releases,
                leaf_fills: report.leaf_fills,
                prefetch_fills: report.prefetch_fills,
                parked_end: u64::try_from(report.parked_end).expect("parked count fits u64"),
                hydrating_end: u64::try_from(report.hydrating_end)
                    .expect("hydrating count fits u64"),
                fault_hits: report.fault_coverage.clone(),
                ..Coverage::default()
            };
            let mut violations = report.violations.clone();
            violations.extend(coverage.outcome_violations(scenario.outcomes()));
            Outcome {
                trace_hash: report.trace_hash,
                violations,
                completed_ops: report.completed_ops,
                coverage,
                scenario_sources: sources,
                scenario_specification: specification,
                config: config_debug,
                report: format!("{report:#?}"),
            }
        }
    }
}

fn ratio_ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return if numerator == 0 { 0 } else { u64::MAX };
    }
    let scaled = u128::from(numerator) * 1_000_000 / u128::from(denominator);
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

fn write_failure_artifact(
    directory: &Path,
    scenario_name: &str,
    seed: u64,
    first: &Outcome,
    replay: &Outcome,
) -> std::io::Result<()> {
    fs::create_dir_all(directory)?;
    let mut artifact = String::new();
    writeln!(artifact, "scenario: {scenario_name}").expect("write string");
    writeln!(artifact, "scenario sources: {:?}", first.scenario_sources).expect("write string");
    writeln!(artifact, "seed: {seed}").expect("write string");
    writeln!(
        artifact,
        "replay command: target/release/sweep {scenario_name} {seed} 1"
    )
    .expect("write string");
    writeln!(artifact, "replay identical: {}", first == replay).expect("write string");
    writeln!(artifact, "first trace hash: {:#018x}", first.trace_hash).expect("write string");
    writeln!(artifact, "replay trace hash: {:#018x}", replay.trace_hash).expect("write string");
    writeln!(
        artifact,
        "\ncomposed scenario specification:\n{}",
        first.scenario_specification
    )
    .expect("write string");
    writeln!(artifact, "\nrealized configuration:\n{}", first.config).expect("write string");
    writeln!(artifact, "\nfirst report:\n{}", first.report).expect("write string");
    writeln!(artifact, "\nreplay report:\n{}", replay.report).expect("write string");
    fs::write(
        directory.join(format!("{scenario_name}-seed-{seed}.txt")),
        artifact,
    )
}

fn write_coverage_artifact(
    directory: &Path,
    scenario_name: &str,
    start: u64,
    end: u64,
    coverage: &Coverage,
    missing: &[&str],
) -> std::io::Result<()> {
    fs::create_dir_all(directory)?;
    fs::write(
        directory.join(format!("{scenario_name}-coverage.txt")),
        format!(
            "scenario: {scenario_name}\nseeds: {start}..{end}\nmissing: {missing:?}\ncoverage: {coverage:#?}\n"
        ),
    )
}

#[allow(clippy::too_many_lines)]
fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, scenario_name, start, count] = args.as_slice() else {
        return usage();
    };
    if !scenario::SWEEP_SCENARIOS.contains(&scenario_name.as_str()) {
        return usage();
    }
    let scenario = match scenario::load(scenario_name) {
        Ok(scenario) => scenario,
        Err(error) => {
            eprintln!("invalid scenario {scenario_name}: {error}");
            return ExitCode::from(2);
        }
    };
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
    let require_replay = std::env::var_os("BLOCKD_SWEEP_REQUIRE_REPLAY").is_some();
    let require_distinct = std::env::var_os("BLOCKD_SWEEP_REQUIRE_DISTINCT").is_some();

    let mut failed = 0u64;
    let mut artifact_errors = 0u64;
    let mut coverage = Coverage::default();
    let mut trace_seeds = BTreeMap::new();
    for seed in start..end {
        let first = run_one(&scenario, seed);
        coverage.merge(&first.coverage);
        for violation in &first.violations {
            println!("seed {seed}: VIOLATION: {violation}");
        }
        if first.completed_ops == 0 {
            println!("seed {seed}: LIVENESS: zero guest ops completed");
        }
        let replay = (require_replay || first.failed()).then(|| run_one(&scenario, seed));
        let nondeterministic = replay.as_ref().is_some_and(|replay| first != *replay);
        if nondeterministic {
            let replay = replay.as_ref().expect("compared replay exists");
            println!(
                "seed {seed}: NONDETERMINISM: first trace {:#018x}, replay {:#018x}",
                first.trace_hash, replay.trace_hash
            );
        }
        let collision = if require_distinct {
            trace_seeds
                .insert(first.trace_hash, seed)
                .filter(|previous| *previous != seed)
        } else {
            None
        };
        if let Some(previous) = collision {
            println!(
                "seed {seed}: TRACE COLLISION: seed {previous} also produced {:#018x}",
                first.trace_hash
            );
        }
        if first.failed() || nondeterministic || collision.is_some() {
            failed += 1;
        }
        if first.failed() || nondeterministic {
            let replay = replay.as_ref().expect("failed seed was replayed");
            if let Some(directory) = &artifact_dir
                && let Err(error) =
                    write_failure_artifact(directory, scenario_name, seed, &first, replay)
            {
                artifact_errors += 1;
                eprintln!("seed {seed}: failed to write replay artifact: {error}");
            }
        }
    }
    println!("coverage {scenario_name}: {coverage:?}");

    let missing = coverage.missing_for(scenario.coverage());
    if require_coverage && !missing.is_empty() {
        failed += 1;
        println!(
            "COVERAGE: scenario {scenario_name} missed {}",
            missing.join(", ")
        );
        if let Some(directory) = &artifact_dir
            && let Err(error) =
                write_coverage_artifact(directory, scenario_name, start, end, &coverage, &missing)
        {
            artifact_errors += 1;
            eprintln!("failed to write coverage artifact: {error}");
        }
    }
    println!(
        "sweep {scenario_name} seeds {start}..{end}: {failed} failing of {count}; \
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
    use blockd_sim::scenario;

    use super::{Coverage, ratio_ppm};

    #[test]
    fn coverage_gates_come_from_scenario_documents() {
        let chaos = scenario::load("chaos").expect("chaos scenario");
        assert_eq!(
            Coverage::default().missing_for(chaos.coverage()),
            ["daemon crash", "bit flip", "store retry"]
        );
        let migration = scenario::load("migration").expect("migration scenario");
        assert_eq!(
            Coverage::default().missing_for(migration.coverage()),
            [
                "host recovery",
                "completed migration",
                "peer drop",
                "peer duplicate"
            ]
        );
        let peer = scenario::load("peer-stash").expect("peer scenario");
        assert_eq!(
            Coverage::default().missing_for(peer.coverage()),
            ["replica commit", "store unavailable", "peer drop"]
        );
    }

    #[test]
    fn merging_coverage_preserves_every_observation() {
        let mut total = Coverage {
            runs: 1,
            crashes: 2,
            migrations: 3,
            replica_commits: 4,
            ..Coverage::default()
        };
        total.merge(&Coverage {
            runs: 5,
            crashes: 6,
            migrations: 7,
            replica_commits: 8,
            ..Coverage::default()
        });
        assert_eq!(total.runs, 6);
        assert_eq!(total.crashes, 8);
        assert_eq!(total.migrations, 10);
        assert_eq!(total.replica_commits, 12);
    }

    #[test]
    fn named_fault_gates_require_the_exact_boundary() {
        let scenario = scenario::load("peer-transition-before-cas").expect("scenario");
        let requirement = &scenario.coverage()[0];
        let mut coverage = Coverage::default();
        assert_eq!(
            coverage.missing_for(scenario.coverage()),
            [requirement.label.as_str()]
        );
        let point = requirement
            .fault_point
            .expect("named fault point")
            .to_fault_point();
        coverage.fault_hits.insert(point, 1);
        coverage.peer_faults = 1;
        assert!(coverage.missing_for(scenario.coverage()).is_empty());
    }

    #[test]
    fn per_run_outcomes_enforce_lower_and_upper_bounds() {
        let scenario = scenario::load("leaf-rot").expect("scenario");
        let matching = Coverage {
            restores: 1,
            guest_deaths: 1,
            ..Coverage::default()
        };
        assert!(matching.outcome_violations(scenario.outcomes()).is_empty());

        let wrong = Coverage {
            restores: 1,
            guest_deaths: 2,
            ..Coverage::default()
        };
        assert_eq!(wrong.outcome_violations(scenario.outcomes()).len(), 1);
    }

    #[test]
    fn space_amplification_ratio_is_integer_ppm_and_handles_empty_live_data() {
        assert_eq!(ratio_ppm(3, 2), 1_500_000);
        assert_eq!(ratio_ppm(0, 0), 0);
        assert_eq!(ratio_ppm(1, 0), u64::MAX);
    }
}
