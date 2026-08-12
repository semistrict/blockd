//! Multi-host properties over the deterministic actor topology.

use blockd_core::journal::VsetConfig;
use blockd_core::types::{VsetId, millis, secs};
use blockd_sim::cluster::{ClusterConfig, ClusterReport, FaultPoint, run};
use blockd_sim::scenario::RealizedScenario;

fn assert_clean(report: &ClusterReport) {
    assert!(report.violations.is_empty(), "{:?}", report.violations);
}

fn restore_config() -> ClusterConfig {
    blockd_sim::presets::cluster_kill_race()
}

fn migration_config() -> ClusterConfig {
    ClusterConfig {
        hosts: 2,
        vset_count: 1,
        vset_config: VsetConfig::compute(2, 16),
        kill_hosts_at: vec![],
        crash_hosts_at: vec![],
        drop_peer: None,
        race_restore: false,
        migrate_at: vec![(millis(500), VsetId(1), 1)],
        checkpoint_interval: None,
        horizon: secs(2),
        ..restore_config()
    }
}

#[test]
fn cluster_replays_byte_for_byte_and_distinct_seeds_diverge() {
    let first = run(31, restore_config());
    let replay = run(31, restore_config());
    let other = run(44, restore_config());
    assert_eq!(first, replay);
    assert_ne!(first.trace_hash, other.trace_hash);
}

#[test]
fn workload_horizon_begins_after_initial_creation() {
    let mut config = migration_config();
    config.store.latency_min = millis(50);
    config.store.latency_max = millis(50);
    config.store.ns_per_byte = 0;
    config.migrate_at.clear();
    config.horizon = millis(20);
    config.think = (millis(1), millis(1));

    let report = run(37, config);

    assert_clean(&report);
    assert!(report.completed_ops > 0, "{report:?}");
}

#[test]
fn host_death_restores_one_authoritative_runner() {
    let report = run(31, restore_config());
    assert_clean(&report);
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_vsets, 3);
    assert_eq!(report.audited_pages, 3 * 3 * 16);
    assert_eq!(report.restores, 1);
    assert_eq!(report.claims_lost, 1);
    assert_eq!(report.loss_bound_verified, 1);
    assert_eq!(report.guest_deaths, 0);
    assert!(report.completed_ops > 100);
}

#[test]
fn crash_drops_the_host_tree_then_recovers_from_its_disk() {
    let mut config = migration_config();
    config.migrate_at.clear();
    config.crash_hosts_at = vec![(millis(600), 0)];
    let report = run(53, config);
    assert_clean(&report);
    assert_eq!(report.host_crashes, 1);
    assert_eq!(report.recoveries, 1);
    assert_eq!(report.guest_deaths, 0);
    assert!(report.completed_ops > 50);
}

#[test]
fn migration_is_lossless_and_background_hydration_releases_the_source() {
    let report = run(7, migration_config());
    assert_clean(&report);
    assert_eq!(report.migrations, 1);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.restores, 0);
    assert!(report.max_migration_pause_ns > 0);
    assert!(
        report.max_migration_pause_ns < millis(500),
        "migration pause was {} ns",
        report.max_migration_pause_ns
    );
    assert!(report.hydrate_fills > 0);
    assert!(report.releases >= 1);
    assert_eq!(report.blobs_per_host[0], 0);
}

#[test]
fn lossy_duplicating_links_preserve_migration_and_replay() {
    let config = || ClusterConfig {
        peer_drop: (1, 4),
        peer_dup: (1, 8),
        ..migration_config()
    };
    let first = run(71, config());
    let replay = run(71, config());
    assert_eq!(first, replay);
    assert_clean(&first);
    assert_eq!(first.migrations, 1, "{first:?}");
    assert_eq!(first.guest_deaths, 0);
    assert!(first.peer_drops > 0);
    assert!(first.peer_dups > 0);
}

#[test]
fn return_migration_and_crash_preserve_every_page() {
    let mut config = migration_config();
    config.migrate_at = vec![(millis(400), VsetId(1), 1), (millis(1_000), VsetId(1), 0)];
    config.crash_hosts_at = vec![(millis(750), 0), (millis(2_200), 1)];
    config.horizon = secs(3);
    let report = run(1, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 2, "{report:?}");
    assert!(report.releases >= 2, "{report:?}");
    assert_eq!(report.host_crashes, 2);
}

#[test]
fn released_source_residue_never_starts_a_second_guest() {
    let mut config = migration_config();
    config.think = (millis(20), millis(20));
    config.migrate_at = vec![(millis(400), VsetId(1), 1), (millis(1_500), VsetId(1), 0)];
    config.crash_hosts_at = vec![(millis(405), 1), (millis(2_200), 1)];
    config.horizon = secs(3);
    let report = run(1, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 2, "{report:?}");
    assert!(report.releases >= 1);
    assert_eq!(report.host_crashes, 2);
}

#[test]
fn migration_quiesces_an_inflight_guest_operation_before_capture() {
    let scenario = blockd_sim::scenario::load("migration").expect("migration scenario");
    let RealizedScenario::Cluster(config) = scenario.realize(261).expect("cluster configuration")
    else {
        panic!("migration scenario must realize as a cluster");
    };
    let report = run(261, config);
    assert_clean(&report);
    assert!(report.migrations > 0, "{report:?}");
    assert!(report.host_crashes > 0, "{report:?}");
}

#[test]
fn migration_drains_inflight_guest_operations_before_recovery_cuts() {
    let scenario = blockd_sim::scenario::load("migration").expect("migration scenario");
    for seed in [19, 429, 487, 499, 640, 714, 736, 826, 890, 896, 922] {
        let RealizedScenario::Cluster(config) =
            scenario.realize(seed).expect("cluster configuration")
        else {
            panic!("migration scenario must realize as a cluster");
        };
        let report = run(seed, config);
        assert_clean(&report);
        assert!(report.completed_ops > 0, "seed {seed}: {report:?}");
        assert!(report.migrations > 0, "seed {seed}: {report:?}");
        assert!(report.host_crashes > 0, "seed {seed}: {report:?}");
    }
}

#[test]
fn returning_migration_avoids_stale_destination_journal_names() {
    let scenario = blockd_sim::scenario::load("migration").expect("migration scenario");
    let RealizedScenario::Cluster(config) = scenario.realize(952).expect("cluster configuration")
    else {
        panic!("migration scenario must realize as a cluster");
    };
    let report = run(952, config);
    assert_clean(&report);
    assert!(report.migrations > 0, "{report:?}");
}

#[test]
fn passive_replica_commits_uploads_and_unlinks_without_rewrite() {
    let mut config = blockd_sim::presets::peer_stash_chaos();
    config.peer_drop = (0, 1);
    config.peer_dup = (0, 1);
    config.store_outage = None;
    config.vset_count = 1;
    config.horizon = millis(500);
    config.think = (millis(2), millis(4));
    let report = run(91, config);
    assert_clean(&report);
    assert!(report.replica_logical_bytes > 0, "{report:?}");
    assert!(report.replica_network_bytes >= report.replica_logical_bytes);
    assert!(report.replica_store_bytes > 0);
    assert!(report.replica_artifact_flushes > 0);
    assert!(report.replica_commit_flushes > 0);
    assert!(report.replica_unlinks > 0);
    assert_eq!(report.replica_nonactive_bytes, 0);
    assert_eq!(report.replica_cleanup_rewrite_bytes, 0);
}

#[test]
fn outages_and_directional_clogs_are_observed_then_heal() {
    let report = run(119, blockd_sim::presets::swizzle_peer_links());
    assert_clean(&report);
    assert!(report.peer_link_clogs > 0);
    assert!(report.store_unavailable > 0);
    assert!(report.store_retries > 0);
    assert_eq!(report.parked_end, 0);
}

#[test]
fn forced_replica_fault_point_is_hit_and_idempotent() {
    let mut config = blockd_sim::presets::peer_stash_chaos();
    config.peer_drop = (0, 1);
    config.peer_dup = (0, 1);
    config.store_outage = None;
    config.vset_count = 1;
    config.horizon = millis(500);
    config.think = (millis(2), millis(4));
    config.fault_points = vec![FaultPoint::ReleaseOverlap];
    let report = run(131, config);
    assert_clean(&report);
    assert!(
        report
            .fault_coverage
            .get(&FaultPoint::ReleaseOverlap)
            .copied()
            .unwrap_or(0)
            > 0
    );
    assert!(report.replica_unlinks > 0);
    assert_eq!(report.replica_cleanup_rewrite_bytes, 0);
}

#[test]
fn injected_replica_crash_cancels_and_recovers_the_host_actor_tree() {
    let mut config = blockd_sim::presets::peer_stash_chaos();
    config.peer_drop = (0, 1);
    config.peer_dup = (0, 1);
    config.store_outage = None;
    config.vset_count = 1;
    config.horizon = millis(800);
    config.think = (millis(2), millis(4));
    let point = FaultPoint::CrashPrimaryAfterClosureCapture;
    config.fault_points = vec![point];
    let report = run(139, config);
    assert_clean(&report);
    assert!(report.fault_coverage.get(&point).copied().unwrap_or(0) > 0);
    assert!(
        report.host_crashes > 0,
        "injected abort did not cancel the host"
    );
}

#[test]
fn corrupt_resume_set_costs_warmth_not_correctness() {
    let mut config = restore_config();
    config.vset_count = 1;
    config.race_restore = false;
    config.kill_hosts_at = vec![(millis(800), 0), (millis(2_200), 1)];
    config.rot_resume_set_at = Some(millis(2_100));
    config.horizon = secs(4);
    let report = run(11, config);
    assert_clean(&report);
    assert_eq!(report.restores, 2, "{report:?}");
    assert_eq!(report.guest_deaths, 0);
}
