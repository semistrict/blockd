//! Cluster-level authority and convergence properties.

use blockd_core::types::{millis, secs};
use blockd_sim::cluster::run;

#[test]
fn racing_restore_claims_have_one_winner_and_one_loser() {
    let report = run(31, blockd_sim::presets::cluster_kill_race());
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert_eq!(report.restores, 1);
    assert_eq!(report.claims_lost, 1);
    assert_eq!(report.store_cas_conflicts, 1);
    assert_eq!(report.guest_deaths, 0);
}

#[test]
fn store_outage_defers_restore_without_losing_authority() {
    let mut config = blockd_sim::presets::cluster_kill_race();
    config.store_outage = Some((millis(1_200), millis(2_400)));
    config.horizon = secs(3);
    let report = run(31, config);
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert!(report.restores >= 1);
    assert!(report.store_unavailable > 0);
    assert!(report.store_retries > 0);
    assert!(report.max_restore_ns >= millis(700));
    assert_eq!(report.guest_deaths, 0);
}

#[test]
fn random_cluster_crashes_replay_and_converge() {
    let mut config = blockd_sim::presets::cluster_kill_race();
    config.kill_hosts_at.clear();
    config.race_restore = false;
    config.crash_mean_interval = millis(500);
    config.restart_delay = (millis(10), millis(50));
    config.horizon = secs(2);
    let first = run(83, config.clone());
    let replay = run(83, config);
    assert_eq!(first, replay);
    assert!(first.violations.is_empty(), "{:?}", first.violations);
    assert!(first.host_crashes > 0);
    assert!(first.recoveries > 0);
    assert!(first.recoveries <= first.host_crashes);
    assert!(first.completed_ops > 0);
    assert_eq!(first.parked_end, 0);
}
