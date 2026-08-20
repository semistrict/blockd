//! Cluster-level authority and convergence properties.

use blockd_core::types::{millis, secs};
use blockd_sim::cluster::run;

#[test]
fn migration_seed_one_drains_before_the_full_page_audit() {
    let report = run(1, blockd_sim::presets::migration_chaos());
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert!(report.migrations > 0);
}

#[test]
fn accepted_inbound_replaces_a_superseded_handoff_across_destination_crash() {
    let scenario = blockd_sim::scenario::load("migration").expect("migration scenario");
    let blockd_sim::scenario::RealizedScenario::Cluster(config) =
        scenario.realize(180).expect("realize migration seed")
    else {
        panic!("migration must be a cluster scenario");
    };
    let report = run(180, config);
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert!(report.migrations > 0);
}

#[test]
fn migration_replaces_a_nonserving_stale_outbound_destination() {
    let report = run(79, blockd_sim::presets::migration_chaos());
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert!(report.migrations > 0);
    assert_eq!(report.guest_deaths, 0);
}

#[test]
fn migration_reoffer_uses_the_current_post_copy_page_index() {
    let report = run(91, blockd_sim::presets::migration_chaos());
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert!(report.migrations > 0);
    assert_eq!(report.guest_deaths, 0);
}

#[test]
fn delayed_offer_from_an_earlier_source_tenure_cannot_reclaim_the_head() {
    let report = run(133, blockd_sim::presets::migration_chaos());
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert!(report.migrations > 0);
    assert_eq!(report.guest_deaths, 0);
}

#[test]
fn superseded_offer_backlog_does_not_starve_the_current_migration_tenure() {
    let report = run(762, blockd_sim::presets::migration_chaos());
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert!(report.migrations > 0);
    assert_eq!(report.guest_deaths, 0);
}

#[test]
fn recovered_outbound_finalizes_its_provisional_head_before_reoffer() {
    let report = run(66, blockd_sim::presets::migration_chaos());
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audit_runs, 1);
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert!(report.migrations > 0);
    assert_eq!(report.guest_deaths, 0);
}

/// Regression PROD-021: production membership transitions must be a checked-in,
/// deterministic cluster scenario rather than coverage that exists only in the
/// wall-clock runtime tests.
#[test]
fn dynamic_membership_scenario_is_reproducible_and_sweep_gated() {
    let name = "dynamic-membership";
    assert!(
        blockd_sim::scenario::SWEEP_SCENARIOS.contains(&name),
        "dynamic membership scenario is not part of the sweep gate"
    );
    let scenario =
        blockd_sim::scenario::load(name).expect("checked-in dynamic membership scenario");
    let blockd_sim::scenario::RealizedScenario::Cluster(config) =
        scenario.realize(211).expect("realize scenario")
    else {
        panic!("dynamic membership must be a cluster scenario");
    };
    let first = run(211, config.clone());
    let replay = run(211, config);
    assert_eq!(first, replay, "same-seed replay diverged");
    assert!(first.violations.is_empty(), "{first:#?}");
    assert_eq!(first.membership_claims, 6);
    assert!(first.membership_claim_retries >= 3);
    assert_eq!(first.membership_publications, 8);
    assert_eq!(first.membership_committed_lost, 3);
    assert_eq!(first.membership_transitions, 3);
    assert_eq!(first.membership_joins, 1);
    assert_eq!(first.membership_leaves, 2);
    assert!(first.membership_lease_expiries >= 2);
    assert!(first.membership_certificate_rotations >= 1);
    assert!(first.peer_certificate_authorization_drops >= 1);
    assert!(first.peer_renewed_certificate_frames >= 1);
    assert!(first.peer_link_clogs >= 1);
    assert_eq!(first.membership_lists, 6);
    assert_eq!(first.membership_reordered_lists, 6);
    assert_eq!(first.membership_reordered_gets, 6);
    assert!(first.membership_gets >= 65);
    assert_eq!(first.membership_fast_restarts, 1);
    assert_eq!(first.membership_slow_restarts, 1);
    assert_eq!(first.membership_rolling_restarts, 2);
    assert_eq!(first.membership_lease_preserved_restarts, 4);
    assert_eq!(first.placement_epoch_initial, 7);
    assert_eq!(first.placement_epoch_final, 10);
    assert_eq!(first.durable_placement_writes, 3);
    assert!(first.placement_recovered_after_restart > 0);
    assert_eq!(first.placement_owner_recovered_after_restart, 1);
    assert!(first.placement_owner_first_faults_after_restart >= 1);
    assert!(first.stash_recoveries >= 1);
    assert_eq!(first.protected_sync_volumes, 4);
    assert_eq!(first.continuous_volumes, 4);
    assert!(first.completed_ops > 0);
    assert_eq!(first.guest_deaths, 0);
}

/// Regression PROD-003: restart must consume the durable placement epoch
/// before recovering an existing synced volume. The scenario poisons the
/// controller's in-memory placement slot before bouncing the volume owner, so
/// only an object-store-backed startup can satisfy these assertions.
#[test]
fn rolling_restart_loads_durable_placement_before_recovering_synced_owner() {
    let scenario =
        blockd_sim::scenario::load("dynamic-membership").expect("dynamic membership scenario");
    let blockd_sim::scenario::RealizedScenario::Cluster(config) =
        scenario.realize(211).expect("realize scenario")
    else {
        panic!("dynamic membership must be a cluster scenario");
    };
    let report = run(211, config);
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.placement_epoch_final, 10);
    assert_eq!(report.durable_placement_writes, 3);
    assert_eq!(report.placement_owner_recovered_after_restart, 1);
    assert!(report.placement_owner_first_faults_after_restart >= 1);
    assert_eq!(report.protected_sync_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert_eq!(report.continuous_volumes, 4);
    assert!(report.completed_ops > 0);
    assert_eq!(report.guest_deaths, 0);
}

#[test]
fn dynamic_membership_preserves_stash_through_store_fenced_recovery() {
    let scenario =
        blockd_sim::scenario::load("dynamic-membership").expect("dynamic membership scenario");
    let blockd_sim::scenario::RealizedScenario::Cluster(config) =
        scenario.realize(201).expect("realize scenario")
    else {
        panic!("dynamic membership must be a cluster scenario");
    };
    let report = run(201, config);
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.restores, 1);
    assert_eq!(report.stash_recoveries, 1);
    assert_eq!(report.continuous_volumes, 4);
}

#[test]
fn dynamic_membership_control_state_is_store_committed_before_the_final_page_audit() {
    let scenario =
        blockd_sim::scenario::load("dynamic-membership").expect("dynamic membership scenario");
    let blockd_sim::scenario::RealizedScenario::Cluster(config) =
        scenario.realize(200).expect("realize scenario")
    else {
        panic!("dynamic membership must be a cluster scenario");
    };
    let report = run(200, config);
    assert!(report.violations.is_empty(), "{report:#?}");
    assert_eq!(report.audited_volumes, 4);
    assert_eq!(report.audited_pages, 64);
    assert_eq!(report.protected_sync_volumes, 4);
    assert_eq!(report.continuous_volumes, 4);
}

#[test]
fn racing_restore_claims_have_one_winner_and_one_loser() {
    let report = run(31, blockd_sim::presets::cluster_kill_race());
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert_eq!(report.restores, 1);
    assert_eq!(report.claims_lost, 1);
    assert!(report.store_cas_conflicts >= 1);
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
