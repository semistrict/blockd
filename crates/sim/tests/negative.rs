//! Negative tests at the daemon/world level: deliberately break one rule
//! and assert the oracle CATCHES it. A green suite is only meaningful if
//! these fail red — an oracle that misses planted misbehavior would pass
//! every honest run vacuously.

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{HostId, millis, secs};
use blockd_sim::harness::{FaultPlan, HarnessConfig, Sabotage, run};
use blockd_sim::world::blobdev::BlobDevConfig;
use blockd_sim::world::store::StoreConfig;

fn base_config() -> HarnessConfig {
    let mut config = blockd_sim::presets::single_host_base();
    config.vset_count = 2;
    config
}

#[test]
fn oracle_catches_corrupted_fills() {
    // R8.1's check is not vacuous: bytes that differ from ghost truth by a
    // single bit, delivered through the only door into guest memory, are
    // flagged the moment any guest observes them.
    let mut config = base_config();
    config.sabotage = Some(Sabotage::CorruptFill);
    let report = run(3, config);
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.contains("R8.1") || v.contains("expected seq")),
        "corrupted fills went unnoticed: {:?}",
        report.violations
    );
}

#[test]
fn oracle_catches_dropped_write_protection() {
    // A daemon that loses its dirty tracking captures stale pages; after a
    // crash the recovered state contradicts ghost truth. The oracle must
    // see it (R3.8/R8.1) — this is the failure mode write protection
    // exists to prevent.
    let mut config = base_config();
    config.horizon = secs(4);
    config.checkpoint_interval = Some(millis(200));
    config.faults = FaultPlan {
        crash_mean_interval: millis(700),
        restart_delay: (millis(10), millis(100)),
        bitflip_mean_interval: 0,
        journal_bitflip_mean_interval: 0,
        store_outage: None,
    };
    config.sabotage = Some(Sabotage::DropWriteProtect);
    let report = run(3, config);
    assert!(
        !report.violations.is_empty(),
        "stale captures from dropped write protection went unnoticed"
    );
}

/// The two-sided handoff is not vacuous either: a source that ACTS on a
/// handoff durability it does not have (the marker write acked but never
/// persisted) recovers its vset RUNNABLE after a crash — while the
/// destination is already running it. The harness's ground-truth
/// two-runners check must catch the double-run R7.2 forbids.
#[test]
fn oracle_catches_a_source_that_skips_the_durable_handoff() {
    let config = blockd_sim::cluster::ClusterConfig {
        hosts: 2,
        vset_count: 1,
        vset_config: VsetConfig::compute(2, 16, false),
        nonbacked_vsets: 0,
        daemon: DaemonConfig {
            host: HostId(0),
            cache_pages: 128,
            writeback_interval: millis(20),
            backup_retry: millis(100),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 25,
            replica_placement: None,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        horizon: secs(4),
        think: (millis(1), millis(5)),
        checkpoint_interval: Some(millis(300)),
        kill_hosts_at: vec![],
        crash_hosts_at: vec![(millis(1510), 0)],
        restart_delay: (millis(50), millis(200)),
        crash_mean_interval: 0,
        migrate_mean_interval: 0,
        peer_drop: (0, 1),
        peer_dup: (0, 1),
        peer_link_outages: vec![],
        fault_points: vec![],
        store_outage: None,
        rot_resume_set_at: None,
        rot_leaves_at: None,
        drop_peer: None,
        race_restore: false,
        migrate_at: Some((millis(1500), blockd_core::types::VsetId(1), 1)),
        sabotage: Some(Sabotage::EagerHandoffAck),
        guest_sync_share: None,
    };
    let report = blockd_sim::cluster::run(7, config);
    // The migration itself completed — that is what makes the sabotage
    // dangerous rather than merely broken.
    assert_eq!(report.migrations, 1);
    assert!(
        report.violations.iter().any(|v| v.contains("two runners")),
        "the double-run went uncaught: {:?}",
        report.violations
    );
}
