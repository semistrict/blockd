//! Negative tests at the daemon/world level: deliberately break one rule
//! and assert the oracle CATCHES it. A green suite is only meaningful if
//! these fail red — an oracle that misses planted misbehavior would pass
//! every honest run vacuously.

use blockd_core::hostmeta::HostConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{HostId, millis, secs};
use blockd_sim::harness::{FaultPlan, HarnessConfig, Sabotage, run};
use blockd_sim::world::blobdev::BlobDevConfig;
use blockd_sim::world::store::StoreConfig;

fn base_config() -> HarnessConfig {
    HarnessConfig {
        daemon: HostConfig {
            archive: blockd_core::hostmeta::ArchivePolicy::default(),
            host: HostId(0),
            cache_pages: 256,
            writeback_interval: millis(20),
            backup_retry: millis(200),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 25,
            replica_placement: None,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        vset_count: 2,
        vset_config: VsetConfig::compute(2, 16),
        horizon: secs(2),
        think: (millis(1), millis(5)),
        checkpoint_interval: None,
        faults: FaultPlan::none(),
        sabotage: None,
        guest_sync_share: None,
        guest_hot_pages: None,
        rot_records_at: vec![],
        crash_at: vec![],
    }
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
            .any(|v| v.contains("stale or foreign bytes")),
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
    config.vset_count = 1;
    config.daemon.cache_pages = 8;
    config.horizon = millis(500);
    config.checkpoint_interval = Some(millis(100));
    config.crash_at = vec![millis(250)];
    config.faults = FaultPlan {
        crash_mean_interval: 0,
        restart_delay: (millis(10), millis(10)),
        bitflip_mean_interval: 0,
        journal_bitflip_mean_interval: 0,
        store_outage: None,
    };
    config.sabotage = Some(Sabotage::DropWriteProtect);
    let caught = (0..16).any(|seed| !run(seed, config.clone()).violations.is_empty());
    assert!(
        caught,
        "stale captures from dropped write protection went unnoticed"
    );
}

/// The fenced head is an independent exclusion boundary: even if the local
/// handoff marker lies about durability, a crashed source may not recover as
/// a second runner after the destination claims ownership.
#[test]
fn head_fence_prevents_double_run_after_a_lied_about_handoff() {
    let config = blockd_sim::cluster::ClusterConfig {
        hosts: 2,
        vset_count: 1,
        vset_config: VsetConfig::compute(2, 16),
        daemon: HostConfig {
            archive: blockd_core::hostmeta::ArchivePolicy::default(),
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
        crash_hosts_at: vec![(millis(1575), 0)],
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
        migrate_at: vec![(millis(1_000), blockd_core::types::VsetId(1), 1)],
        sabotage: Some(Sabotage::EagerHandoffAck),
        guest_sync_share: None,
    };
    let report = blockd_sim::cluster::run(7, config);
    // The migration completed and the source crashed in the old danger
    // window, but the head fence still excludes its stale local recovery.
    assert_eq!(report.migrations, 1);
    assert_eq!(report.violations, Vec::<String>::new());
    assert_eq!(report.guest_deaths, 0);
}
