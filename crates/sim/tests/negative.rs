//! Fault injection tests that verify oracle violations are detected.

use blockd_core::hostmeta::HostConfig;
use blockd_core::journal::VolumeConfig;
use blockd_core::types::{HostId, millis, secs};
use blockd_sim::cluster::Sabotage;
use blockd_sim::harness::{FaultPlan, HarnessConfig, run};
use blockd_sim::model::{BlobDevConfig, StoreConfig};

fn base_config() -> HarnessConfig {
    HarnessConfig {
        host: HostConfig {
            archive: blockd_core::hostmeta::ArchivePolicy::default(),
            host: HostId::new(0),
            cache_pages: 256,
            writeback_interval: millis(20),
            backup_retry: millis(200),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 25,
            cluster_placement: None,
        },
        passive_disk_capacity: None,
        blobs: BlobDevConfig::nvme(),
        store: StoreConfig::gcs(),
        volume_count: 2,
        volume: VolumeConfig::data(16),
        horizon: secs(2),
        think: (millis(1), millis(5)),
        checkpoint_interval: None,
        faults: FaultPlan::none(),
        sync_share: None,
        hot_pages: None,
        corrupt_fills: false,
        drop_write_protect: false,
    }
}

#[test]
fn oracle_catches_corrupted_fills() {
    // R8.1's check is not vacuous: bytes that differ from ghost truth by a
    // single bit, delivered through the only door into guest memory, are
    // flagged the moment any guest observes them.
    let mut config = base_config();
    config.corrupt_fills = true;
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
    config.volume_count = 1;
    config.host.cache_pages = 8;
    config.horizon = millis(500);
    config.checkpoint_interval = Some(millis(100));
    config.faults.crash_at = vec![millis(250)];
    config.faults = FaultPlan {
        crash_mean_interval: 0,
        restart_delay: (millis(10), millis(10)),
        bitflip_mean_interval: 0,
        journal_bitflip_mean_interval: 0,
        store_outage: None,
        ..FaultPlan::default()
    };
    config.drop_write_protect = true;
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
        volume_count: 1,
        volume_config: VolumeConfig::memory(16),
        daemon: HostConfig {
            archive: blockd_core::hostmeta::ArchivePolicy::default(),
            host: HostId::new(0),
            cache_pages: 128,
            writeback_interval: millis(20),
            backup_retry: millis(100),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 25,
            cluster_placement: None,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::gcs(),
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
        drop_peer: None,
        race_restore: false,
        migrate_at: vec![(millis(1_000), blockd_core::types::VolumeId(1), 1)],
        sabotage: Some(Sabotage::EagerHandoffAck),
        guest_sync_share: None,
        membership_events: vec![],
    };
    let report = blockd_sim::cluster::run(7, config);
    // The migration completed and the source crashed in the old danger
    // window, but the head fence still excludes its stale local recovery.
    assert_eq!(report.migrations, 1);
    assert_eq!(report.violations, Vec::<String>::new());
    assert_eq!(report.guest_deaths, 0);
}
