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
    HarnessConfig {
        daemon: DaemonConfig {
            host: HostId(0),
            cache_pages: 256,
            writeback_interval: millis(20),
            backup_retry: millis(200),
            disk_capacity: None,
            disk_headroom: 0,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        vset_count: 2,
        backed_vsets: 0,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            backed_up: false,
        },
        horizon: secs(2),
        think: (millis(1), millis(5)),
        checkpoint_interval: None,
        faults: FaultPlan::none(),
        sabotage: None,
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
