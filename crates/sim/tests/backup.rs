//! Milestone 4, single-host slice: the backup pipeline (R4.2), the R4.4
//! zero-objects contract, store outages (R8.3), and restore-from-store after
//! local durable state is destroyed (R6.1 on one host). Exact assertions —
//! runs are deterministic.

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::layout;
use blockd_core::types::{HostId, VsetId, millis, secs};
use blockd_sim::harness::{FaultPlan, HarnessConfig, RunReport, run};
use blockd_sim::world::blobdev::BlobDevConfig;
use blockd_sim::world::store::StoreConfig;

fn base_config() -> HarnessConfig {
    HarnessConfig {
        daemon: DaemonConfig {
            host: HostId(0),
            cache_pages: 256,
            writeback_interval: millis(20),
            backup_retry: millis(100),
            disk_capacity: None,
            disk_headroom: 0,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        vset_count: 2,
        backed_vsets: 1,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            backed_up: false, // overridden per vset by `backed_vsets`
        },
        horizon: secs(2),
        think: (millis(1), millis(5)),
        checkpoint_interval: None,
        faults: FaultPlan::none(),
        sabotage: None,
        guest_sync_share: None,
    }
}

fn assert_clean(report: &RunReport) {
    assert_eq!(report.violations, Vec::<String>::new());
}

#[test]
fn backup_flows_continuously_and_unbacked_vsets_write_nothing() {
    let report = run(11, base_config());
    assert_clean(&report);
    // R4.2: backup flowed continuously without checkpoints (R3.2).
    assert_eq!(report.counters.checkpoints_done, 0);
    assert_eq!(report.counters.manifests_published, 6);
    assert_eq!(report.counters.fenced, 0);
    // The store holds the backed vset's head + exactly one manifest and the
    // live segments — nothing of the non-backed vset, ever (R4.4).
    let backed_prefix = layout::vset_prefix(VsetId(1));
    assert!(
        report
            .store_keys
            .iter()
            .all(|k| k.starts_with(&backed_prefix)),
        "foreign keys in store: {:?}",
        report.store_keys
    );
    assert!(report.store_keys.contains(&layout::head_key(VsetId(1))));
    let manifests = report
        .store_keys
        .iter()
        .filter(|k| k.starts_with(&format!("{backed_prefix}m/")))
        .count();
    assert_eq!(manifests, 1, "superseded manifests are reclaimed (R4.5)");
    assert_eq!(report.store_keys.len(), 33);
}

#[test]
fn store_outage_queues_backups_and_drains_after() {
    // R8.3: an outage stalls backup, never local durability; queued copies
    // drain when the store returns.
    let mut config = base_config();
    config.horizon = secs(3);
    config.faults.store_outage = Some((millis(500), millis(1800)));
    let report = run(12, config);
    assert_clean(&report);
    assert_eq!(report.counters.store_retries, 28);
    assert_eq!(report.counters.manifests_published, 5);
    assert_eq!(report.counters.fenced, 0);
    // Local durability was untouched throughout (R8.3): guests progressed.
    assert_eq!(report.completed_ops, 1938);
}

#[test]
fn journal_rot_is_survived_via_restore_from_backup() {
    // Milestone 3 could not survive journal bit rot; with the backup tier
    // the vset comes back from the store (R6.1) at the newest backed-up
    // point, with sync loss bounded by the backup lag (R4.3).
    let mut config = base_config();
    config.vset_count = 1;
    config.horizon = secs(4);
    config.checkpoint_interval = Some(millis(300));
    config.faults = FaultPlan {
        crash_mean_interval: millis(600),
        restart_delay: (millis(10), millis(200)),
        bitflip_mean_interval: 0,
        journal_bitflip_mean_interval: millis(400),
        store_outage: None,
    };
    let report = run(13, config);
    assert_clean(&report);
    assert_eq!(report.crashes, 7);
    assert_eq!(report.unrestorable, 0);
    assert_eq!(report.restores, 0);
    assert_eq!(report.completed_ops, 1296);
}

#[test]
fn backed_runs_replay_byte_for_byte() {
    let mut config = base_config();
    config.horizon = secs(2);
    config.checkpoint_interval = Some(millis(400));
    config.faults = FaultPlan {
        crash_mean_interval: millis(700),
        restart_delay: (millis(10), millis(200)),
        bitflip_mean_interval: 0,
        journal_bitflip_mean_interval: millis(900),
        store_outage: Some((millis(400), millis(900))),
    };
    for seed in [11, 21] {
        let a = run(seed, config.clone());
        let b = run(seed, config.clone());
        assert_eq!(a, b, "seed {seed} diverged on replay");
    }
}

#[test]
fn nvme_pressure_reclaims_backed_segments_and_never_corrupts() {
    // R2.7, droppable class: segments the backup already holds are dropped
    // under disk pressure; refaults refetch from the store (R2.3). Slowness
    // and loud pressure — never corruption, never a kill.
    let mut config = base_config();
    config.vset_count = 1;
    config.daemon.cache_pages = 24;
    config.daemon.disk_capacity = Some(256 * 1024);
    config.daemon.disk_headroom = 64 * 1024;
    let report = run(14, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.counters.nvme_reclaims, 26);
    assert_eq!(report.counters.nvme_stalls, 53);
    assert_eq!(report.completed_ops, 355);
}

#[test]
fn nvme_exhaustion_without_backup_stalls_loudly_and_kills_nothing() {
    // R2.7's irreducible residue: nothing is droppable without the backup
    // tier, so at exhaustion writeback stalls, syncs wait, guests slow —
    // and nobody dies, nothing corrupts.
    let mut config = base_config();
    config.vset_count = 1;
    config.backed_vsets = 0;
    config.daemon.disk_capacity = Some(96 * 1024);
    config.daemon.disk_headroom = 16 * 1024;
    let report = run(15, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.counters.nvme_reclaims, 0, "nothing is droppable");
    assert_eq!(report.counters.nvme_stalls, 181);
    assert_eq!(report.completed_ops, 131);
    assert_eq!(report.store_keys.len(), 0, "R4.4 holds under pressure too");
}
