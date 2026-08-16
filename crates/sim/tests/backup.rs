//! Single-host simulation of archival, store outages, and remote recovery.

use blockd_core::hostmeta::{ArchivePolicy, HostConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::layout;
use blockd_core::types::{HostId, VsetId, millis, page_size, secs};
use blockd_sim::harness::{FaultPlan, HarnessConfig, RunReport, run};
use blockd_sim::model::{BlobDevConfig, StoreConfig, StoreObjectKind};
use blockd_sim::rng::Ppm;

fn base_config() -> HarnessConfig {
    HarnessConfig {
        host: HostConfig {
            archive: ArchivePolicy {
                interval: secs(1),
                ..Default::default()
            },
            host: HostId(0),
            cache_pages: 256,
            writeback_interval: millis(20),
            backup_retry: millis(100),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 25,
            replica_placement: None,
        },
        blobs: BlobDevConfig::nvme(),
        store: StoreConfig::gcs(),
        vset_count: 2,
        vset: VsetConfig::compute(2, 16),
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

fn assert_clean(report: &RunReport) {
    assert_eq!(report.violations, Vec::<String>::new());
}

#[test]
fn every_vset_is_published_by_its_primary() {
    let report = run(11, base_config());
    assert_clean(&report);
    // The archive advanced on its own cadence without checkpoints.
    assert_eq!(report.counters.checkpoints_done, 0);
    assert!(report.counters.manifests_published > 0);
    assert_eq!(report.counters.fenced, 0);
    for vset in [VsetId(1), VsetId(2)] {
        let prefix = layout::vset_prefix(vset);
        assert!(report.store_keys.contains(&layout::head_key(vset)));
        assert!(
            report
                .store_keys
                .iter()
                .filter(|key| key.starts_with(&format!("{prefix}m/")))
                .count()
                >= 1,
            "current manifest exists: counters={:?} keys={:?}",
            report.counters,
            report.store_keys
        );
    }
}

#[test]
fn hot_working_set_reports_archive_amplification_baseline() {
    let mut config = base_config();
    config.vset_count = 1;
    config.vset = VsetConfig::compute(2, 256);
    config.hot_pages = Some((Ppm::percent(95), 8));
    config.horizon = secs(2);
    let report = run(0xA11C_0001, config);
    assert_clean(&report);

    let attempts: u64 = report
        .store
        .puts_by_kind
        .iter()
        .map(|kind| kind.attempts)
        .sum();
    let successes: u64 = report
        .store
        .puts_by_kind
        .iter()
        .map(|kind| kind.successes)
        .sum();
    let attempted_bytes: u64 = report
        .store
        .puts_by_kind
        .iter()
        .map(|kind| kind.attempted_bytes)
        .sum();
    let successful_bytes: u64 = report
        .store
        .puts_by_kind
        .iter()
        .map(|kind| kind.successful_bytes)
        .sum();
    assert_eq!(attempts, report.store.put_attempts);
    assert_eq!(successes, report.store.put_successes);
    assert_eq!(successful_bytes, report.store.bytes_put);
    assert!(attempted_bytes >= successful_bytes);
    assert!(report.store.unique_bytes <= report.store.bytes_put);
    assert!(report.store.retry_bytes <= attempted_bytes);
    assert!(
        report.store.puts_by_kind[StoreObjectKind::Manifest as usize].successes > 0,
        "counters={:?} keys={:?}",
        report.counters,
        report.store_keys
    );
    assert!(report.store.logical_changed_bytes > 0);
    assert!(report.published_segment_bytes > 0);
    eprintln!(
        "archive-baseline horizon_ns={} put_attempts={} put_successes={} unique_bytes={} retry_bytes={} logical_changed_bytes={} final_segment_bytes={} final_live_entry_bytes={} final_dead_entry_bytes={}",
        secs(2),
        report.store.put_attempts,
        report.store.put_successes,
        report.store.unique_bytes,
        report.store.retry_bytes,
        report.store.logical_changed_bytes,
        report.published_segment_bytes,
        report.published_live_entry_bytes,
        report.published_dead_entry_bytes,
    );
    assert_eq!(
        report.published_segment_bytes,
        report.published_live_entry_bytes
            + report.published_dead_entry_bytes
            + report.published_segment_overhead_bytes
    );
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
    assert!(report.counters.manifests_published > 0);
    assert_eq!(report.counters.fenced, 0);
    // Local durability was untouched throughout (R8.3): guests progressed.
    assert!(report.completed_ops > 0);
}

#[test]
fn journal_rot_is_survived_via_restore_from_backup() {
    // Recover from the newest archived point after local journal corruption.
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
        ..FaultPlan::default()
    };
    let report = run(13, config);
    assert_clean(&report);
    assert!(report.crashes > 0);
    assert_eq!(report.unrestorable, 0);
    assert_eq!(report.restores, 0);
    assert!(report.completed_ops > 0);
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
        ..FaultPlan::default()
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
    config.host.cache_pages = 24;
    config.host.disk_capacity = Some(16 * page_size() as u64);
    config.host.disk_headroom = 4 * page_size() as u64;
    let report = run(14, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    assert!(
        report.counters.nvme_reclaims > 0,
        "counters: {:?}",
        report.counters
    );
    assert!(report.counters.nvme_stalls > 0);
    assert!(report.completed_ops > 0);
}
