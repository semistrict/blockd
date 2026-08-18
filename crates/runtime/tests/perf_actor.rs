//! Bare actor-task profiles over the deterministic model world.

#![allow(clippy::cast_precision_loss)] // presentation math
#![allow(clippy::disallowed_methods, clippy::disallowed_types)] // wall profile only

use std::time::Instant;

use blockd_core::hostmeta::{Counters, HostConfig};
use blockd_core::journal::VolumeConfig;
use blockd_core::types::{HostId, millis};
use blockd_exec::rng::Ppm;
use blockd_sim::harness::{FaultPlan, HarnessConfig, RunReport, run, run_capture_profile};
use blockd_sim::model::{BlobDevConfig, StoreConfig};

const DRAIN_PAGES_PER_POLL: u64 = 64;

fn config(volumes: u16, pages: u32) -> HarnessConfig {
    HarnessConfig {
        host: HostConfig {
            archive: blockd_core::hostmeta::ArchivePolicy::default(),
            host: HostId(0),
            cache_pages: usize::from(volumes)
                .saturating_mul(pages as usize)
                .saturating_mul(2)
                .max(1_024),
            writeback_interval: millis(5),
            backup_retry: millis(20),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 500,
            replica_placement: None,
        },
        passive_disk_capacity: None,
        blobs: BlobDevConfig {
            read_latency_min: 1,
            read_latency_max: 1,
            write_latency_min: 1,
            write_latency_max: 1,
            ns_per_byte: 0,
            full_window: None,
            handoff_full_writes: 0,
            eio_at: None,
        },
        store: StoreConfig {
            latency_min: 1,
            latency_max: 1,
            ns_per_byte: 0,
        },
        volume_count: volumes,
        volume: VolumeConfig::data(pages),
        horizon: millis(15),
        think: (50_000, 50_000),
        sync_share: Some(Ppm(20_000)),
        hot_pages: Some((Ppm(800_000), pages.min(32))),
        checkpoint_interval: None,
        faults: FaultPlan::default(),
        corrupt_fills: false,
        drop_write_protect: false,
    }
}

fn assert_paths_ran(report: &RunReport) {
    let Counters {
        zero_fills,
        records_written,
        pages_flushed,
        ..
    } = report.counters;
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    assert!(zero_fills > 0, "no first-touch zero fills happened");
    assert!(records_written > 0, "writeback never ran");
    assert!(pages_flushed > 0, "capture never flushed a page");
    assert!(report.actor_polls > 0, "actor tasks did no work");
    assert!(
        report.max_page_reads_in_poll <= DRAIN_PAGES_PER_POLL,
        "one poll read {} pages; cooperative capture bound is {DRAIN_PAGES_PER_POLL}",
        report.max_page_reads_in_poll
    );
}

#[test]
fn profile_huge_volume_capture_stall() {
    let full = std::env::var_os("BLOCKD_PERF_FULL").is_some();
    let dirty_pages = if full { 300_000 } else { 10_000 };
    let mut profile = config(1, dirty_pages);
    profile.host.cache_pages = dirty_pages as usize + 1_024;

    let started = Instant::now();
    let report = run_capture_profile(7, profile, dirty_pages);
    let wall = started.elapsed();
    assert_paths_ran(&report);
    assert_eq!(report.completed_ops, u64::from(dirty_pages));
    assert!(
        report.counters.pages_flushed >= u64::from(dirty_pages),
        "capture flushed only {} of {dirty_pages} dirty pages",
        report.counters.pages_flushed
    );
    eprintln!(
        "capture profile: {dirty_pages} dirty pages, {} polls, max {} page reads/poll, {wall:.1?}",
        report.actor_polls, report.max_page_reads_in_poll
    );
}

#[test]
fn profile_actor_poll_ceiling() {
    for volumes in [1_u16, 100, 300] {
        let started = Instant::now();
        let report = run(11, config(volumes, 256));
        let wall = started.elapsed();
        assert_paths_ran(&report);
        assert!(
            report.counters.wp_faults > 0,
            "no write-protect faults happened"
        );
        assert!(report.completed_ops > u64::from(volumes));
        let mean_poll_ns = wall.as_nanos() / u128::from(report.actor_polls);
        eprintln!(
            "actor profile: {volumes:>3} volumes, {} ops, {} polls, {wall:.1?}, mean {mean_poll_ns}ns/poll",
            report.completed_ops, report.actor_polls
        );
        assert!(
            mean_poll_ns < 1_000_000,
            "mean actor poll cost exceeded 1ms: {mean_poll_ns}ns"
        );
    }
}
