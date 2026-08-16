//! Single-host simulation of guest load, persistence, recovery, pressure, and
//! corruption.

use blockd_core::hostmeta::HostConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{micros, millis, page_size, secs};
use blockd_sim::harness::{FaultPlan, HarnessConfig, RunReport, run};
use blockd_sim::rng::Ppm;

fn base_config() -> HarnessConfig {
    blockd_sim::presets::single_host_base()
}

fn assert_clean(report: &RunReport) {
    assert_eq!(report.violations, Vec::<String>::new());
}

#[test]
fn quiet_run_serves_and_syncs_without_incident() {
    let report = run(1, base_config());
    assert_clean(&report);
    assert_eq!(report.crashes, 0);
    assert_eq!(report.guest_deaths, 0);
    assert!(
        report.completed_ops > 1_000,
        "completed only {} operations: {report:?}",
        report.completed_ops
    );
    assert_eq!(report.counters.faults_unservable, 0);
    assert_eq!(report.counters.pressure_waits, 0);
    assert_eq!(report.counters.checkpoints_done, 0);
    assert!(report.counters.syncs_acked > 100);
    assert_eq!(report.counters.guest_rejected, 0);
}

#[test]
fn runs_replay_byte_for_byte() {
    let mut config = base_config();
    config.checkpoint_interval = Some(millis(250));
    config.faults = FaultPlan {
        crash_mean_interval: millis(400),
        restart_delay: (millis(10), millis(200)),
        bitflip_mean_interval: millis(500),
        journal_bitflip_mean_interval: 0,
        store_outage: None,
        ..FaultPlan::default()
    };
    for seed in [1, 2, 7] {
        let a = run(seed, config.clone());
        let b = run(seed, config.clone());
        assert_eq!(a, b, "seed {seed} diverged on replay");
    }
}

#[test]
fn crash_storm_with_checkpoints_resumes() {
    let mut config = base_config();
    config.horizon = secs(4);
    config.checkpoint_interval = Some(millis(250));
    config.faults = FaultPlan {
        crash_mean_interval: millis(400),
        restart_delay: (millis(10), millis(200)),
        bitflip_mean_interval: 0,
        journal_bitflip_mean_interval: 0,
        store_outage: None,
        ..FaultPlan::default()
    };
    let report = run(3, config);
    assert_clean(&report);
    assert!(report.crashes > 0);
    assert!(report.resumes + report.cold_boots > 0);
    assert!(report.cold_boots > 0);
    assert_eq!(report.unrestorable, report.restores);
    assert_eq!(report.guest_deaths, 0);
    assert!(report.completed_ops > 1_000);
}

#[test]
fn crash_storm_without_checkpoints_cold_boots_at_sync_consistency() {
    // R3.2: nothing relies on checkpoints — recovery still works, as cold
    // boots honoring sync ordering.
    let mut config = base_config();
    config.horizon = secs(4);
    config.faults = FaultPlan {
        crash_mean_interval: millis(400),
        restart_delay: (millis(10), millis(200)),
        bitflip_mean_interval: 0,
        journal_bitflip_mean_interval: 0,
        store_outage: None,
        ..FaultPlan::default()
    };
    let report = run(4, config);
    assert_clean(&report);
    assert!(report.crashes > 0);
    assert_eq!(report.resumes, 0);
    assert!(report.cold_boots > 0);
    assert_eq!(report.guest_deaths, 0);
    assert!(
        report.completed_ops > 750,
        "completed only {} operations: {report:?}",
        report.completed_ops
    );
}

#[test]
fn repeated_checkpoints_accrue_no_storage_debt() {
    // R3.3/R3.4: checkpoint forever; storage stays proportional to live
    // data, not checkpoint count.
    let mut config = base_config();
    config.horizon = secs(4);
    config.checkpoint_interval = Some(millis(50));
    let report = run(5, config);
    assert_clean(&report);
    assert!(report.counters.checkpoints_done > 100);
    // R3.1: the guest-visible pause is the VMM pause round-trip only —
    // capture and persistence never extend it. Far under the 250 ms budget.
    assert!(report.max_pause_ns < millis(250));
    assert!(report.counters.records_written > report.counters.checkpoints_done);
    // Bounded by live data and the current journal tail, not checkpoint
    // count. This stays far below the number of records written.
    // One complete snapshot may also be pinned while its head update is in
    // flight. The bound therefore includes the live set plus one upload set.
    assert!(
        report.blob_count < 256,
        "retained {} blobs: {report:?}",
        report.blob_count
    );
    assert!(
        u64::try_from(report.blob_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(3)
            < report.counters.records_written,
        "blob count grew with record history: {report:?}"
    );
}

#[test]
fn one_rotated_record_copy_recovers_from_its_intact_mirror() {
    let mut config = base_config();
    config.vset_count = 1;
    config.horizon = millis(500);
    config.faults.rot_records_at = vec![(millis(250), false)];
    config.faults.crash_at = vec![millis(300)];
    config.faults.restart_delay = (millis(1), millis(1));
    let report = run(47, config);
    assert_clean(&report);
    assert_eq!(report.bitflips, 1);
    assert_eq!(report.crashes, 1);
    assert_eq!(report.unrestorable, 0);
    assert!(report.cold_boots + report.resumes > 0);
}

#[test]
fn pressure_slows_guests_but_never_kills() {
    // R2.5: cache far smaller than the combined working set. Faults wait on
    // writeback-driven eviction; everyone still progresses; nobody dies.
    let mut config = base_config();
    config.host.cache_pages = 8;
    config.vset_count = 4;
    config.vset.pages_per_volume = 32;
    let report = run(6, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    assert!(report.counters.pressure_waits > 0);
    let progressed: Vec<u64> = report.per_guest_completed.values().copied().collect();
    assert!(progressed.iter().all(|&completed| completed > 100));
}

#[test]
fn full_disk_stalls_captures_until_space_returns_without_killing_guests() {
    let mut config = base_config();
    config.horizon = secs(3);
    config.checkpoint_interval = Some(millis(200));
    config.blobs.full_window = Some((millis(600), millis(1400)));
    let report = run(21, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    assert!(report.counters.nvme_stalls > 0, "{report:?}");
    assert!(report.counters.checkpoints_done > 0, "{report:?}");
    assert!(report.counters.syncs_acked > 0, "{report:?}");
}

#[test]
fn non_capacity_write_failure_fail_stops_the_host() {
    let mut config = base_config();
    config.horizon = secs(2);
    config.blobs.eio_at = Some(millis(700));
    let report = run(23, config);
    assert_clean(&report);
    assert_eq!(report.crashes, 1, "{report:?}");
    assert_eq!(report.guest_deaths, 0, "{report:?}");
}

#[test]
fn bit_rot_never_serves_corrupt_bytes() {
    // R8.1: damaged segments must never serve bytes. Random damage may land
    // on obsolete data, but any unservable read must fail its guest loudly.
    let mut config = base_config();
    config.horizon = secs(3);
    // Small cache: evictions force refaults, so damaged segments are read.
    config.host.cache_pages = 24;
    config.faults = FaultPlan {
        crash_mean_interval: 0,
        restart_delay: (millis(10), millis(200)),
        bitflip_mean_interval: millis(150),
        journal_bitflip_mean_interval: 0,
        store_outage: None,
        ..FaultPlan::default()
    };
    let report = run(7, config);
    assert_clean(&report);
    assert!(report.bitflips > 0);
    assert_eq!(report.counters.faults_unservable, report.guest_deaths);
}

/// Adversarial rot, not Poisson luck: flip a bit in the NEWEST record —
/// the sole carrier of its newly-acked syncs — and crash before any newer
/// record can cover for it; later in the run, the same against the MIRROR
/// copy. Recovery must land at-or-past every acked sync both times
/// (R3.8): surviving this is exactly what the record's second copy
/// exists for — a single-copy journal provably fails it, and the hazard
/// originally surfaced only by an unlucky seed. Never again by luck.
#[test]
fn rot_on_either_record_copy_never_rolls_back_acked_syncs() {
    let mut config = base_config();
    config.vset_count = 1;
    config.horizon = secs(3);
    // Crash 50µs behind each flip — inside the window where the rotted
    // record is still the newest, before another record covers its syncs.
    config.faults.rot_records_at = vec![(millis(900), false), (millis(1800), true)];
    config.faults.crash_at = vec![millis(900) + micros(50), millis(1800) + micros(50)];
    let report = run(9, config);
    assert_clean(&report);
    assert_eq!(report.bitflips, 2, "both targeted flips must land");
    assert_eq!(report.crashes, 2);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.cold_boots, 2);
    assert!(report.completed_ops > 300);
}

#[test]
fn full_chaos_stays_consistent() {
    let mut config = base_config();
    config.horizon = secs(5);
    config.checkpoint_interval = Some(millis(300));
    config.faults = FaultPlan {
        crash_mean_interval: millis(500),
        restart_delay: (millis(10), millis(300)),
        bitflip_mean_interval: millis(400),
        journal_bitflip_mean_interval: 0,
        store_outage: None,
        ..FaultPlan::default()
    };
    let report = run(8, config);
    assert_clean(&report);
    assert!(report.crashes > 0);
    assert!(report.resumes + report.cold_boots > 0);
    assert_eq!(report.unrestorable, report.restores);
    assert_eq!(report.guest_deaths, 0, "{report:?}");
    assert!(report.completed_ops > 500);
}

#[test]
fn scale_run_hosts_many_overcommitted_vsets() {
    // R1.3, proportionally scaled: one host, 100 concurrently live vsets,
    // memory overcommitted ~4x (100 vsets x 48 pages of working set
    // against a 1200-page cache). Thin provisioning + writeback-driven
    // eviction keep every guest progressing (R2.4/R2.5): nobody dies,
    // nothing corrupts, pressure only slows.
    let mut config = base_config();
    config.vset_count = 100;
    config.host.cache_pages = 1200;
    config.horizon = secs(1);
    let report = run(17, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.per_guest_completed.len(), 100);
    let laggard = report.per_guest_completed.values().min().copied();
    assert!(
        laggard.is_some_and(|ops| ops > 0),
        "a guest starved under overcommit: {:?}",
        report.per_guest_completed
    );
    // The overcommit was real: eviction pressure actually bit.
    assert!(report.counters.pressure_waits > 0 || report.counters.fills > 0);
}

/// The standing long-haul suite (R10.1): full-nemesis runs over the
/// committed regression seed corpus. Every seed that ever exposed a bug
/// belongs in this list, forever.
/// The step-cost bound (design flaw 2): one writeback tick starts a
/// bounded, rotating share of the fleet's captures — never every vset at
/// once — and each capture reads at most one drain batch in any step (a
/// larger set arms an incremental drain and reads NOTHING in the tick).
/// Wall time cannot pass inside a sim step; counted work units can, and
/// this pins them: a step's reads are O(slots × batch), independent of
/// both fleet size and dirty-set size.
#[test]
fn writeback_work_per_step_stays_bounded_at_fleet_scale() {
    let mut config = base_config();
    config.vset_count = 64;
    config.horizon = secs(3);
    config.vset.pages_per_volume = 24;
    let report = run(5, config);
    assert_clean(&report);
    // 8 rotation slots × the 64-page synchronous-capture ceiling; a vset's
    // full 72-page set never lands in one step at all.
    let step_ceiling: u64 = 8 * 64;
    assert!(
        report.max_page_reads_in_poll <= step_ceiling,
        "one step read {} pages — a capture or tick exceeded its batch",
        report.max_page_reads_in_poll
    );
    // Non-vacuous: the fleet's total dirty work far exceeded one step's
    // bound, so the pacing genuinely spread it.
    assert!(
        report.counters.pages_flushed > step_ceiling,
        "fleet never generated more work than one step's budget"
    );
}

/// 2a-full, the residual after the tick stagger: ONE vset with a huge
/// dirty set must not cost O(dirty) inside any single step. The capture
/// arms the whole set behind write protection in one cheap read-free
/// step, drains a bounded batch per continuation step, and a guest write
/// to an armed-but-unread page is captured immediately, out of order
/// (copy-on-fault) — so the record remains an exact cut at the arm
/// instant while the guest keeps running through the drain. The oracle's
/// byte checks and durability accounting hold as everywhere else.
#[test]
fn huge_dirty_sets_capture_incrementally_with_copy_on_fault() {
    let mut config = base_config();
    config.vset_count = 1;
    config.host.cache_pages = 4096;
    config.vset.pages_per_volume = 600; // 3 volumes × 600 pages
    config.horizon = secs(2);
    // A hot writer: the dirty set between writeback ticks dwarfs one
    // drain batch, and writes keep landing while the drain runs. No
    // syncs — a pending sync parks the guest until the capture's record
    // lands, and this test needs the guest AWAKE mid-drain.
    config.think = (micros(5), micros(50));
    config.sync_share = Some(Ppm::NEVER);
    let report = run(9, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    // No step — arm, drain, fault, or tick — read more than one batch.
    assert!(
        report.max_page_reads_in_poll <= 64,
        "one step read {} pages — a capture read past its batch",
        report.max_page_reads_in_poll
    );
    // The load was real: far more than one batch's worth was captured…
    assert!(
        report.counters.pages_flushed > 10 * 64,
        "only {} pages flushed — the dirty set never exceeded a batch",
        report.counters.pages_flushed
    );
    // …and the guest raced a drain into copy-on-fault, repeatedly.
    assert!(
        report.counters.cow_captures > 0,
        "no copy-on-fault ever fired — drains never overlapped guest writes"
    );
}

#[test]
fn chaos_seed_corpus_stays_consistent() {
    for seed in [3, 8, 21, 29, 47, 63, 77, 90, 104, 131] {
        let report = run(seed, blockd_sim::presets::single_host_chaos());
        assert_eq!(
            report.violations,
            Vec::<String>::new(),
            "seed {seed} violated an invariant"
        );
    }
}

/// Journal metadata must stay bounded as a large vset fills. Page locations
/// are rebuilt from BLX footers and are never persisted as a separate map.
#[test]
fn large_vsets_keep_journal_metadata_bounded() {
    let config = HarnessConfig {
        host: HostConfig {
            // A warm cache: the test measures metadata cost, not thrash.
            cache_pages: 32_768,
            ..base_config().host
        },
        vset_count: 1,
        vset: VsetConfig::compute(2, 8_000),
        think: (micros(5), micros(25)),
        horizon: secs(1),
        checkpoint_interval: Some(millis(300)),
        // A writeback-shaped workload under continuous dirtying.
        sync_share: Some(Ppm(1_000)),
        ..base_config()
    };
    let report = run(41, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    // Enough pages and records to expose metadata that scales with vset size.
    assert!(
        report.counters.pages_flushed > 6_000,
        "workload too small to expose metadata growth: {} pages flushed",
        report.counters.pages_flushed
    );
    assert!(
        report.counters.records_written > 25,
        "only {} records",
        report.counters.records_written
    );
    // No journal record contains a page map, so file references—not page
    // count—bound its size.
    assert!(
        report.max_record_blob_bytes < 128 * 1024,
        "largest journal record was {} bytes — scaled with the page map",
        report.max_record_blob_bytes
    );
    // Total journal metadata remains proportional to changed data and record
    // count. Rewriting a full page map on every capture exceeds this budget.
    let budget = 900 * report.counters.pages_flushed + 16_384 * report.counters.records_written;
    assert!(
        report.map_bytes_written < budget,
        "journal metadata cost {} bytes ({} flushed pages, {} records; budget {budget}) — \
         rewriting a page map per capture",
        report.map_bytes_written,
        report.counters.pages_flushed,
        report.counters.records_written,
    );
}

/// R2.7/R4.5: disk space tracks LIVE data, not write history. The workload
/// is the pathological shape for write-once segments: a small hot set
/// churns every interval (its old entries die immediately) while cold
/// pages arrive once and survive — so every segment ends up mostly dead
/// but pinned by its few cold survivors. Without compaction the device
/// accumulates one mostly-dead segment per capture, unbounded in the
/// horizon; with it, dead bytes are bounded by a constant factor of live.
#[test]
fn steady_overwrites_dont_amplify_disk_space() {
    let config = HarnessConfig {
        host: HostConfig {
            cache_pages: 8_192,
            ..base_config().host
        },
        vset_count: 1,
        vset: VsetConfig::compute(2, 4_096),
        think: (micros(10), micros(50)),
        horizon: secs(2),
        checkpoint_interval: None,
        // Writeback-shaped (syncs rare), 90% of picks in a 32-page hot
        // set: each capture's segment is mostly hot pages that the next
        // capture supersedes, plus a few cold survivors that pin it.
        sync_share: Some(Ppm(1_000)),
        hot_pages: Some((Ppm::percent(90), 32)),
        ..base_config()
    };
    let report = run(43, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    // The workload genuinely has the pathological shape: many captures,
    // far more bytes written than the working set holds.
    assert!(
        report.counters.records_written > 50,
        "{} records",
        report.counters.records_written
    );
    assert!(
        report.counters.pages_flushed > 9_000,
        "only {} pages flushed — not enough churn to expose amplification",
        report.counters.pages_flushed
    );
    // The structural bound compaction buys: every surviving segment is
    // majority-live, so disk ≤ 2 × live plus the not-yet-compacted tail.
    // History (≈7 MB of segment writes per simulated second) must NOT
    // accumulate — without compaction this run ends at 20.6 MB and grows
    // linearly with the horizon.
    assert!(
        report.seg_bytes_end
            < 2 * report.seg_live_bytes_end + 1_500_000 * page_size() as u64 / 4096,
        "{} segment bytes on disk for {} live — space amplifying with history",
        report.seg_bytes_end,
        report.seg_live_bytes_end
    );
    // And the daemon's live accounting can't be excusing the bound: the
    // whole vset is 3 volumes × 4096 pages at ≤ ~700 compressed-framed
    // bytes each ≈ 8.6 MB ceiling, and the measured end state sits well
    // under it (live 4.7 MB, disk 7.2 MB at seed 43).
    assert!(
        report.seg_live_bytes_end < 5_500_000 * page_size() as u64 / 4096,
        "{} live bytes — accounting inflated past the working set",
        report.seg_live_bytes_end
    );
    assert!(
        report.seg_bytes_end < 9_000_000 * page_size() as u64 / 4096,
        "{} segment bytes on disk at end",
        report.seg_bytes_end
    );
    // Compaction did the reclaiming, not luck.
    assert!(
        report.counters.segs_compacted > 0,
        "no segment was ever compacted"
    );
}
