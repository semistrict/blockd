//! Milestone 3: the real daemon under the single-host harness — guest load,
//! writeback, syncs, checkpoints, daemon crashes with torn writes, recovery
//! verdicts, pressure, bit rot. Every run is deterministic, so assertions
//! are exact; any drift is a real behavior change.

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{micros, millis, secs};
use blockd_sim::harness::{FaultPlan, HarnessConfig, RunReport, run};
use blockd_sim::rng::Ppm;

/// The library preset, by reference: the `sweep` binary drives the same
/// schedules, so corpus and sweep can never drift apart.
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
    assert_eq!(report.completed_ops, 1947);
    assert_eq!(report.counters.faults_unservable, 0);
    assert_eq!(report.counters.pressure_waits, 0);
    assert_eq!(report.counters.checkpoints_done, 0);
    assert_eq!(report.counters.syncs_acked, 274);
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
    };
    let report = run(3, config);
    assert_clean(&report);
    assert_eq!(report.crashes, 12);
    assert_eq!(report.resumes, 11);
    assert_eq!(report.cold_boots, 25);
    assert_eq!(report.unrestorable, 0);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.completed_ops, 3388);
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
    };
    let report = run(4, config);
    assert_clean(&report);
    assert_eq!(report.crashes, 9);
    assert_eq!(report.resumes, 0);
    assert_eq!(report.cold_boots, 27);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.completed_ops, 2987);
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
    assert_eq!(report.counters.checkpoints_done, 240);
    // R3.1: the guest-visible pause is the VMM pause round-trip only —
    // capture and persistence never extend it. Far under the 250 ms budget.
    assert_eq!(report.max_pause_ns, 199_414);
    assert_eq!(report.counters.records_written, 1630);
    // Bounded by live data: at worst ~one segment per live page plus two
    // records per vset (48 pages × 3 vsets ⇒ well under 150), an order of
    // magnitude below records_written — not growing with checkpoint count.
    assert_eq!(report.blob_count, 57);
}

#[test]
fn pressure_slows_guests_but_never_kills() {
    // R2.5: cache far smaller than the combined working set. Faults wait on
    // writeback-driven eviction; everyone still progresses; nobody dies.
    let mut config = base_config();
    config.daemon.cache_pages = 8;
    config.vset_count = 4;
    config.vset_config.pages_per_volume = 32;
    let report = run(6, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.counters.pressure_waits, 259);
    let progressed: Vec<u64> = report.per_guest_completed.values().copied().collect();
    assert_eq!(progressed, [536, 542, 524, 528]);
}

#[test]
fn bit_rot_kills_loudly_and_only_where_injected() {
    // R8.1: damaged segments must never serve bytes — the affected guest
    // fails loudly, everyone else is untouched.
    let mut config = base_config();
    config.horizon = secs(3);
    // Small cache: evictions force refaults, so damaged segments are read.
    config.daemon.cache_pages = 24;
    config.faults = FaultPlan {
        crash_mean_interval: 0,
        restart_delay: (millis(10), millis(200)),
        bitflip_mean_interval: millis(150),
        journal_bitflip_mean_interval: 0,
        store_outage: None,
    };
    let report = run(7, config);
    assert_clean(&report);
    assert_eq!(report.bitflips, 16);
    assert_eq!(report.guest_deaths, 3);
    assert_eq!(report.counters.faults_unservable, 3);
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
    config.rot_records_at = vec![(millis(900), false), (millis(1800), true)];
    config.crash_at = vec![millis(900) + micros(50), millis(1800) + micros(50)];
    let report = run(9, config);
    assert_clean(&report);
    assert_eq!(report.bitflips, 2, "both targeted flips must land");
    assert_eq!(report.crashes, 2);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.cold_boots, 2);
    assert_eq!(report.completed_ops, 687);
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
    };
    let report = run(8, config);
    assert_clean(&report);
    assert_eq!(
        (
            report.crashes,
            report.resumes,
            report.cold_boots,
            report.unrestorable,
            report.guest_deaths,
            report.completed_ops,
        ),
        (10, 17, 13, 0, 20, 3000)
    );
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
    config.daemon.cache_pages = 1200;
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
/// The step-cost bound (design flaw 2): one writeback tick captures a
/// bounded, rotating share of the fleet — never every vset at once — so a
/// step's page-read work is O(slots × working set), not O(fleet). Wall
/// time cannot pass inside a sim step; counted work units can, and this
/// pins them.
#[test]
fn writeback_work_per_step_stays_bounded_at_fleet_scale() {
    let mut config = base_config();
    config.vset_count = 64;
    config.horizon = secs(3);
    config.vset_config.pages_per_volume = 24;
    let report = run(5, config);
    assert_clean(&report);
    // 3 volumes × 24 pages = 72 resident pages per vset; 8 rotation slots.
    let per_vset: u64 = 3 * 24;
    assert!(
        report.max_step_page_reads <= 8 * per_vset,
        "one step read {} pages — the tick captured more than its slots",
        report.max_step_page_reads
    );
    // Non-vacuous: the fleet's total dirty work far exceeded one tick's
    // bound, so the rotation genuinely spread it across ticks.
    assert!(
        report.counters.pages_flushed > 8 * per_vset,
        "fleet never generated more work than one tick's budget"
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

/// The cost of remembering where pages live must track the DELTA, not the
/// vset size: as a large vset fills, per-capture map metadata stays small
/// and bounded, and the total written across the run is amortized-
/// proportional to the pages actually written — never (records × map
/// size). The full-map record fails both.
#[test]
fn big_maps_cost_deltas_not_size() {
    let config = HarnessConfig {
        daemon: DaemonConfig {
            // A warm cache: the test measures the map's cost, not thrash.
            cache_pages: 32_768,
            ..base_config().daemon
        },
        vset_count: 1,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 8_000,
            backed_up: false,
        },
        think: (micros(5), micros(25)),
        horizon: secs(1),
        checkpoint_interval: Some(millis(300)),
        // A writeback-shaped workload: this test measures the map's cost
        // under continuous dirtying. (Every sync buys a whole consistency
        // point — record frequency under sync-saturation is its own cost,
        // bounded by the record-size assert below.)
        guest_sync_share: Some(Ppm(1_000)),
        ..base_config()
    };
    let report = run(41, config);
    assert_clean(&report);
    assert_eq!(report.guest_deaths, 0);
    // Enough distinct pages written (and records committed) that the map
    // is genuinely large and continuously re-persisted.
    assert!(
        report.counters.pages_flushed > 6_000,
        "workload too small to expose the map: {} pages flushed",
        report.counters.pages_flushed
    );
    assert!(report.counters.records_written > 50);
    assert!(report.counters.leaf_rolls > 0, "no span ever rolled");
    // No single record may scale with the vset: the overlay cap bounds it
    // structurally, while a full-map encoding grows without limit (and
    // hits the R4.6 64 MiB manifest wall at ~1.5M pages).
    assert!(
        report.max_record_blob_bytes < 128 * 1024,
        "largest journal record was {} bytes — O(map), not bounded",
        report.max_record_blob_bytes
    );
    // Amortized write cost: bounded per-record overhead plus a bounded
    // per-flushed-page cost (records are written twice — the rot mirror —
    // so an overlay entry costs ~90 B per record it rides in, and a leaf
    // amortizes ~720 B per rolled page). A full-map encoding blows this
    // budget because every record costs the whole map over again.
    let budget = 900 * report.counters.pages_flushed + 16_384 * report.counters.records_written;
    assert!(
        report.map_bytes_written < budget,
        "map metadata cost {} bytes ({} flushed pages, {} records; budget {budget}) — \
         re-writing the whole map per capture",
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
        daemon: DaemonConfig {
            cache_pages: 8_192,
            ..base_config().daemon
        },
        vset_count: 1,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 4_096,
            backed_up: false,
        },
        think: (micros(10), micros(50)),
        horizon: secs(2),
        checkpoint_interval: None,
        // Writeback-shaped (syncs rare), 90% of picks in a 32-page hot
        // set: each capture's segment is mostly hot pages that the next
        // capture supersedes, plus a few cold survivors that pin it.
        guest_sync_share: Some(Ppm(1_000)),
        guest_hot_pages: Some((Ppm::percent(90), 32)),
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
        report.counters.pages_flushed > 10_000,
        "only {} pages flushed — not enough churn to expose amplification",
        report.counters.pages_flushed
    );
    // The structural bound compaction buys: every surviving segment is
    // majority-live, so disk ≤ 2 × live plus the not-yet-compacted tail.
    // History (≈7 MB of segment writes per simulated second) must NOT
    // accumulate — without compaction this run ends at 20.6 MB and grows
    // linearly with the horizon.
    assert!(
        report.seg_bytes_end < 2 * report.seg_live_bytes_end + 1_500_000,
        "{} segment bytes on disk for {} live — space amplifying with history",
        report.seg_bytes_end,
        report.seg_live_bytes_end
    );
    // And the daemon's live accounting can't be excusing the bound: the
    // whole vset is 3 volumes × 4096 pages at ≤ ~700 compressed-framed
    // bytes each ≈ 8.6 MB ceiling, and the measured end state sits well
    // under it (live 4.7 MB, disk 7.2 MB at seed 43).
    assert!(
        report.seg_live_bytes_end < 5_500_000,
        "{} live bytes — accounting inflated past the working set",
        report.seg_live_bytes_end
    );
    assert!(
        report.seg_bytes_end < 9_000_000,
        "{} segment bytes on disk at end",
        report.seg_bytes_end
    );
    // Compaction did the reclaiming, not luck.
    assert!(
        report.counters.segs_compacted > 0,
        "no segment was ever compacted"
    );
}
