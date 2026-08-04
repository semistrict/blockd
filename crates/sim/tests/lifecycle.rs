//! Milestone 3: the real daemon under the single-host harness — guest load,
//! writeback, syncs, checkpoints, daemon crashes with torn writes, recovery
//! verdicts, pressure, bit rot. Every run is deterministic, so assertions
//! are exact; any drift is a real behavior change.

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{HostId, micros, millis, secs};
use blockd_sim::harness::{FaultPlan, HarnessConfig, RunReport, run};
use blockd_sim::rng::Ppm;
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
        vset_count: 3,
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
        guest_sync_share: None,
    }
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
    assert_eq!(report.completed_ops, 1980);
    assert_eq!(report.counters.faults_unservable, 0);
    assert_eq!(report.counters.pressure_waits, 0);
    assert_eq!(report.counters.checkpoints_done, 0);
    assert_eq!(report.counters.syncs_acked, 277);
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
    assert_eq!(report.crashes, 9);
    assert_eq!(report.resumes, 0);
    assert_eq!(report.cold_boots, 27);
    assert_eq!(report.unrestorable, 0);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.completed_ops, 4185);
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
    assert_eq!(report.crashes, 10);
    assert_eq!(report.resumes, 0);
    assert_eq!(report.cold_boots, 30);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.completed_ops, 3422);
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
    assert_eq!(report.counters.checkpoints_done, 243);
    // R3.1: the guest-visible pause is the VMM pause round-trip only —
    // capture and persistence never extend it. Far under the 250 ms budget.
    assert_eq!(report.max_pause_ns, 199_220);
    assert_eq!(report.counters.records_written, 1659);
    // Bounded by live data: at worst ~one segment per live page plus two
    // records per vset (48 pages × 3 vsets ⇒ well under 150), an order of
    // magnitude below records_written — not growing with checkpoint count.
    assert_eq!(report.blob_count, 92);
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
    assert_eq!(report.counters.pressure_waits, 299);
    let progressed: Vec<u64> = report.per_guest_completed.values().copied().collect();
    assert_eq!(progressed, [532, 537, 530, 529]);
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
    assert_eq!(report.bitflips, 22);
    assert_eq!(report.guest_deaths, 3);
    assert_eq!(report.counters.faults_unservable, 3);
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
        (10, 12, 18, 0, 8, 4183)
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
#[test]
fn chaos_seed_corpus_stays_consistent() {
    for seed in [3, 8, 21, 47, 90] {
        let mut config = base_config();
        config.horizon = secs(3);
        config.checkpoint_interval = Some(millis(300));
        config.backed_vsets = 1;
        config.vset_config.pages_per_volume = 12;
        config.faults = FaultPlan {
            crash_mean_interval: millis(600),
            restart_delay: (millis(10), millis(300)),
            bitflip_mean_interval: millis(500),
            journal_bitflip_mean_interval: 0,
            store_outage: Some((millis(1200), millis(1900))),
        };
        let report = run(seed, config);
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
    // per-flushed-page cost. A full-map encoding blows this budget because
    // every record costs the whole map over again.
    let budget = 720 * report.counters.pages_flushed + 8_192 * report.counters.records_written;
    assert!(
        report.map_bytes_written < budget,
        "map metadata cost {} bytes ({} flushed pages, {} records; budget {budget}) — \
         re-writing the whole map per capture",
        report.map_bytes_written,
        report.counters.pages_flushed,
        report.counters.records_written,
    );
}
