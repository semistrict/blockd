//! Milestone 4, cluster level: host death under load, racing restore claims
//! resolving to exactly one runner (R6.3), the R4.3 loss bound verified
//! against the head at the kill instant, and byte-for-byte replay of whole
//! cluster runs.

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{HostId, VsetId, millis, secs};
use blockd_sim::cluster::{ClusterConfig, ClusterReport, run};
use blockd_sim::world::blobdev::BlobDevConfig;
use blockd_sim::world::store::StoreConfig;

fn base_config() -> ClusterConfig {
    ClusterConfig {
        hosts: 3,
        daemon: DaemonConfig {
            host: HostId(0), // overridden per host
            cache_pages: 128,
            writeback_interval: millis(20),
            backup_retry: millis(100),
            disk_capacity: None,
            disk_headroom: 0,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        vset_count: 3,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            backed_up: true,
        },
        horizon: secs(4),
        think: (millis(1), millis(5)),
        checkpoint_interval: Some(millis(300)),
        kill_hosts_at: vec![(millis(1500), 0)],
        race_restore: true,
        migrate_at: None,
    }
}

fn assert_clean(report: &ClusterReport) {
    assert_eq!(report.violations, Vec::<String>::new());
}

#[test]
fn host_death_restores_orphans_elsewhere_with_racing_claims() {
    let report = run(31, base_config());
    assert_clean(&report);
    // Host 0 carried one vset (round-robin of 3 across 3 hosts); its
    // restore was raced to hosts 1 and 2: exactly one won (R6.3).
    assert_eq!(report.restores, 1);
    assert_eq!(report.claims_lost, 1);
    // The recovered point was exactly the head's manifest at death (R4.3).
    assert_eq!(report.loss_bound_verified, 1);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.completed_ops, 4498);
}

#[test]
fn cluster_runs_replay_byte_for_byte() {
    for seed in [31, 44] {
        let a = run(seed, base_config());
        let b = run(seed, base_config());
        assert_eq!(a, b, "seed {seed} diverged on replay");
    }
}

/// Two hosts, one non-backed-up vset (the mode that must migrate, R7.2)
/// under load, migrated mid-run.
fn migrate_config() -> ClusterConfig {
    ClusterConfig {
        hosts: 2,
        vset_count: 1,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            backed_up: false,
        },
        kill_hosts_at: vec![],
        race_restore: false,
        migrate_at: Some((millis(1500), VsetId(1), 1)),
        ..base_config()
    }
}

#[test]
fn migration_moves_a_nonbacked_vset_losslessly() {
    let report = run(7, migrate_config());
    assert_clean(&report);
    // The handoff completed: source captured, wrote its handoff marker,
    // offered; the destination durably recorded before resuming (R7.2).
    assert_eq!(report.migrations, 1);
    // Post-copy: the guest resumed on the destination and kept running,
    // demand-faulting its tail from the source — losslessly (R7.1). A
    // non-backed vset wrote nothing to the store (R4.4), so zero restores.
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.restores, 0);
    assert_eq!(report.completed_ops, 1487);
    // R7.1: the guest-observed pause — source pause through destination
    // resume — stays far inside the 500 ms budget.
    assert!(
        report.max_migration_pause_ns > 0 && report.max_migration_pause_ns < millis(500),
        "R7.1 pause: {} ns",
        report.max_migration_pause_ns
    );
}

/// R7.3: in non-backed-up mode the source's storage IS the vset's tail
/// until the drain completes. Killing the source mid-drain — the handoff
/// done (r101 lands at ~1501.5ms on this seed), the post-copy pull not —
/// kills the vset: loudly, at the first unservable fault, never silently.
#[test]
fn source_death_mid_drain_costs_the_nonbacked_vset_loudly() {
    let config = ClusterConfig {
        kill_hosts_at: vec![(millis(1520), 0)],
        ..migrate_config()
    };
    let report = run(7, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 1);
    // The destination's guest died at its first peer fetch the dead source
    // could not answer (the sanctioned R7.3 loss).
    assert_eq!(report.guest_deaths, 1);
    assert_eq!(report.completed_ops, 555);
}

/// R6.2: a restore onto a host with none of the vset's bytes reaches its
/// verdict in well under 200 ms — a bounded number of small store reads
/// (head, claim, manifest), never a data transfer — independent of vset
/// size. And the SECOND restore prefetches the resume set the first
/// resume recorded.
#[test]
fn restores_meet_the_200ms_budget_and_prefetch_the_resume_set() {
    for pages_per_volume in [16, 64] {
        let config = ClusterConfig {
            vset_count: 1,
            vset_config: VsetConfig {
                disk_volumes: 2,
                pages_per_volume,
                backed_up: true,
            },
            race_restore: false,
            // Host 0 dies; the vset restores onto host 1, resumes, records
            // its resume set; then host 1 dies and host 2 restores it.
            kill_hosts_at: vec![(millis(1500), 0), (millis(2800), 1)],
            horizon: secs(4),
            ..base_config()
        };
        let report = run(11, config);
        assert_clean(&report);
        assert_eq!(report.restores, 2);
        assert_eq!(report.guest_deaths, 0);
        assert!(
            report.max_restore_ns < millis(200),
            "R6.2: restore took {} ns at {pages_per_volume} pages/volume",
            report.max_restore_ns
        );
        // The second restore found the first resume's recorded set and
        // warmed the cache from it.
        assert!(
            report.prefetch_fills > 0,
            "no resume-set prefetch at {pages_per_volume} pages/volume"
        );
    }
}

#[test]
fn migration_runs_replay_byte_for_byte() {
    let a = run(7, migrate_config());
    let b = run(7, migrate_config());
    assert_eq!(a, b, "migration run diverged on replay");
}

/// Cluster-level regression corpus: host death + racing claims across
/// seeds, every run held to the full oracle.
#[test]
fn cluster_seed_corpus_stays_consistent() {
    for seed in [7, 13, 31, 44, 71] {
        let report = run(seed, base_config());
        assert_eq!(
            report.violations,
            Vec::<String>::new(),
            "seed {seed} violated an invariant"
        );
        // Exactly one runner per orphan, every time (R6.3).
        assert_eq!(report.restores, 1, "seed {seed}");
        assert_eq!(report.claims_lost, 1, "seed {seed}");
    }
}
