//! Milestone 4, cluster level: host death under load, racing restore claims
//! resolving to exactly one runner (R6.3), the R4.3 loss bound verified
//! against the head at the kill instant, and byte-for-byte replay of whole
//! cluster runs.

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{VsetId, micros, millis, secs};
use blockd_sim::cluster::{ClusterConfig, ClusterReport, run};
use blockd_sim::rng::Ppm;

/// The library preset, by reference: the `sweep` binary drives the same
/// schedules, so corpus and sweep can never drift apart.
fn base_config() -> ClusterConfig {
    blockd_sim::presets::cluster_kill_race()
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
    assert_eq!(report.completed_ops, 4361);
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
    assert_eq!(report.completed_ops, 1773);
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
    assert_eq!(report.completed_ops, 581);
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

/// The two-sided handoff must hold at EVERY crash instant, not at one
/// hand-tuned nanosecond: sweep source crashes across the whole handoff
/// window on a fixed grid, and at every offset demand the binary outcome —
/// the migration either never happened (the vset lives on the recovered
/// source, blobs intact) or completed exactly once (drained, released,
/// source reclaimed to zero) — with the oracle's two-runners and
/// byte-exactness checks standing throughout. The grid is finer than the
/// device's write latency, so every torn-write window is visited by
/// construction; no timing re-derivation when traces shift.
#[test]
fn a_source_crash_at_every_handoff_instant_resolves_to_exactly_one_runner() {
    let (mut never_happened, mut completed) = (0, 0);
    for step in 0..40u64 {
        let at = millis(1500) + step * 50_000; // 50µs grid across 2ms
        let config = ClusterConfig {
            crash_hosts_at: vec![(at, 0)],
            ..migrate_config()
        };
        let report = run(7, config);
        assert_eq!(
            report.violations,
            Vec::<String>::new(),
            "crash at {at}ns violated an invariant"
        );
        assert_eq!(report.guest_deaths, 0, "crash at {at}ns");
        // A release happens exactly when a migration completed.
        assert_eq!(report.releases, report.migrations, "crash at {at}ns");
        match report.migrations {
            0 => {
                assert!(
                    report.blobs_per_host[0] > 0,
                    "crash at {at}ns: unmigrated vset lost its blobs"
                );
                never_happened += 1;
            }
            1 => {
                assert_eq!(
                    report.blobs_per_host[0], 0,
                    "crash at {at}ns: released source kept blobs"
                );
                completed += 1;
            }
            n => panic!("crash at {at}ns: {n} migrations of one vset"),
        }
    }
    // Coverage guard: the sweep must straddle the commit point, or it
    // proved nothing about the interesting instants.
    assert!(
        never_happened > 0 && completed > 0,
        "sweep never straddled the handoff ({never_happened} never, {completed} completed)"
    );
}

/// Wider corpus over the kill+racing-claims config, oracle-only: safety
/// must hold on seeds whose exact outcome shape varies (a takeover inside
/// the R6.4 window can make both claims "win" — that is legal; a lost
/// vset or a double-run is not).
#[test]
fn cluster_wide_seed_corpus_stays_safe() {
    for seed in [3, 5, 19, 52, 88, 101] {
        let report = run(seed, base_config());
        assert_eq!(
            report.violations,
            Vec::<String>::new(),
            "seed {seed} violated an invariant"
        );
        assert!(
            report.restores >= 1,
            "seed {seed}: the orphan never came back"
        );
        assert_eq!(report.guest_deaths, 0, "seed {seed}");
    }
}

/// The drain completes without the guest's help: hydration pulls the tail
/// in the background, the destination releases the source, and the source
/// reclaims every byte (R4.5: explicit). A later crash of the source then
/// finds nothing and costs nothing.
#[test]
fn hydration_drains_the_tail_and_releases_the_source() {
    let config = ClusterConfig {
        crash_hosts_at: vec![(millis(3000), 0)],
        ..migrate_config()
    };
    let report = run(7, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 1);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.releases, 1);
    assert!(report.hydrate_fills > 0, "the tail never hydrated");
    // Released: the source deleted the vset's segments, journal records
    // and handoff marker — its device ends the run empty.
    assert_eq!(report.blobs_per_host[0], 0);
    // The post-release crash recovers a daemon with nothing to say — and
    // the two-runners check stays silent.
    assert_eq!(report.recoveries, 1);
    assert_eq!(report.completed_ops, 1614);
}

/// The recovery side of the two-sided handoff (R7.2): a source that
/// crashes mid-drain restarts as OUTBOUND — it re-offers idempotently,
/// serves fetches, and never runs the guest. The migrated vset never
/// notices beyond latency: the drain still completes and releases.
#[test]
fn source_crash_mid_drain_recovers_outbound_and_never_runs_the_guest() {
    let config = ClusterConfig {
        crash_hosts_at: vec![(millis(1520), 0)],
        ..migrate_config()
    };
    let report = run(7, config);
    assert_clean(&report); // includes the two-runners check
    assert_eq!(report.migrations, 1);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.recoveries, 1);
    // The restarted source served the drain to completion and was
    // released and reclaimed.
    assert_eq!(report.releases, 1);
    assert_eq!(report.blobs_per_host[0], 0);
    assert_eq!(report.completed_ops, 1348);
}

/// A crash that tears the handoff marker means the migration never
/// happened (R7.2): the source recovers the vset RUNNABLE and resumes it;
/// the destination never hears of it and no release ever fires.
#[test]
fn torn_handoff_marker_means_the_migration_never_happened() {
    let config = ClusterConfig {
        // The marker write is submitted at ~1500.21ms on this seed: crash
        // while it is in flight, so the device crash tears it.
        crash_hosts_at: vec![(1_500_260_000, 0)],
        ..migrate_config()
    };
    let report = run(7, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 0);
    assert_eq!(report.releases, 0);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.recoveries, 1);
    // The vset still lives on the source: its blobs are there.
    assert!(report.blobs_per_host[0] > 0);
    assert_eq!(report.completed_ops, 1460);
}

/// R7.3's mirror: the DESTINATION crashes mid-drain. Its durable records
/// name the migration source (R7.2), so recovery restores the peer link
/// the crash interrupted: hydration resumes against the still-alive
/// source, the drain finishes, and the source — outbound the whole time,
/// never running the guest — is released and reclaimed. Exactly one
/// runner throughout; nothing dies.
#[test]
fn dest_crash_mid_drain_recovers_and_finishes_the_drain() {
    let config = ClusterConfig {
        crash_hosts_at: vec![(millis(1520), 1)],
        ..migrate_config()
    };
    let report = run(7, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 1);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.releases, 1);
    assert_eq!(report.recoveries, 1);
    // Released and reclaimed: the source holds nothing.
    assert_eq!(report.blobs_per_host[0], 0);
    // Well past the old die-loudly count (592): the guest lived on.
    assert_eq!(report.completed_ops, 1508);
}

/// Migration completes losslessly over a channel that drops a quarter of
/// its messages and duplicates an eighth: offer, accept, fetch, page and
/// release are all at-least-once with idempotent handlers.
#[test]
fn migration_survives_a_lossy_duplicating_peer_channel() {
    let config = ClusterConfig {
        peer_drop: (1, 4),
        peer_dup: (1, 8),
        ..migrate_config()
    };
    let report = run(7, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 1);
    assert_eq!(report.guest_deaths, 0);
    assert_eq!(report.releases, 1);
    assert_eq!(report.blobs_per_host[0], 0);
    // Non-vacuous: the channel really misbehaved.
    assert!(
        report.peer_drops > 0 && report.peer_dups > 0,
        "channel unexercised: {} drops, {} dups",
        report.peer_drops,
        report.peer_dups
    );
    assert_eq!(report.completed_ops, 1463);
}

#[test]
fn lossy_migration_replays_byte_for_byte() {
    let config = || ClusterConfig {
        peer_drop: (1, 4),
        peer_dup: (1, 8),
        crash_hosts_at: vec![(millis(1520), 0)],
        ..migrate_config()
    };
    let a = run(7, config());
    let b = run(7, config());
    assert_eq!(a, b, "lossy migration run diverged on replay");
}

fn migration_chaos_config() -> ClusterConfig {
    blockd_sim::presets::migration_chaos()
}

/// Randomized composition: migrations keep firing while hosts crash and
/// restart under a lossy peer channel and a mid-run store outage. Every
/// seed must stay oracle-clean — the only sanctioned deaths are a
/// destination crashing mid-drain (its tail dies with its peer link).
#[test]
fn migration_chaos_corpus_stays_consistent() {
    let mut migrations = 0;
    let mut drops = 0;
    // 192: a write fault answered by an unsolicited protected fill (the
    //      harness's WP-retrap model). 1614: destination crashed after its
    //      durable accept, before anyone learned — recovery must complete
    //      the handshake, not ignite a second runner.
    for seed in [7, 13, 19, 31, 44, 52, 71, 88, 192, 1614] {
        let report = run(seed, migration_chaos_config());
        assert_eq!(
            report.violations,
            Vec::<String>::new(),
            "seed {seed} violated an invariant"
        );
        assert!(report.recoveries > 0, "seed {seed}: no crash recovered");
        migrations += report.migrations;
        drops += report.peer_drops;
    }
    // Coverage across the corpus: the nemesis really migrated and the
    // channel really lost messages.
    assert!(migrations > 0, "corpus never completed a migration");
    assert!(drops > 0, "corpus never dropped a peer message");
}

#[test]
fn migration_chaos_replays_byte_for_byte() {
    let a = run(31, migration_chaos_config());
    let b = run(31, migration_chaos_config());
    assert_eq!(a, b, "chaos run diverged on replay");
}

/// R8.3: a restore that lands inside a store outage parks and retries —
/// it never fails. The outage also SERIALIZES the racing claimants: their
/// retry timers fire apart, so the second claimant's fresh head read sees
/// the first winner and its claim fences it — a takeover inside R6.4's
/// bounded double-run window, converging to exactly one runner. Both
/// claims "succeed"; the loser is the fenced first winner, not a CAS
/// conflict.
#[test]
fn restore_waits_out_a_store_outage() {
    let config = ClusterConfig {
        store_outage: Some((millis(1200), millis(2400))),
        ..base_config()
    };
    let report = run(31, config);
    assert_clean(&report);
    assert_eq!(report.restores, 2);
    assert_eq!(report.claims_lost, 0);
    assert_eq!(report.guest_deaths, 0);
    // Neither restore beat the outage: the kill was at 1500ms, the window
    // lifted at 2400ms — the first verdict took at least the difference.
    assert!(
        report.max_restore_ns > millis(700),
        "restore finished during the outage: {} ns",
        report.max_restore_ns
    );
    assert_eq!(report.completed_ops, 3903);
}

/// R6.2's prefetch is a bet, and a rotten resume-set object must cost
/// exactly the warmth: the second restore still meets its budget and
/// nobody dies — nothing is prefetched, that is all.
#[test]
fn a_rotten_resume_set_is_ignored_not_fatal() {
    let config = ClusterConfig {
        vset_count: 1,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            backed_up: true,
        },
        race_restore: false,
        kill_hosts_at: vec![(millis(1500), 0), (millis(2800), 1)],
        rot_resume_set_at: Some(millis(2700)),
        horizon: secs(4),
        ..base_config()
    };
    let report = run(11, config);
    assert_clean(&report);
    assert_eq!(report.restores, 2);
    assert_eq!(report.guest_deaths, 0);
    assert!(
        report.max_restore_ns < millis(200),
        "R6.2: restore took {} ns",
        report.max_restore_ns
    );
    assert_eq!(report.prefetch_fills, 0);
    assert_eq!(report.completed_ops, 690);
}

/// A vset large enough that its map genuinely shards into leaves. Low
/// sync share and a warm cache: these tests exercise the map machinery,
/// not sync torture or thrash.
fn multi_leaf_config() -> ClusterConfig {
    ClusterConfig {
        daemon: DaemonConfig {
            cache_pages: 16_384,
            ..base_config().daemon
        },
        vset_count: 1,
        // Just past the span size, so each disk volume shards into two
        // leaves — the smallest genuinely multi-leaf shape.
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 5_000,
            backed_up: true,
        },
        think: (micros(20), micros(100)),
        guest_sync_share: Some(Ppm(1_000)),
        race_restore: false,
        kill_hosts_at: vec![],
        horizon: secs(2),
        ..base_config()
    }
}

/// Restore of a multi-leaf vset: the verdict is three small objects no
/// matter the map size; the leaves hydrate lazily from the store while
/// faults into unhydrated spans park — and every byte still verifies.
#[test]
fn restores_hydrate_multi_leaf_maps_lazily() {
    let config = ClusterConfig {
        kill_hosts_at: vec![(millis(1200), 0)],
        ..multi_leaf_config()
    };
    let report = run(11, config);
    assert_clean(&report);
    assert_eq!(report.restores, 1);
    assert_eq!(report.guest_deaths, 0);
    // R6.2, strengthened by lazy hydration: the verdict never waits for
    // the map, whatever the vset size.
    assert!(
        report.max_restore_ns < millis(200),
        "R6.2: restore took {} ns",
        report.max_restore_ns
    );
    // The map really was sharded, and really hydrated span by span.
    assert!(report.leaf_fills > 0, "no leaves hydrated");
    assert_eq!(report.completed_ops, 24874);
}

/// Migration of a multi-leaf vset: the offer stays small, the destination
/// hydrates the map from the SOURCE (leaves before data), and the release
/// still fires once nothing references it.
#[test]
fn migration_hydrates_multi_leaf_maps_from_the_source() {
    let config = ClusterConfig {
        hosts: 2,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 5_000,
            backed_up: false,
        },
        migrate_at: Some((millis(1200), VsetId(1), 1)),
        ..multi_leaf_config()
    };
    let report = run(7, config);
    assert_clean(&report);
    assert_eq!(report.migrations, 1);
    assert_eq!(report.guest_deaths, 0);
    assert!(report.leaf_fills > 0, "no leaves hydrated from the source");
    // The data tail also drains (though a working set this size outlives
    // the horizon at the current hydration budget — release timing is a
    // throughput concern, not a correctness one, and is asserted by the
    // small-map release tests).
    assert!(report.hydrate_fills > 0);
    assert_eq!(report.completed_ops, 29078);
}

/// A leaf object rotten in the store makes exactly its span unservable:
/// the restore still reaches its verdict, hydration marks the span dead,
/// and the first fault into it dies loudly (R8.1) — one guest, no
/// silent corruption, nothing else touched.
#[test]
fn a_rotten_leaf_kills_its_span_loudly_and_nothing_else() {
    let config = ClusterConfig {
        // Rot lands just before the kill: the manifest the restore will
        // choose already references these leaves, and no fresh upload can
        // replace them in the 1 ms left.
        rot_leaves_at: Some(1_199_000_000),
        kill_hosts_at: vec![(millis(1200), 0)],
        ..multi_leaf_config()
    };
    let report = run(11, config);
    assert_clean(&report);
    assert_eq!(report.restores, 1);
    // The reborn guest's verification pass hit the dead span: loud death.
    assert_eq!(report.guest_deaths, 1);
    assert_eq!(report.completed_ops, 24437);
}
