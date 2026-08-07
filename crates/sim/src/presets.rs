//! The chaos configurations the committed seed corpora run — exposed from
//! the library so the corpus tests and the `sweep` binary drive the SAME
//! schedules from one definition. A seed that fails under `sweep` fails
//! identically under the corresponding corpus test; promote it there
//! (policy: every seed that ever exposed a bug belongs in the corpus,
//! forever).

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::{DurabilityMode, VsetConfig};
use blockd_core::placement::PeerCandidate;
use blockd_core::types::{HostId, millis, secs};

use crate::cluster::ClusterConfig;
use crate::harness::{FaultPlan, HarnessConfig};
use crate::world::blobdev::BlobDevConfig;
use crate::world::store::StoreConfig;

/// Single host, three non-backed vsets, no faults — the base most
/// single-host scenarios perturb.
pub fn single_host_base() -> HarnessConfig {
    HarnessConfig {
        daemon: DaemonConfig {
            host: HostId(0),
            cache_pages: 256,
            writeback_interval: millis(20),
            backup_retry: millis(200),
            disk_capacity: None,
            disk_headroom: 0,
            // 25 ticks × 20 ms = 500 ms: reachable inside sim horizons, so
            // the wedge watch is exercised, not decorative.
            wedge_ticks: 25,
            replica_placement: None,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        vset_count: 3,
        backed_vsets: 0,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            durability: DurabilityMode::Local,
        },
        horizon: secs(2),
        think: (millis(1), millis(5)),
        checkpoint_interval: None,
        faults: FaultPlan::none(),
        sabotage: None,
        guest_sync_share: None,
        guest_hot_pages: None,
        rot_records_at: vec![],
        crash_at: vec![],
    }
}

/// The single-host chaos schedule of `chaos_seed_corpus_stays_consistent`:
/// checkpoints, one backed vset, Poisson crashes and bit rot, and a
/// mid-run store outage.
pub fn single_host_chaos() -> HarnessConfig {
    let mut config = single_host_base();
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
    config
}

/// The cluster schedule of `cluster_seed_corpus_stays_consistent`: three
/// hosts under load, host 0 killed mid-run, its restore deliberately
/// raced by the survivors (exactly-one-runner, R6.3).
pub fn cluster_kill_race() -> ClusterConfig {
    ClusterConfig {
        hosts: 3,
        daemon: DaemonConfig {
            host: HostId(0), // overridden per host
            cache_pages: 128,
            writeback_interval: millis(20),
            backup_retry: millis(100),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 25,
            replica_placement: None,
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        vset_count: 3,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            durability: DurabilityMode::Backup,
        },
        nonbacked_vsets: 0,
        horizon: secs(4),
        think: (millis(1), millis(5)),
        checkpoint_interval: Some(millis(300)),
        kill_hosts_at: vec![(millis(1500), 0)],
        crash_hosts_at: vec![],
        restart_delay: (millis(50), millis(200)),
        crash_mean_interval: 0,
        migrate_mean_interval: 0,
        peer_drop: (0, 1),
        peer_dup: (0, 1),
        peer_link_outages: vec![],
        fault_points: vec![],
        store_outage: None,
        rot_resume_set_at: None,
        rot_leaves_at: None,
        drop_peer: None,
        race_restore: true,
        migrate_at: None,
        sabotage: None,
        guest_sync_share: None,
    }
}

/// The randomized composition of `migration_chaos_corpus_stays_consistent`:
/// migrations keep firing while hosts crash and restart under a lossy,
/// duplicating peer channel and a mid-run store outage.
pub fn migration_chaos() -> ClusterConfig {
    ClusterConfig {
        hosts: 3,
        vset_count: 4,
        nonbacked_vsets: 2,
        migrate_mean_interval: millis(400),
        crash_mean_interval: millis(1100),
        restart_delay: (millis(50), millis(200)),
        peer_drop: (1, 8),
        peer_dup: (1, 8),
        store_outage: Some((millis(1800), millis(2600))),
        kill_hosts_at: vec![],
        drop_peer: None,
        race_restore: false,
        ..cluster_kill_race()
    }
}

/// Three hosts with one deterministic passive target per vset, lossy peer
/// links, and a prolonged S3 outage. Sync liveness depends on the peer commit,
/// while eventual upload/release exercises the no-rewrite cleanup path.
pub fn peer_stash_chaos() -> ClusterConfig {
    let mut config = cluster_kill_race();
    config.vset_count = 3;
    config.nonbacked_vsets = 0;
    config.vset_config.durability = DurabilityMode::PeerStashed;
    config.daemon.replica_placement = Some(blockd_core::daemon::ReplicaPlacementConfig {
        membership_epoch: 1,
        local_failure_domain: 1,
        roster: (0..config.hosts)
            .map(|host| PeerCandidate {
                host: HostId(host),
                weight: host + 1,
                failure_domain: host + 1,
                drained: false,
            })
            .collect(),
    });
    config.kill_hosts_at.clear();
    config.race_restore = false;
    config.peer_drop = (1, 12);
    config.peer_dup = (1, 10);
    config.store_outage = Some((millis(400), millis(2200)));
    config.guest_sync_share = Some(crate::rng::Ppm::percent(35));
    config
}

/// Active-peer process/disk loss long enough to force a fenced B-to-C
/// replacement, followed by restart scanning of the old peer residue.
pub fn peer_attrition() -> ClusterConfig {
    let mut config = peer_stash_chaos();
    config.vset_count = 1;
    config.horizon = secs(2);
    config.peer_drop = (0, 1);
    config.peer_dup = (0, 1);
    config.store_outage = None;
    config.restart_delay = (millis(700), millis(800));
    let roster = &config
        .daemon
        .replica_placement
        .as_ref()
        .expect("peer placement")
        .roster;
    let active = blockd_core::placement::rank_stash_candidates(
        1,
        HostId(0),
        1,
        blockd_core::types::VsetId(1),
        roster,
    )[0];
    config.crash_hosts_at = vec![(millis(400), active.0)];
    config
}

/// Directional partitions in both directions of the initial active link,
/// composed with a control/data-store outage and reliable links elsewhere.
pub fn swizzle_peer_links() -> ClusterConfig {
    let mut config = peer_stash_chaos();
    config.vset_count = 1;
    config.horizon = secs(2);
    config.peer_drop = (0, 1);
    config.peer_dup = (0, 1);
    let roster = &config
        .daemon
        .replica_placement
        .as_ref()
        .expect("peer placement")
        .roster;
    let active = blockd_core::placement::rank_stash_candidates(
        1,
        HostId(0),
        1,
        blockd_core::types::VsetId(1),
        roster,
    )[0];
    config.peer_link_outages = vec![
        (millis(300), millis(650), 0, active.0),
        (millis(700), millis(1050), active.0, 0),
    ];
    config.store_outage = Some((millis(1100), millis(1450)));
    config
}

/// Aggressive deterministic rare-branch preset. Every named point must
/// report nonzero coverage or the test fails rather than calling it chaos.
pub fn peer_stash_rare() -> ClusterConfig {
    let mut config = peer_attrition();
    config.horizon = secs(8);
    let crashed = config.crash_hosts_at[0].1;
    config.crash_hosts_at = vec![(secs(2), crashed)];
    config.peer_drop = (1, 12);
    config.peer_dup = (1, 10);
    config.store_outage = Some((millis(200), millis(800)));
    config.fault_points = vec![
        crate::cluster::FaultPoint::ReplicaRetryTimer,
        crate::cluster::FaultPoint::DuplicateAck,
        crate::cluster::FaultPoint::StatusReconciliation,
        crate::cluster::FaultPoint::AssignmentCasRace,
        crate::cluster::FaultPoint::StoreUnknownResult,
        crate::cluster::FaultPoint::RestartScan,
    ];
    config
}

/// Four-host placement failure: B is lost, then C is lost during its seed,
/// so the next fenced assignment epoch selects only D. Both old peers return
/// later and must not regain send authority.
pub fn placement_fear() -> ClusterConfig {
    let mut config = peer_attrition();
    config.hosts = 4;
    config.horizon = secs(4);
    config.restart_delay = (millis(900), millis(1000));
    let roster: Vec<_> = (0..config.hosts)
        .map(|host| PeerCandidate {
            host: HostId(host),
            weight: host + 1,
            failure_domain: host + 1,
            drained: false,
        })
        .collect();
    config
        .daemon
        .replica_placement
        .as_mut()
        .expect("placement")
        .roster
        .clone_from(&roster);
    let ranked = blockd_core::placement::rank_stash_candidates(
        1,
        HostId(0),
        1,
        blockd_core::types::VsetId(1),
        &roster,
    );
    config.crash_hosts_at = vec![(millis(400), ranked[0].0), (millis(750), ranked[1].0)];
    config
}
