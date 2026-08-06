//! The chaos configurations the committed seed corpora run — exposed from
//! the library so the corpus tests and the `sweep` binary drive the SAME
//! schedules from one definition. A seed that fails under `sweep` fails
//! identically under the corresponding corpus test; promote it there
//! (policy: every seed that ever exposed a bug belongs in the corpus,
//! forever).

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
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
        },
        bdev: BlobDevConfig::nvme(),
        store: StoreConfig::s3(),
        vset_count: 3,
        vset_config: VsetConfig {
            disk_volumes: 2,
            pages_per_volume: 16,
            backed_up: true,
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
        store_outage: None,
        rot_resume_set_at: None,
        rot_leaves_at: None,
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
        race_restore: false,
        ..cluster_kill_race()
    }
}
