//! Stable multi-host simulation API backed by deterministic async actors.

use std::collections::BTreeMap;

use blockd_core::hostmeta::HostConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::VsetId;

use crate::harness::Sabotage;
use crate::world::blobdev::BlobDevConfig;
use crate::world::store::{StoreConfig, StoreCounters};

pub use blockd_exec::FaultPoint;

/// Peer message category used by the targeted link nemesis.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum PeerKind {
    Offer,
    Accept,
    FetchRange,
    Page,
    FetchLeaf,
    Leaf,
    Released,
    ReleasedAck,
    ReplicaPut,
    ReplicaPutAck,
    ReplicaCommit,
    ReplicaCommitAck,
    ReplicaArchive,
    ReplicaStatus,
    ReplicaStatusReply,
    ReplicaUploadDone,
    ReplicaRelease,
    ReplicaReleaseAck,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub hosts: u16,
    pub daemon: HostConfig,
    pub bdev: BlobDevConfig,
    pub store: StoreConfig,
    pub vset_count: u16,
    pub vset_config: VsetConfig,
    pub horizon: u64,
    pub think: (u64, u64),
    pub checkpoint_interval: Option<u64>,
    pub kill_hosts_at: Vec<(u64, u16)>,
    pub crash_hosts_at: Vec<(u64, u16)>,
    pub restart_delay: (u64, u64),
    pub crash_mean_interval: u64,
    /// Nemesis: mean interval between random migrations (0 disables).
    pub migrate_mean_interval: u64,
    pub peer_drop: (u64, u64),
    pub peer_dup: (u64, u64),
    pub peer_link_outages: Vec<(u64, u64, u16, u16)>,
    pub fault_points: Vec<FaultPoint>,
    pub store_outage: Option<(u64, u64)>,
    pub rot_resume_set_at: Option<u64>,
    pub rot_leaves_at: Option<u64>,
    pub drop_peer: Option<(PeerKind, u64, u64)>,
    pub race_restore: bool,
    pub migrate_at: Vec<(u64, VsetId, u16)>,
    pub sabotage: Option<Sabotage>,
    pub guest_sync_share: Option<crate::rng::Ppm>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClusterReport {
    pub trace_hash: u64,
    pub violations: Vec<String>,
    pub audit_runs: u64,
    pub audited_vsets: u64,
    pub audited_pages: u64,
    pub completed_ops: u64,
    pub restores: u64,
    pub claims_lost: u64,
    pub guest_deaths: u64,
    pub loss_bound_verified: u64,
    pub migrations: u64,
    pub max_restore_ns: u64,
    pub max_migration_pause_ns: u64,
    pub prefetch_fills: u64,
    pub peer_drops: u64,
    pub peer_dups: u64,
    pub peer_link_clogs: u64,
    pub host_crashes: u64,
    pub disk_crash_applied: u64,
    pub disk_crash_dropped: u64,
    pub disk_crash_torn: u64,
    pub disk_bitflips: u64,
    pub store_unavailable: u64,
    pub store_cas_conflicts: u64,
    pub fault_coverage: BTreeMap<FaultPoint, u64>,
    pub sync_samples: u64,
    pub sync_latency_p50_ns: u64,
    pub sync_latency_p95_ns: u64,
    pub sync_latency_p99_ns: u64,
    pub sync_latency_max_ns: u64,
    pub recoveries: u64,
    pub releases: u64,
    pub migrations_refused: u64,
    pub hydrate_fills: u64,
    pub leaf_fills: u64,
    pub store_retries: u64,
    pub peer_rejected: u64,
    pub nemesis_drops: u64,
    pub wedged_guests: u64,
    pub wedged_hydration: u64,
    pub wedged_outbound: u64,
    pub parked_end: usize,
    pub hydrating_end: usize,
    pub replica_bytes: u64,
    pub replica_commits: u64,
    pub replica_store_bytes: u64,
    pub replica_unlinks: u64,
    pub replica_network_bytes: u64,
    pub replica_logical_bytes: u64,
    pub replica_nonactive_bytes: u64,
    pub replica_replacement_bytes: u64,
    pub replica_cleanup_rewrite_bytes: u64,
    pub replica_artifact_flushes: u64,
    pub replica_commit_flushes: u64,
    pub replica_rotations: u64,
    pub archive_cycles: u64,
    pub archive_commits_coalesced: u64,
    pub replica_capacity_backpressure: u64,
    pub published_segment_bytes: u64,
    pub published_live_entry_bytes: u64,
    pub published_dead_entry_bytes: u64,
    pub published_segment_overhead_bytes: u64,
    pub replica_spool_bytes: u64,
    pub peer_committed_through: u64,
    pub archived_through: u64,
    pub archive_lag_bytes: u64,
    pub max_archive_lag_age_ns: u64,
    pub segs_compacted: u64,
    pub pages_compacted: u64,
    pub store: StoreCounters,
    /// Blobs left on each host's device at the end of the run.
    pub blobs_per_host: Vec<usize>,
    /// Primary journal/segment/leaf blobs, excluding passive replica spools.
    pub primary_blobs_per_host: Vec<usize>,
}

#[allow(clippy::needless_pass_by_value)]
pub fn run(seed: u64, config: ClusterConfig) -> ClusterReport {
    crate::actor_cluster::run(seed, config)
}
