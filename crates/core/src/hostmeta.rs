use crate::placement::PeerCandidate;
use crate::types::{HostId, VsetId};

#[derive(Clone, Debug)]
pub struct HostConfig {
    pub archive: ArchivePolicy,
    pub host: HostId,
    pub cache_pages: usize,
    pub writeback_interval: u64,
    pub backup_retry: u64,
    pub disk_capacity: Option<u64>,
    pub disk_headroom: u64,
    pub wedge_ticks: u64,
    pub replica_placement: Option<ReplicaPlacementConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArchivePolicy {
    pub interval: u64,
    pub max_unpublished_bytes: u64,
    pub spool_capacity_bytes: u64,
    pub spool_headroom_bytes: u64,
}

impl Default for ArchivePolicy {
    fn default() -> Self {
        Self {
            interval: crate::types::secs(10),
            max_unpublished_bytes: 32 * 1024 * 1024,
            spool_capacity_bytes: 2 * 1024 * 1024 * 1024,
            spool_headroom_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplicaPlacementConfig {
    pub membership_epoch: u64,
    pub local_failure_domain: u16,
    pub roster: Vec<PeerCandidate>,
    pub authority: Option<AuthorityHostConfig>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityHostConfig {
    pub cluster_id: u64,
    pub poll_interval: u64,
    pub max_poll_staleness: u64,
    pub challenge_interval: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub fills: u64,
    pub zero_fills: u64,
    pub shared_fills: u64,
    pub wp_faults: u64,
    pub guest_pages_dirtied: u64,
    pub faults_unservable: u64,
    pub pressure_waits: u64,
    pub pages_flushed: u64,
    pub records_written: u64,
    pub checkpoints_done: u64,
    pub syncs_acked: u64,
    pub guest_rejected: u64,
    pub peer_rejected: u64,
    pub blobs_deleted: u64,
    pub manifests_published: u64,
    pub store_retries: u64,
    pub fenced: u64,
    pub assignment_claims: u64,
    pub assignment_claim_conflicts: u64,
    pub nvme_reclaims: u64,
    pub nvme_stalls: u64,
    pub prefetch_fills: u64,
    pub hydrate_fills: u64,
    pub peer_retries: u64,
    pub cow_captures: u64,
    pub wedged_guests: u64,
    pub wedged_hydration: u64,
    pub wedged_outbound: u64,
    pub leaf_rolls: u64,
    pub leaf_fills: u64,
    pub segs_compacted: u64,
    pub pages_compacted: u64,
    pub replica_bytes: u64,
    pub replica_rejected: u64,
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
    pub lease_gets: u64,
    pub lease_challenges: u64,
    pub lease_defenses: u64,
    pub lease_self_fences: u64,
    pub vnode_adoptions: u64,
    pub vnode_stale_rejections: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaVsetMetrics {
    pub vset: VsetId,
    pub active_peer: Option<HostId>,
    pub transition_peer: Option<HostId>,
    pub assignment_epoch: Option<u64>,
    pub local_covered_through: u64,
    pub peer_committed_through: u64,
    pub store_published_through: u64,
    pub sync_ack_through: u64,
    pub queued_syncs: usize,
    pub upload_lag: u64,
    pub current_retries: u8,
    pub queued_releases: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaSpoolMetrics {
    pub source: HostId,
    pub vset: VsetId,
    pub assignment_epoch: u64,
    pub stored_bytes: u64,
    pub host_capacity_bytes: u64,
    pub current_generation: u64,
    pub committed_through: u64,
    pub uploaded_through: u64,
    pub unarchived_age_ns: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VsetStats {
    pub vset: VsetId,
    pub role: VsetRole,
    pub fence: u64,
    pub dirty_pages: usize,
    pub unstable_pages: usize,
    pub parked_faults: usize,
    pub pending_syncs: usize,
    pub pending_leaf_spans: usize,
    pub hydration_remaining_pages: usize,
    pub archive_lag_captures: Option<u64>,
    pub archive_lag_bytes: Option<u64>,
    pub operations: VsetOperations,
    pub live_segment_bytes: u64,
    pub local_segment_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VsetRole {
    Initializing,
    Serving,
    Hydrating,
    Outbound,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VsetOperations(pub(crate) u8);

impl VsetOperations {
    pub const CAPTURE: u8 = 1;
    pub const CHECKPOINT: u8 = 2;
    pub const BACKUP: u8 = 4;
    pub const HYDRATION: u8 = 8;

    pub fn active(self, operation: u8) -> bool {
        self.0 & operation != 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DaemonStats {
    pub cache_capacity_pages: usize,
    pub resident_pages: usize,
    pub shared_resident_pages: usize,
    pub reserved_pages: usize,
    pub dirty_pages: usize,
    pub unstable_pages: usize,
    pub pressure_waiting_faults: usize,
    pub parked_faults: usize,
    pub local_blob_bytes: u64,
    pub disk_capacity_bytes: Option<u64>,
    pub disk_headroom_bytes: u64,
    pub live_segment_bytes: u64,
    pub local_segment_bytes: u64,
    pub vsets: Vec<VsetStats>,
}
