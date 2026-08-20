//! Stable commands, replies, peer messages, and faults shared by actors and worlds.
//!
//! These types retain their byte-level meaning across the async migration.

use std::fmt;

use crate::journal::VolumeConfig;
use crate::types::{Epoch, HostId, ObjectId, VolumeId};

/// Largest object accepted by the durable-store seam, including framing
/// overhead for payloads whose unframed contract is 64 MiB.
pub const MAX_OBJECT_BYTES: u32 = 64 * 1024 * 1024 + 4096;

/// Actor-issued peer request id, unique within one host process session.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerRequestId(pub u64);

impl fmt::Debug for PeerRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "io{}", self.0)
    }
}

/// Request id for explicit guest/admin requests (syncs, admin commands).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReqId(pub u64);

impl fmt::Debug for ReqId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "r{}", self.0)
    }
}

/// A store operation's failure (the only faults the R4.6 contract admits).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreFault {
    /// Outage (R8.3): the operation reached nothing; retry later.
    Unavailable,
    /// CAS expectation not met; carries the actual current version.
    CasConflict { actual: Option<u64> },
}

/// Typed identity of an immutable artifact placed in a passive peer spool.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum ReplicaArtifact {
    Blx { fence: u64, object: ObjectId },
}

/// Field order is the durability significance order; derived ordering is used
/// to compare replication frontiers throughout the engine.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ReplicaCommitInfo {
    pub writer_fence: u64,
    pub seq: crate::types::JournalSeq,
    pub sync_covered_through: u64,
}

/// Daemon-to-daemon messages (the mTLS peer protocol, R11.1 — membership
/// and encryption are the runtime's layer; the simulation models cluster
/// membership as the set of reachable hosts).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PeerMsg {
    /// Post-copy migration offer (R7.1): the final captured record's exact
    /// bytes. The sender has already made its outbound handoff durable
    /// (R7.2: durable on both sides before either acts).
    MigrateOffer {
        volume: VolumeId,
        record: Vec<u8>,
        /// Canonical VMM bytes when they fit in the offer. A recovered source
        /// may omit them; the destination then fetches the verified BLX ranges.
        vmstate: Option<Vec<u8>>,
    },
    /// The destination's accept: its side of the handoff is durable and its
    /// guest is resuming. The source now serves fetches and never runs the
    /// guest again.
    MigrateAccept {
        volume: VolumeId,
        /// The source fence from the exact offered record being accepted.
        offer_fence: u64,
    },
    /// Demand fetch of one page entry from a peer (the peer tier of R2.3).
    FetchRange {
        io: PeerRequestId,
        volume: VolumeId,
        /// Present when the requester is the primary reading its committed
        /// copy from the assigned passive. Absent for migration-source reads.
        replica_assignment_epoch: Option<u64>,
        fence: u64,
        object: ObjectId,
        offset: u32,
        len: u32,
    },
    /// Peer fetch response: raw stored bytes, damage included (the reader
    /// verifies, R8.1). `None` = the peer no longer has it.
    Page {
        io: PeerRequestId,
        bytes: Option<Vec<u8>>,
    },
    /// The destination holds every byte it needs: the source may reclaim
    /// the volume's local state.
    Released {
        volume: VolumeId,
        release_fence: u64,
    },
    /// The source's acknowledgment of `Released` (which is retried until
    /// acked; a source that already reclaimed still acks).
    ReleasedAck {
        volume: VolumeId,
        release_fence: u64,
    },
    /// Store one immutable artifact on the assigned passive peer. An
    /// identical retry re-acks without another append.
    ReplicaPut {
        volume: VolumeId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
    },
    ReplicaPutAck {
        volume: VolumeId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
    },
    /// Commit one exact recovery record after every required artifact is
    /// stable on this peer or already truthfully known in the store.
    ReplicaCommit {
        volume: VolumeId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        required: Vec<ReplicaArtifact>,
        record: Vec<u8>,
    },
    ReplicaCommitAck {
        volume: VolumeId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
    },
    ReplicaStatus {
        volume: VolumeId,
        assignment_epoch: u64,
    },
    ReplicaStatusReply {
        volume: VolumeId,
        assignment_epoch: u64,
        committed: Option<ReplicaCommitInfo>,
    },
    ReplicaRelease {
        volume: VolumeId,
        assignment_epoch: u64,
        through: ReplicaCommitInfo,
    },
    ReplicaReleaseAck {
        volume: VolumeId,
        assignment_epoch: u64,
        through: ReplicaCommitInfo,
    },
}

impl PeerMsg {
    /// Stable wire discriminant shared by encoders and transport fault models.
    pub const fn tag(&self) -> u8 {
        match self {
            Self::MigrateOffer { .. } => 0,
            Self::MigrateAccept { .. } => 1,
            Self::FetchRange { .. } => 2,
            Self::Page { .. } => 3,
            Self::Released { .. } => 6,
            Self::ReleasedAck { .. } => 7,
            Self::ReplicaPut { .. } => 8,
            Self::ReplicaPutAck { .. } => 9,
            Self::ReplicaCommit { .. } => 10,
            Self::ReplicaCommitAck { .. } => 11,
            Self::ReplicaStatus { .. } => 12,
            Self::ReplicaStatusReply { .. } => 13,
            Self::ReplicaRelease { .. } => 15,
            Self::ReplicaReleaseAck { .. } => 16,
        }
    }
}

/// In-process administrative call. Completion is routed by its owned reply
/// promise; only checkpoint retains a request identity for durable retry
/// idempotency.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AdminCall {
    /// Replace the actor's live view of the single CAS-serialized cluster
    /// placement. Durable heads authorize already assigned replica spools.
    UpdateClusterPlacement {
        placement: crate::hostmeta::ClusterPlacementConfig,
    },
    CreateVolume {
        volume: VolumeId,
        config: VolumeConfig,
        from_base: Option<u64>,
    },
    KeepBase {
        volume: VolumeId,
        base: u64,
    },
    DeleteBase {
        base: u64,
    },
    Checkpoint {
        retry: ReqId,
        volume: VolumeId,
    },
    RestoreVolume {
        volume: VolumeId,
    },
    MigrateOut {
        volume: VolumeId,
        to: HostId,
    },
}

/// Successful completion of an in-process administrative operation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminSuccess {
    ClusterPlacementUpdated,
    VolumeCreated { volume: VolumeId },
    CheckpointDone { volume: VolumeId, epoch: Epoch },
    VolumeRestored { volume: VolumeId, verdict: Verdict },
    BaseKept { base: u64 },
    BaseDeleted { base: u64 },
    VolumeForked { volume: VolumeId, verdict: Verdict },
    MigratedOut { volume: VolumeId },
}

pub type AdminResult = Result<AdminSuccess, AdminError>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminError {
    Rejected,
    Busy,
    NotFound,
    Stale,
    Unavailable,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminEvent {
    /// A backed-up volume finished local recovery and this host retained
    /// authority with local state at least as new as the backup.
    VolumeRecovered { volume: VolumeId, verdict: Verdict },
    /// An inbound migration is live on this host, per the verdict.
    VolumeMigratedIn { volume: VolumeId, verdict: Verdict },
}

/// Recovery verdict for one volume (R8.2: explicit, per volume).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    /// Newest usable recovery point is a whole checkpoint: resume — memory,
    /// vmstate and disks of one epoch (R1.2).
    Resume { epoch: Epoch, vmstate: u64 },
    /// Newest usable recovery point is disk-only: boot fresh from disks at
    /// sync consistency; memory is invalid (R3.7).
    ColdBoot,
    /// Durable state exists but no intact record: nothing restorable.
    Unrestorable,
}
