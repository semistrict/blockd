//! Stable commands, replies, peer messages, and faults shared by actors and worlds.
//!
//! These types retain their byte-level meaning across the async migration.

use std::fmt;

use crate::database::AttachmentId;
use crate::journal::VsetConfig;
use crate::types::{Epoch, HostId, SegId, VmId, VsetId};

/// Largest object accepted by the durable-store seam, including framing
/// overhead for payloads whose unframed contract is 64 MiB.
pub const MAX_OBJECT_BYTES: u32 = 64 * 1024 * 1024 + 4096;

/// Actor-issued peer request id, unique per host incarnation.
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
    Segment { fence: u64, seg: SegId },
    Leaf { fence: u64, id: u64 },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
        vset: VsetId,
        record: Vec<u8>,
        /// Canonical VMM bytes when they fit in the offer. A recovered source
        /// may omit them; the destination then fetches the verified BLX ranges.
        vmstate: Option<Vec<u8>>,
    },
    /// The destination's accept: its side of the handoff is durable and its
    /// guest is resuming. The source now serves fetches and never runs the
    /// guest again.
    MigrateAccept {
        vset: VsetId,
        /// The source fence from the exact offered record being accepted.
        offer_fence: u64,
    },
    /// Demand fetch of one page entry from a peer (the peer tier of R2.3).
    FetchRange {
        io: PeerRequestId,
        vset: VsetId,
        /// Present when the requester is the primary reading its committed
        /// copy from the assigned passive. Absent for migration-source reads.
        replica_assignment_epoch: Option<u64>,
        fence: u64,
        seg: SegId,
        offset: u32,
        len: u32,
    },
    /// Peer fetch response: raw stored bytes, damage included (the reader
    /// verifies, R8.1). `None` = the peer no longer has it.
    Page {
        io: PeerRequestId,
        bytes: Option<Vec<u8>>,
    },
    /// Fetch one map leaf blob from a peer (post-copy hydration of a
    /// migrated vset's map, R7.1). `base` is 0 for the vset's own
    /// namespace, else the base whose leaf this is.
    FetchLeaf {
        io: PeerRequestId,
        vset: VsetId,
        base: u64,
        fence: u64,
        id: u64,
    },
    /// Leaf fetch response: the blob's raw bytes, damage included.
    Leaf {
        io: PeerRequestId,
        bytes: Option<Vec<u8>>,
    },
    /// The destination holds every byte it needs: the source may reclaim
    /// the vset's local state.
    Released {
        vset: VsetId,
        release_fence: u64,
    },
    /// The source's acknowledgment of `Released` (which is retried until
    /// acked; a source that already reclaimed still acks).
    ReleasedAck {
        vset: VsetId,
        release_fence: u64,
    },
    /// Store one immutable artifact on the assigned passive peer. An
    /// identical retry re-acks without another append.
    ReplicaPut {
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
    },
    ReplicaPutAck {
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
    },
    /// Commit one exact recovery record after every required artifact is
    /// stable on this peer or already truthfully known in the store.
    ReplicaCommit {
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        required: Vec<ReplicaArtifact>,
        record: Vec<u8>,
    },
    ReplicaCommitAck {
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
    },
    ReplicaStatus {
        vset: VsetId,
        assignment_epoch: u64,
    },
    ReplicaStatusReply {
        vset: VsetId,
        assignment_epoch: u64,
        committed: Option<ReplicaCommitInfo>,
    },
    ReplicaRelease {
        vset: VsetId,
        assignment_epoch: u64,
        through: ReplicaCommitInfo,
    },
    ReplicaReleaseAck {
        vset: VsetId,
        assignment_epoch: u64,
        through: ReplicaCommitInfo,
    },
    /// Cold-path vnode failover. The receiver independently GETs and verifies
    /// the object-store proof before durably adopting its generation.
    VnodeAdopt {
        io: PeerRequestId,
        proof: crate::authority::AuthorityProof,
    },
    VnodeAdoptAck {
        io: PeerRequestId,
        proof: crate::authority::AuthorityProof,
        closures: Vec<crate::vnode_member::ProtectedClosureRef>,
    },
    VnodeFetchClosure {
        io: PeerRequestId,
        vnode: crate::authority::VnodeId,
        closure: crate::vnode_member::ProtectedClosureRef,
    },
    VnodeClosure {
        io: PeerRequestId,
        bytes: Option<Vec<u8>>,
    },
    VnodeCommit {
        io: PeerRequestId,
        proof: crate::authority::AuthorityProof,
        vset: VsetId,
        sequence: u64,
        bytes: Vec<u8>,
    },
    VnodeCommitAck {
        io: PeerRequestId,
        closure: crate::vnode_member::ProtectedClosureRef,
    },
}

/// In-process administrative call. Completion is routed by its owned reply
/// promise; only checkpoint retains a request identity for durable retry
/// idempotency.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminCall {
    CreateVset {
        vset: VsetId,
        config: VsetConfig,
        from_base: Option<u64>,
    },
    KeepBase {
        vset: VsetId,
        base: u64,
    },
    DeleteBase {
        base: u64,
    },
    Checkpoint {
        retry: ReqId,
        vset: VsetId,
    },
    RestoreVset {
        vset: VsetId,
    },
    MigrateOut {
        vset: VsetId,
        to: HostId,
    },
    AttachDatabase {
        vset: VsetId,
        vm: VmId,
    },
    BeginDetachDatabase {
        vset: VsetId,
        attachment: AttachmentId,
        mode: DetachMode,
    },
    FinishDetachDatabase {
        vset: VsetId,
        attachment: AttachmentId,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DetachMode {
    Graceful,
    Forced,
}

/// Successful completion of an in-process administrative operation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminSuccess {
    VsetCreated {
        vset: VsetId,
    },
    CheckpointDone {
        vset: VsetId,
        epoch: Epoch,
    },
    VsetRestored {
        vset: VsetId,
        verdict: Verdict,
    },
    BaseKept {
        base: u64,
    },
    BaseDeleted {
        base: u64,
    },
    VsetForked {
        vset: VsetId,
        verdict: Verdict,
    },
    MigratedOut {
        vset: VsetId,
    },
    DatabaseAttached {
        vset: VsetId,
        attachment: AttachmentId,
    },
    DatabaseDetachStarted {
        vset: VsetId,
        attachment: AttachmentId,
        forced: bool,
    },
    DatabaseDetached {
        vset: VsetId,
        attachment: AttachmentId,
    },
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
    /// A backed-up vset finished local recovery and this host retained
    /// authority with local state at least as new as the backup.
    VsetRecovered { vset: VsetId, verdict: Verdict },
    /// An inbound migration is live on this host, per the verdict.
    VsetMigratedIn { vset: VsetId, verdict: Verdict },
}

/// Recovery verdict for one vset (R8.2: explicit, per vset).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    /// Newest usable recovery point is a whole checkpoint: resume — memory,
    /// vmstate and disks of one epoch (R1.2).
    Resume { epoch: Epoch, vmstate: u64 },
    /// Newest usable recovery point is disk-only: boot fresh from disks at
    /// sync consistency; memory is invalid (R3.7).
    ColdBoot,
    /// A storage-only `SQLite` vset recovered at a durable file/page point.
    /// It is ready for a separate attachment and carries no VM state.
    DatabaseReady { synced_through: u64 },
    /// Durable state exists but no intact record: nothing restorable.
    Unrestorable,
}
