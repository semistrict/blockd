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

/// Daemon-issued I/O id, unique per daemon incarnation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct IoId(pub u64);

impl fmt::Debug for IoId {
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
    },
    /// The destination's accept: its side of the handoff is durable and its
    /// guest is resuming. The source now serves fetches and never runs the
    /// guest again.
    MigrateAccept {
        vset: VsetId,
    },
    /// Demand fetch of one page entry from a peer (the peer tier of R2.3).
    FetchRange {
        io: IoId,
        vset: VsetId,
        fence: u64,
        seg: SegId,
        offset: u32,
        len: u32,
    },
    /// Peer fetch response: raw stored bytes, damage included (the reader
    /// verifies, R8.1). `None` = the peer no longer has it.
    Page {
        io: IoId,
        bytes: Option<Vec<u8>>,
    },
    /// Fetch one map leaf blob from a peer (post-copy hydration of a
    /// migrated vset's map, R7.1). `base` is 0 for the vset's own
    /// namespace, else the base whose leaf this is.
    FetchLeaf {
        io: IoId,
        vset: VsetId,
        base: u64,
        fence: u64,
        id: u64,
    },
    /// Leaf fetch response: the blob's raw bytes, damage included.
    Leaf {
        io: IoId,
        bytes: Option<Vec<u8>>,
    },
    /// The destination holds every byte it needs: the source may reclaim
    /// the vset's local state.
    Released {
        vset: VsetId,
    },
    /// The source's acknowledgment of `Released` (which is retried until
    /// acked; a source that already reclaimed still acks).
    ReleasedAck {
        vset: VsetId,
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
    /// The passive peer uploaded every object needed by this commit. The
    /// primary may now perform the small fenced head publication.
    ReplicaUploadDone {
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        record: Vec<u8>,
    },
    /// The primary needs the covering committed cut archived before an
    /// explicit lifecycle transition (for example migration) can finish.
    ReplicaArchive {
        vset: VsetId,
        assignment_epoch: u64,
        through: ReplicaCommitInfo,
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
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminCmd {
    /// Create a vset — fresh, or forked from a base in O(1) metadata
    /// (R5.1: no bytes copied, regardless of base size). Acknowledged once
    /// its first journal record is durable.
    CreateVset {
        req: ReqId,
        vset: VsetId,
        config: VsetConfig,
        from_base: Option<u64>,
    },
    /// Keep the vset's pinned checkpoint as a base (R5.2): its segments are
    /// copied into the base's own namespace in the store, alive until an
    /// explicit delete — never by age (R4.5). Backed-up vsets only (R4.4
    /// forbids any store write for the other mode).
    KeepBase { req: ReqId, vset: VsetId, base: u64 },
    /// Explicitly delete a base (R4.5): removing its record unroots it, and
    /// the GC's next sweep reclaims its unshared segments (R9.3).
    DeleteBase { req: ReqId, base: u64 },
    /// Whole-vset checkpoint (R3.1). Idempotent by `req` (R3.5).
    Checkpoint { req: ReqId, vset: VsetId },
    /// Restore a backed-up vset onto this host from the object store alone
    /// (R6.1): claim the head by CAS, fetch the newest manifest, serve.
    RestoreVset { req: ReqId, vset: VsetId },
    /// Live-migrate the vset to a peer, post-copy (R7.1): cut over first,
    /// fault the remainder.
    MigrateOut {
        req: ReqId,
        vset: VsetId,
        to: HostId,
    },
    /// Grant one VM volatile authority over a locally ready database vset.
    AttachDatabase { req: ReqId, vset: VsetId, vm: VmId },
    /// Start a graceful drain, or immediately retire this generation.
    BeginDetachDatabase {
        req: ReqId,
        vset: VsetId,
        attachment: AttachmentId,
        mode: DetachMode,
    },
    /// Complete a graceful detach once handles are closed and its accepted
    /// mutation prefix is durable.
    FinishDetachDatabase {
        req: ReqId,
        vset: VsetId,
        attachment: AttachmentId,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DetachMode {
    Graceful,
    Forced,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AdminReply {
    VsetCreated {
        req: ReqId,
        vset: VsetId,
    },
    CheckpointDone {
        req: ReqId,
        vset: VsetId,
        epoch: Epoch,
    },
    AdminFailed {
        req: ReqId,
    },
    /// Restore succeeded: this host now runs the vset, recovered per the
    /// verdict (R6.1).
    VsetRestored {
        req: ReqId,
        vset: VsetId,
        verdict: Verdict,
    },
    /// A backed-up vset finished local recovery AND the head confirmed this
    /// host still holds it with local state at least as new as the backup:
    /// it now serves, per the verdict.
    VsetRecovered {
        vset: VsetId,
        verdict: Verdict,
    },
    /// The base is durable in the store and forkable from any host (R5.2).
    BaseKept {
        req: ReqId,
        base: u64,
    },
    /// The base record's delete was issued: the base is unrooted (R9.3).
    BaseDeleted {
        req: ReqId,
        base: u64,
    },
    /// A fork came up: resumed if the base was whole (memory + vmstate),
    /// cold-booted if disk-only (R5.2).
    VsetForked {
        req: ReqId,
        vset: VsetId,
        verdict: Verdict,
    },
    /// The migration cut over: the destination runs the vset now.
    MigratedOut {
        req: ReqId,
        vset: VsetId,
    },
    /// An inbound migration is live on this host, per the verdict.
    VsetMigratedIn {
        vset: VsetId,
        verdict: Verdict,
    },
    DatabaseAttached {
        req: ReqId,
        vset: VsetId,
        attachment: AttachmentId,
    },
    DatabaseDetachStarted {
        req: ReqId,
        vset: VsetId,
        attachment: AttachmentId,
        forced: bool,
    },
    DatabaseDetached {
        req: ReqId,
        vset: VsetId,
        attachment: AttachmentId,
    },
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
