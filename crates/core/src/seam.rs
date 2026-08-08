//! The seam (R10.1): everything that crosses the daemon boundary. The daemon
//! is a sans-IO state machine — [`Event`]s in, [`Effect`]s out. In simulation
//! the kernel interprets effects against the world model; in production a
//! runtime interprets the same effects against real I/O.
//!
//! The guest boundary is userfaultfd (R9.1: MINOR + write-protect) over
//! guest RAM and virtio-pmem/DAX disks. Guests never send reads or writes:
//! a touched non-resident page raises a missing fault and the daemon
//! resolves it with a [`Effect::Fill`] (`UFFDIO_CONTINUE`); the first write to
//! a clean page raises a write-protect fault and the daemon resolves it with
//! [`Effect::Unprotect`]; after that, writes run at memory speed and the
//! daemon sees nothing until it captures — reading page contents directly
//! from the shared mapping ([`HostMap`]) and re-arming write-protection.
//! The one explicit guest request is the pmem sync (R3.7/R3.8).
//!
//! Everything durable crosses this seam as raw bytes. The world never
//! pre-verifies anything: damaged bytes come back exactly as stored, and the
//! daemon's own frame checks (R8.1) are the only line of defense.
//!
//! # Interpreter obligations
//!
//! What an interpreter (the simulation's world model, the runtime host, or
//! anything else) owes the daemon. The daemon's correctness proofs assume
//! exactly this much — an interpreter that provides less silently voids
//! them; one that provides more (the simulation applies everything
//! synchronously, in order, instantaneously) proves nothing about a weaker
//! interpreter. Divergence between interpreters lives here or nowhere.
//!
//! **Memory effects** ([`Effect::Fill`], [`Effect::FillShared`],
//! [`Effect::Unprotect`], [`Effect::WriteProtect`], [`Effect::Evict`]):
//! applied in emission order per page, before any later effect of the same
//! step that touches the same page. Capture reads through [`HostMap`] MUST
//! write-protect a page before reading it, so a concurrent guest store
//! either lands in the returned bytes or traps after them — never between
//! (the runtime's MMU provides this; the simulation's parked-vCPU model
//! mirrors it). A fill installed write-protected under a blocked writer
//! re-traps as a write-protect fault (real uffd's double fault); the
//! simulation models the retry explicitly.
//!
//! **Blob effects**: reads and writes may run concurrently and complete in
//! any order — write-once names make that safe, and the daemon only reads
//! names whose `BlobWriteDone` it has seen. [`Effect::BlobDelete`]s MUST
//! apply in emission order relative to each other: reclaim deletes a
//! vset's records before its handoff marker, and recovery's reading of
//! "records without a marker" as ownership depends on that order surviving
//! a crash mid-reclaim. Deletes need not be ordered against reads or
//! writes. A completed `BlobWriteDone` means durable through power loss —
//! file AND directory entries (the runtime fsyncs the parent chain).
//!
//! **Store effects**: freely concurrent, any completion order. Completions
//! are truthful: `Ok` means the store's answer, `Err(Unavailable)` means
//! the outcome is UNKNOWN (the operation may have applied — every caller
//! path must be idempotent under retry, and is).
//!
//! **Crash cut**: a crash may cut an effect batch at any prefix, and
//! within the surviving prefix an in-flight blob write may land whole,
//! vanish, or tear (the simulation's crash fates). Recovery must tolerate
//! every such cut; the delete-ordering rule above is what keeps the cuts
//! it cannot tolerate unreachable. Known model gap: the simulation applies
//! deletes instantly and cannot yet express a crash BETWEEN two deletes of
//! one batch — the runtime's single delete lane provides the order the
//! sim assumes.
//!
//! **Events**: delivery order between sources (faults, timers, completions,
//! peers) is unconstrained — the proofs quantify over orderings by seed.
//! [`Event::Timer`] fires no earlier than its `after`; it may fire
//! arbitrarily late. Peer delivery is at-least-once with duplication;
//! every peer handler is idempotent and, since R11.1, authorizes its
//! counterparty in the protocol, not the transport.

use crate::database::{DatabaseReply, DatabaseRequest};
use crate::types::{HostId, PageId, SegId, VolumeId, VsetId};

pub use crate::protocol::{
    AdminCmd, AdminReply, DetachMode, IoId, MAX_OBJECT_BYTES, PeerMsg, ReplicaArtifact,
    ReplicaCommitInfo, ReqId, StoreFault, Verdict,
};

/// The daemon's synchronous window onto guest memory: in production this is
/// the shared mapping itself (plain loads); in simulation the harness's
/// guest-memory model. Only resident pages may be read.
///
/// **Capture contract**: callers arm write protection before scheduling any
/// read. The returned bytes are then write-stable: a concurrent guest write
/// either traps before changing them or happens after copy-on-fault has saved
/// them. The single-threaded simulation gets the same ordering because effects
/// apply atomically with a step.
pub trait HostMap {
    /// Arm dirty pages before a synchronous small capture reads them. The
    /// production view collapses these into contiguous ranges; modeled views
    /// may leave this as the default no-op and apply the matching effect.
    fn arm_write_protect(&self, _pages: &[PageId]) {}

    fn read_page(&self, page: PageId) -> Vec<u8>;

    /// The accessed-bit harvest behind MGLRU-mirrored aging (R2.6): which
    /// pages were touched since the last harvest? Reporting is one-shot
    /// (each access is returned once) and may include pages no longer
    /// resident — the cache skips those. The cost contract matters: a
    /// harvest is O(touched), never O(resident); production reads and
    /// clears hardware accessed bits (idle page tracking — the daemon runs
    /// as root, R9.1), the simulation answers from the guests' true access
    /// history. The default sees nothing — aging then relies on the inline
    /// write-protect-fault promotions alone.
    fn harvest_accessed(&self) -> Vec<PageId> {
        Vec::new()
    }
}

/// Timer identity; the daemon owns the meaning.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum TimerId {
    Writeback,
    /// Backup/claim retry for one vset after a store fault (R8.3: copies
    /// queue, with backpressure — never loss).
    Backup(VsetId),
    /// Retry the currently unacknowledged passive-replica protocol message.
    Replica {
        vset: VsetId,
        generation: u64,
    },
    /// Retry a release until the passive peer confirms durable unlink.
    ReplicaRelease(VsetId),
    /// Retry a passive peer's asynchronous spool-to-store upload.
    ReplicaUpload {
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        generation: u64,
    },
    /// End of the post-resume recording window (R6.2): the pages faulted
    /// so far become the vset's recorded resume set.
    ResumeSet(VsetId),
    /// Re-send a migration offer until the destination accepts. The peer
    /// channel is at-least-once; every migration handler is idempotent.
    MigrateOffer(VsetId),
    /// Re-issue a peer fetch that has gone unanswered (a lost `FetchRange`
    /// or `Page` must not hang a guest forever).
    PeerRetry(IoId),
    /// Re-issue demand fills parked by a store outage (R8.3: an outage is
    /// not absence — the blocked guests wait it out, they are not killed).
    FillRetry(VsetId),
    /// Continue an in-flight incremental capture drain (armed at a single
    /// write-protected instant, read out a bounded batch per step). Set
    /// with `after: 0`: the next batch runs as soon as the loop has served
    /// whatever else is waiting — that yield is the entire point.
    CaptureStep(VsetId),
    /// Continue bounded decompression of a verified compaction victim.
    CompactStep(VsetId),
    /// Continue a database request after a bounded page-copy slice.
    DatabaseStep(VsetId),
    /// Post-migration hydration tick (R7.1's tail drain): pull pages whose
    /// locations still reference the source until none remain, then release
    /// the source.
    Hydrate(VsetId),
    /// Retry a restore whose store operations hit an outage (R8.3: outages
    /// queue work, they never fail it).
    RestoreRetry(VsetId),
    /// Re-issue store fetches for map leaves still pending after a fault
    /// (lazy hydration never gives up on a transient outage).
    LeafRetry(VsetId),
    /// Retry an async database page fetch parked by a store outage.
    DatabaseRetry(VsetId),
    /// Start a detached database vset's final migration capture.
    DatabaseMigrate(VsetId),
    /// Retry assignment-head transfer for an inbound backed database move.
    DatabaseMigrateHead(VsetId),
}

/// Everything that can happen to the daemon.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Event {
    /// A vCPU faulted on a page and is blocked until the daemon resolves it:
    /// a missing fault if the page is non-resident (resolve with `Fill`), a
    /// write-protect fault if it is resident and protected (resolve with
    /// `Unprotect`). `write` is the access type.
    GuestFault {
        page: PageId,
        write: bool,
    },
    /// virtio-pmem sync on a disk volume (R3.7/R3.8).
    GuestSync {
        req: ReqId,
        volume: VolumeId,
    },
    /// One operation decoded from a VM-authenticated database transport.
    Database(DatabaseRequest),
    /// The VMM paused the vset's vCPUs (response to `PauseGuest`); `vmstate`
    /// is the serialized device/vCPU state captured at the pause instant.
    GuestPaused {
        vset: VsetId,
        vmstate: u64,
    },
    /// A peer message arrived (mTLS-authenticated cluster member, R11.1).
    PeerDelivered {
        from: HostId,
        msg: PeerMsg,
    },
    /// Runtime-prepared replica artifact. Expensive frame verification and
    /// spool sealing happened off the decider; `frame: None` means either
    /// checksum or artifact validation failed.
    ReplicaPutPrepared {
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
        frame: Option<Vec<u8>>,
    },
    Admin(AdminCmd),
    /// A blob write became durable.
    BlobWriteDone {
        io: IoId,
    },
    /// A passive-replica spool unlink did not complete. The receiver leaves
    /// the residue authoritative and permits the source's release retry to
    /// issue another durable delete.
    ReplicaDeleteFailed {
        io: IoId,
    },
    /// A blob (or range) read completed: raw stored bytes, damage included;
    /// `None` means the blob does not exist. A range read of a short blob
    /// returns fewer bytes than asked — the frame checks catch it.
    BlobReadDone {
        io: IoId,
        bytes: Option<Vec<u8>>,
    },
    /// An object-store write (put or CAS) completed; `Ok` carries the new
    /// object version.
    StorePutDone {
        io: IoId,
        result: Result<u64, StoreFault>,
    },
    /// An object-store read (get or ranged get) completed; payload bytes are
    /// verbatim, damage included (R8.1 applies to the store too).
    StoreGetDone {
        io: IoId,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
    },
    Timer(TimerId),
}

/// Everything the daemon can do to the world.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Effect {
    /// Resolve a missing fault: install `bytes` into the mapping
    /// (`UFFDIO_CONTINUE`). The page becomes resident; write-protected unless
    /// `writable` (a write fault dirties it immediately). When `share` is
    /// set, the bytes are a base page entering the host's shared tier
    /// (R5.3): later forks map the same physical page.
    Fill {
        page: PageId,
        bytes: Vec<u8>,
        writable: bool,
        share: Option<(u64, u64, SegId, u32)>,
    },
    /// Resolve a missing fault by mapping the already-resident shared base
    /// page — zero-copy, no I/O, no new physical page (R5.3). `writable`
    /// means the guest is about to write: the harness/runtime installs a
    /// private copy instead (copy-on-write divergence).
    FillShared {
        page: PageId,
        share: (u64, u64, SegId, u32),
        writable: bool,
    },
    /// The page exists nowhere intact: the faulting guest hangs and is
    /// killed loudly (R8.1/R8.2).
    FillFailed {
        page: PageId,
    },
    /// Resolve a write-protect fault: writes may proceed.
    Unprotect {
        page: PageId,
    },
    /// Re-arm write tracking on captured pages (`UFFDIO_WRITEPROTECT`).
    WriteProtect {
        pages: Vec<PageId>,
    },
    /// Drop a clean resident page from the mapping (`MADV_DONTNEED`): the
    /// next touch is a missing fault (R2.1/R2.4).
    Evict {
        page: PageId,
    },
    /// Install or replace one database cache page. Interpreters apply this
    /// before a later reply in the same batch.
    DatabaseInstall {
        page: PageId,
        bytes: Vec<u8>,
    },
    /// Complete a database request. Request ids permit out-of-order transport
    /// delivery even though each database accepts mutations in event order.
    Database(DatabaseReply),
    /// Pause the vset's vCPUs for a checkpoint capture (R3.1); the VMM
    /// answers with `GuestPaused`.
    PauseGuest {
        vset: VsetId,
    },
    /// Resume after capture. The pause-to-resume window is the guest-visible
    /// checkpoint pause and must stay within budget (R3.1).
    ResumeGuest {
        vset: VsetId,
    },
    /// Sync acknowledgment: only after the sync's ordering is durably locked
    /// in (R3.8).
    SyncOk {
        req: ReqId,
    },
    /// Sync of something that is not a disk volume of a live vset (R11.2).
    SyncFailed {
        req: ReqId,
    },
    /// Durably write a new blob (write-once: names are never reused).
    BlobWrite {
        io: IoId,
        name: String,
        bytes: Vec<u8>,
    },
    /// Append one verified frame to a typed passive-replica spool and make
    /// the append durable before completing `io`.
    ReplicaAppend {
        io: IoId,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        generation: u64,
        bytes: Vec<u8>,
    },
    /// Durably unlink every generation through the named one before
    /// completing `io`.
    ReplicaDelete {
        io: IoId,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        through_generation: u64,
    },
    /// Truncate a crash-torn replica tail to the last verified frame.
    ReplicaTruncate {
        io: IoId,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        generation: u64,
        len: u64,
    },
    /// Read a whole blob.
    BlobRead {
        io: IoId,
        name: String,
    },
    /// Read a byte range of a blob (the fault path, R2.3).
    BlobReadRange {
        io: IoId,
        name: String,
        offset: u64,
        len: u64,
    },
    /// Reclaim a superseded blob (R4.5: explicit, never by age).
    BlobDelete {
        name: String,
    },
    SetTimer {
        timer: TimerId,
        after: u64,
    },
    /// Unconditional object write (write-once keys only: segments and
    /// manifests live in fence namespaces, R6.4).
    StorePut {
        io: IoId,
        key: String,
        bytes: Vec<u8>,
    },
    /// Conditional object write — the CAS of R6.3 (head records only).
    StoreCas {
        io: IoId,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    },
    /// Read a whole object.
    StoreGet {
        io: IoId,
        key: String,
    },
    /// Ranged object read (the store-tier fill path, R2.3).
    StoreGetRange {
        io: IoId,
        key: String,
        offset: u64,
        len: u64,
    },
    /// Reclaim a superseded object (R4.5: explicit, never by age).
    StoreDelete {
        key: String,
    },
    /// This host lost the vset's assignment (R6.4): it stops serving, its
    /// guests hang and are killed by the node manager.
    VsetFenced {
        vset: VsetId,
    },
    /// One vset's recovery material is locally unservable. The node manager
    /// kills only that guest; other vsets on the daemon remain available.
    VsetUnservable {
        page: PageId,
    },
    /// Send a message to a cluster peer (R11.1).
    PeerSend {
        to: HostId,
        msg: PeerMsg,
    },
    Admin(AdminReply),
    /// R8.2: unrecoverable local fault — the process dies loudly, now.
    Abort {
        reason: &'static str,
    },
}
