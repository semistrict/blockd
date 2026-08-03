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

use crate::journal::VsetConfig;
use crate::types::{Epoch, HostId, PageId, SegId, VolumeId, VsetId};
use std::fmt;

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

/// The daemon's synchronous window onto guest memory: in production this is
/// the shared mapping itself (plain loads); in simulation the harness's
/// guest-memory model. Only resident pages may be read.
///
/// **Capture contract**: the returned bytes must be *write-stable* — a
/// concurrent guest write must either be included in the returned bytes or
/// trap afterwards. A truly concurrent implementation achieves this by
/// arming write protection on the page *before* reading it (the capture's
/// later `WriteProtect` effect is then idempotent); the single-threaded
/// simulation gets it for free because effects apply atomically with the
/// step.
pub trait HostMap {
    fn read_page(&self, page: PageId) -> Vec<u8>;

    /// The accessed-bit harvest behind MGLRU-mirrored aging (R2.6): of the
    /// given resident pages, which were touched since the last harvest?
    /// Production reads and clears hardware accessed bits (idle page
    /// tracking — the daemon runs as root, R9.1); the simulation answers
    /// from the guests' true access history. The default sees nothing —
    /// aging then relies on the inline write-protect-fault promotions
    /// alone.
    fn harvest_accessed(&self, resident: &[PageId]) -> Vec<PageId> {
        let _ = resident;
        Vec::new()
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

/// Daemon-to-daemon messages (the mTLS peer protocol, R11.1 — membership
/// and encryption are the runtime's layer; the simulation models cluster
/// membership as the set of reachable hosts).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PeerMsg {
    /// Post-copy migration offer (R7.1): the final captured record's exact
    /// bytes. The sender has already made its outbound handoff durable
    /// (R7.2: durable on both sides before either acts).
    MigrateOffer { vset: VsetId, record: Vec<u8> },
    /// The destination's accept: its side of the handoff is durable and its
    /// guest is resuming. The source now serves fetches and never runs the
    /// guest again.
    MigrateAccept { vset: VsetId },
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
    Page { io: IoId, bytes: Option<Vec<u8>> },
    /// The destination holds every byte it needs: the source may reclaim
    /// the vset's local state.
    Released { vset: VsetId },
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
}

/// Timer identity; the daemon owns the meaning.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum TimerId {
    Writeback,
    /// Backup/claim retry for one vset after a store fault (R8.3: copies
    /// queue, with backpressure — never loss).
    Backup(VsetId),
    /// End of the post-resume recording window (R6.2): the pages faulted
    /// so far become the vset's recorded resume set.
    ResumeSet(VsetId),
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
    Admin(AdminCmd),
    /// A blob write became durable.
    BlobWriteDone {
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

/// Recovery verdict for one vset (R8.2: explicit, per vset).
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
