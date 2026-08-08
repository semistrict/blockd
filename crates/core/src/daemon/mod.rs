//! The blockd daemon for one host, as a sans-IO state machine (R10.1).
//!
//! Guest boundary (userfaultfd, R9.1): a missing fault is resolved by a
//! `Fill` (`UFFDIO_CONTINUE`) whose bytes the daemon fetched and verified from
//! storage; a write-protect fault marks the page dirty and resolves with
//! `Unprotect`; from then on guest writes run at memory speed, invisible
//! until a *capture* — background writeback (R2.4), a sync commit (R3.8),
//! or a whole-vset checkpoint (R3.1) — reads the dirty pages straight out of
//! the shared mapping ([`HostMap`]), re-write-protects them, and persists
//! them: one fresh write-once segment blob, then one journal record holding
//! the vset's full page→location map at the capture instant. The record is
//! the atomic point (R3.5): a crash leaves exactly the old or exactly the
//! new consistency point, and torn tails fail checksum verification (R8.1).
//! Superseded blobs are deleted only after the newer record is durable
//! (R4.5), so per-vset storage never grows with checkpoint count (R3.4).
//!
//! Checkpoints pause the guest only for the capture snapshot itself:
//! `PauseGuest` → (VMM hands over vmstate) → snapshot + `ResumeGuest`;
//! everything after the pause — segment writes, the record, backup — is
//! background (R3.1).
//!
//! Sync ordering (R3.8): every record carries the vset's monotone
//! *synced-through watermark* — the highest sync barrier acknowledged (or
//! acknowledged by that record's own durability). A sync is acknowledged
//! only once a durable record's watermark covers it, and recovery never
//! resumes a checkpoint whose capture predates the highest watermark on
//! disk. The watermark is monotone across records, so reclaiming the record
//! that originally covered a sync can never lose the constraint.

mod backup;
mod capture;
mod database;
mod guest;
mod lineage;
mod migrate;
mod recover;
pub use recover::RecoveryBlob;
mod replica;
mod restore;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::cache::Cache;
use crate::head::ManifestPtr;
use crate::journal::{DatabaseMeta, DurabilityMode, JournalRecord, VsetConfig};
use crate::layout;
use crate::mapleaf::LeafPtr;
use crate::placement::{PeerCandidate, rank_stash_candidates};
use crate::seam::{Effect, Event, HostMap, IoId, ReqId, TimerId};
use crate::segment::PageLoc;
use crate::types::{Epoch, Gen, HostId, JournalSeq, PageId, SegId, VsetId};

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    /// This daemon's host identity (from the control plane roster, R6.5).
    pub host: HostId,
    /// Host cache capacity in pages (memory overcommit is real: the sum of
    /// vset sizes may far exceed this, R1.3).
    pub cache_pages: usize,
    /// Continuous writeback cadence (R2.4), nanoseconds.
    pub writeback_interval: u64,
    /// Retry delay after an object-store fault (R8.3), nanoseconds.
    pub backup_retry: u64,
    /// Local NVMe capacity in bytes (`None` = unbounded). R2.7: pressure
    /// slows, reclaims, and stalls — it never corrupts and never kills.
    pub disk_capacity: Option<u64>,
    /// Reclaim starts when usage crosses `capacity - headroom`, leaving room
    /// for in-flight writeback to complete (R2.7).
    pub disk_headroom: u64,
    /// Liveness watch (R9.2): writeback ticks a stalled concern — parked
    /// guests, hydration, an outbound handoff — may go without its
    /// progress signal before a wedge counter fires. A still-wedged
    /// concern re-fires every interval (silence would read as recovery).
    /// 0 disables the watch.
    pub wedge_ticks: u64,
    /// Versioned authenticated roster used only to choose one passive stash.
    /// `None` keeps peer-stashed creation disabled on this host.
    pub replica_placement: Option<ReplicaPlacementConfig>,
}

#[derive(Clone, Debug)]
pub struct ReplicaPlacementConfig {
    pub membership_epoch: u64,
    pub local_failure_domain: u16,
    pub roster: Vec<PeerCandidate>,
}

/// The operable minimum of observability (R9.2). Plain counters; the
/// production runtime exports them as Prometheus series.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Missing faults served from storage.
    pub fills: u64,
    /// Missing faults of never-written pages (zero fill).
    pub zero_fills: u64,
    /// Missing faults served by mapping an already-resident shared base
    /// page — zero-copy, zero I/O (R5.3).
    pub shared_fills: u64,
    /// First-writes caught by write protection.
    pub wp_faults: u64,
    /// Guest pages transitioning from clean to dirty. Unlike `wp_faults`,
    /// this includes write-intent missing faults.
    pub guest_pages_dirtied: u64,
    /// Faults for pages that exist nowhere intact (R8.1's loud failure).
    pub faults_unservable: u64,
    pub pressure_waits: u64,
    pub pages_flushed: u64,
    pub records_written: u64,
    pub checkpoints_done: u64,
    pub syncs_acked: u64,
    pub guest_rejected: u64,
    /// Peer messages refused by a protocol guard (R11.1): a `Released` or
    /// fetch from the wrong counterparty, or a stale re-offer of a
    /// released incarnation. Authorization lives in the protocol, not
    /// merely the transport.
    pub peer_rejected: u64,
    pub blobs_deleted: u64,
    /// Manifests made durable in the object store (R4.2).
    pub manifests_published: u64,
    /// Publishes/claims deferred by a store fault (R8.3).
    pub store_retries: u64,
    /// Times this host lost a vset to another holder's claim (R6.4).
    pub fenced: u64,
    /// Successful object-store assignment claims (create or restore).
    pub assignment_claims: u64,
    /// Assignment claims lost because another holder won the CAS.
    pub assignment_claim_conflicts: u64,
    /// Local segments dropped because backup holds them (R2.7's droppable
    /// class); refaults refetch from the store.
    pub nvme_reclaims: u64,
    /// Captures deferred because the disk is at capacity (R2.7's explicit
    /// coupled stall: writeback waits, so memory relief waits, so guests
    /// slow — loudly, never corruptly).
    pub nvme_stalls: u64,
    /// Resume-set pages prefetched into the cache after a restore (R6.2).
    pub prefetch_fills: u64,
    /// Tail pages pulled from the migration source in the background
    /// (post-copy hydration, R7.1).
    pub hydrate_fills: u64,
    /// Peer fetches re-issued after going unanswered (lossy channel).
    pub peer_retries: u64,
    /// Armed-but-unread pages captured out of order because the guest
    /// wrote them mid-drain (2a-full's copy-on-fault).
    pub cow_captures: u64,
    /// Wedge incidents (R9.2): a vset held parked guests for
    /// `wedge_ticks` writeback ticks with no fill landing.
    pub wedged_guests: u64,
    /// Wedge incidents: a hydrating vset (post-copy tail, pending map
    /// leaves, or an unacked release) made no hydration progress for
    /// `wedge_ticks` ticks.
    pub wedged_hydration: u64,
    /// Wedge incidents: an outbound (handed-off) vset served its
    /// destination nothing for `wedge_ticks` ticks — a vanished
    /// destination, or an accept that never arrives.
    pub wedged_outbound: u64,
    /// Map spans rolled into fresh leaf blobs (the amortized half of the
    /// map's write cost; records carry only the overlay).
    pub leaf_rolls: u64,
    /// Map leaves hydrated lazily (restore/migration/fork adoption).
    pub leaf_fills: u64,
    /// Mostly-dead segments whose live pages were rewritten forward, so
    /// the segment could be reclaimed (the space-amplification bound).
    pub segs_compacted: u64,
    /// Live pages rewritten forward by compaction.
    pub pages_compacted: u64,
    /// Verbatim bytes appended to passive replica spools.
    pub replica_bytes: u64,
    /// Replica messages rejected for identity, epoch, integrity, or ordering.
    pub replica_rejected: u64,
    /// Durable replica commits accepted by this host.
    pub replica_commits: u64,
    /// Immutable artifact/manifest bytes sent from a passive spool to S3.
    pub replica_store_bytes: u64,
    /// Replica spool files unlinked after a covering fenced head publish.
    pub replica_unlinks: u64,
    /// Artifact bytes sent from a primary to its one selected peer.
    pub replica_network_bytes: u64,
    /// Unique compressed recovery bytes produced for passive replication.
    pub replica_logical_bytes: u64,
    /// Artifact bytes sent anywhere except the active/transition target.
    pub replica_nonactive_bytes: u64,
    /// Bytes used to seed a replacement during a fenced transition.
    pub replica_replacement_bytes: u64,
    /// Cleanup bytes copied forward. This invariant counter remains zero.
    pub replica_cleanup_rewrite_bytes: u64,
    pub replica_artifact_flushes: u64,
    pub replica_commit_flushes: u64,
    /// New spool generations opened because the prior generation reached
    /// its size bound.
    pub replica_rotations: u64,
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
    pub source_capacity_bytes: u64,
    pub current_generation: u64,
    pub committed_through: u64,
    pub uploaded_through: u64,
}

/// One live vset's current operational state. This is deliberately a
/// point-in-time view: exporters can remove retired vsets instead of
/// leaving stale high-cardinality series behind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VsetStats {
    pub vset: VsetId,
    pub backed_up: bool,
    pub role: VsetRole,
    pub fence: u64,
    pub dirty_pages: usize,
    pub unstable_pages: usize,
    pub parked_faults: usize,
    pub pending_syncs: usize,
    pub pending_leaf_spans: usize,
    pub hydration_remaining_pages: usize,
    pub backup_lag_captures: Option<u64>,
    pub backup_lag_bytes: Option<u64>,
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
pub struct VsetOperations(u8);

impl VsetOperations {
    pub const CAPTURE: u8 = 1;
    pub const CHECKPOINT: u8 = 2;
    pub const BACKUP: u8 = 4;
    pub const HYDRATION: u8 = 8;

    pub fn active(self, operation: u8) -> bool {
        self.0 & operation != 0
    }
}

/// Host-wide gauges needed to operate pressure, writeback and hydration.
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

/// The page→location map of one consistency point.
type PageMap = BTreeMap<PageId, (Gen, PageLoc)>;

/// One entry rescued by compaction: identity, generation at read, bytes.
type Rescue = (PageId, Gen, Vec<u8>);

/// One in-flight capture: a consistency point on its way to durability.
// `capture_seq` deliberately matches the JournalRecord field it becomes.
#[allow(clippy::struct_field_names)]
#[derive(Debug)]
struct Capture {
    capture_seq: u64,
    /// Database file metadata at the same acceptance cut as this map.
    database: DatabaseMeta,
    /// `Some` for checkpoints: (new epoch, vmstate, requester).
    checkpoint: Option<(Epoch, u64, ReqId)>,
    /// The map at the capture instant: bounded overlay + one leaf per
    /// span. The record is exactly these two.
    overlay: PageMap,
    leaf_table: BTreeMap<u32, LeafPtr>,
    /// The content generations of the leaves this capture ROLLED — what
    /// lets the finalize shrink the vset's overlay to entries genuinely
    /// newer than the adopted leaves.
    rolled_gens: BTreeMap<PageId, Gen>,
    /// Blob writes (segment + rolled leaves) the record still waits on.
    writes_pending: usize,
    /// Record copies (primary + mirror) still in flight; the consistency
    /// point exists — and syncs ack — only when both are durable.
    record_writes: u8,
    /// The watermark this capture's record carries; fixed when the record
    /// write is issued.
    sync_covered_through: u64,
    /// The exact record written for this capture (kept for backup: the
    /// manifest is these bytes, verbatim).
    record: Option<JournalRecord>,
}

#[derive(Clone, Debug)]
enum Pending {
    SegWrite {
        vset: VsetId,
        seq: JournalSeq,
        /// Only the entries in this segment become durable on this completion.
        new_locs: Vec<(PageId, (Gen, PageLoc))>,
        /// Cache pages represented by this segment; compaction entries are not
        /// cache-resident and therefore absent.
        flushes: Vec<PageId>,
    },
    /// A rolled map leaf on its way to disk, gating its capture's record.
    LeafWrite {
        vset: VsetId,
        seq: JournalSeq,
    },
    /// A hydrated leaf's local verbatim copy (no capture waits on it).
    LeafCopyWrite,
    RecordWrite {
        vset: VsetId,
        seq: JournalSeq,
    },
    ReplicaArtifactAppend {
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        artifact: crate::seam::ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
        frame_len: u64,
    },
    ReplicaCommitAppend {
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        info: crate::seam::ReplicaCommitInfo,
        record_checksum: u32,
        frame_len: u64,
    },
    ReplicaUploadArtifact {
        key: ReplicaKey,
        artifact: crate::seam::ReplicaArtifact,
    },
    ReplicaUploadManifest {
        key: ReplicaKey,
        info: crate::seam::ReplicaCommitInfo,
    },
    ReplicaHeadCas {
        vset: VsetId,
        info: crate::seam::ReplicaCommitInfo,
        ptr: ManifestPtr,
        record: JournalRecord,
    },
    ReplicaTransitionCas {
        vset: VsetId,
        assignment: crate::head::StashAssignment,
    },
    ReplicaActivateCas {
        vset: VsetId,
        assignment: crate::head::StashAssignment,
        retired: crate::head::RetiredStash,
        info: crate::seam::ReplicaCommitInfo,
    },
    ReplicaHistoryCas {
        vset: VsetId,
        removed: crate::head::RetiredStash,
    },
    ReplicaReleaseDelete {
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        through: crate::seam::ReplicaCommitInfo,
    },
    ReplicaTailTruncate {
        key: ReplicaKey,
        generation: u64,
    },
    /// Lazy hydration: a leaf object fetch from the store.
    LeafGet {
        vset: VsetId,
        span: u32,
        ptr: LeafPtr,
    },
    /// Serving a peer's leaf fetch from local storage.
    PeerLeafRead {
        requester: HostId,
        peer_io: IoId,
    },
    /// Lazy hydration: a leaf fetch from the migration source.
    PeerLeafFetch {
        vset: VsetId,
        span: u32,
        ptr: LeafPtr,
    },
    /// Compaction: a mostly-dead segment read back whole so its live
    /// pages can ride the next capture into a fresh home. Does not pin —
    /// a concurrently deleted blob just yields nothing to rescue.
    CompactRead {
        vset: VsetId,
        fence: u64,
        seg: SegId,
    },
    Fetch {
        page: PageId,
        write: bool,
        generation: Gen,
        /// Pins the segment against cleanup while the read is in flight.
        loc: PageLoc,
    },
    /// Database byte I/O fetching a durable page from local storage.
    DatabaseFetch {
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
    },
    /// Database byte I/O fallback fetch from object storage.
    DatabaseStoreFetch {
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
    },
    /// Database byte I/O fallback fetch from a migration source.
    DatabasePeerFetch {
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
    },
    /// Store-tier fill (R2.3: local first, then the store).
    StoreFetch {
        page: PageId,
        write: bool,
        generation: Gen,
        loc: PageLoc,
    },
    /// Backup pipeline: local read of a segment on its way to the store.
    PubSegRead {
        vset: VsetId,
        fence: u64,
        seg: SegId,
    },
    /// Backup pipeline: segment object write.
    PubSegPut {
        vset: VsetId,
        fence: u64,
        seg: SegId,
    },
    /// Backup pipeline: local read of a map leaf on its way to the store.
    PubLeafRead {
        vset: VsetId,
        fence: u64,
        id: u64,
    },
    /// Backup pipeline: leaf object write.
    PubLeafPut {
        vset: VsetId,
        fence: u64,
        id: u64,
    },
    /// Passive-replica sender: local immutable artifact read.
    ReplicaSourceRead {
        vset: VsetId,
        artifact: crate::seam::ReplicaArtifact,
    },
    /// Backup pipeline: manifest object write.
    PubManifestPut {
        vset: VsetId,
        ptr: ManifestPtr,
    },
    /// Backup pipeline: head CAS advancing the manifest pointer.
    PubHeadCas {
        vset: VsetId,
        ptr: ManifestPtr,
    },
    /// Head creation CAS at vset creation (backed-up vsets).
    HeadCreate {
        vset: VsetId,
    },
    /// Head re-read after local recovery (takeover detection, R6.4).
    HeadRefresh {
        vset: VsetId,
    },
    /// Restore: initial head read.
    RestoreHeadGet {
        req: ReqId,
        vset: VsetId,
    },
    /// Backed database migration: read the assignment head after the
    /// provisional inbound record is durable.
    MigrateHeadGet {
        vset: VsetId,
        source: HostId,
    },
    /// Backed database migration: claim the head while preserving its newest
    /// backup manifest.
    MigrateHeadClaim {
        vset: VsetId,
        manifest: Option<ManifestPtr>,
    },
    /// Restore: claim CAS.
    RestoreClaim {
        req: ReqId,
        vset: VsetId,
        ptr: ManifestPtr,
    },
    /// Restore: manifest fetch after a won claim.
    RestoreManifestGet {
        req: ReqId,
        vset: VsetId,
        ptr: ManifestPtr,
        fence: u64,
    },
    /// Base keep: local read of a pinned segment on its way to the base.
    KeepSegRead {
        vset: VsetId,
        fence: u64,
        seg: SegId,
    },
    /// Base keep: base-namespace segment write.
    KeepSegPut {
        vset: VsetId,
        fence: u64,
        seg: SegId,
    },
    /// Base keep: local read of a map leaf on its way to the base.
    KeepLeafRead {
        vset: VsetId,
        fence: u64,
        id: u64,
    },
    /// Base keep: base-namespace leaf write (content re-homed).
    KeepLeafPut {
        vset: VsetId,
        fence: u64,
        id: u64,
    },
    /// Base keep: the base record write.
    KeepRecordPut {
        vset: VsetId,
        base: u64,
        req: ReqId,
    },
    /// Fork: base record fetch.
    ForkBaseGet {
        vset: VsetId,
        base: u64,
    },
    /// Migration: the outbound handoff marker write.
    HandoffWrite {
        vset: VsetId,
    },
    /// Migration: serving a peer's fetch from local storage.
    PeerRead {
        requester: HostId,
        peer_io: IoId,
    },
    /// Migration: a fill waiting on the source peer (the peer tier, R2.3).
    PeerFetch {
        page: PageId,
        write: bool,
        generation: Gen,
        loc: PageLoc,
    },
    /// Hydration: a tail page being pulled from the source in the
    /// background (no guest is waiting; the tick re-issues losses).
    HydrateFetch {
        page: PageId,
        generation: Gen,
    },
    /// Resume-set publish (R6.2): fire-and-forget, best effort.
    ResumeSetPut,
    /// Restore: resume-set fetch after the verdict (R6.2).
    RestoreRsGet {
        vset: VsetId,
    },
    /// Restore: one prefetched resume-set page in flight (R6.2).
    Prefetch {
        page: PageId,
        generation: Gen,
        loc: PageLoc,
    },
}

/// One vset's liveness watch (R9.2). Each concern pairs a monotone
/// progress signal (bumped where the work actually lands) with a count of
/// consecutive writeback ticks the concern was active but the signal did
/// not move; crossing `wedge_ticks` fires the matching incident counter.
#[derive(Debug, Default)]
struct Wedge {
    /// Fills served to this vset, any tier — parked guests' progress.
    fills: u64,
    fills_seen: u64,
    parked_ticks: u64,
    /// Hydration events: leaf arrivals resolved, tail pages installed.
    hydration: u64,
    hydration_seen: u64,
    hydration_ticks: u64,
    /// Peer fetches served while outbound — the destination's drain is
    /// this vset's only remaining purpose; silence is the wedge.
    served: u64,
    served_seen: u64,
    outbound_ticks: u64,
}

// The flags are independent protocol states, not an encoding smell.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct Vset {
    config: VsetConfig,
    /// Durable `SQLite` file namespace metadata; empty for compute vsets.
    database: DatabaseMeta,
    /// Newest database metadata known to be represented by a durable record.
    database_durable: DatabaseMeta,
    /// Volatile attachment, handles, and serialized database request queue.
    database_runtime: database::DatabaseRuntime,
    /// Spans whose next record must rebuild a leaf after truncate/delete.
    database_prune_spans: BTreeMap<u32, u64>,
    /// This holder incarnation's fence: the head record's CAS version at
    /// claim (R6.3). Everything this incarnation writes is namespaced by it.
    fence: u64,
    /// Vset is usable once its first record (seq 0) is durable.
    ready: bool,
    create_req: Option<ReqId>,
    epoch: Epoch,
    /// Monotone mutation counter, bumped per first-dirty (write-protect
    /// fault or write-intent fill); sync barriers and capture instants.
    mutation_seq: u64,
    /// Serving map: every written page's newest durable location.
    /// Invariant: `page_locs = materialize(leaf_table) ⊕ overlay`, except
    /// for spans still in `pending_leaves` (their leaf half is unknown).
    page_locs: PageMap,
    /// Live bytes per own-namespace segment, tracking every supersession
    /// in the serving map — what compaction selects mostly-dead victims
    /// on. Maintained only through [`Vset::map_adopt`] and
    /// [`Vset::rebuild_seg_live`].
    seg_live: BTreeMap<(u64, SegId), u64>,
    /// In-flight compaction reads: mostly-dead segments being read back.
    compacting: BTreeSet<(u64, SegId)>,
    /// Verified compaction victims awaiting bounded per-entry decompression.
    compact_decode: VecDeque<capture::CompactDecode>,
    /// Compaction's rescues, riding the next capture into a fresh
    /// segment: per victim, its live entries. Volatile — a crash or
    /// stall just re-runs the compaction.
    compact_stash: Vec<((u64, SegId), Vec<Rescue>)>,
    /// The durable map's sharded half: one leaf per span, as the newest
    /// record references it.
    leaf_table: BTreeMap<u32, LeafPtr>,
    /// Entries newer than their span's leaf; what the next record carries
    /// inline. Bounded by the roll rule (plus pending spans, transiently).
    overlay: PageMap,
    /// Spans whose leaf content is not yet local (lazy hydration): faults
    /// into them park until the leaf arrives.
    pending_leaves: BTreeMap<u32, LeafPtr>,
    /// Parked faults per pending span.
    leaf_waiters: BTreeMap<u32, Vec<(PageId, bool)>>,
    /// Spans whose leaf is missing or corrupt everywhere: their pages are
    /// unservable (R8.1's loud failure).
    dead_spans: BTreeSet<u32>,
    /// A store fault deferred some leaf fetches; the retry timer is armed.
    leaf_retrying: bool,
    /// Local leaf blobs: ptr → (bytes, segments its entries reference).
    leaf_blobs: BTreeMap<LeafPtr, (u64, BTreeSet<(u64, SegId)>)>,
    /// Own-namespace leaves already durable in the store.
    backed_leaves: BTreeSet<(u64, u64)>,
    next_leaf: u64,
    /// Newest durable record: `(capture_seq, seq)`.
    best: Option<(u64, JournalSeq)>,
    /// Pinned checkpoint record (its blobs are never reclaimed) — the
    /// material a base keep publishes (R5.2).
    pinned: Option<JournalRecord>,
    captures: BTreeMap<JournalSeq, Capture>,
    /// A commit capture is running (checkpoints tracked via `ckpt_running`).
    commit_running: bool,
    /// A checkpoint is running: paused, capturing, or writing.
    ckpt_running: bool,
    /// The requester whose `PauseGuest` is outstanding.
    ckpt_pausing: Option<ReqId>,
    ckpt_queue: VecDeque<ReqId>,
    /// Checkpoint idempotency (R3.5): request → outcome. Bounded — old
    /// outcomes age out FIFO so eternal checkpointing accrues no memory
    /// debt (R3.4); a client retrying across that horizon re-runs the
    /// checkpoint, which R3.5's atomicity makes harmless.
    ckpt_done: BTreeMap<ReqId, Epoch>,
    ckpt_done_order: VecDeque<ReqId>,
    pending_syncs: Vec<(ReqId, u64)>,
    /// Database sync waiters use database replies rather than virtio-pmem
    /// acknowledgements, but share the same durable watermark.
    pending_database_syncs: Vec<(ReqId, u64)>,
    /// Demand fills parked by a store outage (R8.3), re-issued by the
    /// `FillRetry` timer: each entry's guest is blocked on it.
    store_fill_retry: Vec<(PageId, bool, Gen, crate::segment::PageLoc)>,
    /// Highest sync barrier covered by a durable record's watermark (R3.8).
    local_covered_through: u64,
    /// Highest barrier whose configured durability domain is proven. For
    /// ordinary modes this follows local coverage; peer-stashed mode advances
    /// only from a peer commit or a covering S3 publication.
    sync_ack_through: u64,
    next_gen: u64,
    next_seq: u64,
    next_seg: u64,
    /// Journal records written and not yet deleted: seq → (writer fence,
    /// synced-through watermark). Cleanup must never drop the highest
    /// watermark.
    record_ws: BTreeMap<JournalSeq, (u64, u64)>,
    /// Local segments: (fence, seg, size in bytes).
    seg_blobs: Vec<(u64, SegId, u64)>,
    /// The newest durable record itself (its bytes are the next manifest).
    best_record: Option<JournalRecord>,
    /// Known store version of the head record (CAS expectation); `None`
    /// until learned or while unowned.
    head_version: Option<u64>,
    head_refreshing: bool,
    stash_assignment: Option<crate::head::StashAssignment>,
    retired_stashes: Vec<crate::head::RetiredStash>,
    /// Newest manifest durable in the store (R4.2); the loss bound on host
    /// death is `best` minus this (R4.3).
    backed: Option<ManifestPtr>,
    backed_segs: BTreeSet<(u64, SegId)>,
    store_manifests: BTreeSet<(u64, JournalSeq)>,
    publish: Option<Publish>,
    /// Artifacts already acknowledged by the current passive peer. Volatile:
    /// after restart, identical puts are safely re-acknowledged by the peer.
    peer_artifacts: BTreeSet<crate::seam::ReplicaArtifact>,
    peer_committed: Option<crate::seam::ReplicaCommitInfo>,
    peer_committed_record: Option<JournalRecord>,
    store_published_through: u64,
    peer_upload_done: Option<(u64, crate::seam::ReplicaCommitInfo)>,
    replica_head_inflight: bool,
    replica_assignment_inflight: bool,
    replica_assignment_proposal: Option<(
        crate::head::StashAssignment,
        Option<crate::seam::ReplicaCommitInfo>,
    )>,
    replica_send: Option<ReplicaSend>,
    replica_release: Option<(HostId, u64, crate::seam::ReplicaCommitInfo)>,
    replica_release_queue: VecDeque<(HostId, u64, crate::seam::ReplicaCommitInfo)>,
    replica_history_inflight: bool,
    /// Backed-up recovery holds its verdict until the head confirms this
    /// host still owns the vset and local state is not behind the backup.
    pending_verdict: Option<crate::seam::Verdict>,
    /// In-flight base keep (R5.2).
    keep: Option<lineage::BaseKeep>,
    /// This vset was created as a fork of the given base (R5.1).
    fork_from: Option<u64>,
    /// The base's vmstate, if the fork resumes.
    fork_vmstate: Option<u64>,
    /// Reply verdict for a fork, delivered when its first record lands.
    fork_verdict: Option<crate::seam::Verdict>,
    /// Post-resume fault recording toward the next resume set (R6.2).
    resume_recording: Option<Vec<PageId>>,
    /// An in-flight incremental commit capture (2a-full): the unstable set
    /// was write-protected in one arm step; its pages are read out a
    /// bounded batch per `CaptureStep`, or immediately on a write-protect
    /// fault (copy-on-fault) — so the record is an exact cut at the arm
    /// instant without any step ever reading the whole set.
    drain: Option<capture::Drain>,
    /// Source-side migration in flight.
    migrate: Option<migrate::MigrateOut>,
    /// This vset was handed off (R7.2): serve peer fetches, never guests.
    outbound: Option<HostId>,
    /// Destination-side: the source peer serving our post-copy tail.
    peer_source: Option<HostId>,
    /// Foreign page locations left to re-home from `peer_source`.
    hydration_remaining_pages: usize,
    /// Incremental cursor and cycle state for bounded hydration scans.
    hydrate_cursor: Option<PageId>,
    /// Reply verdict for an inbound migration, delivered when its first
    /// record lands.
    migrated_verdict: Option<crate::seam::Verdict>,
    /// Liveness watch state (R9.2), advanced once per writeback tick.
    wedge: Wedge,
    /// The destination won the backed-vset head and is writing its first
    /// record in the returned fence namespace.
    migration_head_claimed: bool,
}

/// One in-flight backup publish (R4.2): segments verbatim, then the
/// manifest, then the head CAS.
#[derive(Debug)]
struct Publish {
    record: JournalRecord,
    segs_todo: Vec<(u64, SegId)>,
    /// Own-namespace leaves the record references and the store lacks.
    leaves_todo: Vec<(u64, u64)>,
}

/// One sequential passive-replica transfer. At most one message is awaiting
/// an ACK, which bounds memory and makes retry identity explicit.
#[derive(Debug)]
struct ReplicaSend {
    target: HostId,
    assignment_epoch: u64,
    record: JournalRecord,
    required: Vec<crate::seam::ReplicaArtifact>,
    todo: Vec<crate::seam::ReplicaArtifact>,
    awaiting: Option<crate::seam::PeerMsg>,
    retries: u8,
    timer_generation: u64,
}

impl Vset {
    fn new(config: VsetConfig) -> Vset {
        Vset {
            config,
            database: DatabaseMeta::default(),
            database_durable: DatabaseMeta::default(),
            database_runtime: database::DatabaseRuntime::default(),
            database_prune_spans: BTreeMap::new(),
            fence: 1,
            ready: false,
            create_req: None,
            epoch: Epoch(0),
            mutation_seq: 0,
            page_locs: BTreeMap::new(),
            seg_live: BTreeMap::new(),
            compacting: BTreeSet::new(),
            compact_decode: VecDeque::new(),
            compact_stash: Vec::new(),
            best: None,
            leaf_table: BTreeMap::new(),
            overlay: BTreeMap::new(),
            pending_leaves: BTreeMap::new(),
            leaf_waiters: BTreeMap::new(),
            dead_spans: BTreeSet::new(),
            leaf_retrying: false,
            leaf_blobs: BTreeMap::new(),
            backed_leaves: BTreeSet::new(),
            next_leaf: 0,
            pinned: None,
            captures: BTreeMap::new(),
            commit_running: false,
            ckpt_running: false,
            ckpt_pausing: None,
            ckpt_queue: VecDeque::new(),
            ckpt_done: BTreeMap::new(),
            ckpt_done_order: VecDeque::new(),
            pending_syncs: Vec::new(),
            pending_database_syncs: Vec::new(),
            store_fill_retry: Vec::new(),
            local_covered_through: 0,
            sync_ack_through: 0,
            next_gen: 0,
            next_seq: 0,
            next_seg: 0,
            record_ws: BTreeMap::new(),
            seg_blobs: Vec::new(),
            best_record: None,
            head_version: None,
            head_refreshing: false,
            stash_assignment: None,
            retired_stashes: Vec::new(),
            backed: None,
            backed_segs: BTreeSet::new(),
            store_manifests: BTreeSet::new(),
            publish: None,
            peer_artifacts: BTreeSet::new(),
            peer_committed: None,
            peer_committed_record: None,
            store_published_through: 0,
            peer_upload_done: None,
            replica_head_inflight: false,
            replica_assignment_inflight: false,
            replica_assignment_proposal: None,
            replica_send: None,
            replica_release: None,
            replica_release_queue: VecDeque::new(),
            replica_history_inflight: false,
            pending_verdict: None,
            keep: None,
            fork_from: None,
            fork_vmstate: None,
            fork_verdict: None,
            resume_recording: None,
            drain: None,
            migrate: None,
            outbound: None,
            peer_source: None,
            hydration_remaining_pages: 0,
            hydrate_cursor: None,
            migrated_verdict: None,
            wedge: Wedge::default(),
            migration_head_claimed: false,
        }
    }

    fn adopt_local_ack_if_allowed(&mut self) {
        if !self.config.durability.requires_peer_sync() {
            self.sync_ack_through = self.sync_ack_through.max(self.local_covered_through);
        }
    }

    /// The one door for serving-map adoption (newest generation wins).
    /// Every supersession moves the old location's bytes from live to
    /// dead in its segment's accounting — the signal compaction reads.
    fn map_adopt(&mut self, page: PageId, generation: Gen, loc: PageLoc) {
        let old = self.page_locs.get(&page).copied();
        let was_foreign = self.peer_source.is_some()
            && old.is_some_and(|(_, old)| old.base == 0 && old.fence < self.fence);
        let is_foreign = self.peer_source.is_some() && loc.base == 0 && loc.fence < self.fence;
        match old {
            Some((have, _)) if have >= generation => return,
            Some((_, old)) if old.base == 0 => {
                if let Some(live) = self.seg_live.get_mut(&(old.fence, old.seg)) {
                    *live = live.saturating_sub(u64::from(old.len));
                    if *live == 0 {
                        self.seg_live.remove(&(old.fence, old.seg));
                    }
                }
            }
            _ => {}
        }
        if loc.base == 0 {
            *self.seg_live.entry((loc.fence, loc.seg)).or_insert(0) += u64::from(loc.len);
        }
        self.page_locs.insert(page, (generation, loc));
        match (was_foreign, is_foreign) {
            (true, false) => {
                self.hydration_remaining_pages = self.hydration_remaining_pages.saturating_sub(1);
            }
            (false, true) => self.hydration_remaining_pages += 1,
            _ => {}
        }
    }

    fn map_remove(&mut self, page: PageId) {
        let Some((_, old)) = self.page_locs.remove(&page) else {
            return;
        };
        if old.base == 0 {
            if let Some(live) = self.seg_live.get_mut(&(old.fence, old.seg)) {
                *live = live.saturating_sub(u64::from(old.len));
                if *live == 0 {
                    self.seg_live.remove(&(old.fence, old.seg));
                }
            }
            if self.peer_source.is_some() && old.fence < self.fence {
                self.hydration_remaining_pages = self.hydration_remaining_pages.saturating_sub(1);
            }
        }
    }

    /// Rebuild the live accounting after a wholesale map replacement
    /// (restore, migration, fork, recovery, cold-boot trimming).
    fn rebuild_seg_live(&mut self) {
        self.seg_live.clear();
        for (_, loc) in self.page_locs.values() {
            if loc.base == 0 {
                *self.seg_live.entry((loc.fence, loc.seg)).or_insert(0) += u64::from(loc.len);
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct ReplicaKey {
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
}

#[derive(Debug, Default)]
struct PassiveReplica {
    artifacts: BTreeMap<crate::seam::ReplicaArtifact, (u32, Vec<u8>)>,
    uncommitted_artifacts: BTreeSet<crate::seam::ReplicaArtifact>,
    committed: Option<(crate::seam::ReplicaCommitInfo, u32)>,
    pending_commit: Option<ReplicaPendingCommit>,
    upload: Option<ReplicaUpload>,
    upload_queue: VecDeque<ReplicaUpload>,
    uploaded_artifacts: BTreeSet<crate::seam::ReplicaArtifact>,
    upload_done: Option<crate::seam::ReplicaCommitInfo>,
    append_inflight: bool,
    stored_bytes: u64,
    current_generation: u64,
    current_file_bytes: u64,
}

#[derive(Debug)]
struct ReplicaPendingCommit {
    info: crate::seam::ReplicaCommitInfo,
    required: Vec<crate::seam::ReplicaArtifact>,
    record: Vec<u8>,
}

#[derive(Debug)]
struct ReplicaUpload {
    info: crate::seam::ReplicaCommitInfo,
    todo: Vec<crate::seam::ReplicaArtifact>,
    record: Vec<u8>,
    inflight: bool,
}

pub struct Daemon {
    config: DaemonConfig,
    vsets: BTreeMap<VsetId, Vset>,
    cache: Cache,
    next_io: u64,
    next_attachment_generation: u64,
    pending: BTreeMap<IoId, Pending>,
    /// Faults waiting for a cache slot (pressure, R2.5): FIFO, never
    /// dropped, never killed. `(page, write)`.
    waiters: VecDeque<(PageId, bool)>,
    /// Restores waiting out a store outage (R8.3), vset → admin req.
    restore_retries: BTreeMap<VsetId, ReqId>,
    /// Where the writeback rotation resumes (the last vset that took a
    /// budgeted capture slot): a tick captures a bounded, rotating share
    /// of the fleet rather than all of it at once.
    writeback_cursor: u64,
    /// The fence each released (migrated-away) vset last ran at here. A
    /// late duplicate `MigrateOffer` carrying a record at or below this
    /// fence is a dead incarnation — adopting it would resurrect a second
    /// runner (R7.2). In-memory only: a crash forgets, and the reclaimed
    /// disk plus the placement authority guard the residual.
    released_fences: BTreeMap<VsetId, u64>,
    /// The highest fence this host's disk has ever held per vset —
    /// populated by recovery from every scanned name, bumped on adoption.
    /// An inbound migration takes its fence strictly ABOVE this: an
    /// earlier incarnation abandoned as unrestorable leaves its blobs
    /// behind (R4.5: reclaim is explicit), and a re-adoption deriving its
    /// fence from the offer alone would re-enter that namespace and
    /// collide with the wreckage's surviving write-once names.
    fence_floors: BTreeMap<VsetId, u64>,
    replicas: BTreeMap<ReplicaKey, PassiveReplica>,
    replica_latest_epoch: BTreeMap<(HostId, VsetId), u64>,
    /// Local bytes written and not yet deleted (the daemon wrote every byte,
    /// so it does its own accounting; R2.7).
    local_bytes: u64,
    pub counters: Counters,
}

impl Daemon {
    /// Segment-space observability (R9.2): summed over resident vsets,
    /// (live bytes still referenced by serving maps, local segment blob
    /// bytes on disk). The gap between the two is reclaimable dead space;
    /// compaction holds it under roughly one live-set's worth.
    pub fn seg_space(&self) -> (u64, u64) {
        self.vsets.values().fold((0, 0), |(live, disk), s| {
            (
                live + s.seg_live.values().sum::<u64>(),
                disk + s.seg_blobs.iter().map(|&(_, _, b)| b).sum::<u64>(),
            )
        })
    }

    /// A bounded, current-state view for production telemetry (R9.2).
    #[allow(clippy::too_many_lines)]
    pub fn stats(&self) -> DaemonStats {
        let mut blocked: BTreeMap<VsetId, usize> = BTreeMap::new();
        for &(page, _) in &self.waiters {
            *blocked.entry(page.volume.vset).or_default() += 1;
        }
        for pending in self.pending.values() {
            let page = match pending {
                Pending::Fetch { page, .. }
                | Pending::StoreFetch { page, .. }
                | Pending::PeerFetch { page, .. } => Some(*page),
                _ => None,
            };
            if let Some(page) = page {
                *blocked.entry(page.volume.vset).or_default() += 1;
            }
        }

        let vsets = self
            .vsets
            .iter()
            .map(|(&vset, state)| {
                let dirty_pages = self.cache.dirty_pages_of(vset).len();
                let unstable_pages = self.cache.unstable_pages_of(vset).len();
                let outage_parked = state.store_fill_retry.len()
                    + state.leaf_waiters.values().map(Vec::len).sum::<usize>();
                let hydration_remaining_pages = state.hydration_remaining_pages;
                let live_segment_bytes = state.seg_live.values().sum();
                let local_segment_bytes = state.seg_blobs.iter().map(|&(_, _, bytes)| bytes).sum();
                let best = state.best.map_or(0, |(capture, _)| capture);
                let backed = state.backed.map_or(0, |ptr| ptr.capture_seq);
                let backed_up = state.config.durability == DurabilityMode::Backup;
                let backup_lag_bytes = backed_up.then(|| {
                    let Some(record) = state.best_record.as_ref() else {
                        return 0;
                    };
                    let pending_segments: BTreeSet<(u64, SegId)> = record
                        .overlay
                        .values()
                        .filter(|(_, loc)| loc.base == 0)
                        .map(|(_, loc)| (loc.fence, loc.seg))
                        .chain(
                            record
                                .leaves
                                .values()
                                .filter(|ptr| ptr.base == 0)
                                .filter_map(|ptr| state.leaf_blobs.get(ptr))
                                .flat_map(|(_, segments)| segments.iter().copied()),
                        )
                        .filter(|segment| !state.backed_segs.contains(segment))
                        .collect();
                    state
                        .seg_blobs
                        .iter()
                        .filter(|&&(fence, seg, _)| pending_segments.contains(&(fence, seg)))
                        .map(|&(_, _, bytes)| bytes)
                        .sum()
                });
                let hydrating = state.peer_source.is_some() || !state.pending_leaves.is_empty();
                let role = if state.outbound.is_some() {
                    VsetRole::Outbound
                } else if hydrating {
                    VsetRole::Hydrating
                } else if state.ready {
                    VsetRole::Serving
                } else {
                    VsetRole::Initializing
                };
                let mut operations = 0;
                if state.commit_running || state.drain.is_some() {
                    operations |= VsetOperations::CAPTURE;
                }
                if state.ckpt_running {
                    operations |= VsetOperations::CHECKPOINT;
                }
                if state.publish.is_some() {
                    operations |= VsetOperations::BACKUP;
                }
                if hydrating {
                    operations |= VsetOperations::HYDRATION;
                }
                VsetStats {
                    vset,
                    backed_up,
                    role,
                    fence: state.fence,
                    dirty_pages,
                    unstable_pages,
                    parked_faults: blocked.get(&vset).copied().unwrap_or(0) + outage_parked,
                    pending_syncs: state.pending_syncs.len(),
                    pending_leaf_spans: state.pending_leaves.len(),
                    hydration_remaining_pages,
                    backup_lag_captures: backed_up.then_some(best.saturating_sub(backed)),
                    backup_lag_bytes,
                    operations: VsetOperations(operations),
                    live_segment_bytes,
                    local_segment_bytes,
                }
            })
            .collect();
        let (live_segment_bytes, local_segment_bytes) = self.seg_space();
        DaemonStats {
            cache_capacity_pages: self.cache.capacity(),
            resident_pages: self.cache.resident_count(),
            shared_resident_pages: self.cache.base_resident_count(),
            reserved_pages: self.cache.reserved_count(),
            dirty_pages: self.cache.dirty_count(),
            unstable_pages: self.cache.unstable_count(),
            pressure_waiting_faults: self.waiters.len(),
            parked_faults: self.parked_fills(),
            local_blob_bytes: self.local_bytes,
            disk_capacity_bytes: self.config.disk_capacity,
            disk_headroom_bytes: self.config.disk_headroom,
            live_segment_bytes,
            local_segment_bytes,
            vsets,
        }
    }

    pub fn new(config: DaemonConfig) -> (Daemon, Vec<Effect>) {
        let cache = Cache::new(config.cache_pages);
        let daemon = Daemon {
            config,
            vsets: BTreeMap::new(),
            cache,
            next_io: 0,
            next_attachment_generation: 1,
            pending: BTreeMap::new(),
            waiters: VecDeque::new(),
            restore_retries: BTreeMap::new(),
            writeback_cursor: 0,
            released_fences: BTreeMap::new(),
            fence_floors: BTreeMap::new(),
            replicas: BTreeMap::new(),
            replica_latest_epoch: BTreeMap::new(),
            local_bytes: 0,
            counters: Counters::default(),
        };
        let interval = daemon.config.writeback_interval;
        (
            daemon,
            vec![Effect::SetTimer {
                timer: TimerId::Writeback,
                after: interval,
            }],
        )
    }

    fn io(&mut self) -> IoId {
        let io = IoId(self.next_io);
        self.next_io += 1;
        io
    }

    pub fn replica_metrics(&self) -> Vec<ReplicaVsetMetrics> {
        self.vsets
            .iter()
            .filter(|(_, state)| state.config.durability.requires_peer_sync())
            .map(|(&vset, state)| ReplicaVsetMetrics {
                vset,
                active_peer: state
                    .stash_assignment
                    .map(|assignment| assignment.active_peer),
                transition_peer: state
                    .stash_assignment
                    .and_then(|assignment| assignment.transition_peer),
                assignment_epoch: state
                    .stash_assignment
                    .map(|assignment| assignment.assignment_epoch),
                local_covered_through: state.local_covered_through,
                peer_committed_through: state
                    .peer_committed
                    .map_or(0, |info| info.sync_covered_through),
                store_published_through: state.store_published_through,
                sync_ack_through: state.sync_ack_through,
                queued_syncs: state.pending_syncs.len(),
                upload_lag: state
                    .peer_committed
                    .map_or(0, |info| info.sync_covered_through)
                    .saturating_sub(state.store_published_through),
                current_retries: state.replica_send.as_ref().map_or(0, |send| send.retries),
                queued_releases: state.replica_release_queue.len()
                    + usize::from(state.replica_release.is_some()),
            })
            .collect()
    }

    pub fn replica_spool_metrics(&self) -> Vec<ReplicaSpoolMetrics> {
        self.replicas
            .iter()
            .map(|(key, replica)| ReplicaSpoolMetrics {
                source: key.source,
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
                stored_bytes: replica.stored_bytes,
                source_capacity_bytes: replica::MAX_REPLICA_SOURCE_BYTES,
                current_generation: replica.current_generation,
                committed_through: replica
                    .committed
                    .map_or(0, |(info, _)| info.sync_covered_through),
                uploaded_through: replica
                    .upload_done
                    .map_or(0, |info| info.sync_covered_through),
            })
            .collect()
    }

    fn initial_stash_assignment(&self, vset: VsetId) -> Option<crate::head::StashAssignment> {
        let placement = self.config.replica_placement.as_ref()?;
        let active_peer = rank_stash_candidates(
            placement.membership_epoch,
            self.config.host,
            placement.local_failure_domain,
            vset,
            &placement.roster,
        )
        .into_iter()
        .next()?;
        Some(crate::head::StashAssignment {
            assignment_epoch: 1,
            active_peer,
            active_assignment_epoch: 1,
            transition_peer: None,
            membership_epoch: placement.membership_epoch,
        })
    }

    /// Handle one event; returns the effects to apply. Pure state machine:
    /// no clock, no RNG, no I/O — `mem` is the shared guest mapping, read
    /// synchronously exactly as production reads mapped memory.
    pub fn step(&mut self, event: Event, mem: &dyn HostMap) -> Vec<Effect> {
        let mut out = Vec::new();
        match event {
            Event::GuestFault { page, write } => self.fault(page, write, mem, &mut out),
            Event::GuestSync { req, volume } => self.sync(req, volume, mem, &mut out),
            Event::Database(request) => self.database_request(request, mem, &mut out),
            Event::GuestPaused { vset, vmstate } => self.paused(vset, vmstate, mem, &mut out),
            Event::PeerDelivered { from, msg } => self.peer(from, msg, mem, &mut out),
            Event::ReplicaPutPrepared {
                from,
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
                frame,
            } => self.replica_put_prepared(
                from,
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
                frame,
                &mut out,
            ),
            Event::Admin(cmd) => self.admin(cmd, mem, &mut out),
            Event::BlobWriteDone { io } => self.blob_write_done(io, mem, &mut out),
            Event::ReplicaDeleteFailed { io } => self.replica_delete_failed(io, &mut out),
            Event::BlobReadDone { io, bytes } => self.blob_read_routed(io, bytes, mem, &mut out),
            Event::StorePutDone { io, result } => self.store_put_done(io, result, &mut out),
            Event::StoreGetDone { io, result } => self.store_get_done(io, result, mem, &mut out),
            Event::Timer(TimerId::Backup(vset)) => self.backup_tick(vset, mem, &mut out),
            Event::Timer(TimerId::Replica { vset, generation }) => {
                self.replica_retry(vset, generation, &mut out);
            }
            Event::Timer(TimerId::ReplicaRelease(vset)) => {
                self.replica_release_retry(vset, &mut out);
            }
            Event::Timer(TimerId::ReplicaUpload {
                source,
                vset,
                assignment_epoch,
            }) => self.replica_upload_retry(source, vset, assignment_epoch, &mut out),
            Event::Timer(TimerId::ResumeSet(vset)) => self.resume_set_flush(vset, &mut out),
            Event::Timer(TimerId::MigrateOffer(vset)) => self.migrate_offer_tick(vset, &mut out),
            Event::Timer(TimerId::PeerRetry(io)) => self.peer_retry(io, mem, &mut out),
            Event::Timer(TimerId::FillRetry(vset)) => self.fill_retry_tick(vset, &mut out),
            Event::Timer(TimerId::CaptureStep(vset)) => self.capture_step(vset, mem, &mut out),
            Event::Timer(TimerId::CompactStep(vset)) => self.compact_step(vset, &mut out),
            Event::Timer(TimerId::DatabaseStep(vset)) => {
                self.drive_database(vset, mem, &mut out);
            }
            Event::Timer(TimerId::Hydrate(vset)) => self.hydrate_tick(vset, &mut out),
            Event::Timer(TimerId::RestoreRetry(vset)) => self.restore_retry(vset, &mut out),
            Event::Timer(TimerId::LeafRetry(vset)) => self.leaf_retry(vset, &mut out),
            Event::Timer(TimerId::DatabaseRetry(vset)) => self.database_retry(vset, &mut out),
            Event::Timer(TimerId::DatabaseMigrate(vset)) => {
                self.database_migrate_capture(vset, mem, &mut out);
            }
            Event::Timer(TimerId::DatabaseMigrateHead(vset)) => {
                self.start_migrate_head_claim(vset, &mut out);
            }
            Event::Timer(TimerId::Writeback) => {
                // MGLRU-mirrored aging (R2.6) rides the writeback cadence,
                // as reclaim-driven aging rides kswapd in the kernel.
                self.cache.age(|| mem.harvest_accessed());
                self.wedge_tick();
                self.writeback_tick(mem, &mut out);
                out.push(Effect::SetTimer {
                    timer: TimerId::Writeback,
                    after: self.config.writeback_interval,
                });
            }
        }
        out
    }

    fn store_put_done(
        &mut self,
        io: IoId,
        result: Result<u64, crate::seam::StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        match self.pending.remove(&io) {
            Some(
                p @ (Pending::PubSegPut { .. }
                | Pending::PubLeafPut { .. }
                | Pending::PubManifestPut { .. }
                | Pending::PubHeadCas { .. }),
            ) => self.pub_put_done(p, result, out),
            Some(
                p @ (Pending::ReplicaUploadArtifact { .. }
                | Pending::ReplicaUploadManifest { .. }
                | Pending::ReplicaHeadCas { .. }
                | Pending::ReplicaTransitionCas { .. }
                | Pending::ReplicaActivateCas { .. }
                | Pending::ReplicaHistoryCas { .. }),
            ) => self.replica_store_done(p, result, out),
            Some(Pending::HeadCreate { vset }) => self.head_create_done(vset, result, out),
            Some(
                p @ (Pending::KeepSegPut { .. }
                | Pending::KeepLeafPut { .. }
                | Pending::KeepRecordPut { .. }),
            ) => {
                self.keep_put_done(p, result, out);
            }
            Some(Pending::RestoreClaim { req, vset, ptr }) => {
                self.restore_claim_done(req, vset, ptr, result, out);
            }
            Some(Pending::MigrateHeadClaim { vset, manifest }) => {
                self.migrate_head_claim_done(vset, manifest, result, out);
            }
            // Best effort (R6.2): a lost resume set only costs demand faults.
            Some(Pending::ResumeSetPut) => {}
            _ => out.push(Effect::Abort {
                reason: "store put completion for unknown io",
            }),
        }
    }

    #[allow(clippy::type_complexity)]
    fn store_get_done(
        &mut self,
        io: IoId,
        result: Result<Option<(u64, Vec<u8>)>, crate::seam::StoreFault>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        match self.pending.remove(&io) {
            Some(Pending::HeadRefresh { vset }) => self.head_refresh_done(vset, result, out),
            Some(Pending::RestoreHeadGet { req, vset }) => {
                self.restore_head_done(req, vset, result, out);
            }
            Some(Pending::MigrateHeadGet { vset, source }) => {
                self.migrate_head_done(vset, source, result, out);
            }
            Some(Pending::RestoreManifestGet {
                req,
                vset,
                ptr,
                fence,
            }) => self.restore_manifest_done(req, vset, ptr, fence, result, out),
            Some(Pending::StoreFetch {
                page,
                write,
                generation,
                loc,
            }) => self.store_fill_done(page, write, generation, loc, result, out),
            Some(Pending::DatabaseStoreFetch {
                vset,
                page,
                generation,
                loc,
            }) => self.database_store_fetch_done(vset, page, generation, loc, result, mem, out),
            Some(Pending::LeafGet { vset, span, ptr }) => {
                self.leaf_get_done(vset, span, ptr, result, mem, out);
            }
            Some(Pending::ForkBaseGet { vset, base }) => {
                self.fork_base_done(vset, base, result, out);
            }
            Some(Pending::RestoreRsGet { vset }) => self.rs_get_done(vset, result, out),
            Some(Pending::Prefetch {
                page,
                generation,
                loc,
            }) => self.prefetch_done(page, generation, loc, result, out),
            _ => out.push(Effect::Abort {
                reason: "store get completion for unknown io",
            }),
        }
    }

    fn blob_read_routed(
        &mut self,
        io: IoId,
        bytes: Option<Vec<u8>>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        match self.pending.remove(&io) {
            Some(Pending::Fetch {
                page,
                write,
                generation,
                loc,
            }) => self.fill_read_done(page, write, generation, loc, bytes, out),
            Some(Pending::DatabaseFetch {
                vset,
                page,
                generation,
                loc,
            }) => self.database_fetch_done(vset, page, generation, loc, bytes, mem, out),
            Some(Pending::CompactRead { vset, fence, seg }) => {
                self.compact_read_done(vset, fence, seg, bytes, out);
            }
            Some(Pending::PubSegRead { vset, fence, seg }) => {
                self.pub_seg_read_done(vset, fence, seg, bytes, out);
            }
            Some(Pending::ReplicaSourceRead { vset, artifact }) => {
                self.replica_source_read_done(vset, artifact, bytes, out);
            }
            Some(Pending::KeepSegRead { vset, fence, seg }) => {
                self.keep_seg_read_done(vset, fence, seg, bytes, out);
            }
            Some(Pending::KeepLeafRead { vset, fence, id }) => {
                self.keep_leaf_read_done(vset, fence, id, bytes.as_deref(), out);
            }
            Some(Pending::PeerRead { requester, peer_io }) => {
                Self::peer_read_done(requester, peer_io, bytes, out);
            }
            Some(Pending::PubLeafRead { vset, fence, id }) => {
                self.pub_leaf_read_done(vset, fence, id, bytes, out);
            }
            Some(Pending::PeerLeafRead { requester, peer_io }) => {
                out.push(Effect::PeerSend {
                    to: requester,
                    msg: crate::seam::PeerMsg::Leaf { io: peer_io, bytes },
                });
            }
            _ => out.push(Effect::Abort {
                reason: "blob read completion for unknown or non-read io",
            }),
        }
    }

    /// This host lost the vset (R6.4): drop it entirely; guests hang and
    /// the node manager kills them. In-flight io completions for it are
    /// tolerated and ignored.
    pub(super) fn fence_vset(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        if self.vsets.remove(&vset).is_some() {
            self.counters.fenced += 1;
            self.purge_vset_pages(vset, out);
            out.push(Effect::VsetFenced { vset });
        }
    }

    /// A vset left this daemon: purge its cache residency and unmap its
    /// pages — a later incarnation of the vset here must fault fresh.
    pub(super) fn purge_vset_pages(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        for page in self.cache.purge_vset(vset) {
            out.push(Effect::Evict { page });
        }
        self.drain_waiters(out);
    }

    /// Would writing `bytes` more stay inside the device (R2.7)? Reclaim
    /// first if the soft limit is crossed.
    pub(super) fn disk_has_room(&self, bytes: u64) -> bool {
        self.config
            .disk_capacity
            .is_none_or(|cap| self.local_bytes + bytes <= cap)
    }

    pub(super) fn over_soft_limit(&self) -> bool {
        self.config
            .disk_capacity
            .is_some_and(|cap| self.local_bytes + self.config.disk_headroom > cap)
    }

    /// R2.7 reclaim ladder, first class: local segments whose bytes the
    /// backup tier already holds are droppable — refaults refetch from the
    /// store (source order R2.3). The irreducible residue (non-backed-up
    /// state and sole copies) is what remains, and it is observable.
    pub(super) fn nvme_reclaim(&mut self, out: &mut Vec<Effect>) {
        if !self.over_soft_limit() {
            return;
        }
        let vsets: Vec<VsetId> = self.vsets.keys().copied().collect();
        for vset_id in vsets {
            if !self.over_soft_limit() {
                return;
            }
            let state = &self.vsets[&vset_id];
            if !state.config.durability.uses_store() {
                continue;
            }
            // Droppable: fully backed and not pinned by in-flight work.
            let mut pinned: std::collections::BTreeSet<(u64, SegId)> =
                std::collections::BTreeSet::new();
            for capture in state.captures.values() {
                pinned.extend(capture.overlay.values().map(|(_, l)| (l.fence, l.seg)));
                for ptr in capture.leaf_table.values() {
                    if let Some((_, segs)) = state.leaf_blobs.get(ptr) {
                        pinned.extend(segs.iter().copied());
                    }
                }
            }
            if let Some(publish) = &state.publish {
                pinned.extend(
                    publish
                        .record
                        .overlay
                        .values()
                        .map(|(_, l)| (l.fence, l.seg)),
                );
                pinned.extend(publish.segs_todo.iter().copied());
            }
            for p in self.pending.values() {
                if let Pending::Fetch { page, loc, .. } = p
                    && page.volume.vset == vset_id
                {
                    pinned.insert((loc.fence, loc.seg));
                }
            }
            let state = &self.vsets[&vset_id];
            let droppable: Vec<(u64, SegId, u64)> = state
                .seg_blobs
                .iter()
                .copied()
                .filter(|&(fence, seg, _)| {
                    state.backed_segs.contains(&(fence, seg)) && !pinned.contains(&(fence, seg))
                })
                .collect();
            for (fence, seg, size) in droppable {
                if !self.over_soft_limit() {
                    break;
                }
                let state = self.vsets.get_mut(&vset_id).expect("listed");
                state
                    .seg_blobs
                    .retain(|&(f, sg, _)| (f, sg) != (fence, seg));
                self.local_bytes = self.local_bytes.saturating_sub(size);
                self.counters.nvme_reclaims += 1;
                out.push(Effect::BlobDelete {
                    name: layout::segment_blob(vset_id, fence, seg),
                });
            }
        }
    }

    /// Backup lag in capture units (R4.3: measured, never bounded).
    pub fn backup_lag(&self, vset: VsetId) -> Option<u64> {
        let state = self.vsets.get(&vset)?;
        if !state.config.durability.uses_store() {
            return None;
        }
        let best = state.best.map_or(0, |(capture, _)| capture);
        let backed = state.backed.map_or(0, |ptr| ptr.capture_seq);
        Some(best.saturating_sub(backed))
    }

    /// Test/oracle introspection: private resident page count.
    pub fn resident_pages(&self) -> usize {
        self.cache.resident_count()
    }

    /// Test/oracle introspection: shared base pages resident (one physical
    /// copy each, regardless of fork count — R5.3).
    pub fn base_resident_pages(&self) -> usize {
        self.cache.base_resident_count()
    }

    /// Test/oracle introspection: faults waiting on pressure right now.
    pub fn waiting_guests(&self) -> usize {
        self.waiters.len()
    }

    /// Test/oracle introspection: every parked fill — pressure waiters,
    /// outage-parked store fills, faults parked on unhydrated spans. A
    /// healed world must drain this to zero (the liveness oracle).
    pub fn parked_fills(&self) -> usize {
        self.waiters.len()
            + self
                .vsets
                .values()
                .map(|s| {
                    s.store_fill_retry.len() + s.leaf_waiters.values().map(Vec::len).sum::<usize>()
                })
                .sum::<usize>()
    }

    /// Test/oracle introspection: vsets still mid-hydration — a post-copy
    /// tail, pending map leaves, or an unacknowledged release.
    pub fn hydrating_vsets(&self) -> usize {
        self.vsets
            .values()
            .filter(|s| s.peer_source.is_some() || !s.pending_leaves.is_empty())
            .count()
    }

    /// One liveness concern's tick (R9.2): idle or progressing concerns
    /// reset; an active concern whose progress signal has not moved for
    /// `threshold` consecutive ticks fires — and restarts, so a concern
    /// wedged forever re-fires every interval instead of going quiet.
    fn watch(
        active: bool,
        progress: u64,
        seen: &mut u64,
        ticks: &mut u64,
        threshold: u64,
        fired: &mut u64,
    ) {
        if !active || progress != *seen {
            *seen = progress;
            *ticks = 0;
            return;
        }
        *ticks += 1;
        if *ticks >= threshold {
            *fired += 1;
            *ticks = 0;
        }
    }

    /// The liveness watch (R9.2), on the writeback cadence: nothing the
    /// daemon waits on may stall silently. Wedges only count — loud
    /// counters, no recovery action: every watched path already has its
    /// own retry, and what these surface is the retry not working.
    fn wedge_tick(&mut self) {
        let threshold = self.config.wedge_ticks;
        if threshold == 0 {
            return;
        }
        let mut blocked: BTreeMap<VsetId, usize> = BTreeMap::new();
        for &(page, _) in &self.waiters {
            *blocked.entry(page.volume.vset).or_default() += 1;
        }
        // In-flight demand fetches count as blocked too: an outage-parked
        // fill spends part of every retry cycle in flight, and a watch
        // that only saw the parked half would reset in the gaps. Healthy
        // fetches complete and bump the progress signal, so counting them
        // costs nothing.
        for p in self.pending.values() {
            let (Pending::Fetch { page, .. }
            | Pending::StoreFetch { page, .. }
            | Pending::PeerFetch { page, .. }) = p
            else {
                continue;
            };
            *blocked.entry(page.volume.vset).or_default() += 1;
        }
        for (vset_id, state) in &mut self.vsets {
            let w = &mut state.wedge;
            let parked = blocked.get(vset_id).copied().unwrap_or(0)
                + state.store_fill_retry.len()
                + state.leaf_waiters.values().map(Vec::len).sum::<usize>();
            Self::watch(
                parked > 0,
                w.fills,
                &mut w.fills_seen,
                &mut w.parked_ticks,
                threshold,
                &mut self.counters.wedged_guests,
            );
            Self::watch(
                state.peer_source.is_some() || !state.pending_leaves.is_empty(),
                w.hydration,
                &mut w.hydration_seen,
                &mut w.hydration_ticks,
                threshold,
                &mut self.counters.wedged_hydration,
            );
            Self::watch(
                state.outbound.is_some(),
                w.served,
                &mut w.served_seen,
                &mut w.outbound_ticks,
                threshold,
                &mut self.counters.wedged_outbound,
            );
        }
    }
}
