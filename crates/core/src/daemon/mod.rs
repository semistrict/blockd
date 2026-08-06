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
mod guest;
mod lineage;
mod migrate;
mod recover;
mod restore;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::cache::Cache;
use crate::head::ManifestPtr;
use crate::journal::{JournalRecord, VsetConfig};
use crate::layout;
use crate::mapleaf::LeafPtr;
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
    /// The fresh locations this capture's segment holds — guest flushes
    /// and compaction rewrites alike. They merge into the serving map on
    /// segment durability; empty for record-only captures.
    new_locs: Vec<(PageId, (Gen, PageLoc))>,
    /// The subset of `new_locs` pages read out of the cache (dirty or
    /// mid-flush): exactly these get `end_flush` on segment durability.
    /// Compaction rewrites are not cache-resident and are not here.
    flushes: Vec<PageId>,
    /// Blob writes (segment + rolled leaves) the record still waits on.
    writes_pending: usize,
    /// Record copies (primary + mirror) still in flight; the consistency
    /// point exists — and syncs ack — only when both are durable.
    record_writes: u8,
    /// The watermark this capture's record carries; fixed when the record
    /// write is issued.
    synced_through: u64,
    /// The exact record written for this capture (kept for backup: the
    /// manifest is these bytes, verbatim).
    record: Option<JournalRecord>,
}

#[derive(Debug)]
enum Pending {
    SegWrite {
        vset: VsetId,
        seq: JournalSeq,
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

// The flags are independent protocol states, not an encoding smell.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct Vset {
    config: VsetConfig,
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
    /// Demand fills parked by a store outage (R8.3), re-issued by the
    /// `FillRetry` timer: each entry's guest is blocked on it.
    store_fill_retry: Vec<(PageId, bool, Gen, crate::segment::PageLoc)>,
    /// Highest sync barrier covered by a durable record's watermark (R3.8).
    durable_watermark: u64,
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
    /// Newest manifest durable in the store (R4.2); the loss bound on host
    /// death is `best` minus this (R4.3).
    backed: Option<ManifestPtr>,
    backed_segs: BTreeSet<(u64, SegId)>,
    store_manifests: BTreeSet<(u64, JournalSeq)>,
    publish: Option<Publish>,
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
    /// Source-side migration in flight.
    migrate: Option<migrate::MigrateOut>,
    /// This vset was handed off (R7.2): serve peer fetches, never guests.
    outbound: Option<HostId>,
    /// Destination-side: the source peer serving our post-copy tail.
    peer_source: Option<HostId>,
    /// Reply verdict for an inbound migration, delivered when its first
    /// record lands.
    migrated_verdict: Option<crate::seam::Verdict>,
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

impl Vset {
    fn new(config: VsetConfig) -> Vset {
        Vset {
            config,
            fence: 1,
            ready: false,
            create_req: None,
            epoch: Epoch(0),
            mutation_seq: 0,
            page_locs: BTreeMap::new(),
            seg_live: BTreeMap::new(),
            compacting: BTreeSet::new(),
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
            store_fill_retry: Vec::new(),
            durable_watermark: 0,
            next_gen: 0,
            next_seq: 0,
            next_seg: 0,
            record_ws: BTreeMap::new(),
            seg_blobs: Vec::new(),
            best_record: None,
            head_version: None,
            head_refreshing: false,
            backed: None,
            backed_segs: BTreeSet::new(),
            store_manifests: BTreeSet::new(),
            publish: None,
            pending_verdict: None,
            keep: None,
            fork_from: None,
            fork_vmstate: None,
            fork_verdict: None,
            resume_recording: None,
            migrate: None,
            outbound: None,
            peer_source: None,
            migrated_verdict: None,
        }
    }

    /// The one door for serving-map adoption (newest generation wins).
    /// Every supersession moves the old location's bytes from live to
    /// dead in its segment's accounting — the signal compaction reads.
    fn map_adopt(&mut self, page: PageId, generation: Gen, loc: PageLoc) {
        match self.page_locs.get(&page).copied() {
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

pub struct Daemon {
    config: DaemonConfig,
    vsets: BTreeMap<VsetId, Vset>,
    cache: Cache,
    next_io: u64,
    pending: BTreeMap<IoId, Pending>,
    /// Faults waiting for a cache slot (pressure, R2.5): FIFO, never
    /// dropped, never killed. `(page, write)`.
    waiters: VecDeque<(PageId, bool)>,
    /// Restores waiting out a store outage (R8.3), vset → admin req.
    restore_retries: BTreeMap<VsetId, ReqId>,
    /// The fence each released (migrated-away) vset last ran at here. A
    /// late duplicate `MigrateOffer` carrying a record at or below this
    /// fence is a dead incarnation — adopting it would resurrect a second
    /// runner (R7.2). In-memory only: a crash forgets, and the reclaimed
    /// disk plus the placement authority guard the residual.
    released_fences: BTreeMap<VsetId, u64>,
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

    pub fn new(config: DaemonConfig) -> (Daemon, Vec<Effect>) {
        let cache = Cache::new(config.cache_pages);
        let daemon = Daemon {
            config,
            vsets: BTreeMap::new(),
            cache,
            next_io: 0,
            pending: BTreeMap::new(),
            waiters: VecDeque::new(),
            restore_retries: BTreeMap::new(),
            released_fences: BTreeMap::new(),
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

    /// Handle one event; returns the effects to apply. Pure state machine:
    /// no clock, no RNG, no I/O — `mem` is the shared guest mapping, read
    /// synchronously exactly as production reads mapped memory.
    pub fn step(&mut self, event: Event, mem: &dyn HostMap) -> Vec<Effect> {
        let mut out = Vec::new();
        match event {
            Event::GuestFault { page, write } => self.fault(page, write, &mut out),
            Event::GuestSync { req, volume } => self.sync(req, volume, mem, &mut out),
            Event::GuestPaused { vset, vmstate } => self.paused(vset, vmstate, mem, &mut out),
            Event::PeerDelivered { from, msg } => self.peer(from, msg, &mut out),
            Event::Admin(cmd) => self.admin(cmd, &mut out),
            Event::BlobWriteDone { io } => self.blob_write_done(io, mem, &mut out),
            Event::BlobReadDone { io, bytes } => self.blob_read_routed(io, bytes, &mut out),
            Event::StorePutDone { io, result } => self.store_put_done(io, result, &mut out),
            Event::StoreGetDone { io, result } => self.store_get_done(io, result, &mut out),
            Event::Timer(TimerId::Backup(vset)) => self.backup_tick(vset, mem, &mut out),
            Event::Timer(TimerId::ResumeSet(vset)) => self.resume_set_flush(vset, &mut out),
            Event::Timer(TimerId::MigrateOffer(vset)) => self.migrate_offer_tick(vset, &mut out),
            Event::Timer(TimerId::PeerRetry(io)) => self.peer_retry(io, &mut out),
            Event::Timer(TimerId::FillRetry(vset)) => self.fill_retry_tick(vset, &mut out),
            Event::Timer(TimerId::Hydrate(vset)) => self.hydrate_tick(vset, &mut out),
            Event::Timer(TimerId::RestoreRetry(vset)) => self.restore_retry(vset, &mut out),
            Event::Timer(TimerId::LeafRetry(vset)) => self.leaf_retry(vset, &mut out),
            Event::Timer(TimerId::Writeback) => {
                // MGLRU-mirrored aging (R2.6) rides the writeback cadence,
                // as reclaim-driven aging rides kswapd in the kernel.
                self.cache.age(|| mem.harvest_accessed());
                let vsets: Vec<VsetId> = self.vsets.keys().copied().collect();
                for vset in vsets {
                    self.maybe_start_commit(vset, mem, &mut out);
                    self.maybe_start_compact(vset, &mut out);
                }
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
        out: &mut Vec<Effect>,
    ) {
        match self.pending.remove(&io) {
            Some(Pending::HeadRefresh { vset }) => self.head_refresh_done(vset, result, out),
            Some(Pending::RestoreHeadGet { req, vset }) => {
                self.restore_head_done(req, vset, result, out);
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
            Some(Pending::LeafGet { vset, span, ptr }) => {
                self.leaf_get_done(vset, span, ptr, result, out);
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

    fn blob_read_routed(&mut self, io: IoId, bytes: Option<Vec<u8>>, out: &mut Vec<Effect>) {
        match self.pending.remove(&io) {
            Some(Pending::Fetch {
                page,
                write,
                generation,
                loc,
            }) => self.fill_read_done(page, write, generation, loc, bytes, out),
            Some(Pending::CompactRead { vset, fence, seg }) => {
                self.compact_read_done(vset, fence, seg, bytes);
            }
            Some(Pending::PubSegRead { vset, fence, seg }) => {
                self.pub_seg_read_done(vset, fence, seg, bytes, out);
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
            if !state.config.backed_up {
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
        if !state.config.backed_up {
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
}
