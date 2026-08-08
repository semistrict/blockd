//! Captures and the durable write path: writeback commits, sync commits,
//! checkpoints, journal records, and reclamation.
//!
//! The map's durable shape (R3.3/R3.4): each capture carries a bounded
//! inline OVERLAY plus one leaf pointer per span. Fresh locations join the
//! overlay; when a span's overlay share crosses [`ROLL_THRESHOLD`] — or
//! the whole overlay crosses [`OVERLAY_MAX`] — that span ROLLS: its full
//! content (from the serving map) is written as a fresh leaf blob, and
//! the record waits on it exactly as it waits on its segment. Metadata
//! cost per capture is O(delta), never O(vset).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::{Capture, Daemon, PageMap, Pending, Rescue, Vset};
use crate::journal::{JournalRecord, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::{LEAF_SPAN, LeafPtr, MapLeaf, span_of};
use crate::seam::{AdminCmd, AdminReply, Effect, HostMap, IoId, ReqId, TimerId};
use crate::segment::{PageLoc, SegmentBatchBuilder};
use crate::types::{Epoch, Gen, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId};

/// Remembered checkpoint outcomes per vset (R3.5 idempotence). Old entries
/// age out FIFO so eternal checkpointing accrues no memory debt (R3.4).
const CKPT_DONE_KEPT: usize = 128;

/// A record carries at most this many inline map entries (plus, only
/// transiently, entries of spans still hydrating — those cannot roll).
pub(super) const OVERLAY_MAX: usize = 2048;

/// A span rolls into a fresh leaf once this many of its entries sit in
/// the overlay: one leaf write (≤ ~180 KB) per this many page updates.
const ROLL_THRESHOLD: usize = 256;

/// A rolled map leaf on its way to disk: pointer, encoded bytes, and the
/// own-namespace segments its entries reference.
type LeafWrite = (LeafPtr, Vec<u8>, BTreeSet<(u64, SegId)>);

/// A fresh location a capture's segment holds for one page.
type NewLoc = (PageId, (Gen, PageLoc));

/// One sealed segment and the page locations encoded within it.
type SegmentBlob = (SegId, Vec<u8>, Vec<(PageId, Gen, PageLoc)>);

/// A commit capture whose set exceeds this many pages goes incremental
/// (2a-full): armed whole in one cheap step, then read out this many
/// pages per `CaptureStep`. The value bounds a step's read+compress work
/// to sub-millisecond territory; smaller sets keep the synchronous path
/// (one step, at most this many reads) byte-for-byte unchanged.
const DRAIN_PAGES_PER_STEP: usize = 64;

/// One in-flight incremental commit capture (2a-full). The consistency
/// cut is fixed at the ARM step, where the entire unstable set went (or
/// already was) behind write protection with NOTHING read yet; from that
/// instant no armed page can change without the daemon hearing first, so
/// reads may spread over as many steps as they like — every one returns
/// the arm-instant bytes exactly.
#[derive(Debug)]
pub(super) struct Drain {
    seq: JournalSeq,
    capture_seq: u64,
    database: crate::journal::DatabaseMeta,
    database_prune_spans: BTreeSet<u32>,
    checkpoint: Option<(Epoch, u64, ReqId)>,
    builder: SegmentBatchBuilder,
    /// Armed pages not yet read: drained in key order, or immediately on
    /// a write-protect fault (copy-on-fault, the crux — see `drain_cow`).
    unread: BTreeMap<PageId, Gen>,
    /// Every armed page — `begin_flush`ed at the arm; exactly these get
    /// `end_flush` on segment durability (or in the unwind).
    armed: Vec<PageId>,
    /// Compaction rescues awaiting the builder. Their bytes are already
    /// in hand, but compression is the cost being spread — they take
    /// drain budget like everything else.
    rescues: Vec<(PageId, Gen, Vec<u8>)>,
    compact_victims: BTreeSet<(u64, SegId)>,
    compact_pages: u64,
}

/// A fully-built capture on its way to `seal_capture`: everything the
/// synchronous path produces in one step and the drain produces across
/// many.
struct Built {
    seq: JournalSeq,
    capture_seq: u64,
    database: crate::journal::DatabaseMeta,
    database_prune_spans: BTreeSet<u32>,
    checkpoint: Option<(Epoch, u64, ReqId)>,
    new_locs: Vec<NewLoc>,
    seg_blobs: Vec<SegmentBlob>,
    flushed: Vec<PageId>,
    compact_victims: BTreeSet<(u64, SegId)>,
    compact_pages: u64,
}

/// One verified victim whose live entries are decompressed in bounded slices.
#[derive(Debug)]
pub(super) struct CompactDecode {
    victim: (u64, SegId),
    bytes: Vec<u8>,
    entries: VecDeque<(PageId, Gen, PageLoc)>,
    rescued: Vec<Rescue>,
}

impl Daemon {
    // ── admin ───────────────────────────────────────────────────────────

    pub(super) fn admin(&mut self, cmd: AdminCmd, mem: &dyn HostMap, out: &mut Vec<Effect>) {
        match cmd {
            AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base,
            } => {
                if self.vsets.contains_key(&vset) {
                    out.push(Effect::Admin(AdminReply::AdminFailed { req }));
                    return;
                }
                if config.durability.requires_peer_sync()
                    && self.initial_stash_assignment(vset).is_none()
                {
                    out.push(Effect::Admin(AdminReply::AdminFailed { req }));
                    return;
                }
                let mut state = Vset::new(config);
                state.create_req = Some(req);
                state.fork_from = from_base;
                self.vsets.insert(vset, state);
                if config.durability.uses_store() {
                    // Backed-up vsets claim their head first (R6.3): the
                    // returned CAS version is this incarnation's fence.
                    self.head_create(vset, out);
                } else if from_base.is_some() {
                    // Non-backed forks still read shared base data (R4.4
                    // allows reads; writes stay forbidden).
                    self.fork_fetch_base(vset, out);
                } else {
                    // The creation record: seq 0, empty map, durable before
                    // the vset is usable (R4.4: the store is never touched).
                    self.start_record_only_capture(vset, out);
                }
            }
            AdminCmd::KeepBase { req, vset, base } => self.keep_base(req, vset, base, out),
            AdminCmd::Checkpoint { req, vset } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    out.push(Effect::Admin(AdminReply::AdminFailed { req }));
                    return;
                };
                if !state.ready || state.config.kind != VsetKind::Compute {
                    out.push(Effect::Admin(AdminReply::AdminFailed { req }));
                    return;
                }
                if let Some(&epoch) = state.ckpt_done.get(&req) {
                    // Idempotent replay (R3.5).
                    out.push(Effect::Admin(AdminReply::CheckpointDone {
                        req,
                        vset,
                        epoch,
                    }));
                    return;
                }
                state.ckpt_queue.push_back(req);
                self.maybe_start_checkpoint(vset, out);
            }
            AdminCmd::RestoreVset { req, vset } => self.restore_vset(req, vset, out),
            AdminCmd::MigrateOut { req, vset, to } => self.migrate_out(req, vset, to, out),
            AdminCmd::DeleteBase { req, base } => Self::delete_base(req, base, out),
            AdminCmd::AttachDatabase { req, vset, vm } => {
                self.attach_database(req, vset, vm, out);
            }
            AdminCmd::BeginDetachDatabase {
                req,
                vset,
                attachment,
                mode,
            } => self.begin_detach_database(req, vset, attachment, mode, mem, out),
            AdminCmd::FinishDetachDatabase {
                req,
                vset,
                attachment,
            } => self.finish_detach_database(req, vset, attachment, mem, out),
        }
    }

    // ── captures ────────────────────────────────────────────────────────

    /// One writeback tick's captures, bounded (R2.4 with a step-cost
    /// bound): a small capture reads its whole set in-step (a large one
    /// arms an incremental drain), so a tick must not start one for every
    /// vset of a large fleet at once — every guest's fault would wait
    /// behind the pile.
    /// Sync-pending vsets always capture (one blocked guest sits behind
    /// each pending sync, so their inflow is self-limiting); the rest
    /// take at most `WRITEBACK_VSETS_PER_TICK` slots, rotating from a
    /// cursor so every vset's turn comes around. When the budget does not
    /// bind, the rotation visits keys in order from the top — exactly the
    /// unbounded tick's behavior.
    pub(super) fn writeback_tick(&mut self, mem: &dyn HostMap, out: &mut Vec<Effect>) {
        const WRITEBACK_VSETS_PER_TICK: usize = 8;
        let keys: Vec<VsetId> = self.vsets.keys().copied().collect();
        if keys.is_empty() {
            return;
        }
        // Fleets within the budget keep the plain key order (the rotation
        // would reshuffle capture order without bounding anything).
        let start = if keys.len() <= WRITEBACK_VSETS_PER_TICK {
            0
        } else {
            keys.partition_point(|&v| v.0 <= self.writeback_cursor) % keys.len()
        };
        let mut budget = WRITEBACK_VSETS_PER_TICK;
        for i in 0..keys.len() {
            let vset = keys[(start + i) % keys.len()];
            let must = self
                .vsets
                .get(&vset)
                .is_some_and(|s| !s.pending_syncs.is_empty());
            if budget > 0 || must {
                let was = self.vsets.get(&vset).is_some_and(|s| s.commit_running);
                self.maybe_start_commit(vset, mem, out);
                let now = self.vsets.get(&vset).is_some_and(|s| s.commit_running);
                if !was && now && !must {
                    budget -= 1;
                    self.writeback_cursor = vset.0;
                }
            }
            self.maybe_start_compact(vset, out);
        }
    }

    pub(super) fn maybe_start_commit(
        &mut self,
        vset: VsetId,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if state.commit_running || !state.ready || state.outbound.is_some() {
            return;
        }
        let has_dirty = self.cache.has_dirty_of(vset);
        let has_uncovered_sync = state
            .pending_syncs
            .iter()
            .chain(state.pending_database_syncs.iter())
            .any(|&(_, barrier)| barrier > state.local_covered_through);
        let detach_needs_commit = state
            .database_runtime
            .detach_barrier()
            .is_some_and(|barrier| barrier > state.local_covered_through);
        // Any pending sync needs a record whose watermark covers it; a
        // graceful detach needs its final barrier represented too. A
        // compaction rescue needs a capture to carry it home.
        if has_dirty
            || state.database != state.database_durable
            || has_uncovered_sync
            || detach_needs_commit
            || !state.compact_stash.is_empty()
        {
            self.start_capture(vset, None, mem, out);
        }
    }

    /// Read back local segments past the amplification threshold — at
    /// least half their bytes superseded — deadest first; their live
    /// pages ride the next capture into a fresh segment, releasing the
    /// victims to cleanup. Batched on the writeback cadence (churn
    /// creates up to one mostly-dead segment per tick, so reclaim must
    /// outpace one per tick). The bound this buys: every surviving
    /// segment is majority-live, so disk stays within ~2× live data plus
    /// the uncompacted tail — and each rewritten byte reclaims at least
    /// one dead one.
    pub(super) fn maybe_start_compact(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        const COMPACT_BATCH: usize = 8;
        let Some(state) = self.vsets.get(&vset_id) else {
            return;
        };
        if !state.ready
            || state.outbound.is_some()
            || state.migrate.is_some()
            || !state.pending_leaves.is_empty()
            || state.compacting.len() + state.compact_decode.len() >= COMPACT_BATCH
        {
            return;
        }
        let mut victims: Vec<(u64, u64, SegId)> = state
            .seg_blobs
            .iter()
            .filter_map(|&(fence, seg, size)| {
                let live = state.seg_live.get(&(fence, seg)).copied().unwrap_or(0);
                let decoding = state
                    .compact_decode
                    .iter()
                    .any(|task| task.victim == (fence, seg));
                (live > 0
                    && live * 2 <= size
                    && !state.compacting.contains(&(fence, seg))
                    && !decoding)
                    .then_some((live * 1_000_000 / size, fence, seg))
            })
            .collect();
        victims.sort_unstable();
        victims.truncate(COMPACT_BATCH - state.compacting.len() - state.compact_decode.len());
        for (_, fence, seg) in victims {
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.compacting.insert((fence, seg));
            let io = self.io();
            self.pending.insert(
                io,
                Pending::CompactRead {
                    vset: vset_id,
                    fence,
                    seg,
                },
            );
            out.push(Effect::BlobRead {
                io,
                name: layout::segment_blob(vset_id, fence, seg),
            });
        }
    }

    /// The victim segment's bytes are back: keep exactly the entries the
    /// serving map still points at (identity, generation AND location
    /// verified against the blob's own checksummed frames) for the next
    /// capture to rewrite. Anything superseded meanwhile is dropped;
    /// a vanished or corrupt blob rescues nothing — the fill path owns
    /// loud failure for pages that exist nowhere intact.
    pub(super) fn compact_read_done(
        &mut self,
        vset_id: VsetId,
        fence: u64,
        seg: SegId,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        if !state.compacting.remove(&(fence, seg)) {
            return;
        }
        let Some(bytes) = bytes else {
            return;
        };
        let Ok((owner, blob_fence, blob_seg, entries)) = crate::segment::scan_segment(&bytes)
        else {
            return;
        };
        if (owner, blob_fence, blob_seg) != (vset_id, fence, seg) {
            return;
        }
        state.compact_decode.push_back(CompactDecode {
            victim: (fence, seg),
            bytes,
            entries: entries.into(),
            rescued: Vec::new(),
        });
        out.push(Effect::SetTimer {
            timer: TimerId::CompactStep(vset_id),
            after: 0,
        });
    }

    /// Decompress no more than one capture-sized page batch. A damaged entry
    /// discards the victim's entire partial rescue, matching the old
    /// all-or-nothing behavior without monopolizing one event.
    pub(super) fn compact_step(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let mut budget = DRAIN_PAGES_PER_STEP;
        let mut damaged = false;
        while budget > 0 {
            let Some(task) = state.compact_decode.front_mut() else {
                break;
            };
            let Some((page, generation, loc)) = task.entries.pop_front() else {
                let complete = state.compact_decode.pop_front().expect("front existed");
                if !complete.rescued.is_empty() {
                    state
                        .compact_stash
                        .push((complete.victim, complete.rescued));
                }
                continue;
            };
            budget -= 1;
            let live = state
                .page_locs
                .get(&page)
                .is_some_and(|&(g, l)| g == generation && l == loc);
            if !live {
                continue;
            }
            let start = loc.offset as usize;
            let end = start.saturating_add(loc.len as usize);
            let Some(frame) = task.bytes.get(start..end) else {
                damaged = true;
                break;
            };
            let Ok((_, _, raw)) = crate::segment::open_entry(vset_id, frame) else {
                damaged = true;
                break;
            };
            task.rescued.push((page, generation, raw));
        }
        if damaged {
            state.compact_decode.pop_front();
        }
        if !state.compact_decode.is_empty() {
            out.push(Effect::SetTimer {
                timer: TimerId::CompactStep(vset_id),
                after: 0,
            });
        }
    }

    pub(super) fn maybe_start_checkpoint(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        // A migrating vset takes no more checkpoints: the guest is (or is
        // about to be) paused for the handoff and never resumes here.
        if state.ckpt_running || !state.ready || state.migrate.is_some() || state.outbound.is_some()
        {
            return;
        }
        let Some(req) = state.ckpt_queue.pop_front() else {
            return;
        };
        // Checkpoints capture at a paused instant (R3.1): ask the VMM to
        // pause; the capture happens on `GuestPaused`.
        state.ckpt_running = true;
        state.ckpt_pausing = Some(req);
        out.push(Effect::PauseGuest { vset });
    }

    /// The VMM paused the guest and handed over its vmstate: capture the
    /// whole vset at this instant, then resume immediately — the pause ends
    /// here; persistence is background (R3.1).
    pub(super) fn paused(
        &mut self,
        vset: VsetId,
        vmstate: u64,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            out.push(Effect::Abort {
                reason: "pause reply for unknown vset",
            });
            return;
        };
        if state.ckpt_pausing.is_none() && state.migrate.is_some() {
            // Migration's final capture: the guest does NOT resume here —
            // it resumes on the destination (post-copy cutover, R7.1). An
            // in-flight drain is dropped first: the final capture re-reads
            // its pages at this paused instant.
            self.abandon_drain(vset);
            self.start_capture(vset, Some((ReqId(u64::MAX), vmstate)), mem, out);
            return;
        }
        let Some(req) = state.ckpt_pausing.take() else {
            out.push(Effect::Abort {
                reason: "pause reply without a pending checkpoint",
            });
            return;
        };
        self.start_capture(vset, Some((req, vmstate)), mem, out);
        out.push(Effect::ResumeGuest { vset });
    }

    /// A capture with no pages to flush (vset creation, migrate-in):
    /// straight to the record — the map passes through as it stands,
    /// pending leaf pointers included.
    pub(super) fn start_record_only_capture(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let state = self.vsets.get_mut(&vset_id).expect("known vset");
        let seq = JournalSeq(state.next_seq);
        state.next_seq += 1;
        state.commit_running = true;
        let capture_seq = state.database_runtime.capture_seq(state.mutation_seq);
        let database = state.database;
        let overlay = state.overlay.clone();
        let leaf_table = state.leaf_table.clone();
        state.captures.insert(
            seq,
            Capture {
                capture_seq,
                database,
                checkpoint: None,
                overlay,
                leaf_table,
                rolled_gens: BTreeMap::new(),
                writes_pending: 0,
                record_writes: 0,
                sync_covered_through: 0,
                record: None,
            },
        );
        self.write_record(vset_id, seq, out);
    }

    /// The capture-instant map: the vset's overlay plus this capture's
    /// fresh locations, with over-threshold spans ROLLED into fresh leaf
    /// blobs (returned for writing; the record waits on them). Spans still
    /// hydrating cannot roll — their content is unknown — so the overlay
    /// bound is transiently `OVERLAY_MAX` plus their entries.
    /// `force_rolls` spans roll regardless of their overlay share:
    /// compaction re-homes pages whose old locations live in leaves, and
    /// the leaf must stop referencing the dying segment.
    fn shard_map(
        state: &mut Vset,
        vset_id: VsetId,
        new_locs: &[NewLoc],
        force_rolls: &BTreeSet<u32>,
    ) -> (
        PageMap,
        BTreeMap<u32, LeafPtr>,
        BTreeMap<PageId, Gen>,
        Vec<LeafWrite>,
    ) {
        let mut overlay = state.overlay.clone();
        for &(page, entry) in new_locs {
            if overlay.get(&page).is_none_or(|(g, _)| *g < entry.0) {
                overlay.insert(page, entry);
            }
        }
        let mut new_locs_by_span: BTreeMap<u32, Vec<NewLoc>> = BTreeMap::new();
        for &(page, entry) in new_locs {
            new_locs_by_span
                .entry(span_of(page))
                .or_default()
                .push((page, entry));
        }
        let mut leaf_table = state.leaf_table.clone();
        let mut span_counts: BTreeMap<u32, usize> = BTreeMap::new();
        for page in overlay.keys() {
            *span_counts.entry(span_of(*page)).or_default() += 1;
        }
        let mut to_roll: BTreeSet<u32> = span_counts
            .iter()
            .filter(|&(span, &n)| {
                (n >= ROLL_THRESHOLD || force_rolls.contains(span))
                    && !state.pending_leaves.contains_key(span)
            })
            .map(|(&span, _)| span)
            .collect();
        to_roll.extend(
            force_rolls
                .iter()
                .copied()
                .filter(|span| !state.pending_leaves.contains_key(span)),
        );
        // Overlay-cap pressure: roll the fattest spans until back under.
        let mut remaining = overlay.len()
            - to_roll
                .iter()
                .map(|span| span_counts.get(span).copied().unwrap_or(0))
                .sum::<usize>();
        while remaining > OVERLAY_MAX {
            let fattest = span_counts
                .iter()
                .filter(|(span, _)| {
                    !to_roll.contains(span) && !state.pending_leaves.contains_key(span)
                })
                .max_by_key(|&(span, &n)| (n, *span));
            let Some((&span, &n)) = fattest else {
                break; // everything left is hydrating: transient overshoot
            };
            to_roll.insert(span);
            remaining -= n;
        }

        let mut rolled_gens = BTreeMap::new();
        let mut writes = Vec::new();
        for span in to_roll {
            // A truncate/delete may remove every entry in the span, so clear
            // the old inline half independently of the rebuilt content.
            overlay.retain(|page, _| span_of(*page) != span);
            // Full span content: the serving map ⊕ this capture's locs.
            let lo_key = u64::from(span) * LEAF_SPAN;
            let idx = VolumeIdx(u8::try_from(lo_key >> 32).expect("volume index"));
            let page_of = |n: u64| PageId {
                volume: VolumeId { vset: vset_id, idx },
                page: PageNo(u32::try_from(n & 0xFFFF_FFFF).expect("page number")),
            };
            let lo = page_of(lo_key);
            let hi = page_of(lo_key + LEAF_SPAN - 1);
            let mut content: BTreeMap<PageId, (Gen, PageLoc)> = state
                .page_locs
                .range(lo..=hi)
                .map(|(page, entry)| (*page, *entry))
                .collect();
            if let Some(entries) = new_locs_by_span.get(&span) {
                for &(page, entry) in entries {
                    if content.get(&page).is_none_or(|(g, _)| *g < entry.0) {
                        content.insert(page, entry);
                    }
                }
            }
            let id = state.next_leaf;
            state.next_leaf += 1;
            let ptr = LeafPtr {
                base: 0,
                fence: state.fence,
                id,
            };
            let entries: Vec<_> = content
                .iter()
                .map(|(page, &(generation, loc))| (page.volume.idx, page.page, generation, loc))
                .collect();
            // Own-namespace segments only: this set feeds local cleanup
            // pinning and the backup's upload list (base segments are
            // shared and kept by their base, R5.3).
            let segs: BTreeSet<(u64, SegId)> = content
                .values()
                .filter(|(_, loc)| loc.base == 0)
                .map(|&(_, loc)| (loc.fence, loc.seg))
                .collect();
            for (page, &(generation, _)) in &content {
                rolled_gens.insert(*page, generation);
                overlay.remove(page);
            }
            let bytes = MapLeaf { span, entries }.encode(vset_id, state.fence, id);
            leaf_table.insert(span, ptr);
            writes.push((ptr, bytes, segs));
        }
        (overlay, leaf_table, rolled_gens, writes)
    }

    /// Begin a capture of the vset's exact current state. Every non-empty
    /// mapped-page capture first arms write protection as one effect, then
    /// reads through bounded `CaptureStep`s. For checkpoints the arm lands
    /// before `ResumeGuest`, preserving the paused-instant cut without doing
    /// page copies, compression, or one ioctl per page in the pause event.
    #[allow(clippy::too_many_lines)]
    pub(super) fn start_capture(
        &mut self,
        vset_id: VsetId,
        ckpt: Option<(ReqId, u64)>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let state = self.vsets.get_mut(&vset_id).expect("capture of known vset");
        let seq = JournalSeq(state.next_seq);
        state.next_seq += 1;
        let capture_seq = state.database_runtime.capture_seq(state.mutation_seq);
        let database = state.database;
        let database_prune_spans: BTreeSet<u32> = state
            .database_prune_spans
            .iter()
            .filter_map(|(&span, &op)| (op <= capture_seq).then_some(span))
            .collect();
        let checkpoint = ckpt.map(|(req, vmstate)| (Epoch(state.epoch.0 + 1), vmstate, req));
        if checkpoint.is_none() {
            state.commit_running = true;
        }

        // Persist everything not yet durably current: dirty pages, plus
        // pages whose only current copy is an in-flight flush of another
        // capture (re-written here so no capture depends on a foreign
        // write). Dirty pages get re-write-protected so the next guest
        // write faults again.
        let to_flush = self.cache.unstable_pages_of(vset_id);
        let to_protect = self.cache.dirty_pages_of(vset_id);
        let state = self.vsets.get_mut(&vset_id).expect("just seen");

        // Compaction's rescues ride this capture: entries still current
        // (not superseded, and not re-flushed fresh from the guest above).
        let flush_set: BTreeSet<PageId> = to_flush.iter().copied().collect();
        let mut compact_victims: BTreeSet<(u64, SegId)> = BTreeSet::new();
        let mut compact_pages: Vec<(PageId, Gen, Vec<u8>)> = Vec::new();
        for (victim, entries) in std::mem::take(&mut state.compact_stash) {
            let before = compact_pages.len();
            compact_pages.extend(entries.into_iter().filter(|(page, generation, _)| {
                !flush_set.contains(page)
                    && state
                        .page_locs
                        .get(page)
                        .is_some_and(|(g, _)| g == generation)
            }));
            if compact_pages.len() > before {
                compact_victims.insert(victim);
            }
        }

        // Large captures always drain incrementally, including checkpoints
        // and migration's final cut. The batched arm effect lands before a
        // checkpoint resumes, so the paused-instant bytes remain stable.
        if to_flush.len() + compact_pages.len() > DRAIN_PAGES_PER_STEP {
            self.arm_drain(
                vset_id,
                seq,
                capture_seq,
                database,
                database_prune_spans,
                checkpoint,
                &to_flush,
                to_protect,
                compact_victims,
                compact_pages,
                out,
            );
            return;
        }

        let mut new_locs: Vec<(PageId, (Gen, PageLoc))> = Vec::new();
        let mut seg_blobs = Vec::new();
        let mut flushed = Vec::new();
        if !to_flush.is_empty() || !compact_pages.is_empty() {
            let fence = state.fence;
            let mut builder = SegmentBatchBuilder::new(vset_id, fence, SegId(state.next_seg));
            let mut gens = Vec::new();
            for page in &to_flush {
                let generation = Gen(state.next_gen);
                state.next_gen += 1;
                gens.push((*page, generation));
            }
            // Rewrites get fresh generations too — the serving map re-homes
            // on durability, the victim's live count drains to zero, and
            // cleanup reclaims it.
            let mut compact_gens = Vec::new();
            for _ in &compact_pages {
                compact_gens.push(Gen(state.next_gen));
                state.next_gen += 1;
            }
            // Small captures remain single-step for latency and compatibility,
            // but arm all dirty pages in contiguous runs before the first copy.
            mem.arm_write_protect(&to_protect);
            for (page, generation) in &gens {
                let bytes = mem.read_page(*page);
                builder.add(*page, *generation, &bytes);
                self.cache.begin_flush(*page);
                flushed.push(*page);
            }
            if !to_protect.is_empty() {
                out.push(Effect::WriteProtect { pages: to_protect });
            }
            for ((page, _, bytes), generation) in compact_pages.iter().zip(&compact_gens) {
                builder.add(*page, *generation, bytes);
            }
            seg_blobs = builder.finish();
            state.next_seg += u64::try_from(seg_blobs.len()).expect("segment count fits u64");
            for (_, _, locs) in &seg_blobs {
                for &(page, generation, loc) in locs {
                    new_locs.push((page, (generation, loc)));
                }
            }
        }

        let gens_allocated = new_locs.len() as u64;
        let segs_allocated = u64::try_from(seg_blobs.len()).expect("segment count fits u64");
        let sealed = self.seal_capture(
            vset_id,
            Built {
                seq,
                capture_seq,
                database,
                database_prune_spans,
                checkpoint,
                new_locs,
                seg_blobs,
                flushed,
                compact_victims,
                compact_pages: compact_pages.len() as u64,
            },
            out,
        );
        if !sealed {
            // Deferred within one step: nothing can have interleaved, so
            // the identifiers allocated above rewind exactly.
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.next_seq = state.next_seq.saturating_sub(1);
            state.next_gen = state.next_gen.saturating_sub(gens_allocated);
            state.next_seg = state.next_seg.saturating_sub(segs_allocated);
        }
    }

    /// Seal a fully-built capture: roll the map, check disk room, issue
    /// the blob writes, and register the [`Capture`]. Returns `false` when
    /// the disk is full even after reclaim (R2.7's coupled stall) — the
    /// shared unwind has run (pages back to dirty, running flag cleared,
    /// `next_leaf` rewound); the caller reverses whatever identifiers it
    /// allocated that are still safely reversible. Dirty pages stay dirty;
    /// the writeback timer retries; syncs wait; nothing corrupts, nothing
    /// dies.
    fn seal_capture(&mut self, vset_id: VsetId, built: Built, out: &mut Vec<Effect>) -> bool {
        let Built {
            seq,
            capture_seq,
            database,
            database_prune_spans,
            checkpoint,
            new_locs,
            seg_blobs,
            flushed,
            compact_victims,
            compact_pages,
        } = built;
        let state = self.vsets.get_mut(&vset_id).expect("sealing a known vset");
        // Every leaf still referencing a victim segment — via live
        // entries (all in the rescue) or stale ones — must rotate, or the
        // leaf keeps the dying segment pinned.
        let mut force_rolls: BTreeSet<u32> = if compact_victims.is_empty() {
            BTreeSet::new()
        } else {
            state
                .leaf_table
                .iter()
                .filter(|(_, ptr)| {
                    state.leaf_blobs.get(ptr).is_some_and(|(_, segs)| {
                        segs.iter().any(|seg| compact_victims.contains(seg))
                    })
                })
                .map(|(&span, _)| span)
                .collect()
        };
        force_rolls.extend(database_prune_spans.iter().copied());
        let leaves_before = state.next_leaf;
        let (overlay, leaf_table, rolled_gens, leaf_writes) =
            Self::shard_map(state, vset_id, &new_locs, &force_rolls);

        self.nvme_reclaim(out);
        let bytes_needed = seg_blobs
            .iter()
            .map(|(_, b, _)| b.len() as u64)
            .sum::<u64>()
            + leaf_writes
                .iter()
                .map(|(_, b, _)| b.len() as u64)
                .sum::<u64>();
        if !self.disk_has_room(bytes_needed) {
            self.counters.nvme_stalls += 1;
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.next_leaf = leaves_before;
            if checkpoint.is_none() {
                state.commit_running = false;
            } else {
                state.ckpt_running = false;
            }
            for page in &flushed {
                self.cache.end_flush(*page);
                self.cache.mark_dirty(*page);
            }
            return false;
        }

        self.local_bytes += bytes_needed;
        if !seg_blobs.is_empty() {
            self.counters.pages_flushed += flushed.len() as u64;
            self.counters.segs_compacted += compact_victims.len() as u64;
            self.counters.pages_compacted += compact_pages;
        }
        let writes_pending =
            self.issue_capture_writes(vset_id, seq, seg_blobs, &flushed, leaf_writes, out);
        let state = self.vsets.get_mut(&vset_id).expect("just seen");
        state.captures.insert(
            seq,
            Capture {
                capture_seq,
                database,
                checkpoint,
                overlay,
                leaf_table,
                rolled_gens,
                writes_pending,
                record_writes: 0,
                sync_covered_through: 0,
                record: None,
            },
        );
        if writes_pending == 0 {
            self.write_record(vset_id, seq, out);
        }
        true
    }

    /// Issue a sealed capture's blob writes — the segment and every rolled
    /// leaf — returning how many the record must wait on.
    fn issue_capture_writes(
        &mut self,
        vset_id: VsetId,
        seq: JournalSeq,
        seg_blobs: Vec<SegmentBlob>,
        flushed: &[PageId],
        leaf_writes: Vec<LeafWrite>,
        out: &mut Vec<Effect>,
    ) -> usize {
        let mut writes_pending = 0;
        let fence = self.vsets[&vset_id].fence;
        let flush_set: BTreeSet<_> = flushed.iter().copied().collect();
        for (seg, blob, locs) in seg_blobs {
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.seg_blobs.push((fence, seg, blob.len() as u64));
            let pending_locs: Vec<_> = locs
                .into_iter()
                .map(|(page, generation, loc)| (page, (generation, loc)))
                .collect();
            let pending_flushes: Vec<_> = pending_locs
                .iter()
                .filter_map(|(page, _)| flush_set.contains(page).then_some(*page))
                .collect();
            let io = self.io();
            self.pending.insert(
                io,
                Pending::SegWrite {
                    vset: vset_id,
                    seq,
                    new_locs: pending_locs,
                    flushes: pending_flushes,
                },
            );
            out.push(Effect::BlobWrite {
                io,
                name: layout::segment_blob(vset_id, fence, seg),
                bytes: blob,
            });
            writes_pending += 1;
        }
        for (ptr, bytes, segs) in leaf_writes {
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.leaf_blobs.insert(ptr, (bytes.len() as u64, segs));
            self.counters.leaf_rolls += 1;
            let io = self.io();
            self.pending
                .insert(io, Pending::LeafWrite { vset: vset_id, seq });
            out.push(Effect::BlobWrite {
                io,
                name: layout::leaf_blob(vset_id, ptr.fence, ptr.id),
                bytes,
            });
            writes_pending += 1;
        }
        writes_pending
    }

    // ── the incremental drain (2a-full) ─────────────────────────────────

    /// ARM: fix the cut. Every armed page is behind write protection when
    /// this step's effects land — dirty pages by the batched
    /// `WriteProtect` emitted here, mid-flush pages since the capture that
    /// flushed them — and NOTHING has been read: from this instant a page
    /// in the set cannot change without a write-protect fault arriving
    /// first, so the reads may spread over later steps and still return
    /// arm-instant bytes exactly. Identifiers (seq, generations, the
    /// segment) are allocated here; an abandoned drain burns them rather
    /// than rewinding — a concurrent checkpoint may have allocated past
    /// them mid-drain, and every name is write-once, so gaps are free.
    #[allow(clippy::too_many_arguments)]
    fn arm_drain(
        &mut self,
        vset_id: VsetId,
        seq: JournalSeq,
        capture_seq: u64,
        database: crate::journal::DatabaseMeta,
        database_prune_spans: BTreeSet<u32>,
        checkpoint: Option<(Epoch, u64, ReqId)>,
        to_flush: &[PageId],
        to_protect: Vec<PageId>,
        compact_victims: BTreeSet<(u64, SegId)>,
        compact_pages: Vec<(PageId, Gen, Vec<u8>)>,
        out: &mut Vec<Effect>,
    ) {
        let state = self.vsets.get_mut(&vset_id).expect("arming a known vset");
        let seg = SegId(state.next_seg);
        // Reserve the worst case (one segment per page) before the
        // incremental drain yields, so an interleaved checkpoint cannot
        // allocate a name the batch later needs. Unused ids are harmless.
        state.next_seg +=
            u64::try_from(to_flush.len() + compact_pages.len()).expect("drain page count fits u64");
        let fence = state.fence;
        let mut unread = BTreeMap::new();
        for page in to_flush {
            unread.insert(*page, Gen(state.next_gen));
            state.next_gen += 1;
        }
        let mut rescues = Vec::new();
        for (page, _, bytes) in compact_pages {
            rescues.push((page, Gen(state.next_gen), bytes));
            state.next_gen += 1;
        }
        let compact_count = rescues.len() as u64;
        state.drain = Some(Drain {
            seq,
            capture_seq,
            database,
            database_prune_spans,
            checkpoint,
            builder: SegmentBatchBuilder::new(vset_id, fence, seg),
            unread,
            armed: to_flush.to_vec(),
            rescues,
            compact_victims,
            compact_pages: compact_count,
        });
        for page in to_flush {
            self.cache.begin_flush(*page);
        }
        if !to_protect.is_empty() {
            out.push(Effect::WriteProtect { pages: to_protect });
        }
        out.push(Effect::SetTimer {
            timer: TimerId::CaptureStep(vset_id),
            after: 0,
        });
    }

    /// DRAIN: one continuation step — read and compress a bounded batch
    /// of armed pages into the in-flight segment, then re-arm the step
    /// timer; when nothing is left, seal. A stale timer (the drain
    /// finished, was abandoned, or its vset left this host) finds no
    /// drain and does nothing.
    pub(super) fn capture_step(
        &mut self,
        vset_id: VsetId,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let Some(drain) = &mut state.drain else {
            return;
        };
        let mut budget = DRAIN_PAGES_PER_STEP;
        while budget > 0 {
            let Some((&page, &generation)) = drain.unread.first_key_value() else {
                break;
            };
            drain.unread.remove(&page);
            let bytes = mem.read_page(page);
            drain.builder.add(page, generation, &bytes);
            budget -= 1;
        }
        let take = budget.min(drain.rescues.len());
        for (page, generation, bytes) in drain.rescues.drain(..take) {
            drain.builder.add(page, generation, &bytes);
        }
        if drain.unread.is_empty() && drain.rescues.is_empty() {
            self.finish_drain(vset_id, out);
        } else {
            out.push(Effect::SetTimer {
                timer: TimerId::CaptureStep(vset_id),
                after: 0,
            });
        }
    }

    /// COPY-ON-FAULT: the guest is writing an armed-but-unread page of an
    /// in-flight drain. Capture it now, out of order — write protection
    /// has held its arm-instant content, and after this read the caller
    /// unprotects: the write lands, the page re-dirties, and the NEW
    /// content belongs to the next capture. One page per fault, bounded.
    pub(super) fn drain_cow(&mut self, page: PageId, mem: &dyn HostMap) {
        let Some(state) = self.vsets.get_mut(&page.volume.vset) else {
            return;
        };
        let Some(drain) = &mut state.drain else {
            return;
        };
        let Some(generation) = drain.unread.remove(&page) else {
            return;
        };
        let bytes = mem.read_page(page);
        drain.builder.add(page, generation, &bytes);
        self.counters.cow_captures += 1;
    }

    /// The drain read everything: build the blob and seal exactly as the
    /// synchronous path would have. On a full disk the arm-time
    /// identifiers burn (see `arm_drain`); the shared unwind in
    /// `seal_capture` has already returned the pages to dirty.
    fn finish_drain(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let state = self.vsets.get_mut(&vset_id).expect("draining vset");
        let drain = state.drain.take().expect("drain in flight");
        let seg_blobs = drain.builder.finish();
        let new_locs = seg_blobs
            .iter()
            .flat_map(|(_, _, locs)| locs.iter().copied())
            .map(|(page, generation, loc)| (page, (generation, loc)))
            .collect();
        self.seal_capture(
            vset_id,
            Built {
                seq: drain.seq,
                capture_seq: drain.capture_seq,
                database: drain.database,
                database_prune_spans: drain.database_prune_spans,
                checkpoint: drain.checkpoint,
                new_locs,
                seg_blobs,
                flushed: drain.armed,
                compact_victims: drain.compact_victims,
                compact_pages: drain.compact_pages,
            },
            out,
        );
    }

    /// Drop an in-flight drain: its pages return to dirty and its
    /// identifiers burn. The migration's final capture re-reads
    /// everything at the paused instant — a drain record landing after
    /// the handoff would trail the final record for nothing — and the
    /// dropped rescues just re-run compaction later (they are volatile by
    /// design).
    pub(super) fn abandon_drain(&mut self, vset_id: VsetId) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let Some(drain) = state.drain.take() else {
            return;
        };
        state.commit_running = false;
        for page in drain.armed {
            self.cache.end_flush(page);
            self.cache.mark_dirty(page);
        }
    }

    /// The capture's data is durable: write its journal record. The
    /// watermark is decided here, before the record exists, so a record
    /// that acknowledges syncs always carries it (R3.8).
    pub(super) fn write_record(&mut self, vset_id: VsetId, seq: JournalSeq, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let capture_seq = state.captures[&seq].capture_seq;
        let inherited_checkpoint = state
            .captures
            .iter()
            .filter(|(other_seq, _)| **other_seq != seq)
            .find_map(|(_, other)| {
                (other.capture_seq == capture_seq)
                    .then_some(other.checkpoint)
                    .flatten()
                    .map(|(epoch, vmstate, _)| RecordKind::Checkpoint { epoch, vmstate })
            })
            .or_else(|| {
                state
                    .pinned
                    .as_ref()
                    .filter(|checkpoint| checkpoint.capture_seq == capture_seq)
                    .map(|checkpoint| checkpoint.kind)
            });
        let capture = state.captures.get_mut(&seq).expect("capture exists");
        // The watermark is monotone: everything already durable-acked plus
        // every pending barrier this capture covers (R3.8).
        capture.sync_covered_through = state
            .pending_syncs
            .iter()
            .chain(state.pending_database_syncs.iter())
            .filter(|&&(_, barrier)| barrier <= capture.capture_seq)
            .map(|&(_, barrier)| barrier)
            .chain(
                state
                    .database_runtime
                    .detach_barrier()
                    .filter(|barrier| *barrier <= capture.capture_seq),
            )
            .fold(state.local_covered_through, u64::max);
        let record = JournalRecord {
            config: state.config,
            seq,
            fence: state.fence,
            // A metadata-only recapture (for example compaction) of an
            // active or pinned checkpoint is still that exact recovery
            // point. Keep its epoch/vmstate so physical relocation cannot
            // silently downgrade a resumable backup to a cold boot.
            kind: match capture.checkpoint {
                None => inherited_checkpoint.unwrap_or(RecordKind::Commit),
                Some((epoch, vmstate, _)) => RecordKind::Checkpoint { epoch, vmstate },
            },
            capture_seq: capture.capture_seq,
            sync_covered_through: capture.sync_covered_through,
            database: capture.database,
            overlay: capture.overlay.clone(),
            leaves: capture.leaf_table.clone(),
            migrated_from: state.peer_source,
        };
        state
            .record_ws
            .insert(seq, (state.fence, capture.sync_covered_through));
        capture.record = Some(record.clone());
        capture.record_writes = 2;
        let fence = state.fence;
        let bytes = record.encode(vset_id);
        self.local_bytes += 2 * bytes.len() as u64;
        // Primary and mirror: the newest record is the sole carrier of its
        // newly-acked sync watermark, so one rotten bit must not be able
        // to roll acked syncs back (R3.8) — recovery accepts either copy.
        for name in [
            layout::journal_blob(vset_id, fence, seq),
            layout::journal_mirror_blob(vset_id, fence, seq),
        ] {
            let io = self.io();
            self.pending
                .insert(io, Pending::RecordWrite { vset: vset_id, seq });
            out.push(Effect::BlobWrite {
                io,
                name,
                bytes: bytes.clone(),
            });
        }
    }

    pub(super) fn blob_write_done(&mut self, io: IoId, mem: &dyn HostMap, out: &mut Vec<Effect>) {
        match self.pending.remove(&io) {
            None => out.push(Effect::Abort {
                reason: "completion for unknown io",
            }),
            Some(Pending::SegWrite {
                vset,
                seq,
                new_locs,
                flushes,
            }) => {
                // A fenced vset may have vanished with ios still in flight.
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                let capture = state.captures.get_mut(&seq).expect("capture exists");
                capture.writes_pending -= 1;
                let done = capture.writes_pending == 0;
                // The segment is durable: its pages are current-and-durable,
                // so they become clean/evictable and serve refaults. The
                // serving map AND the overlay adopt the fresh locations
                // (the overlay half holds them until a roll re-homes them
                // into a leaf and the finalize adopts it).
                for &(page, (generation, loc)) in &new_locs {
                    state.map_adopt(page, generation, loc);
                    if state
                        .overlay
                        .get(&page)
                        .is_none_or(|(g, _)| *g < generation)
                    {
                        state.overlay.insert(page, (generation, loc));
                    }
                }
                for page in flushes {
                    self.cache.end_flush(page);
                }
                if done {
                    self.write_record(vset, seq, out);
                }
                self.drain_waiters(out);
                self.drive_database_waiters(mem, out);
            }
            Some(Pending::LeafWrite { vset, seq }) => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                let capture = state.captures.get_mut(&seq).expect("capture exists");
                capture.writes_pending -= 1;
                if capture.writes_pending == 0 {
                    self.write_record(vset, seq, out);
                }
            }
            Some(Pending::LeafCopyWrite) => {}
            Some(Pending::RecordWrite { vset, seq }) => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                let capture = state.captures.get_mut(&seq).expect("capture exists");
                capture.record_writes -= 1;
                if capture.record_writes == 0 {
                    self.finalize_record(vset, seq, mem, out);
                }
            }
            Some(Pending::HandoffWrite { vset }) => self.handoff_written(vset, out),
            Some(
                pending @ (Pending::ReplicaArtifactAppend { .. }
                | Pending::ReplicaCommitAppend { .. }
                | Pending::ReplicaReleaseDelete { .. }
                | Pending::ReplicaTailTruncate { .. }),
            ) => self.replica_append_done(pending, out),
            Some(_) => out.push(Effect::Abort {
                reason: "blob write completion for a non-write io",
            }),
        }
    }

    /// A record is durable: this is the moment its consistency point exists.
    #[allow(clippy::too_many_lines)]
    pub(super) fn finalize_record(
        &mut self,
        vset_id: VsetId,
        seq: JournalSeq,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let capture = state.captures.remove(&seq).expect("capture exists");

        if state
            .best
            .is_none_or(|(c, s)| (capture.capture_seq, seq) > (c, s))
        {
            state.best = Some((capture.capture_seq, seq));
            state.best_record.clone_from(&capture.record);
            state.database_durable = capture.database;
            state
                .database_prune_spans
                .retain(|_, op| *op > capture.capture_seq);
            // Adopt the record's map shape: the leaf table advances, and
            // the overlay shrinks to entries genuinely newer than the
            // rolled leaves' content.
            state.leaf_table = capture.leaf_table.clone();
            state.overlay.retain(|page, (generation, _)| {
                capture
                    .rolled_gens
                    .get(page)
                    .is_none_or(|rolled| generation.0 > rolled.0)
            });
        }
        self.counters.records_written += 1;

        let mut claim_migration_head = false;
        if !state.ready {
            state.ready = true;
            if state.migrated_verdict.is_some()
                && state.config.durability.uses_store()
                && !state.migration_head_claimed
            {
                // The first local record is the backed destination's
                // provisional side. Claim the assignment head next, then
                // write once more in the returned fence before accepting.
                state.ready = false;
                claim_migration_head = true;
            } else if let Some(verdict) = state.migrated_verdict.take() {
                // Inbound migration durable: accept + serve (R7.2 dest side),
                // and start draining the tail off the source (R7.1).
                let source = state.peer_source.expect("offer sender recorded");
                out.push(Effect::PeerSend {
                    to: source,
                    msg: crate::seam::PeerMsg::MigrateAccept { vset: vset_id },
                });
                out.push(Effect::Admin(AdminReply::VsetMigratedIn {
                    vset: vset_id,
                    verdict,
                }));
                out.push(Effect::SetTimer {
                    timer: crate::seam::TimerId::Hydrate(vset_id),
                    after: super::migrate::HYDRATE_TICK,
                });
            } else if let Some(req) = state.create_req.take() {
                if let Some(verdict) = state.fork_verdict.take() {
                    out.push(Effect::Admin(AdminReply::VsetForked {
                        req,
                        vset: vset_id,
                        verdict,
                    }));
                } else {
                    out.push(Effect::Admin(AdminReply::VsetCreated {
                        req,
                        vset: vset_id,
                    }));
                }
            }
        }

        match capture.checkpoint {
            None if state.config.kind == VsetKind::Database && state.migrate.is_some() => {
                state.commit_running = false;
                let record = capture.record.clone().expect("record kept");
                self.migrate_capture_done(vset_id, record, out);
            }
            None => state.commit_running = false,
            Some((_, _, req)) if state.migrate.is_some() && req == ReqId(u64::MAX) => {
                // The migration's final capture is durable: hand off.
                let record = capture.record.clone().expect("record kept");
                self.migrate_capture_done(vset_id, record, out);
            }
            Some((epoch, _, req)) => {
                state.ckpt_running = false;
                state.epoch = epoch;
                state.pinned.clone_from(&capture.record);
                state.ckpt_done.insert(req, epoch);
                state.ckpt_done_order.push_back(req);
                while state.ckpt_done_order.len() > CKPT_DONE_KEPT {
                    let old = state.ckpt_done_order.pop_front().expect("nonempty");
                    state.ckpt_done.remove(&old);
                }
                self.counters.checkpoints_done += 1;
                out.push(Effect::Admin(AdminReply::CheckpointDone {
                    req,
                    vset: vset_id,
                    epoch,
                }));
            }
        }
        if claim_migration_head {
            self.start_migrate_head_claim(vset_id, out);
        }

        let state = self.vsets.get_mut(&vset_id).expect("known vset");
        state.local_covered_through = state
            .local_covered_through
            .max(capture.sync_covered_through);
        state.adopt_local_ack_if_allowed();
        self.drain_sync_acks(vset_id, out);

        self.maybe_replicate(vset_id, out);
        self.cleanup(vset_id, out);
        // A database mutation parked for cache pressure or a tail-page fetch
        // cannot resume until this record makes the preceding consistency
        // point durable. Re-drive it now that `commit_running` is clear.
        self.drive_database(vset_id, mem, out);
        // Chain a commit immediately only for waiting syncs (R3.8 wants
        // their record now); plain dirtiness waits for the writeback tick —
        // its interval IS the capture cadence (R2.4), and chaining on it
        // would re-write the record (and its overlay) at I/O latency
        // instead, for nothing.
        if self.vsets.get(&vset_id).is_some_and(|s| {
            s.pending_syncs
                .iter()
                .chain(s.pending_database_syncs.iter())
                .any(|&(_, barrier)| barrier > s.local_covered_through)
                || s.database_runtime
                    .detach_barrier()
                    .is_some_and(|barrier| barrier > s.local_covered_through)
        }) {
            self.maybe_start_commit(vset_id, mem, out);
        }
        self.maybe_start_checkpoint(vset_id, out);
        // Backup flows continuously as records finalize (R4.2), never gated
        // on checkpoints (R3.2).
        if self
            .vsets
            .get(&vset_id)
            .is_some_and(|s| s.config.durability.uses_store())
        {
            self.maybe_publish(vset_id, out);
        }
    }

    pub(super) fn drain_sync_acks(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let watermark = state.sync_ack_through;
        let (acked, kept): (Vec<_>, Vec<_>) = state
            .pending_syncs
            .drain(..)
            .partition(|&(_, barrier)| barrier <= watermark);
        state.pending_syncs = kept;
        for (req, _) in acked {
            self.counters.syncs_acked += 1;
            out.push(Effect::SyncOk { req });
        }
        let (database_acked, database_kept): (Vec<_>, Vec<_>) = state
            .pending_database_syncs
            .drain(..)
            .partition(|&(_, barrier)| barrier <= watermark);
        state.pending_database_syncs = database_kept;
        for (req, barrier) in database_acked {
            self.counters.syncs_acked += 1;
            out.push(Effect::Database(crate::database::DatabaseReply::Synced {
                req,
                sequence: barrier,
            }));
        }
    }

    /// Reclaim superseded blobs (R4.5: explicit; R3.4: storage never grows
    /// with checkpoint count). Everything referenced by the best record, the
    /// pinned checkpoint, the serving map, any in-flight capture or fetch,
    /// or the watermark anchor stays.
    // One keep-set per artifact class; splitting would scatter the rule.
    #[allow(clippy::too_many_lines)]
    pub(super) fn cleanup(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let mut keep_records: BTreeSet<JournalSeq> = BTreeSet::new();
        if let Some((_, seq)) = state.best {
            keep_records.insert(seq);
        }
        if let Some(pinned) = &state.pinned {
            keep_records.insert(pinned.seq);
        }
        keep_records.extend(state.captures.keys().copied());
        // An in-flight backup publish pins its record and segments: the
        // store copy reads them from local disk (R4.2), and writeback must
        // not race them away.
        if let Some(publish) = &state.publish {
            keep_records.insert(publish.record.seq);
        }
        if let Some(send) = &state.replica_send {
            keep_records.insert(send.record.seq);
        }
        // The watermark anchor (R3.8): if none of the kept records carries
        // the highest synced-through watermark, the record that does must
        // survive — recovery reads the constraint from intact records only.
        let kept_w = keep_records
            .iter()
            .filter_map(|seq| state.record_ws.get(seq).map(|&(_, w)| w))
            .max()
            .unwrap_or(0);
        if let Some((&anchor, &(_, w))) = state
            .record_ws
            .iter()
            .max_by_key(|&(seq, &(_, w))| (w, *seq))
            && w > kept_w
        {
            keep_records.insert(anchor);
        }

        // Leaves stay while any kept record (or the serving table, or an
        // in-flight capture/publish) references them.
        let mut keep_leaves: BTreeSet<LeafPtr> = state.leaf_table.values().copied().collect();
        if let Some(best) = &state.best_record {
            keep_leaves.extend(best.leaves.values());
        }
        if let Some(pinned) = &state.pinned {
            keep_leaves.extend(pinned.leaves.values());
        }
        for capture in state.captures.values() {
            keep_leaves.extend(capture.leaf_table.values());
        }
        if let Some(publish) = &state.publish {
            keep_leaves.extend(publish.record.leaves.values());
        }
        if let Some(send) = &state.replica_send {
            keep_leaves.extend(send.record.leaves.values());
        }

        let mut keep_segs: BTreeSet<(u64, SegId)> = BTreeSet::new();
        // `seg_live` is maintained on every serving-map adoption. Using its
        // keys avoids rescanning every served page each time a record
        // finalizes; the remaining scans below are bounded capture metadata.
        keep_segs.extend(state.seg_live.keys().copied());
        if let Some(best) = &state.best_record {
            keep_segs.extend(best.overlay.values().map(|(_, loc)| (loc.fence, loc.seg)));
        }
        if let Some(pinned) = &state.pinned {
            keep_segs.extend(pinned.overlay.values().map(|(_, loc)| (loc.fence, loc.seg)));
        }
        for capture in state.captures.values() {
            keep_segs.extend(
                capture
                    .overlay
                    .values()
                    .map(|(_, loc)| (loc.fence, loc.seg)),
            );
        }
        if let Some(publish) = &state.publish {
            keep_segs.extend(
                publish
                    .record
                    .overlay
                    .values()
                    .map(|(_, loc)| (loc.fence, loc.seg)),
            );
        }
        if let Some(send) = &state.replica_send {
            keep_segs.extend(
                send.record
                    .overlay
                    .values()
                    .map(|(_, loc)| (loc.fence, loc.seg)),
            );
        }
        // Every kept leaf pins the segments its entries reference.
        for ptr in &keep_leaves {
            if let Some((_, segs)) = state.leaf_blobs.get(ptr) {
                keep_segs.extend(segs.iter().copied());
            }
        }
        // In-flight fetches pin their segment: deleting one under a read
        // would turn a served page into a loud failure for nothing.
        for pending in self.pending.values() {
            if let Pending::Fetch { page, loc, .. } = pending
                && page.volume.vset == vset_id
            {
                keep_segs.insert((loc.fence, loc.seg));
            }
        }
        let state = self.vsets.get_mut(&vset_id).expect("known vset");

        let dead_r: Vec<(JournalSeq, u64)> = state
            .record_ws
            .iter()
            .filter(|(seq, _)| !keep_records.contains(seq))
            .map(|(&seq, &(fence, _))| (seq, fence))
            .collect();
        for (seq, _) in &dead_r {
            state.record_ws.remove(seq);
        }
        let (kept_s, dead_s): (Vec<_>, Vec<_>) = state
            .seg_blobs
            .drain(..)
            .partition(|&(f, sg, _)| keep_segs.contains(&(f, sg)));
        state.seg_blobs = kept_s;
        let dead_l: Vec<(LeafPtr, u64)> = state
            .leaf_blobs
            .iter()
            .filter(|(ptr, _)| !keep_leaves.contains(ptr))
            .map(|(&ptr, &(size, _))| (ptr, size))
            .collect();
        for (ptr, _) in &dead_l {
            state.leaf_blobs.remove(ptr);
        }

        for (seq, fence) in dead_r {
            self.counters.blobs_deleted += 1;
            out.push(Effect::BlobDelete {
                name: layout::journal_blob(vset_id, fence, seq),
            });
            out.push(Effect::BlobDelete {
                name: layout::journal_mirror_blob(vset_id, fence, seq),
            });
        }
        for (fence, seg, size) in dead_s {
            self.counters.blobs_deleted += 1;
            self.local_bytes = self.local_bytes.saturating_sub(size);
            out.push(Effect::BlobDelete {
                name: layout::segment_blob(vset_id, fence, seg),
            });
        }
        for (ptr, size) in dead_l {
            self.counters.blobs_deleted += 1;
            self.local_bytes = self.local_bytes.saturating_sub(size);
            let name = if ptr.base == 0 {
                layout::leaf_blob(vset_id, ptr.fence, ptr.id)
            } else {
                layout::base_leaf_blob(vset_id, ptr.base, ptr.fence, ptr.id)
            };
            out.push(Effect::BlobDelete { name });
        }
    }
}
