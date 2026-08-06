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

use std::collections::{BTreeMap, BTreeSet};

use super::{Capture, Daemon, PageMap, Pending, Vset};
use crate::journal::{JournalRecord, RecordKind};
use crate::layout;
use crate::mapleaf::{LEAF_SPAN, LeafPtr, MapLeaf, span_of};
use crate::seam::{AdminCmd, AdminReply, Effect, HostMap, IoId, ReqId};
use crate::segment::{PageLoc, SegmentBuilder};
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

impl Daemon {
    // ── admin ───────────────────────────────────────────────────────────

    pub(super) fn admin(&mut self, cmd: AdminCmd, out: &mut Vec<Effect>) {
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
                let mut state = Vset::new(config);
                state.create_req = Some(req);
                state.fork_from = from_base;
                self.vsets.insert(vset, state);
                if config.backed_up {
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
                if !state.ready {
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
        }
    }

    // ── captures ────────────────────────────────────────────────────────

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
        // Any pending sync needs a record whose watermark covers it; a
        // compaction rescue needs a capture to carry it home.
        if has_dirty || !state.pending_syncs.is_empty() || !state.compact_stash.is_empty() {
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
            || state.compacting.len() >= COMPACT_BATCH
        {
            return;
        }
        let mut victims: Vec<(u64, u64, SegId)> = state
            .seg_blobs
            .iter()
            .filter_map(|&(fence, seg, size)| {
                let live = state.seg_live.get(&(fence, seg)).copied().unwrap_or(0);
                (live > 0 && live * 2 <= size && !state.compacting.contains(&(fence, seg)))
                    .then_some((live * 1_000_000 / size, fence, seg))
            })
            .collect();
        victims.sort_unstable();
        victims.truncate(COMPACT_BATCH - state.compacting.len());
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
        let mut rescued = Vec::new();
        for (page, generation, loc) in entries {
            let live = state
                .page_locs
                .get(&page)
                .is_some_and(|&(g, l)| g == generation && l == loc);
            if !live {
                continue;
            }
            let start = loc.offset as usize;
            let Ok((_, _, raw)) =
                crate::segment::open_entry(vset_id, &bytes[start..start + loc.len as usize])
            else {
                return; // damaged mid-blob: rescue nothing, fills decide
            };
            rescued.push((page, generation, raw));
        }
        if rescued.is_empty() {
            return;
        }
        state.compact_stash.push(((fence, seg), rescued));
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
            // it resumes on the destination (post-copy cutover, R7.1).
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
        let capture_seq = state.mutation_seq;
        let overlay = state.overlay.clone();
        let leaf_table = state.leaf_table.clone();
        state.captures.insert(
            seq,
            Capture {
                capture_seq,
                checkpoint: None,
                overlay,
                leaf_table,
                rolled_gens: BTreeMap::new(),
                new_locs: Vec::new(),
                flushes: Vec::new(),
                writes_pending: 0,
                record_writes: 0,
                synced_through: 0,
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
    #[allow(clippy::type_complexity)]
    fn shard_map(
        state: &mut Vset,
        vset_id: VsetId,
        new_locs: &[(PageId, (Gen, PageLoc))],
        force_rolls: &BTreeSet<u32>,
    ) -> (
        PageMap,
        BTreeMap<u32, LeafPtr>,
        BTreeMap<PageId, Gen>,
        Vec<(LeafPtr, Vec<u8>, BTreeSet<(u64, SegId)>)>,
    ) {
        let mut overlay = state.overlay.clone();
        for &(page, entry) in new_locs {
            if overlay.get(&page).is_none_or(|(g, _)| *g < entry.0) {
                overlay.insert(page, entry);
            }
        }
        let mut new_locs_by_span: BTreeMap<u32, Vec<(PageId, (Gen, PageLoc))>> = BTreeMap::new();
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
        // Overlay-cap pressure: roll the fattest spans until back under.
        let mut remaining =
            overlay.len() - to_roll.iter().map(|span| span_counts[span]).sum::<usize>();
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

    /// Begin a capture of the vset's exact current state, reading dirty page
    /// contents straight from the shared mapping and re-arming write
    /// protection on them.
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
        let capture_seq = state.mutation_seq;
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

        let mut new_locs: Vec<(PageId, (Gen, PageLoc))> = Vec::new();
        let mut seg_blob: Option<(SegId, Vec<u8>)> = None;
        let mut flushed = Vec::new();
        if !to_flush.is_empty() || !compact_pages.is_empty() {
            let seg = SegId(state.next_seg);
            state.next_seg += 1;
            let fence = state.fence;
            let mut builder = SegmentBuilder::new(vset_id, fence, seg);
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
            let (blob, locs) = builder.finish();
            for (page, generation, loc) in locs {
                new_locs.push((page, (generation, loc)));
            }
            seg_blob = Some((seg, blob));
        }

        let state = self.vsets.get_mut(&vset_id).expect("just seen");
        // Every leaf still referencing a victim segment — via live
        // entries (all in the rescue) or stale ones — must rotate, or the
        // leaf keeps the dying segment pinned.
        let force_rolls: BTreeSet<u32> = if compact_victims.is_empty() {
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
        let leaves_before = state.next_leaf;
        let (overlay, leaf_table, rolled_gens, leaf_writes) =
            Self::shard_map(state, vset_id, &new_locs, &force_rolls);

        // R2.7: no room even after reclaim ⇒ the capture is deferred — the
        // coupled stall. Dirty pages stay dirty; the writeback timer
        // retries; syncs wait; nothing corrupts, nothing dies.
        self.nvme_reclaim(out);
        let bytes_needed = seg_blob.as_ref().map_or(0, |(_, b)| b.len() as u64)
            + leaf_writes
                .iter()
                .map(|(_, b, _)| b.len() as u64)
                .sum::<u64>();
        if !self.disk_has_room(bytes_needed) {
            self.counters.nvme_stalls += 1;
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.next_seq = state.next_seq.saturating_sub(1);
            state.next_gen = state.next_gen.saturating_sub(new_locs.len() as u64);
            state.next_leaf = leaves_before;
            if seg_blob.is_some() {
                state.next_seg = state.next_seg.saturating_sub(1);
            }
            if checkpoint.is_none() {
                state.commit_running = false;
            } else {
                state.ckpt_running = false;
            }
            for page in &flushed {
                self.cache.end_flush(*page);
                self.cache.mark_dirty(*page);
            }
            return;
        }

        self.local_bytes += bytes_needed;
        let mut writes_pending = 0;
        let fence = self.vsets[&vset_id].fence;
        if let Some((seg, blob)) = seg_blob {
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.seg_blobs.push((fence, seg, blob.len() as u64));
            self.counters.pages_flushed += flushed.len() as u64;
            self.counters.segs_compacted += compact_victims.len() as u64;
            self.counters.pages_compacted += compact_pages.len() as u64;
            let io = self.io();
            self.pending
                .insert(io, Pending::SegWrite { vset: vset_id, seq });
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
        let state = self.vsets.get_mut(&vset_id).expect("just seen");
        state.captures.insert(
            seq,
            Capture {
                capture_seq,
                checkpoint,
                overlay,
                leaf_table,
                rolled_gens,
                new_locs,
                flushes: flushed,
                writes_pending,
                record_writes: 0,
                synced_through: 0,
                record: None,
            },
        );
        if writes_pending == 0 {
            self.write_record(vset_id, seq, out);
        }
    }

    /// The capture's data is durable: write its journal record. The
    /// watermark is decided here, before the record exists, so a record
    /// that acknowledges syncs always carries it (R3.8).
    pub(super) fn write_record(&mut self, vset_id: VsetId, seq: JournalSeq, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let capture = state.captures.get_mut(&seq).expect("capture exists");
        // The watermark is monotone: everything already durable-acked plus
        // every pending barrier this capture covers (R3.8).
        capture.synced_through = state
            .pending_syncs
            .iter()
            .filter(|&&(_, barrier)| barrier <= capture.capture_seq)
            .map(|&(_, barrier)| barrier)
            .fold(state.durable_watermark, u64::max);
        let record = JournalRecord {
            config: state.config,
            seq,
            fence: state.fence,
            kind: match capture.checkpoint {
                None => RecordKind::Commit,
                Some((epoch, vmstate, _)) => RecordKind::Checkpoint { epoch, vmstate },
            },
            capture_seq: capture.capture_seq,
            synced_through: capture.synced_through,
            overlay: capture.overlay.clone(),
            leaves: capture.leaf_table.clone(),
            migrated_from: state.peer_source,
        };
        state
            .record_ws
            .insert(seq, (state.fence, capture.synced_through));
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
            Some(Pending::SegWrite { vset, seq }) => {
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
                let new_locs = capture.new_locs.clone();
                let flushes = capture.flushes.clone();
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
            Some(_) => out.push(Effect::Abort {
                reason: "blob write completion for a non-write io",
            }),
        }
    }

    /// A record is durable: this is the moment its consistency point exists.
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

        if !state.ready {
            state.ready = true;
            if let Some(verdict) = state.migrated_verdict.take() {
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

        {
            let state = self.vsets.get_mut(&vset_id).expect("known vset");
            state.durable_watermark = state.durable_watermark.max(capture.synced_through);
            let watermark = state.durable_watermark;
            let (acked, kept): (Vec<_>, Vec<_>) = state
                .pending_syncs
                .drain(..)
                .partition(|&(_, barrier)| barrier <= watermark);
            state.pending_syncs = kept;
            for (req, _) in acked {
                self.counters.syncs_acked += 1;
                out.push(Effect::SyncOk { req });
            }
        }

        self.cleanup(vset_id, out);
        // Chain a commit immediately only for waiting syncs (R3.8 wants
        // their record now); plain dirtiness waits for the writeback tick —
        // its interval IS the capture cadence (R2.4), and chaining on it
        // would re-write the record (and its overlay) at I/O latency
        // instead, for nothing.
        if self
            .vsets
            .get(&vset_id)
            .is_some_and(|s| !s.pending_syncs.is_empty())
        {
            self.maybe_start_commit(vset_id, mem, out);
        }
        self.maybe_start_checkpoint(vset_id, out);
        // Backup flows continuously as records finalize (R4.2), never gated
        // on checkpoints (R3.2).
        if self.vsets.get(&vset_id).is_some_and(|s| s.config.backed_up) {
            self.maybe_publish(vset_id, out);
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

        let mut keep_segs: BTreeSet<(u64, SegId)> = BTreeSet::new();
        keep_segs.extend(
            state
                .page_locs
                .values()
                .map(|(_, loc)| (loc.fence, loc.seg)),
        );
        keep_segs.extend(state.overlay.values().map(|(_, loc)| (loc.fence, loc.seg)));
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
