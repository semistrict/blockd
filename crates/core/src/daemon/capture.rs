//! Captures and the durable write path: writeback commits, sync commits,
//! checkpoints, journal records, and reclamation.

use super::{Capture, Daemon, Pending, Vset};
use crate::journal::{JournalRecord, RecordKind};
use crate::layout;
use crate::seam::{AdminCmd, AdminReply, Effect, HostMap, IoId, ReqId};
use crate::segment::{PageLoc, SegmentBuilder};
use crate::types::{Epoch, Gen, JournalSeq, PageId, SegId, VsetId};

/// Remembered checkpoint outcomes per vset (R3.5 idempotence). Old entries
/// age out FIFO so eternal checkpointing accrues no memory debt (R3.4).
const CKPT_DONE_KEPT: usize = 128;

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
        // Any pending sync needs a record whose watermark covers it.
        if has_dirty || !state.pending_syncs.is_empty() {
            self.start_capture(vset, None, mem, out);
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

    /// A capture with no pages (vset creation): straight to the record.
    pub(super) fn start_record_only_capture(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let state = self.vsets.get_mut(&vset_id).expect("known vset");
        let seq = JournalSeq(state.next_seq);
        state.next_seq += 1;
        state.commit_running = true;
        let capture_seq = state.mutation_seq;
        let pages = state.page_locs.clone();
        state.captures.insert(
            seq,
            Capture {
                capture_seq,
                checkpoint: None,
                pages,
                flushed: Vec::new(),
                seg_done: true,
                synced_through: 0,
                record: None,
            },
        );
        self.write_record(vset_id, seq, out);
    }

    /// Begin a capture of the vset's exact current state, reading dirty page
    /// contents straight from the shared mapping and re-arming write
    /// protection on them.
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
        let mut pages = state.page_locs.clone();
        let mut flushed = Vec::new();
        if to_flush.is_empty() {
            state.captures.insert(
                seq,
                Capture {
                    capture_seq,
                    checkpoint,
                    pages,
                    flushed,
                    seg_done: true,
                    synced_through: 0,
                    record: None,
                },
            );
            self.write_record(vset_id, seq, out);
            return;
        }

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
        for (page, generation) in &gens {
            let bytes = mem.read_page(*page);
            builder.add(*page, *generation, &bytes);
            self.cache.begin_flush(*page);
            flushed.push(*page);
        }
        if !to_protect.is_empty() {
            out.push(Effect::WriteProtect { pages: to_protect });
        }
        let (blob, locs) = builder.finish();
        for (page, generation, loc) in locs {
            pages.insert(page, (generation, loc));
        }
        // R2.7: no room even after reclaim ⇒ the capture is deferred — the
        // coupled stall. Dirty pages stay dirty; the writeback timer
        // retries; syncs wait; nothing corrupts, nothing dies.
        self.nvme_reclaim(out);
        if !self.disk_has_room(blob.len() as u64) {
            self.counters.nvme_stalls += 1;
            let state = self.vsets.get_mut(&vset_id).expect("just seen");
            state.next_seq = state.next_seq.saturating_sub(1);
            state.next_seg = state.next_seg.saturating_sub(1);
            state.next_gen = state.next_gen.saturating_sub(gens.len() as u64);
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
        self.local_bytes += blob.len() as u64;
        let state = self.vsets.get_mut(&vset_id).expect("just seen");
        state.seg_blobs.push((fence, seg, blob.len() as u64));
        state.captures.insert(
            seq,
            Capture {
                capture_seq,
                checkpoint,
                pages,
                flushed,
                seg_done: false,
                synced_through: 0,
                record: None,
            },
        );
        self.counters.pages_flushed += gens.len() as u64;
        let io = self.io();
        self.pending
            .insert(io, Pending::SegWrite { vset: vset_id, seq });
        out.push(Effect::BlobWrite {
            io,
            name: layout::segment_blob(vset_id, fence, seg),
            bytes: blob,
        });
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
            pages: capture.pages.clone(),
        };
        state
            .record_ws
            .insert(seq, (state.fence, capture.synced_through));
        capture.record = Some(record.clone());
        let fence = state.fence;
        let bytes = record.encode(vset_id);
        self.local_bytes += bytes.len() as u64;
        let io = self.io();
        self.pending
            .insert(io, Pending::RecordWrite { vset: vset_id, seq });
        out.push(Effect::BlobWrite {
            io,
            name: layout::journal_blob(vset_id, fence, seq),
            bytes,
        });
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
                capture.seg_done = true;
                // The segment is durable: its pages are current-and-durable,
                // so they become clean/evictable and serve refaults.
                let flushed = capture.flushed.clone();
                let new_locs: Vec<(PageId, (Gen, PageLoc))> =
                    flushed.iter().map(|p| (*p, capture.pages[p])).collect();
                for (page, (generation, loc)) in new_locs {
                    let current = state.page_locs.get(&page);
                    if current.is_none_or(|(g, _)| *g < generation) {
                        state.page_locs.insert(page, (generation, loc));
                    }
                }
                for page in flushed {
                    self.cache.end_flush(page);
                }
                self.write_record(vset, seq, out);
                self.drain_waiters(out);
            }
            Some(Pending::RecordWrite { vset, seq }) => self.finalize_record(vset, seq, mem, out),
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
            state.best_pages = capture.pages.clone();
            state.best_record.clone_from(&capture.record);
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
        self.maybe_start_commit(vset_id, mem, out);
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
    pub(super) fn cleanup(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let mut keep_records: Vec<JournalSeq> = Vec::new();
        if let Some((_, seq)) = state.best {
            keep_records.push(seq);
        }
        if let Some(pinned) = &state.pinned {
            keep_records.push(pinned.seq);
        }
        let in_flight: Vec<JournalSeq> = state.captures.keys().copied().collect();
        keep_records.extend(&in_flight);
        // An in-flight backup publish pins its record and segments: the
        // store copy reads them from local disk (R4.2), and writeback must
        // not race them away.
        if let Some(publish) = &state.publish {
            keep_records.push(publish.record.seq);
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
            keep_records.push(anchor);
        }

        let mut keep_segs: Vec<(u64, SegId)> = Vec::new();
        let maps = [&state.best_pages, &state.page_locs];
        for map in maps {
            keep_segs.extend(map.values().map(|(_, loc)| (loc.fence, loc.seg)));
        }
        if let Some(pinned) = &state.pinned {
            keep_segs.extend(pinned.pages.values().map(|(_, loc)| (loc.fence, loc.seg)));
        }
        for capture in state.captures.values() {
            keep_segs.extend(capture.pages.values().map(|(_, loc)| (loc.fence, loc.seg)));
        }
        if let Some(publish) = &state.publish {
            keep_segs.extend(
                publish
                    .record
                    .pages
                    .values()
                    .map(|(_, loc)| (loc.fence, loc.seg)),
            );
        }
        // In-flight fetches pin their segment: deleting one under a read
        // would turn a served page into a loud failure for nothing.
        for pending in self.pending.values() {
            if let Pending::Fetch { page, loc, .. } = pending
                && page.volume.vset == vset_id
            {
                keep_segs.push((loc.fence, loc.seg));
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

        for (seq, fence) in dead_r {
            self.counters.blobs_deleted += 1;
            out.push(Effect::BlobDelete {
                name: layout::journal_blob(vset_id, fence, seq),
            });
        }
        for (fence, seg, size) in dead_s {
            self.counters.blobs_deleted += 1;
            self.local_bytes = self.local_bytes.saturating_sub(size);
            out.push(Effect::BlobDelete {
                name: layout::segment_blob(vset_id, fence, seg),
            });
        }
    }
}
