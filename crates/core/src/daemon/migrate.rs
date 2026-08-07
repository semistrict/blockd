//! Live migration (R7): post-copy — cut over first, fault the remainder.
//!
//! The at-most-one-runner exclusion for non-backed-up vsets rests on state
//! locality plus a **two-sided durable handoff** (R6.3/R7.2):
//!
//! 1. The source pauses the guest, captures a final whole record, and
//!    durably writes a local *handoff marker* before offering anything. A
//!    source that crashes past this point recovers as *outbound*: it serves
//!    peer fetches but can never run the guest again.
//! 2. The destination durably writes the received record as its own first
//!    journal record before resuming the guest. A destination that crashes
//!    past this point recovers the vset normally.
//!
//! The destination resumes immediately (pause = capture + one message,
//! R7.1) and demand-faults the tail from the source — the peer tier of
//! R2.3. Source death mid-drain costs the vset (non-backed mode's premise,
//! R7.3) — loudly, at the destination's first unservable fetch.

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::journal::JournalRecord;
use crate::layout;
use crate::seam::{AdminReply, Effect, PeerMsg, ReqId, TimerId, Verdict};
use crate::segment::PageLoc;
use crate::types::{Epoch, Gen, HostId, PageId, VsetId, millis};

use super::{Daemon, Pending, Vset};

pub const MAGIC_HANDOFF: u32 = u32::from_le_bytes(*b"BHF1");

/// The peer channel is at-least-once: offers and releases re-send on this
/// cadence until acknowledged (every handler is idempotent).
pub(super) const OFFER_RETRY: u64 = millis(5);
/// An unanswered peer fetch re-issues after this long — peer RTT is
/// microseconds, so this only fires across losses and source downtime.
pub(super) const PEER_RETRY: u64 = millis(5);
/// Background tail-drain cadence and per-tick fetch budget (R7.1): the
/// destination pulls the source-homed remainder until nothing references
/// the source, then releases it.
pub(super) const HYDRATE_TICK: u64 = millis(10);
const HYDRATE_BATCH: usize = 8;

/// The synthetic request id of a re-offer started by crash recovery — it
/// has no admin caller to answer.
const RECOVERED_REQ: ReqId = ReqId(u64::MAX);

/// The durable outbound marker (R7.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Handoff {
    pub vset: VsetId,
    pub to: HostId,
}

impl Handoff {
    pub(super) fn encode(self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(1);
        e.u64(self.vset.0);
        e.u16(self.to.0);
        seal_frame(MAGIC_HANDOFF, &e.finish())
    }

    pub(super) fn decode(vset: VsetId, bytes: &[u8]) -> Result<Handoff, DecodeError> {
        let payload = open_frame(MAGIC_HANDOFF, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != 1 || d.u64()? != vset.0 {
            return Err(DecodeError);
        }
        let to = HostId(d.u16()?);
        d.finish()?;
        Ok(Handoff { vset, to })
    }
}

/// Source-side migration state.
#[derive(Debug)]
pub(super) struct MigrateOut {
    pub req: ReqId,
    pub to: HostId,
    /// The final captured record, offered once the handoff is durable.
    pub record: Option<JournalRecord>,
}

impl Daemon {
    pub(super) fn migrate_out(
        &mut self,
        req: ReqId,
        vset: VsetId,
        to: HostId,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        // V1 migrates the mode that has no other way to move (R7.2);
        // backed-up vsets relocate via restore. One migration at a time;
        // outbound vsets are already gone; a vset still draining its OWN
        // tail off a source may not chain-migrate — its record would point
        // the new destination at segments the middle host never had.
        if !state.ready
            || state.config.durability.uses_store()
            || state.outbound.is_some()
            || state.migrate.is_some()
            || state.peer_source.is_some()
            || state.ckpt_pausing.is_some()
        {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        }
        state.migrate = Some(MigrateOut {
            req,
            to,
            record: None,
        });
        // Cut over: pause, capture, hand off (R7.1's guest-observed pause
        // is this pause plus one network hop).
        out.push(Effect::PauseGuest { vset });
    }

    /// The final capture's record is durable: persist the handoff marker —
    /// the source's side of the two-sided exclusion (R7.2).
    pub(super) fn migrate_capture_done(
        &mut self,
        vset: VsetId,
        record: JournalRecord,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(migrate) = &mut state.migrate else {
            return;
        };
        migrate.record = Some(record);
        let handoff = Handoff {
            vset,
            to: migrate.to,
        };
        let io = self.io();
        self.pending.insert(io, Pending::HandoffWrite { vset });
        out.push(Effect::BlobWrite {
            io,
            name: layout::handoff_blob(vset),
            bytes: handoff.encode(),
        });
    }

    pub(super) fn handoff_written(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(migrate) = &state.migrate else {
            return;
        };
        let to = migrate.to;
        let record = migrate.record.clone().expect("captured before handoff");
        state.outbound = Some(to);
        out.push(Effect::PeerSend {
            to,
            msg: PeerMsg::MigrateOffer {
                vset,
                record: record.encode(vset),
            },
        });
        out.push(Effect::SetTimer {
            timer: TimerId::MigrateOffer(vset),
            after: OFFER_RETRY,
        });
    }

    /// Re-send the offer until the destination's accept arrives (the take
    /// of `migrate` on accept is what stops this).
    pub(super) fn migrate_offer_tick(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return; // released and reclaimed
        };
        let (Some(migrate), Some(to)) = (&state.migrate, state.outbound) else {
            return; // accepted, or the handoff never became durable
        };
        let record = migrate
            .record
            .clone()
            .or_else(|| state.best_record.clone())
            .expect("an outbound vset has its final record");
        out.push(Effect::PeerSend {
            to,
            msg: PeerMsg::MigrateOffer {
                vset,
                record: record.encode(vset),
            },
        });
        out.push(Effect::SetTimer {
            timer: TimerId::MigrateOffer(vset),
            after: OFFER_RETRY,
        });
    }

    /// Crash recovery of an outbound vset: the crash may have eaten the
    /// offer or its accept, which would strand the vset — the source can
    /// never run it (R7.2) and the destination may not know it exists. The
    /// marker names the destination, so recovery re-offers the final
    /// record; duplicates are re-acked, staleness is impossible (the
    /// record was durable before the marker was written).
    pub(super) fn recovered_outbound(
        state: &mut Vset,
        vset: VsetId,
        to: HostId,
        effects: &mut Vec<Effect>,
    ) {
        state.outbound = Some(to);
        state.migrate = Some(MigrateOut {
            req: RECOVERED_REQ,
            to,
            record: None,
        });
        let record = state
            .best_record
            .clone()
            .expect("an outbound vset has its final record");
        effects.push(Effect::PeerSend {
            to,
            msg: PeerMsg::MigrateOffer {
                vset,
                record: record.encode(vset),
            },
        });
        effects.push(Effect::SetTimer {
            timer: TimerId::MigrateOffer(vset),
            after: OFFER_RETRY,
        });
    }

    /// R11.1 guard: operations on a HELD vset are only valid from its
    /// recorded outbound destination — accepts commit the handoff, fetches
    /// read live pages, `Released` triggers reclaim. Absent state is not
    /// rejected here: each arm keeps its own absent-state semantics (the
    /// loud R7.3 miss, the idempotent re-ack).
    fn rejects_counterparty(&mut self, vset: VsetId, from: HostId) -> bool {
        let wrong = self
            .vsets
            .get(&vset)
            .is_some_and(|state| state.outbound != Some(from));
        if wrong {
            self.counters.peer_rejected += 1;
        }
        wrong
    }

    /// A peer message arrived (authenticated cluster member, R11.1).
    #[allow(clippy::too_many_lines)]
    pub(super) fn peer(
        &mut self,
        from: HostId,
        msg: PeerMsg,
        mem: &dyn crate::seam::HostMap,
        out: &mut Vec<Effect>,
    ) {
        match msg {
            PeerMsg::MigrateOffer { vset, record } => self.migrate_in(from, vset, &record, out),
            PeerMsg::MigrateAccept { vset } => {
                if self.rejects_counterparty(vset, from) {
                    return;
                }
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                if let Some(migrate) = state.migrate.take()
                    && migrate.req != RECOVERED_REQ
                {
                    out.push(Effect::Admin(AdminReply::MigratedOut {
                        req: migrate.req,
                        vset,
                    }));
                }
            }
            PeerMsg::FetchRange {
                io,
                vset,
                fence,
                seg,
                offset,
                len,
            } => {
                // Serve from local storage — outbound vsets keep serving
                // until released (R7.2); absent state answers None (the
                // requester's loud R7.3 miss).
                let Some(our_io) = self.serve_peer(vset, from, false, io) else {
                    return;
                };
                out.push(Effect::BlobReadRange {
                    io: our_io,
                    name: layout::segment_blob(vset, fence, seg),
                    offset: u64::from(offset),
                    len: u64::from(len),
                });
            }
            PeerMsg::Page { io, bytes } => self.peer_fill_done(io, bytes, out),
            PeerMsg::FetchLeaf {
                io,
                vset,
                base,
                fence,
                id,
            } => {
                // Serve leaves like ranges: from local storage, verbatim.
                let Some(our_io) = self.serve_peer(vset, from, true, io) else {
                    return;
                };
                let name = if base == 0 {
                    layout::leaf_blob(vset, fence, id)
                } else {
                    layout::base_leaf_blob(vset, base, fence, id)
                };
                out.push(Effect::BlobRead { io: our_io, name });
            }
            PeerMsg::Leaf { io, bytes } => {
                match self.pending.remove(&io) {
                    Some(Pending::PeerLeafFetch { vset, span, ptr }) => {
                        self.leaf_arrived(vset, span, ptr, bytes, mem, out);
                    }
                    Some(other) => {
                        self.pending.insert(io, other);
                    }
                    // Stale reply to a re-issued fetch: ignore.
                    None => {}
                }
            }
            PeerMsg::Released { vset } => {
                // `Released` is the reclaim trigger: no ack for a rejected
                // sender; the legitimate one keeps retrying.
                if self.rejects_counterparty(vset, from) {
                    return;
                }
                self.released(vset, out);
                // Always ack — a duplicate release after reclamation must
                // still stop the sender.
                out.push(Effect::PeerSend {
                    to: from,
                    msg: PeerMsg::ReleasedAck { vset },
                });
            }
            PeerMsg::ReleasedAck { vset } => {
                // The source is gone as a tier; hydration's next tick sees
                // no source and stops.
                if let Some(state) = self.vsets.get_mut(&vset) {
                    state.peer_source = None;
                }
            }
            msg @ (PeerMsg::ReplicaPut { .. }
            | PeerMsg::ReplicaPutAck { .. }
            | PeerMsg::ReplicaCommit { .. }
            | PeerMsg::ReplicaCommitAck { .. }
            | PeerMsg::ReplicaStatus { .. }
            | PeerMsg::ReplicaStatusReply { .. }
            | PeerMsg::ReplicaUploadDone { .. }
            | PeerMsg::ReplicaRelease { .. }
            | PeerMsg::ReplicaReleaseAck { .. }) => self.replica_peer(from, msg, out),
        }
    }

    /// Admit one peer fetch: authorize the counterparty (R11.1), note the
    /// outbound liveness progress, and register the local read that will
    /// answer it. `None` means rejected.
    fn serve_peer(
        &mut self,
        vset: VsetId,
        from: HostId,
        leaf: bool,
        peer_io: crate::seam::IoId,
    ) -> Option<crate::seam::IoId> {
        if self.rejects_counterparty(vset, from) {
            return None;
        }
        if let Some(state) = self.vsets.get_mut(&vset) {
            state.wedge.served += 1;
        }
        let our_io = self.io();
        let pending = if leaf {
            Pending::PeerLeafRead {
                requester: from,
                peer_io,
            }
        } else {
            Pending::PeerRead {
                requester: from,
                peer_io,
            }
        };
        self.pending.insert(our_io, pending);
        Some(our_io)
    }

    /// The destination holds everything: reclaim the released vset's
    /// local state — segments, both record copies, leaves, the handoff
    /// marker (R4.5: explicit).
    fn released(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.remove(&vset) else {
            return;
        };
        // Remember the incarnation that just died here: a late duplicate
        // offer at or below this fence must never resurrect it.
        self.released_fences.insert(vset, state.fence);
        for (fence, seg, _) in state.seg_blobs {
            out.push(Effect::BlobDelete {
                name: layout::segment_blob(vset, fence, seg),
            });
        }
        for (&seq, &(fence, _)) in &state.record_ws {
            out.push(Effect::BlobDelete {
                name: layout::journal_blob(vset, fence, seq),
            });
            out.push(Effect::BlobDelete {
                name: layout::journal_mirror_blob(vset, fence, seq),
            });
        }
        for ptr in state.leaf_blobs.keys() {
            let name = if ptr.base == 0 {
                layout::leaf_blob(vset, ptr.fence, ptr.id)
            } else {
                layout::base_leaf_blob(vset, ptr.base, ptr.fence, ptr.id)
            };
            out.push(Effect::BlobDelete { name });
        }
        out.push(Effect::BlobDelete {
            name: layout::handoff_blob(vset),
        });
        self.purge_vset_pages(vset, out);
    }

    /// Destination: the offer's record becomes this host's vset, durably,
    /// before the guest resumes (the destination's handoff side, R7.2).
    fn migrate_in(&mut self, from: HostId, vset: VsetId, record: &[u8], out: &mut Vec<Effect>) {
        if let Some(existing) = self.vsets.get(&vset) {
            // Duplicate offer (retries and duplication are normal). Re-ack
            // once this side is durable — before that, silence: the accept
            // MEANS "my first record is durable" (R7.2).
            if existing.peer_source == Some(from) && existing.ready {
                out.push(Effect::PeerSend {
                    to: from,
                    msg: PeerMsg::MigrateAccept { vset },
                });
            }
            return;
        }
        let Ok(record) = JournalRecord::decode(vset, record) else {
            return; // damaged in flight: the source will keep serving
        };
        if let Some(&released) = self.released_fences.get(&vset)
            && record.fence <= released
        {
            // A stale offer of an incarnation this host already ran and
            // released (late channel duplicate): adopting it would raise a
            // second runner from a dead record (R7.2).
            self.counters.peer_rejected += 1;
            return;
        }
        let crate::journal::RecordKind::Checkpoint { epoch, vmstate } = record.kind else {
            return; // migration always offers a whole point
        };
        let mut state = Vset::new(record.config);
        // Strictly above BOTH the offer's fence and this host's fence
        // floor: a dead local incarnation of this vset (crashed dest
        // recovered unrestorable) left its write-once names on disk, and
        // adopting at the offer-derived fence alone would re-enter that
        // namespace and re-write them.
        let floor = self.fence_floors.get(&vset).copied().unwrap_or(0);
        state.fence = record.fence.max(floor) + 1;
        self.fence_floors.insert(vset, state.fence);
        state.epoch = Epoch(epoch.0);
        state.mutation_seq = record.capture_seq;
        state.local_covered_through = record.sync_covered_through;
        state.adopt_local_ack_if_allowed();
        state.next_seq = record.seq.0 + 1;
        state.next_gen = record
            .overlay
            .values()
            .map(|(g, _)| g.0 + 1)
            .max()
            .unwrap_or(0);
        // The map hydrates lazily from the source: overlay serves now,
        // every leaf span parks its faults until fetched (post-copy is
        // already the contract — a parked fault is just a further page).
        state.page_locs = record.overlay.clone();
        state.rebuild_seg_live();
        state.overlay = record.overlay.clone();
        state.leaf_table = record.leaves.clone();
        state.pending_leaves = record.leaves.clone();
        state.best = Some((record.capture_seq, record.seq));
        state.best_record = Some(record);
        state.peer_source = Some(from);
        state.migrated_verdict = Some(Verdict::Resume { epoch, vmstate });
        self.vsets.insert(vset, state);
        self.request_pending_leaves(vset, out);
        // The first local record IS the acceptance; `finalize_record`
        // replies and sends the accept once it is durable.
        self.start_record_only_capture(vset, out);
    }

    /// Destination fill from the source (the peer tier, R2.3).
    pub(super) fn peer_fill_done(
        &mut self,
        io: crate::seam::IoId,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        match self.pending.remove(&io) {
            Some(Pending::PeerFetch {
                page,
                write,
                generation,
                loc,
            }) => self.peer_fetch_resolved(page, write, generation, loc, bytes, out),
            Some(Pending::HydrateFetch { page, generation }) => {
                self.hydrate_fetch_done(page, generation, bytes, out);
            }
            Some(other) => {
                // Io ids are never reused, so a `Page` can only name a peer
                // fetch — but keep foreign entries untouched regardless.
                self.pending.insert(io, other);
            }
            // Stale reply to a retried or resolved fetch: ignore.
            None => {}
        }
    }

    /// A peer fetch went unanswered (lost message or source downtime):
    /// re-run the fill ladder for the page, which re-issues the fetch with
    /// a fresh io — stale replies to the old io fall into the ignore path.
    pub(super) fn peer_retry(&mut self, io: crate::seam::IoId, out: &mut Vec<Effect>) {
        let Some(&Pending::PeerFetch {
            page,
            write,
            generation,
            loc,
        }) = self.pending.get(&io)
        else {
            return; // answered in time
        };
        self.pending.remove(&io);
        self.counters.peer_retries += 1;
        self.fill_read_done(page, write, generation, loc, None, out);
    }

    // ── hydration: the post-copy tail drain (R7.1) ──────────────────────

    /// One hydration tick: pull a bounded batch of source-homed pages, and
    /// once nothing references the source, release it. The tick is also
    /// the retry for its own lost fetches and lost releases.
    pub(super) fn hydrate_tick(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        let Some(source) = state.peer_source else {
            return; // released and acked: hydration is over
        };
        let fence = state.fence;
        // The MAP hydrates before the data: guests park on unhydrated
        // spans, so leaves are the wider blocker — and the tick doubles as
        // the retry for their lost fetches (drop stale in-flight entries;
        // arrival is idempotent).
        if !state.pending_leaves.is_empty() {
            let in_flight: Vec<crate::seam::IoId> = self
                .pending
                .iter()
                .filter(|(_, p)| {
                    matches!(p, Pending::PeerLeafFetch { vset: owner, .. } if *owner == vset)
                })
                .map(|(&io, _)| io)
                .collect();
            for io in in_flight {
                self.pending.remove(&io);
            }
            self.request_pending_leaves(vset, out);
            out.push(Effect::SetTimer {
                timer: TimerId::Hydrate(vset),
                after: HYDRATE_TICK,
            });
            return;
        }
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        // The tail: pages whose durable home is still a source segment
        // (base-origin pages live in the store, not on the source).
        let foreign: Vec<(PageId, Gen, PageLoc)> = state
            .page_locs
            .iter()
            .filter(|&(_, &(_, loc))| loc.base == 0 && loc.fence < fence)
            .map(|(&page, &(generation, loc))| (page, generation, loc))
            .collect();
        if foreign.is_empty() {
            out.push(Effect::PeerSend {
                to: source,
                msg: PeerMsg::Released { vset },
            });
            out.push(Effect::SetTimer {
                timer: TimerId::Hydrate(vset),
                after: HYDRATE_TICK,
            });
            return;
        }
        let mut issued = 0;
        let mut marked = 0u64;
        for (page, generation, loc) in foreign {
            if self.cache.is_resident(page) {
                // The bytes are already in guest memory: dirtying the page
                // is enough — writeback re-homes it into our own segment.
                if !self.cache.is_dirty(page) {
                    self.cache.mark_dirty(page);
                    marked += 1;
                }
                continue;
            }
            if issued >= HYDRATE_BATCH {
                break;
            }
            let in_flight = self.pending.values().any(|p| {
                matches!(
                    p,
                    Pending::Fetch { page: q, .. }
                    | Pending::StoreFetch { page: q, .. }
                    | Pending::PeerFetch { page: q, .. }
                    | Pending::HydrateFetch { page: q, .. } if *q == page
                )
            });
            if in_flight {
                continue;
            }
            let io = self.io();
            self.pending
                .insert(io, Pending::HydrateFetch { page, generation });
            out.push(Effect::PeerSend {
                to: source,
                msg: PeerMsg::FetchRange {
                    io,
                    vset,
                    fence: loc.fence,
                    seg: loc.seg,
                    offset: loc.offset,
                    len: loc.len,
                },
            });
            issued += 1;
        }
        if marked > 0 {
            // Re-homing resident tail pages through writeback IS hydration
            // progress — without this the watch would cry wedge while the
            // captures do the work.
            if let Some(state) = self.vsets.get_mut(&vset) {
                state.wedge.hydration += marked;
            }
        }
        out.push(Effect::SetTimer {
            timer: TimerId::Hydrate(vset),
            after: HYDRATE_TICK,
        });
    }

    /// A hydration fetch arrived: install it like a prefetch — never evict
    /// for it, lose races gracefully (the tick retries) — and mark it
    /// dirty so writeback re-homes it locally.
    fn hydrate_fetch_done(
        &mut self,
        page: PageId,
        generation: Gen,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        let Some(raw) = Self::verify_entry(page, generation, bytes) else {
            return; // damaged or missing: the next tick re-issues
        };
        let in_flight = self.pending.values().any(|p| {
            matches!(
                p,
                Pending::Fetch { page: q, .. }
                | Pending::StoreFetch { page: q, .. }
                | Pending::PeerFetch { page: q, .. } if *q == page
            )
        });
        if self.cache.is_resident(page) || in_flight {
            return;
        }
        if !self.cache.reserve_if_free() {
            return; // pressure: hydration never evicts anyone
        }
        self.cache.fill_slot_cold(page);
        self.cache.mark_dirty(page);
        if let Some(state) = self.vsets.get_mut(&page.volume.vset) {
            state.wedge.hydration += 1;
        }
        self.counters.hydrate_fills += 1;
        out.push(Effect::Fill {
            page,
            bytes: raw,
            writable: false,
            share: None,
        });
    }

    /// A peer read completed locally: answer the requester.
    pub(super) fn peer_read_done(
        requester: HostId,
        peer_io: crate::seam::IoId,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        out.push(Effect::PeerSend {
            to: requester,
            msg: PeerMsg::Page { io: peer_io, bytes },
        });
    }
}
