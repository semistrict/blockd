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
use crate::seam::{AdminReply, Effect, PeerMsg, ReqId, Verdict};
use crate::types::{Epoch, HostId, VsetId};

use super::{Daemon, Pending, Vset};

pub const MAGIC_HANDOFF: u32 = u32::from_le_bytes(*b"BHF1");

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
        // outbound vsets are already gone.
        if !state.ready
            || state.config.backed_up
            || state.outbound.is_some()
            || state.migrate.is_some()
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
    }

    /// A peer message arrived (authenticated cluster member, R11.1).
    pub(super) fn peer(&mut self, from: HostId, msg: PeerMsg, out: &mut Vec<Effect>) {
        match msg {
            PeerMsg::MigrateOffer { vset, record } => self.migrate_in(from, vset, &record, out),
            PeerMsg::MigrateAccept { vset } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                if let Some(migrate) = state.migrate.take() {
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
                // until released (R7.2); absent state answers None.
                let our_io = self.io();
                self.pending.insert(
                    our_io,
                    Pending::PeerRead {
                        requester: from,
                        peer_io: io,
                    },
                );
                out.push(Effect::BlobReadRange {
                    io: our_io,
                    name: layout::segment_blob(vset, fence, seg),
                    offset: u64::from(offset),
                    len: u64::from(len),
                });
            }
            PeerMsg::Page { io, bytes } => self.peer_fill_done(io, bytes, out),
            PeerMsg::Released { vset } => {
                // The destination holds everything: reclaim the vset's
                // local state (R4.5: explicit).
                if let Some(state) = self.vsets.remove(&vset) {
                    for (fence, seg, _) in state.seg_blobs {
                        out.push(Effect::BlobDelete {
                            name: layout::segment_blob(vset, fence, seg),
                        });
                    }
                    for (&seq, &(fence, _)) in &state.record_ws {
                        out.push(Effect::BlobDelete {
                            name: layout::journal_blob(vset, fence, seq),
                        });
                    }
                    out.push(Effect::BlobDelete {
                        name: layout::handoff_blob(vset),
                    });
                }
            }
        }
    }

    /// Destination: the offer's record becomes this host's vset, durably,
    /// before the guest resumes (the destination's handoff side, R7.2).
    fn migrate_in(&mut self, from: HostId, vset: VsetId, record: &[u8], out: &mut Vec<Effect>) {
        if self.vsets.contains_key(&vset) {
            return; // duplicate offer (message duplication is normal)
        }
        let Ok(record) = JournalRecord::decode(vset, record) else {
            return; // damaged in flight: the source will keep serving
        };
        let crate::journal::RecordKind::Checkpoint { epoch, vmstate } = record.kind else {
            return; // migration always offers a whole point
        };
        let mut state = Vset::new(record.config);
        state.fence = record.fence + 1;
        state.epoch = Epoch(epoch.0);
        state.mutation_seq = record.capture_seq;
        state.durable_watermark = record.synced_through;
        state.next_seq = record.seq.0 + 1;
        state.next_gen = record
            .pages
            .values()
            .map(|(g, _)| g.0 + 1)
            .max()
            .unwrap_or(0);
        state.page_locs = record.pages.clone();
        state.best = Some((record.capture_seq, record.seq));
        state.best_pages = record.pages.clone();
        state.best_record = Some(record);
        state.peer_source = Some(from);
        state.migrated_verdict = Some(Verdict::Resume { epoch, vmstate });
        self.vsets.insert(vset, state);
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
        let Some(Pending::PeerFetch {
            page,
            write,
            generation,
            loc,
        }) = self.pending.remove(&io)
        else {
            // Not ours / already resolved: ignore (duplicates are normal).
            return;
        };
        self.peer_fetch_resolved(page, write, generation, loc, bytes, out);
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
