//! Restore (R6.1): bring a backed-up vset onto this host from the object
//! store alone — no prior local state, no reachable previous host. The head
//! CAS is the entire assignment protocol (R6.3): read the head, claim it
//! with the observed version as the expectation, and exactly one of any
//! number of racing hosts wins; the losers get a conflict and nothing else
//! happens. The winner's new head version is its fence.

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::head::{HeadRecord, ManifestPtr};
use crate::journal::{JournalRecord, RecordKind};
use crate::layout;
use crate::seam::{AdminReply, Effect, ReqId, StoreFault, TimerId, Verdict};
use crate::segment::PageLoc;
use crate::types::{Epoch, Gen, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};

use super::{Daemon, Pending, Vset};

pub const MAGIC_RESUME_SET: u32 = u32::from_le_bytes(*b"BRS1");

/// How long after a resume faults are recorded as the resume set (R6.2).
/// Long enough to span a boot's storage faults; the set itself stays
/// capped at [`RESUME_SET_MAX`].
const RESUME_RECORD_WINDOW: u64 = millis(1000);

/// Resume sets stay small — they exist to beat demand faulting to the
/// working set, not to rehydrate the vset.
pub(super) const RESUME_SET_MAX: usize = 512;

fn encode_resume_set(vset: VsetId, pages: &[PageId]) -> Vec<u8> {
    let mut e = Enc::new();
    e.u16(1);
    e.u64(vset.0);
    e.u32(u32::try_from(pages.len()).expect("bounded by RESUME_SET_MAX"));
    for page in pages {
        e.u8(page.volume.idx.0);
        e.u32(page.page.0);
    }
    seal_frame(MAGIC_RESUME_SET, &e.finish())
}

fn decode_resume_set(vset: VsetId, bytes: &[u8]) -> Result<Vec<PageId>, DecodeError> {
    let payload = open_frame(MAGIC_RESUME_SET, bytes)?;
    let mut d = Dec::new(payload);
    if d.u16()? != 1 || d.u64()? != vset.0 {
        return Err(DecodeError);
    }
    let count = d.u32()? as usize;
    if count > RESUME_SET_MAX {
        return Err(DecodeError);
    }
    let mut pages = Vec::with_capacity(count);
    for _ in 0..count {
        let idx = VolumeIdx(d.u8()?);
        let page = PageNo(d.u32()?);
        pages.push(PageId {
            volume: VolumeId { vset, idx },
            page,
        });
    }
    d.finish()?;
    Ok(pages)
}

impl Daemon {
    pub(super) fn restore_vset(&mut self, req: ReqId, vset: VsetId, out: &mut Vec<Effect>) {
        if self.vsets.contains_key(&vset) {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        }
        let io = self.io();
        self.pending
            .insert(io, Pending::RestoreHeadGet { req, vset });
        out.push(Effect::StoreGet {
            io,
            key: layout::head_key(vset),
        });
    }

    pub(super) fn restore_head_done(
        &mut self,
        req: ReqId,
        vset: VsetId,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        let fail = |out: &mut Vec<Effect>| {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
        };
        let Ok(Some((version, bytes))) = result else {
            fail(out);
            return;
        };
        let Ok(head) = HeadRecord::decode(vset, &bytes) else {
            fail(out);
            return;
        };
        let Some(ptr) = head.manifest else {
            // Nothing ever backed up: nothing to restore (R6.1 applies to
            // backed-up recovery points).
            fail(out);
            return;
        };
        let claim = HeadRecord {
            vset,
            holder: self.config.host,
            // Informational; the authoritative fence is the CAS version.
            fence: 0,
            manifest: Some(ptr),
        };
        let io = self.io();
        self.pending
            .insert(io, Pending::RestoreClaim { req, vset, ptr });
        out.push(Effect::StoreCas {
            io,
            key: layout::head_key(vset),
            expected: Some(version),
            bytes: claim.encode(),
        });
    }

    pub(super) fn restore_claim_done(
        &mut self,
        req: ReqId,
        vset: VsetId,
        ptr: ManifestPtr,
        result: Result<u64, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        match result {
            Ok(fence) => {
                let io = self.io();
                self.pending.insert(
                    io,
                    Pending::RestoreManifestGet {
                        req,
                        vset,
                        ptr,
                        fence,
                    },
                );
                out.push(Effect::StoreGet {
                    io,
                    key: layout::manifest_key(vset, ptr.fence, ptr.seq),
                });
            }
            // A lost race (some other host claimed first) or an outage:
            // this host simply is not the runner (R6.3).
            Err(_) => out.push(Effect::Admin(AdminReply::AdminFailed { req })),
        }
    }

    pub(super) fn restore_manifest_done(
        &mut self,
        req: ReqId,
        vset: VsetId,
        ptr: ManifestPtr,
        fence: u64,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        let Ok(Some((_, bytes))) = result else {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        let Ok(record) = JournalRecord::decode(vset, &bytes) else {
            // The newest backed-up manifest is damaged: restore fails
            // loudly (R8.1); operators decide what comes next.
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        // The manifest is the newest backed-up recovery point (R6.1): its
        // kind decides the recovery style (R4.3).
        let resume = matches!(record.kind, RecordKind::Checkpoint { .. })
            && record.capture_seq >= record.synced_through;
        let mut chosen = record;
        let verdict = if resume {
            let RecordKind::Checkpoint { epoch, vmstate } = chosen.kind else {
                unreachable!("checked above");
            };
            Verdict::Resume { epoch, vmstate }
        } else {
            chosen.pages.retain(|page, _| !page.volume.idx.is_memory());
            Verdict::ColdBoot
        };

        let mut state = Vset::new(chosen.config);
        state.fence = fence;
        state.ready = true;
        if let RecordKind::Checkpoint { epoch, .. } = chosen.kind {
            state.epoch = epoch;
            if resume {
                state.pinned = Some(chosen.clone());
            }
        } else {
            state.epoch = Epoch(0);
        }
        state.mutation_seq = chosen.capture_seq;
        state.durable_watermark = chosen.synced_through;
        state.next_seq = chosen.seq.0 + 1;
        state.next_seg = 0; // fresh fence namespace: no collisions possible
        state.next_gen = chosen
            .pages
            .values()
            .map(|(g, _)| g.0 + 1)
            .max()
            .unwrap_or(0);
        state.page_locs = chosen.pages.clone();
        state.best = Some((chosen.capture_seq, chosen.seq));
        state.best_pages = chosen.pages.clone();
        state.backed = Some(ptr);
        state.backed_segs = chosen
            .pages
            .values()
            .map(|(_, loc)| (loc.fence, loc.seg))
            .collect();
        state.store_manifests.insert((ptr.fence, ptr.seq));
        state.head_version = Some(fence);
        state.best_record = Some(chosen);
        self.vsets.insert(vset, state);
        out.push(Effect::Admin(AdminReply::VsetRestored {
            req,
            vset,
            verdict,
        }));
        // R6.2: reach the first instruction on the verdict alone; prefetch
        // the recorded resume set concurrently, and record a fresh one from
        // what this start actually touches. Cold boots carry no latency
        // target, but warming their boot set is the same one-object bet.
        self.start_resume_recording(vset, out);
        let io = self.io();
        self.pending.insert(io, Pending::RestoreRsGet { vset });
        out.push(Effect::StoreGet {
            io,
            key: layout::resume_set_key(vset),
        });
    }

    // ── resume sets (R6.2) ──────────────────────────────────────────────

    /// Start recording the faults of a fresh resume; the window's end
    /// publishes them as the vset's resume set.
    pub(super) fn start_resume_recording(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        state.resume_recording = Some(Vec::new());
        out.push(Effect::SetTimer {
            timer: TimerId::ResumeSet(vset),
            after: RESUME_RECORD_WINDOW,
        });
    }

    /// The recording window closed: publish what the resume touched.
    pub(super) fn resume_set_flush(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(pages) = state.resume_recording.take() else {
            return;
        };
        // R4.4: only backed-up vsets may write objects; and a fenced or
        // outbound vset has no business publishing anything.
        if !state.config.backed_up || !state.ready || state.outbound.is_some() {
            return;
        }
        let bytes = encode_resume_set(vset, &pages);
        let io = self.io();
        self.pending.insert(io, Pending::ResumeSetPut);
        out.push(Effect::StorePut {
            io,
            key: layout::resume_set_key(vset),
            bytes,
        });
    }

    /// The restored vset's recorded resume set arrived (or didn't — best
    /// effort): prefetch whatever of it is still meaningful.
    pub(super) fn rs_get_done(
        &mut self,
        vset: VsetId,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        let Ok(Some((_, bytes))) = result else {
            return;
        };
        let Ok(pages) = decode_resume_set(vset, &bytes) else {
            return;
        };
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        let fetches: Vec<(PageId, Gen, PageLoc)> = pages
            .iter()
            .filter_map(|page| {
                state
                    .page_locs
                    .get(page)
                    .map(|&(generation, loc)| (*page, generation, loc))
            })
            .collect();
        for (page, generation, loc) in fetches {
            if self.cache.is_resident(page) {
                continue;
            }
            let io = self.io();
            self.pending.insert(
                io,
                Pending::Prefetch {
                    page,
                    generation,
                    loc,
                },
            );
            let key = if loc.base != 0 {
                layout::base_segment_key(loc.base, loc.fence, loc.seg)
            } else {
                layout::segment_key(vset, loc.fence, loc.seg)
            };
            out.push(Effect::StoreGetRange {
                io,
                key,
                offset: u64::from(loc.offset),
                len: u64::from(loc.len),
            });
        }
    }

    /// One prefetched page arrived: install it if nothing beat it here.
    /// Prefetch never evicts and never fails loudly — it is a bet, and a
    /// lost bet costs exactly one demand fault.
    pub(super) fn prefetch_done(
        &mut self,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        let bytes = match result {
            Ok(Some((_, b))) => Some(b),
            _ => None,
        };
        let Some(raw) = Self::verify_entry(page, generation, bytes) else {
            return;
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
        let share = (loc.base != 0).then_some((loc.base, loc.fence, loc.seg, loc.offset));
        if let Some(key) = share {
            if self.cache.base_is_resident(key) || !self.cache.reserve_if_free() {
                return;
            }
            self.cache.base_insert(key);
        } else {
            if !self.cache.reserve_if_free() {
                return;
            }
            // Readahead placement (R2.6): prefetched pages join the oldest
            // generation — an unused guess ages out first.
            self.cache.fill_slot_cold(page);
        }
        self.counters.prefetch_fills += 1;
        out.push(Effect::Fill {
            page,
            bytes: raw,
            writable: false,
            share,
        });
    }
}
