//! Restore (R6.1): bring a backed-up vset onto this host from the object
//! store alone — no prior local state, no reachable previous host. The head
//! CAS is the entire assignment protocol (R6.3): read the head, claim it
//! with the observed version as the expectation, and exactly one of any
//! number of racing hosts wins; the losers get a conflict and nothing else
//! happens. The winner's new head version is its fence.

use std::collections::BTreeSet;

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::head::{HeadRecord, ManifestPtr};
use crate::journal::{JournalRecord, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::{LeafPtr, MapLeaf};
use crate::seam::{AdminReply, Effect, HostMap, ReqId, StoreFault, TimerId, Verdict};
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
const LEAF_FETCH_IN_FLIGHT: usize = 32;

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
        let (version, bytes) = match result {
            Ok(Some(found)) => found,
            // An outage queues the restore, it never fails it (R8.3).
            Err(StoreFault::Unavailable) => {
                self.park_restore(req, vset, out);
                return;
            }
            Ok(None) | Err(StoreFault::CasConflict { .. }) => {
                fail(out);
                return;
            }
        };
        let Ok(head) = HeadRecord::decode(vset, &bytes) else {
            fail(out);
            return;
        };
        if head.stash.is_some() {
            // A peer-stashed head does not record the peer's protected-sync
            // watermark. The assigned peer may therefore hold a newer
            // recovery point than the published manifest. Only the operator
            // inventory path can compare both copies before claiming.
            fail(out);
            return;
        }
        let Some(ptr) = head.manifest else {
            // Nothing ever backed up: nothing to restore (R6.1 applies to
            // backed-up recovery points).
            fail(out);
            return;
        };
        let Some(stash) = self.initial_stash_assignment(vset) else {
            fail(out);
            return;
        };
        let claim = HeadRecord {
            vset,
            holder: self.config.host,
            // Informational; the authoritative fence is the CAS version.
            fence: 0,
            manifest: Some(ptr),
            stash: Some(stash),
            retired_stashes: head.retired_stashes,
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
                self.counters.assignment_claims += 1;
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
            // An outage leaves the claim's outcome unknown: retry from the
            // head read (R8.3) — if our claim actually landed, the re-read
            // sees this host as holder and the re-claim just bumps the
            // fence.
            Err(StoreFault::Unavailable) => self.park_restore(req, vset, out),
            // A lost race (some other host claimed first): this host
            // simply is not the runner (R6.3).
            Err(StoreFault::CasConflict { .. }) => {
                self.counters.assignment_claim_conflicts += 1;
                out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            }
        }
    }

    /// Park a restore that hit a store outage; the retry timer re-runs it
    /// from the head read (every step up to serving is idempotent).
    fn park_restore(&mut self, req: ReqId, vset: VsetId, out: &mut Vec<Effect>) {
        self.counters.store_retries += 1;
        self.restore_retries.insert(vset, req);
        out.push(Effect::SetTimer {
            timer: TimerId::RestoreRetry(vset),
            after: self.config.backup_retry,
        });
    }

    pub(super) fn restore_retry(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(req) = self.restore_retries.remove(&vset) else {
            return;
        };
        self.restore_vset(req, vset, out);
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
        let bytes = match result {
            Ok(Some((_, bytes))) => bytes,
            // We hold the claim already; wait the outage out (R8.3).
            Err(StoreFault::Unavailable) => {
                self.park_restore(req, vset, out);
                return;
            }
            Ok(None) | Err(StoreFault::CasConflict { .. }) => {
                out.push(Effect::Admin(AdminReply::AdminFailed { req }));
                return;
            }
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
            && record.capture_seq >= record.sync_covered_through;
        let mut chosen = record;
        let verdict = if chosen.config.kind == VsetKind::Database {
            Verdict::DatabaseReady {
                synced_through: chosen.sync_covered_through,
            }
        } else if resume {
            let RecordKind::Checkpoint { epoch, vmstate } = chosen.kind else {
                unreachable!("checked above");
            };
            Verdict::Resume { epoch, vmstate }
        } else {
            // Memory is invalid on cold boot (R3.7): its overlay entries
            // drop, and its spans' leaves are never even fetched.
            chosen
                .overlay
                .retain(|page, _| !chosen.config.is_memory(page.volume.idx));
            chosen
                .leaves
                .retain(|span, _| !crate::mapleaf::span_is_memory(*span));
            Verdict::ColdBoot
        };

        let database_vset = chosen.config.kind == VsetKind::Database;
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
        // The chosen record was fetched through the published manifest, so
        // it satisfies the stronger mode without consulting a peer.
        state.sync_ack_through = chosen.sync_covered_through;
        state.next_seg = 0; // fresh fence namespace: no collisions possible
        state.backed = Some(ptr);
        state.backed_segs = chosen
            .overlay
            .values()
            .filter(|(_, loc)| loc.base == 0)
            .map(|(_, loc)| (loc.fence, loc.seg))
            .collect();
        state.store_manifests.insert((ptr.fence, ptr.seq));
        state.head_version = Some(fence);
        state.adopt_record(chosen);
        state.stash_assignment = self.initial_stash_assignment(vset);
        self.vsets.insert(vset, state);
        out.push(Effect::Admin(AdminReply::VsetRestored {
            req,
            vset,
            verdict,
        }));
        self.request_pending_leaves(vset, out);
        // R6.2: reach the first instruction on the verdict alone; prefetch
        // the recorded resume set concurrently, and record a fresh one from
        // what this start actually touches. Cold boots carry no latency
        // target, but warming their boot set is the same one-object bet.
        if !database_vset {
            self.start_resume_recording(vset, out);
            let io = self.io();
            self.pending.insert(io, Pending::RestoreRsGet { vset });
            out.push(Effect::StoreGet {
                io,
                key: layout::resume_set_key(vset),
            });
        }
    }

    // ── lazy leaf hydration (restore, migration, forks) ─────────────────

    /// Issue fetches for every span still pending — at adoption and on
    /// retries. Source: the migration peer if one serves us, else the
    /// store. Arrival is idempotent; in-flight spans are skipped.
    pub(super) fn request_pending_leaves(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        let peer = state.peer_source;
        let in_flight: BTreeSet<u32> = self
            .pending
            .values()
            .filter_map(|pending| match pending {
                Pending::LeafGet {
                    vset: owner, span, ..
                }
                | Pending::PeerLeafFetch {
                    vset: owner, span, ..
                } if *owner == vset => Some(*span),
                _ => None,
            })
            .collect();
        let available = LEAF_FETCH_IN_FLIGHT.saturating_sub(in_flight.len());
        let pending: Vec<(u32, LeafPtr)> = state
            .pending_leaves
            .iter()
            .filter(|(span, _)| !in_flight.contains(span))
            .take(available)
            .map(|(&span, &ptr)| (span, ptr))
            .collect();
        for (span, ptr) in pending {
            let io = self.io();
            if let Some(source) = peer {
                self.pending
                    .insert(io, Pending::PeerLeafFetch { vset, span, ptr });
                out.push(Effect::PeerSend {
                    to: source,
                    msg: crate::seam::PeerMsg::FetchLeaf {
                        io,
                        vset,
                        base: ptr.base,
                        fence: ptr.fence,
                        id: ptr.id,
                    },
                });
                out.push(Effect::SetTimer {
                    timer: TimerId::PeerRetry(io),
                    after: super::migrate::PEER_RETRY,
                });
            } else {
                self.pending
                    .insert(io, Pending::LeafGet { vset, span, ptr });
                let key = if ptr.base == 0 {
                    layout::leaf_key(vset, ptr.fence, ptr.id)
                } else {
                    layout::base_leaf_key(ptr.base, ptr.fence, ptr.id)
                };
                out.push(Effect::StoreGet { io, key });
            }
        }
    }

    /// A store leaf fetch resolved.
    #[allow(clippy::type_complexity)]
    pub(super) fn leaf_get_done(
        &mut self,
        vset: VsetId,
        span: u32,
        ptr: LeafPtr,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        if let Ok(found) = result {
            self.leaf_arrived(vset, span, ptr, found.map(|(_, b)| b), mem, out);
            return;
        }
        // Transient outage: retry — hydration never gives up (R8.3).
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        if !state.leaf_retrying {
            state.leaf_retrying = true;
            out.push(Effect::SetTimer {
                timer: TimerId::LeafRetry(vset),
                after: self.config.backup_retry,
            });
        }
    }

    pub(super) fn leaf_retry(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        if let Some(state) = self.vsets.get_mut(&vset) {
            state.leaf_retrying = false;
            self.request_pending_leaves(vset, out);
        }
    }

    /// A leaf's bytes arrived (store or peer): verify, keep a local
    /// verbatim copy (recovery must find every leaf its records name),
    /// merge into the serving map, and wake the span's parked faults.
    /// Missing or corrupt = the span is dead and its pages fail loudly
    /// (R8.1).
    pub(super) fn leaf_arrived(
        &mut self,
        vset_id: VsetId,
        span: u32,
        ptr: LeafPtr,
        bytes: Option<Vec<u8>>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        if state.pending_leaves.get(&span) != Some(&ptr) {
            return; // stale reply to a span already resolved
        }
        let owner = if ptr.base == 0 {
            vset_id
        } else {
            VsetId(ptr.base)
        };
        let leaf = bytes
            .as_deref()
            .and_then(|b| MapLeaf::decode(owner, ptr.fence, ptr.id, b).ok())
            .filter(|leaf| leaf.span == span);
        let Some(leaf) = leaf else {
            state.pending_leaves.remove(&span);
            state.wedge.hydration += 1;
            state.dead_spans.insert(span);
            let waiters = state.leaf_waiters.remove(&span).unwrap_or_default();
            for (page, _) in waiters {
                self.counters.faults_unservable += 1;
                out.push(Effect::FillFailed { page });
            }
            self.drive_database(vset_id, mem, out);
            self.request_pending_leaves(vset_id, out);
            return;
        };
        let blob = bytes.expect("decoded from bytes");
        let segs: BTreeSet<(u64, crate::types::SegId)> = leaf
            .entries
            .iter()
            .filter(|(_, _, _, loc)| loc.base == 0)
            .map(|&(_, _, _, loc)| (loc.fence, loc.seg))
            .collect();
        // Own-namespace leaves fetched from the store are, by definition,
        // already backed — as are the segments they reference.
        let backed = state.peer_source.is_none() && ptr.base == 0;
        if backed {
            state.backed_leaves.insert((ptr.fence, ptr.id));
            state.backed_segs.extend(segs.iter().copied());
        }
        state.leaf_blobs.insert(ptr, (blob.len() as u64, segs));
        for &(idx, page_no, generation, loc) in &leaf.entries {
            let page = PageId {
                volume: VolumeId { vset: vset_id, idx },
                page: page_no,
            };
            if !state.config.contains(page) {
                continue;
            }
            state.map_adopt(page, generation, loc);
            state.next_gen = state.next_gen.max(generation.0 + 1);
        }
        state.pending_leaves.remove(&span);
        state.wedge.hydration += 1;
        let waiters = state.leaf_waiters.remove(&span).unwrap_or_default();
        let name = if ptr.base == 0 {
            layout::leaf_blob(vset_id, ptr.fence, ptr.id)
        } else {
            layout::base_leaf_blob(vset_id, ptr.base, ptr.fence, ptr.id)
        };
        self.local_bytes += blob.len() as u64;
        self.counters.leaf_fills += 1;
        let io = self.io();
        self.pending.insert(io, Pending::LeafCopyWrite);
        out.push(Effect::BlobWrite {
            io,
            name,
            bytes: blob,
        });
        for (page, write) in waiters {
            self.fault(page, write, mem, out);
        }
        self.drive_database(vset_id, mem, out);
        self.request_pending_leaves(vset_id, out);
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
        // A fenced or outbound vset has no business publishing anything.
        if !state.ready || state.outbound.is_some() {
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
            let memory = self.vsets[&page.volume.vset]
                .config
                .is_memory(page.volume.idx);
            self.cache.fill_slot_cold(page, memory);
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
