//! The guest boundary (R11.2): faults, fills, syncs — validate everything,
//! reject loudly, and never let one guest's behavior touch another vset.

use super::{Daemon, Pending};
use crate::layout;
use crate::seam::{Effect, HostMap, ReqId, StoreFault};
use crate::segment::{PageLoc, open_entry};
use crate::types::{Gen, PageId, VolumeId, VsetId, page_size};

impl Daemon {
    pub(super) fn fault(
        &mut self,
        page: PageId,
        write: bool,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(vset) = self.vsets.get_mut(&page.volume.vset) else {
            self.counters.guest_rejected += 1;
            out.push(Effect::FillFailed { page });
            return;
        };
        if !vset.ready || vset.outbound.is_some() || !vset.config.contains(page) {
            self.counters.guest_rejected += 1;
            out.push(Effect::FillFailed { page });
            return;
        }
        if self.cache.is_resident(page) {
            // Write-protect fault: first write since the last capture. (A
            // spurious resolve is harmless; a real read never traps here.)
            if write && !self.cache.is_dirty(page) {
                // An armed-but-unread page of an in-flight drain must be
                // captured before the write may land (copy-on-fault).
                self.drain_cow(page, mem);
                self.cache.mark_dirty(page);
                let vset = self.vsets.get_mut(&page.volume.vset).expect("validated");
                vset.mutation_seq += 1;
                self.counters.wp_faults += 1;
                self.counters.guest_pages_dirtied += 1;
            }
            out.push(Effect::Unprotect { page });
            return;
        }
        self.missing_fault(page, write, out);
    }

    /// Missing fault: fill from storage (R2.1). May wait under pressure.
    // One arm per fill source; splitting would scatter the fault protocol.
    #[allow(clippy::too_many_lines)]
    pub(super) fn missing_fault(&mut self, page: PageId, write: bool, out: &mut Vec<Effect>) {
        let vset = self.vsets.get_mut(&page.volume.vset).expect("validated");
        // Lazy hydration: a page whose span's leaf is not local yet has an
        // UNKNOWN location — absent-from-map means zero-fill only once the
        // span is materialized. Park until the leaf arrives; a span dead
        // everywhere is the loud R8.1 failure.
        let span = crate::mapleaf::span_of(page);
        if vset.pending_leaves.contains_key(&span) {
            vset.leaf_waiters
                .entry(span)
                .or_default()
                .push((page, write));
            return;
        }
        if vset.dead_spans.contains(&span) && !vset.page_locs.contains_key(&page) {
            self.counters.faults_unservable += 1;
            out.push(Effect::FillFailed { page });
            return;
        }
        let loc = vset.page_locs.get(&page).copied();
        // Post-resume recording window (R6.2): what faults now is what the
        // next restore should prefetch. Zero fills cost no fetch and so
        // record nothing.
        if loc.is_some()
            && let Some(recording) = &mut vset.resume_recording
            && recording.len() < super::restore::RESUME_SET_MAX
        {
            recording.push(page);
        }
        // Shared-tier hit (R5.3): a read of an already-resident base page
        // maps the same physical page — no slot, no I/O. A write diverges
        // via copy-on-write below (it needs a private slot).
        if let Some((_, loc)) = loc
            && loc.base != 0
        {
            let key = (loc.base, loc.fence, loc.seg, loc.offset);
            if self.cache.base_is_resident(key) {
                if write {
                    match self.cache.reserve_slot() {
                        None => {
                            self.counters.pressure_waits += 1;
                            self.waiters.push_back((page, write));
                        }
                        Some(victim) => {
                            if let Some(victim) = victim {
                                out.push(Effect::Evict { page: victim });
                            }
                            self.cache.fill_slot(page, true);
                            let vset = self.vsets.get_mut(&page.volume.vset).expect("validated");
                            vset.mutation_seq += 1;
                            self.counters.guest_pages_dirtied += 1;
                            vset.wedge.fills += 1;
                            self.counters.shared_fills += 1;
                            out.push(Effect::FillShared {
                                page,
                                share: key,
                                writable: true,
                            });
                        }
                    }
                } else {
                    let vset = self.vsets.get_mut(&page.volume.vset).expect("validated");
                    vset.wedge.fills += 1;
                    self.counters.shared_fills += 1;
                    out.push(Effect::FillShared {
                        page,
                        share: key,
                        writable: false,
                    });
                }
                return;
            }
        }
        match self.cache.reserve_slot() {
            None => {
                self.counters.pressure_waits += 1;
                self.waiters.push_back((page, write));
            }
            Some(victim) => {
                if let Some(victim) = victim {
                    out.push(Effect::Evict { page: victim });
                }
                match loc {
                    None => {
                        // Never written: the zero page.
                        self.cache.fill_slot(page, write);
                        let vset = self.vsets.get_mut(&page.volume.vset).expect("validated");
                        if write {
                            vset.mutation_seq += 1;
                            self.counters.guest_pages_dirtied += 1;
                        }
                        vset.wedge.fills += 1;
                        self.counters.zero_fills += 1;
                        out.push(Effect::Fill {
                            page,
                            bytes: vec![0; page_size()],
                            writable: write,
                            share: None,
                        });
                    }
                    Some((generation, loc)) => {
                        let io = self.io();
                        self.pending.insert(
                            io,
                            Pending::Fetch {
                                page,
                                write,
                                generation,
                                loc,
                            },
                        );
                        if loc.base != 0 {
                            // Base pages live only in the store (their local
                            // caching tier arrives with the shared cache).
                            out.push(Effect::StoreGetRange {
                                io,
                                key: layout::base_segment_key(loc.base, loc.fence, loc.seg),
                                offset: u64::from(loc.offset),
                                len: u64::from(loc.len),
                            });
                            let Some(Pending::Fetch {
                                page,
                                write,
                                generation,
                                loc,
                            }) = self.pending.remove(&io)
                            else {
                                unreachable!("just inserted");
                            };
                            self.pending.insert(
                                io,
                                Pending::StoreFetch {
                                    page,
                                    write,
                                    generation,
                                    loc,
                                },
                            );
                        } else {
                            out.push(Effect::BlobReadRange {
                                io,
                                name: layout::segment_blob(page.volume.vset, loc.fence, loc.seg),
                                offset: u64::from(loc.offset),
                                len: u64::from(loc.len),
                            });
                        }
                    }
                }
            }
        }
    }

    /// Retry waiting faults (a slot may have opened).
    pub(super) fn drain_waiters(&mut self, out: &mut Vec<Effect>) {
        while let Some((page, write)) = self.waiters.pop_front() {
            let before = self.waiters.len();
            let pressure_before = self.counters.pressure_waits;
            self.missing_fault(page, write, out);
            // If it re-queued itself, no slot opened: stop (FIFO fairness).
            if self.waiters.len() > before {
                self.counters.pressure_waits = pressure_before;
                let requeued = self.waiters.pop_back().expect("just pushed");
                self.waiters.push_front(requeued);
                break;
            }
        }
    }

    pub(super) fn sync(
        &mut self,
        req: ReqId,
        volume: VolumeId,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(vset) = self.vsets.get_mut(&volume.vset) else {
            self.counters.guest_rejected += 1;
            out.push(Effect::SyncFailed { req });
            return;
        };
        if !vset.ready || volume.idx.is_memory() || volume.idx.0 > vset.config.disk_volumes {
            self.counters.guest_rejected += 1;
            out.push(Effect::SyncFailed { req });
            return;
        }
        let barrier = vset.mutation_seq;
        // Ack now only if a durable record's watermark already covers this
        // barrier (R3.8) — the watermark, not merely a covering capture, is
        // what recovery honors.
        if vset.durable_watermark >= barrier {
            self.counters.syncs_acked += 1;
            out.push(Effect::SyncOk { req });
            return;
        }
        vset.pending_syncs.push((req, barrier));
        self.maybe_start_commit(volume.vset, mem, out);
    }

    // ── fill path ───────────────────────────────────────────────────────

    /// Verify fetched bytes (R8.1): frame checksum, page identity, and the
    /// exact generation must all match before a guest can observe anything.
    /// Identity is (volume index, page number, generation): a fork reads its
    /// base's segments, whose entries carry the ancestor's vset id — the
    /// vset binding comes from the fenced namespace the key lives in.
    pub(super) fn verify_entry(
        page: PageId,
        generation: Gen,
        bytes: Option<Vec<u8>>,
    ) -> Option<Vec<u8>> {
        bytes
            .and_then(|b| open_entry(page.volume.vset, &b).ok())
            .and_then(|(got_page, got_gen, raw)| {
                (got_page.volume.idx == page.volume.idx
                    && got_page.page == page.page
                    && got_gen == generation)
                    .then_some(raw)
            })
    }

    fn serve_fill(
        &mut self,
        page: PageId,
        write: bool,
        loc: PageLoc,
        raw: Vec<u8>,
        out: &mut Vec<Effect>,
    ) {
        // A base page read enters the shared tier: one physical copy for
        // every fork that ever maps it (R5.3). Writes stay private.
        let share = (loc.base != 0 && !write).then_some((loc.base, loc.fence, loc.seg, loc.offset));
        if let Some(key) = share {
            self.cache.base_insert(key);
        } else {
            self.cache.fill_slot(page, write);
        }
        if let Some(vset) = self.vsets.get_mut(&page.volume.vset) {
            if write {
                vset.mutation_seq += 1;
                self.counters.guest_pages_dirtied += 1;
            }
            vset.wedge.fills += 1;
        }
        self.counters.fills += 1;
        out.push(Effect::Fill {
            page,
            bytes: raw,
            writable: write,
            share,
        });
    }

    fn fill_exhausted(&mut self, page: PageId, out: &mut Vec<Effect>) {
        self.cache.release_slot();
        self.counters.faults_unservable += 1;
        out.push(Effect::FillFailed { page });
        self.drain_waiters(out);
    }

    /// Local fetch completed. On damage or absence, backed-up vsets fall
    /// back to the object store (R2.3's source order; the peer tier arrives
    /// with migration); everything else is exhausted and fails loudly.
    pub(super) fn fill_read_done(
        &mut self,
        page: PageId,
        write: bool,
        generation: Gen,
        loc: PageLoc,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        if let Some(raw) = Self::verify_entry(page, generation, bytes) {
            self.serve_fill(page, write, loc, raw, out);
            return;
        }
        // Peer tier (R2.3): a migrated-in vset's tail lives on its source.
        let peer = self
            .vsets
            .get(&page.volume.vset)
            .and_then(|v| v.peer_source);
        if let Some(source) = peer {
            let io = self.io();
            self.pending.insert(
                io,
                Pending::PeerFetch {
                    page,
                    write,
                    generation,
                    loc,
                },
            );
            out.push(Effect::PeerSend {
                to: source,
                msg: crate::seam::PeerMsg::FetchRange {
                    io,
                    vset: page.volume.vset,
                    fence: loc.fence,
                    seg: loc.seg,
                    offset: loc.offset,
                    len: loc.len,
                },
            });
            // The channel is lossy and the source may be down: a guest is
            // blocked on this, so re-issue until answered.
            out.push(Effect::SetTimer {
                timer: crate::seam::TimerId::PeerRetry(io),
                after: super::migrate::PEER_RETRY,
            });
            return;
        }
        let backed = self
            .vsets
            .get(&page.volume.vset)
            .is_some_and(|v| v.config.backed_up);
        if backed {
            let io = self.io();
            self.pending.insert(
                io,
                Pending::StoreFetch {
                    page,
                    write,
                    generation,
                    loc,
                },
            );
            let key = if loc.base != 0 {
                crate::layout::base_segment_key(loc.base, loc.fence, loc.seg)
            } else {
                crate::layout::segment_key(page.volume.vset, loc.fence, loc.seg)
            };
            out.push(Effect::StoreGetRange {
                io,
                key,
                offset: u64::from(loc.offset),
                len: u64::from(loc.len),
            });
            return;
        }
        self.fill_exhausted(page, out);
    }

    /// A peer fetch resolved (or failed): verify and serve, or exhaust.
    pub(super) fn peer_fetch_resolved(
        &mut self,
        page: PageId,
        write: bool,
        generation: Gen,
        loc: PageLoc,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        if let Some(raw) = Self::verify_entry(page, generation, bytes) {
            self.serve_fill(page, write, loc, raw, out);
        } else {
            self.fill_exhausted(page, out);
        }
    }

    /// Store-tier fetch completed: the last source (until peers exist).
    pub(super) fn store_fill_done(
        &mut self,
        page: PageId,
        write: bool,
        generation: Gen,
        loc: PageLoc,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        // An outage is not absence (R8.3): the store heals, and a parked
        // guest is recoverable where a killed one is not. Park the fault
        // (its cache slot stays reserved) and re-issue on the retry timer.
        // Absence and damage stay loud below (R8.1).
        if matches!(result, Err(StoreFault::Unavailable))
            && let Some(vset) = self.vsets.get_mut(&page.volume.vset)
        {
            vset.store_fill_retry.push((page, write, generation, loc));
            self.counters.store_retries += 1;
            out.push(Effect::SetTimer {
                timer: crate::seam::TimerId::FillRetry(page.volume.vset),
                after: self.config.backup_retry,
            });
            return;
        }
        let bytes = match result {
            Ok(Some((_, bytes))) => Some(bytes),
            _ => None,
        };
        if let Some(raw) = Self::verify_entry(page, generation, bytes) {
            self.serve_fill(page, write, loc, raw, out);
        } else {
            self.fill_exhausted(page, out);
        }
    }

    /// Re-issue the demand fills a store outage parked: their guests are
    /// still blocked, and the fetch that parks again re-arms the timer.
    pub(super) fn fill_retry_tick(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return; // fenced or released while parked: the guests died with it
        };
        let parked = std::mem::take(&mut state.store_fill_retry);
        for (page, write, generation, loc) in parked {
            let io = self.io();
            self.pending.insert(
                io,
                Pending::StoreFetch {
                    page,
                    write,
                    generation,
                    loc,
                },
            );
            let key = if loc.base != 0 {
                crate::layout::base_segment_key(loc.base, loc.fence, loc.seg)
            } else {
                crate::layout::segment_key(page.volume.vset, loc.fence, loc.seg)
            };
            out.push(Effect::StoreGetRange {
                io,
                key,
                offset: u64::from(loc.offset),
                len: u64::from(loc.len),
            });
        }
    }
}
