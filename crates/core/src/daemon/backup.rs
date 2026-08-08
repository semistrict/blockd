//! Backup (R4.2): an asynchronous background copy of locally durable state,
//! flowing continuously as records finalize — never on a guest-visible path.
//! Pipeline per vset, one publish at a time: every segment the newest record
//! references and the store lacks is copied **verbatim** (R8.4), then the
//! record's bytes become the manifest, then a head CAS advances the newest
//! backed-up pointer. A lost head CAS means another host holds the vset:
//! this one is fenced (R6.4) and structurally cannot publish again.
//!
//! Store faults never lose queued work (R8.3): the publish is dropped and a
//! retry timer re-derives it from current local truth.

use super::{Daemon, Pending, Publish};
use crate::head::{HeadRecord, ManifestPtr};
use crate::journal::DurabilityMode;
use crate::layout;
use crate::seam::{Effect, HostMap, StoreFault, TimerId};
use crate::segment::scan_segment;
use crate::types::{SegId, VsetId};

impl Daemon {
    /// Start (or continue) publishing if the newest record has not been
    /// backed up. Called after every finalize and on retry ticks.
    pub(super) fn maybe_publish(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        if state.config.durability != DurabilityMode::Backup
            || state.publish.is_some()
            || (state.migrated_verdict.is_some() && !state.migration_head_claimed)
        {
            return;
        }
        let Some(head_version) = state.head_version else {
            // Head unknown (fresh recovery): learn it first.
            if !state.head_refreshing {
                state.head_refreshing = true;
                let io = self.io();
                self.pending
                    .insert(io, Pending::HeadRefresh { vset: vset_id });
                out.push(Effect::StoreGet {
                    io,
                    key: layout::head_key(vset_id),
                });
            }
            return;
        };
        let _ = head_version;
        let Some(record) = self.vsets[&vset_id].best_record.clone() else {
            return;
        };
        let state = self.vsets.get_mut(&vset_id).expect("just seen");
        let newer = state
            .backed
            .is_none_or(|ptr| (record.capture_seq, record.seq) > (ptr.capture_seq, ptr.seq));
        if !newer {
            return;
        }
        // Segments the record references: its overlay's, plus those of
        // every local leaf it points at (a pending leaf came FROM the
        // store, so its segments are already backed).
        let mut segs_todo: Vec<(u64, SegId)> = record
            .overlay
            .values()
            .filter(|(_, loc)| loc.base == 0) // base segments are shared, kept by their base
            .map(|(_, loc)| (loc.fence, loc.seg))
            .chain(
                record
                    .leaves
                    .values()
                    .filter(|ptr| ptr.base == 0)
                    .filter_map(|ptr| state.leaf_blobs.get(ptr))
                    .flat_map(|(_, segs)| segs.iter().copied()),
            )
            .filter(|key| !state.backed_segs.contains(key))
            .collect();
        segs_todo.sort_unstable();
        segs_todo.dedup();
        let leaves_todo: Vec<(u64, u64)> = record
            .leaves
            .values()
            .filter(|ptr| ptr.base == 0)
            .map(|ptr| (ptr.fence, ptr.id))
            .filter(|key| !state.backed_leaves.contains(key))
            .collect();
        state.publish = Some(Publish {
            record,
            segs_todo,
            leaves_todo,
        });
        self.publish_step(vset_id, out);
    }

    /// Drive the publish pipeline one step: next segment, else manifest.
    pub(super) fn publish_step(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let Some(publish) = &mut state.publish else {
            return;
        };
        if let Some(&(fence, seg)) = publish.segs_todo.last() {
            let io = self.io();
            self.pending.insert(
                io,
                Pending::PubSegRead {
                    vset: vset_id,
                    fence,
                    seg,
                },
            );
            out.push(Effect::BlobRead {
                io,
                name: layout::segment_blob(vset_id, fence, seg),
            });
            return;
        }
        if let Some(&(fence, id)) = publish.leaves_todo.last() {
            let io = self.io();
            self.pending.insert(
                io,
                Pending::PubLeafRead {
                    vset: vset_id,
                    fence,
                    id,
                },
            );
            out.push(Effect::BlobRead {
                io,
                name: layout::leaf_blob(vset_id, fence, id),
            });
            return;
        }
        let record = &publish.record;
        let ptr = ManifestPtr {
            fence: record.fence,
            seq: record.seq,
            capture_seq: record.capture_seq,
        };
        let bytes = record.encode(vset_id);
        let io = self.io();
        self.pending
            .insert(io, Pending::PubManifestPut { vset: vset_id, ptr });
        out.push(Effect::StorePut {
            io,
            key: layout::manifest_key(vset_id, ptr.fence, ptr.seq),
            bytes,
        });
    }

    /// Local read of a segment headed for the store.
    pub(super) fn pub_seg_read_done(
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
        if state.publish.is_none() {
            return;
        }
        // Verify every frame checksum before upload: publishing damaged
        // bytes would poison the backup tier (R8.1 applies both ways).
        let intact = bytes.as_ref().is_some_and(|b| {
            scan_segment(b).is_ok_and(|(v, f, s, _)| v == vset_id && f == fence && s == seg)
        });
        let Some(blob) = bytes.filter(|_| intact) else {
            // Superseded-and-deleted or damaged: abandon; the retry re-derives
            // from the (by then newer) local truth.
            state.publish = None;
            self.backup_backoff(vset_id, out);
            return;
        };
        let io = self.io();
        self.pending.insert(
            io,
            Pending::PubSegPut {
                vset: vset_id,
                fence,
                seg,
            },
        );
        out.push(Effect::StorePut {
            io,
            key: layout::segment_key(vset_id, fence, seg),
            bytes: blob,
        });
    }

    /// Local read of a map leaf headed for the store.
    pub(super) fn pub_leaf_read_done(
        &mut self,
        vset_id: VsetId,
        fence: u64,
        id: u64,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        if state.publish.is_none() {
            return;
        }
        // Verify before upload (R8.1 in both directions).
        let intact = bytes
            .as_deref()
            .is_some_and(|b| crate::mapleaf::MapLeaf::decode(vset_id, fence, id, b).is_ok());
        let Some(blob) = bytes.filter(|_| intact) else {
            state.publish = None;
            self.backup_backoff(vset_id, out);
            return;
        };
        let io = self.io();
        self.pending.insert(
            io,
            Pending::PubLeafPut {
                vset: vset_id,
                fence,
                id,
            },
        );
        out.push(Effect::StorePut {
            io,
            key: layout::leaf_key(vset_id, fence, id),
            bytes: blob,
        });
    }

    /// A store write of the publish pipeline completed.
    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn pub_put_done(
        &mut self,
        pending: Pending,
        result: Result<u64, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        match pending {
            Pending::PubSegPut { vset, fence, seg } => match result {
                Ok(_) => {
                    let Some(state) = self.vsets.get_mut(&vset) else {
                        return;
                    };
                    state.backed_segs.insert((fence, seg));
                    if let Some(publish) = &mut state.publish {
                        publish.segs_todo.retain(|&k| k != (fence, seg));
                    }
                    self.publish_step(vset, out);
                }
                Err(_) => self.backup_fault(vset, out),
            },
            Pending::PubLeafPut { vset, fence, id } => match result {
                Ok(_) => {
                    let Some(state) = self.vsets.get_mut(&vset) else {
                        return;
                    };
                    state.backed_leaves.insert((fence, id));
                    if let Some(publish) = &mut state.publish {
                        publish.leaves_todo.retain(|&k| k != (fence, id));
                    }
                    self.publish_step(vset, out);
                }
                Err(_) => self.backup_fault(vset, out),
            },
            Pending::PubManifestPut { vset, ptr } => match result {
                Ok(_) => {
                    let Some(state) = self.vsets.get_mut(&vset) else {
                        return;
                    };
                    state.store_manifests.insert((ptr.fence, ptr.seq));
                    let head = HeadRecord {
                        vset,
                        holder: self.config.host,
                        fence: state.fence,
                        manifest: Some(ptr),
                        stash: state.stash_assignment,
                        retired_stashes: state.retired_stashes.clone(),
                    };
                    let expected = state.head_version;
                    let io = self.io();
                    self.pending.insert(io, Pending::PubHeadCas { vset, ptr });
                    out.push(Effect::StoreCas {
                        io,
                        key: layout::head_key(vset),
                        expected,
                        bytes: head.encode(),
                    });
                }
                Err(_) => self.backup_fault(vset, out),
            },
            Pending::PubHeadCas { vset, ptr } => match result {
                Ok(version) => {
                    self.counters.manifests_published += 1;
                    let Some(state) = self.vsets.get_mut(&vset) else {
                        return;
                    };
                    state.head_version = Some(version);
                    state.backed = Some(ptr);
                    let published_sync = state
                        .publish
                        .as_ref()
                        .map_or(0, |publish| publish.record.sync_covered_through);
                    state.sync_ack_through = state.sync_ack_through.max(published_sync);
                    state.publish = None;
                    self.drain_sync_acks(vset, out);
                    self.store_cleanup(vset, out);
                    self.maybe_publish(vset, out);
                    self.maybe_finish_backed_migration(vset, out);
                }
                Err(StoreFault::CasConflict { .. }) => {
                    // Another holder claimed the head: structurally fenced
                    // (R6.4) — nothing this host writes is reachable now.
                    self.fence_vset(vset, out);
                }
                Err(StoreFault::Unavailable) => self.backup_fault(vset, out),
            },
            _ => out.push(Effect::Abort {
                reason: "store put completion for non-put io",
            }),
        }
    }

    /// A store fault paused this vset's publish (R8.3): drop it and retry.
    pub(super) fn backup_fault(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        if let Some(state) = self.vsets.get_mut(&vset_id) {
            state.publish = None;
        }
        self.backup_backoff(vset_id, out);
    }

    pub(super) fn backup_backoff(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        self.counters.store_retries += 1;
        out.push(Effect::SetTimer {
            timer: TimerId::Backup(vset_id),
            after: self.config.backup_retry,
        });
    }

    /// Retry tick: re-derive whatever store work is owed.
    pub(super) fn backup_tick(
        &mut self,
        vset_id: VsetId,
        _mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        if state.config.durability.uses_store() && !state.ready && state.create_req.is_some() {
            // Creation-time head claim still owed.
            self.head_create(vset_id, out);
            return;
        }
        if state.config.durability.requires_peer_sync() {
            self.maybe_peer_head_publish(vset_id, out);
            self.replica_tick(vset_id, out);
            return;
        }
        self.maybe_publish(vset_id, out);
    }

    /// Claim a brand-new head at vset creation (create-if-absent CAS).
    pub(super) fn head_create(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let stash = self
            .vsets
            .get(&vset_id)
            .is_some_and(|state| state.config.durability.requires_peer_sync())
            .then(|| self.initial_stash_assignment(vset_id))
            .flatten();
        if let Some(state) = self.vsets.get_mut(&vset_id) {
            state.stash_assignment = stash;
        }
        let head = HeadRecord {
            vset: vset_id,
            holder: self.config.host,
            // The authoritative fence is the version this CAS returns; the
            // content field is informational and corrected on first publish.
            fence: 0,
            manifest: None,
            stash,
            retired_stashes: Vec::new(),
        };
        let io = self.io();
        self.pending
            .insert(io, Pending::HeadCreate { vset: vset_id });
        out.push(Effect::StoreCas {
            io,
            key: layout::head_key(vset_id),
            expected: None,
            bytes: head.encode(),
        });
    }

    /// Completion of the creation-time head claim.
    pub(super) fn head_create_done(
        &mut self,
        vset_id: VsetId,
        result: Result<u64, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        match result {
            Ok(version) => {
                self.counters.assignment_claims += 1;
                state.fence = version;
                state.head_version = Some(version);
                if state.fork_from.is_some() {
                    // Forked creation: materialize the base's map first.
                    self.fork_fetch_base(vset_id, out);
                } else {
                    // Head owned: now make the vset locally durable.
                    self.start_record_only_capture(vset_id, out);
                }
            }
            Err(StoreFault::CasConflict { .. }) => {
                self.counters.assignment_claim_conflicts += 1;
                // The id is already claimed: creation fails loudly (ids are
                // never reused, R6.5 — this is a control-plane defect).
                let req = state.create_req.take();
                self.vsets.remove(&vset_id);
                self.purge_vset_pages(vset_id, out);
                if let Some(req) = req {
                    out.push(Effect::Admin(crate::seam::AdminReply::AdminFailed { req }));
                }
            }
            Err(StoreFault::Unavailable) => self.backup_backoff(vset_id, out),
        }
    }

    /// Head re-read after local recovery: detect takeover (R6.4).
    #[allow(clippy::too_many_lines)]
    pub(super) fn head_refresh_done(
        &mut self,
        vset_id: VsetId,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        state.head_refreshing = false;
        match result {
            Ok(Some((version, bytes))) => {
                let Ok(head) = HeadRecord::decode(vset_id, &bytes) else {
                    // A damaged head is a damaged assignment authority:
                    // stand down loudly rather than guess (R8.1).
                    self.fence_vset(vset_id, out);
                    return;
                };
                let expected_membership = self
                    .config
                    .replica_placement
                    .as_ref()
                    .map(|placement| placement.membership_epoch);
                if state.config.durability == DurabilityMode::PeerStashed
                    && (expected_membership.is_none()
                        || head.stash.map(|stash| stash.membership_epoch) != expected_membership)
                {
                    // Differently configured hosts must not interpret the
                    // same assignment epoch differently.
                    self.fence_vset(vset_id, out);
                    return;
                }
                let releasable_retired: Vec<_> = head.manifest.map_or_else(Vec::new, |manifest| {
                    head.retired_stashes
                        .iter()
                        .copied()
                        .filter(|retired| {
                            (retired.through.writer_fence, retired.through.seq)
                                <= (manifest.fence, manifest.seq)
                        })
                        .collect()
                });
                if head.holder == self.config.host {
                    let assignment_changed = state.stash_assignment != head.stash;
                    state.head_version = Some(version);
                    state.backed = head.manifest;
                    state.stash_assignment = head.stash;
                    state.retired_stashes = head.retired_stashes;
                    if assignment_changed {
                        // Reads and timers from the losing assignment CAS may
                        // still complete. Cancel their scoped send state so a
                        // late local read cannot emit bytes to the old target;
                        // maybe_replicate below re-derives from this head.
                        state.replica_send = None;
                        state.peer_artifacts.clear();
                        state.peer_committed = None;
                        state.peer_committed_record = None;
                        state.peer_upload_done = None;
                    }
                    if let Some(ptr) = head.manifest {
                        state.store_manifests.insert((ptr.fence, ptr.seq));
                    }
                    let releasable_active = head.manifest.and_then(|manifest| {
                        state.best_record.as_ref().and_then(|record| {
                            ((record.fence, record.seq, record.capture_seq)
                                == (manifest.fence, manifest.seq, manifest.capture_seq))
                                .then(|| {
                                    state.stash_assignment.map(|assignment| {
                                        (
                                            assignment.active_peer,
                                            assignment.active_assignment_epoch,
                                            Self::commit_info(record),
                                        )
                                    })
                                })
                                .flatten()
                        })
                    });
                    if let Some(verdict) = state.pending_verdict.take() {
                        // Serve only if local truth is at least as new as
                        // the backup; journal damage can leave it behind, in
                        // which case the store copy is the truth (R3.8) —
                        // stand down and let the control plane restore.
                        let local = state.best.map_or((0, 0), |(c, s)| (c, s.0));
                        let behind = head
                            .manifest
                            .is_some_and(|ptr| (ptr.capture_seq, ptr.seq.0) > local);
                        if behind {
                            self.fence_vset(vset_id, out);
                            return;
                        }
                        state.ready = true;
                        let resumed = matches!(verdict, crate::seam::Verdict::Resume { .. });
                        out.push(Effect::Admin(crate::seam::AdminReply::VsetRecovered {
                            vset: vset_id,
                            verdict,
                        }));
                        if resumed {
                            // Record what this resume touches as the next
                            // restore's resume set (R6.2).
                            self.start_resume_recording(vset_id, out);
                        }
                    }
                    self.maybe_publish(vset_id, out);
                    self.maybe_replicate(vset_id, out);
                    self.maybe_peer_head_publish(vset_id, out);
                    if let Some(release) = releasable_active {
                        self.queue_replica_release(vset_id, release);
                    }
                    for retired in releasable_retired {
                        self.queue_replica_release(
                            vset_id,
                            (retired.peer, retired.assignment_epoch, retired.through),
                        );
                    }
                    self.replica_release_retry(vset_id, out);
                } else {
                    self.fence_vset(vset_id, out);
                }
            }
            Ok(None) => {
                // Backed-up vset with no head: create it (recovered from a
                // crash that predated the claim).
                self.head_create(vset_id, out);
            }
            Err(_) => self.backup_backoff(vset_id, out),
        }
    }

    /// After a successful head advance, reclaim superseded store objects
    /// (R4.5: superseded state, explicit deletes, never by age).
    pub(super) fn store_cleanup(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset_id) else {
            return;
        };
        let Some(current) = state.backed else {
            return;
        };
        let keep_manifest = (current.fence, current.seq);
        let dead_manifests: Vec<(u64, crate::types::JournalSeq)> = state
            .store_manifests
            .iter()
            .copied()
            .filter(|&key| key != keep_manifest)
            .collect();
        for key in dead_manifests {
            state.store_manifests.remove(&key);
            out.push(Effect::StoreDelete {
                key: layout::manifest_key(vset_id, key.0, key.1),
            });
        }
        let Some(record) = state
            .best_record
            .as_ref()
            .filter(|r| (r.capture_seq, r.seq) == (current.capture_seq, current.seq))
        else {
            return;
        };
        // The backed manifest's references: overlay segments, its leaves,
        // and the segments those leaves hold (from the local copies).
        let mut referenced: std::collections::BTreeSet<(u64, SegId)> = record
            .overlay
            .values()
            .map(|(_, loc)| (loc.fence, loc.seg))
            .collect();
        let leaf_refs: std::collections::BTreeSet<(u64, u64)> = record
            .leaves
            .values()
            .filter(|ptr| ptr.base == 0)
            .map(|ptr| (ptr.fence, ptr.id))
            .collect();
        for ptr in record.leaves.values() {
            if let Some((_, segs)) = state.leaf_blobs.get(ptr) {
                referenced.extend(segs.iter().copied());
            }
        }
        if referenced.is_empty() && leaf_refs.is_empty() {
            return;
        }
        let dead_leaves: Vec<(u64, u64)> = state
            .backed_leaves
            .iter()
            .copied()
            .filter(|key| !leaf_refs.contains(key))
            .collect();
        for (fence, id) in dead_leaves {
            state.backed_leaves.remove(&(fence, id));
            out.push(Effect::StoreDelete {
                key: layout::leaf_key(vset_id, fence, id),
            });
        }
        // In-flight store fetches pin their segment, exactly like local
        // cleanup does for local fetches.
        let mut pinned: std::collections::BTreeSet<(u64, SegId)> =
            std::collections::BTreeSet::new();
        for pending in self.pending.values() {
            if let Pending::StoreFetch { page, loc, .. } = pending
                && page.volume.vset == vset_id
            {
                pinned.insert((loc.fence, loc.seg));
            }
        }
        let state = self.vsets.get_mut(&vset_id).expect("just seen");
        let dead_segs: Vec<(u64, SegId)> = state
            .backed_segs
            .iter()
            .copied()
            .filter(|key| !referenced.contains(key) && !pinned.contains(key))
            .collect();
        for (fence, seg) in dead_segs {
            state.backed_segs.remove(&(fence, seg));
            out.push(Effect::StoreDelete {
                key: layout::segment_key(vset_id, fence, seg),
            });
        }
    }
}
