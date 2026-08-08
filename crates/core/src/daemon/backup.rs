//! Object-store head ownership, retry, recovery refresh, and cleanup.
//!
//! Recovery data reaches the object store through the passive replica. The
//! primary performs only fenced head operations and explicit reclamation.

use super::{Daemon, Pending};
use crate::head::HeadRecord;
use crate::layout;
use crate::seam::{Effect, HostMap, StoreFault, TimerId};
use crate::types::{SegId, VsetId};

impl Daemon {
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
        let Some(state) = self.vsets.get(&vset_id) else {
            return;
        };
        if !state.ready && state.create_req.is_some() {
            // Creation-time head claim still owed.
            self.head_create(vset_id, out);
            return;
        }
        self.maybe_peer_head_publish(vset_id, out);
        self.replica_tick(vset_id, out);
    }

    /// Claim a brand-new head at vset creation (create-if-absent CAS).
    pub(super) fn head_create(&mut self, vset_id: VsetId, out: &mut Vec<Effect>) {
        let stash = self.initial_stash_assignment(vset_id);
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
                let recovering_legacy = state.pending_verdict.is_some();
                let forked = state.fork_from.is_some();
                if recovering_legacy {
                    self.finish_pending_recovery(vset_id, out);
                }
                if forked {
                    // Forked creation: materialize the base's map first.
                    self.fork_fetch_base(vset_id, out);
                } else {
                    // Head owned: now make the vset locally durable.
                    self.start_record_only_capture(vset_id, out);
                }
            }
            Err(StoreFault::CasConflict { .. }) => {
                self.counters.assignment_claim_conflicts += 1;
                if state.pending_verdict.is_some() && state.create_req.is_none() {
                    // A legacy local-only vset may race another bootstrap of
                    // its first durable head. Re-read authority instead of
                    // deleting recoverable local state.
                    state.head_refreshing = true;
                    let io = self.io();
                    self.pending
                        .insert(io, super::Pending::HeadRefresh { vset: vset_id });
                    out.push(Effect::StoreGet {
                        io,
                        key: crate::layout::head_key(vset_id),
                    });
                    return;
                }
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
                if head.stash.is_none() {
                    // Legacy local/backup heads predate passive assignment.
                    // The current holder upgrades the head once, then opens
                    // the recovered vset; all future syncs remain gated on a
                    // newly seeded passive commit.
                    if head.holder != self.config.host || expected_membership.is_none() {
                        self.fence_vset(vset_id, out);
                        return;
                    }
                    let Some(stash) = self.initial_stash_assignment(vset_id) else {
                        self.fence_vset(vset_id, out);
                        return;
                    };
                    let state = self.vsets.get_mut(&vset_id).expect("known vset");
                    state.head_version = Some(version);
                    state.backed = head.manifest;
                    state.stash_assignment = Some(stash);
                    state.retired_stashes.clear();
                    state.replica_assignment_proposal = Some(super::ReplicaAssignmentProposal {
                        assignment: stash,
                        activation: None,
                    });
                    if let Some(ptr) = head.manifest {
                        state.store_manifests.insert((ptr.fence, ptr.seq));
                    }
                    self.replica_head_write_step(vset_id, out);
                    return;
                }
                if expected_membership.is_none()
                    || head.stash.map(|stash| stash.membership_epoch) != expected_membership
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
                    let proposal = state.replica_assignment_proposal;
                    let proposal_published =
                        proposal.is_some_and(|proposal| head.stash == Some(proposal.assignment));
                    let provisional = proposal.is_some_and(|proposal| {
                        state.stash_assignment == Some(proposal.assignment)
                    });
                    let head_assignment_epoch = head
                        .stash
                        .map_or(0, |assignment| assignment.assignment_epoch);
                    let preserve_provisional = proposal.is_some_and(|proposal| {
                        provisional
                            && !proposal_published
                            && head.fence == state.fence
                            && head_assignment_epoch < proposal.assignment.assignment_epoch
                    });
                    let assignment_changed =
                        !preserve_provisional && state.stash_assignment != head.stash;
                    state.head_version = Some(version);
                    state.backed = head.manifest;
                    if proposal_published {
                        state.replica_assignment_proposal = None;
                    } else if !preserve_provisional {
                        // A same-or-newer authoritative assignment supersedes
                        // a proposal that was never installed locally.
                        state.replica_assignment_proposal = None;
                    }
                    if !preserve_provisional {
                        state.stash_assignment = head.stash;
                        state.retired_stashes = head.retired_stashes;
                    }
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
                    if state.pending_verdict.is_some() {
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
                        self.finish_pending_recovery(vset_id, out);
                    }
                    self.replica_head_write_step(vset_id, out);
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

    pub(super) fn finish_pending_recovery(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(verdict) = state.pending_verdict.take() else {
            return;
        };
        state.ready = true;
        let resumed = matches!(verdict, crate::seam::Verdict::Resume { .. });
        out.push(Effect::Admin(crate::seam::AdminReply::VsetRecovered {
            vset,
            verdict,
        }));
        if resumed {
            // Record what this resume touches as the next restore's resume
            // set (R6.2).
            self.start_resume_recording(vset, out);
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
            // A passive peer can finish later uploads before the source's
            // earlier head CAS completes. Only reclaim manifests that the
            // authoritative head has actually superseded.
            .filter(|&key| key < keep_manifest)
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
