//! Passive replica receiver. It accepts frames only when deterministic
//! placement selects this host for the source/vset assignment, verifies every
//! immutable artifact before issuing an append, and acknowledges only after
//! the append completion event proves durability.

use super::{Daemon, Pending, ReplicaKey, ReplicaSend, Vset};
use crate::format::crc32c;
use crate::head::{HeadRecord, MAX_RETIRED_STASHES, ManifestPtr, RetiredStash};
use crate::layout;
use crate::placement::rank_stash_candidates;
use crate::replica_spool::{seal_replica_artifact, seal_replica_commit};
use crate::seam::{Effect, IoId, PeerMsg, ReplicaArtifact, ReplicaCommitInfo, StoreFault, TimerId};
use crate::types::{HostId, VsetId};

pub(super) const MAX_REPLICA_SOURCE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_REPLICA_TOTAL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// A generation is allowed to exceed this only when one verified frame is
/// itself larger. Rotation happens before the append and therefore never
/// copies live data.
pub(super) const MAX_REPLICA_SPOOL_GENERATION_BYTES: u64 = 64 * 1024 * 1024;

impl Daemon {
    pub(super) fn replica_peer(&mut self, from: HostId, msg: PeerMsg, out: &mut Vec<Effect>) {
        match msg {
            PeerMsg::ReplicaPut {
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
            } => self.replica_put(from, vset, assignment_epoch, artifact, checksum, bytes, out),
            PeerMsg::ReplicaCommit {
                vset,
                assignment_epoch,
                info,
                required,
                record,
            } => self.replica_commit(from, vset, assignment_epoch, info, &required, &record, out),
            PeerMsg::ReplicaStatus {
                vset,
                assignment_epoch,
            } => {
                if !self.replica_request_authorized(from, vset, assignment_epoch) {
                    self.counters.replica_rejected += 1;
                    return;
                }
                let key = ReplicaKey {
                    source: from,
                    vset,
                    assignment_epoch,
                };
                let committed = self
                    .replicas
                    .get(&key)
                    .and_then(|replica| replica.committed.map(|(info, _)| info));
                out.push(Effect::PeerSend {
                    to: from,
                    msg: PeerMsg::ReplicaStatusReply {
                        vset,
                        assignment_epoch,
                        committed,
                    },
                });
            }
            PeerMsg::ReplicaPutAck {
                vset,
                assignment_epoch,
                artifact,
                checksum,
            } => self.replica_put_ack(from, vset, assignment_epoch, artifact, checksum, out),
            PeerMsg::ReplicaCommitAck {
                vset,
                assignment_epoch,
                info,
            } => self.replica_commit_ack(from, vset, assignment_epoch, info, out),
            PeerMsg::ReplicaStatusReply {
                vset,
                assignment_epoch,
                committed,
            } => self.replica_status_reply(from, vset, assignment_epoch, committed, out),
            // Upload and release are consumed by the store-publication state
            // machine; they confer no sync authority by themselves.
            PeerMsg::ReplicaUploadDone {
                vset,
                assignment_epoch,
                info,
            } => self.replica_upload_notice(from, vset, assignment_epoch, info, out),
            PeerMsg::ReplicaReleaseAck {
                vset,
                assignment_epoch,
                through,
            } => self.replica_release_ack(from, vset, assignment_epoch, through, out),
            PeerMsg::ReplicaRelease {
                vset,
                assignment_epoch,
                through,
            } => self.replica_release(from, vset, assignment_epoch, through, out),
            _ => unreachable!("non-replica message routed to replica receiver"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn replica_put(
        &mut self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
        out: &mut Vec<Effect>,
    ) {
        if !self.replica_request_authorized(source, vset, assignment_epoch)
            || crc32c(&bytes) != checksum
        {
            self.counters.replica_rejected += 1;
            return;
        }
        let Ok(frame) = seal_replica_artifact(source, vset, assignment_epoch, artifact, &bytes)
        else {
            self.counters.replica_rejected += 1;
            return;
        };
        if !self.replica_has_capacity(source, frame.len() as u64) {
            return;
        }
        let key = ReplicaKey {
            source,
            vset,
            assignment_epoch,
        };
        let replica = self.replicas.entry(key).or_default();
        match replica.artifacts.get(&artifact) {
            Some((known, _)) if *known == checksum => {
                out.push(Effect::PeerSend {
                    to: source,
                    msg: PeerMsg::ReplicaPutAck {
                        vset,
                        assignment_epoch,
                        artifact,
                        checksum,
                    },
                });
                return;
            }
            Some(_) => {
                self.counters.replica_rejected += 1;
                return;
            }
            None => {}
        }
        // One ordered append at a time per spool. An overlapping sender retry
        // receives no optimistic ACK and will be retried after completion.
        if replica.append_inflight {
            return;
        }
        let frame_len = frame.len() as u64;
        let rotated = replica.current_file_bytes != 0
            && replica.current_file_bytes.saturating_add(frame_len)
                > MAX_REPLICA_SPOOL_GENERATION_BYTES;
        if rotated {
            replica.current_generation = replica.current_generation.saturating_add(1);
            replica.current_file_bytes = 0;
        }
        let generation = replica.current_generation;
        replica.append_inflight = true;
        let io = self.io();
        self.pending.insert(
            io,
            Pending::ReplicaArtifactAppend {
                source,
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
                frame_len,
            },
        );
        self.counters.replica_bytes += frame_len;
        self.counters.replica_rotations += u64::from(rotated);
        out.push(Effect::ReplicaAppend {
            io,
            source,
            vset,
            assignment_epoch,
            generation,
            bytes: frame,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn replica_commit(
        &mut self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        required: &[ReplicaArtifact],
        record: &[u8],
        out: &mut Vec<Effect>,
    ) {
        if !self.replica_request_authorized(source, vset, assignment_epoch) {
            self.counters.replica_rejected += 1;
            return;
        }
        let Ok(frame) = seal_replica_commit(source, vset, assignment_epoch, info, required, record)
        else {
            self.counters.replica_rejected += 1;
            return;
        };
        if !self.replica_has_capacity(source, frame.len() as u64) {
            return;
        }
        let record_checksum = crc32c(record);
        let key = ReplicaKey {
            source,
            vset,
            assignment_epoch,
        };
        let replica = self.replicas.entry(key).or_default();
        if let Some((known, known_checksum)) = replica.committed {
            if known == info && known_checksum == record_checksum {
                out.push(Effect::PeerSend {
                    to: source,
                    msg: PeerMsg::ReplicaCommitAck {
                        vset,
                        assignment_epoch,
                        info,
                    },
                });
                if replica.upload_done == Some(info) {
                    out.push(Effect::PeerSend {
                        to: source,
                        msg: PeerMsg::ReplicaUploadDone {
                            vset,
                            assignment_epoch,
                            info,
                        },
                    });
                }
                return;
            }
            if (info.writer_fence, info.seq) <= (known.writer_fence, known.seq) {
                self.counters.replica_rejected += 1;
                return;
            }
        }
        if replica.append_inflight
            || required
                .iter()
                .any(|artifact| !replica.artifacts.contains_key(artifact))
        {
            return;
        }
        let frame_len = frame.len() as u64;
        let rotated = replica.current_file_bytes != 0
            && replica.current_file_bytes.saturating_add(frame_len)
                > MAX_REPLICA_SPOOL_GENERATION_BYTES;
        if rotated {
            replica.current_generation = replica.current_generation.saturating_add(1);
            replica.current_file_bytes = 0;
        }
        let generation = replica.current_generation;
        replica.append_inflight = true;
        replica.pending_commit = Some(super::ReplicaPendingCommit {
            info,
            required: required.to_vec(),
            record: record.to_vec(),
        });
        let io = self.io();
        self.pending.insert(
            io,
            Pending::ReplicaCommitAppend {
                source,
                vset,
                assignment_epoch,
                info,
                record_checksum,
                frame_len,
            },
        );
        self.counters.replica_bytes += frame_len;
        self.counters.replica_rotations += u64::from(rotated);
        out.push(Effect::ReplicaAppend {
            io,
            source,
            vset,
            assignment_epoch,
            generation,
            bytes: frame,
        });
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn replica_append_done(&mut self, pending: Pending, out: &mut Vec<Effect>) {
        match pending {
            Pending::ReplicaArtifactAppend {
                source,
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
                frame_len,
            } => {
                let replica = self
                    .replicas
                    .entry(ReplicaKey {
                        source,
                        vset,
                        assignment_epoch,
                    })
                    .or_default();
                replica.append_inflight = false;
                replica.artifacts.insert(artifact, (checksum, bytes));
                replica.uncommitted_artifacts.insert(artifact);
                replica.stored_bytes = replica.stored_bytes.saturating_add(frame_len);
                replica.current_file_bytes = replica.current_file_bytes.saturating_add(frame_len);
                self.counters.replica_artifact_flushes += 1;
                out.push(Effect::PeerSend {
                    to: source,
                    msg: PeerMsg::ReplicaPutAck {
                        vset,
                        assignment_epoch,
                        artifact,
                        checksum,
                    },
                });
            }
            Pending::ReplicaCommitAppend {
                source,
                vset,
                assignment_epoch,
                info,
                record_checksum,
                frame_len,
            } => {
                let replica = self
                    .replicas
                    .entry(ReplicaKey {
                        source,
                        vset,
                        assignment_epoch,
                    })
                    .or_default();
                replica.append_inflight = false;
                replica.committed = Some((info, record_checksum));
                replica.stored_bytes = replica.stored_bytes.saturating_add(frame_len);
                replica.current_file_bytes = replica.current_file_bytes.saturating_add(frame_len);
                self.counters.replica_commit_flushes += 1;
                self.counters.replica_commits += 1;
                out.push(Effect::PeerSend {
                    to: source,
                    msg: PeerMsg::ReplicaCommitAck {
                        vset,
                        assignment_epoch,
                        info,
                    },
                });
                let pending = replica
                    .pending_commit
                    .take()
                    .expect("commit append retains upload material");
                for artifact in &pending.required {
                    replica.uncommitted_artifacts.remove(artifact);
                }
                let todo = pending
                    .required
                    .iter()
                    .copied()
                    .filter(|artifact| !replica.uploaded_artifacts.contains(artifact))
                    .collect();
                let upload = super::ReplicaUpload {
                    info: pending.info,
                    todo,
                    record: pending.record,
                    inflight: false,
                };
                if replica.upload.is_none() {
                    replica.upload = Some(upload);
                } else {
                    // Every commit carries the full closure that is not yet known to be
                    // store-backed.  Once one upload is in flight, only the newest queued
                    // commit can advance the authoritative head; retaining intermediate
                    // manifests would let a slow store grow this in-memory queue without
                    // bound while adding no durability.
                    replica.upload_queue.clear();
                    replica.upload_queue.push_back(upload);
                }
                let key = ReplicaKey {
                    source,
                    vset,
                    assignment_epoch,
                };
                self.replica_upload_step(key, out);
            }
            Pending::ReplicaReleaseDelete {
                source,
                vset,
                assignment_epoch,
                through,
            } => {
                self.replicas.remove(&ReplicaKey {
                    source,
                    vset,
                    assignment_epoch,
                });
                self.counters.replica_unlinks += 1;
                out.push(Effect::PeerSend {
                    to: source,
                    msg: PeerMsg::ReplicaReleaseAck {
                        vset,
                        assignment_epoch,
                        through,
                    },
                });
            }
            Pending::ReplicaTailTruncate { key, generation } => {
                if let Some(replica) = self.replicas.get_mut(&key) {
                    debug_assert_eq!(replica.current_generation, generation);
                    replica.append_inflight = false;
                }
                self.replica_upload_step(key, out);
            }
            _ => unreachable!("non-replica append completion"),
        }
    }

    pub(super) fn replica_delete_failed(&mut self, io: IoId, out: &mut Vec<Effect>) {
        let Some(Pending::ReplicaReleaseDelete {
            source,
            vset,
            assignment_epoch,
            ..
        }) = self.pending.remove(&io)
        else {
            out.push(Effect::Abort {
                reason: "replica delete failure for unknown io",
            });
            return;
        };
        if let Some(replica) = self.replicas.get_mut(&ReplicaKey {
            source,
            vset,
            assignment_epoch,
        }) {
            replica.append_inflight = false;
        }
    }

    pub(super) fn replica_authorized(
        &self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
    ) -> bool {
        if assignment_epoch == 0 {
            return false;
        }
        let Some(placement) = self.config.replica_placement.as_ref() else {
            return false;
        };
        let Some(source_domain) = placement
            .roster
            .iter()
            .find(|candidate| candidate.host == source)
            .map(|candidate| candidate.failure_domain)
        else {
            return false;
        };
        let candidates = rank_stash_candidates(
            placement.membership_epoch,
            source,
            source_domain,
            vset,
            &placement.roster,
        );
        let Ok(index) = usize::try_from(assignment_epoch - 1) else {
            return false;
        };
        candidates
            .get(index)
            .is_some_and(|&host| host == self.config.host)
    }

    fn replica_has_capacity(&self, source: HostId, additional: u64) -> bool {
        let total: u64 = self
            .replicas
            .values()
            .map(|replica| replica.stored_bytes)
            .sum();
        let source_total: u64 = self
            .replicas
            .iter()
            .filter(|(key, _)| key.source == source)
            .map(|(_, replica)| replica.stored_bytes)
            .sum();
        total.saturating_add(additional) <= MAX_REPLICA_TOTAL_BYTES
            && source_total.saturating_add(additional) <= MAX_REPLICA_SOURCE_BYTES
    }

    fn replica_request_authorized(
        &mut self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
    ) -> bool {
        if !self.replica_authorized(source, vset, assignment_epoch) {
            return false;
        }
        let latest = self.replica_latest_epoch.entry((source, vset)).or_default();
        if assignment_epoch < *latest {
            return false;
        }
        *latest = (*latest).max(assignment_epoch);
        true
    }

    fn replica_release(
        &mut self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        through: ReplicaCommitInfo,
        out: &mut Vec<Effect>,
    ) {
        if !self.replica_request_authorized(source, vset, assignment_epoch) {
            self.counters.replica_rejected += 1;
            return;
        }
        let key = ReplicaKey {
            source,
            vset,
            assignment_epoch,
        };
        let Some(replica) = self.replicas.get_mut(&key) else {
            out.push(Effect::PeerSend {
                to: source,
                msg: PeerMsg::ReplicaReleaseAck {
                    vset,
                    assignment_epoch,
                    through,
                },
            });
            return;
        };
        let covered = replica.committed.is_some_and(|(known, _)| {
            (known.writer_fence, known.seq, known.sync_covered_through)
                <= (
                    through.writer_fence,
                    through.seq,
                    through.sync_covered_through,
                )
        }) && replica.upload_done.is_some_and(|uploaded| {
            (
                uploaded.writer_fence,
                uploaded.seq,
                uploaded.sync_covered_through,
            ) <= (
                through.writer_fence,
                through.seq,
                through.sync_covered_through,
            )
        });
        if !covered || replica.append_inflight || !replica.uncommitted_artifacts.is_empty() {
            return;
        }
        let through_generation = replica.current_generation;
        replica.append_inflight = true;
        let io = self.io();
        self.pending.insert(
            io,
            Pending::ReplicaReleaseDelete {
                source,
                vset,
                assignment_epoch,
                through,
            },
        );
        out.push(Effect::ReplicaDelete {
            io,
            source,
            vset,
            assignment_epoch,
            through_generation,
        });
    }

    fn replica_release_ack(
        &mut self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        through: ReplicaCommitInfo,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        if state.replica_release != Some((from, assignment_epoch, through)) {
            return;
        }
        let retired = state.retired_stashes.iter().copied().find(|retired| {
            (retired.peer, retired.assignment_epoch, retired.through)
                == (from, assignment_epoch, through)
        });
        if let Some(retired) = retired {
            self.replica_history_cleanup(vset, retired, out);
        } else {
            state.replica_release = None;
            state.peer_artifacts.clear();
            self.replica_release_retry(vset, out);
        }
    }

    pub(super) fn replica_release_retry(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let state = self.vsets.get_mut(&vset);
        let Some(state) = state else {
            return;
        };
        if state.replica_history_inflight {
            return;
        }
        if state.replica_release.is_none() {
            state.replica_release = state.replica_release_queue.pop_front();
        }
        let Some((target, assignment_epoch, through)) = self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_release)
        else {
            return;
        };
        out.push(Effect::PeerSend {
            to: target,
            msg: PeerMsg::ReplicaRelease {
                vset,
                assignment_epoch,
                through,
            },
        });
        out.push(Effect::SetTimer {
            timer: TimerId::ReplicaRelease(vset),
            after: self.config.backup_retry,
        });
    }

    pub(super) fn queue_replica_release(
        &mut self,
        vset: VsetId,
        release: (HostId, u64, ReplicaCommitInfo),
    ) {
        let state = self.vsets.get_mut(&vset).expect("known vset");
        let rank =
            |info: ReplicaCommitInfo| (info.writer_fence, info.seq, info.sync_covered_through);
        if let Some(current) = state.replica_release.as_mut()
            && (current.0, current.1) == (release.0, release.1)
        {
            if rank(release.2) > rank(current.2) {
                current.2 = release.2;
            }
            return;
        }
        if let Some(queued) = state
            .replica_release_queue
            .iter_mut()
            .find(|queued| (queued.0, queued.1) == (release.0, release.1))
        {
            if rank(release.2) > rank(queued.2) {
                queued.2 = release.2;
            }
            return;
        }
        if state.replica_release != Some(release) && !state.replica_release_queue.contains(&release)
        {
            state.replica_release_queue.push_back(release);
        }
    }

    fn replica_history_cleanup(
        &mut self,
        vset: VsetId,
        removed: RetiredStash,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if Self::replica_head_write_busy(state) {
            return;
        }
        let Some(expected) = state.head_version else {
            return;
        };
        let retired_stashes = state
            .retired_stashes
            .iter()
            .copied()
            .filter(|entry| *entry != removed)
            .collect();
        let head = HeadRecord {
            vset,
            holder: self.config.host,
            fence: state.fence,
            manifest: state.backed,
            stash: state.stash_assignment,
            retired_stashes,
        };
        let io = self.io();
        self.pending
            .insert(io, Pending::ReplicaHistoryCas { vset, removed });
        self.vsets
            .get_mut(&vset)
            .expect("known")
            .replica_history_inflight = true;
        out.push(Effect::StoreCas {
            io,
            key: layout::head_key(vset),
            expected: Some(expected),
            bytes: head.encode(),
        });
    }

    fn replica_head_write_busy(state: &super::Vset) -> bool {
        state.replica_head_inflight
            || state.replica_assignment_inflight
            || state.replica_history_inflight
            || state.head_refreshing
    }

    /// Start at most one queued mutation of the authoritative per-vset head.
    /// Every caller that completes a head CAS runs this again so work blocked
    /// behind another mutation does not have to wait for a retry timer.
    fn replica_head_write_step(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if Self::replica_head_write_busy(state) {
            return;
        }
        if state.replica_assignment_proposal.is_some() {
            self.issue_replica_assignment_cas(vset, out);
            return;
        }
        if state.peer_upload_done.is_some() && state.peer_committed_record.is_some() {
            self.maybe_peer_head_publish(vset, out);
            if self
                .vsets
                .get(&vset)
                .is_some_and(Self::replica_head_write_busy)
            {
                return;
            }
        }
        let retired = self.vsets.get(&vset).and_then(|state| {
            let (peer, assignment_epoch, through) = state.replica_release?;
            state.retired_stashes.iter().copied().find(|retired| {
                (retired.peer, retired.assignment_epoch, retired.through)
                    == (peer, assignment_epoch, through)
            })
        });
        if let Some(retired) = retired {
            self.replica_history_cleanup(vset, retired, out);
        }
    }

    /// Begin replication of the newest locally durable sync point, if one is
    /// waiting for peer durability. The peer is queried first so a primary
    /// restart can recover an ACK that was lost after the remote commit.
    pub(super) fn maybe_replicate(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if !state.config.durability.requires_peer_sync()
            || state.replica_send.is_some()
            || state.replica_assignment_inflight
            || state.replica_assignment_proposal.is_some()
        {
            return;
        }
        let Some(record) = state.best_record.clone() else {
            return;
        };
        if !state.pending_syncs.iter().any(|&(_, barrier)| {
            barrier > state.sync_ack_through && barrier <= record.sync_covered_through
        }) {
            return;
        }
        let Some(assignment) = state.stash_assignment else {
            return;
        };
        let required = Self::replica_closure(state, &record);
        let todo = required
            .iter()
            .copied()
            .filter(|artifact| !state.peer_artifacts.contains(artifact))
            .collect();
        let status = PeerMsg::ReplicaStatus {
            vset,
            assignment_epoch: assignment.assignment_epoch,
        };
        let target = assignment.transition_peer.unwrap_or(assignment.active_peer);
        self.vsets.get_mut(&vset).expect("known vset").replica_send = Some(ReplicaSend {
            target,
            assignment_epoch: assignment.assignment_epoch,
            record,
            required,
            todo,
            awaiting: Some(status.clone()),
            retries: 0,
            timer_generation: 0,
        });
        self.replica_send_message(vset, target, status, out);
    }

    fn replica_closure(
        state: &Vset,
        record: &crate::journal::JournalRecord,
    ) -> Vec<ReplicaArtifact> {
        let mut artifacts: Vec<ReplicaArtifact> = record
            .overlay
            .values()
            .filter(|(_, loc)| loc.base == 0)
            .map(|(_, loc)| ReplicaArtifact::Segment {
                fence: loc.fence,
                seg: loc.seg,
            })
            .chain(
                record
                    .leaves
                    .values()
                    .filter(|ptr| ptr.base == 0)
                    .filter_map(|ptr| state.leaf_blobs.get(ptr))
                    .flat_map(|(_, segs)| segs.iter().copied())
                    .map(|(fence, seg)| ReplicaArtifact::Segment { fence, seg }),
            )
            .filter(|artifact| match artifact {
                ReplicaArtifact::Segment { fence, seg } => {
                    !state.backed_segs.contains(&(*fence, *seg))
                }
                ReplicaArtifact::Leaf { .. } => unreachable!(),
            })
            .chain(
                record
                    .leaves
                    .values()
                    .filter(|ptr| ptr.base == 0)
                    .map(|ptr| ReplicaArtifact::Leaf {
                        fence: ptr.fence,
                        id: ptr.id,
                    })
                    .filter(|artifact| match artifact {
                        ReplicaArtifact::Leaf { fence, id } => {
                            !state.backed_leaves.contains(&(*fence, *id))
                        }
                        ReplicaArtifact::Segment { .. } => unreachable!(),
                    }),
            )
            .collect();
        artifacts.sort_unstable();
        artifacts.dedup();
        artifacts
    }

    fn replica_send_message(
        &mut self,
        vset: VsetId,
        target: HostId,
        msg: PeerMsg,
        out: &mut Vec<Effect>,
    ) {
        if let PeerMsg::ReplicaPut { bytes, .. } = &msg {
            let len = bytes.len() as u64;
            self.counters.replica_network_bytes += len;
            let selected = self.vsets.get(&vset).and_then(|state| {
                state
                    .stash_assignment
                    .map(|assignment| assignment.transition_peer.unwrap_or(assignment.active_peer))
            });
            if selected != Some(target) {
                self.counters.replica_nonactive_bytes += len;
            }
            if self.vsets.get(&vset).is_some_and(|state| {
                state
                    .stash_assignment
                    .is_some_and(|assignment| assignment.transition_peer == Some(target))
            }) {
                self.counters.replica_replacement_bytes += len;
            }
        }
        let generation = self
            .vsets
            .get_mut(&vset)
            .and_then(|state| state.replica_send.as_mut())
            .map_or(0, |send| {
                send.timer_generation = send.timer_generation.saturating_add(1);
                send.timer_generation
            });
        out.push(Effect::PeerSend { to: target, msg });
        out.push(Effect::SetTimer {
            timer: TimerId::Replica { vset, generation },
            after: self.config.backup_retry,
        });
    }

    fn replica_send_step(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(send) = self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_send.as_ref())
        else {
            return;
        };
        if send.awaiting.is_some() {
            return;
        }
        if let Some(&artifact) = send.todo.last() {
            let name = match artifact {
                ReplicaArtifact::Segment { fence, seg } => layout::segment_blob(vset, fence, seg),
                ReplicaArtifact::Leaf { fence, id } => layout::leaf_blob(vset, fence, id),
            };
            let io = self.io();
            self.pending
                .insert(io, Pending::ReplicaSourceRead { vset, artifact });
            out.push(Effect::BlobRead { io, name });
            return;
        }
        let send = self.vsets[&vset].replica_send.as_ref().expect("checked");
        let info = Self::commit_info(&send.record);
        let msg = PeerMsg::ReplicaCommit {
            vset,
            assignment_epoch: send.assignment_epoch,
            info,
            required: send.required.clone(),
            record: send.record.encode(vset),
        };
        let target = send.target;
        self.vsets
            .get_mut(&vset)
            .expect("known")
            .replica_send
            .as_mut()
            .expect("sending")
            .awaiting = Some(msg.clone());
        self.replica_send_message(vset, target, msg, out);
    }

    pub(super) fn replica_source_read_done(
        &mut self,
        vset: VsetId,
        artifact: ReplicaArtifact,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        let Some(send) = self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_send.as_ref())
        else {
            return;
        };
        if send.awaiting.is_some() || send.todo.last() != Some(&artifact) {
            return;
        }
        let Some(bytes) = bytes else {
            out.push(Effect::Abort {
                reason: "replica source artifact disappeared",
            });
            return;
        };
        // This performs the same identity and checksum verification as the
        // receiver before any damaged local bytes can enter the protocol.
        if seal_replica_artifact(
            self.config.host,
            vset,
            send.assignment_epoch,
            artifact,
            &bytes,
        )
        .is_err()
        {
            out.push(Effect::Abort {
                reason: "replica source artifact corrupt",
            });
            return;
        }
        let msg = PeerMsg::ReplicaPut {
            vset,
            assignment_epoch: send.assignment_epoch,
            artifact,
            checksum: crc32c(&bytes),
            bytes,
        };
        if let PeerMsg::ReplicaPut { bytes, .. } = &msg {
            self.counters.replica_logical_bytes += bytes.len() as u64;
        }
        let target = send.target;
        self.vsets
            .get_mut(&vset)
            .expect("known")
            .replica_send
            .as_mut()
            .expect("sending")
            .awaiting = Some(msg.clone());
        self.replica_send_message(vset, target, msg, out);
    }

    fn replica_put_ack(
        &mut self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        out: &mut Vec<Effect>,
    ) {
        let expected = self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_send.as_ref())
            .and_then(|send| send.awaiting.as_ref())
            .is_some_and(|msg| {
                matches!(msg, PeerMsg::ReplicaPut {
                    assignment_epoch: epoch,
                    artifact: sent,
                    checksum: sum,
                    ..
                } if from == self.vsets[&vset].replica_send.as_ref().expect("sending").target
                    && *epoch == assignment_epoch && *sent == artifact && *sum == checksum)
            });
        if !expected {
            return;
        }
        let state = self.vsets.get_mut(&vset).expect("known");
        state.peer_artifacts.insert(artifact);
        let send = state.replica_send.as_mut().expect("sending");
        send.awaiting = None;
        send.retries = 0;
        if send.todo.last() == Some(&artifact) {
            send.todo.pop();
        }
        self.replica_send_step(vset, out);
    }

    fn replica_commit_ack(
        &mut self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        out: &mut Vec<Effect>,
    ) {
        let Some(send) = self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_send.as_ref())
        else {
            return;
        };
        let expected = from == send.target
            && assignment_epoch == send.assignment_epoch
            && info == Self::commit_info(&send.record)
            && matches!(send.awaiting, Some(PeerMsg::ReplicaCommit { info: sent_info, .. }) if sent_info == info);
        if !expected {
            return;
        }
        let committed_record = send.record.clone();
        let transitioning = self.vsets[&vset]
            .stash_assignment
            .is_some_and(|assignment| assignment.transition_peer.is_some());
        let state = self.vsets.get_mut(&vset).expect("known");
        state.peer_committed = Some(info);
        state.peer_committed_record = Some(committed_record);
        state.replica_send = None;
        if transitioning {
            self.start_replica_activation(vset, info, out);
        } else {
            let state = self.vsets.get_mut(&vset).expect("known");
            state.sync_ack_through = state.sync_ack_through.max(info.sync_covered_through);
            self.drain_sync_acks(vset, out);
            self.cleanup(vset, out);
            self.maybe_replicate(vset, out);
        }
    }

    fn replica_status_reply(
        &mut self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        committed: Option<ReplicaCommitInfo>,
        out: &mut Vec<Effect>,
    ) {
        let Some(send) = self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_send.as_ref())
        else {
            return;
        };
        if from != send.target
            || assignment_epoch != send.assignment_epoch
            || !matches!(send.awaiting, Some(PeerMsg::ReplicaStatus { .. }))
        {
            return;
        }
        let wanted = Self::commit_info(&send.record);
        let covered = committed.is_some_and(|known| {
            known.writer_fence == wanted.writer_fence
                && known.seq >= wanted.seq
                && known.sync_covered_through >= wanted.sync_covered_through
        });
        if covered {
            let known = committed.expect("covered is some");
            let transitioning = self.vsets[&vset]
                .stash_assignment
                .is_some_and(|assignment| assignment.transition_peer.is_some());
            let state = self.vsets.get_mut(&vset).expect("known");
            state.peer_committed = Some(known);
            state.peer_committed_record = (known == wanted)
                .then(|| state.replica_send.as_ref().expect("sending").record.clone())
                .or_else(|| {
                    state
                        .best_record
                        .as_ref()
                        .filter(|record| Self::commit_info(record) == known)
                        .cloned()
                });
            state.replica_send = None;
            if transitioning {
                self.start_replica_activation(vset, known, out);
            } else {
                let state = self.vsets.get_mut(&vset).expect("known");
                state.sync_ack_through = state.sync_ack_through.max(known.sync_covered_through);
                self.drain_sync_acks(vset, out);
                self.cleanup(vset, out);
                self.maybe_replicate(vset, out);
            }
        } else {
            self.vsets
                .get_mut(&vset)
                .expect("known")
                .replica_send
                .as_mut()
                .expect("sending")
                .awaiting = None;
            self.vsets
                .get_mut(&vset)
                .expect("known")
                .replica_send
                .as_mut()
                .expect("sending")
                .retries = 0;
            self.replica_send_step(vset, out);
        }
    }

    pub(super) fn replica_retry(&mut self, vset: VsetId, generation: u64, out: &mut Vec<Effect>) {
        if self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_send.as_ref())
            .is_none_or(|send| send.timer_generation != generation)
        {
            return;
        }
        let Some(send) = self
            .vsets
            .get(&vset)
            .and_then(|state| state.replica_send.as_ref())
        else {
            self.maybe_replicate(vset, out);
            return;
        };
        let Some(msg) = send.awaiting.clone() else {
            self.replica_send_step(vset, out);
            return;
        };
        let target = send.target;
        let retries = {
            let send = self
                .vsets
                .get_mut(&vset)
                .expect("known")
                .replica_send
                .as_mut()
                .expect("sending");
            send.retries = send.retries.saturating_add(1);
            send.retries
        };
        if retries >= 3 {
            self.start_replica_rebind(vset, out);
        } else {
            self.replica_send_message(vset, target, msg, out);
        }
    }

    pub(super) fn replica_tick(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        if self.vsets.get(&vset).is_some_and(|state| {
            state.replica_assignment_proposal.is_some() && !state.replica_assignment_inflight
        }) {
            self.issue_replica_assignment_cas(vset, out);
        } else {
            self.maybe_replicate(vset, out);
        }
    }

    pub(super) fn commit_info(record: &crate::journal::JournalRecord) -> ReplicaCommitInfo {
        ReplicaCommitInfo {
            writer_fence: record.fence,
            seq: record.seq,
            sync_covered_through: record.sync_covered_through,
        }
    }

    fn start_replica_rebind(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if state.replica_assignment_inflight {
            return;
        }
        if state.retired_stashes.len() >= MAX_RETIRED_STASHES {
            return;
        }
        let Some(current) = state.stash_assignment else {
            return;
        };
        let Some(placement) = self.config.replica_placement.as_ref() else {
            return;
        };
        let candidates = rank_stash_candidates(
            placement.membership_epoch,
            self.config.host,
            placement.local_failure_domain,
            vset,
            &placement.roster,
        );
        let next_epoch = current.assignment_epoch.saturating_add(1);
        let Ok(index) = usize::try_from(next_epoch - 1) else {
            return;
        };
        let Some(&next) = candidates.get(index) else {
            return;
        };
        let proposal = crate::head::StashAssignment {
            assignment_epoch: next_epoch,
            active_peer: current.active_peer,
            active_assignment_epoch: current.active_assignment_epoch,
            transition_peer: Some(next),
            membership_epoch: current.membership_epoch,
        };
        let state = self.vsets.get_mut(&vset).expect("known");
        state.replica_send = None;
        state.replica_assignment_proposal = Some((proposal, None));
        self.issue_replica_assignment_cas(vset, out);
    }

    fn start_replica_activation(
        &mut self,
        vset: VsetId,
        info: ReplicaCommitInfo,
        out: &mut Vec<Effect>,
    ) {
        let Some(current) = self.vsets[&vset].stash_assignment else {
            return;
        };
        let Some(next) = current.transition_peer else {
            return;
        };
        let proposal = crate::head::StashAssignment {
            assignment_epoch: current.assignment_epoch,
            active_peer: next,
            active_assignment_epoch: current.assignment_epoch,
            transition_peer: None,
            membership_epoch: current.membership_epoch,
        };
        let state = self.vsets.get_mut(&vset).expect("known");
        state.replica_assignment_proposal = Some((proposal, Some(info)));
        self.issue_replica_assignment_cas(vset, out);
    }

    fn issue_replica_assignment_cas(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if Self::replica_head_write_busy(state) {
            return;
        }
        let Some((assignment, info)) = state.replica_assignment_proposal else {
            return;
        };
        let Some(expected) = state.head_version else {
            return;
        };
        let retired = info.map(|info| RetiredStash {
            peer: state
                .stash_assignment
                .expect("activation has current assignment")
                .active_peer,
            assignment_epoch: state
                .stash_assignment
                .expect("activation has current assignment")
                .active_assignment_epoch,
            through: info,
        });
        let mut retired_stashes = state.retired_stashes.clone();
        if let Some(retired) = retired
            && !retired_stashes.contains(&retired)
        {
            retired_stashes.push(retired);
        }
        let head = HeadRecord {
            vset,
            holder: self.config.host,
            fence: state.fence,
            manifest: state.backed,
            stash: Some(assignment),
            retired_stashes,
        };
        let io = self.io();
        let pending = if let Some(info) = info {
            Pending::ReplicaActivateCas {
                vset,
                assignment,
                retired: retired.expect("activation retires old active"),
                info,
            }
        } else {
            Pending::ReplicaTransitionCas { vset, assignment }
        };
        self.pending.insert(io, pending);
        self.vsets
            .get_mut(&vset)
            .expect("known")
            .replica_assignment_inflight = true;
        out.push(Effect::StoreCas {
            io,
            key: layout::head_key(vset),
            expected: Some(expected),
            bytes: head.encode(),
        });
    }

    pub(super) fn replica_upload_step(&mut self, key: ReplicaKey, out: &mut Vec<Effect>) {
        let Some(replica) = self.replicas.get(&key) else {
            return;
        };
        let Some(upload) = replica.upload.as_ref() else {
            return;
        };
        if upload.inflight {
            return;
        }
        if let Some(&artifact) = upload.todo.last() {
            let Some((_, bytes)) = replica.artifacts.get(&artifact) else {
                return;
            };
            let object_key = match artifact {
                ReplicaArtifact::Segment { fence, seg } => {
                    layout::segment_key(key.vset, fence, seg)
                }
                ReplicaArtifact::Leaf { fence, id } => layout::leaf_key(key.vset, fence, id),
            };
            let bytes = bytes.clone();
            let io = self.io();
            self.pending
                .insert(io, Pending::ReplicaUploadArtifact { key, artifact });
            self.replicas
                .get_mut(&key)
                .expect("known")
                .upload
                .as_mut()
                .expect("uploading")
                .inflight = true;
            self.counters.replica_store_bytes += bytes.len() as u64;
            out.push(Effect::StorePut {
                io,
                key: object_key,
                bytes,
            });
            return;
        }
        let record = upload.record.clone();
        let info = upload.info;
        let io = self.io();
        self.pending
            .insert(io, Pending::ReplicaUploadManifest { key, info });
        self.replicas
            .get_mut(&key)
            .expect("known")
            .upload
            .as_mut()
            .expect("uploading")
            .inflight = true;
        self.counters.replica_store_bytes += record.len() as u64;
        out.push(Effect::StorePut {
            io,
            key: layout::manifest_key(key.vset, info.writer_fence, info.seq),
            bytes: record,
        });
    }

    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    pub(super) fn replica_store_done(
        &mut self,
        pending: Pending,
        result: Result<u64, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        match pending {
            Pending::ReplicaUploadArtifact { key, artifact } => {
                let Some(replica) = self.replicas.get_mut(&key) else {
                    return;
                };
                let Some(upload) = replica.upload.as_mut() else {
                    return;
                };
                upload.inflight = false;
                if result.is_ok() {
                    if upload.todo.last() == Some(&artifact) {
                        upload.todo.pop();
                    }
                    replica.uploaded_artifacts.insert(artifact);
                    self.replica_upload_step(key, out);
                } else {
                    self.replica_upload_backoff(key, out);
                }
            }
            Pending::ReplicaUploadManifest { key, info } => {
                let Some(replica) = self.replicas.get_mut(&key) else {
                    return;
                };
                let Some(upload) = replica.upload.as_mut() else {
                    return;
                };
                upload.inflight = false;
                if result.is_ok() && upload.info == info {
                    replica.upload = None;
                    replica.upload_done = Some(info);
                    out.push(Effect::PeerSend {
                        to: key.source,
                        msg: PeerMsg::ReplicaUploadDone {
                            vset: key.vset,
                            assignment_epoch: key.assignment_epoch,
                            info,
                        },
                    });
                    if let Some(next) = replica.upload_queue.pop_front() {
                        replica.upload = Some(next);
                        self.replica_upload_step(key, out);
                    }
                } else {
                    self.replica_upload_backoff(key, out);
                }
            }
            Pending::ReplicaHeadCas {
                vset,
                info,
                ptr,
                record,
            } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                state.replica_head_inflight = false;
                match result {
                    Ok(version) => {
                        let published_artifacts = Self::replica_closure(state, &record);
                        for artifact in published_artifacts {
                            match artifact {
                                ReplicaArtifact::Segment { fence, seg } => {
                                    state.backed_segs.insert((fence, seg));
                                }
                                ReplicaArtifact::Leaf { fence, id } => {
                                    state.backed_leaves.insert((fence, id));
                                }
                            }
                        }
                        state.head_version = Some(version);
                        state.backed = Some(ptr);
                        state.store_published_through =
                            state.store_published_through.max(info.sync_covered_through);
                        state.sync_ack_through = state
                            .sync_ack_through
                            .max(state.store_published_through)
                            .max(
                                state
                                    .peer_committed
                                    .map_or(0, |known| known.sync_covered_through),
                            );
                        if state
                            .peer_upload_done
                            .is_some_and(|(_, uploaded)| uploaded == info)
                        {
                            state.peer_upload_done = None;
                        }
                        if state
                            .peer_committed_record
                            .as_ref()
                            .is_some_and(|known| Self::commit_info(known) == info)
                        {
                            state.peer_committed_record = None;
                        }
                        self.counters.manifests_published += 1;
                        self.drain_sync_acks(vset, out);
                        self.store_cleanup(vset, out);
                        if let Some(assignment) = self.vsets[&vset].stash_assignment {
                            self.queue_replica_release(
                                vset,
                                (
                                    assignment.active_peer,
                                    assignment.active_assignment_epoch,
                                    info,
                                ),
                            );
                            let retired: Vec<_> = self.vsets[&vset]
                                .retired_stashes
                                .iter()
                                .copied()
                                .filter(|retired| {
                                    (
                                        retired.through.writer_fence,
                                        retired.through.seq,
                                        retired.through.sync_covered_through,
                                    ) <= (info.writer_fence, info.seq, info.sync_covered_through)
                                })
                                .collect();
                            for retired in retired {
                                self.queue_replica_release(
                                    vset,
                                    (retired.peer, retired.assignment_epoch, retired.through),
                                );
                            }
                            self.replica_release_retry(vset, out);
                        }
                        self.replica_head_write_step(vset, out);
                    }
                    Err(StoreFault::CasConflict { .. }) => self.fence_vset(vset, out),
                    Err(StoreFault::Unavailable) => self.backup_backoff(vset, out),
                }
            }
            Pending::ReplicaTransitionCas { vset, assignment } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                state.replica_assignment_inflight = false;
                match result {
                    Ok(version) => {
                        state.head_version = Some(version);
                        state.stash_assignment = Some(assignment);
                        state.replica_assignment_proposal = None;
                        state.replica_send = None;
                        state.peer_artifacts.clear();
                        state.peer_committed = None;
                        state.peer_committed_record = None;
                        self.maybe_replicate(vset, out);
                        self.replica_head_write_step(vset, out);
                    }
                    Err(StoreFault::CasConflict { .. }) => {
                        self.replica_assignment_conflict(vset, out);
                    }
                    Err(StoreFault::Unavailable) => self.backup_backoff(vset, out),
                }
            }
            Pending::ReplicaActivateCas {
                vset,
                assignment,
                retired,
                info,
            } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                state.replica_assignment_inflight = false;
                match result {
                    Ok(version) => {
                        state.head_version = Some(version);
                        state.stash_assignment = Some(assignment);
                        state.replica_send = None;
                        if !state.retired_stashes.contains(&retired) {
                            state.retired_stashes.push(retired);
                        }
                        state.replica_assignment_proposal = None;
                        state.peer_committed = Some(info);
                        state.sync_ack_through =
                            state.sync_ack_through.max(info.sync_covered_through);
                        self.drain_sync_acks(vset, out);
                        self.cleanup(vset, out);
                        self.maybe_replicate(vset, out);
                        self.replica_head_write_step(vset, out);
                    }
                    Err(StoreFault::CasConflict { .. }) => {
                        self.replica_assignment_conflict(vset, out);
                    }
                    Err(StoreFault::Unavailable) => self.backup_backoff(vset, out),
                }
            }
            Pending::ReplicaHistoryCas { vset, removed } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                state.replica_history_inflight = false;
                match result {
                    Ok(version) => {
                        state.head_version = Some(version);
                        state.retired_stashes.retain(|entry| *entry != removed);
                        state.replica_release = None;
                        self.replica_release_retry(vset, out);
                        self.replica_head_write_step(vset, out);
                    }
                    Err(StoreFault::CasConflict { .. }) => {
                        self.replica_assignment_conflict(vset, out);
                    }
                    Err(StoreFault::Unavailable) => {
                        out.push(Effect::SetTimer {
                            timer: TimerId::ReplicaRelease(vset),
                            after: self.config.backup_retry,
                        });
                    }
                }
            }
            _ => unreachable!("non-replica store completion"),
        }
    }

    fn replica_upload_backoff(&mut self, key: ReplicaKey, out: &mut Vec<Effect>) {
        self.counters.store_retries += 1;
        out.push(Effect::SetTimer {
            timer: TimerId::ReplicaUpload {
                source: key.source,
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
            },
            after: self.config.backup_retry,
        });
    }

    fn replica_assignment_conflict(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        state.replica_assignment_inflight = false;
        state.replica_assignment_proposal = None;
        if state.head_refreshing {
            return;
        }
        state.head_refreshing = true;
        let io = self.io();
        self.pending.insert(io, Pending::HeadRefresh { vset });
        out.push(Effect::StoreGet {
            io,
            key: layout::head_key(vset),
        });
    }

    pub(super) fn replica_upload_retry(
        &mut self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        out: &mut Vec<Effect>,
    ) {
        self.replica_upload_step(
            ReplicaKey {
                source,
                vset,
                assignment_epoch,
            },
            out,
        );
    }

    fn replica_upload_notice(
        &mut self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(assignment) = state.stash_assignment else {
            return;
        };
        if assignment.active_peer != from || assignment.assignment_epoch != assignment_epoch {
            self.counters.replica_rejected += 1;
            return;
        }
        state.peer_upload_done = Some((assignment_epoch, info));
        self.maybe_peer_head_publish(vset, out);
    }

    pub(super) fn maybe_peer_head_publish(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if Self::replica_head_write_busy(state) {
            return;
        }
        let Some((assignment_epoch, info)) = state.peer_upload_done else {
            return;
        };
        let Some(assignment) = state.stash_assignment else {
            return;
        };
        let Some(record) = state.peer_committed_record.clone() else {
            return;
        };
        if assignment.assignment_epoch != assignment_epoch
            || Self::commit_info(&record) != info
            || state.head_version.is_none()
        {
            return;
        }
        let ptr = ManifestPtr {
            fence: record.fence,
            seq: record.seq,
            capture_seq: record.capture_seq,
        };
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
        self.pending.insert(
            io,
            Pending::ReplicaHeadCas {
                vset,
                info,
                ptr,
                record,
            },
        );
        self.vsets
            .get_mut(&vset)
            .expect("known")
            .replica_head_inflight = true;
        out.push(Effect::StoreCas {
            io,
            key: layout::head_key(vset),
            expected,
            bytes: head.encode(),
        });
    }
}
