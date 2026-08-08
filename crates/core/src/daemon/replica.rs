//! Passive replica receiver. It accepts frames only when deterministic
//! placement selects this host for the source/vset assignment, verifies every
//! immutable artifact before issuing an append, and acknowledges only after
//! the append completion event proves durability.

use std::collections::{BTreeMap, BTreeSet};

use super::{Daemon, Pending, ReplicaKey, ReplicaSend, Vset};
use crate::format::crc32c;
use crate::head::{HeadRecord, ManifestPtr, RetiredStash};
use crate::layout;
use crate::mapleaf::{LeafPtr, MapLeaf};
use crate::placement::rank_stash_candidates;
use crate::replica_spool::{
    seal_replica_commit, seal_verified_replica_artifact, verify_replica_artifact,
};
use crate::seam::{Effect, IoId, PeerMsg, ReplicaArtifact, ReplicaCommitInfo, StoreFault, TimerId};
use crate::segment::{SegmentBatchBuilder, open_entry};
use crate::types::{HostId, PageId, SegId, VsetId};

/// A generation is allowed to exceed this only when one verified frame is
/// itself larger. Rotation happens before the append and therefore never
/// copies live data.
pub(super) const MAX_REPLICA_SPOOL_GENERATION_BYTES: u64 = 64 * 1024 * 1024;

impl Daemon {
    /// Return one checksum-verified artifact retained in a durable passive
    /// spool on this host. Artifact names are immutable, so any assignment
    /// carrying the exact identity is an equivalent local recovery source.
    pub(super) fn replica_artifact_bytes(
        &self,
        vset: VsetId,
        artifact: ReplicaArtifact,
    ) -> Option<&[u8]> {
        self.replicas
            .iter()
            .filter(|(key, _)| key.vset == vset)
            .filter_map(|(_, replica)| replica.artifacts.get(&artifact))
            .map(|(_, bytes)| bytes.as_slice())
            .next()
    }

    pub(super) fn replica_segment_range(
        &self,
        vset: VsetId,
        loc: crate::segment::PageLoc,
    ) -> Option<Vec<u8>> {
        let artifact = ReplicaArtifact::Segment {
            fence: loc.fence,
            seg: loc.seg,
        };
        let bytes = self.replica_artifact_bytes(vset, artifact)?;
        let start = usize::try_from(loc.offset).ok()?;
        let end = start.checked_add(usize::try_from(loc.len).ok()?)?;
        bytes.get(start..end).map(<[u8]>::to_vec)
    }

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
                record,
            } => self.replica_upload_notice(from, vset, assignment_epoch, info, &record, out),
            PeerMsg::ReplicaArchive {
                vset,
                assignment_epoch,
                through,
            } => self.replica_archive_request(from, vset, assignment_epoch, through, out),
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
        let Ok(frame) = seal_verified_replica_artifact(
            source,
            vset,
            assignment_epoch,
            artifact,
            checksum,
            &bytes,
        ) else {
            self.counters.replica_rejected += 1;
            return;
        };
        self.replica_put_verified(
            source,
            vset,
            assignment_epoch,
            artifact,
            checksum,
            bytes,
            frame,
            out,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replica_put_prepared(
        &mut self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
        frame: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        let Some(frame) = frame else {
            self.counters.replica_rejected += 1;
            return;
        };
        if !self.replica_request_authorized(source, vset, assignment_epoch) {
            self.counters.replica_rejected += 1;
            return;
        }
        self.replica_put_verified(
            source,
            vset,
            assignment_epoch,
            artifact,
            checksum,
            bytes,
            frame,
            out,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn replica_put_verified(
        &mut self,
        source: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
        frame: Vec<u8>,
        out: &mut Vec<Effect>,
    ) {
        let key = ReplicaKey {
            source,
            vset,
            assignment_epoch,
        };
        {
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
        }
        if self.replica_capacity_exhausted(frame.len() as u64) {
            self.counters.replica_capacity_backpressure += 1;
            return;
        }
        let replica = self.replicas.get_mut(&key).expect("created");
        // One ordered append at a time per spool. An overlapping sender retry
        // receives no optimistic ACK and will be retried after completion.
        if replica.append_inflight {
            return;
        }
        let frame_len = frame.len() as u64;
        self.start_replica_append(
            key,
            Pending::ReplicaArtifactAppend {
                source,
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
                frame_len,
            },
            frame,
            out,
        );
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
        let record_checksum = crc32c(record);
        let key = ReplicaKey {
            source,
            vset,
            assignment_epoch,
        };
        {
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
                    if replica.upload_done == Some(info)
                        && let Some(record) = replica.upload_done_record.clone()
                    {
                        out.push(Effect::PeerSend {
                            to: source,
                            msg: PeerMsg::ReplicaUploadDone {
                                vset,
                                assignment_epoch,
                                info,
                                record,
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
        }
        if self.replica_capacity_exhausted(frame.len() as u64) {
            self.counters.replica_capacity_backpressure += 1;
            return;
        }
        let replica = self.replicas.get_mut(&key).expect("created");
        if replica.append_inflight
            || required
                .iter()
                .any(|artifact| !replica.artifacts.contains_key(artifact))
        {
            return;
        }
        let frame_len = frame.len() as u64;
        replica.pending_commit = Some(super::ReplicaPendingCommit {
            info,
            required: required.to_vec(),
            record: record.to_vec(),
        });
        self.start_replica_append(
            key,
            Pending::ReplicaCommitAppend {
                source,
                vset,
                assignment_epoch,
                info,
                record_checksum,
                frame_len,
            },
            frame,
            out,
        );
    }

    fn start_replica_append(
        &mut self,
        key: ReplicaKey,
        pending: Pending,
        frame: Vec<u8>,
        out: &mut Vec<Effect>,
    ) {
        let frame_len = frame.len() as u64;
        let replica = self.replicas.get_mut(&key).expect("validated replica");
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
        self.pending.insert(io, pending);
        self.counters.replica_bytes += frame_len;
        self.counters.replica_rotations += u64::from(rotated);
        out.push(Effect::ReplicaAppend {
            io,
            source: key.source,
            vset: key.vset,
            assignment_epoch: key.assignment_epoch,
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
                replica.committed_required.clone_from(&pending.required);
                replica.committed_record.clone_from(&pending.record);
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
                    derived: BTreeMap::new(),
                    inflight: false,
                };
                let first_pending = replica.upload_queue.is_empty();
                if !first_pending {
                    self.counters.archive_commits_coalesced += 1;
                }
                // The peer spool is the durable timeline. The archive queue
                // retains only the newest immutable cut not already in an
                // active cycle; intermediate manifests add no recovery value.
                if first_pending && replica.upload.is_none() && replica.upload_done.is_none() {
                    replica.unarchived_age = 0;
                } else {
                    replica.upload_queue.clear();
                }
                replica.upload_queue.push_back(upload);
                let key = ReplicaKey {
                    source,
                    vset,
                    assignment_epoch,
                };
                self.replica_archive_schedule(key, out);
                self.replica_compact_maybe(key, out);
                self.replica_superseded_cleanup(key, out);
            }
            Pending::ReplicaCompactAppend {
                key,
                old_through_generation,
                new_generation,
                reclaim_bytes,
                rewritten_bytes,
                retained,
            } => {
                let Some(replica) = self.replicas.get_mut(&key) else {
                    return;
                };
                replica.current_generation = new_generation;
                replica.current_file_bytes = rewritten_bytes;
                replica.stored_bytes = replica.stored_bytes.saturating_add(rewritten_bytes);
                replica.compaction = Some(super::ReplicaCompaction {
                    through_generation: old_through_generation,
                    reclaim_bytes,
                    rewritten_bytes,
                    retained,
                });
                let io = self.io();
                self.pending
                    .insert(io, Pending::ReplicaCompactDelete { key });
                out.push(Effect::ReplicaDelete {
                    io,
                    source: key.source,
                    vset: key.vset,
                    assignment_epoch: key.assignment_epoch,
                    through_generation: old_through_generation,
                });
            }
            Pending::ReplicaCompactDelete { key } => {
                let Some(replica) = self.replicas.get_mut(&key) else {
                    return;
                };
                let compaction = replica.compaction.take().expect("compaction delete state");
                replica.stored_bytes = replica
                    .stored_bytes
                    .saturating_sub(compaction.reclaim_bytes);
                let mut retained = compaction.retained;
                if let Some(upload) = &replica.upload {
                    retained.extend(upload.todo.iter().copied());
                }
                for upload in &replica.upload_queue {
                    retained.extend(upload.todo.iter().copied());
                }
                replica
                    .artifacts
                    .retain(|artifact, _| retained.contains(artifact));
                replica.uncommitted_artifacts.clear();
                replica.append_inflight = false;
                self.counters.replica_cleanup_rewrite_bytes = self
                    .counters
                    .replica_cleanup_rewrite_bytes
                    .saturating_add(compaction.rewritten_bytes);
                self.replica_archive_schedule(key, out);
            }
            Pending::ReplicaSupersededDelete { key } => {
                self.replicas.remove(&key);
                self.replica_timer_generations.remove(&key);
                self.counters.replica_unlinks = self.counters.replica_unlinks.saturating_add(1);
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
                self.replica_archive_schedule(key, out);
            }
            _ => unreachable!("non-replica append completion"),
        }
    }

    pub(super) fn replica_delete_failed(&mut self, io: IoId, out: &mut Vec<Effect>) {
        match self.pending.remove(&io) {
            Some(Pending::ReplicaReleaseDelete {
                source,
                vset,
                assignment_epoch,
                ..
            }) => {
                if let Some(replica) = self.replicas.get_mut(&ReplicaKey {
                    source,
                    vset,
                    assignment_epoch,
                }) {
                    replica.append_inflight = false;
                }
            }
            Some(Pending::ReplicaCompactDelete { key }) => {
                self.replica_set_upload_timer(key, self.config.backup_retry, out);
            }
            Some(Pending::ReplicaSupersededDelete { key }) => {
                if let Some(replica) = self.replicas.get_mut(&key) {
                    replica.append_inflight = false;
                }
            }
            _ => out.push(Effect::Abort {
                reason: "replica delete failure for unknown io",
            }),
        }
    }

    fn replica_compact_maybe(&mut self, key: ReplicaKey, out: &mut Vec<Effect>) {
        let Some(replica) = self.replicas.get(&key) else {
            return;
        };
        if replica.append_inflight
            || replica.compaction.is_some()
            || !replica.uncommitted_artifacts.is_empty()
            || replica.committed.is_none()
        {
            return;
        }
        let mut bytes = Vec::new();
        let retained: BTreeSet<_> = replica.committed_required.iter().copied().collect();
        for artifact in &replica.committed_required {
            let Some((checksum, artifact_bytes)) = replica.artifacts.get(artifact) else {
                return;
            };
            let Ok(frame) = seal_verified_replica_artifact(
                key.source,
                key.vset,
                key.assignment_epoch,
                *artifact,
                *checksum,
                artifact_bytes,
            ) else {
                return;
            };
            bytes.extend(frame);
        }
        let (info, _) = replica.committed.expect("checked");
        let Ok(commit) = seal_replica_commit(
            key.source,
            key.vset,
            key.assignment_epoch,
            info,
            &replica.committed_required,
            &replica.committed_record,
        ) else {
            return;
        };
        bytes.extend(commit);
        let rewritten_bytes = bytes.len() as u64;
        if replica.stored_bytes <= rewritten_bytes.saturating_mul(2)
            || !self.replica_has_capacity(rewritten_bytes)
        {
            return;
        }
        let Some(new_generation) = replica.current_generation.checked_add(1) else {
            return;
        };
        let old_through_generation = replica.current_generation;
        let reclaim_bytes = replica.stored_bytes;
        self.replicas.get_mut(&key).expect("known").append_inflight = true;
        let io = self.io();
        self.pending.insert(
            io,
            Pending::ReplicaCompactAppend {
                key,
                old_through_generation,
                new_generation,
                reclaim_bytes,
                rewritten_bytes,
                retained,
            },
        );
        self.counters.replica_bytes = self.counters.replica_bytes.saturating_add(rewritten_bytes);
        self.counters.replica_rotations = self.counters.replica_rotations.saturating_add(1);
        out.push(Effect::ReplicaAppend {
            io,
            source: key.source,
            vset: key.vset,
            assignment_epoch: key.assignment_epoch,
            generation: new_generation,
            bytes,
        });
    }

    fn replica_compact_delete_retry(&mut self, key: ReplicaKey, out: &mut Vec<Effect>) {
        let Some(compaction) = self
            .replicas
            .get(&key)
            .and_then(|replica| replica.compaction.as_ref())
        else {
            return;
        };
        let through_generation = compaction.through_generation;
        let io = self.io();
        self.pending
            .insert(io, Pending::ReplicaCompactDelete { key });
        out.push(Effect::ReplicaDelete {
            io,
            source: key.source,
            vset: key.vset,
            assignment_epoch: key.assignment_epoch,
            through_generation,
        });
    }

    fn replica_superseded_cleanup(&mut self, newest: ReplicaKey, out: &mut Vec<Effect>) {
        let stale: Vec<_> = self
            .replicas
            .iter()
            .filter(|(key, replica)| {
                key.source == newest.source
                    && key.vset == newest.vset
                    && key.assignment_epoch < newest.assignment_epoch
                    && !replica.append_inflight
                    && replica.uncommitted_artifacts.is_empty()
            })
            .map(|(key, replica)| (*key, replica.current_generation))
            .collect();
        for (key, through_generation) in stale {
            self.replicas.get_mut(&key).expect("known").append_inflight = true;
            let io = self.io();
            self.pending
                .insert(io, Pending::ReplicaSupersededDelete { key });
            out.push(Effect::ReplicaDelete {
                io,
                source: key.source,
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
                through_generation,
            });
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
        let Ok(epoch_index) = usize::try_from(assignment_epoch - 1) else {
            return false;
        };
        if candidates.is_empty() {
            return false;
        }
        let index = epoch_index % candidates.len();
        candidates
            .get(index)
            .is_some_and(|&host| host == self.config.host)
    }

    fn replica_pending_append_bytes(&self) -> u64 {
        self.pending
            .values()
            .filter_map(|pending| match pending {
                Pending::ReplicaArtifactAppend { frame_len, .. }
                | Pending::ReplicaCommitAppend { frame_len, .. } => Some(*frame_len),
                Pending::ReplicaCompactAppend {
                    rewritten_bytes, ..
                } => Some(*rewritten_bytes),
                _ => None,
            })
            .sum()
    }

    fn replica_spool_bytes(&self) -> u64 {
        let stored: u64 = self
            .replicas
            .values()
            .map(|replica| replica.stored_bytes)
            .sum();
        stored.saturating_add(self.replica_pending_append_bytes())
    }

    fn replica_has_capacity(&self, additional: u64) -> bool {
        self.replica_spool_bytes().saturating_add(additional)
            <= self.config.archive.spool_capacity_bytes
    }

    fn replica_capacity_exhausted(&self, additional: u64) -> bool {
        !self.replica_has_capacity(additional)
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
    pub(super) fn replica_head_write_step(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
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

    /// Begin replication of the newest local recovery record. The peer is
    /// queried first so a primary restart can recover a commit whose ACK was
    /// lost. Replicating every newer record keeps checkpoint and cold-boot
    /// recovery points on the same path as sync durability.
    pub(super) fn maybe_replicate(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        let unpublished_assignment = state
            .replica_assignment_proposal
            .is_some_and(|proposal| state.stash_assignment != Some(proposal.assignment));
        if state.replica_send.is_some()
            || state.replica_assignment_inflight
            || unpublished_assignment
            || !state.ready
        {
            return;
        }
        let Some(record) = state.best_record.clone() else {
            return;
        };
        let info = Self::commit_info(&record);
        if state.peer_committed == Some(info)
            || state.backed.is_some_and(|ptr| {
                (ptr.fence, ptr.seq, ptr.capture_seq)
                    == (record.fence, record.seq, record.capture_seq)
            })
        {
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
        let local_segments: BTreeSet<_> = state
            .seg_blobs
            .iter()
            .map(|&(fence, seg, _)| (fence, seg))
            .collect();
        let mut artifacts: Vec<ReplicaArtifact> =
            record
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
                        local_segments.contains(&(*fence, *seg))
                            && !state.backed_segs.contains(&(*fence, *seg))
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
                                state.leaf_blobs.keys().any(|ptr| {
                                    ptr.base == 0 && (ptr.fence, ptr.id) == (*fence, *id)
                                }) && !state.backed_leaves.contains(&(*fence, *id))
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
        if verify_replica_artifact(vset, artifact, &bytes).is_err() {
            // Damaged bytes never enter the passive. Fail only this recovery
            // domain, as a demand read would; unrelated vsets keep running.
            let failed_page = self.vsets[&vset].page_locs.keys().next().copied();
            if let Some(page) = failed_page {
                let state = self.vsets.get_mut(&vset).expect("known");
                state.ready = false;
                state.replica_send = None;
                self.counters.faults_unservable += 1;
                out.push(Effect::VsetUnservable { page });
            } else {
                out.push(Effect::Abort {
                    reason: "replica source artifact corrupt without an owning page",
                });
            }
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
        self.accept_replica_commit(vset, info, Some(committed_record), out);
        self.maybe_request_migration_archive(from, vset, assignment_epoch, info, out);
        self.maybe_finish_backed_migration(vset, out);
    }

    fn maybe_request_migration_archive(
        &self,
        peer: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        out: &mut Vec<Effect>,
    ) {
        let required = self.vsets.get(&vset).is_some_and(|state| {
            state
                .migrate
                .as_ref()
                .and_then(|migration| migration.record.as_ref())
                .is_some_and(|record| Self::commit_info(record) == info)
        });
        if required {
            out.push(Effect::PeerSend {
                to: peer,
                msg: PeerMsg::ReplicaArchive {
                    vset,
                    assignment_epoch,
                    through: info,
                },
            });
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
            let state = &self.vsets[&vset];
            let committed_record = (known == wanted)
                .then(|| state.replica_send.as_ref().expect("sending").record.clone())
                .or_else(|| {
                    state
                        .best_record
                        .as_ref()
                        .filter(|record| Self::commit_info(record) == known)
                        .cloned()
                });
            self.accept_replica_commit(vset, known, committed_record, out);
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

    fn accept_replica_commit(
        &mut self,
        vset: VsetId,
        info: ReplicaCommitInfo,
        record: Option<crate::journal::JournalRecord>,
        out: &mut Vec<Effect>,
    ) {
        let transitioning = self.vsets[&vset]
            .stash_assignment
            .is_some_and(|assignment| assignment.transition_peer.is_some());
        let state = self.vsets.get_mut(&vset).expect("known");
        state.peer_committed = Some(info);
        state.peer_committed_record = record;
        state.replica_send = None;
        if transitioning {
            self.start_replica_activation(vset, info, out);
            return;
        }
        let state = self.vsets.get_mut(&vset).expect("known");
        state.sync_ack_through = state.sync_ack_through.max(info.sync_covered_through);
        self.drain_sync_acks(vset, out);
        self.cleanup(vset, out);
        self.maybe_replicate(vset, out);
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
            // A two-host placement has no alternate candidate. Rebinding is
            // therefore impossible, but transient loss must not strand the
            // sole durable path: keep retrying the authorized peer.
            if self
                .vsets
                .get(&vset)
                .and_then(|state| state.replica_send.as_ref())
                .is_some()
            {
                self.vsets
                    .get_mut(&vset)
                    .expect("known")
                    .replica_send
                    .as_mut()
                    .expect("checked")
                    .retries = 0;
                self.replica_send_message(vset, target, msg, out);
            }
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
        if candidates.len() < 2 {
            return;
        }
        let mut next_epoch = current.assignment_epoch;
        let mut next = None;
        for _ in 0..candidates.len() {
            let Some(epoch) = next_epoch.checked_add(1) else {
                return;
            };
            next_epoch = epoch;
            let Ok(epoch_index) = usize::try_from(next_epoch - 1) else {
                return;
            };
            let candidate = candidates[epoch_index % candidates.len()];
            if candidate != current.active_peer && current.transition_peer != Some(candidate) {
                next = Some(candidate);
                break;
            }
        }
        let Some(next) = next else {
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
        state.replica_assignment_proposal = Some(super::ReplicaAssignmentProposal {
            assignment: proposal,
            activation: None,
        });
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
        let retired = RetiredStash {
            peer: current.active_peer,
            assignment_epoch: current.active_assignment_epoch,
            through: info,
        };
        let state = self.vsets.get_mut(&vset).expect("known");
        state.replica_assignment_proposal = Some(super::ReplicaAssignmentProposal {
            assignment: proposal,
            activation: Some(super::ReplicaActivation { retired, info }),
        });
        self.issue_replica_assignment_cas(vset, out);
    }

    fn issue_replica_assignment_cas(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if Self::replica_head_write_busy(state) {
            return;
        }
        let Some(proposal) = state.replica_assignment_proposal else {
            return;
        };
        let assignment = proposal.assignment;
        let Some(expected) = state.head_version else {
            return;
        };
        // Once the replacement has durably committed `info`, it is a complete
        // recovery source. Older retired copies are redundant under the
        // single-node-loss contract and must never form a finite failover
        // budget when their dead hosts cannot acknowledge cleanup.
        let retired_stashes = proposal
            .activation
            .map(|activation| activation.retired)
            .into_iter()
            .collect();
        let head = HeadRecord {
            vset,
            holder: self.config.host,
            fence: state.fence,
            manifest: state.backed,
            stash: Some(assignment),
            retired_stashes,
        };
        let io = self.io();
        let pending = if let Some(activation) = proposal.activation {
            Pending::ReplicaActivateCas {
                vset,
                assignment,
                retired: activation.retired,
                info: activation.info,
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
            let bytes = upload
                .derived
                .get(&artifact)
                .or_else(|| replica.artifacts.get(&artifact).map(|(_, bytes)| bytes));
            let Some(bytes) = bytes else {
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

    fn replica_archive_request(
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
            return;
        };
        let covered = replica.committed.is_some_and(|(known, _)| {
            (known.writer_fence, known.seq, known.sync_covered_through)
                >= (
                    through.writer_fence,
                    through.seq,
                    through.sync_covered_through,
                )
        });
        if !covered {
            return;
        }
        replica.archive_urgent = true;
        self.replica_archive_start(key, out);
    }

    pub(super) fn replica_archive_schedule(&mut self, key: ReplicaKey, out: &mut Vec<Effect>) {
        let Some(replica) = self.replicas.get(&key) else {
            return;
        };
        if replica.upload.is_some() {
            return;
        }
        if replica.upload_queue.is_empty() {
            // Uploaded objects are not an archived frontier. Until the
            // primary's head CAS is proven by ReplicaRelease, keep measuring
            // the cut's age; this is observability, never admission control.
            if replica.upload_done.is_some() && !replica.archive_timer_armed {
                self.replicas
                    .get_mut(&key)
                    .expect("known")
                    .archive_timer_armed = true;
                self.replica_set_upload_timer(key, self.config.archive.interval.max(1), out);
            }
            return;
        }
        let unpublished_bytes = replica.upload_queue.back().map_or(0, |upload| {
            upload.record.len() as u64
                + upload
                    .todo
                    .iter()
                    .filter_map(|artifact| replica.artifacts.get(artifact))
                    .map(|(_, bytes)| bytes.len() as u64)
                    .sum::<u64>()
        });
        let pressure_at = self
            .config
            .archive
            .spool_capacity_bytes
            .saturating_sub(self.config.archive.spool_headroom_bytes);
        let urgent = unpublished_bytes >= self.config.archive.max_unpublished_bytes
            || self.replica_spool_bytes() >= pressure_at;
        if urgent {
            self.replica_archive_start(key, out);
            return;
        }
        let arm = !self.replicas[&key].archive_timer_armed;
        if arm {
            self.replicas
                .get_mut(&key)
                .expect("known")
                .archive_timer_armed = true;
            self.replica_set_upload_timer(key, self.config.archive.interval.max(1), out);
        }
    }

    fn replica_archive_start(&mut self, key: ReplicaKey, out: &mut Vec<Effect>) {
        let Some(replica) = self.replicas.get(&key) else {
            return;
        };
        if replica.upload.is_some() || replica.upload_queue.is_empty() {
            return;
        }
        // Starting an archive consumes any cadence/fallback timer that led to
        // it. Invalidate that generation so a delayed timer cannot start a
        // second, premature cycle against this replica (or a recreated one).
        let generation = self.replica_timer_generations.entry(key).or_default();
        *generation = generation.wrapping_add(1).max(1);
        let generation = *generation;
        let replica = self.replicas.get_mut(&key).expect("known replica");
        replica.archive_timer_generation = generation;
        let Some(newest) = replica.upload_queue.pop_back() else {
            return;
        };
        replica.upload_queue.clear();
        replica.archive_timer_armed = false;
        replica.archive_urgent = false;
        replica.upload = Some(Self::pack_replica_upload(key.vset, replica, newest));
        self.counters.archive_cycles += 1;
        self.replica_upload_step(key, out);
    }

    /// Rewrite the selected peer-committed cut into archive-sized objects.
    ///
    /// This runs entirely from the passive spool: the source record remains
    /// byte-for-byte untouched and guest capture never participates. Only
    /// locally staged, not-yet-uploaded segments are victims; already
    /// published and base-owned entries remain referenced in place. A crash
    /// before publication recovers the original exact commit from the spool
    /// and deterministically derives the same candidate again.
    #[allow(clippy::too_many_lines)]
    pub(super) fn pack_replica_upload(
        vset: VsetId,
        replica: &super::PassiveReplica,
        upload: super::ReplicaUpload,
    ) -> super::ReplicaUpload {
        let victims: BTreeSet<(u64, SegId)> = upload
            .todo
            .iter()
            .filter_map(|artifact| match artifact {
                ReplicaArtifact::Segment { fence, seg } => Some((*fence, *seg)),
                ReplicaArtifact::Leaf { .. } => None,
            })
            .collect();
        if victims.is_empty() {
            return upload;
        }
        let Ok(mut record) = crate::journal::JournalRecord::decode(vset, &upload.record) else {
            return upload;
        };

        let mut leaves = BTreeMap::new();
        for (&span, &ptr) in &record.leaves {
            if ptr.base != 0 {
                continue;
            }
            let artifact = ReplicaArtifact::Leaf {
                fence: ptr.fence,
                id: ptr.id,
            };
            let Some((_, bytes)) = replica.artifacts.get(&artifact) else {
                continue;
            };
            let Ok(leaf) = MapLeaf::decode(vset, ptr.fence, ptr.id, bytes) else {
                return upload;
            };
            leaves.insert(span, (ptr, leaf));
        }

        // Overlay wins over leaves, matching normal map lookup. Therefore a
        // shadowed leaf entry is dead and must not be carried into the pack.
        let mut live = BTreeMap::new();
        for (_, leaf) in leaves.values() {
            for &(idx, page, generation, loc) in &leaf.entries {
                live.insert(
                    PageId {
                        volume: crate::types::VolumeId { vset, idx },
                        page,
                    },
                    (generation, loc),
                );
            }
        }
        for (&page, &(generation, loc)) in &record.overlay {
            live.insert(page, (generation, loc));
        }

        let mut pages = Vec::new();
        for (&page, &(generation, loc)) in &live {
            if loc.base != 0 || !victims.contains(&(loc.fence, loc.seg)) {
                continue;
            }
            let artifact = ReplicaArtifact::Segment {
                fence: loc.fence,
                seg: loc.seg,
            };
            let Some((_, bytes)) = replica.artifacts.get(&artifact) else {
                return upload;
            };
            let Ok(start) = usize::try_from(loc.offset) else {
                return upload;
            };
            let Some(end) = start.checked_add(loc.len as usize) else {
                return upload;
            };
            let Some(entry) = bytes.get(start..end) else {
                return upload;
            };
            let Ok((found_page, found_generation, raw)) = open_entry(vset, entry) else {
                return upload;
            };
            if (found_page, found_generation) != (page, generation) {
                return upload;
            }
            pages.push((page, generation, raw));
        }
        if pages.is_empty() {
            return upload;
        }

        // Archive objects occupy a deterministic namespace distinct from
        // source capture identifiers. The selected record sequence makes
        // each cut unique, including metadata-only cuts with no new source
        // segment id.
        let archive_fence = u64::MAX.saturating_sub(record.seq.0);
        let mut builder = SegmentBatchBuilder::new(vset, archive_fence, SegId(0));
        for (page, generation, raw) in pages {
            builder.add(page, generation, &raw);
        }
        let packed = builder.finish();
        let mut replacements = BTreeMap::new();
        let mut new_artifacts = Vec::new();
        let mut derived = BTreeMap::new();
        for (seg, bytes, entries) in packed {
            let artifact = ReplicaArtifact::Segment {
                fence: archive_fence,
                seg,
            };
            for (page, generation, loc) in entries {
                replacements.insert(page, (generation, loc));
            }
            new_artifacts.push(artifact);
            derived.insert(artifact, bytes);
        }
        for (page, entry) in &mut record.overlay {
            if let Some(replacement) = replacements.get(page) {
                *entry = *replacement;
            }
        }

        let mut next_leaf = 0;
        let mut replaced_leaves = BTreeSet::new();
        for (span, (old_ptr, mut leaf)) in leaves {
            let mut changed = false;
            for (idx, page, generation, loc) in &mut leaf.entries {
                let page_id = PageId {
                    volume: crate::types::VolumeId { vset, idx: *idx },
                    page: *page,
                };
                if let Some((new_generation, new_loc)) = replacements.get(&page_id) {
                    *generation = *new_generation;
                    *loc = *new_loc;
                    changed = true;
                }
            }
            if !changed {
                continue;
            }
            let ptr = LeafPtr {
                base: 0,
                fence: archive_fence,
                id: next_leaf,
            };
            next_leaf = next_leaf.saturating_add(1);
            let bytes = leaf.encode(vset, ptr.fence, ptr.id);
            let artifact = ReplicaArtifact::Leaf {
                fence: ptr.fence,
                id: ptr.id,
            };
            derived.insert(artifact, bytes);
            new_artifacts.push(artifact);
            replaced_leaves.insert(ReplicaArtifact::Leaf {
                fence: old_ptr.fence,
                id: old_ptr.id,
            });
            record.leaves.insert(span, ptr);
        }

        let mut todo: Vec<_> = upload
            .todo
            .into_iter()
            .filter(|artifact| match artifact {
                ReplicaArtifact::Segment { fence, seg } => !victims.contains(&(*fence, *seg)),
                ReplicaArtifact::Leaf { .. } => !replaced_leaves.contains(artifact),
            })
            .chain(new_artifacts)
            .collect();
        todo.sort_unstable();
        todo.dedup();
        super::ReplicaUpload {
            info: upload.info,
            todo,
            record: record.encode(vset),
            derived,
            inflight: false,
        }
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
                    let record = upload.record.clone();
                    replica.upload = None;
                    replica.upload_done = Some(info);
                    replica.upload_done_record = Some(record.clone());
                    out.push(Effect::PeerSend {
                        to: key.source,
                        msg: PeerMsg::ReplicaUploadDone {
                            vset: key.vset,
                            assignment_epoch: key.assignment_epoch,
                            info,
                            record,
                        },
                    });
                    if replica.archive_urgent {
                        self.replica_archive_start(key, out);
                    } else {
                        self.replica_archive_schedule(key, out);
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
                        state.store_manifests.insert((ptr.fence, ptr.seq));
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
                            .as_ref()
                            .is_some_and(|(_, uploaded, _)| *uploaded == info)
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
                        self.maybe_finish_backed_migration(vset, out);
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
                    Err(StoreFault::CasConflict { .. }) => {
                        if self.vsets[&vset].outbound.is_some() {
                            // The destination may claim the head while the
                            // source's asynchronous archive CAS is in
                            // flight. Ownership transfer is expected here;
                            // the outbound source must retain its local tail
                            // until Released instead of fencing it away.
                            self.vsets.get_mut(&vset).expect("known").peer_upload_done = None;
                        } else {
                            self.fence_vset(vset, out);
                        }
                    }
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
                        if state.replica_assignment_proposal.is_some_and(|proposal| {
                            proposal.assignment == assignment && proposal.activation.is_none()
                        }) {
                            state.replica_assignment_proposal = None;
                        }
                        state.replica_send = None;
                        state.peer_artifacts.clear();
                        state.peer_committed = None;
                        state.peer_committed_record = None;
                        self.finish_pending_recovery(vset, out);
                        self.maybe_replicate(vset, out);
                        self.replica_head_write_step(vset, out);
                    }
                    Err(StoreFault::CasConflict { .. }) => {
                        self.replica_assignment_conflict(vset, out);
                    }
                    Err(StoreFault::Unavailable) => {
                        // The holder's existing writer fence is still the
                        // authority during a global store outage. Install the
                        // transition locally so a replacement can be seeded;
                        // the proposal stays queued for eventual publication.
                        if state.replica_assignment_proposal.is_some_and(|proposal| {
                            proposal.assignment == assignment && proposal.activation.is_none()
                        }) {
                            state.stash_assignment = Some(assignment);
                            state.replica_send = None;
                            state.peer_artifacts.clear();
                            state.peer_committed = None;
                            state.peer_committed_record = None;
                            self.maybe_replicate(vset, out);
                        } else {
                            self.replica_head_write_step(vset, out);
                        }
                        self.backup_backoff(vset, out);
                    }
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
                        state.retired_stashes.clear();
                        state.retired_stashes.push(retired);
                        if state.replica_assignment_proposal.is_some_and(|proposal| {
                            proposal.assignment == assignment
                                && proposal.activation.is_some_and(|activation| {
                                    activation.retired == retired && activation.info == info
                                })
                        }) {
                            state.replica_assignment_proposal = None;
                        }
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
                    Err(StoreFault::Unavailable) => {
                        // A complete passive commit is the durability proof.
                        // Do not turn object-store availability into an
                        // admission-control dependency: activate locally and
                        // reconcile the head asynchronously.
                        state.stash_assignment = Some(assignment);
                        state.replica_send = None;
                        state.retired_stashes.clear();
                        state.retired_stashes.push(retired);
                        state.peer_committed = Some(info);
                        state.sync_ack_through =
                            state.sync_ack_through.max(info.sync_covered_through);
                        self.drain_sync_acks(vset, out);
                        self.cleanup(vset, out);
                        self.maybe_replicate(vset, out);
                        self.backup_backoff(vset, out);
                    }
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
        self.replica_set_upload_timer(key, self.config.backup_retry, out);
    }

    fn replica_set_upload_timer(&mut self, key: ReplicaKey, after: u64, out: &mut Vec<Effect>) {
        let Some(replica) = self.replicas.get_mut(&key) else {
            return;
        };
        let generation = self.replica_timer_generations.entry(key).or_default();
        *generation = generation.saturating_add(1);
        replica.archive_timer_generation = *generation;
        let generation = *generation;
        out.push(Effect::SetTimer {
            timer: TimerId::ReplicaUpload {
                source: key.source,
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
                generation,
            },
            after,
        });
    }

    fn replica_assignment_conflict(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        state.replica_assignment_inflight = false;
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
        generation: u64,
        out: &mut Vec<Effect>,
    ) {
        let key = ReplicaKey {
            source,
            vset,
            assignment_epoch,
        };
        if self.replica_timer_generations.get(&key).copied() != Some(generation)
            || !self.replicas.contains_key(&key)
        {
            return;
        }
        if self.replicas[&key].compaction.is_some() {
            self.replica_compact_delete_retry(key, out);
            return;
        }
        let cadence = self
            .replicas
            .get(&key)
            .is_some_and(|replica| replica.archive_timer_armed);
        if let Some(replica) = self.replicas.get_mut(&key) {
            replica.archive_timer_armed = false;
            replica.unarchived_age = replica.unarchived_age.saturating_add(if cadence {
                self.config.archive.interval.max(1)
            } else {
                self.config.backup_retry
            });
        }
        if self
            .replicas
            .get(&key)
            .is_some_and(|replica| replica.upload.is_some())
        {
            self.replica_upload_step(key, out);
        } else {
            self.replica_archive_start(key, out);
            self.replica_archive_schedule(key, out);
        }
    }

    fn replica_upload_notice(
        &mut self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        record: &[u8],
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
        let Ok(record) = crate::journal::JournalRecord::decode(vset, record) else {
            self.counters.replica_rejected += 1;
            return;
        };
        if Self::commit_info(&record) != info {
            self.counters.replica_rejected += 1;
            return;
        }
        state.store_manifests.insert((info.writer_fence, info.seq));
        state.peer_upload_done = Some((assignment_epoch, info, record));
        self.maybe_peer_head_publish(vset, out);
    }

    pub(super) fn maybe_peer_head_publish(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        if Self::replica_head_write_busy(state) {
            return;
        }
        let Some((assignment_epoch, info, record)) = state.peer_upload_done.clone() else {
            return;
        };
        let Some(assignment) = state.stash_assignment else {
            return;
        };
        if assignment.assignment_epoch != assignment_epoch || state.head_version.is_none() {
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
