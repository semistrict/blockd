use std::collections::BTreeSet;
use std::rc::Rc;

use blockd_exec::{FaultPoint, delay, fault_point, now, spawn};

use super::backup::claim_new_head_with_stash;
use super::capture::finish_creation;
use super::peer_client::PeerRpcError;
use super::reclaim::reclaim_backed_segments;
use super::state::{PublicationOwner, ReplicaArchiveCut, ReplicaKey, SharedHost};
use crate::format::crc32c;
use crate::head::{HeadRecord, MAX_RETIRED_STASHES, ManifestPtr, RetiredStash, StashAssignment};
use crate::journal::{JournalRecord, VsetConfig};
use crate::layout;
use crate::placement::rank_stash_candidates;
use crate::protocol::{
    AdminError, AdminResult, AdminSuccess, PeerMsg, ReplicaArtifact, ReplicaCommitInfo, StoreFault,
};
use crate::replica_spool::{
    seal_replica_commit, seal_verified_replica_artifact, verify_replica_artifact,
};
use crate::types::{HostId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

/// Rotation happens before an append. A single verified frame may therefore
/// exceed this bound, but existing bytes are never copied between generations.
pub const MAX_REPLICA_SPOOL_GENERATION_BYTES: u64 = 64 * 1024 * 1024;

#[allow(clippy::too_many_lines)]
pub async fn replica_message<W>(state: SharedHost, world: &W, from: HostId, message: PeerMsg)
where
    W: Blobs + Store + Peers + AdminIo,
{
    match message {
        PeerMsg::ReplicaPut {
            vset,
            assignment_epoch,
            artifact,
            checksum,
            bytes,
        } => {
            replica_put(
                &state,
                world,
                from,
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
            )
            .await;
        }
        PeerMsg::ReplicaCommit {
            vset,
            assignment_epoch,
            info,
            required,
            record,
        } => {
            replica_commit(
                &state,
                world,
                from,
                vset,
                assignment_epoch,
                info,
                required,
                record,
            )
            .await;
        }
        PeerMsg::ReplicaStatus {
            vset,
            assignment_epoch,
        } => {
            if !request_authorized(&state, from, vset, assignment_epoch) {
                state.borrow_mut().counters.replica_rejected += 1;
                return;
            }
            let committed = state
                .borrow()
                .replicas
                .get(&ReplicaKey {
                    source: from,
                    vset,
                    assignment_epoch,
                })
                .and_then(|replica| replica.committed);
            Peers::send(
                world,
                from,
                PeerMsg::ReplicaStatusReply {
                    vset,
                    assignment_epoch,
                    committed,
                },
            )
            .await;
        }
        PeerMsg::ReplicaRelease {
            vset,
            assignment_epoch,
            through,
        } => {
            replica_release(&state, world, from, vset, assignment_epoch, through).await;
        }
        PeerMsg::ReplicaPutAck {
            vset,
            assignment_epoch,
            artifact,
            checksum,
        } => {
            state
                .borrow_mut()
                .peer_client
                .resolve_put(from, (from, vset, assignment_epoch, artifact, checksum));
        }
        PeerMsg::ReplicaCommitAck {
            vset,
            assignment_epoch,
            info,
        } => {
            state
                .borrow_mut()
                .peer_client
                .resolve_commit(from, commit_wait_key(from, vset, assignment_epoch, info));
        }
        PeerMsg::ReplicaStatusReply {
            vset,
            assignment_epoch,
            committed,
        } => {
            state
                .borrow_mut()
                .peer_client
                .resolve_status(from, vset, assignment_epoch, committed);
        }
        PeerMsg::ReplicaUploadDone {
            vset,
            assignment_epoch,
            info,
            record,
        } => {
            let decoded = JournalRecord::decode(vset, &record).ok();
            let valid = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
                vset_state.stash_assignment.is_some_and(|stash| {
                    stash.assignment_epoch == assignment_epoch
                        && stash.transition_peer.unwrap_or(stash.active_peer) == from
                }) && decoded
                    .as_ref()
                    .is_some_and(|record| commit_info(record) == info)
            });
            if valid {
                let obsolete = if let (Some(vset_state), Some(record)) =
                    (state.borrow_mut().vsets.get_mut(&vset), decoded)
                {
                    let obsolete = vset_state.backed.is_some_and(|backed| {
                        (record.capture_seq, record.seq) < (backed.capture_seq, backed.seq)
                    });
                    let published = vset_state.backed.is_some_and(|backed| {
                        (record.capture_seq, record.seq) == (backed.capture_seq, backed.seq)
                    });
                    if obsolete {
                        vset_state
                            .store_manifests
                            .remove(&(info.writer_fence, info.seq));
                    } else if !published {
                        vset_state
                            .store_manifests
                            .insert((info.writer_fence, info.seq));
                        retain_latest_upload(vset_state, info, record);
                    }
                    obsolete
                } else {
                    false
                };
                state.borrow_mut().schedule_vset(vset);
                if obsolete {
                    let _ = Store::delete(
                        world,
                        &layout::manifest_key(vset, info.writer_fence, info.seq),
                    )
                    .await;
                    let _ = Store::delete(
                        world,
                        &layout::pending_manifest_key(vset, info.writer_fence, info.seq),
                    )
                    .await;
                }
            } else {
                state.borrow_mut().counters.replica_rejected += 1;
            }
        }
        PeerMsg::ReplicaArchive {
            vset,
            assignment_epoch,
            through,
        } => {
            replica_archive(&state, world, from, vset, assignment_epoch, through).await;
        }
        PeerMsg::ReplicaReleaseAck {
            vset,
            assignment_epoch,
            through,
        } => {
            let retired = {
                let mut host = state.borrow_mut();
                let Some(index) = host
                    .replica_releases
                    .iter()
                    .position(|release| *release == (from, vset, assignment_epoch, through))
                else {
                    return;
                };
                host.replica_releases.swap_remove(index);
                host.vsets.get(&vset).and_then(|vset_state| {
                    vset_state
                        .retired_stashes
                        .iter()
                        .copied()
                        .find(|retired| {
                            (retired.peer, retired.assignment_epoch, retired.through)
                                == (from, assignment_epoch, through)
                        })
                        .map(|retired| {
                            (
                                vset_state.incarnation,
                                vset_state.stash_assignment,
                                retired,
                                vset_state
                                    .retired_stashes
                                    .iter()
                                    .copied()
                                    .filter(|entry| *entry != retired)
                                    .collect::<Vec<_>>(),
                            )
                        })
                })
            };
            if let Some((incarnation, Some(assignment), _retired, remaining)) = retired {
                let _ = cas_assignment(
                    &state,
                    world,
                    vset,
                    incarnation,
                    assignment,
                    remaining,
                    None,
                )
                .await;
            }
        }
        _ => unreachable!("non-replica message"),
    }
}

fn retain_latest_upload(
    vset_state: &mut super::state::VsetState,
    info: ReplicaCommitInfo,
    record: JournalRecord,
) {
    if vset_state
        .peer_upload_done
        .is_none_or(|current| commit_rank(info) >= commit_rank(current))
    {
        vset_state.peer_upload_done = Some(info);
        vset_state.peer_upload_record = Some(record);
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn replica_put<W>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    artifact: ReplicaArtifact,
    checksum: u32,
    bytes: Vec<u8>,
) where
    W: Blobs + Peers + AdminIo,
{
    if !request_authorized(state, source, vset, assignment_epoch) || crc32c(&bytes) != checksum {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    let Ok(frame) =
        seal_verified_replica_artifact(source, vset, assignment_epoch, artifact, checksum, &bytes)
    else {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    };
    let key = ReplicaKey {
        source,
        vset,
        assignment_epoch,
    };
    let known = state.borrow().replicas.get(&key).and_then(|replica| {
        replica
            .artifacts
            .get(&artifact)
            .map(|(checksum, _)| *checksum)
    });
    if let Some(known) = known {
        if known == checksum {
            Peers::send(
                world,
                source,
                PeerMsg::ReplicaPutAck {
                    vset,
                    assignment_epoch,
                    artifact,
                    checksum,
                },
            )
            .await;
        } else {
            state.borrow_mut().counters.replica_rejected += 1;
        }
        return;
    }
    let Some((generation, rotated)) = replica_append_plan(state, key, frame.len() as u64) else {
        state.borrow_mut().counters.replica_capacity_backpressure += 1;
        return;
    };
    let spool_name = layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation);
    if !state
        .borrow_mut()
        .try_reserve_append(spool_name.clone(), frame.len() as u64)
    {
        state.borrow_mut().counters.replica_capacity_backpressure += 1;
        return;
    }
    if Blobs::append(world, spool_name.clone(), frame.clone())
        .await
        .is_err()
    {
        state.borrow_mut().fail("replica artifact append failed");
        return;
    }
    {
        let mut host = state.borrow_mut();
        let replica = host.replicas.entry(key).or_default();
        if rotated {
            replica.current_generation = generation;
            replica.current_file_bytes = 0;
        }
        replica.artifacts.insert(artifact, (checksum, bytes));
        replica.bytes += frame.len() as u64;
        replica.current_file_bytes += frame.len() as u64;
        host.counters.replica_bytes += frame.len() as u64;
        host.counters.replica_rotations += u64::from(rotated);
        host.counters.replica_artifact_flushes += 1;
    }
    if fault_point(FaultPoint::CrashPeerAfterDataFlushBeforeCommit) {
        state.borrow_mut().fail("injected replica crash");
        return;
    }
    Peers::send(
        world,
        source,
        PeerMsg::ReplicaPutAck {
            vset,
            assignment_epoch,
            artifact,
            checksum,
        },
    )
    .await;
    if fault_point(FaultPoint::DuplicateAck) {
        Peers::send(
            world,
            source,
            PeerMsg::ReplicaPutAck {
                vset,
                assignment_epoch,
                artifact,
                checksum,
            },
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn replica_commit<W>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
    required: Vec<ReplicaArtifact>,
    record: Vec<u8>,
) where
    W: Blobs + Store + Peers + AdminIo,
{
    if !request_authorized(state, source, vset, assignment_epoch) {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    let key = ReplicaKey {
        source,
        vset,
        assignment_epoch,
    };
    let complete = required.iter().all(|artifact| {
        state
            .borrow()
            .replicas
            .get(&key)
            .is_some_and(|replica| replica.artifacts.contains_key(artifact))
    });
    let Ok(frame) = seal_replica_commit(source, vset, assignment_epoch, info, &required, &record)
    else {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    };
    if !complete {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    let known = state
        .borrow()
        .replicas
        .get(&key)
        .and_then(|replica| replica.committed);
    if known == Some(info) {
        Peers::send(
            world,
            source,
            PeerMsg::ReplicaCommitAck {
                vset,
                assignment_epoch,
                info,
            },
        )
        .await;
        let uploaded = state
            .borrow()
            .replicas
            .get(&key)
            .and_then(|replica| replica.uploaded)
            == Some(info);
        if uploaded {
            Peers::send(
                world,
                source,
                PeerMsg::ReplicaUploadDone {
                    vset,
                    assignment_epoch,
                    info,
                    record,
                },
            )
            .await;
        } else {
            queue_archive(state, key, info, required, record);
        }
        return;
    }
    if known.is_some_and(|known| commit_rank(info) <= commit_rank(known)) {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    let Some((generation, rotated)) = replica_append_plan(state, key, frame.len() as u64) else {
        state.borrow_mut().counters.replica_capacity_backpressure += 1;
        return;
    };
    let spool_name = layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation);
    if !state
        .borrow_mut()
        .try_reserve_append(spool_name.clone(), frame.len() as u64)
    {
        state.borrow_mut().counters.replica_capacity_backpressure += 1;
        return;
    }
    if Blobs::append(world, spool_name.clone(), frame.clone())
        .await
        .is_err()
    {
        state.borrow_mut().fail("replica commit append failed");
        return;
    }
    {
        let mut host = state.borrow_mut();
        let replica = host.replicas.entry(key).or_default();
        if rotated {
            replica.current_generation = generation;
            replica.current_file_bytes = 0;
        }
        replica.committed = Some(info);
        replica.committed_record = Some(record.clone());
        replica.bytes += frame.len() as u64;
        replica.current_file_bytes += frame.len() as u64;
        host.counters.replica_bytes += frame.len() as u64;
        host.counters.replica_rotations += u64::from(rotated);
        host.counters.replica_commits += 1;
        host.counters.replica_commit_flushes += 1;
    }
    if fault_point(FaultPoint::CrashPeerAfterCommitBeforeAck) {
        state.borrow_mut().fail("injected replica crash");
        return;
    }
    Peers::send(
        world,
        source,
        PeerMsg::ReplicaCommitAck {
            vset,
            assignment_epoch,
            info,
        },
    )
    .await;
    if fault_point(FaultPoint::DuplicateAck) {
        Peers::send(
            world,
            source,
            PeerMsg::ReplicaCommitAck {
                vset,
                assignment_epoch,
                info,
            },
        )
        .await;
    }
    queue_archive(state, key, info, required, record);
}

fn queue_archive(
    state: &SharedHost,
    key: ReplicaKey,
    info: ReplicaCommitInfo,
    required: Vec<ReplicaArtifact>,
    record: Vec<u8>,
) {
    let queued_at = now();
    let mut host = state.borrow_mut();
    let total_spool_bytes = host
        .replicas
        .values()
        .map(|replica| replica.bytes)
        .fold(0u64, u64::saturating_add);
    let archive = host.config.archive;
    let Some(replica) = host.replicas.get_mut(&key) else {
        return;
    };
    let unpublished_bytes = record.len() as u64
        + required
            .iter()
            .filter_map(|artifact| replica.artifacts.get(artifact))
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>();
    let coalesced = replica.archive_pending.is_some();
    if !replica.archive_inflight && replica.archive_pending.is_none() && replica.uploaded.is_none()
    {
        replica.unarchived_age = 0;
    }
    replica.archive_pending = Some(ReplicaArchiveCut {
        info,
        required,
        record,
    });
    let pressure_at = archive
        .spool_capacity_bytes
        .saturating_sub(archive.spool_headroom_bytes);
    let urgent =
        unpublished_bytes >= archive.max_unpublished_bytes || total_spool_bytes >= pressure_at;
    let due = if urgent {
        queued_at
    } else {
        queued_at.saturating_add(archive.interval)
    };
    replica.archive_due = Some(replica.archive_due.map_or(due, |current| current.min(due)));
    if coalesced {
        host.counters.archive_commits_coalesced += 1;
    }
}

pub fn advance_archive_age(state: &SharedHost, elapsed: u64) {
    for replica in state.borrow_mut().replicas.values_mut() {
        if replica.committed.is_some() {
            replica.unarchived_age = replica.unarchived_age.saturating_add(elapsed);
        }
    }
}

pub fn archives_ready(state: &SharedHost) -> Vec<ReplicaKey> {
    let current = now();
    state
        .borrow()
        .replicas
        .iter()
        .filter_map(|(&key, replica)| {
            (!replica.archive_inflight
                && replica.archive_pending.is_some()
                && replica.archive_due.is_some_and(|due| due <= current))
            .then_some(key)
        })
        .collect()
}

pub async fn archive_latest<W>(state: SharedHost, world: Rc<W>, key: ReplicaKey)
where
    W: Blobs + Store + Peers + AdminIo + 'static,
{
    let cut = {
        let mut host = state.borrow_mut();
        let Some(replica) = host.replicas.get_mut(&key) else {
            return;
        };
        if replica.archive_inflight {
            return;
        }
        let Some(cut) = replica.archive_pending.take() else {
            return;
        };
        replica.archive_inflight = true;
        replica.archive_due = None;
        host.counters.archive_cycles += 1;
        cut
    };
    let _lease = ArchiveLease {
        state: Rc::clone(&state),
        key,
    };
    upload_commit(
        state,
        world,
        key.source,
        key.vset,
        key.assignment_epoch,
        cut.info,
        &cut.required,
        cut.record,
    )
    .await;
}

async fn replica_release<W: Blobs + Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    through: ReplicaCommitInfo,
) {
    if !request_authorized(state, source, vset, assignment_epoch) {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    let key = ReplicaKey {
        source,
        vset,
        assignment_epoch,
    };
    if !state.borrow().replicas.contains_key(&key) {
        Peers::send(
            world,
            source,
            PeerMsg::ReplicaReleaseAck {
                vset,
                assignment_epoch,
                through,
            },
        )
        .await;
        return;
    }
    let releasable = state.borrow().replicas.get(&key).is_some_and(|replica| {
        replica
            .committed
            .is_some_and(|committed| commit_rank(committed) <= commit_rank(through))
            && replica
                .uploaded
                .is_some_and(|uploaded| commit_rank(uploaded) <= commit_rank(through))
    });
    if !releasable {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    let current_generation = state
        .borrow()
        .replicas
        .get(&key)
        .map_or(0, |replica| replica.current_generation);
    let names = (0..=current_generation)
        .map(|generation| {
            layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation)
        })
        .collect::<Vec<_>>();
    if Blobs::delete_many_durable(world, &names).await.is_err() {
        return;
    }
    {
        let mut host = state.borrow_mut();
        host.replicas.remove(&key);
        host.forget_blobs(&names);
        host.counters.replica_unlinks += 1;
    }
    Peers::send(
        world,
        source,
        PeerMsg::ReplicaReleaseAck {
            vset,
            assignment_epoch,
            through,
        },
    )
    .await;
    if fault_point(FaultPoint::ReleaseOverlap) {
        Peers::send(
            world,
            source,
            PeerMsg::ReplicaReleaseAck {
                vset,
                assignment_epoch,
                through,
            },
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn upload_commit<W>(
    state: SharedHost,
    world: Rc<W>,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
    required: &[ReplicaArtifact],
    record: Vec<u8>,
) where
    W: Store + Peers + AdminIo + 'static,
{
    let key = ReplicaKey {
        source,
        vset,
        assignment_epoch,
    };
    let Some(artifacts) = ({
        let host = state.borrow();
        host.replicas.get(&key).map(|replica| {
            required
                .iter()
                .filter_map(|artifact| {
                    replica
                        .artifacts
                        .get(artifact)
                        .map(|(_, bytes)| (*artifact, bytes.clone()))
                })
                .collect::<Vec<_>>()
        })
    }) else {
        return;
    };
    if artifacts.len() != required.len() {
        return;
    }
    let retry = state.borrow().config.backup_retry;
    if store_put_retry(
        &state,
        world.as_ref(),
        layout::pending_manifest_key(vset, info.writer_fence, info.seq),
        record.clone(),
        retry,
    )
    .await
    .is_none()
    {
        state
            .borrow_mut()
            .fail("replica pending manifest upload failed");
        return;
    }
    let mut uploads = Vec::with_capacity(artifacts.len());
    for (artifact, bytes) in artifacts {
        if fault_point(FaultPoint::CrashPeerDuringUpload) {
            state.borrow_mut().fail("injected replica crash");
            return;
        }
        let object_key = match artifact {
            ReplicaArtifact::Segment { fence, seg } => layout::segment_key(vset, fence, seg),
            ReplicaArtifact::Leaf { fence, id } => layout::leaf_key(vset, fence, id),
        };
        let state = Rc::clone(&state);
        let world = Rc::clone(&world);
        uploads.push(spawn(async move {
            let len = bytes.len() as u64;
            let uploaded = store_put_retry(&state, world.as_ref(), object_key, bytes, retry)
                .await
                .is_some();
            (uploaded, len)
        }));
    }
    for upload in uploads {
        let Ok((uploaded, bytes)) = upload.await else {
            return;
        };
        if !uploaded {
            state.borrow_mut().fail("replica artifact upload failed");
            return;
        }
        state.borrow_mut().counters.replica_store_bytes += bytes;
    }
    if store_put_retry(
        &state,
        world.as_ref(),
        layout::manifest_key(vset, info.writer_fence, info.seq),
        record.clone(),
        retry,
    )
    .await
    .is_none()
    {
        state.borrow_mut().fail("replica manifest upload failed");
        return;
    }
    state.borrow_mut().counters.replica_store_bytes += record.len() as u64;
    if let Some(replica) = state.borrow_mut().replicas.get_mut(&key) {
        replica.uploaded = Some(info);
        replica.uploaded_record = Some(record.clone());
    }
    if fault_point(FaultPoint::CrashPeerAfterUploadBeforeHead) {
        state.borrow_mut().fail("injected replica crash");
        return;
    }
    Peers::send(
        world.as_ref(),
        source,
        PeerMsg::ReplicaUploadDone {
            vset,
            assignment_epoch,
            info,
            record,
        },
    )
    .await;
}

async fn replica_archive<W: Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    through: ReplicaCommitInfo,
) {
    if !request_authorized(state, source, vset, assignment_epoch) {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    let key = ReplicaKey {
        source,
        vset,
        assignment_epoch,
    };
    let record = {
        let mut host = state.borrow_mut();
        let Some(replica) = host.replicas.get_mut(&key) else {
            return;
        };
        let record = (replica.uploaded == Some(through))
            .then(|| replica.uploaded_record.clone())
            .flatten();
        if record.is_none()
            && replica
                .committed
                .is_some_and(|known| commit_rank(known) >= commit_rank(through))
        {
            replica.archive_due = Some(now());
        }
        record
    };
    if let Some(record) = record {
        Peers::send(
            world,
            source,
            PeerMsg::ReplicaUploadDone {
                vset,
                assignment_epoch,
                info: through,
                record,
            },
        )
        .await;
    }
}

async fn store_put_retry<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    retry: u64,
) -> Option<u64> {
    loop {
        match Store::put(world, key.clone(), bytes.clone()).await {
            Ok(version) => {
                if fault_point(FaultPoint::StoreUnknownResult) {
                    state.borrow_mut().counters.store_retries += 1;
                    blockd_exec::delay(retry).await;
                    continue;
                }
                return Some(version);
            }
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                blockd_exec::delay(retry).await;
            }
            Err(
                StoreError::TooLarge
                | StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. }),
            ) => return None,
        }
    }
}

#[allow(clippy::too_many_lines)]
pub async fn publish_replica_head<W>(state: SharedHost, world: Rc<W>, vset: VsetId)
where
    W: Blobs + Store + Peers + AdminIo + 'static,
{
    let Some((incarnation, expected, pointer, head, record)) = ({
        let mut host = state.borrow_mut();
        let holder = host.config.host;
        host.vsets.get_mut(&vset).and_then(|vset_state| {
            if vset_state.operations.publication_owner().is_some() {
                return None;
            }
            let info = vset_state.peer_upload_done?;
            let record = vset_state.peer_upload_record.clone()?;
            if commit_info(&record) != info {
                return None;
            }
            let pointer = ManifestPtr {
                fence: info.writer_fence,
                seq: info.seq,
                capture_seq: record.capture_seq,
            };
            if vset_state.backed.is_some_and(|backed| {
                (pointer.capture_seq, pointer.seq) <= (backed.capture_seq, backed.seq)
            }) {
                return None;
            }
            let expected = vset_state.head_version?;
            assert!(
                vset_state
                    .operations
                    .try_start_publication(PublicationOwner::Replica)
            );
            Some((
                vset_state.incarnation,
                expected,
                pointer,
                HeadRecord {
                    vset,
                    holder,
                    fence: vset_state.fence,
                    manifest: Some(pointer),
                    stash: vset_state.stash_assignment,
                    retired_stashes: vset_state.retired_stashes.clone(),
                },
                record,
            ))
        })
    }) else {
        return;
    };
    let _lease = ReplicaPublishLease::new(&state, vset, incarnation);
    let mut expected = expected;
    let mut attempts = 0u8;
    let version = loop {
        attempts = attempts.saturating_add(1);
        match Store::put_cas(
            world.as_ref(),
            layout::head_key(vset),
            Some(expected),
            head.encode(),
        )
        .await
        {
            Ok(version) => break version,
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                fence_primary(&state, vset, incarnation);
                return;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world.as_ref(), &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let Ok(found) = HeadRecord::decode(vset, &bytes) else {
                            state.borrow_mut().fail("damaged replica head");
                            return;
                        };
                        if found == head {
                            break version;
                        }
                        if found.holder != head.holder
                            || found.fence != head.fence
                            || found.stash != head.stash
                        {
                            fence_primary(&state, vset, incarnation);
                            return;
                        }
                        expected = version;
                        if attempts >= 3 {
                            return;
                        }
                    }
                    Ok(None) => {
                        fence_primary(&state, vset, incarnation);
                        return;
                    }
                    Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        return;
                    }
                    Err(
                        StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. }),
                    ) => {
                        state
                            .borrow_mut()
                            .fail("replica head reconciliation failed");
                        return;
                    }
                }
            }
        }
    };
    let dead_manifests = {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
        else {
            return;
        };
        let previous = vset_state.backed;
        vset_state.head_version = Some(version);
        vset_state.backed = Some(pointer);
        vset_state.peer_published = Some(commit_info(&record));
        if vset_state.peer_upload_done == Some(commit_info(&record)) {
            vset_state.peer_upload_done = None;
            vset_state.peer_upload_record = None;
        }
        for (_, location) in record
            .overlay
            .values()
            .filter(|(_, location)| location.base == 0)
        {
            vset_state
                .backed_segments
                .insert((location.fence, location.seg));
        }
        for pointer in record.leaves.values().filter(|pointer| pointer.base == 0) {
            vset_state.backed_leaves.insert((pointer.fence, pointer.id));
            if let Some((_, segments)) = vset_state.leaf_blobs.get(pointer) {
                vset_state.backed_segments.extend(segments.iter().copied());
            }
        }
        vset_state
            .store_manifests
            .insert((pointer.fence, pointer.seq));
        let mut dead = vset_state
            .store_manifests
            .iter()
            .copied()
            .filter(|candidate| *candidate < (pointer.fence, pointer.seq))
            .collect::<Vec<_>>();
        for candidate in &dead {
            vset_state.store_manifests.remove(candidate);
        }
        if let Some(previous) = previous.filter(|previous| *previous != pointer)
            && !dead.contains(&(previous.fence, previous.seq))
        {
            dead.push((previous.fence, previous.seq));
        }
        host.counters.manifests_published += 1;
        dead
    };
    for (fence, seq) in dead_manifests {
        let _ = Store::delete(world.as_ref(), &layout::manifest_key(vset, fence, seq)).await;
        let _ = Store::delete(
            world.as_ref(),
            &layout::pending_manifest_key(vset, fence, seq),
        )
        .await;
    }
    let _ = Store::delete(
        world.as_ref(),
        &layout::pending_manifest_key(vset, pointer.fence, pointer.seq),
    )
    .await;
    if fault_point(FaultPoint::CrashPrimaryAfterHeadBeforeRelease) {
        state.borrow_mut().fail("injected primary crash");
        return;
    }
    let reclaim_requested = state.borrow().disk_reclaim_requested;
    if reclaim_requested
        && reclaim_backed_segments(Rc::clone(&state), world.as_ref())
            .await
            .is_err()
    {
        state.borrow_mut().fail("backed segment reclaim failed");
        return;
    }
    retry_replica_releases(Rc::clone(&state), Rc::clone(&world), vset).await;
}

pub async fn retry_replica_releases<W: Peers + 'static>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
) {
    let releases = {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host.vsets.get(&vset) else {
            return;
        };
        let Some(info) = vset_state.peer_published else {
            return;
        };
        let published = vset_state
            .backed
            .is_some_and(|pointer| (pointer.fence, pointer.seq) == (info.writer_fence, info.seq));
        if !published {
            return;
        }
        let mut discovered = Vec::new();
        if let Some(assignment) = vset_state.stash_assignment {
            discovered.push((
                assignment.active_peer,
                vset,
                assignment.active_assignment_epoch,
                info,
            ));
        }
        discovered.extend(
            vset_state
                .retired_stashes
                .iter()
                .filter(|retired| commit_rank(retired.through) <= commit_rank(info))
                .map(|retired| {
                    (
                        retired.peer,
                        vset,
                        retired.assignment_epoch,
                        retired.through,
                    )
                }),
        );
        for release in discovered {
            if !host.replica_releases.contains(&release) {
                host.replica_releases.push(release);
            }
        }
        host.replica_releases
            .iter()
            .copied()
            .filter(|(_, owner, _, _)| *owner == vset)
            .collect::<Vec<_>>()
    };
    for (peer, owner, assignment_epoch, through) in releases {
        Peers::send(
            world.as_ref(),
            peer,
            PeerMsg::ReplicaRelease {
                vset: owner,
                assignment_epoch,
                through,
            },
        )
        .await;
        if fault_point(FaultPoint::ReleaseOverlap) {
            Peers::send(
                world.as_ref(),
                peer,
                PeerMsg::ReplicaRelease {
                    vset: owner,
                    assignment_epoch,
                    through,
                },
            )
            .await;
        }
    }
}

/// Ask the assigned passive peer to archive the newest committed cut now.
/// Local NVMe pressure uses this to create a reclaimable store copy without
/// waiting for the normal archive cadence.
pub async fn request_replica_archive<W: Peers>(state: SharedHost, world: Rc<W>, vset: VsetId) {
    let request = state.borrow().vsets.get(&vset).and_then(|vset_state| {
        let assignment = vset_state.stash_assignment?;
        let through = vset_state.peer_committed?;
        Some((
            assignment.transition_peer.unwrap_or(assignment.active_peer),
            assignment.assignment_epoch,
            through,
        ))
    });
    if let Some((peer, assignment_epoch, through)) = request {
        Peers::send(
            world.as_ref(),
            peer,
            PeerMsg::ReplicaArchive {
                vset,
                assignment_epoch,
                through,
            },
        )
        .await;
    }
}

pub async fn retry_archive_notices<W: Peers>(state: SharedHost, world: Rc<W>) {
    let notices = state
        .borrow()
        .replicas
        .iter()
        .filter_map(|(key, replica)| {
            Some((*key, replica.uploaded?, replica.uploaded_record.clone()?))
        })
        .collect::<Vec<_>>();
    for (key, info, record) in notices {
        Peers::send(
            world.as_ref(),
            key.source,
            PeerMsg::ReplicaUploadDone {
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
                info,
                record,
            },
        )
        .await;
    }
}

fn fence_primary(state: &SharedHost, vset: VsetId, incarnation: u64) {
    let removed = state
        .borrow()
        .vsets
        .get(&vset)
        .is_some_and(|vset_state| vset_state.incarnation == incarnation);
    if removed {
        state.borrow_mut().vsets.remove(&vset);
        state.borrow_mut().counters.fenced += 1;
        state.borrow_mut().fail("replica primary fenced");
    }
}

pub async fn create_peer_stashed<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    config: VsetConfig,
) -> Option<AdminResult>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    if state.borrow().vsets.contains_key(&vset) {
        return Some(Err(AdminError::Busy));
    }
    let Some(stash) = initial_stash(&state, vset) else {
        return Some(Err(AdminError::Unavailable));
    };
    let incarnation = state.borrow_mut().insert_fresh(vset, config);
    let _ = fault_point(FaultPoint::AssignmentCasRace);
    let Some(fence) =
        claim_new_head_with_stash(&state, world.as_ref(), vset, incarnation, Some(stash)).await
    else {
        state.borrow_mut().vsets.remove(&vset);
        return Some(Err(AdminError::Rejected));
    };
    {
        let mut host = state.borrow_mut();
        let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
        vset_state.fence = fence;
        vset_state.head_version = Some(fence);
        vset_state.stash_assignment = Some(stash);
        host.counters.assignment_claims += 1;
    }
    if !finish_creation(Rc::clone(&state), world.as_ref(), vset, incarnation).await {
        state
            .borrow_mut()
            .fail("peer-stashed journal creation failed");
        return None;
    }
    Some(Ok(AdminSuccess::VsetCreated { vset }))
}

struct ReplicationLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
}

struct ReplicaPublishLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
}

struct ArchiveLease {
    state: SharedHost,
    key: ReplicaKey,
}

impl ReplicaPublishLease {
    fn new(state: &SharedHost, vset: VsetId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
        }
    }
}

impl Drop for ReplicaPublishLease {
    fn drop(&mut self) {
        if let Some(vset_state) = self
            .state
            .borrow_mut()
            .vsets
            .get_mut(&self.vset)
            .filter(|vset_state| vset_state.incarnation == self.incarnation)
        {
            vset_state
                .operations
                .finish_publication(PublicationOwner::Replica);
        }
        self.state.borrow_mut().schedule_vset(self.vset);
    }
}

impl Drop for ArchiveLease {
    fn drop(&mut self) {
        if let Some(replica) = self.state.borrow_mut().replicas.get_mut(&self.key) {
            replica.archive_inflight = false;
        }
    }
}

impl Drop for ReplicationLease {
    fn drop(&mut self) {
        if let Some(vset_state) = self
            .state
            .borrow_mut()
            .vsets
            .get_mut(&self.vset)
            .filter(|vset_state| vset_state.incarnation == self.incarnation)
        {
            vset_state.operations.finish_replication();
        }
        self.state.borrow_mut().schedule_vset(self.vset);
    }
}

#[allow(clippy::too_many_lines)]
pub async fn replicate_latest<W>(state: SharedHost, world: Rc<W>, vset: VsetId)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let Some((incarnation, target, assignment_epoch, record, retry)) = ({
        let mut host = state.borrow_mut();
        let retry = host.config.backup_retry;
        host.vsets.get_mut(&vset).and_then(|vset_state| {
            let record = vset_state.best_record.clone()?;
            let stash = vset_state.stash_assignment?;
            let record_is_backed = vset_state.backed.is_some_and(|pointer| {
                (pointer.capture_seq, pointer.seq) == (record.capture_seq, record.seq)
            });
            let needed =
                !record_is_backed || record.sync_covered_through > vset_state.sync_ack_through;
            (needed && vset_state.operations.try_start_replication()).then(|| {
                (
                    vset_state.incarnation,
                    stash.transition_peer.unwrap_or(stash.active_peer),
                    stash.assignment_epoch,
                    record,
                    retry,
                )
            })
        })
    }) else {
        return;
    };
    let _lease = ReplicationLease {
        state: Rc::clone(&state),
        vset,
        incarnation,
    };
    let info = commit_info(&record);
    let Ok(status) = wait_status(
        &state,
        world.as_ref(),
        target,
        vset,
        assignment_epoch,
        retry,
    )
    .await
    else {
        let _ = transition_stash(&state, world.as_ref(), vset, incarnation).await;
        return;
    };
    if status.is_some_and(|committed| commit_rank(committed) >= commit_rank(info)) {
        finish_primary_commit(&state, vset, incarnation, info);
        return;
    }
    if fault_point(FaultPoint::CrashPrimaryBeforeClosureCapture) {
        state.borrow_mut().fail("injected primary crash");
        return;
    }
    let Some(required) = replica_closure(&state, vset, incarnation, &record) else {
        return;
    };
    if fault_point(FaultPoint::CrashPrimaryAfterClosureCapture) {
        state.borrow_mut().fail("injected primary crash");
        return;
    }
    for &artifact in &required {
        let name = match artifact {
            ReplicaArtifact::Segment { fence, seg } => layout::segment_blob(vset, fence, seg),
            ReplicaArtifact::Leaf { fence, id } => layout::leaf_blob(vset, fence, id),
        };
        let Ok(Some(bytes)) = Blobs::read(world.as_ref(), &name).await else {
            return;
        };
        if verify_replica_artifact(vset, artifact, &bytes).is_err() {
            return;
        }
        if fault_point(FaultPoint::CrashPrimaryDuringArtifactTransfer) {
            state.borrow_mut().fail("injected primary crash");
            return;
        }
        {
            let mut host = state.borrow_mut();
            host.counters.replica_logical_bytes = host
                .counters
                .replica_logical_bytes
                .saturating_add(bytes.len() as u64);
        }
        let checksum = crc32c(&bytes);
        if wait_put_ack(
            &state,
            world.as_ref(),
            target,
            vset,
            assignment_epoch,
            artifact,
            checksum,
            bytes,
            retry,
        )
        .await
        .is_err()
        {
            let _ = transition_stash(&state, world.as_ref(), vset, incarnation).await;
            return;
        }
    }
    if wait_commit_ack(
        &state,
        world.as_ref(),
        target,
        vset,
        assignment_epoch,
        info,
        required,
        record.encode(vset),
        retry,
    )
    .await
    .is_err()
    {
        let _ = transition_stash(&state, world.as_ref(), vset, incarnation).await;
        return;
    }
    let transitioning = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
        vset_state.stash_assignment.is_some_and(|stash| {
            stash.assignment_epoch == assignment_epoch && stash.transition_peer == Some(target)
        })
    });
    if transitioning {
        if fault_point(FaultPoint::CrashPrimaryAfterSeedBeforeActiveCas) {
            state.borrow_mut().fail("injected primary crash");
            return;
        }
        if !activate_stash(
            &state,
            world.as_ref(),
            vset,
            incarnation,
            target,
            assignment_epoch,
            info,
        )
        .await
        {
            return;
        }
    }
    if fault_point(FaultPoint::CrashPrimaryAfterAckBeforeSyncOk) {
        state.borrow_mut().fail("injected primary crash");
        return;
    }
    finish_primary_commit(&state, vset, incarnation, info);
    if fault_point(FaultPoint::CrashPrimaryAfterSyncOk) {
        state.borrow_mut().fail("injected primary crash");
    }
}

async fn transition_stash<W>(state: &SharedHost, world: &W, vset: VsetId, incarnation: u64) -> bool
where
    W: Store + AdminIo,
{
    let Some(proposal) = (|| {
        let host = state.borrow();
        let vset_state = host
            .vsets
            .get(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)?;
        if vset_state.retired_stashes.len() >= MAX_RETIRED_STASHES {
            return None;
        }
        let current = vset_state.stash_assignment?;
        if current.transition_peer.is_some() {
            return Some(current);
        }
        let placement = host.config.replica_placement.as_ref()?;
        let candidates = rank_stash_candidates(
            placement.membership_epoch,
            host.config.host,
            placement.local_failure_domain,
            vset,
            &placement.roster,
        );
        if candidates.is_empty() {
            return None;
        }
        let mut next_epoch = current.assignment_epoch;
        let mut next = None;
        for _ in 0..candidates.len() {
            next_epoch = next_epoch.checked_add(1)?;
            let epoch_index = usize::try_from(next_epoch - 1).ok()?;
            let candidate = candidates[epoch_index % candidates.len()];
            if candidate != current.active_peer && current.transition_peer != Some(candidate) {
                next = Some(candidate);
                break;
            }
        }
        let next = next?;
        Some(StashAssignment {
            assignment_epoch: next_epoch,
            active_peer: current.active_peer,
            active_assignment_epoch: current.active_assignment_epoch,
            transition_peer: Some(next),
            membership_epoch: current.membership_epoch,
        })
    })() else {
        return false;
    };
    if fault_point(FaultPoint::CrashPrimaryBeforeTransitionCas) {
        state.borrow_mut().fail("injected primary crash");
        return false;
    }
    let retired = state.borrow().vsets[&vset].retired_stashes.clone();
    cas_assignment(state, world, vset, incarnation, proposal, retired, None).await
}

#[allow(clippy::too_many_arguments)]
async fn activate_stash<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
    target: HostId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
) -> bool
where
    W: Store + AdminIo,
{
    let Some((assignment, retired)) = (|| {
        let host = state.borrow();
        let vset_state = host
            .vsets
            .get(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)?;
        let current = vset_state.stash_assignment?;
        if current.assignment_epoch != assignment_epoch || current.transition_peer != Some(target) {
            return None;
        }
        let assignment = StashAssignment {
            assignment_epoch,
            active_peer: target,
            active_assignment_epoch: assignment_epoch,
            transition_peer: None,
            membership_epoch: current.membership_epoch,
        };
        let mut retired = vset_state.retired_stashes.clone();
        let former = RetiredStash {
            peer: current.active_peer,
            assignment_epoch: current.active_assignment_epoch,
            through: info,
        };
        if !retired.contains(&former) {
            retired.push(former);
        }
        Some((assignment, retired))
    })() else {
        return false;
    };
    cas_assignment(
        state,
        world,
        vset,
        incarnation,
        assignment,
        retired,
        Some(FaultPoint::CrashPrimaryAfterActiveCasBeforeCommit),
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn cas_assignment<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
    assignment: StashAssignment,
    retired_stashes: Vec<RetiredStash>,
    crash_after: Option<FaultPoint>,
) -> bool
where
    W: Store + AdminIo,
{
    let retry = state.borrow().config.backup_retry;
    loop {
        let Some((expected, head, current_assignment, current_retired)) = (|| {
            let host = state.borrow();
            let vset_state = host
                .vsets
                .get(&vset)
                .filter(|vset_state| vset_state.incarnation == incarnation)?;
            Some((
                vset_state.head_version?,
                HeadRecord {
                    vset,
                    holder: host.config.host,
                    fence: vset_state.fence,
                    manifest: vset_state.backed,
                    stash: Some(assignment),
                    retired_stashes: retired_stashes.clone(),
                },
                vset_state.stash_assignment,
                vset_state.retired_stashes.clone(),
            ))
        })() else {
            return false;
        };
        let _ = fault_point(FaultPoint::AssignmentCasRace);
        let result =
            Store::put_cas(world, layout::head_key(vset), Some(expected), head.encode()).await;
        if let Ok(version) = result
            && !fault_point(FaultPoint::StoreUnknownResult)
        {
            if crash_after.is_some_and(fault_point) {
                state.borrow_mut().fail("injected primary crash");
                return false;
            }
            return adopt_assignment(
                state,
                vset,
                incarnation,
                version,
                assignment,
                retired_stashes,
            );
        }
        if matches!(result, Err(StoreError::TooLarge)) {
            return false;
        }
        state.borrow_mut().counters.store_retries += 1;
        let (version, bytes) = loop {
            match Store::get(world, &layout::head_key(vset)).await {
                Ok(Some(found)) => break found,
                Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                    delay(retry).await;
                }
                Ok(None)
                | Err(
                    StoreError::TooLarge
                    | StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. }),
                ) => return false,
            }
        };
        let Ok(found) = HeadRecord::decode(vset, &bytes) else {
            return false;
        };
        let local = state.borrow().config.host;
        let Some(fence) = state
            .borrow()
            .vsets
            .get(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
            .map(|vset_state| vset_state.fence)
        else {
            return false;
        };
        if found.holder != local || found.fence != fence {
            state.borrow_mut().fail("replica assignment fenced");
            return false;
        }
        if found == head {
            if crash_after.is_some_and(fault_point) {
                state.borrow_mut().fail("injected primary crash");
                return false;
            }
            return adopt_assignment(
                state,
                vset,
                incarnation,
                version,
                assignment,
                retired_stashes,
            );
        }
        if found.stash == current_assignment && found.retired_stashes == current_retired {
            {
                let mut host = state.borrow_mut();
                let Some(vset_state) = host
                    .vsets
                    .get_mut(&vset)
                    .filter(|vset_state| vset_state.incarnation == incarnation)
                else {
                    return false;
                };
                vset_state.head_version = Some(version);
                vset_state.backed = found.manifest;
            }
            delay(retry).await;
            continue;
        }
        return adopt_assignment_from_head(state, vset, incarnation, version, found);
    }
}

fn adopt_assignment(
    state: &SharedHost,
    vset: VsetId,
    incarnation: u64,
    version: u64,
    assignment: StashAssignment,
    retired_stashes: Vec<RetiredStash>,
) -> bool {
    let mut host = state.borrow_mut();
    let Some(vset_state) = host
        .vsets
        .get_mut(&vset)
        .filter(|vset_state| vset_state.incarnation == incarnation)
    else {
        return false;
    };
    if vset_state.stash_assignment != Some(assignment) {
        // Exact commit information belongs to the peer/assignment that
        // acknowledged it.  Keeping it across a transition could make the
        // scheduler ask the new peer to archive a cut it has not committed.
        vset_state.peer_committed = None;
        vset_state.peer_upload_done = None;
        vset_state.peer_upload_record = None;
    }
    vset_state.head_version = Some(version);
    vset_state.stash_assignment = Some(assignment);
    vset_state.retired_stashes = retired_stashes;
    true
}

fn adopt_assignment_from_head(
    state: &SharedHost,
    vset: VsetId,
    incarnation: u64,
    version: u64,
    head: HeadRecord,
) -> bool {
    let mut host = state.borrow_mut();
    let Some(vset_state) = host
        .vsets
        .get_mut(&vset)
        .filter(|vset_state| vset_state.incarnation == incarnation)
    else {
        return false;
    };
    if vset_state.stash_assignment != head.stash {
        vset_state.peer_committed = None;
        vset_state.peer_upload_done = None;
        vset_state.peer_upload_record = None;
    }
    vset_state.head_version = Some(version);
    vset_state.backed = head.manifest;
    vset_state.stash_assignment = head.stash;
    vset_state.retired_stashes = head.retired_stashes;
    true
}

pub(super) fn initial_stash(state: &SharedHost, vset: VsetId) -> Option<StashAssignment> {
    let host = state.borrow();
    let placement = host.config.replica_placement.as_ref()?;
    let target = rank_stash_candidates(
        placement.membership_epoch,
        host.config.host,
        placement.local_failure_domain,
        vset,
        &placement.roster,
    )
    .into_iter()
    .next()?;
    Some(StashAssignment {
        assignment_epoch: 1,
        active_peer: target,
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: placement.membership_epoch,
    })
}

fn replica_closure(
    state: &SharedHost,
    vset: VsetId,
    incarnation: u64,
    record: &JournalRecord,
) -> Option<Vec<ReplicaArtifact>> {
    let host = state.borrow();
    let vset_state = host
        .vsets
        .get(&vset)
        .filter(|vset_state| vset_state.incarnation == incarnation)?;
    let mut required = BTreeSet::new();
    for (_, location) in record
        .overlay
        .values()
        .filter(|(_, location)| location.base == 0)
    {
        if !vset_state
            .backed_segments
            .contains(&(location.fence, location.seg))
        {
            required.insert(ReplicaArtifact::Segment {
                fence: location.fence,
                seg: location.seg,
            });
        }
    }
    for pointer in record.leaves.values().filter(|pointer| pointer.base == 0) {
        if !vset_state
            .backed_leaves
            .contains(&(pointer.fence, pointer.id))
        {
            required.insert(ReplicaArtifact::Leaf {
                fence: pointer.fence,
                id: pointer.id,
            });
        }
        let (_, segments) = vset_state.leaf_blobs.get(pointer)?;
        for &(fence, seg) in segments {
            if !vset_state.backed_segments.contains(&(fence, seg)) {
                required.insert(ReplicaArtifact::Segment { fence, seg });
            }
        }
    }
    Some(required.into_iter().collect())
}

fn commit_info(record: &JournalRecord) -> ReplicaCommitInfo {
    ReplicaCommitInfo {
        writer_fence: record.fence,
        seq: record.seq,
        sync_covered_through: record.sync_covered_through,
    }
}

fn commit_rank(info: ReplicaCommitInfo) -> (u64, crate::types::JournalSeq, u64) {
    (info.writer_fence, info.seq, info.sync_covered_through)
}

fn commit_wait_key(
    target: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
) -> (HostId, VsetId, u64, u64, crate::types::JournalSeq, u64) {
    (
        target,
        vset,
        assignment_epoch,
        info.writer_fence,
        info.seq,
        info.sync_covered_through,
    )
}

fn finish_primary_commit(
    state: &SharedHost,
    vset: VsetId,
    incarnation: u64,
    info: ReplicaCommitInfo,
) {
    let completed = {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
        else {
            return;
        };
        if vset_state
            .peer_committed
            .is_none_or(|committed| commit_rank(info) >= commit_rank(committed))
        {
            vset_state.peer_committed = Some(info);
        }
        vset_state.peer_committed_through = vset_state
            .peer_committed_through
            .max(info.sync_covered_through);
        vset_state.sync_ack_through = vset_state.sync_ack_through.max(info.sync_covered_through);
        let mut completed = Vec::new();
        let pending = std::mem::take(&mut vset_state.pending_syncs);
        for sync in pending {
            if sync.barrier <= vset_state.sync_ack_through {
                completed.push(sync);
            } else {
                vset_state.pending_syncs.push(sync);
            }
        }
        host.counters.syncs_acked += completed.len() as u64;
        completed
    };
    for sync in completed {
        sync.resolve(true);
    }
}

async fn wait_status<W: Peers>(
    state: &SharedHost,
    world: &W,
    target: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    retry: u64,
) -> Result<Option<ReplicaCommitInfo>, PeerRpcError> {
    let client = state.borrow().peer_client.clone();
    let (committed, _) = client
        .replica_status(world, target, vset, assignment_epoch, retry)
        .await?;
    let _ = fault_point(FaultPoint::StatusReconciliation);
    Ok(committed)
}

#[allow(clippy::too_many_arguments)]
async fn wait_put_ack<W: Peers>(
    state: &SharedHost,
    world: &W,
    target: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    artifact: ReplicaArtifact,
    checksum: u32,
    bytes: Vec<u8>,
    retry: u64,
) -> Result<(), PeerRpcError> {
    let client = state.borrow().peer_client.clone();
    let result = client
        .replica_put(
            world,
            target,
            vset,
            assignment_epoch,
            artifact,
            checksum,
            bytes.clone(),
            retry,
        )
        .await;
    let (attempts, retries) = match &result {
        Ok(attempts) => (*attempts, attempts.saturating_sub(1)),
        Err(error) => (error.attempts, error.attempts),
    };
    let mut host = state.borrow_mut();
    host.counters.replica_network_bytes = host
        .counters
        .replica_network_bytes
        .saturating_add((bytes.len() as u64).saturating_mul(u64::from(attempts)));
    host.counters.peer_retries = host
        .counters
        .peer_retries
        .saturating_add(u64::from(retries));
    drop(host);
    result.map(|_| ())
}

#[allow(clippy::too_many_arguments)]
async fn wait_commit_ack<W: Peers>(
    state: &SharedHost,
    world: &W,
    target: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
    required: Vec<ReplicaArtifact>,
    record: Vec<u8>,
    retry: u64,
) -> Result<(), PeerRpcError> {
    let client = state.borrow().peer_client.clone();
    let result = client
        .replica_commit(
            world,
            target,
            vset,
            assignment_epoch,
            info,
            required,
            record,
            retry,
        )
        .await;
    let retries = match &result {
        Ok(attempts) => attempts.saturating_sub(1),
        Err(error) => error.attempts,
    };
    let mut host = state.borrow_mut();
    host.counters.peer_retries = host
        .counters
        .peer_retries
        .saturating_add(u64::from(retries));
    drop(host);
    result.map(|_| ())
}

fn replica_append_plan(
    state: &SharedHost,
    key: ReplicaKey,
    additional: u64,
) -> Option<(u64, bool)> {
    let host = state.borrow();
    let total = host
        .replicas
        .values()
        .map(|replica| replica.bytes)
        .fold(0u64, u64::saturating_add);
    if total.saturating_add(additional) > host.config.archive.spool_capacity_bytes {
        return None;
    }
    let replica = host.replicas.get(&key);
    let current_generation = replica.map_or(0, |replica| replica.current_generation);
    let current_file_bytes = replica.map_or(0, |replica| replica.current_file_bytes);
    let rotated = current_file_bytes != 0
        && current_file_bytes.saturating_add(additional) > MAX_REPLICA_SPOOL_GENERATION_BYTES;
    Some((
        current_generation.saturating_add(u64::from(rotated)),
        rotated,
    ))
}

fn request_authorized(
    state: &SharedHost,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
) -> bool {
    if assignment_epoch == 0 {
        return false;
    }
    let authorized = {
        let host = state.borrow();
        let Some(placement) = &host.config.replica_placement else {
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
        let Ok(index) = usize::try_from(assignment_epoch - 1) else {
            return false;
        };
        let candidates = rank_stash_candidates(
            placement.membership_epoch,
            source,
            source_domain,
            vset,
            &placement.roster,
        );
        !candidates.is_empty() && candidates[index % candidates.len()] == host.config.host
    };
    if !authorized {
        return false;
    }
    let mut host = state.borrow_mut();
    let latest = host.replica_latest_epoch.entry((source, vset)).or_default();
    if assignment_epoch < *latest {
        return false;
    }
    *latest = (*latest).max(assignment_epoch);
    true
}

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use super::*;
    use crate::hostmeta::{HostConfig, ReplicaPlacementConfig};
    use crate::placement::PeerCandidate;
    use crate::types::JournalSeq;
    use blockd_exec::Executor;

    fn test_state(host: HostId) -> SharedHost {
        Rc::new(RefCell::new(super::super::state::HostState::new(
            HostConfig {
                archive: Default::default(),
                host,
                cache_pages: 1,
                writeback_interval: 1,
                backup_retry: 1,
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 0,
                replica_placement: None,
            },
        )))
    }

    #[test]
    fn append_plan_rotates_before_crossing_the_generation_bound() {
        let state = test_state(HostId(1));
        let key = ReplicaKey {
            source: HostId(2),
            vset: VsetId(3),
            assignment_epoch: 1,
        };
        state.borrow_mut().replicas.insert(
            key,
            super::super::state::ReplicaState {
                bytes: MAX_REPLICA_SPOOL_GENERATION_BYTES,
                current_file_bytes: MAX_REPLICA_SPOOL_GENERATION_BYTES,
                ..Default::default()
            },
        );
        assert_eq!(replica_append_plan(&state, key, 1), Some((1, true)));
    }

    #[test]
    fn older_upload_completion_cannot_replace_the_latest_commit() {
        let mut state = super::super::state::VsetState::fresh(VsetConfig::compute(1, 4), 1);
        let newer = ReplicaCommitInfo {
            writer_fence: 3,
            seq: JournalSeq(7),
            sync_covered_through: 9,
        };
        let older = ReplicaCommitInfo {
            writer_fence: 3,
            seq: JournalSeq(6),
            sync_covered_through: 8,
        };
        let record = |info: ReplicaCommitInfo| JournalRecord {
            config: VsetConfig::compute(1, 4),
            seq: info.seq,
            fence: info.writer_fence,
            kind: crate::journal::RecordKind::Commit,
            capture_seq: info.sync_covered_through,
            sync_covered_through: info.sync_covered_through,
            database: Default::default(),
            overlay: Default::default(),
            leaves: Default::default(),
            migrated_from: None,
        };
        retain_latest_upload(&mut state, newer, record(newer));
        retain_latest_upload(&mut state, older, record(older));
        assert_eq!(state.peer_upload_done, Some(newer));
    }

    #[test]
    fn assignment_change_forgets_commit_owned_by_previous_peer() {
        let state = test_state(HostId(1));
        let vset = VsetId(3);
        let current = StashAssignment {
            assignment_epoch: 1,
            active_peer: HostId(2),
            active_assignment_epoch: 1,
            transition_peer: None,
            membership_epoch: 1,
        };
        let next = StashAssignment {
            assignment_epoch: 2,
            active_peer: HostId(2),
            active_assignment_epoch: 1,
            transition_peer: Some(HostId(4)),
            membership_epoch: 1,
        };
        let committed = ReplicaCommitInfo {
            writer_fence: 3,
            seq: JournalSeq(7),
            sync_covered_through: 9,
        };
        {
            let mut host = state.borrow_mut();
            let mut vset_state =
                super::super::state::VsetState::fresh(VsetConfig::compute(1, 4), 1);
            vset_state.stash_assignment = Some(current);
            vset_state.peer_committed = Some(committed);
            vset_state.peer_committed_through = committed.sync_covered_through;
            host.vsets.insert(vset, vset_state);
        }

        assert!(adopt_assignment(&state, vset, 1, 5, next, Vec::new()));

        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        assert_eq!(vset_state.peer_committed, None);
        assert_eq!(
            vset_state.peer_committed_through,
            committed.sync_covered_through
        );
    }

    #[test]
    fn append_plan_enforces_configured_host_capacity() {
        let state = test_state(HostId(1));
        state.borrow_mut().config.archive.spool_capacity_bytes = 100;
        let key = ReplicaKey {
            source: HostId(2),
            vset: VsetId(3),
            assignment_epoch: 1,
        };
        state.borrow_mut().replicas.insert(
            key,
            super::super::state::ReplicaState {
                bytes: 100,
                ..Default::default()
            },
        );
        assert_eq!(replica_append_plan(&state, key, 1), None);
    }

    #[test]
    fn archive_cadence_coalesces_to_the_newest_committed_cut() {
        let state = test_state(HostId(1));
        {
            let mut host = state.borrow_mut();
            host.config.archive.interval = 100;
            host.config.archive.max_unpublished_bytes = u64::MAX;
            host.config.archive.spool_capacity_bytes = 1_000;
            host.config.archive.spool_headroom_bytes = 100;
        }
        let key = ReplicaKey {
            source: HostId(2),
            vset: VsetId(3),
            assignment_epoch: 1,
        };
        state.borrow_mut().replicas.insert(key, Default::default());
        let first = ReplicaCommitInfo {
            writer_fence: 1,
            seq: JournalSeq(1),
            sync_covered_through: 1,
        };
        let second = ReplicaCommitInfo {
            writer_fence: 1,
            seq: JournalSeq(2),
            sync_covered_through: 2,
        };
        let mut executor = Executor::simulation(1);
        executor.block_on({
            let state = Rc::clone(&state);
            async move {
                queue_archive(&state, key, first, Vec::new(), vec![1]);
                delay(50).await;
                queue_archive(&state, key, second, Vec::new(), vec![2]);
                assert!(archives_ready(&state).is_empty());
            }
        });
        assert_eq!(state.borrow().counters.archive_commits_coalesced, 1);
        assert_eq!(
            state.borrow().replicas[&key]
                .archive_pending
                .as_ref()
                .map(|cut| cut.info),
            Some(second)
        );
        executor.advance_to(100);
        executor.block_on({
            let state = Rc::clone(&state);
            async move { assert_eq!(archives_ready(&state), [key]) }
        });
    }

    #[test]
    fn request_authority_rejects_an_epoch_below_the_recovered_floor() {
        let source = HostId(10);
        let vset = VsetId(12);
        let roster = vec![
            PeerCandidate {
                host: source,
                weight: 1,
                failure_domain: 1,
                drained: false,
            },
            PeerCandidate {
                host: HostId(11),
                weight: 1,
                failure_domain: 2,
                drained: false,
            },
            PeerCandidate {
                host: HostId(12),
                weight: 1,
                failure_domain: 3,
                drained: false,
            },
        ];
        let ranked = rank_stash_candidates(5, source, 1, vset, &roster);
        let local = ranked[0];
        let state = test_state(local);
        state.borrow_mut().config.replica_placement = Some(ReplicaPlacementConfig {
            membership_epoch: 5,
            local_failure_domain: roster
                .iter()
                .find(|candidate| candidate.host == local)
                .expect("ranked host is in roster")
                .failure_domain,
            roster,
        });
        state
            .borrow_mut()
            .replica_latest_epoch
            .insert((source, vset), 2);
        assert!(!request_authorized(&state, source, vset, 1));
        assert_eq!(
            state.borrow().replica_latest_epoch,
            BTreeMap::from([((source, vset), 2)])
        );
    }
}
