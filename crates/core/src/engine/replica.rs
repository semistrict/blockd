use std::collections::BTreeSet;
use std::rc::Rc;

use blockd_exec::channel::oneshot;
use blockd_exec::{FaultPoint, delay, fault_point, timeout};

use super::backup::claim_new_head_with_stash;
use super::capture::finish_creation;
use super::state::{ReplicaKey, SharedHost};
use crate::format::crc32c;
use crate::head::{HeadRecord, MAX_RETIRED_STASHES, ManifestPtr, RetiredStash, StashAssignment};
use crate::journal::{DurabilityMode, JournalRecord, VsetConfig};
use crate::layout;
use crate::placement::rank_stash_candidates;
use crate::protocol::{AdminReply, PeerMsg, ReplicaArtifact, ReplicaCommitInfo, ReqId, StoreFault};
use crate::replica_spool::{
    seal_replica_commit, seal_verified_replica_artifact, verify_replica_artifact,
};
use crate::types::{HostId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

pub const MAX_REPLICA_SOURCE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_REPLICA_TOTAL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
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
            if let Some(waiter) = state.borrow_mut().replica_put_waiters.remove(&(
                vset,
                assignment_epoch,
                artifact,
                checksum,
            )) {
                let _ = waiter.send(from);
            }
        }
        PeerMsg::ReplicaCommitAck {
            vset,
            assignment_epoch,
            info,
        } => {
            if let Some(waiter) = state
                .borrow_mut()
                .replica_commit_waiters
                .remove(&commit_wait_key(vset, assignment_epoch, info))
            {
                let _ = waiter.send(from);
            }
        }
        PeerMsg::ReplicaStatusReply {
            vset,
            assignment_epoch,
            committed,
        } => {
            if let Some(waiter) = state
                .borrow_mut()
                .replica_status_waiters
                .remove(&(vset, assignment_epoch))
            {
                let _ = waiter.send((from, committed));
            }
        }
        PeerMsg::ReplicaUploadDone {
            vset,
            assignment_epoch,
            info,
        } => {
            let valid = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
                vset_state.stash_assignment.is_some_and(|stash| {
                    stash.assignment_epoch == assignment_epoch
                        && stash.transition_peer.unwrap_or(stash.active_peer) == from
                })
            });
            if valid {
                if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                    vset_state.peer_upload_done = Some(info);
                }
            } else {
                state.borrow_mut().counters.replica_rejected += 1;
            }
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
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    };
    let spool_name = layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation);
    if !state
        .borrow_mut()
        .try_reserve_append(spool_name.clone(), frame.len() as u64)
    {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    if Blobs::append(world, spool_name.clone(), frame.clone())
        .await
        .is_err()
    {
        AdminIo::abort(world, "replica artifact append failed").await;
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
        AdminIo::abort(world, "injected replica crash").await;
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
    let complete = state.borrow().replicas.get(&key).is_some_and(|replica| {
        required
            .iter()
            .all(|artifact| replica.artifacts.contains_key(artifact))
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
    if state
        .borrow()
        .replicas
        .get(&key)
        .and_then(|replica| replica.committed)
        == Some(info)
    {
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
                },
            )
            .await;
        } else {
            upload_commit(
                state,
                world,
                source,
                vset,
                assignment_epoch,
                info,
                &required,
                record,
            )
            .await;
        }
        return;
    }
    let Some((generation, rotated)) = replica_append_plan(state, key, frame.len() as u64) else {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    };
    let spool_name = layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation);
    if !state
        .borrow_mut()
        .try_reserve_append(spool_name.clone(), frame.len() as u64)
    {
        state.borrow_mut().counters.replica_rejected += 1;
        return;
    }
    if Blobs::append(world, spool_name.clone(), frame.clone())
        .await
        .is_err()
    {
        AdminIo::abort(world, "replica commit append failed").await;
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
        replica.bytes += frame.len() as u64;
        replica.current_file_bytes += frame.len() as u64;
        host.counters.replica_bytes += frame.len() as u64;
        host.counters.replica_rotations += u64::from(rotated);
        host.counters.replica_commits += 1;
        host.counters.replica_commit_flushes += 1;
    }
    if fault_point(FaultPoint::CrashPeerAfterCommitBeforeAck) {
        AdminIo::abort(world, "injected replica crash").await;
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
    upload_commit(
        state,
        world,
        source,
        vset,
        assignment_epoch,
        info,
        &required,
        record,
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

#[allow(clippy::too_many_arguments)]
async fn upload_commit<W>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
    required: &[ReplicaArtifact],
    record: Vec<u8>,
) where
    W: Store + Peers + AdminIo,
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
    for (artifact, bytes) in artifacts {
        if fault_point(FaultPoint::CrashPeerDuringUpload) {
            AdminIo::abort(world, "injected replica crash").await;
            return;
        }
        let key = match artifact {
            ReplicaArtifact::Segment { fence, seg } => layout::segment_key(vset, fence, seg),
            ReplicaArtifact::Leaf { fence, id } => layout::leaf_key(vset, fence, id),
        };
        if store_put_retry(state, world, key, bytes.clone(), retry)
            .await
            .is_none()
        {
            AdminIo::abort(world, "replica artifact upload failed").await;
            return;
        }
        state.borrow_mut().counters.replica_store_bytes += bytes.len() as u64;
    }
    if store_put_retry(
        state,
        world,
        layout::manifest_key(vset, info.writer_fence, info.seq),
        record.clone(),
        retry,
    )
    .await
    .is_none()
    {
        AdminIo::abort(world, "replica manifest upload failed").await;
        return;
    }
    state.borrow_mut().counters.replica_store_bytes += record.len() as u64;
    if let Some(replica) = state.borrow_mut().replicas.get_mut(&key) {
        replica.uploaded = Some(info);
    }
    if fault_point(FaultPoint::CrashPeerAfterUploadBeforeHead) {
        AdminIo::abort(world, "injected replica crash").await;
        return;
    }
    Peers::send(
        world,
        source,
        PeerMsg::ReplicaUploadDone {
            vset,
            assignment_epoch,
            info,
        },
    )
    .await;
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
    W: Store + Peers + AdminIo + 'static,
{
    let Some((incarnation, expected, pointer, head, retry, record)) = ({
        let host = state.borrow();
        host.vsets.get(&vset).and_then(|vset_state| {
            let info = vset_state.peer_upload_done?;
            let record = vset_state.best_record.clone()?;
            if commit_info(&record) != info {
                return None;
            }
            let pointer = ManifestPtr {
                fence: info.writer_fence,
                seq: info.seq,
                capture_seq: record.capture_seq,
            };
            Some((
                vset_state.incarnation,
                vset_state.head_version?,
                pointer,
                HeadRecord {
                    vset,
                    holder: host.config.host,
                    fence: vset_state.fence,
                    manifest: Some(pointer),
                    stash: vset_state.stash_assignment,
                    retired_stashes: vset_state.retired_stashes.clone(),
                },
                host.config.backup_retry,
                record,
            ))
        })
    }) else {
        return;
    };
    let mut expected = expected;
    let version = loop {
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
                fence_primary(&state, world.as_ref(), vset, incarnation).await;
                return;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world.as_ref(), &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let Ok(found) = HeadRecord::decode(vset, &bytes) else {
                            AdminIo::abort(world.as_ref(), "damaged replica head").await;
                            return;
                        };
                        if found == head {
                            break version;
                        }
                        if found.holder != head.holder
                            || found.fence != head.fence
                            || found.stash != head.stash
                        {
                            fence_primary(&state, world.as_ref(), vset, incarnation).await;
                            return;
                        }
                        expected = version;
                    }
                    Ok(None) => {
                        fence_primary(&state, world.as_ref(), vset, incarnation).await;
                        return;
                    }
                    Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        blockd_exec::delay(retry).await;
                    }
                    Err(
                        StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. }),
                    ) => {
                        AdminIo::abort(world.as_ref(), "replica head reconciliation failed").await;
                        return;
                    }
                }
            }
        }
    };
    {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
        else {
            return;
        };
        vset_state.head_version = Some(version);
        vset_state.backed = Some(pointer);
        vset_state.peer_upload_done = None;
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
        host.counters.manifests_published += 1;
    }
    if fault_point(FaultPoint::CrashPrimaryAfterHeadBeforeRelease) {
        AdminIo::abort(world.as_ref(), "injected primary crash").await;
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
        let Some(record) = vset_state.best_record.as_ref() else {
            return;
        };
        let info = commit_info(record);
        let published = vset_state.backed.is_some_and(|pointer| {
            (pointer.capture_seq, pointer.seq) == (record.capture_seq, record.seq)
        });
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

async fn fence_primary<W: AdminIo>(state: &SharedHost, world: &W, vset: VsetId, incarnation: u64) {
    let removed = state
        .borrow()
        .vsets
        .get(&vset)
        .is_some_and(|vset_state| vset_state.incarnation == incarnation);
    if removed {
        state.borrow_mut().vsets.remove(&vset);
        state.borrow_mut().counters.fenced += 1;
        AdminIo::abort(world, "replica primary fenced").await;
    }
}

pub async fn create_peer_stashed<W>(
    state: SharedHost,
    world: Rc<W>,
    req: ReqId,
    vset: VsetId,
    config: VsetConfig,
) where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    if config.durability != DurabilityMode::PeerStashed || state.borrow().vsets.contains_key(&vset)
    {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    }
    let Some(stash) = initial_stash(&state, vset) else {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };
    let incarnation = state.borrow_mut().insert_fresh(vset, config);
    let _ = fault_point(FaultPoint::AssignmentCasRace);
    let Some(fence) =
        claim_new_head_with_stash(&state, world.as_ref(), vset, incarnation, Some(stash)).await
    else {
        state.borrow_mut().vsets.remove(&vset);
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };
    {
        let mut host = state.borrow_mut();
        let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
        vset_state.fence = fence;
        vset_state.head_version = Some(fence);
        vset_state.stash_assignment = Some(stash);
        host.counters.assignment_claims += 1;
    }
    if !finish_creation(Rc::clone(&state), world.as_ref(), req, vset, incarnation).await {
        AdminIo::abort(world.as_ref(), "peer-stashed journal creation failed").await;
    }
}

struct ReplicationLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
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
            vset_state.replicating = false;
        }
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
            let needed = vset_state.config.durability.requires_peer_sync()
                && !vset_state.replicating
                && record.sync_covered_through > vset_state.sync_ack_through;
            needed.then(|| {
                vset_state.replicating = true;
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
        finish_primary_commit(&state, world.as_ref(), vset, incarnation, info).await;
        return;
    }
    if fault_point(FaultPoint::CrashPrimaryBeforeClosureCapture) {
        AdminIo::abort(world.as_ref(), "injected primary crash").await;
        return;
    }
    let Some(required) = replica_closure(&state, vset, incarnation, &record) else {
        return;
    };
    if fault_point(FaultPoint::CrashPrimaryAfterClosureCapture) {
        AdminIo::abort(world.as_ref(), "injected primary crash").await;
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
            AdminIo::abort(world.as_ref(), "injected primary crash").await;
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
            AdminIo::abort(world.as_ref(), "injected primary crash").await;
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
        AdminIo::abort(world.as_ref(), "injected primary crash").await;
        return;
    }
    finish_primary_commit(&state, world.as_ref(), vset, incarnation, info).await;
    if fault_point(FaultPoint::CrashPrimaryAfterSyncOk) {
        AdminIo::abort(world.as_ref(), "injected primary crash").await;
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
        let next_epoch = current.assignment_epoch.checked_add(1)?;
        let next = *candidates.get(usize::try_from(next_epoch - 1).ok()?)?;
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
        AdminIo::abort(world, "injected primary crash").await;
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
                AdminIo::abort(world, "injected primary crash").await;
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
        let fence = state.borrow().vsets[&vset].fence;
        if found.holder != local || found.fence != fence {
            AdminIo::abort(world, "replica assignment fenced").await;
            return false;
        }
        if found == head {
            if crash_after.is_some_and(fault_point) {
                AdminIo::abort(world, "injected primary crash").await;
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
    vset_state.head_version = Some(version);
    vset_state.backed = head.manifest;
    vset_state.stash_assignment = head.stash;
    vset_state.retired_stashes = head.retired_stashes;
    true
}

fn initial_stash(state: &SharedHost, vset: VsetId) -> Option<StashAssignment> {
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
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
) -> (VsetId, u64, u64, crate::types::JournalSeq, u64) {
    (
        vset,
        assignment_epoch,
        info.writer_fence,
        info.seq,
        info.sync_covered_through,
    )
}

async fn finish_primary_commit<W: GuestMem>(
    state: &SharedHost,
    world: &W,
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
        vset_state.peer_committed_through = vset_state
            .peer_committed_through
            .max(info.sync_covered_through);
        vset_state.sync_ack_through = vset_state.sync_ack_through.max(info.sync_covered_through);
        let mut completed = Vec::new();
        vset_state.pending_syncs.retain(|(req, barrier)| {
            if *barrier <= vset_state.sync_ack_through {
                completed.push(*req);
                false
            } else {
                true
            }
        });
        host.counters.syncs_acked += completed.len() as u64;
        completed
    };
    for req in completed {
        GuestMem::sync_ok(world, req).await;
    }
}

async fn wait_status<W: Peers>(
    state: &SharedHost,
    world: &W,
    target: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    retry: u64,
) -> Result<Option<ReplicaCommitInfo>, ()> {
    let mut retries = 0_u8;
    loop {
        let (send, receive) = oneshot();
        state
            .borrow_mut()
            .replica_status_waiters
            .insert((vset, assignment_epoch), send);
        Peers::send(
            world,
            target,
            PeerMsg::ReplicaStatus {
                vset,
                assignment_epoch,
            },
        )
        .await;
        if let Ok(Ok((from, committed))) = timeout(retry, receive).await
            && from == target
        {
            let _ = fault_point(FaultPoint::StatusReconciliation);
            return Ok(committed);
        }
        state
            .borrow_mut()
            .replica_status_waiters
            .remove(&(vset, assignment_epoch));
        retries = retries.saturating_add(1);
        if fault_point(FaultPoint::ReplicaRetryTimer) || retries >= 3 {
            return Err(());
        }
    }
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
) -> Result<(), ()> {
    let mut retries = 0_u8;
    loop {
        let (send, receive) = oneshot();
        state
            .borrow_mut()
            .replica_put_waiters
            .insert((vset, assignment_epoch, artifact, checksum), send);
        {
            let mut host = state.borrow_mut();
            host.counters.replica_network_bytes = host
                .counters
                .replica_network_bytes
                .saturating_add(bytes.len() as u64);
        }
        Peers::send(
            world,
            target,
            PeerMsg::ReplicaPut {
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes: bytes.clone(),
            },
        )
        .await;
        if let Ok(Ok(from)) = timeout(retry, receive).await
            && from == target
        {
            return Ok(());
        }
        state.borrow_mut().replica_put_waiters.remove(&(
            vset,
            assignment_epoch,
            artifact,
            checksum,
        ));
        state.borrow_mut().counters.peer_retries += 1;
        retries = retries.saturating_add(1);
        if fault_point(FaultPoint::ReplicaRetryTimer) || retries >= 3 {
            return Err(());
        }
    }
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
) -> Result<(), ()> {
    let mut retries = 0_u8;
    loop {
        let (send, receive) = oneshot();
        state
            .borrow_mut()
            .replica_commit_waiters
            .insert(commit_wait_key(vset, assignment_epoch, info), send);
        Peers::send(
            world,
            target,
            PeerMsg::ReplicaCommit {
                vset,
                assignment_epoch,
                info,
                required: required.clone(),
                record: record.clone(),
            },
        )
        .await;
        if let Ok(Ok(from)) = timeout(retry, receive).await
            && from == target
        {
            return Ok(());
        }
        state
            .borrow_mut()
            .replica_commit_waiters
            .remove(&commit_wait_key(vset, assignment_epoch, info));
        state.borrow_mut().counters.peer_retries += 1;
        retries = retries.saturating_add(1);
        if fault_point(FaultPoint::ReplicaRetryTimer) || retries >= 3 {
            return Err(());
        }
    }
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
    let source_total = host
        .replicas
        .iter()
        .filter(|(candidate, _)| candidate.source == key.source)
        .map(|(_, replica)| replica.bytes)
        .fold(0u64, u64::saturating_add);
    if total.saturating_add(additional) > MAX_REPLICA_TOTAL_BYTES
        || source_total.saturating_add(additional) > MAX_REPLICA_SOURCE_BYTES
    {
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
        rank_stash_candidates(
            placement.membership_epoch,
            source,
            source_domain,
            vset,
            &placement.roster,
        )
        .get(index)
            == Some(&host.config.host)
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
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use super::*;
    use crate::hostmeta::{HostConfig, ReplicaPlacementConfig};
    use crate::placement::PeerCandidate;

    fn test_state(host: HostId) -> SharedHost {
        Rc::new(RefCell::new(super::super::state::HostState::new(
            HostConfig {
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
    fn append_plan_enforces_per_source_and_host_capacity() {
        let state = test_state(HostId(1));
        let key = ReplicaKey {
            source: HostId(2),
            vset: VsetId(3),
            assignment_epoch: 1,
        };
        state.borrow_mut().replicas.insert(
            key,
            super::super::state::ReplicaState {
                bytes: MAX_REPLICA_SOURCE_BYTES,
                ..Default::default()
            },
        );
        assert_eq!(replica_append_plan(&state, key, 1), None);

        let state = test_state(HostId(1));
        for source in 2..6 {
            state.borrow_mut().replicas.insert(
                ReplicaKey {
                    source: HostId(source),
                    vset: VsetId(u64::from(source)),
                    assignment_epoch: 1,
                },
                super::super::state::ReplicaState {
                    bytes: MAX_REPLICA_SOURCE_BYTES,
                    ..Default::default()
                },
            );
        }
        let new_key = ReplicaKey {
            source: HostId(7),
            vset: VsetId(7),
            assignment_epoch: 1,
        };
        assert_eq!(replica_append_plan(&state, new_key, 1), None);
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
