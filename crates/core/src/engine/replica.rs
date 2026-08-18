use std::collections::BTreeSet;
use std::rc::Rc;

use blockd_exec::{FaultPoint, delay, fault_point, spawn};

use super::backup::claim_new_head_with_stash;
use super::capture::finish_creation;
use super::commit_active_vnode_quorum;
use super::peer_client::PeerRpcError;
use super::state::{ReplicaKey, SharedHost};
use crate::format::crc32c;
use crate::head::{HeadRecord, MAX_RETIRED_STASHES, RetiredStash, StashAssignment};
use crate::journal::{JournalRecord, VsetConfig};
use crate::layout;
use crate::placement::rank_stash_candidates;
use crate::protocol::{
    AdminError, AdminResult, AdminSuccess, PeerMsg, ReplicaArtifact, ReplicaCommitInfo,
};
use crate::replica_spool::{
    seal_replica_commit, seal_verified_replica_artifact, verify_replica_artifact,
};
use crate::types::{HostId, VsetId};
use crate::vnode_member::VnodeRecoveryClosure;
use crate::world::{AdminIo, BlobError, Blobs, GuestMem, Peers, Store, StoreError};

/// Rotation happens before an append. A single verified frame may therefore
/// exceed this bound, but existing bytes are never copied between generations.
pub const MAX_REPLICA_SPOOL_GENERATION_BYTES: u64 = 64 * 1024 * 1024;
const REPLICA_TRANSFER_CONCURRENCY: usize = 64;

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
                        .map(|retired| (vset_state.incarnation, retired))
                })
            };
            if let Some((incarnation, retired)) = retired {
                let _ = remove_retired_stash(&state, world, vset, incarnation, retired).await;
            }
        }
        _ => unreachable!("non-replica message"),
    }
}

async fn remove_retired_stash<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
    retired: RetiredStash,
) -> bool
where
    W: Store + AdminIo,
{
    let retry = state.borrow().config.backup_retry;
    loop {
        let Some((expected, head)) = (|| {
            let host = state.borrow();
            let vset_state = host
                .vsets
                .get(&vset)
                .filter(|vset_state| vset_state.incarnation == incarnation)?;
            if !vset_state.retired_stashes.contains(&retired) {
                return Some((None, None));
            }
            let remaining = vset_state
                .retired_stashes
                .iter()
                .copied()
                .filter(|entry| *entry != retired)
                .collect();
            Some((
                Some(vset_state.head_version?),
                Some(HeadRecord {
                    vset,
                    holder: host.config.host,
                    fence: vset_state.fence,
                    manifest: vset_state.backed,
                    stash: vset_state.stash_assignment,
                    retired_stashes: remaining,
                }),
            ))
        })() else {
            return false;
        };
        let (Some(expected), Some(head)) = (expected, head) else {
            return true;
        };
        let result =
            Store::put_cas(world, layout::head_key(vset), Some(expected), head.encode()).await;
        if let Ok(version) = result
            && !fault_point(FaultPoint::StoreUnknownResult)
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
            vset_state.retired_stashes.clone_from(&head.retired_stashes);
            return true;
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
            state.borrow_mut().fail("replica release cleanup fenced");
            return false;
        }
        let removed = !found.retired_stashes.contains(&retired);
        if !adopt_assignment_from_head(state, vset, incarnation, version, found) {
            return false;
        }
        if removed {
            return true;
        }
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
            if let Some(replica) = state.borrow_mut().replicas.get_mut(&key) {
                replica.uncommitted_artifacts.insert(artifact);
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
    match Blobs::append(world, spool_name.clone(), frame.clone()).await {
        Ok(()) => {}
        Err(BlobError::Full) => {
            let mut host = state.borrow_mut();
            host.rollback_append_reservation(&spool_name, frame.len() as u64);
            host.note_blob_full();
            return;
        }
        Err(BlobError::Io) => {
            state.borrow_mut().fail("replica artifact append failed");
            return;
        }
    }
    {
        let mut host = state.borrow_mut();
        let replica = host.replicas.entry(key).or_default();
        if rotated {
            replica.current_generation = generation;
            replica.current_file_bytes = 0;
        }
        replica.artifacts.insert(artifact, (checksum, bytes));
        replica.uncommitted_artifacts.insert(artifact);
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
    match Blobs::append(world, spool_name.clone(), frame.clone()).await {
        Ok(()) => {}
        Err(BlobError::Full) => {
            let mut host = state.borrow_mut();
            host.rollback_append_reservation(&spool_name, frame.len() as u64);
            host.note_blob_full();
            return;
        }
        Err(BlobError::Io) => {
            state.borrow_mut().fail("replica commit append failed");
            return;
        }
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
        for artifact in &required {
            replica.uncommitted_artifacts.remove(artifact);
        }
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
    let superseded = state
        .borrow()
        .replicas
        .get(&key)
        .and_then(|replica| replica.committed)
        .is_some_and(|committed| commit_rank(committed) > commit_rank(through));
    if superseded {
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
            && replica.uncommitted_artifacts.is_empty()
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
        let published = vset_state.peer_published == Some(info) && vset_state.backed.is_some();
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

impl Drop for ReplicationLease {
    fn drop(&mut self) {
        if let Some(vset_state) = self
            .state
            .borrow_mut()
            .vsets
            .get_mut(&self.vset)
            .filter(|vset_state| vset_state.incarnation == self.incarnation)
        {
            vset_state.replicating_segments.clear();
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
            let required = commit_info(&record);
            let needed = vset_state
                .peer_committed
                .is_none_or(|committed| commit_rank(committed) < commit_rank(required));
            if needed && vset_state.operations.try_start_replication() {
                vset_state.replicating_segments = record
                    .files
                    .iter()
                    .filter_map(|file| {
                        (file.identity.namespace_kind == crate::blx::NamespaceKind::Vset
                            && file.identity.namespace_id == vset.0)
                            .then_some((
                                file.identity.writer_fence,
                                crate::types::SegId(file.identity.object_id),
                            ))
                    })
                    .collect();
                Some((
                    vset_state.incarnation,
                    stash.transition_peer.unwrap_or(stash.active_peer),
                    stash.assignment_epoch,
                    record,
                    retry,
                ))
            } else {
                None
            }
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
    let vnode_authority_enabled = state
        .borrow()
        .config
        .replica_placement
        .as_ref()
        .and_then(|placement| placement.authority)
        .is_some();
    if !vnode_authority_enabled
        && status.is_some_and(|committed| commit_rank(committed) >= commit_rank(info))
    {
        finish_primary_commit(&state, vset, incarnation, info, record.clone());
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
    let mut closure_artifacts = Vec::with_capacity(required.len());
    for batch in required.chunks(REPLICA_TRANSFER_CONCURRENCY) {
        let mut transfers = Vec::with_capacity(batch.len());
        for &artifact in batch {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            transfers.push(spawn(async move {
                let name = match artifact {
                    ReplicaArtifact::Segment { fence, seg } => {
                        layout::segment_blob(vset, fence, seg)
                    }
                };
                let Ok(Some(bytes)) = Blobs::read(world.as_ref(), &name).await else {
                    return None;
                };
                if verify_replica_artifact(vset, artifact, &bytes).is_err() {
                    return None;
                }
                if fault_point(FaultPoint::CrashPrimaryDuringArtifactTransfer) {
                    state.borrow_mut().fail("injected primary crash");
                    return None;
                }
                {
                    let mut host = state.borrow_mut();
                    host.counters.replica_logical_bytes = host
                        .counters
                        .replica_logical_bytes
                        .saturating_add(bytes.len() as u64);
                }
                let checksum = crc32c(&bytes);
                wait_put_ack(
                    &state,
                    world.as_ref(),
                    target,
                    vset,
                    assignment_epoch,
                    artifact,
                    checksum,
                    bytes.clone(),
                    retry,
                )
                .await
                .ok()
                .map(|()| (artifact, bytes))
            }));
        }
        let mut failed = false;
        for transfer in transfers {
            match transfer.await {
                Ok(Some(artifact)) => closure_artifacts.push(artifact),
                Ok(None) | Err(_) => failed = true,
            }
        }
        if failed {
            let _ = transition_stash(&state, world.as_ref(), vset, incarnation).await;
            return;
        }
    }
    let record_bytes = record.encode(vset);
    if wait_commit_ack(
        &state,
        world.as_ref(),
        target,
        vset,
        assignment_epoch,
        info,
        required,
        record_bytes.clone(),
        retry,
    )
    .await
    .is_err()
    {
        let _ = transition_stash(&state, world.as_ref(), vset, incarnation).await;
        return;
    }
    if vnode_authority_enabled && info.sync_covered_through > 0 {
        let Ok(closure) = (VnodeRecoveryClosure {
            record: record_bytes,
            artifacts: closure_artifacts,
        })
        .encode(vset) else {
            return;
        };
        if commit_active_vnode_quorum(
            &state,
            world.as_ref(),
            vset,
            info.sync_covered_through,
            closure,
        )
        .await
        .is_err()
        {
            return;
        }
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
    finish_primary_commit(&state, vset, incarnation, info, record);
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
        // acknowledged it. The primary may archive only a cut acknowledged
        // by the current assignment.
        vset_state.peer_committed = None;
        vset_state.peer_committed_record = None;
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
        vset_state.peer_committed_record = None;
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
    let peer_segments = vset_state
        .peer_committed_record
        .iter()
        .flat_map(|record| &record.files)
        .filter(|file| {
            file.identity.namespace_kind == crate::blx::NamespaceKind::Vset
                && file.identity.namespace_id == vset.0
        })
        .map(|file| {
            (
                file.identity.writer_fence,
                crate::types::SegId(file.identity.object_id),
            )
        })
        .collect::<BTreeSet<_>>();
    let required = record
        .files
        .iter()
        .filter(|file| {
            file.identity.namespace_kind == crate::blx::NamespaceKind::Vset
                && file.identity.namespace_id == vset.0
        })
        .filter_map(|file| {
            let segment = (
                file.identity.writer_fence,
                crate::types::SegId(file.identity.object_id),
            );
            (!vset_state.backed_segments.contains(&segment) && !peer_segments.contains(&segment))
                .then_some(ReplicaArtifact::Segment {
                    fence: segment.0,
                    seg: segment.1,
                })
        })
        .collect::<BTreeSet<_>>();
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
    record: JournalRecord,
) {
    let completed = {
        let mut host = state.borrow_mut();
        let authority_serving = host.vset_authorized(vset);
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
            vset_state.peer_committed_record = Some(record);
        }
        vset_state.peer_committed_through = vset_state
            .peer_committed_through
            .max(info.sync_covered_through);
        if !authority_serving {
            return;
        }
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
        host.schedule_vset(vset);
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
    let network_bytes = (bytes.len() as u64).saturating_mul(u64::from(attempts));
    let mut host = state.borrow_mut();
    host.counters.replica_network_bytes = host
        .counters
        .replica_network_bytes
        .saturating_add(network_bytes);
    let assignment = host
        .vsets
        .get(&vset)
        .and_then(|vset_state| vset_state.stash_assignment);
    if assignment.map(|assignment| assignment.transition_peer.unwrap_or(assignment.active_peer))
        != Some(target)
    {
        host.counters.replica_nonactive_bytes = host
            .counters
            .replica_nonactive_bytes
            .saturating_add(network_bytes);
    }
    if assignment.is_some_and(|assignment| assignment.transition_peer == Some(target)) {
        host.counters.replica_replacement_bytes = host
            .counters
            .replica_replacement_bytes
            .saturating_add(network_bytes);
    }
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
            authority: None,
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
