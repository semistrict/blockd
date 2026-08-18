use std::collections::BTreeSet;
use std::rc::Rc;

use blockd_exec::{FaultPoint, delay, fault_point, spawn};

use super::backup::claim_new_head_with_stash;
use super::capture::finish_creation;
use super::commit_active_vnode_quorum;
use super::ctx::{HostCtx, VolumeCtx, VolumeRun};
use super::peer_client::PeerRpcError;
use super::state::{ReplicaKey, SharedHost};
use crate::format::crc32c;
use crate::head::{HeadRecord, MAX_RETIRED_STASHES, RetiredStash, StashAssignment};
use crate::journal::{JournalRecord, VolumeConfig};
use crate::layout;
use crate::placement::rank_stash_candidates;
use crate::protocol::{
    AdminError, AdminResult, AdminSuccess, PeerMsg, ReplicaArtifact, ReplicaCommitInfo,
};
use crate::replica_spool::{
    seal_replica_commit, seal_verified_replica_artifact, verify_replica_artifact,
};
use crate::types::{HostId, VolumeId};
use crate::vnode_member::VnodeRecoveryClosure;
use crate::world::{AdminIo, BlobError, Blobs, GuestMem, Peers, Store, StoreError};

/// Rotation happens before an append. A single verified frame may therefore
/// exceed this bound, but existing bytes are never copied between generations.
pub const MAX_REPLICA_SPOOL_GENERATION_BYTES: u64 = 64 * 1024 * 1024;
const REPLICA_TRANSFER_CONCURRENCY: usize = 64;

struct ReplicaInbox<'a, W> {
    state: &'a SharedHost,
    world: &'a W,
    key: ReplicaKey,
}

impl<'a, W> ReplicaInbox<'a, W> {
    fn new(
        state: &'a SharedHost,
        world: &'a W,
        source: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
    ) -> Self {
        Self {
            state,
            world,
            key: ReplicaKey {
                source,
                volume,
                assignment_epoch,
            },
        }
    }

    fn authorized(&self) -> bool {
        let ReplicaKey {
            source,
            volume,
            assignment_epoch,
        } = self.key;
        if assignment_epoch == 0 {
            return false;
        }
        let authorized = {
            let host = self.state.borrow();
            let Ok(index) = usize::try_from(assignment_epoch - 1) else {
                return false;
            };
            host.config
                .replica_placement
                .iter()
                .chain(host.replica_placement_history.iter())
                .any(|placement| {
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
                        volume,
                        &placement.roster,
                    );
                    !candidates.is_empty()
                        && candidates[index % candidates.len()] == host.config.host
                })
        };
        if !authorized {
            return false;
        }
        let mut host = self.state.borrow_mut();
        let latest = host
            .replica_latest_epoch
            .entry((source, volume))
            .or_default();
        if assignment_epoch < *latest {
            return false;
        }
        *latest = (*latest).max(assignment_epoch);
        true
    }

    fn append_plan(&self, additional: u64) -> Option<(u64, bool)> {
        let host = self.state.borrow();
        let total = host
            .replicas
            .values()
            .map(|replica| replica.bytes)
            .fold(0u64, u64::saturating_add);
        if total.saturating_add(additional) > host.config.archive.spool_capacity_bytes {
            return None;
        }
        let replica = host.replicas.get(&self.key);
        let current_generation = replica.map_or(0, |replica| replica.current_generation);
        let current_file_bytes = replica.map_or(0, |replica| replica.current_file_bytes);
        let rotated = current_file_bytes != 0
            && current_file_bytes.saturating_add(additional) > MAX_REPLICA_SPOOL_GENERATION_BYTES;
        Some((
            current_generation.saturating_add(u64::from(rotated)),
            rotated,
        ))
    }
}

impl<W: Peers> ReplicaInbox<'_, W> {
    async fn status(&self) {
        let ReplicaKey {
            source,
            volume,
            assignment_epoch,
        } = self.key;
        if !self.authorized() {
            self.state.borrow_mut().counters.replica_rejected += 1;
            return;
        }
        let committed = self
            .state
            .borrow()
            .replicas
            .get(&self.key)
            .and_then(|replica| replica.committed);
        Peers::send(
            self.world,
            source,
            PeerMsg::ReplicaStatusReply {
                volume,
                assignment_epoch,
                committed,
            },
        )
        .await;
    }
}

#[allow(clippy::too_many_lines)]
pub async fn replica_message<W>(state: SharedHost, world: &W, from: HostId, message: PeerMsg)
where
    W: Blobs + Store + Peers + AdminIo,
{
    match message {
        PeerMsg::ReplicaPut {
            volume,
            assignment_epoch,
            artifact,
            checksum,
            bytes,
        } => {
            ReplicaInbox::new(&state, world, from, volume, assignment_epoch)
                .put(artifact, checksum, bytes)
                .await;
        }
        PeerMsg::ReplicaCommit {
            volume,
            assignment_epoch,
            info,
            required,
            record,
        } => {
            ReplicaInbox::new(&state, world, from, volume, assignment_epoch)
                .commit(info, required, record)
                .await;
        }
        PeerMsg::ReplicaStatus {
            volume,
            assignment_epoch,
        } => {
            ReplicaInbox::new(&state, world, from, volume, assignment_epoch)
                .status()
                .await;
        }
        PeerMsg::ReplicaRelease {
            volume,
            assignment_epoch,
            through,
        } => {
            ReplicaInbox::new(&state, world, from, volume, assignment_epoch)
                .release(through)
                .await;
        }
        PeerMsg::ReplicaPutAck {
            volume,
            assignment_epoch,
            artifact,
            checksum,
        } => {
            state.borrow_mut().peer_client.resolve_put(
                from,
                volume,
                assignment_epoch,
                artifact,
                checksum,
            );
        }
        PeerMsg::ReplicaCommitAck {
            volume,
            assignment_epoch,
            info,
        } => {
            state
                .borrow_mut()
                .peer_client
                .resolve_commit(from, volume, assignment_epoch, info);
        }
        PeerMsg::ReplicaStatusReply {
            volume,
            assignment_epoch,
            committed,
        } => {
            state.borrow_mut().peer_client.resolve_status(
                from,
                volume,
                assignment_epoch,
                committed,
            );
        }
        PeerMsg::ReplicaReleaseAck {
            volume,
            assignment_epoch,
            through,
        } => {
            let retired = {
                let mut host = state.borrow_mut();
                let Some(index) = host
                    .replica_releases
                    .iter()
                    .position(|release| *release == (from, volume, assignment_epoch, through))
                else {
                    return;
                };
                host.replica_releases.swap_remove(index);
                host.volumes.get(&volume).and_then(|volume_state| {
                    volume_state
                        .retired_stashes
                        .iter()
                        .copied()
                        .find(|retired| {
                            (retired.peer, retired.assignment_epoch, retired.through)
                                == (from, assignment_epoch, through)
                        })
                        .map(|retired| (volume_state.incarnation, retired))
                })
            };
            if let Some((incarnation, retired)) = retired {
                let _ = remove_retired_stash(&state, world, volume, incarnation, retired).await;
            }
        }
        _ => unreachable!("non-replica message"),
    }
}

#[allow(clippy::too_many_lines)]
async fn remove_retired_stash<W>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
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
            let volume_state = host
                .volumes
                .get(&volume)
                .filter(|volume_state| volume_state.incarnation == incarnation)?;
            if !volume_state.retired_stashes.contains(&retired) {
                return Some((None, None));
            }
            let remaining = volume_state
                .retired_stashes
                .iter()
                .copied()
                .filter(|entry| *entry != retired)
                .collect();
            Some((
                Some(volume_state.head_version?),
                Some(HeadRecord {
                    volume,
                    holder: host.config.host,
                    fence: volume_state.fence,
                    manifest: volume_state.backed,
                    stash: volume_state.stash_assignment,
                    retired_stashes: remaining,
                }),
            ))
        })() else {
            return false;
        };
        let (Some(expected), Some(head)) = (expected, head) else {
            return true;
        };
        let result = Store::put_cas(
            world,
            layout::head_key(volume),
            Some(expected),
            head.encode(),
        )
        .await;
        if let Ok(version) = result
            && !fault_point(FaultPoint::StoreUnknownResult)
        {
            let mut host = state.borrow_mut();
            let Some(volume_state) = host
                .volumes
                .get_mut(&volume)
                .filter(|volume_state| volume_state.incarnation == incarnation)
            else {
                return false;
            };
            volume_state.head_version = Some(version);
            volume_state
                .retired_stashes
                .clone_from(&head.retired_stashes);
            return true;
        }
        if matches!(result, Err(StoreError::TooLarge)) {
            return false;
        }
        state.borrow_mut().counters.store_retries += 1;
        let (version, bytes) = loop {
            match Store::get(world, &layout::head_key(volume)).await {
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
        let Ok(found) = HeadRecord::decode(volume, &bytes) else {
            return false;
        };
        let local = state.borrow().config.host;
        let Some(fence) = state
            .borrow()
            .volumes
            .get(&volume)
            .filter(|volume_state| volume_state.incarnation == incarnation)
            .map(|volume_state| volume_state.fence)
        else {
            return false;
        };
        if found.holder != local || found.fence != fence {
            state.borrow_mut().fail("replica release cleanup fenced");
            return false;
        }
        let removed = !found.retired_stashes.contains(&retired);
        if !state
            .borrow_mut()
            .adopt_assignment_from_head(volume, incarnation, version, found)
        {
            return false;
        }
        if removed {
            return true;
        }
    }
}

impl<W> ReplicaInbox<'_, W>
where
    W: Blobs + Peers + AdminIo,
{
    #[allow(clippy::too_many_lines)]
    async fn put(&self, artifact: ReplicaArtifact, checksum: u32, bytes: Vec<u8>) {
        let state = self.state;
        let world = self.world;
        let ReplicaKey {
            source,
            volume,
            assignment_epoch,
        } = self.key;
        if !self.authorized() || crc32c(&bytes) != checksum {
            state.borrow_mut().counters.replica_rejected += 1;
            return;
        }
        let Ok(frame) = seal_verified_replica_artifact(
            source,
            volume,
            assignment_epoch,
            artifact,
            checksum,
            &bytes,
        ) else {
            state.borrow_mut().counters.replica_rejected += 1;
            return;
        };
        let key = self.key;
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
                        volume,
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
        let Some((generation, rotated)) = self.append_plan(frame.len() as u64) else {
            state.borrow_mut().counters.replica_capacity_backpressure += 1;
            return;
        };
        let spool_name =
            layout::replica_spool_generation_blob(source, volume, assignment_epoch, generation);
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
                volume,
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
                    volume,
                    assignment_epoch,
                    artifact,
                    checksum,
                },
            )
            .await;
        }
    }
}

impl<W> ReplicaInbox<'_, W>
where
    W: Blobs + Store + Peers + AdminIo,
{
    #[allow(clippy::too_many_lines)]
    async fn commit(
        &self,
        info: ReplicaCommitInfo,
        required: Vec<ReplicaArtifact>,
        record: Vec<u8>,
    ) {
        let state = self.state;
        let world = self.world;
        let ReplicaKey {
            source,
            volume,
            assignment_epoch,
        } = self.key;
        if !self.authorized() {
            state.borrow_mut().counters.replica_rejected += 1;
            return;
        }
        let key = self.key;
        let complete = required.iter().all(|artifact| {
            state
                .borrow()
                .replicas
                .get(&key)
                .is_some_and(|replica| replica.artifacts.contains_key(artifact))
        });
        let Ok(frame) =
            seal_replica_commit(source, volume, assignment_epoch, info, &required, &record)
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
                    volume,
                    assignment_epoch,
                    info,
                },
            )
            .await;
            return;
        }
        if known.is_some_and(|known| info <= known) {
            state.borrow_mut().counters.replica_rejected += 1;
            return;
        }
        let Some((generation, rotated)) = self.append_plan(frame.len() as u64) else {
            state.borrow_mut().counters.replica_capacity_backpressure += 1;
            return;
        };
        let spool_name =
            layout::replica_spool_generation_blob(source, volume, assignment_epoch, generation);
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
                volume,
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
                    volume,
                    assignment_epoch,
                    info,
                },
            )
            .await;
        }
    }
}

impl<W: Blobs + Peers> ReplicaInbox<'_, W> {
    async fn release(&self, through: ReplicaCommitInfo) {
        let state = self.state;
        let world = self.world;
        let ReplicaKey {
            source,
            volume,
            assignment_epoch,
        } = self.key;
        if !self.authorized() {
            state.borrow_mut().counters.replica_rejected += 1;
            return;
        }
        let key = self.key;
        if !state.borrow().replicas.contains_key(&key) {
            Peers::send(
                world,
                source,
                PeerMsg::ReplicaReleaseAck {
                    volume,
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
            .is_some_and(|committed| committed > through);
        if superseded {
            Peers::send(
                world,
                source,
                PeerMsg::ReplicaReleaseAck {
                    volume,
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
                .is_some_and(|committed| committed <= through)
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
                layout::replica_spool_generation_blob(source, volume, assignment_epoch, generation)
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
                volume,
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
                    volume,
                    assignment_epoch,
                    through,
                },
            )
            .await;
        }
    }
}

pub async fn retry_replica_releases<W: Peers + 'static>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
) {
    HostCtx::new(state, world)
        .volume(volume)
        .retry_releases()
        .await;
}

impl<W: Peers + 'static> VolumeCtx<W> {
    pub(super) async fn retry_releases(&self) {
        let state = self.host().state();
        let world = self.host().world();
        let volume = self.id();
        let releases = {
            let mut host = state.borrow_mut();
            let Some(volume_state) = host.volumes.get(&volume) else {
                return;
            };
            let Some(info) = volume_state.peer_published else {
                return;
            };
            let published =
                volume_state.peer_published == Some(info) && volume_state.backed.is_some();
            if !published {
                return;
            }
            let mut discovered = Vec::new();
            if let Some(assignment) = volume_state.stash_assignment {
                discovered.push((
                    assignment.active_peer,
                    volume,
                    assignment.active_assignment_epoch,
                    info,
                ));
            }
            discovered.extend(
                volume_state
                    .retired_stashes
                    .iter()
                    .filter(|retired| retired.through <= info)
                    .map(|retired| {
                        (
                            retired.peer,
                            volume,
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
                .filter(|(_, owner, _, _)| *owner == volume)
                .collect::<Vec<_>>()
        };
        for (peer, owner, assignment_epoch, through) in releases {
            Peers::send(
                world.as_ref(),
                peer,
                PeerMsg::ReplicaRelease {
                    volume: owner,
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
                        volume: owner,
                        assignment_epoch,
                        through,
                    },
                )
                .await;
            }
        }
    }
}

pub async fn create_peer_stashed<W>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
    config: VolumeConfig,
) -> Option<AdminResult>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world)
        .volume(volume)
        .create(config)
        .await
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    pub(super) async fn create(&self, config: VolumeConfig) -> Option<AdminResult> {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        if state.borrow().volumes.contains_key(&volume) {
            return Some(Err(AdminError::Busy));
        }
        let Some(stash) = initial_stash(&state, volume) else {
            return Some(Err(AdminError::Unavailable));
        };
        let incarnation = state.borrow_mut().insert_fresh(volume, config);
        let _ = fault_point(FaultPoint::AssignmentCasRace);
        let Some(fence) =
            claim_new_head_with_stash(&state, world.as_ref(), volume, incarnation, Some(stash))
                .await
        else {
            state.borrow_mut().volumes.remove(&volume);
            return Some(Err(AdminError::Rejected));
        };
        {
            let mut host = state.borrow_mut();
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.fence = fence;
            volume_state.head_version = Some(fence);
            volume_state.stash_assignment = Some(stash);
            host.counters.assignment_claims += 1;
        }
        if !finish_creation(Rc::clone(&state), world.as_ref(), volume, incarnation).await {
            state
                .borrow_mut()
                .fail("peer-stashed journal creation failed");
            return None;
        }
        Some(Ok(AdminSuccess::VolumeCreated { volume }))
    }
}

struct ReplicationLease {
    state: SharedHost,
    volume: VolumeId,
    incarnation: u64,
}

struct ReplicaLink<W> {
    run: VolumeRun<W>,
    target: HostId,
    assignment_epoch: u64,
}

struct ReplicationRun<W> {
    link: ReplicaLink<W>,
    _lease: ReplicationLease,
}

impl<W> ReplicationRun<W> {
    fn new(run: VolumeRun<W>, target: HostId, assignment_epoch: u64) -> Self {
        let state = Rc::clone(run.volume().host().state());
        let volume = run.volume().id();
        let incarnation = run.incarnation();
        Self {
            link: ReplicaLink {
                run,
                target,
                assignment_epoch,
            },
            _lease: ReplicationLease {
                state,
                volume,
                incarnation,
            },
        }
    }
}

impl<W> Clone for ReplicaLink<W> {
    fn clone(&self) -> Self {
        Self {
            run: self.run.clone(),
            target: self.target,
            assignment_epoch: self.assignment_epoch,
        }
    }
}

impl<W> ReplicaLink<W> {
    fn state(&self) -> &SharedHost {
        self.run.volume().host().state()
    }

    fn world(&self) -> &W {
        self.run.volume().host().world().as_ref()
    }

    fn volume(&self) -> VolumeId {
        self.run.volume().id()
    }
}

impl Drop for ReplicationLease {
    fn drop(&mut self) {
        if let Some(volume_state) = self
            .state
            .borrow_mut()
            .volumes
            .get_mut(&self.volume)
            .filter(|volume_state| volume_state.incarnation == self.incarnation)
        {
            volume_state.replicating_blx_files.clear();
            volume_state.operations.finish_replication();
        }
        self.state.borrow_mut().schedule_volume(self.volume);
    }
}

#[allow(clippy::too_many_lines)]
pub async fn replicate_latest<W>(state: SharedHost, world: Rc<W>, volume: VolumeId)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world).volume(volume).replicate().await;
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    pub(super) async fn replicate(&self) {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        let Some((incarnation, target, assignment_epoch, record)) = ({
            let mut host = state.borrow_mut();
            host.volumes.get_mut(&volume).and_then(|volume_state| {
                let record = volume_state.best_record.clone()?;
                let stash = volume_state.stash_assignment?;
                let required = record.commit_info();
                let needed = volume_state
                    .peer_committed
                    .is_none_or(|committed| committed < required);
                if needed && volume_state.operations.try_start_replication() {
                    volume_state.replicating_blx_files = record
                        .files
                        .iter()
                        .filter_map(|file| {
                            (file.identity.namespace_kind == crate::blx::NamespaceKind::Volume
                                && file.identity.namespace_id == volume.0)
                                .then_some(file.identity)
                        })
                        .collect();
                    Some((
                        volume_state.incarnation,
                        stash.transition_peer.unwrap_or(stash.active_peer),
                        stash.assignment_epoch,
                        record,
                    ))
                } else {
                    None
                }
            })
        }) else {
            return;
        };
        let run = ReplicationRun::new(self.pin(incarnation), target, assignment_epoch);
        let info = record.commit_info();
        let Ok(status) = run.link.status().await else {
            let _ = transition_stash(&state, world.as_ref(), volume, incarnation).await;
            return;
        };
        let vnode_authority_enabled = state
            .borrow()
            .config
            .replica_placement
            .as_ref()
            .and_then(|placement| placement.authority)
            .is_some();
        if !vnode_authority_enabled && status.is_some_and(|committed| committed >= info) {
            run.link.finish_primary_commit(info, record.clone());
            return;
        }
        if fault_point(FaultPoint::CrashPrimaryBeforeClosureCapture) {
            state.borrow_mut().fail("injected primary crash");
            return;
        }
        let Some(required) = replica_closure(&state, volume, incarnation, &record) else {
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
                let link = run.link.clone();
                transfers.push(spawn(async move {
                    let name = match artifact {
                        ReplicaArtifact::Blx { fence, object } => {
                            layout::blx_blob(volume, fence, object)
                        }
                    };
                    let Ok(Some(bytes)) = Blobs::read(link.world(), &name).await else {
                        return None;
                    };
                    if verify_replica_artifact(volume, artifact, &bytes).is_err() {
                        return None;
                    }
                    if fault_point(FaultPoint::CrashPrimaryDuringArtifactTransfer) {
                        link.state().borrow_mut().fail("injected primary crash");
                        return None;
                    }
                    {
                        let mut host = link.state().borrow_mut();
                        host.counters.replica_logical_bytes = host
                            .counters
                            .replica_logical_bytes
                            .saturating_add(bytes.len() as u64);
                    }
                    let checksum = crc32c(&bytes);
                    link.put(artifact, checksum, bytes.clone())
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
                let _ = transition_stash(&state, world.as_ref(), volume, incarnation).await;
                return;
            }
        }
        let record_bytes = record.encode(volume);
        if run
            .link
            .commit(info, required, record_bytes.clone())
            .await
            .is_err()
        {
            let _ = transition_stash(&state, world.as_ref(), volume, incarnation).await;
            return;
        }
        if vnode_authority_enabled && info.sync_covered_through > 0 {
            let Ok(closure) = (VnodeRecoveryClosure {
                record: record_bytes,
                artifacts: closure_artifacts,
            })
            .encode(volume) else {
                return;
            };
            if commit_active_vnode_quorum(
                &state,
                world.as_ref(),
                volume,
                info.sync_covered_through,
                closure,
            )
            .await
            .is_err()
            {
                return;
            }
        }
        let transitioning = state
            .borrow()
            .volumes
            .get(&volume)
            .is_some_and(|volume_state| {
                volume_state.stash_assignment.is_some_and(|stash| {
                    stash.assignment_epoch == assignment_epoch
                        && stash.transition_peer == Some(target)
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
                volume,
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
        run.link.finish_primary_commit(info, record);
        if fault_point(FaultPoint::CrashPrimaryAfterSyncOk) {
            state.borrow_mut().fail("injected primary crash");
        }
    }
}

async fn transition_stash<W>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    incarnation: u64,
) -> bool
where
    W: Store + AdminIo,
{
    let Some(proposal) = (|| {
        let host = state.borrow();
        let volume_state = host
            .volumes
            .get(&volume)
            .filter(|volume_state| volume_state.incarnation == incarnation)?;
        if volume_state.retired_stashes.len() >= MAX_RETIRED_STASHES {
            return None;
        }
        let current = volume_state.stash_assignment?;
        if current.transition_peer.is_some() {
            return Some(current);
        }
        let placement = host.config.replica_placement.as_ref()?;
        let candidates = rank_stash_candidates(
            placement.membership_epoch,
            host.config.host,
            placement.local_failure_domain,
            volume,
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
    let retired = state.borrow().volumes[&volume].retired_stashes.clone();
    cas_assignment(state, world, volume, incarnation, proposal, retired, None).await
}

#[allow(clippy::too_many_arguments)]
async fn activate_stash<W>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
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
        let volume_state = host
            .volumes
            .get(&volume)
            .filter(|volume_state| volume_state.incarnation == incarnation)?;
        let current = volume_state.stash_assignment?;
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
        let mut retired = volume_state.retired_stashes.clone();
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
        volume,
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
    volume: VolumeId,
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
            let volume_state = host
                .volumes
                .get(&volume)
                .filter(|volume_state| volume_state.incarnation == incarnation)?;
            Some((
                volume_state.head_version?,
                HeadRecord {
                    volume,
                    holder: host.config.host,
                    fence: volume_state.fence,
                    manifest: volume_state.backed,
                    stash: Some(assignment),
                    retired_stashes: retired_stashes.clone(),
                },
                volume_state.stash_assignment,
                volume_state.retired_stashes.clone(),
            ))
        })() else {
            return false;
        };
        let _ = fault_point(FaultPoint::AssignmentCasRace);
        let result = Store::put_cas(
            world,
            layout::head_key(volume),
            Some(expected),
            head.encode(),
        )
        .await;
        if let Ok(version) = result
            && !fault_point(FaultPoint::StoreUnknownResult)
        {
            if crash_after.is_some_and(fault_point) {
                state.borrow_mut().fail("injected primary crash");
                return false;
            }
            return state.borrow_mut().adopt_assignment(
                volume,
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
            match Store::get(world, &layout::head_key(volume)).await {
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
        let Ok(found) = HeadRecord::decode(volume, &bytes) else {
            return false;
        };
        let local = state.borrow().config.host;
        let Some(fence) = state
            .borrow()
            .volumes
            .get(&volume)
            .filter(|volume_state| volume_state.incarnation == incarnation)
            .map(|volume_state| volume_state.fence)
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
            return state.borrow_mut().adopt_assignment(
                volume,
                incarnation,
                version,
                assignment,
                retired_stashes,
            );
        }
        if found.stash == current_assignment && found.retired_stashes == current_retired {
            {
                let mut host = state.borrow_mut();
                let Some(volume_state) = host
                    .volumes
                    .get_mut(&volume)
                    .filter(|volume_state| volume_state.incarnation == incarnation)
                else {
                    return false;
                };
                volume_state.head_version = Some(version);
                volume_state.backed = found.manifest;
            }
            delay(retry).await;
            continue;
        }
        return state
            .borrow_mut()
            .adopt_assignment_from_head(volume, incarnation, version, found);
    }
}

pub(super) fn initial_stash(state: &SharedHost, volume: VolumeId) -> Option<StashAssignment> {
    let host = state.borrow();
    let placement = host.config.replica_placement.as_ref()?;
    let target = rank_stash_candidates(
        placement.membership_epoch,
        host.config.host,
        placement.local_failure_domain,
        volume,
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
    volume: VolumeId,
    incarnation: u64,
    record: &JournalRecord,
) -> Option<Vec<ReplicaArtifact>> {
    let host = state.borrow();
    let volume_state = host
        .volumes
        .get(&volume)
        .filter(|volume_state| volume_state.incarnation == incarnation)?;
    let peer_blx = volume_state
        .peer_committed_record
        .iter()
        .flat_map(|record| &record.files)
        .filter(|file| {
            file.identity.namespace_kind == crate::blx::NamespaceKind::Volume
                && file.identity.namespace_id == volume.0
        })
        .map(|file| file.identity)
        .collect::<BTreeSet<_>>();
    let required = record
        .files
        .iter()
        .filter(|file| {
            file.identity.namespace_kind == crate::blx::NamespaceKind::Volume
                && file.identity.namespace_id == volume.0
        })
        .filter_map(|file| {
            let identity = file.identity;
            (!volume_state.backed_blx_files.contains(&identity) && !peer_blx.contains(&identity))
                .then_some(ReplicaArtifact::Blx {
                    fence: identity.writer_fence,
                    object: crate::types::ObjectId(identity.object_id),
                })
        })
        .collect::<BTreeSet<_>>();
    Some(required.into_iter().collect())
}

impl<W: Peers> ReplicaLink<W> {
    fn finish_primary_commit(&self, info: ReplicaCommitInfo, record: JournalRecord) {
        let completed = self.state().borrow_mut().finish_primary_commit(
            self.volume(),
            self.run.incarnation(),
            info,
            record,
        );
        for sync in completed {
            sync.resolve(true);
        }
    }

    async fn status(&self) -> Result<Option<ReplicaCommitInfo>, PeerRpcError> {
        let state = self.state();
        let client = state.borrow().peer_client.clone();
        let (committed, _) = client
            .replica_status(
                self.world(),
                self.target,
                self.volume(),
                self.assignment_epoch,
            )
            .await?;
        let _ = fault_point(FaultPoint::StatusReconciliation);
        Ok(committed)
    }

    async fn put(
        &self,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
    ) -> Result<(), PeerRpcError> {
        let state = self.state();
        let client = state.borrow().peer_client.clone();
        let result = client
            .replica_put(
                self.world(),
                self.target,
                self.volume(),
                self.assignment_epoch,
                artifact,
                checksum,
                bytes.clone(),
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
            .volumes
            .get(&self.volume())
            .and_then(|volume_state| volume_state.stash_assignment);
        if assignment.map(|assignment| assignment.transition_peer.unwrap_or(assignment.active_peer))
            != Some(self.target)
        {
            host.counters.replica_nonactive_bytes = host
                .counters
                .replica_nonactive_bytes
                .saturating_add(network_bytes);
        }
        if assignment.is_some_and(|assignment| assignment.transition_peer == Some(self.target)) {
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

    async fn commit(
        &self,
        info: ReplicaCommitInfo,
        required: Vec<ReplicaArtifact>,
        record: Vec<u8>,
    ) -> Result<(), PeerRpcError> {
        let state = self.state();
        let client = state.borrow().peer_client.clone();
        let result = client
            .replica_commit(
                self.world(),
                self.target,
                self.volume(),
                self.assignment_epoch,
                info,
                required,
                record,
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
            volume: VolumeId(3),
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
        assert_eq!(
            ReplicaInbox::new(&state, &(), key.source, key.volume, key.assignment_epoch)
                .append_plan(1),
            Some((1, true))
        );
    }

    #[test]
    fn assignment_change_forgets_commit_owned_by_previous_peer() {
        let state = test_state(HostId(1));
        let volume = VolumeId(3);
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
            let mut volume_state =
                super::super::state::VolumeState::fresh(VolumeConfig::data(4), 1);
            volume_state.stash_assignment = Some(current);
            volume_state.peer_committed = Some(committed);
            volume_state.peer_committed_through = committed.sync_covered_through;
            host.volumes.insert(volume, volume_state);
        }

        assert!(
            state
                .borrow_mut()
                .adopt_assignment(volume, 1, 5, next, Vec::new())
        );

        let host = state.borrow();
        let volume_state = &host.volumes[&volume];
        assert_eq!(volume_state.peer_committed, None);
        assert_eq!(
            volume_state.peer_committed_through,
            committed.sync_covered_through
        );
    }

    #[test]
    fn append_plan_enforces_configured_host_capacity() {
        let state = test_state(HostId(1));
        state.borrow_mut().config.archive.spool_capacity_bytes = 100;
        let key = ReplicaKey {
            source: HostId(2),
            volume: VolumeId(3),
            assignment_epoch: 1,
        };
        state.borrow_mut().replicas.insert(
            key,
            super::super::state::ReplicaState {
                bytes: 100,
                ..Default::default()
            },
        );
        assert_eq!(
            ReplicaInbox::new(&state, &(), key.source, key.volume, key.assignment_epoch)
                .append_plan(1),
            None
        );
    }

    #[test]
    fn request_authority_rejects_an_epoch_below_the_recovered_floor() {
        let source = HostId(10);
        let volume = VolumeId(12);
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
        let ranked = rank_stash_candidates(5, source, 1, volume, &roster);
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
            .insert((source, volume), 2);
        assert!(!ReplicaInbox::new(&state, &(), source, volume, 1).authorized());
        assert_eq!(
            state.borrow().replica_latest_epoch,
            BTreeMap::from([((source, volume), 2)])
        );
    }
}
