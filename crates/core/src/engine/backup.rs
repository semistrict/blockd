use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::delay;

use super::SharedHost;
use super::ctx::{HostCtx, VolumeCtx, VolumeRun};
use super::recovery_policy::recovery_metadata;
use super::store_retry::{
    read as get_retry, write as put_retry, write_immutable as put_immutable_retry,
};
use crate::blx::{
    BatchMeta, BlxCompactor, BlxObject, MAX_OVERLAPPING_FILES, NamespaceKind,
    compaction_object_id_start,
};
use crate::format::checksum64;
use crate::head::{HeadRecord, ManifestPtr, StashAssignment};
use crate::journal::JournalRecord;
use crate::layout;
use crate::manifest::{
    BaseManifest, BaseRef, BaseRoot, CompleteFileList, Manifest, ManifestClosure, ObjectRef,
    bound_manifest, max_object_overlap, validate_file_state_chain, validate_object_refs,
};
use crate::protocol::{AdminEvent, StoreFault};
use crate::types::{JournalSeq, ObjectId, VolumeId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

pub(super) async fn claim_new_head_with_stash<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    incarnation: u64,
    stash: Option<StashAssignment>,
) -> Option<u64> {
    let head = HeadRecord {
        volume,
        holder: state.borrow().config.host,
        fence: 0,
        manifest: None,
        stash,
        retired_stashes: Vec::new(),
    };
    let retry = state.borrow().config.backup_retry;
    loop {
        match Store::put_cas(world, layout::head_key(volume), None, head.encode()).await {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                state.borrow_mut().counters.assignment_claim_conflicts += 1;
                match Store::get(world, &layout::head_key(volume)).await {
                    Ok(Some((version, bytes))) => {
                        let ours = HeadRecord::decode(volume, &bytes).is_ok_and(|found| {
                            found.holder == state.borrow().config.host
                                && found.fence == 0
                                && found.manifest.is_none()
                                && found.stash == stash
                        });
                        if ours {
                            return Some(version);
                        }
                    }
                    Ok(None)
                    | Err(
                        StoreError::Fault(StoreFault::Unavailable | StoreFault::CasConflict { .. })
                        | StoreError::TooLarge,
                    ) => {}
                }
                return None;
            }
            Err(StoreError::TooLarge) => return None,
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world, &layout::head_key(volume)).await {
                    Ok(Some((version, bytes))) => {
                        let recovered = HeadRecord::decode(volume, &bytes).ok();
                        let ours = recovered.is_some_and(|found| {
                            found.holder == state.borrow().config.host
                                && found.fence == 0
                                && found.manifest.is_none()
                                && found.stash == stash
                        });
                        if ours
                            && state
                                .borrow()
                                .volumes
                                .get(&volume)
                                .is_some_and(|volume| volume.incarnation == incarnation)
                        {
                            return Some(version);
                        }
                        return None;
                    }
                    Ok(None) | Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        delay(retry).await;
                    }
                    Err(
                        StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge,
                    ) => return None,
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
pub async fn publish_latest<W>(state: SharedHost, world: Rc<W>, volume: VolumeId)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world).volume(volume).publish().await;
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    pub(super) async fn publish(self) {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        let Some((incarnation, retry)) = ({
            let mut host = state.borrow_mut();
            let retry = host.config.backup_retry;
            host.volumes.get_mut(&volume).and_then(|volume_state| {
                volume_state
                    .operations
                    .try_start_publication()
                    .then_some((volume_state.incarnation, retry))
            })
        }) else {
            return;
        };
        let run = self.pin(incarnation);
        let lease = PublishLease::new(&state, volume, run.incarnation());
        'publish: while let Some(snapshot) = publish_snapshot(&state, volume, incarnation) {
            if run.interrupted() {
                break;
            }
            {
                let mut host = state.borrow_mut();
                let Some(volume_state) = host.volume_at_mut(volume, incarnation) else {
                    return;
                };
                volume_state
                    .publishing_blx_files
                    .clone_from(&snapshot.blx_files);
            }
            let already_published = snapshot.published_commit.is_some_and(|published| {
                (published.writer_fence, published.seq)
                    >= (snapshot.commit.writer_fence, snapshot.commit.seq)
                    && published.sync_covered_through >= snapshot.commit.sync_covered_through
            }) || (snapshot.published_commit.is_none()
                && snapshot
                    .backed
                    .is_some_and(|backed| backed.capture_seq >= snapshot.record.capture_seq));
            let compact_only = already_published
                && archive_needs_compaction(&state, world.as_ref(), volume, snapshot.backed).await;
            if run.interrupted() {
                break;
            }
            if already_published && !compact_only {
                break;
            }
            let Some(prepared) = run.prepare_archive(&snapshot, retry, compact_only).await else {
                if run.interrupted() {
                    break;
                }
                delay(retry).await;
                continue 'publish;
            };
            if run.interrupted() {
                let _ = Store::delete(world.as_ref(), &prepared.pending_key).await;
                break;
            }
            match publish_head(
                &state,
                world.as_ref(),
                volume,
                incarnation,
                prepared.pointer,
                retry,
            )
            .await
            {
                PublishHead::Published(version) => {
                    {
                        let mut host = state.borrow_mut();
                        let Some(volume_state) = host.volume_at_mut(volume, incarnation) else {
                            return;
                        };
                        volume_state.head_version = Some(version);
                        volume_state.backed = Some(prepared.pointer);
                        volume_state
                            .backed_blx_files
                            .extend(snapshot.blx_files.iter().copied());
                        if volume_state.stash_assignment.is_some() {
                            volume_state.peer_published = Some(snapshot.commit);
                        }
                        host.counters.manifests_published += 1;
                    }
                    let _ = Store::delete(world.as_ref(), &prepared.pending_key).await;
                    run.volume().retry_releases().await;
                }
                PublishHead::Fenced => {
                    fence_volume(&state, world.as_ref(), volume, Some(incarnation)).await;
                    return;
                }
                PublishHead::Fatal => {
                    state.borrow_mut().fail("head publication failed");
                    return;
                }
            }
        }
        lease.commit();
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn reconcile_backed_recovery_event<W>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
) -> Option<AdminEvent>
where
    W: Store + GuestMem + AdminIo + 'static,
{
    enum RecoveryDecision {
        Fence,
        Outbound,
        Ready(crate::protocol::Verdict),
    }

    let retry = state.borrow().config.backup_retry;
    loop {
        match Store::get(world.as_ref(), &layout::head_key(volume)).await {
            Ok(Some((version, bytes))) => {
                let Ok(head) = HeadRecord::decode(volume, &bytes) else {
                    fence_volume(&state, world.as_ref(), volume, None).await;
                    return None;
                };
                let Some((archive_objects, archive_base)) =
                    load_archive_closure(&state, world.as_ref(), volume, head.manifest).await
                else {
                    fence_volume(&state, world.as_ref(), volume, None).await;
                    return None;
                };
                if head.stash.is_none() {
                    let local_host = state.borrow().config.host;
                    let Some(stash) = super::replica::initial_stash(&state, volume) else {
                        fence_volume(&state, world.as_ref(), volume, None).await;
                        return None;
                    };
                    if head.holder != local_host {
                        fence_volume(&state, world.as_ref(), volume, None).await;
                        return None;
                    }
                    let (incarnation, fence) = state
                        .borrow()
                        .volumes
                        .get(&volume)
                        .map(|volume_state| (volume_state.incarnation, volume_state.fence))?;
                    let upgraded = HeadRecord {
                        volume,
                        holder: local_host,
                        fence,
                        manifest: head.manifest,
                        stash: Some(stash),
                        retired_stashes: head.retired_stashes.clone(),
                    };
                    match Store::put_cas(
                        world.as_ref(),
                        layout::head_key(volume),
                        Some(version),
                        upgraded.encode(),
                    )
                    .await
                    {
                        Ok(upgraded_version) => {
                            let peer_published = recover_peer_published(
                                &state,
                                world.as_ref(),
                                volume,
                                upgraded.manifest,
                                retry,
                            )
                            .await;
                            let verdict = {
                                let mut host = state.borrow_mut();
                                let volume_state =
                                    host.volumes.get_mut(&volume).filter(|volume_state| {
                                        volume_state.incarnation == incarnation
                                    })?;
                                volume_state.head_version = Some(upgraded_version);
                                volume_state.backed = upgraded.manifest;
                                volume_state.install_archive_closure(
                                    volume,
                                    &archive_objects,
                                    archive_base,
                                );
                                volume_state.peer_published = peer_published;
                                if let Some(manifest) = upgraded.manifest {
                                    volume_state
                                        .store_manifests
                                        .insert((manifest.fence, manifest.seq));
                                }
                                volume_state.stash_assignment = upgraded.stash;
                                volume_state
                                    .retired_stashes
                                    .clone_from(&upgraded.retired_stashes);
                                if volume_state.outbound.is_some() {
                                    None
                                } else {
                                    volume_state.ready = true;
                                    volume_state.operations.take_recovery()
                                }
                            };
                            state.borrow_mut().schedule_volume(volume);
                            return verdict
                                .map(|verdict| AdminEvent::VolumeRecovered { volume, verdict });
                        }
                        Err(StoreError::Fault(StoreFault::CasConflict { .. })) => continue,
                        Err(StoreError::Fault(StoreFault::Unavailable)) => {
                            state.borrow_mut().counters.store_retries += 1;
                            delay(retry).await;
                            continue;
                        }
                        Err(StoreError::TooLarge) => {
                            fence_volume(&state, world.as_ref(), volume, None).await;
                            return None;
                        }
                    }
                }
                let expected_membership = state
                    .borrow()
                    .config
                    .replica_placement
                    .as_ref()
                    .map(|placement| placement.membership_epoch);
                if head.stash.map(|stash| stash.membership_epoch) != expected_membership {
                    fence_volume(&state, world.as_ref(), volume, None).await;
                    return None;
                }
                let publication_is_ours = {
                    let host = state.borrow();
                    let local_host = host.config.host;
                    host.volumes.get(&volume).is_some_and(|volume_state| {
                        let local = volume_state
                            .best_record
                            .as_ref()
                            .map_or((0, JournalSeq(0)), |record| {
                                (record.capture_seq, record.seq)
                            });
                        let behind = head.manifest.is_some_and(|manifest| {
                            (manifest.capture_seq, manifest.journal_seq) > local
                        });
                        head.holder == local_host
                            && (head.fence == 0 || head.fence == volume_state.fence)
                            && !behind
                    })
                };
                let peer_published = if publication_is_ours {
                    recover_peer_published(&state, world.as_ref(), volume, head.manifest, retry)
                        .await
                } else {
                    None
                };
                let decision = {
                    let mut host = state.borrow_mut();
                    let local_host = host.config.host;
                    let volume_state = host.volumes.get_mut(&volume)?;
                    let local = volume_state
                        .best_record
                        .as_ref()
                        .map_or((0, JournalSeq(0)), |record| {
                            (record.capture_seq, record.seq)
                        });
                    let behind = head.manifest.is_some_and(|manifest| {
                        (manifest.capture_seq, manifest.journal_seq) > local
                    });
                    if head.holder != local_host
                        || (head.fence != 0 && head.fence != volume_state.fence)
                        || behind
                    {
                        RecoveryDecision::Fence
                    } else {
                        volume_state.head_version = Some(version);
                        volume_state.backed = head.manifest;
                        volume_state.install_archive_closure(
                            volume,
                            &archive_objects,
                            archive_base,
                        );
                        volume_state.peer_published = peer_published;
                        if let Some(manifest) = head.manifest {
                            volume_state
                                .store_manifests
                                .insert((manifest.fence, manifest.seq));
                        }
                        volume_state.stash_assignment = head.stash;
                        volume_state.retired_stashes = head.retired_stashes;
                        if volume_state.outbound.is_some() {
                            RecoveryDecision::Outbound
                        } else {
                            volume_state.ready = true;
                            RecoveryDecision::Ready(
                                volume_state
                                    .operations
                                    .take_recovery()
                                    .expect("recovery verdict retained"),
                            )
                        }
                    }
                };
                state.borrow_mut().schedule_volume(volume);
                match decision {
                    RecoveryDecision::Fence => {
                        fence_volume(&state, world.as_ref(), volume, None).await;
                    }
                    RecoveryDecision::Outbound => {}
                    RecoveryDecision::Ready(verdict) => {
                        return Some(AdminEvent::VolumeRecovered { volume, verdict });
                    }
                }
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Ok(None) => {
                let local_host = state.borrow().config.host;
                let Some(stash) = super::replica::initial_stash(&state, volume) else {
                    fence_volume(&state, world.as_ref(), volume, None).await;
                    return None;
                };
                let head = HeadRecord {
                    volume,
                    holder: local_host,
                    fence: 0,
                    manifest: None,
                    stash: Some(stash),
                    retired_stashes: Vec::new(),
                };
                match Store::put_cas(
                    world.as_ref(),
                    layout::head_key(volume),
                    None,
                    head.encode(),
                )
                .await
                {
                    Ok(version) => {
                        let verdict = {
                            let mut host = state.borrow_mut();
                            let verdict = {
                                let volume_state = host.volumes.get_mut(&volume)?;
                                volume_state.fence = version;
                                volume_state.head_version = Some(version);
                                volume_state.backed = None;
                                volume_state.stash_assignment = Some(stash);
                                volume_state.retired_stashes.clear();
                                if volume_state.outbound.is_some() {
                                    None
                                } else {
                                    volume_state.ready = true;
                                    volume_state.operations.take_recovery()
                                }
                            };
                            host.counters.assignment_claims += 1;
                            verdict
                        };
                        state.borrow_mut().schedule_volume(volume);
                        return verdict
                            .map(|verdict| AdminEvent::VolumeRecovered { volume, verdict });
                    }
                    Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                        state.borrow_mut().counters.assignment_claim_conflicts += 1;
                    }
                    Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        state.borrow_mut().counters.store_retries += 1;
                        delay(retry).await;
                    }
                    Err(StoreError::TooLarge) => {
                        fence_volume(&state, world.as_ref(), volume, None).await;
                        return None;
                    }
                }
            }
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                fence_volume(&state, world.as_ref(), volume, None).await;
                return None;
            }
        }
    }
}

async fn load_archive_closure<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    pointer: Option<ManifestPtr>,
) -> Option<(Vec<ObjectRef>, Option<BaseRef>)> {
    let Some(pointer) = pointer else {
        return Some((Vec::new(), None));
    };
    let (manifest, list) = load_archive_metadata(state, world, volume, Some(pointer)).await??;
    let mut objects = match manifest.base {
        None => Vec::new(),
        Some(reference) => {
            let root = BaseRoot {
                base_id: reference.base_id,
                manifest_id: reference.manifest_id,
                manifest_checksum: reference.manifest_checksum,
                post_state_checksum: reference.post_state_checksum,
            };
            let bytes = get_retry(
                state,
                world,
                &layout::base_manifest_key(reference.base_id, reference.manifest_id),
            )
            .await?;
            BaseManifest::decode(root, &bytes).ok()?.objects
        }
    };
    objects.extend(manifest.current_files(list.as_ref()).ok()?);
    validate_object_refs(&objects).ok()?;
    Some((objects, manifest.base))
}

async fn recover_peer_published<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    pointer: Option<ManifestPtr>,
    retry: u64,
) -> Option<crate::protocol::ReplicaCommitInfo> {
    let pointer = pointer?;
    loop {
        match Store::get(world, &pointer.manifest_key(volume)).await {
            Ok(Some((_, bytes))) => {
                if checksum64(&bytes) != pointer.checksum {
                    return None;
                }
                let manifest = Manifest::decode(volume, &bytes).ok()?;
                if (
                    manifest.writer_fence,
                    manifest.journal_seq,
                    manifest.archive_seq,
                ) != (pointer.fence, pointer.journal_seq.0, pointer.seq.0)
                {
                    return None;
                }
                return state
                    .borrow()
                    .volumes
                    .get(&volume)
                    .and_then(|volume_state| {
                        let record = volume_state.best_record.as_ref()?;
                        (manifest.journal_seq == record.seq.0
                            && manifest.capture_seq == record.capture_seq
                            && manifest.metadata_checksum == checksum64(&record.encode(volume)))
                        .then_some(crate::protocol::ReplicaCommitInfo {
                            writer_fence: record.fence,
                            seq: record.seq,
                            sync_covered_through: record.sync_covered_through,
                        })
                    });
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Ok(None)
            | Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
        }
    }
}

struct PublishSnapshot {
    record: JournalRecord,
    base: Option<BaseRef>,
    base_objects: Vec<ObjectRef>,
    blx_files: BTreeSet<crate::manifest::ObjectIdentity>,
    blx_refs: Vec<ObjectRef>,
    backed: Option<ManifestPtr>,
    published_commit: Option<crate::protocol::ReplicaCommitInfo>,
    commit: crate::protocol::ReplicaCommitInfo,
    state_checksum: u64,
}

fn publish_snapshot(
    state: &SharedHost,
    volume: VolumeId,
    incarnation: u64,
) -> Option<PublishSnapshot> {
    let host = state.borrow();
    let volume_state = host
        .volumes
        .get(&volume)
        .filter(|volume| volume.incarnation == incarnation)?;
    let record = if volume_state.stash_assignment.is_some() {
        volume_state.peer_committed_record.as_ref()?
    } else {
        volume_state.best_record.as_ref()?
    };
    let blx_refs = record
        .files
        .iter()
        .filter(|file| {
            file.identity.namespace_kind == NamespaceKind::Volume
                && file.identity.namespace_id == volume.0
        })
        .copied()
        .collect::<Vec<_>>();
    let blx_files = blx_refs
        .iter()
        .map(|file| file.identity)
        .collect::<BTreeSet<_>>();
    Some(PublishSnapshot {
        record: record.clone(),
        base: volume_state.archive_base,
        base_objects: volume_state
            .archive_objects
            .iter()
            .filter(|object| {
                object.identity.namespace_kind != NamespaceKind::Volume
                    || object.identity.namespace_id != volume.0
            })
            .copied()
            .collect(),
        blx_files,
        blx_refs,
        backed: volume_state.backed,
        published_commit: volume_state.peer_published,
        commit: crate::protocol::ReplicaCommitInfo {
            writer_fence: record.fence,
            seq: record.seq,
            sync_covered_through: record.sync_covered_through,
        },
        state_checksum: record.post_state_checksum,
    })
}

struct PreparedArchive {
    pointer: ManifestPtr,
    pending_key: String,
}

async fn archive_needs_compaction<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    pointer: Option<ManifestPtr>,
) -> bool {
    let Some(Some((manifest, list))) = load_archive_metadata(state, world, volume, pointer).await
    else {
        return false;
    };
    manifest
        .current_files(list.as_ref())
        .is_ok_and(|files| max_object_overlap(&files) >= MAX_OVERLAPPING_FILES)
}

fn source_object_is_covered(frontier: Option<(u64, u64)>, writer_fence: u64, max_seq: u64) -> bool {
    frontier.is_some_and(|(fence, seq)| writer_fence == fence && max_seq <= seq)
}

#[allow(clippy::too_many_lines)]
impl<W: Blobs + Store> VolumeRun<W> {
    async fn prepare_archive(
        &self,
        snapshot: &PublishSnapshot,
        retry: u64,
        force_compaction: bool,
    ) -> Option<PreparedArchive> {
        let state = self.volume().host().state();
        let world = self.volume().host().world().as_ref();
        let volume = self.volume().id();
        let previous = load_archive_metadata(state, world, volume, snapshot.backed).await?;
        if self.interrupted() {
            return None;
        }
        let previous_files = match &previous {
            None => Vec::new(),
            Some((manifest, list)) => manifest.current_files(list.as_ref()).ok()?,
        };
        let previous_source_frontier = previous
            .as_ref()
            .map(|(manifest, _)| (manifest.writer_fence, manifest.journal_seq));
        let mut current = previous_files
            .into_iter()
            .map(|object| (object.identity, object))
            .collect::<BTreeMap<_, _>>();
        for reference in &snapshot.blx_refs {
            if self.interrupted() {
                return None;
            }
            let identity = reference.identity;
            let fence = identity.writer_fence;
            let blx = ObjectId(identity.object_id);
            if current.contains_key(&identity) {
                continue;
            }
            if source_object_is_covered(
                previous_source_frontier,
                identity.writer_fence,
                reference.max_seq,
            ) {
                continue;
            }
            let bytes =
                read_blob_retry(world, &layout::blx_blob(volume, fence, blx), retry).await?;
            let object = BlxObject::open(&bytes).ok()?;
            if ObjectRef::from_blx(&object) != *reference {
                return None;
            }
            put_immutable_retry(state, world, identity.store_key(), bytes).await?;
            if self.interrupted() {
                return None;
            }
            current.insert(identity, *reference);
        }

        let mut current_files = current.into_values().collect::<Vec<_>>();
        let combined_valid = |own: &[ObjectRef]| {
            let mut combined = snapshot.base_objects.clone();
            combined.extend_from_slice(own);
            validate_object_refs(&combined).is_ok()
        };
        let base_checksum = snapshot.base.map_or(0, |base| base.post_state_checksum);
        if !combined_valid(&current_files)
            || validate_file_state_chain(base_checksum, snapshot.state_checksum, &current_files)
                .is_err()
            || (force_compaction && max_object_overlap(&current_files) >= MAX_OVERLAPPING_FILES)
        {
            // Compacted bytes are immutable and materialize one exact journal
            // state. A failed head publication may retry the same archive number
            // with a newer journal state, so the archive number cannot safely
            // identify these objects.
            let mut groups = BTreeMap::new();
            for reference in &current_files {
                let partition = reference.first_key.file_partition();
                if reference.last_key.file_partition() != partition {
                    return None;
                }
                groups
                    .entry(partition)
                    .or_insert_with(Vec::new)
                    .push(*reference);
            }
            let mut next_object_id = compaction_object_id_start(snapshot.record.seq.0)?;
            current_files.clear();
            for references in groups.into_values() {
                if self.interrupted() {
                    return None;
                }
                let mut compactor = BlxCompactor::default();
                for reference in references {
                    let bytes = get_retry(state, world, &reference.identity.store_key()).await?;
                    if checksum64(&bytes) != reference.object_checksum {
                        return None;
                    }
                    compactor.add_object(&bytes).ok()?;
                }
                let compacted = compactor.finish(
                    BatchMeta {
                        namespace_kind: NamespaceKind::Volume,
                        namespace_id: volume.0,
                        writer_fence: snapshot.record.fence,
                        first_object_id: next_object_id,
                        min_seq: snapshot.record.seq.0,
                        max_seq: snapshot.record.seq.0,
                        batch_id: next_object_id,
                        pre_state_checksum: snapshot.state_checksum,
                        post_state_checksum: snapshot.state_checksum,
                    },
                    true,
                );
                next_object_id =
                    next_object_id.checked_add(u64::try_from(compacted.len()).ok()?)?;
                for object in compacted {
                    let reference = ObjectRef::from_blx(&object);
                    put_immutable_retry(state, world, reference.identity.store_key(), object.bytes)
                        .await?;
                    if self.interrupted() {
                        return None;
                    }
                    current_files.push(reference);
                }
            }
            if !combined_valid(&current_files)
                || validate_file_state_chain(base_checksum, snapshot.state_checksum, &current_files)
                    .is_err()
            {
                return None;
            }
        }
        let (complete_list, added, removed, mut new_list) = match &previous {
            None => {
                let list = CompleteFileList {
                    volume,
                    writer_fence: snapshot.record.fence,
                    list_id: snapshot.record.seq.0,
                    objects: current_files.clone(),
                };
                (Some(list.reference()), Vec::new(), Vec::new(), Some(list))
            }
            Some((previous_manifest, previous_list)) => {
                let baseline = previous_list.as_ref().map_or_else(BTreeMap::new, |list| {
                    list.objects
                        .iter()
                        .copied()
                        .map(|object| (object.identity, object))
                        .collect()
                });
                let current = current_files
                    .iter()
                    .copied()
                    .map(|object| (object.identity, object))
                    .collect::<BTreeMap<_, _>>();
                let added = current
                    .iter()
                    .filter(|(identity, _)| !baseline.contains_key(identity))
                    .map(|(_, object)| *object)
                    .collect();
                let removed = baseline
                    .keys()
                    .filter(|identity| !current.contains_key(identity))
                    .copied()
                    .collect();
                (previous_manifest.complete_list, added, removed, None)
            }
        };
        let (recovery_kind, checkpoint_epoch, vmstate_logical_length) =
            recovery_metadata(&snapshot.record);
        let archive_seq = previous
            .as_ref()
            .map_or(0, |(manifest, _)| manifest.archive_seq.saturating_add(1));
        let manifest = Manifest {
            volume,
            writer_fence: snapshot.record.fence,
            journal_seq: snapshot.record.seq.0,
            archive_seq,
            capture_seq: snapshot.record.capture_seq,
            sync_covered_through: snapshot.record.sync_covered_through,
            recovery_kind,
            checkpoint_epoch,
            config: snapshot.record.config,
            vmstate_logical_length,
            base: snapshot.base,
            complete_list,
            post_state_checksum: snapshot.state_checksum,
            metadata_checksum: checksum64(&snapshot.record.encode(volume)),
            added,
            removed,
        };
        let prior_list = previous.as_ref().and_then(|(_, list)| list.as_ref());
        let bounded = bound_manifest(manifest, prior_list, archive_seq).ok()?;
        if bounded.new_complete_list.is_some() {
            new_list = bounded.new_complete_list;
        }
        if let Some(list) = new_list {
            let bytes = list.encode();
            put_immutable_retry(state, world, list.store_key(), bytes).await?;
            if self.interrupted() {
                return None;
            }
        }
        let manifest_bytes = bounded.manifest.encode().ok()?;
        let pointer = bounded.manifest.pointer(&manifest_bytes);
        let pending_key = pointer.pending_key(volume);
        put_retry(state, world, pending_key.clone(), manifest_bytes.clone()).await?;
        if self.interrupted() {
            return None;
        }
        put_immutable_retry(state, world, pointer.manifest_key(volume), manifest_bytes).await?;
        Some(PreparedArchive {
            pointer,
            pending_key,
        })
    }
}

async fn load_archive_metadata<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    pointer: Option<ManifestPtr>,
) -> Option<Option<(Manifest, Option<CompleteFileList>)>> {
    let Some(pointer) = pointer else {
        return Some(None);
    };
    let bytes = get_retry(state, world, &pointer.manifest_key(volume)).await?;
    let list_bytes = if let Some(reference) = Manifest::decode(volume, &bytes).ok()?.complete_list {
        Some(get_retry(state, world, &reference.store_key(volume)).await?)
    } else {
        None
    };
    let closure = ManifestClosure::decode(volume, pointer, &bytes, list_bytes.as_deref()).ok()?;
    Some(Some((closure.manifest, closure.complete_list)))
}

async fn read_blob_retry<W: Blobs>(world: &W, name: &str, retry: u64) -> Option<Vec<u8>> {
    loop {
        match Blobs::read(world, name).await {
            Ok(Some(bytes)) => return Some(bytes),
            Ok(None) => return None,
            Err(_) => delay(retry).await,
        }
    }
}

enum PublishHead {
    Published(u64),
    Fenced,
    Fatal,
}

async fn publish_head<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    incarnation: u64,
    pointer: ManifestPtr,
    retry: u64,
) -> PublishHead {
    loop {
        let Some((expected, head)) = ({
            let host = state.borrow();
            host.volumes
                .get(&volume)
                .filter(|volume| volume.incarnation == incarnation)
                .and_then(|volume_state| {
                    Some((
                        volume_state.head_version?,
                        HeadRecord {
                            volume,
                            holder: host.config.host,
                            fence: volume_state.fence,
                            manifest: Some(pointer),
                            stash: volume_state.stash_assignment,
                            retired_stashes: volume_state.retired_stashes.clone(),
                        },
                    ))
                })
        }) else {
            return PublishHead::Fenced;
        };
        match Store::put_cas(
            world,
            layout::head_key(volume),
            Some(expected),
            head.encode(),
        )
        .await
        {
            Ok(version) => return PublishHead::Published(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                match Store::get(world, &layout::head_key(volume)).await {
                    Ok(Some((version, bytes))) => {
                        let Ok(found) = HeadRecord::decode(volume, &bytes) else {
                            return PublishHead::Fatal;
                        };
                        if found.holder != head.holder || found.fence != head.fence {
                            return PublishHead::Fenced;
                        }
                        if found.manifest.is_some_and(|manifest| {
                            (manifest.capture_seq, manifest.seq)
                                >= (pointer.capture_seq, pointer.seq)
                        }) {
                            return PublishHead::Published(version);
                        }
                        if let Some(volume_state) = state.borrow_mut().volumes.get_mut(&volume) {
                            volume_state.head_version = Some(version);
                        }
                    }
                    Ok(None) => return PublishHead::Fenced,
                    Err(StoreError::Fault(StoreFault::Unavailable)) => delay(retry).await,
                    Err(
                        StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge,
                    ) => return PublishHead::Fatal,
                }
            }
            Err(StoreError::TooLarge) => return PublishHead::Fatal,
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world, &layout::head_key(volume)).await {
                    Ok(Some((version, bytes))) => {
                        let Ok(found) = HeadRecord::decode(volume, &bytes) else {
                            return PublishHead::Fatal;
                        };
                        if found.holder != head.holder || found.fence != head.fence {
                            return PublishHead::Fenced;
                        }
                        if found.manifest.is_some_and(|manifest| {
                            (manifest.capture_seq, manifest.seq)
                                >= (pointer.capture_seq, pointer.seq)
                        }) {
                            return PublishHead::Published(version);
                        }
                        if let Some(volume_state) = state.borrow_mut().volumes.get_mut(&volume) {
                            volume_state.head_version = Some(version);
                        }
                    }
                    Ok(None) => return PublishHead::Fenced,
                    Err(StoreError::Fault(StoreFault::Unavailable)) => delay(retry).await,
                    Err(
                        StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge,
                    ) => return PublishHead::Fatal,
                }
            }
        }
    }
}

async fn fence_volume<W: GuestMem>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    incarnation: Option<u64>,
) {
    let pages =
        {
            let mut host = state.borrow_mut();
            if host.volumes.get(&volume).is_none_or(|volume| {
                incarnation.is_some_and(|expected| volume.incarnation != expected)
            }) {
                return;
            }
            host.volumes.remove(&volume);
            host.counters.fenced += 1;
            host.cache.purge_volume(volume)
        };
    if GuestMem::fence(world, volume).await.is_err() {
        state.borrow_mut().fail("guest fence notification failed");
        return;
    }
    for page in pages {
        if GuestMem::evict(world, page).await.is_err() {
            state.borrow_mut().fail("fenced guest page eviction failed");
            return;
        }
    }
}

struct PublishLease {
    state: SharedHost,
    volume: VolumeId,
    incarnation: u64,
    active: bool,
}

impl PublishLease {
    fn new(state: &SharedHost, volume: VolumeId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            volume,
            incarnation,
            active: true,
        }
    }

    fn commit(mut self) {
        self.release();
        self.state.borrow_mut().schedule_volume(self.volume);
        self.active = false;
    }

    fn release(&self) {
        let waiters = {
            let mut host = self.state.borrow_mut();
            let Some(volume) = host
                .volumes
                .get_mut(&self.volume)
                .filter(|volume| volume.incarnation == self.incarnation)
            else {
                return;
            };
            volume.publishing_blx_files.clear();
            volume.operations.finish_publication();
            std::mem::take(&mut volume.publication_waiters)
        };
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

impl Drop for PublishLease {
    fn drop(&mut self) {
        if self.active {
            self.release();
        }
        self.state.borrow_mut().schedule_volume(self.volume);
    }
}

#[cfg(test)]
mod tests {
    use super::source_object_is_covered;

    #[test]
    fn archive_source_frontier_is_scoped_to_one_writer_fence() {
        let frontier = Some((9, 40));
        assert!(source_object_is_covered(frontier, 9, 40));
        assert!(source_object_is_covered(frontier, 9, 12));
        assert!(!source_object_is_covered(frontier, 9, 41));
        assert!(!source_object_is_covered(frontier, 10, 1));
    }
}
