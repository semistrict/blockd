use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::delay;

use super::SharedHost;
use super::capture::finish_creation;
use super::replica::retry_replica_releases;
use super::state::PublicationOwner;
use crate::blx::{
    BatchMeta, BlxCompactor, MAX_OVERLAPPING_FILES, NamespaceKind, compaction_object_id_start,
    open_object,
};
use crate::format::checksum64;
use crate::head::{HeadRecord, ManifestPtr, StashAssignment};
use crate::journal::{JournalRecord, RecordKind, VsetConfig, VsetKind};
use crate::layout;
use crate::manifest::{
    BaseManifest, BaseRef, BaseRoot, CompleteFileList, Manifest, ObjectRef, RecoveryKind,
    bound_manifest, max_object_overlap, validate_file_state_chain, validate_object_refs,
};
use crate::protocol::{AdminError, AdminEvent, AdminResult, AdminSuccess, StoreFault};
use crate::types::{JournalSeq, SegId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

pub async fn create_backed<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    config: VsetConfig,
) -> Option<AdminResult>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let duplicate = state.borrow().vsets.contains_key(&vset);
    if duplicate {
        return Some(Err(AdminError::Busy));
    }
    let incarnation = state.borrow_mut().insert_fresh(vset, config);
    let Some(version) = claim_new_head(&state, world.as_ref(), vset, incarnation).await else {
        state.borrow_mut().vsets.remove(&vset);
        return Some(Err(AdminError::Rejected));
    };
    {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        vset_state.fence = version;
        vset_state.head_version = Some(version);
        host.counters.assignment_claims += 1;
    }
    if !finish_creation(Rc::clone(&state), world.as_ref(), vset, incarnation).await {
        state.borrow_mut().fail("backed journal creation failed");
        return None;
    }
    publish_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
    let published = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
        let Some(record) = vset_state.best_record.as_ref() else {
            return false;
        };
        vset_state.backed.is_some_and(|pointer| {
            (pointer.capture_seq, pointer.journal_seq) >= (record.capture_seq, record.seq)
        })
    });
    if published {
        Some(Ok(AdminSuccess::VsetCreated { vset }))
    } else {
        Some(Err(AdminError::Unavailable))
    }
}

pub(super) async fn claim_new_head<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
) -> Option<u64> {
    claim_new_head_with_stash(state, world, vset, incarnation, None).await
}

pub(super) async fn claim_new_head_with_stash<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
    stash: Option<StashAssignment>,
) -> Option<u64> {
    let head = HeadRecord {
        vset,
        holder: state.borrow().config.host,
        fence: 0,
        manifest: None,
        stash,
        retired_stashes: Vec::new(),
    };
    let retry = state.borrow().config.backup_retry;
    loop {
        match Store::put_cas(world, layout::head_key(vset), None, head.encode()).await {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                state.borrow_mut().counters.assignment_claim_conflicts += 1;
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let ours = HeadRecord::decode(vset, &bytes).is_ok_and(|found| {
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
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let recovered = HeadRecord::decode(vset, &bytes).ok();
                        let ours = recovered.is_some_and(|found| {
                            found.holder == state.borrow().config.host
                                && found.fence == 0
                                && found.manifest.is_none()
                                && found.stash == stash
                        });
                        if ours
                            && state
                                .borrow()
                                .vsets
                                .get(&vset)
                                .is_some_and(|vset| vset.incarnation == incarnation)
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
pub async fn publish_latest<W>(state: SharedHost, world: Rc<W>, vset: VsetId)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let Some((incarnation, retry)) = ({
        let mut host = state.borrow_mut();
        let retry = host.config.backup_retry;
        host.vsets.get_mut(&vset).and_then(|vset_state| {
            vset_state
                .operations
                .try_start_publication(PublicationOwner::Direct)
                .then_some((vset_state.incarnation, retry))
        })
    }) else {
        return;
    };
    let lease = PublishLease::new(&state, vset, incarnation);
    'publish: while let Some(snapshot) = publish_snapshot(&state, vset, incarnation) {
        if publication_interrupted(&state, vset, incarnation) {
            break;
        }
        {
            let mut host = state.borrow_mut();
            let Some(vset_state) = host
                .vsets
                .get_mut(&vset)
                .filter(|vset| vset.incarnation == incarnation)
            else {
                return;
            };
            vset_state
                .publishing_segments
                .clone_from(&snapshot.segments);
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
            && archive_needs_compaction(&state, world.as_ref(), vset, snapshot.backed, retry).await;
        if publication_interrupted(&state, vset, incarnation) {
            break;
        }
        if already_published && !compact_only {
            break;
        }
        let Some(prepared) = prepare_archive(
            &state,
            world.as_ref(),
            vset,
            incarnation,
            &snapshot,
            retry,
            compact_only,
        )
        .await
        else {
            if publication_interrupted(&state, vset, incarnation) {
                break;
            }
            delay(retry).await;
            continue 'publish;
        };
        if publication_interrupted(&state, vset, incarnation) {
            let _ = Store::delete(world.as_ref(), &prepared.pending_key).await;
            break;
        }
        match publish_head(
            &state,
            world.as_ref(),
            vset,
            incarnation,
            prepared.pointer,
            retry,
        )
        .await
        {
            PublishHead::Published(version) => {
                {
                    let mut host = state.borrow_mut();
                    let Some(vset_state) = host
                        .vsets
                        .get_mut(&vset)
                        .filter(|vset| vset.incarnation == incarnation)
                    else {
                        return;
                    };
                    vset_state.head_version = Some(version);
                    vset_state.backed = Some(prepared.pointer);
                    vset_state
                        .backed_segments
                        .extend(snapshot.segments.iter().copied());
                    if vset_state.stash_assignment.is_some() {
                        vset_state.peer_published = Some(snapshot.commit);
                    }
                    host.counters.manifests_published += 1;
                }
                let _ = Store::delete(world.as_ref(), &prepared.pending_key).await;
                retry_replica_releases(Rc::clone(&state), Rc::clone(&world), vset).await;
            }
            PublishHead::Fenced => {
                fence_vset(&state, world.as_ref(), vset, Some(incarnation)).await;
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

fn publication_interrupted(state: &SharedHost, vset: VsetId, incarnation: u64) -> bool {
    state
        .borrow()
        .vsets
        .get(&vset)
        .filter(|vset_state| vset_state.incarnation == incarnation)
        .is_none_or(|vset_state| {
            vset_state.operations.migration_running() || vset_state.outbound.is_some()
        })
}

#[allow(clippy::too_many_lines)]
pub async fn reconcile_backed_recovery<W>(state: SharedHost, world: Rc<W>, vset: VsetId)
where
    W: Store + GuestMem + AdminIo + 'static,
{
    if let Some(event) = reconcile_backed_recovery_event(state, Rc::clone(&world), vset).await {
        AdminIo::emit_admin_event(world.as_ref(), event).await;
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn reconcile_backed_recovery_event<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
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
        match Store::get(world.as_ref(), &layout::head_key(vset)).await {
            Ok(Some((version, bytes))) => {
                let Ok(head) = HeadRecord::decode(vset, &bytes) else {
                    fence_vset(&state, world.as_ref(), vset, None).await;
                    return None;
                };
                let Some((archive_objects, archive_base)) =
                    load_archive_closure(&state, world.as_ref(), vset, head.manifest, retry).await
                else {
                    fence_vset(&state, world.as_ref(), vset, None).await;
                    return None;
                };
                if head.stash.is_none() {
                    let local_host = state.borrow().config.host;
                    let Some(stash) = super::replica::initial_stash(&state, vset) else {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                        return None;
                    };
                    if head.holder != local_host {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                        return None;
                    }
                    let (incarnation, fence) = state
                        .borrow()
                        .vsets
                        .get(&vset)
                        .map(|vset_state| (vset_state.incarnation, vset_state.fence))?;
                    let upgraded = HeadRecord {
                        vset,
                        holder: local_host,
                        fence,
                        manifest: head.manifest,
                        stash: Some(stash),
                        retired_stashes: Vec::new(),
                    };
                    match Store::put_cas(
                        world.as_ref(),
                        layout::head_key(vset),
                        Some(version),
                        upgraded.encode(),
                    )
                    .await
                    {
                        Ok(upgraded_version) => {
                            let peer_published = recover_peer_published(
                                &state,
                                world.as_ref(),
                                vset,
                                upgraded.manifest,
                                retry,
                            )
                            .await;
                            let verdict = {
                                let mut host = state.borrow_mut();
                                let vset_state = host
                                    .vsets
                                    .get_mut(&vset)
                                    .filter(|vset_state| vset_state.incarnation == incarnation)?;
                                vset_state.head_version = Some(upgraded_version);
                                vset_state.backed = upgraded.manifest;
                                install_archive_closure(
                                    vset_state,
                                    vset,
                                    &archive_objects,
                                    archive_base,
                                );
                                vset_state.peer_published = peer_published;
                                if let Some(manifest) = upgraded.manifest {
                                    vset_state
                                        .store_manifests
                                        .insert((manifest.fence, manifest.seq));
                                }
                                vset_state.stash_assignment = upgraded.stash;
                                vset_state.retired_stashes.clear();
                                if vset_state.outbound.is_some() {
                                    None
                                } else {
                                    vset_state.ready = true;
                                    vset_state.operations.take_recovery()
                                }
                            };
                            state.borrow_mut().schedule_vset(vset);
                            return verdict
                                .map(|verdict| AdminEvent::VsetRecovered { vset, verdict });
                        }
                        Err(StoreError::Fault(StoreFault::CasConflict { .. })) => continue,
                        Err(StoreError::Fault(StoreFault::Unavailable)) => {
                            state.borrow_mut().counters.store_retries += 1;
                            delay(retry).await;
                            continue;
                        }
                        Err(StoreError::TooLarge) => {
                            fence_vset(&state, world.as_ref(), vset, None).await;
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
                    fence_vset(&state, world.as_ref(), vset, None).await;
                    return None;
                }
                let publication_is_ours = {
                    let host = state.borrow();
                    let local_host = host.config.host;
                    host.vsets.get(&vset).is_some_and(|vset_state| {
                        let local = vset_state
                            .best_record
                            .as_ref()
                            .map_or((0, JournalSeq(0)), |record| {
                                (record.capture_seq, record.seq)
                            });
                        let behind = head.manifest.is_some_and(|manifest| {
                            (manifest.capture_seq, manifest.journal_seq) > local
                        });
                        head.holder == local_host
                            && (head.fence == 0 || head.fence == vset_state.fence)
                            && !behind
                    })
                };
                let peer_published = if publication_is_ours {
                    recover_peer_published(&state, world.as_ref(), vset, head.manifest, retry).await
                } else {
                    None
                };
                let decision = {
                    let mut host = state.borrow_mut();
                    let local_host = host.config.host;
                    let vset_state = host.vsets.get_mut(&vset)?;
                    let local = vset_state
                        .best_record
                        .as_ref()
                        .map_or((0, JournalSeq(0)), |record| {
                            (record.capture_seq, record.seq)
                        });
                    let behind = head.manifest.is_some_and(|manifest| {
                        (manifest.capture_seq, manifest.journal_seq) > local
                    });
                    if head.holder != local_host
                        || (head.fence != 0 && head.fence != vset_state.fence)
                        || behind
                    {
                        RecoveryDecision::Fence
                    } else {
                        vset_state.head_version = Some(version);
                        vset_state.backed = head.manifest;
                        install_archive_closure(vset_state, vset, &archive_objects, archive_base);
                        vset_state.peer_published = peer_published;
                        if let Some(manifest) = head.manifest {
                            vset_state
                                .store_manifests
                                .insert((manifest.fence, manifest.seq));
                        }
                        vset_state.stash_assignment = head.stash;
                        vset_state.retired_stashes = head.retired_stashes;
                        if vset_state.outbound.is_some() {
                            RecoveryDecision::Outbound
                        } else {
                            vset_state.ready = true;
                            RecoveryDecision::Ready(
                                vset_state
                                    .operations
                                    .take_recovery()
                                    .expect("recovery verdict retained"),
                            )
                        }
                    }
                };
                state.borrow_mut().schedule_vset(vset);
                match decision {
                    RecoveryDecision::Fence => {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                    }
                    RecoveryDecision::Outbound => {}
                    RecoveryDecision::Ready(verdict) => {
                        return Some(AdminEvent::VsetRecovered { vset, verdict });
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
                let Some(stash) = super::replica::initial_stash(&state, vset) else {
                    fence_vset(&state, world.as_ref(), vset, None).await;
                    return None;
                };
                let head = HeadRecord {
                    vset,
                    holder: local_host,
                    fence: 0,
                    manifest: None,
                    stash: Some(stash),
                    retired_stashes: Vec::new(),
                };
                match Store::put_cas(world.as_ref(), layout::head_key(vset), None, head.encode())
                    .await
                {
                    Ok(version) => {
                        let verdict = {
                            let mut host = state.borrow_mut();
                            let verdict = {
                                let vset_state = host.vsets.get_mut(&vset)?;
                                vset_state.fence = version;
                                vset_state.head_version = Some(version);
                                vset_state.backed = None;
                                vset_state.stash_assignment = Some(stash);
                                vset_state.retired_stashes.clear();
                                if vset_state.outbound.is_some() {
                                    None
                                } else {
                                    vset_state.ready = true;
                                    vset_state.operations.take_recovery()
                                }
                            };
                            host.counters.assignment_claims += 1;
                            verdict
                        };
                        state.borrow_mut().schedule_vset(vset);
                        return verdict.map(|verdict| AdminEvent::VsetRecovered { vset, verdict });
                    }
                    Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                        state.borrow_mut().counters.assignment_claim_conflicts += 1;
                    }
                    Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        state.borrow_mut().counters.store_retries += 1;
                        delay(retry).await;
                    }
                    Err(StoreError::TooLarge) => {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                        return None;
                    }
                }
            }
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                fence_vset(&state, world.as_ref(), vset, None).await;
                return None;
            }
        }
    }
}

fn install_archive_closure(
    vset_state: &mut super::state::VsetState,
    vset: VsetId,
    objects: &[ObjectRef],
    base: Option<BaseRef>,
) {
    vset_state.archive_objects = objects.to_vec();
    vset_state.archive_base = base;
    vset_state.archive_footers.clear();
    vset_state.archive_resolved_pages.clear();
    vset_state.backed_segments = objects
        .iter()
        .filter(|object| {
            object.identity.namespace_kind == NamespaceKind::Vset
                && object.identity.namespace_id == vset.0
        })
        .map(|object| {
            (
                object.identity.writer_fence,
                SegId(object.identity.object_id),
            )
        })
        .collect();
}

async fn load_archive_closure<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    pointer: Option<ManifestPtr>,
    retry: u64,
) -> Option<(Vec<ObjectRef>, Option<BaseRef>)> {
    let Some(pointer) = pointer else {
        return Some((Vec::new(), None));
    };
    let Some((manifest, list)) =
        load_archive_metadata(state, world, vset, Some(pointer), retry).await?
    else {
        return None;
    };
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
                retry,
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
    vset: VsetId,
    pointer: Option<ManifestPtr>,
    retry: u64,
) -> Option<crate::protocol::ReplicaCommitInfo> {
    let pointer = pointer?;
    loop {
        match Store::get(
            world,
            &layout::manifest_key(vset, pointer.fence, pointer.seq),
        )
        .await
        {
            Ok(Some((_, bytes))) => {
                if checksum64(&bytes) != pointer.checksum {
                    return None;
                }
                let manifest = Manifest::decode(vset, &bytes).ok()?;
                if (
                    manifest.writer_fence,
                    manifest.journal_seq,
                    manifest.archive_seq,
                ) != (pointer.fence, pointer.journal_seq.0, pointer.seq.0)
                {
                    return None;
                }
                return state.borrow().vsets.get(&vset).and_then(|vset_state| {
                    let record = vset_state.best_record.as_ref()?;
                    (manifest.journal_seq == record.seq.0
                        && manifest.capture_seq == record.capture_seq
                        && manifest.metadata_checksum == checksum64(&record.encode(vset)))
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
    segments: BTreeSet<(u64, SegId)>,
    segment_refs: Vec<ObjectRef>,
    backed: Option<ManifestPtr>,
    published_commit: Option<crate::protocol::ReplicaCommitInfo>,
    commit: crate::protocol::ReplicaCommitInfo,
    state_checksum: u64,
}

fn publish_snapshot(state: &SharedHost, vset: VsetId, incarnation: u64) -> Option<PublishSnapshot> {
    let host = state.borrow();
    let vset_state = host
        .vsets
        .get(&vset)
        .filter(|vset| vset.incarnation == incarnation)?;
    let record = if vset_state.stash_assignment.is_some() {
        vset_state.peer_committed_record.as_ref()?
    } else {
        vset_state.best_record.as_ref()?
    };
    let segment_refs = record
        .files
        .iter()
        .filter(|file| {
            file.identity.namespace_kind == NamespaceKind::Vset
                && file.identity.namespace_id == vset.0
        })
        .copied()
        .collect::<Vec<_>>();
    let segments = segment_refs
        .iter()
        .map(|file| (file.identity.writer_fence, SegId(file.identity.object_id)))
        .collect::<BTreeSet<_>>();
    Some(PublishSnapshot {
        record: record.clone(),
        base: vset_state.archive_base,
        base_objects: vset_state
            .archive_objects
            .iter()
            .filter(|object| {
                object.identity.namespace_kind != NamespaceKind::Vset
                    || object.identity.namespace_id != vset.0
            })
            .copied()
            .collect(),
        segments,
        segment_refs,
        backed: vset_state.backed,
        published_commit: vset_state.peer_published,
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
    vset: VsetId,
    pointer: Option<ManifestPtr>,
    retry: u64,
) -> bool {
    let Some(Some((manifest, list))) =
        load_archive_metadata(state, world, vset, pointer, retry).await
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

async fn prepare_archive<W: Blobs + Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
    snapshot: &PublishSnapshot,
    retry: u64,
    force_compaction: bool,
) -> Option<PreparedArchive> {
    let Some(previous) = load_archive_metadata(state, world, vset, snapshot.backed, retry).await
    else {
        return None;
    };
    if publication_interrupted(state, vset, incarnation) {
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
    for reference in &snapshot.segment_refs {
        if publication_interrupted(state, vset, incarnation) {
            return None;
        }
        let identity = reference.identity;
        let fence = identity.writer_fence;
        let segment = SegId(identity.object_id);
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
            read_blob_retry(world, &layout::segment_blob(vset, fence, segment), retry).await?;
        let object = open_object(&bytes).ok()?;
        if ObjectRef::from_blx(&object) != *reference {
            return None;
        }
        put_immutable_retry(state, world, identity.store_key(), bytes, retry).await?;
        if publication_interrupted(state, vset, incarnation) {
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
            if publication_interrupted(state, vset, incarnation) {
                return None;
            }
            let mut compactor = BlxCompactor::default();
            for reference in references {
                let bytes = get_retry(state, world, &reference.identity.store_key(), retry).await?;
                if checksum64(&bytes) != reference.object_checksum {
                    return None;
                }
                compactor.add_object(&bytes).ok()?;
            }
            let compacted = compactor.finish(
                BatchMeta {
                    namespace_kind: NamespaceKind::Vset,
                    namespace_id: vset.0,
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
            next_object_id = next_object_id.checked_add(u64::try_from(compacted.len()).ok()?)?;
            for object in compacted {
                let reference = ObjectRef::from_blx(&object);
                put_immutable_retry(
                    state,
                    world,
                    reference.identity.store_key(),
                    object.bytes,
                    retry,
                )
                .await?;
                if publication_interrupted(state, vset, incarnation) {
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
                vset,
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
    let (recovery_kind, checkpoint_epoch, vmstate_logical_length) = match snapshot.record.kind {
        RecordKind::Checkpoint {
            epoch,
            vmstate_logical_length,
            ..
        } if snapshot.record.capture_seq >= snapshot.record.sync_covered_through => {
            (RecoveryKind::Whole, epoch, vmstate_logical_length)
        }
        _ if snapshot.record.config.kind == VsetKind::Database => {
            (RecoveryKind::Database, crate::types::Epoch(0), 0)
        }
        _ => (RecoveryKind::DiskOnly, crate::types::Epoch(0), 0),
    };
    let archive_seq = previous
        .as_ref()
        .map_or(0, |(manifest, _)| manifest.archive_seq.saturating_add(1));
    let manifest = Manifest {
        vset,
        writer_fence: snapshot.record.fence,
        journal_seq: snapshot.record.seq.0,
        archive_seq,
        capture_seq: snapshot.record.capture_seq,
        sync_covered_through: snapshot.record.sync_covered_through,
        recovery_kind,
        checkpoint_epoch,
        config: snapshot.record.config,
        database: snapshot.record.database,
        vmstate_logical_length,
        base: snapshot.base,
        complete_list,
        post_state_checksum: snapshot.state_checksum,
        metadata_checksum: checksum64(&snapshot.record.encode(vset)),
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
        put_immutable_retry(
            state,
            world,
            layout::complete_file_list_key(vset, list.writer_fence, list.list_id),
            bytes,
            retry,
        )
        .await?;
        if publication_interrupted(state, vset, incarnation) {
            return None;
        }
    }
    let manifest_bytes = bounded.manifest.encode().ok()?;
    let pointer = ManifestPtr {
        fence: bounded.manifest.writer_fence,
        journal_seq: JournalSeq(bounded.manifest.journal_seq),
        seq: JournalSeq(bounded.manifest.archive_seq),
        capture_seq: bounded.manifest.capture_seq,
        checksum: checksum64(&manifest_bytes),
    };
    let pending_key = layout::pending_manifest_key(vset, pointer.fence, pointer.seq);
    put_retry(
        state,
        world,
        pending_key.clone(),
        manifest_bytes.clone(),
        retry,
    )
    .await?;
    if publication_interrupted(state, vset, incarnation) {
        return None;
    }
    put_immutable_retry(
        state,
        world,
        layout::manifest_key(vset, pointer.fence, pointer.seq),
        manifest_bytes,
        retry,
    )
    .await?;
    Some(PreparedArchive {
        pointer,
        pending_key,
    })
}

async fn load_archive_metadata<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    pointer: Option<ManifestPtr>,
    retry: u64,
) -> Option<Option<(Manifest, Option<CompleteFileList>)>> {
    let Some(pointer) = pointer else {
        return Some(None);
    };
    let bytes = get_retry(
        state,
        world,
        &layout::manifest_key(vset, pointer.fence, pointer.seq),
        retry,
    )
    .await?;
    if checksum64(&bytes) != pointer.checksum {
        return None;
    }
    let manifest = Manifest::decode(vset, &bytes).ok()?;
    if (
        manifest.writer_fence,
        manifest.journal_seq,
        manifest.archive_seq,
        manifest.capture_seq,
    ) != (
        pointer.fence,
        pointer.journal_seq.0,
        pointer.seq.0,
        pointer.capture_seq,
    ) {
        return None;
    }
    let list = if let Some(reference) = manifest.complete_list {
        let bytes = get_retry(
            state,
            world,
            &layout::complete_file_list_key(vset, reference.writer_fence, reference.list_id),
            retry,
        )
        .await?;
        Some(CompleteFileList::decode(reference, vset, &bytes).ok()?)
    } else {
        None
    };
    Some(Some((manifest, list)))
}

async fn get_retry<W: Store>(
    state: &SharedHost,
    world: &W,
    key: &str,
    retry: u64,
) -> Option<Vec<u8>> {
    loop {
        match Store::get(world, key).await {
            Ok(Some((_, bytes))) => return Some(bytes),
            Ok(None) | Err(StoreError::TooLarge) => return None,
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => return None,
        }
    }
}

async fn put_immutable_retry<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    retry: u64,
) -> Option<u64> {
    loop {
        match Store::put_cas(world, key.clone(), None, bytes.clone()).await {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                let Some((version, found)) = Store::get(world, &key).await.ok().flatten() else {
                    delay(retry).await;
                    continue;
                };
                return (found == bytes).then_some(version);
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::TooLarge) => return None,
        }
    }
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

async fn put_retry<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    retry: u64,
) -> Option<u64> {
    loop {
        match Store::put(world, key.clone(), bytes.clone()).await {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
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
    vset: VsetId,
    incarnation: u64,
    pointer: ManifestPtr,
    retry: u64,
) -> PublishHead {
    loop {
        let Some((expected, head)) = ({
            let host = state.borrow();
            host.vsets
                .get(&vset)
                .filter(|vset| vset.incarnation == incarnation)
                .and_then(|vset_state| {
                    Some((
                        vset_state.head_version?,
                        HeadRecord {
                            vset,
                            holder: host.config.host,
                            fence: vset_state.fence,
                            manifest: Some(pointer),
                            stash: vset_state.stash_assignment,
                            retired_stashes: vset_state.retired_stashes.clone(),
                        },
                    ))
                })
        }) else {
            return PublishHead::Fenced;
        };
        match Store::put_cas(world, layout::head_key(vset), Some(expected), head.encode()).await {
            Ok(version) => return PublishHead::Published(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let Ok(found) = HeadRecord::decode(vset, &bytes) else {
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
                        if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                            vset_state.head_version = Some(version);
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
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let Ok(found) = HeadRecord::decode(vset, &bytes) else {
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
                        if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                            vset_state.head_version = Some(version);
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

async fn fence_vset<W: GuestMem>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: Option<u64>,
) {
    let pages = {
        let mut host = state.borrow_mut();
        if host
            .vsets
            .get(&vset)
            .is_none_or(|vset| incarnation.is_some_and(|expected| vset.incarnation != expected))
        {
            return;
        }
        host.vsets.remove(&vset);
        host.counters.fenced += 1;
        host.cache.purge_vset(vset)
    };
    if GuestMem::fence(world, vset).await.is_err() {
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
    vset: VsetId,
    incarnation: u64,
    active: bool,
}

impl PublishLease {
    fn new(state: &SharedHost, vset: VsetId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            active: true,
        }
    }

    fn commit(mut self) {
        if let Some(vset) = self
            .state
            .borrow_mut()
            .vsets
            .get_mut(&self.vset)
            .filter(|vset| vset.incarnation == self.incarnation)
        {
            vset.publishing_segments.clear();
            vset.operations.finish_publication(PublicationOwner::Direct);
        }
        self.state.borrow_mut().schedule_vset(self.vset);
        self.active = false;
    }
}

impl Drop for PublishLease {
    fn drop(&mut self) {
        if self.active
            && let Some(vset) = self
                .state
                .borrow_mut()
                .vsets
                .get_mut(&self.vset)
                .filter(|vset| vset.incarnation == self.incarnation)
        {
            vset.publishing_segments.clear();
            vset.operations.finish_publication(PublicationOwner::Direct);
        }
        self.state.borrow_mut().schedule_vset(self.vset);
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
