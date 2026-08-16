use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::delay;

use super::SharedHost;
use super::backup::claim_new_head_with_stash;
use super::capture::write_record_copies;
use super::recovery_policy::{manifest_verdict, recovery_metadata};
use super::replica::initial_stash;
use super::store_retry::{read as get_retry, write_immutable as put_immutable_retry};
use crate::blx::{BatchMeta, BlxCompactor, MAX_OVERLAPPING_FILES, NamespaceKind, open_object};
use crate::format::checksum64;
use crate::head::{HeadRecord, ManifestPtr};
use crate::journal::{JournalRecord, RecordKind, VsetConfig};
use crate::layout;
use crate::manifest::{
    BaseManifest, BaseRoot, Manifest, ObjectRef, RecoveryKind, validate_object_refs,
};
use crate::protocol::{AdminError, AdminResult, AdminSuccess, StoreFault, Verdict};
use crate::types::{Epoch, JournalSeq, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

pub async fn delete_base<W>(state: SharedHost, world: Rc<W>, base: u64) -> AdminResult
where
    W: Store + AdminIo + 'static,
{
    let retry = state.borrow().config.backup_retry;
    loop {
        match Store::delete(world.as_ref(), &layout::base_root_key(base)).await {
            Ok(_) => return Ok(AdminSuccess::BaseDeleted { base }),
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. })) => {
                return Err(AdminError::Rejected);
            }
        }
    }
}

/// Keep a pinned recovery point by writing references to its immutable BLX
/// files. Existing page data stays under the namespace that created it.
#[allow(clippy::too_many_lines)]
pub async fn keep_base<W>(state: SharedHost, world: Rc<W>, vset: VsetId, base: u64) -> AdminResult
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let Some((record, retry, state_checksum)) = ({
        let host = state.borrow();
        host.vsets
            .get(&vset)
            .filter(|vset| vset.ready)
            .and_then(|vset| {
                Some((
                    vset.pinned.clone()?,
                    host.config.backup_retry,
                    vset.state_checksum,
                ))
            })
    }) else {
        return Err(AdminError::Rejected);
    };

    let inherited = state
        .borrow()
        .vsets
        .get(&vset)
        .map(|vset| {
            vset.archive_objects
                .iter()
                .map(|object| object.identity)
                .collect::<BTreeSet<_>>()
        })
        .ok_or(AdminError::Rejected)?;
    let Some(mut objects) =
        collect_record_objects(&state, world.as_ref(), vset, &record, retry).await
    else {
        return Err(AdminError::Rejected);
    };
    if maximum_overlap(&objects) >= MAX_OVERLAPPING_FILES
        && objects
            .iter()
            .any(|object| !inherited.contains(&object.identity))
    {
        let mut groups = BTreeMap::new();
        for object in objects
            .iter()
            .filter(|object| !inherited.contains(&object.identity))
        {
            let partition = object.first_key.file_partition();
            if object.last_key.file_partition() != partition {
                return Err(AdminError::Rejected);
            }
            groups
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(*object);
        }
        let mut next_object_id = 0;
        objects.retain(|object| inherited.contains(&object.identity));
        for references in groups.into_values() {
            let mut compactor = BlxCompactor::default();
            for object in references {
                let Some(bytes) =
                    get_retry(&state, world.as_ref(), &object.identity.store_key(), retry).await
                else {
                    return Err(AdminError::Rejected);
                };
                if checksum64(&bytes) != object.object_checksum {
                    return Err(AdminError::Rejected);
                }
                compactor
                    .add_object(&bytes)
                    .map_err(|_| AdminError::Rejected)?;
            }
            let compacted = compactor.finish(
                BatchMeta {
                    namespace_kind: NamespaceKind::ImportedBase,
                    namespace_id: base,
                    writer_fence: record.fence,
                    first_object_id: next_object_id,
                    min_seq: record.seq.0,
                    max_seq: record.seq.0,
                    batch_id: next_object_id,
                    pre_state_checksum: state_checksum,
                    post_state_checksum: state_checksum,
                },
                true,
            );
            next_object_id = next_object_id
                .checked_add(u64::try_from(compacted.len()).map_err(|_| AdminError::Rejected)?)
                .ok_or(AdminError::Rejected)?;
            for object in compacted {
                let reference = ObjectRef::from_blx(&object);
                if put_immutable_retry(
                    &state,
                    world.as_ref(),
                    reference.identity.store_key(),
                    object.bytes,
                    retry,
                )
                .await
                .is_none()
                {
                    return Err(AdminError::Rejected);
                }
                objects.push(reference);
            }
        }
    }
    if validate_object_refs(&objects).is_err() {
        return Err(AdminError::Rejected);
    }

    let (recovery_kind, checkpoint_epoch, vmstate_logical_length) = recovery_metadata(&record);
    let manifest = BaseManifest {
        base_id: base,
        manifest_id: 1,
        capture_seq: record.capture_seq,
        sync_covered_through: record.sync_covered_through,
        recovery_kind,
        checkpoint_epoch,
        config: record.config,
        vmstate_logical_length,
        post_state_checksum: state_checksum,
        metadata_checksum: checksum64(&record.encode(vset)),
        objects,
    };
    let manifest_bytes = manifest.encode();
    let root = manifest.root();
    if put_immutable_retry(
        &state,
        world.as_ref(),
        layout::base_manifest_key(base, manifest.manifest_id),
        manifest_bytes,
        retry,
    )
    .await
    .is_none()
        || put_immutable_retry(
            &state,
            world.as_ref(),
            layout::base_root_key(base),
            root.encode(),
            retry,
        )
        .await
        .is_none()
    {
        return Err(AdminError::Rejected);
    }
    Ok(AdminSuccess::BaseKept { base })
}

/// Create a child with no data files of its own. The only archive dependency
/// is the one fixed-size base pointer in its initial manifest.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn create_fork<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    config: VsetConfig,
    base: u64,
) -> Option<AdminResult>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    if state.borrow().vsets.contains_key(&vset) {
        return Some(Err(AdminError::Rejected));
    }
    let retry = state.borrow().config.backup_retry;
    let Some((root, base_manifest)) = get_base(&state, world.as_ref(), base, retry).await else {
        return Some(Err(AdminError::Rejected));
    };
    if base_manifest.config != config {
        return Some(Err(AdminError::Rejected));
    }
    let Some(stash) = initial_stash(&state, vset) else {
        return Some(Err(AdminError::Rejected));
    };
    let incarnation = state.borrow_mut().insert_fresh(vset, config);
    let Some(fence) =
        claim_new_head_with_stash(&state, world.as_ref(), vset, incarnation, Some(stash)).await
    else {
        state.borrow_mut().vsets.remove(&vset);
        return Some(Err(AdminError::Rejected));
    };

    let (verdict, kind) = manifest_verdict(
        base_manifest.recovery_kind,
        Epoch(0),
        base_manifest.vmstate_logical_length,
    );
    let record = JournalRecord {
        config,
        seq: JournalSeq(0),
        fence,
        kind,
        capture_seq: base_manifest.capture_seq,
        sync_covered_through: base_manifest.sync_covered_through,
        post_state_checksum: base_manifest.post_state_checksum,
        files: Vec::new(),
        overlay: BTreeMap::new(),
        migrated_from: None,
    };
    {
        let mut host = state.borrow_mut();
        let vset_state = host.vsets.get_mut(&vset).expect("fork insertion retained");
        vset_state.fence = fence;
        vset_state.head_version = Some(fence);
        vset_state.stash_assignment = Some(stash);
        vset_state.mutation_seq = base_manifest.capture_seq;
        vset_state.state_checksum = base_manifest.post_state_checksum;
        vset_state.archived_memory_usable = !matches!(verdict, Verdict::ColdBoot);
        vset_state.archive_base = Some(root.as_base_ref());
        vset_state
            .archive_objects
            .clone_from(&base_manifest.objects);
        vset_state.best_record = Some(record.clone());
        host.counters.assignment_claims += 1;
    }
    if !write_record_copies(&state, world.as_ref(), vset, &record, &BTreeMap::new()).await {
        state.borrow_mut().vsets.remove(&vset);
        state.borrow_mut().fail("fork journal write failed");
        return None;
    }

    let child_manifest = Manifest {
        vset,
        writer_fence: fence,
        journal_seq: record.seq.0,
        archive_seq: 0,
        capture_seq: base_manifest.capture_seq,
        sync_covered_through: base_manifest.sync_covered_through,
        recovery_kind: base_manifest.recovery_kind,
        checkpoint_epoch: if base_manifest.recovery_kind == RecoveryKind::Whole {
            Epoch(0)
        } else {
            base_manifest.checkpoint_epoch
        },
        config,
        vmstate_logical_length: base_manifest.vmstate_logical_length,
        base: Some(root.as_base_ref()),
        complete_list: None,
        post_state_checksum: root.post_state_checksum,
        metadata_checksum: base_manifest.metadata_checksum,
        added: Vec::new(),
        removed: Vec::new(),
    };
    let Ok(manifest_bytes) = child_manifest.encode() else {
        state.borrow_mut().vsets.remove(&vset);
        return Some(Err(AdminError::Rejected));
    };
    let pointer = ManifestPtr {
        fence,
        journal_seq: record.seq,
        seq: JournalSeq(0),
        capture_seq: child_manifest.capture_seq,
        checksum: checksum64(&manifest_bytes),
    };
    if put_immutable_retry(
        &state,
        world.as_ref(),
        layout::manifest_key(vset, fence, pointer.seq),
        manifest_bytes,
        retry,
    )
    .await
    .is_none()
    {
        state.borrow_mut().vsets.remove(&vset);
        return Some(Err(AdminError::Rejected));
    }
    let expected = state.borrow().vsets.get(&vset)?.head_version?;
    let head = HeadRecord {
        vset,
        holder: state.borrow().config.host,
        fence,
        manifest: Some(pointer),
        stash: Some(stash),
        retired_stashes: Vec::new(),
    };
    let Some(version) = put_head_retry(&state, world.as_ref(), vset, expected, &head, retry).await
    else {
        state.borrow_mut().vsets.remove(&vset);
        return Some(Err(AdminError::Rejected));
    };
    {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|vset| vset.incarnation == incarnation)?;
        vset_state.ready = true;
        vset_state.next_seq = 1;
        vset_state.backed = Some(pointer);
        vset_state.head_version = Some(version);
        vset_state.record_writes.insert(JournalSeq(0), (fence, 0));
        if matches!(record.kind, RecordKind::Checkpoint { .. }) {
            vset_state.pinned = Some(record);
        }
        host.counters.records_written += 1;
        host.counters.manifests_published += 1;
        host.schedule_vset(vset);
    }
    Some(Ok(AdminSuccess::VsetForked { vset, verdict }))
}

fn maximum_overlap(objects: &[ObjectRef]) -> usize {
    objects
        .iter()
        .map(|candidate| {
            objects
                .iter()
                .filter(|object| {
                    object.first_key <= candidate.first_key
                        && candidate.first_key <= object.last_key
                })
                .count()
        })
        .max()
        .unwrap_or(0)
}

async fn collect_record_objects<W: Blobs + Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    record: &JournalRecord,
    retry: u64,
) -> Option<Vec<ObjectRef>> {
    // A fork's inherited files are already direct immutable references. Carry
    // all of them into the new base manifest so the new base is flat rather
    // than pointing through the older base.
    let mut objects = state
        .borrow()
        .vsets
        .get(&vset)?
        .archive_objects
        .iter()
        .copied()
        .map(|object| (object.identity, object))
        .collect::<BTreeMap<_, _>>();
    for file in &record.files {
        let identity = file.identity;
        if objects.contains_key(&identity) {
            continue;
        }
        let bytes =
            if identity.namespace_kind == NamespaceKind::Vset && identity.namespace_id == vset.0 {
                match Blobs::read(
                    world,
                    &layout::segment_blob(
                        vset,
                        identity.writer_fence,
                        crate::types::SegId(identity.object_id),
                    ),
                )
                .await
                .ok()
                .flatten()
                {
                    Some(bytes) => bytes,
                    None => get_retry(state, world, &identity.store_key(), retry).await?,
                }
            } else {
                get_retry(state, world, &identity.store_key(), retry).await?
            };
        let object = open_object(&bytes).ok()?;
        let reference = ObjectRef::from_blx(&object);
        if reference != *file {
            return None;
        }
        if identity.namespace_kind == NamespaceKind::Vset && identity.namespace_id == vset.0 {
            put_immutable_retry(state, world, identity.store_key(), bytes, retry).await?;
        }
        objects.insert(identity, reference);
    }
    Some(objects.into_values().collect())
}

async fn get_base<W: Store>(
    state: &SharedHost,
    world: &W,
    base: u64,
    retry: u64,
) -> Option<(BaseRoot, BaseManifest)> {
    let root_bytes = get_retry(state, world, &layout::base_root_key(base), retry).await?;
    let root = BaseRoot::decode(base, &root_bytes).ok()?;
    let bytes = get_retry(
        state,
        world,
        &layout::base_manifest_key(base, root.manifest_id),
        retry,
    )
    .await?;
    let manifest = BaseManifest::decode(root, &bytes).ok()?;
    Some((root, manifest))
}

async fn put_head_retry<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    expected: u64,
    head: &HeadRecord,
    retry: u64,
) -> Option<u64> {
    loop {
        match Store::put_cas(world, layout::head_key(vset), Some(expected), head.encode()).await {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) if bytes == head.encode() => return Some(version),
                    Ok(Some(_) | None)
                    | Err(
                        StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. }),
                    ) => return None,
                    Err(StoreError::Fault(StoreFault::Unavailable)) => delay(retry).await,
                }
            }
        }
    }
}
