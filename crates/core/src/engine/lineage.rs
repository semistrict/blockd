use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::delay;

use super::SharedHost;
use super::backup::claim_new_head_with_stash;
use super::capture::write_record_copies;
use super::ctx::{HostCtx, VolumeCtx};
use super::recovery_policy::{manifest_verdict, recovery_metadata};
use super::replica::initial_stash;
use super::store_retry::{read as get_retry, write_immutable as put_immutable_retry};
use crate::blx::{BatchMeta, BlxCompactor, BlxObject, MAX_OVERLAPPING_FILES, NamespaceKind};
use crate::format::checksum64;
use crate::head::HeadRecord;
use crate::journal::{JournalRecord, RecordKind, VolumeConfig};
use crate::layout;
use crate::manifest::{
    BaseManifest, BaseRoot, Manifest, ObjectRef, RecoveryKind, validate_object_refs,
};
use crate::protocol::{AdminError, AdminResult, AdminSuccess, StoreFault, Verdict};
use crate::types::{Epoch, JournalSeq, VolumeId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

pub async fn delete_base<W>(state: SharedHost, world: Rc<W>, base: u64) -> AdminResult
where
    W: Store + AdminIo + 'static,
{
    HostCtx::new(state, world).delete_base(base).await
}

impl<W> HostCtx<W>
where
    W: Store + AdminIo + 'static,
{
    pub(super) async fn delete_base(&self, base: u64) -> AdminResult {
        let state = self.state();
        let world = self.world();
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
}

/// Keep a pinned recovery point by writing references to its immutable BLX
/// files. Existing page data stays under the namespace that created it.
#[allow(clippy::too_many_lines)]
pub async fn keep_base<W>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
    base: u64,
) -> AdminResult
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world)
        .volume(volume)
        .keep_base(base)
        .await
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    pub(super) async fn keep_base(&self, base: u64) -> AdminResult {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        let Some((record, state_checksum)) = ({
            let host = state.borrow();
            host.volumes
                .get(&volume)
                .filter(|volume| volume.ready)
                .and_then(|volume| Some((volume.pinned.clone()?, volume.state_checksum)))
        }) else {
            return Err(AdminError::Rejected);
        };

        let inherited = state
            .borrow()
            .volumes
            .get(&volume)
            .map(|volume| {
                volume
                    .archive_objects
                    .iter()
                    .map(|object| object.identity)
                    .collect::<BTreeSet<_>>()
            })
            .ok_or(AdminError::Rejected)?;
        let Some(mut objects) =
            collect_record_objects(&state, world.as_ref(), volume, &record).await
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
                        get_retry(&state, world.as_ref(), &object.identity.store_key()).await
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
            metadata_checksum: checksum64(&record.encode(volume)),
            objects,
        };
        let manifest_bytes = manifest.encode();
        let root = manifest.root();
        if put_immutable_retry(
            &state,
            world.as_ref(),
            layout::base_manifest_key(base, manifest.manifest_id),
            manifest_bytes,
        )
        .await
        .is_none()
            || put_immutable_retry(
                &state,
                world.as_ref(),
                layout::base_root_key(base),
                root.encode(),
            )
            .await
            .is_none()
        {
            return Err(AdminError::Rejected);
        }
        Ok(AdminSuccess::BaseKept { base })
    }
}

/// Create a child with no data files of its own. The only archive dependency
/// is the one fixed-size base pointer in its initial manifest.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn create_fork<W>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
    config: VolumeConfig,
    base: u64,
) -> Option<AdminResult>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world)
        .volume(volume)
        .create_fork(config, base)
        .await
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    pub(super) async fn create_fork(&self, config: VolumeConfig, base: u64) -> Option<AdminResult> {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        if state.borrow().volumes.contains_key(&volume) {
            return Some(Err(AdminError::Rejected));
        }
        let retry = state.borrow().config.backup_retry;
        let Some((root, base_manifest)) = get_base(&state, world.as_ref(), base).await else {
            return Some(Err(AdminError::Rejected));
        };
        if base_manifest.config != config {
            return Some(Err(AdminError::Rejected));
        }
        let Some(stash) = initial_stash(&state, volume) else {
            return Some(Err(AdminError::Rejected));
        };
        let incarnation = state.borrow_mut().insert_fresh(volume, config);
        let Some(fence) =
            claim_new_head_with_stash(&state, world.as_ref(), volume, incarnation, Some(stash))
                .await
        else {
            state.borrow_mut().volumes.remove(&volume);
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
            runtime_page_index: BTreeMap::new(),
            migrated_from: None,
        };
        {
            let mut host = state.borrow_mut();
            let volume_state = host
                .volumes
                .get_mut(&volume)
                .expect("fork insertion retained");
            volume_state.fence = fence;
            volume_state.head_version = Some(fence);
            volume_state.stash_assignment = Some(stash);
            volume_state.mutation_seq = base_manifest.capture_seq;
            volume_state.state_checksum = base_manifest.post_state_checksum;
            volume_state.archived_memory_usable = !matches!(verdict, Verdict::ColdBoot);
            volume_state.archived_non_data_reset = !matches!(verdict, Verdict::ColdBoot);
            volume_state.archive_base = Some(root.as_base_ref());
            volume_state
                .archive_objects
                .clone_from(&base_manifest.objects);
            volume_state.best_record = Some(record.clone());
            host.counters.assignment_claims += 1;
        }
        if !write_record_copies(&state, world.as_ref(), volume, &record, &BTreeMap::new()).await {
            state.borrow_mut().volumes.remove(&volume);
            state.borrow_mut().fail("fork journal write failed");
            return None;
        }

        let child_manifest = Manifest {
            volume,
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
            state.borrow_mut().volumes.remove(&volume);
            return Some(Err(AdminError::Rejected));
        };
        let pointer = child_manifest.pointer(&manifest_bytes);
        if put_immutable_retry(
            &state,
            world.as_ref(),
            layout::manifest_key(volume, fence, pointer.seq),
            manifest_bytes,
        )
        .await
        .is_none()
        {
            state.borrow_mut().volumes.remove(&volume);
            return Some(Err(AdminError::Rejected));
        }
        let expected = state.borrow().volumes.get(&volume)?.head_version?;
        let head = HeadRecord {
            volume,
            holder: state.borrow().config.host,
            fence,
            manifest: Some(pointer),
            stash: Some(stash),
            retired_stashes: Vec::new(),
        };
        let Some(version) =
            put_head_retry(&state, world.as_ref(), volume, expected, &head, retry).await
        else {
            state.borrow_mut().volumes.remove(&volume);
            return Some(Err(AdminError::Rejected));
        };
        {
            let mut host = state.borrow_mut();
            let volume_state = host.volume_at_mut(volume, incarnation)?;
            volume_state.ready = true;
            volume_state.next_seq = 1;
            volume_state.backed = Some(pointer);
            volume_state.head_version = Some(version);
            volume_state.record_writes.insert(JournalSeq(0), (fence, 0));
            if matches!(record.kind, RecordKind::Checkpoint { .. }) {
                volume_state.pinned = Some(record);
            }
            host.counters.records_written += 1;
            host.counters.manifests_published += 1;
            host.schedule_volume(volume);
        }
        Some(Ok(AdminSuccess::VolumeForked { volume, verdict }))
    }
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
    volume: VolumeId,
    record: &JournalRecord,
) -> Option<Vec<ObjectRef>> {
    // A fork's inherited files are already direct immutable references. Carry
    // all of them into the new base manifest so the new base is flat rather
    // than pointing through the older base.
    let mut objects = state
        .borrow()
        .volumes
        .get(&volume)?
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
        let bytes = if identity.namespace_kind == NamespaceKind::Volume
            && identity.namespace_id == volume.0
        {
            match Blobs::read(
                world,
                &layout::blx_blob(
                    volume,
                    identity.writer_fence,
                    crate::types::ObjectId(identity.object_id),
                ),
            )
            .await
            .ok()
            .flatten()
            {
                Some(bytes) => bytes,
                None => get_retry(state, world, &identity.store_key()).await?,
            }
        } else {
            get_retry(state, world, &identity.store_key()).await?
        };
        let object = BlxObject::open(&bytes).ok()?;
        let reference = ObjectRef::from_blx(&object);
        if reference != *file {
            return None;
        }
        if identity.namespace_kind == NamespaceKind::Volume && identity.namespace_id == volume.0 {
            put_immutable_retry(state, world, identity.store_key(), bytes).await?;
        }
        objects.insert(identity, reference);
    }
    Some(objects.into_values().collect())
}

async fn get_base<W: Store>(
    state: &SharedHost,
    world: &W,
    base: u64,
) -> Option<(BaseRoot, BaseManifest)> {
    let root_bytes = get_retry(state, world, &layout::base_root_key(base)).await?;
    let root = BaseRoot::decode(base, &root_bytes).ok()?;
    let bytes = get_retry(
        state,
        world,
        &layout::base_manifest_key(base, root.manifest_id),
    )
    .await?;
    let manifest = BaseManifest::decode(root, &bytes).ok()?;
    Some((root, manifest))
}

async fn put_head_retry<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    expected: u64,
    head: &HeadRecord,
    retry: u64,
) -> Option<u64> {
    loop {
        match Store::put_cas(
            world,
            layout::head_key(volume),
            Some(expected),
            head.encode(),
        )
        .await
        {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world, &layout::head_key(volume)).await {
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
