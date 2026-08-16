use std::collections::BTreeMap;
use std::rc::Rc;

use blockd_exec::delay;

use super::recovery_policy::record_verdict;
use super::store_retry;
use super::{SharedHost, VsetState};
use crate::blx::{
    BlockKey, BlockSpace, BlxEntry, EntryKind, NamespaceKind, open_entry, open_footer,
};
use crate::head::{HeadRecord, ManifestPtr, RetiredStash, StashAssignment};
use crate::journal::{JournalRecord, RecordKind};
use crate::layout;
use crate::manifest::{
    BaseManifest, BaseRef, BaseRoot, Manifest, ObjectRef, RecoveryKind, decode_manifest_closure,
    validate_object_refs,
};
use crate::protocol::{AdminError, AdminResult, AdminSuccess, StoreFault, Verdict};
use crate::types::{JournalSeq, SegId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Store, StoreError};

#[allow(clippy::too_many_lines)]
pub async fn restore_vset<W>(state: SharedHost, world: Rc<W>, vset: VsetId) -> AdminResult
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    if state.borrow().vsets.contains_key(&vset) {
        return Err(AdminError::Busy);
    }
    let retry = state.borrow().config.backup_retry;
    let Some((fence, pointer, stash, retired_stashes)) =
        claim_restore(&state, world.as_ref(), vset, retry).await
    else {
        return Err(AdminError::NotFound);
    };
    let Some((mut record, archive_objects, archive_base, state_checksum, vmstate)) =
        get_manifest(&state, world.as_ref(), vset, pointer, retry).await
    else {
        return Err(AdminError::Unavailable);
    };
    if let Some(loaded) = vmstate.as_ref()
        && GuestMem::install_vmstate(world.as_ref(), vset, loaded.bytes.clone())
            .await
            .is_err()
    {
        return Err(AdminError::Unavailable);
    }
    let verdict = record_verdict(&record);
    if matches!(verdict, Verdict::ColdBoot) {
        record
            .overlay
            .retain(|page, _| !record.config.is_memory(page.volume.idx));
    }
    if state.borrow().vsets.contains_key(&vset) {
        return Err(AdminError::Busy);
    }

    {
        let mut host = state.borrow_mut();
        let incarnation = host.allocate_incarnation();
        let mut restored = VsetState::fresh(record.config, incarnation);
        restored.ready = true;
        restored.fence = fence;
        if let Verdict::Resume { epoch, .. } = verdict {
            restored.epoch = epoch;
            restored.pinned = Some(record.clone());
        }
        restored.mutation_seq = record.capture_seq;
        restored.state_checksum = state_checksum;
        restored.archived_memory_usable = !matches!(verdict, Verdict::ColdBoot);
        if let Some(loaded) = vmstate {
            restored.block_checksums.extend(loaded.block_checksums);
        }
        restored.next_seq = record.seq.0 + 1;
        restored.local_covered_through = record.sync_covered_through;
        restored.sync_ack_through = record.sync_covered_through;
        restored.page_locs = record.overlay.clone();
        restored.archive_objects.clone_from(&archive_objects);
        restored.archive_base = archive_base;
        restored.best_record = Some(record.clone());
        restored.head_version = Some(fence);
        restored.backed = Some(pointer);
        restored.stash_assignment = stash;
        restored.retired_stashes = retired_stashes;
        restored.backed_segments = archive_objects
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
        host.vsets.insert(vset, restored);
        host.schedule_vset(vset);
        host.counters.assignment_claims += 1;
    }
    Ok(AdminSuccess::VsetRestored { vset, verdict })
}

async fn claim_restore<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    retry: u64,
) -> Option<(u64, ManifestPtr, Option<StashAssignment>, Vec<RetiredStash>)> {
    loop {
        let (version, bytes) = match Store::get(world, &layout::head_key(vset)).await {
            Ok(Some(found)) => found,
            Ok(None)
            | Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
                continue;
            }
        };
        let head = HeadRecord::decode(vset, &bytes).ok()?;
        if head.stash.is_some() && head.holder != state.borrow().config.host {
            return None;
        }
        let pointer = head.manifest?;
        let claim = HeadRecord {
            vset,
            holder: state.borrow().config.host,
            fence: 0,
            manifest: Some(pointer),
            stash: head.stash,
            retired_stashes: head.retired_stashes.clone(),
        };
        match Store::put_cas(world, layout::head_key(vset), Some(version), claim.encode()).await {
            Ok(fence) => return Some((fence, pointer, head.stash, head.retired_stashes)),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                state.borrow_mut().counters.assignment_claim_conflicts += 1;
                return None;
            }
            Err(StoreError::TooLarge) => return None,
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn get_manifest<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    pointer: ManifestPtr,
    retry: u64,
) -> Option<(
    JournalRecord,
    Vec<ObjectRef>,
    Option<BaseRef>,
    u64,
    Option<LoadedVmstate>,
)> {
    let bytes = store_retry::read(
        state,
        world,
        &layout::manifest_key(vset, pointer.fence, pointer.seq),
        retry,
    )
    .await?;
    let list_bytes = match Manifest::decode(vset, &bytes).ok()?.complete_list {
        None => None,
        Some(reference) => {
            let bytes = store_retry::read(
                state,
                world,
                &layout::complete_file_list_key(vset, reference.writer_fence, reference.list_id),
                retry,
            )
            .await?;
            Some(bytes)
        }
    };
    let closure = decode_manifest_closure(vset, pointer, &bytes, list_bytes.as_deref()).ok()?;
    let manifest = closure.manifest;
    let own = closure.files;
    let base = match manifest.base {
        None => Vec::new(),
        Some(reference) => {
            let root = BaseRoot {
                base_id: reference.base_id,
                manifest_id: reference.manifest_id,
                manifest_checksum: reference.manifest_checksum,
                post_state_checksum: reference.post_state_checksum,
            };
            let bytes = store_retry::read(
                state,
                world,
                &layout::base_manifest_key(reference.base_id, reference.manifest_id),
                retry,
            )
            .await?;
            BaseManifest::decode(root, &bytes).ok()?.objects
        }
    };
    let mut objects = base;
    objects.extend(own);
    validate_object_refs(&objects).ok()?;
    let vmstate = match manifest.recovery_kind {
        RecoveryKind::Whole => Some(
            load_vmstate(
                state,
                world,
                &objects,
                manifest.vmstate_logical_length,
                retry,
            )
            .await?,
        ),
        RecoveryKind::DiskOnly => None,
    };
    let kind = match manifest.recovery_kind {
        RecoveryKind::Whole => {
            let bytes = &vmstate.as_ref()?.bytes;
            let raw: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
            RecordKind::Checkpoint {
                epoch: manifest.checkpoint_epoch,
                vmstate: u64::from_le_bytes(raw),
                vmstate_logical_length: manifest.vmstate_logical_length,
            }
        }
        RecoveryKind::DiskOnly => RecordKind::Commit,
    };
    let record = JournalRecord {
        config: manifest.config,
        seq: JournalSeq(manifest.journal_seq),
        fence: manifest.writer_fence,
        kind,
        capture_seq: manifest.capture_seq,
        sync_covered_through: manifest.sync_covered_through,
        post_state_checksum: manifest.post_state_checksum,
        files: Vec::new(),
        overlay: BTreeMap::default(),
        migrated_from: None,
    };
    Some((
        record,
        objects,
        manifest.base,
        manifest.post_state_checksum,
        vmstate,
    ))
}

struct LoadedVmstate {
    bytes: Vec<u8>,
    block_checksums: BTreeMap<BlockKey, (crate::types::Gen, u64)>,
}

async fn load_vmstate<W: Store>(
    state: &SharedHost,
    world: &W,
    objects: &[ObjectRef],
    logical_length: u64,
    retry: u64,
) -> Option<LoadedVmstate> {
    let block_count = logical_length.div_ceil(crate::types::page_size() as u64);
    let mut output = Vec::with_capacity(usize::try_from(logical_length).ok()?);
    let mut block_checksums = BTreeMap::new();
    for block in 0..block_count {
        let key = BlockKey {
            space: BlockSpace::Vmm,
            volume: 0,
            block: u32::try_from(block).ok()?,
        };
        let mut winner = None;
        for reference in objects
            .iter()
            .filter(|reference| reference.first_key <= key && key <= reference.last_key)
        {
            let bytes = get_store_range(
                state,
                world,
                &reference.identity.store_key(),
                u64::from(reference.footer_offset),
                u64::from(reference.footer_length),
                retry,
            )
            .await?;
            let footer = open_footer(&bytes).ok()?;
            let Some(entry) = footer.find(key) else {
                continue;
            };
            if winner.as_ref().is_none_or(
                |(old, old_ref): &(crate::blx::FooterEntry, ObjectRef)| {
                    (entry.generation, reference.identity) > (old.generation, old_ref.identity)
                },
            ) {
                winner = Some((entry, *reference));
            }
        }
        let (indexed, reference) = winner?;
        if indexed.kind != EntryKind::Data {
            return None;
        }
        let bytes = get_store_range(
            state,
            world,
            &reference.identity.store_key(),
            u64::from(indexed.offset),
            u64::from(indexed.length),
            retry,
        )
        .await?;
        let BlxEntry::Data {
            key: found,
            generation,
            bytes,
        } = open_entry(&bytes).ok()?
        else {
            return None;
        };
        if found != key
            || generation != indexed.generation
            || crate::format::checksum64(&bytes) != indexed.value_checksum
        {
            return None;
        }
        block_checksums.insert(key, (generation, indexed.value_checksum));
        output.extend_from_slice(&bytes);
    }
    output.truncate(usize::try_from(logical_length).ok()?);
    Some(LoadedVmstate {
        bytes: output,
        block_checksums,
    })
}

async fn get_store_range<W: Store>(
    state: &SharedHost,
    world: &W,
    key: &str,
    offset: u64,
    length: u64,
    retry: u64,
) -> Option<Vec<u8>> {
    let (_, bytes) = store_retry::get_range(state, world, key, offset, length, retry)
        .await
        .ok()??;
    (bytes.len() == usize::try_from(length).ok()?).then_some(bytes)
}
