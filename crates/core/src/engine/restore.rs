use std::collections::BTreeMap;
use std::rc::Rc;

use blockd_exec::delay;

use super::ctx::{HostCtx, VolumeCtx};
use super::recovery_policy::record_verdict;
use super::store_retry;
use super::{SharedHost, VolumeState};
use crate::blx::{BlockKey, BlockSpace, BlxEntry, BlxFooter, EntryKind, NamespaceKind};
use crate::head::{HeadRecord, ManifestPtr, RetiredStash, StashAssignment};
use crate::journal::{JournalRecord, RecordKind};
use crate::layout;
use crate::manifest::{
    BaseManifest, BaseRef, BaseRoot, Manifest, ManifestClosure, ObjectRef, RecoveryKind,
    validate_object_refs,
};
use crate::protocol::{AdminError, AdminResult, AdminSuccess, StoreFault, Verdict};
use crate::types::{JournalSeq, VolumeId};
use crate::world::{AdminIo, Blobs, GuestMem, Store, StoreError};

#[allow(clippy::too_many_lines)]
pub async fn restore_volume<W>(state: SharedHost, world: Rc<W>, volume: VolumeId) -> AdminResult
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world).volume(volume).restore().await
}

struct RestoreClaim {
    fence: u64,
    head_version: u64,
    manifest: ManifestPtr,
    stash: Option<StashAssignment>,
    retired_stashes: Vec<RetiredStash>,
}

struct RestoreImage {
    record: JournalRecord,
    archive_objects: Vec<ObjectRef>,
    archive_base: Option<BaseRef>,
    state_checksum: u64,
    vmstate: Option<LoadedVmstate>,
}

struct LoadedVmstate {
    bytes: Vec<u8>,
    block_checksums: BTreeMap<BlockKey, (crate::types::Gen, u64)>,
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    pub(super) async fn restore(&self) -> AdminResult {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        if state.borrow().volumes.contains_key(&volume) {
            return Err(AdminError::Busy);
        }
        let retry = state.borrow().config.backup_retry;
        let Some(claim) = self.claim_restore(retry).await else {
            return Err(AdminError::NotFound);
        };
        let Some(RestoreImage {
            mut record,
            archive_objects,
            archive_base,
            state_checksum,
            vmstate,
        }) = self.load_restore_image(claim.manifest).await
        else {
            return Err(AdminError::Unavailable);
        };
        if let Some(loaded) = vmstate.as_ref()
            && GuestMem::install_vmstate(world.as_ref(), volume, loaded.bytes.clone())
                .await
                .is_err()
        {
            return Err(AdminError::Unavailable);
        }
        let verdict = record_verdict(&record);
        if matches!(verdict, Verdict::ColdBoot) && record.config.is_memory() {
            record.runtime_page_index.clear();
        }
        if state.borrow().volumes.contains_key(&volume) {
            return Err(AdminError::Busy);
        }

        {
            let mut host = state.borrow_mut();
            let run_generation = host.allocate_run_generation();
            let mut restored = VolumeState::fresh(record.config, run_generation);
            restored.ready = true;
            restored.fence = claim.fence;
            if let Verdict::Resume { epoch, .. } = verdict {
                restored.epoch = epoch;
                restored.pinned = Some(record.clone());
            }
            restored.mutation_seq = record.capture_seq;
            restored.state_checksum = state_checksum;
            restored.archived_memory_usable = !matches!(verdict, Verdict::ColdBoot);
            restored.archived_non_data_reset = !matches!(verdict, Verdict::ColdBoot);
            if let Some(loaded) = vmstate {
                restored.block_checksums.extend(loaded.block_checksums);
            }
            restored.next_seq = record.seq.0 + 1;
            restored.local_covered_through = record.sync_covered_through;
            restored.sync_ack_through = record.sync_covered_through;
            restored.page_locs = record.runtime_page_index.clone();
            restored.archive_objects.clone_from(&archive_objects);
            restored.archive_base = archive_base;
            restored.best_record = Some(record.clone());
            restored.head_version = Some(claim.head_version);
            restored.backed = Some(claim.manifest);
            restored.stash_assignment = claim.stash;
            restored.retired_stashes = claim.retired_stashes;
            restored.backed_blx_files = archive_objects
                .iter()
                .filter(|object| {
                    object.identity.namespace_kind == NamespaceKind::Volume
                        && object.identity.namespace_id == volume.0
                })
                .map(|object| object.identity)
                .collect();
            host.volumes.insert(volume, restored);
            host.schedule_volume(volume);
            host.counters.assignment_claims += 1;
        }
        Ok(AdminSuccess::VolumeRestored { volume, verdict })
    }
    async fn claim_restore(&self, retry: u64) -> Option<RestoreClaim> {
        let state = self.host().state();
        let world = self.host().world().as_ref();
        let volume = self.id();
        loop {
            let (version, bytes) = match Store::get(world, &layout::head_key(volume)).await {
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
            let head = HeadRecord::decode(volume, &bytes).ok()?;
            if head.stash.is_some() && head.holder != state.borrow().config.host {
                return None;
            }
            let pointer = head.manifest?;
            let claim = HeadRecord {
                volume,
                holder: state.borrow().config.host,
                fence: 0,
                manifest: Some(pointer),
                stash: head.stash,
                retired_stashes: head.retired_stashes.clone(),
            };
            match Store::put_cas(
                world,
                layout::head_key(volume),
                Some(version),
                claim.encode(),
            )
            .await
            {
                Ok(fence) => {
                    let head_version = self.finalize_restore_head(retry, &claim, fence).await?;
                    return Some(RestoreClaim {
                        fence,
                        head_version,
                        manifest: pointer,
                        stash: head.stash,
                        retired_stashes: head.retired_stashes,
                    });
                }
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

    async fn finalize_restore_head(
        &self,
        retry: u64,
        claim: &HeadRecord,
        fence: u64,
    ) -> Option<u64> {
        let state = self.host().state();
        let world = self.host().world().as_ref();
        let volume = self.id();
        let finalized = HeadRecord {
            fence,
            ..claim.clone()
        };
        loop {
            let (current_version, current_bytes) =
                match Store::get(world, &layout::head_key(volume)).await {
                    Ok(Some(found)) => found,
                    Ok(None)
                    | Err(
                        StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. }),
                    ) => {
                        return None;
                    }
                    Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        state.borrow_mut().counters.store_retries += 1;
                        delay(retry).await;
                        continue;
                    }
                };
            let current = HeadRecord::decode(volume, &current_bytes).ok()?;
            if current == finalized {
                return Some(current_version);
            }
            if current != *claim {
                return None;
            }
            match Store::put_cas(
                world,
                layout::head_key(volume),
                Some(current_version),
                finalized.encode(),
            )
            .await
            {
                Ok(version) => return Some(version),
                Err(StoreError::Fault(StoreFault::Unavailable)) => {
                    state.borrow_mut().counters.store_retries += 1;
                    delay(retry).await;
                }
                Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {}
                Err(StoreError::TooLarge) => return None,
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn load_restore_image(&self, pointer: ManifestPtr) -> Option<RestoreImage> {
        let state = self.host().state();
        let world = self.host().world().as_ref();
        let volume = self.id();
        let bytes = store_retry::read(state, world, &pointer.manifest_key(volume)).await?;
        let list_bytes = match Manifest::decode(volume, &bytes).ok()?.complete_list {
            None => None,
            Some(reference) => {
                let bytes = store_retry::read(state, world, &reference.store_key(volume)).await?;
                Some(bytes)
            }
        };
        let closure =
            ManifestClosure::decode(volume, pointer, &bytes, list_bytes.as_deref()).ok()?;
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
                self.load_vmstate(&objects, manifest.vmstate_logical_length)
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
            runtime_page_index: BTreeMap::default(),
            migrated_from: None,
        };
        Some(RestoreImage {
            record,
            archive_objects: objects,
            archive_base: manifest.base,
            state_checksum: manifest.post_state_checksum,
            vmstate,
        })
    }

    async fn load_vmstate(
        &self,
        objects: &[ObjectRef],
        logical_length: u64,
    ) -> Option<LoadedVmstate> {
        let block_count = logical_length.div_ceil(crate::types::page_size() as u64);
        let mut output = Vec::with_capacity(usize::try_from(logical_length).ok()?);
        let mut block_checksums = BTreeMap::new();
        for block in 0..block_count {
            let key = BlockKey {
                space: BlockSpace::Vmm,
                block: u32::try_from(block).ok()?,
            };
            let mut winner = None;
            for reference in objects
                .iter()
                .filter(|reference| reference.first_key <= key && key <= reference.last_key)
            {
                let bytes = self
                    .get_store_range(
                        &reference.identity.store_key(),
                        u64::from(reference.footer_offset),
                        u64::from(reference.footer_length),
                    )
                    .await?;
                let footer = BlxFooter::open(&bytes).ok()?;
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
            let bytes = self
                .get_store_range(
                    &reference.identity.store_key(),
                    u64::from(indexed.offset),
                    u64::from(indexed.length),
                )
                .await?;
            let BlxEntry::Data {
                key: found,
                generation,
                bytes,
            } = BlxEntry::open(&bytes).ok()?
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

    async fn get_store_range(&self, key: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        let state = self.host().state();
        let world = self.host().world().as_ref();
        let (_, bytes) = store_retry::get_range(state, world, key, offset, length)
            .await
            .ok()??;
        (bytes.len() == usize::try_from(length).ok()?).then_some(bytes)
    }
}
