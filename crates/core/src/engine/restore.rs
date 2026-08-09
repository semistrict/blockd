use std::rc::Rc;

use blockd_exec::delay;

use super::{SharedHost, VsetState};
use crate::head::{HeadRecord, ManifestPtr};
use crate::journal::{JournalRecord, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::span_is_memory;
use crate::protocol::{AdminReply, ReqId, StoreFault, Verdict};
use crate::types::VsetId;
use crate::world::{AdminIo, Blobs, GuestMem, Store, StoreError};

#[allow(clippy::too_many_lines)]
pub async fn restore_vset<W>(state: SharedHost, world: Rc<W>, req: ReqId, vset: VsetId)
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    if state.borrow().vsets.contains_key(&vset) {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    }
    let retry = state.borrow().config.backup_retry;
    let Some((fence, pointer)) = claim_restore(&state, world.as_ref(), vset, retry).await else {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };
    let Some(mut record) = get_manifest(world.as_ref(), vset, pointer, retry).await else {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };
    let verdict = recovery_verdict(&mut record);
    if state.borrow().vsets.contains_key(&vset) {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    }

    {
        let mut host = state.borrow_mut();
        let incarnation = host.allocate_incarnation();
        let mut restored = VsetState::fresh(record.config, incarnation);
        restored.ready = true;
        restored.database = record.database;
        restored.fence = fence;
        if let Verdict::Resume { epoch, .. } = verdict {
            restored.epoch = epoch;
            restored.pinned = Some(record.clone());
        }
        restored.mutation_seq = record.capture_seq;
        restored.next_seq = record.seq.0 + 1;
        restored.local_covered_through = record.sync_covered_through;
        restored.sync_ack_through = record.sync_covered_through;
        restored.overlay = record.overlay.clone();
        restored.leaf_table = record.leaves.clone();
        restored.page_locs = record.overlay.clone();
        restored.next_gen = restored
            .page_locs
            .values()
            .map(|(generation, _)| generation.0 + 1)
            .max()
            .unwrap_or(0);
        restored.best_record = Some(record.clone());
        restored.head_version = Some(fence);
        restored.backed = Some(pointer);
        restored.backed_segments = record
            .overlay
            .values()
            .filter(|(_, location)| location.base == 0)
            .map(|(_, location)| (location.fence, location.seg))
            .collect();
        for &pointer in record.leaves.values() {
            if pointer.base == 0 {
                restored.backed_leaves.insert((pointer.fence, pointer.id));
            }
        }
        host.vsets.insert(vset, restored);
        host.counters.assignment_claims += 1;
    }
    AdminIo::reply_admin(
        world.as_ref(),
        AdminReply::VsetRestored { req, vset, verdict },
    )
    .await;
}

async fn claim_restore<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    retry: u64,
) -> Option<(u64, ManifestPtr)> {
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
        if head.stash.is_some() {
            return None;
        }
        let pointer = head.manifest?;
        let claim = HeadRecord {
            vset,
            holder: state.borrow().config.host,
            fence: 0,
            manifest: Some(pointer),
            stash: None,
            retired_stashes: head.retired_stashes,
        };
        match Store::put_cas(world, layout::head_key(vset), Some(version), claim.encode()).await {
            Ok(fence) => return Some((fence, pointer)),
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

async fn get_manifest<W: Store>(
    world: &W,
    vset: VsetId,
    pointer: ManifestPtr,
    retry: u64,
) -> Option<JournalRecord> {
    loop {
        match Store::get(
            world,
            &layout::manifest_key(vset, pointer.fence, pointer.seq),
        )
        .await
        {
            Ok(Some((_, bytes))) => {
                let record = JournalRecord::decode(vset, &bytes).ok()?;
                return ((record.fence, record.seq, record.capture_seq)
                    == (pointer.fence, pointer.seq, pointer.capture_seq))
                    .then_some(record);
            }
            Ok(None)
            | Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => delay(retry).await,
        }
    }
}

fn recovery_verdict(record: &mut JournalRecord) -> Verdict {
    if record.config.kind == VsetKind::Database {
        return Verdict::DatabaseReady {
            synced_through: record.sync_covered_through,
        };
    }
    if let RecordKind::Checkpoint { epoch, vmstate } = record.kind
        && record.capture_seq >= record.sync_covered_through
    {
        return Verdict::Resume { epoch, vmstate };
    }
    record
        .overlay
        .retain(|page, _| !record.config.is_memory(page.volume.idx));
    record.leaves.retain(|span, _| !span_is_memory(*span));
    Verdict::ColdBoot
}
