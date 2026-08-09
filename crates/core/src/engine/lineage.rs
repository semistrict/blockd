use std::collections::BTreeMap;
use std::rc::Rc;

use blockd_exec::delay;

use super::backup::{claim_new_head, claim_new_head_with_stash};
use super::capture::write_record_copies;
use super::replica::initial_stash;
use super::{SharedHost, hydrate_mapping, publish_latest, publish_replica_head, replicate_latest};
use crate::journal::{JournalRecord, RecordKind, VsetConfig, VsetKind};
use crate::layout;
use crate::mapleaf::{LEAF_SPAN, LeafPtr, MapLeaf, span_is_memory};
use crate::protocol::{AdminReply, ReqId, StoreFault, Verdict};
use crate::segment::scan_segment;
use crate::types::{Epoch, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

type PageMap = BTreeMap<PageId, (crate::types::Gen, crate::segment::PageLoc)>;
type BaseArtifacts = (Vec<(u64, SegId)>, Vec<(u64, u64)>);
type BaseAdoption = (Verdict, RecordKind, PageMap, BTreeMap<u32, LeafPtr>);

pub async fn delete_base<W>(state: SharedHost, world: Rc<W>, req: ReqId, base: u64)
where
    W: Store + AdminIo + 'static,
{
    let retry = state.borrow().config.backup_retry;
    loop {
        match Store::delete(world.as_ref(), &layout::base_record_key(base)).await {
            Ok(_) => break,
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. })) => {
                AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
                return;
            }
        }
    }
    AdminIo::reply_admin(world.as_ref(), AdminReply::BaseDeleted { req, base }).await;
}

#[allow(clippy::too_many_lines)]
pub async fn keep_base<W>(state: SharedHost, world: Rc<W>, req: ReqId, vset: VsetId, base: u64)
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let Some((incarnation, mut record, retry)) = ({
        let host = state.borrow();
        host.vsets.get(&vset).and_then(|vset_state| {
            (vset_state.ready && vset_state.config.durability.uses_store())
                .then(|| {
                    Some((
                        vset_state.incarnation,
                        vset_state.pinned.clone()?,
                        host.config.backup_retry,
                    ))
                })
                .flatten()
        })
    }) else {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };

    for &span in record.leaves.keys() {
        let page = first_page(vset, span);
        if hydrate_mapping(&state, world.as_ref(), page, incarnation)
            .await
            .is_err()
        {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        }
    }
    let Some((mut segments, leaves)) = base_closure(&state, vset, incarnation, &record) else {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };
    segments.sort_unstable();
    segments.dedup();
    for (fence, segment) in segments {
        let name = layout::segment_blob(vset, fence, segment);
        let Ok(Some(bytes)) = Blobs::read(world.as_ref(), &name).await else {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        };
        if !scan_segment(&bytes).is_ok_and(|(owner, found_fence, found_segment, _)| {
            (owner, found_fence, found_segment) == (vset, fence, segment)
        }) || put_retry(
            &state,
            world.as_ref(),
            layout::base_segment_key(base, fence, segment),
            bytes,
            retry,
        )
        .await
        .is_none()
        {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        }
    }
    for (fence, id) in leaves {
        let name = layout::leaf_blob(vset, fence, id);
        let Ok(Some(bytes)) = Blobs::read(world.as_ref(), &name).await else {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        };
        let Ok(mut leaf) = MapLeaf::decode(vset, fence, id, &bytes) else {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        };
        for (_, _, _, location) in &mut leaf.entries {
            if location.base == 0 {
                location.base = base;
            }
        }
        if put_retry(
            &state,
            world.as_ref(),
            layout::base_leaf_key(base, fence, id),
            leaf.encode(VsetId(base), fence, id),
            retry,
        )
        .await
        .is_none()
        {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        }
    }
    for (_, location) in record.overlay.values_mut() {
        if location.base == 0 {
            location.base = base;
        }
    }
    if put_retry(
        &state,
        world.as_ref(),
        layout::base_record_key(base),
        record.encode(VsetId(base)),
        retry,
    )
    .await
    .is_none()
    {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    }
    AdminIo::reply_admin(world.as_ref(), AdminReply::BaseKept { req, base }).await;
}

fn base_closure(
    state: &SharedHost,
    vset: VsetId,
    incarnation: u64,
    record: &JournalRecord,
) -> Option<BaseArtifacts> {
    let host = state.borrow();
    let vset_state = host
        .vsets
        .get(&vset)
        .filter(|vset| vset.incarnation == incarnation)?;
    let mut segments = record
        .overlay
        .values()
        .filter(|(_, location)| location.base == 0)
        .map(|(_, location)| (location.fence, location.seg))
        .collect::<Vec<_>>();
    let mut leaves = Vec::new();
    for pointer in record.leaves.values().filter(|pointer| pointer.base == 0) {
        let (_, leaf_segments) = vset_state.leaf_blobs.get(pointer)?;
        segments.extend(leaf_segments.iter().copied());
        leaves.push((pointer.fence, pointer.id));
    }
    Some((segments, leaves))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn create_fork<W>(
    state: SharedHost,
    world: Rc<W>,
    req: ReqId,
    vset: VsetId,
    config: VsetConfig,
    base: u64,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    if state.borrow().vsets.contains_key(&vset) {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    }
    let retry = state.borrow().config.backup_retry;
    let Some(base_record) = get_base(&state, world.as_ref(), base, retry).await else {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };
    let stash = if config.durability == crate::journal::DurabilityMode::PeerStashed {
        let Some(stash) = initial_stash(&state, vset) else {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        };
        Some(stash)
    } else {
        None
    };
    let incarnation = state.borrow_mut().insert_fresh(vset, config);
    if config.durability.uses_store() {
        let claimed = if stash.is_some() {
            claim_new_head_with_stash(&state, world.as_ref(), vset, incarnation, stash).await
        } else {
            claim_new_head(&state, world.as_ref(), vset, incarnation).await
        };
        let Some(fence) = claimed else {
            state.borrow_mut().vsets.remove(&vset);
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        };
        let mut host = state.borrow_mut();
        let vset_state = host.vsets.get_mut(&vset).expect("fork insertion retained");
        vset_state.fence = fence;
        vset_state.head_version = Some(fence);
        vset_state.stash_assignment = stash;
        host.counters.assignment_claims += 1;
    }
    let (verdict, kind, overlay, leaves) = adopt_base(vset, base, config, &base_record);
    let record = {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|vset| vset.incarnation == incarnation)
        else {
            return;
        };
        vset_state.database = base_record.database;
        vset_state.mutation_seq = base_record.capture_seq;
        vset_state.overlay.clone_from(&overlay);
        vset_state.page_locs.clone_from(&overlay);
        vset_state.leaf_table.clone_from(&leaves);
        vset_state.next_gen = overlay
            .values()
            .map(|(generation, _)| generation.0 + 1)
            .max()
            .unwrap_or(0);
        let record = JournalRecord {
            config,
            seq: JournalSeq(0),
            fence: vset_state.fence,
            kind,
            capture_seq: base_record.capture_seq,
            sync_covered_through: 0,
            database: base_record.database,
            overlay,
            leaves,
            migrated_from: None,
        };
        vset_state.best_record = Some(record.clone());
        record
    };
    if !write_record_copies(&state, world.as_ref(), vset, &record).await {
        state.borrow_mut().vsets.remove(&vset);
        AdminIo::abort(world.as_ref(), "fork journal write failed").await;
        return;
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
        vset_state.ready = true;
        vset_state.next_seq = 1;
        vset_state
            .record_writes
            .insert(JournalSeq(0), (record.fence, 0));
        if matches!(record.kind, RecordKind::Checkpoint { .. }) {
            vset_state.pinned = Some(record.clone());
        }
        host.counters.records_written += 1;
    }
    if config.durability.uses_store() {
        loop {
            match config.durability {
                crate::journal::DurabilityMode::Backup => {
                    publish_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
                }
                crate::journal::DurabilityMode::PeerStashed => {
                    replicate_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
                    publish_replica_head(Rc::clone(&state), Rc::clone(&world), vset).await;
                }
                crate::journal::DurabilityMode::Local => unreachable!(),
            }
            let published = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
                vset_state.backed.is_some_and(|pointer| {
                    (pointer.capture_seq, pointer.seq) == (record.capture_seq, record.seq)
                })
            });
            if published {
                break;
            }
            if !state.borrow().vsets.contains_key(&vset) {
                AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
                return;
            }
            delay(retry).await;
        }
    }
    AdminIo::reply_admin(
        world.as_ref(),
        AdminReply::VsetForked { req, vset, verdict },
    )
    .await;
}

fn adopt_base(vset: VsetId, base: u64, config: VsetConfig, record: &JournalRecord) -> BaseAdoption {
    let whole = matches!(record.kind, RecordKind::Checkpoint { .. });
    let overlay = record
        .overlay
        .iter()
        .filter_map(|(page, entry)| {
            let page = PageId {
                volume: VolumeId {
                    vset,
                    idx: page.volume.idx,
                },
                page: page.page,
            };
            (config.contains(page) && (whole || !config.is_memory(page.volume.idx)))
                .then_some((page, *entry))
        })
        .collect();
    let leaves = record
        .leaves
        .iter()
        .filter(|(span, _)| whole || !span_is_memory(**span))
        .map(|(&span, &pointer)| {
            (
                span,
                LeafPtr {
                    base: if pointer.base == 0 {
                        base
                    } else {
                        pointer.base
                    },
                    fence: pointer.fence,
                    id: pointer.id,
                },
            )
        })
        .collect();
    let (verdict, kind) = match (record.config.kind, record.kind) {
        (VsetKind::Database, RecordKind::Commit) => (
            Verdict::DatabaseReady {
                synced_through: record.sync_covered_through,
            },
            RecordKind::Commit,
        ),
        (VsetKind::Compute, RecordKind::Checkpoint { vmstate, .. }) => (
            Verdict::Resume {
                epoch: Epoch(0),
                vmstate,
            },
            RecordKind::Checkpoint {
                epoch: Epoch(0),
                vmstate,
            },
        ),
        (VsetKind::Compute, RecordKind::Commit) => (Verdict::ColdBoot, RecordKind::Commit),
        (VsetKind::Database, RecordKind::Checkpoint { .. }) => {
            unreachable!("database checkpoint record")
        }
    };
    (verdict, kind, overlay, leaves)
}

async fn get_base<W: Store>(
    state: &SharedHost,
    world: &W,
    base: u64,
    retry: u64,
) -> Option<JournalRecord> {
    loop {
        match Store::get(world, &layout::base_record_key(base)).await {
            Ok(Some((_, bytes))) => return JournalRecord::decode(VsetId(base), &bytes).ok(),
            Ok(None)
            | Err(StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. })) => {
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
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
            Err(StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. })) => {
                return None;
            }
        }
    }
}

fn first_page(vset: VsetId, span: u32) -> PageId {
    let key = u64::from(span) * LEAF_SPAN;
    PageId {
        volume: VolumeId {
            vset,
            idx: VolumeIdx(u8::try_from(key >> 32).expect("volume index")),
        },
        page: PageNo(u32::try_from(key & 0xffff_ffff).expect("page number")),
    }
}
