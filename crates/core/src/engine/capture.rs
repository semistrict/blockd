use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::{join2, yield_now};

use super::cleanup_local;
use super::state::{CaptureLease, DrainState, SharedHost};
use crate::journal::{JournalRecord, RecordKind, VsetConfig};
use crate::layout;
use crate::mapleaf::{LEAF_SPAN, LeafPtr, MapLeaf, span_of};
use crate::protocol::{AdminReply, ReqId};
use crate::segment::SegmentBatchBuilder;
use crate::types::{Gen, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Store};

const DRAIN_PAGES_PER_POLL: usize = 64;
const OVERLAY_MAX: usize = 2048;
const ROLL_THRESHOLD: usize = 256;

type LeafWrite = (LeafPtr, Vec<u8>, BTreeSet<(u64, SegId)>);
type PageMap = BTreeMap<PageId, (Gen, crate::segment::PageLoc)>;

#[derive(Clone, Copy)]
struct Checkpoint {
    req: Option<ReqId>,
    epoch: crate::types::Epoch,
    vmstate: u64,
}

pub async fn create_fresh_local<W>(
    state: SharedHost,
    world: Rc<W>,
    req: ReqId,
    vset: VsetId,
    config: VsetConfig,
) where
    W: Blobs + AdminIo + 'static,
{
    let duplicate = state.borrow().vsets.contains_key(&vset);
    if duplicate {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    }
    let incarnation = {
        let mut host = state.borrow_mut();
        host.insert_fresh(vset, config)
    };
    if !finish_creation(Rc::clone(&state), world.as_ref(), req, vset, incarnation).await {
        state.borrow_mut().vsets.remove(&vset);
        AdminIo::abort(world.as_ref(), "local journal write failed").await;
    }
}

fn initial_record(config: VsetConfig, fence: u64) -> JournalRecord {
    JournalRecord {
        config,
        seq: JournalSeq(0),
        fence,
        kind: RecordKind::Commit,
        capture_seq: 0,
        sync_covered_through: 0,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    }
}

pub(super) async fn finish_creation<W: Blobs + AdminIo>(
    state: SharedHost,
    world: &W,
    req: ReqId,
    vset: VsetId,
    incarnation: u64,
) -> bool {
    let Some((config, fence)) = state
        .borrow()
        .vsets
        .get(&vset)
        .filter(|state| state.incarnation == incarnation)
        .map(|state| (state.config, state.fence))
    else {
        return false;
    };
    let record = initial_record(config, fence);
    if !write_record_copies(&state, world, vset, &record).await {
        return false;
    }
    {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)
        else {
            return false;
        };
        vset_state.ready = true;
        vset_state.next_seq = 1;
        vset_state.best_record = Some(record.clone());
        vset_state.record_writes.insert(record.seq, (fence, 0));
        host.counters.records_written += 1;
    }
    AdminIo::reply_admin(world, AdminReply::VsetCreated { req, vset }).await;
    true
}

pub async fn capture_local<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
) -> Option<JournalRecord>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    capture_record(state, world, vset, None, None).await
}

pub async fn checkpoint_local<W>(state: SharedHost, world: Rc<W>, req: ReqId, vset: VsetId)
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let (incarnation, epoch, lease) = loop {
        enum Decision {
            Missing,
            Invalid,
            Existing(crate::types::Epoch),
            Busy,
            Reserved {
                incarnation: u64,
                epoch: crate::types::Epoch,
            },
        }
        let decision = {
            let mut host = state.borrow_mut();
            if let Some(vset_state) = host.vsets.get_mut(&vset) {
                if !vset_state.ready || vset_state.config.kind != crate::journal::VsetKind::Compute
                {
                    Decision::Invalid
                } else if let Some(&epoch) = vset_state.checkpoint_results.get(&req) {
                    Decision::Existing(epoch)
                } else if vset_state.commit_running || vset_state.checkpoint_running {
                    Decision::Busy
                } else {
                    let epoch = crate::types::Epoch(vset_state.epoch.0 + 1);
                    vset_state.commit_running = true;
                    vset_state.checkpoint_running = true;
                    Decision::Reserved {
                        incarnation: vset_state.incarnation,
                        epoch,
                    }
                }
            } else {
                Decision::Missing
            }
        };
        match decision {
            Decision::Missing | Decision::Invalid => {
                AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
                return;
            }
            Decision::Existing(epoch) => {
                AdminIo::reply_admin(
                    world.as_ref(),
                    AdminReply::CheckpointDone { req, vset, epoch },
                )
                .await;
                return;
            }
            Decision::Busy => yield_now().await,
            Decision::Reserved { incarnation, epoch } => {
                break (
                    incarnation,
                    epoch,
                    CaptureLease::new(&state, vset, incarnation),
                );
            }
        }
    };
    let vmstate = GuestMem::pause(world.as_ref(), vset).await;
    let checkpoint = Checkpoint {
        req: Some(req),
        epoch,
        vmstate,
    };
    let _ = capture_record(
        state,
        world,
        vset,
        Some(checkpoint),
        Some((incarnation, lease)),
    )
    .await;
}

#[allow(clippy::too_many_lines)]
async fn capture_record<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    checkpoint: Option<Checkpoint>,
    reservation: Option<(u64, CaptureLease)>,
) -> Option<JournalRecord>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let pre_reserved = reservation.is_some();
    let reserved_incarnation = reservation.as_ref().map(|(incarnation, _)| *incarnation);
    let (incarnation, seq, capture_seq, fence, pages, protect, first_seg) = {
        let mut host = state.borrow_mut();
        let vset_state = host.vsets.get(&vset)?;
        let needs_commit = host.cache.has_dirty_of(vset)
            || vset_state
                .pending_syncs
                .iter()
                .any(|(_, barrier)| *barrier > vset_state.local_covered_through);
        if !vset_state.ready
            || (!pre_reserved
                && (vset_state.commit_running || vset_state.checkpoint_running || !needs_commit))
            || reserved_incarnation.is_some_and(|incarnation| {
                vset_state.incarnation != incarnation
                    || !vset_state.commit_running
                    || !vset_state.checkpoint_running
            })
        {
            return None;
        }
        let incarnation = vset_state.incarnation;
        let pages = host.cache.unstable_pages_of(vset);
        let protect = host.cache.dirty_pages_of(vset);
        let (seq, capture_seq, first_seg, fence) = {
            let vset_state = host.vsets.get_mut(&vset).expect("just observed");
            let seq = JournalSeq(vset_state.next_seq);
            vset_state.next_seq += 1;
            let capture_seq = vset_state.mutation_seq;
            let first_seg = SegId(vset_state.next_seg);
            vset_state.next_seg = vset_state
                .next_seg
                .checked_add(u64::try_from(pages.len()).expect("page count fits u64"))
                .expect("segment id overflow");
            let mut unread = BTreeMap::new();
            for &page in &pages {
                unread.insert(page, Gen(vset_state.next_gen));
                vset_state.next_gen += 1;
            }
            vset_state.commit_running = true;
            vset_state.drain = Some(DrainState {
                seq,
                capture_seq,
                unread,
                copied_on_fault: BTreeMap::new(),
                armed: pages.clone(),
            });
            (seq, capture_seq, first_seg, vset_state.fence)
        };
        for &page in &pages {
            host.cache.begin_flush(page);
        }
        (
            incarnation,
            seq,
            capture_seq,
            fence,
            pages,
            protect,
            first_seg,
        )
    };
    let lease = reservation.map_or_else(
        || CaptureLease::new(&state, vset, incarnation),
        |(_, lease)| lease,
    );
    if !protect.is_empty() {
        GuestMem::arm_write_protect(world.as_ref(), &protect).await;
    }
    if checkpoint.is_some_and(|checkpoint| checkpoint.req.is_some()) {
        GuestMem::resume(world.as_ref(), vset).await;
    }

    let mut builder = SegmentBatchBuilder::new(vset, fence, first_seg);
    for (index, page) in pages.iter().copied().enumerate() {
        let observed = GuestMem::read_page(world.as_ref(), page).await;
        let (generation, bytes) = {
            let mut host = state.borrow_mut();
            let vset_state = host
                .vsets
                .get_mut(&vset)
                .filter(|state| state.incarnation == incarnation)?;
            let drain = vset_state.drain.as_mut()?;
            match drain.unread.remove(&page) {
                Some(generation) => (generation, observed),
                None => drain.copied_on_fault.remove(&page)?,
            }
        };
        builder.add(page, generation, &bytes);
        if (index + 1) % DRAIN_PAGES_PER_POLL == 0 {
            yield_now().await;
        }
    }

    let segment_blobs = builder.finish();
    for (segment, bytes, entries) in &segment_blobs {
        let name = layout::segment_blob(vset, fence, *segment);
        if !state
            .borrow_mut()
            .try_reserve_blob(name.clone(), bytes.len() as u64)
        {
            return None;
        }
        if Blobs::write(world.as_ref(), name.clone(), bytes.clone())
            .await
            .is_err()
        {
            AdminIo::abort(world.as_ref(), "local segment write failed").await;
            return None;
        }
        let mut host = state.borrow_mut();
        host.record_blob(name, bytes.len() as u64);
        {
            let vset_state = host
                .vsets
                .get_mut(&vset)
                .filter(|state| state.incarnation == incarnation)?;
            for &(page, generation, location) in entries {
                vset_state.page_locs.insert(page, (generation, location));
                vset_state.overlay.insert(page, (generation, location));
                if let Some(drain) = vset_state.drain.as_mut() {
                    drain.armed.retain(|armed| *armed != page);
                }
            }
            vset_state
                .segment_blobs
                .push((fence, *segment, bytes.len() as u64));
        }
        for &(page, _, _) in entries {
            host.cache.end_flush(page);
        }
        host.counters.pages_flushed += entries.len() as u64;
        host.wake_pressure_waiter();
    }

    let (record_overlay, record_leaves, leaf_writes) = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        shard_map(vset_state, vset)
    };
    for (pointer, bytes, segments) in &leaf_writes {
        let name = layout::leaf_blob(vset, pointer.fence, pointer.id);
        if !state
            .borrow_mut()
            .try_reserve_blob(name.clone(), bytes.len() as u64)
        {
            return None;
        }
        if Blobs::write(world.as_ref(), name.clone(), bytes.clone())
            .await
            .is_err()
        {
            AdminIo::abort(world.as_ref(), "local map-leaf write failed").await;
            return None;
        }
        let mut host = state.borrow_mut();
        host.record_blob(name, bytes.len() as u64);
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        vset_state
            .leaf_blobs
            .insert(*pointer, (bytes.len() as u64, segments.clone()));
        host.counters.leaf_rolls += 1;
    }
    {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        vset_state.overlay.clone_from(&record_overlay);
        vset_state.leaf_table.clone_from(&record_leaves);
    }

    let record = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        let covered = vset_state
            .pending_syncs
            .iter()
            .filter(|(_, barrier)| *barrier <= capture_seq)
            .map(|(_, barrier)| *barrier)
            .fold(vset_state.local_covered_through, u64::max);
        JournalRecord {
            config: vset_state.config,
            seq,
            fence,
            kind: checkpoint.map_or(RecordKind::Commit, |checkpoint| RecordKind::Checkpoint {
                epoch: checkpoint.epoch,
                vmstate: checkpoint.vmstate,
            }),
            capture_seq,
            sync_covered_through: covered,
            database: vset_state.database,
            overlay: record_overlay,
            leaves: record_leaves,
            migrated_from: None,
        }
    };
    if !write_record_copies(&state, world.as_ref(), vset, &record).await {
        AdminIo::abort(world.as_ref(), "local journal write failed").await;
        return None;
    }

    let syncs = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        vset_state.best_record = Some(record.clone());
        vset_state.local_covered_through = record.sync_covered_through;
        if !vset_state.config.durability.requires_peer_sync() {
            vset_state.sync_ack_through =
                vset_state.sync_ack_through.max(record.sync_covered_through);
        }
        let mut completed = Vec::new();
        vset_state.pending_syncs.retain(|(req, barrier)| {
            if *barrier <= vset_state.sync_ack_through {
                completed.push(*req);
                false
            } else {
                true
            }
        });
        vset_state
            .record_writes
            .insert(record.seq, (fence, record.sync_covered_through));
        vset_state.drain = None;
        vset_state.commit_running = false;
        vset_state.checkpoint_running = false;
        if let Some(checkpoint) = checkpoint {
            vset_state.epoch = checkpoint.epoch;
            vset_state.pinned = Some(record.clone());
            if let Some(req) = checkpoint.req {
                vset_state.checkpoint_results.insert(req, checkpoint.epoch);
            }
        }
        host.counters.records_written += 1;
        host.counters.syncs_acked += completed.len() as u64;
        host.counters.checkpoints_done += u64::from(checkpoint.is_some());
        completed
    };
    lease.commit();
    for req in syncs {
        GuestMem::sync_ok(world.as_ref(), req).await;
    }
    if let Some(checkpoint) = checkpoint
        && let Some(req) = checkpoint.req
    {
        AdminIo::reply_admin(
            world.as_ref(),
            AdminReply::CheckpointDone {
                req,
                vset,
                epoch: checkpoint.epoch,
            },
        )
        .await;
    }
    if cleanup_local(Rc::clone(&state), world.as_ref(), vset, incarnation)
        .await
        .is_err()
    {
        AdminIo::abort(world.as_ref(), "local reclaim failed").await;
        return None;
    }
    Some(record)
}

pub async fn capture_migration<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
) -> Option<JournalRecord>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let (incarnation, epoch, lease) = loop {
        let reserved = {
            let mut host = state.borrow_mut();
            let vset_state = host.vsets.get_mut(&vset)?;
            if !vset_state.ready
                || vset_state.config.kind != crate::journal::VsetKind::Compute
                || vset_state.commit_running
                || vset_state.checkpoint_running
            {
                None
            } else {
                let epoch = crate::types::Epoch(vset_state.epoch.0 + 1);
                vset_state.commit_running = true;
                vset_state.checkpoint_running = true;
                Some((vset_state.incarnation, epoch))
            }
        };
        if let Some((incarnation, epoch)) = reserved {
            break (
                incarnation,
                epoch,
                CaptureLease::new(&state, vset, incarnation),
            );
        }
        yield_now().await;
    };
    let vmstate = GuestMem::pause(world.as_ref(), vset).await;
    capture_record(
        state,
        world,
        vset,
        Some(Checkpoint {
            req: None,
            epoch,
            vmstate,
        }),
        Some((incarnation, lease)),
    )
    .await
}

pub(super) fn shard_map(
    state: &mut super::VsetState,
    vset: VsetId,
) -> (PageMap, BTreeMap<u32, LeafPtr>, Vec<LeafWrite>) {
    let mut overlay = state.overlay.clone();
    let mut leaf_table = state.leaf_table.clone();
    let mut span_counts = BTreeMap::<u32, usize>::new();
    for &page in overlay.keys() {
        *span_counts.entry(span_of(page)).or_default() += 1;
    }
    let mut to_roll = span_counts
        .iter()
        .filter_map(|(&span, &count)| (count >= ROLL_THRESHOLD).then_some(span))
        .collect::<BTreeSet<_>>();
    let mut remaining = overlay.len()
        - to_roll
            .iter()
            .map(|span| span_counts.get(span).copied().unwrap_or(0))
            .sum::<usize>();
    while remaining > OVERLAY_MAX {
        let Some((&span, &count)) = span_counts
            .iter()
            .filter(|(span, _)| !to_roll.contains(span))
            .max_by_key(|&(span, count)| (*count, *span))
        else {
            break;
        };
        to_roll.insert(span);
        remaining -= count;
    }

    let mut writes = Vec::new();
    for span in to_roll {
        let lo_key = u64::from(span) * LEAF_SPAN;
        let idx = VolumeIdx(u8::try_from(lo_key >> 32).expect("volume index"));
        let page_at = |key: u64| PageId {
            volume: VolumeId { vset, idx },
            page: PageNo(u32::try_from(key & 0xffff_ffff).expect("page number")),
        };
        let lo = page_at(lo_key);
        let hi = page_at(lo_key + LEAF_SPAN - 1);
        let content = state
            .page_locs
            .range(lo..=hi)
            .map(|(&page, &entry)| (page, entry))
            .collect::<BTreeMap<_, _>>();
        let id = state.next_leaf;
        state.next_leaf += 1;
        let pointer = LeafPtr {
            base: 0,
            fence: state.fence,
            id,
        };
        let entries = content
            .iter()
            .map(|(page, &(generation, location))| {
                (page.volume.idx, page.page, generation, location)
            })
            .collect();
        let segments = content
            .values()
            .filter(|(_, location)| location.base == 0)
            .map(|(_, location)| (location.fence, location.seg))
            .collect();
        overlay.retain(|page, _| span_of(*page) != span);
        leaf_table.insert(span, pointer);
        let bytes = MapLeaf { span, entries }.encode(vset, state.fence, id);
        writes.push((pointer, bytes, segments));
    }
    (overlay, leaf_table, writes)
}

pub(super) async fn write_record_copies<W: Blobs>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    record: &JournalRecord,
) -> bool {
    let bytes = record.encode(vset);
    let primary_name = layout::journal_blob(vset, record.fence, record.seq);
    let mirror_name = layout::journal_mirror_blob(vset, record.fence, record.seq);
    let len = bytes.len() as u64;
    let (primary, mirror) = join2(
        Blobs::write(world, primary_name.clone(), bytes.clone()),
        Blobs::write(world, mirror_name.clone(), bytes),
    )
    .await;
    if primary.is_ok() {
        state.borrow_mut().record_blob(primary_name, len);
    }
    if mirror.is_ok() {
        state.borrow_mut().record_blob(mirror_name, len);
    }
    primary.is_ok() && mirror.is_ok()
}
