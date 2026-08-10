use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::channel::{OneReceiver, OneSender, oneshot};
use blockd_exec::inject::{Injector, Lane, injector};
use blockd_exec::{join2, spawn, yield_now};

use super::cleanup_local;
use super::state::{CaptureKind, CaptureLease, DrainState, MutationOwner, SharedHost};
use crate::journal::{JournalRecord, MigrationSource, RecordKind, VsetConfig};
use crate::layout;
use crate::mapleaf::{LEAF_SPAN, LeafPtr, MapLeaf, span_of};
use crate::protocol::{AdminError, AdminResult, AdminSuccess, ReqId};
use crate::segment::{PageLoc, SegmentBatchBuilder, open_entry, scan_segment};
use crate::types::{Gen, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, GuestPause, Store};

const DRAIN_PAGES_PER_POLL: usize = 64;
const OVERLAY_MAX: usize = 2048;
const ROLL_THRESHOLD: usize = 256;
const COMPACT_BATCH: usize = 8;

type LeafWrite = (LeafPtr, Vec<u8>, BTreeSet<(u64, SegId)>);
type PageMap = BTreeMap<PageId, (Gen, crate::segment::PageLoc)>;

#[derive(Clone, Copy)]
struct Checkpoint {
    req: Option<ReqId>,
    epoch: crate::types::Epoch,
    vmstate: u64,
}

enum CheckpointDecision {
    Missing,
    Invalid,
    Existing(crate::types::Epoch),
    Busy(OneReceiver<()>),
    Reserved {
        incarnation: u64,
        epoch: crate::types::Epoch,
        cleanup: OneSender<Vec<PageId>>,
        protections: OneReceiver<Vec<PageId>>,
    },
}

fn reserve_checkpoint(state: &SharedHost, req: ReqId, vset: VsetId) -> CheckpointDecision {
    let mut host = state.borrow_mut();
    let Some(vset_state) = host.vsets.get_mut(&vset) else {
        return CheckpointDecision::Missing;
    };
    if !vset_state.ready || vset_state.config.kind != crate::journal::VsetKind::Compute {
        return CheckpointDecision::Invalid;
    }
    if let Some(&epoch) = vset_state.checkpoint_results.get(&req) {
        return CheckpointDecision::Existing(epoch);
    }
    if vset_state.operations.migration_running() {
        return CheckpointDecision::Invalid;
    }
    if vset_state.operations.mutation_blocked() {
        let (wake, wait) = oneshot();
        vset_state.mutation_waiters.push(wake);
        return CheckpointDecision::Busy(wait);
    }

    let epoch = crate::types::Epoch(vset_state.epoch.0 + 1);
    assert!(
        vset_state
            .operations
            .try_start_mutation(MutationOwner::Capture(CaptureKind::Checkpoint))
    );
    let (cleanup, protections) = oneshot();
    CheckpointDecision::Reserved {
        incarnation: vset_state.incarnation,
        epoch,
        cleanup,
        protections,
    }
}

pub(super) struct PausedGuest<W: GuestMem + AdminIo + 'static> {
    tracker: PauseTracker<W>,
    cancel: Injector<()>,
    cleanup_done: Option<OneReceiver<bool>>,
}

struct PauseTracker<W: GuestMem + AdminIo + 'static> {
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    incarnation: u64,
    pause: GuestPause,
    active: Rc<Cell<bool>>,
}

impl<W: GuestMem + AdminIo + 'static> Clone for PauseTracker<W> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            world: Rc::clone(&self.world),
            vset: self.vset,
            incarnation: self.incarnation,
            pause: self.pause,
            active: Rc::clone(&self.active),
        }
    }
}

impl<W: GuestMem + AdminIo + 'static> PauseTracker<W> {
    async fn resume(&self) -> bool {
        if !self.active.get() {
            return true;
        }
        let current = self
            .state
            .borrow()
            .vsets
            .get(&self.vset)
            .is_some_and(|vset| {
                vset.incarnation == self.incarnation && vset.operations.guest_resume_pending()
            });
        if !current {
            self.active.set(false);
            return true;
        }
        let resumed = GuestMem::resume(self.world.as_ref(), self.vset, Some(self.pause))
            .await
            .is_ok();
        if self.active.replace(false) {
            self.finish_pending();
        }
        if !resumed {
            self.state
                .borrow_mut()
                .fail("guest resume after capture failed");
        }
        resumed
    }

    fn disarm(&self) {
        if self.active.replace(false) {
            self.finish_pending();
        }
    }

    fn finish_pending(&self) {
        let waiters = {
            let mut host = self.state.borrow_mut();
            let Some(vset) = host
                .vsets
                .get_mut(&self.vset)
                .filter(|vset| vset.incarnation == self.incarnation)
            else {
                return;
            };
            vset.operations.finish_guest_resume();
            std::mem::take(&mut vset.mutation_waiters)
        };
        self.state.borrow_mut().schedule_vset(self.vset);
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

impl<W: GuestMem + AdminIo + 'static> PausedGuest<W> {
    fn new(
        state: &SharedHost,
        world: &Rc<W>,
        vset: VsetId,
        incarnation: u64,
        pause: GuestPause,
        protections: OneReceiver<Vec<PageId>>,
    ) -> Option<Self> {
        let active = Rc::new(Cell::new(true));
        {
            let mut host = state.borrow_mut();
            let vset_state = host
                .vsets
                .get_mut(&vset)
                .filter(|vset_state| vset_state.incarnation == incarnation)?;
            if !vset_state.operations.start_guest_resume() {
                return None;
            }
        }
        let tracker = PauseTracker {
            state: Rc::clone(state),
            world: Rc::clone(world),
            vset,
            incarnation,
            pause,
            active,
        };
        let (cancel, cancelled) = injector();
        let (cleanup_done, cleanup_wait) = oneshot();
        let cleanup = tracker.clone();
        spawn(async move {
            if cancelled.recv().await.is_some() {
                let mut cleaned = true;
                if let Ok(pages) = protections.await {
                    for page in pages {
                        if GuestMem::unprotect(cleanup.world.as_ref(), page)
                            .await
                            .is_err()
                        {
                            cleanup
                                .state
                                .borrow_mut()
                                .fail("guest unprotect after capture cancellation failed");
                            cleaned = false;
                            break;
                        }
                    }
                }
                let resumed = cleaned && cleanup.resume().await;
                let _ = cleanup_done.send(resumed);
            }
        })
        .detach();
        Some(Self {
            tracker,
            cancel,
            cleanup_done: Some(cleanup_wait),
        })
    }

    fn tracker(&self) -> PauseTracker<W> {
        self.tracker.clone()
    }

    pub(super) async fn resume(mut self) -> bool {
        let _ = self.cancel.push(Lane::Critical, ());
        match self.cleanup_done.take() {
            Some(done) => done.await == Ok(true),
            None => false,
        }
    }

    pub(super) fn disarm(self) {
        self.tracker.disarm();
        let _ = self.cancel.push(Lane::Critical, ());
    }
}

impl<W: GuestMem + AdminIo + 'static> Drop for PausedGuest<W> {
    fn drop(&mut self) {
        let _ = self.cancel.push(Lane::Critical, ());
    }
}

struct CompactionRescue {
    victim: (u64, SegId),
    entries: Vec<(PageId, Gen, PageLoc, Vec<u8>)>,
}

/// Verify and decompress the live entries of the sparsest local segments.
/// The serving map is rechecked when the capture reserves its cut, so reads
/// that race a newer capture are harmless stale work.
async fn prepare_compaction<W: Blobs>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
) -> Vec<CompactionRescue> {
    let candidates = {
        let host = state.borrow();
        let Some(vset_state) = host.vsets.get(&vset).filter(|state| state.ready) else {
            return Vec::new();
        };
        if vset_state.outbound.is_some()
            || vset_state.operations.migration_running()
            || vset_state.page_locs.len() < ROLL_THRESHOLD
        {
            return Vec::new();
        }
        let mut live_by_segment = BTreeMap::<(u64, SegId), u64>::new();
        for (_, location) in vset_state.page_locs.values() {
            if location.base == 0 {
                *live_by_segment
                    .entry((location.fence, location.seg))
                    .or_default() += u64::from(location.len);
            }
        }
        let mut candidates = vset_state
            .segment_blobs
            .iter()
            .filter_map(|&(fence, segment, size)| {
                let live = live_by_segment.get(&(fence, segment)).copied().unwrap_or(0);
                (live > 0 && live.saturating_mul(2) <= size).then_some((
                    live.saturating_mul(1_000_000) / size,
                    fence,
                    segment,
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.truncate(COMPACT_BATCH);
        candidates
    };

    let mut rescues = Vec::new();
    for (_, fence, segment) in candidates {
        let expected = {
            let host = state.borrow();
            let Some(vset_state) = host.vsets.get(&vset) else {
                break;
            };
            vset_state
                .page_locs
                .iter()
                .filter(|(_, (_, location))| {
                    location.base == 0 && (location.fence, location.seg) == (fence, segment)
                })
                .map(|(&page, &entry)| (page, entry))
                .collect::<BTreeMap<_, _>>()
        };
        let name = layout::segment_blob(vset, fence, segment);
        let Ok(Some(bytes)) = Blobs::read(world, &name).await else {
            continue;
        };
        let Ok((owner, blob_fence, blob_segment, entries)) = scan_segment(&bytes) else {
            continue;
        };
        if (owner, blob_fence, blob_segment) != (vset, fence, segment) {
            continue;
        }
        let mut decoded = Vec::new();
        let mut damaged = false;
        for (index, (page, generation, location)) in entries.into_iter().enumerate() {
            if expected.get(&page) != Some(&(generation, location)) {
                continue;
            }
            let start = location.offset as usize;
            let end = start.saturating_add(location.len as usize);
            let Some(frame) = bytes.get(start..end) else {
                damaged = true;
                break;
            };
            let Ok((decoded_page, decoded_generation, raw)) = open_entry(vset, frame) else {
                damaged = true;
                break;
            };
            if (decoded_page, decoded_generation) != (page, generation) {
                damaged = true;
                break;
            }
            decoded.push((page, generation, location, raw));
            if (index + 1) % DRAIN_PAGES_PER_POLL == 0 {
                yield_now().await;
            }
        }
        if !damaged && decoded.len() == expected.len() {
            rescues.push(CompactionRescue {
                victim: (fence, segment),
                entries: decoded,
            });
        }
    }
    rescues
}

pub async fn create_fresh_local<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    config: VsetConfig,
) -> Option<AdminResult>
where
    W: Blobs + AdminIo + 'static,
{
    let duplicate = state.borrow().vsets.contains_key(&vset);
    if duplicate {
        return Some(Err(AdminError::Busy));
    }
    let incarnation = {
        let mut host = state.borrow_mut();
        host.insert_fresh(vset, config)
    };
    if !finish_creation(Rc::clone(&state), world.as_ref(), vset, incarnation).await {
        state.borrow_mut().vsets.remove(&vset);
        state.borrow_mut().fail("local journal write failed");
        return None;
    }
    Some(Ok(AdminSuccess::VsetCreated { vset }))
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

pub(super) async fn finish_creation<W: Blobs>(
    state: SharedHost,
    world: &W,
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
        host.schedule_vset(vset);
    }
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
    capture_record(state, world, vset, None, None, None).await
}

pub async fn checkpoint_local<W>(
    state: SharedHost,
    world: Rc<W>,
    req: ReqId,
    vset: VsetId,
) -> Option<AdminResult>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let (incarnation, epoch, lease, protections) = loop {
        match reserve_checkpoint(&state, req, vset) {
            CheckpointDecision::Missing | CheckpointDecision::Invalid => {
                return Some(Err(AdminError::Rejected));
            }
            CheckpointDecision::Existing(epoch) => {
                return Some(Ok(AdminSuccess::CheckpointDone { vset, epoch }));
            }
            CheckpointDecision::Busy(wait) => {
                let _ = wait.await;
            }
            CheckpointDecision::Reserved {
                incarnation,
                epoch,
                cleanup,
                protections,
            } => {
                break (
                    incarnation,
                    epoch,
                    CaptureLease::new(
                        &state,
                        vset,
                        incarnation,
                        MutationOwner::Capture(CaptureKind::Checkpoint),
                        cleanup,
                    ),
                    protections,
                );
            }
        }
    };
    let Ok(pause) = GuestMem::pause(world.as_ref(), vset).await else {
        state.borrow_mut().fail("guest pause failed");
        return None;
    };
    let Some(paused) = PausedGuest::new(&state, &world, vset, incarnation, pause, protections)
    else {
        if GuestMem::resume(world.as_ref(), vset, Some(pause))
            .await
            .is_err()
        {
            state
                .borrow_mut()
                .fail("stale checkpoint guest resume failed");
        }
        return None;
    };
    let checkpoint = Checkpoint {
        req: Some(req),
        epoch,
        vmstate: pause.vmstate,
    };
    let captured = capture_record(
        Rc::clone(&state),
        Rc::clone(&world),
        vset,
        Some(checkpoint),
        Some((incarnation, lease)),
        Some(paused.tracker()),
    )
    .await;
    if !paused.resume().await {
        return None;
    }
    captured.map(|_| Ok(AdminSuccess::CheckpointDone { vset, epoch }))
}

#[allow(clippy::too_many_lines)]
async fn capture_record<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    checkpoint: Option<Checkpoint>,
    reservation: Option<(u64, CaptureLease)>,
    resumed: Option<PauseTracker<W>>,
) -> Option<JournalRecord>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let pre_reserved = reservation.is_some();
    let reserved_incarnation = reservation.as_ref().map(|(incarnation, _)| *incarnation);
    let capture_kind = checkpoint.map_or(CaptureKind::Writeback, |checkpoint| {
        if checkpoint.req.is_some() {
            CaptureKind::Checkpoint
        } else {
            CaptureKind::Migration
        }
    });
    let capture_owner = MutationOwner::Capture(capture_kind);
    let prepared_compaction = prepare_compaction(&state, world.as_ref(), vset).await;
    let (incarnation, seq, capture_seq, fence, pages, protect, first_seg, compact_victims) = {
        let mut host = state.borrow_mut();
        let vset_state = host.vsets.get(&vset)?;
        let flush_set = host
            .cache
            .unstable_pages_of(vset)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut rescues = Vec::new();
        let mut compact_victims = BTreeSet::new();
        for prepared in prepared_compaction {
            let prepared_pages = prepared
                .entries
                .iter()
                .map(|(page, generation, location, _)| (*page, (*generation, *location)))
                .collect::<BTreeMap<_, _>>();
            let all_live_covered = vset_state.page_locs.iter().all(|(page, entry)| {
                let location = entry.1;
                (location.base != 0 || (location.fence, location.seg) != prepared.victim)
                    || flush_set.contains(page)
                    || prepared_pages.get(page) == Some(entry)
            });
            if !all_live_covered {
                continue;
            }
            for (page, generation, location, bytes) in prepared.entries {
                if !flush_set.contains(&page)
                    && vset_state.page_locs.get(&page) == Some(&(generation, location))
                {
                    rescues.push((page, bytes));
                }
            }
            compact_victims.insert(prepared.victim);
        }
        let needs_commit = host.cache.has_dirty_of(vset)
            || !compact_victims.is_empty()
            || vset_state
                .pending_syncs
                .iter()
                .any(|sync| sync.barrier > vset_state.local_covered_through);
        if !vset_state.ready
            || (!pre_reserved
                && (vset_state.operations.mutation_blocked()
                    || vset_state.operations.migration_running()
                    || !needs_commit))
            || reserved_incarnation.is_some_and(|incarnation| {
                vset_state.incarnation != incarnation
                    || vset_state.operations.mutation_owner() != Some(capture_owner)
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
                .checked_add(
                    u64::try_from(pages.len() + rescues.len()).expect("page count fits u64"),
                )
                .expect("segment id overflow");
            let mut unread = BTreeMap::new();
            for &page in &pages {
                unread.insert(page, Gen(vset_state.next_gen));
                vset_state.next_gen += 1;
            }
            if !pre_reserved {
                assert!(vset_state.operations.try_start_mutation(capture_owner));
            }
            vset_state.operations.begin_drain(DrainState {
                unread,
                copied_on_fault: BTreeMap::new(),
                armed: pages.clone(),
                rescues: rescues
                    .into_iter()
                    .map(|(page, bytes)| {
                        let generation = Gen(vset_state.next_gen);
                        vset_state.next_gen += 1;
                        (page, generation, bytes)
                    })
                    .collect(),
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
            compact_victims,
        )
    };
    let lease = reservation.map_or_else(
        || {
            let (cleanup, protections) = oneshot();
            let cleanup_state = Rc::clone(&state);
            let cleanup_world = Rc::clone(&world);
            spawn(async move {
                let Ok(pages) = protections.await else {
                    return;
                };
                for page in pages {
                    if GuestMem::unprotect(cleanup_world.as_ref(), page)
                        .await
                        .is_err()
                    {
                        cleanup_state
                            .borrow_mut()
                            .fail("guest unprotect after capture failure failed");
                        break;
                    }
                }
                let waiters = {
                    let mut host = cleanup_state.borrow_mut();
                    let Some(vset_state) = host
                        .vsets
                        .get_mut(&vset)
                        .filter(|vset_state| vset_state.incarnation == incarnation)
                    else {
                        return;
                    };
                    vset_state.operations.finish_mutation(capture_owner);
                    let waiters = std::mem::take(&mut vset_state.mutation_waiters);
                    host.wake_pressure_waiter();
                    host.schedule_vset(vset);
                    waiters
                };
                for waiter in waiters {
                    let _ = waiter.send(());
                }
            })
            .detach();
            CaptureLease::new_with_serialized_cleanup(
                &state,
                vset,
                incarnation,
                capture_owner,
                cleanup,
            )
        },
        |(_, lease)| lease,
    );
    if !protect.is_empty()
        && GuestMem::arm_write_protect(world.as_ref(), &protect)
            .await
            .is_err()
    {
        state.borrow_mut().fail("guest write protection failed");
        return None;
    }

    let resume_after_capture = checkpoint.is_some_and(|checkpoint| checkpoint.req.is_some());
    let mut captured_pages = Vec::with_capacity(pages.len());
    let mut capture_valid = true;
    for (index, page) in pages.iter().copied().enumerate() {
        let observed = GuestMem::read_page(world.as_ref(), page).await;
        let captured = {
            let mut host = state.borrow_mut();
            host.vsets
                .get_mut(&vset)
                .filter(|state| state.incarnation == incarnation)
                .and_then(|vset_state| vset_state.operations.drain_mut())
                .and_then(|drain| match drain.unread.remove(&page) {
                    Some(generation) => Some((generation, observed)),
                    None => drain.copied_on_fault.remove(&page),
                })
        };
        let Some((generation, bytes)) = captured else {
            capture_valid = false;
            break;
        };
        captured_pages.push((page, generation, bytes));
        if (index + 1) % DRAIN_PAGES_PER_POLL == 0 {
            yield_now().await;
        }
    }
    if resume_after_capture
        && let Some(resumed) = resumed
        && !resumed.resume().await
    {
        return None;
    }
    if !capture_valid {
        return None;
    }

    let mut builder = SegmentBatchBuilder::new(vset, fence, first_seg);
    for (page, generation, bytes) in captured_pages {
        builder.add(page, generation, &bytes);
    }
    let rescues = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        std::mem::take(&mut vset_state.operations.drain_mut()?.rescues)
    };
    for (index, (page, generation, bytes)) in rescues.into_iter().enumerate() {
        builder.add(page, generation, &bytes);
        if (index + 1) % DRAIN_PAGES_PER_POLL == 0 {
            yield_now().await;
        }
    }

    let segment_blobs = builder.finish();
    let flushed_pages = pages.iter().copied().collect::<BTreeSet<_>>();
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
            state.borrow_mut().fail("local segment write failed");
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
                if let Some(drain) = vset_state.operations.drain_mut() {
                    drain.armed.retain(|armed| *armed != page);
                }
            }
            vset_state
                .segment_blobs
                .push((fence, *segment, bytes.len() as u64));
        }
        for &(page, _, _) in entries {
            if flushed_pages.contains(&page) {
                host.cache.end_flush(page);
            }
        }
        host.wake_pressure_waiter();
    }

    let (record_overlay, record_leaves, leaf_writes) = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        force_victim_spans(vset_state, &compact_victims);
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
            state.borrow_mut().fail("local map-leaf write failed");
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
            .filter(|sync| sync.barrier <= capture_seq)
            .map(|sync| sync.barrier)
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
            migrated_from: vset_state.peer_source.map(|host| MigrationSource {
                host,
                offer_fence: vset_state.peer_source_offer_fence,
            }),
        }
    };
    if !write_record_copies(&state, world.as_ref(), vset, &record).await {
        state.borrow_mut().fail("local journal write failed");
        return None;
    }

    let (syncs, waiters) = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        vset_state.best_record = Some(record.clone());
        vset_state.local_covered_through = record.sync_covered_through;
        let mut completed = Vec::new();
        let pending = std::mem::take(&mut vset_state.pending_syncs);
        for sync in pending {
            if sync.barrier <= vset_state.sync_ack_through {
                completed.push(sync);
            } else {
                vset_state.pending_syncs.push(sync);
            }
        }
        vset_state
            .record_writes
            .insert(record.seq, (fence, record.sync_covered_through));
        vset_state.operations.finish_mutation(capture_owner);
        let waiters = std::mem::take(&mut vset_state.mutation_waiters);
        if let Some(checkpoint) = checkpoint {
            vset_state.epoch = checkpoint.epoch;
            vset_state.pinned = Some(record.clone());
            if let Some(req) = checkpoint.req {
                vset_state.checkpoint_results.insert(req, checkpoint.epoch);
            }
        }
        host.counters.records_written += 1;
        host.counters.pages_flushed += pages.len() as u64;
        host.counters.segs_compacted += compact_victims.len() as u64;
        host.counters.pages_compacted += u64::try_from(
            segment_blobs
                .iter()
                .flat_map(|(_, _, entries)| entries)
                .filter(|(page, _, _)| !flushed_pages.contains(page))
                .count(),
        )
        .expect("compacted page count fits u64");
        host.counters.syncs_acked += completed.len() as u64;
        host.counters.checkpoints_done += u64::from(checkpoint.is_some());
        (completed, waiters)
    };
    lease.commit();
    for waiter in waiters {
        let _ = waiter.send(());
    }
    for sync in syncs {
        sync.resolve(true);
    }
    if cleanup_local(Rc::clone(&state), world.as_ref(), vset, incarnation)
        .await
        .is_err()
    {
        state.borrow_mut().fail("local reclaim failed");
        return None;
    }
    Some(record)
}

pub(super) async fn capture_migration<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
) -> Option<(JournalRecord, PausedGuest<W>)>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let (incarnation, epoch, lease, protections) = loop {
        enum Decision {
            Invalid,
            Busy(OneReceiver<()>),
            Reserved(
                u64,
                crate::types::Epoch,
                blockd_exec::channel::OneSender<Vec<PageId>>,
                OneReceiver<Vec<PageId>>,
            ),
        }
        let reserved = {
            let mut host = state.borrow_mut();
            let vset_state = host.vsets.get_mut(&vset)?;
            if !vset_state.ready || vset_state.config.kind != crate::journal::VsetKind::Compute {
                Decision::Invalid
            } else if vset_state.operations.mutation_blocked() {
                let (wake, wait) = oneshot();
                vset_state.mutation_waiters.push(wake);
                Decision::Busy(wait)
            } else {
                let epoch = crate::types::Epoch(vset_state.epoch.0 + 1);
                assert!(
                    vset_state
                        .operations
                        .try_start_mutation(MutationOwner::Capture(CaptureKind::Migration))
                );
                let (cleanup, protections) = oneshot();
                Decision::Reserved(vset_state.incarnation, epoch, cleanup, protections)
            }
        };
        match reserved {
            Decision::Invalid => return None,
            Decision::Busy(wait) => {
                let _ = wait.await;
            }
            Decision::Reserved(incarnation, epoch, cleanup, protections) => {
                break (
                    incarnation,
                    epoch,
                    CaptureLease::new(
                        &state,
                        vset,
                        incarnation,
                        MutationOwner::Capture(CaptureKind::Migration),
                        cleanup,
                    ),
                    protections,
                );
            }
        }
    };
    let Ok(pause) = GuestMem::pause(world.as_ref(), vset).await else {
        state.borrow_mut().fail("guest pause failed");
        return None;
    };
    let Some(paused) = PausedGuest::new(&state, &world, vset, incarnation, pause, protections)
    else {
        if GuestMem::resume(world.as_ref(), vset, Some(pause))
            .await
            .is_err()
        {
            state
                .borrow_mut()
                .fail("stale migration guest resume failed");
        }
        return None;
    };
    let record = capture_record(
        Rc::clone(&state),
        Rc::clone(&world),
        vset,
        Some(Checkpoint {
            req: None,
            epoch,
            vmstate: pause.vmstate,
        }),
        Some((incarnation, lease)),
        None,
    )
    .await;
    if let Some(record) = record {
        Some((record, paused))
    } else {
        let _ = paused.resume().await;
        None
    }
}

fn force_victim_spans(state: &mut super::VsetState, victims: &BTreeSet<(u64, SegId)>) {
    if victims.is_empty() {
        return;
    }
    let spans = state
        .leaf_table
        .iter()
        .filter_map(|(&span, pointer)| {
            state
                .leaf_blobs
                .get(pointer)
                .is_some_and(|(_, segments)| {
                    segments.iter().any(|segment| victims.contains(segment))
                })
                .then_some(span)
        })
        .collect::<Vec<_>>();
    for span in spans {
        state.leaf_table.remove(&span);
        state.overlay.extend(
            state
                .page_locs
                .iter()
                .filter(|(page, _)| span_of(**page) == span)
                .map(|(&page, &entry)| (page, entry)),
        );
    }
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
    let (had_primary, had_mirror) = {
        let mut host = state.borrow_mut();
        let had_primary = host.blob_sizes.contains_key(&primary_name);
        let had_mirror = host.blob_sizes.contains_key(&mirror_name);
        if !host
            .try_reserve_metadata_blobs(&[(primary_name.clone(), len), (mirror_name.clone(), len)])
        {
            return false;
        }
        (had_primary, had_mirror)
    };
    let (primary, mirror) = join2(
        Blobs::write(world, primary_name.clone(), bytes.clone()),
        Blobs::write(world, mirror_name.clone(), bytes),
    )
    .await;
    if primary.is_ok() {
        state.borrow_mut().record_blob(primary_name, len);
    } else if !had_primary {
        state.borrow_mut().blob_sizes.remove(&primary_name);
    }
    if mirror.is_ok() {
        state.borrow_mut().record_blob(mirror_name, len);
    } else if !had_mirror {
        state.borrow_mut().blob_sizes.remove(&mirror_name);
    }
    primary.is_ok() && mirror.is_ok()
}
