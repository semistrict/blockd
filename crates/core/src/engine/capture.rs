use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::channel::{OneReceiver, OneSender, oneshot};
use blockd_exec::inject::{Injector, Lane, injector};
use blockd_exec::{join2, spawn, yield_now};

use super::cleanup_local;
use super::state::{CaptureKind, CaptureLease, DrainState, MutationOwner, SharedHost, VsetState};
use crate::blx::{BlockKey, open_object, replace_state_block};
use crate::format::checksum64;
use crate::journal::{JournalRecord, MigrationSource, RecordKind, VsetConfig, VsetKind};
use crate::layout;
use crate::manifest::ObjectRef;
use crate::mapleaf::LeafPtr;
use crate::protocol::{AdminError, AdminResult, AdminSuccess, ReqId};
use crate::segment::{PageLoc, SegmentBatchBuilder, open_entry, scan_segment};
use crate::types::{Gen, JournalSeq, PageId, SegId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, GuestPause, Store};

const DRAIN_PAGES_PER_POLL: usize = 64;
const ROLL_THRESHOLD: usize = 256;
const COMPACT_BATCH: usize = 8;

type PageMap = BTreeMap<PageId, (Gen, crate::segment::PageLoc)>;

pub(super) fn recovery_files(vset: &VsetState) -> Vec<ObjectRef> {
    let mut needed_segments = vset
        .page_locs
        .values()
        .filter_map(|(_, location)| (location.base == 0).then_some((location.fence, location.seg)))
        .collect::<BTreeSet<_>>();
    needed_segments.extend(vset.tombstone_segments.iter().copied());
    needed_segments.extend(vset.vmm_segments.iter().copied());

    let needed_batches = vset
        .segment_refs
        .iter()
        .filter(|(segment, _)| needed_segments.contains(segment))
        .map(|(_, file)| (file.identity.writer_fence, file.batch_id))
        .collect::<BTreeSet<_>>();
    vset.segment_refs
        .values()
        .filter(|file| needed_batches.contains(&(file.identity.writer_fence, file.batch_id)))
        .copied()
        .collect()
}

#[derive(Clone)]
struct Checkpoint {
    req: Option<ReqId>,
    epoch: crate::types::Epoch,
    vmstate: u64,
    vmstate_bytes: Vec<u8>,
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
            pause: self.pause.clone(),
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
        let resumed = GuestMem::resume(self.world.as_ref(), self.vset, Some(self.pause.clone()))
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

    pub(super) async fn commit(&mut self) -> bool {
        let committed = GuestMem::commit_pause(
            self.tracker.world.as_ref(),
            self.tracker.vset,
            self.tracker.pause.clone(),
        )
        .await
        .is_ok();
        if !committed {
            self.tracker
                .state
                .borrow_mut()
                .fail("guest stop after migration cut failed");
        }
        committed
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
        post_state_checksum: 0,
        database: crate::journal::DatabaseMeta::default(),
        files: Vec::new(),
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
    let Some(paused) = PausedGuest::new(
        &state,
        &world,
        vset,
        incarnation,
        pause.clone(),
        protections,
    ) else {
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
        vmstate_bytes: pause.vmstate_bytes.clone(),
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
    let capture_kind = checkpoint
        .as_ref()
        .map_or(CaptureKind::Writeback, |checkpoint| {
            if checkpoint.req.is_some() {
                CaptureKind::Checkpoint
            } else {
                CaptureKind::Migration
            }
        });
    let capture_owner = MutationOwner::Capture(capture_kind);
    let prepared_compaction = prepare_compaction(&state, world.as_ref(), vset).await;
    let (
        incarnation,
        seq,
        capture_seq,
        fence,
        pages,
        protect,
        first_seg,
        compact_victims,
        pending_tombstones,
    ) = {
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
            || !vset_state.pending_tombstones.is_empty()
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
        let (seq, capture_seq, first_seg, fence, pending_tombstones) = {
            let vset_state = host.vsets.get_mut(&vset).expect("just observed");
            let seq = JournalSeq(vset_state.next_seq);
            vset_state.next_seq += 1;
            let capture_seq = vset_state.mutation_seq;
            let first_seg = SegId(vset_state.next_seg);
            vset_state.next_seg = vset_state
                .next_seg
                .checked_add(
                    u64::try_from(
                        pages.len() + rescues.len() + vset_state.pending_tombstones.len(),
                    )
                    .expect("entry count fits u64"),
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
            let pending_keys = vset_state
                .pending_tombstones
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let mut pending_tombstones = Vec::with_capacity(pending_keys.len());
            for &key in &pending_keys {
                let generation = if key.space == crate::blx::BlockSpace::Vmm {
                    Gen(seq.0)
                } else {
                    let generation = Gen(vset_state.next_gen);
                    vset_state.next_gen += 1;
                    generation
                };
                pending_tombstones.push((key, generation));
            }
            (
                seq,
                capture_seq,
                first_seg,
                vset_state.fence,
                pending_tombstones,
            )
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
            pending_tombstones,
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

    let resume_after_capture = checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.req.is_some());
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

    let mut batch_pages = captured_pages;
    let rescues = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        std::mem::take(&mut vset_state.operations.drain_mut()?.rescues)
    };
    for (index, rescued) in rescues.into_iter().enumerate() {
        batch_pages.push(rescued);
        if (index + 1) % DRAIN_PAGES_PER_POLL == 0 {
            yield_now().await;
        }
    }

    let (pre_state_checksum, post_state_checksum, block_checksums) = {
        let host = state.borrow();
        let vset_state = host.vsets.get(&vset)?;
        let mut checksum = vset_state.state_checksum;
        let mut blocks = vset_state.block_checksums.clone();
        for (page, generation, bytes) in &batch_pages {
            replace_state_block(
                &mut checksum,
                &mut blocks,
                BlockKey::from_page(VsetKind::Compute, *page),
                Some((*generation, checksum64(bytes))),
            );
        }
        if let Some(checkpoint) = checkpoint.as_ref() {
            let old_vmm = blocks
                .keys()
                .filter(|key| key.space == crate::blx::BlockSpace::Vmm)
                .copied()
                .collect::<Vec<_>>();
            for key in old_vmm {
                replace_state_block(&mut checksum, &mut blocks, key, None);
            }
            for (block, chunk) in checkpoint
                .vmstate_bytes
                .chunks(crate::types::page_size())
                .enumerate()
            {
                let mut padded = vec![0; crate::types::page_size()];
                padded[..chunk.len()].copy_from_slice(chunk);
                replace_state_block(
                    &mut checksum,
                    &mut blocks,
                    BlockKey {
                        space: crate::blx::BlockSpace::Vmm,
                        volume: 0,
                        block: u32::try_from(block).expect("VMM block fits u32"),
                    },
                    Some((Gen(seq.0), checksum64(&padded))),
                );
            }
        }
        (vset_state.state_checksum, checksum, blocks)
    };
    let mut builder = SegmentBatchBuilder::new_for_record_with_checksums(
        VsetKind::Compute,
        vset,
        fence,
        first_seg,
        seq.0,
        pre_state_checksum,
        post_state_checksum,
    );
    for (page, generation, bytes) in &batch_pages {
        builder.add(*page, *generation, bytes);
    }
    for &(key, generation) in &pending_tombstones {
        if key.space == crate::blx::BlockSpace::Vmm {
            builder.add_vmm_tombstone(key.block, generation);
        } else if let Some(page) = key.to_page(VsetKind::Compute, vset) {
            builder
                .try_add_tombstone(page, generation)
                .expect("reserved segment IDs cover pending tombstones");
        }
    }
    if let Some(checkpoint) = checkpoint.as_ref() {
        let old_vmm_blocks = state
            .borrow()
            .vsets
            .get(&vset)?
            .block_checksums
            .keys()
            .filter(|key| key.space == crate::blx::BlockSpace::Vmm)
            .map(|key| key.block)
            .collect::<BTreeSet<_>>();
        let new_count = checkpoint
            .vmstate_bytes
            .len()
            .div_ceil(crate::types::page_size());
        for block in old_vmm_blocks {
            if block as usize >= new_count {
                builder.add_vmm_tombstone(block, Gen(seq.0));
            }
        }
        builder.add_vmm_snapshot(Gen(seq.0), &checkpoint.vmstate_bytes);
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
        if super::blob::write(&state, world.as_ref(), name.clone(), bytes.clone())
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
            let object = open_object(bytes).ok()?;
            vset_state
                .segment_refs
                .insert((fence, *segment), ObjectRef::from_blx(&object));
            if object
                .footer
                .entries
                .iter()
                .any(|entry| entry.kind == crate::blx::EntryKind::Tombstone)
            {
                vset_state.tombstone_segments.insert((fence, *segment));
            }
            vset_state.state_checksum = post_state_checksum;
            vset_state.block_checksums.clone_from(&block_checksums);
        }
        for &(page, _, _) in entries {
            if flushed_pages.contains(&page) {
                host.cache.end_flush(page);
            }
        }
        host.wake_pressure_waiter();
    }
    if checkpoint.is_some() {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        vset_state.vmm_segments = segment_blobs
            .iter()
            .map(|(segment, _, _)| (fence, *segment))
            .collect();
    }

    let (record_overlay, record_leaves): (PageMap, BTreeMap<u32, LeafPtr>) = {
        let host = state.borrow();
        let vset_state = host
            .vsets
            .get(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        (vset_state.page_locs.clone(), BTreeMap::new())
    };
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
            kind: checkpoint
                .as_ref()
                .map_or(RecordKind::Commit, |checkpoint| RecordKind::Checkpoint {
                    epoch: checkpoint.epoch,
                    vmstate: checkpoint.vmstate,
                    vmstate_logical_length: checkpoint.vmstate_bytes.len() as u64,
                }),
            capture_seq,
            sync_covered_through: covered,
            post_state_checksum,
            database: vset_state.database,
            files: recovery_files(vset_state),
            overlay: record_overlay,
            leaves: record_leaves,
            migrated_from: vset_state.peer_source.map(|host| MigrationSource {
                host,
                offer_fence: vset_state.peer_source_offer_fence,
            }),
        }
    };
    let wrote_record = if record.migrated_from.is_some() {
        write_migration_record_copies(&state, world.as_ref(), vset, &record, &block_checksums).await
    } else {
        write_record_copies(&state, world.as_ref(), vset, &record).await
    };
    if !wrote_record {
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
        for (key, _) in &pending_tombstones {
            vset_state.pending_tombstones.remove(key);
        }
        if let Some((segment, _, _)) = segment_blobs.last() {
            vset_state.next_seg = vset_state.next_seg.max(segment.0.saturating_add(1));
        }
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
        vset_state.record_segments.insert(
            record.seq,
            segment_blobs
                .iter()
                .map(|(segment, _, _)| (fence, *segment))
                .collect(),
        );
        vset_state.operations.finish_mutation(capture_owner);
        let waiters = std::mem::take(&mut vset_state.mutation_waiters);
        if let Some(checkpoint) = checkpoint.as_ref() {
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
) -> Option<(JournalRecord, PausedGuest<W>, Vec<u8>)>
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
    let Some(paused) = PausedGuest::new(
        &state,
        &world,
        vset,
        incarnation,
        pause.clone(),
        protections,
    ) else {
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
            vmstate_bytes: pause.vmstate_bytes.clone(),
        }),
        Some((incarnation, lease)),
        None,
    )
    .await;
    if let Some(record) = record {
        Some((record, paused, pause.vmstate_bytes))
    } else {
        let _ = paused.resume().await;
        None
    }
}

pub(super) async fn write_record_copies<W: Blobs>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    record: &JournalRecord,
) -> bool {
    // A destination that still depends on its migration source must survive a
    // daemon crash without forgetting the remote page locations. Keep that
    // temporary lookup index in both local journal copies until hydration has
    // made the cut wholly local.
    let bytes = if record.migrated_from.is_some() {
        let checksums = state
            .borrow()
            .vsets
            .get(&vset)
            .map(|vset| vset.block_checksums.clone())
            .unwrap_or_default();
        record.encode_migration_with_checksums(vset, &checksums)
    } else {
        record.encode(vset)
    };
    write_encoded_record_copies(state, world, vset, record, bytes).await
}

pub(super) async fn write_migration_record_copies<W: Blobs>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    record: &JournalRecord,
    block_checksums: &BTreeMap<BlockKey, (Gen, u64)>,
) -> bool {
    let bytes = record.encode_migration_with_checksums(vset, block_checksums);
    write_encoded_record_copies(state, world, vset, record, bytes).await
}

async fn write_encoded_record_copies<W: Blobs>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    record: &JournalRecord,
    bytes: Vec<u8>,
) -> bool {
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
        super::blob::write(state, world, primary_name.clone(), bytes.clone()),
        super::blob::write(state, world, mirror_name.clone(), bytes),
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
