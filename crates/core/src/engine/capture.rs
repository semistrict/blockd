use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::channel::{OneReceiver, OneSender, oneshot};
use blockd_exec::inject::{Injector, Lane, injector};
use blockd_exec::{join2, spawn, yield_now};

use super::ctx::{HostCtx, VolumeCtx};
use super::state::{CaptureKind, CaptureLease, DrainState, MutationOwner, SharedHost, VolumeState};
use super::{cleanup_local, store_retry};
use crate::blx::{
    BlockKey, BlockSpace, BlxFooter, BlxObject, EntryKind, NamespaceKind, replace_state_block,
    state_contribution,
};
use crate::format::checksum64;
use crate::journal::{JournalRecord, MigrationSource, RecordKind, VolumeConfig};
use crate::layout;
use crate::manifest::{ObjectIdentity, ObjectRef};
use crate::page_file::{PageBatchBuilder, PageFileLoc, open_entry, scan_page_file};
use crate::protocol::{AdminError, AdminResult, AdminSuccess, ReqId};
use crate::types::{Gen, JournalSeq, ObjectId, PageId, VolumeId};
use crate::world::{AdminIo, Blobs, GuestMem, GuestPause, Store};

const DRAIN_PAGES_PER_POLL: usize = 64;
const ROLL_THRESHOLD: usize = 256;
const COMPACT_BATCH: usize = 8;

type PageMap = BTreeMap<PageId, (Gen, crate::page_file::PageFileLoc)>;

pub(super) fn recovery_files(volume: &VolumeState) -> Vec<ObjectRef> {
    let mut needed_blx = volume
        .page_locs
        .iter()
        .filter_map(|(page, (_, location))| {
            (location.base == 0).then_some(location.identity(page.volume))
        })
        .collect::<BTreeSet<_>>();
    needed_blx.extend(volume.tombstone_blx_files.iter().copied());
    needed_blx.extend(volume.vmm_blx_files.iter().copied());

    let needed_batches = volume
        .blx_refs
        .iter()
        .filter(|(blx, _)| needed_blx.contains(blx))
        .map(|(_, file)| (file.identity.writer_fence, file.batch_id))
        .collect::<BTreeSet<_>>();
    volume
        .blx_refs
        .values()
        .filter(|file| needed_batches.contains(&(file.identity.writer_fence, file.batch_id)))
        .copied()
        .collect()
}

/// Cold boot discards the archived memory and VMM state without eagerly
/// reading archive footers during restore. Before the next local capture,
/// materialize just those footer entries once so their checksum contributions
/// can be removed and all later generations can supersede them.
async fn reset_archived_non_data<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
) -> Option<()> {
    let (incarnation, objects, cached) = {
        let host = state.borrow();
        let volume_state = host.volumes.get(&volume)?;
        if volume_state.archived_non_data_reset {
            return Some(());
        }
        (
            volume_state.incarnation,
            volume_state.archive_objects.clone(),
            volume_state.archive_footers.clone(),
        )
    };
    let mut footers = cached;
    for object in &objects {
        if object.first_key.space == BlockSpace::Data && object.last_key.space == BlockSpace::Data {
            continue;
        }
        if footers.contains_key(&object.identity) {
            continue;
        }
        let bytes = store_retry::get_range(
            state,
            world,
            &object.identity.store_key(),
            u64::from(object.footer_offset),
            u64::from(object.footer_length),
        )
        .await
        .ok()??
        .1;
        let footer = BlxFooter::open(&bytes).ok()?;
        let valid = footer.entries.first().is_some_and(|entry| {
            entry.key == object.first_key
                && footer
                    .entries
                    .last()
                    .is_some_and(|last| last.key == object.last_key)
        }) && footer.entries.iter().all(|entry| {
            entry
                .offset
                .checked_add(entry.length)
                .is_some_and(|end| end <= object.footer_offset)
        });
        if !valid {
            return None;
        }
        footers.insert(object.identity, footer);
    }

    let mut winners = BTreeMap::new();
    for object in &objects {
        let Some(footer) = footers.get(&object.identity) else {
            continue;
        };
        let own = object.identity.namespace_kind == NamespaceKind::Volume
            && object.identity.namespace_id == volume.0;
        for entry in footer
            .entries
            .iter()
            .filter(|entry| entry.key.space != BlockSpace::Data)
        {
            let replace = winners.get(&entry.key).is_none_or(
                |(old_entry, old_own, old_object): &(crate::blx::FooterEntry, bool, ObjectRef)| {
                    (entry.generation, own, object.identity)
                        > (old_entry.generation, *old_own, old_object.identity)
                },
            );
            if replace {
                winners.insert(entry.key, (*entry, own, *object));
            }
        }
    }

    let mut host = state.borrow_mut();
    let volume_state = host
        .volumes
        .get_mut(&volume)
        .filter(|volume_state| volume_state.incarnation == incarnation)?;
    if volume_state.archived_non_data_reset {
        return Some(());
    }
    for (key, (entry, _, _)) in winners {
        volume_state.next_gen = volume_state
            .next_gen
            .max(entry.generation.0.saturating_add(1));
        if entry.kind == EntryKind::Data {
            volume_state.state_checksum ^=
                state_contribution(key, entry.generation, entry.value_checksum);
        }
    }
    volume_state.archive_footers = footers;
    volume_state.archived_non_data_reset = true;
    Some(())
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

fn reserve_checkpoint(state: &SharedHost, req: ReqId, volume: VolumeId) -> CheckpointDecision {
    let mut host = state.borrow_mut();
    let Some(volume_state) = host.volumes.get_mut(&volume) else {
        return CheckpointDecision::Missing;
    };
    if !volume_state.ready {
        return CheckpointDecision::Invalid;
    }
    if let Some(&epoch) = volume_state.checkpoint_results.get(&req) {
        return CheckpointDecision::Existing(epoch);
    }
    if volume_state.operations.migration_running() {
        return CheckpointDecision::Invalid;
    }
    if volume_state.operations.mutation_blocked() {
        let (wake, wait) = oneshot();
        volume_state.mutation_waiters.push(wake);
        return CheckpointDecision::Busy(wait);
    }

    let epoch = crate::types::Epoch(volume_state.epoch.0 + 1);
    assert!(
        volume_state
            .operations
            .try_start_mutation(MutationOwner::Capture(CaptureKind::Checkpoint))
    );
    let (cleanup, protections) = oneshot();
    CheckpointDecision::Reserved {
        incarnation: volume_state.incarnation,
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
    volume: VolumeId,
    incarnation: u64,
    pause: GuestPause,
    active: Rc<Cell<bool>>,
}

impl<W: GuestMem + AdminIo + 'static> Clone for PauseTracker<W> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            world: Rc::clone(&self.world),
            volume: self.volume,
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
            .volumes
            .get(&self.volume)
            .is_some_and(|volume| {
                volume.incarnation == self.incarnation && volume.operations.guest_resume_pending()
            });
        if !current {
            self.active.set(false);
            return true;
        }
        let resumed = GuestMem::resume(self.world.as_ref(), self.volume, Some(self.pause.clone()))
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
            let Some(volume) = host
                .volumes
                .get_mut(&self.volume)
                .filter(|volume| volume.incarnation == self.incarnation)
            else {
                return;
            };
            volume.operations.finish_guest_resume();
            std::mem::take(&mut volume.mutation_waiters)
        };
        self.state.borrow_mut().schedule_volume(self.volume);
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

impl<W: GuestMem + AdminIo + 'static> PausedGuest<W> {
    async fn acquire(
        state: &SharedHost,
        world: &Rc<W>,
        volume: VolumeId,
        incarnation: u64,
        protections: OneReceiver<Vec<PageId>>,
    ) -> Option<Self> {
        let Ok(pause) = GuestMem::pause(world.as_ref(), volume).await else {
            state.borrow_mut().fail("guest pause failed");
            return None;
        };
        let Some(paused) = Self::new(
            state,
            world,
            volume,
            incarnation,
            pause.clone(),
            protections,
        ) else {
            if GuestMem::resume(world.as_ref(), volume, Some(pause))
                .await
                .is_err()
            {
                state.borrow_mut().fail("stale capture guest resume failed");
            }
            return None;
        };
        Some(paused)
    }

    fn new(
        state: &SharedHost,
        world: &Rc<W>,
        volume: VolumeId,
        incarnation: u64,
        pause: GuestPause,
        protections: OneReceiver<Vec<PageId>>,
    ) -> Option<Self> {
        let active = Rc::new(Cell::new(true));
        {
            let mut host = state.borrow_mut();
            let volume_state = host
                .volumes
                .get_mut(&volume)
                .filter(|volume_state| volume_state.incarnation == incarnation)?;
            if !volume_state.operations.start_guest_resume() {
                return None;
            }
        }
        let tracker = PauseTracker {
            state: Rc::clone(state),
            world: Rc::clone(world),
            volume,
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

    fn pause(&self) -> &GuestPause {
        &self.tracker.pause
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
            self.tracker.volume,
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
    victim: (u64, ObjectId),
    entries: Vec<(PageId, Gen, PageFileLoc, Vec<u8>)>,
}

/// Verify and decompress the live entries of the sparsest local BLX files.
/// The serving map is rechecked when the capture reserves its cut, so reads
/// that race a newer capture are harmless stale work.
async fn prepare_compaction<W: Blobs>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
) -> Vec<CompactionRescue> {
    let candidates = {
        let host = state.borrow();
        let Some(volume_state) = host.volumes.get(&volume).filter(|state| state.ready) else {
            return Vec::new();
        };
        if volume_state.outbound.is_some()
            || volume_state.operations.migration_running()
            || volume_state.page_locs.len() < ROLL_THRESHOLD
        {
            return Vec::new();
        }
        let mut live_by_blx = BTreeMap::<ObjectIdentity, u64>::new();
        for (_, location) in volume_state.page_locs.values() {
            if location.base == 0 {
                *live_by_blx.entry(location.identity(volume)).or_default() +=
                    u64::from(location.len);
            }
        }
        let mut candidates = volume_state
            .blx_blobs
            .iter()
            .filter_map(|&(identity, size)| {
                let live = live_by_blx.get(&identity).copied().unwrap_or(0);
                (live > 0 && live.saturating_mul(2) <= size)
                    .then_some((live.saturating_mul(1_000_000) / size, identity))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.truncate(COMPACT_BATCH);
        candidates
    };

    let mut rescues = Vec::new();
    for (_, identity) in candidates {
        let fence = identity.writer_fence;
        let blx = ObjectId(identity.object_id);
        let expected = {
            let host = state.borrow();
            let Some(volume_state) = host.volumes.get(&volume) else {
                break;
            };
            volume_state
                .page_locs
                .iter()
                .filter(|(_, (_, location))| {
                    location.base == 0 && (location.fence, location.object) == (fence, blx)
                })
                .map(|(&page, &entry)| (page, entry))
                .collect::<BTreeMap<_, _>>()
        };
        let name = layout::blx_blob(volume, fence, blx);
        let Ok(Some(bytes)) = Blobs::read(world, &name).await else {
            continue;
        };
        let Ok((owner, blob_fence, blob_object, entries)) = scan_page_file(&bytes) else {
            continue;
        };
        if (owner, blob_fence, blob_object) != (volume, fence, blx) {
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
            let Ok((decoded_page, decoded_generation, raw)) = open_entry(volume, frame) else {
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
                victim: (fence, blx),
                entries: decoded,
            });
        }
    }
    rescues
}

fn initial_record(config: VolumeConfig, fence: u64) -> JournalRecord {
    JournalRecord {
        config,
        seq: JournalSeq(0),
        fence,
        kind: RecordKind::Commit,
        capture_seq: 0,
        sync_covered_through: 0,
        post_state_checksum: 0,
        files: Vec::new(),
        runtime_page_index: BTreeMap::new(),
        migrated_from: None,
    }
}

pub(super) async fn finish_creation<W: Blobs>(
    state: SharedHost,
    world: &W,
    volume: VolumeId,
    incarnation: u64,
) -> bool {
    let Some((config, fence)) = state
        .borrow()
        .volumes
        .get(&volume)
        .filter(|state| state.incarnation == incarnation)
        .map(|state| (state.config, state.fence))
    else {
        return false;
    };
    let record = initial_record(config, fence);
    if !write_record_copies(&state, world, volume, &record, &BTreeMap::new()).await {
        return false;
    }
    {
        let mut host = state.borrow_mut();
        let Some(volume_state) = host
            .volumes
            .get_mut(&volume)
            .filter(|state| state.incarnation == incarnation)
        else {
            return false;
        };
        volume_state.ready = true;
        volume_state.next_seq = 1;
        volume_state.best_record = Some(record.clone());
        volume_state.record_writes.insert(record.seq, (fence, 0));
        host.counters.records_written += 1;
        host.schedule_volume(volume);
    }
    true
}

pub async fn capture_local<W>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
) -> Option<JournalRecord>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world).volume(volume).capture().await
}

pub async fn checkpoint_local<W>(
    state: SharedHost,
    world: Rc<W>,
    req: ReqId,
    volume: VolumeId,
) -> Option<AdminResult>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world)
        .volume(volume)
        .checkpoint(req)
        .await
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    pub(super) async fn capture(&self) -> Option<JournalRecord> {
        capture_record(
            Rc::clone(self.host().state()),
            Rc::clone(self.host().world()),
            self.id(),
            CaptureKind::Writeback,
            None,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    pub(super) async fn checkpoint(&self, req: ReqId) -> Option<AdminResult> {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        let kind = {
            let host = state.borrow();
            let volume_state = host.volumes.get(&volume)?;
            volume_state.config.kind
        };

        let (incarnation, epoch, lease, protections) = loop {
            match reserve_checkpoint(&state, req, volume) {
                CheckpointDecision::Missing | CheckpointDecision::Invalid => {
                    return Some(Err(AdminError::Rejected));
                }
                CheckpointDecision::Existing(epoch) => {
                    return Some(Ok(AdminSuccess::CheckpointDone { volume, epoch }));
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
                            volume,
                            incarnation,
                            MutationOwner::Capture(CaptureKind::Checkpoint),
                            cleanup,
                        ),
                        protections,
                    );
                }
            }
        };
        if kind == crate::journal::VolumeKind::Data {
            let captured = capture_record(
                Rc::clone(&state),
                Rc::clone(&world),
                volume,
                CaptureKind::Checkpoint,
                None,
                Some((incarnation, lease)),
                None,
            )
            .await?;
            let mut host = state.borrow_mut();
            let volume_state = host
                .volumes
                .get_mut(&volume)
                .filter(|volume_state| volume_state.incarnation == incarnation)?;
            volume_state.epoch = epoch;
            volume_state.pinned = Some(captured);
            volume_state.checkpoint_results.insert(req, epoch);
            host.counters.checkpoints_done = host.counters.checkpoints_done.saturating_add(1);
            return Some(Ok(AdminSuccess::CheckpointDone { volume, epoch }));
        }
        let paused = PausedGuest::acquire(&state, &world, volume, incarnation, protections).await?;
        let checkpoint = Checkpoint {
            req: Some(req),
            epoch,
            vmstate: paused.pause().vmstate,
            vmstate_bytes: paused.pause().vmstate_bytes.clone(),
        };
        let captured = capture_record(
            Rc::clone(&state),
            Rc::clone(&world),
            volume,
            CaptureKind::Checkpoint,
            Some(checkpoint),
            Some((incarnation, lease)),
            Some(paused.tracker()),
        )
        .await;
        if !paused.resume().await {
            return None;
        }
        captured.map(|_| Ok(AdminSuccess::CheckpointDone { volume, epoch }))
    }
}

#[allow(clippy::too_many_lines)]
async fn capture_record<W>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
    capture_kind: CaptureKind,
    checkpoint: Option<Checkpoint>,
    reservation: Option<(u64, CaptureLease)>,
    resumed: Option<PauseTracker<W>>,
) -> Option<JournalRecord>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let pre_reserved = reservation.is_some();
    let reserved_incarnation = reservation.as_ref().map(|(incarnation, _)| *incarnation);
    let capture_owner = MutationOwner::Capture(capture_kind);
    reset_archived_non_data(&state, world.as_ref(), volume).await?;
    let complete_memory_snapshot = checkpoint.is_some()
        && state
            .borrow()
            .volumes
            .get(&volume)
            .is_some_and(|volume_state| !volume_state.archived_memory_usable);
    let prepared_compaction = prepare_compaction(&state, world.as_ref(), volume).await;
    let (
        incarnation,
        seq,
        capture_seq,
        fence,
        pages,
        flushed_pages,
        protect,
        first_object,
        compact_victims,
        pending_tombstones,
    ) = {
        let mut host = state.borrow_mut();
        let volume_state = host.volumes.get(&volume)?;
        let flush_set = host
            .cache
            .unstable_pages_of(volume)
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
            let all_live_covered = volume_state.page_locs.iter().all(|(page, entry)| {
                let location = entry.1;
                (location.base != 0 || (location.fence, location.object) != prepared.victim)
                    || flush_set.contains(page)
                    || prepared_pages.get(page) == Some(entry)
            });
            if !all_live_covered {
                continue;
            }
            for (page, generation, location, bytes) in prepared.entries {
                if !flush_set.contains(&page)
                    && volume_state.page_locs.get(&page) == Some(&(generation, location))
                {
                    rescues.push((page, bytes));
                }
            }
            compact_victims.insert(prepared.victim);
        }
        let needs_commit = host.cache.has_dirty_of(volume)
            || !compact_victims.is_empty()
            || !volume_state.pending_tombstones.is_empty()
            || volume_state
                .pending_syncs
                .iter()
                .any(|sync| sync.barrier > volume_state.local_covered_through);
        if !volume_state.ready
            || (!pre_reserved
                && (volume_state.operations.mutation_blocked()
                    || volume_state.operations.migration_running()
                    || !needs_commit))
            || reserved_incarnation.is_some_and(|incarnation| {
                volume_state.incarnation != incarnation
                    || volume_state.operations.mutation_owner() != Some(capture_owner)
            })
        {
            return None;
        }
        let incarnation = volume_state.incarnation;
        let flushed_pages = host.cache.unstable_pages_of(volume);
        let mut pages = flushed_pages.iter().copied().collect::<BTreeSet<_>>();
        if complete_memory_snapshot {
            pages.extend((0..volume_state.config.pages).map(|page| PageId {
                volume,
                page: crate::types::PageNo(page),
            }));
        }
        let pages = pages.into_iter().collect::<Vec<_>>();
        let protect = host.cache.dirty_pages_of(volume);
        let (seq, capture_seq, first_object, fence, pending_tombstones) = {
            let volume_state = host.volumes.get_mut(&volume).expect("just observed");
            let seq = JournalSeq(volume_state.next_seq);
            volume_state.next_seq += 1;
            let capture_seq = volume_state.mutation_seq;
            let first_object = ObjectId(volume_state.next_object_id);
            volume_state.next_object_id = volume_state
                .next_object_id
                .checked_add(
                    u64::try_from(
                        pages.len() + rescues.len() + volume_state.pending_tombstones.len(),
                    )
                    .expect("entry count fits u64"),
                )
                .expect("blx id overflow");
            let mut unread = BTreeMap::new();
            for &page in &pages {
                unread.insert(page, Gen(volume_state.next_gen));
                volume_state.next_gen += 1;
            }
            if !pre_reserved {
                assert!(volume_state.operations.try_start_mutation(capture_owner));
            }
            volume_state.operations.begin_drain(DrainState {
                unread,
                copied_on_fault: BTreeMap::new(),
                armed: pages.clone(),
                rescues: rescues
                    .into_iter()
                    .map(|(page, bytes)| {
                        let generation = Gen(volume_state.next_gen);
                        volume_state.next_gen += 1;
                        (page, generation, bytes)
                    })
                    .collect(),
            });
            let pending_keys = volume_state
                .pending_tombstones
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let mut pending_tombstones = Vec::with_capacity(pending_keys.len());
            for &key in &pending_keys {
                let generation = if key.space == crate::blx::BlockSpace::Vmm {
                    Gen(seq.0)
                } else {
                    let generation = Gen(volume_state.next_gen);
                    volume_state.next_gen += 1;
                    generation
                };
                pending_tombstones.push((key, generation));
            }
            (
                seq,
                capture_seq,
                first_object,
                volume_state.fence,
                pending_tombstones,
            )
        };
        for &page in &flushed_pages {
            host.cache.begin_flush(page);
        }
        (
            incarnation,
            seq,
            capture_seq,
            fence,
            pages,
            flushed_pages,
            protect,
            first_object,
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
                    let Some(volume_state) = host
                        .volumes
                        .get_mut(&volume)
                        .filter(|volume_state| volume_state.incarnation == incarnation)
                    else {
                        return;
                    };
                    volume_state.operations.finish_mutation(capture_owner);
                    let waiters = std::mem::take(&mut volume_state.mutation_waiters);
                    host.wake_pressure_waiter();
                    host.schedule_volume(volume);
                    waiters
                };
                for waiter in waiters {
                    let _ = waiter.send(());
                }
            })
            .detach();
            CaptureLease::new_with_serialized_cleanup(
                &state,
                volume,
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
            host.volumes
                .get_mut(&volume)
                .filter(|state| state.incarnation == incarnation)
                .and_then(|volume_state| volume_state.operations.drain_mut())
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
        let volume_state = host
            .volumes
            .get_mut(&volume)
            .filter(|state| state.incarnation == incarnation)?;
        std::mem::take(&mut volume_state.operations.drain_mut()?.rescues)
    };
    for (index, rescued) in rescues.into_iter().enumerate() {
        batch_pages.push(rescued);
        if (index + 1) % DRAIN_PAGES_PER_POLL == 0 {
            yield_now().await;
        }
    }

    let (kind, pre_state_checksum, post_state_checksum, block_checksums) = {
        let host = state.borrow();
        let volume_state = host.volumes.get(&volume)?;
        let mut checksum = volume_state.state_checksum;
        let mut blocks = volume_state.block_checksums.clone();
        for (page, generation, bytes) in &batch_pages {
            replace_state_block(
                &mut checksum,
                &mut blocks,
                BlockKey::from_page(volume_state.config.kind, *page),
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
            for (key, padded) in crate::blx::vmm_snapshot_blocks(&checkpoint.vmstate_bytes) {
                replace_state_block(
                    &mut checksum,
                    &mut blocks,
                    key,
                    Some((Gen(seq.0), checksum64(&padded))),
                );
            }
        }
        (
            volume_state.config.kind,
            volume_state.state_checksum,
            checksum,
            blocks,
        )
    };
    let mut builder = PageBatchBuilder::new_with_checksums(
        kind,
        volume,
        fence,
        first_object,
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
        } else if let Some(page) = key.to_page(kind, volume) {
            builder
                .try_add_tombstone(page, generation)
                .expect("reserved blx IDs cover pending tombstones");
        }
    }
    if let Some(checkpoint) = checkpoint.as_ref() {
        let old_vmm_blocks = state
            .borrow()
            .volumes
            .get(&volume)?
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

    let blx_blobs = builder.finish();
    let flushed_pages = flushed_pages.into_iter().collect::<BTreeSet<_>>();
    for (blx, bytes, entries) in &blx_blobs {
        let name = layout::blx_blob(volume, fence, *blx);
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
            state.borrow_mut().fail("local blx write failed");
            return None;
        }
        let mut host = state.borrow_mut();
        host.record_blob(name, bytes.len() as u64);
        {
            let volume_state = host
                .volumes
                .get_mut(&volume)
                .filter(|state| state.incarnation == incarnation)?;
            for &(page, generation, location) in entries {
                volume_state.page_locs.insert(page, (generation, location));
                if let Some(drain) = volume_state.operations.drain_mut() {
                    drain.armed.retain(|armed| *armed != page);
                }
            }
            let identity = ObjectIdentity::volume(volume, fence, blx.0);
            volume_state.blx_blobs.push((identity, bytes.len() as u64));
            let object = BlxObject::open(bytes).ok()?;
            volume_state
                .blx_refs
                .insert(identity, ObjectRef::from_blx(&object));
            if object
                .footer
                .entries
                .iter()
                .any(|entry| entry.kind == crate::blx::EntryKind::Tombstone)
            {
                volume_state.tombstone_blx_files.insert(identity);
            }
            volume_state.state_checksum = post_state_checksum;
            volume_state.block_checksums.clone_from(&block_checksums);
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
        let volume_state = host
            .volumes
            .get_mut(&volume)
            .filter(|state| state.incarnation == incarnation)?;
        volume_state.vmm_blx_files = blx_blobs
            .iter()
            .map(|(blx, _, _)| ObjectIdentity::volume(volume, fence, blx.0))
            .collect();
    }

    let record_overlay: PageMap = {
        let host = state.borrow();
        let volume_state = host
            .volumes
            .get(&volume)
            .filter(|state| state.incarnation == incarnation)?;
        if capture_kind == CaptureKind::Migration
            || volume_state.peer_source.is_some()
            || volume_state
                .page_locs
                .values()
                .any(|(_, location)| location.base != 0)
        {
            volume_state.page_locs.clone()
        } else {
            BTreeMap::new()
        }
    };

    let record = {
        let mut host = state.borrow_mut();
        let volume_state = host
            .volumes
            .get_mut(&volume)
            .filter(|state| state.incarnation == incarnation)?;
        let covered = volume_state
            .pending_syncs
            .iter()
            .filter(|sync| sync.barrier <= capture_seq)
            .map(|sync| sync.barrier)
            .fold(volume_state.local_covered_through, u64::max);
        JournalRecord {
            config: volume_state.config,
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
            files: recovery_files(volume_state),
            runtime_page_index: record_overlay,
            migrated_from: volume_state.peer_source.map(|host| MigrationSource {
                host,
                offer_fence: volume_state.peer_source_offer_fence,
            }),
        }
    };
    let wrote_record =
        write_record_copies(&state, world.as_ref(), volume, &record, &block_checksums).await;
    if !wrote_record {
        state.borrow_mut().fail("local journal write failed");
        return None;
    }

    let (syncs, waiters) = {
        let mut host = state.borrow_mut();
        let volume_state = host
            .volumes
            .get_mut(&volume)
            .filter(|state| state.incarnation == incarnation)?;
        volume_state.best_record = Some(record.clone());
        if complete_memory_snapshot {
            volume_state.archived_memory_usable = true;
        }
        for (key, _) in &pending_tombstones {
            volume_state.pending_tombstones.remove(key);
        }
        if let Some((blx, _, _)) = blx_blobs.last() {
            volume_state.next_object_id = volume_state.next_object_id.max(blx.0.saturating_add(1));
        }
        volume_state.local_covered_through = record.sync_covered_through;
        let mut completed = Vec::new();
        let pending = std::mem::take(&mut volume_state.pending_syncs);
        for sync in pending {
            if sync.barrier <= volume_state.sync_ack_through {
                completed.push(sync);
            } else {
                volume_state.pending_syncs.push(sync);
            }
        }
        volume_state
            .record_writes
            .insert(record.seq, (fence, record.sync_covered_through));
        volume_state.record_blx_files.insert(
            record.seq,
            blx_blobs
                .iter()
                .map(|(blx, _, _)| ObjectIdentity::volume(volume, fence, blx.0))
                .collect(),
        );
        volume_state.operations.finish_mutation(capture_owner);
        let waiters = std::mem::take(&mut volume_state.mutation_waiters);
        if let Some(checkpoint) = checkpoint.as_ref() {
            volume_state.epoch = checkpoint.epoch;
            volume_state.pinned = Some(record.clone());
            if let Some(req) = checkpoint.req {
                volume_state
                    .checkpoint_results
                    .insert(req, checkpoint.epoch);
            }
        }
        host.counters.records_written += 1;
        host.counters.pages_flushed += pages.len() as u64;
        host.counters.blx_files_compacted += compact_victims.len() as u64;
        host.counters.pages_compacted += u64::try_from(
            blx_blobs
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
    if cleanup_local(Rc::clone(&state), world.as_ref(), volume, incarnation)
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
    volume: VolumeId,
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
            let volume_state = host.volumes.get_mut(&volume)?;
            if !volume_state.ready || volume_state.config.kind != crate::journal::VolumeKind::Memory
            {
                Decision::Invalid
            } else if volume_state.operations.mutation_blocked() {
                let (wake, wait) = oneshot();
                volume_state.mutation_waiters.push(wake);
                Decision::Busy(wait)
            } else {
                let epoch = crate::types::Epoch(volume_state.epoch.0 + 1);
                assert!(
                    volume_state
                        .operations
                        .try_start_mutation(MutationOwner::Capture(CaptureKind::Migration))
                );
                let (cleanup, protections) = oneshot();
                Decision::Reserved(volume_state.incarnation, epoch, cleanup, protections)
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
                        volume,
                        incarnation,
                        MutationOwner::Capture(CaptureKind::Migration),
                        cleanup,
                    ),
                    protections,
                );
            }
        }
    };
    let paused = PausedGuest::acquire(&state, &world, volume, incarnation, protections).await?;
    let vmstate = paused.pause().vmstate;
    let vmstate_bytes = paused.pause().vmstate_bytes.clone();
    let record = capture_record(
        Rc::clone(&state),
        Rc::clone(&world),
        volume,
        CaptureKind::Migration,
        Some(Checkpoint {
            req: None,
            epoch,
            vmstate,
            vmstate_bytes: vmstate_bytes.clone(),
        }),
        Some((incarnation, lease)),
        None,
    )
    .await;
    if let Some(record) = record {
        Some((record, paused, vmstate_bytes))
    } else {
        let _ = paused.resume().await;
        None
    }
}

pub(super) async fn write_record_copies<W: Blobs>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    record: &JournalRecord,
    block_checksums: &BTreeMap<BlockKey, (Gen, u64)>,
) -> bool {
    // A destination that still depends on its migration source must survive a
    // daemon crash without forgetting the remote page locations. Keep that
    // temporary lookup index in both local journal copies until hydration has
    // made the cut wholly local.
    let bytes = if record.migrated_from.is_some() {
        record.encode_migration_with_checksums(volume, block_checksums)
    } else {
        record.encode(volume)
    };
    write_encoded_record_copies(state, world, volume, record, bytes).await
}

async fn write_encoded_record_copies<W: Blobs>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    record: &JournalRecord,
    bytes: Vec<u8>,
) -> bool {
    let primary_name = layout::journal_blob(volume, record.fence, record.seq);
    let mirror_name = layout::journal_mirror_blob(volume, record.fence, record.seq);
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
