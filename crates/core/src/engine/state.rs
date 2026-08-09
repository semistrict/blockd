use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use blockd_exec::channel::OneSender;

use crate::cache::Cache;
use crate::database::{AttachmentId, DatabaseFile};
use crate::head::ManifestPtr;
use crate::hostmeta::{
    Counters, DaemonStats, HostConfig, ReplicaSpoolMetrics, ReplicaVsetMetrics, VsetOperations,
    VsetRole, VsetStats,
};
use crate::journal::{DatabaseMeta, JournalRecord, VsetConfig};
use crate::mapleaf::LeafPtr;
use crate::segment::PageLoc;
use crate::types::{Epoch, Gen, HostId, JournalSeq, PageId, SegId, VsetId};

pub type SharedHost = Rc<RefCell<HostState>>;
type ReplicaStatusSender = OneSender<(HostId, Option<crate::protocol::ReplicaCommitInfo>)>;
type PeerBytesSender = (HostId, OneSender<Option<Vec<u8>>>);

pub struct HostState {
    pub config: HostConfig,
    pub cache: Cache,
    pub vsets: BTreeMap<VsetId, VsetState>,
    pub counters: Counters,
    pub blob_sizes: BTreeMap<String, u64>,
    pub disk_reclaim_requested: bool,
    pub pressure_waiters: VecDeque<OneSender<()>>,
    pub filling_pages: BTreeSet<PageId>,
    pub page_fill_waiters: BTreeMap<PageId, Vec<OneSender<bool>>>,
    pub peer_pages: BTreeMap<crate::protocol::PeerRequestId, PeerBytesSender>,
    pub peer_leaves: BTreeMap<crate::protocol::PeerRequestId, PeerBytesSender>,
    pub inbound_migrations: BTreeSet<VsetId>,
    pub migration_accepts: BTreeMap<VsetId, OneSender<()>>,
    pub replicas: BTreeMap<ReplicaKey, ReplicaState>,
    pub replica_latest_epoch: BTreeMap<(HostId, VsetId), u64>,
    pub replica_status_waiters: BTreeMap<(VsetId, u64), ReplicaStatusSender>,
    pub replica_put_waiters:
        BTreeMap<(VsetId, u64, crate::protocol::ReplicaArtifact, u32), OneSender<HostId>>,
    pub replica_commit_waiters: BTreeMap<(VsetId, u64, u64, JournalSeq, u64), OneSender<HostId>>,
    pub replica_releases: Vec<(HostId, VsetId, u64, crate::protocol::ReplicaCommitInfo)>,
    next_peer_request: u64,
    next_attachment_generation: u64,
    next_incarnation: u64,
}

impl HostState {
    pub fn new(config: HostConfig) -> Self {
        assert!(
            config.archive.interval > 0,
            "archive interval must be positive"
        );
        assert!(
            config.archive.spool_headroom_bytes < config.archive.spool_capacity_bytes,
            "archive spool headroom must be smaller than capacity"
        );
        Self {
            cache: Cache::new(config.cache_pages),
            config,
            vsets: BTreeMap::new(),
            counters: Counters::default(),
            blob_sizes: BTreeMap::new(),
            disk_reclaim_requested: false,
            pressure_waiters: VecDeque::new(),
            filling_pages: BTreeSet::new(),
            page_fill_waiters: BTreeMap::new(),
            peer_pages: BTreeMap::new(),
            peer_leaves: BTreeMap::new(),
            inbound_migrations: BTreeSet::new(),
            migration_accepts: BTreeMap::new(),
            replicas: BTreeMap::new(),
            replica_latest_epoch: BTreeMap::new(),
            replica_status_waiters: BTreeMap::new(),
            replica_put_waiters: BTreeMap::new(),
            replica_commit_waiters: BTreeMap::new(),
            replica_releases: Vec::new(),
            next_peer_request: 0,
            next_attachment_generation: 0,
            next_incarnation: 0,
        }
    }

    pub fn insert_fresh(&mut self, vset: VsetId, config: VsetConfig) -> u64 {
        let incarnation = self.allocate_incarnation();
        let previous = self
            .vsets
            .insert(vset, VsetState::fresh(config, incarnation));
        assert!(previous.is_none(), "duplicate vset insertion");
        incarnation
    }

    pub(crate) fn allocate_incarnation(&mut self) -> u64 {
        let incarnation = self.next_incarnation;
        self.next_incarnation = self
            .next_incarnation
            .checked_add(1)
            .expect("vset incarnation overflow");
        incarnation
    }

    pub fn wake_pressure_waiter(&mut self) {
        while let Some(waiter) = self.pressure_waiters.pop_front() {
            if waiter.send(()).is_ok() {
                return;
            }
        }
    }

    pub fn allocate_peer_request(&mut self) -> crate::protocol::PeerRequestId {
        let request = crate::protocol::PeerRequestId(self.next_peer_request);
        self.next_peer_request = self
            .next_peer_request
            .checked_add(1)
            .expect("peer request overflow");
        request
    }

    pub fn allocate_attachment(&mut self, vm: crate::types::VmId) -> AttachmentId {
        let attachment = AttachmentId {
            vm,
            generation: self.next_attachment_generation,
        };
        self.next_attachment_generation = self
            .next_attachment_generation
            .checked_add(1)
            .expect("attachment generation overflow");
        attachment
    }

    pub fn wedge_tick(&mut self) {
        let threshold = self.config.wedge_ticks;
        if threshold == 0 {
            return;
        }
        for vset in self.vsets.values_mut() {
            watch(
                !vset.leaf_waiters.is_empty(),
                vset.wedge.fills,
                &mut vset.wedge.fills_seen,
                &mut vset.wedge.parked_ticks,
                threshold,
                &mut self.counters.wedged_guests,
            );
            watch(
                vset.peer_source.is_some() || !vset.leaf_waiters.is_empty(),
                vset.wedge.hydration,
                &mut vset.wedge.hydration_seen,
                &mut vset.wedge.hydration_ticks,
                threshold,
                &mut self.counters.wedged_hydration,
            );
            watch(
                vset.outbound.is_some(),
                vset.wedge.served,
                &mut vset.wedge.served_seen,
                &mut vset.wedge.outbound_ticks,
                threshold,
                &mut self.counters.wedged_outbound,
            );
        }
    }

    pub fn record_blob(&mut self, name: String, bytes: u64) {
        self.blob_sizes.insert(name, bytes);
    }

    pub fn try_reserve_blob(&mut self, name: String, bytes: u64) -> bool {
        let prior = self.blob_sizes.get(&name).copied().unwrap_or(0);
        let next = self
            .blob_sizes
            .values()
            .sum::<u64>()
            .saturating_sub(prior)
            .saturating_add(bytes);
        if self
            .config
            .disk_capacity
            .is_some_and(|capacity| next > capacity.saturating_sub(self.config.disk_headroom))
        {
            self.counters.nvme_stalls += 1;
            self.disk_reclaim_requested = true;
            return false;
        }
        self.blob_sizes.insert(name, bytes);
        true
    }

    pub fn try_reserve_blobs(&mut self, blobs: &[(String, u64)]) -> bool {
        let mut next = self.blob_sizes.values().sum::<u64>();
        for (name, bytes) in blobs {
            next = next
                .saturating_sub(self.blob_sizes.get(name).copied().unwrap_or(0))
                .saturating_add(*bytes);
        }
        if self
            .config
            .disk_capacity
            .is_some_and(|capacity| next > capacity.saturating_sub(self.config.disk_headroom))
        {
            self.counters.nvme_stalls += 1;
            self.disk_reclaim_requested = true;
            return false;
        }
        for (name, bytes) in blobs {
            self.blob_sizes.insert(name.clone(), *bytes);
        }
        true
    }

    /// Reserve commit metadata from the capacity headroom kept aside for it.
    /// Data writes stop below `disk_headroom`; journal copies may consume that
    /// margin, but never exceed the device's actual configured capacity.
    pub fn try_reserve_metadata_blobs(&mut self, blobs: &[(String, u64)]) -> bool {
        let mut next = self.blob_sizes.values().sum::<u64>();
        for (name, bytes) in blobs {
            next = next
                .saturating_sub(self.blob_sizes.get(name).copied().unwrap_or(0))
                .saturating_add(*bytes);
        }
        if self
            .config
            .disk_capacity
            .is_some_and(|capacity| next > capacity)
        {
            self.counters.nvme_stalls += 1;
            self.disk_reclaim_requested = true;
            return false;
        }
        for (name, bytes) in blobs {
            self.blob_sizes.insert(name.clone(), *bytes);
        }
        true
    }

    pub fn try_reserve_append(&mut self, name: String, bytes: u64) -> bool {
        let next = self.blob_sizes.values().sum::<u64>().saturating_add(bytes);
        if self
            .config
            .disk_capacity
            .is_some_and(|capacity| next > capacity.saturating_sub(self.config.disk_headroom))
        {
            self.counters.nvme_stalls += 1;
            self.disk_reclaim_requested = true;
            return false;
        }
        self.append_blob(name, bytes);
        true
    }

    pub fn append_blob(&mut self, name: String, bytes: u64) {
        let stored = self.blob_sizes.entry(name).or_default();
        *stored = stored.saturating_add(bytes);
    }

    pub fn truncate_blob(&mut self, name: &str, bytes: u64) {
        if let Some(stored) = self.blob_sizes.get_mut(name) {
            *stored = bytes;
        }
    }

    pub fn forget_blobs<'a>(&mut self, names: impl IntoIterator<Item = &'a String>) {
        for name in names {
            self.blob_sizes.remove(name);
        }
    }

    pub fn seg_space(&self) -> (u64, u64) {
        self.vsets.values().fold((0, 0), |(live, local), vset| {
            (
                live.saturating_add(vset.live_segment_bytes()),
                local.saturating_add(
                    vset.segment_blobs
                        .iter()
                        .map(|&(_, _, bytes)| bytes)
                        .sum::<u64>(),
                ),
            )
        })
    }

    pub fn stats(&self) -> DaemonStats {
        let vsets = self
            .vsets
            .iter()
            .map(|(&vset, state)| {
                let hydrating = state.peer_source.is_some()
                    || state
                        .leaf_table
                        .keys()
                        .any(|span| !state.hydrated_spans.contains(span));
                let role = if state.outbound.is_some() {
                    VsetRole::Outbound
                } else if hydrating {
                    VsetRole::Hydrating
                } else if state.ready {
                    VsetRole::Serving
                } else {
                    VsetRole::Initializing
                };
                let mut operations = 0;
                if state.commit_running || state.drain.is_some() {
                    operations |= VsetOperations::CAPTURE;
                }
                if state.checkpoint_running {
                    operations |= VsetOperations::CHECKPOINT;
                }
                if state.publishing {
                    operations |= VsetOperations::BACKUP;
                }
                if hydrating {
                    operations |= VsetOperations::HYDRATION;
                }
                let best = state
                    .best_record
                    .as_ref()
                    .map_or(0, |record| record.capture_seq);
                let backed = state.backed.map_or(0, |pointer| pointer.capture_seq);
                let pending_leaf_spans = state
                    .leaf_table
                    .keys()
                    .filter(|span| !state.hydrated_spans.contains(span))
                    .count();
                let hydration_remaining_pages = if state.peer_source.is_some() {
                    state
                        .page_locs
                        .values()
                        .filter(|(_, location)| location.fence < state.fence)
                        .count()
                } else {
                    0
                };
                VsetStats {
                    vset,
                    role,
                    fence: state.fence,
                    dirty_pages: self.cache.dirty_pages_of(vset).len(),
                    unstable_pages: self.cache.unstable_pages_of(vset).len(),
                    parked_faults: state.leaf_waiters.values().map(Vec::len).sum(),
                    pending_syncs: state.pending_syncs.len(),
                    pending_leaf_spans,
                    hydration_remaining_pages,
                    archive_lag_captures: Some(best.saturating_sub(backed)),
                    archive_lag_bytes: Some(state.backup_lag_bytes()),
                    operations: VsetOperations(operations),
                    live_segment_bytes: state.live_segment_bytes(),
                    local_segment_bytes: state
                        .segment_blobs
                        .iter()
                        .map(|&(_, _, bytes)| bytes)
                        .sum(),
                }
            })
            .collect::<Vec<_>>();
        let parked_faults = self.pressure_waiters.len()
            + self
                .vsets
                .values()
                .flat_map(|vset| vset.leaf_waiters.values())
                .map(Vec::len)
                .sum::<usize>();
        let (live_segment_bytes, local_segment_bytes) = self.seg_space();
        DaemonStats {
            cache_capacity_pages: self.cache.capacity(),
            resident_pages: self.cache.resident_count(),
            shared_resident_pages: self.cache.base_resident_count(),
            reserved_pages: self.cache.reserved_count(),
            dirty_pages: self.cache.dirty_count(),
            unstable_pages: self.cache.unstable_count(),
            pressure_waiting_faults: self.pressure_waiters.len(),
            parked_faults,
            local_blob_bytes: self.blob_sizes.values().sum(),
            disk_capacity_bytes: self.config.disk_capacity,
            disk_headroom_bytes: self.config.disk_headroom,
            live_segment_bytes,
            local_segment_bytes,
            vsets,
        }
    }

    pub fn replica_metrics(&self) -> Vec<ReplicaVsetMetrics> {
        self.vsets
            .iter()
            .map(|(&vset, state)| {
                let store_published_through = state.backed.map_or(0, |head| head.capture_seq);
                ReplicaVsetMetrics {
                    vset,
                    active_peer: state.stash_assignment.map(|stash| stash.active_peer),
                    transition_peer: state
                        .stash_assignment
                        .and_then(|stash| stash.transition_peer),
                    assignment_epoch: state.stash_assignment.map(|stash| stash.assignment_epoch),
                    local_covered_through: state.local_covered_through,
                    peer_committed_through: state.peer_committed_through,
                    store_published_through,
                    sync_ack_through: state.sync_ack_through,
                    queued_syncs: state.pending_syncs.len(),
                    upload_lag: state
                        .peer_committed_through
                        .saturating_sub(store_published_through),
                    current_retries: 0,
                    queued_releases: self
                        .replica_releases
                        .iter()
                        .filter(|(_, release_vset, _, _)| *release_vset == vset)
                        .count(),
                }
            })
            .collect()
    }

    pub fn replica_spool_metrics(&self) -> Vec<ReplicaSpoolMetrics> {
        self.replicas
            .iter()
            .map(|(key, replica)| ReplicaSpoolMetrics {
                source: key.source,
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
                stored_bytes: replica.bytes,
                host_capacity_bytes: self.config.archive.spool_capacity_bytes,
                current_generation: replica.current_generation,
                committed_through: replica
                    .committed
                    .map_or(0, |commit| commit.sync_covered_through),
                uploaded_through: replica
                    .uploaded
                    .map_or(0, |commit| commit.sync_covered_through),
                unarchived_age_ns: replica.unarchived_age,
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplicaKey {
    pub source: HostId,
    pub vset: VsetId,
    pub assignment_epoch: u64,
}

#[derive(Clone)]
pub struct ReplicaArchiveCut {
    pub info: crate::protocol::ReplicaCommitInfo,
    pub required: Vec<crate::protocol::ReplicaArtifact>,
    pub record: Vec<u8>,
}

#[derive(Default)]
pub struct ReplicaState {
    pub artifacts: BTreeMap<crate::protocol::ReplicaArtifact, (u32, Vec<u8>)>,
    pub committed: Option<crate::protocol::ReplicaCommitInfo>,
    pub committed_record: Option<Vec<u8>>,
    pub uploaded: Option<crate::protocol::ReplicaCommitInfo>,
    pub uploaded_record: Option<Vec<u8>>,
    pub archive_pending: Option<ReplicaArchiveCut>,
    pub archive_inflight: bool,
    pub archive_due: Option<u64>,
    pub unarchived_age: u64,
    pub bytes: u64,
    pub current_generation: u64,
    pub current_file_bytes: u64,
}

fn watch(
    active: bool,
    progress: u64,
    seen: &mut u64,
    ticks: &mut u64,
    threshold: u64,
    fired: &mut u64,
) {
    if !active || progress != *seen {
        *seen = progress;
        *ticks = 0;
        return;
    }
    *ticks += 1;
    if *ticks >= threshold {
        *fired += 1;
        *ticks = 0;
    }
}

#[derive(Default)]
pub struct WedgeState {
    pub fills: u64,
    fills_seen: u64,
    parked_ticks: u64,
    pub hydration: u64,
    hydration_seen: u64,
    hydration_ticks: u64,
    pub served: u64,
    served_seen: u64,
    outbound_ticks: u64,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum AttachmentPhase {
    #[default]
    Detached,
    Attached(AttachmentId),
    Draining(AttachmentId),
    Forced(AttachmentId),
}

#[derive(Default)]
pub struct DatabaseRuntime {
    pub phase: AttachmentPhase,
    pub handles: BTreeMap<u64, DatabaseFile>,
    pub active: Option<AttachmentId>,
    pub drain_barrier: u64,
}

pub struct DrainState {
    pub seq: JournalSeq,
    pub capture_seq: u64,
    pub unread: BTreeMap<PageId, Gen>,
    pub copied_on_fault: BTreeMap<PageId, (Gen, Vec<u8>)>,
    pub armed: Vec<PageId>,
    pub rescues: Vec<(PageId, Gen, Vec<u8>)>,
    pub compact_victims: BTreeSet<(u64, SegId)>,
}

#[allow(clippy::struct_excessive_bools)]
pub struct VsetState {
    pub incarnation: u64,
    pub config: VsetConfig,
    pub database: DatabaseMeta,
    pub fence: u64,
    pub ready: bool,
    pub epoch: Epoch,
    pub mutation_seq: u64,
    pub page_locs: BTreeMap<PageId, (Gen, PageLoc)>,
    pub overlay: BTreeMap<PageId, (Gen, PageLoc)>,
    pub leaf_table: BTreeMap<u32, LeafPtr>,
    pub leaf_blobs: BTreeMap<LeafPtr, (u64, BTreeSet<(u64, SegId)>)>,
    pub hydrated_spans: BTreeSet<u32>,
    pub failed_spans: BTreeSet<u32>,
    pub leaf_waiters: BTreeMap<u32, Vec<OneSender<()>>>,
    pub next_gen: u64,
    pub next_seq: u64,
    pub next_seg: u64,
    pub next_leaf: u64,
    pub best_record: Option<JournalRecord>,
    pub local_covered_through: u64,
    pub sync_ack_through: u64,
    pub pending_syncs: Vec<(crate::protocol::ReqId, u64)>,
    pub commit_running: bool,
    pub checkpoint_running: bool,
    pub capture_waiters: Vec<OneSender<()>>,
    pub checkpoint_results: BTreeMap<crate::protocol::ReqId, Epoch>,
    pub pinned: Option<JournalRecord>,
    pub drain: Option<DrainState>,
    pub record_writes: BTreeMap<JournalSeq, (u64, u64)>,
    pub segment_blobs: Vec<(u64, SegId, u64)>,
    pub head_version: Option<u64>,
    pub backed: Option<ManifestPtr>,
    pub backed_segments: BTreeSet<(u64, SegId)>,
    pub backed_leaves: BTreeSet<(u64, u64)>,
    pub store_manifests: BTreeSet<(u64, JournalSeq)>,
    pub publishing: bool,
    pub pending_verdict: Option<crate::protocol::Verdict>,
    pub outbound: Option<HostId>,
    pub peer_source: Option<HostId>,
    pub migration_running: bool,
    pub migration_accepted: bool,
    pub stash_assignment: Option<crate::head::StashAssignment>,
    pub retired_stashes: Vec<crate::head::RetiredStash>,
    pub replicating: bool,
    pub peer_committed_through: u64,
    pub peer_upload_done: Option<crate::protocol::ReplicaCommitInfo>,
    pub peer_upload_record: Option<JournalRecord>,
    pub wedge: WedgeState,
    pub database_runtime: DatabaseRuntime,
}

impl VsetState {
    pub(crate) fn fresh(config: VsetConfig, incarnation: u64) -> Self {
        Self {
            incarnation,
            config,
            database: DatabaseMeta::default(),
            fence: 1,
            ready: false,
            epoch: Epoch(0),
            mutation_seq: 0,
            page_locs: BTreeMap::new(),
            overlay: BTreeMap::new(),
            leaf_table: BTreeMap::new(),
            leaf_blobs: BTreeMap::new(),
            hydrated_spans: BTreeSet::new(),
            failed_spans: BTreeSet::new(),
            leaf_waiters: BTreeMap::new(),
            next_gen: 0,
            next_seq: 0,
            next_seg: 0,
            next_leaf: 0,
            best_record: None,
            local_covered_through: 0,
            sync_ack_through: 0,
            pending_syncs: Vec::new(),
            commit_running: false,
            checkpoint_running: false,
            capture_waiters: Vec::new(),
            checkpoint_results: BTreeMap::new(),
            pinned: None,
            drain: None,
            record_writes: BTreeMap::new(),
            segment_blobs: Vec::new(),
            head_version: None,
            backed: None,
            backed_segments: BTreeSet::new(),
            backed_leaves: BTreeSet::new(),
            store_manifests: BTreeSet::new(),
            publishing: false,
            pending_verdict: None,
            outbound: None,
            peer_source: None,
            migration_running: false,
            migration_accepted: false,
            stash_assignment: None,
            retired_stashes: Vec::new(),
            replicating: false,
            peer_committed_through: 0,
            peer_upload_done: None,
            peer_upload_record: None,
            wedge: WedgeState::default(),
            database_runtime: DatabaseRuntime::default(),
        }
    }

    fn live_segment_bytes(&self) -> u64 {
        self.page_locs
            .values()
            .filter(|(_, location)| location.base == 0)
            .map(|(_, location)| u64::from(location.len))
            .sum()
    }

    fn backup_lag_bytes(&self) -> u64 {
        let Some(record) = self.best_record.as_ref() else {
            return 0;
        };
        let pending = record
            .overlay
            .values()
            .filter(|(_, location)| location.base == 0)
            .map(|(_, location)| (location.fence, location.seg))
            .chain(record.leaves.values().flat_map(|pointer| {
                self.leaf_blobs
                    .get(pointer)
                    .into_iter()
                    .flat_map(|(_, segments)| segments.iter().copied())
            }))
            .filter(|segment| !self.backed_segments.contains(segment))
            .collect::<BTreeSet<_>>();
        self.segment_blobs
            .iter()
            .filter(|(fence, segment, _)| pending.contains(&(*fence, *segment)))
            .map(|(_, _, bytes)| *bytes)
            .sum()
    }
}

pub struct CacheReservation {
    state: SharedHost,
    active: bool,
}

pub struct PageFillLease {
    state: SharedHost,
    page: PageId,
    active: bool,
}

impl PageFillLease {
    pub fn new(state: &SharedHost, page: PageId) -> Self {
        Self {
            state: Rc::clone(state),
            page,
            active: true,
        }
    }

    pub fn finish(mut self, success: bool) {
        self.active = false;
        wake_page_fill(&self.state, self.page, success);
    }
}

impl Drop for PageFillLease {
    fn drop(&mut self) {
        if self.active {
            wake_page_fill(&self.state, self.page, false);
        }
    }
}

fn wake_page_fill(state: &SharedHost, page: PageId, success: bool) {
    let waiters = {
        let mut host = state.borrow_mut();
        host.filling_pages.remove(&page);
        host.page_fill_waiters.remove(&page).unwrap_or_default()
    };
    for waiter in waiters {
        let _ = waiter.send(success);
    }
}

pub struct CaptureLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
    active: bool,
}

pub struct CommitFlagLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
    active: bool,
}

impl CommitFlagLease {
    pub fn new(state: &SharedHost, vset: VsetId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            active: true,
        }
    }

    pub fn commit(mut self) {
        self.active = false;
    }
}

impl Drop for CommitFlagLease {
    fn drop(&mut self) {
        if self.active
            && let Some(vset) = self
                .state
                .borrow_mut()
                .vsets
                .get_mut(&self.vset)
                .filter(|vset| vset.incarnation == self.incarnation)
        {
            vset.commit_running = false;
        }
    }
}

impl CaptureLease {
    pub fn new(state: &SharedHost, vset: VsetId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            active: true,
        }
    }

    pub fn commit(mut self) {
        self.active = false;
    }
}

impl Drop for CaptureLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut host = self.state.borrow_mut();
        let Some(vset) = host
            .vsets
            .get_mut(&self.vset)
            .filter(|vset| vset.incarnation == self.incarnation)
        else {
            return;
        };
        let armed = vset
            .drain
            .take()
            .map(|drain| drain.armed)
            .unwrap_or_default();
        vset.commit_running = false;
        vset.checkpoint_running = false;
        let waiters = std::mem::take(&mut vset.capture_waiters);
        for page in armed {
            host.cache.end_flush(page);
            if !host.cache.is_dirty(page) {
                host.cache.mark_dirty(page);
            }
        }
        host.wake_pressure_waiter();
        drop(host);
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

impl CacheReservation {
    pub fn new(state: &SharedHost) -> Self {
        Self {
            state: Rc::clone(state),
            active: true,
        }
    }

    pub fn commit(mut self) {
        self.active = false;
    }
}

impl Drop for CacheReservation {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.state.borrow_mut();
            state.cache.release_slot();
            state.wake_pressure_waiter();
        }
    }
}
