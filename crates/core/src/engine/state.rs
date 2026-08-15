use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use blockd_exec::ReplyTarget;
use blockd_exec::channel::{OneSender, UnboundedSender};

use crate::authority::{AuthorityProof, PlacementRecord, VnodeId};
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

use super::HostFatal;
use super::peer_client::PeerClient;

pub type SharedHost = Rc<RefCell<HostState>>;

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
    pub(super) peer_client: PeerClient,
    pub inbound_migrations: BTreeSet<VsetId>,
    pub replicas: BTreeMap<ReplicaKey, ReplicaState>,
    pub replica_latest_epoch: BTreeMap<(HostId, VsetId), u64>,
    pub replica_releases: Vec<(HostId, VsetId, u64, crate::protocol::ReplicaCommitInfo)>,
    pub(crate) authority_session: Option<u64>,
    pub(crate) authority_host_epoch: u64,
    pub(crate) authority_serving: bool,
    pub(crate) authority_last_poll: u64,
    pub(crate) authority_placement: Option<PlacementRecord>,
    pub(crate) active_vnodes: BTreeMap<VnodeId, AuthorityProof>,
    scheduled_vsets: BTreeSet<VsetId>,
    scheduled_cursor: Option<VsetId>,
    disk_reclaim_scan_cursor: Option<VsetId>,
    disk_reclaim_scan_remaining: usize,
    fatal: Option<OneSender<HostFatal>>,
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
        let authority_serving = config
            .replica_placement
            .as_ref()
            .and_then(|placement| placement.authority)
            .is_none();
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
            peer_client: PeerClient::default(),
            inbound_migrations: BTreeSet::new(),
            replicas: BTreeMap::new(),
            replica_latest_epoch: BTreeMap::new(),
            replica_releases: Vec::new(),
            authority_session: None,
            authority_host_epoch: 0,
            authority_serving,
            authority_last_poll: 0,
            authority_placement: None,
            active_vnodes: BTreeMap::new(),
            scheduled_vsets: BTreeSet::new(),
            scheduled_cursor: None,
            disk_reclaim_scan_cursor: None,
            disk_reclaim_scan_remaining: 0,
            fatal: None,
            next_attachment_generation: 0,
            next_incarnation: 0,
        }
    }

    pub fn authority_serving(&self) -> bool {
        self.authority_serving
    }

    pub fn authority_session(&self) -> Option<u64> {
        self.authority_session
    }

    pub fn vset_authorized(&self, vset: VsetId) -> bool {
        let authority_enabled = self
            .config
            .replica_placement
            .as_ref()
            .and_then(|placement| placement.authority)
            .is_some();
        if !authority_enabled {
            return self.authority_serving;
        }
        self.authority_serving
            && self
                .authority_placement
                .as_ref()
                .is_some_and(|placement| self.active_vnodes.contains_key(&placement.vnode(vset)))
    }

    pub(crate) fn install_fatal_signal(&mut self, signal: OneSender<HostFatal>) {
        assert!(
            self.fatal.replace(signal).is_none(),
            "fatal signal installed once"
        );
    }

    pub(crate) fn clear_fatal_signal(&mut self) {
        self.fatal = None;
    }

    pub(crate) fn fail(&mut self, reason: &'static str) {
        if let Some(signal) = self.fatal.take() {
            let _ = signal.send(HostFatal::new(reason));
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

    pub(crate) fn schedule_vset(&mut self, vset: VsetId) {
        if self.vsets.contains_key(&vset) {
            self.scheduled_vsets.insert(vset);
        }
    }

    pub(crate) fn take_scheduled_vsets(&mut self, limit: usize) -> Vec<VsetId> {
        use std::ops::Bound::{Excluded, Unbounded};

        let selected = if let Some(cursor) = self.scheduled_cursor {
            self.scheduled_vsets
                .range((Excluded(cursor), Unbounded))
                .chain(self.scheduled_vsets.range(..=cursor))
                .take(limit)
                .copied()
                .collect::<Vec<_>>()
        } else {
            self.scheduled_vsets
                .iter()
                .take(limit)
                .copied()
                .collect::<Vec<_>>()
        };
        if let Some(last) = selected.last() {
            self.scheduled_cursor = Some(*last);
        }
        for vset in &selected {
            self.scheduled_vsets.remove(vset);
        }
        selected
    }

    pub(crate) fn scheduled_vset_count(&self) -> usize {
        self.scheduled_vsets.len()
    }

    pub(crate) fn take_disk_reclaim_scan_vsets(&mut self, limit: usize) -> Vec<VsetId> {
        use std::ops::Bound::{Excluded, Unbounded};

        if !self.disk_reclaim_requested {
            self.disk_reclaim_scan_remaining = 0;
            return Vec::new();
        }
        let limit = limit.min(self.disk_reclaim_scan_remaining);
        let selected = if let Some(cursor) = self.disk_reclaim_scan_cursor {
            self.vsets
                .range((Excluded(cursor), Unbounded))
                .chain(self.vsets.range(..=cursor))
                .take(limit)
                .map(|(&vset, _)| vset)
                .collect::<Vec<_>>()
        } else {
            self.vsets.keys().take(limit).copied().collect::<Vec<_>>()
        };
        if let Some(last) = selected.last() {
            self.disk_reclaim_scan_cursor = Some(*last);
        }
        self.disk_reclaim_scan_remaining = self
            .disk_reclaim_scan_remaining
            .saturating_sub(selected.len());
        if selected.is_empty() {
            self.disk_reclaim_scan_remaining = 0;
        }
        selected
    }

    fn request_disk_reclaim(&mut self) {
        if !self.disk_reclaim_requested {
            self.disk_reclaim_scan_cursor = None;
            self.disk_reclaim_scan_remaining = self.vsets.len();
        }
        self.disk_reclaim_requested = true;
    }

    pub(super) fn note_blob_full(&mut self) {
        self.counters.nvme_stalls = self.counters.nvme_stalls.saturating_add(1);
        self.request_disk_reclaim();
    }

    pub fn wake_pressure_waiter(&mut self) {
        while let Some(waiter) = self.pressure_waiters.pop_front() {
            if waiter.send(()).is_ok() {
                return;
            }
        }
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

    pub(crate) fn local_artifact_fences(&self, vset: VsetId) -> BTreeSet<u64> {
        self.blob_sizes
            .keys()
            .filter_map(|name| match crate::layout::parse_blob(name)? {
                crate::layout::BlobName::Journal {
                    vset: found, fence, ..
                }
                | crate::layout::BlobName::Segment {
                    vset: found, fence, ..
                }
                | crate::layout::BlobName::Leaf {
                    vset: found, fence, ..
                } if found == vset => Some(fence),
                _ => None,
            })
            .collect()
    }

    pub fn disk_reclaim_target_met(&self) -> bool {
        self.config.disk_capacity.is_none_or(|capacity| {
            self.blob_sizes
                .values()
                .sum::<u64>()
                .saturating_add(self.config.disk_headroom)
                <= capacity
        })
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
            self.request_disk_reclaim();
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
            self.request_disk_reclaim();
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
            self.request_disk_reclaim();
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
            self.request_disk_reclaim();
            return false;
        }
        self.append_blob(name, bytes);
        true
    }

    pub fn append_blob(&mut self, name: String, bytes: u64) {
        let stored = self.blob_sizes.entry(name).or_default();
        *stored = stored.saturating_add(bytes);
    }

    pub(super) fn rollback_append_reservation(&mut self, name: &str, bytes: u64) {
        let Some(stored) = self.blob_sizes.get_mut(name) else {
            return;
        };
        *stored = stored.saturating_sub(bytes);
        if *stored == 0 {
            self.blob_sizes.remove(name);
        }
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

    #[allow(clippy::too_many_lines)]
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
                if state.operations.mutation_owner().is_some() {
                    operations |= VsetOperations::CAPTURE;
                }
                if matches!(
                    state.operations.mutation_owner(),
                    Some(MutationOwner::Capture(
                        CaptureKind::Checkpoint | CaptureKind::Migration
                    ))
                ) {
                    operations |= VsetOperations::CHECKPOINT;
                }
                if state.operations.publication_owner().is_some() {
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
    pub unread: BTreeMap<PageId, Gen>,
    pub copied_on_fault: BTreeMap<PageId, (Gen, Vec<u8>)>,
    pub armed: Vec<PageId>,
    pub rescues: Vec<(PageId, Gen, Vec<u8>)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CaptureKind {
    Writeback,
    Checkpoint,
    Migration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MutationOwner {
    Capture(CaptureKind),
    Database,
    Hydration,
}

struct MutationOperation {
    owner: MutationOwner,
    drain: Option<DrainState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PublicationOwner {
    Direct,
    Replica,
}

#[derive(Default)]
pub struct VsetOperationState {
    mutation: Option<MutationOperation>,
    migration: bool,
    guest_resume_pending: bool,
    publication: Option<PublicationOwner>,
    replication: bool,
    recovery: Option<crate::protocol::Verdict>,
}

impl VsetOperationState {
    pub(super) fn mutation_owner(&self) -> Option<MutationOwner> {
        self.mutation.as_ref().map(|operation| operation.owner)
    }

    pub(super) fn try_start_mutation(&mut self, owner: MutationOwner) -> bool {
        if self.mutation.is_some()
            || self.guest_resume_pending
            || (self.migration && owner != MutationOwner::Capture(CaptureKind::Migration))
        {
            return false;
        }
        self.mutation = Some(MutationOperation { owner, drain: None });
        true
    }

    pub(super) fn finish_mutation(&mut self, owner: MutationOwner) -> Option<DrainState> {
        if self.mutation_owner() != Some(owner) {
            return None;
        }
        self.mutation.take().and_then(|operation| operation.drain)
    }

    pub(super) fn begin_drain(&mut self, drain: DrainState) {
        let operation = self
            .mutation
            .as_mut()
            .expect("capture owns the mutation slot before draining");
        assert!(
            matches!(operation.owner, MutationOwner::Capture(_)),
            "only capture operations may own a page drain"
        );
        assert!(
            operation.drain.replace(drain).is_none(),
            "capture begins one drain"
        );
    }

    pub(super) fn drain(&self) -> Option<&DrainState> {
        self.mutation
            .as_ref()
            .and_then(|operation| operation.drain.as_ref())
    }

    pub(super) fn drain_mut(&mut self) -> Option<&mut DrainState> {
        self.mutation
            .as_mut()
            .and_then(|operation| operation.drain.as_mut())
    }

    pub(super) fn start_migration(&mut self) -> bool {
        if self.migration || self.guest_resume_pending {
            return false;
        }
        self.migration = true;
        true
    }

    pub(super) fn finish_migration(&mut self) {
        self.migration = false;
    }

    pub(super) fn migration_running(&self) -> bool {
        self.migration
    }

    pub(super) fn mutation_blocked(&self) -> bool {
        self.mutation.is_some() || self.guest_resume_pending
    }

    pub(super) fn start_guest_resume(&mut self) -> bool {
        if self.guest_resume_pending {
            return false;
        }
        self.guest_resume_pending = true;
        true
    }

    pub(super) fn finish_guest_resume(&mut self) {
        self.guest_resume_pending = false;
    }

    pub(super) fn guest_resume_pending(&self) -> bool {
        self.guest_resume_pending
    }

    pub(super) fn try_start_publication(&mut self, owner: PublicationOwner) -> bool {
        if self.publication.is_some() {
            return false;
        }
        self.publication = Some(owner);
        true
    }

    pub(super) fn finish_publication(&mut self, owner: PublicationOwner) {
        if self.publication == Some(owner) {
            self.publication = None;
        }
    }

    pub(super) fn publication_owner(&self) -> Option<PublicationOwner> {
        self.publication
    }

    pub(super) fn try_start_replication(&mut self) -> bool {
        if self.replication {
            return false;
        }
        self.replication = true;
        true
    }

    pub(super) fn finish_replication(&mut self) {
        self.replication = false;
    }

    pub(super) fn replication_running(&self) -> bool {
        self.replication
    }

    pub fn recovery_pending(&self) -> bool {
        self.recovery.is_some()
    }

    pub(super) fn set_recovery(&mut self, verdict: crate::protocol::Verdict) {
        assert!(
            self.recovery.replace(verdict).is_none(),
            "one startup recovery verdict per vset"
        );
    }

    pub(super) fn take_recovery(&mut self) -> Option<crate::protocol::Verdict> {
        self.recovery.take()
    }
}

pub struct PendingSync {
    id: u64,
    pub barrier: u64,
    reply: ReplyTarget<bool>,
    resolved: Option<UnboundedSender<()>>,
}

impl PendingSync {
    pub fn new(
        id: u64,
        barrier: u64,
        reply: ReplyTarget<bool>,
        resolved: UnboundedSender<()>,
    ) -> Self {
        Self {
            id,
            barrier,
            reply,
            resolved: Some(resolved),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn resolve(mut self, success: bool) {
        let _ = self.reply.send(success);
        self.notify_resolved();
    }

    fn notify_resolved(&mut self) {
        if let Some(resolved) = self.resolved.take() {
            let _ = resolved.send(());
        }
    }
}

impl Drop for PendingSync {
    fn drop(&mut self) {
        self.notify_resolved();
    }
}

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
    pub pending_syncs: Vec<PendingSync>,
    pub operations: VsetOperationState,
    pub mutation_waiters: Vec<OneSender<()>>,
    pub checkpoint_results: BTreeMap<crate::protocol::ReqId, Epoch>,
    pub pinned: Option<JournalRecord>,
    pub record_writes: BTreeMap<JournalSeq, (u64, u64)>,
    pub segment_blobs: Vec<(u64, SegId, u64)>,
    pub head_version: Option<u64>,
    pub backed: Option<ManifestPtr>,
    pub backed_segments: BTreeSet<(u64, SegId)>,
    pub backed_leaves: BTreeSet<(u64, u64)>,
    pub store_manifests: BTreeSet<(u64, JournalSeq)>,
    pub outbound: Option<HostId>,
    pub peer_source: Option<HostId>,
    pub peer_source_offer_fence: Option<u64>,
    pub hydration_waiters: Vec<OneSender<bool>>,
    pub stash_assignment: Option<crate::head::StashAssignment>,
    pub retired_stashes: Vec<crate::head::RetiredStash>,
    pub peer_committed: Option<crate::protocol::ReplicaCommitInfo>,
    pub peer_published: Option<crate::protocol::ReplicaCommitInfo>,
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
            operations: VsetOperationState::default(),
            mutation_waiters: Vec::new(),
            checkpoint_results: BTreeMap::new(),
            pinned: None,
            record_writes: BTreeMap::new(),
            segment_blobs: Vec::new(),
            head_version: None,
            backed: None,
            backed_segments: BTreeSet::new(),
            backed_leaves: BTreeSet::new(),
            store_manifests: BTreeSet::new(),
            outbound: None,
            peer_source: None,
            peer_source_offer_fence: None,
            hydration_waiters: Vec::new(),
            stash_assignment: None,
            retired_stashes: Vec::new(),
            peer_committed: None,
            peer_published: None,
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
    owner: MutationOwner,
    cleanup: Option<OneSender<Vec<PageId>>>,
    cleanup_finishes_mutation: bool,
    active: bool,
}

pub struct CommitFlagLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
    owner: MutationOwner,
    active: bool,
}

impl CommitFlagLease {
    pub fn new(state: &SharedHost, vset: VsetId, incarnation: u64, owner: MutationOwner) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            owner,
            active: true,
        }
    }

    pub fn commit(mut self) {
        self.state.borrow_mut().schedule_vset(self.vset);
        self.active = false;
    }
}

impl Drop for CommitFlagLease {
    fn drop(&mut self) {
        let waiters = if self.active {
            let mut host = self.state.borrow_mut();
            host.vsets
                .get_mut(&self.vset)
                .filter(|vset| vset.incarnation == self.incarnation)
                .map_or_else(Vec::new, |vset| {
                    vset.operations.finish_mutation(self.owner);
                    std::mem::take(&mut vset.mutation_waiters)
                })
        } else {
            Vec::new()
        };
        for waiter in waiters {
            let _ = waiter.send(());
        }
        self.state.borrow_mut().schedule_vset(self.vset);
    }
}

impl CaptureLease {
    pub fn new(
        state: &SharedHost,
        vset: VsetId,
        incarnation: u64,
        owner: MutationOwner,
        cleanup: OneSender<Vec<PageId>>,
    ) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            owner,
            cleanup: Some(cleanup),
            cleanup_finishes_mutation: false,
            active: true,
        }
    }

    pub fn new_with_serialized_cleanup(
        state: &SharedHost,
        vset: VsetId,
        incarnation: u64,
        owner: MutationOwner,
        cleanup: OneSender<Vec<PageId>>,
    ) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            owner,
            cleanup: Some(cleanup),
            cleanup_finishes_mutation: true,
            active: true,
        }
    }

    pub fn commit(mut self) {
        if self.cleanup_finishes_mutation {
            self.cleanup.take();
        } else if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.send(Vec::new());
        }
        self.state.borrow_mut().schedule_vset(self.vset);
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
            drop(host);
            if let Some(cleanup) = self.cleanup.take() {
                let _ = cleanup.send(Vec::new());
            }
            return;
        };
        let armed = if self.cleanup_finishes_mutation {
            vset.operations
                .drain_mut()
                .map(|drain| std::mem::take(&mut drain.armed))
                .unwrap_or_default()
        } else {
            vset.operations
                .finish_mutation(self.owner)
                .map(|drain| drain.armed)
                .unwrap_or_default()
        };
        let waiters = if self.cleanup_finishes_mutation {
            Vec::new()
        } else {
            std::mem::take(&mut vset.mutation_waiters)
        };
        for &page in &armed {
            host.cache.end_flush(page);
            if !host.cache.is_dirty(page) {
                host.cache.mark_dirty(page);
            }
        }
        host.wake_pressure_waiter();
        drop(host);
        if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.send(armed);
        }
        for waiter in waiters {
            let _ = waiter.send(());
        }
        if !self.cleanup_finishes_mutation {
            self.state.borrow_mut().schedule_vset(self.vset);
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

#[cfg(test)]
mod tests {
    use super::{CaptureKind, MutationOwner, PublicationOwner, VsetOperationState};

    #[test]
    fn typed_operation_slots_reject_overlapping_owners() {
        let mut operations = VsetOperationState::default();

        assert!(operations.try_start_mutation(MutationOwner::Database));
        assert!(!operations.try_start_mutation(MutationOwner::Hydration));
        assert_eq!(operations.mutation_owner(), Some(MutationOwner::Database));
        assert!(
            operations
                .finish_mutation(MutationOwner::Hydration)
                .is_none()
        );
        assert_eq!(operations.mutation_owner(), Some(MutationOwner::Database));
        operations.finish_mutation(MutationOwner::Database);
        assert!(operations.try_start_mutation(MutationOwner::Hydration));
        operations.finish_mutation(MutationOwner::Hydration);

        assert!(operations.start_migration());
        assert!(!operations.start_migration());
        assert!(!operations.try_start_mutation(MutationOwner::Database));
        assert!(operations.try_start_mutation(MutationOwner::Capture(CaptureKind::Migration)));
        operations.finish_mutation(MutationOwner::Capture(CaptureKind::Migration));
        operations.finish_migration();
        assert!(!operations.migration_running());

        assert!(operations.start_guest_resume());
        assert!(operations.guest_resume_pending());
        assert!(!operations.try_start_mutation(MutationOwner::Database));
        assert!(!operations.start_migration());
        operations.finish_guest_resume();
        assert!(operations.try_start_mutation(MutationOwner::Database));
        operations.finish_mutation(MutationOwner::Database);

        assert!(operations.try_start_publication(PublicationOwner::Direct));
        assert!(!operations.try_start_publication(PublicationOwner::Replica));
        operations.finish_publication(PublicationOwner::Replica);
        assert_eq!(
            operations.publication_owner(),
            Some(PublicationOwner::Direct)
        );
        operations.finish_publication(PublicationOwner::Direct);
        assert!(operations.publication_owner().is_none());
    }
}
