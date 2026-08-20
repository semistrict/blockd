use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use blockd_exec::Reply;
use blockd_exec::channel::{OneSender, UnboundedSender};

use crate::blx::{BlxFooter, NamespaceKind};
use crate::cache::Cache;
use crate::head::{HeadRecord, ManifestPtr, RetiredStash, StashAssignment};
use crate::hostmeta::{
    Counters, DaemonStats, HostConfig, ReplicaSpoolMetrics, ReplicaVolumeMetrics, VolumeOperations,
    VolumeRole, VolumeStats,
};
use crate::journal::{JournalRecord, VolumeConfig};
use crate::manifest::{BaseRef, ObjectIdentity, ObjectRef};
use crate::page_file::PageFileLoc;
use crate::placement::ClusterPlacement;
use crate::protocol::ReplicaCommitInfo;
use crate::types::{Epoch, Gen, HostId, JournalSeq, PageId, VolumeId};

use super::HostFatal;
use super::peer_client::PeerClient;

pub type SharedHost = Rc<RefCell<HostState>>;

pub(crate) struct AuthorityLease {
    pub(crate) session: Option<u64>,
    pub(crate) host_epoch: u64,
    pub(crate) serving: bool,
    pub(crate) last_poll: u64,
    pub(crate) placement: Option<ClusterPlacement>,
}

impl AuthorityLease {
    fn new(serving: bool) -> Self {
        Self {
            session: None,
            host_epoch: 0,
            serving,
            last_poll: 0,
            placement: None,
        }
    }
}

pub struct HostState {
    pub config: HostConfig,
    pub cache: Cache,
    pub volumes: BTreeMap<VolumeId, VolumeState>,
    pub counters: Counters,
    pub blob_sizes: BTreeMap<String, u64>,
    pub disk_reclaim_requested: bool,
    pub pressure_waiters: VecDeque<OneSender<()>>,
    pub filling_pages: BTreeSet<PageId>,
    pub page_fill_waiters: BTreeMap<PageId, Vec<OneSender<bool>>>,
    pub(super) peer_client: PeerClient,
    pub inbound_migrations: BTreeSet<VolumeId>,
    pub(crate) released_migration_fences: BTreeMap<VolumeId, u64>,
    pub replicas: BTreeMap<ReplicaKey, ReplicaState>,
    pub replica_releases: Vec<(HostId, VolumeId, u64, crate::protocol::ReplicaCommitInfo)>,
    replica_volume_counters: BTreeMap<VolumeId, ReplicaVolumeCounters>,
    pub(crate) authority: AuthorityLease,
    scheduled_volumes: BTreeSet<VolumeId>,
    scheduled_cursor: Option<VolumeId>,
    disk_reclaim_scan_cursor: Option<VolumeId>,
    disk_reclaim_scan_remaining: usize,
    fatal: Option<OneSender<HostFatal>>,
    #[cfg(test)]
    pub(super) critical_child_failure: Option<super::host::CriticalChildSource>,
    next_run_generation: u64,
}

#[derive(Clone, Copy, Default)]
struct ReplicaVolumeCounters {
    integrity_rejects: u64,
    replacement_bytes: u64,
    cleanup_unlinks: u64,
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
            .cluster_placement
            .as_ref()
            .and_then(|placement| placement.authority)
            .is_none();
        let peer_client = PeerClient::for_host(config.backup_retry);
        Self {
            cache: Cache::new(config.cache_pages),
            config,
            volumes: BTreeMap::new(),
            counters: Counters::default(),
            blob_sizes: BTreeMap::new(),
            disk_reclaim_requested: false,
            pressure_waiters: VecDeque::new(),
            filling_pages: BTreeSet::new(),
            page_fill_waiters: BTreeMap::new(),
            peer_client,
            inbound_migrations: BTreeSet::new(),
            released_migration_fences: BTreeMap::new(),
            replicas: BTreeMap::new(),
            replica_releases: Vec::new(),
            replica_volume_counters: BTreeMap::new(),
            authority: AuthorityLease::new(authority_serving),
            scheduled_volumes: BTreeSet::new(),
            scheduled_cursor: None,
            disk_reclaim_scan_cursor: None,
            disk_reclaim_scan_remaining: 0,
            fatal: None,
            #[cfg(test)]
            critical_child_failure: None,
            next_run_generation: 0,
        }
    }

    pub fn authority_serving(&self) -> bool {
        self.authority.serving
    }

    pub fn authority_session(&self) -> Option<u64> {
        self.authority.session
    }

    pub fn authority_host_epoch(&self) -> u64 {
        self.authority.host_epoch
    }

    pub fn authority_ready(&self) -> bool {
        self.authority.serving
    }

    pub fn authority_placement_epoch(&self) -> u64 {
        self.authority
            .placement
            .as_ref()
            .map_or(0, |placement| placement.epoch)
    }

    pub fn volume_authorized(&self, _volume: VolumeId) -> bool {
        let authority = self
            .config
            .cluster_placement
            .as_ref()
            .and_then(|placement| placement.authority);
        let Some(authority) = authority else {
            return self.authority.serving;
        };
        if blockd_exec::now().saturating_sub(self.authority.last_poll)
            >= authority.max_poll_staleness
        {
            return false;
        }
        self.authority.serving
            && self
                .authority
                .placement
                .as_ref()
                .is_some_and(|placement| placement.contains(self.config.host))
    }

    pub(super) fn volume_at(&self, volume: VolumeId, run_generation: u64) -> Option<&VolumeState> {
        self.volumes
            .get(&volume)
            .filter(|state| state.run_generation == run_generation)
    }

    pub(super) fn volume_at_mut(
        &mut self,
        volume: VolumeId,
        run_generation: u64,
    ) -> Option<&mut VolumeState> {
        self.volumes
            .get_mut(&volume)
            .filter(|state| state.run_generation == run_generation)
    }

    pub(super) fn finish_primary_commit(
        &mut self,
        volume: VolumeId,
        run_generation: u64,
        info: ReplicaCommitInfo,
        record: JournalRecord,
    ) -> Vec<PendingSync> {
        let authority_serving = self.volume_authorized(volume);
        let Some(volume_state) = self.volume_at_mut(volume, run_generation) else {
            return Vec::new();
        };
        if volume_state
            .peer_committed
            .is_none_or(|committed| info >= committed)
        {
            volume_state.peer_committed = Some(info);
            volume_state.peer_committed_record = Some(record);
        }
        volume_state.peer_committed_through = volume_state
            .peer_committed_through
            .max(info.sync_covered_through);
        if !authority_serving {
            return Vec::new();
        }
        volume_state.sync_ack_through =
            volume_state.sync_ack_through.max(info.sync_covered_through);
        let pending = std::mem::take(&mut volume_state.pending_syncs);
        let (completed, waiting): (Vec<_>, Vec<_>) = pending
            .into_iter()
            .partition(|sync| sync.barrier <= volume_state.sync_ack_through);
        volume_state.pending_syncs = waiting;
        self.counters.syncs_acked += completed.len() as u64;
        self.schedule_volume(volume);
        completed
    }

    pub(super) fn adopt_assignment(
        &mut self,
        volume: VolumeId,
        run_generation: u64,
        version: u64,
        assignment: StashAssignment,
        retired_stashes: Vec<RetiredStash>,
    ) -> bool {
        let Some(volume_state) = self.volume_at_mut(volume, run_generation) else {
            return false;
        };
        volume_state.adopt_assignment(version, assignment, retired_stashes);
        true
    }

    pub(super) fn adopt_assignment_from_head(
        &mut self,
        volume: VolumeId,
        run_generation: u64,
        version: u64,
        head: HeadRecord,
    ) -> bool {
        let Some(volume_state) = self.volume_at_mut(volume, run_generation) else {
            return false;
        };
        volume_state.adopt_assignment_from_head(version, head);
        true
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

    pub fn insert_fresh(&mut self, volume: VolumeId, config: VolumeConfig) -> u64 {
        let run_generation = self.allocate_run_generation();
        let previous = self
            .volumes
            .insert(volume, VolumeState::fresh(config, run_generation));
        assert!(previous.is_none(), "duplicate volume insertion");
        run_generation
    }

    pub(crate) fn allocate_run_generation(&mut self) -> u64 {
        let run_generation = self.next_run_generation;
        self.next_run_generation = self
            .next_run_generation
            .checked_add(1)
            .expect("volume run_generation overflow");
        run_generation
    }

    pub(crate) fn schedule_volume(&mut self, volume: VolumeId) {
        if self.volumes.contains_key(&volume) {
            self.scheduled_volumes.insert(volume);
        }
    }

    pub(crate) fn record_replica_reject(&mut self, volume: VolumeId) {
        self.counters.replica_rejected = self.counters.replica_rejected.saturating_add(1);
        if let Some(counters) = self.replica_volume_counters.get_mut(&volume) {
            counters.integrity_rejects = counters.integrity_rejects.saturating_add(1);
        }
    }

    pub(crate) fn record_replica_replacement(&mut self, volume: VolumeId, bytes: u64) {
        self.counters.replica_replacement_bytes = self
            .counters
            .replica_replacement_bytes
            .saturating_add(bytes);
        let counters = self.replica_volume_counters.entry(volume).or_default();
        counters.replacement_bytes = counters.replacement_bytes.saturating_add(bytes);
    }

    pub(crate) fn record_replica_cleanup(&mut self, volume: VolumeId) {
        self.counters.replica_unlinks = self.counters.replica_unlinks.saturating_add(1);
        let counters = self.replica_volume_counters.entry(volume).or_default();
        counters.cleanup_unlinks = counters.cleanup_unlinks.saturating_add(1);
    }

    pub(crate) fn take_scheduled_volumes(&mut self, limit: usize) -> Vec<VolumeId> {
        use std::ops::Bound::{Excluded, Unbounded};

        let selected = if let Some(cursor) = self.scheduled_cursor {
            self.scheduled_volumes
                .range((Excluded(cursor), Unbounded))
                .chain(self.scheduled_volumes.range(..=cursor))
                .take(limit)
                .copied()
                .collect::<Vec<_>>()
        } else {
            self.scheduled_volumes
                .iter()
                .take(limit)
                .copied()
                .collect::<Vec<_>>()
        };
        if let Some(last) = selected.last() {
            self.scheduled_cursor = Some(*last);
        }
        for volume in &selected {
            self.scheduled_volumes.remove(volume);
        }
        selected
    }

    pub(crate) fn scheduled_volume_count(&self) -> usize {
        self.scheduled_volumes.len()
    }

    pub(crate) fn take_disk_reclaim_scan_volumes(&mut self, limit: usize) -> Vec<VolumeId> {
        use std::ops::Bound::{Excluded, Unbounded};

        if !self.disk_reclaim_requested {
            self.disk_reclaim_scan_remaining = 0;
            return Vec::new();
        }
        let limit = limit.min(self.disk_reclaim_scan_remaining);
        let selected = if let Some(cursor) = self.disk_reclaim_scan_cursor {
            self.volumes
                .range((Excluded(cursor), Unbounded))
                .chain(self.volumes.range(..=cursor))
                .take(limit)
                .map(|(&volume, _)| volume)
                .collect::<Vec<_>>()
        } else {
            self.volumes.keys().take(limit).copied().collect::<Vec<_>>()
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
            self.disk_reclaim_scan_remaining = self.volumes.len();
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

    pub fn wedge_tick(&mut self) {
        let threshold = self.config.wedge_ticks;
        if threshold == 0 {
            return;
        }
        let filling = self
            .filling_pages
            .iter()
            .map(|page| page.volume)
            .collect::<BTreeSet<_>>();
        for (&volume, state) in &mut self.volumes {
            watch(
                filling.contains(&volume),
                state.wedge.fills,
                &mut state.wedge.fills_seen,
                &mut state.wedge.parked_ticks,
                threshold,
                &mut self.counters.wedged_guests,
            );
            watch(
                state.peer_source.is_some(),
                state.wedge.hydration,
                &mut state.wedge.hydration_seen,
                &mut state.wedge.hydration_ticks,
                threshold,
                &mut self.counters.wedged_hydration,
            );
            watch(
                state.outbound.is_some(),
                state.wedge.served,
                &mut state.wedge.served_seen,
                &mut state.wedge.outbound_ticks,
                threshold,
                &mut self.counters.wedged_outbound,
            );
        }
    }

    pub fn record_blob(&mut self, name: String, bytes: u64) {
        self.blob_sizes.insert(name, bytes);
    }

    pub(crate) fn local_artifact_fences(&self, volume: VolumeId) -> BTreeSet<u64> {
        self.blob_sizes
            .keys()
            .filter_map(|name| match crate::layout::parse_blob(name)? {
                crate::layout::BlobName::Journal {
                    volume: found,
                    fence,
                    ..
                }
                | crate::layout::BlobName::Blx {
                    volume: found,
                    fence,
                    ..
                } if found == volume => Some(fence),
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

    pub fn blx_space(&self) -> (u64, u64) {
        self.volumes.values().fold((0, 0), |(live, local), volume| {
            (
                live.saturating_add(volume.live_blx_bytes()),
                local.saturating_add(
                    volume
                        .blx_blobs
                        .iter()
                        .map(|&(_, bytes)| bytes)
                        .sum::<u64>(),
                ),
            )
        })
    }

    #[allow(clippy::too_many_lines)]
    pub fn stats(&self) -> DaemonStats {
        let volumes = self
            .volumes
            .iter()
            .map(|(&volume, state)| {
                let hydrating = state.peer_source.is_some();
                let role = if state.outbound.is_some() {
                    VolumeRole::Outbound
                } else if hydrating {
                    VolumeRole::Hydrating
                } else if state.ready {
                    VolumeRole::Serving
                } else {
                    VolumeRole::Initializing
                };
                let mut operations = 0;
                if state.operations.mutation_owner().is_some() {
                    operations |= VolumeOperations::CAPTURE;
                }
                if matches!(
                    state.operations.mutation_owner(),
                    Some(MutationOwner::Capture(
                        CaptureKind::Checkpoint | CaptureKind::Migration
                    ))
                ) {
                    operations |= VolumeOperations::CHECKPOINT;
                }
                if state.operations.publication_running() {
                    operations |= VolumeOperations::BACKUP;
                }
                if hydrating {
                    operations |= VolumeOperations::HYDRATION;
                }
                let best = state
                    .best_record
                    .as_ref()
                    .map_or(0, |record| record.capture_seq);
                let backed = state.backed.map_or(0, |pointer| pointer.capture_seq);
                let hydration_remaining_pages = if state.peer_source.is_some() {
                    state
                        .page_locs
                        .values()
                        .filter(|(_, location)| location.fence < state.fence)
                        .count()
                } else {
                    0
                };
                VolumeStats {
                    volume,
                    role,
                    fence: state.fence,
                    dirty_pages: self.cache.dirty_pages_of(volume).len(),
                    pages_dirtied_total: state.pages_dirtied_total,
                    unstable_pages: self.cache.unstable_pages_of(volume).len(),
                    pending_syncs: state.pending_syncs.len(),
                    hydration_remaining_pages,
                    archive_lag_captures: Some(best.saturating_sub(backed)),
                    archive_lag_bytes: Some(state.backup_lag_bytes()),
                    operations: VolumeOperations(operations),
                    live_blx_bytes: state.live_blx_bytes(),
                    local_blx_bytes: state.blx_blobs.iter().map(|&(_, bytes)| bytes).sum(),
                }
            })
            .collect::<Vec<_>>();
        let (live_blx_bytes, local_blx_bytes) =
            volumes
                .iter()
                .fold((0_u64, 0_u64), |(live, local), volume| {
                    (
                        live.saturating_add(volume.live_blx_bytes),
                        local.saturating_add(volume.local_blx_bytes),
                    )
                });
        DaemonStats {
            cache_capacity_pages: self.cache.capacity(),
            resident_pages: self.cache.resident_count(),
            shared_resident_pages: self.cache.base_resident_count(),
            reserved_pages: self.cache.reserved_count(),
            dirty_pages: self.cache.dirty_count(),
            unstable_pages: self.cache.unstable_count(),
            pressure_waiting_faults: self.pressure_waiters.len(),
            local_blob_bytes: self.blob_sizes.values().sum(),
            disk_capacity_bytes: self.config.disk_capacity,
            disk_headroom_bytes: self.config.disk_headroom,
            live_blx_bytes,
            local_blx_bytes,
            volumes,
        }
    }

    pub fn replica_metrics(&self) -> Vec<ReplicaVolumeMetrics> {
        self.volumes
            .iter()
            .map(|(&volume, state)| {
                let accounting = self
                    .replica_volume_counters
                    .get(&volume)
                    .copied()
                    .unwrap_or_default();
                let store_published_through = state.backed.map_or(0, |head| head.capture_seq);
                ReplicaVolumeMetrics {
                    volume,
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
                        .filter(|(_, release_volume, _, _)| *release_volume == volume)
                        .count(),
                    integrity_rejects: accounting.integrity_rejects,
                    replacement_bytes: accounting.replacement_bytes,
                    cleanup_unlinks: accounting.cleanup_unlinks,
                }
            })
            .collect()
    }

    pub fn replica_spool_metrics(&self) -> Vec<ReplicaSpoolMetrics> {
        self.replicas
            .iter()
            .map(|(key, replica)| ReplicaSpoolMetrics {
                integrity_rejects: self
                    .replica_volume_counters
                    .get(&key.volume)
                    .map_or(0, |counters| counters.integrity_rejects),
                replacement_bytes: self
                    .replica_volume_counters
                    .get(&key.volume)
                    .map_or(0, |counters| counters.replacement_bytes),
                cleanup_unlinks: self
                    .replica_volume_counters
                    .get(&key.volume)
                    .map_or(0, |counters| counters.cleanup_unlinks),
                source: key.source,
                volume: key.volume,
                assignment_epoch: key.assignment_epoch,
                stored_bytes: replica.bytes,
                host_capacity_bytes: self.config.archive.spool_capacity_bytes,
                current_generation: replica.current_generation,
                committed_through: replica
                    .committed
                    .map_or(0, |commit| commit.sync_covered_through),
                uploaded_through: 0,
                unarchived_age_ns: 0,
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplicaKey {
    pub source: HostId,
    pub volume: VolumeId,
    pub assignment_epoch: u64,
}

#[derive(Default)]
pub struct ReplicaState {
    pub artifacts: BTreeMap<crate::protocol::ReplicaArtifact, (u32, Vec<u8>)>,
    /// Artifacts appended after the last complete commit. A release for the
    /// previous commit must not erase these bytes out from under a new put.
    pub uncommitted_artifacts: BTreeSet<crate::protocol::ReplicaArtifact>,
    pub committed: Option<crate::protocol::ReplicaCommitInfo>,
    pub committed_record: Option<Vec<u8>>,
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
    Hydration,
}

struct MutationOperation {
    owner: MutationOwner,
    drain: Option<DrainState>,
}

#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct VolumeOperationState {
    mutation: Option<MutationOperation>,
    migration: bool,
    guest_resume_pending: bool,
    publication: bool,
    replication: bool,
    recovery: Option<crate::protocol::Verdict>,
}

impl VolumeOperationState {
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

    pub(super) fn try_start_publication(&mut self) -> bool {
        if self.publication {
            return false;
        }
        self.publication = true;
        true
    }

    pub(super) fn finish_publication(&mut self) {
        self.publication = false;
    }

    pub(super) fn publication_running(&self) -> bool {
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
            "one startup recovery verdict per volume"
        );
    }

    pub(super) fn take_recovery(&mut self) -> Option<crate::protocol::Verdict> {
        self.recovery.take()
    }
}

pub struct PendingSync {
    id: u64,
    pub barrier: u64,
    reply: Reply<bool>,
    resolved: Option<UnboundedSender<()>>,
}

impl PendingSync {
    pub fn new(id: u64, barrier: u64, reply: Reply<bool>, resolved: UnboundedSender<()>) -> Self {
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

pub struct VolumeState {
    pub run_generation: u64,
    pub config: VolumeConfig,
    pub fence: u64,
    pub ready: bool,
    pub epoch: Epoch,
    pub mutation_seq: u64,
    pub pages_dirtied_total: u64,
    /// Newest visible value checksum per block and their assembled checksum.
    /// These describe logical state, not how values are split among files.
    pub block_checksums: BTreeMap<crate::blx::BlockKey, (Gen, u64)>,
    pub state_checksum: u64,
    /// Deletion markers that must be written before a later journal can
    /// retain a mixed batch containing values discarded by cold boot.
    pub pending_tombstones: BTreeSet<crate::blx::BlockKey>,
    pub page_locs: BTreeMap<PageId, (Gen, PageFileLoc)>,
    pub next_gen: u64,
    pub next_seq: u64,
    pub next_object_id: u64,
    pub best_record: Option<JournalRecord>,
    pub local_covered_through: u64,
    pub sync_ack_through: u64,
    pub pending_syncs: Vec<PendingSync>,
    pub operations: VolumeOperationState,
    pub mutation_waiters: Vec<OneSender<()>>,
    pub checkpoint_results: BTreeMap<crate::protocol::ReqId, Epoch>,
    pub pinned: Option<JournalRecord>,
    pub record_writes: BTreeMap<JournalSeq, (u64, u64)>,
    /// Every local BLX file written as part of a journal record. This keeps
    /// files that contain only tombstones reachable even though they have no
    /// live page location.
    pub record_blx_files: BTreeMap<JournalSeq, BTreeSet<ObjectIdentity>>,
    pub blx_blobs: Vec<(ObjectIdentity, u64)>,
    pub blx_refs: BTreeMap<ObjectIdentity, ObjectRef>,
    /// The complete local BLX batch that carries the newest resumable VMM
    /// snapshot. It remains in every later recovery closure until replaced by
    /// another checkpoint.
    pub vmm_blx_files: BTreeSet<ObjectIdentity>,
    pub head_version: Option<u64>,
    pub backed: Option<ManifestPtr>,
    pub archive_base: Option<BaseRef>,
    /// Archive metadata is installed at attach time without reading data-file
    /// footers. A footer is fetched and cached only when a block in that
    /// file's key range is first requested.
    pub archive_objects: Vec<ObjectRef>,
    pub archive_footers: BTreeMap<ObjectIdentity, BlxFooter>,
    pub archive_resolved_pages: BTreeSet<PageId>,
    /// A running VM may refault archived memory, but a VM recovered by cold
    /// boot must never see memory from before that boot.
    pub archived_memory_usable: bool,
    /// Whether the pre-cold-boot memory and VMM checksum contributions have
    /// been removed from the running logical state.
    pub archived_non_data_reset: bool,
    /// Local BLX files containing deletion markers remain part of the
    /// unpublished change set until the archive has published them.
    pub tombstone_blx_files: BTreeSet<ObjectIdentity>,
    pub backed_blx_files: BTreeSet<ObjectIdentity>,
    /// Local files held stable while the primary is sending one exact cut to
    /// its passive. Captures may continue concurrently, so the newest record
    /// alone is not enough to protect this older closure from cleanup.
    pub replicating_blx_files: BTreeSet<ObjectIdentity>,
    pub publishing_blx_files: BTreeSet<ObjectIdentity>,
    pub store_manifests: BTreeSet<(u64, JournalSeq)>,
    pub outbound: Option<HostId>,
    pub peer_source: Option<HostId>,
    pub peer_source_offer_fence: Option<u64>,
    pub hydration_waiters: Vec<OneSender<bool>>,
    pub publication_waiters: Vec<OneSender<()>>,
    pub stash_assignment: Option<crate::head::StashAssignment>,
    pub retired_stashes: Vec<crate::head::RetiredStash>,
    pub peer_committed: Option<crate::protocol::ReplicaCommitInfo>,
    pub peer_committed_record: Option<JournalRecord>,
    pub peer_published: Option<crate::protocol::ReplicaCommitInfo>,
    pub peer_committed_through: u64,
    pub wedge: WedgeState,
}

impl VolumeState {
    pub(crate) fn fresh(config: VolumeConfig, run_generation: u64) -> Self {
        Self {
            run_generation,
            config,
            fence: 1,
            ready: false,
            epoch: Epoch(0),
            mutation_seq: 0,
            pages_dirtied_total: 0,
            block_checksums: BTreeMap::new(),
            state_checksum: 0,
            pending_tombstones: BTreeSet::new(),
            page_locs: BTreeMap::new(),
            next_gen: 0,
            next_seq: 0,
            next_object_id: 0,
            best_record: None,
            local_covered_through: 0,
            sync_ack_through: 0,
            pending_syncs: Vec::new(),
            operations: VolumeOperationState::default(),
            mutation_waiters: Vec::new(),
            checkpoint_results: BTreeMap::new(),
            pinned: None,
            record_writes: BTreeMap::new(),
            record_blx_files: BTreeMap::new(),
            blx_blobs: Vec::new(),
            blx_refs: BTreeMap::new(),
            vmm_blx_files: BTreeSet::new(),
            head_version: None,
            backed: None,
            archive_base: None,
            archive_objects: Vec::new(),
            archive_footers: BTreeMap::new(),
            archive_resolved_pages: BTreeSet::new(),
            archived_memory_usable: true,
            archived_non_data_reset: true,
            tombstone_blx_files: BTreeSet::new(),
            backed_blx_files: BTreeSet::new(),
            replicating_blx_files: BTreeSet::new(),
            publishing_blx_files: BTreeSet::new(),
            store_manifests: BTreeSet::new(),
            outbound: None,
            peer_source: None,
            peer_source_offer_fence: None,
            hydration_waiters: Vec::new(),
            publication_waiters: Vec::new(),
            stash_assignment: None,
            retired_stashes: Vec::new(),
            peer_committed: None,
            peer_committed_record: None,
            peer_published: None,
            peer_committed_through: 0,
            wedge: WedgeState::default(),
        }
    }

    pub(super) fn install_archive_closure(
        &mut self,
        volume: VolumeId,
        objects: &[ObjectRef],
        base: Option<BaseRef>,
    ) {
        self.archive_objects = objects.to_vec();
        self.archive_base = base;
        self.archive_footers.clear();
        self.archive_resolved_pages.clear();
        self.backed_blx_files = objects
            .iter()
            .filter(|object| {
                object.identity.namespace_kind == NamespaceKind::Volume
                    && object.identity.namespace_id == volume.0
            })
            .map(|object| object.identity)
            .collect();
    }

    pub(super) fn retention_closure(
        &self,
    ) -> (BTreeSet<(u64, JournalSeq)>, BTreeSet<ObjectIdentity>) {
        let mut records = BTreeSet::new();
        let mut blx_files = BTreeSet::new();
        for record in [
            self.best_record.as_ref(),
            self.pinned.as_ref(),
            self.peer_committed_record.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            records.insert((record.fence, record.seq));
            blx_files.extend(record.files.iter().filter_map(|file| {
                (file.identity.namespace_kind == NamespaceKind::Volume).then_some(file.identity)
            }));
            if let Some(written) = self.record_blx_files.get(&record.seq) {
                blx_files.extend(written);
            }
            blx_files.extend(
                record
                    .runtime_page_index
                    .iter()
                    .filter(|(_, (_, location))| location.base == 0)
                    .map(|(page, (_, location))| location.identity(page.volume)),
            );
        }
        (records, blx_files)
    }

    pub(super) fn adopt_assignment(
        &mut self,
        version: u64,
        assignment: StashAssignment,
        retired_stashes: Vec<RetiredStash>,
    ) {
        if self.stash_assignment != Some(assignment) {
            // Exact commit information belongs to the peer/assignment that
            // acknowledged it. The primary may archive only a cut acknowledged
            // by the current assignment.
            self.peer_committed = None;
            self.peer_committed_record = None;
        }
        self.head_version = Some(version);
        self.stash_assignment = Some(assignment);
        self.retired_stashes = retired_stashes;
    }

    pub(super) fn adopt_assignment_from_head(&mut self, version: u64, head: HeadRecord) {
        if self.stash_assignment != head.stash {
            self.peer_committed = None;
            self.peer_committed_record = None;
        }
        self.head_version = Some(version);
        self.backed = head.manifest;
        self.stash_assignment = head.stash;
        self.retired_stashes = head.retired_stashes;
    }

    fn live_blx_bytes(&self) -> u64 {
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
            .files
            .iter()
            .filter(|file| file.identity.namespace_kind == NamespaceKind::Volume)
            .map(|file| file.identity)
            .filter(|identity| !self.backed_blx_files.contains(identity))
            .collect::<BTreeSet<_>>();
        self.blx_blobs
            .iter()
            .filter(|(identity, _)| pending.contains(identity))
            .map(|(_, bytes)| *bytes)
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
    volume: VolumeId,
    run_generation: u64,
    owner: MutationOwner,
    cleanup: Option<OneSender<Vec<PageId>>>,
    cleanup_finishes_mutation: bool,
    active: bool,
}

impl CaptureLease {
    pub fn new(
        state: &SharedHost,
        volume: VolumeId,
        run_generation: u64,
        owner: MutationOwner,
        cleanup: OneSender<Vec<PageId>>,
    ) -> Self {
        Self {
            state: Rc::clone(state),
            volume,
            run_generation,
            owner,
            cleanup: Some(cleanup),
            cleanup_finishes_mutation: false,
            active: true,
        }
    }

    pub fn new_with_serialized_cleanup(
        state: &SharedHost,
        volume: VolumeId,
        run_generation: u64,
        owner: MutationOwner,
        cleanup: OneSender<Vec<PageId>>,
    ) -> Self {
        Self {
            state: Rc::clone(state),
            volume,
            run_generation,
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
        self.state.borrow_mut().schedule_volume(self.volume);
        self.active = false;
    }
}

impl Drop for CaptureLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut host = self.state.borrow_mut();
        let Some(volume) = host
            .volumes
            .get_mut(&self.volume)
            .filter(|volume| volume.run_generation == self.run_generation)
        else {
            drop(host);
            if let Some(cleanup) = self.cleanup.take() {
                let _ = cleanup.send(Vec::new());
            }
            return;
        };
        let armed = if self.cleanup_finishes_mutation {
            volume
                .operations
                .drain_mut()
                .map(|drain| std::mem::take(&mut drain.armed))
                .unwrap_or_default()
        } else {
            volume
                .operations
                .finish_mutation(self.owner)
                .map(|drain| drain.armed)
                .unwrap_or_default()
        };
        let waiters = if self.cleanup_finishes_mutation {
            Vec::new()
        } else {
            std::mem::take(&mut volume.mutation_waiters)
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
            self.state.borrow_mut().schedule_volume(self.volume);
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
    use super::{CaptureKind, MutationOwner, VolumeOperationState};

    #[test]
    fn typed_operation_slots_reject_overlapping_owners() {
        let mut operations = VolumeOperationState::default();

        assert!(operations.try_start_mutation(MutationOwner::Capture(CaptureKind::Writeback)));
        assert!(!operations.try_start_mutation(MutationOwner::Hydration));
        assert_eq!(
            operations.mutation_owner(),
            Some(MutationOwner::Capture(CaptureKind::Writeback))
        );
        assert!(
            operations
                .finish_mutation(MutationOwner::Hydration)
                .is_none()
        );
        assert_eq!(
            operations.mutation_owner(),
            Some(MutationOwner::Capture(CaptureKind::Writeback))
        );
        operations.finish_mutation(MutationOwner::Capture(CaptureKind::Writeback));
        assert!(operations.try_start_mutation(MutationOwner::Hydration));
        operations.finish_mutation(MutationOwner::Hydration);

        assert!(operations.start_migration());
        assert!(!operations.start_migration());
        assert!(!operations.try_start_mutation(MutationOwner::Capture(CaptureKind::Writeback)));
        assert!(operations.try_start_mutation(MutationOwner::Capture(CaptureKind::Migration)));
        operations.finish_mutation(MutationOwner::Capture(CaptureKind::Migration));
        operations.finish_migration();
        assert!(!operations.migration_running());

        assert!(operations.start_guest_resume());
        assert!(operations.guest_resume_pending());
        assert!(!operations.try_start_mutation(MutationOwner::Capture(CaptureKind::Writeback)));
        assert!(!operations.start_migration());
        operations.finish_guest_resume();
        assert!(operations.try_start_mutation(MutationOwner::Capture(CaptureKind::Writeback)));
        operations.finish_mutation(MutationOwner::Capture(CaptureKind::Writeback));

        assert!(operations.try_start_publication());
        assert!(!operations.try_start_publication());
        assert!(operations.publication_running());
        operations.finish_publication();
        assert!(!operations.publication_running());
    }
}
