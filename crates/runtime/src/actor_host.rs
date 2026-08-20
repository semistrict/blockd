//! Production host for the shared async protocol actors.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use blockd_core::authority::HostSessionRecord;
use blockd_core::engine::{HostFatal, HostState, host_actor_with_state};
use blockd_core::hostmeta::{
    ClusterPlacementConfig, Counters, DaemonStats, HostConfig, ReplicaSpoolMetrics,
    ReplicaVolumeMetrics, VolumeOperations,
};
use blockd_core::journal::{JournalRecord, VolumeConfig, VolumeKind};
use blockd_core::layout::BlobName;
use blockd_core::placement::ClusterPlacement;
use blockd_core::protocol::{
    AdminCall, AdminError, AdminEvent, AdminResult, AdminSuccess, PeerMsg, ReqId, Verdict,
};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId};
use blockd_core::world::{
    AdminIo, AdminRequest, BlobEntry, BlobError, Blobs, FillSource, GuestFault, GuestMem,
    GuestMemoryError, GuestPause, GuestSync, GuestSyncRequest, Peers, Store, StoreError,
};
use blockd_exec::inject::{Injected, Injector, Lane, bounded_injector, injector};
use blockd_exec::{ProductionContext, delay, request};
use blockd_hostmem::{GuestView, HostRegion, Uffd, UffdFeatures, page_size};
use tokio::io::unix::AsyncFd;
use tracing::Instrument as _;

use crate::loopstats::{LoopStats, world_kind};
use crate::metrics::{
    AtomicHistogram, FaultLatency, FaultReaderMetrics, FaultWorkMetrics, LatencySeries,
    TimingSeries, detailed_profile_metrics_enabled,
};
use crate::peer::{PeerConfig, PeerNet, PeerResourceMetrics};
use crate::store::ObjectStore;
use crate::world::{FileBlobs, RuntimeStore};
use crate::{CapacityController, CapacityInputs, CapacitySignal};

pub struct RuntimeConfig {
    pub daemon: HostConfig,
    pub cluster_id: Option<u64>,
    pub blob_dir: PathBuf,
    pub peer: Option<PeerConfig>,
}

#[derive(Debug)]
pub enum RuntimeStartupError {
    LocalDiscovery(String),
    BlobDirectory(std::io::Error),
    PeerListener(std::io::Error),
    PeerMembership(blockd_core::protocol::StoreFault),
    Placement(blockd_core::protocol::StoreFault),
    ThreadSpawn {
        thread: &'static str,
        source: std::io::Error,
    },
}

impl std::fmt::Display for RuntimeStartupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalDiscovery(error) => {
                write!(formatter, "local volume discovery failed: {error}")
            }
            Self::BlobDirectory(error) => {
                write!(formatter, "secure blob directory failed: {error}")
            }
            Self::PeerListener(error) => write!(formatter, "peer listener startup failed: {error}"),
            Self::PeerMembership(error) => {
                write!(
                    formatter,
                    "initial peer membership publication failed: {error:?}"
                )
            }
            Self::Placement(error) => {
                write!(formatter, "initial cluster placement failed: {error:?}")
            }
            Self::ThreadSpawn { thread, source } => {
                write!(formatter, "{thread} thread startup failed: {source}")
            }
        }
    }
}

impl std::error::Error for RuntimeStartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BlobDirectory(error)
            | Self::PeerListener(error)
            | Self::ThreadSpawn { source: error, .. } => Some(error),
            Self::LocalDiscovery(_) | Self::PeerMembership(_) | Self::Placement(_) => None,
        }
    }
}

#[cfg(test)]
fn live_membership_epoch(members: &[HostId]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for member in members {
        for byte in member.get().to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash.max(1)
}

struct LivePlacementStatus {
    state: Mutex<LivePlacementState>,
}

struct LivePlacementState {
    enabled: bool,
    desired: Vec<HostId>,
    applied: Option<(Vec<HostId>, u64)>,
}

impl LivePlacementStatus {
    fn new(enabled: bool) -> Self {
        Self {
            state: Mutex::new(LivePlacementState {
                enabled,
                desired: Vec::new(),
                applied: None,
            }),
        }
    }

    fn publish(&self, members: Vec<HostId>) {
        self.state.lock().expect("placement status lock").desired = members;
    }

    fn is_current(&self, members: &[HostId]) -> bool {
        self.state.lock().expect("placement status lock").desired == members
    }

    fn complete_if_current(
        &self,
        members: &[HostId],
        epoch: u64,
        publish: impl FnOnce() -> bool,
    ) -> bool {
        let mut state = self.state.lock().expect("placement status lock");
        if state.desired != members || !publish() {
            return false;
        }
        state.applied = Some((members.to_vec(), epoch));
        true
    }

    fn readiness(&self) -> (bool, u64) {
        let state = self.state.lock().expect("placement status lock");
        if !state.enabled {
            return (true, 0);
        }
        match &state.applied {
            Some((members, epoch)) if *members == state.desired => (true, *epoch),
            Some((_, epoch)) => (false, *epoch),
            None => (false, 0),
        }
    }
}

fn live_roster_includes_local(members: &[HostId], local: HostId) -> bool {
    members.len() >= blockd_core::placement::MIN_PLACEMENT_MEMBERS && members.contains(&local)
}

fn cluster_placement_config(
    placement: &ClusterPlacement,
    authority: Option<blockd_core::hostmeta::AuthorityHostConfig>,
) -> ClusterPlacementConfig {
    ClusterPlacementConfig {
        membership_epoch: placement.epoch,
        roster: placement.roster.clone(),
        authority,
    }
}

#[cfg(test)]
async fn reconcile_cluster_placement(
    store: Arc<dyn ObjectStore>,
    cluster_id: u64,
    members: Vec<HostId>,
) -> Result<ClusterPlacement, blockd_core::protocol::StoreFault> {
    reconcile_cluster_placement_if_current(store, cluster_id, members, || true)
        .await?
        .ok_or(blockd_core::protocol::StoreFault::Unavailable)
}

async fn reconcile_cluster_placement_if_current(
    store: Arc<dyn ObjectStore>,
    cluster_id: u64,
    members: Vec<HostId>,
    is_current: impl Fn() -> bool,
) -> Result<Option<ClusterPlacement>, blockd_core::protocol::StoreFault> {
    let key = blockd_core::layout::placement_key();
    for _ in 0..16 {
        let found = Arc::clone(&store).get(key.clone()).await?;
        if !is_current() {
            return Ok(None);
        }
        let (generation, existing) = match found {
            Some((generation, bytes)) => {
                let placement = ClusterPlacement::decode(&bytes)
                    .filter(|placement| placement.cluster_id == cluster_id)
                    .ok_or(blockd_core::protocol::StoreFault::Unavailable)?;
                (Some(generation), Some(placement))
            }
            None => (None, None),
        };
        let epoch = match existing.as_ref() {
            Some(placement) => placement
                .epoch
                .checked_add(1)
                .ok_or(blockd_core::protocol::StoreFault::Unavailable)?,
            None => 1,
        };
        let desired = ClusterPlacement::from_members(cluster_id, epoch, members.clone());
        if let Some(existing) = existing
            && existing.roster == desired.roster
        {
            return Ok(Some(existing));
        }
        if !is_current() {
            return Ok(None);
        }
        match Arc::clone(&store)
            .put_cas(key.clone(), generation, desired.encode())
            .await
        {
            Ok(_) => return Ok(Some(desired)),
            Err(blockd_core::protocol::StoreFault::CasConflict { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Err(blockd_core::protocol::StoreFault::Unavailable)
}

struct PlacementOwner {
    store: Arc<dyn ObjectStore>,
    cluster_id: u64,
    authority: Option<blockd_core::hostmeta::AuthorityHostConfig>,
    local: HostId,
    status: Arc<LivePlacementStatus>,
    rosters: tokio::sync::watch::Receiver<Vec<HostId>>,
    admin: Injector<AdminRequest>,
    startup: tokio::sync::oneshot::Sender<Result<ClusterPlacementConfig, RuntimeStartupError>>,
}

impl PlacementOwner {
    async fn run(self) {
        let Self {
            store,
            cluster_id,
            authority,
            local,
            status,
            mut rosters,
            admin,
            startup,
        } = self;
        let mut startup = Some(startup);
        loop {
            let members = rosters.borrow_and_update().clone();
            let mut settled = !live_roster_includes_local(&members, local);
            if live_roster_includes_local(&members, local) {
                let reconciled = reconcile_cluster_placement_if_current(
                    Arc::clone(&store),
                    cluster_id,
                    members.clone(),
                    || status.is_current(&members),
                )
                .await;
                match reconciled {
                    Ok(Some(reconciled)) => {
                        let placement = cluster_placement_config(&reconciled, authority);
                        let published =
                            status.complete_if_current(&members, reconciled.epoch, || {
                                if startup.is_some() {
                                    true
                                } else {
                                    let (update, _reply) =
                                        request(AdminCall::UpdateClusterPlacement {
                                            placement: placement.clone(),
                                        });
                                    admin.push(Lane::Background, update).is_ok()
                                }
                            });
                        if published && let Some(ready) = startup.take() {
                            let _ = ready.send(Ok(placement));
                        }
                        if published {
                            settled = true;
                        } else if startup.is_none() && status.is_current(&members) {
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if let Some(ready) = startup.take() {
                            let _ = ready.send(Err(RuntimeStartupError::Placement(error)));
                            return;
                        }
                        tracing::warn!(?error, "cluster placement reconciliation deferred");
                    }
                }
            }
            if status.is_current(&members) {
                if settled {
                    if rosters.changed().await.is_err() {
                        return;
                    }
                } else {
                    tokio::select! {
                        changed = rosters.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                        () = tokio::time::sleep(Duration::from_millis(100)) => {}
                    }
                }
            }
        }
    }
}

fn assert_peer_stash_transport(config: VolumeConfig, authenticated: bool) {
    let _ = config;
    assert!(
        authenticated,
        "passive durability requires mutually authenticated TLS"
    );
}

struct VolumeHost {
    volume: VolumeId,
    config: VolumeConfig,
    region: Arc<HostRegion>,
    view: Arc<GuestView>,
    uffd: Option<Arc<Uffd>>,
    ctl: GuestCtl,
    fault_latency: [AtomicHistogram; FaultSource::COUNT],
}

struct SharedUffd(Arc<Uffd>);

impl AsRawFd for SharedUffd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

#[derive(Default)]
struct GuestCtl {
    state: Mutex<CtlState>,
    ready: tokio::sync::Notify,
}

#[derive(Default)]
struct CtlState {
    pause_requested: bool,
    paused: bool,
    pause_generation: u64,
    in_op: bool,
    applied: u64,
    pause_waiter: Option<Injector<u64>>,
}

impl VolumeHost {
    fn new(volume: VolumeId, config: VolumeConfig) -> Arc<Self> {
        let pages = usize::try_from(config.pages).expect("page count fits");
        let region = Arc::new(HostRegion::new(pages).expect("guest region"));
        let view = Arc::new(GuestView::map(&region, 0, pages).expect("guest view"));
        let uffd = Some({
            let (uffd, features) = Uffd::new(
                UffdFeatures::PAGEFAULT_FLAG_WP
                    | UffdFeatures::MINOR_SHMEM
                    | UffdFeatures::WP_HUGETLBFS_SHMEM,
            )
            .expect("userfaultfd");
            assert!(
                features.has(UffdFeatures::MINOR_SHMEM)
                    && features.has(UffdFeatures::WP_HUGETLBFS_SHMEM),
                "kernel lacks required userfaultfd features: {features:?}"
            );
            uffd.register_all(&view).expect("register guest view");
            Arc::new(uffd)
        });
        Arc::new(Self {
            volume,
            config,
            region,
            view,
            uffd,
            ctl: GuestCtl::default(),
            fault_latency: std::array::from_fn(|_| AtomicHistogram::default()),
        })
    }

    fn page_index(page: PageId) -> usize {
        usize::try_from(page.page.0).expect("page index fits")
    }

    fn page_of_addr(&self, addr: usize) -> PageId {
        let index = (addr - self.view.addr_of(0)) / page_size();
        PageId {
            volume: self.volume,
            page: PageNo(u32::try_from(index).expect("page number fits")),
        }
    }

    async fn op_start(&self) {
        loop {
            let notified = self.ctl.ready.notified();
            {
                let mut state = self.ctl.state.lock().expect("guest control lock");
                if !state.pause_requested && !state.paused && !state.in_op {
                    state.in_op = true;
                    return;
                }
            }
            notified.await;
        }
    }

    fn op_end(&self) {
        let mut state = self.ctl.state.lock().expect("guest control lock");
        state.in_op = false;
        state.applied = state.applied.saturating_add(1);
        if state.pause_requested && !state.paused {
            state.paused = true;
            let applied = state.applied;
            if let Some(waiter) = state.pause_waiter.take() {
                let _ = waiter.push(Lane::Critical, applied);
            }
        }
        drop(state);
        self.ctl.ready.notify_waiters();
    }
}

#[derive(Clone, Copy)]
enum FaultSource {
    Zero,
    Shared,
    WriteProtect,
    Local,
    Peer,
    Store,
    Unservable,
}

impl FaultSource {
    const COUNT: usize = 7;
    const ALL: [Self; Self::COUNT] = [
        Self::Zero,
        Self::Shared,
        Self::WriteProtect,
        Self::Local,
        Self::Peer,
        Self::Store,
        Self::Unservable,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Shared => "shared",
            Self::WriteProtect => "write_protect",
            Self::Local => "local_nvme",
            Self::Peer => "peer",
            Self::Store => "object_store",
            Self::Unservable => "unservable",
        }
    }
}

impl From<FillSource> for FaultSource {
    fn from(source: FillSource) -> Self {
        match source {
            FillSource::Zero => Self::Zero,
            FillSource::Local => Self::Local,
            FillSource::Peer => Self::Peer,
            FillSource::Store => Self::Store,
        }
    }
}

struct FaultInFlight {
    started: Instant,
    span: tracing::Span,
}

const OPERATION_NAMES: [&str; 6] = [
    "create",
    "checkpoint",
    "restore",
    "migration",
    "sync",
    "fork",
];
const OPERATION_OUTCOMES: [&str; 2] = ["success", "failed"];
const LOCAL_IO_NAMES: [&str; 4] = ["write", "read", "ranged_read", "delete"];
const LOCAL_IO_OUTCOMES: [&str; 2] = ["success", "missing"];
const PAUSE_NAMES: [&str; 2] = ["checkpoint", "migration"];
const FAULT_WORK_NAMES: [&str; 5] = ["fill", "unprotect", "evict", "write_protect", "barrier"];
const FAULT_WORK_PHASES: [&str; 3] = ["queue_wait", "blocking_dispatch", "service"];

struct FaultWorkStats {
    detailed: bool,
    queue: Mutex<VecDeque<(usize, Instant)>>,
    max_queue_depth: AtomicU64,
    active: AtomicU64,
    max_active: AtomicU64,
    join_failures: AtomicU64,
    timing: [[AtomicHistogram; FAULT_WORK_PHASES.len()]; FAULT_WORK_NAMES.len()],
}

impl Default for FaultWorkStats {
    fn default() -> Self {
        Self {
            detailed: detailed_profile_metrics_enabled(),
            queue: Mutex::new(VecDeque::new()),
            max_queue_depth: AtomicU64::new(0),
            active: AtomicU64::new(0),
            max_active: AtomicU64::new(0),
            join_failures: AtomicU64::new(0),
            timing: std::array::from_fn(|_| std::array::from_fn(|_| AtomicHistogram::default())),
        }
    }
}

impl FaultWorkStats {
    fn enqueue(&self, operation: usize) {
        if !self.detailed {
            return;
        }
        let depth = {
            let mut queue = self.queue.lock().expect("fault work metric lock");
            queue.push_back((operation, Instant::now()));
            u64::try_from(queue.len()).unwrap_or(u64::MAX)
        };
        self.max_queue_depth.fetch_max(depth, Ordering::Relaxed);
    }

    fn rollback_enqueue(&self, operation: usize) {
        if !self.detailed {
            return;
        }
        let queued = self
            .queue
            .lock()
            .expect("fault work metric lock")
            .pop_back();
        debug_assert_eq!(queued.map(|(found, _)| found), Some(operation));
    }

    fn dequeue(&self, operation: usize) -> Option<Instant> {
        if !self.detailed {
            return None;
        }
        let queued = self
            .queue
            .lock()
            .expect("fault work metric lock")
            .pop_front()
            .expect("received fault work was measured at enqueue");
        debug_assert_eq!(queued.0, operation);
        Some(queued.1)
    }

    fn observe(&self, operation: usize, phase: usize, elapsed: Duration) {
        if self.detailed {
            self.timing[operation][phase].observe(elapsed);
        }
    }

    fn start(&self) {
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_active.fetch_max(active, Ordering::Relaxed);
    }

    fn complete(&self) {
        let previous = self.active.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "completed fault work was not active");
    }

    fn snapshot(&self) -> FaultWorkMetrics {
        let (queue_depth, oldest_queued_ns) = {
            let queue = self.queue.lock().expect("fault work metric lock");
            (
                u64::try_from(queue.len()).unwrap_or(u64::MAX),
                queue
                    .front()
                    .map_or(0, |(_, enqueued)| elapsed_ns(enqueued.elapsed())),
            )
        };
        let mut timing = Vec::new();
        for (operation, operation_name) in FAULT_WORK_NAMES.iter().enumerate() {
            for (phase, phase_name) in FAULT_WORK_PHASES.iter().enumerate() {
                timing.push(TimingSeries {
                    operation: operation_name,
                    phase: phase_name,
                    histogram: self.timing[operation][phase].snapshot(),
                });
            }
        }
        FaultWorkMetrics {
            queue_depth,
            max_queue_depth: self.max_queue_depth.load(Ordering::Relaxed),
            oldest_queued_ns,
            active: self.active.load(Ordering::Relaxed),
            max_active: self.max_active.load(Ordering::Relaxed),
            join_failures: self.join_failures.load(Ordering::Relaxed),
            timing,
        }
    }
}

struct Shared {
    volumes: Mutex<BTreeMap<VolumeId, Arc<VolumeHost>>>,
    admin_events: Mutex<VecDeque<AdminEvent>>,
    admin_event_ready: tokio::sync::Notify,
    released_volumes: Mutex<BTreeSet<VolumeId>>,
    volume_released: tokio::sync::Notify,
    incidents: Mutex<Vec<String>>,
    quarantines: Mutex<BTreeMap<VolumeId, String>>,
    quarantine_cleanup: tokio::sync::Mutex<()>,
    authority_identity: Mutex<Option<(u64, u64)>>,
    planned_retirement: Mutex<Option<HostSessionRecord>>,
    planned_retirement_verified: AtomicBool,
    authority_placement_epoch: AtomicU64,
    counters: Mutex<Counters>,
    daemon_stats: Mutex<DaemonStats>,
    replica_metrics: Mutex<Vec<ReplicaVolumeMetrics>>,
    replica_spool_metrics: Mutex<Vec<ReplicaSpoolMetrics>>,
    replica_spool_capacity_bytes: u64,
    capacity: Mutex<CapacityController>,
    stats: LoopStats,
    fault_in_flight: Mutex<BTreeMap<PageId, VecDeque<FaultInFlight>>>,
    fault_reader: FaultReaderStats,
    operation_latency: [[AtomicHistogram; OPERATION_OUTCOMES.len()]; OPERATION_NAMES.len()],
    local_io_latency: [[AtomicHistogram; LOCAL_IO_OUTCOMES.len()]; LOCAL_IO_NAMES.len()],
    local_io_in_flight: [AtomicU64; LOCAL_IO_NAMES.len()],
    pause_expected: Mutex<BTreeMap<VolumeId, VecDeque<usize>>>,
    pause_in_flight: Mutex<BTreeMap<VolumeId, (usize, Instant)>>,
    pause_latency: [AtomicHistogram; PAUSE_NAMES.len()],
    fault_work_stats: FaultWorkStats,
    backup_lag_started: Mutex<BTreeMap<VolumeId, Instant>>,
    operation_started: Mutex<BTreeMap<(VolumeId, u8), Instant>>,
    next_req: AtomicU64,
    cluster_placement_epoch: AtomicU64,
    recovery_complete: AtomicBool,
    critical_healthy: AtomicBool,
    critical_failed: tokio::sync::Notify,
    fault_readers: Mutex<BTreeMap<VolumeId, FaultReaderTask>>,
    #[cfg(test)]
    fault_reader_start: Mutex<Option<TestFaultReaderStart>>,
}

struct CriticalThreadGuard {
    shared: Arc<Shared>,
    expected_stop: Arc<AtomicBool>,
    name: &'static str,
}

impl Drop for CriticalThreadGuard {
    fn drop(&mut self) {
        if !self.expected_stop.load(Ordering::SeqCst) {
            self.shared
                .fail_critical(format!("{} stopped unexpectedly", self.name));
        }
    }
}

impl Shared {
    fn new(volumes: BTreeMap<VolumeId, Arc<VolumeHost>>, config: &HostConfig) -> Self {
        let state = HostState::new(config.clone());
        Self {
            volumes: Mutex::new(volumes),
            admin_events: Mutex::new(VecDeque::new()),
            admin_event_ready: tokio::sync::Notify::new(),
            released_volumes: Mutex::new(BTreeSet::new()),
            volume_released: tokio::sync::Notify::new(),
            incidents: Mutex::new(Vec::new()),
            quarantines: Mutex::new(BTreeMap::new()),
            quarantine_cleanup: tokio::sync::Mutex::new(()),
            authority_identity: Mutex::new(None),
            planned_retirement: Mutex::new(None),
            planned_retirement_verified: AtomicBool::new(false),
            authority_placement_epoch: AtomicU64::new(0),
            counters: Mutex::new(Counters::default()),
            daemon_stats: Mutex::new(state.stats()),
            replica_metrics: Mutex::new(Vec::new()),
            replica_spool_metrics: Mutex::new(Vec::new()),
            replica_spool_capacity_bytes: config.archive.spool_capacity_bytes,
            capacity: Mutex::new(CapacityController::default()),
            stats: LoopStats::default(),
            fault_in_flight: Mutex::new(BTreeMap::new()),
            fault_reader: FaultReaderStats::default(),
            operation_latency: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicHistogram::default())
            }),
            local_io_latency: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicHistogram::default())
            }),
            local_io_in_flight: std::array::from_fn(|_| AtomicU64::new(0)),
            pause_expected: Mutex::new(BTreeMap::new()),
            pause_in_flight: Mutex::new(BTreeMap::new()),
            pause_latency: std::array::from_fn(|_| AtomicHistogram::default()),
            fault_work_stats: FaultWorkStats::default(),
            backup_lag_started: Mutex::new(BTreeMap::new()),
            operation_started: Mutex::new(BTreeMap::new()),
            next_req: AtomicU64::new(1),
            cluster_placement_epoch: AtomicU64::new(
                config
                    .cluster_placement
                    .as_ref()
                    .map_or(0, |placement| placement.membership_epoch),
            ),
            recovery_complete: AtomicBool::new(false),
            critical_healthy: AtomicBool::new(true),
            critical_failed: tokio::sync::Notify::new(),
            fault_readers: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            fault_reader_start: Mutex::new(None),
        }
    }

    fn fail_critical(&self, reason: String) {
        self.incidents.lock().expect("incident lock").push(reason);
        self.critical_healthy.store(false, Ordering::SeqCst);
        self.critical_failed.notify_waiters();
    }
}

#[derive(Default)]
struct FaultReaderStats {
    readers_started: AtomicU64,
    readers_exited: AtomicU64,
    events_read: AtomicU64,
    events_injected: AtomicU64,
    terminal_errors: AtomicU64,
    injection_failures: AtomicU64,
}

struct ActiveFaultReader {
    shared: Arc<Shared>,
    expected_stop: Arc<AtomicBool>,
}

impl Drop for ActiveFaultReader {
    fn drop(&mut self) {
        self.shared
            .fault_reader
            .readers_exited
            .fetch_add(1, Ordering::Relaxed);
        if !self.expected_stop.load(Ordering::SeqCst) {
            self.shared
                .fail_critical("fault reader stopped unexpectedly".to_owned());
        }
    }
}

impl FaultReaderStats {
    fn snapshot(&self) -> FaultReaderMetrics {
        FaultReaderMetrics {
            readers_started: self.readers_started.load(Ordering::Relaxed),
            readers_exited: self.readers_exited.load(Ordering::Relaxed),
            events_read: self.events_read.load(Ordering::Relaxed),
            events_injected: self.events_injected.load(Ordering::Relaxed),
            terminal_errors: self.terminal_errors.load(Ordering::Relaxed),
            injection_failures: self.injection_failures.load(Ordering::Relaxed),
        }
    }
}

struct PendingGuestPause {
    shared: Arc<Shared>,
    host: Arc<VolumeHost>,
    volume: VolumeId,
    generation: u64,
    active: bool,
}

impl PendingGuestPause {
    fn finish(mut self) {
        self.active = false;
    }
}

impl Drop for PendingGuestPause {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.host.ctl.state.lock().expect("guest control lock");
        if state.pause_generation != self.generation {
            return;
        }
        state.pause_requested = false;
        state.paused = false;
        state.pause_waiter = None;
        drop(state);
        self.shared.complete_pause(self.volume);
        self.host.ctl.ready.notify_waiters();
    }
}

enum FaultWork {
    Fill {
        host: Arc<VolumeHost>,
        page: PageId,
        bytes: Option<Vec<u8>>,
        writable: bool,
        reply: Injector<Result<(), ()>>,
    },
    Unprotect {
        host: Arc<VolumeHost>,
        page: PageId,
        reply: Injector<Result<(), ()>>,
    },
    Evict {
        host: Arc<VolumeHost>,
        page: PageId,
        reply: Injector<Result<(), ()>>,
    },
    WriteProtect {
        hosts: Vec<(VolumeId, Arc<VolumeHost>, Vec<usize>)>,
        reply: Injector<Result<(), ()>>,
    },
    Barrier {
        done: tokio::sync::oneshot::Sender<()>,
    },
    #[cfg(test)]
    CrashDispatcher,
    #[cfg(test)]
    Test {
        volume: VolumeId,
        entered: std::sync::mpsc::Sender<VolumeId>,
        release: Arc<TestFaultWorkGate>,
        result: Result<(), ()>,
        panics: bool,
        reply: Injector<Result<(), ()>>,
    },
}

const FAULT_WORK_CONCURRENCY: usize = 8;

enum BlockingFaultWork {
    Fill {
        host: Arc<VolumeHost>,
        page: PageId,
        bytes: Option<Vec<u8>>,
        writable: bool,
    },
    Unprotect {
        host: Arc<VolumeHost>,
        page: PageId,
    },
    Evict {
        host: Arc<VolumeHost>,
        page: PageId,
    },
    WriteProtect {
        host: Arc<VolumeHost>,
        indices: Vec<usize>,
    },
    #[cfg(test)]
    Test {
        volume: VolumeId,
        entered: std::sync::mpsc::Sender<VolumeId>,
        release: Arc<TestFaultWorkGate>,
        result: Result<(), ()>,
        panics: bool,
    },
}

#[cfg(test)]
struct TestFaultWorkGate {
    released: Mutex<bool>,
    ready: std::sync::Condvar,
}

#[cfg(test)]
impl TestFaultWorkGate {
    fn closed() -> Self {
        Self {
            released: Mutex::new(false),
            ready: std::sync::Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut released = self.released.lock().expect("test fault-work gate");
        while !*released {
            released = self.ready.wait(released).expect("test fault-work gate");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("test fault-work gate") = true;
        self.ready.notify_all();
    }
}

enum FaultWorkReply {
    Direct(Injector<Result<(), ()>>),
    Batch(u64),
}

struct QueuedFaultWork {
    operation: usize,
    queued: Instant,
    work: BlockingFaultWork,
    reply: FaultWorkReply,
}

struct CompletedFaultWork {
    worker: usize,
    volume: VolumeId,
    operation: usize,
    reply: FaultWorkReply,
    result: Result<(), ()>,
    panicked: bool,
    elapsed: Duration,
}

struct FaultWorkBatch {
    remaining: usize,
    failed: bool,
    reply: Injector<Result<(), ()>>,
}

#[derive(Default)]
struct FaultWorkQueue {
    queues: BTreeMap<VolumeId, VecDeque<QueuedFaultWork>>,
    ready: VecDeque<VolumeId>,
    active: BTreeSet<VolumeId>,
}

struct FaultWorkDispatcher<'a> {
    workers: &'a [std::sync::mpsc::Sender<(VolumeId, QueuedFaultWork)>],
    shared: Arc<Shared>,
    queue: FaultWorkQueue,
    idle_workers: VecDeque<usize>,
    batches: BTreeMap<u64, FaultWorkBatch>,
    next_batch: u64,
    barrier: Option<tokio::sync::oneshot::Sender<()>>,
    incoming_closed: bool,
}

impl<'a> FaultWorkDispatcher<'a> {
    fn new(
        workers: &'a [std::sync::mpsc::Sender<(VolumeId, QueuedFaultWork)>],
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            workers,
            shared,
            queue: FaultWorkQueue::default(),
            idle_workers: (0..workers.len()).collect(),
            batches: BTreeMap::new(),
            next_batch: 1,
            barrier: None,
            incoming_closed: false,
        }
    }
}

impl FaultWorkQueue {
    fn push(&mut self, volume: VolumeId, work: QueuedFaultWork) {
        let queue = self.queues.entry(volume).or_default();
        if queue.is_empty() && !self.active.contains(&volume) {
            self.ready.push_back(volume);
        }
        queue.push_back(work);
    }

    fn start_next(&mut self) -> Option<(VolumeId, QueuedFaultWork)> {
        if self.active.len() >= FAULT_WORK_CONCURRENCY {
            return None;
        }
        let volume = self.ready.pop_front()?;
        assert!(
            self.active.insert(volume),
            "fault-work volume already active"
        );
        let work = self
            .queues
            .get_mut(&volume)
            .and_then(VecDeque::pop_front)
            .expect("ready fault-work volume has pending work");
        Some((volume, work))
    }

    fn complete(&mut self, volume: VolumeId) {
        assert!(
            self.active.remove(&volume),
            "completed fault-work volume was not active"
        );
        if self.queues.get(&volume).is_some_and(VecDeque::is_empty) {
            self.queues.remove(&volume);
        } else {
            self.ready.push_back(volume);
        }
    }

    fn is_idle(&self) -> bool {
        self.queues.is_empty() && self.active.is_empty()
    }
}

impl FaultWork {
    fn operation(&self) -> usize {
        match self {
            Self::Fill { .. } => 0,
            Self::Unprotect { .. } => 1,
            Self::Evict { .. } => 2,
            Self::WriteProtect { .. } => 3,
            Self::Barrier { .. } => 4,
            #[cfg(test)]
            Self::CrashDispatcher => 4,
            #[cfg(test)]
            Self::Test { .. } => 0,
        }
    }
}

fn enqueue_fault_work(
    sender: &tokio::sync::mpsc::UnboundedSender<FaultWork>,
    stats: &FaultWorkStats,
    item: FaultWork,
) -> Result<(), ()> {
    let operation = item.operation();
    stats.enqueue(operation);
    sender.send(item).map_err(|_| {
        stats.rollback_enqueue(operation);
    })
}

fn fault_work_loop(work: tokio::sync::mpsc::UnboundedReceiver<FaultWork>, shared: Arc<Shared>) {
    let (completed, completed_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut worker_senders = Vec::with_capacity(FAULT_WORK_CONCURRENCY);
    let mut workers = Vec::with_capacity(FAULT_WORK_CONCURRENCY);
    for worker in 0..FAULT_WORK_CONCURRENCY {
        let (sender, receiver) = std::sync::mpsc::channel::<(VolumeId, QueuedFaultWork)>();
        let completed = completed.clone();
        workers.push(
            std::thread::Builder::new()
                .name(format!("blockd-fault-syscall-{worker}"))
                .spawn(move || {
                    while let Ok((volume, queued)) = receiver.recv() {
                        let started = Instant::now();
                        let executed =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                execute_fault_work(queued.work)
                            }));
                        let panicked = executed.is_err();
                        let result = executed.unwrap_or(Err(()));
                        if completed
                            .send(CompletedFaultWork {
                                worker,
                                volume,
                                operation: queued.operation,
                                reply: queued.reply,
                                result,
                                panicked,
                                elapsed: started.elapsed(),
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                })
                .expect("spawn fault syscall worker"),
        );
        worker_senders.push(sender);
    }
    drop(completed);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fault-work runtime");
    runtime.block_on(FaultWorkDispatcher::new(&worker_senders, shared).run(work, completed_rx));
    drop(worker_senders);
    for worker in workers {
        worker.join().expect("fault syscall worker joined");
    }
}

impl FaultWorkDispatcher<'_> {
    fn queue(
        &mut self,
        volume: VolumeId,
        operation: usize,
        queued: Instant,
        work: BlockingFaultWork,
        reply: FaultWorkReply,
    ) {
        self.queue.push(
            volume,
            QueuedFaultWork {
                operation,
                queued,
                work,
                reply,
            },
        );
    }

    fn admit_write_protect(
        &mut self,
        hosts: Vec<(VolumeId, Arc<VolumeHost>, Vec<usize>)>,
        reply: Injector<Result<(), ()>>,
        operation: usize,
        queued: Instant,
    ) {
        let batch = self.next_batch;
        self.next_batch = self.next_batch.wrapping_add(1);
        self.batches.insert(
            batch,
            FaultWorkBatch {
                remaining: hosts.len(),
                failed: false,
                reply,
            },
        );
        for (volume, host, indices) in hosts {
            self.queue(
                volume,
                operation,
                queued,
                BlockingFaultWork::WriteProtect { host, indices },
                FaultWorkReply::Batch(batch),
            );
        }
    }

    fn admit(&mut self, item: FaultWork) {
        let operation = item.operation();
        let queued = self.shared.fault_work_stats.dequeue(operation);
        self.shared.fault_work_stats.observe(
            operation,
            0,
            queued.map_or(Duration::ZERO, |started| started.elapsed()),
        );
        let queued = Instant::now();
        match item {
            FaultWork::Fill {
                host,
                page,
                bytes,
                writable,
                reply,
            } => self.queue(
                page.volume,
                operation,
                queued,
                BlockingFaultWork::Fill {
                    host,
                    page,
                    bytes,
                    writable,
                },
                FaultWorkReply::Direct(reply),
            ),
            FaultWork::Unprotect { host, page, reply } => self.queue(
                page.volume,
                operation,
                queued,
                BlockingFaultWork::Unprotect { host, page },
                FaultWorkReply::Direct(reply),
            ),
            FaultWork::Evict { host, page, reply } => self.queue(
                page.volume,
                operation,
                queued,
                BlockingFaultWork::Evict { host, page },
                FaultWorkReply::Direct(reply),
            ),
            FaultWork::WriteProtect { hosts, reply } => {
                if hosts.is_empty() {
                    self.shared
                        .fault_work_stats
                        .observe(operation, 1, Duration::ZERO);
                    self.shared
                        .fault_work_stats
                        .observe(operation, 2, Duration::ZERO);
                    let _ = reply.push(Lane::Critical, Ok(()));
                    return;
                }
                self.admit_write_protect(hosts, reply, operation, queued);
            }
            FaultWork::Barrier { done } => {
                self.shared
                    .fault_work_stats
                    .observe(operation, 1, Duration::ZERO);
                self.shared
                    .fault_work_stats
                    .observe(operation, 2, Duration::ZERO);
                self.barrier = Some(done);
            }
            #[cfg(test)]
            FaultWork::CrashDispatcher => panic!("injected fault dispatcher failure"),
            #[cfg(test)]
            FaultWork::Test {
                volume,
                entered,
                release,
                result,
                panics,
                reply,
            } => self.queue(
                volume,
                operation,
                queued,
                BlockingFaultWork::Test {
                    volume,
                    entered,
                    release,
                    result,
                    panics,
                },
                FaultWorkReply::Direct(reply),
            ),
        }
    }

    fn finish(&mut self, completed: CompletedFaultWork) {
        self.idle_workers.push_back(completed.worker);
        if completed.panicked {
            self.shared
                .fault_work_stats
                .join_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        self.shared
            .fault_work_stats
            .observe(completed.operation, 2, completed.elapsed);
        self.shared.fault_work_stats.complete();
        self.queue.complete(completed.volume);
        match completed.reply {
            FaultWorkReply::Direct(reply) => {
                let _ = reply.push(Lane::Critical, completed.result);
            }
            FaultWorkReply::Batch(batch) => {
                let state = self
                    .batches
                    .get_mut(&batch)
                    .expect("active fault-work batch");
                state.failed |= completed.result.is_err();
                state.remaining -= 1;
                if state.remaining == 0 {
                    let state = self
                        .batches
                        .remove(&batch)
                        .expect("completed fault-work batch");
                    let _ = state
                        .reply
                        .push(Lane::Critical, if state.failed { Err(()) } else { Ok(()) });
                }
            }
        }
    }

    async fn run(
        mut self,
        mut incoming: tokio::sync::mpsc::UnboundedReceiver<FaultWork>,
        mut completed: tokio::sync::mpsc::UnboundedReceiver<CompletedFaultWork>,
    ) {
        loop {
            while let Some(worker) = self.idle_workers.pop_front() {
                let Some((volume, queued)) = self.queue.start_next() else {
                    self.idle_workers.push_front(worker);
                    break;
                };
                self.shared
                    .fault_work_stats
                    .observe(queued.operation, 1, queued.queued.elapsed());
                self.workers[worker]
                    .send((volume, queued))
                    .expect("fault syscall worker alive");
                self.shared.fault_work_stats.start();
            }

            if self.queue.is_idle() {
                if let Some(done) = self.barrier.take() {
                    let _ = done.send(());
                    continue;
                }
                if self.incoming_closed {
                    break;
                }
            }

            tokio::select! {
                biased;
                result = completed.recv(), if self.idle_workers.len() != self.workers.len() => {
                    self.finish(result.expect("active fault syscall worker"));
                }
                item = incoming.recv(), if self.barrier.is_none() && !self.incoming_closed => {
                    let Some(item) = item else {
                        self.incoming_closed = true;
                        continue;
                    };
                    self.admit(item);
                }
            }
        }
    }
}

fn execute_fault_work(work: BlockingFaultWork) -> Result<(), ()> {
    match work {
        BlockingFaultWork::Fill {
            host,
            page,
            bytes,
            writable,
        } => {
            let index = VolumeHost::page_index(page);
            if let Some(bytes) = bytes {
                host.region.write_page(index, &bytes);
            }
            host.uffd
                .as_ref()
                .expect("compute fill")
                .continue_range(host.view.addr_of(index), page_size(), !writable)
                .map_err(|_| ())
        }
        BlockingFaultWork::Unprotect { host, page } => {
            let index = VolumeHost::page_index(page);
            host.uffd
                .as_ref()
                .expect("compute unprotect")
                .writeprotect(host.view.addr_of(index), page_size(), false)
                .map_err(|_| ())
        }
        BlockingFaultWork::Evict { host, page } => {
            let index = VolumeHost::page_index(page);
            host.view
                .evict(index, 1)
                .and_then(|()| host.region.punch_hole(index, 1))
                .map_err(|_| ())
        }
        BlockingFaultWork::WriteProtect { host, mut indices } => {
            let mut result = Ok(());
            let uffd = host.uffd.as_ref().expect("compute write protection");
            for_each_contiguous_run(&mut indices, |start, len| {
                if result.is_ok() {
                    result = uffd
                        .writeprotect(host.view.addr_of(start), len * page_size(), true)
                        .map_err(|_| ());
                }
            });
            result
        }
        #[cfg(test)]
        BlockingFaultWork::Test {
            volume,
            entered,
            release,
            result,
            panics,
        } => {
            entered.send(volume).expect("test observes fault work");
            release.wait();
            assert!(!panics, "synthetic fault-work panic");
            result
        }
    }
}

fn for_each_contiguous_run(indices: &mut Vec<usize>, mut visit: impl FnMut(usize, usize)) {
    indices.sort_unstable();
    indices.dedup();
    let Some((&first, rest)) = indices.split_first() else {
        return;
    };
    let mut start = first;
    let mut end = first;
    for &index in rest {
        if index == end + 1 {
            end = index;
        } else {
            visit(start, end - start + 1);
            start = index;
            end = index;
        }
    }
    visit(start, end - start + 1);
}

type SharedPageKey = (u64, u64, blockd_core::types::ObjectId, u32);

struct ProductionWorld {
    blobs: FileBlobs,
    store: RuntimeStore,
    peers: Option<Arc<PeerNet>>,
    local_host: HostId,
    peer_rx: Injected<(blockd_core::types::HostId, PeerMsg)>,
    fault_rx: Injected<GuestFault>,
    fault_tx: Injector<GuestFault>,
    sync_rx: Injected<GuestSyncRequest>,
    admin_rx: Injected<AdminRequest>,
    shared: Arc<Shared>,
    fault_work: tokio::sync::mpsc::UnboundedSender<FaultWork>,
    shared_pages: RefCell<BTreeMap<SharedPageKey, Vec<u8>>>,
}

impl ProductionWorld {
    fn host(&self, volume: VolumeId) -> Arc<VolumeHost> {
        self.shared.volumes.lock().expect("volume lock")[&volume].clone()
    }

    fn enqueue_fault_work(&self, item: FaultWork) -> Result<(), GuestMemoryError> {
        enqueue_fault_work(&self.fault_work, &self.shared.fault_work_stats, item)
            .map_err(|()| GuestMemoryError::Unavailable)
    }

    async fn exact_planned_retirement_is_durable(&self) -> bool {
        let expected = *self
            .shared
            .planned_retirement
            .lock()
            .expect("planned retirement lock");
        let Some(expected) = expected else {
            return false;
        };
        let key = blockd_core::layout::host_session_key(self.local_host);
        self.store
            .get(&key)
            .await
            .ok()
            .flatten()
            .and_then(|(_, bytes)| HostSessionRecord::decode(&bytes).ok())
            == Some(expected)
    }

    async fn fault_response(
        &self,
        response: &Injected<Result<(), ()>>,
        operation: usize,
    ) -> Result<(), GuestMemoryError> {
        let started = Instant::now();
        let result = response.recv().await.unwrap_or(Err(()));
        self.shared
            .stats
            .record_world(operation, elapsed_ns(started.elapsed()));
        result.map_err(|()| GuestMemoryError::Unavailable)
    }

    async fn blob_observe<T>(
        &self,
        operation: usize,
        local_operation: usize,
        future: impl std::future::Future<Output = Result<T, BlobError>>,
        missing: impl FnOnce(&T) -> bool,
    ) -> Result<T, BlobError> {
        self.shared.local_io_in_flight[local_operation].fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = future.await;
        self.shared.local_io_in_flight[local_operation].fetch_sub(1, Ordering::Relaxed);
        let elapsed = started.elapsed();
        self.shared
            .stats
            .record_world(operation, elapsed_ns(elapsed));
        let outcome = usize::from(result.as_ref().is_ok_and(missing));
        self.shared.local_io_latency[local_operation][outcome].observe(elapsed);
        result
    }

    async fn store_observe<T>(
        &self,
        operation: usize,
        future: impl std::future::Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        let started = Instant::now();
        let result = future.await;
        self.shared
            .stats
            .record_world(operation, elapsed_ns(started.elapsed()));
        result
    }
}

impl Blobs for ProductionWorld {
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
        self.blob_observe(world_kind::BLOB_READ, 1, self.blobs.scan(), |_entries| {
            false
        })
        .await
    }

    async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.blob_observe(
            world_kind::BLOB_WRITE,
            0,
            self.blobs.write(name, bytes),
            |()| false,
        )
        .await
    }

    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.blob_observe(
            world_kind::REPLICA_APPEND,
            0,
            self.blobs.append(name, bytes),
            |()| false,
        )
        .await
    }

    async fn replace_tail_if_len(
        &self,
        name: String,
        expected_total_len: u64,
        valid_prefix_len: u64,
        bytes: Vec<u8>,
    ) -> Result<bool, BlobError> {
        self.blob_observe(
            world_kind::REPLICA_APPEND,
            0,
            self.blobs
                .replace_tail_if_len(name, expected_total_len, valid_prefix_len, bytes),
            |appended| !appended,
        )
        .await
    }

    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
        self.blob_observe(
            world_kind::REPLICA_TRUNCATE,
            0,
            self.blobs.truncate(name, len),
            |()| false,
        )
        .await
    }

    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        self.blob_observe(
            world_kind::BLOB_READ,
            1,
            self.blobs.read(name),
            Option::is_none,
        )
        .await
    }

    async fn read_range(
        &self,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BlobError> {
        self.blob_observe(
            world_kind::BLOB_READ_RANGE,
            2,
            self.blobs.read_range(name, offset, len),
            Option::is_none,
        )
        .await
    }

    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        self.blob_observe(world_kind::BLOB_DELETE, 3, self.blobs.delete(name), |()| {
            false
        })
        .await
    }

    async fn delete_many_durable(&self, names: &[String]) -> Result<(), BlobError> {
        self.blob_observe(
            world_kind::REPLICA_DELETE,
            3,
            self.blobs.delete_many_durable(names),
            |()| false,
        )
        .await
    }
}

impl Store for ProductionWorld {
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
        self.store_observe(world_kind::STORE_PUT, self.store.put(key, bytes))
            .await
    }

    async fn put_cas(
        &self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreError> {
        self.store_observe(
            world_kind::STORE_CAS,
            self.store.put_cas(key, expected, bytes),
        )
        .await
    }

    async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        self.store_observe(world_kind::STORE_GET, self.store.get(key))
            .await
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        self.store_observe(
            world_kind::STORE_GET_RANGE,
            self.store.get_range(key, offset, len),
        )
        .await
    }

    async fn delete(&self, key: &str) -> Result<bool, StoreError> {
        self.store_observe(world_kind::STORE_DELETE, self.store.delete(key))
            .await
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.store_observe(world_kind::STORE_GET, self.store.list_prefix(prefix))
            .await
    }
}

impl Peers for ProductionWorld {
    async fn send(&self, to: HostId, message: PeerMsg) {
        let started = Instant::now();
        if let Some(peers) = &self.peers {
            peers.send_identity(to, &message);
        } else {
            self.shared
                .incidents
                .lock()
                .expect("incident lock")
                .push(format!("peer send to {to:?} with no peer config"));
        }
        self.shared
            .stats
            .record_world(world_kind::PEER_SEND, elapsed_ns(started.elapsed()));
    }

    async fn recv(&self) -> Option<(blockd_core::types::HostId, PeerMsg)> {
        self.peer_rx.recv().await
    }
}

fn group_write_protect_pages(
    pages: &[PageId],
    mut volume_kind: impl FnMut(VolumeId) -> VolumeKind,
) -> BTreeMap<VolumeId, Vec<usize>> {
    let mut by_volume = BTreeMap::<VolumeId, Vec<usize>>::new();
    for &page in pages {
        match volume_kind(page.volume) {
            VolumeKind::Memory | VolumeKind::Data => by_volume
                .entry(page.volume)
                .or_default()
                .push(VolumeHost::page_index(page)),
        }
    }
    by_volume
}

impl GuestMem for ProductionWorld {
    async fn read_page(&self, page: PageId) -> Vec<u8> {
        let host = self.host(page.volume);
        host.region.read_page(VolumeHost::page_index(page))
    }

    async fn arm_write_protect(&self, pages: &[PageId]) -> Result<(), GuestMemoryError> {
        let hosts = {
            let volumes = self.shared.volumes.lock().expect("volume lock");
            let by_volume = group_write_protect_pages(pages, |volume| volumes[&volume].config.kind);
            by_volume
                .into_iter()
                .map(|(volume, pages)| (volume, Arc::clone(&volumes[&volume]), pages))
                .collect()
        };
        let (reply, response) = injector();
        self.enqueue_fault_work(FaultWork::WriteProtect { hosts, reply })?;
        self.fault_response(&response, world_kind::WRITE_PROTECT)
            .await
    }

    async fn fill(
        &self,
        page: PageId,
        bytes: Vec<u8>,
        writable: bool,
        source: FillSource,
    ) -> Result<(), GuestMemoryError> {
        let host = self.host(page.volume);
        let (reply, response) = injector();
        self.enqueue_fault_work(FaultWork::Fill {
            host: Arc::clone(&host),
            page,
            bytes: Some(bytes),
            writable,
            reply,
        })?;
        self.fault_response(&response, world_kind::FILL).await?;
        self.shared
            .complete_fault(Some(&host), page, source.into(), "served");
        Ok(())
    }

    async fn fill_shared(
        &self,
        page: PageId,
        share: (u64, u64, blockd_core::types::ObjectId, u32),
        bytes: Option<Vec<u8>>,
        writable: bool,
    ) -> Result<(), GuestMemoryError> {
        if let Some(bytes) = bytes {
            self.shared_pages.borrow_mut().insert(share, bytes);
        }
        let bytes = self
            .shared_pages
            .borrow()
            .get(&share)
            .cloned()
            .expect("shared base page admitted before reuse");
        let host = self.host(page.volume);
        let (reply, response) = injector();
        self.enqueue_fault_work(FaultWork::Fill {
            host: Arc::clone(&host),
            page,
            bytes: Some(bytes),
            writable,
            reply,
        })?;
        self.fault_response(&response, world_kind::FILL_SHARED)
            .await?;
        self.shared
            .complete_fault(Some(&host), page, FaultSource::Shared, "served");
        Ok(())
    }

    async fn remap(&self, page: PageId, writable: bool) -> Result<(), GuestMemoryError> {
        let host = self.host(page.volume);
        let (reply, response) = injector();
        self.enqueue_fault_work(FaultWork::Fill {
            host: Arc::clone(&host),
            page,
            bytes: None,
            writable,
            reply,
        })?;
        self.fault_response(&response, world_kind::FILL).await?;
        self.shared
            .complete_fault(Some(&host), page, FaultSource::Local, "served");
        Ok(())
    }

    async fn fail(&self, page: PageId) -> Result<(), GuestMemoryError> {
        self.shared.stats.record_world(world_kind::FILL_FAILED, 0);
        self.shared
            .complete_fault(None, page, FaultSource::Unservable, "failed");
        tracing::error!(
            volume_id = page.volume.0,
            ?page,
            "fatal unservable guest page"
        );
        Err(GuestMemoryError::Unservable)
    }

    async fn unprotect(&self, page: PageId) -> Result<(), GuestMemoryError> {
        let host = self.host(page.volume);
        let (reply, response) = injector();
        self.enqueue_fault_work(FaultWork::Unprotect {
            host: Arc::clone(&host),
            page,
            reply,
        })?;
        self.fault_response(&response, world_kind::UNPROTECT)
            .await?;
        self.shared
            .complete_fault(Some(&host), page, FaultSource::WriteProtect, "served");
        Ok(())
    }

    async fn evict(&self, page: PageId) -> Result<(), GuestMemoryError> {
        let host = self.host(page.volume);
        let (reply, response) = injector();
        self.enqueue_fault_work(FaultWork::Evict { host, page, reply })?;
        self.fault_response(&response, world_kind::EVICT).await
    }

    async fn install_vmstate(
        &self,
        volume: VolumeId,
        bytes: Vec<u8>,
    ) -> Result<(), GuestMemoryError> {
        let raw: [u8; 8] = bytes
            .get(..8)
            .ok_or(GuestMemoryError::Unservable)?
            .try_into()
            .map_err(|_| GuestMemoryError::Unservable)?;
        self.host(volume)
            .ctl
            .state
            .lock()
            .expect("guest control lock")
            .applied = u64::from_le_bytes(raw);
        Ok(())
    }

    async fn pause(&self, volume: VolumeId) -> Result<GuestPause, GuestMemoryError> {
        let started = Instant::now();
        self.shared.begin_pause(volume);
        let host = self.host(volume);
        let (generation, response) = {
            let mut state = host.ctl.state.lock().expect("guest control lock");
            state.pause_generation = state
                .pause_generation
                .checked_add(1)
                .expect("guest pause generation overflow");
            let generation = state.pause_generation;
            state.pause_requested = true;
            let response = if state.in_op {
                let (reply, response) = injector();
                state.pause_waiter = Some(reply);
                Some(response)
            } else {
                state.paused = true;
                None
            };
            (generation, response)
        };
        let pending = PendingGuestPause {
            shared: Arc::clone(&self.shared),
            host: Arc::clone(&host),
            volume,
            generation,
            active: true,
        };
        let applied = if let Some(response) = response {
            response.recv().await.ok_or(GuestMemoryError::Unavailable)?
        } else {
            host.ctl.state.lock().expect("guest control lock").applied
        };
        pending.finish();
        self.shared
            .stats
            .record_world(world_kind::PAUSE_GUEST, elapsed_ns(started.elapsed()));
        Ok(GuestPause {
            vmstate: applied,
            vmstate_bytes: applied.to_le_bytes().to_vec(),
            generation,
        })
    }

    async fn resume(
        &self,
        volume: VolumeId,
        pause: Option<GuestPause>,
    ) -> Result<(), GuestMemoryError> {
        let host = self.host(volume);
        let mut state = host.ctl.state.lock().expect("guest control lock");
        if pause.is_some_and(|pause| pause.generation != state.pause_generation) {
            return Ok(());
        }
        state.pause_requested = false;
        state.paused = false;
        drop(state);
        self.shared.complete_pause(volume);
        host.ctl.ready.notify_waiters();
        self.shared.stats.record_world(world_kind::RESUME_GUEST, 0);
        Ok(())
    }

    async fn commit_pause(
        &self,
        volume: VolumeId,
        pause: GuestPause,
    ) -> Result<(), GuestMemoryError> {
        let host = self.host(volume);
        let mut state = host.ctl.state.lock().expect("guest control lock");
        if pause.generation != state.pause_generation {
            return Ok(());
        }
        state.pause_requested = false;
        state.pause_waiter = None;
        drop(state);
        self.shared.complete_pause(volume);
        Ok(())
    }

    async fn harvest_accessed(&self) -> Vec<PageId> {
        Vec::new()
    }

    async fn next_fault(&self) -> Option<GuestFault> {
        self.fault_rx.recv().await
    }

    async fn next_sync(&self) -> Option<GuestSyncRequest> {
        self.sync_rx.recv().await
    }

    async fn fence(&self, volume: VolumeId) -> Result<(), GuestMemoryError> {
        self.shared
            .incidents
            .lock()
            .expect("incident lock")
            .push(format!("fenced: {volume:?}"));
        self.shared.stats.record_world(world_kind::VOLUME_FENCED, 0);
        Ok(())
    }
}

impl AdminIo for ProductionWorld {
    async fn next_admin(&self) -> Option<AdminRequest> {
        self.admin_rx.recv().await
    }

    async fn emit_admin_event(&self, event: AdminEvent) {
        self.shared
            .admin_events
            .lock()
            .expect("admin event lock")
            .push_back(event);
        self.shared.admin_event_ready.notify_waiters();
        self.shared.stats.record_world(world_kind::ADMIN, 0);
    }

    async fn prepare_recovered_volume(&self, volume: VolumeId, config: VolumeConfig) -> bool {
        prepare_recovered_volume_with(
            Arc::clone(&self.shared),
            self.fault_tx.clone(),
            volume,
            config,
        )
        .await
    }

    async fn volume_released(&self, volume: VolumeId) {
        self.shared
            .released_volumes
            .lock()
            .expect("released volume lock")
            .insert(volume);
        self.shared.volume_released.notify_waiters();
    }

    async fn host_failed(&self, failure: HostFatal) {
        use std::io::Write as _;

        if failure.reason == "host session fenced"
            && self.exact_planned_retirement_is_durable().await
        {
            self.shared
                .planned_retirement_verified
                .store(true, Ordering::SeqCst);
            return;
        }
        tracing::error!(reason = failure.reason, "fatal host actor failure");
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(stderr, "fatal host actor failure: {}", failure.reason);
        let _ = stderr.flush();
        drop(stderr);
        self.shared
            .incidents
            .lock()
            .expect("incident lock")
            .push(format!("host failure: {}", failure.reason));
        self.shared.stats.record_world(world_kind::ABORT, 0);
        crate::flush_fatal_records();
        std::process::abort();
    }
}

async fn prepare_recovered_volume_with(
    shared: Arc<Shared>,
    fault_tx: Injector<GuestFault>,
    volume: VolumeId,
    config: VolumeConfig,
) -> bool {
    let (host, existing) = {
        let mut volumes = shared.volumes.lock().expect("volume lock");
        if let Some(existing) = volumes.get(&volume) {
            if existing.config != config {
                return false;
            }
            (Arc::clone(existing), true)
        } else {
            let host = VolumeHost::new(volume, config);
            volumes.insert(volume, Arc::clone(&host));
            (host, false)
        }
    };
    if existing {
        return shared
            .fault_readers
            .lock()
            .expect("fault reader lock")
            .get(&volume)
            .is_some_and(FaultReaderTask::is_live);
    }
    Runtime::spawn_fault_reader_with(&shared, fault_tx, volume, host)
        .await
        .unwrap_or(false)
}

#[derive(Clone)]
struct Inputs {
    admin: Injector<AdminRequest>,
    faults: Injector<GuestFault>,
    syncs: Injector<GuestSyncRequest>,
    peers: Injector<(blockd_core::types::HostId, PeerMsg)>,
}

const PEER_INPUT_CAPACITY: usize = 4;
const OBSERVATION_INTERVAL_NS: u64 = 100_000_000;

impl Inputs {
    fn depths(&self) -> (usize, usize) {
        [
            self.admin.depths(),
            self.faults.depths(),
            self.syncs.depths(),
            self.peers.depths(),
        ]
        .into_iter()
        .fold((0, 0), |(critical, background), (next_c, next_b)| {
            (
                critical.saturating_add(next_c),
                background.saturating_add(next_b),
            )
        })
    }
}

struct RuntimeStartupGuard {
    peers: Option<Arc<PeerNet>>,
    placement_worker: Option<tokio::task::JoinHandle<()>>,
    fault_work: Option<tokio::sync::mpsc::UnboundedSender<FaultWork>>,
    fault_worker: Option<std::thread::JoinHandle<()>>,
    fault_worker_expected_stop: Arc<AtomicBool>,
    armed: bool,
}

struct RuntimeStartupResources {
    peers: Option<Arc<PeerNet>>,
    placement_worker: Option<tokio::task::JoinHandle<()>>,
    #[cfg(test)]
    fault_work: tokio::sync::mpsc::UnboundedSender<FaultWork>,
    fault_worker: std::thread::JoinHandle<()>,
}

impl RuntimeStartupGuard {
    fn new(
        fault_work: tokio::sync::mpsc::UnboundedSender<FaultWork>,
        fault_worker: std::thread::JoinHandle<()>,
        fault_worker_expected_stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            peers: None,
            placement_worker: None,
            fault_work: Some(fault_work),
            fault_worker: Some(fault_worker),
            fault_worker_expected_stop,
            armed: true,
        }
    }

    fn fault_work(&self) -> &tokio::sync::mpsc::UnboundedSender<FaultWork> {
        self.fault_work.as_ref().expect("startup fault sender")
    }

    async fn rollback<T>(mut self, error: RuntimeStartupError) -> Result<T, RuntimeStartupError> {
        self.cleanup().await;
        Err(error)
    }

    async fn cleanup(&mut self) {
        self.fault_worker_expected_stop
            .store(true, Ordering::SeqCst);
        if let Some(worker) = self.placement_worker.take() {
            worker.abort();
            let _ = worker.await;
        }
        if let Some(peers) = self.peers.take() {
            let _ = peers.shutdown().await;
        }
        self.fault_work.take();
        if let Some(worker) = self.fault_worker.take() {
            let _ = tokio::task::spawn_blocking(move || worker.join()).await;
        }
        self.armed = false;
    }

    fn commit(mut self) -> RuntimeStartupResources {
        self.armed = false;
        let fault_work = self.fault_work.take().expect("startup fault sender");
        #[cfg(not(test))]
        drop(fault_work);
        RuntimeStartupResources {
            peers: self.peers.take(),
            placement_worker: self.placement_worker.take(),
            #[cfg(test)]
            fault_work,
            fault_worker: self.fault_worker.take().expect("startup fault worker"),
        }
    }
}

impl Drop for RuntimeStartupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.fault_worker_expected_stop
            .store(true, Ordering::SeqCst);
        self.fault_work.take();
        if let Some(worker) = self.placement_worker.take() {
            worker.abort();
        }
        if let Some(peers) = self.peers.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                let _ = peers.shutdown().await;
            });
        }
        if let Some(worker) = self.fault_worker.take() {
            let _ = std::thread::Builder::new()
                .name("blockd-startup-rollback".to_owned())
                .spawn(move || {
                    let _ = worker.join();
                });
        }
    }
}

pub struct Runtime {
    inputs: Inputs,
    shared: Arc<Shared>,
    blob_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    host: HostId,
    authority_required: bool,
    peers: Option<Arc<PeerNet>>,
    placement_worker: Option<tokio::task::JoinHandle<()>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    actor_expected_stop: Arc<AtomicBool>,
    actor_task: Option<std::thread::JoinHandle<()>>,
    fault_worker: Option<std::thread::JoinHandle<()>>,
    fault_worker_expected_stop: Arc<AtomicBool>,
    live_placement: Arc<LivePlacementStatus>,
    #[cfg(test)]
    actor_failure: tokio::sync::mpsc::UnboundedSender<TestActorFailure>,
    #[cfg(test)]
    fault_work: tokio::sync::mpsc::UnboundedSender<FaultWork>,
}

/// The independently actionable dependencies behind daemon readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // each bool is an independently reported health gate
pub struct RuntimeReadiness {
    pub authority: bool,
    pub membership_ownership: bool,
    pub placement: bool,
    pub recovery: bool,
    pub peer_listener: bool,
    pub critical_tasks: bool,
    pub unfenced: bool,
}

impl RuntimeReadiness {
    pub fn ready(self) -> bool {
        self.authority
            && self.membership_ownership
            && self.placement
            && self.recovery
            && self.peer_listener
            && self.critical_tasks
            && self.unfenced
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy)]
enum TestActorFailure {
    HostActor,
    Observation,
}

struct FaultReaderTask {
    task: tokio::task::JoinHandle<()>,
    expected_stop: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
}

impl FaultReaderTask {
    fn is_live(&self) -> bool {
        self.ready.load(Ordering::SeqCst) && !self.task.is_finished()
    }

    fn stop(self) -> tokio::task::JoinHandle<()> {
        self.expected_stop.store(true, Ordering::SeqCst);
        self.task.abort();
        self.task
    }
}

#[cfg(test)]
#[derive(Clone)]
struct TestFaultReaderStart {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    fail: bool,
}

#[cfg(test)]
impl TestFaultReaderStart {
    fn held(fail: bool) -> Self {
        Self {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            fail,
        }
    }
}

#[cfg(test)]
fn test_fault_reader_starts() -> &'static Mutex<BTreeMap<PathBuf, TestFaultReaderStart>> {
    static STARTS: std::sync::OnceLock<Mutex<BTreeMap<PathBuf, TestFaultReaderStart>>> =
        std::sync::OnceLock::new();
    STARTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn set_test_fault_reader_start(blob_dir: &Path, start: TestFaultReaderStart) {
    test_fault_reader_starts()
        .lock()
        .expect("test fault reader starts lock")
        .insert(blob_dir.to_path_buf(), start);
}

/// A persistent guest thread's access to one compute volume.
///
/// Real VM vCPU threads touch their mappings directly and block in the kernel
/// while userfaultfd work is serviced. Keeping this handle on a dedicated
/// thread avoids a fresh executor handoff for every memory access.
#[derive(Clone)]
pub struct GuestAccess {
    host: Arc<VolumeHost>,
}

pub struct GuestOperation {
    host: Arc<VolumeHost>,
}

impl GuestAccess {
    pub fn try_begin(&self) -> Option<GuestOperation> {
        let mut state = self.host.ctl.state.lock().expect("guest control lock");
        if state.pause_requested || state.paused || state.in_op {
            return None;
        }
        state.in_op = true;
        drop(state);
        Some(GuestOperation {
            host: Arc::clone(&self.host),
        })
    }

    pub async fn begin(&self) -> GuestOperation {
        self.host.op_start().await;
        GuestOperation {
            host: Arc::clone(&self.host),
        }
    }
}

impl GuestOperation {
    pub fn read_word(&self, page: PageId) -> u64 {
        self.host.view.read_word(VolumeHost::page_index(page))
    }

    pub fn read_page(&self, page: PageId) -> Vec<u8> {
        self.host.view.read_page(VolumeHost::page_index(page))
    }

    pub fn write_word(&self, page: PageId, value: u64) {
        self.host
            .view
            .write_word(VolumeHost::page_index(page), value);
    }

    pub fn evict_page(&self, page: PageId) -> std::io::Result<()> {
        self.host.view.evict(VolumeHost::page_index(page), 1)
    }
}

impl Drop for GuestOperation {
    fn drop(&mut self) {
        self.host.op_end();
    }
}

impl Runtime {
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self, RuntimeStartupError> {
        let blob_dir = config.blob_dir.clone();
        let (volume_configs, unidentified_volumes) = tokio::task::spawn_blocking(move || {
            crate::world::prepare_blob_root(&blob_dir)?;
            let mut recovered = BTreeMap::<VolumeId, ((u64, u64), VolumeConfig)>::new();
            let mut discovered = BTreeSet::new();
            for blob in crate::blobscan::scan_blob_dir_for_recovery(&blob_dir) {
                let Some(BlobName::Journal { volume, .. }) =
                    blockd_core::layout::parse_blob(&blob.name)
                else {
                    continue;
                };
                discovered.insert(volume);
                let Ok(record) = JournalRecord::decode(volume, &blob.bytes) else {
                    continue;
                };
                let order = (record.capture_seq, record.seq.0);
                if recovered
                    .get(&volume)
                    .is_none_or(|(current, _)| order > *current)
                {
                    recovered.insert(volume, (order, record.config));
                }
            }
            let configs = recovered
                .into_iter()
                .map(|(volume, (_, config))| (volume, config))
                .collect::<BTreeMap<_, _>>();
            let unidentified = discovered
                .into_iter()
                .filter(|volume| !configs.contains_key(volume))
                .collect::<Vec<_>>();
            Ok::<_, std::io::Error>((configs, unidentified))
        })
        .await
        .map_err(|error| RuntimeStartupError::LocalDiscovery(error.to_string()))?
        .map_err(RuntimeStartupError::BlobDirectory)?;
        let recovered_volumes = volume_configs.keys().copied().collect::<Vec<_>>();
        let hosts = volume_configs
            .into_iter()
            .map(|(volume, volume_config)| {
                assert_peer_stash_transport(volume_config, config.peer.is_some());
                (volume, VolumeHost::new(volume, volume_config))
            })
            .collect();
        let runtime = Self::start(hosts, config, store).await?;
        #[cfg(test)]
        if let Some(start) = test_fault_reader_starts()
            .lock()
            .expect("test fault reader starts lock")
            .remove(&config.blob_dir)
        {
            *runtime
                .shared
                .fault_reader_start
                .lock()
                .expect("fault reader start lock") = Some(start);
        }
        for volume in unidentified_volumes {
            runtime.shared.quarantines.lock().expect("quarantine lock").insert(
                volume,
                "local journal metadata is corrupt; preserve artifacts for repair or explicit audited cleanup"
                    .to_owned(),
            );
        }
        for volume in recovered_volumes {
            let verdict = runtime.wait_recovered(volume).await;
            if verdict == Verdict::Unrestorable {
                runtime.shared.quarantines.lock().expect("quarantine lock").insert(
                    volume,
                    "no intact local recovery point; repair local artifacts or restore from an operator-verified source"
                        .to_owned(),
                );
            } else {
                let host = runtime.host(volume);
                if !runtime.start_fault_reader(volume, host).await {
                    runtime.shared.quarantines.lock().expect("quarantine lock").insert(
                        volume,
                        "runtime fault service failed to register; retain local artifacts and keep the volume unavailable"
                            .to_owned(),
                    );
                }
            }
        }
        let recovery_healthy = runtime
            .shared
            .quarantines
            .lock()
            .expect("quarantine lock")
            .is_empty();
        runtime
            .shared
            .recovery_complete
            .store(recovery_healthy, Ordering::SeqCst);
        Ok(runtime)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub async fn recover(
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
        volume_configs: &BTreeMap<VolumeId, VolumeConfig>,
    ) -> Result<(Self, BTreeMap<VolumeId, Verdict>), RuntimeStartupError> {
        let mut hosts = BTreeMap::new();
        for (&volume, &volume_config) in volume_configs {
            assert_peer_stash_transport(volume_config, config.peer.is_some());
            hosts.insert(volume, VolumeHost::new(volume, volume_config));
        }
        let runtime = Self::start(hosts, config, store).await?;
        let mut verdicts = BTreeMap::new();
        for &volume in volume_configs.keys() {
            let verdict = runtime.wait_recovered(volume).await;
            if verdict == Verdict::Unrestorable {
                runtime.shared.quarantines.lock().expect("quarantine lock").insert(
                    volume,
                    "no intact local recovery point; repair local artifacts or restore from an operator-verified source"
                        .to_owned(),
                );
            } else {
                let host = runtime.host(volume);
                if !runtime.start_fault_reader(volume, host).await {
                    runtime.shared.quarantines.lock().expect("quarantine lock").insert(
                        volume,
                        "runtime fault service failed to register; retain local artifacts and keep the volume unavailable"
                            .to_owned(),
                    );
                }
            }
            verdicts.insert(volume, verdict);
        }
        let recovery_healthy = runtime
            .shared
            .quarantines
            .lock()
            .expect("quarantine lock")
            .is_empty();
        runtime
            .shared
            .recovery_complete
            .store(recovery_healthy, Ordering::SeqCst);
        Ok((runtime, verdicts))
    }

    #[allow(clippy::too_many_lines)]
    async fn start(
        hosts: BTreeMap<VolumeId, Arc<VolumeHost>>,
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self, RuntimeStartupError> {
        let placement_authority = config
            .daemon
            .cluster_placement
            .as_ref()
            .and_then(|placement| placement.authority);
        if let Some(authority) = placement_authority {
            assert_eq!(
                config.cluster_id,
                Some(authority.cluster_id),
                "runtime cluster identity must match authority placement"
            );
        }
        let placement_cluster_id = config
            .cluster_id
            .or_else(|| placement_authority.map(|authority| authority.cluster_id));
        assert!(
            config.peer.is_none() || placement_cluster_id.is_some(),
            "peer runtime requires a durable cluster identity"
        );
        let blob_root = config.blob_dir.clone();
        tokio::task::spawn_blocking(move || crate::world::prepare_blob_root(&blob_root))
            .await
            .map_err(|error| RuntimeStartupError::LocalDiscovery(error.to_string()))?
            .map_err(RuntimeStartupError::BlobDirectory)?;
        let (fault_work, fault_work_rx) = tokio::sync::mpsc::unbounded_channel();
        let (admin, admin_rx_actor) = injector();
        let (faults, fault_rx_actor) = injector();
        let (syncs, sync_rx_actor) = injector();
        let (peer_input, peer_rx_actor) = bounded_injector(PEER_INPUT_CAPACITY);
        let inputs = Inputs {
            admin,
            faults,
            syncs,
            peers: peer_input,
        };
        let shared = Arc::new(Shared::new(hosts, &config.daemon));
        let fault_worker_shared = Arc::clone(&shared);
        let fault_worker_expected_stop = Arc::new(AtomicBool::new(false));
        let fault_worker_stop = Arc::clone(&fault_worker_expected_stop);
        let fault_worker_span = tracing::Span::current();
        let fault_worker = std::thread::Builder::new()
            .name("blockd-fault-work".to_owned())
            .spawn(move || {
                let _span = fault_worker_span.enter();
                let _guard = CriticalThreadGuard {
                    shared: Arc::clone(&fault_worker_shared),
                    expected_stop: fault_worker_stop,
                    name: "fault worker",
                };
                fault_work_loop(fault_work_rx, fault_worker_shared);
            })
            .map_err(|source| RuntimeStartupError::ThreadSpawn {
                thread: "fault worker",
                source,
            })?;
        let mut startup = RuntimeStartupGuard::new(
            fault_work,
            fault_worker,
            Arc::clone(&fault_worker_expected_stop),
        );
        let blobs = FileBlobs::new(&config.blob_dir);
        let peer_store = Arc::clone(&store);
        let runtime_store = RuntimeStore::new(Arc::clone(&store));
        let mut actor_config = config.daemon.clone();
        let peer_input = inputs.peers.clone();
        let placement_input = inputs.admin.clone();
        let placement_host = actor_config.host;
        let live_placement = Arc::new(LivePlacementStatus::new(config.peer.is_some()));
        let (placement_rosters, placement_roster_rx) =
            tokio::sync::watch::channel(Vec::<HostId>::new());
        let peers = match config.peer.clone() {
            Some(peer_config) => {
                let placement_status = Arc::clone(&live_placement);
                PeerNet::start_with_membership_result(
                    &peer_config,
                    placement_host,
                    Arc::clone(&peer_store),
                    move |from, message| {
                        let lane = peer_lane(&message);
                        let _ = peer_input.push(lane, (from, message));
                    },
                    move |members| {
                        placement_status.publish(members.clone());
                        placement_rosters.send_replace(members);
                    },
                )
                .await
                .map(Some)
                .map_err(|error| match error {
                    crate::peer::PeerStartError::Listener(error) => {
                        RuntimeStartupError::PeerListener(error)
                    }
                    crate::peer::PeerStartError::Membership(error) => {
                        RuntimeStartupError::PeerMembership(error)
                    }
                })
            }
            None => Ok(None),
        };
        startup.peers = match peers {
            Ok(peers) => peers,
            Err(error) => return startup.rollback(error).await,
        };
        let peers = startup.peers.as_ref();
        if peers.is_some() {
            let (placement_ready, ready) = tokio::sync::oneshot::channel();
            let placement_owner = tokio::spawn(
                PlacementOwner {
                    store: Arc::clone(&peer_store),
                    cluster_id: placement_cluster_id
                        .expect("peer runtime has a durable cluster identity"),
                    authority: placement_authority,
                    local: placement_host,
                    status: Arc::clone(&live_placement),
                    rosters: placement_roster_rx,
                    admin: placement_input,
                    startup: placement_ready,
                }
                .run()
                .instrument(tracing::Span::current()),
            );
            startup.placement_worker = Some(placement_owner);
            match ready.await {
                Ok(Ok(placement)) => actor_config.cluster_placement = Some(placement),
                Ok(Err(error)) => return startup.rollback(error).await,
                Err(_) => {
                    return startup
                        .rollback(RuntimeStartupError::Placement(
                            blockd_core::protocol::StoreFault::Unavailable,
                        ))
                        .await;
                }
            }
        }
        let actor_shared = Arc::clone(&shared);
        let actor_inputs = inputs.clone();
        let world_fault_work = startup.fault_work().clone();
        let shutdown_fault_work = startup.fault_work().clone();
        let actor_peers = startup.peers.clone();
        let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel();
        #[cfg(test)]
        let (actor_failure, mut actor_failure_rx) = tokio::sync::mpsc::unbounded_channel();
        let poll_stats = Arc::clone(&actor_shared);
        let actor_expected_stop = Arc::new(AtomicBool::new(false));
        let actor_stop = Arc::clone(&actor_expected_stop);
        let actor_guard_shared = Arc::clone(&actor_shared);
        let actor_thread_name = format!("blockd-actor-{}", actor_config.host.get());
        let actor_span = tracing::Span::current();
        let actor_task = std::thread::Builder::new()
            .name(actor_thread_name)
            .spawn(move || {
                let _span = actor_span.enter();
                let _guard = CriticalThreadGuard {
                    shared: actor_guard_shared,
                    expected_stop: actor_stop,
                    name: "actor thread",
                };
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("actor runtime");
                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(async move {
                    let context =
                        ProductionContext::new(move |ns| poll_stats.stats.record_actor_poll(ns));
                    context
                        .scope(async move {
                            let world = Rc::new(ProductionWorld {
                                blobs,
                                store: runtime_store,
                                peers: actor_peers.clone(),
                                local_host: actor_config.host,
                                peer_rx: peer_rx_actor,
                                fault_rx: fault_rx_actor,
                                fault_tx: actor_inputs.faults.clone(),
                                sync_rx: sync_rx_actor,
                                admin_rx: admin_rx_actor,
                                shared: Arc::clone(&actor_shared),
                                fault_work: world_fault_work,
                                shared_pages: RefCell::new(BTreeMap::new()),
                            });
                            let state = Rc::new(RefCell::new(HostState::new(actor_config.clone())));
                            let mut host_actor = blockd_exec::spawn(host_actor_with_state(
                                Rc::clone(&state),
                                Rc::clone(&world),
                            ));
                            let observation_state = Rc::clone(&state);
                            let observation_shared = Arc::clone(&actor_shared);
                            let observation_inputs = actor_inputs.clone();
                            let mut observation = blockd_exec::spawn(async move {
                                loop {
                                    observation_shared.publish_observability(
                                        &observation_state,
                                        &observation_inputs,
                                    );
                                    delay(OBSERVATION_INTERVAL_NS).await;
                                }
                            });
                            #[cfg(test)]
                            let mut injected_failure = Box::pin(actor_failure_rx.recv());
                            #[cfg(not(test))]
                            let mut injected_failure = Box::pin(std::future::pending::<
                                Option<TestActorFailure>,
                            >());
                            let unexpected = loop {
                                tokio::select! {
                                    result = &mut host_actor => {
                                        if actor_shared
                                            .planned_retirement_verified
                                            .load(Ordering::SeqCst)
                                        {
                                            break None;
                                        }
                                        break Some(("host actor", result));
                                    },
                                    result = &mut observation => break Some(("observation task", result)),
                                    _ = &mut shutdown_rx => break None,
                                    failure = &mut injected_failure => match failure {
                                        Some(TestActorFailure::HostActor) => {
                                            host_actor.cancel();
                                            break Some(("host actor", Err(blockd_exec::Cancelled)));
                                        }
                                        Some(TestActorFailure::Observation) => {
                                            observation.cancel();
                                            break Some((
                                                "observation task",
                                                Err(blockd_exec::Cancelled),
                                            ));
                                        }
                                        None => {}
                                    }
                                }
                            };
                            if let Some((task, result)) = unexpected {
                                actor_shared.fail_critical(format!(
                                    "{task} stopped unexpectedly: {result:?}"
                                ));
                            }
                            host_actor.cancel();
                            observation.cancel();
                            for _ in 0..64 {
                                tokio::task::yield_now().await;
                            }
                            let (drained, drain) = tokio::sync::oneshot::channel();
                            if enqueue_fault_work(
                                &shutdown_fault_work,
                                &actor_shared.fault_work_stats,
                                FaultWork::Barrier { done: drained },
                            )
                            .is_ok()
                            {
                                let _ = drain.await;
                            }
                            actor_shared.publish_observability(&state, &actor_inputs);
                        })
                        .await;
                }));
            });
        let actor_task = match actor_task {
            Ok(actor_task) => actor_task,
            Err(source) => {
                return startup
                    .rollback(RuntimeStartupError::ThreadSpawn {
                        thread: "actor",
                        source,
                    })
                    .await;
            }
        };
        let committed = startup.commit();

        Ok(Self {
            inputs,
            shared,
            blob_dir: config.blob_dir.clone(),
            store,
            host: config.daemon.host,
            authority_required: config
                .daemon
                .cluster_placement
                .as_ref()
                .and_then(|placement| placement.authority)
                .is_some(),
            peers: committed.peers,
            placement_worker: committed.placement_worker,
            shutdown: Some(shutdown),
            actor_expected_stop,
            actor_task: Some(actor_task),
            fault_worker: Some(committed.fault_worker),
            fault_worker_expected_stop,
            live_placement,
            #[cfg(test)]
            actor_failure,
            #[cfg(test)]
            fault_work: committed.fault_work,
        })
    }

    pub fn loop_stats(&self) -> &LoopStats {
        &self.shared.stats
    }

    pub fn loop_queue_depths(&self) -> (usize, usize) {
        self.inputs.depths()
    }

    pub fn fault_input_depths(&self) -> (usize, usize) {
        self.inputs.faults.depths()
    }

    pub fn fault_work_metrics(&self) -> FaultWorkMetrics {
        self.shared.fault_work_stats.snapshot()
    }

    pub fn fault_reader_metrics(&self) -> FaultReaderMetrics {
        self.shared.fault_reader.snapshot()
    }

    pub fn faults_in_flight(&self) -> usize {
        self.shared
            .fault_in_flight
            .lock()
            .expect("fault lock")
            .values()
            .map(VecDeque::len)
            .sum()
    }

    pub fn daemon_stats(&self) -> DaemonStats {
        self.shared.daemon_stats.lock().expect("stats lock").clone()
    }

    pub fn capacity_signal(&self) -> CapacitySignal {
        self.shared.capacity.lock().expect("capacity lock").signal()
    }

    pub fn backup_lag_age(&self) -> Vec<(VolumeId, Duration)> {
        self.shared
            .backup_lag_started
            .lock()
            .expect("lag lock")
            .iter()
            .map(|(&volume, started)| (volume, started.elapsed()))
            .collect()
    }

    pub fn active_operation_age(&self) -> Vec<(VolumeId, &'static str, Duration)> {
        self.shared
            .operation_started
            .lock()
            .expect("operation lock")
            .iter()
            .map(|(&(volume, operation), started)| {
                (volume, operation_name(operation), started.elapsed())
            })
            .collect()
    }

    pub fn fault_latency(&self) -> Vec<FaultLatency> {
        let active = self
            .shared
            .daemon_stats
            .lock()
            .expect("stats lock")
            .volumes
            .iter()
            .map(|stats| stats.volume)
            .collect::<BTreeSet<_>>();
        let volumes = self.shared.volumes.lock().expect("volume lock");
        let mut snapshots = Vec::new();
        for (&volume, host) in volumes.iter().filter(|(volume, _)| active.contains(volume)) {
            for source in FaultSource::ALL {
                snapshots.push(FaultLatency {
                    volume,
                    source: source.name(),
                    histogram: host.fault_latency[source.index()].snapshot(),
                });
            }
        }
        snapshots
    }

    pub fn operation_latency(&self) -> Vec<LatencySeries> {
        latency_snapshots(
            &self.shared.operation_latency,
            &OPERATION_NAMES,
            &OPERATION_OUTCOMES,
        )
    }

    pub fn local_io_latency(&self) -> Vec<LatencySeries> {
        latency_snapshots(
            &self.shared.local_io_latency,
            &LOCAL_IO_NAMES,
            &LOCAL_IO_OUTCOMES,
        )
    }

    pub fn local_io_in_flight(&self) -> Vec<(&'static str, u64)> {
        LOCAL_IO_NAMES
            .iter()
            .zip(&self.shared.local_io_in_flight)
            .map(|(operation, value)| (*operation, value.load(Ordering::Relaxed)))
            .collect()
    }

    pub fn guest_pause_latency(&self) -> Vec<LatencySeries> {
        PAUSE_NAMES
            .iter()
            .zip(&self.shared.pause_latency)
            .map(|(operation, histogram)| LatencySeries {
                operation,
                outcome: "success",
                histogram: histogram.snapshot(),
            })
            .collect()
    }

    pub fn peer_dropped_sends(&self) -> u64 {
        self.peers
            .as_ref()
            .map_or(0, |peers| peers.dropped_sends.load(Ordering::SeqCst))
    }

    pub fn peer_overload_rejections(&self) -> u64 {
        self.peers
            .as_ref()
            .map_or(0, |peers| peers.overload_rejections())
    }

    pub fn peer_connections(&self) -> Vec<(HostId, bool)> {
        self.peers
            .as_ref()
            .map_or_else(Vec::new, |peers| peers.connections())
    }

    #[allow(clippy::too_many_lines)] // readiness, error, tracing, and injection are one loop
    fn spawn_fault_reader(&self, volume: VolumeId, host: Arc<VolumeHost>) {
        drop(Self::spawn_fault_reader_with(
            &self.shared,
            self.inputs.faults.clone(),
            volume,
            host,
        ));
    }

    async fn start_fault_reader(&self, volume: VolumeId, host: Arc<VolumeHost>) -> bool {
        Self::spawn_fault_reader_with(&self.shared, self.inputs.faults.clone(), volume, host)
            .await
            .unwrap_or(false)
    }

    #[allow(clippy::too_many_lines)] // readiness, error, tracing, and injection are one loop
    fn spawn_fault_reader_with(
        shared: &Arc<Shared>,
        faults: Injector<GuestFault>,
        volume: VolumeId,
        host: Arc<VolumeHost>,
    ) -> tokio::sync::oneshot::Receiver<bool> {
        let (started, startup) = tokio::sync::oneshot::channel();
        let Some(uffd) = host.uffd.clone() else {
            let _ = started.send(false);
            return startup;
        };
        if let Err(error) = uffd.set_nonblocking(true) {
            shared.fail_critical(format!(
                "fault reader nonblocking setup failed for {volume:?}: {error}"
            ));
            let _ = started.send(false);
            return startup;
        }
        shared
            .fault_reader
            .readers_started
            .fetch_add(1, Ordering::Relaxed);
        let expected_stop = Arc::new(AtomicBool::new(false));
        let reader_expected_stop = Arc::clone(&expected_stop);
        let ready = Arc::new(AtomicBool::new(false));
        let reader_ready = Arc::clone(&ready);
        let reader_shared = Arc::clone(shared);
        let volume_span = tracing::info_span!("blockd.volume", volume_id = volume.0);
        let task = tokio::spawn(
            async move {
                let _active = ActiveFaultReader {
                    shared: Arc::clone(&reader_shared),
                    expected_stop: reader_expected_stop,
                };
                #[cfg(test)]
                let test_start = reader_shared
                    .fault_reader_start
                    .lock()
                    .expect("fault reader start lock")
                    .take();
                #[cfg(test)]
                if let Some(start) = test_start {
                    start.entered.notify_one();
                    start.release.notified().await;
                    if start.fail {
                        reader_shared.fail_critical(format!(
                            "fault reader registration failed for {volume:?}: injected failure"
                        ));
                        let _ = started.send(false);
                        return;
                    }
                }
                let uffd = match AsyncFd::new(SharedUffd(uffd)) {
                    Ok(uffd) => uffd,
                    Err(error) => {
                        reader_shared.fail_critical(format!(
                            "fault reader registration failed for {volume:?}: {error}"
                        ));
                        let _ = started.send(false);
                        return;
                    }
                };
                reader_ready.store(true, Ordering::SeqCst);
                let _ = started.send(true);
                loop {
                    let mut ready = match uffd.readable().await {
                        Ok(ready) => ready,
                        Err(error) => {
                            reader_shared
                                .fault_reader
                                .terminal_errors
                                .fetch_add(1, Ordering::Relaxed);
                            reader_shared.fail_critical(format!(
                                "fault reader readiness failed for {volume:?}: {error}"
                            ));
                            return;
                        }
                    };
                    loop {
                        let events = match ready.try_io(|inner| inner.get_ref().0.read_events()) {
                            Ok(Ok(events)) => events,
                            Ok(Err(error)) => {
                                reader_shared
                                    .fault_reader
                                    .terminal_errors
                                    .fetch_add(1, Ordering::Relaxed);
                                reader_shared.fail_critical(format!(
                                    "fault reader failed for {volume:?}: {error}"
                                ));
                                return;
                            }
                            Err(_) => break,
                        };
                        reader_shared.fault_reader.events_read.fetch_add(
                            u64::try_from(events.len()).unwrap_or(u64::MAX),
                            Ordering::Relaxed,
                        );
                        for event in events {
                            let page = host.page_of_addr(event.address & !(page_size() - 1));
                            let span = tracing::debug_span!(
                                "page.fault",
                                volume_id = volume.0,
                                page = page.page.0,
                                write = event.write,
                                wp = event.wp,
                                minor = event.minor,
                                source = tracing::field::Empty,
                                outcome = tracing::field::Empty,
                                duration_ms = tracing::field::Empty,
                            );
                            reader_shared
                                .fault_in_flight
                                .lock()
                                .expect("fault lock")
                                .entry(page)
                                .or_default()
                                .push_back(FaultInFlight {
                                    started: Instant::now(),
                                    span,
                                });
                            if faults
                                .push(
                                    Lane::Critical,
                                    GuestFault {
                                        page,
                                        write: event.write,
                                        wp: event.wp,
                                        minor: event.minor,
                                    },
                                )
                                .is_err()
                            {
                                reader_shared
                                    .fault_reader
                                    .injection_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                reader_shared.fail_critical(format!(
                                    "fault reader injection failed for {volume:?}"
                                ));
                                return;
                            }
                            reader_shared
                                .fault_reader
                                .events_injected
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            .instrument(volume_span),
        );
        if let Some(previous) = shared
            .fault_readers
            .lock()
            .expect("fault reader lock")
            .insert(
                volume,
                FaultReaderTask {
                    task,
                    expected_stop,
                    ready,
                },
            )
        {
            drop(previous.stop());
        }
        startup
    }

    #[cfg(test)]
    fn stop_fault_reader_unexpectedly_for_test(&self, volume: VolumeId) {
        if let Some(reader) = self
            .shared
            .fault_readers
            .lock()
            .expect("fault reader lock")
            .get(&volume)
        {
            reader.task.abort();
        }
    }

    fn req(&self) -> ReqId {
        ReqId(self.shared.next_req.fetch_add(1, Ordering::SeqCst))
    }

    async fn admin_request(&self, call: AdminCall) -> AdminResult {
        let (request, reply) = request(call);
        self.inputs
            .admin
            .push(Lane::Background, request)
            .unwrap_or_else(|_| panic!("actor host alive"));
        tokio::time::timeout(Duration::from_secs(30), reply)
            .await
            .expect("admin reply within 30 seconds")
            .expect("actor host alive")
    }

    async fn wait_admin_event<T>(&self, mut want: impl FnMut(&AdminEvent) -> Option<T>) -> T {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let notified = self.shared.admin_event_ready.notified();
                if let Some(output) = {
                    let mut events = self.shared.admin_events.lock().expect("admin event lock");
                    events
                        .iter()
                        .enumerate()
                        .find_map(|(index, event)| want(event).map(|output| (index, output)))
                        .map(|(index, output)| {
                            events.remove(index);
                            output
                        })
                } {
                    return output;
                }
                notified.await;
            }
        })
        .await
        .expect("admin event within 30 seconds")
    }

    pub async fn try_create_volume(
        &self,
        volume: VolumeId,
        config: VolumeConfig,
    ) -> Result<(), blockd_core::protocol::AdminError> {
        let started = Instant::now();
        let result = match self
            .admin_request(AdminCall::CreateVolume {
                volume,
                config,
                from_base: None,
            })
            .await
        {
            Ok(AdminSuccess::VolumeCreated { volume: found }) if found == volume => Ok(()),
            Err(error) => Err(error),
            result => panic!("unexpected create result: {result:?}"),
        };
        self.observe_operation(0, result.is_ok(), started.elapsed());
        if result.is_ok() {
            self.install_volume_host(volume, config, true);
        }
        result
    }

    pub async fn create_volume(&self, volume: VolumeId, config: VolumeConfig) {
        self.try_create_volume(volume, config)
            .await
            .expect("volume creation failed");
    }

    pub async fn keep_base(&self, volume: VolumeId, base: u64) {
        match self
            .admin_request(AdminCall::KeepBase { volume, base })
            .await
        {
            Ok(AdminSuccess::BaseKept { base: found }) if found == base => {}
            result => panic!("base retention failed: {result:?}"),
        }
    }

    pub async fn fork_volume(&self, volume: VolumeId, config: VolumeConfig, base: u64) -> Verdict {
        let started = Instant::now();
        self.install_volume_host(volume, config, true);
        let result = match self
            .admin_request(AdminCall::CreateVolume {
                volume,
                config,
                from_base: Some(base),
            })
            .await
        {
            Ok(AdminSuccess::VolumeForked {
                volume: found,
                verdict,
            }) if found == volume => Some(verdict),
            Err(_) => None,
            result => panic!("unexpected fork result: {result:?}"),
        };
        self.observe_operation(5, result.is_some(), started.elapsed());
        result.expect("volume fork failed")
    }

    pub async fn delete_base(&self, base: u64) {
        match self.admin_request(AdminCall::DeleteBase { base }).await {
            Ok(AdminSuccess::BaseDeleted { base: found }) if found == base => {}
            result => panic!("base deletion failed: {result:?}"),
        }
    }

    pub async fn checkpoint(&self, volume: VolumeId) -> u64 {
        let started = Instant::now();
        let req = self.req();
        let pauses_guest = self.host(volume).config.kind == VolumeKind::Memory;
        if pauses_guest {
            self.expect_pause(volume, 0);
        }
        let result = match self
            .admin_request(AdminCall::Checkpoint { retry: req, volume })
            .await
        {
            Ok(AdminSuccess::CheckpointDone { epoch, .. }) => Some(epoch.0),
            Err(_) => None,
            result => panic!("unexpected checkpoint result: {result:?}"),
        };
        self.observe_operation(1, result.is_some(), started.elapsed());
        if pauses_guest && result.is_none() {
            self.cancel_expected_pause(volume, 0);
        }
        result.expect("checkpoint failed")
    }

    pub async fn restore_volume(&self, volume: VolumeId, config: VolumeConfig) -> Verdict {
        self.shared.recovery_complete.store(false, Ordering::SeqCst);
        let started = Instant::now();
        self.install_volume_host(volume, config, false);
        let result = match self
            .admin_request(AdminCall::RestoreVolume { volume })
            .await
        {
            Ok(AdminSuccess::VolumeRestored { verdict, .. }) => Some(verdict),
            Err(_) => None,
            result => panic!("unexpected restore result: {result:?}"),
        };
        self.observe_operation(2, result.is_some(), started.elapsed());
        let verdict = match result {
            Some(verdict) if !matches!(verdict, Verdict::Unrestorable) => {
                let host = self.host(volume);
                let reader_ready = self.start_fault_reader(volume, host).await;
                let mut quarantines = self.shared.quarantines.lock().expect("quarantine lock");
                if reader_ready {
                    quarantines.remove(&volume);
                } else {
                    quarantines.insert(
                        volume,
                        "runtime fault service failed to register; retain local artifacts and keep the volume unavailable"
                            .to_owned(),
                    );
                }
                verdict
            }
            Some(Verdict::Unrestorable) | None => {
                self.shared.quarantines.lock().expect("quarantine lock").insert(
                    volume,
                    "operator restore did not prove an intact recovery point; retain artifacts and retry repair"
                        .to_owned(),
                );
                Verdict::Unrestorable
            }
            Some(_) => unreachable!("all restorable verdicts handled"),
        };
        let recovery_healthy = self
            .shared
            .quarantines
            .lock()
            .expect("quarantine lock")
            .is_empty();
        self.shared
            .recovery_complete
            .store(recovery_healthy, Ordering::SeqCst);
        verdict
    }

    pub async fn wait_recovered(&self, volume: VolumeId) -> Verdict {
        self.wait_admin_event(|event| match event {
            AdminEvent::VolumeRecovered {
                volume: found,
                verdict,
            } if *found == volume => Some(*verdict),
            _ => None,
        })
        .await
    }

    pub fn expect_migration(&self, volume: VolumeId, config: VolumeConfig) {
        self.install_volume_host(volume, config, true);
    }

    fn install_volume_host(
        &self,
        volume: VolumeId,
        config: VolumeConfig,
        start_fault_reader: bool,
    ) {
        assert_peer_stash_transport(
            config,
            self.peers
                .as_ref()
                .is_some_and(|peers| peers.authenticated()),
        );
        let host = VolumeHost::new(volume, config);
        self.shared
            .volumes
            .lock()
            .expect("volume lock")
            .insert(volume, Arc::clone(&host));
        if start_fault_reader {
            self.spawn_fault_reader(volume, host);
        }
    }

    pub async fn try_migrate_out(&self, volume: VolumeId, to: HostId) -> Result<(), AdminError> {
        let started = Instant::now();
        let pauses_guest = self.host(volume).config.kind == VolumeKind::Memory;
        if pauses_guest {
            self.expect_pause(volume, 1);
        }
        let result = match self
            .admin_request(AdminCall::MigrateOut { volume, to })
            .await
        {
            Ok(AdminSuccess::MigratedOut { .. }) => Ok(()),
            Err(error) => Err(error),
            result => panic!("unexpected migration result: {result:?}"),
        };
        self.observe_operation(3, result.is_ok(), started.elapsed());
        if pauses_guest {
            if result.is_ok() {
                self.shared.complete_pause(volume);
            } else {
                self.cancel_expected_pause(volume, 1);
            }
        }
        result
    }

    pub async fn migrate_out(&self, volume: VolumeId, to: HostId) {
        self.try_migrate_out(volume, to)
            .await
            .expect("migrate out failed");
    }

    pub async fn wait_migrated_in(&self, volume: VolumeId) -> Verdict {
        self.wait_admin_event(|event| match event {
            AdminEvent::VolumeMigratedIn {
                volume: found,
                verdict,
                ..
            } if *found == volume => Some(*verdict),
            _ => None,
        })
        .await
    }

    pub async fn wait_volume_released(&self, volume: VolumeId) {
        loop {
            let notified = self.shared.volume_released.notified();
            if self
                .shared
                .released_volumes
                .lock()
                .expect("released volume lock")
                .remove(&volume)
            {
                return;
            }
            notified.await;
        }
    }

    pub fn counters(&self) -> Counters {
        *self.shared.counters.lock().expect("counter lock")
    }

    pub fn replica_metrics(&self) -> Vec<ReplicaVolumeMetrics> {
        self.shared
            .replica_metrics
            .lock()
            .expect("replica metric lock")
            .clone()
    }

    pub fn replica_spool_metrics(&self) -> Vec<ReplicaSpoolMetrics> {
        self.shared
            .replica_spool_metrics
            .lock()
            .expect("replica spool metric lock")
            .clone()
    }

    pub fn incidents(&self) -> Vec<String> {
        self.shared.incidents.lock().expect("incident lock").clone()
    }

    pub fn quarantines(&self) -> BTreeMap<VolumeId, String> {
        self.shared
            .quarantines
            .lock()
            .expect("quarantine lock")
            .clone()
    }

    pub fn volume_inventory(&self) -> Vec<(VolumeId, VolumeConfig, bool)> {
        let quarantines = self.shared.quarantines.lock().expect("quarantine lock");
        self.shared
            .volumes
            .lock()
            .expect("volume lock")
            .iter()
            .map(|(&volume, host)| (volume, host.config, quarantines.contains_key(&volume)))
            .collect()
    }

    /// Permanently remove one quarantined volume after recording immutable
    /// operator intent and completion records beside the local blob store.
    ///
    /// The intent record is synced before any artifact is unlinked. If the
    /// process crashes during cleanup, the surviving intent and any remaining
    /// artifacts make the interrupted operation explicit and retryable.
    pub async fn discard_quarantine(
        &self,
        volume: VolumeId,
        operator_reason: &str,
    ) -> Result<String, String> {
        let operator_reason = operator_reason.trim();
        if operator_reason.is_empty() {
            return Err("an operator reason is required".to_owned());
        }
        if operator_reason.len() > 4_096 {
            return Err("operator reason exceeds 4096 bytes".to_owned());
        }

        let _cleanup = self.shared.quarantine_cleanup.lock().await;
        let quarantine_reason = self
            .shared
            .quarantines
            .lock()
            .expect("quarantine lock")
            .get(&volume)
            .cloned()
            .ok_or_else(|| "volume is not quarantined".to_owned())?;

        let blobs = FileBlobs::new(&self.blob_dir);
        let mut artifacts = blobs
            .scan()
            .await
            .map_err(|error| format!("scan quarantined artifacts: {error:?}"))?
            .into_iter()
            .filter_map(|blob| {
                let belongs = match blockd_core::layout::parse_blob(&blob.name)? {
                    BlobName::Journal { volume: found, .. }
                    | BlobName::Blx { volume: found, .. }
                    | BlobName::Handoff { volume: found }
                    | BlobName::ReplicaSpool { volume: found, .. } => found == volume,
                };
                belongs.then_some(blob.name)
            })
            .collect::<Vec<_>>();
        artifacts.sort();

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| "system clock precedes Unix epoch".to_owned())?
            .as_millis();
        let sequence = self.shared.next_req.fetch_add(1, Ordering::Relaxed);
        let audit_id = format!("{timestamp_ms:032x}-{sequence:016x}");
        let audit_root = format!("quarantine-audit/{:016x}/{audit_id}", volume.0);
        let intent = serde_json::to_vec(&serde_json::json!({
            "schema": 1,
            "audit_id": audit_id,
            "phase": "intent",
            "volume": volume.0,
            "quarantine_reason": quarantine_reason,
            "operator_reason": operator_reason,
            "artifacts": artifacts,
        }))
        .map_err(|error| format!("encode cleanup audit: {error}"))?;
        blobs
            .write(format!("{audit_root}.intent.json"), intent)
            .await
            .map_err(|error| format!("persist cleanup intent: {error:?}"))?;
        blobs
            .delete_many_durable(&artifacts)
            .await
            .map_err(|error| format!("delete quarantined artifacts: {error:?}"))?;

        let completion = serde_json::to_vec(&serde_json::json!({
            "schema": 1,
            "audit_id": audit_id,
            "phase": "complete",
            "volume": volume.0,
            "deleted_artifacts": artifacts.len(),
        }))
        .map_err(|error| format!("encode cleanup completion: {error}"))?;
        blobs
            .write(format!("{audit_root}.complete.json"), completion)
            .await
            .map_err(|error| format!("persist cleanup completion: {error:?}"))?;

        self.shared
            .quarantines
            .lock()
            .expect("quarantine lock")
            .remove(&volume);
        self.shared
            .volumes
            .lock()
            .expect("volume lock")
            .remove(&volume);
        let recovery_healthy = self
            .shared
            .quarantines
            .lock()
            .expect("quarantine lock")
            .is_empty();
        self.shared
            .recovery_complete
            .store(recovery_healthy, Ordering::SeqCst);
        Ok(audit_id)
    }

    pub fn readiness(&self) -> RuntimeReadiness {
        let peer_listener = self
            .peers
            .as_ref()
            .is_none_or(|peers| peers.listener_healthy());
        let peer_tasks = self.peers.as_ref().is_none_or(|peers| peers.healthy());
        let (placement_current, expected_placement_epoch) = self.live_placement.readiness();
        RuntimeReadiness {
            authority: !self.authority_required
                || (self
                    .shared
                    .authority_identity
                    .lock()
                    .expect("authority identity lock")
                    .is_some()
                    && self.authority_control_ready()),
            membership_ownership: self
                .peers
                .as_ref()
                .is_none_or(|peers| peers.membership_owned()),
            placement: placement_current
                && self.shared.cluster_placement_epoch.load(Ordering::SeqCst)
                    == expected_placement_epoch,
            recovery: self.shared.recovery_complete.load(Ordering::SeqCst),
            peer_listener,
            critical_tasks: self.shared.critical_healthy.load(Ordering::SeqCst) && peer_tasks,
            unfenced: self.incidents().iter().all(|incident| {
                !incident.starts_with("host failure:") && !incident.starts_with("fenced:")
            }),
        }
    }

    pub fn authority_session_ready(&self) -> bool {
        !self.authority_required
            || self
                .shared
                .authority_identity
                .lock()
                .expect("authority identity lock")
                .is_some()
    }

    pub fn authority_control_ready(&self) -> bool {
        let (_, expected_placement_epoch) = self.live_placement.readiness();
        !self.authority_required
            || (self.shared.authority_placement_epoch.load(Ordering::SeqCst) != 0
                && self.shared.authority_placement_epoch.load(Ordering::SeqCst)
                    == expected_placement_epoch)
    }

    pub fn is_ready(&self) -> bool {
        self.readiness().ready()
    }

    pub fn peer_resource_metrics(&self) -> PeerResourceMetrics {
        self.peers
            .as_ref()
            .map_or_else(PeerResourceMetrics::default, |peers| {
                peers.resource_metrics()
            })
    }

    pub fn replica_spool_capacity_bytes(&self) -> u64 {
        self.shared.replica_spool_capacity_bytes
    }

    pub async fn critical_failure(&self) {
        let local = self.shared.critical_failed.notified();
        if !self.shared.critical_healthy.load(Ordering::SeqCst) {
            return;
        }
        if let Some(peers) = &self.peers {
            tokio::select! {
                () = local => {},
                () = peers.critical_failure() => {},
            }
        } else {
            local.await;
        }
    }

    #[cfg(test)]
    fn inject_actor_task_failure(&self, failure: TestActorFailure) {
        self.actor_failure
            .send(failure)
            .expect("actor failure injector alive");
    }

    #[cfg(test)]
    fn inject_fault_worker_failure(&self) {
        enqueue_fault_work(
            &self.fault_work,
            &self.shared.fault_work_stats,
            FaultWork::CrashDispatcher,
        )
        .expect("fault worker failure injector alive");
    }

    pub async fn publish_drained(&self) -> Result<(), blockd_core::protocol::StoreFault> {
        match &self.peers {
            Some(peers) => peers.publish_drained().await,
            None => Ok(()),
        }
    }

    pub async fn await_authority_transfer(&self) -> Result<(), String> {
        if !self.authority_required {
            return Ok(());
        }
        loop {
            match Arc::clone(&self.store)
                .get(blockd_core::layout::placement_key())
                .await
            {
                Ok(Some((_, bytes))) => {
                    let placement = ClusterPlacement::decode(&bytes)
                        .ok_or_else(|| "authority placement is corrupt during drain".to_owned())?;
                    if !placement.contains(self.host) {
                        return Ok(());
                    }
                }
                Ok(None) => return Err("authority placement disappeared during drain".to_owned()),
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn relinquish_authority(&self) -> Result<(), String> {
        let identity = *self
            .shared
            .authority_identity
            .lock()
            .expect("authority identity lock");
        let Some((session, epoch)) = identity else {
            return if self.authority_required {
                Err("authority session was not established before drain".to_owned())
            } else {
                Ok(())
            };
        };
        let key = blockd_core::layout::host_session_key(self.host);
        let Some((generation, encoded)) = Arc::clone(&self.store)
            .get(key.clone())
            .await
            .map_err(|error| format!("read host session during drain: {error:?}"))?
        else {
            return Err("host session disappeared during drain".to_owned());
        };
        let record = blockd_core::authority::HostSessionRecord::decode(&encoded)
            .map_err(|_| "host session is corrupt during drain".to_owned())?;
        if !matches!(
            record,
            blockd_core::authority::HostSessionRecord::Active {
                session: found,
                epoch: found_epoch,
            } if found == session && found_epoch == epoch
        ) {
            return Err("host session was replaced before drain".to_owned());
        }
        let nonce = self.shared.next_req.fetch_add(1, Ordering::SeqCst).max(1);
        let retired = record
            .retire(session, nonce)
            .map_err(|_| "host session could not be retired".to_owned())?;
        *self
            .shared
            .planned_retirement
            .lock()
            .expect("planned retirement lock") = Some(retired);
        let result = Arc::clone(&self.store)
            .put_cas(key, Some(generation), retired.encode())
            .await;
        if let Err(error) = result {
            *self
                .shared
                .planned_retirement
                .lock()
                .expect("planned retirement lock") = None;
            return Err(format!("retire host session during drain: {error:?}"));
        }
        Ok(())
    }

    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
    }

    pub fn host_id(&self) -> HostId {
        self.host
    }

    pub fn blob_filesystem_space(&self) -> Option<(u64, u64)> {
        let stats = rustix::fs::statvfs(&self.blob_dir).ok()?;
        Some((
            stats.f_blocks.saturating_mul(stats.f_frsize),
            stats.f_bavail.saturating_mul(stats.f_frsize),
        ))
    }

    fn host(&self, volume: VolumeId) -> Arc<VolumeHost> {
        self.shared.volumes.lock().expect("volume lock")[&volume].clone()
    }

    pub fn guest_access(&self, volume: VolumeId) -> GuestAccess {
        GuestAccess {
            host: self.host(volume),
        }
    }

    pub async fn guest_write(&self, volume: VolumeId, page: PageId, value: u64) {
        let host = self.host(volume);
        host.op_start().await;
        tokio::task::spawn_blocking(move || {
            host.view.write_word(VolumeHost::page_index(page), value);
            host.op_end();
        })
        .await
        .expect("guest write worker");
    }

    pub async fn guest_read(&self, volume: VolumeId, page: PageId) -> Vec<u8> {
        let host = self.host(volume);
        host.op_start().await;
        tokio::task::spawn_blocking(move || {
            let bytes = host.view.read_page(VolumeHost::page_index(page));
            host.op_end();
            bytes
        })
        .await
        .expect("guest read worker")
    }

    pub async fn guest_sync(&self, volume: VolumeId) -> bool {
        let started = Instant::now();
        let host = self.host(volume);
        host.op_start().await;
        let req = self.req();
        let (request, reply) = request(GuestSync { req, volume });
        self.inputs
            .syncs
            .push(Lane::Critical, request)
            .unwrap_or_else(|_| panic!("actor host alive"));
        let ok = tokio::time::timeout(Duration::from_secs(30), reply)
            .await
            .expect("sync reply within 30 seconds")
            .expect("actor host alive");
        host.op_end();
        self.observe_operation(4, ok, started.elapsed());
        ok
    }

    pub fn guest_applied(&self, volume: VolumeId) -> u64 {
        self.host(volume)
            .ctl
            .state
            .lock()
            .expect("guest control lock")
            .applied
    }

    pub fn guest_resident_bytes(&self, volume: VolumeId) -> usize {
        self.host(volume)
            .region
            .resident_bytes()
            .expect("resident byte query")
    }

    fn observe_operation(&self, operation: usize, success: bool, elapsed: Duration) {
        self.shared.operation_latency[operation][usize::from(!success)].observe(elapsed);
    }

    fn expect_pause(&self, volume: VolumeId, operation: usize) {
        self.shared
            .pause_expected
            .lock()
            .expect("pause lock")
            .entry(volume)
            .or_default()
            .push_back(operation);
    }

    fn cancel_expected_pause(&self, volume: VolumeId, operation: usize) {
        let mut expected = self.shared.pause_expected.lock().expect("pause lock");
        if let Some(queue) = expected.get_mut(&volume)
            && let Some(position) = queue.iter().position(|candidate| *candidate == operation)
        {
            queue.remove(position);
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), blockd_core::protocol::StoreFault> {
        if let Some(worker) = self.placement_worker.take() {
            worker.abort();
            let _ = worker.await;
        }
        self.actor_expected_stop.store(true, Ordering::SeqCst);
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(actor_task) = self.actor_task.take() {
            tokio::task::spawn_blocking(move || actor_task.join())
                .await
                .map_err(|_| blockd_core::protocol::StoreFault::Unavailable)?
                .map_err(|_| blockd_core::protocol::StoreFault::Unavailable)?;
        }
        #[cfg(test)]
        {
            let (disconnected, receiver) = tokio::sync::mpsc::unbounded_channel();
            drop(receiver);
            drop(std::mem::replace(&mut self.fault_work, disconnected));
        }
        if let Some(fault_worker) = self.fault_worker.take() {
            self.fault_worker_expected_stop
                .store(true, Ordering::SeqCst);
            tokio::task::spawn_blocking(move || fault_worker.join())
                .await
                .map_err(|_| blockd_core::protocol::StoreFault::Unavailable)?
                .map_err(|_| blockd_core::protocol::StoreFault::Unavailable)?;
        }
        let readers =
            std::mem::take(&mut *self.shared.fault_readers.lock().expect("fault reader lock"));
        for (_, reader) in readers {
            let reader = reader.stop();
            let _ = reader.await;
        }
        if let Some(peers) = &self.peers {
            peers.shutdown().await?;
        }
        Ok(())
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(worker) = self.placement_worker.take() {
            worker.abort();
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(actor_task) = self.actor_task.take() {
            let _ = actor_task.join();
        }
        for (_, reader) in
            std::mem::take(&mut *self.shared.fault_readers.lock().expect("fault reader lock"))
        {
            drop(reader.stop());
        }
        self.fault_worker.take();
    }
}

fn latency_snapshots<const N: usize>(
    histograms: &[[AtomicHistogram; 2]; N],
    names: &[&'static str; N],
    outcomes: &[&'static str; 2],
) -> Vec<LatencySeries> {
    let mut snapshots = Vec::new();
    for (operation, name) in names.iter().enumerate() {
        for (outcome, outcome_name) in outcomes.iter().enumerate() {
            snapshots.push(LatencySeries {
                operation: name,
                outcome: outcome_name,
                histogram: histograms[operation][outcome].snapshot(),
            });
        }
    }
    snapshots
}

fn peer_lane(message: &PeerMsg) -> Lane {
    if matches!(message, PeerMsg::Page { .. } | PeerMsg::FetchRange { .. }) {
        Lane::Critical
    } else {
        Lane::Background
    }
}

fn elapsed_ns(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

impl Shared {
    fn complete_fault(
        &self,
        host: Option<&VolumeHost>,
        page: PageId,
        source: FaultSource,
        outcome: &'static str,
    ) {
        let fault = {
            let mut pending = self.fault_in_flight.lock().expect("fault lock");
            let fault = pending.get_mut(&page).and_then(VecDeque::pop_front);
            if pending.get(&page).is_some_and(VecDeque::is_empty) {
                pending.remove(&page);
            }
            fault
        };
        let Some(fault) = fault else {
            return;
        };
        let elapsed = fault.started.elapsed();
        if let Some(host) = host {
            host.fault_latency[source.index()].observe(elapsed);
        }
        fault.span.record("source", source.name());
        fault.span.record("outcome", outcome);
        fault
            .span
            .record("duration_ms", elapsed.as_secs_f64() * 1000.0);
    }

    fn complete_pause(&self, volume: VolumeId) {
        if let Some((operation, started)) = self
            .pause_in_flight
            .lock()
            .expect("pause lock")
            .remove(&volume)
        {
            self.pause_latency[operation].observe(started.elapsed());
        }
    }

    fn begin_pause(&self, volume: VolumeId) {
        let operation = self
            .pause_expected
            .lock()
            .expect("pause lock")
            .get_mut(&volume)
            .and_then(VecDeque::pop_front);
        if let Some(operation) = operation {
            self.pause_in_flight
                .lock()
                .expect("pause lock")
                .insert(volume, (operation, Instant::now()));
        }
    }
}

const BACKGROUND_OPERATIONS: [u8; 4] = [
    VolumeOperations::CAPTURE,
    VolumeOperations::CHECKPOINT,
    VolumeOperations::BACKUP,
    VolumeOperations::HYDRATION,
];

fn operation_name(operation: u8) -> &'static str {
    match operation {
        VolumeOperations::CAPTURE => "capture",
        VolumeOperations::CHECKPOINT => "checkpoint",
        VolumeOperations::BACKUP => "backup",
        VolumeOperations::HYDRATION => "hydration",
        _ => unreachable!("known background operation"),
    }
}

impl Shared {
    fn publish_observability(&self, state: &Rc<RefCell<HostState>>, inputs: &Inputs) {
        let state = state.borrow();
        self.cluster_placement_epoch.store(
            state
                .config
                .cluster_placement
                .as_ref()
                .map_or(0, |placement| placement.membership_epoch),
            Ordering::SeqCst,
        );
        let daemon = state.stats();
        *self
            .authority_identity
            .lock()
            .expect("authority identity lock") = if state.authority_serving() {
            state
                .authority_session()
                .map(|session| (session, state.authority_host_epoch()))
        } else {
            None
        };
        self.authority_placement_epoch
            .store(state.authority_placement_epoch(), Ordering::SeqCst);
        *self.counters.lock().expect("counter lock") = state.counters;
        *self.daemon_stats.lock().expect("stats lock") = daemon.clone();
        *self.replica_metrics.lock().expect("replica metric lock") = state.replica_metrics();
        *self
            .replica_spool_metrics
            .lock()
            .expect("replica spool metric lock") = state.replica_spool_metrics();
        drop(state);
        self.update_backup_lag(&daemon);
        self.update_active_operations(&daemon);
        self.update_capacity_signal(&daemon, inputs);
    }

    fn update_backup_lag(&self, stats: &DaemonStats) {
        let now = Instant::now();
        let lagging = stats
            .volumes
            .iter()
            .filter(|volume| volume.archive_lag_captures.is_some_and(|lag| lag > 0))
            .map(|volume| volume.volume)
            .collect::<BTreeSet<_>>();
        let mut started = self.backup_lag_started.lock().expect("lag lock");
        started.retain(|volume, _| lagging.contains(volume));
        for volume in lagging {
            started.entry(volume).or_insert(now);
        }
    }

    fn update_active_operations(&self, stats: &DaemonStats) {
        let now = Instant::now();
        let active = stats
            .volumes
            .iter()
            .flat_map(|volume| {
                BACKGROUND_OPERATIONS
                    .into_iter()
                    .filter(move |operation| volume.operations.active(*operation))
                    .map(move |operation| (volume.volume, operation))
            })
            .collect::<BTreeSet<_>>();
        let mut started = self.operation_started.lock().expect("operation lock");
        started.retain(|operation, _| active.contains(operation));
        for operation in active {
            started.entry(operation).or_insert(now);
        }
    }

    fn update_capacity_signal(&self, daemon: &DaemonStats, actor_inputs: &Inputs) {
        let local_io_in_flight = self
            .local_io_in_flight
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .sum();
        let oldest_backup_lag = self
            .backup_lag_started
            .lock()
            .expect("lag lock")
            .values()
            .map(Instant::elapsed)
            .max()
            .unwrap_or_default();
        let replica_metrics = self.replica_metrics.lock().expect("replica metric lock");
        let stash_missing = replica_metrics
            .iter()
            .any(|metric| metric.assignment_epoch.is_none() || metric.active_peer.is_none());
        let stash_replacement_active = replica_metrics
            .iter()
            .any(|metric| metric.transition_peer.is_some());
        drop(replica_metrics);
        let spool_metrics = self
            .replica_spool_metrics
            .lock()
            .expect("replica spool metric lock");
        let (peer_spool_used_bytes, peer_spool_capacity_bytes) = spool_metrics
            .iter()
            .filter(|metric| metric.host_capacity_bytes > 0)
            .max_by(|left, right| {
                (u128::from(left.stored_bytes) * u128::from(right.host_capacity_bytes))
                    .cmp(&(u128::from(right.stored_bytes) * u128::from(left.host_capacity_bytes)))
            })
            .map_or((0, 0), |metric| {
                (metric.stored_bytes, metric.host_capacity_bytes)
            });
        drop(spool_metrics);
        let (critical_queue_depth, background_queue_depth) = actor_inputs.depths();
        let inputs = CapacityInputs {
            cache_capacity_pages: daemon.cache_capacity_pages,
            cache_used_pages: daemon
                .resident_pages
                .saturating_add(daemon.shared_resident_pages)
                .saturating_add(daemon.reserved_pages),
            dirty_pages: daemon.dirty_pages,
            pressure_waiting_faults: daemon.pressure_waiting_faults,
            disk_used_bytes: daemon.local_blob_bytes,
            disk_capacity_bytes: daemon.disk_capacity_bytes,
            disk_headroom_bytes: daemon.disk_headroom_bytes,
            local_io_in_flight,
            actor_busy_ns: self.stats.actor_busy_ns(),
            actor_idle_ns: self.stats.actor_idle_ns(),
            critical_queue_depth,
            background_queue_depth,
            oldest_backup_lag,
            peer_spool_used_bytes,
            peer_spool_capacity_bytes,
            stash_missing,
            stash_replacement_active,
        };
        self.capacity.lock().expect("capacity lock").observe(inputs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BlockFirstPlacementRead {
        inner: Arc<dyn ObjectStore>,
        blocked: AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl BlockFirstPlacementRead {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                blocked: AtomicBool::new(false),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for BlockFirstPlacementRead {
        async fn put(
            self: Arc<Self>,
            key: String,
            bytes: Vec<u8>,
        ) -> Result<u64, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.inner).put(key, bytes).await
        }

        async fn put_cas(
            self: Arc<Self>,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.inner).put_cas(key, expected, bytes).await
        }

        async fn get(self: Arc<Self>, key: String) -> crate::GetResult {
            if key == blockd_core::layout::placement_key()
                && !self.blocked.swap(true, Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Arc::clone(&self.inner).get(key).await
        }

        async fn get_range(
            self: Arc<Self>,
            key: String,
            offset: u64,
            len: u64,
        ) -> crate::GetResult {
            Arc::clone(&self.inner).get_range(key, offset, len).await
        }

        async fn delete(
            self: Arc<Self>,
            key: String,
        ) -> Result<bool, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.inner).delete(key).await
        }

        async fn delete_cas(
            self: Arc<Self>,
            key: String,
            expected: u64,
        ) -> Result<bool, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.inner).delete_cas(key, expected).await
        }

        async fn list_prefix(
            self: Arc<Self>,
            prefix: String,
        ) -> Result<Vec<String>, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.inner).list_prefix(prefix).await
        }

        async fn list_prefix_versioned(
            self: Arc<Self>,
            prefix: String,
        ) -> Result<Vec<crate::ListedObject>, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.inner).list_prefix_versioned(prefix).await
        }
    }

    struct FailPlacementStore(Arc<dyn ObjectStore>);

    #[async_trait::async_trait]
    impl ObjectStore for FailPlacementStore {
        async fn put(
            self: Arc<Self>,
            key: String,
            bytes: Vec<u8>,
        ) -> Result<u64, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.0).put(key, bytes).await
        }

        async fn put_cas(
            self: Arc<Self>,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.0).put_cas(key, expected, bytes).await
        }

        async fn get(self: Arc<Self>, key: String) -> crate::GetResult {
            if key == blockd_core::layout::placement_key() {
                return Err(blockd_core::protocol::StoreFault::Unavailable);
            }
            Arc::clone(&self.0).get(key).await
        }

        async fn get_range(
            self: Arc<Self>,
            key: String,
            offset: u64,
            len: u64,
        ) -> crate::GetResult {
            Arc::clone(&self.0).get_range(key, offset, len).await
        }

        async fn delete(
            self: Arc<Self>,
            key: String,
        ) -> Result<bool, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.0).delete(key).await
        }

        async fn delete_cas(
            self: Arc<Self>,
            key: String,
            expected: u64,
        ) -> Result<bool, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.0).delete_cas(key, expected).await
        }

        async fn list_prefix(
            self: Arc<Self>,
            prefix: String,
        ) -> Result<Vec<String>, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.0).list_prefix(prefix).await
        }

        async fn list_prefix_versioned(
            self: Arc<Self>,
            prefix: String,
        ) -> Result<Vec<crate::ListedObject>, blockd_core::protocol::StoreFault> {
            Arc::clone(&self.0).list_prefix_versioned(prefix).await
        }
    }

    async fn stop_test_fault_readers(shared: &Arc<Shared>) {
        let readers = std::mem::take(&mut *shared.fault_readers.lock().expect("fault reader lock"));
        for (_, reader) in readers {
            let _ = reader.stop().await;
        }
    }

    #[test]
    fn capture_write_protects_memory_and_data_pages() {
        let memory = VolumeId(1);
        let data = VolumeId(2);
        let pages = [
            PageId {
                volume: memory,
                page: PageNo(3),
            },
            PageId {
                volume: data,
                page: PageNo(5),
            },
        ];

        let grouped = group_write_protect_pages(&pages, |volume| {
            if volume == memory {
                VolumeKind::Memory
            } else {
                VolumeKind::Data
            }
        });

        assert_eq!(grouped.get(&memory), Some(&vec![3]));
        assert_eq!(grouped.get(&data), Some(&vec![5]));
    }

    #[tokio::test]
    async fn recovered_volume_preparation_waits_for_fault_reader_registration() {
        let shared = Arc::new(Shared::new(BTreeMap::new(), &test_host_config()));
        let gate = TestFaultReaderStart::held(false);
        *shared
            .fault_reader_start
            .lock()
            .expect("fault reader start lock") = Some(gate.clone());
        let (faults, _fault_rx) = injector();
        let volume = VolumeId(92);
        let preparation = prepare_recovered_volume_with(
            Arc::clone(&shared),
            faults,
            volume,
            VolumeConfig::data(1),
        );
        tokio::pin!(preparation);
        tokio::select! {
            () = gate.entered.notified() => {}
            ready = &mut preparation => panic!("preparation completed before reader registration: {ready}"),
        }
        gate.release.notify_one();
        assert!(preparation.await);
        assert!(shared.fault_readers.lock().expect("fault reader lock")[&volume].is_live());
        stop_test_fault_readers(&shared).await;
    }

    #[tokio::test]
    async fn failed_or_lost_fault_reader_blocks_recovered_volume_preparation() {
        let failed = Arc::new(Shared::new(BTreeMap::new(), &test_host_config()));
        let gate = TestFaultReaderStart::held(true);
        gate.release.notify_one();
        *failed
            .fault_reader_start
            .lock()
            .expect("fault reader start lock") = Some(gate);
        let (faults, _fault_rx) = injector();
        assert!(
            !prepare_recovered_volume_with(
                Arc::clone(&failed),
                faults,
                VolumeId(93),
                VolumeConfig::data(1),
            )
            .await
        );
        assert!(!failed.critical_healthy.load(Ordering::SeqCst));

        let lost = Arc::new(Shared::new(BTreeMap::new(), &test_host_config()));
        let (faults, _fault_rx) = injector();
        let volume = VolumeId(94);
        let config = VolumeConfig::data(1);
        assert!(
            prepare_recovered_volume_with(Arc::clone(&lost), faults.clone(), volume, config).await
        );
        lost.fault_readers.lock().expect("fault reader lock")[&volume]
            .task
            .abort();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if lost.fault_readers.lock().expect("fault reader lock")[&volume]
                    .task
                    .is_finished()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fault reader stopped");
        assert!(
            !prepare_recovered_volume_with(Arc::clone(&lost), faults, volume, config).await,
            "an existing volume without a live reader was reported serviceable"
        );
        assert!(!lost.critical_healthy.load(Ordering::SeqCst));
        stop_test_fault_readers(&lost).await;
    }

    #[test]
    fn stale_live_placement_completion_cannot_overwrite_a_newer_roster_shrink() {
        let status = LivePlacementStatus::new(true);
        let before_shrink = vec![HostId::new(1), HostId::new(2), HostId::new(3)];
        status.publish(before_shrink.clone());
        status.publish(vec![HostId::new(1), HostId::new(2)]);

        assert!(!status.complete_if_current(&before_shrink, 11, || true));
        assert_eq!(status.readiness(), (false, 0));

        let recovered = vec![HostId::new(1), HostId::new(2), HostId::new(4)];
        status.publish(recovered.clone());
        assert!(status.complete_if_current(&recovered, 12, || true));
        assert_eq!(status.readiness(), (true, 12));
    }

    #[test]
    fn live_placement_requires_the_local_host_and_replication_factor() {
        let local = HostId::new(2);
        assert!(!live_roster_includes_local(
            &[HostId::new(1), HostId::new(3), HostId::new(4)],
            local,
        ));
        assert!(!live_roster_includes_local(&[HostId::new(1), local], local,));
        assert!(live_roster_includes_local(
            &[HostId::new(1), local, HostId::new(3)],
            local,
        ));
    }

    fn start_test_fault_worker() -> (
        tokio::sync::mpsc::UnboundedSender<FaultWork>,
        Arc<Shared>,
        std::thread::JoinHandle<()>,
    ) {
        let shared = Arc::new(Shared::new(BTreeMap::new(), &test_host_config()));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let worker_shared = Arc::clone(&shared);
        let worker = std::thread::spawn(move || fault_work_loop(receiver, worker_shared));
        (sender, shared, worker)
    }

    fn enqueue_test_fault_work(
        sender: &tokio::sync::mpsc::UnboundedSender<FaultWork>,
        shared: &Shared,
        volume: VolumeId,
        entered: std::sync::mpsc::Sender<VolumeId>,
        release: Arc<TestFaultWorkGate>,
    ) -> Injected<Result<(), ()>> {
        let (reply, response) = injector();
        enqueue_fault_work(
            sender,
            &shared.fault_work_stats,
            FaultWork::Test {
                volume,
                entered,
                release,
                result: Ok(()),
                panics: false,
                reply,
            },
        )
        .expect("fault worker alive");
        response
    }

    fn test_host_config() -> HostConfig {
        HostConfig {
            archive: blockd_core::hostmeta::ArchivePolicy::default(),
            host: HostId::new(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            cluster_placement: None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_work_metrics_separate_queue_dispatch_and_service() {
        let shared = Arc::new(Shared::new(BTreeMap::new(), &test_host_config()));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (done, completed) = tokio::sync::oneshot::channel();
        enqueue_fault_work(
            &sender,
            &shared.fault_work_stats,
            FaultWork::Barrier { done },
        )
        .expect("fault worker alive");
        tokio::time::sleep(Duration::from_millis(2)).await;

        let worker_shared = Arc::clone(&shared);
        let worker = std::thread::spawn(move || fault_work_loop(receiver, worker_shared));
        completed.await.expect("barrier completed");
        drop(sender);
        worker.join().expect("fault worker joined");

        let metrics = shared.fault_work_stats.snapshot();
        assert_eq!(metrics.queue_depth, 0);
        assert_eq!(metrics.max_queue_depth, 1);
        assert_eq!(metrics.join_failures, 0);
        let barrier = metrics
            .timing
            .iter()
            .filter(|series| series.operation == "barrier")
            .collect::<Vec<_>>();
        assert_eq!(barrier.len(), FAULT_WORK_PHASES.len());
        assert!(barrier.iter().all(|series| series.histogram.count == 1));
        assert!(
            barrier
                .iter()
                .find(|series| series.phase == "queue_wait")
                .expect("queue wait series")
                .histogram
                .sum_ns
                >= 1_000_000
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_work_overlaps_distinct_volumes() {
        let (sender, shared, worker) = start_test_fault_worker();
        let (entered, observed) = std::sync::mpsc::channel();
        let release = Arc::new(TestFaultWorkGate::closed());
        let first = enqueue_test_fault_work(
            &sender,
            &shared,
            VolumeId(1),
            entered.clone(),
            Arc::clone(&release),
        );
        let second =
            enqueue_test_fault_work(&sender, &shared, VolumeId(2), entered, Arc::clone(&release));

        let first_entered = observed
            .recv_timeout(Duration::from_secs(1))
            .expect("first independent volume entered");
        let second_entered = observed
            .recv_timeout(Duration::from_secs(1))
            .expect("second independent volume overlapped");
        assert_ne!(first_entered, second_entered);
        release.release();
        assert_eq!(first.recv().await, Some(Ok(())));
        assert_eq!(second.recv().await, Some(Ok(())));
        let metrics = shared.fault_work_stats.snapshot();
        assert_eq!(metrics.active, 0);
        assert!(metrics.max_active >= 2);
        drop(sender);
        worker.join().expect("fault worker joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_work_serializes_each_volume() {
        let (sender, shared, worker) = start_test_fault_worker();
        let (entered, observed) = std::sync::mpsc::channel();
        let first_release = Arc::new(TestFaultWorkGate::closed());
        let second_release = Arc::new(TestFaultWorkGate::closed());
        let first = enqueue_test_fault_work(
            &sender,
            &shared,
            VolumeId(1),
            entered.clone(),
            Arc::clone(&first_release),
        );
        let second = enqueue_test_fault_work(
            &sender,
            &shared,
            VolumeId(1),
            entered,
            Arc::clone(&second_release),
        );

        assert_eq!(
            observed.recv_timeout(Duration::from_secs(1)),
            Ok(VolumeId(1))
        );
        assert!(
            observed.recv_timeout(Duration::from_millis(50)).is_err(),
            "same-volume successor entered before its predecessor completed"
        );
        first_release.release();
        assert_eq!(first.recv().await, Some(Ok(())));
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(1)),
            Ok(VolumeId(1))
        );
        second_release.release();
        assert_eq!(second.recv().await, Some(Ok(())));
        drop(sender);
        worker.join().expect("fault worker joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_work_barrier_drains_only_its_prefix() {
        let (sender, shared, worker) = start_test_fault_worker();
        let (entered, observed) = std::sync::mpsc::channel();
        let first_release = Arc::new(TestFaultWorkGate::closed());
        let second_release = Arc::new(TestFaultWorkGate::closed());
        let first = enqueue_test_fault_work(
            &sender,
            &shared,
            VolumeId(1),
            entered.clone(),
            Arc::clone(&first_release),
        );
        let (done, completed) = tokio::sync::oneshot::channel();
        enqueue_fault_work(
            &sender,
            &shared.fault_work_stats,
            FaultWork::Barrier { done },
        )
        .expect("fault worker alive");
        let second = enqueue_test_fault_work(
            &sender,
            &shared,
            VolumeId(2),
            entered,
            Arc::clone(&second_release),
        );

        assert_eq!(
            observed.recv_timeout(Duration::from_secs(1)),
            Ok(VolumeId(1))
        );
        assert!(
            observed.recv_timeout(Duration::from_millis(50)).is_err(),
            "post-barrier work entered while the prefix was draining"
        );
        first_release.release();
        assert_eq!(first.recv().await, Some(Ok(())));
        completed.await.expect("barrier completed after prefix");
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(1)),
            Ok(VolumeId(2))
        );
        second_release.release();
        assert_eq!(second.recv().await, Some(Ok(())));
        drop(sender);
        worker.join().expect("fault worker joined");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_work_panic_does_not_strand_the_volume() {
        let (sender, shared, worker) = start_test_fault_worker();
        let (entered, observed) = std::sync::mpsc::channel();
        let release = Arc::new(TestFaultWorkGate::closed());
        release.release();
        let (panic_reply, panic_response) = injector();
        enqueue_fault_work(
            &sender,
            &shared.fault_work_stats,
            FaultWork::Test {
                volume: VolumeId(1),
                entered: entered.clone(),
                release: Arc::clone(&release),
                result: Ok(()),
                panics: true,
                reply: panic_reply,
            },
        )
        .expect("fault worker alive");
        let successor =
            enqueue_test_fault_work(&sender, &shared, VolumeId(1), entered, Arc::clone(&release));

        assert_eq!(panic_response.recv().await, Some(Err(())));
        assert_eq!(successor.recv().await, Some(Ok(())));
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(1)),
            Ok(VolumeId(1))
        );
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(1)),
            Ok(VolumeId(1))
        );
        let metrics = shared.fault_work_stats.snapshot();
        assert_eq!(metrics.active, 0);
        assert_eq!(metrics.join_failures, 1);
        drop(sender);
        worker.join().expect("fault worker joined");
    }

    #[test]
    fn persistent_guest_access_serializes_each_volume() {
        let guest = GuestAccess {
            host: VolumeHost::new(VolumeId(1), VolumeConfig::data(1)),
        };
        let operation = guest.try_begin().expect("first operation starts");
        assert!(
            guest.try_begin().is_none(),
            "a second operation entered the non-thread-safe volume"
        );
        drop(operation);
        assert!(guest.try_begin().is_some());
    }

    #[tokio::test]
    async fn concurrent_admin_requests_keep_out_of_order_replies_with_their_callers() {
        let (admin, incoming) = injector::<AdminRequest>();
        let actor = tokio::spawn(async move {
            let first = incoming.recv().await.expect("first request");
            let second = incoming.recv().await.expect("second request");
            let (first, mut first_reply) = first.into_parts();
            let (second, mut second_reply) = second.into_parts();
            let AdminCall::DeleteBase { base: second_base } = second else {
                panic!("delete-base request");
            };
            let AdminCall::DeleteBase { base: first_base } = first else {
                panic!("delete-base request");
            };
            second_reply
                .send(Ok(AdminSuccess::BaseDeleted { base: second_base }))
                .expect("second caller alive");
            first_reply
                .send(Ok(AdminSuccess::BaseDeleted { base: first_base }))
                .expect("first caller alive");
        });

        let call = async |base: u64, admin: Injector<AdminRequest>| {
            let (request, reply) = request(AdminCall::DeleteBase { base });
            admin
                .push(Lane::Background, request)
                .unwrap_or_else(|_| panic!("actor alive"));
            tokio::time::timeout(Duration::from_secs(1), reply)
                .await
                .expect("reply without shared-stream timeout")
                .expect("actor alive")
        };
        let (first, second) = tokio::join!(call(1, admin.clone()), call(2, admin));

        for (expected, reply) in [(1, first), (2, second)] {
            assert_eq!(reply, Ok(AdminSuccess::BaseDeleted { base: expected }));
        }
        actor.await.expect("actor task");
    }

    async fn supervision_runtime(
        prefix: &str,
    ) -> (tempfile::TempDir, crate::fakegcs::FakeGcsServer, Runtime) {
        let root = tempfile::tempdir().expect("supervision data directory");
        let (fake, store) = test_object_store(prefix).await;
        let config = RuntimeConfig {
            daemon: test_host_config(),
            cluster_id: None,
            blob_dir: root.path().join("blobs"),
            peer: None,
        };
        let runtime = Runtime::new(&config, store).await.expect("runtime startup");
        (root, fake, runtime)
    }

    async fn assert_drain_waits_for_authority_exclusion(quarantined: bool) {
        let (_root, _fake, mut runtime) = supervision_runtime(if quarantined {
            "quarantined-drain-authority/"
        } else {
            "empty-drain-authority/"
        })
        .await;
        runtime.authority_required = true;
        if quarantined {
            runtime
                .shared
                .quarantines
                .lock()
                .expect("quarantine lock")
                .insert(VolumeId(91), "test quarantine".to_owned());
            assert!(
                runtime
                    .volume_inventory()
                    .iter()
                    .all(|(_, _, quarantined)| *quarantined)
            );
        } else {
            assert!(runtime.volume_inventory().is_empty());
        }
        let local = runtime.host;
        let other = [HostId::new(41), HostId::new(42), HostId::new(43)];
        let mut retaining_members = vec![local, other[0], other[1]];
        retaining_members.sort_unstable();
        retaining_members.dedup();
        let retaining =
            ClusterPlacement::new(99, 1, retaining_members).expect("retaining placement");
        Arc::clone(&runtime.store)
            .put(
                blockd_core::layout::placement_key().clone(),
                retaining.encode(),
            )
            .await
            .expect("publish retaining placement");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(350),
                runtime.await_authority_transfer()
            )
            .await
            .is_err(),
            "drain bypassed authority placement because inventory had no serviceable volumes"
        );

        let excluded = ClusterPlacement::new(99, 2, other.to_vec()).expect("excluded placement");
        Arc::clone(&runtime.store)
            .put(
                blockd_core::layout::placement_key().clone(),
                excluded.encode(),
            )
            .await
            .expect("publish excluded placement");
        tokio::time::timeout(Duration::from_secs(1), runtime.await_authority_transfer())
            .await
            .expect("drain observed authority exclusion")
            .expect("valid authority placement");
        runtime.shutdown().await.expect("runtime shutdown");
    }

    #[tokio::test]
    async fn empty_node_drain_waits_for_authority_placement_exclusion() {
        assert_drain_waits_for_authority_exclusion(false).await;
    }

    #[tokio::test]
    async fn all_quarantined_node_drain_waits_for_authority_placement_exclusion() {
        assert_drain_waits_for_authority_exclusion(true).await;
    }

    async fn assert_injected_critical_failure(runtime: &Runtime, expected: &str) {
        tokio::time::timeout(Duration::from_secs(1), runtime.critical_failure())
            .await
            .expect("critical task failure propagated");
        assert!(!runtime.is_ready());
        assert!(
            runtime
                .incidents()
                .iter()
                .any(|incident| incident.contains(expected)),
            "missing incident for {expected}: {:?}",
            runtime.incidents()
        );
    }

    #[tokio::test]
    async fn actor_and_observation_termination_are_host_fatal() {
        let (_root, _fake, mut actor) = supervision_runtime("actor-supervision/").await;
        actor.inject_actor_task_failure(TestActorFailure::HostActor);
        assert_injected_critical_failure(&actor, "host actor stopped unexpectedly").await;
        actor.shutdown().await.expect("failed actor still joins");

        let (_root, _fake, mut observation) = supervision_runtime("observation-supervision/").await;
        observation.inject_actor_task_failure(TestActorFailure::Observation);
        assert_injected_critical_failure(&observation, "observation task stopped unexpectedly")
            .await;
        observation
            .shutdown()
            .await
            .expect("failed observation still joins");
    }

    #[tokio::test]
    async fn fault_worker_termination_is_host_fatal() {
        let (_root, _fake, mut runtime) = supervision_runtime("fault-worker-supervision/").await;
        runtime.inject_fault_worker_failure();
        assert_injected_critical_failure(&runtime, "fault worker stopped unexpectedly").await;
        assert!(
            runtime.shutdown().await.is_err(),
            "a panicked fault worker must make shutdown report failure"
        );
    }

    struct RecoveryTestPeers {
        runtimes: Vec<Runtime>,
        _roots: Vec<tempfile::TempDir>,
    }

    impl RecoveryTestPeers {
        async fn shutdown(mut self) {
            for runtime in &mut self.runtimes {
                runtime.shutdown().await.expect("peer runtime shutdown");
            }
        }
    }

    fn recovery_test_addr() -> std::net::SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind recovery peer address")
            .local_addr()
            .expect("recovery peer address")
    }

    async fn start_peer_backed_recovery_runtime(
        mut main: RuntimeConfig,
        store: Arc<dyn ObjectStore>,
    ) -> (RuntimeConfig, RecoveryTestPeers, Runtime) {
        const CLUSTER_ID: u64 = 0x5245_434f_5645_5259;

        let mut roots = Vec::new();
        let mut configs = Vec::new();
        let main_addr = recovery_test_addr();
        main.cluster_id = Some(CLUSTER_ID);
        main.peer = Some(PeerConfig {
            listen: main_addr,
            advertise: main_addr,
        });
        configs.push(main);
        for host in [HostId::new(2), HostId::new(3)] {
            let root = tempfile::tempdir().expect("recovery peer root");
            let address = recovery_test_addr();
            let mut daemon = test_host_config();
            daemon.host = host;
            configs.push(RuntimeConfig {
                daemon,
                cluster_id: Some(CLUSTER_ID),
                blob_dir: root.path().join("blobs"),
                peer: Some(PeerConfig {
                    listen: address,
                    advertise: address,
                }),
            });
            roots.push(root);
        }
        let mut runtimes = futures_util::future::join_all(
            configs
                .iter()
                .map(|config| Runtime::new(config, Arc::clone(&store))),
        )
        .await
        .into_iter()
        .map(|runtime| runtime.expect("authenticated recovery runtime startup"))
        .collect::<Vec<_>>();
        let main_config = configs.remove(0);
        let main_runtime = runtimes.remove(0);
        (
            main_config,
            RecoveryTestPeers {
                runtimes,
                _roots: roots,
            },
            main_runtime,
        )
    }

    #[tokio::test]
    async fn startup_rolls_back_owned_membership_and_resources_after_reconciliation_failure() {
        const CLUSTER_ID: u64 = 0x5245_434f_5645_5259;

        let (_fake, store) = test_object_store("startup-rollback/").await;
        let baseline_root = tempfile::tempdir().expect("baseline root");
        let baseline = RuntimeConfig {
            daemon: test_host_config(),
            cluster_id: None,
            blob_dir: baseline_root.path().join("blobs"),
            peer: None,
        };
        let (_baseline, baseline_peers, mut baseline_main) =
            start_peer_backed_recovery_runtime(baseline, Arc::clone(&store)).await;

        let failed_root = tempfile::tempdir().expect("failed startup root");
        let address = recovery_test_addr();
        let identity = HostId::new(4);
        let mut daemon = test_host_config();
        daemon.host = identity;
        let failed = RuntimeConfig {
            daemon,
            cluster_id: Some(CLUSTER_ID),
            blob_dir: failed_root.path().join("blobs"),
            peer: Some(PeerConfig {
                listen: address,
                advertise: address,
            }),
        };
        let failing: Arc<dyn ObjectStore> = Arc::new(FailPlacementStore(Arc::clone(&store)));
        let error = match Runtime::new(&failed, failing).await {
            Ok(mut runtime) => {
                runtime
                    .shutdown()
                    .await
                    .expect("unexpected runtime shutdown");
                panic!("cluster placement read succeeded after injected failure");
            }
            Err(error) => error,
        };
        assert!(matches!(error, RuntimeStartupError::Placement(_)));
        assert!(
            Arc::clone(&store)
                .get(blockd_core::layout::peer_membership_key(identity))
                .await
                .expect("rolled-back membership lookup")
                .is_none(),
            "failed startup leaked its exact owned membership"
        );
        let rebound = std::net::TcpListener::bind(address).expect("peer listener was released");
        drop(rebound);

        let mut retry = Runtime::new(&failed, Arc::clone(&store))
            .await
            .expect("same node resources are reusable after rollback");
        assert!(
            Arc::clone(&store)
                .get(blockd_core::layout::peer_membership_key(identity))
                .await
                .expect("retry membership lookup")
                .is_some()
        );
        retry.shutdown().await.expect("retry shutdown");
        baseline_main.shutdown().await.expect("baseline shutdown");
        baseline_peers.shutdown().await;
    }

    /// Regression PROD-004: the production constructor used by `blockd serve` must
    /// rebuild runtime-side volume mappings and fault readers from local state.
    #[tokio::test]
    async fn runtime_new_recovers_local_volume_mapping_and_fault_reader() {
        use crate::fakegcs::FakeGcs;
        use crate::{GcsConfig, GcsStore};
        use blockd_core::journal::{JournalRecord, RecordKind, VolumeConfig};
        use blockd_core::types::{JournalSeq, VolumeId};

        let volume = VolumeId(404);
        let root = tempfile::tempdir().expect("data directory");
        let blob_dir = root.path().join("blobs");
        let journal = blob_dir.join(blockd_core::layout::journal_blob(volume, 1, JournalSeq(0)));
        tokio::fs::create_dir_all(journal.parent().expect("journal parent"))
            .await
            .expect("journal directory");
        let record = JournalRecord {
            config: VolumeConfig::data(1),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 0,
            sync_covered_through: 0,
            post_state_checksum: 0,
            files: Vec::new(),
            runtime_page_index: BTreeMap::new(),
            migrated_from: None,
        };
        tokio::fs::write(&journal, record.encode(volume))
            .await
            .expect("durable journal");

        let (_fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "runtime-new/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let config = RuntimeConfig {
            daemon: HostConfig {
                backup_retry: blockd_core::types::millis(100),
                writeback_interval: blockd_core::types::millis(10),
                ..test_host_config()
            },
            cluster_id: None,
            blob_dir,
            peer: None,
        };
        let start = TestFaultReaderStart::held(false);
        set_test_fault_reader_start(&config.blob_dir, start.clone());
        let startup = tokio::spawn(start_peer_backed_recovery_runtime(config, store));
        tokio::time::timeout(Duration::from_secs(1), start.entered.notified())
            .await
            .expect("recovered fault reader entered registration");
        assert!(
            !startup.is_finished(),
            "Runtime::new returned before recovered fault service registered"
        );
        start.release.notify_one();
        let (_config, peers, mut runtime) = startup.await.expect("runtime startup task");

        assert!(
            runtime
                .shared
                .volumes
                .lock()
                .expect("volume lock")
                .contains_key(&volume),
            "local volume was absent from the runtime mapping"
        );
        assert!(
            runtime
                .shared
                .fault_readers
                .lock()
                .expect("fault reader lock")
                .contains_key(&volume),
            "recovered volume had no fault reader"
        );
        runtime.stop_fault_reader_unexpectedly_for_test(volume);
        tokio::time::timeout(Duration::from_secs(1), runtime.critical_failure())
            .await
            .expect("unexpected fault-reader exit becomes host-fatal");
        assert!(!runtime.is_ready());
        runtime.shutdown().await.expect("runtime shutdown");
        peers.shutdown().await;
    }

    #[tokio::test]
    async fn runtime_new_keeps_recovery_unready_when_fault_reader_registration_fails() {
        use crate::fakegcs::FakeGcs;
        use crate::{GcsConfig, GcsStore};
        use blockd_core::journal::{JournalRecord, RecordKind, VolumeConfig};
        use blockd_core::types::{JournalSeq, VolumeId};

        let volume = VolumeId(409);
        let root = tempfile::tempdir().expect("data directory");
        let blob_dir = root.path().join("blobs");
        let journal = blob_dir.join(blockd_core::layout::journal_blob(volume, 1, JournalSeq(0)));
        tokio::fs::create_dir_all(journal.parent().expect("journal parent"))
            .await
            .expect("journal directory");
        let record = JournalRecord {
            config: VolumeConfig::data(1),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 0,
            sync_covered_through: 0,
            post_state_checksum: 0,
            files: Vec::new(),
            runtime_page_index: BTreeMap::new(),
            migrated_from: None,
        };
        tokio::fs::write(&journal, record.encode(volume))
            .await
            .expect("durable journal");

        let (_fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "runtime-new-reader-failure/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let config = RuntimeConfig {
            daemon: HostConfig {
                backup_retry: blockd_core::types::millis(100),
                writeback_interval: blockd_core::types::millis(10),
                ..test_host_config()
            },
            cluster_id: None,
            blob_dir,
            peer: None,
        };
        let start = TestFaultReaderStart::held(true);
        set_test_fault_reader_start(&config.blob_dir, start.clone());
        let startup = tokio::spawn(start_peer_backed_recovery_runtime(config, store));
        tokio::time::timeout(Duration::from_secs(1), start.entered.notified())
            .await
            .expect("recovered fault reader entered registration");
        assert!(
            !startup.is_finished(),
            "Runtime::new returned before failed registration was reported"
        );
        start.release.notify_one();
        let (_config, peers, mut runtime) = startup.await.expect("runtime startup task");
        assert!(
            runtime
                .quarantines()
                .get(&volume)
                .is_some_and(|reason| reason.contains("fault service"))
        );
        assert!(!runtime.shared.recovery_complete.load(Ordering::SeqCst));
        assert!(!runtime.is_ready());
        assert!(
            !runtime
                .shared
                .fault_readers
                .lock()
                .expect("fault reader lock")[&volume]
                .is_live()
        );
        runtime.shutdown().await.expect("runtime shutdown");
        peers.shutdown().await;
    }

    fn missing_blx_fixture(root: &Path, volume: VolumeId) -> (RuntimeConfig, String, Vec<u8>) {
        use blockd_core::blx::BlxObject;
        use blockd_core::journal::{JournalRecord, RecordKind, VolumeConfig};
        use blockd_core::page_file::PageBatchBuilder;
        use blockd_core::types::{Gen, JournalSeq, ObjectId, PageNo};

        let blob_dir = root.join("blobs");
        let page = PageId {
            volume,
            page: PageNo(0),
        };
        let mut builder = PageBatchBuilder::new(volume, 1, ObjectId(0));
        builder.add(page, Gen(1), &vec![7; page_size()]);
        let (object, blx, locations) = builder.finish().pop().expect("fixture BLX");
        let object_ref = blockd_core::manifest::ObjectRef::from_blx(
            &BlxObject::open(&blx).expect("fixture BLX opens"),
        );
        let journal_name = blockd_core::layout::journal_blob(volume, 1, JournalSeq(0));
        let journal = blob_dir.join(&journal_name);
        std::fs::create_dir_all(journal.parent().expect("journal parent"))
            .expect("journal directory");
        let record = JournalRecord {
            config: VolumeConfig::data(1),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            post_state_checksum: 0,
            files: vec![object_ref],
            runtime_page_index: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            migrated_from: None,
        };
        std::fs::write(journal, record.encode(volume)).expect("journal fixture");
        (
            RuntimeConfig {
                daemon: HostConfig {
                    backup_retry: blockd_core::types::millis(100),
                    writeback_interval: blockd_core::types::millis(10),
                    ..test_host_config()
                },
                cluster_id: None,
                blob_dir,
                peer: None,
            },
            blockd_core::layout::blx_blob(volume, 1, object),
            blx,
        )
    }

    async fn test_object_store(
        prefix: &str,
    ) -> (crate::fakegcs::FakeGcsServer, Arc<dyn ObjectStore>) {
        use crate::fakegcs::FakeGcs;
        use crate::{GcsConfig, GcsStore};

        let (fake, endpoint) = FakeGcs::start().await;
        (
            fake,
            Arc::new(GcsStore::new(GcsConfig {
                bucket: "cluster".to_owned(),
                prefix: prefix.to_owned(),
                endpoint: endpoint.clone(),
                metadata_endpoint: endpoint,
            })),
        )
    }

    #[tokio::test]
    async fn startup_coalesces_a_roster_change_inside_the_reconciliation_window() {
        const CLUSTER_ID: u64 = 79;

        let (_fake, inner) = test_object_store("startup-placement-race/").await;
        let blocked = Arc::new(BlockFirstPlacementRead::new(inner));
        let store: Arc<dyn ObjectStore> = blocked.clone();
        let placement_status = Arc::new(LivePlacementStatus::new(true));
        let (rosters, roster_rx) = tokio::sync::watch::channel(Vec::new());
        let (admin, _admin_rx) = injector();
        let local = test_host_config().host;
        let old_roster = vec![local, HostId::new(2), HostId::new(3)];
        let current_roster = vec![local, HostId::new(2), HostId::new(4)];
        placement_status.publish(old_roster.clone());
        rosters.send_replace(old_roster);

        let (startup_ready, startup) = tokio::sync::oneshot::channel();
        let startup_status = Arc::clone(&placement_status);
        let owner = tokio::spawn(async move {
            PlacementOwner {
                store,
                cluster_id: CLUSTER_ID,
                authority: None,
                local,
                status: startup_status,
                rosters: roster_rx,
                admin,
                startup: startup_ready,
            }
            .run()
            .await;
        });

        tokio::time::timeout(Duration::from_secs(1), blocked.entered.notified())
            .await
            .expect("startup entered the old reconciliation race window");
        placement_status.publish(current_roster.clone());
        rosters.send_replace(current_roster.clone());
        blocked.release.notify_one();

        let installed = tokio::time::timeout(Duration::from_secs(2), startup)
            .await
            .expect("startup reconciled the replacement roster")
            .expect("placement owner alive")
            .expect("startup placement reconciliation");
        owner.abort();
        let durable = Arc::clone(&blocked.inner)
            .get(blockd_core::layout::placement_key())
            .await
            .expect("durable placement read")
            .and_then(|(_, bytes)| ClusterPlacement::decode(&bytes))
            .expect("canonical durable placement");

        assert_eq!(durable.cluster_id, CLUSTER_ID);
        assert_eq!(
            durable.epoch, 1,
            "a roster superseded before its store read completes must never be published"
        );
        assert_eq!(durable.roster, current_roster);
        assert_eq!(installed.membership_epoch, durable.epoch);
        assert_eq!(installed.roster, durable.roster);
        assert_eq!(placement_status.readiness(), (true, durable.epoch));
    }

    #[tokio::test]
    async fn cluster_placement_is_monotonic_and_restart_readable() {
        let (_fake, store) = test_object_store("cluster-placement/").await;
        let initial = reconcile_cluster_placement(
            Arc::clone(&store),
            77,
            vec![HostId::new(3), HostId::new(1), HostId::new(2)],
        )
        .await
        .expect("initial placement");
        assert_eq!(initial.epoch, 1);
        assert_eq!(
            initial.roster,
            vec![HostId::new(1), HostId::new(2), HostId::new(3)]
        );
        let unchanged = reconcile_cluster_placement(
            Arc::clone(&store),
            77,
            vec![HostId::new(1), HostId::new(2), HostId::new(3)],
        )
        .await
        .expect("unchanged placement");
        assert_eq!(unchanged.epoch, 1);

        let changed = reconcile_cluster_placement(
            Arc::clone(&store),
            77,
            vec![HostId::new(1), HostId::new(2), HostId::new(4)],
        )
        .await
        .expect("changed placement");
        assert_eq!(changed.epoch, 2);
        let disjoint = reconcile_cluster_placement(
            Arc::clone(&store),
            77,
            vec![HostId::new(5), HostId::new(6), HostId::new(7)],
        )
        .await
        .expect("object-store CAS accepts a disjoint live roster");
        assert_eq!(disjoint.epoch, 3);
        let (_, encoded) = Arc::clone(&store)
            .get(blockd_core::layout::placement_key())
            .await
            .expect("read placement")
            .expect("placement exists");
        assert_eq!(ClusterPlacement::decode(&encoded), Some(disjoint));
    }

    #[tokio::test]
    async fn durable_epoch_orders_a_roster_change_when_legacy_hash_decreases() {
        let partial_roster = vec![HostId::new(0), HostId::new(1), HostId::new(2)];
        let full_roster = vec![
            HostId::new(0),
            HostId::new(1),
            HostId::new(2),
            HostId::new(3),
        ];
        assert!(live_membership_epoch(&partial_roster) > live_membership_epoch(&full_roster));

        let (_fake, store) = test_object_store("nonmonotonic-membership-hash/").await;
        let initial = reconcile_cluster_placement(Arc::clone(&store), 78, partial_roster)
            .await
            .expect("partial placement");
        let changed = reconcile_cluster_placement(Arc::clone(&store), 78, full_roster.clone())
            .await
            .expect("full placement");
        let unchanged = reconcile_cluster_placement(store, 78, full_roster)
            .await
            .expect("unchanged full placement");

        assert_eq!(initial.epoch, 1);
        assert_eq!(changed.epoch, 2);
        assert_eq!(unchanged.epoch, changed.epoch);
    }

    #[tokio::test]
    async fn quarantine_cleanup_requires_reason_and_leaves_two_phase_audit() {
        let volume = VolumeId(405);
        let root = tempfile::tempdir().expect("data directory");
        let (config, _missing_blx, _bytes) = missing_blx_fixture(root.path(), volume);
        let (_fake, store) = test_object_store("discard-quarantine/").await;
        let (config, peers, mut runtime) = start_peer_backed_recovery_runtime(config, store).await;
        assert!(runtime.quarantines().contains_key(&volume));
        assert!(runtime.discard_quarantine(volume, "  ").await.is_err());

        let audit_id = runtime
            .discard_quarantine(volume, "operator approved irreversible cleanup")
            .await
            .expect("explicit quarantine cleanup");
        assert!(!runtime.quarantines().contains_key(&volume));
        assert!(runtime.volume_inventory().is_empty());
        assert!(crate::blobscan::scan_blob_dir_for_recovery(&config.blob_dir).is_empty());
        let audit = config
            .blob_dir
            .join("quarantine-audit")
            .join(format!("{:016x}", volume.0));
        let intent = std::fs::read_to_string(audit.join(format!("{audit_id}.intent.json")))
            .expect("durable cleanup intent");
        let complete = std::fs::read_to_string(audit.join(format!("{audit_id}.complete.json")))
            .expect("durable cleanup completion");
        assert!(intent.contains("operator approved irreversible cleanup"));
        assert!(complete.contains("\"phase\":\"complete\""));
        runtime.shutdown().await.expect("runtime shutdown");
        peers.shutdown().await;
    }

    #[tokio::test]
    async fn corrupt_journal_without_decodable_config_is_reported_and_cleanable() {
        use blockd_core::types::JournalSeq;

        let volume = VolumeId(407);
        let root = tempfile::tempdir().expect("data directory");
        let blob_dir = root.path().join("blobs");
        let journal = blob_dir.join(blockd_core::layout::journal_blob(volume, 1, JournalSeq(0)));
        std::fs::create_dir_all(journal.parent().expect("journal parent"))
            .expect("journal directory");
        std::fs::write(&journal, b"corrupt journal").expect("corrupt fixture");
        let config = RuntimeConfig {
            daemon: test_host_config(),
            cluster_id: None,
            blob_dir,
            peer: None,
        };
        let (_fake, store) = test_object_store("corrupt-quarantine/").await;
        let mut runtime = Runtime::new(&config, store).await.expect("runtime startup");
        assert!(
            runtime
                .quarantines()
                .get(&volume)
                .is_some_and(|reason| { reason.contains("corrupt") && reason.contains("repair") })
        );
        runtime
            .discard_quarantine(volume, "forensics complete")
            .await
            .expect("audited cleanup");
        assert!(!journal.exists());
        runtime.shutdown().await.expect("runtime shutdown");
    }

    #[tokio::test]
    async fn quarantined_false_negative_can_be_repaired_before_cleanup() {
        let volume = VolumeId(406);
        let root = tempfile::tempdir().expect("data directory");
        let (config, missing_blx, bytes) = missing_blx_fixture(root.path(), volume);
        let (_fake, store) = test_object_store("repair-quarantine/").await;
        let (config, peers, mut first) =
            start_peer_backed_recovery_runtime(config, Arc::clone(&store)).await;
        assert!(first.quarantines().contains_key(&volume));
        first.shutdown().await.expect("first runtime shutdown");

        let blx_path = config.blob_dir.join(missing_blx);
        std::fs::create_dir_all(blx_path.parent().expect("BLX parent")).expect("BLX directory");
        std::fs::write(blx_path, bytes).expect("operator repair");
        let mut repaired = Runtime::new(&config, store).await.expect("runtime startup");
        assert!(!repaired.quarantines().contains_key(&volume));
        assert!(
            repaired
                .volume_inventory()
                .iter()
                .any(|(found, _, quarantined)| *found == volume && !quarantined)
        );
        repaired
            .shutdown()
            .await
            .expect("repaired runtime shutdown");
        peers.shutdown().await;
    }

    #[tokio::test]
    async fn restart_after_verified_repair_clears_quarantine_and_starts_fault_reader() {
        let volume = VolumeId(408);
        let root = tempfile::tempdir().expect("data directory");
        let (config, missing_blx, bytes) = missing_blx_fixture(root.path(), volume);
        let (_fake, store) = test_object_store("live-repair-quarantine/").await;
        let (config, peers, mut runtime) =
            start_peer_backed_recovery_runtime(config, Arc::clone(&store)).await;
        assert!(runtime.quarantines().contains_key(&volume));
        assert!(
            !runtime
                .shared
                .fault_readers
                .lock()
                .expect("fault reader lock")
                .contains_key(&volume)
        );

        assert!(runtime.quarantines().contains_key(&volume));
        assert!(
            !runtime
                .shared
                .fault_readers
                .lock()
                .expect("fault reader lock")
                .contains_key(&volume),
            "failed live repair started a fault reader for quarantined data"
        );

        let repaired_path = config.blob_dir.join(missing_blx);
        std::fs::create_dir_all(repaired_path.parent().expect("BLX parent"))
            .expect("BLX directory");
        std::fs::write(repaired_path, bytes).expect("operator repair");
        runtime
            .shutdown()
            .await
            .expect("quarantined runtime shutdown");
        let mut repaired = Runtime::new(&config, store)
            .await
            .expect("repaired runtime startup");
        assert!(!repaired.quarantines().contains_key(&volume));
        assert!(
            repaired
                .shared
                .fault_readers
                .lock()
                .expect("fault reader lock")
                .contains_key(&volume),
            "verified repair did not start its fault reader after restart"
        );
        repaired.shutdown().await.expect("runtime shutdown");
        peers.shutdown().await;
    }

    #[test]
    fn successful_migration_completes_pause_once() {
        let shared = Shared::new(BTreeMap::new(), &test_host_config());
        let volume = VolumeId(7);
        shared
            .pause_expected
            .lock()
            .expect("pause lock")
            .entry(volume)
            .or_default()
            .push_back(1);

        shared.begin_pause(volume);
        assert!(
            shared
                .pause_in_flight
                .lock()
                .expect("pause lock")
                .contains_key(&volume)
        );

        shared.complete_pause(volume);
        shared.complete_pause(volume);

        assert!(
            !shared
                .pause_in_flight
                .lock()
                .expect("pause lock")
                .contains_key(&volume)
        );
        assert_eq!(shared.pause_latency[1].snapshot().count, 1);
    }

    #[test]
    fn cancelled_pending_pause_releases_guest_control() {
        let volume = VolumeId(8);
        let host = VolumeHost::new(volume, VolumeConfig::data(1));
        let shared = Arc::new(Shared::new(
            BTreeMap::from([(volume, Arc::clone(&host))]),
            &test_host_config(),
        ));
        shared
            .pause_expected
            .lock()
            .expect("pause lock")
            .entry(volume)
            .or_default()
            .push_back(0);
        shared.begin_pause(volume);
        {
            let mut state = host.ctl.state.lock().expect("guest control lock");
            state.pause_generation = 1;
            state.pause_requested = true;
            state.paused = true;
        }

        drop(PendingGuestPause {
            shared: Arc::clone(&shared),
            host: Arc::clone(&host),
            volume,
            generation: 1,
            active: true,
        });

        let state = host.ctl.state.lock().expect("guest control lock");
        assert!(!state.pause_requested);
        assert!(!state.paused);
        drop(state);
        assert!(
            !shared
                .pause_in_flight
                .lock()
                .expect("pause lock")
                .contains_key(&volume)
        );
    }
}
