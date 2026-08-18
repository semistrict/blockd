//! Production host for the shared async protocol actors.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use blockd_core::engine::{HostFatal, HostState, host_actor_with_state};
use blockd_core::hostmeta::{
    Counters, DaemonStats, HostConfig, ReplicaPlacementConfig, ReplicaSpoolMetrics,
    ReplicaVolumeMetrics, VolumeOperations,
};
use blockd_core::journal::{VolumeConfig, VolumeKind};
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::{
    AdminCall, AdminEvent, AdminResult, AdminSuccess, PeerMsg, ReqId, Verdict,
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

use crate::loopstats::{LoopStats, world_kind};
use crate::metrics::{
    AtomicHistogram, FaultLatency, FaultReaderMetrics, FaultWorkMetrics, LatencySeries,
    TimingSeries, detailed_profile_metrics_enabled,
};
use crate::peer::{PeerConfig, PeerNet};
use crate::store::ObjectStore;
use crate::world::{FileBlobs, RuntimeStore};
use crate::{CapacityController, CapacityInputs, CapacitySignal};

pub struct RuntimeConfig {
    pub daemon: HostConfig,
    pub blob_dir: PathBuf,
    pub peer: Option<PeerConfig>,
}

fn live_membership_epoch(members: &[HostId]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for member in members {
        for byte in member.0.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash.max(1)
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

    fn page_index(&self, page: PageId) -> usize {
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
    incidents: Mutex<Vec<String>>,
    counters: Mutex<Counters>,
    daemon_stats: Mutex<DaemonStats>,
    replica_metrics: Mutex<Vec<ReplicaVolumeMetrics>>,
    replica_spool_metrics: Mutex<Vec<ReplicaSpoolMetrics>>,
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
}

impl Shared {
    fn new(volumes: BTreeMap<VolumeId, Arc<VolumeHost>>, config: &HostConfig) -> Self {
        let state = HostState::new(config.clone());
        Self {
            volumes: Mutex::new(volumes),
            admin_events: Mutex::new(VecDeque::new()),
            admin_event_ready: tokio::sync::Notify::new(),
            incidents: Mutex::new(Vec::new()),
            counters: Mutex::new(Counters::default()),
            daemon_stats: Mutex::new(state.stats()),
            replica_metrics: Mutex::new(Vec::new()),
            replica_spool_metrics: Mutex::new(Vec::new()),
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
        }
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
}

impl Drop for ActiveFaultReader {
    fn drop(&mut self) {
        self.shared
            .fault_reader
            .readers_exited
            .fetch_add(1, Ordering::Relaxed);
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
            let index = host.page_index(page);
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
            let index = host.page_index(page);
            host.uffd
                .as_ref()
                .expect("compute unprotect")
                .writeprotect(host.view.addr_of(index), page_size(), false)
                .map_err(|_| ())
        }
        BlockingFaultWork::Evict { host, page } => {
            let index = host.page_index(page);
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
    self_id: HostId,
    peer_rx: Injected<(HostId, PeerMsg)>,
    fault_rx: Injected<GuestFault>,
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
            peers.send(self.self_id, to, &message);
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

    async fn recv(&self) -> Option<(HostId, PeerMsg)> {
        self.peer_rx.recv().await
    }
}

impl GuestMem for ProductionWorld {
    async fn read_page(&self, page: PageId) -> Vec<u8> {
        let host = self.host(page.volume);
        host.region.read_page(host.page_index(page))
    }

    async fn arm_write_protect(&self, pages: &[PageId]) -> Result<(), GuestMemoryError> {
        let hosts = {
            let mut by_volume = BTreeMap::<VolumeId, Vec<usize>>::new();
            let volumes = self.shared.volumes.lock().expect("volume lock");
            for &page in pages {
                let host = &volumes[&page.volume];
                if host.config.kind == VolumeKind::Data {
                    by_volume
                        .entry(page.volume)
                        .or_default()
                        .push(host.page_index(page));
                }
            }
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
        tracing::error!(?page, "fatal unservable guest page");
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

    async fn host_failed(&self, failure: HostFatal) {
        tracing::error!(reason = failure.reason, "fatal host actor failure");
        eprintln!("fatal host actor failure: {}", failure.reason);
        self.shared
            .incidents
            .lock()
            .expect("incident lock")
            .push(format!("host failure: {}", failure.reason));
        self.shared.stats.record_world(world_kind::ABORT, 0);
        std::process::abort();
    }
}

#[derive(Clone)]
struct Inputs {
    admin: Injector<AdminRequest>,
    faults: Injector<GuestFault>,
    syncs: Injector<GuestSyncRequest>,
    peers: Injector<(HostId, PeerMsg)>,
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

pub struct Runtime {
    inputs: Inputs,
    shared: Arc<Shared>,
    blob_dir: PathBuf,
    peers: Option<Arc<PeerNet>>,
    authenticated_peers: bool,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    actor_task: Option<std::thread::JoinHandle<()>>,
    fault_readers: Mutex<BTreeMap<VolumeId, tokio::task::JoinHandle<()>>>,
    fault_worker: Option<std::thread::JoinHandle<()>>,
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
        self.host.view.read_word(self.host.page_index(page))
    }

    pub fn read_page(&self, page: PageId) -> Vec<u8> {
        self.host.view.read_page(self.host.page_index(page))
    }

    pub fn write_word(&self, page: PageId, value: u64) {
        self.host.view.write_word(self.host.page_index(page), value);
    }

    pub fn evict_page(&self, page: PageId) -> std::io::Result<()> {
        self.host.view.evict(self.host.page_index(page), 1)
    }
}

impl Drop for GuestOperation {
    fn drop(&mut self) {
        self.host.op_end();
    }
}

impl Runtime {
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(config: &RuntimeConfig, store: Arc<dyn ObjectStore>) -> Self {
        Self::start(BTreeMap::new(), config, store).await
    }

    #[allow(clippy::needless_pass_by_value)]
    pub async fn recover(
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
        volume_configs: &BTreeMap<VolumeId, VolumeConfig>,
    ) -> (Self, BTreeMap<VolumeId, Verdict>) {
        let mut hosts = BTreeMap::new();
        for (&volume, &volume_config) in volume_configs {
            assert_peer_stash_transport(volume_config, config.peer.is_some());
            hosts.insert(volume, VolumeHost::new(volume, volume_config));
        }
        let runtime = Self::start(hosts, config, store).await;
        (runtime, BTreeMap::new())
    }

    #[allow(clippy::too_many_lines)]
    async fn start(
        hosts: BTreeMap<VolumeId, Arc<VolumeHost>>,
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        tokio::fs::create_dir_all(&config.blob_dir)
            .await
            .expect("blob directory");
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
        let fault_worker = std::thread::Builder::new()
            .name("blockd-fault-work".to_owned())
            .spawn(move || fault_work_loop(fault_work_rx, fault_worker_shared))
            .expect("spawn fault worker");
        let blobs = FileBlobs::new(&config.blob_dir);
        let peer_store = Arc::clone(&store);
        let runtime_store = RuntimeStore::new(store);
        let actor_config = config.daemon.clone();
        let peer_input = inputs.peers.clone();
        let placement_input = inputs.admin.clone();
        let placement_host = actor_config.host;
        let placement_authority = actor_config
            .replica_placement
            .as_ref()
            .and_then(|placement| placement.authority);
        let peers = match config.peer.clone() {
            Some(peer_config) => Some(
                PeerNet::start_with_membership(
                    &peer_config,
                    actor_config.host,
                    peer_store,
                    move |from, message| {
                        let lane = peer_lane(&message);
                        let _ = peer_input.push(lane, (from, message));
                    },
                    move |members| {
                        let roster = members
                            .iter()
                            .map(|member| PeerCandidate {
                                host: *member,
                                weight: 1,
                                failure_domain: member.0,
                                drained: false,
                            })
                            .collect();
                        let placement = ReplicaPlacementConfig {
                            membership_epoch: live_membership_epoch(&members),
                            local_failure_domain: placement_host.0,
                            roster,
                            authority: placement_authority,
                        };
                        let (update, _reply) =
                            request(AdminCall::UpdateReplicaPlacement { placement });
                        let _ = placement_input.push(Lane::Background, update);
                    },
                )
                .await
                .expect("peer listen"),
            ),
            None => None,
        };
        let authenticated_peers = peers.as_ref().is_some_and(|peers| peers.authenticated());
        let actor_shared = Arc::clone(&shared);
        let actor_inputs = inputs.clone();
        let world_fault_work = fault_work.clone();
        let shutdown_fault_work = fault_work.clone();
        let actor_peers = peers.clone();
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let poll_stats = Arc::clone(&actor_shared);
        let actor_thread_name = format!("blockd-actor-{}", actor_config.host.0);
        let actor_task = std::thread::Builder::new()
            .name(actor_thread_name)
            .spawn(move || {
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
                                self_id: actor_config.host,
                                peer_rx: peer_rx_actor,
                                fault_rx: fault_rx_actor,
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
                            let _ = shutdown_rx.await;
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
            })
            .expect("spawn actor thread");

        let runtime = Self {
            inputs,
            shared,
            blob_dir: config.blob_dir.clone(),
            peers,
            authenticated_peers,
            shutdown: Some(shutdown),
            actor_task: Some(actor_task),
            fault_readers: Mutex::new(BTreeMap::new()),
            fault_worker: Some(fault_worker),
        };
        let hosts = runtime
            .shared
            .volumes
            .lock()
            .expect("volume lock")
            .iter()
            .map(|(&volume, host)| (volume, Arc::clone(host)))
            .collect::<Vec<_>>();
        for (volume, host) in hosts {
            runtime.spawn_fault_reader(volume, host);
        }
        runtime
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

    pub fn peer_connections(&self) -> Vec<(HostId, bool)> {
        self.peers
            .as_ref()
            .map_or_else(Vec::new, |peers| peers.connections())
    }

    #[allow(clippy::too_many_lines)] // readiness, error, tracing, and injection are one loop
    fn spawn_fault_reader(&self, volume: VolumeId, host: Arc<VolumeHost>) {
        let faults = self.inputs.faults.clone();
        let shared = Arc::clone(&self.shared);
        let uffd = host
            .uffd
            .as_ref()
            .expect("compute volume has userfaultfd")
            .clone();
        uffd.set_nonblocking(true).expect("nonblocking userfaultfd");
        shared
            .fault_reader
            .readers_started
            .fetch_add(1, Ordering::Relaxed);
        let task = tokio::spawn(async move {
            let _active = ActiveFaultReader {
                shared: Arc::clone(&shared),
            };
            let uffd = AsyncFd::new(SharedUffd(uffd)).expect("register runtime userfaultfd");
            loop {
                let mut ready = match uffd.readable().await {
                    Ok(ready) => ready,
                    Err(error) => {
                        shared
                            .fault_reader
                            .terminal_errors
                            .fetch_add(1, Ordering::Relaxed);
                        shared
                            .incidents
                            .lock()
                            .expect("incident lock")
                            .push(format!(
                                "fault reader readiness failed for {volume:?}: {error}"
                            ));
                        return;
                    }
                };
                loop {
                    let events = match ready.try_io(|inner| inner.get_ref().0.read_events()) {
                        Ok(Ok(events)) => events,
                        Ok(Err(error)) => {
                            shared
                                .fault_reader
                                .terminal_errors
                                .fetch_add(1, Ordering::Relaxed);
                            shared
                                .incidents
                                .lock()
                                .expect("incident lock")
                                .push(format!("fault reader failed for {volume:?}: {error}"));
                            return;
                        }
                        Err(_) => break,
                    };
                    shared.fault_reader.events_read.fetch_add(
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
                        shared
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
                            shared
                                .fault_reader
                                .injection_failures
                                .fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        shared
                            .fault_reader
                            .events_injected
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        if let Some(previous) = self
            .fault_readers
            .lock()
            .expect("fault reader lock")
            .insert(volume, task)
        {
            previous.abort();
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

    pub async fn create_volume(&self, volume: VolumeId, config: VolumeConfig) {
        let started = Instant::now();
        self.install_volume_host(volume, config);
        let created = match self
            .admin_request(AdminCall::CreateVolume {
                volume,
                config,
                from_base: None,
            })
            .await
        {
            Ok(AdminSuccess::VolumeCreated { volume: found }) if found == volume => true,
            Err(_) => false,
            result => panic!("unexpected create result: {result:?}"),
        };
        self.observe_operation(0, created, started.elapsed());
        assert!(created, "volume creation failed");
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
        self.install_volume_host(volume, config);
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
        let started = Instant::now();
        self.install_volume_host(volume, config);
        let result = match self
            .admin_request(AdminCall::RestoreVolume { volume })
            .await
        {
            Ok(AdminSuccess::VolumeRestored { verdict, .. }) => Some(verdict),
            Err(_) => None,
            result => panic!("unexpected restore result: {result:?}"),
        };
        self.observe_operation(2, result.is_some(), started.elapsed());
        result.expect("restore failed")
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
        self.install_volume_host(volume, config);
    }

    fn install_volume_host(&self, volume: VolumeId, config: VolumeConfig) {
        assert_peer_stash_transport(config, self.authenticated_peers);
        let host = VolumeHost::new(volume, config);
        self.shared
            .volumes
            .lock()
            .expect("volume lock")
            .insert(volume, Arc::clone(&host));
        self.spawn_fault_reader(volume, host);
    }

    pub async fn migrate_out(&self, volume: VolumeId, to: HostId) {
        let started = Instant::now();
        let pauses_guest = self.host(volume).config.kind == VolumeKind::Memory;
        if pauses_guest {
            self.expect_pause(volume, 1);
        }
        let migrated = match self
            .admin_request(AdminCall::MigrateOut { volume, to })
            .await
        {
            Ok(AdminSuccess::MigratedOut { .. }) => true,
            Err(_) => false,
            result => panic!("unexpected migration result: {result:?}"),
        };
        self.observe_operation(3, migrated, started.elapsed());
        if pauses_guest {
            if migrated {
                self.shared.complete_pause(volume);
            } else {
                self.cancel_expected_pause(volume, 1);
            }
        }
        assert!(migrated, "migrate out failed");
    }

    pub async fn wait_migrated_in(&self, volume: VolumeId) -> Verdict {
        self.wait_admin_event(|event| match event {
            AdminEvent::VolumeMigratedIn {
                volume: found,
                verdict,
            } if *found == volume => Some(*verdict),
            _ => None,
        })
        .await
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

    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
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
            host.view.write_word(host.page_index(page), value);
            host.op_end();
        })
        .await
        .expect("guest write worker");
    }

    pub async fn guest_read(&self, volume: VolumeId, page: PageId) -> Vec<u8> {
        let host = self.host(volume);
        host.op_start().await;
        tokio::task::spawn_blocking(move || {
            let bytes = host.view.read_page(host.page_index(page));
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

    pub async fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(actor_task) = self.actor_task.take() {
            let _ = tokio::task::spawn_blocking(move || actor_task.join()).await;
        }
        if let Some(fault_worker) = self.fault_worker.take() {
            let _ = tokio::task::spawn_blocking(move || fault_worker.join()).await;
        }
        let readers = std::mem::take(&mut *self.fault_readers.lock().expect("fault reader lock"));
        for (_, reader) in readers {
            reader.abort();
            let _ = reader.await;
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(actor_task) = self.actor_task.take() {
            let _ = actor_task.join();
        }
        for (_, reader) in
            std::mem::take(&mut *self.fault_readers.lock().expect("fault reader lock"))
        {
            reader.abort();
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
        let daemon = state.stats();
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
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
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
