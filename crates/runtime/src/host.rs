//! The runtime host: one thread runs the real `Daemon` state machine; its
//! effects are interpreted against real guest memory (userfaultfd), real
//! disk blobs, real timers, and the S3-shaped store. Guest workloads run
//! on caller threads and touch mapped memory directly — their faults reach
//! the daemon exactly as production faults do.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TrySendError, channel, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use blockd_core::daemon::{Daemon, DaemonConfig};
use blockd_core::database::{AttachmentId, DatabaseError, DatabaseReply, DatabaseRequest};
use blockd_core::format::crc32c;
use blockd_core::journal::VsetConfig;
use blockd_core::journal::VsetKind;
use blockd_core::layout;
use blockd_core::protocol::{AdminCmd, AdminReply, IoId, PeerMsg, ReplicaArtifact, ReqId, Verdict};
use blockd_core::replica_spool::seal_verified_replica_artifact;
use blockd_core::seam::{Effect, Event, HostMap, TimerId};
use blockd_core::types::{HostId, PageId, PageNo, VmId, VolumeId, VolumeIdx, VsetId};
use blockd_hostmem::{GuestView, HostRegion, Uffd, UffdFeatures, page_size};
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;

use crate::loopstats::{LoopStats, effect_kind, event_kind};
use crate::metrics::{AtomicHistogram, FaultLatency, LatencySeries};
use crate::peer::{PeerConfig, PeerNet};
use crate::store::ObjectStore;
use crate::{CapacityController, CapacityInputs, CapacitySignal};

pub struct RuntimeConfig {
    pub daemon: DaemonConfig,
    /// The local blob device: a real directory on real disk.
    pub blob_dir: PathBuf,
    /// Peer transport (`None` = a host that never migrates).
    pub peer: Option<PeerConfig>,
}

fn assert_peer_stash_transport(config: VsetConfig, authenticated: bool) {
    let _ = config;
    assert!(
        authenticated,
        "passive durability requires mutually authenticated TLS"
    );
}

/// One vset's guest memory on this host: the region (daemon view), the
/// guest's mapping, its uffd, and the vCPU control block.
struct VsetHost {
    config: VsetConfig,
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
    cv: Condvar,
}

#[derive(Default)]
struct CtlState {
    pause_requested: bool,
    paused: bool,
    in_op: bool,
    /// Applied guest operations — the op numbering vmstate refers to.
    applied: u64,
}

impl VsetHost {
    fn new(config: VsetConfig) -> Arc<VsetHost> {
        let pages = (usize::from(config.disk_volumes) + 1)
            * usize::try_from(config.pages_per_volume).expect("fits");
        let region = Arc::new(HostRegion::new(pages).expect("region"));
        let view = Arc::new(GuestView::map(&region, 0, pages).expect("view"));
        let uffd = (config.kind == VsetKind::Compute).then(|| {
            let (uffd, features) = Uffd::new(
                UffdFeatures::PAGEFAULT_FLAG_WP
                    | UffdFeatures::MINOR_SHMEM
                    | UffdFeatures::WP_HUGETLBFS_SHMEM,
            )
            .expect("userfaultfd");
            assert!(
                features.has(UffdFeatures::MINOR_SHMEM)
                    && features.has(UffdFeatures::WP_HUGETLBFS_SHMEM),
                "kernel lacks required uffd features: {features:?}"
            );
            uffd.register_all(&view).expect("register");
            Arc::new(uffd)
        });
        Arc::new(VsetHost {
            config,
            region,
            view,
            uffd,
            ctl: GuestCtl::default(),
            fault_latency: std::array::from_fn(|_| AtomicHistogram::default()),
        })
    }

    fn page_index(&self, page: PageId) -> usize {
        usize::from(page.volume.idx.0)
            * usize::try_from(self.config.pages_per_volume).expect("fits")
            + usize::try_from(page.page.0).expect("fits")
    }

    fn page_of_addr(&self, vset: VsetId, addr: usize) -> PageId {
        let index = (addr - self.view.addr_of(0)) / page_size();
        let per = usize::try_from(self.config.pages_per_volume).expect("fits");
        PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(u8::try_from(index / per).expect("fits")),
            },
            page: PageNo(u32::try_from(index % per).expect("fits")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    const ALL: [FaultSource; Self::COUNT] = [
        FaultSource::Zero,
        FaultSource::Shared,
        FaultSource::WriteProtect,
        FaultSource::Local,
        FaultSource::Peer,
        FaultSource::Store,
        FaultSource::Unservable,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    const fn name(self) -> &'static str {
        match self {
            FaultSource::Zero => "zero",
            FaultSource::Shared => "shared",
            FaultSource::WriteProtect => "write_protect",
            FaultSource::Local => "local_nvme",
            FaultSource::Peer => "peer",
            FaultSource::Store => "object_store",
            FaultSource::Unservable => "unservable",
        }
    }
}

struct FaultInFlight {
    started: Instant,
    span: tracing::Span,
}

const OPERATION_NAMES: [&str; 5] = ["create", "checkpoint", "restore", "migration", "sync"];
const OPERATION_OUTCOMES: [&str; 2] = ["success", "failed"];
const LOCAL_IO_NAMES: [&str; 4] = ["write", "read", "ranged_read", "delete"];
const LOCAL_IO_OUTCOMES: [&str; 2] = ["success", "missing"];
const PAUSE_NAMES: [&str; 2] = ["checkpoint", "migration"];

pub(crate) enum Msg {
    Ev(Event),
    /// A guest reached an op boundary with a pause pending.
    Quiesced(VsetId),
    Stop,
}

/// The loop's inbox, two lanes: events a guest is blocked on RIGHT NOW —
/// faults, fill-path read completions, pause boundaries — outrank bursts
/// of write/admin completions, so a pile of writeback acks never queues
/// ahead of a vCPU stuck in a page fault. Timers ride the critical lane
/// too: the tick is the pacemaker, and delaying it under load does not
/// shed work — it batches the next capture bigger and lengthens the very
/// stalls the lanes exist to avoid. A fairness valve drains one
/// background event per `BACKGROUND_SHARE` criticals so completion
/// processing can never starve outright.
pub(crate) struct LoopQueue {
    lanes: Mutex<Lanes>,
    cv: Condvar,
}

#[derive(Default)]
struct Lanes {
    critical: VecDeque<Msg>,
    background: VecDeque<Msg>,
    /// Criticals served since the last background pop.
    streak: u32,
}

const BACKGROUND_SHARE: u32 = 32;

impl LoopQueue {
    fn new() -> Arc<LoopQueue> {
        Arc::new(LoopQueue {
            lanes: Mutex::new(Lanes::default()),
            cv: Condvar::new(),
        })
    }

    pub(crate) fn push(&self, msg: Msg) {
        let critical = match &msg {
            Msg::Ev(
                Event::GuestFault { .. }
                | Event::BlobReadDone { .. }
                | Event::StoreGetDone { .. }
                | Event::Timer(_),
            )
            | Msg::Quiesced(_)
            | Msg::Stop => true,
            Msg::Ev(Event::PeerDelivered { msg, .. }) => matches!(
                msg,
                blockd_core::protocol::PeerMsg::Page { .. }
                    | blockd_core::protocol::PeerMsg::Leaf { .. }
                    | blockd_core::protocol::PeerMsg::FetchRange { .. }
                    | blockd_core::protocol::PeerMsg::FetchLeaf { .. }
            ),
            Msg::Ev(_) => false,
        };
        let mut lanes = self.lanes.lock().expect("lock");
        if critical {
            lanes.critical.push_back(msg);
        } else {
            lanes.background.push_back(msg);
        }
        drop(lanes);
        self.cv.notify_one();
    }

    fn pop(&self) -> Msg {
        let mut lanes = self.lanes.lock().expect("lock");
        loop {
            let starve_valve = lanes.streak >= BACKGROUND_SHARE;
            if !lanes.background.is_empty() && (lanes.critical.is_empty() || starve_valve) {
                lanes.streak = 0;
                return lanes.background.pop_front().expect("checked");
            }
            if let Some(msg) = lanes.critical.pop_front() {
                lanes.streak += 1;
                return msg;
            }
            lanes = self.cv.wait(lanes).expect("wait");
        }
    }

    fn depths(&self) -> (usize, usize) {
        let lanes = self.lanes.lock().expect("lock");
        (lanes.critical.len(), lanes.background.len())
    }
}

struct Shared {
    vsets: Mutex<BTreeMap<VsetId, Arc<VsetHost>>>,
    sync_waiters: Mutex<BTreeMap<ReqId, Sender<bool>>>,
    database_waiters: Mutex<BTreeMap<ReqId, Sender<DatabaseReply>>>,
    /// `VsetFenced` / `FillFailed` and friends: anything the tests must know
    /// went wrong (asserted empty in healthy scenarios).
    incidents: Mutex<Vec<String>>,
    /// The daemon's counters, copied out after every step (R9.2).
    counters: Mutex<blockd_core::daemon::Counters>,
    daemon_stats: Mutex<blockd_core::daemon::DaemonStats>,
    replica_metrics: Mutex<Vec<blockd_core::daemon::ReplicaVsetMetrics>>,
    replica_spool_metrics: Mutex<Vec<blockd_core::daemon::ReplicaSpoolMetrics>>,
    /// Smoothed host pressure recommendation for placement and optional work.
    capacity: Mutex<CapacityController>,
    /// Loop-thread time attribution (R9.2's perf side): decide vs effect
    /// execution vs idle, by kind.
    stats: LoopStats,
    fault_in_flight: Mutex<BTreeMap<PageId, VecDeque<FaultInFlight>>>,
    operation_latency: [[AtomicHistogram; OPERATION_OUTCOMES.len()]; OPERATION_NAMES.len()],
    local_io_latency: [[AtomicHistogram; LOCAL_IO_OUTCOMES.len()]; LOCAL_IO_NAMES.len()],
    local_io_in_flight: [AtomicU64; LOCAL_IO_NAMES.len()],
    pause_expected: Mutex<BTreeMap<VsetId, VecDeque<usize>>>,
    pause_in_flight: Mutex<BTreeMap<VsetId, (usize, Instant)>>,
    pause_latency: [AtomicHistogram; PAUSE_NAMES.len()],
    archive_lag_started: Mutex<BTreeMap<VsetId, Instant>>,
    operation_started: Mutex<BTreeMap<(VsetId, u8), Instant>>,
    next_req: AtomicU64,
}

impl Shared {
    fn new(
        vsets: BTreeMap<VsetId, Arc<VsetHost>>,
        daemon_stats: blockd_core::daemon::DaemonStats,
        replica_metrics: Vec<blockd_core::daemon::ReplicaVsetMetrics>,
        replica_spool_metrics: Vec<blockd_core::daemon::ReplicaSpoolMetrics>,
    ) -> Shared {
        Shared {
            vsets: Mutex::new(vsets),
            sync_waiters: Mutex::new(BTreeMap::new()),
            database_waiters: Mutex::new(BTreeMap::new()),
            incidents: Mutex::new(Vec::new()),
            counters: Mutex::new(blockd_core::daemon::Counters::default()),
            daemon_stats: Mutex::new(daemon_stats),
            replica_metrics: Mutex::new(replica_metrics),
            replica_spool_metrics: Mutex::new(replica_spool_metrics),
            capacity: Mutex::new(CapacityController::default()),
            stats: LoopStats::default(),
            fault_in_flight: Mutex::new(BTreeMap::new()),
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
            archive_lag_started: Mutex::new(BTreeMap::new()),
            operation_started: Mutex::new(BTreeMap::new()),
            next_req: AtomicU64::new(1),
        }
    }
}

/// The daemon's synchronous window onto the mappings. Capture arms write
/// protection in one batched effect before scheduling reads, so this view
/// only copies bytes and never issues an ioctl per page.
struct MapView<'a> {
    vsets: &'a BTreeMap<VsetId, Arc<VsetHost>>,
    fault_work: &'a tokio::sync::mpsc::Sender<FaultWork>,
}

impl HostMap for MapView<'_> {
    fn arm_write_protect(&self, pages: &[PageId]) {
        let (done, armed) = sync_channel(0);
        self.fault_work
            .blocking_send(FaultWork::WriteProtect {
                hosts: writeprotect_jobs(self.vsets, pages),
                done,
            })
            .expect("fault I/O runtime alive");
        armed.recv().expect("fault I/O runtime alive");
    }

    fn read_page(&self, page: PageId) -> Vec<u8> {
        let host = &self.vsets[&page.volume.vset];
        let index = host.page_index(page);
        host.region.read_page(index)
    }
}

pub struct Runtime {
    tx: Arc<LoopQueue>,
    shared: Arc<Shared>,
    admin_rx: Mutex<Receiver<AdminReply>>,
    admin_backlog: Mutex<VecDeque<AdminReply>>,
    blob_dir: PathBuf,
    peers: Option<Arc<PeerNet>>,
    fault_io: FaultIo,
    authenticated_peers: bool,
    #[cfg(test)]
    fault_reader_count: Arc<std::sync::atomic::AtomicUsize>,
    loop_thread: Option<thread::JoinHandle<()>>,
}

struct FaultIo {
    handle: tokio::runtime::Handle,
    work: tokio::sync::mpsc::Sender<FaultWork>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

enum FaultWork {
    Fill {
        shared: Arc<Shared>,
        host: Arc<VsetHost>,
        page: PageId,
        bytes: Vec<u8>,
        writable: bool,
        source: FaultSource,
    },
    Unprotect {
        shared: Arc<Shared>,
        host: Arc<VsetHost>,
        page: PageId,
    },
    Evict {
        host: Arc<VsetHost>,
        page: PageId,
    },
    WriteProtect {
        hosts: Vec<(Arc<VsetHost>, Vec<usize>)>,
        done: std::sync::mpsc::SyncSender<()>,
    },
    Barrier {
        done: std::sync::mpsc::SyncSender<()>,
    },
}

impl FaultIo {
    fn shutdown(&mut self) {
        let (done, drained) = sync_channel(0);
        let mut barrier = FaultWork::Barrier { done };
        let queued = loop {
            match self.work.try_send(barrier) {
                Ok(()) => break true,
                Err(tokio::sync::mpsc::error::TrySendError::Full(work)) => {
                    barrier = work;
                    thread::yield_now();
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break false,
            }
        };
        if queued {
            let _ = drained.recv();
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn spawn_fault_io_runtime() -> FaultIo {
    let (ready_tx, ready_rx) = sync_channel(1);
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    // Bound outstanding completions so a fault storm applies backpressure
    // instead of turning blocked guest pages into unbounded host memory.
    let (work, mut work_rx) = tokio::sync::mpsc::channel(1024);
    let thread = thread::Builder::new()
        .name("blockd-fault-io".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .expect("fault I/O runtime");
            ready_tx
                .send(runtime.handle().clone())
                .expect("runtime owner alive");
            runtime.block_on(async {
                tokio::pin!(shutdown_rx);
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        item = work_rx.recv() => {
                            let Some(item) = item else { break };
                            execute_fault_work(item);
                        }
                    }
                }
            });
        })
        .expect("spawn fault I/O runtime");
    FaultIo {
        handle: ready_rx.recv().expect("fault I/O runtime started"),
        work,
        shutdown: Some(shutdown),
        thread: Some(thread),
    }
}

fn execute_fault_work(work: FaultWork) {
    match work {
        FaultWork::Fill {
            shared,
            host,
            page,
            bytes,
            writable,
            source,
        } => {
            let index = host.page_index(page);
            host.region.write_page(index, &bytes);
            if let Err(error) = host.uffd.as_ref().expect("compute fill").continue_range(
                host.view.addr_of(index),
                page_size(),
                !writable,
            ) {
                tracing::error!(?page, %error, "fatal fill completion failure");
                std::process::abort();
            }
            complete_fault(&shared, Some(&host), page, source, "served");
        }
        FaultWork::Unprotect { shared, host, page } => {
            let index = host.page_index(page);
            if let Err(error) = host.uffd.as_ref().expect("compute unprotect").writeprotect(
                host.view.addr_of(index),
                page_size(),
                false,
            ) {
                tracing::error!(?page, %error, "fatal unprotect failure");
                std::process::abort();
            }
            complete_fault(
                &shared,
                Some(&host),
                page,
                FaultSource::WriteProtect,
                "served",
            );
        }
        FaultWork::Evict { host, page } => {
            let index = host.page_index(page);
            host.view.evict(index, 1).expect("evict");
            host.region.punch_hole(index, 1).expect("punch");
        }
        FaultWork::WriteProtect { hosts, done } => {
            for (host, mut indices) in hosts {
                let uffd = host.uffd.as_ref().expect("compute write-protect");
                for_each_contiguous_run(&mut indices, |start, len| {
                    uffd.writeprotect(host.view.addr_of(start), len * page_size(), true)
                        .expect("write-protect");
                });
            }
            let _ = done.send(());
        }
        FaultWork::Barrier { done } => {
            let _ = done.send(());
        }
    }
}

struct ReplicaPrepareJob {
    from: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    artifact: ReplicaArtifact,
    checksum: u32,
    bytes: Vec<u8>,
}

const REPLICA_PREPARE_WORKERS: usize = 2;
const REPLICA_PREPARE_QUEUE_CAPACITY: usize = 2;

fn prepare_replica_put(job: ReplicaPrepareJob) -> Event {
    let ReplicaPrepareJob {
        from,
        vset,
        assignment_epoch,
        artifact,
        checksum,
        bytes,
    } = job;
    let frame = (crc32c(&bytes) == checksum)
        .then(|| {
            seal_verified_replica_artifact(from, vset, assignment_epoch, artifact, checksum, &bytes)
        })
        .and_then(Result::ok);
    Event::ReplicaPutPrepared {
        from,
        vset,
        assignment_epoch,
        artifact,
        checksum,
        bytes,
        frame,
    }
}

fn spawn_replica_prepare_workers(tx: &Arc<LoopQueue>) -> SyncSender<ReplicaPrepareJob> {
    let (work, rx) = sync_channel::<ReplicaPrepareJob>(REPLICA_PREPARE_QUEUE_CAPACITY);
    let rx = Arc::new(Mutex::new(rx));
    for _ in 0..REPLICA_PREPARE_WORKERS {
        let rx = rx.clone();
        let tx = tx.clone();
        thread::Builder::new()
            .name("blockd-replica-prepare".to_owned())
            .spawn(move || {
                loop {
                    let Ok(job) = rx.lock().expect("lock").recv() else {
                        return;
                    };
                    tx.push(Msg::Ev(prepare_replica_put(job)));
                }
            })
            .expect("spawn replica preparation worker");
    }
    work
}

fn writeprotect_jobs(
    vsets: &BTreeMap<VsetId, Arc<VsetHost>>,
    pages: &[PageId],
) -> Vec<(Arc<VsetHost>, Vec<usize>)> {
    let mut by_vset: BTreeMap<VsetId, Vec<usize>> = BTreeMap::new();
    for &page in pages {
        let host = &vsets[&page.volume.vset];
        if host.config.kind == VsetKind::Compute {
            by_vset
                .entry(page.volume.vset)
                .or_default()
                .push(host.page_index(page));
        }
    }
    by_vset
        .into_iter()
        .map(|(vset, indices)| (vsets[&vset].clone(), indices))
        .collect()
}

#[cfg(test)]
struct ActiveFaultReader(Arc<std::sync::atomic::AtomicUsize>);

#[cfg(test)]
impl Drop for ActiveFaultReader {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Runtime {
    /// A fresh daemon on an empty (or to-be-ignored) blob directory.
    // Ownership transfer: the async store executor clones from it.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(config: &RuntimeConfig, store: Arc<dyn ObjectStore>) -> Runtime {
        let (daemon, effects) = Daemon::new(config.daemon.clone());
        Runtime::start(daemon, effects, BTreeMap::new(), config, &store)
    }

    /// Recover a daemon from the blobs actually on disk (R8.2). The caller
    /// supplies the vset configs so guest memory can be rebuilt (in
    /// production the control plane knows them; the records carry them
    /// too). Store claims and passive inventory complete asynchronously;
    /// recovered vsets report through admin replies after head refresh.
    #[allow(clippy::needless_pass_by_value)]
    pub fn recover(
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
        vset_configs: &BTreeMap<VsetId, VsetConfig>,
    ) -> (Runtime, BTreeMap<VsetId, Verdict>) {
        let blobs = crate::blobscan::scan_blob_dir_for_recovery(&config.blob_dir);
        let (daemon, verdicts, effects) = Daemon::recover_with_metadata(
            config.daemon.clone(),
            blobs
                .iter()
                .map(crate::blobscan::ScannedBlob::recovery_blob),
        );
        let mut hosts = BTreeMap::new();
        for (&vset, &vc) in vset_configs {
            assert_peer_stash_transport(
                vc,
                config.peer.as_ref().is_some_and(|peer| peer.tls.is_some()),
            );
            hosts.insert(vset, VsetHost::new(vc));
        }
        let runtime = Runtime::start(daemon, effects, hosts, config, &store);
        for (&vset, host) in runtime.shared.vsets.lock().expect("lock").iter() {
            if host.config.kind == VsetKind::Compute {
                runtime.spawn_fault_reader(vset, host.clone());
            }
        }
        (runtime, verdicts)
    }

    #[allow(clippy::too_many_lines)]
    fn start(
        mut daemon: Daemon,
        boot_effects: Vec<Effect>,
        hosts: BTreeMap<VsetId, Arc<VsetHost>>,
        config: &RuntimeConfig,
        store: &Arc<dyn ObjectStore>,
    ) -> Runtime {
        std::fs::create_dir_all(&config.blob_dir).expect("blob dir");
        let tx = LoopQueue::new();
        let rx = tx.clone();
        let (admin_tx, admin_rx) = channel::<AdminReply>();
        let daemon_stats = daemon.stats();
        let shared = Arc::new(Shared::new(
            hosts,
            daemon_stats,
            daemon.replica_metrics(),
            daemon.replica_spool_metrics(),
        ));
        update_capacity_signal(&shared, &tx);

        let replica_prepare = spawn_replica_prepare_workers(&tx);
        let peers = config.peer.as_ref().map(|p| {
            let tx = tx.clone();
            let replica_prepare = replica_prepare.clone();
            PeerNet::start(p, config.daemon.host, move |from, msg| {
                if let PeerMsg::ReplicaPut {
                    vset,
                    assignment_epoch,
                    artifact,
                    checksum,
                    bytes,
                } = msg
                {
                    let job = ReplicaPrepareJob {
                        from,
                        vset,
                        assignment_epoch,
                        artifact,
                        checksum,
                        bytes,
                    };
                    if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
                        replica_prepare.try_send(job)
                    {
                        // Replica transfer is retry-driven. Dropping here
                        // bounds preparation memory without blocking the
                        // single-threaded peer I/O runtime.
                        tracing::warn!("replica preparation queue full or stopped");
                    }
                } else {
                    tx.push(Msg::Ev(Event::PeerDelivered { from, msg }));
                }
            })
        });
        let fault_io = spawn_fault_io_runtime();
        let fault_effects = fault_io.handle.clone();
        let fault_work = fault_io.work.clone();

        let effect_context = EffectContext {
            shared: shared.clone(),
            io: spawn_io_workers(&config.blob_dir, store, &tx, &shared),
            admin: admin_tx,
            tx: tx.clone(),
            peers: peers.clone(),
            self_id: config.daemon.host,
            fault_io: fault_effects,
            fault_work,
        };

        // The event loop: the daemon lives here.
        let loop_thread = {
            let shared = shared.clone();
            thread::spawn(move || {
                let apply = |effects: Vec<Effect>, source: Option<FaultSource>| {
                    for effect in effects {
                        let kind = effect_kind(&effect);
                        let started = Instant::now();
                        apply_effect(effect, &effect_context, source);
                        shared
                            .stats
                            .record_effect(kind, elapsed_ns(started.elapsed()));
                    }
                };
                apply(boot_effects, None);
                loop {
                    let blocked = Instant::now();
                    let msg = rx.pop();
                    shared.stats.record_idle(elapsed_ns(blocked.elapsed()));
                    let event = match msg {
                        Msg::Ev(event) => event,
                        Msg::Quiesced(vset) => {
                            let vmstate = {
                                let host = shared.vsets.lock().expect("lock")[&vset].clone();
                                let mut state = host.ctl.state.lock().expect("lock");
                                state.paused = true;
                                state.applied
                            };
                            Event::GuestPaused { vset, vmstate }
                        }
                        Msg::Stop => break,
                    };
                    let refresh_observability = matches!(event, Event::Timer(_));
                    let refresh_capacity = matches!(&event, Event::Timer(TimerId::Writeback));
                    let kind = event_kind(&event);
                    let source = fault_source_of_event(&event);
                    let started = Instant::now();
                    let effects = {
                        let vsets = shared.vsets.lock().expect("lock");
                        daemon.step(
                            event,
                            &MapView {
                                vsets: &vsets,
                                fault_work: &effect_context.fault_work,
                            },
                        )
                    };
                    *shared.counters.lock().expect("lock") = daemon.counters;
                    if refresh_observability {
                        let daemon_stats = daemon.stats();
                        update_archive_lag(&shared, &daemon_stats);
                        update_active_operations(&shared, &daemon_stats);
                        *shared.daemon_stats.lock().expect("lock") = daemon_stats;
                        *shared.replica_metrics.lock().expect("lock") = daemon.replica_metrics();
                        *shared.replica_spool_metrics.lock().expect("lock") =
                            daemon.replica_spool_metrics();
                        if refresh_capacity {
                            update_capacity_signal(&shared, &effect_context.tx);
                        }
                    }
                    shared
                        .stats
                        .record_decide(kind, elapsed_ns(started.elapsed()));
                    apply(effects, source);
                }
            })
        };

        Runtime {
            tx,
            shared,
            admin_rx: Mutex::new(admin_rx),
            admin_backlog: Mutex::new(VecDeque::new()),
            blob_dir: config.blob_dir.clone(),
            peers,
            fault_io,
            authenticated_peers: config.peer.as_ref().is_some_and(|peer| peer.tls.is_some()),
            #[cfg(test)]
            fault_reader_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            loop_thread: Some(loop_thread),
        }
    }

    /// Loop-thread time attribution: decide vs effects vs idle, by kind.
    pub fn loop_stats(&self) -> &LoopStats {
        &self.shared.stats
    }

    pub fn loop_queue_depths(&self) -> (usize, usize) {
        self.tx.depths()
    }

    pub fn daemon_stats(&self) -> blockd_core::daemon::DaemonStats {
        self.shared.daemon_stats.lock().expect("lock").clone()
    }

    /// Current host-capacity recommendation for control-plane consumers.
    pub fn capacity_signal(&self) -> CapacitySignal {
        self.shared.capacity.lock().expect("lock").signal()
    }

    pub fn archive_lag_age(&self) -> Vec<(VsetId, Duration)> {
        self.shared
            .archive_lag_started
            .lock()
            .expect("lock")
            .iter()
            .map(|(&vset, started)| (vset, started.elapsed()))
            .collect()
    }

    pub fn active_operation_age(&self) -> Vec<(VsetId, &'static str, Duration)> {
        self.shared
            .operation_started
            .lock()
            .expect("lock")
            .iter()
            .map(|(&(vset, operation), started)| {
                (vset, operation_name(operation), started.elapsed())
            })
            .collect()
    }

    pub fn fault_latency(&self) -> Vec<FaultLatency> {
        let active: std::collections::BTreeSet<VsetId> = self
            .shared
            .daemon_stats
            .lock()
            .expect("lock")
            .vsets
            .iter()
            .map(|stats| stats.vset)
            .collect();
        let vsets = self.shared.vsets.lock().expect("lock");
        let mut snapshots = Vec::new();
        for (&vset, host) in vsets.iter().filter(|(vset, _)| active.contains(vset)) {
            for source in FaultSource::ALL {
                snapshots.push(FaultLatency {
                    vset,
                    source: source.name(),
                    histogram: host.fault_latency[source.index()].snapshot(),
                });
            }
        }
        snapshots
    }

    pub fn operation_latency(&self) -> Vec<LatencySeries> {
        let mut snapshots = Vec::new();
        for (operation, operation_name) in OPERATION_NAMES.iter().enumerate() {
            for (outcome, outcome_name) in OPERATION_OUTCOMES.iter().enumerate() {
                snapshots.push(LatencySeries {
                    operation: operation_name,
                    outcome: outcome_name,
                    histogram: self.shared.operation_latency[operation][outcome].snapshot(),
                });
            }
        }
        snapshots
    }

    pub fn local_io_latency(&self) -> Vec<LatencySeries> {
        let mut snapshots = Vec::new();
        for (operation, operation_name) in LOCAL_IO_NAMES.iter().enumerate() {
            for (outcome, outcome_name) in LOCAL_IO_OUTCOMES.iter().enumerate() {
                snapshots.push(LatencySeries {
                    operation: operation_name,
                    outcome: outcome_name,
                    histogram: self.shared.local_io_latency[operation][outcome].snapshot(),
                });
            }
        }
        snapshots
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

    /// Peer frames dropped on the floor so far (queue full or peer down)
    /// — the retry timers' workload, visible.
    pub fn peer_dropped_sends(&self) -> u64 {
        self.peers.as_ref().map_or(0, |p| {
            p.dropped_sends.load(std::sync::atomic::Ordering::SeqCst)
        })
    }

    pub fn peer_connections(&self) -> Vec<(blockd_core::types::HostId, bool)> {
        self.peers
            .as_ref()
            .map_or_else(Vec::new, |peers| peers.connections())
    }

    fn spawn_fault_reader(&self, vset: VsetId, host: Arc<VsetHost>) {
        let tx = self.tx.clone();
        let shared = self.shared.clone();
        let uffd = host
            .uffd
            .as_ref()
            .expect("compute vset has userfaultfd")
            .clone();
        uffd.set_nonblocking(true).expect("nonblocking userfaultfd");
        #[cfg(test)]
        let fault_reader_count = self.fault_reader_count.clone();
        self.fault_io.handle.spawn(async move {
            #[cfg(test)]
            let _active = {
                fault_reader_count.fetch_add(1, Ordering::SeqCst);
                ActiveFaultReader(fault_reader_count)
            };
            let uffd = AsyncFd::new(SharedUffd(uffd)).expect("register runtime userfaultfd");
            loop {
                let Ok(mut ready) = uffd.readable().await else {
                    return;
                };
                let events = match ready.try_io(|inner| inner.get_ref().0.read_events()) {
                    Ok(Ok(events)) => events,
                    Ok(Err(_)) => return,
                    Err(_) => continue,
                };
                for event in events {
                    let page = host.page_of_addr(vset, event.address & !(page_size() - 1));
                    let span = tracing::debug_span!(
                        "page.fault",
                        vset_id = vset.0,
                        volume = page.volume.idx.0,
                        page = page.page.0,
                        write = event.write,
                        source = tracing::field::Empty,
                        outcome = tracing::field::Empty,
                        duration_ms = tracing::field::Empty,
                    );
                    shared
                        .fault_in_flight
                        .lock()
                        .expect("lock")
                        .entry(page)
                        .or_default()
                        .push_back(FaultInFlight {
                            started: Instant::now(),
                            span,
                        });
                    tx.push(Msg::Ev(Event::GuestFault {
                        page,
                        write: event.write,
                    }));
                }
            }
        });
    }

    fn req(&self) -> ReqId {
        ReqId(self.shared.next_req.fetch_add(1, Ordering::SeqCst))
    }

    /// Wait for the next admin reply matching `want`, stashing others.
    fn wait_admin<T>(&self, mut want: impl FnMut(&AdminReply) -> Option<T>) -> T {
        {
            let mut backlog = self.admin_backlog.lock().expect("lock");
            for i in 0..backlog.len() {
                if let Some(out) = want(&backlog[i]) {
                    backlog.remove(i);
                    return out;
                }
            }
        }
        let rx = self.admin_rx.lock().expect("lock");
        loop {
            let reply = rx
                .recv_timeout(Duration::from_secs(30))
                .expect("admin reply within 30s");
            if let Some(out) = want(&reply) {
                return out;
            }
            self.admin_backlog.lock().expect("lock").push_back(reply);
        }
    }

    // ── admin surface ───────────────────────────────────────────────────

    #[tracing::instrument(
        skip(self, config),
        fields(vset_id = vset.0)
    )]
    pub fn create_vset(&self, vset: VsetId, config: VsetConfig) {
        let started = Instant::now();
        self.install_vset_host(vset, config);
        let req = self.req();
        self.tx.push(Msg::Ev(Event::Admin(AdminCmd::CreateVset {
            req,
            vset,
            config,
            from_base: None,
        })));
        let created = self.wait_admin(|reply| match reply {
            AdminReply::VsetCreated { req: r, vset: v } if *r == req && *v == vset => Some(true),
            AdminReply::AdminFailed { req: r } if *r == req => Some(false),
            _ => None,
        });
        self.observe_operation(0, created, started.elapsed());
        assert!(created, "vset creation failed");
    }

    #[tracing::instrument(skip(self), fields(vset_id = vset.0))]
    pub fn checkpoint(&self, vset: VsetId) -> u64 {
        let started = Instant::now();
        let req = self.req();
        self.expect_pause(vset, 0);
        self.tx
            .push(Msg::Ev(Event::Admin(AdminCmd::Checkpoint { req, vset })));
        let result = self.wait_admin(|reply| match reply {
            AdminReply::CheckpointDone { req: r, epoch, .. } if *r == req => Some(Some(epoch.0)),
            AdminReply::AdminFailed { req: r } if *r == req => Some(None),
            _ => None,
        });
        self.observe_operation(1, result.is_some(), started.elapsed());
        if result.is_none() {
            self.cancel_expected_pause(vset, 0);
        }
        result.expect("checkpoint failed")
    }

    pub fn attach_database(&self, vset: VsetId, vm: VmId) -> AttachmentId {
        self.try_attach_database(vset, vm)
            .expect("database attach failed")
    }

    pub fn try_attach_database(&self, vset: VsetId, vm: VmId) -> Option<AttachmentId> {
        let req = self.req();
        self.tx.push(Msg::Ev(Event::Admin(AdminCmd::AttachDatabase {
            req,
            vset,
            vm,
        })));
        self.wait_admin(|reply| match reply {
            AdminReply::DatabaseAttached {
                req: r,
                vset: got,
                attachment,
            } if *r == req && *got == vset => Some(Some(*attachment)),
            AdminReply::AdminFailed { req: r } if *r == req => Some(None),
            _ => None,
        })
    }

    pub fn begin_detach_database(
        &self,
        vset: VsetId,
        attachment: AttachmentId,
        mode: blockd_core::protocol::DetachMode,
    ) {
        let req = self.req();
        self.tx
            .push(Msg::Ev(Event::Admin(AdminCmd::BeginDetachDatabase {
                req,
                vset,
                attachment,
                mode,
            })));
        self.wait_admin(|reply| match reply {
            AdminReply::DatabaseDetachStarted { req: r, .. } if *r == req => Some(()),
            AdminReply::AdminFailed { req: r } if *r == req => {
                panic!("database detach failed")
            }
            _ => None,
        });
    }

    pub fn finish_detach_database(&self, vset: VsetId, attachment: AttachmentId) -> bool {
        let req = self.req();
        self.tx
            .push(Msg::Ev(Event::Admin(AdminCmd::FinishDetachDatabase {
                req,
                vset,
                attachment,
            })));
        self.wait_admin(|reply| match reply {
            AdminReply::DatabaseDetached { req: r, .. } if *r == req => Some(true),
            AdminReply::AdminFailed { req: r } if *r == req => Some(false),
            _ => None,
        })
    }

    /// Submit a decoded request whose VM identity was bound by the listener.
    /// The daemon still performs the authoritative attachment check.
    pub fn database_request(&self, mut request: DatabaseRequest) -> DatabaseReply {
        let caller_req = request.req;
        let req = self.req();
        request.req = req;
        let (done_tx, done_rx) = channel();
        {
            let mut waiters = self.shared.database_waiters.lock().expect("lock");
            match waiters.entry(req) {
                Entry::Occupied(_) => {
                    return DatabaseReply::Failed {
                        req: caller_req,
                        error: DatabaseError::Busy,
                    };
                }
                Entry::Vacant(slot) => {
                    slot.insert(done_tx);
                }
            }
        }
        self.tx.push(Msg::Ev(Event::Database(request)));
        if let Ok(reply) = done_rx.recv_timeout(Duration::from_secs(30)) {
            reply.with_req(caller_req)
        } else {
            self.shared
                .database_waiters
                .lock()
                .expect("lock")
                .remove(&req);
            DatabaseReply::Failed {
                req: caller_req,
                error: DatabaseError::Io,
            }
        }
    }

    /// Restore a backed-up vset from the store onto this host (R6.1).
    #[tracing::instrument(
        skip(self, config),
        fields(vset_id = vset.0)
    )]
    pub fn restore_vset(&self, vset: VsetId, config: VsetConfig) -> Verdict {
        let started = Instant::now();
        self.install_vset_host(vset, config);
        let req = self.req();
        self.tx
            .push(Msg::Ev(Event::Admin(AdminCmd::RestoreVset { req, vset })));
        let result = self.wait_admin(|reply| match reply {
            AdminReply::VsetRestored {
                req: r, verdict, ..
            } if *r == req => Some(Some(*verdict)),
            AdminReply::AdminFailed { req: r } if *r == req => Some(None),
            _ => None,
        });
        self.observe_operation(2, result.is_some(), started.elapsed());
        result.expect("restore failed")
    }

    /// Wait for a backed vset's post-recovery verdict (head refresh).
    pub fn wait_recovered(&self, vset: VsetId) -> Verdict {
        self.wait_admin(|reply| match reply {
            AdminReply::VsetRecovered { vset: v, verdict } if *v == vset => Some(*verdict),
            _ => None,
        })
    }

    /// Pre-create guest memory for a vset about to migrate IN. Must run on
    /// the destination BEFORE the source's `migrate_out`: the offer's
    /// effects (fills, the eventual resume) index this host's vsets and
    /// would find nothing otherwise.
    pub fn expect_migration(&self, vset: VsetId, config: VsetConfig) {
        self.install_vset_host(vset, config);
    }

    fn install_vset_host(&self, vset: VsetId, config: VsetConfig) {
        assert_peer_stash_transport(config, self.authenticated_peers);
        let host = VsetHost::new(config);
        self.shared
            .vsets
            .lock()
            .expect("lock")
            .insert(vset, host.clone());
        if config.kind == VsetKind::Compute {
            self.spawn_fault_reader(vset, host);
        }
    }

    /// Hand a protected vset off to `to` (R7.2): prepares, pauses, captures, makes
    /// the handoff durable on both sides, then serves the post-copy drain.
    /// Returns once this side's `MigratedOut` lands.
    #[tracing::instrument(skip(self), fields(vset_id = vset.0, destination_host = to.0))]
    pub fn migrate_out(&self, vset: VsetId, to: blockd_core::types::HostId) {
        let started = Instant::now();
        let req = self.req();
        self.expect_pause(vset, 1);
        self.tx.push(Msg::Ev(Event::Admin(AdminCmd::MigrateOut {
            req,
            vset,
            to,
        })));
        let migrated = self.wait_admin(|reply| match reply {
            AdminReply::MigratedOut { req: r, .. } if *r == req => Some(true),
            AdminReply::AdminFailed { req: r } if *r == req => Some(false),
            _ => None,
        });
        self.observe_operation(3, migrated, started.elapsed());
        if !migrated {
            self.cancel_expected_pause(vset, 1);
        }
        assert!(migrated, "migrate out failed");
    }

    /// Destination side: wait for the inbound migration's verdict (the
    /// moment this host's first record is durable and the vset runs here).
    pub fn wait_migrated_in(&self, vset: VsetId) -> Verdict {
        self.wait_admin(|reply| match reply {
            AdminReply::VsetMigratedIn { vset: v, verdict } if *v == vset => Some(*verdict),
            _ => None,
        })
    }

    /// The daemon's own counters (R9.2), as of the last step.
    pub fn counters(&self) -> blockd_core::daemon::Counters {
        *self.shared.counters.lock().expect("lock")
    }

    pub fn replica_metrics(&self) -> Vec<blockd_core::daemon::ReplicaVsetMetrics> {
        self.shared.replica_metrics.lock().expect("lock").clone()
    }

    pub fn replica_spool_metrics(&self) -> Vec<blockd_core::daemon::ReplicaSpoolMetrics> {
        self.shared
            .replica_spool_metrics
            .lock()
            .expect("lock")
            .clone()
    }

    pub fn incidents(&self) -> Vec<String> {
        self.shared.incidents.lock().expect("lock").clone()
    }

    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
    }

    /// Capacity and unprivileged-available bytes on the filesystem holding
    /// local durable blobs. This catches pressure outside the daemon's own
    /// accounting (logs, stale files, and other users of the mount).
    pub fn blob_filesystem_space(&self) -> Option<(u64, u64)> {
        let stats = rustix::fs::statvfs(&self.blob_dir).ok()?;
        Some((
            stats.f_blocks.saturating_mul(stats.f_frsize),
            stats.f_bavail.saturating_mul(stats.f_frsize),
        ))
    }

    pub fn database_dax_file(
        &self,
        vset: VsetId,
        file: blockd_core::database::DatabaseFile,
    ) -> std::io::Result<(std::fs::File, u64)> {
        let host = self
            .shared
            .vsets
            .lock()
            .expect("lock")
            .get(&vset)
            .cloned()
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
        if host.config.kind != VsetKind::Database {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let file_offset = u64::from(file.volume_index().0)
            * u64::from(host.config.pages_per_volume)
            * page_size() as u64;
        Ok((host.region.try_clone_file()?, file_offset))
    }

    // ── the guest boundary (called from workload threads) ───────────────

    fn host(&self, vset: VsetId) -> Arc<VsetHost> {
        self.shared.vsets.lock().expect("lock")[&vset].clone()
    }

    fn op_start(host: &VsetHost) {
        let mut state = host.ctl.state.lock().expect("lock");
        while state.pause_requested || state.paused {
            state = host.ctl.cv.wait(state).expect("wait");
        }
        state.in_op = true;
    }

    fn op_end(&self, vset: VsetId, host: &VsetHost) {
        let mut state = host.ctl.state.lock().expect("lock");
        state.in_op = false;
        state.applied += 1;
        if state.pause_requested && !state.paused {
            drop(state);
            self.tx.push(Msg::Quiesced(vset));
        }
    }

    /// A guest store to one 64-bit word of a page (the write path: WP or
    /// missing faults resolve through the daemon under the hood).
    pub fn guest_write(&self, vset: VsetId, page: PageId, value: u64) {
        let host = self.host(vset);
        Runtime::op_start(&host);
        host.view.write_word(host.page_index(page), value);
        self.op_end(vset, &host);
    }

    /// A guest load of a whole page.
    pub fn guest_read(&self, vset: VsetId, page: PageId) -> Vec<u8> {
        let host = self.host(vset);
        Runtime::op_start(&host);
        let bytes = host.view.read_page(host.page_index(page));
        self.op_end(vset, &host);
        bytes
    }

    /// A guest pmem sync barrier (R3.8): blocks until acknowledged.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(vset_id = vset.0, volume = volume.0)
    )]
    pub fn guest_sync(&self, vset: VsetId, volume: VolumeIdx) -> bool {
        let started = Instant::now();
        let host = self.host(vset);
        Runtime::op_start(&host);
        let req = self.req();
        let (done_tx, done_rx) = channel();
        self.shared
            .sync_waiters
            .lock()
            .expect("lock")
            .insert(req, done_tx);
        self.tx.push(Msg::Ev(Event::GuestSync {
            req,
            volume: VolumeId { vset, idx: volume },
        }));
        let ok = done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("sync ack within 30s");
        self.op_end(vset, &host);
        self.observe_operation(4, ok, started.elapsed());
        ok
    }

    /// The guest's applied-op counter (what vmstate refers to).
    pub fn guest_applied(&self, vset: VsetId) -> u64 {
        self.host(vset).ctl.state.lock().expect("lock").applied
    }

    /// Physical bytes the vset's guest memory holds right now (the
    /// backing memfd's page-cache residency).
    pub fn guest_resident_bytes(&self, vset: VsetId) -> usize {
        self.host(vset).region.resident_bytes().expect("resident")
    }

    fn observe_operation(&self, operation: usize, success: bool, elapsed: Duration) {
        self.shared.operation_latency[operation][usize::from(!success)].observe(elapsed);
    }

    fn expect_pause(&self, vset: VsetId, operation: usize) {
        self.shared
            .pause_expected
            .lock()
            .expect("lock")
            .entry(vset)
            .or_default()
            .push_back(operation);
    }

    fn cancel_expected_pause(&self, vset: VsetId, operation: usize) {
        let mut expected = self.shared.pause_expected.lock().expect("lock");
        if let Some(queue) = expected.get_mut(&vset)
            && let Some(position) = queue.iter().position(|candidate| *candidate == operation)
        {
            queue.remove(position);
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.tx.push(Msg::Stop);
        if let Some(handle) = self.loop_thread.take() {
            let _ = handle.join();
        }
        self.fault_io.shutdown();
    }
}

// ── effect interpretation ───────────────────────────────────────────────

fn fault_source_of_event(event: &Event) -> Option<FaultSource> {
    match event {
        Event::BlobReadDone { .. } => Some(FaultSource::Local),
        Event::StoreGetDone { .. } => Some(FaultSource::Store),
        Event::PeerDelivered {
            msg: blockd_core::protocol::PeerMsg::Page { .. },
            ..
        } => Some(FaultSource::Peer),
        _ => None,
    }
}

fn complete_fault(
    shared: &Shared,
    host: Option<&VsetHost>,
    page: PageId,
    source: FaultSource,
    outcome: &'static str,
) {
    let fault = {
        let mut pending = shared.fault_in_flight.lock().expect("lock");
        let fault = pending.get_mut(&page).and_then(VecDeque::pop_front);
        if pending.get(&page).is_some_and(VecDeque::is_empty) {
            pending.remove(&page);
        }
        fault
    };
    let Some(fault) = fault else {
        return; // unsolicited prefetch/hydration fill
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

    let slow = match source {
        FaultSource::Zero | FaultSource::Shared | FaultSource::WriteProtect => {
            elapsed >= Duration::from_millis(10)
        }
        FaultSource::Local => elapsed >= Duration::from_millis(5),
        FaultSource::Peer => elapsed >= Duration::from_millis(25),
        FaultSource::Store => elapsed >= Duration::from_secs(1),
        FaultSource::Unservable => true,
    };
    if slow {
        tracing::warn!(
            parent: &fault.span,
            vset_id = page.volume.vset.0,
            volume = page.volume.idx.0,
            page = page.page.0,
            source = source.name(),
            outcome,
            duration_ms = elapsed.as_secs_f64() * 1000.0,
            "slow or failed page fault"
        );
    }
}

fn complete_pause(shared: &Shared, vset: VsetId) {
    let started = shared.pause_in_flight.lock().expect("lock").remove(&vset);
    if let Some((operation, started)) = started {
        shared.pause_latency[operation].observe(started.elapsed());
    }
}

fn update_archive_lag(shared: &Shared, stats: &blockd_core::daemon::DaemonStats) {
    let now = Instant::now();
    let lagging: std::collections::BTreeSet<VsetId> = stats
        .vsets
        .iter()
        .filter(|vset| vset.archive_lag_captures.is_some_and(|lag| lag > 0))
        .map(|vset| vset.vset)
        .collect();
    let mut started = shared.archive_lag_started.lock().expect("lock");
    started.retain(|vset, _| lagging.contains(vset));
    for vset in lagging {
        started.entry(vset).or_insert(now);
    }
}

const BACKGROUND_OPERATIONS: [u8; 4] = [
    blockd_core::daemon::VsetOperations::CAPTURE,
    blockd_core::daemon::VsetOperations::CHECKPOINT,
    blockd_core::daemon::VsetOperations::BACKUP,
    blockd_core::daemon::VsetOperations::HYDRATION,
];

fn operation_name(operation: u8) -> &'static str {
    match operation {
        blockd_core::daemon::VsetOperations::CAPTURE => "capture",
        blockd_core::daemon::VsetOperations::CHECKPOINT => "checkpoint",
        blockd_core::daemon::VsetOperations::BACKUP => "backup",
        blockd_core::daemon::VsetOperations::HYDRATION => "hydration",
        _ => unreachable!("known background operation"),
    }
}

fn update_capacity_signal(shared: &Shared, queue: &LoopQueue) {
    let daemon = shared.daemon_stats.lock().expect("lock").clone();
    let (critical_queue_depth, background_queue_depth) = queue.depths();
    let local_io_in_flight = shared
        .local_io_in_flight
        .iter()
        .map(|value| value.load(Ordering::Relaxed))
        .sum();
    let oldest_backup_lag = shared
        .archive_lag_started
        .lock()
        .expect("lock")
        .values()
        .map(Instant::elapsed)
        .max()
        .unwrap_or_default();

    let replica_metrics = shared.replica_metrics.lock().expect("lock");
    let stash_missing = replica_metrics
        .iter()
        .any(|metric| metric.assignment_epoch.is_none() || metric.active_peer.is_none());
    let stash_replacement_active = replica_metrics
        .iter()
        .any(|metric| metric.transition_peer.is_some());
    drop(replica_metrics);

    let spool_metrics = shared.replica_spool_metrics.lock().expect("lock");
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
        loop_busy_ns: shared.stats.busy_ns(),
        loop_idle_ns: shared.stats.idle_ns(),
        critical_queue_depth,
        background_queue_depth,
        oldest_backup_lag,
        peer_spool_used_bytes,
        peer_spool_capacity_bytes,
        stash_missing,
        stash_replacement_active,
    };
    shared.capacity.lock().expect("lock").observe(inputs);
}

fn update_active_operations(shared: &Shared, stats: &blockd_core::daemon::DaemonStats) {
    let now = Instant::now();
    let active: std::collections::BTreeSet<(VsetId, u8)> = stats
        .vsets
        .iter()
        .flat_map(|vset| {
            BACKGROUND_OPERATIONS
                .into_iter()
                .filter(move |operation| vset.operations.active(*operation))
                .map(move |operation| (vset.vset, operation))
        })
        .collect();
    let mut started = shared.operation_started.lock().expect("lock");
    started.retain(|operation, _| active.contains(operation));
    for operation in active {
        started.entry(operation).or_insert(now);
    }
}

#[allow(clippy::too_many_lines)]
fn apply_effect(effect: Effect, context: &EffectContext, source: Option<FaultSource>) {
    let shared = &context.shared;
    let store_tx = &context.io.store;
    let blob_tx = &context.io.blob;
    let blob_delete_tx = &context.io.blob_delete;
    let replica_tx = &context.io.replica;
    let timer_tx = &context.io.timer;
    let admin_tx = &context.admin;
    let tx = &context.tx;
    let peers = context.peers.as_ref();
    let self_id = context.self_id;
    let fault_io = &context.fault_io;
    let fault_work = &context.fault_work;
    match effect {
        Effect::Fill {
            page,
            bytes,
            writable,
            share,
        } => {
            assert!(share.is_none(), "base sharing is not wired in e2e v1");
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            fault_work
                .blocking_send(FaultWork::Fill {
                    shared: shared.clone(),
                    host,
                    page,
                    bytes,
                    writable,
                    source: source.unwrap_or(FaultSource::Zero),
                })
                .expect("fault I/O runtime alive");
        }
        Effect::FillShared { .. } => unreachable!("base sharing is not wired in e2e v1"),
        Effect::FillFailed { page } => {
            complete_fault(shared, None, page, FaultSource::Unservable, "failed");
            // Unservable page: in production this SIGBUSes the guest. The
            // e2e scenarios never sanction it — die loudly right here.
            tracing::error!(?page, "fatal unservable page");
            std::process::abort();
        }
        Effect::Unprotect { page } => {
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            fault_work
                .blocking_send(FaultWork::Unprotect {
                    shared: shared.clone(),
                    host,
                    page,
                })
                .expect("fault I/O runtime alive");
        }
        Effect::WriteProtect { pages } => {
            let hosts = {
                let vsets = shared.vsets.lock().expect("lock");
                writeprotect_jobs(&vsets, &pages)
            };
            let (done, armed) = sync_channel(0);
            fault_work
                .blocking_send(FaultWork::WriteProtect { hosts, done })
                .expect("fault I/O runtime alive");
            armed.recv().expect("fault I/O runtime alive");
        }
        Effect::Evict { page } => {
            // Real reclaim: the daemon only evicts clean pages (their
            // segment is durable), so dropping the PTE AND punching the
            // backing genuinely frees RAM; the refault refills from the
            // local segment on disk (R2.4).
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            if host.config.kind == VsetKind::Compute {
                // Fill, unprotect, write-protect, and eviction all mutate
                // the same mapping. One FIFO preserves the daemon's effect
                // order even while completions run off the event loop.
                fault_work
                    .blocking_send(FaultWork::Evict { host, page })
                    .expect("fault I/O runtime alive");
            } else {
                let index = host.page_index(page);
                host.view.evict(index, 1).expect("evict");
                host.region.punch_hole(index, 1).expect("punch");
            }
        }
        Effect::DatabaseInstall { page, bytes } => {
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            host.region.write_page(host.page_index(page), &bytes);
        }
        Effect::Database(reply) => {
            if let Some(waiter) = shared
                .database_waiters
                .lock()
                .expect("lock")
                .remove(&reply.req())
            {
                let _ = waiter.send(reply);
            }
        }
        Effect::PauseGuest { vset } => {
            let operation = shared
                .pause_expected
                .lock()
                .expect("lock")
                .get_mut(&vset)
                .and_then(VecDeque::pop_front);
            if let Some(operation) = operation {
                shared
                    .pause_in_flight
                    .lock()
                    .expect("lock")
                    .insert(vset, (operation, Instant::now()));
            }
            let host = shared.vsets.lock().expect("lock")[&vset].clone();
            let mut state = host.ctl.state.lock().expect("lock");
            state.pause_requested = true;
            if state.in_op {
                // The op boundary sends Quiesced; GuestPaused follows.
            } else {
                drop(state);
                tx.push(Msg::Quiesced(vset));
            }
        }
        Effect::ResumeGuest { vset } => {
            complete_pause(shared, vset);
            let host = shared.vsets.lock().expect("lock")[&vset].clone();
            let mut state = host.ctl.state.lock().expect("lock");
            state.pause_requested = false;
            state.paused = false;
            drop(state);
            host.ctl.cv.notify_all();
        }
        Effect::SyncOk { req } => {
            if let Some(waiter) = shared.sync_waiters.lock().expect("lock").remove(&req) {
                let _ = waiter.send(true);
            }
        }
        Effect::SyncFailed { req } => {
            if let Some(waiter) = shared.sync_waiters.lock().expect("lock").remove(&req) {
                let _ = waiter.send(false);
            }
        }
        Effect::BlobWrite { io, name, bytes } => {
            blob_tx
                .send(BlobJob::Write { io, name, bytes })
                .expect("blob workers alive");
        }
        Effect::ReplicaAppend {
            io,
            source,
            vset,
            assignment_epoch,
            generation,
            bytes,
        } => {
            replica_tx
                .send(BlobJob::Append {
                    io,
                    name: layout::replica_spool_segment_blob(
                        source,
                        vset,
                        assignment_epoch,
                        generation,
                    ),
                    bytes,
                })
                .expect("replica worker alive");
        }
        Effect::ReplicaDelete {
            io,
            source,
            vset,
            assignment_epoch,
            through_generation,
        } => {
            replica_tx
                .send(BlobJob::DeleteManyDurable {
                    io,
                    names: (0..=through_generation)
                        .map(|generation| {
                            layout::replica_spool_segment_blob(
                                source,
                                vset,
                                assignment_epoch,
                                generation,
                            )
                        })
                        .collect(),
                })
                .expect("replica worker alive");
        }
        Effect::ReplicaTruncate {
            io,
            source,
            vset,
            assignment_epoch,
            generation,
            len,
        } => {
            replica_tx
                .send(BlobJob::Truncate {
                    io,
                    name: layout::replica_spool_segment_blob(
                        source,
                        vset,
                        assignment_epoch,
                        generation,
                    ),
                    len,
                })
                .expect("replica worker alive");
        }
        Effect::BlobRead { io, name } => {
            blob_tx
                .send(BlobJob::Read { io, name })
                .expect("blob workers alive");
        }
        Effect::BlobReadRange {
            io,
            name,
            offset,
            len,
        } => {
            blob_tx
                .send(BlobJob::ReadRange {
                    io,
                    name,
                    offset,
                    len,
                })
                .expect("blob workers alive");
        }
        Effect::BlobDelete { name } => {
            // The single ordered lane: see `spawn_io_workers` on why
            // reclaim deletes must not reorder.
            blob_delete_tx
                .send(BlobJob::Delete { name })
                .expect("blob delete worker alive");
        }
        Effect::SetTimer { timer, after } => {
            let _ = timer_tx.send((timer, after));
        }
        Effect::StorePut { io, key, bytes } => {
            store_tx
                .try_send(StoreJob::Put { io, key, bytes })
                .expect("store queue capacity exceeds daemon pipeline");
        }
        Effect::StoreCas {
            io,
            key,
            expected,
            bytes,
        } => {
            store_tx
                .try_send(StoreJob::Cas {
                    io,
                    key,
                    expected,
                    bytes,
                })
                .expect("store queue capacity exceeds daemon pipeline");
        }
        Effect::StoreGet { io, key } => {
            store_tx
                .try_send(StoreJob::Get { io, key })
                .expect("store queue capacity exceeds daemon pipeline");
        }
        Effect::StoreGetRange {
            io,
            key,
            offset,
            len,
        } => {
            store_tx
                .try_send(StoreJob::GetRange {
                    io,
                    key,
                    offset,
                    len,
                })
                .expect("store queue capacity exceeds daemon pipeline");
        }
        Effect::StoreDelete { key } => {
            store_tx
                .try_send(StoreJob::Delete { key })
                .expect("store queue capacity exceeds daemon pipeline");
        }
        Effect::VsetFenced { vset } => {
            tracing::warn!(vset_id = vset.0, "vset fenced by a newer assignment");
            shared
                .incidents
                .lock()
                .expect("lock")
                .push(format!("fenced: {vset:?}"));
        }
        Effect::VsetUnservable { page } => {
            tracing::error!(?page, "vset recovery material is unservable");
            shared
                .incidents
                .lock()
                .expect("lock")
                .push(format!("unservable: {:?}", page.volume.vset));
        }
        Effect::Admin(reply) => {
            if let AdminReply::MigratedOut { vset, .. } = reply {
                complete_pause(shared, vset);
            }
            let _ = admin_tx.send(reply);
        }
        Effect::PeerSend { to, msg } => {
            if let Some(net) = peers {
                let net = net.clone();
                fault_io.spawn_blocking(move || net.send(self_id, to, &msg));
            } else {
                tracing::warn!(
                    peer_host = to.0,
                    ?msg,
                    "peer send attempted without peer config"
                );
                shared
                    .incidents
                    .lock()
                    .expect("lock")
                    .push(format!("peer send to {to:?} with no peer config"));
            }
        }
        Effect::Abort { reason } => {
            tracing::error!(%reason, "fatal daemon abort");
            std::process::abort();
        }
    }
}

/// Sort, deduplicate, and visit the minimal contiguous runs in a page-index
/// batch. The callback keeps the production path allocation-free after the
/// per-vset grouping vector has been built.
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

/// Concurrent object-store round-trips. The daemon's own pipelines bound
/// what is outstanding (one publish per vset, deduped cold fetches); this
/// only caps the parallelism of what it issues.
const STORE_CRITICAL_WORKERS: usize = 2;
const STORE_BACKGROUND_WORKERS: usize = 6;
const STORE_QUEUE_CAPACITY: usize = 1024;

/// Local disks can service independent segment reads concurrently. This is
/// also the upper bound on simultaneous fsyncs issued by the runtime.
const BLOB_WORKERS: usize = 8;

enum BlobJob {
    Write {
        io: IoId,
        name: String,
        bytes: Vec<u8>,
    },
    Append {
        io: IoId,
        name: String,
        bytes: Vec<u8>,
    },
    Read {
        io: IoId,
        name: String,
    },
    ReadRange {
        io: IoId,
        name: String,
        offset: u64,
        len: u64,
    },
    Delete {
        name: String,
    },
    DeleteManyDurable {
        io: IoId,
        names: Vec<String>,
    },
    Truncate {
        io: IoId,
        name: String,
        len: u64,
    },
}

/// Sync directory entries from the immediate parent through the blob root,
/// preserving the child-to-root durability order.
fn fsync_to_root(root: &Path, parent: &Path) -> std::io::Result<()> {
    let mut dir = parent;
    loop {
        std::fs::File::open(dir)?.sync_all()?;
        if dir == root {
            return Ok(());
        }
        dir = dir.parent().expect("under root");
    }
}

#[allow(clippy::too_many_lines)]
fn blob_worker_loop(
    rx: &Arc<Mutex<Receiver<BlobJob>>>,
    root: &Path,
    tx: &Arc<LoopQueue>,
    shared: &Shared,
) {
    loop {
        let Ok(job) = rx.lock().expect("lock").recv() else {
            return;
        };
        let event = match job {
            BlobJob::Write { io, name, bytes } => {
                let started = Instant::now();
                shared.local_io_in_flight[0].fetch_add(1, Ordering::Relaxed);
                let path = root.join(name);
                let parent = path.parent().expect("has parent");
                std::fs::create_dir_all(parent).expect("mkdir");
                std::fs::write(&path, bytes).expect("blob write");
                std::fs::File::open(&path)
                    .expect("open")
                    .sync_all()
                    .expect("fsync");
                // Durability includes the directory entries: a record acked
                // as durable must survive power loss of a freshly created
                // path, so fsync every directory up to the blob root.
                fsync_to_root(root, parent).expect("fsync dirs");
                shared.local_io_in_flight[0].fetch_sub(1, Ordering::Relaxed);
                shared.local_io_latency[0][0].observe(started.elapsed());
                Some(Event::BlobWriteDone { io })
            }
            BlobJob::Append { io, name, bytes } => {
                let path = root.join(name);
                let parent = path.parent().expect("has parent");
                std::fs::create_dir_all(parent).expect("mkdir");
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .expect("open replica spool");
                file.write_all(&bytes).expect("append replica frame");
                file.sync_all().expect("fsync replica spool");
                fsync_to_root(root, parent).expect("fsync dirs");
                Some(Event::BlobWriteDone { io })
            }
            BlobJob::Read { io, name } => {
                let started = Instant::now();
                shared.local_io_in_flight[1].fetch_add(1, Ordering::Relaxed);
                let bytes = std::fs::File::open(root.join(name))
                    .ok()
                    .and_then(|mut file| {
                        let mut buf = Vec::new();
                        file.read_to_end(&mut buf).ok().map(|_| buf)
                    });
                shared.local_io_in_flight[1].fetch_sub(1, Ordering::Relaxed);
                let outcome = usize::from(bytes.is_none());
                shared.local_io_latency[1][outcome].observe(started.elapsed());
                Some(Event::BlobReadDone { io, bytes })
            }
            BlobJob::ReadRange {
                io,
                name,
                offset,
                len,
            } => {
                let started = Instant::now();
                shared.local_io_in_flight[2].fetch_add(1, Ordering::Relaxed);
                let bytes = std::fs::File::open(root.join(name)).ok().and_then(|file| {
                    let mut buf = vec![0u8; usize::try_from(len).expect("fits")];
                    file.read_exact_at(&mut buf, offset).ok().map(|()| buf)
                });
                shared.local_io_in_flight[2].fetch_sub(1, Ordering::Relaxed);
                let outcome = usize::from(bytes.is_none());
                shared.local_io_latency[2][outcome].observe(started.elapsed());
                Some(Event::BlobReadDone { io, bytes })
            }
            BlobJob::Delete { name } => {
                let started = Instant::now();
                shared.local_io_in_flight[3].fetch_add(1, Ordering::Relaxed);
                let _ = std::fs::remove_file(root.join(name));
                shared.local_io_in_flight[3].fetch_sub(1, Ordering::Relaxed);
                shared.local_io_latency[3][0].observe(started.elapsed());
                None
            }
            BlobJob::DeleteManyDurable { io, names } => {
                let result = (|| -> std::io::Result<()> {
                    let mut parents = std::collections::BTreeSet::new();
                    for name in names {
                        let path = root.join(name);
                        parents.insert(path.parent().expect("has parent").to_path_buf());
                        match std::fs::remove_file(path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => return Err(error),
                        }
                    }
                    for parent in parents {
                        fsync_to_root(root, &parent)?;
                    }
                    Ok(())
                })();
                match result {
                    Ok(()) => Some(Event::BlobWriteDone { io }),
                    Err(_) => Some(Event::ReplicaDeleteFailed { io }),
                }
            }
            BlobJob::Truncate { io, name, len } => {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(root.join(name))
                    .expect("open torn replica spool");
                file.set_len(len).expect("truncate replica tail");
                file.sync_all().expect("fsync replica truncation");
                Some(Event::BlobWriteDone { io })
            }
        };
        if let Some(event) = event {
            tx.push(Msg::Ev(event));
        }
    }
}

/// One object-store operation, executed off the event loop; its completion
/// returns as an event. Deletes are fire-and-forget (R4.5 reclaim).
enum StoreJob {
    Put {
        io: IoId,
        key: String,
        bytes: Vec<u8>,
    },
    Cas {
        io: IoId,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    },
    Get {
        io: IoId,
        key: String,
    },
    GetRange {
        io: IoId,
        key: String,
        offset: u64,
        len: u64,
    },
    Delete {
        key: String,
    },
}

#[derive(Clone)]
struct StoreSenders {
    critical: mpsc::Sender<StoreJob>,
    background: mpsc::Sender<StoreJob>,
}

impl StoreSenders {
    fn try_send(&self, job: StoreJob) -> Result<(), mpsc::error::TrySendError<StoreJob>> {
        if matches!(job, StoreJob::Get { .. } | StoreJob::GetRange { .. }) {
            self.critical.try_send(job)
        } else {
            self.background.try_send(job)
        }
    }
}

async fn store_worker_loop(
    mut rx: mpsc::Receiver<StoreJob>,
    store: Arc<dyn ObjectStore>,
    tx: Arc<LoopQueue>,
    concurrency: usize,
) {
    let concurrency = Arc::new(tokio::sync::Semaphore::new(concurrency));
    while let Some(job) = rx.recv().await {
        let permit = concurrency
            .clone()
            .acquire_owned()
            .await
            .expect("store executor open");
        let store = store.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let event = match job {
                StoreJob::Put { io, key, bytes } => Event::StorePutDone {
                    io,
                    result: store.put(key, bytes).await,
                },
                StoreJob::Cas {
                    io,
                    key,
                    expected,
                    bytes,
                } => Event::StorePutDone {
                    io,
                    result: store.put_cas(key, expected, bytes).await,
                },
                StoreJob::Get { io, key } => Event::StoreGetDone {
                    io,
                    result: store.get(key).await,
                },
                StoreJob::GetRange {
                    io,
                    key,
                    offset,
                    len,
                } => Event::StoreGetDone {
                    io,
                    result: store.get_range(key, offset, len).await,
                },
                StoreJob::Delete { key } => {
                    store.delete(key).await;
                    return;
                }
            };
            tx.push(Msg::Ev(event));
        });
    }
}

fn timer_loop(rx: &Receiver<(TimerId, u64)>, tx: &Arc<LoopQueue>) {
    let mut armed: Vec<(Instant, TimerId)> = Vec::new();
    loop {
        let now = Instant::now();
        // Fire everything due.
        let mut i = 0;
        while i < armed.len() {
            if armed[i].0 <= now {
                let (_, timer) = armed.remove(i);
                tx.push(Msg::Ev(Event::Timer(timer)));
            } else {
                i += 1;
            }
        }
        let wait = armed
            .iter()
            .map(|(at, _)| at.saturating_duration_since(now))
            .min()
            .unwrap_or(Duration::from_millis(50));
        match rx.recv_timeout(wait.max(Duration::from_micros(200))) {
            Ok((timer, after)) => {
                armed.push((now + Duration::from_nanos(after), timer));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Spawn the asynchronous store executor and the blocking local-I/O workers;
/// only
/// senders come back — completions return to the loop as events.
///
/// - Timer thread: real clock, feeding `Timer` events back in.
/// - Store executor: object-store sockets are polled by Tokio with eight
///   requests in flight at most; completions return as daemon events.
/// - Blob workers: local blob I/O has the same completion-event
///   contract — one slow filesystem operation (fsync included) must not
///   stall guest faults.
fn spawn_io_workers(
    blob_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    tx: &Arc<LoopQueue>,
    shared: &Arc<Shared>,
) -> IoLanes {
    let (timer_tx, timer_rx) = channel::<(TimerId, u64)>();
    {
        let tx = tx.clone();
        thread::spawn(move || timer_loop(&timer_rx, &tx));
    }

    let (store_critical_tx, store_critical_rx) = mpsc::channel::<StoreJob>(STORE_QUEUE_CAPACITY);
    let (store_background_tx, store_background_rx) =
        mpsc::channel::<StoreJob>(STORE_QUEUE_CAPACITY);
    let store_tx = StoreSenders {
        critical: store_critical_tx,
        background: store_background_tx,
    };
    {
        let store = store.clone();
        let tx = tx.clone();
        thread::Builder::new()
            .name("blockd-store-io".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("store I/O runtime");
                runtime.block_on(async move {
                    tokio::join!(
                        store_worker_loop(
                            store_critical_rx,
                            store.clone(),
                            tx.clone(),
                            STORE_CRITICAL_WORKERS,
                        ),
                        store_worker_loop(
                            store_background_rx,
                            store,
                            tx,
                            STORE_BACKGROUND_WORKERS,
                        ),
                    );
                });
            })
            .expect("spawn store I/O runtime");
    }

    let (blob_tx, blob_rx) = channel::<BlobJob>();
    {
        let blob_rx = Arc::new(Mutex::new(blob_rx));
        for _ in 0..BLOB_WORKERS {
            let blob_dir = blob_dir.to_path_buf();
            let tx = tx.clone();
            let blob_rx = blob_rx.clone();
            let shared = shared.clone();
            thread::spawn(move || blob_worker_loop(&blob_rx, &blob_dir, &tx, &shared));
        }
    }

    // Deletes get ONE lane of their own: reclaim order is load-bearing.
    // `released` deletes a vset's records BEFORE its handoff marker, so a
    // crash mid-reclaim can never leave records on disk without the
    // marker that says they were handed off — recovery would resurrect a
    // stale owner. The read/write pool would reorder them.
    let (blob_delete_tx, blob_delete_rx) = channel::<BlobJob>();
    {
        let blob_delete_rx = Arc::new(Mutex::new(blob_delete_rx));
        let blob_dir = blob_dir.to_path_buf();
        let tx = tx.clone();
        let shared = shared.clone();
        thread::spawn(move || blob_worker_loop(&blob_delete_rx, &blob_dir, &tx, &shared));
    }

    // Replica appends are ordered. The core permits one append in flight per
    // spool, and this lane preserves submission order across real fsyncs.
    let (replica_tx, replica_rx) = channel::<BlobJob>();
    {
        let replica_rx = Arc::new(Mutex::new(replica_rx));
        let blob_dir = blob_dir.to_path_buf();
        let tx = tx.clone();
        let shared = shared.clone();
        thread::spawn(move || blob_worker_loop(&replica_rx, &blob_dir, &tx, &shared));
    }

    IoLanes {
        store: store_tx,
        blob: blob_tx,
        blob_delete: blob_delete_tx,
        replica: replica_tx,
        timer: timer_tx,
    }
}

/// The job senders `spawn_io_workers` hands back, one per lane.
struct IoLanes {
    store: StoreSenders,
    blob: Sender<BlobJob>,
    blob_delete: Sender<BlobJob>,
    replica: Sender<BlobJob>,
    timer: Sender<(TimerId, u64)>,
}

struct EffectContext {
    shared: Arc<Shared>,
    io: IoLanes,
    admin: Sender<AdminReply>,
    tx: Arc<LoopQueue>,
    peers: Option<Arc<PeerNet>>,
    self_id: blockd_core::types::HostId,
    fault_io: tokio::runtime::Handle,
    fault_work: tokio::sync::mpsc::Sender<FaultWork>,
}

fn elapsed_ns(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).expect("fits")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use super::*;

    struct SaturatedStore {
        uploads_started: AtomicUsize,
        read_completed: AtomicBool,
        release_uploads: tokio::sync::Semaphore,
    }

    #[async_trait::async_trait]
    impl ObjectStore for SaturatedStore {
        async fn put(
            self: Arc<Self>,
            _key: String,
            _bytes: Vec<u8>,
        ) -> Result<u64, blockd_core::protocol::StoreFault> {
            self.uploads_started.fetch_add(1, Ordering::SeqCst);
            self.release_uploads
                .acquire()
                .await
                .expect("release semaphore open")
                .forget();
            Ok(1)
        }

        async fn put_cas(
            self: Arc<Self>,
            _key: String,
            _expected: Option<u64>,
            _bytes: Vec<u8>,
        ) -> Result<u64, blockd_core::protocol::StoreFault> {
            unreachable!("profile only submits unconditional uploads")
        }

        async fn get(self: Arc<Self>, _key: String) -> crate::store::GetResult {
            self.read_completed.store(true, Ordering::SeqCst);
            Ok(None)
        }

        async fn get_range(
            self: Arc<Self>,
            _key: String,
            _offset: u64,
            _len: u64,
        ) -> crate::store::GetResult {
            self.read_completed.store(true, Ordering::SeqCst);
            Ok(None)
        }

        async fn delete(self: Arc<Self>, _key: String) {}
    }

    fn test_shared() -> Arc<Shared> {
        Arc::new(Shared::new(
            BTreeMap::new(),
            blockd_core::daemon::DaemonStats::default(),
            Vec::new(),
            Vec::new(),
        ))
    }

    #[test]
    fn dropping_runtime_stops_its_fault_reader() {
        let root =
            std::env::temp_dir().join(format!("blockd-fault-reader-drop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let runtime = Runtime::new(
            &RuntimeConfig {
                daemon: DaemonConfig {
                    archive: Default::default(),
                    host: blockd_core::types::HostId(0),
                    cache_pages: 8,
                    writeback_interval: blockd_core::types::millis(5),
                    backup_retry: blockd_core::types::millis(20),
                    disk_capacity: None,
                    disk_headroom: 0,
                    wedge_ticks: 25,
                    replica_placement: None,
                },
                blob_dir: root.clone(),
                peer: None,
            },
            Arc::new(crate::s3::S3Store::new()),
        );
        let fault_reader_count = runtime.fault_reader_count.clone();
        runtime.create_vset(VsetId(99), VsetConfig::compute(1, 1));
        let deadline = Instant::now() + Duration::from_secs(1);
        while fault_reader_count.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            thread::yield_now();
        }
        assert_eq!(fault_reader_count.load(Ordering::SeqCst), 1);

        drop(runtime);
        let deadline = Instant::now() + Duration::from_millis(250);
        while fault_reader_count.load(Ordering::SeqCst) != 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(fault_reader_count.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(root).expect("cleanup test blobs");
    }

    #[test]
    fn daemon_observability_refreshes_only_on_timer_events() {
        let root = std::env::temp_dir().join(format!(
            "blockd-observability-cadence-{}-{:?}",
            std::process::id(),
            thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let runtime = Runtime::new(
            &RuntimeConfig {
                daemon: DaemonConfig {
                    archive: Default::default(),
                    host: blockd_core::types::HostId(0),
                    cache_pages: 8,
                    writeback_interval: blockd_core::types::secs(60),
                    backup_retry: blockd_core::types::millis(20),
                    disk_capacity: None,
                    disk_headroom: 0,
                    wedge_ticks: 25,
                    replica_placement: None,
                },
                blob_dir: root.clone(),
                peer: None,
            },
            Arc::new(crate::s3::S3Store::new()),
        );

        let first = VsetId(1);
        runtime.create_vset(first, VsetConfig::database(1));
        assert!(runtime.daemon_stats().vsets.is_empty());

        runtime.tx.push(Msg::Ev(Event::Timer(TimerId::Writeback)));
        runtime.create_vset(VsetId(2), VsetConfig::database(1));
        let published = runtime.daemon_stats();
        assert_eq!(
            published
                .vsets
                .iter()
                .map(|stats| stats.vset)
                .collect::<Vec<_>>(),
            [first]
        );

        drop(runtime);
        std::fs::remove_dir_all(root).expect("cleanup test blobs");
    }

    #[test]
    fn contiguous_pages_collapse_to_minimal_ranges() {
        let mut indices = vec![9, 3, 4, 4, 5, 12, 11, 20];
        let mut runs = Vec::new();
        for_each_contiguous_run(&mut indices, |start, len| runs.push((start, len)));
        assert_eq!(runs, [(3, 3), (9, 1), (11, 2), (20, 1)]);

        let mut empty = Vec::new();
        for_each_contiguous_run(&mut empty, |_, _| panic!("empty input has no runs"));
    }

    #[test]
    fn store_reads_have_admission_when_background_queue_is_full() {
        let (critical_tx, mut critical_rx) = mpsc::channel(1);
        let (background_tx, _background_rx) = mpsc::channel(1);
        let senders = StoreSenders {
            critical: critical_tx,
            background: background_tx,
        };
        senders
            .try_send(StoreJob::Put {
                io: IoId(1),
                key: "large".to_owned(),
                bytes: vec![0; 64],
            })
            .expect("first background job fits");
        assert!(matches!(
            senders.try_send(StoreJob::Put {
                io: IoId(2),
                key: "blocked".to_owned(),
                bytes: Vec::new(),
            }),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        senders
            .try_send(StoreJob::GetRange {
                io: IoId(3),
                key: "fault".to_owned(),
                offset: 9,
                len: 4096,
            })
            .expect("critical queue remains available");
        assert!(matches!(
            critical_rx.try_recv().expect("critical job queued"),
            StoreJob::GetRange { io: IoId(3), .. }
        ));
    }

    #[test]
    fn store_read_worker_runs_while_every_upload_worker_is_blocked() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let store = Arc::new(SaturatedStore {
                uploads_started: AtomicUsize::new(0),
                read_completed: AtomicBool::new(false),
                release_uploads: tokio::sync::Semaphore::new(0),
            });
            let tx = LoopQueue::new();
            let (critical_tx, critical_rx) = mpsc::channel(STORE_QUEUE_CAPACITY);
            let (background_tx, background_rx) = mpsc::channel(STORE_QUEUE_CAPACITY);
            let critical = tokio::spawn(store_worker_loop(
                critical_rx,
                store.clone(),
                tx.clone(),
                STORE_CRITICAL_WORKERS,
            ));
            let background = tokio::spawn(store_worker_loop(
                background_rx,
                store.clone(),
                tx,
                STORE_BACKGROUND_WORKERS,
            ));
            for io in 0..STORE_BACKGROUND_WORKERS {
                background_tx
                    .send(StoreJob::Put {
                        io: IoId(u64::try_from(io).expect("fits")),
                        key: format!("upload-{io}"),
                        bytes: Vec::new(),
                    })
                    .await
                    .expect("background worker open");
            }
            tokio::time::timeout(Duration::from_secs(1), async {
                while store.uploads_started.load(Ordering::SeqCst) < STORE_BACKGROUND_WORKERS {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("all upload workers saturated");

            critical_tx
                .send(StoreJob::GetRange {
                    io: IoId(99),
                    key: "fault".to_owned(),
                    offset: 0,
                    len: 4096,
                })
                .await
                .expect("critical worker open");
            tokio::time::timeout(Duration::from_secs(1), async {
                while !store.read_completed.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("fault read bypassed saturated uploads");

            store.release_uploads.add_permits(STORE_BACKGROUND_WORKERS);
            drop(critical_tx);
            drop(background_tx);
            critical.await.expect("critical worker");
            background.await.expect("background worker");
        });
    }

    #[test]
    fn queued_blob_io_does_not_block_followup_effects() {
        let shared = test_shared();
        let (store_critical_tx, _store_critical_rx) = mpsc::channel(1);
        let (store_background_tx, _store_background_rx) = mpsc::channel(1);
        let store_tx = StoreSenders {
            critical: store_critical_tx,
            background: store_background_tx,
        };
        // No worker receives from this channel: it models every disk worker
        // being occupied by a slow filesystem operation.
        let (blob_tx, blob_rx) = channel();
        let (blob_delete_tx, _blob_delete_rx) = channel();
        let (replica_tx, _replica_rx) = channel();
        let (timer_tx, timer_rx) = channel();
        let (admin_tx, _admin_rx) = channel();
        let tx = LoopQueue::new();
        let mut fault_io = spawn_fault_io_runtime();
        let context = EffectContext {
            shared: shared.clone(),
            io: IoLanes {
                store: store_tx,
                blob: blob_tx,
                blob_delete: blob_delete_tx,
                replica: replica_tx,
                timer: timer_tx,
            },
            admin: admin_tx,
            tx,
            peers: None,
            self_id: blockd_core::types::HostId(0),
            fault_io: fault_io.handle.clone(),
            fault_work: fault_io.work.clone(),
        };

        apply_effect(
            Effect::BlobWrite {
                io: IoId(7),
                name: "held/blob".to_owned(),
                bytes: b"payload".to_vec(),
            },
            &context,
            None,
        );
        apply_effect(
            Effect::SetTimer {
                timer: TimerId::Backup(VsetId(9)),
                after: 123,
            },
            &context,
            None,
        );

        assert!(matches!(
            blob_rx.try_recv().expect("blob job was enqueued"),
            BlobJob::Write {
                io: IoId(7),
                name,
                bytes,
            } if name == "held/blob" && bytes == b"payload"
        ));
        assert_eq!(
            timer_rx.try_recv().expect("follow-up effect ran"),
            (TimerId::Backup(VsetId(9)), 123)
        );
        fault_io.shutdown();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn blob_worker_writes_durably_and_reads_ranges() {
        let root = std::env::temp_dir().join(format!(
            "blockd-blob-worker-{}-{:?}",
            std::process::id(),
            thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let (job_tx, job_rx) = channel();
        let job_rx = Arc::new(Mutex::new(job_rx));
        let msg_queue = LoopQueue::new();
        let shared = test_shared();
        let worker = {
            let root = root.clone();
            let msg_queue = msg_queue.clone();
            let shared = shared.clone();
            thread::spawn(move || blob_worker_loop(&job_rx, &root, &msg_queue, &shared))
        };

        job_tx
            .send(BlobJob::Write {
                io: IoId(1),
                name: "nested/blob".to_owned(),
                bytes: b"abcdefgh".to_vec(),
            })
            .expect("send write");
        assert!(matches!(
            msg_queue.pop(),
            Msg::Ev(Event::BlobWriteDone { io: IoId(1) })
        ));

        job_tx
            .send(BlobJob::ReadRange {
                io: IoId(2),
                name: "nested/blob".to_owned(),
                offset: 2,
                len: 4,
            })
            .expect("send read");
        assert!(matches!(
            msg_queue.pop(),
            Msg::Ev(Event::BlobReadDone {
                io: IoId(2),
                bytes: Some(bytes),
            })
            if bytes == b"cdef"
        ));

        job_tx
            .send(BlobJob::Append {
                io: IoId(3),
                name: "r/0000/0000000000000007/0000000000000001.spool".to_owned(),
                bytes: b"first".to_vec(),
            })
            .expect("send append");
        assert!(matches!(
            msg_queue.pop(),
            Msg::Ev(Event::BlobWriteDone { io: IoId(3) })
        ));
        job_tx
            .send(BlobJob::Append {
                io: IoId(4),
                name: "r/0000/0000000000000007/0000000000000001.spool".to_owned(),
                bytes: b"second".to_vec(),
            })
            .expect("send append");
        assert!(matches!(
            msg_queue.pop(),
            Msg::Ev(Event::BlobWriteDone { io: IoId(4) })
        ));
        assert_eq!(
            std::fs::read(root.join("r/0000/0000000000000007/0000000000000001.spool"))
                .expect("spool exists"),
            b"firstsecond"
        );
        job_tx
            .send(BlobJob::Truncate {
                io: IoId(5),
                name: "r/0000/0000000000000007/0000000000000001.spool".to_owned(),
                len: 5,
            })
            .expect("send durable tail truncation");
        assert!(matches!(
            msg_queue.pop(),
            Msg::Ev(Event::BlobWriteDone { io: IoId(5) })
        ));
        assert_eq!(
            std::fs::read(root.join("r/0000/0000000000000007/0000000000000001.spool"))
                .expect("truncated spool exists"),
            b"first"
        );
        job_tx
            .send(BlobJob::DeleteManyDurable {
                io: IoId(6),
                names: vec!["r/0000/0000000000000007/0000000000000001.spool".to_owned()],
            })
            .expect("send durable delete");
        assert!(matches!(
            msg_queue.pop(),
            Msg::Ev(Event::BlobWriteDone { io: IoId(6) })
        ));
        assert!(
            !root
                .join("r/0000/0000000000000007/0000000000000001.spool")
                .exists()
        );

        drop(job_tx);
        worker.join().expect("worker");
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn durable_delete_does_not_acknowledge_a_failed_unlink() {
        let root = std::env::temp_dir().join(format!(
            "blockd-blob-delete-failure-{}-{:?}",
            std::process::id(),
            thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let undeletable_as_file = root.join("nested/not-a-file");
        std::fs::create_dir_all(&undeletable_as_file).expect("create directory target");
        let (job_tx, job_rx) = channel();
        let job_rx = Arc::new(Mutex::new(job_rx));
        let msg_queue = LoopQueue::new();
        let worker = {
            let root = root.clone();
            let msg_queue = msg_queue.clone();
            let shared = test_shared();
            thread::spawn(move || blob_worker_loop(&job_rx, &root, &msg_queue, &shared))
        };

        job_tx
            .send(BlobJob::DeleteManyDurable {
                io: IoId(77),
                names: vec!["nested/not-a-file".to_owned()],
            })
            .expect("send failing durable delete");
        let completion = msg_queue.pop();
        drop(job_tx);
        let _ = worker.join();
        std::fs::remove_dir_all(root).expect("cleanup");

        assert!(matches!(
            completion,
            Msg::Ev(Event::ReplicaDeleteFailed { io: IoId(77) })
        ));
    }
}
