//! Production host for the shared async protocol actors.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use blockd_core::database::{AttachmentId, DatabaseError, DatabaseReply, DatabaseRequest};
use blockd_core::engine::{HostFatal, HostState, host_actor_with_state};
use blockd_core::hostmeta::{
    Counters, DaemonStats, HostConfig, ReplicaSpoolMetrics, ReplicaVsetMetrics, VsetOperations,
};
use blockd_core::journal::{VsetConfig, VsetKind};
use blockd_core::protocol::{
    AdminCall, AdminEvent, AdminResult, AdminSuccess, PeerMsg, ReqId, Verdict,
};
use blockd_core::types::{HostId, PageId, PageNo, VmId, VolumeId, VolumeIdx, VsetId};
use blockd_core::world::{
    AdminIo, AdminRequest, BlobEntry, BlobError, Blobs, DatabaseActorRequest, FillSource,
    GuestFault, GuestMem, GuestMemoryError, GuestPause, GuestSync, GuestSyncRequest, Peers, Store,
    StoreError,
};
use blockd_exec::inject::{Injected, Injector, Lane, bounded_injector, injector};
use blockd_exec::{Executor, bridge_request, delay};
use blockd_hostmem::{GuestView, HostRegion, Uffd, UffdFeatures, page_size};
use tokio::io::unix::AsyncFd;

use crate::loopstats::{LoopStats, world_kind};
use crate::metrics::{AtomicHistogram, FaultLatency, LatencySeries};
use crate::peer::{PeerConfig, PeerNet};
use crate::store::ObjectStore;
use crate::world::{FileBlobs, RuntimeStore};
use crate::{CapacityController, CapacityInputs, CapacitySignal};

pub struct RuntimeConfig {
    pub daemon: HostConfig,
    pub blob_dir: PathBuf,
    pub peer: Option<PeerConfig>,
}

fn assert_peer_stash_transport(config: VsetConfig, authenticated: bool) {
    let _ = config;
    assert!(
        authenticated,
        "passive durability requires mutually authenticated TLS"
    );
}

struct SharedUffd(Arc<Uffd>);

impl AsRawFd for SharedUffd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

struct VsetHost {
    config: VsetConfig,
    region: Arc<HostRegion>,
    view: Arc<GuestView>,
    uffd: Option<Arc<Uffd>>,
    ctl: GuestCtl,
    fault_latency: [AtomicHistogram; FaultSource::COUNT],
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
    pause_generation: u64,
    in_op: bool,
    applied: u64,
    pause_waiter: Option<Injector<u64>>,
}

impl VsetHost {
    fn new(config: VsetConfig) -> Arc<Self> {
        let pages = (usize::from(config.disk_volumes) + 1)
            * usize::try_from(config.pages_per_volume).expect("page count fits");
        let region = Arc::new(HostRegion::new(pages).expect("guest region"));
        let view = Arc::new(GuestView::map(&region, 0, pages).expect("guest view"));
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
                "kernel lacks required userfaultfd features: {features:?}"
            );
            uffd.register_all(&view).expect("register guest view");
            Arc::new(uffd)
        });
        Arc::new(Self {
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
            * usize::try_from(self.config.pages_per_volume).expect("page count fits")
            + usize::try_from(page.page.0).expect("page index fits")
    }

    fn page_of_addr(&self, vset: VsetId, addr: usize) -> PageId {
        let index = (addr - self.view.addr_of(0)) / page_size();
        let per = usize::try_from(self.config.pages_per_volume).expect("page count fits");
        PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(u8::try_from(index / per).expect("volume index fits")),
            },
            page: PageNo(u32::try_from(index % per).expect("page number fits")),
        }
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

const OPERATION_NAMES: [&str; 5] = ["create", "checkpoint", "restore", "migration", "sync"];
const OPERATION_OUTCOMES: [&str; 2] = ["success", "failed"];
const LOCAL_IO_NAMES: [&str; 4] = ["write", "read", "ranged_read", "delete"];
const LOCAL_IO_OUTCOMES: [&str; 2] = ["success", "missing"];
const PAUSE_NAMES: [&str; 2] = ["checkpoint", "migration"];

struct Shared {
    vsets: Mutex<BTreeMap<VsetId, Arc<VsetHost>>>,
    admin_events: Mutex<VecDeque<AdminEvent>>,
    admin_event_ready: Condvar,
    incidents: Mutex<Vec<String>>,
    counters: Mutex<Counters>,
    daemon_stats: Mutex<DaemonStats>,
    replica_metrics: Mutex<Vec<ReplicaVsetMetrics>>,
    replica_spool_metrics: Mutex<Vec<ReplicaSpoolMetrics>>,
    capacity: Mutex<CapacityController>,
    stats: LoopStats,
    fault_in_flight: Mutex<BTreeMap<PageId, VecDeque<FaultInFlight>>>,
    operation_latency: [[AtomicHistogram; OPERATION_OUTCOMES.len()]; OPERATION_NAMES.len()],
    local_io_latency: [[AtomicHistogram; LOCAL_IO_OUTCOMES.len()]; LOCAL_IO_NAMES.len()],
    local_io_in_flight: [AtomicU64; LOCAL_IO_NAMES.len()],
    pause_expected: Mutex<BTreeMap<VsetId, VecDeque<usize>>>,
    pause_in_flight: Mutex<BTreeMap<VsetId, (usize, Instant)>>,
    pause_latency: [AtomicHistogram; PAUSE_NAMES.len()],
    backup_lag_started: Mutex<BTreeMap<VsetId, Instant>>,
    operation_started: Mutex<BTreeMap<(VsetId, u8), Instant>>,
    next_req: AtomicU64,
}

impl Shared {
    fn new(vsets: BTreeMap<VsetId, Arc<VsetHost>>, config: &HostConfig) -> Self {
        let state = HostState::new(config.clone());
        Self {
            vsets: Mutex::new(vsets),
            admin_events: Mutex::new(VecDeque::new()),
            admin_event_ready: Condvar::new(),
            incidents: Mutex::new(Vec::new()),
            counters: Mutex::new(Counters::default()),
            daemon_stats: Mutex::new(state.stats()),
            replica_metrics: Mutex::new(Vec::new()),
            replica_spool_metrics: Mutex::new(Vec::new()),
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
            backup_lag_started: Mutex::new(BTreeMap::new()),
            operation_started: Mutex::new(BTreeMap::new()),
            next_req: AtomicU64::new(1),
        }
    }
}

struct PendingGuestPause {
    shared: Arc<Shared>,
    host: Arc<VsetHost>,
    vset: VsetId,
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
        complete_pause(&self.shared, self.vset);
        self.host.ctl.cv.notify_all();
    }
}

enum FaultWork {
    Fill {
        host: Arc<VsetHost>,
        page: PageId,
        bytes: Vec<u8>,
        writable: bool,
        reply: Injector<Result<(), ()>>,
    },
    Unprotect {
        host: Arc<VsetHost>,
        page: PageId,
        reply: Injector<Result<(), ()>>,
    },
    Evict {
        host: Arc<VsetHost>,
        page: PageId,
        reply: Injector<Result<(), ()>>,
    },
    WriteProtect {
        hosts: Vec<(Arc<VsetHost>, Vec<usize>)>,
        reply: Injector<Result<(), ()>>,
    },
    Install {
        host: Arc<VsetHost>,
        page: PageId,
        bytes: Vec<u8>,
        reply: Injector<Result<(), ()>>,
    },
    Barrier {
        done: SyncSender<()>,
    },
}

struct FaultIo {
    handle: tokio::runtime::Handle,
    work: tokio::sync::mpsc::UnboundedSender<FaultWork>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl FaultIo {
    fn shutdown(&mut self) {
        let (done, drained) = sync_channel(0);
        if self.work.send(FaultWork::Barrier { done }).is_ok() {
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
    let (work, mut work_rx) = tokio::sync::mpsc::unbounded_channel();
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
            host,
            page,
            bytes,
            writable,
            reply,
        } => {
            let index = host.page_index(page);
            host.region.write_page(index, &bytes);
            let result = host
                .uffd
                .as_ref()
                .expect("compute fill")
                .continue_range(host.view.addr_of(index), page_size(), !writable)
                .map_err(|_| ());
            let _ = reply.push(Lane::Critical, result);
            return;
        }
        FaultWork::Unprotect { host, page, reply } => {
            let index = host.page_index(page);
            let result = host
                .uffd
                .as_ref()
                .expect("compute unprotect")
                .writeprotect(host.view.addr_of(index), page_size(), false)
                .map_err(|_| ());
            let _ = reply.push(Lane::Critical, result);
            return;
        }
        FaultWork::Evict { host, page, reply } => {
            let index = host.page_index(page);
            let result = host
                .view
                .evict(index, 1)
                .and_then(|()| host.region.punch_hole(index, 1))
                .map_err(|_| ());
            let _ = reply.push(Lane::Critical, result);
        }
        FaultWork::WriteProtect { hosts, reply } => {
            let mut result = Ok(());
            for (host, mut indices) in hosts {
                let uffd = host.uffd.as_ref().expect("compute write protection");
                for_each_contiguous_run(&mut indices, |start, len| {
                    if result.is_ok() {
                        result = uffd
                            .writeprotect(host.view.addr_of(start), len * page_size(), true)
                            .map_err(|_| ());
                    }
                });
            }
            let _ = reply.push(Lane::Critical, result);
            return;
        }
        FaultWork::Install {
            host,
            page,
            bytes,
            reply,
        } => {
            host.region.write_page(host.page_index(page), &bytes);
            let _ = reply.push(Lane::Critical, Ok(()));
            return;
        }
        FaultWork::Barrier { done } => {
            let _ = done.send(());
            return;
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

struct ProductionWorld {
    blobs: FileBlobs,
    store: RuntimeStore,
    peers: Option<Arc<PeerNet>>,
    self_id: HostId,
    peer_rx: Injected<(HostId, PeerMsg)>,
    fault_rx: Injected<GuestFault>,
    sync_rx: Injected<GuestSyncRequest>,
    admin_rx: Injected<AdminRequest>,
    database_rx: Injected<DatabaseActorRequest>,
    shared: Arc<Shared>,
    fault_work: tokio::sync::mpsc::UnboundedSender<FaultWork>,
    shared_pages: RefCell<BTreeMap<(u64, u64, blockd_core::types::SegId, u32), Vec<u8>>>,
}

impl ProductionWorld {
    fn host(&self, vset: VsetId) -> Arc<VsetHost> {
        self.shared.vsets.lock().expect("vset lock")[&vset].clone()
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
    fn protocol_version(&self, to: HostId) -> u16 {
        self.peers
            .as_ref()
            .map_or(0, |peers| peers.protocol_version(to))
    }

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
        let host = self.host(page.volume.vset);
        host.region.read_page(host.page_index(page))
    }

    async fn arm_write_protect(&self, pages: &[PageId]) -> Result<(), GuestMemoryError> {
        let hosts = {
            let mut by_vset = BTreeMap::<VsetId, Vec<usize>>::new();
            let vsets = self.shared.vsets.lock().expect("vset lock");
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
                .map(|(vset, pages)| (Arc::clone(&vsets[&vset]), pages))
                .collect()
        };
        let (reply, response) = injector();
        self.fault_work
            .send(FaultWork::WriteProtect { hosts, reply })
            .map_err(|_| GuestMemoryError::Unavailable)?;
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
        let host = self.host(page.volume.vset);
        let (reply, response) = injector();
        self.fault_work
            .send(FaultWork::Fill {
                host: Arc::clone(&host),
                page,
                bytes,
                writable,
                reply,
            })
            .map_err(|_| GuestMemoryError::Unavailable)?;
        self.fault_response(&response, world_kind::FILL).await?;
        complete_fault(&self.shared, Some(&host), page, source.into(), "served");
        Ok(())
    }

    async fn fill_shared(
        &self,
        page: PageId,
        share: (u64, u64, blockd_core::types::SegId, u32),
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
        let host = self.host(page.volume.vset);
        let (reply, response) = injector();
        self.fault_work
            .send(FaultWork::Fill {
                host: Arc::clone(&host),
                page,
                bytes,
                writable,
                reply,
            })
            .map_err(|_| GuestMemoryError::Unavailable)?;
        self.fault_response(&response, world_kind::FILL_SHARED)
            .await?;
        complete_fault(
            &self.shared,
            Some(&host),
            page,
            FaultSource::Shared,
            "served",
        );
        Ok(())
    }

    async fn fail(&self, page: PageId) -> Result<(), GuestMemoryError> {
        self.shared.stats.record_world(world_kind::FILL_FAILED, 0);
        complete_fault(&self.shared, None, page, FaultSource::Unservable, "failed");
        tracing::error!(?page, "fatal unservable guest page");
        Err(GuestMemoryError::Unservable)
    }

    async fn unprotect(&self, page: PageId) -> Result<(), GuestMemoryError> {
        let host = self.host(page.volume.vset);
        let (reply, response) = injector();
        self.fault_work
            .send(FaultWork::Unprotect {
                host: Arc::clone(&host),
                page,
                reply,
            })
            .map_err(|_| GuestMemoryError::Unavailable)?;
        self.fault_response(&response, world_kind::UNPROTECT)
            .await?;
        complete_fault(
            &self.shared,
            Some(&host),
            page,
            FaultSource::WriteProtect,
            "served",
        );
        Ok(())
    }

    async fn evict(&self, page: PageId) -> Result<(), GuestMemoryError> {
        let host = self.host(page.volume.vset);
        let (reply, response) = injector();
        self.fault_work
            .send(FaultWork::Evict { host, page, reply })
            .map_err(|_| GuestMemoryError::Unavailable)?;
        self.fault_response(&response, world_kind::EVICT).await
    }

    async fn install_database(&self, page: PageId, bytes: Vec<u8>) -> Result<(), GuestMemoryError> {
        let (reply, response) = injector();
        self.fault_work
            .send(FaultWork::Install {
                host: self.host(page.volume.vset),
                page,
                bytes,
                reply,
            })
            .map_err(|_| GuestMemoryError::Unavailable)?;
        self.fault_response(&response, world_kind::DATABASE_INSTALL)
            .await
    }

    async fn pause(&self, vset: VsetId) -> Result<GuestPause, GuestMemoryError> {
        let started = Instant::now();
        begin_pause(&self.shared, vset);
        let host = self.host(vset);
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
            vset,
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
            generation,
        })
    }

    async fn resume(
        &self,
        vset: VsetId,
        pause: Option<GuestPause>,
    ) -> Result<(), GuestMemoryError> {
        let host = self.host(vset);
        let mut state = host.ctl.state.lock().expect("guest control lock");
        if pause.is_some_and(|pause| pause.generation != state.pause_generation) {
            return Ok(());
        }
        state.pause_requested = false;
        state.paused = false;
        drop(state);
        complete_pause(&self.shared, vset);
        host.ctl.cv.notify_all();
        self.shared.stats.record_world(world_kind::RESUME_GUEST, 0);
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

    async fn fence(&self, vset: VsetId) -> Result<(), GuestMemoryError> {
        self.shared
            .incidents
            .lock()
            .expect("incident lock")
            .push(format!("fenced: {vset:?}"));
        self.shared.stats.record_world(world_kind::VSET_FENCED, 0);
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
        self.shared.admin_event_ready.notify_all();
        self.shared.stats.record_world(world_kind::ADMIN, 0);
    }

    async fn next_database(&self) -> Option<DatabaseActorRequest> {
        self.database_rx.recv().await
    }

    async fn host_failed(&self, failure: HostFatal) {
        tracing::error!(reason = failure.reason, "fatal host actor failure");
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
    database: Injector<DatabaseActorRequest>,
    faults: Injector<GuestFault>,
    syncs: Injector<GuestSyncRequest>,
    peers: Injector<(HostId, PeerMsg)>,
    stop: Injector<()>,
}

const PEER_INPUT_CAPACITY: usize = 4;

impl Inputs {
    fn depths(&self) -> (usize, usize) {
        [
            self.admin.depths(),
            self.database.depths(),
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
    fault_io: FaultIo,
    authenticated_peers: bool,
    loop_thread: Option<thread::JoinHandle<()>>,
}

impl Runtime {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(config: &RuntimeConfig, store: Arc<dyn ObjectStore>) -> Self {
        Self::start(BTreeMap::new(), config, store)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn recover(
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
        vset_configs: &BTreeMap<VsetId, VsetConfig>,
    ) -> (Self, BTreeMap<VsetId, Verdict>) {
        let mut hosts = BTreeMap::new();
        for (&vset, &vset_config) in vset_configs {
            assert_peer_stash_transport(
                vset_config,
                config.peer.as_ref().is_some_and(|peer| peer.tls.is_some()),
            );
            hosts.insert(vset, VsetHost::new(vset_config));
        }
        let runtime = Self::start(hosts, config, store);
        (runtime, BTreeMap::new())
    }

    #[allow(clippy::too_many_lines)]
    fn start(
        hosts: BTreeMap<VsetId, Arc<VsetHost>>,
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        std::fs::create_dir_all(&config.blob_dir).expect("blob directory");
        let fault_io = spawn_fault_io_runtime();
        let (admin, admin_rx_actor) = injector();
        let (database, database_rx_actor) = injector();
        let (faults, fault_rx_actor) = injector();
        let (syncs, sync_rx_actor) = injector();
        let (peer_input, peer_rx_actor) = bounded_injector(PEER_INPUT_CAPACITY);
        let (stop, stop_rx_actor) = injector();
        let inputs = Inputs {
            admin,
            database,
            faults,
            syncs,
            peers: peer_input,
            stop,
        };
        let shared = Arc::new(Shared::new(hosts, &config.daemon));
        let peers = config.peer.as_ref().map(|peer_config| {
            let incoming = inputs.peers.clone();
            PeerNet::start(peer_config, config.daemon.host, move |from, message| {
                let lane = peer_lane(&message);
                let _ = incoming.push(lane, (from, message));
            })
        });
        let authenticated_peers = peers.as_ref().is_some_and(|peers| peers.authenticated());
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("store runtime");
        let blobs = FileBlobs::new(&config.blob_dir).expect("file blob world");
        let runtime_store = RuntimeStore::new(tokio.handle().clone(), store);
        let (ready, started) = sync_channel(0);
        let actor_config = config.daemon.clone();
        let actor_shared = Arc::clone(&shared);
        let actor_inputs = inputs.clone();
        let actor_peers = peers.clone();
        let fault_work = fault_io.work.clone();
        let monotonic_epoch = Instant::now();
        let loop_thread = thread::Builder::new()
            .name("blockd-actor-host".to_owned())
            .spawn(move || {
                let world = Rc::new(ProductionWorld {
                    blobs,
                    store: runtime_store,
                    peers: actor_peers,
                    self_id: actor_config.host,
                    peer_rx: peer_rx_actor,
                    fault_rx: fault_rx_actor,
                    sync_rx: sync_rx_actor,
                    admin_rx: admin_rx_actor,
                    database_rx: database_rx_actor,
                    shared: Arc::clone(&actor_shared),
                    fault_work,
                    shared_pages: RefCell::new(BTreeMap::new()),
                });
                let clock = Arc::new(move || elapsed_ns(monotonic_epoch.elapsed()));
                let mut executor = Executor::production_with_clock(clock);
                let state = Rc::new(RefCell::new(HostState::new(actor_config.clone())));
                executor
                    .spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)))
                    .detach();
                let stopped = Rc::new(Cell::new(false));
                let stop_flag = Rc::clone(&stopped);
                executor
                    .spawn(async move {
                        let _ = stop_rx_actor.recv().await;
                        stop_flag.set(true);
                    })
                    .detach();
                let observation_state = Rc::clone(&state);
                let observation_shared = Arc::clone(&actor_shared);
                let observation_inputs = actor_inputs.clone();
                let observation_interval = actor_config.writeback_interval.clamp(1, 1_000_000);
                executor
                    .spawn(async move {
                        loop {
                            publish_observability(
                                &observation_shared,
                                &observation_state,
                                &observation_inputs,
                            );
                            delay(observation_interval).await;
                        }
                    })
                    .detach();
                ready.send(()).expect("runtime owner alive");
                while !stopped.get() {
                    let busy = Instant::now();
                    executor.run_until_stalled();
                    actor_shared
                        .stats
                        .record_actor_poll(elapsed_ns(busy.elapsed()));
                    if stopped.get() {
                        break;
                    }
                    let idle = Instant::now();
                    executor.wait_for_wake();
                    actor_shared.stats.record_idle(elapsed_ns(idle.elapsed()));
                }
                publish_observability(&actor_shared, &state, &actor_inputs);
                drop(tokio);
            })
            .expect("spawn actor host");
        started.recv().expect("actor host started");

        let runtime = Self {
            inputs,
            shared,
            blob_dir: config.blob_dir.clone(),
            peers,
            fault_io,
            authenticated_peers,
            loop_thread: Some(loop_thread),
        };
        let hosts = runtime
            .shared
            .vsets
            .lock()
            .expect("vset lock")
            .iter()
            .map(|(&vset, host)| (vset, Arc::clone(host)))
            .collect::<Vec<_>>();
        for (vset, host) in hosts {
            if host.config.kind == VsetKind::Compute {
                runtime.spawn_fault_reader(vset, host);
            }
        }
        runtime
    }

    pub fn loop_stats(&self) -> &LoopStats {
        &self.shared.stats
    }

    pub fn loop_queue_depths(&self) -> (usize, usize) {
        self.inputs.depths()
    }

    pub fn daemon_stats(&self) -> DaemonStats {
        self.shared.daemon_stats.lock().expect("stats lock").clone()
    }

    pub fn capacity_signal(&self) -> CapacitySignal {
        self.shared.capacity.lock().expect("capacity lock").signal()
    }

    pub fn backup_lag_age(&self) -> Vec<(VsetId, Duration)> {
        self.shared
            .backup_lag_started
            .lock()
            .expect("lag lock")
            .iter()
            .map(|(&vset, started)| (vset, started.elapsed()))
            .collect()
    }

    pub fn active_operation_age(&self) -> Vec<(VsetId, &'static str, Duration)> {
        self.shared
            .operation_started
            .lock()
            .expect("operation lock")
            .iter()
            .map(|(&(vset, operation), started)| {
                (vset, operation_name(operation), started.elapsed())
            })
            .collect()
    }

    pub fn fault_latency(&self) -> Vec<FaultLatency> {
        let active = self
            .shared
            .daemon_stats
            .lock()
            .expect("stats lock")
            .vsets
            .iter()
            .map(|stats| stats.vset)
            .collect::<BTreeSet<_>>();
        let vsets = self.shared.vsets.lock().expect("vset lock");
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

    fn spawn_fault_reader(&self, vset: VsetId, host: Arc<VsetHost>) {
        let faults = self.inputs.faults.clone();
        let shared = Arc::clone(&self.shared);
        let uffd = host
            .uffd
            .as_ref()
            .expect("compute vset has userfaultfd")
            .clone();
        uffd.set_nonblocking(true).expect("nonblocking userfaultfd");
        self.fault_io.handle.spawn(async move {
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
                            },
                        )
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
    }

    fn req(&self) -> ReqId {
        ReqId(self.shared.next_req.fetch_add(1, Ordering::SeqCst))
    }

    fn admin_request(&self, call: AdminCall) -> AdminResult {
        let (request, reply) = bridge_request(call);
        self.inputs
            .admin
            .push(Lane::Background, request)
            .unwrap_or_else(|_| panic!("actor host alive"));
        reply
            .blocking_recv_timeout(Duration::from_secs(30))
            .expect("admin reply within 30 seconds")
    }

    fn wait_admin_event<T>(&self, mut want: impl FnMut(&AdminEvent) -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut events = self.shared.admin_events.lock().expect("admin event lock");
        loop {
            if let Some((index, output)) = events
                .iter()
                .enumerate()
                .find_map(|(index, event)| want(event).map(|output| (index, output)))
            {
                events.remove(index);
                return output;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "admin event within 30 seconds");
            let (next, timeout) = self
                .shared
                .admin_event_ready
                .wait_timeout(events, remaining)
                .expect("admin event lock");
            events = next;
            assert!(!timeout.timed_out(), "admin event within 30 seconds");
        }
    }

    pub fn create_vset(&self, vset: VsetId, config: VsetConfig) {
        let started = Instant::now();
        self.install_vset_host(vset, config);
        let created = match self.admin_request(AdminCall::CreateVset {
            vset,
            config,
            from_base: None,
        }) {
            Ok(AdminSuccess::VsetCreated { vset: found }) if found == vset => true,
            Err(_) => false,
            result => panic!("unexpected create result: {result:?}"),
        };
        self.observe_operation(0, created, started.elapsed());
        assert!(created, "vset creation failed");
    }

    pub fn checkpoint(&self, vset: VsetId) -> u64 {
        let started = Instant::now();
        let req = self.req();
        self.expect_pause(vset, 0);
        let result = match self.admin_request(AdminCall::Checkpoint { retry: req, vset }) {
            Ok(AdminSuccess::CheckpointDone { epoch, .. }) => Some(epoch.0),
            Err(_) => None,
            result => panic!("unexpected checkpoint result: {result:?}"),
        };
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
        match self.admin_request(AdminCall::AttachDatabase { vset, vm }) {
            Ok(AdminSuccess::DatabaseAttached {
                vset: found_vset,
                attachment,
            }) if found_vset == vset => Some(attachment),
            Err(_) => None,
            result => panic!("unexpected database attach result: {result:?}"),
        }
    }

    pub fn begin_detach_database(
        &self,
        vset: VsetId,
        attachment: AttachmentId,
        mode: blockd_core::protocol::DetachMode,
    ) {
        match self.admin_request(AdminCall::BeginDetachDatabase {
            vset,
            attachment,
            mode,
        }) {
            Ok(AdminSuccess::DatabaseDetachStarted { .. }) => {}
            Err(error) => panic!("database detach failed: {error:?}"),
            result => panic!("unexpected database detach result: {result:?}"),
        }
    }

    pub fn finish_detach_database(&self, vset: VsetId, attachment: AttachmentId) -> bool {
        match self.admin_request(AdminCall::FinishDetachDatabase { vset, attachment }) {
            Ok(AdminSuccess::DatabaseDetached { .. }) => true,
            Err(_) => false,
            result => panic!("unexpected database detach result: {result:?}"),
        }
    }

    pub fn database_request(&self, request: DatabaseRequest) -> DatabaseReply {
        let (req, call) = request.into_call();
        let (request, reply) = bridge_request(call);
        self.inputs
            .database
            .push(Lane::Background, request)
            .unwrap_or_else(|_| panic!("actor host alive"));
        let result = reply
            .blocking_recv_timeout(Duration::from_secs(30))
            .unwrap_or(Err(DatabaseError::Io));
        DatabaseReply::from_result(req, result)
    }

    pub fn restore_vset(&self, vset: VsetId, config: VsetConfig) -> Verdict {
        let started = Instant::now();
        self.install_vset_host(vset, config);
        let result = match self.admin_request(AdminCall::RestoreVset { vset }) {
            Ok(AdminSuccess::VsetRestored { verdict, .. }) => Some(verdict),
            Err(_) => None,
            result => panic!("unexpected restore result: {result:?}"),
        };
        self.observe_operation(2, result.is_some(), started.elapsed());
        result.expect("restore failed")
    }

    pub fn wait_recovered(&self, vset: VsetId) -> Verdict {
        self.wait_admin_event(|event| match event {
            AdminEvent::VsetRecovered {
                vset: found,
                verdict,
            } if *found == vset => Some(*verdict),
            _ => None,
        })
    }

    pub fn expect_migration(&self, vset: VsetId, config: VsetConfig) {
        self.install_vset_host(vset, config);
    }

    fn install_vset_host(&self, vset: VsetId, config: VsetConfig) {
        assert_peer_stash_transport(config, self.authenticated_peers);
        let host = VsetHost::new(config);
        self.shared
            .vsets
            .lock()
            .expect("vset lock")
            .insert(vset, Arc::clone(&host));
        if config.kind == VsetKind::Compute {
            self.spawn_fault_reader(vset, host);
        }
    }

    pub fn migrate_out(&self, vset: VsetId, to: HostId) {
        let started = Instant::now();
        self.expect_pause(vset, 1);
        let migrated = match self.admin_request(AdminCall::MigrateOut { vset, to }) {
            Ok(AdminSuccess::MigratedOut { .. }) => true,
            Err(_) => false,
            result => panic!("unexpected migration result: {result:?}"),
        };
        self.observe_operation(3, migrated, started.elapsed());
        if migrated {
            complete_pause(&self.shared, vset);
        } else {
            self.cancel_expected_pause(vset, 1);
        }
        assert!(migrated, "migrate out failed");
    }

    pub fn wait_migrated_in(&self, vset: VsetId) -> Verdict {
        self.wait_admin_event(|event| match event {
            AdminEvent::VsetMigratedIn {
                vset: found,
                verdict,
            } if *found == vset => Some(*verdict),
            _ => None,
        })
    }

    pub fn counters(&self) -> Counters {
        *self.shared.counters.lock().expect("counter lock")
    }

    pub fn replica_metrics(&self) -> Vec<ReplicaVsetMetrics> {
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

    pub fn database_dax_file(
        &self,
        vset: VsetId,
        file: blockd_core::database::DatabaseFile,
    ) -> std::io::Result<(std::fs::File, u64)> {
        let host = self
            .shared
            .vsets
            .lock()
            .expect("vset lock")
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

    fn host(&self, vset: VsetId) -> Arc<VsetHost> {
        self.shared.vsets.lock().expect("vset lock")[&vset].clone()
    }

    fn op_start(host: &VsetHost) {
        let mut state = host.ctl.state.lock().expect("guest control lock");
        while state.pause_requested || state.paused {
            state = host.ctl.cv.wait(state).expect("guest control wait");
        }
        state.in_op = true;
    }

    fn op_end(host: &VsetHost) {
        let mut state = host.ctl.state.lock().expect("guest control lock");
        state.in_op = false;
        state.applied = state.applied.saturating_add(1);
        if state.pause_requested && !state.paused {
            state.paused = true;
            let applied = state.applied;
            if let Some(waiter) = state.pause_waiter.take() {
                let _ = waiter.push(Lane::Critical, applied);
            }
        }
    }

    pub fn guest_write(&self, vset: VsetId, page: PageId, value: u64) {
        let host = self.host(vset);
        Self::op_start(&host);
        host.view.write_word(host.page_index(page), value);
        Self::op_end(&host);
    }

    pub fn guest_read(&self, vset: VsetId, page: PageId) -> Vec<u8> {
        let host = self.host(vset);
        Self::op_start(&host);
        let bytes = host.view.read_page(host.page_index(page));
        Self::op_end(&host);
        bytes
    }

    pub fn guest_sync(&self, vset: VsetId, volume: VolumeIdx) -> bool {
        let started = Instant::now();
        let host = self.host(vset);
        Self::op_start(&host);
        let req = self.req();
        let (request, reply) = bridge_request(GuestSync {
            req,
            volume: VolumeId { vset, idx: volume },
        });
        self.inputs
            .syncs
            .push(Lane::Critical, request)
            .unwrap_or_else(|_| panic!("actor host alive"));
        let ok = reply
            .blocking_recv_timeout(Duration::from_secs(30))
            .expect("sync reply within 30 seconds");
        Self::op_end(&host);
        self.observe_operation(4, ok, started.elapsed());
        ok
    }

    pub fn guest_applied(&self, vset: VsetId) -> u64 {
        self.host(vset)
            .ctl
            .state
            .lock()
            .expect("guest control lock")
            .applied
    }

    pub fn guest_resident_bytes(&self, vset: VsetId) -> usize {
        self.host(vset)
            .region
            .resident_bytes()
            .expect("resident byte query")
    }

    fn observe_operation(&self, operation: usize, success: bool, elapsed: Duration) {
        self.shared.operation_latency[operation][usize::from(!success)].observe(elapsed);
    }

    fn expect_pause(&self, vset: VsetId, operation: usize) {
        self.shared
            .pause_expected
            .lock()
            .expect("pause lock")
            .entry(vset)
            .or_default()
            .push_back(operation);
    }

    fn cancel_expected_pause(&self, vset: VsetId, operation: usize) {
        let mut expected = self.shared.pause_expected.lock().expect("pause lock");
        if let Some(queue) = expected.get_mut(&vset)
            && let Some(position) = queue.iter().position(|candidate| *candidate == operation)
        {
            queue.remove(position);
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.inputs.stop.push(Lane::Critical, ());
        if let Some(handle) = self.loop_thread.take() {
            let _ = handle.join();
        }
        self.fault_io.shutdown();
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
    if matches!(
        message,
        PeerMsg::Page { .. }
            | PeerMsg::Leaf { .. }
            | PeerMsg::FetchRange { .. }
            | PeerMsg::FetchLeaf { .. }
    ) {
        Lane::Critical
    } else {
        Lane::Background
    }
}

fn elapsed_ns(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

fn complete_fault(
    shared: &Shared,
    host: Option<&VsetHost>,
    page: PageId,
    source: FaultSource,
    outcome: &'static str,
) {
    let fault = {
        let mut pending = shared.fault_in_flight.lock().expect("fault lock");
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

fn complete_pause(shared: &Shared, vset: VsetId) {
    if let Some((operation, started)) = shared
        .pause_in_flight
        .lock()
        .expect("pause lock")
        .remove(&vset)
    {
        shared.pause_latency[operation].observe(started.elapsed());
    }
}

fn begin_pause(shared: &Shared, vset: VsetId) {
    let operation = shared
        .pause_expected
        .lock()
        .expect("pause lock")
        .get_mut(&vset)
        .and_then(VecDeque::pop_front);
    if let Some(operation) = operation {
        shared
            .pause_in_flight
            .lock()
            .expect("pause lock")
            .insert(vset, (operation, Instant::now()));
    }
}

const BACKGROUND_OPERATIONS: [u8; 4] = [
    VsetOperations::CAPTURE,
    VsetOperations::CHECKPOINT,
    VsetOperations::BACKUP,
    VsetOperations::HYDRATION,
];

fn operation_name(operation: u8) -> &'static str {
    match operation {
        VsetOperations::CAPTURE => "capture",
        VsetOperations::CHECKPOINT => "checkpoint",
        VsetOperations::BACKUP => "backup",
        VsetOperations::HYDRATION => "hydration",
        _ => unreachable!("known background operation"),
    }
}

fn publish_observability(shared: &Shared, state: &Rc<RefCell<HostState>>, inputs: &Inputs) {
    let state = state.borrow();
    let daemon = state.stats();
    *shared.counters.lock().expect("counter lock") = state.counters;
    *shared.daemon_stats.lock().expect("stats lock") = daemon.clone();
    *shared.replica_metrics.lock().expect("replica metric lock") = state.replica_metrics();
    *shared
        .replica_spool_metrics
        .lock()
        .expect("replica spool metric lock") = state.replica_spool_metrics();
    drop(state);
    update_backup_lag(shared, &daemon);
    update_active_operations(shared, &daemon);
    update_capacity_signal(shared, &daemon, inputs);
}

fn update_backup_lag(shared: &Shared, stats: &DaemonStats) {
    let now = Instant::now();
    let lagging = stats
        .vsets
        .iter()
        .filter(|vset| vset.archive_lag_captures.is_some_and(|lag| lag > 0))
        .map(|vset| vset.vset)
        .collect::<BTreeSet<_>>();
    let mut started = shared.backup_lag_started.lock().expect("lag lock");
    started.retain(|vset, _| lagging.contains(vset));
    for vset in lagging {
        started.entry(vset).or_insert(now);
    }
}

fn update_active_operations(shared: &Shared, stats: &DaemonStats) {
    let now = Instant::now();
    let active = stats
        .vsets
        .iter()
        .flat_map(|vset| {
            BACKGROUND_OPERATIONS
                .into_iter()
                .filter(move |operation| vset.operations.active(*operation))
                .map(move |operation| (vset.vset, operation))
        })
        .collect::<BTreeSet<_>>();
    let mut started = shared.operation_started.lock().expect("operation lock");
    started.retain(|operation, _| active.contains(operation));
    for operation in active {
        started.entry(operation).or_insert(now);
    }
}

fn update_capacity_signal(shared: &Shared, daemon: &DaemonStats, actor_inputs: &Inputs) {
    let local_io_in_flight = shared
        .local_io_in_flight
        .iter()
        .map(|value| value.load(Ordering::Relaxed))
        .sum();
    let oldest_backup_lag = shared
        .backup_lag_started
        .lock()
        .expect("lag lock")
        .values()
        .map(Instant::elapsed)
        .max()
        .unwrap_or_default();
    let replica_metrics = shared.replica_metrics.lock().expect("replica metric lock");
    let stash_missing = replica_metrics
        .iter()
        .any(|metric| metric.assignment_epoch.is_none() || metric.active_peer.is_none());
    let stash_replacement_active = replica_metrics
        .iter()
        .any(|metric| metric.transition_peer.is_some());
    drop(replica_metrics);
    let spool_metrics = shared
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
    shared
        .capacity
        .lock()
        .expect("capacity lock")
        .observe(inputs);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_host_config() -> HostConfig {
        HostConfig {
            archive: Default::default(),
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

    #[test]
    fn concurrent_admin_requests_keep_out_of_order_replies_with_their_callers() {
        let (admin, incoming) = injector::<AdminRequest>();
        let actor = thread::spawn(move || {
            let mut executor = Executor::production();
            executor.block_on(async move {
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
        });

        let call = |base: u64, admin: Injector<AdminRequest>| {
            thread::spawn(move || {
                let (request, reply) = bridge_request(AdminCall::DeleteBase { base });
                admin.push(Lane::Background, request).expect("actor alive");
                reply
                    .blocking_recv_timeout(Duration::from_secs(1))
                    .expect("reply without shared-stream timeout")
            })
        };
        let first = call(1, admin.clone());
        let second = call(2, admin);

        for (expected, caller) in [(1, first), (2, second)] {
            assert_eq!(
                caller.join().expect("caller thread"),
                Ok(AdminSuccess::BaseDeleted { base: expected })
            );
        }
        actor.join().expect("actor thread");
    }

    #[test]
    fn successful_migration_completes_pause_once() {
        let shared = Shared::new(BTreeMap::new(), &test_host_config());
        let vset = VsetId(7);
        shared
            .pause_expected
            .lock()
            .expect("pause lock")
            .entry(vset)
            .or_default()
            .push_back(1);

        begin_pause(&shared, vset);
        assert!(
            shared
                .pause_in_flight
                .lock()
                .expect("pause lock")
                .contains_key(&vset)
        );

        complete_pause(&shared, vset);
        complete_pause(&shared, vset);

        assert!(
            !shared
                .pause_in_flight
                .lock()
                .expect("pause lock")
                .contains_key(&vset)
        );
        assert_eq!(shared.pause_latency[1].snapshot().count, 1);
    }

    #[test]
    fn cancelled_pending_pause_releases_guest_control() {
        let vset = VsetId(8);
        let host = VsetHost::new(VsetConfig::database(1));
        let shared = Arc::new(Shared::new(
            BTreeMap::from([(vset, Arc::clone(&host))]),
            &test_host_config(),
        ));
        shared
            .pause_expected
            .lock()
            .expect("pause lock")
            .entry(vset)
            .or_default()
            .push_back(0);
        begin_pause(&shared, vset);
        {
            let mut state = host.ctl.state.lock().expect("guest control lock");
            state.pause_generation = 1;
            state.pause_requested = true;
            state.paused = true;
        }

        drop(PendingGuestPause {
            shared: Arc::clone(&shared),
            host: Arc::clone(&host),
            vset,
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
                .contains_key(&vset)
        );
    }
}
