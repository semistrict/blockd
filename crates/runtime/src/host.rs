//! The runtime host: one thread runs the real `Daemon` state machine; its
//! effects are interpreted against real guest memory (userfaultfd), real
//! disk blobs, real timers, and the S3-shaped store. Guest workloads run
//! on caller threads and touch mapped memory directly — their faults reach
//! the daemon exactly as production faults do.

use std::collections::{BTreeMap, VecDeque};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use blockd_core::daemon::{Daemon, DaemonConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::seam::{
    AdminCmd, AdminReply, Effect, Event, HostMap, IoId, ReqId, TimerId, Verdict,
};
use blockd_core::types::{PageId, PageNo, VolumeId, VolumeIdx, VsetId};
use blockd_hostmem::{GuestView, HostRegion, Uffd, UffdFeatures, page_size};

use crate::loopstats::{LoopStats, effect_kind, event_kind};
use crate::metrics::{AtomicHistogram, HistogramSnapshot};
use crate::peer::{PeerConfig, PeerNet};
use crate::store::ObjectStore;

pub struct RuntimeConfig {
    pub daemon: DaemonConfig,
    /// The local blob device: a real directory on real disk.
    pub blob_dir: PathBuf,
    /// Peer transport (`None` = a host that never migrates).
    pub peer: Option<PeerConfig>,
}

/// One vset's guest memory on this host: the region (daemon view), the
/// guest's mapping, its uffd, and the vCPU control block.
struct VsetHost {
    config: VsetConfig,
    region: Arc<HostRegion>,
    view: Arc<GuestView>,
    uffd: Arc<Uffd>,
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
        Arc::new(VsetHost {
            config,
            region,
            view,
            uffd: Arc::new(uffd),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultLatency {
    pub vset: VsetId,
    pub source: &'static str,
    pub histogram: HistogramSnapshot,
}

const OPERATION_NAMES: [&str; 5] = ["create", "checkpoint", "restore", "migration", "sync"];
const OPERATION_OUTCOMES: [&str; 2] = ["success", "failed"];
const LOCAL_IO_NAMES: [&str; 4] = ["write", "read", "ranged_read", "delete"];
const LOCAL_IO_OUTCOMES: [&str; 2] = ["success", "missing"];
const PAUSE_NAMES: [&str; 2] = ["checkpoint", "migration"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeOperationLatency {
    pub operation: &'static str,
    pub outcome: &'static str,
    pub histogram: HistogramSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalIoLatency {
    pub operation: &'static str,
    pub outcome: &'static str,
    pub histogram: HistogramSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuestPauseLatency {
    pub operation: &'static str,
    pub histogram: HistogramSnapshot,
}

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
                blockd_core::seam::PeerMsg::Page { .. } | blockd_core::seam::PeerMsg::Leaf { .. }
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
    /// `VsetFenced` / `FillFailed` and friends: anything the tests must know
    /// went wrong (asserted empty in healthy scenarios).
    incidents: Mutex<Vec<String>>,
    /// The daemon's counters, copied out after every step (R9.2).
    counters: Mutex<blockd_core::daemon::Counters>,
    daemon_stats: Mutex<blockd_core::daemon::DaemonStats>,
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
    backup_lag_started: Mutex<BTreeMap<VsetId, Instant>>,
    operation_started: Mutex<BTreeMap<(VsetId, u8), Instant>>,
    next_req: AtomicU64,
}

/// The daemon's synchronous window onto the mappings. Capture contract
/// (see `HostMap`): arm write protection BEFORE reading, so a concurrent
/// guest write either lands in the returned bytes or traps after them —
/// never silently between.
struct MapView<'a> {
    vsets: &'a BTreeMap<VsetId, Arc<VsetHost>>,
}

impl HostMap for MapView<'_> {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        let host = &self.vsets[&page.volume.vset];
        let index = host.page_index(page);
        host.uffd
            .writeprotect(host.view.addr_of(index), page_size(), true)
            .expect("capture write-protect");
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
    loop_thread: Option<thread::JoinHandle<()>>,
}

impl Runtime {
    /// A fresh daemon on an empty (or to-be-ignored) blob directory.
    // Ownership transfer: the store workers clone from it.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(config: &RuntimeConfig, store: Arc<dyn ObjectStore>) -> Runtime {
        let (daemon, effects) = Daemon::new(config.daemon.clone());
        Runtime::start(daemon, effects, BTreeMap::new(), config, &store)
    }

    /// Recover a daemon from the blobs actually on disk (R8.2). The caller
    /// supplies the vset configs so guest memory can be rebuilt (in
    /// production the control plane knows them; the records carry them
    /// too). Returns the per-vset verdicts for non-backed vsets; backed
    /// vsets report `VsetRecovered` through admin replies after their head
    /// refresh.
    #[allow(clippy::needless_pass_by_value)]
    pub fn recover(
        config: &RuntimeConfig,
        store: Arc<dyn ObjectStore>,
        vset_configs: &BTreeMap<VsetId, VsetConfig>,
    ) -> (Runtime, BTreeMap<VsetId, Verdict>) {
        let blobs = crate::blobscan::scan_blob_dir(&config.blob_dir);
        let (daemon, verdicts, effects) = Daemon::recover(
            config.daemon.clone(),
            blobs
                .iter()
                .map(|(name, bytes)| (name.as_str(), bytes.as_slice())),
        );
        let mut hosts = BTreeMap::new();
        for (&vset, &vc) in vset_configs {
            hosts.insert(vset, VsetHost::new(vc));
        }
        let runtime = Runtime::start(daemon, effects, hosts, config, &store);
        for (&vset, host) in runtime.shared.vsets.lock().expect("lock").iter() {
            runtime.spawn_fault_reader(vset, host.clone());
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
        let shared = Arc::new(Shared {
            vsets: Mutex::new(hosts),
            sync_waiters: Mutex::new(BTreeMap::new()),
            incidents: Mutex::new(Vec::new()),
            counters: Mutex::new(blockd_core::daemon::Counters::default()),
            daemon_stats: Mutex::new(daemon_stats),
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
        });

        let peers = config.peer.as_ref().map(|p| {
            let tx = tx.clone();
            PeerNet::start(p, config.daemon.host, move |from, msg| {
                tx.push(Msg::Ev(Event::PeerDelivered { from, msg }));
            })
        });

        let IoLanes {
            store: store_tx,
            blob: blob_tx,
            blob_delete: blob_delete_tx,
            timer: timer_tx,
        } = spawn_io_workers(&config.blob_dir, store, &tx, &shared);

        // The event loop: the daemon lives here.
        let loop_thread = {
            let shared = shared.clone();
            let tx = tx.clone();
            let peers = peers.clone();
            let self_id = config.daemon.host;
            thread::spawn(move || {
                let apply = |effects: Vec<Effect>, source: Option<FaultSource>| {
                    for effect in effects {
                        let kind = effect_kind(&effect);
                        let started = Instant::now();
                        apply_effect(
                            effect,
                            &shared,
                            &store_tx,
                            &blob_tx,
                            &blob_delete_tx,
                            &timer_tx,
                            &admin_tx,
                            &tx,
                            peers.as_ref(),
                            self_id,
                            source,
                        );
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
                    let kind = event_kind(&event);
                    let source = fault_source_of_event(&event);
                    let started = Instant::now();
                    let effects = {
                        let vsets = shared.vsets.lock().expect("lock");
                        daemon.step(event, &MapView { vsets: &vsets })
                    };
                    *shared.counters.lock().expect("lock") = daemon.counters;
                    let daemon_stats = daemon.stats();
                    update_backup_lag(&shared, &daemon_stats);
                    update_active_operations(&shared, &daemon_stats);
                    *shared.daemon_stats.lock().expect("lock") = daemon_stats;
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

    pub fn backup_lag_age(&self) -> Vec<(VsetId, Duration)> {
        self.shared
            .backup_lag_started
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

    pub fn operation_latency(&self) -> Vec<RuntimeOperationLatency> {
        let mut snapshots = Vec::new();
        for (operation, operation_name) in OPERATION_NAMES.iter().enumerate() {
            for (outcome, outcome_name) in OPERATION_OUTCOMES.iter().enumerate() {
                snapshots.push(RuntimeOperationLatency {
                    operation: operation_name,
                    outcome: outcome_name,
                    histogram: self.shared.operation_latency[operation][outcome].snapshot(),
                });
            }
        }
        snapshots
    }

    pub fn local_io_latency(&self) -> Vec<LocalIoLatency> {
        let mut snapshots = Vec::new();
        for (operation, operation_name) in LOCAL_IO_NAMES.iter().enumerate() {
            for (outcome, outcome_name) in LOCAL_IO_OUTCOMES.iter().enumerate() {
                snapshots.push(LocalIoLatency {
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

    pub fn guest_pause_latency(&self) -> Vec<GuestPauseLatency> {
        PAUSE_NAMES
            .iter()
            .zip(&self.shared.pause_latency)
            .map(|(operation, histogram)| GuestPauseLatency {
                operation,
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
        // Exits when the vset's uffd closes (the vset was dropped).
        thread::spawn(move || {
            while let Ok(events) = host.uffd.read_events() {
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
        fields(vset_id = vset.0, backed = config.backed_up)
    )]
    pub fn create_vset(&self, vset: VsetId, config: VsetConfig) {
        let started = Instant::now();
        let host = VsetHost::new(config);
        self.shared
            .vsets
            .lock()
            .expect("lock")
            .insert(vset, host.clone());
        self.spawn_fault_reader(vset, host);
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

    /// Restore a backed-up vset from the store onto this host (R6.1).
    #[tracing::instrument(
        skip(self, config),
        fields(vset_id = vset.0, backed = config.backed_up)
    )]
    pub fn restore_vset(&self, vset: VsetId, config: VsetConfig) -> Verdict {
        let started = Instant::now();
        let host = VsetHost::new(config);
        self.shared
            .vsets
            .lock()
            .expect("lock")
            .insert(vset, host.clone());
        self.spawn_fault_reader(vset, host);
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
        let host = VsetHost::new(config);
        self.shared
            .vsets
            .lock()
            .expect("lock")
            .insert(vset, host.clone());
        self.spawn_fault_reader(vset, host);
    }

    /// Hand a non-backed vset off to `to` (R7.2): pauses, captures, makes
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
    }
}

// ── effect interpretation ───────────────────────────────────────────────

fn fault_source_of_event(event: &Event) -> Option<FaultSource> {
    match event {
        Event::BlobReadDone { .. } => Some(FaultSource::Local),
        Event::StoreGetDone { .. } => Some(FaultSource::Store),
        Event::PeerDelivered {
            msg: blockd_core::seam::PeerMsg::Page { .. },
            ..
        } => Some(FaultSource::Peer),
        _ => None,
    }
}

fn complete_fault(shared: &Shared, page: PageId, source: FaultSource, outcome: &'static str) {
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
    if let Some(host) = shared
        .vsets
        .lock()
        .expect("lock")
        .get(&page.volume.vset)
        .cloned()
    {
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

fn update_backup_lag(shared: &Shared, stats: &blockd_core::daemon::DaemonStats) {
    let now = Instant::now();
    let lagging: std::collections::BTreeSet<VsetId> = stats
        .vsets
        .iter()
        .filter(|vset| vset.backup_lag_captures.is_some_and(|lag| lag > 0))
        .map(|vset| vset.vset)
        .collect();
    let mut started = shared.backup_lag_started.lock().expect("lock");
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn apply_effect(
    effect: Effect,
    shared: &Arc<Shared>,
    store_tx: &Sender<StoreJob>,
    blob_tx: &Sender<BlobJob>,
    blob_delete_tx: &Sender<BlobJob>,
    timer_tx: &Sender<(TimerId, u64)>,
    admin_tx: &Sender<AdminReply>,
    tx: &Arc<LoopQueue>,
    peers: Option<&Arc<PeerNet>>,
    self_id: blockd_core::types::HostId,
    source: Option<FaultSource>,
) {
    match effect {
        Effect::Fill {
            page,
            bytes,
            writable,
            share,
        } => {
            complete_fault(shared, page, source.unwrap_or(FaultSource::Zero), "served");
            assert!(share.is_none(), "base sharing is not wired in e2e v1");
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            let index = host.page_index(page);
            host.region.write_page(index, &bytes);
            // Non-writable fills install write-protected: the next guest
            // store traps, keeping dirty tracking exact (R2.4).
            host.uffd
                .continue_range(host.view.addr_of(index), page_size(), !writable)
                .expect("continue");
        }
        Effect::FillShared { .. } => unreachable!("base sharing is not wired in e2e v1"),
        Effect::FillFailed { page } => {
            complete_fault(shared, page, FaultSource::Unservable, "failed");
            // Unservable page: in production this SIGBUSes the guest. The
            // e2e scenarios never sanction it — die loudly right here.
            tracing::error!(?page, "fatal unservable page");
            std::process::abort();
        }
        Effect::Unprotect { page } => {
            complete_fault(shared, page, FaultSource::WriteProtect, "served");
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            let index = host.page_index(page);
            host.uffd
                .writeprotect(host.view.addr_of(index), page_size(), false)
                .expect("unprotect");
        }
        Effect::WriteProtect { pages } => {
            let vsets = shared.vsets.lock().expect("lock");
            let mut by_vset: BTreeMap<VsetId, Vec<usize>> = BTreeMap::new();
            for page in pages {
                let host = &vsets[&page.volume.vset];
                by_vset
                    .entry(page.volume.vset)
                    .or_default()
                    .push(host.page_index(page));
            }
            for (vset, mut indices) in by_vset {
                let host = &vsets[&vset];
                for_each_contiguous_run(&mut indices, |start, len| {
                    host.uffd
                        .writeprotect(host.view.addr_of(start), len * page_size(), true)
                        .expect("write-protect");
                });
            }
        }
        Effect::Evict { page } => {
            // Real reclaim: the daemon only evicts clean pages (their
            // segment is durable), so dropping the PTE AND punching the
            // backing genuinely frees RAM; the refault refills from the
            // local segment on disk (R2.4).
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            let index = host.page_index(page);
            host.view.evict(index, 1).expect("evict");
            host.region.punch_hole(index, 1).expect("punch");
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
            let state = host.ctl.state.lock().expect("lock");
            if state.in_op {
                let mut state = state;
                state.pause_requested = true;
                // The op boundary sends Quiesced; GuestPaused follows.
            } else {
                let mut state = state;
                state.pause_requested = true;
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
            let _ = store_tx.send(StoreJob::Put { io, key, bytes });
        }
        Effect::StoreCas {
            io,
            key,
            expected,
            bytes,
        } => {
            let _ = store_tx.send(StoreJob::Cas {
                io,
                key,
                expected,
                bytes,
            });
        }
        Effect::StoreGet { io, key } => {
            let _ = store_tx.send(StoreJob::Get { io, key });
        }
        Effect::StoreGetRange {
            io,
            key,
            offset,
            len,
        } => {
            let _ = store_tx.send(StoreJob::GetRange {
                io,
                key,
                offset,
                len,
            });
        }
        Effect::StoreDelete { key } => {
            let _ = store_tx.send(StoreJob::Delete { key });
        }
        Effect::VsetFenced { vset } => {
            tracing::warn!(vset_id = vset.0, "vset fenced by a newer assignment");
            shared
                .incidents
                .lock()
                .expect("lock")
                .push(format!("fenced: {vset:?}"));
        }
        Effect::Admin(reply) => {
            if let AdminReply::MigratedOut { vset, .. } = reply {
                complete_pause(shared, vset);
            }
            let _ = admin_tx.send(reply);
        }
        Effect::PeerSend { to, msg } => {
            if let Some(net) = peers {
                net.send(self_id, to, &msg);
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
const STORE_WORKERS: usize = 8;

/// Local disks can service independent segment reads concurrently. This is
/// also the upper bound on simultaneous fsyncs issued by the runtime.
const BLOB_WORKERS: usize = 8;

enum BlobJob {
    Write {
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
}

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
                let mut dir = parent;
                loop {
                    std::fs::File::open(dir)
                        .expect("open dir")
                        .sync_all()
                        .expect("fsync dir");
                    if dir == root {
                        break;
                    }
                    dir = dir.parent().expect("under root");
                }
                shared.local_io_in_flight[0].fetch_sub(1, Ordering::Relaxed);
                shared.local_io_latency[0][0].observe(started.elapsed());
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

fn store_worker_loop(
    rx: &Arc<Mutex<Receiver<StoreJob>>>,
    store: &dyn ObjectStore,
    tx: &Arc<LoopQueue>,
) {
    loop {
        let Ok(job) = rx.lock().expect("lock").recv() else {
            return;
        };
        let event = match job {
            StoreJob::Put { io, key, bytes } => Event::StorePutDone {
                io,
                result: store.put(&key, bytes),
            },
            StoreJob::Cas {
                io,
                key,
                expected,
                bytes,
            } => Event::StorePutDone {
                io,
                result: store.put_cas(&key, expected, bytes),
            },
            StoreJob::Get { io, key } => Event::StoreGetDone {
                io,
                result: store.get(&key),
            },
            StoreJob::GetRange {
                io,
                key,
                offset,
                len,
            } => Event::StoreGetDone {
                io,
                result: store.get_range(&key, offset, len),
            },
            StoreJob::Delete { key } => {
                store.delete(&key);
                continue;
            }
        };
        tx.push(Msg::Ev(event));
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

/// Spawn everything that does blocking I/O on the daemon's behalf; only
/// senders come back — completions return to the loop as events.
///
/// - Timer thread: real clock, feeding `Timer` events back in.
/// - Store workers: object-store round-trips run here, never on the
///   event loop — a store put at real latency on the loop thread would
///   stall fault resolution for every vset (the daemon is sans-IO
///   precisely so completions can arrive as events).
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

    let (store_tx, store_rx) = channel::<StoreJob>();
    {
        let store_rx = Arc::new(Mutex::new(store_rx));
        for _ in 0..STORE_WORKERS {
            let store = store.clone();
            let tx = tx.clone();
            let store_rx = store_rx.clone();
            thread::spawn(move || store_worker_loop(&store_rx, store.as_ref(), &tx));
        }
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

    IoLanes {
        store: store_tx,
        blob: blob_tx,
        blob_delete: blob_delete_tx,
        timer: timer_tx,
    }
}

/// The job senders `spawn_io_workers` hands back, one per lane.
struct IoLanes {
    store: Sender<StoreJob>,
    blob: Sender<BlobJob>,
    blob_delete: Sender<BlobJob>,
    timer: Sender<(TimerId, u64)>,
}

fn elapsed_ns(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).expect("fits")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_shared() -> Arc<Shared> {
        Arc::new(Shared {
            vsets: Mutex::new(BTreeMap::new()),
            sync_waiters: Mutex::new(BTreeMap::new()),
            incidents: Mutex::new(Vec::new()),
            counters: Mutex::new(blockd_core::daemon::Counters::default()),
            daemon_stats: Mutex::new(blockd_core::daemon::DaemonStats::default()),
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
        })
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
    fn queued_blob_io_does_not_block_followup_effects() {
        let shared = test_shared();
        let (store_tx, _store_rx) = channel();
        // No worker receives from this channel: it models every disk worker
        // being occupied by a slow filesystem operation.
        let (blob_tx, blob_rx) = channel();
        let (blob_delete_tx, _blob_delete_rx) = channel();
        let (timer_tx, timer_rx) = channel();
        let (admin_tx, _admin_rx) = channel();
        let tx = LoopQueue::new();

        apply_effect(
            Effect::BlobWrite {
                io: IoId(7),
                name: "held/blob".to_owned(),
                bytes: b"payload".to_vec(),
            },
            &shared,
            &store_tx,
            &blob_tx,
            &blob_delete_tx,
            &timer_tx,
            &admin_tx,
            &tx,
            None,
            blockd_core::types::HostId(0),
            None,
        );
        apply_effect(
            Effect::SetTimer {
                timer: TimerId::Backup(VsetId(9)),
                after: 123,
            },
            &shared,
            &store_tx,
            &blob_tx,
            &blob_delete_tx,
            &timer_tx,
            &admin_tx,
            &tx,
            None,
            blockd_core::types::HostId(0),
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
    }

    #[test]
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

        drop(job_tx);
        worker.join().expect("worker");
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
