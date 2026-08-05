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
use blockd_hostmem::{GuestView, HostRegion, PAGE_SIZE, Uffd, UffdFeatures};

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
        })
    }

    fn page_index(&self, page: PageId) -> usize {
        usize::from(page.volume.idx.0)
            * usize::try_from(self.config.pages_per_volume).expect("fits")
            + usize::try_from(page.page.0).expect("fits")
    }

    fn page_of_addr(&self, vset: VsetId, addr: usize) -> PageId {
        let index = (addr - self.view.addr_of(0)) / PAGE_SIZE;
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

pub(crate) enum Msg {
    Ev(Event),
    /// A guest reached an op boundary with a pause pending.
    Quiesced(VsetId),
    Stop,
}

struct Shared {
    vsets: Mutex<BTreeMap<VsetId, Arc<VsetHost>>>,
    sync_waiters: Mutex<BTreeMap<ReqId, Sender<bool>>>,
    /// `VsetFenced` / `FillFailed` and friends: anything the tests must know
    /// went wrong (asserted empty in healthy scenarios).
    incidents: Mutex<Vec<String>>,
    /// The daemon's counters, copied out after every step (R9.2).
    counters: Mutex<blockd_core::daemon::Counters>,
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
            .writeprotect(host.view.addr_of(index), PAGE_SIZE, true)
            .expect("capture write-protect");
        host.region.read_page(index)
    }
}

pub struct Runtime {
    tx: Sender<Msg>,
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
        let mut blobs: Vec<(String, Vec<u8>)> = Vec::new();
        scan_blobs(&config.blob_dir, &config.blob_dir, &mut blobs);
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

    fn start(
        mut daemon: Daemon,
        boot_effects: Vec<Effect>,
        hosts: BTreeMap<VsetId, Arc<VsetHost>>,
        config: &RuntimeConfig,
        store: &Arc<dyn ObjectStore>,
    ) -> Runtime {
        std::fs::create_dir_all(&config.blob_dir).expect("blob dir");
        let (tx, rx) = channel::<Msg>();
        let (admin_tx, admin_rx) = channel::<AdminReply>();
        let shared = Arc::new(Shared {
            vsets: Mutex::new(hosts),
            sync_waiters: Mutex::new(BTreeMap::new()),
            incidents: Mutex::new(Vec::new()),
            counters: Mutex::new(blockd_core::daemon::Counters::default()),
            next_req: AtomicU64::new(1),
        });

        let peers = config.peer.as_ref().map(|p| {
            let tx = tx.clone();
            PeerNet::start(p, config.daemon.host, move |from, msg| {
                let _ = tx.send(Msg::Ev(Event::PeerDelivered { from, msg }));
            })
        });

        // Timer thread: real clock, feeding Timer events back in.
        let (timer_tx, timer_rx) = channel::<(TimerId, u64)>();
        {
            let tx = tx.clone();
            thread::spawn(move || timer_loop(&timer_rx, &tx));
        }

        // Store workers: object-store round-trips run here, never on the
        // event loop — a store put at real latency on the loop thread
        // would stall fault resolution for every vset (the daemon is
        // sans-IO precisely so completions can arrive as events).
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

        // The event loop: the daemon lives here.
        let loop_thread = {
            let shared = shared.clone();
            let blob_dir = config.blob_dir.clone();
            let tx = tx.clone();
            let peers = peers.clone();
            let self_id = config.daemon.host;
            thread::spawn(move || {
                let mut local: VecDeque<Event> = VecDeque::new();
                let apply = |effects: Vec<Effect>, local: &mut VecDeque<Event>| {
                    for effect in effects {
                        apply_effect(
                            effect,
                            &shared,
                            &store_tx,
                            &blob_dir,
                            &timer_tx,
                            &admin_tx,
                            &tx,
                            local,
                            peers.as_ref(),
                            self_id,
                        );
                    }
                };
                apply(boot_effects, &mut local);
                loop {
                    // Follow-up I/O completions first (they belong to the
                    // step that issued them), then the outside world.
                    let event = if let Some(event) = local.pop_front() {
                        event
                    } else {
                        match rx.recv() {
                            Ok(Msg::Ev(event)) => event,
                            Ok(Msg::Quiesced(vset)) => {
                                let vmstate = {
                                    let host = shared.vsets.lock().expect("lock")[&vset].clone();
                                    let mut state = host.ctl.state.lock().expect("lock");
                                    state.paused = true;
                                    state.applied
                                };
                                Event::GuestPaused { vset, vmstate }
                            }
                            Ok(Msg::Stop) | Err(_) => break,
                        }
                    };
                    let effects = {
                        let vsets = shared.vsets.lock().expect("lock");
                        daemon.step(event, &MapView { vsets: &vsets })
                    };
                    *shared.counters.lock().expect("lock") = daemon.counters;
                    apply(effects, &mut local);
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

    /// Peer frames dropped on the floor so far (queue full or peer down)
    /// — the retry timers' workload, visible.
    pub fn peer_dropped_sends(&self) -> u64 {
        self.peers.as_ref().map_or(0, |p| {
            p.dropped_sends.load(std::sync::atomic::Ordering::SeqCst)
        })
    }

    fn spawn_fault_reader(&self, vset: VsetId, host: Arc<VsetHost>) {
        let tx = self.tx.clone();
        thread::spawn(move || {
            while let Ok(event) = host.uffd.read_event() {
                let page = host.page_of_addr(vset, event.address & !(PAGE_SIZE - 1));
                if tx
                    .send(Msg::Ev(Event::GuestFault {
                        page,
                        write: event.write,
                    }))
                    .is_err()
                {
                    break;
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

    pub fn create_vset(&self, vset: VsetId, config: VsetConfig) {
        let host = VsetHost::new(config);
        self.shared
            .vsets
            .lock()
            .expect("lock")
            .insert(vset, host.clone());
        self.spawn_fault_reader(vset, host);
        let req = self.req();
        self.tx
            .send(Msg::Ev(Event::Admin(AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base: None,
            })))
            .expect("send");
        self.wait_admin(|reply| match reply {
            AdminReply::VsetCreated { req: r, vset: v } if *r == req && *v == vset => Some(()),
            _ => None,
        });
    }

    pub fn checkpoint(&self, vset: VsetId) -> u64 {
        let req = self.req();
        self.tx
            .send(Msg::Ev(Event::Admin(AdminCmd::Checkpoint { req, vset })))
            .expect("send");
        self.wait_admin(|reply| match reply {
            AdminReply::CheckpointDone { req: r, epoch, .. } if *r == req => Some(epoch.0),
            AdminReply::AdminFailed { req: r } if *r == req => {
                panic!("checkpoint failed")
            }
            _ => None,
        })
    }

    /// Restore a backed-up vset from the store onto this host (R6.1).
    pub fn restore_vset(&self, vset: VsetId, config: VsetConfig) -> Verdict {
        let host = VsetHost::new(config);
        self.shared
            .vsets
            .lock()
            .expect("lock")
            .insert(vset, host.clone());
        self.spawn_fault_reader(vset, host);
        let req = self.req();
        self.tx
            .send(Msg::Ev(Event::Admin(AdminCmd::RestoreVset { req, vset })))
            .expect("send");
        self.wait_admin(|reply| match reply {
            AdminReply::VsetRestored {
                req: r, verdict, ..
            } if *r == req => Some(*verdict),
            AdminReply::AdminFailed { req: r } if *r == req => panic!("restore failed"),
            _ => None,
        })
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
    pub fn migrate_out(&self, vset: VsetId, to: blockd_core::types::HostId) {
        let req = self.req();
        self.tx
            .send(Msg::Ev(Event::Admin(AdminCmd::MigrateOut {
                req,
                vset,
                to,
            })))
            .expect("send");
        self.wait_admin(|reply| match reply {
            AdminReply::MigratedOut { req: r, .. } if *r == req => Some(()),
            AdminReply::AdminFailed { req: r } if *r == req => panic!("migrate out failed"),
            _ => None,
        });
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
            self.tx.send(Msg::Quiesced(vset)).expect("send");
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
    pub fn guest_sync(&self, vset: VsetId, volume: VolumeIdx) -> bool {
        let host = self.host(vset);
        Runtime::op_start(&host);
        let req = self.req();
        let (done_tx, done_rx) = channel();
        self.shared
            .sync_waiters
            .lock()
            .expect("lock")
            .insert(req, done_tx);
        self.tx
            .send(Msg::Ev(Event::GuestSync {
                req,
                volume: VolumeId { vset, idx: volume },
            }))
            .expect("send");
        let ok = done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("sync ack within 30s");
        self.op_end(vset, &host);
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
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Stop);
        if let Some(handle) = self.loop_thread.take() {
            let _ = handle.join();
        }
    }
}

// ── effect interpretation ───────────────────────────────────────────────

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn apply_effect(
    effect: Effect,
    shared: &Arc<Shared>,
    store_tx: &Sender<StoreJob>,
    blob_dir: &Path,
    timer_tx: &Sender<(TimerId, u64)>,
    admin_tx: &Sender<AdminReply>,
    tx: &Sender<Msg>,
    local: &mut VecDeque<Event>,
    peers: Option<&Arc<PeerNet>>,
    self_id: blockd_core::types::HostId,
) {
    match effect {
        Effect::Fill {
            page,
            bytes,
            writable,
            share,
        } => {
            assert!(share.is_none(), "base sharing is not wired in e2e v1");
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            let index = host.page_index(page);
            host.region.write_page(index, &bytes);
            // Non-writable fills install write-protected: the next guest
            // store traps, keeping dirty tracking exact (R2.4).
            host.uffd
                .continue_range(host.view.addr_of(index), PAGE_SIZE, !writable)
                .expect("continue");
        }
        Effect::FillShared { .. } => unreachable!("base sharing is not wired in e2e v1"),
        Effect::FillFailed { page } => {
            // Unservable page: in production this SIGBUSes the guest. The
            // e2e scenarios never sanction it — die loudly right here.
            eprintln!("FATAL: unservable page {page:?}");
            std::process::abort();
        }
        Effect::Unprotect { page } => {
            let host = shared.vsets.lock().expect("lock")[&page.volume.vset].clone();
            let index = host.page_index(page);
            host.uffd
                .writeprotect(host.view.addr_of(index), PAGE_SIZE, false)
                .expect("unprotect");
        }
        Effect::WriteProtect { pages } => {
            let vsets = shared.vsets.lock().expect("lock");
            for page in pages {
                let host = &vsets[&page.volume.vset];
                let index = host.page_index(page);
                host.uffd
                    .writeprotect(host.view.addr_of(index), PAGE_SIZE, true)
                    .expect("write-protect");
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
                tx.send(Msg::Quiesced(vset)).expect("send");
            }
        }
        Effect::ResumeGuest { vset } => {
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
            let path = blob_dir.join(&name);
            std::fs::create_dir_all(path.parent().expect("has parent")).expect("mkdir");
            std::fs::write(&path, &bytes).expect("blob write");
            std::fs::File::open(&path)
                .expect("open")
                .sync_all()
                .expect("fsync");
            local.push_back(Event::BlobWriteDone { io });
        }
        Effect::BlobRead { io, name } => {
            let bytes = std::fs::File::open(blob_dir.join(&name))
                .ok()
                .and_then(|mut file| {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).ok().map(|_| buf)
                });
            local.push_back(Event::BlobReadDone { io, bytes });
        }
        Effect::BlobReadRange {
            io,
            name,
            offset,
            len,
        } => {
            let bytes = std::fs::File::open(blob_dir.join(&name))
                .ok()
                .and_then(|file| {
                    let mut buf = vec![0u8; usize::try_from(len).expect("fits")];
                    file.read_exact_at(&mut buf, offset).ok().map(|()| buf)
                });
            local.push_back(Event::BlobReadDone { io, bytes });
        }
        Effect::BlobDelete { name } => {
            let _ = std::fs::remove_file(blob_dir.join(&name));
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
            shared
                .incidents
                .lock()
                .expect("lock")
                .push(format!("fenced: {vset:?}"));
        }
        Effect::Admin(reply) => {
            let _ = admin_tx.send(reply);
        }
        Effect::PeerSend { to, msg } => match peers {
            Some(net) => net.send(self_id, to, &msg),
            None => shared
                .incidents
                .lock()
                .expect("lock")
                .push(format!("peer send to {to:?} with no peer config")),
        },
        Effect::Abort { reason } => {
            eprintln!("FATAL: daemon abort: {reason}");
            std::process::abort();
        }
    }
}

/// Concurrent object-store round-trips. The daemon's own pipelines bound
/// what is outstanding (one publish per vset, deduped cold fetches); this
/// only caps the parallelism of what it issues.
const STORE_WORKERS: usize = 8;

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
    tx: &Sender<Msg>,
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
        if tx.send(Msg::Ev(event)).is_err() {
            return;
        }
    }
}

fn timer_loop(rx: &Receiver<(TimerId, u64)>, tx: &Sender<Msg>) {
    let mut armed: Vec<(Instant, TimerId)> = Vec::new();
    loop {
        let now = Instant::now();
        // Fire everything due.
        let mut i = 0;
        while i < armed.len() {
            if armed[i].0 <= now {
                let (_, timer) = armed.remove(i);
                if tx.send(Msg::Ev(Event::Timer(timer))).is_err() {
                    return;
                }
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

fn scan_blobs(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_blobs(root, &path, out);
        } else {
            let name = path
                .strip_prefix(root)
                .expect("under root")
                .to_str()
                .expect("utf8")
                .to_owned();
            let bytes = std::fs::read(&path).expect("blob read");
            out.push((name, bytes));
        }
    }
}
