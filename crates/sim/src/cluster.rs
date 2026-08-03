//! The multi-host harness: N real daemons, one shared object store, guests
//! that follow their vset's placement. Host death is permanent here — the
//! recovery story is the head-CAS restore onto a peer (R6.1/R6.3), with the
//! control plane deliberately racing two claimants to keep the
//! exactly-one-runner property under fire. Loss on host death is checked
//! against the head's manifest pointer at the instant of death (R4.3: the
//! backup lag, nothing more).

use std::collections::{BTreeMap, BTreeSet};

use blockd_core::daemon::{Daemon, DaemonConfig};
use blockd_core::head::{HeadRecord, ManifestPtr};
use blockd_core::journal::VsetConfig;
use blockd_core::layout;
use blockd_core::seam::{AdminCmd, AdminReply, Effect, Event, HostMap, IoId, ReqId, Verdict};
use blockd_core::types::{HostId, PageId, SimTime, VsetId, micros, millis};

use crate::guest::{Guest, GuestState, PendingOp, page_pattern};
use crate::kernel::Kernel;
use crate::oracle::Oracle;
use crate::world::blobdev::{BdevIo, BlobDev, BlobDevConfig};
use crate::world::store::{ObjectStore, StoreConfig, Version};

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub hosts: u16,
    pub daemon: DaemonConfig,
    pub bdev: BlobDevConfig,
    pub store: StoreConfig,
    /// All vsets are backed up (multi-host recovery is their story; R4.4 is
    /// covered by the single-host suite).
    pub vset_count: u16,
    pub vset_config: VsetConfig,
    pub horizon: u64,
    pub think: (u64, u64),
    pub checkpoint_interval: Option<u64>,
    /// Permanently kill hosts at instants.
    pub kill_hosts_at: Vec<(u64, u16)>,
    /// Send the restore of each orphaned vset to TWO hosts (CAS race).
    pub race_restore: bool,
    /// Migrate a vset to a destination host at an instant (R7).
    pub migrate_at: Option<(u64, VsetId, u16)>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClusterReport {
    pub trace_hash: u64,
    pub violations: Vec<String>,
    pub completed_ops: u64,
    pub restores: u64,
    /// Restore claims that lost the CAS race (exactly-one-runner, R6.3).
    pub claims_lost: u64,
    pub guest_deaths: u64,
    /// Restored vsets whose recovered point equalled the head's manifest at
    /// the kill instant (the R4.3 loss bound, verified).
    pub loss_bound_verified: u64,
    /// Completed migrations (both sides acknowledged, R7).
    pub migrations: u64,
    /// Slowest restore, `RestoreVset` to `VsetRestored` (the R6.2 budget).
    pub max_restore_ns: u64,
    /// Migration's guest-observed pause: source `PauseGuest` to the
    /// destination's `VsetMigratedIn` (the R7.1 budget).
    pub max_migration_pause_ns: u64,
    /// Resume-set pages prefetched across all live daemons (R6.2).
    pub prefetch_fills: u64,
}

#[derive(Debug, Default)]
struct VsetMem {
    pages: BTreeMap<PageId, Vec<u8>>,
    protected: BTreeSet<PageId>,
    /// Pages the guest touched since the last accessed-bit harvest — the
    /// simulation's ground truth behind MGLRU aging (R2.6).
    accessed: std::cell::RefCell<BTreeSet<PageId>>,
}

struct MemView<'a>(&'a BTreeMap<VsetId, VsetMem>);

impl HostMap for MemView<'_> {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        self.0[&page.volume.vset].pages[&page].clone()
    }

    fn harvest_accessed(&self, resident: &[PageId]) -> Vec<PageId> {
        resident
            .iter()
            .filter(|page| {
                self.0
                    .get(&page.volume.vset)
                    .is_some_and(|mem| mem.accessed.borrow_mut().remove(page))
            })
            .copied()
            .collect()
    }
}

struct HostState {
    daemon: Option<Daemon>,
    inc: u32,
    bdev: BlobDev,
    mems: BTreeMap<VsetId, VsetMem>,
    /// The host's shared base-page tier bytes (R5.3).
    shared_base: BTreeMap<(u64, u64, blockd_core::types::SegId, u32), Vec<u8>>,
}

#[derive(Debug)]
enum Ev {
    Daemon {
        host: u16,
        inc: u32,
        event: Event,
    },
    BdevWriteDone {
        host: u16,
        inc: u32,
        bdev_io: BdevIo,
        io: IoId,
    },
    BdevReadDone {
        host: u16,
        inc: u32,
        io: IoId,
        bytes: Option<Vec<u8>>,
    },
    GuestStep {
        vset: VsetId,
    },
    CheckpointTick {
        vset: VsetId,
    },
    KillHost(u16),
    MigrateAt {
        vset: VsetId,
        to: u16,
    },
    PeerDeliver {
        from: u16,
        to: u16,
        msg: blockd_core::seam::PeerMsg,
    },
}

struct Cluster {
    config: ClusterConfig,
    kernel: Kernel<Ev>,
    hosts: Vec<HostState>,
    store: ObjectStore,
    placement: BTreeMap<VsetId, u16>,
    guests: BTreeMap<VsetId, Guest>,
    oracle: Oracle,
    next_req: u64,
    sync_reqs: BTreeMap<ReqId, VsetId>,
    admin_reqs: BTreeMap<ReqId, VsetId>,
    /// Head manifest pointer captured at the kill instant, per orphan.
    expected_ptr: BTreeMap<VsetId, Option<ManifestPtr>>,
    /// `RestoreVset` send instants, for the R6.2 latency measurement.
    restore_sent: BTreeMap<ReqId, SimTime>,
    /// Last `PauseGuest` instant per vset (the R7.1 pause measurement).
    paused_at: BTreeMap<VsetId, SimTime>,
    /// Migrated-in vsets and the source host still serving their tail.
    migrated_from: BTreeMap<VsetId, u16>,
    /// Vsets whose migration source died mid-drain: unservable pages are
    /// the sanctioned R7.3 loss, not a violation.
    doomed: BTreeSet<VsetId>,
    report: ClusterReport,
}

pub fn run(seed: u64, config: ClusterConfig) -> ClusterReport {
    let kernel = Kernel::new(seed);
    let store = ObjectStore::new(config.store.clone());
    let mut hosts = Vec::new();
    let mut boot_effects = Vec::new();
    for h in 0..config.hosts {
        let mut daemon_config = config.daemon.clone();
        daemon_config.host = HostId(h);
        let (daemon, effects) = Daemon::new(daemon_config);
        hosts.push(HostState {
            daemon: Some(daemon),
            inc: 0,
            bdev: BlobDev::new(config.bdev.clone()),
            mems: BTreeMap::new(),
            shared_base: BTreeMap::new(),
        });
        boot_effects.push(effects);
    }
    let mut c = Cluster {
        config,
        kernel,
        hosts,
        store,
        placement: BTreeMap::new(),
        guests: BTreeMap::new(),
        oracle: Oracle::new(),
        next_req: 0,
        sync_reqs: BTreeMap::new(),
        admin_reqs: BTreeMap::new(),
        expected_ptr: BTreeMap::new(),
        restore_sent: BTreeMap::new(),
        paused_at: BTreeMap::new(),
        migrated_from: BTreeMap::new(),
        doomed: BTreeSet::new(),
        report: ClusterReport::default(),
    };
    for (h, effects) in boot_effects.into_iter().enumerate() {
        c.apply_effects(u16::try_from(h).expect("fits"), effects);
    }

    for n in 1..=c.config.vset_count {
        let vset = VsetId(u64::from(n));
        let host = (n - 1) % c.config.hosts;
        c.placement.insert(vset, host);
        let req = c.req();
        c.admin_reqs.insert(req, vset);
        let config = c.config.vset_config;
        c.step_daemon(
            host,
            Event::Admin(AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base: None,
            }),
        );
    }
    for &(at, host) in &c.config.kill_hosts_at {
        c.kernel.schedule_at(SimTime(at), Ev::KillHost(host));
    }
    if let Some((at, vset, to)) = c.config.migrate_at {
        c.kernel
            .schedule_at(SimTime(at), Ev::MigrateAt { vset, to });
    }

    let end = SimTime(c.config.horizon + 2 * millis(1000));
    while let Some((at, event)) = c.kernel.pop() {
        if at > end {
            break;
        }
        c.dispatch(event);
    }

    c.report.trace_hash = c.kernel.trace_hash();
    c.report
        .violations
        .extend(std::mem::take(&mut c.oracle.violations));
    c.report.completed_ops = c.guests.values().map(|g| g.completed).sum();
    c.report.prefetch_fills = c
        .hosts
        .iter()
        .filter_map(|h| h.daemon.as_ref())
        .map(|d| d.counters.prefetch_fills)
        .sum();
    c.report
}

impl Cluster {
    fn req(&mut self) -> ReqId {
        let req = ReqId(self.next_req);
        self.next_req += 1;
        req
    }

    fn step_daemon(&mut self, host: u16, event: Event) {
        let state = &mut self.hosts[usize::from(host)];
        let Some(daemon) = &mut state.daemon else {
            return;
        };
        let effects = daemon.step(event, &MemView(&state.mems));
        self.apply_effects(host, effects);
    }

    #[allow(clippy::too_many_lines)]
    fn apply_effects(&mut self, host: u16, effects: Vec<Effect>) {
        for effect in effects {
            self.kernel.observe(&(host, &effect));
            let inc = self.hosts[usize::from(host)].inc;
            match effect {
                Effect::Fill {
                    page,
                    bytes,
                    writable,
                    share,
                } => {
                    if let Some(key) = share {
                        self.hosts[usize::from(host)]
                            .shared_base
                            .insert(key, bytes.clone());
                    }
                    self.fill(host, page, bytes, writable);
                }
                Effect::FillShared {
                    page,
                    share,
                    writable,
                } => {
                    let bytes = self.hosts[usize::from(host)].shared_base[&share].clone();
                    self.fill(host, page, bytes, writable);
                }
                Effect::FillFailed { page } => self.fill_failed(page),
                Effect::Unprotect { page } => {
                    let mems = &mut self.hosts[usize::from(host)].mems;
                    if let Some(mem) = mems.get_mut(&page.volume.vset) {
                        mem.protected.remove(&page);
                    }
                    self.resolve_write(host, page);
                }
                Effect::WriteProtect { pages } => {
                    let mems = &mut self.hosts[usize::from(host)].mems;
                    for page in pages {
                        if let Some(mem) = mems.get_mut(&page.volume.vset) {
                            mem.protected.insert(page);
                        }
                    }
                }
                Effect::Evict { page } => {
                    let mems = &mut self.hosts[usize::from(host)].mems;
                    if let Some(mem) = mems.get_mut(&page.volume.vset) {
                        mem.pages.remove(&page);
                        mem.protected.remove(&page);
                    }
                }
                Effect::PauseGuest { vset } => {
                    self.paused_at.insert(vset, self.kernel.now());
                    let guest = self.guests.get_mut(&vset).expect("guest exists");
                    guest.paused = true;
                    let vmstate = guest.applied;
                    let delay = self.kernel.rng().range(micros(20), micros(200));
                    self.kernel.schedule_after(
                        delay,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::GuestPaused { vset, vmstate },
                        },
                    );
                }
                Effect::ResumeGuest { vset } => {
                    let guest = self.guests.get_mut(&vset).expect("guest exists");
                    guest.paused = false;
                    self.unpark(host, vset);
                }
                Effect::SyncOk { req } => self.sync_done(req, true),
                Effect::SyncFailed { req } => self.sync_done(req, false),
                Effect::BlobWrite { io, name, bytes } => {
                    let now = self.kernel.now();
                    let state = &mut self.hosts[usize::from(host)];
                    let (bdev_io, at) =
                        state.bdev.submit_write(now, self.kernel.rng(), name, bytes);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevWriteDone {
                            host,
                            inc,
                            bdev_io,
                            io,
                        },
                    );
                }
                Effect::BlobRead { io, name } => {
                    let now = self.kernel.now();
                    let state = &mut self.hosts[usize::from(host)];
                    let (at, bytes) = state.bdev.read(now, self.kernel.rng(), &name);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevReadDone {
                            host,
                            inc,
                            io,
                            bytes,
                        },
                    );
                }
                Effect::BlobReadRange {
                    io,
                    name,
                    offset,
                    len,
                } => {
                    let now = self.kernel.now();
                    let state = &mut self.hosts[usize::from(host)];
                    let (at, bytes) =
                        state
                            .bdev
                            .read_range(now, self.kernel.rng(), &name, offset, len);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevReadDone {
                            host,
                            inc,
                            io,
                            bytes,
                        },
                    );
                }
                Effect::BlobDelete { name } => {
                    self.hosts[usize::from(host)].bdev.delete(&name);
                }
                Effect::SetTimer { timer, after } => {
                    self.kernel.schedule_after(
                        after,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::Timer(timer),
                        },
                    );
                }
                Effect::StorePut { io, key, bytes } => {
                    let now = self.kernel.now();
                    let (at, result) = self.store.put(now, self.kernel.rng(), &key, bytes);
                    let result = result.map(|v| v.0).map_err(store_fault);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StorePutDone { io, result },
                        },
                    );
                }
                Effect::StoreCas {
                    io,
                    key,
                    expected,
                    bytes,
                } => {
                    let now = self.kernel.now();
                    let (at, result) = self.store.put_cas(
                        now,
                        self.kernel.rng(),
                        &key,
                        expected.map(Version),
                        bytes,
                    );
                    let result = result.map(|v| v.0).map_err(store_fault);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StorePutDone { io, result },
                        },
                    );
                }
                Effect::StoreGet { io, key } => {
                    let now = self.kernel.now();
                    let (at, result) = self.store.get(now, self.kernel.rng(), &key);
                    let result = result
                        .map(|found| found.map(|(v, b)| (v.0, b)))
                        .map_err(store_fault);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StoreGetDone { io, result },
                        },
                    );
                }
                Effect::StoreGetRange {
                    io,
                    key,
                    offset,
                    len,
                } => {
                    let now = self.kernel.now();
                    let (at, result) =
                        self.store
                            .get_range(now, self.kernel.rng(), &key, offset, len);
                    let result = result
                        .map(|found| found.map(|(v, b)| (v.0, b)))
                        .map_err(store_fault);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StoreGetDone { io, result },
                        },
                    );
                }
                Effect::StoreDelete { key } => {
                    let now = self.kernel.now();
                    let _ = self.store.delete(now, self.kernel.rng(), &key);
                }
                Effect::VsetFenced { vset } => {
                    if self.placement.get(&vset) == Some(&host)
                        && let Some(guest) = self.guests.get_mut(&vset)
                    {
                        guest.state = GuestState::Dead;
                    }
                }
                Effect::Admin(reply) => self.admin_reply(host, reply),
                Effect::PeerSend { to, msg } => {
                    // The cluster network: peers reach each other with a
                    // small latency; a dead destination just never answers
                    // (handled at delivery).
                    let delay = self.kernel.rng().range(micros(50), micros(500));
                    self.kernel.schedule_after(
                        delay,
                        Ev::PeerDeliver {
                            from: host,
                            to: to.0,
                            msg,
                        },
                    );
                }
                Effect::Abort { reason } => {
                    self.report
                        .violations
                        .push(format!("daemon {host} aborted: {reason}"));
                }
            }
        }
    }

    fn dispatch(&mut self, event: Ev) {
        match event {
            Ev::Daemon { host, inc, event } => {
                if self.hosts[usize::from(host)].inc == inc {
                    self.step_daemon(host, event);
                }
            }
            Ev::BdevWriteDone {
                host,
                inc,
                bdev_io,
                io,
            } => {
                if self.hosts[usize::from(host)].inc == inc {
                    self.hosts[usize::from(host)].bdev.complete_write(bdev_io);
                    self.step_daemon(host, Event::BlobWriteDone { io });
                }
            }
            Ev::BdevReadDone {
                host,
                inc,
                io,
                bytes,
            } => {
                if self.hosts[usize::from(host)].inc == inc {
                    self.step_daemon(host, Event::BlobReadDone { io, bytes });
                }
            }
            Ev::GuestStep { vset } => self.guest_step(vset),
            Ev::CheckpointTick { vset } => {
                let host = self.placement[&vset];
                if self.hosts[usize::from(host)].daemon.is_some() {
                    let req = self.req();
                    self.admin_reqs.insert(req, vset);
                    self.step_daemon(host, Event::Admin(AdminCmd::Checkpoint { req, vset }));
                }
                if let Some(interval) = self.config.checkpoint_interval
                    && self.kernel.now().nanos() <= self.config.horizon
                {
                    let delay = self.kernel.rng().range(1, 2 * interval);
                    self.kernel
                        .schedule_after(delay, Ev::CheckpointTick { vset });
                }
            }
            Ev::KillHost(host) => self.kill_host(host),
            Ev::MigrateAt { vset, to } => {
                let host = self.placement[&vset];
                if self.hosts[usize::from(host)].daemon.is_some() {
                    let req = self.req();
                    self.admin_reqs.insert(req, vset);
                    self.step_daemon(
                        host,
                        Event::Admin(AdminCmd::MigrateOut {
                            req,
                            vset,
                            to: HostId(to),
                        }),
                    );
                }
            }
            Ev::PeerDeliver { from, to, msg } => {
                if self.hosts[usize::from(to)].daemon.is_some() {
                    self.step_daemon(
                        to,
                        Event::PeerDelivered {
                            from: HostId(from),
                            msg,
                        },
                    );
                } else if let blockd_core::seam::PeerMsg::FetchRange { io, .. } = msg {
                    // A dead source answers nothing; the harness surfaces
                    // the silence as an explicit miss so the R7.3 failure
                    // is loud, not a hang.
                    let delay = self.kernel.rng().range(micros(50), micros(500));
                    self.kernel.schedule_after(
                        delay,
                        Ev::PeerDeliver {
                            from: to,
                            to: from,
                            msg: blockd_core::seam::PeerMsg::Page { io, bytes: None },
                        },
                    );
                }
            }
        }
    }

    /// Permanent host death (R6.1's premise): volatile state and guests are
    /// gone; the control plane restores each backed-up orphan elsewhere —
    /// racing two claimants when configured.
    fn kill_host(&mut self, host: u16) {
        let state = &mut self.hosts[usize::from(host)];
        if state.daemon.take().is_none() {
            return;
        }
        state.inc += 1;
        state.mems.clear();
        state.shared_base.clear();
        // Migrated-in vsets whose source this was lose their tail: their
        // guests' next unservable fault is the sanctioned R7.3 cost.
        for (&vset, &source) in &self.migrated_from {
            if source == host {
                self.doomed.insert(vset);
            }
        }
        let orphans: Vec<VsetId> = self
            .placement
            .iter()
            .filter(|&(_, h)| *h == host)
            .map(|(v, _)| *v)
            .collect();
        for vset in orphans {
            if let Some(guest) = self.guests.get_mut(&vset) {
                guest.state = GuestState::Dead;
            }
            // The R4.3 bound: whatever the head points at right now is the
            // most anyone may recover.
            let ptr = self
                .store
                .peek(&layout::head_key(vset))
                .and_then(|bytes| HeadRecord::decode(vset, bytes).ok())
                .and_then(|head| head.manifest);
            self.expected_ptr.insert(vset, ptr);

            let first = (host + 1) % self.config.hosts;
            let req = self.req();
            self.admin_reqs.insert(req, vset);
            self.restore_sent.insert(req, self.kernel.now());
            self.step_daemon(first, Event::Admin(AdminCmd::RestoreVset { req, vset }));
            if self.config.race_restore && self.config.hosts > 2 {
                let second = (host + 2) % self.config.hosts;
                let req = self.req();
                self.admin_reqs.insert(req, vset);
                self.restore_sent.insert(req, self.kernel.now());
                self.step_daemon(second, Event::Admin(AdminCmd::RestoreVset { req, vset }));
            }
        }
    }

    // ── guests (identical semantics to the single-host harness, routed by
    // placement) ─────────────────────────────────────────────────────────

    fn schedule_guest(&mut self, vset: VsetId) {
        let (lo, hi) = self.config.think;
        let delay = self.kernel.rng().range(lo, hi);
        self.kernel.schedule_after(delay, Ev::GuestStep { vset });
    }

    fn guest_step(&mut self, vset: VsetId) {
        let host = self.placement[&vset];
        if self.hosts[usize::from(host)].daemon.is_none() {
            return;
        }
        let Cluster {
            kernel,
            guests,
            oracle,
            ..
        } = self;
        let Some(guest) = guests.get_mut(&vset) else {
            return;
        };
        if guest.state != GuestState::Idle || guest.paused {
            return;
        }
        match guest.next_op(kernel.rng(), |volume| oracle.next_vol_seq(volume)) {
            Err(volume) => {
                let req = self.req();
                self.sync_reqs.insert(req, vset);
                self.guests.get_mut(&vset).expect("guest exists").state =
                    GuestState::Syncing { req, volume };
                self.step_daemon(host, Event::GuestSync { req, volume });
            }
            Ok(op) => self.attempt_op(host, vset, op),
        }
    }

    fn attempt_op(&mut self, host: u16, vset: VsetId, op: PendingOp) {
        let mems = &mut self.hosts[usize::from(host)].mems;
        let mem = mems.entry(vset).or_default();
        let (page, write) = match op {
            PendingOp::Write { page, .. } => (page, true),
            PendingOp::Read { page } | PendingOp::Fsck { page } => (page, false),
        };
        let resident = mem.pages.contains_key(&page);
        let trapped = !resident || (write && mem.protected.contains(&page));
        if trapped {
            self.guests.get_mut(&vset).expect("guest exists").state = GuestState::Faulted { op };
            self.step_daemon(host, Event::GuestFault { page, write });
            return;
        }
        self.complete_op(host, vset, op);
    }

    fn complete_op(&mut self, host: u16, vset: VsetId, op: PendingOp) {
        // Every retired op sets the page's accessed bit — the ground truth
        // MGLRU aging harvests (R2.6).
        let (PendingOp::Write { page, .. } | PendingOp::Read { page } | PendingOp::Fsck { page }) =
            op;
        if let Some(mem) = self.hosts[usize::from(host)].mems.get(&vset) {
            mem.accessed.borrow_mut().insert(page);
        }
        match op {
            PendingOp::Write { page, vol_seq } => {
                let mems = &mut self.hosts[usize::from(host)].mems;
                let mem = mems.get_mut(&vset).expect("mapped");
                assert!(!mem.protected.contains(&page), "write to protected page");
                mem.pages.insert(page, page_pattern(page, vol_seq));
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.applied += 1;
                let op_index = guest.applied;
                self.oracle.on_write_ok(page, vol_seq, op_index);
            }
            PendingOp::Read { .. } => {
                self.guests.get_mut(&vset).expect("guest exists").applied += 1;
            }
            PendingOp::Fsck { .. } => {
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.applied += 1;
                let done = guest.fsck.is_empty();
                let cold = guest.cold_booting;
                if done && cold {
                    guest.cold_booting = false;
                    self.oracle.finish_cold_boot(vset);
                }
            }
        }
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        guest.state = GuestState::Idle;
        guest.completed += 1;
        let fsck_pending = !guest.fsck.is_empty();
        if self.kernel.now().nanos() <= self.config.horizon || fsck_pending {
            self.schedule_guest(vset);
        }
    }

    fn fill(&mut self, host: u16, page: PageId, bytes: Vec<u8>, writable: bool) {
        let vset = page.volume.vset;
        if self.placement.get(&vset) != Some(&host) {
            return; // fill from a fenced incarnation's tail
        }
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        let waited = match guest.state {
            GuestState::Faulted { op } => {
                let (PendingOp::Write { page: p, .. }
                | PendingOp::Read { page: p }
                | PendingOp::Fsck { page: p }) = op;
                (p == page).then_some(op)
            }
            _ => None,
        };
        let Some(op) = waited else {
            // Unsolicited fill: prefetch pre-population (R6.2) — the
            // daemon wrote the bytes into the shmem backing and mapped
            // them with UFFDIO_CONTINUE ahead of the fault (COPY would
            // make a private page and break R5.3 sharing). Validate and
            // install; nothing retires.
            // During cold-boot inference the bytes are the restored disk
            // state, exactly like an fsck fill.
            self.oracle.check_fill(page, &bytes, guest.cold_booting);
            let mems = &mut self.hosts[usize::from(host)].mems;
            let mem = mems.entry(vset).or_default();
            mem.pages.insert(page, bytes);
            mem.protected.insert(page);
            return;
        };
        let cold_fsck = matches!(op, PendingOp::Fsck { .. }) && guest.cold_booting;
        self.oracle.check_fill(page, &bytes, cold_fsck);
        let mems = &mut self.hosts[usize::from(host)].mems;
        let mem = mems.entry(vset).or_default();
        mem.pages.insert(page, bytes);
        if writable {
            mem.protected.remove(&page);
        } else {
            mem.protected.insert(page);
        }
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        if guest.paused {
            // Memory is resolved, but a paused vCPU retires nothing: the
            // op completes on resume (captures see one instant).
            guest.state = GuestState::Parked { op };
            return;
        }
        guest.state = GuestState::Idle;
        self.complete_op(host, vset, op);
    }

    fn resolve_write(&mut self, host: u16, page: PageId) {
        let vset = page.volume.vset;
        if self.placement.get(&vset) != Some(&host) {
            return;
        }
        let Some(guest) = self.guests.get_mut(&vset) else {
            return;
        };
        let GuestState::Faulted { op } = guest.state else {
            return;
        };
        guest.state = GuestState::Idle;
        self.complete_op(host, vset, op);
    }

    fn fill_failed(&mut self, page: PageId) {
        let vset = page.volume.vset;
        let Some(guest) = self.guests.get_mut(&vset) else {
            return;
        };
        let GuestState::Faulted { op } = guest.state else {
            return;
        };
        // No damage is injected in cluster runs: an unservable page is a
        // real violation — unless the vset's migration source died with the
        // post-copy drain incomplete, the sanctioned R7.3 loss.
        self.oracle
            .on_fill_failed(page, self.doomed.contains(&vset));
        if matches!(op, PendingOp::Fsck { .. }) && guest.cold_booting {
            self.oracle.on_fsck_aborted(vset);
        }
        guest.state = GuestState::Dead;
        self.report.guest_deaths += 1;
    }

    fn sync_done(&mut self, req: ReqId, ok: bool) {
        let Some(vset) = self.sync_reqs.remove(&req) else {
            return;
        };
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        let GuestState::Syncing {
            req: waiting,
            volume,
        } = guest.state
        else {
            return;
        };
        if waiting != req || !ok {
            return;
        }
        if guest.paused {
            guest.state = GuestState::SyncParked { volume };
            return;
        }
        guest.applied += 1;
        guest.state = GuestState::Idle;
        guest.completed += 1;
        self.oracle.on_sync_ok(volume);
        if self.kernel.now().nanos() <= self.config.horizon {
            self.schedule_guest(vset);
        }
    }

    /// Retire whatever completed while the vCPU was paused.
    fn unpark(&mut self, host: u16, vset: VsetId) {
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        match guest.state {
            GuestState::Parked { op } => {
                guest.state = GuestState::Idle;
                self.complete_op(host, vset, op);
            }
            GuestState::SyncParked { volume } => {
                guest.applied += 1;
                guest.state = GuestState::Idle;
                guest.completed += 1;
                self.oracle.on_sync_ok(volume);
                if self.kernel.now().nanos() <= self.config.horizon {
                    self.schedule_guest(vset);
                }
            }
            _ => self.schedule_guest(vset),
        }
    }

    fn admin_reply(&mut self, host: u16, reply: AdminReply) {
        if std::env::var_os("BLOCKD_SIM_DEBUG").is_some() {
            eprintln!("[{:>12}] host {host}: {reply:?}", self.kernel.now().nanos());
        }
        match reply {
            AdminReply::VsetCreated { req, vset } => {
                self.admin_reqs.remove(&req);
                self.oracle.register(vset, self.config.vset_config);
                self.hosts[usize::from(host)]
                    .mems
                    .insert(vset, VsetMem::default());
                self.guests
                    .insert(vset, Guest::new(vset, self.config.vset_config));
                self.schedule_guest(vset);
                if let Some(interval) = self.config.checkpoint_interval {
                    let delay = self.kernel.rng().range(1, 2 * interval);
                    self.kernel
                        .schedule_after(delay, Ev::CheckpointTick { vset });
                }
            }
            AdminReply::CheckpointDone { req, .. } | AdminReply::AdminFailed { req } => {
                if self.admin_reqs.remove(&req).is_some()
                    && matches!(reply, AdminReply::AdminFailed { .. })
                {
                    // Restore losers land here: exactly-one-runner (R6.3).
                    self.report.claims_lost += 1;
                }
            }
            AdminReply::VsetRestored { req, vset, verdict } => {
                self.admin_reqs.remove(&req);
                self.report.restores += 1;
                if let Some(sent) = self.restore_sent.remove(&req) {
                    let latency = self.kernel.now().nanos() - sent.nanos();
                    self.report.max_restore_ns = self.report.max_restore_ns.max(latency);
                }
                self.placement.insert(vset, host);
                self.hosts[usize::from(host)]
                    .mems
                    .insert(vset, VsetMem::default());
                // Verify the R4.3 loss bound: the restored point is exactly
                // what the head promised at the kill instant.
                let restored = self
                    .store
                    .peek(&layout::head_key(vset))
                    .and_then(|bytes| HeadRecord::decode(vset, bytes).ok())
                    .and_then(|head| head.manifest);
                match (self.expected_ptr.remove(&vset), restored) {
                    (Some(expected), got) if expected == got => {
                        self.report.loss_bound_verified += 1;
                    }
                    (None, _) => {}
                    (Some(expected), got) => self.report.violations.push(format!(
                        "R4.3: {vset:?} restored to {got:?}, head at death said {expected:?}"
                    )),
                }
                self.oracle.allow_sync_loss(vset);
                match verdict {
                    Verdict::Resume { vmstate, .. } => {
                        self.oracle.on_resume(vset, vmstate);
                        let infer = self.oracle.needs_disk_inference(vset);
                        let guest = self.guests.get_mut(&vset).expect("guest exists");
                        guest.reborn(vmstate, infer);
                    }
                    Verdict::ColdBoot | Verdict::Unrestorable => {
                        self.oracle.start_cold_boot(vset);
                        let guest = self.guests.get_mut(&vset).expect("guest exists");
                        guest.reborn(0, true);
                    }
                }
                self.schedule_guest(vset);
            }
            AdminReply::MigratedOut { req, .. } => {
                self.admin_reqs.remove(&req);
                self.report.migrations += 1;
            }
            AdminReply::VsetMigratedIn { vset, verdict } => {
                // R7.1: the guest-observed pause spans the source's pause
                // to the destination coming up ready to serve.
                if let Some(paused) = self.paused_at.get(&vset) {
                    let pause = self.kernel.now().nanos() - paused.nanos();
                    self.report.max_migration_pause_ns =
                        self.report.max_migration_pause_ns.max(pause);
                }
                // The destination's first record is durable: the vset now
                // runs here (R7.2), demand-faulting its tail from the
                // source (R7.1: memory arrives post-copy).
                let source = self.placement.insert(vset, host).expect("was placed");
                self.migrated_from.insert(vset, source);
                self.hosts[usize::from(host)]
                    .mems
                    .insert(vset, VsetMem::default());
                let Verdict::Resume { vmstate, .. } = verdict else {
                    self.report
                        .violations
                        .push(format!("R7: {vset:?} migrated in without resume state"));
                    return;
                };
                // Migration is lossless: the same on_resume check as a
                // restore, with NO sync-loss allowance (R7.1 vs R4.3).
                self.oracle.on_resume(vset, vmstate);
                let infer = self.oracle.needs_disk_inference(vset);
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.reborn(vmstate, infer);
                self.schedule_guest(vset);
            }
            AdminReply::VsetRecovered { .. }
            | AdminReply::BaseKept { .. }
            | AdminReply::BaseDeleted { .. }
            | AdminReply::VsetForked { .. } => {}
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn store_fault(err: crate::world::store::StoreError) -> blockd_core::seam::StoreFault {
    use crate::world::store::StoreError;
    match err {
        StoreError::Unavailable | StoreError::TooLarge => {
            blockd_core::seam::StoreFault::Unavailable
        }
        StoreError::CasConflict { actual } => blockd_core::seam::StoreFault::CasConflict {
            actual: actual.map(|v| v.0),
        },
    }
}
