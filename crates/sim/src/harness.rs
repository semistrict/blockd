//! The single-host harness: wires the real daemon (`blockd_core::daemon`)
//! to the simulated world — blob device, guest memory, guests, admin,
//! nemesis — under one kernel. Guest memory lives here, exactly like the
//! shared mapping in production: resident writable pages are read and
//! written by guests with no daemon involvement; only faults, syncs and
//! pauses cross the boundary. A run is `(seed, config) → RunReport`,
//! byte-for-byte replayable.

use std::collections::{BTreeMap, BTreeSet};

use blockd_core::daemon::{Counters, Daemon, DaemonConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::layout::{self, BlobName};
use blockd_core::seam::{AdminCmd, AdminReply, Effect, Event, HostMap, IoId, ReqId, Verdict};
use blockd_core::types::{PageId, SimTime, VsetId, micros, millis};

use crate::guest::{Guest, GuestState, PendingOp, page_pattern};
use crate::kernel::Kernel;
use crate::oracle::Oracle;
use crate::world::blobdev::{BdevIo, BlobDev, BlobDevConfig};
use crate::world::store::{ObjectStore, StoreConfig, StoreError, Version};
use blockd_core::seam::StoreFault;

#[derive(Clone, Debug)]
pub struct FaultPlan {
    /// Mean nanoseconds between daemon crashes (0 disables).
    pub crash_mean_interval: u64,
    /// Restart delay range after a crash.
    pub restart_delay: (u64, u64),
    /// Mean nanoseconds between segment bit flips (0 disables).
    pub bitflip_mean_interval: u64,
    /// Mean nanoseconds between journal bit flips (0 disables). Only
    /// backed-up vsets can survive these — via restore from the store.
    pub journal_bitflip_mean_interval: u64,
    /// One store outage window (R8.3), sim-time nanoseconds.
    pub store_outage: Option<(u64, u64)>,
}

impl FaultPlan {
    pub fn none() -> FaultPlan {
        FaultPlan {
            crash_mean_interval: 0,
            restart_delay: (millis(10), millis(500)),
            bitflip_mean_interval: 0,
            journal_bitflip_mean_interval: 0,
            store_outage: None,
        }
    }
}

/// Deliberate misbehavior for negative tests: prove the oracle catches a
/// broken daemon/world, so green runs mean something.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sabotage {
    /// Corrupt every fill's first byte after delivery-side verification —
    /// bytes the daemon never vouched for reach the guest.
    CorruptFill,
    /// Silently drop `WriteProtect` effects: the daemon misses re-dirtied
    /// pages and captures stale state.
    DropWriteProtect,
    /// Acknowledge the migration handoff marker's write without persisting
    /// it (cluster harness only): the source offers before its side of the
    /// two-sided handoff is durable — a crash then recovers it runnable
    /// while the destination also runs (the double-run R7.2 forbids).
    EagerHandoffAck,
    /// A host that is NOT the migration destination sends `Released` to
    /// the source mid-drain (cluster harness only). Unguarded, the source
    /// reclaims a live vset's tail out from under the real destination —
    /// the guard must reject the wrong counterparty (R11.1).
    RogueRelease,
}

#[derive(Clone, Debug)]
pub struct HarnessConfig {
    pub daemon: DaemonConfig,
    pub bdev: BlobDevConfig,
    pub store: StoreConfig,
    pub vset_count: u16,
    /// The first `backed_vsets` vsets are created backed-up (R4.1); the
    /// rest never touch the store (R4.4).
    pub backed_vsets: u16,
    pub vset_config: VsetConfig,
    /// Stop issuing new work after this instant; the run drains briefly and
    /// stops.
    pub horizon: u64,
    /// Guest think time between operations.
    pub think: (u64, u64),
    /// Periodic whole-vset checkpoints (None = never; R3.2 — nothing may
    /// rely on them).
    pub checkpoint_interval: Option<u64>,
    pub faults: FaultPlan,
    /// Negative-test hook; `None` in every honest run.
    pub sabotage: Option<Sabotage>,
    /// Override the guests' sync share of the op mix (`None` = default).
    pub guest_sync_share: Option<crate::rng::Ppm>,
    /// Guest access skew (`None` = uniform): (share, N) sends that share
    /// of page picks to each volume's first N pages.
    pub guest_hot_pages: Option<(crate::rng::Ppm, u32)>,
    /// Targeted rot (adversarial, not Poisson): at each instant, flip one
    /// bit in the NEWEST journal record's primary (`false`) or mirror
    /// (`true`) copy. The newest record is the single carrier of its
    /// newly-acked syncs — exactly the copy whose loss must be survivable.
    pub rot_records_at: Vec<(u64, bool)>,
    /// Scheduled daemon crashes at exact instants (besides the Poisson
    /// plan) — what lets a test crash inside a specific protocol window.
    pub crash_at: Vec<u64>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RunReport {
    pub trace_hash: u64,
    pub violations: Vec<String>,
    pub counters: Counters,
    pub completed_ops: u64,
    pub per_guest_completed: BTreeMap<u64, u64>,
    pub crashes: u64,
    pub resumes: u64,
    pub cold_boots: u64,
    pub unrestorable: u64,
    pub guest_deaths: u64,
    pub bitflips: u64,
    pub blob_count: usize,
    /// Longest guest-visible checkpoint pause (R3.1).
    pub max_pause_ns: u64,
    /// Restores from the object store that succeeded (R6.1).
    pub restores: u64,
    /// Every object key in the store at the end of the run (R4.4 audits).
    pub store_keys: Vec<String>,
    /// Total bytes of page-map metadata written locally across the run —
    /// journal records and map leaves. The cost of remembering where pages
    /// live must track the DELTA, not the vset size.
    pub map_bytes_written: u64,
    /// The most pages any single `Daemon::step` read through `HostMap` —
    /// the step-cost bound (2c): a step's work must not scale with fleet
    /// size, or every guest's fault waits behind it in the real runtime.
    pub max_step_page_reads: u64,
    /// The largest single journal record ever written: bounded by the
    /// overlay cap regardless of vset size (leaves carry the bulk).
    pub max_record_blob_bytes: u64,
    /// Total bytes of segment blobs left on the device at the end: the
    /// space-amplification measure — bounded by live data, not by history.
    pub seg_bytes_end: u64,
    /// Bytes the serving maps still reference at the end (the daemon's
    /// own live accounting, R9.2) — what `seg_bytes_end` is bounded by.
    pub seg_live_bytes_end: u64,
    /// The liveness oracle's end-state: fills still parked (pressure,
    /// outage, unhydrated spans) when the run drained. A healed run must
    /// end at 0 — convergence, not just safety.
    pub parked_end: usize,
}

#[derive(Debug)]
enum Ev {
    Daemon {
        inc: u32,
        event: Event,
    },
    BdevWriteDone {
        inc: u32,
        bdev_io: BdevIo,
        io: IoId,
    },
    BdevReadDone {
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
    CrashDaemon,
    RestartDaemon,
    Bitflip,
    JournalBitflip,
    /// Targeted rot: the newest record's primary or mirror (`true`) copy.
    RotNewestRecord(bool),
    StoreOutage(bool),
}

// The checkpoint's vset is carried for Debug context only.
#[allow(dead_code)]
#[derive(Debug)]
enum AdminKind {
    Create(VsetId),
    Checkpoint(VsetId),
    Restore(VsetId),
}

/// One vset's guest memory: the shared mapping. Bytes are the real page
/// contents; `protected` pages trap writes (uffd write-protect).
#[derive(Debug, Default)]
struct VsetMem {
    pages: BTreeMap<PageId, Vec<u8>>,
    protected: BTreeSet<PageId>,
    /// Pages the guest touched since the last accessed-bit harvest — the
    /// simulation's ground truth behind MGLRU aging (R2.6).
    accessed: std::cell::RefCell<BTreeSet<PageId>>,
}

/// The daemon's synchronous window onto the mappings.
struct MemView<'a> {
    mems: &'a BTreeMap<VsetId, VsetMem>,
    /// Page reads this step — the sim's window onto STEP COST (2c): wall
    /// time cannot pass inside a step, but work units can be counted, so
    /// "one step captures a bounded amount" becomes a checkable invariant.
    reads: &'a std::cell::Cell<u64>,
}

impl HostMap for MemView<'_> {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        self.reads.set(self.reads.get() + 1);
        self.mems[&page.volume.vset].pages[&page].clone()
    }

    fn harvest_accessed(&self) -> Vec<PageId> {
        // One-shot: drain every guest's touch record. `mems` is a BTreeMap
        // and each set is ordered, so the result is deterministic.
        self.mems
            .values()
            .flat_map(|mem| mem.accessed.take())
            .collect()
    }
}

struct Harness {
    config: HarnessConfig,
    kernel: Kernel<Ev>,
    bdev: BlobDev,
    store: ObjectStore,
    daemon: Option<Daemon>,
    inc: u32,
    mems: BTreeMap<VsetId, VsetMem>,
    guests: BTreeMap<VsetId, Guest>,
    oracle: Oracle,
    next_req: u64,
    sync_reqs: BTreeMap<ReqId, VsetId>,
    admin_reqs: BTreeMap<ReqId, AdminKind>,
    poisoned: BTreeSet<VsetId>,
    /// The host's shared base-page tier bytes (R5.3), keyed by location.
    shared_base: BTreeMap<(u64, u64, blockd_core::types::SegId, u32), Vec<u8>>,
    /// Write ops whose page just installed write-protected: the vCPU's
    /// retry traps again as a WP fault once the current effect batch is
    /// applied (real uffd's double fault under an unsolicited fill).
    refaults: Vec<PageId>,
    pause_started: BTreeMap<VsetId, SimTime>,
    last_counters: Counters,
    report: RunReport,
}

/// Arm the run's fault plan: Poisson streams, the outage window, and the
/// scheduled adversarial instants (targeted rot, exact crashes).
fn schedule_faults(h: &mut Harness) {
    if h.config.faults.crash_mean_interval > 0 {
        let at = h.next_after(h.config.faults.crash_mean_interval);
        h.kernel.schedule_at(at, Ev::CrashDaemon);
    }
    if h.config.faults.bitflip_mean_interval > 0 {
        let at = h.next_after(h.config.faults.bitflip_mean_interval);
        h.kernel.schedule_at(at, Ev::Bitflip);
    }
    if h.config.faults.journal_bitflip_mean_interval > 0 {
        let at = h.next_after(h.config.faults.journal_bitflip_mean_interval);
        h.kernel.schedule_at(at, Ev::JournalBitflip);
    }
    if let Some((begin, end)) = h.config.faults.store_outage {
        h.kernel.schedule_at(SimTime(begin), Ev::StoreOutage(true));
        h.kernel.schedule_at(SimTime(end), Ev::StoreOutage(false));
    }
    for &(at, mirror) in &h.config.rot_records_at {
        h.kernel
            .schedule_at(SimTime(at), Ev::RotNewestRecord(mirror));
    }
    for &at in &h.config.crash_at {
        h.kernel.schedule_at(SimTime(at), Ev::CrashDaemon);
    }
}

pub fn run(seed: u64, config: HarnessConfig) -> RunReport {
    run_final_blobs(seed, config).0
}

/// Run, and also export the blob device's final contents verbatim — every
/// name and byte exactly as the run left them, torn tails and bit rot
/// included. The differential recovery test writes these to a real
/// directory and demands the runtime's on-disk scan recover identically.
pub fn run_final_blobs(seed: u64, config: HarnessConfig) -> (RunReport, Vec<(String, Vec<u8>)>) {
    let kernel = Kernel::new(seed);
    let (daemon, effects) = Daemon::new(config.daemon.clone());
    let bdev = BlobDev::new(config.bdev.clone());
    let store = ObjectStore::new(config.store.clone());
    let mut h = Harness {
        config,
        kernel,
        bdev,
        store,
        daemon: Some(daemon),
        inc: 0,
        mems: BTreeMap::new(),
        guests: BTreeMap::new(),
        oracle: Oracle::new(),
        next_req: 0,
        sync_reqs: BTreeMap::new(),
        admin_reqs: BTreeMap::new(),
        poisoned: BTreeSet::new(),
        refaults: Vec::new(),
        shared_base: BTreeMap::new(),
        pause_started: BTreeMap::new(),
        last_counters: Counters::default(),
        report: RunReport::default(),
    };
    h.apply_effects(effects);

    for n in 1..=h.config.vset_count {
        let vset = VsetId(u64::from(n));
        let req = h.req();
        h.admin_reqs.insert(req, AdminKind::Create(vset));
        let cmd = AdminCmd::CreateVset {
            req,
            vset,
            config: h.vset_config_for(vset),
            from_base: None,
        };
        h.step_daemon(Event::Admin(cmd));
    }
    schedule_faults(&mut h);

    let end = SimTime(h.config.horizon + 2 * millis(1000));
    while let Some((at, event)) = h.kernel.pop() {
        if at > end {
            break;
        }
        h.dispatch(event);
    }

    h.report.trace_hash = h.kernel.trace_hash();
    h.report
        .violations
        .extend(std::mem::take(&mut h.oracle.violations));
    h.report.counters = h.daemon.as_ref().map_or(h.last_counters, |d| d.counters);
    h.report.completed_ops = h.guests.values().map(|g| g.completed).sum();
    h.report.per_guest_completed = h.guests.iter().map(|(v, g)| (v.0, g.completed)).collect();
    h.report.bitflips = h.bdev.counters.bitflips;
    h.report.blob_count = h.bdev.blob_count();
    h.report.seg_bytes_end = h
        .bdev
        .scan()
        .filter(|(name, _)| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("seg"))
        })
        .map(|(_, bytes)| bytes.len() as u64)
        .sum();
    h.report.seg_live_bytes_end = h.daemon.as_ref().map_or(0, |d| d.seg_space().0);
    h.report.parked_end = h.daemon.as_ref().map_or(0, Daemon::parked_fills);
    if std::env::var_os("BLOCKD_SIM_DEBUG").is_some() {
        let mut blobs: Vec<(usize, &String)> = h
            .bdev
            .scan()
            .map(|(name, bytes)| (bytes.len(), name))
            .collect();
        blobs.sort_unstable_by(|a, b| b.cmp(a));
        for (size, name) in blobs.iter().take(80) {
            eprintln!("BLOB {size:>9} {name}");
        }
    }
    let now = h.kernel.now();
    h.store.set_outage(false);
    let (_, keys) = h.store.list_prefix(now, h.kernel.rng(), "");
    h.report.store_keys = keys.expect("outage lifted");
    let blobs = h
        .bdev
        .scan()
        .map(|(name, bytes)| (name.clone(), bytes.clone()))
        .collect();
    (h.report, blobs)
}

impl Harness {
    fn vset_config_for(&self, vset: VsetId) -> VsetConfig {
        let mut config = self.config.vset_config;
        config.backed_up = vset.0 <= u64::from(self.config.backed_vsets);
        config
    }

    fn req(&mut self) -> ReqId {
        let req = ReqId(self.next_req);
        self.next_req += 1;
        req
    }

    /// A jittered "mean" interval: uniform in [1, 2 * mean].
    fn next_after(&mut self, mean: u64) -> SimTime {
        let delay = self.kernel.rng().range(1, 2 * mean);
        self.kernel.now().after(delay)
    }

    fn within_horizon(&self) -> bool {
        self.kernel.now().nanos() <= self.config.horizon
    }

    fn step_daemon(&mut self, event: Event) {
        let Some(daemon) = &mut self.daemon else {
            return;
        };
        let reads = std::cell::Cell::new(0);
        let effects = daemon.step(
            event,
            &MemView {
                mems: &self.mems,
                reads: &reads,
            },
        );
        self.report.max_step_page_reads = self.report.max_step_page_reads.max(reads.get());
        self.apply_effects(effects);
        // Retried writes trap again after the batch (see `refaults`).
        while let Some(page) = self.refaults.pop() {
            self.step_daemon(Event::GuestFault { page, write: true });
        }
    }

    // One arm per effect kind; splitting would only scatter the seam.
    #[allow(clippy::too_many_lines)]
    fn apply_effects(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            self.kernel.observe(&effect);
            if let Some(filter) = std::env::var_os("BLOCKD_SIM_TRACE_PAGE") {
                let text = format!("{effect:?}");
                let needle = filter.to_string_lossy();
                if text.len() < 400 && text.contains(needle.as_ref()) {
                    eprintln!("[t={:?}] {text}", self.kernel.now());
                }
            }
            match effect {
                Effect::Fill {
                    page,
                    mut bytes,
                    writable,
                    share,
                } => {
                    if self.config.sabotage == Some(Sabotage::CorruptFill) {
                        bytes[0] ^= 0x01;
                    }
                    if let Some(key) = share {
                        self.shared_base.insert(key, bytes.clone());
                    }
                    self.fill(page, bytes, writable);
                }
                Effect::FillShared {
                    page,
                    share,
                    writable,
                } => {
                    // Zero-copy map (or CoW copy) of the shared base page.
                    let bytes = self.shared_base[&share].clone();
                    self.fill(page, bytes, writable);
                }
                Effect::FillFailed { page } => self.fill_failed(page),
                Effect::Unprotect { page } => {
                    let mem = self.mems.get_mut(&page.volume.vset).expect("mapped");
                    mem.protected.remove(&page);
                    self.resolve_write(page);
                }
                Effect::WriteProtect { pages }
                    if self.config.sabotage != Some(Sabotage::DropWriteProtect) =>
                {
                    for page in pages {
                        let mem = self.mems.get_mut(&page.volume.vset).expect("mapped");
                        assert!(mem.pages.contains_key(&page), "protecting unmapped page");
                        mem.protected.insert(page);
                    }
                }
                Effect::WriteProtect { .. } => {} // dropped: Sabotage::DropWriteProtect
                Effect::Evict { page } => {
                    let mem = self.mems.get_mut(&page.volume.vset).expect("mapped");
                    mem.pages.remove(&page).expect("evicting unmapped page");
                    mem.protected.remove(&page);
                }
                Effect::PauseGuest { vset } => {
                    let guest = self.guests.get_mut(&vset).expect("guest exists");
                    guest.paused = true;
                    let vmstate = guest.applied;
                    self.pause_started.insert(vset, self.kernel.now());
                    // The VMM takes a moment to park vCPUs and serialize
                    // device state.
                    let delay = self.kernel.rng().range(micros(20), micros(200));
                    self.kernel.schedule_after(
                        delay,
                        Ev::Daemon {
                            inc: self.inc,
                            event: Event::GuestPaused { vset, vmstate },
                        },
                    );
                }
                Effect::ResumeGuest { vset } => {
                    if let Some(started) = self.pause_started.remove(&vset) {
                        let pause = self.kernel.now().nanos() - started.nanos();
                        self.report.max_pause_ns = self.report.max_pause_ns.max(pause);
                    }
                    let guest = self.guests.get_mut(&vset).expect("guest exists");
                    guest.paused = false;
                    self.unpark(vset);
                }
                Effect::SyncOk { req } => self.sync_done(req, true),
                Effect::SyncFailed { req } => self.sync_done(req, false),
                Effect::BlobWrite { io, name, bytes } => {
                    // Blob names are lowercase by construction; this is a
                    // suffix check on our own layout, not a file extension.
                    #[allow(clippy::case_sensitive_file_extension_comparisons)]
                    let (rec, map) = (
                        name.ends_with(".rec") || name.ends_with(".recm"),
                        name.ends_with(".map"),
                    );
                    if rec || map {
                        self.report.map_bytes_written += bytes.len() as u64;
                    }
                    if rec {
                        self.report.max_record_blob_bytes =
                            self.report.max_record_blob_bytes.max(bytes.len() as u64);
                    }
                    let now = self.kernel.now();
                    let (bdev_io, at) = self.bdev.submit_write(now, self.kernel.rng(), name, bytes);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevWriteDone {
                            inc: self.inc,
                            bdev_io,
                            io,
                        },
                    );
                }
                Effect::BlobRead { io, name } => {
                    let now = self.kernel.now();
                    let (at, bytes) = self.bdev.read(now, self.kernel.rng(), &name);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevReadDone {
                            inc: self.inc,
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
                    let (at, bytes) =
                        self.bdev
                            .read_range(now, self.kernel.rng(), &name, offset, len);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevReadDone {
                            inc: self.inc,
                            io,
                            bytes,
                        },
                    );
                }
                Effect::BlobDelete { name } => {
                    self.bdev.delete(&name);
                }
                Effect::SetTimer { timer, after } => {
                    // A zero-delay timer means "continue once the loop is
                    // free": the emitting step's own work has real duration
                    // in the runtime, so model one — without it a drain
                    // would run every continuation at a single instant and
                    // nothing (no guest write, no copy-on-fault) could
                    // ever interleave with it.
                    let after = if after == 0 {
                        self.kernel.rng().range(micros(20), micros(200))
                    } else {
                        after
                    };
                    self.kernel.schedule_after(
                        after,
                        Ev::Daemon {
                            inc: self.inc,
                            event: Event::Timer(timer),
                        },
                    );
                }
                Effect::StorePut { io, key, bytes } => {
                    let now = self.kernel.now();
                    let (at, result) = self.store.put(now, self.kernel.rng(), &key, bytes);
                    let result = map_put(result, &mut self.report);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            inc: self.inc,
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
                    let result = map_put(result, &mut self.report);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            inc: self.inc,
                            event: Event::StorePutDone { io, result },
                        },
                    );
                }
                Effect::StoreGet { io, key } => {
                    let now = self.kernel.now();
                    let (at, result) = self.store.get(now, self.kernel.rng(), &key);
                    let result = map_get(result, &mut self.report);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            inc: self.inc,
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
                    let result = map_get(result, &mut self.report);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            inc: self.inc,
                            event: Event::StoreGetDone { io, result },
                        },
                    );
                }
                Effect::StoreDelete { key } => {
                    let now = self.kernel.now();
                    let _ = self.store.delete(now, self.kernel.rng(), &key);
                }
                Effect::VsetFenced { vset } => {
                    // The node manager kills the fenced vset's guest (R6.4);
                    // for backed-up vsets the control plane restores from
                    // the store (R6.1).
                    if let Some(guest) = self.guests.get_mut(&vset) {
                        guest.state = GuestState::Dead;
                    }
                    if self.vset_config_for(vset).backed_up {
                        let req = self.req();
                        self.admin_reqs.insert(req, AdminKind::Restore(vset));
                        self.step_daemon(Event::Admin(AdminCmd::RestoreVset { req, vset }));
                    }
                }
                Effect::Admin(reply) => self.admin_reply(reply),
                Effect::PeerSend { .. } => {
                    self.report
                        .violations
                        .push("peer send on a single host".to_string());
                }
                Effect::Abort { reason } => {
                    self.report
                        .violations
                        .push(format!("daemon aborted: {reason}"));
                    self.crash();
                }
            }
        }
    }

    fn dispatch(&mut self, event: Ev) {
        match event {
            Ev::Daemon { inc, event } => {
                if inc == self.inc {
                    self.step_daemon(event);
                }
            }
            Ev::BdevWriteDone { inc, bdev_io, io } => {
                if inc == self.inc {
                    self.bdev.complete_write(bdev_io);
                    self.step_daemon(Event::BlobWriteDone { io });
                }
            }
            Ev::BdevReadDone { inc, io, bytes } => {
                if inc == self.inc {
                    self.step_daemon(Event::BlobReadDone { io, bytes });
                }
            }
            Ev::GuestStep { vset } => self.guest_step(vset),
            Ev::CheckpointTick { vset } => {
                if self.daemon.is_some() && self.guests.contains_key(&vset) {
                    let req = self.req();
                    self.admin_reqs.insert(req, AdminKind::Checkpoint(vset));
                    self.step_daemon(Event::Admin(AdminCmd::Checkpoint { req, vset }));
                }
                if let Some(interval) = self.config.checkpoint_interval
                    && self.within_horizon()
                {
                    let at = self.next_after(interval);
                    self.kernel.schedule_at(at, Ev::CheckpointTick { vset });
                }
            }
            Ev::CrashDaemon => {
                self.crash();
                if self.within_horizon() && self.config.faults.crash_mean_interval > 0 {
                    let at = self.next_after(self.config.faults.crash_mean_interval);
                    self.kernel.schedule_at(at, Ev::CrashDaemon);
                }
            }
            Ev::RestartDaemon => self.restart(),
            Ev::JournalBitflip => {
                let flipped = self.bdev.flip_random_bit_where(self.kernel.rng(), |name| {
                    matches!(layout::parse_blob(name), Some(BlobName::Journal { .. }))
                });
                if let Some(name) = flipped
                    && let Some(BlobName::Journal { vset, .. }) = layout::parse_blob(&name)
                {
                    self.poisoned.insert(vset);
                }
                if self.within_horizon() && self.config.faults.journal_bitflip_mean_interval > 0 {
                    let at = self.next_after(self.config.faults.journal_bitflip_mean_interval);
                    self.kernel.schedule_at(at, Ev::JournalBitflip);
                }
            }
            Ev::RotNewestRecord(mirror) => {
                // The newest record by (fence, seq), in the chosen copy —
                // the adversarial target: it alone carries its newly-acked
                // syncs, so its loss is the rollback hazard.
                let target = self
                    .bdev
                    .scan()
                    .filter_map(|(name, _)| {
                        let parsed = layout::parse_blob(name)?;
                        let BlobName::Journal { fence, seq, .. } = parsed else {
                            return None;
                        };
                        let is_mirror = std::path::Path::new(name)
                            .extension()
                            .is_some_and(|e| e.eq_ignore_ascii_case("recm"));
                        (is_mirror == mirror).then(|| (fence, seq, name.clone()))
                    })
                    .max();
                if let Some((_, _, name)) = target {
                    self.bdev
                        .flip_random_bit_where(self.kernel.rng(), |n| n == name);
                }
            }
            Ev::StoreOutage(out) => self.store.set_outage(out),
            Ev::Bitflip => {
                let flipped = self.bdev.flip_random_bit_where(self.kernel.rng(), |name| {
                    matches!(layout::parse_blob(name), Some(BlobName::Segment { .. }))
                });
                if let Some(name) = flipped
                    && let Some(BlobName::Segment { vset, .. }) = layout::parse_blob(&name)
                {
                    self.poisoned.insert(vset);
                }
                if self.within_horizon() && self.config.faults.bitflip_mean_interval > 0 {
                    let at = self.next_after(self.config.faults.bitflip_mean_interval);
                    self.kernel.schedule_at(at, Ev::Bitflip);
                }
            }
        }
    }

    // ── guests ──────────────────────────────────────────────────────────

    fn schedule_guest(&mut self, vset: VsetId) {
        let (lo, hi) = self.config.think;
        let delay = self.kernel.rng().range(lo, hi);
        self.kernel.schedule_after(delay, Ev::GuestStep { vset });
    }

    fn guest_step(&mut self, vset: VsetId) {
        if self.daemon.is_none() {
            return;
        }
        let Harness {
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
                self.step_daemon(Event::GuestSync { req, volume });
            }
            Ok(op) => self.attempt_op(vset, op),
        }
    }

    /// Try a memory operation against the mapping; fault if it traps.
    fn attempt_op(&mut self, vset: VsetId, op: PendingOp) {
        let mem = self.mems.entry(vset).or_default();
        let (page, write) = match op {
            PendingOp::Write { page, .. } => (page, true),
            PendingOp::Read { page } | PendingOp::Fsck { page } => (page, false),
        };
        let resident = mem.pages.contains_key(&page);
        let trapped = !resident || (write && mem.protected.contains(&page));
        if trapped {
            self.guests.get_mut(&vset).expect("guest exists").state = GuestState::Faulted { op };
            self.step_daemon(Event::GuestFault { page, write });
            return;
        }
        self.complete_op(vset, op);
    }

    /// The access proceeds in plain memory: apply it and finish the op.
    fn complete_op(&mut self, vset: VsetId, op: PendingOp) {
        // Every retired op sets the page's accessed bit — the ground truth
        // MGLRU aging harvests (R2.6).
        let (PendingOp::Write { page, .. } | PendingOp::Read { page } | PendingOp::Fsck { page }) =
            op;
        if let Some(mem) = self.mems.get(&vset) {
            mem.accessed.borrow_mut().insert(page);
        }
        if let Some(filter) = std::env::var_os("BLOCKD_SIM_TRACE_PAGE") {
            let text = format!("{op:?}");
            if text.contains(filter.to_string_lossy().as_ref()) {
                eprintln!("[t={:?}] complete {text}", self.kernel.now());
            }
        }
        match op {
            PendingOp::Write { page, vol_seq } => {
                let mem = self.mems.get_mut(&vset).expect("mapped");
                assert!(!mem.protected.contains(&page), "write to protected page");
                mem.pages.insert(page, page_pattern(page, vol_seq));
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.applied += 1;
                let op_index = guest.applied;
                self.oracle.on_write_ok(page, vol_seq, op_index);
            }
            PendingOp::Read { .. } => {
                // Resident content is either the guest's own writes or a
                // validated fill: nothing to check, nothing to tell.
                self.guests.get_mut(&vset).expect("guest exists").applied += 1;
            }
            PendingOp::Fsck { page } => {
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.applied += 1;
                let done = guest.fsck.is_empty();
                let cold = guest.cold_booting;
                let _ = page;
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
        if self.within_horizon() || fsck_pending {
            self.schedule_guest(vset);
        }
    }

    /// A fill resolved a missing fault: install the bytes, validate them
    /// (fills are the only door storage bytes enter through — R8.1), and
    /// let the blocked operation proceed.
    fn fill(&mut self, page: PageId, bytes: Vec<u8>, writable: bool) {
        let vset = page.volume.vset;
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
            let mem = self.mems.get_mut(&vset).expect("mapped");
            mem.pages.insert(page, bytes);
            mem.protected.insert(page);
            return;
        };
        let cold_fsck = matches!(op, PendingOp::Fsck { .. }) && guest.cold_booting;
        self.oracle.check_fill(page, &bytes, cold_fsck);
        let mem = self.mems.get_mut(&vset).expect("mapped");
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
        if !writable && matches!(op, PendingOp::Write { .. }) {
            // An unsolicited fill (prefetch pre-population) landed
            // write-protected under this waiting writer. Real uffd resolves
            // the missing fault, the write retries, and traps again as a WP
            // fault — model exactly that; the op retires via `Unprotect`.
            self.refaults.push(page);
            return;
        }
        guest.state = GuestState::Idle;
        self.complete_op(vset, op);
    }

    /// The write-protect fault resolved: the blocked write proceeds.
    fn resolve_write(&mut self, page: PageId) {
        let vset = page.volume.vset;
        let Some(guest) = self.guests.get_mut(&vset) else {
            return;
        };
        let GuestState::Faulted { op } = guest.state else {
            // A capture may unprotect-resolve spuriously; nothing waits.
            return;
        };
        guest.state = GuestState::Idle;
        self.complete_op(vset, op);
    }

    fn fill_failed(&mut self, page: PageId) {
        let vset = page.volume.vset;
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        let GuestState::Faulted { op } = guest.state else {
            self.report
                .violations
                .push(format!("fill failure for non-faulted guest: {page:?}"));
            return;
        };
        let sanctioned = self.poisoned.contains(&vset);
        self.oracle.on_fill_failed(page, sanctioned);
        if matches!(op, PendingOp::Fsck { .. }) && guest.cold_booting {
            self.oracle.on_fsck_aborted(vset);
        }
        self.guests.get_mut(&vset).expect("guest exists").state = GuestState::Dead;
        self.report.guest_deaths += 1;
    }

    fn sync_done(&mut self, req: ReqId, ok: bool) {
        let Some(vset) = self.sync_reqs.remove(&req) else {
            self.report
                .violations
                .push(format!("sync reply for unknown request {req:?}"));
            return;
        };
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        let GuestState::Syncing {
            req: waiting,
            volume,
        } = guest.state
        else {
            self.report
                .violations
                .push(format!("sync reply for non-syncing guest of {vset:?}"));
            return;
        };
        assert_eq!(waiting, req, "sequential guest got a foreign sync reply");
        if !ok {
            self.report
                .violations
                .push(format!("unexpected sync rejection for {vset:?}"));
            guest.state = GuestState::Dead;
            self.report.guest_deaths += 1;
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
        if self.within_horizon() {
            self.schedule_guest(vset);
        }
    }

    /// Retire whatever completed while the vCPU was paused.
    fn unpark(&mut self, vset: VsetId) {
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        match guest.state {
            GuestState::Parked { op } => {
                // The resumed vCPU retries the instruction: re-attempt, so
                // a write whose page re-protected meanwhile traps again
                // instead of completing into a protected page.
                guest.state = GuestState::Idle;
                self.attempt_op(vset, op);
            }
            GuestState::SyncParked { volume } => {
                guest.applied += 1;
                guest.state = GuestState::Idle;
                guest.completed += 1;
                self.oracle.on_sync_ok(volume);
                if self.within_horizon() {
                    self.schedule_guest(vset);
                }
            }
            _ => self.schedule_guest(vset),
        }
    }

    // ── admin ───────────────────────────────────────────────────────────

    fn admin_reply(&mut self, reply: AdminReply) {
        match reply {
            AdminReply::VsetCreated { req, vset } => {
                let Some(AdminKind::Create(expected)) = self.admin_reqs.remove(&req) else {
                    self.report
                        .violations
                        .push(format!("unexpected creation reply {req:?}"));
                    return;
                };
                assert_eq!(vset, expected);
                let config = self.vset_config_for(vset);
                self.oracle.register(vset, config);
                self.mems.insert(vset, VsetMem::default());
                let mut guest = Guest::new(vset, config);
                guest.sync_share = self.config.guest_sync_share;
                guest.hot_pages = self.config.guest_hot_pages;
                self.guests.insert(vset, guest);
                self.schedule_guest(vset);
                if let Some(interval) = self.config.checkpoint_interval {
                    let at = self.next_after(interval);
                    self.kernel.schedule_at(at, Ev::CheckpointTick { vset });
                }
            }
            AdminReply::CheckpointDone { req, .. } => {
                self.admin_reqs.remove(&req);
            }
            // Lineage and migration replies are exercised by the dedicated
            // suites (single hosts have no peers).
            AdminReply::BaseKept { .. }
            | AdminReply::BaseDeleted { .. }
            | AdminReply::VsetForked { .. }
            | AdminReply::MigratedOut { .. }
            | AdminReply::VsetMigratedIn { .. } => {}
            AdminReply::VsetRecovered { vset, verdict } => {
                self.mems.insert(vset, VsetMem::default());
                match verdict {
                    Verdict::Resume { vmstate, .. } => {
                        self.report.resumes += 1;
                        self.oracle.on_resume(vset, vmstate);
                        let infer = self.oracle.needs_disk_inference(vset);
                        let guest = self.guests.get_mut(&vset).expect("guest exists");
                        guest.reborn(vmstate, infer);
                    }
                    Verdict::ColdBoot | Verdict::Unrestorable => {
                        self.report.cold_boots += 1;
                        self.oracle.start_cold_boot(vset);
                        let guest = self.guests.get_mut(&vset).expect("guest exists");
                        guest.reborn(0, true);
                    }
                }
                self.schedule_guest(vset);
            }
            AdminReply::VsetRestored { req, vset, verdict } => {
                self.admin_reqs.remove(&req);
                self.report.restores += 1;
                self.mems.insert(vset, VsetMem::default());
                // Restores may legitimately land behind acked syncs: the
                // loss bound on host death is the backup lag (R4.3).
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
            AdminReply::AdminFailed { req } => {
                // Checkpoints racing a crash are re-tried by the tick chain;
                // anything else is a harness bug.
                match self.admin_reqs.remove(&req) {
                    Some(AdminKind::Checkpoint(_)) => {}
                    other => self
                        .report
                        .violations
                        .push(format!("unexpected admin failure {req:?} ({other:?})")),
                }
            }
        }
    }

    // ── crash & recovery ────────────────────────────────────────────────

    fn crash(&mut self) {
        let Some(daemon) = self.daemon.take() else {
            return;
        };
        self.last_counters = daemon.counters;
        self.inc += 1;
        self.report.crashes += 1;
        self.bdev.crash(self.kernel.rng());
        // Daemon death kills the guests and their mappings (R8.2).
        self.mems.clear();
        self.sync_reqs.clear();
        self.admin_reqs.clear();
        self.pause_started.clear();
        let (lo, hi) = self.config.faults.restart_delay;
        let delay = self.kernel.rng().range(lo, hi);
        self.kernel.schedule_after(delay, Ev::RestartDaemon);
    }

    fn restart(&mut self) {
        if self.daemon.is_some() {
            return;
        }
        let scan: Vec<(String, Vec<u8>)> = self
            .bdev
            .scan()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();
        let (daemon, verdicts, effects) = Daemon::recover(
            self.config.daemon.clone(),
            scan.iter().map(|(n, b)| (n.as_str(), b.as_slice())),
        );
        self.daemon = Some(daemon);
        self.apply_effects(effects);

        if std::env::var_os("BLOCKD_SIM_DEBUG").is_some() {
            eprintln!("[restart t={:?}] verdicts: {verdicts:?}", self.kernel.now());
        }
        let vsets: Vec<VsetId> = self.guests.keys().copied().collect();
        for vset in vsets {
            self.mems.insert(vset, VsetMem::default());
            match verdicts.get(&vset) {
                Some(Verdict::Resume { vmstate, .. }) => {
                    self.report.resumes += 1;
                    self.oracle.on_resume(vset, *vmstate);
                    // If an earlier cold-boot fsck was interrupted, the disk
                    // ghost is still unresolved: this boot's verification
                    // pass must re-infer disk state rather than trust it.
                    let infer = self.oracle.needs_disk_inference(vset);
                    let guest = self.guests.get_mut(&vset).expect("listed");
                    guest.reborn(*vmstate, infer);
                    self.schedule_guest(vset);
                }
                Some(Verdict::ColdBoot) => {
                    self.report.cold_boots += 1;
                    self.oracle.start_cold_boot(vset);
                    let guest = self.guests.get_mut(&vset).expect("listed");
                    guest.reborn(0, true);
                    self.schedule_guest(vset);
                }
                None if self.vset_config_for(vset).backed_up => {
                    // Recovery defers the verdict until the head confirms
                    // ownership; the guest waits for `VsetRecovered` (or a
                    // fence followed by a restore).
                }
                Some(Verdict::Unrestorable) | None => {
                    self.report.unrestorable += 1;
                    if !self.poisoned.contains(&vset) {
                        self.report
                            .violations
                            .push(format!("{vset:?} unrestorable without injected damage"));
                    }
                    self.guests.get_mut(&vset).expect("listed").state = GuestState::Dead;
                    // Backed-up vsets come back from the store (R6.1): the
                    // control plane requests a restore.
                    if self.vset_config_for(vset).backed_up {
                        let req = self.req();
                        self.admin_reqs.insert(req, AdminKind::Restore(vset));
                        self.step_daemon(Event::Admin(AdminCmd::RestoreVset { req, vset }));
                    }
                }
            }
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_put(result: Result<Version, StoreError>, report: &mut RunReport) -> Result<u64, StoreFault> {
    match result {
        Ok(version) => Ok(version.0),
        Err(StoreError::Unavailable) => Err(StoreFault::Unavailable),
        Err(StoreError::CasConflict { actual }) => Err(StoreFault::CasConflict {
            actual: actual.map(|v| v.0),
        }),
        Err(StoreError::TooLarge) => {
            // The daemon must never exceed the 64 MiB contract (R4.6).
            report
                .violations
                .push("R4.6: daemon wrote an oversized object".to_owned());
            Err(StoreFault::Unavailable)
        }
    }
}

#[allow(clippy::type_complexity)]
fn map_get(
    result: Result<Option<(Version, Vec<u8>)>, StoreError>,
    report: &mut RunReport,
) -> Result<Option<(u64, Vec<u8>)>, StoreFault> {
    match result {
        Ok(found) => Ok(found.map(|(v, b)| (v.0, b))),
        Err(StoreError::Unavailable) => Err(StoreFault::Unavailable),
        Err(StoreError::CasConflict { actual }) => Err(StoreFault::CasConflict {
            actual: actual.map(|v| v.0),
        }),
        Err(StoreError::TooLarge) => {
            report
                .violations
                .push("R4.6: oversized object on a read path".to_owned());
            Err(StoreFault::Unavailable)
        }
    }
}
