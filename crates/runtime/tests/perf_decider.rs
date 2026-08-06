//! The decider ceiling: how many events per second can ONE core of
//! `Daemon::step` sustain, and does per-event cost grow with vset count?
//!
//! The daemon is sans-IO, so it can be benchmarked bare: every effect is
//! answered synchronously from an in-memory world at zero cost — no
//! uffd, no disk, no store, no threads. What remains is pure decide
//! cost, the number that bounds how many sandboxes one event loop can
//! carry once byte-work moves off it. Runs on any OS.
//!
//! Profiles print to stderr (`--no-capture`); assertions pin the shape
//! (the paths actually exercised), not machine-dependent microseconds.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_precision_loss)] // presentation math

use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use blockd_core::daemon::{Daemon, DaemonConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::seam::{AdminCmd, AdminReply, Effect, Event, HostMap, ReqId, StoreFault, TimerId};
use blockd_core::types::{HostId, PAGE_SIZE, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};

/// Pages per vset volume — a small sandbox working set.
const PAGES_PER_VOLUME: u32 = 512;
/// Ops per scale point; enough for many writeback cycles.
const OPS: u64 = 400_000;
/// Virtual milliseconds advance every this many ops (drives timers).
const OPS_PER_MS: u64 = 512;

/// The daemon's window onto guest memory: all zeros. Capture reads pages
/// through this; content is irrelevant to decide cost.
struct ZeroMap;

impl HostMap for ZeroMap {
    fn read_page(&self, _page: PageId) -> Vec<u8> {
        vec![0u8; PAGE_SIZE]
    }
}

/// Step-time buckets, split finer than `Event`'s kinds where the decide
/// path differs (a miss fill is not a write-protect fault).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Bucket {
    FaultMiss,
    FaultWp,
    Timer,
    BlobDone,
    StoreDone,
    Admin,
    Other,
}

/// The zero-latency world: answers every effect inline, models page
/// residency so the workload faults exactly when a real guest would.
struct World {
    daemon: Daemon,
    now: u64,
    blobs: BTreeMap<String, Vec<u8>>,
    store: BTreeMap<String, (u64, Vec<u8>)>,
    timers: Vec<(u64, TimerId)>,
    /// Resident pages and whether they are writable (false = WP-armed).
    resident: BTreeMap<PageId, bool>,
    admin: Vec<AdminReply>,
    applied: u64,
    /// Per bucket: (count, total ns, max single-step ns).
    times: BTreeMap<Bucket, (u64, u64, u64)>,
}

impl World {
    fn new(config: DaemonConfig) -> World {
        let (daemon, boot) = Daemon::new(config);
        let mut world = World {
            daemon,
            now: 0,
            blobs: BTreeMap::new(),
            store: BTreeMap::new(),
            timers: Vec::new(),
            resident: BTreeMap::new(),
            admin: Vec::new(),
            applied: 0,
            times: BTreeMap::new(),
        };
        let mut local = VecDeque::new();
        for effect in boot {
            world.handle_effect(effect, &mut local);
        }
        while let Some(event) = local.pop_front() {
            world.step_untimed(event);
        }
        world
    }

    /// One timed `Daemon::step` plus its full completion cascade (the
    /// cascade's steps are timed too, under their own buckets).
    fn step(&mut self, event: Event, bucket: Bucket) {
        let mut local = VecDeque::new();
        let started = Instant::now();
        let effects = self.daemon.step(event, &ZeroMap);
        self.record(bucket, started.elapsed());
        for effect in effects {
            self.handle_effect(effect, &mut local);
        }
        while let Some(event) = local.pop_front() {
            let bucket = match event {
                Event::BlobWriteDone { .. } | Event::BlobReadDone { .. } => Bucket::BlobDone,
                Event::StorePutDone { .. } | Event::StoreGetDone { .. } => Bucket::StoreDone,
                _ => Bucket::Other,
            };
            let started = Instant::now();
            let effects = self.daemon.step(event, &ZeroMap);
            self.record(bucket, started.elapsed());
            for effect in effects {
                self.handle_effect(effect, &mut local);
            }
        }
    }

    fn step_untimed(&mut self, event: Event) {
        let mut local = VecDeque::new();
        let effects = self.daemon.step(event, &ZeroMap);
        for effect in effects {
            self.handle_effect(effect, &mut local);
        }
        while let Some(event) = local.pop_front() {
            let effects = self.daemon.step(event, &ZeroMap);
            for effect in effects {
                self.handle_effect(effect, &mut local);
            }
        }
    }

    fn record(&mut self, bucket: Bucket, elapsed: std::time::Duration) {
        let ns = u64::try_from(elapsed.as_nanos()).expect("fits");
        let cell = self.times.entry(bucket).or_insert((0, 0, 0));
        cell.0 += 1;
        cell.1 += ns;
        cell.2 = cell.2.max(ns);
    }

    #[allow(clippy::too_many_lines)] // one arm per Effect variant, exhaustive by design
    fn handle_effect(&mut self, effect: Effect, local: &mut VecDeque<Event>) {
        match effect {
            Effect::Fill { page, writable, .. } | Effect::FillShared { page, writable, .. } => {
                self.resident.insert(page, writable);
            }
            Effect::Unprotect { page } => {
                self.resident.insert(page, true);
            }
            Effect::WriteProtect { pages } => {
                for page in pages {
                    if let Some(writable) = self.resident.get_mut(&page) {
                        *writable = false;
                    }
                }
            }
            Effect::Evict { page } => {
                self.resident.remove(&page);
            }
            Effect::PauseGuest { vset } => {
                self.applied += 1;
                local.push_back(Event::GuestPaused {
                    vset,
                    vmstate: self.applied,
                });
            }
            Effect::ResumeGuest { .. } | Effect::SyncOk { .. } | Effect::SyncFailed { .. } => {}
            Effect::BlobWrite { io, name, bytes } => {
                self.blobs.insert(name, bytes);
                local.push_back(Event::BlobWriteDone { io });
            }
            Effect::BlobRead { io, name } => {
                local.push_back(Event::BlobReadDone {
                    io,
                    bytes: self.blobs.get(&name).cloned(),
                });
            }
            Effect::BlobReadRange {
                io,
                name,
                offset,
                len,
            } => {
                // Same contract as the runtime host: a short blob fails
                // the exact-range read.
                let bytes = self.blobs.get(&name).and_then(|blob| {
                    let start = usize::try_from(offset).expect("fits");
                    let end = start + usize::try_from(len).expect("fits");
                    blob.get(start..end).map(<[u8]>::to_vec)
                });
                local.push_back(Event::BlobReadDone { io, bytes });
            }
            Effect::BlobDelete { name } => {
                self.blobs.remove(&name);
            }
            Effect::SetTimer { timer, after } => {
                self.timers.push((self.now + after, timer));
            }
            Effect::StorePut { io, key, bytes } => {
                let version = self.store.get(&key).map_or(0, |(v, _)| *v) + 1;
                self.store.insert(key, (version, bytes));
                local.push_back(Event::StorePutDone {
                    io,
                    result: Ok(version),
                });
            }
            Effect::StoreCas {
                io,
                key,
                expected,
                bytes,
            } => {
                let actual = self.store.get(&key).map(|(v, _)| *v);
                let result = if actual == expected {
                    let version = actual.unwrap_or(0) + 1;
                    self.store.insert(key, (version, bytes));
                    Ok(version)
                } else {
                    Err(StoreFault::CasConflict { actual })
                };
                local.push_back(Event::StorePutDone { io, result });
            }
            Effect::StoreGet { io, key } => {
                local.push_back(Event::StoreGetDone {
                    io,
                    result: Ok(self.store.get(&key).cloned()),
                });
            }
            Effect::StoreGetRange {
                io,
                key,
                offset,
                len,
            } => {
                // S3 range semantics: a short object returns fewer bytes.
                let result = self.store.get(&key).map(|(version, bytes)| {
                    let start = usize::try_from(offset).expect("fits").min(bytes.len());
                    let end = (start + usize::try_from(len).expect("fits")).min(bytes.len());
                    (*version, bytes[start..end].to_vec())
                });
                local.push_back(Event::StoreGetDone {
                    io,
                    result: Ok(result),
                });
            }
            Effect::StoreDelete { key } => {
                self.store.remove(&key);
            }
            Effect::Admin(reply) => {
                self.admin.push(reply);
            }
            Effect::FillFailed { page } => panic!("unservable page {page:?}"),
            Effect::VsetFenced { vset } => panic!("unexpected fence of {vset:?}"),
            Effect::PeerSend { to, .. } => panic!("unexpected peer send to {to:?}"),
            Effect::Abort { reason } => panic!("daemon abort: {reason}"),
        }
    }

    /// Advance the virtual clock, firing every timer that comes due.
    fn advance(&mut self, by_ns: u64) {
        self.now += by_ns;
        loop {
            let due = self
                .timers
                .iter()
                .position(|&(fire_at, _)| fire_at <= self.now);
            let Some(index) = due else { break };
            let (_, timer) = self.timers.remove(index);
            self.step(Event::Timer(timer), Bucket::Timer);
        }
    }
}

/// Seeded LCG, same recipe the e2e workloads use.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

fn drive(vsets: u64) -> World {
    let config = VsetConfig {
        disk_volumes: 1,
        pages_per_volume: PAGES_PER_VOLUME,
        backed_up: false,
    };
    let mut world = World::new(DaemonConfig {
        host: HostId(0),
        cache_pages: 1 << 22, // never under pressure: measure decide, not eviction storms
        writeback_interval: millis(5),
        backup_retry: millis(20),
        disk_capacity: None,
        disk_headroom: 0,
    });
    for n in 0..vsets {
        world.step(
            Event::Admin(AdminCmd::CreateVset {
                req: ReqId(n + 1),
                vset: VsetId(n + 1),
                config,
                from_base: None,
            }),
            Bucket::Admin,
        );
    }
    let created = world
        .admin
        .iter()
        .filter(|reply| matches!(reply, AdminReply::VsetCreated { .. }))
        .count();
    assert_eq!(created, usize::try_from(vsets).expect("fits"));

    // The guest workload: uniform writes across every vset's disk volume.
    // First touches miss-fault; captures WP-arm; re-touches wp-fault.
    let mut lcg = Lcg(42);
    for op in 0..OPS {
        let page = PageId {
            volume: VolumeId {
                vset: VsetId(op % vsets + 1),
                idx: VolumeIdx(1),
            },
            page: PageNo(u32::try_from(lcg.next() % u64::from(PAGES_PER_VOLUME)).expect("fits")),
        };
        match world.resident.get(&page) {
            None => world.step(Event::GuestFault { page, write: true }, Bucket::FaultMiss),
            Some(false) => world.step(Event::GuestFault { page, write: true }, Bucket::FaultWp),
            Some(true) => {} // resident and writable: the guest writes for free
        }
        if (op + 1).is_multiple_of(OPS_PER_MS) {
            world.advance(millis(1));
        }
    }
    world
}

/// 2a-full's target number: ONE vset with a giant dirty set. Before the
/// incremental drain, the writeback capture read and compressed the whole
/// set inside a single step — seconds of stall every other sandbox's
/// fault waited behind. Now the arm step reads nothing, every drain
/// continuation reads one bounded batch, and the worst single step is the
/// SEAL (map metadata, O(delta)) — not O(dirty) page work.
#[test]
fn profile_huge_vset_capture_stall() {
    const HUGE_PAGES: u32 = 300_000;
    let mut world = World::new(DaemonConfig {
        host: HostId(0),
        cache_pages: 1 << 22,
        writeback_interval: millis(5),
        backup_retry: millis(20),
        disk_capacity: None,
        disk_headroom: 0,
    });
    world.step(
        Event::Admin(AdminCmd::CreateVset {
            req: ReqId(1),
            vset: VsetId(1),
            config: VsetConfig {
                disk_volumes: 1,
                pages_per_volume: HUGE_PAGES,
                backed_up: false,
            },
            from_base: None,
        }),
        Bucket::Admin,
    );
    // Dirty the entire volume without letting a writeback tick run: one
    // capture then owes all 300k pages at once.
    for n in 0..HUGE_PAGES {
        let page = PageId {
            volume: VolumeId {
                vset: VsetId(1),
                idx: VolumeIdx(1),
            },
            page: PageNo(n),
        };
        world.step(Event::GuestFault { page, write: true }, Bucket::FaultMiss);
    }
    world.advance(millis(20));

    let counters = world.daemon.counters;
    assert!(
        counters.pages_flushed >= u64::from(HUGE_PAGES),
        "the capture never flushed the dirty set: {}",
        counters.pages_flushed
    );
    let (timer_steps, _, worst_timer) = world.times[&Bucket::Timer];
    assert!(
        timer_steps > u64::from(HUGE_PAGES) / 64,
        "the capture did not drain in batches: {timer_steps} timer steps"
    );
    eprintln!(
        "── PROFILE: one {HUGE_PAGES}-dirty-page vset ── {timer_steps} drain/tick steps, \
         worst single step {:.1}ms",
        worst_timer as f64 / 1e6,
    );
    // Sanity ceiling only (machine-dependent): the pre-drain whole-set
    // read+compress cost seconds; batches plus the seal's metadata step
    // must stay well under that.
    assert!(
        worst_timer < 1_000_000_000,
        "worst step {worst_timer}ns — the drain failed to bound the stall"
    );
}

#[test]
fn profile_decider_event_ceiling() {
    eprintln!("── PROFILE: bare Daemon::step ceiling (zero-cost I/O, one thread) ──");
    for vsets in [1u64, 100, 300] {
        let started = Instant::now();
        let world = drive(vsets);
        let wall = started.elapsed();

        let (events, step_ns): (u64, u64) = world
            .times
            .values()
            .fold((0, 0), |(count, ns), &(c, n, _)| (count + c, ns + n));
        eprintln!(
            "  {vsets:>3} vsets: {events} events in {wall:.1?} wall; decide total {:.0}ms",
            step_ns as f64 / 1e6,
        );
        for (bucket, &(count, ns, max)) in &world.times {
            eprintln!(
                "      {:<10} {count:>7} × {:>7.2}µs   worst {:>9.1}µs",
                format!("{bucket:?}"),
                ns as f64 / count as f64 / 1e3,
                max as f64 / 1e3,
            );
        }
        let fault_count = world.times[&Bucket::FaultMiss].0 + world.times[&Bucket::FaultWp].0;
        let fault_ns = world.times[&Bucket::FaultMiss].1 + world.times[&Bucket::FaultWp].1;
        let faults_per_sec = fault_count as f64 / (fault_ns as f64 / 1e9);
        let worst_stall = world
            .times
            .values()
            .map(|&(_, _, max)| max)
            .max()
            .unwrap_or(0);
        eprintln!(
            "      fault decide alone: {faults_per_sec:.0}/s/core → ~{:.0} sandboxes/core \
             at 1000 faults/s each",
            faults_per_sec / 1000.0,
        );
        eprintln!(
            "      worst single decide stall: {:.1}ms (every sandbox's fault waits behind it)",
            worst_stall as f64 / 1e6,
        );

        // Shape: every path this profile claims to measure actually ran.
        let counters = world.daemon.counters;
        assert!(
            counters.zero_fills > 0,
            "no first-touch zero fills happened"
        );
        assert!(counters.wp_faults > 0, "no write-protect faults happened");
        assert!(counters.records_written > 0, "writeback never ran");
        assert!(counters.pages_flushed > 0, "capture never flushed a page");
        let faults = world.times[&Bucket::FaultMiss].0 + world.times[&Bucket::FaultWp].0;
        assert!(
            faults > OPS / 20,
            "workload barely faulted: {faults} of {OPS} ops"
        );
        // Sanity ceiling only — real regressions move the printed numbers.
        assert!(
            step_ns / events < 1_000_000,
            "mean decide cost exceeded 1ms/event"
        );
    }
}
