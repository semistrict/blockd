//! Where the event loop's time goes. The loop thread is the daemon's one
//! core: every guest fault on this host waits behind whatever it is doing.
//! These counters attribute its wall time three ways — deciding (inside
//! `Daemon::step`, keyed by event kind), executing effects (keyed by
//! effect kind — this is where on-loop byte-work like `Fill` copies and
//! `BlobWrite` fsyncs show up), and idle (blocked on the event channel).
//!
//! Recording happens only on the loop thread; reads may come from
//! anywhere, so cells are relaxed atomics. Costs two `Instant::now()`
//! calls per event and two per effect — noise next to a syscall, cheap
//! enough to leave on always.

use std::sync::atomic::{AtomicU64, Ordering};

use blockd_core::seam::{Effect, Event};

pub(crate) const EVENT_KINDS: [&str; 11] = [
    "GuestFault",
    "GuestSync",
    "GuestPaused",
    "PeerDelivered",
    "Admin",
    "BlobWriteDone",
    "BlobReadDone",
    "StorePutDone",
    "StoreGetDone",
    "Timer",
    "Database",
];

pub(crate) fn event_kind(event: &Event) -> usize {
    match event {
        Event::GuestFault { .. } => 0,
        Event::GuestSync { .. } => 1,
        Event::GuestPaused { .. } => 2,
        Event::PeerDelivered { .. } | Event::ReplicaPutPrepared { .. } => 3,
        Event::Admin(_) => 4,
        Event::BlobWriteDone { .. } | Event::ReplicaDeleteFailed { .. } => 5,
        Event::BlobReadDone { .. } => 6,
        Event::StorePutDone { .. } => 7,
        Event::StoreGetDone { .. } => 8,
        Event::Timer(_) => 9,
        Event::Database(_) => 10,
    }
}

pub(crate) const EFFECT_KINDS: [&str; 30] = [
    "Fill",
    "FillShared",
    "FillFailed",
    "Unprotect",
    "WriteProtect",
    "Evict",
    "PauseGuest",
    "ResumeGuest",
    "SyncOk",
    "SyncFailed",
    "BlobWrite",
    "ReplicaAppend",
    "ReplicaDelete",
    "ReplicaTruncate",
    "BlobRead",
    "BlobReadRange",
    "BlobDelete",
    "SetTimer",
    "StorePut",
    "StoreCas",
    "StoreGet",
    "StoreGetRange",
    "StoreDelete",
    "VsetFenced",
    "Admin",
    "PeerSend",
    "Abort",
    "DatabaseInstall",
    "Database",
    "VsetUnservable",
];

pub(crate) fn effect_kind(effect: &Effect) -> usize {
    match effect {
        Effect::Fill { .. } => 0,
        Effect::FillShared { .. } => 1,
        Effect::FillFailed { .. } => 2,
        Effect::Unprotect { .. } => 3,
        Effect::WriteProtect { .. } => 4,
        Effect::Evict { .. } => 5,
        Effect::PauseGuest { .. } => 6,
        Effect::ResumeGuest { .. } => 7,
        Effect::SyncOk { .. } => 8,
        Effect::SyncFailed { .. } => 9,
        Effect::BlobWrite { .. } => 10,
        Effect::ReplicaAppend { .. } => 11,
        Effect::ReplicaDelete { .. } => 12,
        Effect::ReplicaTruncate { .. } => 13,
        Effect::BlobRead { .. } => 14,
        Effect::BlobReadRange { .. } => 15,
        Effect::BlobDelete { .. } => 16,
        Effect::SetTimer { .. } => 17,
        Effect::StorePut { .. } => 18,
        Effect::StoreCas { .. } => 19,
        Effect::StoreGet { .. } => 20,
        Effect::StoreGetRange { .. } => 21,
        Effect::StoreDelete { .. } => 22,
        Effect::VsetFenced { .. } => 23,
        Effect::Admin(_) => 24,
        Effect::PeerSend { .. } => 25,
        Effect::Abort { .. } => 26,
        Effect::DatabaseInstall { .. } => 27,
        Effect::Database(_) => 28,
        Effect::VsetUnservable { .. } => 29,
    }
}

#[derive(Default)]
struct Cell {
    count: AtomicU64,
    ns: AtomicU64,
}

impl Cell {
    fn add(&self, ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.ns.fetch_add(ns, Ordering::Relaxed);
    }

    fn read(&self) -> (u64, u64) {
        (
            self.count.load(Ordering::Relaxed),
            self.ns.load(Ordering::Relaxed),
        )
    }
}

#[derive(Default)]
pub struct LoopStats {
    decide: [Cell; EVENT_KINDS.len()],
    effect: [Cell; EFFECT_KINDS.len()],
    idle_ns: AtomicU64,
}

impl LoopStats {
    pub(crate) fn record_decide(&self, kind: usize, ns: u64) {
        self.decide[kind].add(ns);
    }

    pub(crate) fn record_effect(&self, kind: usize, ns: u64) {
        self.effect[kind].add(ns);
    }

    pub(crate) fn record_idle(&self, ns: u64) {
        self.idle_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// (name, count, total ns) per event kind, decide time only.
    pub fn decide_totals(&self) -> Vec<(&'static str, u64, u64)> {
        EVENT_KINDS
            .iter()
            .zip(&self.decide)
            .map(|(name, cell)| {
                let (count, ns) = cell.read();
                (*name, count, ns)
            })
            .collect()
    }

    /// (name, count, total ns) per effect kind, execution time on the loop.
    pub fn effect_totals(&self) -> Vec<(&'static str, u64, u64)> {
        EFFECT_KINDS
            .iter()
            .zip(&self.effect)
            .map(|(name, cell)| {
                let (count, ns) = cell.read();
                (*name, count, ns)
            })
            .collect()
    }

    /// Loop time spent deciding plus executing effects.
    pub fn busy_ns(&self) -> u64 {
        let cells = self.decide.iter().chain(&self.effect);
        cells.map(|cell| cell.ns.load(Ordering::Relaxed)).sum()
    }

    /// Loop time spent blocked waiting for an event.
    pub fn idle_ns(&self) -> u64 {
        self.idle_ns.load(Ordering::Relaxed)
    }

    /// Busy fraction of the loop's observed lifetime, in [0, 1].
    #[allow(clippy::cast_precision_loss)] // presentation math
    pub fn occupancy(&self) -> f64 {
        let busy = self.busy_ns();
        let total = busy + self.idle_ns();
        if total == 0 {
            return 0.0;
        }
        busy as f64 / total as f64
    }

    /// Human-readable attribution table, nonzero rows only, sorted by
    /// total time within each section.
    #[allow(clippy::cast_precision_loss)] // presentation math
    pub fn report(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let busy = self.busy_ns().max(1);
        let mut section = |title: &str, rows: Vec<(&'static str, u64, u64)>| {
            let mut rows: Vec<_> = rows
                .into_iter()
                .filter(|(_, count, _)| *count > 0)
                .collect();
            rows.sort_by_key(|(_, _, ns)| std::cmp::Reverse(*ns));
            let _ = writeln!(out, "  {title}");
            for (name, count, ns) in rows {
                let _ = writeln!(
                    out,
                    "    {name:<14} {count:>9} × {:>8.1}µs = {:>8.1}ms ({:>4.1}% of busy)",
                    ns as f64 / count as f64 / 1_000.0,
                    ns as f64 / 1_000_000.0,
                    ns as f64 * 100.0 / busy as f64,
                );
            }
        };
        section(
            "decide (Daemon::step, by event kind):",
            self.decide_totals(),
        );
        section("effects (on-loop execution):", self.effect_totals());
        let _ = writeln!(
            out,
            "  occupancy {:.1}% (busy {:.1}ms, idle {:.1}ms)",
            self.occupancy() * 100.0,
            self.busy_ns() as f64 / 1_000_000.0,
            self.idle_ns() as f64 / 1_000_000.0,
        );
        out
    }
}
