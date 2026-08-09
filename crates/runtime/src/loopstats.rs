//! Where the actor executor's thread and production-world operations spend
//! wall time. Actor polls are the straight-line protocol work; world calls
//! are the async I/O boundary. Guest faults wait behind runnable actors, but
//! never behind the blocking I/O itself.
//!
//! Recording happens only on the loop thread; reads may come from
//! anywhere, so cells are relaxed atomics. Costs two `Instant::now()`
//! calls per event and two per effect — noise next to a syscall, cheap
//! enough to leave on always.

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const EVENT_KINDS: [&str; 1] = ["ActorPoll"];

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

pub(crate) mod world_kind {
    pub const FILL: usize = 0;
    pub const FILL_SHARED: usize = 1;
    pub const FILL_FAILED: usize = 2;
    pub const UNPROTECT: usize = 3;
    pub const WRITE_PROTECT: usize = 4;
    pub const EVICT: usize = 5;
    pub const PAUSE_GUEST: usize = 6;
    pub const RESUME_GUEST: usize = 7;
    pub const SYNC_OK: usize = 8;
    pub const SYNC_FAILED: usize = 9;
    pub const BLOB_WRITE: usize = 10;
    pub const REPLICA_APPEND: usize = 11;
    pub const REPLICA_DELETE: usize = 12;
    pub const REPLICA_TRUNCATE: usize = 13;
    pub const BLOB_READ: usize = 14;
    pub const BLOB_READ_RANGE: usize = 15;
    pub const BLOB_DELETE: usize = 16;
    pub const STORE_PUT: usize = 18;
    pub const STORE_CAS: usize = 19;
    pub const STORE_GET: usize = 20;
    pub const STORE_GET_RANGE: usize = 21;
    pub const STORE_DELETE: usize = 22;
    pub const VSET_FENCED: usize = 23;
    pub const ADMIN: usize = 24;
    pub const PEER_SEND: usize = 25;
    pub const ABORT: usize = 26;
    pub const DATABASE_INSTALL: usize = 27;
    pub const DATABASE: usize = 28;
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
    pub(crate) fn record_actor_poll(&self, ns: u64) {
        self.decide[0].add(ns);
    }

    pub(crate) fn record_world(&self, kind: usize, ns: u64) {
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
        section("actor polls:", self.decide_totals());
        section("production world operations:", self.effect_totals());
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
