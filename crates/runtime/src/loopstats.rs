//! Where the actor Tokio thread and production-world operations spend
//! wall time. Actor polls are the straight-line protocol work; world calls
//! are the async I/O boundary. Guest faults wait behind runnable actors, but
//! never behind the blocking I/O itself.
//!
//! Recording happens only on the loop thread; reads may come from
//! anywhere, so cells are relaxed atomics. Poll duration recording costs the
//! two runtime timestamps plus one timestamp here for inter-poll gaps; world
//! operations use two timestamps. The large-host profile measures this
//! instrumentation overhead before retaining performance conclusions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::metrics::{AtomicHistogram, HistogramSnapshot, detailed_profile_metrics_enabled};

pub(crate) const POLL_KINDS: [&str; 1] = ["ActorPoll"];

pub(crate) const WORLD_KINDS: [&str; 24] = [
    "Fill",
    "FillShared",
    "FillFailed",
    "Unprotect",
    "WriteProtect",
    "Evict",
    "PauseGuest",
    "ResumeGuest",
    "BlobWrite",
    "ReplicaAppend",
    "ReplicaDelete",
    "ReplicaTruncate",
    "BlobRead",
    "BlobReadRange",
    "BlobDelete",
    "StorePut",
    "StoreCas",
    "StoreGet",
    "StoreGetRange",
    "StoreDelete",
    "VsetFenced",
    "Admin",
    "PeerSend",
    "Abort",
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
    pub const BLOB_WRITE: usize = 8;
    pub const REPLICA_APPEND: usize = 9;
    pub const REPLICA_DELETE: usize = 10;
    pub const REPLICA_TRUNCATE: usize = 11;
    pub const BLOB_READ: usize = 12;
    pub const BLOB_READ_RANGE: usize = 13;
    pub const BLOB_DELETE: usize = 14;
    pub const STORE_PUT: usize = 15;
    pub const STORE_CAS: usize = 16;
    pub const STORE_GET: usize = 17;
    pub const STORE_GET_RANGE: usize = 18;
    pub const STORE_DELETE: usize = 19;
    pub const VSET_FENCED: usize = 20;
    pub const ADMIN: usize = 21;
    pub const PEER_SEND: usize = 22;
    pub const ABORT: usize = 23;
}

struct Cell {
    count: AtomicU64,
    ns: AtomicU64,
    timing: AtomicHistogram,
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            ns: AtomicU64::new(0),
            timing: AtomicHistogram::default(),
        }
    }
}

impl Cell {
    fn add(&self, ns: u64, detailed: bool) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.ns.fetch_add(ns, Ordering::Relaxed);
        if detailed {
            self.timing.observe_ns(ns);
        }
    }

    fn read(&self) -> (u64, u64) {
        (
            self.count.load(Ordering::Relaxed),
            self.ns.load(Ordering::Relaxed),
        )
    }

    fn snapshot(&self) -> HistogramSnapshot {
        self.timing.snapshot()
    }
}

pub struct LoopStats {
    poll: [Cell; POLL_KINDS.len()],
    world: [Cell; WORLD_KINDS.len()],
    poll_gap: AtomicHistogram,
    last_poll_end_ns: AtomicU64,
    detailed: bool,
    started: Instant,
}

impl Default for LoopStats {
    fn default() -> Self {
        Self {
            poll: std::array::from_fn(|_| Cell::default()),
            world: std::array::from_fn(|_| Cell::default()),
            poll_gap: AtomicHistogram::default(),
            last_poll_end_ns: AtomicU64::new(0),
            detailed: detailed_profile_metrics_enabled(),
            started: Instant::now(),
        }
    }
}

impl LoopStats {
    pub(crate) fn record_actor_poll(&self, ns: u64) {
        if self.detailed {
            let end_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let start_ns = end_ns.saturating_sub(ns);
            let previous_end = self.last_poll_end_ns.swap(end_ns, Ordering::Relaxed);
            if previous_end != 0 {
                self.poll_gap
                    .observe_ns(start_ns.saturating_sub(previous_end));
            }
        }
        self.poll[0].add(ns, self.detailed);
    }

    pub(crate) fn record_world(&self, kind: usize, ns: u64) {
        self.world[kind].add(ns, self.detailed);
    }

    /// (name, count, total ns) per actor-poll kind.
    pub fn poll_totals(&self) -> Vec<(&'static str, u64, u64)> {
        POLL_KINDS
            .iter()
            .zip(&self.poll)
            .map(|(name, cell)| {
                let (count, ns) = cell.read();
                (*name, count, ns)
            })
            .collect()
    }

    /// (name, count, total ns) per async world-operation kind.
    pub fn world_totals(&self) -> Vec<(&'static str, u64, u64)> {
        WORLD_KINDS
            .iter()
            .zip(&self.world)
            .map(|(name, cell)| {
                let (count, ns) = cell.read();
                (*name, count, ns)
            })
            .collect()
    }

    /// Cumulative duration distributions per actor-poll kind.
    pub fn poll_histograms(&self) -> Vec<(&'static str, HistogramSnapshot)> {
        POLL_KINDS
            .iter()
            .zip(&self.poll)
            .map(|(name, cell)| (*name, cell.snapshot()))
            .collect()
    }

    /// Cumulative duration distributions per async world-operation kind.
    pub fn world_histograms(&self) -> Vec<(&'static str, HistogramSnapshot)> {
        WORLD_KINDS
            .iter()
            .zip(&self.world)
            .map(|(name, cell)| (*name, cell.snapshot()))
            .collect()
    }

    /// Time for which no actor poll was running between consecutive polls.
    pub fn poll_gap_histogram(&self) -> HistogramSnapshot {
        self.poll_gap.snapshot()
    }

    /// Loop time spent polling actors plus completing world operations.
    pub fn busy_ns(&self) -> u64 {
        let cells = self.poll.iter().chain(&self.world);
        cells.map(|cell| cell.read().1).sum()
    }

    /// Wall time spent actively polling protocol actors.
    ///
    /// Unlike [`Self::busy_ns`], this excludes time awaited by world
    /// operations and is therefore the appropriate numerator for diagnosing
    /// saturation of the current-thread actor executor.
    pub fn actor_busy_ns(&self) -> u64 {
        self.poll.iter().map(|cell| cell.read().1).sum()
    }

    /// Observed lifetime not spent actively polling protocol actors.
    pub fn actor_idle_ns(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .saturating_sub(self.actor_busy_ns())
    }

    /// Poll-only actor-thread occupancy, in [0, 1].
    #[allow(clippy::cast_precision_loss)] // presentation math
    pub fn actor_occupancy(&self) -> f64 {
        let busy = self.actor_busy_ns();
        let total = busy.saturating_add(self.actor_idle_ns());
        if total == 0 {
            return 0.0;
        }
        busy as f64 / total as f64
    }

    /// Loop time spent blocked waiting for an event.
    pub fn idle_ns(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .saturating_sub(self.busy_ns())
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
        section("actor polls:", self.poll_totals());
        section("production world operations:", self.world_totals());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_include_poll_and_world_maxima() {
        let stats = LoopStats::default();
        stats.record_actor_poll(10_000);
        stats.record_actor_poll(25_000);
        stats.record_world(world_kind::FILL, 40_000);

        let poll = &stats.poll_histograms()[0].1;
        assert_eq!(poll.count, 2);
        assert_eq!(poll.sum_ns, 35_000);
        assert_eq!(poll.max_ns, 25_000);
        let fill = &stats.world_histograms()[world_kind::FILL].1;
        assert_eq!(fill.count, 1);
        assert_eq!(fill.max_ns, 40_000);
        assert_eq!(stats.poll_gap_histogram().count, 1);
        assert!((0.0..=1.0).contains(&stats.actor_occupancy()));
    }
}
