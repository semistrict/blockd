//! Lock-free latency accumulators shared by the runtime and its exporters.
//! The buckets cover the local-fault microsecond range through outage-scale
//! operations without making the runtime depend on a metrics backend.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use blockd_core::types::VolumeId;

#[cfg(target_os = "linux")]
pub(crate) fn detailed_profile_metrics_enabled() -> bool {
    std::env::var_os("BLOCKD_PROFILE_DETAILED_METRICS").is_none_or(|value| value != "0")
}

pub const LATENCY_BUCKETS_NS: [u64; 22] = [
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
    300_000_000_000,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistogramSnapshot {
    /// Cumulative counts, one for each [`LATENCY_BUCKETS_NS`] boundary.
    pub buckets: Vec<u64>,
    pub count: u64,
    pub sum_ns: u64,
    pub max_ns: u64,
}

impl HistogramSnapshot {
    /// The observations added after `earlier` was captured.
    ///
    /// Concurrent relaxed snapshots are not transactional, so subtraction is
    /// saturating. `max_ns` is the cumulative maximum rather than an interval
    /// maximum; callers that need an exact phase maximum must use a fresh
    /// process or an independently scoped accumulator.
    #[must_use]
    pub fn delta(&self, earlier: &Self) -> Self {
        Self {
            buckets: self
                .buckets
                .iter()
                .zip(&earlier.buckets)
                .map(|(current, previous)| current.saturating_sub(*previous))
                .collect(),
            count: self.count.saturating_sub(earlier.count),
            sum_ns: self.sum_ns.saturating_sub(earlier.sum_ns),
            max_ns: self.max_ns,
        }
    }

    /// Inclusive upper bound for the requested quantile.
    ///
    /// The fixed histogram cannot recover an exact sample value. Observations
    /// above the final bucket use the observed cumulative maximum as their
    /// bound.
    #[must_use]
    pub fn quantile_upper_ns(&self, numerator: u64, denominator: u64) -> Option<u64> {
        if self.count == 0 || numerator == 0 || denominator == 0 || numerator > denominator {
            return None;
        }
        let rank =
            (u128::from(self.count) * u128::from(numerator)).div_ceil(u128::from(denominator));
        let rank = u64::try_from(rank).unwrap_or(u64::MAX);
        self.buckets
            .iter()
            .zip(LATENCY_BUCKETS_NS)
            .find_map(|(count, upper)| (*count >= rank).then_some(upper))
            .or(Some(self.max_ns))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatencySeries {
    pub operation: &'static str,
    pub outcome: &'static str,
    pub histogram: HistogramSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultLatency {
    pub volume: VolumeId,
    pub source: &'static str,
    pub histogram: HistogramSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimingSeries {
    pub operation: &'static str,
    pub phase: &'static str,
    pub histogram: HistogramSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultWorkMetrics {
    pub queue_depth: u64,
    pub max_queue_depth: u64,
    pub oldest_queued_ns: u64,
    pub active: u64,
    pub max_active: u64,
    pub join_failures: u64,
    pub timing: Vec<TimingSeries>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultReaderMetrics {
    pub readers_started: u64,
    pub readers_exited: u64,
    pub events_read: u64,
    pub events_injected: u64,
    pub terminal_errors: u64,
    pub injection_failures: u64,
}

#[derive(Debug)]
pub struct AtomicHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_NS.len()],
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Default for AtomicHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }
}

impl AtomicHistogram {
    pub fn observe(&self, elapsed: Duration) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.observe_ns(ns);
    }

    pub fn observe_ns(&self, ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
        if let Some(index) = LATENCY_BUCKETS_NS.iter().position(|upper| ns <= *upper) {
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum_ns(&self) -> u64 {
        self.sum_ns.load(Ordering::Relaxed)
    }

    pub fn max_ns(&self) -> u64 {
        self.max_ns.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut cumulative = 0u64;
        HistogramSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| {
                    cumulative = cumulative.saturating_add(bucket.load(Ordering::Relaxed));
                    cumulative
                })
                .collect(),
            count: self.count(),
            sum_ns: self.sum_ns(),
            max_ns: self.max_ns(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let histogram = AtomicHistogram::default();
        histogram.observe(Duration::from_micros(40));
        histogram.observe(Duration::from_millis(3));
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.count, 2);
        assert_eq!(snapshot.buckets[0], 0);
        assert_eq!(snapshot.buckets[2], 1);
        assert_eq!(snapshot.buckets[8], 2);
        assert!(snapshot.sum_ns >= 3_040_000);
        assert_eq!(snapshot.max_ns, 3_000_000);
    }

    #[test]
    fn histogram_delta_is_saturating_and_preserves_cumulative_maximum() {
        let histogram = AtomicHistogram::default();
        histogram.observe(Duration::from_micros(40));
        let earlier = histogram.snapshot();
        histogram.observe(Duration::from_millis(3));
        let delta = histogram.snapshot().delta(&earlier);
        assert_eq!(delta.count, 1);
        assert_eq!(delta.sum_ns, 3_000_000);
        assert_eq!(delta.buckets[2], 0);
        assert_eq!(delta.buckets[8], 1);
        assert_eq!(delta.max_ns, 3_000_000);
    }

    #[test]
    fn histogram_quantiles_return_fixed_bucket_upper_bounds() {
        let histogram = AtomicHistogram::default();
        histogram.observe(Duration::from_micros(40));
        histogram.observe(Duration::from_millis(3));
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.quantile_upper_ns(50, 100), Some(50_000));
        assert_eq!(snapshot.quantile_upper_ns(99, 100), Some(5_000_000));
        assert_eq!(snapshot.quantile_upper_ns(0, 100), None);
    }
}
