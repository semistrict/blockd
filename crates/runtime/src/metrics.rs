//! Lock-free latency accumulators shared by the runtime and its exporters.
//! The buckets cover the local-fault microsecond range through outage-scale
//! operations without making the runtime depend on a metrics backend.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
}

#[derive(Debug)]
pub struct AtomicHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_NS.len()],
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl Default for AtomicHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
        }
    }
}

impl AtomicHistogram {
    pub fn observe(&self, elapsed: Duration) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        for (upper, bucket) in LATENCY_BUCKETS_NS.iter().zip(&self.buckets) {
            if ns <= *upper {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
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
    }
}
