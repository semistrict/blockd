//! Per-host monotonic clocks (R6.4): every timing bound in the system may
//! rest only on these. Each clock has its own random epoch — two hosts'
//! readings are never comparable as absolute values, so any algorithm that
//! compares clock readings across hosts breaks in simulation — plus a fixed
//! bounded drift rate.

use blockd_core::types::SimTime;

use crate::rng::Pcg64;

/// A host boots with its monotonic clock at an arbitrary point (up to ~30
/// days in), so absolute readings carry no cross-host meaning.
const MAX_EPOCH_OFFSET: u64 = 30 * 24 * 3600 * 1_000_000_000;

#[derive(Clone, Copy, Debug)]
pub struct HostClock {
    offset: u64,
    drift_ppm: i64,
}

impl HostClock {
    pub fn new(rng: &mut Pcg64, max_drift_ppm: u32) -> HostClock {
        let span = 2 * u64::from(max_drift_ppm) + 1;
        let drift_ppm = i64::try_from(rng.below(span)).unwrap() - i64::from(max_drift_ppm);
        HostClock {
            offset: rng.below(MAX_EPOCH_OFFSET),
            drift_ppm,
        }
    }

    /// Zero offset, zero drift — unit-test convenience only.
    pub fn exact() -> HostClock {
        HostClock {
            offset: 0,
            drift_ppm: 0,
        }
    }

    /// This host's reading of the given instant, in its own nanoseconds.
    /// Linear in sim time, hence monotonic.
    pub fn read(&self, now: SimTime) -> u64 {
        let elapsed = i128::from(now.nanos());
        let skew = elapsed * i128::from(self.drift_ppm) / 1_000_000;
        self.offset + u64::try_from(elapsed + skew).expect("drift bounded well below 100%")
    }

    pub fn drift_ppm(&self) -> i64 {
        self.drift_ppm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockd_core::types::millis;

    #[test]
    fn clocks_are_monotonic_with_bounded_drift_but_unshared_epochs() {
        let mut rng = Pcg64::new(0x000b_10cd, 0);
        let a = HostClock::new(&mut rng, 500);
        let b = HostClock::new(&mut rng, 500);
        // Absolute readings mean nothing across hosts (R6.4): the epochs
        // differ by minutes-to-days, dwarfing any elapsed time in a test.
        let gap = a.read(SimTime::ZERO).abs_diff(b.read(SimTime::ZERO));
        assert_eq!(gap, 1_969_866_798_145_243);

        for clock in [a, b] {
            assert!(clock.drift_ppm().abs() <= 500);
            let base = clock.read(SimTime::ZERO);
            let mut last = base;
            for step in 1..=100u64 {
                let t = SimTime(step * millis(10));
                let reading = clock.read(t);
                assert!(reading >= last, "clock went backwards");
                let elapsed_here = reading - base;
                let skew = i128::from(elapsed_here) - i128::from(t.nanos());
                let bound = i128::from(t.nanos()) * 500 / 1_000_000 + 1;
                assert!(skew.abs() <= bound, "skew {skew} beyond bound {bound}");
                last = reading;
            }
        }
    }

    #[test]
    fn exact_clock_reads_sim_time() {
        let clock = HostClock::exact();
        assert_eq!(clock.read(SimTime(12_345)), 12_345);
    }
}
