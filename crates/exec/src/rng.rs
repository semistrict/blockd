//! Pinned PCG XSL RR 128/64 generator for simulated runs.

const MULTIPLIER: u128 = 0x2360_ed05_1fc6_5da4_4385_df64_9fcc_cf45;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Ppm(pub u32);

impl Ppm {
    pub const NEVER: Self = Self(0);
    pub const ALWAYS: Self = Self(1_000_000);

    pub const fn percent(value: u32) -> Self {
        assert!(value <= 100);
        Self(value * 10_000)
    }
}

#[derive(Clone, Debug)]
pub struct Pcg64 {
    state: u128,
    increment: u128,
}

impl Pcg64 {
    pub fn new(seed: u64, stream: u64) -> Self {
        let mut rng = Self {
            state: 0,
            increment: (u128::from(stream) << 1) | 1,
        };
        rng.step();
        rng.state = rng.state.wrapping_add(u128::from(seed));
        rng.step();
        rng
    }

    fn step(&mut self) {
        self.state = self
            .state
            .wrapping_mul(MULTIPLIER)
            .wrapping_add(self.increment);
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn next_u64(&mut self) -> u64 {
        self.step();
        let xored = ((self.state >> 64) as u64) ^ (self.state as u64);
        let rotation = (self.state >> 122) as u32;
        xored.rotate_right(rotation)
    }

    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "below(0)");
        self.next_u64() % bound
    }

    pub fn range(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "range({low}, {high})");
        low + self.below(high - low + 1)
    }

    pub fn hit(&mut self, probability: Ppm) -> bool {
        self.below(1_000_000) < u64::from(probability.0)
    }

    #[must_use]
    pub fn fork(&mut self, stream: u64) -> Self {
        Self::new(self.next_u64(), stream)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{Pcg64, Ppm};

    #[test]
    fn stream_is_pinned() {
        let mut rng = Pcg64::new(42, 54);
        let first: Vec<u64> = (0..6).map(|_| rng.next_u64()).collect();
        assert_eq!(
            first,
            [
                0x0817_df2d_87ef_e1b3,
                0xd627_9b58_04ff_4b8a,
                0x585b_a3d8_7944_a916,
                0x69c3_9583_e9d8_3283,
                0x3edc_3470_0bc9_58f7,
                0xed67_8f06_9353_1ac6,
            ]
        );
    }

    #[test]
    fn streams_are_independent() {
        let mut first = Pcg64::new(7, 1);
        let mut second = Pcg64::new(7, 2);
        let first_values: Vec<u64> = (0..4).map(|_| first.next_u64()).collect();
        let second_values: Vec<u64> = (0..4).map(|_| second.next_u64()).collect();
        assert_ne!(first_values, second_values);
    }

    #[test]
    fn probability_and_ranges_are_pinned() {
        let mut rng = Pcg64::new(1, 0);
        let hits = (0..10_000).filter(|_| rng.hit(Ppm::percent(50))).count();
        assert_eq!(hits, 5029);
        assert!(!rng.hit(Ppm::NEVER));
        assert!(rng.hit(Ppm::ALWAYS));

        let mut rng = Pcg64::new(3, 0);
        let mut seen = BTreeSet::new();
        for _ in 0..1_000 {
            seen.insert(rng.range(10, 13));
        }
        assert_eq!(seen.into_iter().collect::<Vec<_>>(), [10, 11, 12, 13]);
    }
}
