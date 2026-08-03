//! PCG XSL RR 128/64 — the classic pcg64 generator, implemented locally so
//! the byte stream is pinned by this repo, not by a dependency's version.
//! Reference: O'Neill, "PCG: A Family of Simple Fast Space-Efficient
//! Statistically Good Algorithms for Random Number Generation" (public domain
//! reference implementation).
//!
//! Deliberately float-free: probabilities are expressed in parts per million.

const MULTIPLIER: u128 = 0x2360_ed05_1fc6_5da4_4385_df64_9fcc_cf45;

/// Probability in parts per million. `Ppm(1_000_000)` always hits.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Ppm(pub u32);

impl Ppm {
    pub const NEVER: Ppm = Ppm(0);
    pub const ALWAYS: Ppm = Ppm(1_000_000);

    /// `Ppm` from a percentage.
    pub const fn percent(p: u32) -> Ppm {
        assert!(p <= 100);
        Ppm(p * 10_000)
    }
}

#[derive(Clone, Debug)]
pub struct Pcg64 {
    state: u128,
    inc: u128,
}

impl Pcg64 {
    pub fn new(seed: u64, stream: u64) -> Pcg64 {
        let mut rng = Pcg64 {
            state: 0,
            inc: (u128::from(stream) << 1) | 1,
        };
        rng.step();
        rng.state = rng.state.wrapping_add(u128::from(seed));
        rng.step();
        rng
    }

    fn step(&mut self) {
        self.state = self.state.wrapping_mul(MULTIPLIER).wrapping_add(self.inc);
    }

    // The truncating casts are the XSL RR output function itself.
    #[allow(clippy::cast_possible_truncation)]
    pub fn next_u64(&mut self) -> u64 {
        self.step();
        let xored = ((self.state >> 64) as u64) ^ (self.state as u64);
        let rot = (self.state >> 122) as u32;
        xored.rotate_right(rot)
    }

    /// Uniform value in `[0, bound)`. Modulo bias is irrelevant at sim scales
    /// and the simpler draw keeps the stream easy to reason about.
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "below(0)");
        self.next_u64() % bound
    }

    /// Uniform value in `[lo, hi]` (inclusive).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "range({lo}, {hi})");
        lo + self.below(hi - lo + 1)
    }

    /// True with the given probability.
    pub fn hit(&mut self, p: Ppm) -> bool {
        self.below(1_000_000) < u64::from(p.0)
    }

    /// Pick a uniformly random element of a non-empty slice.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        assert!(!items.is_empty(), "pick from empty slice");
        &items[usize::try_from(self.below(items.len() as u64)).unwrap()]
    }

    /// Derive an independent generator (e.g. one per world component) so a
    /// draw in one component never perturbs another component's stream.
    #[must_use]
    pub fn fork(&mut self, stream: u64) -> Pcg64 {
        Pcg64::new(self.next_u64(), stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_is_pinned() {
        // If this stream changes, every recorded regression seed in the repo
        // silently means a different run — so it must fail loudly.
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
        let mut a = Pcg64::new(7, 1);
        let mut b = Pcg64::new(7, 2);
        let sa: Vec<u64> = (0..4).map(|_| a.next_u64()).collect();
        let sb: Vec<u64> = (0..4).map(|_| b.next_u64()).collect();
        assert_ne!(sa, sb);
    }

    #[test]
    fn hit_frequency_matches_probability() {
        let mut rng = Pcg64::new(1, 0);
        let hits = (0..10_000).filter(|_| rng.hit(Ppm::percent(50))).count();
        assert_eq!(hits, 5029);
    }

    #[test]
    fn hit_extremes() {
        let mut rng = Pcg64::new(1, 0);
        assert!(!rng.hit(Ppm::NEVER));
        assert!(rng.hit(Ppm::ALWAYS));
    }

    #[test]
    fn range_is_inclusive_and_bounded() {
        let mut rng = Pcg64::new(3, 0);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..1_000 {
            seen.insert(rng.range(10, 13));
        }
        assert_eq!(seen.into_iter().collect::<Vec<_>>(), [10, 11, 12, 13]);
    }
}
