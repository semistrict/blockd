//! FNV-1a 64-bit, implemented locally so trace hashes are pinned by this repo.

use std::fmt::{self, Write as _};

const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug)]
pub struct Fnv64(u64);

impl Fnv64 {
    pub fn new() -> Fnv64 {
        Fnv64(OFFSET)
    }

    pub fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    pub fn finish(self) -> u64 {
        self.0
    }
}

impl Default for Fnv64 {
    fn default() -> Fnv64 {
        Fnv64::new()
    }
}

impl fmt::Write for Fnv64 {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.write(s.as_bytes());
        Ok(())
    }
}

/// Running hash of everything a simulation run observed, in order. Two runs
/// are byte-for-byte identical iff their trace hashes match at every step.
#[derive(Clone, Copy, Debug)]
pub struct TraceHasher {
    hash: Fnv64,
    records: u64,
}

impl TraceHasher {
    pub fn new() -> TraceHasher {
        TraceHasher {
            hash: Fnv64::new(),
            records: 0,
        }
    }

    /// Fold one record into the trace. The record's `Debug` rendering is the
    /// canonical encoding — derived, total, and deterministic.
    pub fn record(&mut self, record: &dyn fmt::Debug) {
        write!(self.hash, "{record:?}\x1f").expect("Fnv64 write is infallible");
        self.records += 1;
    }

    pub fn finish(&self) -> u64 {
        self.hash.finish()
    }

    pub fn records(&self) -> u64 {
        self.records
    }
}

impl Default for TraceHasher {
    fn default() -> TraceHasher {
        TraceHasher::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv64_is_pinned() {
        let mut h = Fnv64::new();
        h.write(b"blockd");
        assert_eq!(h.finish(), 0x63ab_f79b_9221_c332);
    }

    #[test]
    fn fnv64_empty_is_offset_basis() {
        assert_eq!(Fnv64::new().finish(), 0xcbf2_9ce4_8422_2325);
    }
}
