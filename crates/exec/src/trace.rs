//! Pinned trace hashing used as the replay oracle.

use std::fmt::{self, Write as _};

const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug)]
pub struct Fnv64(u64);

impl Fnv64 {
    pub const fn new() -> Self {
        Self(OFFSET)
    }

    pub fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    pub const fn finish(self) -> u64 {
        self.0
    }
}

impl Default for Fnv64 {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Write for Fnv64 {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.write(value.as_bytes());
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TraceHasher {
    hash: Fnv64,
    records: u64,
}

impl TraceHasher {
    pub const fn new() -> Self {
        Self {
            hash: Fnv64::new(),
            records: 0,
        }
    }

    pub fn record(&mut self, record: &dyn fmt::Debug) {
        write!(self.hash, "{record:?}\x1f").expect("writing to FNV is infallible");
        self.records += 1;
    }

    pub const fn finish(&self) -> u64 {
        self.hash.finish()
    }

    pub const fn records(&self) -> u64 {
        self.records
    }
}

#[cfg(test)]
mod tests {
    use super::Fnv64;

    #[test]
    fn hash_is_pinned() {
        let mut hash = Fnv64::new();
        hash.write(b"blockd");
        assert_eq!(hash.finish(), 0x63ab_f79b_9221_c332);
    }
}
