//! The object-store boundary of the runtime: exactly the five operations
//! the daemon's `Effect::Store*` arms need, all blocking (the effect
//! interpreter is synchronous by design). Versions are opaque u64s the
//! store itself derives — a version must survive process restarts and be
//! comparable across hosts, because the head CAS (R6.3) is the cluster's
//! single-writer authority.

use blockd_core::seam::StoreFault;

/// One get's outcome: `Ok(None)` means the key does not exist — a normal
/// answer, not a fault.
pub type GetResult = Result<Option<(u64, Vec<u8>)>, StoreFault>;

pub trait ObjectStore: Send + Sync + 'static {
    /// Unconditional put of a write-once key. Returns the stored version.
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<u64, StoreFault>;

    /// Conditional put (the head CAS, R6.3): `expected: None` = the key
    /// must not exist; `Some(v)` = it must currently be version `v`.
    /// A conflict carries the current version so fences can compare.
    fn put_cas(&self, key: &str, expected: Option<u64>, bytes: Vec<u8>) -> Result<u64, StoreFault>;

    fn get(&self, key: &str) -> GetResult;

    /// Ranged read of `len > 0` bytes at `offset` (the fault path, R2.3).
    /// A range starting past the object's end is `Ok(None)`; a short tail
    /// is returned as-is (frames above catch truncation).
    fn get_range(&self, key: &str, offset: u64, len: u64) -> GetResult;

    /// Fire-and-forget delete (R4.5 reclamation); idempotent.
    fn delete(&self, key: &str);
}
