//! The asynchronous object-store boundary of the runtime. Network operations
//! run on Tokio and their completions wake the shared actor executor. Versions are
//! opaque u64s the store itself derives — a version must survive process
//! restarts and be comparable across hosts, because the head CAS (R6.3) is the
//! cluster's single-writer authority.

use std::sync::Arc;

use async_trait::async_trait;
use blockd_core::protocol::StoreFault;

/// One get's outcome: `Ok(None)` means the key does not exist — a normal
/// answer, not a fault.
pub type GetResult = Result<Option<(u64, Vec<u8>)>, StoreFault>;

#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Unconditional put of a write-once key. Returns the stored version.
    async fn put(self: Arc<Self>, key: String, bytes: Vec<u8>) -> Result<u64, StoreFault>;

    /// Conditional put (the head CAS, R6.3): `expected: None` = the key
    /// must not exist; `Some(v)` = it must currently be version `v`.
    /// A conflict carries the current version so fences can compare.
    async fn put_cas(
        self: Arc<Self>,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreFault>;

    async fn get(self: Arc<Self>, key: String) -> GetResult;

    /// Ranged read of `len > 0` bytes at `offset` (the fault path, R2.3).
    /// A range starting past the object's end is `Ok(None)`; a short tail
    /// is returned as-is (frames above catch truncation).
    async fn get_range(self: Arc<Self>, key: String, offset: u64, len: u64) -> GetResult;

    /// Fire-and-forget delete (R4.5 reclamation); idempotent.
    async fn delete(self: Arc<Self>, key: String);

    /// Enumerate keys below a prefix for actor-driven GC. A backend that
    /// cannot provide a complete snapshot must fail closed: an empty
    /// successful result would make a collector mistake live objects for an
    /// empty namespace.
    async fn list_prefix(
        self: Arc<Self>,
        _prefix: String,
    ) -> Result<Vec<String>, StoreFault> {
        Err(StoreFault::Unavailable)
    }
}
