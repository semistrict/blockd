//! Async boundary between protocol actors and their world.
//!
//! Actors depend only on these contracts. Simulation and production must
//! implement the same ordering, durability, damage, and outage semantics;
//! interpreter divergence belongs here or nowhere.
//!
//! # Contract
//!
//! Guest-memory operations are applied in call order per page. Capture MUST
//! arm write protection before reading a page, so a concurrent store either
//! lands in the captured bytes or traps after them. Installing a protected
//! fill under a blocked writer produces a second write-protect fault.
//!
//! Completed blob writes and appends are durable through power loss,
//! including directory entries. Reads return stored bytes verbatim, including
//! damage and short crash-torn tails. Deletes are durable and complete in
//! invocation order relative to other deletes; reclaim depends on records
//! disappearing before their handoff marker.
//!
//! Store operations may complete in any order. `Unavailable` means the
//! outcome is unknown and every caller must remain idempotent under retry.
//! Peers are at-least-once with drops and duplication; handlers authenticate
//! counterparties in the protocol and are idempotent.
//!
//! Dropping an in-flight method future cancels only the wait. A submitted I/O
//! may still land, which is the crash cut actors are required to tolerate.

// These statically dispatched actor-world traits intentionally return local,
// non-`Send` futures because one current-thread Tokio runtime owns the actor tree.
#![allow(async_fn_in_trait)]

use blockd_exec::Request;

use crate::engine::HostFatal;
use crate::protocol::{AdminCall, AdminEvent, AdminResult, PeerMsg, ReqId, StoreFault};
use crate::types::{HostId, PageId, VolumeId, VsetId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobError {
    /// The operation left durable storage exactly as it was before the call,
    /// so retrying the same write, append, or truncate is safe.
    Full,
    /// The device outcome is not trustworthy; the host must fail-stop.
    Io,
}

/// One durable local artifact discovered during recovery. Immutable segment
/// payloads may leave `bytes` empty while still reporting their exact length;
/// metadata-bearing artifacts provide their complete bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobEntry {
    pub name: String,
    pub bytes: Vec<u8>,
    pub len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    Fault(StoreFault),
    TooLarge,
}

impl From<StoreFault> for StoreError {
    fn from(fault: StoreFault) -> Self {
        Self::Fault(fault)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestFault {
    pub page: PageId,
    pub write: bool,
    /// The kernel trapped a write to an already-mapped write-protected page.
    pub wp: bool,
    /// The backing shmem page exists but is not mapped in this guest view.
    pub minor: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestSync {
    pub req: ReqId,
    pub volume: VolumeId,
}

pub type AdminRequest = Request<AdminCall, AdminResult>;
pub type GuestSyncRequest = Request<GuestSync, bool>;

pub trait Blobs {
    /// Return a canonicalizable snapshot of durable local artifacts. Unknown
    /// files may be omitted. Actors sort by name before interpreting it, so
    /// directory enumeration order cannot affect recovery.
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError>;
    async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError>;
    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError>;
    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError>;
    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError>;
    async fn read_range(
        &self,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BlobError>;
    async fn delete(&self, name: &str) -> Result<(), BlobError>;

    /// Delete in the supplied order and durably complete the entire prefix.
    async fn delete_many_durable(&self, names: &[String]) -> Result<(), BlobError> {
        for name in names {
            self.delete(name).await?;
        }
        Ok(())
    }
}

pub trait Store {
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError>;
    async fn put_cas(
        &self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreError>;
    async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError>;
    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError>;
    async fn delete(&self, key: &str) -> Result<bool, StoreError>;
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

pub trait Peers {
    /// Fire-and-forget. Delivery is at-least-once, not reliable or ordered.
    async fn send(&self, to: HostId, message: PeerMsg);
    async fn recv(&self) -> Option<(HostId, PeerMsg)>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FillSource {
    Zero,
    Local,
    Peer,
    Store,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuestMemoryError {
    Unavailable,
    Unservable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuestPause {
    pub vmstate: u64,
    /// Canonical VMM snapshot bytes captured at this pause.
    pub vmstate_bytes: Vec<u8>,
    pub generation: u64,
}

pub trait GuestMem {
    async fn read_page(&self, page: PageId) -> Vec<u8>;
    async fn arm_write_protect(&self, pages: &[PageId]) -> Result<(), GuestMemoryError>;
    async fn fill(
        &self,
        page: PageId,
        bytes: Vec<u8>,
        writable: bool,
        source: FillSource,
    ) -> Result<(), GuestMemoryError>;
    async fn fill_shared(
        &self,
        page: PageId,
        share: (u64, u64, crate::types::SegId, u32),
        bytes: Option<Vec<u8>>,
        writable: bool,
    ) -> Result<(), GuestMemoryError>;
    /// Continue a minor fault for a page whose shared backing is already
    /// resident. Production can remap it without copying; deterministic
    /// worlds use the equivalent read-and-fill default.
    async fn remap(&self, page: PageId, writable: bool) -> Result<(), GuestMemoryError> {
        let bytes = self.read_page(page).await;
        self.fill(page, bytes, writable, FillSource::Local).await
    }
    async fn fail(&self, page: PageId) -> Result<(), GuestMemoryError>;
    async fn unprotect(&self, page: PageId) -> Result<(), GuestMemoryError>;
    async fn evict(&self, page: PageId) -> Result<(), GuestMemoryError>;
    async fn install_vmstate(&self, vset: VsetId, bytes: Vec<u8>) -> Result<(), GuestMemoryError>;
    async fn pause(&self, vset: VsetId) -> Result<GuestPause, GuestMemoryError>;
    async fn resume(&self, vset: VsetId, pause: Option<GuestPause>)
    -> Result<(), GuestMemoryError>;
    /// Commit a paused guest as stopped on this host without resuming it.
    /// Migration uses this after its durable local cut and before any peer or
    /// object-store work.
    async fn commit_pause(&self, vset: VsetId, pause: GuestPause) -> Result<(), GuestMemoryError>;
    async fn harvest_accessed(&self) -> Vec<PageId>;
    async fn next_fault(&self) -> Option<GuestFault>;
    async fn next_sync(&self) -> Option<GuestSyncRequest>;
    async fn fence(&self, vset: VsetId) -> Result<(), GuestMemoryError>;
}

pub trait AdminIo {
    async fn next_admin(&self) -> Option<AdminRequest>;
    async fn emit_admin_event(&self, event: AdminEvent);
    async fn host_failed(&self, failure: HostFatal);
}
