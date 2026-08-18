//! Configuration and observability types for the actor-owned simulation world.

/// R4.6: the largest object the contract guarantees.
pub const MAX_OBJECT_BYTES: usize = 64 << 20;

#[derive(Clone, Copy, Debug)]
pub struct BlobDevConfig {
    pub read_latency_min: u64,
    pub read_latency_max: u64,
    pub write_latency_min: u64,
    pub write_latency_max: u64,
    pub ns_per_byte: u64,
    pub full_window: Option<(u64, u64)>,
    pub handoff_full_writes: u8,
    pub eio_at: Option<u64>,
}

impl BlobDevConfig {
    pub fn nvme() -> Self {
        Self {
            read_latency_min: 20_000,
            read_latency_max: 150_000,
            write_latency_min: 30_000,
            write_latency_max: 400_000,
            ns_per_byte: 1,
            full_window: None,
            handoff_full_writes: 0,
            eio_at: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CrashFate {
    Applied,
    Dropped,
    Torn { kept: usize },
}

#[derive(Clone, Copy, Debug)]
pub struct StoreConfig {
    pub latency_min: u64,
    pub latency_max: u64,
    pub ns_per_byte: u64,
}

impl StoreConfig {
    pub fn gcs() -> Self {
        Self {
            latency_min: 5_000_000,
            latency_max: 60_000_000,
            ns_per_byte: 5,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PutCounters {
    pub attempts: u64,
    pub successes: u64,
    pub attempted_bytes: u64,
    pub successful_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum StoreObjectKind {
    Head = 0,
    Manifest = 1,
    Blx = 2,
    Base = 5,
    Other = 6,
}

impl StoreObjectKind {
    pub const COUNT: usize = 7;
}

pub const OBJECT_SIZE_BUCKETS: usize = 7;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreCounters {
    pub gets: u64,
    pub puts: u64,
    pub deletes: u64,
    pub put_attempts: u64,
    pub put_successes: u64,
    pub cas_attempts: u64,
    pub cas_successes: u64,
    pub cas_conflicts: u64,
    pub unavailable: u64,
    pub too_large: u64,
    pub bytes_put: u64,
    pub unique_bytes: u64,
    pub retry_bytes: u64,
    pub bytes_got: u64,
    pub bitflips: u64,
    pub puts_by_kind: [PutCounters; StoreObjectKind::COUNT],
    pub object_size_histogram: [u64; OBJECT_SIZE_BUCKETS],
    pub logical_changed_bytes: u64,
}
