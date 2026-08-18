//! blockd-runtime: the production host for the shared async protocol actors.
//!
//! The same actor tree used by deterministic simulation runs here against
//! Linux userfaultfd/memfd mappings, a durable blob directory, peers, and an
//! object-store adapter. Threads, wall clocks, and blocking I/O stay here.

// This crate owns the nondeterministic world boundary.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(target_os = "linux")]
mod actor_host;
mod blobscan;
mod capacity;
pub mod fakegcs;
#[cfg(target_os = "linux")]
pub mod fc;
mod gcs;
#[cfg(target_os = "linux")]
mod loopstats;
mod metrics;
mod peer;
mod replica_recovery;
mod store;
pub mod world;

#[cfg(target_os = "linux")]
pub use actor_host::{GuestAccess, GuestOperation, Runtime, RuntimeConfig};
pub use capacity::{
    CapacityController, CapacityInputs, CapacityReason, CapacitySignal, CapacityState,
};
pub use gcs::{GcsConfig, GcsStats, GcsStore};
#[cfg(target_os = "linux")]
pub use loopstats::LoopStats;
pub use metrics::{
    AtomicHistogram, FaultLatency, FaultReaderMetrics, FaultWorkMetrics, HistogramSnapshot,
    LATENCY_BUCKETS_NS, LatencySeries, TimingSeries,
};
pub use peer::{PeerConfig, PeerNet};
pub use replica_recovery::{
    InstallReplicaRecoveryError, InstalledReplicaRecovery, install_replica_recovery,
};
pub use store::{GetResult, ObjectStore};
