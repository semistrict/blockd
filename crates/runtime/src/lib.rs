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
#[cfg(target_os = "linux")]
pub mod database;
pub mod directory_store;
pub mod fakegcs;
#[cfg(target_os = "linux")]
pub mod fc;
mod gcs;
#[cfg(target_os = "linux")]
mod loopstats;
mod metrics;
mod peer;
mod replica_recovery;
#[cfg(target_os = "linux")]
mod s3;
mod store;
#[cfg(all(target_os = "linux", feature = "vsetfs"))]
pub mod vsetfs;
pub mod world;

#[cfg(target_os = "linux")]
pub use actor_host::{Runtime, RuntimeConfig};
pub use blobscan::scan_blob_dir;
pub use capacity::{
    CapacityController, CapacityInputs, CapacityReason, CapacitySignal, CapacityState,
};
pub use gcs::{GcsConfig, GcsStats, GcsStore};
#[cfg(target_os = "linux")]
pub use loopstats::LoopStats;
pub use metrics::{
    AtomicHistogram, FaultLatency, HistogramSnapshot, LATENCY_BUCKETS_NS, LatencySeries,
};
pub use peer::{PeerConfig, PeerNet, PeerTlsConfig};
pub use replica_recovery::{
    InstallReplicaRecoveryError, InstalledReplicaRecovery, install_replica_recovery,
};
#[cfg(target_os = "linux")]
pub use s3::{ListObjectsV2Output, S3Error, S3LatencyModel, S3Sim, S3Stats, S3Store};
pub use store::{GetResult, ObjectStore};
