//! blockd-runtime: the production-side host that runs the REAL daemon
//! state machine (`blockd_core::Daemon`) against REAL Linux machinery —
//! guest memory through `blockd-hostmem` (memfd + userfaultfd), local
//! blobs as files on real disk, and an object store spoken through an
//! accurate S3-shaped API.
//!
//! This is the other interpreter of the same `Effect` seam the
//! deterministic simulation interprets (R10.1): the daemon code is
//! byte-for-byte the code the simulation proved; only the world differs.
//! Threads, wall clocks, and blocking I/O live HERE — never in core.

// This crate is the nondeterministic side of the seam: threads and wall
// time are the implementation, not a hazard.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod blobscan;
#[cfg(target_os = "linux")]
pub mod database;
pub mod directory_store;
pub mod fakegcs;
#[cfg(target_os = "linux")]
pub mod fc;
mod gcs;
#[cfg(target_os = "linux")]
mod host;
#[cfg(target_os = "linux")]
mod loopstats;
mod metrics;
mod peer;
mod replica_recovery;
#[cfg(target_os = "linux")]
mod s3;
mod store;
#[cfg(target_os = "linux")]
pub mod vsetfs;

pub use blobscan::scan_blob_dir;
pub use gcs::{GcsConfig, GcsLatency, GcsStats, GcsStore};
#[cfg(target_os = "linux")]
pub use host::{
    FaultLatency, GuestPauseLatency, LocalIoLatency, Runtime, RuntimeConfig,
    RuntimeOperationLatency,
};
#[cfg(target_os = "linux")]
pub use loopstats::LoopStats;
pub use metrics::{AtomicHistogram, HistogramSnapshot, LATENCY_BUCKETS_NS};
pub use peer::{PeerConfig, PeerNet, PeerTlsConfig};
pub use replica_recovery::{
    InstallReplicaRecoveryError, InstalledReplicaRecovery, install_replica_recovery,
};
#[cfg(target_os = "linux")]
pub use s3::{ListObjectsV2Output, S3Error, S3LatencyModel, S3Sim, S3Stats, S3Store};
pub use store::{GetResult, ObjectStore};
