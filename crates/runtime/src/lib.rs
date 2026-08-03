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

#[cfg(target_os = "linux")]
pub mod fc;
#[cfg(target_os = "linux")]
mod host;
#[cfg(target_os = "linux")]
mod s3;

#[cfg(target_os = "linux")]
pub use host::{Runtime, RuntimeConfig};
#[cfg(target_os = "linux")]
pub use s3::{ListObjectsV2Output, S3Error, S3LatencyModel, S3Sim, S3Stats, S3Store};
