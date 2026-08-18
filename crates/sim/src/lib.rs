//! blockd-sim: deterministic simulation of the blockd distributed core.
//!
//! Turmoil and its paused Tokio runtimes own the clock and event queues in a
//! run (R10.1); seeded actor scopes own domain fault choices and randomness;
//! world components model the network, disks, object store and hosts with
//! injectable faults; the oracle checks the requirements' invariants against
//! ghost truth. A run is `(seed, config) → trace hash`, byte-for-byte
//! replayable.

mod checkpoint_schedule;
pub mod cluster;
pub mod guest;
pub mod harness;
pub mod model;
pub mod peer_transport;
pub mod presets;
pub mod scenario;
pub(crate) mod world;

pub use blockd_exec::rng;
pub use blockd_exec::trace as hash;
