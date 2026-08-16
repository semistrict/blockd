//! blockd-sim: deterministic simulation of the blockd distributed core.
//!
//! Turmoil and its paused Tokio runtimes own the clock and event queues in a
//! run (R10.1); seeded actor scopes own domain fault choices and randomness;
//! world components model the network, disks, object store and hosts with
//! injectable faults; the oracle checks the requirements' invariants against
//! ghost truth. A run is `(seed, config) → trace hash`, byte-for-byte
//! replayable.

pub mod actor_cluster;
pub mod actor_harness;
pub(crate) mod actor_world;
mod checkpoint_schedule;
pub use actor_cluster as cluster;
pub mod guest;
pub use actor_harness as harness;
pub mod model;
pub mod peer_transport;
pub mod presets;
pub mod scenario;

pub use blockd_exec::rng;
pub use blockd_exec::trace as hash;
