//! blockd-sim: deterministic simulation of the blockd distributed core.
//!
//! The kernel owns the only clock, RNG and event queue in a run (R10.1);
//! world components model the network, disks, object store and hosts with
//! injectable faults; the oracle checks the requirements' invariants against
//! ghost truth. A run is `(seed, config) → trace hash`, byte-for-byte
//! replayable.

pub mod actor_harness;
pub(crate) mod actor_world;
pub mod cluster;
pub mod guest;
pub mod harness;
pub mod kernel;
pub mod oracle;
pub mod presets;
pub mod scenario;
pub mod world;

pub use blockd_exec::rng;
pub use blockd_exec::trace as hash;
