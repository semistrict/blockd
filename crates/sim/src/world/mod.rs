//! World components: deterministic models of local disks, the object store,
//! and per-host clocks. Each component is a pure state machine: operations
//! take `now` and an RNG, mutate internal state, and return what should be
//! scheduled; each harness owns its network, fault plan, and kernel scheduling.

pub mod blobdev;
pub mod clock;
pub mod store;
