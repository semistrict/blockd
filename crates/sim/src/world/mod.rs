//! World components: deterministic models of everything outside the daemon —
//! network, local disks, the object store, per-host clocks. Each component is
//! a pure state machine: operations take `now` and an RNG, mutate internal
//! state, and return what should be scheduled; the harness owns the kernel
//! and does the scheduling. Fault injection lives here, driven by [`crate::nemesis`].

pub mod blobdev;
pub mod clock;
pub mod network;
pub mod store;
