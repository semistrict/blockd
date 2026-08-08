#![forbid(unsafe_code)]

//! Deterministic current-thread async actors.
//!
//! The executor owns task ordering, virtual time, randomness, and fault
//! injection. Simulation has no external injector; production admits events
//! through the explicit two-lane injector while running the same futures.

pub mod channel;
pub mod fault;
pub mod inject;
pub mod rng;
pub mod select;
pub mod trace;

mod runtime;

pub use fault::{FaultConfig, FaultPoint};
pub use runtime::{
    Cancelled, Delay, Executor, Mode, TaskHandle, TaskId, WakeSource, delay, fault_point, now,
    observe, random_u64, yield_now,
};
pub use select::{Either, OneOf3, Select2, Select3, Timeout, select2, select3, timeout};
