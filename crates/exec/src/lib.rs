#![forbid(unsafe_code)]

//! Tokio-backed current-thread async actor support.
//!
//! Tokio owns task scheduling, timers, waking, and cancellation. This crate
//! keeps the small domain-specific pieces shared by production and simulation:
//! typed request/reply channels, priority injection, seeded randomness, fault points,
//! and stable observations.

pub mod channel;
pub mod fault;
pub mod inject;
pub mod request;
pub mod rng;
pub mod select;
pub mod task_set;
pub mod trace;

mod runtime;

pub use fault::{FaultConfig, FaultPoint};
pub use request::{Reply, Request, Response, TryRecvError, request};
pub use runtime::{
    Cancelled, Delay, ProductionContext, SimulationContext, TaskHandle, TaskId, advance_to,
    current_poll, delay, fault_point, now, observe, random_between, random_hit, random_u64,
    run_ready, simulation_polls, simulation_scope, simulation_trace_hash, spawn, yield_now,
};
pub use select::{Either, OneOf3, Timeout, join2, select2, select3, timeout};
pub use task_set::TaskSet;
