//! blockd-core: seam types and (eventually) the daemon state machines.
//!
//! Everything here is sans-IO and deterministic: state machines receive
//! events plus explicit time, and return effects. Nothing in this crate may
//! touch a clock, a thread, an RNG, or any I/O.

pub mod cache;
pub mod daemon;
pub mod format;
pub mod gc;
pub mod head;
pub mod journal;
pub mod layout;
pub mod seam;
pub mod segment;
pub mod types;
