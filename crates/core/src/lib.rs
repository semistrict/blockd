//! blockd-core: deterministic async protocol actors and pinned data formats.
//!
//! Everything here is sans-IO and deterministic. Actors await explicit world
//! interfaces and run as Tokio tasks; no host clock, thread, or
//! external I/O is reachable from this crate.

pub mod authority;
pub mod blx;
pub mod cache;
pub mod engine;
pub mod format;
pub mod gc;
pub mod head;
pub mod hostmeta;
pub mod journal;
pub mod layout;
pub mod manifest;
pub mod page_file;
pub mod peer;
pub mod placement;
pub mod protocol;
pub mod replica_recovery;
pub mod replica_spool;
mod replica_wire;
pub mod types;
pub mod vnode_member;
pub mod world;
