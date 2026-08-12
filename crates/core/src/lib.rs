//! blockd-core: deterministic async protocol actors and pinned data formats.
//!
//! Everything here is sans-IO and deterministic. Actors await explicit world
//! interfaces and run under the blockd executor; no host clock, thread, or
//! external I/O is reachable from this crate.

pub mod authority;
pub mod cache;
pub mod database;
pub mod dbproto;
pub mod engine;
pub mod format;
pub mod gc;
pub mod head;
pub mod hostmeta;
pub mod journal;
pub mod layout;
pub mod mapleaf;
pub mod peer;
pub mod placement;
pub mod protocol;
pub mod replica_recovery;
pub mod replica_spool;
mod replica_wire;
pub mod segment;
pub mod types;
pub mod vnode_member;
pub mod vsetfs;
pub mod world;
