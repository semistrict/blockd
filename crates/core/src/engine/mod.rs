//! Deterministic async protocol actors.

mod backup;
mod capture;
mod database;
mod fault;
mod host;
mod hydration;
mod lineage;
mod migration;
mod reclaim;
mod recovery;
mod replica;
mod restore;
mod state;

pub use backup::{create_backed, publish_latest, reconcile_backed_recovery};
pub use capture::{capture_local, capture_migration, checkpoint_local, create_fresh_local};
pub use database::{
    attach_database, begin_detach_database, database_source, finish_detach_database,
};
pub use fault::serve_fault;
pub use host::{host_actor, host_actor_with_state};
pub use hydration::hydrate_mapping;
pub use lineage::{create_fork, delete_base, keep_base};
pub use migration::{
    hydrate_tail, migrate_out, peer_fetch_leaf, peer_fetch_page, peer_source, reoffer_outbound,
};
pub use reclaim::cleanup_local;
pub use recovery::recover_local;
pub use replica::{
    create_peer_stashed, publish_replica_head, replica_message, replicate_latest,
    retry_replica_releases,
};
pub use restore::restore_vset;
pub use state::{HostState, SharedHost, VsetState};
