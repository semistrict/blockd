//! Deterministic async protocol actors.

mod backup;
mod capture;
mod database;
mod error;
mod fault;
mod host;
mod hydration;
mod keyed_queue;
mod lineage;
mod migration;
mod peer_client;
mod reclaim;
mod recovery;
mod replica;
mod restore;
mod state;
mod store_gc;

pub(crate) use backup::reconcile_backed_recovery_event;
pub use backup::{create_backed, publish_latest, reconcile_backed_recovery};
pub use capture::{capture_local, checkpoint_local, create_fresh_local};
pub(crate) use database::database_source;
pub use database::{
    attach_database, begin_detach_database, drain_detached_database, finish_detach_database,
};
pub use error::HostFatal;
pub use fault::serve_fault;
pub use host::{host_actor, host_actor_with_state};
pub(crate) use hydration::hydrate_mapping;
pub use lineage::{create_fork, delete_base, keep_base};
pub use migration::{
    hydrate_tail, migrate_out, peer_fetch_leaf, peer_fetch_page, peer_source, reoffer_outbound,
};
pub use reclaim::{cleanup_local, reclaim_backed_segments};
pub use recovery::recover_local;
pub use replica::{
    advance_archive_age, archive_latest, archives_ready, create_peer_stashed, publish_replica_head,
    replica_message, replicate_latest, request_replica_archive, retry_archive_notices,
    retry_replica_releases,
};
pub use restore::restore_vset;
pub use state::{HostState, SharedHost, VsetState};
