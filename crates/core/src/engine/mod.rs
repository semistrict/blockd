//! Deterministic async protocol actors.

mod authority;
mod backup;
mod blob;
mod capture;
mod error;
mod fault;
mod host;
mod keyed_queue;
mod lease;
mod lineage;
mod migration;
mod peer_client;
mod reclaim;
mod recovery;
pub(crate) mod recovery_policy;
mod replica;
mod restore;
mod state;
mod store_gc;
mod store_retry;
mod vnode_member;

pub use authority::{
    AuthorityError, PollSession, VersionedSession, activate_host_session, cas_placement,
    cas_vnode_authority, challenge_host_session, create_host_session, poll_or_defend_host_session,
    read_host_session, read_placement, read_vnode_authority, revoke_host_session,
    verify_authority_proof,
};
pub(crate) use backup::reconcile_backed_recovery_event;
pub use backup::{publish_latest, reconcile_backed_recovery};
pub use capture::{capture_local, checkpoint_local};
pub use error::HostFatal;
pub use fault::serve_fault;
pub use host::{host_actor, host_actor_with_state};
pub use lineage::{create_fork, delete_base, keep_base};
pub use migration::{
    hydrate_tail, migrate_out, peer_fetch_page, peer_fetch_replica_page, peer_source,
    reoffer_outbound,
};
pub use reclaim::{cleanup_local, reclaim_backed_segments};
pub use recovery::recover_local;
pub use replica::{create_peer_stashed, replica_message, replicate_latest, retry_replica_releases};
pub use restore::restore_vset;
pub use state::{HostState, SharedHost, VsetState};
pub use vnode_member::{
    adopt_vnode_generation, adopt_vnode_quorum, claim_vnode_authority, commit_active_vnode_quorum,
    commit_vnode_closure, failover_vnode, read_vnode_closure, read_vnode_member,
};
