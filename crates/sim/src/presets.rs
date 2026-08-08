//! Compatibility names for the committed simulation seed corpora.
//!
//! The checked-in scenario documents are the single source of truth. These
//! wrappers preserve the existing Rust API used by regression tests while the
//! sweep runner can realize the same specifications directly by name and seed.

use crate::cluster::ClusterConfig;
use crate::harness::HarnessConfig;
use crate::scenario::{self, RealizedScenario};

fn single_host(name: &str) -> HarnessConfig {
    match scenario::load(name)
        .unwrap_or_else(|error| panic!("invalid checked-in scenario {name}: {error}"))
        .realize(0)
        .unwrap_or_else(|error| panic!("cannot realize checked-in scenario {name}: {error}"))
    {
        RealizedScenario::SingleHost(config) => config,
        RealizedScenario::Cluster(_) => panic!("scenario {name} is not single-host"),
    }
}

fn cluster(name: &str) -> ClusterConfig {
    match scenario::load(name)
        .unwrap_or_else(|error| panic!("invalid checked-in scenario {name}: {error}"))
        .realize(0)
        .unwrap_or_else(|error| panic!("cannot realize checked-in scenario {name}: {error}"))
    {
        RealizedScenario::Cluster(config) => config,
        RealizedScenario::SingleHost(_) => panic!("scenario {name} is not a cluster"),
    }
}

pub fn single_host_base() -> HarnessConfig {
    single_host("single-host-base")
}

pub fn single_host_chaos() -> HarnessConfig {
    single_host("chaos")
}

pub fn cluster_kill_race() -> ClusterConfig {
    cluster("cluster")
}

pub fn migration_chaos() -> ClusterConfig {
    cluster("migration")
}

pub fn peer_stash_chaos() -> ClusterConfig {
    cluster("peer-stash")
}

pub fn peer_attrition() -> ClusterConfig {
    cluster("peer-attrition")
}

pub fn swizzle_peer_links() -> ClusterConfig {
    cluster("peer-links")
}

pub fn peer_stash_rare() -> ClusterConfig {
    cluster("peer-rare")
}

pub fn placement_fear() -> ClusterConfig {
    cluster("placement-fear")
}
