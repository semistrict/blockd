//! Shared-workload differential gate against the real Linux runtime.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "support/workload.rs"]
mod runtime_workload;

use std::sync::Arc;

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::types::{HostId, VsetId, micros, millis};
use blockd_runtime::{Runtime, RuntimeConfig, S3Store};
use blockd_workload::{load, run};
use runtime_workload::{RuntimeDataBackend, RuntimeLifecycleBackend};

#[test]
fn checked_in_steady_io_matches_the_real_runtime() {
    let directory = tempfile::tempdir().expect("temporary blob directory");
    let runtime = Runtime::new(
        &RuntimeConfig {
            daemon: DaemonConfig {
                host: HostId(0),
                cache_pages: 256,
                writeback_interval: millis(5),
                backup_retry: millis(20),
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 500,
                replica_placement: None,
            },
            blob_dir: directory.path().to_owned(),
            peer: None,
        },
        Arc::new(S3Store::new()),
    );
    let spec = load("steady-io").expect("checked-in workload");
    let mut backend = RuntimeDataBackend::new(&runtime, VsetId(1));
    let outcome = run(&spec, &mut backend).expect("real runtime workload");

    assert_eq!(outcome.completed, 206);
    assert_eq!(outcome.syncs, 4);
    assert_eq!(outcome.verifications, 1);
    assert_eq!(backend.metrics().writes, outcome.writes);
    assert_eq!(backend.metrics().reads, outcome.reads);
    assert_eq!(backend.metrics().syncs, outcome.syncs);
    assert!(backend.metrics().write_time > std::time::Duration::ZERO);
}

#[test]
fn checked_in_checkpoint_recovery_matches_deterministic_simulation() {
    let spec = load("checkpoint-recovery").expect("checked-in workload");
    let mut sim_config = blockd_sim::presets::single_host_base();
    sim_config.vset_count = 1;
    sim_config.backed_vsets = u16::from(spec.shape.backed_up);
    sim_config.vset_config = VsetConfig::compute(
        spec.shape.disk_volumes,
        spec.shape.pages_per_volume,
        spec.shape.backed_up,
    );
    sim_config.horizon = millis(10_000);
    sim_config.think = (micros(1), micros(2));
    sim_config.checkpoint_interval = None;
    sim_config.faults = blockd_sim::harness::FaultPlan::none();
    sim_config.rot_records_at.clear();
    sim_config.crash_at.clear();
    let simulated = blockd_sim::harness::run_workload(0x53_53, sim_config, spec.clone())
        .expect("simulated workload");

    let directory = tempfile::tempdir().expect("temporary blob directory");
    let store = Arc::new(S3Store::new());
    let config = RuntimeConfig {
        daemon: DaemonConfig {
            host: HostId(0),
            cache_pages: 256,
            writeback_interval: millis(5),
            backup_retry: millis(20),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 500,
            replica_placement: None,
        },
        blob_dir: directory.path().to_owned(),
        peer: None,
    };
    let mut backend = RuntimeLifecycleBackend::new(config, store, VsetId(1));
    let actual = run(&spec, &mut backend).expect("real runtime workload");

    assert_eq!(actual, simulated.workload);
    assert_eq!(backend.metrics().checkpoints, actual.checkpoints);
    assert_eq!(backend.metrics().crashes, actual.crashes);
    assert_eq!(backend.metrics().restores, actual.restores);
    assert_eq!(backend.runtime().incidents(), Vec::<String>::new());
}
