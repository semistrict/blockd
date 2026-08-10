//! Shared-workload differential gate against the real Linux runtime.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "support/workload.rs"]
mod runtime_workload;
mod support;

use std::path::PathBuf;
use std::sync::Arc;

use blockd_core::journal::VsetConfig;
use blockd_core::types::{VsetId, micros, millis};
use blockd_runtime::{Runtime, RuntimeConfig, S3Store};
use blockd_workload::{load, run};
use runtime_workload::{RuntimeDataBackend, RuntimeLifecycleBackend};

fn durable_runtime_configs(tag: &str) -> ([PathBuf; 3], [RuntimeConfig; 3], Arc<S3Store>) {
    let addresses = [
        support::free_addr(),
        support::free_addr(),
        support::free_addr(),
    ];
    let roots = std::array::from_fn(|host| support::temp_root(&format!("{tag}-{host}")));
    let configs = std::array::from_fn(|host| {
        support::three_host_runtime_config(
            u16::try_from(host).expect("host fits"),
            roots[host].clone(),
            addresses,
        )
    });
    (roots, configs, Arc::new(S3Store::new()))
}

#[test]
fn checked_in_steady_io_matches_the_real_runtime() {
    let (roots, configs, store) = durable_runtime_configs("workload-steady");
    let runtimes = configs
        .iter()
        .map(|config| Runtime::new(config, store.clone()))
        .collect::<Vec<_>>();
    let spec = load("steady-io").expect("checked-in workload");
    {
        let mut backend = RuntimeDataBackend::new(&runtimes[0], VsetId(1));
        let outcome = run(&spec, &mut backend).expect("real runtime workload");

        assert_eq!(outcome.completed, 206);
        assert_eq!(outcome.syncs, 4);
        assert_eq!(outcome.verifications, 1);
        assert_eq!(backend.metrics().writes, outcome.writes);
        assert_eq!(backend.metrics().reads, outcome.reads);
        assert_eq!(backend.metrics().syncs, outcome.syncs);
        assert!(backend.metrics().write_time > std::time::Duration::ZERO);
    }

    drop(runtimes);
    for root in roots {
        std::fs::remove_dir_all(root).expect("cleanup runtime root");
    }
}

#[test]
fn checked_in_checkpoint_recovery_matches_deterministic_simulation() {
    let spec = load("checkpoint-recovery").expect("checked-in workload");
    let mut sim_config = blockd_sim::presets::single_host_base();
    sim_config.vset_count = 1;
    sim_config.vset_config =
        VsetConfig::compute(spec.shape.disk_volumes, spec.shape.pages_per_volume);
    sim_config.horizon = millis(10_000);
    sim_config.think = (micros(1), micros(2));
    sim_config.checkpoint_interval = None;
    sim_config.faults = blockd_sim::harness::FaultPlan::none();
    sim_config.rot_records_at.clear();
    sim_config.crash_at.clear();
    let simulated = blockd_sim::harness::run_workload(0x53_53, sim_config, spec.clone())
        .expect("simulated workload");

    let (roots, [primary, passive_b, passive_c], store) =
        durable_runtime_configs("workload-recovery");
    let passives = vec![
        Runtime::new(&passive_b, store.clone()),
        Runtime::new(&passive_c, store.clone()),
    ];
    let mut backend = RuntimeLifecycleBackend::new(primary, store, passives, VsetId(1));
    let actual = run(&spec, &mut backend).expect("real runtime workload");

    assert_eq!(actual, simulated.workload);
    assert_eq!(backend.metrics().checkpoints, actual.checkpoints);
    assert_eq!(backend.metrics().crashes, actual.crashes);
    assert_eq!(backend.metrics().restores, actual.restores);
    assert_eq!(backend.runtime().incidents(), Vec::<String>::new());
    assert!(
        backend
            .passive_runtimes()
            .iter()
            .all(|runtime| runtime.incidents().is_empty())
    );

    drop(backend);
    for root in roots {
        std::fs::remove_dir_all(root).expect("cleanup runtime root");
    }
}
