//! Shared-workload differential gate against the real Linux runtime.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "support/workload.rs"]
mod runtime_workload;
mod support;

use std::path::PathBuf;

use blockd_core::journal::VolumeConfig;
use blockd_core::types::{VolumeId, micros, millis};
use blockd_runtime::{Runtime, RuntimeConfig};
use blockd_workload::{load, run};
use runtime_workload::{RuntimeDataBackend, RuntimeLifecycleBackend};

async fn durable_runtime_configs(
    tag: &str,
) -> ([PathBuf; 3], [RuntimeConfig; 3], support::TestGcs) {
    let addresses = [
        support::free_addr(),
        support::free_addr(),
        support::free_addr(),
    ];
    let roots = std::array::from_fn(|host| support::temp_root(&format!("{tag}-{host}")));
    let configs = std::array::from_fn(|host| {
        support::three_host_runtime_config(
            u32::try_from(host).expect("host fits"),
            roots[host].clone(),
            addresses,
        )
    });
    (roots, configs, support::test_gcs(tag).await)
}

#[tokio::test(flavor = "current_thread")]
async fn checked_in_steady_io_matches_the_real_runtime() {
    Box::pin(tokio::task::LocalSet::new().run_until(async {
        let (roots, configs, gcs) = durable_runtime_configs("workload-steady").await;
        let store = gcs.store.clone();
        let (runtime_a, runtime_b, runtime_c) = tokio::join!(
            Runtime::new(&configs[0], store.clone()),
            Runtime::new(&configs[1], store.clone()),
            Runtime::new(&configs[2], store.clone()),
        );
        let runtimes = vec![
            runtime_a.expect("runtime A startup"),
            runtime_b.expect("runtime B startup"),
            runtime_c.expect("runtime C startup"),
        ];
        let spec = load("steady-io").expect("checked-in workload");
        {
            let mut backend = RuntimeDataBackend::new(&runtimes[0], VolumeId(1));
            let outcome = run(&spec, &mut backend)
                .await
                .expect("real runtime workload");

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
    }))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn checked_in_checkpoint_recovery_matches_deterministic_simulation() {
    Box::pin(tokio::task::LocalSet::new().run_until(async {
        let spec = load("checkpoint-recovery").expect("checked-in workload");
        let mut sim_config = blockd_sim::presets::single_host_base();
        sim_config.volume_count = u16::from(spec.shape.disk_volumes) + 1;
        sim_config.volume = VolumeConfig::data(spec.shape.pages);
        sim_config.horizon = millis(10_000);
        sim_config.think = (micros(1), micros(2));
        sim_config.checkpoint_interval = None;
        sim_config.faults = blockd_sim::harness::FaultPlan::none();
        sim_config.faults.rot_records_at.clear();
        sim_config.faults.crash_at.clear();
        let simulated_spec = spec.clone();
        let simulated = tokio::task::spawn_blocking(move || {
            blockd_sim::harness::run_workload(0x53_53, sim_config, simulated_spec)
        })
        .await
        .expect("simulation worker")
        .expect("simulated workload");

        let (roots, [primary, passive_b, passive_c], gcs) =
            durable_runtime_configs("workload-recovery").await;
        let store = gcs.store.clone();
        let (primary_started, first_passive_started, second_passive_started) = tokio::join!(
            Runtime::new(&primary, store.clone()),
            Runtime::new(&passive_b, store.clone()),
            Runtime::new(&passive_c, store.clone()),
        );
        let passives = vec![
            first_passive_started.expect("passive B startup"),
            second_passive_started.expect("passive C startup"),
        ];
        let mut backend = RuntimeLifecycleBackend::new(
            primary,
            store,
            primary_started.expect("primary startup"),
            passives,
            VolumeId(1),
        );
        let actual = run(&spec, &mut backend)
            .await
            .expect("real runtime workload");

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
    }))
    .await;
}
