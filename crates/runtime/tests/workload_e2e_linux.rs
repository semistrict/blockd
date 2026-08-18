//! Shared-workload differential gate against the real Linux runtime.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "support/workload.rs"]
mod runtime_workload;
mod support;

use std::path::PathBuf;

use blockd_core::journal::VsetConfig;
use blockd_core::types::{VsetId, micros, millis};
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
            u16::try_from(host).expect("host fits"),
            roots[host].clone(),
            addresses,
        )
    });
    (roots, configs, support::test_gcs(tag).await)
}

#[tokio::test(flavor = "current_thread")]
async fn checked_in_steady_io_matches_the_real_runtime() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (roots, configs, gcs) = durable_runtime_configs("workload-steady").await;
            let store = gcs.store.clone();
            let mut runtimes = Vec::new();
            for config in &configs {
                runtimes.push(Runtime::new(config, store.clone()).await);
            }
            let spec = load("steady-io").expect("checked-in workload");
            {
                let mut backend = RuntimeDataBackend::new(&runtimes[0], VsetId(1));
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
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn checked_in_checkpoint_recovery_matches_deterministic_simulation() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let spec = load("checkpoint-recovery").expect("checked-in workload");
            let mut sim_config = blockd_sim::presets::single_host_base();
            sim_config.vset_count = 1;
            sim_config.vset =
                VsetConfig::compute(spec.shape.disk_volumes, spec.shape.pages_per_volume);
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
            let passives = vec![
                Runtime::new(&passive_b, store.clone()).await,
                Runtime::new(&passive_c, store.clone()).await,
            ];
            let mut backend = RuntimeLifecycleBackend::new(primary, store, passives, VsetId(1));
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
        })
        .await;
}
