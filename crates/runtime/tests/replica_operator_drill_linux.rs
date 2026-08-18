#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use blockd_core::head::HeadRecord;
use blockd_core::journal::VsetConfig;
use blockd_core::layout;
use blockd_core::types::{PageId, PageNo, VolumeId, VolumeIdx, VsetId};
use blockd_runtime::fakegcs::{FakeGcs, Fault};
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore, Runtime};
use tokio::process::Command;

mod support;

const VSET: VsetId = VsetId(1);

fn store(endpoint: &str, prefix: &str) -> Arc<GcsStore> {
    Arc::new(GcsStore::new(GcsConfig {
        bucket: "drill".to_owned(),
        prefix: prefix.to_owned(),
        endpoint: endpoint.to_owned(),
        metadata_endpoint: endpoint.to_owned(),
    }))
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn repeated_operator_command_recovers_the_last_acknowledged_sync() {
    support::local(async {
        let iterations = std::env::var("BLOCKD_RECOVERY_DRILL_ITERATIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(5);
        for iteration in 0..iterations {
            let (fake, endpoint) = FakeGcs::start().await;
            let prefix = format!("drill-{iteration}/");
            let addresses = [
                support::free_addr(),
                support::free_addr(),
                support::free_addr(),
            ];
            let roots = [
                support::temp_root(&format!("operator-drill-{iteration}-0")),
                support::temp_root(&format!("operator-drill-{iteration}-1")),
                support::temp_root(&format!("operator-drill-{iteration}-2")),
            ];
            let configs = [
                support::three_host_runtime_config(0, roots[0].clone(), addresses),
                support::three_host_runtime_config(1, roots[1].clone(), addresses),
                support::three_host_runtime_config(2, roots[2].clone(), addresses),
            ];
            let a = Runtime::new(&configs[0], store(&endpoint, &prefix)).await;
            let b = Runtime::new(&configs[1], store(&endpoint, &prefix)).await;
            let c = Runtime::new(&configs[2], store(&endpoint, &prefix)).await;
            for runtime in [&a, &b, &c] {
                support::wait_for_peer_membership(runtime, 2).await;
            }
            let vset_config = VsetConfig {
                kind: blockd_core::journal::VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 8,
            };
            a.create_vset(VSET, vset_config).await;
            let (_, head_bytes) = store(&endpoint, &prefix)
                .get(layout::head_key(VSET))
                .await
                .expect("head read")
                .expect("head");
            let head = HeadRecord::decode(VSET, &head_bytes).expect("head decode");
            let active = head.stash.expect("assignment").active_peer;
            fake.faults
                .lock()
                .expect("fault lock")
                .extend(std::iter::repeat_n(Fault::Status(503), 5_000));
            let page = PageId {
                volume: VolumeId {
                    vset: VSET,
                    idx: VolumeIdx(1),
                },
                page: PageNo(3),
            };
            let value = 0xA500_0000_0000_0000 | u64::try_from(iteration).expect("fits");
            a.guest_write(VSET, page, value).await;
            assert!(a.guest_sync(VSET, VolumeIdx(1)).await);
            drop(a);
            drop(b);
            drop(c);
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::remove_dir_all(&roots[0]).expect("delete primary root");
            fake.faults.lock().expect("fault lock").clear();

            let peer_root = &roots[usize::from(active.0)];
            let common = [
                "--endpoint",
                endpoint.as_str(),
                "--metadata-endpoint",
                endpoint.as_str(),
                "--bucket",
                "drill",
                "--prefix",
                prefix.as_str(),
                "--source",
                "0",
                "--vset",
                "1",
                "--residue-root",
                peer_root.to_str().expect("path"),
            ];
            let report = Command::new(env!("CARGO_BIN_EXE_peer_stash_recover"))
                .arg("report")
                .args(common)
                .arg("--peer")
                .arg(active.0.to_string())
                .output()
                .await
                .expect("run report command");
            assert!(
                report.status.success(),
                "{}",
                String::from_utf8_lossy(&report.stderr)
            );
            let report = String::from_utf8(report.stdout).expect("UTF-8 report");
            assert!(report.contains("\"status\":\"complete\""), "{report}");
            assert!(
                report.contains(&format!("\"chosen_peer\":{}", active.0)),
                "{report}"
            );

            let install = Command::new(env!("CARGO_BIN_EXE_peer_stash_recover"))
                .arg("install")
                .args(common)
                .arg("--peer")
                .arg(active.0.to_string())
                .arg("--claimant")
                .arg("0")
                .arg("--target")
                .arg(&roots[0])
                .output()
                .await
                .expect("run install command");
            assert!(
                install.status.success(),
                "{}",
                String::from_utf8_lossy(&install.stderr)
            );

            let (recovered, verdicts) = Runtime::recover(
                &configs[0],
                store(&endpoint, &prefix),
                &BTreeMap::from([(VSET, vset_config)]),
            )
            .await;
            assert!(verdicts.is_empty());
            let _ = recovered.wait_recovered(VSET).await;
            let bytes = recovered.guest_read(VSET, page).await;
            assert_eq!(
                u64::from_ne_bytes(bytes[..8].try_into().expect("word")),
                value
            );
            assert!(recovered.incidents().is_empty());
            drop(recovered);
            for root in &roots {
                std::fs::remove_dir_all(root).expect("cleanup root");
            }
        }
    })
    .await;
}
