#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use blockd_core::head::HeadRecord;
use blockd_core::journal::VolumeConfig;
use blockd_core::layout;
use blockd_core::protocol::Verdict;
use blockd_core::types::{PageId, PageNo, VolumeId};
use blockd_runtime::fakegcs::{FakeGcs, Fault};
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore, Runtime};
use tokio::process::Command;

mod support;

const VOLUME: VolumeId = VolumeId(1);

fn store(endpoint: &str, prefix: &str) -> Arc<GcsStore> {
    Arc::new(GcsStore::new(GcsConfig {
        bucket: "drill".to_owned(),
        prefix: prefix.to_owned(),
        endpoint: endpoint.to_owned(),
        metadata_endpoint: endpoint.to_owned(),
    }))
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::large_futures, clippy::too_many_lines)]
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
            // Direct Runtime fixtures bypass daemon bootstrap, so seed the
            // permanent source claim required by the offline recovery tool.
            store(&endpoint, &prefix)
                .put(
                    layout::node_claim_key(configs[0].daemon.host),
                    vec![0x5a; 16],
                )
                .await
                .expect("seed source claim");
            let (a, b, c) = tokio::join!(
                Runtime::new(&configs[0], store(&endpoint, &prefix)),
                Runtime::new(&configs[1], store(&endpoint, &prefix)),
                Runtime::new(&configs[2], store(&endpoint, &prefix)),
            );
            let a = a.expect("runtime startup");
            let b = b.expect("runtime startup");
            let c = c.expect("runtime startup");
            for runtime in [&a, &b, &c] {
                support::wait_for_peer_membership(runtime, 2).await;
            }
            let volume_config = VolumeConfig {
                kind: blockd_core::journal::VolumeKind::Data,
                pages: 8,
            };
            a.create_volume(VOLUME, volume_config).await;
            let (_, head_bytes) = store(&endpoint, &prefix)
                .get(layout::head_key(VOLUME))
                .await
                .expect("head read")
                .expect("head");
            let head = HeadRecord::decode(VOLUME, &head_bytes).expect("head decode");
            let active = head.stash.expect("assignment").active_peer;
            fake.faults
                .lock()
                .expect("fault lock")
                .extend(std::iter::repeat_n(Fault::Status(503), 5_000));
            let page = PageId {
                volume: VOLUME,
                page: PageNo(3),
            };
            let value = 0xA500_0000_0000_0000 | u64::try_from(iteration).expect("fits");
            a.guest_write(VOLUME, page, value).await;
            assert!(a.guest_sync(VOLUME).await);
            drop(a);
            drop(b);
            drop(c);
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::remove_dir_all(&roots[0]).expect("delete primary root");
            fake.faults.lock().expect("fault lock").clear();

            let peer_root = &roots[usize::try_from(active.get()).expect("host index")];
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
                "--volume",
                "1",
                "--residue-root",
                peer_root.to_str().expect("path"),
            ];
            let report = Command::new(env!("CARGO_BIN_EXE_peer_stash_recover"))
                .arg("report")
                .args(common)
                .arg("--peer")
                .arg(active.get().to_string())
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
                report.contains(&format!("\"chosen_peer\":{}", active.get())),
                "{report}"
            );

            let install = Command::new(env!("CARGO_BIN_EXE_peer_stash_recover"))
                .arg("install")
                .args(common)
                .arg("--peer")
                .arg(active.get().to_string())
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

            let expected_volumes = BTreeMap::from([(VOLUME, volume_config)]);
            let (recovery, passive_b, passive_c) = tokio::join!(
                Runtime::recover(&configs[0], store(&endpoint, &prefix), &expected_volumes,),
                Runtime::new(&configs[1], store(&endpoint, &prefix)),
                Runtime::new(&configs[2], store(&endpoint, &prefix)),
            );
            let recovered_passives = [
                passive_b.expect("passive B restart"),
                passive_c.expect("passive C restart"),
            ];
            let (recovered, verdicts) = recovery.expect("runtime startup");
            assert_eq!(verdicts, BTreeMap::from([(VOLUME, Verdict::ColdBoot)]));
            let bytes = recovered.guest_read(VOLUME, page).await;
            assert_eq!(
                u64::from_ne_bytes(bytes[..8].try_into().expect("word")),
                value
            );
            assert!(recovered.incidents().is_empty());
            drop(recovered);
            drop(recovered_passives);
            for root in &roots {
                std::fs::remove_dir_all(root).expect("cleanup root");
            }
        }
    })
    .await;
}
