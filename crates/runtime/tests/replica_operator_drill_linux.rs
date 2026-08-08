#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use blockd_core::daemon::{DaemonConfig, ReplicaPlacementConfig};
use blockd_core::head::HeadRecord;
use blockd_core::journal::{DurabilityMode, VsetConfig};
use blockd_core::layout;
use blockd_core::placement::PeerCandidate;
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
use blockd_runtime::fakegcs::{FakeGcs, Fault};
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore, PeerConfig, Runtime, RuntimeConfig};

mod support;

const VSET: VsetId = VsetId(1);

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
}

fn root(iteration: usize, host: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "blockd-operator-drill-{}-{iteration}-{host}",
        std::process::id()
    ))
}

fn runtime_config(host: u16, root: PathBuf, addresses: [SocketAddr; 3]) -> RuntimeConfig {
    RuntimeConfig {
        daemon: DaemonConfig {
            host: HostId(host),
            cache_pages: 64,
            writeback_interval: millis(5),
            backup_retry: millis(20),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 500,
            replica_placement: Some(ReplicaPlacementConfig {
                membership_epoch: 1,
                local_failure_domain: host + 1,
                roster: (0..3)
                    .map(|candidate| PeerCandidate {
                        host: HostId(candidate),
                        weight: 1,
                        failure_domain: candidate + 1,
                        drained: false,
                    })
                    .collect(),
            }),
        },
        blob_dir: root,
        peer: Some(PeerConfig {
            listen: addresses[usize::from(host)],
            peers: addresses
                .into_iter()
                .enumerate()
                .map(|(id, address)| (HostId(u16::try_from(id).expect("fits")), address))
                .collect(),
            outbound_protocol_versions: BTreeMap::new(),
            tls: Some(support::peer_tls(usize::from(host), 3)),
        }),
    }
}

fn store(endpoint: &str, prefix: &str) -> Arc<GcsStore> {
    Arc::new(GcsStore::new(GcsConfig {
        bucket: "drill".to_owned(),
        prefix: prefix.to_owned(),
        endpoint: endpoint.to_owned(),
        metadata_endpoint: endpoint.to_owned(),
    }))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn repeated_operator_command_recovers_the_last_acknowledged_sync() {
    let iterations = std::env::var("BLOCKD_RECOVERY_DRILL_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(5);
    for iteration in 0..iterations {
        let (fake, endpoint) = FakeGcs::start();
        let prefix = format!("drill-{iteration}/");
        let addresses = [free_addr(), free_addr(), free_addr()];
        let roots = [root(iteration, 0), root(iteration, 1), root(iteration, 2)];
        for root in &roots {
            let _ = std::fs::remove_dir_all(root);
        }
        let configs = [
            runtime_config(0, roots[0].clone(), addresses),
            runtime_config(1, roots[1].clone(), addresses),
            runtime_config(2, roots[2].clone(), addresses),
        ];
        let a = Runtime::new(&configs[0], store(&endpoint, &prefix));
        let b = Runtime::new(&configs[1], store(&endpoint, &prefix));
        let c = Runtime::new(&configs[2], store(&endpoint, &prefix));
        let vset_config = VsetConfig {
            kind: blockd_core::journal::VsetKind::Compute,
            disk_volumes: 1,
            pages_per_volume: 8,
            durability: DurabilityMode::PeerStashed,
        };
        a.create_vset(VSET, vset_config);
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
        a.guest_write(VSET, page, value);
        assert!(a.guest_sync(VSET, VolumeIdx(1)));
        drop(a);
        drop(b);
        drop(c);
        std::thread::sleep(Duration::from_millis(50));
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
        );
        assert!(verdicts.is_empty());
        let _ = recovered.wait_recovered(VSET);
        let bytes = recovered.guest_read(VSET, page);
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
}
