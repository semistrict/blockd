use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use blockd_core::types::VolumeId;
use blockd_runtime::cluster::GcsStoreUri;
use blockd_runtime::fakegcs::FakeGcs;
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore};

#[tokio::test]
async fn collector_process_preserves_control_state_beyond_grace() {
    let (_fake, endpoint) = FakeGcs::start().await;
    let uri = GcsStoreUri::parse("gs://cluster/collector-process/").expect("URI");
    let concrete = Arc::new(GcsStore::new(GcsConfig {
        bucket: uri.bucket,
        prefix: uri.prefix,
        endpoint: endpoint.clone(),
        metadata_endpoint: endpoint.clone(),
    }));
    let store: Arc<dyn ObjectStore> = concrete.clone();
    let control_keys = [
        "cluster/metadata",
        "cluster/nodes/00000001.claim",
        "cluster/tls/public-keys/00000001.member",
        "cluster/placement",
        "hosts/00000001/session",
    ];
    for key in control_keys {
        Arc::clone(&store)
            .put(key.to_owned(), vec![1])
            .await
            .expect("control fixture");
    }
    let orphan = blockd_core::layout::blx_key(VolumeId(1), 1, 9);
    Arc::clone(&store)
        .put(orphan.clone(), vec![1])
        .await
        .expect("orphan fixture");

    let mut child = Command::new(env!("CARGO_BIN_EXE_blockd_gc"))
        .args([
            "gs://cluster/collector-process/",
            "--interval-secs",
            "0.02",
            "--grace-secs",
            "0.05",
            "--endpoint",
            &endpoint,
            "--metadata-endpoint",
            &endpoint,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start collector process");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if Arc::clone(&concrete)
                .get(orphan.clone())
                .await
                .expect("poll orphan")
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("collector deleted the orphan beyond grace");
    child.kill().expect("stop collector process");
    let output = child.wait_with_output().expect("collector output");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("deleted 1 objects"), "{stderr}");

    for key in control_keys {
        assert!(
            Arc::clone(&concrete)
                .get(key.to_owned())
                .await
                .expect("read control")
                .is_some(),
            "collector process deleted control record {key}"
        );
    }
    assert!(
        Arc::clone(&concrete)
            .get(orphan)
            .await
            .expect("read orphan")
            .is_none()
    );
}
