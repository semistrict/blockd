use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
use blockd_core::placement::PeerCandidate;
use blockd_core::types::HostId;
use blockd_core::types::millis;
use blockd_runtime::fakegcs::{FakeGcs, FakeGcsServer};
use blockd_runtime::{GcsConfig, GcsStore, PeerConfig, Runtime, RuntimeConfig};

#[allow(dead_code)]
pub(crate) async fn local<F: Future>(future: F) -> F::Output {
    // These tests exercise process-global and kernel-backed runtime state
    // through !Send local tasks. Keep separate harness threads from driving
    // independent LocalSets through that state at the same time.
    static LOCAL_RUNTIME: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    let runtime = LOCAL_RUNTIME.get_or_init(|| tokio::sync::Mutex::new(()));
    let _guard = runtime.lock().await;
    tokio::task::LocalSet::new().run_until(future).await
}

#[allow(dead_code)]
pub(crate) async fn wait_for_peer_membership(runtime: &Runtime, expected: usize) {
    let converged = tokio::time::timeout(Duration::from_secs(15), async {
        while runtime.peer_connections().len() != expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        converged.is_ok(),
        "peer membership did not converge: {:?}",
        runtime.peer_connections()
    );
}

#[allow(dead_code)]
pub(crate) struct TestGcs {
    pub fake: FakeGcsServer,
    pub store: Arc<GcsStore>,
}

#[allow(dead_code)]
pub(crate) async fn test_gcs(tag: &str) -> TestGcs {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = Arc::new(GcsStore::new(GcsConfig {
        bucket: "test".to_owned(),
        prefix: format!("{tag}/"),
        endpoint: endpoint.clone(),
        metadata_endpoint: endpoint,
    }));
    TestGcs { fake, store }
}

const MAX_TEST_HOSTS: usize = 3;

#[allow(dead_code)]
pub(crate) fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
}

#[allow(dead_code)]
pub(crate) fn temp_root(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("blockd-runtime-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

#[allow(dead_code)]
pub(crate) fn base_daemon_config(host: u16) -> HostConfig {
    HostConfig {
        archive: blockd_core::hostmeta::ArchivePolicy::default(),
        host: HostId(host),
        cache_pages: 64,
        writeback_interval: millis(5),
        backup_retry: millis(20),
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 500,
        replica_placement: None,
    }
}

#[allow(dead_code)]
pub(crate) fn three_host_runtime_config(
    host: u16,
    blob_dir: PathBuf,
    addresses: [SocketAddr; MAX_TEST_HOSTS],
) -> RuntimeConfig {
    let mut daemon = base_daemon_config(host);
    daemon.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 1,
        local_failure_domain: host + 1,
        roster: (0..MAX_TEST_HOSTS)
            .map(|candidate| {
                let candidate = u16::try_from(candidate).expect("fits");
                PeerCandidate {
                    host: HostId(candidate),
                    weight: 1,
                    failure_domain: candidate + 1,
                    drained: false,
                }
            })
            .collect(),
        authority: None,
    });
    RuntimeConfig {
        daemon,
        blob_dir,
        peer: Some(PeerConfig {
            listen: addresses[usize::from(host)],
        }),
    }
}
