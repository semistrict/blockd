#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::protocol::{DetachMode, StoreFault, Verdict};
use blockd_core::types::{HostId, VmId, VsetId, millis};
use blockd_runtime::database::DatabaseEndpoint;
use blockd_runtime::{GetResult, ObjectStore, Runtime, RuntimeConfig};
use blockd_sqlite_vfs::register_unix;
use rusqlite::{Connection, OpenFlags};

struct EmptyStore;

#[async_trait::async_trait]
impl ObjectStore for EmptyStore {
    async fn put(self: Arc<Self>, _key: String, _bytes: Vec<u8>) -> Result<u64, StoreFault> {
        Ok(1)
    }

    async fn put_cas(
        self: Arc<Self>,
        _key: String,
        _expected: Option<u64>,
        _bytes: Vec<u8>,
    ) -> Result<u64, StoreFault> {
        Ok(1)
    }

    async fn get(self: Arc<Self>, _key: String) -> GetResult {
        Ok(None)
    }

    async fn get_range(self: Arc<Self>, _key: String, _offset: u64, _len: u64) -> GetResult {
        Ok(None)
    }

    async fn delete(self: Arc<Self>, _key: String) {}
}

fn config(root: &Path) -> RuntimeConfig {
    RuntimeConfig {
        daemon: DaemonConfig {
            archive: Default::default(),
            host: HostId(0),
            cache_pages: 32,
            writeback_interval: millis(5),
            backup_retry: millis(20),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 25,
            replica_placement: None,
        },
        blob_dir: root.join("blobs"),
        peer: None,
    }
}

fn open(name: &str) -> Connection {
    Connection::open_with_flags_and_vfs(
        "vset-51",
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        name,
    )
    .expect("open through VFS")
}

#[test]
fn sqlite_commit_survives_daemon_restart_and_generation_change() {
    let root = PathBuf::from(format!("/tmp/bdv-runtime-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("test root");
    let runtime_config = config(&root);
    let store: Arc<dyn ObjectStore> = Arc::new(EmptyStore);
    let vset = VsetId(51);
    let vm = VmId(12);
    let database_config = VsetConfig::database(256);

    let runtime = Arc::new(Runtime::new(&runtime_config, store.clone()));
    runtime.create_vset(vset, database_config);
    let first = runtime.attach_database(vset, vm);
    let endpoint_path = root.join("first.sock");
    let endpoint =
        DatabaseEndpoint::bind_unix(runtime.clone(), vm, &endpoint_path).expect("first endpoint");
    let registration = register_unix(
        Some("blockd-runtime-first"),
        &endpoint_path,
        &root.join("locks"),
        vset,
        first,
    )
    .expect("first registration");
    let connection = open("blockd-runtime-first");
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE durable(value TEXT NOT NULL);
             INSERT INTO durable VALUES ('one'), ('two'), ('three');",
        )
        .expect("commit");
    assert_eq!(
        connection
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .expect("integrity"),
        "ok"
    );
    drop(connection);
    drop(registration);
    drop(endpoint);
    runtime.begin_detach_database(vset, first, DetachMode::Graceful);
    let detach_deadline = Instant::now() + Duration::from_secs(5);
    while !runtime.finish_detach_database(vset, first) {
        assert!(
            Instant::now() < detach_deadline,
            "database detach did not become durable"
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
    drop(runtime);

    let configs = BTreeMap::from([(vset, database_config)]);
    let (recovered, verdicts) = Runtime::recover(&runtime_config, store, &configs);
    assert!(matches!(
        verdicts.get(&vset),
        Some(Verdict::DatabaseReady { .. })
    ));
    let recovered = Arc::new(recovered);
    let second = recovered.attach_database(vset, vm);
    let endpoint_path = root.join("second.sock");
    let endpoint = DatabaseEndpoint::bind_unix(recovered.clone(), vm, &endpoint_path)
        .expect("second endpoint");
    let registration = register_unix(
        Some("blockd-runtime-second"),
        &endpoint_path,
        &root.join("locks"),
        vset,
        second,
    )
    .expect("second registration");
    let connection = open("blockd-runtime-second");
    let count: i64 = connection
        .query_row("SELECT count(*) FROM durable", [], |row| row.get(0))
        .expect("recovered rows");
    assert_eq!(count, 3);
    assert_eq!(
        connection
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .expect("recovered integrity"),
        "ok"
    );
    drop(connection);
    drop(registration);
    drop(endpoint);
    drop(recovered);
    let _ = std::fs::remove_dir_all(root);
}
