//! Real guest acceptance test for stock `SQLite` Unix VFS over virtio-fs.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::seam::{DetachMode, StoreFault};
use blockd_core::types::{HostId, VmId, VsetId, millis};
use blockd_runtime::fc::FcVm;
use blockd_runtime::vsetfs::VsetFsEndpoint;
use blockd_runtime::{GetResult, ObjectStore, Runtime, RuntimeConfig};

const MEM_MIB: u32 = 128;

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

struct Artifacts {
    firecracker: PathBuf,
    kernel: PathBuf,
    initramfs: PathBuf,
    scratch: PathBuf,
}

fn artifacts(test_name: &str) -> Artifacts {
    let directory = PathBuf::from(
        std::env::var("BLOCKD_FC_DIR").unwrap_or_else(|_| "/var/tmp/blockd-fc".to_owned()),
    );
    for name in ["firecracker", "vmlinux", "initramfs.cpio"] {
        assert!(
            directory.join(name).exists(),
            "missing {name} in {}; run the Firecracker provisioning step",
            directory.display()
        );
    }
    let scratch = PathBuf::from("/var/tmp/blockd-scratch/fc-sqlite-virtiofs").join(test_name);
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch directory");
    Artifacts {
        firecracker: directory.join("firecracker"),
        kernel: directory.join("vmlinux"),
        initramfs: directory.join("initramfs.cpio"),
        scratch,
    }
}

fn runtime_config(root: &Path) -> RuntimeConfig {
    RuntimeConfig {
        daemon: DaemonConfig {
            host: HostId(0),
            cache_pages: 64,
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

fn boot(
    artifacts: &Artifacts,
    runtime: Arc<Runtime>,
    vm: VmId,
    name: &str,
    vset: VsetId,
) -> (FcVm, VsetFsEndpoint, blockd_core::database::AttachmentId) {
    let socket = artifacts.scratch.join(format!("{name}.vhost-user-fs"));
    let endpoint = VsetFsEndpoint::bind(runtime, vm, &socket, "vsets").expect("vsetfs endpoint");
    let export = endpoint.attach("orders", vset).expect("database export");
    let mut machine = FcVm::spawn(
        &artifacts.firecracker,
        &artifacts.scratch.join(format!("{name}.api.sock")),
    );
    machine.api(
        "PUT",
        "/logger",
        &format!(
            "{{\"log_path\": \"{}\", \"level\": \"Debug\", \"show_level\": true, \"show_log_origin\": true}}",
            artifacts.scratch.join(format!("{name}.firecracker.log")).display()
        ),
    );
    machine.configure_vsetfs(&socket);
    machine.boot(&artifacts.kernel, &artifacts.initramfs, MEM_MIB);
    machine.wait_line("READY");
    (machine, endpoint, export.attachment)
}

#[test]
fn stock_sqlite_wal_survives_hot_unplug_restart_and_replug() {
    let artifacts = artifacts("hotplug");
    let config = runtime_config(&artifacts.scratch);
    let store: Arc<dyn ObjectStore> = Arc::new(EmptyStore);
    let vset = VsetId(61);

    let runtime = Arc::new(Runtime::new(&config, Arc::clone(&store)));
    runtime.create_vset(vset, VsetConfig::database(512, false));
    let (mut first, first_fs, first_attachment) =
        boot(&artifacts, Arc::clone(&runtime), VmId(81), "first", vset);
    let first_status = first.cmd("fs-status", "FSSTATUS ");
    assert!(
        first_status.starts_with("ok orders "),
        "unexpected filesystem status: {first_status}"
    );
    let created = first.cmd("fs-db-create orders", "FSDBCREATED ");
    assert_eq!(created, "4 1010 3.53.4", "unexpected create");
    assert_eq!(first.cmd("db-check", "DBCHECK "), "4 1010 ok");
    assert_eq!(first.cmd("db-close", "DBCLOSED"), "");
    assert_eq!(
        first.cmd("fs-db-open orders", "FSDBOPEN "),
        "3.53.4",
        "unexpected DAX reopen"
    );
    assert_eq!(first.cmd("db-check", "DBCHECK "), "4 1010 ok");
    assert!(
        first_fs.dax_map_count() > 0,
        "SQLite did not cause the guest kernel to establish a DAX mapping"
    );
    assert_eq!(first.cmd("db-close", "DBCLOSED"), "");

    first_fs
        .begin_detach("orders", vset, first_attachment, DetachMode::Graceful)
        .expect("begin graceful detach");
    first_fs
        .finish_detach("orders", vset, first_attachment)
        .expect("finish graceful detach");
    assert!(first_fs.dax_unmap_count() > 0, "DAX window was not revoked");
    first.kill();
    first_fs.wait().expect("first backend exit");
    drop(runtime);

    let configs = BTreeMap::from([(vset, VsetConfig::database(512, false))]);
    let (restarted, verdicts) = Runtime::recover(&config, store, &configs);
    assert!(matches!(
        verdicts.get(&vset),
        Some(blockd_core::seam::Verdict::DatabaseReady { .. })
    ));
    let restarted = Arc::new(restarted);
    let (mut second, second_fs, _second_attachment) =
        boot(&artifacts, restarted, VmId(82), "second", vset);
    let second_status = second.cmd("fs-status", "FSSTATUS ");
    assert!(
        second_status.starts_with("ok orders "),
        "unexpected filesystem status: {second_status}"
    );
    let opened = second.cmd("fs-db-open orders", "FSDBOPEN ");
    assert_eq!(opened, "3.53.4", "unexpected reopen");
    assert_eq!(second.cmd("db-check", "DBCHECK "), "4 1010 ok");
    assert_eq!(second.cmd("db-close", "DBCLOSED"), "");
    second.kill();
    second_fs.wait().expect("second backend exit");
}

#[test]
fn forced_detach_makes_a_retained_dax_mapping_inaccessible() {
    let artifacts = artifacts("forced-revoke");
    let config = runtime_config(&artifacts.scratch);
    let store: Arc<dyn ObjectStore> = Arc::new(EmptyStore);
    let runtime = Arc::new(Runtime::new(&config, store));
    let vset = VsetId(63);
    let vm = VmId(84);
    runtime.create_vset(vset, VsetConfig::database(512, false));

    let (mut machine, filesystem, attachment) =
        boot(&artifacts, Arc::clone(&runtime), vm, "forced-revoke", vset);
    assert_eq!(
        machine.cmd("fs-db-create orders", "FSDBCREATED "),
        "4 1010 3.53.4"
    );
    assert_eq!(machine.cmd("db-close", "DBCLOSED"), "");
    assert_eq!(machine.cmd("fs-retain-map orders", "FSMAPPED"), "");

    let mappings = filesystem
        .begin_detach("orders", vset, attachment, DetachMode::Forced)
        .expect("begin forced detach");
    assert!(!mappings.is_empty(), "retained mmap did not establish DAX");
    filesystem
        .revoke_dax_mappings(&mappings)
        .expect("revoke forced mappings");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !filesystem.finish_forced_detach(vset, attachment) {
        assert!(
            std::time::Instant::now() < deadline,
            "forced detach did not become durable"
        );
        std::thread::park_timeout(std::time::Duration::from_millis(2));
    }

    let replacement = VsetId(64);
    runtime.create_vset(replacement, VsetConfig::database(512, false));
    filesystem
        .attach("users", replacement)
        .expect("attach replacement database after forced revocation");
    assert_eq!(
        machine.cmd("fs-db-create users", "FSDBCREATED "),
        "4 1010 3.53.4",
        "replacement database could not use the remaining DAX aperture"
    );

    assert_eq!(
        machine.try_cmd(
            "fs-stale-read",
            "FSSTALE ",
            std::time::Duration::from_secs(3),
        ),
        None,
        "stale DAX access remained readable after forced detach"
    );
    machine.kill();
    filesystem.wait().expect("forced-revoke backend exit");
}

#[test]
fn repeated_graceful_hotplug_reuses_the_dax_aperture() {
    let artifacts = artifacts("graceful-reuse");
    let config = runtime_config(&artifacts.scratch);
    let store: Arc<dyn ObjectStore> = Arc::new(EmptyStore);
    let runtime = Arc::new(Runtime::new(&config, store));
    let vm = VmId(85);
    let first_vset = VsetId(70);
    runtime.create_vset(first_vset, VsetConfig::database(512, false));
    let (mut machine, filesystem, first_attachment) = boot(
        &artifacts,
        Arc::clone(&runtime),
        vm,
        "graceful-reuse",
        first_vset,
    );
    let mut current = ("orders".to_owned(), first_vset, first_attachment);

    for index in 0..40_u64 {
        assert_eq!(
            machine.cmd(&format!("fs-db-create {}", current.0), "FSDBCREATED "),
            "4 1010 3.53.4"
        );
        assert_eq!(machine.cmd("db-close", "DBCLOSED"), "");
        filesystem
            .begin_detach(&current.0, current.1, current.2, DetachMode::Graceful)
            .unwrap();
        filesystem
            .finish_detach(&current.0, current.1, current.2)
            .unwrap();

        if index + 1 < 40 {
            let vset = VsetId(first_vset.0 + index + 1);
            let name = format!("db{}", index + 1);
            runtime.create_vset(vset, VsetConfig::database(512, false));
            let attachment = filesystem.attach(&name, vset).unwrap().attachment;
            current = (name, vset, attachment);
        }
    }

    assert!(filesystem.dax_unmap_count() >= 40);
    machine.kill();
    filesystem.wait().expect("graceful-reuse backend exit");
}

#[test]
fn open_sqlite_connection_survives_firecracker_memory_snapshot() {
    let artifacts = artifacts("snapshot");
    let config = runtime_config(&artifacts.scratch);
    let store: Arc<dyn ObjectStore> = Arc::new(EmptyStore);
    let runtime = Arc::new(Runtime::new(&config, Arc::clone(&store)));
    let vset = VsetId(62);
    let vm = VmId(83);
    runtime.create_vset(vset, VsetConfig::database(512, false));

    let (mut first, first_fs, _attachment) =
        boot(&artifacts, Arc::clone(&runtime), vm, "snapshot", vset);
    assert_eq!(
        first.cmd("fs-db-create orders", "FSDBCREATED "),
        "4 1010 3.53.4"
    );
    assert_eq!(first.cmd("db-check", "DBCHECK "), "4 1010 ok");
    assert!(first_fs.dax_map_count() > 0, "snapshot has no DAX mappings");

    let snapshot = artifacts.scratch.join("vmstate");
    let memory = artifacts.scratch.join("memory");
    first.pause();
    first.snapshot(&snapshot, &memory);
    first.kill();
    first_fs.wait().expect("snapshot backend exit");
    drop(runtime);

    // Warm restore must reconstruct volatile attachment and handle state from
    // the validated filesystem snapshot; retaining the original daemon would
    // conceal stale attachment generations and unopened durable handles.
    let configs = BTreeMap::from([(vset, VsetConfig::database(512, false))]);
    let (recovered, verdicts) = Runtime::recover(&config, store, &configs);
    assert!(matches!(
        verdicts.get(&vset),
        Some(blockd_core::seam::Verdict::DatabaseReady { .. })
    ));
    let runtime = Arc::new(recovered);

    let socket = artifacts.scratch.join("snapshot.vhost-user-fs");
    let restored_fs = VsetFsEndpoint::bind(Arc::clone(&runtime), vm, &socket, "vsets")
        .expect("restored vsetfs endpoint");
    let mut restored = FcVm::spawn(
        &artifacts.firecracker,
        &artifacts.scratch.join("restored.api.sock"),
    );
    restored.load_snapshot_shared(&snapshot, &memory);
    assert_eq!(restored.cmd("ping", "PONG"), "");
    assert_eq!(restored.cmd("db-check", "DBCHECK "), "4 1010 ok");
    assert!(
        restored_fs.dax_map_count() > 0,
        "restore did not replay DAX mappings"
    );
    assert_eq!(restored.cmd("db-close", "DBCLOSED"), "");
    restored.kill();
    restored_fs.wait().expect("restored backend exit");
}
