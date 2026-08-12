//! Real guest acceptance test for stock `SQLite` Unix VFS over virtio-fs.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use blockd_core::journal::VsetConfig;
use blockd_core::protocol::{DetachMode, Verdict};
use blockd_core::types::{VmId, VsetId};
use blockd_runtime::fc::FcVm;
use blockd_runtime::vsetfs::VsetFsEndpoint;
use blockd_runtime::{Runtime, S3Store};

const MEM_MIB: u32 = 128;

mod support;

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

struct RuntimeCluster {
    root: PathBuf,
    addresses: [SocketAddr; 3],
    store: Arc<S3Store>,
    runtimes: Vec<Option<Arc<Runtime>>>,
}

impl RuntimeCluster {
    fn new(root: &Path) -> Self {
        let addresses = [
            support::free_addr(),
            support::free_addr(),
            support::free_addr(),
        ];
        let store = Arc::new(S3Store::new());
        let runtimes = (0..3)
            .map(|host| {
                Some(Arc::new(Runtime::new(
                    &support::three_host_runtime_config(
                        host,
                        root.join(format!("host-{host}")),
                        addresses,
                    ),
                    store.clone(),
                )))
            })
            .collect();
        std::thread::sleep(Duration::from_millis(100));
        Self {
            root: root.to_owned(),
            addresses,
            store,
            runtimes,
        }
    }

    fn primary(&self) -> Arc<Runtime> {
        Arc::clone(self.runtimes[0].as_ref().expect("primary runtime"))
    }

    fn take_primary(&mut self) -> Arc<Runtime> {
        self.runtimes[0].take().expect("primary runtime")
    }

    fn recover(
        &self,
        configs: &BTreeMap<VsetId, VsetConfig>,
    ) -> (Runtime, BTreeMap<VsetId, Verdict>) {
        Runtime::recover(
            &support::three_host_runtime_config(0, self.root.join("host-0"), self.addresses),
            self.store.clone(),
            configs,
        )
    }
}

fn boot(
    artifacts: &Artifacts,
    runtime: Arc<Runtime>,
    vm: VmId,
    name: &str,
    vset: VsetId,
) -> (FcVm, VsetFsEndpoint, blockd_core::database::AttachmentId) {
    boot_with_vcpus(artifacts, runtime, vm, name, vset, 1)
}

fn boot_with_vcpus(
    artifacts: &Artifacts,
    runtime: Arc<Runtime>,
    vm: VmId,
    name: &str,
    vset: VsetId,
    vcpu_count: u8,
) -> (FcVm, VsetFsEndpoint, blockd_core::database::AttachmentId) {
    let socket = artifacts.scratch.join(format!("{name}.vhost-user-fs"));
    let endpoint =
        VsetFsEndpoint::bind(Arc::clone(&runtime), vm, &socket, "vsets").expect("vsetfs endpoint");
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
    machine.boot_with_vcpus(&artifacts.kernel, &artifacts.initramfs, MEM_MIB, vcpu_count);
    machine.wait_line("READY");
    (machine, endpoint, export.attachment)
}

#[test]
fn stock_sqlite_wal_survives_hot_unplug_restart_and_replug() {
    let artifacts = artifacts("hotplug");
    let mut cluster = RuntimeCluster::new(&artifacts.scratch);
    let vset = VsetId(61);

    let runtime = cluster.take_primary();
    runtime.create_vset(vset, VsetConfig::database(512));
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

    let configs = BTreeMap::from([(vset, VsetConfig::database(512))]);
    let (restarted, verdicts) = cluster.recover(&configs);
    assert!(verdicts.is_empty());
    let restarted = Arc::new(restarted);
    assert!(matches!(
        restarted.wait_recovered(vset),
        Verdict::DatabaseReady { .. }
    ));
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
    let cluster = RuntimeCluster::new(&artifacts.scratch);
    let runtime = cluster.primary();
    let vset = VsetId(63);
    let vm = VmId(84);
    runtime.create_vset(vset, VsetConfig::database(512));

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
    runtime.create_vset(replacement, VsetConfig::database(512));
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
    let cluster = RuntimeCluster::new(&artifacts.scratch);
    let runtime = cluster.primary();
    let vm = VmId(85);
    let first_vset = VsetId(70);
    runtime.create_vset(first_vset, VsetConfig::database(512));
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
            runtime.create_vset(vset, VsetConfig::database(512));
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
    let mut cluster = RuntimeCluster::new(&artifacts.scratch);
    let runtime = cluster.take_primary();
    let vset = VsetId(62);
    let vm = VmId(83);
    runtime.create_vset(vset, VsetConfig::database(512));

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
    let configs = BTreeMap::from([(vset, VsetConfig::database(512))]);
    let (recovered, verdicts) = cluster.recover(&configs);
    assert!(verdicts.is_empty());
    let runtime = Arc::new(recovered);
    assert!(matches!(
        runtime.wait_recovered(vset),
        Verdict::DatabaseReady { .. }
    ));

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

#[test]
#[ignore = "performance profile; requires staged Firecracker artifacts"]
fn profile_parallel_virtiofs_request_queues() {
    let artifacts = artifacts("parallel-queues");
    let cluster = RuntimeCluster::new(&artifacts.scratch);
    let runtime = cluster.primary();
    let exports = [
        ("orders", VsetId(90)),
        ("users", VsetId(91)),
        ("inventory", VsetId(92)),
        ("audit", VsetId(93)),
    ];
    for (_, vset) in exports {
        runtime.create_vset(vset, VsetConfig::database(512));
    }
    let (mut machine, filesystem, _) = boot_with_vcpus(
        &artifacts,
        Arc::clone(&runtime),
        VmId(86),
        "parallel-queues",
        exports[0].1,
        4,
    );
    for &(name, vset) in &exports[1..] {
        filesystem.attach(name, vset).unwrap();
    }
    let status = machine.cmd("fs-status", "FSSTATUS ");
    if !status.starts_with("ok ") {
        machine.kill();
        let backend = filesystem.wait();
        panic!("parallel filesystem did not mount: {status}; backend={backend:?}");
    }
    for (name, _) in exports {
        assert!(
            machine
                .cmd(&format!("fs-db-create {name}"), "FSDBCREATED ")
                .starts_with("4 1010 ")
        );
        assert_eq!(machine.cmd("db-close", "DBCLOSED"), "");
    }

    let single = machine.cmd("fs-open-storm orders 200", "FSOPEN ");
    let parallel = machine.cmd("fs-open-storm orders,users,inventory,audit 200", "FSOPEN ");
    let parse = |reply: &str| {
        let mut fields = reply.split_whitespace();
        let completed = fields.next().unwrap().parse::<u64>().unwrap();
        let elapsed_us = fields.next().unwrap().parse::<u64>().unwrap();
        (completed, elapsed_us)
    };
    let (single_ops, single_us) = parse(&single);
    let (parallel_ops, parallel_us) = parse(&parallel);
    assert_eq!(single_ops, 200);
    assert_eq!(parallel_ops, 800);
    let single_per_second = single_ops.saturating_mul(1_000_000) / single_us.max(1);
    let parallel_per_second = parallel_ops.saturating_mul(1_000_000) / parallel_us.max(1);
    eprintln!("── PROFILE: virtio-fs request queues ──");
    eprintln!(
        "  one queue-active worker: {single_ops} open+stat operations in {single_us}µs ({single_per_second}/s)"
    );
    eprintln!(
        "  four workers/vsets:      {parallel_ops} open+stat operations in {parallel_us}µs ({parallel_per_second}/s)"
    );
    machine.kill();
    filesystem.wait().expect("parallel backend exit");
}
