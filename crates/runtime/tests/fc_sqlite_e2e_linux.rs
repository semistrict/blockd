//! Real guest acceptance test for `SQLite` -> custom VFS -> `AF_VSOCK` ->
//! Firecracker -> host daemon -> durable database-vset records.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::journal::VsetConfig;
use blockd_core::protocol::{DetachMode, Verdict};
use blockd_core::types::{VmId, VsetId};
use blockd_runtime::database::{DEFAULT_DATABASE_VSOCK_PORT, DatabaseEndpoint};
use blockd_runtime::fc::FcVm;
use blockd_runtime::{Runtime, S3Store};

const MEM_MIB: u32 = 128;

mod support;

struct Artifacts {
    firecracker: PathBuf,
    kernel: PathBuf,
    initramfs: PathBuf,
    scratch: PathBuf,
}

fn artifacts() -> Artifacts {
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
    let scratch = PathBuf::from("/var/tmp/blockd-scratch/fc-sqlite-vsock");
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch directory");
    Artifacts {
        firecracker: directory.join("firecracker"),
        kernel: directory.join("vmlinux"),
        initramfs: directory.join("initramfs.cpio"),
        scratch,
    }
}

fn runtime_cluster(root: &Path) -> ([SocketAddr; 3], Arc<S3Store>, Vec<Option<Arc<Runtime>>>) {
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
    (addresses, store, runtimes)
}

fn boot(
    artifacts: &Artifacts,
    runtime: Arc<Runtime>,
    vm: VmId,
    name: &str,
) -> (FcVm, DatabaseEndpoint) {
    let vsock_path = artifacts.scratch.join(format!("{name}.vsock"));
    let endpoint =
        DatabaseEndpoint::bind_firecracker(runtime, vm, &vsock_path, DEFAULT_DATABASE_VSOCK_PORT)
            .expect("database vsock listener");
    let mut machine = FcVm::spawn(
        &artifacts.firecracker,
        &artifacts.scratch.join(format!("{name}.api.sock")),
    );
    machine.configure_vsock(u32::try_from(vm.0).expect("guest CID fits"), &vsock_path);
    machine.boot(&artifacts.kernel, &artifacts.initramfs, MEM_MIB);
    machine.wait_line("READY");
    (machine, endpoint)
}

fn finish_detach(runtime: &Runtime, vset: VsetId, attachment: blockd_core::database::AttachmentId) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !runtime.finish_detach_database(vset, attachment) {
        assert!(
            Instant::now() < deadline,
            "database detach did not become durable"
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
}

#[test]
fn sqlite_inside_firecracker_survives_detach_daemon_restart_and_new_vm() {
    let artifacts = artifacts();
    let (addresses, store, mut runtimes) = runtime_cluster(&artifacts.scratch);
    let vset = VsetId(51);
    let database_config = VsetConfig::database(512);

    let runtime = runtimes[0].take().expect("primary runtime");
    runtime.create_vset(vset, database_config);
    let first_vm = VmId(77);
    let first_attachment = runtime.attach_database(vset, first_vm);
    let (mut first, first_endpoint) = boot(&artifacts, runtime.clone(), first_vm, "first");
    let created = first.cmd(
        &format!(
            "db-create {} {} {} {}",
            vset.0, first_vm.0, first_attachment.generation, DEFAULT_DATABASE_VSOCK_PORT
        ),
        "DBCREATED ",
    );
    assert!(
        created.starts_with("4 1010 3."),
        "unexpected create result: {created}"
    );
    assert_eq!(first.cmd("db-check", "DBCHECK "), "4 1010 ok");
    first.cmd("db-close", "DBCLOSED");
    runtime.begin_detach_database(vset, first_attachment, DetachMode::Graceful);
    finish_detach(&runtime, vset, first_attachment);
    first.kill();
    drop(first_endpoint);
    drop(runtime);

    let configs = BTreeMap::from([(vset, database_config)]);
    let (recovered, verdicts) = Runtime::recover(
        &support::three_host_runtime_config(0, artifacts.scratch.join("host-0"), addresses),
        store,
        &configs,
    );
    assert!(verdicts.is_empty());
    let recovered = Arc::new(recovered);
    assert!(matches!(
        recovered.wait_recovered(vset),
        Verdict::DatabaseReady { .. }
    ));
    let second_vm = VmId(78);
    let second_attachment = recovered.attach_database(vset, second_vm);
    let (mut second, second_endpoint) = boot(&artifacts, recovered.clone(), second_vm, "second");
    let opened = second.cmd(
        &format!(
            "db-open {} {} {} {}",
            vset.0, second_vm.0, second_attachment.generation, DEFAULT_DATABASE_VSOCK_PORT
        ),
        "DBOPEN ",
    );
    assert!(
        opened.starts_with("3."),
        "unexpected SQLite version: {opened}"
    );
    assert_eq!(second.cmd("db-check", "DBCHECK "), "4 1010 ok");
    second.cmd("db-close", "DBCLOSED");
    recovered.begin_detach_database(vset, second_attachment, DetachMode::Graceful);
    finish_detach(&recovered, vset, second_attachment);
    second.kill();
    drop(second_endpoint);
    drop(recovered);
    drop(runtimes);
    let _ = std::fs::remove_dir_all(artifacts.scratch);
}
