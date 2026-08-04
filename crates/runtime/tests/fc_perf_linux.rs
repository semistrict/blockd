//! Performance profiles of REAL Firecracker workloads: restore latency and
//! per-fault service profiles for BOTH memory backends (upstream `File`
//! and the patched `UffdShmem`), and a cold restore served from the
//! S3-shaped store with realistic same-region latency injected — recording
//! exactly how many S3 calls of which types the scenario cost.
//!
//! Profiles print to stderr (`--no-capture`); assertions pin the shape
//! (call counts, sanity bounds), not machine-dependent microseconds.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_precision_loss)] // presentation math

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use blockd_runtime::fc::{FcVm, ShmemServer, upload_mem_parts};
use blockd_runtime::{S3LatencyModel, S3Store};

const MEM_MIB: u32 = 128;
const ARENA_PAGES: u32 = 4096; // 16 MiB guest working set
const PART_BYTES: u64 = 8 * 1024 * 1024; // segment-object size for S3 parts

struct Artifacts {
    fc: PathBuf,
    kernel: PathBuf,
    initrd: PathBuf,
    scratch: PathBuf,
}

fn artifacts(tag: &str) -> Artifacts {
    let dir = PathBuf::from(
        std::env::var("BLOCKD_FC_DIR").unwrap_or_else(|_| "/var/tmp/blockd-fc".into()),
    );
    for name in ["firecracker", "vmlinux", "initramfs.cpio"] {
        assert!(
            dir.join(name).exists(),
            "missing {name} in {} — stage the Firecracker artifacts first",
            dir.display()
        );
    }
    // Real disk, never tmpfs (see fc_e2e_linux.rs).
    let scratch = PathBuf::from(format!("/var/tmp/blockd-scratch/fcperf-{tag}"));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch");
    Artifacts {
        fc: dir.join("firecracker"),
        kernel: dir.join("vmlinux"),
        initrd: dir.join("initramfs.cpio"),
        scratch,
    }
}

/// The `UffdShmem` backing file must live on a REAL shmem mount: guest
/// memory is RAM, and uffd registration of a private file mapping is only
/// supported for shmem. Disk scratch is for snapshots and sockets.
fn shmem_path(tag: &str) -> PathBuf {
    let path = PathBuf::from(format!("/dev/shm/blockd-{tag}.shmem"));
    let _ = std::fs::remove_file(&path);
    path
}

/// Boot a VM, run the seeded workload, snapshot it, kill it. Returns the
/// guest's own checksum of its worked state.
fn worked_snapshot(art: &Artifacts) -> (PathBuf, PathBuf, String) {
    let mut vm = FcVm::spawn(&art.fc, &art.scratch.join("base.sock"));
    vm.boot(&art.kernel, &art.initrd, MEM_MIB);
    vm.wait_line("READY");
    vm.cmd(&format!("fill 7 {ARENA_PAGES}"), "FILLED ");
    let sum = vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM ");
    let snap = art.scratch.join("base.vmstate");
    let mem = art.scratch.join("base.mem");
    vm.pause();
    vm.snapshot(&snap, &mem);
    vm.kill();
    (snap, mem, sum)
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    let index = (sorted.len() - 1) * pct / 100;
    sorted[index]
}

fn fault_profile(micros: &Arc<std::sync::Mutex<Vec<u64>>>) -> String {
    let mut sorted = micros.lock().expect("lock").clone();
    if sorted.is_empty() {
        return "no faults".to_owned();
    }
    sorted.sort_unstable();
    format!(
        "{} faults: p50 {}µs  p90 {}µs  p99 {}µs  max {}µs",
        sorted.len(),
        percentile(&sorted, 50),
        percentile(&sorted, 90),
        percentile(&sorted, 99),
        percentile(&sorted, 100),
    )
}

/// PROFILE 1 & 2: restore + full working-set drain, upstream `File`
/// backend vs the patched `UffdShmem` backend, same worked snapshot.
#[test]
fn profile_restore_file_vs_uffdshmem() {
    let art = artifacts("backends");
    let (snap, mem, base_sum) = worked_snapshot(&art);

    // ── File backend (upstream): kernel MAP_PRIVATE of the mem file ─────
    let started = Instant::now();
    let mut file_vm = FcVm::spawn(&art.fc, &art.scratch.join("file.sock"));
    file_vm.load_snapshot(&snap, &mem, None);
    let file_first_response = {
        let t = Instant::now();
        file_vm.cmd("ping", "PONG");
        t.elapsed()
    };
    let file_restore_total = started.elapsed();
    let t = Instant::now();
    assert_eq!(file_vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM "), base_sum);
    let file_drain = t.elapsed();
    file_vm.kill();

    // ── UffdShmem backend (our patch): handler-served, warm local tier ──
    let uffd_sock = art.scratch.join("shm.sock");
    let shmem = shmem_path("perf-backends");
    let listener = std::os::unix::net::UnixListener::bind(&uffd_sock).expect("bind");
    let server = ShmemServer::start(
        listener,
        mem.clone(),
        &shmem,
        u64::from(MEM_MIB) * 1024 * 1024,
    );
    let started = Instant::now();
    let mut shm_vm = FcVm::spawn(&art.fc, &art.scratch.join("shmvm.sock"));
    shm_vm.load_snapshot_shmem(&snap, &uffd_sock, &shmem);
    let shm_first_response = {
        let t = Instant::now();
        shm_vm.cmd("ping", "PONG");
        t.elapsed()
    };
    let shm_restore_total = started.elapsed();
    let t = Instant::now();
    assert_eq!(shm_vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM "), base_sum);
    let shm_drain = t.elapsed();
    shm_vm.kill();

    eprintln!("── PROFILE: restore + 16 MiB working-set drain ──");
    eprintln!(
        "  File backend      restore {file_restore_total:.1?} (first response \
         {file_first_response:.1?})  drain {file_drain:.1?}"
    );
    eprintln!(
        "  UffdShmem backend restore {shm_restore_total:.1?} (first response \
         {shm_first_response:.1?})  drain {shm_drain:.1?}"
    );
    eprintln!(
        "  UffdShmem faults  {}  (pages filled {})",
        fault_profile(&server.fault_micros),
        server.filled(),
    );
    // Shape assertions: both restored correctly (checked above) and within
    // generous sanity bounds for a shared CI-class VM.
    assert!(file_restore_total < Duration::from_secs(5));
    assert!(shm_restore_total < Duration::from_secs(5));
    assert!(server.faults.load(Ordering::SeqCst) > 0);
}

/// PROFILE 3: cold restore of a real microVM served ENTIRELY from the
/// S3-shaped store under realistic same-region latency, with the exact
/// S3 request bill recorded — how many calls, of which types, how many
/// bytes.
#[test]
fn profile_cold_restore_from_simulated_s3() {
    let art = artifacts("s3cold");
    let (snap, mem, base_sum) = worked_snapshot(&art);

    // The bucket, with same-region latency on every request.
    let mut store = S3Store::new();
    store.s3.set_latency(S3LatencyModel::same_region());
    let store = Arc::new(store);

    // Backup: the snapshot memory as segment objects (R4.6-sized parts).
    let t = Instant::now();
    let parts = upload_mem_parts(store.as_ref(), &mem, "v/0000000000000001/mem", PART_BYTES);
    let upload_time = t.elapsed();
    assert_eq!(parts, u64::from(MEM_MIB) * 1024 * 1024 / PART_BYTES);
    let puts_after_upload = store.s3.stats.put_object.load(Ordering::SeqCst);
    assert_eq!(puts_after_upload, parts, "one PutObject per part");

    // Cold restore: every byte the guest touches is fetched from "S3" by
    // the handler, one GetObject per touched part. Readahead is OFF so the
    // bill below stays exactly demand-shaped (the readahead machinery is
    // pinned by tests/part_fetch_linux.rs).
    let uffd_sock = art.scratch.join("s3.sock");
    let shmem = shmem_path("perf-s3cold");
    let listener = std::os::unix::net::UnixListener::bind(&uffd_sock).expect("bind");
    let server = ShmemServer::start_s3(
        listener,
        store.clone(),
        "v/0000000000000001/mem".to_owned(),
        PART_BYTES,
        &shmem,
        u64::from(MEM_MIB) * 1024 * 1024,
        0,
    );
    let started = Instant::now();
    let mut vm = FcVm::spawn(&art.fc, &art.scratch.join("s3vm.sock"));
    vm.load_snapshot_shmem(&snap, &uffd_sock, &shmem);
    let first_response = {
        let t = Instant::now();
        vm.cmd("ping", "PONG");
        t.elapsed()
    };
    let restore_total = started.elapsed();
    let gets_at_first_response = store.s3.stats.get_object.load(Ordering::SeqCst);

    let t = Instant::now();
    assert_eq!(vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM "), base_sum);
    let drain = t.elapsed();
    // Liveness after the cold path: keep working.
    let refilled = vm.cmd("fill 11 1024", "FILLED ");
    assert_eq!(vm.cmd("sum 1024", "SUM "), refilled);
    vm.kill();

    let gets_total = store.s3.stats.get_object.load(Ordering::SeqCst);
    eprintln!("── PROFILE: cold restore from simulated same-region S3 ──");
    eprintln!(
        "  upload: {parts} parts of {} MiB in {upload_time:.1?}",
        PART_BYTES / 1024 / 1024
    );
    eprintln!(
        "  restore {restore_total:.1?} (first response {first_response:.1?}, \
         {gets_at_first_response} GetObject calls to get there)"
    );
    eprintln!("  16 MiB working-set drain {drain:.1?} ({gets_total} GetObject calls total)");
    eprintln!("  faults: {}", fault_profile(&server.fault_micros));
    eprintln!("  S3 bill: {}", store.s3.stats.report());

    // The request bill's SHAPE is deterministic: one PutObject per part on
    // backup; on restore, one GetObject per distinct part the guest's
    // touches reached — never per page, never the whole memory eagerly.
    assert!(gets_total >= 3, "cold restore barely touched the store");
    assert!(
        gets_total <= parts,
        "more GetObjects than parts: per-page fetching crept in"
    );
    assert_eq!(
        store.s3.stats.get_object_range.load(Ordering::SeqCst)
            + store.s3.stats.put_object_conditional.load(Ordering::SeqCst),
        0,
        "unexpected request types in this scenario"
    );
    assert_eq!(
        store.s3.stats.bytes_downloaded.load(Ordering::SeqCst),
        gets_total * PART_BYTES,
        "download bytes must equal fetched parts exactly"
    );
    // Demand paging under same-region latency still resumes promptly: the
    // guest answered within a handful of part fetches (~0.6s in
    // isolation; the bound leaves room for full-suite scheduling on a
    // loaded CI-class VM — a per-page-fetching regression would blow the
    // GetObject-count asserts above and take minutes, not seconds).
    assert!(
        first_response < Duration::from_secs(4),
        "first response took {first_response:?}"
    );
}
