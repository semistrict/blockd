//! Firecracker performance profiles for restore, page-fault service, memory
//! backends, and cold object-store reads. Assertions check workload shape,
//! not machine-dependent latency.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_precision_loss)] // presentation math

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use blockd_core::protocol::StoreFault;
use blockd_runtime::fc::{FcVm, ShmemServer, upload_mem_parts_async};
use blockd_runtime::{GetResult, ObjectStore};

mod support;

const MEM_MIB: u32 = 128;
const ARENA_PAGES: u32 = 4096; // 16 MiB guest working set
const PART_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Default)]
struct UploadProbe {
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
    puts: AtomicU64,
}

#[async_trait::async_trait]
impl ObjectStore for UploadProbe {
    async fn put(self: Arc<Self>, _key: String, bytes: Vec<u8>) -> Result<u64, StoreFault> {
        let current = self
            .in_flight
            .fetch_add(bytes.len() as u64, Ordering::SeqCst)
            + bytes.len() as u64;
        self.max_in_flight.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(2)).await;
        self.in_flight
            .fetch_sub(bytes.len() as u64, Ordering::SeqCst);
        Ok(self.puts.fetch_add(1, Ordering::SeqCst) + 1)
    }

    async fn put_cas(
        self: Arc<Self>,
        _key: String,
        _expected: Option<u64>,
        _bytes: Vec<u8>,
    ) -> Result<u64, StoreFault> {
        unreachable!()
    }

    async fn get(self: Arc<Self>, _key: String) -> GetResult {
        unreachable!()
    }

    async fn get_range(self: Arc<Self>, _key: String, _offset: u64, _len: u64) -> GetResult {
        unreachable!()
    }

    async fn delete(self: Arc<Self>, _key: String) {
        unreachable!()
    }
}

#[tokio::test]
#[ignore = "performance profile; run explicitly in release mode"]
async fn profile_streaming_snapshot_upload() {
    let scratch = tempfile::tempdir().expect("scratch");
    let memory = scratch.path().join("memory");
    std::fs::File::create(&memory)
        .expect("memory file")
        .set_len(u64::from(MEM_MIB) * 1024 * 1024)
        .expect("size memory file");
    let store = Arc::new(UploadProbe::default());
    let started = Instant::now();
    let parts =
        upload_mem_parts_async(store.clone(), memory, "snapshot/mem".to_owned(), PART_BYTES).await;
    let elapsed = started.elapsed();
    let max_in_flight = store.max_in_flight.load(Ordering::SeqCst);
    assert_eq!(parts, 16);
    assert!(max_in_flight <= PART_BYTES * 8);
    eprintln!(
        "streamed 128 MiB as {parts} parts in {:.1}ms; at most {:.1} MiB in upload futures",
        elapsed.as_secs_f64() * 1_000.0,
        max_in_flight as f64 / 1024.0 / 1024.0,
    );
}

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
async fn worked_snapshot(art: &Artifacts) -> (PathBuf, PathBuf, String) {
    let mut vm = FcVm::spawn(&art.fc, &art.scratch.join("base.sock")).await;
    vm.boot(&art.kernel, &art.initrd, MEM_MIB).await;
    vm.wait_line("READY").await;
    vm.cmd(&format!("fill 7 {ARENA_PAGES}"), "FILLED ").await;
    let sum = vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM ").await;
    let snap = art.scratch.join("base.vmstate");
    let mem = art.scratch.join("base.mem");
    vm.pause().await;
    vm.snapshot(&snap, &mem).await;
    vm.kill().await;
    (snap, mem, sum)
}

fn fault_percentile(histogram: &blockd_runtime::HistogramSnapshot, pct: u64) -> u64 {
    let rank = histogram.count.saturating_mul(pct).div_ceil(100);
    blockd_runtime::LATENCY_BUCKETS_NS
        .iter()
        .zip(&histogram.buckets)
        .find(|(_, count)| **count >= rank)
        .map_or(u64::MAX, |(upper, _)| upper / 1_000)
}

fn fault_profile(histogram: &blockd_runtime::HistogramSnapshot) -> String {
    if histogram.count == 0 {
        return "no faults".to_owned();
    }
    format!(
        "{} faults: p50 <= {}µs  p90 <= {}µs  p99 <= {}µs  max <= {}µs",
        histogram.count,
        fault_percentile(histogram, 50),
        fault_percentile(histogram, 90),
        fault_percentile(histogram, 99),
        fault_percentile(histogram, 100),
    )
}

/// PROFILE 1 & 2: restore + full working-set drain, upstream `File`
/// backend vs the patched `UffdShmem` backend, same worked snapshot.
#[tokio::test]
async fn profile_restore_file_vs_uffdshmem() {
    let art = artifacts("backends");
    let (snap, mem, base_sum) = worked_snapshot(&art).await;

    // ── File backend (upstream): kernel MAP_PRIVATE of the mem file ─────
    let started = Instant::now();
    let mut file_vm = FcVm::spawn(&art.fc, &art.scratch.join("file.sock")).await;
    file_vm.load_snapshot(&snap, &mem, None).await;
    let file_first_response = {
        let t = Instant::now();
        file_vm.cmd("ping", "PONG").await;
        t.elapsed()
    };
    let file_restore_total = started.elapsed();
    let t = Instant::now();
    assert_eq!(
        file_vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM ").await,
        base_sum
    );
    let file_drain = t.elapsed();
    file_vm.kill().await;

    // ── UffdShmem backend (our patch): handler-served, warm local tier ──
    let uffd_sock = art.scratch.join("shm.sock");
    let shmem = shmem_path("perf-backends");
    let listener = tokio::net::UnixListener::bind(&uffd_sock).expect("bind");
    let server = ShmemServer::start(
        listener,
        mem.clone(),
        &shmem,
        u64::from(MEM_MIB) * 1024 * 1024,
    )
    .await;
    let started = Instant::now();
    let mut shm_vm = FcVm::spawn(&art.fc, &art.scratch.join("shmvm.sock")).await;
    shm_vm.load_snapshot_shmem(&snap, &uffd_sock, &shmem).await;
    let shm_first_response = {
        let t = Instant::now();
        shm_vm.cmd("ping", "PONG").await;
        t.elapsed()
    };
    let shm_restore_total = started.elapsed();
    let t = Instant::now();
    assert_eq!(
        shm_vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM ").await,
        base_sum
    );
    let shm_drain = t.elapsed();
    shm_vm.kill().await;

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
        fault_profile(&server.fault_latency()),
        server.filled(),
    );
    // Shape assertions: both restored correctly (checked above) and within
    // generous sanity bounds for a shared CI-class VM.
    assert!(file_restore_total < Duration::from_secs(5));
    assert!(shm_restore_total < Duration::from_secs(5));
    assert!(server.faults.load(Ordering::SeqCst) > 0);
}

/// PROFILE 3: cold restore of a real microVM served ENTIRELY from the
/// GCS client under realistic same-region latency, with the exact HTTP
/// request bill recorded.
#[tokio::test]
async fn profile_cold_restore_from_fake_gcs() {
    let art = artifacts("gcs-cold");
    let (snap, mem, base_sum) = worked_snapshot(&art).await;

    // The bucket, with same-region latency on every request.
    let gcs = support::test_gcs("fc-cold").await;
    gcs.fake.latency_ms.store(56, Ordering::SeqCst);
    let store = gcs.store.clone();

    // Backup: the snapshot memory as blx objects (R4.6-sized parts).
    let t = Instant::now();
    let parts = upload_mem_parts_async(
        store.clone(),
        mem.clone(),
        "v/0000000000000001/mem".to_owned(),
        PART_BYTES,
    )
    .await;
    let upload_time = t.elapsed();
    assert_eq!(parts, u64::from(MEM_MIB) * 1024 * 1024 / PART_BYTES);
    let puts_after_upload = gcs
        .fake
        .seen
        .lock()
        .expect("seen lock")
        .iter()
        .filter(|request| request.method == "PUT")
        .count() as u64;
    assert_eq!(puts_after_upload, parts, "one PutObject per part");

    // Cold restore: every byte the guest touches is fetched through GCS by
    // the handler, one GetObject per touched part. Readahead is OFF so the
    // bill below stays exactly demand-shaped (the readahead machinery is
    // pinned by tests/part_fetch_linux.rs).
    let uffd_sock = art.scratch.join("gcs.sock");
    let shmem = shmem_path("perf-gcs-cold");
    let listener = tokio::net::UnixListener::bind(&uffd_sock).expect("bind");
    let server = ShmemServer::start_store(
        listener,
        store.clone(),
        "v/0000000000000001/mem".to_owned(),
        PART_BYTES,
        &shmem,
        u64::from(MEM_MIB) * 1024 * 1024,
        0,
    )
    .await;
    let started = Instant::now();
    let mut vm = FcVm::spawn(&art.fc, &art.scratch.join("s3vm.sock")).await;
    vm.load_snapshot_shmem(&snap, &uffd_sock, &shmem).await;
    let first_response = {
        let t = Instant::now();
        vm.cmd("ping", "PONG").await;
        t.elapsed()
    };
    let restore_total = started.elapsed();
    let gets_at_first_response = gcs
        .fake
        .seen
        .lock()
        .expect("seen lock")
        .iter()
        .filter(|request| request.method == "GET")
        .count() as u64;

    let t = Instant::now();
    assert_eq!(
        vm.cmd(&format!("sum {ARENA_PAGES}"), "SUM ").await,
        base_sum
    );
    let drain = t.elapsed();
    // Liveness after the cold path: keep working.
    let refilled = vm.cmd("fill 11 1024", "FILLED ").await;
    assert_eq!(vm.cmd("sum 1024", "SUM ").await, refilled);
    vm.kill().await;

    let seen = gcs.fake.seen.lock().expect("seen lock");
    let gets_total = seen
        .iter()
        .filter(|request| request.method == "GET")
        .count() as u64;
    let range_requests = seen
        .iter()
        .filter(|request| request.headers.contains_key("range"))
        .count();
    eprintln!("── PROFILE: cold restore through GcsStore + FakeGcs ──");
    eprintln!(
        "  upload: {parts} parts of {} MiB in {upload_time:.1?}",
        PART_BYTES / 1024 / 1024
    );
    eprintln!(
        "  restore {restore_total:.1?} (first response {first_response:.1?}, \
         {gets_at_first_response} GetObject calls to get there)"
    );
    eprintln!("  16 MiB working-set drain {drain:.1?} ({gets_total} GetObject calls total)");
    eprintln!("  faults: {}", fault_profile(&server.fault_latency()));
    eprintln!("  HTTP bill: {puts_after_upload} PUT, {gets_total} GET");

    // The request bill's SHAPE is deterministic: one PutObject per part on
    // backup; on restore, one GetObject per distinct part the guest's
    // touches reached — never per page, never the whole memory eagerly.
    assert!(gets_total >= 3, "cold restore barely touched the store");
    assert!(
        gets_total <= parts,
        "more GetObjects than parts: per-page fetching crept in"
    );
    assert_eq!(
        range_requests, 0,
        "unexpected range request in this scenario"
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
