//! End-to-end: the REAL system — the exact daemon state machine the
//! deterministic simulation proved, driving real userfaultfd guest memory,
//! real disk blobs, real timers — under simulated VM workloads, against an
//! S3-shaped store. Every byte a guest ever reads is checked against the
//! workload's own model.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::daemon::DaemonConfig;
use blockd_core::head::HeadRecord;
use blockd_core::journal::{JournalRecord, RecordKind, VsetConfig};
use blockd_core::layout;
use blockd_core::seam::Verdict;
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
use blockd_hostmem::page_size;
use blockd_runtime::fakegcs::FakeGcs;
use blockd_runtime::{GcsConfig, GcsStore, Runtime, RuntimeConfig, S3Store};

const VSET: VsetId = VsetId(1);

fn pid(idx: u8, page: u32) -> PageId {
    PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(idx),
        },
        page: PageNo(page),
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("blockd-e2e-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn runtime_config(tag: &str, cache_pages: usize) -> RuntimeConfig {
    RuntimeConfig {
        daemon: DaemonConfig {
            host: HostId(0),
            cache_pages,
            writeback_interval: millis(5),
            backup_retry: millis(20),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 500,
            replica_placement: None,
        },
        blob_dir: temp_dir(tag),
        peer: None,
    }
}

fn vset_config(disk_volumes: u8, pages_per_volume: u32, backed_up: bool) -> VsetConfig {
    VsetConfig::compute(disk_volumes, pages_per_volume, backed_up)
}

/// The simulated VM workload: seeded ops with a full model; every read is
/// verified byte-for-byte (word 0 carries the last write; the rest of the
/// page must be the fill it arrived with — zeros).
struct Workload {
    lcg: u64,
    config: VsetConfig,
    model: BTreeMap<PageId, u64>,
}

impl Workload {
    fn new(seed: u64, config: VsetConfig) -> Workload {
        Workload {
            lcg: seed.max(1),
            config,
            model: BTreeMap::new(),
        }
    }

    fn next(&mut self) -> u64 {
        self.lcg = self
            .lcg
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.lcg
    }

    fn random_page(&mut self) -> PageId {
        let idx =
            u8::try_from(self.next() % u64::from(self.config.disk_volumes) + 1).expect("fits");
        let page =
            u32::try_from(self.next() % u64::from(self.config.pages_per_volume)).expect("fits");
        pid(idx, page)
    }

    /// One guest op: mostly writes, some verifying reads.
    fn step(&mut self, rt: &Runtime, op: u64) {
        let page = self.random_page();
        if self.next().is_multiple_of(4) {
            self.verify_page(rt, page);
        } else {
            let value = 0x1000_0000 + op;
            rt.guest_write(VSET, page, value);
            self.model.insert(page, value);
        }
    }

    fn verify_page(&self, rt: &Runtime, page: PageId) {
        let bytes = rt.guest_read(VSET, page);
        let want = self.model.get(&page).copied().unwrap_or(0);
        assert_eq!(
            u64::from_le_bytes(bytes[0..8].try_into().expect("sized")),
            want,
            "{page:?}: word 0 diverged from the model"
        );
        assert!(
            bytes[8..].iter().all(|&b| b == 0),
            "{page:?}: fill tail corrupted"
        );
    }

    /// The fsck: every page of every volume, byte-for-byte.
    fn verify_all(&self, rt: &Runtime, include_memory: bool) {
        for idx in 0..=self.config.disk_volumes {
            if idx == 0 && !include_memory {
                continue;
            }
            for page in 0..self.config.pages_per_volume {
                self.verify_page(rt, pid(idx, page));
            }
        }
    }

    /// Cold boot dropped memory: it must read as zeros.
    fn expect_zero_memory(&mut self, rt: &Runtime) {
        for page in 0..self.config.pages_per_volume {
            self.model.remove(&pid(0, page));
            let bytes = rt.guest_read(VSET, pid(0, page));
            assert!(
                bytes.iter().all(|&b| b == 0),
                "memory page {page} survived a cold boot"
            );
        }
    }
}

/// Lifecycle end to end (R2/R3/R8.2): guest load with faults, syncs and a
/// checkpoint — then a crash, recovery from the real on-disk blobs, a
/// RESUME at the checkpoint's vmstate, and byte-exact content. And R4.4:
/// the non-backed vset wrote not one object.
#[test]
fn lifecycle_checkpoint_crash_resume_end_to_end() {
    let store = Arc::new(S3Store::new());
    let config = runtime_config("lifecycle", 256);
    let vc = vset_config(2, 16, false);
    let rt = Runtime::new(&config, store.clone());
    rt.create_vset(VSET, vc);

    let mut workload = Workload::new(7, vc);
    for op in 0..200 {
        workload.step(&rt, op);
        if op % 50 == 49 {
            assert!(rt.guest_sync(VSET, VolumeIdx(1)), "sync rejected");
        }
    }
    // A few memory-volume writes too (memory resumes with a checkpoint).
    for page in 0..4 {
        rt.guest_write(VSET, pid(0, page), 0xAAA0 + u64::from(page));
        workload
            .model
            .insert(pid(0, page), 0xAAA0 + u64::from(page));
    }

    let applied_before = rt.guest_applied(VSET);
    let epoch = rt.checkpoint(VSET);
    assert_eq!(epoch, 1);
    assert_eq!(
        rt.guest_applied(VSET),
        applied_before,
        "the pause retired ops"
    );
    workload.verify_all(&rt, true);
    assert_eq!(rt.incidents(), Vec::<String>::new());

    // ── crash ───────────────────────────────────────────────────────────
    drop(rt);
    let (rt, verdicts) = Runtime::recover(&config, store.clone(), &BTreeMap::from([(VSET, vc)]));
    assert_eq!(
        verdicts.get(&VSET),
        Some(&Verdict::Resume {
            epoch: blockd_core::types::Epoch(1),
            vmstate: applied_before
        }),
        "recovery must resume the checkpoint"
    );
    // The fsck: every page, served by MISSING faults from the real
    // segment files the previous incarnation wrote.
    workload.verify_all(&rt, true);

    // Liveness after resume: more load, another sync, another checkpoint.
    for op in 200..250 {
        workload.step(&rt, op);
    }
    assert!(rt.guest_sync(VSET, VolumeIdx(2)));
    assert_eq!(rt.checkpoint(VSET), 2);
    workload.verify_all(&rt, true);
    assert_eq!(rt.incidents(), Vec::<String>::new());

    // R4.4: a non-backed-up vset writes ZERO objects, ever.
    let listing = store.s3.list_objects_v2("", None, 100);
    assert_eq!(listing.contents, Vec::new(), "non-backed vset touched S3");
}

/// R3.8 end to end: acknowledged syncs survive a crash with NO checkpoint
/// ever taken — cold boot lands at sync consistency on the real disks;
/// memory is honestly gone.
#[test]
fn sync_then_crash_cold_boots_at_sync_consistency() {
    let store = Arc::new(S3Store::new());
    let config = runtime_config("coldboot", 256);
    let vc = vset_config(2, 16, false);
    let rt = Runtime::new(&config, store.clone());
    rt.create_vset(VSET, vc);

    let mut workload = Workload::new(21, vc);
    for op in 0..120 {
        workload.step(&rt, op);
    }
    // Memory writes that must NOT survive (no checkpoint → no memory).
    rt.guest_write(VSET, pid(0, 3), 0xDEAD);
    // The barrier: every write so far becomes crash-durable.
    assert!(rt.guest_sync(VSET, VolumeIdx(1)));
    assert!(rt.guest_sync(VSET, VolumeIdx(2)));
    assert_eq!(rt.incidents(), Vec::<String>::new());

    drop(rt); // crash — no checkpoint was ever taken
    let (rt, verdicts) = Runtime::recover(&config, store, &BTreeMap::from([(VSET, vc)]));
    assert_eq!(verdicts.get(&VSET), Some(&Verdict::ColdBoot));
    // Disks: exactly the model at the sync barrier (nothing was written
    // after it). Memory: zeros.
    workload.verify_all(&rt, false);
    workload.expect_zero_memory(&rt);
}

/// R2.4/R2.5 end to end: a cache far smaller than the working set. The
/// daemon's writeback+evict cycle keeps ACTUAL physical guest memory
/// bounded (evict punches the backing — measured), refaults refill from
/// the real segment files, and no guest ever reads a wrong byte.
#[test]
fn eviction_pressure_bounds_physical_memory_and_serves_from_disk() {
    let store = Arc::new(S3Store::new());
    let config = runtime_config("pressure", 8); // 8-page cache
    let vc = vset_config(1, 32, false); // 64-page working set
    let rt = Runtime::new(&config, store);
    rt.create_vset(VSET, vc);

    let mut workload = Workload::new(3, vc);
    for op in 0..300 {
        workload.step(&rt, op);
        if op % 60 == 59 {
            assert!(rt.guest_sync(VSET, VolumeIdx(1)));
        }
    }
    // Physical guest memory stayed near the cache budget, far below the
    // 32-page working set — the overcommit is real, measured on the memfd.
    let resident = rt.guest_resident_bytes(VSET);
    assert!(
        resident <= 16 * page_size(),
        "evictions did not bound physical memory: {resident} bytes resident"
    );
    // And every byte still reads back correctly — from disk segments.
    workload.verify_all(&rt, true);
    assert_eq!(rt.incidents(), Vec::<String>::new());
}

/// The full multi-host story against the S3-shaped bucket (R4/R6): host A
/// backs up continuously (conditional-write head CAS, Range-read fills),
/// dies; host B restores from the bucket alone, resumes at the last
/// backed-up checkpoint, and serves the exact bytes. Segments travel
/// verbatim (R8.4): the S3 object is byte-identical to A's local file.
#[test]
#[allow(clippy::too_many_lines)]
fn backup_restore_moves_the_vset_between_hosts_via_s3() {
    let store = Arc::new(S3Store::new());
    let config_a = runtime_config("host-a", 256);
    let vc = vset_config(2, 16, true);
    let host_a = Runtime::new(&config_a, store.clone());
    host_a.create_vset(VSET, vc);

    let mut workload = Workload::new(11, vc);
    for op in 0..150 {
        workload.step(&host_a, op);
        if op % 40 == 39 {
            assert!(host_a.guest_sync(VSET, VolumeIdx(1)));
        }
    }
    for page in 0..4 {
        host_a.guest_write(VSET, pid(0, page), 0xBBB0 + u64::from(page));
        workload
            .model
            .insert(pid(0, page), 0xBBB0 + u64::from(page));
    }
    let applied_at_ckpt = host_a.guest_applied(VSET);
    assert_eq!(host_a.checkpoint(VSET), 1);

    // Wait for backup to publish the checkpoint: the head object's
    // manifest pointer resolves to a Checkpoint-kind record.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observed = store
            .get(&layout::head_key(VSET))
            .expect("store up")
            .and_then(|(_, bytes)| HeadRecord::decode(VSET, &bytes).ok())
            .and_then(|head| head.manifest.map(|manifest| (head, manifest)))
            .and_then(|ptr| {
                store
                    .get(&layout::manifest_key(VSET, ptr.1.fence, ptr.1.seq))
                    .expect("store up")
                    .map(|(_, bytes)| (ptr.0, bytes))
            })
            .and_then(|(head, bytes)| {
                JournalRecord::decode(VSET, &bytes)
                    .ok()
                    .map(|record| (head, record))
            });
        if observed
            .as_ref()
            .is_some_and(|(_, record)| matches!(record.kind, RecordKind::Checkpoint { .. }))
        {
            break;
        }
        if Instant::now() >= deadline {
            let listing = store.s3.list_objects_v2("v/", None, 1_000);
            panic!(
                "backup never published the checkpoint; observed={observed:?}; counters={:?}; s3={}; keys={:?}; incidents={:?}",
                host_a.counters(),
                store.s3.stats.report(),
                listing.contents,
                host_a.incidents(),
            );
        }
        std::thread::park_timeout(Duration::from_millis(20));
    }
    assert_eq!(host_a.incidents(), Vec::<String>::new());

    // The bucket's shape: head, manifests, segments under the vset's
    // namespace — walked with real paginated ListObjectsV2.
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = store.s3.list_objects_v2("v/", token.as_deref(), 3);
        for (key, size, etag) in &page.contents {
            assert!(key.starts_with("v/"), "foreign key {key}");
            assert!(*size > 0);
            assert!(
                etag.starts_with('"') && etag.ends_with('"'),
                "unquoted etag"
            );
        }
        keys.extend(page.contents.into_iter().map(|(k, _, _)| k));
        match page.next_continuation_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    assert!(keys.iter().any(|k| k.ends_with("/head")));
    assert!(keys.iter().any(|k| k.contains("/m/")));
    assert!(keys.iter().any(|k| k.contains("/s/")));

    // R8.4: segment objects are the local segment files, verbatim. (Store
    // keys and local blob names differ in suffix; `layout` maps them. Old
    // local segments may have been reclaimed — at least one live pair must
    // match byte-for-byte.)
    let mut verbatim = 0;
    for key in keys.iter().filter(|k| k.contains("/s/")) {
        let Some(layout::StoreKey::Segment { vset, fence, seg }) = layout::parse_key(key) else {
            panic!("unparseable segment key {key}");
        };
        let local_path = config_a
            .blob_dir
            .join(layout::segment_blob(vset, fence, seg));
        if let Ok(local) = std::fs::read(&local_path) {
            let (_, object) = store.get(key).expect("store up").expect("exists");
            assert_eq!(object, local, "segment {key} changed in flight");
            verbatim += 1;
        }
    }
    assert!(verbatim > 0, "no live segment pair to compare");

    // ── host A dies; host B restores from the bucket alone ──────────────
    drop(host_a);
    let config_b = runtime_config("host-b", 256);
    let host_b = Runtime::new(&config_b, store.clone());
    let verdict = host_b.restore_vset(VSET, vc);
    assert_eq!(
        verdict,
        Verdict::Resume {
            epoch: blockd_core::types::Epoch(1),
            vmstate: applied_at_ckpt
        },
        "restore must resume the backed-up checkpoint"
    );
    // Every page — memory included — byte-exact, served by S3 Range gets.
    workload.verify_all(&host_b, true);

    // Liveness on B: keep working, keep checkpointing, keep backing up.
    for op in 150..200 {
        workload.step(&host_b, op);
    }
    assert!(host_b.guest_sync(VSET, VolumeIdx(2)));
    assert_eq!(host_b.checkpoint(VSET), 2);
    workload.verify_all(&host_b, true);
    assert_eq!(host_b.incidents(), Vec::<String>::new());
}

/// Regression: store round-trips must never ride the event loop. With a
/// real store's latency (~100ms here) a synchronous seam starves fault
/// resolution — guest writes trickle one per publish cycle, each spawning
/// its own record, and syncs never ack (the GCP-demo livelock). Backed
/// vset, latency-injected store: syncs must stay prompt, and captures must
/// go quiet once the guest does.
#[test]
fn backed_vset_syncs_stay_prompt_at_real_store_latency() {
    let (fake, endpoint) = FakeGcs::start();
    fake.latency_ms
        .store(100, std::sync::atomic::Ordering::SeqCst);
    let store = Arc::new(GcsStore::new(GcsConfig {
        bucket: "demo".into(),
        prefix: "blockd/".into(),
        endpoint: endpoint.clone(),
        metadata_endpoint: endpoint,
    }));
    let config = runtime_config("gcslat", 256);
    let vc = vset_config(1, 64, true);
    let rt = Runtime::new(&config, store);
    rt.create_vset(VSET, vc);

    let mut workload = Workload::new(11, vc);
    for burst in 0..3u64 {
        for op in 0..40 {
            workload.step(&rt, burst * 40 + op);
        }
        let start = Instant::now();
        assert!(rt.guest_sync(VSET, VolumeIdx(1)), "sync rejected");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "sync starved by store latency: {:?}",
            start.elapsed()
        );
    }
    workload.verify_all(&rt, true);

    // The livelock's signature was captures churning forever with an idle
    // guest. Let the backup tail drain, then demand quiescence.
    std::thread::sleep(Duration::from_millis(1500));
    let settled = rt.counters().records_written;
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        rt.counters().records_written,
        settled,
        "captures churning with an idle guest"
    );
    assert_eq!(rt.incidents(), Vec::<String>::new());
}
