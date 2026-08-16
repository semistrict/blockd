//! Cross-sandbox interference through the one event loop: K noisy vsets
//! dirtying memory as fast as they can, one probe vset doing paced ops,
//! REAL uffd + REAL disk blobs (fsync included). The probe's latency as K
//! grows IS the number the single-loop architecture must answer for; the
//! loop-stats attribution shows the remaining on-loop dispatch cost after
//! fill completion and blob I/O move to worker lanes.
//!
//! Profiles print to stderr (`--no-capture`); assertions pin the shape
//! (the traffic actually flowed), not machine-dependent microseconds.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_precision_loss)] // presentation math

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use blockd_core::journal::VsetConfig;
use blockd_core::types::{PageId, PageNo, VolumeId, VolumeIdx, VsetId};
use blockd_runtime::{Runtime, RuntimeConfig};

mod support;

const NOISY_FLEETS: [usize; 4] = [0, 4, 16, 48];
const PAGES_PER_VOLUME: u32 = 2048; // 8 MiB working set per vset
const PHASE: Duration = Duration::from_secs(3);
const PROBE_PACE: Duration = Duration::from_millis(2);
const PROBE_VSET: VsetId = VsetId(1000);

fn pid(vset: VsetId, page: u32) -> PageId {
    PageId {
        volume: VolumeId {
            vset,
            idx: VolumeIdx(1),
        },
        page: PageNo(page),
    }
}

/// Real disk, never tmpfs: the `BlobWrite` fsyncs on the loop must pay
/// the real price (see `fc_e2e_linux.rs`).
fn scratch_dir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(format!(
        "/var/tmp/blockd-scratch/loopbench-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn vset_config() -> VsetConfig {
    VsetConfig::compute(1, PAGES_PER_VOLUME)
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    sorted[(sorted.len() - 1) * pct / 100]
}

struct PhaseResult {
    probe_micros: Vec<u64>,
    noisy_ops: u64,
    occupancy: f64,
    fills: u64,
    fill_ns: u64,
    blob_writes: u64,
    report: String,
}

fn operation_stat(rows: &[(&'static str, u64, u64)], name: &str) -> (u64, u64) {
    rows.iter()
        .find(|(row, _, _)| *row == name)
        .map_or((0, 0), |(_, count, ns)| (*count, *ns))
}

async fn run_phase(noisy: usize) -> PhaseResult {
    let addresses = [
        support::free_addr(),
        support::free_addr(),
        support::free_addr(),
    ];
    let roots: [PathBuf; 3] = std::array::from_fn(|host| scratch_dir(&format!("k{noisy}-h{host}")));
    let mut configs: [RuntimeConfig; 3] = std::array::from_fn(|host| {
        support::three_host_runtime_config(
            u16::try_from(host).expect("host fits"),
            roots[host].clone(),
            addresses,
        )
    });
    configs[0].daemon.cache_pages = 1 << 20; // isolate loop contention, not eviction pressure
    let test_gcs = support::test_gcs(&format!("loop-{noisy}")).await;
    let store = test_gcs.store.clone();
    let passives = vec![
        Runtime::new(&configs[1], store.clone()).await,
        Runtime::new(&configs[2], store.clone()).await,
    ];
    let rt = Arc::new(Runtime::new(&configs[0], store).await);
    for n in 0..noisy {
        rt.create_vset(VsetId(u64::try_from(n).expect("fits") + 1), vset_config())
            .await;
    }
    rt.create_vset(PROBE_VSET, vset_config()).await;

    let stop = Arc::new(AtomicBool::new(false));
    let noisy_ops = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for n in 0..noisy {
        let rt = rt.clone();
        let stop = stop.clone();
        let noisy_ops = noisy_ops.clone();
        let vset = VsetId(u64::try_from(n).expect("fits") + 1);
        workers.push(tokio::task::spawn_local(async move {
            let mut lcg = Lcg(0x9e37 + u64::try_from(n).expect("fits"));
            let mut op = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let page = u32::try_from(lcg.next() % u64::from(PAGES_PER_VOLUME)).expect("fits");
                rt.guest_write(vset, pid(vset, page), 0x6000_0000 + op)
                    .await;
                op += 1;
            }
            noisy_ops.fetch_add(op, Ordering::Relaxed);
        }));
    }

    // The probe: one paced sandbox measuring end-to-end guest-op latency
    // (each op is a write + a whole-page read through the fault path).
    let probe = {
        let rt = rt.clone();
        let stop = stop.clone();
        tokio::task::spawn_local(async move {
            let mut lcg = Lcg(7);
            let mut micros = Vec::new();
            let mut op = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let page = u32::try_from(lcg.next() % u64::from(PAGES_PER_VOLUME)).expect("fits");
                let started = Instant::now();
                rt.guest_write(PROBE_VSET, pid(PROBE_VSET, page), 0x7000_0000 + op)
                    .await;
                let _ = rt.guest_read(PROBE_VSET, pid(PROBE_VSET, page)).await;
                micros.push(u64::try_from(started.elapsed().as_micros()).expect("fits"));
                op += 1;
                tokio::time::sleep(PROBE_PACE).await;
            }
            micros
        })
    };

    tokio::time::sleep(PHASE).await;
    stop.store(true, Ordering::Relaxed);
    for worker in workers {
        worker.await.expect("noisy worker");
    }
    let probe_micros = probe.await.expect("probe");

    let stats = rt.loop_stats();
    let world_operations = stats.world_totals();
    let (fills, fill_ns) = operation_stat(&world_operations, "Fill");
    let (blob_writes, _) = operation_stat(&world_operations, "BlobWrite");
    let result = PhaseResult {
        probe_micros,
        noisy_ops: noisy_ops.load(Ordering::Relaxed),
        occupancy: stats.occupancy(),
        fills,
        fill_ns,
        blob_writes,
        report: stats.report(),
    };
    drop(rt);
    drop(passives);
    for root in roots {
        std::fs::remove_dir_all(root).expect("cleanup runtime root");
    }
    result
}

#[tokio::test(flavor = "current_thread")]
async fn profile_probe_latency_under_noisy_neighbors() {
    support::local(async {
        eprintln!("── PROFILE: probe guest-op latency vs noisy-neighbor count ──");
        for noisy in NOISY_FLEETS {
            let result = run_phase(noisy).await;
            let mut sorted = result.probe_micros.clone();
            sorted.sort_unstable();
            // At 48 noisy vsets today the probe barely moves (~56 ops in 3s,
            // ~50ms/op against a 2ms pace) — the collapse is the finding, and
            // the printed numbers carry it; the floor only guards vacuity.
            assert!(
                sorted.len() >= 20,
                "probe managed only {} ops under {noisy} noisy vsets",
                sorted.len()
            );
            eprintln!(
                "  {noisy:>2} noisy: probe {} ops  p50 {}µs  p90 {}µs  p99 {}µs  max {}µs   \
             noisy {:.0} ops/s  loop occupancy {:.1}%  fill dispatch {:.1}µs/op",
                sorted.len(),
                percentile(&sorted, 50),
                percentile(&sorted, 90),
                percentile(&sorted, 99),
                percentile(&sorted, 100),
                result.noisy_ops as f64 / PHASE.as_secs_f64(),
                result.occupancy * 100.0,
                result.fill_ns as f64 / result.fills.max(1) as f64 / 1_000.0,
            );
            eprint!("{}", result.report);

            // Shape: the traffic this profile claims to measure actually flowed.
            assert!(result.fills > 0, "no fills reached the loop");
            assert!(result.blob_writes > 0, "writeback never wrote a blob");
            if noisy > 0 {
                assert!(result.noisy_ops > 0, "noisy fleet did no work");
            }
        }
    })
    .await;
}
