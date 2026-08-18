//! Parameterized large-host Firecracker fork profile.
//!
//! Every parent with children is snapshotted once and gets one `ShmemServer`;
//! its children map that server's file privately. This makes the recorded
//! provenance graph the physical sharing graph rather than benchmark metadata.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_precision_loss)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use blockd_runtime::fc::{FcVm, ShmemServer, rss_pss_of_pid};
use blockd_runtime::{AtomicHistogram, HistogramSnapshot};
use futures_util::future::join_all;
use serde_json::{Value, json};

#[path = "support/provenance.rs"]
mod provenance;

use provenance::{Provenance, Topology, VolumeProvenance};

const DEFAULT_VOLUME_COUNT: usize = 12;
const DEFAULT_MEM_MIB: u32 = 128;
const DEFAULT_HOT_PAGES: u32 = 1_024;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_RECLAIM_MILLIS: u64 = 1_000;

struct ProfileConfig {
    artifact_dir: PathBuf,
    provenance: Provenance,
    fc: PathBuf,
    kernel: PathBuf,
    initrd: PathBuf,
    mem_mib: u32,
    hot_pages: u32,
    duration: Duration,
    reclaim_interval: Duration,
    seed: u64,
}

impl ProfileConfig {
    fn from_env() -> Result<Self, String> {
        let artifact_dir = std::env::var_os("BLOCKD_PROFILE_ARTIFACT_DIR")
            .map(PathBuf::from)
            .ok_or_else(|| "BLOCKD_PROFILE_ARTIFACT_DIR must name a new directory".to_owned())?;
        let volume_count = env_parse("BLOCKD_PROFILE_VOLUME_COUNT", DEFAULT_VOLUME_COUNT)?;
        let topology = std::env::var("BLOCKD_PROFILE_PROVENANCE")
            .unwrap_or_else(|_| "star".to_owned())
            .parse::<Topology>()?;
        let provenance = Provenance::build(volume_count, &topology)?;
        let fc_dir = PathBuf::from(
            std::env::var("BLOCKD_FC_DIR").unwrap_or_else(|_| "/var/tmp/blockd-fc".to_owned()),
        );
        let fc = fc_dir.join("firecracker");
        let kernel = fc_dir.join("vmlinux");
        let initrd = fc_dir.join("initramfs.cpio");
        for path in [&fc, &kernel, &initrd] {
            if !path.is_file() {
                return Err(format!(
                    "missing Firecracker profile artifact: {}",
                    path.display()
                ));
            }
        }
        let mem_mib = env_parse("BLOCKD_PROFILE_FC_MEM_MIB", DEFAULT_MEM_MIB)?;
        let hot_pages = env_parse("BLOCKD_PROFILE_HOT_PAGES", DEFAULT_HOT_PAGES)?;
        if hot_pages == 0
            || u64::from(hot_pages).saturating_mul(4_096)
                > u64::from(mem_mib).saturating_mul(1_024 * 1_024)
        {
            return Err(format!(
                "hot set of {hot_pages} pages does not fit {mem_mib} MiB"
            ));
        }
        if provenance.max_generation >= hot_pages {
            return Err(format!(
                "lineage generation {} needs more than {hot_pages} hot pages",
                provenance.max_generation
            ));
        }
        let duration_secs = env_parse("BLOCKD_PROFILE_DURATION_SECS", DEFAULT_DURATION_SECS)?;
        if duration_secs == 0 {
            return Err("profile duration must be positive".to_owned());
        }
        let reclaim_millis = env_parse("BLOCKD_PROFILE_RECLAIM_MILLIS", DEFAULT_RECLAIM_MILLIS)?;
        if reclaim_millis == 0 {
            return Err("reclaim interval must be positive".to_owned());
        }
        let seed = env_parse("BLOCKD_PROFILE_SEED", 0x4b1d_5eed_u64)?;
        Ok(Self {
            artifact_dir,
            provenance,
            fc,
            kernel,
            initrd,
            mem_mib,
            hot_pages,
            duration: Duration::from_secs(duration_secs),
            reclaim_interval: Duration::from_millis(reclaim_millis),
            seed,
        })
    }

    fn prepare_artifacts(&self) -> Result<(), String> {
        let parent = self
            .artifact_dir
            .parent()
            .ok_or_else(|| "artifact directory needs an existing parent".to_owned())?;
        if !parent.is_dir() {
            return Err(format!(
                "artifact parent does not exist: {}",
                parent.display()
            ));
        }
        std::fs::create_dir(&self.artifact_dir).map_err(|error| {
            format!(
                "artifact directory must be new ({}): {error}",
                self.artifact_dir.display()
            )
        })
    }

    fn mem_bytes(&self) -> u64 {
        u64::from(self.mem_mib).saturating_mul(1_024 * 1_024)
    }
}

fn env_parse<T>(name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    value
        .to_string_lossy()
        .parse::<T>()
        .map_err(|error| format!("invalid {name}: {error}"))
}

struct OwnedScratch(PathBuf);

impl OwnedScratch {
    fn create() -> Self {
        let path = PathBuf::from(format!(
            "/var/tmp/blockd-scratch/large-host-fc-profile-{}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("new Firecracker profile scratch directory");
        Self(path)
    }
}

impl Drop for OwnedScratch {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!(
                "failed to remove owned Firecracker scratch {}: {error}",
                self.0.display()
            );
        }
    }
}

#[derive(Default)]
struct OwnedShmem(Vec<PathBuf>);

impl OwnedShmem {
    fn reserve(&mut self, parent: u64) -> PathBuf {
        let path = PathBuf::from(format!(
            "/dev/shm/blockd-large-host-{}-{parent}.shmem",
            std::process::id()
        ));
        assert!(
            !path.exists(),
            "shared-memory path already exists: {}",
            path.display()
        );
        self.0.push(path.clone());
        path
    }
}

impl Drop for OwnedShmem {
    fn drop(&mut self) {
        for path in &self.0 {
            if let Err(error) = std::fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                eprintln!("failed to remove owned shmem {}: {error}", path.display());
            }
        }
    }
}

struct BaseServer {
    parent: u64,
    server: ShmemServer,
}

async fn create_root(
    config: &ProfileConfig,
    scratch: &Path,
    node: &VolumeProvenance,
) -> (FcVm, String) {
    let mut vm = FcVm::spawn(
        &config.fc,
        &scratch.join(format!("vm-{}.sock", node.volume)),
    )
    .await;
    vm.boot(&config.kernel, &config.initrd, config.mem_mib)
        .await;
    vm.wait_line("READY").await;
    vm.cmd(
        &format!("fill {} {}", config.seed ^ node.volume, config.hot_pages),
        "FILLED ",
    )
    .await;
    let checksum = vm.cmd(&format!("sum {}", config.hot_pages), "SUM ").await;
    (vm, checksum)
}

async fn snapshot_parent(
    config: &ProfileConfig,
    scratch: &Path,
    parent: u64,
    vm: &FcVm,
    shmem: &mut OwnedShmem,
) -> (PathBuf, PathBuf, BaseServer) {
    let snapshot = scratch.join(format!("base-{parent}.vmstate"));
    let memory = scratch.join(format!("base-{parent}.mem"));
    vm.pause().await;
    vm.snapshot(&snapshot, &memory).await;
    vm.resume().await;
    let socket = scratch.join(format!("base-{parent}.uffd.sock"));
    let listener = tokio::net::UnixListener::bind(&socket).expect("bind shmem server");
    let shmem_path = shmem.reserve(parent);
    let server = ShmemServer::start(listener, memory, &shmem_path, config.mem_bytes()).await;
    (snapshot, shmem_path, BaseServer { parent, server })
}

async fn create_fleet(
    config: &ProfileConfig,
    scratch: &Path,
    shmem: &mut OwnedShmem,
) -> (BTreeMap<u64, FcVm>, Vec<BaseServer>, BTreeMap<u64, String>) {
    let mut vms = BTreeMap::new();
    let mut checksums = BTreeMap::new();
    let mut bases = BTreeMap::<u64, (PathBuf, PathBuf)>::new();
    let mut servers = Vec::new();
    for node in &config.provenance.nodes {
        let (mut vm, inherited_checksum) = if let Some(parent) = node.parent {
            #[allow(clippy::map_entry)] // server construction awaits before the value can exist
            if !bases.contains_key(&parent) {
                let (snapshot, shmem_path, server) =
                    snapshot_parent(config, scratch, parent, &vms[&parent], shmem).await;
                bases.insert(parent, (snapshot, shmem_path));
                servers.push(server);
            }
            let (snapshot, shmem_path) = &bases[&parent];
            let mut child = FcVm::spawn(
                &config.fc,
                &scratch.join(format!("vm-{}.sock", node.volume)),
            )
            .await;
            child
                .load_snapshot_shmem(
                    snapshot,
                    &scratch.join(format!("base-{parent}.uffd.sock")),
                    shmem_path,
                )
                .await;
            child.cmd("ping", "PONG").await;
            let inherited = child
                .cmd(&format!("sum {}", config.hot_pages), "SUM ")
                .await;
            assert_eq!(
                inherited, checksums[&parent],
                "fork {} did not inherit parent {parent}",
                node.volume
            );
            (child, inherited)
        } else {
            create_root(config, scratch, node).await
        };
        let marker_page = node.generation;
        vm.cmd(
            &format!(
                "mark {marker_page} {}",
                0xb10c_0000_0000_0000_u64 | node.volume
            ),
            "MARKED",
        )
        .await;
        let checksum = vm.cmd(&format!("sum {}", config.hot_pages), "SUM ").await;
        assert_ne!(
            checksum, inherited_checksum,
            "volume {} did not diverge",
            node.volume
        );
        checksums.insert(node.volume, checksum);
        vms.insert(node.volume, vm);
    }
    (vms, servers, checksums)
}

struct WorkerResult {
    volume: u64,
    vm: FcVm,
    operations: u64,
    errors: u64,
    latency: HistogramSnapshot,
}

async fn run_worker(
    volume: u64,
    mut vm: FcVm,
    mut checksum: String,
    hot_pages: u32,
    start_at: Instant,
    deadline: Instant,
) -> WorkerResult {
    tokio::time::sleep_until(start_at.into()).await;
    let mut operations = 0u64;
    let mut errors = 0u64;
    let latency = AtomicHistogram::default();
    while Instant::now() < deadline {
        let started = Instant::now();
        if operations.is_multiple_of(16) {
            let page = u32::try_from(operations / 16 % u64::from(hot_pages)).expect("page fits");
            vm.cmd(
                &format!("mark {page} {}", volume ^ operations.rotate_left(11)),
                "MARKED",
            )
            .await;
            checksum = vm.cmd(&format!("sum {hot_pages}"), "SUM ").await;
        } else {
            let observed = vm.cmd(&format!("sum {hot_pages}"), "SUM ").await;
            errors += u64::from(observed != checksum);
        }
        latency.observe(started.elapsed());
        operations += 1;
    }
    WorkerResult {
        volume,
        vm,
        operations,
        errors,
        latency: latency.snapshot(),
    }
}

fn histogram_json(histogram: &HistogramSnapshot) -> Value {
    json!({
        "buckets": histogram.buckets,
        "count": histogram.count,
        "sum_ns": histogram.sum_ns,
        "p50_upper_ns": histogram.quantile_upper_ns(50, 100),
        "p90_upper_ns": histogram.quantile_upper_ns(90, 100),
        "p99_upper_ns": histogram.quantile_upper_ns(99, 100),
        "p999_upper_ns": histogram.quantile_upper_ns(999, 1000),
        "max_ns": histogram.max_ns,
    })
}

fn aggregate_histograms(results: &[WorkerResult]) -> HistogramSnapshot {
    let mut aggregate = HistogramSnapshot {
        buckets: vec![0; blockd_runtime::LATENCY_BUCKETS_NS.len()],
        count: 0,
        sum_ns: 0,
        max_ns: 0,
    };
    for result in results {
        for (total, count) in aggregate.buckets.iter_mut().zip(&result.latency.buckets) {
            *total = total.saturating_add(*count);
        }
        aggregate.count = aggregate.count.saturating_add(result.latency.count);
        aggregate.sum_ns = aggregate.sum_ns.saturating_add(result.latency.sum_ns);
        aggregate.max_ns = aggregate.max_ns.max(result.latency.max_ns);
    }
    aggregate
}

fn server_snapshot(servers: &[BaseServer]) -> Vec<Value> {
    servers
        .iter()
        .map(|base| {
            json!({
                "parent": base.parent,
                "source": base.server.source(),
                "faults": base.server.faults.load(std::sync::atomic::Ordering::Relaxed),
                "filled": base.server.filled(),
                "fault_latency": histogram_json(&base.server.fault_latency()),
            })
        })
        .collect()
}

fn write_json(path: &Path, value: &impl serde::Serialize) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize profile artifact");
    std::fs::write(path, bytes).expect("write profile artifact");
}

#[tokio::test]
#[ignore = "large-host Firecracker profile; run explicitly in release mode"]
#[allow(clippy::too_many_lines)] // one end-to-end setup, measurement, and cleanup workflow
async fn profile_firecracker_scale_and_fork_provenance() {
    let config = ProfileConfig::from_env().expect("valid Firecracker profile configuration");
    config.prepare_artifacts().expect("new artifact directory");
    write_json(
        &config.artifact_dir.join("manifest.json"),
        &json!({
            "schema": 1,
            "profile": "firecracker-shared-mapping-lineage",
            "revision": std::env::var("BLOCKD_PROFILE_REVISION").ok(),
            "volume_count": config.provenance.volume_count,
            "fork_provenance": config.provenance,
            "mem_mib": config.mem_mib,
            "hot_pages": config.hot_pages,
            "duration_secs": config.duration.as_secs(),
            "reclaim_millis": config.reclaim_interval.as_millis(),
            "seed": config.seed,
            "cpu_list": std::env::var("BLOCKD_PROFILE_CPU_LIST").ok(),
            "detailed_profile_metrics": std::env::var_os("BLOCKD_PROFILE_DETAILED_METRICS")
                .is_none_or(|value| value != "0"),
            "available_parallelism": std::thread::available_parallelism().map(std::num::NonZeroUsize::get).ok(),
            "pid": std::process::id(),
        }),
    );

    let scratch = OwnedScratch::create();
    let mut shmem = OwnedShmem::default();
    let (vms, servers, checksums) = create_fleet(&config, &scratch.0, &mut shmem).await;
    let before = server_snapshot(&servers);
    let start_at = Instant::now() + Duration::from_millis(250);
    let deadline = start_at + config.duration;
    let workers = vms
        .into_iter()
        .map(|(volume, vm)| {
            run_worker(
                volume,
                vm,
                checksums[&volume].clone(),
                config.hot_pages,
                start_at,
                deadline,
            )
        })
        .collect::<Vec<_>>();
    let reclaim = async {
        tokio::time::sleep_until((start_at + config.reclaim_interval).into()).await;
        while Instant::now() < deadline {
            for base in &servers {
                base.server.reclaim_all(config.mem_bytes()).await;
            }
            tokio::time::sleep(config.reclaim_interval).await;
        }
    };
    let (results, ()) = tokio::join!(join_all(workers), reclaim);
    let after = server_snapshot(&servers);

    let operations = results.iter().map(|result| result.operations).sum::<u64>();
    let errors = results.iter().map(|result| result.errors).sum::<u64>();
    assert_eq!(
        errors, 0,
        "Firecracker profile observed checksum mismatches"
    );
    assert!(
        operations >= u64::try_from(config.provenance.volume_count).expect("volume count fits")
    );
    let latency = aggregate_histograms(&results);
    let mut process_memory = Vec::with_capacity(results.len());
    for result in &results {
        let (rss, pss) = rss_pss_of_pid(result.vm.pid()).await;
        process_memory.push(json!({ "volume": result.volume, "rss": rss, "pss": pss }));
    }
    let base_resident = join_all(servers.iter().map(|base| base.server.resident_bytes()))
        .await
        .into_iter()
        .sum::<usize>();
    write_json(
        &config.artifact_dir.join("runtime.json"),
        &json!({
            "servers_before": before,
            "servers_after": after,
            "process_memory": process_memory,
            "base_resident_bytes": base_resident,
        }),
    );
    write_json(
        &config.artifact_dir.join("summary.json"),
        &json!({
            "volume_count": config.provenance.volume_count,
            "provenance_topology": config.provenance.topology,
            "root_count": config.provenance.roots,
            "max_generation": config.provenance.max_generation,
            "operations": operations,
            "operations_per_second": operations as f64 / config.duration.as_secs_f64(),
            "errors": errors,
            "operation_latency": histogram_json(&latency),
            "base_resident_bytes": base_resident,
        }),
    );

    for result in results {
        result.vm.kill().await;
    }
    drop(servers);
    drop(shmem);
    drop(scratch);
}
