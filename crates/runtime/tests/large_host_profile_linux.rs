//! Parameterized large-host runtime profile.
//!
//! This profile uses the real Linux userfaultfd, memfd mappings, disk blobs,
//! peer transport, checkpoint/base creation, and fork restore paths. It is
//! deliberately ignored by the normal suite and asserts correctness/traffic
//! shape rather than machine-specific latency.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_precision_loss)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::journal::VolumeConfig;
use blockd_core::protocol::Verdict;
use blockd_core::types::{PageId, PageNo, VolumeId};
use blockd_runtime::{
    AtomicHistogram, FaultWorkMetrics, HistogramSnapshot, Runtime, RuntimeConfig,
};
use serde_json::{Value, json};

#[path = "support/provenance.rs"]
mod provenance;
mod support;

use provenance::{Provenance, Topology, VolumeProvenance};

const DEFAULT_VOLUME_COUNT: usize = 64;
const DEFAULT_PAGES_PER_VOLUME: u32 = 256;
const DEFAULT_HOT_PAGES: u32 = 64;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_WRITE_PPM: u32 = 200_000;
const DEFAULT_LATENCY_SAMPLE_RATE: u64 = 1_024;
const DEADLINE_CHECK_INTERVAL: u16 = 256;

#[allow(clippy::struct_excessive_bools)]
struct ProfileConfig {
    artifact_dir: PathBuf,
    provenance: Provenance,
    pages: u32,
    hot_pages: u32,
    cache_pages: usize,
    runtime_shards: usize,
    prefault_hotset: bool,
    seed_shared_hotset: bool,
    measure_roots: bool,
    refault_each_access: bool,
    duration: Duration,
    write_ppm: u32,
    latency_sample_rate: u64,
    pace: Duration,
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
        let pages = env_parse("BLOCKD_PROFILE_PAGES_PER_VOLUME", DEFAULT_PAGES_PER_VOLUME)?;
        let hot_pages = env_parse("BLOCKD_PROFILE_HOT_PAGES", DEFAULT_HOT_PAGES)?;
        if hot_pages == 0 || hot_pages > pages {
            return Err(format!("hot pages must be in 1..={pages}, got {hot_pages}"));
        }
        if provenance.max_generation >= pages {
            return Err(format!(
                "lineage generation {} needs more than {pages} pages",
                provenance.max_generation
            ));
        }
        let cache_pages = env_parse(
            "BLOCKD_PROFILE_CACHE_PAGES_PER_VOLUME",
            usize::try_from(hot_pages)
                .expect("hot pages fit")
                .saturating_mul(2),
        )?;
        if cache_pages == 0 {
            return Err("cache pages per volume must be positive".to_owned());
        }
        let requested_runtime_shards = env_parse("BLOCKD_PROFILE_RUNTIME_SHARDS", 1usize)?;
        let runtime_shards = if requested_runtime_shards == 0 {
            std::thread::available_parallelism()
                .map_or(1, std::num::NonZeroUsize::get)
                .min(provenance.roots)
        } else {
            requested_runtime_shards
        };
        if runtime_shards > provenance.roots {
            return Err(format!(
                "runtime shards must be zero (auto) or in 1..={}, got {runtime_shards}",
                provenance.roots
            ));
        }
        let prefault_hotset = env_bool("BLOCKD_PROFILE_PREFAULT_HOTSET", true)?;
        let seed_shared_hotset = env_bool("BLOCKD_PROFILE_SEED_SHARED_HOTSET", false)?;
        let measure_roots = env_bool("BLOCKD_PROFILE_MEASURE_ROOTS", true)?;
        let refault_each_access = env_bool("BLOCKD_PROFILE_REFAULT_EACH_ACCESS", false)?;
        if !measure_roots && provenance.nodes.iter().all(|node| node.parent.is_none()) {
            return Err(
                "BLOCKD_PROFILE_MEASURE_ROOTS=0 requires at least one forked volume".to_owned(),
            );
        }
        let duration_secs = env_parse("BLOCKD_PROFILE_DURATION_SECS", DEFAULT_DURATION_SECS)?;
        if duration_secs == 0 {
            return Err("profile duration must be positive".to_owned());
        }
        let write_ppm = env_parse("BLOCKD_PROFILE_WRITE_PPM", DEFAULT_WRITE_PPM)?;
        if write_ppm > 1_000_000 {
            return Err(format!("write share exceeds one million: {write_ppm}"));
        }
        let latency_sample_rate = env_parse(
            "BLOCKD_PROFILE_LATENCY_SAMPLE_RATE",
            DEFAULT_LATENCY_SAMPLE_RATE,
        )?;
        if latency_sample_rate == 0 {
            return Err("latency sample rate must be positive".to_owned());
        }
        let pace_micros = env_parse("BLOCKD_PROFILE_PACE_MICROS", 0u64)?;
        let seed = env_parse("BLOCKD_PROFILE_SEED", 0x4b1d_5eed_u64)?;
        Ok(Self {
            artifact_dir,
            provenance,
            pages,
            hot_pages,
            cache_pages,
            runtime_shards,
            prefault_hotset,
            seed_shared_hotset,
            measure_roots,
            refault_each_access,
            duration: Duration::from_secs(duration_secs),
            write_ppm,
            latency_sample_rate,
            pace: Duration::from_micros(pace_micros),
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
}

fn env_bool(name: &str, default: bool) -> Result<bool, String> {
    match env_parse(name, u8::from(default))? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(format!("{name} must be 0 or 1, got {value}")),
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

fn page(volume: u64, page: u32) -> PageId {
    PageId {
        volume: VolumeId(volume),
        page: PageNo(page),
    }
}

fn marker(node: &VolumeProvenance) -> u64 {
    0xb10c_0000_0000_0000 | node.volume
}

fn shared_seed(root: u64, page_no: u32) -> u64 {
    0x5eed_0000_0000_0000 ^ root.rotate_left(19) ^ u64::from(page_no)
}

fn runtime_shard(node: &VolumeProvenance, shard_count: usize) -> usize {
    usize::try_from(node.root - 1).expect("root index fits") % shard_count
}

async fn create_fleet(runtimes: &[Arc<Runtime>], config: &ProfileConfig) {
    let volume_config = VolumeConfig::data(config.pages);
    let mut prepared_bases = BTreeSet::new();
    for node in &config.provenance.nodes {
        let runtime = &runtimes[runtime_shard(node, runtimes.len())];
        eprintln!(
            "profile setup: volume={} parent={:?} generation={}",
            node.volume, node.parent, node.generation
        );
        if let Some(parent) = node.parent {
            if prepared_bases.insert(parent) {
                eprintln!("profile setup: checkpoint parent={parent}");
                runtime.checkpoint(VolumeId(parent)).await;
                eprintln!("profile setup: retain base={parent}");
                runtime.keep_base(VolumeId(parent), parent).await;
            }
            eprintln!("profile setup: fork volume={} base={parent}", node.volume);
            let verdict = runtime
                .fork_volume(VolumeId(node.volume), volume_config, parent)
                .await;
            assert!(
                matches!(verdict, Verdict::Resume { .. }),
                "fork {} from {parent} did not resume: {verdict:?}",
                node.volume
            );
            let inherited_page = config.provenance.nodes
                [usize::try_from(parent - 1).expect("parent index fits")]
            .generation;
            eprintln!(
                "profile setup: verify inherited page volume={}",
                node.volume
            );
            let bytes = runtime
                .guest_read(VolumeId(node.volume), page(node.volume, inherited_page))
                .await;
            let inherited = u64::from_le_bytes(bytes[..8].try_into().expect("word page"));
            assert_eq!(
                inherited,
                marker(
                    &config.provenance.nodes
                        [usize::try_from(parent - 1).expect("parent index fits")]
                ),
                "fork {} did not inherit parent {parent}'s marker",
                node.volume
            );
        } else {
            eprintln!("profile setup: create root volume={}", node.volume);
            runtime
                .create_volume(VolumeId(node.volume), volume_config)
                .await;
            if config.seed_shared_hotset {
                eprintln!(
                    "profile setup: seed shared hot set volume={} pages={}",
                    node.volume, config.hot_pages
                );
                for page_no in 0..config.hot_pages {
                    runtime
                        .guest_write(
                            VolumeId(node.volume),
                            page(node.volume, page_no),
                            shared_seed(node.volume, page_no),
                        )
                        .await;
                }
            }
        }
        eprintln!("profile setup: write marker volume={}", node.volume);
        runtime
            .guest_write(
                VolumeId(node.volume),
                page(node.volume, node.generation),
                marker(node),
            )
            .await;
    }
    eprintln!("profile setup: fleet ready");
}

fn expected_hotset(config: &ProfileConfig, node: &VolumeProvenance) -> Vec<u64> {
    let mut expected = (0..config.hot_pages)
        .map(|page_no| {
            if config.seed_shared_hotset {
                shared_seed(node.root, page_no)
            } else {
                0
            }
        })
        .collect::<Vec<_>>();
    let mut ancestor = Some(node);
    while let Some(current) = ancestor {
        if current.generation < config.hot_pages {
            expected[usize::try_from(current.generation).expect("generation fits")] =
                marker(current);
        }
        ancestor = current.parent.map(|parent| {
            &config.provenance.nodes[usize::try_from(parent - 1).expect("parent index fits")]
        });
    }
    expected
}

struct WorkerResult {
    operations: u64,
    errors: u64,
    latency: HistogramSnapshot,
}

#[derive(Default)]
struct FinishState {
    completed: usize,
    released: bool,
}

#[derive(Default)]
struct FinishGate {
    state: std::sync::Mutex<FinishState>,
    changed: std::sync::Condvar,
}

impl FinishGate {
    fn arrive_and_wait(&self) {
        let mut state = self.state.lock().expect("finish gate lock");
        state.completed = state.completed.saturating_add(1);
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("finish gate wait");
        }
    }

    fn wait_for(&self, workers: usize) {
        let mut state = self.state.lock().expect("finish gate lock");
        while state.completed < workers {
            state = self.changed.wait(state).expect("finish gate wait");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("finish gate lock");
        state.released = true;
        self.changed.notify_all();
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ProcessCpuSnapshot {
    run_ns: u64,
    runqueue_wait_ns: u64,
    schedules: u64,
}

impl ProcessCpuSnapshot {
    fn capture() -> Result<Self, String> {
        let mut snapshot = Self::default();
        let entries = fs::read_dir("/proc/self/task")
            .map_err(|error| format!("read process tasks: {error}"))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("read process task entry: {error}"))?;
            let path = entry.path().join("schedstat");
            let Ok(contents) = fs::read_to_string(path) else {
                continue;
            };
            let thread = parse_schedstat(&contents)?;
            snapshot.run_ns = snapshot.run_ns.saturating_add(thread.run_ns);
            snapshot.runqueue_wait_ns = snapshot
                .runqueue_wait_ns
                .saturating_add(thread.runqueue_wait_ns);
            snapshot.schedules = snapshot.schedules.saturating_add(thread.schedules);
        }
        Ok(snapshot)
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            run_ns: self.run_ns.saturating_sub(earlier.run_ns),
            runqueue_wait_ns: self
                .runqueue_wait_ns
                .saturating_sub(earlier.runqueue_wait_ns),
            schedules: self.schedules.saturating_sub(earlier.schedules),
        }
    }
}

fn parse_schedstat(contents: &str) -> Result<ProcessCpuSnapshot, String> {
    let mut fields = contents.split_whitespace();
    let mut next = |name: &str| {
        fields
            .next()
            .ok_or_else(|| format!("schedstat is missing {name}"))?
            .parse::<u64>()
            .map_err(|error| format!("invalid schedstat {name}: {error}"))
    };
    Ok(ProcessCpuSnapshot {
        run_ns: next("run time")?,
        runqueue_wait_ns: next("run-queue wait")?,
        schedules: next("schedule count")?,
    })
}

fn run_worker(
    guest: &blockd_runtime::GuestAccess,
    runtime_handle: &tokio::runtime::Handle,
    config: &ProfileConfig,
    node: &VolumeProvenance,
    ready: &std::sync::Barrier,
    start: &std::sync::Barrier,
    finished: &FinishGate,
) -> WorkerResult {
    let operation = guest
        .try_begin()
        .unwrap_or_else(|| runtime_handle.block_on(guest.begin()));
    eprintln!("profile warmup: start volume={}", node.volume);
    let mut expected = expected_hotset(config, node);
    if config.prefault_hotset {
        for page_no in 0..config.hot_pages {
            let observed = operation.read_word(page(node.volume, page_no));
            assert_eq!(
                observed,
                expected[usize::try_from(page_no).expect("page index fits")],
                "initial page contents for volume {} page {page_no}",
                node.volume
            );
        }
    }
    eprintln!("profile warmup: ready volume={}", node.volume);
    ready.wait();
    start.wait();

    let deadline = Instant::now() + config.duration;
    let mut random = Lcg(config.seed ^ node.volume.rotate_left(17));
    let mut operations = 0u64;
    let mut errors = 0u64;
    let latency = AtomicHistogram::default();
    let mut deadline_checks_remaining = 0_u16;
    loop {
        if deadline_checks_remaining == 0 {
            if Instant::now() >= deadline {
                break;
            }
            deadline_checks_remaining = DEADLINE_CHECK_INTERVAL;
        }
        deadline_checks_remaining -= 1;
        let page_no =
            u32::try_from(random.next() % u64::from(config.hot_pages)).expect("hot page fits");
        let index = usize::try_from(page_no).expect("page index fits");
        let started = operations
            .is_multiple_of(config.latency_sample_rate)
            .then(Instant::now);
        if random.next() % 1_000_000 < u64::from(config.write_ppm) {
            let value = marker(node) ^ operations.rotate_left(23) ^ u64::from(page_no);
            operation.write_word(page(node.volume, page_no), value);
            expected[index] = value;
        } else {
            let observed = operation.read_word(page(node.volume, page_no));
            errors += u64::from(observed != expected[index]);
        }
        if config.refault_each_access {
            operation
                .evict_page(page(node.volume, page_no))
                .expect("evict guest PTE for refault profile");
        }
        if let Some(started) = started {
            latency.observe(started.elapsed());
        }
        operations += 1;
        if !config.pace.is_zero() {
            std::thread::sleep(config.pace);
        }
    }
    let result = WorkerResult {
        operations,
        errors,
        latency: latency.snapshot(),
    };
    drop(operation);
    finished.arrive_and_wait();
    result
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

fn fault_work_json(metrics: &FaultWorkMetrics) -> Value {
    json!({
        "queue_depth": metrics.queue_depth,
        "max_queue_depth": metrics.max_queue_depth,
        "oldest_queued_ns": metrics.oldest_queued_ns,
        "active": metrics.active,
        "max_active": metrics.max_active,
        "join_failures": metrics.join_failures,
        "timing": metrics.timing.iter().map(|series| json!({
            "operation": series.operation,
            "phase": series.phase,
            "histogram": histogram_json(&series.histogram),
        })).collect::<Vec<_>>(),
    })
}

fn runtime_snapshot(runtime: &Runtime) -> Value {
    let loop_stats = runtime.loop_stats();
    json!({
        "loop": {
            "poll": loop_stats.poll_histograms().into_iter().map(|(name, histogram)| json!({
                "operation": name,
                "histogram": histogram_json(&histogram),
            })).collect::<Vec<_>>(),
            "poll_gap": histogram_json(&loop_stats.poll_gap_histogram()),
            "world": loop_stats.world_histograms().into_iter().map(|(name, histogram)| json!({
                "operation": name,
                "histogram": histogram_json(&histogram),
            })).collect::<Vec<_>>(),
            "actor_busy_ns": loop_stats.actor_busy_ns(),
            "actor_idle_ns": loop_stats.actor_idle_ns(),
            "actor_occupancy": loop_stats.actor_occupancy(),
            "queue_depths": runtime.loop_queue_depths(),
        },
        "fault_work": fault_work_json(&runtime.fault_work_metrics()),
        "fault_latency": runtime.fault_latency().into_iter().map(|series| json!({
            "volume": series.volume.0,
            "source": series.source,
            "histogram": histogram_json(&series.histogram),
        })).collect::<Vec<_>>(),
        "operation_latency": runtime.operation_latency().into_iter().map(|series| json!({
            "operation": series.operation,
            "outcome": series.outcome,
            "histogram": histogram_json(&series.histogram),
        })).collect::<Vec<_>>(),
        "local_io_latency": runtime.local_io_latency().into_iter().map(|series| json!({
            "operation": series.operation,
            "outcome": series.outcome,
            "histogram": histogram_json(&series.histogram),
        })).collect::<Vec<_>>(),
        "local_io_in_flight": runtime.local_io_in_flight(),
        "incidents": runtime.incidents(),
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

fn write_json(path: &Path, value: &impl serde::Serialize) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize profile artifact");
    std::fs::write(path, bytes).expect("write profile artifact");
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

struct OwnedScratch(PathBuf);

impl OwnedScratch {
    fn create(path: PathBuf) -> Self {
        std::fs::create_dir(&path).expect("new dedicated profile scratch directory");
        Self(path)
    }
}

impl Drop for OwnedScratch {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!(
                "failed to remove owned profile scratch {}: {error}",
                self.0.display()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "large-host performance profile; run explicitly in release mode"]
#[allow(clippy::too_many_lines)] // one end-to-end setup, measurement, and cleanup workflow
async fn profile_volume_scale_and_fork_provenance() {
    support::local(async {
        let config = Arc::new(ProfileConfig::from_env().expect("valid profile configuration"));
        config.prepare_artifacts().expect("new artifact directory");
        write_json(
            &config.artifact_dir.join("manifest.json"),
            &json!({
                "schema": 1,
                "profile": "runtime-lineage-faults",
                "revision": std::env::var("BLOCKD_PROFILE_REVISION").ok(),
                "volume_count": config.provenance.volume_count,
                "active_volume_count": config.provenance.nodes.iter()
                    .filter(|node| config.measure_roots || node.parent.is_some())
                    .count(),
                "fork_provenance": config.provenance,
                "pages": config.pages,
                "hot_pages": config.hot_pages,
                "cache_pages": config.cache_pages,
                "runtime_shards": config.runtime_shards,
                "prefault_hotset": config.prefault_hotset,
                "seed_shared_hotset": config.seed_shared_hotset,
                "measure_roots": config.measure_roots,
                "refault_each_access": config.refault_each_access,
                "duration_secs": config.duration.as_secs(),
                "write_ppm": config.write_ppm,
                "latency_sample_rate": config.latency_sample_rate,
                "pace_micros": config.pace.as_micros(),
                "seed": config.seed,
                "cpu_list": std::env::var("BLOCKD_PROFILE_CPU_LIST").ok(),
                "detailed_profile_metrics": std::env::var_os("BLOCKD_PROFILE_DETAILED_METRICS")
                    .is_none_or(|value| value != "0"),
                "available_parallelism": std::thread::available_parallelism().map(std::num::NonZeroUsize::get).ok(),
                "pid": std::process::id(),
            }),
        );

        let scratch = OwnedScratch::create(PathBuf::from(format!(
            "/var/tmp/blockd-scratch/large-host-profile-{}",
            std::process::id()
        )));
        let test_gcs = support::test_gcs("large-host-profile").await;
        let store = test_gcs.store.clone();
        let mut runtimes = Vec::with_capacity(config.runtime_shards);
        let mut passives = Vec::with_capacity(config.runtime_shards.saturating_mul(2));
        for shard in 0..config.runtime_shards {
            let addresses = [
                support::free_addr(),
                support::free_addr(),
                support::free_addr(),
            ];
            let roots: [PathBuf; 3] = std::array::from_fn(|host| {
                scratch.0.join(format!("shard-{shard}/host-{host}"))
            });
            let mut runtime_configs: [RuntimeConfig; 3] = std::array::from_fn(|host| {
                support::three_host_runtime_config(
                    u16::try_from(host).expect("host fits"),
                    roots[host].clone(),
                    addresses,
                )
            });
            let shard_volumes = config
                .provenance
                .nodes
                .iter()
                .filter(|node| runtime_shard(node, config.runtime_shards) == shard)
                .count();
            runtime_configs[0].daemon.cache_pages = shard_volumes
                .saturating_mul(config.cache_pages)
                .max(1);
            passives.push(Runtime::new(&runtime_configs[1], store.clone()).await);
            passives.push(Runtime::new(&runtime_configs[2], store.clone()).await);
            runtimes.push(Arc::new(
                Runtime::new(&runtime_configs[0], store.clone()).await,
            ));
        }
        create_fleet(&runtimes, &config).await;

        let measured_nodes = config
            .provenance
            .nodes
            .iter()
            .filter(|node| config.measure_roots || node.parent.is_some())
            .cloned()
            .collect::<Vec<_>>();
        let active_volume_count = measured_nodes.len();
        let participants = active_volume_count.saturating_add(1);
        let ready = Arc::new(std::sync::Barrier::new(participants));
        let start = Arc::new(std::sync::Barrier::new(participants));
        let finished = Arc::new(FinishGate::default());
        let runtime_handle = tokio::runtime::Handle::current();
        let mut workers = Vec::with_capacity(active_volume_count);
        for node in measured_nodes {
            eprintln!("profile setup: spawn workload volume={}", node.volume);
            let runtime = &runtimes[runtime_shard(&node, runtimes.len())];
            let guest = runtime.guest_access(VolumeId(node.volume));
            let config = Arc::clone(&config);
            let ready = Arc::clone(&ready);
            let start = Arc::clone(&start);
            let finished = Arc::clone(&finished);
            let runtime_handle = runtime_handle.clone();
            workers.push(std::thread::spawn(move || {
                run_worker(
                    &guest,
                    &runtime_handle,
                    &config,
                    &node,
                    &ready,
                    &start,
                    &finished,
                )
            }));
        }
        eprintln!("profile setup: wait for workload warmup");
        ready.wait();
        eprintln!("profile setup: workload warmup ready");
        let before = runtimes
            .iter()
            .map(|runtime| runtime_snapshot(runtime))
            .collect::<Vec<_>>();
        let process_cpu_before =
            ProcessCpuSnapshot::capture().expect("process CPU snapshot before measurement");
        let started = Instant::now();
        eprintln!("profile measurement: start");
        start.wait();
        let diagnostics = std::env::var_os("BLOCKD_PROFILE_PROGRESS_DIAGNOSTICS").map(|_| {
            let runtime = Arc::clone(&runtimes[0]);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    eprintln!(
                        "profile progress: reader={:?} in_flight={} input={:?} fault_input={:?} work={:?} daemon={:?} incidents={:?}",
                        runtime.fault_reader_metrics(),
                        runtime.faults_in_flight(),
                        runtime.loop_queue_depths(),
                        runtime.fault_input_depths(),
                        runtime.fault_work_metrics(),
                        runtime.daemon_stats(),
                        runtime.incidents(),
                    );
                }
            })
        });
        finished.wait_for(active_volume_count);
        if let Some(diagnostics) = diagnostics {
            diagnostics.abort();
        }
        eprintln!("profile measurement: workers complete");
        let elapsed = started.elapsed();
        let process_cpu_after =
            ProcessCpuSnapshot::capture().expect("process CPU snapshot after measurement");
        let process_cpu = process_cpu_after.saturating_sub(process_cpu_before);
        let after = runtimes
            .iter()
            .map(|runtime| runtime_snapshot(runtime))
            .collect::<Vec<_>>();
        finished.release();
        let mut results = Vec::with_capacity(workers.len());
        for worker in workers {
            results.push(worker.join().expect("profile worker"));
        }

        let operations = results.iter().map(|result| result.operations).sum::<u64>();
        let errors = results.iter().map(|result| result.errors).sum::<u64>();
        assert_eq!(errors, 0, "profile observed guest-data mismatches");
        assert!(operations >= u64::try_from(active_volume_count).expect("volume count fits u64"));
        let latency = aggregate_histograms(&results);
        write_json(
            &config.artifact_dir.join("runtime.json"),
            &json!({ "before": before, "after": after }),
        );
        write_json(
            &config.artifact_dir.join("summary.json"),
            &json!({
                "volume_count": config.provenance.volume_count,
                "active_volume_count": active_volume_count,
                "provenance_topology": config.provenance.topology,
                "root_count": config.provenance.roots,
                "max_generation": config.provenance.max_generation,
                "elapsed_ns": elapsed.as_nanos(),
                "operations": operations,
                "operations_per_second": operations as f64 / elapsed.as_secs_f64(),
                "process_cpu": {
                    "run_ns": process_cpu.run_ns,
                    "runqueue_wait_ns": process_cpu.runqueue_wait_ns,
                    "schedules": process_cpu.schedules,
                    "average_cores": process_cpu.run_ns as f64 / elapsed.as_nanos() as f64,
                },
                "errors": errors,
                "operation_latency": histogram_json(&latency),
            }),
        );

        for runtime in runtimes {
            let mut runtime = Arc::try_unwrap(runtime)
                .ok()
                .expect("workers released runtime");
            runtime.shutdown().await;
        }
        drop(passives);
        drop(test_gcs);
        drop(scratch);
    })
    .await;
}

#[test]
fn shared_seed_expected_hotset_preserves_lineage_markers() {
    let provenance =
        Provenance::build(3, &Topology::Chain { max_depth: 2 }).expect("valid test provenance");
    let config = ProfileConfig {
        artifact_dir: PathBuf::from("unused"),
        provenance,
        pages: 8,
        hot_pages: 8,
        cache_pages: 8,
        runtime_shards: 1,
        prefault_hotset: false,
        seed_shared_hotset: true,
        measure_roots: false,
        refault_each_access: false,
        duration: Duration::from_secs(1),
        write_ppm: DEFAULT_WRITE_PPM,
        latency_sample_rate: DEFAULT_LATENCY_SAMPLE_RATE,
        pace: Duration::ZERO,
        seed: 1,
    };
    let node = &config.provenance.nodes[2];
    let expected = expected_hotset(&config, node);

    assert_eq!(expected[0], marker(&config.provenance.nodes[0]));
    assert_eq!(expected[1], marker(&config.provenance.nodes[1]));
    assert_eq!(expected[2], marker(&config.provenance.nodes[2]));
    assert_eq!(expected[3], shared_seed(1, 3));
}

#[test]
fn runtime_sharding_keeps_every_lineage_on_one_lane() {
    let provenance = Provenance::build(
        64,
        &Topology::Mixed {
            seed: 17,
            root_ppm: 250_000,
            max_depth: 8,
        },
    )
    .expect("valid mixed provenance");
    assert!(provenance.roots > 1);
    for node in &provenance.nodes {
        let root = &provenance.nodes[usize::try_from(node.root - 1).expect("root index fits")];
        assert_eq!(runtime_shard(node, 8), runtime_shard(root, 8));
    }
}

#[test]
fn schedstat_parser_reads_linux_thread_counters() {
    assert_eq!(
        parse_schedstat("123 456 789\n"),
        Ok(ProcessCpuSnapshot {
            run_ns: 123,
            runqueue_wait_ns: 456,
            schedules: 789,
        })
    );
    assert!(parse_schedstat("123 456").is_err());
}
