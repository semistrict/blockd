#[cfg(target_os = "linux")]
use std::future::Future;
#[cfg(target_os = "linux")]
use std::net::{IpAddr, SocketAddr};
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::RwLock;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(target_os = "linux")]
use axum::http::StatusCode;
#[cfg(target_os = "linux")]
use axum::response::IntoResponse as _;
#[cfg(target_os = "linux")]
use axum::{Router, routing::get};
#[cfg(target_os = "linux")]
use blockd_core::hostmeta::{
    ArchivePolicy, AuthorityHostConfig, ClusterPlacementConfig, Counters, DaemonStats, HostConfig,
    ReplicaSpoolMetrics, ReplicaVolumeMetrics,
};
#[cfg(target_os = "linux")]
use blockd_core::types::HostId;
#[cfg(target_os = "linux")]
use blockd_runtime::cluster::{GcsStoreUri, NodeIdentity, bootstrap};
#[cfg(target_os = "linux")]
use blockd_runtime::{
    FaultLatency, GcsConfig, GcsStore, ObjectStore, PeerConfig, PeerResourceMetrics, Runtime,
    RuntimeConfig, RuntimeReadiness, RuntimeStartupError,
};
#[cfg(target_os = "linux")]
use tracing::Instrument as _;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("blockd is Linux-only");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
struct ServeArgs {
    store: GcsStoreUri,
    gcs_endpoint: String,
    metadata_endpoint: String,
    data_dir: PathBuf,
    peer: Option<SocketAddr>,
    health: SocketAddr,
    capacity_bytes: u64,
    headroom_bytes: u64,
    firecracker: PathBuf,
    firecracker_sha256: [u8; 32],
    control: PathBuf,
    test_control: bool,
    #[cfg(test)]
    drain_hook: Option<DrainTestHook>,
    #[cfg(test)]
    drain_timeout: Option<std::time::Duration>,
    #[cfg(test)]
    metrics_render_hook: Option<MetricsRenderTestHook>,
}

#[cfg(all(target_os = "linux", test))]
#[derive(Clone, Default)]
struct DrainTestHook {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(all(target_os = "linux", test))]
#[derive(Clone, Default)]
struct MetricsRenderTestHook {
    armed: Arc<AtomicBool>,
    entered: Arc<AtomicBool>,
    release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

#[cfg(all(target_os = "linux", test))]
impl MetricsRenderTestHook {
    fn armed() -> Self {
        Self {
            armed: Arc::new(AtomicBool::new(true)),
            ..Self::default()
        }
    }

    fn hold_once(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        self.entered.store(true, Ordering::SeqCst);
        let (released, wake) = &*self.release;
        let mut released = released.lock().expect("metrics render release lock");
        while !*released {
            released = wake.wait(released).expect("metrics render release wait");
        }
    }

    fn release(&self) {
        let (released, wake) = &*self.release;
        *released.lock().expect("metrics render release lock") = true;
        wake.notify_all();
    }
}

#[cfg(target_os = "linux")]
struct HealthState {
    snapshot: RwLock<HealthSnapshot>,
    metrics_expositions: AtomicU64,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, PartialEq, Eq)]
struct HealthSnapshot {
    ready: bool,
    identity_current: bool,
    store_access: bool,
    phase: u8,
    diagnostic: String,
}

#[cfg(target_os = "linux")]
impl HealthState {
    fn new(snapshot: HealthSnapshot) -> Self {
        Self {
            snapshot: RwLock::new(snapshot),
            metrics_expositions: AtomicU64::new(0),
        }
    }

    fn publish(&self, snapshot: HealthSnapshot) {
        *self.snapshot.write().expect("health snapshot lock") = snapshot;
    }

    fn snapshot(&self) -> HealthSnapshot {
        self.snapshot.read().expect("health snapshot lock").clone()
    }
}

#[cfg(target_os = "linux")]
type MetricsRequest = tokio::sync::oneshot::Sender<bytes::Bytes>;

#[cfg(target_os = "linux")]
struct PrometheusSnapshot {
    ready: bool,
    identity_current: bool,
    store_access: bool,
    phase: u8,
    metrics_expositions: u64,
    readiness: RuntimeReadiness,
    counters: Counters,
    peer: PeerResourceMetrics,
    daemon: DaemonStats,
    replica_metrics: Vec<ReplicaVolumeMetrics>,
    spool_metrics: Vec<ReplicaSpoolMetrics>,
    spool_capacity_bytes: u64,
    fault_latency: Vec<FaultLatency>,
}

#[cfg(target_os = "linux")]
impl PrometheusSnapshot {
    fn capture(runtime: &Runtime, state: &HealthState) -> Self {
        let peer = runtime.peer_resource_metrics();
        let health = state.snapshot();
        Self {
            ready: health.ready,
            identity_current: health.identity_current,
            store_access: health.store_access,
            phase: health.phase,
            metrics_expositions: state.metrics_expositions.load(Ordering::Relaxed),
            readiness: runtime.readiness(),
            counters: runtime.counters(),
            peer,
            daemon: runtime.daemon_stats(),
            replica_metrics: runtime.replica_metrics(),
            spool_metrics: runtime.replica_spool_metrics(),
            spool_capacity_bytes: runtime.replica_spool_capacity_bytes(),
            fault_latency: runtime.fault_latency(),
        }
    }
}

#[cfg(target_os = "linux")]
const PHASE_RECOVERING: u8 = 0;
#[cfg(target_os = "linux")]
const PHASE_READY: u8 = 1;
#[cfg(target_os = "linux")]
const PHASE_DRAINING: u8 = 2;
#[cfg(target_os = "linux")]
const PHASE_FENCED: u8 = 3;
/// Joined membership with startup recovery complete, but another readiness
/// dependency (usually authority or placement) has not converged yet.
#[cfg(target_os = "linux")]
const PHASE_JOINED: u8 = 4;
#[cfg(all(target_os = "linux", test))]
const STORE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
#[cfg(all(target_os = "linux", not(test)))]
const STORE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
#[cfg(all(target_os = "linux", test))]
const DRAIN_PROPAGATION: std::time::Duration = std::time::Duration::from_millis(100);
#[cfg(all(target_os = "linux", not(test)))]
const DRAIN_PROPAGATION: std::time::Duration = std::time::Duration::from_secs(2);
#[cfg(target_os = "linux")]
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
#[cfg(all(target_os = "linux", test))]
const METRICS_CACHE_TTL: std::time::Duration = std::time::Duration::from_millis(200);
#[cfg(all(target_os = "linux", not(test)))]
const METRICS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_lines)] // one complete bounded-cardinality metrics exposition
fn prometheus(snapshot: PrometheusSnapshot) -> String {
    use std::collections::BTreeMap;
    use std::fmt::Write as _;

    let mut output = String::new();
    let ready = u8::from(snapshot.ready);
    let phase = snapshot.phase;
    writeln!(output, "blockd_up 1").expect("string write");
    writeln!(output, "blockd_ready {ready}").expect("string write");
    writeln!(output, "blockd_lifecycle_phase {phase}").expect("string write");
    writeln!(
        output,
        "blockd_metrics_expositions_total {}",
        snapshot.metrics_expositions
    )
    .expect("string write");
    for (name, value) in [
        ("recovering", PHASE_RECOVERING),
        ("ready", PHASE_READY),
        ("draining", PHASE_DRAINING),
        ("fenced", PHASE_FENCED),
        ("joined", PHASE_JOINED),
    ] {
        writeln!(
            output,
            "blockd_lifecycle_state{{state=\"{name}\"}} {}",
            u8::from(phase == value)
        )
        .expect("string write");
    }
    let readiness = snapshot.readiness;
    for (dependency, available) in [
        ("identity", snapshot.identity_current),
        ("object_store", snapshot.store_access),
        ("membership_ownership", readiness.membership_ownership),
        ("authority", readiness.authority),
        ("recovery", readiness.recovery),
        ("placement", readiness.placement),
        ("peer_listener", readiness.peer_listener),
        ("critical_tasks", readiness.critical_tasks),
        ("unfenced", readiness.unfenced),
    ] {
        writeln!(
            output,
            "blockd_readiness_dependency{{dependency=\"{dependency}\"}} {}",
            u8::from(available)
        )
        .expect("string write");
    }
    let counters = snapshot.counters;
    let peer_resources = snapshot.peer;
    for (name, value) in [
        ("assignment_claims_total", counters.assignment_claims),
        ("fences_total", counters.fenced),
        ("store_retries_total", counters.store_retries),
        ("peer_retries_total", counters.peer_retries),
        ("integrity_rejects_total", counters.replica_rejected),
        (
            "replica_replacement_bytes_total",
            counters.replica_replacement_bytes,
        ),
        ("replica_cleanup_unlinks_total", counters.replica_unlinks),
        (
            "replica_nonactive_bytes_total",
            counters.replica_nonactive_bytes,
        ),
        (
            "replica_cleanup_rewrite_bytes_total",
            counters.replica_cleanup_rewrite_bytes,
        ),
        ("lease_self_fences_total", counters.lease_self_fences),
        (
            "peer_overload_rejections_total",
            peer_resources.overload_rejections,
        ),
        (
            "peer_outbound_worker_rejections_total",
            peer_resources.outbound_worker_rejections,
        ),
        (
            "peer_outbound_queue_rejections_total",
            peer_resources.outbound_queue_rejections,
        ),
        (
            "peer_payload_budget_waits_total",
            peer_resources.payload_budget_waits,
        ),
        (
            "peer_frame_read_timeouts_total",
            peer_resources.frame_read_timeouts,
        ),
        ("peer_idle_timeouts_total", peer_resources.idle_timeouts),
        ("dirty_pages_total", counters.guest_pages_dirtied),
        ("syncs_acked_total", counters.syncs_acked),
        ("pressure_waits_total", counters.pressure_waits),
        (
            "replica_capacity_backpressure_total",
            counters.replica_capacity_backpressure,
        ),
    ] {
        writeln!(output, "blockd_{name} {value}").expect("string write");
    }
    for (name, value) in [
        (
            "peer_outbound_active_workers",
            peer_resources.outbound_active_workers,
        ),
        (
            "peer_outbound_buffered_messages",
            peer_resources.outbound_buffered_messages,
        ),
        (
            "peer_outbound_buffered_bytes",
            peer_resources.outbound_buffered_bytes,
        ),
    ] {
        writeln!(output, "blockd_{name} {value}").expect("string write");
    }
    let daemon = snapshot.daemon;
    writeln!(
        output,
        "blockd_memory_resident_pages {}",
        daemon.resident_pages
    )
    .expect("string write");
    writeln!(output, "blockd_memory_dirty_pages {}", daemon.dirty_pages).expect("string write");
    writeln!(
        output,
        "blockd_memory_capacity_pages {}",
        daemon.cache_capacity_pages
    )
    .expect("string write");
    writeln!(
        output,
        "blockd_memory_pressure_waiting_faults {}",
        daemon.pressure_waiting_faults
    )
    .expect("string write");
    writeln!(
        output,
        "blockd_disk_local_bytes {}",
        daemon.local_blob_bytes
    )
    .expect("string write");
    writeln!(
        output,
        "blockd_disk_capacity_bytes {}",
        daemon.disk_capacity_bytes.unwrap_or(0)
    )
    .expect("string write");
    writeln!(
        output,
        "blockd_disk_headroom_bytes {}",
        daemon.disk_headroom_bytes
    )
    .expect("string write");
    let volume_ids = daemon
        .volumes
        .iter()
        .map(|volume| volume.volume.0)
        .collect::<Vec<_>>();
    for volume in &daemon.volumes {
        let id = volume.volume.0;
        writeln!(
            output,
            "blockd_volume_dirty_pages{{volume_id=\"{id}\"}} {}",
            volume.dirty_pages
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_pages_dirtied_total{{volume_id=\"{id}\"}} {}",
            volume.pages_dirtied_total
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_hydration_remaining_pages{{volume_id=\"{id}\"}} {}",
            volume.hydration_remaining_pages
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_archive_lag_bytes{{volume_id=\"{id}\"}} {}",
            volume.archive_lag_bytes.unwrap_or(0)
        )
        .expect("string write");
    }
    let replica_metrics = snapshot.replica_metrics;
    let spool_metrics = snapshot.spool_metrics;
    let mut replica_accounting = BTreeMap::<u64, (u64, u64, u64)>::new();
    for volume in &volume_ids {
        replica_accounting.entry(*volume).or_default();
    }
    for metric in &replica_metrics {
        replica_accounting.insert(
            metric.volume.0,
            (
                metric.integrity_rejects,
                metric.replacement_bytes,
                metric.cleanup_unlinks,
            ),
        );
    }
    for spool in &spool_metrics {
        let accounting = replica_accounting.entry(spool.volume.0).or_default();
        accounting.0 = accounting.0.max(spool.integrity_rejects);
        accounting.1 = accounting.1.max(spool.replacement_bytes);
        accounting.2 = accounting.2.max(spool.cleanup_unlinks);
    }
    for metric in replica_metrics {
        let id = metric.volume.0;
        writeln!(
            output,
            "blockd_volume_protected_sync_lag{{volume_id=\"{id}\"}} {}",
            metric
                .local_covered_through
                .saturating_sub(metric.peer_committed_through)
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_stalled_syncs{{volume_id=\"{id}\"}} {}",
            metric.queued_syncs
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_active_peer{{volume_id=\"{id}\"}} {}",
            metric
                .active_peer
                .map_or(-1_i64, |peer| i64::from(peer.get()))
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_transition_peer{{volume_id=\"{id}\"}} {}",
            metric
                .transition_peer
                .map_or(-1_i64, |peer| i64::from(peer.get()))
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_assignment_epoch{{volume_id=\"{id}\"}} {}",
            metric.assignment_epoch.unwrap_or(0)
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_replica_retries{{volume_id=\"{id}\"}} {}",
            metric.current_retries
        )
        .expect("string write");
    }
    let mut spool_accounting = BTreeMap::<u64, (u64, u64)>::new();
    for volume in &volume_ids {
        spool_accounting.insert(*volume, (0, snapshot.spool_capacity_bytes));
    }
    for spool in spool_metrics {
        let accounting = spool_accounting.entry(spool.volume.0).or_default();
        accounting.0 = accounting.0.saturating_add(spool.stored_bytes);
        accounting.1 = accounting.1.max(spool.host_capacity_bytes);
    }
    for (volume, (stored_bytes, capacity_bytes)) in spool_accounting {
        writeln!(
            output,
            "blockd_volume_replica_spool_bytes{{volume_id=\"{volume}\"}} {stored_bytes}"
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_replica_spool_capacity_bytes{{volume_id=\"{volume}\"}} {capacity_bytes}"
        )
        .expect("string write");
    }
    for (volume, (rejects, replacement_bytes, cleanup_unlinks)) in replica_accounting {
        writeln!(
            output,
            "blockd_volume_integrity_rejects_total{{volume_id=\"{volume}\"}} {rejects}"
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_replica_replacement_bytes_total{{volume_id=\"{volume}\"}} {replacement_bytes}"
        )
        .expect("string write");
        writeln!(
            output,
            "blockd_volume_replica_cleanup_unlinks_total{{volume_id=\"{volume}\"}} {cleanup_unlinks}"
        )
        .expect("string write");
    }
    append_fault_latency(&mut output, snapshot.fault_latency).expect("string write");
    output
}

#[cfg(target_os = "linux")]
fn append_fault_latency(
    output: &mut impl std::fmt::Write,
    latencies: impl IntoIterator<Item = blockd_runtime::FaultLatency>,
) -> std::fmt::Result {
    for latency in latencies {
        let labels = format!(
            "volume_id=\"{}\",source=\"{}\"",
            latency.volume.0, latency.source
        );
        for (&boundary_ns, count) in blockd_runtime::LATENCY_BUCKETS_NS
            .iter()
            .zip(&latency.histogram.buckets)
        {
            writeln!(
                output,
                "blockd_volume_fault_latency_seconds_bucket{{{labels},le=\"{}\"}} {count}",
                std::time::Duration::from_nanos(boundary_ns).as_secs_f64()
            )?;
        }
        writeln!(
            output,
            "blockd_volume_fault_latency_seconds_bucket{{{labels},le=\"+Inf\"}} {}",
            latency.histogram.count
        )?;
        writeln!(
            output,
            "blockd_volume_fault_latency_seconds_count{{{labels}}} {}",
            latency.histogram.count
        )?;
        writeln!(
            output,
            "blockd_volume_fault_latency_seconds_sum{{{labels}}} {}",
            std::time::Duration::from_nanos(latency.histogram.sum_ns).as_secs_f64()
        )?;
    }
    Ok(())
}

#[cfg(all(target_os = "linux", not(test)))]
fn start_metrics_render(
    reply: MetricsRequest,
    snapshot: PrometheusSnapshot,
) -> tokio::task::JoinHandle<(MetricsRequest, bytes::Bytes)> {
    start_metrics_render_inner(reply, snapshot, None)
}

#[cfg(target_os = "linux")]
fn start_metrics_render_inner(
    reply: MetricsRequest,
    snapshot: PrometheusSnapshot,
    before_render: Option<Box<dyn FnOnce() + Send>>,
) -> tokio::task::JoinHandle<(MetricsRequest, bytes::Bytes)> {
    tokio::task::spawn_blocking(move || {
        if let Some(before_render) = before_render {
            before_render();
        }
        (reply, bytes::Bytes::from(prometheus(snapshot)))
    })
}

#[cfg(target_os = "linux")]
fn usage() -> ! {
    eprintln!(
        "usage: blockd serve gs://BUCKET/PREFIX --capacity-bytes BYTES --headroom-bytes BYTES --firecracker PATH --firecracker-sha256 HEX [--data-dir PATH] [--peer IP:PORT] [--health IP:PORT] [--control-socket PATH] [--gcs-endpoint URL] [--metadata-endpoint URL]"
    );
    blockd_runtime::flush_fatal_records();
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_lines)] // explicit CLI option parsing and validation remain one contract
fn parse_args() -> Result<ServeArgs, String> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("serve") {
        usage();
    }
    let store = args.next().ok_or_else(|| "missing store URI".to_owned())?;
    let mut gcs_endpoint = "https://storage.googleapis.com".to_owned();
    let mut metadata_endpoint = "http://metadata.google.internal".to_owned();
    let mut data_dir = PathBuf::from("/var/lib/blockd");
    let mut peer = None;
    let mut health = "127.0.0.1:7002".parse().expect("default health address");
    let mut capacity_bytes = None;
    let mut headroom_bytes = None;
    let mut firecracker = None;
    let mut firecracker_sha256 = None;
    let mut control = None;
    #[cfg(feature = "shipped-test-control")]
    let mut test_control = false;
    #[cfg(not(feature = "shipped-test-control"))]
    let test_control = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--data-dir" => {
                data_dir = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or_else(|| "--data-dir requires a path".to_owned())?;
            }
            "--peer" => {
                peer = Some(
                    args.next()
                        .ok_or_else(|| "--peer requires IP:PORT".to_owned())?
                        .parse()
                        .map_err(|_| "invalid --peer address".to_owned())?,
                );
            }
            "--health" => {
                health = args
                    .next()
                    .ok_or_else(|| "--health requires IP:PORT".to_owned())?
                    .parse()
                    .map_err(|_| "invalid --health address".to_owned())?;
            }
            "--capacity-bytes" => {
                capacity_bytes = Some(
                    args.next()
                        .ok_or_else(|| "--capacity-bytes requires BYTES".to_owned())?
                        .parse::<u64>()
                        .map_err(|_| "invalid --capacity-bytes".to_owned())?,
                );
            }
            "--headroom-bytes" => {
                headroom_bytes = Some(
                    args.next()
                        .ok_or_else(|| "--headroom-bytes requires BYTES".to_owned())?
                        .parse::<u64>()
                        .map_err(|_| "invalid --headroom-bytes".to_owned())?,
                );
            }
            "--firecracker" => {
                firecracker = Some(
                    args.next()
                        .map(PathBuf::from)
                        .ok_or_else(|| "--firecracker requires a path".to_owned())?,
                );
            }
            "--firecracker-sha256" => {
                firecracker_sha256 =
                    Some(parse_sha256(&args.next().ok_or_else(|| {
                        "--firecracker-sha256 requires 64 hex digits".to_owned()
                    })?)?);
            }
            "--control-socket" => {
                control = Some(
                    args.next()
                        .map(PathBuf::from)
                        .ok_or_else(|| "--control-socket requires a path".to_owned())?,
                );
            }
            "--gcs-endpoint" => {
                gcs_endpoint = args
                    .next()
                    .filter(|endpoint| !endpoint.trim().is_empty())
                    .ok_or_else(|| "--gcs-endpoint requires a URL".to_owned())?;
            }
            "--metadata-endpoint" => {
                metadata_endpoint = args
                    .next()
                    .filter(|endpoint| !endpoint.trim().is_empty())
                    .ok_or_else(|| "--metadata-endpoint requires a URL".to_owned())?;
            }
            "--enable-shipped-test-control" => {
                #[cfg(feature = "shipped-test-control")]
                {
                    test_control = true;
                }
                #[cfg(not(feature = "shipped-test-control"))]
                return Err("unknown argument: --enable-shipped-test-control".to_owned());
            }
            _ => return Err(format!("unknown argument: {flag}")),
        }
    }
    let capacity_bytes = capacity_bytes
        .filter(|capacity| *capacity > 0)
        .ok_or_else(|| "--capacity-bytes must be positive".to_owned())?;
    let headroom_bytes = headroom_bytes
        .filter(|headroom| *headroom > 0 && *headroom < capacity_bytes)
        .ok_or_else(|| "--headroom-bytes must be positive and below capacity".to_owned())?;
    let control = control.unwrap_or_else(|| data_dir.join("control.sock"));
    Ok(ServeArgs {
        store: GcsStoreUri::parse(&store).map_err(|error| error.to_string())?,
        gcs_endpoint,
        metadata_endpoint,
        data_dir,
        peer,
        health,
        capacity_bytes,
        headroom_bytes,
        firecracker: firecracker.ok_or_else(|| "--firecracker is required".to_owned())?,
        firecracker_sha256: firecracker_sha256
            .ok_or_else(|| "--firecracker-sha256 is required".to_owned())?,
        control,
        test_control,
        #[cfg(test)]
        drain_hook: None,
        #[cfg(test)]
        drain_timeout: None,
        #[cfg(test)]
        metrics_render_hook: None,
    })
}

#[cfg(target_os = "linux")]
fn parse_sha256(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("--firecracker-sha256 requires exactly 64 hex digits".to_owned());
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).expect("ASCII hex pair");
        digest[index] = u8::from_str_radix(text, 16).expect("validated hex pair");
    }
    Ok(digest)
}

#[cfg(target_os = "linux")]
struct PreparedServePaths {
    _data_dir: std::fs::File,
    _blob_dir: std::fs::File,
    _control_parent: std::fs::File,
}

#[cfg(target_os = "linux")]
fn descriptor_path(directory: &std::fs::File) -> PathBuf {
    use std::os::fd::AsRawFd as _;

    PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
}

#[cfg(target_os = "linux")]
fn duplicate_directory(directory: &std::fs::File) -> Result<std::fs::File, String> {
    rustix::io::dup(directory)
        .map(std::fs::File::from)
        .map_err(|error| format!("duplicate anchored directory: {error}"))
}

#[cfg(target_os = "linux")]
fn prepare_serve_paths(args: &mut ServeArgs) -> Result<PreparedServePaths, String> {
    use std::path::Component;

    let original_data_dir = args.data_dir.clone();
    let control_parent_path = args
        .control
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| "control socket requires an explicit parent directory".to_owned())?;
    let control_name = args
        .control
        .file_name()
        .ok_or_else(|| "control socket path has no file name".to_owned())?
        .to_owned();
    let mut control_components = Path::new(&control_name).components();
    if !matches!(control_components.next(), Some(Component::Normal(name)) if name == control_name)
        || control_components.next().is_some()
    {
        return Err("control socket name must be one path component".to_owned());
    }

    // This is the first mutating operation in shipped startup. Every path
    // component is opened relative to the preceding directory with NOFOLLOW;
    // the returned descriptor stays alive for the entire serve lifecycle.
    let data_dir = blockd_runtime::world::create_private_directory(&original_data_dir)
        .map_err(|error| format!("data directory setup failed: {error}"))?;
    let blob_dir = blockd_runtime::world::create_private_subdirectory(
        &data_dir,
        std::ffi::OsStr::new("blobs"),
    )
    .map_err(|error| format!("blob directory setup failed: {error}"))?;
    let control_parent = if control_parent_path == original_data_dir {
        duplicate_directory(&data_dir)?
    } else {
        blockd_runtime::world::create_private_directory(control_parent_path)
            .map_err(|error| format!("control directory setup failed: {error}"))?
    };

    args.data_dir = descriptor_path(&data_dir);
    args.control = descriptor_path(&control_parent).join(control_name);
    Ok(PreparedServePaths {
        _data_dir: data_dir,
        _blob_dir: blob_dir,
        _control_parent: control_parent,
    })
}

#[cfg(target_os = "linux")]
fn configured_store(args: &ServeArgs) -> Arc<GcsStore> {
    Arc::new(GcsStore::new(GcsConfig {
        bucket: args.store.bucket.clone(),
        prefix: args.store.prefix.clone(),
        endpoint: args.gcs_endpoint.clone(),
        metadata_endpoint: args.metadata_endpoint.clone(),
    }))
}

#[cfg(target_os = "linux")]
#[derive(serde::Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
enum ControlCommand {
    WritePage {
        volume: u64,
        page: u32,
        value: u64,
    },
    ReadPage {
        volume: u64,
        page: u32,
    },
    Sync {
        volume: u64,
    },
    Inventory {},
    Quarantines {},
    DiscardQuarantine {
        volume: u64,
        reason: String,
    },
    Create {
        volume: u64,
        pages: u32,
        #[serde(default)]
        kind: ControlVolumeKind,
    },
    Restore {
        volume: u64,
        pages: u32,
        #[serde(default)]
        kind: ControlVolumeKind,
    },
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum ControlVolumeKind {
    #[default]
    Data,
    Memory,
}

#[cfg(target_os = "linux")]
impl ControlVolumeKind {
    fn config(self, pages: u32) -> blockd_core::journal::VolumeConfig {
        match self {
            Self::Data => blockd_core::journal::VolumeConfig::data(pages),
            Self::Memory => blockd_core::journal::VolumeConfig::memory(pages),
        }
    }
}

#[cfg(target_os = "linux")]
impl ControlCommand {
    async fn execute(self, runtime: &Runtime, test_control: bool) -> String {
        match self {
            Self::WritePage { .. } | Self::ReadPage { .. } | Self::Sync { .. } => {
                self.execute_test_probe(runtime, test_control).await
            }
            Self::Inventory {} | Self::Quarantines {} | Self::DiscardQuarantine { .. } => {
                self.execute_inspection(runtime).await
            }
            Self::Create { .. } | Self::Restore { .. } => self.execute_lifecycle(runtime).await,
        }
    }

    async fn execute_test_probe(self, runtime: &Runtime, enabled: bool) -> String {
        use blockd_core::types::{PageId, PageNo, VolumeId};

        if !enabled {
            return "{\"error\":\"operation is unavailable on the production control protocol\"}\n"
                .to_owned();
        }
        match self {
            Self::WritePage {
                volume,
                page,
                value,
            } => {
                let volume = VolumeId(volume);
                runtime
                    .guest_write(
                        volume,
                        PageId {
                            volume,
                            page: PageNo(page),
                        },
                        value,
                    )
                    .await;
                format!("{}\n", serde_json::json!({"written": value}))
            }
            Self::ReadPage { volume, page } => {
                let volume = VolumeId(volume);
                let bytes = runtime
                    .guest_read(
                        volume,
                        PageId {
                            volume,
                            page: PageNo(page),
                        },
                    )
                    .await;
                let value = bytes
                    .get(..8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u64::from_le_bytes);
                format!("{}\n", serde_json::json!({"value": value}))
            }
            Self::Sync { volume } => {
                let synced = runtime.guest_sync(VolumeId(volume)).await;
                format!("{}\n", serde_json::json!({"synced": synced}))
            }
            _ => unreachable!("probe dispatcher receives only probe commands"),
        }
    }

    async fn execute_inspection(self, runtime: &Runtime) -> String {
        use blockd_core::types::VolumeId;

        match self {
            Self::Inventory {} => {
                let entries = runtime
                    .volume_inventory()
                    .into_iter()
                    .map(|(volume, config, quarantined)| {
                        serde_json::json!({
                            "volume": volume.0,
                            "pages": config.pages,
                            "kind": format!("{:?}", config.kind).to_lowercase(),
                            "quarantined": quarantined,
                        })
                    })
                    .collect::<Vec<_>>();
                format!("{}\n", serde_json::json!({"volumes": entries}))
            }
            Self::Quarantines {} => {
                let entries = runtime
                    .quarantines()
                    .into_iter()
                    .map(|(volume, reason)| {
                        serde_json::json!({"volume": volume.0, "reason": reason})
                    })
                    .collect::<Vec<_>>();
                format!("{}\n", serde_json::json!({"quarantines": entries}))
            }
            Self::DiscardQuarantine { volume, reason } => {
                match runtime.discard_quarantine(VolumeId(volume), &reason).await {
                    Ok(audit_id) => format!(
                        "{}\n",
                        serde_json::json!({"discarded": volume, "audit_id": audit_id})
                    ),
                    Err(error) => format!("{}\n", serde_json::json!({"error": error})),
                }
            }
            _ => unreachable!("inspection dispatcher receives only inspection commands"),
        }
    }

    async fn execute_lifecycle(self, runtime: &Runtime) -> String {
        use blockd_core::types::VolumeId;

        match self {
            Self::Create { pages: 0, .. } | Self::Restore { pages: 0, .. } => {
                "{\"error\":\"positive pages are required\"}\n".to_owned()
            }
            Self::Create {
                volume,
                pages,
                kind,
            } => {
                let volume = VolumeId(volume);
                match runtime.try_create_volume(volume, kind.config(pages)).await {
                    Ok(()) => format!("{}\n", serde_json::json!({"created": volume.0})),
                    Err(blockd_core::protocol::AdminError::Unavailable) => format!(
                        "{}\n",
                        serde_json::json!({
                            "error": "volume is not owned by this node; retry against the current authority"
                        })
                    ),
                    Err(error) => format!(
                        "{}\n",
                        serde_json::json!({"error": format!("volume creation rejected: {error:?}")})
                    ),
                }
            }
            Self::Restore {
                volume,
                pages,
                kind,
            } => {
                let volume = VolumeId(volume);
                let verdict = runtime.restore_volume(volume, kind.config(pages)).await;
                format!(
                    "{}\n",
                    serde_json::json!({"restored": volume.0, "verdict": format!("{verdict:?}")})
                )
            }
            _ => unreachable!("lifecycle dispatcher receives only lifecycle commands"),
        }
    }
}

#[cfg(target_os = "linux")]
struct ControlRequest {
    command: ControlCommand,
    reply: tokio::sync::oneshot::Sender<String>,
}

#[cfg(target_os = "linux")]
struct ControlSocketAnchor {
    parent: std::fs::File,
    name: std::ffi::OsString,
    bind_path: PathBuf,
}

#[cfg(target_os = "linux")]
impl ControlSocketAnchor {
    fn open(path: &Path) -> Result<Self, String> {
        use std::path::Component;

        let parent_path = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| "control socket requires an anchored parent".to_owned())?;
        let name = path
            .file_name()
            .ok_or_else(|| "control socket path has no file name".to_owned())?
            .to_owned();
        let mut components = Path::new(&name).components();
        if !matches!(components.next(), Some(Component::Normal(component)) if component == name)
            || components.next().is_some()
        {
            return Err("control socket name must be one path component".to_owned());
        }
        let parent = blockd_runtime::world::open_private_directory(parent_path)
            .map_err(|error| format!("open control socket parent: {error}"))?;
        let bind_path = descriptor_path(&parent).join(&name);
        Ok(Self {
            parent,
            name,
            bind_path,
        })
    }

    fn remove_existing(&self) -> Result<(), String> {
        match rustix::fs::statat(
            &self.parent,
            &self.name,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(stat)
                if (stat.st_mode & libc::S_IFMT) == libc::S_IFSOCK
                    && stat.st_nlink == 1
                    && stat.st_uid == rustix::process::geteuid().as_raw() =>
            {
                rustix::fs::unlinkat(&self.parent, &self.name, rustix::fs::AtFlags::empty())
                    .map_err(|error| format!("remove stale control socket: {error}"))?;
                self.parent
                    .sync_all()
                    .map_err(|error| format!("sync control socket parent: {error}"))?;
                Ok(())
            }
            Ok(_) => Err("control socket path exists and is not a private socket".to_owned()),
            Err(rustix::io::Errno::NOENT) => Ok(()),
            Err(error) => Err(format!("inspect control socket: {error}")),
        }
    }
}

#[cfg(target_os = "linux")]
fn remove_control_socket(path: &Path) -> Result<(), String> {
    let anchor = ControlSocketAnchor::open(path)?;
    anchor.remove_existing()
}

#[cfg(target_os = "linux")]
async fn control_listener(
    path: PathBuf,
    requests: tokio::sync::mpsc::Sender<ControlRequest>,
    accepting: Arc<AtomicBool>,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt as _;
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

    let anchor = ControlSocketAnchor::open(&path)?;
    anchor.remove_existing()?;
    let listener = tokio::net::UnixListener::bind(&anchor.bind_path)
        .map_err(|error| format!("bind control socket: {error}"))?;
    tokio::fs::set_permissions(&anchor.bind_path, std::fs::Permissions::from_mode(0o600))
        .await
        .map_err(|error| format!("secure control socket: {error}"))?;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|error| format!("accept control request: {error}"))?;
        let requests = requests.clone();
        let accepting = Arc::clone(&accepting);
        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut line = String::new();
            let mut reader = tokio::io::BufReader::new(reader).take(64 * 1024);
            let response = match reader.read_line(&mut line).await {
                Ok(0) => "{\"error\":\"empty request\"}\n".to_owned(),
                Ok(_) => match serde_json::from_str::<ControlCommand>(&line) {
                    Ok(_) if !accepting.load(Ordering::SeqCst) => {
                        "{\"error\":\"node is draining\"}\n".to_owned()
                    }
                    Ok(command) => {
                        let (reply, response) = tokio::sync::oneshot::channel();
                        if requests
                            .send(ControlRequest { command, reply })
                            .await
                            .is_err()
                        {
                            "{\"error\":\"daemon stopping\"}\n".to_owned()
                        } else {
                            response
                                .await
                                .unwrap_or_else(|_| "{\"error\":\"daemon stopping\"}\n".to_owned())
                        }
                    }
                    Err(error) => {
                        format!("{{\"error\":{}}}\n", serde_json::json!(error.to_string()))
                    }
                },
                Err(error) => format!("{{\"error\":{}}}\n", serde_json::json!(error.to_string())),
            };
            let _ = writer.write_all(response.as_bytes()).await;
        });
    }
}

#[cfg(target_os = "linux")]
async fn handle_control(runtime: &Runtime, request: ControlRequest, test_control: bool) {
    let response = request.command.execute(runtime, test_control).await;
    let _ = request.reply.send(response);
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // a diagnostic snapshot of independent prerequisites
struct HostPreflight {
    root: bool,
    xfs_dedicated_mount: bool,
    swap_disabled: bool,
    userfaultfd_features: bool,
    kvm_available: bool,
    firecracker_approved: bool,
    capacity_fits: bool,
    headroom_available: bool,
}

#[cfg(target_os = "linux")]
impl HostPreflight {
    fn validate(&self) -> Result<(), String> {
        let checks = [
            (self.root, "daemon must run as root"),
            (
                self.xfs_dedicated_mount,
                "blob data directory must be an exact, dedicated XFS mount",
            ),
            (self.swap_disabled, "swap must be disabled"),
            (
                self.userfaultfd_features,
                "kernel lacks required userfaultfd MINOR/WP features",
            ),
            (self.kvm_available, "/dev/kvm is unavailable"),
            (
                self.firecracker_approved,
                "configured Firecracker is not executable or does not match --firecracker-sha256",
            ),
            (
                self.capacity_fits,
                "configured capacity exceeds the data filesystem",
            ),
            (
                self.headroom_available,
                "configured headroom is not currently available",
            ),
        ];
        let failures = checks
            .into_iter()
            .filter_map(|(ok, message)| (!ok).then_some(message))
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!("host preflight failed: {}", failures.join("; ")))
        }
    }
}

#[cfg(target_os = "linux")]
trait PrerequisiteProbes {
    fn effective_uid_is_root(&self) -> bool;
    fn canonicalize(&self, path: &Path) -> Option<PathBuf>;
    fn mountinfo(&self) -> Option<String>;
    fn swap_disabled(&self) -> bool;
    fn userfaultfd_features(&self) -> bool;
    fn kvm_available(&self) -> bool;
    fn firecracker_approved(&self, path: &Path, expected_sha256: &[u8; 32]) -> bool;
    fn filesystem_space(&self, path: &Path) -> Option<(u64, u64)>;
}

#[cfg(target_os = "linux")]
struct ProductionProbes;

#[cfg(target_os = "linux")]
impl PrerequisiteProbes for ProductionProbes {
    fn effective_uid_is_root(&self) -> bool {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix("Uid:"))
                    .and_then(|uids| uids.split_whitespace().nth(1))
                    .and_then(|uid| uid.parse::<u32>().ok())
            })
            == Some(0)
    }

    fn canonicalize(&self, path: &Path) -> Option<PathBuf> {
        path.canonicalize().ok()
    }

    fn mountinfo(&self) -> Option<String> {
        std::fs::read_to_string("/proc/self/mountinfo").ok()
    }

    fn swap_disabled(&self) -> bool {
        std::fs::read_to_string("/proc/swaps")
            .is_ok_and(|swaps| swaps.lines().skip(1).all(|line| line.trim().is_empty()))
    }

    fn userfaultfd_features(&self) -> bool {
        blockd_hostmem::Uffd::new(
            blockd_hostmem::UffdFeatures::PAGEFAULT_FLAG_WP
                | blockd_hostmem::UffdFeatures::MINOR_SHMEM
                | blockd_hostmem::UffdFeatures::WP_HUGETLBFS_SHMEM,
        )
        .is_ok_and(|(_, features)| {
            features.has(blockd_hostmem::UffdFeatures::MINOR_SHMEM)
                && features.has(blockd_hostmem::UffdFeatures::WP_HUGETLBFS_SHMEM)
        })
    }

    fn kvm_available(&self) -> bool {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/kvm")
            .is_ok()
    }

    fn firecracker_approved(&self, path: &Path, expected_sha256: &[u8; 32]) -> bool {
        use std::io::Read as _;
        use std::os::unix::fs::PermissionsExt as _;

        let Ok(mut binary) = std::fs::File::open(path) else {
            return false;
        };
        let Ok(metadata) = binary.metadata() else {
            return false;
        };
        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            return false;
        }
        let mut context = ring::digest::Context::new(&ring::digest::SHA256);
        let mut buffer = [0_u8; 8 * 1024];
        loop {
            match binary.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => context.update(&buffer[..read]),
                Err(_) => return false,
            }
        }
        context.finish().as_ref() == expected_sha256
    }

    fn filesystem_space(&self, path: &Path) -> Option<(u64, u64)> {
        rustix::fs::statvfs(path).ok().map(|stats| {
            (
                stats.f_blocks.saturating_mul(stats.f_frsize),
                stats.f_bavail.saturating_mul(stats.f_frsize),
            )
        })
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq)]
struct MountRecord {
    device: String,
    root: PathBuf,
    mountpoint: PathBuf,
    filesystem: String,
}

#[cfg(target_os = "linux")]
fn decode_mountinfo_path(encoded: &str) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt as _;

    let mut decoded = Vec::with_capacity(encoded.len());
    let bytes = encoded.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let octal = bytes.get(index + 1..index + 4)?;
        if !octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
            return None;
        }
        let value = (octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + (octal[2] - b'0');
        decoded.push(value);
        index += 4;
    }
    Some(PathBuf::from(std::ffi::OsString::from_vec(decoded)))
}

#[cfg(target_os = "linux")]
fn parse_mountinfo(line: &str) -> Option<MountRecord> {
    let (mount, filesystem) = line.split_once(" - ")?;
    let mut fields = mount.split_whitespace();
    let _mount_id = fields.next()?;
    let _parent_id = fields.next()?;
    let device = fields.next()?.to_owned();
    let root = decode_mountinfo_path(fields.next()?)?;
    let mountpoint = decode_mountinfo_path(fields.next()?)?;
    let filesystem = filesystem.split_whitespace().next()?.to_owned();
    Some(MountRecord {
        device,
        root,
        mountpoint,
        filesystem,
    })
}

#[cfg(target_os = "linux")]
fn dedicated_xfs_mount(path: &Path, mountinfo: &str) -> bool {
    let mounts = mountinfo
        .lines()
        .filter_map(parse_mountinfo)
        .collect::<Vec<_>>();
    let Some(candidate) = mounts.iter().find(|mount| mount.mountpoint == path) else {
        return false;
    };
    candidate.filesystem == "xfs"
        && candidate.root == Path::new("/")
        && candidate.mountpoint != Path::new("/")
        && mounts
            .iter()
            .filter(|mount| mount.device == candidate.device)
            .count()
            == 1
}

#[cfg(target_os = "linux")]
fn observe_host_preflight(args: &ServeArgs, probes: &impl PrerequisiteProbes) -> HostPreflight {
    let blob_dir = args.data_dir.join("blobs");
    let canonical_data_dir = probes.canonicalize(&blob_dir);
    let xfs_dedicated_mount = canonical_data_dir.as_ref().is_some_and(|path| {
        probes
            .mountinfo()
            .is_some_and(|mountinfo| dedicated_xfs_mount(path, &mountinfo))
    });
    let (capacity_fits, headroom_available) =
        probes
            .filesystem_space(&blob_dir)
            .map_or((false, false), |(total, available)| {
                (
                    args.capacity_bytes <= total,
                    args.headroom_bytes <= available,
                )
            });
    HostPreflight {
        root: probes.effective_uid_is_root(),
        xfs_dedicated_mount,
        swap_disabled: probes.swap_disabled(),
        userfaultfd_features: probes.userfaultfd_features(),
        kvm_available: probes.kvm_available(),
        firecracker_approved: probes
            .firecracker_approved(&args.firecracker, &args.firecracker_sha256),
        capacity_fits,
        headroom_available,
    }
}

#[cfg(target_os = "linux")]
fn discover_private_ip() -> Result<IpAddr, String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")
        .map_err(|error| format!("address discovery bind failed: {error}"))?;
    socket
        .connect("169.254.169.254:80")
        .map_err(|error| format!("address discovery failed: {error}"))?;
    socket
        .local_addr()
        .map(|address| address.ip())
        .map_err(|error| format!("address discovery failed: {error}"))
}

#[cfg(target_os = "linux")]
fn production_runtime_config(
    host: HostId,
    cluster_id: u64,
    data_dir: &std::path::Path,
    peer: SocketAddr,
    capacity_bytes: u64,
    headroom_bytes: u64,
) -> RuntimeConfig {
    let roster = vec![host];
    RuntimeConfig {
        cluster_id: Some(cluster_id),
        daemon: HostConfig {
            archive: ArchivePolicy::default(),
            host,
            cache_pages: 4096,
            writeback_interval: blockd_core::types::millis(10),
            backup_retry: blockd_core::types::millis(100),
            disk_capacity: Some(capacity_bytes),
            disk_headroom: headroom_bytes,
            wedge_ticks: 500,
            cluster_placement: Some(ClusterPlacementConfig {
                membership_epoch: 1,
                roster,
                authority: Some(AuthorityHostConfig {
                    cluster_id,
                    poll_interval: blockd_core::types::secs(1),
                    max_poll_staleness: blockd_core::types::secs(5),
                    challenge_interval: blockd_core::types::secs(10),
                }),
            }),
        },
        blob_dir: data_dir.join("blobs"),
        peer: Some(PeerConfig {
            listen: SocketAddr::new("0.0.0.0".parse().expect("wildcard address"), peer.port()),
            advertise: peer,
        }),
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
struct StoreProbe {
    accessible: bool,
    identity_current: bool,
}

#[cfg(target_os = "linux")]
async fn current_store_status(
    store: &Arc<dyn ObjectStore>,
    identity: &NodeIdentity,
    cluster_id: u64,
) -> StoreProbe {
    match tokio::time::timeout(
        STORE_PROBE_TIMEOUT,
        identity.remote_bindings_match(Arc::clone(store), cluster_id),
    )
    .await
    {
        Ok(Ok(identity_current)) => StoreProbe {
            accessible: true,
            identity_current,
        },
        Ok(Err(_)) | Err(_) => StoreProbe {
            accessible: false,
            identity_current: false,
        },
    }
}

#[cfg(target_os = "linux")]
fn readiness_status(
    runtime: &Runtime,
    store_access: bool,
    identity_current: bool,
) -> (bool, u8, String, RuntimeReadiness) {
    let runtime_state = runtime.readiness();
    let ready = store_access && identity_current && runtime_state.ready();
    let phase = if !runtime_state.unfenced || !runtime_state.critical_tasks {
        PHASE_FENCED
    } else if ready {
        PHASE_READY
    } else if runtime_state.recovery {
        PHASE_JOINED
    } else {
        PHASE_RECOVERING
    };
    let mut missing = Vec::new();
    if !store_access {
        missing.push("object_store");
    }
    if !identity_current {
        missing.push("identity");
    }
    if !runtime_state.authority {
        if !runtime.authority_session_ready() {
            missing.push("authority_session");
        }
        if !runtime.authority_control_ready() {
            missing.push("authority_placement");
        }
    }
    if !runtime_state.membership_ownership {
        missing.push("membership_ownership");
    }
    if !runtime_state.placement {
        missing.push("placement");
    }
    if !runtime_state.recovery {
        missing.push("recovery");
    }
    if !runtime_state.peer_listener {
        missing.push("peer_listener");
    }
    if !runtime_state.critical_tasks {
        missing.push("critical_task");
    }
    if !runtime_state.unfenced {
        missing.push("unfenced");
    }
    let diagnostic = if missing.is_empty() {
        "ready\n".to_owned()
    } else {
        format!("not ready: missing {}\n", missing.join(","))
    };
    (ready, phase, diagnostic, runtime_state)
}

#[cfg(target_os = "linux")]
async fn start_health_server(
    address: SocketAddr,
    state: Arc<HealthState>,
    metrics_tx: tokio::sync::mpsc::Sender<MetricsRequest>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| format!("health endpoint bind failed: {error}"))?;
    Ok(tokio::spawn(async move {
        let live = || async { "ok\n" };
        let ready_state = Arc::clone(&state);
        let ready = move || {
            let state = Arc::clone(&ready_state);
            async move {
                let snapshot = state.snapshot();
                let status = if snapshot.ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                (status, snapshot.diagnostic).into_response()
            }
        };
        let metrics = move || {
            let metrics_tx = metrics_tx.clone();
            async move {
                let (reply, response) = tokio::sync::oneshot::channel();
                if metrics_tx.try_send(reply).is_err() {
                    return (StatusCode::SERVICE_UNAVAILABLE, "metrics exporter busy\n")
                        .into_response();
                }
                match tokio::time::timeout(std::time::Duration::from_secs(5), response).await {
                    Ok(Ok(exposition)) => (StatusCode::OK, exposition).into_response(),
                    Ok(Err(_)) => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "metrics exporter stopped\n",
                    )
                        .into_response(),
                    Err(_) => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "metrics exporter timed out\n",
                    )
                        .into_response(),
                }
            }
        };
        let routes = Router::new()
            .route("/live", get(live))
            .route("/ready", get(ready))
            .route("/metrics", get(metrics));
        let _ = axum::serve(listener, routes).await;
    }))
}

#[cfg(target_os = "linux")]
fn runtime_startup_context(
    identity: &NodeIdentity,
    cluster_id: u64,
    peer: SocketAddr,
    error: &RuntimeStartupError,
) -> String {
    format!(
        "runtime startup failed: node_id={} cluster_id={cluster_id:016x} peer={peer}: {error}",
        identity.host.get()
    )
}

#[cfg(target_os = "linux")]
async fn transfer_serviceable_volumes(
    runtime: &Runtime,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), String> {
    for (volume, _, quarantined) in runtime.volume_inventory() {
        if quarantined {
            continue;
        }
        let (_, bytes) = Arc::clone(store)
            .get(blockd_core::layout::head_key(volume))
            .await
            .map_err(|error| format!("volume {volume:?} head read failed: {error:?}"))?
            .ok_or_else(|| format!("volume {volume:?} head disappeared during drain"))?;
        let head = blockd_core::head::HeadRecord::decode(volume, &bytes)
            .map_err(|_| format!("volume {volume:?} head is corrupt during drain"))?;
        if head.holder == runtime.host_id() {
            let destination = head
                .stash
                .map(|stash| stash.active_peer)
                .filter(|&peer| peer != runtime.host_id())
                .ok_or_else(|| {
                    format!("volume {volume:?} has no protected migration target during drain")
                })?;
            runtime
                .try_migrate_out(volume, destination)
                .await
                .map_err(|error| {
                    format!(
                        "volume {volume:?} migration to {destination:?} failed during drain: {error:?}"
                    )
                })?;
        }
        runtime.wait_volume_released(volume).await;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[allow(clippy::too_many_lines)] // startup, recovery, serving, and bounded drain are one lifecycle
async fn serve_with_store(
    args: ServeArgs,
    store: Arc<dyn ObjectStore>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    #[cfg(test)]
    let drain_hook = args.drain_hook.clone();
    #[cfg(test)]
    let metrics_render_hook = args.metrics_render_hook.clone();
    #[cfg(test)]
    let drain_timeout = args.drain_timeout.unwrap_or(DRAIN_TIMEOUT);
    #[cfg(not(test))]
    let drain_timeout = DRAIN_TIMEOUT;
    let store_binding = args.store.to_string();
    let (cluster_id, identity) = bootstrap(Arc::clone(&store), &args.data_dir, &store_binding)
        .await
        .map_err(|error| format!("cluster bootstrap failed: {error}"))?;
    let node_span = tracing::info_span!(
        "blockd.node",
        node_id = identity.host.get(),
        cluster_id = format_args!("{cluster_id:016x}"),
        host_id = identity.host.get(),
        volume_id = tracing::field::Empty,
        authority = "session",
    );
    async move {
        let peer = match args.peer {
            Some(peer) => peer,
            None => SocketAddr::new(
                discover_private_ip().map_err(|error| format!("{error}; pass --peer IP:PORT"))?,
                7001,
            ),
        };
        let config = production_runtime_config(
            identity.host,
            cluster_id,
            &args.data_dir,
            peer,
            args.capacity_bytes,
            args.headroom_bytes,
        );
        let health_state = Arc::new(HealthState::new(HealthSnapshot {
            ready: false,
            identity_current: true,
            store_access: true,
            phase: PHASE_RECOVERING,
            diagnostic: "not ready: missing recovery\n".to_owned(),
        }));
        // Expositions are deliberately rendered only in response to a scrape.
        // The bounded rendezvous prevents concurrent scrapes from multiplying
        // the per-volume formatting and memory cost.
        let (metrics_tx, mut metrics_rx) = tokio::sync::mpsc::channel(1);
        let mut health_task =
            start_health_server(args.health, Arc::clone(&health_state), metrics_tx).await?;
        let mut runtime = Runtime::new(&config, Arc::clone(&store))
            .await
            .map_err(|error| runtime_startup_context(&identity, cluster_id, peer, &error))?;
        let (initial_ready, initial_phase, initial_diagnostic, _) =
            readiness_status(&runtime, true, true);
        health_state.publish(HealthSnapshot {
            ready: initial_ready,
            identity_current: true,
            store_access: true,
            phase: initial_phase,
            diagnostic: initial_diagnostic,
        });
        let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(32);
        let control_path = args.control.clone();
        let accepting = Arc::new(AtomicBool::new(true));
        let mut control_task = tokio::spawn(control_listener(
            control_path.clone(),
            control_tx,
            Arc::clone(&accepting),
        ));
        tracing::info!(
            cluster_id = format_args!("{cluster_id:016x}"),
            host_id = identity.host.get(),
            peer = %peer,
            authority = "session",
            "joined cluster"
        );
        let mut readiness_tick = tokio::time::interval(std::time::Duration::from_secs(1));
        readiness_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut metrics_cache: Option<(tokio::time::Instant, bytes::Bytes)> = None;
        let mut metrics_render: Option<tokio::task::JoinHandle<(MetricsRequest, bytes::Bytes)>> =
            None;
        tokio::pin!(shutdown);
        let planned = loop {
            tokio::select! {
                () = &mut shutdown => break true,
                () = runtime.critical_failure() => break false,
                result = &mut health_task => {
                    tracing::error!(?result, "health server stopped unexpectedly");
                    break false;
                }
                result = &mut control_task => {
                    tracing::error!(?result, "control listener stopped unexpectedly");
                    break false;
                }
                _ = readiness_tick.tick() => {
                    let probe = current_store_status(&store, &identity, cluster_id).await;
                    let (ready, phase, diagnostic, _) =
                        readiness_status(&runtime, probe.accessible, probe.identity_current);
                    health_state.publish(HealthSnapshot {
                        ready,
                        identity_current: probe.identity_current,
                        store_access: probe.accessible,
                        phase,
                        diagnostic,
                    });
                    if probe.accessible && !probe.identity_current {
                        tracing::error!("remote cluster or node identity binding drifted");
                        break false;
                    }
                }
                request = control_rx.recv() => match request {
                    Some(request) => {
                        handle_control(&runtime, request, args.test_control).await;
                    }
                    None => break false,
                },
                request = metrics_rx.recv(), if metrics_render.is_none() => match request {
                    Some(reply) if !reply.is_closed() => {
                        if let Some((rendered_at, exposition)) = metrics_cache.as_ref()
                            && rendered_at.elapsed() < METRICS_CACHE_TTL
                        {
                            let _ = reply.send(exposition.clone());
                        } else {
                            health_state.metrics_expositions.fetch_add(1, Ordering::Relaxed);
                            let snapshot = PrometheusSnapshot::capture(&runtime, &health_state);
                            #[cfg(test)]
                            let render = {
                                let hook = metrics_render_hook.clone();
                                start_metrics_render_inner(
                                    reply,
                                    snapshot,
                                    hook.map(|hook| {
                                        Box::new(move || hook.hold_once())
                                            as Box<dyn FnOnce() + Send>
                                    }),
                                )
                            };
                            #[cfg(not(test))]
                            let render = start_metrics_render(reply, snapshot);
                            metrics_render = Some(render);
                        }
                    }
                    Some(_) => {}
                    None => break false,
                },
                rendered = async {
                    metrics_render
                        .as_mut()
                        .expect("guarded metrics renderer")
                        .await
                }, if metrics_render.is_some() => {
                    metrics_render = None;
                    if let Ok((reply, exposition)) = rendered {
                        metrics_cache = Some((tokio::time::Instant::now(), exposition.clone()));
                        let _ = reply.send(exposition);
                    }
                }
            }
        };
        if let Some(render) = metrics_render.take() {
            tokio::spawn(async move {
                if let Ok((reply, exposition)) = render.await {
                    let _ = reply.send(exposition);
                }
            });
        }
        if planned {
            accepting.store(false, Ordering::SeqCst);
            let current = health_state.snapshot();
            health_state.publish(HealthSnapshot {
                ready: false,
                phase: PHASE_DRAINING,
                diagnostic: "not ready: draining\n".to_owned(),
                ..current
            });
            health_state
                .metrics_expositions
                .fetch_add(1, Ordering::Relaxed);
            let draining_snapshot = PrometheusSnapshot::capture(&runtime, &health_state);
            #[cfg(test)]
            let terminal_metrics_hook = metrics_render_hook.clone();
            let mut draining_render = Some(tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if let Some(hook) = terminal_metrics_hook {
                    hook.hold_once();
                }
                bytes::Bytes::from(prometheus(draining_snapshot))
            }));
            let mut draining_metrics = metrics_cache.map(|(_, exposition)| exposition);
            let drain = tokio::time::timeout(drain_timeout, async {
                #[cfg(test)]
                if let Some(hook) = drain_hook {
                    hook.entered.notify_one();
                    hook.release.notified().await;
                }
                transfer_serviceable_volumes(&runtime, &store).await?;
                runtime
                    .publish_drained()
                    .await
                    .map_err(|error| format!("drain publication failed: {error:?}"))?;
                runtime.await_authority_transfer().await?;
                runtime.relinquish_authority().await?;
                tokio::time::sleep(DRAIN_PROPAGATION).await;
                runtime
                    .shutdown()
                    .await
                    .map_err(|error| format!("membership withdrawal failed: {error:?}"))?;
                Ok::<(), String>(())
            });
            tokio::pin!(drain);
            let drain = loop {
                tokio::select! {
                    biased;
                    result = &mut drain => break result,
                    rendered = async {
                        draining_render
                            .as_mut()
                            .expect("guarded terminal metrics renderer")
                            .await
                    }, if draining_render.is_some() => {
                        draining_render = None;
                        if let Ok(exposition) = rendered {
                            draining_metrics = Some(exposition);
                        }
                    },
                    request = control_rx.recv() => {
                        if let Some(request) = request {
                            let _ = request
                                .reply
                                .send("{\"error\":\"node is draining\"}\n".to_owned());
                        }
                    },
                    request = metrics_rx.recv() => {
                        if let Some(reply) = request
                            && let Some(exposition) = draining_metrics.as_ref()
                        {
                            let _ = reply.send(exposition.clone());
                        }
                    }
                }
            };
            if let Some(render) = draining_render.take() {
                render.abort();
            }
            health_task.abort();
            control_task.abort();
            let _ = remove_control_socket(&control_path);
            let _ = health_task.await;
            match drain {
                Ok(result) => result,
                Err(_) => Err("graceful drain exceeded its configured safety bound".to_owned()),
            }
        } else {
            tracing::error!(
                cluster_id = format_args!("{cluster_id:016x}"),
                host_id = identity.host.get(),
                peer = %peer,
                authority = "session",
                "critical runtime task stopped; node fenced"
            );
            let current = health_state.snapshot();
            health_state.publish(HealthSnapshot {
                ready: false,
                phase: PHASE_FENCED,
                diagnostic: "not ready: fenced\n".to_owned(),
                ..current
            });
            health_task.abort();
            control_task.abort();
            let _ = remove_control_socket(&control_path);
            let _ = health_task.await;
            Err("critical runtime task stopped; node fenced".to_owned())
        }
    }
    .instrument(node_span)
    .await
}

#[cfg(target_os = "linux")]
async fn shutdown_signal() {
    shutdown_signal_with_ready(|| {}).await;
}

#[cfg(target_os = "linux")]
async fn shutdown_signal_with_ready(ready: impl FnOnce()) {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install SIGINT handler");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    ready();
    tokio::select! {
        _ = interrupt.recv() => {},
        _ = terminate.recv() => {}
    }
}

#[cfg(target_os = "linux")]
fn fatal_exit(message: &str) -> ! {
    eprintln!("{message}");
    blockd_runtime::flush_fatal_records();
    std::process::exit(1);
}

#[cfg(target_os = "linux")]
async fn serve_entry(
    args: ServeArgs,
    store: Arc<dyn ObjectStore>,
    shutdown: impl Future<Output = ()>,
) {
    if let Err(error) = serve_with_store(args, store, shutdown).await {
        fatal_exit(&error);
    }
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(true)
        .try_init();
    let mut args = parse_args().unwrap_or_else(|error| {
        eprintln!("{error}");
        usage();
    });
    let _prepared_paths = prepare_serve_paths(&mut args).unwrap_or_else(|error| {
        eprintln!("{error}");
        blockd_runtime::flush_fatal_records();
        std::process::exit(1);
    });
    observe_host_preflight(&args, &ProductionProbes)
        .validate()
        .unwrap_or_else(|error| {
            eprintln!("{error}");
            blockd_runtime::flush_fatal_records();
            std::process::exit(1);
        });
    let store: Arc<dyn ObjectStore> = configured_store(&args);
    serve_entry(args, store, shutdown_signal()).await;
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::time::Duration;

    use super::*;
    use async_trait::async_trait;
    use blockd_core::layout::peer_membership_prefix;
    use blockd_core::placement::ClusterPlacement;
    use blockd_core::protocol::StoreFault;
    use blockd_runtime::fakegcs::FakeGcs;
    use blockd_runtime::{GcsConfig, GcsStore, GetResult, ListedObject, Runtime};
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

    struct ChildGuard {
        child: Option<tokio::process::Child>,
    }

    impl ChildGuard {
        fn spawn(command: &mut tokio::process::Command) -> std::io::Result<Self> {
            command.kill_on_drop(true);
            command.spawn().map(|child| Self { child: Some(child) })
        }

        fn id(&self) -> Option<u32> {
            self.child.as_ref().and_then(tokio::process::Child::id)
        }

        fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
            self.child.as_mut().and_then(|child| child.stdout.take())
        }

        async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
            let result = self.child.as_mut().expect("live child guard").wait().await;
            if result.is_ok() {
                self.child.take();
            }
            result
        }

        async fn wait_with_output(mut self) -> std::io::Result<std::process::Output> {
            self.child
                .take()
                .expect("live child guard")
                .wait_with_output()
                .await
        }
    }

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let Some(mut child) = self.child.take() else {
                return;
            };
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }

    fn test_config() -> RuntimeConfig {
        production_runtime_config(
            HostId::new(7),
            0x0102_0304_0506_0708,
            std::path::Path::new("/var/lib/blockd"),
            "10.0.0.7:7001".parse().expect("peer address"),
            1024 * 1024 * 1024,
            128 * 1024 * 1024,
        )
    }

    /// Regression PROD-002: the shipped daemon must participate in authority
    /// sessions so a former holder self-fences within the configured bound.
    #[test]
    fn production_config_enables_bounded_authority_fencing() {
        let config = test_config();
        let authority = config
            .daemon
            .cluster_placement
            .expect("production placement")
            .authority
            .expect("production authority must be enabled");
        assert_eq!(authority.cluster_id, 0x0102_0304_0506_0708);
        assert!(authority.poll_interval > 0);
        assert!(authority.max_poll_staleness >= authority.poll_interval);
        assert!(authority.challenge_interval > authority.max_poll_staleness);
    }

    /// Regression PROD-011: a production host must have an enforceable capacity and
    /// reserve rather than silently disabling pressure accounting.
    #[test]
    fn production_config_requires_capacity_and_headroom() {
        let config = test_config();
        let capacity = config
            .daemon
            .disk_capacity
            .expect("production disk capacity must be configured");
        assert!(capacity > 0);
        assert!(config.daemon.disk_headroom > 0);
        assert!(config.daemon.disk_headroom < capacity);
    }

    #[tokio::test]
    async fn shutdown_signal_handles_sigint_and_sigterm() {
        if std::env::var_os("BLOCKD_SIGNAL_CHILD").is_some() {
            shutdown_signal_with_ready(|| {
                use std::io::Write as _;

                println!("SIGNAL_HANDLER_READY");
                std::io::stdout().lock().flush().expect("flush readiness");
            })
            .await;
            return;
        }

        for signal in ["INT", "TERM"] {
            let executable = std::env::current_exe().expect("current test executable");
            let mut command = tokio::process::Command::new(executable);
            command
                .args([
                    "--exact",
                    "tests::shutdown_signal_handles_sigint_and_sigterm",
                    "--nocapture",
                ])
                .env("BLOCKD_SIGNAL_CHILD", signal)
                .stdout(std::process::Stdio::piped());
            let mut child = ChildGuard::spawn(&mut command).expect("spawn signal child");
            let child_stdout = child.take_stdout().expect("signal child stdout");
            let mut lines = tokio::io::BufReader::new(child_stdout).lines();
            tokio::time::timeout(Duration::from_secs(3), async {
                while let Some(line) = lines.next_line().await.expect("signal child output") {
                    if line.contains("SIGNAL_HANDLER_READY") {
                        return;
                    }
                }
                panic!("signal child stopped before installing handlers");
            })
            .await
            .expect("signal handler installation");

            let status = tokio::process::Command::new("/bin/kill")
                .args([
                    format!("-{signal}"),
                    child.id().expect("signal child pid").to_string(),
                ])
                .status()
                .await
                .expect("send real Unix signal");
            assert!(status.success(), "failed to send SIG{signal}");
            assert!(
                tokio::time::timeout(Duration::from_secs(3), child.wait())
                    .await
                    .expect("signal child exited")
                    .expect("signal child status")
                    .success(),
                "SIG{signal} did not complete the shutdown future"
            );
        }
    }

    #[derive(Clone)]
    #[allow(clippy::struct_excessive_bools)] // injectable prerequisite snapshot for matrix tests
    struct FakeProbes {
        root: bool,
        canonical: Option<PathBuf>,
        mountinfo: Option<String>,
        swap_disabled: bool,
        userfaultfd_features: bool,
        kvm_available: bool,
        firecracker_approved: bool,
        filesystem_space: Option<(u64, u64)>,
    }

    impl PrerequisiteProbes for FakeProbes {
        fn effective_uid_is_root(&self) -> bool {
            self.root
        }

        fn canonicalize(&self, _path: &Path) -> Option<PathBuf> {
            self.canonical.clone()
        }

        fn mountinfo(&self) -> Option<String> {
            self.mountinfo.clone()
        }

        fn swap_disabled(&self) -> bool {
            self.swap_disabled
        }

        fn userfaultfd_features(&self) -> bool {
            self.userfaultfd_features
        }

        fn kvm_available(&self) -> bool {
            self.kvm_available
        }

        fn firecracker_approved(&self, _path: &Path, _expected_sha256: &[u8; 32]) -> bool {
            self.firecracker_approved
        }

        fn filesystem_space(&self, _path: &Path) -> Option<(u64, u64)> {
            self.filesystem_space
        }
    }

    type BreakProbe = fn(&mut FakeProbes);

    fn valid_preflight_args() -> ServeArgs {
        ServeArgs {
            store: GcsStoreUri::parse("gs://cluster/preflight/").expect("store URI"),
            gcs_endpoint: "https://storage.googleapis.com".to_owned(),
            metadata_endpoint: "http://metadata.google.internal".to_owned(),
            data_dir: PathBuf::from("/srv/blockd"),
            peer: Some("10.0.0.1:7001".parse().expect("peer address")),
            health: "127.0.0.1:7002".parse().expect("health address"),
            capacity_bytes: 1_000,
            headroom_bytes: 100,
            firecracker: PathBuf::from("/usr/local/bin/firecracker"),
            firecracker_sha256: [7; 32],
            control: PathBuf::from("/run/blockd/control.sock"),
            test_control: false,
            drain_hook: None,
            drain_timeout: None,
            metrics_render_hook: None,
        }
    }

    fn valid_probes() -> FakeProbes {
        FakeProbes {
            root: true,
            canonical: Some(PathBuf::from("/srv/blockd/blobs")),
            mountinfo: Some(
                "36 25 259:1 / /srv/blockd/blobs rw,relatime - xfs /dev/nvme0n1 rw\n".to_owned(),
            ),
            swap_disabled: true,
            userfaultfd_features: true,
            kvm_available: true,
            firecracker_approved: true,
            filesystem_space: Some((2_000, 500)),
        }
    }

    #[test]
    fn every_host_preflight_is_independently_required() {
        let args = valid_preflight_args();
        assert_eq!(
            observe_host_preflight(&args, &valid_probes()).validate(),
            Ok(())
        );
        let cases: &[(&str, BreakProbe)] = &[
            ("root", |probe| probe.root = false),
            ("mount", |probe| probe.mountinfo = None),
            ("swap", |probe| probe.swap_disabled = false),
            ("userfaultfd", |probe| probe.userfaultfd_features = false),
            ("kvm", |probe| probe.kvm_available = false),
            ("firecracker", |probe| probe.firecracker_approved = false),
            ("capacity", |probe| {
                probe.filesystem_space = Some((999, 500));
            }),
            ("headroom", |probe| {
                probe.filesystem_space = Some((2_000, 99));
            }),
        ];
        for &(name, break_probe) in cases {
            let mut probes = valid_probes();
            break_probe(&mut probes);
            assert!(
                observe_host_preflight(&args, &probes).validate().is_err(),
                "{name} probe was not required"
            );
        }
    }

    #[test]
    fn dedicated_mount_requires_one_exact_whole_xfs_filesystem() {
        let cases = [
            (
                "exact XFS filesystem",
                "/srv/blockd",
                "36 25 259:1 / /srv/blockd rw - xfs /dev/nvme0n1 rw\n",
                true,
            ),
            (
                "XFS ancestor",
                "/srv/blockd",
                "36 25 259:1 / /srv rw - xfs /dev/nvme0n1 rw\n",
                false,
            ),
            (
                "wrong filesystem",
                "/srv/blockd",
                "36 25 259:1 / /srv/blockd rw - ext4 /dev/nvme0n1 rw\n",
                false,
            ),
            (
                "bind-mounted subtree",
                "/srv/blockd",
                "36 25 259:1 /shared/blockd /srv/blockd rw - xfs /dev/nvme0n1 rw\n",
                false,
            ),
            (
                "same device mounted elsewhere",
                "/srv/blockd",
                "36 25 259:1 / /srv/blockd rw - xfs /dev/nvme0n1 rw\n\
                 37 25 259:1 / /mnt/shared rw - xfs /dev/nvme0n1 rw\n",
                false,
            ),
            (
                "escaped mountpoint",
                "/srv/blockd data",
                "36 25 259:1 / /srv/blockd\\040data rw - xfs /dev/nvme0n1 rw\n",
                true,
            ),
            (
                "root mount",
                "/",
                "36 25 259:1 / / rw - xfs /dev/nvme0n1 rw\n",
                false,
            ),
        ];
        for (name, path, mountinfo, expected) in cases {
            assert_eq!(
                dedicated_xfs_mount(Path::new(path), mountinfo),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn firecracker_requires_the_configured_binary_digest() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().expect("temporary directory");
        let binary = root.path().join("firecracker");
        let bytes = b"approved patched Firecracker fixture";
        std::fs::write(&binary, bytes).expect("write fixture");
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
            .expect("make fixture executable");
        let expected: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, bytes)
            .as_ref()
            .try_into()
            .expect("SHA-256 width");
        assert!(ProductionProbes.firecracker_approved(&binary, &expected));
        assert!(!ProductionProbes.firecracker_approved(&binary, &[0; 32]));
    }

    #[test]
    fn firecracker_digest_parser_rejects_ambiguous_identity() {
        assert_eq!(parse_sha256(&"ab".repeat(32)), Ok([0xab; 32]));
        let invalid = [
            String::new(),
            "ab".to_owned(),
            "g0".repeat(32),
            "ab".repeat(33),
        ];
        for invalid in invalid {
            assert!(parse_sha256(&invalid).is_err(), "accepted {invalid:?}");
        }
    }

    fn free_addr() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind ephemeral address")
            .local_addr()
            .expect("ephemeral address")
    }

    struct FailMembershipStore(Arc<GcsStore>);

    #[async_trait]
    impl ObjectStore for FailMembershipStore {
        async fn put(self: Arc<Self>, key: String, bytes: Vec<u8>) -> Result<u64, StoreFault> {
            if key.starts_with(&peer_membership_prefix()) {
                return Err(StoreFault::Unavailable);
            }
            Arc::clone(&self.0).put(key, bytes).await
        }

        async fn put_cas(
            self: Arc<Self>,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, StoreFault> {
            if key.starts_with(&peer_membership_prefix()) {
                return Err(StoreFault::Unavailable);
            }
            Arc::clone(&self.0).put_cas(key, expected, bytes).await
        }

        async fn get(self: Arc<Self>, key: String) -> GetResult {
            Arc::clone(&self.0).get(key).await
        }

        async fn get_range(self: Arc<Self>, key: String, offset: u64, len: u64) -> GetResult {
            Arc::clone(&self.0).get_range(key, offset, len).await
        }

        async fn delete(self: Arc<Self>, key: String) -> Result<bool, StoreFault> {
            Arc::clone(&self.0).delete(key).await
        }

        async fn delete_cas(
            self: Arc<Self>,
            key: String,
            expected: u64,
        ) -> Result<bool, StoreFault> {
            Arc::clone(&self.0).delete_cas(key, expected).await
        }

        async fn list_prefix(self: Arc<Self>, prefix: String) -> Result<Vec<String>, StoreFault> {
            Arc::clone(&self.0).list_prefix(prefix).await
        }

        async fn list_prefix_versioned(
            self: Arc<Self>,
            prefix: String,
        ) -> Result<Vec<ListedObject>, StoreFault> {
            Arc::clone(&self.0).list_prefix_versioned(prefix).await
        }
    }

    fn subprocess_serve_args() -> (ServeArgs, Arc<GcsStore>) {
        let endpoint = std::env::var("BLOCKD_TEST_STORE_ENDPOINT").expect("store endpoint");
        let data_dir = PathBuf::from(std::env::var_os("BLOCKD_TEST_DATA_DIR").expect("data dir"));
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "serve-contract/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint.clone(),
        }));
        let args = ServeArgs {
            store: GcsStoreUri::parse("gs://cluster/serve-contract/").expect("store URI"),
            gcs_endpoint: endpoint.clone(),
            metadata_endpoint: endpoint.clone(),
            data_dir: data_dir.clone(),
            peer: Some(
                std::env::var("BLOCKD_TEST_PEER")
                    .expect("peer address")
                    .parse()
                    .expect("peer address"),
            ),
            health: std::env::var("BLOCKD_TEST_HEALTH")
                .expect("health address")
                .parse()
                .expect("health address"),
            capacity_bytes: 1024 * 1024 * 1024,
            headroom_bytes: 128 * 1024 * 1024,
            firecracker: PathBuf::from("/usr/bin/firecracker"),
            firecracker_sha256: [0; 32],
            control: data_dir.join("control.sock"),
            test_control: true,
            drain_hook: None,
            drain_timeout: None,
            metrics_render_hook: None,
        };
        (args, store)
    }

    fn subprocess_command(test: &str, args: &ServeArgs, endpoint: &str) -> tokio::process::Command {
        let mut command =
            tokio::process::Command::new(std::env::current_exe().expect("current test executable"));
        command
            .args(["--exact", test, "--nocapture"])
            .env("BLOCKD_TEST_STORE_ENDPOINT", endpoint)
            .env("BLOCKD_TEST_DATA_DIR", &args.data_dir)
            .env(
                "BLOCKD_TEST_PEER",
                args.peer.expect("configured peer").to_string(),
            )
            .env("BLOCKD_TEST_HEALTH", args.health.to_string());
        command
    }

    fn test_serve_args(endpoint: &str) -> (ServeArgs, Arc<GcsStore>, tempfile::TempDir) {
        let root = tempfile::tempdir().expect("data dir");
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "serve-contract/".to_owned(),
            endpoint: endpoint.to_owned(),
            metadata_endpoint: endpoint.to_owned(),
        }));
        let args = test_serve_args_in(root.path(), endpoint);
        (args, store, root)
    }

    fn test_serve_args_in(data_dir: &Path, endpoint: &str) -> ServeArgs {
        ServeArgs {
            store: GcsStoreUri::parse("gs://cluster/serve-contract/").expect("store URI"),
            gcs_endpoint: endpoint.to_owned(),
            metadata_endpoint: endpoint.to_owned(),
            data_dir: data_dir.to_path_buf(),
            peer: Some(free_addr()),
            health: free_addr(),
            capacity_bytes: 1024 * 1024 * 1024,
            headroom_bytes: 128 * 1024 * 1024,
            firecracker: PathBuf::from("/usr/bin/firecracker"),
            firecracker_sha256: [0; 32],
            control: data_dir.join("control.sock"),
            test_control: true,
            drain_hook: None,
            drain_timeout: None,
            metrics_render_hook: None,
        }
    }

    #[test]
    fn shipped_path_setup_rejects_intermediate_symlink_without_outside_creation() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("path root");
        let outside = tempfile::tempdir().expect("outside root");
        let safe = root.path().join("safe");
        std::fs::create_dir(&safe).expect("safe parent");
        symlink(outside.path(), safe.join("redirect")).expect("intermediate symlink");
        let data_dir = safe.join("redirect/state");
        let mut args = test_serve_args_in(&data_dir, "http://127.0.0.1:1");

        let Err(error) = prepare_serve_paths(&mut args) else {
            panic!("symlink path must be rejected");
        };
        assert!(error.contains("data directory setup failed"), "{error}");
        assert!(
            !outside.path().join("state").exists(),
            "descriptor walk created state outside its trusted hierarchy"
        );
    }

    #[tokio::test]
    async fn anchored_control_socket_ignores_parent_rename_and_substitution() {
        use std::os::unix::fs::{FileTypeExt as _, symlink};

        let root = tempfile::tempdir().expect("path root");
        let live_parent = root.path().join("live");
        let parked_parent = root.path().join("parked");
        let data_dir = live_parent.join("state");
        let mut args = test_serve_args_in(&data_dir, "http://127.0.0.1:1");
        let _prepared = prepare_serve_paths(&mut args).expect("anchored setup");

        std::fs::rename(&live_parent, &parked_parent).expect("rename original parent");
        std::fs::create_dir_all(&data_dir).expect("substitute hierarchy");
        let victim = root.path().join("outside-victim");
        std::fs::write(&victim, b"untouched").expect("outside victim");
        let substitute_socket = data_dir.join("control.sock");
        symlink(&victim, &substitute_socket).expect("socket substitution");

        let (requests, _receiver) = tokio::sync::mpsc::channel(1);
        let accepting = Arc::new(AtomicBool::new(true));
        let listener = tokio::spawn(control_listener(args.control.clone(), requests, accepting));
        let anchored_socket = parked_parent.join("state/control.sock");
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if std::fs::symlink_metadata(&anchored_socket)
                    .is_ok_and(|metadata| metadata.file_type().is_socket())
                {
                    break;
                }
                assert!(!listener.is_finished(), "control listener stopped early");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("anchored control bind");

        assert!(
            std::fs::symlink_metadata(&substitute_socket)
                .expect("substitution remains")
                .file_type()
                .is_symlink(),
            "control listener replaced the swapped hierarchy entry"
        );
        assert_eq!(
            std::fs::read(&victim).expect("victim readable"),
            b"untouched"
        );

        listener.abort();
        let _ = listener.await;
        remove_control_socket(&args.control).expect("anchored socket cleanup");
        assert!(!anchored_socket.exists(), "anchored socket was not removed");
        assert!(
            std::fs::symlink_metadata(&substitute_socket).is_ok(),
            "cleanup touched the swapped hierarchy"
        );
        assert_eq!(
            std::fs::read(&victim).expect("victim readable"),
            b"untouched"
        );
    }

    #[tokio::test]
    async fn startup_failures_use_production_nonzero_fatal_entry_with_node_context() {
        if let Some(mode) = std::env::var_os("BLOCKD_STARTUP_FAILURE_CHILD") {
            let mode = mode.to_string_lossy();
            let (args, concrete) = subprocess_serve_args();
            let store: Arc<dyn ObjectStore> = match mode.as_ref() {
                "occupied-peer" => concrete,
                "membership-store" => Arc::new(FailMembershipStore(concrete)),
                unexpected => panic!("unexpected startup failure mode {unexpected}"),
            };
            serve_entry(args, store, std::future::pending()).await;
            unreachable!("production serve entry exits on startup failure");
        }

        for mode in ["occupied-peer", "membership-store"] {
            let (_fake, endpoint) = FakeGcs::start().await;
            let (args, _store, _root) = test_serve_args(&endpoint);
            let occupied = (mode == "occupied-peer").then(|| {
                std::net::TcpListener::bind(args.peer.expect("configured peer"))
                    .expect("occupy peer port")
            });
            let mut command = subprocess_command(
                "tests::startup_failures_use_production_nonzero_fatal_entry_with_node_context",
                &args,
                &endpoint,
            );
            command
                .env("BLOCKD_STARTUP_FAILURE_CHILD", mode)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped());
            let child = ChildGuard::spawn(&mut command).expect("spawn startup failure child");
            let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
                .await
                .expect("startup failure child exited")
                .expect("run startup failure child");
            drop(occupied);
            let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
            assert!(!output.status.success(), "{mode} child exited successfully");
            assert!(stderr.contains("runtime startup failed"), "{stderr}");
            assert!(stderr.contains("node_id="), "{stderr}");
            assert!(stderr.contains("cluster_id="), "{stderr}");
            assert!(stderr.contains("peer="), "{stderr}");
            match mode {
                "occupied-peer" => assert!(stderr.contains("peer listener startup failed")),
                "membership-store" => {
                    assert!(stderr.contains("initial peer membership publication failed"));
                }
                _ => unreachable!(),
            }
            assert!(!stderr.contains("panicked"), "{stderr}");
        }
    }

    async fn wait_for_membership(store: &Arc<GcsStore>) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if !store
                    .clone()
                    .list_prefix(peer_membership_prefix())
                    .await
                    .expect("membership listing")
                    .is_empty()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("membership publication");
    }

    async fn get_http(addr: SocketAddr, path: &str) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(mut stream) => {
                        stream
                            .write_all(
                                format!(
                                    "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                                )
                                .as_bytes(),
                            )
                            .await
                            .expect("health request");
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .await
                            .expect("health response");
                        return response;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("health endpoint became reachable")
    }

    async fn control_request(path: &std::path::Path, request: &str) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match tokio::net::UnixStream::connect(path).await {
                    Ok(mut stream) => {
                        stream
                            .write_all(format!("{request}\n").as_bytes())
                            .await
                            .expect("control request");
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .await
                            .expect("control response");
                        return serde_json::from_slice(&response).expect("JSON control response");
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("control socket became reachable")
    }

    #[tokio::test]
    async fn control_protocol_rejects_fields_not_owned_by_the_operation() {
        let root = tempfile::tempdir().expect("control root");
        let path = root.path().join("control.sock");
        let (requests, mut dispatched) = tokio::sync::mpsc::channel(1);
        let listener = tokio::spawn(control_listener(
            path.clone(),
            requests,
            Arc::new(AtomicBool::new(true)),
        ));
        let dispatch = tokio::spawn(async move {
            let request = dispatched.recv().await.expect("control request");
            let _ = request.reply.send("{\"dispatched\":true}\n".to_owned());
        });

        let response = control_request(&path, r#"{"operation":"inventory","volume":7}"#).await;

        assert!(
            response["error"].is_string(),
            "invalid command reached dispatch"
        );
        assert!(!dispatch.is_finished(), "invalid command was dispatched");
        dispatch.abort();
        listener.abort();
    }

    #[tokio::test]
    async fn readiness_response_and_metrics_state_share_one_published_snapshot() {
        let address = free_addr();
        let state = Arc::new(HealthState::new(HealthSnapshot {
            ready: true,
            identity_current: true,
            store_access: true,
            phase: PHASE_READY,
            diagnostic: "ready\n".to_owned(),
        }));
        let (metrics, _requests) = tokio::sync::mpsc::channel(1);
        let server = start_health_server(address, Arc::clone(&state), metrics)
            .await
            .expect("health server");

        state.publish(HealthSnapshot {
            ready: false,
            identity_current: true,
            store_access: true,
            phase: PHASE_DRAINING,
            diagnostic: "not ready: draining\n".to_owned(),
        });
        let response = get_http(address, "/ready").await;
        let published = state.snapshot();

        assert!(response.starts_with(b"HTTP/1.1 503"), "{response:?}");
        assert!(response.ends_with(b"not ready: draining\n"), "{response:?}");
        assert!(!published.ready);
        assert_eq!(published.phase, PHASE_DRAINING);
        assert!(published.identity_current);
        assert!(published.store_access);
        server.abort();
    }

    async fn wait_ready(addr: SocketAddr) -> Vec<u8> {
        wait_ready_for(addr, Duration::from_secs(30)).await
    }

    async fn wait_ready_for(addr: SocketAddr, bound: Duration) -> Vec<u8> {
        match wait_ready_result(addr, bound).await {
            Ok(response) => response,
            Err(last) => panic!(
                "daemon became ready; last response: {}",
                String::from_utf8_lossy(&last)
            ),
        }
    }

    async fn wait_ready_result(addr: SocketAddr, bound: Duration) -> Result<Vec<u8>, Vec<u8>> {
        let last = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&last);
        let ready = tokio::time::timeout(bound, async move {
            loop {
                let response = get_http(addr, "/ready").await;
                if response.starts_with(b"HTTP/1.1 200") {
                    return response;
                }
                *observed.lock().expect("ready response lock") = response;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        match ready {
            Ok(response) => Ok(response),
            Err(_) => Err(last.lock().expect("ready response lock").clone()),
        }
    }

    async fn authority_progress(store: &Arc<GcsStore>, host: HostId) -> String {
        let session = Arc::clone(store)
            .get(blockd_core::layout::host_session_key(host))
            .await
            .ok()
            .flatten()
            .and_then(|(_, bytes)| blockd_core::authority::HostSessionRecord::decode(&bytes).ok());
        let placement = Arc::clone(store)
            .get(blockd_core::layout::placement_key())
            .await
            .ok()
            .flatten()
            .and_then(|(_, bytes)| ClusterPlacement::decode(&bytes));
        let Some(placement) = placement else {
            return format!("session={session:?}, placement=missing");
        };
        format!("session={session:?}, placement_epoch={}", placement.epoch)
    }

    async fn wait_unready(addr: SocketAddr, dependency: &str) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let response = get_http(addr, "/ready").await;
                if response.starts_with(b"HTTP/1.1 503")
                    && response
                        .windows(dependency.len())
                        .any(|window| window == dependency.as_bytes())
                {
                    return response;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("daemon became unready with an actionable diagnostic")
    }

    async fn wait_metrics(addr: SocketAddr, needle: &str) -> String {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let response = String::from_utf8(get_http(addr, "/metrics").await)
                    .expect("UTF-8 metrics response");
                if response.contains(needle) {
                    return response;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("metrics series became observable")
    }

    fn metric_value(metrics: &str, series: &str) -> u64 {
        metrics
            .lines()
            .find_map(|line| {
                let (name, value) = line.rsplit_once(' ')?;
                (name == series).then(|| value.parse().expect("integer metric"))
            })
            .unwrap_or_else(|| panic!("missing metric {series}"))
    }

    #[derive(Default)]
    struct CountingWriter {
        bytes: usize,
        lines: usize,
    }

    impl std::fmt::Write for CountingWriter {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            self.bytes = self.bytes.saturating_add(value.len());
            self.lines = self.lines.saturating_add(value.matches('\n').count());
            Ok(())
        }
    }

    #[test]
    fn fault_histograms_include_infinite_bucket_and_large_rosters_stay_linear() {
        const SOURCES: [&str; 7] = [
            "memory", "journal", "local", "peer", "archive", "zero", "unknown",
        ];
        let histogram = blockd_runtime::HistogramSnapshot {
            buckets: vec![0; blockd_runtime::LATENCY_BUCKETS_NS.len()],
            count: 1,
            sum_ns: blockd_runtime::LATENCY_BUCKETS_NS
                [blockd_runtime::LATENCY_BUCKETS_NS.len() - 1]
                + 1,
            max_ns: blockd_runtime::LATENCY_BUCKETS_NS
                [blockd_runtime::LATENCY_BUCKETS_NS.len() - 1]
                + 1,
        };
        let one = blockd_runtime::FaultLatency {
            volume: blockd_core::types::VolumeId(1),
            source: "archive",
            histogram: histogram.clone(),
        };
        let mut exposition = String::new();
        append_fault_latency(&mut exposition, [one]).expect("format histogram");
        assert!(exposition.contains(
            "blockd_volume_fault_latency_seconds_bucket{volume_id=\"1\",source=\"archive\",le=\"+Inf\"} 1"
        ));

        let series = (0..10_000_u64).flat_map(|volume| {
            let histogram = histogram.clone();
            SOURCES
                .into_iter()
                .map(move |source| blockd_runtime::FaultLatency {
                    volume: blockd_core::types::VolumeId(volume),
                    source,
                    histogram: histogram.clone(),
                })
        });
        let mut counted = CountingWriter::default();
        append_fault_latency(&mut counted, series).expect("count large exposition");
        let lines_per_series = blockd_runtime::LATENCY_BUCKETS_NS.len() + 3;
        assert_eq!(counted.lines, 10_000 * SOURCES.len() * lines_per_series);
        let series_count = 10_000 * SOURCES.len();
        assert!(
            counted.bytes < series_count * 2_560,
            "{} bytes exceeded the per-series linear bound",
            counted.bytes
        );
    }

    async fn assert_metrics_cache_cadence(health: SocketAddr, metrics: &str) {
        let rendered = metric_value(metrics, "blockd_metrics_expositions_total");
        let cached_metrics =
            String::from_utf8(get_http(health, "/metrics").await).expect("UTF-8 metrics response");
        assert_eq!(
            metric_value(&cached_metrics, "blockd_metrics_expositions_total"),
            rendered,
            "scrapes inside the cache cadence must share one exposition"
        );
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let next_metrics =
            String::from_utf8(get_http(health, "/metrics").await).expect("UTF-8 metrics response");
        assert_eq!(
            metric_value(&next_metrics, "blockd_metrics_expositions_total"),
            rendered + 1,
            "readiness cadence must not rebuild the metrics exposition"
        );
    }

    async fn wait_for_retired_session(store: &Arc<GcsStore>, host: HostId) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let retired = store
                    .clone()
                    .get(blockd_core::layout::host_session_key(host))
                    .await
                    .expect("host session lookup")
                    .and_then(|(_, bytes)| {
                        blockd_core::authority::HostSessionRecord::decode(&bytes).ok()
                    })
                    .is_some_and(|record| {
                        matches!(
                            record,
                            blockd_core::authority::HostSessionRecord::Revoked { .. }
                        )
                    });
                if retired {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("authority session retired during drain");
    }

    struct LiveTestPeers {
        main_identity: HostId,
        runtimes: Vec<Runtime>,
        _roots: Vec<tempfile::TempDir>,
    }

    type TestPeerStartup = std::pin::Pin<Box<dyn Future<Output = LiveTestPeers>>>;

    impl LiveTestPeers {
        fn healthy(&self) -> bool {
            self.runtimes.iter().all(|runtime| {
                let readiness = runtime.readiness();
                readiness.peer_listener && readiness.critical_tasks && readiness.unfenced
            })
        }

        async fn shutdown(mut self) {
            for runtime in &mut self.runtimes {
                runtime
                    .shutdown()
                    .await
                    .expect("test peer membership withdrawal");
            }
        }
    }

    async fn seed_test_placement(
        args: &ServeArgs,
        store: &Arc<GcsStore>,
    ) -> (HostId, TestPeerStartup) {
        let binding = args.store.to_string();
        let abstract_store: Arc<dyn ObjectStore> = store.clone();
        let (cluster_id, identity) = bootstrap(abstract_store, &args.data_dir, &binding)
            .await
            .expect("test bootstrap");
        let host = identity.host;
        let host_identity = host;
        drop(identity);
        let second = HostId::new(host.get().wrapping_add(1));
        let third = HostId::new(host.get().wrapping_add(2));
        let fourth = HostId::new(host.get().wrapping_add(3));
        let mut roster = vec![host_identity, second, third, fourth];
        roster.sort_unstable();
        let placement = ClusterPlacement::new(cluster_id, 1, roster).expect("test placement");
        Arc::clone(store)
            .put(blockd_core::layout::placement_key(), placement.encode())
            .await
            .expect("seed placement");
        let mut roots = Vec::new();
        let mut configs = Vec::new();
        for host in [second, third, fourth] {
            let address = free_addr();
            let root = tempfile::tempdir().expect("passive runtime root");
            let config = production_runtime_config(
                host,
                cluster_id,
                root.path(),
                address,
                args.capacity_bytes,
                args.headroom_bytes,
            );
            configs.push(config);
            roots.push(root);
        }
        let store = Arc::clone(store);
        (
            host,
            Box::pin(async move {
                let runtimes = futures_util::future::join_all(configs.iter().map(|config| {
                    let peer_store: Arc<dyn ObjectStore> = store.clone();
                    Runtime::new(config, peer_store)
                }))
                .await
                .into_iter()
                .map(|runtime| runtime.expect("passive runtime startup"))
                .collect();
                LiveTestPeers {
                    main_identity: host_identity,
                    runtimes,
                    _roots: roots,
                }
            }),
        )
    }

    /// Regression PROD-012: the production daemon must expose a readiness endpoint,
    /// and it must not become reachable merely because the process exists.
    #[tokio::test]
    async fn serve_exposes_a_readiness_endpoint() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, _root) = test_serve_args(&endpoint);
        let (_host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let control = args.control.clone();
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let _live_peers = pending_peers.await;
        wait_for_membership(&concrete).await;
        let response = wait_ready(health).await;
        let live = get_http(health, "/live").await;
        let created = control_request(
            &control,
            r#"{"operation":"create","volume":67,"pages":1,"kind":"data"}"#,
        )
        .await;
        assert_eq!(created["created"], 67);
        let written = control_request(
            &control,
            r#"{"operation":"write-page","volume":67,"page":0,"value":1311768467463790320}"#,
        )
        .await;
        assert_eq!(written["written"], 1_311_768_467_463_790_320_u64);
        let synced = control_request(&control, r#"{"operation":"sync","volume":67}"#).await;
        assert_eq!(synced["synced"], true);
        let read = control_request(
            &control,
            r#"{"operation":"read-page","volume":67,"page":0}"#,
        )
        .await;
        assert_eq!(read["value"], 1_311_768_467_463_790_320_u64);
        let metrics = wait_metrics(health, "blockd_syncs_acked_total 1").await;
        assert!(metric_value(&metrics, "blockd_dirty_pages_total") > 0);
        assert!(metric_value(&metrics, "blockd_syncs_acked_total") > 0);
        assert!(metric_value(&metrics, "blockd_assignment_claims_total") > 0);
        assert!(
            metric_value(
                &metrics,
                "blockd_volume_pages_dirtied_total{volume_id=\"67\"}",
            ) > 0
        );
        assert!(metric_value(&metrics, "blockd_volume_assignment_epoch{volume_id=\"67\"}",) > 0);
        assert_metrics_cache_cadence(health, &metrics).await;
        let _ = stop.send(());
        task.await.expect("serve task").expect("clean shutdown");

        assert!(
            response.starts_with(b"HTTP/1.1 200"),
            "readiness endpoint did not return HTTP 200"
        );
        assert!(live.starts_with(b"HTTP/1.1 200"));
        for required in [
            "blockd_up 1",
            "blockd_ready 1",
            "blockd_readiness_dependency{dependency=\"object_store\"} 1",
            "blockd_store_retries_total",
            "blockd_assignment_claims_total",
            "blockd_fences_total",
            "blockd_peer_overload_rejections_total",
            "blockd_peer_outbound_worker_rejections_total",
            "blockd_peer_outbound_queue_rejections_total",
            "blockd_peer_outbound_active_workers",
            "blockd_peer_outbound_buffered_messages",
            "blockd_peer_outbound_buffered_bytes",
            "blockd_disk_local_bytes",
            "blockd_memory_pressure_waiting_faults",
            "blockd_volume_fault_latency_seconds_bucket{volume_id=\"67\"",
            "le=\"+Inf\"}",
            "blockd_volume_hydration_remaining_pages{volume_id=\"67\"}",
            "blockd_volume_archive_lag_bytes{volume_id=\"67\"}",
            "blockd_volume_pages_dirtied_total{volume_id=\"67\"}",
            "blockd_volume_active_peer{volume_id=\"67\"}",
            "blockd_volume_transition_peer{volume_id=\"67\"}",
            "blockd_volume_assignment_epoch{volume_id=\"67\"}",
            "blockd_volume_protected_sync_lag{volume_id=\"67\"}",
            "blockd_volume_replica_spool_bytes{volume_id=\"67\"}",
            "blockd_volume_replica_spool_capacity_bytes{volume_id=\"67\"}",
            "blockd_volume_stalled_syncs{volume_id=\"67\"}",
            "blockd_volume_replica_retries{volume_id=\"67\"}",
            "blockd_volume_integrity_rejects_total{volume_id=\"67\"}",
            "blockd_volume_replica_replacement_bytes_total{volume_id=\"67\"}",
            "blockd_volume_replica_cleanup_unlinks_total{volume_id=\"67\"}",
            "blockd_replica_nonactive_bytes_total 0",
            "blockd_replica_cleanup_rewrite_bytes_total 0",
        ] {
            assert!(metrics.contains(required), "missing metric {required}");
        }
    }

    #[tokio::test]
    async fn readiness_tracks_current_store_access_and_recovers() {
        let (fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, _root) = test_serve_args(&endpoint);
        let (_host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let _live_peers = pending_peers.await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        fake.outage.store(true, Ordering::SeqCst);
        assert!(
            wait_unready(health, "object_store")
                .await
                .starts_with(b"HTTP/1.1 503")
        );
        assert!(get_http(health, "/live").await.starts_with(b"HTTP/1.1 200"));
        fake.outage.store(false, Ordering::SeqCst);
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        let _ = stop.send(());
        task.await.expect("serve task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn held_metrics_render_keeps_real_control_readiness_and_shutdown_responsive() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (mut args, concrete, _root) = test_serve_args(&endpoint);
        let (_host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let metrics_hook = MetricsRenderTestHook::armed();
        let drain_hook = DrainTestHook::default();
        args.metrics_render_hook = Some(metrics_hook.clone());
        args.drain_hook = Some(drain_hook.clone());
        let health = args.health;
        let control = args.control.clone();
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let _live_peers = pending_peers.await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        let scrape = tokio::spawn(get_http(health, "/metrics"));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !metrics_hook.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cold renderer reached held section");
        let inventory = control_request(&control, r#"{"operation":"inventory"}"#).await;
        assert!(inventory["volumes"].is_array());

        stop.send(()).expect("request shutdown");
        tokio::time::timeout(Duration::from_secs(5), drain_hook.entered.notified())
            .await
            .expect("serve loop entered bounded drain while render was held");
        let draining = get_http(health, "/ready").await;
        assert!(draining.starts_with(b"HTTP/1.1 503"));
        assert!(String::from_utf8_lossy(&draining).contains("draining"));

        metrics_hook.release();
        let metrics = tokio::time::timeout(Duration::from_secs(1), scrape)
            .await
            .expect("held metrics render was reaped")
            .expect("metrics request task");
        assert!(metrics.starts_with(b"HTTP/1.1 200"));
        drain_hook.release.notify_one();
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("bounded daemon shutdown")
            .expect("serve task")
            .expect("clean shutdown");
    }

    #[tokio::test]
    async fn held_terminal_metrics_render_cannot_delay_safety_drain() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (mut args, concrete, _root) = test_serve_args(&endpoint);
        let (_host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let metrics_hook = MetricsRenderTestHook::armed();
        let drain_hook = DrainTestHook::default();
        args.metrics_render_hook = Some(metrics_hook.clone());
        args.drain_hook = Some(drain_hook.clone());
        let health = args.health;
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let _live_peers = pending_peers.await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        stop.send(()).expect("request shutdown");
        tokio::time::timeout(Duration::from_secs(1), async {
            while !metrics_hook.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal renderer reached held section");
        tokio::time::timeout(Duration::from_secs(1), drain_hook.entered.notified())
            .await
            .expect("safety drain began without waiting for terminal metrics");
        let draining = get_http(health, "/ready").await;
        assert!(draining.starts_with(b"HTTP/1.1 503"));
        assert!(String::from_utf8_lossy(&draining).contains("draining"));

        metrics_hook.release();
        drain_hook.release.notify_one();
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .expect("bounded daemon shutdown")
            .expect("serve task")
            .expect("clean shutdown");
    }

    #[tokio::test]
    async fn remote_identity_claim_drift_is_host_fatal() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, _root) = test_serve_args(&endpoint);
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let task = tokio::spawn(serve_with_store(args, store, std::future::pending()));
        let _live_peers = pending_peers.await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        concrete
            .clone()
            .put(
                blockd_core::layout::node_claim_key(host),
                b"different durable owner".to_vec(),
            )
            .await
            .expect("replace durable identity claim");
        let result = tokio::time::timeout(Duration::from_secs(4), task)
            .await
            .expect("identity drift terminated daemon")
            .expect("serve task");
        assert!(
            result
                .expect_err("identity drift must not be a clean shutdown")
                .contains("node fenced")
        );
    }

    #[tokio::test]
    async fn readiness_rejects_a_live_roster_below_replication_factor() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, _root) = test_serve_args(&endpoint);
        let (_host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (_stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let live_peers = pending_peers.await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        live_peers.shutdown().await;
        assert!(
            wait_unready(health, "placement")
                .await
                .starts_with(b"HTTP/1.1 503")
        );
        assert!(
            wait_metrics(health, "blockd_lifecycle_state{state=\"joined\"} 1")
                .await
                .contains("blockd_ready 0")
        );
        assert!(get_http(health, "/live").await.starts_with(b"HTTP/1.1 200"));

        task.abort();
        assert!(
            task.await
                .expect_err("aborted readiness fixture")
                .is_cancelled(),
            "readiness fixture task must be cancelled during teardown"
        );
    }

    #[tokio::test]
    async fn replaced_membership_ownership_is_host_fatal() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, _root) = test_serve_args(&endpoint);
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let task = tokio::spawn(serve_with_store(args, store, std::future::pending()));
        let _live_peers = pending_peers.await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        concrete
            .clone()
            .put(
                blockd_core::layout::peer_membership_key(host),
                b"replacement owner".to_vec(),
            )
            .await
            .expect("replace membership ownership");
        let result = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .expect("ownership replacement terminated daemon")
            .expect("serve task");
        assert!(
            result
                .expect_err("ownership replacement must not be graceful")
                .contains("node fenced")
        );
    }

    #[tokio::test]
    async fn unexpected_session_revoke_remains_host_fatal() {
        if std::env::var_os("BLOCKD_UNEXPECTED_REVOKE_CHILD").is_some() {
            let (args, concrete) = subprocess_serve_args();
            let store: Arc<dyn ObjectStore> = concrete;
            serve_with_store(args, store, shutdown_signal())
                .await
                .expect("unexpected revoke child only exits through host-fatal fencing");
            return;
        }

        let (_fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, _root) = test_serve_args(&endpoint);
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let mut command = subprocess_command(
            "tests::unexpected_session_revoke_remains_host_fatal",
            &args,
            &endpoint,
        );
        command
            .env("BLOCKD_UNEXPECTED_REVOKE_CHILD", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        let child = ChildGuard::spawn(&mut command).expect("spawn unexpected revoke child");
        let _live_peers = pending_peers.await;
        assert!(
            wait_ready_for(health, Duration::from_secs(30))
                .await
                .starts_with(b"HTTP/1.1 200")
        );
        let session_key = blockd_core::layout::host_session_key(host);
        let active = concrete
            .clone()
            .get(session_key.clone())
            .await
            .expect("host session lookup")
            .and_then(|(_, bytes)| blockd_core::authority::HostSessionRecord::decode(&bytes).ok())
            .expect("active host session");
        let session = match active {
            blockd_core::authority::HostSessionRecord::Active { session, .. } => session,
            unexpected => panic!("expected active host session, got {unexpected:?}"),
        };
        let revoked = active
            .retire(session, 0xfeed_cafe)
            .expect("unexpected revocation fixture");
        concrete
            .clone()
            .put(session_key, revoked.encode())
            .await
            .expect("publish unexpected revocation");
        let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
            .await
            .expect("unexpected revocation fenced child")
            .expect("unexpected revoke child output");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "unexpected revoke exited cleanly");
        assert!(stderr.contains("host session fenced"), "{stderr}");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // full child-process crash, restart, recovery, and drain lifecycle
    async fn crashed_daemon_rolling_restart_preserves_roster_and_local_identity() {
        if std::env::var_os("BLOCKD_CRASH_LIFECYCLE_CHILD").is_some() {
            let (args, concrete) = subprocess_serve_args();
            let store: Arc<dyn ObjectStore> = concrete;
            serve_with_store(args, store, shutdown_signal())
                .await
                .expect("subprocess daemon lifecycle");
            return;
        }

        let (_fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, root) = test_serve_args(&endpoint);
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let control = args.control.clone();
        let member_key = blockd_core::layout::peer_membership_key(host);
        let mut crash_command = subprocess_command(
            "tests::crashed_daemon_rolling_restart_preserves_roster_and_local_identity",
            &args,
            &endpoint,
        );
        crash_command
            .env("BLOCKD_CRASH_LIFECYCLE_CHILD", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let mut crashed = ChildGuard::spawn(&mut crash_command).expect("spawn daemon child");
        let live_peers = pending_peers.await;
        let exact_identity = live_peers.main_identity;
        assert!(
            wait_ready_for(health, Duration::from_secs(30))
                .await
                .starts_with(b"HTTP/1.1 200")
        );
        let first_record = concrete
            .clone()
            .get(member_key.clone())
            .await
            .expect("membership lookup")
            .expect("membership before crash")
            .1;
        let created = control_request(
            &control,
            r#"{"operation":"create","volume":82,"pages":1,"kind":"data"}"#,
        )
        .await;
        assert_eq!(created["created"], 82);
        let written = control_request(
            &control,
            r#"{"operation":"write-page","volume":82,"page":0,"value":1234605616436508552}"#,
        )
        .await;
        assert_eq!(written["written"], 1_234_605_616_436_508_552_u64);
        let synced = control_request(&control, r#"{"operation":"sync","volume":82}"#).await;
        assert_eq!(synced["synced"], true);

        let first_session = concrete
            .clone()
            .get(blockd_core::layout::host_session_key(host))
            .await
            .expect("host session lookup")
            .and_then(|(_, bytes)| blockd_core::authority::HostSessionRecord::decode(&bytes).ok())
            .expect("active host session before crash");
        assert!(matches!(
            first_session,
            blockd_core::authority::HostSessionRecord::Active { .. }
        ));

        let killed = tokio::process::Command::new("/bin/kill")
            .args([
                "-KILL",
                &crashed.id().expect("daemon child pid").to_string(),
            ])
            .status()
            .await
            .expect("send SIGKILL");
        assert!(killed.success(), "failed to send SIGKILL");
        assert!(
            !tokio::time::timeout(Duration::from_secs(5), crashed.wait())
                .await
                .expect("crashed child exited")
                .expect("crashed child status")
                .success(),
            "SIGKILL child exited successfully"
        );
        assert!(
            concrete
                .clone()
                .get(member_key.clone())
                .await
                .expect("membership lookup")
                .is_some(),
            "a crash performed a graceful membership withdrawal"
        );
        assert!(live_peers.healthy());
        let binding = args.store.to_string();
        let bootstrap_store: Arc<dyn ObjectStore> = concrete.clone();
        let (_, persisted_identity) = bootstrap(bootstrap_store, root.path(), &binding)
            .await
            .expect("bootstrap same durable state after crash");
        assert_eq!(persisted_identity.host, exact_identity);
        drop(persisted_identity);

        let restarted_args = test_serve_args_in(root.path(), &endpoint);
        let restarted_health = restarted_args.health;
        let restarted_control = restarted_args.control.clone();
        let mut restart_command = subprocess_command(
            "tests::crashed_daemon_rolling_restart_preserves_roster_and_local_identity",
            &restarted_args,
            &endpoint,
        );
        restart_command
            .env("BLOCKD_CRASH_LIFECYCLE_CHILD", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        let restarted =
            ChildGuard::spawn(&mut restart_command).expect("spawn restarted daemon child");
        assert!(
            wait_ready_for(restarted_health, Duration::from_secs(40))
                .await
                .starts_with(b"HTTP/1.1 200")
        );
        let second_record = concrete
            .clone()
            .get(member_key.clone())
            .await
            .expect("membership lookup")
            .expect("membership after restart")
            .1;
        assert_ne!(
            first_record, second_record,
            "restart did not rotate the ephemeral peer identity"
        );
        let restarted_session = concrete
            .clone()
            .get(blockd_core::layout::host_session_key(host))
            .await
            .expect("host session lookup")
            .and_then(|(_, bytes)| blockd_core::authority::HostSessionRecord::decode(&bytes).ok())
            .expect("active host session after restart");
        assert!(matches!(
            restarted_session,
            blockd_core::authority::HostSessionRecord::Active { .. }
        ));
        assert!(live_peers.healthy());
        let inventory = control_request(&restarted_control, r#"{"operation":"inventory"}"#).await;
        assert!(inventory["volumes"].as_array().is_some_and(|volumes| {
            volumes
                .iter()
                .any(|volume| volume["volume"] == 82 && volume["quarantined"] == false)
        }));
        let read = control_request(
            &restarted_control,
            r#"{"operation":"read-page","volume":82,"page":0}"#,
        )
        .await;
        assert_eq!(
            read["value"], 1_234_605_616_436_508_552_u64,
            "first post-restart fault did not restore the synced page"
        );

        let terminated = tokio::process::Command::new("/bin/kill")
            .args([
                "-TERM",
                &restarted.id().expect("restarted child pid").to_string(),
            ])
            .status()
            .await
            .expect("send SIGTERM");
        assert!(terminated.success(), "failed to send SIGTERM");
        let output = tokio::time::timeout(Duration::from_secs(40), restarted.wait_with_output())
            .await
            .expect("restarted child exited")
            .expect("restarted child output");
        assert!(
            output.status.success(),
            "restarted daemon did not drain cleanly: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            concrete
                .clone()
                .get(member_key)
                .await
                .expect("membership lookup")
                .is_none(),
            "graceful restart shutdown left membership published"
        );
        wait_for_retired_session(&concrete, host).await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // black-box control and metrics contract is asserted end to end
    async fn control_socket_manages_and_inventories_volumes() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (args, concrete, _root) = test_serve_args(&endpoint);
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let control = args.control.clone();
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let _live_peers = pending_peers.await;
        wait_for_membership(&concrete).await;
        match wait_ready_result(health, Duration::from_secs(30)).await {
            Ok(response) => assert!(response.starts_with(b"HTTP/1.1 200")),
            Err(last) => panic!(
                "daemon authority startup stalled: {}; last response: {}",
                authority_progress(&concrete, host).await,
                String::from_utf8_lossy(&last)
            ),
        }
        let created = control_request(
            &control,
            r#"{"operation":"create","volume":75,"pages":1,"kind":"data"}"#,
        )
        .await;
        assert_eq!(created["created"], 75);
        let inventory = control_request(&control, r#"{"operation":"inventory"}"#).await;
        assert!(inventory["volumes"].as_array().is_some_and(|volumes| {
            volumes
                .iter()
                .any(|volume| volume["volume"] == 75 && volume["quarantined"] == false)
        }));
        let refused = control_request(
            &control,
            r#"{"operation":"discard-quarantine","volume":75}"#,
        )
        .await;
        assert!(
            refused["error"]
                .as_str()
                .is_some_and(|error| error.contains("reason"))
        );

        let unrestorable = control_request(
            &control,
            r#"{"operation":"restore","volume":78,"pages":1,"kind":"data"}"#,
        )
        .await;
        assert_eq!(unrestorable["verdict"], "Unrestorable");
        assert!(
            wait_unready(health, "recovery")
                .await
                .starts_with(b"HTTP/1.1 503")
        );
        assert!(
            wait_metrics(health, "blockd_lifecycle_state{state=\"recovering\"} 1")
                .await
                .contains("blockd_ready 0")
        );
        let discarded = control_request(
            &control,
            r#"{"operation":"discard-quarantine","volume":78,"reason":"verified empty test restore"}"#,
        )
        .await;
        assert_eq!(discarded["discarded"], 78);
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        let _ = stop.send(());
        task.await.expect("serve task").expect("clean shutdown");
        assert!(!control.exists(), "shutdown left the control socket behind");
        let (_, session) = concrete
            .clone()
            .get(blockd_core::layout::host_session_key(host))
            .await
            .expect("read retired authority session")
            .expect("authority session remains as a fencing tombstone");
        assert!(matches!(
            blockd_core::authority::HostSessionRecord::decode(&session)
                .expect("decode retired authority session"),
            blockd_core::authority::HostSessionRecord::Revoked { .. }
        ));
    }

    /// Regression PROD-013: a planned shutdown must withdraw the membership record
    /// rather than leaving an apparently live node for lease expiry.
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // active-volume drain asserts admission, transfer, and durable withdrawal
    async fn planned_shutdown_conditionally_withdraws_membership() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (mut args, concrete, _root) = test_serve_args(&endpoint);
        let drain_hook = DrainTestHook::default();
        args.drain_hook = Some(drain_hook.clone());
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let control = args.control.clone();
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let live_peers = pending_peers.await;
        wait_for_membership(&concrete).await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));
        let created = control_request(
            &control,
            r#"{"operation":"create","volume":85,"pages":1,"kind":"data"}"#,
        )
        .await;
        assert_eq!(created["created"], 85);
        let written = control_request(
            &control,
            r#"{"operation":"write-page","volume":85,"page":0,"value":16045690984503098046}"#,
        )
        .await;
        assert_eq!(written["written"], 16_045_690_984_503_098_046_u64);
        let synced = control_request(&control, r#"{"operation":"sync","volume":85}"#).await;
        assert_eq!(synced["synced"], true);
        let (_, head_bytes) = concrete
            .clone()
            .get(blockd_core::layout::head_key(blockd_core::types::VolumeId(
                85,
            )))
            .await
            .expect("active volume head lookup")
            .expect("active volume head");
        let head =
            blockd_core::head::HeadRecord::decode(blockd_core::types::VolumeId(85), &head_bytes)
                .expect("active volume head record");
        assert!(
            head.stash.is_some(),
            "planned drain fixture must have real peer-protected work"
        );
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(1), drain_hook.entered.notified())
            .await
            .expect("drain closed lifecycle admission");
        assert!(
            wait_unready(health, "draining")
                .await
                .starts_with(b"HTTP/1.1 503"),
            "planned drain did not publish a coherent readiness diagnostic"
        );
        let rejected = control_request(
            &control,
            r#"{"operation":"create","volume":94,"pages":1,"kind":"data"}"#,
        )
        .await;
        assert_eq!(rejected["error"], "node is draining");
        drain_hook.release.notify_one();
        task.await.expect("serve task").expect("clean shutdown");

        assert!(
            concrete
                .clone()
                .get(blockd_core::layout::peer_membership_key(host))
                .await
                .expect("membership lookup")
                .is_none(),
            "planned shutdown left its owned membership record behind"
        );
        let (_, placement_bytes) = concrete
            .clone()
            .get(blockd_core::layout::placement_key())
            .await
            .expect("authority placement lookup")
            .expect("authority placement remains");
        let placement = ClusterPlacement::decode(&placement_bytes).expect("authority placement");
        assert!(
            !placement.contains(host),
            "planned shutdown withdrew membership before authority transfer"
        );
        let replacement = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(replacement) = live_peers.runtimes.iter().find(|runtime| {
                    runtime.is_ready()
                        && runtime
                            .volume_inventory()
                            .iter()
                            .any(|(volume, _, quarantined)| {
                                *volume == blockd_core::types::VolumeId(85) && !quarantined
                            })
                }) {
                    break replacement;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("replacement recovered active volume");
        let recovered = replacement
            .guest_read(
                blockd_core::types::VolumeId(85),
                blockd_core::types::PageId {
                    volume: blockd_core::types::VolumeId(85),
                    page: blockd_core::types::PageNo(0),
                },
            )
            .await;
        assert_eq!(
            u64::from_le_bytes(recovered[..8].try_into().expect("word-sized page prefix")),
            16_045_690_984_503_098_046_u64,
            "replacement did not serve the peer-protected synced value"
        );
        let (_, session_bytes) = concrete
            .clone()
            .get(blockd_core::layout::host_session_key(host))
            .await
            .expect("retired host session lookup")
            .expect("retired host session remains as a fence");
        assert!(matches!(
            blockd_core::authority::HostSessionRecord::decode(&session_bytes)
                .expect("retired host session"),
            blockd_core::authority::HostSessionRecord::Revoked { .. }
        ));
    }

    #[tokio::test]
    async fn planned_shutdown_fails_if_membership_ownership_was_replaced() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let (mut args, concrete, _root) = test_serve_args(&endpoint);
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        let drain_hook = DrainTestHook::default();
        args.drain_hook = Some(drain_hook.clone());
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let _live_peers = pending_peers.await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));

        let _ = stop.send(());
        drain_hook.entered.notified().await;
        let replacement = b"replacement owner during drain".to_vec();
        concrete
            .clone()
            .put(
                blockd_core::layout::peer_membership_key(host),
                replacement.clone(),
            )
            .await
            .expect("replace membership during drain");
        drain_hook.release.notify_one();
        let result = task.await.expect("serve task");
        assert!(
            result
                .expect_err("stale membership ownership must fail drain")
                .contains("drain publication failed")
        );
        assert_eq!(
            concrete
                .clone()
                .get(blockd_core::layout::peer_membership_key(host))
                .await
                .expect("membership lookup")
                .expect("replacement membership preserved")
                .1,
            replacement
        );
    }

    #[tokio::test]
    async fn planned_shutdown_has_a_forced_timeout_and_keeps_membership_on_failure() {
        let (fake, endpoint) = FakeGcs::start().await;
        let (mut args, concrete, _root) = test_serve_args(&endpoint);
        let (host, pending_peers) = seed_test_placement(&args, &concrete).await;
        let health = args.health;
        args.drain_timeout = Some(Duration::from_secs(2));
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_with_store(args, store, async {
            let _ = stopped.await;
        }));
        let _live_peers = pending_peers.await;
        wait_for_membership(&concrete).await;
        assert!(wait_ready(health).await.starts_with(b"HTTP/1.1 200"));
        fake.latency_ms.store(5_000, Ordering::SeqCst);
        let _ = stop.send(());
        let result = tokio::time::timeout(Duration::from_secs(8), task)
            .await
            .expect("forced drain is bounded")
            .expect("serve task");
        assert!(
            result.is_err(),
            "a drain whose durable publication cannot finish reported success"
        );
        fake.latency_ms.store(0, Ordering::SeqCst);
        assert!(
            concrete
                .clone()
                .get(blockd_core::layout::peer_membership_key(host))
                .await
                .expect("membership lookup")
                .is_some(),
            "failed drain unconditionally removed membership ownership"
        );
    }
}
