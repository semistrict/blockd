//! Process-wide telemetry and the daemon's Prometheus registry.
//!
//! Logs always stay local as newline-delimited JSON. Trace export is an
//! optional second subscriber layer, enabled only when an OTLP endpoint is
//! configured, so a missing collector cannot affect the daemon's work.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter, Write as _};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use blockd_core::daemon::Counters;
use blockd_core::daemon::{DaemonStats, VsetOperations, VsetRole};
use blockd_runtime::{HistogramSnapshot, LATENCY_BUCKETS_NS};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{SpanExporter, WithHttpConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, fmt};

const DEFAULT_FILTER: &str = "demod=info,blockd_runtime=info";
const MAX_OTLP_RESPONSE_BYTES: usize = 1024 * 1024;

struct AsyncHttpClient {
    client: reqwest::Client,
}

impl AsyncHttpClient {
    fn new() -> AsyncHttpClient {
        AsyncHttpClient {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(10))
                .build()
                .expect("OTLP HTTP client"),
        }
    }
}

impl Debug for AsyncHttpClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AsyncHttpClient")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl HttpClient for AsyncHttpClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();
        let mut outgoing = self
            .client
            .request(parts.method, parts.uri.to_string())
            .body(body);
        for (name, value) in &parts.headers {
            outgoing = outgoing.header(name, value);
        }
        let mut response = outgoing.send().await?;
        let status = response.status();
        let headers = response.headers().clone();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if body.len().saturating_add(chunk.len()) > MAX_OTLP_RESPONSE_BYTES {
                return Err("OTLP response exceeded 1 MiB".into());
            }
            body.extend_from_slice(&chunk);
        }
        let mut result = Response::builder().status(status).body(Bytes::from(body))?;
        *result.headers_mut() = headers;
        Ok(result)
    }
}

/// Keeps the batched exporter alive and flushes it during an orderly exit.
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = &self.provider
            && let Err(error) = provider.shutdown()
        {
            tracing::error!(%error, "failed to shut down trace provider");
        }
    }
}

/// Install JSON logging and, when explicitly configured, batched OTLP/HTTP
/// trace export. The exporter uses an asynchronous HTTP client; request
/// handlers only enqueue completed spans.
pub fn init(host: Option<u16>) -> TelemetryGuard {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let json = fmt::layer().json().with_writer(std::io::stderr);
    let provider = if otlp_enabled() {
        let exporter = SpanExporter::builder()
            .with_http()
            .with_http_client(AsyncHttpClient::new())
            .build()
            .expect("OTLP trace exporter configuration");
        let service_name =
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "blockd-demod".to_owned());
        let mut resource = Resource::builder().with_service_name(service_name);
        if let Some(host) = host {
            resource =
                resource.with_attribute(KeyValue::new("service.instance.id", host.to_string()));
        }
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource.build())
            .build();
        global::set_tracer_provider(provider.clone());
        Some(provider)
    } else {
        None
    };

    let otel = provider.as_ref().map(|provider| {
        let tracer = provider.tracer("blockd-demod");
        tracing_opentelemetry::layer().with_tracer(tracer)
    });
    tracing_subscriber::registry()
        .with(filter)
        .with(json)
        .with(otel)
        .init();

    TelemetryGuard { provider }
}

fn otlp_enabled() -> bool {
    if std::env::var("OTEL_SDK_DISABLED").is_ok_and(|value| value.eq_ignore_ascii_case("true"))
        || std::env::var("OTEL_TRACES_EXPORTER")
            .is_ok_and(|value| value.eq_ignore_ascii_case("none"))
    {
        return false;
    }
    std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
}

pub struct Metrics {
    registry: Registry,
    http_requests: IntCounterVec,
    http_duration: HistogramVec,
    http_in_flight: IntGauge,
}

pub struct RequestMetrics {
    metrics: Arc<Metrics>,
    method: String,
    route: String,
    started: Instant,
    status: u16,
}

impl RequestMetrics {
    pub fn set_status(&mut self, status: u16) {
        self.status = status;
    }
}

impl Drop for RequestMetrics {
    fn drop(&mut self) {
        self.metrics.http_in_flight.dec();
        self.metrics
            .http_requests
            .with_label_values(&[&self.method, &self.route, &self.status.to_string()])
            .inc();
        self.metrics
            .http_duration
            .with_label_values(&[&self.method, &self.route])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

pub struct MetricsSnapshot {
    pub host: u16,
    pub vms: BTreeMap<(String, bool), u64>,
    pub runtime: Counters,
    pub store: StoreMetrics,
    pub peer_dropped_sends: u64,
    pub peer_connections: Vec<(u16, bool)>,
    pub incidents: u64,
    pub daemon: DaemonStats,
    pub loop_decide: Vec<(&'static str, u64, u64)>,
    pub loop_effect: Vec<(&'static str, u64, u64)>,
    pub loop_idle_ns: u64,
    pub loop_occupancy: f64,
    pub loop_queue_depths: (usize, usize),
    pub fault_latency: Vec<FaultLatencySnapshot>,
    pub operation_latency: Vec<LatencySeries>,
    pub guest_pause_latency: Vec<LatencySeries>,
    pub local_io_latency: Vec<LatencySeries>,
    pub local_io_in_flight: Vec<(&'static str, u64)>,
    pub store_latency: Vec<LatencySeries>,
    pub firecracker_fault_latency: Vec<(&'static str, HistogramSnapshot)>,
    pub blob_filesystem_space: Option<(u64, u64)>,
    pub backup_lag_age: Vec<(u64, f64)>,
    pub active_operation_age: Vec<(u64, &'static str, f64)>,
}

pub struct FaultLatencySnapshot {
    pub vset: u64,
    pub source: &'static str,
    pub histogram: HistogramSnapshot,
}

pub struct LatencySeries {
    pub operation: &'static str,
    pub outcome: &'static str,
    pub histogram: HistogramSnapshot,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StoreMetrics {
    pub puts: u64,
    pub cas_puts: u64,
    pub gets: u64,
    pub ranged_gets: u64,
    pub precondition_failures: u64,
    pub deletes: u64,
    pub unavailable: u64,
    pub token_refreshes: u64,
    pub bytes_up: u64,
    pub bytes_down: u64,
}

impl Metrics {
    pub fn new() -> Metrics {
        let registry = Registry::new();
        let http_requests = IntCounterVec::new(
            Opts::new(
                "blockd_http_requests_total",
                "HTTP requests completed by the control API.",
            ),
            &["method", "route", "status_code"],
        )
        .expect("HTTP request counter");
        let http_duration = HistogramVec::new(
            HistogramOpts::new(
                "blockd_http_request_duration_seconds",
                "Control API request duration in seconds.",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &["method", "route"],
        )
        .expect("HTTP duration histogram");
        let http_in_flight = IntGauge::with_opts(Opts::new(
            "blockd_http_requests_in_flight",
            "Control API requests currently being handled.",
        ))
        .expect("HTTP in-flight gauge");

        registry
            .register(Box::new(http_requests.clone()))
            .expect("register HTTP request counter");
        registry
            .register(Box::new(http_duration.clone()))
            .expect("register HTTP duration histogram");
        registry
            .register(Box::new(http_in_flight.clone()))
            .expect("register HTTP in-flight gauge");
        #[cfg(target_os = "linux")]
        registry
            .register(Box::new(
                prometheus::process_collector::ProcessCollector::for_self(),
            ))
            .expect("register process collector");

        Metrics {
            registry,
            http_requests,
            http_duration,
            http_in_flight,
        }
    }

    pub fn start_request(self: &Arc<Metrics>, method: &str, route: &str) -> RequestMetrics {
        self.http_in_flight.inc();
        RequestMetrics {
            metrics: self.clone(),
            method: method.to_owned(),
            route: route.to_owned(),
            started: Instant::now(),
            // A panic or early return is an internal failure, not a success.
            status: 500,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn encode(&self, snapshot: &MetricsSnapshot) -> String {
        let encoder = TextEncoder::new();
        let mut bytes = Vec::new();
        encoder
            .encode(&self.registry.gather(), &mut bytes)
            .expect("encode registered metrics");
        let mut out = String::from_utf8(bytes).expect("Prometheus text is UTF-8");

        write!(
            out,
            "# HELP blockd_host_info Static identity of this daemon host.\n\
             # TYPE blockd_host_info gauge\n\
             blockd_host_info{{host_id=\"{}\"}} 1\n",
            snapshot.host
        )
        .expect("string write");

        out.push_str("# HELP blockd_vms Current VMs by lifecycle state and backup mode.\n");
        out.push_str("# TYPE blockd_vms gauge\n");
        for ((state, backed), count) in &snapshot.vms {
            writeln!(
                out,
                "blockd_vms{{state=\"{}\",backed=\"{backed}\"}} {count}",
                escape_label(state)
            )
            .expect("string write");
        }

        append_runtime_counters(&mut out, &snapshot.runtime);
        append_counter(
            &mut out,
            "blockd_peer_dropped_sends_total",
            "Peer frames dropped because a queue was full or a peer was unavailable.",
            snapshot.peer_dropped_sends,
        );
        out.push_str("# HELP blockd_peer_connected Whether the outbound peer connection is currently established.\n");
        out.push_str("# TYPE blockd_peer_connected gauge\n");
        for (peer, connected) in &snapshot.peer_connections {
            writeln!(
                out,
                "blockd_peer_connected{{peer_host=\"{peer}\"}} {}",
                u8::from(*connected)
            )
            .expect("string write");
        }
        append_counter(
            &mut out,
            "blockd_incidents_total",
            "Runtime incidents recorded by this process.",
            snapshot.incidents,
        );
        append_store_metrics(&mut out, snapshot.store);
        append_daemon_state(&mut out, &snapshot.daemon);
        append_backup_lag_age(&mut out, &snapshot.backup_lag_age);
        append_active_operation_age(&mut out, &snapshot.active_operation_age);
        if let Some((capacity, available)) = snapshot.blob_filesystem_space {
            out.push_str(
                "# HELP blockd_blob_filesystem_bytes Filesystem holding local durable blobs.\n",
            );
            out.push_str("# TYPE blockd_blob_filesystem_bytes gauge\n");
            writeln!(
                out,
                "blockd_blob_filesystem_bytes{{state=\"capacity\"}} {capacity}"
            )
            .expect("string write");
            writeln!(
                out,
                "blockd_blob_filesystem_bytes{{state=\"available\"}} {available}"
            )
            .expect("string write");
            writeln!(
                out,
                "blockd_blob_filesystem_bytes{{state=\"used\"}} {}",
                capacity.saturating_sub(available)
            )
            .expect("string write");
        }
        append_loop_metrics(&mut out, snapshot);
        append_fault_latency(&mut out, &snapshot.fault_latency);
        append_latency_histogram(
            &mut out,
            "blockd_runtime_operation_duration_seconds",
            "Synchronous runtime operation duration in seconds.",
            &snapshot.operation_latency,
        );
        append_latency_histogram(
            &mut out,
            "blockd_guest_pause_duration_seconds",
            "Guest-visible pause duration for checkpoint and migration cutover.",
            &snapshot.guest_pause_latency,
        );
        append_source_histograms(
            &mut out,
            "blockd_firecracker_page_fault_duration_seconds",
            "End-to-end Firecracker snapshot-memory fault service time.",
            &snapshot.firecracker_fault_latency,
        );
        append_latency_histogram(
            &mut out,
            "blockd_local_io_duration_seconds",
            "Local durable-blob operation service time in seconds.",
            &snapshot.local_io_latency,
        );
        append_latency_histogram(
            &mut out,
            "blockd_store_request_duration_seconds",
            "Object-store request duration in seconds, including authentication retries.",
            &snapshot.store_latency,
        );
        out.push_str(
            "# HELP blockd_local_io_in_flight Local durable-blob operations currently executing.\n",
        );
        out.push_str("# TYPE blockd_local_io_in_flight gauge\n");
        for (operation, value) in &snapshot.local_io_in_flight {
            writeln!(
                out,
                "blockd_local_io_in_flight{{operation=\"{}\"}} {value}",
                escape_label(operation)
            )
            .expect("string write");
        }
        out
    }

    pub fn content_type() -> &'static str {
        prometheus::TEXT_FORMAT
    }
}

fn append_source_histograms(
    out: &mut String,
    name: &str,
    help: &str,
    series: &[(&'static str, HistogramSnapshot)],
) {
    writeln!(out, "# HELP {name} {help}").expect("string write");
    writeln!(out, "# TYPE {name} histogram").expect("string write");
    let mut aggregate: BTreeMap<&str, HistogramSnapshot> = BTreeMap::new();
    for (source, histogram) in series {
        let entry = aggregate
            .entry(source)
            .or_insert_with(|| HistogramSnapshot {
                buckets: vec![0; LATENCY_BUCKETS_NS.len()],
                count: 0,
                sum_ns: 0,
            });
        for (total, value) in entry.buckets.iter_mut().zip(&histogram.buckets) {
            *total += value;
        }
        entry.count += histogram.count;
        entry.sum_ns += histogram.sum_ns;
    }
    for (source, histogram) in aggregate {
        append_histogram_sample(out, name, &[("source", source)], &histogram);
    }
}

#[allow(clippy::too_many_lines)]
fn append_daemon_state(out: &mut String, stats: &DaemonStats) {
    out.push_str("# HELP blockd_cache_pages Host cache pages by state.\n");
    out.push_str("# TYPE blockd_cache_pages gauge\n");
    for (state, value) in [
        ("capacity", stats.cache_capacity_pages),
        ("resident_private", stats.resident_pages),
        ("resident_shared", stats.shared_resident_pages),
        ("reserved", stats.reserved_pages),
        ("dirty", stats.dirty_pages),
        ("unstable", stats.unstable_pages),
    ] {
        writeln!(out, "blockd_cache_pages{{state=\"{state}\"}} {value}").expect("string write");
    }
    append_gauge(
        out,
        "blockd_pressure_waiting_faults",
        "Page faults currently waiting for a cache slot.",
        stats.pressure_waiting_faults,
    );
    append_gauge(
        out,
        "blockd_parked_faults",
        "All page faults currently parked on pressure, storage, or map hydration.",
        stats.parked_faults,
    );

    out.push_str("# HELP blockd_nvme_bytes Local durable storage bytes by state.\n");
    out.push_str("# TYPE blockd_nvme_bytes gauge\n");
    for (state, value) in [
        ("used", stats.local_blob_bytes),
        ("segment_live", stats.live_segment_bytes),
        ("segment_stored", stats.local_segment_bytes),
        ("headroom", stats.disk_headroom_bytes),
    ] {
        writeln!(out, "blockd_nvme_bytes{{state=\"{state}\"}} {value}").expect("string write");
    }
    if let Some(capacity) = stats.disk_capacity_bytes {
        writeln!(out, "blockd_nvme_bytes{{state=\"capacity\"}} {capacity}").expect("string write");
    }

    out.push_str("# HELP blockd_vset_state Current vset lifecycle state.\n");
    out.push_str("# TYPE blockd_vset_state gauge\n");
    out.push_str("# HELP blockd_vset_pages Current per-vset pages by operational state.\n");
    out.push_str("# TYPE blockd_vset_pages gauge\n");
    out.push_str("# HELP blockd_vset_pending Current per-vset pending work.\n");
    out.push_str("# TYPE blockd_vset_pending gauge\n");
    out.push_str(
        "# HELP blockd_vset_backup_lag_captures Durable local captures not yet published.\n",
    );
    out.push_str("# TYPE blockd_vset_backup_lag_captures gauge\n");
    out.push_str(
        "# HELP blockd_vset_backup_lag_bytes Durable local segment bytes not yet published.\n",
    );
    out.push_str("# TYPE blockd_vset_backup_lag_bytes gauge\n");
    out.push_str(
        "# HELP blockd_vset_operation_in_progress Whether per-vset background work is active.\n",
    );
    out.push_str("# TYPE blockd_vset_operation_in_progress gauge\n");
    out.push_str("# HELP blockd_vset_segment_bytes Per-vset segment bytes by state.\n");
    out.push_str("# TYPE blockd_vset_segment_bytes gauge\n");
    for vset in &stats.vsets {
        let id = vset.vset.0;
        let lifecycle = match vset.role {
            VsetRole::Initializing => "initializing",
            VsetRole::Serving => "serving",
            VsetRole::Hydrating => "hydrating",
            VsetRole::Outbound => "outbound",
        };
        writeln!(
            out,
            "blockd_vset_state{{vset_id=\"{id}\",state=\"{lifecycle}\",backed=\"{}\"}} 1",
            vset.backed_up
        )
        .expect("string write");
        for (page_state, value) in [
            ("dirty", vset.dirty_pages),
            ("unstable", vset.unstable_pages),
            ("parked", vset.parked_faults),
            ("hydration_remaining", vset.hydration_remaining_pages),
        ] {
            writeln!(
                out,
                "blockd_vset_pages{{vset_id=\"{id}\",state=\"{page_state}\"}} {value}"
            )
            .expect("string write");
        }
        for (kind, value) in [
            ("sync", vset.pending_syncs),
            ("map_leaf", vset.pending_leaf_spans),
        ] {
            writeln!(
                out,
                "blockd_vset_pending{{vset_id=\"{id}\",kind=\"{kind}\"}} {value}"
            )
            .expect("string write");
        }
        if let Some(lag) = vset.backup_lag_captures {
            writeln!(
                out,
                "blockd_vset_backup_lag_captures{{vset_id=\"{id}\"}} {lag}"
            )
            .expect("string write");
        }
        if let Some(bytes) = vset.backup_lag_bytes {
            writeln!(
                out,
                "blockd_vset_backup_lag_bytes{{vset_id=\"{id}\"}} {bytes}"
            )
            .expect("string write");
        }
        for (operation, active) in [
            ("capture", vset.operations.active(VsetOperations::CAPTURE)),
            (
                "checkpoint",
                vset.operations.active(VsetOperations::CHECKPOINT),
            ),
            ("backup", vset.operations.active(VsetOperations::BACKUP)),
            (
                "hydration",
                vset.operations.active(VsetOperations::HYDRATION),
            ),
        ] {
            writeln!(
                out,
                "blockd_vset_operation_in_progress{{vset_id=\"{id}\",operation=\"{operation}\"}} {}",
                u8::from(active)
            )
            .expect("string write");
        }
        for (state, value) in [
            ("live", vset.live_segment_bytes),
            ("stored", vset.local_segment_bytes),
        ] {
            writeln!(
                out,
                "blockd_vset_segment_bytes{{vset_id=\"{id}\",state=\"{state}\"}} {value}"
            )
            .expect("string write");
        }
    }
}

fn append_backup_lag_age(out: &mut String, lag_age: &[(u64, f64)]) {
    out.push_str(
        "# HELP blockd_vset_backup_lag_seconds Continuous time with unpublished captures.\n",
    );
    out.push_str("# TYPE blockd_vset_backup_lag_seconds gauge\n");
    for (vset, seconds) in lag_age {
        writeln!(
            out,
            "blockd_vset_backup_lag_seconds{{vset_id=\"{vset}\"}} {seconds}"
        )
        .expect("string write");
    }
}

fn append_active_operation_age(out: &mut String, ages: &[(u64, &'static str, f64)]) {
    out.push_str(
        "# HELP blockd_vset_operation_active_seconds Continuous time the current background operation has been active.\n",
    );
    out.push_str("# TYPE blockd_vset_operation_active_seconds gauge\n");
    for (vset, operation, seconds) in ages {
        writeln!(
            out,
            "blockd_vset_operation_active_seconds{{vset_id=\"{vset}\",operation=\"{operation}\"}} {seconds}"
        )
        .expect("string write");
    }
}

#[allow(clippy::cast_precision_loss)]
fn append_loop_metrics(out: &mut String, snapshot: &MetricsSnapshot) {
    out.push_str(
        "# HELP blockd_event_loop_events_total Event-loop decisions and effects by kind.\n",
    );
    out.push_str("# TYPE blockd_event_loop_events_total counter\n");
    out.push_str(
        "# HELP blockd_event_loop_seconds_total Event-loop wall time by phase and kind.\n",
    );
    out.push_str("# TYPE blockd_event_loop_seconds_total counter\n");
    for (phase, rows) in [
        ("decide", snapshot.loop_decide.as_slice()),
        ("effect", snapshot.loop_effect.as_slice()),
    ] {
        for (kind, count, ns) in rows {
            writeln!(
                out,
                "blockd_event_loop_events_total{{phase=\"{phase}\",kind=\"{kind}\"}} {count}"
            )
            .expect("string write");
            writeln!(
                out,
                "blockd_event_loop_seconds_total{{phase=\"{phase}\",kind=\"{kind}\"}} {}",
                *ns as f64 / 1_000_000_000.0
            )
            .expect("string write");
        }
    }
    writeln!(
        out,
        "blockd_event_loop_seconds_total{{phase=\"idle\",kind=\"all\"}} {}",
        snapshot.loop_idle_ns as f64 / 1_000_000_000.0
    )
    .expect("string write");
    out.push_str("# HELP blockd_event_loop_occupancy_ratio Cumulative fraction of observed loop time spent busy.\n");
    out.push_str("# TYPE blockd_event_loop_occupancy_ratio gauge\n");
    writeln!(
        out,
        "blockd_event_loop_occupancy_ratio {}",
        snapshot.loop_occupancy
    )
    .expect("string write");
    out.push_str("# HELP blockd_event_loop_queue_depth Current queued events by priority lane.\n");
    out.push_str("# TYPE blockd_event_loop_queue_depth gauge\n");
    writeln!(
        out,
        "blockd_event_loop_queue_depth{{priority=\"critical\"}} {}",
        snapshot.loop_queue_depths.0
    )
    .expect("string write");
    writeln!(
        out,
        "blockd_event_loop_queue_depth{{priority=\"background\"}} {}",
        snapshot.loop_queue_depths.1
    )
    .expect("string write");
}

#[allow(clippy::cast_precision_loss)]
fn append_fault_latency(out: &mut String, series: &[FaultLatencySnapshot]) {
    out.push_str("# HELP blockd_page_fault_duration_seconds End-to-end page-fault service time by final source.\n");
    out.push_str("# TYPE blockd_page_fault_duration_seconds histogram\n");
    out.push_str("# HELP blockd_vset_page_faults_total Per-vset page faults by final source.\n");
    out.push_str("# TYPE blockd_vset_page_faults_total counter\n");
    out.push_str("# HELP blockd_vset_page_fault_duration_seconds_total Per-vset cumulative page-fault service time by final source.\n");
    out.push_str("# TYPE blockd_vset_page_fault_duration_seconds_total counter\n");
    let mut aggregate: BTreeMap<&str, HistogramSnapshot> = BTreeMap::new();
    for item in series {
        let entry = aggregate
            .entry(item.source)
            .or_insert_with(|| HistogramSnapshot {
                buckets: vec![0; LATENCY_BUCKETS_NS.len()],
                count: 0,
                sum_ns: 0,
            });
        for (total, value) in entry.buckets.iter_mut().zip(&item.histogram.buckets) {
            *total += value;
        }
        entry.count += item.histogram.count;
        entry.sum_ns += item.histogram.sum_ns;
        writeln!(
            out,
            "blockd_vset_page_faults_total{{vset_id=\"{}\",source=\"{}\"}} {}",
            item.vset, item.source, item.histogram.count
        )
        .expect("string write");
        writeln!(
            out,
            "blockd_vset_page_fault_duration_seconds_total{{vset_id=\"{}\",source=\"{}\"}} {}",
            item.vset,
            item.source,
            item.histogram.sum_ns as f64 / 1_000_000_000.0
        )
        .expect("string write");
    }
    for (source, histogram) in aggregate {
        append_histogram_sample(
            out,
            "blockd_page_fault_duration_seconds",
            &[("source", source)],
            &histogram,
        );
    }
}

fn append_latency_histogram(out: &mut String, name: &str, help: &str, series: &[LatencySeries]) {
    writeln!(out, "# HELP {name} {help}").expect("string write");
    writeln!(out, "# TYPE {name} histogram").expect("string write");
    for item in series {
        append_histogram_sample(
            out,
            name,
            &[("operation", item.operation), ("outcome", item.outcome)],
            &item.histogram,
        );
    }
}

#[allow(clippy::cast_precision_loss)]
fn append_histogram_sample(
    out: &mut String,
    name: &str,
    labels: &[(&str, &str)],
    histogram: &HistogramSnapshot,
) {
    let label_text = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", escape_label(value)))
        .collect::<Vec<_>>()
        .join(",");
    for (upper_ns, count) in LATENCY_BUCKETS_NS.iter().zip(&histogram.buckets) {
        let upper = *upper_ns as f64 / 1_000_000_000.0;
        writeln!(out, "{name}_bucket{{{label_text},le=\"{upper}\"}} {count}")
            .expect("string write");
    }
    writeln!(
        out,
        "{name}_bucket{{{label_text},le=\"+Inf\"}} {}",
        histogram.count
    )
    .expect("string write");
    writeln!(
        out,
        "{name}_sum{{{label_text}}} {}",
        histogram.sum_ns as f64 / 1_000_000_000.0
    )
    .expect("string write");
    writeln!(out, "{name}_count{{{label_text}}} {}", histogram.count).expect("string write");
}

fn append_gauge(out: &mut String, name: &str, help: &str, value: usize) {
    writeln!(out, "# HELP {name} {help}").expect("string write");
    writeln!(out, "# TYPE {name} gauge").expect("string write");
    writeln!(out, "{name} {value}").expect("string write");
}

fn append_runtime_counters(out: &mut String, counters: &Counters) {
    macro_rules! counters {
        ($($field:ident => $help:literal),+ $(,)?) => {
            $(append_counter(
                out,
                concat!("blockd_runtime_", stringify!($field), "_total"),
                $help,
                counters.$field,
            );)+
        };
    }
    counters! {
        fills => "Missing page faults served from storage.",
        zero_fills => "Missing page faults served with an unwritten zero page.",
        shared_fills => "Missing page faults served from shared resident base pages.",
        wp_faults => "First writes caught by write protection.",
        guest_pages_dirtied => "Guest pages transitioning from clean to dirty.",
        faults_unservable => "Page faults that could not be served from any intact copy.",
        pressure_waits => "Guest operations delayed by cache pressure.",
        pages_flushed => "Dirty pages flushed to durable segments.",
        records_written => "Journal record copies written.",
        checkpoints_done => "Checkpoints completed.",
        syncs_acked => "Guest sync requests acknowledged.",
        guest_rejected => "Guest operations rejected by lifecycle guards.",
        peer_rejected => "Peer messages rejected by protocol guards.",
        blobs_deleted => "Local blobs deleted after becoming unreachable.",
        manifests_published => "Backup manifests published to object storage.",
        store_retries => "Store operations deferred for retry after transient faults.",
        fenced => "Vsets lost to a newer ownership claim.",
        assignment_claims => "Successful object-store assignment claims.",
        assignment_claim_conflicts => "Assignment claims lost to another holder.",
        nvme_reclaims => "Backed local segments reclaimed under capacity pressure.",
        nvme_stalls => "Captures stalled by local disk capacity.",
        prefetch_fills => "Pages prefetched after restore.",
        hydrate_fills => "Migration tail pages hydrated in the background.",
        peer_retries => "Peer fetches retried after no response.",
        cow_captures => "Snapshot pages captured on a concurrent guest write.",
        wedged_guests => "Guest-service liveness wedge detections.",
        wedged_hydration => "Hydration liveness wedge detections.",
        wedged_outbound => "Outbound migration liveness wedge detections.",
        leaf_rolls => "Map spans rolled into new leaf blobs.",
        leaf_fills => "Map leaves hydrated lazily.",
        segs_compacted => "Mostly-dead segments compacted.",
        pages_compacted => "Live pages rewritten by compaction.",
    }
}

fn append_store_metrics(out: &mut String, store: StoreMetrics) {
    out.push_str("# HELP blockd_store_requests_total Object-store requests by operation.\n");
    out.push_str("# TYPE blockd_store_requests_total counter\n");
    for (operation, value) in [
        ("put", store.puts),
        ("conditional_put", store.cas_puts),
        ("get", store.gets),
        ("ranged_get", store.ranged_gets),
        ("delete", store.deletes),
    ] {
        writeln!(
            out,
            "blockd_store_requests_total{{operation=\"{operation}\"}} {value}"
        )
        .expect("string write");
    }
    append_counter(
        out,
        "blockd_store_precondition_failures_total",
        "Object-store conditional writes rejected by generation preconditions.",
        store.precondition_failures,
    );
    append_counter(
        out,
        "blockd_store_unavailable_total",
        "Object-store requests that failed with a transient availability error.",
        store.unavailable,
    );
    append_counter(
        out,
        "blockd_store_token_refreshes_total",
        "Object-store authentication token refreshes.",
        store.token_refreshes,
    );
    out.push_str("# HELP blockd_store_transferred_bytes_total Object-store bytes transferred by direction.\n");
    out.push_str("# TYPE blockd_store_transferred_bytes_total counter\n");
    writeln!(
        out,
        "blockd_store_transferred_bytes_total{{direction=\"up\"}} {}",
        store.bytes_up
    )
    .expect("string write");
    writeln!(
        out,
        "blockd_store_transferred_bytes_total{{direction=\"down\"}} {}",
        store.bytes_down
    )
    .expect("string write");
}

fn append_counter(out: &mut String, name: &str, help: &str, value: u64) {
    writeln!(out, "# HELP {name} {help}").expect("string write");
    writeln!(out, "# TYPE {name} counter").expect("string write");
    writeln!(out, "{name} {value}").expect("string write");
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_use_normalized_routes_and_include_runtime_state() {
        let metrics = Arc::new(Metrics::new());
        let mut request = metrics.start_request("POST", "/vm/{id}/work");
        request.set_status(200);
        drop(request);
        let mut vms = BTreeMap::new();
        vms.insert(("running".to_owned(), true), 2);
        let snapshot = MetricsSnapshot {
            host: 7,
            vms,
            runtime: Counters {
                fills: 11,
                wedged_guests: 1,
                ..Counters::default()
            },
            store: StoreMetrics {
                gets: 3,
                bytes_down: 4096,
                ..StoreMetrics::default()
            },
            peer_dropped_sends: 2,
            peer_connections: vec![(8, false)],
            incidents: 4,
            daemon: DaemonStats {
                cache_capacity_pages: 100,
                dirty_pages: 3,
                vsets: vec![blockd_core::daemon::VsetStats {
                    vset: blockd_core::types::VsetId(42),
                    backed_up: true,
                    role: VsetRole::Hydrating,
                    fence: 8,
                    dirty_pages: 3,
                    unstable_pages: 4,
                    parked_faults: 1,
                    pending_syncs: 2,
                    pending_leaf_spans: 5,
                    hydration_remaining_pages: 9,
                    backup_lag_captures: Some(6),
                    backup_lag_bytes: Some(3072),
                    operations: VsetOperations::default(),
                    live_segment_bytes: 1024,
                    local_segment_bytes: 2048,
                }],
                ..DaemonStats::default()
            },
            loop_decide: vec![("GuestFault", 2, 1_000)],
            loop_effect: Vec::new(),
            loop_idle_ns: 0,
            loop_occupancy: 0.0,
            loop_queue_depths: (0, 0),
            fault_latency: vec![FaultLatencySnapshot {
                vset: 42,
                source: "local_nvme",
                histogram: HistogramSnapshot {
                    buckets: vec![1; LATENCY_BUCKETS_NS.len()],
                    count: 1,
                    sum_ns: 5_000,
                },
            }],
            operation_latency: Vec::new(),
            guest_pause_latency: Vec::new(),
            local_io_latency: Vec::new(),
            local_io_in_flight: Vec::new(),
            store_latency: Vec::new(),
            firecracker_fault_latency: Vec::new(),
            blob_filesystem_space: None,
            backup_lag_age: vec![(42, 12.5)],
            active_operation_age: vec![(42, "hydration", 8.25)],
        };

        let text = metrics.encode(&snapshot);
        assert!(text.contains(
            "blockd_http_requests_total{method=\"POST\",route=\"/vm/{id}/work\",status_code=\"200\"} 1"
        ));
        assert!(text.contains("blockd_runtime_fills_total 11"));
        assert!(text.contains("blockd_runtime_wedged_guests_total 1"));
        assert!(text.contains("blockd_peer_connected{peer_host=\"8\"} 0"));
        assert!(text.contains("blockd_store_requests_total{operation=\"get\"} 3"));
        assert!(text.contains("blockd_vms{state=\"running\",backed=\"true\"} 2"));
        assert!(text.contains("blockd_cache_pages{state=\"dirty\"} 3"));
        assert!(text.contains("blockd_vset_backup_lag_captures{vset_id=\"42\"} 6"));
        assert!(text.contains("blockd_vset_backup_lag_bytes{vset_id=\"42\"} 3072"));
        assert!(text.contains("blockd_vset_backup_lag_seconds{vset_id=\"42\"} 12.5"));
        assert!(text.contains(
            "blockd_vset_operation_active_seconds{vset_id=\"42\",operation=\"hydration\"} 8.25"
        ));
        assert!(
            text.contains("blockd_vset_page_faults_total{vset_id=\"42\",source=\"local_nvme\"} 1")
        );
        assert!(text.contains("blockd_page_fault_duration_seconds_count{source=\"local_nvme\"} 1"));
        assert!(
            text.contains("blockd_event_loop_events_total{phase=\"decide\",kind=\"GuestFault\"} 2")
        );
        assert!(!text.contains("/vm/42/work"));
    }

    #[test]
    fn labels_are_escaped_for_prometheus_text_format() {
        assert_eq!(escape_label("a\\b\n\"c"), "a\\\\b\\n\\\"c");
    }
}
