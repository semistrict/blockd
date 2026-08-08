//! The demo control API, served by Axum on Tokio. It has no authentication;
//! bind it only to the cluster's internal management network.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use axum::Json;
use axum::Router;
use axum::extract::{ConnectInfo, MatchedPath, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt as _;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::observability::{Metrics, MetricsSnapshot, StoreMetrics};
use crate::vm::Demod;

const MAX_REQUEST_BODY_BYTES: usize = 1024;
const MAX_OPERATION_CONCURRENCY: usize = 8;
const MAX_BACKGROUND_CONCURRENCY: usize = 32;
const MAX_OBSERVATION_CONCURRENCY: usize = 2;
const REQUEST_TIMEOUT: Duration = Duration::from_mins(5);

#[derive(Clone)]
struct ApiState {
    daemon: Arc<Demod>,
    operation_slots: Arc<Semaphore>,
    background_slots: Arc<Semaphore>,
    observation_slots: Arc<Semaphore>,
}

pub async fn serve(daemon: Arc<Demod>) {
    let address = daemon.cfg.api;
    let host_id = daemon.cfg.host.0;
    let state = ApiState {
        daemon,
        operation_slots: Arc::new(Semaphore::new(MAX_OPERATION_CONCURRENCY)),
        background_slots: Arc::new(Semaphore::new(MAX_BACKGROUND_CONCURRENCY)),
        observation_slots: Arc::new(Semaphore::new(MAX_OBSERVATION_CONCURRENCY)),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("api listen");
    tracing::info!(
        host_id,
        listen_address = %address,
        max_operation_concurrency = MAX_OPERATION_CONCURRENCY,
        max_background_concurrency = MAX_BACKGROUND_CONCURRENCY,
        "control API serving"
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .expect("serve control API");
}

async fn shutdown_signal() {
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        signal.recv().await;
    };
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.expect("install SIGINT handler"),
        () = terminate => {}
    }
    tracing::info!("shutdown requested");
}

fn router(state: ApiState) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/base", post(base))
        .route("/vm", post(start_vm))
        .route("/vm/{id}/work", post(work))
        .route("/vm/{id}/verify", post(verify))
        .route("/vm/{id}/fork", post(fork))
        .route("/vm/{id}/expect", post(expect))
        .route("/vm/{id}/migrate", post(migrate))
        .route("/vm/{id}/restore", post(restore))
        .fallback(fallback)
        .layer(RequestBodyLimitLayer::new(MAX_REQUEST_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            observe_request,
        ))
        .with_state(state)
}

async fn observe_request(
    State(state): State<ApiState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str)
        .to_owned();
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(peer)| *peer);
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    let mut metric = state.daemon.metrics.start_request(method.as_str(), &route);
    let started = Instant::now();
    let span = tracing::info_span!(
        "http.request",
        "otel.name" = format!("{method} {route}"),
        "otel.kind" = "server",
        "http.request.method" = %method,
        "url.path" = %path,
        "url.scheme" = "http",
        "http.route" = %route,
        "http.response.status_code" = tracing::field::Empty,
        "otel.status_code" = tracing::field::Empty,
        "client.address" = tracing::field::Empty,
        "client.port" = tracing::field::Empty,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
    );
    let _ = span.set_parent(parent);
    let context = span.context();
    let otel_span = context.span();
    let span_context = otel_span.span_context();
    if span_context.is_valid() {
        span.record("trace_id", span_context.trace_id().to_string());
        span.record("span_id", span_context.span_id().to_string());
    }
    if let Some(peer) = peer {
        span.record("client.address", peer.ip().to_string());
        span.record("client.port", peer.port());
    }

    let response = next.run(request).instrument(span.clone()).await;
    let status = response.status();
    metric.set_status(status.as_u16());
    span.record("http.response.status_code", status.as_u16());
    if status.is_server_error() {
        span.record("otel.status_code", "ERROR");
    }
    tracing::info!(
        parent: &span,
        status_code = status.as_u16(),
        duration_ms = started.elapsed().as_secs_f64() * 1000.0,
        "control API request completed"
    );
    response
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(axum::http::HeaderName::as_str).collect()
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> ApiError {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> ApiError {
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

async fn run_blocking<T, F>(state: &ApiState, operation: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce(Arc<Demod>) -> T + Send + 'static,
{
    let permit = state
        .operation_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("operation executor closed"))?;
    let daemon = state.daemon.clone();
    let parent = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        parent.in_scope(|| operation(daemon))
    })
    .await
    .map_err(|error| ApiError::internal(format!("operation failed: {error}")))
}

async fn start_background<F>(state: &ApiState, operation: F) -> ApiResult<()>
where
    F: FnOnce(Arc<Demod>, tokio::sync::oneshot::Sender<()>) + Send + 'static,
{
    let permit = state
        .background_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("background executor closed"))?;
    let daemon = state.daemon.clone();
    let parent = tracing::Span::current();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        parent.in_scope(|| operation(daemon, ready_tx));
    });
    tokio::spawn(async move {
        if let Err(error) = handle.await {
            tracing::error!(%error, "background operation failed");
        }
    });
    ready_rx
        .await
        .map_err(|_| ApiError::internal("background operation failed during startup"))
}

async fn run_observation<T, F>(state: &ApiState, observation: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce(Arc<Demod>) -> T + Send + 'static,
{
    let permit = state
        .observation_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("observation executor closed"))?;
    let daemon = state.daemon.clone();
    let parent = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        parent.in_scope(|| observation(daemon))
    })
    .await
    .map_err(|error| ApiError::internal(format!("observation failed: {error}")))
}

fn arg(query: &BTreeMap<String, String>, key: &str, default: u64) -> u64 {
    query
        .get(key)
        .map_or(default, |value| value.parse().unwrap_or(default))
}

fn parse_id(id: &str) -> ApiResult<u64> {
    id.parse().map_err(|_| ApiError::bad_request("bad id"))
}

async fn status(State(state): State<ApiState>) -> ApiResult<Json<Value>> {
    let value = run_observation(&state, |daemon| status_value(&daemon)).await?;
    Ok(Json(value))
}

async fn metrics(State(state): State<ApiState>) -> ApiResult<Response> {
    let body = run_observation(&state, |daemon| metrics_text(&daemon)).await?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, Metrics::content_type())],
        body,
    )
        .into_response())
}

async fn base(State(state): State<ApiState>) -> ApiResult<Json<Value>> {
    let sum = run_blocking(&state, |daemon| daemon.bake_base()).await?;
    Ok(Json(json!({ "baked": true, "sum": sum })))
}

async fn start_vm(
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let backed = arg(&query, "backed", 0) == 1;
    let id = run_blocking(&state, move |daemon| daemon.start_vm(backed)).await?;
    Ok(Json(json!({ "id": id })))
}

async fn work(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let id = parse_id(&id)?;
    let bursts = arg(&query, "bursts", 1);
    let (burst, sum) = run_blocking(&state, move |daemon| daemon.work(id, bursts)).await?;
    Ok(Json(json!({ "id": id, "burst": burst, "guest_sum": sum })))
}

async fn verify(State(state): State<ApiState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let id = parse_id(&id)?;
    let (burst, mismatches) = run_blocking(&state, move |daemon| daemon.verify(id)).await?;
    Ok(Json(json!({
        "id": id,
        "burst": burst,
        "mismatches": mismatches,
        "ok": mismatches == 0,
    })))
}

async fn fork(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let id = parse_id(&id)?;
    let n = u32::try_from(arg(&query, "n", 3)).map_err(|_| ApiError::bad_request("bad n"))?;
    let (ids, rss, pss, resident) = run_blocking(&state, move |daemon| daemon.fork(id, n)).await?;
    Ok(Json(json!({
        "forks": ids,
        "rss_sum": rss,
        "pss_sum": pss,
        "base_resident": resident,
    })))
}

async fn expect(State(state): State<ApiState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let id = parse_id(&id)?;
    start_background(&state, move |daemon, ready| {
        daemon.expect(id, || {
            let _ = ready.send(());
        });
    })
    .await?;
    Ok(Json(json!({ "id": id, "expecting": true })))
}

async fn migrate(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> ApiResult<Json<Value>> {
    let id = parse_id(&id)?;
    let to = u16::try_from(arg(&query, "to", 1)).map_err(|_| ApiError::bad_request("bad to"))?;
    let timings = run_blocking(&state, move |daemon| daemon.migrate(id, to)).await?;
    Ok(Json(json!({
        "id": id,
        "to": to,
        "snapshot_ms": timings.snapshot_write_ms + timings.publish_ms,
        "snapshot_write_ms": timings.snapshot_write_ms,
        "publish_ms": timings.publish_ms,
        "handoff_ms": timings.handoff_ms,
        "migration_ms": timings.total_ms,
        "overlap_ms": timings.overlap_ms,
    })))
}

async fn restore(State(state): State<ApiState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let id = parse_id(&id)?;
    let verdict = run_blocking(&state, move |daemon| daemon.restore(id)).await?;
    Ok(Json(json!({ "id": id, "verdict": verdict })))
}

async fn fallback(method: axum::http::Method, uri: Uri) -> ApiError {
    ApiError {
        status: StatusCode::NOT_FOUND,
        message: format!("no route for {method} {}", uri.path()),
    }
}

#[allow(clippy::too_many_lines)]
fn metrics_text(state: &Arc<Demod>) -> String {
    let mut vms = BTreeMap::new();
    for vm in state.vms.lock().expect("lock").values() {
        *vms.entry((vm.state.clone(), vm.backed)).or_insert(0) += 1;
    }
    let store = &state.store.stats;
    let loop_stats = state.rt.loop_stats();
    let snapshot = MetricsSnapshot {
        host: state.cfg.host.0,
        vms,
        runtime: state.rt.counters(),
        store: StoreMetrics {
            puts: store.puts.load(Ordering::SeqCst),
            cas_puts: store.cas_puts.load(Ordering::SeqCst),
            gets: store.gets.load(Ordering::SeqCst),
            ranged_gets: store.ranged_gets.load(Ordering::SeqCst),
            precondition_failures: store.precondition_failures.load(Ordering::SeqCst),
            deletes: store.deletes.load(Ordering::SeqCst),
            unavailable: store.unavailable.load(Ordering::SeqCst),
            token_refreshes: store.token_refreshes.load(Ordering::SeqCst),
            bytes_up: store.bytes_up.load(Ordering::SeqCst),
            bytes_down: store.bytes_down.load(Ordering::SeqCst),
        },
        peer_dropped_sends: state.rt.peer_dropped_sends(),
        peer_connections: state
            .rt
            .peer_connections()
            .into_iter()
            .map(|(peer, connected)| (peer.0, connected))
            .collect(),
        incidents: u64::try_from(state.rt.incidents().len()).unwrap_or(u64::MAX),
        daemon: state.rt.daemon_stats(),
        capacity: state.rt.capacity_signal(),
        loop_decide: loop_stats.decide_totals(),
        loop_effect: loop_stats.effect_totals(),
        loop_idle_ns: loop_stats.idle_ns(),
        loop_occupancy: loop_stats.occupancy(),
        loop_queue_depths: state.rt.loop_queue_depths(),
        fault_latency: state.rt.fault_latency(),
        operation_latency: state.rt.operation_latency(),
        guest_pause_latency: state.rt.guest_pause_latency(),
        local_io_latency: state.rt.local_io_latency(),
        local_io_in_flight: state.rt.local_io_in_flight(),
        store_latency: store.latency(),
        firecracker_fault_latency: state.firecracker_fault_latency(),
        blob_filesystem_space: state.rt.blob_filesystem_space(),
        backup_lag_age: state
            .rt
            .backup_lag_age()
            .into_iter()
            .map(|(vset, age)| (vset.0, age.as_secs_f64()))
            .collect(),
        active_operation_age: state
            .rt
            .active_operation_age()
            .into_iter()
            .map(|(vset, operation, age)| (vset.0, operation, age.as_secs_f64()))
            .collect(),
    };
    state.metrics.encode(&snapshot)
}

fn status_value(state: &Arc<Demod>) -> Value {
    let vms = state
        .vms
        .lock()
        .expect("lock")
        .iter()
        .map(|(id, vm)| {
            json!({
                "id": id,
                "state": vm.state,
                "backed": vm.backed,
                "prefix": vm.prefix,
            })
        })
        .collect::<Vec<_>>();
    let counters = state.rt.counters();
    let capacity = state.rt.capacity_signal();
    let store = &state.store.stats;
    json!({
        "host": state.cfg.host.0,
        "vms": vms,
        "capacity": capacity_value(capacity),
        "counters": {
            "fills": counters.fills,
            "pages_flushed": counters.pages_flushed,
            "records_written": counters.records_written,
            "syncs_acked": counters.syncs_acked,
            "manifests_published": counters.manifests_published,
            "hydrate_fills": counters.hydrate_fills,
            "segs_compacted": counters.segs_compacted,
        },
        "store": {
            "puts": store.puts.load(Ordering::SeqCst),
            "cas_puts": store.cas_puts.load(Ordering::SeqCst),
            "gets": store.gets.load(Ordering::SeqCst),
            "ranged_gets": store.ranged_gets.load(Ordering::SeqCst),
            "deletes": store.deletes.load(Ordering::SeqCst),
            "unavailable": store.unavailable.load(Ordering::SeqCst),
            "bytes_up": store.bytes_up.load(Ordering::SeqCst),
            "bytes_down": store.bytes_down.load(Ordering::SeqCst),
        },
        "peer_dropped_sends": state.rt.peer_dropped_sends(),
        "incidents": state.rt.incidents().len(),
    })
}

fn capacity_value(signal: blockd_runtime::CapacitySignal) -> Value {
    json!({
        "state": signal.state.as_str(),
        "limiting_reason": signal.limiting_reason.map(blockd_runtime::CapacityReason::as_str),
        "admission_percent": signal.admission_percent,
        "allow_migrations": signal.allow_migrations,
        "allow_prefetch": signal.allow_prefetch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_extractor_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "00-abc-def-01".parse().expect("header"));
        let extractor = HeaderExtractor(&headers);
        assert_eq!(extractor.get("TraceParent"), Some("00-abc-def-01"));
    }

    #[test]
    fn invalid_ids_are_bad_requests() {
        assert_eq!(
            parse_id("not-an-id").expect_err("invalid id").status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn capacity_status_is_actionable() {
        let value = capacity_value(blockd_runtime::CapacitySignal {
            state: blockd_runtime::CapacityState::Critical,
            limiting_reason: Some(blockd_runtime::CapacityReason::DiskHeadroom),
            admission_percent: 0,
            allow_migrations: false,
            allow_prefetch: false,
        });
        assert_eq!(
            value,
            json!({
                "state": "critical",
                "limiting_reason": "disk_headroom",
                "admission_percent": 0,
                "allow_migrations": false,
                "allow_prefetch": false,
            })
        );
    }
}
