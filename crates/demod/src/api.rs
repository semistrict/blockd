//! The demo control API: hand-rolled HTTP/1.1 over `std::net` (the house
//! style — this repo already speaks HTTP to Firecracker the same way).
//! Bound to an internal address only; reach it through an SSH tunnel.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::observability::{
    FaultLatencySnapshot, LatencySeries, Metrics, MetricsSnapshot, StoreMetrics,
};
use crate::vm::Demod;

pub fn serve(state: &Arc<Demod>) {
    let listener = TcpListener::bind(state.cfg.api).expect("api listen");
    tracing::info!(
        host_id = state.cfg.host.0,
        listen_address = %state.cfg.api,
        "control API serving"
    );
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(%error, "failed to accept control API connection");
                continue;
            }
        };
        let state = state.clone();
        std::thread::spawn(move || handle(&state, stream));
    }
}

fn handle(state: &Arc<Demod>, mut stream: TcpStream) {
    let Some(request) = read_request(&mut stream) else {
        tracing::warn!("failed to read control API request");
        return;
    };
    let segments: Vec<&str> = request
        .path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let route_name = route_name(&request.method, &segments);
    let mut metric = state.metrics.start_request(&request.method, route_name);
    let started = Instant::now();
    let peer = stream.peer_addr().ok();
    let span_name = format!("{} {route_name}", request.method);
    let span = tracing::info_span!(
        "http.request",
        "otel.name" = span_name,
        "otel.kind" = "server",
        "http.request.method" = request.method,
        "url.path" = request.path,
        "url.scheme" = "http",
        "http.route" = route_name,
        "http.response.status_code" = tracing::field::Empty,
        "otel.status_code" = tracing::field::Empty,
        "client.address" = tracing::field::Empty,
        "client.port" = tracing::field::Empty,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
    );
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(&request.headers))
    });
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
    let _entered = span.enter();

    let response = match route(state, &request.method, &segments, &request.query) {
        Ok(response) => response,
        Err(message) => Response {
            status: 400,
            content_type: "application/json",
            body: format!("{{\"error\":\"{message}\"}}\n"),
        },
    };
    metric.set_status(response.status);
    span.record("http.response.status_code", response.status);
    if response.status >= 500 {
        span.record("otel.status_code", "ERROR");
    }
    tracing::info!(
        status_code = response.status,
        duration_ms = started.elapsed().as_secs_f64() * 1000.0,
        "control API request completed"
    );
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Error",
    };
    let _ = stream.write_all(
        format!(
            "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.status,
            response.content_type,
            response.body.len(),
            response.body,
        )
        .as_bytes(),
    );
}

struct Request {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
}

struct Response {
    status: u16,
    content_type: &'static str,
    body: String,
}

impl Response {
    fn json(body: String) -> Response {
        Response {
            status: 200,
            content_type: "application/json",
            body,
        }
    }
}

struct HeaderExtractor<'a>(&'a BTreeMap<String, String>);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(&key.to_ascii_lowercase()).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

fn route(
    state: &Arc<Demod>,
    method: &str,
    segments: &[&str],
    query: &BTreeMap<String, String>,
) -> Result<Response, String> {
    let arg = |key: &str, default: u64| -> u64 {
        query
            .get(key)
            .map_or(default, |value| value.parse().unwrap_or(default))
    };
    match (method, segments) {
        ("GET", ["status"]) => Ok(Response::json(status_json(state))),
        ("GET", ["metrics"]) => Ok(Response {
            status: 200,
            content_type: Metrics::content_type(),
            body: metrics_text(state),
        }),
        ("POST", ["base"]) => {
            let sum = state.bake_base();
            Ok(Response::json(format!(
                "{{\"baked\":true,\"sum\":\"{sum}\"}}\n"
            )))
        }
        ("POST", ["vm"]) => {
            let id = state.start_vm(arg("backed", 0) == 1);
            Ok(Response::json(format!("{{\"id\":{id}}}\n")))
        }
        ("POST", ["vm", id, "work"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let (burst, sum) = state.work(id, arg("bursts", 1));
            Ok(Response::json(format!(
                "{{\"id\":{id},\"burst\":{burst},\"guest_sum\":\"{sum}\"}}\n"
            )))
        }
        ("POST", ["vm", id, "verify"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let (burst, mismatches) = state.verify(id);
            Ok(Response::json(format!(
                "{{\"id\":{id},\"burst\":{burst},\"mismatches\":{mismatches},\"ok\":{}}}\n",
                mismatches == 0
            )))
        }
        ("POST", ["vm", id, "fork"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let n = u32::try_from(arg("n", 3)).map_err(|_| "bad n".to_owned())?;
            let (ids, rss, pss, resident) = state.fork(id, n);
            let ids: Vec<String> = ids.iter().map(u64::to_string).collect();
            Ok(Response::json(format!(
                "{{\"forks\":[{}],\"rss_sum\":{rss},\"pss_sum\":{pss},\"base_resident\":{resident}}}\n",
                ids.join(",")
            )))
        }
        ("POST", ["vm", id, "expect"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            state.expect(id);
            Ok(Response::json(format!(
                "{{\"id\":{id},\"expecting\":true}}\n"
            )))
        }
        ("POST", ["vm", id, "migrate"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let to = u16::try_from(arg("to", 1)).map_err(|_| "bad to".to_owned())?;
            let (snap_ms, handoff_ms) = state.migrate(id, to);
            Ok(Response::json(format!(
                "{{\"id\":{id},\"to\":{to},\"snapshot_ms\":{snap_ms},\"handoff_ms\":{handoff_ms}}}\n"
            )))
        }
        ("POST", ["vm", id, "restore"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let verdict = state.restore(id);
            Ok(Response::json(format!(
                "{{\"id\":{id},\"verdict\":\"{verdict}\"}}\n"
            )))
        }
        _ => Err(format!("no route for {method} /{}", segments.join("/"))),
    }
}

fn route_name(method: &str, segments: &[&str]) -> &'static str {
    match (method, segments) {
        ("GET", ["status"]) => "/status",
        ("GET", ["metrics"]) => "/metrics",
        ("POST", ["base"]) => "/base",
        ("POST", ["vm"]) => "/vm",
        ("POST", ["vm", _, "work"]) => "/vm/{id}/work",
        ("POST", ["vm", _, "verify"]) => "/vm/{id}/verify",
        ("POST", ["vm", _, "fork"]) => "/vm/{id}/fork",
        ("POST", ["vm", _, "expect"]) => "/vm/{id}/expect",
        ("POST", ["vm", _, "migrate"]) => "/vm/{id}/migrate",
        ("POST", ["vm", _, "restore"]) => "/vm/{id}/restore",
        _ => "unmatched",
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
        loop_decide: loop_stats.decide_totals(),
        loop_effect: loop_stats.effect_totals(),
        loop_idle_ns: loop_stats.idle_ns(),
        loop_occupancy: loop_stats.occupancy(),
        loop_queue_depths: state.rt.loop_queue_depths(),
        fault_latency: state
            .rt
            .fault_latency()
            .into_iter()
            .map(|item| FaultLatencySnapshot {
                vset: item.vset.0,
                source: item.source,
                histogram: item.histogram,
            })
            .collect(),
        operation_latency: state
            .rt
            .operation_latency()
            .into_iter()
            .map(|item| LatencySeries {
                operation: item.operation,
                outcome: item.outcome,
                histogram: item.histogram,
            })
            .collect(),
        guest_pause_latency: state
            .rt
            .guest_pause_latency()
            .into_iter()
            .map(|item| LatencySeries {
                operation: item.operation,
                outcome: "success",
                histogram: item.histogram,
            })
            .collect(),
        local_io_latency: state
            .rt
            .local_io_latency()
            .into_iter()
            .map(|item| LatencySeries {
                operation: item.operation,
                outcome: item.outcome,
                histogram: item.histogram,
            })
            .collect(),
        local_io_in_flight: state.rt.local_io_in_flight(),
        store_latency: store
            .latency()
            .into_iter()
            .map(|item| LatencySeries {
                operation: item.operation,
                outcome: item.outcome,
                histogram: item.histogram,
            })
            .collect(),
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

fn status_json(state: &Arc<Demod>) -> String {
    let mut vms = String::new();
    for (id, vm) in state.vms.lock().expect("lock").iter() {
        if !vms.is_empty() {
            vms.push(',');
        }
        write!(
            vms,
            "{{\"id\":{id},\"state\":\"{}\",\"backed\":{},\"prefix\":\"{}\"}}",
            vm.state, vm.backed, vm.prefix
        )
        .expect("string write");
    }
    let counters = state.rt.counters();
    let bill = &state.store.stats;
    format!(
        "{{\"host\":{},\"vms\":[{vms}],\
         \"counters\":{{\"fills\":{},\"pages_flushed\":{},\"records_written\":{},\
         \"syncs_acked\":{},\"manifests_published\":{},\"hydrate_fills\":{},\
         \"segs_compacted\":{}}},\
         \"store\":{{\"puts\":{},\"cas_puts\":{},\"gets\":{},\"ranged_gets\":{},\
         \"deletes\":{},\"unavailable\":{},\"bytes_up\":{},\"bytes_down\":{}}},\
         \"peer_dropped_sends\":{},\"incidents\":{}}}\n",
        state.cfg.host.0,
        counters.fills,
        counters.pages_flushed,
        counters.records_written,
        counters.syncs_acked,
        counters.manifests_published,
        counters.hydrate_fills,
        counters.segs_compacted,
        bill.puts.load(Ordering::SeqCst),
        bill.cas_puts.load(Ordering::SeqCst),
        bill.gets.load(Ordering::SeqCst),
        bill.ranged_gets.load(Ordering::SeqCst),
        bill.deletes.load(Ordering::SeqCst),
        bill.unavailable.load(Ordering::SeqCst),
        bill.bytes_up.load(Ordering::SeqCst),
        bill.bytes_down.load(Ordering::SeqCst),
        state.rt.peer_dropped_sends(),
        state.rt.incidents().len(),
    )
}

fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        if buf.len() >= 64 * 1024 {
            return None;
        }
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => return None,
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let request = text.lines().next()?;
    let mut parts = request.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?;
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let mut query = BTreeMap::new();
    for pair in query_text.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        query.insert(key.to_owned(), value.to_owned());
    }
    let mut headers = BTreeMap::new();
    for line in text.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    Some(Request {
        method,
        path: path.to_owned(),
        query,
        headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_with_ids_have_bounded_metric_labels() {
        assert_eq!(
            route_name("POST", &["vm", "18446744073709551615", "work"]),
            "/vm/{id}/work"
        );
        assert_eq!(route_name("GET", &["unknown", "value"]), "unmatched");
    }

    #[test]
    fn header_extractor_is_case_insensitive() {
        let mut headers = BTreeMap::new();
        headers.insert("traceparent".to_owned(), "00-abc-def-01".to_owned());
        let extractor = HeaderExtractor(&headers);
        assert_eq!(extractor.get("TraceParent"), Some("00-abc-def-01"));
    }
}
