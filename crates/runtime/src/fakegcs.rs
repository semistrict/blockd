//! An in-process GCS lookalike for tests and local demos: serves metadata
//! tokens and emulates object PUT / GET / HEAD / DELETE with generation
//! preconditions over real HTTP — enough surface for [`crate::GcsStore`]
//! to run unmodified against it. Never a production store: no
//! durability, no auth, one process's memory.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::Response;
use blockd_core::protocol::MAX_OBJECT_BYTES;
use futures_util::stream;

/// One parsed request, recorded for assertions.
#[derive(Clone, Debug)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub peer: SocketAddr,
}

/// A scripted deviation from stateful behavior, applied to the next
/// object request (token requests are never scripted).
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    Status(u16),
    DropConnection,
}

pub struct FakeGcs {
    objects: Mutex<BTreeMap<String, (u64, Vec<u8>)>>,
    next_gen: AtomicU64,
    pub tokens_served: AtomicU64,
    pub token_expires_in: AtomicU64,
    pub seen: Mutex<Vec<Seen>>,
    pub faults: Mutex<Vec<Fault>>,
    /// Added to every object request (not token requests): emulates a
    /// real store's round-trip so cadence bugs reproduce locally.
    pub latency_ms: AtomicU64,
    in_flight: AtomicU64,
    pub max_in_flight: AtomicU64,
    /// Test-only availability controls. Data outage preserves `/head` so
    /// placement/fencing can continue while immutable uploads are stalled.
    pub outage: std::sync::atomic::AtomicBool,
    pub data_outage: std::sync::atomic::AtomicBool,
}

/// Owned lifetime of one fake object-store HTTP service.
pub struct FakeGcsServer {
    state: Arc<FakeGcs>,
    task: tokio::task::JoinHandle<()>,
}

impl Deref for FakeGcsServer {
    type Target = FakeGcs;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl Drop for FakeGcsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeGcs {
    pub fn request_count(&self) -> usize {
        self.seen
            .lock()
            .expect("fake GCS seen mutex poisoned")
            .len()
    }

    pub fn method_count(&self, method: &str) -> usize {
        self.seen
            .lock()
            .expect("fake GCS seen mutex poisoned")
            .iter()
            .filter(|request| request.method == method)
            .count()
    }

    /// Bind an ephemeral port on the current Tokio runtime.
    pub async fn start() -> (FakeGcsServer, String) {
        FakeGcs::start_on("127.0.0.1:0".parse().expect("addr")).await
    }

    /// Serve on a specific address (the demo's shared local store).
    pub async fn start_on(addr: SocketAddr) -> (FakeGcsServer, String) {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind fake gcs");
        let addr = listener.local_addr().expect("addr");
        let fake = Arc::new(FakeGcs {
            objects: Mutex::new(BTreeMap::new()),
            // GCS generations look like microsecond timestamps: large.
            next_gen: AtomicU64::new(1_700_000_000_000_001),
            tokens_served: AtomicU64::new(0),
            token_expires_in: AtomicU64::new(3599),
            seen: Mutex::new(Vec::new()),
            faults: Mutex::new(Vec::new()),
            latency_ms: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
            outage: std::sync::atomic::AtomicBool::new(false),
            data_outage: std::sync::atomic::AtomicBool::new(false),
        });
        let state = Arc::clone(&fake);
        let app = Router::new()
            .fallback(handle)
            .layer(DefaultBodyLimit::max(
                usize::try_from(MAX_OBJECT_BYTES).expect("object cap fits usize"),
            ))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .expect("serve fake GCS");
        });
        (
            FakeGcsServer { state: fake, task },
            format!("http://{addr}"),
        )
    }

    fn object_response(&self, seen: &Seen, body: Vec<u8>) -> Response {
        let key = seen.path.trim_start_matches('/').to_owned();
        let mut objects = self.objects.lock().expect("lock");
        match seen.method.as_str() {
            "PUT" => {
                let current = objects.get(&key).map(|&(generation, _)| generation);
                if let Some(want) = seen.headers.get("x-goog-if-generation-match") {
                    let want: u64 = want.parse().expect("precondition is a number");
                    let held = current.unwrap_or(0);
                    if want != held {
                        return response(412, &[], b"precondition failed".to_vec());
                    }
                }
                let generation = self.next_gen.fetch_add(1, Ordering::SeqCst);
                objects.insert(key, (generation, body));
                response(
                    200,
                    &[("x-goog-generation", generation.to_string())],
                    Vec::new(),
                )
            }
            "GET" | "HEAD" => {
                let Some((generation, bytes)) = objects.get(&key) else {
                    return response_for(&seen.method, 404, &[], b"NoSuchKey".to_vec());
                };
                let headers = [("x-goog-generation", generation.to_string())];
                if seen.method == "HEAD" {
                    return response(200, &headers, Vec::new());
                }
                if let Some(range) = seen.headers.get("range") {
                    let spec = range.strip_prefix("bytes=").expect("range shape");
                    let (first, last) = spec.split_once('-').expect("range shape");
                    let first: usize = first.parse().expect("number");
                    let last: usize = last.parse().expect("number");
                    if first >= bytes.len() {
                        return response(416, &[], b"InvalidRange".to_vec());
                    }
                    let end = (last + 1).min(bytes.len());
                    return response(206, &headers, bytes[first..end].to_vec());
                }
                response(200, &headers, bytes.clone())
            }
            "DELETE" => {
                objects.remove(&key);
                response(204, &[], Vec::new())
            }
            _ => response(405, &[], b"method not allowed".to_vec()),
        }
    }

    fn list_response(&self, bucket_path: &str, query: &str) -> Response {
        let prefix = query
            .split('&')
            .find_map(|part| part.strip_prefix("prefix="))
            .map(percent_decode)
            .unwrap_or_default();
        let token = query
            .split('&')
            .find_map(|part| part.strip_prefix("continuation-token="))
            .map(percent_decode);
        let root = format!("{}/", bucket_path.trim_start_matches('/'));
        let mut names = self
            .objects
            .lock()
            .expect("lock")
            .keys()
            .filter_map(|key| key.strip_prefix(&root))
            .filter(|key| key.starts_with(&prefix))
            .filter(|key| token.as_deref().is_none_or(|token| *key > token))
            .take(1_001)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let truncated = names.len() > 1_000;
        names.truncate(1_000);
        let next = truncated
            .then(|| names.last().cloned())
            .flatten()
            .map(|key| {
                format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    xml_escape(&key)
                )
            })
            .unwrap_or_default();
        let contents = names.iter().fold(String::new(), |mut contents, key| {
            write!(
                contents,
                "<Contents><Key>{}</Key></Contents>",
                xml_escape(key)
            )
            .expect("writing XML into a string cannot fail");
            contents
        });
        response(
            200,
            &[],
            format!(
                "<ListBucketResult><IsTruncated>{truncated}</IsTruncated>{next}{contents}</ListBucketResult>"
            )
            .into_bytes(),
        )
    }
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Ok(byte) = u8::from_str_radix(&value[index + 1..index + 3], 16)
        {
            decoded.push(byte);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

async fn handle(
    State(server): State<Arc<FakeGcs>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if uri.path().starts_with("/computeMetadata/") {
        let n = server.tokens_served.fetch_add(1, Ordering::SeqCst) + 1;
        let body = format!(
            "{{\"access_token\":\"token-{n}\",\"expires_in\":{},\"token_type\":\"Bearer\"}}",
            server.token_expires_in.load(Ordering::SeqCst)
        );
        return response(200, &[], body.into_bytes());
    }
    match uri.path() {
        "/__control/outage/on" => server.outage.store(true, Ordering::SeqCst),
        "/__control/outage/off" => server.outage.store(false, Ordering::SeqCst),
        "/__control/data-outage/on" => server.data_outage.store(true, Ordering::SeqCst),
        "/__control/data-outage/off" => server.data_outage.store(false, Ordering::SeqCst),
        _ => {}
    }
    if uri.path().starts_with("/__control/") {
        return response(200, &[], b"ok".to_vec());
    }
    let seen = Seen {
        method: method.to_string(),
        path: uri.path().to_owned(),
        peer,
        headers: headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect(),
    };
    server.seen.lock().expect("lock").push(seen.clone());
    let current = server.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    server.max_in_flight.fetch_max(current, Ordering::SeqCst);
    let _in_flight = InFlight(&server.in_flight);
    let latency = server.latency_ms.load(Ordering::SeqCst);
    if latency > 0 {
        tokio::time::sleep(Duration::from_millis(latency)).await;
    }
    if server.outage.load(Ordering::SeqCst)
        || (server.data_outage.load(Ordering::SeqCst) && !seen.path.ends_with("/head"))
    {
        return response_for(&seen.method, 503, &[], b"unavailable".to_vec());
    }
    let fault = {
        let mut faults = server.faults.lock().expect("lock");
        if faults.is_empty() {
            None
        } else {
            Some(faults.remove(0))
        }
    };
    match fault {
        Some(Fault::Status(status)) => {
            response_for(&seen.method, status, &[], b"scripted".to_vec())
        }
        Some(Fault::DropConnection) => dropped_connection_response(),
        None if method == Method::GET
            && uri
                .query()
                .is_some_and(|query| query.contains("list-type=2")) =>
        {
            server.list_response(uri.path(), uri.query().unwrap_or_default())
        }
        None => server.object_response(&seen, body.to_vec()),
    }
}

struct InFlight<'a>(&'a AtomicU64);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn response(status: u16, headers: &[(&str, String)], body: Vec<u8>) -> Response {
    let mut builder = Response::builder().status(StatusCode::from_u16(status).expect("status"));
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    builder.body(Body::from(body)).expect("response")
}

fn response_for(method: &str, status: u16, headers: &[(&str, String)], body: Vec<u8>) -> Response {
    response(
        status,
        headers,
        if method == "HEAD" { Vec::new() } else { body },
    )
}

/// Start a successful response and fail its body. Hyper terminates the HTTP/1
/// message mid-stream, exercising the client's transport error path rather
/// than its status mapping.
fn dropped_connection_response() -> Response {
    let body = Body::from_stream(stream::once(async {
        Err::<Bytes, std::io::Error>(std::io::Error::other("scripted connection drop"))
    }));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONNECTION, "close")
        .body(body)
        .expect("response")
}
