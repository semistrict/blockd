//! An in-process GCS lookalike for tests and local demos: serves metadata
//! tokens and emulates object PUT / GET / HEAD / DELETE with generation
//! preconditions over real HTTP — enough surface for [`crate::GcsStore`]
//! to run unmodified against it. Never a production store: no
//! durability, no auth, one process's memory.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

/// One parsed request, recorded for assertions.
#[derive(Clone, Debug)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
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
    /// Test-only availability controls. Data outage preserves `/head` so
    /// placement/fencing can continue while immutable uploads are stalled.
    pub outage: std::sync::atomic::AtomicBool,
    pub data_outage: std::sync::atomic::AtomicBool,
}

impl FakeGcs {
    /// Bind an ephemeral port and serve forever; returns the endpoint URL
    /// (usable as both `endpoint` and `metadata_endpoint`).
    pub fn start() -> (Arc<FakeGcs>, String) {
        FakeGcs::start_on("127.0.0.1:0".parse().expect("addr"))
    }

    /// Serve on a specific address (the demo's shared local store).
    pub fn start_on(addr: SocketAddr) -> (Arc<FakeGcs>, String) {
        let listener = TcpListener::bind(addr).expect("bind fake gcs");
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
            outage: std::sync::atomic::AtomicBool::new(false),
            data_outage: std::sync::atomic::AtomicBool::new(false),
        });
        let server = fake.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let server = server.clone();
                thread::spawn(move || server.serve(stream));
            }
        });
        (fake, format!("http://{addr}"))
    }

    fn serve(&self, mut stream: TcpStream) {
        loop {
            let Some((seen, body)) = read_request(&mut stream) else {
                return;
            };
            if seen.path.starts_with("/computeMetadata/") {
                self.tokens_served.fetch_add(1, Ordering::SeqCst);
                let n = self.tokens_served.load(Ordering::SeqCst);
                let body = format!(
                    "{{\"access_token\":\"token-{n}\",\"expires_in\":{},\"token_type\":\"Bearer\"}}",
                    self.token_expires_in.load(Ordering::SeqCst)
                );
                respond(&mut stream, 200, &[], body.as_bytes());
                continue;
            }
            if seen.path == "/__control/outage/on" {
                self.outage.store(true, Ordering::SeqCst);
                respond(&mut stream, 200, &[], b"ok");
                continue;
            }
            if seen.path == "/__control/outage/off" {
                self.outage.store(false, Ordering::SeqCst);
                respond(&mut stream, 200, &[], b"ok");
                continue;
            }
            if seen.path == "/__control/data-outage/on" {
                self.data_outage.store(true, Ordering::SeqCst);
                respond(&mut stream, 200, &[], b"ok");
                continue;
            }
            if seen.path == "/__control/data-outage/off" {
                self.data_outage.store(false, Ordering::SeqCst);
                respond(&mut stream, 200, &[], b"ok");
                continue;
            }
            self.seen.lock().expect("lock").push(seen.clone());
            let latency = self.latency_ms.load(Ordering::SeqCst);
            if latency > 0 {
                thread::sleep(std::time::Duration::from_millis(latency));
            }
            if self.outage.load(Ordering::SeqCst)
                || (self.data_outage.load(Ordering::SeqCst) && !seen.path.ends_with("/head"))
            {
                respond_to(&seen.method, &mut stream, 503, &[], b"unavailable");
                continue;
            }
            if let Some(fault) = {
                let mut faults = self.faults.lock().expect("lock");
                if faults.is_empty() {
                    None
                } else {
                    Some(faults.remove(0))
                }
            } {
                match fault {
                    Fault::Status(status) => {
                        respond_to(&seen.method, &mut stream, status, &[], b"scripted");
                        continue;
                    }
                    Fault::DropConnection => return,
                }
            }
            self.object_request(&mut stream, &seen, body);
        }
    }

    fn object_request(&self, stream: &mut TcpStream, seen: &Seen, body: Vec<u8>) {
        let key = seen.path.trim_start_matches('/').to_owned();
        let mut objects = self.objects.lock().expect("lock");
        match seen.method.as_str() {
            "PUT" => {
                let current = objects.get(&key).map(|&(generation, _)| generation);
                if let Some(want) = seen.headers.get("x-goog-if-generation-match") {
                    let want: u64 = want.parse().expect("precondition is a number");
                    let held = current.unwrap_or(0);
                    if want != held {
                        respond(stream, 412, &[], b"precondition failed");
                        return;
                    }
                }
                let generation = self.next_gen.fetch_add(1, Ordering::SeqCst);
                objects.insert(key, (generation, body));
                let generation = generation.to_string();
                respond(
                    stream,
                    200,
                    &[("x-goog-generation", generation.as_str())],
                    b"",
                );
            }
            "GET" | "HEAD" => {
                let Some((generation, bytes)) = objects.get(&key) else {
                    respond_to(&seen.method, stream, 404, &[], b"NoSuchKey");
                    return;
                };
                let generation = generation.to_string();
                let headers = [("x-goog-generation", generation.as_str())];
                if seen.method == "HEAD" {
                    respond(stream, 200, &headers, b"");
                    return;
                }
                if let Some(range) = seen.headers.get("range") {
                    let spec = range.strip_prefix("bytes=").expect("range shape");
                    let (first, last) = spec.split_once('-').expect("range shape");
                    let first: usize = first.parse().expect("number");
                    let last: usize = last.parse().expect("number");
                    if first >= bytes.len() {
                        respond(stream, 416, &[], b"InvalidRange");
                        return;
                    }
                    let end = (last + 1).min(bytes.len());
                    respond(stream, 206, &headers, &bytes[first..end]);
                    return;
                }
                respond(stream, 200, &headers, bytes);
            }
            "DELETE" => {
                objects.remove(&key);
                respond(stream, 204, &[], b"");
            }
            other => panic!("unexpected method {other}"),
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Option<(Seen, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => return None,
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let mut lines = text.lines();
    let request = lines.next()?;
    let mut parts = request.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();
    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    let length: usize = headers
        .get("content-length")
        .map_or(0, |v| v.parse().expect("length"));
    let mut body = vec![0u8; length];
    if length > 0 {
        stream.read_exact(&mut body).ok()?;
    }
    Some((
        Seen {
            method,
            path,
            headers,
        },
        body,
    ))
}

fn respond(stream: &mut TcpStream, status: u16, headers: &[(&str, &str)], body: &[u8]) {
    let mut resp = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\n", body.len());
    for (name, value) in headers {
        write!(resp, "{name}: {value}\r\n").expect("string write");
    }
    resp.push_str("\r\n");
    let _ = stream.write_all(resp.as_bytes());
    let _ = stream.write_all(body);
}

/// HEAD responses must carry no body bytes or the keep-alive stream
/// desyncs; everything else answers normally.
fn respond_to(
    method: &str,
    stream: &mut TcpStream,
    status: u16,
    headers: &[(&str, &str)],
    body: &[u8],
) {
    if method == "HEAD" {
        respond(stream, status, headers, b"");
    } else {
        respond(stream, status, headers, body);
    }
}
