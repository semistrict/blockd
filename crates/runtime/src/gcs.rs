//! Google Cloud Storage as the object store, spoken over the XML API.
//!
//! GCS generations fit the store seam directly: every object
//! carries a native, monotone `generation` (int64), returned in the
//! `x-goog-generation` header on writes and reads, and conditional writes
//! take `x-goog-if-generation-match: 0|N` — exactly the seam's u64 CAS
//! (R6.3), with no client-side version bookkeeping at all. Versions
//! therefore survive process restarts and compare across hosts without
//! client-side version bookkeeping.
//!
//! Fault taxonomy (R8.3/R8.2): anything transient — timeouts, connection
//! errors, 408/429/5xx — maps to `StoreFault::Unavailable` and the
//! daemon's retry timers re-drive it. A missing key is `Ok(None)`, a
//! normal answer. Anything that cannot heal by retrying — IAM denial, a
//! missing bucket, a response without its generation — aborts loudly.
//!
//! Auth is the GCE metadata server (plain HTTP inside the VM): a cached
//! bearer token, refreshed early; the demo VMs run with a service account
//! scoped to the one bucket.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use blockd_core::protocol::{MAX_OBJECT_BYTES, StoreFault};
use bytes::Bytes;
use tokio::sync::Mutex;

use crate::metrics::{AtomicHistogram, LatencySeries};
use crate::store::{GetResult, ObjectStore};

/// Refresh the token while this much of its lifetime remains.
const TOKEN_SLACK: Duration = Duration::from_mins(5);
const MAX_TRANSIENT_RETRIES: u32 = 2;
const RETRY_BASE: Duration = Duration::from_millis(100);
const RETRY_CAP: Duration = Duration::from_secs(2);
const RETRY_AFTER_CAP: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct GcsConfig {
    pub bucket: String,
    /// Prepended to every key (e.g. `"blockd/"`); may be empty.
    pub prefix: String,
    /// `https://storage.googleapis.com` in production; a fake in tests.
    pub endpoint: String,
    /// `http://metadata.google.internal` in production; a fake in tests.
    pub metadata_endpoint: String,
}

/// Request counts and bytes — the bill and the rate-limit conversation.
#[derive(Debug, Default)]
pub struct GcsStats {
    pub gets: AtomicU64,
    pub ranged_gets: AtomicU64,
    pub puts: AtomicU64,
    pub cas_puts: AtomicU64,
    pub precondition_failures: AtomicU64,
    pub deletes: AtomicU64,
    pub unavailable: AtomicU64,
    pub token_refreshes: AtomicU64,
    pub bytes_up: AtomicU64,
    pub bytes_down: AtomicU64,
    latency: [[AtomicHistogram; GCS_OUTCOMES]; GCS_OPERATIONS],
}

const GCS_OPERATIONS: usize = 6;
const GCS_OUTCOMES: usize = 3;
const GCS_OPERATION_NAMES: [&str; GCS_OPERATIONS] = [
    "put",
    "conditional_put",
    "get",
    "ranged_get",
    "delete",
    "token_refresh",
];
const GCS_OUTCOME_NAMES: [&str; GCS_OUTCOMES] = ["success", "unavailable", "conflict"];

impl GcsStats {
    fn observe(&self, operation: usize, outcome: usize, elapsed: Duration) {
        self.latency[operation][outcome].observe(elapsed);
    }

    pub fn latency(&self) -> Vec<LatencySeries> {
        let mut snapshots = Vec::with_capacity(GCS_OPERATIONS * GCS_OUTCOMES);
        for (operation, operation_name) in GCS_OPERATION_NAMES.iter().enumerate() {
            for (outcome, outcome_name) in GCS_OUTCOME_NAMES.iter().enumerate() {
                snapshots.push(LatencySeries {
                    operation: operation_name,
                    outcome: outcome_name,
                    histogram: self.latency[operation][outcome].snapshot(),
                });
            }
        }
        snapshots
    }
}

fn outcome_of<T>(result: &Result<T, StoreFault>) -> usize {
    match result {
        Ok(_) => 0,
        Err(StoreFault::Unavailable) => 1,
        Err(StoreFault::CasConflict { .. }) => 2,
    }
}

struct CachedToken {
    bearer: String,
    expires_at: Instant,
}

pub struct GcsStore {
    cfg: GcsConfig,
    /// Reads and writes intentionally use independent connection pools. A
    /// fault-critical ranged GET must not share an HTTP/2 congestion window
    /// with the large immutable uploads running in the background lane.
    read_client: reqwest::Client,
    write_client: reqwest::Client,
    token: Mutex<Option<CachedToken>>,
    retry_nonce: AtomicU64,
    pub stats: GcsStats,
}

/// Extract `"access_token"` and `"expires_in"` from the metadata server's
/// token response — the one JSON in the system, with a fixed documented
/// shape, hand-parsed per house style (tokens are URL-safe characters;
/// anything escaped is rejected as malformed).
fn parse_token_json(body: &str) -> Option<(String, u64)> {
    fn string_field(body: &str, name: &str) -> Option<String> {
        let at = body.find(&format!("\"{name}\""))?;
        let rest = &body[at + name.len() + 2..];
        let colon = rest.find(':')?;
        let rest = rest[colon + 1..].trim_start();
        let rest = rest.strip_prefix('"')?;
        let end = rest.find('"')?;
        let value = &rest[..end];
        if value.contains('\\') {
            return None;
        }
        Some(value.to_owned())
    }
    fn number_field(body: &str, name: &str) -> Option<u64> {
        let at = body.find(&format!("\"{name}\""))?;
        let rest = &body[at + name.len() + 2..];
        let colon = rest.find(':')?;
        let digits: String = rest[colon + 1..]
            .trim_start()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        digits.parse().ok()
    }
    Some((
        string_field(body, "access_token")?,
        number_field(body, "expires_in")?,
    ))
}

/// Percent-encode a key for the URL path, leaving `/` and unreserved
/// characters. Every `layout.rs` key passes through unchanged; this is
/// defense against a future key shape, not a requirement today.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                use std::fmt::Write;
                write!(out, "%{b:02X}").expect("string write");
            }
        }
    }
    out
}

fn xml_text(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        rest = &rest[start + open.len()..];
        let Some(end) = rest.find(&close) else {
            break;
        };
        values.push(
            rest[..end]
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'"),
        );
        rest = &rest[end + close.len()..];
    }
    values
}

/// One request's distilled outcome.
struct GcsResponse {
    status: u16,
    generation: Option<u64>,
    body: Vec<u8>,
}

fn abort(context: &str, detail: &str) -> ! {
    tracing::error!(gcs_context = context, detail, "fatal GCS error");
    std::process::abort()
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value).ok().map(|deadline| {
                deadline
                    .duration_since(SystemTime::now())
                    .unwrap_or_default()
            })
        })
}

impl GcsStore {
    pub fn new(cfg: GcsConfig) -> GcsStore {
        let make_client = || {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(8)
                .build()
                .expect("GCS HTTP client")
        };
        let retry_seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                elapsed.as_secs() ^ u64::from(elapsed.subsec_nanos()).rotate_left(32)
            })
            ^ u64::from(std::process::id());
        GcsStore {
            cfg,
            read_client: make_client(),
            write_client: make_client(),
            token: Mutex::new(None),
            retry_nonce: AtomicU64::new(retry_seed),
            stats: GcsStats::default(),
        }
    }

    fn object_url(&self, key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.cfg.endpoint,
            self.cfg.bucket,
            encode_key(&format!("{}{key}", self.cfg.prefix))
        )
    }

    /// A bearer token with at least [`TOKEN_SLACK`] of life left.
    async fn token(&self) -> Result<String, StoreFault> {
        // Serialize refreshes so an expiry burst sends one metadata request,
        // not one per concurrent object request. This is an async mutex: no
        // runtime worker is occupied while the metadata request is in flight.
        let mut cached = self.token.lock().await;
        if let Some(token) = cached.as_ref()
            && token.expires_at > Instant::now() + TOKEN_SLACK
        {
            return Ok(token.bearer.clone());
        }
        let started = Instant::now();
        let result = self.refresh_token_inner().await;
        self.stats
            .observe(5, outcome_of(&result), started.elapsed());
        if let Ok((bearer, expires_in)) = result {
            *cached = Some(CachedToken {
                bearer: bearer.clone(),
                expires_at: Instant::now() + Duration::from_secs(expires_in),
            });
            Ok(bearer)
        } else {
            Err(StoreFault::Unavailable)
        }
    }

    async fn refresh_token_inner(&self) -> Result<(String, u64), StoreFault> {
        let url = format!(
            "{}/computeMetadata/v1/instance/service-accounts/default/token",
            self.cfg.metadata_endpoint
        );
        self.stats.token_refreshes.fetch_add(1, Ordering::SeqCst);
        let result = self
            .write_client
            .get(&url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await;
        let Ok(resp) = result else {
            return Err(StoreFault::Unavailable);
        };
        if resp.status() != reqwest::StatusCode::OK {
            return Err(StoreFault::Unavailable);
        }
        let Ok(body) = resp.text().await else {
            return Err(StoreFault::Unavailable);
        };
        let Some((bearer, expires_in)) = parse_token_json(&body) else {
            abort("metadata token", "unparseable token response");
        };
        Ok((bearer, expires_in))
    }

    /// One authorized request with the single 401-refresh-retry. Transport
    /// errors and transient statuses become `Unavailable`; everything else
    /// is returned for the caller's status mapping.
    async fn request(
        &self,
        method: &str,
        key: &str,
        headers: &[(&str, String)],
        body: Option<&Bytes>,
    ) -> Result<GcsResponse, StoreFault> {
        let url = self.object_url(key);
        self.request_url(method, url, headers, body).await
    }

    async fn request_url(
        &self,
        method: &str,
        url: String,
        headers: &[(&str, String)],
        body: Option<&Bytes>,
    ) -> Result<GcsResponse, StoreFault> {
        let mut refreshed = false;
        let mut transient_retries = 0;
        loop {
            let bearer = self.token().await?;
            let client = if matches!(method, "GET" | "HEAD") {
                &self.read_client
            } else {
                &self.write_client
            };
            let method = reqwest::Method::from_bytes(method.as_bytes())
                .expect("supported object-store method");
            let mut request = client.request(method, &url).bearer_auth(&bearer);
            for (name, value) in headers {
                request = request.header(*name, value);
            }
            if let Some(bytes) = body {
                request = request.body(bytes.clone());
            }
            let Ok(mut resp) = request.send().await else {
                if transient_retries < MAX_TRANSIENT_RETRIES {
                    let delay = self.retry_delay(transient_retries, None);
                    transient_retries += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                self.stats.unavailable.fetch_add(1, Ordering::SeqCst);
                return Err(StoreFault::Unavailable);
            };
            let status = resp.status().as_u16();
            if status == 401 && !refreshed {
                refreshed = true;
                self.token.lock().await.take();
                continue;
            }
            if matches!(status, 408 | 429 | 500..=599) {
                if transient_retries < MAX_TRANSIENT_RETRIES {
                    let retry_after = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|value| value.to_str().ok())
                        .and_then(parse_retry_after);
                    let delay = self.retry_delay(transient_retries, retry_after);
                    transient_retries += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                self.stats.unavailable.fetch_add(1, Ordering::SeqCst);
                return Err(StoreFault::Unavailable);
            }
            let generation = resp
                .headers()
                .get("x-goog-generation")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            if resp
                .content_length()
                .is_some_and(|length| length > u64::from(MAX_OBJECT_BYTES))
            {
                abort("response body", "object exceeds maximum size");
            }
            let mut body = Vec::new();
            loop {
                match resp.chunk().await {
                    Ok(Some(chunk)) => {
                        let next = body.len().saturating_add(chunk.len());
                        if u64::try_from(next).unwrap_or(u64::MAX) > u64::from(MAX_OBJECT_BYTES) {
                            abort("response body", "object exceeds maximum size");
                        }
                        body.extend_from_slice(&chunk);
                    }
                    Ok(None) => break,
                    Err(_) => {
                        self.stats.unavailable.fetch_add(1, Ordering::SeqCst);
                        return Err(StoreFault::Unavailable);
                    }
                }
            }
            return Ok(GcsResponse {
                status,
                generation,
                body,
            });
        }
    }

    fn retry_delay(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        if let Some(delay) = retry_after {
            return delay.min(RETRY_AFTER_CAP);
        }
        let exponent = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
        let cap = RETRY_BASE.saturating_mul(exponent).min(RETRY_CAP);
        let mut value = self.retry_nonce.fetch_add(1, Ordering::Relaxed);
        // SplitMix64: cheap per-request diffusion so simultaneous failures do
        // not wake every host on the same retry boundary.
        value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        let cap_nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(value % cap_nanos.max(1))
    }

    fn generation_or_abort(context: &str, resp: &GcsResponse) -> u64 {
        let Some(generation) = resp.generation else {
            abort(context, "response without x-goog-generation");
        };
        generation
    }

    /// The current generation of a key (`None` = absent) — what fills in
    /// `CasConflict::actual` after a 412.
    async fn head_generation(&self, key: &str) -> Result<Option<u64>, StoreFault> {
        let resp = self.request("HEAD", key, &[], None).await?;
        match resp.status {
            200 => Ok(Some(Self::generation_or_abort("HEAD", &resp))),
            404 => Ok(None),
            status => abort("HEAD", &format!("status {status}")),
        }
    }
}

#[async_trait]
impl ObjectStore for GcsStore {
    async fn put(
        self: std::sync::Arc<Self>,
        key: String,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreFault> {
        GcsStore::put(&self, &key, bytes).await
    }

    async fn put_cas(
        self: std::sync::Arc<Self>,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreFault> {
        GcsStore::put_cas(&self, &key, expected, bytes).await
    }

    async fn get(self: std::sync::Arc<Self>, key: String) -> GetResult {
        GcsStore::get(&self, &key).await
    }

    async fn get_range(
        self: std::sync::Arc<Self>,
        key: String,
        offset: u64,
        len: u64,
    ) -> GetResult {
        GcsStore::get_range(&self, &key, offset, len).await
    }

    async fn delete(self: std::sync::Arc<Self>, key: String) {
        GcsStore::delete(&self, &key).await;
    }

    async fn list_prefix(
        self: std::sync::Arc<Self>,
        prefix: String,
    ) -> Result<Vec<String>, StoreFault> {
        GcsStore::list_prefix(&self, &prefix).await
    }
}

impl GcsStore {
    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<u64, StoreFault> {
        let started = Instant::now();
        let result = self.put_inner(key, Bytes::from(bytes)).await;
        self.stats
            .observe(0, outcome_of(&result), started.elapsed());
        result
    }

    pub async fn put_cas(
        &self,
        key: &str,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreFault> {
        let started = Instant::now();
        let result = self.put_cas_inner(key, expected, Bytes::from(bytes)).await;
        self.stats
            .observe(1, outcome_of(&result), started.elapsed());
        result
    }

    pub async fn get(&self, key: &str) -> GetResult {
        let started = Instant::now();
        let result = self.get_inner(key).await;
        self.stats
            .observe(2, outcome_of(&result), started.elapsed());
        result
    }

    pub async fn get_range(&self, key: &str, offset: u64, len: u64) -> GetResult {
        let started = Instant::now();
        let result = self.get_range_inner(key, offset, len).await;
        self.stats
            .observe(3, outcome_of(&result), started.elapsed());
        result
    }

    pub async fn delete(&self, key: &str) {
        let started = Instant::now();
        self.stats.deletes.fetch_add(1, Ordering::SeqCst);
        // Fire-and-forget (R4.5): a failed delete leaks one superseded
        // object; the daemon never re-drives deletes, so neither do we.
        let result = self.request("DELETE", key, &[], None).await.map(|_| ());
        self.stats
            .observe(4, outcome_of(&result), started.elapsed());
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreFault> {
        let full_prefix = format!("{}{prefix}", self.cfg.prefix);
        let mut continuation = None::<String>;
        let mut keys = Vec::new();
        loop {
            let mut url = format!(
                "{}/{}?list-type=2&max-keys=1000&prefix={}",
                self.cfg.endpoint,
                self.cfg.bucket,
                encode_key(&full_prefix)
            );
            if let Some(token) = continuation.as_deref() {
                url.push_str("&continuation-token=");
                url.push_str(&encode_key(token));
            }
            let response = self.request_url("GET", url, &[], None).await?;
            if response.status != 200 {
                return Err(StoreFault::Unavailable);
            }
            let body = String::from_utf8(response.body).map_err(|_| StoreFault::Unavailable)?;
            for key in xml_text(&body, "Key") {
                let Some(key) = key.strip_prefix(&self.cfg.prefix) else {
                    return Err(StoreFault::Unavailable);
                };
                if key.starts_with(prefix) {
                    keys.push(key.to_owned());
                }
            }
            let truncated = xml_text(&body, "IsTruncated")
                .first()
                .is_some_and(|value| value == "true");
            if !truncated {
                break;
            }
            continuation = xml_text(&body, "NextContinuationToken").into_iter().next();
            if continuation.is_none() {
                return Err(StoreFault::Unavailable);
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    async fn put_inner(&self, key: &str, bytes: Bytes) -> Result<u64, StoreFault> {
        self.stats.puts.fetch_add(1, Ordering::SeqCst);
        self.stats
            .bytes_up
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        let resp = self.request("PUT", key, &[], Some(&bytes)).await?;
        match resp.status {
            200 => Ok(Self::generation_or_abort("PUT", &resp)),
            status => abort(
                "PUT",
                &format!(
                    "status {status} for {key}: {}",
                    String::from_utf8_lossy(&resp.body)
                ),
            ),
        }
    }

    async fn put_cas_inner(
        &self,
        key: &str,
        expected: Option<u64>,
        bytes: Bytes,
    ) -> Result<u64, StoreFault> {
        self.stats.cas_puts.fetch_add(1, Ordering::SeqCst);
        self.stats
            .bytes_up
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        let precondition = expected.unwrap_or(0).to_string();
        let headers = [("x-goog-if-generation-match", precondition)];
        let resp = self.request("PUT", key, &headers, Some(&bytes)).await?;
        match resp.status {
            200 => Ok(Self::generation_or_abort("CAS PUT", &resp)),
            // The condition failed: someone else holds the key. Any
            // current generation observed after our loss is a truthful
            // `actual` (the same race the in-process store has).
            412 => {
                self.stats
                    .precondition_failures
                    .fetch_add(1, Ordering::SeqCst);
                Err(StoreFault::CasConflict {
                    actual: self.head_generation(key).await?,
                })
            }
            status => abort(
                "CAS PUT",
                &format!(
                    "status {status} for {key}: {}",
                    String::from_utf8_lossy(&resp.body)
                ),
            ),
        }
    }

    async fn get_inner(&self, key: &str) -> GetResult {
        self.stats.gets.fetch_add(1, Ordering::SeqCst);
        let resp = self.request("GET", key, &[], None).await?;
        match resp.status {
            200 => {
                self.stats
                    .bytes_down
                    .fetch_add(resp.body.len() as u64, Ordering::SeqCst);
                let generation = Self::generation_or_abort("GET", &resp);
                Ok(Some((generation, resp.body)))
            }
            404 => Ok(None),
            status => abort("GET", &format!("status {status} for {key}")),
        }
    }

    async fn get_range_inner(&self, key: &str, offset: u64, len: u64) -> GetResult {
        assert!(len > 0, "zero-length range read");
        self.stats.ranged_gets.fetch_add(1, Ordering::SeqCst);
        let range = format!("bytes={offset}-{}", offset + len - 1);
        let headers = [("Range", range)];
        let resp = self.request("GET", key, &headers, None).await?;
        match resp.status {
            // 206 is the ranged answer; 200 with offset 0 is a server
            // electing to return the whole (small) object — truncate.
            206 => {
                self.stats
                    .bytes_down
                    .fetch_add(resp.body.len() as u64, Ordering::SeqCst);
                let generation = Self::generation_or_abort("ranged GET", &resp);
                Ok(Some((generation, resp.body)))
            }
            200 if offset == 0 => {
                let generation = Self::generation_or_abort("ranged GET", &resp);
                let mut body = resp.body;
                body.truncate(usize::try_from(len).expect("len fits"));
                self.stats
                    .bytes_down
                    .fetch_add(body.len() as u64, Ordering::SeqCst);
                Ok(Some((generation, body)))
            }
            // Range starts past the end — the expected miss signal.
            404 | 416 => Ok(None),
            status => abort("ranged GET", &format!("status {status} for {key}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_json_parses_the_documented_shape() {
        let body = r#"{"access_token":"ya29.A0AfB_x","expires_in":3599,"token_type":"Bearer"}"#;
        assert_eq!(
            parse_token_json(body),
            Some(("ya29.A0AfB_x".to_owned(), 3599))
        );
        // Field order does not matter.
        let reordered = r#"{ "token_type": "Bearer", "expires_in": 100, "access_token": "t" }"#;
        assert_eq!(parse_token_json(reordered), Some(("t".to_owned(), 100)));
        // Escapes cannot appear in a real token: reject as malformed.
        assert_eq!(
            parse_token_json(r#"{"access_token":"a\"b","expires_in":10}"#),
            None
        );
        assert_eq!(parse_token_json("not json"), None);
        assert_eq!(parse_token_json(r#"{"access_token":"t"}"#), None);
    }

    #[test]
    fn layout_keys_pass_encoding_unchanged() {
        for key in [
            "v/000000000badcafe/head",
            "v/000000000badcafe/m/0000000000000002-000000000000001f.manifest",
            "v/000000000badcafe/f/0000000000000002-0000000000000007.files",
            "v/000000000badcafe/o/0000000000000002-0000000000000003.blx",
            "v/000000000badcafe/p/0000000000000002-000000000000001f",
            "b/000000000badcafe/root",
            "b/000000000badcafe/m/0000000000000007.manifest",
            "cluster/tls/public-keys/0002.member",
        ] {
            assert_eq!(encode_key(key), key);
        }
        assert_eq!(encode_key("a key%"), "a%20key%25");
    }

    #[test]
    fn client_configuration_builds() {
        let _store = GcsStore::new(GcsConfig {
            bucket: "bucket".to_owned(),
            prefix: String::new(),
            endpoint: "http://127.0.0.1".to_owned(),
            metadata_endpoint: "http://127.0.0.1".to_owned(),
        });
    }

    #[test]
    fn retry_after_is_honored_and_capped() {
        let store = GcsStore::new(GcsConfig {
            bucket: "bucket".to_owned(),
            prefix: String::new(),
            endpoint: "https://storage.googleapis.com".to_owned(),
            metadata_endpoint: "http://metadata.google.internal".to_owned(),
        });
        assert_eq!(
            store.retry_delay(0, Some(Duration::from_secs(7))),
            Duration::from_secs(7)
        );
        assert_eq!(
            store.retry_delay(0, Some(Duration::from_mins(1))),
            RETRY_AFTER_CAP
        );
        assert!(store.retry_delay(0, None) < RETRY_BASE);
        assert!(store.retry_delay(1, None) < RETRY_BASE * 2);
        assert_eq!(parse_retry_after("7"), Some(Duration::from_secs(7)));
        assert_eq!(
            parse_retry_after("Thu, 01 Jan 1970 00:00:00 GMT"),
            Some(Duration::ZERO)
        );
    }
}
