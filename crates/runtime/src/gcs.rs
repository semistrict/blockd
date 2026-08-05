//! Google Cloud Storage as the object store, spoken over the XML API.
//!
//! GCS is a better fit for the store seam than S3's shape: every object
//! carries a native, monotone `generation` (int64), returned in the
//! `x-goog-generation` header on writes and reads, and conditional writes
//! take `x-goog-if-generation-match: 0|N` — exactly the seam's u64 CAS
//! (R6.3), with no client-side version bookkeeping at all. Versions
//! therefore survive process restarts and compare across hosts, which the
//! in-process `S3Store` registry cannot do.
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

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use blockd_core::seam::StoreFault;

use crate::store::{GetResult, ObjectStore};

/// R4.6: no object exceeds 64 MiB; anything larger is a protocol
/// violation, not data.
const MAX_OBJECT: u64 = 64 * 1024 * 1024 + 4096;

/// Refresh the token while this much of its lifetime remains.
const TOKEN_SLACK: Duration = Duration::from_mins(5);

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
}

struct CachedToken {
    bearer: String,
    expires_at: Instant,
}

pub struct GcsStore {
    cfg: GcsConfig,
    agent: ureq::Agent,
    token: Mutex<Option<CachedToken>>,
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

/// One request's distilled outcome.
struct GcsResponse {
    status: u16,
    generation: Option<u64>,
    body: Vec<u8>,
}

fn abort(context: &str, detail: &str) -> ! {
    eprintln!("FATAL: GCS {context}: {detail}");
    std::process::abort()
}

impl GcsStore {
    pub fn new(cfg: GcsConfig) -> GcsStore {
        let config = ureq::config::Config::builder()
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(2)))
            .timeout_global(Some(Duration::from_secs(30)))
            .build();
        GcsStore {
            cfg,
            agent: config.new_agent(),
            token: Mutex::new(None),
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
    fn token(&self) -> Result<String, StoreFault> {
        {
            let cached = self.token.lock().expect("lock");
            if let Some(t) = cached.as_ref()
                && t.expires_at > Instant::now() + TOKEN_SLACK
            {
                return Ok(t.bearer.clone());
            }
        }
        self.refresh_token()
    }

    fn refresh_token(&self) -> Result<String, StoreFault> {
        let url = format!(
            "{}/computeMetadata/v1/instance/service-accounts/default/token",
            self.cfg.metadata_endpoint
        );
        self.stats.token_refreshes.fetch_add(1, Ordering::SeqCst);
        let result = self
            .agent
            .get(&url)
            .header("Metadata-Flavor", "Google")
            .call();
        let Ok(mut resp) = result else {
            return Err(StoreFault::Unavailable);
        };
        if resp.status().as_u16() != 200 {
            return Err(StoreFault::Unavailable);
        }
        let Ok(body) = resp.body_mut().read_to_string() else {
            return Err(StoreFault::Unavailable);
        };
        let Some((bearer, expires_in)) = parse_token_json(&body) else {
            abort("metadata token", "unparseable token response");
        };
        let mut cached = self.token.lock().expect("lock");
        *cached = Some(CachedToken {
            bearer: bearer.clone(),
            expires_at: Instant::now() + Duration::from_secs(expires_in),
        });
        Ok(bearer)
    }

    /// One authorized request with the single 401-refresh-retry. Transport
    /// errors and transient statuses become `Unavailable`; everything else
    /// is returned for the caller's status mapping.
    fn request(
        &self,
        method: &str,
        key: &str,
        headers: &[(&str, String)],
        body: Option<&[u8]>,
    ) -> Result<GcsResponse, StoreFault> {
        let url = self.object_url(key);
        let mut refreshed = false;
        loop {
            let bearer = self.token()?;
            let auth = format!("Bearer {bearer}");
            let result = if let Some(bytes) = body {
                assert_eq!(method, "PUT", "only puts carry a body");
                let mut req = self.agent.put(&url).header("Authorization", &auth);
                for (name, value) in headers {
                    req = req.header(*name, value);
                }
                req.send(bytes)
            } else {
                let mut req = match method {
                    "GET" => self.agent.get(&url),
                    "HEAD" => self.agent.head(&url),
                    "DELETE" => self.agent.delete(&url),
                    other => unreachable!("unsupported method {other}"),
                }
                .header("Authorization", &auth);
                for (name, value) in headers {
                    req = req.header(*name, value);
                }
                req.call()
            };
            let Ok(mut resp) = result else {
                self.stats.unavailable.fetch_add(1, Ordering::SeqCst);
                return Err(StoreFault::Unavailable);
            };
            let status = resp.status().as_u16();
            if status == 401 && !refreshed {
                refreshed = true;
                self.token.lock().expect("lock").take();
                continue;
            }
            if matches!(status, 408 | 429 | 500..=599) {
                self.stats.unavailable.fetch_add(1, Ordering::SeqCst);
                return Err(StoreFault::Unavailable);
            }
            let generation = resp
                .headers()
                .get("x-goog-generation")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            let Ok(body) = resp
                .body_mut()
                .with_config()
                .limit(MAX_OBJECT)
                .read_to_vec()
            else {
                self.stats.unavailable.fetch_add(1, Ordering::SeqCst);
                return Err(StoreFault::Unavailable);
            };
            return Ok(GcsResponse {
                status,
                generation,
                body,
            });
        }
    }

    fn generation_or_abort(context: &str, resp: &GcsResponse) -> u64 {
        let Some(generation) = resp.generation else {
            abort(context, "response without x-goog-generation");
        };
        generation
    }

    /// The current generation of a key (`None` = absent) — what fills in
    /// `CasConflict::actual` after a 412.
    fn head_generation(&self, key: &str) -> Result<Option<u64>, StoreFault> {
        let resp = self.request("HEAD", key, &[], None)?;
        match resp.status {
            200 => Ok(Some(Self::generation_or_abort("HEAD", &resp))),
            404 => Ok(None),
            status => abort("HEAD", &format!("status {status}")),
        }
    }
}

impl ObjectStore for GcsStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<u64, StoreFault> {
        self.stats.puts.fetch_add(1, Ordering::SeqCst);
        self.stats
            .bytes_up
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        let resp = self.request("PUT", key, &[], Some(&bytes))?;
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

    fn put_cas(&self, key: &str, expected: Option<u64>, bytes: Vec<u8>) -> Result<u64, StoreFault> {
        self.stats.cas_puts.fetch_add(1, Ordering::SeqCst);
        self.stats
            .bytes_up
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        let precondition = expected.unwrap_or(0).to_string();
        let headers = [("x-goog-if-generation-match", precondition)];
        let resp = self.request("PUT", key, &headers, Some(&bytes))?;
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
                    actual: self.head_generation(key)?,
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

    fn get(&self, key: &str) -> GetResult {
        self.stats.gets.fetch_add(1, Ordering::SeqCst);
        let resp = self.request("GET", key, &[], None)?;
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

    fn get_range(&self, key: &str, offset: u64, len: u64) -> GetResult {
        assert!(len > 0, "zero-length range read");
        self.stats.ranged_gets.fetch_add(1, Ordering::SeqCst);
        let range = format!("bytes={offset}-{}", offset + len - 1);
        let headers = [("Range", range)];
        let resp = self.request("GET", key, &headers, None)?;
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

    fn delete(&self, key: &str) {
        self.stats.deletes.fetch_add(1, Ordering::SeqCst);
        // Fire-and-forget (R4.5): a failed delete leaks one superseded
        // object; the daemon never re-drives deletes, so neither do we.
        let _ = self.request("DELETE", key, &[], None);
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
            "v/000000000badcafe/rs",
            "v/000000000badcafe/m/0000000000000002-000000000000001f",
            "v/000000000badcafe/s/0000000000000002-0000000000000003",
            "v/000000000badcafe/l/0000000000000002-0000000000000007",
            "b/000000000badcafe/rec",
            "b/000000000badcafe/l/0000000000000002-0000000000000007",
        ] {
            assert_eq!(encode_key(key), key);
        }
        assert_eq!(encode_key("a key%"), "a%20key%25");
    }
}
