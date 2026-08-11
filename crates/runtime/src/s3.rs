//! An in-process object store with an ACCURATE S3 API shape: the exact
//! operations, conditions, and semantics blockd uses in production —
//! `PutObject` with `If-None-Match: *` / `If-Match: <etag>` conditional
//! writes (the R6.3 CAS instrument), `GetObject` with HTTP `Range`
//! headers, idempotent `DeleteObject`, and paginated `ListObjectsV2`.
//! Bodies and errors follow S3's contract (opaque quoted `ETags`, 412
//! `PreconditionFailed`, `NoSuchKey`, 416 `InvalidRange`); only the wire
//! is elided.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Injected request latency, for performance testing against realistic
/// object-store behavior. The `same_region` preset uses typical published
/// same-region S3 figures: small-object GET time-to-first-byte ~12 ms,
/// PUT ~20 ms, LIST ~30 ms, DELETE ~15 ms, ~90 MB/s streaming.
#[derive(Clone, Copy, Debug)]
pub struct S3LatencyModel {
    pub get_first_byte: Duration,
    pub put_first_byte: Duration,
    pub list: Duration,
    pub delete: Duration,
    /// Streaming cost per mebibyte transferred, on top of first-byte.
    pub per_mib: Duration,
}

impl S3LatencyModel {
    pub fn same_region() -> S3LatencyModel {
        S3LatencyModel {
            get_first_byte: Duration::from_millis(12),
            put_first_byte: Duration::from_millis(20),
            list: Duration::from_millis(30),
            delete: Duration::from_millis(15),
            per_mib: Duration::from_millis(11), // ~90 MB/s
        }
    }

    #[allow(clippy::cast_precision_loss)] // presentation math, test scale
    fn transfer(self, first_byte: Duration, bytes: usize) -> Duration {
        first_byte + self.per_mib.mul_f64(bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Request statistics by S3 operation type — what a bill and a rate-limit
/// conversation are made of.
#[derive(Debug, Default)]
pub struct S3Stats {
    pub get_object: AtomicU64,
    pub get_object_range: AtomicU64,
    pub put_object: AtomicU64,
    pub put_object_conditional: AtomicU64,
    pub precondition_failures: AtomicU64,
    pub delete_object: AtomicU64,
    pub list_objects_v2: AtomicU64,
    pub bytes_downloaded: AtomicU64,
    pub bytes_uploaded: AtomicU64,
}

impl S3Stats {
    #[allow(clippy::cast_precision_loss)] // presentation math, test scale
    pub fn report(&self) -> String {
        format!(
            "GetObject {} (+{} ranged)  PutObject {} (+{} conditional, {} 412s)               DeleteObject {}  ListObjectsV2 {}  down {:.1} MiB  up {:.1} MiB",
            self.get_object.load(Ordering::SeqCst),
            self.get_object_range.load(Ordering::SeqCst),
            self.put_object.load(Ordering::SeqCst),
            self.put_object_conditional.load(Ordering::SeqCst),
            self.precondition_failures.load(Ordering::SeqCst),
            self.delete_object.load(Ordering::SeqCst),
            self.list_objects_v2.load(Ordering::SeqCst),
            self.bytes_downloaded.load(Ordering::SeqCst) as f64 / (1024.0 * 1024.0),
            self.bytes_uploaded.load(Ordering::SeqCst) as f64 / (1024.0 * 1024.0),
        )
    }

    pub fn total_requests(&self) -> u64 {
        self.get_object.load(Ordering::SeqCst)
            + self.get_object_range.load(Ordering::SeqCst)
            + self.put_object.load(Ordering::SeqCst)
            + self.put_object_conditional.load(Ordering::SeqCst)
            + self.delete_object.load(Ordering::SeqCst)
            + self.list_objects_v2.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum S3Error {
    /// 404 `NoSuchKey`.
    NoSuchKey,
    /// 412 `PreconditionFailed`: the `If-Match`/`If-None-Match` condition
    /// did not hold.
    PreconditionFailed,
    /// 416 `InvalidRange`: the requested range starts past the object.
    InvalidRange,
}

#[derive(Clone, Debug)]
struct Object {
    etag: String,
    body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListObjectsV2Output {
    /// (key, size, etag) — `Contents`, lexicographic like S3.
    pub contents: Vec<(String, usize, String)>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

/// The bucket.
pub struct S3Sim {
    objects: Mutex<BTreeMap<String, Object>>,
    /// Monotone counter making every `ETag` unique (S3 `ETags` are opaque;
    /// equal-content puts still produce distinct entity tags across
    /// versions of a key here, which is the conservative shape).
    etag_seq: Mutex<u64>,
    /// Injected per-request latency (perf testing); `None` = instant.
    latency: Option<S3LatencyModel>,
    /// Request counts and bytes by operation type.
    pub stats: S3Stats,
}

impl Default for S3Sim {
    fn default() -> S3Sim {
        S3Sim::new()
    }
}

impl S3Sim {
    pub fn new() -> S3Sim {
        S3Sim {
            objects: Mutex::new(BTreeMap::new()),
            etag_seq: Mutex::new(0),
            latency: None,
            stats: S3Stats::default(),
        }
    }

    /// Inject a request-latency model (performance testing).
    pub fn set_latency(&mut self, model: S3LatencyModel) {
        self.latency = Some(model);
    }

    fn delay(&self, base: impl Fn(S3LatencyModel) -> Duration, bytes: usize) {
        if let Some(model) = self.latency {
            std::thread::sleep(model.transfer(base(model), bytes));
        }
    }

    fn mint_etag(&self, body: &[u8]) -> String {
        let mut seq = self.etag_seq.lock().expect("lock");
        *seq += 1;
        // FNV-1a over the body, mixed with the sequence: opaque, unique,
        // quoted — the shape of a real ETag.
        let mut h = 0xcbf2_9ce4_8422_2325u64;
        for &b in body {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("\"{h:016x}{:08x}\"", *seq)
    }

    /// `PutObject`. `if_none_match_any` models `If-None-Match: *` (create
    /// only); `if_match` models `If-Match: <etag>` (replace exactly this
    /// version). Both `None`/`false` is a plain last-writer-wins put.
    /// Returns the new `ETag`.
    pub fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        if_match: Option<&str>,
        if_none_match_any: bool,
    ) -> Result<String, S3Error> {
        if if_match.is_some() || if_none_match_any {
            self.stats
                .put_object_conditional
                .fetch_add(1, Ordering::SeqCst);
        } else {
            self.stats.put_object.fetch_add(1, Ordering::SeqCst);
        }
        self.stats
            .bytes_uploaded
            .fetch_add(body.len() as u64, Ordering::SeqCst);
        self.delay(|m| m.put_first_byte, body.len());
        let etag = self.mint_etag(&body);
        let mut objects = self.objects.lock().expect("lock");
        let current = objects.get(key);
        if if_none_match_any && current.is_some() {
            self.stats
                .precondition_failures
                .fetch_add(1, Ordering::SeqCst);
            return Err(S3Error::PreconditionFailed);
        }
        if let Some(expected) = if_match {
            match current {
                Some(object) if object.etag == expected => {}
                _ => {
                    self.stats
                        .precondition_failures
                        .fetch_add(1, Ordering::SeqCst);
                    return Err(S3Error::PreconditionFailed);
                }
            }
        }
        objects.insert(
            key.to_owned(),
            Object {
                etag: etag.clone(),
                body,
            },
        );
        Ok(etag)
    }

    /// `GetObject`, optionally with a `Range: bytes=first-last` header
    /// (inclusive, per HTTP). A range beyond the end is truncated like S3
    /// truncates; a range starting past the end is 416.
    pub fn get_object(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<(String, Vec<u8>), S3Error> {
        if range.is_some() {
            self.stats.get_object_range.fetch_add(1, Ordering::SeqCst);
        } else {
            self.stats.get_object.fetch_add(1, Ordering::SeqCst);
        }
        let objects = self.objects.lock().expect("lock");
        let object = objects.get(key).ok_or(S3Error::NoSuchKey)?;
        let Some((first, last)) = range else {
            let body = object.body.clone();
            let etag = object.etag.clone();
            drop(objects);
            self.stats
                .bytes_downloaded
                .fetch_add(body.len() as u64, Ordering::SeqCst);
            self.delay(|m| m.get_first_byte, body.len());
            return Ok((etag, body));
        };
        let len = object.body.len() as u64;
        if first >= len || first > last {
            return Err(S3Error::InvalidRange);
        }
        let end = usize::try_from((last + 1).min(len)).expect("fits");
        let start = usize::try_from(first).expect("fits");
        let body = object.body[start..end].to_vec();
        let etag = object.etag.clone();
        drop(objects);
        self.stats
            .bytes_downloaded
            .fetch_add(body.len() as u64, Ordering::SeqCst);
        self.delay(|m| m.get_first_byte, body.len());
        Ok((etag, body))
    }

    /// `DeleteObject`: 204 regardless of existence, like S3.
    pub fn delete_object(&self, key: &str) {
        self.stats.delete_object.fetch_add(1, Ordering::SeqCst);
        self.delay(|m| m.delete, 0);
        self.objects.lock().expect("lock").remove(key);
    }

    /// `ListObjectsV2` with prefix and pagination.
    pub fn list_objects_v2(
        &self,
        prefix: &str,
        continuation_token: Option<&str>,
        max_keys: usize,
    ) -> ListObjectsV2Output {
        self.stats.list_objects_v2.fetch_add(1, Ordering::SeqCst);
        self.delay(|m| m.list, 0);
        let objects = self.objects.lock().expect("lock");
        let start_after = continuation_token.unwrap_or("");
        let mut contents = Vec::new();
        let mut truncated = false;
        for (key, object) in objects.range::<str, _>((
            std::ops::Bound::Excluded(start_after),
            std::ops::Bound::Unbounded,
        )) {
            if !key.starts_with(prefix) {
                if key.as_str() > prefix && !prefix.is_empty() {
                    break; // past the prefix range entirely
                }
                continue;
            }
            if contents.len() == max_keys {
                truncated = true;
                break;
            }
            contents.push((key.clone(), object.body.len(), object.etag.clone()));
        }
        let next = truncated.then(|| contents.last().expect("nonempty").0.clone());
        ListObjectsV2Output {
            contents,
            is_truncated: truncated,
            next_continuation_token: next,
        }
    }
}

/// The seam adapter: blockd-core speaks (key, u64 version) CAS; S3 speaks
/// `ETags`. Every writer derives versions the same way — by counting a
/// key's successful puts — so the u64 the daemon sees is just a name for
/// an `ETag`. Shared by every host against the same bucket.
/// Per-key registry entry: (current version, version → etag).
type KeyVersions = (u64, BTreeMap<u64, String>);

pub struct S3Store {
    pub s3: S3Sim,
    registry: Mutex<BTreeMap<String, KeyVersions>>,
    outage: std::sync::atomic::AtomicBool,
    data_outage: std::sync::atomic::AtomicBool,
}

impl Default for S3Store {
    fn default() -> S3Store {
        S3Store::new()
    }
}

pub type GetResult = Result<Option<(u64, Vec<u8>)>, blockd_core::protocol::StoreFault>;

impl S3Store {
    pub fn new() -> S3Store {
        S3Store {
            s3: S3Sim::new(),
            registry: Mutex::new(BTreeMap::new()),
            outage: std::sync::atomic::AtomicBool::new(false),
            data_outage: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn set_outage(&self, unavailable: bool) {
        self.outage.store(unavailable, Ordering::SeqCst);
    }

    /// Fault injection for a stalled data plane while the small fenced head
    /// control plane remains available. This models the condition replacement
    /// must tolerate: peer uploads cannot make progress, but assignment CASes
    /// still linearize.
    pub fn set_data_outage(&self, unavailable: bool) {
        self.data_outage.store(unavailable, Ordering::SeqCst);
    }

    fn unavailable(&self) -> bool {
        self.outage.load(Ordering::SeqCst)
    }

    fn data_unavailable(&self, key: &str) -> bool {
        self.data_outage.load(Ordering::SeqCst) && !key.ends_with("/head")
    }

    fn record_put(&self, key: &str, etag: String) -> u64 {
        let mut registry = self.registry.lock().expect("lock");
        let entry = registry
            .entry(key.to_owned())
            .or_insert_with(|| (0, BTreeMap::new()));
        entry.0 += 1;
        entry.1.insert(entry.0, etag);
        entry.0
    }

    fn version_of(&self, key: &str, etag: &str) -> u64 {
        let registry = self.registry.lock().expect("lock");
        let (_, versions) = registry.get(key).expect("every write went through us");
        *versions
            .iter()
            .find(|(_, e)| e.as_str() == etag)
            .expect("etag minted by this bucket")
            .0
    }

    fn current_version(&self, key: &str) -> Option<u64> {
        let registry = self.registry.lock().expect("lock");
        registry.get(key).map(|(v, _)| *v)
    }

    /// Unconditional put (segments, manifests, resume sets).
    pub fn put(&self, key: &str, bytes: Vec<u8>) -> Result<u64, blockd_core::protocol::StoreFault> {
        if self.unavailable() || self.data_unavailable(key) {
            return Err(blockd_core::protocol::StoreFault::Unavailable);
        }
        let etag = self
            .s3
            .put_object(key, bytes, None, false)
            .expect("unconditional puts cannot fail");
        Ok(self.record_put(key, etag))
    }

    /// Conditional put (the head CAS, R6.3): `expected: None` is
    /// `If-None-Match: *`; `Some(v)` is `If-Match` on v's `ETag`.
    pub fn put_cas(
        &self,
        key: &str,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, blockd_core::protocol::StoreFault> {
        if self.unavailable() {
            return Err(blockd_core::protocol::StoreFault::Unavailable);
        }
        let result = match expected {
            None => self.s3.put_object(key, bytes, None, true),
            Some(version) => {
                let etag = {
                    let registry = self.registry.lock().expect("lock");
                    registry
                        .get(key)
                        .and_then(|(_, versions)| versions.get(&version).cloned())
                };
                match etag {
                    Some(etag) => self.s3.put_object(key, bytes, Some(&etag), false),
                    // The daemon expects a version this bucket never
                    // minted: the condition cannot hold.
                    None => Err(S3Error::PreconditionFailed),
                }
            }
        };
        match result {
            Ok(etag) => Ok(self.record_put(key, etag)),
            Err(S3Error::PreconditionFailed) => {
                Err(blockd_core::protocol::StoreFault::CasConflict {
                    actual: self.current_version(key),
                })
            }
            Err(other) => panic!("unexpected S3 error on put: {other:?}"),
        }
    }

    pub fn get(&self, key: &str) -> GetResult {
        if self.unavailable() || self.data_unavailable(key) {
            return Err(blockd_core::protocol::StoreFault::Unavailable);
        }
        match self.s3.get_object(key, None) {
            Ok((etag, body)) => Ok(Some((self.version_of(key, &etag), body))),
            Err(S3Error::NoSuchKey) => Ok(None),
            Err(other) => panic!("unexpected S3 error on get: {other:?}"),
        }
    }

    pub fn get_range(&self, key: &str, offset: u64, len: u64) -> GetResult {
        if self.unavailable() || self.data_unavailable(key) {
            return Err(blockd_core::protocol::StoreFault::Unavailable);
        }
        match self.s3.get_object(key, Some((offset, offset + len - 1))) {
            Ok((etag, body)) => Ok(Some((self.version_of(key, &etag), body))),
            Err(S3Error::NoSuchKey | S3Error::InvalidRange) => Ok(None),
            Err(other) => panic!("unexpected S3 error on get_range: {other:?}"),
        }
    }

    pub fn delete(&self, key: &str) {
        if self.unavailable() || self.data_unavailable(key) {
            return;
        }
        self.s3.delete_object(key);
    }

    pub fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, blockd_core::protocol::StoreFault> {
        if self.unavailable() || self.data_unavailable(prefix) {
            return Err(blockd_core::protocol::StoreFault::Unavailable);
        }
        let mut keys = Vec::new();
        let mut continuation = None;
        loop {
            let page = self
                .s3
                .list_objects_v2(prefix, continuation.as_deref(), 1_000);
            keys.extend(page.contents.into_iter().map(|(key, _, _)| key));
            if !page.is_truncated {
                break;
            }
            continuation = page.next_continuation_token;
            if continuation.is_none() {
                return Err(blockd_core::protocol::StoreFault::Unavailable);
            }
        }
        Ok(keys)
    }
}

#[async_trait::async_trait]
impl crate::store::ObjectStore for S3Store {
    async fn put(
        self: std::sync::Arc<Self>,
        key: String,
        bytes: Vec<u8>,
    ) -> Result<u64, blockd_core::protocol::StoreFault> {
        tokio::task::spawn_blocking(move || S3Store::put(&self, &key, bytes))
            .await
            .expect("S3 simulation task")
    }

    async fn put_cas(
        self: std::sync::Arc<Self>,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, blockd_core::protocol::StoreFault> {
        tokio::task::spawn_blocking(move || S3Store::put_cas(&self, &key, expected, bytes))
            .await
            .expect("S3 simulation task")
    }

    async fn get(self: std::sync::Arc<Self>, key: String) -> crate::store::GetResult {
        tokio::task::spawn_blocking(move || S3Store::get(&self, &key))
            .await
            .expect("S3 simulation task")
    }

    async fn get_range(
        self: std::sync::Arc<Self>,
        key: String,
        offset: u64,
        len: u64,
    ) -> crate::store::GetResult {
        tokio::task::spawn_blocking(move || S3Store::get_range(&self, &key, offset, len))
            .await
            .expect("S3 simulation task")
    }

    async fn delete(self: std::sync::Arc<Self>, key: String) {
        tokio::task::spawn_blocking(move || S3Store::delete(&self, &key))
            .await
            .expect("S3 simulation task");
    }

    async fn list_prefix(
        self: std::sync::Arc<Self>,
        prefix: String,
    ) -> Result<Vec<String>, blockd_core::protocol::StoreFault> {
        tokio::task::spawn_blocking(move || S3Store::list_prefix(&self, &prefix))
            .await
            .expect("S3 simulation task")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockd_core::protocol::StoreFault;

    #[test]
    fn injected_outage_rejects_reads_and_writes_without_mutating_objects() {
        let store = S3Store::new();
        let version = store.put("stable", vec![1, 2, 3]).expect("initial put");
        store.set_outage(true);

        assert_eq!(store.get("stable"), Err(StoreFault::Unavailable));
        assert_eq!(
            store.get_range("stable", 0, 1),
            Err(StoreFault::Unavailable)
        );
        assert_eq!(store.put("new", vec![4]), Err(StoreFault::Unavailable));
        assert_eq!(
            store.put_cas("stable", Some(version), vec![9]),
            Err(StoreFault::Unavailable)
        );
        store.delete("stable");

        store.set_outage(false);
        assert_eq!(store.get("stable"), Ok(Some((version, vec![1, 2, 3]))));
        assert_eq!(store.get("new"), Ok(None));
    }

    #[test]
    fn data_outage_keeps_head_get_and_cas_available() {
        let store = S3Store::new();
        let head = "v/0000000000000001/head";
        let version = store.put_cas(head, None, vec![1]).expect("create head");
        store.set_data_outage(true);

        assert_eq!(
            store.put("v/1/segment", vec![2]),
            Err(StoreFault::Unavailable)
        );
        assert_eq!(store.get("v/1/segment"), Err(StoreFault::Unavailable));
        assert_eq!(store.get(head), Ok(Some((version, vec![1]))));
        assert_eq!(store.put_cas(head, Some(version), vec![3]), Ok(version + 1));
    }

    #[test]
    fn prefix_listing_paginates_and_filters() {
        let store = S3Store::new();
        for index in 0..1_005 {
            store
                .put(&format!("v/1/{index:04}"), vec![1])
                .expect("put object");
        }
        store.put("v/2/other", vec![1]).expect("put other prefix");
        let keys = store.list_prefix("v/1/").expect("list prefix");
        assert_eq!(keys.len(), 1_005);
        assert!(keys.iter().all(|key| key.starts_with("v/1/")));
    }
}
