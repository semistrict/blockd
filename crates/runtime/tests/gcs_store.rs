//! The GCS adapter against a scripted in-process HTTP server: status and
//! header mapping, token lifecycle, and the store contract — no GCP
//! involved. The one test that talks to a real bucket is `#[ignore]`d and
//! keyed by `BLOCKD_GCS_TEST_BUCKET` (run it on a GCE VM).

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::Ordering;

use blockd_core::seam::StoreFault;
use blockd_runtime::fakegcs::{FakeGcs, Fault, Seen};
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore};

fn store_against(endpoint: &str) -> GcsStore {
    GcsStore::new(GcsConfig {
        bucket: "demo-bucket".to_owned(),
        prefix: "blockd/".to_owned(),
        endpoint: endpoint.to_owned(),
        metadata_endpoint: endpoint.to_owned(),
    })
}

/// The store contract, end to end against the stateful fake: create-only
/// CAS, replace CAS, conflicts carrying the current generation, misses as
/// `Ok(None)`, ranged reads with EOF semantics, delete.
#[test]
fn the_store_contract_holds_against_generation_semantics() {
    let (fake, endpoint) = FakeGcs::start();
    let store = store_against(&endpoint);

    // Create-only CAS wins once, then conflicts with the current version.
    let v1 = store
        .put_cas("v/01/head", None, b"head-1".to_vec())
        .expect("create");
    let conflict = store.put_cas("v/01/head", None, b"usurper".to_vec());
    assert_eq!(conflict, Err(StoreFault::CasConflict { actual: Some(v1) }));
    // Replace CAS with the right version wins; a stale version conflicts.
    let v2 = store
        .put_cas("v/01/head", Some(v1), b"head-2".to_vec())
        .expect("replace");
    assert!(v2 > v1, "generations are monotone");
    let stale = store.put_cas("v/01/head", Some(v1), b"stale".to_vec());
    assert_eq!(stale, Err(StoreFault::CasConflict { actual: Some(v2) }));
    // CAS against an absent key reports absence.
    let ghost = store.put_cas("v/01/ghost", Some(7), b"x".to_vec());
    assert_eq!(ghost, Err(StoreFault::CasConflict { actual: None }));

    // Plain put/get round-trip with matching generations.
    let seg = (0u8..=255).cycle().take(10_000).collect::<Vec<u8>>();
    let vs = store.put("v/01/s/seg-0", seg.clone()).expect("put");
    assert_eq!(store.get("v/01/s/seg-0"), Ok(Some((vs, seg.clone()))));
    assert_eq!(store.get("v/01/absent"), Ok(None));

    // Ranged reads: exact slice, EOF-straddling tail, past-EOF miss.
    assert_eq!(
        store.get_range("v/01/s/seg-0", 256, 512),
        Ok(Some((vs, seg[256..768].to_vec())))
    );
    assert_eq!(
        store.get_range("v/01/s/seg-0", 9_900, 500),
        Ok(Some((vs, seg[9_900..].to_vec())))
    );
    assert_eq!(store.get_range("v/01/s/seg-0", 10_000, 1), Ok(None));
    assert_eq!(store.get_range("v/01/nothing", 0, 8), Ok(None));

    // Delete is fire-and-forget and idempotent.
    store.delete("v/01/s/seg-0");
    store.delete("v/01/s/seg-0");
    assert_eq!(store.get("v/01/s/seg-0"), Ok(None));

    // The wire carried exactly the headers the contract requires.
    let seen = fake.seen.lock().expect("lock").clone();
    let cas_creates: Vec<&Seen> = seen
        .iter()
        .filter(|s| s.headers.get("x-goog-if-generation-match") == Some(&"0".to_owned()))
        .collect();
    assert_eq!(cas_creates.len(), 2, "two create-only CAS attempts");
    assert!(
        seen.iter()
            .any(|s| s.headers.get("x-goog-if-generation-match") == Some(&v1.to_string())),
        "replace CAS carried the expected generation"
    );
    assert!(
        seen.iter()
            .any(|s| s.headers.get("range") == Some(&"bytes=256-767".to_owned())),
        "ranged read carried the exact byte range"
    );
    assert!(
        seen.iter()
            .all(|s| s.path.starts_with("/demo-bucket/blockd/")),
        "every object request under bucket + prefix"
    );

    let latency = store.stats.latency();
    let samples = |operation: &str, outcome: &str| {
        latency
            .iter()
            .find(|item| item.operation == operation && item.outcome == outcome)
            .expect("latency series")
            .histogram
            .count
    };
    assert_eq!(samples("conditional_put", "success"), 2);
    assert_eq!(samples("conditional_put", "conflict"), 3);
    assert_eq!(samples("put", "success"), 1);
    assert!(samples("get", "success") >= 2);
}

/// A 412 is followed by a HEAD to fill in `actual` — visible on the wire.
#[test]
fn a_cas_conflict_heads_for_the_current_generation() {
    let (fake, endpoint) = FakeGcs::start();
    let store = store_against(&endpoint);
    let v1 = store.put_cas("k", None, b"one".to_vec()).expect("create");
    store
        .put_cas("k", None, b"two".to_vec())
        .expect_err("conflict");
    let seen = fake.seen.lock().expect("lock").clone();
    let methods: Vec<&str> = seen.iter().map(|s| s.method.as_str()).collect();
    assert_eq!(methods, ["PUT", "PUT", "HEAD"]);
    let _ = v1;
}

/// Transient statuses and dead connections are `Unavailable` — the
/// daemon's retry timers own the response. The store never invents data.
#[test]
fn transient_faults_map_to_unavailable() {
    let (fake, endpoint) = FakeGcs::start();
    let store = store_against(&endpoint);
    store.put("k", b"seed".to_vec()).expect("seed");
    for fault in [
        Fault::Status(429),
        Fault::Status(500),
        Fault::Status(503),
        Fault::DropConnection,
    ] {
        fake.faults.lock().expect("lock").push(fault);
        assert_eq!(
            store.get("k"),
            Err(StoreFault::Unavailable),
            "{fault:?} must be retryable"
        );
    }
    // The fault drained: the same get now succeeds (fresh connection).
    assert!(matches!(store.get("k"), Ok(Some(_))));
}

/// One 401 buys one token refresh and a retry; the operation succeeds.
#[test]
fn a_401_refreshes_the_token_once_and_retries() {
    let (fake, endpoint) = FakeGcs::start();
    let store = store_against(&endpoint);
    store.put("k", b"seed".to_vec()).expect("seed");
    assert_eq!(fake.tokens_served.load(Ordering::SeqCst), 1);
    fake.faults.lock().expect("lock").push(Fault::Status(401));
    assert!(matches!(store.get("k"), Ok(Some(_))));
    assert_eq!(
        fake.tokens_served.load(Ordering::SeqCst),
        2,
        "the 401 forced exactly one refresh"
    );
    let methods: Vec<String> = fake
        .seen
        .lock()
        .expect("lock")
        .iter()
        .map(|s| s.method.clone())
        .collect();
    assert_eq!(methods, ["PUT", "GET", "GET"]);
}

/// Tokens are cached while comfortably alive and re-fetched when the
/// remaining life dips under the refresh slack.
#[test]
fn tokens_are_cached_until_the_slack_window() {
    let (fake, endpoint) = FakeGcs::start();
    let store = store_against(&endpoint);
    store.put("a", b"1".to_vec()).expect("put");
    store.put("b", b"2".to_vec()).expect("put");
    assert_eq!(fake.tokens_served.load(Ordering::SeqCst), 1, "cached");
    // A token whose whole life is inside the slack is never fresh enough:
    // every operation refreshes.
    fake.token_expires_in.store(100, Ordering::SeqCst);
    let (fake2, endpoint2) = (fake, endpoint);
    let short_store = store_against(&endpoint2);
    short_store.put("c", b"3".to_vec()).expect("put");
    short_store.put("d", b"4".to_vec()).expect("put");
    assert_eq!(
        fake2.tokens_served.load(Ordering::SeqCst),
        3,
        "refreshed each"
    );
}

/// The real thing, run manually on a GCE VM with a bucket-scoped service
/// account:
/// `BLOCKD_GCS_TEST_BUCKET=my-bucket cargo test -p blockd-runtime
/// --test gcs_store -- --ignored`
#[test]
#[ignore = "requires a GCE VM and BLOCKD_GCS_TEST_BUCKET"]
fn gcs_real_bucket_round_trip() {
    let bucket = std::env::var("BLOCKD_GCS_TEST_BUCKET")
        .expect("set BLOCKD_GCS_TEST_BUCKET to run this test");
    let prefix = format!("test/{}/", std::process::id());
    let store = GcsStore::new(GcsConfig {
        bucket,
        prefix: prefix.clone(),
        endpoint: "https://storage.googleapis.com".to_owned(),
        metadata_endpoint: "http://metadata.google.internal".to_owned(),
    });
    let v1 = store.put_cas("head", None, b"h1".to_vec()).expect("create");
    assert_eq!(
        store.put_cas("head", None, b"h2".to_vec()),
        Err(StoreFault::CasConflict { actual: Some(v1) })
    );
    let v2 = store
        .put_cas("head", Some(v1), b"h2".to_vec())
        .expect("replace");
    assert!(v2 > v1);
    let body = vec![0xA5u8; 100_000];
    let vs = store.put("seg", body.clone()).expect("put");
    assert_eq!(store.get("seg"), Ok(Some((vs, body.clone()))));
    assert_eq!(
        store.get_range("seg", 50_000, 1_000),
        Ok(Some((vs, body[50_000..51_000].to_vec())))
    );
    assert_eq!(store.get_range("seg", 100_000, 1), Ok(None));
    assert_eq!(store.get("missing"), Ok(None));
    store.delete("seg");
    store.delete("head");
    assert_eq!(store.get("seg"), Ok(None));
}
