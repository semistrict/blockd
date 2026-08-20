//! The GCS adapter against a scripted in-process HTTP server: status and
//! header mapping, token lifecycle, and the store contract — no GCP
//! involved. The one test that talks to a real bucket is `#[ignore]`d and
//! keyed by `BLOCKD_GCS_TEST_BUCKET` (run it on a GCE VM).

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::Ordering;

use blockd_core::protocol::StoreFault;
use blockd_runtime::fakegcs::{FakeGcs, Fault, Seen};
use blockd_runtime::{GcsConfig, GcsStore};

fn store_against(endpoint: &str) -> GcsStore {
    GcsStore::new(GcsConfig {
        bucket: "demo-bucket".to_owned(),
        prefix: "blockd/".to_owned(),
        endpoint: endpoint.to_owned(),
        metadata_endpoint: endpoint.to_owned(),
    })
}

fn store_against_with_timeout(endpoint: &str, timeout: std::time::Duration) -> GcsStore {
    GcsStore::with_request_timeout(
        GcsConfig {
            bucket: "demo-bucket".to_owned(),
            prefix: "blockd/".to_owned(),
            endpoint: endpoint.to_owned(),
            metadata_endpoint: endpoint.to_owned(),
        },
        timeout,
    )
}

async fn control(endpoint: &str, path: &str) {
    let response = reqwest::Client::new()
        .get(format!("{endpoint}{path}"))
        .send()
        .await
        .expect("control request");
    assert!(response.status().is_success(), "{response:?}");
}

#[tokio::test]
async fn availability_controls_distinguish_fencing_from_immutable_data() {
    let (_server, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    store
        .put("v/01/head", b"head".to_vec())
        .await
        .expect("head");
    store
        .put("v/01/data", b"data".to_vec())
        .await
        .expect("data");

    control(&endpoint, "/__control/data-outage/on").await;
    assert!(matches!(store.get("v/01/head").await, Ok(Some(_))));
    assert_eq!(store.get("v/01/data").await, Err(StoreFault::Unavailable));
    control(&endpoint, "/__control/data-outage/off").await;
    assert!(matches!(store.get("v/01/data").await, Ok(Some(_))));

    control(&endpoint, "/__control/outage/on").await;
    assert_eq!(store.get("v/01/head").await, Err(StoreFault::Unavailable));
    control(&endpoint, "/__control/outage/off").await;
    assert!(matches!(store.get("v/01/head").await, Ok(Some(_))));
}

#[tokio::test]
async fn prefix_listing_returns_complete_logical_keys() {
    let (_server, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    for key in [
        "v/01/head",
        "v/01/o/0001-0001.blx",
        "v/02/head",
        "b/01/root",
    ] {
        store.put(key, vec![1]).await.expect("put listed object");
    }
    assert_eq!(
        store.list_prefix("v/01/").await.expect("list prefix"),
        vec!["v/01/head".to_owned(), "v/01/o/0001-0001.blx".to_owned(),]
    );
}

#[tokio::test]
async fn versioned_listing_separates_lease_generation_from_content_fingerprint() {
    let (_server, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    let key = "cluster/tls/public-keys/00000001.member";
    let first_generation = store
        .put(key, b"same membership body".to_vec())
        .await
        .expect("first heartbeat");
    let first = store
        .list_prefix_versioned("cluster/tls/public-keys/")
        .await
        .expect("first listing")
        .pop()
        .expect("listed member");
    assert_eq!(first.generation, first_generation);
    let fingerprint = first.fingerprint.expect("GCS listing fingerprint");

    let second_generation = store
        .put(key, b"same membership body".to_vec())
        .await
        .expect("identical heartbeat");
    let second = store
        .list_prefix_versioned("cluster/tls/public-keys/")
        .await
        .expect("second listing")
        .pop()
        .expect("listed member");
    assert_ne!(second_generation, first_generation);
    assert_eq!(second.generation, second_generation);
    assert_eq!(second.fingerprint.as_deref(), Some(fingerprint.as_str()));

    store
        .put(key, b"changed membership body".to_vec())
        .await
        .expect("changed descriptor");
    let changed = store
        .list_prefix_versioned("cluster/tls/public-keys/")
        .await
        .expect("changed listing")
        .pop()
        .expect("listed member");
    assert_ne!(changed.fingerprint.as_deref(), Some(fingerprint.as_str()));
}

/// The store contract, end to end against the stateful fake: create-only
/// CAS, replace CAS, conflicts carrying the current generation, misses as
/// `Ok(None)`, ranged reads with EOF semantics, delete.
#[tokio::test]
async fn the_store_contract_holds_against_generation_semantics() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);

    // Create-only CAS wins once, then conflicts with the current version.
    let v1 = store
        .put_cas("v/01/head", None, b"head-1".to_vec())
        .await
        .expect("create");
    let conflict = store.put_cas("v/01/head", None, b"usurper".to_vec()).await;
    assert_eq!(conflict, Err(StoreFault::CasConflict { actual: Some(v1) }));
    // Replace CAS with the right version wins; a stale version conflicts.
    let v2 = store
        .put_cas("v/01/head", Some(v1), b"head-2".to_vec())
        .await
        .expect("replace");
    assert!(v2 > v1, "generations are monotone");
    let stale = store
        .put_cas("v/01/head", Some(v1), b"stale".to_vec())
        .await;
    assert_eq!(stale, Err(StoreFault::CasConflict { actual: Some(v2) }));
    // CAS against an absent key reports absence.
    let ghost = store.put_cas("v/01/ghost", Some(7), b"x".to_vec()).await;
    assert_eq!(ghost, Err(StoreFault::CasConflict { actual: None }));

    // Plain put/get round-trip with matching generations.
    let object = (0u8..=255).cycle().take(10_000).collect::<Vec<u8>>();
    let key = "v/01/o/0001-0001.blx";
    let vs = store.put(key, object.clone()).await.expect("put");
    assert_eq!(store.get(key).await, Ok(Some((vs, object.clone()))));
    assert_eq!(store.get("v/01/absent").await, Ok(None));

    // Ranged reads: exact slice, EOF-straddling tail, past-EOF miss.
    assert_eq!(
        store.get_range(key, 256, 512).await,
        Ok(Some((vs, object[256..768].to_vec())))
    );
    assert_eq!(
        store.get_range(key, 9_900, 500).await,
        Ok(Some((vs, object[9_900..].to_vec())))
    );
    assert_eq!(store.get_range(key, 10_000, 1).await, Ok(None));
    assert_eq!(store.get_range("v/01/nothing", 0, 8).await, Ok(None));

    // Delete is fire-and-forget and idempotent.
    assert!(store.delete(key).await.expect("delete object"));
    assert!(!store.delete(key).await.expect("idempotent delete"));
    assert_eq!(store.get(key).await, Ok(None));

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
#[tokio::test]
async fn a_cas_conflict_heads_for_the_current_generation() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    let v1 = store
        .put_cas("k", None, b"one".to_vec())
        .await
        .expect("create");
    store
        .put_cas("k", None, b"two".to_vec())
        .await
        .expect_err("conflict");
    let seen = fake.seen.lock().expect("lock").clone();
    let methods: Vec<&str> = seen.iter().map(|s| s.method.as_str()).collect();
    assert_eq!(methods, ["PUT", "PUT", "HEAD"]);
    let _ = v1;
}

/// Transient statuses and dead connections are retried with bounded jitter;
/// an exhausted retry budget remains `Unavailable`. The store never invents
/// data.
#[tokio::test]
async fn transient_faults_retry_then_map_exhaustion_to_unavailable() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    store.put("k", b"seed".to_vec()).await.expect("seed");
    for fault in [
        Fault::Status(429),
        Fault::Status(500),
        Fault::Status(503),
        Fault::DropConnection,
    ] {
        fake.faults.lock().expect("lock").push(fault);
        assert!(
            matches!(store.get("k").await, Ok(Some(_))),
            "{fault:?} must recover on retry"
        );
    }
    fake.faults
        .lock()
        .expect("lock")
        .extend([Fault::Status(503); 3]);
    assert_eq!(store.get("k").await, Err(StoreFault::Unavailable));
}

#[tokio::test]
async fn delete_distinguishes_success_absence_and_backend_failures() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    store.put("delete-me", vec![1]).await.expect("seed");
    assert_eq!(store.delete("delete-me").await, Ok(true));
    assert_eq!(store.delete("delete-me").await, Ok(false));

    for permanent in [403, 409] {
        fake.faults
            .lock()
            .expect("lock")
            .push(Fault::Status(permanent));
        assert_eq!(
            store.delete("denied").await,
            Err(StoreFault::Unavailable),
            "status {permanent} must not be reported as deleted"
        );
    }
    for transient in [408, 429, 500, 503] {
        fake.faults
            .lock()
            .expect("lock")
            .extend([Fault::Status(transient); 3]);
        assert_eq!(
            store.delete("retry-exhausted").await,
            Err(StoreFault::Unavailable),
            "status {transient} must retry and then fail"
        );
    }
    fake.faults.lock().expect("lock").extend([
        Fault::DropConnection,
        Fault::DropConnection,
        Fault::DropConnection,
    ]);
    assert_eq!(
        store.delete("transport-drop").await,
        Err(StoreFault::Unavailable)
    );

    let timeout_store = store_against_with_timeout(&endpoint, std::time::Duration::from_millis(20));
    fake.faults.lock().expect("lock").extend([
        Fault::Stall(std::time::Duration::from_millis(100)),
        Fault::Stall(std::time::Duration::from_millis(100)),
        Fault::Stall(std::time::Duration::from_millis(100)),
    ]);
    assert_eq!(
        timeout_store.delete("transport-timeout").await,
        Err(StoreFault::Unavailable)
    );
}

/// One 401 buys one token refresh and a retry; the operation succeeds.
#[tokio::test]
async fn a_401_refreshes_the_token_once_and_retries() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    store.put("k", b"seed".to_vec()).await.expect("seed");
    assert_eq!(fake.tokens_served.load(Ordering::SeqCst), 1);
    fake.faults.lock().expect("lock").push(Fault::Status(401));
    assert!(matches!(store.get("k").await, Ok(Some(_))));
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
#[tokio::test]
async fn tokens_are_cached_until_the_slack_window() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    store.put("a", b"1".to_vec()).await.expect("put");
    store.put("b", b"2".to_vec()).await.expect("put");
    assert_eq!(fake.tokens_served.load(Ordering::SeqCst), 1, "cached");
    // A token whose whole life is inside the slack is never fresh enough:
    // every operation refreshes.
    fake.token_expires_in.store(100, Ordering::SeqCst);
    let (fake2, endpoint2) = (fake, endpoint);
    let short_store = store_against(&endpoint2);
    short_store.put("c", b"3".to_vec()).await.expect("put");
    short_store.put("d", b"4".to_vec()).await.expect("put");
    assert_eq!(
        fake2.tokens_served.load(Ordering::SeqCst),
        3,
        "refreshed each"
    );
}

#[tokio::test]
async fn independent_requests_are_in_flight_together() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    store.put("a", b"1".to_vec()).await.expect("put");
    store.put("b", b"2".to_vec()).await.expect("put");
    fake.latency_ms.store(50, Ordering::SeqCst);

    let (a, b) = tokio::join!(store.get("a"), store.get("b"));
    assert!(matches!(a, Ok(Some(_))));
    assert!(matches!(b, Ok(Some(_))));
    assert!(
        fake.max_in_flight.load(Ordering::SeqCst) >= 2,
        "object requests must overlap rather than occupy a thread serially"
    );
}

#[tokio::test]
async fn reads_and_uploads_use_independent_connection_pools() {
    let (fake, endpoint) = FakeGcs::start().await;
    let store = store_against(&endpoint);
    store.put("a", b"1".to_vec()).await.expect("put");
    assert!(matches!(store.get("a").await, Ok(Some(_))));

    let seen = fake.seen.lock().expect("lock");
    let put_peer = seen
        .iter()
        .find(|request| request.method == "PUT")
        .expect("PUT observed")
        .peer;
    let get_peer = seen
        .iter()
        .find(|request| request.method == "GET")
        .expect("GET observed")
        .peer;
    assert_ne!(
        put_peer.port(),
        get_peer.port(),
        "read and write clients must not share one transport connection"
    );
}

/// The real thing, run manually on a GCE VM with a bucket-scoped service
/// account:
/// `BLOCKD_GCS_TEST_BUCKET=my-bucket cargo test -p blockd-runtime
/// --test gcs_store -- --ignored`
#[tokio::test]
#[ignore = "requires a GCE VM and BLOCKD_GCS_TEST_BUCKET"]
async fn gcs_real_bucket_round_trip() {
    let bucket = std::env::var("BLOCKD_GCS_TEST_BUCKET")
        .expect("set BLOCKD_GCS_TEST_BUCKET to run this test");
    let prefix = format!("test/{}/", std::process::id());
    let store = GcsStore::new(GcsConfig {
        bucket,
        prefix: prefix.clone(),
        endpoint: "https://storage.googleapis.com".to_owned(),
        metadata_endpoint: "http://metadata.google.internal".to_owned(),
    });
    let v1 = store
        .put_cas("head", None, b"h1".to_vec())
        .await
        .expect("create");
    assert_eq!(
        store.put_cas("head", None, b"h2".to_vec()).await,
        Err(StoreFault::CasConflict { actual: Some(v1) })
    );
    let v2 = store
        .put_cas("head", Some(v1), b"h2".to_vec())
        .await
        .expect("replace");
    assert!(v2 > v1);
    let body = vec![0xA5u8; 100_000];
    let vs = store.put("object", body.clone()).await.expect("put");
    assert_eq!(store.get("object").await, Ok(Some((vs, body.clone()))));
    assert_eq!(
        store.get_range("object", 50_000, 1_000).await,
        Ok(Some((vs, body[50_000..51_000].to_vec())))
    );
    assert_eq!(store.get_range("object", 100_000, 1).await, Ok(None));
    assert_eq!(store.get("missing").await, Ok(None));
    assert!(
        store
            .list_prefix("")
            .await
            .expect("list")
            .contains(&"head".to_owned())
    );
    store.delete("object").await.expect("delete object");
    store.delete("head").await.expect("delete head");
    assert_eq!(store.get("object").await, Ok(None));
}
