//! The cold-path part-fetch engine, pinned against a latency-injected
//! store: a fault storm must make progress at CONCURRENT fetch speed, not
//! one store round-trip at a time. Before the `PartTable`, the shmem
//! server held one global lock across each ~100ms `GetObject` — a storm
//! over N parts cost N round-trips serially, ~10 pages/second of forward
//! progress on unlucky access patterns.
//!
//! These tests drive the engine directly with channel wakers (no VM, no
//! uffd — the wake wiring itself is exercised by the FC cold-restore
//! test); the store's same-region latency model makes wall-clock the
//! honest measure of concurrency.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};

use blockd_hostmem::page_size;
use blockd_runtime::fc::{PartTable, upload_mem_parts};
use blockd_runtime::{S3LatencyModel, S3Store};

const PART_BYTES: u64 = 4 * 1024 * 1024;
const PARTS: u64 = 6;
const MEM_BYTES: u64 = PART_BYTES * PARTS;

/// A latency-injected bucket holding `PARTS` recognizable parts, and the
/// shmem file fills land in. Same-region GET of a 4 MiB part costs
/// ~12ms first-byte + ~44ms transfer ≈ 56ms.
fn store_and_shmem(tag: &str) -> (Arc<S3Store>, Arc<std::fs::File>, PathBuf) {
    let scratch = PathBuf::from(format!("/var/tmp/blockd-scratch/partfetch-{tag}"));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch");
    let mem_path = scratch.join("mem");
    let mem: Vec<u8> = (0..MEM_BYTES)
        .map(|i| u8::try_from((i / PART_BYTES + 1) * 17 % 251).expect("fits"))
        .collect();
    std::fs::write(&mem_path, &mem).expect("mem file");
    let mut store = S3Store::new();
    let parts = upload_mem_parts(&store, &mem_path, "v/0000000000000009/mem", PART_BYTES);
    assert_eq!(parts, PARTS);
    store.s3.set_latency(S3LatencyModel::same_region());
    let shmem_file = scratch.join("shmem");
    let shmem = Arc::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&shmem_file)
            .expect("shmem"),
    );
    shmem.set_len(MEM_BYTES).expect("size");
    (Arc::new(store), shmem, scratch)
}

fn cold_table(store: &Arc<S3Store>, shmem: &Arc<std::fs::File>, readahead: u64) -> Arc<PartTable> {
    PartTable::store(
        store.clone(),
        "v/0000000000000009/mem".to_owned(),
        PART_BYTES,
        shmem,
        MEM_BYTES,
        readahead,
    )
}

fn assert_part_filled(shmem: &std::fs::File, part: u64) {
    use std::os::unix::fs::FileExt;
    let mut byte = [0u8; 1];
    shmem
        .read_exact_at(&mut byte, part * PART_BYTES)
        .expect("read");
    assert_eq!(
        u64::from(byte[0]),
        (part + 1) * 17 % 251,
        "part {part} bytes wrong in shmem"
    );
}

/// A storm of faults across every part, three faulters per part: distinct
/// parts fetch CONCURRENTLY (wall clock ~one round-trip, not six) and
/// concurrent faults on one part share ONE fetch (exactly one `GetObject`
/// per part — the dedupe is what keeps a fork fleet from multiplying the
/// bill).
#[test]
fn a_fault_storm_fetches_distinct_parts_concurrently_and_dedupes_within() {
    let (store, shmem, _scratch) = store_and_shmem("storm");
    let table = cold_table(&store, &shmem, 0);
    let (tx, rx) = channel();
    let started = Instant::now();
    for part in 0..PARTS {
        for faulter in 0..3 {
            let tx = tx.clone();
            // Faults land on different pages of the part.
            table.fault(
                part * PART_BYTES + faulter * page_size() as u64,
                move || {
                    tx.send(part).expect("send");
                },
            );
        }
    }
    let mut woken = Vec::new();
    for _ in 0..PARTS * 3 {
        woken.push(rx.recv_timeout(Duration::from_secs(10)).expect("wake"));
    }
    let elapsed = started.elapsed();
    woken.sort_unstable();
    let expected: Vec<u64> = (0..PARTS).flat_map(|p| [p, p, p]).collect();
    assert_eq!(woken, expected, "every fault woken exactly once");
    assert_eq!(
        store.s3.stats.get_object.load(Ordering::SeqCst),
        PARTS,
        "one GetObject per part, regardless of faulters"
    );
    for part in 0..PARTS {
        assert_part_filled(&shmem, part);
    }
    // Serial fetching costs PARTS × ~56ms ≈ 336ms; concurrent is one
    // round-trip plus scheduling. The bound sits between the two with
    // room for a loaded CI-class VM.
    assert!(
        elapsed < Duration::from_millis(200),
        "storm took {elapsed:?} — parts are fetching serially"
    );
}

/// A fault into a part mid-fetch parks and wakes on the ONE in-flight
/// fetch; a fault after completion wakes inline with no new request.
#[test]
fn late_faults_join_the_inflight_fetch_and_ready_parts_wake_inline() {
    let (store, shmem, _scratch) = store_and_shmem("join");
    let table = cold_table(&store, &shmem, 0);
    let (tx, rx) = channel();
    let first = tx.clone();
    table.fault(0, move || first.send("first").expect("send"));
    // Immediately behind it, while the fetch is in flight.
    let second = tx.clone();
    table.fault(page_size() as u64, move || {
        second.send("second").expect("send");
    });
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10)).expect("wake"),
        "first"
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10)).expect("wake"),
        "second"
    );
    // Ready now: the wake is synchronous and free.
    let third = tx.clone();
    let started = Instant::now();
    table.fault(2 * page_size() as u64, move || {
        third.send("third").expect("send");
    });
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).expect("wake"),
        "third"
    );
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "ready part stalled"
    );
    assert_eq!(
        store.s3.stats.get_object.load(Ordering::SeqCst),
        1,
        "three faults on one part cost one GetObject"
    );
}

/// Readahead: a demand fault on part N starts part N+1's fetch
/// immediately, so a sequential reader finds the next part already in
/// flight (or done) instead of paying a full round-trip at each part
/// boundary — and readahead never chains off its own fills into eagerly
/// streaming the whole memory.
#[test]
fn readahead_keeps_the_next_part_in_flight_for_a_sequential_reader() {
    let (store, shmem, _scratch) = store_and_shmem("readahead");
    let table = cold_table(&store, &shmem, 1);
    let (tx, rx) = channel();
    let first = tx.clone();
    table.fault(0, move || first.send(0u64).expect("send"));
    assert_eq!(rx.recv_timeout(Duration::from_secs(10)).expect("wake"), 0);
    // Part 1's fetch was started by part 0's DEMAND fault, concurrently:
    // the sequential reader reaches it with the round-trip already paid.
    assert_eq!(
        store.s3.stats.get_object.load(Ordering::SeqCst),
        2,
        "part 0's demand fault must have issued part 1's readahead"
    );
    let started = Instant::now();
    let second = tx.clone();
    table.fault(PART_BYTES, move || second.send(1).expect("send"));
    assert_eq!(rx.recv_timeout(Duration::from_secs(10)).expect("wake"), 1);
    assert!(
        started.elapsed() < Duration::from_millis(30),
        "part 1 was not prefetched (waited {:?})",
        started.elapsed()
    );
    // Part 1's demand fault triggered part 2's readahead; parts 3..6 stay
    // untouched — readahead follows demand, it does not run away.
    let deadline = Instant::now() + Duration::from_secs(10);
    while table.filled.load(Ordering::SeqCst) < 3 * (PART_BYTES / page_size() as u64) {
        assert!(Instant::now() < deadline, "part 2 readahead never landed");
        std::thread::park_timeout(Duration::from_millis(5));
    }
    assert_eq!(
        store.s3.stats.get_object.load(Ordering::SeqCst),
        3,
        "exactly parts 0, 1 (readahead), 2 (readahead) were fetched"
    );
    assert_part_filled(&shmem, 0);
    assert_part_filled(&shmem, 1);
    assert_part_filled(&shmem, 2);
}
