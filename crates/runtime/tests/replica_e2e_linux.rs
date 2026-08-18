//! Real three-runtime peer-stash recovery: TCP, durable filesystem spools,
//! object-store outage, loss of the primary root, peer export, restart,
//! publication, failover during archive-data outage, and crash-safe spool
//! reclamation.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use blockd_core::head::HeadRecord;
use blockd_core::journal::VolumeConfig;
use blockd_core::layout;
use blockd_core::protocol::Verdict;
use blockd_core::replica_recovery::{ReplicaResidue, export_replica_recovery};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId};
use blockd_hostmem::page_size;
use blockd_runtime::{ObjectStore, Runtime, install_replica_recovery};

mod support;

const VOLUME: VolumeId = VolumeId(1);

fn spool_files(root: &Path, source: HostId, volume: VolumeId) -> Vec<(u64, PathBuf)> {
    fn visit(
        base: &Path,
        directory: &Path,
        source: HostId,
        volume: VolumeId,
        found: &mut Vec<(u64, PathBuf)>,
    ) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                visit(base, &path, source, volume, found);
            } else if let Ok(relative) = path.strip_prefix(base)
                && let Some(layout::BlobName::ReplicaSpool {
                    source: got_source,
                    volume: got_volume,
                    generation,
                    ..
                }) = layout::parse_blob(relative.to_string_lossy().as_ref())
                && (got_source, got_volume) == (source, volume)
            {
                found.push((generation, path));
            }
        }
    }
    let mut found = Vec::new();
    visit(root, root, source, volume, &mut found);
    found.sort_by_key(|(generation, _)| *generation);
    found
}

async fn durable_cluster(
    tag: &str,
    cache_pages: usize,
) -> (
    [SocketAddr; 3],
    [PathBuf; 3],
    support::TestGcs,
    Vec<Option<Runtime>>,
) {
    let addresses = [
        support::free_addr(),
        support::free_addr(),
        support::free_addr(),
    ];
    let roots = std::array::from_fn(|host| support::temp_root(&format!("{tag}-{host}")));
    let gcs = support::test_gcs(tag).await;
    let store = gcs.store.clone();
    let mut runtimes = Vec::new();
    for host in 0..3 {
        let mut config =
            support::three_host_runtime_config(host, roots[usize::from(host)].clone(), addresses);
        config.daemon.cache_pages = cache_pages;
        runtimes.push(Some(Runtime::new(&config, store.clone()).await));
    }
    (addresses, roots, gcs, runtimes)
}

fn page(volume: u8, number: u32) -> PageId {
    volume_page(VolumeId(u64::from(volume)), number)
}

fn volume_page(volume: VolumeId, number: u32) -> PageId {
    PageId {
        volume: volume,
        page: PageNo(number),
    }
}

async fn read_word(runtime: &Runtime, page: PageId) -> u64 {
    let bytes = runtime.guest_read(page.volume, page).await;
    u64::from_ne_bytes(bytes[..8].try_into().expect("word"))
}

#[tokio::test(flavor = "current_thread")]
async fn forked_shared_page_read_then_write_diverges_and_refaults() {
    support::local(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (_addresses, roots, _gcs, mut runtimes) = durable_cluster("fork-write", 8).await;
            let primary = runtimes[0].take().expect("primary");
            let config = VolumeConfig::data(32);
            let child = VolumeId(2);
            let inherited = volume_page(VOLUME, 0);
            let child_page = volume_page(child, 0);
            let parent_value = 0x1111_2222_3333_4444;
            let child_value = 0xaaaa_bbbb_cccc_dddd;

            primary.create_volume(VOLUME, config).await;
            primary.guest_write(VOLUME, inherited, parent_value).await;
            assert_eq!(primary.checkpoint(VOLUME).await, 1);
            primary.keep_base(VOLUME, 700).await;
            assert!(matches!(
                primary.fork_volume(child, config, 700).await,
                Verdict::ColdBoot
            ));

            assert_eq!(read_word(&primary, child_page).await, parent_value);
            primary.guest_write(child, child_page, child_value).await;
            assert_eq!(read_word(&primary, inherited).await, parent_value);
            assert_eq!(read_word(&primary, child_page).await, child_value);
            assert_eq!(primary.checkpoint(child).await, 1);

            for number in 1..24 {
                primary
                    .guest_write(child, volume_page(child, number), u64::from(number))
                    .await;
                if number % 4 == 0 {
                    assert!(primary.guest_sync(child).await);
                }
            }
            assert!(primary.guest_sync(child).await);
            assert_eq!(read_word(&primary, inherited).await, parent_value);
            assert_eq!(read_word(&primary, child_page).await, child_value);
            assert!(primary.incidents().is_empty());

            drop(primary);
            drop(runtimes);
            for root in roots {
                std::fs::remove_dir_all(root).expect("cleanup root");
            }
        })
        .await
        .expect("fork read-write regression completed");
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn persistent_guest_page_read_refaults_after_backing_eviction() {
    support::local(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (_addresses, roots, _gcs, mut runtimes) =
                durable_cluster("persistent-page-refault", 1).await;
            let primary = runtimes[0].take().expect("primary");
            let config = VolumeConfig::data(4);
            let evicted = page(1, 0);
            let replacement = page(1, 1);
            let expected = 0x1234_5678_9abc_def0;

            primary.create_volume(VOLUME, config).await;
            primary.guest_write(VOLUME, evicted, expected).await;
            assert!(primary.guest_sync(VOLUME).await);

            // With a one-page cache, faulting this page must evict and punch
            // the durable first page's backing before the persistent access.
            primary
                .guest_write(VOLUME, replacement, 0xfedc_ba98_7654_3210)
                .await;

            let guest = primary.guest_access(VOLUME);
            let bytes = tokio::task::spawn_blocking(move || {
                let operation = guest.try_begin().expect("guest operation starts");
                operation.read_page(evicted)
            })
            .await
            .expect("guest read worker");
            assert_eq!(
                u64::from_ne_bytes(bytes[..8].try_into().expect("word")),
                expected
            );
            assert!(primary.incidents().is_empty());

            drop(primary);
            drop(runtimes);
            for root in roots {
                std::fs::remove_dir_all(root).expect("cleanup root");
            }
        })
        .await
        .expect("persistent guest page refault completed");
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn four_volumes_complete_concurrent_cold_faults_with_balanced_delivery() {
    support::local(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (_addresses, roots, _gcs, mut runtimes) =
                durable_cluster("fault-progress", 2_048).await;
            let primary = Arc::new(runtimes[0].take().expect("primary"));
            let config = VolumeConfig::data(256);
            for number in 1..=4 {
                primary.create_volume(VolumeId(number), config).await;
            }

            let mut workers = tokio::task::JoinSet::new();
            for number in 1..=4 {
                let runtime = Arc::clone(&primary);
                workers.spawn(async move {
                    let volume = VolumeId(number);
                    for page_number in 0..256 {
                        let page = volume_page(volume, page_number);
                        let value = (number << 32) | u64::from(page_number);
                        runtime.guest_write(volume, page, value).await;
                        assert_eq!(read_word(&runtime, page).await, value);
                    }
                });
            }
            while let Some(result) = workers.join_next().await {
                result.expect("cold-fault worker");
            }

            let reader = primary.fault_reader_metrics();
            assert_eq!(reader.readers_started, 4);
            assert_eq!(reader.readers_exited, 0);
            assert_eq!(reader.events_read, reader.events_injected);
            assert_eq!(reader.terminal_errors, 0);
            assert_eq!(reader.injection_failures, 0);
            assert!(primary.incidents().is_empty());

            drop(primary);
            drop(runtimes);
            for root in roots {
                std::fs::remove_dir_all(root).expect("cleanup root");
            }
        })
        .await
        .expect("concurrent cold faults completed");
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn independently_snapshotted_volumes_recover_their_own_state() {
    support::local(async {
        let (addresses, roots, gcs, mut runtimes) = durable_cluster("checkpoint", 64).await;
        let store = gcs.store.clone();
        let memory = VolumeId(1);
        let data_a = VolumeId(2);
        let data_b = VolumeId(3);
        let memory_config = VolumeConfig::memory(16);
        let data_config = VolumeConfig::data(16);
        let primary = runtimes[0].take().expect("primary");
        primary.create_volume(memory, memory_config).await;
        primary.create_volume(data_a, data_config).await;
        primary.create_volume(data_b, data_config).await;
        primary
            .guest_write(data_a, volume_page(data_a, 3), 0x1111)
            .await;
        primary
            .guest_write(data_b, volume_page(data_b, 7), 0x2222)
            .await;
        primary
            .guest_write(memory, volume_page(memory, 5), 0xAAAA)
            .await;
        assert!(primary.guest_sync(data_a).await);
        assert!(primary.guest_sync(data_b).await);
        let epochs = futures_util::future::join_all(
            [memory, data_a, data_b]
                .into_iter()
                .map(|volume| primary.checkpoint(volume)),
        )
        .await;
        assert_eq!(epochs, vec![1, 1, 1]);
        let vmstate = primary.guest_applied(memory);
        drop(primary);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (recovered, immediate) = Runtime::recover(
            &support::three_host_runtime_config(0, roots[0].clone(), addresses),
            store,
            &BTreeMap::from([
                (memory, memory_config),
                (data_a, data_config),
                (data_b, data_config),
            ]),
        )
        .await;
        assert!(immediate.is_empty());
        assert_eq!(
            recovered.wait_recovered(memory).await,
            Verdict::Resume {
                epoch: blockd_core::types::Epoch(1),
                vmstate,
            }
        );
        assert_eq!(recovered.wait_recovered(data_a).await, Verdict::ColdBoot);
        assert_eq!(recovered.wait_recovered(data_b).await, Verdict::ColdBoot);
        assert_eq!(read_word(&recovered, volume_page(data_a, 3)).await, 0x1111);
        assert_eq!(read_word(&recovered, volume_page(data_b, 7)).await, 0x2222);
        assert_eq!(read_word(&recovered, volume_page(memory, 5)).await, 0xAAAA);
        assert!(recovered.incidents().is_empty());

        drop(recovered);
        drop(runtimes);
        for root in roots {
            std::fs::remove_dir_all(root).expect("cleanup root");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn synced_data_recovers_while_unsnapshotted_memory_is_discarded() {
    support::local(async {
        let (addresses, roots, gcs, mut runtimes) = durable_cluster("cold-boot", 64).await;
        let store = gcs.store.clone();
        let memory = VolumeId(1);
        let data_a = VolumeId(2);
        let data_b = VolumeId(3);
        let memory_config = VolumeConfig::memory(16);
        let data_config = VolumeConfig::data(16);
        let primary = runtimes[0].take().expect("primary");
        primary.create_volume(memory, memory_config).await;
        primary.create_volume(data_a, data_config).await;
        primary.create_volume(data_b, data_config).await;
        primary
            .guest_write(data_a, volume_page(data_a, 2), 0x1234)
            .await;
        primary
            .guest_write(data_b, volume_page(data_b, 9), 0x5678)
            .await;
        primary
            .guest_write(memory, volume_page(memory, 4), 0xDEAD)
            .await;
        assert!(primary.guest_sync(data_a).await);
        assert!(primary.guest_sync(data_b).await);
        drop(primary);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (recovered, immediate) = Runtime::recover(
            &support::three_host_runtime_config(0, roots[0].clone(), addresses),
            store,
            &BTreeMap::from([
                (memory, memory_config),
                (data_a, data_config),
                (data_b, data_config),
            ]),
        )
        .await;
        assert!(immediate.is_empty());
        assert_eq!(recovered.wait_recovered(memory).await, Verdict::ColdBoot);
        assert_eq!(recovered.wait_recovered(data_a).await, Verdict::ColdBoot);
        assert_eq!(recovered.wait_recovered(data_b).await, Verdict::ColdBoot);
        assert_eq!(read_word(&recovered, volume_page(data_a, 2)).await, 0x1234);
        assert_eq!(read_word(&recovered, volume_page(data_b, 9)).await, 0x5678);
        assert!(
            recovered
                .guest_read(memory, volume_page(memory, 4))
                .await
                .iter()
                .all(|byte| *byte == 0)
        );
        assert!(recovered.incidents().is_empty());

        drop(recovered);
        drop(runtimes);
        for root in roots {
            std::fs::remove_dir_all(root).expect("cleanup root");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn durable_eviction_bounds_residency_and_refaults_exact_disk_bytes() {
    support::local(async {
        let (_addresses, roots, _store, runtimes) = durable_cluster("eviction", 8).await;
        let config = VolumeConfig::data(32);
        let primary = runtimes[0].as_ref().expect("primary");
        primary.create_volume(VOLUME, config).await;
        for number in 0..32 {
            primary
                .guest_write(VOLUME, page(1, number), 0x1000 + u64::from(number))
                .await;
            if number % 8 == 7 {
                assert!(primary.guest_sync(VOLUME).await);
            }
        }
        let resident = primary.guest_resident_bytes(VOLUME);
        assert!(
            resident <= 16 * page_size(),
            "eviction did not bound physical memory: {resident} bytes"
        );
        for number in 0..32 {
            assert_eq!(
                read_word(primary, page(1, number)).await,
                0x1000 + u64::from(number)
            );
        }
        assert!(primary.incidents().is_empty());

        drop(runtimes);
        for root in roots {
            std::fs::remove_dir_all(root).expect("cleanup root");
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn peer_commit_recovers_a_deleted_primary_root_then_publishes_and_unlinks() {
    support::local(async {
    let addresses = [
        support::free_addr(),
        support::free_addr(),
        support::free_addr(),
    ];
    let roots = [
        support::temp_root("replica-a"),
        support::temp_root("replica-b"),
        support::temp_root("replica-c"),
    ];
    let gcs = support::test_gcs("replica-recover").await;
    let store = gcs.store.clone();
    let a_config = support::three_host_runtime_config(0, roots[0].clone(), addresses);
    let a = Runtime::new(&a_config, store.clone()).await;
    let b = Runtime::new(
        &support::three_host_runtime_config(1, roots[1].clone(), addresses),
        store.clone(),
    ).await;
    let c = Runtime::new(
        &support::three_host_runtime_config(2, roots[2].clone(), addresses),
        store.clone(),
    ).await;
    for runtime in [&a, &b, &c] {
        support::wait_for_peer_membership(runtime, 2).await;
    }
    let config = VolumeConfig {
        kind: blockd_core::journal::VolumeKind::Data,
        pages: 8,
    };
    a.create_volume(VOLUME, config).await;
    let (head_version, head_bytes) = store
        .clone()
        .get(layout::head_key(VOLUME))
        .await
        .expect("store available")
        .expect("head exists");
    let head = HeadRecord::decode(VOLUME, &head_bytes).expect("head valid");
    let active = head.stash.expect("stash").active_peer;

    gcs.fake.outage.store(true, Ordering::SeqCst);
    let page = PageId {
        volume: VOLUME,
        page: PageNo(3),
    };
    a.guest_write(VOLUME, page, 0x1122_3344_5566_7788).await;
    assert!(a.guest_sync(VOLUME).await);

    drop(a);
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::remove_dir_all(&roots[0]).expect("delete primary durable root");
    let active_root = &roots[usize::from(active.0)];
    let spool_paths = spool_files(active_root, HostId(0), VOLUME);
    assert!(!spool_paths.is_empty(), "peer spool exists");
    let mut spool = Vec::new();
    for (_, spool_path) in spool_paths {
        spool.extend(std::fs::read(spool_path).expect("read peer spool"));
    }
    let export = export_replica_recovery(
        HostId(0),
        VOLUME,
        head_version,
        &head,
        &[ReplicaResidue {
            peer: active,
            assignment_epoch: 1,
            bytes: &spool,
        }],
        &BTreeMap::new(),
    )
    .expect("peer residue is a complete recovery point");
    gcs.fake.outage.store(false, Ordering::SeqCst);
    install_replica_recovery(
        &roots[0],
        store.clone(),
        HostId(0),
        VOLUME,
        head_version,
        &export,
    )
    .await
    .expect("fenced recovery promotion");

    let (recovered, verdicts) = Runtime::recover(
        &support::three_host_runtime_config(0, roots[0].clone(), addresses),
        store.clone(),
        &BTreeMap::from([(VOLUME, config)]),
    ).await;
    assert!(
        verdicts.is_empty(),
        "backed recovery waits for head refresh"
    );
    let _ = recovered.wait_recovered(VOLUME).await;
    let bytes = recovered.guest_read(VOLUME, page).await;
    assert_eq!(
        u64::from_ne_bytes(bytes[..8].try_into().expect("word")),
        0x1122_3344_5566_7788
    );

    // File removal precedes the completion event that records the unlink.
    // Wait for both observable sides of the asynchronous release.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let active_counters = if active == HostId(1) {
            b.counters()
        } else {
            c.counters()
        };
        if spool_files(active_root, HostId(0), VOLUME).is_empty()
            && active_counters.replica_unlinks > 0
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "peer spool was not released: passive={:?} passive_counters={:?} passive_connections={:?} passive_dropped={} spools={:?} recovered_replica={:?} recovered_counters={:?} connections={:?} dropped={} incidents={:?}",
            if active == HostId(1) {
                b.replica_spool_metrics()
            } else {
                c.replica_spool_metrics()
            },
            if active == HostId(1) {
                b.counters()
            } else {
                c.counters()
            },
            if active == HostId(1) {
                b.peer_connections()
            } else {
                c.peer_connections()
            },
            if active == HostId(1) {
                b.peer_dropped_sends()
            } else {
                c.peer_dropped_sends()
            },
            spool_files(active_root, HostId(0), VOLUME),
            recovered.replica_metrics(),
            recovered.counters(),
            recovered.peer_connections(),
            recovered.peer_dropped_sends(),
            recovered.incidents(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let active_counters = if active == HostId(1) {
        b.counters()
    } else {
        c.counters()
    };
    assert!(active_counters.replica_unlinks > 0);
    assert!(recovered.incidents().is_empty());

    drop(recovered);
    drop(b);
    drop(c);
    for root in roots {
        std::fs::remove_dir_all(root).expect("cleanup root");
    }
    }).await;
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn failed_active_peer_seeds_only_replacement_and_recovery_uses_replacement() {
    support::local(async {
        let addresses = [
            support::free_addr(),
            support::free_addr(),
            support::free_addr(),
        ];
        let roots = [
            support::temp_root("replica-replace-a"),
            support::temp_root("replica-replace-b"),
            support::temp_root("replica-replace-c"),
        ];
        let gcs = support::test_gcs("replica-replace").await;
        let store = gcs.store.clone();
        let mut runtimes: Vec<Option<Runtime>> = Vec::new();
        for host in 0..3 {
            runtimes.push(Some(
                Runtime::new(
                    &support::three_host_runtime_config(
                        host,
                        roots[usize::from(host)].clone(),
                        addresses,
                    ),
                    store.clone(),
                )
                .await,
            ));
        }
        let config = VolumeConfig {
            kind: blockd_core::journal::VolumeKind::Data,
            pages: 8,
        };
        for runtime in &runtimes {
            support::wait_for_peer_membership(runtime.as_ref().expect("runtime"), 2).await;
        }
        let primary = runtimes[0].as_ref().expect("primary");
        primary.create_volume(VOLUME, config).await;
        let (_, initial_head_bytes) = store
            .clone()
            .get(layout::head_key(VOLUME))
            .await
            .expect("head get")
            .expect("head");
        let initial_head = HeadRecord::decode(VOLUME, &initial_head_bytes).expect("valid head");
        let failed_peer = initial_head.stash.expect("stash").active_peer;
        assert_ne!(failed_peer, HostId(0));

        gcs.fake.data_outage.store(true, Ordering::SeqCst);
        let page = PageId {
            volume: VOLUME,
            page: PageNo(4),
        };
        for value in 1..=3 {
            primary.guest_write(VOLUME, page, value).await;
            assert!(primary.guest_sync(VOLUME).await);
        }

        drop(runtimes[usize::from(failed_peer.0)].take());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let primary = runtimes[0].as_ref().expect("primary remains");
        primary.guest_write(VOLUME, page, 4).await;
        assert!(primary.guest_sync(VOLUME).await);
        let replacement = [HostId(1), HostId(2)]
            .into_iter()
            .find(|peer| *peer != failed_peer)
            .expect("one replacement candidate");
        let replacement_root = &roots[usize::from(replacement.0)];
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let counters = primary.counters();
            if counters.replica_replacement_bytes > 0
                && !spool_files(replacement_root, HostId(0), VOLUME).is_empty()
            {
                assert_eq!(counters.replica_nonactive_bytes, 0);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "replacement did not become durable during the store outage"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        drop(runtimes[0].take());
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::remove_dir_all(&roots[0]).expect("delete primary durable root");
        let spool_paths = spool_files(replacement_root, HostId(0), VOLUME);
        assert!(
            !spool_paths.is_empty(),
            "replacement retained recovery residue"
        );
        let mut spool = Vec::new();
        for (_, path) in spool_paths {
            spool.extend(std::fs::read(path).expect("read replacement spool"));
        }
        gcs.fake.data_outage.store(false, Ordering::SeqCst);
        let (head_version, head_bytes) = store
            .clone()
            .get(layout::head_key(VOLUME))
            .await
            .expect("head get")
            .expect("head");
        let head = HeadRecord::decode(VOLUME, &head_bytes).expect("valid stale assignment head");
        let export = export_replica_recovery(
            HostId(0),
            VOLUME,
            head_version,
            &head,
            &[ReplicaResidue {
                peer: replacement,
                assignment_epoch: 2,
                bytes: &spool,
            }],
            &BTreeMap::new(),
        )
        .expect("current-fence replacement survives unpublished assignment");

        install_replica_recovery(
            &roots[0],
            store.clone(),
            HostId(0),
            VOLUME,
            head_version,
            &export,
        )
        .await
        .expect("install fenced replacement recovery");
        let (recovered, verdicts) = Runtime::recover(
            &support::three_host_runtime_config(0, roots[0].clone(), addresses),
            store.clone(),
            &BTreeMap::from([(VOLUME, config)]),
        )
        .await;
        assert!(verdicts.is_empty());
        let _ = recovered.wait_recovered(VOLUME).await;
        let bytes = recovered.guest_read(VOLUME, page).await;
        assert_eq!(u64::from_ne_bytes(bytes[..8].try_into().expect("word")), 4);

        // File removal precedes the completion event that records the unlink.
        // Wait for both observable sides of the asynchronous release.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let replacement_counters = runtimes[usize::from(replacement.0)]
                .as_ref()
                .expect("replacement runtime")
                .counters();
            let release_head = store
                .clone()
                .get(layout::head_key(VOLUME))
                .await
                .expect("release head get")
                .and_then(|(_, bytes)| HeadRecord::decode(VOLUME, &bytes).ok());
            let release_cleared = release_head.as_ref().is_some_and(|head| {
                    head.retired_stashes.iter().all(|retired| {
                        (retired.peer, retired.assignment_epoch)
                            != (replacement, export.assignment_epoch)
                    })
                });
            if spool_files(replacement_root, HostId(0), VOLUME).is_empty()
                && replacement_counters.replica_unlinks > 0
                && release_cleared
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "replacement release did not finish: spools={:?} replacement_counters={:?} head={:?} recovered_counters={:?} recovered_replica={:?} connections={:?} dropped={} incidents={:?}",
                spool_files(replacement_root, HostId(0), VOLUME),
                replacement_counters,
                release_head,
                recovered.counters(),
                recovered.replica_metrics(),
                recovered.peer_connections(),
                recovered.peer_dropped_sends(),
                recovered.incidents(),
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let replacement_counters = runtimes[usize::from(replacement.0)]
            .as_ref()
            .expect("replacement runtime")
            .counters();
        assert!(replacement_counters.replica_unlinks > 0);
        let (_, released_head_bytes) = store
            .clone()
            .get(layout::head_key(VOLUME))
            .await
            .expect("released head get")
            .expect("released head");
        let released_head =
            HeadRecord::decode(VOLUME, &released_head_bytes).expect("valid released head");
        assert!(released_head.retired_stashes.iter().all(|retired| {
            (retired.peer, retired.assignment_epoch) != (replacement, export.assignment_epoch)
        }));
        assert!(recovered.incidents().is_empty());

        drop(recovered);
        drop(runtimes);
        for root in roots {
            std::fs::remove_dir_all(root).expect("cleanup root");
        }
    })
    .await;
}
