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
use blockd_core::journal::VsetConfig;
use blockd_core::layout;
use blockd_core::protocol::Verdict;
use blockd_core::replica_recovery::{ReplicaResidue, export_replica_recovery};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId};
use blockd_hostmem::page_size;
use blockd_runtime::{ObjectStore, Runtime, install_replica_recovery};

mod support;

const VSET: VsetId = VsetId(1);

fn spool_files(root: &Path, source: HostId, vset: VsetId) -> Vec<(u64, PathBuf)> {
    fn visit(
        base: &Path,
        directory: &Path,
        source: HostId,
        vset: VsetId,
        found: &mut Vec<(u64, PathBuf)>,
    ) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                visit(base, &path, source, vset, found);
            } else if let Ok(relative) = path.strip_prefix(base)
                && let Some(layout::BlobName::ReplicaSpool {
                    source: got_source,
                    vset: got_vset,
                    generation,
                    ..
                }) = layout::parse_blob(relative.to_string_lossy().as_ref())
                && (got_source, got_vset) == (source, vset)
            {
                found.push((generation, path));
            }
        }
    }
    let mut found = Vec::new();
    visit(root, root, source, vset, &mut found);
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
    vset_page(VSET, volume, number)
}

fn vset_page(vset: VsetId, volume: u8, number: u32) -> PageId {
    PageId {
        volume: VolumeId {
            vset,
            idx: VolumeIdx(volume),
        },
        page: PageNo(number),
    }
}

async fn read_word(runtime: &Runtime, page: PageId) -> u64 {
    let bytes = runtime.guest_read(page.volume.vset, page).await;
    u64::from_ne_bytes(bytes[..8].try_into().expect("word"))
}

#[tokio::test(flavor = "current_thread")]
async fn forked_shared_page_read_then_write_diverges_and_refaults() {
    support::local(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (_addresses, roots, _gcs, mut runtimes) = durable_cluster("fork-write", 8).await;
            let primary = runtimes[0].take().expect("primary");
            let config = VsetConfig::compute(1, 32);
            let child = VsetId(2);
            let inherited = vset_page(VSET, 1, 0);
            let child_page = vset_page(child, 1, 0);
            let parent_value = 0x1111_2222_3333_4444;
            let child_value = 0xaaaa_bbbb_cccc_dddd;

            primary.create_vset(VSET, config).await;
            primary.guest_write(VSET, inherited, parent_value).await;
            assert_eq!(primary.checkpoint(VSET).await, 1);
            primary.keep_base(VSET, 700).await;
            assert!(matches!(
                primary.fork_vset(child, config, 700).await,
                Verdict::Resume { .. }
            ));

            assert_eq!(read_word(&primary, child_page).await, parent_value);
            primary.guest_write(child, child_page, child_value).await;
            assert_eq!(read_word(&primary, inherited).await, parent_value);
            assert_eq!(read_word(&primary, child_page).await, child_value);
            assert_eq!(primary.checkpoint(child).await, 1);

            for number in 1..24 {
                primary
                    .guest_write(child, vset_page(child, 1, number), u64::from(number))
                    .await;
                if number % 4 == 0 {
                    assert!(primary.guest_sync(child, VolumeIdx(1)).await);
                }
            }
            assert!(primary.guest_sync(child, VolumeIdx(1)).await);
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
            let config = VsetConfig::compute(1, 4);
            let evicted = page(1, 0);
            let replacement = page(1, 1);
            let expected = 0x1234_5678_9abc_def0;

            primary.create_vset(VSET, config).await;
            primary.guest_write(VSET, evicted, expected).await;
            assert!(primary.guest_sync(VSET, VolumeIdx(1)).await);

            // With a one-page cache, faulting this page must evict and punch
            // the durable first page's backing before the persistent access.
            primary
                .guest_write(VSET, replacement, 0xfedc_ba98_7654_3210)
                .await;

            let guest = primary.guest_access(VSET);
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
async fn four_vsets_complete_concurrent_cold_faults_with_balanced_delivery() {
    support::local(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let (_addresses, roots, _gcs, mut runtimes) =
                durable_cluster("fault-progress", 2_048).await;
            let primary = Arc::new(runtimes[0].take().expect("primary"));
            let config = VsetConfig::compute(1, 256);
            for number in 1..=4 {
                primary.create_vset(VsetId(number), config).await;
            }

            let mut workers = tokio::task::JoinSet::new();
            for number in 1..=4 {
                let runtime = Arc::clone(&primary);
                workers.spawn(async move {
                    let vset = VsetId(number);
                    for page_number in 0..256 {
                        let page = vset_page(vset, 1, page_number);
                        let value = (number << 32) | u64::from(page_number);
                        runtime.guest_write(vset, page, value).await;
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
async fn durable_checkpoint_crash_resumes_memory_and_disks_byte_exactly() {
    support::local(async {
        let (addresses, roots, gcs, mut runtimes) = durable_cluster("checkpoint", 64).await;
        let store = gcs.store.clone();
        let config = VsetConfig::compute(2, 16);
        let primary = runtimes[0].take().expect("primary");
        primary.create_vset(VSET, config).await;
        primary.guest_write(VSET, page(1, 3), 0x1111).await;
        primary.guest_write(VSET, page(2, 7), 0x2222).await;
        primary.guest_write(VSET, page(0, 5), 0xAAAA).await;
        assert!(primary.guest_sync(VSET, VolumeIdx(1)).await);
        assert_eq!(primary.checkpoint(VSET).await, 1);
        let vmstate = primary.guest_applied(VSET);
        drop(primary);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (recovered, immediate) = Runtime::recover(
            &support::three_host_runtime_config(0, roots[0].clone(), addresses),
            store,
            &BTreeMap::from([(VSET, config)]),
        )
        .await;
        assert!(immediate.is_empty());
        assert_eq!(
            recovered.wait_recovered(VSET).await,
            Verdict::Resume {
                epoch: blockd_core::types::Epoch(1),
                vmstate,
            }
        );
        assert_eq!(read_word(&recovered, page(1, 3)).await, 0x1111);
        assert_eq!(read_word(&recovered, page(2, 7)).await, 0x2222);
        assert_eq!(read_word(&recovered, page(0, 5)).await, 0xAAAA);
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
async fn durable_sync_crash_cold_boots_disks_and_discards_memory() {
    support::local(async {
        let (addresses, roots, gcs, mut runtimes) = durable_cluster("cold-boot", 64).await;
        let store = gcs.store.clone();
        let config = VsetConfig::compute(2, 16);
        let primary = runtimes[0].take().expect("primary");
        primary.create_vset(VSET, config).await;
        primary.guest_write(VSET, page(1, 2), 0x1234).await;
        primary.guest_write(VSET, page(2, 9), 0x5678).await;
        primary.guest_write(VSET, page(0, 4), 0xDEAD).await;
        assert!(primary.guest_sync(VSET, VolumeIdx(1)).await);
        assert!(primary.guest_sync(VSET, VolumeIdx(2)).await);
        drop(primary);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (recovered, immediate) = Runtime::recover(
            &support::three_host_runtime_config(0, roots[0].clone(), addresses),
            store,
            &BTreeMap::from([(VSET, config)]),
        )
        .await;
        assert!(immediate.is_empty());
        assert_eq!(recovered.wait_recovered(VSET).await, Verdict::ColdBoot);
        assert_eq!(read_word(&recovered, page(1, 2)).await, 0x1234);
        assert_eq!(read_word(&recovered, page(2, 9)).await, 0x5678);
        assert!(
            recovered
                .guest_read(VSET, page(0, 4))
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
        let config = VsetConfig::compute(1, 32);
        let primary = runtimes[0].as_ref().expect("primary");
        primary.create_vset(VSET, config).await;
        for number in 0..32 {
            primary
                .guest_write(VSET, page(1, number), 0x1000 + u64::from(number))
                .await;
            if number % 8 == 7 {
                assert!(primary.guest_sync(VSET, VolumeIdx(1)).await);
            }
        }
        let resident = primary.guest_resident_bytes(VSET);
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
    let config = VsetConfig {
        kind: blockd_core::journal::VsetKind::Compute,
        disk_volumes: 1,
        pages_per_volume: 8,
    };
    a.create_vset(VSET, config).await;
    let (head_version, head_bytes) = store
        .clone()
        .get(layout::head_key(VSET))
        .await
        .expect("store available")
        .expect("head exists");
    let head = HeadRecord::decode(VSET, &head_bytes).expect("head valid");
    let active = head.stash.expect("stash").active_peer;

    gcs.fake.outage.store(true, Ordering::SeqCst);
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(3),
    };
    a.guest_write(VSET, page, 0x1122_3344_5566_7788).await;
    assert!(a.guest_sync(VSET, VolumeIdx(1)).await);

    drop(a);
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::remove_dir_all(&roots[0]).expect("delete primary durable root");
    let active_root = &roots[usize::from(active.0)];
    let spool_paths = spool_files(active_root, HostId(0), VSET);
    assert!(!spool_paths.is_empty(), "peer spool exists");
    let mut spool = Vec::new();
    for (_, spool_path) in spool_paths {
        spool.extend(std::fs::read(spool_path).expect("read peer spool"));
    }
    let export = export_replica_recovery(
        HostId(0),
        VSET,
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
        VSET,
        head_version,
        &export,
    )
    .await
    .expect("fenced recovery promotion");

    let (recovered, verdicts) = Runtime::recover(
        &support::three_host_runtime_config(0, roots[0].clone(), addresses),
        store.clone(),
        &BTreeMap::from([(VSET, config)]),
    ).await;
    assert!(
        verdicts.is_empty(),
        "backed recovery waits for head refresh"
    );
    let _ = recovered.wait_recovered(VSET).await;
    let bytes = recovered.guest_read(VSET, page).await;
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
        if spool_files(active_root, HostId(0), VSET).is_empty()
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
            spool_files(active_root, HostId(0), VSET),
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
    assert_eq!(active_counters.replica_cleanup_rewrite_bytes, 0);
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
        let config = VsetConfig {
            kind: blockd_core::journal::VsetKind::Compute,
            disk_volumes: 1,
            pages_per_volume: 8,
        };
        for runtime in &runtimes {
            support::wait_for_peer_membership(runtime.as_ref().expect("runtime"), 2).await;
        }
        let primary = runtimes[0].as_ref().expect("primary");
        primary.create_vset(VSET, config).await;
        let (_, initial_head_bytes) = store
            .clone()
            .get(layout::head_key(VSET))
            .await
            .expect("head get")
            .expect("head");
        let initial_head = HeadRecord::decode(VSET, &initial_head_bytes).expect("valid head");
        let failed_peer = initial_head.stash.expect("stash").active_peer;
        assert_ne!(failed_peer, HostId(0));

        gcs.fake.data_outage.store(true, Ordering::SeqCst);
        let page = PageId {
            volume: VolumeId {
                vset: VSET,
                idx: VolumeIdx(1),
            },
            page: PageNo(4),
        };
        for value in 1..=3 {
            primary.guest_write(VSET, page, value).await;
            assert!(primary.guest_sync(VSET, VolumeIdx(1)).await);
        }

        drop(runtimes[usize::from(failed_peer.0)].take());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let primary = runtimes[0].as_ref().expect("primary remains");
        primary.guest_write(VSET, page, 4).await;
        assert!(primary.guest_sync(VSET, VolumeIdx(1)).await);
        let replacement = [HostId(1), HostId(2)]
            .into_iter()
            .find(|peer| *peer != failed_peer)
            .expect("one replacement candidate");
        let replacement_root = &roots[usize::from(replacement.0)];
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let counters = primary.counters();
            if counters.replica_replacement_bytes > 0
                && !spool_files(replacement_root, HostId(0), VSET).is_empty()
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
        let spool_paths = spool_files(replacement_root, HostId(0), VSET);
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
            .get(layout::head_key(VSET))
            .await
            .expect("head get")
            .expect("head");
        let head = HeadRecord::decode(VSET, &head_bytes).expect("valid stale assignment head");
        let export = export_replica_recovery(
            HostId(0),
            VSET,
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
            VSET,
            head_version,
            &export,
        )
        .await
        .expect("install fenced replacement recovery");
        let (recovered, verdicts) = Runtime::recover(
            &support::three_host_runtime_config(0, roots[0].clone(), addresses),
            store.clone(),
            &BTreeMap::from([(VSET, config)]),
        )
        .await;
        assert!(verdicts.is_empty());
        let _ = recovered.wait_recovered(VSET).await;
        let bytes = recovered.guest_read(VSET, page).await;
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
                .get(layout::head_key(VSET))
                .await
                .expect("release head get")
                .and_then(|(_, bytes)| HeadRecord::decode(VSET, &bytes).ok());
            let release_cleared = release_head.as_ref().is_some_and(|head| {
                    head.retired_stashes.iter().all(|retired| {
                        (retired.peer, retired.assignment_epoch)
                            != (replacement, export.assignment_epoch)
                    })
                });
            if spool_files(replacement_root, HostId(0), VSET).is_empty()
                && replacement_counters.replica_unlinks > 0
                && release_cleared
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "replacement release did not finish: spools={:?} replacement_counters={:?} head={:?} recovered_counters={:?} recovered_replica={:?} connections={:?} dropped={} incidents={:?}",
                spool_files(replacement_root, HostId(0), VSET),
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
        assert_eq!(replacement_counters.replica_cleanup_rewrite_bytes, 0);
        let (_, released_head_bytes) = store
            .clone()
            .get(layout::head_key(VSET))
            .await
            .expect("released head get")
            .expect("released head");
        let released_head =
            HeadRecord::decode(VSET, &released_head_bytes).expect("valid released head");
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
