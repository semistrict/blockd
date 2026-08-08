//! Real three-runtime peer-stash recovery: TCP, durable filesystem spools,
//! object-store outage, loss of the primary root, peer export, restart,
//! publication, and whole-spool unlink without cleanup rewriting.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::head::HeadRecord;
use blockd_core::journal::{DurabilityMode, VsetConfig};
use blockd_core::layout;
use blockd_core::replica_recovery::{ReplicaResidue, export_replica_recovery};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId};
use blockd_runtime::{Runtime, S3Store, install_replica_recovery};

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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn peer_commit_recovers_a_deleted_primary_root_then_publishes_and_unlinks() {
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
    let store = Arc::new(S3Store::new());
    let a_config = support::three_host_runtime_config(0, roots[0].clone(), addresses);
    let a = Runtime::new(&a_config, store.clone());
    let b = Runtime::new(
        &support::three_host_runtime_config(1, roots[1].clone(), addresses),
        store.clone(),
    );
    let c = Runtime::new(
        &support::three_host_runtime_config(2, roots[2].clone(), addresses),
        store.clone(),
    );
    let config = VsetConfig {
        kind: blockd_core::journal::VsetKind::Compute,
        disk_volumes: 1,
        pages_per_volume: 8,
        durability: DurabilityMode::PeerStashed,
    };
    a.create_vset(VSET, config);
    let (head_version, head_bytes) = store
        .get(&layout::head_key(VSET))
        .expect("store available")
        .expect("head exists");
    let head = HeadRecord::decode(VSET, &head_bytes).expect("head valid");
    let active = head.stash.expect("stash").active_peer;

    store.set_outage(true);
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(3),
    };
    a.guest_write(VSET, page, 0x1122_3344_5566_7788);
    assert!(a.guest_sync(VSET, VolumeIdx(1)));

    drop(a);
    std::thread::sleep(Duration::from_millis(100));
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
        &head,
        &[ReplicaResidue {
            peer: active,
            assignment_epoch: 1,
            bytes: &spool,
        }],
        &BTreeMap::new(),
    )
    .expect("peer residue is a complete recovery point");
    store.set_outage(false);
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
    );
    assert!(
        verdicts.is_empty(),
        "backed recovery waits for head refresh"
    );
    let _ = recovered.wait_recovered(VSET);
    let bytes = recovered.guest_read(VSET, page);
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
        assert!(Instant::now() < deadline, "peer spool was not released");
        std::thread::sleep(Duration::from_millis(20));
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
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failed_active_peer_seeds_only_replacement_and_recovery_uses_replacement() {
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
    let store = Arc::new(S3Store::new());
    let mut runtimes: Vec<Option<Runtime>> = (0..3)
        .map(|host| {
            Some(Runtime::new(
                &support::three_host_runtime_config(
                    host,
                    roots[usize::from(host)].clone(),
                    addresses,
                ),
                store.clone(),
            ))
        })
        .collect();
    let config = VsetConfig {
        kind: blockd_core::journal::VsetKind::Compute,
        disk_volumes: 1,
        pages_per_volume: 8,
        durability: DurabilityMode::PeerStashed,
    };
    let primary = runtimes[0].as_ref().expect("primary");
    primary.create_vset(VSET, config);
    let initial_head = HeadRecord::decode(
        VSET,
        &store
            .get(&layout::head_key(VSET))
            .expect("head get")
            .expect("head")
            .1,
    )
    .expect("valid head");
    let failed_peer = initial_head.stash.expect("stash").active_peer;
    assert_ne!(failed_peer, HostId(0));

    store.set_data_outage(true);
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(4),
    };
    for value in 1..=3 {
        primary.guest_write(VSET, page, value);
        assert!(primary.guest_sync(VSET, VolumeIdx(1)));
    }

    drop(runtimes[usize::from(failed_peer.0)].take());
    std::thread::sleep(Duration::from_millis(100));
    let primary = runtimes[0].as_ref().expect("primary remains");
    primary.guest_write(VSET, page, 4);
    assert!(primary.guest_sync(VSET, VolumeIdx(1)));

    let (head_version, head_bytes) = store
        .get(&layout::head_key(VSET))
        .expect("head get")
        .expect("head");
    let head = HeadRecord::decode(VSET, &head_bytes).expect("valid replacement head");
    let assignment = head.stash.expect("replacement assignment");
    let replacement = assignment.active_peer;
    assert_ne!(replacement, failed_peer);
    assert_eq!(assignment.transition_peer, None);
    assert_eq!(assignment.assignment_epoch, 2);
    let primary_counters = primary.counters();
    assert!(primary_counters.replica_replacement_bytes > 0);
    assert_eq!(primary_counters.replica_nonactive_bytes, 0);

    drop(runtimes[0].take());
    std::thread::sleep(Duration::from_millis(100));
    std::fs::remove_dir_all(&roots[0]).expect("delete primary durable root");
    let replacement_root = &roots[usize::from(replacement.0)];
    let spool_paths = spool_files(replacement_root, HostId(0), VSET);
    assert!(
        !spool_paths.is_empty(),
        "replacement retained recovery residue"
    );
    let mut spool = Vec::new();
    for (_, path) in spool_paths {
        spool.extend(std::fs::read(path).expect("read replacement spool"));
    }
    let export = export_replica_recovery(
        HostId(0),
        VSET,
        &head,
        &[ReplicaResidue {
            peer: replacement,
            assignment_epoch: assignment.active_assignment_epoch,
            bytes: &spool,
        }],
        &BTreeMap::new(),
    )
    .expect("replacement has a complete acknowledged recovery point");

    store.set_data_outage(false);
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
    );
    assert!(verdicts.is_empty());
    let _ = recovered.wait_recovered(VSET);
    let bytes = recovered.guest_read(VSET, page);
    assert_eq!(u64::from_ne_bytes(bytes[..8].try_into().expect("word")), 4);

    // File removal precedes the completion event that records the unlink.
    // Wait for both observable sides of the asynchronous release.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let replacement_counters = runtimes[usize::from(replacement.0)]
            .as_ref()
            .expect("replacement runtime")
            .counters();
        if spool_files(replacement_root, HostId(0), VSET).is_empty()
            && replacement_counters.replica_unlinks > 0
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replacement spool was not released"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let replacement_counters = runtimes[usize::from(replacement.0)]
        .as_ref()
        .expect("replacement runtime")
        .counters();
    assert!(replacement_counters.replica_unlinks > 0);
    assert_eq!(replacement_counters.replica_cleanup_rewrite_bytes, 0);
    assert!(recovered.incidents().is_empty());

    drop(recovered);
    drop(runtimes);
    for root in roots {
        std::fs::remove_dir_all(root).expect("cleanup root");
    }
}
