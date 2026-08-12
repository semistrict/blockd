//! Abrupt process death against a real filesystem spool, followed by the real
//! actor recovery scanner. This test does not require guest memory/userfaultfd.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use blockd_core::engine::{HostState, recover_local};
use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::{JournalRecord, RecordKind, VsetConfig};
use blockd_core::layout;
use blockd_core::placement::{PeerCandidate, rank_stash_candidates};
use blockd_core::protocol::{ReplicaArtifact, ReplicaCommitInfo};
use blockd_core::replica_spool::{seal_replica_artifact, seal_replica_commit};
use blockd_core::segment::SegmentBuilder;
use blockd_core::types::{
    Gen, HostId, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId, page_size,
};
use blockd_exec::Executor;
use blockd_runtime::world::FileBlobs;

const VSET: VsetId = VsetId(7);

fn fixture() -> (Vec<u8>, Vec<u8>) {
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(2),
    };
    let mut builder = SegmentBuilder::new(VSET, 4, SegId(9));
    builder.add(page, Gen(3), &vec![0xA5; page_size()]);
    let (segment, locs) = builder.finish();
    let artifact = ReplicaArtifact::Segment {
        fence: 4,
        seg: SegId(9),
    };
    let info = ReplicaCommitInfo {
        writer_fence: 4,
        seq: JournalSeq(8),
        sync_covered_through: 12,
    };
    let record = JournalRecord {
        config: VsetConfig {
            kind: blockd_core::journal::VsetKind::Compute,
            disk_volumes: 1,
            pages_per_volume: 8,
        },
        seq: info.seq,
        fence: info.writer_fence,
        kind: RecordKind::Commit,
        capture_seq: 12,
        sync_covered_through: info.sync_covered_through,
        database: blockd_core::journal::DatabaseMeta::default(),
        overlay: BTreeMap::from([(page, (Gen(3), locs[0].2))]),
        leaves: BTreeMap::new(),
        migrated_from: None,
    }
    .encode(VSET);
    (
        seal_replica_artifact(HostId(0), VSET, 1, artifact, &segment).expect("artifact"),
        seal_replica_commit(HostId(0), VSET, 1, info, &[artifact], &record).expect("commit"),
    )
}

fn sync_parent_chain(root: &Path, mut directory: &Path) {
    loop {
        std::fs::File::open(directory)
            .expect("open directory")
            .sync_all()
            .expect("sync directory");
        if directory == root {
            break;
        }
        directory = directory.parent().expect("below root");
    }
}

#[test]
fn replica_kill_child() {
    let Some(root) = std::env::var_os("BLOCKD_REPLICA_KILL_CHILD").map(PathBuf::from) else {
        return;
    };
    let (artifact, commit) = fixture();
    let name = layout::replica_spool_blob(HostId(0), VSET, 1);
    let path = root.join(name);
    let parent = path.parent().expect("parent");
    std::fs::create_dir_all(parent).expect("create spool directory");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open spool");
    file.write_all(&artifact).expect("append durable artifact");
    file.sync_all().expect("fsync artifact");
    sync_parent_chain(&root, parent);
    file.write_all(&commit[..commit.len() / 2])
        .expect("append torn footer prefix");
    file.sync_all().expect("fsync torn prefix");
    std::fs::write(root.join("ready"), b"ready").expect("ready marker");
    std::fs::File::open(root.join("ready"))
        .expect("ready marker")
        .sync_all()
        .expect("fsync ready marker");
    sync_parent_chain(&root, &root);
    thread::sleep(Duration::from_mins(1));
}

#[test]
fn abrupt_process_kill_leaves_only_a_truncatable_tail() {
    let root = std::env::temp_dir().join(format!("blockd-replica-kill-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("root");
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "replica_kill_child", "--nocapture"])
        .env("BLOCKD_REPLICA_KILL_CHILD", &root)
        .spawn()
        .expect("spawn append helper");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !root.join("ready").exists() {
        assert!(
            Instant::now() < deadline,
            "append helper did not become ready"
        );
        thread::sleep(Duration::from_millis(10));
    }
    child.kill().expect("kill append helper");
    let status = child.wait().expect("reap append helper");
    assert!(!status.success(), "helper must die abruptly");

    let name = layout::replica_spool_blob(HostId(0), VSET, 1);
    let path = root.join(&name);
    let bytes = std::fs::read(&path).expect("surviving spool");
    let (artifact, _) = fixture();
    assert!(bytes.len() > artifact.len());
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
        PeerCandidate {
            host: HostId(2),
            weight: 1,
            failure_domain: 3,
            drained: false,
        },
    ];
    let target = rank_stash_candidates(6, HostId(0), 1, VSET, &roster)[0];
    let target_domain = roster
        .iter()
        .find(|candidate| candidate.host == target)
        .expect("target")
        .failure_domain;
    let host_config = HostConfig {
        archive: blockd_core::hostmeta::ArchivePolicy::default(),
        host: target,
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 500,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 6,
            local_failure_domain: target_domain,
            roster,
            authority: None,
        }),
    };
    let blobs = Rc::new(FileBlobs::new(&root).expect("file actor world"));
    let state = Rc::new(RefCell::new(HostState::new(host_config.clone())));
    let mut executor = Executor::production();
    let verdicts = executor
        .block_on({
            let state = Rc::clone(&state);
            let blobs = Rc::clone(&blobs);
            async move { recover_local(state, blobs.as_ref()).await }
        })
        .expect("actor recovery succeeds");
    assert!(verdicts.is_empty());
    let repaired = std::fs::read(&path).expect("repaired spool");
    assert_eq!(repaired, artifact, "actor recovery did not trim torn tail");

    let second_state = Rc::new(RefCell::new(HostState::new(host_config)));
    executor
        .block_on({
            let state = Rc::clone(&second_state);
            let blobs = Rc::clone(&blobs);
            async move { recover_local(state, blobs.as_ref()).await }
        })
        .expect("second actor recovery succeeds");
    assert_eq!(
        std::fs::read(&path).expect("stable spool"),
        artifact,
        "a valid recovered spool was truncated again"
    );

    std::fs::remove_dir_all(root).expect("cleanup root");
}
