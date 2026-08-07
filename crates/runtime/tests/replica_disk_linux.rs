//! Abrupt process death against a real filesystem spool, followed by the real
//! daemon restart scanner. This test does not require guest memory/userfaultfd.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use blockd_core::daemon::{Daemon, DaemonConfig, ReplicaPlacementConfig};
use blockd_core::journal::{DurabilityMode, JournalRecord, RecordKind, VsetConfig};
use blockd_core::layout;
use blockd_core::placement::{PeerCandidate, rank_stash_candidates};
use blockd_core::replica_spool::{seal_replica_artifact, seal_replica_commit};
use blockd_core::seam::{Effect, ReplicaArtifact, ReplicaCommitInfo};
use blockd_core::segment::SegmentBuilder;
use blockd_core::types::{
    Gen, HostId, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId, page_size,
};

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
            disk_volumes: 1,
            pages_per_volume: 8,
            durability: DurabilityMode::PeerStashed,
        },
        seq: info.seq,
        fence: info.writer_fence,
        kind: RecordKind::Commit,
        capture_seq: 12,
        sync_covered_through: info.sync_covered_through,
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
    let daemon_config = DaemonConfig {
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
        }),
    };
    let (_, verdicts, effects) = Daemon::recover(
        daemon_config.clone(),
        [(name.as_str(), bytes.as_slice())].into_iter(),
    );
    assert!(verdicts.is_empty());
    let truncate = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaTruncate { len, .. } => Some(*len),
            _ => None,
        })
        .expect("restart rejects and truncates the torn footer");
    assert_eq!(truncate, artifact.len() as u64);

    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open spool for truncation");
    file.set_len(truncate).expect("truncate invalid tail");
    file.sync_all().expect("fsync truncation");
    let repaired = std::fs::read(&path).expect("repaired spool");
    let (_, _, effects) = Daemon::recover(
        daemon_config,
        [(name.as_str(), repaired.as_slice())].into_iter(),
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::ReplicaTruncate { .. }))
    );

    std::fs::remove_dir_all(root).expect("cleanup root");
}
