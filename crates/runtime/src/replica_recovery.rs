//! Fenced promotion of a verified passive-replica export. Verification is
//! read-only in core; this module performs the separate ownership CAS, S3
//! publication, durable quarantine write, and atomic local promotion.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blockd_core::head::{HeadRecord, ManifestPtr};
use blockd_core::journal::JournalRecord;
use blockd_core::layout::{self, BlobName};
use blockd_core::replica_recovery::{
    ReplicaExport, prepare_replica_recovery_claim, refence_replica_export,
};
use blockd_core::types::{HostId, VsetId};

use crate::store::ObjectStore;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InstalledReplicaRecovery {
    pub writer_fence: u64,
    pub head_version: u64,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum InstallReplicaRecoveryError {
    LocalTargetExists,
    HeadUnavailable,
    HeadMissing,
    HeadCorrupt,
    ClaimConflict,
    ExportCorrupt,
    PublishFailed,
    LocalIo,
}

/// Claim and install one already-verified export. `verified_head_version` must
/// be the exact fenced head version used to authorize and select that export.
/// Nothing in `target` becomes visible before both fenced head CASes and every
/// required store put succeed.
pub async fn install_replica_recovery(
    target: &Path,
    store: Arc<dyn ObjectStore>,
    claimant: HostId,
    vset: VsetId,
    verified_head_version: u64,
    export: &ReplicaExport,
) -> Result<InstalledReplicaRecovery, InstallReplicaRecoveryError> {
    if target.exists() {
        return Err(InstallReplicaRecoveryError::LocalTargetExists);
    }
    let (observed_version, observed_bytes) = store
        .clone()
        .get(layout::head_key(vset))
        .await
        .map_err(|_| InstallReplicaRecoveryError::HeadUnavailable)?
        .ok_or(InstallReplicaRecoveryError::HeadMissing)?;
    let observed = HeadRecord::decode(vset, &observed_bytes)
        .map_err(|_| InstallReplicaRecoveryError::HeadCorrupt)?;
    if observed_version != verified_head_version {
        return Err(InstallReplicaRecoveryError::ClaimConflict);
    }
    let claim = prepare_replica_recovery_claim(observed_version, &observed, claimant);
    let writer_fence = store
        .clone()
        .put_cas(
            layout::head_key(vset),
            Some(claim.expected_version),
            claim.head.encode(),
        )
        .await
        .map_err(|_| InstallReplicaRecoveryError::ClaimConflict)?;
    let export = refence_replica_export(vset, export, writer_fence)
        .map_err(|_| InstallReplicaRecoveryError::ExportCorrupt)?;
    let record_bytes = export
        .blobs
        .iter()
        .find_map(|(name, bytes)| {
            matches!(
                layout::parse_blob(name),
                Some(BlobName::Journal { fence, .. }) if fence == writer_fence
            )
            .then_some(bytes)
        })
        .ok_or(InstallReplicaRecoveryError::ExportCorrupt)?;
    let record = JournalRecord::decode(vset, record_bytes)
        .map_err(|_| InstallReplicaRecoveryError::ExportCorrupt)?;
    for (name, bytes) in &export.blobs {
        let key = match layout::parse_blob(name) {
            Some(BlobName::Segment { fence, seg, .. }) => {
                Some(layout::segment_key(vset, fence, seg))
            }
            Some(BlobName::Leaf { fence, id, .. }) => Some(layout::leaf_key(vset, fence, id)),
            _ => None,
        };
        if let Some(key) = key {
            store
                .clone()
                .put(key, bytes.clone())
                .await
                .map_err(|_| InstallReplicaRecoveryError::PublishFailed)?;
        }
    }
    store
        .clone()
        .put(
            layout::manifest_key(vset, writer_fence, record.seq),
            record_bytes.clone(),
        )
        .await
        .map_err(|_| InstallReplicaRecoveryError::PublishFailed)?;
    let mut published_head = claim.head;
    published_head.fence = writer_fence;
    published_head.manifest = Some(ManifestPtr {
        fence: writer_fence,
        seq: record.seq,
        capture_seq: record.capture_seq,
    });
    let head_version = store
        .put_cas(
            layout::head_key(vset),
            Some(writer_fence),
            published_head.encode(),
        )
        .await
        .map_err(|_| InstallReplicaRecoveryError::ClaimConflict)?;
    let target = target.to_owned();
    let blobs = export.blobs.clone();
    tokio::task::spawn_blocking(move || durable_promote(&target, vset, writer_fence, &blobs))
        .await
        .map_err(|_| InstallReplicaRecoveryError::LocalIo)??;
    Ok(InstalledReplicaRecovery {
        writer_fence,
        head_version,
    })
}

fn durable_promote(
    target: &Path,
    vset: VsetId,
    writer_fence: u64,
    blobs: &[(String, Vec<u8>)],
) -> Result<(), InstallReplicaRecoveryError> {
    let parent = target
        .parent()
        .ok_or(InstallReplicaRecoveryError::LocalIo)?;
    std::fs::create_dir_all(parent).map_err(|_| InstallReplicaRecoveryError::LocalIo)?;
    let target_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(InstallReplicaRecoveryError::LocalIo)?;
    let quarantine = parent.join(format!(
        ".{target_name}.recovery-{:016x}-{writer_fence:016x}",
        vset.0
    ));
    std::fs::create_dir(&quarantine).map_err(|_| InstallReplicaRecoveryError::LocalIo)?;
    let mut directories = BTreeSet::from([quarantine.clone()]);
    for (name, bytes) in blobs {
        let path = quarantine.join(name);
        let directory = path.parent().ok_or(InstallReplicaRecoveryError::LocalIo)?;
        std::fs::create_dir_all(directory).map_err(|_| InstallReplicaRecoveryError::LocalIo)?;
        let mut current = directory;
        loop {
            directories.insert(current.to_path_buf());
            if current == quarantine {
                break;
            }
            current = current
                .parent()
                .ok_or(InstallReplicaRecoveryError::LocalIo)?;
        }
        std::fs::write(&path, bytes).map_err(|_| InstallReplicaRecoveryError::LocalIo)?;
        std::fs::File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|_| InstallReplicaRecoveryError::LocalIo)?;
    }
    let mut directories: Vec<PathBuf> = directories.into_iter().collect();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        std::fs::File::open(directory)
            .and_then(|file| file.sync_all())
            .map_err(|_| InstallReplicaRecoveryError::LocalIo)?;
    }
    std::fs::rename(&quarantine, target).map_err(|_| InstallReplicaRecoveryError::LocalIo)?;
    std::fs::File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|_| InstallReplicaRecoveryError::LocalIo)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    use blockd_core::head::StashAssignment;
    use blockd_core::journal::{DurabilityMode, RecordKind, VsetConfig};
    use blockd_core::seam::{ReplicaCommitInfo, StoreFault};
    use blockd_core::types::JournalSeq;

    use super::*;

    #[derive(Default)]
    struct MemoryStore(Mutex<HashMap<String, (u64, Vec<u8>)>>);

    #[async_trait::async_trait]
    impl ObjectStore for MemoryStore {
        async fn put(self: Arc<Self>, key: String, bytes: Vec<u8>) -> Result<u64, StoreFault> {
            let mut objects = self.0.lock().expect("lock");
            let version = objects.get(&key).map_or(1, |(version, _)| version + 1);
            objects.insert(key, (version, bytes));
            Ok(version)
        }

        async fn put_cas(
            self: Arc<Self>,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, StoreFault> {
            let mut objects = self.0.lock().expect("lock");
            let actual = objects.get(&key).map(|(version, _)| *version);
            if actual != expected {
                return Err(StoreFault::CasConflict { actual });
            }
            let version = actual.unwrap_or(0) + 1;
            objects.insert(key, (version, bytes));
            Ok(version)
        }

        async fn get(self: Arc<Self>, key: String) -> crate::store::GetResult {
            Ok(self.0.lock().expect("lock").get(&key).cloned())
        }

        async fn get_range(
            self: Arc<Self>,
            _key: String,
            _offset: u64,
            _len: u64,
        ) -> crate::store::GetResult {
            unreachable!()
        }

        async fn delete(self: Arc<Self>, key: String) {
            self.0.lock().expect("lock").remove(&key);
        }
    }

    #[tokio::test]
    async fn promotion_claims_refences_publishes_then_atomically_installs() {
        let vset = VsetId(7);
        let store = Arc::new(MemoryStore::default());
        let head = HeadRecord {
            vset,
            holder: HostId(0),
            fence: 1,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: HostId(1),
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 1,
            }),
            retired_stashes: Vec::new(),
        };
        store
            .clone()
            .put_cas(layout::head_key(vset), None, head.encode())
            .await
            .expect("initial head");
        let record = JournalRecord {
            config: VsetConfig {
                disk_volumes: 1,
                pages_per_volume: 8,
                durability: DurabilityMode::PeerStashed,
            },
            seq: JournalSeq(3),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 4,
            sync_covered_through: 5,
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let bytes = record.encode(vset);
        let export = ReplicaExport {
            source_peer: HostId(1),
            assignment_epoch: 1,
            info: ReplicaCommitInfo {
                writer_fence: 1,
                seq: record.seq,
                sync_covered_through: 5,
            },
            sync_covered_through: 5,
            blobs: vec![
                (layout::journal_blob(vset, 1, record.seq), bytes.clone()),
                (layout::journal_mirror_blob(vset, 1, record.seq), bytes),
            ],
        };
        let target = std::env::temp_dir().join(format!(
            "blockd-recovery-install-{}-{}",
            std::process::id(),
            vset.0
        ));
        let _ = std::fs::remove_dir_all(&target);
        let installed =
            install_replica_recovery(&target, store.clone(), HostId(2), vset, 1, &export)
                .await
                .expect("install");
        assert_eq!(installed.writer_fence, 2);
        assert_eq!(installed.head_version, 3);
        let (_, head_bytes) = store
            .clone()
            .get(layout::head_key(vset))
            .await
            .expect("get")
            .expect("head");
        let published = HeadRecord::decode(vset, &head_bytes).expect("head");
        assert_eq!(published.holder, HostId(2));
        assert_eq!(published.manifest.expect("manifest").fence, 2);
        assert!(
            target
                .join(layout::journal_blob(vset, 2, record.seq))
                .exists()
        );
        std::fs::remove_dir_all(target).expect("cleanup");
    }

    #[tokio::test]
    async fn promotion_rejects_an_export_verified_against_an_older_head() {
        let vset = VsetId(8);
        let store = Arc::new(MemoryStore::default());
        let assignment = StashAssignment {
            assignment_epoch: 1,
            active_peer: HostId(1),
            active_assignment_epoch: 1,
            transition_peer: None,
            membership_epoch: 1,
        };
        let verified_head = HeadRecord {
            vset,
            holder: HostId(0),
            fence: 1,
            manifest: None,
            stash: Some(assignment),
            retired_stashes: Vec::new(),
        };
        store
            .clone()
            .put_cas(layout::head_key(vset), None, verified_head.encode())
            .await
            .expect("initial head");
        let record = JournalRecord {
            config: VsetConfig {
                disk_volumes: 1,
                pages_per_volume: 8,
                durability: DurabilityMode::PeerStashed,
            },
            seq: JournalSeq(3),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 4,
            sync_covered_through: 5,
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let bytes = record.encode(vset);
        let export = ReplicaExport {
            source_peer: assignment.active_peer,
            assignment_epoch: assignment.assignment_epoch,
            info: ReplicaCommitInfo {
                writer_fence: record.fence,
                seq: record.seq,
                sync_covered_through: record.sync_covered_through,
            },
            sync_covered_through: record.sync_covered_through,
            blobs: vec![
                (layout::journal_blob(vset, 1, record.seq), bytes.clone()),
                (layout::journal_mirror_blob(vset, 1, record.seq), bytes),
            ],
        };

        let advanced_head = HeadRecord {
            manifest: Some(ManifestPtr {
                fence: 1,
                seq: JournalSeq(4),
                capture_seq: 6,
            }),
            ..verified_head
        };
        store
            .clone()
            .put_cas(layout::head_key(vset), Some(1), advanced_head.encode())
            .await
            .expect("concurrent head advance");

        let target = std::env::temp_dir().join(format!(
            "blockd-stale-recovery-install-{}-{}",
            std::process::id(),
            vset.0
        ));
        let _ = std::fs::remove_dir_all(&target);
        let result = install_replica_recovery(&target, store, HostId(2), vset, 1, &export).await;
        if target.exists() {
            std::fs::remove_dir_all(&target).expect("cleanup unexpected install");
        }
        assert_eq!(
            result,
            Err(InstallReplicaRecoveryError::ClaimConflict),
            "installation must remain bound to the head against which the export was verified"
        );
    }
}
