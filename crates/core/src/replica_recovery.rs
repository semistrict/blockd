//! Read-only recovery of a lost primary from the peers named by its fenced
//! head. This module verifies a complete committed closure and exports normal
//! local blob names; it deliberately does not claim ownership or run a guest.

use std::collections::{BTreeMap, BTreeSet};

use crate::blx::{BatchMeta, BlxObject, NamespaceKind, compact_objects};
use crate::head::{HeadRecord, ManifestPtr, RetiredStash};
use crate::journal::JournalRecord;
use crate::layout;
use crate::manifest::{CompleteFileList, Manifest, ObjectRef};
use crate::page_file::scan_page_file;
use crate::protocol::{ReplicaArtifact, ReplicaCommitInfo};
use crate::replica_spool::{ReplicaCommitFrame, ReplicaSpoolScan, scan_replica_spool};
use crate::types::{HostId, VolumeId};

pub type ReplicaStoreObjects = Vec<(String, Vec<u8>)>;

#[derive(Clone, Copy, Debug)]
pub struct ReplicaResidue<'a> {
    pub source: HostId,
    pub peer: HostId,
    pub assignment_epoch: u64,
    pub bytes: &'a [u8],
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaInventory {
    pub peer: HostId,
    pub assignment_epoch: u64,
    pub committed: Option<ReplicaCommitInfo>,
    pub valid_bytes: usize,
    pub torn_tail: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaExport {
    pub source_peer: HostId,
    pub assignment_epoch: u64,
    pub info: ReplicaCommitInfo,
    pub sync_covered_through: u64,
    /// Normal local blob identities suitable for a quarantined recovery root.
    pub blobs: Vec<(String, Vec<u8>)>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplicaRecoveryStatus {
    Complete,
    Incomplete,
}

/// Machine-readable recovery assessment. `human_summary` renders the same
/// decision for an operator without changing or claiming any state.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaRecoveryReport {
    pub status: ReplicaRecoveryStatus,
    pub inventories: Vec<ReplicaInventory>,
    pub chosen_source: Option<HostId>,
    pub chosen_assignment_epoch: Option<u64>,
    pub covered_sync_through: Option<u64>,
    pub missing_objects: Vec<String>,
    pub corrupt_objects: Vec<String>,
}

impl ReplicaRecoveryReport {
    pub fn human_summary(&self) -> String {
        match (
            self.status,
            self.chosen_source,
            self.chosen_assignment_epoch,
            self.covered_sync_through,
        ) {
            (ReplicaRecoveryStatus::Complete, Some(peer), Some(epoch), Some(through)) => {
                format!(
                    "complete: peer {}, assignment {epoch}, sync watermark {through}",
                    peer.get()
                )
            }
            _ => format!(
                "incomplete: source={:?} missing=[{}] corrupt=[{}]",
                self.chosen_source,
                self.missing_objects.join(","),
                self.corrupt_objects.join(",")
            ),
        }
    }
}

/// A separate, fenced ownership operation for a verified quarantined export.
/// The caller CASes `head.encode()` using `expected_version`; the returned CAS
/// version is then passed to `refence_replica_export` before promotion/start.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaRecoveryClaim {
    pub expected_version: u64,
    pub head: HeadRecord,
}

/// The single fenced publication produced from a verified passive export.
/// Store objects are ordered data first, then the complete list, then the
/// manifest; the head CAS is the sole visibility transition.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaPublication {
    pub export: ReplicaExport,
    pub store_objects: ReplicaStoreObjects,
    pub head: HeadRecord,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplicaRecoveryError {
    NoAssignment,
    UnnamedPeer { peer: HostId },
    StaleAssignment { peer: HostId, assignment_epoch: u64 },
    CorruptSpool { peer: HostId },
    NoCommit,
    StoreRecoveryPointNewer,
    MissingObject { key: String },
    CorruptObject { key: String },
}

pub fn prepare_replica_recovery_claim(
    observed_version: u64,
    observed_head: &HeadRecord,
    claimant: HostId,
    export: &ReplicaExport,
) -> ReplicaRecoveryClaim {
    let mut head = observed_head.clone();
    head.holder = claimant;
    if let Some(observed_assignment) = observed_head.stash {
        head.stash = Some(crate::head::StashAssignment {
            assignment_epoch: export.assignment_epoch,
            active_peer: export.source_peer,
            active_assignment_epoch: export.assignment_epoch,
            transition_peer: None,
            membership_epoch: observed_assignment.membership_epoch,
        });
        // The verified export is a complete covering cut, so older stash
        // copies are no longer recovery roots under the single-loss contract.
        head.retired_stashes.clear();
    }
    // Informational until the CAS returns its authoritative fence.
    head.fence = 0;
    ReplicaRecoveryClaim {
        expected_version: observed_version,
        head,
    }
}

pub fn refence_replica_export(
    volume: VolumeId,
    export: &ReplicaExport,
    new_fence: u64,
) -> Result<ReplicaExport, ReplicaRecoveryError> {
    let record_bytes = export
        .blobs
        .iter()
        .find_map(|(name, bytes)| {
            matches!(
                layout::parse_blob(name),
                Some(layout::BlobName::Journal { .. })
            )
            .then_some(bytes)
        })
        .ok_or(ReplicaRecoveryError::NoCommit)?;
    let mut record = JournalRecord::decode(volume, record_bytes).map_err(|_| {
        ReplicaRecoveryError::CorruptObject {
            key: "recovery journal".to_owned(),
        }
    })?;
    record.fence = new_fence;
    let record_bytes = record.encode(volume);
    let mut blobs: Vec<_> = export
        .blobs
        .iter()
        .filter(|(name, _)| {
            !matches!(
                layout::parse_blob(name),
                Some(layout::BlobName::Journal { .. })
            )
        })
        .cloned()
        .collect();
    blobs.push((
        layout::journal_blob(volume, new_fence, record.seq),
        record_bytes.clone(),
    ));
    blobs.push((
        layout::journal_mirror_blob(volume, new_fence, record.seq),
        record_bytes,
    ));
    blobs.sort_by(|left, right| left.0.cmp(&right.0));
    let mut result = export.clone();
    result.info.writer_fence = new_fence;
    result.blobs = blobs;
    Ok(result)
}

pub fn prepare_replica_publication(
    volume: VolumeId,
    claimant: HostId,
    writer_fence: u64,
    claimed_head: &HeadRecord,
    export: &ReplicaExport,
) -> Result<ReplicaPublication, ReplicaRecoveryError> {
    let recovered_stash = RetiredStash {
        peer: export.source_peer,
        assignment_epoch: export.assignment_epoch,
        through: export.info,
    };
    let export = refence_replica_export(volume, export, writer_fence)?;
    let (record, record_bytes) = publication_record(volume, writer_fence, &export)?;
    let (store_objects, manifest) =
        build_replica_archive(volume, writer_fence, &record, record_bytes, &export)?;
    let mut head = claimed_head.clone();
    head.volume = volume;
    head.holder = claimant;
    head.fence = writer_fence;
    head.manifest = Some(manifest);
    head.stash = None;
    // Publication makes the recovered passive copy redundant, but its exact
    // identity must remain durable until a restarted holder sends and
    // acknowledges ReplicaRelease.
    head.retired_stashes = vec![recovered_stash];
    Ok(ReplicaPublication {
        export,
        store_objects,
        head,
    })
}

fn publication_record(
    volume: VolumeId,
    writer_fence: u64,
    export: &ReplicaExport,
) -> Result<(JournalRecord, &[u8]), ReplicaRecoveryError> {
    let bytes = export
        .blobs
        .iter()
        .find_map(|(name, bytes)| {
            matches!(
                layout::parse_blob(name),
                Some(layout::BlobName::Journal { fence, .. }) if fence == writer_fence
            )
            .then_some(bytes)
        })
        .ok_or(ReplicaRecoveryError::NoCommit)?;
    let record =
        JournalRecord::decode(volume, bytes).map_err(|_| ReplicaRecoveryError::CorruptObject {
            key: "recovery journal".to_owned(),
        })?;
    Ok((record, bytes))
}

fn build_replica_archive(
    volume: VolumeId,
    writer_fence: u64,
    record: &JournalRecord,
    record_bytes: &[u8],
    export: &ReplicaExport,
) -> Result<(ReplicaStoreObjects, ManifestPtr), ReplicaRecoveryError> {
    if record.sync_covered_through > record.capture_seq {
        return Err(ReplicaRecoveryError::CorruptObject {
            key: "recovery journal".to_owned(),
        });
    }
    let mut inputs = Vec::new();
    for (name, bytes) in &export.blobs {
        if matches!(layout::parse_blob(name), Some(layout::BlobName::Blx { .. })) {
            inputs.push(
                BlxObject::open(bytes)
                    .map_err(|_| ReplicaRecoveryError::CorruptObject { key: name.clone() })?,
            );
        }
    }
    let compacted = compact_objects(
        BatchMeta {
            namespace_kind: NamespaceKind::Volume,
            namespace_id: volume.0,
            writer_fence,
            first_object_id: u64::MAX - 1024,
            min_seq: record.seq.0,
            max_seq: record.seq.0,
            batch_id: u64::MAX - 1024,
            pre_state_checksum: record.post_state_checksum,
            post_state_checksum: record.post_state_checksum,
        },
        &inputs,
        true,
    )
    .map_err(|_| ReplicaRecoveryError::CorruptObject {
        key: "recovery blx".to_owned(),
    })?;
    let mut store_objects = Vec::new();
    let objects = compacted
        .into_iter()
        .map(|object| {
            let reference = ObjectRef::from_blx(&object);
            store_objects.push((reference.identity.store_key(), object.bytes));
            reference
        })
        .collect::<Vec<_>>();
    let list = CompleteFileList {
        volume,
        writer_fence,
        list_id: record.seq.0,
        objects,
    };
    store_objects.push((
        layout::complete_file_list_key(volume, writer_fence, record.seq.0),
        list.encode(),
    ));
    let (recovery_kind, checkpoint_epoch, vmstate_logical_length) =
        crate::engine::recovery_policy::recovery_metadata(record);
    let archive = Manifest {
        volume,
        writer_fence,
        journal_seq: record.seq.0,
        archive_seq: record.seq.0,
        capture_seq: record.capture_seq,
        sync_covered_through: record.sync_covered_through,
        recovery_kind,
        checkpoint_epoch,
        config: record.config,
        vmstate_logical_length,
        base: None,
        complete_list: Some(list.reference()),
        post_state_checksum: record.post_state_checksum,
        metadata_checksum: crate::format::checksum64(record_bytes),
        added: Vec::new(),
        removed: Vec::new(),
    };
    let archive_bytes = archive
        .encode()
        .map_err(|_| ReplicaRecoveryError::CorruptObject {
            key: "recovery manifest".to_owned(),
        })?;
    let manifest = archive.pointer(&archive_bytes);
    store_objects.push((
        layout::manifest_key(volume, manifest.fence, manifest.seq),
        archive_bytes,
    ));
    Ok((store_objects, manifest))
}

pub fn report_replica_recovery(
    source: HostId,
    volume: VolumeId,
    head_version: u64,
    head: &HeadRecord,
    residues: &[ReplicaResidue<'_>],
    store_objects: &BTreeMap<String, Vec<u8>>,
) -> ReplicaRecoveryReport {
    let mut report = ReplicaRecoveryReport {
        status: ReplicaRecoveryStatus::Incomplete,
        inventories: Vec::new(),
        chosen_source: None,
        chosen_assignment_epoch: None,
        covered_sync_through: None,
        missing_objects: Vec::new(),
        corrupt_objects: Vec::new(),
    };
    let mut best = None;
    for &residue in residues {
        match inventory_replica(source, volume, residue) {
            Ok(inventory) => {
                if let Some(info) = inventory.committed {
                    let rank = (info.sync_covered_through, info.seq);
                    if best.is_none_or(|(known, _, _)| rank > known) {
                        best = Some((rank, residue.peer, residue.assignment_epoch));
                    }
                }
                report.inventories.push(inventory);
            }
            Err(_) => report.corrupt_objects.push(format!(
                "peer:{}/assignment:{}",
                residue.peer.get(),
                residue.assignment_epoch
            )),
        }
    }
    if let Some(((covered, _), peer, epoch)) = best {
        report.chosen_source = Some(peer);
        report.chosen_assignment_epoch = Some(epoch);
        report.covered_sync_through = Some(covered);
    }
    match export_replica_recovery(source, volume, head_version, head, residues, store_objects) {
        Ok(export) => {
            report.status = ReplicaRecoveryStatus::Complete;
            report.chosen_source = Some(export.source_peer);
            report.chosen_assignment_epoch = Some(export.assignment_epoch);
            report.covered_sync_through = Some(export.sync_covered_through);
        }
        Err(ReplicaRecoveryError::MissingObject { key }) => report.missing_objects.push(key),
        Err(ReplicaRecoveryError::CorruptObject { key }) => report.corrupt_objects.push(key),
        Err(ReplicaRecoveryError::CorruptSpool { peer }) => {
            report.corrupt_objects.push(format!("peer:{}", peer.get()));
        }
        Err(_) => {}
    }
    report.missing_objects.sort();
    report.missing_objects.dedup();
    report.corrupt_objects.sort();
    report.corrupt_objects.dedup();
    report
}

pub fn inventory_replica(
    source: HostId,
    volume: VolumeId,
    residue: ReplicaResidue<'_>,
) -> Result<ReplicaInventory, ReplicaRecoveryError> {
    let scan = scan_replica_spool(residue.bytes)
        .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
    verify_scan_identity(source, volume, residue, &scan)?;
    Ok(ReplicaInventory {
        peer: residue.peer,
        assignment_epoch: residue.assignment_epoch,
        committed: scan.commits.last().map(|commit| commit.info),
        valid_bytes: scan.valid_len,
        torn_tail: scan.truncated_tail,
    })
}

/// Verify and export the newest committed residue. Normally the source must be
/// named by `head`; a higher assignment epoch is also accepted when its
/// complete commit was written by the holder at the head's current fence. That
/// is the durable proof left by an offline passive replacement whose head CAS
/// could not complete during an object-store outage.
pub fn export_replica_recovery(
    source: HostId,
    volume: VolumeId,
    head_version: u64,
    head: &HeadRecord,
    residues: &[ReplicaResidue<'_>],
    store_objects: &BTreeMap<String, Vec<u8>>,
) -> Result<ReplicaExport, ReplicaRecoveryError> {
    let assignment = head.stash.ok_or(ReplicaRecoveryError::NoAssignment)?;
    let allowed = allowed_replica_epochs(head, assignment);
    let mut candidates = Vec::new();
    for &residue in residues {
        let named = allowed
            .get(&residue.peer)
            .is_some_and(|epochs| epochs.contains(&residue.assignment_epoch));
        let possible_offline_replacement = residue.assignment_epoch > assignment.assignment_epoch;
        if !named && !possible_offline_replacement {
            return if allowed.contains_key(&residue.peer) {
                Err(ReplicaRecoveryError::StaleAssignment {
                    peer: residue.peer,
                    assignment_epoch: residue.assignment_epoch,
                })
            } else {
                Err(ReplicaRecoveryError::UnnamedPeer { peer: residue.peer })
            };
        }
        let scan = scan_replica_spool(residue.bytes)
            .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
        verify_scan_identity(source, volume, residue, &scan)?;
        if let Some(commit) = scan.commits.last() {
            let record = JournalRecord::decode(volume, &commit.record)
                .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
            if !named
                && (head.holder != source
                    || commit.info.writer_fence != head_version
                    || record.fence != head_version
                    || record.seq != commit.info.seq
                    || record.sync_covered_through != commit.info.sync_covered_through)
            {
                return Err(ReplicaRecoveryError::StaleAssignment {
                    peer: residue.peer,
                    assignment_epoch: residue.assignment_epoch,
                });
            }
            candidates.push((
                (
                    commit.info.sync_covered_through,
                    record.capture_seq,
                    commit.info.seq,
                ),
                residue,
                scan,
            ));
        } else if !named {
            return Err(ReplicaRecoveryError::StaleAssignment {
                peer: residue.peer,
                assignment_epoch: residue.assignment_epoch,
            });
        }
    }
    candidates.sort_by_key(|(rank, _, _)| *rank);
    let Some((_, residue, scan)) = candidates.pop() else {
        return Err(ReplicaRecoveryError::NoCommit);
    };
    let commit = scan.commits.last().expect("candidate has commit");
    if let Some(ptr) = head.manifest {
        let key = ptr.manifest_key(volume);
        let bytes = store_objects
            .get(&key)
            .ok_or_else(|| ReplicaRecoveryError::MissingObject { key: key.clone() })?;
        if crate::format::checksum64(bytes) != ptr.checksum {
            return Err(ReplicaRecoveryError::CorruptObject { key });
        }
        let store_record = Manifest::decode(volume, bytes)
            .map_err(|_| ReplicaRecoveryError::CorruptObject { key: key.clone() })?;
        if (
            store_record.writer_fence,
            store_record.archive_seq,
            store_record.capture_seq,
        ) != (ptr.fence, ptr.seq.0, ptr.capture_seq)
        {
            return Err(ReplicaRecoveryError::CorruptObject { key });
        }
        let peer_record = JournalRecord::decode(volume, &commit.record)
            .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
        let peer_rank = (
            commit.info.sync_covered_through,
            peer_record.capture_seq,
            commit.info.seq,
        );
        let store_rank = (
            store_record.sync_covered_through,
            store_record.capture_seq,
            crate::types::JournalSeq(store_record.archive_seq),
        );
        if peer_rank < store_rank {
            return Err(ReplicaRecoveryError::StoreRecoveryPointNewer);
        }
    }
    export_commit(volume, residue, &scan, commit, store_objects)
}

fn allowed_replica_epochs(
    head: &HeadRecord,
    assignment: crate::head::StashAssignment,
) -> BTreeMap<HostId, BTreeSet<u64>> {
    let mut allowed: BTreeMap<HostId, BTreeSet<u64>> = BTreeMap::new();
    allowed
        .entry(assignment.active_peer)
        .or_default()
        .insert(assignment.active_assignment_epoch);
    if let Some(peer) = assignment.transition_peer {
        allowed
            .entry(peer)
            .or_default()
            .insert(assignment.assignment_epoch);
    }
    for retired in &head.retired_stashes {
        allowed
            .entry(retired.peer)
            .or_default()
            .insert(retired.assignment_epoch);
    }
    allowed
}

fn verify_scan_identity(
    source: HostId,
    volume: VolumeId,
    residue: ReplicaResidue<'_>,
    scan: &ReplicaSpoolScan,
) -> Result<(), ReplicaRecoveryError> {
    let identity_ok = residue.source == source
        && scan.artifacts.values().all(|frame| {
            frame.source == residue.source
                && frame.volume == volume
                && frame.assignment_epoch == residue.assignment_epoch
        })
        && scan.commits.iter().all(|commit| {
            commit.source == residue.source
                && commit.volume == volume
                && commit.assignment_epoch == residue.assignment_epoch
        });
    if identity_ok {
        Ok(())
    } else {
        Err(ReplicaRecoveryError::CorruptSpool { peer: residue.peer })
    }
}

fn export_commit(
    volume: VolumeId,
    residue: ReplicaResidue<'_>,
    scan: &ReplicaSpoolScan,
    commit: &ReplicaCommitFrame,
    store_objects: &BTreeMap<String, Vec<u8>>,
) -> Result<ReplicaExport, ReplicaRecoveryError> {
    let record = JournalRecord::decode(volume, &commit.record)
        .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
    let needed_blx_files: BTreeSet<crate::manifest::ObjectIdentity> = record
        .files
        .iter()
        .filter(|file| {
            file.identity.namespace_kind == crate::blx::NamespaceKind::Volume
                && file.identity.namespace_id == volume.0
        })
        .map(|file| file.identity)
        .collect();
    let mut blobs = BTreeMap::new();
    for identity in needed_blx_files {
        let fence = identity.writer_fence;
        let object = crate::types::ObjectId(identity.object_id);
        let artifact = ReplicaArtifact::Blx { fence, object };
        let key = layout::blx_key(volume, fence, object.0);
        let bytes = artifact_bytes(scan, artifact, &key, store_objects)?;
        let intact = scan_page_file(&bytes).is_ok_and(|(got_volume, got_fence, got_seg, _)| {
            (got_volume, got_fence, got_seg) == (volume, fence, object)
        });
        if !intact {
            return Err(ReplicaRecoveryError::CorruptObject { key });
        }
        blobs.insert(layout::blx_blob(volume, fence, object), bytes);
    }
    blobs.insert(
        layout::journal_blob(volume, record.fence, record.seq),
        commit.record.clone(),
    );
    blobs.insert(
        layout::journal_mirror_blob(volume, record.fence, record.seq),
        commit.record.clone(),
    );
    Ok(ReplicaExport {
        source_peer: residue.peer,
        assignment_epoch: residue.assignment_epoch,
        info: commit.info,
        sync_covered_through: commit.info.sync_covered_through,
        blobs: blobs.into_iter().collect(),
    })
}

fn artifact_bytes(
    scan: &ReplicaSpoolScan,
    artifact: ReplicaArtifact,
    store_key: &str,
    store_objects: &BTreeMap<String, Vec<u8>>,
) -> Result<Vec<u8>, ReplicaRecoveryError> {
    scan.artifacts
        .get(&artifact)
        .map(|frame| frame.bytes.clone())
        .or_else(|| store_objects.get(store_key).cloned())
        .ok_or_else(|| ReplicaRecoveryError::MissingObject {
            key: store_key.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn id(host: u32) -> HostId {
        HostId::new(host)
    }
    use crate::head::StashAssignment;
    use crate::journal::{RecordKind, VolumeConfig};
    use crate::page_file::PageBatchBuilder;
    use crate::replica_spool::{seal_replica_artifact, seal_replica_commit};
    use crate::types::{Gen, HostId, JournalSeq, ObjectId, PageId, PageNo, page_size};

    fn file_ref(bytes: &[u8]) -> crate::manifest::ObjectRef {
        crate::manifest::ObjectRef::from_blx(&BlxObject::open(bytes).expect("valid BLX"))
    }

    fn recovery_spool(
        source: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        seq: u64,
        covered: u64,
        fill: u8,
    ) -> (Vec<u8>, Vec<u8>, JournalRecord) {
        recovery_spool_identity(source, volume, assignment_epoch, seq, covered, fill)
    }

    fn recovery_spool_identity(
        source: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        seq: u64,
        covered: u64,
        fill: u8,
    ) -> (Vec<u8>, Vec<u8>, JournalRecord) {
        let page = PageId {
            volume,
            page: PageNo(2),
        };
        let object = ObjectId(seq);
        let mut builder = PageBatchBuilder::new(volume, 4, object);
        builder.add(page, Gen(seq), &vec![fill; page_size()]);
        let (_, blx, locs) = builder.finish().pop().expect("fixture object");
        let artifact = ReplicaArtifact::Blx { fence: 4, object };
        let info = ReplicaCommitInfo {
            writer_fence: 4,
            seq: JournalSeq(seq),
            sync_covered_through: covered,
        };
        let record = JournalRecord {
            config: VolumeConfig::data(8),
            seq: info.seq,
            fence: info.writer_fence,
            kind: RecordKind::Commit,
            capture_seq: seq,
            sync_covered_through: covered,
            post_state_checksum: 0,
            files: vec![file_ref(&blx)],
            runtime_page_index: locs
                .into_iter()
                .map(|(page, generation, loc)| (page, (generation, loc)))
                .collect(),
            migrated_from: None,
        };
        let encoded = record.encode(volume);
        let mut spool = seal_replica_artifact(source, volume, assignment_epoch, artifact, &blx)
            .expect("artifact");
        spool.extend(
            seal_replica_commit(
                source,
                volume,
                assignment_epoch,
                info,
                &[artifact],
                &encoded,
            )
            .expect("commit"),
        );
        (spool, blx, record)
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn lost_primary_exports_a_complete_normal_local_recovery_set() {
        let source = id(0);
        let peer = id(1);
        let volume = VolumeId(7);
        let page = PageId {
            volume,
            page: PageNo(2),
        };
        let mut builder = PageBatchBuilder::new(volume, 4, ObjectId(9));
        builder.add(page, Gen(3), &vec![0xA5; page_size()]);
        let (_, blx, locs) = builder.finish().pop().expect("fixture object");
        let artifact = ReplicaArtifact::Blx {
            fence: 4,
            object: ObjectId(9),
        };
        let info = ReplicaCommitInfo {
            writer_fence: 4,
            seq: JournalSeq(8),
            sync_covered_through: 12,
        };
        let record = JournalRecord {
            config: VolumeConfig {
                kind: crate::journal::VolumeKind::Data,
                pages: 8,
            },
            seq: info.seq,
            fence: info.writer_fence,
            kind: RecordKind::Commit,
            capture_seq: 11,
            sync_covered_through: info.sync_covered_through,
            post_state_checksum: 0,
            files: vec![file_ref(&blx)],
            runtime_page_index: locs
                .into_iter()
                .map(|(page, generation, loc)| (page, (generation, loc)))
                .collect(),
            migrated_from: None,
        }
        .encode(volume);
        let mut spool = seal_replica_artifact(source, volume, 1, artifact, &blx).expect("artifact");
        spool.extend(
            seal_replica_commit(source, volume, 1, info, &[artifact], &record).expect("commit"),
        );
        let head = HeadRecord {
            volume,
            holder: source,
            fence: 4,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: peer,
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 6,
            }),
            retired_stashes: Vec::new(),
        };
        let export = export_replica_recovery(
            source,
            volume,
            head.fence,
            &head,
            &[ReplicaResidue {
                source,
                peer,
                assignment_epoch: 1,
                bytes: &spool,
            }],
            &BTreeMap::new(),
        )
        .expect("complete peer residue recovers without object-store data");
        assert_eq!(export.source_peer, peer);
        assert_eq!(export.sync_covered_through, 12);
        let report = report_replica_recovery(
            source,
            volume,
            head.fence,
            &head,
            &[ReplicaResidue {
                source,
                peer,
                assignment_epoch: 1,
                bytes: &spool,
            }],
            &BTreeMap::new(),
        );
        assert_eq!(report.status, ReplicaRecoveryStatus::Complete);
        assert!(report.human_summary().starts_with("complete:"));
        let claim = prepare_replica_recovery_claim(9, &head, id(3), &export);
        assert_eq!(claim.expected_version, 9);
        assert_eq!(claim.head.holder, id(3));
        assert_eq!(claim.head.stash.expect("verified source").active_peer, peer);
        let refenced = refence_replica_export(volume, &export, 10).expect("fresh writer fence");
        let (_, journal) = refenced
            .blobs
            .iter()
            .find(|(name, _)| *name == layout::journal_blob(volume, 10, JournalSeq(8)))
            .expect("refenced journal");
        assert_eq!(
            JournalRecord::decode(volume, journal)
                .expect("journal")
                .fence,
            10
        );
        assert!(export.blobs.iter().any(|(name, bytes)| {
            name == &layout::blx_blob(volume, 4, ObjectId(9)) && bytes == &blx
        }));
        assert_eq!(
            export
                .blobs
                .iter()
                .filter(|(name, _)| {
                    name == &layout::journal_blob(volume, 4, JournalSeq(8))
                        || name == &layout::journal_mirror_blob(volume, 4, JournalSeq(8))
                })
                .count(),
            2
        );
    }

    #[test]
    fn recovery_never_scans_an_unnamed_peer() {
        let volume = VolumeId(7);
        let head = HeadRecord {
            volume,
            holder: id(0),
            fence: 1,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: id(1),
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 6,
            }),
            retired_stashes: Vec::new(),
        };
        assert_eq!(
            export_replica_recovery(
                id(0),
                volume,
                head.fence,
                &head,
                &[ReplicaResidue {
                    source: id(0),
                    peer: id(2),
                    assignment_epoch: 1,
                    bytes: &[],
                }],
                &BTreeMap::new(),
            ),
            Err(ReplicaRecoveryError::UnnamedPeer { peer: id(2) })
        );
    }

    #[test]
    fn recovery_accepts_a_complete_offline_replacement_from_the_current_writer_fence() {
        let source = id(0);
        let replacement = id(2);
        let volume = VolumeId(17);
        let (spool, _, _) = recovery_spool(source, volume, 2, 8, 12, 0xA5);
        let head = HeadRecord {
            volume,
            holder: source,
            fence: 4,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: id(1),
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 6,
            }),
            retired_stashes: Vec::new(),
        };
        let export = export_replica_recovery(
            source,
            volume,
            head.fence,
            &head,
            &[ReplicaResidue {
                source,
                peer: replacement,
                assignment_epoch: 2,
                bytes: &spool,
            }],
            &BTreeMap::new(),
        )
        .expect("the current fenced writer may finish replacement while the store is down");
        assert_eq!(export.source_peer, replacement);
        assert_eq!(export.assignment_epoch, 2);
        assert_eq!(export.sync_covered_through, 12);
    }

    #[test]
    fn recovery_rejects_an_offline_replacement_from_a_stale_writer_fence() {
        let source = id(0);
        let replacement = id(2);
        let volume = VolumeId(18);
        let (spool, _, _) = recovery_spool(source, volume, 2, 8, 12, 0xA5);
        let head = HeadRecord {
            volume,
            holder: source,
            fence: 5,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: id(1),
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 6,
            }),
            retired_stashes: Vec::new(),
        };
        assert_eq!(
            export_replica_recovery(
                source,
                volume,
                head.fence,
                &head,
                &[ReplicaResidue {
                    source,
                    peer: replacement,
                    assignment_epoch: 2,
                    bytes: &spool,
                }],
                &BTreeMap::new(),
            ),
            Err(ReplicaRecoveryError::StaleAssignment {
                peer: replacement,
                assignment_epoch: 2,
            })
        );
    }

    #[test]
    fn recovery_refuses_peer_residue_older_than_the_store_manifest() {
        let source = id(0);
        let peer = id(1);
        let volume = VolumeId(8);
        let config = VolumeConfig {
            kind: crate::journal::VolumeKind::Data,
            pages: 8,
        };
        let peer_info = ReplicaCommitInfo {
            writer_fence: 4,
            seq: JournalSeq(8),
            sync_covered_through: 12,
        };
        let peer_record = JournalRecord {
            config,
            seq: peer_info.seq,
            fence: peer_info.writer_fence,
            kind: RecordKind::Commit,
            capture_seq: 11,
            sync_covered_through: peer_info.sync_covered_through,
            post_state_checksum: 0,
            files: Vec::new(),
            runtime_page_index: BTreeMap::new(),
            migrated_from: None,
        }
        .encode(volume);
        let spool = seal_replica_commit(source, volume, 1, peer_info, &[], &peer_record)
            .expect("valid older residue");

        let store_record = JournalRecord {
            config,
            seq: JournalSeq(9),
            fence: 4,
            kind: RecordKind::Commit,
            capture_seq: 13,
            sync_covered_through: 13,
            post_state_checksum: 0,
            files: Vec::new(),
            runtime_page_index: BTreeMap::new(),
            migrated_from: None,
        };
        let head = HeadRecord {
            volume,
            holder: source,
            fence: 4,
            manifest: Some(crate::head::ManifestPtr {
                fence: store_record.fence,
                journal_seq: store_record.seq,
                seq: store_record.seq,
                capture_seq: store_record.capture_seq,
                checksum: crate::format::checksum64(&store_record.encode(volume)),
            }),
            stash: Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: peer,
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 6,
            }),
            retired_stashes: Vec::new(),
        };
        let store_objects = BTreeMap::from([(
            layout::manifest_key(volume, store_record.fence, store_record.seq),
            store_record.encode(volume),
        )]);

        assert!(
            export_replica_recovery(
                source,
                volume,
                head.fence,
                &head,
                &[ReplicaResidue {
                    source,
                    peer,
                    assignment_epoch: 1,
                    bytes: &spool,
                }],
                &store_objects,
            )
            .is_err(),
            "recovery must not replace a newer store recovery point with stale peer residue"
        );
    }

    #[test]
    fn recovery_after_activation_selects_the_new_active_and_exact_bytes() {
        let source = id(0);
        let volume = VolumeId(19);
        let (old_spool, old_blx, _) = recovery_spool(source, volume, 8, 80, 80, 0x18);
        let (active_spool, active_blx, active_record) =
            recovery_spool(source, volume, 9, 90, 90, 0xA9);
        assert_ne!(old_blx, active_blx);
        let head = HeadRecord {
            volume,
            holder: source,
            fence: 4,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 9,
                active_peer: id(3),
                active_assignment_epoch: 9,
                transition_peer: None,
                membership_epoch: 6,
            }),
            retired_stashes: vec![crate::head::RetiredStash {
                peer: id(2),
                assignment_epoch: 8,
                through: ReplicaCommitInfo {
                    writer_fence: 4,
                    seq: JournalSeq(80),
                    sync_covered_through: 80,
                },
            }],
        };
        let residues = [
            ReplicaResidue {
                source,
                peer: id(2),
                assignment_epoch: 8,
                bytes: &old_spool,
            },
            ReplicaResidue {
                source,
                peer: id(3),
                assignment_epoch: 9,
                bytes: &active_spool,
            },
        ];
        let export = export_replica_recovery(
            source,
            volume,
            head.fence,
            &head,
            &residues,
            &BTreeMap::new(),
        )
        .expect("new active is a complete recovery source");
        assert_eq!(export.source_peer, id(3));
        assert_eq!(export.assignment_epoch, 9);
        assert_eq!(export.sync_covered_through, 90);
        assert!(export.blobs.iter().any(|(name, bytes)| {
            name == &layout::blx_blob(volume, 4, ObjectId(90)) && bytes == &active_blx
        }));
        let (_, bytes) = export
            .blobs
            .iter()
            .find(|(name, _)| name == &layout::journal_blob(volume, 4, JournalSeq(90)))
            .expect("active journal exported");
        let mut expected = active_record;
        expected.runtime_page_index.clear();
        assert_eq!(
            JournalRecord::decode(volume, bytes).expect("exported journal"),
            expected
        );
    }

    #[test]
    fn recovery_during_transition_uses_only_a_complete_covering_cut() {
        let source = id(0);
        let volume = VolumeId(20);
        let (active_spool, _, active_record) = recovery_spool(source, volume, 11, 110, 110, 0x11);
        let (transition_spool, transition_blx, transition_record) =
            recovery_spool(source, volume, 12, 120, 120, 0x12);
        let head = HeadRecord {
            volume,
            holder: source,
            fence: 4,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 12,
                active_peer: id(1),
                active_assignment_epoch: 11,
                transition_peer: Some(id(2)),
                membership_epoch: 6,
            }),
            retired_stashes: Vec::new(),
        };

        let active_only = export_replica_recovery(
            source,
            volume,
            head.fence,
            &head,
            &[ReplicaResidue {
                source,
                peer: id(1),
                assignment_epoch: 11,
                bytes: &active_spool,
            }],
            &BTreeMap::new(),
        )
        .expect("incomplete replacement leaves old active authoritative");
        assert_eq!(active_only.source_peer, id(1));
        assert_eq!(active_only.sync_covered_through, 110);
        let (_, journal) = active_only
            .blobs
            .iter()
            .find(|(name, _)| name == &layout::journal_blob(volume, 4, JournalSeq(110)))
            .expect("old active journal");
        let mut expected = active_record;
        expected.runtime_page_index.clear();
        assert_eq!(JournalRecord::decode(volume, journal).unwrap(), expected);

        let transition_complete = export_replica_recovery(
            source,
            volume,
            head.fence,
            &head,
            &[
                ReplicaResidue {
                    source,
                    peer: id(1),
                    assignment_epoch: 11,
                    bytes: &active_spool,
                },
                ReplicaResidue {
                    source,
                    peer: id(2),
                    assignment_epoch: 12,
                    bytes: &transition_spool,
                },
            ],
            &BTreeMap::new(),
        )
        .expect("complete replacement is the newest protected cut");
        assert_eq!(transition_complete.source_peer, id(2));
        assert_eq!(transition_complete.sync_covered_through, 120);
        assert!(transition_complete.blobs.iter().any(|(name, bytes)| {
            name == &layout::blx_blob(volume, 4, ObjectId(120)) && bytes == &transition_blx
        }));
        let (_, journal) = transition_complete
            .blobs
            .iter()
            .find(|(name, _)| name == &layout::journal_blob(volume, 4, JournalSeq(120)))
            .expect("transition journal");
        let mut expected = transition_record;
        expected.runtime_page_index.clear();
        assert_eq!(JournalRecord::decode(volume, journal).unwrap(), expected);
    }
}
