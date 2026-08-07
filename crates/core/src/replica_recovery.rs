//! Read-only recovery of a lost primary from the peers named by its fenced
//! head. This module verifies a complete committed closure and exports normal
//! local blob names; it deliberately does not claim ownership or run a guest.

use std::collections::{BTreeMap, BTreeSet};

use crate::head::HeadRecord;
use crate::journal::JournalRecord;
use crate::layout;
use crate::mapleaf::MapLeaf;
use crate::replica_spool::{ReplicaCommitFrame, ReplicaSpoolScan, scan_replica_spool};
use crate::seam::{ReplicaArtifact, ReplicaCommitInfo};
use crate::segment::scan_segment;
use crate::types::{HostId, VsetId};

#[derive(Clone, Copy, Debug)]
pub struct ReplicaResidue<'a> {
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
                    peer.0
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
) -> ReplicaRecoveryClaim {
    let mut head = observed_head.clone();
    head.holder = claimant;
    // Informational until the CAS returns its authoritative fence.
    head.fence = 0;
    ReplicaRecoveryClaim {
        expected_version: observed_version,
        head,
    }
}

pub fn refence_replica_export(
    vset: VsetId,
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
    let mut record = JournalRecord::decode(vset, record_bytes).map_err(|_| {
        ReplicaRecoveryError::CorruptObject {
            key: "recovery journal".to_owned(),
        }
    })?;
    record.fence = new_fence;
    let record_bytes = record.encode(vset);
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
        layout::journal_blob(vset, new_fence, record.seq),
        record_bytes.clone(),
    ));
    blobs.push((
        layout::journal_mirror_blob(vset, new_fence, record.seq),
        record_bytes,
    ));
    blobs.sort_by(|left, right| left.0.cmp(&right.0));
    let mut result = export.clone();
    result.info.writer_fence = new_fence;
    result.blobs = blobs;
    Ok(result)
}

pub fn report_replica_recovery(
    source: HostId,
    vset: VsetId,
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
        match inventory_replica(source, vset, residue) {
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
                residue.peer.0, residue.assignment_epoch
            )),
        }
    }
    if let Some(((covered, _), peer, epoch)) = best {
        report.chosen_source = Some(peer);
        report.chosen_assignment_epoch = Some(epoch);
        report.covered_sync_through = Some(covered);
    }
    match export_replica_recovery(source, vset, head, residues, store_objects) {
        Ok(export) => {
            report.status = ReplicaRecoveryStatus::Complete;
            report.chosen_source = Some(export.source_peer);
            report.chosen_assignment_epoch = Some(export.assignment_epoch);
            report.covered_sync_through = Some(export.sync_covered_through);
        }
        Err(ReplicaRecoveryError::MissingObject { key }) => report.missing_objects.push(key),
        Err(ReplicaRecoveryError::CorruptObject { key }) => report.corrupt_objects.push(key),
        Err(ReplicaRecoveryError::CorruptSpool { peer }) => {
            report.corrupt_objects.push(format!("peer:{}", peer.0));
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
    vset: VsetId,
    residue: ReplicaResidue<'_>,
) -> Result<ReplicaInventory, ReplicaRecoveryError> {
    let scan = scan_replica_spool(residue.bytes)
        .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
    verify_scan_identity(source, vset, residue, &scan)?;
    Ok(ReplicaInventory {
        peer: residue.peer,
        assignment_epoch: residue.assignment_epoch,
        committed: scan.commits.last().map(|commit| commit.info),
        valid_bytes: scan.valid_len,
        torn_tail: scan.truncated_tail,
    })
}

/// Verify and export the newest committed residue among only the active and
/// transition peers named by `head`. Store objects fill the portion of the
/// closure that the primary had already truthfully marked as S3-durable.
pub fn export_replica_recovery(
    source: HostId,
    vset: VsetId,
    head: &HeadRecord,
    residues: &[ReplicaResidue<'_>],
    store_objects: &BTreeMap<String, Vec<u8>>,
) -> Result<ReplicaExport, ReplicaRecoveryError> {
    let assignment = head.stash.ok_or(ReplicaRecoveryError::NoAssignment)?;
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
    let mut candidates = Vec::new();
    for &residue in residues {
        let Some(epochs) = allowed.get(&residue.peer) else {
            return Err(ReplicaRecoveryError::UnnamedPeer { peer: residue.peer });
        };
        if !epochs.contains(&residue.assignment_epoch) {
            return Err(ReplicaRecoveryError::StaleAssignment {
                peer: residue.peer,
                assignment_epoch: residue.assignment_epoch,
            });
        }
        let scan = scan_replica_spool(residue.bytes)
            .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
        verify_scan_identity(source, vset, residue, &scan)?;
        if let Some(commit) = scan.commits.last() {
            let record = JournalRecord::decode(vset, &commit.record)
                .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
            candidates.push((
                (
                    commit.info.sync_covered_through,
                    record.capture_seq,
                    commit.info.seq,
                ),
                residue,
                scan,
            ));
        }
    }
    candidates.sort_by_key(|(rank, _, _)| *rank);
    let Some((_, residue, scan)) = candidates.pop() else {
        return Err(ReplicaRecoveryError::NoCommit);
    };
    let commit = scan.commits.last().expect("candidate has commit");
    if let Some(ptr) = head.manifest {
        let key = layout::manifest_key(vset, ptr.fence, ptr.seq);
        let bytes = store_objects
            .get(&key)
            .ok_or_else(|| ReplicaRecoveryError::MissingObject { key: key.clone() })?;
        let store_record = JournalRecord::decode(vset, bytes)
            .map_err(|_| ReplicaRecoveryError::CorruptObject { key: key.clone() })?;
        if (
            store_record.fence,
            store_record.seq,
            store_record.capture_seq,
        ) != (ptr.fence, ptr.seq, ptr.capture_seq)
        {
            return Err(ReplicaRecoveryError::CorruptObject { key });
        }
        let peer_record = JournalRecord::decode(vset, &commit.record)
            .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
        let peer_rank = (
            commit.info.sync_covered_through,
            peer_record.capture_seq,
            commit.info.seq,
        );
        let store_rank = (
            store_record.sync_covered_through,
            store_record.capture_seq,
            store_record.seq,
        );
        if peer_rank < store_rank {
            return Err(ReplicaRecoveryError::StoreRecoveryPointNewer);
        }
    }
    export_commit(vset, residue, &scan, commit, store_objects)
}

fn verify_scan_identity(
    source: HostId,
    vset: VsetId,
    residue: ReplicaResidue<'_>,
    scan: &ReplicaSpoolScan,
) -> Result<(), ReplicaRecoveryError> {
    let identity_ok = scan.artifacts.values().all(|frame| {
        frame.source == source
            && frame.vset == vset
            && frame.assignment_epoch == residue.assignment_epoch
    }) && scan.commits.iter().all(|commit| {
        commit.source == source
            && commit.vset == vset
            && commit.assignment_epoch == residue.assignment_epoch
    });
    if identity_ok {
        Ok(())
    } else {
        Err(ReplicaRecoveryError::CorruptSpool { peer: residue.peer })
    }
}

fn export_commit(
    vset: VsetId,
    residue: ReplicaResidue<'_>,
    scan: &ReplicaSpoolScan,
    commit: &ReplicaCommitFrame,
    store_objects: &BTreeMap<String, Vec<u8>>,
) -> Result<ReplicaExport, ReplicaRecoveryError> {
    let record = JournalRecord::decode(vset, &commit.record)
        .map_err(|_| ReplicaRecoveryError::CorruptSpool { peer: residue.peer })?;
    let mut needed_segments: BTreeSet<(u64, crate::types::SegId)> = record
        .overlay
        .values()
        .filter(|(_, loc)| loc.base == 0)
        .map(|(_, loc)| (loc.fence, loc.seg))
        .collect();
    let mut blobs = BTreeMap::new();
    for ptr in record.leaves.values().filter(|ptr| ptr.base == 0) {
        let artifact = ReplicaArtifact::Leaf {
            fence: ptr.fence,
            id: ptr.id,
        };
        let key = layout::leaf_key(vset, ptr.fence, ptr.id);
        let bytes = artifact_bytes(scan, artifact, &key, store_objects)?;
        let leaf = MapLeaf::decode(vset, ptr.fence, ptr.id, &bytes)
            .map_err(|_| ReplicaRecoveryError::CorruptObject { key: key.clone() })?;
        needed_segments.extend(
            leaf.entries
                .iter()
                .filter(|(_, _, _, loc)| loc.base == 0)
                .map(|(_, _, _, loc)| (loc.fence, loc.seg)),
        );
        blobs.insert(layout::leaf_blob(vset, ptr.fence, ptr.id), bytes);
    }
    for (fence, seg) in needed_segments {
        let artifact = ReplicaArtifact::Segment { fence, seg };
        let key = layout::segment_key(vset, fence, seg);
        let bytes = artifact_bytes(scan, artifact, &key, store_objects)?;
        let intact = scan_segment(&bytes).is_ok_and(|(got_vset, got_fence, got_seg, _)| {
            (got_vset, got_fence, got_seg) == (vset, fence, seg)
        });
        if !intact {
            return Err(ReplicaRecoveryError::CorruptObject { key });
        }
        blobs.insert(layout::segment_blob(vset, fence, seg), bytes);
    }
    blobs.insert(
        layout::journal_blob(vset, record.fence, record.seq),
        commit.record.clone(),
    );
    blobs.insert(
        layout::journal_mirror_blob(vset, record.fence, record.seq),
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
    use crate::head::StashAssignment;
    use crate::journal::{DurabilityMode, RecordKind, VsetConfig};
    use crate::replica_spool::{seal_replica_artifact, seal_replica_commit};
    use crate::segment::SegmentBuilder;
    use crate::types::{Gen, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, page_size};

    #[test]
    #[allow(clippy::too_many_lines)]
    fn lost_primary_exports_a_complete_normal_local_recovery_set() {
        let source = HostId(0);
        let peer = HostId(1);
        let vset = VsetId(7);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(2),
        };
        let mut builder = SegmentBuilder::new(vset, 4, SegId(9));
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
            capture_seq: 11,
            sync_covered_through: info.sync_covered_through,
            overlay: locs
                .into_iter()
                .map(|(page, generation, loc)| (page, (generation, loc)))
                .collect(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        }
        .encode(vset);
        let mut spool =
            seal_replica_artifact(source, vset, 1, artifact, &segment).expect("artifact");
        spool.extend(
            seal_replica_commit(source, vset, 1, info, &[artifact], &record).expect("commit"),
        );
        let head = HeadRecord {
            vset,
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
            vset,
            &head,
            &[ReplicaResidue {
                peer,
                assignment_epoch: 1,
                bytes: &spool,
            }],
            &BTreeMap::new(),
        )
        .expect("complete peer residue recovers without S3 data");
        assert_eq!(export.source_peer, peer);
        assert_eq!(export.sync_covered_through, 12);
        let report = report_replica_recovery(
            source,
            vset,
            &head,
            &[ReplicaResidue {
                peer,
                assignment_epoch: 1,
                bytes: &spool,
            }],
            &BTreeMap::new(),
        );
        assert_eq!(report.status, ReplicaRecoveryStatus::Complete);
        assert!(report.human_summary().starts_with("complete:"));
        let claim = prepare_replica_recovery_claim(9, &head, HostId(3));
        assert_eq!(claim.expected_version, 9);
        assert_eq!(claim.head.holder, HostId(3));
        let refenced = refence_replica_export(vset, &export, 10).expect("fresh writer fence");
        let (_, journal) = refenced
            .blobs
            .iter()
            .find(|(name, _)| *name == layout::journal_blob(vset, 10, JournalSeq(8)))
            .expect("refenced journal");
        assert_eq!(
            JournalRecord::decode(vset, journal).expect("journal").fence,
            10
        );
        assert!(export.blobs.iter().any(|(name, bytes)| {
            name == &layout::segment_blob(vset, 4, SegId(9)) && bytes == &segment
        }));
        assert_eq!(
            export
                .blobs
                .iter()
                .filter(|(name, _)| {
                    name == &layout::journal_blob(vset, 4, JournalSeq(8))
                        || name == &layout::journal_mirror_blob(vset, 4, JournalSeq(8))
                })
                .count(),
            2
        );
    }

    #[test]
    fn recovery_never_scans_an_unnamed_peer() {
        let vset = VsetId(7);
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
                membership_epoch: 6,
            }),
            retired_stashes: Vec::new(),
        };
        assert_eq!(
            export_replica_recovery(
                HostId(0),
                vset,
                &head,
                &[ReplicaResidue {
                    peer: HostId(2),
                    assignment_epoch: 1,
                    bytes: &[],
                }],
                &BTreeMap::new(),
            ),
            Err(ReplicaRecoveryError::UnnamedPeer { peer: HostId(2) })
        );
    }

    #[test]
    fn recovery_refuses_peer_residue_older_than_the_store_manifest() {
        let source = HostId(0);
        let peer = HostId(1);
        let vset = VsetId(8);
        let config = VsetConfig {
            disk_volumes: 1,
            pages_per_volume: 8,
            durability: DurabilityMode::PeerStashed,
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
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        }
        .encode(vset);
        let spool = seal_replica_commit(source, vset, 1, peer_info, &[], &peer_record)
            .expect("valid older residue");

        let store_record = JournalRecord {
            config,
            seq: JournalSeq(9),
            fence: 4,
            kind: RecordKind::Commit,
            capture_seq: 13,
            sync_covered_through: 13,
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let head = HeadRecord {
            vset,
            holder: source,
            fence: 4,
            manifest: Some(crate::head::ManifestPtr {
                fence: store_record.fence,
                seq: store_record.seq,
                capture_seq: store_record.capture_seq,
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
            layout::manifest_key(vset, store_record.fence, store_record.seq),
            store_record.encode(vset),
        )]);

        assert!(
            export_replica_recovery(
                source,
                vset,
                &head,
                &[ReplicaResidue {
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
}
