//! Checksummed append-only passive-replica spool frames. A receiver appends
//! artifact frames followed by a commit footer; only a footer whose complete
//! required set appears earlier is a recovery point. Restart scans stop at an
//! invalid tail and never manufacture a commit from partial bytes.

use std::collections::{BTreeMap, BTreeSet};

use crate::format::{Dec, Enc, FRAME_HEADER, crc32c, open_frame, seal_frame};
use crate::journal::{DurabilityMode, JournalRecord};
use crate::mapleaf::MapLeaf;
use crate::replica_wire::{
    decode_artifact, decode_commit_info, encode_artifact, encode_commit_info,
};
use crate::seam::{ReplicaArtifact, ReplicaCommitInfo};
use crate::segment::scan_segment;
use crate::types::{HostId, VsetId};

pub const MAGIC_REPLICA_ARTIFACT: u32 = u32::from_le_bytes(*b"BRA1");
pub const MAGIC_REPLICA_COMMIT: u32 = u32::from_le_bytes(*b"BRC1");

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaArtifactFrame {
    pub source: HostId,
    pub vset: VsetId,
    pub assignment_epoch: u64,
    pub artifact: ReplicaArtifact,
    pub checksum: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaCommitFrame {
    pub source: HostId,
    pub vset: VsetId,
    pub assignment_epoch: u64,
    pub info: ReplicaCommitInfo,
    pub required: Vec<ReplicaArtifact>,
    pub record: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReplicaSpoolScan {
    pub valid_len: usize,
    pub truncated_tail: bool,
    pub artifacts: BTreeMap<ReplicaArtifact, ReplicaArtifactFrame>,
    /// Artifacts appended after the last commit footer that covered them.
    pub uncommitted_artifacts: BTreeSet<ReplicaArtifact>,
    pub commits: Vec<ReplicaCommitFrame>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReplicaSpoolError;

pub fn seal_replica_artifact(
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    artifact: ReplicaArtifact,
    bytes: &[u8],
) -> Result<Vec<u8>, ReplicaSpoolError> {
    let checksum = crc32c(bytes);
    seal_verified_replica_artifact(source, vset, assignment_epoch, artifact, checksum, bytes)
}

/// Verify an immutable artifact without constructing its spool frame. The
/// source uses this before network transfer so validation does not allocate
/// and checksum a full frame that will immediately be discarded.
pub fn verify_replica_artifact(
    vset: VsetId,
    artifact: ReplicaArtifact,
    bytes: &[u8],
) -> Result<(), ReplicaSpoolError> {
    verify_artifact(vset, artifact, bytes)
}

/// Seal an artifact whose transport checksum has already been computed.
/// Callers must still get full identity/frame validation here; only the
/// redundant second checksum pass is skipped.
pub fn seal_verified_replica_artifact(
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    artifact: ReplicaArtifact,
    checksum: u32,
    bytes: &[u8],
) -> Result<Vec<u8>, ReplicaSpoolError> {
    verify_artifact(vset, artifact, bytes)?;
    let mut e = Enc::new();
    e.u16(1);
    e.u16(source.0);
    e.u64(vset.0);
    e.u64(assignment_epoch);
    encode_artifact(&mut e, artifact);
    e.u32(checksum);
    e.u32(u32::try_from(bytes.len()).map_err(|_| ReplicaSpoolError)?);
    e.bytes(bytes);
    Ok(seal_frame(MAGIC_REPLICA_ARTIFACT, &e.finish()))
}

pub fn seal_replica_commit(
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
    required: &[ReplicaArtifact],
    record: &[u8],
) -> Result<Vec<u8>, ReplicaSpoolError> {
    if required.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ReplicaSpoolError);
    }
    verify_record(vset, info, record)?;
    let mut e = Enc::new();
    e.u16(1);
    e.u16(source.0);
    e.u64(vset.0);
    e.u64(assignment_epoch);
    encode_commit_info(&mut e, info);
    e.u32(u32::try_from(required.len()).map_err(|_| ReplicaSpoolError)?);
    for &artifact in required {
        encode_artifact(&mut e, artifact);
    }
    e.u32(u32::try_from(record.len()).map_err(|_| ReplicaSpoolError)?);
    e.bytes(record);
    Ok(seal_frame(MAGIC_REPLICA_COMMIT, &e.finish()))
}

pub fn scan_replica_spool(bytes: &[u8]) -> Result<ReplicaSpoolScan, ReplicaSpoolError> {
    let mut scan = ReplicaSpoolScan {
        valid_len: 0,
        truncated_tail: false,
        artifacts: BTreeMap::new(),
        uncommitted_artifacts: BTreeSet::new(),
        commits: Vec::new(),
    };
    let mut identity: Option<(HostId, VsetId, u64)> = None;
    while scan.valid_len < bytes.len() {
        let rest = &bytes[scan.valid_len..];
        let Some(frame_len) = framed_len(rest) else {
            scan.truncated_tail = true;
            break;
        };
        let frame = &rest[..frame_len];
        let magic = u32::from_le_bytes(frame[0..4].try_into().expect("four-byte magic"));
        match magic {
            MAGIC_REPLICA_ARTIFACT => {
                let artifact = open_artifact(frame)?;
                check_identity(
                    &mut identity,
                    artifact.source,
                    artifact.vset,
                    artifact.assignment_epoch,
                )?;
                match scan.artifacts.get(&artifact.artifact) {
                    None => {
                        scan.uncommitted_artifacts.insert(artifact.artifact);
                        scan.artifacts.insert(artifact.artifact, artifact);
                    }
                    Some(existing)
                        if existing.checksum == artifact.checksum
                            && existing.bytes == artifact.bytes => {}
                    Some(_) => return Err(ReplicaSpoolError),
                }
            }
            MAGIC_REPLICA_COMMIT => {
                let commit = open_commit(frame)?;
                check_identity(
                    &mut identity,
                    commit.source,
                    commit.vset,
                    commit.assignment_epoch,
                )?;
                if commit
                    .required
                    .iter()
                    .any(|artifact| !scan.artifacts.contains_key(artifact))
                {
                    return Err(ReplicaSpoolError);
                }
                if let Some(previous) = scan.commits.last()
                    && (commit.info.sync_covered_through, commit.info.seq.0)
                        < (previous.info.sync_covered_through, previous.info.seq.0)
                {
                    return Err(ReplicaSpoolError);
                }
                if scan.commits.last() != Some(&commit) {
                    for artifact in &commit.required {
                        scan.uncommitted_artifacts.remove(artifact);
                    }
                    scan.commits.push(commit);
                }
            }
            _ => {
                scan.truncated_tail = true;
                break;
            }
        }
        scan.valid_len += frame_len;
    }
    Ok(scan)
}

fn framed_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < FRAME_HEADER {
        return None;
    }
    let payload = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    let len = FRAME_HEADER.checked_add(usize::try_from(payload).ok()?)?;
    (len <= bytes.len()).then_some(len)
}

fn check_identity(
    identity: &mut Option<(HostId, VsetId, u64)>,
    source: HostId,
    vset: VsetId,
    assignment_epoch: u64,
) -> Result<(), ReplicaSpoolError> {
    let incoming = (source, vset, assignment_epoch);
    match identity {
        None => *identity = Some(incoming),
        Some(existing) if *existing == incoming => {}
        Some(_) => return Err(ReplicaSpoolError),
    }
    Ok(())
}

fn open_artifact(bytes: &[u8]) -> Result<ReplicaArtifactFrame, ReplicaSpoolError> {
    let payload = open_frame(MAGIC_REPLICA_ARTIFACT, bytes).map_err(|_| ReplicaSpoolError)?;
    let mut d = Dec::new(payload);
    if d.u16().map_err(|_| ReplicaSpoolError)? != 1 {
        return Err(ReplicaSpoolError);
    }
    let source = HostId(d.u16().map_err(|_| ReplicaSpoolError)?);
    let vset = VsetId(d.u64().map_err(|_| ReplicaSpoolError)?);
    let assignment_epoch = d.u64().map_err(|_| ReplicaSpoolError)?;
    let artifact = decode_artifact(&mut d).map_err(|_| ReplicaSpoolError)?;
    let checksum = d.u32().map_err(|_| ReplicaSpoolError)?;
    let len =
        usize::try_from(d.u32().map_err(|_| ReplicaSpoolError)?).map_err(|_| ReplicaSpoolError)?;
    let artifact_bytes = d.bytes(len).map_err(|_| ReplicaSpoolError)?.to_vec();
    d.finish().map_err(|_| ReplicaSpoolError)?;
    if crc32c(&artifact_bytes) != checksum {
        return Err(ReplicaSpoolError);
    }
    verify_artifact(vset, artifact, &artifact_bytes)?;
    Ok(ReplicaArtifactFrame {
        source,
        vset,
        assignment_epoch,
        artifact,
        checksum,
        bytes: artifact_bytes,
    })
}

fn open_commit(bytes: &[u8]) -> Result<ReplicaCommitFrame, ReplicaSpoolError> {
    let payload = open_frame(MAGIC_REPLICA_COMMIT, bytes).map_err(|_| ReplicaSpoolError)?;
    let mut d = Dec::new(payload);
    if d.u16().map_err(|_| ReplicaSpoolError)? != 1 {
        return Err(ReplicaSpoolError);
    }
    let source = HostId(d.u16().map_err(|_| ReplicaSpoolError)?);
    let vset = VsetId(d.u64().map_err(|_| ReplicaSpoolError)?);
    let assignment_epoch = d.u64().map_err(|_| ReplicaSpoolError)?;
    let info = decode_commit_info(&mut d).map_err(|_| ReplicaSpoolError)?;
    let count = d.u32().map_err(|_| ReplicaSpoolError)?;
    if count > 1_000_000 {
        return Err(ReplicaSpoolError);
    }
    let mut required = Vec::new();
    for _ in 0..count {
        required.push(decode_artifact(&mut d).map_err(|_| ReplicaSpoolError)?);
    }
    if required.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ReplicaSpoolError);
    }
    let len =
        usize::try_from(d.u32().map_err(|_| ReplicaSpoolError)?).map_err(|_| ReplicaSpoolError)?;
    let record = d.bytes(len).map_err(|_| ReplicaSpoolError)?.to_vec();
    d.finish().map_err(|_| ReplicaSpoolError)?;
    verify_record(vset, info, &record)?;
    Ok(ReplicaCommitFrame {
        source,
        vset,
        assignment_epoch,
        info,
        required,
        record,
    })
}

fn verify_artifact(
    vset: VsetId,
    artifact: ReplicaArtifact,
    bytes: &[u8],
) -> Result<(), ReplicaSpoolError> {
    match artifact {
        ReplicaArtifact::Segment { fence, seg } => {
            let (got_vset, got_fence, got_seg, _) =
                scan_segment(bytes).map_err(|_| ReplicaSpoolError)?;
            if (got_vset, got_fence, got_seg) != (vset, fence, seg) {
                return Err(ReplicaSpoolError);
            }
        }
        ReplicaArtifact::Leaf { fence, id } => {
            MapLeaf::decode(vset, fence, id, bytes).map_err(|_| ReplicaSpoolError)?;
        }
    }
    Ok(())
}

fn verify_record(
    vset: VsetId,
    info: ReplicaCommitInfo,
    bytes: &[u8],
) -> Result<(), ReplicaSpoolError> {
    let record = JournalRecord::decode(vset, bytes).map_err(|_| ReplicaSpoolError)?;
    if record.config.durability != DurabilityMode::PeerStashed
        || record.fence != info.writer_fence
        || record.seq != info.seq
        || record.sync_covered_through != info.sync_covered_through
    {
        return Err(ReplicaSpoolError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::journal::{RecordKind, VsetConfig};
    use crate::segment::SegmentBuilder;
    use crate::types::{Gen, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, page_size};

    fn fixture() -> (ReplicaArtifact, Vec<u8>, ReplicaCommitInfo, Vec<u8>) {
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
                kind: crate::journal::VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 16,
                durability: DurabilityMode::PeerStashed,
            },
            seq: info.seq,
            fence: info.writer_fence,
            kind: RecordKind::Commit,
            capture_seq: 12,
            sync_covered_through: info.sync_covered_through,
            database: crate::journal::DatabaseMeta::default(),
            overlay: BTreeMap::from([(page, (Gen(3), locs[0].2))]),
            leaves: BTreeMap::new(),
            migrated_from: None,
        }
        .encode(vset);
        (artifact, segment, info, record)
    }

    #[test]
    fn complete_spool_scans_to_one_recovery_commit() {
        let (artifact, segment, info, record) = fixture();
        let mut spool = seal_replica_artifact(HostId(2), VsetId(7), 5, artifact, &segment)
            .expect("artifact valid");
        spool.extend(
            seal_replica_commit(HostId(2), VsetId(7), 5, info, &[artifact], &record)
                .expect("commit valid"),
        );
        let scan = scan_replica_spool(&spool).expect("spool valid");
        assert_eq!(scan.valid_len, spool.len());
        assert!(!scan.truncated_tail);
        assert_eq!(scan.artifacts.len(), 1);
        assert!(scan.uncommitted_artifacts.is_empty());
        assert_eq!(scan.commits.len(), 1);
        assert_eq!(scan.commits[0].info, info);
    }

    #[test]
    fn commit_spool_frame_bytes_are_pinned() {
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
                kind: crate::journal::VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 16,
                durability: DurabilityMode::PeerStashed,
            },
            seq: info.seq,
            fence: info.writer_fence,
            kind: RecordKind::Commit,
            capture_seq: 12,
            sync_covered_through: info.sync_covered_through,
            database: crate::journal::DatabaseMeta::default(),
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        }
        .encode(VsetId(7));
        let frame = seal_replica_commit(HostId(2), VsetId(7), 5, info, &[artifact], &record)
            .expect("commit valid");
        let expected: &[u8] = match page_size() {
            4096 => &[
                66, 82, 67, 49, 162, 0, 0, 0, 153, 125, 153, 154, 1, 0, 2, 0, 7, 0, 0, 0, 0, 0, 0,
                0, 5, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 12, 0,
                0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0,
                93, 0, 0, 0, 66, 74, 82, 49, 81, 0, 0, 0, 186, 146, 237, 236, 6, 0, 0, 16, 0, 0, 7,
                0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 1, 16, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            16_384 => &[
                66, 82, 67, 49, 162, 0, 0, 0, 248, 103, 143, 117, 1, 0, 2, 0, 7, 0, 0, 0, 0, 0, 0,
                0, 5, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 12, 0,
                0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0,
                93, 0, 0, 0, 66, 74, 82, 49, 81, 0, 0, 0, 126, 13, 221, 201, 6, 0, 0, 64, 0, 0, 7,
                0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 1, 16, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            size => panic!("spool frame pin missing for {size}-byte pages"),
        };
        assert_eq!(frame, expected);
    }

    #[test]
    fn scan_retains_artifacts_appended_after_the_last_commit() {
        let (artifact, segment, info, record) = fixture();
        let mut spool = seal_replica_artifact(HostId(2), VsetId(7), 5, artifact, &segment).unwrap();
        spool.extend(
            seal_replica_commit(HostId(2), VsetId(7), 5, info, &[artifact], &record).unwrap(),
        );

        let page = PageId {
            volume: VolumeId {
                vset: VsetId(7),
                idx: VolumeIdx(1),
            },
            page: PageNo(3),
        };
        let mut builder = SegmentBuilder::new(VsetId(7), 4, SegId(10));
        builder.add(page, Gen(4), &vec![0x5A; page_size()]);
        let (next_segment, _) = builder.finish();
        let next = ReplicaArtifact::Segment {
            fence: 4,
            seg: SegId(10),
        };
        spool.extend(seal_replica_artifact(HostId(2), VsetId(7), 5, next, &next_segment).unwrap());

        let scan = scan_replica_spool(&spool).expect("spool valid");
        assert_eq!(scan.commits.len(), 1);
        assert_eq!(scan.uncommitted_artifacts, BTreeSet::from([next]));
    }

    #[test]
    fn every_torn_commit_tail_is_ignored() {
        let (artifact, segment, info, record) = fixture();
        let artifact_frame =
            seal_replica_artifact(HostId(2), VsetId(7), 5, artifact, &segment).unwrap();
        let commit =
            seal_replica_commit(HostId(2), VsetId(7), 5, info, &[artifact], &record).unwrap();
        for kept in 0..commit.len() {
            let mut torn = artifact_frame.clone();
            torn.extend_from_slice(&commit[..kept]);
            let scan = scan_replica_spool(&torn).expect("torn tail is recoverable");
            assert!(scan.commits.is_empty());
            assert_eq!(scan.valid_len, artifact_frame.len());
            if kept > 0 {
                assert!(scan.truncated_tail);
            }
        }
    }

    #[test]
    fn every_torn_artifact_tail_is_ignored() {
        let (artifact, segment, _, _) = fixture();
        let frame = seal_replica_artifact(HostId(2), VsetId(7), 5, artifact, &segment).unwrap();
        for kept in 0..frame.len() {
            let scan = scan_replica_spool(&frame[..kept]).expect("torn tail is recoverable");
            assert!(scan.artifacts.is_empty());
            assert_eq!(scan.valid_len, 0);
            if kept > 0 {
                assert!(scan.truncated_tail);
            }
        }
    }

    #[test]
    fn commit_before_required_data_is_rejected() {
        let (artifact, _, info, record) = fixture();
        let commit =
            seal_replica_commit(HostId(2), VsetId(7), 5, info, &[artifact], &record).unwrap();
        assert_eq!(scan_replica_spool(&commit), Err(ReplicaSpoolError));
    }

    #[test]
    fn conflicting_duplicate_artifact_is_rejected() {
        let (artifact, segment, _, _) = fixture();
        let frame = seal_replica_artifact(HostId(2), VsetId(7), 5, artifact, &segment).unwrap();
        let mut conflicting_segment = segment;
        let last = conflicting_segment.len() - 1;
        conflicting_segment[last] ^= 1;
        assert!(
            seal_replica_artifact(HostId(2), VsetId(7), 5, artifact, &conflicting_segment).is_err()
        );
        let mut duplicate = frame.clone();
        duplicate.extend(frame);
        let scan = scan_replica_spool(&duplicate).expect("identical duplicate is harmless");
        assert_eq!(scan.artifacts.len(), 1);
    }

    #[test]
    fn every_single_bit_spool_corruption_removes_the_recovery_commit() {
        let (artifact, segment, info, record) = fixture();
        let mut spool = seal_replica_artifact(HostId(2), VsetId(7), 5, artifact, &segment).unwrap();
        spool.extend(
            seal_replica_commit(HostId(2), VsetId(7), 5, info, &[artifact], &record).unwrap(),
        );
        for bit in 0..spool.len() * 8 {
            let mut damaged = spool.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            if let Ok(scan) = scan_replica_spool(&damaged) {
                assert!(
                    scan.commits.is_empty(),
                    "bit {bit} left corrupt residue recoverable"
                );
            }
        }
    }
}
