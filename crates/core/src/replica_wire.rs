use crate::format::{Dec, DecodeError, Enc};
use crate::seam::{ReplicaArtifact, ReplicaCommitInfo};
use crate::types::{JournalSeq, SegId};

pub(crate) fn encode_artifact(e: &mut Enc, artifact: ReplicaArtifact) {
    match artifact {
        ReplicaArtifact::Segment { fence, seg } => {
            e.u8(0);
            e.u64(fence);
            e.u64(seg.0);
        }
        ReplicaArtifact::Leaf { fence, id } => {
            e.u8(1);
            e.u64(fence);
            e.u64(id);
        }
    }
}

pub(crate) fn decode_artifact(d: &mut Dec<'_>) -> Result<ReplicaArtifact, DecodeError> {
    match d.u8()? {
        0 => Ok(ReplicaArtifact::Segment {
            fence: d.u64()?,
            seg: SegId(d.u64()?),
        }),
        1 => Ok(ReplicaArtifact::Leaf {
            fence: d.u64()?,
            id: d.u64()?,
        }),
        _ => Err(DecodeError),
    }
}

pub(crate) fn encode_commit_info(e: &mut Enc, info: ReplicaCommitInfo) {
    e.u64(info.writer_fence);
    e.u64(info.seq.0);
    e.u64(info.sync_covered_through);
}

pub(crate) fn decode_commit_info(d: &mut Dec<'_>) -> Result<ReplicaCommitInfo, DecodeError> {
    Ok(ReplicaCommitInfo {
        writer_fence: d.u64()?,
        seq: JournalSeq(d.u64()?),
        sync_covered_through: d.u64()?,
    })
}
