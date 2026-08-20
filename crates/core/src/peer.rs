//! The peer protocol's wire form (R11.1's payload layer): one checksummed
//! frame per [`PeerMsg`], carrying the sender and intended recipient so a transport
//! needs no handshake state. The channel contract is at-least-once — the
//! daemon's retry timers re-drive anything a transport drops — so a frame
//! that fails verification is simply discarded, never repaired.
//!
//! The standard integrity frame contains one bounded canonical protobuf
//! envelope: authenticated sender, intended recipient, the stable protocol
//! discriminant, and a typed oneof body. Embedded blobs (records and BLX entries) are
//! already framed and verified by their consumers. Inline VMM bytes are
//! protected by the peer frame and checked against the offered record before
//! use (R8.1: the reader decides).

use prost::Message;

use crate::format::{DecodeError, open_frame, seal_frame};
use crate::protocol::{
    MAX_OBJECT_BYTES, PeerMsg, PeerRequestId, ReplicaArtifact, ReplicaCommitInfo,
};
use crate::types::{HostId, JournalSeq, ObjectId, VolumeId};

pub const MAGIC_PEER: u32 = u32::from_le_bytes(*b"BPM1");

/// Frame payload cap for transports: R4.6's 64 MiB object cap bounds every
/// embedded blob, so anything larger is a desynced or hostile stream.
pub const MAX_PEER_PAYLOAD: u32 = MAX_OBJECT_BYTES;
const PEER_FORMAT_VERSION: u32 = 1;

#[derive(Clone, PartialEq, Message)]
struct PeerEnvelopeWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint32, tag = "2")]
    from: u32,
    #[prost(uint32, tag = "3")]
    to: u32,
    /// Existing peer discriminant. Its numeric values remain stable even
    /// though the body is now a protobuf oneof.
    #[prost(uint32, tag = "4")]
    kind: u32,
    #[prost(
        oneof = "peer_envelope_wire::Body",
        tags = "20, 21, 22, 23, 26, 27, 28, 29, 30, 31, 32, 33, 35, 36"
    )]
    body: Option<peer_envelope_wire::Body>,
}

mod peer_envelope_wire {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Body {
        #[prost(message, tag = "20")]
        MigrateOffer(super::MigrateOfferWire),
        #[prost(message, tag = "21")]
        MigrateAccept(super::VolumeFenceWire),
        #[prost(message, tag = "22")]
        FetchRange(super::FetchRangeWire),
        #[prost(message, tag = "23")]
        Page(super::PageWire),
        #[prost(message, tag = "26")]
        Released(super::VolumeFenceWire),
        #[prost(message, tag = "27")]
        ReleasedAck(super::VolumeFenceWire),
        #[prost(message, tag = "28")]
        ReplicaPut(super::ReplicaPutWire),
        #[prost(message, tag = "29")]
        ReplicaPutAck(super::ReplicaPutAckWire),
        #[prost(message, tag = "30")]
        ReplicaCommit(super::ReplicaCommitWire),
        #[prost(message, tag = "31")]
        ReplicaCommitAck(super::ReplicaCommitAckWire),
        #[prost(message, tag = "32")]
        ReplicaStatus(super::ReplicaStatusWire),
        #[prost(message, tag = "33")]
        ReplicaStatusReply(super::ReplicaStatusReplyWire),
        #[prost(message, tag = "35")]
        ReplicaRelease(super::ReplicaReleaseWire),
        #[prost(message, tag = "36")]
        ReplicaReleaseAck(super::ReplicaReleaseWire),
    }
}

#[derive(Clone, PartialEq, Message)]
struct MigrateOfferWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(bytes = "vec", tag = "2")]
    record: Vec<u8>,
    #[prost(bytes = "vec", optional, tag = "3")]
    vmstate: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct VolumeFenceWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    fence: u64,
}

#[derive(Clone, PartialEq, Message)]
struct FetchRangeWire {
    #[prost(uint64, tag = "1")]
    io: u64,
    #[prost(uint64, tag = "2")]
    volume: u64,
    #[prost(uint64, optional, tag = "3")]
    replica_assignment_epoch: Option<u64>,
    #[prost(uint64, tag = "4")]
    fence: u64,
    #[prost(uint64, tag = "5")]
    object: u64,
    #[prost(uint32, tag = "6")]
    offset: u32,
    #[prost(uint32, tag = "7")]
    len: u32,
}

#[derive(Clone, PartialEq, Message)]
struct PageWire {
    #[prost(uint64, tag = "1")]
    io: u64,
    #[prost(bytes = "vec", optional, tag = "2")]
    bytes: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaArtifactWire {
    #[prost(oneof = "replica_artifact_wire::Artifact", tags = "1")]
    artifact: Option<replica_artifact_wire::Artifact>,
}

mod replica_artifact_wire {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Artifact {
        #[prost(message, tag = "1")]
        Blx(super::BlxArtifactWire),
    }
}

#[derive(Clone, PartialEq, Message)]
struct BlxArtifactWire {
    #[prost(uint64, tag = "1")]
    fence: u64,
    #[prost(uint64, tag = "2")]
    object: u64,
}

#[derive(Clone, Copy, PartialEq, Message)]
struct ReplicaCommitInfoWire {
    #[prost(uint64, tag = "1")]
    writer_fence: u64,
    #[prost(uint64, tag = "2")]
    seq: u64,
    #[prost(uint64, tag = "3")]
    sync_covered_through: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaPutWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    assignment_epoch: u64,
    #[prost(message, optional, tag = "3")]
    artifact: Option<ReplicaArtifactWire>,
    #[prost(uint32, tag = "4")]
    checksum: u32,
    #[prost(bytes = "vec", tag = "5")]
    bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaPutAckWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    assignment_epoch: u64,
    #[prost(message, optional, tag = "3")]
    artifact: Option<ReplicaArtifactWire>,
    #[prost(uint32, tag = "4")]
    checksum: u32,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaCommitWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    assignment_epoch: u64,
    #[prost(message, optional, tag = "3")]
    info: Option<ReplicaCommitInfoWire>,
    #[prost(message, repeated, tag = "4")]
    required: Vec<ReplicaArtifactWire>,
    #[prost(bytes = "vec", tag = "5")]
    record: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaCommitAckWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    assignment_epoch: u64,
    #[prost(message, optional, tag = "3")]
    info: Option<ReplicaCommitInfoWire>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaStatusWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    assignment_epoch: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaStatusReplyWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    assignment_epoch: u64,
    #[prost(message, optional, tag = "3")]
    committed: Option<ReplicaCommitInfoWire>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaReleaseWire {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    assignment_epoch: u64,
    #[prost(message, optional, tag = "3")]
    through: Option<ReplicaCommitInfoWire>,
}

fn artifact_to_wire(artifact: ReplicaArtifact) -> ReplicaArtifactWire {
    let ReplicaArtifact::Blx { fence, object } = artifact;
    ReplicaArtifactWire {
        artifact: Some(replica_artifact_wire::Artifact::Blx(BlxArtifactWire {
            fence,
            object: object.0,
        })),
    }
}

fn artifact_from_wire(wire: ReplicaArtifactWire) -> Result<ReplicaArtifact, DecodeError> {
    match wire.artifact.ok_or(DecodeError)? {
        replica_artifact_wire::Artifact::Blx(blx) => Ok(ReplicaArtifact::Blx {
            fence: blx.fence,
            object: ObjectId(blx.object),
        }),
    }
}

fn commit_info_to_wire(info: ReplicaCommitInfo) -> ReplicaCommitInfoWire {
    ReplicaCommitInfoWire {
        writer_fence: info.writer_fence,
        seq: info.seq.0,
        sync_covered_through: info.sync_covered_through,
    }
}

fn commit_info_from_wire(wire: ReplicaCommitInfoWire) -> ReplicaCommitInfo {
    ReplicaCommitInfo {
        writer_fence: wire.writer_fence,
        seq: JournalSeq(wire.seq),
        sync_covered_through: wire.sync_covered_through,
    }
}

/// Encode one message as a sealed frame carrying the sender's identity.
pub fn encode_peer(from: HostId, msg: &PeerMsg) -> Vec<u8> {
    encode_peer_routed(from, from, msg)
}

/// Encode authenticated sender and intended recipient host IDs.
pub fn encode_peer_routed(from: HostId, to: HostId, msg: &PeerMsg) -> Vec<u8> {
    let payload = PeerEnvelopeWire {
        version: PEER_FORMAT_VERSION,
        from: from.get(),
        to: to.get(),
        kind: u32::from(msg.tag()),
        body: Some(peer_body_to_wire(msg)),
    }
    .encode_to_vec();
    assert!(
        payload.len() <= MAX_PEER_PAYLOAD as usize,
        "peer payload exceeds protocol cap"
    );
    seal_frame(MAGIC_PEER, &payload)
}

fn peer_body_to_wire(msg: &PeerMsg) -> peer_envelope_wire::Body {
    use peer_envelope_wire::Body;

    match msg {
        PeerMsg::MigrateOffer {
            volume,
            record,
            vmstate,
        } => Body::MigrateOffer(MigrateOfferWire {
            volume: volume.0,
            record: record.clone(),
            vmstate: vmstate.clone(),
        }),
        PeerMsg::MigrateAccept {
            volume,
            offer_fence,
        } => Body::MigrateAccept(VolumeFenceWire {
            volume: volume.0,
            fence: *offer_fence,
        }),
        PeerMsg::FetchRange {
            io,
            volume,
            replica_assignment_epoch,
            fence,
            object,
            offset,
            len,
        } => Body::FetchRange(FetchRangeWire {
            io: io.0,
            volume: volume.0,
            replica_assignment_epoch: *replica_assignment_epoch,
            fence: *fence,
            object: object.0,
            offset: *offset,
            len: *len,
        }),
        PeerMsg::Page { io, bytes } => Body::Page(PageWire {
            io: io.0,
            bytes: bytes.clone(),
        }),
        PeerMsg::Released {
            volume,
            release_fence,
        } => Body::Released(VolumeFenceWire {
            volume: volume.0,
            fence: *release_fence,
        }),
        PeerMsg::ReleasedAck {
            volume,
            release_fence,
        } => Body::ReleasedAck(VolumeFenceWire {
            volume: volume.0,
            fence: *release_fence,
        }),
        _ => replica_body_to_wire(msg),
    }
}

fn replica_body_to_wire(msg: &PeerMsg) -> peer_envelope_wire::Body {
    use peer_envelope_wire::Body;

    match msg {
        PeerMsg::ReplicaPut {
            volume,
            assignment_epoch,
            artifact: id,
            checksum,
            bytes,
        } => Body::ReplicaPut(ReplicaPutWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
            artifact: Some(artifact_to_wire(*id)),
            checksum: *checksum,
            bytes: bytes.clone(),
        }),
        PeerMsg::ReplicaPutAck {
            volume,
            assignment_epoch,
            artifact: id,
            checksum,
        } => Body::ReplicaPutAck(ReplicaPutAckWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
            artifact: Some(artifact_to_wire(*id)),
            checksum: *checksum,
        }),
        PeerMsg::ReplicaCommit {
            volume,
            assignment_epoch,
            info,
            required,
            record,
        } => Body::ReplicaCommit(ReplicaCommitWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
            info: Some(commit_info_to_wire(*info)),
            required: required.iter().copied().map(artifact_to_wire).collect(),
            record: record.clone(),
        }),
        PeerMsg::ReplicaCommitAck {
            volume,
            assignment_epoch,
            info,
        } => Body::ReplicaCommitAck(ReplicaCommitAckWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
            info: Some(commit_info_to_wire(*info)),
        }),
        PeerMsg::ReplicaStatus {
            volume,
            assignment_epoch,
        } => Body::ReplicaStatus(ReplicaStatusWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
        }),
        PeerMsg::ReplicaStatusReply {
            volume,
            assignment_epoch,
            committed,
        } => Body::ReplicaStatusReply(ReplicaStatusReplyWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
            committed: committed.map(commit_info_to_wire),
        }),
        PeerMsg::ReplicaRelease {
            volume,
            assignment_epoch,
            through,
        } => Body::ReplicaRelease(ReplicaReleaseWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
            through: Some(commit_info_to_wire(*through)),
        }),
        PeerMsg::ReplicaReleaseAck {
            volume,
            assignment_epoch,
            through,
        } => Body::ReplicaReleaseAck(ReplicaReleaseWire {
            volume: volume.0,
            assignment_epoch: *assignment_epoch,
            through: Some(commit_info_to_wire(*through)),
        }),
        PeerMsg::MigrateOffer { .. }
        | PeerMsg::MigrateAccept { .. }
        | PeerMsg::FetchRange { .. }
        | PeerMsg::Page { .. }
        | PeerMsg::Released { .. }
        | PeerMsg::ReleasedAck { .. } => unreachable!("migration message handled above"),
    }
}

/// Verify and decode one frame. Any damage, unknown discriminant, or trailing
/// bytes is one answer: corrupt — the transport drops the frame (and typically
/// the connection) and the retry timers re-drive.
pub fn decode_peer(bytes: &[u8]) -> Result<(HostId, PeerMsg), DecodeError> {
    decode_peer_routed(bytes).map(|(from, _, message)| (from, message))
}

/// Decode sender, intended recipient, and payload.
pub fn decode_peer_routed(bytes: &[u8]) -> Result<(HostId, HostId, PeerMsg), DecodeError> {
    if bytes.len() > crate::format::FRAME_HEADER + MAX_PEER_PAYLOAD as usize {
        return Err(DecodeError);
    }
    let payload = open_frame(MAGIC_PEER, bytes)?;
    if payload.len() > MAX_PEER_PAYLOAD as usize {
        return Err(DecodeError);
    }
    let wire = PeerEnvelopeWire::decode(payload).map_err(|_| DecodeError)?;
    if wire.version != PEER_FORMAT_VERSION || wire.encode_to_vec() != payload {
        return Err(DecodeError);
    }
    let from = HostId::new(wire.from);
    let to = HostId::new(wire.to);
    let body = wire.body.ok_or(DecodeError)?;
    let msg = peer_message_from_wire(wire.kind, body)?;
    Ok((from, to, msg))
}

#[allow(clippy::too_many_lines)]
fn peer_message_from_wire(
    kind: u32,
    body: peer_envelope_wire::Body,
) -> Result<PeerMsg, DecodeError> {
    use peer_envelope_wire::Body;

    let message = match (kind, body) {
        (0, Body::MigrateOffer(wire)) => PeerMsg::MigrateOffer {
            volume: VolumeId(wire.volume),
            record: wire.record,
            vmstate: wire.vmstate,
        },
        (1, Body::MigrateAccept(wire)) => PeerMsg::MigrateAccept {
            volume: VolumeId(wire.volume),
            offer_fence: wire.fence,
        },
        (2, Body::FetchRange(wire)) => PeerMsg::FetchRange {
            io: PeerRequestId(wire.io),
            volume: VolumeId(wire.volume),
            replica_assignment_epoch: wire.replica_assignment_epoch,
            fence: wire.fence,
            object: ObjectId(wire.object),
            offset: wire.offset,
            len: wire.len,
        },
        (3, Body::Page(wire)) => PeerMsg::Page {
            io: PeerRequestId(wire.io),
            bytes: wire.bytes,
        },
        (6, Body::Released(wire)) => PeerMsg::Released {
            volume: VolumeId(wire.volume),
            release_fence: wire.fence,
        },
        (7, Body::ReleasedAck(wire)) => PeerMsg::ReleasedAck {
            volume: VolumeId(wire.volume),
            release_fence: wire.fence,
        },
        (8, Body::ReplicaPut(wire)) => PeerMsg::ReplicaPut {
            volume: VolumeId(wire.volume),
            assignment_epoch: wire.assignment_epoch,
            artifact: artifact_from_wire(wire.artifact.ok_or(DecodeError)?)?,
            checksum: wire.checksum,
            bytes: wire.bytes,
        },
        (9, Body::ReplicaPutAck(wire)) => PeerMsg::ReplicaPutAck {
            volume: VolumeId(wire.volume),
            assignment_epoch: wire.assignment_epoch,
            artifact: artifact_from_wire(wire.artifact.ok_or(DecodeError)?)?,
            checksum: wire.checksum,
        },
        (10, Body::ReplicaCommit(wire)) => {
            if wire.required.len() > 1_000_000 {
                return Err(DecodeError);
            }
            PeerMsg::ReplicaCommit {
                volume: VolumeId(wire.volume),
                assignment_epoch: wire.assignment_epoch,
                info: commit_info_from_wire(wire.info.ok_or(DecodeError)?),
                required: wire
                    .required
                    .into_iter()
                    .map(artifact_from_wire)
                    .collect::<Result<_, _>>()?,
                record: wire.record,
            }
        }
        (11, Body::ReplicaCommitAck(wire)) => PeerMsg::ReplicaCommitAck {
            volume: VolumeId(wire.volume),
            assignment_epoch: wire.assignment_epoch,
            info: commit_info_from_wire(wire.info.ok_or(DecodeError)?),
        },
        (12, Body::ReplicaStatus(wire)) => PeerMsg::ReplicaStatus {
            volume: VolumeId(wire.volume),
            assignment_epoch: wire.assignment_epoch,
        },
        (13, Body::ReplicaStatusReply(wire)) => PeerMsg::ReplicaStatusReply {
            volume: VolumeId(wire.volume),
            assignment_epoch: wire.assignment_epoch,
            committed: wire.committed.map(commit_info_from_wire),
        },
        (15, Body::ReplicaRelease(wire)) => PeerMsg::ReplicaRelease {
            volume: VolumeId(wire.volume),
            assignment_epoch: wire.assignment_epoch,
            through: commit_info_from_wire(wire.through.ok_or(DecodeError)?),
        },
        (16, Body::ReplicaReleaseAck(wire)) => PeerMsg::ReplicaReleaseAck {
            volume: VolumeId(wire.volume),
            assignment_epoch: wire.assignment_epoch,
            through: commit_info_from_wire(wire.through.ok_or(DecodeError)?),
        },
        _ => return Err(DecodeError),
    };
    Ok(message)
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::format::crc32c;
    use crate::protocol::{ReplicaArtifact, ReplicaCommitInfo};
    use crate::types::JournalSeq;

    #[allow(clippy::too_many_lines)]
    fn samples() -> Vec<PeerMsg> {
        let blx = ReplicaArtifact::Blx {
            fence: 4,
            object: ObjectId(12),
        };
        let info = ReplicaCommitInfo {
            writer_fence: 4,
            seq: JournalSeq(13),
            sync_covered_through: 99,
        };
        vec![
            PeerMsg::MigrateOffer {
                volume: VolumeId(7),
                record: vec![0xAB; 17],
                vmstate: Some(vec![0xCD; 9]),
            },
            PeerMsg::MigrateAccept {
                volume: VolumeId(7),
                offer_fence: 0,
            },
            PeerMsg::FetchRange {
                io: PeerRequestId(99),
                volume: VolumeId(7),
                replica_assignment_epoch: Some(8),
                fence: 3,
                object: ObjectId(12),
                offset: 4096,
                len: 640,
            },
            PeerMsg::Page {
                io: PeerRequestId(99),
                bytes: Some(vec![0x5A; 640]),
            },
            PeerMsg::Page {
                io: PeerRequestId(100),
                bytes: None,
            },
            PeerMsg::Released {
                volume: VolumeId(7),
                release_fence: 3,
            },
            PeerMsg::ReleasedAck {
                volume: VolumeId(7),
                release_fence: 3,
            },
            PeerMsg::ReplicaPut {
                volume: VolumeId(7),
                assignment_epoch: 3,
                artifact: blx,
                checksum: 0xAABB_CCDD,
                bytes: vec![0x5A; 31],
            },
            PeerMsg::ReplicaPutAck {
                volume: VolumeId(7),
                assignment_epoch: 3,
                artifact: blx,
                checksum: 0xAABB_CCDD,
            },
            PeerMsg::ReplicaCommit {
                volume: VolumeId(7),
                assignment_epoch: 3,
                info,
                required: vec![blx],
                record: vec![0xC3; 27],
            },
            PeerMsg::ReplicaCommitAck {
                volume: VolumeId(7),
                assignment_epoch: 3,
                info,
            },
            PeerMsg::ReplicaStatus {
                volume: VolumeId(7),
                assignment_epoch: 3,
            },
            PeerMsg::ReplicaStatusReply {
                volume: VolumeId(7),
                assignment_epoch: 3,
                committed: None,
            },
            PeerMsg::ReplicaStatusReply {
                volume: VolumeId(7),
                assignment_epoch: 3,
                committed: Some(info),
            },
            PeerMsg::ReplicaRelease {
                volume: VolumeId(7),
                assignment_epoch: 3,
                through: info,
            },
            PeerMsg::ReplicaReleaseAck {
                volume: VolumeId(7),
                assignment_epoch: 3,
                through: info,
            },
        ]
    }

    #[derive(Clone, PartialEq, Message)]
    struct PeerEnvelopeProbe {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(uint32, tag = "2")]
        from: u32,
        #[prost(uint32, tag = "3")]
        to: u32,
        #[prost(uint32, tag = "4")]
        kind: u32,
        #[prost(oneof = "peer_envelope_probe::Body", tags = "20")]
        body: Option<peer_envelope_probe::Body>,
    }

    mod peer_envelope_probe {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum Body {
            #[prost(message, tag = "20")]
            MigrateOffer(super::MigrateOfferProbe),
        }
    }

    #[derive(Clone, PartialEq, Message)]
    struct MigrateOfferProbe {
        #[prost(uint64, tag = "1")]
        volume: u64,
        #[prost(bytes = "vec", tag = "2")]
        record: Vec<u8>,
        #[prost(bytes = "vec", optional, tag = "3")]
        vmstate: Option<Vec<u8>>,
    }

    #[test]
    fn peer_control_frame_is_a_routed_protobuf_envelope() {
        let message = PeerMsg::MigrateOffer {
            volume: VolumeId(7),
            record: vec![1, 2, 3],
            vmstate: Some(vec![4, 5]),
        };
        let encoded = encode_peer_routed(HostId::new(2), HostId::new(9), &message);
        let payload = open_frame(MAGIC_PEER, &encoded).expect("peer frame");
        let envelope = PeerEnvelopeProbe::decode(payload).expect("peer protobuf");

        assert_eq!(
            (envelope.version, envelope.from, envelope.to, envelope.kind),
            (1, 2, 9, 0)
        );
        assert!(matches!(
            envelope.body,
            Some(peer_envelope_probe::Body::MigrateOffer(MigrateOfferProbe {
                volume: 7,
                record,
                vmstate: Some(vmstate),
            })) if record == [1, 2, 3] && vmstate == [4, 5]
        ));
    }

    #[test]
    fn every_variant_round_trips_with_its_sender() {
        for msg in samples() {
            let framed = encode_peer(HostId::new(2), &msg);
            assert_eq!(decode_peer(&framed), Ok((HostId::new(2), msg)));
        }
    }

    #[test]
    fn frames_are_byte_pinned() {
        // The concatenation of every sample frame pins the whole layout
        // (R10.2): any encoding change must be seen and decided.
        let bytes: Vec<u8> = samples()
            .iter()
            .flat_map(|msg| encode_peer(HostId::new(2), msg))
            .collect();
        assert_eq!(bytes.len(), 1246);
        assert_eq!(crc32c(&bytes), 0x6FA0_F52E);
    }

    #[test]
    fn any_single_bit_flip_of_every_variant_is_rejected() {
        for (variant, message) in samples().iter().enumerate() {
            let framed = encode_peer(HostId::new(1), message);
            for bit in 0..framed.len() * 8 {
                let mut damaged = framed.clone();
                damaged[bit / 8] ^= 1 << (bit % 8);
                assert!(
                    decode_peer(&damaged).is_err(),
                    "variant {variant} flip of bit {bit} went undetected"
                );
            }
        }
    }

    #[test]
    fn unknown_discriminants_and_trailers_are_rejected() {
        let offer = peer_envelope_wire::Body::MigrateOffer(MigrateOfferWire {
            volume: 7,
            record: vec![1],
            vmstate: None,
        });
        let invalid_kind = PeerEnvelopeWire {
            version: PEER_FORMAT_VERSION,
            from: 1,
            to: 2,
            kind: 24,
            body: Some(offer.clone()),
        };
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &invalid_kind.encode_to_vec())).is_err());

        let missing_body = PeerEnvelopeWire {
            kind: 0,
            body: None,
            ..invalid_kind.clone()
        };
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &missing_body.encode_to_vec())).is_err());

        let canonical = PeerEnvelopeWire {
            kind: 0,
            body: Some(offer),
            ..invalid_kind
        }
        .encode_to_vec();
        let mut unknown_field = canonical;
        unknown_field.extend_from_slice(&[0x98, 0x06, 0x01]);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &unknown_field)).is_err());
    }
}
