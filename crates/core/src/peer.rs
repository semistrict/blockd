//! The peer protocol's wire form (R11.1's payload layer): one checksummed
//! frame per [`PeerMsg`], carrying the sender's identity so a transport
//! needs no handshake state. The channel contract is at-least-once — the
//! daemon's retry timers re-drive anything a transport drops — so a frame
//! that fails verification is simply discarded, never repaired.
//!
//! Payload layout after the standard frame header: `from u16 | discriminant
//! u8 | fields`, fields in [`PeerMsg`] declaration order.
//! `Vec<u8>` encodes as `len u32 | bytes`; `Option<Vec<u8>>` prefixes a
//! presence byte. Embedded blobs (records and blx entries) are
//! already framed and verified by their consumers. Inline VMM bytes are
//! protected by the peer frame and checked against the offered record before
//! use (R8.1: the reader decides).

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::protocol::{MAX_OBJECT_BYTES, PeerMsg, PeerRequestId};
use crate::replica_wire::{
    decode_artifact, decode_commit_info, encode_artifact, encode_commit_info,
};
use crate::types::{HostId, ObjectId, VolumeId};

pub const MAGIC_PEER: u32 = u32::from_le_bytes(*b"BPM1");

/// Frame payload cap for transports: R4.6's 64 MiB object cap bounds every
/// embedded blob, so anything larger is a desynced or hostile stream.
pub const MAX_PEER_PAYLOAD: u32 = MAX_OBJECT_BYTES;

fn opt_bytes(e: &mut Enc, bytes: Option<&[u8]>) {
    match bytes {
        None => e.u8(0),
        Some(bytes) => {
            e.u8(1);
            e.u32(u32::try_from(bytes.len()).expect("blob fits u32"));
            e.bytes(bytes);
        }
    }
}

fn decode_opt_bytes(d: &mut Dec) -> Result<Option<Vec<u8>>, DecodeError> {
    match d.u8()? {
        0 => Ok(None),
        1 => {
            let len = usize::try_from(d.u32()?).expect("u32 fits usize");
            Ok(Some(d.bytes(len)?.to_vec()))
        }
        _ => Err(DecodeError),
    }
}

/// Encode one message as a sealed frame carrying the sender's identity.
#[allow(clippy::too_many_lines)]
pub fn encode_peer(from: HostId, msg: &PeerMsg) -> Vec<u8> {
    let mut e = Enc::new();
    e.u16(from.0);
    e.u8(msg.tag());
    match msg {
        PeerMsg::MigrateOffer {
            volume,
            record,
            vmstate,
        } => {
            e.u64(volume.0);
            e.u32(u32::try_from(record.len()).expect("record fits u32"));
            e.bytes(record);
            opt_bytes(&mut e, vmstate.as_deref());
        }
        PeerMsg::MigrateAccept {
            volume,
            offer_fence,
        } => {
            e.u64(volume.0);
            e.u64(*offer_fence);
        }
        PeerMsg::FetchRange {
            io,
            volume,
            replica_assignment_epoch,
            fence,
            object,
            offset,
            len,
        } => {
            e.u64(io.0);
            e.u64(volume.0);
            if let Some(epoch) = replica_assignment_epoch {
                e.u8(1);
                e.u64(*epoch);
            } else {
                e.u8(0);
                e.u64(0);
            }
            e.u64(*fence);
            e.u64(object.0);
            e.u32(*offset);
            e.u32(*len);
        }
        PeerMsg::Page { io, bytes } | PeerMsg::VnodeClosure { io, bytes } => {
            e.u64(io.0);
            opt_bytes(&mut e, bytes.as_deref());
        }
        PeerMsg::ReleasedAck {
            volume,
            release_fence,
        }
        | PeerMsg::Released {
            volume,
            release_fence,
        } => {
            e.u64(volume.0);
            e.u64(*release_fence);
        }
        PeerMsg::ReplicaPut {
            volume,
            assignment_epoch,
            artifact: id,
            checksum,
            bytes,
        } => {
            e.u64(volume.0);
            e.u64(*assignment_epoch);
            encode_artifact(&mut e, *id);
            e.u32(*checksum);
            e.u32(u32::try_from(bytes.len()).expect("artifact fits u32"));
            e.bytes(bytes);
        }
        PeerMsg::ReplicaPutAck {
            volume,
            assignment_epoch,
            artifact: id,
            checksum,
        } => {
            e.u64(volume.0);
            e.u64(*assignment_epoch);
            encode_artifact(&mut e, *id);
            e.u32(*checksum);
        }
        PeerMsg::ReplicaCommit {
            volume,
            assignment_epoch,
            info,
            required,
            record,
        } => {
            e.u64(volume.0);
            e.u64(*assignment_epoch);
            encode_commit_info(&mut e, *info);
            e.u32(u32::try_from(required.len()).expect("required count fits u32"));
            for id in required {
                encode_artifact(&mut e, *id);
            }
            e.u32(u32::try_from(record.len()).expect("record fits u32"));
            e.bytes(record);
        }
        PeerMsg::ReplicaCommitAck {
            volume,
            assignment_epoch,
            info,
        } => {
            e.u64(volume.0);
            e.u64(*assignment_epoch);
            encode_commit_info(&mut e, *info);
        }
        PeerMsg::ReplicaStatus {
            volume,
            assignment_epoch,
        } => {
            e.u64(volume.0);
            e.u64(*assignment_epoch);
        }
        PeerMsg::ReplicaStatusReply {
            volume,
            assignment_epoch,
            committed,
        } => {
            e.u64(volume.0);
            e.u64(*assignment_epoch);
            match committed {
                None => e.u8(0),
                Some(info) => {
                    e.u8(1);
                    encode_commit_info(&mut e, *info);
                }
            }
        }
        PeerMsg::ReplicaReleaseAck {
            volume,
            assignment_epoch,
            through,
        }
        | PeerMsg::ReplicaRelease {
            volume,
            assignment_epoch,
            through,
        } => {
            e.u64(volume.0);
            e.u64(*assignment_epoch);
            encode_commit_info(&mut e, *through);
        }
        PeerMsg::VnodeAdopt { io, proof } => {
            e.u64(io.0);
            encode_authority_proof(&mut e, *proof);
        }
        PeerMsg::VnodeAdoptAck {
            io,
            proof,
            closures,
        } => {
            e.u64(io.0);
            encode_authority_proof(&mut e, *proof);
            e.u32(u32::try_from(closures.len()).expect("closure count fits"));
            for closure in closures {
                e.u64(closure.volume.0);
                e.u64(closure.sequence);
                e.u32(closure.checksum);
                e.u32(closure.len);
            }
        }
        PeerMsg::VnodeFetchClosure { io, vnode, closure } => {
            e.u64(io.0);
            e.u32(vnode.0);
            encode_protected_closure(&mut e, *closure);
        }
        PeerMsg::VnodeCommit {
            io,
            proof,
            volume,
            sequence,
            bytes,
        } => {
            e.u64(io.0);
            encode_authority_proof(&mut e, *proof);
            e.u64(volume.0);
            e.u64(*sequence);
            e.u32(u32::try_from(bytes.len()).expect("closure fits u32"));
            e.bytes(bytes);
        }
        PeerMsg::VnodeCommitAck { io, closure } => {
            e.u64(io.0);
            encode_protected_closure(&mut e, *closure);
        }
    }
    seal_frame(MAGIC_PEER, &e.finish())
}

/// Verify and decode one frame. Any damage, unknown discriminant, or trailing
/// bytes is one answer: corrupt — the transport drops the frame (and typically
/// the connection) and the retry timers re-drive.
#[allow(clippy::too_many_lines)]
pub fn decode_peer(bytes: &[u8]) -> Result<(HostId, PeerMsg), DecodeError> {
    if bytes.len() > crate::format::FRAME_HEADER + MAX_PEER_PAYLOAD as usize {
        return Err(DecodeError);
    }
    let payload = open_frame(MAGIC_PEER, bytes)?;
    let mut d = Dec::new(payload);
    let from = HostId(d.u16()?);
    let msg = match d.u8()? {
        0 => {
            let volume = VolumeId(d.u64()?);
            let len = usize::try_from(d.u32()?).expect("u32 fits usize");
            let record = d.bytes(len)?.to_vec();
            let vmstate = decode_opt_bytes(&mut d)?;
            PeerMsg::MigrateOffer {
                volume,
                record,
                vmstate,
            }
        }
        1 => PeerMsg::MigrateAccept {
            volume: VolumeId(d.u64()?),
            offer_fence: d.u64()?,
        },
        2 => PeerMsg::FetchRange {
            io: PeerRequestId(d.u64()?),
            volume: VolumeId(d.u64()?),
            replica_assignment_epoch: match d.u8()? {
                0 if d.u64()? == 0 => None,
                1 => Some(d.u64()?),
                _ => return Err(DecodeError),
            },
            fence: d.u64()?,
            object: ObjectId(d.u64()?),
            offset: d.u32()?,
            len: d.u32()?,
        },
        3 => PeerMsg::Page {
            io: PeerRequestId(d.u64()?),
            bytes: decode_opt_bytes(&mut d)?,
        },
        6 => PeerMsg::Released {
            volume: VolumeId(d.u64()?),
            release_fence: d.u64()?,
        },
        7 => PeerMsg::ReleasedAck {
            volume: VolumeId(d.u64()?),
            release_fence: d.u64()?,
        },
        8 => {
            let volume = VolumeId(d.u64()?);
            let assignment_epoch = d.u64()?;
            let artifact = decode_artifact(&mut d)?;
            let checksum = d.u32()?;
            let len = usize::try_from(d.u32()?).expect("u32 fits usize");
            PeerMsg::ReplicaPut {
                volume,
                assignment_epoch,
                artifact,
                checksum,
                bytes: d.bytes(len)?.to_vec(),
            }
        }
        9 => PeerMsg::ReplicaPutAck {
            volume: VolumeId(d.u64()?),
            assignment_epoch: d.u64()?,
            artifact: decode_artifact(&mut d)?,
            checksum: d.u32()?,
        },
        10 => {
            let volume = VolumeId(d.u64()?);
            let assignment_epoch = d.u64()?;
            let info = decode_commit_info(&mut d)?;
            let count = d.u32()?;
            if count > 1_000_000 {
                return Err(DecodeError);
            }
            let mut required = Vec::new();
            for _ in 0..count {
                required.push(decode_artifact(&mut d)?);
            }
            let len = usize::try_from(d.u32()?).expect("u32 fits usize");
            PeerMsg::ReplicaCommit {
                volume,
                assignment_epoch,
                info,
                required,
                record: d.bytes(len)?.to_vec(),
            }
        }
        11 => PeerMsg::ReplicaCommitAck {
            volume: VolumeId(d.u64()?),
            assignment_epoch: d.u64()?,
            info: decode_commit_info(&mut d)?,
        },
        12 => PeerMsg::ReplicaStatus {
            volume: VolumeId(d.u64()?),
            assignment_epoch: d.u64()?,
        },
        13 => PeerMsg::ReplicaStatusReply {
            volume: VolumeId(d.u64()?),
            assignment_epoch: d.u64()?,
            committed: match d.u8()? {
                0 => None,
                1 => Some(decode_commit_info(&mut d)?),
                _ => return Err(DecodeError),
            },
        },
        15 => PeerMsg::ReplicaRelease {
            volume: VolumeId(d.u64()?),
            assignment_epoch: d.u64()?,
            through: decode_commit_info(&mut d)?,
        },
        16 => PeerMsg::ReplicaReleaseAck {
            volume: VolumeId(d.u64()?),
            assignment_epoch: d.u64()?,
            through: decode_commit_info(&mut d)?,
        },
        18 => PeerMsg::VnodeAdopt {
            io: PeerRequestId(d.u64()?),
            proof: decode_authority_proof(&mut d)?,
        },
        19 => {
            let io = PeerRequestId(d.u64()?);
            let proof = decode_authority_proof(&mut d)?;
            let count = d.u32()?;
            if count > 1_000_000 {
                return Err(DecodeError);
            }
            let mut closures = Vec::with_capacity(usize::try_from(count).expect("count fits"));
            for _ in 0..count {
                closures.push(crate::vnode_member::ProtectedClosureRef {
                    volume: VolumeId(d.u64()?),
                    sequence: d.u64()?,
                    checksum: d.u32()?,
                    len: d.u32()?,
                });
            }
            PeerMsg::VnodeAdoptAck {
                io,
                proof,
                closures,
            }
        }
        20 => PeerMsg::VnodeFetchClosure {
            io: PeerRequestId(d.u64()?),
            vnode: crate::authority::VnodeId(d.u32()?),
            closure: decode_protected_closure(&mut d)?,
        },
        21 => PeerMsg::VnodeClosure {
            io: PeerRequestId(d.u64()?),
            bytes: decode_opt_bytes(&mut d)?,
        },
        22 => {
            let io = PeerRequestId(d.u64()?);
            let proof = decode_authority_proof(&mut d)?;
            let volume = VolumeId(d.u64()?);
            let sequence = d.u64()?;
            let len = usize::try_from(d.u32()?).map_err(|_| DecodeError)?;
            PeerMsg::VnodeCommit {
                io,
                proof,
                volume,
                sequence,
                bytes: d.bytes(len)?.to_vec(),
            }
        }
        23 => PeerMsg::VnodeCommitAck {
            io: PeerRequestId(d.u64()?),
            closure: decode_protected_closure(&mut d)?,
        },
        _ => return Err(DecodeError),
    };
    d.finish()?;
    Ok((from, msg))
}

fn encode_authority_proof(e: &mut Enc, proof: crate::authority::AuthorityProof) {
    e.u64(proof.store_version);
    let bytes = proof.authority.encode();
    e.u32(u32::try_from(bytes.len()).expect("authority proof fits"));
    e.bytes(&bytes);
}

fn encode_protected_closure(e: &mut Enc, closure: crate::vnode_member::ProtectedClosureRef) {
    e.u64(closure.volume.0);
    e.u64(closure.sequence);
    e.u32(closure.checksum);
    e.u32(closure.len);
}

fn decode_protected_closure(
    d: &mut Dec<'_>,
) -> Result<crate::vnode_member::ProtectedClosureRef, DecodeError> {
    Ok(crate::vnode_member::ProtectedClosureRef {
        volume: VolumeId(d.u64()?),
        sequence: d.u64()?,
        checksum: d.u32()?,
        len: d.u32()?,
    })
}

fn decode_authority_proof(
    d: &mut Dec<'_>,
) -> Result<crate::authority::AuthorityProof, DecodeError> {
    let store_version = d.u64()?;
    let len = usize::try_from(d.u32()?).map_err(|_| DecodeError)?;
    let authority = crate::authority::VnodeAuthority::decode(d.bytes(len)?)?;
    Ok(crate::authority::AuthorityProof {
        store_version,
        authority,
    })
}

#[cfg(test)]
mod tests {
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
        let proof = crate::authority::AuthorityProof {
            store_version: 17,
            authority: crate::authority::VnodeAuthority {
                cluster_id: 8,
                placement_epoch: 4,
                vnode: crate::authority::VnodeId(2),
                generation: 9,
                primary: HostId(2),
                primary_session: 55,
                primary_host_epoch: 3,
            },
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
            PeerMsg::VnodeAdopt {
                io: PeerRequestId(103),
                proof,
            },
            PeerMsg::VnodeAdoptAck {
                io: PeerRequestId(103),
                proof,
                closures: vec![crate::vnode_member::ProtectedClosureRef {
                    volume: VolumeId(7),
                    sequence: 44,
                    checksum: 0x1234_5678,
                    len: 99,
                }],
            },
            PeerMsg::VnodeFetchClosure {
                io: PeerRequestId(104),
                vnode: crate::authority::VnodeId(2),
                closure: crate::vnode_member::ProtectedClosureRef {
                    volume: VolumeId(7),
                    sequence: 44,
                    checksum: 0x1234_5678,
                    len: 99,
                },
            },
            PeerMsg::VnodeClosure {
                io: PeerRequestId(104),
                bytes: Some(vec![0xE5; 99]),
            },
            PeerMsg::VnodeCommit {
                io: PeerRequestId(105),
                proof,
                volume: VolumeId(7),
                sequence: 45,
                bytes: vec![0xA6; 101],
            },
            PeerMsg::VnodeCommitAck {
                io: PeerRequestId(105),
                closure: crate::vnode_member::ProtectedClosureRef {
                    volume: VolumeId(7),
                    sequence: 45,
                    checksum: 0x8765_4321,
                    len: 101,
                },
            },
        ]
    }

    #[test]
    fn every_variant_round_trips_with_its_sender() {
        for msg in samples() {
            let framed = encode_peer(HostId(2), &msg);
            assert_eq!(decode_peer(&framed), Ok((HostId(2), msg)));
        }
    }

    #[test]
    fn frames_are_byte_pinned() {
        // The concatenation of every sample frame pins the whole layout
        // (R10.2): any encoding change must be seen and decided.
        let bytes: Vec<u8> = samples()
            .iter()
            .flat_map(|msg| encode_peer(HostId(2), msg))
            .collect();
        assert_eq!(bytes.len(), 2096);
        assert_eq!(crc32c(&bytes), 0xF56F_4AA0);
    }

    #[test]
    fn any_single_bit_flip_of_every_variant_is_rejected() {
        for (variant, message) in samples().iter().enumerate() {
            let framed = encode_peer(HostId(1), message);
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
        // Discriminant 24 does not exist.
        let mut e = Enc::new();
        e.u16(2);
        e.u8(24);
        e.u64(7);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &e.finish())).is_err());
        // A presence byte outside {0, 1} is corrupt.
        let mut e = Enc::new();
        e.u16(1);
        e.u8(3); // Page
        e.u64(9);
        e.u8(2);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &e.finish())).is_err());
        // Trailing bytes after a complete message are corrupt.
        let mut e = Enc::new();
        e.u16(1);
        e.u8(6); // Released
        e.u64(7);
        e.u64(3);
        e.u8(0xFF);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &e.finish())).is_err());
    }
}
