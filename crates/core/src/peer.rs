//! The peer protocol's wire form (R11.1's payload layer): one checksummed
//! frame per [`PeerMsg`], carrying the sender's identity so a transport
//! needs no handshake state. The channel contract is at-least-once — the
//! daemon's retry timers re-drive anything a transport drops — so a frame
//! that fails verification is simply discarded, never repaired.
//!
//! Payload layout after the standard frame header: `version u16 | from u16
//! | discriminant u8 | fields`, fields in [`PeerMsg`] declaration order.
//! `Vec<u8>` encodes as `len u32 | bytes`; `Option<Vec<u8>>` prefixes a
//! presence byte. Embedded blobs (records, segment entries, leaves) are
//! already framed and verified by their consumers — they pass through
//! verbatim, damage included (R8.1: the reader decides).

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::seam::{IoId, PeerMsg};
use crate::types::{HostId, SegId, VsetId};

pub const MAGIC_PEER: u32 = u32::from_le_bytes(*b"BPM1");

/// Frame payload cap for transports: R4.6's 64 MiB object cap bounds every
/// embedded blob, so anything larger is a desynced or hostile stream.
pub const MAX_PEER_PAYLOAD: u32 = 64 * 1024 * 1024 + 4096;

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
pub fn encode_peer(from: HostId, msg: &PeerMsg) -> Vec<u8> {
    let mut e = Enc::new();
    e.u16(1); // version
    e.u16(from.0);
    match msg {
        PeerMsg::MigrateOffer { vset, record } => {
            e.u8(0);
            e.u64(vset.0);
            e.u32(u32::try_from(record.len()).expect("record fits u32"));
            e.bytes(record);
        }
        PeerMsg::MigrateAccept { vset } => {
            e.u8(1);
            e.u64(vset.0);
        }
        PeerMsg::FetchRange {
            io,
            vset,
            fence,
            seg,
            offset,
            len,
        } => {
            e.u8(2);
            e.u64(io.0);
            e.u64(vset.0);
            e.u64(*fence);
            e.u64(seg.0);
            e.u32(*offset);
            e.u32(*len);
        }
        PeerMsg::Page { io, bytes } => {
            e.u8(3);
            e.u64(io.0);
            opt_bytes(&mut e, bytes.as_deref());
        }
        PeerMsg::FetchLeaf {
            io,
            vset,
            base,
            fence,
            id,
        } => {
            e.u8(4);
            e.u64(io.0);
            e.u64(vset.0);
            e.u64(*base);
            e.u64(*fence);
            e.u64(*id);
        }
        PeerMsg::Leaf { io, bytes } => {
            e.u8(5);
            e.u64(io.0);
            opt_bytes(&mut e, bytes.as_deref());
        }
        PeerMsg::Released { vset } => {
            e.u8(6);
            e.u64(vset.0);
        }
        PeerMsg::ReleasedAck { vset } => {
            e.u8(7);
            e.u64(vset.0);
        }
    }
    seal_frame(MAGIC_PEER, &e.finish())
}

/// Verify and decode one frame. Any damage, unknown version, unknown
/// discriminant, or trailing bytes is one answer: corrupt — the transport
/// drops the frame (and typically the connection) and the retry timers
/// re-drive.
pub fn decode_peer(bytes: &[u8]) -> Result<(HostId, PeerMsg), DecodeError> {
    let payload = open_frame(MAGIC_PEER, bytes)?;
    let mut d = Dec::new(payload);
    if d.u16()? != 1 {
        return Err(DecodeError);
    }
    let from = HostId(d.u16()?);
    let msg = match d.u8()? {
        0 => {
            let vset = VsetId(d.u64()?);
            let len = usize::try_from(d.u32()?).expect("u32 fits usize");
            let record = d.bytes(len)?.to_vec();
            PeerMsg::MigrateOffer { vset, record }
        }
        1 => PeerMsg::MigrateAccept {
            vset: VsetId(d.u64()?),
        },
        2 => PeerMsg::FetchRange {
            io: IoId(d.u64()?),
            vset: VsetId(d.u64()?),
            fence: d.u64()?,
            seg: SegId(d.u64()?),
            offset: d.u32()?,
            len: d.u32()?,
        },
        3 => PeerMsg::Page {
            io: IoId(d.u64()?),
            bytes: decode_opt_bytes(&mut d)?,
        },
        4 => PeerMsg::FetchLeaf {
            io: IoId(d.u64()?),
            vset: VsetId(d.u64()?),
            base: d.u64()?,
            fence: d.u64()?,
            id: d.u64()?,
        },
        5 => PeerMsg::Leaf {
            io: IoId(d.u64()?),
            bytes: decode_opt_bytes(&mut d)?,
        },
        6 => PeerMsg::Released {
            vset: VsetId(d.u64()?),
        },
        7 => PeerMsg::ReleasedAck {
            vset: VsetId(d.u64()?),
        },
        _ => return Err(DecodeError),
    };
    d.finish()?;
    Ok((from, msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;

    fn samples() -> Vec<PeerMsg> {
        vec![
            PeerMsg::MigrateOffer {
                vset: VsetId(7),
                record: vec![0xAB; 17],
            },
            PeerMsg::MigrateAccept { vset: VsetId(7) },
            PeerMsg::FetchRange {
                io: IoId(99),
                vset: VsetId(7),
                fence: 3,
                seg: SegId(12),
                offset: 4096,
                len: 640,
            },
            PeerMsg::Page {
                io: IoId(99),
                bytes: Some(vec![0x5A; 640]),
            },
            PeerMsg::Page {
                io: IoId(100),
                bytes: None,
            },
            PeerMsg::FetchLeaf {
                io: IoId(101),
                vset: VsetId(7),
                base: 0,
                fence: 3,
                id: 2,
            },
            PeerMsg::Leaf {
                io: IoId(101),
                bytes: Some(vec![0xC3; 136]),
            },
            PeerMsg::Leaf {
                io: IoId(102),
                bytes: None,
            },
            PeerMsg::Released { vset: VsetId(7) },
            PeerMsg::ReleasedAck { vset: VsetId(7) },
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
        assert_eq!(bytes.len(), 1123);
        assert_eq!(crc32c(&bytes), 0x8031_8DA2);
    }

    #[test]
    fn any_single_bit_flip_is_rejected() {
        let framed = encode_peer(
            HostId(1),
            &PeerMsg::FetchRange {
                io: IoId(1),
                vset: VsetId(2),
                fence: 3,
                seg: SegId(4),
                offset: 5,
                len: 6,
            },
        );
        for bit in 0..framed.len() * 8 {
            let mut damaged = framed.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                decode_peer(&damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn unknown_versions_discriminants_and_trailers_are_rejected() {
        // Version 2 does not exist.
        let mut e = Enc::new();
        e.u16(2);
        e.u16(1);
        e.u8(1);
        e.u64(7);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &e.finish())).is_err());
        // Discriminant 8 does not exist.
        let mut e = Enc::new();
        e.u16(1);
        e.u16(1);
        e.u8(8);
        e.u64(7);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &e.finish())).is_err());
        // A presence byte outside {0, 1} is corrupt.
        let mut e = Enc::new();
        e.u16(1);
        e.u16(1);
        e.u8(3); // Page
        e.u64(9);
        e.u8(2);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &e.finish())).is_err());
        // Trailing bytes after a complete message are corrupt.
        let mut e = Enc::new();
        e.u16(1);
        e.u16(1);
        e.u8(6); // Released
        e.u64(7);
        e.u8(0xFF);
        assert!(decode_peer(&seal_frame(MAGIC_PEER, &e.finish())).is_err());
    }
}
