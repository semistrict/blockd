//! The head record: one small object per backed-up vset at
//! `layout::head_key`, and the system's assignment authority (R6.3). Every
//! update goes through the store's compare-and-swap; the store version that
//! a successful claim returns *is* the claimant's fence — the namespace all
//! of its segments and manifests live under. Two hosts racing to restore
//! resolve to exactly one runner by CAS alone, and a fenced former holder's
//! CAS failures make it structurally unable to publish (R6.4).
//!
//! The head is the one non-backup use of the object store (R4.2): small,
//! rare, never on a guest-visible path.

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::types::{HostId, JournalSeq, VsetId};

pub const MAGIC_HEAD: u32 = u32::from_le_bytes(*b"BHD1");

/// Pointer to the newest backed-up manifest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ManifestPtr {
    pub fence: u64,
    pub seq: JournalSeq,
    /// The manifest's capture instant (restore planning, lag observability).
    pub capture_seq: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HeadRecord {
    pub vset: VsetId,
    /// The host currently assigned to run this vset.
    pub holder: HostId,
    /// The holder's fence (the head version its claim returned).
    pub fence: u64,
    /// Newest backed-up recovery point; `None` until the first backup lands.
    pub manifest: Option<ManifestPtr>,
}

impl HeadRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(1); // version
        e.u64(self.vset.0);
        e.u16(self.holder.0);
        e.u64(self.fence);
        match &self.manifest {
            None => e.u8(0),
            Some(ptr) => {
                e.u8(1);
                e.u64(ptr.fence);
                e.u64(ptr.seq.0);
                e.u64(ptr.capture_seq);
            }
        }
        seal_frame(MAGIC_HEAD, &e.finish())
    }

    /// Verify and decode a head record (R8.1 applies to the store too).
    pub fn decode(vset: VsetId, bytes: &[u8]) -> Result<HeadRecord, DecodeError> {
        let payload = open_frame(MAGIC_HEAD, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != 1 {
            return Err(DecodeError);
        }
        if d.u64()? != vset.0 {
            return Err(DecodeError);
        }
        let holder = HostId(d.u16()?);
        let fence = d.u64()?;
        let manifest = match d.u8()? {
            0 => None,
            1 => Some(ManifestPtr {
                fence: d.u64()?,
                seq: JournalSeq(d.u64()?),
                capture_seq: d.u64()?,
            }),
            _ => return Err(DecodeError),
        };
        d.finish()?;
        Ok(HeadRecord {
            vset,
            holder,
            fence,
            manifest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;

    fn sample() -> HeadRecord {
        HeadRecord {
            vset: VsetId(0xA1),
            holder: HostId(3),
            fence: 4,
            manifest: Some(ManifestPtr {
                fence: 2,
                seq: JournalSeq(17),
                capture_seq: 99,
            }),
        }
    }

    #[test]
    fn heads_round_trip_and_are_byte_pinned() {
        let bytes = sample().encode();
        assert_eq!(HeadRecord::decode(VsetId(0xA1), &bytes), Ok(sample()));
        // Byte pin (R10.2): any change here is a storage format change.
        assert_eq!(bytes.len(), 57);
        assert_eq!(crc32c(&bytes), 0xF278_CBEC);
    }

    #[test]
    fn heads_reject_any_single_bit_flip() {
        let bytes = sample().encode();
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                HeadRecord::decode(VsetId(0xA1), &damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn heads_without_manifest_round_trip() {
        let head = HeadRecord {
            vset: VsetId(0xA1),
            holder: HostId(0),
            fence: 1,
            manifest: None,
        };
        let bytes = head.encode();
        assert_eq!(HeadRecord::decode(VsetId(0xA1), &bytes), Ok(head));
    }
}
