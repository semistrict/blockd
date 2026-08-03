//! Journal records: one framed record per consistency point, write-once at
//! `layout::journal_blob(vset, seq)`. A record is self-contained — full
//! page→location map plus the vset's immutable config — so per-vset journal
//! length stays O(1) records no matter how many checkpoints were ever taken
//! (R3.4), and recovery needs nothing but intact records (R8.2). The same
//! bytes are the backup manifest in the object store, verbatim.

use std::collections::BTreeMap;

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::segment::PageLoc;
use crate::types::{Epoch, Gen, JournalSeq, PageId, PageNo, VolumeId, VolumeIdx, VsetId};

pub const MAGIC_JOURNAL: u32 = u32::from_le_bytes(*b"BJR1");

/// Immutable configuration of a vset, carried in every record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct VsetConfig {
    pub disk_volumes: u8,
    pub pages_per_volume: u32,
    /// The one durability knob, set at creation, immutable (R4.1).
    pub backed_up: bool,
}

impl VsetConfig {
    /// All volumes of the vset: memory first, then disks.
    pub fn volumes(&self, vset: VsetId) -> impl Iterator<Item = VolumeId> + use<> {
        (0..=self.disk_volumes).map(move |idx| VolumeId {
            vset,
            idx: VolumeIdx(idx),
        })
    }

    pub fn contains(&self, page: PageId) -> bool {
        page.volume.idx.0 <= self.disk_volumes && page.page.0 < self.pages_per_volume
    }
}

/// What kind of consistency point a record is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecordKind {
    /// Background writeback / sync commit: disk volumes restorable by cold
    /// boot; memory entries serve refaults but restore invalid (R3.7).
    Commit,
    /// A whole-vset checkpoint (R1.2): memory, vmstate and disks of one
    /// instant; restore resumes.
    Checkpoint { epoch: Epoch, vmstate: u64 },
}

/// A full consistency point of one vset at one capture instant.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct JournalRecord {
    pub config: VsetConfig,
    pub seq: JournalSeq,
    /// The writer incarnation (head CAS version at claim, R6.3/R6.4).
    pub fence: u64,
    pub kind: RecordKind,
    /// The vset mutation counter at the capture instant.
    pub capture_seq: u64,
    /// Sync watermark (R3.8): the highest sync barrier this vset has ever
    /// acknowledged (or acknowledges by this record's durability). Monotone
    /// across records, so it survives reclamation of the record that
    /// originally covered a sync. Recovery must never choose a resume point
    /// older than the highest watermark it can see.
    pub synced_through: u64,
    /// The captured image: every written page's durable location.
    pub pages: BTreeMap<PageId, (Gen, PageLoc)>,
}

impl JournalRecord {
    pub fn encode(&self, vset: VsetId) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(1); // version
        e.u64(vset.0);
        e.u64(self.seq.0);
        e.u64(self.fence);
        match self.kind {
            RecordKind::Commit => {
                e.u8(0);
                e.u64(0);
                e.u64(0);
            }
            RecordKind::Checkpoint { epoch, vmstate } => {
                e.u8(1);
                e.u64(epoch.0);
                e.u64(vmstate);
            }
        }
        e.u64(self.capture_seq);
        e.u64(self.synced_through);
        e.u8(self.config.disk_volumes);
        e.u32(self.config.pages_per_volume);
        e.u8(u8::from(self.config.backed_up));
        e.u32(u32::try_from(self.pages.len()).expect("page count fits u32"));
        for (page, (generation, loc)) in &self.pages {
            e.u8(page.volume.idx.0);
            e.u32(page.page.0);
            e.u64(generation.0);
            e.u64(loc.base);
            e.u64(loc.fence);
            e.u64(loc.seg.0);
            e.u32(loc.offset);
            e.u32(loc.len);
        }
        seal_frame(MAGIC_JOURNAL, &e.finish())
    }

    /// Verify and decode a record. Any damage is one answer: corrupt.
    pub fn decode(vset: VsetId, bytes: &[u8]) -> Result<JournalRecord, DecodeError> {
        let payload = open_frame(MAGIC_JOURNAL, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != 1 {
            return Err(DecodeError);
        }
        if d.u64()? != vset.0 {
            return Err(DecodeError);
        }
        let seq = JournalSeq(d.u64()?);
        let fence = d.u64()?;
        let kind_tag = d.u8()?;
        let epoch = Epoch(d.u64()?);
        let vmstate = d.u64()?;
        let kind = match kind_tag {
            0 => RecordKind::Commit,
            1 => RecordKind::Checkpoint { epoch, vmstate },
            _ => return Err(DecodeError),
        };
        let capture_seq = d.u64()?;
        let synced_through = d.u64()?;
        let config = VsetConfig {
            disk_volumes: d.u8()?,
            pages_per_volume: d.u32()?,
            backed_up: match d.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError),
            },
        };
        let count = d.u32()?;
        let mut pages = BTreeMap::new();
        for _ in 0..count {
            let volume = VolumeIdx(d.u8()?);
            let page_no = PageNo(d.u32()?);
            let generation = Gen(d.u64()?);
            let loc = PageLoc {
                base: d.u64()?,
                fence: d.u64()?,
                seg: crate::types::SegId(d.u64()?),
                offset: d.u32()?,
                len: d.u32()?,
            };
            let page = PageId {
                volume: VolumeId { vset, idx: volume },
                page: page_no,
            };
            if !config.contains(page) {
                return Err(DecodeError);
            }
            if pages.insert(page, (generation, loc)).is_some() {
                return Err(DecodeError);
            }
        }
        d.finish()?;
        Ok(JournalRecord {
            config,
            seq,
            fence,
            kind,
            capture_seq,
            synced_through,
            pages,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;
    use crate::types::SegId;

    fn sample_page(volume: u8, page: u32) -> PageId {
        PageId {
            volume: VolumeId {
                vset: VsetId(0xA1),
                idx: VolumeIdx(volume),
            },
            page: PageNo(page),
        }
    }

    fn sample_record() -> JournalRecord {
        let mut pages = BTreeMap::new();
        pages.insert(
            sample_page(0, 3),
            (
                Gen(7),
                PageLoc {
                    base: 0,
                    fence: 6,
                    seg: SegId(2),
                    offset: 27,
                    len: 90,
                },
            ),
        );
        pages.insert(
            sample_page(1, 0),
            (
                Gen(9),
                PageLoc {
                    base: 0,
                    fence: 6,
                    seg: SegId(2),
                    offset: 117,
                    len: 88,
                },
            ),
        );
        JournalRecord {
            config: VsetConfig {
                disk_volumes: 2,
                pages_per_volume: 16,
                backed_up: true,
            },
            seq: JournalSeq(5),
            fence: 6,
            kind: RecordKind::Checkpoint {
                epoch: Epoch(3),
                vmstate: 41,
            },
            capture_seq: 99,
            synced_through: 90,
            pages,
        }
    }

    #[test]
    fn records_round_trip_and_are_byte_pinned() {
        let record = sample_record();
        let bytes = record.encode(VsetId(0xA1));
        assert_eq!(JournalRecord::decode(VsetId(0xA1), &bytes), Ok(record));
        // Byte pin (R10.2): any change here is a storage format change.
        assert_eq!(bytes.len(), 171);
        assert_eq!(crc32c(&bytes), 0xE40A_BD37);
    }

    #[test]
    fn records_reject_any_single_bit_flip() {
        let bytes = sample_record().encode(VsetId(0xA1));
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                JournalRecord::decode(VsetId(0xA1), &damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn records_are_bound_to_their_vset() {
        let bytes = sample_record().encode(VsetId(0xA1));
        assert!(JournalRecord::decode(VsetId(0xA2), &bytes).is_err());
    }
}
