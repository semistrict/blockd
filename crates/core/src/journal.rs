//! Journal records: one framed record per consistency point. The durable
//! record names the complete BLX file set needed for recovery and carries the
//! checksum of that exact logical state. Recovery rebuilds its block lookup
//! from BLX footers; no durable page-to-file map is encoded. Migration may
//! append a separate in-memory lookup index to its wire message, but that
//! index is never written to either journal.

use std::collections::BTreeMap;

use crate::blx::{BlockKey, BlockSpace};
use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::manifest::ObjectRef;
use crate::page_file::PageFileLoc;
use crate::types::{Epoch, Gen, HostId, JournalSeq, PageId, PageNo, VolumeId, page_size};

pub const MAGIC_JOURNAL: u32 = u32::from_le_bytes(*b"BJR1");
const MAGIC_MIGRATION_INDEX: u32 = u32::from_le_bytes(*b"BMIX");

pub type MigrationBlockChecksums = BTreeMap<BlockKey, (Gen, u64)>;

/// The immutable consistency/lifecycle kind of a volume (R1.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum VolumeKind {
    Memory = 0,
    Data = 1,
}

/// Immutable configuration of a volume, carried in every record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct VolumeConfig {
    pub kind: VolumeKind,
    pub pages: u32,
}

impl VolumeConfig {
    pub const fn memory(pages: u32) -> Self {
        Self {
            kind: VolumeKind::Memory,
            pages,
        }
    }

    pub const fn data(pages: u32) -> Self {
        Self {
            kind: VolumeKind::Data,
            pages,
        }
    }

    pub fn contains(&self, page: PageId) -> bool {
        page.page.0 < self.pages
    }

    pub fn is_memory(&self) -> bool {
        self.kind == VolumeKind::Memory
    }

    fn valid(self) -> bool {
        self.pages > 0
    }
}

/// What kind of consistency point a record is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecordKind {
    /// Background writeback / sync commit: disk volumes restorable by cold
    /// boot; memory entries serve refaults but restore invalid (R3.7).
    Commit,
    /// A memory-volume checkpoint (R1.2): memory and vmstate from one
    /// instant; restore resumes.
    Checkpoint {
        epoch: Epoch,
        vmstate: u64,
        vmstate_logical_length: u64,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MigrationSource {
    pub host: HostId,
    /// Exact source-writer fence whose offered cut this destination installed.
    pub offer_fence: Option<u64>,
}

/// A full consistency point of one volume at one capture instant.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct JournalRecord {
    pub config: VolumeConfig,
    pub seq: JournalSeq,
    /// The writer incarnation (head CAS version at claim, R6.3/R6.4).
    pub fence: u64,
    pub kind: RecordKind,
    /// The volume mutation counter at the capture instant.
    pub capture_seq: u64,
    /// Sync watermark (R3.8): the highest sync barrier this volume has ever
    /// acknowledged (or acknowledges by this record's durability). Monotone
    /// across records, so it survives reclamation of the record that
    /// originally covered a sync. Recovery must never choose a resume point
    /// older than the highest watermark it can see.
    pub sync_covered_through: u64,
    /// Checksum of the complete logical state represented by this record.
    /// It is captured with the record so delayed publication cannot observe
    /// a newer in-memory state.
    pub post_state_checksum: u64,
    /// The BLX files needed by this recovery point. This is the durable data
    /// index; page locations below are runtime-only lookup state.
    pub files: Vec<ObjectRef>,
    /// Runtime-only lookup state used by migration messages.
    pub runtime_page_index: BTreeMap<PageId, (Gen, PageFileLoc)>,
    /// Migration provenance (R7.2): while an in-migrated volume still
    /// hydrates from its source, every record names that source. The
    /// destination's first record — the durable ACCEPT of the handoff —
    /// must let a recovery finish the handshake it interrupts: answer the
    /// source's re-offers and keep pulling the tail. Cleared by the first
    /// capture after `Released`.
    pub migrated_from: Option<MigrationSource>,
}

impl JournalRecord {
    pub const fn commit_info(&self) -> crate::protocol::ReplicaCommitInfo {
        crate::protocol::ReplicaCommitInfo {
            writer_fence: self.fence,
            seq: self.seq,
            sync_covered_through: self.sync_covered_through,
        }
    }

    pub fn encode(&self, volume: VolumeId) -> Vec<u8> {
        assert!(self.config.valid(), "invalid volume config");
        assert!(
            !matches!(self.kind, RecordKind::Checkpoint { .. }) || self.config.is_memory(),
            "only memory volumes carry checkpoint vmstate"
        );
        let mut e = Enc::new();
        e.u16(8); // version: one independently addressed volume
        e.u32(u32::try_from(page_size()).expect("page size fits u32"));
        e.u64(volume.0);
        e.u64(self.seq.0);
        e.u64(self.fence);
        match self.kind {
            RecordKind::Commit => {
                e.u8(0);
                e.u64(0);
                e.u64(0);
                e.u64(0);
            }
            RecordKind::Checkpoint {
                epoch,
                vmstate,
                vmstate_logical_length,
            } => {
                e.u8(1);
                e.u64(epoch.0);
                e.u64(vmstate);
                e.u64(vmstate_logical_length);
            }
        }
        e.u64(self.capture_seq);
        e.u64(self.sync_covered_through);
        e.u64(self.post_state_checksum);
        match self.migrated_from {
            None => {
                e.u8(0);
                e.u16(0);
            }
            Some(source) => {
                e.u8(1);
                e.u16(source.host.0);
            }
        }
        match self.migrated_from.and_then(|source| source.offer_fence) {
            None => {
                e.u8(0);
                e.u64(0);
            }
            Some(fence) => {
                e.u8(1);
                e.u64(fence);
            }
        }
        e.u8(self.config.kind as u8);
        e.u32(self.config.pages);
        // The sole current durability policy is primary plus one passive.
        e.u8(2);
        e.u32(u32::try_from(self.files.len()).expect("file count fits u32"));
        for file in &self.files {
            file.encode_into(&mut e);
        }
        seal_frame(MAGIC_JOURNAL, &e.finish())
    }

    /// Migration carries the source's current lookup index after the durable
    /// record. A destination persists this form only while it still depends
    /// on the source, so local recovery can resume post-copy hydration after a
    /// daemon crash. Ordinary journal records never carry the index.
    pub fn encode_migration(&self, volume: VolumeId) -> Vec<u8> {
        self.encode_migration_with_checksums(volume, &BTreeMap::new())
    }

    pub fn encode_migration_with_checksums(
        &self,
        volume: VolumeId,
        block_checksums: &BTreeMap<BlockKey, (Gen, u64)>,
    ) -> Vec<u8> {
        let durable = self.encode(volume);
        let mut index = Enc::new();
        index.u32(
            u32::try_from(self.runtime_page_index.len())
                .expect("runtime_page_index count fits u32"),
        );
        for (page, (generation, loc)) in &self.runtime_page_index {
            index.u32(page.page.0);
            index.u64(generation.0);
            index.u64(loc.base);
            index.u64(loc.fence);
            index.u64(loc.object.0);
            index.u32(loc.offset);
            index.u32(loc.len);
        }
        index.u32(u32::try_from(block_checksums.len()).expect("block checksum count fits u32"));
        for (key, (generation, checksum)) in block_checksums {
            index.u8(key.space as u8);
            index.u32(key.block);
            index.u64(generation.0);
            index.u64(*checksum);
        }
        let index = seal_frame(MAGIC_MIGRATION_INDEX, &index.finish());
        let mut bytes = Vec::with_capacity(8 + durable.len() + index.len());
        bytes.extend_from_slice(
            &u64::try_from(durable.len())
                .expect("journal length fits u64")
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&durable);
        bytes.extend_from_slice(&index);
        bytes
    }

    /// Verify and decode a record. Any damage is one answer: corrupt.
    #[allow(clippy::too_many_lines)]
    pub fn decode(volume: VolumeId, bytes: &[u8]) -> Result<JournalRecord, DecodeError> {
        let payload = open_frame(MAGIC_JOURNAL, bytes)?;
        let mut d = Dec::new(payload);
        let version = d.u16()?;
        if version != 8 {
            return Err(DecodeError);
        }
        if d.u32()? != u32::try_from(page_size()).expect("page size fits u32") {
            return Err(DecodeError);
        }
        if d.u64()? != volume.0 {
            return Err(DecodeError);
        }
        let seq = JournalSeq(d.u64()?);
        let fence = d.u64()?;
        let kind_tag = d.u8()?;
        let epoch = Epoch(d.u64()?);
        let vmstate = d.u64()?;
        let vmstate_logical_length = d.u64()?;
        let kind = match kind_tag {
            0 if epoch.0 == 0 && vmstate == 0 && vmstate_logical_length == 0 => RecordKind::Commit,
            1 => RecordKind::Checkpoint {
                epoch,
                vmstate,
                vmstate_logical_length,
            },
            _ => return Err(DecodeError),
        };
        let capture_seq = d.u64()?;
        let sync_covered_through = d.u64()?;
        let post_state_checksum = d.u64()?;
        let migrated_host = match (d.u8()?, d.u16()?) {
            (0, 0) => None,
            (1, source) => Some(HostId(source)),
            _ => return Err(DecodeError),
        };
        let offered_fence = match (d.u8()?, d.u64()?) {
            (0, 0) => None,
            (1, fence) => Some(fence),
            _ => return Err(DecodeError),
        };
        if migrated_host.is_none() && offered_fence.is_some() {
            return Err(DecodeError);
        }
        let migrated_from = migrated_host.map(|host| MigrationSource {
            host,
            offer_fence: offered_fence,
        });
        let volume_kind = match d.u8()? {
            0 => VolumeKind::Memory,
            1 => VolumeKind::Data,
            _ => return Err(DecodeError),
        };
        let config = VolumeConfig {
            kind: volume_kind,
            pages: d.u32()?,
        };
        let durability_tag = d.u8()?;
        if durability_tag != 2 {
            return Err(DecodeError);
        }
        if !config.valid() {
            return Err(DecodeError);
        }
        if matches!(kind, RecordKind::Checkpoint { .. }) && !config.is_memory() {
            return Err(DecodeError);
        }
        let file_count = d.u32()?;
        let mut files = Vec::with_capacity(usize::try_from(file_count).expect("u32 fits usize"));
        for _ in 0..file_count {
            files.push(ObjectRef::decode_from(&mut d)?);
        }
        d.finish()?;
        crate::manifest::validate_journal_object_refs(&files)?;
        Ok(JournalRecord {
            config,
            seq,
            fence,
            kind,
            capture_seq,
            sync_covered_through,
            post_state_checksum,
            files,
            runtime_page_index: BTreeMap::new(),
            migrated_from,
        })
    }

    pub fn decode_migration_with_checksums(
        volume: VolumeId,
        bytes: &[u8],
    ) -> Result<(JournalRecord, MigrationBlockChecksums), DecodeError> {
        let prefix = bytes.get(..8).ok_or(DecodeError)?;
        let durable_len = usize::try_from(u64::from_le_bytes(
            prefix.try_into().map_err(|_| DecodeError)?,
        ))
        .map_err(|_| DecodeError)?;
        let durable_end = 8usize.checked_add(durable_len).ok_or(DecodeError)?;
        let mut record = Self::decode(volume, bytes.get(8..durable_end).ok_or(DecodeError)?)?;
        let payload = open_frame(
            MAGIC_MIGRATION_INDEX,
            bytes.get(durable_end..).ok_or(DecodeError)?,
        )?;
        let mut d = Dec::new(payload);
        let count = d.u32()?;
        let mut runtime_page_index = BTreeMap::new();
        for _ in 0..count {
            let page_no = PageNo(d.u32()?);
            let generation = Gen(d.u64()?);
            let loc = PageFileLoc {
                base: d.u64()?,
                fence: d.u64()?,
                object: crate::types::ObjectId(d.u64()?),
                offset: d.u32()?,
                len: d.u32()?,
            };
            let page = PageId {
                volume,
                page: page_no,
            };
            if !record.config.contains(page) {
                return Err(DecodeError);
            }
            if runtime_page_index.insert(page, (generation, loc)).is_some() {
                return Err(DecodeError);
            }
        }
        let checksum_count = d.u32()?;
        let mut block_checksums = BTreeMap::new();
        for _ in 0..checksum_count {
            let space = match d.u8()? {
                0 => BlockSpace::Memory,
                1 => BlockSpace::Data,
                2 => BlockSpace::Vmm,
                _ => return Err(DecodeError),
            };
            let key = BlockKey {
                space,
                block: d.u32()?,
            };
            let generation = Gen(d.u64()?);
            let checksum = d.u64()?;
            let valid = match key.space {
                BlockSpace::Memory => {
                    record.config.kind == VolumeKind::Memory && key.block < record.config.pages
                }
                BlockSpace::Vmm => record.config.kind == VolumeKind::Memory,
                BlockSpace::Data => {
                    record.config.kind == VolumeKind::Data && key.block < record.config.pages
                }
            };
            if !valid
                || block_checksums
                    .insert(key, (generation, checksum))
                    .is_some()
            {
                return Err(DecodeError);
            }
        }
        d.finish()?;
        record.runtime_page_index = runtime_page_index;
        Ok((record, block_checksums))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blx::{BlockKey, BlockSpace, NamespaceKind};
    use crate::format::{crc32c, open_frame, seal_frame};
    use crate::manifest::ObjectIdentity;
    use crate::types::ObjectId;

    fn sample_page(_kind: u8, page: u32) -> PageId {
        PageId {
            volume: VolumeId(0xA1),
            page: PageNo(page),
        }
    }

    fn sample_record() -> JournalRecord {
        let mut pages = BTreeMap::new();
        pages.insert(
            sample_page(0, 3),
            (
                Gen(7),
                PageFileLoc {
                    base: 0,
                    fence: 6,
                    object: ObjectId(2),
                    offset: 27,
                    len: 90,
                },
            ),
        );
        pages.insert(
            sample_page(1, 0),
            (
                Gen(9),
                PageFileLoc {
                    base: 0,
                    fence: 6,
                    object: ObjectId(2),
                    offset: 117,
                    len: 88,
                },
            ),
        );
        JournalRecord {
            config: VolumeConfig {
                kind: VolumeKind::Memory,
                pages: 16,
            },
            seq: JournalSeq(5),
            fence: 6,
            kind: RecordKind::Checkpoint {
                epoch: Epoch(3),
                vmstate: 41,
                vmstate_logical_length: 8,
            },
            capture_seq: 99,
            sync_covered_through: 90,
            post_state_checksum: 23,
            files: Vec::new(),
            runtime_page_index: pages,
            migrated_from: Some(MigrationSource {
                host: HostId(2),
                offer_fence: Some(5),
            }),
        }
    }

    #[test]
    fn records_round_trip_and_are_byte_pinned() {
        let record = sample_record();
        let bytes = record.encode(VolumeId(0xA1));
        let mut durable = record.clone();
        durable.runtime_page_index.clear();
        assert_eq!(JournalRecord::decode(VolumeId(0xA1), &bytes), Ok(durable));
        // Byte pin (R10.2): any change here is a storage format change.
        let expected = match page_size() {
            4096 => (113, 0x468A_9C73),
            16_384 => (113, 0xAC8B_6E78),
            size => panic!("byte pin missing for {size}-byte pages"),
        };
        assert_eq!((bytes.len(), crc32c(&bytes)), expected);
    }

    #[test]
    fn zero_migration_offer_fence_round_trips_as_present() {
        let mut record = sample_record();
        record
            .migrated_from
            .as_mut()
            .expect("migration provenance")
            .offer_fence = Some(0);
        let bytes = record.encode_migration(VolumeId(0xA1));
        assert_eq!(
            JournalRecord::decode_migration_with_checksums(VolumeId(0xA1), &bytes)
                .map(|(decoded, _)| decoded),
            Ok(record)
        );
    }

    #[test]
    fn records_reject_any_single_bit_flip() {
        let bytes = sample_record().encode(VolumeId(0xA1));
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                JournalRecord::decode(VolumeId(0xA1), &damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn records_are_bound_to_their_volume() {
        let bytes = sample_record().encode(VolumeId(0xA1));
        assert!(JournalRecord::decode(VolumeId(0xA2), &bytes).is_err());
    }

    #[test]
    fn records_reject_a_different_system_page_size() {
        let bytes = sample_record().encode(VolumeId(0xA1));
        let mut payload = open_frame(MAGIC_JOURNAL, &bytes)
            .expect("record frame")
            .to_vec();
        let incompatible = if page_size() == 4096 {
            16_384u32
        } else {
            4096u32
        };
        payload[2..6].copy_from_slice(&incompatible.to_le_bytes());
        let incompatible = seal_frame(MAGIC_JOURNAL, &payload);
        assert!(JournalRecord::decode(VolumeId(0xA1), &incompatible).is_err());
    }

    #[test]
    fn runtime_page_index_is_not_persisted() {
        let volume = VolumeId(0xA1);
        let record = sample_record();
        let mut without_runtime_index = record.clone();
        without_runtime_index.runtime_page_index.clear();

        assert_eq!(record.encode(volume), without_runtime_index.encode(volume));
        assert_eq!(
            JournalRecord::decode(volume, &record.encode(volume)),
            Ok(without_runtime_index)
        );
    }

    #[test]
    fn uncompacted_local_files_may_exceed_the_archive_overlap_limit() {
        let volume = VolumeId(0xA1);
        let mut record = sample_record();
        record.runtime_page_index.clear();
        let key = BlockKey {
            space: BlockSpace::Data,
            block: 7,
        };
        record.files = (0..=crate::blx::MAX_OVERLAPPING_FILES)
            .map(|index| ObjectRef {
                identity: ObjectIdentity {
                    namespace_kind: NamespaceKind::Volume,
                    namespace_id: volume.0,
                    writer_fence: record.fence,
                    object_id: u64::try_from(index).expect("index fits u64"),
                },
                min_seq: u64::try_from(index).expect("index fits u64"),
                max_seq: u64::try_from(index).expect("index fits u64"),
                batch_id: u64::try_from(index).expect("index fits u64"),
                chunk_index: 0,
                chunk_count: 1,
                first_key: key,
                last_key: key,
                pre_state_checksum: 0,
                post_state_checksum: record.post_state_checksum,
                size: 100,
                footer_offset: 50,
                footer_length: 25,
                object_checksum: u64::try_from(index + 1).expect("index fits u64"),
            })
            .collect();

        let bytes = record.encode(volume);
        assert_eq!(JournalRecord::decode(volume, &bytes), Ok(record));
    }
}
