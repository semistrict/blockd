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
use crate::mapleaf::LeafPtr;
use crate::segment::PageLoc;
use crate::types::{
    Epoch, Gen, HostId, JournalSeq, PageId, PageNo, VolumeId, VolumeIdx, VsetId, page_size,
};

pub const MAGIC_JOURNAL: u32 = u32::from_le_bytes(*b"BJR1");
const MAGIC_MIGRATION_INDEX: u32 = u32::from_le_bytes(*b"BMIX");

/// The immutable consistency/lifecycle kind of a vset (R1.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum VsetKind {
    /// Guest memory at volume zero followed by guest disk volumes.
    Compute = 0,
    /// `SQLite` main, WAL and rollback-journal files; no memory or vmstate.
    Database = 1,
}

/// One durable `SQLite` file's namespace metadata (R12.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct DatabaseFileMeta {
    pub exists: bool,
    pub size: u64,
}

/// File metadata committed atomically with a database vset's page map.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct DatabaseMeta {
    pub main: DatabaseFileMeta,
    pub wal: DatabaseFileMeta,
    pub journal: DatabaseFileMeta,
}

impl DatabaseMeta {
    fn files(self) -> [DatabaseFileMeta; 3] {
        [self.main, self.wal, self.journal]
    }

    fn is_empty(self) -> bool {
        self == Self::default()
    }

    fn valid(self, max_size: u64) -> bool {
        self.files()
            .iter()
            .all(|file| file.size <= max_size && (file.exists || file.size == 0))
    }
}

/// Immutable configuration of a vset, carried in every record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct VsetConfig {
    pub kind: VsetKind,
    /// Highest valid volume index. Compute volume zero is memory; database
    /// indices 0, 1 and 2 are main, WAL and rollback journal respectively.
    pub disk_volumes: u8,
    pub pages_per_volume: u32,
}

impl VsetConfig {
    pub const fn compute(disk_volumes: u8, pages_per_volume: u32) -> Self {
        Self {
            kind: VsetKind::Compute,
            disk_volumes,
            pages_per_volume,
        }
    }

    pub const fn database(pages_per_file: u32) -> Self {
        Self {
            kind: VsetKind::Database,
            disk_volumes: 2,
            pages_per_volume: pages_per_file,
        }
    }

    /// All logical volumes/files of the vset.
    pub fn volumes(&self, vset: VsetId) -> impl Iterator<Item = VolumeId> + use<> {
        (0..=self.disk_volumes).map(move |idx| VolumeId {
            vset,
            idx: VolumeIdx(idx),
        })
    }

    pub fn contains(&self, page: PageId) -> bool {
        page.volume.idx.0 <= self.disk_volumes && page.page.0 < self.pages_per_volume
    }

    pub fn is_memory(&self, idx: VolumeIdx) -> bool {
        self.kind == VsetKind::Compute && idx.is_memory()
    }

    fn valid(self) -> bool {
        self.pages_per_volume > 0
            && match self.kind {
                VsetKind::Compute => true,
                VsetKind::Database => self.disk_volumes == 2,
            }
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
    pub sync_covered_through: u64,
    /// Checksum of the complete logical state represented by this record.
    /// It is captured with the record so delayed publication cannot observe
    /// a newer in-memory state.
    pub post_state_checksum: u64,
    /// Present only for database vsets; compute records carry the canonical
    /// empty value.
    pub database: DatabaseMeta,
    /// The BLX files needed by this recovery point. This is the durable data
    /// index; page locations below are runtime-only lookup state.
    pub files: Vec<ObjectRef>,
    /// Runtime-only lookup state used by migration messages.
    pub overlay: BTreeMap<PageId, (Gen, PageLoc)>,
    /// Runtime-only auxiliary lookup state used by migration messages.
    pub leaves: BTreeMap<u32, LeafPtr>,
    /// Migration provenance (R7.2): while an in-migrated vset still
    /// hydrates from its source, every record names that source. The
    /// destination's first record — the durable ACCEPT of the handoff —
    /// must let a recovery finish the handshake it interrupts: answer the
    /// source's re-offers and keep pulling the tail. Cleared by the first
    /// capture after `Released`.
    pub migrated_from: Option<MigrationSource>,
}

impl JournalRecord {
    pub fn encode(&self, vset: VsetId) -> Vec<u8> {
        assert!(self.config.valid(), "invalid vset config");
        assert!(
            match self.config.kind {
                VsetKind::Compute => self.database.is_empty(),
                VsetKind::Database => {
                    matches!(self.kind, RecordKind::Commit)
                        && self
                            .database
                            .valid(u64::from(self.config.pages_per_volume) * page_size() as u64)
                }
            },
            "record kind/metadata disagrees with vset kind"
        );
        let mut e = Enc::new();
        e.u16(7); // version: accepted migration source fence
        e.u32(u32::try_from(page_size()).expect("page size fits u32"));
        e.u64(vset.0);
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
        e.u8(self.config.disk_volumes);
        e.u32(self.config.pages_per_volume);
        // The sole current durability policy is primary plus one passive.
        e.u8(2);
        if self.config.kind == VsetKind::Database {
            for file in self.database.files() {
                e.u8(u8::from(file.exists));
                e.u64(file.size);
            }
        }
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
    pub fn encode_migration(&self, vset: VsetId) -> Vec<u8> {
        self.encode_migration_with_checksums(vset, &BTreeMap::new())
    }

    pub fn encode_migration_with_checksums(
        &self,
        vset: VsetId,
        block_checksums: &BTreeMap<BlockKey, (Gen, u64)>,
    ) -> Vec<u8> {
        let durable = self.encode(vset);
        let mut index = Enc::new();
        index.u32(u32::try_from(self.overlay.len()).expect("overlay count fits u32"));
        for (page, (generation, loc)) in &self.overlay {
            index.u8(page.volume.idx.0);
            index.u32(page.page.0);
            index.u64(generation.0);
            index.u64(loc.base);
            index.u64(loc.fence);
            index.u64(loc.seg.0);
            index.u32(loc.offset);
            index.u32(loc.len);
        }
        index.u32(u32::try_from(self.leaves.len()).expect("leaf count fits u32"));
        for (&span, ptr) in &self.leaves {
            index.u32(span);
            index.u64(ptr.base);
            index.u64(ptr.fence);
            index.u64(ptr.id);
        }
        index.u32(u32::try_from(block_checksums.len()).expect("block checksum count fits u32"));
        for (key, (generation, checksum)) in block_checksums {
            index.u8(key.space as u8);
            index.u8(key.volume);
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
    pub fn decode(vset: VsetId, bytes: &[u8]) -> Result<JournalRecord, DecodeError> {
        let payload = open_frame(MAGIC_JOURNAL, bytes)?;
        let mut d = Dec::new(payload);
        let version = d.u16()?;
        if version != 7 {
            return Err(DecodeError);
        }
        if d.u32()? != u32::try_from(page_size()).expect("page size fits u32") {
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
        let vset_kind = match d.u8()? {
            0 => VsetKind::Compute,
            1 => VsetKind::Database,
            _ => return Err(DecodeError),
        };
        let config = VsetConfig {
            kind: vset_kind,
            disk_volumes: d.u8()?,
            pages_per_volume: d.u32()?,
        };
        let durability_tag = d.u8()?;
        if durability_tag != 2 {
            return Err(DecodeError);
        }
        if !config.valid() {
            return Err(DecodeError);
        }
        let database = if config.kind == VsetKind::Database {
            let mut file = || -> Result<DatabaseFileMeta, DecodeError> {
                let exists = match d.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(DecodeError),
                };
                let size = d.u64()?;
                let max_size = u64::from(config.pages_per_volume) * page_size() as u64;
                if (!exists && size != 0) || size > max_size {
                    return Err(DecodeError);
                }
                Ok(DatabaseFileMeta { exists, size })
            };
            DatabaseMeta {
                main: file()?,
                wal: file()?,
                journal: file()?,
            }
        } else {
            DatabaseMeta::default()
        };
        if config.kind == VsetKind::Database && !matches!(kind, RecordKind::Commit) {
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
            database,
            files,
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from,
        })
    }

    pub fn decode_migration(vset: VsetId, bytes: &[u8]) -> Result<JournalRecord, DecodeError> {
        Self::decode_migration_with_checksums(vset, bytes).map(|(record, _)| record)
    }

    pub fn decode_migration_with_checksums(
        vset: VsetId,
        bytes: &[u8],
    ) -> Result<(JournalRecord, BTreeMap<BlockKey, (Gen, u64)>), DecodeError> {
        let prefix = bytes.get(..8).ok_or(DecodeError)?;
        let durable_len = usize::try_from(u64::from_le_bytes(
            prefix.try_into().map_err(|_| DecodeError)?,
        ))
        .map_err(|_| DecodeError)?;
        let durable_end = 8usize.checked_add(durable_len).ok_or(DecodeError)?;
        let mut record = Self::decode(vset, bytes.get(8..durable_end).ok_or(DecodeError)?)?;
        let payload = open_frame(
            MAGIC_MIGRATION_INDEX,
            bytes.get(durable_end..).ok_or(DecodeError)?,
        )?;
        let mut d = Dec::new(payload);
        let count = d.u32()?;
        let mut overlay = BTreeMap::new();
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
            if !record.config.contains(page) {
                return Err(DecodeError);
            }
            if overlay.insert(page, (generation, loc)).is_some() {
                return Err(DecodeError);
            }
        }
        let leaf_count = d.u32()?;
        let mut leaves = BTreeMap::new();
        for _ in 0..leaf_count {
            let span = d.u32()?;
            let ptr = LeafPtr {
                base: d.u64()?,
                fence: d.u64()?,
                id: d.u64()?,
            };
            if leaves.insert(span, ptr).is_some() {
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
                volume: d.u8()?,
                block: d.u32()?,
            };
            let generation = Gen(d.u64()?);
            let checksum = d.u64()?;
            let valid = match key.space {
                BlockSpace::Memory => {
                    record.config.kind == VsetKind::Compute
                        && key.volume == 0
                        && key.block < record.config.pages_per_volume
                }
                BlockSpace::Vmm => record.config.kind == VsetKind::Compute && key.volume == 0,
                BlockSpace::Data => {
                    key.volume <= record.config.disk_volumes
                        && key.block < record.config.pages_per_volume
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
        record.overlay = overlay;
        record.leaves = leaves;
        Ok((record, block_checksums))
    }

    /// Every span this record's map depends on beyond its inline overlay.
    pub fn leaf_ptrs(&self) -> impl Iterator<Item = (u32, LeafPtr)> + '_ {
        self.leaves.iter().map(|(&span, &ptr)| (span, ptr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blx::{BlockKey, BlockSpace, NamespaceKind};
    use crate::format::{crc32c, open_frame, seal_frame};
    use crate::manifest::ObjectIdentity;
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
                kind: VsetKind::Compute,
                disk_volumes: 2,
                pages_per_volume: 16,
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
            database: DatabaseMeta::default(),
            files: Vec::new(),
            overlay: pages,
            leaves: BTreeMap::from([(
                0x100,
                LeafPtr {
                    base: 0,
                    fence: 4,
                    id: 11,
                },
            )]),
            migrated_from: Some(MigrationSource {
                host: HostId(2),
                offer_fence: Some(5),
            }),
        }
    }

    #[test]
    fn records_round_trip_and_are_byte_pinned() {
        let record = sample_record();
        let bytes = record.encode(VsetId(0xA1));
        let mut durable = record.clone();
        durable.overlay.clear();
        durable.leaves.clear();
        assert_eq!(JournalRecord::decode(VsetId(0xA1), &bytes), Ok(durable));
        // Byte pin (R10.2): any change here is a storage format change.
        let expected = match page_size() {
            4096 => (114, 0xF261_B291),
            16_384 => (114, 0x36DF_2844),
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
        let bytes = record.encode_migration(VsetId(0xA1));
        assert_eq!(
            JournalRecord::decode_migration(VsetId(0xA1), &bytes),
            Ok(record)
        );
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

    #[test]
    fn records_reject_a_different_system_page_size() {
        let bytes = sample_record().encode(VsetId(0xA1));
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
        assert!(JournalRecord::decode(VsetId(0xA1), &incompatible).is_err());
    }

    #[test]
    fn database_records_round_trip_with_file_metadata() {
        let record = JournalRecord {
            config: VsetConfig::database(1024),
            seq: JournalSeq(7),
            fence: 3,
            kind: RecordKind::Commit,
            capture_seq: 18,
            sync_covered_through: 16,
            post_state_checksum: 0,
            database: DatabaseMeta {
                main: DatabaseFileMeta {
                    exists: true,
                    size: 8192,
                },
                wal: DatabaseFileMeta {
                    exists: true,
                    size: 512,
                },
                journal: DatabaseFileMeta::default(),
            },
            files: Vec::new(),
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let bytes = record.encode(VsetId(0xD1));
        assert_eq!(JournalRecord::decode(VsetId(0xD1), &bytes), Ok(record));
    }

    #[test]
    #[should_panic(expected = "record kind/metadata disagrees with vset kind")]
    fn database_records_cannot_carry_vmstate() {
        let record = JournalRecord {
            config: VsetConfig::database(1),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Checkpoint {
                epoch: Epoch(0),
                vmstate: 0,
                vmstate_logical_length: 0,
            },
            capture_seq: 0,
            sync_covered_through: 0,
            post_state_checksum: 0,
            database: DatabaseMeta::default(),
            files: Vec::new(),
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let _ = record.encode(VsetId(0xD1));
    }

    #[test]
    fn runtime_page_index_is_not_persisted() {
        let vset = VsetId(0xA1);
        let record = sample_record();
        let mut without_runtime_index = record.clone();
        without_runtime_index.overlay.clear();
        without_runtime_index.leaves.clear();

        assert_eq!(record.encode(vset), without_runtime_index.encode(vset));
        assert_eq!(
            JournalRecord::decode(vset, &record.encode(vset)),
            Ok(without_runtime_index)
        );
    }

    #[test]
    fn uncompacted_local_files_may_exceed_the_archive_overlap_limit() {
        let vset = VsetId(0xA1);
        let mut record = sample_record();
        record.overlay.clear();
        record.leaves.clear();
        let key = BlockKey {
            space: BlockSpace::Data,
            volume: 1,
            block: 7,
        };
        record.files = (0..=crate::blx::MAX_OVERLAPPING_FILES)
            .map(|index| ObjectRef {
                identity: ObjectIdentity {
                    namespace_kind: NamespaceKind::Vset,
                    namespace_id: vset.0,
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

        let bytes = record.encode(vset);
        assert_eq!(JournalRecord::decode(vset, &bytes), Ok(record));
    }
}
