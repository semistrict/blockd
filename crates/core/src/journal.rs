//! Journal records: one framed record per consistency point, write-once at
//! `layout::journal_blob(vset, seq)`. A record is the atomic point of the
//! vset's page→location map, carried as a bounded inline OVERLAY plus one
//! pointer per map leaf ([`crate::mapleaf`]): the overlay holds entries
//! newer than their span's leaf, lookups read overlay-then-leaf, and
//! record size is O(overlay + spans) — never O(pages). Per-vset journal
//! length stays O(1) records no matter how many checkpoints were ever
//! taken (R3.4); recovery needs intact records and the leaves they name
//! (R8.2). Archive manifests use this same encoding, with page locations
//! optionally rewritten by passive packing.

use std::collections::BTreeMap;

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::mapleaf::LeafPtr;
use crate::segment::PageLoc;
use crate::types::{
    Epoch, Gen, HostId, JournalSeq, PageId, PageNo, VolumeId, VolumeIdx, VsetId, page_size,
};

pub const MAGIC_JOURNAL: u32 = u32::from_le_bytes(*b"BJR1");

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
    Checkpoint { epoch: Epoch, vmstate: u64 },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MigrationSource {
    pub host: HostId,
    /// Exact source-writer fence whose offered cut this destination installed.
    /// Legacy records lack the value and may only use the unfenced v1/v2
    /// migration handshake.
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
    /// Present only for database vsets; compute records carry the canonical
    /// empty value.
    pub database: DatabaseMeta,
    /// Map entries newer than their span's leaf (bounded by the writer's
    /// overlay cap). Overlay wins over leaf on lookup.
    pub overlay: BTreeMap<PageId, (Gen, PageLoc)>,
    /// The rest of the captured map: one leaf blob per span.
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
            }
            RecordKind::Checkpoint { epoch, vmstate } => {
                e.u8(1);
                e.u64(epoch.0);
                e.u64(vmstate);
            }
        }
        e.u64(self.capture_seq);
        e.u64(self.sync_covered_through);
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
        // Version 6 previously encoded three per-vset policies here. The
        // peer-first policy is now the sole accepted value; retaining its
        // existing tag keeps already-created peer-first records readable.
        e.u8(2);
        if self.config.kind == VsetKind::Database {
            for file in self.database.files() {
                e.u8(u8::from(file.exists));
                e.u64(file.size);
            }
        }
        e.u32(u32::try_from(self.overlay.len()).expect("overlay count fits u32"));
        for (page, (generation, loc)) in &self.overlay {
            e.u8(page.volume.idx.0);
            e.u32(page.page.0);
            e.u64(generation.0);
            e.u64(loc.base);
            e.u64(loc.fence);
            e.u64(loc.seg.0);
            e.u32(loc.offset);
            e.u32(loc.len);
        }
        e.u32(u32::try_from(self.leaves.len()).expect("leaf count fits u32"));
        for (&span, ptr) in &self.leaves {
            e.u32(span);
            e.u64(ptr.base);
            e.u64(ptr.fence);
            e.u64(ptr.id);
        }
        seal_frame(MAGIC_JOURNAL, &e.finish())
    }

    /// Verify and decode a record. Any damage is one answer: corrupt.
    #[allow(clippy::too_many_lines)]
    pub fn decode(vset: VsetId, bytes: &[u8]) -> Result<JournalRecord, DecodeError> {
        let payload = open_frame(MAGIC_JOURNAL, bytes)?;
        let mut d = Dec::new(payload);
        let version = d.u16()?;
        if !matches!(version, 4..=7) {
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
        let kind = match kind_tag {
            0 => RecordKind::Commit,
            1 => RecordKind::Checkpoint { epoch, vmstate },
            _ => return Err(DecodeError),
        };
        let capture_seq = d.u64()?;
        let sync_covered_through = d.u64()?;
        let migrated_host = match (d.u8()?, d.u16()?) {
            (0, 0) => None,
            (1, source) => Some(HostId(source)),
            _ => return Err(DecodeError),
        };
        let offered_fence = if version >= 7 {
            match (d.u8()?, d.u64()?) {
                (0, 0) => None,
                (1, fence) => Some(fence),
                _ => return Err(DecodeError),
            }
        } else {
            None
        };
        if migrated_host.is_none() && offered_fence.is_some() {
            return Err(DecodeError);
        }
        let migrated_from = migrated_host.map(|host| MigrationSource {
            host,
            offer_fence: offered_fence,
        });
        let vset_kind = if version >= 6 {
            match d.u8()? {
                0 => VsetKind::Compute,
                1 => VsetKind::Database,
                _ => return Err(DecodeError),
            }
        } else {
            VsetKind::Compute
        };
        let config = VsetConfig {
            kind: vset_kind,
            disk_volumes: d.u8()?,
            pages_per_volume: d.u32()?,
        };
        let legacy_durability = d.u8()?;
        if !matches!((version, legacy_durability), (4..=7, 0 | 1) | (5..=7, 2)) {
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
            if !config.contains(page) {
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
        d.finish()?;
        Ok(JournalRecord {
            config,
            seq,
            fence,
            kind,
            capture_seq,
            sync_covered_through,
            database,
            overlay,
            leaves,
            migrated_from,
        })
    }

    /// Every span this record's map depends on beyond its inline overlay.
    pub fn leaf_ptrs(&self) -> impl Iterator<Item = (u32, LeafPtr)> + '_ {
        self.leaves.iter().map(|(&span, &ptr)| (span, ptr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{crc32c, open_frame, seal_frame};
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
            },
            capture_seq: 99,
            sync_covered_through: 90,
            database: DatabaseMeta::default(),
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
        assert_eq!(JournalRecord::decode(VsetId(0xA1), &bytes), Ok(record));
        // Byte pin (R10.2): any change here is a storage format change.
        let expected = match page_size() {
            4096 => (220, 0x52C3_9C5D),
            16_384 => (220, 0x8DBC_0E33),
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
        let bytes = record.encode(VsetId(0xA1));
        assert_eq!(JournalRecord::decode(VsetId(0xA1), &bytes), Ok(record));
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
            },
            capture_seq: 0,
            sync_covered_through: 0,
            database: DatabaseMeta::default(),
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let _ = record.encode(VsetId(0xD1));
    }

    /// The record for a LARGE vset must stay small: a million written
    /// pages is unremarkable in production,
    /// and the record is written per capture and uploaded per publish —
    /// its size must be O(delta + leaves), never O(pages). Before the map
    /// was sharded, this map's only representation was 45 MB of inline
    /// entries — and the R4.6 64 MiB object cap made backed vsets beyond
    /// ~1.5M pages impossible outright.
    #[test]
    fn a_big_map_encodes_small() {
        use crate::mapleaf::{LEAF_SPAN, MapLeaf};
        let vset = VsetId(0xA1);
        let loc_of = |n: u32| PageLoc {
            base: 0,
            fence: 3,
            seg: SegId(u64::from(n) / 4096),
            offset: (n % 4096) * 8,
            len: 8,
        };
        // The map as the daemon durably represents it: full spans in leaf
        // blobs, the freshest tail inline in the record's overlay.
        let total: u32 = 1_000_000;
        let overlay_from: u32 = total - 2048;
        let mut leaves = BTreeMap::new();
        let mut leaf_bytes_total = 0usize;
        let span_count = u32::try_from(u64::from(overlay_from).div_ceil(LEAF_SPAN)).expect("fits");
        for span in 0..span_count {
            let lo = span * u32::try_from(LEAF_SPAN).expect("fits");
            let hi = overlay_from.min(lo + u32::try_from(LEAF_SPAN).expect("fits"));
            let leaf = MapLeaf {
                span,
                entries: (lo..hi)
                    .map(|n| (VolumeIdx(0), PageNo(n), Gen(u64::from(n)), loc_of(n)))
                    .collect(),
            };
            let bytes = leaf.encode(vset, 3, u64::from(span));
            assert!(
                bytes.len() < 256 * 1024,
                "leaf {span} is {} bytes",
                bytes.len()
            );
            leaf_bytes_total += bytes.len();
            leaves.insert(
                span,
                LeafPtr {
                    base: 0,
                    fence: 3,
                    id: u64::from(span),
                },
            );
        }
        let overlay: BTreeMap<PageId, (Gen, PageLoc)> = (overlay_from..total)
            .map(|n| {
                (
                    PageId {
                        volume: VolumeId {
                            vset,
                            idx: VolumeIdx(0),
                        },
                        page: PageNo(n),
                    },
                    (Gen(u64::from(n)), loc_of(n)),
                )
            })
            .collect();
        let record = JournalRecord {
            config: VsetConfig {
                kind: VsetKind::Compute,
                disk_volumes: 0,
                pages_per_volume: total,
            },
            seq: JournalSeq(9),
            fence: 3,
            kind: RecordKind::Checkpoint {
                epoch: Epoch(1),
                vmstate: 42,
            },
            capture_seq: u64::from(total),
            sync_covered_through: 0,
            database: DatabaseMeta::default(),
            overlay,
            leaves,
            migrated_from: None,
        };
        let bytes = record.encode(vset);
        assert!(
            bytes.len() < 1024 * 1024,
            "a 1M-page map's record encoded to {} bytes — O(pages), not O(delta + leaves)",
            bytes.len()
        );
        // The leaves carry the bulk exactly once (~45 B/page), and every
        // object involved sits far under the R4.6 cap at any vset size.
        assert!(leaf_bytes_total > 40 * 1024 * 1024);
        assert_eq!(JournalRecord::decode(vset, &bytes), Ok(record));
    }
}
