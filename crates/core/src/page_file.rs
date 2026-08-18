//! Runtime page locations and capture batching over durable BLX objects.

use crate::blx::{
    BatchMeta, BlockKey, BlxBatchBuilder, BlxEntry, BlxObject, NamespaceKind, vmm_snapshot_blocks,
};
use crate::format::DecodeError;
use crate::journal::VolumeKind;
use crate::manifest::ObjectIdentity;
use crate::types::{Gen, ObjectId, PageId, VolumeId, page_size};

/// Where a page's durable bytes live: a byte range of one BLX object
/// covering the page's whole framed entry. Ranged reads of exactly this span
/// are the fault path.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PageFileLoc {
    /// 0 for the volume's own namespace; otherwise the base id whose shared
    /// BLX objects hold this page (R5.3: forks reference, never copy).
    pub base: u64,
    /// The writer incarnation that produced the object (its namespace).
    pub fence: u64,
    pub object: ObjectId,
    pub offset: u32,
    pub len: u32,
}

impl PageFileLoc {
    pub const fn identity(self, volume: VolumeId) -> ObjectIdentity {
        ObjectIdentity::volume(volume, self.fence, self.object.0)
    }
}

/// Every page a BLX object holds: identity, generation, and byte range.
pub type PageFileEntries = Vec<(PageId, Gen, PageFileLoc)>;

/// Builds one capture's page entries into consecutive, independently bounded
/// BLX objects. Rotation happens before an entry, so an entry is never split.
#[derive(Debug)]
pub struct PageBatchBuilder {
    kind: VolumeKind,
    volume: VolumeId,
    fence: u64,
    builder: BlxBatchBuilder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectIdOverflow;

impl PageBatchBuilder {
    pub fn new(volume: VolumeId, fence: u64, first_object: ObjectId) -> Self {
        Self::new_with_checksums(VolumeKind::Data, volume, fence, first_object, 0, 0, 0)
    }

    pub fn new_with_checksums(
        kind: VolumeKind,
        volume: VolumeId,
        fence: u64,
        first_object: ObjectId,
        sequence: u64,
        pre_state_checksum: u64,
        post_state_checksum: u64,
    ) -> Self {
        let meta = BatchMeta {
            namespace_kind: NamespaceKind::Volume,
            namespace_id: volume.0,
            writer_fence: fence,
            first_object_id: first_object.0,
            min_seq: sequence,
            max_seq: sequence,
            batch_id: sequence,
            pre_state_checksum,
            post_state_checksum,
        };
        Self {
            kind,
            volume,
            fence,
            builder: BlxBatchBuilder::new_partitioned(meta),
        }
    }

    pub fn add(&mut self, page: PageId, generation: Gen, page_bytes: &[u8]) {
        self.try_add(page, generation, page_bytes)
            .expect("BLX object id overflow");
    }

    pub fn try_add(
        &mut self,
        page: PageId,
        generation: Gen,
        page_bytes: &[u8],
    ) -> Result<(), ObjectIdOverflow> {
        assert_eq!(page_bytes.len(), page_size(), "BLX stores whole pages");
        assert_eq!(page.volume, self.volume, "BLX object is per-volume");
        self.builder.add_data(
            BlockKey::from_page(self.kind, page),
            generation,
            page_bytes.to_vec(),
        );
        self.builder
            .object_ids_fit()
            .then_some(())
            .ok_or(ObjectIdOverflow)
    }

    pub fn try_add_tombstone(
        &mut self,
        page: PageId,
        generation: Gen,
    ) -> Result<(), ObjectIdOverflow> {
        assert_eq!(page.volume, self.volume, "BLX object is per-volume");
        self.builder
            .add_tombstone(BlockKey::from_page(self.kind, page), generation);
        self.builder
            .object_ids_fit()
            .then_some(())
            .ok_or(ObjectIdOverflow)
    }

    /// Store a canonical VMM snapshot as padded BLX blocks. The manifest
    /// carries the exact logical byte length.
    pub fn add_vmm_snapshot(&mut self, generation: Gen, bytes: &[u8]) {
        for (key, padded) in vmm_snapshot_blocks(bytes) {
            self.builder.add_data(key, generation, padded);
        }
    }

    pub fn add_vmm_block(&mut self, block: u32, generation: Gen, bytes: &[u8]) {
        assert_eq!(bytes.len(), page_size(), "BLX stores whole VMM blocks");
        self.builder.add_data(
            BlockKey {
                space: crate::blx::BlockSpace::Vmm,
                block,
            },
            generation,
            bytes.to_vec(),
        );
    }

    pub fn add_vmm_tombstone(&mut self, block: u32, generation: Gen) {
        self.builder.add_tombstone(
            BlockKey {
                space: crate::blx::BlockSpace::Vmm,
                block,
            },
            generation,
        );
    }

    pub fn finish(self) -> Vec<(ObjectId, Vec<u8>, PageFileEntries)> {
        self.builder
            .finish()
            .into_iter()
            .map(|object| {
                let object_id = ObjectId(object.header.object_id);
                let entries = object
                    .footer
                    .entries
                    .iter()
                    .filter(|entry| entry.kind == crate::blx::EntryKind::Data)
                    .filter_map(|entry| {
                        let page = entry.key.to_page_id(self.volume)?;
                        Some((
                            page,
                            entry.generation,
                            PageFileLoc {
                                base: 0,
                                fence: self.fence,
                                object: object_id,
                                offset: entry.offset,
                                len: entry.length,
                            },
                        ))
                    })
                    .collect();
                (object_id, object.bytes, entries)
            })
            .collect()
    }
}

/// Decode and verify one entry read by ranged read (the fault path). Returns
/// the page identity, generation and the decompressed page bytes.
pub fn open_entry(volume: VolumeId, bytes: &[u8]) -> Result<(PageId, Gen, Vec<u8>), DecodeError> {
    match BlxEntry::open(bytes)? {
        BlxEntry::Data {
            key,
            generation,
            bytes,
        } => Ok((
            key.to_page_id(volume).ok_or(DecodeError)?,
            generation,
            bytes,
        )),
        BlxEntry::Tombstone { .. } => Err(DecodeError),
    }
}

/// Scan a whole blx blob (recovery, hydration, backup verification),
/// checksumming the header and every stored entry and returning identities
/// and locations. Individual entries are decompressed only when consumed.
pub fn scan_page_file(
    bytes: &[u8],
) -> Result<(VolumeId, u64, ObjectId, PageFileEntries), DecodeError> {
    let (header, footer) = BlxObject::scan(bytes)?;
    if header.namespace_kind != NamespaceKind::Volume {
        return Err(DecodeError);
    }
    let volume = VolumeId(header.namespace_id);
    let fence = header.writer_fence;
    let object = ObjectId(header.object_id);
    let entries = footer
        .entries
        .into_iter()
        .filter(|entry| entry.kind == crate::blx::EntryKind::Data)
        .filter_map(|entry| {
            let page = match entry.key.to_page_id(volume) {
                Some(page) => page,
                None if entry.key.space == crate::blx::BlockSpace::Vmm => return None,
                None => return Some(Err(DecodeError)),
            };
            Some(Ok((
                page,
                entry.generation,
                PageFileLoc {
                    base: 0,
                    fence,
                    object,
                    offset: entry.offset,
                    len: entry.length,
                },
            )))
        })
        .collect::<Result<Vec<_>, DecodeError>>()?;
    Ok((volume, fence, object, entries))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::blx::{BlxHeader, MAGIC_HEADER, TARGET_FILE_BYTES};
    use crate::format::{crc32c, open_frame, seal_frame};
    use crate::types::PageNo;

    fn sample_page(_kind: u8, page: u32) -> PageId {
        PageId {
            volume: VolumeId(0xA1),
            page: PageNo(page),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn pattern_page(fill: u8) -> Vec<u8> {
        (0..page_size()).map(|i| fill ^ (i as u8)).collect()
    }

    fn sample_blx() -> (Vec<u8>, PageFileEntries) {
        let mut b = PageBatchBuilder::new(VolumeId(0xA1), 6, ObjectId(2));
        b.add(sample_page(0, 3), Gen(7), &pattern_page(0x55));
        b.add(sample_page(1, 0), Gen(9), &pattern_page(0xAA));
        let (_, bytes, entries) = b.finish().pop().expect("sample object");
        (bytes, entries)
    }

    #[test]
    fn a_batch_splits_incompressible_pages_without_losing_or_repeating_any() {
        let volume = VolumeId(0xA1);
        // Enough pseudo-random pages to exceed the cap even if LZ4 happens to
        // shave a little from each one.
        let pages = TARGET_FILE_BYTES / page_size() + 100;
        let mut batch = PageBatchBuilder::new(volume, 6, ObjectId(10));
        let mut expected = Vec::with_capacity(pages);
        for n in 0..pages {
            let page = sample_page(0, u32::try_from(n).expect("test page fits"));
            let generation = Gen(u64::try_from(n).expect("test generation fits"));
            let mut x = u64::try_from(n).expect("page count fits") ^ 0xA076_1D64_78BD_642F;
            let raw: Vec<u8> = (0..page_size())
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    u8::try_from(x & 0xFF).expect("masked to a byte")
                })
                .collect();
            batch.add(page, generation, &raw);
            expected.push((page, generation));
        }
        let blobs = batch.finish();
        assert!(blobs.len() >= 2, "cap-crossing batch must rotate");
        let mut actual = Vec::new();
        for (expected_seg, blob, locs) in blobs {
            assert!(blob.len() <= TARGET_FILE_BYTES);
            let (got_volume, fence, got_seg, scanned) = scan_page_file(&blob).expect("blx scans");
            assert_eq!((got_volume, fence, got_seg), (volume, 6, expected_seg));
            assert_eq!(scanned, locs);
            actual.extend(
                locs.into_iter()
                    .map(|(page, generation, _)| (page, generation)),
            );
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn batch_rotation_reports_object_id_overflow_before_building_a_successor() {
        let mut batch = PageBatchBuilder::new(VolumeId(0xA1), 6, ObjectId(u64::MAX));
        let page = pattern_page(0x55);
        let mut result = Ok(());
        for n in 0..(TARGET_FILE_BYTES / page_size() + 2) {
            result = batch.try_add(
                sample_page(0, u32::try_from(n).expect("page fits")),
                Gen(u64::try_from(n).expect("generation fits")),
                &page,
            );
            if result.is_err() {
                break;
            }
        }
        assert_eq!(result, Err(ObjectIdOverflow));
    }

    #[test]
    fn one_mib_delta_is_one_bounded_exactly_reconstructable_closure() {
        let volume = VolumeId(0xA1);
        let pages = (1024 * 1024) / page_size();
        let mut batch = PageBatchBuilder::new(volume, 6, ObjectId(40));
        let mut expected = BTreeMap::new();
        for n in 0..pages {
            let page = sample_page(0, u32::try_from(n).expect("page fits"));
            let mut x = u64::try_from(n).expect("fits") ^ 0xE703_7ED1_A0B4_28DB;
            let raw: Vec<u8> = (0..page_size())
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    u8::try_from(x & 0xFF).expect("byte")
                })
                .collect();
            expected.insert(page, raw.clone());
            batch.add(page, Gen(u64::try_from(n).expect("fits")), &raw);
        }
        assert_eq!(expected.len() * page_size(), 1024 * 1024);
        let blobs = batch.finish();
        assert_eq!(blobs.len(), 1);
        let (_, blob, locs) = &blobs[0];
        assert!(blob.len() <= TARGET_FILE_BYTES);
        for (page, generation, loc) in locs {
            let start = usize::try_from(loc.offset).expect("offset fits");
            let end = start + usize::try_from(loc.len).expect("length fits");
            let (got_page, got_generation, bytes) =
                open_entry(volume, &blob[start..end]).expect("entry");
            assert_eq!((got_page, got_generation), (*page, *generation));
            assert_eq!(bytes, expected[page]);
        }
    }

    #[test]
    fn blx_files_round_trip_scan_and_ranged_reads() {
        let volume = VolumeId(0xA1);
        let (blob, locs) = sample_blx();

        let (scanned_volume, fence, object, entries) =
            scan_page_file(&blob).expect("intact blx scans");
        assert_eq!((scanned_volume, fence, object), (volume, 6, ObjectId(2)));
        assert_eq!(entries, locs);

        for (page, generation, loc) in &locs {
            let start = usize::try_from(loc.offset).unwrap();
            let end = start + usize::try_from(loc.len).unwrap();
            let (got_page, got_gen, bytes) =
                open_entry(volume, &blob[start..end]).expect("entry opens");
            assert_eq!((got_page, got_gen), (*page, *generation));
            let expected = if *generation == Gen(7) {
                pattern_page(0x55)
            } else {
                pattern_page(0xAA)
            };
            assert_eq!(bytes, expected);
        }
    }

    #[test]
    fn blx_files_are_byte_pinned() {
        let (blob, _) = sample_blx();
        // Pins the whole stack including lz4 output (R10.2): a dependency
        // update that changes compressed bytes is a storage format change
        // and must be seen.
        let expected = match page_size() {
            4096 => (863, 0xE1D9_E331),
            16_384 => (961, 0xA60B_82DB),
            size => panic!("byte pin missing for {size}-byte pages"),
        };
        assert_eq!((blob.len(), crc32c(&blob)), expected);
    }

    #[test]
    fn blx_files_reject_a_different_system_page_size() {
        let (blob, _) = sample_blx();
        let (_, header_len) = BlxHeader::open(&blob).expect("BLX header");
        let mut payload = open_frame(MAGIC_HEADER, &blob[..header_len])
            .expect("blx header")
            .to_vec();
        let incompatible = if page_size() == 4096 {
            16_384u32
        } else {
            4096u32
        };
        payload[2..6].copy_from_slice(&incompatible.to_le_bytes());
        let mut incompatible = seal_frame(MAGIC_HEADER, &payload);
        incompatible.extend_from_slice(&blob[header_len..]);
        assert!(scan_page_file(&incompatible).is_err());
    }

    #[test]
    fn torn_blx_files_and_damaged_entries_are_rejected() {
        let volume = VolumeId(0xA1);
        let (blob, locs) = sample_blx();

        for keep in 0..blob.len() {
            assert!(
                scan_page_file(&blob[..keep]).is_err(),
                "torn blx (kept {keep}) went undetected"
            );
        }

        let loc = locs[1].2;
        let start = usize::try_from(loc.offset).unwrap();
        let end = start + usize::try_from(loc.len).unwrap();
        let mut entry = blob[start..end].to_vec();
        entry[20] ^= 0x40;
        assert!(open_entry(volume, &entry).is_err());
    }

    #[test]
    fn blx_scans_reject_any_single_bit_flip() {
        let (blob, _) = sample_blx();
        for byte in 0..blob.len() {
            for bit in 0..8 {
                let mut damaged = blob.clone();
                damaged[byte] ^= 1 << bit;
                assert!(
                    scan_page_file(&damaged).is_err(),
                    "bit {bit} of byte {byte} escaped the blx scan"
                );
            }
        }
    }
}
