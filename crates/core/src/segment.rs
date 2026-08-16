//! Compatibility API for the live engine's page locations.
//!
//! The durable bytes are BLX objects. The engine still calls them segments
//! internally while the page-map removal is completed, but there is no second
//! page-data encoding behind this module.

use crate::blx::{
    BatchMeta, BlockKey, BlxBatchBuilder, BlxEntry, NamespaceKind, TARGET_FILE_BYTES,
    open_entry as open_blx_entry, scan_object, vmm_snapshot_blocks,
};
use crate::format::DecodeError;
use crate::journal::VsetKind;
use crate::types::{Gen, PageId, SegId, VsetId, page_size};

/// The writer rotates near the BLX target. Every resulting object remains
/// below the format's separate 64 MiB hard limit.
pub const MAX_SEGMENT_BYTES: usize = TARGET_FILE_BYTES;

/// Where a page's durable bytes live: a byte range of one segment blob
/// covering the page's whole framed entry. Ranged reads of exactly this span
/// are the fault path.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PageLoc {
    /// 0 for the vset's own namespace; otherwise the base id whose shared
    /// segments hold this page (R5.3: forks reference, never copy).
    pub base: u64,
    /// The writer incarnation that produced the segment (its namespace).
    pub fence: u64,
    pub seg: SegId,
    pub offset: u32,
    pub len: u32,
}

/// Every page a segment holds: identity, generation, and byte range.
pub type SegmentEntries = Vec<(PageId, Gen, PageLoc)>;

/// Builds one capture's page entries into consecutive, independently bounded
/// segment blobs. Rotation happens before an entry, so an entry is never split.
#[derive(Debug)]
pub struct SegmentBatchBuilder {
    kind: VsetKind,
    vset: VsetId,
    fence: u64,
    builder: BlxBatchBuilder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentIdOverflow;

impl SegmentBatchBuilder {
    pub fn new(vset: VsetId, fence: u64, first_seg: SegId) -> Self {
        Self::new_for_kind(VsetKind::Compute, vset, fence, first_seg)
    }

    pub fn new_for_kind(kind: VsetKind, vset: VsetId, fence: u64, first_seg: SegId) -> Self {
        Self::new_for_record(kind, vset, fence, first_seg, 0)
    }

    pub fn new_for_record(
        kind: VsetKind,
        vset: VsetId,
        fence: u64,
        first_seg: SegId,
        sequence: u64,
    ) -> Self {
        Self::new_for_record_with_checksums(kind, vset, fence, first_seg, sequence, 0, 0)
    }

    pub fn new_for_record_with_checksums(
        kind: VsetKind,
        vset: VsetId,
        fence: u64,
        first_seg: SegId,
        sequence: u64,
        pre_state_checksum: u64,
        post_state_checksum: u64,
    ) -> Self {
        let meta = BatchMeta {
            namespace_kind: NamespaceKind::Vset,
            namespace_id: vset.0,
            writer_fence: fence,
            first_object_id: first_seg.0,
            min_seq: sequence,
            max_seq: sequence,
            batch_id: sequence,
            pre_state_checksum,
            post_state_checksum,
        };
        Self {
            kind,
            vset,
            fence,
            builder: BlxBatchBuilder::new_partitioned(meta),
        }
    }

    pub fn add(&mut self, page: PageId, generation: Gen, page_bytes: &[u8]) {
        self.try_add(page, generation, page_bytes)
            .expect("segment id overflow");
    }

    pub fn try_add(
        &mut self,
        page: PageId,
        generation: Gen,
        page_bytes: &[u8],
    ) -> Result<(), SegmentIdOverflow> {
        assert_eq!(page_bytes.len(), page_size(), "BLX stores whole pages");
        assert_eq!(page.volume.vset, self.vset, "BLX object is per-vset");
        self.builder.add_data(
            BlockKey::from_page(self.kind, page),
            generation,
            page_bytes.to_vec(),
        );
        self.builder
            .object_ids_fit()
            .then_some(())
            .ok_or(SegmentIdOverflow)
    }

    pub fn try_add_tombstone(
        &mut self,
        page: PageId,
        generation: Gen,
    ) -> Result<(), SegmentIdOverflow> {
        assert_eq!(page.volume.vset, self.vset, "BLX object is per-vset");
        self.builder
            .add_tombstone(BlockKey::from_page(self.kind, page), generation);
        self.builder
            .object_ids_fit()
            .then_some(())
            .ok_or(SegmentIdOverflow)
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
                volume: 0,
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
                volume: 0,
                block,
            },
            generation,
        );
    }

    pub fn finish(self) -> Vec<(SegId, Vec<u8>, SegmentEntries)> {
        self.builder
            .finish()
            .into_iter()
            .map(|object| {
                let seg = SegId(object.header.object_id);
                let entries = object
                    .footer
                    .entries
                    .iter()
                    .filter(|entry| entry.kind == crate::blx::EntryKind::Data)
                    .filter_map(|entry| {
                        let page = entry.key.to_page_id(self.vset)?;
                        Some((
                            page,
                            entry.generation,
                            PageLoc {
                                base: 0,
                                fence: self.fence,
                                seg,
                                offset: entry.offset,
                                len: entry.length,
                            },
                        ))
                    })
                    .collect();
                (seg, object.bytes, entries)
            })
            .collect()
    }
}

/// Decode and verify one entry read by ranged read (the fault path). Returns
/// the page identity, generation and the decompressed page bytes.
pub fn open_entry(vset: VsetId, bytes: &[u8]) -> Result<(PageId, Gen, Vec<u8>), DecodeError> {
    match open_blx_entry(bytes)? {
        BlxEntry::Data {
            key,
            generation,
            bytes,
        } => Ok((key.to_page_id(vset).ok_or(DecodeError)?, generation, bytes)),
        BlxEntry::Tombstone { .. } => Err(DecodeError),
    }
}

/// Scan a whole segment blob (recovery, hydration, backup verification),
/// checksumming the header and every stored entry and returning identities
/// and locations. Individual entries are decompressed only when consumed.
pub fn scan_segment(bytes: &[u8]) -> Result<(VsetId, u64, SegId, SegmentEntries), DecodeError> {
    let (header, footer) = scan_object(bytes)?;
    if header.namespace_kind != NamespaceKind::Vset {
        return Err(DecodeError);
    }
    let vset = VsetId(header.namespace_id);
    let fence = header.writer_fence;
    let seg = SegId(header.object_id);
    let entries = footer
        .entries
        .into_iter()
        .filter(|entry| entry.kind == crate::blx::EntryKind::Data)
        .filter_map(|entry| {
            let page = match entry.key.to_page_id(vset) {
                Some(page) => page,
                None if entry.key.space == crate::blx::BlockSpace::Vmm => return None,
                None => return Some(Err(DecodeError)),
            };
            Some(Ok((
                page,
                entry.generation,
                PageLoc {
                    base: 0,
                    fence,
                    seg,
                    offset: entry.offset,
                    len: entry.length,
                },
            )))
        })
        .collect::<Result<Vec<_>, DecodeError>>()?;
    Ok((vset, fence, seg, entries))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::blx::{MAGIC_HEADER, open_header};
    use crate::format::{crc32c, open_frame, seal_frame};
    use crate::types::{PageNo, VolumeId, VolumeIdx};

    fn sample_page(volume: u8, page: u32) -> PageId {
        PageId {
            volume: VolumeId {
                vset: VsetId(0xA1),
                idx: VolumeIdx(volume),
            },
            page: PageNo(page),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn pattern_page(fill: u8) -> Vec<u8> {
        (0..page_size()).map(|i| fill ^ (i as u8)).collect()
    }

    fn sample_segment() -> (Vec<u8>, SegmentEntries) {
        let mut b = SegmentBatchBuilder::new(VsetId(0xA1), 6, SegId(2));
        b.add(sample_page(0, 3), Gen(7), &pattern_page(0x55));
        b.add(sample_page(1, 0), Gen(9), &pattern_page(0xAA));
        let (_, bytes, entries) = b.finish().pop().expect("sample object");
        (bytes, entries)
    }

    #[test]
    fn a_batch_splits_incompressible_pages_without_losing_or_repeating_any() {
        let vset = VsetId(0xA1);
        // Enough pseudo-random pages to exceed the cap even if LZ4 happens to
        // shave a little from each one.
        let pages = MAX_SEGMENT_BYTES / page_size() + 100;
        let mut batch = SegmentBatchBuilder::new(vset, 6, SegId(10));
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
            assert!(blob.len() <= MAX_SEGMENT_BYTES);
            let (got_vset, fence, got_seg, scanned) = scan_segment(&blob).expect("segment scans");
            assert_eq!((got_vset, fence, got_seg), (vset, 6, expected_seg));
            assert_eq!(scanned, locs);
            actual.extend(
                locs.into_iter()
                    .map(|(page, generation, _)| (page, generation)),
            );
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn batch_rotation_reports_segment_id_overflow_before_building_a_successor() {
        let mut batch = SegmentBatchBuilder::new(VsetId(0xA1), 6, SegId(u64::MAX));
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
        assert_eq!(result, Err(SegmentIdOverflow));
    }

    #[test]
    fn one_mib_delta_is_one_bounded_exactly_reconstructable_closure() {
        let vset = VsetId(0xA1);
        let pages = (1024 * 1024) / page_size();
        let mut batch = SegmentBatchBuilder::new(vset, 6, SegId(40));
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
        assert!(blob.len() <= MAX_SEGMENT_BYTES);
        for (page, generation, loc) in locs {
            let start = usize::try_from(loc.offset).expect("offset fits");
            let end = start + usize::try_from(loc.len).expect("length fits");
            let (got_page, got_generation, bytes) =
                open_entry(vset, &blob[start..end]).expect("entry");
            assert_eq!((got_page, got_generation), (*page, *generation));
            assert_eq!(bytes, expected[page]);
        }
    }

    #[test]
    fn segments_round_trip_scan_and_ranged_reads() {
        let vset = VsetId(0xA1);
        let (blob, locs) = sample_segment();

        let (scanned_vset, fence, seg, entries) =
            scan_segment(&blob).expect("intact segment scans");
        assert_eq!((scanned_vset, fence, seg), (vset, 6, SegId(2)));
        assert_eq!(entries, locs);

        for (page, generation, loc) in &locs {
            let start = usize::try_from(loc.offset).unwrap();
            let end = start + usize::try_from(loc.len).unwrap();
            let (got_page, got_gen, bytes) =
                open_entry(vset, &blob[start..end]).expect("entry opens");
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
    fn segment_blobs_are_byte_pinned() {
        let (blob, _) = sample_segment();
        // Pins the whole stack including lz4 output (R10.2): a dependency
        // update that changes compressed bytes is a storage format change
        // and must be seen.
        let expected = match page_size() {
            4096 => (863, 0x4219_DF1A),
            16_384 => (961, 0x301D_DAAF),
            size => panic!("byte pin missing for {size}-byte pages"),
        };
        assert_eq!((blob.len(), crc32c(&blob)), expected);
    }

    #[test]
    fn segments_reject_a_different_system_page_size() {
        let (blob, _) = sample_segment();
        let (_, header_len) = open_header(&blob).expect("BLX header");
        let mut payload = open_frame(MAGIC_HEADER, &blob[..header_len])
            .expect("segment header")
            .to_vec();
        let incompatible = if page_size() == 4096 {
            16_384u32
        } else {
            4096u32
        };
        payload[2..6].copy_from_slice(&incompatible.to_le_bytes());
        let mut incompatible = seal_frame(MAGIC_HEADER, &payload);
        incompatible.extend_from_slice(&blob[header_len..]);
        assert!(scan_segment(&incompatible).is_err());
    }

    #[test]
    fn torn_segments_and_damaged_entries_are_rejected() {
        let vset = VsetId(0xA1);
        let (blob, locs) = sample_segment();

        for keep in 0..blob.len() {
            assert!(
                scan_segment(&blob[..keep]).is_err(),
                "torn segment (kept {keep}) went undetected"
            );
        }

        let loc = locs[1].2;
        let start = usize::try_from(loc.offset).unwrap();
        let end = start + usize::try_from(loc.len).unwrap();
        let mut entry = blob[start..end].to_vec();
        entry[20] ^= 0x40;
        assert!(open_entry(vset, &entry).is_err());
    }

    #[test]
    fn segment_scans_reject_any_single_bit_flip() {
        let (blob, _) = sample_segment();
        for byte in 0..blob.len() {
            for bit in 0..8 {
                let mut damaged = blob.clone();
                damaged[byte] ^= 1 << bit;
                assert!(
                    scan_segment(&damaged).is_err(),
                    "bit {bit} of byte {byte} escaped the segment scan"
                );
            }
        }
    }
}
