//! Segments: the unit of page storage and transfer. A segment is one
//! write-once blob holding lz4-compressed page entries, each individually
//! framed and checksummed. Primary-to-passive transfer moves source segments
//! verbatim; the passive may decode live entries into archive-only packs in
//! the same format (R8.4). The fault path pays exactly one decompression of
//! one entry.
//!
//! Layout: `header frame | entry frame | entry frame | …`. Every frame is
//! `format::seal_frame`; a torn tail fails a frame check and is detected as
//! corruption (R8.1) — there is nothing to trust beyond the checksums.

use crate::format::{Dec, DecodeError, Enc, FRAME_HEADER, open_frame, seal_frame};
use crate::types::{Gen, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId, page_size};

pub const MAGIC_SEG_HDR: u32 = u32::from_le_bytes(*b"BSH1");
pub const MAGIC_SEG_ENT: u32 = u32::from_le_bytes(*b"BSE1");

/// Hard ceiling shared by local capture, peer transfer, and object-store
/// publication. It leaves ample envelope room below R4.6's 64 MiB cap.
pub const MAX_SEGMENT_BYTES: usize = 32 * 1024 * 1024;

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

const HDR_PAYLOAD: usize = 2 + 4 + 8 + 8 + 8 + 4; // version, page size, vset, fence, seg, count
const ENTRY_PAYLOAD_FIXED: usize = 1 + 4 + 8 + 4;

fn max_entry_bytes() -> usize {
    FRAME_HEADER + ENTRY_PAYLOAD_FIXED + lz4_flex::block::get_maximum_output_size(page_size())
}

/// Every page a segment holds: identity, generation, and byte range.
pub type SegmentEntries = Vec<(PageId, Gen, PageLoc)>;

/// Builds a segment blob, returning the exact byte range of every entry.
#[derive(Debug)]
pub struct SegmentBuilder {
    vset: VsetId,
    fence: u64,
    seg: SegId,
    entries: Vec<(PageId, Gen, PageLoc)>,
    body: Vec<u8>,
}

impl SegmentBuilder {
    pub fn new(vset: VsetId, fence: u64, seg: SegId) -> SegmentBuilder {
        SegmentBuilder {
            vset,
            fence,
            seg,
            entries: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Append one page. `page_bytes` must be exactly one page.
    pub fn add(&mut self, page: PageId, generation: Gen, page_bytes: &[u8]) {
        assert_eq!(page_bytes.len(), page_size(), "segments store whole pages");
        assert_eq!(page.volume.vset, self.vset, "segment is per-vset");
        let stored = lz4_flex::block::compress(page_bytes);
        let mut e = Enc::new();
        e.u8(page.volume.idx.0);
        e.u32(page.page.0);
        e.u64(generation.0);
        e.u32(u32::try_from(stored.len()).expect("compressed page fits u32"));
        e.bytes(&stored);
        let entry = seal_frame(MAGIC_SEG_ENT, &e.finish());
        let offset = FRAME_HEADER + HDR_PAYLOAD + self.body.len();
        let loc = PageLoc {
            base: 0,
            fence: self.fence,
            seg: self.seg,
            offset: u32::try_from(offset).expect("segment fits u32"),
            len: u32::try_from(entry.len()).expect("entry fits u32"),
        };
        self.entries.push((page, generation, loc));
        self.body.extend_from_slice(&entry);
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn can_add_page(&self) -> bool {
        FRAME_HEADER + HDR_PAYLOAD + self.body.len() + max_entry_bytes() <= MAX_SEGMENT_BYTES
    }

    /// Finish the blob: bytes plus every entry's location.
    pub fn finish(self) -> (Vec<u8>, SegmentEntries) {
        let mut h = Enc::new();
        h.u16(2); // version
        h.u32(u32::try_from(page_size()).expect("page size fits u32"));
        h.u64(self.vset.0);
        h.u64(self.fence);
        h.u64(self.seg.0);
        h.u32(u32::try_from(self.entries.len()).expect("entry count fits u32"));
        let mut blob = seal_frame(MAGIC_SEG_HDR, &h.finish());
        debug_assert_eq!(blob.len(), FRAME_HEADER + HDR_PAYLOAD);
        blob.extend_from_slice(&self.body);
        (blob, self.entries)
    }
}

/// Builds one capture's page entries into consecutive, independently bounded
/// segment blobs. Rotation happens before an entry, so an entry is never split.
#[derive(Debug)]
pub struct SegmentBatchBuilder {
    vset: VsetId,
    fence: u64,
    current: SegmentBuilder,
    finished: Vec<(SegId, Vec<u8>, SegmentEntries)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentIdOverflow;

impl SegmentBatchBuilder {
    pub fn new(vset: VsetId, fence: u64, first_seg: SegId) -> Self {
        Self {
            vset,
            fence,
            current: SegmentBuilder::new(vset, fence, first_seg),
            finished: Vec::new(),
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
        if !self.current.is_empty() && !self.current.can_add_page() {
            self.try_rotate()?;
        }
        self.current.add(page, generation, page_bytes);
        Ok(())
    }

    fn try_rotate(&mut self) -> Result<(), SegmentIdOverflow> {
        let next = SegId(self.current.seg.0.checked_add(1).ok_or(SegmentIdOverflow)?);
        let current = std::mem::replace(
            &mut self.current,
            SegmentBuilder::new(self.vset, self.fence, next),
        );
        let seg = current.seg;
        let (blob, entries) = current.finish();
        debug_assert!(blob.len() <= MAX_SEGMENT_BYTES);
        self.finished.push((seg, blob, entries));
        Ok(())
    }

    pub fn finish(mut self) -> Vec<(SegId, Vec<u8>, SegmentEntries)> {
        if !self.current.is_empty() {
            let seg = self.current.seg;
            let (blob, entries) = self.current.finish();
            debug_assert!(blob.len() <= MAX_SEGMENT_BYTES);
            self.finished.push((seg, blob, entries));
        }
        self.finished
    }
}

/// Decode and verify one entry read by ranged read (the fault path). Returns
/// the page identity, generation and the decompressed page bytes.
pub fn open_entry(vset: VsetId, bytes: &[u8]) -> Result<(PageId, Gen, Vec<u8>), DecodeError> {
    let (page, generation, stored) = parse_entry(vset, bytes)?;
    let raw = lz4_flex::block::decompress(stored, page_size()).map_err(|_| DecodeError)?;
    if raw.len() != page_size() {
        return Err(DecodeError);
    }
    Ok((page, generation, raw))
}

/// Verify an entry's frame and metadata without expanding its payload.
/// Whole-segment scans use this path: the frame CRC validates the stored
/// compressed bytes, while page decompression stays on the selected-entry
/// path instead of multiplying capture, compaction, and publish CPU cost.
fn parse_entry(vset: VsetId, bytes: &[u8]) -> Result<(PageId, Gen, &[u8]), DecodeError> {
    let payload = open_frame(MAGIC_SEG_ENT, bytes)?;
    let mut d = Dec::new(payload);
    let volume = VolumeIdx(d.u8()?);
    let page_no = PageNo(d.u32()?);
    let generation = Gen(d.u64()?);
    let stored_len = usize::try_from(d.u32()?).expect("u32 fits usize");
    let stored = d.bytes(stored_len)?;
    d.finish()?;
    let page = PageId {
        volume: VolumeId { vset, idx: volume },
        page: page_no,
    };
    Ok((page, generation, stored))
}

/// Scan a whole segment blob (recovery, hydration, backup verification),
/// checksumming the header and every stored entry and returning identities
/// and locations. Individual entries are decompressed only when consumed.
pub fn scan_segment(bytes: &[u8]) -> Result<(VsetId, u64, SegId, SegmentEntries), DecodeError> {
    let hdr_end = FRAME_HEADER + HDR_PAYLOAD;
    if bytes.len() < hdr_end {
        return Err(DecodeError);
    }
    let payload = open_frame(MAGIC_SEG_HDR, &bytes[..hdr_end])?;
    let mut d = Dec::new(payload);
    let version = d.u16()?;
    if version != 2 {
        return Err(DecodeError);
    }
    if d.u32()? != u32::try_from(page_size()).expect("page size fits u32") {
        return Err(DecodeError);
    }
    let vset = VsetId(d.u64()?);
    let fence = d.u64()?;
    let seg = SegId(d.u64()?);
    let count = usize::try_from(d.u32()?).expect("u32 fits usize");
    d.finish()?;

    let mut entries = Vec::with_capacity(count);
    let mut offset = hdr_end;
    while offset < bytes.len() {
        let rest = &bytes[offset..];
        if rest.len() < FRAME_HEADER {
            return Err(DecodeError);
        }
        let mut fd = Dec::new(&rest[4..8]);
        let payload_len = usize::try_from(fd.u32().expect("slice of 4")).expect("u32 fits usize");
        let entry_len = FRAME_HEADER + payload_len;
        if rest.len() < entry_len {
            return Err(DecodeError);
        }
        let (page, generation, _) = parse_entry(vset, &rest[..entry_len])?;
        entries.push((
            page,
            generation,
            PageLoc {
                base: 0,
                fence,
                seg,
                offset: u32::try_from(offset).expect("segment fits u32"),
                len: u32::try_from(entry_len).expect("entry fits u32"),
            },
        ));
        offset += entry_len;
    }
    if entries.len() != count {
        return Err(DecodeError);
    }
    Ok((vset, fence, seg, entries))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::format::{crc32c, open_frame, seal_frame};

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
        let mut b = SegmentBuilder::new(VsetId(0xA1), 6, SegId(2));
        b.add(sample_page(0, 3), Gen(7), &pattern_page(0x55));
        b.add(sample_page(1, 0), Gen(9), &pattern_page(0xAA));
        b.finish()
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
        batch
            .current
            .add(sample_page(0, 0), Gen(0), &pattern_page(0x55));
        assert_eq!(batch.try_rotate(), Err(SegmentIdOverflow));
        assert!(batch.finished.is_empty());
        assert_eq!(batch.current.seg, SegId(u64::MAX));
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
            4096 => (668, 0xDF52_B4D5),
            16_384 => (766, 0xE87B_B6DD),
            size => panic!("byte pin missing for {size}-byte pages"),
        };
        assert_eq!((blob.len(), crc32c(&blob)), expected);
    }

    #[test]
    fn segments_reject_a_different_system_page_size() {
        let (blob, _) = sample_segment();
        let header_len = FRAME_HEADER + HDR_PAYLOAD;
        let mut payload = open_frame(MAGIC_SEG_HDR, &blob[..header_len])
            .expect("segment header")
            .to_vec();
        let incompatible = if page_size() == 4096 {
            16_384u32
        } else {
            4096u32
        };
        payload[2..6].copy_from_slice(&incompatible.to_le_bytes());
        let mut incompatible = seal_frame(MAGIC_SEG_HDR, &payload);
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
