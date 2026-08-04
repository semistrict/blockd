//! Map leaves: the sharded half of the page→location map (R3.3/R3.4).
//!
//! A record no longer carries the whole map. The map's key space is cut
//! into fixed SPANS of [`LEAF_SPAN`] pages; each span's entries live in a
//! write-once *leaf blob*, and the record carries a bounded inline overlay
//! plus one [`LeafPtr`] per span. Lookup order is overlay first, then the
//! span's leaf; absent from both means never written (zero fill). Record
//! size is O(overlay + spans) — never O(pages) — which keeps per-capture
//! metadata cost proportional to the delta and keeps the manifest far
//! under the R4.6 object cap at any vset size.
//!
//! Leaf entries carry no vset identity: the frame binds the WRITER (the
//! vset itself, or a base id for base-namespace leaves), and the reader
//! re-keys entries under its own vset. That is what makes forks O(1)
//! metadata (R5.1): a fork's record points at its base's leaf blobs
//! unchanged.

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::segment::PageLoc;
use crate::types::{Gen, PageId, PageNo, SegId, VolumeIdx, VsetId};

pub const MAGIC_LEAF: u32 = u32::from_le_bytes(*b"BML1");

/// Pages per leaf span. Volumes never share a span: the linearized key
/// places each volume at a 2^32 boundary, a multiple of the span size.
pub const LEAF_SPAN: u64 = 4096;

/// The linearized map key of a page within its vset.
pub fn page_key(idx: VolumeIdx, page: PageNo) -> u64 {
    (u64::from(idx.0) << 32) | u64::from(page.0)
}

/// The span a page belongs to.
pub fn span_of(page: PageId) -> u32 {
    u32::try_from(page_key(page.volume.idx, page.page) / LEAF_SPAN).expect("span fits u32")
}

/// Whether a span holds memory-volume pages (volume index 0 is memory;
/// spans never straddle volumes).
pub fn span_is_memory(span: u32) -> bool {
    u64::from(span) * LEAF_SPAN < 1 << 32
}

/// A record's pointer to one leaf blob. `base` is 0 for the vset's own
/// namespace, otherwise the base id whose namespace holds the leaf
/// (R5.3: forks reference, never copy).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct LeafPtr {
    pub base: u64,
    pub fence: u64,
    pub id: u64,
}

/// One span's entries. Keys are (volume, page) — the reader supplies the
/// vset identity.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MapLeaf {
    pub span: u32,
    /// Strictly ascending by (volume, page), all within the span.
    pub entries: Vec<(VolumeIdx, PageNo, Gen, PageLoc)>,
}

impl MapLeaf {
    /// Encode under the writer's identity: the vset itself, or the base id
    /// for base-namespace leaves.
    pub fn encode(&self, owner: VsetId, fence: u64, id: u64) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(1);
        e.u64(owner.0);
        e.u64(fence);
        e.u64(id);
        e.u32(self.span);
        e.u32(u32::try_from(self.entries.len()).expect("entry count fits u32"));
        for &(idx, page, generation, loc) in &self.entries {
            e.u8(idx.0);
            e.u32(page.0);
            e.u64(generation.0);
            e.u64(loc.base);
            e.u64(loc.fence);
            e.u64(loc.seg.0);
            e.u32(loc.offset);
            e.u32(loc.len);
        }
        seal_frame(MAGIC_LEAF, &e.finish())
    }

    /// Verify and decode: the frame's owner, fence and id must match what
    /// the reader expects (a leaf whose name and payload disagree is
    /// damage), entries must ascend strictly within the span.
    pub fn decode(
        owner: VsetId,
        fence: u64,
        id: u64,
        bytes: &[u8],
    ) -> Result<MapLeaf, DecodeError> {
        let payload = open_frame(MAGIC_LEAF, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != 1 || d.u64()? != owner.0 || d.u64()? != fence || d.u64()? != id {
            return Err(DecodeError);
        }
        let span = d.u32()?;
        let count = d.u32()? as usize;
        let lo = u64::from(span) * LEAF_SPAN;
        let hi = lo + LEAF_SPAN;
        let mut entries = Vec::with_capacity(count);
        let mut last_key = None;
        for _ in 0..count {
            let idx = VolumeIdx(d.u8()?);
            let page = PageNo(d.u32()?);
            let generation = Gen(d.u64()?);
            let loc = PageLoc {
                base: d.u64()?,
                fence: d.u64()?,
                seg: SegId(d.u64()?),
                offset: d.u32()?,
                len: d.u32()?,
            };
            let key = page_key(idx, page);
            if key < lo || key >= hi || last_key.is_some_and(|k| key <= k) {
                return Err(DecodeError);
            }
            last_key = Some(key);
            entries.push((idx, page, generation, loc));
        }
        d.finish()?;
        Ok(MapLeaf { span, entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;
    use crate::types::VolumeId;

    fn sample_leaf() -> MapLeaf {
        MapLeaf {
            span: 3,
            entries: vec![
                (
                    VolumeIdx(0),
                    PageNo(3 * 4096 + 1),
                    Gen(9),
                    PageLoc {
                        base: 0,
                        fence: 2,
                        seg: SegId(5),
                        offset: 0,
                        len: 64,
                    },
                ),
                (
                    VolumeIdx(0),
                    PageNo(3 * 4096 + 7),
                    Gen(11),
                    PageLoc {
                        base: 4,
                        fence: 1,
                        seg: SegId(0),
                        offset: 128,
                        len: 64,
                    },
                ),
            ],
        }
    }

    #[test]
    fn leaves_round_trip_and_are_byte_pinned() {
        let leaf = sample_leaf();
        let bytes = leaf.encode(VsetId(0xA1), 2, 6);
        assert_eq!(MapLeaf::decode(VsetId(0xA1), 2, 6, &bytes), Ok(leaf));
        // Byte pin (R10.2): any change here is a storage format change.
        assert_eq!(bytes.len(), 136);
        assert_eq!(crc32c(&bytes), 0x65F7_89C1);
    }

    #[test]
    fn leaves_reject_any_single_bit_flip() {
        let bytes = sample_leaf().encode(VsetId(0xA1), 2, 6);
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                MapLeaf::decode(VsetId(0xA1), 2, 6, &damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn leaves_are_bound_to_owner_fence_and_id() {
        let bytes = sample_leaf().encode(VsetId(0xA1), 2, 6);
        assert!(MapLeaf::decode(VsetId(0xA2), 2, 6, &bytes).is_err());
        assert!(MapLeaf::decode(VsetId(0xA1), 3, 6, &bytes).is_err());
        assert!(MapLeaf::decode(VsetId(0xA1), 2, 7, &bytes).is_err());
    }

    #[test]
    fn leaves_reject_out_of_span_and_unsorted_entries() {
        let mut leaf = sample_leaf();
        leaf.entries[1].1 = PageNo(2 * 4096); // outside span 3
        let bytes = leaf.encode(VsetId(0xA1), 2, 6);
        assert!(MapLeaf::decode(VsetId(0xA1), 2, 6, &bytes).is_err());

        let mut leaf = sample_leaf();
        leaf.entries.swap(0, 1); // descending keys
        let bytes = leaf.encode(VsetId(0xA1), 2, 6);
        assert!(MapLeaf::decode(VsetId(0xA1), 2, 6, &bytes).is_err());
    }

    #[test]
    fn spans_linearize_volumes_apart() {
        let page = |idx: u8, n: u32| PageId {
            volume: VolumeId {
                vset: VsetId(1),
                idx: VolumeIdx(idx),
            },
            page: PageNo(n),
        };
        assert_eq!(span_of(page(0, 0)), 0);
        assert_eq!(span_of(page(0, 4095)), 0);
        assert_eq!(span_of(page(0, 4096)), 1);
        // Volume 1 starts at key 2^32: its spans never touch volume 0's.
        assert_eq!(span_of(page(1, 0)), 1 << 20);
        assert!(span_is_memory(span_of(page(0, u32::MAX))));
        assert!(!span_is_memory(span_of(page(1, 0))));
    }
}
