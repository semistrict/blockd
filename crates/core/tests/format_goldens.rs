//! Golden storage formats: frozen encoded bytes, decoded back and compared
//! against the values that produced them. The in-module byte pins catch an
//! ENCODER that drifts; these catch a DECODER that stops reading bytes
//! already durable on some disk. Whenever a format version bumps, these
//! tests fail — that failure is the ratchet asking a deliberate question:
//! keep a reader for the old version, migrate, or (pre-production only)
//! consciously break and re-freeze.

use std::collections::BTreeMap;

use blockd_core::head::{HeadRecord, ManifestPtr};
use blockd_core::journal::{JournalRecord, RecordKind, VsetConfig};
use blockd_core::mapleaf::{LeafPtr, MapLeaf};
use blockd_core::segment::PageLoc;
use blockd_core::types::{
    Epoch, Gen, HostId, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId,
};

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

fn golden_journal_record() -> (VsetId, JournalRecord) {
    let vset = VsetId(0xA1);
    let page = |idx: u8, n: u32| PageId {
        volume: VolumeId {
            vset,
            idx: VolumeIdx(idx),
        },
        page: PageNo(n),
    };
    let record = JournalRecord {
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
        overlay: BTreeMap::from([(
            page(1, 7),
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
        )]),
        leaves: BTreeMap::from([(
            0x100,
            LeafPtr {
                base: 0,
                fence: 4,
                id: 11,
            },
        )]),
        migrated_from: Some(HostId(2)),
    };
    (vset, record)
}

fn golden_head() -> (VsetId, HeadRecord) {
    let vset = VsetId(0xB2);
    let head = HeadRecord {
        vset,
        holder: HostId(3),
        fence: 17,
        manifest: Some(ManifestPtr {
            fence: 16,
            seq: JournalSeq(40),
            capture_seq: 512,
        }),
    };
    (vset, head)
}

fn golden_leaf() -> (VsetId, u64, u64, MapLeaf) {
    // VolumeIdx(1)'s pages 0..4095 linearize to span (1 << 32) / LEAF_SPAN.
    let leaf = MapLeaf {
        span: 0x10_0000,
        entries: vec![
            (
                VolumeIdx(1),
                PageNo(0),
                Gen(4),
                PageLoc {
                    base: 0,
                    fence: 3,
                    seg: SegId(1),
                    offset: 0,
                    len: 64,
                },
            ),
            (
                VolumeIdx(1),
                PageNo(3),
                Gen(7),
                PageLoc {
                    base: 9,
                    fence: 2,
                    seg: SegId(5),
                    offset: 128,
                    len: 90,
                },
            ),
        ],
    };
    (VsetId(0xC3), 4, 11, leaf)
}

const JOURNAL_V3: &str = "424a52319500000084bfb5220300a10000000000000005000000000000000600000000000000010300000000000000290000000000000063000000000000005a00000000000000010200021000000001010000000107000000090000000000000000000000000000000600000000000000020000000000000075000000580000000100000000010000000000000000000004000000000000000b00000000000000";
const HEAD_V1: &str = "424844312d0000005e45983e0100b2000000000000000300110000000000000001100000000000000028000000000000000002000000000000";
const LEAF_V1: &str = "424d4c317c000000936201c70100c30000000000000004000000000000000b00000000000000000010000200000001000000000400000000000000000000000000000003000000000000000100000000000000000000004000000001030000000700000000000000090000000000000002000000000000000500000000000000800000005a000000";

#[test]
fn journal_v3_golden_bytes_decode() {
    let (vset, record) = golden_journal_record();
    assert_eq!(hex(&record.encode(vset)), JOURNAL_V3, "encoder drifted");
    assert_eq!(
        JournalRecord::decode(vset, &unhex(JOURNAL_V3)),
        Ok(record),
        "decoder no longer reads the frozen v3 bytes"
    );
}

#[test]
fn head_v1_golden_bytes_decode() {
    let (vset, head) = golden_head();
    assert_eq!(hex(&head.encode()), HEAD_V1, "encoder drifted");
    assert_eq!(
        HeadRecord::decode(vset, &unhex(HEAD_V1)),
        Ok(head),
        "decoder no longer reads the frozen v1 bytes"
    );
}

#[test]
fn leaf_v1_golden_bytes_decode() {
    let (owner, fence, id, leaf) = golden_leaf();
    assert_eq!(
        hex(&leaf.encode(owner, fence, id)),
        LEAF_V1,
        "encoder drifted"
    );
    assert_eq!(
        MapLeaf::decode(owner, fence, id, &unhex(LEAF_V1)),
        Ok(leaf),
        "decoder no longer reads the frozen v1 bytes"
    );
}
