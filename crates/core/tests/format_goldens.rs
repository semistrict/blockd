//! Exact-byte tests for the single current storage format.

use std::collections::BTreeMap;

use blockd_core::head::{HeadRecord, ManifestPtr};
use blockd_core::journal::{JournalRecord, MigrationSource, RecordKind, VolumeConfig, VolumeKind};
use blockd_core::page_file::PageFileLoc;
use blockd_core::types::{
    Epoch, Gen, HostId, JournalSeq, ObjectId, PageId, PageNo, VolumeId, page_size,
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

fn golden_journal_record() -> (VolumeId, JournalRecord) {
    let volume = VolumeId(0xA1);
    let page = |_kind: u8, n: u32| PageId {
        volume,
        page: PageNo(n),
    };
    let record = JournalRecord {
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
        runtime_page_index: BTreeMap::from([(
            page(1, 7),
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
        )]),
        migrated_from: Some(MigrationSource {
            host: HostId(2),
            offer_fence: Some(5),
        }),
    };
    (volume, record)
}

fn golden_head() -> (VolumeId, HeadRecord) {
    let volume = VolumeId(0xB2);
    let head = HeadRecord {
        volume,
        holder: HostId(3),
        fence: 17,
        manifest: Some(ManifestPtr {
            fence: 16,
            journal_seq: JournalSeq(33),
            seq: JournalSeq(40),
            capture_seq: 512,
            checksum: 0x0102_0304_0506_0708,
        }),
        stash: None,
        retired_stashes: Vec::new(),
    };
    (volume, head)
}

// Journal v4 embeds the writer's page size, so each size this project
// runs on freezes its own golden bytes.
const JOURNAL_V4_4K: &str = "424a523199000000fe206d19040000100000a10000000000000005000000000000000600000000000000010300000000000000290000000000000063000000000000005a00000000000000010200021000000001010000000107000000090000000000000000000000000000000600000000000000020000000000000075000000580000000100000000010000000000000000000004000000000000000b00000000000000";
const JOURNAL_V4_16K: &str = "424a5231990000003f5a3b5d040000400000a10000000000000005000000000000000600000000000000010300000000000000290000000000000063000000000000005a00000000000000010200021000000001010000000107000000090000000000000000000000000000000600000000000000020000000000000075000000580000000100000000010000000000000000000004000000000000000b00000000000000";
const JOURNAL_V5_4K: &str = "424a523199000000217b6b6f050000100000a10000000000000005000000000000000600000000000000010300000000000000290000000000000063000000000000005a00000000000000010200021000000001010000000107000000090000000000000000000000000000000600000000000000020000000000000075000000580000000100000000010000000000000000000004000000000000000b00000000000000";
const JOURNAL_V5_16K: &str = "424a523199000000e0013d2b050000400000a10000000000000005000000000000000600000000000000010300000000000000290000000000000063000000000000005a00000000000000010200021000000001010000000107000000090000000000000000000000000000000600000000000000020000000000000075000000580000000100000000010000000000000000000004000000000000000b00000000000000";
const JOURNAL_V6_4K: &str = "424a52319a0000000fc0a899060000100000a10000000000000005000000000000000600000000000000010300000000000000290000000000000063000000000000005a0000000000000001020000021000000001010000000107000000090000000000000000000000000000000600000000000000020000000000000075000000580000000100000000010000000000000000000004000000000000000b00000000000000";
const JOURNAL_V6_16K: &str = "424a52319a0000009ed98b07060000400000a10000000000000005000000000000000600000000000000010300000000000000290000000000000063000000000000005a0000000000000001020000021000000002010000000107000000090000000000000000000000000000000600000000000000020000000000000075000000580000000100000000010000000000000000000004000000000000000b00000000000000";
const JOURNAL_V7_4K: &str = "424a5231660000000bcce059070000100000a100000000000000050000000000000006000000000000000103000000000000002900000000000000080000000000000063000000000000005a0000000000000017000000000000000102000105000000000000000002100000000200000000";
const JOURNAL_V7_16K: &str = "424a5231660000007203a42f070000400000a100000000000000050000000000000006000000000000000103000000000000002900000000000000080000000000000063000000000000005a0000000000000017000000000000000102000105000000000000000002100000000200000000";
const JOURNAL_V8_4K: &str = "424a523165000000b7f55331080000100000a100000000000000050000000000000006000000000000000103000000000000002900000000000000080000000000000063000000000000005a00000000000000170000000000000001020001050000000000000000100000000200000000";
const JOURNAL_V8_16K: &str = "424a52316500000010c14df4080000400000a100000000000000050000000000000006000000000000000103000000000000002900000000000000080000000000000063000000000000005a00000000000000170000000000000001020001050000000000000000100000000200000000";

fn journal_v6_golden() -> &'static str {
    match page_size() {
        4096 => JOURNAL_V6_4K,
        16_384 => JOURNAL_V6_16K,
        size => panic!("journal golden missing for {size}-byte pages"),
    }
}

fn journal_v7_golden() -> &'static str {
    match page_size() {
        4096 => JOURNAL_V7_4K,
        16_384 => JOURNAL_V7_16K,
        size => panic!("journal golden missing for {size}-byte pages"),
    }
}

fn journal_v8_golden() -> &'static str {
    match page_size() {
        4096 => JOURNAL_V8_4K,
        16_384 => JOURNAL_V8_16K,
        size => panic!("journal golden missing for {size}-byte pages"),
    }
}
const HEAD_V1: &str = "424844312d0000005e45983e0100b2000000000000000300110000000000000001100000000000000028000000000000000002000000000000";
const HEAD_V4: &str = "424844313f000000953c48e50400b2000000000000000300110000000000000001100000000000000021000000000000002800000000000000000200000000000008070605040302010000";

#[test]
fn single_volume_v8_is_pinned_and_old_records_are_rejected() {
    let (volume, record) = golden_journal_record();
    assert_eq!(
        hex(&record.encode(volume)),
        journal_v8_golden(),
        "v8 encoder drifted"
    );
    let legacy_v4 = match page_size() {
        4096 => JOURNAL_V4_4K,
        16_384 => JOURNAL_V4_16K,
        size => panic!("journal golden missing for {size}-byte pages"),
    };
    let legacy_v5 = match page_size() {
        4096 => JOURNAL_V5_4K,
        16_384 => JOURNAL_V5_16K,
        size => panic!("journal golden missing for {size}-byte pages"),
    };
    assert!(JournalRecord::decode(volume, &unhex(legacy_v4)).is_err());
    assert!(JournalRecord::decode(volume, &unhex(legacy_v5)).is_err());
    assert!(JournalRecord::decode(volume, &unhex(journal_v6_golden())).is_err());
    assert!(JournalRecord::decode(volume, &unhex(journal_v7_golden())).is_err());
    let mut durable = record;
    durable.runtime_page_index.clear();
    assert_eq!(
        JournalRecord::decode(volume, &durable.encode(volume)),
        Ok(durable)
    );
}

#[test]
fn current_head_bytes_are_pinned_and_old_bytes_are_rejected() {
    let (volume, head) = golden_head();
    assert_eq!(hex(&head.encode()), HEAD_V4, "v4 encoder drifted");
    assert!(
        HeadRecord::decode(volume, &unhex(HEAD_V1)).is_err(),
        "old head bytes must not be accepted"
    );
}
