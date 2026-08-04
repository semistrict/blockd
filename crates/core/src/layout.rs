//! The exact naming of every durable artifact — local blob names and object
//! store keys. This *is* the production layout; the simulation and a real
//! deployment use these byte-for-byte.
//!
//! Every segment and manifest is namespaced by the writer's **fence** — the
//! head record's CAS version at claim time (R6.3). A fenced former holder
//! keeps its own namespace, and since only the head record (updated by CAS)
//! makes state reachable, nothing a fenced holder writes can ever fork
//! durable state (R6.4): its keys simply dangle.
//!
//! Local blobs (relative to the daemon's data root):
//! - `v/<vset:016x>/j/<fence:016x>-<seq:016x>.rec` — journal record
//!   (framed, R10.2), plus a byte-identical `.recm` mirror: the newest
//!   record is the sole carrier of its newly-acked sync watermark, and a
//!   bit rotting it after the ack would silently roll acked syncs back
//!   (R3.8) — recovery accepts whichever copy decodes intact
//! - `v/<vset:016x>/s/<fence:016x>-<seg:016x>.seg` — segment of compressed
//!   page entries
//!
//! Object store keys (relative to the cluster's bucket + prefix, R9.1):
//! - `v/<vset:016x>/head`  — head record: CAS assignment authority (R6.3)
//!   and pointer to the newest backed-up manifest
//! - `v/<vset:016x>/m/<fence:016x>-<seq:016x>` — manifest: the journal
//!   record's bytes, verbatim
//! - `v/<vset:016x>/s/<fence:016x>-<seg:016x>` — segment: the local blob's
//!   bytes, verbatim (R8.4: transfers move stored bytes unchanged)
//! - `b/<base:016x>/…` — bases (lineage milestone)

use crate::types::{JournalSeq, SegId, VsetId};

pub fn journal_blob(vset: VsetId, fence: u64, seq: JournalSeq) -> String {
    format!("v/{:016x}/j/{fence:016x}-{:016x}.rec", vset.0, seq.0)
}

/// The record's byte-identical mirror (rot redundancy, R3.8/R8.1).
pub fn journal_mirror_blob(vset: VsetId, fence: u64, seq: JournalSeq) -> String {
    format!("v/{:016x}/j/{fence:016x}-{:016x}.recm", vset.0, seq.0)
}

pub fn segment_blob(vset: VsetId, fence: u64, seg: SegId) -> String {
    format!("v/{:016x}/s/{fence:016x}-{:016x}.seg", vset.0, seg.0)
}

/// A map leaf in the vset's own namespace (local blob).
pub fn leaf_blob(vset: VsetId, fence: u64, id: u64) -> String {
    format!("v/{:016x}/l/{fence:016x}-{id:016x}.map", vset.0)
}

/// A local copy of a base-namespace map leaf, held under the vset that
/// references it (base ids and fences share a numeric space, so the name
/// carries the base discriminator).
pub fn base_leaf_blob(vset: VsetId, base: u64, fence: u64, id: u64) -> String {
    format!(
        "v/{:016x}/lb/{base:016x}-{fence:016x}-{id:016x}.map",
        vset.0
    )
}

pub fn head_key(vset: VsetId) -> String {
    format!("v/{:016x}/head", vset.0)
}

/// The vset's recorded resume set (R6.2): what the last resume touched
/// first, so the next restore can prefetch it. Overwritten in place; best
/// effort — a missing or stale set only costs demand faults.
pub fn resume_set_key(vset: VsetId) -> String {
    format!("v/{:016x}/rs", vset.0)
}

pub fn manifest_key(vset: VsetId, fence: u64, seq: JournalSeq) -> String {
    format!("v/{:016x}/m/{fence:016x}-{:016x}", vset.0, seq.0)
}

pub fn segment_key(vset: VsetId, fence: u64, seg: SegId) -> String {
    format!("v/{:016x}/s/{fence:016x}-{:016x}", vset.0, seg.0)
}

/// A map leaf in the store, the local blob's bytes verbatim (R8.4).
pub fn leaf_key(vset: VsetId, fence: u64, id: u64) -> String {
    format!("v/{:016x}/l/{fence:016x}-{id:016x}", vset.0)
}

/// A base's map leaves: copied store-side at keep time, referenced (never
/// copied) by every fork's records (R5.1/R5.3).
pub fn base_leaf_key(base: u64, fence: u64, id: u64) -> String {
    format!("b/{base:016x}/l/{fence:016x}-{id:016x}")
}

/// Prefix under which every object of one vset lives (R4.4 audits, GC).
pub fn vset_prefix(vset: VsetId) -> String {
    format!("v/{:016x}/", vset.0)
}

/// A base's record: a manifest kept alive until explicit delete (R5.2).
pub fn base_record_key(base: u64) -> String {
    format!("b/{base:016x}/rec")
}

/// A base's segments: copied store-side at keep time, shared by every fork
/// (R5.3: the base is stored once, forever, regardless of fork count).
pub fn base_segment_key(base: u64, fence: u64, seg: SegId) -> String {
    format!("b/{base:016x}/s/{fence:016x}-{:016x}", seg.0)
}

/// The local outbound-handoff marker (R7.2): its durable presence means
/// this host gave the vset away and may only serve peer fetches for it.
pub fn handoff_blob(vset: VsetId) -> String {
    format!("v/{:016x}/handoff", vset.0)
}

/// Parse an object-store key back into its meaning (GC's mark phase).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreKey {
    Head {
        vset: VsetId,
    },
    Manifest {
        vset: VsetId,
        fence: u64,
        seq: JournalSeq,
    },
    Segment {
        vset: VsetId,
        fence: u64,
        seg: SegId,
    },
    Leaf {
        vset: VsetId,
        fence: u64,
        id: u64,
    },
    BaseRecord {
        base: u64,
    },
    BaseSegment {
        base: u64,
        fence: u64,
        seg: SegId,
    },
    BaseLeaf {
        base: u64,
        fence: u64,
        id: u64,
    },
}

pub fn parse_key(key: &str) -> Option<StoreKey> {
    if let Some(rest) = key.strip_prefix("v/") {
        let (vset_hex, rest) = rest.split_once('/')?;
        let vset = VsetId(u64::from_str_radix(vset_hex, 16).ok()?);
        if rest == "head" {
            return Some(StoreKey::Head { vset });
        }
        if let Some(body) = rest.strip_prefix("m/") {
            let (fence_hex, seq_hex) = body.split_once('-')?;
            return Some(StoreKey::Manifest {
                vset,
                fence: u64::from_str_radix(fence_hex, 16).ok()?,
                seq: JournalSeq(u64::from_str_radix(seq_hex, 16).ok()?),
            });
        }
        if let Some(body) = rest.strip_prefix("s/") {
            let (fence_hex, seg_hex) = body.split_once('-')?;
            return Some(StoreKey::Segment {
                vset,
                fence: u64::from_str_radix(fence_hex, 16).ok()?,
                seg: SegId(u64::from_str_radix(seg_hex, 16).ok()?),
            });
        }
        if let Some(body) = rest.strip_prefix("l/") {
            let (fence_hex, id_hex) = body.split_once('-')?;
            return Some(StoreKey::Leaf {
                vset,
                fence: u64::from_str_radix(fence_hex, 16).ok()?,
                id: u64::from_str_radix(id_hex, 16).ok()?,
            });
        }
        return None;
    }
    if let Some(rest) = key.strip_prefix("b/") {
        let (base_hex, rest) = rest.split_once('/')?;
        let base = u64::from_str_radix(base_hex, 16).ok()?;
        if rest == "rec" {
            return Some(StoreKey::BaseRecord { base });
        }
        if let Some(body) = rest.strip_prefix("s/") {
            let (fence_hex, seg_hex) = body.split_once('-')?;
            return Some(StoreKey::BaseSegment {
                base,
                fence: u64::from_str_radix(fence_hex, 16).ok()?,
                seg: SegId(u64::from_str_radix(seg_hex, 16).ok()?),
            });
        }
        if let Some(body) = rest.strip_prefix("l/") {
            let (fence_hex, id_hex) = body.split_once('-')?;
            return Some(StoreKey::BaseLeaf {
                base,
                fence: u64::from_str_radix(fence_hex, 16).ok()?,
                id: u64::from_str_radix(id_hex, 16).ok()?,
            });
        }
        return None;
    }
    None
}

/// Parse a local blob name back into its meaning (recovery scan).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BlobName {
    Journal {
        vset: VsetId,
        fence: u64,
        seq: JournalSeq,
    },
    Segment {
        vset: VsetId,
        fence: u64,
        seg: SegId,
    },
    Leaf {
        vset: VsetId,
        fence: u64,
        id: u64,
    },
    BaseLeaf {
        vset: VsetId,
        base: u64,
        fence: u64,
        id: u64,
    },
    Handoff {
        vset: VsetId,
    },
}

pub fn parse_blob(name: &str) -> Option<BlobName> {
    let rest = name.strip_prefix("v/")?;
    let (vset_hex, rest) = rest.split_once('/')?;
    let vset = VsetId(u64::from_str_radix(vset_hex, 16).ok()?);
    // A mirror parses as the same journal record: recovery accepts
    // whichever copy decodes intact.
    if let Some(body) = rest
        .strip_prefix("j/")
        .and_then(|r| r.strip_suffix(".recm").or_else(|| r.strip_suffix(".rec")))
    {
        let (fence_hex, seq_hex) = body.split_once('-')?;
        let fence = u64::from_str_radix(fence_hex, 16).ok()?;
        let seq = JournalSeq(u64::from_str_radix(seq_hex, 16).ok()?);
        return Some(BlobName::Journal { vset, fence, seq });
    }
    if let Some(body) = rest.strip_prefix("s/").and_then(|r| r.strip_suffix(".seg")) {
        let (fence_hex, seg_hex) = body.split_once('-')?;
        let fence = u64::from_str_radix(fence_hex, 16).ok()?;
        let seg = SegId(u64::from_str_radix(seg_hex, 16).ok()?);
        return Some(BlobName::Segment { vset, fence, seg });
    }
    if let Some(body) = rest.strip_prefix("l/").and_then(|r| r.strip_suffix(".map")) {
        let (fence_hex, id_hex) = body.split_once('-')?;
        let fence = u64::from_str_radix(fence_hex, 16).ok()?;
        let id = u64::from_str_radix(id_hex, 16).ok()?;
        return Some(BlobName::Leaf { vset, fence, id });
    }
    if let Some(body) = rest
        .strip_prefix("lb/")
        .and_then(|r| r.strip_suffix(".map"))
    {
        let mut parts = body.splitn(3, '-');
        let base = u64::from_str_radix(parts.next()?, 16).ok()?;
        let fence = u64::from_str_radix(parts.next()?, 16).ok()?;
        let id = u64::from_str_radix(parts.next()?, 16).ok()?;
        return Some(BlobName::BaseLeaf {
            vset,
            base,
            fence,
            id,
        });
    }
    if rest == "handoff" {
        return Some(BlobName::Handoff { vset });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_pinned_and_parse_back() {
        let vset = VsetId(0x0BAD_CAFE);
        assert_eq!(
            journal_blob(vset, 2, JournalSeq(0x1F)),
            "v/000000000badcafe/j/0000000000000002-000000000000001f.rec"
        );
        assert_eq!(
            segment_blob(vset, 2, SegId(3)),
            "v/000000000badcafe/s/0000000000000002-0000000000000003.seg"
        );
        assert_eq!(head_key(vset), "v/000000000badcafe/head");
        assert_eq!(
            manifest_key(vset, 2, JournalSeq(5)),
            "v/000000000badcafe/m/0000000000000002-0000000000000005"
        );
        assert_eq!(
            segment_key(vset, 2, SegId(3)),
            "v/000000000badcafe/s/0000000000000002-0000000000000003"
        );
        assert_eq!(vset_prefix(vset), "v/000000000badcafe/");
        assert_eq!(
            leaf_blob(vset, 2, 7),
            "v/000000000badcafe/l/0000000000000002-0000000000000007.map"
        );
        assert_eq!(
            base_leaf_blob(vset, 9, 2, 7),
            "v/000000000badcafe/lb/0000000000000009-0000000000000002-0000000000000007.map"
        );
        assert_eq!(
            leaf_key(vset, 2, 7),
            "v/000000000badcafe/l/0000000000000002-0000000000000007"
        );
        assert_eq!(
            base_leaf_key(9, 2, 7),
            "b/0000000000000009/l/0000000000000002-0000000000000007"
        );
        assert_eq!(
            parse_blob("v/000000000badcafe/l/0000000000000002-0000000000000007.map"),
            Some(BlobName::Leaf {
                vset,
                fence: 2,
                id: 7
            })
        );
        assert_eq!(
            parse_blob(
                "v/000000000badcafe/lb/0000000000000009-0000000000000002-0000000000000007.map"
            ),
            Some(BlobName::BaseLeaf {
                vset,
                base: 9,
                fence: 2,
                id: 7
            })
        );
        assert_eq!(
            parse_key("v/000000000badcafe/l/0000000000000002-0000000000000007"),
            Some(StoreKey::Leaf {
                vset,
                fence: 2,
                id: 7
            })
        );
        assert_eq!(
            parse_key("b/0000000000000009/l/0000000000000002-0000000000000007"),
            Some(StoreKey::BaseLeaf {
                base: 9,
                fence: 2,
                id: 7
            })
        );
        assert_eq!(
            parse_blob("v/000000000badcafe/j/0000000000000002-000000000000001f.rec"),
            Some(BlobName::Journal {
                vset,
                fence: 2,
                seq: JournalSeq(0x1F)
            })
        );
        assert_eq!(
            parse_blob("v/000000000badcafe/s/0000000000000002-0000000000000003.seg"),
            Some(BlobName::Segment {
                vset,
                fence: 2,
                seg: SegId(3)
            })
        );
        assert_eq!(parse_blob("garbage"), None);
        assert_eq!(parse_blob("v/000000000badcafe/s/junk.seg"), None);
        assert_eq!(parse_blob("v/000000000badcafe/j/junk.rec"), None);
    }
}
