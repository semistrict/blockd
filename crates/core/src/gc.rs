//! Cluster garbage collection (R9.3): a separate process, against the
//! bucket alone. Mark from the only roots that exist — head records and
//! base records — then sweep what nothing reachable references, and only
//! once it is older than the in-flight grace (which protects publishes
//! whose manifest has not landed yet; the grace is never retention policy,
//! R4.5).
//!
//! It can never delete a base (base records are roots until an explicit
//! `DeleteBase` removes them), never a live vset's state (its head roots
//! its newest manifest and every segment that manifest references,
//! including inherited base segments), and never anything an explicit
//! delete has not unrooted. Undecodable roots are kept: GC refuses to act
//! on anything it cannot vouch for (R8.1's spirit).

use std::collections::BTreeSet;

use crate::head::HeadRecord;
use crate::journal::JournalRecord;
use crate::layout::{self, StoreKey};
use crate::mapleaf::MapLeaf;
use crate::types::{SimTime, VsetId};

/// Compute the deletion list for one GC pass over a bucket listing.
/// `objects` is the full LIST (key, last-write time, bytes); reads of
/// manifests, base records and map leaves happen from these bytes exactly
/// as the real process would GET them.
pub fn plan(now: SimTime, grace: u64, objects: &[(String, SimTime, Vec<u8>)]) -> Vec<String> {
    let mut keep: BTreeSet<&str> = BTreeSet::new();
    let find = |key: &str| objects.iter().find(|(k, _, _)| k == key);

    // Mark. Roots: every head record and every base record — plus
    // everything undecodable among them (never act on what you can't read).
    for (key, _, bytes) in objects {
        match layout::parse_key(key) {
            Some(StoreKey::Head { vset }) => {
                keep.insert(key);
                // A live head roots its resume set (R6.2) — tiny, refreshed
                // in place, and worthless the moment the head is gone.
                if let Some((k, _, _)) = find(&layout::resume_set_key(vset)) {
                    keep.insert(k);
                }
                let Ok(head) = HeadRecord::decode(vset, bytes) else {
                    continue;
                };
                let Some(ptr) = head.manifest else {
                    continue;
                };
                let manifest_key = layout::manifest_key(vset, ptr.fence, ptr.seq);
                keep.insert(
                    find(&manifest_key)
                        .map(|(k, _, _)| k.as_str())
                        .unwrap_or_default(),
                );
                if let Some((_, _, manifest_bytes)) = find(&manifest_key)
                    && let Ok(record) = JournalRecord::decode(vset, manifest_bytes)
                {
                    mark_record(objects, &mut keep, &record, Namespace::Vset(vset));
                }
            }
            Some(StoreKey::BaseRecord { base }) => {
                keep.insert(key);
                let Ok(record) = JournalRecord::decode(VsetId(base), bytes) else {
                    continue;
                };
                // A base's own artifacts live in its namespace; inherited
                // ones carry an ancestor base's id (flattened chains).
                mark_record(objects, &mut keep, &record, Namespace::Base(base));
            }
            _ => {}
        }
    }
    keep.remove("");

    // Sweep: unreferenced and older than the grace.
    objects
        .iter()
        .filter(|(key, put_at, _)| {
            !keep.contains(key.as_str()) && now.nanos().saturating_sub(put_at.nanos()) > grace
        })
        .map(|(key, _, _)| key.clone())
        .collect()
}

/// Whose namespace a record's `base == 0` references resolve into.
#[derive(Clone, Copy)]
enum Namespace {
    Vset(VsetId),
    Base(u64),
}

/// Mark one record's reachable set: its overlay's segments, its leaf
/// objects, and the segments those leaves hold.
fn mark_record<'a>(
    objects: &'a [(String, SimTime, Vec<u8>)],
    keep: &mut BTreeSet<&'a str>,
    record: &JournalRecord,
    namespace: Namespace,
) {
    let find = |key: &str| objects.iter().find(|(k, _, _)| k == key);
    let seg_key = |loc: &crate::segment::PageLoc| {
        if loc.base != 0 {
            return layout::base_segment_key(loc.base, loc.fence, loc.seg);
        }
        match namespace {
            Namespace::Vset(vset) => layout::segment_key(vset, loc.fence, loc.seg),
            Namespace::Base(base) => layout::base_segment_key(base, loc.fence, loc.seg),
        }
    };
    for (_, loc) in record.overlay.values() {
        if let Some((k, _, _)) = find(&seg_key(loc)) {
            keep.insert(k);
        }
    }
    for ptr in record.leaves.values() {
        let (leaf_key, owner) = if ptr.base != 0 {
            (
                layout::base_leaf_key(ptr.base, ptr.fence, ptr.id),
                VsetId(ptr.base),
            )
        } else {
            match namespace {
                Namespace::Vset(vset) => (layout::leaf_key(vset, ptr.fence, ptr.id), vset),
                Namespace::Base(base) => {
                    (layout::base_leaf_key(base, ptr.fence, ptr.id), VsetId(base))
                }
            }
        };
        let Some((k, _, leaf_bytes)) = find(&leaf_key) else {
            continue;
        };
        keep.insert(k);
        if let Ok(leaf) = MapLeaf::decode(owner, ptr.fence, ptr.id, leaf_bytes) {
            for (_, _, _, loc) in &leaf.entries {
                if let Some((seg, _, _)) = find(&seg_key(loc)) {
                    keep.insert(seg);
                }
            }
        }
    }
}
