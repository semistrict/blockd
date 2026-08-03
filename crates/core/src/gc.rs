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
use crate::types::{SimTime, VsetId};

/// Compute the deletion list for one GC pass over a bucket listing.
/// `objects` is the full LIST (key, last-write time, bytes); reads of
/// manifests and base records happen from these bytes exactly as the real
/// process would GET them.
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
                    for (_, loc) in record.pages.values() {
                        let seg_key = if loc.base != 0 {
                            layout::base_segment_key(loc.base, loc.fence, loc.seg)
                        } else {
                            layout::segment_key(vset, loc.fence, loc.seg)
                        };
                        if let Some((k, _, _)) = find(&seg_key) {
                            keep.insert(k);
                        }
                    }
                }
            }
            Some(StoreKey::BaseRecord { base }) => {
                keep.insert(key);
                let Ok(record) = JournalRecord::decode(VsetId(base), bytes) else {
                    continue;
                };
                for (_, loc) in record.pages.values() {
                    // A base's own pages carry its id; inherited pages carry
                    // an ancestor base's (flattened chains).
                    let seg_key = layout::base_segment_key(loc.base, loc.fence, loc.seg);
                    if let Some((k, _, _)) = find(&seg_key) {
                        keep.insert(k);
                    }
                }
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
