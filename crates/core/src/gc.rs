//! Cluster garbage collection (R9.3): a host actor operating against the
//! bucket alone. Mark from the only roots that exist — head records and
//! base roots — then sweep what nothing reachable references, and only
//! once it is older than the in-flight grace (which protects publishes
//! whose manifest has not landed yet; the grace is never retention policy,
//! R4.5).
//!
//! It can never delete a base (base roots are roots until an explicit
//! `DeleteBase` removes them), never a live vset's state (its head roots
//! its newest manifest and every segment that manifest references,
//! including inherited base segments), and never anything an explicit
//! delete has not unrooted. Undecodable roots are kept: GC refuses to act
//! on anything it cannot vouch for (R8.1's spirit).

use std::collections::BTreeSet;

use crate::head::HeadRecord;
use crate::layout::{self, StoreKey};
use crate::manifest::{BaseManifest, BaseRoot, CompleteFileList, Manifest, ObjectRef};
use crate::types::SimTime;

/// Compute the deletion list for one GC pass over a bucket listing.
/// `objects` is the full LIST (key, last-write time, bytes); reads of
/// manifests and base roots happen from these bytes exactly
/// as the real process would GET them.
pub fn plan(now: SimTime, grace: u64, objects: &[(String, SimTime, Vec<u8>)]) -> Vec<String> {
    let mut keep: BTreeSet<&str> = BTreeSet::new();
    let find = |key: &str| objects.iter().find(|(k, _, _)| k == key);
    let mut unsafe_root = false;

    // Mark. Roots: every head record and every base root — plus
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
                    unsafe_root = true;
                    continue;
                };
                let Some(ptr) = head.manifest else {
                    continue;
                };
                let manifest_key = layout::manifest_key(vset, ptr.fence, ptr.seq);
                let Some((key, _, manifest_bytes)) = find(&manifest_key) else {
                    unsafe_root = true;
                    continue;
                };
                keep.insert(key);
                if crate::format::checksum64(manifest_bytes) != ptr.checksum {
                    unsafe_root = true;
                    continue;
                }
                let Ok(manifest) = Manifest::decode(vset, manifest_bytes) else {
                    unsafe_root = true;
                    continue;
                };
                if !mark_manifest(objects, &mut keep, &manifest) {
                    unsafe_root = true;
                }
            }
            Some(StoreKey::BaseRoot { base }) => {
                keep.insert(key);
                let Ok(root) = BaseRoot::decode(base, bytes) else {
                    unsafe_root = true;
                    continue;
                };
                let manifest_key = layout::base_manifest_key(base, root.manifest_id);
                let Some((manifest_key, _, manifest_bytes)) = find(&manifest_key) else {
                    unsafe_root = true;
                    continue;
                };
                keep.insert(manifest_key);
                let Ok(manifest) = BaseManifest::decode(root, manifest_bytes) else {
                    unsafe_root = true;
                    continue;
                };
                mark_objects(objects, &mut keep, &manifest.objects);
            }
            _ => {}
        }
    }

    // A pending manifest is a durable, cluster-visible publication root.
    // It closes the unbounded retry window between the first artifact PUT
    // and the head CAS. Once that head reaches this record (or a newer one),
    // the marker becomes ordinary garbage because the head owns the closure.
    for (key, _, bytes) in objects {
        let Some(StoreKey::PendingManifest { vset, fence, seq }) = layout::parse_key(key) else {
            continue;
        };
        let Ok(manifest) = Manifest::decode(vset, bytes) else {
            keep.insert(key);
            unsafe_root = true;
            continue;
        };
        if manifest.writer_fence != fence || manifest.archive_seq != seq.0 {
            keep.insert(key);
            unsafe_root = true;
            continue;
        }
        let published = find(&layout::head_key(vset)).is_some_and(|(_, _, bytes)| {
            HeadRecord::decode(vset, bytes)
                .ok()
                .and_then(|head| head.manifest)
                .is_some_and(|current| {
                    (current.capture_seq, current.seq)
                        >= (
                            manifest.capture_seq,
                            crate::types::JournalSeq(manifest.archive_seq),
                        )
                })
        });
        if published {
            continue;
        }
        keep.insert(key);
        if !mark_manifest(objects, &mut keep, &manifest) {
            unsafe_root = true;
        }
    }
    keep.remove("");

    if unsafe_root {
        return Vec::new();
    }

    // Sweep: unreferenced and older than the grace.
    objects
        .iter()
        .filter(|(key, put_at, _)| {
            !keep.contains(key.as_str()) && now.nanos().saturating_sub(put_at.nanos()) > grace
        })
        .map(|(key, _, _)| key.clone())
        .collect()
}

fn mark_manifest<'a>(
    objects: &'a [(String, SimTime, Vec<u8>)],
    keep: &mut BTreeSet<&'a str>,
    manifest: &Manifest,
) -> bool {
    let find = |key: &str| objects.iter().find(|(k, _, _)| k == key);
    let list = match manifest.complete_list {
        None => None,
        Some(reference) => {
            let key = layout::complete_file_list_key(
                manifest.vset,
                reference.writer_fence,
                reference.list_id,
            );
            let Some((key, _, bytes)) = find(&key) else {
                return false;
            };
            keep.insert(key);
            let Ok(list) = CompleteFileList::decode(reference, manifest.vset, bytes) else {
                return false;
            };
            Some(list)
        }
    };
    let Ok(current) = manifest.current_files(list.as_ref()) else {
        return false;
    };
    mark_objects(objects, keep, &current);
    if let Some(base) = manifest.base {
        let key = layout::base_manifest_key(base.base_id, base.manifest_id);
        let Some((key, _, bytes)) = find(&key) else {
            return false;
        };
        keep.insert(key);
        let root = BaseRoot {
            base_id: base.base_id,
            manifest_id: base.manifest_id,
            manifest_checksum: base.manifest_checksum,
            post_state_checksum: base.post_state_checksum,
        };
        let Ok(base_manifest) = BaseManifest::decode(root, bytes) else {
            return false;
        };
        mark_objects(objects, keep, &base_manifest.objects);
    }
    true
}

fn mark_objects<'a>(
    objects: &'a [(String, SimTime, Vec<u8>)],
    keep: &mut BTreeSet<&'a str>,
    references: &[ObjectRef],
) {
    for reference in references {
        let key = reference.identity.store_key();
        if let Some((key, _, _)) = objects.iter().find(|(candidate, _, _)| candidate == &key) {
            keep.insert(key);
        }
    }
}
