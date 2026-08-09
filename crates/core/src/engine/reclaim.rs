use std::collections::BTreeSet;

use super::SharedHost;
use crate::journal::JournalRecord;
use crate::layout;
use crate::mapleaf::LeafPtr;
use crate::types::{JournalSeq, SegId, VsetId};
use crate::world::{BlobError, Blobs};

type LeafBlobs = std::collections::BTreeMap<LeafPtr, (u64, BTreeSet<(u64, SegId)>)>;

/// Drop local segment copies whose immutable bytes are already present in
/// Store. Serving maps keep their locations and fault through to Store after
/// the unlink, so this changes placement rather than logical state.
pub async fn reclaim_backed_segments<W: Blobs>(
    state: SharedHost,
    world: &W,
) -> Result<(), BlobError> {
    loop {
        let candidate = {
            let host = state.borrow();
            let over_limit = host.config.disk_capacity.is_some_and(|capacity| {
                host.blob_sizes
                    .values()
                    .sum::<u64>()
                    .saturating_add(host.config.disk_headroom)
                    > capacity
            });
            if !over_limit && !host.disk_reclaim_requested {
                return Ok(());
            }
            host.vsets.iter().find_map(|(&vset, vset_state)| {
                vset_state
                    .config
                    .durability
                    .uses_store()
                    .then(|| {
                        vset_state
                            .segment_blobs
                            .iter()
                            .find_map(|&(fence, segment, bytes)| {
                                vset_state
                                    .backed_segments
                                    .contains(&(fence, segment))
                                    .then_some((vset, fence, segment, bytes))
                            })
                    })
                    .flatten()
            })
        };
        let Some((vset, fence, segment, _bytes)) = candidate else {
            return Ok(());
        };
        let name = layout::segment_blob(vset, fence, segment);
        Blobs::delete(world, &name).await?;
        let mut host = state.borrow_mut();
        host.blob_sizes.remove(&name);
        host.disk_reclaim_requested = false;
        if let Some(vset_state) = host.vsets.get_mut(&vset) {
            vset_state
                .segment_blobs
                .retain(|(stored_fence, stored_segment, _)| {
                    (*stored_fence, *stored_segment) != (fence, segment)
                });
        }
        host.counters.nvme_reclaims += 1;
        host.counters.blobs_deleted += 1;
    }
}

#[allow(clippy::too_many_lines)]
pub async fn cleanup_local<W: Blobs>(
    state: SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
) -> Result<(), BlobError> {
    let (names, records_to_remove, segments_to_remove, leaves_to_remove) = {
        let host = state.borrow();
        let Some(vset_state) = host
            .vsets
            .get(&vset)
            .filter(|vset| vset.incarnation == incarnation)
        else {
            return Ok(());
        };
        let mut keep_records = BTreeSet::new();
        let mut keep_segments = BTreeSet::new();
        let mut keep_leaves = BTreeSet::new();
        if let Some(record) = &vset_state.best_record {
            add_closure(
                record,
                &vset_state.leaf_blobs,
                &mut keep_records,
                &mut keep_segments,
                &mut keep_leaves,
            );
        }
        if let Some(record) = &vset_state.pinned {
            add_closure(
                record,
                &vset_state.leaf_blobs,
                &mut keep_records,
                &mut keep_segments,
                &mut keep_leaves,
            );
        }
        let records_to_remove = vset_state
            .record_writes
            .iter()
            .filter_map(|(&seq, &(fence, _))| {
                (!keep_records.contains(&(fence, seq))).then_some((fence, seq))
            })
            .collect::<Vec<_>>();
        let segments_to_remove = vset_state
            .segment_blobs
            .iter()
            .filter_map(|&(fence, segment, _)| {
                (!keep_segments.contains(&(fence, segment))).then_some((fence, segment))
            })
            .collect::<Vec<_>>();
        let leaves_to_remove = vset_state
            .leaf_blobs
            .keys()
            .filter(|pointer| pointer.base == 0 && !keep_leaves.contains(pointer))
            .copied()
            .collect::<Vec<_>>();
        let names = records_to_remove
            .iter()
            .flat_map(|&(fence, seq)| {
                [
                    layout::journal_blob(vset, fence, seq),
                    layout::journal_mirror_blob(vset, fence, seq),
                ]
            })
            .chain(
                segments_to_remove
                    .iter()
                    .map(|&(fence, segment)| layout::segment_blob(vset, fence, segment)),
            )
            .chain(
                leaves_to_remove
                    .iter()
                    .map(|pointer| layout::leaf_blob(vset, pointer.fence, pointer.id)),
            )
            .collect::<Vec<_>>();
        (
            names,
            records_to_remove,
            segments_to_remove,
            leaves_to_remove,
        )
    };
    if names.is_empty() {
        return Ok(());
    }
    Blobs::delete_many_durable(world, &names).await?;
    let mut host = state.borrow_mut();
    host.forget_blobs(&names);
    let Some(vset_state) = host
        .vsets
        .get_mut(&vset)
        .filter(|vset| vset.incarnation == incarnation)
    else {
        return Ok(());
    };
    for (_, seq) in records_to_remove {
        vset_state.record_writes.remove(&seq);
    }
    let segment_set = segments_to_remove.into_iter().collect::<BTreeSet<_>>();
    vset_state
        .segment_blobs
        .retain(|(fence, segment, _)| !segment_set.contains(&(*fence, *segment)));
    for pointer in leaves_to_remove {
        vset_state.leaf_blobs.remove(&pointer);
    }
    host.counters.blobs_deleted += names.len() as u64;
    Ok(())
}

fn add_closure(
    record: &JournalRecord,
    leaf_blobs: &LeafBlobs,
    records: &mut BTreeSet<(u64, JournalSeq)>,
    segments: &mut BTreeSet<(u64, SegId)>,
    leaves: &mut BTreeSet<LeafPtr>,
) {
    records.insert((record.fence, record.seq));
    segments.extend(
        record
            .overlay
            .values()
            .filter(|(_, location)| location.base == 0)
            .map(|(_, location)| (location.fence, location.seg)),
    );
    for pointer in record.leaves.values().filter(|pointer| pointer.base == 0) {
        leaves.insert(*pointer);
        if let Some((_, leaf_segments)) = leaf_blobs.get(pointer) {
            segments.extend(leaf_segments);
        }
    }
}
