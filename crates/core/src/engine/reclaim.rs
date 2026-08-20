use std::collections::BTreeSet;

use super::SharedHost;
use crate::layout;
use crate::types::{ObjectId, VolumeId};
use crate::world::{BlobError, Blobs};

/// Drop local blx copies whose immutable bytes are already present in
/// Store. Serving maps keep their locations and fault through to Store after
/// the unlink, so this changes placement rather than logical state.
pub async fn reclaim_backed_blx_files<W: Blobs>(
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
            host.volumes.iter().find_map(|(&volume, volume_state)| {
                volume_state
                    .blx_blobs
                    .iter()
                    .find_map(|&(identity, bytes)| {
                        volume_state
                            .backed_blx_files
                            .contains(&identity)
                            .then_some((volume, identity, bytes))
                    })
            })
        };
        let Some((volume, identity, _bytes)) = candidate else {
            return Ok(());
        };
        let name = layout::blx_blob(volume, identity.writer_fence, ObjectId(identity.object_id));
        Blobs::delete(world, &name).await?;
        let mut host = state.borrow_mut();
        host.blob_sizes.remove(&name);
        host.disk_reclaim_requested = !host.disk_reclaim_target_met();
        if let Some(volume_state) = host.volumes.get_mut(&volume) {
            volume_state
                .blx_blobs
                .retain(|(stored, _)| *stored != identity);
            volume_state.tombstone_blx_files.remove(&identity);
            volume_state.vmm_blx_files.remove(&identity);
            volume_state.blx_refs.remove(&identity);
        }
        host.counters.nvme_reclaims += 1;
        host.counters.blobs_deleted += 1;
    }
}

#[allow(clippy::too_many_lines)]
pub async fn cleanup_local<W: Blobs>(
    state: SharedHost,
    world: &W,
    volume: VolumeId,
    run_generation: u64,
) -> Result<(), BlobError> {
    let (names, records_to_remove, blx_to_remove, pressure_reclaims) = {
        let host = state.borrow();
        let Some(volume_state) = host
            .volumes
            .get(&volume)
            .filter(|volume| volume.run_generation == run_generation)
        else {
            return Ok(());
        };
        let (keep_records, mut keep_blx) = volume_state.retention_closure();
        keep_blx.extend(
            volume_state
                .tombstone_blx_files
                .iter()
                .filter(|blx| !volume_state.backed_blx_files.contains(blx))
                .copied(),
        );
        keep_blx.extend(volume_state.publishing_blx_files.iter().copied());
        keep_blx.extend(volume_state.replicating_blx_files.iter().copied());
        let records_to_remove = volume_state
            .record_writes
            .iter()
            .filter_map(|(&seq, &(fence, _))| {
                (!keep_records.contains(&(fence, seq))).then_some((fence, seq))
            })
            .collect::<Vec<_>>();
        let blx_to_remove = volume_state
            .blx_blobs
            .iter()
            .filter_map(|&(identity, _)| (!keep_blx.contains(&identity)).then_some(identity))
            .collect::<Vec<_>>();
        let pressure_reclaims = if host.disk_reclaim_requested {
            blx_to_remove
                .iter()
                .filter(|candidate| volume_state.backed_blx_files.contains(candidate))
                .count()
        } else {
            0
        };
        let names = records_to_remove
            .iter()
            .flat_map(|&(fence, seq)| {
                [
                    layout::journal_blob(volume, fence, seq),
                    layout::journal_mirror_blob(volume, fence, seq),
                ]
            })
            .chain(blx_to_remove.iter().map(|identity| {
                layout::blx_blob(volume, identity.writer_fence, ObjectId(identity.object_id))
            }))
            .collect::<Vec<_>>();
        (names, records_to_remove, blx_to_remove, pressure_reclaims)
    };
    if names.is_empty() {
        return Ok(());
    }
    Blobs::delete_many_durable(world, &names).await?;
    let mut host = state.borrow_mut();
    host.forget_blobs(&names);
    let Some(volume_state) = host
        .volumes
        .get_mut(&volume)
        .filter(|volume| volume.run_generation == run_generation)
    else {
        return Ok(());
    };
    for (_, seq) in records_to_remove {
        volume_state.record_writes.remove(&seq);
        volume_state.record_blx_files.remove(&seq);
    }
    let blx_set = blx_to_remove.into_iter().collect::<BTreeSet<_>>();
    volume_state
        .blx_blobs
        .retain(|(identity, _)| !blx_set.contains(identity));
    volume_state
        .tombstone_blx_files
        .retain(|blx| !blx_set.contains(blx));
    volume_state
        .vmm_blx_files
        .retain(|blx| !blx_set.contains(blx));
    volume_state
        .blx_refs
        .retain(|blx, _| !blx_set.contains(blx));
    host.counters.blobs_deleted += names.len() as u64;
    if pressure_reclaims > 0 {
        host.disk_reclaim_requested = !host.disk_reclaim_target_met();
        host.counters.nvme_reclaims = host
            .counters
            .nvme_reclaims
            .saturating_add(u64::try_from(pressure_reclaims).expect("reclaim count fits u64"));
    }
    Ok(())
}
