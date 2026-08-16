use std::collections::{BTreeMap, BTreeSet};

use blockd_exec::{FaultPoint, fault_point, yield_now};

use super::state::ReplicaKey;
use super::{SharedHost, VsetState};
use crate::blx::{
    BlxFooter, EntryKind, HEADER_BYTES, NamespaceKind, TRAILER_BYTES, open_footer, open_header,
    open_trailer, scan_object,
};
use crate::journal::{JournalRecord, RecordKind};
use crate::layout::{self, BlobName};
use crate::protocol::Verdict;
use crate::replica_spool::scan_replica_spool;
use crate::segment::PageLoc;
use crate::types::{JournalSeq, PageId, SegId, VsetId};
use crate::world::{BlobEntry, Blobs};

#[derive(Default)]
struct Found {
    records: Vec<JournalRecord>,
    migration_checksums:
        BTreeMap<(u64, JournalSeq), BTreeMap<crate::blx::BlockKey, (crate::types::Gen, u64)>>,
    journals: Vec<(u64, JournalSeq)>,
    segments: Vec<(u64, SegId, u64)>,
    record_segments: BTreeMap<JournalSeq, BTreeSet<(u64, SegId)>>,
    tombstone_segments: BTreeSet<(u64, SegId)>,
    segment_refs: BTreeMap<(u64, SegId), crate::manifest::ObjectRef>,
    segment_footers: BTreeMap<(u64, SegId), BlxFooter>,
    names: Vec<String>,
    max_seq: u64,
    max_seg: u64,
    handoff: Option<crate::types::HostId>,
}

/// Rebuild locally-owned vsets from the durable blob set. The scan is sorted
/// before interpretation, making recovery independent of directory order.
#[allow(clippy::too_many_lines)]
pub async fn recover_local<W: Blobs>(
    state: SharedHost,
    world: &W,
) -> Result<BTreeMap<VsetId, Verdict>, crate::world::BlobError> {
    let mut scan = Blobs::scan(world).await?;
    scan.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    state.borrow_mut().blob_sizes = scan
        .iter()
        .map(|blob| (blob.name.clone(), blob.len))
        .collect();
    let mut found = BTreeMap::<VsetId, Found>::new();
    let mut replica_blobs = BTreeMap::<ReplicaKey, BTreeMap<u64, BlobEntry>>::new();
    for blob in scan {
        if let Some(BlobName::ReplicaSpool {
            source,
            vset,
            assignment_epoch,
            generation,
        }) = layout::parse_blob(&blob.name)
        {
            replica_blobs
                .entry(ReplicaKey {
                    source,
                    vset,
                    assignment_epoch,
                })
                .or_default()
                .insert(generation, blob);
            continue;
        }
        collect_blob(&mut found, &blob);
        if blob.bytes.is_empty()
            && let Some(BlobName::Segment { vset, fence, seg }) = layout::parse_blob(&blob.name)
        {
            collect_segment_metadata(&mut found, world, &blob, vset, fence, seg).await?;
        }
    }
    for (key, generations) in replica_blobs {
        recover_replica_blobs(&state, world, key, generations).await?;
    }
    let available_segments = found
        .iter()
        .flat_map(|(&vset, found)| {
            found
                .segments
                .iter()
                .map(move |&(fence, segment, bytes)| ((vset, fence, segment), bytes))
        })
        .collect::<BTreeMap<_, _>>();

    let mut verdicts = BTreeMap::new();
    for (vset, found) in found {
        let usable =
            |record: &&JournalRecord| record_usable(vset, record, &found, &available_segments);
        let chosen = found
            .records
            .iter()
            .filter(usable)
            .max_by_key(|record| (record.capture_seq, record.seq))
            .cloned();
        let Some(chosen) = chosen else {
            verdicts.insert(vset, Verdict::Unrestorable);
            if found.handoff.is_none() {
                Blobs::delete_many_durable(world, &found.names).await?;
                state.borrow_mut().forget_blobs(&found.names);
            }
            continue;
        };
        let usable_watermark = found
            .records
            .iter()
            .filter(usable)
            .map(|record| record.sync_covered_through)
            .max()
            .unwrap_or(0);
        let durability_watermark = found
            .records
            .iter()
            .filter(|record| record.sync_covered_through <= record.capture_seq)
            .map(|record| record.sync_covered_through)
            .max()
            .unwrap_or(0);
        if chosen.capture_seq < durability_watermark {
            verdicts.insert(vset, Verdict::Unrestorable);
            if found.handoff.is_none() {
                Blobs::delete_many_durable(world, &found.names).await?;
                state.borrow_mut().forget_blobs(&found.names);
            }
            continue;
        }
        let watermark = usable_watermark.max(durability_watermark);
        let mut page_locs = if chosen.overlay.is_empty() {
            materialize_files(vset, chosen.config, &chosen.files, &found)
        } else {
            chosen.overlay.clone()
        };
        let migration_checksums = found.migration_checksums.get(&(chosen.fence, chosen.seq));
        let (mut block_checksums, materialized_state_checksum) =
            if let Some(migration_checksums) = migration_checksums {
                let checksum = migration_checksums.iter().fold(
                    0,
                    |checksum, (&key, &(generation, value_checksum))| {
                        checksum ^ crate::blx::state_contribution(key, generation, value_checksum)
                    },
                );
                (migration_checksums.clone(), checksum)
            } else {
                materialize_checksums(&chosen.files, &found)
            };
        let mut state_checksum = chosen.post_state_checksum;
        let mut vmm_segments = materialize_vmm_segments(&chosen.files, &found);
        let mut pending_tombstones = BTreeSet::new();
        let verdict = if let RecordKind::Checkpoint { epoch, vmstate, .. } = chosen.kind
            && chosen.capture_seq >= watermark
        {
            Verdict::Resume { epoch, vmstate }
        } else {
            pending_tombstones.extend(
                block_checksums
                    .keys()
                    .filter(|key| key.space != crate::blx::BlockSpace::Data)
                    .copied(),
            );
            page_locs.retain(|page, _| !chosen.config.is_memory(page.volume.idx));
            block_checksums.retain(|key, _| key.space == crate::blx::BlockSpace::Data);
            vmm_segments.clear();
            state_checksum = block_checksums.iter().fold(
                0,
                |checksum, (&key, &(generation, value_checksum))| {
                    checksum ^ crate::blx::state_contribution(key, generation, value_checksum)
                },
            );
            Verdict::ColdBoot
        };
        if matches!(verdict, Verdict::Resume { .. })
            && migration_checksums.is_some()
            && materialized_state_checksum != chosen.post_state_checksum
        {
            verdicts.insert(vset, Verdict::Unrestorable);
            continue;
        }

        let backed = true;
        let mut host = state.borrow_mut();
        let incarnation = host.allocate_incarnation();
        let mut recovered = VsetState::fresh(chosen.config, incarnation);
        recovered.ready = false;
        recovered.peer_source = chosen.migrated_from.map(|source| source.host);
        recovered.peer_source_offer_fence =
            chosen.migrated_from.and_then(|source| source.offer_fence);
        recovered.fence = chosen.fence;
        recovered.mutation_seq = chosen.capture_seq;
        recovered.next_seq = found.max_seq;
        recovered.next_seg = found.max_seg;
        recovered.next_gen = found
            .segment_footers
            .values()
            .flat_map(|footer| footer.entries.iter())
            .map(|entry| entry.generation.0.saturating_add(1))
            .chain(
                block_checksums
                    .values()
                    .map(|(generation, _)| generation.0.saturating_add(1)),
            )
            .max()
            .unwrap_or(0);
        recovered.local_covered_through = watermark.max(chosen.sync_covered_through);
        recovered.sync_ack_through = chosen.sync_covered_through;
        recovered.peer_committed_through = chosen.sync_covered_through;
        recovered.page_locs = page_locs;
        recovered.block_checksums = block_checksums;
        recovered.state_checksum = state_checksum;
        recovered.archived_memory_usable = !matches!(verdict, Verdict::ColdBoot);
        recovered.pending_tombstones = pending_tombstones;
        recovered.vmm_segments = vmm_segments;
        if let Verdict::Resume { epoch, .. } = verdict {
            recovered.epoch = epoch;
            recovered.pinned = Some(chosen.clone());
        }
        recovered.best_record = Some(chosen);
        if let Some(destination) = found.handoff {
            recovered.outbound = Some(destination);
        }
        // An outbound handoff is already authoritative for this incarnation.
        // It must remain available to serve the destination's post-copy tail
        // even after the destination has claimed the durable head.
        if backed && found.handoff.is_none() {
            recovered.operations.set_recovery(verdict);
        }
        recovered.record_writes = found
            .journals
            .into_iter()
            .map(|(fence, seq)| {
                let watermark = found
                    .records
                    .iter()
                    .find(|record| record.fence == fence && record.seq == seq)
                    .map_or(0, |record| record.sync_covered_through);
                (seq, (fence, watermark))
            })
            .collect();
        recovered.record_segments = found.record_segments;
        recovered.tombstone_segments = found.tombstone_segments;
        recovered.segment_refs = found.segment_refs;
        recovered.segment_blobs = found.segments;
        let previous = host.vsets.insert(vset, recovered);
        assert!(previous.is_none(), "duplicate recovered vset");
        if !backed && found.handoff.is_none() {
            verdicts.insert(vset, verdict);
        }
    }
    Ok(verdicts)
}

async fn recover_replica_blobs<W: Blobs>(
    state: &SharedHost,
    world: &W,
    key: ReplicaKey,
    generations: BTreeMap<u64, BlobEntry>,
) -> Result<(), crate::world::BlobError> {
    if fault_point(FaultPoint::RestartScan) {
        yield_now().await;
    }
    let Some((&current_generation, current_blob)) = generations.last_key_value() else {
        return Ok(());
    };
    let mut combined = Vec::new();
    let mut boundaries = Vec::new();
    for (&generation, blob) in &generations {
        let start = combined.len();
        combined.extend_from_slice(&blob.bytes);
        boundaries.push((generation, start, blob.bytes.len(), blob.name.clone()));
    }
    let Ok(scan) = scan_replica_spool(&combined) else {
        return Ok(());
    };
    let current_file_bytes = if scan.truncated_tail {
        let Some((generation, start, _, name)) = boundaries
            .iter()
            .find(|(_, start, len, _)| scan.valid_len < start.saturating_add(*len))
        else {
            return Ok(());
        };
        if *generation != current_generation {
            return Ok(());
        }
        let valid = scan.valid_len.saturating_sub(*start) as u64;
        super::blob::truncate(state, world, name, valid).await?;
        state.borrow_mut().truncate_blob(name, valid);
        valid
    } else {
        current_blob.bytes.len() as u64
    };
    let committed_cut = scan.commits.last().filter(|commit| {
        (commit.source, commit.vset, commit.assignment_epoch)
            == (key.source, key.vset, key.assignment_epoch)
    });
    let committed = committed_cut.map(|commit| commit.info);
    let committed_record = committed_cut.map(|commit| commit.record.clone());
    let uncommitted_artifacts = scan.uncommitted_artifacts.clone();
    let artifacts = scan
        .artifacts
        .into_iter()
        .filter_map(|(artifact, frame)| {
            ((frame.source, frame.vset, frame.assignment_epoch)
                == (key.source, key.vset, key.assignment_epoch))
                .then_some((artifact, (frame.checksum, frame.bytes)))
        })
        .collect();
    let replica = super::state::ReplicaState {
        artifacts,
        uncommitted_artifacts,
        committed,
        committed_record,
        bytes: scan.valid_len as u64,
        current_generation,
        current_file_bytes,
    };
    let mut host = state.borrow_mut();
    host.counters.replica_bytes = host
        .counters
        .replica_bytes
        .saturating_add(scan.valid_len as u64);
    host.counters.replica_rotations = host
        .counters
        .replica_rotations
        .saturating_add(current_generation);
    host.replica_latest_epoch
        .entry((key.source, key.vset))
        .and_modify(|latest| *latest = (*latest).max(key.assignment_epoch))
        .or_insert(key.assignment_epoch);
    host.replicas.insert(key, replica);
    Ok(())
}

fn collect_blob(found: &mut BTreeMap<VsetId, Found>, blob: &BlobEntry) {
    let Some(parsed) = layout::parse_blob(&blob.name) else {
        return;
    };
    let vset = match parsed {
        BlobName::Journal { vset, .. }
        | BlobName::Segment { vset, .. }
        | BlobName::Handoff { vset } => vset,
        BlobName::ReplicaSpool { .. } => return,
    };
    let entry = found.entry(vset).or_default();
    entry.names.push(blob.name.clone());
    match parsed {
        BlobName::Journal { fence, seq, .. } => {
            entry.journals.push((fence, seq));
            entry.max_seq = entry.max_seq.max(seq.0 + 1);
            if let Ok(record) = JournalRecord::decode(vset, &blob.bytes) {
                if record.fence == fence && record.seq == seq {
                    entry.records.push(record);
                }
            } else if let Ok((record, checksums)) =
                JournalRecord::decode_migration_with_checksums(vset, &blob.bytes)
                && record.fence == fence
                && record.seq == seq
            {
                entry.migration_checksums.insert((fence, seq), checksums);
                entry.records.push(record);
            }
        }
        BlobName::Segment { fence, seg, .. } => {
            entry.max_seg = entry.max_seg.max(seg.0 + 1);
            entry.segments.push((fence, seg, blob.len));
            if let Ok((header, _)) = open_header(&blob.bytes)
                && header.namespace_kind == NamespaceKind::Vset
                && header.namespace_id == vset.0
                && header.writer_fence == fence
                && header.object_id == seg.0
                && header.min_seq == header.max_seq
            {
                entry
                    .record_segments
                    .entry(JournalSeq(header.max_seq))
                    .or_default()
                    .insert((fence, seg));
            }
            if scan_object(&blob.bytes).is_ok_and(|(_, footer)| {
                footer
                    .entries
                    .iter()
                    .any(|entry| entry.kind == EntryKind::Tombstone)
            }) {
                entry.tombstone_segments.insert((fence, seg));
            }
            if let Ok(object) = crate::blx::open_object(&blob.bytes) {
                entry
                    .segment_refs
                    .insert((fence, seg), crate::manifest::ObjectRef::from_blx(&object));
                entry.segment_footers.insert((fence, seg), object.footer);
            }
        }
        BlobName::Handoff { .. } => {
            entry.handoff = super::migration::decode_handoff(vset, &blob.bytes);
        }
        BlobName::ReplicaSpool { .. } => {}
    }
}

async fn collect_segment_metadata<W: Blobs>(
    found: &mut BTreeMap<VsetId, Found>,
    world: &W,
    blob: &BlobEntry,
    vset: VsetId,
    fence: u64,
    seg: SegId,
) -> Result<(), crate::world::BlobError> {
    let Some(header_bytes) = Blobs::read_range(world, &blob.name, 0, HEADER_BYTES as u64).await?
    else {
        return Ok(());
    };
    let Some(trailer_offset) = blob.len.checked_sub(TRAILER_BYTES as u64) else {
        return Ok(());
    };
    let Some(trailer_bytes) =
        Blobs::read_range(world, &blob.name, trailer_offset, TRAILER_BYTES as u64).await?
    else {
        return Ok(());
    };
    let (Ok((header, header_end)), Ok((footer_offset, footer_length))) =
        (open_header(&header_bytes), open_trailer(&trailer_bytes))
    else {
        return Ok(());
    };
    let footer_end = u64::from(footer_offset) + u64::from(footer_length);
    if header.namespace_kind != NamespaceKind::Vset
        || header.namespace_id != vset.0
        || header.writer_fence != fence
        || header.object_id != seg.0
        || header.min_seq != header.max_seq
        || u64::from(footer_offset) < header_end as u64
        || footer_end.checked_add(TRAILER_BYTES as u64) != Some(blob.len)
    {
        return Ok(());
    }
    let Some(footer_bytes) = Blobs::read_range(
        world,
        &blob.name,
        u64::from(footer_offset),
        u64::from(footer_length),
    )
    .await?
    else {
        return Ok(());
    };
    let Ok(footer) = open_footer(&footer_bytes) else {
        return Ok(());
    };
    if footer.entries.len() != header.entry_count as usize
        || footer.entries.first().map(|entry| entry.key) != Some(header.first_key)
        || footer.entries.last().map(|entry| entry.key) != Some(header.last_key)
        || footer
            .entries
            .last()
            .and_then(|entry| entry.offset.checked_add(entry.length))
            != Some(footer_offset)
    {
        return Ok(());
    }
    let Some(size) = u32::try_from(blob.len).ok() else {
        return Ok(());
    };
    let object_ref = crate::manifest::ObjectRef {
        identity: crate::manifest::ObjectIdentity {
            namespace_kind: header.namespace_kind,
            namespace_id: header.namespace_id,
            writer_fence: header.writer_fence,
            object_id: header.object_id,
        },
        min_seq: header.min_seq,
        max_seq: header.max_seq,
        batch_id: header.batch_id,
        chunk_index: header.chunk_index,
        chunk_count: header.chunk_count,
        first_key: header.first_key,
        last_key: header.last_key,
        pre_state_checksum: header.pre_state_checksum,
        post_state_checksum: header.post_state_checksum,
        size,
        footer_offset,
        footer_length,
        // Local recovery validates the header, trailer, and footer with bounded
        // reads. Each selected entry remains independently checksummed when it
        // is faulted; reading the whole object here would defeat lazy recovery.
        object_checksum: 0,
    };
    let entry = found.entry(vset).or_default();
    entry
        .record_segments
        .entry(JournalSeq(header.max_seq))
        .or_default()
        .insert((fence, seg));
    if footer
        .entries
        .iter()
        .any(|entry| entry.kind == EntryKind::Tombstone)
    {
        entry.tombstone_segments.insert((fence, seg));
    }
    entry.segment_refs.insert((fence, seg), object_ref);
    entry.segment_footers.insert((fence, seg), footer);
    Ok(())
}

fn record_usable(
    vset: VsetId,
    record: &JournalRecord,
    found: &Found,
    _segments: &BTreeMap<(VsetId, u64, SegId), u64>,
) -> bool {
    if record.files.is_empty() {
        return record.capture_seq == 0
            || record.migrated_from.is_some()
            || record.post_state_checksum != 0;
    }
    record.files.iter().all(|file| {
        if file.identity.namespace_kind != NamespaceKind::Vset
            || file.identity.namespace_id != vset.0
        {
            return false;
        }
        let key = (file.identity.writer_fence, SegId(file.identity.object_id));
        found.segment_refs.get(&key).is_some_and(|found_ref| {
            found_ref == file
                || (found_ref.object_checksum == 0 && {
                    let mut expected = *file;
                    expected.object_checksum = 0;
                    *found_ref == expected
                })
        }) || (record.migrated_from.is_some() && file.identity.writer_fence < record.fence)
    })
}

fn materialize_files(
    vset: VsetId,
    config: crate::journal::VsetConfig,
    files: &[crate::manifest::ObjectRef],
    found: &Found,
) -> BTreeMap<PageId, (crate::types::Gen, PageLoc)> {
    let mut newest = BTreeMap::<PageId, (crate::types::Gen, Option<PageLoc>)>::new();
    for file in files {
        let key = (file.identity.writer_fence, SegId(file.identity.object_id));
        let Some(footer) = found.segment_footers.get(&key) else {
            continue;
        };
        for entry in &footer.entries {
            let Some(page) = entry.key.to_page(config.kind, vset) else {
                continue;
            };
            let location = (entry.kind == EntryKind::Data).then_some(PageLoc {
                base: 0,
                fence: file.identity.writer_fence,
                seg: SegId(file.identity.object_id),
                offset: entry.offset,
                len: entry.length,
            });
            if newest
                .get(&page)
                .is_none_or(|(generation, _)| *generation <= entry.generation)
            {
                newest.insert(page, (entry.generation, location));
            }
        }
    }
    newest
        .into_iter()
        .filter_map(|(page, (generation, location))| {
            location.map(|location| (page, (generation, location)))
        })
        .collect()
}

fn materialize_checksums(
    files: &[crate::manifest::ObjectRef],
    found: &Found,
) -> (
    BTreeMap<crate::blx::BlockKey, (crate::types::Gen, u64)>,
    u64,
) {
    let mut newest = BTreeMap::<crate::blx::BlockKey, (crate::types::Gen, EntryKind, u64)>::new();
    for file in files {
        let key = (file.identity.writer_fence, SegId(file.identity.object_id));
        let Some(footer) = found.segment_footers.get(&key) else {
            continue;
        };
        for entry in &footer.entries {
            if newest
                .get(&entry.key)
                .is_none_or(|(generation, _, _)| *generation <= entry.generation)
            {
                newest.insert(
                    entry.key,
                    (entry.generation, entry.kind, entry.value_checksum),
                );
            }
        }
    }
    let blocks = newest
        .into_iter()
        .filter_map(|(key, (generation, kind, value_checksum))| {
            (kind == EntryKind::Data).then_some((key, (generation, value_checksum)))
        })
        .collect::<BTreeMap<_, _>>();
    let checksum = blocks
        .iter()
        .fold(0, |checksum, (&key, &(generation, value))| {
            checksum ^ crate::blx::state_contribution(key, generation, value)
        });
    (blocks, checksum)
}

fn materialize_vmm_segments(
    files: &[crate::manifest::ObjectRef],
    found: &Found,
) -> BTreeSet<(u64, SegId)> {
    files
        .iter()
        .filter_map(|file| {
            let segment = (file.identity.writer_fence, SegId(file.identity.object_id));
            found
                .segment_footers
                .get(&segment)
                .is_some_and(|footer| {
                    footer
                        .entries
                        .iter()
                        .any(|entry| entry.key.space == crate::blx::BlockSpace::Vmm)
                })
                .then_some(segment)
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::Future;
    use std::rc::Rc;

    use blockd_exec::{FaultConfig, simulation_scope};

    use super::*;
    use crate::blx::BlockKey;
    use crate::format::{checksum64, crc32c};
    use crate::hostmeta::HostConfig;
    use crate::journal::{RecordKind, VsetConfig, VsetKind};
    use crate::protocol::{ReplicaArtifact, ReplicaCommitInfo};
    use crate::replica_spool::{seal_replica_artifact, seal_replica_commit};
    use crate::segment::SegmentBatchBuilder;
    use crate::types::{Gen, HostId, PageNo, VolumeId, VolumeIdx, page_size};
    use crate::world::BlobError;

    #[derive(Default)]
    struct TestBlobs(RefCell<BTreeMap<String, Vec<u8>>>);

    impl Blobs for TestBlobs {
        async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
            Ok(self
                .0
                .borrow()
                .iter()
                .map(|(name, bytes)| BlobEntry {
                    name: name.clone(),
                    bytes: bytes.clone(),
                    len: bytes.len() as u64,
                })
                .collect())
        }

        async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            self.0.borrow_mut().insert(name, bytes);
            Ok(())
        }

        async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            self.0.borrow_mut().entry(name).or_default().extend(bytes);
            Ok(())
        }

        async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
            self.0
                .borrow_mut()
                .get_mut(name)
                .expect("blob exists")
                .truncate(usize::try_from(len).expect("test length fits"));
            Ok(())
        }

        async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.0.borrow().get(name).cloned())
        }

        async fn read_range(
            &self,
            name: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.0.borrow().get(name).map(|bytes| {
                let start = usize::try_from(offset).expect("test offset fits");
                let end = start.saturating_add(usize::try_from(len).expect("test length fits"));
                bytes[start.min(bytes.len())..end.min(bytes.len())].to_vec()
            }))
        }

        async fn delete(&self, name: &str) -> Result<(), BlobError> {
            self.0.borrow_mut().remove(name);
            Ok(())
        }
    }

    struct MetadataOnlyBlobs(TestBlobs);

    impl Blobs for MetadataOnlyBlobs {
        async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
            Ok(self
                .0
                .0
                .borrow()
                .iter()
                .map(|(name, bytes)| BlobEntry {
                    name: name.clone(),
                    bytes: if matches!(layout::parse_blob(name), Some(BlobName::Segment { .. })) {
                        Vec::new()
                    } else {
                        bytes.clone()
                    },
                    len: bytes.len() as u64,
                })
                .collect())
        }

        async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            Blobs::write(&self.0, name, bytes).await
        }

        async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            Blobs::append(&self.0, name, bytes).await
        }

        async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
            Blobs::truncate(&self.0, name, len).await
        }

        async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
            panic!("recovery read immutable segment payload {name}")
        }

        async fn read_range(
            &self,
            name: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<Vec<u8>>, BlobError> {
            Blobs::read_range(&self.0, name, offset, len).await
        }

        async fn delete(&self, name: &str) -> Result<(), BlobError> {
            Blobs::delete(&self.0, name).await
        }
    }

    fn test_state() -> SharedHost {
        Rc::new(RefCell::new(super::super::state::HostState::new(
            HostConfig {
                archive: Default::default(),
                host: HostId(8),
                cache_pages: 1,
                writeback_interval: 1,
                backup_retry: 1,
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 0,
                replica_placement: None,
            },
        )))
    }

    async fn simulate<T>(seed: u64, future: impl Future<Output = T>) -> T {
        simulation_scope(seed, FaultConfig::default(), future).await
    }

    fn file_ref(bytes: &[u8]) -> crate::manifest::ObjectRef {
        crate::manifest::ObjectRef::from_blx(&crate::blx::open_object(bytes).expect("valid BLX"))
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_rejects_a_record_with_a_missing_segment() {
        let vset = VsetId(1);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let mut builder = SegmentBatchBuilder::new(vset, 1, SegId(0));
        builder.add(page, Gen(1), &vec![7; page_size()]);
        let (_, segment, locations) = builder.finish().pop().expect("fixture object");
        let record = JournalRecord {
            config: VsetConfig::compute(1, 4),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            post_state_checksum: 0,
            files: vec![file_ref(&segment)],
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            migrated_from: None,
        };
        let world = Rc::new(TestBlobs::default());
        world.0.borrow_mut().insert(
            layout::journal_blob(vset, 1, JournalSeq(0)),
            record.encode(vset),
        );
        let state = test_state();
        let verdicts = simulate(2, {
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");
        assert_eq!(verdicts.get(&vset), Some(&Verdict::Unrestorable));
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_uses_segment_metadata_without_reading_payloads() {
        let vset = VsetId(3);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let mut builder = SegmentBatchBuilder::new(vset, 1, SegId(0));
        builder.add(page, Gen(1), &vec![7; page_size()]);
        let (_, segment, locations) = builder.finish().pop().expect("fixture object");
        let record = JournalRecord {
            config: VsetConfig::compute(1, 4),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            post_state_checksum: 0,
            files: vec![file_ref(&segment)],
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            migrated_from: None,
        };
        let blobs = TestBlobs::default();
        blobs
            .0
            .borrow_mut()
            .insert(layout::segment_blob(vset, 1, SegId(0)), segment);
        blobs.0.borrow_mut().insert(
            layout::journal_blob(vset, 1, record.seq),
            record.encode(vset),
        );
        let world = Rc::new(MetadataOnlyBlobs(blobs));
        let state = test_state();
        let verdicts = simulate(2, {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");

        assert_eq!(verdicts.get(&vset), None);
        assert_eq!(state.borrow().vsets[&vset].local_covered_through, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_accepts_a_local_delta_over_an_archived_baseline() {
        let vset = VsetId(6);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(0),
        };
        let bytes = vec![0x6a; page_size()];
        let generation = Gen(4);
        let contribution = crate::blx::state_contribution(
            BlockKey::from_page(VsetKind::Compute, page),
            generation,
            checksum64(&bytes),
        );
        let archive_baseline = 0x1234_5678_9abc_def0;
        let post_state_checksum = archive_baseline ^ contribution;
        let mut builder = SegmentBatchBuilder::new_for_record_with_checksums(
            VsetKind::Compute,
            vset,
            3,
            SegId(0),
            8,
            archive_baseline,
            post_state_checksum,
        );
        builder.add(page, generation, &bytes);
        let [(segment, segment_bytes, _)] = builder.finish().try_into().expect("one segment");
        let record = JournalRecord {
            config: VsetConfig::compute(1, 4),
            seq: JournalSeq(8),
            fence: 3,
            kind: RecordKind::Checkpoint {
                epoch: crate::types::Epoch(2),
                vmstate: 9,
                vmstate_logical_length: 8,
            },
            capture_seq: 8,
            sync_covered_through: 8,
            post_state_checksum,
            files: vec![file_ref(&segment_bytes)],
            overlay: BTreeMap::new(),
            migrated_from: None,
        };
        let world = Rc::new(TestBlobs::default());
        world
            .0
            .borrow_mut()
            .insert(layout::segment_blob(vset, 3, segment), segment_bytes);
        world.0.borrow_mut().insert(
            layout::journal_blob(vset, 3, record.seq),
            record.encode(vset),
        );
        let state = test_state();
        let verdicts = simulate(8, {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");

        assert_eq!(verdicts.get(&vset), None);
        assert_eq!(
            state.borrow().vsets[&vset].state_checksum,
            post_state_checksum
        );
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_accepts_an_empty_local_delta_over_an_archived_baseline() {
        let vset = VsetId(7);
        let post_state_checksum = 0xfeed_face_cafe_beef;
        let record = JournalRecord {
            config: VsetConfig::compute(1, 4),
            seq: JournalSeq(11),
            fence: 5,
            kind: RecordKind::Checkpoint {
                epoch: crate::types::Epoch(3),
                vmstate: 12,
                vmstate_logical_length: 8,
            },
            capture_seq: 11,
            sync_covered_through: 11,
            post_state_checksum,
            files: Vec::new(),
            overlay: BTreeMap::new(),
            migrated_from: None,
        };
        let world = Rc::new(TestBlobs::default());
        world.0.borrow_mut().insert(
            layout::journal_blob(vset, 5, record.seq),
            record.encode(vset),
        );
        let state = test_state();
        let verdicts = simulate(9, {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");

        assert_eq!(verdicts.get(&vset), None);
        assert_eq!(
            state.borrow().vsets[&vset].state_checksum,
            post_state_checksum
        );
    }

    #[tokio::test(start_paused = true)]
    #[allow(clippy::too_many_lines)]
    async fn cold_boot_schedules_tombstones_for_discarded_state_in_mixed_batches() {
        let vset = VsetId(5);
        let config = VsetConfig::compute(1, 4);
        let memory_page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(0),
        };
        let disk_page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let memory_bytes = vec![0x31; page_size()];
        let disk_bytes = vec![0x42; page_size()];
        let vmm_bytes = vec![0x53; page_size()];
        let memory_key = BlockKey::from_page(VsetKind::Compute, memory_page);
        let disk_key = BlockKey::from_page(VsetKind::Compute, disk_page);
        let vmm_key = BlockKey {
            space: crate::blx::BlockSpace::Vmm,
            volume: 0,
            block: 0,
        };
        let entries = [
            (memory_key, Gen(1), checksum64(&memory_bytes)),
            (disk_key, Gen(2), checksum64(&disk_bytes)),
            (vmm_key, Gen(3), checksum64(&vmm_bytes)),
        ];
        let state_checksum = entries
            .iter()
            .fold(0, |checksum, &(key, generation, value)| {
                checksum ^ crate::blx::state_contribution(key, generation, value)
            });
        let mut builder = SegmentBatchBuilder::new_for_record_with_checksums(
            VsetKind::Compute,
            vset,
            1,
            SegId(0),
            0,
            0,
            state_checksum,
        );
        builder.add(memory_page, Gen(1), &memory_bytes);
        builder.add(disk_page, Gen(2), &disk_bytes);
        builder.add_vmm_block(0, Gen(3), &vmm_bytes);
        let objects = builder.finish();
        let record = JournalRecord {
            config,
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            post_state_checksum: state_checksum,
            files: objects
                .iter()
                .map(|(_, bytes, _)| file_ref(bytes))
                .collect(),
            overlay: BTreeMap::new(),
            migrated_from: None,
        };
        let world = Rc::new(TestBlobs::default());
        for (segment, bytes, _) in objects {
            world
                .0
                .borrow_mut()
                .insert(layout::segment_blob(vset, 1, segment), bytes);
        }
        world.0.borrow_mut().insert(
            layout::journal_blob(vset, 1, record.seq),
            record.encode(vset),
        );
        let state = test_state();
        simulate(6, {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");

        let host = state.borrow();
        let recovered = &host.vsets[&vset];
        assert_eq!(
            recovered.page_locs.keys().copied().collect::<Vec<_>>(),
            vec![disk_page]
        );
        assert_eq!(
            recovered
                .block_checksums
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![disk_key]
        );
        assert_eq!(
            recovered.pending_tombstones,
            BTreeSet::from([memory_key, vmm_key])
        );
        assert!(!recovered.archived_memory_usable);
    }

    #[tokio::test(start_paused = true)]
    async fn unrestorable_outbound_handoff_keeps_source_blobs() {
        let vset = VsetId(4);
        let mut builder = SegmentBatchBuilder::new(vset, 1, SegId(0));
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        builder.add(page, Gen(1), &vec![9; page_size()]);
        let (_, segment, _) = builder.finish().pop().expect("fixture object");
        let segment_name = layout::segment_blob(vset, 1, SegId(0));
        let handoff_name = layout::handoff_blob(vset);
        let handoff = super::super::migration::encode_handoff(vset, HostId(9));
        let world = Rc::new(TestBlobs::default());
        world.0.borrow_mut().insert(segment_name.clone(), segment);
        world.0.borrow_mut().insert(handoff_name.clone(), handoff);
        let verdicts = simulate(3, {
            let world = Rc::clone(&world);
            async move { recover_local(test_state(), world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");

        assert_eq!(verdicts.get(&vset), Some(&Verdict::Unrestorable));
        assert!(world.0.borrow().contains_key(&segment_name));
        assert!(world.0.borrow().contains_key(&handoff_name));
    }

    #[tokio::test(start_paused = true)]
    async fn unusable_newer_record_does_not_advance_recovery_watermark() {
        let vset = VsetId(2);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(0),
        };
        let page_bytes = vec![3; page_size()];
        let mut builder = SegmentBatchBuilder::new(vset, 1, SegId(0));
        builder.add(page, Gen(1), &page_bytes);
        let (_, segment, locations) = builder.finish().pop().expect("fixture object");
        let config = VsetConfig::compute(1, 4);
        let checkpoint = JournalRecord {
            config,
            seq: JournalSeq(1),
            fence: 1,
            kind: RecordKind::Checkpoint {
                epoch: crate::types::Epoch(1),
                vmstate: 5,
                vmstate_logical_length: 8,
            },
            capture_seq: 5,
            sync_covered_through: 5,
            post_state_checksum: crate::blx::state_contribution(
                crate::blx::BlockKey::from_page(VsetKind::Compute, page),
                Gen(1),
                checksum64(&page_bytes),
            ),
            files: vec![file_ref(&segment)],
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            migrated_from: None,
        };
        let mut missing = locations[0].2;
        missing.seg = SegId(9);
        let mut missing_file = checkpoint.files[0];
        missing_file.identity.object_id = 9;
        let unusable = JournalRecord {
            seq: JournalSeq(2),
            kind: RecordKind::Commit,
            capture_seq: 6,
            sync_covered_through: 100,
            files: vec![missing_file],
            overlay: BTreeMap::from([(page, (Gen(2), missing))]),
            ..checkpoint.clone()
        };
        let world = Rc::new(TestBlobs::default());
        {
            let mut blobs = world.0.borrow_mut();
            blobs.insert(layout::segment_blob(vset, 1, SegId(0)), segment.clone());
            blobs.insert(
                layout::journal_blob(vset, 1, checkpoint.seq),
                checkpoint.encode(vset),
            );
            blobs.insert(
                layout::journal_blob(vset, 1, unusable.seq),
                unusable.encode(vset),
            );
        }
        let state = test_state();
        let verdicts = simulate(3, {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");
        assert_eq!(verdicts.get(&vset), None);
        assert_eq!(state.borrow().vsets[&vset].local_covered_through, 5);

        let mut sane_unusable = unusable;
        sane_unusable.sync_covered_through = sane_unusable.capture_seq;
        let world = Rc::new(TestBlobs::default());
        {
            let mut blobs = world.0.borrow_mut();
            blobs.insert(layout::segment_blob(vset, 1, SegId(0)), segment);
            blobs.insert(
                layout::journal_blob(vset, 1, checkpoint.seq),
                checkpoint.encode(vset),
            );
            blobs.insert(
                layout::journal_blob(vset, 1, sane_unusable.seq),
                sane_unusable.encode(vset),
            );
        }
        let verdicts = simulate(4, {
            let world = Rc::clone(&world);
            async move { recover_local(test_state(), world.as_ref()).await }
        })
        .await
        .expect("scan succeeds");
        assert_eq!(verdicts.get(&vset), Some(&Verdict::Unrestorable));
    }

    #[tokio::test(start_paused = true)]
    async fn replica_recovery_joins_generations_and_truncates_only_the_torn_tail() {
        let source = HostId(4);
        let vset = VsetId(9);
        let assignment_epoch = 2;
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let mut builder = SegmentBatchBuilder::new(vset, 7, SegId(3));
        builder.add(page, Gen(1), &vec![0x61; page_size()]);
        let (_, segment, locations) = builder.finish().pop().expect("fixture object");
        let artifact = ReplicaArtifact::Segment {
            fence: 7,
            seg: SegId(3),
        };
        let info = ReplicaCommitInfo {
            writer_fence: 7,
            seq: JournalSeq(5),
            sync_covered_through: 11,
        };
        let record = JournalRecord {
            config: VsetConfig {
                kind: VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 4,
            },
            seq: info.seq,
            fence: info.writer_fence,
            kind: RecordKind::Commit,
            capture_seq: 11,
            sync_covered_through: info.sync_covered_through,
            post_state_checksum: 0,
            files: vec![file_ref(&segment)],
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            migrated_from: None,
        }
        .encode(vset);
        let artifact_frame =
            seal_replica_artifact(source, vset, assignment_epoch, artifact, &segment)
                .expect("valid artifact");
        let commit_frame =
            seal_replica_commit(source, vset, assignment_epoch, info, &[artifact], &record)
                .expect("valid commit");
        let generation_zero = layout::replica_spool_blob(source, vset, assignment_epoch);
        let generation_one = layout::replica_spool_segment_blob(source, vset, assignment_epoch, 1);
        let world = Rc::new(TestBlobs::default());
        world
            .0
            .borrow_mut()
            .insert(generation_zero, artifact_frame.clone());
        let mut torn = commit_frame.clone();
        torn.extend_from_slice(&[0x42, 0x53, 0x50]);
        world.0.borrow_mut().insert(generation_one.clone(), torn);
        let state = Rc::new(RefCell::new(super::super::state::HostState::new(
            HostConfig {
                archive: Default::default(),
                host: HostId(8),
                cache_pages: 1,
                writeback_interval: 1,
                backup_retry: 1,
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 0,
                replica_placement: None,
            },
        )));
        let recovered = simulate(1, {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        })
        .await;
        assert_eq!(recovered.expect("recovery succeeds"), BTreeMap::new());
        let host = state.borrow();
        let key = ReplicaKey {
            source,
            vset,
            assignment_epoch,
        };
        let replica = &host.replicas[&key];
        assert_eq!(replica.committed, Some(info));
        assert_eq!(replica.artifacts[&artifact].0, crc32c(&segment));
        assert_eq!(replica.current_generation, 1);
        assert_eq!(replica.current_file_bytes, commit_frame.len() as u64);
        assert_eq!(
            replica.bytes,
            (artifact_frame.len() + commit_frame.len()) as u64
        );
        assert_eq!(world.0.borrow()[&generation_one], commit_frame);
    }
}
