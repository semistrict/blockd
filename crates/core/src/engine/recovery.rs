use std::collections::BTreeMap;

use blockd_exec::{FaultPoint, fault_point, yield_now};

use super::state::ReplicaKey;
use super::{SharedHost, VsetState};
use crate::journal::{JournalRecord, RecordKind, VsetKind};
use crate::layout::{self, BlobName};
use crate::mapleaf::{LeafPtr, MapLeaf, span_is_memory};
use crate::protocol::Verdict;
use crate::replica_spool::scan_replica_spool;
use crate::segment::PageLoc;
use crate::types::{JournalSeq, PageId, SegId, VolumeId, VsetId};
use crate::world::{BlobEntry, Blobs};

#[derive(Default)]
struct Found {
    records: Vec<JournalRecord>,
    journals: Vec<(u64, JournalSeq)>,
    segments: Vec<(u64, SegId, u64)>,
    leaves: BTreeMap<LeafPtr, (u64, MapLeaf)>,
    names: Vec<String>,
    max_seq: u64,
    max_seg: u64,
    max_leaf: u64,
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
        let usable = |record: &&JournalRecord| {
            record_usable(vset, record, &found.leaves, &available_segments)
        };
        let chosen = found
            .records
            .iter()
            .filter(usable)
            .max_by_key(|record| (record.capture_seq, record.seq))
            .cloned();
        let Some(mut chosen) = chosen else {
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
        let verdict = if chosen.config.kind == VsetKind::Database {
            Verdict::DatabaseReady {
                synced_through: chosen.sync_covered_through,
            }
        } else if let RecordKind::Checkpoint { epoch, vmstate } = chosen.kind
            && chosen.capture_seq >= watermark
        {
            Verdict::Resume { epoch, vmstate }
        } else {
            chosen
                .overlay
                .retain(|page, _| !chosen.config.is_memory(page.volume.idx));
            chosen.leaves.retain(|span, _| !span_is_memory(*span));
            Verdict::ColdBoot
        };

        let backed = chosen.config.durability.uses_store();
        let mut host = state.borrow_mut();
        let incarnation = host.allocate_incarnation();
        let mut recovered = VsetState::fresh(chosen.config, incarnation);
        recovered.ready = !backed && found.handoff.is_none();
        recovered.database = chosen.database;
        recovered.peer_source = chosen.migrated_from;
        recovered.fence = chosen.fence;
        recovered.mutation_seq = chosen.capture_seq;
        recovered.next_seq = found.max_seq;
        recovered.next_seg = found.max_seg;
        recovered.next_leaf = found.max_leaf;
        recovered.next_gen = chosen
            .overlay
            .values()
            .map(|(generation, _)| generation.0 + 1)
            .max()
            .unwrap_or(0);
        recovered.local_covered_through = watermark.max(chosen.sync_covered_through);
        if !recovered.config.durability.requires_peer_sync() {
            recovered.sync_ack_through = recovered.local_covered_through;
        }
        recovered.overlay = chosen.overlay.clone();
        recovered.leaf_table = chosen.leaves.clone();
        recovered.hydrated_spans = chosen.leaves.keys().copied().collect();
        recovered.page_locs = materialize(vset, &chosen, &found.leaves);
        recovered.leaf_blobs = found
            .leaves
            .iter()
            .map(|(&pointer, (size, leaf))| {
                let segments = leaf
                    .entries
                    .iter()
                    .filter(|(_, _, _, location)| location.base == 0)
                    .map(|(_, _, _, location)| (location.fence, location.seg))
                    .collect();
                (pointer, (*size, segments))
            })
            .collect();
        recovered.next_gen = recovered
            .page_locs
            .values()
            .map(|(generation, _)| generation.0 + 1)
            .max()
            .unwrap_or(recovered.next_gen);
        if let Verdict::Resume { epoch, .. } = verdict {
            recovered.epoch = epoch;
            recovered.pinned = Some(chosen.clone());
        }
        recovered.best_record = Some(chosen);
        if let Some(destination) = found.handoff {
            recovered.outbound = Some(destination);
            recovered.migration_running = true;
        }
        if backed {
            recovered.pending_verdict = Some(verdict);
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
        Blobs::truncate(world, name, valid).await?;
        state.borrow_mut().truncate_blob(name, valid);
        valid
    } else {
        current_blob.bytes.len() as u64
    };
    let committed = scan.commits.last().and_then(|commit| {
        ((commit.source, commit.vset, commit.assignment_epoch)
            == (key.source, key.vset, key.assignment_epoch))
            .then_some(commit.info)
    });
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
        committed,
        uploaded: None,
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
        | BlobName::Leaf { vset, .. }
        | BlobName::BaseLeaf { vset, .. }
        | BlobName::Handoff { vset } => vset,
        BlobName::ReplicaSpool { .. } => return,
    };
    let entry = found.entry(vset).or_default();
    entry.names.push(blob.name.clone());
    match parsed {
        BlobName::Journal { fence, seq, .. } => {
            entry.journals.push((fence, seq));
            entry.max_seq = entry.max_seq.max(seq.0 + 1);
            if let Ok(record) = JournalRecord::decode(vset, &blob.bytes)
                && record.fence == fence
                && record.seq == seq
            {
                entry.records.push(record);
            }
        }
        BlobName::Segment { fence, seg, .. } => {
            entry.max_seg = entry.max_seg.max(seg.0 + 1);
            entry.segments.push((fence, seg, blob.len));
        }
        BlobName::Leaf { fence, id, .. } => {
            entry.max_leaf = entry.max_leaf.max(id + 1);
            if let Ok(leaf) = MapLeaf::decode(vset, fence, id, &blob.bytes) {
                entry
                    .leaves
                    .insert(LeafPtr { base: 0, fence, id }, (blob.len, leaf));
            }
        }
        BlobName::BaseLeaf {
            base, fence, id, ..
        } => {
            if let Ok(leaf) = MapLeaf::decode(VsetId(base), fence, id, &blob.bytes) {
                entry
                    .leaves
                    .insert(LeafPtr { base, fence, id }, (blob.len, leaf));
            }
        }
        BlobName::Handoff { .. } => {
            entry.handoff = super::migration::decode_handoff(vset, &blob.bytes);
        }
        BlobName::ReplicaSpool { .. } => {}
    }
}

fn record_usable(
    vset: VsetId,
    record: &JournalRecord,
    leaves: &BTreeMap<LeafPtr, (u64, MapLeaf)>,
    segments: &BTreeMap<(VsetId, u64, SegId), u64>,
) -> bool {
    let location_exists = |location: &PageLoc| {
        let owner = if location.base == 0 {
            vset
        } else {
            VsetId(location.base)
        };
        match segments.get(&(owner, location.fence, location.seg)) {
            Some(bytes) => {
                location.len != 0
                    && u64::from(location.offset)
                        .checked_add(u64::from(location.len))
                        .is_some_and(|end| end <= *bytes)
            }
            None => {
                location.base == 0
                    && record.migrated_from.is_some()
                    && location.fence < record.fence
            }
        }
    };
    record
        .overlay
        .values()
        .all(|(_, location)| location_exists(location))
        && record.leaves.values().all(|pointer| {
            leaves.get(pointer).is_some_and(|(_, leaf)| {
                leaf.entries
                    .iter()
                    .all(|(_, _, _, location)| location_exists(location))
            })
        })
}

fn materialize(
    vset: VsetId,
    record: &JournalRecord,
    leaves: &BTreeMap<LeafPtr, (u64, MapLeaf)>,
) -> BTreeMap<PageId, (crate::types::Gen, crate::segment::PageLoc)> {
    let mut locations = BTreeMap::new();
    for pointer in record.leaves.values() {
        let leaf = &leaves[pointer].1;
        for &(idx, page, generation, location) in &leaf.entries {
            let page = PageId {
                volume: VolumeId { vset, idx },
                page,
            };
            if record.config.contains(page) {
                locations.insert(page, (generation, location));
            }
        }
    }
    for (&page, &location) in &record.overlay {
        locations.insert(page, location);
    }
    locations
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use async_trait::async_trait;
    use blockd_exec::Executor;

    use super::*;
    use crate::format::crc32c;
    use crate::hostmeta::HostConfig;
    use crate::journal::{DatabaseMeta, DurabilityMode, RecordKind, VsetConfig, VsetKind};
    use crate::protocol::{ReplicaArtifact, ReplicaCommitInfo};
    use crate::replica_spool::{seal_replica_artifact, seal_replica_commit};
    use crate::segment::SegmentBuilder;
    use crate::types::{Gen, HostId, PageNo, VolumeIdx, page_size};
    use crate::world::BlobError;

    #[derive(Default)]
    struct TestBlobs(RefCell<BTreeMap<String, Vec<u8>>>);

    #[async_trait(?Send)]
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

    #[async_trait(?Send)]
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

    #[test]
    fn recovery_rejects_a_record_with_a_missing_segment() {
        let vset = VsetId(1);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let mut builder = SegmentBuilder::new(vset, 1, SegId(0));
        builder.add(page, Gen(1), &vec![7; page_size()]);
        let (_, locations) = builder.finish();
        let record = JournalRecord {
            config: VsetConfig::compute(1, 4, false),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            database: DatabaseMeta::default(),
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let world = Rc::new(TestBlobs::default());
        world.0.borrow_mut().insert(
            layout::journal_blob(vset, 1, JournalSeq(0)),
            record.encode(vset),
        );
        let state = test_state();
        let mut executor = Executor::simulation(2);
        let verdicts = executor
            .block_on({
                let world = Rc::clone(&world);
                async move { recover_local(state, world.as_ref()).await }
            })
            .expect("scan succeeds");
        assert_eq!(verdicts.get(&vset), Some(&Verdict::Unrestorable));
    }

    #[test]
    fn recovery_uses_segment_metadata_without_reading_payloads() {
        let vset = VsetId(3);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let mut builder = SegmentBuilder::new(vset, 1, SegId(0));
        builder.add(page, Gen(1), &vec![7; page_size()]);
        let (segment, locations) = builder.finish();
        let record = JournalRecord {
            config: VsetConfig::compute(1, 4, false),
            seq: JournalSeq(0),
            fence: 1,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            database: DatabaseMeta::default(),
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            leaves: BTreeMap::new(),
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
        let mut executor = Executor::simulation(2);

        let verdicts = executor
            .block_on({
                let world = Rc::clone(&world);
                async move { recover_local(test_state(), world.as_ref()).await }
            })
            .expect("scan succeeds");

        assert_eq!(verdicts.get(&vset), Some(&Verdict::ColdBoot));
    }

    #[test]
    fn unrestorable_outbound_handoff_keeps_source_blobs() {
        let vset = VsetId(4);
        let mut builder = SegmentBuilder::new(vset, 1, SegId(0));
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        builder.add(page, Gen(1), &vec![9; page_size()]);
        let (segment, _) = builder.finish();
        let segment_name = layout::segment_blob(vset, 1, SegId(0));
        let handoff_name = layout::handoff_blob(vset);
        let handoff = super::super::migration::encode_handoff(vset, HostId(9));
        let world = Rc::new(TestBlobs::default());
        world.0.borrow_mut().insert(segment_name.clone(), segment);
        world.0.borrow_mut().insert(handoff_name.clone(), handoff);
        let mut executor = Executor::simulation(3);

        let verdicts = executor
            .block_on({
                let world = Rc::clone(&world);
                async move { recover_local(test_state(), world.as_ref()).await }
            })
            .expect("scan succeeds");

        assert_eq!(verdicts.get(&vset), Some(&Verdict::Unrestorable));
        assert!(world.0.borrow().contains_key(&segment_name));
        assert!(world.0.borrow().contains_key(&handoff_name));
    }

    #[test]
    fn unusable_newer_record_does_not_advance_recovery_watermark() {
        let vset = VsetId(2);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(0),
        };
        let mut builder = SegmentBuilder::new(vset, 1, SegId(0));
        builder.add(page, Gen(1), &vec![3; page_size()]);
        let (segment, locations) = builder.finish();
        let config = VsetConfig::compute(1, 4, false);
        let checkpoint = JournalRecord {
            config,
            seq: JournalSeq(1),
            fence: 1,
            kind: RecordKind::Checkpoint {
                epoch: crate::types::Epoch(1),
                vmstate: 5,
            },
            capture_seq: 5,
            sync_covered_through: 5,
            database: DatabaseMeta::default(),
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let mut missing = locations[0].2;
        missing.seg = SegId(9);
        let unusable = JournalRecord {
            seq: JournalSeq(2),
            kind: RecordKind::Commit,
            capture_seq: 6,
            sync_covered_through: 100,
            overlay: BTreeMap::from([(page, (Gen(2), missing))]),
            ..checkpoint.clone()
        };
        let world = Rc::new(TestBlobs::default());
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
        drop(blobs);
        let state = test_state();
        let mut executor = Executor::simulation(3);
        let verdicts = executor
            .block_on({
                let state = Rc::clone(&state);
                let world = Rc::clone(&world);
                async move { recover_local(state, world.as_ref()).await }
            })
            .expect("scan succeeds");
        assert_eq!(
            verdicts.get(&vset),
            Some(&Verdict::Resume {
                epoch: crate::types::Epoch(1),
                vmstate: 5,
            })
        );
        assert_eq!(state.borrow().vsets[&vset].local_covered_through, 5);

        let mut sane_unusable = unusable;
        sane_unusable.sync_covered_through = sane_unusable.capture_seq;
        let world = Rc::new(TestBlobs::default());
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
        drop(blobs);
        let mut executor = Executor::simulation(4);
        let verdicts = executor
            .block_on({
                let world = Rc::clone(&world);
                async move { recover_local(test_state(), world.as_ref()).await }
            })
            .expect("scan succeeds");
        assert_eq!(verdicts.get(&vset), Some(&Verdict::Unrestorable));
    }

    #[test]
    fn replica_recovery_joins_generations_and_truncates_only_the_torn_tail() {
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
        let mut builder = SegmentBuilder::new(vset, 7, SegId(3));
        builder.add(page, Gen(1), &vec![0x61; page_size()]);
        let (segment, locations) = builder.finish();
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
                durability: DurabilityMode::PeerStashed,
            },
            seq: info.seq,
            fence: info.writer_fence,
            kind: RecordKind::Commit,
            capture_seq: 11,
            sync_covered_through: info.sync_covered_through,
            database: DatabaseMeta::default(),
            overlay: BTreeMap::from([(page, (Gen(1), locations[0].2))]),
            leaves: BTreeMap::new(),
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
        let mut executor = Executor::simulation(1);
        let recovered = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { recover_local(state, world.as_ref()).await }
        });
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
