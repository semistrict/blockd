//! Recovery (R8.2): rebuild a daemon from durable state alone, with an
//! explicit per-vset verdict.

use std::collections::{BTreeMap, BTreeSet};

use super::{Daemon, DaemonConfig, PassiveReplica, ReplicaKey, Vset};
use crate::format::crc32c;
use crate::journal::{JournalRecord, RecordKind, VsetKind};
use crate::layout::{self, BlobName};
use crate::mapleaf::{LeafPtr, MapLeaf, span_is_memory};
use crate::protocol::Verdict;
use crate::replica_spool::scan_replica_spool;
use crate::seam::Effect;
use crate::types::{HostId, JournalSeq, PageId, SegId, VolumeId, VsetId};

/// One local recovery entry. Segment contents are verified lazily on first
/// read, so callers may omit their bytes while still reporting the on-disk
/// length. Metadata-bearing blobs must provide their complete contents.
#[derive(Clone, Copy)]
pub struct RecoveryBlob<'a> {
    pub name: &'a str,
    pub bytes: &'a [u8],
    pub len: u64,
}

impl Daemon {
    /// Rebuild a daemon from a scan of the local device. Only journal blobs
    /// are decoded (records carry every location); segment bytes are
    /// verified lazily on the fill path. Returns per-vset verdicts and the
    /// effects that reclaim garbage and arm the writeback timer.
    #[allow(clippy::too_many_lines)]
    pub fn recover<'a>(
        config: DaemonConfig,
        blobs: impl Iterator<Item = (&'a str, &'a [u8])>,
    ) -> (Daemon, BTreeMap<VsetId, Verdict>, Vec<Effect>) {
        Self::recover_with_metadata(
            config,
            blobs.map(|(name, bytes)| RecoveryBlob {
                name,
                bytes,
                len: bytes.len() as u64,
            }),
        )
    }

    /// Runtime recovery variant that avoids loading segment payloads merely
    /// to learn their sizes.
    #[allow(clippy::too_many_lines)]
    pub fn recover_with_metadata<'a>(
        config: DaemonConfig,
        blobs: impl Iterator<Item = RecoveryBlob<'a>>,
    ) -> (Daemon, BTreeMap<VsetId, Verdict>, Vec<Effect>) {
        struct Found<'a> {
            records: Vec<JournalRecord>,
            journal_names: Vec<(u64, JournalSeq)>,
            seg_names: Vec<(u64, SegId, u64)>,
            /// Intact leaf blobs: ptr → (size, decoded content).
            leaves: BTreeMap<LeafPtr, (u64, MapLeaf)>,
            /// Every scanned name of this vset, for wreckage reclaim.
            names: Vec<&'a str>,
            max_seq: u64,
            max_seg: u64,
            max_leaf: u64,
            handoff: Option<crate::types::HostId>,
        }
        // Canonicalize: recovery must be a function of the blob SET, not
        // the scan sequence. A real directory walk yields readdir order;
        // the simulation's blob device yields name order — and scan order
        // leaks into recovered state (`seg_blobs` order, the cold-record
        // tiebreak, which duplicate-seq record wins `record_ws`). Sorting
        // here makes every production recovery byte-identical to the one
        // the simulation proved on the same bytes.
        let mut scan: Vec<RecoveryBlob<'a>> = blobs.collect();
        scan.sort_unstable_by_key(|blob| blob.name);
        let mut found: BTreeMap<VsetId, Found> = BTreeMap::new();
        // The fence floor: the highest fence this disk has EVER held per
        // vset, including vsets recovery abandons as unrestorable. Their
        // blobs stay behind (reclaim is explicit, R4.5) — and a later
        // inbound migration that derived its fence from the offer alone
        // could land on the same fence and collide with the wreckage's
        // surviving write-once names. Adoption goes strictly above this.
        let mut fence_floors: BTreeMap<VsetId, u64> = BTreeMap::new();
        let mut replica_blobs: BTreeMap<ReplicaKey, BTreeMap<u64, &'a [u8]>> = BTreeMap::new();
        for RecoveryBlob { name, bytes, len } in scan {
            let Some(parsed) = layout::parse_blob(name) else {
                continue;
            };
            if let BlobName::ReplicaSpool {
                source,
                vset,
                assignment_epoch,
                generation,
            } = parsed
            {
                replica_blobs
                    .entry(ReplicaKey {
                        source,
                        vset,
                        assignment_epoch,
                    })
                    .or_default()
                    .insert(generation, bytes);
                continue;
            }
            let vset = match parsed {
                BlobName::Journal { vset, .. }
                | BlobName::Segment { vset, .. }
                | BlobName::Leaf { vset, .. }
                | BlobName::BaseLeaf { vset, .. }
                | BlobName::Handoff { vset } => vset,
                BlobName::ReplicaSpool { .. } => unreachable!("handled above"),
            };
            if let BlobName::Journal { fence, .. }
            | BlobName::Segment { fence, .. }
            | BlobName::Leaf { fence, .. } = parsed
            {
                let floor = fence_floors.entry(vset).or_insert(0);
                *floor = (*floor).max(fence);
            }
            let f = found.entry(vset).or_insert_with(|| Found {
                records: Vec::new(),
                journal_names: Vec::new(),
                seg_names: Vec::new(),
                leaves: BTreeMap::new(),
                names: Vec::new(),
                max_seq: 0,
                max_seg: 0,
                max_leaf: 0,
                handoff: None,
            });
            f.names.push(name);
            match parsed {
                BlobName::Journal { fence, seq, .. } => {
                    f.journal_names.push((fence, seq));
                    f.max_seq = f.max_seq.max(seq.0 + 1);
                    if let Ok(record) = JournalRecord::decode(vset, bytes) {
                        // A record whose name and payload disagree is damage.
                        if record.seq == seq && record.fence == fence {
                            f.records.push(record);
                        }
                    }
                }
                BlobName::Segment { fence, seg, .. } => {
                    f.seg_names.push((fence, seg, len));
                    f.max_seg = f.max_seg.max(seg.0 + 1);
                }
                BlobName::Leaf { fence, id, .. } => {
                    f.max_leaf = f.max_leaf.max(id + 1);
                    // A damaged leaf simply is not there: any record that
                    // needs it becomes unusable and recovery falls back.
                    if let Ok(leaf) = MapLeaf::decode(vset, fence, id, bytes) {
                        let ptr = LeafPtr { base: 0, fence, id };
                        f.leaves.insert(ptr, (bytes.len() as u64, leaf));
                    }
                }
                BlobName::BaseLeaf {
                    base, fence, id, ..
                } => {
                    if let Ok(leaf) = MapLeaf::decode(VsetId(base), fence, id, bytes) {
                        let ptr = LeafPtr { base, fence, id };
                        f.leaves.insert(ptr, (bytes.len() as u64, leaf));
                    }
                }
                BlobName::Handoff { .. } => {
                    // An intact marker means the handoff committed (R7.2);
                    // a torn one means it never did — recover normally.
                    if let Ok(h) = super::migrate::Handoff::decode(vset, bytes) {
                        f.handoff = Some(h.to);
                    }
                }
                BlobName::ReplicaSpool { .. } => unreachable!("handled above"),
            }
        }

        let mut recovered_replicas: BTreeMap<ReplicaKey, PassiveReplica> = BTreeMap::new();
        let mut replica_truncations: BTreeMap<ReplicaKey, (u64, u64)> = BTreeMap::new();
        let mut replica_bytes = 0u64;
        let mut recovered_rotations = 0u64;
        for (key, generations) in replica_blobs {
            let Some((&current_generation, current_bytes)) = generations.last_key_value() else {
                continue;
            };
            // Ordinary generations form one ordered log, but a compaction
            // generation is also a self-contained checkpoint. Trying every
            // generation suffix lets recovery survive an interrupted durable
            // unlink that left arbitrary obsolete files beside that checkpoint.
            let mut combined = Vec::new();
            let mut boundaries = Vec::new();
            for (&generation, bytes) in &generations {
                let start = combined.len();
                combined.extend_from_slice(bytes);
                boundaries.push((generation, start, bytes.len()));
            }
            let Some((scan_start, scan)) = boundaries
                .iter()
                .filter_map(|(_, start, _)| {
                    scan_replica_spool(&combined[*start..])
                        .ok()
                        .filter(|scan| scan.valid_len > 0)
                        .map(|scan| (*start, scan))
                })
                .max_by_key(|(start, scan)| {
                    let newest = scan.commits.last().map(|commit| {
                        (
                            commit.info.sync_covered_through,
                            commit.info.writer_fence,
                            commit.info.seq.0,
                        )
                    });
                    (newest.is_some(), newest.unwrap_or_default(), *start)
                })
            else {
                continue;
            };
            let valid_end = scan_start.saturating_add(scan.valid_len);
            let current_file_bytes = if scan.truncated_tail {
                let Some(&(generation, start, _)) = boundaries
                    .iter()
                    .find(|(_, start, len)| valid_end < start.saturating_add(*len))
                else {
                    continue;
                };
                // The ordered append lane cannot create a later generation
                // after an incomplete append. Refuse impossible residue.
                if generation != current_generation {
                    continue;
                }
                let valid_in_generation = valid_end.saturating_sub(start) as u64;
                replica_truncations.insert(key, (generation, valid_in_generation));
                valid_in_generation
            } else {
                current_bytes.len() as u64
            };
            let stored_bytes = (combined.len() - current_bytes.len()) as u64 + current_file_bytes;
            let artifacts = scan
                .artifacts
                .into_iter()
                .map(|(id, frame)| (id, (frame.checksum, frame.bytes)))
                .collect();
            let uncommitted_artifacts = scan.uncommitted_artifacts;
            let last_commit = scan.commits.last();
            let committed = last_commit.map(|commit| (commit.info, crc32c(&commit.record)));
            let committed_required =
                last_commit.map_or_else(Vec::new, |commit| commit.required.clone());
            let committed_record =
                last_commit.map_or_else(Vec::new, |commit| commit.record.clone());
            let upload = last_commit.map(|commit| super::ReplicaUpload {
                info: commit.info,
                todo: commit.required.clone(),
                record: commit.record.clone(),
                derived: BTreeMap::new(),
                inflight: false,
            });
            replica_bytes = replica_bytes.saturating_add(stored_bytes);
            recovered_rotations = recovered_rotations.saturating_add(current_generation);
            recovered_replicas.insert(
                key,
                PassiveReplica {
                    artifacts,
                    uncommitted_artifacts,
                    committed,
                    committed_required,
                    committed_record,
                    pending_commit: None,
                    upload: None,
                    upload_queue: upload.into_iter().collect(),
                    uploaded_artifacts: BTreeSet::new(),
                    upload_done: None,
                    upload_done_record: None,
                    archive_timer_armed: false,
                    archive_urgent: false,
                    archive_timer_generation: 0,
                    unarchived_age: 0,
                    compaction: None,
                    append_inflight: false,
                    stored_bytes,
                    current_generation,
                    current_file_bytes,
                },
            );
        }

        let replica_latest_epoch = recovered_replicas.keys().fold(
            BTreeMap::<(HostId, VsetId), u64>::new(),
            |mut latest, key| {
                latest
                    .entry((key.source, key.vset))
                    .and_modify(|epoch| *epoch = (*epoch).max(key.assignment_epoch))
                    .or_insert(key.assignment_epoch);
                latest
            },
        );
        let (mut daemon, mut effects) = Daemon::new(config);
        daemon.fence_floors = fence_floors;
        daemon.replicas = recovered_replicas;
        daemon.replica_latest_epoch = replica_latest_epoch;
        daemon.counters.replica_bytes = replica_bytes;
        daemon.counters.replica_rotations = recovered_rotations;
        let mut verdicts = BTreeMap::new();
        let mut recovered_bytes: u64 = 0;
        for (vset_id, f) in found {
            // Cold-boot candidate: newest intact consistency point whose
            // every referenced leaf also decoded intact — a record with a
            // missing or damaged leaf is unusable, exactly like a torn
            // record, and recovery falls back to the next-best. A record
            // still naming its migration source is exempt: its leaves are
            // EXPECTED to be absent (they hydrate from the peer, R7.2).
            let usable = |r: &&JournalRecord| {
                r.migrated_from.is_some() || r.leaves.values().all(|ptr| f.leaves.contains_key(ptr))
            };
            let cold = f
                .records
                .iter()
                .filter(usable)
                .max_by_key(|r| (r.capture_seq, r.seq))
                .cloned();
            let Some(cold) = cold else {
                verdicts.insert(vset_id, Verdict::Unrestorable);
                // Unrestorable wreckage claims nothing and can only do
                // harm left behind: an intact stray record would read as
                // ownership to a LATER recovery, and its write-once names
                // squat the fence namespace (the fence floor guards the
                // window until these deletes land). Reclaim it — UNLESS an
                // intact handoff marker stands: an outbound source's
                // segments still serve the destination's post-copy tail
                // by raw reads, records or no records.
                if f.handoff.is_none() {
                    for name in f.names {
                        daemon.counters.blobs_deleted += 1;
                        effects.push(Effect::BlobDelete {
                            name: name.to_owned(),
                        });
                    }
                }
                continue;
            };
            // Recovery is always to the NEWEST committed recovery point
            // (R8.2); its kind decides the style (R4.3): resumed if it is a
            // whole checkpoint, cold-booted at sync consistency otherwise.
            // Resuming an *older* checkpoint would discard newer durable
            // state — and, as the simulation demonstrated, can revive a
            // state the guest's history has long since left behind. The
            // watermark guard stays as belt-and-braces (R3.8), though the
            // newest record's capture always covers every intact watermark.
            let watermark = f
                .records
                .iter()
                .map(|r| r.sync_covered_through)
                .max()
                .unwrap_or(0);
            let resume = Some(cold.clone())
                .filter(|c| matches!(c.kind, RecordKind::Checkpoint { .. }))
                .filter(|c| c.capture_seq >= watermark);

            let mut state = Vset::new(cold.config);
            state.ready = true;
            state.next_seq = f.max_seq;
            state.next_seg = f.max_seg;
            state.next_leaf = f.max_leaf;
            state.local_covered_through = watermark;

            let (verdict, chosen) = if cold.config.kind == VsetKind::Database {
                (
                    Verdict::DatabaseReady {
                        synced_through: cold.sync_covered_through,
                    },
                    cold,
                )
            } else if let Some(c) = resume {
                let RecordKind::Checkpoint { epoch, vmstate } = c.kind else {
                    unreachable!("filtered to checkpoints");
                };
                state.epoch = epoch;
                state.pinned = Some(c.clone());
                (Verdict::Resume { epoch, vmstate }, c)
            } else {
                // Disk-only recovery point: memory is invalid (R3.7) — its
                // entries are dropped and reclaimed, leaf spans included.
                let mut c = cold;
                c.overlay
                    .retain(|page, _| !c.config.is_memory(page.volume.idx));
                c.leaves.retain(|span, _| !span_is_memory(*span));
                (Verdict::ColdBoot, c)
            };
            state.database = chosen.database;
            state.database_durable = chosen.database;
            state.fence = chosen.fence;
            state.mutation_seq = chosen.capture_seq;
            // Materialize the serving map: leaves first, overlay wins. A
            // leaf not local (mid-hydration migration) parks its span.
            let mut locs = super::PageMap::new();
            for ptr in chosen.leaves.values() {
                let Some((_, leaf)) = f.leaves.get(ptr) else {
                    continue;
                };
                for &(idx, page_no, generation, loc) in &leaf.entries {
                    let page = PageId {
                        volume: VolumeId { vset: vset_id, idx },
                        page: page_no,
                    };
                    if chosen.config.contains(page) {
                        locs.insert(page, (generation, loc));
                    }
                }
            }
            for (page, entry) in &chosen.overlay {
                locs.insert(*page, *entry);
            }
            state.next_gen = locs.values().map(|(g, _)| g.0 + 1).max().unwrap_or(0);
            state.page_locs = locs;
            state.rebuild_seg_live();
            state.overlay = chosen.overlay.clone();
            state.leaf_table = chosen.leaves.clone();
            // Every intact local leaf blob is tracked (cleanup reclaims the
            // unreferenced); its own-namespace segments pin against reclaim.
            state.leaf_blobs = f
                .leaves
                .iter()
                .map(|(&ptr, (size, leaf))| {
                    let segs: BTreeSet<(u64, SegId)> = leaf
                        .entries
                        .iter()
                        .filter(|(_, _, _, loc)| loc.base == 0)
                        .map(|&(_, _, _, loc)| (loc.fence, loc.seg))
                        .collect();
                    (ptr, (*size, segs))
                })
                .collect();
            state.best = Some((chosen.capture_seq, chosen.seq));
            state.local_covered_through = watermark.max(chosen.sync_covered_through);
            // Every on-disk record name, with its watermark where intact
            // (corrupt records contribute nothing and are reclaimable).
            state.record_ws = f
                .journal_names
                .iter()
                .map(|&(fence, seq)| {
                    let w = f
                        .records
                        .iter()
                        .find(|r| r.seq == seq && r.fence == fence)
                        .map_or(0, |r| r.sync_covered_through);
                    (seq, (fence, w))
                })
                .collect();
            recovered_bytes += f.seg_names.iter().map(|&(_, _, size)| size).sum::<u64>();
            recovered_bytes += f.leaves.values().map(|(size, _)| size).sum::<u64>();
            state.seg_blobs = f.seg_names;
            // Recovery landed mid-migration-handshake (R7.2): the durable
            // accept IS ownership, but the tail still lives on the source.
            // Restore the destination side — foreign pages and missing
            // leaves keep hydrating from the peer, and the source's
            // re-offers (it never saw the accept) get re-acked instead of
            // igniting a second runner.
            state.peer_source = chosen.migrated_from;
            state.hydration_remaining_pages = state
                .page_locs
                .values()
                .filter(|(_, loc)| loc.base == 0 && loc.fence < state.fence)
                .count();
            let hydrating = chosen.migrated_from.is_some();
            if hydrating {
                state.pending_leaves = chosen
                    .leaves
                    .iter()
                    .filter(|(_, ptr)| !f.leaves.contains_key(ptr))
                    .map(|(&span, &ptr)| (span, ptr))
                    .collect();
                effects.push(Effect::SetTimer {
                    timer: crate::seam::TimerId::Hydrate(vset_id),
                    after: super::migrate::HYDRATE_TICK,
                });
            }
            state.best_record = Some(chosen);
            if let Some(to) = f.handoff {
                // Handed off before the crash (R7.2): this vset now exists
                // only to serve the destination's post-copy fetches. No
                // verdict, no cleanup (every segment may still be fetched),
                // and the guest gate (`outbound`) never opens. Re-offer:
                // the crash may have eaten the offer or its accept, and
                // without a re-send the vset would be stranded — outbound
                // here, unknown there.
                super::Daemon::recovered_outbound(&mut state, vset_id, to, &mut effects);
                daemon.vsets.insert(vset_id, state);
                continue;
            }
            // Local state may be behind the published head. Refresh
            // assignment and backup truth before opening the guest gate.
            state.ready = false;
            state.head_refreshing = true;
            state.pending_verdict = Some(verdict);
            daemon.vsets.insert(vset_id, state);
            if hydrating {
                daemon.request_pending_leaves(vset_id, &mut effects);
            }
            daemon.cleanup(vset_id, &mut effects);
            let io = daemon.io();
            daemon
                .pending
                .insert(io, super::Pending::HeadRefresh { vset: vset_id });
            effects.push(Effect::StoreGet {
                io,
                key: crate::layout::head_key(vset_id),
            });
        }
        daemon.local_bytes = recovered_bytes;
        let replica_keys: Vec<_> = daemon.replicas.keys().copied().collect();
        for key in replica_keys {
            if daemon.replica_authorized(key.source, key.vset, key.assignment_epoch) {
                if let Some(&(generation, len)) = replica_truncations.get(&key) {
                    let io = daemon.io();
                    daemon
                        .pending
                        .insert(io, super::Pending::ReplicaTailTruncate { key, generation });
                    daemon
                        .replicas
                        .get_mut(&key)
                        .expect("recovered replica")
                        .append_inflight = true;
                    effects.push(Effect::ReplicaTruncate {
                        io,
                        source: key.source,
                        vset: key.vset,
                        assignment_epoch: key.assignment_epoch,
                        generation,
                        len,
                    });
                } else {
                    daemon.replica_archive_schedule(key, &mut effects);
                }
            } else {
                daemon.replicas.remove(&key);
            }
        }
        (daemon, verdicts, effects)
    }
}
