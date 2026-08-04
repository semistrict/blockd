//! Recovery (R8.2): rebuild a daemon from durable state alone, with an
//! explicit per-vset verdict.

use std::collections::BTreeMap;

use super::{Daemon, DaemonConfig, Vset};
use crate::journal::{JournalRecord, RecordKind};
use crate::layout::{self, BlobName};
use crate::seam::{Effect, Verdict};
use crate::types::{JournalSeq, SegId, VsetId};

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
        struct Found {
            records: Vec<JournalRecord>,
            journal_names: Vec<(u64, JournalSeq)>,
            seg_names: Vec<(u64, SegId, u64)>,
            max_seq: u64,
            max_seg: u64,
            handoff: Option<crate::types::HostId>,
        }
        let mut found: BTreeMap<VsetId, Found> = BTreeMap::new();
        for (name, bytes) in blobs {
            let Some(parsed) = layout::parse_blob(name) else {
                continue;
            };
            let vset = match parsed {
                BlobName::Journal { vset, .. }
                | BlobName::Segment { vset, .. }
                | BlobName::Handoff { vset } => vset,
            };
            let f = found.entry(vset).or_insert_with(|| Found {
                records: Vec::new(),
                journal_names: Vec::new(),
                seg_names: Vec::new(),
                max_seq: 0,
                max_seg: 0,
                handoff: None,
            });
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
                    f.seg_names.push((fence, seg, bytes.len() as u64));
                    f.max_seg = f.max_seg.max(seg.0 + 1);
                }
                BlobName::Handoff { .. } => {
                    // An intact marker means the handoff committed (R7.2);
                    // a torn one means it never did — recover normally.
                    if let Ok(h) = super::migrate::Handoff::decode(vset, bytes) {
                        f.handoff = Some(h.to);
                    }
                }
            }
        }

        let (mut daemon, mut effects) = Daemon::new(config);
        let mut verdicts = BTreeMap::new();
        let mut recovered_bytes: u64 = 0;
        for (vset_id, f) in found {
            // Cold-boot candidate: newest intact consistency point.
            let cold = f
                .records
                .iter()
                .max_by_key(|r| (r.capture_seq, r.seq))
                .cloned();
            let Some(cold) = cold else {
                verdicts.insert(vset_id, Verdict::Unrestorable);
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
                .map(|r| r.synced_through)
                .max()
                .unwrap_or(0);
            let resume = Some(cold.clone())
                .filter(|c| matches!(c.kind, RecordKind::Checkpoint { .. }))
                .filter(|c| c.capture_seq >= watermark);

            let mut state = Vset::new(cold.config);
            state.ready = true;
            state.next_seq = f.max_seq;
            state.next_seg = f.max_seg;
            state.durable_watermark = watermark;

            let (verdict, chosen) = if let Some(c) = resume {
                let RecordKind::Checkpoint { epoch, vmstate } = c.kind else {
                    unreachable!("filtered to checkpoints");
                };
                state.epoch = epoch;
                state.pinned = Some(c.clone());
                (Verdict::Resume { epoch, vmstate }, c)
            } else {
                // Disk-only recovery point: memory is invalid (R3.7) — its
                // pages are dropped and reclaimed.
                let mut c = cold;
                c.pages.retain(|page, _| !page.volume.idx.is_memory());
                (Verdict::ColdBoot, c)
            };
            state.fence = chosen.fence;
            state.mutation_seq = chosen.capture_seq;
            state.next_gen = chosen
                .pages
                .values()
                .map(|(g, _)| g.0 + 1)
                .max()
                .unwrap_or(0);
            state.page_locs = chosen.pages.clone();
            state.best = Some((chosen.capture_seq, chosen.seq));
            state.best_pages = chosen.pages.clone();
            state.durable_watermark = watermark.max(chosen.synced_through);
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
                        .map_or(0, |r| r.synced_through);
                    (seq, (fence, w))
                })
                .collect();
            recovered_bytes += f.seg_names.iter().map(|&(_, _, size)| size).sum::<u64>();
            state.seg_blobs = f.seg_names;
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
            let backed = state.config.backed_up;
            if backed {
                // A backed-up vset may not serve yet: journal damage can
                // leave local state BEHIND the backup, and serving it would
                // roll back acknowledged syncs (R3.8). The head refresh
                // resolves the verdict — or fences us (R6.4).
                state.ready = false;
                state.head_refreshing = true;
                state.pending_verdict = Some(verdict);
            }
            daemon.vsets.insert(vset_id, state);
            daemon.cleanup(vset_id, &mut effects);
            if backed {
                let io = daemon.io();
                daemon
                    .pending
                    .insert(io, super::Pending::HeadRefresh { vset: vset_id });
                effects.push(Effect::StoreGet {
                    io,
                    key: crate::layout::head_key(vset_id),
                });
            } else {
                verdicts.insert(vset_id, verdict);
            }
        }
        daemon.local_bytes = recovered_bytes;
        (daemon, verdicts, effects)
    }
}
