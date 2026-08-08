//! Lineage (R5): bases and forks.
//!
//! A **base keep** publishes the vset's pinned checkpoint into the base's
//! own store namespace: each referenced own-namespace segment is copied
//! (verbatim, R8.4) to `b/<base>/s/…`, then the record — with its locations
//! rewritten to base origin — lands at `b/<base>/rec`. The base is
//! immutable, forkable from any host, and alive until explicit delete
//! (R5.2, R4.5). Inherited base-origin locations are kept as-is: a base of
//! a fork references its ancestor's segments directly, so sharing follows
//! lineage with no copies (R5.3) and no chains to walk at fault time.
//!
//! A **fork** is O(1) metadata (R5.1): read the base record, re-key its
//! pages under the new vset, write the fork's first local journal record.
//! Every untouched page keeps pointing into the base namespace; the first
//! write diverges it into the fork's own segments — the fork pays only for
//! what it changes (R5.3).

use std::collections::BTreeMap;

use super::{Daemon, Pending, StoreCopyArtifact, Vset};
use crate::journal::{JournalRecord, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::{LeafPtr, MapLeaf, span_is_memory};
use crate::protocol::{AdminReply, ReqId, StoreFault, Verdict};
use crate::seam::Effect;
use crate::segment::scan_segment;
use crate::types::{PageId, SegId, VolumeId, VsetId};

/// One in-flight base keep: segments and leaves to copy, then the record.
#[derive(Debug)]
pub(super) struct BaseKeep {
    pub req: ReqId,
    pub base: u64,
    pub record: JournalRecord,
    pub segs_todo: Vec<(u64, SegId)>,
    /// Own-namespace leaves to copy (content re-homed to base origin).
    pub leaves_todo: Vec<(u64, u64)>,
}

impl Daemon {
    /// Explicit base delete (R4.5): removing the record is the unroot — the
    /// GC's next sweep reclaims whatever segments nothing else references
    /// (R9.3). Fire-and-forget like every other store cleanup.
    pub(super) fn delete_base(req: ReqId, base: u64, out: &mut Vec<Effect>) {
        out.push(Effect::StoreDelete {
            key: layout::base_record_key(base),
        });
        out.push(Effect::Admin(AdminReply::BaseDeleted { req, base }));
    }

    pub(super) fn keep_base(&mut self, req: ReqId, vset: VsetId, base: u64, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        // Only a pinned whole checkpoint (or disk-only point) can be kept.
        if !state.ready || state.keep.is_some() {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        }
        let Some(record) = state.pinned.clone() else {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        // Own-namespace references to copy: the overlay's segments plus
        // everything the record's own leaves hold (a keep on a vset whose
        // leaves are still hydrating fails cleanly at the local read).
        let closure = state.own_namespace_closure(&record);
        let mut segs_todo = closure.segments;
        segs_todo.sort_unstable();
        segs_todo.dedup();
        let leaves_todo = closure.leaves;
        state.keep = Some(BaseKeep {
            req,
            base,
            record,
            segs_todo,
            leaves_todo,
        });
        self.keep_step(vset, out);
    }

    pub(super) fn keep_step(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(keep) = &state.keep else {
            return;
        };
        if let Some(artifact) = StoreCopyArtifact::next(&keep.segs_todo, &keep.leaves_todo) {
            let io = self.io();
            let pending = match artifact {
                StoreCopyArtifact::Segment { fence, seg } => {
                    Pending::KeepSegRead { vset, fence, seg }
                }
                StoreCopyArtifact::Leaf { fence, id } => Pending::KeepLeafRead { vset, fence, id },
            };
            self.pending.insert(io, pending);
            out.push(Effect::BlobRead {
                io,
                name: artifact.blob_name(vset),
            });
            return;
        }
        // All segments and leaves copied: publish the record with
        // own-namespace locations rewritten to the base's (its leaf
        // pointers keep base 0 — within the base record, "own" IS the base
        // namespace, and forks rewrite them on adoption).
        let base = keep.base;
        let req = keep.req;
        let mut record = keep.record.clone();
        for (_, loc) in record.overlay.values_mut() {
            if loc.base == 0 {
                loc.base = base;
            }
        }
        let bytes = record.encode(VsetId(base));
        let io = self.io();
        self.pending
            .insert(io, Pending::KeepRecordPut { vset, base, req });
        out.push(Effect::StorePut {
            io,
            key: layout::base_record_key(base),
            bytes,
        });
    }

    pub(super) fn keep_seg_read_done(
        &mut self,
        vset: VsetId,
        fence: u64,
        seg: SegId,
        bytes: Option<Vec<u8>>,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(keep) = &state.keep else {
            return;
        };
        let intact = bytes.as_ref().is_some_and(|b| {
            scan_segment(b).is_ok_and(|(v, f, s, _)| v == vset && f == fence && s == seg)
        });
        let Some(blob) = bytes.filter(|_| intact) else {
            // Pinned segments are never reclaimed, so this is real damage:
            // the keep fails loudly (R8.1).
            let req = keep.req;
            state.keep = None;
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        let base = keep.base;
        let io = self.io();
        self.pending
            .insert(io, Pending::KeepSegPut { vset, fence, seg });
        out.push(Effect::StorePut {
            io,
            key: layout::base_segment_key(base, fence, seg),
            bytes: blob,
        });
    }

    /// A leaf headed for the base: verify, re-home its own-namespace
    /// locations to base origin, re-encode under the base's identity.
    pub(super) fn keep_leaf_read_done(
        &mut self,
        vset: VsetId,
        fence: u64,
        id: u64,
        bytes: Option<&[u8]>,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let Some(keep) = &state.keep else {
            return;
        };
        let leaf = bytes.and_then(|b| MapLeaf::decode(vset, fence, id, b).ok());
        let Some(mut leaf) = leaf else {
            // Kept leaves are never reclaimed, so this is real damage (or a
            // still-hydrating map): the keep fails loudly (R8.1).
            let req = keep.req;
            state.keep = None;
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        let base = keep.base;
        for (_, _, _, loc) in &mut leaf.entries {
            if loc.base == 0 {
                loc.base = base;
            }
        }
        let blob = leaf.encode(VsetId(base), fence, id);
        let io = self.io();
        self.pending
            .insert(io, Pending::KeepLeafPut { vset, fence, id });
        out.push(Effect::StorePut {
            io,
            key: layout::base_leaf_key(base, fence, id),
            bytes: blob,
        });
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn keep_put_done(
        &mut self,
        pending: Pending,
        result: Result<u64, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        match pending {
            Pending::KeepSegPut { vset, fence, seg } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                if result.is_ok() {
                    if let Some(keep) = &mut state.keep {
                        keep.segs_todo.retain(|&k| k != (fence, seg));
                    }
                    self.keep_step(vset, out);
                } else {
                    // Outage: retry from scratch on the backup tick.
                    state.keep = None;
                    self.backup_backoff(vset, out);
                }
            }
            Pending::KeepLeafPut { vset, fence, id } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                if result.is_ok() {
                    if let Some(keep) = &mut state.keep {
                        keep.leaves_todo.retain(|&k| k != (fence, id));
                    }
                    self.keep_step(vset, out);
                } else {
                    state.keep = None;
                    self.backup_backoff(vset, out);
                }
            }
            Pending::KeepRecordPut { vset, base, req } => {
                let Some(state) = self.vsets.get_mut(&vset) else {
                    return;
                };
                state.keep = None;
                if result.is_ok() {
                    out.push(Effect::Admin(AdminReply::BaseKept { req, base }));
                } else {
                    out.push(Effect::Admin(AdminReply::AdminFailed { req }));
                }
            }
            _ => out.push(Effect::Abort {
                reason: "keep completion for non-keep io",
            }),
        }
    }

    // ── forks ───────────────────────────────────────────────────────────

    /// Fetch the base record for a forked creation (after the head claim,
    /// before adopting the protected fork).
    pub(super) fn fork_fetch_base(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some(state) = self.vsets.get(&vset) else {
            return;
        };
        let Some(base) = state.fork_from else {
            return;
        };
        let io = self.io();
        self.pending.insert(io, Pending::ForkBaseGet { vset, base });
        out.push(Effect::StoreGet {
            io,
            key: layout::base_record_key(base),
        });
    }

    pub(super) fn fork_base_done(
        &mut self,
        vset: VsetId,
        base: u64,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            return;
        };
        let fail = |state: &mut Vset, out: &mut Vec<Effect>| {
            if let Some(req) = state.create_req.take() {
                out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            }
        };
        let Ok(Some((_, bytes))) = result else {
            fail(state, out);
            self.vsets.remove(&vset);
            return;
        };
        let Ok(record) = JournalRecord::decode(VsetId(base), &bytes) else {
            fail(state, out);
            self.vsets.remove(&vset);
            return;
        };
        // O(1) fork (R5.1): re-key the base's overlay under the new vset
        // and REFERENCE the base's leaf blobs in place — no map copy of
        // any size (R5.3). The leaves hydrate lazily from the store
        // (reads only); divergence goes to
        // the overlay and rolls into the fork's own leaves.
        let mut overlay: BTreeMap<PageId, _> = BTreeMap::new();
        let whole = matches!(record.kind, RecordKind::Checkpoint { .. });
        for (page, entry) in &record.overlay {
            if !whole && record.config.is_memory(page.volume.idx) {
                continue;
            }
            let rekeyed = PageId {
                volume: VolumeId {
                    vset,
                    idx: page.volume.idx,
                },
                page: page.page,
            };
            overlay.insert(rekeyed, *entry);
        }
        let mut leaves: BTreeMap<u32, LeafPtr> = record
            .leaves
            .iter()
            .map(|(&span, &ptr)| {
                // "Own" in a base record means the base's namespace.
                let base_of = if ptr.base == 0 { base } else { ptr.base };
                (
                    span,
                    LeafPtr {
                        base: base_of,
                        fence: ptr.fence,
                        id: ptr.id,
                    },
                )
            })
            .collect();
        if !whole && record.config.kind == VsetKind::Compute {
            leaves.retain(|span, _| !span_is_memory(*span));
        }
        let verdict = match (record.config.kind, record.kind) {
            (VsetKind::Database, RecordKind::Commit) => Verdict::DatabaseReady {
                synced_through: record.sync_covered_through,
            },
            (VsetKind::Compute, RecordKind::Checkpoint { epoch: _, vmstate }) => {
                // Forks of a whole base resume (R5.2) — at their own epoch 0.
                state.fork_vmstate = Some(vmstate);
                Verdict::Resume {
                    epoch: crate::types::Epoch(0),
                    vmstate,
                }
            }
            (VsetKind::Compute, RecordKind::Commit) => Verdict::ColdBoot,
            (VsetKind::Database, RecordKind::Checkpoint { .. }) => {
                unreachable!("database records cannot carry vmstate")
            }
        };
        state.database = record.database;
        state.database_durable = record.database;
        state.fork_verdict = Some(verdict);
        state.mutation_seq = record.capture_seq;
        state.local_covered_through = 0;
        state.sync_ack_through = 0;
        state.next_gen = overlay.values().map(|(g, _)| g.0 + 1).max().unwrap_or(0);
        state.page_locs = overlay.clone();
        state.rebuild_seg_live();
        state.overlay = overlay;
        state.leaf_table = leaves.clone();
        state.pending_leaves = leaves;
        self.request_pending_leaves(vset, out);
        // The fork's first local record makes the lineage durable.
        self.start_record_only_capture(vset, out);
    }
}
