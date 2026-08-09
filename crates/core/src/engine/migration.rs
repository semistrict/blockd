use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::channel::oneshot;
use blockd_exec::{TaskSet, delay, timeout, yield_now};

use super::backup::publish_latest;
use super::capture::{capture_migration, shard_map, write_record_copies};
use super::replica::publish_replica_head;
use super::state::CommitFlagLease;
use super::{SharedHost, VsetState, replica_message};
use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::head::{HeadRecord, ManifestPtr};
use crate::journal::{DurabilityMode, JournalRecord, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::{LeafPtr, MapLeaf};
use crate::protocol::{AdminReply, PeerMsg, ReqId, Verdict};
use crate::segment::{SegmentBatchBuilder, open_entry};
use crate::types::{HostId, PageId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

const MAGIC_HANDOFF: u32 = u32::from_le_bytes(*b"BHF1");
const OFFER_RETRY: u64 = 5_000_000;
const PEER_RETRY: u64 = 50_000_000;
const HYDRATE_BATCH: usize = 64;

struct Handoff {
    vset: VsetId,
    to: HostId,
}

struct InboundLease {
    state: SharedHost,
    vset: VsetId,
}

struct MigrationLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
    active: bool,
}

impl MigrationLease {
    fn new(state: &SharedHost, vset: VsetId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            active: true,
        }
    }

    fn commit(mut self) {
        self.active = false;
    }
}

impl Drop for MigrationLease {
    fn drop(&mut self) {
        if self.active
            && let Some(vset_state) = self
                .state
                .borrow_mut()
                .vsets
                .get_mut(&self.vset)
                .filter(|vset_state| vset_state.incarnation == self.incarnation)
        {
            vset_state.migration_running = false;
        }
    }
}

impl Drop for InboundLease {
    fn drop(&mut self) {
        self.state
            .borrow_mut()
            .inbound_migrations
            .remove(&self.vset);
    }
}

impl Handoff {
    fn encode(&self) -> Vec<u8> {
        let mut encoder = Enc::new();
        encoder.u16(1);
        encoder.u64(self.vset.0);
        encoder.u16(self.to.0);
        seal_frame(MAGIC_HANDOFF, &encoder.finish())
    }

    fn decode(vset: VsetId, bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_HANDOFF, bytes)?;
        let mut decoder = Dec::new(payload);
        if decoder.u16()? != 1 || decoder.u64()? != vset.0 {
            return Err(DecodeError);
        }
        let to = HostId(decoder.u16()?);
        decoder.finish()?;
        Ok(Self { vset, to })
    }
}

pub(super) fn decode_handoff(vset: VsetId, bytes: &[u8]) -> Option<HostId> {
    Handoff::decode(vset, bytes).ok().map(|handoff| handoff.to)
}

pub async fn migrate_out<W>(state: SharedHost, world: Rc<W>, req: ReqId, vset: VsetId, to: HostId)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let incarnation = {
        let mut host = state.borrow_mut();
        host.vsets.get_mut(&vset).and_then(|vset_state| {
            let allowed = vset_state.ready
                && (vset_state.config.kind == VsetKind::Database
                    || !vset_state.config.durability.uses_store())
                && vset_state.peer_source.is_none()
                && vset_state.outbound.is_none()
                && !vset_state.migration_running
                && (vset_state.config.kind != VsetKind::Database
                    || (vset_state.database_runtime.phase
                        == super::state::AttachmentPhase::Detached
                        && vset_state.database_runtime.active.is_none()
                        && vset_state.database_runtime.handles.is_empty()));
            if allowed {
                vset_state.migration_running = true;
            }
            allowed.then_some(vset_state.incarnation)
        })
    };
    let Some(incarnation) = incarnation else {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    };
    let lease = MigrationLease::new(&state, vset, incarnation);
    let kind = state.borrow().vsets[&vset].config.kind;
    let record = if kind == VsetKind::Compute {
        let Some(record) = capture_migration(Rc::clone(&state), Rc::clone(&world), vset).await
        else {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        };
        record
    } else {
        let Some(record) = state.borrow().vsets[&vset].best_record.clone() else {
            AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            return;
        };
        record
    };
    if record.config.durability.uses_store() {
        loop {
            match record.config.durability {
                DurabilityMode::Backup => {
                    publish_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
                }
                DurabilityMode::PeerStashed => {
                    publish_replica_head(Rc::clone(&state), Rc::clone(&world), vset).await;
                }
                DurabilityMode::Local => unreachable!(),
            }
            let published = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
                vset_state.backed.is_some_and(|pointer| {
                    (pointer.capture_seq, pointer.seq) == (record.capture_seq, record.seq)
                })
            });
            if published {
                break;
            }
            let retry = state.borrow().config.backup_retry;
            delay(retry).await;
        }
    }
    let handoff = Handoff { vset, to };
    let handoff_name = layout::handoff_blob(vset);
    let handoff_bytes = handoff.encode();
    if !state
        .borrow_mut()
        .try_reserve_blob(handoff_name.clone(), handoff_bytes.len() as u64)
    {
        AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
        return;
    }
    if Blobs::write(world.as_ref(), handoff_name.clone(), handoff_bytes.clone())
        .await
        .is_err()
    {
        AdminIo::abort(world.as_ref(), "migration handoff write failed").await;
        return;
    }
    {
        let mut host = state.borrow_mut();
        host.record_blob(handoff_name, handoff_bytes.len() as u64);
        let Some(vset_state) = host.vsets.get_mut(&vset) else {
            return;
        };
        vset_state.ready = false;
        vset_state.outbound = Some(to);
    }
    offer_until_accepted(&state, world.as_ref(), vset, to, record.encode(vset)).await;
    lease.commit();
    AdminIo::reply_admin(world.as_ref(), AdminReply::MigratedOut { req, vset }).await;
}

async fn offer_until_accepted<W: Peers>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    to: HostId,
    bytes: Vec<u8>,
) {
    loop {
        let (wake, wait) = oneshot();
        state.borrow_mut().migration_accepts.insert(vset, wake);
        Peers::send(
            world,
            to,
            PeerMsg::MigrateOffer {
                vset,
                record: bytes.clone(),
            },
        )
        .await;
        if let Ok(Ok(())) = timeout(OFFER_RETRY, wait).await {
            break;
        }
        state.borrow_mut().migration_accepts.remove(&vset);
        if state
            .borrow()
            .vsets
            .get(&vset)
            .is_none_or(|vset_state| vset_state.migration_accepted)
        {
            break;
        }
    }
}

pub async fn reoffer_outbound<W: Peers>(state: SharedHost, world: Rc<W>, vset: VsetId) {
    let Some((to, record)) = state.borrow().vsets.get(&vset).and_then(|vset_state| {
        Some((
            vset_state.outbound?,
            vset_state.best_record.as_ref()?.encode(vset),
        ))
    }) else {
        return;
    };
    offer_until_accepted(&state, world.as_ref(), vset, to, record).await;
}

#[allow(clippy::too_many_lines)]
pub async fn peer_source<W>(state: SharedHost, world: Rc<W>)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let mut handlers = TaskSet::new();
    while let Some((from, message)) = Peers::recv(world.as_ref()).await {
        handlers.reap();
        match message {
            PeerMsg::MigrateOffer { vset, record } => {
                handlers.spawn(migrate_in(
                    Rc::clone(&state),
                    Rc::clone(&world),
                    from,
                    vset,
                    record,
                ));
            }
            PeerMsg::MigrateAccept { vset } => {
                let accepted = state
                    .borrow()
                    .vsets
                    .get(&vset)
                    .is_some_and(|vset_state| vset_state.outbound == Some(from));
                if accepted {
                    if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                        vset_state.migration_accepted = true;
                    }
                    if let Some(waiter) = state.borrow_mut().migration_accepts.remove(&vset) {
                        let _ = waiter.send(());
                    }
                }
            }
            PeerMsg::FetchRange {
                io,
                vset,
                fence,
                seg,
                offset,
                len,
            } => {
                let authorized = state
                    .borrow()
                    .vsets
                    .get(&vset)
                    .is_some_and(|vset_state| vset_state.outbound == Some(from));
                if authorized && let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                    vset_state.wedge.served += 1;
                }
                let bytes = if authorized {
                    Blobs::read_range(
                        world.as_ref(),
                        &layout::segment_blob(vset, fence, seg),
                        u64::from(offset),
                        u64::from(len),
                    )
                    .await
                    .ok()
                    .flatten()
                } else {
                    None
                };
                Peers::send(world.as_ref(), from, PeerMsg::Page { io, bytes }).await;
            }
            PeerMsg::Page { io, bytes } => {
                let expected = state
                    .borrow()
                    .peer_pages
                    .get(&io)
                    .is_some_and(|(expected, _)| *expected == from);
                if expected && let Some((_, waiter)) = state.borrow_mut().peer_pages.remove(&io) {
                    let _ = waiter.send(bytes);
                }
            }
            PeerMsg::FetchLeaf {
                io,
                vset,
                base,
                fence,
                id,
            } => {
                let authorized = state
                    .borrow()
                    .vsets
                    .get(&vset)
                    .is_some_and(|vset_state| vset_state.outbound == Some(from));
                if authorized && let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                    vset_state.wedge.served += 1;
                }
                let name = if base == 0 {
                    layout::leaf_blob(vset, fence, id)
                } else {
                    layout::base_leaf_blob(vset, base, fence, id)
                };
                let bytes = if authorized {
                    Blobs::read(world.as_ref(), &name).await.ok().flatten()
                } else {
                    None
                };
                Peers::send(world.as_ref(), from, PeerMsg::Leaf { io, bytes }).await;
            }
            PeerMsg::Leaf { io, bytes } => {
                let expected = state
                    .borrow()
                    .peer_leaves
                    .get(&io)
                    .is_some_and(|(expected, _)| *expected == from);
                if expected && let Some((_, waiter)) = state.borrow_mut().peer_leaves.remove(&io) {
                    let _ = waiter.send(bytes);
                }
            }
            PeerMsg::Released { vset } => {
                release_source(&state, world.as_ref(), from, vset).await;
            }
            PeerMsg::ReleasedAck { vset } => {
                if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                    vset_state.peer_source = None;
                }
            }
            message @ (PeerMsg::ReplicaPut { .. }
            | PeerMsg::ReplicaPutAck { .. }
            | PeerMsg::ReplicaCommit { .. }
            | PeerMsg::ReplicaCommitAck { .. }
            | PeerMsg::ReplicaStatus { .. }
            | PeerMsg::ReplicaStatusReply { .. }
            | PeerMsg::ReplicaUploadDone { .. }
            | PeerMsg::ReplicaRelease { .. }
            | PeerMsg::ReplicaReleaseAck { .. }) => {
                replica_message(Rc::clone(&state), world.as_ref(), from, message).await;
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn migrate_in<W>(state: SharedHost, world: Rc<W>, from: HostId, vset: VsetId, bytes: Vec<u8>)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let existing = state
        .borrow()
        .vsets
        .get(&vset)
        .map(|existing| existing.peer_source == Some(from) && existing.ready);
    if let Some(ready) = existing {
        if ready {
            Peers::send(world.as_ref(), from, PeerMsg::MigrateAccept { vset }).await;
        }
        return;
    }
    if !state.borrow_mut().inbound_migrations.insert(vset) {
        return;
    }
    let _lease = InboundLease {
        state: Rc::clone(&state),
        vset,
    };
    let Ok(offered) = JournalRecord::decode(vset, &bytes) else {
        return;
    };
    let verdict = match (offered.config.kind, offered.kind) {
        (VsetKind::Compute, RecordKind::Checkpoint { epoch, vmstate }) => {
            Verdict::Resume { epoch, vmstate }
        }
        (VsetKind::Database, RecordKind::Commit) => Verdict::DatabaseReady {
            synced_through: offered.sync_covered_through,
        },
        _ => return,
    };
    let mut leaves = BTreeMap::new();
    for (&span, &pointer) in &offered.leaves {
        let Some(bytes) = peer_fetch_leaf(&state, world.as_ref(), from, vset, pointer).await else {
            return;
        };
        let owner = if pointer.base == 0 {
            vset
        } else {
            VsetId(pointer.base)
        };
        let Ok(leaf) = MapLeaf::decode(owner, pointer.fence, pointer.id, &bytes) else {
            return;
        };
        if leaf.span != span {
            return;
        }
        let name = if pointer.base == 0 {
            layout::leaf_blob(vset, pointer.fence, pointer.id)
        } else {
            layout::base_leaf_blob(vset, pointer.base, pointer.fence, pointer.id)
        };
        if !state
            .borrow_mut()
            .try_reserve_blob(name.clone(), bytes.len() as u64)
        {
            return;
        }
        if Blobs::write(world.as_ref(), name.clone(), bytes.clone())
            .await
            .is_err()
        {
            return;
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
        leaves.insert(pointer, (bytes.len() as u64, leaf));
    }
    let incarnation = state.borrow_mut().allocate_incarnation();
    let fence = offered
        .fence
        .checked_add(1)
        .expect("migration fence overflow");
    let mut record = offered.clone();
    record.seq = crate::types::JournalSeq(0);
    record.fence = fence;
    record.migrated_from = Some(from);
    if !write_record_copies(&state, world.as_ref(), vset, &record).await {
        AdminIo::abort(world.as_ref(), "inbound migration journal write failed").await;
        return;
    }
    {
        let mut host = state.borrow_mut();
        if host.vsets.contains_key(&vset) {
            return;
        }
        let mut incoming = VsetState::fresh(record.config, incarnation);
        incoming.ready = !record.config.durability.uses_store();
        incoming.peer_source = Some(from);
        incoming.fence = fence;
        if let Verdict::Resume { epoch, .. } = verdict {
            incoming.epoch = epoch;
        }
        incoming.database = record.database;
        incoming.mutation_seq = record.capture_seq;
        incoming.local_covered_through = record.sync_covered_through;
        incoming.sync_ack_through = record.sync_covered_through;
        incoming.next_seq = 1;
        incoming.overlay = record.overlay.clone();
        incoming.leaf_table = record.leaves.clone();
        incoming.hydrated_spans = record.leaves.keys().copied().collect();
        incoming.page_locs = materialize(vset, &record, &leaves);
        incoming.next_gen = incoming
            .page_locs
            .values()
            .map(|(generation, _)| generation.0 + 1)
            .max()
            .unwrap_or(0);
        incoming.leaf_blobs = leaves
            .iter()
            .map(|(&pointer, (size, leaf))| {
                let segments = leaf
                    .entries
                    .iter()
                    .filter(|(_, _, _, location)| location.base == 0)
                    .map(|(_, _, _, location)| (location.fence, location.seg))
                    .collect::<BTreeSet<_>>();
                (pointer, (*size, segments))
            })
            .collect();
        incoming.best_record = Some(record.clone());
        if matches!(verdict, Verdict::Resume { .. }) {
            incoming.pinned = Some(record.clone());
        }
        incoming
            .record_writes
            .insert(record.seq, (fence, record.sync_covered_through));
        host.vsets.insert(vset, incoming);
        host.counters.records_written += 1;
    }
    if record.config.durability.uses_store()
        && !claim_migrated_database_head(&state, world.as_ref(), from, vset, incarnation, &offered)
            .await
    {
        state.borrow_mut().vsets.remove(&vset);
        return;
    }
    if matches!(verdict, Verdict::Resume { .. }) {
        GuestMem::resume(world.as_ref(), vset).await;
    }
    AdminIo::reply_admin(world.as_ref(), AdminReply::VsetMigratedIn { vset, verdict }).await;
    Peers::send(world.as_ref(), from, PeerMsg::MigrateAccept { vset }).await;
}

#[allow(clippy::too_many_lines)]
async fn claim_migrated_database_head<W>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    incarnation: u64,
    offered: &JournalRecord,
) -> bool
where
    W: Blobs + Store + AdminIo,
{
    let local = state.borrow().config.host;
    let retry = state.borrow().config.backup_retry;
    let expected_manifest = Some(ManifestPtr {
        fence: offered.fence,
        seq: offered.seq,
        capture_seq: offered.capture_seq,
    });
    let (claim_fence, stash, retired_stashes) = loop {
        let current = match Store::get(world, &layout::head_key(vset)).await {
            Ok(Some(current)) => current,
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
                continue;
            }
            Ok(None)
            | Err(
                StoreError::TooLarge
                | StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. }),
            ) => return false,
        };
        let Ok(head) = HeadRecord::decode(vset, &current.1) else {
            return false;
        };
        if head.holder == local && head.manifest == expected_manifest {
            if head.fence == 0 {
                break (current.0, head.stash, head.retired_stashes);
            }
            break (head.fence, head.stash, head.retired_stashes);
        }
        if head.holder != source || head.manifest != expected_manifest {
            return false;
        }
        let claim = HeadRecord {
            vset,
            holder: local,
            fence: 0,
            manifest: head.manifest,
            stash: head.stash,
            retired_stashes: head.retired_stashes.clone(),
        };
        match Store::put_cas(
            world,
            layout::head_key(vset),
            Some(current.0),
            claim.encode(),
        )
        .await
        {
            Ok(version) => break (version, head.stash, head.retired_stashes),
            Err(StoreError::Fault(
                crate::protocol::StoreFault::Unavailable
                | crate::protocol::StoreFault::CasConflict { .. },
            )) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::TooLarge) => return false,
        }
    };

    let mut claimed = offered.clone();
    claimed.fence = claim_fence;
    claimed.seq = crate::types::JournalSeq(
        offered
            .seq
            .0
            .checked_add(1)
            .expect("migration journal sequence overflow"),
    );
    claimed.migrated_from = Some(source);
    if !write_record_copies(state, world, vset, &claimed).await {
        AdminIo::abort(world, "claimed migration journal write failed").await;
        return false;
    }
    {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
        else {
            return false;
        };
        vset_state.fence = claim_fence;
        vset_state.head_version = Some(claim_fence);
        vset_state.backed = expected_manifest;
        vset_state.stash_assignment = stash;
        vset_state.retired_stashes.clone_from(&retired_stashes);
        vset_state.next_seq = claimed.seq.0 + 1;
        vset_state.best_record = Some(claimed.clone());
        vset_state
            .record_writes
            .insert(claimed.seq, (claimed.fence, claimed.sync_covered_through));
        host.counters.records_written += 1;
    }

    let finalized = HeadRecord {
        vset,
        holder: local,
        fence: claim_fence,
        manifest: expected_manifest,
        stash,
        retired_stashes,
    };
    let head_version = loop {
        let current = match Store::get(world, &layout::head_key(vset)).await {
            Ok(Some(current)) => current,
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
                continue;
            }
            Ok(None)
            | Err(
                StoreError::TooLarge
                | StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. }),
            ) => return false,
        };
        let Ok(head) = HeadRecord::decode(vset, &current.1) else {
            return false;
        };
        if head == finalized {
            break current.0;
        }
        if head.holder != local
            || head.fence != 0
            || head.manifest != expected_manifest
            || head.stash != stash
        {
            return false;
        }
        match Store::put_cas(
            world,
            layout::head_key(vset),
            Some(current.0),
            finalized.encode(),
        )
        .await
        {
            Ok(version) => break version,
            Err(StoreError::Fault(
                crate::protocol::StoreFault::Unavailable
                | crate::protocol::StoreFault::CasConflict { .. },
            )) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::TooLarge) => return false,
        }
    };
    let mut host = state.borrow_mut();
    let Some(vset_state) = host
        .vsets
        .get_mut(&vset)
        .filter(|vset_state| vset_state.incarnation == incarnation)
    else {
        return false;
    };
    vset_state.head_version = Some(head_version);
    vset_state.ready = true;
    true
}

pub async fn peer_fetch_page<W: Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    location: crate::segment::PageLoc,
) -> Option<Vec<u8>> {
    loop {
        let (send, receive) = oneshot();
        let io = {
            let mut host = state.borrow_mut();
            let io = host.allocate_peer_request();
            host.peer_pages.insert(io, (source, send));
            io
        };
        Peers::send(
            world,
            source,
            PeerMsg::FetchRange {
                io,
                vset,
                fence: location.fence,
                seg: location.seg,
                offset: location.offset,
                len: location.len,
            },
        )
        .await;
        match timeout(PEER_RETRY, receive).await {
            Ok(Ok(bytes)) => return bytes,
            Ok(Err(_)) | Err(_) => {
                state.borrow_mut().peer_pages.remove(&io);
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
pub async fn hydrate_tail<W>(state: SharedHost, world: Rc<W>, vset: VsetId)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let Some((incarnation, source, fence, first_segment, pages)) = ({
        let mut host = state.borrow_mut();
        host.vsets.get_mut(&vset).and_then(|vset_state| {
            let source = vset_state.peer_source?;
            if !vset_state.ready || vset_state.commit_running {
                return None;
            }
            let pages = vset_state
                .page_locs
                .iter()
                .filter(|(_, (_, location))| {
                    location.base == 0 && location.fence < vset_state.fence
                })
                .take(HYDRATE_BATCH)
                .map(|(&page, &entry)| (page, entry))
                .collect::<Vec<_>>();
            if pages.is_empty() {
                return Some((
                    vset_state.incarnation,
                    source,
                    vset_state.fence,
                    crate::types::SegId(vset_state.next_seg),
                    pages,
                ));
            }
            vset_state.commit_running = true;
            Some((
                vset_state.incarnation,
                source,
                vset_state.fence,
                crate::types::SegId(vset_state.next_seg),
                pages,
            ))
        })
    }) else {
        return;
    };
    if pages.is_empty() {
        Peers::send(world.as_ref(), source, PeerMsg::Released { vset }).await;
        return;
    }
    let lease = CommitFlagLease::new(&state, vset, incarnation);
    let mut fetched = Vec::new();
    for (page, (generation, location)) in pages {
        let Some(bytes) = peer_fetch_page(&state, world.as_ref(), source, vset, location).await
        else {
            finish_hydration(&state, vset, incarnation);
            return;
        };
        let Some(raw) = open_entry(vset, &bytes)
            .ok()
            .and_then(|(found, found_generation, raw)| {
                (found == page && found_generation == generation).then_some(raw)
            })
        else {
            finish_hydration(&state, vset, incarnation);
            return;
        };
        fetched.push((page, raw));
    }
    let Some((generations, mut staged, seq, kind, capture_seq, covered, config, database)) = ({
        let host = state.borrow();
        host.vsets
            .get(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
            .map(|vset_state| {
                let generations = fetched
                    .iter()
                    .enumerate()
                    .map(|(offset, (page, _))| {
                        (
                            *page,
                            crate::types::Gen(
                                vset_state.next_gen
                                    + u64::try_from(offset).expect("hydration batch fits u64"),
                            ),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                let mut staged = VsetState::fresh(vset_state.config, incarnation);
                staged.fence = vset_state.fence;
                staged.page_locs = vset_state.page_locs.clone();
                staged.overlay = vset_state.overlay.clone();
                staged.leaf_table = vset_state.leaf_table.clone();
                staged.next_leaf = vset_state.next_leaf;
                (
                    generations,
                    staged,
                    crate::types::JournalSeq(vset_state.next_seq),
                    vset_state
                        .best_record
                        .as_ref()
                        .map_or(RecordKind::Commit, |record| record.kind),
                    vset_state.mutation_seq,
                    vset_state.local_covered_through,
                    vset_state.config,
                    vset_state.database,
                )
            })
    }) else {
        return;
    };
    let mut builder = SegmentBatchBuilder::new(vset, fence, first_segment);
    for (page, raw) in &fetched {
        builder.add(*page, generations[page], raw);
    }
    let segments = builder.finish();
    for (_, _, entries) in &segments {
        for &(page, generation, location) in entries {
            staged.page_locs.insert(page, (generation, location));
            staged.overlay.insert(page, (generation, location));
        }
    }
    let (overlay, leaves, leaf_writes) = shard_map(&mut staged, vset);
    let record = JournalRecord {
        config,
        seq,
        fence,
        kind,
        capture_seq,
        sync_covered_through: covered,
        database,
        overlay: overlay.clone(),
        leaves: leaves.clone(),
        migrated_from: Some(source),
    };
    let reservations = segments
        .iter()
        .map(|(segment, bytes, _)| {
            (
                layout::segment_blob(vset, fence, *segment),
                bytes.len() as u64,
            )
        })
        .chain(leaf_writes.iter().map(|(pointer, bytes, _)| {
            (
                layout::leaf_blob(vset, pointer.fence, pointer.id),
                bytes.len() as u64,
            )
        }))
        .collect::<Vec<_>>();
    if !state.borrow_mut().try_reserve_blobs(&reservations) {
        return;
    }
    for (segment, bytes, _) in &segments {
        let name = layout::segment_blob(vset, fence, *segment);
        if Blobs::write(world.as_ref(), name.clone(), bytes.clone())
            .await
            .is_err()
        {
            AdminIo::abort(world.as_ref(), "hydrated segment write failed").await;
            return;
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
    }
    let mut new_leaf_blobs = Vec::new();
    for (pointer, bytes, segments) in &leaf_writes {
        let name = layout::leaf_blob(vset, pointer.fence, pointer.id);
        if Blobs::write(world.as_ref(), name.clone(), bytes.clone())
            .await
            .is_err()
        {
            AdminIo::abort(world.as_ref(), "hydrated map-leaf write failed").await;
            return;
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
        new_leaf_blobs.push((*pointer, (bytes.len() as u64, segments.clone())));
    }
    if !write_record_copies(&state, world.as_ref(), vset, &record).await {
        AdminIo::abort(world.as_ref(), "hydration journal write failed").await;
        return;
    }
    {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
        else {
            return;
        };
        vset_state.next_gen = vset_state
            .next_gen
            .saturating_add(u64::try_from(fetched.len()).expect("hydration batch fits u64"));
        vset_state.next_seq = seq.0.saturating_add(1);
        vset_state.next_seg = segments
            .iter()
            .map(|(segment, _, _)| segment.0.saturating_add(1))
            .max()
            .unwrap_or(vset_state.next_seg)
            .max(vset_state.next_seg);
        vset_state.next_leaf = staged.next_leaf;
        vset_state.page_locs = staged.page_locs;
        vset_state.overlay = overlay;
        vset_state.leaf_table = leaves;
        vset_state.segment_blobs.extend(
            segments
                .iter()
                .map(|(segment, bytes, _)| (fence, *segment, bytes.len() as u64)),
        );
        vset_state.leaf_blobs.extend(new_leaf_blobs);
        vset_state.wedge.hydration += fetched.len() as u64;
        vset_state.best_record = Some(record.clone());
        vset_state
            .record_writes
            .insert(record.seq, (record.fence, record.sync_covered_through));
        vset_state.commit_running = false;
        host.counters.records_written += 1;
        host.counters.hydrate_fills += fetched.len() as u64;
        host.counters.leaf_rolls += leaf_writes.len() as u64;
    }
    lease.commit();
}

fn finish_hydration(state: &SharedHost, vset: VsetId, incarnation: u64) {
    if let Some(vset_state) = state
        .borrow_mut()
        .vsets
        .get_mut(&vset)
        .filter(|vset_state| vset_state.incarnation == incarnation)
    {
        vset_state.commit_running = false;
    }
}

pub async fn peer_fetch_leaf<W: Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    pointer: LeafPtr,
) -> Option<Vec<u8>> {
    loop {
        let (send, receive) = oneshot();
        let io = {
            let mut host = state.borrow_mut();
            let io = host.allocate_peer_request();
            host.peer_leaves.insert(io, (source, send));
            io
        };
        Peers::send(
            world,
            source,
            PeerMsg::FetchLeaf {
                io,
                vset,
                base: pointer.base,
                fence: pointer.fence,
                id: pointer.id,
            },
        )
        .await;
        match timeout(PEER_RETRY, receive).await {
            Ok(Ok(bytes)) => return bytes,
            Ok(Err(_)) | Err(_) => {
                state.borrow_mut().peer_leaves.remove(&io);
            }
        }
    }
}

async fn release_source<W: Blobs + Peers + GuestMem>(
    state: &SharedHost,
    world: &W,
    from: HostId,
    vset: VsetId,
) {
    let authorized = state
        .borrow()
        .vsets
        .get(&vset)
        .is_none_or(|vset_state| vset_state.outbound == Some(from));
    let (removed, resident) = if authorized {
        let mut host = state.borrow_mut();
        let removed = host.vsets.remove(&vset);
        let resident = host.cache.purge_vset(vset);
        (removed, resident)
    } else {
        (None, Vec::new())
    };
    if let Some(vset_state) = removed {
        let mut names = vset_state
            .segment_blobs
            .iter()
            .map(|(fence, segment, _)| layout::segment_blob(vset, *fence, *segment))
            .collect::<Vec<_>>();
        for (&seq, &(fence, _)) in &vset_state.record_writes {
            names.push(layout::journal_blob(vset, fence, seq));
            names.push(layout::journal_mirror_blob(vset, fence, seq));
        }
        names.extend(vset_state.leaf_blobs.keys().map(|pointer| {
            if pointer.base == 0 {
                layout::leaf_blob(vset, pointer.fence, pointer.id)
            } else {
                layout::base_leaf_blob(vset, pointer.base, pointer.fence, pointer.id)
            }
        }));
        names.push(layout::handoff_blob(vset));
        let _ = Blobs::delete_many_durable(world, &names).await;
        state.borrow_mut().forget_blobs(&names);
    }
    if authorized {
        for (index, page) in resident.into_iter().enumerate() {
            GuestMem::evict(world, page).await;
            if (index + 1) % HYDRATE_BATCH == 0 {
                yield_now().await;
            }
        }
        Peers::send(world, from, PeerMsg::ReleasedAck { vset }).await;
    }
}

fn materialize(
    vset: VsetId,
    record: &JournalRecord,
    leaves: &BTreeMap<LeafPtr, (u64, MapLeaf)>,
) -> BTreeMap<PageId, (crate::types::Gen, crate::segment::PageLoc)> {
    let mut locations = BTreeMap::new();
    for pointer in record.leaves.values() {
        for &(idx, page, generation, location) in &leaves[pointer].1.entries {
            let page = PageId {
                volume: crate::types::VolumeId { vset, idx },
                page,
            };
            if record.config.contains(page) {
                locations.insert(page, (generation, location));
            }
        }
    }
    locations.extend(record.overlay.iter().map(|(&page, &entry)| (page, entry)));
    locations
}
