use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::channel::{OneReceiver, Sender, TrySendError, bounded, oneshot, unbounded};
use blockd_exec::{Either, TaskSet, delay, select2, timeout, yield_now};

use super::capture::{capture_migration, shard_map, write_record_copies};
use super::keyed_queue::KeyedQueue;
use super::replica::publish_replica_head;
use super::state::MutationOwner;
use super::{SharedHost, VsetState, replica_message, replicate_latest};
use super::{adopt_vnode_generation, commit_vnode_closure, read_vnode_closure};
use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::head::HeadRecord;
use crate::journal::{JournalRecord, MigrationSource, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::{LeafPtr, MapLeaf};
use crate::protocol::{AdminError, AdminEvent, AdminResult, AdminSuccess, PeerMsg, Verdict};
use crate::segment::{SegmentBatchBuilder, open_entry};
use crate::types::{HostId, PageId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

const MAGIC_HANDOFF: u32 = u32::from_le_bytes(*b"BHF1");
const OFFER_RETRY: u64 = 5_000_000;
const PEER_RETRY: u64 = 50_000_000;
const HYDRATE_FETCH_ATTEMPTS: u64 = 3;
const HYDRATE_BATCH: usize = 64;
const PEER_STORAGE_SHARDS: usize = 16;
const PEER_STORAGE_CAPACITY: usize = 64;
const REPLICA_ROUTE_SHARDS: usize = 32;
const REPLICA_ROUTE_CAPACITY: usize = 128;
const PEER_LIFECYCLE_CONCURRENCY: usize = 64;
const PEER_LIFECYCLE_QUEUE_CAPACITY: usize = 64;
const PEER_INGRESS_BATCH: usize = 64;

enum PeerStorageRequest {
    Range {
        from: HostId,
        io: crate::protocol::PeerRequestId,
        vset: VsetId,
        fence: u64,
        seg: crate::types::SegId,
        offset: u32,
        len: u32,
    },
    Leaf {
        from: HostId,
        io: crate::protocol::PeerRequestId,
        vset: VsetId,
        base: u64,
        fence: u64,
        id: u64,
    },
}

enum PeerLifecycleRequest {
    MigrateOffer {
        from: HostId,
        vset: VsetId,
        record: Vec<u8>,
    },
    Released {
        from: HostId,
        vset: VsetId,
        release_fence: u64,
    },
}

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

struct HydrationLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
    active: bool,
}

impl HydrationLease {
    fn new(state: &SharedHost, vset: VsetId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            active: true,
        }
    }

    fn commit(mut self) {
        self.state.borrow_mut().schedule_vset(self.vset);
        self.active = false;
    }
}

impl Drop for HydrationLease {
    fn drop(&mut self) {
        if self.active {
            finish_hydration(&self.state, self.vset, self.incarnation);
        }
    }
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
        self.state.borrow_mut().schedule_vset(self.vset);
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
            vset_state.operations.finish_migration();
        }
        self.state.borrow_mut().schedule_vset(self.vset);
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

pub(super) fn encode_handoff(vset: VsetId, to: HostId) -> Vec<u8> {
    Handoff { vset, to }.encode()
}

#[allow(clippy::too_many_lines)]
pub async fn migrate_out<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    to: HostId,
) -> Option<AdminResult>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let incarnation = loop {
        enum Decision {
            Invalid,
            Hydrating(OneReceiver<bool>),
            Reserved(u64),
        }
        let decision =
            {
                let mut host = state.borrow_mut();
                let Some(vset_state) = host.vsets.get_mut(&vset) else {
                    return Some(Err(AdminError::Rejected));
                };
                let allowed = vset_state.ready
                    && vset_state.outbound.is_none()
                    && !vset_state.operations.migration_running()
                    && !vset_state.operations.guest_resume_pending()
                    && (vset_state.config.kind != VsetKind::Database
                        || (vset_state.database_runtime.phase
                            == super::state::AttachmentPhase::Detached
                            && vset_state.database_runtime.active.is_none()
                            && vset_state.database_runtime.handles.is_empty()));
                if !allowed {
                    Decision::Invalid
                } else if vset_state.peer_source.is_some()
                    && (vset_state.page_locs.values().any(|(_, location)| {
                        location.base == 0 && location.fence < vset_state.fence
                    }) || vset_state
                        .leaf_table
                        .keys()
                        .any(|span| !vset_state.hydrated_spans.contains(span)))
                {
                    let (wake, wait) = oneshot();
                    vset_state.hydration_waiters.push(wake);
                    host.schedule_vset(vset);
                    Decision::Hydrating(wait)
                } else {
                    assert!(vset_state.operations.start_migration());
                    Decision::Reserved(vset_state.incarnation)
                }
            };
        match decision {
            Decision::Invalid => return Some(Err(AdminError::Rejected)),
            Decision::Hydrating(wait) => {
                if wait.await != Ok(true) {
                    return Some(Err(AdminError::Unavailable));
                }
            }
            Decision::Reserved(incarnation) => break incarnation,
        }
    };
    let lease = MigrationLease::new(&state, vset, incarnation);
    let kind = state.borrow().vsets[&vset].config.kind;
    let mut paused = None;
    let record = if kind == VsetKind::Compute {
        let Some((record, guard)) =
            capture_migration(Rc::clone(&state), Rc::clone(&world), vset).await
        else {
            return Some(Err(AdminError::Unavailable));
        };
        paused = Some(guard);
        record
    } else {
        let Some(record) = state.borrow().vsets[&vset].best_record.clone() else {
            return Some(Err(AdminError::Rejected));
        };
        record
    };
    replicate_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
    let archive = state.borrow().vsets.get(&vset).and_then(|vset_state| {
        let stash = vset_state.stash_assignment?;
        Some((
            stash.transition_peer.unwrap_or(stash.active_peer),
            stash.assignment_epoch,
            crate::protocol::ReplicaCommitInfo {
                writer_fence: record.fence,
                seq: record.seq,
                sync_covered_through: record.sync_covered_through,
            },
        ))
    });
    let Some((archive_peer, assignment_epoch, through)) = archive else {
        if let Some(paused) = paused.take()
            && !paused.resume().await
        {
            return None;
        }
        return Some(Err(AdminError::Unavailable));
    };
    loop {
        Peers::send(
            world.as_ref(),
            archive_peer,
            PeerMsg::ReplicaArchive {
                vset,
                assignment_epoch,
                through,
            },
        )
        .await;
        publish_replica_head(Rc::clone(&state), Rc::clone(&world), vset).await;
        let published = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
            vset_state.backed.is_some_and(|pointer| {
                (pointer.capture_seq, pointer.seq) >= (record.capture_seq, record.seq)
            })
        });
        if published {
            break;
        }
        // A scheduler-owned replication may have occupied the slot when the
        // migration first tried. Keep driving the exact migration cut after
        // an unsuccessful publication attempt.
        replicate_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
        let retry = state.borrow().config.backup_retry;
        delay(retry).await;
    }
    let handoff_name = layout::handoff_blob(vset);
    let handoff_bytes = encode_handoff(vset, to);
    // A vset can return to a host before an older release has removed that
    // host's previous outbound marker. The in-memory state above proves this
    // is a new migration, so replace any stale marker before publishing the
    // current destination.
    if Blobs::delete(world.as_ref(), &handoff_name).await.is_err() {
        state
            .borrow_mut()
            .fail("stale migration handoff cleanup failed");
        return None;
    }
    state
        .borrow_mut()
        .forget_blobs(std::iter::once(&handoff_name));
    if !state
        .borrow_mut()
        .try_reserve_blob(handoff_name.clone(), handoff_bytes.len() as u64)
    {
        if let Some(paused) = paused.take()
            && !paused.resume().await
        {
            return None;
        }
        return Some(Err(AdminError::Unavailable));
    }
    if Blobs::write(world.as_ref(), handoff_name.clone(), handoff_bytes.clone())
        .await
        .is_err()
    {
        state.borrow_mut().fail("migration handoff write failed");
        return None;
    }
    {
        let mut host = state.borrow_mut();
        host.record_blob(handoff_name, handoff_bytes.len() as u64);
        let vset_state = host.vsets.get_mut(&vset)?;
        vset_state.ready = false;
        vset_state.outbound = Some(to);
    }
    if let Some(paused) = paused.take() {
        paused.disarm();
    }
    let encoded = record.encode(vset);
    while !offer_once(
        &state,
        world.as_ref(),
        vset,
        to,
        record.fence,
        encoded.clone(),
    )
    .await
    {}
    lease.commit();
    Some(Ok(AdminSuccess::MigratedOut { vset }))
}

async fn offer_once<W: Peers>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    to: HostId,
    offer_fence: u64,
    bytes: Vec<u8>,
) -> bool {
    let client = state.borrow().peer_client.clone();
    client
        .offer_migration_once(world, to, vset, offer_fence, bytes, OFFER_RETRY)
        .await
}

pub async fn reoffer_outbound<W: Peers>(state: SharedHost, world: Rc<W>, vset: VsetId) {
    let Some((to, offer_fence, record)) = state.borrow().vsets.get(&vset).and_then(|vset_state| {
        let record = vset_state.best_record.as_ref()?;
        Some((vset_state.outbound?, record.fence, record.encode(vset)))
    }) else {
        return;
    };
    let _ = offer_once(&state, world.as_ref(), vset, to, offer_fence, record).await;
    state.borrow_mut().schedule_vset(vset);
}

#[allow(clippy::too_many_lines)]
pub async fn peer_source<W>(state: SharedHost, world: Rc<W>)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let mut handlers = TaskSet::new();
    let storage_routes = (0..PEER_STORAGE_SHARDS)
        .map(|_| {
            let (send, mut receive) = bounded(PEER_STORAGE_CAPACITY);
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            handlers.spawn(async move {
                while let Some(request) = receive.recv().await {
                    handle_peer_storage(Rc::clone(&state), world.as_ref(), request).await;
                }
            });
            send
        })
        .collect::<Vec<Sender<PeerStorageRequest>>>();
    let replica_routes = (0..REPLICA_ROUTE_SHARDS)
        .map(|_| {
            let (send, mut receive) = bounded(REPLICA_ROUTE_CAPACITY);
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            handlers.spawn(async move {
                while let Some((from, message)) = receive.recv().await {
                    replica_message(Rc::clone(&state), world.as_ref(), from, message).await;
                }
            });
            send
        })
        .collect::<Vec<Sender<(HostId, PeerMsg)>>>();
    let (replica_reply_send, mut replica_reply_receive) = bounded(REPLICA_ROUTE_CAPACITY);
    {
        let state = Rc::clone(&state);
        let world = Rc::clone(&world);
        handlers.spawn(async move {
            while let Some((from, message)) = replica_reply_receive.recv().await {
                replica_message(Rc::clone(&state), world.as_ref(), from, message).await;
            }
        });
    }
    let (lifecycle_completed, mut lifecycle_completions) = unbounded();
    let mut lifecycle = KeyedQueue::new();
    let mut ingress_open = true;
    let mut ingress_batch = 0;
    loop {
        while let Some((vset, request)) = lifecycle.start_next(PEER_LIFECYCLE_CONCURRENCY) {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            let lifecycle_completed = lifecycle_completed.clone();
            handlers.spawn(async move {
                handle_peer_lifecycle(state, world, request).await;
                let _ = lifecycle_completed.send(vset);
            });
        }
        if !ingress_open && lifecycle.is_idle() {
            return;
        }
        let event = if ingress_open {
            select2(lifecycle_completions.recv(), Peers::recv(world.as_ref())).await
        } else {
            Either::First(lifecycle_completions.recv().await)
        };
        let (from, message) = match event {
            Either::First(Some(vset)) => {
                lifecycle.complete(vset);
                continue;
            }
            Either::First(None) => return,
            Either::Second(Some(message)) => message,
            Either::Second(None) => {
                ingress_open = false;
                continue;
            }
        };
        match message {
            PeerMsg::VnodeAdopt { io, proof } => {
                let placement = state.borrow().authority_placement.clone();
                let member = state.borrow().config.host;
                if let Some(placement) = placement
                    && let Ok(receipt) =
                        adopt_vnode_generation(world.as_ref(), &placement, member, proof).await
                {
                    state.borrow_mut().counters.vnode_adoptions += 1;
                    Peers::send(
                        world.as_ref(),
                        from,
                        PeerMsg::VnodeAdoptAck {
                            io,
                            proof: receipt.proof,
                            closures: receipt.closures,
                        },
                    )
                    .await;
                }
            }
            PeerMsg::VnodeAdoptAck {
                io,
                proof,
                closures,
            } => {
                state
                    .borrow_mut()
                    .peer_client
                    .resolve_adoption(io, from, proof, closures);
            }
            PeerMsg::VnodeFetchClosure { io, vnode, closure } => {
                let bytes = read_vnode_closure(world.as_ref(), vnode, closure)
                    .await
                    .ok();
                Peers::send(world.as_ref(), from, PeerMsg::VnodeClosure { io, bytes }).await;
            }
            PeerMsg::VnodeClosure { io, bytes } => {
                state
                    .borrow_mut()
                    .peer_client
                    .resolve_vnode_closure(io, from, bytes);
            }
            PeerMsg::VnodeCommit {
                io,
                proof,
                vset,
                sequence,
                bytes,
            } => {
                let placement = state.borrow().authority_placement.clone();
                if from == proof.authority.primary
                    && let Some(placement) = placement
                    && let Ok(closure) = commit_vnode_closure(
                        world.as_ref(),
                        &placement,
                        proof.authority,
                        vset,
                        sequence,
                        bytes,
                    )
                    .await
                {
                    Peers::send(
                        world.as_ref(),
                        from,
                        PeerMsg::VnodeCommitAck { io, closure },
                    )
                    .await;
                } else {
                    state.borrow_mut().counters.vnode_stale_rejections += 1;
                }
            }
            PeerMsg::VnodeCommitAck { io, closure } => {
                state
                    .borrow_mut()
                    .peer_client
                    .resolve_vnode_commit(io, from, closure);
            }
            PeerMsg::MigrateOffer { vset, record } => {
                if lifecycle.pending_len() < PEER_LIFECYCLE_QUEUE_CAPACITY
                    && !lifecycle.contains_key(vset)
                {
                    lifecycle.push(
                        vset,
                        PeerLifecycleRequest::MigrateOffer { from, vset, record },
                    );
                }
            }
            PeerMsg::MigrateAccept { vset, offer_fence } => {
                let accepted = state
                    .borrow()
                    .vsets
                    .get(&vset)
                    .is_some_and(|vset_state| vset_state.outbound == Some(from));
                if accepted {
                    state
                        .borrow()
                        .peer_client
                        .resolve_migration(vset, from, offer_fence);
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
                let request = PeerStorageRequest::Range {
                    from,
                    io,
                    vset,
                    fence,
                    seg,
                    offset,
                    len,
                };
                let route = peer_storage_route(from, vset);
                let _ = storage_routes[route].try_send(request);
            }
            PeerMsg::Page { io, bytes } => {
                state.borrow_mut().peer_client.resolve_page(io, from, bytes);
            }
            PeerMsg::FetchLeaf {
                io,
                vset,
                base,
                fence,
                id,
            } => {
                let request = PeerStorageRequest::Leaf {
                    from,
                    io,
                    vset,
                    base,
                    fence,
                    id,
                };
                let route = peer_storage_route(from, vset);
                let _ = storage_routes[route].try_send(request);
            }
            PeerMsg::Leaf { io, bytes } => {
                state.borrow_mut().peer_client.resolve_leaf(io, from, bytes);
            }
            PeerMsg::Released {
                vset,
                release_fence,
            } => {
                if lifecycle.pending_len() < PEER_LIFECYCLE_QUEUE_CAPACITY
                    && !lifecycle.contains_key(vset)
                {
                    lifecycle.push(
                        vset,
                        PeerLifecycleRequest::Released {
                            from,
                            vset,
                            release_fence,
                        },
                    );
                }
            }
            PeerMsg::ReleasedAck {
                vset,
                release_fence,
            } => {
                let waiters = state
                    .borrow_mut()
                    .vsets
                    .get_mut(&vset)
                    .filter(|vset_state| {
                        vset_state.peer_source == Some(from) && vset_state.fence == release_fence
                    })
                    .map(|vset_state| {
                        vset_state.peer_source = None;
                        vset_state.peer_source_offer_fence = None;
                        std::mem::take(&mut vset_state.hydration_waiters)
                    })
                    .unwrap_or_default();
                for waiter in waiters {
                    let _ = waiter.send(true);
                }
            }
            message @ (PeerMsg::ReplicaPutAck { .. }
            | PeerMsg::ReplicaCommitAck { .. }
            | PeerMsg::ReplicaStatusReply { .. }) => {
                if let Err(error) = replica_reply_send.try_send((from, message)) {
                    let (from, message) = match error {
                        TrySendError::Full(message) | TrySendError::Closed(message) => message,
                    };
                    replica_message(Rc::clone(&state), world.as_ref(), from, message).await;
                }
            }
            message @ (PeerMsg::ReplicaPut { .. }
            | PeerMsg::ReplicaCommit { .. }
            | PeerMsg::ReplicaStatus { .. }
            | PeerMsg::ReplicaUploadDone { .. }
            | PeerMsg::ReplicaArchive { .. }
            | PeerMsg::ReplicaRelease { .. }
            | PeerMsg::ReplicaReleaseAck { .. }) => {
                let key = replica_route_key(from, &message);
                let route = replica_route(key);
                if replica_routes[route].try_send((from, message)).is_err() {
                    state.borrow_mut().counters.replica_capacity_backpressure += 1;
                }
            }
        }
        ingress_batch += 1;
        if ingress_batch == PEER_INGRESS_BATCH {
            ingress_batch = 0;
            yield_now().await;
        }
    }
}

async fn handle_peer_lifecycle<W>(state: SharedHost, world: Rc<W>, request: PeerLifecycleRequest)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    match request {
        PeerLifecycleRequest::MigrateOffer { from, vset, record } => {
            migrate_in(state, world, from, vset, record).await;
        }
        PeerLifecycleRequest::Released {
            from,
            vset,
            release_fence,
        } => {
            release_source(&state, world.as_ref(), from, vset, release_fence).await;
        }
    }
}

fn peer_storage_route(from: HostId, vset: VsetId) -> usize {
    usize::try_from((u64::from(from.0).wrapping_mul(31) ^ vset.0) % PEER_STORAGE_SHARDS as u64)
        .expect("peer storage route fits")
}

fn replica_route((from, vset, assignment_epoch): (HostId, VsetId, u64)) -> usize {
    usize::try_from(
        (u64::from(from.0).wrapping_mul(31) ^ vset.0.wrapping_mul(17) ^ assignment_epoch)
            % REPLICA_ROUTE_SHARDS as u64,
    )
    .expect("replica route fits")
}

async fn handle_peer_storage<W: Blobs + Peers>(
    state: SharedHost,
    world: &W,
    request: PeerStorageRequest,
) {
    let (from, vset) = match &request {
        PeerStorageRequest::Range { from, vset, .. }
        | PeerStorageRequest::Leaf { from, vset, .. } => (*from, *vset),
    };
    let authorized = state
        .borrow()
        .vsets
        .get(&vset)
        .is_some_and(|vset_state| vset_state.outbound == Some(from));
    if authorized && let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
        vset_state.wedge.served += 1;
    }
    match request {
        PeerStorageRequest::Range {
            io,
            fence,
            seg,
            offset,
            len,
            ..
        } => {
            let bytes = if authorized {
                Blobs::read_range(
                    world,
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
            Peers::send(world, from, PeerMsg::Page { io, bytes }).await;
        }
        PeerStorageRequest::Leaf {
            io,
            base,
            fence,
            id,
            ..
        } => {
            let name = if base == 0 {
                layout::leaf_blob(vset, fence, id)
            } else {
                layout::base_leaf_blob(vset, base, fence, id)
            };
            let bytes = if authorized {
                Blobs::read(world, &name).await.ok().flatten()
            } else {
                None
            };
            Peers::send(world, from, PeerMsg::Leaf { io, bytes }).await;
        }
    }
}

fn replica_route_key(from: HostId, message: &PeerMsg) -> (HostId, VsetId, u64) {
    let (vset, assignment_epoch) = match message {
        PeerMsg::ReplicaPut {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaPutAck {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaCommit {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaCommitAck {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaStatus {
            vset,
            assignment_epoch,
        }
        | PeerMsg::ReplicaStatusReply {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaUploadDone {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaArchive {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaRelease {
            vset,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaReleaseAck {
            vset,
            assignment_epoch,
            ..
        } => (*vset, *assignment_epoch),
        _ => unreachable!("non-replica message"),
    };
    (from, vset, assignment_epoch)
}

#[allow(clippy::too_many_lines)]
pub(super) async fn migrate_in<W>(
    state: SharedHost,
    world: Rc<W>,
    from: HostId,
    vset: VsetId,
    bytes: Vec<u8>,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let Ok(offered) = JournalRecord::decode(vset, &bytes) else {
        return;
    };
    let existing = state.borrow().vsets.get(&vset).map(|existing| {
        existing.peer_source == Some(from)
            && existing.ready
            && existing.peer_source_offer_fence == Some(offered.fence)
    });
    if let Some(ready) = existing {
        if ready {
            Peers::send(
                world.as_ref(),
                from,
                PeerMsg::MigrateAccept {
                    vset,
                    offer_fence: offered.fence,
                },
            )
            .await;
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
    let Some(fence) = available_inbound_fence(&state, vset, offered.fence) else {
        return;
    };
    let incarnation = state.borrow_mut().allocate_incarnation();
    let mut record = offered.clone();
    record.seq = crate::types::JournalSeq(0);
    record.fence = fence;
    record.migrated_from = Some(MigrationSource {
        host: from,
        offer_fence: Some(offered.fence),
    });
    if !write_record_copies(&state, world.as_ref(), vset, &record).await {
        state
            .borrow_mut()
            .fail("inbound migration journal write failed");
        return;
    }
    {
        let mut host = state.borrow_mut();
        if host.vsets.contains_key(&vset) {
            return;
        }
        let mut incoming = VsetState::fresh(record.config, incarnation);
        incoming.ready = false;
        incoming.peer_source = Some(from);
        incoming.peer_source_offer_fence = Some(offered.fence);
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
    if !claim_migrated_database_head(&state, world.as_ref(), from, vset, incarnation, &offered)
        .await
    {
        state.borrow_mut().vsets.remove(&vset);
        return;
    }
    if matches!(verdict, Verdict::Resume { .. })
        && GuestMem::resume(world.as_ref(), vset, None).await.is_err()
    {
        state.borrow_mut().fail("migrated guest resume failed");
        return;
    }
    AdminIo::emit_admin_event(world.as_ref(), AdminEvent::VsetMigratedIn { vset, verdict }).await;
    Peers::send(
        world.as_ref(),
        from,
        PeerMsg::MigrateAccept {
            vset,
            offer_fence: offered.fence,
        },
    )
    .await;
}

pub(super) fn available_inbound_fence(
    state: &SharedHost,
    vset: VsetId,
    offered: u64,
) -> Option<u64> {
    let occupied = state.borrow().local_artifact_fences(vset);
    offered
        .max(occupied.iter().copied().max().unwrap_or(0))
        .checked_add(1)
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
    let Some(local_stash) = super::replica::initial_stash(state, vset) else {
        return false;
    };
    let (claim_fence, manifest, stash, retired_stashes) = loop {
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
        if head.holder != local && head.holder != source {
            return false;
        }
        let locally_held = head.holder == local;
        let stash = if locally_held {
            head.stash.or(Some(local_stash))
        } else {
            Some(local_stash)
        };
        let retired_stashes = if locally_held {
            head.retired_stashes.clone()
        } else {
            Vec::new()
        };
        let claim = HeadRecord {
            vset,
            holder: local,
            fence: 0,
            manifest: head.manifest,
            stash,
            retired_stashes: retired_stashes.clone(),
        };
        match Store::put_cas(
            world,
            layout::head_key(vset),
            Some(current.0),
            claim.encode(),
        )
        .await
        {
            Ok(version) if version > head.fence => {
                break (version, head.manifest, stash, retired_stashes);
            }
            Ok(_) => {
                state
                    .borrow_mut()
                    .fail("head CAS did not advance the ownership fence");
                return false;
            }
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
    claimed.migrated_from = Some(MigrationSource {
        host: source,
        offer_fence: Some(offered.fence),
    });
    if !write_record_copies(state, world, vset, &claimed).await {
        state
            .borrow_mut()
            .fail("claimed migration journal write failed");
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
        vset_state.backed = manifest;
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
        manifest,
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
            || head.manifest != manifest
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
    host.schedule_vset(vset);
    true
}

pub async fn peer_fetch_page<W: Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    location: crate::segment::PageLoc,
) -> Option<Vec<u8>> {
    let client = state.borrow().peer_client.clone();
    client
        .fetch_page(world, source, vset, location, PEER_RETRY)
        .await
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
            if !vset_state.ready || vset_state.operations.mutation_blocked() {
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
            assert!(
                vset_state
                    .operations
                    .try_start_mutation(MutationOwner::Hydration)
            );
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
        Peers::send(
            world.as_ref(),
            source,
            PeerMsg::Released {
                vset,
                release_fence: fence,
            },
        )
        .await;
        return;
    }
    let lease = HydrationLease::new(&state, vset, incarnation);
    let mut workers = TaskSet::new();
    let mut outcomes = Vec::with_capacity(pages.len());
    for (page, (generation, location)) in pages {
        let (send, receive) = oneshot();
        let state = Rc::clone(&state);
        let world = Rc::clone(&world);
        workers.spawn(async move {
            let bytes = timeout(
                PEER_RETRY.saturating_mul(HYDRATE_FETCH_ATTEMPTS),
                peer_fetch_page(&state, world.as_ref(), source, vset, location),
            )
            .await
            .unwrap_or(None);
            let _ = send.send((page, generation, bytes));
        });
        outcomes.push(receive);
    }
    let mut fetched = Vec::new();
    for outcome in outcomes {
        let Ok((page, generation, Some(bytes))) = outcome.await else {
            return;
        };
        let Some(raw) = open_entry(vset, &bytes)
            .ok()
            .and_then(|(found, found_generation, raw)| {
                (found == page && found_generation == generation).then_some(raw)
            })
        else {
            return;
        };
        fetched.push((page, raw));
    }
    let Some((
        generations,
        mut staged,
        seq,
        kind,
        capture_seq,
        covered,
        config,
        database,
        source_offer_fence,
    )) = ({
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
                    vset_state.peer_source_offer_fence,
                )
            })
    })
    else {
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
        migrated_from: Some(MigrationSource {
            host: source,
            offer_fence: source_offer_fence,
        }),
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
            state.borrow_mut().fail("hydrated segment write failed");
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
            state.borrow_mut().fail("hydrated map-leaf write failed");
            return;
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
        new_leaf_blobs.push((*pointer, (bytes.len() as u64, segments.clone())));
    }
    if !write_record_copies(&state, world.as_ref(), vset, &record).await {
        state.borrow_mut().fail("hydration journal write failed");
        return;
    }
    let (hydration_waiters, mutation_waiters) = {
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
        vset_state
            .operations
            .finish_mutation(MutationOwner::Hydration);
        let hydration_waiters = std::mem::take(&mut vset_state.hydration_waiters);
        let mutation_waiters = std::mem::take(&mut vset_state.mutation_waiters);
        host.counters.records_written += 1;
        host.counters.hydrate_fills += fetched.len() as u64;
        host.counters.leaf_rolls += leaf_writes.len() as u64;
        (hydration_waiters, mutation_waiters)
    };
    lease.commit();
    for waiter in hydration_waiters {
        let _ = waiter.send(true);
    }
    for waiter in mutation_waiters {
        let _ = waiter.send(());
    }
}

pub(super) fn finish_hydration(state: &SharedHost, vset: VsetId, incarnation: u64) {
    let (hydration_waiters, mutation_waiters) = {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
        else {
            return;
        };
        vset_state
            .operations
            .finish_mutation(MutationOwner::Hydration);
        let hydration_waiters = std::mem::take(&mut vset_state.hydration_waiters);
        let mutation_waiters = std::mem::take(&mut vset_state.mutation_waiters);
        host.schedule_vset(vset);
        (hydration_waiters, mutation_waiters)
    };
    for waiter in hydration_waiters {
        let _ = waiter.send(false);
    }
    for waiter in mutation_waiters {
        let _ = waiter.send(());
    }
}

pub async fn peer_fetch_leaf<W: Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    pointer: LeafPtr,
) -> Option<Vec<u8>> {
    let client = state.borrow().peer_client.clone();
    client
        .fetch_leaf(world, source, vset, pointer, PEER_RETRY)
        .await
}

async fn release_source<W: Blobs + Peers + GuestMem>(
    state: &SharedHost,
    world: &W,
    from: HostId,
    vset: VsetId,
    release_fence: u64,
) {
    let authorized = state.borrow().vsets.get(&vset).is_none_or(|vset_state| {
        vset_state.outbound == Some(from) && release_fence > vset_state.fence
    });
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
            if GuestMem::evict(world, page).await.is_err() {
                state
                    .borrow_mut()
                    .fail("released guest page eviction failed");
                return;
            }
            if (index + 1) % HYDRATE_BATCH == 0 {
                yield_now().await;
            }
        }
        Peers::send(
            world,
            from,
            PeerMsg::ReleasedAck {
                vset,
                release_fence,
            },
        )
        .await;
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

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::engine::HostState;
    use crate::hostmeta::HostConfig;
    use crate::journal::VsetConfig;

    #[test]
    fn successful_hydration_preserves_an_overlapping_migration_reservation() {
        let state = Rc::new(RefCell::new(HostState::new(HostConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let vset = VsetId(1);
        let incarnation = {
            let mut host = state.borrow_mut();
            let incarnation = host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let operations = &mut host.vsets.get_mut(&vset).expect("inserted vset").operations;
            assert!(operations.try_start_mutation(MutationOwner::Hydration));
            assert!(operations.start_migration());
            operations.finish_mutation(MutationOwner::Hydration);
            incarnation
        };

        HydrationLease::new(&state, vset, incarnation).commit();

        let mut host = state.borrow_mut();
        let operations = &mut host.vsets.get_mut(&vset).expect("inserted vset").operations;
        assert!(operations.migration_running());
        assert!(!operations.start_migration());
    }
}
