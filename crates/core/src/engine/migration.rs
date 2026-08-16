use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::channel::{OneReceiver, Sender, TrySendError, bounded, oneshot, unbounded};
use blockd_exec::{Either, TaskSet, delay, select2, timeout, yield_now};

use super::capture::{
    capture_migration, recovery_files, write_migration_record_copies, write_record_copies,
};
use super::keyed_queue::KeyedQueue;
use super::state::MutationOwner;
use super::{SharedHost, VsetState, replica_message, replicate_latest};
use super::{adopt_vnode_generation, commit_vnode_closure, read_vnode_closure};
use crate::blx::{
    BlockKey, BlockSpace, BlxEntry, EntryKind, NamespaceKind, open_entry as open_blx_entry,
    open_footer, open_object, replace_state_block,
};
use crate::format::{Dec, DecodeError, Enc, checksum64, open_frame, seal_frame};
use crate::head::HeadRecord;
use crate::journal::{JournalRecord, MigrationSource, RecordKind, VsetKind};
use crate::layout;
use crate::manifest::ObjectRef;
use crate::mapleaf::{LeafPtr, MapLeaf};
use crate::protocol::{AdminError, AdminEvent, AdminResult, AdminSuccess, PeerMsg, Verdict};
use crate::segment::{PageLoc, SegmentBatchBuilder, open_entry};
use crate::types::{Gen, HostId, PageId, SegId, VsetId};
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
        replica_assignment_epoch: Option<u64>,
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
        vmstate: Option<Vec<u8>>,
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
    let (incarnation, publishing) = loop {
        enum Decision {
            Invalid,
            Hydrating(OneReceiver<bool>),
            Reserved { incarnation: u64, publishing: bool },
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
                    Decision::Reserved {
                        incarnation: vset_state.incarnation,
                        publishing: vset_state.operations.publication_owner().is_some(),
                    }
                }
            };
        match decision {
            Decision::Invalid => return Some(Err(AdminError::Rejected)),
            Decision::Hydrating(wait) => {
                if wait.await != Ok(true) {
                    return Some(Err(AdminError::Unavailable));
                }
            }
            Decision::Reserved {
                incarnation,
                publishing,
            } => break (incarnation, publishing),
        }
    };
    let lease = MigrationLease::new(&state, vset, incarnation);
    if publishing {
        loop {
            let publication_finished = {
                let host = state.borrow();
                let Some(vset_state) = host
                    .vsets
                    .get(&vset)
                    .filter(|vset_state| vset_state.incarnation == incarnation)
                else {
                    return None;
                };
                vset_state.operations.publication_owner().is_none()
            };
            if publication_finished {
                break;
            }
            let retry = state.borrow().config.backup_retry;
            delay(retry).await;
        }
    }
    let kind = state.borrow().vsets[&vset].config.kind;

    // Do not enter the pause unless a passive has already been assigned. No
    // network operation belongs between capture_migration and commit below.
    if state
        .borrow()
        .vsets
        .get(&vset)
        .and_then(|vset_state| vset_state.stash_assignment)
        .is_none()
    {
        return Some(Err(AdminError::Unavailable));
    }

    let mut paused = None;
    let mut offered_vmstate = None;
    let record = if kind == VsetKind::Compute {
        let Some((record, guard, vmstate)) =
            capture_migration(Rc::clone(&state), Rc::clone(&world), vset).await
        else {
            return Some(Err(AdminError::Unavailable));
        };
        paused = Some(guard);
        offered_vmstate = Some(vmstate);
        record
    } else {
        let Some(record) = state.borrow().vsets[&vset].best_record.clone() else {
            return Some(Err(AdminError::Rejected));
        };
        record
    };
    if let Some(paused) = paused.as_mut()
        && !paused.commit().await
    {
        return None;
    }
    // The source is now stopped, so network work cannot extend the guest
    // pause. Protect the exact local handoff cut on the passive before
    // offering it to the destination. Object-store archival remains
    // background work and is not a migration prerequisite.
    loop {
        replicate_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
        let protected = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
            vset_state.peer_committed.is_some_and(|committed| {
                (
                    committed.writer_fence,
                    committed.seq,
                    committed.sync_covered_through,
                ) >= (record.fence, record.seq, record.sync_covered_through)
            })
        });
        if protected {
            break;
        }
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
    if super::blob::write(
        &state,
        world.as_ref(),
        handoff_name.clone(),
        handoff_bytes.clone(),
    )
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
    let checksums = state.borrow().vsets.get(&vset)?.block_checksums.clone();
    let encoded = record.encode_migration_with_checksums(vset, &checksums);
    let inline_vmstate = offered_vmstate.filter(|vmstate| {
        encoded
            .len()
            .checked_add(vmstate.len())
            .and_then(|size| size.checked_add(32))
            .is_some_and(|size| size <= crate::peer::MAX_PEER_PAYLOAD as usize)
    });
    while !offer_once(
        &state,
        world.as_ref(),
        vset,
        to,
        record.fence,
        encoded.clone(),
        inline_vmstate.clone(),
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
    vmstate: Option<Vec<u8>>,
) -> bool {
    let client = state.borrow().peer_client.clone();
    client
        .offer_migration_once(world, to, vset, offer_fence, bytes, vmstate, OFFER_RETRY)
        .await
}

pub async fn reoffer_outbound<W: Peers>(state: SharedHost, world: Rc<W>, vset: VsetId) {
    let Some((to, offer_fence, record)) = state.borrow().vsets.get(&vset).and_then(|vset_state| {
        let record = vset_state.best_record.as_ref()?;
        Some((
            vset_state.outbound?,
            record.fence,
            record.encode_migration_with_checksums(vset, &vset_state.block_checksums),
        ))
    }) else {
        return;
    };
    let _ = offer_once(&state, world.as_ref(), vset, to, offer_fence, record, None).await;
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
            PeerMsg::MigrateOffer {
                vset,
                record,
                vmstate,
            } => {
                if lifecycle.pending_len() < PEER_LIFECYCLE_QUEUE_CAPACITY
                    && !lifecycle.contains_key(vset)
                {
                    lifecycle.push(
                        vset,
                        PeerLifecycleRequest::MigrateOffer {
                            from,
                            vset,
                            record,
                            vmstate,
                        },
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
                replica_assignment_epoch,
                fence,
                seg,
                offset,
                len,
            } => {
                let request = PeerStorageRequest::Range {
                    from,
                    io,
                    vset,
                    replica_assignment_epoch,
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
        PeerLifecycleRequest::MigrateOffer {
            from,
            vset,
            record,
            vmstate,
        } => {
            migrate_in(state, world, from, vset, record, vmstate).await;
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
            replica_assignment_epoch,
            fence,
            seg,
            offset,
            len,
            ..
        } => {
            let replica_bytes = replica_assignment_epoch.and_then(|assignment_epoch| {
                let host = state.borrow();
                let key = super::state::ReplicaKey {
                    source: from,
                    vset,
                    assignment_epoch,
                };
                let artifact = crate::protocol::ReplicaArtifact::Segment { fence, seg };
                host.replicas.get(&key).and_then(|replica| {
                    (!replica.uncommitted_artifacts.contains(&artifact))
                        .then(|| {
                            replica
                                .artifacts
                                .get(&artifact)
                                .map(|(_, bytes)| bytes.clone())
                        })
                        .flatten()
                })
            });
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
            } else if let Some(bytes) = replica_bytes {
                let start = usize::try_from(offset).ok();
                let end = start.and_then(|start| {
                    start.checked_add(usize::try_from(len).expect("u32 fits usize"))
                });
                start
                    .zip(end)
                    .and_then(|(start, end)| bytes.get(start..end).map(<[u8]>::to_vec))
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
    inline_vmstate: Option<Vec<u8>>,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let Ok((offered, offered_checksums)) =
        JournalRecord::decode_migration_with_checksums(vset, &bytes)
    else {
        return;
    };
    let offered_state_checksum =
        offered_checksums
            .iter()
            .fold(0, |checksum, (&key, &(generation, value_checksum))| {
                checksum ^ crate::blx::state_contribution(key, generation, value_checksum)
            });
    if offered_state_checksum != offered.post_state_checksum {
        return;
    }
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
        (VsetKind::Compute, RecordKind::Checkpoint { epoch, vmstate, .. }) => {
            Verdict::Resume { epoch, vmstate }
        }
        (VsetKind::Database, RecordKind::Commit) => Verdict::DatabaseReady {
            synced_through: offered.sync_covered_through,
        },
        _ => return,
    };
    let vmstate = match offered.kind {
        RecordKind::Checkpoint {
            vmstate_logical_length,
            vmstate: expected_vmstate,
            ..
        } => {
            let loaded = if let Some(bytes) = inline_vmstate {
                decode_inline_vmstate(
                    bytes,
                    vmstate_logical_length,
                    Gen(offered.seq.0),
                    expected_vmstate,
                )
            } else {
                load_migrated_vmstate(
                    &state,
                    world.as_ref(),
                    from,
                    vset,
                    &offered.files,
                    vmstate_logical_length,
                )
                .await
            };
            let Some(loaded) = loaded else {
                return;
            };
            Some(loaded)
        }
        RecordKind::Commit => None,
    };
    if let Some(loaded) = vmstate.as_ref() {
        let offered_vmm_blocks = offered_checksums
            .iter()
            .filter(|(key, _)| key.space == BlockSpace::Vmm)
            .count();
        if offered_vmm_blocks != loaded.block_checksums.len()
            || loaded
                .block_checksums
                .iter()
                .any(|(key, checksum)| offered_checksums.get(key) != Some(checksum))
        {
            return;
        }
    }
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
        if super::blob::write(&state, world.as_ref(), name.clone(), bytes.clone())
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
    let local_vmm = if let Some(loaded) = vmstate.as_ref() {
        let mut builder = SegmentBatchBuilder::new_for_record_with_checksums(
            offered.config.kind,
            vset,
            fence,
            SegId(0),
            0,
            offered.post_state_checksum,
            offered.post_state_checksum,
        );
        for (block, chunk) in loaded.bytes.chunks(crate::types::page_size()).enumerate() {
            let block = u32::try_from(block).expect("validated VMM block count fits u32");
            let key = BlockKey {
                space: BlockSpace::Vmm,
                volume: 0,
                block,
            };
            let Some(&(generation, _)) = loaded.block_checksums.get(&key) else {
                return;
            };
            let mut padded = vec![0; crate::types::page_size()];
            padded[..chunk.len()].copy_from_slice(chunk);
            builder.add_vmm_block(block, generation, &padded);
        }
        builder.finish()
    } else {
        Vec::new()
    };
    let local_vmm_refs = local_vmm
        .iter()
        .map(|(segment, bytes, _)| {
            open_object(bytes)
                .map(|object| ((*segment), ObjectRef::from_blx(&object)))
                .ok()
        })
        .collect::<Option<Vec<_>>>();
    let Some(local_vmm_refs) = local_vmm_refs else {
        return;
    };
    let reservations = local_vmm
        .iter()
        .map(|(segment, bytes, _)| {
            (
                layout::segment_blob(vset, fence, *segment),
                bytes.len() as u64,
            )
        })
        .collect::<Vec<_>>();
    if !state.borrow_mut().try_reserve_blobs(&reservations) {
        return;
    }
    for (segment, bytes, _) in &local_vmm {
        let name = layout::segment_blob(vset, fence, *segment);
        if Blobs::write(world.as_ref(), name.clone(), bytes.clone())
            .await
            .is_err()
        {
            return;
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
    }
    let mut record = offered.clone();
    record.seq = crate::types::JournalSeq(0);
    record.fence = fence;
    record
        .files
        .extend(local_vmm_refs.iter().map(|(_, reference)| *reference));
    record.migrated_from = Some(MigrationSource {
        host: from,
        offer_fence: Some(offered.fence),
    });
    if !write_migration_record_copies(&state, world.as_ref(), vset, &record, &offered_checksums)
        .await
    {
        state
            .borrow_mut()
            .fail("inbound migration journal write failed");
        return;
    }
    if let Some(loaded) = vmstate.as_ref()
        && GuestMem::install_vmstate(world.as_ref(), vset, loaded.bytes.clone())
            .await
            .is_err()
    {
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
        incoming.state_checksum = record.post_state_checksum;
        incoming.block_checksums = offered_checksums;
        incoming.local_covered_through = record.sync_covered_through;
        incoming.sync_ack_through = record.sync_covered_through;
        // The source does not offer this cut until its passive has committed
        // it. Preserve that inherited protection watermark while the
        // destination hydrates and establishes its own passive copy.
        incoming.peer_committed_through = record.sync_covered_through;
        incoming.next_seq = 1;
        incoming.next_seg = u64::try_from(local_vmm.len()).expect("VMM object count fits u64");
        incoming.overlay = record.overlay.clone();
        incoming.leaf_table = record.leaves.clone();
        incoming.hydrated_spans = record.leaves.keys().copied().collect();
        incoming.page_locs = materialize(vset, &record, &leaves);
        incoming.next_gen = incoming
            .block_checksums
            .values()
            .map(|(generation, _)| generation.0.saturating_add(1))
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
        incoming.segment_blobs.extend(
            local_vmm
                .iter()
                .map(|(segment, bytes, _)| (fence, *segment, bytes.len() as u64)),
        );
        incoming
            .segment_refs
            .extend(record.files.iter().filter_map(|reference| {
                (reference.identity.namespace_kind == NamespaceKind::Vset
                    && reference.identity.namespace_id == vset.0)
                    .then_some((
                        (
                            reference.identity.writer_fence,
                            SegId(reference.identity.object_id),
                        ),
                        *reference,
                    ))
            }));
        incoming
            .vmm_segments
            .extend(local_vmm_refs.iter().map(|(segment, _)| (fence, *segment)));
        incoming.best_record = Some(record.clone());
        if matches!(verdict, Verdict::Resume { .. }) {
            incoming.pinned = Some(record.clone());
        }
        incoming
            .record_writes
            .insert(record.seq, (fence, record.sync_covered_through));
        incoming.record_segments.insert(
            record.seq,
            local_vmm_refs
                .iter()
                .map(|(segment, _)| (fence, *segment))
                .collect(),
        );
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
        .fetch_page(world, source, vset, location, None, PEER_RETRY)
        .await
}

pub async fn peer_fetch_replica_page<W: Peers>(
    state: &SharedHost,
    world: &W,
    passive: HostId,
    assignment_epoch: u64,
    vset: VsetId,
    location: crate::segment::PageLoc,
) -> Option<Vec<u8>> {
    let client = state.borrow().peer_client.clone();
    client
        .fetch_page(
            world,
            passive,
            vset,
            location,
            Some(assignment_epoch),
            PEER_RETRY,
        )
        .await
}

struct MigratedVmstate {
    bytes: Vec<u8>,
    block_checksums: BTreeMap<BlockKey, (Gen, u64)>,
}

fn decode_inline_vmstate(
    bytes: Vec<u8>,
    logical_length: u64,
    generation: Gen,
    expected_vmstate: u64,
) -> Option<MigratedVmstate> {
    if bytes.len() != usize::try_from(logical_length).ok()? {
        return None;
    }
    let raw: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
    if u64::from_le_bytes(raw) != expected_vmstate {
        return None;
    }
    let mut block_checksums = BTreeMap::new();
    for (block, chunk) in bytes.chunks(crate::types::page_size()).enumerate() {
        let mut padded = vec![0; crate::types::page_size()];
        padded[..chunk.len()].copy_from_slice(chunk);
        block_checksums.insert(
            BlockKey {
                space: BlockSpace::Vmm,
                volume: 0,
                block: u32::try_from(block).ok()?,
            },
            (generation, checksum64(&padded)),
        );
    }
    Some(MigratedVmstate {
        bytes,
        block_checksums,
    })
}

#[allow(clippy::too_many_lines)]
async fn load_migrated_vmstate<W: Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    vset: VsetId,
    objects: &[ObjectRef],
    logical_length: u64,
) -> Option<MigratedVmstate> {
    let block_count = logical_length.div_ceil(crate::types::page_size() as u64);
    if block_count == 0 {
        return None;
    }
    let first_vmstate_key = BlockKey {
        space: BlockSpace::Vmm,
        volume: 0,
        block: 0,
    };
    let last_vmstate_key = BlockKey {
        block: u32::try_from(block_count.checked_sub(1)?).ok()?,
        ..first_vmstate_key
    };
    let mut indexed = Vec::new();
    for reference in objects {
        if reference.identity.namespace_kind != NamespaceKind::Vset
            || reference.identity.namespace_id != vset.0
            || reference.last_key < first_vmstate_key
            || last_vmstate_key < reference.first_key
        {
            continue;
        }
        let bytes = peer_fetch_page(
            state,
            world,
            source,
            vset,
            PageLoc {
                base: 0,
                fence: reference.identity.writer_fence,
                seg: SegId(reference.identity.object_id),
                offset: reference.footer_offset,
                len: reference.footer_length,
            },
        )
        .await?;
        let footer = open_footer(&bytes).ok()?;
        if footer.entries.first().map(|entry| entry.key) != Some(reference.first_key)
            || footer.entries.last().map(|entry| entry.key) != Some(reference.last_key)
        {
            return None;
        }
        indexed.push((*reference, footer));
    }

    let mut output = Vec::with_capacity(usize::try_from(logical_length).ok()?);
    let mut block_checksums = BTreeMap::new();
    for block in 0..block_count {
        let key = BlockKey {
            space: BlockSpace::Vmm,
            volume: 0,
            block: u32::try_from(block).ok()?,
        };
        let mut winner = None;
        for (reference, footer) in &indexed {
            let Some(entry) = footer.find(key) else {
                continue;
            };
            if winner.as_ref().is_none_or(
                |(old, old_ref): &(crate::blx::FooterEntry, ObjectRef)| {
                    (entry.generation, reference.identity) > (old.generation, old_ref.identity)
                },
            ) {
                winner = Some((entry, *reference));
            }
        }
        let (entry, reference) = winner?;
        if entry.kind != EntryKind::Data {
            return None;
        }
        let bytes = peer_fetch_page(
            state,
            world,
            source,
            vset,
            PageLoc {
                base: 0,
                fence: reference.identity.writer_fence,
                seg: SegId(reference.identity.object_id),
                offset: entry.offset,
                len: entry.length,
            },
        )
        .await?;
        let BlxEntry::Data {
            key: found,
            generation,
            bytes,
        } = open_blx_entry(&bytes).ok()?
        else {
            return None;
        };
        if found != key
            || generation != entry.generation
            || checksum64(&bytes) != entry.value_checksum
        {
            return None;
        }
        block_checksums.insert(key, (generation, entry.value_checksum));
        output.extend_from_slice(&bytes);
    }
    output.truncate(usize::try_from(logical_length).ok()?);
    Some(MigratedVmstate {
        bytes: output,
        block_checksums,
    })
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
                staged.block_checksums = vset_state.block_checksums.clone();
                staged.state_checksum = vset_state.state_checksum;
                staged.pending_tombstones = vset_state.pending_tombstones.clone();
                staged.archived_memory_usable = vset_state.archived_memory_usable;
                staged.leaf_table = vset_state.leaf_table.clone();
                staged.next_leaf = vset_state.next_leaf;
                staged.segment_refs = vset_state.segment_refs.clone();
                staged.vmm_segments = vset_state.vmm_segments.clone();
                staged.tombstone_segments = vset_state.tombstone_segments.clone();
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
    let pre_state_checksum = staged.state_checksum;
    for (page, raw) in &fetched {
        replace_state_block(
            &mut staged.state_checksum,
            &mut staged.block_checksums,
            BlockKey::from_page(config.kind, *page),
            Some((generations[page], checksum64(raw))),
        );
    }
    let mut builder = SegmentBatchBuilder::new_for_record_with_checksums(
        config.kind,
        vset,
        fence,
        first_segment,
        seq.0,
        pre_state_checksum,
        staged.state_checksum,
    );
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
    let overlay = staged.page_locs.clone();
    let leaves = BTreeMap::new();
    let mut files = state
        .borrow()
        .vsets
        .get(&vset)
        .map(|vset| vset.segment_refs.clone())
        .unwrap_or_default();
    for (segment, bytes, _) in &segments {
        let Ok(object) = open_object(bytes) else {
            state
                .borrow_mut()
                .fail("hydrated BLX object failed verification");
            return;
        };
        let reference = ObjectRef::from_blx(&object);
        files.insert((fence, *segment), reference);
        staged.segment_refs.insert((fence, *segment), reference);
    }
    let record = JournalRecord {
        config,
        seq,
        fence,
        kind,
        capture_seq,
        sync_covered_through: covered,
        post_state_checksum: staged.state_checksum,
        database,
        files: recovery_files(&staged),
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
        .collect::<Vec<_>>();
    if !state.borrow_mut().try_reserve_blobs(&reservations) {
        return;
    }
    for (segment, bytes, _) in &segments {
        let name = layout::segment_blob(vset, fence, *segment);
        if super::blob::write(&state, world.as_ref(), name.clone(), bytes.clone())
            .await
            .is_err()
        {
            state.borrow_mut().fail("hydrated segment write failed");
            return;
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
    }
    if !write_migration_record_copies(
        &state,
        world.as_ref(),
        vset,
        &record,
        &staged.block_checksums,
    )
    .await
    {
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
        vset_state.block_checksums = staged.block_checksums;
        vset_state.state_checksum = staged.state_checksum;
        vset_state.pending_tombstones = staged.pending_tombstones;
        vset_state.overlay = overlay;
        vset_state.leaf_table = leaves;
        vset_state.segment_blobs.extend(
            segments
                .iter()
                .map(|(segment, bytes, _)| (fence, *segment, bytes.len() as u64)),
        );
        vset_state.segment_refs = record
            .files
            .iter()
            .map(|reference| {
                (
                    (
                        reference.identity.writer_fence,
                        SegId(reference.identity.object_id),
                    ),
                    *reference,
                )
            })
            .collect();
        vset_state.wedge.hydration += fetched.len() as u64;
        vset_state.best_record = Some(record.clone());
        vset_state
            .record_writes
            .insert(record.seq, (record.fence, record.sync_covered_through));
        vset_state.record_segments.insert(
            record.seq,
            segments
                .iter()
                .map(|(segment, _, _)| (fence, *segment))
                .collect(),
        );
        vset_state
            .operations
            .finish_mutation(MutationOwner::Hydration);
        let hydration_waiters = std::mem::take(&mut vset_state.hydration_waiters);
        let mutation_waiters = std::mem::take(&mut vset_state.mutation_waiters);
        host.counters.records_written += 1;
        host.counters.hydrate_fills += fetched.len() as u64;
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
    if authorized {
        let mut names = removed
            .as_ref()
            .into_iter()
            .flat_map(|vset_state| {
                vset_state
                    .segment_blobs
                    .iter()
                    .map(|(fence, segment, _)| layout::segment_blob(vset, *fence, *segment))
            })
            .collect::<Vec<_>>();
        if let Some(vset_state) = removed.as_ref() {
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
        }
        if let Ok(blobs) = Blobs::scan(world).await {
            names.extend(blobs.into_iter().filter_map(|blob| {
                let belongs_to_released_source = match layout::parse_blob(&blob.name) {
                    Some(
                        layout::BlobName::Journal {
                            vset: owner, fence, ..
                        }
                        | layout::BlobName::Segment {
                            vset: owner, fence, ..
                        }
                        | layout::BlobName::Leaf {
                            vset: owner, fence, ..
                        },
                    ) => owner == vset && fence < release_fence,
                    Some(layout::BlobName::BaseLeaf { vset: owner, .. }) => owner == vset,
                    Some(layout::BlobName::Handoff { vset: owner }) => owner == vset,
                    Some(layout::BlobName::ReplicaSpool { .. }) | None => false,
                };
                belongs_to_released_source.then_some(blob.name)
            }));
        }
        names.sort_unstable();
        names.dedup();
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
    fn inline_vmstate_is_validated_and_reconstructs_block_checksums() {
        let bytes = 77_u64.to_le_bytes().to_vec();
        let loaded = decode_inline_vmstate(bytes.clone(), 8, Gen(14), 77)
            .expect("valid inline VMM snapshot");
        assert_eq!(loaded.bytes, bytes);
        let key = BlockKey {
            space: BlockSpace::Vmm,
            volume: 0,
            block: 0,
        };
        let mut padded = vec![0; crate::types::page_size()];
        padded[..8].copy_from_slice(&77_u64.to_le_bytes());
        assert_eq!(
            loaded.block_checksums.get(&key),
            Some(&(Gen(14), checksum64(&padded)))
        );
        assert!(decode_inline_vmstate(bytes.clone(), 7, Gen(14), 77).is_none());
        assert!(decode_inline_vmstate(bytes, 8, Gen(14), 78).is_none());
    }

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
