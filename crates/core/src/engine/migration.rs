use std::collections::BTreeMap;
use std::rc::Rc;

use blockd_exec::channel::{OneReceiver, Sender, TrySendError, bounded, oneshot};
use blockd_exec::{Either, TaskSet, delay, select2, timeout, yield_now};

use super::capture::{
    capture_migration, recovery_files, write_record_copies, write_record_copies_with_archive,
};
use super::ctx::{HostCtx, VolumeCtx};
use super::keyed_queue::KeyedQueue;
use super::recovery_policy::record_verdict;
use super::state::MutationOwner;
use super::store_retry;
use super::{SharedHost, VolumeState, replica_message, replicate_latest};
use crate::blx::{
    BlockKey, BlockSpace, BlxEntry, BlxFooter, BlxObject, EntryKind, NamespaceKind,
    replace_state_block,
};
use crate::format::{Dec, DecodeError, Enc, checksum64, open_frame, seal_frame};
use crate::head::HeadRecord;
use crate::journal::{
    JournalRecord, MigrationArchiveClosure, MigrationSource, RecordKind, VolumeKind,
};
use crate::layout;
use crate::manifest::{ObjectIdentity, ObjectRef};
use crate::page_file::{PageBatchBuilder, PageFileLoc, open_entry};
use crate::protocol::{AdminError, AdminEvent, AdminResult, AdminSuccess, PeerMsg, Verdict};
use crate::types::{Gen, HostId, ObjectId, VolumeId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

const MAGIC_HANDOFF: u32 = u32::from_le_bytes(*b"BHF1");
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
        from: crate::types::HostId,
        io: crate::protocol::PeerRequestId,
        volume: VolumeId,
        replica_assignment_epoch: Option<u64>,
        fence: u64,
        object: crate::types::ObjectId,
        offset: u32,
        len: u32,
    },
}

enum PeerLifecycleRequest {
    MigrateOffer {
        from: HostId,
        volume: VolumeId,
        record: Vec<u8>,
        vmstate: Option<Vec<u8>>,
    },
    Released {
        from: HostId,
        volume: VolumeId,
        release_fence: u64,
    },
}

struct Handoff {
    volume: VolumeId,
    to: HostId,
}

struct InboundLease {
    state: SharedHost,
    volume: VolumeId,
}

struct InboundMigration<W> {
    state: SharedHost,
    world: Rc<W>,
    from: HostId,
    volume: VolumeId,
    record: Vec<u8>,
    inline_vmstate: Option<Vec<u8>>,
}

struct MigrationLease {
    state: SharedHost,
    volume: VolumeId,
    run_generation: u64,
    active: bool,
}

struct HydrationLease {
    state: SharedHost,
    volume: VolumeId,
    run_generation: u64,
    active: bool,
}

impl HydrationLease {
    fn new(state: &SharedHost, volume: VolumeId, run_generation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            volume,
            run_generation,
            active: true,
        }
    }

    fn commit(mut self) {
        self.state.borrow_mut().schedule_volume(self.volume);
        self.active = false;
    }
}

impl Drop for HydrationLease {
    fn drop(&mut self) {
        if self.active {
            finish_hydration(&self.state, self.volume, self.run_generation);
        }
    }
}

impl MigrationLease {
    fn new(state: &SharedHost, volume: VolumeId, run_generation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            volume,
            run_generation,
            active: true,
        }
    }

    fn commit(mut self) {
        self.state.borrow_mut().schedule_volume(self.volume);
        self.active = false;
    }
}

impl Drop for MigrationLease {
    fn drop(&mut self) {
        if self.active
            && let Some(volume_state) = self
                .state
                .borrow_mut()
                .volumes
                .get_mut(&self.volume)
                .filter(|volume_state| volume_state.run_generation == self.run_generation)
        {
            volume_state.operations.finish_migration();
        }
        self.state.borrow_mut().schedule_volume(self.volume);
    }
}

impl Drop for InboundLease {
    fn drop(&mut self) {
        self.state
            .borrow_mut()
            .inbound_migrations
            .remove(&self.volume);
    }
}

impl Handoff {
    fn encode(&self) -> Vec<u8> {
        let mut encoder = Enc::new();
        encoder.u16(1);
        encoder.u64(self.volume.0);
        encoder.u32(self.to.get());
        seal_frame(MAGIC_HANDOFF, &encoder.finish())
    }

    fn decode(volume: VolumeId, bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_HANDOFF, bytes)?;
        let mut decoder = Dec::new(payload);
        if decoder.u16()? != 1 || decoder.u64()? != volume.0 {
            return Err(DecodeError);
        }
        let to = HostId::new(decoder.u32()?);
        decoder.finish()?;
        Ok(Self { volume, to })
    }
}

impl VolumeState {
    /// Encode one exact post-copy offer from a logical record and the live page
    /// lookup state. Ordinary journals may omit this index because local
    /// recovery reconstructs it from BLX files; a migration destination needs
    /// it to demand-fetch every page from the source.
    fn encode_migration_offer(&self, volume: VolumeId, record: &JournalRecord) -> Vec<u8> {
        let mut offered = record.clone();
        offered.runtime_page_index = self.page_locs.clone();
        offered.encode_migration_state(
            volume,
            &self.block_checksums,
            &MigrationArchiveClosure {
                objects: self.archive_objects.clone(),
                base: self.archive_base,
            },
        )
    }
}

pub(super) fn decode_handoff(volume: VolumeId, bytes: &[u8]) -> Option<HostId> {
    Handoff::decode(volume, bytes)
        .ok()
        .map(|handoff| handoff.to)
}

pub(super) fn encode_handoff(volume: VolumeId, to: HostId) -> Vec<u8> {
    Handoff { volume, to }.encode()
}

#[allow(clippy::too_many_lines)]
pub async fn migrate_out<W>(
    state: SharedHost,
    world: Rc<W>,
    volume: VolumeId,
    to: HostId,
) -> Option<AdminResult>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world)
        .volume(volume)
        .migrate_to(to)
        .await
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    pub(super) async fn migrate_to(&self, to: HostId) -> Option<AdminResult> {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        let to = state
            .borrow()
            .config
            .cluster_placement
            .as_ref()
            .and_then(|placement| placement.roster.contains(&to).then_some(to))?;
        let (run_generation, publication) = loop {
            enum Decision {
                Invalid,
                Hydrating(OneReceiver<bool>),
                Reserved {
                    run_generation: u64,
                    publication: Option<OneReceiver<()>>,
                },
            }
            let decision = {
                let mut host = state.borrow_mut();
                let Some(volume_state) = host.volumes.get_mut(&volume) else {
                    return Some(Err(AdminError::Rejected));
                };
                let allowed = volume_state.ready
                    && volume_state.outbound.is_none()
                    && !volume_state.operations.migration_running()
                    && !volume_state.operations.guest_resume_pending();
                if !allowed {
                    Decision::Invalid
                } else if volume_state.peer_source.is_some_and(|source| source == to)
                    || (volume_state.peer_source.is_some()
                        && volume_state.page_locs.values().any(|(_, location)| {
                            location.base == 0 && location.fence < volume_state.fence
                        }))
                {
                    // A local tail is not enough to return to the prior source:
                    // wait until Released/ReleasedAck proves that exact host
                    // removed its old resident state. Migration to another host
                    // still needs only the normal hydration safety boundary.
                    let (wake, wait) = oneshot();
                    volume_state.hydration_waiters.push(wake);
                    host.schedule_volume(volume);
                    Decision::Hydrating(wait)
                } else {
                    assert!(volume_state.operations.start_migration());
                    let publication = volume_state.operations.publication_running().then(|| {
                        let (wake, wait) = oneshot();
                        volume_state.publication_waiters.push(wake);
                        wait
                    });
                    Decision::Reserved {
                        run_generation: volume_state.run_generation,
                        publication,
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
                    run_generation,
                    publication,
                } => break (run_generation, publication),
            }
        };
        let lease = MigrationLease::new(&state, volume, run_generation);
        if let Some(wait) = publication
            && wait.await.is_err()
        {
            return Some(Err(AdminError::Unavailable));
        }
        let Some(kind) = state
            .borrow()
            .volumes
            .get(&volume)
            .filter(|volume_state| volume_state.run_generation == run_generation)
            .map(|volume_state| volume_state.config.kind)
        else {
            return Some(Err(AdminError::Unavailable));
        };

        // Do not enter the pause unless a passive has already been assigned. No
        // network operation belongs between capture_migration and commit below.
        if state
            .borrow()
            .volumes
            .get(&volume)
            .filter(|volume_state| volume_state.run_generation == run_generation)
            .and_then(|volume_state| volume_state.stash_assignment)
            .is_none()
        {
            return Some(Err(AdminError::Unavailable));
        }

        let mut paused = None;
        let mut offered_vmstate = None;
        let record = if kind == VolumeKind::Memory {
            let Some((record, guard, vmstate)) =
                capture_migration(Rc::clone(&state), Rc::clone(&world), volume).await
            else {
                return Some(Err(AdminError::Unavailable));
            };
            paused = Some(guard);
            offered_vmstate = Some(vmstate);
            record
        } else {
            let Some(record) = state.borrow().volumes[&volume].best_record.clone() else {
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
            replicate_latest(Rc::clone(&state), Rc::clone(&world), volume).await;
            let protected = state
                .borrow()
                .volumes
                .get(&volume)
                .filter(|volume_state| volume_state.run_generation == run_generation)
                .is_some_and(|volume_state| {
                    volume_state.peer_committed.is_some_and(|committed| {
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
        let handoff_name = layout::handoff_blob(volume);
        let handoff_bytes = encode_handoff(volume, to);
        // A volume can return to a host before an older release has removed that
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
            let volume_state = host
                .volumes
                .get_mut(&volume)
                .filter(|volume_state| volume_state.run_generation == run_generation)?;
            volume_state.ready = false;
            volume_state.outbound = Some(to);
        }
        if let Some(paused) = paused.take() {
            paused.disarm();
        }
        let encoded = {
            let host = state.borrow();
            let volume_state = host
                .volumes
                .get(&volume)
                .filter(|volume_state| volume_state.run_generation == run_generation)?;
            volume_state.encode_migration_offer(volume, &record)
        };
        let inline_vmstate = offered_vmstate.filter(|vmstate| {
            encoded
                .len()
                .checked_add(vmstate.len())
                .and_then(|size| size.checked_add(32))
                .is_some_and(|size| size <= crate::peer::MAX_PEER_PAYLOAD as usize)
        });
        loop {
            if ensure_outbound_head(&state, world.as_ref(), volume, to, record.fence).await
                && offer_once(
                    &state,
                    world.as_ref(),
                    volume,
                    to,
                    record.fence,
                    encoded.clone(),
                    inline_vmstate.clone(),
                )
                .await
            {
                break;
            }
            let retry = state.borrow().config.backup_retry;
            delay(retry).await;
        }
        lease.commit();
        Some(Ok(AdminSuccess::MigratedOut { volume }))
    }
}

async fn offer_once<W: Peers>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    to: HostId,
    offer_fence: u64,
    bytes: Vec<u8>,
    vmstate: Option<Vec<u8>>,
) -> bool {
    let client = state.borrow().peer_client.clone();
    client
        .offer_migration_once(world, to, volume, offer_fence, bytes, vmstate)
        .await
}

pub async fn reoffer_outbound<W: Peers + Store>(state: SharedHost, world: Rc<W>, volume: VolumeId) {
    HostCtx::new(state, world).volume(volume).reoffer().await;
}

impl<W: Peers + Store> VolumeCtx<W> {
    pub(super) async fn reoffer(&self) {
        let state = self.host().state();
        let world = self.host().world();
        let volume = self.id();
        let Some((to, offer_fence, record)) =
            state
                .borrow()
                .volumes
                .get(&volume)
                .and_then(|volume_state| {
                    let record = volume_state.best_record.as_ref()?;
                    Some((
                        volume_state.outbound?,
                        record.fence,
                        volume_state.encode_migration_offer(volume, record),
                    ))
                })
        else {
            return;
        };
        if ensure_outbound_head(state, world.as_ref(), volume, to, offer_fence).await {
            let _ = offer_once(state, world.as_ref(), volume, to, offer_fence, record, None).await;
        }
        state.borrow_mut().schedule_volume(volume);
    }
}

async fn ensure_outbound_head<W: Store>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    destination: HostId,
    ownership_fence: u64,
) -> bool {
    let Ok(Some((version, bytes))) = Store::get(world, &layout::head_key(volume)).await else {
        return false;
    };
    let Ok(head) = HeadRecord::decode(volume, &bytes) else {
        return false;
    };
    if head.holder == destination {
        return true;
    }
    if head.holder != state.borrow().config.host {
        return false;
    }
    if head.fence == ownership_fence {
        return true;
    }
    if head.fence != 0 {
        return false;
    }
    let finalized = HeadRecord {
        fence: ownership_fence,
        ..head
    };
    Store::put_cas(
        world,
        layout::head_key(volume),
        Some(version),
        finalized.encode(),
    )
    .await
    .is_ok()
}

pub async fn peer_source<W>(state: SharedHost, world: Rc<W>)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world).peer_source().await;
}

impl<W> HostCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    pub(super) async fn peer_source(self) {
        let state = Rc::clone(self.state());
        let world = Rc::clone(self.world());
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
            .collect::<Vec<Sender<(crate::types::HostId, PeerMsg)>>>();
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
        let mut lifecycle_handlers = TaskSet::new();
        let mut lifecycle_volumes = BTreeMap::new();
        let mut lifecycle = KeyedQueue::new();
        let mut ingress_open = true;
        let mut ingress_batch = 0;
        loop {
            while let Some((volume, request)) = lifecycle.start_next(PEER_LIFECYCLE_CONCURRENCY) {
                let state = Rc::clone(&state);
                let world = Rc::clone(&world);
                let child = lifecycle_handlers.spawn(async move {
                    handle_peer_lifecycle(state, world, request).await;
                });
                lifecycle_volumes.insert(child, volume);
            }
            if !ingress_open && lifecycle.is_idle() {
                return;
            }
            let event = if ingress_open {
                select2(lifecycle_handlers.next_done(), Peers::recv(world.as_ref())).await
            } else {
                Either::First(lifecycle_handlers.next_done().await)
            };
            let (from, message) = match event {
                Either::First(Some(child)) => {
                    let volume = lifecycle_volumes
                        .remove(&child)
                        .expect("peer lifecycle child tracked");
                    lifecycle.complete(volume);
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
                PeerMsg::MigrateOffer {
                    volume,
                    record,
                    vmstate,
                } => {
                    if lifecycle.pending_len() < PEER_LIFECYCLE_QUEUE_CAPACITY
                        && !lifecycle.contains_key(volume)
                    {
                        lifecycle.push(
                            volume,
                            PeerLifecycleRequest::MigrateOffer {
                                from,
                                volume,
                                record,
                                vmstate,
                            },
                        );
                    }
                }
                PeerMsg::MigrateAccept {
                    volume,
                    offer_fence,
                } => {
                    resolve_migration_accept(&state, from, volume, offer_fence);
                }
                PeerMsg::FetchRange {
                    io,
                    volume,
                    replica_assignment_epoch,
                    fence,
                    object,
                    offset,
                    len,
                } => {
                    let request = PeerStorageRequest::Range {
                        from,
                        io,
                        volume,
                        replica_assignment_epoch,
                        fence,
                        object,
                        offset,
                        len,
                    };
                    let route = peer_storage_route(from, volume);
                    let _ = storage_routes[route].try_send(request);
                }
                PeerMsg::Page { io, bytes } => {
                    state.borrow_mut().peer_client.resolve_page(io, from, bytes);
                }
                PeerMsg::Released {
                    volume,
                    release_fence,
                } => {
                    if lifecycle.pending_len() < PEER_LIFECYCLE_QUEUE_CAPACITY
                        && !lifecycle.contains_key(volume)
                    {
                        lifecycle.push(
                            volume,
                            PeerLifecycleRequest::Released {
                                from,
                                volume,
                                release_fence,
                            },
                        );
                    }
                }
                PeerMsg::ReleasedAck {
                    volume,
                    release_fence,
                } => {
                    let waiters = state
                        .borrow_mut()
                        .volumes
                        .get_mut(&volume)
                        .filter(|volume_state| {
                            volume_state.peer_source == Some(from)
                                && volume_state.fence == release_fence
                        })
                        .map(|volume_state| {
                            volume_state.peer_source = None;
                            volume_state.peer_source_offer_fence = None;
                            std::mem::take(&mut volume_state.hydration_waiters)
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
}

fn resolve_migration_accept(state: &SharedHost, from: HostId, volume: VolumeId, offer_fence: u64) {
    // The destination can hydrate and release the source before a delayed
    // MigrateAccept reaches this actor. The peer client still binds an active
    // retry to the exact authenticated host, volume, and offer fence, so local
    // volume residency is neither necessary nor valid authorization here.
    state
        .borrow()
        .peer_client
        .resolve_migration(volume, from, offer_fence);
}

async fn handle_peer_lifecycle<W>(state: SharedHost, world: Rc<W>, request: PeerLifecycleRequest)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    match request {
        PeerLifecycleRequest::MigrateOffer {
            from,
            volume,
            record,
            vmstate,
        } => {
            migrate_in(state, world, from, volume, record, vmstate).await;
        }
        PeerLifecycleRequest::Released {
            from,
            volume,
            release_fence,
        } => {
            release_source(&state, world.as_ref(), from, volume, release_fence).await;
        }
    }
}

fn peer_storage_route(from: HostId, volume: VolumeId) -> usize {
    usize::try_from(
        (u64::from(from.get()).wrapping_mul(31) ^ volume.0) % PEER_STORAGE_SHARDS as u64,
    )
    .expect("peer storage route fits")
}

fn replica_route((from, volume, assignment_epoch): (HostId, VolumeId, u64)) -> usize {
    usize::try_from(
        (u64::from(from.get()).wrapping_mul(31) ^ volume.0.wrapping_mul(17) ^ assignment_epoch)
            % REPLICA_ROUTE_SHARDS as u64,
    )
    .expect("replica route fits")
}

async fn handle_peer_storage<W: Blobs + Peers>(
    state: SharedHost,
    world: &W,
    request: PeerStorageRequest,
) {
    let (from, volume) = match &request {
        PeerStorageRequest::Range { from, volume, .. } => (*from, *volume),
    };
    let authorized = state
        .borrow()
        .volumes
        .get(&volume)
        .is_some_and(|volume_state| volume_state.outbound == Some(from));
    if authorized && let Some(volume_state) = state.borrow_mut().volumes.get_mut(&volume) {
        volume_state.wedge.served += 1;
    }
    match request {
        PeerStorageRequest::Range {
            io,
            replica_assignment_epoch,
            fence,
            object,
            offset,
            len,
            ..
        } => {
            let replica_bytes = replica_assignment_epoch.and_then(|assignment_epoch| {
                let host = state.borrow();
                let key = super::state::ReplicaKey {
                    source: from,
                    volume,
                    assignment_epoch,
                };
                let artifact = crate::protocol::ReplicaArtifact::Blx { fence, object };
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
                    &layout::blx_blob(volume, fence, object),
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
    }
}

fn replica_route_key(from: HostId, message: &PeerMsg) -> (HostId, VolumeId, u64) {
    let (volume, assignment_epoch) = match message {
        PeerMsg::ReplicaPut {
            volume,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaPutAck {
            volume,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaCommit {
            volume,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaCommitAck {
            volume,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaStatus {
            volume,
            assignment_epoch,
        }
        | PeerMsg::ReplicaStatusReply {
            volume,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaRelease {
            volume,
            assignment_epoch,
            ..
        }
        | PeerMsg::ReplicaReleaseAck {
            volume,
            assignment_epoch,
            ..
        } => (*volume, *assignment_epoch),
        _ => unreachable!("non-replica message"),
    };
    (from, volume, assignment_epoch)
}

#[allow(clippy::too_many_lines)]
pub(super) async fn migrate_in<W>(
    state: SharedHost,
    world: Rc<W>,
    from: HostId,
    volume: VolumeId,
    bytes: Vec<u8>,
    inline_vmstate: Option<Vec<u8>>,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    InboundMigration {
        state,
        world,
        from,
        volume,
        record: bytes,
        inline_vmstate,
    }
    .run()
    .await;
}

impl<W> InboundMigration<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    async fn run(self) {
        let Self {
            state,
            world,
            from,
            volume,
            record: bytes,
            inline_vmstate,
        } = self;
        let Ok((offered, offered_checksums, offered_archive)) =
            JournalRecord::decode_migration_state(volume, &bytes)
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
        let existing = state.borrow().volumes.get(&volume).map(|existing| {
            let duplicate = existing.peer_source == Some(from)
                && existing.peer_source_offer_fence == Some(offered.fence);
            let replaceable_stale_outbound =
                !existing.ready && existing.outbound.is_some() && offered.fence > existing.fence;
            (duplicate, replaceable_stale_outbound)
        });
        let mut replacing_stale_outbound = false;
        if let Some((duplicate, is_replaceable_stale_outbound)) = existing {
            if duplicate {
                Peers::send(
                    world.as_ref(),
                    from,
                    PeerMsg::MigrateAccept {
                        volume,
                        offer_fence: offered.fence,
                    },
                )
                .await;
            }
            if !duplicate
                && !is_replaceable_stale_outbound
                && offer_was_superseded(&state, world.as_ref(), from, volume, offered.fence).await
            {
                Peers::send(
                    world.as_ref(),
                    from,
                    PeerMsg::MigrateAccept {
                        volume,
                        offer_fence: offered.fence,
                    },
                )
                .await;
            }
            if !is_replaceable_stale_outbound {
                return;
            }
            replacing_stale_outbound = true;
        }
        if !state.borrow_mut().inbound_migrations.insert(volume) {
            return;
        }
        let _lease = InboundLease {
            state: Rc::clone(&state),
            volume,
        };
        // Reject a delayed offer before fetching its VMM image or writing any
        // destination state. The final head CAS below remains the fence, but
        // this preflight keeps a backlog of superseded retries from starving
        // the source's current tenure.
        if !current_offer_matches_head(&state, world.as_ref(), from, volume, offered.fence).await {
            return;
        }
        if replacing_stale_outbound {
            let handoff = layout::handoff_blob(volume);
            if Blobs::delete(world.as_ref(), &handoff).await.is_err() {
                state
                    .borrow_mut()
                    .fail("superseded migration handoff cleanup failed");
                return;
            }
            state.borrow_mut().forget_blobs(std::iter::once(&handoff));
        }
        let verdict = record_verdict(&offered);
        let vmstate = if offered.config.kind == VolumeKind::Memory {
            let Verdict::Resume { .. } = verdict else {
                return;
            };
            match offered.kind {
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
                            volume,
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
            }
        } else {
            if inline_vmstate.is_some() || !matches!(offered.kind, RecordKind::Commit) {
                return;
            }
            None
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
        let Some(fence) = available_inbound_fence(&state, volume, offered.fence) else {
            return;
        };
        let run_generation = state.borrow_mut().allocate_run_generation();
        let local_vmm = if let Some(loaded) = vmstate.as_ref() {
            let mut builder = PageBatchBuilder::new_with_checksums(
                offered.config.kind,
                volume,
                fence,
                ObjectId(0),
                0,
                offered.post_state_checksum,
                offered.post_state_checksum,
            );
            for (key, padded) in crate::blx::vmm_snapshot_blocks(&loaded.bytes) {
                let Some(&(generation, _)) = loaded.block_checksums.get(&key) else {
                    return;
                };
                builder.add_vmm_block(key.block, generation, &padded);
            }
            builder.finish()
        } else {
            Vec::new()
        };
        let local_vmm_refs = local_vmm
            .iter()
            .map(|(blx, bytes, _)| {
                BlxObject::open(bytes)
                    .map(|object| ((*blx), ObjectRef::from_blx(&object)))
                    .ok()
            })
            .collect::<Option<Vec<_>>>();
        let Some(local_vmm_refs) = local_vmm_refs else {
            return;
        };
        let reservations = local_vmm
            .iter()
            .map(|(blx, bytes, _)| (layout::blx_blob(volume, fence, *blx), bytes.len() as u64))
            .collect::<Vec<_>>();
        if !state.borrow_mut().try_reserve_blobs(&reservations) {
            return;
        }
        for (blx, bytes, _) in &local_vmm {
            let name = layout::blx_blob(volume, fence, *blx);
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
        if !write_record_copies_with_archive(
            &state,
            world.as_ref(),
            volume,
            &record,
            &offered_checksums,
            &offered_archive,
        )
        .await
        {
            state
                .borrow_mut()
                .fail("inbound migration journal write failed");
            return;
        }
        if !AdminIo::prepare_recovered_volume(world.as_ref(), volume, offered.config).await {
            return;
        }
        if let Some(loaded) = vmstate.as_ref()
            && GuestMem::install_vmstate(world.as_ref(), volume, loaded.bytes.clone())
                .await
                .is_err()
        {
            return;
        }
        let stale_resident = {
            let mut host = state.borrow_mut();
            let replaceable_stale_outbound = host.volumes.get(&volume).is_some_and(|existing| {
                !existing.ready && existing.outbound.is_some() && offered.fence > existing.fence
            });
            if host.volumes.contains_key(&volume) && !replaceable_stale_outbound {
                return;
            }
            let mut incoming = VolumeState::fresh(record.config, run_generation);
            incoming.ready = false;
            incoming.peer_source = Some(from);
            incoming.peer_source_offer_fence = Some(offered.fence);
            incoming.fence = fence;
            if let Verdict::Resume { epoch, .. } = verdict {
                incoming.epoch = epoch;
            }
            incoming.mutation_seq = record.capture_seq;
            incoming.state_checksum = record.post_state_checksum;
            incoming.block_checksums = offered_checksums;
            incoming.install_archive_closure(
                volume,
                &offered_archive.objects,
                offered_archive.base,
            );
            incoming.local_covered_through = record.sync_covered_through;
            incoming.sync_ack_through = record.sync_covered_through;
            // The source does not offer this cut until its passive has committed
            // it. Preserve that inherited protection watermark while the
            // destination hydrates and establishes its own passive copy.
            incoming.peer_committed_through = record.sync_covered_through;
            incoming.next_seq = 1;
            incoming.next_object_id =
                u64::try_from(local_vmm.len()).expect("VMM object count fits u64");
            incoming.page_locs = record.runtime_page_index.clone();
            incoming.next_gen = incoming
                .block_checksums
                .values()
                .map(|(generation, _)| generation.0.saturating_add(1))
                .max()
                .unwrap_or(0);
            incoming
                .blx_blobs
                .extend(local_vmm.iter().map(|(blx, bytes, _)| {
                    (
                        ObjectIdentity::volume(volume, fence, blx.0),
                        bytes.len() as u64,
                    )
                }));
            incoming
                .blx_refs
                .extend(record.files.iter().filter_map(|reference| {
                    (reference.identity.namespace_kind == NamespaceKind::Volume
                        && reference.identity.namespace_id == volume.0)
                        .then_some((reference.identity, *reference))
                }));
            incoming.vmm_blx_files.extend(
                local_vmm_refs
                    .iter()
                    .map(|(blx, _)| ObjectIdentity::volume(volume, fence, blx.0)),
            );
            incoming.best_record = Some(record.clone());
            if matches!(verdict, Verdict::Resume { .. }) {
                incoming.pinned = Some(record.clone());
            }
            incoming
                .record_writes
                .insert(record.seq, (fence, record.sync_covered_through));
            incoming.record_blx_files.insert(
                record.seq,
                local_vmm_refs
                    .iter()
                    .map(|(blx, _)| ObjectIdentity::volume(volume, fence, blx.0))
                    .collect(),
            );
            // A non-duplicate inbound install starts a new guest-memory
            // lifetime. Cache entries can outlive the old volume record after
            // release or restart, and retaining them would make faults treat
            // stale pages as resident instead of following the offered index.
            let stale_resident = host.cache.purge_volume(volume);
            host.volumes.insert(volume, incoming);
            host.counters.records_written += 1;
            stale_resident
        };
        for page in stale_resident {
            if GuestMem::evict(world.as_ref(), page).await.is_err() {
                state
                    .borrow_mut()
                    .fail("stale page eviction failed during inbound migration");
                return;
            }
        }
        if !claim_migrated_head(
            &state,
            world.as_ref(),
            from,
            volume,
            run_generation,
            &offered,
        )
        .await
        {
            state.borrow_mut().volumes.remove(&volume);
            return;
        }
        if matches!(verdict, Verdict::Resume { .. })
            && GuestMem::resume(world.as_ref(), volume, None)
                .await
                .is_err()
        {
            state.borrow_mut().fail("migrated guest resume failed");
            return;
        }
        AdminIo::emit_admin_event(
            world.as_ref(),
            AdminEvent::VolumeMigratedIn { volume, verdict },
        )
        .await;
        Peers::send(
            world.as_ref(),
            from,
            PeerMsg::MigrateAccept {
                volume,
                offer_fence: offered.fence,
            },
        )
        .await;
    }
}

async fn offer_was_superseded<W: Store>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    volume: VolumeId,
    offer_fence: u64,
) -> bool {
    let Ok(Some((version, bytes))) =
        store_retry::get(state, world, &layout::head_key(volume)).await
    else {
        return false;
    };
    let Ok(head) = HeadRecord::decode(volume, &bytes) else {
        return false;
    };
    let effective_fence = if head.fence == 0 { version } else { head.fence };
    head.holder != source && effective_fence > offer_fence
}

async fn current_offer_matches_head<W: Store>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    volume: VolumeId,
    offer_fence: u64,
) -> bool {
    let Ok(Some((version, bytes))) =
        store_retry::get(state, world, &layout::head_key(volume)).await
    else {
        return false;
    };
    let Ok(head) = HeadRecord::decode(volume, &bytes) else {
        return false;
    };
    let local = state.borrow().config.host;
    let effective_fence = if head.fence == 0 { version } else { head.fence };
    (head.holder == source && effective_fence == offer_fence)
        // A prior attempt may have committed this destination's provisional
        // claim and then lost the response or crashed before finalization.
        // Retrying its durable inbound journal is the only path that can
        // finish that exact local claim.
        || (head.holder == local && head.fence == 0)
}

pub(super) fn available_inbound_fence(
    state: &SharedHost,
    volume: VolumeId,
    offered: u64,
) -> Option<u64> {
    let occupied = state.borrow().local_artifact_fences(volume);
    offered
        .max(occupied.iter().copied().max().unwrap_or(0))
        .checked_add(1)
}

#[allow(clippy::too_many_lines)]
async fn claim_migrated_head<W>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    volume: VolumeId,
    run_generation: u64,
    offered: &JournalRecord,
) -> bool
where
    W: Blobs + Store + AdminIo,
{
    let local = state.borrow().config.host;
    let retry = state.borrow().config.backup_retry;
    let Some(local_stash) = super::replica::initial_stash(state, volume) else {
        return false;
    };
    let (claim_fence, manifest, stash, retired_stashes) = loop {
        let current = match store_retry::get(state, world, &layout::head_key(volume)).await {
            Ok(Some(current)) => current,
            Ok(None) | Err(StoreError::TooLarge | StoreError::Fault(_)) => return false,
        };
        let Ok(head) = HeadRecord::decode(volume, &current.1) else {
            return false;
        };
        if head.holder != local && head.holder != source {
            return false;
        }
        // An offer fence names one exact ownership tenure, not merely one
        // host allocation. If the same source becomes holder again later, a
        // delayed offer from its earlier tenure must not reclaim the head.
        if head.holder == source {
            // A newly created head retains the provisional zero in its body;
            // its object-store version is the holder's effective fence.
            let source_fence = if head.fence == 0 {
                current.0
            } else {
                head.fence
            };
            if source_fence != offered.fence {
                return false;
            }
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
            volume,
            holder: local,
            fence: 0,
            manifest: head.manifest,
            stash,
            retired_stashes: retired_stashes.clone(),
        };
        match Store::put_cas(
            world,
            layout::head_key(volume),
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
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. })) => {
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
    let checksums = state
        .borrow()
        .volumes
        .get(&volume)
        .filter(|volume_state| volume_state.run_generation == run_generation)
        .map(|volume_state| volume_state.block_checksums.clone())
        .unwrap_or_default();
    if !write_record_copies(state, world, volume, &claimed, &checksums).await {
        state
            .borrow_mut()
            .fail("claimed migration journal write failed");
        return false;
    }
    {
        let mut host = state.borrow_mut();
        let Some(volume_state) = host
            .volumes
            .get_mut(&volume)
            .filter(|volume_state| volume_state.run_generation == run_generation)
        else {
            return false;
        };
        volume_state.fence = claim_fence;
        volume_state.head_version = Some(claim_fence);
        volume_state.backed = manifest;
        volume_state.stash_assignment = stash;
        volume_state.retired_stashes.clone_from(&retired_stashes);
        volume_state.next_seq = claimed.seq.0 + 1;
        volume_state.best_record = Some(claimed.clone());
        volume_state
            .record_writes
            .insert(claimed.seq, (claimed.fence, claimed.sync_covered_through));
        host.counters.records_written += 1;
    }

    let finalized = HeadRecord {
        volume,
        holder: local,
        fence: claim_fence,
        manifest,
        stash,
        retired_stashes,
    };
    let head_version = loop {
        let current = match store_retry::get(state, world, &layout::head_key(volume)).await {
            Ok(Some(current)) => current,
            Ok(None) | Err(StoreError::TooLarge | StoreError::Fault(_)) => return false,
        };
        let Ok(head) = HeadRecord::decode(volume, &current.1) else {
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
            layout::head_key(volume),
            Some(current.0),
            finalized.encode(),
        )
        .await
        {
            Ok(version) => break version,
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. })) => {
                delay(retry).await;
            }
            Err(StoreError::TooLarge) => return false,
        }
    };
    let mut host = state.borrow_mut();
    let Some(volume_state) = host
        .volumes
        .get_mut(&volume)
        .filter(|volume_state| volume_state.run_generation == run_generation)
    else {
        return false;
    };
    volume_state.head_version = Some(head_version);
    volume_state.ready = true;
    host.schedule_volume(volume);
    true
}

pub async fn peer_fetch_page<W: Peers>(
    state: &SharedHost,
    world: &W,
    source: HostId,
    volume: VolumeId,
    location: crate::page_file::PageFileLoc,
) -> Option<Vec<u8>> {
    let client = state.borrow().peer_client.clone();
    client
        .fetch_page(world, source, volume, location, None)
        .await
}

pub async fn peer_fetch_replica_page<W: Peers>(
    state: &SharedHost,
    world: &W,
    passive: HostId,
    assignment_epoch: u64,
    volume: VolumeId,
    location: crate::page_file::PageFileLoc,
) -> Option<Vec<u8>> {
    let client = state.borrow().peer_client.clone();
    client
        .fetch_page(world, passive, volume, location, Some(assignment_epoch))
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
    for (key, padded) in crate::blx::vmm_snapshot_blocks(&bytes) {
        block_checksums.insert(key, (generation, checksum64(&padded)));
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
    volume: VolumeId,
    objects: &[ObjectRef],
    logical_length: u64,
) -> Option<MigratedVmstate> {
    let block_count = logical_length.div_ceil(crate::types::page_size() as u64);
    if block_count == 0 {
        return None;
    }
    let first_vmstate_key = BlockKey {
        space: BlockSpace::Vmm,
        block: 0,
    };
    let last_vmstate_key = BlockKey {
        block: u32::try_from(block_count.checked_sub(1)?).ok()?,
        ..first_vmstate_key
    };
    let mut indexed = Vec::new();
    for reference in objects {
        if reference.identity.namespace_kind != NamespaceKind::Volume
            || reference.identity.namespace_id != volume.0
            || reference.last_key < first_vmstate_key
            || last_vmstate_key < reference.first_key
        {
            continue;
        }
        let bytes = peer_fetch_page(
            state,
            world,
            source,
            volume,
            PageFileLoc {
                base: 0,
                fence: reference.identity.writer_fence,
                object: ObjectId(reference.identity.object_id),
                offset: reference.footer_offset,
                len: reference.footer_length,
            },
        )
        .await?;
        let footer = BlxFooter::open(&bytes).ok()?;
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
            volume,
            PageFileLoc {
                base: 0,
                fence: reference.identity.writer_fence,
                object: ObjectId(reference.identity.object_id),
                offset: entry.offset,
                len: entry.length,
            },
        )
        .await?;
        let BlxEntry::Data {
            key: found,
            generation,
            bytes,
        } = BlxEntry::open(&bytes).ok()?
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
pub async fn hydrate_tail<W>(state: SharedHost, world: Rc<W>, volume: VolumeId)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    HostCtx::new(state, world).volume(volume).hydrate().await;
}

impl<W> VolumeCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    pub(super) async fn hydrate(&self) {
        let state = Rc::clone(self.host().state());
        let world = Rc::clone(self.host().world());
        let volume = self.id();
        let Some((run_generation, source, fence, first_object, pages)) = ({
            let mut host = state.borrow_mut();
            host.volumes.get_mut(&volume).and_then(|volume_state| {
                let source = volume_state.peer_source?;
                if !volume_state.ready || volume_state.operations.mutation_blocked() {
                    return None;
                }
                let pages = volume_state
                    .page_locs
                    .iter()
                    .filter(|(_, (_, location))| {
                        location.base == 0 && location.fence < volume_state.fence
                    })
                    .take(HYDRATE_BATCH)
                    .map(|(&page, &entry)| (page, entry))
                    .collect::<Vec<_>>();
                if pages.is_empty() {
                    return Some((
                        volume_state.run_generation,
                        source,
                        volume_state.fence,
                        crate::types::ObjectId(volume_state.next_object_id),
                        pages,
                    ));
                }
                assert!(
                    volume_state
                        .operations
                        .try_start_mutation(MutationOwner::Hydration)
                );
                Some((
                    volume_state.run_generation,
                    source,
                    volume_state.fence,
                    crate::types::ObjectId(volume_state.next_object_id),
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
                    volume,
                    release_fence: fence,
                },
            )
            .await;
            return;
        }
        let lease = HydrationLease::new(&state, volume, run_generation);
        let mut workers = TaskSet::new();
        let mut outcomes = Vec::with_capacity(pages.len());
        for (page, (generation, location)) in pages {
            let (send, receive) = oneshot();
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            workers.spawn(async move {
                let bytes = timeout(
                    PEER_RETRY.saturating_mul(HYDRATE_FETCH_ATTEMPTS),
                    peer_fetch_page(&state, world.as_ref(), source, volume, location),
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
            let Some(raw) =
                open_entry(volume, &bytes)
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
            source_offer_fence,
        )) = ({
            let host = state.borrow();
            host.volumes
                .get(&volume)
                .filter(|volume_state| volume_state.run_generation == run_generation)
                .map(|volume_state| {
                    let generations = fetched
                        .iter()
                        .enumerate()
                        .map(|(offset, (page, _))| {
                            (
                                *page,
                                crate::types::Gen(
                                    volume_state.next_gen
                                        + u64::try_from(offset).expect("hydration batch fits u64"),
                                ),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                    let mut staged = VolumeState::fresh(volume_state.config, run_generation);
                    staged.fence = volume_state.fence;
                    staged.page_locs = volume_state.page_locs.clone();
                    staged.block_checksums = volume_state.block_checksums.clone();
                    staged.state_checksum = volume_state.state_checksum;
                    staged
                        .pending_tombstones
                        .clone_from(&volume_state.pending_tombstones);
                    staged.archived_memory_usable = volume_state.archived_memory_usable;
                    staged.archived_non_data_reset = volume_state.archived_non_data_reset;
                    staged.blx_refs = volume_state.blx_refs.clone();
                    staged.vmm_blx_files.clone_from(&volume_state.vmm_blx_files);
                    staged
                        .tombstone_blx_files
                        .clone_from(&volume_state.tombstone_blx_files);
                    (
                        generations,
                        staged,
                        crate::types::JournalSeq(volume_state.next_seq),
                        volume_state
                            .best_record
                            .as_ref()
                            .map_or(RecordKind::Commit, |record| record.kind),
                        volume_state.mutation_seq,
                        volume_state.local_covered_through,
                        volume_state.config,
                        volume_state.peer_source_offer_fence,
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
        let mut builder = PageBatchBuilder::new_with_checksums(
            config.kind,
            volume,
            fence,
            first_object,
            seq.0,
            pre_state_checksum,
            staged.state_checksum,
        );
        for (page, raw) in &fetched {
            builder.add(*page, generations[page], raw);
        }
        let blx_files = builder.finish();
        for (_, _, entries) in &blx_files {
            for &(page, generation, location) in entries {
                staged.page_locs.insert(page, (generation, location));
            }
        }
        let runtime_page_index = staged.page_locs.clone();
        let mut files = state
            .borrow()
            .volumes
            .get(&volume)
            .map(|volume| volume.blx_refs.clone())
            .unwrap_or_default();
        for (_blx, bytes, _) in &blx_files {
            let Ok(object) = BlxObject::open(bytes) else {
                state
                    .borrow_mut()
                    .fail("hydrated BLX object failed verification");
                return;
            };
            let reference = ObjectRef::from_blx(&object);
            files.insert(reference.identity, reference);
            staged.blx_refs.insert(reference.identity, reference);
        }
        let record = JournalRecord {
            config,
            seq,
            fence,
            kind,
            capture_seq,
            sync_covered_through: covered,
            post_state_checksum: staged.state_checksum,
            files: recovery_files(&staged),
            runtime_page_index: runtime_page_index.clone(),
            migrated_from: Some(MigrationSource {
                host: source,
                offer_fence: source_offer_fence,
            }),
        };
        let reservations = blx_files
            .iter()
            .map(|(blx, bytes, _)| (layout::blx_blob(volume, fence, *blx), bytes.len() as u64))
            .collect::<Vec<_>>();
        if !state.borrow_mut().try_reserve_blobs(&reservations) {
            return;
        }
        for (blx, bytes, _) in &blx_files {
            let name = layout::blx_blob(volume, fence, *blx);
            if super::blob::write(&state, world.as_ref(), name.clone(), bytes.clone())
                .await
                .is_err()
            {
                state.borrow_mut().fail("hydrated blx write failed");
                return;
            }
            state.borrow_mut().record_blob(name, bytes.len() as u64);
        }
        if !write_record_copies(
            &state,
            world.as_ref(),
            volume,
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
            let Some(volume_state) = host
                .volumes
                .get_mut(&volume)
                .filter(|volume_state| volume_state.run_generation == run_generation)
            else {
                return;
            };
            volume_state.next_gen = volume_state
                .next_gen
                .saturating_add(u64::try_from(fetched.len()).expect("hydration batch fits u64"));
            volume_state.next_seq = seq.0.saturating_add(1);
            volume_state.next_object_id = blx_files
                .iter()
                .map(|(blx, _, _)| blx.0.saturating_add(1))
                .max()
                .unwrap_or(volume_state.next_object_id)
                .max(volume_state.next_object_id);
            volume_state.page_locs = staged.page_locs;
            volume_state.block_checksums = staged.block_checksums;
            volume_state.state_checksum = staged.state_checksum;
            volume_state.pending_tombstones = staged.pending_tombstones;
            volume_state
                .blx_blobs
                .extend(blx_files.iter().map(|(blx, bytes, _)| {
                    (
                        ObjectIdentity::volume(volume, fence, blx.0),
                        bytes.len() as u64,
                    )
                }));
            volume_state.blx_refs = record
                .files
                .iter()
                .map(|reference| (reference.identity, *reference))
                .collect();
            volume_state.wedge.hydration += fetched.len() as u64;
            volume_state.best_record = Some(record.clone());
            volume_state
                .record_writes
                .insert(record.seq, (record.fence, record.sync_covered_through));
            volume_state.record_blx_files.insert(
                record.seq,
                blx_files
                    .iter()
                    .map(|(blx, _, _)| ObjectIdentity::volume(volume, fence, blx.0))
                    .collect(),
            );
            volume_state
                .operations
                .finish_mutation(MutationOwner::Hydration);
            let hydration_waiters = std::mem::take(&mut volume_state.hydration_waiters);
            let mutation_waiters = std::mem::take(&mut volume_state.mutation_waiters);
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
}

pub(super) fn finish_hydration(state: &SharedHost, volume: VolumeId, run_generation: u64) {
    let (hydration_waiters, mutation_waiters) = {
        let mut host = state.borrow_mut();
        let Some(volume_state) = host
            .volumes
            .get_mut(&volume)
            .filter(|volume_state| volume_state.run_generation == run_generation)
        else {
            return;
        };
        volume_state
            .operations
            .finish_mutation(MutationOwner::Hydration);
        let hydration_waiters = std::mem::take(&mut volume_state.hydration_waiters);
        let mutation_waiters = std::mem::take(&mut volume_state.mutation_waiters);
        host.schedule_volume(volume);
        (hydration_waiters, mutation_waiters)
    };
    for waiter in hydration_waiters {
        let _ = waiter.send(false);
    }
    for waiter in mutation_waiters {
        let _ = waiter.send(());
    }
}

async fn send_released_ack<W: Peers>(world: &W, to: HostId, volume: VolumeId, release_fence: u64) {
    Peers::send(
        world,
        to,
        PeerMsg::ReleasedAck {
            volume,
            release_fence,
        },
    )
    .await;
}

async fn release_source<W: Blobs + Peers + GuestMem + AdminIo>(
    state: &SharedHost,
    world: &W,
    from: HostId,
    volume: VolumeId,
    release_fence: u64,
) {
    if state
        .borrow()
        .released_migration_fences
        .get(&volume)
        .is_some_and(|released| *released >= release_fence)
    {
        send_released_ack(world, from, volume, release_fence).await;
        return;
    }
    let authorized = state
        .borrow()
        .volumes
        .get(&volume)
        .is_none_or(|volume_state| {
            volume_state.outbound == Some(from) && release_fence > volume_state.fence
        });
    let (removed, resident) = if authorized {
        let mut host = state.borrow_mut();
        let removed = host.volumes.remove(&volume);
        let resident = host.cache.purge_volume(volume);
        (removed, resident)
    } else {
        (None, Vec::new())
    };
    if authorized {
        let mut names = removed
            .as_ref()
            .into_iter()
            .flat_map(|volume_state| {
                volume_state.blx_blobs.iter().map(|(identity, _)| {
                    layout::blx_blob(volume, identity.writer_fence, ObjectId(identity.object_id))
                })
            })
            .collect::<Vec<_>>();
        if let Some(volume_state) = removed.as_ref() {
            for (&seq, &(fence, _)) in &volume_state.record_writes {
                names.push(layout::journal_blob(volume, fence, seq));
                names.push(layout::journal_mirror_blob(volume, fence, seq));
            }
        }
        if let Ok(blobs) = Blobs::scan(world).await {
            names.extend(blobs.into_iter().filter_map(|blob| {
                let belongs_to_released_source = match layout::parse_blob(&blob.name) {
                    Some(
                        layout::BlobName::Journal {
                            volume: owner,
                            fence,
                            ..
                        }
                        | layout::BlobName::Blx {
                            volume: owner,
                            fence,
                            ..
                        },
                    ) => owner == volume && fence < release_fence,
                    Some(layout::BlobName::Handoff { volume: owner }) => owner == volume,
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
        state
            .borrow_mut()
            .released_migration_fences
            .entry(volume)
            .and_modify(|released| *released = (*released).max(release_fence))
            .or_insert(release_fence);
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
        AdminIo::volume_released(world, volume).await;
        send_released_ack(world, from, volume, release_fence).await;
    }
}

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use blockd_exec::{FaultConfig, simulation_scope};

    use super::*;
    use crate::engine::HostState;
    use crate::hostmeta::HostConfig;
    use crate::journal::VolumeConfig;

    struct AcceptAfterSourceRelease {
        state: SharedHost,
        offer_fence: u64,
    }

    impl Peers for AcceptAfterSourceRelease {
        async fn send(&self, to: HostId, message: PeerMsg) {
            let PeerMsg::MigrateOffer { volume, .. } = message else {
                return;
            };
            assert!(self.state.borrow().volumes.is_empty());
            resolve_migration_accept(&self.state, to, volume, self.offer_fence);
        }

        async fn recv(&self) -> Option<(HostId, PeerMsg)> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn migration_accept_survives_source_release_before_reply() {
        let state = Rc::new(RefCell::new(HostState::new(HostConfig {
            archive: Default::default(),
            host: HostId::new(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            cluster_placement: None,
        })));
        let destination = HostId::new(2);
        let volume = VolumeId(7);
        let offer_fence = 11;
        let client = state.borrow().peer_client.clone();
        let peers = AcceptAfterSourceRelease {
            state: Rc::clone(&state),
            offer_fence,
        };

        let accepted = simulation_scope(
            31,
            FaultConfig::default(),
            client.offer_migration_once(&peers, destination, volume, offer_fence, vec![1], None),
        )
        .await;

        assert!(accepted);
    }

    #[test]
    fn memory_migration_offer_carries_the_live_page_index() {
        let volume = VolumeId(8);
        let page = crate::types::PageId {
            volume,
            page: crate::types::PageNo(3),
        };
        let location = PageFileLoc {
            base: 0,
            fence: 17,
            object: ObjectId(4),
            offset: 31,
            len: 47,
        };
        let mut state = VolumeState::fresh(VolumeConfig::memory(8), 1);
        state.page_locs.insert(page, (Gen(6), location));
        let record = JournalRecord {
            config: state.config,
            seq: crate::types::JournalSeq(9),
            fence: 17,
            kind: RecordKind::Checkpoint {
                epoch: crate::types::Epoch(2),
                vmstate: 0,
                vmstate_logical_length: 0,
            },
            capture_seq: 5,
            sync_covered_through: 5,
            post_state_checksum: 0,
            files: Vec::new(),
            runtime_page_index: BTreeMap::new(),
            migrated_from: None,
        };

        let (offered, _, _) = JournalRecord::decode_migration_state(
            volume,
            &state.encode_migration_offer(volume, &record),
        )
        .expect("migration offer");

        assert_eq!(offered.runtime_page_index, state.page_locs);
        assert!(record.runtime_page_index.is_empty());
    }

    #[test]
    fn inline_vmstate_is_validated_and_reconstructs_block_checksums() {
        let bytes = 77_u64.to_le_bytes().to_vec();
        let loaded = decode_inline_vmstate(bytes.clone(), 8, Gen(14), 77)
            .expect("valid inline VMM snapshot");
        assert_eq!(loaded.bytes, bytes);
        let key = BlockKey {
            space: BlockSpace::Vmm,
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
            host: HostId::new(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            cluster_placement: None,
        })));
        let volume = VolumeId(1);
        let run_generation = {
            let mut host = state.borrow_mut();
            let run_generation = host.insert_fresh(volume, VolumeConfig::data(1));
            let operations = &mut host
                .volumes
                .get_mut(&volume)
                .expect("inserted volume")
                .operations;
            assert!(operations.try_start_mutation(MutationOwner::Hydration));
            assert!(operations.start_migration());
            operations.finish_mutation(MutationOwner::Hydration);
            run_generation
        };

        HydrationLease::new(&state, volume, run_generation).commit();

        let mut host = state.borrow_mut();
        let operations = &mut host
            .volumes
            .get_mut(&volume)
            .expect("inserted volume")
            .operations;
        assert!(operations.migration_running());
        assert!(!operations.start_migration());
    }
}
