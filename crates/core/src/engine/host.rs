use std::rc::Rc;

use blockd_exec::channel::{oneshot, unbounded};
use blockd_exec::inject::{Lane, injector};
use blockd_exec::{Either, OneOf3, TaskSet, delay, select2, select3, yield_now};

use super::database::DetachedDatabaseDrain;
use super::state::PendingSync;
use super::{
    HostFatal, SharedHost, advance_archive_age, archive_latest, archives_ready, attach_database,
    begin_detach_database, capture_local, checkpoint_local, create_fork, create_peer_stashed,
    database_source, delete_base, finish_detach_database, hydrate_tail, keep_base, migrate_out,
    peer_source, publish_replica_head, reclaim_backed_segments, reconcile_backed_recovery_event,
    recover_local, reoffer_outbound, replicate_latest, request_replica_archive, restore_vset,
    retry_archive_notices, retry_replica_releases, serve_fault,
};
use crate::hostmeta::HostConfig;
use crate::journal::VsetKind;
use crate::protocol::{AdminCall, AdminEvent};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store};

pub trait HostWorld: Blobs + Store + Peers + GuestMem + AdminIo + 'static {}

impl<T> HostWorld for T where T: Blobs + Store + Peers + GuestMem + AdminIo + 'static {}

const SCHEDULED_WORK_CONCURRENCY: usize = 64;
const STARTUP_RECONCILE_CONCURRENCY: usize = 32;
const ADMIN_CONCURRENCY: usize = 32;
const FAULT_CONCURRENCY: usize = 64;
const SYNC_CONCURRENCY: usize = 64;
const SYNC_QUEUE_CAPACITY: usize = 1_024;
const SYNC_INGRESS_BATCH: usize = 64;

#[derive(Clone, Copy)]
enum ScheduledWork {
    Capture,
    Hydrate,
    Replicate,
    Publish,
    Release,
    Archive,
    Reoffer,
}

struct FatalSignalLease(SharedHost);

impl Drop for FatalSignalLease {
    fn drop(&mut self) {
        self.0.borrow_mut().clear_fatal_signal();
    }
}

pub async fn host_actor<W: HostWorld>(config: HostConfig, world: Rc<W>) {
    let state = Rc::new(std::cell::RefCell::new(super::HostState::new(config)));
    host_actor_with_state(state, world).await;
}

pub async fn host_actor_with_state<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    let (send_fatal, receive_fatal) = oneshot();
    state.borrow_mut().install_fatal_signal(send_fatal);
    let _fatal_signal = FatalSignalLease(Rc::clone(&state));
    let outcome = select2(
        receive_fatal,
        host_work(Rc::clone(&state), Rc::clone(&world)),
    )
    .await;
    let failure = match outcome {
        Either::First(Ok(failure)) | Either::Second(Err(failure)) => failure,
        Either::First(Err(_)) => HostFatal::new("host fatal signal closed"),
        Either::Second(Ok(())) => return,
    };
    AdminIo::host_failed(world.as_ref(), failure).await;
}

async fn host_work<W: HostWorld>(state: SharedHost, world: Rc<W>) -> Result<(), HostFatal> {
    let config = state.borrow().config.clone();
    let Ok(verdicts) = recover_local(Rc::clone(&state), world.as_ref()).await else {
        return Err(HostFatal::new("local recovery scan failed"));
    };
    for (vset, verdict) in verdicts {
        AdminIo::emit_admin_event(world.as_ref(), AdminEvent::VsetRecovered { vset, verdict })
            .await;
    }
    let backed = state
        .borrow()
        .vsets
        .iter()
        .filter_map(|(&vset, state)| state.operations.recovery_pending().then_some(vset))
        .collect::<Vec<_>>();
    reconcile_backed_vsets(&state, &world, &backed).await?;
    let initial_work = state
        .borrow()
        .vsets
        .iter()
        .filter_map(|(&vset, vset_state)| {
            (vset_state.ready || vset_state.outbound.is_some()).then_some(vset)
        })
        .collect::<Vec<_>>();
    for vset in initial_work {
        state.borrow_mut().schedule_vset(vset);
    }
    let mut children = TaskSet::new();
    let (database_drains, pending_database_drains) = unbounded();
    children.spawn(admin_source(
        Rc::clone(&state),
        Rc::clone(&world),
        database_drains,
    ));
    children.spawn(fault_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(database_source(
        Rc::clone(&state),
        Rc::clone(&world),
        pending_database_drains,
    ));
    children.spawn(scheduled_work_source(
        Rc::clone(&state),
        Rc::clone(&world),
        config.writeback_interval,
        true,
    ));
    children.spawn(super::store_gc::store_gc_actor(
        Rc::clone(&state),
        Rc::clone(&world),
    ));
    loop {
        delay(config.writeback_interval).await;
        advance_archive_age(&state, config.writeback_interval);
        for key in archives_ready(&state) {
            children.spawn(archive_latest(Rc::clone(&state), Rc::clone(&world), key));
        }
        children.spawn(retry_archive_notices(Rc::clone(&state), Rc::clone(&world)));
        if reclaim_backed_segments(Rc::clone(&state), world.as_ref())
            .await
            .is_err()
        {
            return Err(HostFatal::new("backed segment reclaim failed"));
        }
        let accessed = GuestMem::harvest_accessed(world.as_ref()).await;
        let mut host = state.borrow_mut();
        host.cache.age(|| accessed);
        host.wedge_tick();
    }
}

async fn reconcile_backed_vsets<W: HostWorld>(
    state: &SharedHost,
    world: &Rc<W>,
    vsets: &[crate::types::VsetId],
) -> Result<(), HostFatal> {
    for batch in vsets.chunks(STARTUP_RECONCILE_CONCURRENCY) {
        let mut children = TaskSet::new();
        let mut outcomes = Vec::with_capacity(batch.len());
        for &vset in batch {
            let (send, receive) = oneshot();
            let child_state = Rc::clone(state);
            let child_world = Rc::clone(world);
            children.spawn(async move {
                let event = reconcile_backed_recovery_event(child_state, child_world, vset).await;
                let _ = send.send(event);
            });
            outcomes.push(receive);
        }
        for outcome in outcomes {
            let event = outcome
                .await
                .map_err(|_| HostFatal::new("startup reconciliation actor cancelled"))?;
            if let Some(event) = event {
                AdminIo::emit_admin_event(world.as_ref(), event).await;
            }
        }
    }
    for &vset in vsets {
        retry_replica_releases(Rc::clone(state), Rc::clone(world), vset).await;
    }
    Ok(())
}

fn work_ready(host: &super::HostState, vset: crate::types::VsetId) -> Vec<ScheduledWork> {
    let Some(vset_state) = host.vsets.get(&vset) else {
        return Vec::new();
    };
    let idle_commit =
        !vset_state.operations.mutation_blocked() && !vset_state.operations.migration_running();
    let mut work = Vec::new();
    if vset_state.ready
        && idle_commit
        && (host.cache.has_dirty_of(vset)
            || vset_state
                .pending_syncs
                .iter()
                .any(|sync| sync.barrier > vset_state.local_covered_through))
    {
        work.push(ScheduledWork::Capture);
    }
    let remote_tail_complete = vset_state.peer_source.is_some()
        && !vset_state
            .page_locs
            .values()
            .any(|(_, location)| location.base == 0 && location.fence < vset_state.fence);
    if !vset_state.operations.mutation_blocked()
        && ((vset_state.ready && vset_state.peer_source.is_some())
            || remote_tail_complete
            || (vset_state.ready
                && vset_state
                    .leaf_table
                    .keys()
                    .any(|span| !vset_state.hydrated_spans.contains(span))))
    {
        work.push(ScheduledWork::Hydrate);
    }
    let replication_needed = vset_state.best_record.as_ref().is_some_and(|record| {
        let record_is_backed = vset_state.backed.is_some_and(|pointer| {
            (pointer.capture_seq, pointer.seq) == (record.capture_seq, record.seq)
        });
        !record_is_backed || record.sync_covered_through > vset_state.sync_ack_through
    });
    if vset_state.ready
        && !vset_state.operations.replication_running()
        && vset_state.stash_assignment.is_some()
        && replication_needed
    {
        work.push(ScheduledWork::Replicate);
    }
    if vset_state.operations.publication_owner().is_none()
        && vset_state.peer_upload_done.is_some()
        && vset_state.best_record.is_some()
    {
        work.push(ScheduledWork::Publish);
    }
    if host
        .replica_releases
        .iter()
        .any(|(_, pending, _, _)| *pending == vset)
    {
        work.push(ScheduledWork::Release);
    }
    if host.disk_reclaim_requested
        && vset_state.stash_assignment.is_some()
        && vset_state.peer_committed.is_some_and(|committed| {
            vset_state.backed.is_none_or(|pointer| {
                (pointer.fence, pointer.seq) < (committed.writer_fence, committed.seq)
            })
        })
    {
        work.push(ScheduledWork::Archive);
    }
    if vset_state.outbound.is_some()
        && vset_state.best_record.is_some()
        && !vset_state.operations.migration_running()
    {
        work.push(ScheduledWork::Reoffer);
    }
    work
}

fn schedule_vset_work<W: HostWorld>(
    state: &SharedHost,
    world: &Rc<W>,
    children: &mut TaskSet,
    completed: &blockd_exec::channel::UnboundedSender<crate::types::VsetId>,
    active: &mut usize,
) -> usize {
    if *active == SCHEDULED_WORK_CONCURRENCY {
        return 0;
    }
    let mut examined = 0;
    while examined < SCHEDULED_WORK_CONCURRENCY && *active < SCHEDULED_WORK_CONCURRENCY {
        let Some(vset) = state.borrow_mut().take_scheduled_vsets(1).pop() else {
            break;
        };
        examined += 1;
        let work = work_ready(&state.borrow(), vset);
        for work in work {
            if *active == SCHEDULED_WORK_CONCURRENCY {
                state.borrow_mut().schedule_vset(vset);
                break;
            }
            let state = Rc::clone(state);
            let world = Rc::clone(world);
            let completed = completed.clone();
            children.spawn(async move {
                match work {
                    ScheduledWork::Capture => {
                        let _ = capture_local(Rc::clone(&state), world, vset).await;
                    }
                    ScheduledWork::Hydrate => {
                        hydrate_tail(Rc::clone(&state), world, vset).await;
                    }
                    ScheduledWork::Replicate => {
                        replicate_latest(Rc::clone(&state), world, vset).await;
                    }
                    ScheduledWork::Publish => {
                        publish_replica_head(Rc::clone(&state), world, vset).await;
                    }
                    ScheduledWork::Release => {
                        retry_replica_releases(Rc::clone(&state), world, vset).await;
                    }
                    ScheduledWork::Archive => {
                        request_replica_archive(Rc::clone(&state), world, vset).await;
                    }
                    ScheduledWork::Reoffer => {
                        reoffer_outbound(Rc::clone(&state), world, vset).await;
                    }
                }
                let _ = completed.send(vset);
            });
            *active += 1;
        }
    }
    examined
}

async fn scheduled_work_source<W: HostWorld>(
    state: SharedHost,
    world: Rc<W>,
    writeback_interval: u64,
    start_immediately: bool,
) {
    let mut children = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = 0usize;
    let mut retry_next_cadence = std::collections::BTreeSet::new();
    let mut first = true;
    loop {
        if !start_immediately || !std::mem::take(&mut first) {
            delay(writeback_interval).await;
        }
        {
            let mut host = state.borrow_mut();
            for vset in std::mem::take(&mut retry_next_cadence) {
                host.schedule_vset(vset);
            }
            for vset in host.take_disk_reclaim_scan_vsets(SCHEDULED_WORK_CONCURRENCY) {
                host.schedule_vset(vset);
            }
        }
        let mut drained = false;
        while let Ok(vset) = completions.try_recv() {
            active = active.checked_sub(1).expect("scheduled child completed");
            retry_next_cadence.insert(vset);
            drained = true;
        }
        if drained {
            yield_now().await;
        }
        let mut remaining = state.borrow().scheduled_vset_count();
        while remaining > 0 {
            let examined =
                schedule_vset_work(&state, &world, &mut children, &completed, &mut active);
            remaining = state.borrow().scheduled_vset_count();
            if remaining == 0 {
                break;
            }
            if examined == 0 || active == SCHEDULED_WORK_CONCURRENCY {
                let Some(vset) = completions.recv().await else {
                    return;
                };
                active = active.checked_sub(1).expect("scheduled child completed");
                retry_next_cadence.insert(vset);
                yield_now().await;
            } else {
                yield_now().await;
            }
        }
    }
}

async fn admin_source<W: HostWorld>(
    state: SharedHost,
    world: Rc<W>,
    database_drains: blockd_exec::channel::UnboundedSender<DetachedDatabaseDrain>,
) {
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded::<Option<DetachedDatabaseDrain>>();
    let mut active = 0usize;
    loop {
        let event = if active == ADMIN_CONCURRENCY {
            Either::First(completions.recv().await)
        } else {
            select2(completions.recv(), AdminIo::next_admin(world.as_ref())).await
        };
        match event {
            Either::First(Some(drain)) => {
                active = active.checked_sub(1).expect("admin child completed");
                if let Some(drain) = drain {
                    let _ = database_drains.send(drain);
                }
            }
            Either::First(None) | Either::Second(None) => return,
            Either::Second(Some(request)) => {
                let state = Rc::clone(&state);
                let world = Rc::clone(&world);
                let completed = completed.clone();
                actors.spawn(async move {
                    let drain = handle_admin(state, world, request).await;
                    let _ = completed.send(drain);
                });
                active += 1;
            }
        }
    }
}

async fn handle_admin<W: HostWorld>(
    state: SharedHost,
    world: Rc<W>,
    mut request: crate::world::AdminRequest,
) -> Option<DetachedDatabaseDrain> {
    let (cancel, cancelled) = injector();
    let _cancel_guard = cancel.clone();
    request.on_cancel(move || {
        let _ = cancel.push(Lane::Critical, ());
    });
    let (call, mut reply) = request.into_parts();
    let response = match call {
        AdminCall::CreateVset {
            vset,
            config,
            from_base: Some(base),
        } => create_fork(Rc::clone(&state), Rc::clone(&world), vset, config, base).await,
        AdminCall::KeepBase { vset, base } => {
            Some(keep_base(Rc::clone(&state), Rc::clone(&world), vset, base).await)
        }
        AdminCall::DeleteBase { base } => {
            Some(delete_base(Rc::clone(&state), Rc::clone(&world), base).await)
        }
        AdminCall::CreateVset {
            vset,
            config,
            from_base: None,
        } => create_peer_stashed(Rc::clone(&state), Rc::clone(&world), vset, config).await,
        AdminCall::Checkpoint { retry, vset } => {
            match select2(
                checkpoint_local(Rc::clone(&state), Rc::clone(&world), retry, vset),
                cancelled.recv(),
            )
            .await
            {
                Either::First(response) => response,
                Either::Second(_) => return None,
            }
        }
        AdminCall::RestoreVset { vset } => {
            Some(restore_vset(Rc::clone(&state), Rc::clone(&world), vset).await)
        }
        AdminCall::MigrateOut { vset, to } => {
            match select2(
                migrate_out(Rc::clone(&state), Rc::clone(&world), vset, to),
                cancelled.recv(),
            )
            .await
            {
                Either::First(response) => response,
                Either::Second(_) => return None,
            }
        }
        AdminCall::AttachDatabase { vset, vm } => Some(attach_database(&state, vset, vm)),
        AdminCall::BeginDetachDatabase {
            vset,
            attachment,
            mode,
        } => {
            let (response, drain) = begin_detach_database(&state, vset, attachment, mode);
            let _ = reply.send(response);
            return drain.then_some(DetachedDatabaseDrain { vset, attachment });
        }
        AdminCall::FinishDetachDatabase { vset, attachment } => {
            Some(finish_detach_database(&state, vset, attachment))
        }
    };
    if let Some(response) = response {
        let _ = reply.send(response);
    }
    None
}

async fn fault_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    let mut faults = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = 0usize;
    loop {
        if active == FAULT_CONCURRENCY {
            if completions.recv().await.is_none() {
                return;
            }
            active -= 1;
            continue;
        }
        let Some(fault) = GuestMem::next_fault(world.as_ref()).await else {
            return;
        };
        let state = Rc::clone(&state);
        let world = Rc::clone(&world);
        let completed = completed.clone();
        faults.spawn(async move {
            serve_fault(state, world, fault.page, fault.write).await;
            let _ = completed.send(());
        });
        active += 1;
    }
}

enum SyncSourceEvent<T> {
    Resolved(Option<()>),
    Cancelled(Option<(crate::types::VsetId, u64)>),
    Completed(Option<crate::types::VsetId>),
    Ingress(Option<T>),
}

async fn sync_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    let mut syncs = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let (resolved, mut resolutions) = unbounded();
    let (cancelled, cancellations) = injector();
    let mut requests = super::keyed_queue::KeyedQueue::new();
    let mut ingress_open = true;
    let mut admitted = 0usize;
    let mut next_admission = 0u64;
    let mut ingress_batch = 0;
    loop {
        while let Some((vset, (id, request))) = requests.start_next(SYNC_CONCURRENCY) {
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            let completed = completed.clone();
            let resolved = resolved.clone();
            syncs.spawn(async move {
                handle_sync(state, world, id, request, resolved).await;
                let _ = completed.send(vset);
            });
        }
        if !ingress_open && requests.is_idle() {
            return;
        }

        let event = if !ingress_open || admitted >= SYNC_QUEUE_CAPACITY {
            match select3(resolutions.recv(), cancellations.recv(), completions.recv()).await {
                OneOf3::First(resolved) => SyncSourceEvent::Resolved(resolved),
                OneOf3::Second(cancelled) => SyncSourceEvent::Cancelled(cancelled),
                OneOf3::Third(completed) => SyncSourceEvent::Completed(completed),
            }
        } else {
            match select3(
                resolutions.recv(),
                cancellations.recv(),
                select2(completions.recv(), GuestMem::next_sync(world.as_ref())),
            )
            .await
            {
                OneOf3::First(resolved) => SyncSourceEvent::Resolved(resolved),
                OneOf3::Second(cancelled) => SyncSourceEvent::Cancelled(cancelled),
                OneOf3::Third(Either::First(completed)) => SyncSourceEvent::Completed(completed),
                OneOf3::Third(Either::Second(request)) => SyncSourceEvent::Ingress(request),
            }
        };
        match event {
            SyncSourceEvent::Resolved(Some(())) => {
                admitted = admitted.checked_sub(1).expect("admitted sync resolved");
            }
            SyncSourceEvent::Resolved(None)
            | SyncSourceEvent::Cancelled(None)
            | SyncSourceEvent::Completed(None) => return,
            SyncSourceEvent::Cancelled(Some((vset, id))) => {
                if requests
                    .remove_where(vset, |(queued_id, _)| *queued_id == id)
                    .is_some()
                {
                    let _ = resolved.send(());
                } else {
                    cancel_pending_sync(&state, vset, id);
                }
            }
            SyncSourceEvent::Completed(Some(vset)) => requests.complete(vset),
            SyncSourceEvent::Ingress(Some(mut request)) => {
                let id = next_admission;
                next_admission = next_admission
                    .checked_add(1)
                    .expect("sync admission id overflow");
                let vset = request.body.volume.vset;
                let cancellation = cancelled.clone();
                request.on_cancel(move || {
                    let _ = cancellation.push(Lane::Critical, (vset, id));
                });
                admitted += 1;
                requests.push(vset, (id, request));
                ingress_batch += 1;
                if ingress_batch == SYNC_INGRESS_BATCH {
                    ingress_batch = 0;
                    yield_now().await;
                }
            }
            SyncSourceEvent::Ingress(None) => ingress_open = false,
        }
    }
}

async fn handle_sync<W: HostWorld>(
    state: SharedHost,
    world: Rc<W>,
    id: u64,
    request: crate::world::GuestSyncRequest,
    resolved: blockd_exec::channel::UnboundedSender<()>,
) {
    let (sync, reply) = request.into_parts();
    let mut reply = Some(reply);
    if reply.as_ref().expect("sync reply available").is_cancelled() {
        let _ = resolved.send(());
        return;
    }
    let action = {
        let mut host = state.borrow_mut();
        if let Some(vset) = host.vsets.get_mut(&sync.volume.vset) {
            if !vset.ready
                || vset.config.kind != VsetKind::Compute
                || sync.volume.idx.is_memory()
                || sync.volume.idx.0 > vset.config.disk_volumes
            {
                host.counters.guest_rejected += 1;
                None
            } else {
                let barrier = vset.mutation_seq;
                if vset.sync_ack_through >= barrier {
                    host.counters.syncs_acked += 1;
                    Some(false)
                } else {
                    vset.pending_syncs.push(PendingSync::new(
                        id,
                        barrier,
                        reply.take().expect("sync reply available"),
                        resolved.clone(),
                    ));
                    Some(true)
                }
            }
        } else {
            host.counters.guest_rejected += 1;
            None
        }
    };
    match action {
        None => {
            let _ = reply.as_mut().expect("sync reply available").send(false);
            let _ = resolved.send(());
        }
        Some(false) => {
            let _ = reply.as_mut().expect("sync reply available").send(true);
            let _ = resolved.send(());
        }
        Some(true) => {
            let _ = capture_local(Rc::clone(&state), Rc::clone(&world), sync.volume.vset).await;
        }
    }
}

fn cancel_pending_sync(state: &SharedHost, vset: crate::types::VsetId, id: u64) {
    let mut host = state.borrow_mut();
    let Some(pending) = host.vsets.get_mut(&vset).and_then(|vset_state| {
        let index = vset_state
            .pending_syncs
            .iter()
            .position(|sync| sync.id() == id)?;
        Some(vset_state.pending_syncs.remove(index))
    }) else {
        return;
    };
    drop(host);
    drop(pending);
}

#[cfg(test)]
#[allow(
    clippy::case_sensitive_file_extension_comparisons,
    clippy::default_trait_access,
    clippy::match_same_arms,
    clippy::unnecessary_wraps
)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::rc::Rc;
    use std::time::Duration;

    use blockd_exec::channel::{oneshot, unbounded};
    use blockd_exec::{BridgeRecvError, Executor, TaskSet, bridge_request, delay, spawn};

    use super::{
        ADMIN_CONCURRENCY, FAULT_CONCURRENCY, admin_source, capture_local, checkpoint_local,
        database_source, fault_source, handle_admin, host_actor, host_actor_with_state,
        reclaim_backed_segments, reconcile_backed_vsets, schedule_vset_work, scheduled_work_source,
        sync_source, work_ready,
    };
    use crate::database::{
        AttachmentId, DatabaseError, DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest,
    };
    use crate::engine::migration::{available_inbound_fence, migrate_in};
    use crate::engine::peer_source;
    use crate::engine::state::{CaptureKind, MutationOwner, PendingSync};
    use crate::engine::{HostFatal, HostState, cleanup_local, migrate_out};
    use crate::head::{HeadRecord, StashAssignment};
    use crate::hostmeta::{HostConfig as DaemonConfig, ReplicaPlacementConfig};
    use crate::journal::{JournalRecord, RecordKind, VsetConfig};
    use crate::layout;
    use crate::mapleaf::{LeafPtr, span_of};
    use crate::protocol::{
        AdminCall, AdminError, AdminEvent, AdminResult, AdminSuccess, DetachMode, PeerMsg,
        PeerRequestId, ReqId, StoreFault,
    };
    use crate::segment::{PageLoc, open_entry};
    use crate::types::{
        Gen, HostId, PageId, PageNo, SegId, VmId, VolumeId, VolumeIdx, VsetId, page_size,
    };
    use crate::world::{
        AdminIo, BlobEntry, BlobError, Blobs, GuestFault, GuestMem, GuestMemoryError, GuestSync,
        Peers, Store, StoreError,
    };

    type ModelStore = Rc<RefCell<BTreeMap<String, (u64, Vec<u8>)>>>;
    const TEST_PASSIVE: HostId = HostId(u16::MAX);

    fn test_replica_placement(local: HostId) -> Option<ReplicaPlacementConfig> {
        use crate::placement::PeerCandidate;

        Some(ReplicaPlacementConfig {
            membership_epoch: 1,
            local_failure_domain: local.0,
            roster: vec![
                PeerCandidate {
                    host: local,
                    weight: 1,
                    failure_domain: local.0,
                    drained: false,
                },
                PeerCandidate {
                    host: TEST_PASSIVE,
                    weight: 1,
                    failure_domain: TEST_PASSIVE.0,
                    drained: false,
                },
            ],
        })
    }

    #[derive(Default)]
    struct ModelWorld {
        admin: RefCell<VecDeque<AdminCall>>,
        faults: RefCell<VecDeque<GuestFault>>,
        syncs: RefCell<VecDeque<GuestSync>>,
        replies: Rc<RefCell<Vec<AdminResult>>>,
        events: RefCell<Vec<AdminEvent>>,
        sync_ok: Rc<RefCell<Vec<ReqId>>>,
        cancel_sync_replies: Cell<bool>,
        blobs: RefCell<BTreeMap<String, Vec<u8>>>,
        blob_write_delay: Cell<u64>,
        blob_read_delay: Cell<u64>,
        guest_read_delay: Cell<u64>,
        guest_fill_delay: Cell<u64>,
        guest_resume_delay: Cell<u64>,
        guest_unprotect_delay: Cell<u64>,
        pause_generation: Cell<u64>,
        fail_write_protect: Cell<bool>,
        slow_guest_vset: Cell<Option<VsetId>>,
        store: ModelStore,
        next_store_version: Rc<Cell<u64>>,
        store_get_delay: Cell<u64>,
        store_get_inflight: Cell<usize>,
        store_get_max_inflight: Cell<usize>,
        memory: RefCell<BTreeMap<PageId, Vec<u8>>>,
        paused_vsets: RefCell<BTreeSet<VsetId>>,
        shared_pages: RefCell<BTreeMap<crate::cache::BaseKey, Vec<u8>>>,
        peer_inbox: RefCell<VecDeque<(HostId, PeerMsg)>>,
        peer_outbox: RefCell<Vec<(HostId, PeerMsg)>>,
        peer_protocol_versions: RefCell<BTreeMap<HostId, u16>>,
        peer_send_delay: Cell<u64>,
        database_requests: RefCell<VecDeque<DatabaseRequest>>,
        database_replies: Rc<RefCell<Vec<DatabaseReply>>>,
        unprotected: RefCell<Vec<PageId>>,
        host_failures: RefCell<Vec<HostFatal>>,
    }

    async fn next<T>(queue: &RefCell<VecDeque<T>>) -> T {
        loop {
            if let Some(value) = queue.borrow_mut().pop_front() {
                return value;
            }
            delay(1).await;
        }
    }

    fn deliver(from: HostId, source: &ModelWorld, destination: &ModelWorld, to: HostId) {
        let messages = source
            .peer_outbox
            .borrow_mut()
            .drain(..)
            .collect::<Vec<_>>();
        for (target, message) in messages {
            assert_eq!(target, to);
            destination
                .peer_inbox
                .borrow_mut()
                .push_back((from, message));
        }
    }

    impl Blobs for ModelWorld {
        async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
            Ok(self
                .blobs
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
            delay(self.blob_write_delay.get().max(1)).await;
            assert!(self.blobs.borrow_mut().insert(name, bytes).is_none());
            Ok(())
        }

        async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            self.blobs
                .borrow_mut()
                .entry(name)
                .or_default()
                .extend_from_slice(&bytes);
            Ok(())
        }

        async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
            if let Some(bytes) = self.blobs.borrow_mut().get_mut(name) {
                bytes.truncate(usize::try_from(len).expect("length fits"));
            }
            Ok(())
        }

        async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
            if self.blob_read_delay.get() != 0 {
                delay(self.blob_read_delay.get()).await;
            }
            Ok(self.blobs.borrow().get(name).cloned())
        }

        async fn read_range(
            &self,
            name: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<Vec<u8>>, BlobError> {
            if self.blob_read_delay.get() != 0 {
                delay(self.blob_read_delay.get()).await;
            }
            Ok(self.blobs.borrow().get(name).map(|bytes| {
                let start = usize::try_from(offset.min(bytes.len() as u64)).expect("fits");
                let end = usize::try_from((offset + len).min(bytes.len() as u64)).expect("fits");
                bytes[start..end].to_vec()
            }))
        }

        async fn delete(&self, name: &str) -> Result<(), BlobError> {
            self.blobs.borrow_mut().remove(name);
            Ok(())
        }
    }

    impl Store for ModelWorld {
        async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
            let version = self.next_store_version.get() + 1;
            self.next_store_version.set(version);
            self.store.borrow_mut().insert(key, (version, bytes));
            Ok(version)
        }

        async fn put_cas(
            &self,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, StoreError> {
            let actual = self.store.borrow().get(&key).map(|(version, _)| *version);
            if actual != expected {
                return Err(StoreError::Fault(StoreFault::CasConflict { actual }));
            }
            let version = self.next_store_version.get() + 1;
            self.next_store_version.set(version);
            self.store.borrow_mut().insert(key, (version, bytes));
            Ok(version)
        }

        async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            let inflight = self.store_get_inflight.get() + 1;
            self.store_get_inflight.set(inflight);
            self.store_get_max_inflight
                .set(self.store_get_max_inflight.get().max(inflight));
            if self.store_get_delay.get() != 0 {
                delay(self.store_get_delay.get()).await;
            }
            let result = self.store.borrow().get(key).cloned();
            self.store_get_inflight
                .set(self.store_get_inflight.get() - 1);
            Ok(result)
        }

        async fn get_range(
            &self,
            key: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            Ok(self.store.borrow().get(key).map(|(version, bytes)| {
                let start = usize::try_from(offset.min(bytes.len() as u64)).expect("fits");
                let end = usize::try_from((offset + len).min(bytes.len() as u64)).expect("fits");
                (*version, bytes[start..end].to_vec())
            }))
        }

        async fn delete(&self, key: &str) -> Result<bool, StoreError> {
            Ok(self.store.borrow_mut().remove(key).is_some())
        }

        async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            Ok(self
                .store
                .borrow()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    impl Peers for ModelWorld {
        fn protocol_version(&self, to: HostId) -> u16 {
            self.peer_protocol_versions
                .borrow()
                .get(&to)
                .copied()
                .unwrap_or(crate::peer::CURRENT_PEER_VERSION)
        }

        async fn send(&self, to: HostId, message: PeerMsg) {
            if self.peer_send_delay.get() != 0 {
                delay(self.peer_send_delay.get()).await;
            }
            if to == TEST_PASSIVE {
                match message {
                    PeerMsg::ReplicaStatus {
                        vset,
                        assignment_epoch,
                    } => self.peer_inbox.borrow_mut().push_back((
                        TEST_PASSIVE,
                        PeerMsg::ReplicaStatusReply {
                            vset,
                            assignment_epoch,
                            committed: None,
                        },
                    )),
                    PeerMsg::ReplicaPut {
                        vset,
                        assignment_epoch,
                        artifact,
                        checksum,
                        bytes,
                    } => {
                        let key = match artifact {
                            crate::protocol::ReplicaArtifact::Segment { fence, seg } => {
                                layout::segment_key(vset, fence, seg)
                            }
                            crate::protocol::ReplicaArtifact::Leaf { fence, id } => {
                                layout::leaf_key(vset, fence, id)
                            }
                        };
                        Store::put(self, key, bytes)
                            .await
                            .expect("test archive put");
                        self.peer_inbox.borrow_mut().push_back((
                            TEST_PASSIVE,
                            PeerMsg::ReplicaPutAck {
                                vset,
                                assignment_epoch,
                                artifact,
                                checksum,
                            },
                        ));
                    }
                    PeerMsg::ReplicaCommit {
                        vset,
                        assignment_epoch,
                        info,
                        record,
                        ..
                    } => {
                        Store::put(
                            self,
                            layout::manifest_key(vset, info.writer_fence, info.seq),
                            record.clone(),
                        )
                        .await
                        .expect("test manifest put");
                        self.peer_inbox.borrow_mut().extend([
                            (
                                TEST_PASSIVE,
                                PeerMsg::ReplicaCommitAck {
                                    vset,
                                    assignment_epoch,
                                    info,
                                },
                            ),
                            (
                                TEST_PASSIVE,
                                PeerMsg::ReplicaUploadDone {
                                    vset,
                                    assignment_epoch,
                                    info,
                                    record,
                                },
                            ),
                        ]);
                    }
                    PeerMsg::ReplicaRelease {
                        vset,
                        assignment_epoch,
                        through,
                    } => self.peer_inbox.borrow_mut().push_back((
                        TEST_PASSIVE,
                        PeerMsg::ReplicaReleaseAck {
                            vset,
                            assignment_epoch,
                            through,
                        },
                    )),
                    PeerMsg::ReplicaArchive { .. } => {}
                    _ => {}
                }
                return;
            }
            self.peer_outbox.borrow_mut().push((to, message));
        }

        async fn recv(&self) -> Option<(HostId, PeerMsg)> {
            Some(next(&self.peer_inbox).await)
        }
    }

    impl GuestMem for ModelWorld {
        async fn read_page(&self, page: PageId) -> Vec<u8> {
            if self.slow_guest_vset.get() == Some(page.volume.vset)
                && self.guest_read_delay.get() != 0
            {
                delay(self.guest_read_delay.get()).await;
            }
            self.memory
                .borrow()
                .get(&page)
                .cloned()
                .unwrap_or_else(|| vec![0; page_size()])
        }

        async fn arm_write_protect(&self, _pages: &[PageId]) -> Result<(), GuestMemoryError> {
            if self.fail_write_protect.get() {
                Err(GuestMemoryError::Unavailable)
            } else {
                Ok(())
            }
        }

        async fn fill(
            &self,
            page: PageId,
            bytes: Vec<u8>,
            _writable: bool,
            _source: crate::world::FillSource,
        ) -> Result<(), GuestMemoryError> {
            if self.guest_fill_delay.get() != 0 {
                delay(self.guest_fill_delay.get()).await;
            }
            self.memory.borrow_mut().insert(page, bytes);
            Ok(())
        }

        async fn fill_shared(
            &self,
            page: PageId,
            share: (u64, u64, crate::types::SegId, u32),
            bytes: Option<Vec<u8>>,
            _writable: bool,
        ) -> Result<(), GuestMemoryError> {
            if let Some(bytes) = bytes {
                self.shared_pages.borrow_mut().insert(share, bytes);
            }
            let bytes = self.shared_pages.borrow()[&share].clone();
            self.memory.borrow_mut().insert(page, bytes);
            Ok(())
        }

        async fn fail(&self, page: PageId) -> Result<(), GuestMemoryError> {
            panic!("unexpected failed fault: {page:?}")
        }

        async fn unprotect(&self, page: PageId) -> Result<(), GuestMemoryError> {
            if self.guest_unprotect_delay.get() != 0 {
                delay(self.guest_unprotect_delay.get()).await;
            }
            self.unprotected.borrow_mut().push(page);
            Ok(())
        }

        async fn evict(&self, page: PageId) -> Result<(), GuestMemoryError> {
            self.memory.borrow_mut().remove(&page);
            Ok(())
        }

        async fn install_database(
            &self,
            _page: PageId,
            _bytes: Vec<u8>,
        ) -> Result<(), GuestMemoryError> {
            unreachable!()
        }

        async fn pause(&self, vset: VsetId) -> Result<crate::world::GuestPause, GuestMemoryError> {
            let generation = self
                .pause_generation
                .get()
                .checked_add(1)
                .expect("guest pause generation overflow");
            self.pause_generation.set(generation);
            self.paused_vsets.borrow_mut().insert(vset);
            Ok(crate::world::GuestPause {
                vmstate: 77,
                generation,
            })
        }

        async fn resume(
            &self,
            vset: VsetId,
            pause: Option<crate::world::GuestPause>,
        ) -> Result<(), GuestMemoryError> {
            if self.guest_resume_delay.get() != 0 {
                delay(self.guest_resume_delay.get()).await;
            }
            if pause.is_some_and(|pause| pause.generation != self.pause_generation.get()) {
                return Ok(());
            }
            self.paused_vsets.borrow_mut().remove(&vset);
            Ok(())
        }

        async fn harvest_accessed(&self) -> Vec<PageId> {
            Vec::new()
        }

        async fn next_fault(&self) -> Option<GuestFault> {
            Some(next(&self.faults).await)
        }

        async fn next_sync(&self) -> Option<crate::world::GuestSyncRequest> {
            let sync = next(&self.syncs).await;
            let req = sync.req;
            let (request, receive) = bridge_request(sync);
            if self.cancel_sync_replies.get() {
                drop(receive);
                return Some(request);
            }
            let completed = Rc::clone(&self.sync_ok);
            spawn(async move {
                if receive.await == Ok(true) {
                    completed.borrow_mut().push(req);
                }
            })
            .detach();
            Some(request)
        }

        async fn fence(&self, _vset: VsetId) -> Result<(), GuestMemoryError> {
            Ok(())
        }
    }

    impl AdminIo for ModelWorld {
        async fn next_admin(&self) -> Option<crate::world::AdminRequest> {
            let command = next(&self.admin).await;
            let (request, receive) = bridge_request(command);
            let replies = Rc::clone(&self.replies);
            spawn(async move {
                if let Ok(result) = receive.await {
                    replies.borrow_mut().push(result);
                }
            })
            .detach();
            Some(request)
        }

        async fn emit_admin_event(&self, event: AdminEvent) {
            self.events.borrow_mut().push(event);
        }

        async fn next_database(&self) -> Option<crate::world::DatabaseActorRequest> {
            let request = next(&self.database_requests).await;
            let (req, call) = request.into_call();
            let (request, receive) = bridge_request(call);
            let replies = Rc::clone(&self.database_replies);
            spawn(async move {
                if let Ok(result) = receive.await {
                    replies
                        .borrow_mut()
                        .push(DatabaseReply::from_result(req, result));
                }
            })
            .detach();
            Some(request)
        }

        async fn host_failed(&self, failure: HostFatal) {
            self.host_failures.borrow_mut().push(failure);
        }
    }

    #[test]
    fn child_fatal_signal_reaches_the_root_and_stops_the_actor_tree() {
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 4,
            writeback_interval: 5,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let world = Rc::new(ModelWorld::default());
        let mut executor = Executor::simulation(9);
        let actor = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        executor.run_ready();

        state.borrow_mut().fail("child failed");
        executor.run_ready();

        assert_eq!(
            *world.host_failures.borrow(),
            [HostFatal::new("child failed")]
        );
        assert!(matches!(executor.block_on(actor), Ok(())));
    }

    fn pressure_reclaim_fixture() -> (Rc<RefCell<HostState>>, Rc<ModelWorld>, VsetId, u64) {
        let vset = VsetId(70);
        let stale_segment = SegId(1);
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: Some(100),
            disk_headroom: 10,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let world = Rc::new(ModelWorld::default());
        let stale_name = layout::segment_blob(vset, 1, stale_segment);
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.segment_blobs.push((1, stale_segment, 20));
            vset_state.backed_segments.insert((1, stale_segment));
            host.record_blob(stale_name.clone(), 20);
            host.record_blob("unbacked-live-segment".to_owned(), 95);
            host.disk_reclaim_requested = true;
        }
        world.blobs.borrow_mut().insert(stale_name, vec![0; 20]);
        let incarnation = state.borrow().vsets[&vset].incarnation;
        (state, world, vset, incarnation)
    }

    #[test]
    fn partial_backed_reclaim_keeps_pressure_above_the_data_watermark() {
        let (state, world, _, _) = pressure_reclaim_fixture();
        let mut executor = Executor::simulation(93);
        let task_state = Rc::clone(&state);
        let task_world = Rc::clone(&world);

        executor
            .block_on(async move { reclaim_backed_segments(task_state, task_world.as_ref()).await })
            .expect("reclaim succeeds");

        let host = state.borrow();
        assert!(!host.disk_reclaim_target_met());
        assert!(host.disk_reclaim_requested);
    }

    #[test]
    fn partial_cleanup_keeps_pressure_above_the_data_watermark() {
        let (state, world, vset, incarnation) = pressure_reclaim_fixture();
        let mut executor = Executor::simulation(94);
        let task_state = Rc::clone(&state);
        let task_world = Rc::clone(&world);

        executor
            .block_on(async move {
                cleanup_local(task_state, task_world.as_ref(), vset, incarnation).await
            })
            .expect("cleanup succeeds");

        let host = state.borrow();
        assert!(!host.disk_reclaim_target_met());
        assert!(host.disk_reclaim_requested);
    }

    #[test]
    fn peer_fetch_replies_only_wake_the_expected_source_waiter() {
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(super::super::state::HostState::new(
            DaemonConfig {
                archive: Default::default(),
                host: HostId(1),
                cache_pages: 1,
                writeback_interval: 1,
                backup_retry: 1,
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 0,
                replica_placement: test_replica_placement(HostId(1)),
            },
        )));
        let (io, receive) = state.borrow().peer_client.page(HostId(9));
        world.peer_inbox.borrow_mut().extend([
            (
                HostId(8),
                PeerMsg::Page {
                    io,
                    bytes: Some(vec![8]),
                },
            ),
            (
                HostId(9),
                PeerMsg::Page {
                    io,
                    bytes: Some(vec![9]),
                },
            ),
        ]);
        let mut executor = Executor::simulation(91);
        let source = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        assert_eq!(executor.block_on(receive), Ok(Some(vec![9])));
        drop(source);
        executor.run_ready();
    }

    #[test]
    fn slow_peer_storage_does_not_block_an_unrelated_reply() {
        let world = Rc::new(ModelWorld::default());
        world.blob_read_delay.set(50);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        })));
        let vset = VsetId(7);
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.outbound = Some(HostId(2));
        }
        let (reply_io, reply) = state.borrow().peer_client.page(HostId(9));
        world.peer_inbox.borrow_mut().extend([
            (
                HostId(2),
                PeerMsg::FetchRange {
                    io: PeerRequestId(100),
                    vset,
                    fence: 1,
                    seg: SegId(1),
                    offset: 0,
                    len: 1,
                },
            ),
            (
                HostId(9),
                PeerMsg::Page {
                    io: reply_io,
                    bytes: Some(vec![9]),
                },
            ),
        ]);

        let mut executor = Executor::simulation(92);
        let mut source = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        assert_eq!(executor.block_on(reply), Ok(Some(vec![9])));
        assert!(
            executor.now() < 50,
            "reply waited for unrelated storage I/O"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn peer_storage_ingress_defers_overload_without_false_missing_data() {
        let world = Rc::new(ModelWorld::default());
        world.blob_read_delay.set(50);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        })));
        let vset = VsetId(7);
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.outbound = Some(HostId(2));
        }
        world.peer_inbox.borrow_mut().extend((0..80).map(|io| {
            (
                HostId(2),
                PeerMsg::FetchRange {
                    io: PeerRequestId(io),
                    vset,
                    fence: 1,
                    seg: SegId(1),
                    offset: 0,
                    len: 1,
                },
            )
        }));

        let mut executor = Executor::simulation(93);
        let mut source = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(10);
        assert!(
            world.peer_outbox.borrow().is_empty(),
            "transient overload must wait for the caller's retry instead of reporting missing data"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn saturated_replica_route_defers_to_retry_without_blocking_unrelated_replies() {
        use crate::placement::PeerCandidate;

        let source = HostId(2);
        let local = HostId(1);
        let vset = VsetId(7);
        let world = Rc::new(ModelWorld::default());
        world.peer_send_delay.set(5);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: local,
            cache_pages: 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(ReplicaPlacementConfig {
                membership_epoch: 1,
                local_failure_domain: 1,
                roster: vec![
                    PeerCandidate {
                        host: source,
                        weight: 1,
                        failure_domain: 2,
                        drained: false,
                    },
                    PeerCandidate {
                        host: local,
                        weight: 1,
                        failure_domain: 1,
                        drained: false,
                    },
                ],
            }),
        })));
        let (page_io, page_reply) = state.borrow().peer_client.page(HostId(9));
        let replica_reply = state.borrow().peer_client.status(HostId(9), VsetId(9), 1);
        world.peer_inbox.borrow_mut().extend((0..300).map(|_| {
            (
                source,
                PeerMsg::ReplicaStatus {
                    vset,
                    assignment_epoch: 1,
                },
            )
        }));
        world.peer_inbox.borrow_mut().push_back((
            HostId(9),
            PeerMsg::Page {
                io: page_io,
                bytes: Some(vec![9]),
            },
        ));
        world.peer_inbox.borrow_mut().push_back((
            HostId(9),
            PeerMsg::ReplicaStatusReply {
                vset: VsetId(9),
                assignment_epoch: 1,
                committed: None,
            },
        ));

        let mut executor = Executor::simulation(96);
        let mut actor = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        assert_eq!(executor.block_on(page_reply), Ok(Some(vec![9])));
        assert_eq!(executor.block_on(replica_reply), Ok(None));
        assert!(
            executor.now() < 5,
            "unrelated reply waited for a saturated replica shard"
        );
        executor.run_until(700);
        assert!(
            state.borrow().counters.replica_capacity_backpressure > 0,
            "saturated request admission must be explicit so the caller retries"
        );
        actor.cancel();
        executor.run_ready();
    }

    #[test]
    fn administrative_ingress_stops_at_the_actor_limit() {
        let world = Rc::new(ModelWorld::default());
        world.blob_write_delay.set(100);
        world.admin.borrow_mut().extend(
            (1..=u64::try_from(ADMIN_CONCURRENCY + 1).expect("limit fits")).map(|id| {
                AdminCall::CreateVset {
                    vset: VsetId(id),
                    config: VsetConfig::compute(1, 1),
                    from_base: None,
                }
            }),
        );
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        })));

        let mut executor = Executor::simulation(97);
        let (database_drains, _pending_database_drains) = unbounded();
        let mut actor = executor.spawn(admin_source(
            Rc::clone(&state),
            Rc::clone(&world),
            database_drains,
        ));
        executor.run_until(2);
        assert_eq!(world.admin.borrow().len(), 1);
        assert_eq!(state.borrow().vsets.len(), ADMIN_CONCURRENCY);
        actor.cancel();
        executor.run_ready();
    }

    #[test]
    fn graceful_database_drains_release_admin_ingress_after_reply() {
        const DATABASES: u64 = 10_000;

        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            for number in 1..=DATABASES {
                let vset = VsetId(number);
                let attachment = AttachmentId {
                    vm: VmId(number),
                    generation: number,
                };
                host.insert_fresh(vset, VsetConfig::database(1));
                let runtime = &mut host
                    .vsets
                    .get_mut(&vset)
                    .expect("inserted database")
                    .database_runtime;
                runtime.phase = super::super::state::AttachmentPhase::Attached(attachment);
                runtime.active = Some(attachment);
                world
                    .admin
                    .borrow_mut()
                    .push_back(AdminCall::BeginDetachDatabase {
                        vset,
                        attachment,
                        mode: DetachMode::Graceful,
                    });
            }
            world.admin.borrow_mut().push_back(AdminCall::CreateVset {
                vset: VsetId(DATABASES + 1),
                config: VsetConfig::compute(1, 1),
                from_base: None,
            });
        }

        let mut executor = Executor::simulation(101);
        let (database_drains, mut pending_database_drains) = unbounded();
        let actor = executor.spawn(admin_source(
            Rc::clone(&state),
            Rc::clone(&world),
            database_drains,
        ));
        executor.run_until(2);

        assert!(
            world.admin.borrow().is_empty(),
            "post-reply database drains must not exhaust admin ingress"
        );
        assert_eq!(
            std::iter::from_fn(|| pending_database_drains.try_recv().ok()).count(),
            usize::try_from(DATABASES).expect("database count fits"),
            "each detach should enqueue bounded keyed work instead of parking an actor"
        );
        drop(actor);
        executor.run_ready();
    }

    #[test]
    fn guest_fault_ingress_stops_at_the_actor_limit() {
        let world = Rc::new(ModelWorld::default());
        world.guest_fill_delay.set(100);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: FAULT_CONCURRENCY + 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            for number in 1..=u64::try_from(FAULT_CONCURRENCY + 1).expect("limit fits") {
                let vset = VsetId(number);
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                host.vsets.get_mut(&vset).expect("inserted vset").ready = true;
                world.faults.borrow_mut().push_back(GuestFault {
                    page: PageId {
                        volume: VolumeId {
                            vset,
                            idx: VolumeIdx(1),
                        },
                        page: PageNo(0),
                    },
                    write: false,
                });
            }
        }

        let mut executor = Executor::simulation(99);
        let mut actor = executor.spawn(fault_source(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(2);
        assert_eq!(world.faults.borrow().len(), 1);
        actor.cancel();
        executor.run_ready();
    }

    #[test]
    fn slow_sync_capture_does_not_block_ingestion_for_another_vset() {
        let world = Rc::new(ModelWorld::default());
        world.slow_guest_vset.set(Some(VsetId(1)));
        world.guest_read_delay.set(50);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 4,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        })));
        let first_page = PageId {
            volume: VolumeId {
                vset: VsetId(1),
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        {
            let mut host = state.borrow_mut();
            for vset in [VsetId(1), VsetId(2)] {
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                host.vsets.get_mut(&vset).expect("inserted vset").ready = true;
            }
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(first_page, true, false);
            host.vsets
                .get_mut(&VsetId(1))
                .expect("first vset")
                .mutation_seq = 1;
        }
        world
            .memory
            .borrow_mut()
            .insert(first_page, vec![1; page_size()]);
        world.syncs.borrow_mut().extend(
            (1..=80)
                .map(|req| GuestSync {
                    req: ReqId(req),
                    volume: first_page.volume,
                })
                .chain(std::iter::once(GuestSync {
                    req: ReqId(100),
                    volume: VolumeId {
                        vset: VsetId(2),
                        idx: VolumeIdx(1),
                    },
                })),
        );

        let mut executor = Executor::simulation(93);
        let mut source = executor.spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(10);
        assert_eq!(*world.sync_ok.borrow(), [ReqId(100)]);
        assert_eq!(
            state.borrow().vsets[&VsetId(1)].pending_syncs.len(),
            1,
            "a burst for one vset must retain only its active sync in actor state"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn sync_durability_waiters_apply_backpressure_at_the_global_cap() {
        let world = Rc::new(ModelWorld::default());
        let vset = VsetId(1);
        let volume = VolumeId {
            vset,
            idx: VolumeIdx(1),
        };
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
        }
        world
            .syncs
            .borrow_mut()
            .extend((0..1_100).map(|req| GuestSync {
                req: ReqId(req),
                volume,
            }));

        let mut executor = Executor::simulation(95);
        let mut source = executor.spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(10);
        assert_eq!(
            state.borrow().vsets[&vset].pending_syncs.len(),
            super::SYNC_QUEUE_CAPACITY
        );
        assert_eq!(
            world.syncs.borrow().len(),
            1_100 - super::SYNC_QUEUE_CAPACITY,
            "ingress must stop while admitted syncs await replica durability"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn cancelled_sync_callers_release_admission_for_later_requests() {
        let world = Rc::new(ModelWorld::default());
        world.cancel_sync_replies.set(true);
        let vset = VsetId(1);
        let volume = VolumeId {
            vset,
            idx: VolumeIdx(1),
        };
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
        }
        world
            .syncs
            .borrow_mut()
            .extend((0..1_024).map(|req| GuestSync {
                req: ReqId(req),
                volume,
            }));

        let mut executor = Executor::simulation(97);
        let mut source = executor.spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(10);
        assert!(state.borrow().vsets[&vset].pending_syncs.is_empty());
        assert!(world.syncs.borrow().is_empty());

        world.cancel_sync_replies.set(false);
        world.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(2_000),
            volume,
        });
        executor.run_until(20);
        assert_eq!(state.borrow().vsets[&vset].pending_syncs.len(), 1);
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn scheduled_work_refills_capacity_within_one_writeback_cadence() {
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 100,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let (resolved, _resolutions) = unbounded();
        {
            let mut host = state.borrow_mut();
            for number in 1..=128 {
                let vset = VsetId(number);
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
                vset_state.ready = true;
                vset_state.mutation_seq = 1;
                let (request, _reply) = bridge_request(());
                let ((), reply) = request.into_parts();
                vset_state
                    .pending_syncs
                    .push(PendingSync::new(number, 1, reply, resolved.clone()));
                host.schedule_vset(vset);
            }
        }

        let mut executor = Executor::simulation(96);
        let mut source = executor.spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        executor.run_until(150);
        assert!(
            state
                .borrow()
                .vsets
                .values()
                .all(|vset_state| vset_state.local_covered_through == 1),
            "vsets beyond the first 64 must not wait for another cadence"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn host_wide_pressure_scans_idle_archive_candidates() {
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: Some(1),
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let vset = VsetId(1);
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.stash_assignment = Some(StashAssignment {
                assignment_epoch: 3,
                active_peer: HostId(2),
                active_assignment_epoch: 3,
                transition_peer: None,
                membership_epoch: 1,
            });
            vset_state.peer_committed = Some(crate::protocol::ReplicaCommitInfo {
                writer_fence: 1,
                seq: crate::types::JournalSeq(1),
                sync_covered_through: 1,
            });
            assert!(!host.try_reserve_blob("pressure".to_owned(), 2));
        }

        let mut executor = Executor::simulation(100);
        let mut source = executor.spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            1,
            false,
        ));
        executor.run_until(2);
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == HostId(2)
                && matches!(
                    message,
                    PeerMsg::ReplicaArchive { vset: archived, .. } if *archived == vset
                )
        }));
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn scheduler_keeps_capacity_wakeups_that_race_with_handle_reaping() {
        for seed in 1..=16 {
            let world = Rc::new(ModelWorld::default());
            world.blob_write_delay.set(100);
            let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
                archive: Default::default(),
                host: HostId(1),
                cache_pages: 1,
                writeback_interval: 100,
                backup_retry: 1,
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 0,
                replica_placement: None,
            })));
            let (resolved, _resolutions) = unbounded();
            {
                let mut host = state.borrow_mut();
                for number in 1..=65 {
                    let vset = VsetId(number);
                    host.insert_fresh(vset, VsetConfig::compute(1, 1));
                    let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
                    vset_state.ready = true;
                    vset_state.mutation_seq = 1;
                    let (request, _reply) = bridge_request(());
                    let ((), reply) = request.into_parts();
                    vset_state.pending_syncs.push(PendingSync::new(
                        number,
                        1,
                        reply,
                        resolved.clone(),
                    ));
                    if number <= 64 {
                        host.schedule_vset(vset);
                    }
                }
            }

            let mut executor = Executor::simulation(seed);
            let late_state = Rc::clone(&state);
            executor
                .spawn(async move {
                    delay(199).await;
                    late_state.borrow_mut().schedule_vset(VsetId(65));
                })
                .detach();
            let mut source = executor.spawn(scheduled_work_source(
                Rc::clone(&state),
                Rc::clone(&world),
                100,
                false,
            ));
            executor.run_until(350);
            assert_eq!(
                state.borrow().vsets[&VsetId(65)].local_covered_through,
                1,
                "seed {seed} lost the only scheduler capacity notification"
            );
            source.cancel();
            executor.run_ready();
        }
    }

    #[test]
    fn stalled_maintenance_attempts_cannot_monopolize_scheduler_capacity() {
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 100,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let (resolved, _resolutions) = unbounded();
        {
            let mut host = state.borrow_mut();
            for number in 1..=64 {
                let vset = VsetId(number);
                let page = PageId {
                    volume: VolumeId {
                        vset,
                        idx: VolumeIdx(1),
                    },
                    page: PageNo(0),
                };
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
                vset_state.ready = true;
                vset_state.fence = 2;
                vset_state.peer_source = Some(HostId(2));
                vset_state.page_locs.insert(
                    page,
                    (
                        Gen(0),
                        PageLoc {
                            base: 0,
                            fence: 1,
                            seg: SegId(1),
                            offset: 0,
                            len: u32::try_from(page_size()).expect("page size fits"),
                        },
                    ),
                );
                host.schedule_vset(vset);
            }
            let vset = VsetId(65);
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
            let (request, _reply) = bridge_request(());
            let ((), reply) = request.into_parts();
            vset_state
                .pending_syncs
                .push(PendingSync::new(65, 1, reply, resolved));
            host.schedule_vset(vset);
        }

        let mut executor = Executor::simulation(98);
        let mut source = executor.spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        executor.run_until(150_000_250);
        assert_eq!(
            state.borrow().vsets[&VsetId(65)].local_covered_through,
            1,
            "timed-out hydration attempts must yield capacity to later captures"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn multi_work_vsets_do_not_count_unadmitted_later_ids_as_examined() {
        let world = Rc::new(ModelWorld::default());
        world.blob_write_delay.set(10);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 100,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let (resolved, _resolutions) = unbounded();
        {
            let mut host = state.borrow_mut();
            for number in 1..=64 {
                let vset = VsetId(number);
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
                vset_state.ready = true;
                if number <= 32 || number == 64 {
                    vset_state.mutation_seq = 1;
                    let (request, _reply) = bridge_request(());
                    let ((), reply) = request.into_parts();
                    vset_state.pending_syncs.push(PendingSync::new(
                        number,
                        1,
                        reply,
                        resolved.clone(),
                    ));
                }
                if number <= 32 {
                    vset_state.peer_source = Some(HostId(2));
                }
                host.schedule_vset(vset);
            }
        }

        let mut executor = Executor::simulation(99);
        let mut source = executor.spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        executor.run_until(150);
        assert_eq!(
            state.borrow().vsets[&VsetId(64)].local_covered_through,
            1,
            "later IDs reinserted without a slot must be reconsidered in the same cadence"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn capacity_requeued_work_finishes_in_the_same_scheduler_cadence() {
        let world = Rc::new(ModelWorld::default());
        world.blob_write_delay.set(10);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 100,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let (resolved, _resolutions) = unbounded();
        {
            let mut host = state.borrow_mut();
            for number in 1..=64 {
                let vset = VsetId(number);
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
                vset_state.ready = true;
                vset_state.mutation_seq = 1;
                let (request, _reply) = bridge_request(());
                let ((), reply) = request.into_parts();
                vset_state
                    .pending_syncs
                    .push(PendingSync::new(number, 1, reply, resolved.clone()));
                if number == 64 {
                    vset_state.peer_source = Some(HostId(2));
                }
                host.schedule_vset(vset);
            }
        }

        let mut executor = Executor::simulation(102);
        let mut source = executor.spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        executor.run_until(150);
        assert!(
            world.peer_outbox.borrow().iter().any(|(_, message)| {
                matches!(
                    message,
                    PeerMsg::Released {
                        vset: VsetId(64),
                        ..
                    }
                )
            }),
            "work requeued at the capacity edge must run after a completion, not next cadence"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn no_progress_maintenance_retries_once_per_cadence_under_saturation() {
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 100,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            for number in 1..=65 {
                let vset = VsetId(number);
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
                vset_state.ready = true;
                vset_state.peer_source = Some(HostId(2));
                host.schedule_vset(vset);
            }
        }

        let mut executor = Executor::simulation(104);
        let mut source = executor.spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        executor.run_until(350);
        let releases = world
            .peer_outbox
            .borrow()
            .iter()
            .filter(|(_, message)| matches!(message, PeerMsg::Released { .. }))
            .count();
        assert_eq!(
            releases,
            65 * 2,
            "no-progress maintenance must not spin between timer cadences"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn idle_ten_thousand_vsets_schedule_no_child_work_in_bounded_polls() {
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 4,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        })));
        {
            let mut host = state.borrow_mut();
            for number in 1..=10_000 {
                let vset = VsetId(number);
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                host.schedule_vset(vset);
            }
            assert_eq!(host.scheduled_vset_count(), 10_000);
        }
        let mut executor = Executor::simulation(94);
        let spawned = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                let mut children = TaskSet::new();
                let (completed, _completions) = unbounded();
                let mut active = 0;
                schedule_vset_work(&state, &world, &mut children, &completed, &mut active);
                assert!(children.is_empty());
                assert_eq!(active, 0);
                children.len()
            }
        });
        assert_eq!(spawned, 0);
        assert_eq!(state.borrow().scheduled_vset_count(), 10_000 - 64);
        assert!(
            executor.polls() < 400,
            "idle scan used {} polls",
            executor.polls()
        );
    }

    #[test]
    fn scheduled_vset_batches_rotate_past_continuously_rescheduled_low_ids() {
        let mut host = HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 4,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        });
        for number in 1..=128 {
            let vset = VsetId(number);
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            host.schedule_vset(vset);
        }
        let first = host.take_scheduled_vsets(64);
        assert_eq!(first, (1..=64).map(VsetId).collect::<Vec<_>>());
        for vset in first {
            host.schedule_vset(vset);
        }
        assert_eq!(
            host.take_scheduled_vsets(64),
            (65..=128).map(VsetId).collect::<Vec<_>>()
        );
    }

    #[test]
    fn startup_reconciliation_is_bounded_and_emits_in_vset_order() {
        let world = Rc::new(ModelWorld::default());
        world.store_get_delay.set(10);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 4,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        })));
        let vsets = (1..=65).map(VsetId).collect::<Vec<_>>();
        {
            let mut host = state.borrow_mut();
            for &vset in &vsets {
                host.insert_fresh(vset, VsetConfig::compute(1, 1));
                host.vsets
                    .get_mut(&vset)
                    .expect("inserted vset")
                    .operations
                    .set_recovery(crate::protocol::Verdict::ColdBoot);
            }
        }

        let mut executor = Executor::simulation(95);
        let reconcile_state = Rc::clone(&state);
        let reconcile_world = Rc::clone(&world);
        let reconcile_vsets = vsets.clone();
        executor
            .block_on(async move {
                reconcile_backed_vsets(&reconcile_state, &reconcile_world, &reconcile_vsets).await
            })
            .expect("startup reconciliation succeeds");

        assert_eq!(world.store_get_max_inflight.get(), 32);
        assert_eq!(world.store_get_inflight.get(), 0);
        assert_eq!(
            *world.events.borrow(),
            vsets
                .iter()
                .map(|&vset| AdminEvent::VsetRecovered {
                    vset,
                    verdict: crate::protocol::Verdict::ColdBoot,
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn create_fault_and_sync_form_one_durable_protocol_scenario() {
        let vset = VsetId(7);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(3),
        };
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::compute(1, 8),
            from_base: None,
        });
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(0),
            cache_pages: 4,
            writeback_interval: 5,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(0)),
        };
        let mut executor = Executor::simulation(4);
        let actor_world = Rc::clone(&world);
        let actor = executor.spawn(host_actor(config.clone(), actor_world));
        executor.run_until(5);
        assert_eq!(
            *world.replies.borrow(),
            [Ok(AdminSuccess::VsetCreated { vset })]
        );

        world
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: true });
        executor.run_until(7);
        let expected = vec![0x5a; page_size()];
        world.memory.borrow_mut().insert(page, expected.clone());
        world.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(2),
            volume: page.volume,
        });
        executor.run_until(19);
        assert_eq!(*world.sync_ok.borrow(), [ReqId(2)]);
        world
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: false });
        executor.run_until(19);
        assert!(world.unprotected.borrow().is_empty());

        world.admin.borrow_mut().push_back(AdminCall::Checkpoint {
            retry: ReqId(3),
            vset,
        });
        executor.run_until(25);
        assert_eq!(
            *world.replies.borrow(),
            [
                Ok(AdminSuccess::VsetCreated { vset }),
                Ok(AdminSuccess::CheckpointDone {
                    vset,
                    epoch: crate::types::Epoch(1)
                })
            ]
        );

        let blobs = world.blobs.borrow();
        let (fence, seq, record_bytes) = blobs
            .iter()
            .filter(|(name, _)| name.ends_with(".rec"))
            .filter_map(|(name, bytes)| {
                let layout::BlobName::Journal {
                    vset: found_vset,
                    fence,
                    seq,
                } = layout::parse_blob(name)?
                else {
                    return None;
                };
                let record = JournalRecord::decode(vset, bytes).ok()?;
                (found_vset == vset
                    && matches!(record.kind, crate::journal::RecordKind::Checkpoint { .. }))
                .then_some((fence, seq, bytes))
            })
            .max_by_key(|(_, seq, _)| *seq)
            .expect("checkpoint record persisted");
        assert_eq!(
            record_bytes,
            &blobs[&layout::journal_mirror_blob(vset, fence, seq)]
        );
        let record = JournalRecord::decode(vset, record_bytes).expect("valid record");
        assert_eq!(record.capture_seq, 1);
        assert_eq!(record.sync_covered_through, 1);
        let durable_manifest = {
            let store = world.store.borrow();
            let (_, head_bytes) = &store[&layout::head_key(vset)];
            let head = crate::head::HeadRecord::decode(vset, head_bytes).expect("valid head");
            let pointer = head.manifest.expect("published manifest");
            let (_, manifest_bytes) =
                &store[&layout::manifest_key(vset, pointer.fence, pointer.seq)];
            JournalRecord::decode(vset, manifest_bytes).expect("valid durable manifest")
        };
        let expected_peer_publication = crate::protocol::ReplicaCommitInfo {
            writer_fence: durable_manifest.fence,
            seq: durable_manifest.seq,
            sync_covered_through: durable_manifest.sync_covered_through,
        };
        assert_eq!(
            record.kind,
            crate::journal::RecordKind::Checkpoint {
                epoch: crate::types::Epoch(1),
                vmstate: 77
            }
        );
        let (_, location) = record.overlay[&page];
        let segment = &blobs[&layout::segment_blob(vset, location.fence, location.seg)];
        let start = usize::try_from(location.offset).expect("fits");
        let end = start + usize::try_from(location.len).expect("fits");
        let (_, _, raw) = open_entry(vset, &segment[start..end]).expect("valid page entry");
        assert_eq!(raw, expected);

        drop(blobs);
        drop(actor);
        executor.run_ready();

        world.memory.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world.events.borrow_mut().clear();
        let recovered_world = Rc::clone(&world);
        let recovered_state = Rc::new(RefCell::new(HostState::new(config)));
        let recovered = executor.spawn(host_actor_with_state(
            Rc::clone(&recovered_state),
            recovered_world,
        ));
        executor.run_until(27);
        assert_eq!(
            *world.events.borrow(),
            [AdminEvent::VsetRecovered {
                vset,
                verdict: crate::protocol::Verdict::Resume {
                    epoch: crate::types::Epoch(1),
                    vmstate: 77
                }
            }]
        );
        assert_eq!(
            recovered_state
                .borrow()
                .vsets
                .get(&vset)
                .and_then(|vset_state| vset_state.peer_published),
            Some(expected_peer_publication),
            "recovery must retain the published commit needed to release replica state"
        );
        world
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: false });
        executor.run_until(29);
        assert_eq!(world.memory.borrow().get(&page), Some(&expected));

        drop(recovered);
        executor.run_ready();
    }

    #[test]
    fn slow_database_work_does_not_block_an_unrelated_vset() {
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 8,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let world = Rc::new(ModelWorld::default());
        world.blob_write_delay.set(50);
        let first = VsetId(1);
        let second = VsetId(17);
        let first_attachment = AttachmentId {
            vm: VmId(1),
            generation: 1,
        };
        let second_attachment = AttachmentId {
            vm: VmId(2),
            generation: 2,
        };
        {
            let mut host = state.borrow_mut();
            for (vset, attachment) in [(first, first_attachment), (second, second_attachment)] {
                host.insert_fresh(vset, VsetConfig::database(8));
                let vset_state = host.vsets.get_mut(&vset).expect("inserted database");
                vset_state.ready = true;
                vset_state.database_runtime.phase =
                    super::super::state::AttachmentPhase::Attached(attachment);
            }
        }
        world.database_requests.borrow_mut().extend([
            DatabaseRequest {
                req: ReqId(1),
                vset: first,
                attachment: first_attachment,
                op: DatabaseOp::Open {
                    handle: 1,
                    file: DatabaseFile::Main,
                    create: true,
                },
            },
            DatabaseRequest {
                req: ReqId(2),
                vset: second,
                attachment: second_attachment,
                op: DatabaseOp::Access {
                    file: DatabaseFile::Main,
                },
            },
        ]);

        let mut executor = Executor::simulation(41);
        let (_database_drains, pending_database_drains) = unbounded();
        let mut source = executor.spawn(database_source(
            Rc::clone(&state),
            Rc::clone(&world),
            pending_database_drains,
        ));
        executor.run_until(10);
        assert_eq!(
            *world.database_replies.borrow(),
            [DatabaseReply::Access {
                req: ReqId(2),
                exists: false,
            }]
        );
        executor.run_until(200);
        assert!(
            world
                .database_replies
                .borrow()
                .contains(&DatabaseReply::Opened { req: ReqId(1) })
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    fn database_ingress_rejects_overload_without_unbounded_queueing() {
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 8,
            writeback_interval: 1_000,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(1)),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let world = Rc::new(ModelWorld::default());
        world.blob_write_delay.set(50);
        let vset = VsetId(1);
        let attachment = AttachmentId {
            vm: VmId(1),
            generation: 1,
        };
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::database(8));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted database");
            vset_state.ready = true;
            vset_state.database_runtime.phase =
                super::super::state::AttachmentPhase::Attached(attachment);
        }
        world
            .database_requests
            .borrow_mut()
            .extend((0..80).map(|req| DatabaseRequest {
                req: ReqId(req),
                vset,
                attachment,
                op: DatabaseOp::Open {
                    handle: req,
                    file: DatabaseFile::Main,
                    create: true,
                },
            }));

        let mut executor = Executor::simulation(42);
        let (_database_drains, pending_database_drains) = unbounded();
        let mut source = executor.spawn(database_source(
            Rc::clone(&state),
            Rc::clone(&world),
            pending_database_drains,
        ));
        executor.run_until(10);
        assert!(
            world.database_replies.borrow().iter().any(|reply| matches!(
                reply,
                DatabaseReply::Failed {
                    error: DatabaseError::Busy,
                    ..
                }
            )),
            "overload must be rejected before slow database storage completes"
        );
        source.cancel();
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn database_actor_persists_byte_io_sync_truncate_and_delete() {
        let vset = VsetId(70);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::database(8),
            from_base: None,
        });
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(0),
            cache_pages: 4,
            writeback_interval: 100_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(0)),
        };
        let mut executor = Executor::simulation(70);
        let actor = executor.spawn(host_actor(config.clone(), Rc::clone(&world)));
        executor.run_until(5);
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::AttachDatabase { vset, vm: VmId(9) });
        executor.run_until(8);
        let attachment = world
            .replies
            .borrow()
            .iter()
            .find_map(|reply| match reply {
                Ok(AdminSuccess::DatabaseAttached { attachment, .. }) => Some(*attachment),
                _ => None,
            })
            .expect("database attachment");
        let request = |req, op| DatabaseRequest {
            req: ReqId(req),
            vset,
            attachment,
            op,
        };
        let offset = page_size() as u64 - 3;
        world.database_requests.borrow_mut().extend([
            request(
                102,
                DatabaseOp::Open {
                    handle: 1,
                    file: DatabaseFile::Main,
                    create: true,
                },
            ),
            request(
                103,
                DatabaseOp::Write {
                    handle: 1,
                    offset,
                    bytes: b"abcdefgh".to_vec(),
                },
            ),
            request(
                104,
                DatabaseOp::Read {
                    handle: 1,
                    offset,
                    len: 8,
                },
            ),
            request(105, DatabaseOp::Sync { handle: 1 }),
            request(
                106,
                DatabaseOp::Truncate {
                    handle: 1,
                    size: page_size() as u64 + 2,
                },
            ),
            request(
                107,
                DatabaseOp::Read {
                    handle: 1,
                    offset,
                    len: 8,
                },
            ),
            request(
                108,
                DatabaseOp::Delete {
                    file: DatabaseFile::Main,
                },
            ),
            request(
                109,
                DatabaseOp::Access {
                    file: DatabaseFile::Main,
                },
            ),
        ]);
        executor.run_until(80);
        assert_eq!(
            *world.database_replies.borrow(),
            [
                DatabaseReply::Opened { req: ReqId(102) },
                DatabaseReply::Written {
                    req: ReqId(103),
                    sequence: 2,
                },
                DatabaseReply::Read {
                    req: ReqId(104),
                    bytes: b"abcdefgh".to_vec(),
                    eof: false,
                },
                DatabaseReply::Synced {
                    req: ReqId(105),
                    sequence: 2,
                },
                DatabaseReply::Truncated {
                    req: ReqId(106),
                    sequence: 3,
                },
                DatabaseReply::Read {
                    req: ReqId(107),
                    bytes: b"abcde".to_vec(),
                    eof: true,
                },
                DatabaseReply::Deleted {
                    req: ReqId(108),
                    sequence: 4,
                },
                DatabaseReply::Access {
                    req: ReqId(109),
                    exists: false,
                },
            ]
        );
        let records = world
            .blobs
            .borrow()
            .iter()
            .filter_map(|(name, bytes)| {
                name.starts_with(&format!("v/{:016x}/j/", vset.0))
                    .then(|| JournalRecord::decode(vset, bytes).ok())
                    .flatten()
            })
            .collect::<Vec<_>>();
        assert!(records.iter().any(|record| {
            record.capture_seq == 4
                && record.sync_covered_through == 2
                && !record.database.main.exists
        }));

        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::BeginDetachDatabase {
                vset,
                attachment,
                mode: DetachMode::Graceful,
            });
        world
            .database_requests
            .borrow_mut()
            .push_back(request(111, DatabaseOp::Close { handle: 1 }));
        executor.run_until(100);
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::DatabaseDetachStarted {
                    vset,
                    attachment,
                    forced: false,
                }))
        );
        assert!(
            world
                .database_replies
                .borrow()
                .contains(&DatabaseReply::Closed { req: ReqId(111) })
        );
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::FinishDetachDatabase { vset, attachment });
        executor.run_until(104);
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::DatabaseDetached { vset, attachment }))
        );
        world.database_requests.borrow_mut().push_back(request(
            113,
            DatabaseOp::Access {
                file: DatabaseFile::Main,
            },
        ));
        executor.run_until(106);
        assert!(
            world
                .database_replies
                .borrow()
                .contains(&DatabaseReply::Failed {
                    req: ReqId(113),
                    error: crate::database::DatabaseError::StaleAttachment,
                })
        );

        drop(actor);
        executor.run_ready();
        world.replies.borrow_mut().clear();
        world.events.borrow_mut().clear();
        world.database_replies.borrow_mut().clear();
        let recovered = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(110);
        assert!(
            world.events.borrow().contains(&AdminEvent::VsetRecovered {
                vset,
                verdict: crate::protocol::Verdict::DatabaseReady { synced_through: 4 },
            }),
            "events: {:?}",
            world.events.borrow()
        );

        drop(recovered);
        executor.run_ready();
    }

    #[test]
    fn backed_creation_claims_and_publishes_a_fenced_head() {
        let vset = VsetId(11);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::compute(1, 8),
            from_base: None,
        });
        let config = DaemonConfig {
            archive: crate::hostmeta::ArchivePolicy {
                interval: 1,
                ..Default::default()
            },
            host: HostId(3),
            cache_pages: 4,
            writeback_interval: 5,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(3)),
        };
        let mut executor = Executor::simulation(5);
        let actor = executor.spawn(host_actor(config.clone(), Rc::clone(&world)));
        executor.run_until(100);

        assert_eq!(
            *world.replies.borrow(),
            [Ok(AdminSuccess::VsetCreated { vset })]
        );
        let store = world.store.borrow();
        let (_, head_bytes) = &store[&layout::head_key(vset)];
        let head = HeadRecord::decode(vset, head_bytes).expect("valid head");
        assert_eq!(head.holder, HostId(3));
        assert_eq!(head.fence, 1);
        let manifest = head.manifest.expect("initial record published");
        assert_eq!(manifest.fence, 1);
        assert_eq!(manifest.seq, crate::types::JournalSeq(0));
        let (_, manifest_bytes) = &store[&layout::manifest_key(vset, 1, manifest.seq)];
        let record = JournalRecord::decode(vset, manifest_bytes).expect("valid manifest");
        assert_eq!(record.fence, 1);
        assert_eq!(record.seq, manifest.seq);

        drop(store);
        drop(actor);
        executor.run_ready();

        world.replies.borrow_mut().clear();
        world.events.borrow_mut().clear();
        let recovered = executor.spawn(host_actor(config.clone(), Rc::clone(&world)));
        executor.run_until(105);
        assert_eq!(
            *world.events.borrow(),
            [AdminEvent::VsetRecovered {
                vset,
                verdict: crate::protocol::Verdict::ColdBoot
            }]
        );

        drop(recovered);
        executor.run_ready();

        world.blobs.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::RestoreVset { vset });
        let restore_config = DaemonConfig {
            host: HostId(4),
            replica_placement: test_replica_placement(HostId(4)),
            ..config
        };
        let restored = executor.spawn(host_actor(restore_config, Rc::clone(&world)));
        executor.run_until(110);
        assert_eq!(*world.replies.borrow(), [Err(AdminError::NotFound)]);
        let store = world.store.borrow();
        let (_, head_bytes) = &store[&layout::head_key(vset)];
        let head = HeadRecord::decode(vset, head_bytes).expect("valid retained head");
        assert_eq!(head.holder, HostId(3));
        assert_eq!(head.manifest, Some(manifest));

        drop(store);
        drop(restored);
        executor.run_ready();
    }

    #[test]
    fn failed_backed_fork_does_not_leave_a_head_claim() {
        let vset = VsetId(111);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::compute(1, 8),
            from_base: Some(999),
        });
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(3),
            cache_pages: 4,
            writeback_interval: 5,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(3)),
        };
        let mut executor = Executor::simulation(51);
        let actor = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(8);
        assert_eq!(*world.replies.borrow(), [Err(AdminError::Rejected)]);
        assert!(!world.store.borrow().contains_key(&layout::head_key(vset)));
        drop(actor);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn restore_hydrates_only_the_faulted_map_leaf() {
        let vset = VsetId(12);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::compute(1, 300),
            from_base: None,
        });
        let config = DaemonConfig {
            archive: crate::hostmeta::ArchivePolicy {
                interval: 1,
                ..Default::default()
            },
            host: HostId(5),
            cache_pages: 300,
            writeback_interval: 10,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(5)),
        };
        let mut executor = Executor::simulation(6);
        let actor = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(4);

        let pages = (0..256)
            .map(|number| PageId {
                volume: VolumeId {
                    vset,
                    idx: VolumeIdx(1),
                },
                page: PageNo(number),
            })
            .collect::<Vec<_>>();
        world.faults.borrow_mut().extend(
            pages
                .iter()
                .copied()
                .map(|page| GuestFault { page, write: true }),
        );
        executor.run_until(7);
        for (number, &page) in pages.iter().enumerate() {
            world.memory.borrow_mut().insert(
                page,
                vec![u8::try_from(number).expect("bounded"); page_size()],
            );
        }
        world.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(21),
            volume: pages[0].volume,
        });
        executor.run_until(100);
        assert_eq!(*world.sync_ok.borrow(), [ReqId(21)]);
        let store = world.store.borrow();
        let (_, head_bytes) = &store[&layout::head_key(vset)];
        let head = HeadRecord::decode(vset, head_bytes).expect("valid head");
        let manifest = head.manifest.expect("capture published");
        let (_, manifest_bytes) = &store[&layout::manifest_key(vset, manifest.fence, manifest.seq)];
        let record = JournalRecord::decode(vset, manifest_bytes).expect("valid manifest");
        let pointer = record
            .leaves
            .values()
            .next()
            .copied()
            .expect("rolled map leaf");
        let local_leaf = layout::leaf_blob(vset, pointer.fence, pointer.id);
        drop(store);
        drop(actor);
        executor.run_ready();

        let head_key = layout::head_key(vset);
        let (version, head_bytes) = world.store.borrow()[&head_key].clone();
        let mut retired_head = HeadRecord::decode(vset, &head_bytes).expect("valid head");
        retired_head.stash = None;
        world
            .store
            .borrow_mut()
            .insert(head_key, (version, retired_head.encode()));

        world.blobs.borrow_mut().clear();
        world.memory.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::RestoreVset { vset });
        let restore_config = DaemonConfig {
            archive: Default::default(),
            host: HostId(6),
            cache_pages: 16,
            writeback_interval: 10,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(6)),
        };
        let restored = executor.spawn(host_actor(restore_config, Rc::clone(&world)));
        executor.run_until(105);
        assert_eq!(
            *world.replies.borrow(),
            [Ok(AdminSuccess::VsetRestored {
                vset,
                verdict: crate::protocol::Verdict::ColdBoot
            })]
        );
        assert!(!world.blobs.borrow().contains_key(&local_leaf));

        let faulted = pages[42];
        world.faults.borrow_mut().push_back(GuestFault {
            page: faulted,
            write: false,
        });
        executor.run_until(110);
        assert!(world.blobs.borrow().contains_key(&local_leaf));
        assert_eq!(
            world.memory.borrow().get(&faulted),
            Some(&vec![42; page_size()])
        );

        drop(restored);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn pinned_checkpoint_becomes_a_faultable_fork_base() {
        let source = VsetId(13);
        let fork = VsetId(14);
        let base = 90;
        let source_page = PageId {
            volume: VolumeId {
                vset: source,
                idx: VolumeIdx(0),
            },
            page: PageNo(2),
        };
        let fork_page = PageId {
            volume: VolumeId {
                vset: fork,
                idx: VolumeIdx(0),
            },
            page: PageNo(2),
        };
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset: source,
            config: VsetConfig::compute(1, 8),
            from_base: None,
        });
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(7),
            cache_pages: 8,
            writeback_interval: 20,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(7)),
        };
        let mut executor = Executor::simulation(7);
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let actor = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(4);
        world.faults.borrow_mut().push_back(GuestFault {
            page: source_page,
            write: true,
        });
        executor.run_until(7);
        let expected = vec![0xa7; page_size()];
        world
            .memory
            .borrow_mut()
            .insert(source_page, expected.clone());
        world.admin.borrow_mut().push_back(AdminCall::Checkpoint {
            retry: ReqId(31),
            vset: source,
        });
        executor.run_until(13);
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::KeepBase { vset: source, base });
        executor.run_until(18);
        assert!(
            world
                .store
                .borrow()
                .contains_key(&layout::base_record_key(base))
        );

        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset: fork,
            config: VsetConfig::compute(1, 8),
            from_base: Some(base),
        });
        executor.run_until(23);
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::VsetForked {
                    vset: fork,
                    verdict: crate::protocol::Verdict::Resume {
                        epoch: crate::types::Epoch(0),
                        vmstate: 77,
                    },
                }))
        );
        world.faults.borrow_mut().push_back(GuestFault {
            page: fork_page,
            write: false,
        });
        executor.run_until(26);
        assert_eq!(world.memory.borrow().get(&fork_page), Some(&expected));
        assert_eq!(state.borrow().cache.base_resident_count(), 1);

        world.faults.borrow_mut().push_back(GuestFault {
            page: fork_page,
            write: false,
        });
        executor.run_until(27);
        world.faults.borrow_mut().push_back(GuestFault {
            page: fork_page,
            write: true,
        });
        executor.run_until(28);
        assert_eq!(world.memory.borrow().get(&fork_page), Some(&expected));
        assert!(state.borrow().cache.is_dirty(fork_page));
        assert_eq!(state.borrow().counters.shared_fills, 3);

        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::DeleteBase { base });
        executor.run_until(30);
        assert!(
            !world
                .store
                .borrow()
                .contains_key(&layout::base_record_key(base))
        );

        drop(actor);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn migration_accepts_only_after_the_destination_record_is_durable() {
        let vset = VsetId(15);
        let source_host = HostId(8);
        let destination_host = HostId(9);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(1),
        };
        let source = Rc::new(ModelWorld::default());
        let destination = Rc::new(ModelWorld {
            store: Rc::clone(&source.store),
            next_store_version: Rc::clone(&source.next_store_version),
            ..ModelWorld::default()
        });
        source.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::compute(1, 8),
            from_base: None,
        });
        let config = |host| DaemonConfig {
            archive: Default::default(),
            host,
            cache_pages: 8,
            writeback_interval: if host == destination_host {
                40
            } else {
                100_000_000
            },
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(host),
        };
        let mut executor = Executor::simulation(8);
        let source_actor = executor.spawn(host_actor(config(source_host), Rc::clone(&source)));
        let destination_actor = executor.spawn(host_actor(
            config(destination_host),
            Rc::clone(&destination),
        ));
        executor.run_until(5);
        source
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: true });
        executor.run_until(8);
        let expected = vec![0xc4; page_size()];
        source.memory.borrow_mut().insert(page, expected.clone());
        source.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            vset,
            to: destination_host,
        });
        executor.run_until(15);
        source.peer_inbox.borrow_mut().push_back((
            HostId(77),
            PeerMsg::MigrateAccept {
                vset,
                offer_fence: 0,
            },
        ));
        executor.run_until(16);
        assert!(
            !source
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::MigratedOut { vset }))
        );
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(20);
        assert!(
            destination
                .events
                .borrow()
                .contains(&AdminEvent::VsetMigratedIn {
                    vset,
                    verdict: crate::protocol::Verdict::Resume {
                        epoch: crate::types::Epoch(1),
                        vmstate: 77,
                    },
                })
        );
        deliver(destination_host, &destination, &source, source_host);
        executor.run_until(25);
        assert!(
            source
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::MigratedOut { vset }))
        );
        destination
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: false });
        executor.run_until(28);
        deliver(destination_host, &destination, &source, source_host);
        executor.run_until(31);
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(34);
        assert_eq!(destination.memory.borrow().get(&page), Some(&expected));

        executor.run_until(41);
        deliver(destination_host, &destination, &source, source_host);
        executor.run_until(44);
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(47);
        executor.run_until(88);
        deliver(destination_host, &destination, &source, source_host);
        executor.run_until(91);
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(94);
        assert!(
            !source
                .blobs
                .borrow()
                .contains_key(&layout::handoff_blob(vset))
        );
        assert!(!source.memory.borrow().contains_key(&page));

        drop(source_actor);
        drop(destination_actor);
        executor.run_ready();
    }

    #[test]
    fn duplicate_migration_accept_requires_the_installed_offer_fence() {
        let vset = VsetId(16);
        let source = HostId(8);
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(9),
            cache_pages: 1,
            writeback_interval: 100,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let existing = host.vsets.get_mut(&vset).expect("inserted vset");
            existing.ready = true;
            existing.peer_source = Some(source);
            existing.peer_source_offer_fence = Some(7);
        }
        let mut offered = JournalRecord {
            config: VsetConfig::compute(1, 1),
            seq: crate::types::JournalSeq(3),
            fence: 8,
            kind: RecordKind::Commit,
            capture_seq: 2,
            sync_covered_through: 2,
            database: Default::default(),
            overlay: Default::default(),
            leaves: Default::default(),
            migrated_from: None,
        };
        let mut executor = Executor::simulation(103);

        executor.block_on(migrate_in(
            Rc::clone(&state),
            Rc::clone(&world),
            source,
            vset,
            offered.encode(vset),
        ));
        assert!(
            world.peer_outbox.borrow().is_empty(),
            "a different source cut must not receive a correlated acceptance"
        );

        offered.fence = 7;
        executor.block_on(migrate_in(
            state,
            Rc::clone(&world),
            source,
            vset,
            offered.encode(vset),
        ));
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == source
                && matches!(
                    message,
                    PeerMsg::MigrateAccept {
                        vset: accepted,
                        offer_fence: 7,
                    } if *accepted == vset
                )
        }));
    }

    #[test]
    fn legacy_duplicate_accept_survives_a_peer_upgrade() {
        let vset = VsetId(17);
        let source = HostId(8);
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(9),
            cache_pages: 1,
            writeback_interval: 100,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let existing = host.vsets.get_mut(&vset).expect("inserted vset");
            existing.ready = true;
            existing.peer_source = Some(source);
            existing.peer_source_offer_fence = None;
        }
        let offered = JournalRecord {
            config: VsetConfig::compute(1, 1),
            seq: crate::types::JournalSeq(3),
            fence: 8,
            kind: RecordKind::Commit,
            capture_seq: 2,
            sync_covered_through: 2,
            database: Default::default(),
            overlay: Default::default(),
            leaves: Default::default(),
            migrated_from: None,
        };
        let mut executor = Executor::simulation(104);

        executor.block_on(migrate_in(
            state,
            Rc::clone(&world),
            source,
            vset,
            offered.encode(vset),
        ));

        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == source
                && matches!(
                    message,
                    PeerMsg::MigrateAccept {
                        vset: accepted,
                        offer_fence: 8,
                    } if *accepted == vset
                )
        }));
    }

    #[test]
    fn outbound_migration_waits_across_retries_and_cancellation_releases_its_slot() {
        let vset = VsetId(150);
        let destination = HostId(9);
        let world = Rc::new(ModelWorld::default());
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 8,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(8)),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::database(8),
            from_base: None,
        });
        let mut executor = Executor::simulation(80);
        let mut host = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(15);
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::VsetCreated { vset }))
        );
        host.cancel();
        executor.run_ready();
        while state.borrow_mut().take_scheduled_vsets(64).len() == 64 {}
        world.peer_outbox.borrow_mut().clear();
        let mut peers = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));

        let (request, reply) = bridge_request(AdminCall::MigrateOut {
            vset,
            to: destination,
        });
        let handler = executor.spawn(handle_admin(Rc::clone(&state), Rc::clone(&world), request));
        executor.run_until(5_000_100);

        assert_eq!(
            reply.blocking_recv_timeout(Duration::ZERO),
            Err(BridgeRecvError::Timeout)
        );
        let offers = world
            .peer_outbox
            .borrow()
            .iter()
            .filter(|(to, message)| {
                *to == destination
                    && matches!(message, PeerMsg::MigrateOffer { vset: found, .. } if *found == vset)
            })
            .count();
        assert!(
            offers >= 2,
            "migration must retry without reporting success; observed {offers} offer(s)"
        );

        executor.run_ready();
        assert_eq!(executor.block_on(handler), Ok(None));
        assert_eq!(state.borrow().vsets[&vset].outbound, Some(destination));
        assert!(!state.borrow().vsets[&vset].operations.migration_running());
        assert!(
            work_ready(&state.borrow(), vset)
                .iter()
                .any(|work| matches!(work, super::ScheduledWork::Reoffer))
        );
        peers.cancel();
        executor.run_ready();
    }

    #[test]
    fn migration_reservation_failure_releases_the_operation_for_retry() {
        let vset = VsetId(151);
        let destination = HostId(9);
        let world = Rc::new(ModelWorld::default());
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 8,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(8)),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::database(8),
            from_base: None,
        });
        let mut executor = Executor::simulation(81);
        let actor = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(15);
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::VsetCreated { vset }))
        );
        state.borrow_mut().config.disk_capacity = Some(0);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            vset,
            to: destination,
        });
        executor.run_until(20);
        assert!(
            world
                .replies
                .borrow()
                .contains(&Err(AdminError::Unavailable))
        );
        assert!(!state.borrow().vsets[&vset].operations.migration_running());

        state.borrow_mut().config.disk_capacity = None;
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            vset,
            to: destination,
        });
        executor.run_until(30);
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == destination
                && matches!(message, PeerMsg::MigrateOffer { vset: found, .. } if *found == vset)
        }));
        drop(actor);
        executor.run_ready();
    }

    #[test]
    fn migration_capture_failure_resumes_the_compute_guest() {
        let vset = VsetId(153);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.memory.borrow_mut().insert(page, vec![1; page_size()]);
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 8,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(8)),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::compute(1, 8),
            from_base: None,
        });
        let mut executor = Executor::simulation(82);
        let actor = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(15);
        {
            let mut host = state.borrow_mut();
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
            host.vsets
                .get_mut(&vset)
                .expect("created vset")
                .mutation_seq = 1;
        }
        state.borrow_mut().config.disk_capacity = Some(0);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            vset,
            to: HostId(9),
        });
        executor.run_until(25);

        assert!(
            world
                .replies
                .borrow()
                .contains(&Err(AdminError::Unavailable))
        );
        assert!(!world.paused_vsets.borrow().contains(&vset));
        assert!(world.unprotected.borrow().contains(&page));
        assert!(!state.borrow().vsets[&vset].operations.migration_running());
        drop(actor);
        executor.run_ready();
    }

    #[test]
    fn failed_writeback_keeps_the_mutation_slot_until_pages_are_unprotected() {
        let vset = VsetId(154);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.memory.borrow_mut().insert(page, vec![1; page_size()]);
        world.guest_unprotect_delay.set(10);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: Some(0),
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
        }

        let mut executor = Executor::simulation(83);
        let capture = executor.spawn(capture_local(Rc::clone(&state), Rc::clone(&world), vset));
        executor.run_ready();

        assert!(state.borrow().vsets[&vset].operations.mutation_blocked());
        assert!(world.unprotected.borrow().is_empty());
        executor.run_until(10);
        assert!(!state.borrow().vsets[&vset].operations.mutation_blocked());
        assert_eq!(*world.unprotected.borrow(), [page]);
        assert_eq!(executor.block_on(capture), Ok(None));
    }

    #[test]
    fn inbound_fence_is_above_every_surviving_local_artifact_namespace() {
        let vset = VsetId(156);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        state
            .borrow_mut()
            .record_blob(layout::segment_blob(vset, 4, SegId(9)), 1);
        state
            .borrow_mut()
            .record_blob(layout::leaf_blob(vset, 8, 3), 1);
        state.borrow_mut().record_blob(
            layout::journal_blob(vset, 6, crate::types::JournalSeq(11)),
            1,
        );

        assert_eq!(available_inbound_fence(&state, vset, 3), Some(9));
    }

    #[test]
    fn release_ack_requires_the_current_source_and_destination_fence() {
        let vset = VsetId(157);
        let source = HostId(7);
        let world = Rc::new(ModelWorld::default());
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let hydration_done = {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.fence = 9;
            vset_state.peer_source = Some(source);
            let (wake, wait) = oneshot();
            vset_state.hydration_waiters.push(wake);
            wait
        };
        let mut executor = Executor::simulation(85);
        let actor = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));

        world.peer_inbox.borrow_mut().extend([
            (
                HostId(6),
                PeerMsg::ReleasedAck {
                    vset,
                    release_fence: 9,
                },
            ),
            (
                source,
                PeerMsg::ReleasedAck {
                    vset,
                    release_fence: 8,
                },
            ),
        ]);
        executor.run_ready();
        assert_eq!(state.borrow().vsets[&vset].peer_source, Some(source));

        world.peer_inbox.borrow_mut().push_back((
            source,
            PeerMsg::ReleasedAck {
                vset,
                release_fence: 9,
            },
        ));
        executor.run_until(1);
        assert_eq!(state.borrow().vsets[&vset].peer_source, None);
        assert_eq!(executor.block_on(hydration_done), Ok(true));
        drop(actor);
        executor.run_ready();
    }

    #[test]
    fn legacy_outbound_handoff_accepts_its_v2_release() {
        let vset = VsetId(158);
        let destination = HostId(7);
        let world = Rc::new(ModelWorld::default());
        world
            .peer_protocol_versions
            .borrow_mut()
            .insert(destination, 2);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = false;
            vset_state.fence = 9;
            vset_state.outbound = Some(destination);
        }
        world.peer_inbox.borrow_mut().push_back((
            destination,
            PeerMsg::Released {
                vset,
                release_fence: 0,
            },
        ));

        let mut executor = Executor::simulation(86);
        let actor = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(1);

        assert!(!state.borrow().vsets.contains_key(&vset));
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == destination
                && matches!(
                    message,
                    PeerMsg::ReleasedAck {
                        vset: released,
                        release_fence: 0,
                    } if *released == vset
                )
        }));
        drop(actor);
        executor.run_ready();
    }

    #[test]
    fn legacy_inbound_handoff_accepts_its_v2_release_ack() {
        let vset = VsetId(159);
        let source = HostId(7);
        let world = Rc::new(ModelWorld::default());
        world.peer_protocol_versions.borrow_mut().insert(source, 2);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let hydration_done = {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.fence = 9;
            vset_state.peer_source = Some(source);
            let (wake, wait) = oneshot();
            vset_state.hydration_waiters.push(wake);
            wait
        };
        world.peer_inbox.borrow_mut().push_back((
            source,
            PeerMsg::ReleasedAck {
                vset,
                release_fence: 0,
            },
        ));

        let mut executor = Executor::simulation(87);
        let actor = executor.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(1);

        assert_eq!(state.borrow().vsets[&vset].peer_source, None);
        assert_eq!(executor.block_on(hydration_done), Ok(true));
        drop(actor);
        executor.run_ready();
    }

    #[test]
    fn migration_rejects_a_peer_without_the_fenced_release_protocol() {
        let vset = VsetId(158);
        let destination = HostId(9);
        let world = Rc::new(ModelWorld::default());
        world
            .peer_protocol_versions
            .borrow_mut()
            .insert(destination, 2);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            host.vsets.get_mut(&vset).expect("inserted vset").ready = true;
        }
        let mut executor = Executor::simulation(86);
        let result = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move { migrate_out(state, world, vset, destination).await }
        });

        assert_eq!(result, Some(Err(AdminError::Rejected)));
        assert!(!state.borrow().vsets[&vset].operations.migration_running());
        assert!(world.paused_vsets.borrow().is_empty());
    }

    #[test]
    fn checkpoint_capture_failure_resumes_the_compute_guest() {
        let vset = VsetId(155);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.fail_write_protect.set(true);
        world.memory.borrow_mut().insert(page, vec![1; page_size()]);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }

        let mut executor = Executor::simulation(84);
        let result = executor.block_on(checkpoint_local(
            Rc::clone(&state),
            Rc::clone(&world),
            ReqId(1),
            vset,
        ));

        assert!(result.is_none());
        assert!(!world.paused_vsets.borrow().contains(&vset));
    }

    #[test]
    fn cancelling_a_checkpoint_orders_resume_before_readmission() {
        let vset = VsetId(157);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.slow_guest_vset.set(Some(vset));
        world.guest_read_delay.set(100);
        world.guest_resume_delay.set(50);
        world.memory.borrow_mut().insert(page, vec![1; page_size()]);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }

        let mut executor = Executor::simulation(86);
        let (request, reply) = bridge_request(AdminCall::Checkpoint {
            retry: ReqId(1),
            vset,
        });
        let handler = executor.spawn(handle_admin(Rc::clone(&state), Rc::clone(&world), request));
        executor.run_until(2);
        assert!(world.paused_vsets.borrow().contains(&vset));
        assert_eq!(
            reply.blocking_recv_timeout(Duration::ZERO),
            Err(BridgeRecvError::Timeout)
        );
        executor.run_ready();
        assert_eq!(executor.block_on(handler), Ok(None));

        let mut second = executor.spawn(checkpoint_local(
            Rc::clone(&state),
            Rc::clone(&world),
            ReqId(2),
            vset,
        ));
        executor.run_until(20);
        assert!(world.unprotected.borrow().contains(&page));
        {
            let host = state.borrow();
            let operations = &host.vsets[&vset].operations;
            assert!(operations.guest_resume_pending());
            assert!(operations.mutation_owner().is_none());
        }

        executor.run_until(55);
        assert!(world.paused_vsets.borrow().contains(&vset));
        assert!(matches!(
            state.borrow().vsets[&vset].operations.mutation_owner(),
            Some(MutationOwner::Capture(CaptureKind::Checkpoint))
        ));
        second.cancel();
        executor.run_until(110);

        assert!(!world.paused_vsets.borrow().contains(&vset));
        assert!(
            !state.borrow().vsets[&vset]
                .operations
                .guest_resume_pending()
        );
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );
    }

    #[test]
    fn cancelling_after_early_resume_unprotects_abandoned_pages() {
        let vset = VsetId(159);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.blob_write_delay.set(100);
        world.memory.borrow_mut().insert(page, vec![1; page_size()]);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }

        let mut executor = Executor::simulation(88);
        let (request, reply) = bridge_request(AdminCall::Checkpoint {
            retry: ReqId(1),
            vset,
        });
        let handler = executor.spawn(handle_admin(Rc::clone(&state), Rc::clone(&world), request));
        executor.run_until(2);
        assert!(!world.paused_vsets.borrow().contains(&vset));
        assert_eq!(
            reply.blocking_recv_timeout(Duration::ZERO),
            Err(BridgeRecvError::Timeout)
        );
        executor.run_ready();

        assert_eq!(executor.block_on(handler), Ok(None));
        assert!(world.unprotected.borrow().contains(&page));
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );
    }

    #[test]
    fn cancelling_an_outbound_migration_resumes_the_paused_guest() {
        let vset = VsetId(158);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.slow_guest_vset.set(Some(vset));
        world.guest_read_delay.set(100);
        world.memory.borrow_mut().insert(page, vec![1; page_size()]);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.mutation_seq = 1;
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }

        let mut executor = Executor::simulation(87);
        let mut migration = executor.spawn(crate::engine::migrate_out(
            Rc::clone(&state),
            Rc::clone(&world),
            vset,
            HostId(9),
        ));
        executor.run_until(2);
        assert!(world.paused_vsets.borrow().contains(&vset));
        migration.cancel();
        executor.run_ready();

        assert!(!world.paused_vsets.borrow().contains(&vset));
        assert!(!state.borrow().vsets[&vset].operations.migration_running());
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );
    }

    #[test]
    fn outbound_migration_hydrates_lazy_leaves_before_reserving_the_cut() {
        let vset = VsetId(154);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 8,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(8)),
        })));
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(vset, VsetConfig::compute(1, 8));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.fence = 2;
            vset_state.peer_source = Some(HostId(7));
            vset_state.leaf_table.insert(
                span_of(page),
                LeafPtr {
                    base: 0,
                    fence: 1,
                    id: 1,
                },
            );
        }
        let world = Rc::new(ModelWorld::default());
        let mut executor = Executor::simulation(83);
        let mut migration = executor.spawn(crate::engine::migrate_out(
            Rc::clone(&state),
            world,
            vset,
            HostId(9),
        ));
        executor.run_ready();

        assert_eq!(state.borrow().scheduled_vset_count(), 1);
        assert_eq!(state.borrow().vsets[&vset].hydration_waiters.len(), 1);
        migration.cancel();
        executor.run_ready();
    }

    #[test]
    fn failed_hydration_resolves_the_waiting_outbound_migration() {
        let vset = VsetId(156);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let incarnation = {
            let mut host = state.borrow_mut();
            let incarnation = host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            vset_state.ready = true;
            vset_state.fence = 2;
            vset_state.peer_source = Some(HostId(7));
            vset_state.page_locs.insert(
                page,
                (
                    Gen(0),
                    PageLoc {
                        base: 0,
                        fence: 1,
                        seg: SegId(1),
                        offset: 0,
                        len: u32::try_from(page_size()).expect("page size fits"),
                    },
                ),
            );
            incarnation
        };
        let world = Rc::new(ModelWorld::default());
        let mut executor = Executor::simulation(85);
        let migration = executor.spawn(crate::engine::migrate_out(
            Rc::clone(&state),
            world,
            vset,
            HostId(9),
        ));
        executor.run_ready();
        crate::engine::migration::finish_hydration(&state, vset, incarnation);

        assert_eq!(
            executor.block_on(migration),
            Ok(Some(Err(AdminError::Unavailable)))
        );
        assert!(state.borrow().vsets[&vset].hydration_waiters.is_empty());
    }

    #[test]
    fn hydration_completion_wakes_shared_mutation_waiters() {
        let vset = VsetId(157);
        let state = Rc::new(RefCell::new(HostState::new(DaemonConfig {
            archive: Default::default(),
            host: HostId(8),
            cache_pages: 1,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        })));
        let (waiter, wake) = oneshot();
        let incarnation = {
            let mut host = state.borrow_mut();
            let incarnation = host.insert_fresh(vset, VsetConfig::compute(1, 1));
            let vset_state = host.vsets.get_mut(&vset).expect("inserted vset");
            assert!(
                vset_state
                    .operations
                    .try_start_mutation(MutationOwner::Hydration)
            );
            vset_state.mutation_waiters.push(waiter);
            incarnation
        };

        crate::engine::migration::finish_hydration(&state, vset, incarnation);

        let mut executor = Executor::simulation(86);
        assert_eq!(executor.block_on(wake), Ok(()));
        assert!(state.borrow().vsets[&vset].mutation_waiters.is_empty());
    }

    #[test]
    fn migration_replaces_a_stale_local_handoff_marker() {
        let vset = VsetId(152);
        let source = HostId(8);
        let old_destination = HostId(7);
        let destination = HostId(9);
        let world = Rc::new(ModelWorld::default());
        let config = DaemonConfig {
            archive: Default::default(),
            host: source,
            cache_pages: 8,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(source),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::database(8),
            from_base: None,
        });
        let mut executor = Executor::simulation(82);
        let actor = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        executor.run_until(15);

        let handoff_name = layout::handoff_blob(vset);
        let stale_handoff = crate::engine::migration::encode_handoff(vset, old_destination);
        world
            .blobs
            .borrow_mut()
            .insert(handoff_name.clone(), stale_handoff.clone());
        state
            .borrow_mut()
            .record_blob(handoff_name.clone(), stale_handoff.len() as u64);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            vset,
            to: destination,
        });
        executor.run_until(30);

        assert_eq!(
            world
                .blobs
                .borrow()
                .get(&handoff_name)
                .and_then(|bytes| crate::engine::migration::decode_handoff(vset, bytes)),
            Some(destination)
        );
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == destination
                && matches!(message, PeerMsg::MigrateOffer { vset: offered, .. } if *offered == vset)
        }));

        drop(actor);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn detached_backed_database_migration_claims_head_and_serves_tail_from_source() {
        let vset = VsetId(71);
        let source_host = HostId(40);
        let destination_host = HostId(41);
        let source = Rc::new(ModelWorld::default());
        let destination = Rc::new(ModelWorld {
            store: Rc::clone(&source.store),
            next_store_version: Rc::clone(&source.next_store_version),
            ..ModelWorld::default()
        });
        source.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::database(8),
            from_base: None,
        });
        let config = |host| DaemonConfig {
            archive: Default::default(),
            host,
            cache_pages: 4,
            writeback_interval: 100_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(host),
        };
        let mut executor = Executor::simulation(71);
        let source_actor = executor.spawn(host_actor(config(source_host), Rc::clone(&source)));
        let destination_actor = executor.spawn(host_actor(
            config(destination_host),
            Rc::clone(&destination),
        ));
        executor.run_until(6);
        source
            .admin
            .borrow_mut()
            .push_back(AdminCall::AttachDatabase { vset, vm: VmId(10) });
        executor.run_until(9);
        let attachment = source
            .replies
            .borrow()
            .iter()
            .find_map(|reply| match reply {
                Ok(AdminSuccess::DatabaseAttached { attachment, .. }) => Some(*attachment),
                _ => None,
            })
            .expect("database attachment");
        let request = |req, op| DatabaseRequest {
            req: ReqId(req),
            vset,
            attachment,
            op,
        };
        source.database_requests.borrow_mut().extend([
            request(
                202,
                DatabaseOp::Open {
                    handle: 1,
                    file: DatabaseFile::Main,
                    create: true,
                },
            ),
            request(
                203,
                DatabaseOp::Write {
                    handle: 1,
                    offset: 17,
                    bytes: b"migrated database".to_vec(),
                },
            ),
            request(204, DatabaseOp::Sync { handle: 1 }),
            request(205, DatabaseOp::Close { handle: 1 }),
        ]);
        executor.run_until(30);
        source
            .admin
            .borrow_mut()
            .push_back(AdminCall::BeginDetachDatabase {
                vset,
                attachment,
                mode: DetachMode::Graceful,
            });
        executor.run_until(44);
        source
            .admin
            .borrow_mut()
            .push_back(AdminCall::FinishDetachDatabase { vset, attachment });
        executor.run_until(48);
        source.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            vset,
            to: destination_host,
        });
        executor.run_until(70);
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(90);
        assert!(
            destination
                .events
                .borrow()
                .contains(&AdminEvent::VsetMigratedIn {
                    vset,
                    verdict: crate::protocol::Verdict::DatabaseReady { synced_through: 2 },
                })
        );
        let (_, head_bytes) = destination.store.borrow()[&layout::head_key(vset)].clone();
        let head = HeadRecord::decode(vset, &head_bytes).expect("claimed head");
        assert_eq!(head.holder, destination_host);
        assert_ne!(head.fence, 0);
        deliver(destination_host, &destination, &source, source_host);
        executor.run_until(94);
        assert!(
            source
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::MigratedOut { vset }))
        );

        destination
            .admin
            .borrow_mut()
            .push_back(AdminCall::AttachDatabase { vset, vm: VmId(11) });
        executor.run_until(97);
        let migrated_attachment = destination
            .replies
            .borrow()
            .iter()
            .find_map(|reply| match reply {
                Ok(AdminSuccess::DatabaseAttached { attachment, .. }) => Some(*attachment),
                _ => None,
            })
            .expect("migrated database attachment");
        let migrated_request = |req, op| DatabaseRequest {
            req: ReqId(req),
            vset,
            attachment: migrated_attachment,
            op,
        };
        destination.database_requests.borrow_mut().extend([
            migrated_request(
                210,
                DatabaseOp::Open {
                    handle: 2,
                    file: DatabaseFile::Main,
                    create: false,
                },
            ),
            migrated_request(
                211,
                DatabaseOp::Read {
                    handle: 2,
                    offset: 17,
                    len: 17,
                },
            ),
        ]);
        executor.run_until(101);
        deliver(destination_host, &destination, &source, source_host);
        executor.run_until(104);
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(108);
        assert!(
            destination
                .database_replies
                .borrow()
                .contains(&DatabaseReply::Read {
                    req: ReqId(211),
                    bytes: b"migrated database".to_vec(),
                    eof: false,
                })
        );

        drop(source_actor);
        drop(destination_actor);
        executor.run_ready();
    }

    #[test]
    fn recovered_handoff_reoffers_without_resuming_the_source() {
        let vset = VsetId(16);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig::compute(1, 4),
            from_base: None,
        });
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(10),
            cache_pages: 4,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(10)),
        };
        let mut executor = Executor::simulation(9);
        let actor = executor.spawn(host_actor(config.clone(), Rc::clone(&world)));
        executor.run_until(4);
        world
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: true });
        executor.run_until(7);
        world
            .memory
            .borrow_mut()
            .insert(page, vec![0xd1; page_size()]);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            vset,
            to: HostId(11),
        });
        executor.run_until(15);
        assert!(
            world
                .blobs
                .borrow()
                .contains_key(&layout::handoff_blob(vset))
        );

        // The destination may claim the durable head before the source
        // crashes. Recovery must still preserve the outbound peer tier.
        {
            let mut store = world.store.borrow_mut();
            let (_, bytes) = store.get_mut(&layout::head_key(vset)).expect("head");
            let mut head = HeadRecord::decode(vset, bytes).expect("valid head");
            head.holder = HostId(11);
            *bytes = head.encode();
        }

        drop(actor);
        executor.run_ready();
        world.peer_outbox.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        let recovered = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(18);
        assert!(
            world.replies.borrow().is_empty(),
            "unexpected recovery replies: {:?}",
            *world.replies.borrow()
        );
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == HostId(11)
                && matches!(message, PeerMsg::MigrateOffer { vset: offered, .. } if *offered == vset)
        }));

        drop(recovered);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn passive_replica_acks_only_after_artifact_and_commit_appends() {
        use crate::format::crc32c;
        use crate::journal::{DatabaseMeta, RecordKind, VsetKind};
        use crate::placement::PeerCandidate;
        use crate::protocol::{ReplicaArtifact, ReplicaCommitInfo};
        use crate::segment::SegmentBatchBuilder;

        let source = HostId(20);
        let receiver = HostId(21);
        let vset = VsetId(17);
        let assignment_epoch = 1;
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let mut builder = SegmentBatchBuilder::new(vset, 3, crate::types::SegId(0));
        builder.add(page, crate::types::Gen(1), &vec![0xe2; page_size()]);
        let (_, segment, entries) = builder.finish().pop().expect("one segment");
        let artifact = ReplicaArtifact::Segment {
            fence: 3,
            seg: crate::types::SegId(0),
        };
        let location = entries[0].2;
        let record = JournalRecord {
            config: VsetConfig {
                kind: VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 4,
            },
            seq: crate::types::JournalSeq(1),
            fence: 3,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            database: DatabaseMeta::default(),
            overlay: [(page, (crate::types::Gen(1), location))]
                .into_iter()
                .collect(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let info = ReplicaCommitInfo {
            writer_fence: 3,
            seq: record.seq,
            sync_covered_through: 1,
        };
        let world = Rc::new(ModelWorld::default());
        let config = DaemonConfig {
            archive: Default::default(),
            host: receiver,
            cache_pages: 4,
            writeback_interval: 100,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(crate::hostmeta::ReplicaPlacementConfig {
                membership_epoch: 7,
                local_failure_domain: 2,
                roster: vec![
                    PeerCandidate {
                        host: source,
                        weight: 1,
                        failure_domain: 1,
                        drained: false,
                    },
                    PeerCandidate {
                        host: receiver,
                        weight: 1,
                        failure_domain: 2,
                        drained: false,
                    },
                ],
            }),
        };
        let mut executor = Executor::simulation(10);
        let actor = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(2);
        world.peer_inbox.borrow_mut().push_back((
            source,
            PeerMsg::ReplicaPut {
                vset,
                assignment_epoch,
                artifact,
                checksum: crc32c(&segment),
                bytes: segment,
            },
        ));
        executor.run_until(5);
        assert!(matches!(
            world.peer_outbox.borrow().last(),
            Some((host, PeerMsg::ReplicaPutAck { artifact: found, .. }))
                if *host == source && *found == artifact
        ));
        world.peer_inbox.borrow_mut().push_back((
            source,
            PeerMsg::ReplicaCommit {
                vset,
                assignment_epoch,
                info,
                required: vec![artifact],
                record: record.encode(vset),
            },
        ));
        executor.run_until(8);
        assert!(world.peer_outbox.borrow().iter().any(|(host, message)| {
            *host == source
                && matches!(message, PeerMsg::ReplicaCommitAck { info: found, .. } if *found == info)
        }));
        let spool = world.blobs.borrow();
        let bytes = &spool[&layout::replica_spool_blob(source, vset, assignment_epoch)];
        let scan = crate::replica_spool::scan_replica_spool(bytes).expect("valid spool");
        assert_eq!(scan.commits.last().map(|commit| commit.info), Some(info));

        drop(spool);
        drop(actor);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn peer_stashed_sync_waits_for_the_exact_passive_commit() {
        use crate::journal::VsetKind;
        use crate::placement::PeerCandidate;

        let primary_host = HostId(30);
        let passive_host = HostId(31);
        let vset = VsetId(18);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let roster = vec![
            PeerCandidate {
                host: primary_host,
                weight: 1,
                failure_domain: 1,
                drained: false,
            },
            PeerCandidate {
                host: passive_host,
                weight: 1,
                failure_domain: 2,
                drained: false,
            },
        ];
        let config = |host, domain| DaemonConfig {
            archive: crate::hostmeta::ArchivePolicy {
                interval: 1,
                ..Default::default()
            },
            host,
            cache_pages: 8,
            writeback_interval: 10,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(crate::hostmeta::ReplicaPlacementConfig {
                membership_epoch: 9,
                local_failure_domain: domain,
                roster: roster.clone(),
            }),
        };
        let primary = Rc::new(ModelWorld::default());
        let passive = Rc::new(ModelWorld::default());
        primary.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig {
                kind: VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 8,
            },
            from_base: None,
        });
        let mut executor = Executor::simulation(11);
        let primary_actor =
            executor.spawn(host_actor(config(primary_host, 1), Rc::clone(&primary)));
        let passive_actor =
            executor.spawn(host_actor(config(passive_host, 2), Rc::clone(&passive)));
        executor.run_until(4);
        primary
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: true });
        executor.run_until(7);
        primary
            .memory
            .borrow_mut()
            .insert(page, vec![0xf3; page_size()]);
        primary.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(61),
            volume: page.volume,
        });
        executor.run_until(9);
        assert!(primary.sync_ok.borrow().is_empty());

        for horizon in 10..50 {
            executor.run_until(horizon);
            if !primary.peer_outbox.borrow().is_empty() {
                deliver(primary_host, &primary, &passive, passive_host);
            }
            if !passive.peer_outbox.borrow().is_empty() {
                let uploaded = passive
                    .store
                    .borrow()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<Vec<_>>();
                for (key, value) in uploaded {
                    if key != layout::head_key(vset) {
                        primary
                            .next_store_version
                            .set(primary.next_store_version.get().max(value.0));
                        primary.store.borrow_mut().insert(key, value);
                    }
                }
                deliver(passive_host, &passive, &primary, primary_host);
            }
        }
        assert_eq!(*primary.sync_ok.borrow(), [ReqId(61)]);
        let spools = passive.blobs.borrow();
        assert!(
            !spools.contains_key(&layout::replica_spool_blob(primary_host, vset, 1)),
            "store-covered replica spool must be released"
        );
        let (_, head_bytes) = &primary.store.borrow()[&layout::head_key(vset)];
        let head = HeadRecord::decode(vset, head_bytes).expect("published peer head");
        assert_eq!(head.manifest.map(|pointer| pointer.capture_seq), Some(1));
        assert_eq!(
            head.stash.map(|stash| stash.active_peer),
            Some(passive_host)
        );

        drop(spools);
        drop(primary_actor);
        drop(passive_actor);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn unreachable_stash_is_seeded_and_activated_before_sync_ack() {
        use crate::journal::VsetKind;
        use crate::placement::{PeerCandidate, rank_stash_candidates};

        let primary_host = HostId(50);
        let candidates = [HostId(51), HostId(52)];
        let vset = VsetId(72);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let roster = vec![
            PeerCandidate {
                host: primary_host,
                weight: 1,
                failure_domain: 1,
                drained: false,
            },
            PeerCandidate {
                host: candidates[0],
                weight: 1,
                failure_domain: 2,
                drained: false,
            },
            PeerCandidate {
                host: candidates[1],
                weight: 1,
                failure_domain: 3,
                drained: false,
            },
        ];
        let ranked = rank_stash_candidates(10, primary_host, 1, vset, &roster);
        let unreachable = ranked[0];
        let replacement_host = ranked[1];
        let domain = roster
            .iter()
            .find(|candidate| candidate.host == replacement_host)
            .expect("replacement in roster")
            .failure_domain;
        let config = |host, local_failure_domain| DaemonConfig {
            archive: Default::default(),
            host,
            cache_pages: 8,
            writeback_interval: 10,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(crate::hostmeta::ReplicaPlacementConfig {
                membership_epoch: 10,
                local_failure_domain,
                roster: roster.clone(),
            }),
        };
        let primary = Rc::new(ModelWorld::default());
        let replacement = Rc::new(ModelWorld {
            store: Rc::clone(&primary.store),
            next_store_version: Rc::clone(&primary.next_store_version),
            ..ModelWorld::default()
        });
        primary.admin.borrow_mut().push_back(AdminCall::CreateVset {
            vset,
            config: VsetConfig {
                kind: VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 8,
            },
            from_base: None,
        });
        let mut executor = Executor::simulation(72);
        let primary_actor =
            executor.spawn(host_actor(config(primary_host, 1), Rc::clone(&primary)));
        let replacement_actor = executor.spawn(host_actor(
            config(replacement_host, domain),
            Rc::clone(&replacement),
        ));
        executor.run_until(5);
        primary
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: true });
        executor.run_until(7);
        primary
            .memory
            .borrow_mut()
            .insert(page, vec![0x8d; page_size()]);
        primary.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(301),
            volume: page.volume,
        });

        for horizon in 8..100 {
            executor.run_until(horizon);
            let outbound = primary
                .peer_outbox
                .borrow_mut()
                .drain(..)
                .collect::<Vec<_>>();
            for (target, message) in outbound {
                if target == replacement_host {
                    replacement
                        .peer_inbox
                        .borrow_mut()
                        .push_back((primary_host, message));
                } else {
                    assert_eq!(target, unreachable);
                }
            }
            let replies = replacement
                .peer_outbox
                .borrow_mut()
                .drain(..)
                .collect::<Vec<_>>();
            for (target, message) in replies {
                assert_eq!(target, primary_host);
                primary
                    .peer_inbox
                    .borrow_mut()
                    .push_back((replacement_host, message));
            }
            if primary.sync_ok.borrow().contains(&ReqId(301)) {
                break;
            }
        }
        assert_eq!(*primary.sync_ok.borrow(), [ReqId(301)]);
        let (_, head_bytes) = &primary.store.borrow()[&layout::head_key(vset)];
        let head = HeadRecord::decode(vset, head_bytes).expect("transitioned head");
        let assignment = head.stash.expect("stash assignment");
        assert_eq!(assignment.active_peer, replacement_host);
        assert_eq!(assignment.active_assignment_epoch, 2);
        assert_eq!(assignment.transition_peer, None);
        assert!(head.retired_stashes.iter().any(|retired| {
            retired.peer == unreachable
                && retired.assignment_epoch == 1
                && retired.through.sync_covered_through == 1
        }));
        let spool = replacement.blobs.borrow();
        let scan = crate::replica_spool::scan_replica_spool(
            &spool[&layout::replica_spool_blob(primary_host, vset, 2)],
        )
        .expect("replacement spool");
        assert_eq!(
            scan.commits
                .last()
                .map(|commit| commit.info.sync_covered_through),
            Some(1)
        );

        drop(spool);
        drop(primary_actor);
        drop(replacement_actor);
        executor.run_ready();
    }
}
