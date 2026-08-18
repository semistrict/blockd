use std::rc::Rc;

use blockd_exec::channel::{oneshot, unbounded};
use blockd_exec::inject::{Lane, injector};
use blockd_exec::{Either, OneOf3, TaskSet, delay, select2, select3, yield_now};

use super::ctx::HostCtx;
use super::lease::{bootstrap_host_authority, host_session_monitor};
use super::state::PendingSync;
use super::{
    HostFatal, SharedHost, reclaim_backed_blx_files, reconcile_backed_recovery_event,
    recover_local, serve_fault,
};
use crate::hostmeta::HostConfig;
use crate::journal::VolumeKind;
use crate::protocol::{AdminCall, AdminEvent};
use crate::world::{AdminIo, GuestMem, HostWorld};

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
    HostCtx::new(state, world).run_actor().await;
}

impl<W: HostWorld> HostCtx<W> {
    async fn run_actor(self) {
        let (send_fatal, receive_fatal) = oneshot();
        self.state().borrow_mut().install_fatal_signal(send_fatal);
        let _fatal_signal = FatalSignalLease(Rc::clone(self.state()));
        let outcome = select2(receive_fatal, self.clone().run()).await;
        let failure = match outcome {
            Either::First(Ok(failure)) | Either::Second(Err(failure)) => failure,
            Either::First(Err(_)) => HostFatal::new("host fatal signal closed"),
            Either::Second(Ok(())) => return,
        };
        AdminIo::host_failed(self.world().as_ref(), failure).await;
    }

    async fn run(self) -> Result<(), HostFatal> {
        let config = self.state().borrow().config.clone();
        bootstrap_host_authority(self.state(), self.world().as_ref())
            .await
            .map_err(|_| HostFatal::new("host authority bootstrap failed"))?;
        let Ok(verdicts) = recover_local(Rc::clone(self.state()), self.world().as_ref()).await
        else {
            return Err(HostFatal::new("local recovery scan failed"));
        };
        for (volume, verdict) in verdicts {
            AdminIo::emit_admin_event(
                self.world().as_ref(),
                AdminEvent::VolumeRecovered { volume, verdict },
            )
            .await;
        }
        let backed = self
            .state()
            .borrow()
            .volumes
            .iter()
            .filter_map(|(&volume, state)| state.operations.recovery_pending().then_some(volume))
            .collect::<Vec<_>>();
        self.reconcile_backed_volumes(&backed).await?;
        let initial_work = self
            .state()
            .borrow()
            .volumes
            .iter()
            .filter_map(|(&volume, volume_state)| {
                (volume_state.ready || volume_state.outbound.is_some()).then_some(volume)
            })
            .collect::<Vec<_>>();
        for volume in initial_work {
            self.state().borrow_mut().schedule_volume(volume);
        }
        let mut children = TaskSet::new();
        children.spawn(host_session_monitor(
            Rc::clone(self.state()),
            Rc::clone(self.world()),
        ));
        children.spawn(self.clone().admin_source());
        children.spawn(self.clone().fault_source());
        children.spawn(self.clone().sync_source());
        children.spawn(self.clone().peer_source());
        children.spawn(
            self.clone()
                .scheduled_work_source(config.writeback_interval, true),
        );
        children.spawn(super::store_gc::store_gc_actor(
            Rc::clone(self.state()),
            Rc::clone(self.world()),
        ));
        loop {
            delay(config.writeback_interval).await;
            if reclaim_backed_blx_files(Rc::clone(self.state()), self.world().as_ref())
                .await
                .is_err()
            {
                return Err(HostFatal::new("backed blx reclaim failed"));
            }
            let accessed = GuestMem::harvest_accessed(self.world().as_ref()).await;
            let mut host = self.state().borrow_mut();
            host.cache.age(|| accessed);
            host.wedge_tick();
        }
    }

    async fn reconcile_backed_volumes(
        &self,
        volumes: &[crate::types::VolumeId],
    ) -> Result<(), HostFatal> {
        for batch in volumes.chunks(STARTUP_RECONCILE_CONCURRENCY) {
            let mut outcomes = Vec::with_capacity(batch.len());
            for &volume in batch {
                let volume = self.volume(volume);
                outcomes.push(blockd_exec::spawn(async move {
                    reconcile_backed_recovery_event(
                        Rc::clone(volume.host().state()),
                        Rc::clone(volume.host().world()),
                        volume.id(),
                    )
                    .await
                }));
            }
            for outcome in outcomes {
                let event = outcome
                    .await
                    .map_err(|_| HostFatal::new("startup reconciliation actor cancelled"))?;
                if let Some(event) = event {
                    AdminIo::emit_admin_event(self.world().as_ref(), event).await;
                }
            }
        }
        for &volume in volumes {
            self.volume(volume).retry_releases().await;
        }
        Ok(())
    }
}

#[cfg(test)]
async fn reconcile_backed_volumes<W: HostWorld>(
    state: &SharedHost,
    world: &Rc<W>,
    volumes: &[crate::types::VolumeId],
) -> Result<(), HostFatal> {
    HostCtx::new(Rc::clone(state), Rc::clone(world))
        .reconcile_backed_volumes(volumes)
        .await
}

impl super::HostState {
    fn work_ready(&self, volume: crate::types::VolumeId) -> Vec<ScheduledWork> {
        let Some(volume_state) = self.volumes.get(&volume) else {
            return Vec::new();
        };
        let idle_commit = !volume_state.operations.mutation_blocked()
            && !volume_state.operations.migration_running();
        let mut work = Vec::new();
        if volume_state.ready
            && idle_commit
            && (self.cache.has_dirty_of(volume)
                || !volume_state.pending_tombstones.is_empty()
                || volume_state
                    .pending_syncs
                    .iter()
                    .any(|sync| sync.barrier > volume_state.local_covered_through))
        {
            work.push(ScheduledWork::Capture);
        }
        let remote_tail_complete = volume_state.peer_source.is_some()
            && !volume_state
                .page_locs
                .values()
                .any(|(_, location)| location.base == 0 && location.fence < volume_state.fence);
        if !volume_state.operations.mutation_blocked()
            && ((volume_state.ready && volume_state.peer_source.is_some()) || remote_tail_complete)
        {
            work.push(ScheduledWork::Hydrate);
        }
        let replication_needed = volume_state.best_record.as_ref().is_some_and(|record| {
            let required = record.commit_info();
            let published_covers = volume_state
                .peer_published
                .is_some_and(|published| published >= required);
            !published_covers
                && volume_state
                    .peer_committed
                    .is_none_or(|committed| committed < required)
        });
        if volume_state.ready
            && !volume_state.operations.replication_running()
            && volume_state.stash_assignment.is_some()
            && replication_needed
        {
            work.push(ScheduledWork::Replicate);
        }
        if self
            .replica_releases
            .iter()
            .any(|(_, pending, _, _)| *pending == volume)
        {
            work.push(ScheduledWork::Release);
        }
        if !volume_state.operations.publication_running()
            && !volume_state.operations.migration_running()
            && volume_state.outbound.is_none()
            && volume_state.stash_assignment.is_some()
            && volume_state.peer_committed.is_some_and(|committed| {
                volume_state.peer_published.is_none_or(|published| {
                    (published.writer_fence, published.seq)
                        < (committed.writer_fence, committed.seq)
                })
            })
        {
            work.push(ScheduledWork::Archive);
        }
        if volume_state.outbound.is_some()
            && volume_state.best_record.is_some()
            && !volume_state.operations.migration_running()
        {
            work.push(ScheduledWork::Reoffer);
        }
        work
    }
}

#[cfg(test)]
fn work_ready(host: &super::HostState, volume: crate::types::VolumeId) -> Vec<ScheduledWork> {
    host.work_ready(volume)
}

impl<W: HostWorld> HostCtx<W> {
    fn schedule_volume_work(
        &self,
        children: &mut TaskSet,
        child_volumes: &mut std::collections::BTreeMap<blockd_exec::TaskId, crate::types::VolumeId>,
        retry_next_cadence: &std::collections::BTreeSet<crate::types::VolumeId>,
    ) -> usize {
        if children.len() == SCHEDULED_WORK_CONCURRENCY {
            return 0;
        }
        let mut examined = 0;
        while examined < SCHEDULED_WORK_CONCURRENCY && children.len() < SCHEDULED_WORK_CONCURRENCY {
            let Some(volume) = self.state().borrow_mut().take_scheduled_volumes(1).pop() else {
                break;
            };
            examined += 1;
            if retry_next_cadence.contains(&volume) {
                continue;
            }
            let work = self.state().borrow().work_ready(volume);
            for work in work {
                if children.len() == SCHEDULED_WORK_CONCURRENCY {
                    self.state().borrow_mut().schedule_volume(volume);
                    break;
                }
                let volume_ctx = self.volume(volume);
                let child = children.spawn(async move {
                    match work {
                        ScheduledWork::Capture => {
                            let _ = volume_ctx.capture().await;
                        }
                        ScheduledWork::Hydrate => {
                            volume_ctx.hydrate().await;
                        }
                        ScheduledWork::Replicate => {
                            volume_ctx.replicate().await;
                        }
                        ScheduledWork::Release => {
                            volume_ctx.retry_releases().await;
                        }
                        ScheduledWork::Archive => {
                            volume_ctx.publish().await;
                        }
                        ScheduledWork::Reoffer => {
                            volume_ctx.reoffer().await;
                        }
                    }
                });
                child_volumes.insert(child, volume);
            }
        }
        examined
    }

    async fn scheduled_work_source(self, writeback_interval: u64, start_immediately: bool) {
        let mut children = TaskSet::new();
        let mut child_volumes = std::collections::BTreeMap::new();
        let mut retry_next_cadence = std::collections::BTreeSet::new();
        let mut first = true;
        loop {
            if !start_immediately || !std::mem::take(&mut first) {
                delay(writeback_interval).await;
            }
            let mut drained = false;
            while let Ok(child) = children.try_next_done() {
                let volume = child_volumes
                    .remove(&child)
                    .expect("scheduled child tracked");
                retry_next_cadence.insert(volume);
                drained = true;
            }
            {
                let mut host = self.state().borrow_mut();
                for volume in std::mem::take(&mut retry_next_cadence) {
                    host.schedule_volume(volume);
                }
                for volume in host.take_disk_reclaim_scan_volumes(SCHEDULED_WORK_CONCURRENCY) {
                    host.schedule_volume(volume);
                }
            }
            if drained {
                yield_now().await;
            }
            let mut remaining = self.state().borrow().scheduled_volume_count();
            while remaining > 0 {
                let examined = self.schedule_volume_work(
                    &mut children,
                    &mut child_volumes,
                    &retry_next_cadence,
                );
                remaining = self.state().borrow().scheduled_volume_count();
                if remaining == 0 {
                    break;
                }
                if examined == 0 || children.len() == SCHEDULED_WORK_CONCURRENCY {
                    let Some(child) = children.next_done().await else {
                        return;
                    };
                    let volume = child_volumes
                        .remove(&child)
                        .expect("scheduled child tracked");
                    retry_next_cadence.insert(volume);
                    yield_now().await;
                } else {
                    yield_now().await;
                }
            }
        }
    }
}

#[cfg(test)]
fn schedule_volume_work<W: HostWorld>(
    state: &SharedHost,
    world: &Rc<W>,
    children: &mut TaskSet,
    child_volumes: &mut std::collections::BTreeMap<blockd_exec::TaskId, crate::types::VolumeId>,
    retry_next_cadence: &std::collections::BTreeSet<crate::types::VolumeId>,
) -> usize {
    HostCtx::new(Rc::clone(state), Rc::clone(world)).schedule_volume_work(
        children,
        child_volumes,
        retry_next_cadence,
    )
}

#[cfg(test)]
async fn scheduled_work_source<W: HostWorld>(
    state: SharedHost,
    world: Rc<W>,
    writeback_interval: u64,
    start_immediately: bool,
) {
    HostCtx::new(state, world)
        .scheduled_work_source(writeback_interval, start_immediately)
        .await;
}

impl<W: HostWorld> HostCtx<W> {
    async fn admin_source(self) {
        let mut actors = TaskSet::new();
        loop {
            let event = if actors.len() == ADMIN_CONCURRENCY {
                Either::First(actors.next_done().await)
            } else {
                select2(
                    actors.next_done(),
                    AdminIo::next_admin(self.world().as_ref()),
                )
                .await
            };
            match event {
                Either::First(Some(_)) => {}
                Either::First(None) | Either::Second(None) => return,
                Either::Second(Some(request)) => {
                    let ctx = self.clone();
                    actors.spawn(async move {
                        ctx.handle_admin(request).await;
                    });
                }
            }
        }
    }

    async fn handle_admin(self, request: crate::world::AdminRequest) {
        let (call, mut reply) = request.into_parts();
        if !self.state().borrow().authority_serving()
            || admin_volume(call)
                .is_some_and(|volume| !self.state().borrow().volume_authorized(volume))
        {
            let _ = reply.send(Err(crate::protocol::AdminError::Unavailable));
            return;
        }
        let response = match call {
            AdminCall::CreateVolume {
                volume,
                config,
                from_base: Some(base),
            } => self.volume(volume).create_fork(config, base).await,
            AdminCall::KeepBase { volume, base } => Some(self.volume(volume).keep_base(base).await),
            AdminCall::DeleteBase { base } => Some(self.delete_base(base).await),
            AdminCall::CreateVolume {
                volume,
                config,
                from_base: None,
            } => self.volume(volume).create(config).await,
            AdminCall::Checkpoint { retry, volume } => {
                match select2(self.volume(volume).checkpoint(retry), reply.closed()).await {
                    Either::First(response) => response,
                    Either::Second(()) => return,
                }
            }
            AdminCall::RestoreVolume { volume } => Some(self.volume(volume).restore().await),
            AdminCall::MigrateOut { volume, to } => {
                match select2(self.volume(volume).migrate_to(to), reply.closed()).await {
                    Either::First(response) => response,
                    Either::Second(()) => return,
                }
            }
        };
        if let Some(response) = response {
            let _ = reply.send(response);
        }
    }
}

#[cfg(test)]
async fn admin_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    HostCtx::new(state, world).admin_source().await;
}

#[cfg(test)]
async fn handle_admin<W: HostWorld>(
    state: SharedHost,
    world: Rc<W>,
    request: crate::world::AdminRequest,
) {
    HostCtx::new(state, world).handle_admin(request).await;
}

fn admin_volume(call: AdminCall) -> Option<crate::types::VolumeId> {
    match call {
        AdminCall::CreateVolume { volume, .. }
        | AdminCall::KeepBase { volume, .. }
        | AdminCall::Checkpoint { volume, .. }
        | AdminCall::RestoreVolume { volume }
        | AdminCall::MigrateOut { volume, .. } => Some(volume),
        AdminCall::DeleteBase { .. } => None,
    }
}

impl<W: HostWorld> HostCtx<W> {
    async fn fault_source(self) {
        let mut faults = TaskSet::new();
        loop {
            while faults.try_next_done().is_ok() {}
            if faults.len() == FAULT_CONCURRENCY {
                if faults.next_done().await.is_none() {
                    return;
                }
                continue;
            }
            let Some(fault) = GuestMem::next_fault(self.world().as_ref()).await else {
                return;
            };
            let ctx = self.clone();
            faults.spawn(async move {
                if ctx.state().borrow().volume_authorized(fault.page.volume) {
                    serve_fault(Rc::clone(ctx.state()), Rc::clone(ctx.world()), fault).await;
                } else {
                    let _ = GuestMem::fail(ctx.world().as_ref(), fault.page).await;
                }
            });
        }
    }
}

#[cfg(test)]
async fn fault_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    HostCtx::new(state, world).fault_source().await;
}

enum SyncSourceEvent<T> {
    Resolved(Option<()>),
    Cancelled(Option<(crate::types::VolumeId, u64)>),
    Completed(Option<blockd_exec::TaskId>),
    Ingress(Option<T>),
}

impl<W: HostWorld> HostCtx<W> {
    async fn sync_source(self) {
        let mut syncs = TaskSet::new();
        let mut child_volumes = std::collections::BTreeMap::new();
        let (resolved, mut resolutions) = unbounded();
        let (cancelled, cancellations) = injector();
        let mut requests = super::keyed_queue::KeyedQueue::new();
        let mut ingress_open = true;
        let mut admitted = 0usize;
        let mut next_admission = 0u64;
        let mut ingress_batch = 0;
        loop {
            while let Some((volume, (id, request))) = requests.start_next(SYNC_CONCURRENCY) {
                let ctx = self.clone();
                let resolved = resolved.clone();
                let child = syncs.spawn(async move {
                    ctx.handle_sync(id, request, resolved).await;
                });
                child_volumes.insert(child, volume);
            }
            if !ingress_open && requests.is_idle() {
                return;
            }

            let event = if !ingress_open || admitted >= SYNC_QUEUE_CAPACITY {
                match select3(resolutions.recv(), cancellations.recv(), syncs.next_done()).await {
                    OneOf3::First(resolved) => SyncSourceEvent::Resolved(resolved),
                    OneOf3::Second(cancelled) => SyncSourceEvent::Cancelled(cancelled),
                    OneOf3::Third(completed) => SyncSourceEvent::Completed(completed),
                }
            } else {
                match select3(
                    resolutions.recv(),
                    cancellations.recv(),
                    select2(
                        syncs.next_done(),
                        GuestMem::next_sync(self.world().as_ref()),
                    ),
                )
                .await
                {
                    OneOf3::First(resolved) => SyncSourceEvent::Resolved(resolved),
                    OneOf3::Second(cancelled) => SyncSourceEvent::Cancelled(cancelled),
                    OneOf3::Third(Either::First(completed)) => {
                        SyncSourceEvent::Completed(completed)
                    }
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
                SyncSourceEvent::Cancelled(Some((volume, id))) => {
                    if requests
                        .remove_where(volume, |(queued_id, _)| *queued_id == id)
                        .is_some()
                    {
                        let _ = resolved.send(());
                    } else {
                        cancel_pending_sync(self.state(), volume, id);
                    }
                }
                SyncSourceEvent::Completed(Some(child)) => {
                    let volume = child_volumes.remove(&child).expect("sync child tracked");
                    requests.complete(volume);
                }
                SyncSourceEvent::Ingress(Some(mut request)) => {
                    let id = next_admission;
                    next_admission = next_admission
                        .checked_add(1)
                        .expect("sync admission id overflow");
                    let volume = request.body.volume;
                    let cancellation = cancelled.clone();
                    request.on_cancel(move || {
                        let _ = cancellation.push(Lane::Critical, (volume, id));
                    });
                    admitted += 1;
                    requests.push(volume, (id, request));
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

    async fn handle_sync(
        self,
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
            let mut host = self.state().borrow_mut();
            if !host.volume_authorized(sync.volume) {
                host.counters.guest_rejected += 1;
                None
            } else if let Some(volume) = host.volumes.get_mut(&sync.volume) {
                if !volume.ready || volume.config.kind != VolumeKind::Data {
                    host.counters.guest_rejected += 1;
                    None
                } else {
                    let barrier = volume.mutation_seq;
                    if volume.sync_ack_through >= barrier {
                        host.counters.syncs_acked += 1;
                        Some(false)
                    } else {
                        volume.pending_syncs.push(PendingSync::new(
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
                let _ = self.volume(sync.volume).capture().await;
            }
        }
    }
}

#[cfg(test)]
async fn sync_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    HostCtx::new(state, world).sync_source().await;
}

fn cancel_pending_sync(state: &SharedHost, volume: crate::types::VolumeId, id: u64) {
    let mut host = state.borrow_mut();
    let Some(pending) = host.volumes.get_mut(&volume).and_then(|volume_state| {
        let index = volume_state
            .pending_syncs
            .iter()
            .position(|sync| sync.id() == id)?;
        Some(volume_state.pending_syncs.remove(index))
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
#[path = "host/tests.rs"]
mod tests;
