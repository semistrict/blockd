//! Actor host for the protocol core.
//!
//! This is the compatibility bridge used while individual protocol modules
//! move from reified continuations to straight-line actors. All scheduling,
//! timers, and I/O completion ordering already flow through the shared
//! executor and async world contracts.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;

use blockd_exec::channel::{Receiver, TryRecvError, UnboundedSender, unbounded};
use blockd_exec::{TaskHandle, TaskId, delay, spawn};

use crate::daemon::{Daemon, DaemonConfig};
use crate::layout;
use crate::protocol::PeerMsg;
use crate::replica_spool::seal_verified_replica_artifact;
use crate::seam::{Effect, Event, HostMap, TimerId};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store, StoreError};

pub trait ActorWorld: Blobs + Store + Peers + GuestMem + AdminIo + HostMap + 'static {}

impl<T> ActorWorld for T where T: Blobs + Store + Peers + GuestMem + AdminIo + HostMap + 'static {}

struct ActorGroup {
    actors: BTreeMap<TaskId, TaskHandle<()>>,
    completed_tx: UnboundedSender<TaskId>,
    completed_rx: Receiver<TaskId>,
}

impl ActorGroup {
    fn new() -> Self {
        let (completed_tx, completed_rx) = unbounded();
        Self {
            actors: BTreeMap::new(),
            completed_tx,
            completed_rx,
        }
    }

    fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
        let completed = self.completed_tx.clone();
        let task_id = Rc::new(Cell::new(u64::MAX));
        let child_id = Rc::clone(&task_id);
        let handle = spawn(async move {
            future.await;
            let _ = completed.send(child_id.get());
        });
        task_id.set(handle.id());
        self.actors.insert(handle.id(), handle);
    }

    fn reap(&mut self) {
        loop {
            match self.completed_rx.try_recv() {
                Ok(task) => {
                    if let Some(handle) = self.actors.remove(&task) {
                        handle.detach();
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Closed) => return,
            }
        }
    }
}

pub async fn host_actor<W: ActorWorld>(config: DaemonConfig, world: Rc<W>) {
    let (events, mut inbox) = unbounded();
    let mut actors = ActorGroup::new();
    let writeback_interval = config.writeback_interval;

    actors.spawn(admin_source(Rc::clone(&world), events.clone()));
    actors.spawn(database_source(Rc::clone(&world), events.clone()));
    actors.spawn(peer_source(Rc::clone(&world), events.clone()));
    actors.spawn(fault_source(Rc::clone(&world), events.clone()));
    actors.spawn(sync_source(Rc::clone(&world), events.clone()));
    actors.spawn(writeback_ticker(writeback_interval, events.clone()));

    let (mut daemon, effects) = Daemon::new(config);
    spawn_effects(&mut actors, &world, &events, effects);

    while let Some(event) = inbox.recv().await {
        actors.reap();
        let effects = daemon.step(event, world.as_ref());
        spawn_effects(&mut actors, &world, &events, effects);
    }
}

fn spawn_effects<W: ActorWorld>(
    actors: &mut ActorGroup,
    world: &Rc<W>,
    events: &UnboundedSender<Event>,
    effects: Vec<Effect>,
) {
    for effect in effects {
        if matches!(
            effect,
            Effect::SetTimer {
                timer: TimerId::Writeback,
                ..
            }
        ) {
            continue;
        }
        actors.spawn(apply_effect(Rc::clone(world), events.clone(), effect));
    }
}

async fn writeback_ticker(interval: u64, events: UnboundedSender<Event>) {
    loop {
        delay(interval).await;
        if events.send(Event::Timer(TimerId::Writeback)).is_err() {
            return;
        }
    }
}

async fn admin_source<W: ActorWorld>(world: Rc<W>, events: UnboundedSender<Event>) {
    while let Some(command) = AdminIo::next_admin(world.as_ref()).await {
        if events.send(Event::Admin(command)).is_err() {
            return;
        }
    }
}

async fn database_source<W: ActorWorld>(world: Rc<W>, events: UnboundedSender<Event>) {
    while let Some(request) = AdminIo::next_database(world.as_ref()).await {
        if events.send(Event::Database(request)).is_err() {
            return;
        }
    }
}

async fn peer_source<W: ActorWorld>(world: Rc<W>, events: UnboundedSender<Event>) {
    while let Some((from, message)) = Peers::recv(world.as_ref()).await {
        let event = match message {
            PeerMsg::ReplicaPut {
                vset,
                assignment_epoch,
                artifact,
                checksum,
                bytes,
            } => {
                let frame = seal_verified_replica_artifact(
                    from,
                    vset,
                    assignment_epoch,
                    artifact,
                    checksum,
                    &bytes,
                )
                .ok();
                Event::ReplicaPutPrepared {
                    from,
                    vset,
                    assignment_epoch,
                    artifact,
                    checksum,
                    bytes,
                    frame,
                }
            }
            message => Event::PeerDelivered { from, msg: message },
        };
        if events.send(event).is_err() {
            return;
        }
    }
}

async fn fault_source<W: ActorWorld>(world: Rc<W>, events: UnboundedSender<Event>) {
    while let Some(fault) = GuestMem::next_fault(world.as_ref()).await {
        if events
            .send(Event::GuestFault {
                page: fault.page,
                write: fault.write,
            })
            .is_err()
        {
            return;
        }
    }
}

async fn sync_source<W: ActorWorld>(world: Rc<W>, events: UnboundedSender<Event>) {
    while let Some(sync) = GuestMem::next_sync(world.as_ref()).await {
        if events
            .send(Event::GuestSync {
                req: sync.req,
                volume: sync.volume,
            })
            .is_err()
        {
            return;
        }
    }
}

async fn blob_done<W: ActorWorld>(
    world: &W,
    events: &UnboundedSender<Event>,
    io: crate::protocol::IoId,
    result: Result<(), crate::world::BlobError>,
) {
    if result.is_ok() {
        let _ = events.send(Event::BlobWriteDone { io });
    } else {
        AdminIo::abort(world, "local blob I/O failed").await;
    }
}

async fn store_put_done<W: ActorWorld>(
    world: &W,
    events: &UnboundedSender<Event>,
    io: crate::protocol::IoId,
    result: Result<u64, StoreError>,
) {
    let result = match result {
        Ok(version) => Ok(version),
        Err(StoreError::Fault(fault)) => Err(fault),
        Err(StoreError::TooLarge) => {
            AdminIo::abort(world, "object exceeds store contract").await;
            return;
        }
    };
    let _ = events.send(Event::StorePutDone { io, result });
}

async fn store_get_done<W: ActorWorld>(
    world: &W,
    events: &UnboundedSender<Event>,
    io: crate::protocol::IoId,
    result: Result<Option<(u64, Vec<u8>)>, StoreError>,
) {
    let result = match result {
        Ok(value) => Ok(value),
        Err(StoreError::Fault(fault)) => Err(fault),
        Err(StoreError::TooLarge) => {
            AdminIo::abort(world, "invalid store read result").await;
            return;
        }
    };
    let _ = events.send(Event::StoreGetDone { io, result });
}

#[allow(clippy::too_many_lines)]
async fn apply_effect<W: ActorWorld>(world: Rc<W>, events: UnboundedSender<Event>, effect: Effect) {
    match effect {
        Effect::Fill {
            page,
            bytes,
            writable,
            ..
        } => GuestMem::fill(world.as_ref(), page, bytes, writable).await,
        Effect::FillShared {
            page,
            share,
            writable,
        } => GuestMem::fill_shared(world.as_ref(), page, share, writable).await,
        Effect::FillFailed { page } => GuestMem::fail(world.as_ref(), page).await,
        Effect::Unprotect { page } => GuestMem::unprotect(world.as_ref(), page).await,
        Effect::WriteProtect { pages } => {
            GuestMem::arm_write_protect(world.as_ref(), &pages).await;
        }
        Effect::Evict { page } => GuestMem::evict(world.as_ref(), page).await,
        Effect::DatabaseInstall { page, bytes } => {
            GuestMem::install_database(world.as_ref(), page, bytes).await;
        }
        Effect::Database(reply) => AdminIo::reply_database(world.as_ref(), reply).await,
        Effect::PauseGuest { vset } => {
            let vmstate = GuestMem::pause(world.as_ref(), vset).await;
            let _ = events.send(Event::GuestPaused { vset, vmstate });
        }
        Effect::ResumeGuest { vset } => GuestMem::resume(world.as_ref(), vset).await,
        Effect::SyncOk { req } => GuestMem::sync_ok(world.as_ref(), req).await,
        Effect::SyncFailed { req } => GuestMem::sync_failed(world.as_ref(), req).await,
        Effect::BlobWrite { io, name, bytes } => {
            let result = Blobs::write(world.as_ref(), name, bytes).await;
            blob_done(world.as_ref(), &events, io, result).await;
        }
        Effect::ReplicaAppend {
            io,
            source,
            vset,
            assignment_epoch,
            generation,
            bytes,
        } => {
            let name =
                layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation);
            let result = Blobs::append(world.as_ref(), name, bytes).await;
            blob_done(world.as_ref(), &events, io, result).await;
        }
        Effect::ReplicaDelete {
            io,
            source,
            vset,
            assignment_epoch,
            through_generation,
        } => {
            let names = (0..=through_generation)
                .map(|generation| {
                    layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation)
                })
                .collect::<Vec<_>>();
            let event = if Blobs::delete_many_durable(world.as_ref(), &names)
                .await
                .is_ok()
            {
                Event::BlobWriteDone { io }
            } else {
                Event::ReplicaDeleteFailed { io }
            };
            let _ = events.send(event);
        }
        Effect::ReplicaTruncate {
            io,
            source,
            vset,
            assignment_epoch,
            generation,
            len,
        } => {
            let name =
                layout::replica_spool_segment_blob(source, vset, assignment_epoch, generation);
            let result = Blobs::truncate(world.as_ref(), &name, len).await;
            blob_done(world.as_ref(), &events, io, result).await;
        }
        Effect::BlobRead { io, name } => {
            let result = Blobs::read(world.as_ref(), &name).await;
            match result {
                Ok(bytes) => {
                    let _ = events.send(Event::BlobReadDone { io, bytes });
                }
                Err(_) => AdminIo::abort(world.as_ref(), "local blob read failed").await,
            }
        }
        Effect::BlobReadRange {
            io,
            name,
            offset,
            len,
        } => {
            let result = Blobs::read_range(world.as_ref(), &name, offset, len).await;
            match result {
                Ok(bytes) => {
                    let _ = events.send(Event::BlobReadDone { io, bytes });
                }
                Err(_) => AdminIo::abort(world.as_ref(), "local blob range read failed").await,
            }
        }
        Effect::BlobDelete { name } => {
            if Blobs::delete(world.as_ref(), &name).await.is_err() {
                AdminIo::abort(world.as_ref(), "local blob delete failed").await;
            }
        }
        Effect::SetTimer { timer, after } => {
            delay(after).await;
            let _ = events.send(Event::Timer(timer));
        }
        Effect::StorePut { io, key, bytes } => {
            let result = Store::put(world.as_ref(), key, bytes).await;
            store_put_done(world.as_ref(), &events, io, result).await;
        }
        Effect::StoreCas {
            io,
            key,
            expected,
            bytes,
        } => {
            let result = Store::put_cas(world.as_ref(), key, expected, bytes).await;
            store_put_done(world.as_ref(), &events, io, result).await;
        }
        Effect::StoreGet { io, key } => {
            let result = Store::get(world.as_ref(), &key).await;
            store_get_done(world.as_ref(), &events, io, result).await;
        }
        Effect::StoreGetRange {
            io,
            key,
            offset,
            len,
        } => {
            let result = Store::get_range(world.as_ref(), &key, offset, len).await;
            store_get_done(world.as_ref(), &events, io, result).await;
        }
        Effect::StoreDelete { key } => {
            let _ = Store::delete(world.as_ref(), &key).await;
        }
        Effect::VsetFenced { vset } => GuestMem::fence(world.as_ref(), vset).await,
        Effect::PeerSend { to, msg } => Peers::send(world.as_ref(), to, msg).await,
        Effect::Admin(reply) => AdminIo::reply_admin(world.as_ref(), reply).await,
        Effect::Abort { reason } => AdminIo::abort(world.as_ref(), reason).await,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};
    use std::rc::Rc;

    use async_trait::async_trait;
    use blockd_exec::Executor;

    use super::host_actor;
    use crate::daemon::DaemonConfig;
    use crate::database::{DatabaseReply, DatabaseRequest};
    use crate::journal::VsetConfig;
    use crate::seam::{AdminCmd, AdminReply, HostMap, PeerMsg, ReqId};
    use crate::types::{HostId, PageId, VsetId};
    use crate::world::{
        AdminIo, BlobError, Blobs, GuestFault, GuestMem, GuestSync, Peers, Store, StoreError,
    };

    struct ModelWorld {
        admin: RefCell<VecDeque<AdminCmd>>,
        replies: RefCell<Vec<AdminReply>>,
        blobs: RefCell<BTreeMap<String, Vec<u8>>>,
    }

    impl HostMap for ModelWorld {
        fn read_page(&self, _page: PageId) -> Vec<u8> {
            panic!("create must not read guest memory")
        }
    }

    #[async_trait(?Send)]
    impl Blobs for ModelWorld {
        async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            assert!(self.blobs.borrow_mut().insert(name, bytes).is_none());
            Ok(())
        }
        async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            self.blobs
                .borrow_mut()
                .entry(name)
                .or_default()
                .extend(bytes);
            Ok(())
        }
        async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
            self.blobs
                .borrow_mut()
                .get_mut(name)
                .unwrap()
                .truncate(usize::try_from(len).unwrap());
            Ok(())
        }
        async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.blobs.borrow().get(name).cloned())
        }
        async fn read_range(
            &self,
            name: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.blobs.borrow().get(name).map(|bytes| {
                let start = usize::try_from(offset.min(bytes.len() as u64)).unwrap();
                let end = usize::try_from((offset + len).min(bytes.len() as u64)).unwrap();
                bytes[start..end].to_vec()
            }))
        }
        async fn delete(&self, name: &str) -> Result<(), BlobError> {
            self.blobs.borrow_mut().remove(name);
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl Store for ModelWorld {
        async fn put(&self, _key: String, _bytes: Vec<u8>) -> Result<u64, StoreError> {
            unreachable!()
        }
        async fn put_cas(
            &self,
            _key: String,
            _expected: Option<u64>,
            _bytes: Vec<u8>,
        ) -> Result<u64, StoreError> {
            unreachable!()
        }
        async fn get(&self, _key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            unreachable!()
        }
        async fn get_range(
            &self,
            _key: &str,
            _offset: u64,
            _len: u64,
        ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            unreachable!()
        }
        async fn delete(&self, _key: &str) -> Result<bool, StoreError> {
            unreachable!()
        }
        async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>, StoreError> {
            unreachable!()
        }
    }

    #[async_trait(?Send)]
    impl Peers for ModelWorld {
        async fn send(&self, _to: HostId, _message: PeerMsg) {
            unreachable!()
        }
        async fn recv(&self) -> Option<(HostId, PeerMsg)> {
            None
        }
    }

    #[async_trait(?Send)]
    impl GuestMem for ModelWorld {
        async fn read_page(&self, _page: PageId) -> Vec<u8> {
            unreachable!()
        }
        async fn arm_write_protect(&self, _pages: &[PageId]) {
            unreachable!()
        }
        async fn fill(&self, _page: PageId, _bytes: Vec<u8>, _writable: bool) {
            unreachable!()
        }
        async fn fill_shared(
            &self,
            _page: PageId,
            _share: (u64, u64, crate::types::SegId, u32),
            _writable: bool,
        ) {
            unreachable!()
        }
        async fn fail(&self, _page: PageId) {
            unreachable!()
        }
        async fn unprotect(&self, _page: PageId) {
            unreachable!()
        }
        async fn evict(&self, _page: PageId) {
            unreachable!()
        }
        async fn install_database(&self, _page: PageId, _bytes: Vec<u8>) {
            unreachable!()
        }
        async fn pause(&self, _vset: VsetId) -> u64 {
            unreachable!()
        }
        async fn resume(&self, _vset: VsetId) {
            unreachable!()
        }
        async fn harvest_accessed(&self) -> Vec<PageId> {
            Vec::new()
        }
        async fn next_fault(&self) -> Option<GuestFault> {
            None
        }
        async fn next_sync(&self) -> Option<GuestSync> {
            None
        }
        async fn sync_ok(&self, _req: ReqId) {
            unreachable!()
        }
        async fn sync_failed(&self, _req: ReqId) {
            unreachable!()
        }
        async fn fence(&self, _vset: VsetId) {
            unreachable!()
        }
    }

    #[async_trait(?Send)]
    impl AdminIo for ModelWorld {
        async fn next_admin(&self) -> Option<AdminCmd> {
            self.admin.borrow_mut().pop_front()
        }
        async fn reply_admin(&self, reply: AdminReply) {
            self.replies.borrow_mut().push(reply);
        }
        async fn next_database(&self) -> Option<DatabaseRequest> {
            None
        }
        async fn reply_database(&self, _reply: DatabaseReply) {
            unreachable!()
        }
        async fn abort(&self, reason: &'static str) {
            panic!("actor aborted: {reason}")
        }
    }

    #[test]
    fn local_vset_lifecycle_runs_as_actors() {
        let vset = VsetId(7);
        let world = Rc::new(ModelWorld {
            admin: RefCell::new(VecDeque::from([AdminCmd::CreateVset {
                req: ReqId(4),
                vset,
                config: VsetConfig::compute(1, 8, false),
                from_base: None,
            }])),
            replies: RefCell::new(Vec::new()),
            blobs: RefCell::new(BTreeMap::new()),
        });
        let config = DaemonConfig {
            host: HostId(0),
            cache_pages: 16,
            writeback_interval: 100,
            backup_retry: 200,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        };
        let mut executor = Executor::simulation(1);
        let actor = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(10);
        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetCreated {
                req: ReqId(4),
                vset
            }]
        );
        let blobs = world.blobs.borrow();
        assert_eq!(blobs.len(), 2);
        let names = blobs.keys().collect::<Vec<_>>();
        let record = names
            .iter()
            .find(|name| {
                std::path::Path::new(name.as_str())
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("rec"))
            })
            .unwrap();
        let mirror = names
            .iter()
            .find(|name| {
                std::path::Path::new(name.as_str())
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("recm"))
            })
            .unwrap();
        assert_eq!(mirror.strip_suffix('m'), Some(record.as_str()));
        drop(blobs);
        drop(actor);
        executor.run_ready();
    }
}
