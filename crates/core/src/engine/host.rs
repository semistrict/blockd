use std::rc::Rc;

use blockd_exec::{TaskSet, delay};

use super::{
    SharedHost, attach_database, begin_detach_database, capture_local, checkpoint_local,
    create_backed, create_fork, create_fresh_local, create_peer_stashed, database_source,
    delete_base, finish_detach_database, hydrate_tail, keep_base, migrate_out, peer_source,
    publish_latest, publish_replica_head, reclaim_backed_segments, reconcile_backed_recovery,
    recover_local, reoffer_outbound, replicate_latest, restore_vset, retry_replica_releases,
    serve_fault,
};
use crate::hostmeta::HostConfig;
use crate::journal::VsetKind;
use crate::protocol::{AdminCmd, AdminReply};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store};

pub trait HostWorld: Blobs + Store + Peers + GuestMem + AdminIo + 'static {}

impl<T> HostWorld for T where T: Blobs + Store + Peers + GuestMem + AdminIo + 'static {}

pub async fn host_actor<W: HostWorld>(config: HostConfig, world: Rc<W>) {
    let state = Rc::new(std::cell::RefCell::new(super::HostState::new(config)));
    host_actor_with_state(state, world).await;
}

pub async fn host_actor_with_state<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    let config = state.borrow().config.clone();
    let Ok(verdicts) = recover_local(Rc::clone(&state), world.as_ref()).await else {
        AdminIo::abort(world.as_ref(), "local recovery scan failed").await;
        return;
    };
    for (vset, verdict) in verdicts {
        AdminIo::reply_admin(world.as_ref(), AdminReply::VsetRecovered { vset, verdict }).await;
    }
    let backed = state
        .borrow()
        .vsets
        .iter()
        .filter_map(|(&vset, state)| state.pending_verdict.is_some().then_some(vset))
        .collect::<Vec<_>>();
    for vset in backed {
        reconcile_backed_recovery(Rc::clone(&state), Rc::clone(&world), vset).await;
    }
    let mut children = TaskSet::new();
    children.spawn(admin_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(fault_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(database_source(Rc::clone(&state), Rc::clone(&world)));
    children.spawn(super::store_gc::store_gc_actor(
        Rc::clone(&state),
        Rc::clone(&world),
    ));
    let outbound = state
        .borrow()
        .vsets
        .iter()
        .filter_map(|(&vset, state)| state.outbound.is_some().then_some(vset))
        .collect::<Vec<_>>();
    for vset in outbound {
        children.spawn(reoffer_outbound(Rc::clone(&state), Rc::clone(&world), vset));
    }

    loop {
        delay(config.writeback_interval).await;
        children.reap();
        if reclaim_backed_segments(Rc::clone(&state), world.as_ref())
            .await
            .is_err()
        {
            AdminIo::abort(world.as_ref(), "backed segment reclaim failed").await;
            return;
        }
        let vsets = state.borrow().vsets.keys().copied().collect::<Vec<_>>();
        for vset in vsets {
            let _ = capture_local(Rc::clone(&state), Rc::clone(&world), vset).await;
            hydrate_tail(Rc::clone(&state), Rc::clone(&world), vset).await;
            children.spawn(publish_latest(Rc::clone(&state), Rc::clone(&world), vset));
            children.spawn(replicate_latest(Rc::clone(&state), Rc::clone(&world), vset));
            children.spawn(publish_replica_head(
                Rc::clone(&state),
                Rc::clone(&world),
                vset,
            ));
            children.spawn(retry_replica_releases(
                Rc::clone(&state),
                Rc::clone(&world),
                vset,
            ));
        }
        let accessed = GuestMem::harvest_accessed(world.as_ref()).await;
        let mut host = state.borrow_mut();
        host.cache.age(|| accessed);
        host.wedge_tick();
    }
}

async fn admin_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    while let Some(command) = AdminIo::next_admin(world.as_ref()).await {
        match command {
            AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base: Some(base),
            } => {
                create_fork(
                    Rc::clone(&state),
                    Rc::clone(&world),
                    req,
                    vset,
                    config,
                    base,
                )
                .await;
            }
            AdminCmd::KeepBase { req, vset, base } => {
                keep_base(Rc::clone(&state), Rc::clone(&world), req, vset, base).await;
            }
            AdminCmd::DeleteBase { req, base } => {
                delete_base(Rc::clone(&state), Rc::clone(&world), req, base).await;
            }
            AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base: None,
            } if !config.durability.uses_store() => {
                create_fresh_local(Rc::clone(&state), Rc::clone(&world), req, vset, config).await;
            }
            AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base: None,
            } if config.durability == crate::journal::DurabilityMode::Backup => {
                create_backed(Rc::clone(&state), Rc::clone(&world), req, vset, config).await;
            }
            AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base: None,
            } if config.durability == crate::journal::DurabilityMode::PeerStashed => {
                create_peer_stashed(Rc::clone(&state), Rc::clone(&world), req, vset, config).await;
            }
            AdminCmd::Checkpoint { req, vset } => {
                checkpoint_local(Rc::clone(&state), Rc::clone(&world), req, vset).await;
            }
            AdminCmd::RestoreVset { req, vset } => {
                restore_vset(Rc::clone(&state), Rc::clone(&world), req, vset).await;
            }
            AdminCmd::MigrateOut { req, vset, to } => {
                migrate_out(Rc::clone(&state), Rc::clone(&world), req, vset, to).await;
            }
            AdminCmd::AttachDatabase { req, vset, vm } => {
                attach_database(Rc::clone(&state), world.as_ref(), req, vset, vm).await;
            }
            AdminCmd::BeginDetachDatabase {
                req,
                vset,
                attachment,
                mode,
            } => {
                begin_detach_database(
                    Rc::clone(&state),
                    Rc::clone(&world),
                    req,
                    vset,
                    attachment,
                    mode,
                )
                .await;
            }
            AdminCmd::FinishDetachDatabase {
                req,
                vset,
                attachment,
            } => {
                finish_detach_database(Rc::clone(&state), world.as_ref(), req, vset, attachment)
                    .await;
            }
            AdminCmd::CreateVset { req, .. } => {
                AdminIo::reply_admin(world.as_ref(), AdminReply::AdminFailed { req }).await;
            }
        }
    }
}

async fn fault_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    let mut faults = TaskSet::new();
    while let Some(fault) = GuestMem::next_fault(world.as_ref()).await {
        faults.reap();
        faults.spawn(serve_fault(
            Rc::clone(&state),
            Rc::clone(&world),
            fault.page,
            fault.write,
        ));
    }
}

async fn sync_source<W: HostWorld>(state: SharedHost, world: Rc<W>) {
    while let Some(sync) = GuestMem::next_sync(world.as_ref()).await {
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
                        vset.pending_syncs.push((sync.req, barrier));
                        Some(true)
                    }
                }
            } else {
                host.counters.guest_rejected += 1;
                None
            }
        };
        match action {
            None => GuestMem::sync_failed(world.as_ref(), sync.req).await,
            Some(false) => GuestMem::sync_ok(world.as_ref(), sync.req).await,
            Some(true) => {
                let _ = capture_local(Rc::clone(&state), Rc::clone(&world), sync.volume.vset).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::{BTreeMap, VecDeque};
    use std::rc::Rc;

    use async_trait::async_trait;
    use blockd_exec::{Executor, delay};

    use super::{host_actor, host_actor_with_state};
    use crate::database::{DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest};
    use crate::engine::HostState;
    use crate::head::HeadRecord;
    use crate::hostmeta::HostConfig as DaemonConfig;
    use crate::journal::{JournalRecord, VsetConfig};
    use crate::layout;
    use crate::protocol::{AdminCmd, AdminReply, DetachMode, PeerMsg, ReqId, StoreFault};
    use crate::segment::open_entry;
    use crate::types::{HostId, PageId, PageNo, VmId, VolumeId, VolumeIdx, VsetId, page_size};
    use crate::world::{
        AdminIo, BlobEntry, BlobError, Blobs, GuestFault, GuestMem, GuestSync, Peers, Store,
        StoreError,
    };

    type ModelStore = Rc<RefCell<BTreeMap<String, (u64, Vec<u8>)>>>;

    #[derive(Default)]
    struct ModelWorld {
        admin: RefCell<VecDeque<AdminCmd>>,
        faults: RefCell<VecDeque<GuestFault>>,
        syncs: RefCell<VecDeque<GuestSync>>,
        replies: RefCell<Vec<AdminReply>>,
        sync_ok: RefCell<Vec<ReqId>>,
        blobs: RefCell<BTreeMap<String, Vec<u8>>>,
        store: ModelStore,
        next_store_version: Rc<Cell<u64>>,
        memory: RefCell<BTreeMap<PageId, Vec<u8>>>,
        shared_pages: RefCell<BTreeMap<crate::cache::BaseKey, Vec<u8>>>,
        peer_inbox: RefCell<VecDeque<(HostId, PeerMsg)>>,
        peer_outbox: RefCell<Vec<(HostId, PeerMsg)>>,
        database_requests: RefCell<VecDeque<DatabaseRequest>>,
        database_replies: RefCell<Vec<DatabaseReply>>,
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

    #[async_trait(?Send)]
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
            delay(1).await;
            assert!(self.blobs.borrow_mut().insert(name, bytes).is_none());
            Ok(())
        }

        async fn append(&self, _name: String, _bytes: Vec<u8>) -> Result<(), BlobError> {
            self.blobs
                .borrow_mut()
                .entry(_name)
                .or_default()
                .extend_from_slice(&_bytes);
            Ok(())
        }

        async fn truncate(&self, _name: &str, _len: u64) -> Result<(), BlobError> {
            if let Some(bytes) = self.blobs.borrow_mut().get_mut(_name) {
                bytes.truncate(usize::try_from(_len).expect("length fits"));
            }
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

    #[async_trait(?Send)]
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
            Ok(self.store.borrow().get(key).cloned())
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

    #[async_trait(?Send)]
    impl Peers for ModelWorld {
        async fn send(&self, to: HostId, message: PeerMsg) {
            self.peer_outbox.borrow_mut().push((to, message));
        }

        async fn recv(&self) -> Option<(HostId, PeerMsg)> {
            Some(next(&self.peer_inbox).await)
        }
    }

    #[async_trait(?Send)]
    impl GuestMem for ModelWorld {
        async fn read_page(&self, page: PageId) -> Vec<u8> {
            self.memory
                .borrow()
                .get(&page)
                .cloned()
                .unwrap_or_else(|| vec![0; page_size()])
        }

        async fn arm_write_protect(&self, _pages: &[PageId]) {}

        async fn fill(
            &self,
            page: PageId,
            bytes: Vec<u8>,
            _writable: bool,
            _source: crate::world::FillSource,
        ) {
            self.memory.borrow_mut().insert(page, bytes);
        }

        async fn fill_shared(
            &self,
            page: PageId,
            share: (u64, u64, crate::types::SegId, u32),
            bytes: Option<Vec<u8>>,
            _writable: bool,
        ) {
            if let Some(bytes) = bytes {
                self.shared_pages.borrow_mut().insert(share, bytes);
            }
            let bytes = self.shared_pages.borrow()[&share].clone();
            self.memory.borrow_mut().insert(page, bytes);
        }

        async fn fail(&self, page: PageId) {
            panic!("unexpected failed fault: {page:?}")
        }

        async fn unprotect(&self, _page: PageId) {}

        async fn evict(&self, page: PageId) {
            self.memory.borrow_mut().remove(&page);
        }

        async fn install_database(&self, _page: PageId, _bytes: Vec<u8>) {
            unreachable!()
        }

        async fn pause(&self, _vset: VsetId) -> u64 {
            77
        }

        async fn resume(&self, _vset: VsetId) {}

        async fn harvest_accessed(&self) -> Vec<PageId> {
            Vec::new()
        }

        async fn next_fault(&self) -> Option<GuestFault> {
            Some(next(&self.faults).await)
        }

        async fn next_sync(&self) -> Option<GuestSync> {
            Some(next(&self.syncs).await)
        }

        async fn sync_ok(&self, req: ReqId) {
            self.sync_ok.borrow_mut().push(req);
        }

        async fn sync_failed(&self, req: ReqId) {
            panic!("unexpected failed sync: {req:?}")
        }

        async fn fence(&self, _vset: VsetId) {
            unreachable!()
        }
    }

    #[async_trait(?Send)]
    impl AdminIo for ModelWorld {
        async fn next_admin(&self) -> Option<AdminCmd> {
            Some(next(&self.admin).await)
        }

        async fn reply_admin(&self, reply: AdminReply) {
            self.replies.borrow_mut().push(reply);
        }

        async fn next_database(&self) -> Option<DatabaseRequest> {
            Some(next(&self.database_requests).await)
        }

        async fn reply_database(&self, reply: DatabaseReply) {
            self.database_replies.borrow_mut().push(reply);
        }

        async fn abort(&self, reason: &'static str) {
            panic!("actor aborted: {reason}")
        }
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
        world.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(1),
            vset,
            config: VsetConfig::compute(1, 8, false),
            from_base: None,
        });
        let config = DaemonConfig {
            host: HostId(0),
            cache_pages: 4,
            writeback_interval: 20,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        };
        let mut executor = Executor::simulation(4);
        let actor_world = Rc::clone(&world);
        let actor = executor.spawn(host_actor(config.clone(), actor_world));
        executor.run_until(5);
        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetCreated {
                req: ReqId(1),
                vset
            }]
        );

        world
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: true });
        executor.run_until(8);
        let expected = vec![0x5a; page_size()];
        world.memory.borrow_mut().insert(page, expected.clone());
        world.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(2),
            volume: page.volume,
        });
        executor.run_until(16);
        assert_eq!(*world.sync_ok.borrow(), [ReqId(2)]);

        world.admin.borrow_mut().push_back(AdminCmd::Checkpoint {
            req: ReqId(3),
            vset,
        });
        executor.run_until(20);
        assert_eq!(
            *world.replies.borrow(),
            [
                AdminReply::VsetCreated {
                    req: ReqId(1),
                    vset
                },
                AdminReply::CheckpointDone {
                    req: ReqId(3),
                    vset,
                    epoch: crate::types::Epoch(1)
                }
            ]
        );

        let blobs = world.blobs.borrow();
        let record_bytes = &blobs[&layout::journal_blob(vset, 1, crate::types::JournalSeq(2))];
        assert_eq!(
            record_bytes,
            &blobs[&layout::journal_mirror_blob(vset, 1, crate::types::JournalSeq(2))]
        );
        let record = JournalRecord::decode(vset, record_bytes).expect("valid record");
        assert_eq!(record.capture_seq, 1);
        assert_eq!(record.sync_covered_through, 1);
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
        let recovered_world = Rc::clone(&world);
        let recovered = executor.spawn(host_actor(config, recovered_world));
        executor.run_until(22);
        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetRecovered {
                vset,
                verdict: crate::protocol::Verdict::Resume {
                    epoch: crate::types::Epoch(1),
                    vmstate: 77
                }
            }]
        );
        world
            .faults
            .borrow_mut()
            .push_back(GuestFault { page, write: false });
        executor.run_until(24);
        assert_eq!(world.memory.borrow().get(&page), Some(&expected));

        drop(recovered);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn database_actor_persists_byte_io_sync_truncate_and_delete() {
        let vset = VsetId(70);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(100),
            vset,
            config: VsetConfig::database(8, false),
            from_base: None,
        });
        let config = DaemonConfig {
            host: HostId(0),
            cache_pages: 4,
            writeback_interval: 100_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        };
        let mut executor = Executor::simulation(70);
        let actor = executor.spawn(host_actor(config.clone(), Rc::clone(&world)));
        executor.run_until(5);
        world
            .admin
            .borrow_mut()
            .push_back(AdminCmd::AttachDatabase {
                req: ReqId(101),
                vset,
                vm: VmId(9),
            });
        executor.run_until(8);
        let attachment = world
            .replies
            .borrow()
            .iter()
            .find_map(|reply| match reply {
                AdminReply::DatabaseAttached { attachment, .. } => Some(*attachment),
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
            .push_back(AdminCmd::BeginDetachDatabase {
                req: ReqId(110),
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
                .contains(&AdminReply::DatabaseDetachStarted {
                    req: ReqId(110),
                    vset,
                    attachment,
                    forced: false,
                })
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
            .push_back(AdminCmd::FinishDetachDatabase {
                req: ReqId(112),
                vset,
                attachment,
            });
        executor.run_until(104);
        assert!(
            world
                .replies
                .borrow()
                .contains(&AdminReply::DatabaseDetached {
                    req: ReqId(112),
                    vset,
                    attachment,
                })
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
        world.database_replies.borrow_mut().clear();
        let recovered = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(110);
        assert!(world.replies.borrow().contains(&AdminReply::VsetRecovered {
            vset,
            verdict: crate::protocol::Verdict::DatabaseReady { synced_through: 4 },
        }));

        drop(recovered);
        executor.run_ready();
    }

    #[test]
    fn backed_creation_claims_and_publishes_a_fenced_head() {
        let vset = VsetId(11);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(9),
            vset,
            config: VsetConfig::compute(1, 8, true),
            from_base: None,
        });
        let config = DaemonConfig {
            host: HostId(3),
            cache_pages: 4,
            writeback_interval: 5,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        };
        let mut executor = Executor::simulation(5);
        let actor = executor.spawn(host_actor(config.clone(), Rc::clone(&world)));
        executor.run_until(8);

        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetCreated {
                req: ReqId(9),
                vset
            }]
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
        let recovered = executor.spawn(host_actor(config.clone(), Rc::clone(&world)));
        executor.run_until(10);
        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetRecovered {
                vset,
                verdict: crate::protocol::Verdict::ColdBoot
            }]
        );

        drop(recovered);
        executor.run_ready();

        world.blobs.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world.admin.borrow_mut().push_back(AdminCmd::RestoreVset {
            req: ReqId(10),
            vset,
        });
        let restore_config = DaemonConfig {
            host: HostId(4),
            ..config
        };
        let restored = executor.spawn(host_actor(restore_config, Rc::clone(&world)));
        executor.run_until(12);
        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetRestored {
                req: ReqId(10),
                vset,
                verdict: crate::protocol::Verdict::ColdBoot
            }]
        );
        let store = world.store.borrow();
        let (_, head_bytes) = &store[&layout::head_key(vset)];
        let head = HeadRecord::decode(vset, head_bytes).expect("valid claimed head");
        assert_eq!(head.holder, HostId(4));
        assert_eq!(head.manifest, Some(manifest));

        drop(store);
        drop(restored);
        executor.run_ready();
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn restore_hydrates_only_the_faulted_map_leaf() {
        let vset = VsetId(12);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(20),
            vset,
            config: VsetConfig::compute(1, 300, true),
            from_base: None,
        });
        let config = DaemonConfig {
            host: HostId(5),
            cache_pages: 300,
            writeback_interval: 10,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
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
        executor.run_until(28);
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

        world.blobs.borrow_mut().clear();
        world.memory.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world.admin.borrow_mut().push_back(AdminCmd::RestoreVset {
            req: ReqId(22),
            vset,
        });
        let restore_config = DaemonConfig {
            host: HostId(6),
            cache_pages: 16,
            writeback_interval: 10,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        };
        let restored = executor.spawn(host_actor(restore_config, Rc::clone(&world)));
        executor.run_until(31);
        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetRestored {
                req: ReqId(22),
                vset,
                verdict: crate::protocol::Verdict::ColdBoot
            }]
        );
        assert!(!world.blobs.borrow().contains_key(&local_leaf));

        let faulted = pages[42];
        world.faults.borrow_mut().push_back(GuestFault {
            page: faulted,
            write: false,
        });
        executor.run_until(35);
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
        world.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(30),
            vset: source,
            config: VsetConfig::compute(1, 8, true),
            from_base: None,
        });
        let config = DaemonConfig {
            host: HostId(7),
            cache_pages: 8,
            writeback_interval: 20,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
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
        world.admin.borrow_mut().push_back(AdminCmd::Checkpoint {
            req: ReqId(31),
            vset: source,
        });
        executor.run_until(13);
        world.admin.borrow_mut().push_back(AdminCmd::KeepBase {
            req: ReqId(32),
            vset: source,
            base,
        });
        executor.run_until(18);
        assert!(
            world
                .store
                .borrow()
                .contains_key(&layout::base_record_key(base))
        );

        world.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(33),
            vset: fork,
            config: VsetConfig::compute(1, 8, false),
            from_base: Some(base),
        });
        executor.run_until(23);
        assert!(world.replies.borrow().contains(&AdminReply::VsetForked {
            req: ReqId(33),
            vset: fork,
            verdict: crate::protocol::Verdict::Resume {
                epoch: crate::types::Epoch(0),
                vmstate: 77,
            },
        }));
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

        world.admin.borrow_mut().push_back(AdminCmd::DeleteBase {
            req: ReqId(34),
            base,
        });
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
        let destination = Rc::new(ModelWorld::default());
        source.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(40),
            vset,
            config: VsetConfig::compute(1, 8, false),
            from_base: None,
        });
        let config = |host| DaemonConfig {
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
            replica_placement: None,
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
        source.admin.borrow_mut().push_back(AdminCmd::MigrateOut {
            req: ReqId(41),
            vset,
            to: destination_host,
        });
        executor.run_until(15);
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(20);
        assert!(
            destination
                .replies
                .borrow()
                .contains(&AdminReply::VsetMigratedIn {
                    vset,
                    verdict: crate::protocol::Verdict::Resume {
                        epoch: crate::types::Epoch(1),
                        vmstate: 77,
                    },
                })
        );
        deliver(destination_host, &destination, &source, source_host);
        executor.run_until(25);
        assert!(source.replies.borrow().contains(&AdminReply::MigratedOut {
            req: ReqId(41),
            vset,
        }));

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

        drop(source_actor);
        drop(destination_actor);
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
        source.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(200),
            vset,
            config: VsetConfig::database(8, true),
            from_base: None,
        });
        let config = |host| DaemonConfig {
            host,
            cache_pages: 4,
            writeback_interval: 100_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
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
            .push_back(AdminCmd::AttachDatabase {
                req: ReqId(201),
                vset,
                vm: VmId(10),
            });
        executor.run_until(9);
        let attachment = source
            .replies
            .borrow()
            .iter()
            .find_map(|reply| match reply {
                AdminReply::DatabaseAttached { attachment, .. } => Some(*attachment),
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
        executor.run_until(40);
        source
            .admin
            .borrow_mut()
            .push_back(AdminCmd::BeginDetachDatabase {
                req: ReqId(206),
                vset,
                attachment,
                mode: DetachMode::Graceful,
            });
        executor.run_until(44);
        source
            .admin
            .borrow_mut()
            .push_back(AdminCmd::FinishDetachDatabase {
                req: ReqId(207),
                vset,
                attachment,
            });
        executor.run_until(48);
        source.admin.borrow_mut().push_back(AdminCmd::MigrateOut {
            req: ReqId(208),
            vset,
            to: destination_host,
        });
        executor.run_until(70);
        deliver(source_host, &source, &destination, destination_host);
        executor.run_until(90);
        assert!(
            destination
                .replies
                .borrow()
                .contains(&AdminReply::VsetMigratedIn {
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
        assert!(source.replies.borrow().contains(&AdminReply::MigratedOut {
            req: ReqId(208),
            vset,
        }));

        destination
            .admin
            .borrow_mut()
            .push_back(AdminCmd::AttachDatabase {
                req: ReqId(209),
                vset,
                vm: VmId(11),
            });
        executor.run_until(97);
        let migrated_attachment = destination
            .replies
            .borrow()
            .iter()
            .find_map(|reply| match reply {
                AdminReply::DatabaseAttached {
                    req: ReqId(209),
                    attachment,
                    ..
                } => Some(*attachment),
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
        world.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(50),
            vset,
            config: VsetConfig::compute(1, 4, false),
            from_base: None,
        });
        let config = DaemonConfig {
            host: HostId(10),
            cache_pages: 4,
            writeback_interval: 100_000_000,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
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
        world.admin.borrow_mut().push_back(AdminCmd::MigrateOut {
            req: ReqId(51),
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

        drop(actor);
        executor.run_ready();
        world.peer_outbox.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        let recovered = executor.spawn(host_actor(config, Rc::clone(&world)));
        executor.run_until(18);
        assert!(world.replies.borrow().is_empty());
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
        use crate::journal::{DatabaseMeta, DurabilityMode, RecordKind, VsetKind};
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
                durability: DurabilityMode::PeerStashed,
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
        use crate::journal::{DurabilityMode, VsetKind};
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
        primary.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(60),
            vset,
            config: VsetConfig {
                kind: VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 8,
                durability: DurabilityMode::PeerStashed,
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
        use crate::journal::{DurabilityMode, VsetKind};
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
        primary.admin.borrow_mut().push_back(AdminCmd::CreateVset {
            req: ReqId(300),
            vset,
            config: VsetConfig {
                kind: VsetKind::Compute,
                disk_volumes: 1,
                pages_per_volume: 8,
                durability: DurabilityMode::PeerStashed,
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
