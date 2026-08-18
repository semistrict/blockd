use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use blockd_exec::channel::{oneshot, unbounded};
use blockd_exec::{FaultConfig, TaskSet, TryRecvError, delay, request, spawn};

macro_rules! simulate {
    ($seed:expr, $future:expr) => {{
        tokio::task::LocalSet::new()
            .run_until(blockd_exec::simulation_scope(
                $seed,
                FaultConfig::default(),
                $future,
            ))
            .await
    }};
}

use super::{
    ADMIN_CONCURRENCY, FAULT_CONCURRENCY, admin_source, fault_source, handle_admin, host_actor,
    host_actor_with_state, reclaim_backed_blx_files, reconcile_backed_volumes,
    schedule_volume_work, scheduled_work_source, sync_source, work_ready,
};
use crate::blx::{BlockKey, BlxObject};
use crate::engine::migration::{available_inbound_fence, migrate_in};
use crate::engine::peer_source;
use crate::engine::state::{CaptureKind, MutationOwner, PendingSync, ReplicaKey, ReplicaState};
use crate::engine::{HostFatal, HostState, capture_local, checkpoint_local, cleanup_local};
use crate::head::{HeadRecord, StashAssignment};
use crate::hostmeta::{HostConfig as DaemonConfig, ReplicaPlacementConfig};
use crate::journal::{JournalRecord, RecordKind, VolumeConfig};
use crate::layout;
use crate::manifest::{Manifest, RecoveryKind};
use crate::page_file::{PageFileLoc, open_entry};
use crate::protocol::{
    AdminCall, AdminError, AdminEvent, AdminResult, AdminSuccess, PeerMsg, PeerRequestId,
    ReplicaArtifact, ReplicaCommitInfo, ReqId, StoreFault,
};
use crate::types::{Gen, HostId, ObjectId, PageId, PageNo, VolumeId, page_size};
use crate::world::{
    AdminIo, BlobEntry, BlobError, Blobs, GuestFault, GuestMem, GuestMemoryError, GuestSync, Peers,
    Store, StoreError,
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
        authority: None,
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
    slow_guest_volume: Cell<Option<VolumeId>>,
    store: ModelStore,
    next_store_version: Rc<Cell<u64>>,
    store_get_delay: Cell<u64>,
    store_get_inflight: Cell<usize>,
    store_get_max_inflight: Cell<usize>,
    store_gets: RefCell<Vec<String>>,
    store_range_gets: RefCell<Vec<String>>,
    memory: RefCell<BTreeMap<PageId, Vec<u8>>>,
    installed_vmstate: RefCell<BTreeMap<VolumeId, Vec<u8>>>,
    paused_volumes: RefCell<BTreeSet<VolumeId>>,
    shared_pages: RefCell<BTreeMap<crate::cache::BaseKey, Vec<u8>>>,
    peer_inbox: RefCell<VecDeque<(HostId, PeerMsg)>>,
    peer_outbox: RefCell<Vec<(HostId, PeerMsg)>>,
    peer_send_delay: Cell<u64>,
    unprotected: RefCell<Vec<PageId>>,
    remapped: RefCell<Vec<(PageId, bool)>>,
    writes_after_unprotect: RefCell<BTreeMap<PageId, Vec<u8>>>,
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
        self.store_gets.borrow_mut().push(key.to_owned());
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
        self.store_range_gets.borrow_mut().push(key.to_owned());
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
    async fn send(&self, to: HostId, message: PeerMsg) {
        if self.peer_send_delay.get() != 0 {
            delay(self.peer_send_delay.get()).await;
        }
        if to == TEST_PASSIVE {
            match message {
                PeerMsg::ReplicaStatus {
                    volume,
                    assignment_epoch,
                } => self.peer_inbox.borrow_mut().push_back((
                    TEST_PASSIVE,
                    PeerMsg::ReplicaStatusReply {
                        volume,
                        assignment_epoch,
                        committed: None,
                    },
                )),
                PeerMsg::ReplicaPut {
                    volume,
                    assignment_epoch,
                    artifact,
                    checksum,
                    bytes,
                } => {
                    let _ = bytes;
                    self.peer_inbox.borrow_mut().push_back((
                        TEST_PASSIVE,
                        PeerMsg::ReplicaPutAck {
                            volume,
                            assignment_epoch,
                            artifact,
                            checksum,
                        },
                    ));
                }
                PeerMsg::ReplicaCommit {
                    volume,
                    assignment_epoch,
                    info,
                    record,
                    ..
                } => {
                    let _ = record;
                    self.peer_inbox.borrow_mut().push_back((
                        TEST_PASSIVE,
                        PeerMsg::ReplicaCommitAck {
                            volume,
                            assignment_epoch,
                            info,
                        },
                    ));
                }
                PeerMsg::ReplicaRelease {
                    volume,
                    assignment_epoch,
                    through,
                } => self.peer_inbox.borrow_mut().push_back((
                    TEST_PASSIVE,
                    PeerMsg::ReplicaReleaseAck {
                        volume,
                        assignment_epoch,
                        through,
                    },
                )),
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
        if self.slow_guest_volume.get() == Some(page.volume) && self.guest_read_delay.get() != 0 {
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
        share: (u64, u64, crate::types::ObjectId, u32),
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

    async fn remap(&self, page: PageId, writable: bool) -> Result<(), GuestMemoryError> {
        self.remapped.borrow_mut().push((page, writable));
        Ok(())
    }

    async fn fail(&self, page: PageId) -> Result<(), GuestMemoryError> {
        panic!("unexpected failed fault: {page:?}")
    }

    async fn unprotect(&self, page: PageId) -> Result<(), GuestMemoryError> {
        if self.guest_unprotect_delay.get() != 0 {
            delay(self.guest_unprotect_delay.get()).await;
        }
        if let Some(bytes) = self.writes_after_unprotect.borrow_mut().remove(&page) {
            self.memory.borrow_mut().insert(page, bytes);
        }
        self.unprotected.borrow_mut().push(page);
        Ok(())
    }

    async fn evict(&self, page: PageId) -> Result<(), GuestMemoryError> {
        self.memory.borrow_mut().remove(&page);
        Ok(())
    }

    async fn install_vmstate(
        &self,
        volume: VolumeId,
        bytes: Vec<u8>,
    ) -> Result<(), GuestMemoryError> {
        self.installed_vmstate.borrow_mut().insert(volume, bytes);
        Ok(())
    }

    async fn pause(&self, volume: VolumeId) -> Result<crate::world::GuestPause, GuestMemoryError> {
        let generation = self
            .pause_generation
            .get()
            .checked_add(1)
            .expect("guest pause generation overflow");
        self.pause_generation.set(generation);
        self.paused_volumes.borrow_mut().insert(volume);
        Ok(crate::world::GuestPause {
            vmstate: 77,
            vmstate_bytes: 77_u64.to_le_bytes().to_vec(),
            generation,
        })
    }

    async fn resume(
        &self,
        volume: VolumeId,
        pause: Option<crate::world::GuestPause>,
    ) -> Result<(), GuestMemoryError> {
        if self.guest_resume_delay.get() != 0 {
            delay(self.guest_resume_delay.get()).await;
        }
        if pause.is_some_and(|pause| pause.generation != self.pause_generation.get()) {
            return Ok(());
        }
        self.paused_volumes.borrow_mut().remove(&volume);
        Ok(())
    }

    async fn commit_pause(
        &self,
        _volume: VolumeId,
        pause: crate::world::GuestPause,
    ) -> Result<(), GuestMemoryError> {
        let _current = pause.generation == self.pause_generation.get();
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
        let (request, receive) = request(sync);
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

    async fn fence(&self, _volume: VolumeId) -> Result<(), GuestMemoryError> {
        Ok(())
    }
}

impl AdminIo for ModelWorld {
    async fn next_admin(&self) -> Option<crate::world::AdminRequest> {
        let command = next(&self.admin).await;
        let (request, receive) = request(command);
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

    async fn host_failed(&self, failure: HostFatal) {
        self.host_failures.borrow_mut().push(failure);
    }
}

#[tokio::test(start_paused = true)]
async fn live_membership_update_becomes_current_and_retains_prior_authorization() {
    simulate!(27, async move {
        let initial = test_replica_placement(HostId(1)).expect("initial placement");
        let config = DaemonConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 4,
            writeback_interval: 5,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(initial.clone()),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let world = Rc::new(ModelWorld::default());
        let mut updated = initial.clone();
        updated.membership_epoch = 2;
        updated.roster.push(crate::placement::PeerCandidate {
            host: HostId(7),
            weight: 1,
            failure_domain: 7,
            drained: false,
        });
        updated.roster.sort_by_key(|candidate| candidate.host);
        let (update, reply) = request(AdminCall::UpdateReplicaPlacement {
            placement: updated.clone(),
        });
        handle_admin(Rc::clone(&state), world, update).await;

        assert_eq!(reply.await, Ok(Ok(AdminSuccess::ReplicaPlacementUpdated)));
        let host = state.borrow();
        assert_eq!(host.config.replica_placement.as_ref(), Some(&updated));
        assert_eq!(host.replica_placement_history, [initial]);
    });
}

#[tokio::test(start_paused = true)]
async fn child_fatal_signal_reaches_the_root_and_stops_the_actor_tree() {
    simulate!(9, async move {
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
        let actor = spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::run_ready().await;

        state.borrow_mut().fail("child failed");
        blockd_exec::run_ready().await;

        assert_eq!(
            *world.host_failures.borrow(),
            [HostFatal::new("child failed")]
        );
        assert!(matches!((actor).await, Ok(())));
    });
}

fn pressure_reclaim_fixture() -> (Rc<RefCell<HostState>>, Rc<ModelWorld>, VolumeId, u64) {
    let volume = VolumeId(70);
    let stale_blx = ObjectId(1);
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
    let stale_name = layout::blx_blob(volume, 1, stale_blx);
    {
        let mut host = state.borrow_mut();
        host.insert_fresh(volume, VolumeConfig::data(1));
        let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
        let identity = crate::manifest::ObjectIdentity::volume(volume, 1, stale_blx.0);
        volume_state.blx_blobs.push((identity, 20));
        volume_state.backed_blx_files.insert(identity);
        host.record_blob(stale_name.clone(), 20);
        host.record_blob("live-blx".to_owned(), 95);
        host.disk_reclaim_requested = true;
    }
    world.blobs.borrow_mut().insert(stale_name, vec![0; 20]);
    let incarnation = state.borrow().volumes[&volume].incarnation;
    (state, world, volume, incarnation)
}

#[tokio::test(start_paused = true)]
async fn partial_backed_reclaim_keeps_pressure_above_the_data_watermark() {
    simulate!(93, async move {
        let (state, world, _, _) = pressure_reclaim_fixture();
        let task_state = Rc::clone(&state);
        let task_world = Rc::clone(&world);

        reclaim_backed_blx_files(task_state, task_world.as_ref())
            .await
            .expect("reclaim succeeds");

        let host = state.borrow();
        assert!(!host.disk_reclaim_target_met());
        assert!(host.disk_reclaim_requested);
    });
}

#[tokio::test(start_paused = true)]
async fn partial_cleanup_keeps_pressure_above_the_data_watermark() {
    simulate!(94, async move {
        let (state, world, volume, incarnation) = pressure_reclaim_fixture();
        let task_state = Rc::clone(&state);
        let task_world = Rc::clone(&world);

        cleanup_local(task_state, task_world.as_ref(), volume, incarnation)
            .await
            .expect("cleanup succeeds");

        let host = state.borrow();
        assert!(!host.disk_reclaim_target_met());
        assert!(host.disk_reclaim_requested);
    });
}

#[tokio::test(start_paused = true)]
async fn peer_fetch_replies_only_wake_the_expected_source_waiter() {
    simulate!(91, async move {
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
        let source = spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        assert_eq!((receive).await, Ok(Some(vec![9])));
        drop(source);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn slow_peer_storage_does_not_block_an_unrelated_reply() {
    simulate!(92, async move {
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
        let volume = VolumeId(7);
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.outbound = Some(HostId(2));
        }
        let (reply_io, reply) = state.borrow().peer_client.page(HostId(9));
        world.peer_inbox.borrow_mut().extend([
            (
                HostId(2),
                PeerMsg::FetchRange {
                    io: PeerRequestId(100),
                    volume,
                    replica_assignment_epoch: None,
                    fence: 1,
                    object: ObjectId(1),
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
        let mut source = spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        assert_eq!((reply).await, Ok(Some(vec![9])));
        assert!(
            blockd_exec::now() < 50,
            "reply waited for unrelated storage I/O"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn passive_range_reads_require_the_committed_assignment() {
    simulate!(94, async move {
        let world = Rc::new(ModelWorld::default());
        let source = HostId(2);
        let volume = VolumeId(7);
        let assignment_epoch = 9;
        let artifact = ReplicaArtifact::Blx {
            fence: 4,
            object: ObjectId(3),
        };
        let bytes = vec![10, 11, 12, 13, 14];
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
        state.borrow_mut().replicas.insert(
            ReplicaKey {
                source,
                volume,
                assignment_epoch,
            },
            ReplicaState {
                artifacts: BTreeMap::from([(artifact, (0, bytes))]),
                committed: Some(ReplicaCommitInfo {
                    writer_fence: 4,
                    seq: crate::types::JournalSeq(5),
                    sync_covered_through: 6,
                }),
                ..ReplicaState::default()
            },
        );
        world.peer_inbox.borrow_mut().extend([
            (
                source,
                PeerMsg::FetchRange {
                    io: PeerRequestId(1),
                    volume,
                    replica_assignment_epoch: Some(assignment_epoch),
                    fence: 4,
                    object: ObjectId(3),
                    offset: 1,
                    len: 3,
                },
            ),
            (
                source,
                PeerMsg::FetchRange {
                    io: PeerRequestId(2),
                    volume,
                    replica_assignment_epoch: Some(assignment_epoch + 1),
                    fence: 4,
                    object: ObjectId(3),
                    offset: 1,
                    len: 3,
                },
            ),
        ]);
        let mut peer = spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(100).await;
        peer.cancel();
        blockd_exec::run_ready().await;

        assert!(world.peer_outbox.borrow().contains(&(
            source,
            PeerMsg::Page {
                io: PeerRequestId(1),
                bytes: Some(vec![11, 12, 13]),
            },
        )));
        assert!(world.peer_outbox.borrow().contains(&(
            source,
            PeerMsg::Page {
                io: PeerRequestId(2),
                bytes: None,
            },
        )));
    });
}

#[tokio::test(start_paused = true)]
async fn peer_storage_ingress_defers_overload_without_false_missing_data() {
    simulate!(93, async move {
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
        let volume = VolumeId(7);
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.outbound = Some(HostId(2));
        }
        world.peer_inbox.borrow_mut().extend((0..80).map(|io| {
            (
                HostId(2),
                PeerMsg::FetchRange {
                    io: PeerRequestId(io),
                    volume,
                    replica_assignment_epoch: None,
                    fence: 1,
                    object: ObjectId(1),
                    offset: 0,
                    len: 1,
                },
            )
        }));
        let mut source = spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(10).await;
        assert!(
            world.peer_outbox.borrow().is_empty(),
            "transient overload must wait for the caller's retry instead of reporting missing data"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn saturated_replica_route_defers_to_retry_without_blocking_unrelated_replies() {
    simulate!(96, async move {
        use crate::placement::PeerCandidate;

        let source = HostId(2);
        let local = HostId(1);
        let volume = VolumeId(7);
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
                authority: None,
            }),
        })));
        let (page_io, page_reply) = state.borrow().peer_client.page(HostId(9));
        let replica_reply = state.borrow().peer_client.status(HostId(9), VolumeId(9), 1);
        world.peer_inbox.borrow_mut().extend((0..300).map(|_| {
            (
                source,
                PeerMsg::ReplicaStatus {
                    volume,
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
                volume: VolumeId(9),
                assignment_epoch: 1,
                committed: None,
            },
        ));
        let mut actor = spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));
        assert_eq!((page_reply).await, Ok(Some(vec![9])));
        assert_eq!((replica_reply).await, Ok(None));
        assert!(
            blockd_exec::now() < 5,
            "unrelated reply waited for a saturated replica shard"
        );
        blockd_exec::advance_to(700).await;
        assert!(
            state.borrow().counters.replica_capacity_backpressure > 0,
            "saturated request admission must be explicit so the caller retries"
        );
        actor.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn administrative_ingress_stops_at_the_actor_limit() {
    simulate!(97, async move {
        let world = Rc::new(ModelWorld::default());
        world.blob_write_delay.set(100);
        world.admin.borrow_mut().extend(
            (1..=u64::try_from(ADMIN_CONCURRENCY + 1).expect("limit fits")).map(|id| {
                AdminCall::CreateVolume {
                    volume: VolumeId(id),
                    config: VolumeConfig::data(1),
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
        let mut actor = spawn(admin_source(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(2).await;
        assert_eq!(world.admin.borrow().len(), 1);
        assert_eq!(state.borrow().volumes.len(), ADMIN_CONCURRENCY);
        actor.cancel();
        blockd_exec::run_ready().await;
    });
}
#[tokio::test(start_paused = true)]
async fn guest_fault_ingress_stops_at_the_actor_limit() {
    simulate!(99, async move {
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
                let volume = VolumeId(number);
                host.insert_fresh(volume, VolumeConfig::data(1));
                host.volumes
                    .get_mut(&volume)
                    .expect("inserted volume")
                    .ready = true;
                world.faults.borrow_mut().push_back(GuestFault {
                    page: PageId {
                        volume,
                        page: PageNo(0),
                    },
                    write: false,
                    wp: false,
                    minor: false,
                });
            }
        }
        let mut actor = spawn(fault_source(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(2).await;
        assert_eq!(world.faults.borrow().len(), 1);
        actor.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn slow_sync_capture_does_not_block_ingestion_for_another_volume() {
    simulate!(93, async move {
        let world = Rc::new(ModelWorld::default());
        world.slow_guest_volume.set(Some(VolumeId(1)));
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
            volume: VolumeId(1),
            page: PageNo(0),
        };
        {
            let mut host = state.borrow_mut();
            for volume in [VolumeId(1), VolumeId(2)] {
                host.insert_fresh(volume, VolumeConfig::data(1));
                host.volumes
                    .get_mut(&volume)
                    .expect("inserted volume")
                    .ready = true;
            }
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(first_page, true, false);
            host.volumes
                .get_mut(&VolumeId(1))
                .expect("first volume")
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
                    volume: VolumeId(2),
                })),
        );
        let mut source = spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(10).await;
        assert_eq!(*world.sync_ok.borrow(), [ReqId(100)]);
        assert_eq!(
            state.borrow().volumes[&VolumeId(1)].pending_syncs.len(),
            1,
            "a burst for one volume must retain only its active sync in actor state"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn sync_durability_waiters_apply_backpressure_at_the_global_cap() {
    simulate!(95, async move {
        let world = Rc::new(ModelWorld::default());
        let volume = VolumeId(1);
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
            host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
        }
        world
            .syncs
            .borrow_mut()
            .extend((0..1_100).map(|req| GuestSync {
                req: ReqId(req),
                volume,
            }));
        let mut source = spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(10).await;
        assert_eq!(
            state.borrow().volumes[&volume].pending_syncs.len(),
            super::SYNC_QUEUE_CAPACITY
        );
        assert_eq!(
            world.syncs.borrow().len(),
            1_100 - super::SYNC_QUEUE_CAPACITY,
            "ingress must stop while admitted syncs await replica durability"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn cancelled_sync_callers_release_admission_for_later_requests() {
    simulate!(97, async move {
        let world = Rc::new(ModelWorld::default());
        world.cancel_sync_replies.set(true);
        let volume = VolumeId(1);
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
            host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
        }
        world
            .syncs
            .borrow_mut()
            .extend((0..1_024).map(|req| GuestSync {
                req: ReqId(req),
                volume,
            }));
        let mut source = spawn(sync_source(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(10).await;
        assert!(state.borrow().volumes[&volume].pending_syncs.is_empty());
        assert!(world.syncs.borrow().is_empty());

        world.cancel_sync_replies.set(false);
        world.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(2_000),
            volume,
        });
        blockd_exec::advance_to(20).await;
        assert_eq!(state.borrow().volumes[&volume].pending_syncs.len(), 1);
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn scheduled_work_refills_capacity_within_one_writeback_cadence() {
    simulate!(96, async move {
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
                let volume = VolumeId(number);
                host.insert_fresh(volume, VolumeConfig::data(1));
                let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
                volume_state.ready = true;
                volume_state.mutation_seq = 1;
                let (request, _reply) = request(());
                let ((), reply) = request.into_parts();
                volume_state.pending_syncs.push(PendingSync::new(
                    number,
                    1,
                    reply,
                    resolved.clone(),
                ));
                host.schedule_volume(volume);
            }
        }
        let mut source = spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        blockd_exec::advance_to(150).await;
        assert!(
            state
                .borrow()
                .volumes
                .values()
                .all(|volume_state| volume_state.local_covered_through == 1),
            "volumes beyond the first 64 must not wait for another cadence"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn host_wide_pressure_never_asks_the_passive_to_archive() {
    simulate!(100, async move {
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
        let volume = VolumeId(1);
        {
            let mut host = state.borrow_mut();
            host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.stash_assignment = Some(StashAssignment {
                assignment_epoch: 3,
                active_peer: HostId(2),
                active_assignment_epoch: 3,
                transition_peer: None,
                membership_epoch: 1,
            });
            volume_state.peer_committed = Some(crate::protocol::ReplicaCommitInfo {
                writer_fence: 1,
                seq: crate::types::JournalSeq(1),
                sync_covered_through: 1,
            });
            assert!(!host.try_reserve_blob("pressure".to_owned(), 2));
        }
        let mut source = spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            1,
            false,
        ));
        blockd_exec::advance_to(2).await;
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn scheduler_keeps_capacity_wakeups_that_race_with_handle_reaping() {
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
                let volume = VolumeId(number);
                host.insert_fresh(volume, VolumeConfig::data(1));
                let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
                volume_state.ready = true;
                volume_state.mutation_seq = 1;
                let (request, _reply) = request(());
                let ((), reply) = request.into_parts();
                volume_state.pending_syncs.push(PendingSync::new(
                    number,
                    1,
                    reply,
                    resolved.clone(),
                ));
                if number <= 64 {
                    host.schedule_volume(volume);
                }
            }
        }

        simulate!(seed, async move {
            let late_state = Rc::clone(&state);
            spawn(async move {
                delay(199).await;
                late_state.borrow_mut().schedule_volume(VolumeId(65));
            })
            .detach();
            let mut source = spawn(scheduled_work_source(
                Rc::clone(&state),
                Rc::clone(&world),
                100,
                false,
            ));
            blockd_exec::advance_to(350).await;
            assert_eq!(
                state.borrow().volumes[&VolumeId(65)].local_covered_through,
                1,
                "seed {seed} lost the only scheduler capacity notification"
            );
            source.cancel();
            blockd_exec::run_ready().await;
        });
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_maintenance_attempts_cannot_monopolize_scheduler_capacity() {
    simulate!(98, async move {
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
                let volume = VolumeId(number);
                let page = PageId {
                    volume,
                    page: PageNo(0),
                };
                host.insert_fresh(volume, VolumeConfig::data(1));
                let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
                volume_state.ready = true;
                volume_state.fence = 2;
                volume_state.peer_source = Some(HostId(2));
                volume_state.page_locs.insert(
                    page,
                    (
                        Gen(0),
                        PageFileLoc {
                            base: 0,
                            fence: 1,
                            object: ObjectId(1),
                            offset: 0,
                            len: u32::try_from(page_size()).expect("page size fits"),
                        },
                    ),
                );
                host.schedule_volume(volume);
            }
            let volume = VolumeId(65);
            host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
            let (request, _reply) = request(());
            let ((), reply) = request.into_parts();
            volume_state
                .pending_syncs
                .push(PendingSync::new(65, 1, reply, resolved));
            host.schedule_volume(volume);
        }
        let mut source = spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        blockd_exec::advance_to(150_000_250).await;
        assert_eq!(
            state.borrow().volumes[&VolumeId(65)].local_covered_through,
            1,
            "timed-out hydration attempts must yield capacity to later captures"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn multi_work_volumes_do_not_count_unadmitted_later_ids_as_examined() {
    simulate!(99, async move {
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
                let volume = VolumeId(number);
                host.insert_fresh(volume, VolumeConfig::data(1));
                let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
                volume_state.ready = true;
                if number <= 32 || number == 64 {
                    volume_state.mutation_seq = 1;
                    let (request, _reply) = request(());
                    let ((), reply) = request.into_parts();
                    volume_state.pending_syncs.push(PendingSync::new(
                        number,
                        1,
                        reply,
                        resolved.clone(),
                    ));
                }
                if number <= 32 {
                    volume_state.peer_source = Some(HostId(2));
                }
                host.schedule_volume(volume);
            }
        }
        let mut source = spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        blockd_exec::advance_to(150).await;
        assert_eq!(
            state.borrow().volumes[&VolumeId(64)].local_covered_through,
            1,
            "later IDs reinserted without a slot must be reconsidered in the same cadence"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn capacity_requeued_work_finishes_in_the_same_scheduler_cadence() {
    simulate!(102, async move {
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
                let volume = VolumeId(number);
                host.insert_fresh(volume, VolumeConfig::data(1));
                let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
                volume_state.ready = true;
                volume_state.mutation_seq = 1;
                let (request, _reply) = request(());
                let ((), reply) = request.into_parts();
                volume_state.pending_syncs.push(PendingSync::new(
                    number,
                    1,
                    reply,
                    resolved.clone(),
                ));
                if number == 64 {
                    volume_state.peer_source = Some(HostId(2));
                }
                host.schedule_volume(volume);
            }
        }
        let mut source = spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        blockd_exec::advance_to(150).await;
        assert!(
            world.peer_outbox.borrow().iter().any(|(_, message)| {
                matches!(
                    message,
                    PeerMsg::Released {
                        volume: VolumeId(64),
                        ..
                    }
                )
            }),
            "work requeued at the capacity edge must run after a completion, not next cadence"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn no_progress_maintenance_retries_once_per_cadence_under_saturation() {
    simulate!(104, async move {
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
                let volume = VolumeId(number);
                host.insert_fresh(volume, VolumeConfig::data(1));
                let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
                volume_state.ready = true;
                volume_state.peer_source = Some(HostId(2));
                host.schedule_volume(volume);
            }
        }
        let mut source = spawn(scheduled_work_source(
            Rc::clone(&state),
            Rc::clone(&world),
            100,
            false,
        ));
        blockd_exec::advance_to(350).await;
        let releases = world
            .peer_outbox
            .borrow()
            .iter()
            .filter(|(_, message)| matches!(message, PeerMsg::Released { .. }))
            .count();
        assert_eq!(
            releases,
            65 * 3,
            "no-progress maintenance must not spin between timer cadences"
        );
        source.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn idle_ten_thousand_volumes_schedule_no_child_work_in_bounded_polls() {
    simulate!(94, async move {
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
                let volume = VolumeId(number);
                host.insert_fresh(volume, VolumeConfig::data(1));
                host.schedule_volume(volume);
            }
            assert_eq!(host.scheduled_volume_count(), 10_000);
        }
        let spawned = ({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                let mut children = TaskSet::new();
                let mut child_volumes = std::collections::BTreeMap::new();
                schedule_volume_work(
                    &state,
                    &world,
                    &mut children,
                    &mut child_volumes,
                    &std::collections::BTreeSet::new(),
                );
                assert!(children.is_empty());
                assert!(child_volumes.is_empty());
                children.len()
            }
        })
        .await;
        assert_eq!(spawned, 0);
        assert_eq!(state.borrow().scheduled_volume_count(), 10_000 - 64);
        assert!(
            blockd_exec::simulation_polls() < 400,
            "idle scan used {} polls",
            blockd_exec::simulation_polls()
        );
    });
}

#[test]
fn scheduled_volume_batches_rotate_past_continuously_rescheduled_low_ids() {
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
        let volume = VolumeId(number);
        host.insert_fresh(volume, VolumeConfig::data(1));
        host.schedule_volume(volume);
    }
    let first = host.take_scheduled_volumes(64);
    assert_eq!(first, (1..=64).map(VolumeId).collect::<Vec<_>>());
    for volume in first {
        host.schedule_volume(volume);
    }
    assert_eq!(
        host.take_scheduled_volumes(64),
        (65..=128).map(VolumeId).collect::<Vec<_>>()
    );
}

#[test]
fn published_record_is_not_replicated_again_until_local_state_advances() {
    let volume = VolumeId(129);
    let config = VolumeConfig::data(8);
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
    host.insert_fresh(volume, config);
    let record = JournalRecord {
        config,
        seq: crate::types::JournalSeq(1),
        fence: 4,
        kind: RecordKind::Commit,
        capture_seq: 1,
        sync_covered_through: 1,
        post_state_checksum: 0,
        files: Vec::new(),
        runtime_page_index: BTreeMap::new(),
        migrated_from: None,
    };
    let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
    volume_state.ready = true;
    volume_state.stash_assignment = Some(StashAssignment {
        assignment_epoch: 1,
        active_peer: TEST_PASSIVE,
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: 1,
    });
    volume_state.best_record = Some(record.clone());
    volume_state.peer_published = Some(ReplicaCommitInfo {
        writer_fence: record.fence,
        seq: record.seq,
        sync_covered_through: record.sync_covered_through,
    });
    assert!(
        work_ready(&host, volume)
            .iter()
            .all(|work| !matches!(work, super::ScheduledWork::Replicate))
    );

    let mut newer = record;
    newer.seq = crate::types::JournalSeq(2);
    newer.capture_seq = 2;
    newer.sync_covered_through = 2;
    host.volumes
        .get_mut(&volume)
        .expect("inserted volume")
        .best_record = Some(newer);
    assert!(
        work_ready(&host, volume)
            .iter()
            .any(|work| matches!(work, super::ScheduledWork::Replicate))
    );
}

#[tokio::test(start_paused = true)]
async fn startup_reconciliation_is_bounded_and_emits_in_volume_order() {
    simulate!(95, async move {
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
        let volumes = (1..=65).map(VolumeId).collect::<Vec<_>>();
        {
            let mut host = state.borrow_mut();
            for &volume in &volumes {
                host.insert_fresh(volume, VolumeConfig::data(1));
                host.volumes
                    .get_mut(&volume)
                    .expect("inserted volume")
                    .operations
                    .set_recovery(crate::protocol::Verdict::ColdBoot);
            }
        }
        let reconcile_state = Rc::clone(&state);
        let reconcile_world = Rc::clone(&world);
        let reconcile_volumes = volumes.clone();
        reconcile_backed_volumes(&reconcile_state, &reconcile_world, &reconcile_volumes)
            .await
            .expect("startup reconciliation succeeds");

        assert_eq!(world.store_get_max_inflight.get(), 32);
        assert_eq!(world.store_get_inflight.get(), 0);
        assert_eq!(
            *world.events.borrow(),
            volumes
                .iter()
                .map(|&volume| AdminEvent::VolumeRecovered {
                    volume,
                    verdict: crate::protocol::Verdict::ColdBoot,
                })
                .collect::<Vec<_>>()
        );
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn create_fault_and_checkpoint_form_one_durable_memory_volume_scenario() {
    simulate!(4, async move {
        let volume = VolumeId(7);
        let page = PageId {
            volume,
            page: PageNo(3),
        };
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::memory(8),
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
        let actor_world = Rc::clone(&world);
        let actor_state = Rc::new(RefCell::new(HostState::new(config.clone())));
        let actor = spawn(host_actor_with_state(Rc::clone(&actor_state), actor_world));
        blockd_exec::advance_to(5).await;
        assert_eq!(
            *world.replies.borrow(),
            [Ok(AdminSuccess::VolumeCreated { volume })]
        );

        world.faults.borrow_mut().push_back(GuestFault {
            page,
            write: true,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(7).await;
        let expected = vec![0x5a; page_size()];
        world.memory.borrow_mut().insert(page, expected.clone());
        blockd_exec::advance_to(19).await;
        world.faults.borrow_mut().push_back(GuestFault {
            page,
            write: false,
            wp: false,
            minor: true,
        });
        let remap_deadline = blockd_exec::now().saturating_add(20);
        while world.remapped.borrow().is_empty() {
            assert!(
                blockd_exec::now() < remap_deadline,
                "resident minor fault was not remapped before its bounded deadline"
            );
            blockd_exec::advance_to(blockd_exec::now().saturating_add(1)).await;
        }
        assert!(world.unprotected.borrow().is_empty());
        assert_eq!(*world.remapped.borrow(), [(page, false)]);

        let capture_deadline = blockd_exec::now().saturating_add(40);
        while actor_state.borrow().volumes[&volume]
            .operations
            .mutation_blocked()
        {
            assert!(
                blockd_exec::now() < capture_deadline,
                "sync capture was not completed before its bounded deadline"
            );
            blockd_exec::advance_to(blockd_exec::now().saturating_add(1)).await;
        }
        let checkpoint_expected = vec![0xa5; page_size()];
        world
            .writes_after_unprotect
            .borrow_mut()
            .insert(page, checkpoint_expected.clone());
        let prior_unprotects = world.unprotected.borrow().len();
        world.faults.borrow_mut().push_back(GuestFault {
            page,
            write: true,
            wp: true,
            minor: false,
        });
        let fault_deadline = blockd_exec::now().saturating_add(20);
        while world.unprotected.borrow().len() == prior_unprotects {
            assert!(
                blockd_exec::now() < fault_deadline,
                "write fault was not served before its bounded deadline"
            );
            blockd_exec::advance_to(blockd_exec::now().saturating_add(1)).await;
        }

        world.admin.borrow_mut().push_back(AdminCall::Checkpoint {
            retry: ReqId(3),
            volume,
        });
        blockd_exec::advance_to(25).await;
        assert_eq!(
            *world.replies.borrow(),
            [
                Ok(AdminSuccess::VolumeCreated { volume }),
                Ok(AdminSuccess::CheckpointDone {
                    volume,
                    epoch: crate::types::Epoch(1)
                })
            ]
        );
        blockd_exec::advance_to(300).await;

        let expected_peer_publication = {
            let blobs = world.blobs.borrow();
            let (fence, seq, record_bytes) = blobs
                .iter()
                .filter(|(name, _)| name.ends_with(".rec"))
                .filter_map(|(name, bytes)| {
                    let layout::BlobName::Journal {
                        volume: found_volume,
                        fence,
                        seq,
                    } = layout::parse_blob(name)?
                    else {
                        return None;
                    };
                    let record = JournalRecord::decode(volume, bytes).ok()?;
                    (found_volume == volume
                        && matches!(record.kind, crate::journal::RecordKind::Checkpoint { .. }))
                    .then_some((fence, seq, bytes))
                })
                .max_by_key(|(_, seq, _)| *seq)
                .expect("checkpoint record persisted");
            assert_eq!(
                record_bytes,
                &blobs[&layout::journal_mirror_blob(volume, fence, seq)]
            );
            let record = JournalRecord::decode(volume, record_bytes).expect("valid record");
            assert_eq!(record.capture_seq, 2);
            assert_eq!(record.sync_covered_through, 0);
            for reference in &record.files {
                let bytes = &blobs[&layout::blx_blob(
                    volume,
                    reference.identity.writer_fence,
                    ObjectId(reference.identity.object_id),
                )];
                let object =
                    BlxObject::open(bytes).expect("journal file must be a valid BLX object");
                assert_eq!(crate::manifest::ObjectRef::from_blx(&object), *reference);
            }
            let durable_manifest = {
                let store = world.store.borrow();
                let (_, head_bytes) = &store[&layout::head_key(volume)];
                let head = crate::head::HeadRecord::decode(volume, head_bytes).expect("valid head");
                let pointer = head.manifest.expect("published manifest");
                let (_, manifest_bytes) = &store[&pointer.manifest_key(volume)];
                let manifest =
                    Manifest::decode(volume, manifest_bytes).expect("valid durable manifest");
                let list = manifest.complete_list.map(|reference| {
                    let (_, bytes) = &store[&reference.store_key(volume)];
                    crate::manifest::CompleteFileList::decode(reference, volume, bytes)
                        .expect("valid durable file list")
                });
                assert!(
                    manifest.current_files(list.as_ref()).is_ok(),
                    "durable manifest has an invalid current file set: manifest={manifest:?} list={list:?}"
                );
                manifest
            };
            let expected_peer_publication = crate::protocol::ReplicaCommitInfo {
                writer_fence: record.fence,
                seq: record.seq,
                sync_covered_through: record.sync_covered_through,
            };
            assert_eq!(
                durable_manifest.capture_seq,
                record.capture_seq,
                "store keys: {:?}; store gets: {:?}; blob keys: {:?}; peer committed: {:?}; peer record: {:?}; peer published: {:?}; best record: {:?}; failures: {:?}",
                world.store.borrow().keys().collect::<Vec<_>>(),
                world.store_gets.borrow(),
                world.blobs.borrow().keys().collect::<Vec<_>>(),
                actor_state.borrow().volumes[&volume].peer_committed,
                actor_state.borrow().volumes[&volume]
                    .peer_committed_record
                    .as_ref()
                    .map(|record| (
                        record.fence,
                        record.seq,
                        record.capture_seq,
                        record.files.clone()
                    )),
                actor_state.borrow().volumes[&volume].peer_published,
                actor_state.borrow().volumes[&volume]
                    .best_record
                    .as_ref()
                    .map(|record| (
                        record.fence,
                        record.seq,
                        record.capture_seq,
                        record.files.len()
                    )),
                world.host_failures.borrow()
            );
            assert_eq!(
                durable_manifest.metadata_checksum,
                crate::format::checksum64(&record.encode(volume)),
                "the manifest must identify the exact journal record it publishes"
            );
            assert_eq!(
                record.kind,
                crate::journal::RecordKind::Checkpoint {
                    epoch: crate::types::Epoch(1),
                    vmstate: 77,
                    vmstate_logical_length: 8,
                }
            );
            let checkpoint_object = record
                .files
                .iter()
                .find_map(|file| {
                    let blx = &blobs[&layout::blx_blob(
                        volume,
                        file.identity.writer_fence,
                        ObjectId(file.identity.object_id),
                    )];
                    let object = BlxObject::open(blx).ok()?;
                    object
                        .footer
                        .entries
                        .iter()
                        .any(|entry| entry.key.space == crate::blx::BlockSpace::Vmm)
                        .then_some(object)
                })
                .expect("checkpoint stores VMM bytes in BLX");
            assert_ne!(checkpoint_object.header.pre_state_checksum, 0);
            assert_ne!(checkpoint_object.header.post_state_checksum, 0);
            assert_ne!(
                checkpoint_object.header.pre_state_checksum,
                checkpoint_object.header.post_state_checksum
            );
            assert_eq!(
                durable_manifest.post_state_checksum,
                checkpoint_object.header.post_state_checksum,
                "manifest={durable_manifest:?}; record checksum={}; peer committed={:?}; peer published={:?}; stash={:?}; publication running={}; scheduled={}; store keys={:?}",
                record.post_state_checksum,
                actor_state.borrow().volumes[&volume].peer_committed,
                actor_state.borrow().volumes[&volume].peer_published,
                actor_state.borrow().volumes[&volume].stash_assignment,
                actor_state.borrow().volumes[&volume]
                    .operations
                    .publication_running(),
                actor_state.borrow().scheduled_volume_count(),
                world.store.borrow().keys().collect::<Vec<_>>()
            );
            assert_eq!(durable_manifest.vmstate_logical_length, 8);
            let archived_files = {
                let store = world.store.borrow();
                let list = durable_manifest.complete_list.map(|reference| {
                    let (_, bytes) = &store[&reference.store_key(volume)];
                    crate::manifest::CompleteFileList::decode(reference, volume, bytes)
                        .expect("complete file list")
                });
                durable_manifest
                    .current_files(list.as_ref())
                    .expect("current archive files")
            };
            assert!(
                crate::manifest::max_object_overlap(&archived_files)
                    <= crate::blx::MAX_OVERLAPPING_FILES,
                "publication must compact before the archive overlap limit is exceeded"
            );
            let key = BlockKey::from_page(record.config.kind, page);
            let (file, indexed) = record
                .files
                .iter()
                .filter_map(|file| {
                    let blx = &blobs[&layout::blx_blob(
                        volume,
                        file.identity.writer_fence,
                        ObjectId(file.identity.object_id),
                    )];
                    let object = BlxObject::open(blx).ok()?;
                    Some((file, object.footer.find(key)?))
                })
                .max_by_key(|(_, entry)| entry.generation)
                .expect("checkpoint BLX entry");
            let blx = &blobs[&layout::blx_blob(
                volume,
                file.identity.writer_fence,
                ObjectId(file.identity.object_id),
            )];
            let start = usize::try_from(indexed.offset).expect("fits");
            let end = start + usize::try_from(indexed.length).expect("fits");
            let (_, _, raw) = open_entry(volume, &blx[start..end]).expect("valid page entry");
            assert_eq!(raw, checkpoint_expected);
            expected_peer_publication
        };
        drop(actor);
        blockd_exec::run_ready().await;

        world.memory.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world.events.borrow_mut().clear();
        let recovered_world = Rc::clone(&world);
        let recovered_state = Rc::new(RefCell::new(HostState::new(config)));
        let recovered = spawn(host_actor_with_state(
            Rc::clone(&recovered_state),
            recovered_world,
        ));
        blockd_exec::advance_to(350).await;
        assert_eq!(
            *world.events.borrow(),
            [AdminEvent::VolumeRecovered {
                volume,
                verdict: crate::protocol::Verdict::Resume {
                    epoch: crate::types::Epoch(1),
                    vmstate: 77
                }
            }]
        );
        assert_eq!(
            recovered_state
                .borrow()
                .volumes
                .get(&volume)
                .and_then(|volume_state| volume_state.peer_published),
            Some(expected_peer_publication),
            "recovery must retain the published commit needed to release replica state"
        );
        world.faults.borrow_mut().push_back(GuestFault {
            page,
            write: false,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(352).await;
        assert_eq!(world.memory.borrow().get(&page), Some(&checkpoint_expected));

        drop(recovered);
        blockd_exec::run_ready().await;
    });
}
#[tokio::test(start_paused = true)]
async fn backed_creation_claims_and_publishes_a_fenced_head() {
    simulate!(5, async move {
        let volume = VolumeId(11);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::data(8),
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
        let actor = spawn(host_actor(config.clone(), Rc::clone(&world)));
        blockd_exec::advance_to(100).await;

        assert_eq!(
            *world.replies.borrow(),
            [Ok(AdminSuccess::VolumeCreated { volume })]
        );
        let manifest = {
            let store = world.store.borrow();
            let (_, head_bytes) = &store[&layout::head_key(volume)];
            let head = HeadRecord::decode(volume, head_bytes).expect("valid head");
            assert_eq!(head.holder, HostId(3));
            assert_eq!(head.fence, 1);
            let manifest = head.manifest.expect("initial record published");
            assert_eq!(manifest.fence, 1);
            assert_eq!(manifest.seq, crate::types::JournalSeq(0));
            let (_, manifest_bytes) = &store[&layout::manifest_key(volume, 1, manifest.seq)];
            let record = Manifest::decode(volume, manifest_bytes).expect("valid manifest");
            assert_eq!(record.writer_fence, 1);
            assert_eq!(record.archive_seq, manifest.seq.0);
            manifest
        };
        drop(actor);
        blockd_exec::run_ready().await;

        world.replies.borrow_mut().clear();
        world.events.borrow_mut().clear();
        let recovered = spawn(host_actor(config.clone(), Rc::clone(&world)));
        blockd_exec::advance_to(105).await;
        assert_eq!(
            *world.events.borrow(),
            [AdminEvent::VolumeRecovered {
                volume,
                verdict: crate::protocol::Verdict::ColdBoot
            }]
        );

        drop(recovered);
        blockd_exec::run_ready().await;

        world.blobs.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::RestoreVolume { volume });
        let restore_config = DaemonConfig {
            host: HostId(4),
            replica_placement: test_replica_placement(HostId(4)),
            ..config
        };
        let restored = spawn(host_actor(restore_config, Rc::clone(&world)));
        blockd_exec::advance_to(110).await;
        assert_eq!(*world.replies.borrow(), [Err(AdminError::NotFound)]);
        {
            let store = world.store.borrow();
            let (_, head_bytes) = &store[&layout::head_key(volume)];
            let head = HeadRecord::decode(volume, head_bytes).expect("valid retained head");
            assert_eq!(head.holder, HostId(3));
            assert_eq!(head.manifest, Some(manifest));
        }
        drop(restored);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn failed_backed_fork_does_not_leave_a_head_claim() {
    simulate!(51, async move {
        let volume = VolumeId(111);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::data(8),
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
        let actor = spawn(host_actor(config, Rc::clone(&world)));
        blockd_exec::advance_to(8).await;
        assert_eq!(*world.replies.borrow(), [Err(AdminError::Rejected)]);
        assert!(!world.store.borrow().contains_key(&layout::head_key(volume)));
        drop(actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn restore_data_volume_reads_metadata_then_only_the_faulted_blx_file() {
    simulate!(6, async move {
        let volume = VolumeId(12);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::data(300),
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
        let actor = spawn(host_actor(config, Rc::clone(&world)));
        blockd_exec::advance_to(4).await;

        let pages = (0..256)
            .map(|number| PageId {
                volume,
                page: PageNo(number),
            })
            .collect::<Vec<_>>();
        world
            .faults
            .borrow_mut()
            .extend(pages.iter().copied().map(|page| GuestFault {
                page,
                write: true,
                wp: false,
                minor: false,
            }));
        blockd_exec::advance_to(7).await;
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
        blockd_exec::advance_to(100).await;
        assert_eq!(*world.sync_ok.borrow(), [ReqId(21)]);
        {
            let store = world.store.borrow();
            let (_, head_bytes) = &store[&layout::head_key(volume)];
            let head = HeadRecord::decode(volume, head_bytes).expect("valid head");
            let manifest = head.manifest.expect("capture published");
            let (_, manifest_bytes) = &store[&manifest.manifest_key(volume)];
            let record = Manifest::decode(volume, manifest_bytes).expect("valid manifest");
            assert_eq!(record.recovery_kind, RecoveryKind::DiskOnly);
            let list = record.complete_list.map(|reference| {
                let (_, bytes) = &store[&reference.store_key(volume)];
                crate::manifest::CompleteFileList::decode(reference, volume, bytes)
                    .expect("valid complete file list")
            });
            let archived_files = record
                .current_files(list.as_ref())
                .expect("valid archive file set");
            assert!(!archived_files.is_empty());
        }
        drop(actor);
        blockd_exec::run_ready().await;

        let head_key = layout::head_key(volume);
        let (version, head_bytes) = world.store.borrow()[&head_key].clone();
        let mut retired_head = HeadRecord::decode(volume, &head_bytes).expect("valid head");
        retired_head.stash = None;
        world
            .store
            .borrow_mut()
            .insert(head_key, (version, retired_head.encode()));

        world.blobs.borrow_mut().clear();
        world.memory.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world.store_gets.borrow_mut().clear();
        world.store_range_gets.borrow_mut().clear();
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::RestoreVolume { volume });
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
        let restored = spawn(host_actor(restore_config, Rc::clone(&world)));
        blockd_exec::advance_to(105).await;
        assert_eq!(
            *world.replies.borrow(),
            [Ok(AdminSuccess::VolumeRestored {
                volume,
                verdict: crate::protocol::Verdict::ColdBoot
            })]
        );
        assert!(world.blobs.borrow().is_empty());
        assert_eq!(world.store_gets.borrow().len(), 3);
        assert!(world.store_range_gets.borrow().is_empty());

        let faulted = pages[42];
        world.faults.borrow_mut().push_back(GuestFault {
            page: faulted,
            write: false,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(110).await;
        assert!(world.blobs.borrow().is_empty());
        assert_eq!(world.store_range_gets.borrow().len(), 2);
        assert_eq!(
            world.memory.borrow().get(&faulted),
            Some(&vec![42; page_size()])
        );

        drop(restored);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn whole_restore_seeds_vmm_checksum_state() {
    simulate!(61, async move {
        let volume = VolumeId(32);
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::memory(8),
            from_base: None,
        });
        let source_config = DaemonConfig {
            archive: crate::hostmeta::ArchivePolicy {
                interval: 1,
                ..Default::default()
            },
            host: HostId(20),
            cache_pages: 8,
            writeback_interval: 10,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(20)),
        };
        let source = spawn(host_actor(source_config, Rc::clone(&world)));
        blockd_exec::advance_to(4).await;
        world.admin.borrow_mut().push_back(AdminCall::Checkpoint {
            retry: ReqId(32),
            volume,
        });
        blockd_exec::advance_to(80).await;
        let head_key = layout::head_key(volume);
        let (version, head_bytes) = world.store.borrow()[&head_key].clone();
        let mut head = HeadRecord::decode(volume, &head_bytes).expect("valid head");
        assert_eq!(
            head.manifest
                .and_then(|pointer| {
                    let (_, bytes) = world
                        .store
                        .borrow()
                        .get(&pointer.manifest_key(volume))?
                        .clone();
                    Manifest::decode(volume, &bytes).ok()
                })
                .map(|manifest| manifest.recovery_kind),
            Some(RecoveryKind::Whole)
        );
        drop(source);
        blockd_exec::run_ready().await;

        head.stash = None;
        world
            .store
            .borrow_mut()
            .insert(head_key, (version, head.encode()));
        world.blobs.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        world.installed_vmstate.borrow_mut().clear();
        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::RestoreVolume { volume });
        let restore_config = DaemonConfig {
            archive: Default::default(),
            host: HostId(21),
            cache_pages: 8,
            writeback_interval: 10,
            backup_retry: 2,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: test_replica_placement(HostId(21)),
        };
        let restored_state = Rc::new(RefCell::new(HostState::new(restore_config)));
        let restored = spawn(host_actor_with_state(
            Rc::clone(&restored_state),
            Rc::clone(&world),
        ));
        blockd_exec::advance_to(100).await;

        assert!(world.replies.borrow().iter().any(|reply| {
            matches!(
                reply,
                Ok(AdminSuccess::VolumeRestored {
                    volume: restored,
                    verdict: crate::protocol::Verdict::Resume { vmstate: 77, .. }
                }) if *restored == volume
            )
        }));
        assert_eq!(
            world.installed_vmstate.borrow().get(&volume),
            Some(&77_u64.to_le_bytes().to_vec())
        );
        {
            let state = restored_state.borrow();
            let restored_volume = &state.volumes[&volume];
            let vmm = restored_volume
                .block_checksums
                .iter()
                .filter(|(key, _)| key.space == crate::blx::BlockSpace::Vmm)
                .collect::<Vec<_>>();
            assert!(!vmm.is_empty());
            assert_eq!(
                vmm.into_iter()
                    .fold(0, |checksum, (key, (generation, value))| {
                        checksum ^ crate::blx::state_contribution(*key, *generation, *value)
                    }),
                restored_volume.state_checksum
            );
        }
        drop(restored);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn independently_pinned_data_volume_becomes_a_faultable_fork_base() {
    simulate!(7, async move {
        let source = VolumeId(13);
        let fork = VolumeId(14);
        let base = 90;
        let source_page = PageId {
            volume: source,
            page: PageNo(2),
        };
        let fork_page = PageId {
            volume: fork,
            page: PageNo(2),
        };
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume: source,
            config: VolumeConfig::data(8),
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
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let actor = spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(4).await;
        world.faults.borrow_mut().push_back(GuestFault {
            page: source_page,
            write: true,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(7).await;
        let expected = vec![0xa7; page_size()];
        world
            .memory
            .borrow_mut()
            .insert(source_page, expected.clone());
        world.admin.borrow_mut().push_back(AdminCall::Checkpoint {
            retry: ReqId(31),
            volume: source,
        });
        blockd_exec::advance_to(13).await;
        assert_eq!(world.pause_generation.get(), 0);
        world.admin.borrow_mut().push_back(AdminCall::KeepBase {
            volume: source,
            base,
        });
        blockd_exec::advance_to(18).await;
        assert!(
            world
                .store
                .borrow()
                .contains_key(&layout::base_root_key(base))
        );

        let blx_before_fork = world
            .store
            .borrow()
            .keys()
            .filter(|key| key.ends_with(".blx"))
            .cloned()
            .collect::<BTreeSet<_>>();

        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume: fork,
            config: VolumeConfig::data(8),
            from_base: Some(base),
        });
        blockd_exec::advance_to(23).await;
        let blx_after_fork = world
            .store
            .borrow()
            .keys()
            .filter(|key| key.ends_with(".blx"))
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(blx_after_fork, blx_before_fork);
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::VolumeForked {
                    volume: fork,
                    verdict: crate::protocol::Verdict::ColdBoot,
                }))
        );
        world.faults.borrow_mut().push_back(GuestFault {
            page: fork_page,
            write: false,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(26).await;
        assert_eq!(world.memory.borrow().get(&fork_page), Some(&expected));
        assert_eq!(state.borrow().cache.base_resident_count(), 1);

        world.faults.borrow_mut().push_back(GuestFault {
            page: fork_page,
            write: true,
            wp: true,
            minor: false,
        });
        blockd_exec::advance_to(28).await;
        assert_eq!(world.memory.borrow().get(&fork_page), Some(&expected));
        assert!(state.borrow().cache.is_dirty(fork_page));
        assert_eq!(&*world.unprotected.borrow(), &[fork_page]);
        assert_eq!(state.borrow().counters.shared_fills, 1);
        assert_eq!(state.borrow().counters.wp_faults, 1);

        world
            .admin
            .borrow_mut()
            .push_back(AdminCall::DeleteBase { base });
        blockd_exec::advance_to(30).await;
        assert!(
            !world
                .store
                .borrow()
                .contains_key(&layout::base_root_key(base))
        );

        drop(actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn migration_accepts_only_after_the_destination_record_is_durable() {
    simulate!(8, async move {
        let volume = VolumeId(15);
        let source_host = HostId(8);
        let destination_host = HostId(9);
        let page = PageId {
            volume,
            page: PageNo(1),
        };
        let source = Rc::new(ModelWorld::default());
        let destination = Rc::new(ModelWorld {
            store: Rc::clone(&source.store),
            next_store_version: Rc::clone(&source.next_store_version),
            ..ModelWorld::default()
        });
        source
            .admin
            .borrow_mut()
            .push_back(AdminCall::CreateVolume {
                volume,
                config: VolumeConfig::memory(8),
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
        let source_actor = spawn(host_actor(config(source_host), Rc::clone(&source)));
        let destination_state = Rc::new(RefCell::new(HostState::new(config(destination_host))));
        let destination_actor = spawn(host_actor_with_state(
            Rc::clone(&destination_state),
            Rc::clone(&destination),
        ));
        blockd_exec::advance_to(5).await;
        source.faults.borrow_mut().push_back(GuestFault {
            page,
            write: true,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(8).await;
        let expected = vec![0xc4; page_size()];
        source.memory.borrow_mut().insert(page, expected.clone());
        source.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            volume,
            to: destination_host,
        });
        blockd_exec::advance_to(15).await;
        source.peer_inbox.borrow_mut().push_back((
            HostId(77),
            PeerMsg::MigrateAccept {
                volume,
                offer_fence: 0,
            },
        ));
        blockd_exec::advance_to(16).await;
        assert!(
            !source
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::MigratedOut { volume }))
        );
        let migrated_in = |destination: &ModelWorld| {
            destination
                .events
                .borrow()
                .contains(&AdminEvent::VolumeMigratedIn {
                    volume,
                    verdict: crate::protocol::Verdict::Resume {
                        epoch: crate::types::Epoch(1),
                        vmstate: 77,
                    },
                })
        };
        let mut tick = 20;
        for _ in 0..20 {
            deliver(source_host, &source, &destination, destination_host);
            blockd_exec::advance_to(tick).await;
            tick += 4;
            if migrated_in(&destination) {
                break;
            }
            deliver(destination_host, &destination, &source, source_host);
            blockd_exec::advance_to(tick).await;
            tick += 4;
        }
        assert!(
            migrated_in(&destination),
            "destination must durably install the offered cut before accepting it"
        );
        assert_eq!(
            destination.installed_vmstate.borrow().get(&volume),
            Some(&77_u64.to_le_bytes().to_vec()),
            "destination must install the offered VMM snapshot before resuming"
        );
        assert!(
            destination_state.borrow().volumes[&volume]
                .block_checksums
                .keys()
                .any(|key| key.space == crate::blx::BlockSpace::Vmm),
            "destination must retain the VMM checksum entries for its next checkpoint"
        );
        deliver(destination_host, &destination, &source, source_host);
        blockd_exec::advance_to(tick).await;
        tick += 4;
        assert!(
            source
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::MigratedOut { volume }))
        );
        destination.faults.borrow_mut().push_back(GuestFault {
            page,
            write: false,
            wp: false,
            minor: false,
        });
        for _ in 0..20 {
            blockd_exec::advance_to(tick).await;
            tick += 4;
            if destination.memory.borrow().get(&page) == Some(&expected) {
                break;
            }
            deliver(destination_host, &destination, &source, source_host);
            blockd_exec::advance_to(tick).await;
            tick += 4;
            deliver(source_host, &source, &destination, destination_host);
        }
        assert_eq!(destination.memory.borrow().get(&page), Some(&expected));

        blockd_exec::advance_to(tick + 8).await;
        tick += 8;
        deliver(destination_host, &destination, &source, source_host);
        blockd_exec::advance_to(tick + 4).await;
        tick += 4;
        deliver(source_host, &source, &destination, destination_host);
        blockd_exec::advance_to(tick + 4).await;
        tick += 4;
        blockd_exec::advance_to(tick + 40).await;
        tick += 40;
        deliver(destination_host, &destination, &source, source_host);
        blockd_exec::advance_to(tick + 4).await;
        tick += 4;
        deliver(source_host, &source, &destination, destination_host);
        blockd_exec::advance_to(tick + 4).await;
        tick += 4;
        for _ in 0..20 {
            if !source
                .blobs
                .borrow()
                .contains_key(&layout::handoff_blob(volume))
                && !source.memory.borrow().contains_key(&page)
            {
                break;
            }
            deliver(destination_host, &destination, &source, source_host);
            blockd_exec::advance_to(tick).await;
            tick += 4;
            deliver(source_host, &source, &destination, destination_host);
            blockd_exec::advance_to(tick).await;
            tick += 4;
        }
        assert!(
            !source
                .blobs
                .borrow()
                .contains_key(&layout::handoff_blob(volume))
        );
        assert!(!source.memory.borrow().contains_key(&page));

        drop(source_actor);
        drop(destination_actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn duplicate_migration_accept_requires_the_installed_offer_fence() {
    simulate!(103, async move {
        let volume = VolumeId(16);
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
            host.insert_fresh(volume, VolumeConfig::memory(1));
            let existing = host.volumes.get_mut(&volume).expect("inserted volume");
            existing.ready = true;
            existing.peer_source = Some(source);
            existing.peer_source_offer_fence = Some(7);
        }
        let mut offered = JournalRecord {
            config: VolumeConfig::data(1),
            seq: crate::types::JournalSeq(3),
            fence: 8,
            kind: RecordKind::Commit,
            capture_seq: 2,
            sync_covered_through: 2,
            post_state_checksum: 0,
            files: Vec::new(),
            runtime_page_index: Default::default(),
            migrated_from: None,
        };
        (migrate_in(
            Rc::clone(&state),
            Rc::clone(&world),
            source,
            volume,
            offered.encode_migration(volume),
            None,
        ))
        .await;
        assert!(
            world.peer_outbox.borrow().is_empty(),
            "a different source cut must not receive a correlated acceptance"
        );

        offered.fence = 7;
        (migrate_in(
            state,
            Rc::clone(&world),
            source,
            volume,
            offered.encode_migration(volume),
            None,
        ))
        .await;
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == source
                && matches!(
                    message,
                    PeerMsg::MigrateAccept {
                        volume: accepted,
                        offer_fence: 7,
                    } if *accepted == volume
                )
        }));
    });
}

#[tokio::test(start_paused = true)]
async fn outbound_migration_waits_across_retries_and_cancellation_releases_its_slot() {
    simulate!(80, async move {
        let volume = VolumeId(150);
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
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::data(8),
            from_base: None,
        });
        let mut host = spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(15).await;
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::VolumeCreated { volume }))
        );
        host.cancel();
        blockd_exec::run_ready().await;
        while state.borrow_mut().take_scheduled_volumes(64).len() == 64 {}
        world.peer_outbox.borrow_mut().clear();
        let mut peers = spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));

        let (request, mut reply) = request(AdminCall::MigrateOut {
            volume,
            to: destination,
        });
        let handler = spawn(handle_admin(Rc::clone(&state), Rc::clone(&world), request));
        blockd_exec::advance_to(5_000_100).await;

        assert_eq!(reply.try_recv(), Err(TryRecvError::Empty));
        drop(reply);
        let offers = world
            .peer_outbox
            .borrow()
            .iter()
            .filter(|(to, message)| {
                *to == destination
                    && matches!(message, PeerMsg::MigrateOffer { volume: found, .. } if *found == volume)
            })
            .count();
        assert!(
            offers >= 2,
            "migration must retry without reporting success; observed {offers} offer(s)"
        );

        blockd_exec::run_ready().await;
        assert_eq!((handler).await, Ok(()));
        assert_eq!(state.borrow().volumes[&volume].outbound, Some(destination));
        assert!(
            !state.borrow().volumes[&volume]
                .operations
                .migration_running()
        );
        assert!(
            work_ready(&state.borrow(), volume)
                .iter()
                .any(|work| matches!(work, super::ScheduledWork::Reoffer))
        );
        peers.cancel();
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn migration_reservation_failure_releases_the_operation_for_retry() {
    simulate!(81, async move {
        let volume = VolumeId(151);
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
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::data(8),
            from_base: None,
        });
        let actor = spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(15).await;
        assert!(
            world
                .replies
                .borrow()
                .contains(&Ok(AdminSuccess::VolumeCreated { volume }))
        );
        state.borrow_mut().config.disk_capacity = Some(0);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            volume,
            to: destination,
        });
        blockd_exec::advance_to(20).await;
        assert!(
            world
                .replies
                .borrow()
                .contains(&Err(AdminError::Unavailable))
        );
        assert!(
            !state.borrow().volumes[&volume]
                .operations
                .migration_running()
        );

        state.borrow_mut().config.disk_capacity = None;
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            volume,
            to: destination,
        });
        blockd_exec::advance_to(30).await;
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == destination
                && matches!(message, PeerMsg::MigrateOffer { volume: found, .. } if *found == volume)
        }));
        drop(actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn migration_capture_failure_resumes_the_compute_guest() {
    simulate!(82, async move {
        let volume = VolumeId(153);
        let page = PageId {
            volume,
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
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::memory(8),
            from_base: None,
        });
        let actor = spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(15).await;
        {
            let mut host = state.borrow_mut();
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
            host.volumes
                .get_mut(&volume)
                .expect("created volume")
                .mutation_seq = 1;
        }
        state.borrow_mut().config.disk_capacity = Some(0);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            volume,
            to: HostId(9),
        });
        blockd_exec::advance_to(25).await;

        assert!(
            world
                .replies
                .borrow()
                .contains(&Err(AdminError::Unavailable))
        );
        assert!(!world.paused_volumes.borrow().contains(&volume));
        assert!(world.unprotected.borrow().contains(&page));
        assert!(
            !state.borrow().volumes[&volume]
                .operations
                .migration_running()
        );
        drop(actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn failed_writeback_keeps_the_mutation_slot_until_pages_are_unprotected() {
    simulate!(83, async move {
        let volume = VolumeId(154);
        let page = PageId {
            volume,
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
            host.insert_fresh(volume, VolumeConfig::memory(1));
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
        }
        let capture = spawn(capture_local(Rc::clone(&state), Rc::clone(&world), volume));
        blockd_exec::run_ready().await;

        assert!(
            state.borrow().volumes[&volume]
                .operations
                .mutation_blocked()
        );
        assert!(world.unprotected.borrow().is_empty());
        blockd_exec::advance_to(10).await;
        assert!(
            !state.borrow().volumes[&volume]
                .operations
                .mutation_blocked()
        );
        assert_eq!(*world.unprotected.borrow(), [page]);
        assert_eq!((capture).await, Ok(None));
    });
}

#[test]
fn inbound_fence_is_above_every_surviving_local_artifact_namespace() {
    let volume = VolumeId(156);
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
        .record_blob(layout::blx_blob(volume, 4, ObjectId(9)), 1);
    state.borrow_mut().record_blob(
        layout::journal_blob(volume, 6, crate::types::JournalSeq(11)),
        1,
    );

    assert_eq!(available_inbound_fence(&state, volume, 3), Some(7));
}

#[tokio::test(start_paused = true)]
async fn release_ack_requires_the_current_source_and_destination_fence() {
    simulate!(85, async move {
        let volume = VolumeId(157);
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
            host.insert_fresh(volume, VolumeConfig::memory(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.fence = 9;
            volume_state.peer_source = Some(source);
            let (wake, wait) = oneshot();
            volume_state.hydration_waiters.push(wake);
            wait
        };
        let actor = spawn(peer_source(Rc::clone(&state), Rc::clone(&world)));

        world.peer_inbox.borrow_mut().extend([
            (
                HostId(6),
                PeerMsg::ReleasedAck {
                    volume,
                    release_fence: 9,
                },
            ),
            (
                source,
                PeerMsg::ReleasedAck {
                    volume,
                    release_fence: 8,
                },
            ),
        ]);
        blockd_exec::run_ready().await;
        assert_eq!(state.borrow().volumes[&volume].peer_source, Some(source));

        world.peer_inbox.borrow_mut().push_back((
            source,
            PeerMsg::ReleasedAck {
                volume,
                release_fence: 9,
            },
        ));
        blockd_exec::advance_to(1).await;
        assert_eq!(state.borrow().volumes[&volume].peer_source, None);
        assert_eq!((hydration_done).await, Ok(true));
        drop(actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
async fn checkpoint_capture_failure_resumes_the_compute_guest() {
    simulate!(84, async move {
        let volume = VolumeId(155);
        let page = PageId {
            volume,
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
            host.insert_fresh(volume, VolumeConfig::memory(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }
        let result =
            (checkpoint_local(Rc::clone(&state), Rc::clone(&world), ReqId(1), volume)).await;

        assert!(result.is_none());
        assert!(!world.paused_volumes.borrow().contains(&volume));
    });
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_checkpoint_orders_resume_before_readmission() {
    simulate!(86, async move {
        let volume = VolumeId(157);
        let page = PageId {
            volume,
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.slow_guest_volume.set(Some(volume));
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
            host.insert_fresh(volume, VolumeConfig::memory(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }
        let (request, mut reply) = request(AdminCall::Checkpoint {
            retry: ReqId(1),
            volume,
        });
        let handler = spawn(handle_admin(Rc::clone(&state), Rc::clone(&world), request));
        blockd_exec::advance_to(2).await;
        assert!(world.paused_volumes.borrow().contains(&volume));
        assert!(
            world.peer_outbox.borrow().is_empty(),
            "migration must not start peer work while taking the local cut"
        );
        assert_eq!(reply.try_recv(), Err(TryRecvError::Empty));
        drop(reply);
        blockd_exec::run_ready().await;
        assert_eq!((handler).await, Ok(()));

        let mut second = spawn(checkpoint_local(
            Rc::clone(&state),
            Rc::clone(&world),
            ReqId(2),
            volume,
        ));
        blockd_exec::advance_to(20).await;
        assert!(world.unprotected.borrow().contains(&page));
        {
            let host = state.borrow();
            let operations = &host.volumes[&volume].operations;
            assert!(operations.guest_resume_pending());
            assert!(operations.mutation_owner().is_none());
        }

        blockd_exec::advance_to(55).await;
        assert!(world.paused_volumes.borrow().contains(&volume));
        assert!(matches!(
            state.borrow().volumes[&volume].operations.mutation_owner(),
            Some(MutationOwner::Capture(CaptureKind::Checkpoint))
        ));
        second.cancel();
        blockd_exec::advance_to(110).await;

        assert!(!world.paused_volumes.borrow().contains(&volume));
        assert!(
            !state.borrow().volumes[&volume]
                .operations
                .guest_resume_pending()
        );
        assert!(
            state.borrow().volumes[&volume]
                .operations
                .mutation_owner()
                .is_none()
        );
    });
}

#[tokio::test(start_paused = true)]
async fn cancelling_after_early_resume_unprotects_abandoned_pages() {
    simulate!(88, async move {
        let volume = VolumeId(159);
        let page = PageId {
            volume,
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
            host.insert_fresh(volume, VolumeConfig::memory(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }
        let (request, mut reply) = request(AdminCall::Checkpoint {
            retry: ReqId(1),
            volume,
        });
        let handler = spawn(handle_admin(Rc::clone(&state), Rc::clone(&world), request));
        blockd_exec::advance_to(2).await;
        assert!(!world.paused_volumes.borrow().contains(&volume));
        assert_eq!(reply.try_recv(), Err(TryRecvError::Empty));
        drop(reply);
        blockd_exec::run_ready().await;

        assert_eq!((handler).await, Ok(()));
        assert!(world.unprotected.borrow().contains(&page));
        assert!(
            state.borrow().volumes[&volume]
                .operations
                .mutation_owner()
                .is_none()
        );
    });
}

#[tokio::test(start_paused = true)]
async fn cancelling_an_outbound_migration_resumes_the_paused_guest() {
    simulate!(87, async move {
        let volume = VolumeId(158);
        let page = PageId {
            volume,
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.slow_guest_volume.set(Some(volume));
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
            host.insert_fresh(volume, VolumeConfig::memory(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.mutation_seq = 1;
            volume_state.stash_assignment = Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: TEST_PASSIVE,
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 1,
            });
            assert_eq!(host.cache.reserve_slot(), Some(None));
            host.cache.fill_slot(page, true, false);
        }
        let mut migration = spawn(crate::engine::migrate_out(
            Rc::clone(&state),
            Rc::clone(&world),
            volume,
            HostId(9),
        ));
        blockd_exec::advance_to(2).await;
        assert!(world.paused_volumes.borrow().contains(&volume));
        migration.cancel();
        blockd_exec::run_ready().await;

        assert!(!world.paused_volumes.borrow().contains(&volume));
        assert!(
            !state.borrow().volumes[&volume]
                .operations
                .migration_running()
        );
        assert!(
            state.borrow().volumes[&volume]
                .operations
                .mutation_owner()
                .is_none()
        );
    });
}

#[tokio::test(start_paused = true)]
async fn failed_hydration_resolves_the_waiting_outbound_migration() {
    simulate!(85, async move {
        let volume = VolumeId(156);
        let page = PageId {
            volume,
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
            let incarnation = host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            volume_state.ready = true;
            volume_state.fence = 2;
            volume_state.peer_source = Some(HostId(7));
            volume_state.page_locs.insert(
                page,
                (
                    Gen(0),
                    PageFileLoc {
                        base: 0,
                        fence: 1,
                        object: ObjectId(1),
                        offset: 0,
                        len: u32::try_from(page_size()).expect("page size fits"),
                    },
                ),
            );
            incarnation
        };
        let world = Rc::new(ModelWorld::default());
        let migration = spawn(crate::engine::migrate_out(
            Rc::clone(&state),
            world,
            volume,
            HostId(9),
        ));
        blockd_exec::run_ready().await;
        crate::engine::migration::finish_hydration(&state, volume, incarnation);

        assert_eq!((migration).await, Ok(Some(Err(AdminError::Unavailable))));
        assert!(state.borrow().volumes[&volume].hydration_waiters.is_empty());
    });
}

#[tokio::test(start_paused = true)]
async fn hydration_completion_wakes_shared_mutation_waiters() {
    simulate!(86, async move {
        let volume = VolumeId(157);
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
            let incarnation = host.insert_fresh(volume, VolumeConfig::data(1));
            let volume_state = host.volumes.get_mut(&volume).expect("inserted volume");
            assert!(
                volume_state
                    .operations
                    .try_start_mutation(MutationOwner::Hydration)
            );
            volume_state.mutation_waiters.push(waiter);
            incarnation
        };

        crate::engine::migration::finish_hydration(&state, volume, incarnation);
        assert_eq!((wake).await, Ok(()));
        assert!(state.borrow().volumes[&volume].mutation_waiters.is_empty());
    });
}

#[tokio::test(start_paused = true)]
async fn migration_replaces_a_stale_local_handoff_marker() {
    simulate!(82, async move {
        let volume = VolumeId(152);
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
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::data(8),
            from_base: None,
        });
        let actor = spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        blockd_exec::advance_to(15).await;

        let handoff_name = layout::handoff_blob(volume);
        let stale_handoff = crate::engine::migration::encode_handoff(volume, old_destination);
        world
            .blobs
            .borrow_mut()
            .insert(handoff_name.clone(), stale_handoff.clone());
        state
            .borrow_mut()
            .record_blob(handoff_name.clone(), stale_handoff.len() as u64);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            volume,
            to: destination,
        });
        blockd_exec::advance_to(30).await;

        assert_eq!(
            world
                .blobs
                .borrow()
                .get(&handoff_name)
                .and_then(|bytes| crate::engine::migration::decode_handoff(volume, bytes)),
            Some(destination)
        );
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == destination
                && matches!(message, PeerMsg::MigrateOffer { volume: offered, .. } if *offered == volume)
        }));

        drop(actor);
        blockd_exec::run_ready().await;
    });
}
#[tokio::test(start_paused = true)]
async fn recovered_handoff_reoffers_without_resuming_the_source() {
    simulate!(9, async move {
        let volume = VolumeId(16);
        let page = PageId {
            volume,
            page: PageNo(0),
        };
        let world = Rc::new(ModelWorld::default());
        world.admin.borrow_mut().push_back(AdminCall::CreateVolume {
            volume,
            config: VolumeConfig::data(4),
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
        let actor = spawn(host_actor(config.clone(), Rc::clone(&world)));
        blockd_exec::advance_to(4).await;
        world.faults.borrow_mut().push_back(GuestFault {
            page,
            write: true,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(7).await;
        world
            .memory
            .borrow_mut()
            .insert(page, vec![0xd1; page_size()]);
        world.admin.borrow_mut().push_back(AdminCall::MigrateOut {
            volume,
            to: HostId(11),
        });
        for tick in 15..80 {
            blockd_exec::advance_to(tick).await;
            if world
                .blobs
                .borrow()
                .contains_key(&layout::handoff_blob(volume))
            {
                break;
            }
        }
        assert!(
            world
                .blobs
                .borrow()
                .contains_key(&layout::handoff_blob(volume))
        );

        // The destination may claim the durable head before the source
        // crashes. Recovery must still preserve the outbound peer tier.
        {
            let mut store = world.store.borrow_mut();
            let (_, bytes) = store.get_mut(&layout::head_key(volume)).expect("head");
            let mut head = HeadRecord::decode(volume, bytes).expect("valid head");
            head.holder = HostId(11);
            *bytes = head.encode();
        }

        drop(actor);
        blockd_exec::run_ready().await;
        world.peer_outbox.borrow_mut().clear();
        world.replies.borrow_mut().clear();
        let recovered = spawn(host_actor(config, Rc::clone(&world)));
        blockd_exec::advance_to(18).await;
        assert!(
            world.replies.borrow().is_empty(),
            "unexpected recovery replies: {:?}",
            *world.replies.borrow()
        );
        assert!(world.peer_outbox.borrow().iter().any(|(to, message)| {
            *to == HostId(11)
                && matches!(message, PeerMsg::MigrateOffer { volume: offered, .. } if *offered == volume)
        }));

        drop(recovered);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn passive_replica_acks_only_after_artifact_and_commit_appends() {
    simulate!(10, async move {
        use crate::format::crc32c;
        use crate::journal::{RecordKind, VolumeKind};
        use crate::page_file::PageBatchBuilder;
        use crate::placement::PeerCandidate;
        use crate::protocol::{ReplicaArtifact, ReplicaCommitInfo};

        let source = HostId(20);
        let receiver = HostId(21);
        let volume = VolumeId(17);
        let assignment_epoch = 1;
        let page = PageId {
            volume,
            page: PageNo(0),
        };
        let mut builder = PageBatchBuilder::new(volume, 3, crate::types::ObjectId(0));
        builder.add(page, crate::types::Gen(1), &vec![0xe2; page_size()]);
        let (_, blx, entries) = builder.finish().pop().expect("one blx");
        let artifact = ReplicaArtifact::Blx {
            fence: 3,
            object: crate::types::ObjectId(0),
        };
        let location = entries[0].2;
        let record = JournalRecord {
            config: VolumeConfig {
                kind: VolumeKind::Data,
                pages: 4,
            },
            seq: crate::types::JournalSeq(1),
            fence: 3,
            kind: RecordKind::Commit,
            capture_seq: 1,
            sync_covered_through: 1,
            post_state_checksum: 0,
            files: Vec::new(),
            runtime_page_index: [(page, (crate::types::Gen(1), location))]
                .into_iter()
                .collect(),
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
                authority: None,
            }),
        };
        let actor = spawn(host_actor(config, Rc::clone(&world)));
        blockd_exec::advance_to(2).await;
        world.peer_inbox.borrow_mut().push_back((
            source,
            PeerMsg::ReplicaPut {
                volume,
                assignment_epoch,
                artifact,
                checksum: crc32c(&blx),
                bytes: blx,
            },
        ));
        blockd_exec::advance_to(5).await;
        assert!(matches!(
            world.peer_outbox.borrow().last(),
            Some((host, PeerMsg::ReplicaPutAck { artifact: found, .. }))
                if *host == source && *found == artifact
        ));
        world.peer_inbox.borrow_mut().push_back((
            source,
            PeerMsg::ReplicaCommit {
                volume,
                assignment_epoch,
                info,
                required: vec![artifact],
                record: record.encode(volume),
            },
        ));
        blockd_exec::advance_to(8).await;
        assert!(world.peer_outbox.borrow().iter().any(|(host, message)| {
            *host == source
                && matches!(message, PeerMsg::ReplicaCommitAck { info: found, .. } if *found == info)
        }));
        {
            let spool = world.blobs.borrow();
            let bytes = &spool[&layout::replica_spool_blob(source, volume, assignment_epoch)];
            let scan = crate::replica_spool::scan_replica_spool(bytes).expect("valid spool");
            assert_eq!(scan.commits.last().map(|commit| commit.info), Some(info));
        }
        drop(actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn peer_stashed_sync_waits_for_the_exact_passive_commit() {
    simulate!(11, async move {
        use crate::journal::VolumeKind;
        use crate::placement::PeerCandidate;

        let primary_host = HostId(30);
        let passive_host = HostId(31);
        let volume = VolumeId(18);
        let page = PageId {
            volume,
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
                authority: None,
            }),
        };
        let primary = Rc::new(ModelWorld::default());
        let passive = Rc::new(ModelWorld::default());
        primary
            .admin
            .borrow_mut()
            .push_back(AdminCall::CreateVolume {
                volume,
                config: VolumeConfig {
                    kind: VolumeKind::Data,
                    pages: 8,
                },
                from_base: None,
            });
        let primary_actor = spawn(host_actor(config(primary_host, 1), Rc::clone(&primary)));
        let passive_actor = spawn(host_actor(config(passive_host, 2), Rc::clone(&passive)));
        blockd_exec::advance_to(4).await;
        primary.faults.borrow_mut().push_back(GuestFault {
            page,
            write: true,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(7).await;
        primary
            .memory
            .borrow_mut()
            .insert(page, vec![0xf3; page_size()]);
        primary.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(61),
            volume: page.volume,
        });
        blockd_exec::advance_to(9).await;
        assert!(primary.sync_ok.borrow().is_empty());

        for horizon in 10..50 {
            blockd_exec::advance_to(horizon).await;
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
                    if key != layout::head_key(volume) {
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
        {
            let spools = passive.blobs.borrow();
            assert!(
                !spools.contains_key(&layout::replica_spool_blob(primary_host, volume, 1)),
                "store-covered replica spool must be released"
            );
            let head_bytes = primary.store.borrow()[&layout::head_key(volume)].1.clone();
            let head = HeadRecord::decode(volume, &head_bytes).expect("published peer head");
            assert_eq!(head.manifest.map(|pointer| pointer.capture_seq), Some(1));
            assert_eq!(
                head.stash.map(|stash| stash.active_peer),
                Some(passive_host)
            );
        }
        drop(primary_actor);
        drop(passive_actor);
        blockd_exec::run_ready().await;
    });
}

#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)]
async fn unreachable_stash_is_seeded_and_activated_before_sync_ack() {
    simulate!(72, async move {
        use crate::journal::VolumeKind;
        use crate::placement::{PeerCandidate, rank_stash_candidates};

        let primary_host = HostId(50);
        let candidates = [HostId(51), HostId(52)];
        let volume = VolumeId(72);
        let page = PageId {
            volume,
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
        let ranked = rank_stash_candidates(10, primary_host, 1, volume, &roster);
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
                authority: None,
            }),
        };
        let primary = Rc::new(ModelWorld::default());
        let replacement = Rc::new(ModelWorld {
            store: Rc::clone(&primary.store),
            next_store_version: Rc::clone(&primary.next_store_version),
            ..ModelWorld::default()
        });
        primary
            .admin
            .borrow_mut()
            .push_back(AdminCall::CreateVolume {
                volume,
                config: VolumeConfig {
                    kind: VolumeKind::Data,
                    pages: 8,
                },
                from_base: None,
            });
        let primary_actor = spawn(host_actor(config(primary_host, 1), Rc::clone(&primary)));
        let replacement_actor = spawn(host_actor(
            config(replacement_host, domain),
            Rc::clone(&replacement),
        ));
        blockd_exec::advance_to(5).await;
        primary.faults.borrow_mut().push_back(GuestFault {
            page,
            write: true,
            wp: false,
            minor: false,
        });
        blockd_exec::advance_to(7).await;
        primary
            .memory
            .borrow_mut()
            .insert(page, vec![0x8d; page_size()]);
        primary.syncs.borrow_mut().push_back(GuestSync {
            req: ReqId(301),
            volume: page.volume,
        });

        for horizon in 8..100 {
            blockd_exec::advance_to(horizon).await;
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
        let head_bytes = primary.store.borrow()[&layout::head_key(volume)].1.clone();
        let head = HeadRecord::decode(volume, &head_bytes).expect("transitioned head");
        let assignment = head.stash.expect("stash assignment");
        assert_eq!(assignment.active_peer, replacement_host);
        assert_eq!(assignment.active_assignment_epoch, 2);
        assert_eq!(assignment.transition_peer, None);
        assert!(head.retired_stashes.iter().any(|retired| {
            retired.peer == unreachable
                && retired.assignment_epoch == 1
                && retired.through.sync_covered_through == 1
        }));
        {
            let spool = replacement.blobs.borrow();
            let scan = crate::replica_spool::scan_replica_spool(
                &spool[&layout::replica_spool_blob(primary_host, volume, 2)],
            )
            .expect("replacement spool");
            assert_eq!(
                scan.commits
                    .last()
                    .map(|commit| commit.info.sync_covered_through),
                Some(1)
            );
        }
        drop(primary_actor);
        drop(replacement_actor);
        blockd_exec::run_ready().await;
    });
}
