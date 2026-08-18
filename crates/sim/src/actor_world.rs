//! Turmoil-hosted simulation implementations of the async core world.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_core::blx::scan_object;
use blockd_core::engine::HostFatal;
use blockd_core::head::{HeadRecord, ManifestPtr};
use blockd_core::layout;
use blockd_core::manifest::{Manifest, ManifestClosure, decode_manifest_closure};
use blockd_core::protocol::{AdminCall, AdminEvent, AdminResult, PeerMsg, StoreFault};
use blockd_core::types::{Gen, HostId, PageId, VsetId, page_size};
use blockd_core::world::{
    AdminIo, AdminRequest, BlobEntry, BlobError, Blobs, FillSource, GuestFault, GuestMem,
    GuestMemoryError, GuestPause, GuestSync, GuestSyncRequest, Peers, Store, StoreError,
};
use blockd_exec::channel::{OneSender, UnboundedReceiver, UnboundedSender, oneshot, unbounded};
use blockd_exec::{Response, current_poll, delay, now, random_u64, request};

use crate::model::{
    BlobDevConfig, CrashFate, MAX_OBJECT_BYTES, StoreConfig, StoreCounters, StoreObjectKind,
};
use crate::peer_transport::PeerTransport;

fn random_between(low: u64, high: u64) -> u64 {
    assert!(low <= high);
    low + random_u64() % (high - low + 1)
}

struct ReceiverLease<'a, T> {
    slot: &'a RefCell<Option<UnboundedReceiver<T>>>,
    discard: &'a Cell<bool>,
    receiver: Option<UnboundedReceiver<T>>,
}

struct PendingGuestPause<'a> {
    world: &'a SimWorld,
    vset: VsetId,
    generation: u64,
    active: bool,
}

struct PendingGuestOperation<'a> {
    world: &'a SimWorld,
    vset: VsetId,
    active: bool,
}

impl PendingGuestOperation<'_> {
    fn disarm(mut self) {
        self.active = false;
    }
}

impl Drop for PendingGuestOperation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.world.cancel_guest_op(self.vset);
        }
    }
}

impl PendingGuestPause<'_> {
    fn finish(mut self) {
        self.active = false;
    }
}

impl Drop for PendingGuestPause<'_> {
    fn drop(&mut self) {
        if !self.active
            || self
                .world
                .pause_generations
                .borrow()
                .get(&self.vset)
                .copied()
                != Some(self.generation)
        {
            return;
        }
        self.world.pause_started.borrow_mut().remove(&self.vset);
        let was_paused = self.world.memory.borrow_mut().paused.remove(&self.vset);
        if was_paused {
            // `fault()` enters through `begin_guest_op()`, so a request that
            // observes this paused bit waits in `operation_waiters`; it cannot
            // create a non-inflight `fault_waiters` entry while paused.
            for waiter in self
                .world
                .operation_waiters
                .borrow_mut()
                .remove(&self.vset)
                .unwrap_or_default()
            {
                let _ = waiter.send(());
            }
        }
    }
}

impl<T> ReceiverLease<'_, T> {
    async fn recv(&mut self) -> Option<T> {
        self.receiver.as_mut()?.recv().await
    }
}

impl<T> Drop for ReceiverLease<'_, T> {
    fn drop(&mut self) {
        let mut receiver = self
            .receiver
            .take()
            .expect("receiver lease always owns the receiver");
        if self.discard.replace(false) {
            while receiver.try_recv().is_ok() {}
        }
        let previous = self.slot.borrow_mut().replace(receiver);
        assert!(
            previous.is_none(),
            "receiver returned into an occupied slot"
        );
    }
}

struct Stream<T> {
    sender: UnboundedSender<T>,
    receiver: RefCell<Option<UnboundedReceiver<T>>>,
    discard: Cell<bool>,
}

impl<T> Stream<T> {
    fn new() -> Self {
        let (sender, receiver) = unbounded();
        Self {
            sender,
            receiver: RefCell::new(Some(receiver)),
            discard: Cell::new(false),
        }
    }

    fn send(&self, value: T) -> bool {
        self.sender.send(value).is_ok()
    }

    fn lease(&self) -> ReceiverLease<'_, T> {
        ReceiverLease {
            slot: &self.receiver,
            discard: &self.discard,
            receiver: Some(
                self.receiver
                    .borrow_mut()
                    .take()
                    .expect("one actor receives from each world stream"),
            ),
        }
    }

    async fn recv(&self) -> Option<T> {
        self.lease().recv().await
    }

    fn try_recv(&self) -> Option<T> {
        self.receiver
            .borrow_mut()
            .as_mut()
            .expect("receiver is not leased")
            .try_recv()
            .ok()
    }

    fn discard_pending(&self) -> usize {
        let mut slot = self.receiver.borrow_mut();
        let Some(receiver) = slot.as_mut() else {
            self.discard.set(true);
            return 0;
        };
        let mut discarded = 0;
        while receiver.try_recv().is_ok() {
            discarded += 1;
        }
        discarded
    }
}

#[derive(Clone, Copy)]
enum BlobWriteKind {
    New,
    Append,
}

struct PendingBlob {
    name: String,
    bytes: Vec<u8>,
    kind: BlobWriteKind,
}

#[derive(Default)]
struct BlobState {
    durable: BTreeMap<String, Vec<u8>>,
    pending: BTreeMap<u64, PendingBlob>,
    next_io: u64,
    delete_available: u64,
    bytes_written: u64,
    bytes_read: u64,
    bitflips: u64,
    map_bytes_written: u64,
    max_record_blob_bytes: u64,
}

#[derive(Default)]
struct StoreState {
    objects: BTreeMap<String, (u64, Vec<u8>)>,
    next_version: u64,
    outage: bool,
    attempted_payloads: BTreeSet<(String, usize, u32)>,
    seen_payloads: BTreeSet<(String, usize, u32)>,
    archived_generations: BTreeMap<VsetId, BTreeMap<PageId, Gen>>,
    counters: StoreCounters,
}

impl StoreState {
    fn manifest_closure(&self, vset: VsetId, pointer: ManifestPtr) -> Option<ManifestClosure> {
        let (_, manifest_bytes) =
            self.objects
                .get(&layout::manifest_key(vset, pointer.fence, pointer.seq))?;
        let list_bytes = Manifest::decode(vset, manifest_bytes)
            .ok()?
            .complete_list
            .and_then(|reference| {
                self.objects
                    .get(&layout::complete_file_list_key(
                        vset,
                        reference.writer_fence,
                        reference.list_id,
                    ))
                    .map(|(_, bytes)| bytes.as_slice())
            });
        decode_manifest_closure(vset, pointer, manifest_bytes, list_bytes).ok()
    }

    fn object_kind(key: &str) -> StoreObjectKind {
        if key.ends_with("/head") {
            StoreObjectKind::Head
        } else if key.contains("/m/") {
            StoreObjectKind::Manifest
        } else if key.starts_with("b/") {
            StoreObjectKind::Base
        } else if key.ends_with("/rs") {
            StoreObjectKind::ResumeSet
        } else if key.contains("/s/") {
            StoreObjectKind::Segment
        } else {
            StoreObjectKind::Other
        }
    }

    fn write_attempt(&mut self, key: &str, bytes: &[u8], cas: bool) -> StoreObjectKind {
        let kind = Self::object_kind(key);
        self.counters.put_attempts += 1;
        self.counters.puts_by_kind[kind as usize].attempts += 1;
        self.counters.puts_by_kind[kind as usize].attempted_bytes += bytes.len() as u64;
        if cas {
            self.counters.cas_attempts += 1;
        }
        let identity = (
            key.to_owned(),
            bytes.len(),
            blockd_core::format::crc32c(bytes),
        );
        if !self.attempted_payloads.insert(identity) {
            self.counters.retry_bytes += bytes.len() as u64;
        }
        kind
    }

    fn write_success(&mut self, key: &str, bytes: &[u8], kind: StoreObjectKind, cas: bool) {
        self.counters.puts += 1;
        self.counters.put_successes += 1;
        self.counters.bytes_put += bytes.len() as u64;
        self.counters.puts_by_kind[kind as usize].successes += 1;
        self.counters.puts_by_kind[kind as usize].successful_bytes += bytes.len() as u64;
        if cas {
            self.counters.cas_successes += 1;
        }
        let identity = (
            key.to_owned(),
            bytes.len(),
            blockd_core::format::crc32c(bytes),
        );
        if self.seen_payloads.insert(identity) {
            self.counters.unique_bytes += bytes.len() as u64;
        }
        let bucket = match bytes.len() {
            0..=4096 => 0,
            4097..=65_536 => 1,
            65_537..=1_048_576 => 2,
            1_048_577..=8_388_608 => 3,
            8_388_609..=33_554_432 => 4,
            33_554_433..=67_108_864 => 5,
            _ => 6,
        };
        self.counters.object_size_histogram[bucket] += 1;
    }

    fn observe_archive_head(&mut self, key: &str, bytes: &[u8]) {
        let Some(encoded_vset) = key
            .strip_prefix("v/")
            .and_then(|rest| rest.split('/').next())
        else {
            return;
        };
        let Ok(raw_vset) = u64::from_str_radix(encoded_vset, 16) else {
            return;
        };
        let vset = VsetId(raw_vset);
        let Ok(head) = HeadRecord::decode(vset, bytes) else {
            return;
        };
        let Some(pointer) = head.manifest else {
            return;
        };
        let Some(closure) = self.manifest_closure(vset, pointer) else {
            return;
        };
        let manifest = closure.manifest;
        let files = closure.files;
        let mut current = BTreeMap::new();
        for file in files {
            let Some((_, bytes)) = self.objects.get(&file.identity.store_key()) else {
                continue;
            };
            let Ok((_, footer)) = scan_object(bytes) else {
                continue;
            };
            for entry in footer.entries {
                if let Some(page) = entry.key.to_page(manifest.config.kind, vset) {
                    current.insert(page, entry.generation);
                }
            }
        }
        let previous = self.archived_generations.entry(vset).or_default();
        let changed = current
            .iter()
            .filter(|(page, generation)| previous.get(page) != Some(generation))
            .count() as u64;
        self.counters.logical_changed_bytes += changed * page_size() as u64;
        *previous = current;
    }
}

#[derive(Default)]
struct MemoryState {
    pages: BTreeMap<PageId, Vec<u8>>,
    shared_pages: BTreeMap<(u64, u64, blockd_core::types::SegId, u32), Vec<u8>>,
    protected: BTreeSet<PageId>,
    accessed: BTreeSet<PageId>,
    paused: BTreeSet<VsetId>,
    in_ops: BTreeSet<VsetId>,
    vmstate: BTreeMap<VsetId, u64>,
    failed: BTreeSet<PageId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct OracleSnapshot {
    pub(crate) pages: BTreeMap<PageId, Vec<u8>>,
    pub(crate) unknown: BTreeSet<PageId>,
}

#[derive(Default)]
struct CheckpointOracle {
    by_vmstate: BTreeMap<(VsetId, u64), Vec<OracleSnapshot>>,
    latest: BTreeMap<VsetId, OracleSnapshot>,
    by_capture: BTreeMap<(VsetId, u64), OracleSnapshot>,
}

type OracleState = (
    Rc<RefCell<BTreeMap<PageId, Vec<u8>>>>,
    Rc<RefCell<BTreeSet<PageId>>>,
);

pub(crate) struct PeerAttachment {
    world: Rc<SimWorld>,
}

impl Drop for PeerAttachment {
    fn drop(&mut self) {
        self.world.peer_transport.borrow_mut().take();
    }
}

pub(crate) struct SimWorld {
    host: HostId,
    blob_config: BlobDevConfig,
    store_config: StoreConfig,
    blobs: RefCell<BlobState>,
    store: Rc<RefCell<StoreState>>,
    memory: RefCell<MemoryState>,
    admin: Stream<AdminRequest>,
    admin_events: Stream<(u64, u64, AdminEvent)>,
    admin_event_generations: RefCell<BTreeMap<VsetId, u64>>,
    admin_event_generation_waiters: RefCell<BTreeMap<VsetId, Vec<OneSender<()>>>>,
    incarnation: Cell<u64>,
    faults: Stream<GuestFault>,
    syncs: Stream<GuestSyncRequest>,
    peers: Stream<(HostId, PeerMsg)>,
    peer_transport: RefCell<Option<Rc<PeerTransport>>>,
    aborts: Stream<&'static str>,
    fault_waiters: RefCell<BTreeMap<PageId, Vec<OneSender<bool>>>>,
    faults_inflight: RefCell<BTreeSet<PageId>>,
    aborted: Cell<bool>,
    abort_reason: RefCell<Option<&'static str>>,
    corrupt_fills: Cell<bool>,
    drop_write_protect: Cell<bool>,
    drop_handoff_writes: Cell<bool>,
    handoff_full_remaining: Cell<u8>,
    eio_fired: Cell<bool>,
    blob_fault_epoch: Cell<u64>,
    page_read_poll: Cell<Option<u64>>,
    page_reads_in_poll: Cell<u64>,
    max_page_reads_in_poll: Cell<u64>,
    pause_started: RefCell<BTreeMap<VsetId, u64>>,
    pause_generations: RefCell<BTreeMap<VsetId, u64>>,
    pause_waiters: RefCell<BTreeMap<VsetId, Vec<OneSender<()>>>>,
    operation_waiters: RefCell<BTreeMap<VsetId, Vec<OneSender<()>>>>,
    oracle_pages: RefCell<BTreeMap<VsetId, OracleState>>,
    checkpoint_oracle: Rc<RefCell<CheckpointOracle>>,
    max_pause_ns: Cell<u64>,
}

impl SimWorld {
    pub(crate) fn host_id(&self) -> HostId {
        self.host
    }

    #[cfg(test)]
    pub(crate) fn new(
        host: HostId,
        blob_config: BlobDevConfig,
        store_config: StoreConfig,
    ) -> Rc<Self> {
        Self::with_store(
            host,
            blob_config,
            store_config,
            Rc::new(RefCell::new(StoreState::default())),
            Rc::new(RefCell::new(CheckpointOracle::default())),
        )
    }

    pub(crate) fn max_page_reads_in_poll(&self) -> u64 {
        self.max_page_reads_in_poll.get()
    }

    pub(crate) fn max_pause_ns(&self) -> u64 {
        self.max_pause_ns.get()
    }

    pub(crate) fn cluster(
        hosts: u16,
        blob_config: BlobDevConfig,
        store_config: StoreConfig,
    ) -> Vec<Rc<Self>> {
        let store = Rc::new(RefCell::new(StoreState::default()));
        let checkpoint_oracle = Rc::new(RefCell::new(CheckpointOracle::default()));
        (0..hosts)
            .map(|host| {
                Self::with_store(
                    HostId(host),
                    blob_config,
                    store_config,
                    Rc::clone(&store),
                    Rc::clone(&checkpoint_oracle),
                )
            })
            .collect()
    }

    pub(crate) fn pair(
        hosts: [HostId; 2],
        blob_config: BlobDevConfig,
        store_config: StoreConfig,
    ) -> [Rc<Self>; 2] {
        let store = Rc::new(RefCell::new(StoreState::default()));
        let checkpoint_oracle = Rc::new(RefCell::new(CheckpointOracle::default()));
        hosts.map(|host| {
            Self::with_store(
                host,
                blob_config,
                store_config,
                Rc::clone(&store),
                Rc::clone(&checkpoint_oracle),
            )
        })
    }

    fn with_store(
        host: HostId,
        blob_config: BlobDevConfig,
        store_config: StoreConfig,
        store: Rc<RefCell<StoreState>>,
        checkpoint_oracle: Rc<RefCell<CheckpointOracle>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            host,
            blob_config,
            store_config,
            blobs: RefCell::new(BlobState::default()),
            store,
            memory: RefCell::new(MemoryState::default()),
            admin: Stream::new(),
            admin_events: Stream::new(),
            admin_event_generations: RefCell::new(BTreeMap::new()),
            admin_event_generation_waiters: RefCell::new(BTreeMap::new()),
            incarnation: Cell::new(0),
            faults: Stream::new(),
            syncs: Stream::new(),
            peers: Stream::new(),
            peer_transport: RefCell::new(None),
            aborts: Stream::new(),
            fault_waiters: RefCell::new(BTreeMap::new()),
            faults_inflight: RefCell::new(BTreeSet::new()),
            aborted: Cell::new(false),
            abort_reason: RefCell::new(None),
            corrupt_fills: Cell::new(false),
            drop_write_protect: Cell::new(false),
            drop_handoff_writes: Cell::new(false),
            handoff_full_remaining: Cell::new(blob_config.handoff_full_writes),
            eio_fired: Cell::new(false),
            blob_fault_epoch: Cell::new(0),
            page_read_poll: Cell::new(None),
            page_reads_in_poll: Cell::new(0),
            max_page_reads_in_poll: Cell::new(0),
            pause_started: RefCell::new(BTreeMap::new()),
            pause_generations: RefCell::new(BTreeMap::new()),
            pause_waiters: RefCell::new(BTreeMap::new()),
            operation_waiters: RefCell::new(BTreeMap::new()),
            oracle_pages: RefCell::new(BTreeMap::new()),
            checkpoint_oracle,
            max_pause_ns: Cell::new(0),
        })
    }

    pub(crate) fn attach_peer_transport(
        self: &Rc<Self>,
        transport: Rc<PeerTransport>,
    ) -> PeerAttachment {
        let previous = self.peer_transport.borrow_mut().replace(transport);
        assert!(previous.is_none(), "peer transport already attached");
        PeerAttachment {
            world: Rc::clone(self),
        }
    }

    pub(crate) fn request_admin(&self, call: AdminCall) -> Response<AdminResult> {
        let (request, reply) = request(call);
        assert!(self.admin.send(request), "admin actor is alive");
        reply
    }

    pub(crate) fn register_oracle_pages(
        &self,
        vset: VsetId,
        pages: Rc<RefCell<BTreeMap<PageId, Vec<u8>>>>,
        unknown: Rc<RefCell<BTreeSet<PageId>>>,
    ) {
        self.oracle_pages
            .borrow_mut()
            .insert(vset, (pages, unknown));
    }

    pub(crate) fn checkpoint_snapshots(&self, vset: VsetId, vmstate: u64) -> Vec<OracleSnapshot> {
        self.checkpoint_oracle
            .borrow()
            .by_vmstate
            .get(&(vset, vmstate))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn capture_snapshot(
        &self,
        vset: VsetId,
        capture_seq: u64,
    ) -> Option<OracleSnapshot> {
        self.checkpoint_oracle
            .borrow()
            .by_capture
            .get(&(vset, capture_seq))
            .cloned()
    }

    pub(crate) async fn next_admin_event(&self) -> Option<AdminEvent> {
        self.next_admin_event_with_generation()
            .await
            .map(|(event, _)| event)
    }

    pub(crate) async fn next_admin_event_with_generation(&self) -> Option<(AdminEvent, u64)> {
        loop {
            let (incarnation, generation, event) = self.admin_events.recv().await?;
            if incarnation == self.incarnation.get() {
                return Some((event, generation));
            }
        }
    }

    pub(crate) fn try_next_admin_event(&self) -> Option<AdminEvent> {
        while let Some((incarnation, _, event)) = self.admin_events.try_recv() {
            if incarnation == self.incarnation.get() {
                return Some(event);
            }
        }
        None
    }

    pub(crate) fn admin_event_generation(&self, vset: VsetId) -> u64 {
        self.admin_event_generations
            .borrow()
            .get(&vset)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) async fn wait_for_admin_event_generation_change(
        &self,
        vset: VsetId,
        generation: u64,
    ) {
        let changed = {
            if self.admin_event_generation(vset) == generation {
                let (wake, changed) = oneshot();
                self.admin_event_generation_waiters
                    .borrow_mut()
                    .entry(vset)
                    .or_default()
                    .push(wake);
                Some(changed)
            } else {
                None
            }
        };
        if let Some(changed) = changed {
            let _ = changed.await;
        }
    }

    pub(crate) fn advance_incarnation(&self) {
        let incarnation = self
            .incarnation
            .get()
            .checked_add(1)
            .expect("host incarnation overflow");
        let invalidated = self
            .admin_event_generations
            .borrow()
            .iter()
            .map(|(&vset, &generation)| {
                (
                    vset,
                    generation
                        .checked_add(1)
                        .expect("admin event generation overflow"),
                )
            })
            .collect();
        *self.admin_event_generations.borrow_mut() = invalidated;
        for waiter in std::mem::take(&mut *self.admin_event_generation_waiters.borrow_mut())
            .into_values()
            .flatten()
        {
            let _ = waiter.send(());
        }
        self.incarnation.set(incarnation);
    }

    pub(crate) fn incarnation(&self) -> u64 {
        self.incarnation.get()
    }

    pub(crate) fn durable_blobs(&self) -> Vec<(String, Vec<u8>)> {
        self.blobs
            .borrow()
            .durable
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.clone()))
            .collect()
    }

    pub(crate) fn store_keys(&self) -> Vec<String> {
        self.store.borrow().objects.keys().cloned().collect()
    }

    pub(crate) fn store_counters(&self) -> (u64, u64) {
        let store = self.store.borrow();
        (store.counters.unavailable, store.counters.cas_conflicts)
    }

    pub(crate) fn store_metrics(&self) -> StoreCounters {
        self.store.borrow().counters
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn published_archive_metrics(&self) -> (u64, u64, u64, u64) {
        let store = self.store.borrow();
        let mut files = BTreeMap::new();
        for (key, (_, head_bytes)) in &store.objects {
            let Some(encoded_vset) = key
                .strip_prefix("v/")
                .and_then(|rest| rest.strip_suffix("/head"))
            else {
                continue;
            };
            let Ok(vset) = u64::from_str_radix(encoded_vset, 16).map(VsetId) else {
                continue;
            };
            let Ok(head) = HeadRecord::decode(vset, head_bytes) else {
                continue;
            };
            let Some(pointer) = head.manifest else {
                continue;
            };
            let Some(closure) = store.manifest_closure(vset, pointer) else {
                continue;
            };
            files.extend(closure.files.into_iter().map(|file| (file.identity, file)));
        }

        let mut total = 0_u64;
        let mut newest = BTreeMap::new();
        let mut indexed = Vec::new();
        for file in files.values() {
            let Some((_, bytes)) = store.objects.get(&file.identity.store_key()) else {
                continue;
            };
            let Ok((_, footer)) = scan_object(bytes) else {
                continue;
            };
            total = total.saturating_add(bytes.len() as u64);
            for entry in footer.entries {
                let location = (file.identity, entry.offset, entry.length);
                if newest
                    .get(&entry.key)
                    .is_none_or(|(generation, _)| *generation <= entry.generation)
                {
                    newest.insert(entry.key, (entry.generation, location));
                }
                indexed.push((location, u64::from(entry.length)));
            }
        }
        let live_locations = newest
            .into_values()
            .map(|(_, location)| location)
            .collect::<BTreeSet<_>>();
        let mut live = 0_u64;
        let mut dead = 0_u64;
        for (location, size) in indexed {
            if live_locations.contains(&location) {
                live += size;
            } else {
                dead += size;
            }
        }
        (total, live, dead, total.saturating_sub(live + dead))
    }

    pub(crate) fn store_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.store
            .borrow()
            .objects
            .get(key)
            .map(|(_, bytes)| bytes.clone())
    }

    pub(crate) fn store_snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        self.store
            .borrow()
            .objects
            .iter()
            .map(|(key, (_, bytes))| (key.clone(), bytes.clone()))
            .collect()
    }

    pub(crate) fn blob_count(&self) -> usize {
        self.blobs.borrow().durable.len()
    }

    pub(crate) fn primary_blob_count(&self) -> usize {
        self.blobs
            .borrow()
            .durable
            .keys()
            .filter(|name| {
                !matches!(
                    blockd_core::layout::parse_blob(name),
                    Some(blockd_core::layout::BlobName::ReplicaSpool { .. })
                )
            })
            .count()
    }

    pub(crate) fn rot_store_suffix(&self, suffix: &str) -> u64 {
        let mut store = self.store.borrow_mut();
        let mut changed = 0_u64;
        for (key, (_, bytes)) in &mut store.objects {
            if key.ends_with(suffix) && !bytes.is_empty() {
                bytes[0] ^= 1;
                changed = changed.saturating_add(1);
            }
        }
        changed
    }

    pub(crate) fn set_store_outage(&self, outage: bool) {
        self.store.borrow_mut().outage = outage;
    }

    pub(crate) fn set_corrupt_fills(&self, enabled: bool) {
        self.corrupt_fills.set(enabled);
    }

    pub(crate) fn set_drop_write_protect(&self, enabled: bool) {
        self.drop_write_protect.set(enabled);
    }

    pub(crate) fn set_drop_handoff_writes(&self, enabled: bool) {
        self.drop_handoff_writes.set(enabled);
    }

    pub(crate) fn set_blob_fault_epoch(&self, epoch: u64) {
        self.blob_fault_epoch.set(epoch);
    }

    pub(crate) fn clear_abort(&self) {
        self.aborted.set(false);
        self.abort_reason.borrow_mut().take();
    }

    pub(crate) fn abort_reason(&self) -> Option<&'static str> {
        *self.abort_reason.borrow()
    }

    pub(crate) async fn next_abort(&self) -> Option<&'static str> {
        self.aborts.recv().await
    }

    pub(crate) fn crash_guest_io(&self) {
        for started in std::mem::take(&mut *self.pause_started.borrow_mut()).into_values() {
            self.max_pause_ns
                .set(self.max_pause_ns.get().max(now().saturating_sub(started)));
        }
        let fault_waiters = std::mem::take(&mut *self.fault_waiters.borrow_mut());
        self.faults_inflight.borrow_mut().clear();
        for waiter in fault_waiters.into_values().flatten() {
            let _ = waiter.send(false);
        }
        self.faults.discard_pending();
        self.syncs.discard_pending();
        self.admin.discard_pending();
        self.peers.discard_pending();
        self.aborts.discard_pending();
        let pause_waiters = std::mem::take(&mut *self.pause_waiters.borrow_mut());
        let operation_waiters = std::mem::take(&mut *self.operation_waiters.borrow_mut());
        for waiter in pause_waiters
            .into_values()
            .flatten()
            .chain(operation_waiters.into_values().flatten())
        {
            let _ = waiter.send(());
        }
        *self.memory.borrow_mut() = MemoryState::default();
    }

    pub(crate) fn reset_guest_memory(&self, vset: VsetId) {
        let mut memory = self.memory.borrow_mut();
        memory.pages.retain(|page, _| page.volume.vset != vset);
        memory.accessed.retain(|page| page.volume.vset != vset);
        memory.protected.retain(|page| page.volume.vset != vset);
        memory.failed.retain(|page| page.volume.vset != vset);
        memory.paused.remove(&vset);
        memory.in_ops.remove(&vset);
        memory.vmstate.remove(&vset);
    }

    pub(crate) fn bitflip_segment(&self) -> bool {
        self.bitflip_local(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("blx"))
        })
    }

    pub(crate) fn bitflip_record(&self, mirror: Option<bool>) -> bool {
        let selected = self
            .blobs
            .borrow()
            .durable
            .iter()
            .filter_map(|(name, bytes)| {
                if bytes.is_empty() {
                    return None;
                }
                let extension = std::path::Path::new(name)
                    .extension()
                    .and_then(|extension| extension.to_str());
                let copy_matches = match mirror {
                    Some(false) => extension == Some("rec"),
                    Some(true) => extension == Some("recm"),
                    None => matches!(extension, Some("rec" | "recm")),
                };
                if !copy_matches {
                    return None;
                }
                match blockd_core::layout::parse_blob(name) {
                    Some(blockd_core::layout::BlobName::Journal { fence, seq, .. }) => {
                        Some((fence, seq, name.clone()))
                    }
                    _ => None,
                }
            })
            .max_by_key(|(fence, seq, _)| (*fence, *seq));
        let Some((_, _, name)) = selected else {
            return false;
        };
        let mut blobs = self.blobs.borrow_mut();
        let bytes = blobs.durable.get_mut(&name).expect("selected blob exists");
        let bit = usize::try_from(random_u64() % (bytes.len() as u64 * 8)).expect("bit fits");
        bytes[bit / 8] ^= 1 << (bit % 8);
        blobs.bitflips += 1;
        true
    }

    fn bitflip_local(&self, matches: impl Fn(&str) -> bool) -> bool {
        let names = self
            .blobs
            .borrow()
            .durable
            .iter()
            .filter(|(name, bytes)| matches(name) && !bytes.is_empty())
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        if names.is_empty() {
            return false;
        }
        let name = &names[usize::try_from(random_u64() % names.len() as u64).expect("index fits")];
        let mut blobs = self.blobs.borrow_mut();
        let bytes = blobs.durable.get_mut(name).expect("selected blob exists");
        let bit = usize::try_from(random_u64() % (bytes.len() as u64 * 8)).expect("bit fits");
        bytes[bit / 8] ^= 1 << (bit % 8);
        blobs.bitflips += 1;
        true
    }

    pub(crate) fn bitflips(&self) -> u64 {
        self.blobs.borrow().bitflips
    }

    pub(crate) fn write_metrics(&self) -> (u64, u64) {
        let blobs = self.blobs.borrow();
        (blobs.map_bytes_written, blobs.max_record_blob_bytes)
    }

    pub(crate) fn set_vmstate(&self, vset: VsetId, vmstate: u64) {
        {
            let mut memory = self.memory.borrow_mut();
            memory.vmstate.insert(vset, vmstate);
            memory.in_ops.remove(&vset);
        }
        for waiter in self
            .pause_waiters
            .borrow_mut()
            .remove(&vset)
            .unwrap_or_default()
        {
            let _ = waiter.send(());
        }
    }

    fn cancel_guest_op(&self, vset: VsetId) {
        self.memory.borrow_mut().in_ops.remove(&vset);
        for waiter in self
            .pause_waiters
            .borrow_mut()
            .remove(&vset)
            .unwrap_or_default()
        {
            let _ = waiter.send(());
        }
    }

    async fn begin_guest_op(&self, vset: VsetId) {
        loop {
            let wait = {
                let mut memory = self.memory.borrow_mut();
                if memory.paused.contains(&vset) {
                    let (send, receive) = oneshot();
                    self.operation_waiters
                        .borrow_mut()
                        .entry(vset)
                        .or_default()
                        .push(send);
                    Some(receive)
                } else {
                    memory.in_ops.insert(vset);
                    None
                }
            };
            let Some(wait) = wait else {
                return;
            };
            let _ = wait.await;
        }
    }

    pub(crate) fn is_paused(&self, vset: VsetId) -> bool {
        self.memory.borrow().paused.contains(&vset)
    }

    pub(crate) fn vmstate_ready(&self, vset: VsetId) -> bool {
        self.memory.borrow().vmstate.contains_key(&vset)
    }

    pub(crate) fn page_bytes(&self, page: PageId) -> Option<Vec<u8>> {
        self.memory.borrow().pages.get(&page).cloned()
    }

    pub(crate) fn write_resident(&self, page: PageId, bytes: Vec<u8>) -> bool {
        let mut memory = self.memory.borrow_mut();
        if memory.paused.contains(&page.volume.vset)
            || memory.failed.contains(&page)
            || !memory.pages.contains_key(&page)
            || memory.protected.contains(&page)
        {
            return false;
        }
        memory.pages.insert(page, bytes);
        memory.accessed.insert(page);
        true
    }

    pub(crate) async fn fault(&self, page: PageId, write: bool) -> bool {
        self.begin_guest_op(page.volume.vset).await;
        let operation = PendingGuestOperation {
            world: self,
            vset: page.volume.vset,
            active: true,
        };
        loop {
            let (ready, paused) = {
                let memory = self.memory.borrow();
                let paused = memory.paused.contains(&page.volume.vset);
                (
                    !memory.failed.contains(&page)
                        && !paused
                        && memory.pages.contains_key(&page)
                        && (!write || !memory.protected.contains(&page)),
                    paused,
                )
            };
            if ready {
                self.memory.borrow_mut().accessed.insert(page);
                operation.disarm();
                return true;
            }
            if self.memory.borrow().failed.contains(&page) {
                return false;
            }
            let (send, receive) = oneshot();
            self.fault_waiters
                .borrow_mut()
                .entry(page)
                .or_default()
                .push(send);
            let wp = {
                let memory = self.memory.borrow();
                write && memory.pages.contains_key(&page) && memory.protected.contains(&page)
            };
            if !paused
                && self.faults_inflight.borrow_mut().insert(page)
                && !self.faults.send(GuestFault {
                    page,
                    write,
                    wp,
                    minor: false,
                })
            {
                self.faults_inflight.borrow_mut().remove(&page);
                return false;
            }
            if receive.await != Ok(true) {
                return false;
            }
        }
    }

    pub(crate) async fn sync(&self, sync: GuestSync) -> bool {
        self.begin_guest_op(sync.volume.vset).await;
        let operation = PendingGuestOperation {
            world: self,
            vset: sync.volume.vset,
            active: true,
        };
        let (request, receive) = request(sync);
        if !self.syncs.send(request) {
            return false;
        }
        let synced = receive.await.unwrap_or(false);
        if synced {
            operation.disarm();
        }
        synced
    }

    pub(crate) fn crash_pending(&self) -> Vec<(String, CrashFate)> {
        let pending = std::mem::take(&mut self.blobs.borrow_mut().pending);
        let mut fates = Vec::new();
        for (_, pending) in pending {
            let fate = match random_u64() % 3 {
                0 => {
                    self.apply_blob(pending.name.clone(), pending.bytes, pending.kind);
                    CrashFate::Applied
                }
                1 => CrashFate::Dropped,
                _ => {
                    let kept = usize::try_from(random_u64() % (pending.bytes.len() as u64 + 1))
                        .expect("prefix length fits");
                    self.apply_blob(
                        pending.name.clone(),
                        pending.bytes[..kept].to_vec(),
                        pending.kind,
                    );
                    CrashFate::Torn { kept }
                }
            };
            fates.push((pending.name, fate));
        }
        fates
    }

    fn apply_blob(&self, name: String, bytes: Vec<u8>, kind: BlobWriteKind) {
        let mut blobs = self.blobs.borrow_mut();
        blobs.bytes_written = blobs.bytes_written.saturating_add(bytes.len() as u64);
        let extension = std::path::Path::new(&name)
            .extension()
            .and_then(|extension| extension.to_str());
        if matches!(extension, Some("rec" | "recm" | "map")) {
            blobs.map_bytes_written = blobs.map_bytes_written.saturating_add(bytes.len() as u64);
        }
        if matches!(extension, Some("rec" | "recm")) {
            blobs.max_record_blob_bytes = blobs.max_record_blob_bytes.max(bytes.len() as u64);
        }
        match kind {
            BlobWriteKind::New => {
                blobs.durable.insert(name, bytes);
            }
            BlobWriteKind::Append => blobs.durable.entry(name).or_default().extend(bytes),
        }
    }

    fn submit_blob(&self, name: String, bytes: Vec<u8>, kind: BlobWriteKind) -> u64 {
        let mut blobs = self.blobs.borrow_mut();
        if matches!(kind, BlobWriteKind::New) {
            assert!(
                !blobs.durable.contains_key(&name)
                    && !blobs.pending.values().any(|pending| pending.name == name),
                "blob name reused: {name}"
            );
        } else {
            assert!(
                !blobs.pending.values().any(|pending| pending.name == name),
                "concurrent append to {name}"
            );
        }
        let io = blobs.next_io;
        blobs.next_io = blobs.next_io.checked_add(1).expect("blob io overflow");
        blobs.pending.insert(io, PendingBlob { name, bytes, kind });
        io
    }

    async fn finish_blob(&self, io: u64, latency: u64) -> Result<(), BlobError> {
        delay(latency).await;
        let pending = self.blobs.borrow_mut().pending.remove(&io);
        let Some(pending) = pending else {
            return Err(BlobError::Io);
        };
        self.apply_blob(pending.name, pending.bytes, pending.kind);
        Ok(())
    }

    fn blob_latency(&self, bytes: usize, write: bool) -> u64 {
        let (low, high) = if write {
            (
                self.blob_config.write_latency_min,
                self.blob_config.write_latency_max,
            )
        } else {
            (
                self.blob_config.read_latency_min,
                self.blob_config.read_latency_max,
            )
        };
        random_between(low, high)
            .saturating_add(self.blob_config.ns_per_byte.saturating_mul(bytes as u64))
    }

    fn blob_write_fault(&self, name: &str) -> Option<BlobError> {
        let submitted = now();
        let fault_epoch = self.blob_fault_epoch.get();
        if self
            .blob_config
            .eio_at
            .is_some_and(|at| submitted >= fault_epoch.saturating_add(at))
            && !self.eio_fired.replace(true)
        {
            return Some(BlobError::Io);
        }
        let handoff_remaining = self.handoff_full_remaining.get();
        if name.ends_with("/handoff") && handoff_remaining > 0 {
            self.handoff_full_remaining.set(handoff_remaining - 1);
            return Some(BlobError::Full);
        }
        self.blob_config
            .full_window
            .filter(|(start, stop)| {
                (fault_epoch.saturating_add(*start)..fault_epoch.saturating_add(*stop))
                    .contains(&submitted)
            })
            .map(|_| BlobError::Full)
    }

    fn store_latency(&self, bytes: usize) -> u64 {
        random_between(self.store_config.latency_min, self.store_config.latency_max)
            .saturating_add(self.store_config.ns_per_byte.saturating_mul(bytes as u64))
    }

    fn wake_fault(&self, page: PageId, success: bool) {
        let waiters = self
            .fault_waiters
            .borrow_mut()
            .remove(&page)
            .unwrap_or_default();
        for waiter in waiters {
            let _ = waiter.send(success);
        }
    }

    fn finish_fault(&self, page: PageId, success: bool) {
        self.faults_inflight.borrow_mut().remove(&page);
        self.wake_fault(page, success);
    }
}

impl Blobs for SimWorld {
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
        let latency = self.blob_latency(0, false);
        delay(latency).await;
        Ok(self
            .blobs
            .borrow()
            .durable
            .iter()
            .map(|(name, bytes)| BlobEntry {
                name: name.clone(),
                bytes: bytes.clone(),
                len: bytes.len() as u64,
            })
            .collect())
    }

    async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        let latency = self.blob_latency(bytes.len(), true);
        if let Some(error) = self.blob_write_fault(&name) {
            delay(latency).await;
            return Err(error);
        }
        if self.drop_handoff_writes.get() && name.ends_with("/handoff") {
            delay(latency).await;
            return Ok(());
        }
        if self.blobs.borrow().durable.get(&name) == Some(&bytes) {
            delay(latency).await;
            return Ok(());
        }
        let io = self.submit_blob(name, bytes, BlobWriteKind::New);
        self.finish_blob(io, latency).await
    }

    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        let latency = self.blob_latency(bytes.len(), true);
        if let Some(error) = self.blob_write_fault(&name) {
            delay(latency).await;
            return Err(error);
        }
        let io = self.submit_blob(name, bytes, BlobWriteKind::Append);
        self.finish_blob(io, latency).await
    }

    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
        delay(self.blob_latency(0, true)).await;
        if let Some(bytes) = self.blobs.borrow_mut().durable.get_mut(name) {
            bytes.truncate(usize::try_from(len).expect("blob length fits usize"));
        }
        Ok(())
    }

    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        let bytes = self.blobs.borrow().durable.get(name).cloned();
        let len = bytes.as_ref().map_or(0, Vec::len);
        delay(self.blob_latency(len, false)).await;
        let mut blobs = self.blobs.borrow_mut();
        blobs.bytes_read = blobs.bytes_read.saturating_add(len as u64);
        Ok(bytes)
    }

    async fn read_range(
        &self,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BlobError> {
        let bytes = self.blobs.borrow().durable.get(name).map(|bytes| {
            let start = usize::try_from(offset.min(bytes.len() as u64)).expect("offset fits");
            let end = usize::try_from(offset.saturating_add(len).min(bytes.len() as u64))
                .expect("end fits");
            bytes[start..end].to_vec()
        });
        let got = bytes.as_ref().map_or(0, Vec::len);
        delay(self.blob_latency(got, false)).await;
        Ok(bytes)
    }

    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        let latency = self.blob_latency(0, true);
        let at = {
            let mut blobs = self.blobs.borrow_mut();
            let start = blobs.delete_available.max(now());
            let at = start.saturating_add(latency);
            blobs.delete_available = at;
            at
        };
        delay(at.saturating_sub(now())).await;
        self.blobs.borrow_mut().durable.remove(name);
        Ok(())
    }
}

impl Store for SimWorld {
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
        let parsed_key = layout::parse_key(&key);
        let result = {
            let mut store = self.store.borrow_mut();
            let kind = store.write_attempt(&key, &bytes, false);
            if store.outage {
                store.counters.unavailable = store.counters.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else if bytes.len() > MAX_OBJECT_BYTES {
                store.counters.too_large = store.counters.too_large.saturating_add(1);
                Err(StoreError::TooLarge)
            } else {
                store.next_version = store.next_version.saturating_add(1);
                let version = store.next_version;
                store.write_success(&key, &bytes, kind, false);
                store.objects.insert(key, (version, bytes.clone()));
                Ok(version)
            }
        };
        if result.is_ok()
            && let Some(
                layout::StoreKey::Manifest { vset, .. }
                | layout::StoreKey::ArchiveManifest { vset, .. },
            ) = parsed_key
            && let Ok(manifest) = Manifest::decode(vset, &bytes)
        {
            let mut oracle = self.checkpoint_oracle.borrow_mut();
            if let Some(snapshot) = oracle.latest.get(&vset).cloned() {
                oracle
                    .by_capture
                    .insert((vset, manifest.capture_seq), snapshot);
            }
        }
        delay(self.store_latency(bytes.len())).await;
        result
    }

    async fn put_cas(
        &self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreError> {
        let result = {
            let mut store = self.store.borrow_mut();
            let kind = store.write_attempt(&key, &bytes, true);
            if store.outage {
                store.counters.unavailable = store.counters.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else if bytes.len() > MAX_OBJECT_BYTES {
                store.counters.too_large = store.counters.too_large.saturating_add(1);
                Err(StoreError::TooLarge)
            } else {
                let actual = store.objects.get(&key).map(|(version, _)| *version);
                if actual == expected {
                    store.next_version = store.next_version.saturating_add(1);
                    let version = store.next_version;
                    store.write_success(&key, &bytes, kind, true);
                    if kind == StoreObjectKind::Head {
                        store.observe_archive_head(&key, &bytes);
                    }
                    store.objects.insert(key, (version, bytes.clone()));
                    Ok(version)
                } else {
                    store.counters.cas_conflicts = store.counters.cas_conflicts.saturating_add(1);
                    Err(StoreError::Fault(StoreFault::CasConflict { actual }))
                }
            }
        };
        delay(self.store_latency(bytes.len())).await;
        result
    }

    async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let result = {
            let mut store = self.store.borrow_mut();
            if store.outage {
                store.counters.unavailable = store.counters.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                let found = store.objects.get(key).cloned();
                store.counters.gets = store.counters.gets.saturating_add(1);
                store.counters.bytes_got = store
                    .counters
                    .bytes_got
                    .saturating_add(found.as_ref().map_or(0, |(_, bytes)| bytes.len() as u64));
                Ok(found)
            }
        };
        let len = result
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map_or(0, |(_, bytes)| bytes.len());
        delay(self.store_latency(len)).await;
        result
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let result = {
            let mut store = self.store.borrow_mut();
            if store.outage {
                store.counters.unavailable = store.counters.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                let found = store.objects.get(key).map(|(version, bytes)| {
                    let start =
                        usize::try_from(offset.min(bytes.len() as u64)).expect("offset fits");
                    let end = usize::try_from(offset.saturating_add(len).min(bytes.len() as u64))
                        .expect("end fits");
                    (*version, bytes[start..end].to_vec())
                });
                store.counters.gets = store.counters.gets.saturating_add(1);
                store.counters.bytes_got = store
                    .counters
                    .bytes_got
                    .saturating_add(found.as_ref().map_or(0, |(_, bytes)| bytes.len() as u64));
                Ok(found)
            }
        };
        let got = result
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map_or(0, |(_, bytes)| bytes.len());
        delay(self.store_latency(got)).await;
        result
    }

    async fn delete(&self, key: &str) -> Result<bool, StoreError> {
        let result = {
            let mut store = self.store.borrow_mut();
            if store.outage {
                store.counters.unavailable = store.counters.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                store.counters.deletes = store.counters.deletes.saturating_add(1);
                Ok(store.objects.remove(key).is_some())
            }
        };
        delay(self.store_latency(0)).await;
        result
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let result = {
            let mut store = self.store.borrow_mut();
            if store.outage {
                store.counters.unavailable = store.counters.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                Ok(store
                    .objects
                    .keys()
                    .filter(|key| key.starts_with(prefix))
                    .cloned()
                    .collect())
            }
        };
        delay(self.store_latency(0)).await;
        result
    }
}

impl Peers for SimWorld {
    async fn send(&self, to: HostId, message: PeerMsg) {
        let transport = self
            .peer_transport
            .borrow()
            .clone()
            .expect("peer send requires an attached Tokio transport");
        let _ = transport.send(to, &message).await;
    }

    async fn recv(&self) -> Option<(HostId, PeerMsg)> {
        let transport = self.peer_transport.borrow().clone();
        if let Some(transport) = transport {
            return transport.recv().await;
        }
        self.peers.recv().await
    }
}

impl GuestMem for SimWorld {
    async fn read_page(&self, page: PageId) -> Vec<u8> {
        let poll = current_poll();
        let reads = if self.page_read_poll.get() == Some(poll) {
            self.page_reads_in_poll.get().saturating_add(1)
        } else {
            self.page_read_poll.set(Some(poll));
            1
        };
        self.page_reads_in_poll.set(reads);
        self.max_page_reads_in_poll
            .set(self.max_page_reads_in_poll.get().max(reads));
        let bytes = self
            .memory
            .borrow()
            .pages
            .get(&page)
            .cloned()
            .unwrap_or_else(|| vec![0; blockd_core::types::page_size()]);
        delay(100).await;
        bytes
    }

    async fn arm_write_protect(&self, pages: &[PageId]) -> Result<(), GuestMemoryError> {
        if self.drop_write_protect.get() {
            return Ok(());
        }
        self.memory.borrow_mut().protected.extend(pages);
        Ok(())
    }

    async fn fill(
        &self,
        page: PageId,
        mut bytes: Vec<u8>,
        writable: bool,
        _source: FillSource,
    ) -> Result<(), GuestMemoryError> {
        if self.corrupt_fills.get() && !bytes.is_empty() {
            bytes[0] ^= 1;
        }
        let mut memory = self.memory.borrow_mut();
        memory.pages.insert(page, bytes);
        memory.failed.remove(&page);
        if writable {
            memory.protected.remove(&page);
        } else {
            memory.protected.insert(page);
        }
        drop(memory);
        self.finish_fault(page, true);
        Ok(())
    }

    async fn fill_shared(
        &self,
        page: PageId,
        share: (u64, u64, blockd_core::types::SegId, u32),
        bytes: Option<Vec<u8>>,
        writable: bool,
    ) -> Result<(), GuestMemoryError> {
        if let Some(bytes) = bytes {
            self.memory.borrow_mut().shared_pages.insert(share, bytes);
        }
        let bytes = self.memory.borrow().shared_pages[&share].clone();
        self.fill(page, bytes, writable, FillSource::Store).await
    }

    async fn fail(&self, page: PageId) -> Result<(), GuestMemoryError> {
        self.memory.borrow_mut().failed.insert(page);
        self.finish_fault(page, false);
        Err(GuestMemoryError::Unservable)
    }

    async fn unprotect(&self, page: PageId) -> Result<(), GuestMemoryError> {
        self.memory.borrow_mut().protected.remove(&page);
        self.finish_fault(page, true);
        Ok(())
    }

    async fn evict(&self, page: PageId) -> Result<(), GuestMemoryError> {
        let mut memory = self.memory.borrow_mut();
        memory.pages.remove(&page);
        memory.protected.remove(&page);
        Ok(())
    }

    async fn install_vmstate(&self, vset: VsetId, bytes: Vec<u8>) -> Result<(), GuestMemoryError> {
        let raw: [u8; 8] = bytes
            .get(..8)
            .ok_or(GuestMemoryError::Unservable)?
            .try_into()
            .map_err(|_| GuestMemoryError::Unservable)?;
        self.memory
            .borrow_mut()
            .vmstate
            .insert(vset, u64::from_le_bytes(raw));
        Ok(())
    }

    async fn pause(&self, vset: VsetId) -> Result<GuestPause, GuestMemoryError> {
        let generation = {
            let mut generations = self.pause_generations.borrow_mut();
            let generation = generations
                .get(&vset)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .expect("guest pause generation overflow");
            generations.insert(vset, generation);
            generation
        };
        self.pause_started.borrow_mut().entry(vset).or_insert(now());
        let pending = PendingGuestPause {
            world: self,
            vset,
            generation,
            active: true,
        };
        loop {
            let decision = {
                let mut memory = self.memory.borrow_mut();
                if memory.in_ops.contains(&vset) || !memory.vmstate.contains_key(&vset) {
                    let (send, receive) = oneshot();
                    self.pause_waiters
                        .borrow_mut()
                        .entry(vset)
                        .or_default()
                        .push(send);
                    Err(receive)
                } else {
                    memory.paused.insert(vset);
                    Ok(memory.vmstate.get(&vset).copied().unwrap_or(0))
                }
            };
            match decision {
                Ok(vmstate) => {
                    if let Some((pages, unknown)) = self.oracle_pages.borrow().get(&vset) {
                        let snapshot = OracleSnapshot {
                            pages: pages.borrow().clone(),
                            unknown: unknown.borrow().clone(),
                        };
                        let memory = self.memory.borrow();
                        let zero = vec![0; blockd_core::types::page_size()];
                        for (page, actual) in memory.pages.iter().filter(|(page, _)| {
                            page.volume.vset == vset && !snapshot.unknown.contains(page)
                        }) {
                            assert_eq!(
                                actual,
                                snapshot.pages.get(page).unwrap_or(&zero),
                                "guest model diverged before checkpoint at vmstate {vmstate} for {page:?}"
                            );
                        }
                        drop(memory);
                        let mut oracle = self.checkpoint_oracle.borrow_mut();
                        oracle.latest.insert(vset, snapshot.clone());
                        let candidates = oracle.by_vmstate.entry((vset, vmstate)).or_default();
                        if candidates.last() != Some(&snapshot) {
                            candidates.push(snapshot);
                        }
                    }
                    pending.finish();
                    return Ok(GuestPause {
                        vmstate,
                        vmstate_bytes: vmstate.to_le_bytes().to_vec(),
                        generation,
                    });
                }
                Err(wait) => {
                    let _ = wait.await;
                }
            }
        }
    }

    async fn resume(
        &self,
        vset: VsetId,
        pause: Option<GuestPause>,
    ) -> Result<(), GuestMemoryError> {
        if pause.is_some_and(|pause| {
            self.pause_generations.borrow().get(&vset).copied() != Some(pause.generation)
        }) {
            return Ok(());
        }
        if let Some(started) = self.pause_started.borrow_mut().remove(&vset) {
            self.max_pause_ns
                .set(self.max_pause_ns.get().max(now().saturating_sub(started)));
        }
        self.memory.borrow_mut().paused.remove(&vset);
        for waiter in self
            .operation_waiters
            .borrow_mut()
            .remove(&vset)
            .unwrap_or_default()
        {
            let _ = waiter.send(());
        }
        let pages = self
            .fault_waiters
            .borrow()
            .keys()
            .filter(|page| page.volume.vset == vset)
            .copied()
            .collect::<Vec<_>>();
        for page in pages {
            if !self.faults_inflight.borrow().contains(&page) {
                self.wake_fault(page, true);
            }
        }
        Ok(())
    }

    async fn commit_pause(&self, vset: VsetId, pause: GuestPause) -> Result<(), GuestMemoryError> {
        if self.pause_generations.borrow().get(&vset).copied() != Some(pause.generation) {
            return Ok(());
        }
        if let Some(started) = self.pause_started.borrow_mut().remove(&vset) {
            self.max_pause_ns
                .set(self.max_pause_ns.get().max(now().saturating_sub(started)));
        }
        Ok(())
    }

    async fn harvest_accessed(&self) -> Vec<PageId> {
        std::mem::take(&mut self.memory.borrow_mut().accessed)
            .into_iter()
            .collect()
    }

    async fn next_fault(&self) -> Option<GuestFault> {
        self.faults.recv().await
    }

    async fn next_sync(&self) -> Option<GuestSyncRequest> {
        self.syncs.recv().await
    }

    async fn fence(&self, vset: VsetId) -> Result<(), GuestMemoryError> {
        let pages = self
            .fault_waiters
            .borrow()
            .keys()
            .filter(|page| page.volume.vset == vset)
            .copied()
            .collect::<Vec<_>>();
        for page in pages {
            self.finish_fault(page, false);
        }
        Ok(())
    }
}

impl AdminIo for SimWorld {
    async fn next_admin(&self) -> Option<AdminRequest> {
        self.admin.recv().await
    }

    async fn emit_admin_event(&self, event: AdminEvent) {
        let vset = match event {
            AdminEvent::VsetRecovered { vset, .. } | AdminEvent::VsetMigratedIn { vset, .. } => {
                vset
            }
        };
        let generation = {
            let mut generations = self.admin_event_generations.borrow_mut();
            let generation = generations
                .get(&vset)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .expect("admin event generation overflow");
            generations.insert(vset, generation);
            generation
        };
        for waiter in self
            .admin_event_generation_waiters
            .borrow_mut()
            .remove(&vset)
            .unwrap_or_default()
        {
            let _ = waiter.send(());
        }
        let _ = self
            .admin_events
            .send((self.incarnation.get(), generation, event));
    }

    async fn host_failed(&self, failure: HostFatal) {
        let reason = failure.reason;
        self.aborted.set(true);
        self.abort_reason.borrow_mut().replace(reason);
        let _ = self.aborts.send(reason);
    }
}

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use blockd_core::authority::{
        HostSessionRecord, PlacementRecord, VnodeAuthority, VnodeId, VnodePlacement,
    };
    use blockd_core::engine::{
        AuthorityError, PollSession, activate_host_session, adopt_vnode_generation, cas_placement,
        cas_vnode_authority, challenge_host_session, claim_vnode_authority,
        commit_active_vnode_quorum, commit_vnode_closure, create_host_session, failover_vnode,
        poll_or_defend_host_session, read_host_session, read_vnode_closure, revoke_host_session,
        verify_authority_proof,
    };
    use blockd_core::engine::{HostState, host_actor_with_state};
    use blockd_core::hostmeta::{AuthorityHostConfig, HostConfig, ReplicaPlacementConfig};
    use blockd_core::journal::VsetConfig;
    use blockd_core::placement::PeerCandidate;
    use blockd_core::protocol::{AdminCall, AdminSuccess, ReqId};
    use blockd_core::types::VsetId;
    use blockd_core::vnode_member::adoption_quorum;
    use blockd_exec::{FaultConfig, SimulationContext, delay, spawn, yield_now};

    use super::*;
    use crate::peer_transport::{PeerTransportFaults, PeerTransportStats};

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

    fn run_on_turmoil_hosts<T: 'static>(
        seed: u64,
        worlds: &[Rc<SimWorld>],
        future: impl Future<Output = T> + 'static,
    ) -> T {
        let output = Rc::new(RefCell::new(None));
        let host_output = Rc::clone(&output);
        let host_future = Rc::new(RefCell::new(Some(future)));
        let ready = Rc::new(Cell::new(0_usize));
        let ready_notify = Rc::new(tokio::sync::Notify::new());
        let finished = Rc::new(tokio::sync::Notify::new());
        let client_finished = Rc::clone(&finished);
        let roster = worlds
            .iter()
            .map(|world| (world.host_id(), format!("host-{}", world.host_id().0)))
            .collect::<BTreeMap<_, _>>();
        let stats = Rc::new(PeerTransportStats::default());
        let mut simulation = turmoil::Builder::new()
            .rng_seed(seed)
            .simulation_duration(Duration::from_secs(10))
            .min_message_latency(Duration::from_micros(100))
            .max_message_latency(Duration::from_micros(500))
            .build();
        for world in worlds {
            let host = world.host_id();
            let world = Rc::clone(world);
            let roster = roster.clone();
            let stats = Rc::clone(&stats);
            let ready = Rc::clone(&ready);
            let ready_notify = Rc::clone(&ready_notify);
            simulation.host(format!("host-{}", host.0), move || {
                let world = Rc::clone(&world);
                let roster = roster.clone();
                let stats = Rc::clone(&stats);
                let ready = Rc::clone(&ready);
                let ready_notify = Rc::clone(&ready_notify);
                async move {
                    let transport = PeerTransport::start(
                        host,
                        roster,
                        PeerTransportFaults {
                            max_frames_per_connection: 1,
                            ..PeerTransportFaults::default()
                        },
                        stats,
                    )
                    .await?;
                    let _attachment = world.attach_peer_transport(transport);
                    ready.set(ready.get() + 1);
                    ready_notify.notify_waiters();
                    std::future::pending::<()>().await;
                    Ok(())
                }
            });
        }
        let expected = worlds.len();
        let context = SimulationContext::new(seed, FaultConfig::disabled());
        simulation.client("controller", async move {
            while ready.get() != expected {
                ready_notify.notified().await;
            }
            let future = host_future
                .borrow_mut()
                .take()
                .expect("test software runs once");
            *host_output.borrow_mut() = Some(context.scope(future).await);
            finished.notify_one();
            Ok(())
        });
        simulation.client("completion", async move {
            client_finished.notified().await;
            Ok(())
        });
        if let Err(error) = simulation.run() {
            panic!(
                "Turmoil test cluster: {error:?}, transport={:?}",
                stats.snapshot()
            );
        }
        output.borrow_mut().take().expect("test completed")
    }

    fn authority_placement() -> PlacementRecord {
        PlacementRecord::new(
            41,
            7,
            vec![VnodePlacement {
                vnode: VnodeId(0),
                members: [HostId(1), HostId(2), HostId(3)],
                next_members: None,
            }],
        )
        .expect("valid test placement")
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_session_polling_uses_reads_without_lease_writes() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let actor_world = Rc::clone(&world);
        simulate!(8, async move {
            create_host_session(actor_world.as_ref(), HostId(1), 101).await?;
            for _ in 0..4 {
                let polled =
                    poll_or_defend_host_session(actor_world.as_ref(), HostId(1), 101).await?;
                assert!(matches!(polled, PollSession::Active(_)));
            }
            Ok::<_, AuthorityError>(())
        })
        .expect("healthy holder remains active");

        let metrics = world.store_metrics();
        assert_eq!(metrics.gets, 4);
        assert_eq!(metrics.cas_attempts, 1);
        assert_eq!(metrics.cas_successes, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn exact_challenge_cas_fences_the_losing_side() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let actor_world = Rc::clone(&world);
        simulate!(9, async move {
            create_host_session(actor_world.as_ref(), HostId(1), 101)
                .await
                .expect("create session");
            let challenged =
                challenge_host_session(actor_world.as_ref(), HostId(1), HostId(2), 9001, 50)
                    .await
                    .expect("install challenge");

            let joined =
                challenge_host_session(actor_world.as_ref(), HostId(1), HostId(3), 9002, 51)
                    .await
                    .expect("concurrent suspect joins existing challenge");
            assert_eq!(joined, challenged);

            assert!(matches!(
                poll_or_defend_host_session(actor_world.as_ref(), HostId(1), 101).await,
                Ok(PollSession::Defended(_))
            ));
            assert_eq!(
                revoke_host_session(actor_world.as_ref(), challenged, 9001).await,
                Err(AuthorityError::Conflict)
            );
        });

        let metrics = world.store_metrics();
        assert_eq!(metrics.cas_successes, 3);
        assert_eq!(metrics.cas_conflicts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn revocation_must_land_before_replacement_session_activates() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        simulate!(10, async move {
            create_host_session(world.as_ref(), HostId(1), 101)
                .await
                .expect("create session");
            let challenged = challenge_host_session(world.as_ref(), HostId(1), HostId(2), 9001, 50)
                .await
                .expect("install challenge");
            let revoked = revoke_host_session(world.as_ref(), challenged, 9001)
                .await
                .expect("revoke old session");
            let replacement = activate_host_session(world.as_ref(), revoked, 202)
                .await
                .expect("activate replacement");
            assert_eq!(
                replacement.record,
                HostSessionRecord::Active {
                    host: HostId(1),
                    session: 202,
                    epoch: 2,
                }
            );
            assert_eq!(
                poll_or_defend_host_session(world.as_ref(), HostId(1), 101).await,
                Err(AuthorityError::Fenced)
            );
        });
    }

    #[tokio::test(start_paused = true)]
    async fn replicas_reread_vnode_authority_and_reject_stale_proofs() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let placement = authority_placement();
        simulate!(11, async move {
            create_host_session(world.as_ref(), HostId(1), 101)
                .await
                .expect("create initial primary session");
            let initial = VnodeAuthority {
                cluster_id: placement.cluster_id,
                placement_epoch: placement.epoch,
                vnode: VnodeId(0),
                generation: 1,
                primary: HostId(1),
                primary_session: 101,
                primary_host_epoch: 1,
            };
            let old_proof = cas_vnode_authority(world.as_ref(), &placement, None, initial)
                .await
                .expect("create vnode authority");
            verify_authority_proof(world.as_ref(), &placement, old_proof)
                .await
                .expect("current proof verifies");

            create_host_session(world.as_ref(), HostId(2), 202)
                .await
                .expect("create replacement primary session");
            let next = initial
                .advance(HostId(2), 202, 1)
                .expect("advance authority");
            let current = cas_vnode_authority(world.as_ref(), &placement, Some(old_proof), next)
                .await
                .expect("advance vnode authority");

            assert_eq!(
                verify_authority_proof(world.as_ref(), &placement, old_proof).await,
                Err(AuthorityError::Fenced)
            );
            verify_authority_proof(world.as_ref(), &placement, current)
                .await
                .expect("new proof verifies");
            assert_eq!(
                cas_vnode_authority(world.as_ref(), &placement, Some(old_proof), next).await,
                Err(AuthorityError::Conflict)
            );
        });
    }

    #[tokio::test(start_paused = true)]
    async fn intersecting_adoption_quorum_preserves_the_latest_protected_closure() {
        let worlds = SimWorld::cluster(4, BlobDevConfig::nvme(), StoreConfig::gcs());
        let placement = authority_placement();
        let old = VnodeAuthority {
            cluster_id: placement.cluster_id,
            placement_epoch: placement.epoch,
            vnode: VnodeId(0),
            generation: 1,
            primary: HostId(1),
            primary_session: 101,
            primary_host_epoch: 1,
        };
        simulate!(12, async move {
            create_host_session(worlds[1].as_ref(), HostId(1), 101)
                .await
                .expect("create old primary session");
            let old_proof = cas_vnode_authority(worlds[1].as_ref(), &placement, None, old)
                .await
                .expect("create old authority");
            adopt_vnode_generation(worlds[1].as_ref(), &placement, HostId(1), old_proof)
                .await
                .expect("member one adopts");
            adopt_vnode_generation(worlds[2].as_ref(), &placement, HostId(2), old_proof)
                .await
                .expect("member two adopts");

            let gets_before_commit = worlds[1].store_metrics().gets;
            let closure = b"latest acknowledged recovery closure".to_vec();
            commit_vnode_closure(
                worlds[2].as_ref(),
                &placement,
                old,
                VsetId(7),
                44,
                closure.clone(),
            )
            .await
            .expect("first member commits");
            commit_vnode_closure(
                worlds[1].as_ref(),
                &placement,
                old,
                VsetId(7),
                44,
                closure.clone(),
            )
            .await
            .expect("second member commits");
            assert_eq!(worlds[1].store_metrics().gets, gets_before_commit);

            create_host_session(worlds[3].as_ref(), HostId(3), 303)
                .await
                .expect("create new primary session");
            let next = old.advance(HostId(3), 303, 1).expect("advance authority");
            let next_proof =
                cas_vnode_authority(worlds[3].as_ref(), &placement, Some(old_proof), next)
                    .await
                    .expect("publish next authority");
            let receipt_two =
                adopt_vnode_generation(worlds[2].as_ref(), &placement, HostId(2), next_proof)
                    .await
                    .expect("intersection member adopts");
            let receipt_three =
                adopt_vnode_generation(worlds[3].as_ref(), &placement, HostId(3), next_proof)
                    .await
                    .expect("new primary member adopts");
            let inventory = adoption_quorum(&placement, next_proof, &[receipt_two, receipt_three])
                .expect("two members form adoption quorum");
            let recovered = inventory[&VsetId(7)];
            assert_eq!(recovered.sequence, 44);
            assert_eq!(
                read_vnode_closure(worlds[2].as_ref(), VnodeId(0), recovered).await,
                Ok(closure)
            );

            assert_eq!(
                commit_vnode_closure(
                    worlds[2].as_ref(),
                    &placement,
                    old,
                    VsetId(7),
                    45,
                    b"stale primary".to_vec(),
                )
                .await,
                Err(AuthorityError::Fenced)
            );
        });
    }

    #[tokio::test(start_paused = true)]
    async fn host_self_fences_when_session_gets_exceed_the_staleness_bound() {
        let worlds = SimWorld::cluster(
            3,
            BlobDevConfig {
                read_latency_min: 1,
                read_latency_max: 1,
                write_latency_min: 1,
                write_latency_max: 1,
                ns_per_byte: 0,
                full_window: None,
                handoff_full_writes: 0,
                eio_at: None,
            },
            StoreConfig {
                latency_min: 1,
                latency_max: 1,
                ns_per_byte: 0,
            },
        );
        let placement = PlacementRecord::new(
            41,
            1,
            vec![VnodePlacement {
                vnode: VnodeId(0),
                members: [HostId(0), HostId(1), HostId(2)],
                next_members: None,
            }],
        )
        .expect("valid placement");
        let roster = [HostId(0), HostId(1), HostId(2)]
            .into_iter()
            .map(|host| PeerCandidate {
                host,
                weight: 1,
                failure_domain: host.0,
                drained: false,
            })
            .collect();
        let config = HostConfig {
            archive: Default::default(),
            host: HostId(0),
            cache_pages: 8,
            writeback_interval: 100,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(ReplicaPlacementConfig {
                membership_epoch: 1,
                local_failure_domain: 0,
                roster,
                authority: Some(AuthorityHostConfig {
                    cluster_id: 41,
                    poll_interval: 10,
                    max_poll_staleness: 30,
                    challenge_interval: 40,
                }),
            }),
        };
        let state = Rc::new(RefCell::new(HostState::new(config)));
        simulate!(13, async move {
            {
                let world = Rc::clone(&worlds[0]);
                cas_placement(world.as_ref(), None, placement)
                    .await
                    .expect("install placement");
            }
            let mut host = spawn(host_actor_with_state(
                Rc::clone(&state),
                Rc::clone(&worlds[0]),
            ));
            let boot_horizon = now().saturating_add(60);
            blockd_exec::advance_to(boot_horizon).await;
            assert!(state.borrow().authority_serving());
            assert!(state.borrow().counters.lease_gets > 0);

            worlds[0].set_store_outage(true);
            blockd_exec::advance_to(boot_horizon.saturating_add(40)).await;
            assert!(!state.borrow().authority_serving());
            assert_eq!(state.borrow().counters.lease_self_fences, 1);
            assert_eq!(worlds[0].abort_reason(), Some("host session fenced"));
            host.cancel();
        });
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn failed_primary_advances_generation_on_a_quorum_before_recovery() {
        let blob = BlobDevConfig {
            read_latency_min: 1,
            read_latency_max: 1,
            write_latency_min: 1,
            write_latency_max: 1,
            ns_per_byte: 0,
            full_window: None,
            handoff_full_writes: 0,
            eio_at: None,
        };
        let store = StoreConfig {
            latency_min: 1,
            latency_max: 1,
            ns_per_byte: 0,
        };
        let worlds = SimWorld::cluster(3, blob, store);
        let placement = PlacementRecord::new(
            71,
            1,
            vec![VnodePlacement {
                vnode: VnodeId(0),
                members: [HostId(0), HostId(1), HostId(2)],
                next_members: None,
            }],
        )
        .expect("valid placement");
        let roster = [HostId(0), HostId(1), HostId(2)]
            .into_iter()
            .map(|host| PeerCandidate {
                host,
                weight: 1,
                failure_domain: host.0,
                drained: false,
            })
            .collect::<Vec<_>>();
        let states = (0..3)
            .map(|host| {
                Rc::new(RefCell::new(HostState::new(HostConfig {
                    archive: Default::default(),
                    host: HostId(host),
                    cache_pages: 8,
                    writeback_interval: 1_000,
                    backup_retry: 100,
                    disk_capacity: None,
                    disk_headroom: 0,
                    wedge_ticks: 0,
                    replica_placement: Some(ReplicaPlacementConfig {
                        membership_epoch: 1,
                        local_failure_domain: host,
                        roster: roster.clone(),
                        authority: Some(AuthorityHostConfig {
                            cluster_id: 71,
                            poll_interval: 10,
                            max_poll_staleness: 30,
                            challenge_interval: 40,
                        }),
                    }),
                })))
            })
            .collect::<Vec<_>>();
        let test_worlds = worlds.clone();
        run_on_turmoil_hosts(14, &worlds, async move {
            cas_placement(test_worlds[0].as_ref(), None, placement.clone())
                .await
                .expect("install placement");
            let mut hosts = states
                .iter()
                .zip(&test_worlds)
                .map(|(state, world)| {
                    spawn(host_actor_with_state(Rc::clone(state), Rc::clone(world)))
                })
                .collect::<Vec<_>>();
            delay(100).await;
            assert!(
                states
                    .iter()
                    .all(|state| state.borrow().authority_serving())
            );

            let old_record = read_host_session(test_worlds[0].as_ref(), HostId(0))
                .await
                .expect("read old session")
                .expect("old session exists")
                .record;
            let HostSessionRecord::Active {
                session: old_session,
                epoch: old_epoch,
                ..
            } = old_record
            else {
                panic!("old session must be active");
            };
            let old = VnodeAuthority {
                cluster_id: 71,
                placement_epoch: 1,
                vnode: VnodeId(0),
                generation: 1,
                primary: HostId(0),
                primary_session: old_session,
                primary_host_epoch: old_epoch,
            };
            let old_proof = claim_vnode_authority(&states[0], test_worlds[0].as_ref(), VnodeId(0))
                .await
                .expect("claim initial authority on a quorum");
            assert_eq!(old_proof.authority, old);
            adopt_vnode_generation(test_worlds[2].as_ref(), &placement, HostId(2), old_proof)
                .await
                .expect("adopt old generation");
            let bytes = b"quorum protected before failover".to_vec();
            commit_vnode_closure(
                test_worlds[0].as_ref(),
                &placement,
                old,
                VsetId(7),
                88,
                bytes.clone(),
            )
            .await
            .expect("commit old closure on primary");
            commit_vnode_closure(
                test_worlds[2].as_ref(),
                &placement,
                old,
                VsetId(7),
                88,
                bytes,
            )
            .await
            .expect("commit old closure on quorum member");

            hosts[0].cancel();
            yield_now().await;
            let (proof, inventory) = failover_vnode(
                &states[1],
                test_worlds[1].as_ref(),
                VnodeId(0),
                HostId(0),
                91,
            )
            .await
            .expect("fail over to host one");
            assert_eq!(proof.authority.generation, 2);
            assert_eq!(proof.authority.primary, HostId(1));
            assert_eq!(inventory[&VsetId(7)].sequence, 88);
            assert_eq!(
                read_vnode_closure(test_worlds[1].as_ref(), VnodeId(0), inventory[&VsetId(7)],)
                    .await
                    .expect("read protected closure"),
                b"quorum protected before failover"
            );
            let store_before = test_worlds[1].store_metrics();
            let committed = commit_active_vnode_quorum(
                &states[1],
                test_worlds[1].as_ref(),
                VsetId(7),
                89,
                b"new primary hot-path closure".to_vec(),
            )
            .await
            .expect("commit on new two-member quorum");
            let store_after = test_worlds[1].store_metrics();
            assert_eq!(store_after.put_attempts, store_before.put_attempts);
            assert_eq!(
                read_vnode_closure(test_worlds[2].as_ref(), VnodeId(0), committed)
                    .await
                    .expect("read new protected closure"),
                b"new primary hot-path closure"
            );
            assert_eq!(
                commit_vnode_closure(
                    test_worlds[1].as_ref(),
                    &placement,
                    old,
                    VsetId(7),
                    90,
                    b"stale".to_vec(),
                )
                .await,
                Err(AuthorityError::Fenced)
            );
            for host in &mut hosts[1..] {
                host.cancel();
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn unservable_page_failure_matches_the_production_fatal_signal() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let page = PageId {
            volume: blockd_core::types::VolumeId {
                vset: VsetId(1),
                idx: blockd_core::types::VolumeIdx(0),
            },
            page: blockd_core::types::PageNo(3),
        };
        let failed = simulate!(8, {
            let world = Rc::clone(&world);
            async move { GuestMem::fail(world.as_ref(), page).await }
        });
        assert_eq!(failed, Err(GuestMemoryError::Unservable));
        assert!(world.memory.borrow().failed.contains(&page));
    }

    #[tokio::test(start_paused = true)]
    async fn actor_world_runs_creation_through_the_real_async_host() {
        let host = HostId(1);
        let passive_host = HostId(2);
        let placement = |local: HostId| ReplicaPlacementConfig {
            membership_epoch: 1,
            local_failure_domain: local.0,
            roster: [host, passive_host]
                .into_iter()
                .map(|candidate| PeerCandidate {
                    host: candidate,
                    weight: 1,
                    failure_domain: candidate.0,
                    drained: false,
                })
                .collect(),
            authority: None,
        };
        let config = HostConfig {
            archive: Default::default(),
            host,
            cache_pages: 8,
            writeback_interval: 10,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(placement(host)),
        };
        let blob_config = BlobDevConfig {
            read_latency_min: 1,
            read_latency_max: 1,
            write_latency_min: 1,
            write_latency_max: 1,
            ns_per_byte: 0,
            full_window: None,
            handoff_full_writes: 0,
            eio_at: None,
        };
        let store_config = StoreConfig {
            latency_min: 1,
            latency_max: 1,
            ns_per_byte: 0,
        };
        let [world, passive_world] =
            SimWorld::pair([host, passive_host], blob_config, store_config);
        let state = Rc::new(RefCell::new(HostState::new(config)));
        let passive_state = Rc::new(RefCell::new(HostState::new(HostConfig {
            archive: Default::default(),
            host: passive_host,
            cache_pages: 8,
            writeback_interval: 10,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: Some(placement(passive_host)),
        })));
        let reply = world.request_admin(AdminCall::CreateVset {
            vset: VsetId(2),
            config: VsetConfig::compute(1, 4),
            from_base: None,
        });
        simulate!(4, async move {
            let passive = spawn(host_actor_with_state(passive_state, passive_world));
            let actor = spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
            let reply = reply.await.ok();
            assert_eq!(
                reply,
                Some(Ok(AdminSuccess::VsetCreated { vset: VsetId(2) }))
            );
            assert!(state.borrow().vsets[&VsetId(2)].ready);
            drop(actor);
            drop(passive);
            blockd_exec::run_ready().await;
        });
    }

    #[tokio::test(start_paused = true)]
    async fn paused_guests_do_not_emit_faults_until_resume() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let page = PageId {
            volume: blockd_core::types::VolumeId {
                vset: VsetId(1),
                idx: blockd_core::types::VolumeIdx(0),
            },
            page: blockd_core::types::PageNo(3),
        };
        let seen_at = Rc::new(Cell::new(None));
        world.set_vmstate(VsetId(1), 0);
        simulate!(9, async move {
            let pause = GuestMem::pause(world.as_ref(), VsetId(1)).await;
            assert_eq!(pause.as_ref().map(|pause| pause.vmstate), Ok(0));
            spawn({
                let world = Rc::clone(&world);
                let seen_at = Rc::clone(&seen_at);
                async move {
                    let fault = GuestMem::next_fault(world.as_ref()).await.expect("fault");
                    seen_at.set(Some(now()));
                    GuestMem::fill(
                        world.as_ref(),
                        fault.page,
                        vec![0; blockd_core::types::page_size()],
                        fault.write,
                        FillSource::Zero,
                    )
                    .await
                    .expect("simulated fill succeeds");
                }
            })
            .detach();
            let client = spawn({
                let world = Rc::clone(&world);
                async move { world.fault(page, false).await }
            });
            spawn({
                let world = Rc::clone(&world);
                async move {
                    delay(10).await;
                    GuestMem::resume(world.as_ref(), VsetId(1), pause.ok())
                        .await
                        .expect("simulated resume succeeds");
                }
            })
            .detach();
            assert_eq!(client.await, Ok(true));
            assert_eq!(seen_at.get(), Some(10));
        });
    }

    #[tokio::test(start_paused = true)]
    async fn stale_resume_cannot_release_a_newer_pause() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let vset = VsetId(1);
        world.set_vmstate(vset, 9);
        simulate!(11, async move {
            let first = GuestMem::pause(world.as_ref(), vset)
                .await
                .expect("first pause");
            let second = GuestMem::pause(world.as_ref(), vset)
                .await
                .expect("second pause");
            GuestMem::resume(world.as_ref(), vset, Some(first))
                .await
                .expect("stale resume is harmless");
            assert!(world.memory.borrow().paused.contains(&vset));
            GuestMem::resume(world.as_ref(), vset, Some(second))
                .await
                .expect("matching resume succeeds");
            assert!(!world.memory.borrow().paused.contains(&vset));
        });
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_pause_waits_for_an_inflight_guest_operation() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let vset = VsetId(1);
        world.memory.borrow_mut().in_ops.insert(vset);
        let paused_at = Rc::new(Cell::new(None));
        simulate!(10, async move {
            let pause = spawn({
                let world = Rc::clone(&world);
                let paused_at = Rc::clone(&paused_at);
                async move {
                    let vmstate = GuestMem::pause(world.as_ref(), vset).await;
                    paused_at.set(Some(now()));
                    vmstate
                }
            });
            spawn({
                let world = Rc::clone(&world);
                async move {
                    delay(10).await;
                    world.set_vmstate(vset, 7);
                }
            })
            .detach();
            assert_eq!(
                pause.await.map(|result| result.map(|pause| pause.vmstate)),
                Ok(Ok(7))
            );
            assert_eq!(paused_at.get(), Some(10));
        });
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_pending_pause_clears_its_bookkeeping() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let vset = VsetId(1);
        world.memory.borrow_mut().in_ops.insert(vset);
        simulate!(12, async move {
            let pause = spawn({
                let world = Rc::clone(&world);
                async move { GuestMem::pause(world.as_ref(), vset).await }
            });
            blockd_exec::run_ready().await;
            assert!(world.pause_started.borrow().contains_key(&vset));
            drop(pause);
            blockd_exec::run_ready().await;
            assert!(!world.pause_started.borrow().contains_key(&vset));
            assert!(!world.memory.borrow().paused.contains(&vset));
        });
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_pending_fault_releases_the_guest_operation() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let vset = VsetId(1);
        let page = PageId {
            volume: blockd_core::types::VolumeId {
                vset,
                idx: blockd_core::types::VolumeIdx(1),
            },
            page: blockd_core::types::PageNo(3),
        };
        simulate!(14, async move {
            let fault = spawn({
                let world = Rc::clone(&world);
                async move { world.fault(page, false).await }
            });
            blockd_exec::run_ready().await;
            assert!(world.memory.borrow().in_ops.contains(&vset));
            drop(fault);
            blockd_exec::run_ready().await;
            assert!(!world.memory.borrow().in_ops.contains(&vset));
        });
    }

    #[tokio::test(start_paused = true)]
    async fn advancing_incarnation_invalidates_active_recovery_generations() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let vset = VsetId(1);
        let (_, generation) = simulate!(13, {
            let world = Rc::clone(&world);
            async move {
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;
                world
                    .next_admin_event_with_generation()
                    .await
                    .expect("recovery event")
            }
        });

        world.advance_incarnation();

        assert_ne!(world.admin_event_generation(vset), generation);
    }

    #[test]
    fn crash_discards_stale_guest_events() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let page = PageId {
            volume: blockd_core::types::VolumeId {
                vset: VsetId(1),
                idx: blockd_core::types::VolumeIdx(1),
            },
            page: blockd_core::types::PageNo(3),
        };
        assert!(world.faults.send(GuestFault {
            page,
            write: false,
            wp: false,
            minor: false,
        }));
        let (sync, _reply) = request(GuestSync {
            req: ReqId(7),
            volume: page.volume,
        });
        assert!(world.syncs.send(sync));

        world.crash_guest_io();

        assert!(world.faults.try_recv().is_none());
        assert!(world.syncs.try_recv().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn scheduled_record_rot_targets_the_newest_requested_copy() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let older =
            blockd_core::layout::journal_blob(VsetId(1), 2, blockd_core::types::JournalSeq(3));
        let newer =
            blockd_core::layout::journal_blob(VsetId(1), 2, blockd_core::types::JournalSeq(4));
        world
            .blobs
            .borrow_mut()
            .durable
            .insert(older.clone(), vec![0; 8]);
        world
            .blobs
            .borrow_mut()
            .durable
            .insert(newer.clone(), vec![0; 8]);

        let task_world = Rc::clone(&world);
        let (flipped, older_bytes, newer_bytes) = simulate!(19, async move {
            let flipped = task_world.bitflip_record(Some(false));
            let blobs = task_world.blobs.borrow();
            (
                flipped,
                blobs.durable[&older].clone(),
                blobs.durable[&newer].clone(),
            )
        });
        assert!(flipped);
        assert_eq!(older_bytes, vec![0; 8]);
        assert_ne!(newer_bytes, vec![0; 8]);
    }

    #[tokio::test(start_paused = true)]
    async fn write_metrics_include_blobs_deleted_before_the_report() {
        let world = SimWorld::new(HostId(1), BlobDevConfig::nvme(), StoreConfig::gcs());
        let name =
            blockd_core::layout::journal_blob(VsetId(1), 1, blockd_core::types::JournalSeq(0));
        let task_world = Rc::clone(&world);
        simulate!(21, async move {
            Blobs::write(task_world.as_ref(), name.clone(), vec![1; 32])
                .await
                .expect("write record");
            Blobs::delete(task_world.as_ref(), &name)
                .await
                .expect("delete record");
        });
        assert_eq!(world.write_metrics(), (32, 32));
        assert_eq!(world.blob_count(), 0);
    }
}
