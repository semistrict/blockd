//! Executor-owned simulation implementations of the async core world.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use async_trait::async_trait;
use blockd_core::database::{DatabaseReply, DatabaseRequest};
use blockd_core::protocol::{AdminCmd, AdminReply, PeerMsg, ReqId, StoreFault};
use blockd_core::types::{HostId, PageId, VsetId};
use blockd_core::world::{
    AdminIo, BlobEntry, BlobError, Blobs, FillSource, GuestFault, GuestMem, GuestSync, Peers,
    Store, StoreError,
};
use blockd_exec::channel::{OneSender, Receiver, UnboundedSender, oneshot, unbounded};
use blockd_exec::{current_poll, delay, now, random_u64, spawn};

use crate::world::blobdev::{BlobDevConfig, CrashFate};
use crate::world::store::{MAX_OBJECT_BYTES, StoreConfig};

fn random_between(low: u64, high: u64) -> u64 {
    assert!(low <= high);
    low + random_u64() % (high - low + 1)
}

struct ReceiverLease<'a, T> {
    slot: &'a RefCell<Option<Receiver<T>>>,
    receiver: Option<Receiver<T>>,
}

impl<T> ReceiverLease<'_, T> {
    async fn recv(&mut self) -> Option<T> {
        self.receiver.as_mut()?.recv().await
    }
}

impl<T> Drop for ReceiverLease<'_, T> {
    fn drop(&mut self) {
        let previous = self.slot.borrow_mut().replace(
            self.receiver
                .take()
                .expect("receiver lease always owns the receiver"),
        );
        assert!(
            previous.is_none(),
            "receiver returned into an occupied slot"
        );
    }
}

struct Stream<T> {
    sender: UnboundedSender<T>,
    receiver: RefCell<Option<Receiver<T>>>,
}

impl<T> Stream<T> {
    fn new() -> Self {
        let (sender, receiver) = unbounded();
        Self {
            sender,
            receiver: RefCell::new(Some(receiver)),
        }
    }

    fn send(&self, value: T) -> bool {
        self.sender.send(value).is_ok()
    }

    fn lease(&self) -> ReceiverLease<'_, T> {
        ReceiverLease {
            slot: &self.receiver,
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
}

#[derive(Default)]
struct StoreState {
    objects: BTreeMap<String, (u64, Vec<u8>)>,
    next_version: u64,
    outage: bool,
    unavailable: u64,
    cas_conflicts: u64,
}

#[derive(Default)]
struct MemoryState {
    pages: BTreeMap<PageId, Vec<u8>>,
    shared_pages: BTreeMap<(u64, u64, blockd_core::types::SegId, u32), Vec<u8>>,
    protected: BTreeSet<PageId>,
    accessed: BTreeSet<PageId>,
    paused: BTreeSet<VsetId>,
    vmstate: BTreeMap<VsetId, u64>,
    failed: BTreeSet<PageId>,
}

type PeerInbox = UnboundedSender<(HostId, PeerMsg)>;

#[derive(Default)]
pub(crate) struct SimNetwork {
    inboxes: RefCell<BTreeMap<HostId, PeerInbox>>,
    blocked: RefCell<BTreeSet<(HostId, HostId)>>,
    down: RefCell<BTreeSet<HostId>>,
    outages: RefCell<Vec<(u64, u64, HostId, HostId)>>,
    targeted_drop: Cell<Option<(u8, u64, u64)>>,
    drop_odds: Cell<(u64, u64)>,
    dup_odds: Cell<(u64, u64)>,
    drops: Cell<u64>,
    dups: Cell<u64>,
    clogs: Cell<u64>,
    targeted_drops: Cell<u64>,
    delivered: RefCell<BTreeMap<u8, u64>>,
    latency: Cell<(u64, u64)>,
}

impl SimNetwork {
    pub(crate) fn set_latency(&self, low: u64, high: u64) {
        self.latency.set((low, high));
    }

    pub(crate) fn configure_faults(
        &self,
        drop_odds: (u64, u64),
        dup_odds: (u64, u64),
        outages: Vec<(u64, u64, HostId, HostId)>,
        targeted_drop: Option<(u8, u64, u64)>,
    ) {
        self.drop_odds.set(drop_odds);
        self.dup_odds.set(dup_odds);
        *self.outages.borrow_mut() = outages;
        self.targeted_drop.set(targeted_drop);
    }

    pub(crate) fn set_host_down(&self, host: HostId, down: bool) {
        if down {
            self.down.borrow_mut().insert(host);
        } else {
            self.down.borrow_mut().remove(&host);
        }
    }

    pub(crate) fn counters(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.drops.get(),
            self.dups.get(),
            self.clogs.get(),
            self.targeted_drops.get(),
            self.delivered.borrow().get(&6).copied().unwrap_or(0),
        )
    }

    fn unavailable(&self, from: HostId, to: HostId, at: u64) -> bool {
        self.down.borrow().contains(&from)
            || self.down.borrow().contains(&to)
            || self.blocked.borrow().contains(&(from, to))
            || self
                .outages
                .borrow()
                .iter()
                .any(|&(begin, end, source, dest)| {
                    source == from && dest == to && (begin..end).contains(&at)
                })
    }
}

pub(crate) struct SimWorld {
    host: HostId,
    blob_config: BlobDevConfig,
    store_config: StoreConfig,
    blobs: RefCell<BlobState>,
    store: Rc<RefCell<StoreState>>,
    memory: RefCell<MemoryState>,
    admin: Stream<AdminCmd>,
    admin_reply_events: Stream<AdminReply>,
    database: Stream<DatabaseRequest>,
    database_replies: RefCell<Vec<DatabaseReply>>,
    faults: Stream<GuestFault>,
    syncs: Stream<GuestSync>,
    peers: Stream<(HostId, PeerMsg)>,
    aborts: Stream<&'static str>,
    network: Rc<SimNetwork>,
    fault_waiters: RefCell<BTreeMap<PageId, Vec<OneSender<bool>>>>,
    sync_waiters: RefCell<BTreeMap<ReqId, OneSender<bool>>>,
    aborted: Cell<bool>,
    abort_reason: RefCell<Option<&'static str>>,
    corrupt_fills: Cell<bool>,
    drop_write_protect: Cell<bool>,
    drop_handoff_writes: Cell<bool>,
    page_read_poll: Cell<Option<u64>>,
    page_reads_in_poll: Cell<u64>,
    max_page_reads_in_poll: Cell<u64>,
    pause_started: RefCell<BTreeMap<VsetId, u64>>,
    max_pause_ns: Cell<u64>,
}

impl SimWorld {
    pub(crate) fn new(
        host: HostId,
        blob_config: BlobDevConfig,
        store_config: StoreConfig,
        network: &Rc<SimNetwork>,
    ) -> Rc<Self> {
        Self::with_store(
            host,
            blob_config,
            store_config,
            network,
            Rc::new(RefCell::new(StoreState::default())),
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
    ) -> (Rc<SimNetwork>, Vec<Rc<Self>>) {
        let network = Rc::new(SimNetwork::default());
        let store = Rc::new(RefCell::new(StoreState::default()));
        let worlds = (0..hosts)
            .map(|host| {
                Self::with_store(
                    HostId(host),
                    blob_config,
                    store_config,
                    &network,
                    Rc::clone(&store),
                )
            })
            .collect();
        (network, worlds)
    }

    fn with_store(
        host: HostId,
        blob_config: BlobDevConfig,
        store_config: StoreConfig,
        network: &Rc<SimNetwork>,
        store: Rc<RefCell<StoreState>>,
    ) -> Rc<Self> {
        let world = Rc::new(Self {
            host,
            blob_config,
            store_config,
            blobs: RefCell::new(BlobState::default()),
            store,
            memory: RefCell::new(MemoryState::default()),
            admin: Stream::new(),
            admin_reply_events: Stream::new(),
            database: Stream::new(),
            database_replies: RefCell::new(Vec::new()),
            faults: Stream::new(),
            syncs: Stream::new(),
            peers: Stream::new(),
            aborts: Stream::new(),
            network: Rc::clone(network),
            fault_waiters: RefCell::new(BTreeMap::new()),
            sync_waiters: RefCell::new(BTreeMap::new()),
            aborted: Cell::new(false),
            abort_reason: RefCell::new(None),
            corrupt_fills: Cell::new(false),
            drop_write_protect: Cell::new(false),
            drop_handoff_writes: Cell::new(false),
            page_read_poll: Cell::new(None),
            page_reads_in_poll: Cell::new(0),
            max_page_reads_in_poll: Cell::new(0),
            pause_started: RefCell::new(BTreeMap::new()),
            max_pause_ns: Cell::new(0),
        });
        network
            .inboxes
            .borrow_mut()
            .insert(host, world.peers.sender.clone());
        world
    }

    pub(crate) fn enqueue_admin(&self, command: AdminCmd) {
        assert!(self.admin.send(command), "admin actor is alive");
    }

    pub(crate) async fn next_admin_reply(&self) -> Option<AdminReply> {
        self.admin_reply_events.recv().await
    }

    pub(crate) fn try_next_admin_reply(&self) -> Option<AdminReply> {
        self.admin_reply_events.try_recv()
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
        (store.unavailable, store.cas_conflicts)
    }

    pub(crate) fn store_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.store
            .borrow()
            .objects
            .get(key)
            .map(|(_, bytes)| bytes.clone())
    }

    pub(crate) fn blob_count(&self) -> usize {
        self.blobs.borrow().durable.len()
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

    pub(crate) fn rot_store_leaf(&self) -> bool {
        let keys = self
            .store
            .borrow()
            .objects
            .iter()
            .filter_map(|(key, (_, bytes))| {
                (!bytes.is_empty()
                    && matches!(
                        blockd_core::layout::parse_key(key),
                        Some(
                            blockd_core::layout::StoreKey::Leaf { .. }
                                | blockd_core::layout::StoreKey::BaseLeaf { .. }
                        )
                    ))
                .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return false;
        }
        let key = &keys[usize::try_from(random_u64() % keys.len() as u64).expect("index fits")];
        self.store
            .borrow_mut()
            .objects
            .get_mut(key)
            .expect("selected leaf exists")
            .1[0] ^= 1;
        true
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
        for waiter in fault_waiters.into_values().flatten() {
            let _ = waiter.send(false);
        }
        let sync_waiters = std::mem::take(&mut *self.sync_waiters.borrow_mut());
        for waiter in sync_waiters.into_values() {
            let _ = waiter.send(false);
        }
        *self.memory.borrow_mut() = MemoryState::default();
    }

    pub(crate) fn bitflip_segment(&self) -> bool {
        self.bitflip_local(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("seg"))
        })
    }

    pub(crate) fn bitflip_record(&self, mirror: Option<bool>) -> bool {
        self.bitflip_local(|name| {
            let extension = std::path::Path::new(name)
                .extension()
                .and_then(|extension| extension.to_str());
            match mirror {
                Some(false) => extension == Some("rec"),
                Some(true) => extension == Some("recm"),
                None => matches!(extension, Some("rec" | "recm")),
            }
        })
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

    pub(crate) fn set_vmstate(&self, vset: VsetId, vmstate: u64) {
        self.memory.borrow_mut().vmstate.insert(vset, vmstate);
    }

    pub(crate) fn is_paused(&self, vset: VsetId) -> bool {
        self.memory.borrow().paused.contains(&vset)
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

    pub(crate) fn write_fault_mutates(&self, page: PageId) -> bool {
        let memory = self.memory.borrow();
        !memory.pages.contains_key(&page) || memory.protected.contains(&page)
    }

    pub(crate) async fn fault(&self, page: PageId, write: bool) -> bool {
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
            if !paused && !self.faults.send(GuestFault { page, write }) {
                return false;
            }
            if receive.await != Ok(true) {
                return false;
            }
        }
    }

    pub(crate) async fn sync(&self, sync: GuestSync) -> bool {
        let (send, receive) = oneshot();
        self.sync_waiters.borrow_mut().insert(sync.req, send);
        if !self.syncs.send(sync) {
            self.sync_waiters.borrow_mut().remove(&sync.req);
            return false;
        }
        receive.await.unwrap_or(false)
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
}

#[async_trait(?Send)]
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
        if self.drop_handoff_writes.get() && name.ends_with("/handoff") {
            delay(latency).await;
            return Ok(());
        }
        let io = self.submit_blob(name, bytes, BlobWriteKind::New);
        self.finish_blob(io, latency).await
    }

    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        let latency = self.blob_latency(bytes.len(), true);
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

#[async_trait(?Send)]
impl Store for SimWorld {
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
        if bytes.len() > MAX_OBJECT_BYTES {
            return Err(StoreError::TooLarge);
        }
        let result = {
            let mut store = self.store.borrow_mut();
            if store.outage {
                store.unavailable = store.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                store.next_version = store.next_version.saturating_add(1);
                let version = store.next_version;
                store.objects.insert(key, (version, bytes.clone()));
                Ok(version)
            }
        };
        delay(self.store_latency(bytes.len())).await;
        result
    }

    async fn put_cas(
        &self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreError> {
        if bytes.len() > MAX_OBJECT_BYTES {
            return Err(StoreError::TooLarge);
        }
        let result = {
            let mut store = self.store.borrow_mut();
            if store.outage {
                store.unavailable = store.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                let actual = store.objects.get(&key).map(|(version, _)| *version);
                if actual == expected {
                    store.next_version = store.next_version.saturating_add(1);
                    let version = store.next_version;
                    store.objects.insert(key, (version, bytes.clone()));
                    Ok(version)
                } else {
                    store.cas_conflicts = store.cas_conflicts.saturating_add(1);
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
                store.unavailable = store.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                Ok(store.objects.get(key).cloned())
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
                store.unavailable = store.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
                Ok(store.objects.get(key).map(|(version, bytes)| {
                    let start =
                        usize::try_from(offset.min(bytes.len() as u64)).expect("offset fits");
                    let end = usize::try_from(offset.saturating_add(len).min(bytes.len() as u64))
                        .expect("end fits");
                    (*version, bytes[start..end].to_vec())
                }))
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
                store.unavailable = store.unavailable.saturating_add(1);
                Err(StoreError::Fault(StoreFault::Unavailable))
            } else {
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
                store.unavailable = store.unavailable.saturating_add(1);
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

#[async_trait(?Send)]
impl Peers for SimWorld {
    async fn send(&self, to: HostId, message: PeerMsg) {
        let from = self.host;
        let network = Rc::clone(&self.network);
        let sent_at = now();
        if network
            .targeted_drop
            .get()
            .is_some_and(|(kind, begin, end)| {
                kind == peer_tag(&message) && (begin..end).contains(&sent_at)
            })
        {
            network
                .targeted_drops
                .set(network.targeted_drops.get().saturating_add(1));
            return;
        }
        if network
            .outages
            .borrow()
            .iter()
            .any(|&(begin, end, source, dest)| {
                source == from && dest == to && (begin..end).contains(&sent_at)
            })
        {
            network.clogs.set(network.clogs.get().saturating_add(1));
            return;
        }
        if network.down.borrow().contains(&from)
            || network.down.borrow().contains(&to)
            || network.blocked.borrow().contains(&(from, to))
            || odds(network.drop_odds.get())
        {
            network.drops.set(network.drops.get().saturating_add(1));
            return;
        }
        let copies = if odds(network.dup_odds.get()) {
            network.dups.set(network.dups.get().saturating_add(1));
            2
        } else {
            1
        };
        let (low, high) = network.latency.get();
        for _ in 0..copies {
            let network = Rc::clone(&network);
            let message = message.clone();
            spawn(async move {
                if high != 0 {
                    delay(random_between(low, high)).await;
                }
                if network.unavailable(from, to, now()) {
                    network.drops.set(network.drops.get().saturating_add(1));
                    return;
                }
                if let Some(inbox) = network.inboxes.borrow().get(&to) {
                    let mut delivered = network.delivered.borrow_mut();
                    *delivered.entry(peer_tag(&message)).or_default() += 1;
                    drop(delivered);
                    let _ = inbox.send((from, message));
                }
            })
            .detach();
        }
    }

    async fn recv(&self) -> Option<(HostId, PeerMsg)> {
        self.peers.recv().await
    }
}

fn odds((numerator, denominator): (u64, u64)) -> bool {
    numerator != 0 && denominator != 0 && random_u64() % denominator < numerator
}

fn peer_tag(message: &PeerMsg) -> u8 {
    match message {
        PeerMsg::MigrateOffer { .. } => 0,
        PeerMsg::MigrateAccept { .. } => 1,
        PeerMsg::FetchRange { .. } => 2,
        PeerMsg::Page { .. } => 3,
        PeerMsg::FetchLeaf { .. } => 4,
        PeerMsg::Leaf { .. } => 5,
        PeerMsg::Released { .. } => 6,
        PeerMsg::ReleasedAck { .. } => 7,
        PeerMsg::ReplicaPut { .. } => 8,
        PeerMsg::ReplicaPutAck { .. } => 9,
        PeerMsg::ReplicaCommit { .. } => 10,
        PeerMsg::ReplicaCommitAck { .. } => 11,
        PeerMsg::ReplicaStatus { .. } => 12,
        PeerMsg::ReplicaStatusReply { .. } => 13,
        PeerMsg::ReplicaUploadDone { .. } => 14,
        PeerMsg::ReplicaRelease { .. } => 15,
        PeerMsg::ReplicaReleaseAck { .. } => 16,
    }
}

#[async_trait(?Send)]
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
        self.memory
            .borrow()
            .pages
            .get(&page)
            .cloned()
            .unwrap_or_else(|| vec![0; blockd_core::types::page_size()])
    }

    async fn arm_write_protect(&self, pages: &[PageId]) {
        if self.drop_write_protect.get() {
            return;
        }
        self.memory.borrow_mut().protected.extend(pages);
    }

    async fn fill(&self, page: PageId, mut bytes: Vec<u8>, writable: bool, _source: FillSource) {
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
        self.wake_fault(page, true);
    }

    async fn fill_shared(
        &self,
        page: PageId,
        share: (u64, u64, blockd_core::types::SegId, u32),
        bytes: Option<Vec<u8>>,
        writable: bool,
    ) {
        if let Some(bytes) = bytes {
            self.memory.borrow_mut().shared_pages.insert(share, bytes);
        }
        let bytes = self.memory.borrow().shared_pages[&share].clone();
        self.fill(page, bytes, writable, FillSource::Store).await;
    }

    async fn fail(&self, page: PageId) {
        self.memory.borrow_mut().failed.insert(page);
        self.wake_fault(page, false);
    }

    async fn unprotect(&self, page: PageId) {
        self.memory.borrow_mut().protected.remove(&page);
        self.wake_fault(page, true);
    }

    async fn evict(&self, page: PageId) {
        let mut memory = self.memory.borrow_mut();
        memory.pages.remove(&page);
        memory.protected.remove(&page);
    }

    async fn install_database(&self, page: PageId, bytes: Vec<u8>) {
        self.memory.borrow_mut().pages.insert(page, bytes);
    }

    async fn pause(&self, vset: VsetId) -> u64 {
        self.pause_started.borrow_mut().entry(vset).or_insert(now());
        let mut memory = self.memory.borrow_mut();
        memory.paused.insert(vset);
        memory.vmstate.get(&vset).copied().unwrap_or(0)
    }

    async fn resume(&self, vset: VsetId) {
        if let Some(started) = self.pause_started.borrow_mut().remove(&vset) {
            self.max_pause_ns
                .set(self.max_pause_ns.get().max(now().saturating_sub(started)));
        }
        self.memory.borrow_mut().paused.remove(&vset);
        let pages = self
            .fault_waiters
            .borrow()
            .keys()
            .filter(|page| page.volume.vset == vset)
            .copied()
            .collect::<Vec<_>>();
        for page in pages {
            self.wake_fault(page, true);
        }
    }

    async fn harvest_accessed(&self) -> Vec<PageId> {
        std::mem::take(&mut self.memory.borrow_mut().accessed)
            .into_iter()
            .collect()
    }

    async fn next_fault(&self) -> Option<GuestFault> {
        self.faults.recv().await
    }

    async fn next_sync(&self) -> Option<GuestSync> {
        self.syncs.recv().await
    }

    async fn sync_ok(&self, req: ReqId) {
        if let Some(waiter) = self.sync_waiters.borrow_mut().remove(&req) {
            let _ = waiter.send(true);
        }
    }

    async fn sync_failed(&self, req: ReqId) {
        if let Some(waiter) = self.sync_waiters.borrow_mut().remove(&req) {
            let _ = waiter.send(false);
        }
    }

    async fn fence(&self, vset: VsetId) {
        let pages = self
            .fault_waiters
            .borrow()
            .keys()
            .filter(|page| page.volume.vset == vset)
            .copied()
            .collect::<Vec<_>>();
        for page in pages {
            self.wake_fault(page, false);
        }
    }
}

#[async_trait(?Send)]
impl AdminIo for SimWorld {
    async fn next_admin(&self) -> Option<AdminCmd> {
        self.admin.recv().await
    }

    async fn reply_admin(&self, reply: AdminReply) {
        let _ = self.admin_reply_events.send(reply);
    }

    async fn next_database(&self) -> Option<DatabaseRequest> {
        self.database.recv().await
    }

    async fn reply_database(&self, reply: DatabaseReply) {
        self.database_replies.borrow_mut().push(reply);
    }

    async fn abort(&self, reason: &'static str) {
        self.aborted.set(true);
        self.abort_reason.borrow_mut().replace(reason);
        let _ = self.aborts.send(reason);
    }
}

#[cfg(test)]
mod tests {
    use blockd_core::engine::{HostState, host_actor_with_state};
    use blockd_core::hostmeta::HostConfig;
    use blockd_core::journal::VsetConfig;
    use blockd_core::protocol::AdminReply;
    use blockd_core::types::VsetId;
    use blockd_exec::Executor;

    use super::*;

    #[test]
    fn actor_world_runs_creation_through_the_real_async_host() {
        let host = HostId(1);
        let config = HostConfig {
            host,
            cache_pages: 8,
            writeback_interval: 10,
            backup_retry: 5,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        };
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(
            host,
            BlobDevConfig {
                read_latency_min: 1,
                read_latency_max: 1,
                write_latency_min: 1,
                write_latency_max: 1,
                ns_per_byte: 0,
            },
            StoreConfig {
                latency_min: 1,
                latency_max: 1,
                ns_per_byte: 0,
            },
            &network,
        );
        let state = Rc::new(RefCell::new(HostState::new(config)));
        world.enqueue_admin(AdminCmd::CreateVset {
            req: ReqId(1),
            vset: VsetId(2),
            config: VsetConfig::compute(1, 4),
            from_base: None,
        });
        let mut executor = Executor::simulation(4);
        let actor = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
        let reply = executor.block_on({
            let world = Rc::clone(&world);
            async move { world.next_admin_reply().await }
        });
        assert_eq!(
            reply,
            Some(AdminReply::VsetCreated {
                req: ReqId(1),
                vset: VsetId(2)
            })
        );
        assert!(state.borrow().vsets[&VsetId(2)].ready);
        drop(actor);
        executor.run_ready();
    }

    #[test]
    fn paused_guests_do_not_emit_faults_until_resume() {
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(
            HostId(1),
            BlobDevConfig::nvme(),
            StoreConfig::s3(),
            &network,
        );
        let page = PageId {
            volume: blockd_core::types::VolumeId {
                vset: VsetId(1),
                idx: blockd_core::types::VolumeIdx(0),
            },
            page: blockd_core::types::PageNo(3),
        };
        let seen_at = Rc::new(Cell::new(None));
        let mut executor = Executor::simulation(9);
        executor.block_on({
            let world = Rc::clone(&world);
            async move { GuestMem::pause(world.as_ref(), VsetId(1)).await }
        });
        executor
            .spawn({
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
                    .await;
                }
            })
            .detach();
        let client = executor.spawn({
            let world = Rc::clone(&world);
            async move { world.fault(page, false).await }
        });
        executor
            .spawn({
                let world = Rc::clone(&world);
                async move {
                    delay(10).await;
                    GuestMem::resume(world.as_ref(), VsetId(1)).await;
                }
            })
            .detach();
        assert_eq!(executor.block_on(client), Ok(true));
        assert_eq!(seen_at.get(), Some(10));
    }
}
