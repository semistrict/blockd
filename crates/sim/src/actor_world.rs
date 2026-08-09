//! Executor-owned simulation implementations of the async core world.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use async_trait::async_trait;
use blockd_core::database::{DatabaseReply, DatabaseRequest};
use blockd_core::head::HeadRecord;
use blockd_core::journal::JournalRecord;
use blockd_core::layout;
use blockd_core::mapleaf::MapLeaf;
use blockd_core::protocol::{AdminCmd, AdminReply, PeerMsg, ReqId, StoreFault};
use blockd_core::types::{Gen, HostId, PageId, VolumeId, VsetId, page_size};
use blockd_core::world::{
    AdminIo, BlobEntry, BlobError, Blobs, FillSource, GuestFault, GuestMem, GuestSync, Peers,
    Store, StoreError,
};
use blockd_exec::channel::{OneSender, Receiver, UnboundedSender, oneshot, unbounded};
use blockd_exec::{current_poll, delay, now, random_u64, spawn};

use crate::world::blobdev::{BlobDevConfig, CrashFate};
use crate::world::store::{MAX_OBJECT_BYTES, StoreConfig, StoreCounters, StoreObjectKind};

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

    fn discard_pending(&self) -> usize {
        self.sender.discard_pending()
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
        } else if key.contains("/l/") || key.contains("/lb/") {
            StoreObjectKind::Leaf
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
        let Some((_, record_bytes)) =
            self.objects
                .get(&layout::manifest_key(vset, pointer.fence, pointer.seq))
        else {
            return;
        };
        let Ok(record) = JournalRecord::decode(vset, record_bytes) else {
            return;
        };
        let mut current = BTreeMap::new();
        for leaf_pointer in record.leaves.values() {
            let (owner, leaf_key) = if leaf_pointer.base == 0 {
                (
                    vset,
                    layout::leaf_key(vset, leaf_pointer.fence, leaf_pointer.id),
                )
            } else {
                (
                    VsetId(leaf_pointer.base),
                    layout::base_leaf_key(leaf_pointer.base, leaf_pointer.fence, leaf_pointer.id),
                )
            };
            let Some((_, leaf_bytes)) = self.objects.get(&leaf_key) else {
                continue;
            };
            let Ok(leaf) = MapLeaf::decode(owner, leaf_pointer.fence, leaf_pointer.id, leaf_bytes)
            else {
                continue;
            };
            for (idx, page, generation, _) in leaf.entries {
                current.insert(
                    PageId {
                        volume: VolumeId { vset, idx },
                        page,
                    },
                    generation,
                );
            }
        }
        current.extend(
            record
                .overlay
                .iter()
                .map(|(&page, &(generation, _))| (page, generation)),
        );
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

type PeerInbox = UnboundedSender<(HostId, PeerMsg)>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct OracleSnapshot {
    pub(crate) pages: BTreeMap<PageId, Vec<u8>>,
    pub(crate) unknown: BTreeSet<PageId>,
}

type OracleState = (
    Rc<RefCell<BTreeMap<PageId, Vec<u8>>>>,
    Rc<RefCell<BTreeSet<PageId>>>,
);

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
    pause_waiters: RefCell<BTreeMap<VsetId, Vec<OneSender<()>>>>,
    operation_waiters: RefCell<BTreeMap<VsetId, Vec<OneSender<()>>>>,
    oracle_pages: RefCell<BTreeMap<VsetId, OracleState>>,
    checkpoint_snapshots: RefCell<BTreeMap<(VsetId, u64), Vec<OracleSnapshot>>>,
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

    pub(crate) fn pair(
        hosts: [HostId; 2],
        blob_config: BlobDevConfig,
        store_config: StoreConfig,
    ) -> (Rc<SimNetwork>, [Rc<Self>; 2]) {
        let network = Rc::new(SimNetwork::default());
        let store = Rc::new(RefCell::new(StoreState::default()));
        let worlds = hosts.map(|host| {
            Self::with_store(host, blob_config, store_config, &network, Rc::clone(&store))
        });
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
            pause_waiters: RefCell::new(BTreeMap::new()),
            operation_waiters: RefCell::new(BTreeMap::new()),
            oracle_pages: RefCell::new(BTreeMap::new()),
            checkpoint_snapshots: RefCell::new(BTreeMap::new()),
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
        self.checkpoint_snapshots
            .borrow()
            .get(&(vset, vmstate))
            .cloned()
            .unwrap_or_default()
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
        (store.counters.unavailable, store.counters.cas_conflicts)
    }

    pub(crate) fn store_metrics(&self) -> StoreCounters {
        self.store.borrow().counters
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn published_archive_metrics(&self) -> (u64, u64, u64, u64) {
        let store = self.store.borrow();
        let mut segments = BTreeSet::new();
        let mut live_locations = BTreeSet::new();
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
            let Some((_, record_bytes)) =
                store
                    .objects
                    .get(&layout::manifest_key(vset, pointer.fence, pointer.seq))
            else {
                continue;
            };
            let Ok(record) = JournalRecord::decode(vset, record_bytes) else {
                continue;
            };
            for (_, location) in record.overlay.values() {
                segments.insert((vset, location.base, location.fence, location.seg));
                live_locations.insert((
                    vset,
                    location.base,
                    location.fence,
                    location.seg,
                    location.offset,
                    location.len,
                ));
            }
            for leaf_pointer in record.leaves.values() {
                let (owner, leaf_key) = if leaf_pointer.base == 0 {
                    (
                        vset,
                        layout::leaf_key(vset, leaf_pointer.fence, leaf_pointer.id),
                    )
                } else {
                    (
                        VsetId(leaf_pointer.base),
                        layout::base_leaf_key(
                            leaf_pointer.base,
                            leaf_pointer.fence,
                            leaf_pointer.id,
                        ),
                    )
                };
                let Some((_, leaf_bytes)) = store.objects.get(&leaf_key) else {
                    continue;
                };
                let Ok(leaf) =
                    MapLeaf::decode(owner, leaf_pointer.fence, leaf_pointer.id, leaf_bytes)
                else {
                    continue;
                };
                for (_, _, _, location) in leaf.entries {
                    segments.insert((vset, location.base, location.fence, location.seg));
                    live_locations.insert((
                        vset,
                        location.base,
                        location.fence,
                        location.seg,
                        location.offset,
                        location.len,
                    ));
                }
            }
        }

        let mut total = 0_u64;
        let mut live = 0_u64;
        let mut dead = 0_u64;
        for (vset, base, fence, segment) in segments {
            let key = if base == 0 {
                layout::segment_key(vset, fence, segment)
            } else {
                layout::base_segment_key(base, fence, segment)
            };
            let Some((_, bytes)) = store.objects.get(&key) else {
                continue;
            };
            let Ok((_, _, _, entries)) = blockd_core::segment::scan_segment(bytes) else {
                continue;
            };
            total = total.saturating_add(bytes.len() as u64);
            for (_, _, location) in entries {
                let size = u64::from(location.len);
                if live_locations.contains(&(
                    vset,
                    base,
                    fence,
                    segment,
                    location.offset,
                    location.len,
                )) {
                    live = live.saturating_add(size);
                } else {
                    dead = dead.saturating_add(size);
                }
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
        self.faults.discard_pending();
        self.syncs.discard_pending();
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

    pub(crate) fn bitflip_segment(&self) -> bool {
        self.bitflip_local(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("seg"))
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

    pub(crate) fn write_fault_mutates(&self, page: PageId) -> bool {
        let memory = self.memory.borrow();
        !memory.pages.contains_key(&page) || memory.protected.contains(&page)
    }

    pub(crate) async fn fault(&self, page: PageId, write: bool) -> bool {
        self.begin_guest_op(page.volume.vset).await;
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
        self.begin_guest_op(sync.volume.vset).await;
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
        if self.blobs.borrow().durable.get(&name) == Some(&bytes) {
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
        PeerMsg::ReplicaArchive { .. } => 15,
        PeerMsg::ReplicaRelease { .. } => 16,
        PeerMsg::ReplicaReleaseAck { .. } => 17,
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
                        let mut snapshots = self.checkpoint_snapshots.borrow_mut();
                        let candidates = snapshots.entry((vset, vmstate)).or_default();
                        if candidates.last() != Some(&snapshot) {
                            candidates.push(snapshot);
                        }
                    }
                    return vmstate;
                }
                Err(wait) => {
                    let _ = wait.await;
                }
            }
        }
    }

    async fn resume(&self, vset: VsetId) {
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
#[allow(clippy::default_trait_access)]
mod tests {
    use blockd_core::engine::{HostState, host_actor_with_state};
    use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
    use blockd_core::journal::VsetConfig;
    use blockd_core::placement::PeerCandidate;
    use blockd_core::protocol::AdminReply;
    use blockd_core::types::VsetId;
    use blockd_exec::Executor;

    use super::*;

    #[test]
    fn actor_world_runs_creation_through_the_real_async_host() {
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
        };
        let store_config = StoreConfig {
            latency_min: 1,
            latency_max: 1,
            ns_per_byte: 0,
        };
        let (_, [world, passive_world]) =
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
        world.enqueue_admin(AdminCmd::CreateVset {
            req: ReqId(1),
            vset: VsetId(2),
            config: VsetConfig::compute(1, 4),
            from_base: None,
        });
        let mut executor = Executor::simulation(4);
        let passive = executor.spawn(host_actor_with_state(passive_state, passive_world));
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
        drop(passive);
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
        world.set_vmstate(VsetId(1), 0);
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

    #[test]
    fn checkpoint_pause_waits_for_an_inflight_guest_operation() {
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(
            HostId(1),
            BlobDevConfig::nvme(),
            StoreConfig::s3(),
            &network,
        );
        let vset = VsetId(1);
        world.memory.borrow_mut().in_ops.insert(vset);
        let mut executor = Executor::simulation(10);
        let paused_at = Rc::new(Cell::new(None));
        let pause = executor.spawn({
            let world = Rc::clone(&world);
            let paused_at = Rc::clone(&paused_at);
            async move {
                let vmstate = GuestMem::pause(world.as_ref(), vset).await;
                paused_at.set(Some(now()));
                vmstate
            }
        });
        executor
            .spawn({
                let world = Rc::clone(&world);
                async move {
                    delay(10).await;
                    world.set_vmstate(vset, 7);
                }
            })
            .detach();
        assert_eq!(executor.block_on(pause), Ok(7));
        assert_eq!(paused_at.get(), Some(10));
    }

    #[test]
    fn crash_discards_stale_guest_events() {
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
                idx: blockd_core::types::VolumeIdx(1),
            },
            page: blockd_core::types::PageNo(3),
        };
        assert!(world.faults.send(GuestFault { page, write: false }));
        assert!(world.syncs.send(GuestSync {
            req: ReqId(7),
            volume: page.volume,
        }));

        world.crash_guest_io();

        assert!(world.faults.try_recv().is_none());
        assert!(world.syncs.try_recv().is_none());
    }

    #[test]
    fn scheduled_record_rot_targets_the_newest_requested_copy() {
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(
            HostId(1),
            BlobDevConfig::nvme(),
            StoreConfig::s3(),
            &network,
        );
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

        let mut executor = Executor::simulation(19);
        let task_world = Rc::clone(&world);
        let (flipped, older_bytes, newer_bytes) = executor.block_on(async move {
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

    #[test]
    fn write_metrics_include_blobs_deleted_before_the_report() {
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(
            HostId(1),
            BlobDevConfig::nvme(),
            StoreConfig::s3(),
            &network,
        );
        let name =
            blockd_core::layout::journal_blob(VsetId(1), 1, blockd_core::types::JournalSeq(0));
        let mut executor = Executor::simulation(21);
        let task_world = Rc::clone(&world);
        executor.block_on(async move {
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
