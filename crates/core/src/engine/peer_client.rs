use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};

use blockd_exec::channel::{Closed, OneReceiver, OneSender, oneshot};
use blockd_exec::{FaultPoint, fault_point, timeout};

use crate::page_file::PageFileLoc;
use crate::protocol::{PeerMsg, PeerRequestId, ReplicaArtifact, ReplicaCommitInfo};
use crate::types::{HostId, JournalSeq, VolumeId};
use crate::world::Peers;

type ReplicaStatusKey = (HostId, VolumeId, u64);
type ReplicaPutKey = (HostId, VolumeId, u64, ReplicaArtifact, u32);
type ReplicaCommitKey = (HostId, VolumeId, u64, u64, JournalSeq, u64);
type MigrationKey = (VolumeId, HostId, u64);

struct PendingKeys;

impl PendingKeys {
    const fn put(
        target: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
    ) -> ReplicaPutKey {
        (target, volume, assignment_epoch, artifact, checksum)
    }

    const fn commit(
        target: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
    ) -> ReplicaCommitKey {
        (
            target,
            volume,
            assignment_epoch,
            info.writer_fence,
            info.seq,
            info.sync_covered_through,
        )
    }
}
struct Pending {
    expected: HostId,
    reply: PendingReply,
}

enum PendingReply {
    Page(OneSender<Option<Vec<u8>>>),
    Unit(OneSender<()>),
    Status(OneSender<Option<ReplicaCommitInfo>>),
}

enum PendingValue {
    Page(Option<Vec<u8>>),
    Unit,
    Status(Option<ReplicaCommitInfo>),
}

impl PendingReply {
    fn send(self, value: PendingValue) -> bool {
        match (self, value) {
            (Self::Page(reply), PendingValue::Page(value)) => reply.send(value).is_ok(),
            (Self::Unit(reply), PendingValue::Unit) => reply.send(()).is_ok(),
            (Self::Status(reply), PendingValue::Status(value)) => reply.send(value).is_ok(),
            _ => false,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PendingKey {
    Page(PeerRequestId),
    Migration(MigrationKey),
    Status(ReplicaStatusKey),
    Put(ReplicaPutKey),
    Commit(ReplicaCommitKey),
}

#[derive(Default)]
struct Broker {
    next_request: u64,
    next_generation: u64,
    pending: BTreeMap<PendingKey, BTreeMap<u64, Pending>>,
    active_migrations: BTreeSet<MigrationKey>,
    accepted_migrations: BTreeSet<MigrationKey>,
}

/// One authenticated peer reply. Dropping the future unregisters it, so a
/// timeout or caller cancellation cannot leave a waiter behind or consume a
/// later retry's reply.
pub(super) struct PeerReply<T> {
    receive: OneReceiver<T>,
    broker: Weak<RefCell<Broker>>,
    key: PendingKey,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PeerRpcError {
    pub attempts: u8,
}

struct MigrationCall {
    broker: Weak<RefCell<Broker>>,
    key: MigrationKey,
}

impl Drop for MigrationCall {
    fn drop(&mut self) {
        if let Some(broker) = self.broker.upgrade() {
            let mut broker = broker.borrow_mut();
            broker.active_migrations.remove(&self.key);
            broker.accepted_migrations.remove(&self.key);
            broker.pending.remove(&PendingKey::Migration(self.key));
        }
    }
}

impl<T> Future for PeerReply<T> {
    type Output = Result<T, Closed>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receive).poll(context)
    }
}

impl<T> Drop for PeerReply<T> {
    fn drop(&mut self) {
        let Some(broker) = self.broker.upgrade() else {
            return;
        };
        broker
            .borrow_mut()
            .remove_if_generation(self.key, self.generation);
    }
}

#[derive(Clone)]
pub(super) struct PeerClient {
    broker: Rc<RefCell<Broker>>,
    retry: u64,
    page_retry: u64,
    migration_retry: u64,
}

impl Default for PeerClient {
    fn default() -> Self {
        Self::new(1)
    }
}

impl PeerClient {
    pub(super) fn new(retry: u64) -> Self {
        Self {
            broker: Rc::new(RefCell::new(Broker::default())),
            retry,
            page_retry: retry,
            migration_retry: retry,
        }
    }

    pub(super) fn for_host(retry: u64) -> Self {
        Self {
            broker: Rc::new(RefCell::new(Broker::default())),
            retry,
            page_retry: 50_000_000,
            migration_retry: 5_000_000,
        }
    }

    pub async fn fetch_page<W: Peers>(
        &self,
        world: &W,
        source: HostId,
        volume: VolumeId,
        location: PageFileLoc,
        replica_assignment_epoch: Option<u64>,
    ) -> Option<Vec<u8>> {
        loop {
            let (io, receive) = self.register_page(source);
            Peers::send(
                world,
                source,
                PeerMsg::FetchRange {
                    io,
                    volume,
                    replica_assignment_epoch,
                    fence: location.fence,
                    object: location.object,
                    offset: location.offset,
                    len: location.len,
                },
            )
            .await;
            if let Ok(Ok(bytes)) = timeout(self.page_retry, receive).await {
                return bytes;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn offer_migration_once<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        volume: VolumeId,
        offer_fence: u64,
        record: Vec<u8>,
        vmstate: Option<Vec<u8>>,
    ) -> bool {
        let key = (volume, target, offer_fence);
        self.broker.borrow_mut().active_migrations.insert(key);
        let _call = MigrationCall {
            broker: Rc::downgrade(&self.broker),
            key,
        };
        if self.broker.borrow_mut().accepted_migrations.remove(&key) {
            return true;
        }
        let receive = self.migration(volume, target, offer_fence);
        Peers::send(
            world,
            target,
            PeerMsg::MigrateOffer {
                volume,
                record,
                vmstate,
            },
        )
        .await;
        matches!(timeout(self.migration_retry, receive).await, Ok(Ok(())))
            || self.take_delayed_migration_accept(key)
    }

    fn take_delayed_migration_accept(&self, key: MigrationKey) -> bool {
        self.broker.borrow_mut().accepted_migrations.remove(&key)
    }

    pub async fn replica_status<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
    ) -> Result<(Option<ReplicaCommitInfo>, u8), PeerRpcError> {
        let mut attempts = 0_u8;
        loop {
            attempts = attempts.saturating_add(1);
            let receive = self.status(target, volume, assignment_epoch);
            Peers::send(
                world,
                target,
                PeerMsg::ReplicaStatus {
                    volume,
                    assignment_epoch,
                },
            )
            .await;
            if let Ok(Ok(committed)) = timeout(self.retry, receive).await {
                return Ok((committed, attempts));
            }
            if fault_point(FaultPoint::ReplicaRetryTimer) || attempts >= 3 {
                return Err(PeerRpcError { attempts });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn replica_put<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
    ) -> Result<u8, PeerRpcError> {
        let mut attempts = 0_u8;
        loop {
            attempts = attempts.saturating_add(1);
            let receive = self.put(
                target,
                PendingKeys::put(target, volume, assignment_epoch, artifact, checksum),
            );
            Peers::send(
                world,
                target,
                PeerMsg::ReplicaPut {
                    volume,
                    assignment_epoch,
                    artifact,
                    checksum,
                    bytes: bytes.clone(),
                },
            )
            .await;
            if let Ok(Ok(())) = timeout(self.retry, receive).await {
                return Ok(attempts);
            }
            if fault_point(FaultPoint::ReplicaRetryTimer) || attempts >= 3 {
                return Err(PeerRpcError { attempts });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn replica_commit<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        required: Vec<ReplicaArtifact>,
        record: Vec<u8>,
    ) -> Result<u8, PeerRpcError> {
        let mut attempts = 0_u8;
        loop {
            attempts = attempts.saturating_add(1);
            let receive = self.commit(
                target,
                PendingKeys::commit(target, volume, assignment_epoch, info),
            );
            Peers::send(
                world,
                target,
                PeerMsg::ReplicaCommit {
                    volume,
                    assignment_epoch,
                    info,
                    required: required.clone(),
                    record: record.clone(),
                },
            )
            .await;
            if let Ok(Ok(())) = timeout(self.retry, receive).await {
                return Ok(attempts);
            }
            if fault_point(FaultPoint::ReplicaRetryTimer) || attempts >= 3 {
                return Err(PeerRpcError { attempts });
            }
        }
    }

    #[cfg(test)]
    pub(super) fn page(&self, source: HostId) -> (PeerRequestId, PeerReply<Option<Vec<u8>>>) {
        self.register_page(source)
    }

    fn register_page(&self, source: HostId) -> (PeerRequestId, PeerReply<Option<Vec<u8>>>) {
        let request = self.broker.borrow_mut().allocate_request();
        let reply = self.pending(source, PendingKey::Page(request), false, PendingReply::Page);
        (request, reply)
    }

    pub fn resolve_page(&self, request: PeerRequestId, from: HostId, bytes: Option<Vec<u8>>) {
        self.broker.borrow_mut().resolve(
            PendingKey::Page(request),
            from,
            PendingValue::Page(bytes),
        );
    }

    fn migration(&self, volume: VolumeId, target: HostId, offer_fence: u64) -> PeerReply<()> {
        let key = (volume, target, offer_fence);
        self.pending(
            target,
            PendingKey::Migration(key),
            false,
            PendingReply::Unit,
        )
    }

    pub fn resolve_migration(&self, volume: VolumeId, from: HostId, offer_fence: u64) {
        let key = (volume, from, offer_fence);
        let mut broker = self.broker.borrow_mut();
        if broker.active_migrations.contains(&key)
            && !broker.resolve(PendingKey::Migration(key), from, PendingValue::Unit)
        {
            broker.accepted_migrations.insert(key);
        }
    }

    pub(super) fn status(
        &self,
        target: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
    ) -> PeerReply<Option<ReplicaCommitInfo>> {
        // Status is a monotonic durable attestation within an authenticated
        // (host, volume, assignment epoch). A late reply may understate progress
        // and cause redundant transfer, but cannot overstate durable progress
        // or satisfy another assignment, so retries intentionally share this
        // semantic key.
        let key = (target, volume, assignment_epoch);
        self.pending(target, PendingKey::Status(key), true, PendingReply::Status)
    }

    pub fn resolve_status(
        &self,
        from: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        committed: Option<ReplicaCommitInfo>,
    ) {
        self.broker.borrow_mut().resolve(
            PendingKey::Status((from, volume, assignment_epoch)),
            from,
            PendingValue::Status(committed),
        );
    }

    fn put(&self, target: HostId, key: ReplicaPutKey) -> PeerReply<()> {
        self.pending(target, PendingKey::Put(key), true, PendingReply::Unit)
    }

    pub fn resolve_put(
        &self,
        from: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
    ) {
        self.broker.borrow_mut().resolve(
            PendingKey::Put(PendingKeys::put(
                from,
                volume,
                assignment_epoch,
                artifact,
                checksum,
            )),
            from,
            PendingValue::Unit,
        );
    }

    fn commit(&self, target: HostId, key: ReplicaCommitKey) -> PeerReply<()> {
        self.pending(target, PendingKey::Commit(key), true, PendingReply::Unit)
    }

    pub fn resolve_commit(
        &self,
        from: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
    ) {
        self.broker.borrow_mut().resolve(
            PendingKey::Commit(PendingKeys::commit(from, volume, assignment_epoch, info)),
            from,
            PendingValue::Unit,
        );
    }

    fn pending<T>(
        &self,
        expected: HostId,
        key: PendingKey,
        grouped: bool,
        wrap: fn(OneSender<T>) -> PendingReply,
    ) -> PeerReply<T> {
        let generation = {
            let mut broker = self.broker.borrow_mut();
            let generation = broker.next_generation;
            broker.next_generation = broker
                .next_generation
                .checked_add(1)
                .expect("peer waiter generation overflow");
            generation
        };
        let (send, receive) = oneshot();
        let mut broker = self.broker.borrow_mut();
        let entries = broker.pending.entry(key).or_default();
        if !grouped {
            entries.clear();
        }
        entries.insert(
            generation,
            Pending {
                expected,
                reply: wrap(send),
            },
        );
        PeerReply {
            receive,
            broker: Rc::downgrade(&self.broker),
            key,
            generation,
        }
    }
}

impl Broker {
    fn allocate_request(&mut self) -> PeerRequestId {
        let request = PeerRequestId(self.next_request);
        self.next_request = self
            .next_request
            .checked_add(1)
            .expect("peer request overflow");
        request
    }

    fn remove_if_generation(&mut self, key: PendingKey, generation: u64) {
        let Some(entries) = self.pending.get_mut(&key) else {
            return;
        };
        entries.remove(&generation);
        if entries.is_empty() {
            self.pending.remove(&key);
        }
    }

    fn resolve(&mut self, key: PendingKey, from: HostId, value: PendingValue) -> bool {
        if !self
            .pending
            .get(&key)
            .is_some_and(|entries| entries.values().all(|entry| entry.expected == from))
        {
            return false;
        }
        let Some(mut entries) = self.pending.remove(&key) else {
            return false;
        };
        let Some((_, last)) = entries.pop_last() else {
            return false;
        };
        for entry in entries.into_values() {
            let cloned = match &value {
                PendingValue::Page(value) => PendingValue::Page(value.clone()),
                PendingValue::Unit => PendingValue::Unit,
                PendingValue::Status(value) => PendingValue::Status(*value),
            };
            let _ = entry.reply.send(cloned);
        }
        let _ = last.reply.send(value);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use blockd_exec::{FaultConfig, join2, simulation_scope};

    use super::*;

    const fn id(host: u32) -> HostId {
        crate::types::HostId::new(host)
    }

    const CURRENT_MIGRATION_FENCE: u64 = 7;

    #[derive(Clone, Copy)]
    enum ReplyMode {
        Silent,
        AcceptMigrationOnSecondOffer,
        DeliverStaleMigrationAccept,
    }

    struct TestPeers {
        client: PeerClient,
        sends: Cell<u8>,
        mode: ReplyMode,
    }

    impl Peers for TestPeers {
        async fn send(&self, to: HostId, message: PeerMsg) {
            let sends = self.sends.get().saturating_add(1);
            self.sends.set(sends);
            if matches!(self.mode, ReplyMode::AcceptMigrationOnSecondOffer)
                && sends == 2
                && let PeerMsg::MigrateOffer { volume, .. } = message
            {
                self.client
                    .resolve_migration(volume, to, CURRENT_MIGRATION_FENCE);
                self.client
                    .resolve_migration(volume, to, CURRENT_MIGRATION_FENCE);
            } else if matches!(self.mode, ReplyMode::DeliverStaleMigrationAccept)
                && let PeerMsg::MigrateOffer { volume, .. } = message
            {
                self.client
                    .resolve_migration(volume, to, CURRENT_MIGRATION_FENCE - 1);
            }
        }

        async fn recv(&self) -> Option<(crate::types::HostId, PeerMsg)> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn page_reply_requires_the_authenticated_source_and_resolves_once() {
        let client = PeerClient::default();
        let (request, receive) = client.page(id(2));

        client.resolve_page(request, id(3), Some(vec![3]));
        client.resolve_page(request, id(2), Some(vec![2]));
        client.resolve_page(request, id(2), Some(vec![4]));

        assert_eq!(
            simulation_scope(1, FaultConfig::default(), receive).await,
            Ok(Some(vec![2]))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_waiter_cleanup_cannot_remove_a_newer_retry() {
        let client = PeerClient::default();
        let stale = client.status(id(2), VolumeId(7), 11);
        let current = client.status(id(2), VolumeId(7), 11);

        drop(stale);
        client.resolve_status(id(3), VolumeId(7), 11, None);
        assert!(
            client
                .broker
                .borrow()
                .pending
                .contains_key(&PendingKey::Status((id(2), VolumeId(7), 11)))
        );
        client.resolve_status(id(2), VolumeId(7), 11, None);

        assert_eq!(
            simulation_scope(2, FaultConfig::default(), current).await,
            Ok(None)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn equivalent_replica_rpcs_to_one_host_resolve_every_waiter() {
        let client = PeerClient::default();
        let first = client.status(id(2), VolumeId(7), 11);
        let second = client.status(id(2), VolumeId(7), 11);
        assert_eq!(client.broker.borrow().pending.len(), 1);
        assert_eq!(
            client.broker.borrow().pending[&PendingKey::Status((id(2), VolumeId(7), 11))].len(),
            2
        );

        client.resolve_status(id(2), VolumeId(7), 11, None);
        assert_eq!(
            simulation_scope(6, FaultConfig::default(), join2(first, second)).await,
            (Ok(None), Ok(None))
        );
        assert!(client.broker.borrow().pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn equivalent_replica_rpcs_to_different_hosts_keep_independent_waiters() {
        let client = PeerClient::default();
        let first = client.status(id(2), VolumeId(7), 11);
        let second = client.status(id(3), VolumeId(7), 11);
        assert_eq!(client.broker.borrow().pending.len(), 2);

        client.resolve_status(id(3), VolumeId(7), 11, None);
        client.resolve_status(id(2), VolumeId(7), 11, None);
        assert_eq!(
            simulation_scope(5, FaultConfig::default(), join2(first, second)).await,
            (Ok(None), Ok(None))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_retry_cleans_each_timed_out_waiter() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::Silent,
        });
        let error = simulation_scope(3, FaultConfig::default(), {
            let client = client.clone();
            let peers = Rc::clone(&peers);
            async move {
                client
                    .replica_status(peers.as_ref(), id(2), VolumeId(7), 11)
                    .await
            }
        })
        .await
        .expect_err("three unanswered attempts time out");

        assert_eq!(error, PeerRpcError { attempts: 3 });
        assert_eq!(peers.sends.get(), 3);
        assert!(client.broker.borrow().pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn migration_retry_and_duplicate_accept_are_encapsulated() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::AcceptMigrationOnSecondOffer,
        });
        let accepted = simulation_scope(4, FaultConfig::default(), {
            let client = client.clone();
            let peers = Rc::clone(&peers);
            async move {
                loop {
                    if client
                        .offer_migration_once(
                            peers.as_ref(),
                            id(2),
                            VolumeId(7),
                            CURRENT_MIGRATION_FENCE,
                            vec![1, 2, 3],
                            None,
                        )
                        .await
                    {
                        break true;
                    }
                }
            }
        })
        .await;

        assert!(accepted);
        assert_eq!(peers.sends.get(), 2);
        let broker = client.broker.borrow();
        assert!(broker.pending.is_empty());
        assert!(broker.active_migrations.is_empty());
        assert!(broker.accepted_migrations.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn one_migration_offer_times_out_and_releases_its_waiter() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::Silent,
        });
        let accepted = simulation_scope(7, FaultConfig::default(), {
            let client = client.clone();
            let peers = Rc::clone(&peers);
            async move {
                client
                    .offer_migration_once(
                        peers.as_ref(),
                        id(2),
                        VolumeId(7),
                        CURRENT_MIGRATION_FENCE,
                        vec![1, 2, 3],
                        None,
                    )
                    .await
            }
        })
        .await;

        assert!(!accepted);
        assert_eq!(peers.sends.get(), 1);
        let broker = client.broker.borrow();
        assert!(broker.pending.is_empty());
        assert!(broker.active_migrations.is_empty());
    }

    #[test]
    fn delayed_exact_accept_is_consumed_at_the_timeout_boundary() {
        let client = PeerClient::default();
        let key = (VolumeId(7), id(2), CURRENT_MIGRATION_FENCE);
        let call = MigrationCall {
            broker: Rc::downgrade(&client.broker),
            key,
        };
        client.broker.borrow_mut().active_migrations.insert(key);
        let timed_out = client.migration(key.0, key.1, key.2);
        drop(timed_out);
        client.resolve_migration(key.0, key.1, key.2);
        assert!(client.broker.borrow().accepted_migrations.contains(&key));
        let accepted = client.take_delayed_migration_accept(key);
        drop(call);

        assert!(accepted);
        let broker = client.broker.borrow();
        assert!(broker.pending.is_empty());
        assert!(broker.active_migrations.is_empty());
        assert!(broker.accepted_migrations.is_empty());
    }

    #[test]
    fn cancelled_migration_attempt_discards_a_racing_accept() {
        let client = PeerClient::default();
        let key = (VolumeId(7), id(2), CURRENT_MIGRATION_FENCE);
        let call = MigrationCall {
            broker: Rc::downgrade(&client.broker),
            key,
        };
        client.broker.borrow_mut().active_migrations.insert(key);
        let cancelled = client.migration(key.0, key.1, key.2);
        drop(cancelled);
        client.resolve_migration(key.0, key.1, key.2);
        assert!(client.broker.borrow().accepted_migrations.contains(&key));

        drop(call);

        let broker = client.broker.borrow();
        assert!(broker.pending.is_empty());
        assert!(broker.active_migrations.is_empty());
        assert!(broker.accepted_migrations.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_prior_accept_cannot_complete_a_new_offer() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::DeliverStaleMigrationAccept,
        });
        let accepted = simulation_scope(8, FaultConfig::default(), {
            let client = client.clone();
            let peers = Rc::clone(&peers);
            async move {
                client
                    .offer_migration_once(
                        peers.as_ref(),
                        id(2),
                        VolumeId(7),
                        CURRENT_MIGRATION_FENCE,
                        vec![4, 5, 6],
                        None,
                    )
                    .await
            }
        })
        .await;

        assert!(!accepted, "an accept from an older handoff must be ignored");
    }
}
