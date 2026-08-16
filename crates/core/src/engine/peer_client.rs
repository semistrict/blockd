use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};

use blockd_exec::channel::{Closed, OneReceiver, OneSender, oneshot};
use blockd_exec::{FaultPoint, fault_point, timeout};

use crate::protocol::{PeerMsg, PeerRequestId, ReplicaArtifact, ReplicaCommitInfo};
use crate::segment::PageLoc;
use crate::types::{HostId, JournalSeq, VsetId};
use crate::world::Peers;
use crate::{authority::AuthorityProof, vnode_member::ProtectedClosureRef};

type ReplicaStatusKey = (HostId, VsetId, u64);
type ReplicaPutKey = (HostId, VsetId, u64, ReplicaArtifact, u32);
type ReplicaCommitKey = (HostId, VsetId, u64, u64, JournalSeq, u64);
type MigrationKey = (VsetId, HostId, u64);
struct Pending {
    expected: HostId,
    reply: PendingReply,
}

enum PendingReply {
    Page(OneSender<Option<Vec<u8>>>),
    Unit(OneSender<()>),
    Status(OneSender<Option<ReplicaCommitInfo>>),
    Adoption(OneSender<(AuthorityProof, Vec<ProtectedClosureRef>)>),
    VnodeCommit(OneSender<ProtectedClosureRef>),
}

enum PendingValue {
    Page(Option<Vec<u8>>),
    Unit,
    Status(Option<ReplicaCommitInfo>),
    Adoption(AuthorityProof, Vec<ProtectedClosureRef>),
    VnodeCommit(ProtectedClosureRef),
}

impl PendingReply {
    fn send(self, value: PendingValue) -> bool {
        match (self, value) {
            (Self::Page(reply), PendingValue::Page(value)) => reply.send(value).is_ok(),
            (Self::Unit(reply), PendingValue::Unit) => reply.send(()).is_ok(),
            (Self::Status(reply), PendingValue::Status(value)) => reply.send(value).is_ok(),
            (Self::Adoption(reply), PendingValue::Adoption(proof, closures)) => {
                reply.send((proof, closures)).is_ok()
            }
            (Self::VnodeCommit(reply), PendingValue::VnodeCommit(value)) => {
                reply.send(value).is_ok()
            }
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
    Adoption(PeerRequestId),
    VnodeClosure(PeerRequestId),
    VnodeCommit(PeerRequestId),
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

#[derive(Clone, Default)]
pub(super) struct PeerClient {
    broker: Rc<RefCell<Broker>>,
}

impl PeerClient {
    pub async fn fetch_page<W: Peers>(
        &self,
        world: &W,
        source: HostId,
        vset: VsetId,
        location: PageLoc,
        replica_assignment_epoch: Option<u64>,
        retry: u64,
    ) -> Option<Vec<u8>> {
        loop {
            let (io, receive) = self.register_page(source);
            Peers::send(
                world,
                source,
                PeerMsg::FetchRange {
                    io,
                    vset,
                    replica_assignment_epoch,
                    fence: location.fence,
                    seg: location.seg,
                    offset: location.offset,
                    len: location.len,
                },
            )
            .await;
            if let Ok(Ok(bytes)) = timeout(retry, receive).await {
                return bytes;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn offer_migration_once<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        vset: VsetId,
        offer_fence: u64,
        record: Vec<u8>,
        vmstate: Option<Vec<u8>>,
        retry: u64,
    ) -> bool {
        let key = (vset, target, offer_fence);
        self.broker.borrow_mut().active_migrations.insert(key);
        let _call = MigrationCall {
            broker: Rc::downgrade(&self.broker),
            key,
        };
        if self.broker.borrow_mut().accepted_migrations.remove(&key) {
            return true;
        }
        let receive = self.migration(vset, target, offer_fence);
        Peers::send(
            world,
            target,
            PeerMsg::MigrateOffer {
                vset,
                record,
                vmstate,
            },
        )
        .await;
        matches!(timeout(retry, receive).await, Ok(Ok(())))
    }

    pub async fn replica_status<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        retry: u64,
    ) -> Result<(Option<ReplicaCommitInfo>, u8), PeerRpcError> {
        let mut attempts = 0_u8;
        loop {
            attempts = attempts.saturating_add(1);
            let receive = self.status(target, vset, assignment_epoch);
            Peers::send(
                world,
                target,
                PeerMsg::ReplicaStatus {
                    vset,
                    assignment_epoch,
                },
            )
            .await;
            if let Ok(Ok(committed)) = timeout(retry, receive).await {
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
        vset: VsetId,
        assignment_epoch: u64,
        artifact: ReplicaArtifact,
        checksum: u32,
        bytes: Vec<u8>,
        retry: u64,
    ) -> Result<u8, PeerRpcError> {
        let mut attempts = 0_u8;
        loop {
            attempts = attempts.saturating_add(1);
            let receive = self.put(target, (target, vset, assignment_epoch, artifact, checksum));
            Peers::send(
                world,
                target,
                PeerMsg::ReplicaPut {
                    vset,
                    assignment_epoch,
                    artifact,
                    checksum,
                    bytes: bytes.clone(),
                },
            )
            .await;
            if let Ok(Ok(())) = timeout(retry, receive).await {
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
        vset: VsetId,
        assignment_epoch: u64,
        info: ReplicaCommitInfo,
        required: Vec<ReplicaArtifact>,
        record: Vec<u8>,
        retry: u64,
    ) -> Result<u8, PeerRpcError> {
        let mut attempts = 0_u8;
        loop {
            attempts = attempts.saturating_add(1);
            let receive = self.commit(target, commit_key(target, vset, assignment_epoch, info));
            Peers::send(
                world,
                target,
                PeerMsg::ReplicaCommit {
                    vset,
                    assignment_epoch,
                    info,
                    required: required.clone(),
                    record: record.clone(),
                },
            )
            .await;
            if let Ok(Ok(())) = timeout(retry, receive).await {
                return Ok(attempts);
            }
            if fault_point(FaultPoint::ReplicaRetryTimer) || attempts >= 3 {
                return Err(PeerRpcError { attempts });
            }
        }
    }

    pub async fn adopt_vnode<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        proof: AuthorityProof,
        retry: u64,
    ) -> Result<Vec<ProtectedClosureRef>, PeerRpcError> {
        let mut attempts = 0_u8;
        loop {
            attempts = attempts.saturating_add(1);
            let (io, receive) = self.register_adoption(target);
            Peers::send(world, target, PeerMsg::VnodeAdopt { io, proof }).await;
            if let Ok(Ok((received, closures))) = timeout(retry, receive).await
                && received == proof
            {
                return Ok(closures);
            }
            if attempts >= 3 {
                return Err(PeerRpcError { attempts });
            }
        }
    }

    fn register_adoption(
        &self,
        target: HostId,
    ) -> (
        PeerRequestId,
        PeerReply<(AuthorityProof, Vec<ProtectedClosureRef>)>,
    ) {
        let request = self.broker.borrow_mut().allocate_request();
        let reply = self.pending(
            target,
            PendingKey::Adoption(request),
            false,
            PendingReply::Adoption,
        );
        (request, reply)
    }

    pub fn resolve_adoption(
        &self,
        request: PeerRequestId,
        from: HostId,
        proof: AuthorityProof,
        closures: Vec<ProtectedClosureRef>,
    ) {
        self.broker.borrow_mut().resolve(
            PendingKey::Adoption(request),
            from,
            PendingValue::Adoption(proof, closures),
        );
    }

    pub async fn fetch_vnode_closure<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        vnode: crate::authority::VnodeId,
        closure: ProtectedClosureRef,
        retry: u64,
    ) -> Option<Vec<u8>> {
        for _ in 0..3 {
            let request = self.broker.borrow_mut().allocate_request();
            let receive = self.pending(
                target,
                PendingKey::VnodeClosure(request),
                false,
                PendingReply::Page,
            );
            Peers::send(
                world,
                target,
                PeerMsg::VnodeFetchClosure {
                    io: request,
                    vnode,
                    closure,
                },
            )
            .await;
            if let Ok(Ok(Some(bytes))) = timeout(retry, receive).await {
                return Some(bytes);
            }
        }
        None
    }

    pub fn resolve_vnode_closure(
        &self,
        request: PeerRequestId,
        from: HostId,
        bytes: Option<Vec<u8>>,
    ) {
        self.broker.borrow_mut().resolve(
            PendingKey::VnodeClosure(request),
            from,
            PendingValue::Page(bytes),
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn commit_vnode_closure<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        proof: AuthorityProof,
        vset: VsetId,
        sequence: u64,
        bytes: Vec<u8>,
        retry: u64,
    ) -> Option<ProtectedClosureRef> {
        for _ in 0..3 {
            let request = self.broker.borrow_mut().allocate_request();
            let receive = self.pending(
                target,
                PendingKey::VnodeCommit(request),
                false,
                PendingReply::VnodeCommit,
            );
            Peers::send(
                world,
                target,
                PeerMsg::VnodeCommit {
                    io: request,
                    proof,
                    vset,
                    sequence,
                    bytes: bytes.clone(),
                },
            )
            .await;
            if let Ok(Ok(closure)) = timeout(retry, receive).await {
                return Some(closure);
            }
        }
        None
    }

    pub fn resolve_vnode_commit(
        &self,
        request: PeerRequestId,
        from: HostId,
        closure: ProtectedClosureRef,
    ) {
        self.broker.borrow_mut().resolve(
            PendingKey::VnodeCommit(request),
            from,
            PendingValue::VnodeCommit(closure),
        );
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

    fn migration(&self, vset: VsetId, target: HostId, offer_fence: u64) -> PeerReply<()> {
        let key = (vset, target, offer_fence);
        self.pending(
            target,
            PendingKey::Migration(key),
            false,
            PendingReply::Unit,
        )
    }

    pub fn resolve_migration(&self, vset: VsetId, from: HostId, offer_fence: u64) {
        let key = (vset, from, offer_fence);
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
        vset: VsetId,
        assignment_epoch: u64,
    ) -> PeerReply<Option<ReplicaCommitInfo>> {
        // Status is a monotonic durable attestation within an authenticated
        // (host, vset, assignment epoch). A late reply may understate progress
        // and cause redundant transfer, but cannot overstate durable progress
        // or satisfy another assignment, so retries intentionally share this
        // semantic key.
        let key = (target, vset, assignment_epoch);
        self.pending(target, PendingKey::Status(key), true, PendingReply::Status)
    }

    pub fn resolve_status(
        &self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        committed: Option<ReplicaCommitInfo>,
    ) {
        self.broker.borrow_mut().resolve(
            PendingKey::Status((from, vset, assignment_epoch)),
            from,
            PendingValue::Status(committed),
        );
    }

    fn put(&self, target: HostId, key: ReplicaPutKey) -> PeerReply<()> {
        self.pending(target, PendingKey::Put(key), true, PendingReply::Unit)
    }

    pub fn resolve_put(&self, from: HostId, key: ReplicaPutKey) {
        self.broker
            .borrow_mut()
            .resolve(PendingKey::Put(key), from, PendingValue::Unit);
    }

    fn commit(&self, target: HostId, key: ReplicaCommitKey) -> PeerReply<()> {
        self.pending(target, PendingKey::Commit(key), true, PendingReply::Unit)
    }

    pub fn resolve_commit(&self, from: HostId, key: ReplicaCommitKey) {
        self.broker
            .borrow_mut()
            .resolve(PendingKey::Commit(key), from, PendingValue::Unit);
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
                PendingValue::Adoption(proof, closures) => {
                    PendingValue::Adoption(*proof, closures.clone())
                }
                PendingValue::VnodeCommit(value) => PendingValue::VnodeCommit(*value),
            };
            let _ = entry.reply.send(cloned);
        }
        let _ = last.reply.send(value);
        true
    }
}

fn commit_key(
    target: HostId,
    vset: VsetId,
    assignment_epoch: u64,
    info: ReplicaCommitInfo,
) -> ReplicaCommitKey {
    (
        target,
        vset,
        assignment_epoch,
        info.writer_fence,
        info.seq,
        info.sync_covered_through,
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use blockd_exec::{FaultConfig, join2, simulation_scope};

    use super::*;

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
                && let PeerMsg::MigrateOffer { vset, .. } = message
            {
                self.client
                    .resolve_migration(vset, to, CURRENT_MIGRATION_FENCE);
                self.client
                    .resolve_migration(vset, to, CURRENT_MIGRATION_FENCE);
            } else if matches!(self.mode, ReplyMode::DeliverStaleMigrationAccept)
                && let PeerMsg::MigrateOffer { vset, .. } = message
            {
                self.client
                    .resolve_migration(vset, to, CURRENT_MIGRATION_FENCE - 1);
            }
        }

        async fn recv(&self) -> Option<(HostId, PeerMsg)> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn page_reply_requires_the_authenticated_source_and_resolves_once() {
        let client = PeerClient::default();
        let (request, receive) = client.page(HostId(2));

        client.resolve_page(request, HostId(3), Some(vec![3]));
        client.resolve_page(request, HostId(2), Some(vec![2]));
        client.resolve_page(request, HostId(2), Some(vec![4]));

        assert_eq!(
            simulation_scope(1, FaultConfig::default(), receive).await,
            Ok(Some(vec![2]))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_waiter_cleanup_cannot_remove_a_newer_retry() {
        let client = PeerClient::default();
        let stale = client.status(HostId(2), VsetId(7), 11);
        let current = client.status(HostId(2), VsetId(7), 11);

        drop(stale);
        client.resolve_status(HostId(3), VsetId(7), 11, None);
        assert!(
            client
                .broker
                .borrow()
                .pending
                .contains_key(&PendingKey::Status((HostId(2), VsetId(7), 11)))
        );
        client.resolve_status(HostId(2), VsetId(7), 11, None);

        assert_eq!(
            simulation_scope(2, FaultConfig::default(), current).await,
            Ok(None)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn equivalent_replica_rpcs_to_one_host_resolve_every_waiter() {
        let client = PeerClient::default();
        let first = client.status(HostId(2), VsetId(7), 11);
        let second = client.status(HostId(2), VsetId(7), 11);
        assert_eq!(client.broker.borrow().pending.len(), 1);
        assert_eq!(
            client.broker.borrow().pending[&PendingKey::Status((HostId(2), VsetId(7), 11))].len(),
            2
        );

        client.resolve_status(HostId(2), VsetId(7), 11, None);
        assert_eq!(
            simulation_scope(6, FaultConfig::default(), join2(first, second)).await,
            (Ok(None), Ok(None))
        );
        assert!(client.broker.borrow().pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn equivalent_replica_rpcs_to_different_hosts_keep_independent_waiters() {
        let client = PeerClient::default();
        let first = client.status(HostId(2), VsetId(7), 11);
        let second = client.status(HostId(3), VsetId(7), 11);
        assert_eq!(client.broker.borrow().pending.len(), 2);

        client.resolve_status(HostId(3), VsetId(7), 11, None);
        client.resolve_status(HostId(2), VsetId(7), 11, None);
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
                    .replica_status(peers.as_ref(), HostId(2), VsetId(7), 11, 1)
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
                            HostId(2),
                            VsetId(7),
                            CURRENT_MIGRATION_FENCE,
                            vec![1, 2, 3],
                            None,
                            1,
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
                        HostId(2),
                        VsetId(7),
                        CURRENT_MIGRATION_FENCE,
                        vec![1, 2, 3],
                        None,
                        1,
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
                        HostId(2),
                        VsetId(7),
                        CURRENT_MIGRATION_FENCE,
                        vec![4, 5, 6],
                        None,
                        1,
                    )
                    .await
            }
        })
        .await;

        assert!(!accepted, "an accept from an older handoff must be ignored");
    }
}
