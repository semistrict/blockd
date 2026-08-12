use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};

use blockd_exec::channel::{Closed, OneReceiver, OneSender, oneshot};
use blockd_exec::{FaultPoint, fault_point, timeout};

use crate::mapleaf::LeafPtr;
use crate::protocol::{PeerMsg, PeerRequestId, ReplicaArtifact, ReplicaCommitInfo};
use crate::segment::PageLoc;
use crate::types::{HostId, JournalSeq, VsetId};
use crate::world::Peers;
use crate::{authority::AuthorityProof, vnode_member::ProtectedClosureRef};

type ReplicaStatusKey = (HostId, VsetId, u64);
type ReplicaPutKey = (HostId, VsetId, u64, ReplicaArtifact, u32);
type ReplicaCommitKey = (HostId, VsetId, u64, u64, JournalSeq, u64);
type MigrationKey = (VsetId, HostId, u64);
type PendingGroup<T> = BTreeMap<u64, Pending<T>>;

struct Pending<T> {
    expected: HostId,
    generation: u64,
    reply: OneSender<T>,
}

#[derive(Clone, Copy)]
enum PendingKey {
    Page(PeerRequestId),
    Leaf(PeerRequestId),
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
    pages: BTreeMap<PeerRequestId, Pending<Option<Vec<u8>>>>,
    leaves: BTreeMap<PeerRequestId, Pending<Option<Vec<u8>>>>,
    migrations: BTreeMap<MigrationKey, Pending<()>>,
    active_migrations: BTreeSet<MigrationKey>,
    accepted_migrations: BTreeSet<MigrationKey>,
    replica_status: BTreeMap<ReplicaStatusKey, PendingGroup<Option<ReplicaCommitInfo>>>,
    replica_put: BTreeMap<ReplicaPutKey, PendingGroup<()>>,
    replica_commit: BTreeMap<ReplicaCommitKey, PendingGroup<()>>,
    adoptions: BTreeMap<PeerRequestId, Pending<(AuthorityProof, Vec<ProtectedClosureRef>)>>,
    vnode_closures: BTreeMap<PeerRequestId, Pending<Option<Vec<u8>>>>,
    vnode_commits: BTreeMap<PeerRequestId, Pending<ProtectedClosureRef>>,
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
            broker.migrations.remove(&self.key);
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

    pub async fn fetch_leaf<W: Peers>(
        &self,
        world: &W,
        source: HostId,
        vset: VsetId,
        pointer: LeafPtr,
        retry: u64,
    ) -> Option<Vec<u8>> {
        loop {
            let (io, receive) = self.register_leaf(source);
            Peers::send(
                world,
                source,
                PeerMsg::FetchLeaf {
                    io,
                    vset,
                    base: pointer.base,
                    fence: pointer.fence,
                    id: pointer.id,
                },
            )
            .await;
            if let Ok(Ok(bytes)) = timeout(retry, receive).await {
                return bytes;
            }
        }
    }

    pub async fn offer_migration_once<W: Peers>(
        &self,
        world: &W,
        target: HostId,
        vset: VsetId,
        offer_fence: u64,
        record: Vec<u8>,
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
        Peers::send(world, target, PeerMsg::MigrateOffer { vset, record }).await;
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
        let (pending, reply) = self.pending(target, PendingKey::Adoption(request));
        self.broker.borrow_mut().adoptions.insert(request, pending);
        (request, reply)
    }

    pub fn resolve_adoption(
        &self,
        request: PeerRequestId,
        from: HostId,
        proof: AuthorityProof,
        closures: Vec<ProtectedClosureRef>,
    ) {
        resolve(
            &mut self.broker.borrow_mut().adoptions,
            &request,
            from,
            (proof, closures),
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
            let (pending, receive) = self.pending(target, PendingKey::VnodeClosure(request));
            self.broker
                .borrow_mut()
                .vnode_closures
                .insert(request, pending);
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
        resolve(
            &mut self.broker.borrow_mut().vnode_closures,
            &request,
            from,
            bytes,
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
            let (pending, receive) = self.pending(target, PendingKey::VnodeCommit(request));
            self.broker
                .borrow_mut()
                .vnode_commits
                .insert(request, pending);
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
        resolve(
            &mut self.broker.borrow_mut().vnode_commits,
            &request,
            from,
            closure,
        );
    }

    #[cfg(test)]
    pub(super) fn page(&self, source: HostId) -> (PeerRequestId, PeerReply<Option<Vec<u8>>>) {
        self.register_page(source)
    }

    fn register_page(&self, source: HostId) -> (PeerRequestId, PeerReply<Option<Vec<u8>>>) {
        let request = self.broker.borrow_mut().allocate_request();
        let (pending, reply) = self.pending(source, PendingKey::Page(request));
        self.broker.borrow_mut().pages.insert(request, pending);
        (request, reply)
    }

    pub fn resolve_page(&self, request: PeerRequestId, from: HostId, bytes: Option<Vec<u8>>) {
        resolve(&mut self.broker.borrow_mut().pages, &request, from, bytes);
    }

    #[cfg(test)]
    pub(super) fn leaf(&self, source: HostId) -> (PeerRequestId, PeerReply<Option<Vec<u8>>>) {
        self.register_leaf(source)
    }

    fn register_leaf(&self, source: HostId) -> (PeerRequestId, PeerReply<Option<Vec<u8>>>) {
        let request = self.broker.borrow_mut().allocate_request();
        let (pending, reply) = self.pending(source, PendingKey::Leaf(request));
        self.broker.borrow_mut().leaves.insert(request, pending);
        (request, reply)
    }

    pub fn resolve_leaf(&self, request: PeerRequestId, from: HostId, bytes: Option<Vec<u8>>) {
        resolve(&mut self.broker.borrow_mut().leaves, &request, from, bytes);
    }

    fn migration(&self, vset: VsetId, target: HostId, offer_fence: u64) -> PeerReply<()> {
        let key = (vset, target, offer_fence);
        let (pending, reply) = self.pending(target, PendingKey::Migration(key));
        self.broker.borrow_mut().migrations.insert(key, pending);
        reply
    }

    pub fn resolve_migration(&self, vset: VsetId, from: HostId, offer_fence: u64) {
        let key = (vset, from, offer_fence);
        let mut broker = self.broker.borrow_mut();
        if broker.active_migrations.contains(&key)
            && !resolve(&mut broker.migrations, &key, from, ())
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
        let (pending, reply) = self.pending(target, PendingKey::Status(key));
        self.broker
            .borrow_mut()
            .replica_status
            .entry(key)
            .or_default()
            .insert(pending.generation, pending);
        reply
    }

    pub fn resolve_status(
        &self,
        from: HostId,
        vset: VsetId,
        assignment_epoch: u64,
        committed: Option<ReplicaCommitInfo>,
    ) {
        resolve_group(
            &mut self.broker.borrow_mut().replica_status,
            &(from, vset, assignment_epoch),
            from,
            committed,
        );
    }

    fn put(&self, target: HostId, key: ReplicaPutKey) -> PeerReply<()> {
        let (pending, reply) = self.pending(target, PendingKey::Put(key));
        self.broker
            .borrow_mut()
            .replica_put
            .entry(key)
            .or_default()
            .insert(pending.generation, pending);
        reply
    }

    pub fn resolve_put(&self, from: HostId, key: ReplicaPutKey) {
        resolve_group(&mut self.broker.borrow_mut().replica_put, &key, from, ());
    }

    fn commit(&self, target: HostId, key: ReplicaCommitKey) -> PeerReply<()> {
        let (pending, reply) = self.pending(target, PendingKey::Commit(key));
        self.broker
            .borrow_mut()
            .replica_commit
            .entry(key)
            .or_default()
            .insert(pending.generation, pending);
        reply
    }

    pub fn resolve_commit(&self, from: HostId, key: ReplicaCommitKey) {
        resolve_group(&mut self.broker.borrow_mut().replica_commit, &key, from, ());
    }

    fn pending<T>(&self, expected: HostId, key: PendingKey) -> (Pending<T>, PeerReply<T>) {
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
        (
            Pending {
                expected,
                generation,
                reply: send,
            },
            PeerReply {
                receive,
                broker: Rc::downgrade(&self.broker),
                key,
                generation,
            },
        )
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
        match key {
            PendingKey::Page(key) => remove_generation(&mut self.pages, &key, generation),
            PendingKey::Leaf(key) => remove_generation(&mut self.leaves, &key, generation),
            PendingKey::Migration(key) => {
                remove_generation(&mut self.migrations, &key, generation);
            }
            PendingKey::Status(key) => {
                remove_group_generation(&mut self.replica_status, &key, generation);
            }
            PendingKey::Put(key) => {
                remove_group_generation(&mut self.replica_put, &key, generation);
            }
            PendingKey::Commit(key) => {
                remove_group_generation(&mut self.replica_commit, &key, generation);
            }
            PendingKey::Adoption(key) => {
                remove_generation(&mut self.adoptions, &key, generation);
            }
            PendingKey::VnodeClosure(key) => {
                remove_generation(&mut self.vnode_closures, &key, generation);
            }
            PendingKey::VnodeCommit(key) => {
                remove_generation(&mut self.vnode_commits, &key, generation);
            }
        }
    }
}

fn resolve<K: Ord, T>(
    pending: &mut BTreeMap<K, Pending<T>>,
    key: &K,
    from: HostId,
    value: T,
) -> bool {
    if pending.get(key).is_some_and(|entry| entry.expected == from)
        && let Some(entry) = pending.remove(key)
    {
        let _ = entry.reply.send(value);
        return true;
    }
    false
}

fn resolve_group<K: Ord, T: Clone>(
    pending: &mut BTreeMap<K, PendingGroup<T>>,
    key: &K,
    from: HostId,
    value: T,
) -> bool {
    if !pending
        .get(key)
        .is_some_and(|entries| entries.values().all(|entry| entry.expected == from))
    {
        return false;
    }
    let Some(entries) = pending.remove(key) else {
        return false;
    };
    for entry in entries.into_values() {
        let _ = entry.reply.send(value.clone());
    }
    true
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

fn remove_generation<K: Ord, T>(pending: &mut BTreeMap<K, Pending<T>>, key: &K, generation: u64) {
    if pending
        .get(key)
        .is_some_and(|entry| entry.generation == generation)
    {
        pending.remove(key);
    }
}

fn remove_group_generation<K: Ord, T>(
    pending: &mut BTreeMap<K, PendingGroup<T>>,
    key: &K,
    generation: u64,
) {
    let remove_group = if let Some(entries) = pending.get_mut(key) {
        entries.remove(&generation);
        entries.is_empty()
    } else {
        false
    };
    if remove_group {
        pending.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use blockd_exec::{Executor, join2};

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

    #[test]
    fn page_reply_requires_the_authenticated_source_and_resolves_once() {
        let client = PeerClient::default();
        let (request, receive) = client.page(HostId(2));

        client.resolve_page(request, HostId(3), Some(vec![3]));
        client.resolve_page(request, HostId(2), Some(vec![2]));
        client.resolve_page(request, HostId(2), Some(vec![4]));

        let mut executor = Executor::simulation(1);
        assert_eq!(executor.block_on(receive), Ok(Some(vec![2])));
    }

    #[test]
    fn cancellation_closes_the_reply_and_ignores_late_delivery() {
        let client = PeerClient::default();
        let (request, receive) = client.leaf(HostId(2));

        drop(receive);
        assert!(client.broker.borrow().leaves.is_empty());
        client.resolve_leaf(request, HostId(2), Some(vec![2]));
        assert!(client.broker.borrow().leaves.is_empty());
    }

    #[test]
    fn stale_waiter_cleanup_cannot_remove_a_newer_retry() {
        let client = PeerClient::default();
        let stale = client.status(HostId(2), VsetId(7), 11);
        let current = client.status(HostId(2), VsetId(7), 11);

        drop(stale);
        client.resolve_status(HostId(3), VsetId(7), 11, None);
        assert!(
            client
                .broker
                .borrow()
                .replica_status
                .contains_key(&(HostId(2), VsetId(7), 11))
        );
        client.resolve_status(HostId(2), VsetId(7), 11, None);

        let mut executor = Executor::simulation(2);
        assert_eq!(executor.block_on(current), Ok(None));
    }

    #[test]
    fn equivalent_replica_rpcs_to_one_host_resolve_every_waiter() {
        let client = PeerClient::default();
        let first = client.status(HostId(2), VsetId(7), 11);
        let second = client.status(HostId(2), VsetId(7), 11);
        assert_eq!(client.broker.borrow().replica_status.len(), 1);
        assert_eq!(
            client.broker.borrow().replica_status[&(HostId(2), VsetId(7), 11)].len(),
            2
        );

        client.resolve_status(HostId(2), VsetId(7), 11, None);
        let mut executor = Executor::simulation(6);
        assert_eq!(
            executor.block_on(join2(first, second)),
            (Ok(None), Ok(None))
        );
        assert!(client.broker.borrow().replica_status.is_empty());
    }

    #[test]
    fn equivalent_replica_rpcs_to_different_hosts_keep_independent_waiters() {
        let client = PeerClient::default();
        let first = client.status(HostId(2), VsetId(7), 11);
        let second = client.status(HostId(3), VsetId(7), 11);
        assert_eq!(client.broker.borrow().replica_status.len(), 2);

        client.resolve_status(HostId(3), VsetId(7), 11, None);
        client.resolve_status(HostId(2), VsetId(7), 11, None);
        let mut executor = Executor::simulation(5);
        assert_eq!(
            executor.block_on(join2(first, second)),
            (Ok(None), Ok(None))
        );
    }

    #[test]
    fn bounded_retry_cleans_each_timed_out_waiter() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::Silent,
        });
        let mut executor = Executor::simulation(3);
        let error = executor
            .block_on({
                let client = client.clone();
                let peers = Rc::clone(&peers);
                async move {
                    client
                        .replica_status(peers.as_ref(), HostId(2), VsetId(7), 11, 1)
                        .await
                }
            })
            .expect_err("three unanswered attempts time out");

        assert_eq!(error, PeerRpcError { attempts: 3 });
        assert_eq!(peers.sends.get(), 3);
        assert!(client.broker.borrow().replica_status.is_empty());
    }

    #[test]
    fn migration_retry_and_duplicate_accept_are_encapsulated() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::AcceptMigrationOnSecondOffer,
        });
        let mut executor = Executor::simulation(4);
        let accepted = executor.block_on({
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
                            1,
                        )
                        .await
                    {
                        break true;
                    }
                }
            }
        });

        assert!(accepted);
        assert_eq!(peers.sends.get(), 2);
        let broker = client.broker.borrow();
        assert!(broker.migrations.is_empty());
        assert!(broker.active_migrations.is_empty());
        assert!(broker.accepted_migrations.is_empty());
    }

    #[test]
    fn one_migration_offer_times_out_and_releases_its_waiter() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::Silent,
        });
        let mut executor = Executor::simulation(7);
        let accepted = executor.block_on({
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
                        1,
                    )
                    .await
            }
        });

        assert!(!accepted);
        assert_eq!(peers.sends.get(), 1);
        let broker = client.broker.borrow();
        assert!(broker.migrations.is_empty());
        assert!(broker.active_migrations.is_empty());
    }

    #[test]
    fn delayed_prior_accept_cannot_complete_a_new_offer() {
        let client = PeerClient::default();
        let peers = Rc::new(TestPeers {
            client: client.clone(),
            sends: Cell::new(0),
            mode: ReplyMode::DeliverStaleMigrationAccept,
        });
        let mut executor = Executor::simulation(8);
        let accepted = executor.block_on({
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
                        1,
                    )
                    .await
            }
        });

        assert!(!accepted, "an accept from an older handoff must be ignored");
    }
}
