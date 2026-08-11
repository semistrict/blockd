use std::collections::BTreeSet;
use std::rc::Rc;

use blockd_exec::delay;

use super::SharedHost;
use super::capture::finish_creation;
use super::state::PublicationOwner;
use crate::head::{HeadRecord, ManifestPtr, StashAssignment};
use crate::journal::{JournalRecord, VsetConfig};
use crate::layout;
use crate::mapleaf::MapLeaf;
use crate::protocol::{AdminError, AdminEvent, AdminResult, AdminSuccess, StoreFault};
use crate::segment::scan_segment;
use crate::types::{JournalSeq, SegId, VsetId};
use crate::world::{AdminIo, Blobs, GuestMem, Store, StoreError};

pub async fn create_backed<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    config: VsetConfig,
) -> Option<AdminResult>
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let duplicate = state.borrow().vsets.contains_key(&vset);
    if duplicate {
        return Some(Err(AdminError::Busy));
    }
    let incarnation = state.borrow_mut().insert_fresh(vset, config);
    let Some(version) = claim_new_head(&state, world.as_ref(), vset, incarnation).await else {
        state.borrow_mut().vsets.remove(&vset);
        return Some(Err(AdminError::Rejected));
    };
    {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|state| state.incarnation == incarnation)?;
        vset_state.fence = version;
        vset_state.head_version = Some(version);
        host.counters.assignment_claims += 1;
    }
    if !finish_creation(Rc::clone(&state), world.as_ref(), vset, incarnation).await {
        state.borrow_mut().fail("backed journal creation failed");
        return None;
    }
    publish_latest(Rc::clone(&state), Rc::clone(&world), vset).await;
    let published = state.borrow().vsets.get(&vset).is_some_and(|vset_state| {
        let Some(record) = vset_state.best_record.as_ref() else {
            return false;
        };
        vset_state.backed.is_some_and(|pointer| {
            (pointer.capture_seq, pointer.seq) == (record.capture_seq, record.seq)
        })
    });
    if published {
        Some(Ok(AdminSuccess::VsetCreated { vset }))
    } else {
        Some(Err(AdminError::Unavailable))
    }
}

pub(super) async fn claim_new_head<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
) -> Option<u64> {
    claim_new_head_with_stash(state, world, vset, incarnation, None).await
}

pub(super) async fn claim_new_head_with_stash<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
    stash: Option<StashAssignment>,
) -> Option<u64> {
    let head = HeadRecord {
        vset,
        holder: state.borrow().config.host,
        fence: 0,
        manifest: None,
        stash,
        retired_stashes: Vec::new(),
    };
    let retry = state.borrow().config.backup_retry;
    loop {
        match Store::put_cas(world, layout::head_key(vset), None, head.encode()).await {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                state.borrow_mut().counters.assignment_claim_conflicts += 1;
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let ours = HeadRecord::decode(vset, &bytes).is_ok_and(|found| {
                            found.holder == state.borrow().config.host
                                && found.fence == 0
                                && found.manifest.is_none()
                                && found.stash == stash
                        });
                        if ours {
                            return Some(version);
                        }
                    }
                    Ok(None)
                    | Err(
                        StoreError::Fault(StoreFault::Unavailable | StoreFault::CasConflict { .. })
                        | StoreError::TooLarge,
                    ) => {}
                }
                return None;
            }
            Err(StoreError::TooLarge) => return None,
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let recovered = HeadRecord::decode(vset, &bytes).ok();
                        let ours = recovered.is_some_and(|found| {
                            found.holder == state.borrow().config.host
                                && found.fence == 0
                                && found.manifest.is_none()
                                && found.stash == stash
                        });
                        if ours
                            && state
                                .borrow()
                                .vsets
                                .get(&vset)
                                .is_some_and(|vset| vset.incarnation == incarnation)
                        {
                            return Some(version);
                        }
                        return None;
                    }
                    Ok(None) | Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        delay(retry).await;
                    }
                    Err(
                        StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge,
                    ) => return None,
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
pub async fn publish_latest<W>(state: SharedHost, world: Rc<W>, vset: VsetId)
where
    W: Blobs + Store + GuestMem + AdminIo + 'static,
{
    let Some((incarnation, retry)) = ({
        let mut host = state.borrow_mut();
        let retry = host.config.backup_retry;
        host.vsets.get_mut(&vset).and_then(|vset_state| {
            vset_state
                .operations
                .try_start_publication(PublicationOwner::Direct)
                .then_some((vset_state.incarnation, retry))
        })
    }) else {
        return;
    };
    let lease = PublishLease::new(&state, vset, incarnation);
    'publish: while let Some(snapshot) = publish_snapshot(&state, vset, incarnation) {
        if snapshot.backed.is_some_and(|backed| {
            (backed.capture_seq, backed.seq) >= (snapshot.pointer.capture_seq, snapshot.pointer.seq)
        }) {
            break;
        }
        let pending_key =
            layout::pending_manifest_key(vset, snapshot.pointer.fence, snapshot.pointer.seq);
        if put_retry(
            &state,
            world.as_ref(),
            pending_key.clone(),
            snapshot.record.clone(),
            retry,
        )
        .await
        .is_none()
        {
            state.borrow_mut().fail("pending manifest write failed");
            return;
        }
        for (fence, segment) in snapshot.segments {
            let name = layout::segment_blob(vset, fence, segment);
            let Some(bytes) = read_blob_retry(world.as_ref(), &name, retry).await else {
                delay(retry).await;
                continue 'publish;
            };
            if !scan_segment(&bytes).is_ok_and(|(found_vset, found_fence, found_segment, _)| {
                (found_vset, found_fence, found_segment) == (vset, fence, segment)
            }) {
                delay(retry).await;
                continue 'publish;
            }
            if put_retry(
                &state,
                world.as_ref(),
                layout::segment_key(vset, fence, segment),
                bytes,
                retry,
            )
            .await
            .is_none()
            {
                state.borrow_mut().fail("segment backup failed");
                return;
            }
            if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                vset_state.backed_segments.insert((fence, segment));
            }
        }
        for (fence, id) in snapshot.leaves {
            let name = layout::leaf_blob(vset, fence, id);
            let Some(bytes) = read_blob_retry(world.as_ref(), &name, retry).await else {
                delay(retry).await;
                continue 'publish;
            };
            if MapLeaf::decode(vset, fence, id, &bytes).is_err() {
                delay(retry).await;
                continue 'publish;
            }
            if put_retry(
                &state,
                world.as_ref(),
                layout::leaf_key(vset, fence, id),
                bytes,
                retry,
            )
            .await
            .is_none()
            {
                state.borrow_mut().fail("map-leaf backup failed");
                return;
            }
            if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                vset_state.backed_leaves.insert((fence, id));
            }
        }
        if put_retry(
            &state,
            world.as_ref(),
            layout::manifest_key(vset, snapshot.pointer.fence, snapshot.pointer.seq),
            snapshot.record,
            retry,
        )
        .await
        .is_none()
        {
            state.borrow_mut().fail("manifest backup failed");
            return;
        }
        match publish_head(
            &state,
            world.as_ref(),
            vset,
            incarnation,
            snapshot.pointer,
            retry,
        )
        .await
        {
            PublishHead::Published(version) => {
                {
                    let mut host = state.borrow_mut();
                    let Some(vset_state) = host
                        .vsets
                        .get_mut(&vset)
                        .filter(|vset| vset.incarnation == incarnation)
                    else {
                        return;
                    };
                    vset_state.head_version = Some(version);
                    vset_state.backed = Some(snapshot.pointer);
                    host.counters.manifests_published += 1;
                }
                let _ = Store::delete(world.as_ref(), &pending_key).await;
            }
            PublishHead::Fenced => {
                fence_vset(&state, world.as_ref(), vset, Some(incarnation)).await;
                return;
            }
            PublishHead::Fatal => {
                state.borrow_mut().fail("head publication failed");
                return;
            }
        }
    }
    lease.commit();
}

#[allow(clippy::too_many_lines)]
pub async fn reconcile_backed_recovery<W>(state: SharedHost, world: Rc<W>, vset: VsetId)
where
    W: Store + GuestMem + AdminIo + 'static,
{
    if let Some(event) = reconcile_backed_recovery_event(state, Rc::clone(&world), vset).await {
        AdminIo::emit_admin_event(world.as_ref(), event).await;
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn reconcile_backed_recovery_event<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
) -> Option<AdminEvent>
where
    W: Store + GuestMem + AdminIo + 'static,
{
    enum RecoveryDecision {
        Fence,
        Outbound,
        Ready(crate::protocol::Verdict),
    }

    let retry = state.borrow().config.backup_retry;
    loop {
        match Store::get(world.as_ref(), &layout::head_key(vset)).await {
            Ok(Some((version, bytes))) => {
                let Ok(head) = HeadRecord::decode(vset, &bytes) else {
                    fence_vset(&state, world.as_ref(), vset, None).await;
                    return None;
                };
                if head.stash.is_none() {
                    let local_host = state.borrow().config.host;
                    let Some(stash) = super::replica::initial_stash(&state, vset) else {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                        return None;
                    };
                    if head.holder != local_host {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                        return None;
                    }
                    let (incarnation, fence) = state
                        .borrow()
                        .vsets
                        .get(&vset)
                        .map(|vset_state| (vset_state.incarnation, vset_state.fence))?;
                    let upgraded = HeadRecord {
                        vset,
                        holder: local_host,
                        fence,
                        manifest: head.manifest,
                        stash: Some(stash),
                        retired_stashes: Vec::new(),
                    };
                    match Store::put_cas(
                        world.as_ref(),
                        layout::head_key(vset),
                        Some(version),
                        upgraded.encode(),
                    )
                    .await
                    {
                        Ok(upgraded_version) => {
                            let peer_published = recover_peer_published(
                                &state,
                                world.as_ref(),
                                vset,
                                upgraded.manifest,
                                retry,
                            )
                            .await;
                            let verdict = {
                                let mut host = state.borrow_mut();
                                let vset_state = host
                                    .vsets
                                    .get_mut(&vset)
                                    .filter(|vset_state| vset_state.incarnation == incarnation)?;
                                vset_state.head_version = Some(upgraded_version);
                                vset_state.backed = upgraded.manifest;
                                vset_state.peer_published = peer_published;
                                if let Some(manifest) = upgraded.manifest {
                                    vset_state
                                        .store_manifests
                                        .insert((manifest.fence, manifest.seq));
                                }
                                vset_state.stash_assignment = upgraded.stash;
                                vset_state.retired_stashes.clear();
                                if vset_state.outbound.is_some() {
                                    None
                                } else {
                                    vset_state.ready = true;
                                    vset_state.operations.take_recovery()
                                }
                            };
                            state.borrow_mut().schedule_vset(vset);
                            return verdict
                                .map(|verdict| AdminEvent::VsetRecovered { vset, verdict });
                        }
                        Err(StoreError::Fault(StoreFault::CasConflict { .. })) => continue,
                        Err(StoreError::Fault(StoreFault::Unavailable)) => {
                            state.borrow_mut().counters.store_retries += 1;
                            delay(retry).await;
                            continue;
                        }
                        Err(StoreError::TooLarge) => {
                            fence_vset(&state, world.as_ref(), vset, None).await;
                            return None;
                        }
                    }
                }
                let expected_membership = state
                    .borrow()
                    .config
                    .replica_placement
                    .as_ref()
                    .map(|placement| placement.membership_epoch);
                if head.stash.map(|stash| stash.membership_epoch) != expected_membership {
                    fence_vset(&state, world.as_ref(), vset, None).await;
                    return None;
                }
                let publication_is_ours = {
                    let host = state.borrow();
                    let local_host = host.config.host;
                    host.vsets.get(&vset).is_some_and(|vset_state| {
                        let local = vset_state
                            .best_record
                            .as_ref()
                            .map_or((0, JournalSeq(0)), |record| {
                                (record.capture_seq, record.seq)
                            });
                        let behind = head
                            .manifest
                            .is_some_and(|manifest| (manifest.capture_seq, manifest.seq) > local);
                        head.holder == local_host
                            && (head.fence == 0 || head.fence == vset_state.fence)
                            && !behind
                    })
                };
                let peer_published = if publication_is_ours {
                    recover_peer_published(&state, world.as_ref(), vset, head.manifest, retry).await
                } else {
                    None
                };
                let decision = {
                    let mut host = state.borrow_mut();
                    let local_host = host.config.host;
                    let vset_state = host.vsets.get_mut(&vset)?;
                    let local = vset_state
                        .best_record
                        .as_ref()
                        .map_or((0, JournalSeq(0)), |record| {
                            (record.capture_seq, record.seq)
                        });
                    let behind = head
                        .manifest
                        .is_some_and(|manifest| (manifest.capture_seq, manifest.seq) > local);
                    if head.holder != local_host
                        || (head.fence != 0 && head.fence != vset_state.fence)
                        || behind
                    {
                        RecoveryDecision::Fence
                    } else {
                        vset_state.head_version = Some(version);
                        vset_state.backed = head.manifest;
                        vset_state.peer_published = peer_published;
                        if let Some(manifest) = head.manifest {
                            vset_state
                                .store_manifests
                                .insert((manifest.fence, manifest.seq));
                        }
                        vset_state.stash_assignment = head.stash;
                        vset_state.retired_stashes = head.retired_stashes;
                        if vset_state.outbound.is_some() {
                            RecoveryDecision::Outbound
                        } else {
                            vset_state.ready = true;
                            RecoveryDecision::Ready(
                                vset_state
                                    .operations
                                    .take_recovery()
                                    .expect("recovery verdict retained"),
                            )
                        }
                    }
                };
                state.borrow_mut().schedule_vset(vset);
                match decision {
                    RecoveryDecision::Fence => {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                    }
                    RecoveryDecision::Outbound => {}
                    RecoveryDecision::Ready(verdict) => {
                        return Some(AdminEvent::VsetRecovered { vset, verdict });
                    }
                }
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Ok(None) => {
                let local_host = state.borrow().config.host;
                let Some(stash) = super::replica::initial_stash(&state, vset) else {
                    fence_vset(&state, world.as_ref(), vset, None).await;
                    return None;
                };
                let head = HeadRecord {
                    vset,
                    holder: local_host,
                    fence: 0,
                    manifest: None,
                    stash: Some(stash),
                    retired_stashes: Vec::new(),
                };
                match Store::put_cas(world.as_ref(), layout::head_key(vset), None, head.encode())
                    .await
                {
                    Ok(version) => {
                        let verdict = {
                            let mut host = state.borrow_mut();
                            let verdict = {
                                let vset_state = host.vsets.get_mut(&vset)?;
                                vset_state.fence = version;
                                vset_state.head_version = Some(version);
                                vset_state.backed = None;
                                vset_state.stash_assignment = Some(stash);
                                vset_state.retired_stashes.clear();
                                if vset_state.outbound.is_some() {
                                    None
                                } else {
                                    vset_state.ready = true;
                                    vset_state.operations.take_recovery()
                                }
                            };
                            host.counters.assignment_claims += 1;
                            verdict
                        };
                        state.borrow_mut().schedule_vset(vset);
                        return verdict.map(|verdict| AdminEvent::VsetRecovered { vset, verdict });
                    }
                    Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                        state.borrow_mut().counters.assignment_claim_conflicts += 1;
                    }
                    Err(StoreError::Fault(StoreFault::Unavailable)) => {
                        state.borrow_mut().counters.store_retries += 1;
                        delay(retry).await;
                    }
                    Err(StoreError::TooLarge) => {
                        fence_vset(&state, world.as_ref(), vset, None).await;
                        return None;
                    }
                }
            }
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                fence_vset(&state, world.as_ref(), vset, None).await;
                return None;
            }
        }
    }
}

async fn recover_peer_published<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    pointer: Option<ManifestPtr>,
    retry: u64,
) -> Option<crate::protocol::ReplicaCommitInfo> {
    let pointer = pointer?;
    if let Some(info) = state.borrow().vsets.get(&vset).and_then(|vset_state| {
        let record = vset_state.best_record.as_ref()?;
        ((pointer.fence, pointer.seq) == (record.fence, record.seq)).then_some(
            crate::protocol::ReplicaCommitInfo {
                writer_fence: record.fence,
                seq: record.seq,
                sync_covered_through: record.sync_covered_through,
            },
        )
    }) {
        return Some(info);
    }
    loop {
        match Store::get(
            world,
            &layout::manifest_key(vset, pointer.fence, pointer.seq),
        )
        .await
        {
            Ok(Some((_, bytes))) => {
                let record = JournalRecord::decode(vset, &bytes).ok()?;
                if (record.fence, record.seq) != (pointer.fence, pointer.seq) {
                    return None;
                }
                return Some(crate::protocol::ReplicaCommitInfo {
                    writer_fence: record.fence,
                    seq: record.seq,
                    sync_covered_through: record.sync_covered_through,
                });
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Ok(None)
            | Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
        }
    }
}

struct PublishSnapshot {
    pointer: ManifestPtr,
    record: Vec<u8>,
    segments: Vec<(u64, SegId)>,
    leaves: Vec<(u64, u64)>,
    backed: Option<ManifestPtr>,
}

fn publish_snapshot(state: &SharedHost, vset: VsetId, incarnation: u64) -> Option<PublishSnapshot> {
    let host = state.borrow();
    let vset_state = host
        .vsets
        .get(&vset)
        .filter(|vset| vset.incarnation == incarnation)?;
    let record = vset_state.best_record.as_ref()?;
    let pointer = ManifestPtr {
        fence: record.fence,
        seq: record.seq,
        capture_seq: record.capture_seq,
    };
    let mut segments = record
        .overlay
        .values()
        .filter(|(_, location)| location.base == 0)
        .map(|(_, location)| (location.fence, location.seg))
        .collect::<BTreeSet<_>>();
    let mut leaves = BTreeSet::new();
    for pointer in record.leaves.values().filter(|pointer| pointer.base == 0) {
        leaves.insert((pointer.fence, pointer.id));
        if let Some((_, leaf_segments)) = vset_state.leaf_blobs.get(pointer) {
            segments.extend(leaf_segments);
        }
    }
    segments.retain(|segment| !vset_state.backed_segments.contains(segment));
    leaves.retain(|leaf| !vset_state.backed_leaves.contains(leaf));
    Some(PublishSnapshot {
        pointer,
        record: record.encode(vset),
        segments: segments.into_iter().collect(),
        leaves: leaves.into_iter().collect(),
        backed: vset_state.backed,
    })
}

async fn read_blob_retry<W: Blobs>(world: &W, name: &str, retry: u64) -> Option<Vec<u8>> {
    loop {
        match Blobs::read(world, name).await {
            Ok(Some(bytes)) => return Some(bytes),
            Ok(None) => return None,
            Err(_) => delay(retry).await,
        }
    }
}

async fn put_retry<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    retry: u64,
) -> Option<u64> {
    loop {
        match Store::put(world, key.clone(), bytes.clone()).await {
            Ok(version) => return Some(version),
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
            Err(StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge) => {
                return None;
            }
        }
    }
}

enum PublishHead {
    Published(u64),
    Fenced,
    Fatal,
}

async fn publish_head<W: Store>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: u64,
    pointer: ManifestPtr,
    retry: u64,
) -> PublishHead {
    loop {
        let Some((expected, head)) = ({
            let host = state.borrow();
            host.vsets
                .get(&vset)
                .filter(|vset| vset.incarnation == incarnation)
                .and_then(|vset_state| {
                    Some((
                        vset_state.head_version?,
                        HeadRecord {
                            vset,
                            holder: host.config.host,
                            fence: vset_state.fence,
                            manifest: Some(pointer),
                            stash: None,
                            retired_stashes: Vec::new(),
                        },
                    ))
                })
        }) else {
            return PublishHead::Fenced;
        };
        match Store::put_cas(world, layout::head_key(vset), Some(expected), head.encode()).await {
            Ok(version) => return PublishHead::Published(version),
            Err(StoreError::Fault(StoreFault::CasConflict { .. })) => {
                return PublishHead::Fenced;
            }
            Err(StoreError::TooLarge) => return PublishHead::Fatal,
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                match Store::get(world, &layout::head_key(vset)).await {
                    Ok(Some((version, bytes))) => {
                        let Ok(found) = HeadRecord::decode(vset, &bytes) else {
                            return PublishHead::Fatal;
                        };
                        if found.holder != head.holder || found.fence != head.fence {
                            return PublishHead::Fenced;
                        }
                        if found.manifest.is_some_and(|manifest| {
                            (manifest.capture_seq, manifest.seq)
                                >= (pointer.capture_seq, pointer.seq)
                        }) {
                            return PublishHead::Published(version);
                        }
                        if let Some(vset_state) = state.borrow_mut().vsets.get_mut(&vset) {
                            vset_state.head_version = Some(version);
                        }
                    }
                    Ok(None) => return PublishHead::Fenced,
                    Err(StoreError::Fault(StoreFault::Unavailable)) => delay(retry).await,
                    Err(
                        StoreError::Fault(StoreFault::CasConflict { .. }) | StoreError::TooLarge,
                    ) => return PublishHead::Fatal,
                }
            }
        }
    }
}

async fn fence_vset<W: GuestMem>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    incarnation: Option<u64>,
) {
    let pages = {
        let mut host = state.borrow_mut();
        if host
            .vsets
            .get(&vset)
            .is_none_or(|vset| incarnation.is_some_and(|expected| vset.incarnation != expected))
        {
            return;
        }
        host.vsets.remove(&vset);
        host.counters.fenced += 1;
        host.cache.purge_vset(vset)
    };
    if GuestMem::fence(world, vset).await.is_err() {
        state.borrow_mut().fail("guest fence notification failed");
        return;
    }
    for page in pages {
        if GuestMem::evict(world, page).await.is_err() {
            state.borrow_mut().fail("fenced guest page eviction failed");
            return;
        }
    }
}

struct PublishLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
    active: bool,
}

impl PublishLease {
    fn new(state: &SharedHost, vset: VsetId, incarnation: u64) -> Self {
        Self {
            state: Rc::clone(state),
            vset,
            incarnation,
            active: true,
        }
    }

    fn commit(mut self) {
        if let Some(vset) = self
            .state
            .borrow_mut()
            .vsets
            .get_mut(&self.vset)
            .filter(|vset| vset.incarnation == self.incarnation)
        {
            vset.operations.finish_publication(PublicationOwner::Direct);
        }
        self.state.borrow_mut().schedule_vset(self.vset);
        self.active = false;
    }
}

impl Drop for PublishLease {
    fn drop(&mut self) {
        if self.active
            && let Some(vset) = self
                .state
                .borrow_mut()
                .vsets
                .get_mut(&self.vset)
                .filter(|vset| vset.incarnation == self.incarnation)
        {
            vset.operations.finish_publication(PublicationOwner::Direct);
        }
        self.state.borrow_mut().schedule_vset(self.vset);
    }
}
