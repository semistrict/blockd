use std::rc::Rc;

use blockd_exec::channel::oneshot;
use blockd_exec::delay;

use super::state::{CacheReservation, PageFillLease, SharedHost};
use super::{hydrate_mapping, peer_fetch_page, peer_fetch_replica_page};
use crate::blx::{BlockKey, EntryKind, NamespaceKind, open_footer};
use crate::format::checksum64;
use crate::journal::VsetKind;
use crate::layout;
use crate::manifest::ObjectRef;
use crate::segment::{PageLoc, open_entry};
use crate::types::{Gen, PageId, VsetId, page_size};
use crate::world::{Blobs, FillSource, GuestMem, Peers, Store, StoreError};

#[derive(Clone, Copy)]
struct FetchPlan {
    generation: Gen,
    location: PageLoc,
    backed: bool,
    source: Option<crate::types::HostId>,
    retry_delay: u64,
}

pub async fn serve_fault<W>(state: SharedHost, world: Rc<W>, page: PageId, write: bool)
where
    W: Blobs + Store + Peers + GuestMem + 'static,
{
    let incarnation = {
        let mut host = state.borrow_mut();
        if let Some(vset) = host.vsets.get(&page.volume.vset) {
            if vset.ready && vset.config.kind == VsetKind::Compute && vset.config.contains(page) {
                Some(vset.incarnation)
            } else {
                host.counters.guest_rejected += 1;
                None
            }
        } else {
            host.counters.guest_rejected += 1;
            None
        }
    };
    let Some(incarnation) = incarnation else {
        let _ = GuestMem::fail(world.as_ref(), page).await;
        state.borrow_mut().fail("unservable guest page");
        return;
    };

    let resident = state.borrow().cache.is_resident(page);
    if resident {
        serve_resident_fault(&state, world.as_ref(), page, write, incarnation).await;
    } else {
        serve_missing_fault(state, world, page, write, incarnation).await;
    }
}

async fn serve_resident_fault<W: GuestMem>(
    state: &SharedHost,
    world: &W,
    page: PageId,
    write: bool,
    incarnation: u64,
) {
    let copy_on_fault = write
        && state
            .borrow()
            .vsets
            .get(&page.volume.vset)
            .filter(|vset| vset.incarnation == incarnation)
            .and_then(|vset| vset.operations.drain())
            .is_some_and(|drain| drain.unread.contains_key(&page));
    if copy_on_fault {
        let bytes = GuestMem::read_page(world, page).await;
        let mut host = state.borrow_mut();
        let Some(vset) = host
            .vsets
            .get_mut(&page.volume.vset)
            .filter(|vset| vset.incarnation == incarnation)
        else {
            return;
        };
        let copied = vset.operations.drain_mut().is_some_and(|drain| {
            drain.unread.remove(&page).is_some_and(|generation| {
                drain
                    .copied_on_fault
                    .insert(page, (generation, bytes))
                    .is_none()
            })
        });
        if copied {
            host.counters.cow_captures += 1;
        }
    }

    if write {
        {
            let mut host = state.borrow_mut();
            let valid = host
                .vsets
                .get(&page.volume.vset)
                .is_some_and(|vset| vset.incarnation == incarnation && vset.ready);
            if !valid || !host.cache.is_resident(page) {
                return;
            }
            if !host.cache.is_dirty(page) {
                host.cache.mark_dirty(page);
                host.vsets
                    .get_mut(&page.volume.vset)
                    .expect("validated vset")
                    .mutation_seq += 1;
                host.counters.wp_faults += 1;
                host.counters.guest_pages_dirtied += 1;
            }
            host.schedule_vset(page.volume.vset);
        }
        if GuestMem::unprotect(world, page).await.is_err() {
            state.borrow_mut().fail("guest page unprotect failed");
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn serve_missing_fault<W>(
    state: SharedHost,
    world: Rc<W>,
    page: PageId,
    write: bool,
    incarnation: u64,
) where
    W: Blobs + Store + Peers + GuestMem + 'static,
{
    enum Slot {
        Ready {
            location: Option<(Gen, PageLoc)>,
            memory: bool,
            backed: bool,
            source: Option<crate::types::HostId>,
            victim: Option<PageId>,
        },
        Shared {
            share: crate::cache::BaseKey,
            memory: bool,
            victim: Option<PageId>,
        },
        Wait(blockd_exec::channel::OneReceiver<()>),
        Filling(blockd_exec::channel::OneReceiver<bool>),
        Gone,
    }

    if hydrate_mapping(&state, world.as_ref(), page, incarnation)
        .await
        .is_err()
        || resolve_archive_mapping(&state, world.as_ref(), page, incarnation)
            .await
            .is_err()
    {
        state.borrow_mut().counters.faults_unservable += 1;
        let _ = GuestMem::fail(world.as_ref(), page).await;
        state.borrow_mut().fail("unservable guest page");
        return;
    }

    loop {
        let slot = {
            let mut host = state.borrow_mut();
            if let Some(vset) = host
                .vsets
                .get(&page.volume.vset)
                .filter(|vset| vset.incarnation == incarnation && vset.ready)
            {
                let location = vset.page_locs.get(&page).copied();
                let memory = vset.config.is_memory(page.volume.idx);
                let backed = true;
                let source = vset.peer_source;
                let shared = location
                    .map(|(_, location)| location)
                    .filter(|location| location.base != 0)
                    .map(|location| (location.base, location.fence, location.seg, location.offset))
                    .filter(|key| host.cache.base_is_resident(*key));
                if host.filling_pages.contains(&page) {
                    let (wake, wait) = oneshot();
                    host.page_fill_waiters.entry(page).or_default().push(wake);
                    Slot::Filling(wait)
                } else if let Some(share) = shared
                    && !write
                {
                    host.filling_pages.insert(page);
                    Slot::Shared {
                        share,
                        memory,
                        victim: None,
                    }
                } else if let Some(victim) = host.cache.reserve_slot() {
                    host.filling_pages.insert(page);
                    if let Some(share) = shared {
                        Slot::Shared {
                            share,
                            memory,
                            victim,
                        }
                    } else {
                        Slot::Ready {
                            location,
                            memory,
                            backed,
                            source,
                            victim,
                        }
                    }
                } else {
                    let (wake, wait) = oneshot();
                    host.pressure_waiters.push_back(wake);
                    host.counters.pressure_waits += 1;
                    Slot::Wait(wait)
                }
            } else {
                Slot::Gone
            }
        };
        let (location, memory, backed, source, victim) = match slot {
            Slot::Ready {
                location,
                memory,
                backed,
                source,
                victim,
            } => (location, memory, backed, source, victim),
            Slot::Shared {
                share,
                memory,
                victim,
            } => {
                let fill_lease = PageFillLease::new(&state, page);
                let reservation = write.then(|| CacheReservation::new(&state));
                if let Some(victim) = victim
                    && GuestMem::evict(world.as_ref(), victim).await.is_err()
                {
                    state.borrow_mut().fail("guest page eviction failed");
                    return;
                }
                if write {
                    let mut host = state.borrow_mut();
                    if !same_incarnation(&host, page, incarnation) {
                        return;
                    }
                    host.cache.fill_slot(page, true, memory);
                    host.vsets
                        .get_mut(&page.volume.vset)
                        .expect("validated vset")
                        .mutation_seq += 1;
                    host.counters.guest_pages_dirtied += 1;
                    host.schedule_vset(page.volume.vset);
                    host.wake_pressure_waiter();
                }
                {
                    let mut host = state.borrow_mut();
                    host.counters.shared_fills += 1;
                    host.vsets
                        .get_mut(&page.volume.vset)
                        .expect("validated vset")
                        .wedge
                        .fills += 1;
                }
                if let Some(reservation) = reservation {
                    reservation.commit();
                }
                if GuestMem::fill_shared(world.as_ref(), page, share, None, write)
                    .await
                    .is_err()
                {
                    state.borrow_mut().fail("guest shared-page fill failed");
                    return;
                }
                fill_lease.finish(true);
                return;
            }
            Slot::Wait(wait) => {
                if wait.await.is_err() {
                    return;
                }
                continue;
            }
            Slot::Filling(wait) => {
                let _ = wait.await;
                return;
            }
            Slot::Gone => return,
        };
        let fill_lease = PageFillLease::new(&state, page);
        let reservation = CacheReservation::new(&state);
        if let Some(victim) = victim
            && GuestMem::evict(world.as_ref(), victim).await.is_err()
        {
            state.borrow_mut().fail("guest page eviction failed");
            return;
        }

        let Some((generation, location)) = location else {
            {
                let mut host = state.borrow_mut();
                if !same_incarnation(&host, page, incarnation) {
                    return;
                }
                host.cache.fill_slot(page, write, memory);
                if write {
                    host.vsets
                        .get_mut(&page.volume.vset)
                        .expect("validated vset")
                        .mutation_seq += 1;
                    host.counters.guest_pages_dirtied += 1;
                    host.schedule_vset(page.volume.vset);
                }
                host.counters.zero_fills += 1;
                host.vsets
                    .get_mut(&page.volume.vset)
                    .expect("validated vset")
                    .wedge
                    .fills += 1;
            }
            reservation.commit();
            if GuestMem::fill(
                world.as_ref(),
                page,
                vec![0; page_size()],
                write,
                FillSource::Zero,
            )
            .await
            .is_err()
            {
                state.borrow_mut().fail("guest zero-page fill failed");
                return;
            }
            fill_lease.finish(true);
            return;
        };

        let retry_delay = state.borrow().config.backup_retry;
        let bytes = fetch_page(
            &state,
            world.as_ref(),
            page,
            FetchPlan {
                generation,
                location,
                backed,
                source,
                retry_delay,
            },
        )
        .await;
        let Some((raw, fill_source)) = bytes else {
            let advanced = state
                .borrow()
                .vsets
                .get(&page.volume.vset)
                .filter(|vset| vset.incarnation == incarnation)
                .and_then(|vset| vset.page_locs.get(&page))
                .is_some_and(|current| *current != (generation, location));
            drop(reservation);
            if advanced {
                continue;
            }
            state.borrow_mut().counters.faults_unservable += 1;
            let _ = GuestMem::fail(world.as_ref(), page).await;
            state.borrow_mut().fail("unservable guest page");
            return;
        };
        {
            let mut host = state.borrow_mut();
            if !same_incarnation(&host, page, incarnation) {
                return;
            }
            let current = host.vsets[&page.volume.vset].page_locs.get(&page).copied();
            if current != Some((generation, location)) {
                drop(host);
                drop(reservation);
                continue;
            }
            let share = (location.base != 0 && !write).then_some((
                location.base,
                location.fence,
                location.seg,
                location.offset,
            ));
            if let Some(share) = share {
                host.cache.base_insert(share);
                host.counters.shared_fills += 1;
            } else {
                host.cache.fill_slot(page, write, memory);
            }
            let kind = host.vsets[&page.volume.vset].config.kind;
            host.vsets
                .get_mut(&page.volume.vset)
                .expect("validated vset")
                .block_checksums
                .insert(
                    BlockKey::from_page(kind, page),
                    (generation, checksum64(&raw)),
                );
            if write {
                host.vsets
                    .get_mut(&page.volume.vset)
                    .expect("validated vset")
                    .mutation_seq += 1;
                host.counters.guest_pages_dirtied += 1;
                host.schedule_vset(page.volume.vset);
            }
            host.counters.fills += 1;
            host.vsets
                .get_mut(&page.volume.vset)
                .expect("validated vset")
                .wedge
                .fills += 1;
        }
        reservation.commit();
        if let Some(share) = (location.base != 0 && !write).then_some((
            location.base,
            location.fence,
            location.seg,
            location.offset,
        )) {
            if GuestMem::fill_shared(world.as_ref(), page, share, Some(raw), false)
                .await
                .is_err()
            {
                state.borrow_mut().fail("guest shared-page fill failed");
                return;
            }
        } else if GuestMem::fill(world.as_ref(), page, raw, write, fill_source)
            .await
            .is_err()
        {
            state.borrow_mut().fail("guest page fill failed");
            return;
        }
        fill_lease.finish(true);
        return;
    }
}

fn same_incarnation(host: &super::state::HostState, page: PageId, incarnation: u64) -> bool {
    host.vsets
        .get(&page.volume.vset)
        .is_some_and(|vset| vset.incarnation == incarnation && vset.ready)
}

async fn fetch_page<W: Blobs + Store + Peers>(
    state: &SharedHost,
    world: &W,
    page: PageId,
    plan: FetchPlan,
) -> Option<(Vec<u8>, FillSource)> {
    let FetchPlan {
        generation,
        location,
        backed,
        source,
        retry_delay,
    } = plan;
    let belongs_to_source = location.base == 0
        && source.is_some()
        && state
            .borrow()
            .vsets
            .get(&page.volume.vset)
            .is_some_and(|vset| location.fence < vset.fence);
    if !belongs_to_source {
        let local_name = layout::segment_blob(page.volume.vset, location.fence, location.seg);
        if let Ok(Some(bytes)) = Blobs::read_range(
            world,
            &local_name,
            u64::from(location.offset),
            u64::from(location.len),
        )
        .await
            && let Some(raw) = verify_entry(page, generation, Some(bytes))
        {
            return Some((raw, FillSource::Local));
        }
    }
    if location.base == 0
        && let Some(source) = source
        && let Some(bytes) = peer_fetch_page(state, world, source, page.volume.vset, location).await
        && let Some(raw) = verify_entry(page, generation, Some(bytes))
    {
        return Some((raw, FillSource::Peer));
    }
    let replica = state
        .borrow()
        .vsets
        .get(&page.volume.vset)
        .and_then(|vset| vset.stash_assignment)
        .map(|stash| {
            (
                stash.transition_peer.unwrap_or(stash.active_peer),
                stash.assignment_epoch,
            )
        });
    if location.base == 0
        && let Some((passive, assignment_epoch)) = replica
        && let Some(bytes) = peer_fetch_replica_page(
            state,
            world,
            passive,
            assignment_epoch,
            page.volume.vset,
            location,
        )
        .await
        && let Some(raw) = verify_entry(page, generation, Some(bytes))
    {
        return Some((raw, FillSource::Peer));
    }
    if !backed && location.base == 0 {
        return None;
    }
    let archive_key = state
        .borrow()
        .vsets
        .get(&page.volume.vset)
        .and_then(|vset| {
            vset.archive_objects.iter().find(|object| {
                object.identity.writer_fence == location.fence
                    && object.identity.object_id == location.seg.0
                    && if location.base == 0 {
                        object.identity.namespace_kind == NamespaceKind::Vset
                            && object.identity.namespace_id == page.volume.vset.0
                    } else {
                        object.identity.namespace_id == location.base
                    }
            })
        })
        .map(|object| object.identity.store_key());
    let key = archive_key.unwrap_or_else(|| {
        if location.base == 0 {
            layout::segment_key(page.volume.vset, location.fence, location.seg)
        } else {
            layout::blx_key(VsetId(location.base), location.fence, location.seg.0)
        }
    });
    loop {
        match Store::get_range(
            world,
            &key,
            u64::from(location.offset),
            u64::from(location.len),
        )
        .await
        {
            Ok(Some((_, bytes))) => {
                return verify_entry(page, generation, Some(bytes))
                    .map(|raw| (raw, FillSource::Store));
            }
            Ok(None) | Err(StoreError::TooLarge) => return None,
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                delay(retry_delay).await;
            }
            Err(StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. })) => {
                return None;
            }
        }
    }
}

/// Resolve one archived page without materializing a durable or in-memory map
/// for the whole vset. Object key ranges select at most the configured overlap
/// bound; only those footers are fetched, and each verified footer is cached.
async fn resolve_archive_mapping<W: Store>(
    state: &SharedHost,
    world: &W,
    page: PageId,
    incarnation: u64,
) -> Result<(), ()> {
    enum Action {
        Ready,
        Fetch { object: ObjectRef, retry: u64 },
        Resolve,
    }

    loop {
        let action = {
            let host = state.borrow();
            let Some(vset) = host
                .vsets
                .get(&page.volume.vset)
                .filter(|vset| vset.incarnation == incarnation && vset.ready)
            else {
                return Err(());
            };
            if vset.page_locs.contains_key(&page) || vset.archive_resolved_pages.contains(&page) {
                Action::Ready
            } else if !vset.archived_memory_usable && vset.config.is_memory(page.volume.idx) {
                Action::Resolve
            } else {
                let key = BlockKey::from_page(vset.config.kind, page);
                let candidate = vset
                    .archive_objects
                    .iter()
                    .filter(|object| object.first_key <= key && key <= object.last_key)
                    .find(|object| !vset.archive_footers.contains_key(&object.identity))
                    .copied();
                candidate.map_or(Action::Resolve, |object| Action::Fetch {
                    object,
                    retry: host.config.backup_retry,
                })
            }
        };
        match action {
            Action::Ready => return Ok(()),
            Action::Fetch { object, retry } => {
                let bytes = loop {
                    match Store::get_range(
                        world,
                        &object.identity.store_key(),
                        u64::from(object.footer_offset),
                        u64::from(object.footer_length),
                    )
                    .await
                    {
                        Ok(Some((_, bytes))) => break bytes,
                        Ok(None)
                        | Err(StoreError::TooLarge)
                        | Err(StoreError::Fault(crate::protocol::StoreFault::CasConflict {
                            ..
                        })) => return Err(()),
                        Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                            delay(retry).await;
                        }
                    }
                };
                let footer = open_footer(&bytes).map_err(|_| ())?;
                let valid = footer.entries.first().is_some_and(|entry| {
                    entry.key == object.first_key
                        && footer
                            .entries
                            .last()
                            .is_some_and(|last| last.key == object.last_key)
                }) && footer.entries.iter().all(|entry| {
                    entry
                        .offset
                        .checked_add(entry.length)
                        .is_some_and(|end| end <= object.footer_offset)
                });
                if !valid {
                    return Err(());
                }
                let mut host = state.borrow_mut();
                let Some(vset) = host
                    .vsets
                    .get_mut(&page.volume.vset)
                    .filter(|vset| vset.incarnation == incarnation && vset.ready)
                else {
                    return Err(());
                };
                if vset.archive_objects.contains(&object) {
                    vset.archive_footers.insert(object.identity, footer);
                }
            }
            Action::Resolve => {
                let mut host = state.borrow_mut();
                let Some(vset) = host
                    .vsets
                    .get_mut(&page.volume.vset)
                    .filter(|vset| vset.incarnation == incarnation && vset.ready)
                else {
                    return Err(());
                };
                if vset.page_locs.contains_key(&page) || vset.archive_resolved_pages.contains(&page)
                {
                    continue;
                }
                if !vset.archived_memory_usable && vset.config.is_memory(page.volume.idx) {
                    vset.archive_resolved_pages.insert(page);
                    return Ok(());
                }
                let key = BlockKey::from_page(vset.config.kind, page);
                let mut winner = None;
                for object in vset
                    .archive_objects
                    .iter()
                    .filter(|object| object.first_key <= key && key <= object.last_key)
                {
                    let Some(entry) = vset
                        .archive_footers
                        .get(&object.identity)
                        .and_then(|footer| footer.find(key))
                    else {
                        continue;
                    };
                    let own = object.identity.namespace_kind == NamespaceKind::Vset
                        && object.identity.namespace_id == page.volume.vset.0;
                    let replace = winner.as_ref().is_none_or(
                        |(old_entry, old_own, old_object): &(
                            crate::blx::FooterEntry,
                            bool,
                            ObjectRef,
                        )| {
                            (entry.generation, own, object.identity)
                                > (old_entry.generation, *old_own, old_object.identity)
                        },
                    );
                    if replace {
                        winner = Some((entry, own, *object));
                    }
                }
                vset.archive_resolved_pages.insert(page);
                if let Some((entry, own, object)) = winner {
                    vset.next_gen = vset.next_gen.max(entry.generation.0.saturating_add(1));
                    if entry.kind == EntryKind::Data {
                        vset.block_checksums
                            .insert(key, (entry.generation, entry.value_checksum));
                        vset.page_locs.insert(
                            page,
                            (
                                entry.generation,
                                PageLoc {
                                    base: if own { 0 } else { object.identity.namespace_id },
                                    fence: object.identity.writer_fence,
                                    seg: crate::types::SegId(object.identity.object_id),
                                    offset: entry.offset,
                                    len: entry.length,
                                },
                            ),
                        );
                    } else {
                        vset.block_checksums.remove(&key);
                        vset.page_locs.remove(&page);
                        vset.overlay.remove(&page);
                    }
                }
                return Ok(());
            }
        }
    }
}

fn verify_entry(page: PageId, generation: Gen, bytes: Option<Vec<u8>>) -> Option<Vec<u8>> {
    bytes
        .and_then(|bytes| open_entry(page.volume.vset, &bytes).ok())
        .and_then(|(found, found_generation, raw)| {
            (found.volume.idx == page.volume.idx
                && found.page == page.page
                && found_generation == generation)
                .then_some(raw)
        })
}

pub(super) async fn load_page_for_database<W>(
    state: &SharedHost,
    world: &W,
    page: PageId,
    incarnation: u64,
) -> Option<Vec<u8>>
where
    W: Blobs + Store + Peers,
{
    hydrate_mapping(state, world, page, incarnation)
        .await
        .ok()?;
    resolve_archive_mapping(state, world, page, incarnation)
        .await
        .ok()?;
    let (location, backed, source, retry) = {
        let host = state.borrow();
        let vset = host
            .vsets
            .get(&page.volume.vset)
            .filter(|vset| vset.incarnation == incarnation && vset.ready)?;
        (
            vset.page_locs.get(&page).copied(),
            true,
            vset.peer_source,
            host.config.backup_retry,
        )
    };
    let Some((generation, location)) = location else {
        return Some(vec![0; page_size()]);
    };
    let raw = fetch_page(
        state,
        world,
        page,
        FetchPlan {
            generation,
            location,
            backed,
            source,
            retry_delay: retry,
        },
    )
    .await?
    .0;
    if let Some(vset) = state
        .borrow_mut()
        .vsets
        .get_mut(&page.volume.vset)
        .filter(|vset| vset.incarnation == incarnation)
    {
        vset.block_checksums.insert(
            BlockKey::from_page(vset.config.kind, page),
            (generation, checksum64(&raw)),
        );
    }
    Some(raw)
}
