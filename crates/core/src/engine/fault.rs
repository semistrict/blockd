use std::rc::Rc;

use blockd_exec::channel::oneshot;
use blockd_exec::delay;

use super::state::{CacheReservation, PageFillLease, SharedHost};
use super::{hydrate_mapping, peer_fetch_page};
use crate::journal::VsetKind;
use crate::layout;
use crate::segment::{PageLoc, open_entry};
use crate::types::{Gen, PageId, page_size};
use crate::world::{Blobs, FillSource, GuestMem, Peers, Store, StoreError};

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
        GuestMem::fail(world.as_ref(), page).await;
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
            .and_then(|vset| vset.drain.as_ref())
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
        let copied = vset.drain.as_mut().is_some_and(|drain| {
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
    }
    GuestMem::unprotect(world, page).await;
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
        Wait(blockd_exec::channel::OneReceiver<()>),
        Filling(blockd_exec::channel::OneReceiver<bool>),
        Gone,
    }

    if hydrate_mapping(&state, world.as_ref(), page, incarnation)
        .await
        .is_err()
    {
        state.borrow_mut().counters.faults_unservable += 1;
        GuestMem::fail(world.as_ref(), page).await;
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
                let backed = vset.config.durability.uses_store();
                let source = vset.peer_source;
                if host.filling_pages.contains(&page) {
                    let (wake, wait) = oneshot();
                    host.page_fill_waiters.entry(page).or_default().push(wake);
                    Slot::Filling(wait)
                } else if let Some(victim) = host.cache.reserve_slot() {
                    host.filling_pages.insert(page);
                    Slot::Ready {
                        location,
                        memory,
                        backed,
                        source,
                        victim,
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
        if let Some(victim) = victim {
            GuestMem::evict(world.as_ref(), victim).await;
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
                }
                host.counters.zero_fills += 1;
                host.vsets
                    .get_mut(&page.volume.vset)
                    .expect("validated vset")
                    .wedge
                    .fills += 1;
            }
            reservation.commit();
            GuestMem::fill(
                world.as_ref(),
                page,
                vec![0; page_size()],
                write,
                FillSource::Zero,
            )
            .await;
            fill_lease.finish(true);
            return;
        };

        let retry_delay = state.borrow().config.backup_retry;
        let bytes = fetch_page(
            &state,
            world.as_ref(),
            page,
            location,
            backed,
            source,
            retry_delay,
        )
        .await;
        let Some((raw, fill_source)) = bytes.and_then(|(bytes, source)| {
            verify_entry(page, generation, Some(bytes)).map(|raw| (raw, source))
        }) else {
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
            GuestMem::fail(world.as_ref(), page).await;
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
            host.cache.fill_slot(page, write, memory);
            if write {
                host.vsets
                    .get_mut(&page.volume.vset)
                    .expect("validated vset")
                    .mutation_seq += 1;
                host.counters.guest_pages_dirtied += 1;
            }
            host.counters.fills += 1;
            host.vsets
                .get_mut(&page.volume.vset)
                .expect("validated vset")
                .wedge
                .fills += 1;
        }
        reservation.commit();
        GuestMem::fill(world.as_ref(), page, raw, write, fill_source).await;
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
    location: PageLoc,
    backed: bool,
    source: Option<crate::types::HostId>,
    retry_delay: u64,
) -> Option<(Vec<u8>, FillSource)> {
    let local_name = layout::segment_blob(page.volume.vset, location.fence, location.seg);
    if let Ok(Some(bytes)) = Blobs::read_range(
        world,
        &local_name,
        u64::from(location.offset),
        u64::from(location.len),
    )
    .await
    {
        return Some((bytes, FillSource::Local));
    }
    if location.base == 0
        && let Some(source) = source
        && let Some(bytes) = peer_fetch_page(state, world, source, page.volume.vset, location).await
    {
        return Some((bytes, FillSource::Peer));
    }
    if !backed && location.base == 0 {
        return None;
    }
    let key = if location.base == 0 {
        layout::segment_key(page.volume.vset, location.fence, location.seg)
    } else {
        layout::base_segment_key(location.base, location.fence, location.seg)
    };
    loop {
        match Store::get_range(
            world,
            &key,
            u64::from(location.offset),
            u64::from(location.len),
        )
        .await
        {
            Ok(Some((_, bytes))) => return Some((bytes, FillSource::Store)),
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
    let (location, backed, source, retry) = {
        let host = state.borrow();
        let vset = host
            .vsets
            .get(&page.volume.vset)
            .filter(|vset| vset.incarnation == incarnation && vset.ready)?;
        (
            vset.page_locs.get(&page).copied(),
            vset.config.durability.uses_store(),
            vset.peer_source,
            host.config.backup_retry,
        )
    };
    let Some((generation, location)) = location else {
        return Some(vec![0; page_size()]);
    };
    verify_entry(
        page,
        generation,
        fetch_page(state, world, page, location, backed, source, retry)
            .await
            .map(|(bytes, _)| bytes),
    )
}
