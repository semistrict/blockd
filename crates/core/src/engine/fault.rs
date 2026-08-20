use std::rc::Rc;

use blockd_exec::channel::oneshot;

use super::state::{CacheReservation, PageFillLease, SharedHost};
use super::store_retry;
use super::{peer_fetch_page, peer_fetch_replica_page};
use crate::blx::{BlockKey, BlxFooter, EntryKind, NamespaceKind};
use crate::format::checksum64;
use crate::layout;
use crate::manifest::ObjectRef;
use crate::page_file::{PageFileLoc, open_entry};
use crate::types::{Gen, HostId, PageId, VolumeId, page_size};
use crate::world::{Blobs, FillSource, GuestFault, GuestMem, Peers, Store};

#[derive(Clone, Copy)]
struct FetchPlan {
    generation: Gen,
    location: PageFileLoc,
    source: Option<HostId>,
}

enum FaultSlot {
    Ready {
        location: Option<(Gen, PageFileLoc)>,
        memory: bool,
        source: Option<HostId>,
        victim: Option<PageId>,
    },
    Shared {
        share: crate::cache::BaseKey,
        memory: bool,
        victim: Option<PageId>,
    },
    Wait(blockd_exec::channel::OneReceiver<()>),
    Filling(blockd_exec::channel::OneReceiver<bool>),
    InvalidWriteProtect,
    Gone,
}

enum ArchiveResolution {
    Ready,
    Fetch { object: ObjectRef },
    Resolve,
}

struct FaultCtx<W> {
    state: SharedHost,
    world: Rc<W>,
    page: PageId,
    run_generation: u64,
}

impl<W> FaultCtx<W> {
    fn new(state: SharedHost, world: Rc<W>, page: PageId, run_generation: u64) -> Self {
        Self {
            state,
            world,
            page,
            run_generation,
        }
    }

    fn current(&self, host: &super::state::HostState) -> bool {
        host.volume_at(self.page.volume, self.run_generation)
            .is_some_and(|volume| volume.ready)
    }
}

pub async fn serve_fault<W>(state: SharedHost, world: Rc<W>, fault: GuestFault)
where
    W: Blobs + Store + Peers + GuestMem + 'static,
{
    let GuestFault {
        page,
        write,
        wp,
        minor,
    } = fault;
    let run_generation = {
        let mut host = state.borrow_mut();
        if let Some(volume) = host.volumes.get(&page.volume) {
            if volume.ready && volume.config.contains(page) {
                Some(volume.run_generation)
            } else {
                host.counters.guest_rejected += 1;
                None
            }
        } else {
            host.counters.guest_rejected += 1;
            None
        }
    };
    let Some(run_generation) = run_generation else {
        let _ = GuestMem::fail(world.as_ref(), page).await;
        state.borrow_mut().fail("unservable guest page");
        return;
    };

    let resident = state.borrow().cache.is_resident(page);
    let fault = FaultCtx::new(state, world, page, run_generation);
    if resident {
        if minor {
            fault.serve_resident_minor().await;
        } else {
            fault.serve_resident_fault(write).await;
        }
    } else {
        fault.serve_missing(write, wp).await;
    }
}

impl<W: GuestMem> FaultCtx<W> {
    async fn serve_resident_minor(&self) {
        let state = &self.state;
        let world = self.world.as_ref();
        let page = self.page;
        let writable = {
            let host = state.borrow();
            let valid = self.current(&host);
            valid
                .then(|| host.cache.is_dirty(page))
                .filter(|_| host.cache.is_resident(page))
        };
        let Some(writable) = writable else {
            return;
        };
        if GuestMem::remap(world, page, writable).await.is_err() {
            state.borrow_mut().fail("resident guest page remap failed");
        }
    }

    async fn serve_resident_fault(&self, write: bool) {
        let state = &self.state;
        let world = self.world.as_ref();
        let page = self.page;
        let run_generation = self.run_generation;
        let copy_on_fault = write
            && state
                .borrow()
                .volumes
                .get(&page.volume)
                .filter(|volume| volume.run_generation == run_generation)
                .and_then(|volume| volume.operations.drain())
                .is_some_and(|drain| drain.unread.contains_key(&page));
        if copy_on_fault {
            let bytes = GuestMem::read_page(world, page).await;
            let mut host = state.borrow_mut();
            let Some(volume) = host
                .volumes
                .get_mut(&page.volume)
                .filter(|volume| volume.run_generation == run_generation)
            else {
                return;
            };
            let copied = volume.operations.drain_mut().is_some_and(|drain| {
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
                let valid = self.current(&host);
                if !valid || !host.cache.is_resident(page) {
                    return;
                }
                if !host.cache.is_dirty(page) {
                    host.cache.mark_dirty(page);
                    let volume = host
                        .volumes
                        .get_mut(&page.volume)
                        .expect("validated volume");
                    volume.mutation_seq += 1;
                    volume.pages_dirtied_total += 1;
                    host.counters.wp_faults += 1;
                    host.counters.guest_pages_dirtied += 1;
                }
                host.schedule_volume(page.volume);
            }
            if GuestMem::unprotect(world, page).await.is_err() {
                state.borrow_mut().fail("guest page unprotect failed");
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
impl<W> FaultCtx<W>
where
    W: Blobs + Store + Peers + GuestMem + 'static,
{
    async fn serve_missing(&self, write: bool, write_protected: bool) {
        let state = &self.state;
        let world = &self.world;
        let page = self.page;
        let run_generation = self.run_generation;
        if self.resolve_archive_mapping().await.is_err() {
            state.borrow_mut().counters.faults_unservable += 1;
            let _ = GuestMem::fail(world.as_ref(), page).await;
            state.borrow_mut().fail("unservable guest page");
            return;
        }

        loop {
            let slot = {
                let mut host = state.borrow_mut();
                if let Some(volume) = host
                    .volumes
                    .get(&page.volume)
                    .filter(|volume| volume.run_generation == run_generation && volume.ready)
                {
                    let location = volume.page_locs.get(&page).copied();
                    let memory = volume.config.is_memory();
                    let source = volume.peer_source;
                    let shared = location
                        .map(|(_, location)| location)
                        .filter(|location| location.base != 0)
                        .map(|location| {
                            (
                                location.base,
                                location.fence,
                                location.object,
                                location.offset,
                            )
                        })
                        .filter(|key| host.cache.base_is_resident(*key));
                    if host.filling_pages.contains(&page) {
                        let (wake, wait) = oneshot();
                        host.page_fill_waiters.entry(page).or_default().push(wake);
                        FaultSlot::Filling(wait)
                    } else if let Some(share) = shared
                        && !write
                    {
                        host.filling_pages.insert(page);
                        FaultSlot::Shared {
                            share,
                            memory,
                            victim: None,
                        }
                    } else if write_protected && shared.is_none() {
                        FaultSlot::InvalidWriteProtect
                    } else if let Some(victim) = host.cache.reserve_slot() {
                        host.filling_pages.insert(page);
                        if let Some(share) = shared {
                            FaultSlot::Shared {
                                share,
                                memory,
                                victim,
                            }
                        } else {
                            FaultSlot::Ready {
                                location,
                                memory,
                                source,
                                victim,
                            }
                        }
                    } else {
                        let (wake, wait) = oneshot();
                        host.pressure_waiters.push_back(wake);
                        host.counters.pressure_waits += 1;
                        FaultSlot::Wait(wait)
                    }
                } else {
                    FaultSlot::Gone
                }
            };
            let (location, memory, source, victim) = match slot {
                FaultSlot::Ready {
                    location,
                    memory,
                    source,
                    victim,
                } => (location, memory, source, victim),
                FaultSlot::Shared {
                    share,
                    memory,
                    victim,
                } => {
                    let fill_lease = PageFillLease::new(state, page);
                    let reservation = write.then(|| CacheReservation::new(state));
                    if let Some(victim) = victim
                        && GuestMem::evict(world.as_ref(), victim).await.is_err()
                    {
                        state.borrow_mut().fail("guest page eviction failed");
                        return;
                    }
                    if write {
                        let mut host = state.borrow_mut();
                        if !self.current(&host) {
                            return;
                        }
                        host.cache.fill_slot(page, true, memory);
                        let volume = host
                            .volumes
                            .get_mut(&page.volume)
                            .expect("validated volume");
                        volume.mutation_seq += 1;
                        volume.pages_dirtied_total += 1;
                        host.counters.guest_pages_dirtied += 1;
                        if write_protected {
                            host.counters.wp_faults += 1;
                        }
                        host.schedule_volume(page.volume);
                        host.wake_pressure_waiter();
                    }
                    if !write_protected {
                        let mut host = state.borrow_mut();
                        host.counters.shared_fills += 1;
                        host.volumes
                            .get_mut(&page.volume)
                            .expect("validated volume")
                            .wedge
                            .fills += 1;
                    }
                    if let Some(reservation) = reservation {
                        reservation.commit();
                    }
                    let result = if write_protected {
                        GuestMem::unprotect(world.as_ref(), page).await
                    } else {
                        GuestMem::fill_shared(world.as_ref(), page, share, None, write).await
                    };
                    if result.is_err() {
                        state.borrow_mut().fail(if write_protected {
                            "guest page unprotect failed"
                        } else {
                            "guest shared-page fill failed"
                        });
                        return;
                    }
                    fill_lease.finish(true);
                    return;
                }
                FaultSlot::Wait(wait) => {
                    if wait.await.is_err() {
                        return;
                    }
                    continue;
                }
                FaultSlot::Filling(wait) => {
                    let _ = wait.await;
                    return;
                }
                FaultSlot::InvalidWriteProtect => {
                    state.borrow_mut().counters.faults_unservable += 1;
                    let _ = GuestMem::fail(world.as_ref(), page).await;
                    state
                        .borrow_mut()
                        .fail("write-protect fault on an unmapped page");
                    return;
                }
                FaultSlot::Gone => return,
            };
            let fill_lease = PageFillLease::new(state, page);
            let reservation = CacheReservation::new(state);
            if let Some(victim) = victim
                && GuestMem::evict(world.as_ref(), victim).await.is_err()
            {
                state.borrow_mut().fail("guest page eviction failed");
                return;
            }

            let Some((generation, location)) = location else {
                {
                    let mut host = state.borrow_mut();
                    if !self.current(&host) {
                        return;
                    }
                    host.cache.fill_slot(page, write, memory);
                    if write {
                        let volume = host
                            .volumes
                            .get_mut(&page.volume)
                            .expect("validated volume");
                        volume.mutation_seq += 1;
                        volume.pages_dirtied_total += 1;
                        host.counters.guest_pages_dirtied += 1;
                        host.schedule_volume(page.volume);
                    }
                    host.counters.zero_fills += 1;
                    host.volumes
                        .get_mut(&page.volume)
                        .expect("validated volume")
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

            let bytes = self
                .fetch_page(FetchPlan {
                    generation,
                    location,
                    source,
                })
                .await;
            let Some((raw, fill_source)) = bytes else {
                let advanced = state
                    .borrow()
                    .volumes
                    .get(&page.volume)
                    .filter(|volume| volume.run_generation == run_generation)
                    .and_then(|volume| volume.page_locs.get(&page))
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
                if !self.current(&host) {
                    return;
                }
                let current = host.volumes[&page.volume].page_locs.get(&page).copied();
                if current != Some((generation, location)) {
                    drop(host);
                    drop(reservation);
                    continue;
                }
                let share = (location.base != 0 && !write).then_some((
                    location.base,
                    location.fence,
                    location.object,
                    location.offset,
                ));
                if let Some(share) = share {
                    host.cache.base_insert(share);
                    host.counters.shared_fills += 1;
                } else {
                    host.cache.fill_slot(page, write, memory);
                }
                let kind = host.volumes[&page.volume].config.kind;
                host.volumes
                    .get_mut(&page.volume)
                    .expect("validated volume")
                    .block_checksums
                    .insert(
                        BlockKey::from_page(kind, page),
                        (generation, checksum64(&raw)),
                    );
                if write {
                    let volume = host
                        .volumes
                        .get_mut(&page.volume)
                        .expect("validated volume");
                    volume.mutation_seq += 1;
                    volume.pages_dirtied_total += 1;
                    host.counters.guest_pages_dirtied += 1;
                    host.schedule_volume(page.volume);
                }
                host.counters.fills += 1;
                host.volumes
                    .get_mut(&page.volume)
                    .expect("validated volume")
                    .wedge
                    .fills += 1;
            }
            reservation.commit();
            if let Some(share) = (location.base != 0 && !write).then_some((
                location.base,
                location.fence,
                location.object,
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

    #[allow(clippy::too_many_lines)]
    async fn fetch_page(&self, plan: FetchPlan) -> Option<(Vec<u8>, FillSource)> {
        let state = &self.state;
        let world = self.world.as_ref();
        let page = self.page;
        let FetchPlan {
            generation,
            location,
            source,
        } = plan;
        let belongs_to_source = location.base == 0
            && source.is_some()
            && state
                .borrow()
                .volumes
                .get(&page.volume)
                .is_some_and(|volume| location.fence < volume.fence);
        if !belongs_to_source {
            let local_name = layout::blx_blob(page.volume, location.fence, location.object);
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
            && let Some(bytes) = peer_fetch_page(state, world, source, page.volume, location).await
            && let Some(raw) = verify_entry(page, generation, Some(bytes))
        {
            return Some((raw, FillSource::Peer));
        }
        let replica = state
            .borrow()
            .volumes
            .get(&page.volume)
            .and_then(|volume| volume.stash_assignment)
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
                page.volume,
                location,
            )
            .await
            && let Some(raw) = verify_entry(page, generation, Some(bytes))
        {
            return Some((raw, FillSource::Peer));
        }
        let archive_key = state
            .borrow()
            .volumes
            .get(&page.volume)
            .and_then(|volume| {
                volume.archive_objects.iter().find(|object| {
                    object.identity.writer_fence == location.fence
                        && object.identity.object_id == location.object.0
                        && if location.base == 0 {
                            object.identity.namespace_kind == NamespaceKind::Volume
                                && object.identity.namespace_id == page.volume.0
                        } else {
                            object.identity.namespace_id == location.base
                        }
                })
            })
            .map(|object| object.identity.store_key());
        let key = archive_key.unwrap_or_else(|| {
            let origin = if location.base == 0 {
                page.volume
            } else {
                VolumeId(location.base)
            };
            layout::blx_key(origin, location.fence, location.object.0)
        });
        let bytes = store_retry::get_range(
            state,
            world,
            &key,
            u64::from(location.offset),
            u64::from(location.len),
        )
        .await
        .ok()??
        .1;
        verify_entry(page, generation, Some(bytes)).map(|raw| (raw, FillSource::Store))
    }

    /// Resolve one archived page without materializing a durable or in-memory map
    /// for the whole volume. Object key ranges select at most the configured overlap
    /// bound; only those footers are fetched, and each verified footer is cached.
    #[allow(clippy::too_many_lines)]
    async fn resolve_archive_mapping(&self) -> Result<(), ()> {
        let state = &self.state;
        let world = self.world.as_ref();
        let page = self.page;
        let run_generation = self.run_generation;
        loop {
            let action = {
                let host = state.borrow();
                let Some(volume) = host
                    .volumes
                    .get(&page.volume)
                    .filter(|volume| volume.run_generation == run_generation && volume.ready)
                else {
                    return Err(());
                };
                if volume.page_locs.contains_key(&page)
                    || volume.archive_resolved_pages.contains(&page)
                {
                    ArchiveResolution::Ready
                } else if !volume.archived_memory_usable && volume.config.is_memory() {
                    ArchiveResolution::Resolve
                } else {
                    let key = BlockKey::from_page(volume.config.kind, page);
                    let candidate = volume
                        .archive_objects
                        .iter()
                        .filter(|object| object.first_key <= key && key <= object.last_key)
                        .find(|object| !volume.archive_footers.contains_key(&object.identity))
                        .copied();
                    candidate.map_or(ArchiveResolution::Resolve, |object| {
                        ArchiveResolution::Fetch { object }
                    })
                }
            };
            match action {
                ArchiveResolution::Ready => return Ok(()),
                ArchiveResolution::Fetch { object } => {
                    let bytes = store_retry::get_range(
                        state,
                        world,
                        &object.identity.store_key(),
                        u64::from(object.footer_offset),
                        u64::from(object.footer_length),
                    )
                    .await
                    .map_err(|_| ())?
                    .ok_or(())?
                    .1;
                    let footer = BlxFooter::open(&bytes).map_err(|_| ())?;
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
                    let Some(volume) = host
                        .volumes
                        .get_mut(&page.volume)
                        .filter(|volume| volume.run_generation == run_generation && volume.ready)
                    else {
                        return Err(());
                    };
                    if volume.archive_objects.contains(&object) {
                        volume.archive_footers.insert(object.identity, footer);
                    }
                }
                ArchiveResolution::Resolve => {
                    let mut host = state.borrow_mut();
                    let Some(volume) = host
                        .volumes
                        .get_mut(&page.volume)
                        .filter(|volume| volume.run_generation == run_generation && volume.ready)
                    else {
                        return Err(());
                    };
                    if volume.page_locs.contains_key(&page)
                        || volume.archive_resolved_pages.contains(&page)
                    {
                        continue;
                    }
                    if !volume.archived_memory_usable && volume.config.is_memory() {
                        volume.archive_resolved_pages.insert(page);
                        return Ok(());
                    }
                    let key = BlockKey::from_page(volume.config.kind, page);
                    let mut winner = None;
                    for object in volume
                        .archive_objects
                        .iter()
                        .filter(|object| object.first_key <= key && key <= object.last_key)
                    {
                        let Some(entry) = volume
                            .archive_footers
                            .get(&object.identity)
                            .and_then(|footer| footer.find(key))
                        else {
                            continue;
                        };
                        let own = object.identity.namespace_kind == NamespaceKind::Volume
                            && object.identity.namespace_id == page.volume.0;
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
                    volume.archive_resolved_pages.insert(page);
                    if let Some((entry, own, object)) = winner {
                        volume.next_gen = volume.next_gen.max(entry.generation.0.saturating_add(1));
                        if entry.kind == EntryKind::Data {
                            volume
                                .block_checksums
                                .insert(key, (entry.generation, entry.value_checksum));
                            volume.page_locs.insert(
                                page,
                                (
                                    entry.generation,
                                    PageFileLoc {
                                        base: if own { 0 } else { object.identity.namespace_id },
                                        fence: object.identity.writer_fence,
                                        object: crate::types::ObjectId(object.identity.object_id),
                                        offset: entry.offset,
                                        len: entry.length,
                                    },
                                ),
                            );
                        } else {
                            volume.block_checksums.remove(&key);
                            volume.page_locs.remove(&page);
                        }
                    }
                    return Ok(());
                }
            }
        }
    }
}

fn verify_entry(page: PageId, generation: Gen, bytes: Option<Vec<u8>>) -> Option<Vec<u8>> {
    bytes
        .and_then(|bytes| open_entry(page.volume, &bytes).ok())
        .and_then(|(found, found_generation, raw)| {
            (found.volume == page.volume
                && found.page == page.page
                && found_generation == generation)
                .then_some(raw)
        })
}
