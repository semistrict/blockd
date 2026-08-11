use std::collections::BTreeSet;

use blockd_exec::channel::{OneReceiver, oneshot};
use blockd_exec::delay;

use super::SharedHost;
use crate::layout;
use crate::mapleaf::{LeafPtr, MapLeaf, span_of};
use crate::protocol::StoreFault;
use crate::types::{PageId, VsetId};
use crate::world::{Blobs, Store, StoreError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HydrationError {
    Stale,
    Failed,
}

enum HydrateAction {
    Ready,
    Failed,
    Wait(OneReceiver<()>),
    Load {
        pointer: LeafPtr,
        backed: bool,
        retry: u64,
    },
}

struct LeafLoadLease {
    state: SharedHost,
    vset: VsetId,
    incarnation: u64,
    span: u32,
    active: bool,
}

impl LeafLoadLease {
    fn new(state: &SharedHost, page: PageId, incarnation: u64, span: u32) -> Self {
        Self {
            state: state.clone(),
            vset: page.volume.vset,
            incarnation,
            span,
            active: true,
        }
    }

    fn finish(mut self) {
        self.wake();
        self.active = false;
    }

    fn wake(&self) {
        let waiters = {
            let mut host = self.state.borrow_mut();
            host.vsets
                .get_mut(&self.vset)
                .filter(|vset| vset.incarnation == self.incarnation)
                .and_then(|vset| vset.leaf_waiters.remove(&self.span))
                .unwrap_or_default()
        };
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

impl Drop for LeafLoadLease {
    fn drop(&mut self) {
        if self.active {
            self.wake();
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn hydrate_mapping<W>(
    state: &SharedHost,
    world: &W,
    page: PageId,
    incarnation: u64,
) -> Result<(), HydrationError>
where
    W: Blobs + Store,
{
    let span = span_of(page);
    loop {
        let action = {
            let mut host = state.borrow_mut();
            let retry = host.config.backup_retry;
            let Some(vset) = host
                .vsets
                .get_mut(&page.volume.vset)
                .filter(|vset| vset.incarnation == incarnation && vset.ready)
            else {
                return Err(HydrationError::Stale);
            };
            if vset.page_locs.contains_key(&page)
                || vset.hydrated_spans.contains(&span)
                || !vset.leaf_table.contains_key(&span)
            {
                HydrateAction::Ready
            } else if vset.failed_spans.contains(&span) {
                HydrateAction::Failed
            } else if let Some(waiters) = vset.leaf_waiters.get_mut(&span) {
                let (wake, wait) = oneshot();
                waiters.push(wake);
                HydrateAction::Wait(wait)
            } else {
                vset.leaf_waiters.insert(span, Vec::new());
                let pointer = vset.leaf_table[&span];
                HydrateAction::Load {
                    pointer,
                    backed: true,
                    retry,
                }
            }
        };
        match action {
            HydrateAction::Ready => return Ok(()),
            HydrateAction::Failed => return Err(HydrationError::Failed),
            HydrateAction::Wait(wait) => {
                let _ = wait.await;
            }
            HydrateAction::Load {
                pointer,
                backed,
                retry,
            } => {
                let lease = LeafLoadLease::new(state, page, incarnation, span);
                let fetched =
                    fetch_leaf(state, world, page.volume.vset, pointer, backed, retry).await;
                let Some((leaf, size)) = fetched else {
                    if let Some(vset) =
                        state
                            .borrow_mut()
                            .vsets
                            .get_mut(&page.volume.vset)
                            .filter(|vset| {
                                vset.incarnation == incarnation
                                    && vset.leaf_table.get(&span) == Some(&pointer)
                            })
                    {
                        vset.failed_spans.insert(span);
                    }
                    lease.finish();
                    return Err(HydrationError::Failed);
                };
                {
                    let mut host = state.borrow_mut();
                    let Some(vset) = host.vsets.get_mut(&page.volume.vset).filter(|vset| {
                        vset.incarnation == incarnation
                            && vset.leaf_table.get(&span) == Some(&pointer)
                    }) else {
                        drop(host);
                        lease.finish();
                        continue;
                    };
                    let segments = leaf
                        .entries
                        .iter()
                        .filter(|(_, _, _, location)| location.base == 0)
                        .map(|(_, _, _, location)| (location.fence, location.seg))
                        .collect::<BTreeSet<_>>();
                    for &(idx, number, generation, location) in &leaf.entries {
                        let leaf_page = PageId {
                            volume: crate::types::VolumeId {
                                vset: page.volume.vset,
                                idx,
                            },
                            page: number,
                        };
                        if vset.config.contains(leaf_page) {
                            vset.page_locs
                                .entry(leaf_page)
                                .or_insert((generation, location));
                            vset.next_gen = vset.next_gen.max(generation.0 + 1);
                        }
                    }
                    vset.backed_segments.extend(segments.iter().copied());
                    if pointer.base == 0 {
                        vset.backed_leaves.insert((pointer.fence, pointer.id));
                    }
                    vset.leaf_blobs.insert(pointer, (size, segments));
                    vset.failed_spans.remove(&span);
                    vset.hydrated_spans.insert(span);
                    vset.wedge.hydration += 1;
                }
                lease.finish();
                return Ok(());
            }
        }
    }
}

async fn fetch_leaf<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    pointer: LeafPtr,
    backed: bool,
    retry: u64,
) -> Option<(MapLeaf, u64)>
where
    W: Blobs + Store,
{
    let owner = if pointer.base == 0 {
        vset
    } else {
        VsetId(pointer.base)
    };
    let local_name = if pointer.base == 0 {
        layout::leaf_blob(vset, pointer.fence, pointer.id)
    } else {
        layout::base_leaf_blob(vset, pointer.base, pointer.fence, pointer.id)
    };
    let local = Blobs::read(world, &local_name).await.ok().flatten();
    if let Some(bytes) = local.as_ref()
        && let Ok(leaf) = MapLeaf::decode(owner, pointer.fence, pointer.id, bytes)
    {
        return Some((leaf, bytes.len() as u64));
    }
    if !backed {
        return None;
    }
    let key = if pointer.base == 0 {
        layout::leaf_key(vset, pointer.fence, pointer.id)
    } else {
        layout::base_leaf_key(pointer.base, pointer.fence, pointer.id)
    };
    let bytes = loop {
        match Store::get(world, &key).await {
            Ok(Some((_, bytes))) => break bytes,
            Ok(None)
            | Err(StoreError::TooLarge | StoreError::Fault(StoreFault::CasConflict { .. })) => {
                return None;
            }
            Err(StoreError::Fault(StoreFault::Unavailable)) => {
                state.borrow_mut().counters.store_retries += 1;
                delay(retry).await;
            }
        }
    };
    let leaf = MapLeaf::decode(owner, pointer.fence, pointer.id, &bytes).ok()?;
    if local.is_some() && Blobs::delete(world, &local_name).await.is_err() {
        return None;
    }
    if local.is_some() {
        state.borrow_mut().blob_sizes.remove(&local_name);
    }
    if !state
        .borrow_mut()
        .try_reserve_blob(local_name.clone(), bytes.len() as u64)
    {
        return None;
    }
    if Blobs::write(world, local_name.clone(), bytes.clone())
        .await
        .is_err()
    {
        return None;
    }
    state
        .borrow_mut()
        .record_blob(local_name, bytes.len() as u64);
    Some((leaf, bytes.len() as u64))
}
