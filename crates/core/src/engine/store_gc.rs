use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_exec::{delay, now};

use super::SharedHost;
use crate::layout::StoreKey;
use crate::types::SimTime;
use crate::world::{Store, StoreError};

const GC_INTERVAL_MULTIPLIER: u64 = 600;
const GC_GRACE_PASSES: u64 = 10;

/// Conservatively mark and sweep the shared object namespace. Object stores
/// expose keys rather than backend-specific mtimes, so `observed_at` records
/// the first complete pass that saw a key. A restart therefore renews every
/// object's grace instead of risking an early delete.
pub(super) async fn store_gc_actor<W: Store>(state: SharedHost, world: Rc<W>) {
    let interval = state
        .borrow()
        .config
        .backup_retry
        .saturating_mul(GC_INTERVAL_MULTIPLIER)
        .max(1);
    let grace = interval.saturating_mul(GC_GRACE_PASSES);
    let mut observed_at = BTreeMap::new();
    loop {
        delay(interval).await;
        if store_gc_pass(world.as_ref(), &mut observed_at, grace)
            .await
            .is_err()
        {
            state.borrow_mut().counters.store_retries += 1;
        }
    }
}

async fn store_gc_pass<W: Store>(
    world: &W,
    observed_at: &mut BTreeMap<String, SimTime>,
    grace: u64,
) -> Result<usize, StoreError> {
    let keys = Store::list_prefix(world, "").await?;
    let present = keys.iter().cloned().collect::<BTreeSet<_>>();
    observed_at.retain(|key, _| present.contains(key));

    let observed_now = SimTime(now());
    let mut objects = Vec::with_capacity(keys.len());
    for key in keys {
        let needs_body = matches!(
            crate::layout::parse_key(&key),
            Some(
                StoreKey::Head { .. }
                    | StoreKey::Manifest { .. }
                    | StoreKey::PendingManifest { .. }
                    | StoreKey::Leaf { .. }
                    | StoreKey::BaseRecord { .. }
                    | StoreKey::BaseLeaf { .. }
            )
        );
        let bytes = if needs_body {
            let Some((_, bytes)) = Store::get(world, &key).await? else {
                observed_at.remove(&key);
                continue;
            };
            bytes
        } else {
            Vec::new()
        };
        let put_at = *observed_at.entry(key.clone()).or_insert(observed_now);
        objects.push((key, put_at, bytes));
    }

    let deletions = crate::gc::plan(observed_now, grace, &objects);
    for key in &deletions {
        Store::delete(world, key).await?;
        observed_at.remove(key);
    }
    Ok(deletions.len())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use blockd_exec::{Executor, delay};

    use super::*;
    use crate::head::{HeadRecord, ManifestPtr};
    use crate::journal::{DatabaseMeta, JournalRecord, RecordKind, VsetConfig};
    use crate::layout;
    use crate::protocol::StoreFault;
    use crate::segment::PageLoc;
    use crate::types::{
        Gen, HostId, JournalSeq, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId,
    };
    use crate::world::StoreError;

    #[derive(Default)]
    struct TestStore {
        objects: RefCell<BTreeMap<String, (u64, Vec<u8>)>>,
        fetched: RefCell<Vec<String>>,
    }

    impl Store for TestStore {
        async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
            self.objects.borrow_mut().insert(key, (1, bytes));
            Ok(1)
        }

        async fn put_cas(
            &self,
            key: String,
            _expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, StoreError> {
            self.put(key, bytes).await
        }

        async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            self.fetched.borrow_mut().push(key.to_owned());
            Ok(self.objects.borrow().get(key).cloned())
        }

        async fn get_range(
            &self,
            key: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            let Some((version, bytes)) = self.objects.borrow().get(key).cloned() else {
                return Ok(None);
            };
            let start = usize::try_from(offset).map_err(|_| StoreError::TooLarge)?;
            let end = start.saturating_add(usize::try_from(len).map_err(|_| StoreError::TooLarge)?);
            Ok(bytes
                .get(start..end.min(bytes.len()))
                .map(|slice| (version, slice.to_vec())))
        }

        async fn delete(&self, key: &str) -> Result<bool, StoreError> {
            Ok(self.objects.borrow_mut().remove(key).is_some())
        }

        async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            Ok(self
                .objects
                .borrow()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    #[test]
    fn collector_renews_grace_then_deletes_only_unreachable_objects() {
        let store = TestStore::default();
        let vset = VsetId(1);
        let head_key = layout::head_key(vset);
        store.objects.borrow_mut().insert(
            head_key.clone(),
            (
                1,
                HeadRecord {
                    vset,
                    holder: HostId(1),
                    fence: 1,
                    manifest: None,
                    stash: None,
                    retired_stashes: Vec::new(),
                }
                .encode(),
            ),
        );
        let orphan = layout::segment_key(vset, 1, crate::types::SegId(9));
        store
            .objects
            .borrow_mut()
            .insert(orphan.clone(), (1, vec![1, 2, 3]));

        let store = Rc::new(store);
        let task_store = Rc::clone(&store);
        let expected_orphan = orphan.clone();
        let mut executor = Executor::simulation(1);
        let result = executor.block_on(async move {
            let mut observed = BTreeMap::new();
            assert_eq!(
                store_gc_pass(task_store.as_ref(), &mut observed, 10).await,
                Ok(0)
            );
            delay(11).await;
            assert_eq!(
                store_gc_pass(task_store.as_ref(), &mut observed, 10).await,
                Ok(1)
            );
            (
                task_store.objects.borrow().contains_key(&head_key),
                task_store.objects.borrow().contains_key(&orphan),
            )
        });
        assert_eq!(result, (true, false));
        assert!(
            !store.fetched.borrow().contains(&expected_orphan),
            "segment payloads must not be fetched during GC"
        );
    }

    #[test]
    fn collector_preserves_inflight_publication_roots() {
        let store = Rc::new(TestStore::default());
        let vset = VsetId(2);
        let fence = 3;
        let seq = JournalSeq(4);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let location = PageLoc {
            base: 0,
            fence,
            seg: SegId(4),
            offset: 0,
            len: 1,
        };
        let record = JournalRecord {
            config: VsetConfig::compute(1, 4),
            seq,
            fence,
            kind: RecordKind::Commit,
            capture_seq: 5,
            sync_covered_through: 5,
            database: DatabaseMeta::default(),
            overlay: BTreeMap::from([(page, (Gen(1), location))]),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let head = layout::head_key(vset);
        let manifest = layout::manifest_key(vset, fence, seq);
        let pending = layout::pending_manifest_key(vset, fence, seq);
        let segment = layout::segment_key(vset, fence, location.seg);
        store.objects.borrow_mut().extend([
            (
                head.clone(),
                (
                    1,
                    HeadRecord {
                        vset,
                        holder: HostId(1),
                        fence,
                        manifest: None,
                        stash: None,
                        retired_stashes: Vec::new(),
                    }
                    .encode(),
                ),
            ),
            (pending.clone(), (1, record.encode(vset))),
            (segment.clone(), (1, vec![1, 2, 3])),
        ]);
        let task_store = Rc::clone(&store);
        let mut executor = Executor::simulation(2);

        executor.block_on(async move {
            let mut observed = BTreeMap::new();
            assert_eq!(
                store_gc_pass(task_store.as_ref(), &mut observed, 10).await,
                Ok(0)
            );
            delay(11).await;
            assert_eq!(
                store_gc_pass(task_store.as_ref(), &mut observed, 10).await,
                Ok(0)
            );
            task_store
                .objects
                .borrow_mut()
                .insert(manifest, (1, record.encode(vset)));
            task_store.objects.borrow_mut().insert(
                head,
                (
                    2,
                    HeadRecord {
                        vset,
                        holder: HostId(1),
                        fence,
                        manifest: Some(ManifestPtr {
                            fence,
                            seq,
                            capture_seq: record.capture_seq,
                        }),
                        stash: None,
                        retired_stashes: Vec::new(),
                    }
                    .encode(),
                ),
            );
            assert_eq!(
                store_gc_pass(task_store.as_ref(), &mut observed, 10).await,
                Ok(1)
            );
        });

        assert!(store.objects.borrow().contains_key(&segment));
        assert!(!store.objects.borrow().contains_key(&pending));
    }

    #[test]
    fn unavailable_listing_fails_closed() {
        struct Unavailable;

        impl Store for Unavailable {
            async fn put(&self, _: String, _: Vec<u8>) -> Result<u64, StoreError> {
                unreachable!()
            }
            async fn put_cas(
                &self,
                _: String,
                _: Option<u64>,
                _: Vec<u8>,
            ) -> Result<u64, StoreError> {
                unreachable!()
            }
            async fn get(&self, _: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
                unreachable!()
            }
            async fn get_range(
                &self,
                _: &str,
                _: u64,
                _: u64,
            ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
                unreachable!()
            }
            async fn delete(&self, _: &str) -> Result<bool, StoreError> {
                unreachable!()
            }
            async fn list_prefix(&self, _: &str) -> Result<Vec<String>, StoreError> {
                Err(StoreFault::Unavailable.into())
            }
        }

        let mut executor = Executor::simulation(1);
        assert_eq!(
            executor
                .block_on(async { store_gc_pass(&Unavailable, &mut BTreeMap::new(), 10).await }),
            Err(StoreError::Fault(StoreFault::Unavailable))
        );
    }
}
