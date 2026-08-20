//! Explicit object-store archive collector.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use blockd_core::layout::{StoreKey, parse_key};
use blockd_core::protocol::StoreFault;

use crate::{ListedObject, ObjectStore};

#[derive(Clone, Copy)]
struct Observation {
    generation: u64,
    first_seen: tokio::time::Instant,
}

pub struct StoreCollector {
    store: Arc<dyn ObjectStore>,
    observed: BTreeMap<String, Observation>,
}

impl StoreCollector {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            observed: BTreeMap::new(),
        }
    }

    pub async fn pass(&mut self, grace: Duration) -> Result<usize, StoreFault> {
        if grace.is_zero() {
            return Err(StoreFault::Unavailable);
        }
        let mut listed = Arc::clone(&self.store)
            .list_prefix_versioned("v/".to_owned())
            .await?;
        listed.extend(
            Arc::clone(&self.store)
                .list_prefix_versioned("b/".to_owned())
                .await?,
        );
        listed.retain(|entry| parse_key(&entry.key).is_some());
        listed.sort_by(|left, right| left.key.cmp(&right.key));
        listed.dedup_by(|left, right| left.key == right.key);
        let present = listed
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<BTreeSet<_>>();
        self.observed.retain(|key, _| present.contains(key));

        let now = tokio::time::Instant::now();
        let mut objects = Vec::with_capacity(listed.len());
        for ListedObject {
            key, generation, ..
        } in listed
        {
            let needs_body = matches!(
                parse_key(&key),
                Some(
                    StoreKey::Head { .. }
                        | StoreKey::ArchiveManifest { .. }
                        | StoreKey::CompleteFileList { .. }
                        | StoreKey::PendingManifest { .. }
                        | StoreKey::BaseRoot { .. }
                        | StoreKey::BaseManifest { .. }
                )
            );
            let bytes = if needs_body {
                let Some((found_generation, bytes)) =
                    Arc::clone(&self.store).get(key.clone()).await?
                else {
                    self.observed.remove(&key);
                    continue;
                };
                if found_generation != generation {
                    self.observed.remove(&key);
                    continue;
                }
                bytes
            } else {
                Vec::new()
            };
            let observation = self.observed.entry(key.clone()).or_insert(Observation {
                generation,
                first_seen: now,
            });
            if observation.generation != generation {
                *observation = Observation {
                    generation,
                    first_seen: now,
                };
            }
            let mature = now.duration_since(observation.first_seen) >= grace;
            objects.push((key, u64::from(!mature) * 2, bytes));
        }

        let planned = blockd_core::gc::plan(2, 1, &objects);
        let mut deleted = 0;
        for key in planned {
            let Some(observation) = self.observed.get(&key).copied() else {
                continue;
            };
            match Arc::clone(&self.store)
                .delete_cas(key.clone(), observation.generation)
                .await
            {
                Ok(was_present) => {
                    deleted += usize::from(was_present);
                    self.observed.remove(&key);
                }
                Err(StoreFault::CasConflict { .. }) => {
                    self.observed.remove(&key);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::GcsStoreUri;
    use crate::fakegcs::FakeGcs;

    #[tokio::test]
    async fn explicit_collector_preserves_control_state_beyond_grace() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let uri = GcsStoreUri::parse("gs://cluster/collector/").expect("URI");
        let concrete = Arc::new(crate::GcsStore::new(crate::GcsConfig {
            bucket: uri.bucket,
            prefix: uri.prefix,
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let control_keys = [
            "cluster/metadata",
            "cluster/nodes/00000001.claim",
            "cluster/tls/public-keys/00000001.member",
            "cluster/placement",
            "hosts/00000001/session",
        ];
        for key in control_keys {
            Arc::clone(&store)
                .put(key.to_owned(), vec![1])
                .await
                .expect("control record");
        }
        let orphan = blockd_core::layout::blx_key(blockd_core::types::VolumeId(1), 1, 9);
        Arc::clone(&store)
            .put(orphan.clone(), vec![1])
            .await
            .expect("orphan");

        let mut collector = StoreCollector::new(store);
        assert_eq!(collector.pass(Duration::from_millis(10)).await, Ok(0));
        tokio::time::sleep(Duration::from_millis(11)).await;
        assert_eq!(collector.pass(Duration::from_millis(10)).await, Ok(1));
        assert_eq!(collector.pass(Duration::from_millis(10)).await, Ok(0));
        for key in control_keys {
            assert!(
                crate::GcsStore::get(&concrete, key)
                    .await
                    .unwrap()
                    .is_some(),
                "collector deleted control record {key}"
            );
        }
        assert!(
            crate::GcsStore::get(&concrete, &orphan)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn production_collector_restarts_grace_after_object_rewrite() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let uri = GcsStoreUri::parse("gs://cluster/rewrite-grace/").expect("URI");
        let concrete = Arc::new(crate::GcsStore::new(crate::GcsConfig {
            bucket: uri.bucket,
            prefix: uri.prefix,
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let orphan = blockd_core::layout::blx_key(blockd_core::types::VolumeId(3), 1, 7);
        Arc::clone(&store)
            .put(orphan.clone(), vec![1])
            .await
            .expect("initial orphan");
        let mut collector = StoreCollector::new(store.clone());
        let grace = Duration::from_millis(20);
        assert_eq!(collector.pass(grace).await, Ok(0));

        tokio::time::sleep(Duration::from_millis(11)).await;
        Arc::clone(&store)
            .put(orphan.clone(), vec![2])
            .await
            .expect("rewrite orphan");
        tokio::time::sleep(Duration::from_millis(11)).await;
        assert_eq!(
            collector.pass(grace).await,
            Ok(0),
            "rewrite inherited the prior generation's grace age"
        );

        tokio::time::sleep(Duration::from_millis(21)).await;
        assert_eq!(collector.pass(grace).await, Ok(1));
        assert!(
            crate::GcsStore::get(&concrete, &orphan)
                .await
                .unwrap()
                .is_none()
        );
    }
}
