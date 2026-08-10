//! Async world adapters for the shared actor executor.

use std::cell::RefCell;
use std::rc::Rc;

use blockd_core::layout::BlobName;
use blockd_core::protocol::StoreFault;
use blockd_core::types::SimTime;
use blockd_core::world::{BlobEntry, BlobError, Blobs, Store, StoreError};
use blockd_exec::{delay, now};

use super::blobdev::{BlobDev, BlobDevConfig};
use super::store::{ObjectStore, StoreConfig, StoreError as ModelStoreError, Version};
use crate::rng::Pcg64;

pub struct SimBlobs {
    device: Rc<RefCell<BlobDev>>,
    rng: Rc<RefCell<Pcg64>>,
}

impl SimBlobs {
    pub fn new(config: BlobDevConfig, rng: Pcg64) -> Self {
        Self {
            device: Rc::new(RefCell::new(BlobDev::new(config))),
            rng: Rc::new(RefCell::new(rng)),
        }
    }

    pub fn device(&self) -> Rc<RefCell<BlobDev>> {
        Rc::clone(&self.device)
    }
}

impl Blobs for SimBlobs {
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
        Ok(self
            .device
            .borrow()
            .scan()
            .filter_map(|(name, bytes)| {
                let parsed = blockd_core::layout::parse_blob(name)?;
                let payload = if matches!(parsed, BlobName::Segment { .. }) {
                    Vec::new()
                } else {
                    bytes.clone()
                };
                Some(BlobEntry {
                    name: name.clone(),
                    bytes: payload,
                    len: bytes.len() as u64,
                })
            })
            .collect())
    }

    async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        let (io, done) = self.device.borrow_mut().submit_write(
            SimTime(now()),
            &mut self.rng.borrow_mut(),
            name,
            bytes,
        );
        delay(done.nanos().saturating_sub(now())).await;
        self.device.borrow_mut().complete_write(io);
        Ok(())
    }

    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        let (io, done) = self.device.borrow_mut().submit_append(
            SimTime(now()),
            &mut self.rng.borrow_mut(),
            name,
            bytes,
        );
        delay(done.nanos().saturating_sub(now())).await;
        self.device.borrow_mut().complete_write(io);
        Ok(())
    }

    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
        let len = usize::try_from(len).map_err(|_| BlobError::Io)?;
        self.device.borrow_mut().truncate(name, len);
        Ok(())
    }

    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        let (done, bytes) =
            self.device
                .borrow_mut()
                .read(SimTime(now()), &mut self.rng.borrow_mut(), name);
        delay(done.nanos().saturating_sub(now())).await;
        Ok(bytes)
    }

    async fn read_range(
        &self,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BlobError> {
        let (done, bytes) = self.device.borrow_mut().read_range(
            SimTime(now()),
            &mut self.rng.borrow_mut(),
            name,
            offset,
            len,
        );
        delay(done.nanos().saturating_sub(now())).await;
        Ok(bytes)
    }

    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        self.device.borrow_mut().delete(name);
        Ok(())
    }
}

pub struct SimStore {
    store: Rc<RefCell<ObjectStore>>,
    rng: Rc<RefCell<Pcg64>>,
}

impl SimStore {
    pub fn new(config: StoreConfig, rng: Pcg64) -> Self {
        Self {
            store: Rc::new(RefCell::new(ObjectStore::new(config))),
            rng: Rc::new(RefCell::new(rng)),
        }
    }

    pub fn store(&self) -> Rc<RefCell<ObjectStore>> {
        Rc::clone(&self.store)
    }
}

fn map_store_error(error: &ModelStoreError) -> StoreError {
    match error {
        ModelStoreError::CasConflict { actual } => StoreFault::CasConflict {
            actual: actual.map(|version| version.0),
        }
        .into(),
        ModelStoreError::Unavailable => StoreFault::Unavailable.into(),
        ModelStoreError::TooLarge => StoreError::TooLarge,
    }
}

impl Store for SimStore {
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
        let (done, result) =
            self.store
                .borrow_mut()
                .put(SimTime(now()), &mut self.rng.borrow_mut(), &key, bytes);
        delay(done.nanos().saturating_sub(now())).await;
        result
            .map(|version| version.0)
            .map_err(|error| map_store_error(&error))
    }

    async fn put_cas(
        &self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreError> {
        let (done, result) = self.store.borrow_mut().put_cas(
            SimTime(now()),
            &mut self.rng.borrow_mut(),
            &key,
            expected.map(Version),
            bytes,
        );
        delay(done.nanos().saturating_sub(now())).await;
        result
            .map(|version| version.0)
            .map_err(|error| map_store_error(&error))
    }

    async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let (done, result) =
            self.store
                .borrow_mut()
                .get(SimTime(now()), &mut self.rng.borrow_mut(), key);
        delay(done.nanos().saturating_sub(now())).await;
        result
            .map(|found| found.map(|(version, bytes)| (version.0, bytes)))
            .map_err(|error| map_store_error(&error))
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let (done, result) = self.store.borrow_mut().get_range(
            SimTime(now()),
            &mut self.rng.borrow_mut(),
            key,
            offset,
            len,
        );
        delay(done.nanos().saturating_sub(now())).await;
        result
            .map(|found| found.map(|(version, bytes)| (version.0, bytes)))
            .map_err(|error| map_store_error(&error))
    }

    async fn delete(&self, key: &str) -> Result<bool, StoreError> {
        let (done, result) =
            self.store
                .borrow_mut()
                .delete(SimTime(now()), &mut self.rng.borrow_mut(), key);
        delay(done.nanos().saturating_sub(now())).await;
        result.map_err(|error| map_store_error(&error))
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let (done, result) =
            self.store
                .borrow_mut()
                .list_prefix(SimTime(now()), &mut self.rng.borrow_mut(), prefix);
        delay(done.nanos().saturating_sub(now())).await;
        result.map_err(|error| map_store_error(&error))
    }
}

#[cfg(test)]
mod tests {
    use blockd_core::protocol::StoreFault;
    use blockd_core::world::{Blobs, Store, StoreError};
    use blockd_exec::Executor;

    use super::{BlobDevConfig, Pcg64, SimBlobs, SimStore, StoreConfig};

    #[test]
    fn blob_contract_is_durable_and_delete_ordered() {
        let mut executor = Executor::simulation(1);
        let blobs = SimBlobs::new(BlobDevConfig::nvme(), Pcg64::new(2, 0));
        let device = blobs.device();
        executor.block_on(async move {
            blobs.write("record".into(), vec![1, 2, 3]).await.unwrap();
            blobs.write("marker".into(), vec![4]).await.unwrap();
            assert_eq!(blobs.read("record").await.unwrap(), Some(vec![1, 2, 3]));
            blobs
                .delete_many_durable(&["record".into(), "marker".into()])
                .await
                .unwrap();
        });
        assert!(!device.borrow().contains("record"));
        assert!(!device.borrow().contains("marker"));
        assert!(device.borrow_mut().crash(&mut Pcg64::new(3, 0)).is_empty());
    }

    #[test]
    fn store_contract_has_strong_reads_and_linearizable_cas() {
        let mut executor = Executor::simulation(4);
        let store = SimStore::new(StoreConfig::s3(), Pcg64::new(5, 0));
        executor.block_on(async move {
            let version = store.put("k".into(), vec![1]).await.unwrap();
            assert_eq!(store.get("k").await.unwrap(), Some((version, vec![1])));
            let next = store
                .put_cas("k".into(), Some(version), vec![2])
                .await
                .unwrap();
            assert_eq!(next, version + 1);
            assert_eq!(
                store.put_cas("k".into(), Some(version), vec![3]).await,
                Err(StoreError::Fault(StoreFault::CasConflict {
                    actual: Some(next)
                }))
            );
        });
    }
}
