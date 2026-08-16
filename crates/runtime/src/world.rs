//! Production implementations of the async actor world contracts.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blockd_core::protocol::{MAX_OBJECT_BYTES, StoreFault};
use blockd_core::world::{BlobEntry, BlobError, Blobs, Store, StoreError};
use tokio::sync::{Mutex, Semaphore};

use crate::ObjectStore;

const FILE_WORKERS: usize = 8;

#[derive(Clone)]
pub struct FileBlobs {
    root: Arc<PathBuf>,
    normal: Arc<Semaphore>,
    ordered: Arc<Mutex<()>>,
}

impl FileBlobs {
    pub fn new(root: &Path) -> Self {
        Self {
            root: Arc::new(root.to_path_buf()),
            normal: Arc::new(Semaphore::new(FILE_WORKERS)),
            ordered: Arc::new(Mutex::new(())),
        }
    }

    async fn run_normal<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Path) -> Result<T, BlobError> + Send + 'static,
    ) -> Result<T, BlobError> {
        let permit = Arc::clone(&self.normal)
            .acquire_owned()
            .await
            .map_err(|_| BlobError::Io)?;
        let root = Arc::clone(&self.root);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(&root)
        })
        .await
        .unwrap_or(Err(BlobError::Io))
    }

    async fn run_ordered<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Path) -> Result<T, BlobError> + Send + 'static,
    ) -> Result<T, BlobError> {
        let guard = Arc::clone(&self.ordered).lock_owned().await;
        let root = Arc::clone(&self.root);
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            operation(&root)
        })
        .await
        .unwrap_or(Err(BlobError::Io))
    }
}

fn fsync_to_root(root: &Path, parent: &Path) -> std::io::Result<()> {
    let mut directory = parent;
    loop {
        File::open(directory)?.sync_all()?;
        if directory == root {
            return Ok(());
        }
        directory = directory.parent().expect("blob path is under root");
    }
}

fn blob_error(error: &std::io::Error) -> BlobError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    ) {
        BlobError::Full
    } else {
        BlobError::Io
    }
}

fn remove_blob_durable(root: &Path, path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => fsync_to_root(root, path.parent().expect("blob has parent")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn rollback_append(root: &Path, path: &Path, original_len: Option<u64>) -> std::io::Result<()> {
    if let Some(len) = original_len {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(len)?;
        file.sync_all()?;
        fsync_to_root(root, path.parent().expect("blob has parent"))
    } else {
        remove_blob_durable(root, path)
    }
}

fn classify_write(
    result: std::io::Result<()>,
    rollback: impl FnOnce() -> std::io::Result<()>,
) -> Result<(), BlobError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if blob_error(&error) == BlobError::Full => {
            rollback().map_or(Err(BlobError::Io), |()| Err(BlobError::Full))
        }
        Err(_) => Err(BlobError::Io),
    }
}

fn write_blob(root: &Path, name: &str, bytes: &[u8]) -> Result<(), BlobError> {
    let path = root.join(name);
    let parent = path.parent().expect("blob has parent");
    let mut created = false;
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(parent)?;
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => {
                created = true;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return if std::fs::read(&path)? == bytes {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            Err(error) => return Err(error),
        };
        file.write_all(bytes)?;
        file.sync_all()?;
        fsync_to_root(root, parent)
    })();
    classify_write(result, || {
        if created {
            remove_blob_durable(root, &path)
        } else {
            Ok(())
        }
    })
}

fn append_blob(root: &Path, name: &str, bytes: &[u8]) -> Result<(), BlobError> {
    let path = root.join(name);
    let parent = path.parent().expect("blob has parent");
    let original_len = match std::fs::metadata(&path) {
        Ok(metadata) => Some(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return Err(BlobError::Io),
    };
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(parent)?;
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fsync_to_root(root, parent)
    })();
    classify_write(result, || rollback_append(root, &path, original_len))
}

fn delete_many(root: &Path, names: Vec<String>) -> std::io::Result<()> {
    let mut parents = BTreeSet::new();
    for name in names {
        let path = root.join(name);
        parents.insert(path.parent().expect("blob has parent").to_path_buf());
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    for parent in parents {
        fsync_to_root(root, &parent)?;
    }
    Ok(())
}

impl Blobs for FileBlobs {
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
        self.run_normal(|root| {
            Ok(crate::blobscan::scan_blob_dir_for_recovery(root)
                .into_iter()
                .map(|blob| BlobEntry {
                    name: blob.name,
                    bytes: blob.bytes,
                    len: blob.len,
                })
                .collect())
        })
        .await
    }

    async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.run_normal(move |root| write_blob(root, &name, &bytes))
            .await
    }

    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.run_ordered(move |root| append_blob(root, &name, &bytes))
            .await
    }

    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
        let name = name.to_owned();
        self.run_ordered(move |root| {
            OpenOptions::new()
                .write(true)
                .open(root.join(name))
                .and_then(|file| {
                    file.set_len(len)?;
                    file.sync_all()
                })
                .map_err(|error| blob_error(&error))
        })
        .await
    }

    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        let name = name.to_owned();
        self.run_normal(move |root| match File::open(root.join(name)) {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)
                    .map(|_| Some(bytes))
                    .map_err(|_| BlobError::Io)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(BlobError::Io),
        })
        .await
    }

    async fn read_range(
        &self,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BlobError> {
        let name = name.to_owned();
        self.run_normal(move |root| match File::open(root.join(name)) {
            Ok(file) => usize::try_from(len)
                .map_err(|_| BlobError::Io)
                .and_then(|len| {
                    let mut bytes = vec![0; len];
                    let mut read = 0;
                    while read < len {
                        let position = offset
                            .checked_add(u64::try_from(read).map_err(|_| BlobError::Io)?)
                            .ok_or(BlobError::Io)?;
                        let count = file
                            .read_at(&mut bytes[read..], position)
                            .map_err(|_| BlobError::Io)?;
                        if count == 0 {
                            break;
                        }
                        read += count;
                    }
                    bytes.truncate(read);
                    Ok(Some(bytes))
                }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(BlobError::Io),
        })
        .await
    }

    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        self.delete_many_durable(&[name.to_owned()]).await
    }

    async fn delete_many_durable(&self, names: &[String]) -> Result<(), BlobError> {
        let names = names.to_vec();
        self.run_ordered(move |root| delete_many(root, names).map_err(|_| BlobError::Io))
            .await
    }
}

pub struct RuntimeStore {
    store: Arc<dyn ObjectStore>,
}

impl RuntimeStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

fn map_fault<T>(result: Result<T, StoreFault>) -> Result<T, StoreError> {
    result.map_err(StoreError::Fault)
}

impl Store for RuntimeStore {
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
        if bytes.len() > MAX_OBJECT_BYTES as usize {
            return Err(StoreError::TooLarge);
        }
        map_fault(Arc::clone(&self.store).put(key, bytes).await)
    }

    async fn put_cas(
        &self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreError> {
        if bytes.len() > MAX_OBJECT_BYTES as usize {
            return Err(StoreError::TooLarge);
        }
        map_fault(Arc::clone(&self.store).put_cas(key, expected, bytes).await)
    }

    async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        map_fault(Arc::clone(&self.store).get(key.to_owned()).await)
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        map_fault(
            Arc::clone(&self.store)
                .get_range(key.to_owned(), offset, len)
                .await,
        )
    }

    async fn delete(&self, key: &str) -> Result<bool, StoreError> {
        Arc::clone(&self.store).delete(key.to_owned()).await;
        Ok(true)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        map_fault(Arc::clone(&self.store).list_prefix(prefix.to_owned()).await)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use blockd_core::protocol::StoreFault;
    use blockd_core::world::{BlobError, Blobs, Store, StoreError};
    use blockd_exec::ProductionContext;

    use super::{FileBlobs, RuntimeStore, classify_write, remove_blob_durable, rollback_append};
    use crate::fakegcs::FakeGcs;
    use crate::{GcsConfig, GcsStore};

    #[tokio::test]
    async fn file_blob_contract_is_durable_and_ordered() {
        let root = tempfile::tempdir().unwrap();
        let blobs = FileBlobs::new(root.path());
        ProductionContext::new(|_| {})
            .scope(async move {
                blobs
                    .write("v/1/record".into(), vec![1, 2, 3])
                    .await
                    .unwrap();
                blobs
                    .write("v/1/record".into(), vec![1, 2, 3])
                    .await
                    .expect("unknown-outcome retry is idempotent");
                assert!(
                    blobs
                        .write("v/1/record".into(), vec![9, 9, 9])
                        .await
                        .is_err(),
                    "immutable blob rewrite with different bytes was accepted"
                );
                blobs.write("v/1/marker".into(), vec![4]).await.unwrap();
                assert_eq!(
                    blobs.read_range("v/1/record", 1, 9).await.unwrap(),
                    Some(vec![2, 3])
                );
                blobs
                    .delete_many_durable(&["v/1/record".into(), "v/1/marker".into()])
                    .await
                    .unwrap();
                assert_eq!(blobs.read("v/1/record").await.unwrap(), None);
                assert_eq!(blobs.read("v/1/marker").await.unwrap(), None);
            })
            .await;
    }

    #[test]
    fn retryable_write_failures_restore_the_exact_prior_filesystem_state() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("nested");
        std::fs::create_dir_all(&parent).unwrap();

        let immutable = parent.join("partial.blob");
        std::fs::write(&immutable, b"partial").unwrap();
        assert_eq!(
            classify_write(
                Err(std::io::Error::from(std::io::ErrorKind::StorageFull)),
                || remove_blob_durable(root.path(), &immutable),
            ),
            Err(BlobError::Full),
        );
        assert!(!immutable.exists());

        let spool = parent.join("existing.spool");
        std::fs::write(&spool, b"goodpartial").unwrap();
        assert_eq!(
            classify_write(
                Err(std::io::Error::from(std::io::ErrorKind::StorageFull)),
                || rollback_append(root.path(), &spool, Some(4)),
            ),
            Err(BlobError::Full),
        );
        assert_eq!(std::fs::read(&spool).unwrap(), b"good");

        assert_eq!(
            classify_write(
                Err(std::io::Error::from(std::io::ErrorKind::StorageFull)),
                || Err(std::io::Error::other("rollback failed")),
            ),
            Err(BlobError::Io),
        );
    }

    #[tokio::test]
    async fn gcs_store_contract_has_strong_reads_and_cas() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: "test".to_owned(),
            prefix: "contract/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let store = RuntimeStore::new(store);
        ProductionContext::new(|_| {})
            .scope(async move {
                let version = store.put("k".into(), vec![1]).await.unwrap();
                assert_eq!(store.get("k").await.unwrap(), Some((version, vec![1])));
                let next = store
                    .put_cas("k".into(), Some(version), vec![2])
                    .await
                    .unwrap();
                assert_eq!(
                    store.put_cas("k".into(), Some(version), vec![3]).await,
                    Err(StoreError::Fault(StoreFault::CasConflict {
                        actual: Some(next)
                    }))
                );
            })
            .await;
    }
}
