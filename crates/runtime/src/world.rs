//! Production implementations of the async actor world contracts.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::FileExt as _;
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};

use blockd_core::protocol::{MAX_OBJECT_BYTES, StoreFault};
use blockd_core::world::{BlobEntry, BlobError, Blobs, Store, StoreError};
use blockd_exec::inject::{Injected, Injector, Lane, injector};

use crate::ObjectStore;

const FILE_WORKERS: usize = 8;
const FILE_QUEUE_CAPACITY: usize = 1024;

enum FileJob {
    Scan {
        reply: Injector<Result<Vec<BlobEntry>, BlobError>>,
    },
    Write {
        name: String,
        bytes: Vec<u8>,
        reply: Injector<Result<(), BlobError>>,
    },
    Append {
        name: String,
        bytes: Vec<u8>,
        reply: Injector<Result<(), BlobError>>,
    },
    Truncate {
        name: String,
        len: u64,
        reply: Injector<Result<(), BlobError>>,
    },
    Read {
        name: String,
        reply: Injector<Result<Option<Vec<u8>>, BlobError>>,
    },
    ReadRange {
        name: String,
        offset: u64,
        len: u64,
        reply: Injector<Result<Option<Vec<u8>>, BlobError>>,
    },
    DeleteMany {
        names: Vec<String>,
        reply: Injector<Result<(), BlobError>>,
    },
}

#[derive(Clone)]
pub struct FileBlobs {
    normal: SyncSender<FileJob>,
    ordered: SyncSender<FileJob>,
}

impl FileBlobs {
    pub fn new(root: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(root)?;
        let (normal, normal_rx) = sync_channel(FILE_QUEUE_CAPACITY);
        let normal_rx = Arc::new(Mutex::new(normal_rx));
        for index in 0..FILE_WORKERS {
            let root = root.to_path_buf();
            let receiver = Arc::clone(&normal_rx);
            std::thread::Builder::new()
                .name(format!("blockd-file-{index}"))
                .spawn(move || file_worker(&root, &receiver))?;
        }

        let (ordered, ordered_rx) = sync_channel(FILE_QUEUE_CAPACITY);
        let ordered_rx = Arc::new(Mutex::new(ordered_rx));
        std::thread::Builder::new()
            .name("blockd-file-ordered".to_owned())
            .spawn({
                let root = root.to_path_buf();
                move || file_worker(&root, &ordered_rx)
            })?;

        Ok(Self { normal, ordered })
    }

    async fn response<T>(receiver: Injected<Result<T, BlobError>>) -> Result<T, BlobError> {
        receiver.recv().await.unwrap_or(Err(BlobError::Io))
    }
}

fn send<T>(reply: &Injector<T>, value: T) {
    let _ = reply.push(Lane::Critical, value);
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

fn write_blob(root: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let path = root.join(name);
    let parent = path.parent().expect("blob has parent");
    std::fs::create_dir_all(parent)?;
    let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
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
}

fn append_blob(root: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let path = root.join(name);
    let parent = path.parent().expect("blob has parent");
    std::fs::create_dir_all(parent)?;
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fsync_to_root(root, parent)
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

fn file_worker(root: &Path, receiver: &Arc<Mutex<Receiver<FileJob>>>) {
    loop {
        let Ok(job) = receiver.lock().expect("file queue lock poisoned").recv() else {
            return;
        };
        match job {
            FileJob::Scan { reply } => {
                let blobs = crate::blobscan::scan_blob_dir_for_recovery(root)
                    .into_iter()
                    .map(|blob| BlobEntry {
                        name: blob.name,
                        bytes: blob.bytes,
                        len: blob.len,
                    })
                    .collect();
                send(&reply, Ok(blobs));
            }
            FileJob::Write { name, bytes, reply } => {
                send(
                    &reply,
                    write_blob(root, &name, &bytes).map_err(|_| BlobError::Io),
                );
            }
            FileJob::Append { name, bytes, reply } => {
                send(
                    &reply,
                    append_blob(root, &name, &bytes).map_err(|_| BlobError::Io),
                );
            }
            FileJob::Truncate { name, len, reply } => {
                let result = OpenOptions::new()
                    .write(true)
                    .open(root.join(name))
                    .and_then(|file| {
                        file.set_len(len)?;
                        file.sync_all()
                    })
                    .map_err(|_| BlobError::Io);
                send(&reply, result);
            }
            FileJob::Read { name, reply } => {
                let result = match File::open(root.join(name)) {
                    Ok(mut file) => {
                        let mut bytes = Vec::new();
                        file.read_to_end(&mut bytes)
                            .map(|_| Some(bytes))
                            .map_err(|_| BlobError::Io)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(_) => Err(BlobError::Io),
                };
                send(&reply, result);
            }
            FileJob::ReadRange {
                name,
                offset,
                len,
                reply,
            } => {
                let result = match File::open(root.join(name)) {
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
                };
                send(&reply, result);
            }
            FileJob::DeleteMany { names, reply } => {
                send(&reply, delete_many(root, names).map_err(|_| BlobError::Io));
            }
        }
    }
}

impl Blobs for FileBlobs {
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
        let (reply, response) = injector();
        self.normal
            .send(FileJob::Scan { reply })
            .map_err(|_| BlobError::Io)?;
        Self::response(response).await
    }

    async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        let (reply, response) = injector();
        self.normal
            .send(FileJob::Write { name, bytes, reply })
            .map_err(|_| BlobError::Io)?;
        Self::response(response).await
    }

    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        let (reply, response) = injector();
        self.ordered
            .send(FileJob::Append { name, bytes, reply })
            .map_err(|_| BlobError::Io)?;
        Self::response(response).await
    }

    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
        let (reply, response) = injector();
        self.ordered
            .send(FileJob::Truncate {
                name: name.to_owned(),
                len,
                reply,
            })
            .map_err(|_| BlobError::Io)?;
        Self::response(response).await
    }

    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        let (reply, response) = injector();
        self.normal
            .send(FileJob::Read {
                name: name.to_owned(),
                reply,
            })
            .map_err(|_| BlobError::Io)?;
        Self::response(response).await
    }

    async fn read_range(
        &self,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BlobError> {
        let (reply, response) = injector();
        self.normal
            .send(FileJob::ReadRange {
                name: name.to_owned(),
                offset,
                len,
                reply,
            })
            .map_err(|_| BlobError::Io)?;
        Self::response(response).await
    }

    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        self.delete_many_durable(&[name.to_owned()]).await
    }

    async fn delete_many_durable(&self, names: &[String]) -> Result<(), BlobError> {
        let (reply, response) = injector();
        self.ordered
            .send(FileJob::DeleteMany {
                names: names.to_vec(),
                reply,
            })
            .map_err(|_| BlobError::Io)?;
        Self::response(response).await
    }
}

pub struct RuntimeStore {
    handle: tokio::runtime::Handle,
    store: Arc<dyn ObjectStore>,
}

impl RuntimeStore {
    pub fn new(handle: tokio::runtime::Handle, store: Arc<dyn ObjectStore>) -> Self {
        Self { handle, store }
    }

    async fn response<T: Send + 'static>(response: Injected<T>) -> T {
        response.recv().await.expect("store runtime stopped")
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
        let (reply, response) = injector();
        let store = Arc::clone(&self.store);
        self.handle.spawn(async move {
            let result = store.put(key, bytes).await;
            let _ = reply.push(Lane::Background, result);
        });
        map_fault(Self::response(response).await)
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
        let (reply, response) = injector();
        let store = Arc::clone(&self.store);
        self.handle.spawn(async move {
            let result = store.put_cas(key, expected, bytes).await;
            let _ = reply.push(Lane::Background, result);
        });
        map_fault(Self::response(response).await)
    }

    async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let (reply, response) = injector();
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        self.handle.spawn(async move {
            let result = store.get(key).await;
            let _ = reply.push(Lane::Critical, result);
        });
        map_fault(Self::response(response).await)
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let (reply, response) = injector();
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        self.handle.spawn(async move {
            let result = store.get_range(key, offset, len).await;
            let _ = reply.push(Lane::Critical, result);
        });
        map_fault(Self::response(response).await)
    }

    async fn delete(&self, key: &str) -> Result<bool, StoreError> {
        let (reply, response) = injector();
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        self.handle.spawn(async move {
            store.delete(key).await;
            let _ = reply.push(Lane::Background, ());
        });
        Self::response(response).await;
        Ok(true)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let (reply, response) = injector();
        let store = Arc::clone(&self.store);
        let prefix = prefix.to_owned();
        self.handle.spawn(async move {
            let result = store.list_prefix(prefix).await;
            let _ = reply.push(Lane::Background, result);
        });
        map_fault(Self::response(response).await)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use blockd_core::protocol::StoreFault;
    use blockd_core::world::{Blobs, Store, StoreError};
    use blockd_exec::Executor;

    use super::{FileBlobs, RuntimeStore};
    use crate::directory_store::DirectoryStore;

    #[test]
    fn file_blob_contract_is_durable_and_ordered() {
        let root = tempfile::tempdir().unwrap();
        let blobs = FileBlobs::new(root.path()).unwrap();
        let mut executor = Executor::production();
        executor.block_on(async move {
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
        });
    }

    #[test]
    fn directory_store_contract_has_strong_reads_and_cas() {
        let root = tempfile::tempdir().unwrap();
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = Arc::new(DirectoryStore::new(root.path().to_owned()).unwrap());
        let store = RuntimeStore::new(tokio.handle().clone(), store);
        let mut executor = Executor::production();
        executor.block_on(async move {
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
        });
    }
}
