//! Filesystem-backed object store for the standalone vsetfs runner.
//!
//! Regular-file operations run on Tokio's blocking pool. A small set of
//! keyed lock shards preserves same-key CAS atomicity without serializing
//! unrelated objects, and ranged reads issue a bounded positional read.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use blockd_core::protocol::StoreFault;

use crate::{GetResult, ObjectStore};

const LOCK_SHARDS: usize = 64;
const OBJECT_HEADER_BYTES: usize = 8;
const OBJECT_HEADER_BYTES_U64: u64 = 8;

pub struct DirectoryStore {
    root: PathBuf,
    locks: Box<[RwLock<()>]>,
    next_temp: AtomicU64,
}

impl DirectoryStore {
    pub fn new(root: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            locks: (0..LOCK_SHARDS)
                .map(|_| RwLock::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_temp: AtomicU64::new(1),
        })
    }

    fn path(&self, key: &str) -> io::Result<PathBuf> {
        let key = Path::new(key);
        if key.is_absolute()
            || key
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid object key",
            ));
        }
        Ok(self.root.join(key))
    }

    fn lock_index(key: &str) -> usize {
        let hash = key
            .as_bytes()
            .iter()
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        usize::try_from(hash % LOCK_SHARDS as u64).expect("lock index fits")
    }

    fn open(path: &Path) -> io::Result<Option<File>> {
        match File::open(path) {
            Ok(file) => Ok(Some(file)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn read_version(path: &Path) -> io::Result<Option<u64>> {
        let Some(mut file) = Self::open(path)? else {
            return Ok(None);
        };
        let mut version = [0; OBJECT_HEADER_BYTES];
        file.read_exact(&mut version)?;
        Ok(Some(u64::from_le_bytes(version)))
    }

    fn read(path: &Path) -> io::Result<Option<(u64, Vec<u8>)>> {
        let Some(mut file) = Self::open(path)? else {
            return Ok(None);
        };
        let mut version = [0; OBJECT_HEADER_BYTES];
        file.read_exact(&mut version)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(Some((u64::from_le_bytes(version), bytes)))
    }

    fn read_range(path: &Path, offset: u64, len: u64) -> io::Result<Option<(u64, Vec<u8>)>> {
        let Some(file) = Self::open(path)? else {
            return Ok(None);
        };
        let object_len = file
            .metadata()?
            .len()
            .saturating_sub(OBJECT_HEADER_BYTES_U64);
        if offset >= object_len {
            return Ok(None);
        }
        let mut version = [0; OBJECT_HEADER_BYTES];
        file.read_exact_at(&mut version, 0)?;
        let read_len = usize::try_from(len.min(object_len - offset))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range is too large"))?;
        let mut bytes = vec![0; read_len];
        file.read_exact_at(&mut bytes, OBJECT_HEADER_BYTES_U64 + offset)?;
        Ok(Some((u64::from_le_bytes(version), bytes)))
    }

    fn write(&self, path: &Path, version: u64, bytes: &[u8]) -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "object has no parent"))?;
        std::fs::create_dir_all(parent)?;
        let temp = parent.join(format!(
            ".blockd-store-{}-{}",
            std::process::id(),
            self.next_temp.fetch_add(1, Ordering::Relaxed)
        ));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            file.write_all(&version.to_le_bytes())?;
            file.write_all(bytes)?;
            file.sync_all()?;
            std::fs::rename(&temp, path)?;
            File::open(parent)?.sync_all()
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temp);
        }
        result
    }

    fn list(root: &Path, directory: &Path, prefix: &str, keys: &mut Vec<String>) -> io::Result<()> {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                Self::list(root, &path, prefix, keys)?;
            } else if file_type.is_file() {
                let key = path
                    .strip_prefix(root)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "object escaped root"))?
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                if !key.starts_with(".blockd-store-") && key.starts_with(prefix) {
                    keys.push(key);
                }
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for DirectoryStore {
    async fn put(self: Arc<Self>, key: String, bytes: Vec<u8>) -> Result<u64, StoreFault> {
        tokio::task::spawn_blocking(move || {
            let _guard = self.locks[Self::lock_index(&key)]
                .write()
                .map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            let version = Self::read_version(&path)
                .map_err(|_| StoreFault::Unavailable)?
                .map_or(1, |version| version.saturating_add(1));
            self.write(&path, version, &bytes)
                .map_err(|_| StoreFault::Unavailable)?;
            Ok(version)
        })
        .await
        .map_err(|_| StoreFault::Unavailable)?
    }

    async fn put_cas(
        self: Arc<Self>,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreFault> {
        tokio::task::spawn_blocking(move || {
            let _guard = self.locks[Self::lock_index(&key)]
                .write()
                .map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            let actual = Self::read_version(&path).map_err(|_| StoreFault::Unavailable)?;
            if actual != expected {
                return Err(StoreFault::CasConflict { actual });
            }
            let version = actual.map_or(1, |version| version.saturating_add(1));
            self.write(&path, version, &bytes)
                .map_err(|_| StoreFault::Unavailable)?;
            Ok(version)
        })
        .await
        .map_err(|_| StoreFault::Unavailable)?
    }

    async fn get(self: Arc<Self>, key: String) -> GetResult {
        tokio::task::spawn_blocking(move || {
            let _guard = self.locks[Self::lock_index(&key)]
                .read()
                .map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            Self::read(&path).map_err(|_| StoreFault::Unavailable)
        })
        .await
        .map_err(|_| StoreFault::Unavailable)?
    }

    async fn get_range(self: Arc<Self>, key: String, offset: u64, len: u64) -> GetResult {
        tokio::task::spawn_blocking(move || {
            let _guard = self.locks[Self::lock_index(&key)]
                .read()
                .map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            Self::read_range(&path, offset, len).map_err(|_| StoreFault::Unavailable)
        })
        .await
        .map_err(|_| StoreFault::Unavailable)?
    }

    async fn delete(self: Arc<Self>, key: String) {
        let _ = tokio::task::spawn_blocking(move || {
            let Ok(_guard) = self.locks[Self::lock_index(&key)].write() else {
                return;
            };
            let Ok(path) = self.path(&key) else { return };
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        let _ = File::open(parent).and_then(|dir| dir.sync_all());
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(_) => {}
            }
        })
        .await;
    }

    async fn list_prefix(
        self: Arc<Self>,
        prefix: String,
    ) -> Result<Vec<String>, StoreFault> {
        tokio::task::spawn_blocking(move || {
            let mut keys = Vec::new();
            Self::list(&self.root, &self.root, &prefix, &mut keys)
                .map_err(|_| StoreFault::Unavailable)?;
            keys.sort();
            Ok(keys)
        })
        .await
        .map_err(|_| StoreFault::Unavailable)?
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn ranged_read_returns_only_the_requested_bytes() {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(DirectoryStore::new(root.path().to_owned()).unwrap());
        let bytes: Vec<_> = (0..4 * 1024 * 1024)
            .map(|offset| u8::try_from(offset % 251).unwrap())
            .collect();
        store
            .clone()
            .put("large".to_owned(), bytes.clone())
            .await
            .unwrap();
        let (_, range) = store
            .clone()
            .get_range("large".to_owned(), 2_000_000, 4096)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(range, bytes[2_000_000..2_004_096]);
    }

    #[tokio::test]
    async fn listing_returns_a_sorted_complete_prefix_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(DirectoryStore::new(root.path().to_owned()).unwrap());
        store.clone().put("v/2/b".to_owned(), vec![2]).await.unwrap();
        store.clone().put("v/1/a".to_owned(), vec![1]).await.unwrap();
        store.clone().put("other".to_owned(), vec![3]).await.unwrap();

        assert_eq!(
            store.clone().list_prefix("v/".to_owned()).await.unwrap(),
            vec!["v/1/a".to_owned(), "v/2/b".to_owned()]
        );
    }

    #[tokio::test]
    #[ignore = "performance profile; run explicitly in release mode"]
    async fn profile_bounded_directory_range_reads() {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(DirectoryStore::new(root.path().to_owned()).unwrap());
        store
            .clone()
            .put("artifact".to_owned(), vec![0x5a; 32 * 1024 * 1024])
            .await
            .unwrap();
        let range_started = Instant::now();
        for offset in (0..32 * 1024 * 1024).step_by(4096) {
            let range = store
                .clone()
                .get_range("artifact".to_owned(), offset, 4096)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(range.1.len(), 4096);
        }
        let range_elapsed = range_started.elapsed();
        let full_started = Instant::now();
        for _ in 0..64 {
            let object = store
                .clone()
                .get("artifact".to_owned())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(object.1.len(), 32 * 1024 * 1024);
        }
        let full_elapsed = full_started.elapsed();
        eprintln!(
            "8192 bounded 4 KiB reads: {:.1}ms ({:.1}µs/read); 64 full 32 MiB reads: {:.1}ms ({:.1}ms/read)",
            range_elapsed.as_secs_f64() * 1_000.0,
            range_elapsed.as_secs_f64() * 1_000_000.0 / 8192.0,
            full_elapsed.as_secs_f64() * 1_000.0,
            full_elapsed.as_secs_f64() * 1_000.0 / 64.0,
        );
    }
}
