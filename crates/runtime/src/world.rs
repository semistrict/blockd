//! Production implementations of the async actor world contracts.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::os::unix::fs::FileExt as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
#[cfg(target_os = "linux")]
use std::path::Component;
use std::path::Path;
use std::sync::Arc;

use blockd_core::protocol::{MAX_OBJECT_BYTES, StoreFault};
use blockd_core::world::{BlobEntry, BlobError, Blobs, Store, StoreError};
use tokio::sync::{Mutex, Semaphore};

use crate::ObjectStore;

const FILE_WORKERS: usize = 8;

#[cfg(test)]
thread_local! {
    static EFFECTIVE_UID_OVERRIDE: std::cell::Cell<Option<u32>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(test)]
pub(crate) struct EffectiveUidOverride(Option<u32>);

#[cfg(test)]
impl Drop for EffectiveUidOverride {
    fn drop(&mut self) {
        EFFECTIVE_UID_OVERRIDE.with(|override_uid| override_uid.set(self.0));
    }
}

#[cfg(test)]
pub(crate) fn override_effective_uid(uid: u32) -> EffectiveUidOverride {
    let previous = EFFECTIVE_UID_OVERRIDE.with(|override_uid| override_uid.replace(Some(uid)));
    EffectiveUidOverride(previous)
}

fn effective_uid() -> u32 {
    #[cfg(test)]
    if let Some(uid) = EFFECTIVE_UID_OVERRIDE.with(std::cell::Cell::get) {
        return uid;
    }
    rustix::process::geteuid().as_raw()
}

fn blob_components(name: &str) -> std::io::Result<Vec<OsString>> {
    let components = Path::new(name)
        .components()
        .map(|component| match component {
            std::path::Component::Normal(name) => Ok(name.to_owned()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "unsafe blob name",
            )),
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    if components.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "unsafe blob name",
        ));
    }
    Ok(components)
}

pub(crate) fn validate_owner(uid: u32) -> std::io::Result<()> {
    if uid != effective_uid() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "local state is owned by another user",
        ));
    }
    Ok(())
}

fn validate_directory(file: &File) -> std::io::Result<()> {
    let metadata = file.metadata()?;
    validate_owner(metadata.uid())?;
    if !metadata.is_dir() {
        return Err(std::io::Error::other("unsafe blob directory"));
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(target_os = "linux"))]
fn open_directory_path(root: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    let file = options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)?;
    validate_directory(&file)?;
    Ok(file)
}

#[cfg(target_os = "linux")]
fn walk_private_directory(path: &Path, create: bool) -> std::io::Result<File> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => components.push(component.to_owned()),
            Component::ParentDir | Component::Prefix(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "unsafe state directory path",
                ));
            }
        }
    }
    if components.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "state directory path has no private component",
        ));
    }

    // Shipped daemon setup retains an already-validated directory descriptor
    // and passes it to lower layers through its process-local procfd name. The
    // descriptor, rather than any renameable ancestor pathname, remains the
    // authority. Follow exactly that procfs descriptor link once, then resume
    // the normal no-follow descriptor walk for all descendants.
    let procfd = path.is_absolute()
        && components.len() >= 4
        && components[0] == "proc"
        && components[1] == "self"
        && components[2] == "fd"
        && components[3].to_str().is_some_and(|value| {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        });
    let (mut current, first_component) = if procfd {
        let anchor = Path::new("/proc/self/fd").join(&components[3]);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(anchor)?;
        validate_directory(&file)?;
        (file, 4)
    } else {
        let anchor = if path.is_absolute() {
            Path::new("/")
        } else {
            Path::new(".")
        };
        (
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(anchor)?,
            0,
        )
    };
    for (index, component) in components.iter().enumerate().skip(first_component) {
        let last = index + 1 == components.len();
        let opened = rustix::fs::openat(
            &current,
            component,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        );
        let (descriptor, created) = match opened {
            Ok(descriptor) => (descriptor, false),
            Err(rustix::io::Errno::NOENT) if create => {
                match rustix::fs::mkdirat(
                    &current,
                    component,
                    rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR | rustix::fs::Mode::XUSR,
                ) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(std::io::Error::from(error)),
                }
                current.sync_all()?;
                (
                    rustix::fs::openat(
                        &current,
                        component,
                        rustix::fs::OFlags::RDONLY
                            | rustix::fs::OFlags::DIRECTORY
                            | rustix::fs::OFlags::NOFOLLOW
                            | rustix::fs::OFlags::CLOEXEC,
                        rustix::fs::Mode::empty(),
                    )
                    .map_err(std::io::Error::from)?,
                    true,
                )
            }
            Err(error) => return Err(std::io::Error::from(error)),
        };
        let next = File::from(descriptor);
        if created || last {
            validate_directory(&next)?;
        }
        current = next;
    }
    Ok(current)
}

#[doc(hidden)]
pub fn open_private_directory(path: &Path) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        walk_private_directory(path, false)
    }
    #[cfg(not(target_os = "linux"))]
    {
        open_directory_path(path)
    }
}

#[doc(hidden)]
pub fn create_private_directory(path: &Path) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        walk_private_directory(path, true)
    }
    #[cfg(not(target_os = "linux"))]
    {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(std::io::Error::other("unsafe state directory"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir_all(path)?;
            }
            Err(error) => return Err(error),
        }
        open_directory_path(path)
    }
}

/// Create or open one private child directory relative to an already-open
/// trusted parent. The name is deliberately one component so callers cannot
/// smuggle a second pathname traversal into an anchored operation.
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub fn create_private_subdirectory(parent: &File, name: &std::ffi::OsStr) -> std::io::Result<File> {
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(component)) if component == name)
        || components.next().is_some()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "private child directory name must be one component",
        ));
    }
    let descriptor = match rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    ) {
        Ok(descriptor) => descriptor,
        Err(rustix::io::Errno::NOENT) => {
            match rustix::fs::mkdirat(
                parent,
                name,
                rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR | rustix::fs::Mode::XUSR,
            ) {
                Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(std::io::Error::from(error)),
            }
            parent.sync_all()?;
            rustix::fs::openat(
                parent,
                name,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(std::io::Error::from)?
        }
        Err(error) => return Err(std::io::Error::from(error)),
    };
    let directory = File::from(descriptor);
    validate_directory(&directory)?;
    Ok(directory)
}

fn open_root(root: &Path) -> std::io::Result<File> {
    open_private_directory(root)
}

pub(crate) fn open_root_for_scan(root: &Path) -> std::io::Result<File> {
    open_root(root)
}

fn open_blob_parent(root: &File, name: &str, create: bool) -> std::io::Result<(File, OsString)> {
    let components = blob_components(name)?;
    let (name, directories) = components
        .split_last()
        .expect("nonempty blob components checked");
    let mut current = File::from(rustix::io::dup(root).map_err(std::io::Error::from)?);
    for component in directories {
        let opened = rustix::fs::openat(
            &current,
            component,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        );
        let descriptor = match opened {
            Ok(descriptor) => descriptor,
            Err(rustix::io::Errno::NOENT) if create => {
                match rustix::fs::mkdirat(
                    &current,
                    component,
                    rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR | rustix::fs::Mode::XUSR,
                ) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(std::io::Error::from(error)),
                }
                current.sync_all()?;
                rustix::fs::openat(
                    &current,
                    component,
                    rustix::fs::OFlags::RDONLY
                        | rustix::fs::OFlags::DIRECTORY
                        | rustix::fs::OFlags::NOFOLLOW
                        | rustix::fs::OFlags::CLOEXEC,
                    rustix::fs::Mode::empty(),
                )
                .map_err(std::io::Error::from)?
            }
            Err(error) => return Err(std::io::Error::from(error)),
        };
        let next = File::from(descriptor);
        validate_directory(&next)?;
        current = next;
    }
    Ok((current, name.clone()))
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn prepare_blob_root(root: &Path) -> std::io::Result<()> {
    create_private_directory(root).map(|_| ())
}

pub(crate) fn validate_open_file(file: &File) -> std::io::Result<()> {
    let metadata = file.metadata()?;
    validate_owner(metadata.uid())?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(std::io::Error::other("unsafe blob file type or link count"));
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[derive(Clone)]
pub struct FileBlobs {
    root: Arc<File>,
    normal: Arc<Semaphore>,
    ordered: Arc<Mutex<()>>,
}

impl FileBlobs {
    pub fn new(root: &Path) -> Self {
        Self {
            root: Arc::new(open_root(root).expect("secure blob root")),
            normal: Arc::new(Semaphore::new(FILE_WORKERS)),
            ordered: Arc::new(Mutex::new(())),
        }
    }

    async fn run_normal<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&File) -> Result<T, BlobError> + Send + 'static,
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
        operation: impl FnOnce(&File) -> Result<T, BlobError> + Send + 'static,
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

fn open_blob_file(
    parent: &File,
    name: &std::ffi::OsStr,
    flags: rustix::fs::OFlags,
) -> std::io::Result<File> {
    rustix::fs::openat(
        parent,
        name,
        flags | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .map(File::from)
    .map_err(std::io::Error::from)
}

fn remove_blob_at(parent: &File, name: &std::ffi::OsStr) -> std::io::Result<()> {
    match rustix::fs::statat(parent, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => {
            validate_owner(stat.st_uid)?;
            if stat.st_nlink != 1 || (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
                return Err(std::io::Error::other("unsafe blob file type or link count"));
            }
        }
        Err(rustix::io::Errno::NOENT) => return Ok(()),
        Err(error) => return Err(std::io::Error::from(error)),
    }
    rustix::fs::unlinkat(parent, name, rustix::fs::AtFlags::empty())
        .map_err(std::io::Error::from)?;
    parent.sync_all()
}

fn remove_blob_durable(root: &File, name: &str) -> std::io::Result<()> {
    let (parent, name) = open_blob_parent(root, name, false)?;
    remove_blob_at(&parent, &name)
}

fn write_blob(root: &File, name: &str, bytes: &[u8]) -> Result<(), BlobError> {
    let (parent, basename) = open_blob_parent(root, name, true).map_err(|_| BlobError::Io)?;
    let mut created = false;
    let result = (|| -> std::io::Result<()> {
        let mut file = match open_blob_file(
            &parent,
            &basename,
            rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::CREATE | rustix::fs::OFlags::EXCL,
        ) {
            Ok(file) => {
                validate_open_file(&file)?;
                created = true;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let mut existing = open_blob_file(&parent, &basename, rustix::fs::OFlags::RDONLY)?;
                validate_open_file(&existing)?;
                let mut found = Vec::new();
                existing.read_to_end(&mut found)?;
                return if found == bytes { Ok(()) } else { Err(error) };
            }
            Err(error) => return Err(error),
        };
        file.write_all(bytes)?;
        file.sync_all()?;
        parent.sync_all()
    })();
    classify_write(result, || {
        if created {
            remove_blob_at(&parent, &basename)
        } else {
            Ok(())
        }
    })
}

fn append_blob(root: &File, name: &str, bytes: &[u8]) -> Result<(), BlobError> {
    let (parent, basename) = open_blob_parent(root, name, true).map_err(|_| BlobError::Io)?;
    let mut created = false;
    let mut file = match open_blob_file(
        &parent,
        &basename,
        rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::APPEND,
    ) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            created = true;
            open_blob_file(
                &parent,
                &basename,
                rustix::fs::OFlags::WRONLY
                    | rustix::fs::OFlags::APPEND
                    | rustix::fs::OFlags::CREATE
                    | rustix::fs::OFlags::EXCL,
            )
            .map_err(|_| BlobError::Io)?
        }
        Err(_) => return Err(BlobError::Io),
    };
    validate_open_file(&file).map_err(|_| BlobError::Io)?;
    let original_len = file.metadata().map_err(|_| BlobError::Io)?.len();
    let result = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        parent.sync_all()
    })();
    classify_write(result, || {
        if created {
            remove_blob_at(&parent, &basename)
        } else {
            file.set_len(original_len)?;
            file.sync_all()?;
            parent.sync_all()
        }
    })
}

fn replace_blob_tail_if_len(
    root: &File,
    name: &str,
    expected_total_len: u64,
    valid_prefix_len: u64,
    bytes: &[u8],
) -> Result<bool, BlobError> {
    if valid_prefix_len > expected_total_len {
        return Err(BlobError::Io);
    }
    let (parent, basename) = open_blob_parent(root, name, true).map_err(|_| BlobError::Io)?;
    let mut created = false;
    let mut file = match open_blob_file(
        &parent,
        &basename,
        rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::APPEND,
    ) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            created = true;
            open_blob_file(
                &parent,
                &basename,
                rustix::fs::OFlags::WRONLY
                    | rustix::fs::OFlags::APPEND
                    | rustix::fs::OFlags::CREATE
                    | rustix::fs::OFlags::EXCL,
            )
            .map_err(|_| BlobError::Io)?
        }
        Err(_) => return Err(BlobError::Io),
    };
    validate_open_file(&file).map_err(|_| BlobError::Io)?;
    let original_len = file.metadata().map_err(|_| BlobError::Io)?.len();
    if original_len != expected_total_len {
        if created {
            remove_blob_at(&parent, &basename).map_err(|_| BlobError::Io)?;
        }
        return Ok(false);
    }
    let result = (|| -> std::io::Result<()> {
        file.set_len(valid_prefix_len)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        parent.sync_all()
    })();
    classify_write(result, || {
        if created {
            remove_blob_at(&parent, &basename)
        } else {
            file.set_len(valid_prefix_len)?;
            file.sync_all()?;
            parent.sync_all()
        }
    })?;
    Ok(true)
}

fn delete_many(root: &File, names: Vec<String>) -> std::io::Result<()> {
    for name in names {
        remove_blob_durable(root, &name)?;
    }
    Ok(())
}

impl Blobs for FileBlobs {
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
        self.run_normal(|root| {
            Ok(crate::blobscan::scan_blob_fd_for_recovery(root)
                .map_err(|_| BlobError::Io)?
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

    async fn replace_tail_if_len(
        &self,
        name: String,
        expected_total_len: u64,
        valid_prefix_len: u64,
        bytes: Vec<u8>,
    ) -> Result<bool, BlobError> {
        self.run_ordered(move |root| {
            replace_blob_tail_if_len(root, &name, expected_total_len, valid_prefix_len, &bytes)
        })
        .await
    }

    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
        let name = name.to_owned();
        self.run_ordered(move |root| {
            let (parent, basename) =
                open_blob_parent(root, &name, false).map_err(|_| BlobError::Io)?;
            open_blob_file(&parent, &basename, rustix::fs::OFlags::WRONLY)
                .and_then(|file| {
                    validate_open_file(&file)?;
                    file.set_len(len)?;
                    file.sync_all()?;
                    parent.sync_all()
                })
                .map_err(|error| blob_error(&error))
        })
        .await
    }

    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        let name = name.to_owned();
        self.run_normal(move |root| {
            let (parent, basename) = match open_blob_parent(root, &name, false) {
                Ok(found) => found,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(_) => return Err(BlobError::Io),
            };
            match open_blob_file(&parent, &basename, rustix::fs::OFlags::RDONLY) {
                Ok(mut file) => {
                    validate_open_file(&file).map_err(|_| BlobError::Io)?;
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes)
                        .map(|_| Some(bytes))
                        .map_err(|_| BlobError::Io)
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(_) => Err(BlobError::Io),
            }
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
        self.run_normal(move |root| {
            let (parent, basename) = match open_blob_parent(root, &name, false) {
                Ok(found) => found,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(_) => return Err(BlobError::Io),
            };
            match open_blob_file(&parent, &basename, rustix::fs::OFlags::RDONLY) {
                Ok(file) => usize::try_from(len)
                    .map_err(|_| BlobError::Io)
                    .and_then(|len| {
                        validate_open_file(&file).map_err(|_| BlobError::Io)?;
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
            }
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
        map_fault(Arc::clone(&self.store).delete(key.to_owned()).await)
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

    use super::{
        FileBlobs, RuntimeStore, classify_write, open_root, prepare_blob_root, remove_blob_durable,
        validate_owner,
    };
    use crate::fakegcs::{FakeGcs, Fault};
    use crate::{GcsConfig, GcsStore};

    #[tokio::test]
    async fn file_blob_contract_is_durable_and_ordered() {
        let root = tempfile::tempdir().unwrap();
        let blobs = FileBlobs::new(root.path());
        ProductionContext::new(|_| {})
            .scope(async move {
                assert_eq!(blobs.read("missing/parent/blob").await.unwrap(), None);
                assert_eq!(
                    blobs.read_range("missing/parent/blob", 0, 8).await.unwrap(),
                    None
                );
                let member = "v/1/member.rec";
                blobs.write(member.into(), vec![1, 2]).await.unwrap();
                blobs.append(member.into(), vec![3, 4]).await.unwrap();
                assert_eq!(blobs.read(member).await.unwrap(), Some(vec![1, 2, 3, 4]));
                let repaired = "v/1/repaired.rec";
                blobs
                    .write(repaired.into(), vec![1, 2, 3, 4, 5])
                    .await
                    .unwrap();
                assert!(
                    blobs
                        .replace_tail_if_len(repaired.into(), 5, 3, vec![9, 10])
                        .await
                        .unwrap()
                );
                assert_eq!(
                    blobs.read(repaired).await.unwrap(),
                    Some(vec![1, 2, 3, 9, 10])
                );
                assert!(
                    !blobs
                        .replace_tail_if_len(repaired.into(), 4, 3, vec![11])
                        .await
                        .unwrap()
                );
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

    #[tokio::test]
    async fn blob_paths_are_owner_only_and_reject_symlink_and_hardlink_substitution() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let root = tempfile::tempdir().unwrap();
        prepare_blob_root(root.path()).unwrap();
        let blobs = FileBlobs::new(root.path());
        ProductionContext::new(|_| {})
            .scope(async {
                blobs.write("v/1/secure".into(), vec![1]).await.unwrap();
                let path = root.path().join("v/1/secure");
                assert_eq!(
                    std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
                assert_eq!(
                    std::fs::metadata(root.path().join("v/1"))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700
                );

                std::fs::remove_file(&path).unwrap();
                let outside = root.path().join("outside");
                std::fs::write(&outside, b"outside").unwrap();
                symlink(&outside, &path).unwrap();
                assert_eq!(blobs.read("v/1/secure").await, Err(BlobError::Io));
                assert_eq!(
                    blobs.append("v/1/secure".into(), vec![9]).await,
                    Err(BlobError::Io)
                );
                assert_eq!(std::fs::read(&outside).unwrap(), b"outside");

                std::fs::remove_file(&path).unwrap();
                std::fs::hard_link(&outside, &path).unwrap();
                assert_eq!(blobs.read("v/1/secure").await, Err(BlobError::Io));
                assert_eq!(
                    blobs.append("v/1/secure".into(), vec![9]).await,
                    Err(BlobError::Io)
                );
                assert_eq!(std::fs::read(&outside).unwrap(), b"outside");

                assert_eq!(blobs.delete("v/1/secure").await, Err(BlobError::Io));
                assert_eq!(std::fs::read(&outside).unwrap(), b"outside");

                std::fs::remove_file(&path).unwrap();
                blobs.write("v/1/secure".into(), vec![3]).await.unwrap();
                let renamed = root.path().join("renamed-secure");
                std::fs::rename(&path, &renamed).unwrap();
                symlink(&outside, &path).unwrap();
                assert_eq!(blobs.delete("v/1/secure").await, Err(BlobError::Io));
                assert_eq!(std::fs::read(&renamed).unwrap(), [3]);
                assert_eq!(std::fs::read(&outside).unwrap(), b"outside");

                std::fs::remove_file(&path).unwrap();
                let escaped = tempfile::tempdir().unwrap();
                symlink(escaped.path(), root.path().join("v/substituted")).unwrap();
                assert_eq!(
                    blobs.write("v/substituted/blob".into(), vec![7]).await,
                    Err(BlobError::Io)
                );
                assert!(!escaped.path().join("blob").exists());
                assert_eq!(blobs.scan().await, Err(BlobError::Io));
            })
            .await;
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn blob_root_creation_rejects_an_intermediate_symlink() {
        use std::os::unix::fs::symlink;

        let anchor = tempfile::tempdir().expect("anchor");
        let outside = tempfile::tempdir().expect("outside");
        symlink(outside.path(), anchor.path().join("redirect")).expect("intermediate symlink");
        let root = anchor.path().join("redirect/blobs");

        assert!(prepare_blob_root(&root).is_err());
        assert!(!outside.path().join("blobs").exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_blob_root_descriptor_survives_concurrent_path_substitution() {
        use std::os::unix::fs::symlink;

        let anchor = tempfile::tempdir().expect("anchor");
        let outside = tempfile::tempdir().expect("outside");
        let root = anchor.path().join("blobs");
        prepare_blob_root(&root).expect("blob root");
        let blobs = FileBlobs::new(&root);
        let retained = anchor.path().join("retained-blobs");
        std::fs::rename(&root, &retained).expect("rename open root");
        symlink(outside.path(), &root).expect("substitute root");

        ProductionContext::new(|_| {})
            .scope(async {
                blobs
                    .write("v/1/record".to_owned(), vec![1, 2, 3])
                    .await
                    .expect("descriptor-relative write");
            })
            .await;

        assert_eq!(
            std::fs::read(retained.join("v/1/record")).unwrap(),
            [1, 2, 3]
        );
        assert!(!outside.path().join("v/1/record").exists());
    }

    #[test]
    fn hostile_local_owner_is_rejected() {
        let current = rustix::process::geteuid().as_raw();
        validate_owner(current).expect("current owner");
        let foreign = current
            .checked_add(1)
            .unwrap_or_else(|| current.saturating_sub(1));
        let error = validate_owner(foreign).expect_err("foreign owner accepted");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn retryable_write_failures_restore_the_exact_prior_filesystem_state() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("nested");
        std::fs::create_dir_all(&parent).unwrap();

        let immutable = parent.join("partial.blob");
        std::fs::write(&immutable, b"partial").unwrap();
        let root_file = open_root(root.path()).unwrap();
        assert_eq!(
            classify_write(
                Err(std::io::Error::from(std::io::ErrorKind::StorageFull)),
                || remove_blob_durable(&root_file, "nested/partial.blob"),
            ),
            Err(BlobError::Full),
        );
        assert!(!immutable.exists());

        let spool = parent.join("existing.spool");
        std::fs::write(&spool, b"goodpartial").unwrap();
        assert_eq!(
            classify_write(
                Err(std::io::Error::from(std::io::ErrorKind::StorageFull)),
                || {
                    let file = std::fs::OpenOptions::new().write(true).open(&spool)?;
                    file.set_len(4)?;
                    file.sync_all()
                },
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

    /// Regression PROD-009: a failed backend DELETE must be observable through the
    /// actor store seam rather than being reported as a successful deletion.
    #[tokio::test]
    async fn object_store_delete_propagates_backend_failure() {
        let (fake, endpoint) = FakeGcs::start().await;
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: "test".to_owned(),
            prefix: "contract/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let store = RuntimeStore::new(store);
        ProductionContext::new(|_| {})
            .scope(async move {
                store.put("k".into(), vec![1]).await.expect("seed object");
                fake.faults
                    .lock()
                    .expect("fault mutex")
                    .push(Fault::Status(403));

                assert!(
                    store.delete("k").await.is_err(),
                    "permission-denied DELETE was reported as successful"
                );
            })
            .await;
    }
}
