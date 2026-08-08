//! Guest-side `SQLite` VFS backed by the database transport.
//!
//! Durable main, WAL, and rollback-journal bytes cross the host protocol.
//! Rollback journaling is retained for `SQLite`'s crash-safe transition of a new
//! database into the supported steady-state WAL mode.
//! Locking and WAL shared memory are delegated to `SQLite`'s built-in Unix VFS
//! on a volatile sidecar path keyed by attachment generation. This preserves
//! `SQLite`'s mature cross-process locking semantics without persisting `-shm`.

mod client;

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;

use blockd_core::database::{AttachmentId, DatabaseError, DatabaseFile, DatabaseOp, DatabaseReply};
use blockd_core::types::VsetId;
use client::{Client, Endpoint};
use libsqlite3_sys as ffi;

const VFS_NAME_DEFAULT: &str = "blockd";

struct VfsState {
    endpoint: TransportEndpoint,
    lock_root: PathBuf,
    vset: VsetId,
    attachment: AttachmentId,
    parent: *mut ffi::sqlite3_vfs,
}

enum TransportEndpoint {
    Unix(PathBuf),
    #[cfg(target_os = "linux")]
    Vsock {
        cid: u32,
        port: u32,
    },
}

impl TransportEndpoint {
    fn borrowed(&self) -> Endpoint<'_> {
        match self {
            TransportEndpoint::Unix(path) => Endpoint::Unix(path),
            #[cfg(target_os = "linux")]
            TransportEndpoint::Vsock { cid, port } => Endpoint::Vsock {
                cid: *cid,
                port: *port,
            },
        }
    }
}

// `parent` is SQLite's process-global immutable VFS registration. The other
// fields are immutable after registration.
unsafe impl Send for VfsState {}
unsafe impl Sync for VfsState {}

#[repr(C)]
struct RemoteFile {
    base: ffi::sqlite3_file,
    client: Option<Client>,
    local: *mut ffi::sqlite3_file,
    local_layout: Layout,
    /// The parent Unix VFS retains the filename pointer for later `-shm`
    /// derivation, so its allocation must outlive the local file handle.
    _local_name: CString,
}

struct Bundle {
    name: CString,
    state: Box<VfsState>,
    vfs: Box<ffi::sqlite3_vfs>,
    #[cfg(test)]
    drop_probe: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

#[cfg(test)]
impl Drop for Bundle {
    fn drop(&mut self) {
        if let Some(probe) = &self.drop_probe {
            probe.store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

/// Owns one registered VFS. Dropping it prevents new opens through this name;
/// existing database connections remain valid until they close.
pub struct Registration {
    bundle: Option<Box<Bundle>>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let Some(mut bundle) = self.bundle.take() else {
            return;
        };
        // SAFETY: `bundle.vfs` remains allocated for this call and was
        // registered by `register`.
        unsafe {
            ffi::sqlite3_vfs_unregister(&raw mut *bundle.vfs);
        }
        // Existing SQLite connections retain the VFS pointer after it is
        // unregistered. SQLite exposes no connection count at this boundary,
        // so retain the small registration bundle for the process lifetime.
        Box::leak(bundle);
    }
}

fn sqlite_error(error: DatabaseError, operation: c_int) -> c_int {
    match error {
        DatabaseError::NotFound if operation == ffi::SQLITE_CANTOPEN => ffi::SQLITE_CANTOPEN,
        DatabaseError::Busy | DatabaseError::Draining => ffi::SQLITE_BUSY,
        DatabaseError::TooLarge => ffi::SQLITE_FULL,
        DatabaseError::NotAttached
        | DatabaseError::StaleAttachment
        | DatabaseError::InvalidHandle
        | DatabaseError::AlreadyOpen
        | DatabaseError::NotFound
        | DatabaseError::InvalidRequest
        | DatabaseError::Io => operation,
    }
}

fn ffi_guard(body: impl FnOnce() -> c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(body)).unwrap_or(ffi::SQLITE_IOERR)
}

unsafe fn state(vfs: *mut ffi::sqlite3_vfs) -> &'static VfsState {
    // SAFETY: every callback receives the registered VFS whose `pAppData`
    // points to the boxed state owned by `Registration`.
    unsafe { &*((*vfs).pAppData.cast::<VfsState>()) }
}

unsafe fn remote(file: *mut ffi::sqlite3_file) -> &'static mut RemoteFile {
    // SAFETY: remote opens initialize the SQLite-provided buffer as a
    // `RemoteFile`, whose first field is `sqlite3_file`.
    unsafe { &mut *file.cast::<RemoteFile>() }
}

fn file_from_flags(flags: c_int) -> Option<DatabaseFile> {
    if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        Some(DatabaseFile::Main)
    } else if flags & ffi::SQLITE_OPEN_WAL != 0 {
        Some(DatabaseFile::Wal)
    } else if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0 {
        Some(DatabaseFile::Journal)
    } else {
        None
    }
}

fn is_remote_name(name: &CStr, vset: VsetId) -> bool {
    let bytes = name.to_bytes();
    let prefix = format!("vset-{}", vset.0);
    bytes == prefix.as_bytes()
        || bytes == format!("{prefix}-wal").as_bytes()
        || bytes == format!("{prefix}-journal").as_bytes()
}

fn local_name(state: &VfsState) -> CString {
    let path = state.lock_root.join(format!(
        "vset-{}-generation-{}",
        state.vset.0, state.attachment.generation
    ));
    CString::new(path.as_os_str().as_encoded_bytes()).expect("lock path has no NUL")
}

unsafe fn parent_methods(file: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    // SAFETY: a successful parent `xOpen` installs a non-null method table.
    unsafe { &*((*file).pMethods) }
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    out: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    #[allow(clippy::cast_ptr_alignment)]
    ffi_guard(|| {
        // SAFETY: callback pointers and output buffers are supplied by SQLite.
        let state = unsafe { state(vfs) };
        let named_remote = (!name.is_null())
            .then(|| unsafe { CStr::from_ptr(name) })
            .filter(|name| is_remote_name(name, state.vset));
        if named_remote.is_some() && file_from_flags(flags).is_none() {
            // Super-journal and other durable namespaces are intentionally
            // unsupported: cross-vset atomic commit is not promised.
            return ffi::SQLITE_CANTOPEN;
        }
        let remote_kind = named_remote.and_then(|_| file_from_flags(flags));
        let Some(file_kind) = remote_kind else {
            let Some(parent_open) = (unsafe { (*state.parent).xOpen }) else {
                return ffi::SQLITE_CANTOPEN;
            };
            return unsafe { parent_open(state.parent, name, out, flags, out_flags) };
        };

        let Ok(client) = Client::connect(state.endpoint.borrowed(), state.vset, state.attachment)
        else {
            return ffi::SQLITE_CANTOPEN;
        };
        let create = flags & ffi::SQLITE_OPEN_CREATE != 0;
        match client.call(DatabaseOp::Open {
            handle: client.handle,
            file: file_kind,
            create,
        }) {
            Ok(DatabaseReply::Opened { .. }) => {}
            Ok(DatabaseReply::Failed { error, .. }) => {
                return sqlite_error(error, ffi::SQLITE_CANTOPEN);
            }
            _ => return ffi::SQLITE_CANTOPEN,
        }

        let local_size = usize::try_from(unsafe { (*state.parent).szOsFile }).expect("positive");
        let Ok(local_layout) =
            Layout::from_size_align(local_size, std::mem::align_of::<ffi::sqlite3_file>())
        else {
            return ffi::SQLITE_NOMEM;
        };
        // SAFETY: layout is nonzero and retained in `RemoteFile` for deallocation.
        let local = unsafe { alloc_zeroed(local_layout).cast::<ffi::sqlite3_file>() };
        if local.is_null() {
            return ffi::SQLITE_NOMEM;
        }
        let lock_name = local_name(state);
        let Some(parent_open) = (unsafe { (*state.parent).xOpen }) else {
            unsafe { dealloc(local.cast(), local_layout) };
            return ffi::SQLITE_CANTOPEN;
        };
        let lock_flags =
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_MAIN_DB;
        let mut local_out_flags = 0;
        let rc = unsafe {
            parent_open(
                state.parent,
                lock_name.as_ptr(),
                local,
                lock_flags,
                &raw mut local_out_flags,
            )
        };
        if rc != ffi::SQLITE_OK {
            unsafe { dealloc(local.cast(), local_layout) };
            return rc;
        }
        let initialized = RemoteFile {
            base: ffi::sqlite3_file {
                pMethods: &raw const IO_METHODS,
            },
            client: Some(client),
            local,
            local_layout,
            _local_name: lock_name,
        };
        // SAFETY: SQLite allocated at least `szOsFile`, which registration
        // sets to `size_of::<RemoteFile>()` or the larger parent size.
        unsafe { ptr::write(out.cast::<RemoteFile>(), initialized) };
        if !out_flags.is_null() {
            unsafe { *out_flags = flags };
        }
        ffi::SQLITE_OK
    })
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    ffi_guard(|| {
        let remote = unsafe { remote(file) };
        let mut rc = ffi::SQLITE_OK;
        if let Some(client) = remote.client.take() {
            rc = match client.call(DatabaseOp::Close {
                handle: client.handle,
            }) {
                Ok(DatabaseReply::Closed { .. }) => ffi::SQLITE_OK,
                _ => ffi::SQLITE_IOERR_CLOSE,
            };
        }
        if !remote.local.is_null() {
            let methods = unsafe { parent_methods(remote.local) };
            if let Some(close) = methods.xClose {
                let local_rc = unsafe { close(remote.local) };
                if rc == ffi::SQLITE_OK {
                    rc = local_rc;
                }
            }
            unsafe { dealloc(remote.local.cast(), remote.local_layout) };
            remote.local = ptr::null_mut();
        }
        // SAFETY: the object was initialized by `x_open` and will not be
        // used again after SQLite observes a successful/failed close.
        unsafe { ptr::drop_in_place(file.cast::<RemoteFile>()) };
        rc
    })
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    output: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    ffi_guard(|| {
        if amount < 0 || offset < 0 || output.is_null() {
            return ffi::SQLITE_IOERR_READ;
        }
        let remote = unsafe { remote(file) };
        let Some(client) = &remote.client else {
            return ffi::SQLITE_IOERR_READ;
        };
        let amount = usize::try_from(amount).expect("nonnegative");
        let target = unsafe { std::slice::from_raw_parts_mut(output.cast::<u8>(), amount) };
        target.fill(0);
        match client.call(DatabaseOp::Read {
            handle: client.handle,
            offset: u64::try_from(offset).expect("nonnegative"),
            len: u32::try_from(amount).unwrap_or(u32::MAX),
        }) {
            Ok(DatabaseReply::Read { bytes, eof, .. }) => {
                let copy = bytes.len().min(target.len());
                target[..copy].copy_from_slice(&bytes[..copy]);
                if eof || copy < target.len() {
                    ffi::SQLITE_IOERR_SHORT_READ
                } else {
                    ffi::SQLITE_OK
                }
            }
            Ok(DatabaseReply::Failed { error, .. }) => sqlite_error(error, ffi::SQLITE_IOERR_READ),
            _ => ffi::SQLITE_IOERR_READ,
        }
    })
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    ffi_guard(|| {
        if amount < 0 || offset < 0 || input.is_null() {
            return ffi::SQLITE_IOERR_WRITE;
        }
        let remote = unsafe { remote(file) };
        let Some(client) = &remote.client else {
            return ffi::SQLITE_IOERR_WRITE;
        };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                input.cast::<u8>(),
                usize::try_from(amount).expect("positive"),
            )
        };
        match client.call(DatabaseOp::Write {
            handle: client.handle,
            offset: u64::try_from(offset).expect("nonnegative"),
            bytes: bytes.to_vec(),
        }) {
            Ok(DatabaseReply::Written { .. }) => ffi::SQLITE_OK,
            Ok(DatabaseReply::Failed { error, .. }) => sqlite_error(error, ffi::SQLITE_IOERR_WRITE),
            _ => ffi::SQLITE_IOERR_WRITE,
        }
    })
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    ffi_guard(|| {
        if size < 0 {
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        let remote = unsafe { remote(file) };
        let Some(client) = &remote.client else {
            return ffi::SQLITE_IOERR_TRUNCATE;
        };
        match client.call(DatabaseOp::Truncate {
            handle: client.handle,
            size: u64::try_from(size).expect("nonnegative"),
        }) {
            Ok(DatabaseReply::Truncated { .. }) => ffi::SQLITE_OK,
            Ok(DatabaseReply::Failed { error, .. }) => {
                sqlite_error(error, ffi::SQLITE_IOERR_TRUNCATE)
            }
            _ => ffi::SQLITE_IOERR_TRUNCATE,
        }
    })
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    ffi_guard(|| {
        let remote = unsafe { remote(file) };
        let Some(client) = &remote.client else {
            return ffi::SQLITE_IOERR_FSYNC;
        };
        match client.call(DatabaseOp::Sync {
            handle: client.handle,
        }) {
            Ok(DatabaseReply::Synced { .. }) => ffi::SQLITE_OK,
            Ok(DatabaseReply::Failed { error, .. }) => sqlite_error(error, ffi::SQLITE_IOERR_FSYNC),
            _ => ffi::SQLITE_IOERR_FSYNC,
        }
    })
}

unsafe extern "C" fn x_file_size(
    file: *mut ffi::sqlite3_file,
    output: *mut ffi::sqlite3_int64,
) -> c_int {
    ffi_guard(|| {
        if output.is_null() {
            return ffi::SQLITE_IOERR_FSTAT;
        }
        let remote = unsafe { remote(file) };
        let Some(client) = &remote.client else {
            return ffi::SQLITE_IOERR_FSTAT;
        };
        match client.call(DatabaseOp::FileSize {
            handle: client.handle,
        }) {
            Ok(DatabaseReply::FileSize { size, .. }) => match i64::try_from(size) {
                Ok(size) => {
                    unsafe { *output = size };
                    ffi::SQLITE_OK
                }
                Err(_) => ffi::SQLITE_IOERR_FSTAT,
            },
            Ok(DatabaseReply::Failed { error, .. }) => sqlite_error(error, ffi::SQLITE_IOERR_FSTAT),
            _ => ffi::SQLITE_IOERR_FSTAT,
        }
    })
}

macro_rules! local_call {
    ($name:ident($($arg:ident : $ty:ty),*) -> $fallback:expr, $field:ident) => {
        unsafe extern "C" fn $name(file: *mut ffi::sqlite3_file, $($arg: $ty),*) -> c_int {
            ffi_guard(|| {
                let remote = unsafe { remote(file) };
                let methods = unsafe { parent_methods(remote.local) };
                match methods.$field {
                    Some(call) => unsafe { call(remote.local, $($arg),*) },
                    None => $fallback,
                }
            })
        }
    };
}

local_call!(x_lock(level: c_int) -> ffi::SQLITE_IOERR_LOCK, xLock);
local_call!(x_unlock(level: c_int) -> ffi::SQLITE_IOERR_UNLOCK, xUnlock);
local_call!(x_check_reserved(output: *mut c_int) -> ffi::SQLITE_IOERR_CHECKRESERVEDLOCK, xCheckReservedLock);
local_call!(x_shm_lock(offset: c_int, count: c_int, flags: c_int) -> ffi::SQLITE_IOERR_SHMLOCK, xShmLock);
local_call!(x_shm_unmap(delete: c_int) -> ffi::SQLITE_IOERR_SHMMAP, xShmUnmap);

unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    page: c_int,
    page_size: c_int,
    extend: c_int,
    output: *mut *mut c_void,
) -> c_int {
    ffi_guard(|| {
        let remote = unsafe { remote(file) };
        let methods = unsafe { parent_methods(remote.local) };
        match methods.xShmMap {
            Some(call) => unsafe { call(remote.local, page, page_size, extend, output) },
            None => ffi::SQLITE_IOERR_SHMMAP,
        }
    })
}

unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let remote = unsafe { remote(file) };
        let methods = unsafe { parent_methods(remote.local) };
        if let Some(call) = methods.xShmBarrier {
            unsafe { call(remote.local) };
        }
    }));
}

unsafe extern "C" fn x_file_control(
    _file: *mut ffi::sqlite3_file,
    _op: c_int,
    _arg: *mut c_void,
) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn x_sector_size(_file: *mut ffi::sqlite3_file) -> c_int {
    4096
}

unsafe extern "C" fn x_device_characteristics(_file: *mut ffi::sqlite3_file) -> c_int {
    // This VFS accepts and correctly zero-fills arbitrary byte-range reads.
    // SQLite 3.47.2 added this bit so non-standard VFSes can opt in to direct
    // overflow-page reads without implying any write-atomicity guarantees.
    ffi::SQLITE_IOCAP_SUBPAGE_READ
}

unsafe extern "C" fn x_fetch(
    _file: *mut ffi::sqlite3_file,
    _offset: ffi::sqlite3_int64,
    _amount: c_int,
    output: *mut *mut c_void,
) -> c_int {
    if !output.is_null() {
        unsafe { *output = ptr::null_mut() };
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_unfetch(
    _file: *mut ffi::sqlite3_file,
    _offset: ffi::sqlite3_int64,
    _mapping: *mut c_void,
) -> c_int {
    ffi::SQLITE_OK
}

static IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: Some(x_fetch),
    xUnfetch: Some(x_unfetch),
};

fn one_shot(state: &VfsState, op: DatabaseOp) -> Result<DatabaseReply, ()> {
    Client::connect(state.endpoint.borrowed(), state.vset, state.attachment)
        .and_then(|client| client.call(op))
        .map_err(|_| ())
}

unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    ffi_guard(|| {
        let state = unsafe { state(vfs) };
        if name.is_null() {
            return ffi::SQLITE_IOERR_DELETE;
        }
        let name_ref = unsafe { CStr::from_ptr(name) };
        let file = if is_remote_name(name_ref, state.vset) {
            if name_ref.to_bytes().ends_with(b"-wal") {
                DatabaseFile::Wal
            } else if name_ref.to_bytes().ends_with(b"-journal") {
                DatabaseFile::Journal
            } else {
                DatabaseFile::Main
            }
        } else {
            let Some(call) = (unsafe { (*state.parent).xDelete }) else {
                return ffi::SQLITE_IOERR_DELETE;
            };
            return unsafe { call(state.parent, name, sync_dir) };
        };
        match one_shot(state, DatabaseOp::Delete { file }) {
            Ok(DatabaseReply::Deleted { .. }) => {
                if file == DatabaseFile::Main
                    && let Some(delete) = unsafe { (*state.parent).xDelete }
                {
                    let lock_name = local_name(state);
                    let _ = unsafe { delete(state.parent, lock_name.as_ptr(), 0) };
                }
                ffi::SQLITE_OK
            }
            Ok(DatabaseReply::Failed {
                error: DatabaseError::NotFound,
                ..
            }) => ffi::SQLITE_IOERR_DELETE_NOENT,
            Ok(DatabaseReply::Failed { error, .. }) => {
                sqlite_error(error, ffi::SQLITE_IOERR_DELETE)
            }
            _ => ffi::SQLITE_IOERR_DELETE,
        }
    })
}

unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    output: *mut c_int,
) -> c_int {
    ffi_guard(|| {
        let state = unsafe { state(vfs) };
        if name.is_null() || output.is_null() {
            return ffi::SQLITE_IOERR_ACCESS;
        }
        let name_ref = unsafe { CStr::from_ptr(name) };
        if !is_remote_name(name_ref, state.vset) {
            let Some(call) = (unsafe { (*state.parent).xAccess }) else {
                return ffi::SQLITE_IOERR_ACCESS;
            };
            return unsafe { call(state.parent, name, flags, output) };
        }
        let file = if name_ref.to_bytes().ends_with(b"-wal") {
            DatabaseFile::Wal
        } else if name_ref.to_bytes().ends_with(b"-journal") {
            DatabaseFile::Journal
        } else {
            DatabaseFile::Main
        };
        match one_shot(state, DatabaseOp::Access { file }) {
            Ok(DatabaseReply::Access { exists, .. }) => {
                unsafe { *output = c_int::from(exists) };
                ffi::SQLITE_OK
            }
            _ => ffi::SQLITE_IOERR_ACCESS,
        }
    })
}

unsafe extern "C" fn x_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    output_len: c_int,
    output: *mut c_char,
) -> c_int {
    ffi_guard(|| {
        if name.is_null() || output.is_null() || output_len <= 0 {
            return ffi::SQLITE_CANTOPEN_FULLPATH;
        }
        let state = unsafe { state(vfs) };
        let name_ref = unsafe { CStr::from_ptr(name) };
        if !is_remote_name(name_ref, state.vset) {
            let Some(call) = (unsafe { (*state.parent).xFullPathname }) else {
                return ffi::SQLITE_CANTOPEN_FULLPATH;
            };
            return unsafe { call(state.parent, name, output_len, output) };
        }
        let bytes = name_ref.to_bytes_with_nul();
        if bytes.len() > usize::try_from(output_len).expect("positive") {
            return ffi::SQLITE_CANTOPEN_FULLPATH;
        }
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr().cast(), output, bytes.len()) };
        ffi::SQLITE_OK
    })
}

unsafe extern "C" fn x_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    amount: c_int,
    output: *mut c_char,
) -> c_int {
    let state = unsafe { state(vfs) };
    unsafe {
        (*state.parent)
            .xRandomness
            .map_or(0, |call| call(state.parent, amount, output))
    }
}

unsafe extern "C" fn x_sleep(vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    let state = unsafe { state(vfs) };
    unsafe {
        (*state.parent)
            .xSleep
            .map_or(0, |call| call(state.parent, micros))
    }
}

unsafe extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, output: *mut f64) -> c_int {
    let state = unsafe { state(vfs) };
    unsafe {
        (*state.parent)
            .xCurrentTime
            .map_or(ffi::SQLITE_ERROR, |call| call(state.parent, output))
    }
}

unsafe extern "C" fn x_current_time_i64(
    vfs: *mut ffi::sqlite3_vfs,
    output: *mut ffi::sqlite3_int64,
) -> c_int {
    let state = unsafe { state(vfs) };
    unsafe {
        (*state.parent)
            .xCurrentTimeInt64
            .map_or(ffi::SQLITE_ERROR, |call| call(state.parent, output))
    }
}

unsafe extern "C" fn x_set_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    call: ffi::sqlite3_syscall_ptr,
) -> c_int {
    let state = unsafe { state(vfs) };
    unsafe {
        (*state.parent)
            .xSetSystemCall
            .map_or(ffi::SQLITE_NOTFOUND, |forward| {
                forward(state.parent, name, call)
            })
    }
}

unsafe extern "C" fn x_get_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> ffi::sqlite3_syscall_ptr {
    let state = unsafe { state(vfs) };
    unsafe {
        (*state.parent)
            .xGetSystemCall
            .and_then(|forward| forward(state.parent, name))
    }
}

unsafe extern "C" fn x_next_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> *const c_char {
    let state = unsafe { state(vfs) };
    unsafe {
        (*state.parent)
            .xNextSystemCall
            .map_or(ptr::null(), |forward| forward(state.parent, name))
    }
}

/// Register one attachment-scoped Unix-stream VFS. The same endpoint may
/// serve hundreds of registrations/databases; each registration has its own
/// vset and generation authority.
pub fn register_unix(
    name: Option<&str>,
    endpoint: &Path,
    lock_root: &Path,
    vset: VsetId,
    attachment: AttachmentId,
) -> Result<Registration, c_int> {
    register(
        name,
        TransportEndpoint::Unix(endpoint.to_owned()),
        lock_root,
        vset,
        attachment,
    )
}

/// Register one attachment-scoped virtio-vsock VFS inside a Linux guest.
/// Firecracker forwards a guest connection to host CID 2 and `port` to the
/// VM-specific `<uds_path>_<port>` listener.
#[cfg(target_os = "linux")]
pub fn register_vsock(
    name: Option<&str>,
    port: u32,
    lock_root: &Path,
    vset: VsetId,
    attachment: AttachmentId,
) -> Result<Registration, c_int> {
    register(
        name,
        TransportEndpoint::Vsock { cid: 2, port },
        lock_root,
        vset,
        attachment,
    )
}

fn register(
    name: Option<&str>,
    endpoint: TransportEndpoint,
    lock_root: &Path,
    vset: VsetId,
    attachment: AttachmentId,
) -> Result<Registration, c_int> {
    std::fs::create_dir_all(lock_root).map_err(|_| ffi::SQLITE_CANTOPEN)?;
    let name = CString::new(name.unwrap_or(VFS_NAME_DEFAULT)).map_err(|_| ffi::SQLITE_MISUSE)?;
    // SAFETY: null selects SQLite's current default VFS.
    let parent = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
    if parent.is_null() {
        return Err(ffi::SQLITE_NOTFOUND);
    }
    let state = Box::new(VfsState {
        endpoint,
        lock_root: lock_root.to_owned(),
        vset,
        attachment,
        parent,
    });
    let size = std::mem::size_of::<RemoteFile>()
        .max(usize::try_from(unsafe { (*parent).szOsFile }).expect("parent size"));
    let mut bundle = Box::new(Bundle {
        name,
        state,
        vfs: Box::new(ffi::sqlite3_vfs {
            iVersion: 3,
            szOsFile: c_int::try_from(size).map_err(|_| ffi::SQLITE_TOOBIG)?,
            mxPathname: 1024,
            pNext: ptr::null_mut(),
            zName: ptr::null(),
            pAppData: ptr::null_mut(),
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: None,
            xDlError: None,
            xDlSym: None,
            xDlClose: None,
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: None,
            xCurrentTimeInt64: Some(x_current_time_i64),
            xSetSystemCall: Some(x_set_system_call),
            xGetSystemCall: Some(x_get_system_call),
            xNextSystemCall: Some(x_next_system_call),
        }),
        #[cfg(test)]
        drop_probe: None,
    });
    bundle.vfs.zName = bundle.name.as_ptr();
    bundle.vfs.pAppData = (&raw mut *bundle.state).cast();
    // SAFETY: the boxed VFS and all pointers it contains remain stable for
    // the returned registration's lifetime.
    let rc = unsafe { ffi::sqlite3_vfs_register(&raw mut *bundle.vfs, 0) };
    if rc != ffi::SQLITE_OK {
        return Err(rc);
    }
    Ok(Registration {
        bundle: Some(bundle),
    })
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;

    use blockd_core::database::{DatabaseOp, DatabaseReply};
    use blockd_core::dbproto::{decode_request, encode_reply};
    use blockd_core::format::FRAME_HEADER;
    use blockd_core::types::VmId;
    use rusqlite::{Connection, OpenFlags};

    use super::*;

    #[derive(Default)]
    struct FileModel {
        exists: bool,
        bytes: Vec<u8>,
    }

    #[derive(Default)]
    struct Model {
        files: BTreeMap<DatabaseFile, FileModel>,
        handles: BTreeMap<u64, DatabaseFile>,
        sequence: u64,
    }

    impl Model {
        fn execute(&mut self, req: blockd_core::seam::ReqId, op: DatabaseOp) -> DatabaseReply {
            match op {
                DatabaseOp::Open {
                    handle,
                    file,
                    create,
                } => {
                    let model = self.files.entry(file).or_default();
                    if !model.exists && !create {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::NotFound,
                        };
                    }
                    model.exists = true;
                    self.handles.insert(handle, file);
                    DatabaseReply::Opened { req }
                }
                DatabaseOp::Close { handle } => {
                    self.handles.remove(&handle);
                    DatabaseReply::Closed { req }
                }
                DatabaseOp::Read {
                    handle,
                    offset,
                    len,
                } => {
                    let file = self.handles[&handle];
                    let bytes = &self.files[&file].bytes;
                    let start = usize::try_from(offset)
                        .expect("test offset")
                        .min(bytes.len());
                    let end = start.saturating_add(len as usize).min(bytes.len());
                    DatabaseReply::Read {
                        req,
                        bytes: bytes[start..end].to_vec(),
                        eof: end - start < len as usize,
                    }
                }
                DatabaseOp::Write {
                    handle,
                    offset,
                    bytes,
                } => {
                    let file = self.handles[&handle];
                    let model = self.files.get_mut(&file).expect("opened");
                    let start = usize::try_from(offset).expect("test offset");
                    model
                        .bytes
                        .resize(model.bytes.len().max(start + bytes.len()), 0);
                    model.bytes[start..start + bytes.len()].copy_from_slice(&bytes);
                    self.sequence += 1;
                    DatabaseReply::Written {
                        req,
                        sequence: self.sequence,
                    }
                }
                DatabaseOp::Truncate { handle, size } => {
                    let file = self.handles[&handle];
                    self.files
                        .get_mut(&file)
                        .expect("opened")
                        .bytes
                        .resize(usize::try_from(size).expect("test size"), 0);
                    self.sequence += 1;
                    DatabaseReply::Truncated {
                        req,
                        sequence: self.sequence,
                    }
                }
                DatabaseOp::FileSize { handle } => {
                    let file = self.handles[&handle];
                    DatabaseReply::FileSize {
                        req,
                        size: self.files[&file].bytes.len() as u64,
                    }
                }
                DatabaseOp::Access { file } => DatabaseReply::Access {
                    req,
                    exists: self.files.get(&file).is_some_and(|file| file.exists),
                },
                DatabaseOp::Delete { file } => {
                    self.files.insert(file, FileModel::default());
                    self.sequence += 1;
                    DatabaseReply::Deleted {
                        req,
                        sequence: self.sequence,
                    }
                }
                DatabaseOp::Sync { handle } => {
                    assert!(self.handles.contains_key(&handle));
                    DatabaseReply::Synced {
                        req,
                        sequence: self.sequence,
                    }
                }
            }
        }
    }

    fn read_frame(stream: &mut UnixStream) -> Option<Vec<u8>> {
        let mut header = [0u8; FRAME_HEADER];
        stream.read_exact(&mut header).ok()?;
        let len = usize::try_from(u32::from_le_bytes(header[4..8].try_into().expect("length")))
            .expect("fits");
        let mut frame = header.to_vec();
        frame.resize(FRAME_HEADER + len, 0);
        stream.read_exact(&mut frame[FRAME_HEADER..]).ok()?;
        Some(frame)
    }

    fn spawn_server(endpoint: &Path) -> Arc<Mutex<Model>> {
        let listener = UnixListener::bind(endpoint).expect("bind");
        let model = Arc::new(Mutex::new(Model::default()));
        let server_model = model.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let model = server_model.clone();
                thread::spawn(move || {
                    while let Some(frame) = read_frame(&mut stream) {
                        let request = decode_request(VmId(3), &frame).expect("request");
                        let reply = model
                            .lock()
                            .expect("model")
                            .execute(request.req, request.op);
                        stream.write_all(&encode_reply(&reply)).expect("reply");
                    }
                });
            }
        });
        model
    }

    #[test]
    fn wal_transactions_use_remote_bytes_and_local_shm() {
        let root = std::env::temp_dir().join(format!("bdv-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("root");
        let endpoint = root.join("database.sock");
        let model = spawn_server(&endpoint);
        let attachment = AttachmentId {
            vm: VmId(3),
            generation: 4,
        };
        let vfs_name = format!("blockd-test-{}", std::process::id());
        let registration = register_unix(
            Some(&vfs_name),
            &endpoint,
            &root.join("locks"),
            VsetId(7),
            attachment,
        )
        .expect("register");
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        let connection = Connection::open_with_flags_and_vfs("vset-7", flags, vfs_name.as_str())
            .expect("open main");
        let mode: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .expect("enable WAL");
        assert_eq!(mode, "wal");
        connection
            .execute_batch(
                "PRAGMA synchronous=FULL;
                 CREATE TABLE values_under_test(value INTEGER NOT NULL);
                 INSERT INTO values_under_test VALUES (11), (22), (33);",
            )
            .expect("WAL transaction");
        let second = Connection::open_with_flags_and_vfs("vset-7", flags, vfs_name.as_str())
            .expect("second connection");
        let sum: i64 = second
            .query_row("SELECT sum(value) FROM values_under_test", [], |row| {
                row.get(0)
            })
            .expect("query");
        assert_eq!(sum, 66);
        drop(second);
        drop(connection);

        let model = model.lock().expect("model");
        assert!(
            model.files[&DatabaseFile::Main]
                .bytes
                .starts_with(b"SQLite format 3\0")
        );
        assert!(model.sequence > 0);
        drop(model);
        drop(registration);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn registration_bundle_outlives_the_registration_handle() {
        let root = std::env::temp_dir().join(format!("bdv-lifetime-{}", std::process::id()));
        let mut registration = register_unix(
            Some("blockd-lifetime-test"),
            &root.join("unused.sock"),
            &root.join("locks"),
            VsetId(9),
            AttachmentId {
                vm: VmId(3),
                generation: 4,
            },
        )
        .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        registration.bundle.as_mut().unwrap().drop_probe = Some(Arc::clone(&dropped));

        drop(registration);

        assert!(!dropped.load(Ordering::Acquire));
        let _ = std::fs::remove_dir_all(root);
    }
}
