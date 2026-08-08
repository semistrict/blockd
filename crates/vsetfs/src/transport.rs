use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::RawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use fuse_backend_rs::api::server::Server;
use fuse_backend_rs::transport::FsCacheReqHandler;
use fuse_backend_rs::transport::{Reader, VirtioFsWriter, Writer};
use vhost::vhost_user::message::{
    VhostTransferStateDirection, VhostTransferStatePhase, VhostUserMMap, VhostUserMMapFlags,
    VhostUserProtocolFeatures, VhostUserShMemConfig, VhostUserVirtioFeatures,
};
use vhost::vhost_user::{Backend, VhostUserFrontendReqHandler};
use vhost_user_backend::{VhostUserBackend, VhostUserDaemon, VringRwLock, VringT};
use virtio_queue::QueueOwnedT;
use vm_memory::{GuestAddressSpace, GuestMemoryAtomic, GuestMemoryMmap};
use vmm_sys_util::epoll::EventSet;
use vmm_sys_util::event::{
    EventConsumer, EventFlag, EventNotifier, new_event_consumer_and_notifier,
};

use crate::{DatabaseIo, VsetFilesystem};
use blockd_core::vsetfs::DaxMapping;

const MAX_TAG_LEN: usize = 36;
const REQUEST_QUEUES: u32 = 4;
const MAX_QUEUE_SIZE: usize = 32_768;
const VIRTIO_F_VERSION_1: u64 = 32;
const VIRTIO_RING_F_INDIRECT_DESC: u64 = 28;
const MAX_SNAPSHOT_BYTES: u64 = 80 * 1024 * 1024;
pub(crate) const DAX_WINDOW_SIZE: u64 = 64 * 1024 * 1024;
const FORCED_DAX_UNMAP_MARKER: u8 = 0xbd;

struct DaxRequest {
    backend: Backend,
    map_count: Arc<AtomicU64>,
    unmap_count: Arc<AtomicU64>,
    forced: bool,
}

impl FsCacheReqHandler for DaxRequest {
    fn map(
        &mut self,
        foffset: u64,
        moffset: u64,
        len: u64,
        flags: u64,
        fd: RawFd,
    ) -> io::Result<()> {
        let flags = if flags & 1 != 0 {
            VhostUserMMapFlags::WRITABLE.bits()
        } else {
            VhostUserMMapFlags::empty().bits()
        };
        self.backend.shmem_map(
            &VhostUserMMap {
                shmid: 0,
                padding: [0; 7],
                fd_offset: foffset,
                shm_offset: moffset,
                len,
                flags,
            },
            &fd,
        )?;
        self.map_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn unmap(
        &mut self,
        requests: Vec<fuse_backend_rs::abi::virtio_fs::RemovemappingOne>,
    ) -> io::Result<()> {
        for request in requests {
            self.backend.shmem_unmap(&VhostUserMMap {
                shmid: 0,
                padding: if self.forced {
                    [FORCED_DAX_UNMAP_MARKER, 0, 0, 0, 0, 0, 0]
                } else {
                    [0; 7]
                },
                fd_offset: 0,
                shm_offset: request.moffset,
                len: request.len,
                flags: 0,
            })?;
            self.unmap_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// The high-priority queue plus the configured request queues.
const VHOST_USER_FS_QUEUES: usize = REQUEST_QUEUES as usize + 1;

type GuestMemory = GuestMemoryMmap<()>;
type AtomicGuestMemory = GuestMemoryAtomic<GuestMemory>;
type StateTransfer = (VhostTransferStateDirection, JoinHandle<io::Result<()>>);

struct BackendInner<I: DatabaseIo> {
    filesystem: Arc<VsetFilesystem<I>>,
    server: Arc<Server<Arc<VsetFilesystem<I>>>>,
    memory: RwLock<Option<AtomicGuestMemory>>,
    event_idx: AtomicBool,
    frozen: AtomicBool,
    transfer: Mutex<Option<StateTransfer>>,
    backend_req: Mutex<Option<Backend>>,
    dax_map_count: Arc<AtomicU64>,
    dax_unmap_count: Arc<AtomicU64>,
    active_requests: Mutex<usize>,
    requests_idle: Condvar,
    exit_events: Vec<(EventConsumer, EventNotifier)>,
    config: [u8; MAX_TAG_LEN + size_of::<u32>()],
}

/// A vhost-user virtio-fs backend whose filesystem state participates in VM snapshots.
pub struct VsetFsBackend<I: DatabaseIo> {
    inner: Arc<BackendInner<I>>,
}

impl<I: DatabaseIo> Clone for VsetFsBackend<I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<I: DatabaseIo> VsetFsBackend<I> {
    pub fn new(filesystem: VsetFilesystem<I>, tag: &str) -> io::Result<Self> {
        if tag.is_empty() || tag.len() > MAX_TAG_LEN {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }

        let filesystem = Arc::new(filesystem);
        let server = Arc::new(Server::new(Arc::clone(&filesystem)));
        let mut config = [0_u8; MAX_TAG_LEN + size_of::<u32>()];
        config[..tag.len()].copy_from_slice(tag.as_bytes());
        config[MAX_TAG_LEN..].copy_from_slice(&REQUEST_QUEUES.to_le_bytes());
        let exit_events = (0..VHOST_USER_FS_QUEUES)
            .map(|_| new_event_consumer_and_notifier(EventFlag::NONBLOCK))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Self {
            inner: Arc::new(BackendInner {
                filesystem,
                server,
                memory: RwLock::new(None),
                event_idx: AtomicBool::new(false),
                frozen: AtomicBool::new(false),
                transfer: Mutex::new(None),
                backend_req: Mutex::new(None),
                dax_map_count: Arc::new(AtomicU64::new(0)),
                dax_unmap_count: Arc::new(AtomicU64::new(0)),
                active_requests: Mutex::new(0),
                requests_idle: Condvar::new(),
                exit_events,
                config,
            }),
        })
    }

    pub fn filesystem(&self) -> Arc<VsetFilesystem<I>> {
        Arc::clone(&self.inner.filesystem)
    }

    pub fn dax_map_count(&self) -> u64 {
        self.inner.dax_map_count.load(Ordering::Acquire)
    }

    pub fn dax_unmap_count(&self) -> u64 {
        self.inner.dax_unmap_count.load(Ordering::Acquire)
    }

    pub fn revoke_dax_mappings(&self, mappings: &[DaxMapping]) -> io::Result<()> {
        self.unmap_dax_mappings(mappings, true)
    }

    pub fn remove_dax_mappings(&self, mappings: &[DaxMapping]) -> io::Result<()> {
        self.unmap_dax_mappings(mappings, false)
    }

    fn unmap_dax_mappings(&self, mappings: &[DaxMapping], forced: bool) -> io::Result<()> {
        if mappings.is_empty() {
            return Ok(());
        }
        let backend = self
            .inner
            .backend_req
            .lock()
            .map_err(|_| poisoned())?
            .clone()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOTCONN))?;
        let mut request = DaxRequest {
            backend,
            map_count: Arc::clone(&self.inner.dax_map_count),
            unmap_count: Arc::clone(&self.inner.dax_unmap_count),
            forced,
        };
        request.unmap(
            mappings
                .iter()
                .map(
                    |mapping| fuse_backend_rs::abi::virtio_fs::RemovemappingOne {
                        moffset: mapping.window_offset,
                        len: mapping.len,
                    },
                )
                .collect(),
        )
    }

    /// Wake the worker's epoll loop so an owner can join the backend after
    /// the frontend has disconnected.
    pub fn shutdown(&self) -> io::Result<()> {
        for (_, notifier) in &self.inner.exit_events {
            notifier.notify()?;
        }
        Ok(())
    }

    fn process_queue(&self, vring: &VringRwLock<AtomicGuestMemory>) -> io::Result<()> {
        let memory = self
            .inner
            .memory
            .read()
            .map_err(|_| poisoned())?
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "guest memory is unset"))?;
        let memory_guard = memory.memory();
        let mut vring_state = vring.get_mut();
        let chains = vring_state
            .get_queue_mut()
            .iter(memory_guard.clone())
            .map_err(other)?
            .collect::<Vec<_>>();

        for chain in chains {
            let head = chain.head_index();
            let used = (|| {
                let reader =
                    Reader::from_descriptor_chain(&*memory_guard, chain.clone()).map_err(other)?;
                let writer = VirtioFsWriter::new(&*memory_guard, chain).map_err(other)?;
                let backend = self
                    .inner
                    .backend_req
                    .lock()
                    .map_err(|_| poisoned())?
                    .clone();
                let mut dax = backend.map(|backend| DaxRequest {
                    backend,
                    map_count: Arc::clone(&self.inner.dax_map_count),
                    unmap_count: Arc::clone(&self.inner.dax_unmap_count),
                    forced: false,
                });
                self.inner
                    .server
                    .handle_message(
                        reader,
                        Writer::from(writer),
                        dax.as_mut()
                            .map(|handler| handler as &mut dyn FsCacheReqHandler),
                        None,
                    )
                    .map_err(other)
            })()
            .unwrap_or(0);
            let used = u32::try_from(used)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "reply is too large"))?;
            vring_state.add_used(head, used).map_err(other)?;

            if !self.inner.event_idx.load(Ordering::Acquire)
                || vring_state.needs_notification().unwrap_or(true)
            {
                vring_state.signal_used_queue()?;
            }
        }

        Ok(())
    }

    // State transfer is host-side blocking I/O and deliberately runs outside
    // the vhost-user protocol thread; it is not deterministic-core work.
    #[allow(clippy::disallowed_methods)]
    fn begin_state_transfer(
        &self,
        direction: VhostTransferStateDirection,
        phase: VhostTransferStatePhase,
        mut file: File,
    ) -> io::Result<()> {
        if phase != VhostTransferStatePhase::STOPPED {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "only stopped-device state transfer is supported",
            ));
        }

        let mut transfer = self.inner.transfer.lock().map_err(|_| poisoned())?;
        if transfer.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a state transfer is already active",
            ));
        }

        let mut active = self.inner.active_requests.lock().map_err(|_| poisoned())?;
        self.inner.frozen.store(true, Ordering::Release);
        while *active != 0 {
            active = self
                .inner
                .requests_idle
                .wait(active)
                .map_err(|_| poisoned())?;
        }
        drop(active);
        let filesystem = Arc::clone(&self.inner.filesystem);
        let handle = match direction {
            VhostTransferStateDirection::SAVE => thread::spawn(move || {
                let bytes = filesystem.save_state()?;
                file.write_all(&bytes)
            }),
            VhostTransferStateDirection::LOAD => thread::spawn(move || {
                let mut bytes = Vec::new();
                file.take(MAX_SNAPSHOT_BYTES + 1).read_to_end(&mut bytes)?;
                if u64::try_from(bytes.len()).expect("vector size fits u64") > MAX_SNAPSHOT_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "filesystem snapshot exceeds the size limit",
                    ));
                }
                filesystem.replace_state(&bytes)
            }),
        };
        *transfer = Some((direction, handle));
        Ok(())
    }

    fn finish_state_transfer(&self) -> io::Result<()> {
        let (direction, handle) = self
            .inner
            .transfer
            .lock()
            .map_err(|_| poisoned())?
            .take()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "no state transfer is active")
            })?;
        let result = handle
            .join()
            .map_err(|_| io::Error::other("filesystem state-transfer worker panicked"))?;
        let result = result.and_then(|()| {
            if direction != VhostTransferStateDirection::LOAD
                || !self.inner.filesystem.has_dax_mappings()?
            {
                return Ok(());
            }
            let backend = self
                .inner
                .backend_req
                .lock()
                .map_err(|_| poisoned())?
                .clone()
                .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOTCONN))?;
            let mut request = DaxRequest {
                backend,
                map_count: Arc::clone(&self.inner.dax_map_count),
                unmap_count: Arc::clone(&self.inner.dax_unmap_count),
                forced: false,
            };
            self.inner.filesystem.restore_dax_mappings(&mut request)
        });
        self.inner.frozen.store(false, Ordering::Release);
        if let Err(error) = &result {
            eprintln!("blockd-vsetfs state transfer failed: {error}");
        }
        result
    }
}

impl<I: DatabaseIo> VhostUserBackend for VsetFsBackend<I> {
    type Bitmap = ();
    type Vring = VringRwLock<AtomicGuestMemory>;

    fn num_queues(&self) -> usize {
        VHOST_USER_FS_QUEUES
    }

    fn max_queue_size(&self) -> usize {
        MAX_QUEUE_SIZE
    }

    fn features(&self) -> u64 {
        (1 << VIRTIO_F_VERSION_1)
            | (1 << VIRTIO_RING_F_INDIRECT_DESC)
            | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
    }

    fn protocol_features(&self) -> VhostUserProtocolFeatures {
        VhostUserProtocolFeatures::MQ
            | VhostUserProtocolFeatures::CONFIG
            | VhostUserProtocolFeatures::BACKEND_REQ
            | VhostUserProtocolFeatures::REPLY_ACK
            | VhostUserProtocolFeatures::DEVICE_STATE
            | VhostUserProtocolFeatures::RESET_DEVICE
            | VhostUserProtocolFeatures::SHMEM
    }

    fn reset_device(&self) {
        self.inner.event_idx.store(false, Ordering::Release);
    }

    fn set_event_idx(&self, enabled: bool) {
        self.inner.event_idx.store(enabled, Ordering::Release);
    }

    fn queues_per_thread(&self) -> Vec<u64> {
        (0..VHOST_USER_FS_QUEUES)
            .map(|queue| 1_u64 << queue)
            .collect()
    }

    fn exit_event(&self, thread_index: usize) -> Option<(EventConsumer, EventNotifier)> {
        self.inner
            .exit_events
            .get(thread_index)
            .map(|(consumer, notifier)| {
                (
                    consumer.try_clone().expect("clone exit-event consumer"),
                    notifier.try_clone().expect("clone exit-event notifier"),
                )
            })
    }

    fn get_config(&self, offset: u32, size: u32) -> Vec<u8> {
        let offset = offset as usize;
        let size = size as usize;
        let mut result = self
            .inner
            .config
            .get(offset..)
            .unwrap_or_default()
            .iter()
            .take(size)
            .copied()
            .collect::<Vec<_>>();
        result.resize(size, 0);
        result
    }

    fn update_memory(&self, memory: AtomicGuestMemory) -> io::Result<()> {
        *self.inner.memory.write().map_err(|_| poisoned())? = Some(memory);
        Ok(())
    }

    fn set_backend_req_fd(&self, backend: Backend) {
        *self.inner.backend_req.lock().expect("backend request lock") = Some(backend);
    }

    fn get_shmem_config(&self) -> io::Result<VhostUserShMemConfig> {
        Ok(VhostUserShMemConfig::new(1, &[DAX_WINDOW_SIZE]))
    }

    fn handle_event(
        &self,
        device_event: u16,
        evset: EventSet,
        vrings: &[Self::Vring],
        _thread_id: usize,
    ) -> io::Result<()> {
        if evset != EventSet::IN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "virtqueue event is not readable",
            ));
        }
        let mut active = self.inner.active_requests.lock().map_err(|_| poisoned())?;
        if self.inner.frozen.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "filesystem is frozen for snapshot",
            ));
        }
        *active += 1;
        drop(active);
        let index = usize::from(device_event);
        let result = vrings
            .get(index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "virtqueue is missing"))
            .and_then(|vring| self.process_queue(vring));
        let mut active = self.inner.active_requests.lock().map_err(|_| poisoned())?;
        *active = active.checked_sub(1).expect("active request count");
        if *active == 0 {
            self.inner.requests_idle.notify_all();
        }
        result
    }

    fn set_device_state_fd(
        &self,
        direction: VhostTransferStateDirection,
        phase: VhostTransferStatePhase,
        file: File,
    ) -> io::Result<Option<File>> {
        self.begin_state_transfer(direction, phase, file)?;
        Ok(None)
    }

    fn check_device_state(&self) -> io::Result<()> {
        self.finish_state_transfer()
    }
}

/// Serve one frontend connection on a Unix socket.
pub fn serve_vhost_user<I: DatabaseIo, P: AsRef<Path>>(
    backend: VsetFsBackend<I>,
    socket: P,
) -> io::Result<()> {
    let memory = GuestMemoryAtomic::new(GuestMemoryMmap::new());
    let mut daemon =
        VhostUserDaemon::new("blockd-vsetfs".to_owned(), backend, memory).map_err(other)?;
    daemon.serve(socket).map_err(other)
}

fn poisoned() -> io::Error {
    io::Error::other("filesystem backend lock is poisoned")
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom};
    use std::path::PathBuf;

    use blockd_core::database::{AttachmentId, DatabaseError, DatabaseReply, DatabaseRequest};
    use blockd_core::types::{VmId, VsetId};
    use blockd_core::vsetfs::VsetFsState;

    use super::*;

    struct NoopIo;

    impl DatabaseIo for NoopIo {
        fn request(&self, request: DatabaseRequest) -> DatabaseReply {
            DatabaseReply::Failed {
                req: request.req,
                error: DatabaseError::Io,
            }
        }
    }

    fn snapshot_file() -> (PathBuf, File) {
        let path = std::env::temp_dir().join(format!(
            "blockd-vsetfs-state-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        (path, file)
    }

    fn backend(vm: VmId, with_export: bool) -> VsetFsBackend<NoopIo> {
        let mut state = VsetFsState::new(vm);
        if with_export {
            state
                .attach("orders", VsetId(42), AttachmentId { vm, generation: 7 })
                .unwrap();
            state.create_shm("orders").unwrap();
            state.write_shm("orders", 3, b"snapshot").unwrap();
        }
        VsetFsBackend::new(VsetFilesystem::new(state, NoopIo), "vsets").unwrap()
    }

    #[test]
    fn config_contains_tag_and_parallel_request_queues() {
        let backend = backend(VmId(5), false);
        assert_eq!(&backend.get_config(0, 5), b"vsets");
        assert_eq!(
            backend.get_config(u32::try_from(MAX_TAG_LEN).expect("tag length fits u32"), 4),
            REQUEST_QUEUES.to_le_bytes()
        );
        assert_eq!(backend.get_config(100, 3), vec![0, 0, 0]);
        assert_eq!(backend.queues_per_thread(), [1, 2, 4, 8, 16]);
    }

    #[test]
    fn stopped_device_transfer_restores_volatile_filesystem_state() {
        let source = backend(VmId(5), true);
        let expected = source.inner.filesystem.save_state().unwrap();
        let (path, mut file) = snapshot_file();

        source
            .begin_state_transfer(
                VhostTransferStateDirection::SAVE,
                VhostTransferStatePhase::STOPPED,
                file.try_clone().unwrap(),
            )
            .unwrap();
        assert!(source.inner.frozen.load(Ordering::Acquire));
        source.finish_state_transfer().unwrap();
        assert!(!source.inner.frozen.load(Ordering::Acquire));

        file.seek(SeekFrom::Start(0)).unwrap();
        let destination = backend(VmId(5), false);
        destination
            .begin_state_transfer(
                VhostTransferStateDirection::LOAD,
                VhostTransferStatePhase::STOPPED,
                file,
            )
            .unwrap();
        destination.finish_state_transfer().unwrap();
        assert_eq!(destination.inner.filesystem.save_state().unwrap(), expected);
        assert_eq!(
            destination.inner.filesystem.exports().unwrap(),
            vec![(
                "orders".to_owned(),
                VsetId(42),
                AttachmentId {
                    vm: VmId(5),
                    generation: 7,
                },
            )]
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn restore_rejects_a_snapshot_bound_to_another_vm() {
        let source = backend(VmId(5), true);
        let (path, mut file) = snapshot_file();
        source
            .begin_state_transfer(
                VhostTransferStateDirection::SAVE,
                VhostTransferStatePhase::STOPPED,
                file.try_clone().unwrap(),
            )
            .unwrap();
        source.finish_state_transfer().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let destination = backend(VmId(6), false);
        destination
            .begin_state_transfer(
                VhostTransferStateDirection::LOAD,
                VhostTransferStatePhase::STOPPED,
                file,
            )
            .unwrap();
        assert_eq!(
            destination
                .finish_state_transfer()
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EXDEV)
        );
        assert!(!destination.inner.frozen.load(Ordering::Acquire));

        std::fs::remove_file(path).unwrap();
    }
}
