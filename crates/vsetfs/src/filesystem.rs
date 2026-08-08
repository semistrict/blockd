use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use blockd_core::database::{
    DatabaseError, DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest,
};
use blockd_core::seam::ReqId;
use blockd_core::types::VsetId;
use blockd_core::vsetfs::{
    AttachedExport, ByteRangeLock, DaxMapping, Node, NodeKind, ROOT_INODE, VSETS_INODE,
    VsetFsError, VsetFsFile, VsetFsState,
};
use fuse_backend_rs::abi::fuse_abi::{Attr, CreateIn, FsOptions, OpenOptions, SetattrValid};
use fuse_backend_rs::abi::virtio_fs::{RemovemappingOne, SetupmappingFlags};
use fuse_backend_rs::api::filesystem::{
    Context, DirEntry, Entry, FileLock, FileSystem, ZeroCopyReader, ZeroCopyWriter,
};
use fuse_backend_rs::transport::FsCacheReqHandler;

const ATTR_TIMEOUT: Duration = Duration::from_mins(1);
const ENTRY_TIMEOUT: Duration = Duration::from_mins(1);

pub trait DatabaseIo: Send + Sync + 'static {
    fn request(&self, request: DatabaseRequest) -> DatabaseReply;

    /// Reconstruct a snapshot-pinned attachment in the current daemon
    /// incarnation before any restored handle or mapping is used.
    fn restore_attachment(
        &self,
        _vset: VsetId,
        saved: blockd_core::database::AttachmentId,
    ) -> io::Result<RestoredAttachment> {
        Ok(RestoredAttachment {
            attachment: saved,
            created: false,
        })
    }

    fn abort_restored_attachment(
        &self,
        _vset: VsetId,
        _attachment: blockd_core::database::AttachmentId,
    ) {
    }

    fn dax_file(&self, _vset: VsetId, _file: DatabaseFile) -> io::Result<(File, u64)> {
        Err(io::Error::from_raw_os_error(libc::ENOTSUP))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RestoredAttachment {
    pub attachment: blockd_core::database::AttachmentId,
    pub created: bool,
}

pub struct VsetFilesystem<I> {
    state: Mutex<VsetFsState>,
    io: I,
    next_request: AtomicU64,
    shm_dax: Mutex<std::collections::BTreeMap<u64, File>>,
}

impl<I: DatabaseIo> VsetFilesystem<I> {
    pub fn new(state: VsetFsState, io: I) -> Self {
        Self {
            state: Mutex::new(state),
            io,
            next_request: AtomicU64::new(1),
            shm_dax: Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    pub fn save_state(&self) -> io::Result<Vec<u8>> {
        self.flush_all_dax()?;
        Ok(self.lock_state()?.encode_snapshot())
    }

    pub fn attach_export(
        &self,
        name: &str,
        vset: VsetId,
        attachment: blockd_core::database::AttachmentId,
    ) -> io::Result<AttachedExport> {
        self.lock_state()?
            .attach(name, vset, attachment)
            .map_err(vset_error)
    }

    pub fn begin_detach_export(&self, name: &str) -> io::Result<()> {
        self.lock_state()?.begin_detach(name).map_err(vset_error)
    }

    pub fn finish_detach_export(
        &self,
        name: &str,
    ) -> io::Result<blockd_core::database::AttachmentId> {
        self.lock_state()?.finish_detach(name).map_err(vset_error)
    }

    pub fn detachable_export(&self, name: &str) -> io::Result<blockd_core::database::AttachmentId> {
        self.lock_state()?
            .detachable_export(name)
            .map_err(vset_error)
    }

    pub fn draining_export_mappings(&self, name: &str) -> io::Result<Vec<DaxMapping>> {
        self.lock_state()?
            .draining_export_mappings(name)
            .map_err(vset_error)
    }

    pub fn forget_export_mappings(&self, name: &str) -> io::Result<()> {
        self.lock_state()?
            .forget_export_mappings(name)
            .map_err(vset_error)
    }

    pub fn exports(
        &self,
    ) -> io::Result<Vec<(String, VsetId, blockd_core::database::AttachmentId)>> {
        Ok(self.lock_state()?.restore_exports())
    }

    pub fn force_detach_export(&self, name: &str) -> io::Result<Vec<DaxMapping>> {
        self.lock_state()?.force_detach(name).map_err(vset_error)
    }

    pub fn replace_state(&self, bytes: &[u8]) -> io::Result<()> {
        let mut restored = VsetFsState::decode_snapshot(bytes)
            .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
        if restored.vm() != self.lock_state()?.vm() {
            return Err(io::Error::from_raw_os_error(libc::EXDEV));
        }

        let mut attachments = Vec::new();
        for (name, vset, saved) in restored.restore_exports() {
            let restored_attachment = match self.io.restore_attachment(vset, saved) {
                Ok(attachment) => attachment,
                Err(error) => {
                    self.rollback_restore(&restored, &[], &attachments);
                    return Err(error);
                }
            };
            if let Err(error) =
                restored.rebind_restored_attachment(&name, saved, restored_attachment.attachment)
            {
                if restored_attachment.created {
                    self.io
                        .abort_restored_attachment(vset, restored_attachment.attachment);
                }
                self.rollback_restore(&restored, &[], &attachments);
                return Err(vset_error(error));
            }
            attachments.push((vset, restored_attachment));
        }

        let mut opened = Vec::new();
        for handle in restored.open_handles() {
            let Some(node) = restored.node(handle.inode) else {
                self.rollback_restore(&restored, &opened, &attachments);
                return Err(io::Error::from_raw_os_error(libc::ESTALE));
            };
            let NodeKind::File(file) = node.kind else {
                self.rollback_restore(&restored, &opened, &attachments);
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            };
            let Some(durable) = file.durable_file() else {
                continue;
            };
            match self.raw_request(
                node,
                DatabaseOp::Open {
                    handle: handle.handle,
                    file: durable,
                    create: false,
                },
            ) {
                Ok(DatabaseReply::Opened { .. }) => opened.push((node, handle.handle)),
                Ok(DatabaseReply::Failed {
                    error: DatabaseError::AlreadyOpen,
                    ..
                }) => {}
                Ok(DatabaseReply::Failed { error, .. }) => {
                    self.rollback_restore(&restored, &opened, &attachments);
                    return Err(database_error(error));
                }
                Ok(_) => {
                    self.rollback_restore(&restored, &opened, &attachments);
                    return Err(io::Error::from_raw_os_error(libc::EPROTO));
                }
                Err(error) => {
                    self.rollback_restore(&restored, &opened, &attachments);
                    return Err(error);
                }
            }
        }

        match self.lock_state() {
            Ok(mut state) => *state = restored,
            Err(error) => {
                self.rollback_restore(&restored, &opened, &attachments);
                return Err(error);
            }
        }
        Ok(())
    }

    fn rollback_restore(
        &self,
        _state: &VsetFsState,
        opened: &[(Node, u64)],
        attachments: &[(VsetId, RestoredAttachment)],
    ) {
        for &(node, handle) in opened.iter().rev() {
            let _ = self.raw_request(node, DatabaseOp::Close { handle });
        }
        for &(vset, attachment) in attachments.iter().rev() {
            if attachment.created {
                self.io
                    .abort_restored_attachment(vset, attachment.attachment);
            }
        }
    }

    pub fn has_dax_mappings(&self) -> io::Result<bool> {
        Ok(!self.lock_state()?.mappings().is_empty())
    }

    pub fn restore_dax_mappings(&self, vu_req: &mut dyn FsCacheReqHandler) -> io::Result<()> {
        let mappings = self.lock_state()?.mappings().to_vec();
        for mapping in mappings {
            let (node, file, handle, temporary) = {
                let mut state = self.lock_state()?;
                let node = state
                    .node(mapping.inode)
                    .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
                let NodeKind::File(file) = node.kind else {
                    return Err(io::Error::from_raw_os_error(libc::EINVAL));
                };
                if let Some(handle) = state.handle_for_inode(mapping.inode) {
                    (node, file, handle, false)
                } else {
                    let handle = state
                        .open(mapping.inode, libc::O_RDWR as u32)
                        .map_err(vset_error)?
                        .handle;
                    (node, file, handle, true)
                }
            };
            if temporary && let Some(durable) = file.durable_file() {
                match self.request(node, DatabaseOp::Access { file: durable })? {
                    DatabaseReply::Access { exists: false, .. } => {
                        self.lock_state()?
                            .unmap(mapping.window_offset, mapping.len)
                            .map_err(vset_error)?;
                        continue;
                    }
                    DatabaseReply::Access { exists: true, .. } => {}
                    _ => return Err(io::Error::from_raw_os_error(libc::EPROTO)),
                }
                match self.request(
                    node,
                    DatabaseOp::Open {
                        handle,
                        file: durable,
                        create: false,
                    },
                )? {
                    DatabaseReply::Opened { .. } => {}
                    _ => return Err(io::Error::from_raw_os_error(libc::EPROTO)),
                }
            }
            let (backing, base) =
                self.dax_backing(node, file, handle, mapping.file_offset, mapping.len)?;
            vu_req.map(
                base + mapping.file_offset,
                mapping.window_offset,
                mapping.len,
                u64::from(mapping.writable),
                backing.as_raw_fd(),
            )?;
            if temporary {
                if file.durable_file().is_some() {
                    match self.request(node, DatabaseOp::Close { handle })? {
                        DatabaseReply::Closed { .. } => {}
                        _ => return Err(io::Error::from_raw_os_error(libc::EPROTO)),
                    }
                }
                self.lock_state()?.close(handle).map_err(vset_error)?;
            }
        }
        Ok(())
    }

    fn lock_state(&self) -> io::Result<MutexGuard<'_, VsetFsState>> {
        self.state
            .lock()
            .map_err(|_| io::Error::from_raw_os_error(libc::EIO))
    }

    fn request(&self, node: Node, op: DatabaseOp) -> io::Result<DatabaseReply> {
        match self.raw_request(node, op)? {
            DatabaseReply::Failed { error, .. } => Err(database_error(error)),
            reply => Ok(reply),
        }
    }

    fn raw_request(&self, node: Node, op: DatabaseOp) -> io::Result<DatabaseReply> {
        let vset = node
            .vset
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
        let attachment = node
            .attachment
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
        let req = ReqId(self.next_request.fetch_add(1, Ordering::Relaxed));
        let reply = self.io.request(DatabaseRequest {
            req,
            vset,
            attachment,
            op,
        });
        match reply {
            reply if reply.req() == req => Ok(reply),
            _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
        }
    }

    fn durable_exists(&self, node: Node, file: DatabaseFile) -> io::Result<bool> {
        match self.request(node, DatabaseOp::Access { file })? {
            DatabaseReply::Access { exists, .. } => Ok(exists),
            _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
        }
    }

    fn durable_size(&self, node: Node, file: DatabaseFile) -> io::Result<u64> {
        let handle = {
            let mut state = self.lock_state()?;
            state
                .open(node.inode, libc::O_RDONLY as u32)
                .map_err(vset_error)?
        };
        let opened = self.request(
            node,
            DatabaseOp::Open {
                handle: handle.handle,
                file,
                create: false,
            },
        );
        if let Err(error) = opened {
            self.lock_state()?
                .close(handle.handle)
                .map_err(vset_error)?;
            return Err(error);
        }
        let result = self
            .request(
                node,
                DatabaseOp::FileSize {
                    handle: handle.handle,
                },
            )
            .and_then(|reply| match reply {
                DatabaseReply::FileSize { size, .. } => Ok(size),
                _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
            });
        let close = self.request(
            node,
            DatabaseOp::Close {
                handle: handle.handle,
            },
        );
        self.lock_state()?
            .close(handle.handle)
            .map_err(vset_error)?;
        match result {
            Ok(size) => close.map(|_| size),
            Err(error) => Err(error),
        }
    }

    fn file_size(&self, node: Node, file: VsetFsFile) -> io::Result<u64> {
        if let Some(file) = file.durable_file() {
            self.durable_size(node, file)
        } else {
            let state = self.lock_state()?;
            let name = state
                .export_name(node.inode)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
            Ok(u64::try_from(state.shm(name).unwrap_or_default().len()).expect("SHM is bounded"))
        }
    }

    fn entry(&self, node: Node) -> io::Result<Entry> {
        let size = match node.kind {
            NodeKind::File(file) => self.file_size(node, file)?,
            _ => 0,
        };
        let attr = Attr {
            ino: guest_inode(node.inode),
            size,
            blocks: size.div_ceil(512),
            blksize: 4096,
            nlink: 1,
            mode: match node.kind {
                NodeKind::Root | NodeKind::Vsets | NodeKind::Attachment => libc::S_IFDIR | 0o755,
                NodeKind::File(_) => libc::S_IFREG | 0o600,
            },
            ..Attr::default()
        };
        Ok(Entry {
            inode: node.inode,
            generation: node
                .attachment
                .map_or(1, |attachment| attachment.generation),
            attr: attr.into(),
            attr_flags: 0,
            attr_timeout: ATTR_TIMEOUT,
            entry_timeout: ENTRY_TIMEOUT,
        })
    }

    fn inode_and_handle(&self, inode: u64, handle: u64) -> io::Result<(Node, VsetFsFile)> {
        let state = self.lock_state()?;
        let open = state
            .handle(handle)
            .filter(|open| open.inode == inode)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBADF))?;
        let node = state
            .node(open.inode)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
        let NodeKind::File(file) = node.kind else {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        };
        Ok((node, file))
    }

    fn dax_inode_handle(&self, inode: u64) -> io::Result<(Node, VsetFsFile, u64, bool)> {
        let (node, file, handle, temporary) = {
            let mut state = self.lock_state()?;
            let node = state
                .node(inode)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
            let NodeKind::File(file) = node.kind else {
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            };
            if let Some(handle) = state.handle_for_inode(inode) {
                (node, file, handle, false)
            } else {
                let handle = state
                    .open(inode, libc::O_RDWR as u32)
                    .map_err(vset_error)?
                    .handle;
                (node, file, handle, true)
            }
        };
        if temporary && let Some(durable) = file.durable_file() {
            let opened = self.request(
                node,
                DatabaseOp::Open {
                    handle,
                    file: durable,
                    create: false,
                },
            );
            if !matches!(opened, Ok(DatabaseReply::Opened { .. })) {
                let _ = self.lock_state()?.close(handle);
                return match opened {
                    Err(error) => Err(error),
                    Ok(_) => Err(io::Error::from_raw_os_error(libc::EPROTO)),
                };
            }
        }
        Ok((node, file, handle, temporary))
    }

    fn finish_dax_inode_handle(
        &self,
        node: Node,
        file: VsetFsFile,
        handle: u64,
        temporary: bool,
        operation: io::Result<()>,
    ) -> io::Result<()> {
        if !temporary {
            return operation;
        }
        let remote_close = if file.durable_file().is_some() {
            self.request(node, DatabaseOp::Close { handle })
                .and_then(|reply| match reply {
                    DatabaseReply::Closed { .. } => Ok(()),
                    _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
                })
        } else {
            Ok(())
        };
        let local_close = self.lock_state()?.close(handle).map_err(vset_error);
        operation.and(remote_close).and(local_close)
    }

    fn shm_name(&self, inode: u64) -> io::Result<String> {
        self.lock_state()?
            .export_name(inode)
            .map(str::to_owned)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))
    }

    fn sync_handle(&self, node: Node, file: VsetFsFile, handle: u64) -> io::Result<()> {
        self.flush_dax_inode(node, file, handle)?;
        if file.durable_file().is_none() {
            return Ok(());
        }
        match self.request(node, DatabaseOp::Sync { handle })? {
            DatabaseReply::Synced { .. } => Ok(()),
            _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
        }
    }

    fn dax_backing(
        &self,
        node: Node,
        file: VsetFsFile,
        handle: u64,
        offset: u64,
        len: u64,
    ) -> io::Result<(File, u64)> {
        if let Some(durable) = file.durable_file() {
            let vset = node
                .vset
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
            let (backing, base) = self.io.dax_file(vset, durable)?;
            let mut cursor = 0_u64;
            while cursor < len {
                let take = (len - cursor).min(blockd_core::database::MAX_DATABASE_IO as u64);
                let take = u32::try_from(take).expect("bounded by database request limit");
                let DatabaseReply::Read { bytes, .. } = self.request(
                    node,
                    DatabaseOp::Read {
                        handle,
                        offset: offset + cursor,
                        len: take,
                    },
                )?
                else {
                    return Err(io::Error::from_raw_os_error(libc::EPROTO));
                };
                if bytes.is_empty() {
                    break;
                }
                backing.write_all_at(&bytes, base + offset + cursor)?;
                cursor += u64::try_from(bytes.len()).expect("request length fits u64");
                if bytes.len() < take as usize {
                    break;
                }
            }
            return Ok((backing, base));
        }

        let mut files = self
            .shm_dax
            .lock()
            .map_err(|_| io::Error::from_raw_os_error(libc::EIO))?;
        if let std::collections::btree_map::Entry::Vacant(entry) = files.entry(node.inode) {
            let backing = vmm_sys_util::tempfile::TempFile::new()?.into_file();
            backing.set_len(blockd_core::vsetfs::MAX_SHM_PER_EXPORT as u64)?;
            let name = self.shm_name(node.inode)?;
            let bytes = self
                .lock_state()?
                .shm(&name)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?
                .to_vec();
            backing.write_all_at(&bytes, 0)?;
            entry.insert(backing);
        }
        Ok((files[&node.inode].try_clone()?, 0))
    }

    fn flush_dax_inode(&self, node: Node, file: VsetFsFile, handle: u64) -> io::Result<()> {
        let mappings: Vec<_> = self
            .lock_state()?
            .mappings()
            .iter()
            .copied()
            .filter(|mapping| mapping.inode == node.inode && mapping.writable)
            .collect();
        if mappings.is_empty() {
            return Ok(());
        }
        if let Some(durable) = file.durable_file() {
            let DatabaseReply::FileSize { size, .. } =
                self.request(node, DatabaseOp::FileSize { handle })?
            else {
                return Err(io::Error::from_raw_os_error(libc::EPROTO));
            };
            let vset = node
                .vset
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
            let (backing, base) = self.io.dax_file(vset, durable)?;
            for mapping in mappings {
                let end = (mapping.file_offset + mapping.len).min(size);
                let mut cursor = mapping.file_offset;
                while cursor < end {
                    let take = usize::try_from(
                        (end - cursor).min(blockd_core::database::MAX_DATABASE_IO as u64),
                    )
                    .expect("bounded by database request limit");
                    let mut bytes = vec![0; take];
                    backing.read_exact_at(&mut bytes, base + cursor)?;
                    match self.request(
                        node,
                        DatabaseOp::Write {
                            handle,
                            offset: cursor,
                            bytes,
                        },
                    )? {
                        DatabaseReply::Written { .. } => {}
                        _ => return Err(io::Error::from_raw_os_error(libc::EPROTO)),
                    }
                    cursor += take as u64;
                }
            }
        } else {
            let name = self.shm_name(node.inode)?;
            let size = self.lock_state()?.shm(&name).map_or(0, <[u8]>::len);
            let files = self
                .shm_dax
                .lock()
                .map_err(|_| io::Error::from_raw_os_error(libc::EIO))?;
            if let Some(backing) = files.get(&node.inode) {
                let mut bytes = vec![0; size];
                backing.read_exact_at(&mut bytes, 0)?;
                self.lock_state()?
                    .replace_shm(&name, bytes)
                    .map_err(vset_error)?;
            }
        }
        Ok(())
    }

    fn flush_all_dax(&self) -> io::Result<()> {
        let inodes: std::collections::BTreeSet<_> = self
            .lock_state()?
            .mappings()
            .iter()
            .filter(|mapping| mapping.writable)
            .map(|mapping| mapping.inode)
            .collect();
        for inode in inodes {
            let (node, file, handle, temporary) = {
                let mut state = self.lock_state()?;
                let node = state
                    .node(inode)
                    .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
                let NodeKind::File(file) = node.kind else {
                    return Err(io::Error::from_raw_os_error(libc::EINVAL));
                };
                if let Some(handle) = state.handle_for_inode(inode) {
                    (node, file, handle, false)
                } else {
                    let handle = state
                        .open(inode, libc::O_RDWR as u32)
                        .map_err(vset_error)?
                        .handle;
                    (node, file, handle, true)
                }
            };

            if let Some(durable) = file.durable_file() {
                let exists = match self.durable_exists(node, durable) {
                    Ok(exists) => exists,
                    Err(error) => {
                        if temporary {
                            let _ = self.lock_state()?.close(handle);
                        }
                        return Err(error);
                    }
                };
                if !exists {
                    if !temporary {
                        return Err(io::Error::from_raw_os_error(libc::EBUSY));
                    }
                    let mut state = self.lock_state()?;
                    state.forget_inode_mappings(inode);
                    state.close(handle).map_err(vset_error)?;
                    continue;
                }
            }

            let mut durable_open = false;
            if temporary && let Some(durable) = file.durable_file() {
                let open_result = self.request(
                    node,
                    DatabaseOp::Open {
                        handle,
                        file: durable,
                        create: false,
                    },
                );
                match open_result {
                    Ok(DatabaseReply::Opened { .. }) => durable_open = true,
                    Ok(_) => {
                        let _ = self.lock_state()?.close(handle);
                        return Err(io::Error::from_raw_os_error(libc::EPROTO));
                    }
                    Err(error) => {
                        let _ = self.lock_state()?.close(handle);
                        return Err(error);
                    }
                }
            }

            let flush_result = self.flush_dax_inode(node, file, handle).and_then(|()| {
                if file.durable_file().is_none() {
                    return Ok(());
                }
                match self.request(node, DatabaseOp::Sync { handle })? {
                    DatabaseReply::Synced { .. } => Ok(()),
                    _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
                }
            });

            if temporary {
                let durable_close = if durable_open {
                    self.request(node, DatabaseOp::Close { handle })
                        .and_then(|reply| match reply {
                            DatabaseReply::Closed { .. } => Ok(()),
                            _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
                        })
                } else {
                    Ok(())
                };
                let state_close = self.lock_state()?.close(handle).map_err(vset_error);
                flush_result.and(durable_close).and(state_close)?;
            } else {
                flush_result?;
            }
        }
        Ok(())
    }

    fn truncate_handle(
        &self,
        node: Node,
        file: VsetFsFile,
        handle: u64,
        size: u64,
    ) -> io::Result<()> {
        if file.durable_file().is_some() {
            match self.request(node, DatabaseOp::Truncate { handle, size })? {
                DatabaseReply::Truncated { .. } => Ok(()),
                _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
            }
        } else {
            let name = self.shm_name(node.inode)?;
            let mut state = self.lock_state()?;
            state.truncate_shm(&name, size).map_err(vset_error)?;
            let bytes = state.shm(&name).unwrap_or_default().to_vec();
            drop(state);
            if let Some(backing) = self
                .shm_dax
                .lock()
                .map_err(|_| io::Error::from_raw_os_error(libc::EIO))?
                .get(&node.inode)
            {
                backing.write_all_at(&bytes, 0)?;
            }
            Ok(())
        }
    }
}

impl<I: DatabaseIo> FileSystem for VsetFilesystem<I> {
    type Inode = u64;
    type Handle = u64;

    fn init(&self, capable: FsOptions) -> io::Result<FsOptions> {
        Ok(capable & (FsOptions::POSIX_LOCKS | FsOptions::FLOCK_LOCKS | FsOptions::MAP_ALIGNMENT))
    }

    fn lookup(&self, _ctx: &Context, parent: u64, name: &CStr) -> io::Result<Entry> {
        let parent = state_inode(parent);
        let name = name
            .to_str()
            .map_err(|_| io::Error::from_raw_os_error(libc::ENOENT))?;
        let node = self
            .lock_state()?
            .lookup(parent, name)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        if let NodeKind::File(file) = node.kind {
            let exists = if let Some(durable) = file.durable_file() {
                self.durable_exists(node, durable)?
            } else {
                let state = self.lock_state()?;
                state
                    .export_name(node.inode)
                    .is_some_and(|export| state.shm_exists(export))
            };
            if !exists {
                return Err(io::Error::from_raw_os_error(libc::ENOENT));
            }
        }
        self.entry(node)
    }

    fn getattr(
        &self,
        _ctx: &Context,
        inode: u64,
        _handle: Option<u64>,
    ) -> io::Result<(libc::stat64, Duration)> {
        let inode = state_inode(inode);
        let node = self
            .lock_state()?
            .node(inode)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        let entry = self.entry(node)?;
        Ok((entry.attr, entry.attr_timeout))
    }

    fn setattr(
        &self,
        _ctx: &Context,
        inode: u64,
        attr: libc::stat64,
        handle: Option<u64>,
        valid: SetattrValid,
    ) -> io::Result<(libc::stat64, Duration)> {
        if valid != SetattrValid::SIZE {
            return Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP));
        }
        if attr.st_size < 0 {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        let handle = handle.ok_or_else(|| io::Error::from_raw_os_error(libc::EBADF))?;
        let (node, file) = self.inode_and_handle(inode, handle)?;
        self.truncate_handle(node, file, handle, attr.st_size.cast_unsigned())?;
        let entry = self.entry(node)?;
        Ok((entry.attr, entry.attr_timeout))
    }

    fn open(
        &self,
        _ctx: &Context,
        inode: u64,
        flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<(Option<u64>, OpenOptions, Option<u32>)> {
        let (handle, node, file, shm_exists) = {
            let mut state = self.lock_state()?;
            let node = state
                .node(inode)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
            let NodeKind::File(file) = node.kind else {
                return Err(io::Error::from_raw_os_error(libc::EISDIR));
            };
            let shm_exists = state
                .export_name(inode)
                .is_some_and(|name| state.shm_exists(name));
            let handle = state.open(inode, flags).map_err(vset_error)?;
            (handle, node, file, shm_exists)
        };
        let opened = if let Some(durable) = file.durable_file() {
            self.request(
                node,
                DatabaseOp::Open {
                    handle: handle.handle,
                    file: durable,
                    create: false,
                },
            )
            .map(|_| ())
        } else if shm_exists {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(libc::ENOENT))
        };
        if let Err(error) = opened {
            self.lock_state()?
                .close(handle.handle)
                .map_err(vset_error)?;
            return Err(error);
        }
        Ok((Some(handle.handle), OpenOptions::empty(), None))
    }

    fn create(
        &self,
        _ctx: &Context,
        parent: u64,
        name: &CStr,
        args: CreateIn,
    ) -> io::Result<(Entry, Option<u64>, OpenOptions, Option<u32>)> {
        let name = name
            .to_str()
            .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
        let (handle, node, file, existed) = {
            let mut state = self.lock_state()?;
            let node = state
                .lookup(parent, name)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EACCES))?;
            let NodeKind::File(file) = node.kind else {
                return Err(io::Error::from_raw_os_error(libc::EISDIR));
            };
            let existed = file == VsetFsFile::Shm
                && state
                    .export_name(node.inode)
                    .is_some_and(|export| state.shm_exists(export));
            let handle = state.open(node.inode, args.flags).map_err(vset_error)?;
            (handle, node, file, existed)
        };
        let result = (|| {
            if let Some(durable) = file.durable_file() {
                let existed = self.durable_exists(node, durable)?;
                if existed && args.flags & libc::O_EXCL as u32 != 0 {
                    Err(io::Error::from_raw_os_error(libc::EEXIST))
                } else {
                    self.request(
                        node,
                        DatabaseOp::Open {
                            handle: handle.handle,
                            file: durable,
                            create: true,
                        },
                    )
                    .map(|_| ())
                }
            } else if existed && args.flags & libc::O_EXCL as u32 != 0 {
                Err(io::Error::from_raw_os_error(libc::EEXIST))
            } else {
                let export = self.shm_name(node.inode)?;
                self.lock_state()?.create_shm(&export).map_err(vset_error)
            }
        })();
        if let Err(error) = result {
            self.lock_state()?
                .close(handle.handle)
                .map_err(vset_error)?;
            return Err(error);
        }
        Ok((
            self.entry(node)?,
            Some(handle.handle),
            OpenOptions::empty(),
            None,
        ))
    }

    fn read(
        &self,
        _ctx: &Context,
        inode: u64,
        handle: u64,
        writer: &mut dyn ZeroCopyWriter,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> io::Result<usize> {
        if size as usize > blockd_core::database::MAX_DATABASE_IO {
            return Err(io::Error::from_raw_os_error(libc::EFBIG));
        }
        let (node, file) = self.inode_and_handle(inode, handle)?;
        let bytes = if file.durable_file().is_some() {
            match self.request(
                node,
                DatabaseOp::Read {
                    handle,
                    offset,
                    len: size,
                },
            )? {
                DatabaseReply::Read { bytes, .. } => bytes,
                _ => return Err(io::Error::from_raw_os_error(libc::EPROTO)),
            }
        } else {
            let name = self.shm_name(inode)?;
            let state = self.lock_state()?;
            if !state.shm_exists(&name) {
                return Err(io::Error::from_raw_os_error(libc::ENOENT));
            }
            let data = state.shm(&name).unwrap_or_default();
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(data.len());
            let end = start.saturating_add(size as usize).min(data.len());
            let len = end - start;
            drop(state);
            if let Some(backing) = self
                .shm_dax
                .lock()
                .map_err(|_| io::Error::from_raw_os_error(libc::EIO))?
                .get(&inode)
            {
                let mut bytes = vec![0; len];
                backing.read_exact_at(&mut bytes, offset)?;
                bytes
            } else {
                self.lock_state()?.shm(&name).unwrap_or_default()[start..end].to_vec()
            }
        };
        writer.write_all(&bytes)?;
        Ok(bytes.len())
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        _ctx: &Context,
        inode: u64,
        handle: u64,
        reader: &mut dyn ZeroCopyReader,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        _flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<usize> {
        if size as usize > blockd_core::database::MAX_DATABASE_IO {
            return Err(io::Error::from_raw_os_error(libc::EFBIG));
        }
        let (node, file) = self.inode_and_handle(inode, handle)?;
        let mut bytes = vec![0; size as usize];
        reader.read_exact(&mut bytes)?;
        if file.durable_file().is_some() {
            match self.request(
                node,
                DatabaseOp::Write {
                    handle,
                    offset,
                    bytes,
                },
            )? {
                DatabaseReply::Written { .. } => Ok(size as usize),
                _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
            }
        } else {
            let name = self.shm_name(inode)?;
            self.lock_state()?
                .write_shm(&name, offset, &bytes)
                .map_err(vset_error)?;
            if let Some(backing) = self
                .shm_dax
                .lock()
                .map_err(|_| io::Error::from_raw_os_error(libc::EIO))?
                .get(&inode)
            {
                backing.write_all_at(&bytes, offset)?;
            }
            Ok(size as usize)
        }
    }

    fn flush(&self, _ctx: &Context, inode: u64, handle: u64, lock_owner: u64) -> io::Result<()> {
        self.inode_and_handle(inode, handle)?;
        self.lock_state()?
            .unlock_owner(inode, lock_owner)
            .map_err(vset_error)
    }

    fn fsync(&self, _ctx: &Context, inode: u64, _datasync: bool, handle: u64) -> io::Result<()> {
        let (node, file) = self.inode_and_handle(inode, handle)?;
        self.sync_handle(node, file, handle)
    }

    fn release(
        &self,
        _ctx: &Context,
        inode: u64,
        _flags: u32,
        handle: u64,
        flush: bool,
        _flock_release: bool,
        lock_owner: Option<u64>,
    ) -> io::Result<()> {
        let (node, file) = self.inode_and_handle(inode, handle)?;
        if let Some(owner) = lock_owner {
            self.lock_state()?
                .unlock_owner(inode, owner)
                .map_err(vset_error)?;
        }
        let sync_result = flush
            .then(|| self.sync_handle(node, file, handle))
            .transpose()
            .map(|_| ());
        let durable_close = if file.durable_file().is_some() {
            self.request(node, DatabaseOp::Close { handle })
                .and_then(|reply| match reply {
                    DatabaseReply::Closed { .. } => Ok(()),
                    _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
                })
        } else {
            Ok(())
        };
        let state_close = self.lock_state()?.close(handle).map_err(vset_error);
        sync_result.and(durable_close).and(state_close)
    }

    fn unlink(&self, _ctx: &Context, parent: u64, name: &CStr) -> io::Result<()> {
        let name = name
            .to_str()
            .map_err(|_| io::Error::from_raw_os_error(libc::ENOENT))?;
        let node = self
            .lock_state()?
            .lookup(parent, name)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        let NodeKind::File(file) = node.kind else {
            return Err(io::Error::from_raw_os_error(libc::EISDIR));
        };
        if let Some(file) = file.durable_file() {
            match self.request(node, DatabaseOp::Delete { file })? {
                DatabaseReply::Deleted { .. } => Ok(()),
                _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
            }
        } else {
            let export = self.shm_name(node.inode)?;
            self.lock_state()?.delete_shm(&export).map_err(vset_error)?;
            self.shm_dax
                .lock()
                .map_err(|_| io::Error::from_raw_os_error(libc::EIO))?
                .remove(&node.inode);
            Ok(())
        }
    }

    fn opendir(
        &self,
        _ctx: &Context,
        inode: u64,
        _flags: u32,
    ) -> io::Result<(Option<u64>, OpenOptions)> {
        let inode = state_inode(inode);
        match self.lock_state()?.node(inode).map(|node| node.kind) {
            Some(NodeKind::Root | NodeKind::Vsets | NodeKind::Attachment) => {
                Ok((Some(inode), OpenOptions::empty()))
            }
            Some(NodeKind::File(_)) => Err(io::Error::from_raw_os_error(libc::ENOTDIR)),
            None => Err(io::Error::from_raw_os_error(libc::ENOENT)),
        }
    }

    fn readdir(
        &self,
        _ctx: &Context,
        inode: u64,
        _handle: u64,
        _size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> io::Result<usize>,
    ) -> io::Result<()> {
        let inode = state_inode(inode);
        let entries = self
            .lock_state()?
            .directory_entries(inode)
            .map_err(vset_error)?;
        let skip = usize::try_from(offset).unwrap_or(usize::MAX);
        for (index, (name, node)) in entries.into_iter().enumerate().skip(skip) {
            let visible = match node.kind {
                NodeKind::File(file) => {
                    if let Some(durable) = file.durable_file() {
                        self.durable_exists(node, durable)?
                    } else {
                        let state = self.lock_state()?;
                        state
                            .export_name(node.inode)
                            .is_some_and(|export| state.shm_exists(export))
                    }
                }
                _ => true,
            };
            if !visible {
                continue;
            }
            if add_entry(DirEntry {
                ino: node.inode,
                offset: u64::try_from(index + 1).expect("entry count is bounded"),
                type_: match node.kind {
                    NodeKind::File(_) => u32::from(libc::DT_REG),
                    _ => u32::from(libc::DT_DIR),
                },
                name: name.as_bytes(),
            })? == 0
            {
                break;
            }
        }
        Ok(())
    }

    fn releasedir(&self, _ctx: &Context, inode: u64, _flags: u32, handle: u64) -> io::Result<()> {
        if state_inode(inode) != handle {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        }
        Ok(())
    }

    fn getlk(
        &self,
        _ctx: &Context,
        inode: u64,
        handle: u64,
        owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> io::Result<FileLock> {
        self.inode_and_handle(inode, handle)?;
        let requested = fuse_lock(handle, owner, lock)?;
        let conflict = self
            .lock_state()?
            .conflicting_lock(requested)
            .map_err(vset_error)?;
        Ok(conflict.map_or(
            FileLock {
                lock_type: libc::F_UNLCK as u32,
                ..lock
            },
            core_lock,
        ))
    }

    fn setlk(
        &self,
        _ctx: &Context,
        inode: u64,
        handle: u64,
        owner: u64,
        lock: FileLock,
        _flags: u32,
    ) -> io::Result<()> {
        self.inode_and_handle(inode, handle)?;
        if lock.lock_type == libc::F_UNLCK as u32 {
            let lock = fuse_lock(handle, owner, lock)?;
            return match self
                .lock_state()?
                .unlock(handle, owner, lock.start, lock.len)
            {
                Ok(()) | Err(VsetFsError::NotFound) => Ok(()),
                Err(error) => Err(vset_error(error)),
            };
        }
        self.lock_state()?
            .lock(fuse_lock(handle, owner, lock)?)
            .map_err(vset_error)
    }

    fn setlkw(
        &self,
        ctx: &Context,
        inode: u64,
        handle: u64,
        owner: u64,
        lock: FileLock,
        flags: u32,
    ) -> io::Result<()> {
        self.setlk(ctx, inode, handle, owner, lock, flags)
    }

    fn setupmapping(
        &self,
        _ctx: &Context,
        inode: u64,
        handle: u64,
        foffset: u64,
        len: u64,
        flags: u64,
        moffset: u64,
        vu_req: &mut dyn FsCacheReqHandler,
    ) -> io::Result<()> {
        // Linux uses `FUSE_INVALID_FH` for DAX faults. The mapping belongs to
        // the inode rather than a particular open file description, so a
        // mapping can outlive the final guest handle. Open a temporary handle
        // for the database I/O needed to materialize such faults.
        let (node, file, handle, temporary) = if handle == u64::MAX {
            self.dax_inode_handle(inode)?
        } else {
            let (node, file) = self.inode_and_handle(inode, handle)?;
            (node, file, handle, false)
        };
        let operation = (|| {
            let flags = SetupmappingFlags::from_bits(flags)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
            let writable = flags.contains(SetupmappingFlags::WRITE);
            let mapping = DaxMapping {
                inode,
                file_offset: foffset,
                window_offset: moffset,
                len,
                writable,
            };
            let (backing, base) = self.dax_backing(node, file, handle, foffset, len)?;
            self.lock_state()?.map(mapping).map_err(vset_error)?;
            if let Err(error) = vu_req.map(
                base + foffset,
                moffset,
                len,
                u64::from(writable),
                backing.as_raw_fd(),
            ) {
                let _ = self.lock_state()?.unmap(moffset, len);
                return Err(error);
            }
            Ok(())
        })();
        self.finish_dax_inode_handle(node, file, handle, temporary, operation)
    }

    fn removemapping(
        &self,
        _ctx: &Context,
        _inode: u64,
        requests: Vec<RemovemappingOne>,
        vu_req: &mut dyn FsCacheReqHandler,
    ) -> io::Result<()> {
        let mappings = self.lock_state()?.mappings().to_vec();
        let mut affected = std::collections::BTreeSet::new();
        for request in &requests {
            let request_end = request
                .moffset
                .checked_add(request.len)
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
            for mapping in &mappings {
                let mapping_end = mapping.window_offset + mapping.len;
                if request.moffset < mapping_end && mapping.window_offset < request_end {
                    affected.insert(mapping.inode);
                }
            }
        }
        for inode in affected {
            let (node, file, handle, temporary) = self.dax_inode_handle(inode)?;
            let operation = self.flush_dax_inode(node, file, handle);
            self.finish_dax_inode_handle(node, file, handle, temporary, operation)?;
        }
        let removals: Vec<_> = requests
            .iter()
            .map(|request| (request.moffset, request.len))
            .collect();
        vu_req.unmap(requests)?;
        for (offset, len) in removals {
            self.lock_state()?.unmap(offset, len).map_err(vset_error)?;
        }
        Ok(())
    }

    fn access(&self, _ctx: &Context, inode: u64, _mask: u32) -> io::Result<()> {
        let inode = state_inode(inode);
        self.lock_state()?
            .node(inode)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        Ok(())
    }
}

/// The mounted virtio-fs root is the export directory itself. The core keeps a
/// synthetic parent for snapshot-format stability, so translate only that
/// boundary and never expose an extra `/vsets/vsets` path to the guest.
const fn state_inode(inode: u64) -> u64 {
    if inode == ROOT_INODE {
        VSETS_INODE
    } else {
        inode
    }
}

const fn guest_inode(inode: u64) -> u64 {
    if inode == VSETS_INODE {
        ROOT_INODE
    } else {
        inode
    }
}

fn fuse_lock(handle: u64, owner: u64, lock: FileLock) -> io::Result<ByteRangeLock> {
    let len = if lock.end == u64::MAX {
        0
    } else {
        lock.end
            .checked_sub(lock.start)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?
    };
    let exclusive = match lock.lock_type {
        value if value == libc::F_RDLCK as u32 => false,
        value if value == libc::F_WRLCK as u32 => true,
        value if value == libc::F_UNLCK as u32 => false,
        _ => return Err(io::Error::from_raw_os_error(libc::EINVAL)),
    };
    Ok(ByteRangeLock {
        handle,
        owner,
        start: lock.start,
        len,
        exclusive,
    })
}

fn core_lock(lock: ByteRangeLock) -> FileLock {
    FileLock {
        start: lock.start,
        end: if lock.len == 0 {
            u64::MAX
        } else {
            lock.start + lock.len - 1
        },
        lock_type: if lock.exclusive {
            libc::F_WRLCK as u32
        } else {
            libc::F_RDLCK as u32
        },
        pid: 0,
    }
}

fn database_error(error: DatabaseError) -> io::Error {
    io::Error::from_raw_os_error(match error {
        DatabaseError::NotAttached | DatabaseError::StaleAttachment => libc::ESTALE,
        DatabaseError::Draining | DatabaseError::Busy => libc::EBUSY,
        DatabaseError::InvalidHandle => libc::EBADF,
        DatabaseError::AlreadyOpen => libc::EEXIST,
        DatabaseError::NotFound => libc::ENOENT,
        DatabaseError::InvalidRequest => libc::EINVAL,
        DatabaseError::TooLarge => libc::EFBIG,
        DatabaseError::Io => libc::EIO,
    })
}

fn vset_error(error: VsetFsError) -> io::Error {
    io::Error::from_raw_os_error(match error {
        VsetFsError::InvalidName | VsetFsError::InvalidRange | VsetFsError::InvalidState => {
            libc::EINVAL
        }
        VsetFsError::WrongVm | VsetFsError::NotFound => libc::ESTALE,
        VsetFsError::AlreadyExists => libc::EEXIST,
        VsetFsError::Draining | VsetFsError::Busy => libc::EBUSY,
        VsetFsError::Conflict => libc::EAGAIN,
        VsetFsError::TooLarge => libc::EFBIG,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Cursor, Read, Write};
    use std::os::unix::io::RawFd;
    use std::sync::Mutex;

    use blockd_core::database::{AttachmentId, DatabaseFile};
    use blockd_core::types::{VmId, VsetId};
    use blockd_core::vsetfs::{ROOT_INODE, VSETS_INODE};
    use fuse_backend_rs::common::file_traits::FileReadWriteVolatile;

    use super::*;

    #[derive(Default)]
    struct FakeState {
        files: BTreeMap<DatabaseFile, Vec<u8>>,
        dax: BTreeMap<DatabaseFile, File>,
        handles: BTreeMap<u64, DatabaseFile>,
        sequence: u64,
        syncs: u64,
        restored_attachments: Vec<(VsetId, AttachmentId)>,
    }

    #[derive(Default)]
    struct FakeIo {
        state: Mutex<FakeState>,
    }

    impl DatabaseIo for FakeIo {
        #[allow(clippy::too_many_lines)]
        fn request(&self, request: DatabaseRequest) -> DatabaseReply {
            let req = request.req;
            let mut state = self.state.lock().unwrap();
            match request.op {
                DatabaseOp::Open {
                    handle,
                    file,
                    create,
                } => {
                    if !state.files.contains_key(&file) && !create {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::NotFound,
                        };
                    }
                    state.files.entry(file).or_default();
                    if state.handles.insert(handle, file).is_some() {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::AlreadyOpen,
                        };
                    }
                    DatabaseReply::Opened { req }
                }
                DatabaseOp::Close { handle } => {
                    if state.handles.remove(&handle).is_some() {
                        DatabaseReply::Closed { req }
                    } else {
                        DatabaseReply::Failed {
                            req,
                            error: DatabaseError::InvalidHandle,
                        }
                    }
                }
                DatabaseOp::Read {
                    handle,
                    offset,
                    len,
                } => {
                    let Some(file) = state.handles.get(&handle).copied() else {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::InvalidHandle,
                        };
                    };
                    let bytes = &state.files[&file];
                    let start = usize::try_from(offset)
                        .unwrap_or(usize::MAX)
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
                    let Some(file) = state.handles.get(&handle).copied() else {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::InvalidHandle,
                        };
                    };
                    let start = usize::try_from(offset).unwrap();
                    let end = start.checked_add(bytes.len()).unwrap();
                    let file_bytes = state.files.get_mut(&file).unwrap();
                    file_bytes.resize(file_bytes.len().max(end), 0);
                    file_bytes[start..end].copy_from_slice(&bytes);
                    state.sequence += 1;
                    DatabaseReply::Written {
                        req,
                        sequence: state.sequence,
                    }
                }
                DatabaseOp::Truncate { handle, size } => {
                    let Some(file) = state.handles.get(&handle).copied() else {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::InvalidHandle,
                        };
                    };
                    state
                        .files
                        .get_mut(&file)
                        .unwrap()
                        .resize(usize::try_from(size).unwrap(), 0);
                    state.sequence += 1;
                    DatabaseReply::Truncated {
                        req,
                        sequence: state.sequence,
                    }
                }
                DatabaseOp::FileSize { handle } => {
                    let Some(file) = state.handles.get(&handle).copied() else {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::InvalidHandle,
                        };
                    };
                    DatabaseReply::FileSize {
                        req,
                        size: state.files[&file].len() as u64,
                    }
                }
                DatabaseOp::Access { file } => DatabaseReply::Access {
                    req,
                    exists: state.files.contains_key(&file),
                },
                DatabaseOp::Delete { file } => {
                    state.files.remove(&file);
                    state.sequence += 1;
                    DatabaseReply::Deleted {
                        req,
                        sequence: state.sequence,
                    }
                }
                DatabaseOp::Sync { handle } => {
                    if !state.handles.contains_key(&handle) {
                        return DatabaseReply::Failed {
                            req,
                            error: DatabaseError::InvalidHandle,
                        };
                    }
                    state.syncs += 1;
                    DatabaseReply::Synced {
                        req,
                        sequence: state.sequence,
                    }
                }
            }
        }

        fn dax_file(&self, _vset: VsetId, file: DatabaseFile) -> io::Result<(File, u64)> {
            Ok((
                self.state
                    .lock()
                    .unwrap()
                    .dax
                    .get(&file)
                    .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?
                    .try_clone()?,
                0,
            ))
        }

        fn restore_attachment(
            &self,
            vset: VsetId,
            saved: AttachmentId,
        ) -> io::Result<RestoredAttachment> {
            self.state
                .lock()
                .unwrap()
                .restored_attachments
                .push((vset, saved));
            Ok(RestoredAttachment {
                attachment: AttachmentId {
                    vm: saved.vm,
                    generation: saved.generation + 100,
                },
                created: true,
            })
        }
    }

    struct TestReader(Cursor<Vec<u8>>);

    impl Read for TestReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.read(buf)
        }
    }

    impl ZeroCopyReader for TestReader {
        fn read_to(
            &mut self,
            _file: &mut dyn FileReadWriteVolatile,
            _count: usize,
            _offset: u64,
        ) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }

    #[derive(Default)]
    struct TestWriter(Vec<u8>);

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl ZeroCopyWriter for TestWriter {
        fn write_from(
            &mut self,
            _file: &mut dyn FileReadWriteVolatile,
            _count: usize,
            _offset: u64,
        ) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }

        fn available_bytes(&self) -> usize {
            usize::MAX
        }
    }

    #[derive(Default)]
    struct MapRecorder {
        maps: usize,
    }

    impl FsCacheReqHandler for MapRecorder {
        fn map(
            &mut self,
            _foffset: u64,
            _moffset: u64,
            _len: u64,
            _flags: u64,
            _fd: RawFd,
        ) -> io::Result<()> {
            self.maps += 1;
            Ok(())
        }

        fn unmap(&mut self, _requests: Vec<RemovemappingOne>) -> io::Result<()> {
            Ok(())
        }
    }

    fn filesystem() -> (VsetFilesystem<FakeIo>, u64) {
        let mut state = VsetFsState::new(VmId(7));
        let directory = state
            .attach(
                "db",
                VsetId(11),
                AttachmentId {
                    vm: VmId(7),
                    generation: 3,
                },
            )
            .unwrap()
            .directory_inode;
        (VsetFilesystem::new(state, FakeIo::default()), directory)
    }

    fn mapped_durable_snapshot() -> (Vec<u8>, u64, Vec<u8>) {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        let (entry, handle, _, _) = fs
            .create(
                &ctx,
                directory,
                c"database.sqlite",
                CreateIn {
                    flags: (libc::O_CREAT | libc::O_RDWR) as u32,
                    mode: 0o600,
                    ..CreateIn::default()
                },
            )
            .unwrap();
        let handle = handle.unwrap();
        let bytes = vec![0x39; 4096];
        let mut input = TestReader(Cursor::new(bytes.clone()));
        fs.write(
            &ctx,
            entry.inode,
            handle,
            &mut input,
            4096,
            0,
            None,
            false,
            0,
            0,
        )
        .unwrap();
        let backing = vmm_sys_util::tempfile::TempFile::new().unwrap().into_file();
        backing.set_len(4096).unwrap();
        backing.write_all_at(&bytes, 0).unwrap();
        fs.io
            .state
            .lock()
            .unwrap()
            .dax
            .insert(DatabaseFile::Main, backing);
        fs.lock_state()
            .unwrap()
            .map(DaxMapping {
                inode: entry.inode,
                file_offset: 0,
                window_offset: 0,
                len: 4096,
                writable: true,
            })
            .unwrap();
        (fs.save_state().unwrap(), entry.inode, bytes)
    }

    fn restore_destination(bytes: &[u8]) -> VsetFilesystem<FakeIo> {
        let io = FakeIo::default();
        let backing = vmm_sys_util::tempfile::TempFile::new().unwrap().into_file();
        backing.set_len(bytes.len() as u64).unwrap();
        backing.write_all_at(bytes, 0).unwrap();
        {
            let mut state = io.state.lock().unwrap();
            state.files.insert(DatabaseFile::Main, bytes.to_vec());
            state.dax.insert(DatabaseFile::Main, backing);
        }
        VsetFilesystem::new(VsetFsState::new(VmId(7)), io)
    }

    #[test]
    fn mounted_root_exposes_database_exports_directly() {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        assert_eq!(fs.getattr(&ctx, ROOT_INODE, None).unwrap().0.st_ino, 1);
        assert_eq!(fs.lookup(&ctx, ROOT_INODE, c"db").unwrap().inode, directory);
        assert_eq!(
            fs.lookup(&ctx, ROOT_INODE, c"vsets")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOENT)
        );
    }

    #[test]
    fn durable_file_create_write_sync_read_and_release() {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        assert_eq!(
            fs.lookup(&ctx, VSETS_INODE, c"db").unwrap().inode,
            directory
        );
        let (entry, handle, _, _) = fs
            .create(
                &ctx,
                directory,
                c"database.sqlite",
                CreateIn {
                    flags: (libc::O_CREAT | libc::O_RDWR) as u32,
                    mode: 0o600,
                    ..CreateIn::default()
                },
            )
            .unwrap();
        let handle = handle.unwrap();
        let mut input = TestReader(Cursor::new(b"sqlite".to_vec()));
        assert_eq!(
            fs.write(
                &ctx,
                entry.inode,
                handle,
                &mut input,
                6,
                3,
                None,
                false,
                0,
                0
            )
            .unwrap(),
            6
        );
        fs.fsync(&ctx, entry.inode, false, handle).unwrap();
        let mut output = TestWriter::default();
        assert_eq!(
            fs.read(&ctx, entry.inode, handle, &mut output, 9, 0, None, 0)
                .unwrap(),
            9
        );
        assert_eq!(output.0, b"\0\0\0sqlite");
        fs.release(&ctx, entry.inode, 0, handle, false, false, None)
            .unwrap();
        let fake = fs.io.state.lock().unwrap();
        assert_eq!(fake.syncs, 1);
        assert!(fake.handles.is_empty());
    }

    #[test]
    fn shm_and_lock_state_survive_backend_snapshot() {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        let (entry, handle, _, _) = fs
            .create(
                &ctx,
                directory,
                c"database.sqlite-shm",
                CreateIn {
                    flags: (libc::O_CREAT | libc::O_RDWR) as u32,
                    mode: 0o600,
                    ..CreateIn::default()
                },
            )
            .unwrap();
        let handle = handle.unwrap();
        let mut input = TestReader(Cursor::new(vec![0x5a; 32]));
        fs.write(
            &ctx,
            entry.inode,
            handle,
            &mut input,
            32,
            4096,
            None,
            false,
            0,
            0,
        )
        .unwrap();
        fs.setlk(
            &ctx,
            entry.inode,
            handle,
            41,
            FileLock {
                start: 120,
                end: 127,
                lock_type: libc::F_WRLCK as u32,
                pid: 9,
            },
            0,
        )
        .unwrap();
        let conflict = fs
            .getlk(
                &ctx,
                entry.inode,
                handle,
                42,
                FileLock {
                    start: 0,
                    end: 200,
                    lock_type: libc::F_RDLCK as u32,
                    pid: 10,
                },
                0,
            )
            .unwrap();
        assert_eq!(conflict.start, 120);
        assert_eq!(conflict.end, 127);
        assert_eq!(conflict.lock_type, libc::F_WRLCK as u32);

        let snapshot = fs.save_state().unwrap();
        let restored = VsetFsState::decode_snapshot(&snapshot).unwrap();
        assert_eq!(restored.shm("db").unwrap().len(), 4096 + 32);
        assert_eq!(&restored.shm("db").unwrap()[4096..], &[0x5a; 32]);
        assert_eq!(restored.locks().len(), 1);
        assert_eq!(restored.handle_count(), 1);
    }

    #[test]
    fn snapshot_flushes_writable_dax_mapping_after_handle_close() {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        let (entry, handle, _, _) = fs
            .create(
                &ctx,
                directory,
                c"database.sqlite",
                CreateIn {
                    flags: (libc::O_CREAT | libc::O_RDWR) as u32,
                    mode: 0o600,
                    ..CreateIn::default()
                },
            )
            .unwrap();
        let handle = handle.unwrap();
        let original = vec![0x11; 4096];
        let mut input = TestReader(Cursor::new(original));
        fs.write(
            &ctx,
            entry.inode,
            handle,
            &mut input,
            4096,
            0,
            None,
            false,
            0,
            0,
        )
        .unwrap();

        let backing = vmm_sys_util::tempfile::TempFile::new().unwrap().into_file();
        backing.set_len(4096).unwrap();
        fs.io
            .state
            .lock()
            .unwrap()
            .dax
            .insert(DatabaseFile::Main, backing.try_clone().unwrap());
        fs.lock_state()
            .unwrap()
            .map(DaxMapping {
                inode: entry.inode,
                file_offset: 0,
                window_offset: 0,
                len: 4096,
                writable: true,
            })
            .unwrap();

        fs.release(&ctx, entry.inode, 0, handle, false, false, None)
            .unwrap();
        let dirty = vec![0x7b; 4096];
        backing.write_all_at(&dirty, 0).unwrap();

        fs.save_state().unwrap();
        let fake = fs.io.state.lock().unwrap();
        assert_eq!(fake.files[&DatabaseFile::Main], dirty);
        assert_eq!(fake.syncs, 1);
        assert!(fake.handles.is_empty());
    }

    #[test]
    fn dax_fault_can_map_after_the_last_file_handle_closes() {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        let (entry, handle, _, _) = fs
            .create(
                &ctx,
                directory,
                c"database.sqlite",
                CreateIn {
                    flags: (libc::O_CREAT | libc::O_RDWR) as u32,
                    mode: 0o600,
                    ..CreateIn::default()
                },
            )
            .unwrap();
        let handle = handle.unwrap();
        let backing = vmm_sys_util::tempfile::TempFile::new().unwrap().into_file();
        backing.set_len(4096).unwrap();
        fs.io
            .state
            .lock()
            .unwrap()
            .dax
            .insert(DatabaseFile::Main, backing);
        fs.release(&ctx, entry.inode, 0, handle, false, false, None)
            .unwrap();

        let mut recorder = MapRecorder::default();
        fs.setupmapping(
            &ctx,
            entry.inode,
            u64::MAX,
            0,
            4096,
            SetupmappingFlags::WRITE.bits(),
            0,
            &mut recorder,
        )
        .unwrap();

        assert_eq!(recorder.maps, 1);
        assert_eq!(fs.lock_state().unwrap().mappings().len(), 1);
        assert!(fs.io.state.lock().unwrap().handles.is_empty());
    }

    #[test]
    fn dax_reclaim_flushes_and_unmaps_after_the_last_file_handle_closes() {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        let (entry, handle, _, _) = fs
            .create(
                &ctx,
                directory,
                c"database.sqlite",
                CreateIn {
                    flags: (libc::O_CREAT | libc::O_RDWR) as u32,
                    mode: 0o600,
                    ..CreateIn::default()
                },
            )
            .unwrap();
        let handle = handle.unwrap();
        let original = vec![0x11; 4096];
        let mut input = TestReader(Cursor::new(original));
        fs.write(
            &ctx,
            entry.inode,
            handle,
            &mut input,
            4096,
            0,
            None,
            false,
            0,
            0,
        )
        .unwrap();
        let backing = vmm_sys_util::tempfile::TempFile::new().unwrap().into_file();
        backing.set_len(4096).unwrap();
        fs.io
            .state
            .lock()
            .unwrap()
            .dax
            .insert(DatabaseFile::Main, backing.try_clone().unwrap());
        fs.lock_state()
            .unwrap()
            .map(DaxMapping {
                inode: entry.inode,
                file_offset: 0,
                window_offset: 0,
                len: 4096,
                writable: true,
            })
            .unwrap();
        fs.release(&ctx, entry.inode, 0, handle, false, false, None)
            .unwrap();
        let dirty = vec![0x7b; 4096];
        backing.write_all_at(&dirty, 0).unwrap();

        let mut recorder = MapRecorder::default();
        fs.removemapping(
            &ctx,
            entry.inode,
            vec![RemovemappingOne {
                moffset: 0,
                len: 4096,
            }],
            &mut recorder,
        )
        .unwrap();

        assert_eq!(
            fs.io.state.lock().unwrap().files[&DatabaseFile::Main],
            dirty
        );
        assert!(fs.lock_state().unwrap().mappings().is_empty());
        assert!(fs.io.state.lock().unwrap().handles.is_empty());
    }

    #[test]
    fn snapshot_rejects_unlinked_open_dax_file() {
        let (fs, directory) = filesystem();
        let ctx = Context::new();
        let (entry, _handle, _, _) = fs
            .create(
                &ctx,
                directory,
                c"database.sqlite-wal",
                CreateIn {
                    flags: (libc::O_CREAT | libc::O_RDWR) as u32,
                    mode: 0o600,
                    ..CreateIn::default()
                },
            )
            .unwrap();
        let backing = vmm_sys_util::tempfile::TempFile::new().unwrap().into_file();
        backing.set_len(4096).unwrap();
        fs.io
            .state
            .lock()
            .unwrap()
            .dax
            .insert(DatabaseFile::Wal, backing);
        fs.lock_state()
            .unwrap()
            .map(DaxMapping {
                inode: entry.inode,
                file_offset: 0,
                window_offset: 0,
                len: 4096,
                writable: true,
            })
            .unwrap();
        fs.unlink(&ctx, directory, c"database.sqlite-wal").unwrap();

        assert_eq!(
            fs.save_state().unwrap_err().raw_os_error(),
            Some(libc::EBUSY)
        );
    }

    #[test]
    fn restore_rebinds_snapshot_attachment_to_current_daemon() {
        let (snapshot, inode, bytes) = mapped_durable_snapshot();
        let destination = restore_destination(&bytes);

        destination.replace_state(&snapshot).unwrap();

        assert_eq!(
            destination
                .lock_state()
                .unwrap()
                .node(inode)
                .unwrap()
                .attachment,
            Some(AttachmentId {
                vm: VmId(7),
                generation: 103,
            })
        );
        assert_eq!(
            destination.io.state.lock().unwrap().restored_attachments,
            [(
                VsetId(11),
                AttachmentId {
                    vm: VmId(7),
                    generation: 3,
                },
            )]
        );
    }

    #[test]
    fn restore_reopens_serialized_handles_before_mapping_replay() {
        let (snapshot, _inode, bytes) = mapped_durable_snapshot();
        let destination = restore_destination(&bytes);
        destination.replace_state(&snapshot).unwrap();
        let mut recorder = MapRecorder::default();

        destination.restore_dax_mappings(&mut recorder).unwrap();

        assert_eq!(recorder.maps, 1);
        assert_eq!(destination.io.state.lock().unwrap().handles.len(), 1);
    }
}
