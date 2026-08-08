//! Runtime ownership for one VM-specific virtio-fs backend connection.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use blockd_core::database::{AttachmentId, DatabaseReply, DatabaseRequest};
use blockd_core::seam::DetachMode;
use blockd_core::types::{VmId, VsetId};
use blockd_core::vsetfs::{AttachedExport, DaxMapping, VsetFsState};
use blockd_vsetfs::{
    DatabaseIo, RestoredAttachment, VsetFilesystem, VsetFsBackend, database_error, serve_vhost_user,
};

use crate::Runtime;

const SOCKET_READY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct RuntimeDatabaseIo {
    runtime: Arc<Runtime>,
}

impl DatabaseIo for RuntimeDatabaseIo {
    fn request(&self, request: DatabaseRequest) -> DatabaseReply {
        self.runtime.database_request(request)
    }

    fn dax_file(
        &self,
        vset: VsetId,
        file: blockd_core::database::DatabaseFile,
    ) -> io::Result<(std::fs::File, u64)> {
        self.runtime.database_dax_file(vset, file)
    }

    fn restore_attachment(
        &self,
        vset: VsetId,
        saved: AttachmentId,
    ) -> io::Result<RestoredAttachment> {
        let probe = self.runtime.database_request(DatabaseRequest {
            req: blockd_core::seam::ReqId(0),
            vset,
            attachment: saved,
            op: blockd_core::database::DatabaseOp::Access {
                file: blockd_core::database::DatabaseFile::Main,
            },
        });
        match probe {
            DatabaseReply::Access { .. } => Ok(RestoredAttachment {
                attachment: saved,
                created: false,
            }),
            DatabaseReply::Failed {
                error:
                    blockd_core::database::DatabaseError::NotAttached
                    | blockd_core::database::DatabaseError::StaleAttachment,
                ..
            } => self
                .runtime
                .try_attach_database(vset, saved.vm)
                .map(|attachment| RestoredAttachment {
                    attachment,
                    created: true,
                })
                .ok_or_else(|| io::Error::from_raw_os_error(libc::EBUSY)),
            DatabaseReply::Failed { error, .. } => Err(database_error(error)),
            _ => Err(io::Error::from_raw_os_error(libc::EPROTO)),
        }
    }

    fn abort_restored_attachment(&self, vset: VsetId, attachment: AttachmentId) {
        self.runtime
            .begin_detach_database(vset, attachment, DetachMode::Forced);
        let _ = self.runtime.finish_detach_database(vset, attachment);
    }
}

/// One VM-authenticated filesystem transport. Logical database attachments
/// are hotplugged into its namespace rather than creating more `VirtIO` devices.
pub struct VsetFsEndpoint {
    runtime: Arc<Runtime>,
    vm: VmId,
    socket: PathBuf,
    backend: VsetFsBackend<RuntimeDatabaseIo>,
    thread: Option<JoinHandle<io::Result<()>>>,
}

impl VsetFsEndpoint {
    pub fn bind(runtime: Arc<Runtime>, vm: VmId, socket: &Path, tag: &str) -> io::Result<Self> {
        match std::fs::remove_file(socket) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let filesystem = VsetFilesystem::new(
            VsetFsState::new(vm),
            RuntimeDatabaseIo {
                runtime: Arc::clone(&runtime),
            },
        );
        let backend = VsetFsBackend::new(filesystem, tag)?;
        let serve_backend = backend.clone();
        let serve_socket = socket.to_owned();
        let thread = thread::spawn(move || serve_vhost_user(serve_backend, serve_socket));

        let deadline = Instant::now() + SOCKET_READY_TIMEOUT;
        while !socket.exists() {
            if thread.is_finished() {
                return thread
                    .join()
                    .map_err(|_| io::Error::other("filesystem backend thread panicked"))?
                    .and_then(|()| {
                        Err(io::Error::other(
                            "filesystem backend exited before creating its socket",
                        ))
                    });
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "filesystem backend socket did not become ready",
                ));
            }
            thread::park_timeout(Duration::from_millis(2));
        }

        Ok(Self {
            runtime,
            vm,
            socket: socket.to_owned(),
            backend,
            thread: Some(thread),
        })
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub fn dax_map_count(&self) -> u64 {
        self.backend.dax_map_count()
    }

    pub fn dax_unmap_count(&self) -> u64 {
        self.backend.dax_unmap_count()
    }

    pub fn revoke_dax_mappings(&self, mappings: &[DaxMapping]) -> io::Result<()> {
        self.backend.revoke_dax_mappings(mappings)
    }

    pub fn attach(&self, name: &str, vset: VsetId) -> io::Result<AttachedExport> {
        let attachment = self
            .runtime
            .try_attach_database(vset, self.vm)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::WouldBlock, "database is already attached")
            })?;
        match self
            .backend
            .filesystem()
            .attach_export(name, vset, attachment)
        {
            Ok(export) => Ok(export),
            Err(error) => {
                self.runtime
                    .begin_detach_database(vset, attachment, DetachMode::Forced);
                let _ = self.runtime.finish_detach_database(vset, attachment);
                Err(error)
            }
        }
    }

    pub fn begin_detach(
        &self,
        name: &str,
        vset: VsetId,
        attachment: AttachmentId,
        mode: DetachMode,
    ) -> io::Result<Vec<DaxMapping>> {
        self.runtime.begin_detach_database(vset, attachment, mode);
        if mode == DetachMode::Forced {
            self.backend.filesystem().force_detach_export(name)
        } else {
            self.backend.filesystem().begin_detach_export(name)?;
            Ok(Vec::new())
        }
    }

    pub fn finish_detach(
        &self,
        name: &str,
        vset: VsetId,
        attachment: AttachmentId,
    ) -> io::Result<()> {
        let mappings = self.backend.filesystem().draining_export_mappings(name)?;
        self.backend.remove_dax_mappings(&mappings)?;
        self.backend.filesystem().forget_export_mappings(name)?;
        let detachable = self.backend.filesystem().detachable_export(name)?;
        if detachable != attachment {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "filesystem selected a different attachment generation",
            ));
        }
        let deadline = Instant::now() + SOCKET_READY_TIMEOUT;
        while !self.runtime.finish_detach_database(vset, attachment) {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "database detach did not become durable",
                ));
            }
            thread::park_timeout(Duration::from_millis(2));
        }
        let retired = self.backend.filesystem().finish_detach_export(name)?;
        if retired != attachment {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "filesystem retired a different attachment generation",
            ));
        }
        Ok(())
    }

    pub fn exports(&self) -> io::Result<Vec<(String, VsetId, AttachmentId)>> {
        self.backend.filesystem().exports()
    }

    /// Complete a forced detach only after the caller has synchronously
    /// revoked every mapping returned by [`Self::begin_detach`].
    pub fn finish_forced_detach(&self, vset: VsetId, attachment: AttachmentId) -> bool {
        self.runtime.finish_detach_database(vset, attachment)
    }

    /// Wait after the frontend has disconnected. This is explicit so callers
    /// cannot accidentally block in `Drop` while a VM is still running.
    pub fn wait(mut self) -> io::Result<()> {
        // `serve()` waits in accept before it creates its epoll worker. A runner
        // can therefore shut down before Firecracker ever connects; wake that
        // accept with a short-lived connection as well as signaling the worker.
        let _ = std::os::unix::net::UnixStream::connect(&self.socket);
        self.backend.shutdown()?;
        let result = self
            .thread
            .take()
            .expect("filesystem thread exists")
            .join()
            .map_err(|_| io::Error::other("filesystem backend thread panicked"))?;
        match std::fs::remove_file(&self.socket) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        result
    }
}
