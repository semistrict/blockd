//! VM-bound filesystem adapter for independently attachable database vsets.

#[cfg(target_os = "linux")]
mod filesystem;
#[cfg(target_os = "linux")]
mod transport;

#[cfg(target_os = "linux")]
pub use filesystem::{DatabaseIo, RestoredAttachment, VsetFilesystem};
#[cfg(target_os = "linux")]
pub use transport::{VHOST_USER_FS_QUEUES, VsetFsBackend, serve_vhost_user};
