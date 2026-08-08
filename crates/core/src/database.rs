//! Typed SQLite-file operations at the deterministic daemon seam.
//!
//! `SQLite` locking and shared-memory operations deliberately do not appear
//! here: they are volatile filesystem execution state owned by `vsetfs` and
//! serialized with warm VM snapshots. This module covers only the durable
//! main, WAL, and rollback-journal namespaces.

use crate::protocol::ReqId;
use crate::types::{PageId, PageNo, VmId, VolumeId, VolumeIdx, VsetId};

/// Largest byte payload accepted in one request or returned in one reply.
pub const MAX_DATABASE_IO: usize = 1024 * 1024;

/// A durable `SQLite` file in a database vset.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum DatabaseFile {
    Main,
    Wal,
    Journal,
}

impl DatabaseFile {
    pub const fn volume_index(self) -> VolumeIdx {
        VolumeIdx(match self {
            DatabaseFile::Main => 0,
            DatabaseFile::Wal => 1,
            DatabaseFile::Journal => 2,
        })
    }

    pub const fn page(self, vset: VsetId, page: u32) -> PageId {
        PageId {
            volume: VolumeId {
                vset,
                idx: self.volume_index(),
            },
            page: PageNo(page),
        }
    }
}

/// Volatile authority for one VM to use one database vset. Generations are
/// monotone within a daemon incarnation and never survive recovery.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct AttachmentId {
    pub vm: VmId,
    pub generation: u64,
}

/// One guest request. `handle` values are chosen by the client but scoped to
/// an attachment generation and validated before use.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatabaseRequest {
    pub req: ReqId,
    pub vset: VsetId,
    pub attachment: AttachmentId,
    pub op: DatabaseOp,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DatabaseOp {
    Open {
        handle: u64,
        file: DatabaseFile,
        create: bool,
    },
    Close {
        handle: u64,
    },
    Read {
        handle: u64,
        offset: u64,
        len: u32,
    },
    Write {
        handle: u64,
        offset: u64,
        bytes: Vec<u8>,
    },
    Truncate {
        handle: u64,
        size: u64,
    },
    FileSize {
        handle: u64,
    },
    Access {
        file: DatabaseFile,
    },
    /// Read existence and size without creating a temporary open handle.
    Stat {
        file: DatabaseFile,
    },
    Delete {
        file: DatabaseFile,
    },
    /// Durably commit the prefix of mutations accepted before this request.
    Sync {
        handle: u64,
    },
}

/// Stable error classes; the transport/VFS maps these to `SQLite` result codes
/// without exposing host paths or internal storage details.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DatabaseError {
    NotAttached,
    StaleAttachment,
    Draining,
    InvalidHandle,
    AlreadyOpen,
    NotFound,
    InvalidRequest,
    TooLarge,
    Busy,
    Io,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DatabaseReply {
    Opened {
        req: ReqId,
    },
    Closed {
        req: ReqId,
    },
    Read {
        req: ReqId,
        bytes: Vec<u8>,
        eof: bool,
    },
    Written {
        req: ReqId,
        sequence: u64,
    },
    Truncated {
        req: ReqId,
        sequence: u64,
    },
    FileSize {
        req: ReqId,
        size: u64,
    },
    Access {
        req: ReqId,
        exists: bool,
    },
    Stat {
        req: ReqId,
        exists: bool,
        size: u64,
    },
    Deleted {
        req: ReqId,
        sequence: u64,
    },
    Synced {
        req: ReqId,
        sequence: u64,
    },
    Failed {
        req: ReqId,
        error: DatabaseError,
    },
}

impl DatabaseReply {
    pub const fn req(&self) -> ReqId {
        match *self {
            DatabaseReply::Opened { req }
            | DatabaseReply::Closed { req }
            | DatabaseReply::Read { req, .. }
            | DatabaseReply::Written { req, .. }
            | DatabaseReply::Truncated { req, .. }
            | DatabaseReply::FileSize { req, .. }
            | DatabaseReply::Access { req, .. }
            | DatabaseReply::Stat { req, .. }
            | DatabaseReply::Deleted { req, .. }
            | DatabaseReply::Synced { req, .. }
            | DatabaseReply::Failed { req, .. } => req,
        }
    }

    /// Replace transport-local correlation with the caller's request id.
    /// Runtime adapters use a host-unique id inside the daemon so equal guest
    /// ids from different VMs or connections can never share a waiter.
    #[must_use]
    pub fn with_req(mut self, replacement: ReqId) -> Self {
        match &mut self {
            DatabaseReply::Opened { req }
            | DatabaseReply::Closed { req }
            | DatabaseReply::Read { req, .. }
            | DatabaseReply::Written { req, .. }
            | DatabaseReply::Truncated { req, .. }
            | DatabaseReply::FileSize { req, .. }
            | DatabaseReply::Access { req, .. }
            | DatabaseReply::Stat { req, .. }
            | DatabaseReply::Deleted { req, .. }
            | DatabaseReply::Synced { req, .. }
            | DatabaseReply::Failed { req, .. } => *req = replacement,
        }
        self
    }
}
