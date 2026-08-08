//! Deterministic virtio-fs namespace and warm-snapshot state.
//!
//! This module deliberately knows nothing about FUSE wire structures, sockets,
//! file descriptors, KVM, or clocks. A transport adapter resolves guest FUSE
//! requests through this state, then submits durable file operations through
//! [`crate::database`]. Keeping identity, locks, mappings, and snapshot state
//! here makes the non-DAX and DAX paths share the same fencing rules.

use std::collections::{BTreeMap, BTreeSet};

use crate::database::{AttachmentId, DatabaseFile};
use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::types::{VmId, VsetId};

pub const ROOT_INODE: u64 = 1;
pub const VSETS_INODE: u64 = 2;
pub const PAGE_SIZE: u64 = 4096;

const MAGIC_VSETFS_STATE: u32 = u32::from_le_bytes(*b"BFS1");
const SNAPSHOT_VERSION: u16 = 1;
const MAX_EXPORTS: usize = 4096;
const MAX_NAME: usize = 128;
const MAX_HANDLES: usize = 65_536;
const MAX_LOCKS: usize = 65_536;
const MAX_MAPPINGS: usize = 65_536;
pub const MAX_SHM_PER_EXPORT: usize = 8 * 1024 * 1024;
const MAX_SHM_TOTAL: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum VsetFsFile {
    Main,
    Wal,
    Journal,
    Shm,
}

impl VsetFsFile {
    pub const ALL: [VsetFsFile; 4] = [
        VsetFsFile::Main,
        VsetFsFile::Wal,
        VsetFsFile::Journal,
        VsetFsFile::Shm,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            VsetFsFile::Main => "database.sqlite",
            VsetFsFile::Wal => "database.sqlite-wal",
            VsetFsFile::Journal => "database.sqlite-journal",
            VsetFsFile::Shm => "database.sqlite-shm",
        }
    }

    pub const fn durable_file(self) -> Option<DatabaseFile> {
        match self {
            VsetFsFile::Main => Some(DatabaseFile::Main),
            VsetFsFile::Wal => Some(DatabaseFile::Wal),
            VsetFsFile::Journal => Some(DatabaseFile::Journal),
            VsetFsFile::Shm => None,
        }
    }

    const fn discriminant(self) -> u8 {
        match self {
            VsetFsFile::Main => 0,
            VsetFsFile::Wal => 1,
            VsetFsFile::Journal => 2,
            VsetFsFile::Shm => 3,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeKind {
    Root,
    Vsets,
    Attachment,
    File(VsetFsFile),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Node {
    pub inode: u64,
    pub kind: NodeKind,
    pub vset: Option<VsetId>,
    pub attachment: Option<AttachmentId>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExportPhase {
    Attached,
    Draining,
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Export {
    name: String,
    vset: VsetId,
    attachment: AttachmentId,
    phase: ExportPhase,
    directory_inode: u64,
    file_inodes: [u64; 4],
    shm_exists: bool,
    shm: Vec<u8>,
}

impl Export {
    fn file_inode(&self, file: VsetFsFile) -> u64 {
        self.file_inodes[usize::from(file.discriminant())]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OpenHandle {
    pub handle: u64,
    pub inode: u64,
    pub flags: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ByteRangeLock {
    pub handle: u64,
    pub owner: u64,
    pub start: u64,
    /// Zero means through EOF, matching POSIX `l_len == 0`.
    pub len: u64,
    pub exclusive: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DaxMapping {
    pub inode: u64,
    pub file_offset: u64,
    pub window_offset: u64,
    pub len: u64,
    pub writable: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AttachedExport {
    pub directory_inode: u64,
    pub attachment: AttachmentId,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VsetFsError {
    InvalidName,
    WrongVm,
    AlreadyExists,
    NotFound,
    Draining,
    Busy,
    Conflict,
    InvalidRange,
    TooLarge,
    InvalidState,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VsetFsState {
    vm: VmId,
    export_generation: u64,
    next_inode: u64,
    next_handle: u64,
    exports: BTreeMap<String, Export>,
    nodes: BTreeMap<u64, Node>,
    inode_exports: BTreeMap<u64, String>,
    handles: BTreeMap<u64, OpenHandle>,
    locks: Vec<ByteRangeLock>,
    mappings: Vec<DaxMapping>,
}

impl VsetFsState {
    pub fn new(vm: VmId) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            ROOT_INODE,
            Node {
                inode: ROOT_INODE,
                kind: NodeKind::Root,
                vset: None,
                attachment: None,
            },
        );
        nodes.insert(
            VSETS_INODE,
            Node {
                inode: VSETS_INODE,
                kind: NodeKind::Vsets,
                vset: None,
                attachment: None,
            },
        );
        Self {
            vm,
            export_generation: 0,
            next_inode: 3,
            next_handle: 1,
            exports: BTreeMap::new(),
            nodes,
            inode_exports: BTreeMap::new(),
            handles: BTreeMap::new(),
            locks: Vec::new(),
            mappings: Vec::new(),
        }
    }

    pub const fn vm(&self) -> VmId {
        self.vm
    }

    pub const fn export_generation(&self) -> u64 {
        self.export_generation
    }

    pub fn export_count(&self) -> usize {
        self.exports.len()
    }

    pub fn handle_count(&self) -> usize {
        self.handles.len()
    }

    pub fn node(&self, inode: u64) -> Option<Node> {
        self.nodes.get(&inode).copied()
    }

    pub fn export_name(&self, inode: u64) -> Option<&str> {
        self.inode_exports.get(&inode).map(String::as_str)
    }

    pub fn directory_entries(&self, inode: u64) -> Result<Vec<(String, Node)>, VsetFsError> {
        match self.node(inode).map(|node| node.kind) {
            Some(NodeKind::Root) => Ok(vec![("vsets".to_owned(), self.nodes[&VSETS_INODE])]),
            Some(NodeKind::Vsets) => Ok(self
                .exports
                .iter()
                .filter_map(|(name, export)| {
                    self.node(export.directory_inode)
                        .map(|node| (name.clone(), node))
                })
                .collect()),
            Some(NodeKind::Attachment) => {
                let export = self.export_for_inode(inode).ok_or(VsetFsError::NotFound)?;
                Ok(VsetFsFile::ALL
                    .into_iter()
                    .filter_map(|file| {
                        self.node(export.file_inode(file))
                            .map(|node| (file.name().to_owned(), node))
                    })
                    .collect())
            }
            _ => Err(VsetFsError::InvalidState),
        }
    }

    pub fn lookup(&self, parent: u64, name: &str) -> Option<Node> {
        match parent {
            ROOT_INODE if name == "vsets" => self.node(VSETS_INODE),
            VSETS_INODE => self
                .exports
                .get(name)
                .and_then(|export| self.node(export.directory_inode)),
            _ => {
                let parent_node = self.node(parent)?;
                if parent_node.kind != NodeKind::Attachment {
                    return None;
                }
                let export = self.export_for_inode(parent)?;
                VsetFsFile::ALL
                    .into_iter()
                    .find(|file| file.name() == name)
                    .and_then(|file| self.node(export.file_inode(file)))
            }
        }
    }

    pub fn attach(
        &mut self,
        name: &str,
        vset: VsetId,
        attachment: AttachmentId,
    ) -> Result<AttachedExport, VsetFsError> {
        if !valid_name(name) {
            return Err(VsetFsError::InvalidName);
        }
        if attachment.vm != self.vm {
            return Err(VsetFsError::WrongVm);
        }
        if self.exports.contains_key(name)
            || self
                .exports
                .values()
                .any(|export| export.vset == vset || export.attachment == attachment)
        {
            return Err(VsetFsError::AlreadyExists);
        }
        if self.exports.len() >= MAX_EXPORTS {
            return Err(VsetFsError::TooLarge);
        }

        let directory_inode = self.allocate_inode()?;
        let mut file_inodes = [0; 4];
        for inode in &mut file_inodes {
            *inode = self.allocate_inode()?;
        }
        let export = Export {
            name: name.to_owned(),
            vset,
            attachment,
            phase: ExportPhase::Attached,
            directory_inode,
            file_inodes,
            shm_exists: false,
            shm: Vec::new(),
        };
        self.insert_export_nodes(&export)?;
        self.exports.insert(name.to_owned(), export);
        self.export_generation = self
            .export_generation
            .checked_add(1)
            .ok_or(VsetFsError::InvalidState)?;
        Ok(AttachedExport {
            directory_inode,
            attachment,
        })
    }

    pub fn begin_detach(&mut self, name: &str) -> Result<(), VsetFsError> {
        let export = self.exports.get_mut(name).ok_or(VsetFsError::NotFound)?;
        if export.phase == ExportPhase::Draining {
            return Ok(());
        }
        export.phase = ExportPhase::Draining;
        self.export_generation = self
            .export_generation
            .checked_add(1)
            .ok_or(VsetFsError::InvalidState)?;
        Ok(())
    }

    pub fn finish_detach(&mut self, name: &str) -> Result<AttachmentId, VsetFsError> {
        let attachment = self.detachable_export(name)?;
        let export = self.exports.remove(name).expect("checked above");
        for inode in export_inodes(&export) {
            self.nodes.remove(&inode);
            self.inode_exports.remove(&inode);
        }
        self.export_generation = self
            .export_generation
            .checked_add(1)
            .ok_or(VsetFsError::InvalidState)?;
        Ok(attachment)
    }

    pub fn detachable_export(&self, name: &str) -> Result<AttachmentId, VsetFsError> {
        let export = self.exports.get(name).ok_or(VsetFsError::NotFound)?;
        if export.phase != ExportPhase::Draining {
            return Err(VsetFsError::InvalidState);
        }
        let inodes = export_inodes(export);
        if self
            .handles
            .values()
            .any(|handle| inodes.contains(&handle.inode))
            || self
                .mappings
                .iter()
                .any(|mapping| inodes.contains(&mapping.inode))
        {
            return Err(VsetFsError::Busy);
        }
        Ok(export.attachment)
    }

    pub fn draining_export_mappings(&self, name: &str) -> Result<Vec<DaxMapping>, VsetFsError> {
        let export = self.exports.get(name).ok_or(VsetFsError::NotFound)?;
        if export.phase != ExportPhase::Draining {
            return Err(VsetFsError::InvalidState);
        }
        let inodes = export_inodes(export);
        if self
            .handles
            .values()
            .any(|handle| inodes.contains(&handle.inode))
        {
            return Err(VsetFsError::Busy);
        }
        Ok(self
            .mappings
            .iter()
            .copied()
            .filter(|mapping| inodes.contains(&mapping.inode))
            .collect())
    }

    pub fn forget_export_mappings(&mut self, name: &str) -> Result<(), VsetFsError> {
        let export = self.exports.get(name).ok_or(VsetFsError::NotFound)?;
        if export.phase != ExportPhase::Draining {
            return Err(VsetFsError::InvalidState);
        }
        let inodes = export_inodes(export);
        self.mappings
            .retain(|mapping| !inodes.contains(&mapping.inode));
        Ok(())
    }

    /// Retire namespace state immediately. The VMM must revoke the returned
    /// mappings before it may acknowledge forced detach to the control plane.
    pub fn force_detach(&mut self, name: &str) -> Result<Vec<DaxMapping>, VsetFsError> {
        let export = self.exports.remove(name).ok_or(VsetFsError::NotFound)?;
        let inodes = export_inodes(&export);
        let revoked: Vec<_> = self
            .mappings
            .iter()
            .copied()
            .filter(|mapping| inodes.contains(&mapping.inode))
            .collect();
        self.mappings
            .retain(|mapping| !inodes.contains(&mapping.inode));
        let retired_handles: BTreeSet<_> = self
            .handles
            .iter()
            .filter_map(|(&handle, state)| inodes.contains(&state.inode).then_some(handle))
            .collect();
        self.handles
            .retain(|handle, _| !retired_handles.contains(handle));
        self.locks
            .retain(|lock| !retired_handles.contains(&lock.handle));
        for inode in inodes {
            self.nodes.remove(&inode);
            self.inode_exports.remove(&inode);
        }
        self.export_generation = self
            .export_generation
            .checked_add(1)
            .ok_or(VsetFsError::InvalidState)?;
        Ok(revoked)
    }

    pub fn open(&mut self, inode: u64, flags: u32) -> Result<OpenHandle, VsetFsError> {
        let node = self.node(inode).ok_or(VsetFsError::NotFound)?;
        if !matches!(node.kind, NodeKind::File(_)) {
            return Err(VsetFsError::InvalidState);
        }
        let export = self.export_for_inode(inode).ok_or(VsetFsError::NotFound)?;
        if export.phase == ExportPhase::Draining {
            return Err(VsetFsError::Draining);
        }
        if self.handles.len() >= MAX_HANDLES {
            return Err(VsetFsError::TooLarge);
        }
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or(VsetFsError::InvalidState)?;
        let state = OpenHandle {
            handle,
            inode,
            flags,
        };
        self.handles.insert(handle, state);
        Ok(state)
    }

    pub fn close(&mut self, handle: u64) -> Result<(), VsetFsError> {
        if self.handles.remove(&handle).is_none() {
            return Err(VsetFsError::NotFound);
        }
        self.locks.retain(|lock| lock.handle != handle);
        Ok(())
    }

    pub fn handle(&self, handle: u64) -> Option<OpenHandle> {
        self.handles.get(&handle).copied()
    }

    pub fn open_handles(&self) -> Vec<OpenHandle> {
        self.handles.values().copied().collect()
    }

    pub fn restore_exports(&self) -> Vec<(String, VsetId, AttachmentId)> {
        self.exports
            .values()
            .map(|export| (export.name.clone(), export.vset, export.attachment))
            .collect()
    }

    pub fn rebind_restored_attachment(
        &mut self,
        name: &str,
        saved: AttachmentId,
        replacement: AttachmentId,
    ) -> Result<(), VsetFsError> {
        if replacement.vm != self.vm {
            return Err(VsetFsError::WrongVm);
        }
        let export = self.exports.get(name).ok_or(VsetFsError::NotFound)?;
        if export.attachment != saved {
            return Err(VsetFsError::InvalidState);
        }
        let inodes = export_inodes(export);
        self.exports
            .get_mut(name)
            .expect("checked above")
            .attachment = replacement;
        for inode in inodes {
            self.nodes
                .get_mut(&inode)
                .ok_or(VsetFsError::InvalidState)?
                .attachment = Some(replacement);
        }
        Ok(())
    }

    pub fn locks(&self) -> &[ByteRangeLock] {
        &self.locks
    }

    pub fn conflicting_lock(
        &self,
        lock: ByteRangeLock,
    ) -> Result<Option<ByteRangeLock>, VsetFsError> {
        let requested = self.handle(lock.handle).ok_or(VsetFsError::NotFound)?;
        range_end(lock.start, lock.len)?;
        Ok(self.locks.iter().copied().find(|existing| {
            let Some(existing_handle) = self.handle(existing.handle) else {
                return false;
            };
            existing_handle.inode == requested.inode
                && existing.owner != lock.owner
                && (existing.exclusive || lock.exclusive)
                && ranges_overlap(existing.start, existing.len, lock.start, lock.len)
        }))
    }

    pub fn lock(&mut self, lock: ByteRangeLock) -> Result<(), VsetFsError> {
        let inode = self.handle(lock.handle).ok_or(VsetFsError::NotFound)?.inode;
        let requested_end = range_end(lock.start, lock.len)?;
        if self.conflicting_lock(lock)?.is_some() {
            return Err(VsetFsError::Conflict);
        }

        let (mut replacement, _) =
            self.split_lock_range(inode, lock.owner, lock.start, lock.len, requested_end);
        replacement.push(lock);
        if replacement.len() > MAX_LOCKS {
            return Err(VsetFsError::TooLarge);
        }
        self.locks = replacement;
        self.locks.sort_by_key(|entry| {
            (
                entry.handle,
                entry.owner,
                entry.start,
                entry.len,
                entry.exclusive,
            )
        });
        Ok(())
    }

    pub fn unlock(
        &mut self,
        handle: u64,
        owner: u64,
        start: u64,
        len: u64,
    ) -> Result<(), VsetFsError> {
        let inode = self.handle(handle).ok_or(VsetFsError::NotFound)?.inode;
        let unlock_end = range_end(start, len)?;
        let (mut replacement, found) = self.split_lock_range(inode, owner, start, len, unlock_end);
        if !found {
            return Err(VsetFsError::NotFound);
        }
        if replacement.len() > MAX_LOCKS {
            return Err(VsetFsError::TooLarge);
        }
        replacement.sort_by_key(|entry| {
            (
                entry.handle,
                entry.owner,
                entry.start,
                entry.len,
                entry.exclusive,
            )
        });
        self.locks = replacement;
        Ok(())
    }

    fn split_lock_range(
        &self,
        inode: u64,
        owner: u64,
        start: u64,
        len: u64,
        end: u64,
    ) -> (Vec<ByteRangeLock>, bool) {
        let handles = &self.handles;
        let mut found = false;
        let mut replacement = Vec::with_capacity(self.locks.len().saturating_add(2));
        for existing in self.locks.iter().copied() {
            let same_owner_and_inode = handles
                .get(&existing.handle)
                .is_some_and(|state| state.inode == inode && existing.owner == owner);
            if !same_owner_and_inode || !ranges_overlap(existing.start, existing.len, start, len) {
                replacement.push(existing);
                continue;
            }
            found = true;
            let existing_end =
                range_end(existing.start, existing.len).expect("stored lock range was validated");
            if existing.start < start {
                replacement.push(ByteRangeLock {
                    len: start - existing.start,
                    ..existing
                });
            }
            if end < existing_end {
                replacement.push(ByteRangeLock {
                    start: end,
                    len: range_len(end, existing_end),
                    ..existing
                });
            }
        }
        (replacement, found)
    }

    pub fn unlock_owner(&mut self, inode: u64, owner: u64) -> Result<(), VsetFsError> {
        if !self.nodes.contains_key(&inode) {
            return Err(VsetFsError::NotFound);
        }
        let handles = &self.handles;
        self.locks.retain(|lock| {
            !(lock.owner == owner
                && handles
                    .get(&lock.handle)
                    .is_some_and(|handle| handle.inode == inode))
        });
        Ok(())
    }

    pub fn map(&mut self, mapping: DaxMapping) -> Result<(), VsetFsError> {
        let node = self.node(mapping.inode).ok_or(VsetFsError::NotFound)?;
        if !matches!(node.kind, NodeKind::File(_)) {
            return Err(VsetFsError::InvalidState);
        }
        validate_mapping(mapping)?;
        if self.mappings.len() >= MAX_MAPPINGS {
            return Err(VsetFsError::TooLarge);
        }
        self.unmap_inner(mapping.window_offset, mapping.len);
        self.mappings.push(mapping);
        self.mappings.sort_by_key(|entry| entry.window_offset);
        Ok(())
    }

    pub fn unmap(&mut self, window_offset: u64, len: u64) -> Result<(), VsetFsError> {
        validate_aligned_range(window_offset, len)?;
        if !self
            .mappings
            .iter()
            .any(|mapping| ranges_overlap(mapping.window_offset, mapping.len, window_offset, len))
        {
            return Err(VsetFsError::NotFound);
        }
        self.unmap_inner(window_offset, len);
        Ok(())
    }

    pub fn mappings(&self) -> &[DaxMapping] {
        &self.mappings
    }

    pub fn forget_inode_mappings(&mut self, inode: u64) {
        self.mappings.retain(|mapping| mapping.inode != inode);
    }

    pub fn handle_for_inode(&self, inode: u64) -> Option<u64> {
        self.handles
            .iter()
            .find_map(|(&handle, open)| (open.inode == inode).then_some(handle))
    }

    pub fn replace_shm(&mut self, name: &str, bytes: Vec<u8>) -> Result<(), VsetFsError> {
        if bytes.len() > MAX_SHM_PER_EXPORT {
            return Err(VsetFsError::TooLarge);
        }
        let old_len = self
            .exports
            .get(name)
            .ok_or(VsetFsError::NotFound)?
            .shm
            .len();
        self.check_shm_budget(old_len, bytes.len())?;
        let export = self.exports.get_mut(name).expect("checked above");
        export.shm = bytes;
        export.shm_exists = true;
        Ok(())
    }

    pub fn create_shm(&mut self, name: &str) -> Result<(), VsetFsError> {
        let export = self.exports.get_mut(name).ok_or(VsetFsError::NotFound)?;
        if export.phase == ExportPhase::Draining {
            return Err(VsetFsError::Draining);
        }
        export.shm_exists = true;
        Ok(())
    }

    pub fn delete_shm(&mut self, name: &str) -> Result<(), VsetFsError> {
        let export = self.exports.get_mut(name).ok_or(VsetFsError::NotFound)?;
        export.shm_exists = false;
        export.shm.clear();
        Ok(())
    }

    pub fn shm_exists(&self, name: &str) -> bool {
        self.exports
            .get(name)
            .is_some_and(|export| export.shm_exists)
    }

    pub fn write_shm(&mut self, name: &str, offset: u64, bytes: &[u8]) -> Result<(), VsetFsError> {
        let offset = usize::try_from(offset).map_err(|_| VsetFsError::TooLarge)?;
        let end = offset
            .checked_add(bytes.len())
            .filter(|end| *end <= MAX_SHM_PER_EXPORT)
            .ok_or(VsetFsError::TooLarge)?;
        let export = self.exports.get(name).ok_or(VsetFsError::NotFound)?;
        if !export.shm_exists {
            return Err(VsetFsError::NotFound);
        }
        self.check_shm_budget(export.shm.len(), export.shm.len().max(end))?;
        let export = self.exports.get_mut(name).expect("checked above");
        export.shm.resize(export.shm.len().max(end), 0);
        export.shm[offset..end].copy_from_slice(bytes);
        Ok(())
    }

    pub fn truncate_shm(&mut self, name: &str, size: u64) -> Result<(), VsetFsError> {
        let size = usize::try_from(size)
            .ok()
            .filter(|size| *size <= MAX_SHM_PER_EXPORT)
            .ok_or(VsetFsError::TooLarge)?;
        let export = self.exports.get(name).ok_or(VsetFsError::NotFound)?;
        if !export.shm_exists {
            return Err(VsetFsError::NotFound);
        }
        self.check_shm_budget(export.shm.len(), size)?;
        self.exports
            .get_mut(name)
            .expect("checked above")
            .shm
            .resize(size, 0);
        Ok(())
    }

    fn check_shm_budget(&self, old_len: usize, new_len: usize) -> Result<(), VsetFsError> {
        self.exports
            .values()
            .map(|entry| entry.shm.len())
            .sum::<usize>()
            .checked_sub(old_len)
            .and_then(|total| total.checked_add(new_len))
            .filter(|total| *total <= MAX_SHM_TOTAL)
            .ok_or(VsetFsError::TooLarge)?;
        Ok(())
    }

    pub fn shm(&self, name: &str) -> Option<&[u8]> {
        self.exports.get(name).map(|export| export.shm.as_slice())
    }

    pub fn encode_snapshot(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(SNAPSHOT_VERSION);
        e.u64(self.vm.0);
        e.u64(self.export_generation);
        e.u64(self.next_inode);
        e.u64(self.next_handle);
        e.u32(u32::try_from(self.exports.len()).expect("bounded exports"));
        for export in self.exports.values() {
            e.u16(u16::try_from(export.name.len()).expect("bounded name"));
            e.bytes(export.name.as_bytes());
            e.u64(export.vset.0);
            e.u64(export.attachment.vm.0);
            e.u64(export.attachment.generation);
            e.u8(match export.phase {
                ExportPhase::Attached => 0,
                ExportPhase::Draining => 1,
            });
            e.u64(export.directory_inode);
            for inode in export.file_inodes {
                e.u64(inode);
            }
            e.u8(u8::from(export.shm_exists));
            e.u32(u32::try_from(export.shm.len()).expect("bounded shm"));
            e.bytes(&export.shm);
        }
        e.u32(u32::try_from(self.handles.len()).expect("bounded handles"));
        for handle in self.handles.values() {
            e.u64(handle.handle);
            e.u64(handle.inode);
            e.u32(handle.flags);
        }
        e.u32(u32::try_from(self.locks.len()).expect("bounded locks"));
        for lock in &self.locks {
            e.u64(lock.handle);
            e.u64(lock.owner);
            e.u64(lock.start);
            e.u64(lock.len);
            e.u8(u8::from(lock.exclusive));
        }
        e.u32(u32::try_from(self.mappings.len()).expect("bounded mappings"));
        for mapping in &self.mappings {
            e.u64(mapping.inode);
            e.u64(mapping.file_offset);
            e.u64(mapping.window_offset);
            e.u64(mapping.len);
            e.u8(u8::from(mapping.writable));
        }
        seal_frame(MAGIC_VSETFS_STATE, &e.finish())
    }

    #[allow(clippy::too_many_lines)]
    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_VSETFS_STATE, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != SNAPSHOT_VERSION {
            return Err(DecodeError);
        }
        let vm = VmId(d.u64()?);
        let export_generation = d.u64()?;
        let next_inode = d.u64()?;
        let next_handle = d.u64()?;
        let export_count = bounded_count(d.u32()?, MAX_EXPORTS)?;
        let mut state = Self::new(vm);
        state.export_generation = export_generation;
        state.next_inode = next_inode;
        state.next_handle = next_handle;
        let mut total_shm = 0usize;
        for _ in 0..export_count {
            let name_len = usize::from(d.u16()?);
            if name_len > MAX_NAME {
                return Err(DecodeError);
            }
            let name = std::str::from_utf8(d.bytes(name_len)?)
                .map_err(|_| DecodeError)?
                .to_owned();
            if !valid_name(&name) {
                return Err(DecodeError);
            }
            let vset = VsetId(d.u64()?);
            let attachment = AttachmentId {
                vm: VmId(d.u64()?),
                generation: d.u64()?,
            };
            if attachment.vm != vm {
                return Err(DecodeError);
            }
            let phase = match d.u8()? {
                0 => ExportPhase::Attached,
                1 => ExportPhase::Draining,
                _ => return Err(DecodeError),
            };
            let directory_inode = d.u64()?;
            let mut file_inodes = [0; 4];
            for inode in &mut file_inodes {
                *inode = d.u64()?;
            }
            let shm_exists = match d.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError),
            };
            let shm_len = bounded_count(d.u32()?, MAX_SHM_PER_EXPORT)?;
            if !shm_exists && shm_len != 0 {
                return Err(DecodeError);
            }
            total_shm = total_shm.checked_add(shm_len).ok_or(DecodeError)?;
            if total_shm > MAX_SHM_TOTAL {
                return Err(DecodeError);
            }
            let export = Export {
                name: name.clone(),
                vset,
                attachment,
                phase,
                directory_inode,
                file_inodes,
                shm_exists,
                shm: d.bytes(shm_len)?.to_vec(),
            };
            if state.exports.contains_key(&name)
                || state
                    .exports
                    .values()
                    .any(|existing| existing.vset == vset || existing.attachment == attachment)
            {
                return Err(DecodeError);
            }
            state
                .insert_export_nodes(&export)
                .map_err(|_| DecodeError)?;
            state.exports.insert(name, export);
        }

        let handle_count = bounded_count(d.u32()?, MAX_HANDLES)?;
        for _ in 0..handle_count {
            let handle = OpenHandle {
                handle: d.u64()?,
                inode: d.u64()?,
                flags: d.u32()?,
            };
            if !matches!(
                state.node(handle.inode).map(|node| node.kind),
                Some(NodeKind::File(_))
            ) || state.handles.insert(handle.handle, handle).is_some()
            {
                return Err(DecodeError);
            }
        }
        let lock_count = bounded_count(d.u32()?, MAX_LOCKS)?;
        for _ in 0..lock_count {
            let lock = ByteRangeLock {
                handle: d.u64()?,
                owner: d.u64()?,
                start: d.u64()?,
                len: d.u64()?,
                exclusive: match d.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(DecodeError),
                },
            };
            state.lock(lock).map_err(|_| DecodeError)?;
        }
        let mapping_count = bounded_count(d.u32()?, MAX_MAPPINGS)?;
        for _ in 0..mapping_count {
            let mapping = DaxMapping {
                inode: d.u64()?,
                file_offset: d.u64()?,
                window_offset: d.u64()?,
                len: d.u64()?,
                writable: match d.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(DecodeError),
                },
            };
            if !matches!(
                state.node(mapping.inode).map(|node| node.kind),
                Some(NodeKind::File(_))
            ) || validate_mapping(mapping).is_err()
                || state.mappings.iter().any(|existing| {
                    ranges_overlap(
                        existing.window_offset,
                        existing.len,
                        mapping.window_offset,
                        mapping.len,
                    )
                })
            {
                return Err(DecodeError);
            }
            state.mappings.push(mapping);
        }
        d.finish()?;
        state.locks.sort_by_key(|entry| {
            (
                entry.handle,
                entry.owner,
                entry.start,
                entry.len,
                entry.exclusive,
            )
        });
        state.mappings.sort_by_key(|entry| entry.window_offset);
        state.validate_counters()?;
        Ok(state)
    }

    fn allocate_inode(&mut self) -> Result<u64, VsetFsError> {
        let inode = self.next_inode;
        self.next_inode = self
            .next_inode
            .checked_add(1)
            .ok_or(VsetFsError::InvalidState)?;
        Ok(inode)
    }

    fn insert_export_nodes(&mut self, export: &Export) -> Result<(), VsetFsError> {
        let attachment_node = Node {
            inode: export.directory_inode,
            kind: NodeKind::Attachment,
            vset: Some(export.vset),
            attachment: Some(export.attachment),
        };
        if self
            .nodes
            .insert(export.directory_inode, attachment_node)
            .is_some()
        {
            return Err(VsetFsError::InvalidState);
        }
        if self
            .inode_exports
            .insert(export.directory_inode, export.name.clone())
            .is_some()
        {
            return Err(VsetFsError::InvalidState);
        }
        for file in VsetFsFile::ALL {
            let inode = export.file_inode(file);
            if self
                .nodes
                .insert(
                    inode,
                    Node {
                        inode,
                        kind: NodeKind::File(file),
                        vset: Some(export.vset),
                        attachment: Some(export.attachment),
                    },
                )
                .is_some()
            {
                return Err(VsetFsError::InvalidState);
            }
            if self
                .inode_exports
                .insert(inode, export.name.clone())
                .is_some()
            {
                return Err(VsetFsError::InvalidState);
            }
        }
        Ok(())
    }

    fn export_for_inode(&self, inode: u64) -> Option<&Export> {
        self.inode_exports
            .get(&inode)
            .and_then(|name| self.exports.get(name))
    }

    fn unmap_inner(&mut self, window_offset: u64, len: u64) {
        let remove_end = window_offset + len;
        let mut replacements = Vec::new();
        for mapping in self.mappings.drain(..) {
            let mapping_end = mapping.window_offset + mapping.len;
            if mapping_end <= window_offset || remove_end <= mapping.window_offset {
                replacements.push(mapping);
                continue;
            }
            if mapping.window_offset < window_offset {
                replacements.push(DaxMapping {
                    len: window_offset - mapping.window_offset,
                    ..mapping
                });
            }
            if remove_end < mapping_end {
                let consumed = remove_end - mapping.window_offset;
                replacements.push(DaxMapping {
                    file_offset: mapping.file_offset + consumed,
                    window_offset: remove_end,
                    len: mapping_end - remove_end,
                    ..mapping
                });
            }
        }
        replacements.sort_by_key(|mapping| mapping.window_offset);
        self.mappings = replacements;
    }

    fn validate_counters(&self) -> Result<(), DecodeError> {
        let max_inode = self.nodes.keys().copied().max().unwrap_or(VSETS_INODE);
        let max_handle = self.handles.keys().copied().max().unwrap_or(0);
        if self.next_inode <= max_inode || self.next_handle <= max_handle {
            return Err(DecodeError);
        }
        Ok(())
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && name != "."
        && name != ".."
}

fn export_inodes(export: &Export) -> [u64; 5] {
    [
        export.directory_inode,
        export.file_inodes[0],
        export.file_inodes[1],
        export.file_inodes[2],
        export.file_inodes[3],
    ]
}

fn bounded_count(value: u32, max: usize) -> Result<usize, DecodeError> {
    let value = usize::try_from(value).map_err(|_| DecodeError)?;
    if value > max {
        return Err(DecodeError);
    }
    Ok(value)
}

fn range_end(start: u64, len: u64) -> Result<u64, VsetFsError> {
    if len == 0 {
        Ok(u64::MAX)
    } else {
        start.checked_add(len).ok_or(VsetFsError::InvalidRange)
    }
}

fn range_len(start: u64, end: u64) -> u64 {
    if end == u64::MAX { 0 } else { end - start }
}

fn ranges_overlap(left_start: u64, left_len: u64, right_start: u64, right_len: u64) -> bool {
    let left_end = range_end(left_start, left_len).unwrap_or(u64::MAX);
    let right_end = range_end(right_start, right_len).unwrap_or(u64::MAX);
    left_start < right_end && right_start < left_end
}

fn validate_aligned_range(offset: u64, len: u64) -> Result<(), VsetFsError> {
    if len == 0
        || !offset.is_multiple_of(PAGE_SIZE)
        || !len.is_multiple_of(PAGE_SIZE)
        || offset.checked_add(len).is_none()
    {
        return Err(VsetFsError::InvalidRange);
    }
    Ok(())
}

fn validate_mapping(mapping: DaxMapping) -> Result<(), VsetFsError> {
    validate_aligned_range(mapping.file_offset, mapping.len)?;
    validate_aligned_range(mapping.window_offset, mapping.len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;

    fn attachment(vm: u64, generation: u64) -> AttachmentId {
        AttachmentId {
            vm: VmId(vm),
            generation,
        }
    }

    fn populated() -> VsetFsState {
        let mut state = VsetFsState::new(VmId(7));
        state.attach("alpha", VsetId(11), attachment(7, 3)).unwrap();
        let directory = state.lookup(VSETS_INODE, "alpha").unwrap();
        let main = state
            .lookup(directory.inode, VsetFsFile::Main.name())
            .unwrap();
        let wal = state
            .lookup(directory.inode, VsetFsFile::Wal.name())
            .unwrap();
        let main_handle = state.open(main.inode, 2).unwrap();
        let wal_handle = state.open(wal.inode, 2).unwrap();
        state
            .lock(ByteRangeLock {
                handle: main_handle.handle,
                owner: 99,
                start: 0,
                len: 1,
                exclusive: true,
            })
            .unwrap();
        state
            .map(DaxMapping {
                inode: main.inode,
                file_offset: 0,
                window_offset: 0x20_000,
                len: PAGE_SIZE * 3,
                writable: false,
            })
            .unwrap();
        state.replace_shm("alpha", vec![0x5a; 32]).unwrap();
        assert_eq!(wal_handle.handle, 2);
        state
    }

    #[test]
    fn namespace_is_vm_bound_and_generation_stable() {
        let mut state = VsetFsState::new(VmId(7));
        assert_eq!(
            state.lookup(ROOT_INODE, "vsets").unwrap().inode,
            VSETS_INODE
        );
        assert_eq!(
            state.attach("bad/name", VsetId(1), attachment(7, 1)),
            Err(VsetFsError::InvalidName)
        );
        assert_eq!(
            state.attach("alpha", VsetId(1), attachment(8, 1)),
            Err(VsetFsError::WrongVm)
        );
        let attached = state.attach("alpha", VsetId(1), attachment(7, 1)).unwrap();
        assert_eq!(state.export_generation(), 1);
        let main = state
            .lookup(attached.directory_inode, VsetFsFile::Main.name())
            .unwrap();
        assert_eq!(main.vset, Some(VsetId(1)));
        assert_eq!(main.attachment, Some(attachment(7, 1)));
        assert_eq!(
            state.attach("other", VsetId(1), attachment(7, 2)),
            Err(VsetFsError::AlreadyExists)
        );
    }

    #[test]
    fn locks_conflict_only_on_same_inode_and_overlapping_foreign_owner() {
        let mut state = VsetFsState::new(VmId(1));
        let directory = state
            .attach("db", VsetId(1), attachment(1, 1))
            .unwrap()
            .directory_inode;
        let main = state.lookup(directory, "database.sqlite").unwrap();
        let one = state.open(main.inode, 2).unwrap();
        let two = state.open(main.inode, 2).unwrap();
        state
            .lock(ByteRangeLock {
                handle: one.handle,
                owner: 10,
                start: 100,
                len: 100,
                exclusive: false,
            })
            .unwrap();
        state
            .lock(ByteRangeLock {
                handle: two.handle,
                owner: 11,
                start: 200,
                len: 100,
                exclusive: true,
            })
            .unwrap();
        assert_eq!(
            state.lock(ByteRangeLock {
                handle: two.handle,
                owner: 11,
                start: 199,
                len: 1,
                exclusive: true,
            }),
            Err(VsetFsError::Conflict)
        );
        state.unlock(one.handle, 10, 0, 0).unwrap();
        state
            .lock(ByteRangeLock {
                handle: two.handle,
                owner: 11,
                start: 199,
                len: 1,
                exclusive: true,
            })
            .unwrap();
    }

    #[test]
    fn replacing_and_unlocking_subranges_preserves_both_lock_tails() {
        let mut state = VsetFsState::new(VmId(1));
        let directory = state
            .attach("db", VsetId(1), attachment(1, 1))
            .unwrap()
            .directory_inode;
        let main = state.lookup(directory, "database.sqlite").unwrap();
        let handle = state.open(main.inode, 2).unwrap();
        state
            .lock(ByteRangeLock {
                handle: handle.handle,
                owner: 10,
                start: 100,
                len: 100,
                exclusive: false,
            })
            .unwrap();
        state
            .lock(ByteRangeLock {
                handle: handle.handle,
                owner: 10,
                start: 125,
                len: 50,
                exclusive: true,
            })
            .unwrap();
        assert_eq!(
            state.locks(),
            &[
                ByteRangeLock {
                    handle: handle.handle,
                    owner: 10,
                    start: 100,
                    len: 25,
                    exclusive: false,
                },
                ByteRangeLock {
                    handle: handle.handle,
                    owner: 10,
                    start: 125,
                    len: 50,
                    exclusive: true,
                },
                ByteRangeLock {
                    handle: handle.handle,
                    owner: 10,
                    start: 175,
                    len: 25,
                    exclusive: false,
                }
            ]
        );
        state.unlock(handle.handle, 10, 140, 20).unwrap();
        assert_eq!(
            state.locks(),
            &[
                ByteRangeLock {
                    handle: handle.handle,
                    owner: 10,
                    start: 100,
                    len: 25,
                    exclusive: false,
                },
                ByteRangeLock {
                    handle: handle.handle,
                    owner: 10,
                    start: 125,
                    len: 15,
                    exclusive: true,
                },
                ByteRangeLock {
                    handle: handle.handle,
                    owner: 10,
                    start: 160,
                    len: 15,
                    exclusive: true,
                },
                ByteRangeLock {
                    handle: handle.handle,
                    owner: 10,
                    start: 175,
                    len: 25,
                    exclusive: false,
                }
            ]
        );
    }

    #[test]
    fn partial_unmap_splits_and_rebases_file_offsets() {
        let mut state = populated();
        state.unmap(0x21_000, PAGE_SIZE).unwrap();
        assert_eq!(
            state.mappings(),
            &[
                DaxMapping {
                    inode: 4,
                    file_offset: 0,
                    window_offset: 0x20_000,
                    len: PAGE_SIZE,
                    writable: false,
                },
                DaxMapping {
                    inode: 4,
                    file_offset: PAGE_SIZE * 2,
                    window_offset: 0x22_000,
                    len: PAGE_SIZE,
                    writable: false,
                }
            ]
        );
    }

    #[test]
    fn shm_has_explicit_existence_sparse_writes_and_truncation() {
        let mut state = VsetFsState::new(VmId(1));
        state.attach("db", VsetId(1), attachment(1, 1)).unwrap();
        assert!(!state.shm_exists("db"));
        assert_eq!(state.write_shm("db", 0, &[1]), Err(VsetFsError::NotFound));
        state.create_shm("db").unwrap();
        state.write_shm("db", 3, &[4, 5]).unwrap();
        assert_eq!(state.shm("db"), Some(&[0, 0, 0, 4, 5][..]));
        state.truncate_shm("db", 8).unwrap();
        assert_eq!(state.shm("db"), Some(&[0, 0, 0, 4, 5, 0, 0, 0][..]));
        state.delete_shm("db").unwrap();
        assert!(!state.shm_exists("db"));
        assert_eq!(state.shm("db"), Some(&[][..]));
    }

    #[test]
    fn graceful_detach_waits_but_forced_detach_returns_revocations() {
        let mut state = populated();
        state.begin_detach("alpha").unwrap();
        let directory = state.lookup(VSETS_INODE, "alpha").unwrap();
        let shm = state
            .lookup(directory.inode, "database.sqlite-shm")
            .unwrap();
        assert_eq!(state.open(shm.inode, 2), Err(VsetFsError::Draining));
        assert_eq!(state.finish_detach("alpha"), Err(VsetFsError::Busy));
        let revoked = state.force_detach("alpha").unwrap();
        assert_eq!(revoked.len(), 1);
        assert_eq!(state.export_count(), 0);
        assert_eq!(state.handle_count(), 0);
        assert_eq!(state.lookup(VSETS_INODE, "alpha"), None);
    }

    #[test]
    fn one_vm_namespace_holds_five_hundred_independent_database_vsets() {
        let vm = VmId(9);
        let mut state = VsetFsState::new(vm);
        for index in 0..500_u64 {
            let name = format!("database-{index:03}");
            state
                .attach(
                    &name,
                    VsetId(10_000 + index),
                    attachment(vm.0, 20_000 + index),
                )
                .unwrap();
        }
        assert_eq!(state.export_count(), 500);
        assert_eq!(state.directory_entries(VSETS_INODE).unwrap().len(), 500);

        for index in [0_u64, 1, 249, 499] {
            let name = format!("database-{index:03}");
            let directory = state.lookup(VSETS_INODE, &name).unwrap();
            let main = state
                .lookup(directory.inode, VsetFsFile::Main.name())
                .unwrap();
            assert_eq!(main.vset, Some(VsetId(10_000 + index)));
            assert_eq!(state.export_name(main.inode), Some(name.as_str()));
        }

        let restored = VsetFsState::decode_snapshot(&state.encode_snapshot()).unwrap();
        assert_eq!(restored.export_count(), 500);
        assert_eq!(restored, state);
    }

    #[test]
    fn warm_snapshot_round_trips_and_is_byte_pinned() {
        let state = populated();
        let bytes = state.encode_snapshot();
        assert_eq!(VsetFsState::decode_snapshot(&bytes), Ok(state));
        assert_eq!(bytes.len(), 277);
        assert_eq!(crc32c(&bytes), 0x3b7a_be38);
    }

    #[test]
    fn every_snapshot_bit_flip_and_truncation_is_rejected() {
        let bytes = populated().encode_snapshot();
        for keep in 0..bytes.len() {
            assert!(VsetFsState::decode_snapshot(&bytes[..keep]).is_err());
        }
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(VsetFsState::decode_snapshot(&damaged).is_err());
        }
    }
}
