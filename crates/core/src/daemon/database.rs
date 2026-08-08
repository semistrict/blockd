//! Storage-only database requests and volatile VM attachment leases.

use std::collections::{BTreeMap, VecDeque};

use super::{Daemon, Pending};
use crate::database::{
    AttachmentId, DatabaseError, DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest,
    MAX_DATABASE_IO,
};
use crate::journal::{DatabaseFileMeta, VsetKind};
use crate::mapleaf::span_of;
use crate::seam::{AdminReply, DetachMode, Effect, HostMap, PeerMsg, ReqId, StoreFault, TimerId};
use crate::segment::PageLoc;
use crate::types::{Gen, PageId, VsetId, page_size};

const MAX_QUEUED: usize = 256;

#[derive(Debug, Default)]
pub(super) struct DatabaseRuntime {
    phase: AttachmentPhase,
    handles: BTreeMap<u64, DatabaseFile>,
    queue: VecDeque<DatabaseRequest>,
    active: Option<Active>,
    store_retry: Option<(PageId, Gen, PageLoc)>,
    drain_barrier: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum AttachmentPhase {
    #[default]
    Detached,
    Attached(AttachmentId),
    Draining(AttachmentId),
    /// Authority is retired, but one already-started mutation may still be
    /// finishing its async read-modify-write. No new attachment is granted
    /// until it reaches an operation boundary.
    ForcedDetached(AttachmentId),
}

#[derive(Debug)]
enum Active {
    Read {
        req: ReqId,
        file: DatabaseFile,
        offset: u64,
        len: usize,
        cursor: usize,
        output: Vec<u8>,
        eof: bool,
        fetched: Option<(PageId, Vec<u8>)>,
    },
    Write {
        req: ReqId,
        file: DatabaseFile,
        offset: u64,
        bytes: Vec<u8>,
        cursor: usize,
        sequence: u64,
        fetched: Option<(PageId, Vec<u8>)>,
    },
    Truncate {
        req: ReqId,
        file: DatabaseFile,
        old_size: u64,
        size: u64,
        sequence: u64,
        fetched: Option<(PageId, Vec<u8>)>,
    },
}

enum PageResolution {
    Ready(Vec<u8>),
    Fetch,
    Park,
    Dead,
}

impl DatabaseRuntime {
    pub(super) fn capture_seq(&self, mutation_seq: u64) -> u64 {
        match self.active {
            Some(Active::Write { sequence, .. } | Active::Truncate { sequence, .. }) => {
                sequence.saturating_sub(1)
            }
            _ => mutation_seq,
        }
    }

    fn mutation_in_flight(&self) -> bool {
        matches!(
            self.active,
            Some(Active::Write { .. } | Active::Truncate { .. })
        )
    }

    fn id(&self) -> Option<AttachmentId> {
        match self.phase {
            AttachmentPhase::Detached => None,
            AttachmentPhase::Attached(id)
            | AttachmentPhase::Draining(id)
            | AttachmentPhase::ForcedDetached(id) => Some(id),
        }
    }

    pub(super) fn is_detached(&self) -> bool {
        self.phase == AttachmentPhase::Detached
            && self.queue.is_empty()
            && self.active.is_none()
            && self.store_retry.is_none()
    }

    pub(super) fn detach_barrier(&self) -> Option<u64> {
        matches!(self.phase, AttachmentPhase::Draining(_)).then_some(self.drain_barrier)
    }
}

impl Daemon {
    pub(super) fn attach_database(
        &mut self,
        req: ReqId,
        vset: VsetId,
        vm: crate::types::VmId,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        if !state.ready
            || state.config.kind != VsetKind::Database
            || state.outbound.is_some()
            || state.migrate.is_some()
            || state.database_runtime.phase != AttachmentPhase::Detached
        {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        }
        let attachment = AttachmentId {
            vm,
            generation: self.next_attachment_generation,
        };
        self.next_attachment_generation += 1;
        state.database_runtime.phase = AttachmentPhase::Attached(attachment);
        out.push(Effect::Admin(AdminReply::DatabaseAttached {
            req,
            vset,
            attachment,
        }));
    }

    pub(super) fn begin_detach_database(
        &mut self,
        req: ReqId,
        vset: VsetId,
        attachment: AttachmentId,
        mode: DetachMode,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        if state.database_runtime.id() != Some(attachment)
            || matches!(state.database_runtime.phase, AttachmentPhase::Detached)
        {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        }
        let forced = mode == DetachMode::Forced;
        if matches!(
            (state.database_runtime.phase, mode),
            (AttachmentPhase::Draining(id), DetachMode::Graceful)
                | (AttachmentPhase::ForcedDetached(id), DetachMode::Forced)
                if id == attachment
        ) {
            out.push(Effect::Admin(AdminReply::DatabaseDetachStarted {
                req,
                vset,
                attachment,
                forced,
            }));
            if !forced {
                self.maybe_start_commit(vset, mem, out);
            }
            return;
        }
        if forced {
            for request in state.database_runtime.queue.drain(..) {
                out.push(Effect::Database(DatabaseReply::Failed {
                    req: request.req,
                    error: DatabaseError::StaleAttachment,
                }));
            }
            state.database_runtime.handles.clear();
            for (sync_req, _) in state.pending_database_syncs.drain(..) {
                out.push(Effect::Database(DatabaseReply::Failed {
                    req: sync_req,
                    error: DatabaseError::StaleAttachment,
                }));
            }
            state.database_runtime.phase = AttachmentPhase::ForcedDetached(attachment);
        } else {
            if !matches!(state.database_runtime.phase, AttachmentPhase::Attached(_)) {
                out.push(Effect::Admin(AdminReply::AdminFailed { req }));
                return;
            }
            state.database_runtime.drain_barrier = state.mutation_seq;
            state.database_runtime.phase = AttachmentPhase::Draining(attachment);
        }
        out.push(Effect::Admin(AdminReply::DatabaseDetachStarted {
            req,
            vset,
            attachment,
            forced,
        }));
        if !forced {
            self.maybe_start_commit(vset, mem, out);
        }
    }

    pub(super) fn finish_detach_database(
        &mut self,
        req: ReqId,
        vset: VsetId,
        attachment: AttachmentId,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let Some(state) = self.vsets.get_mut(&vset) else {
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        };
        let forced = state.database_runtime.phase == AttachmentPhase::ForcedDetached(attachment);
        state.database_runtime.drain_barrier = state.mutation_seq;
        let quiescent = matches!(
            state.database_runtime.phase,
            AttachmentPhase::Draining(id) | AttachmentPhase::ForcedDetached(id)
                if id == attachment
        ) && state.database_runtime.handles.is_empty()
            && state.database_runtime.queue.is_empty()
            && state.database_runtime.active.is_none();
        let durable = forced || state.sync_ack_through >= state.database_runtime.drain_barrier;
        if !quiescent || !durable {
            if quiescent && !durable {
                self.maybe_start_commit(vset, mem, out);
            }
            out.push(Effect::Admin(AdminReply::AdminFailed { req }));
            return;
        }
        state.database_runtime.phase = AttachmentPhase::Detached;
        out.push(Effect::Admin(AdminReply::DatabaseDetached {
            req,
            vset,
            attachment,
        }));
    }

    pub(super) fn database_request(
        &mut self,
        request: DatabaseRequest,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let req = request.req;
        let Some(state) = self.vsets.get_mut(&request.vset) else {
            Self::database_fail(req, DatabaseError::NotAttached, out);
            return;
        };
        if !state.ready || state.config.kind != VsetKind::Database || state.outbound.is_some() {
            Self::database_fail(req, DatabaseError::NotAttached, out);
            return;
        }
        let phase = state.database_runtime.phase;
        if state.database_runtime.id() != Some(request.attachment)
            || matches!(
                phase,
                AttachmentPhase::Detached | AttachmentPhase::ForcedDetached(_)
            )
        {
            Self::database_fail(req, DatabaseError::StaleAttachment, out);
            return;
        }
        if matches!(phase, AttachmentPhase::Draining(_))
            && !matches!(
                request.op,
                DatabaseOp::Close { .. } | DatabaseOp::Sync { .. }
            )
        {
            Self::database_fail(req, DatabaseError::Draining, out);
            return;
        }
        if !Self::database_request_bounded(state, &request.op) {
            Self::database_fail(req, DatabaseError::TooLarge, out);
            return;
        }
        if state.database_runtime.queue.len() >= MAX_QUEUED {
            Self::database_fail(req, DatabaseError::Busy, out);
            return;
        }
        let vset = request.vset;
        state.database_runtime.queue.push_back(request);
        self.drive_database(vset, mem, out);
    }

    fn database_request_bounded(state: &super::Vset, op: &DatabaseOp) -> bool {
        let max = u64::from(state.config.pages_per_volume)
            * u64::try_from(page_size()).expect("page size fits");
        let range = |offset: u64, len: usize| {
            len <= MAX_DATABASE_IO
                && offset
                    .checked_add(u64::try_from(len).expect("request bound fits"))
                    .is_some_and(|end| end <= max)
        };
        match op {
            DatabaseOp::Read { offset, len, .. } => range(
                *offset,
                usize::try_from(*len).unwrap_or(MAX_DATABASE_IO.saturating_add(1)),
            ),
            DatabaseOp::Write { offset, bytes, .. } => range(*offset, bytes.len()),
            DatabaseOp::Truncate { size, .. } => *size <= max,
            _ => true,
        }
    }

    fn database_op_waits_for_capture(op: &DatabaseOp) -> bool {
        matches!(
            op,
            DatabaseOp::Write { .. } | DatabaseOp::Open { create: true, .. }
        )
    }

    fn database_fail(req: ReqId, error: DatabaseError, out: &mut Vec<Effect>) {
        out.push(Effect::Database(DatabaseReply::Failed { req, error }));
    }

    /// Run immediate operations and advance at most until a cache fetch or
    /// cache-pressure wait. The per-vset queue is the mutation order.
    pub(super) fn drive_database(
        &mut self,
        vset: VsetId,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        // A request may cover up to 1 MiB. Yield after a small number of
        // page operations so its copies and temporary page buffers cannot
        // monopolize the decider loop.
        const PAGE_OPS_PER_STEP: usize = 16;
        let mut budget = PAGE_OPS_PER_STEP;
        loop {
            let capture_blocks_mutation = self.vsets.get(&vset).is_some_and(|state| {
                state.commit_running
                    && (state.database_runtime.mutation_in_flight()
                        || (state.database_runtime.active.is_none()
                            && state.database_runtime.queue.front().is_some_and(|request| {
                                Self::database_op_waits_for_capture(&request.op)
                            })))
            });
            if capture_blocks_mutation {
                return;
            }
            if self
                .vsets
                .get(&vset)
                .is_none_or(|state| state.database_runtime.store_retry.is_some())
            {
                return;
            }
            if self.vsets[&vset].database_runtime.active.is_none() {
                let Some(request) = self
                    .vsets
                    .get_mut(&vset)
                    .and_then(|state| state.database_runtime.queue.pop_front())
                else {
                    return;
                };
                if self.start_database_request(request, mem, out) {
                    budget -= 1;
                    if budget == 0 {
                        self.continue_database_later(vset, out);
                        return;
                    }
                    continue;
                }
            }
            let progressed = self.advance_database_active(vset, mem, out);
            if !progressed {
                return;
            }
            budget -= 1;
            if budget == 0 {
                self.continue_database_later(vset, out);
                return;
            }
        }
    }

    fn continue_database_later(&self, vset: VsetId, out: &mut Vec<Effect>) {
        if self.vsets.get(&vset).is_some_and(|state| {
            state.database_runtime.active.is_some() || !state.database_runtime.queue.is_empty()
        }) {
            out.push(Effect::SetTimer {
                timer: TimerId::DatabaseStep(vset),
                after: 0,
            });
        }
    }

    pub(super) fn drive_database_waiters(&mut self, mem: &dyn HostMap, out: &mut Vec<Effect>) {
        let vsets: Vec<VsetId> = self
            .vsets
            .iter()
            .filter_map(|(&vset, state)| state.database_runtime.active.is_some().then_some(vset))
            .collect();
        for vset in vsets {
            self.drive_database(vset, mem, out);
        }
    }

    /// Returns true when the request completed immediately.
    #[allow(clippy::too_many_lines)]
    fn start_database_request(
        &mut self,
        request: DatabaseRequest,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) -> bool {
        let vset = request.vset;
        let req = request.req;
        match request.op {
            DatabaseOp::Open {
                handle,
                file,
                create,
            } => {
                let state = self.vsets.get_mut(&vset).expect("validated");
                if state.database_runtime.handles.contains_key(&handle) {
                    Self::database_fail(req, DatabaseError::AlreadyOpen, out);
                    return true;
                }
                let meta = Self::file_meta(state, file);
                if !meta.exists && !create {
                    Self::database_fail(req, DatabaseError::NotFound, out);
                    return true;
                }
                if !meta.exists {
                    state.mutation_seq += 1;
                    *Self::file_meta_mut(state, file) = DatabaseFileMeta {
                        exists: true,
                        size: 0,
                    };
                }
                state.database_runtime.handles.insert(handle, file);
                out.push(Effect::Database(DatabaseReply::Opened { req }));
                true
            }
            DatabaseOp::Close { handle } => {
                let state = self.vsets.get_mut(&vset).expect("validated");
                if state.database_runtime.handles.remove(&handle).is_none() {
                    Self::database_fail(req, DatabaseError::InvalidHandle, out);
                } else {
                    out.push(Effect::Database(DatabaseReply::Closed { req }));
                }
                true
            }
            DatabaseOp::FileSize { handle } => {
                let state = &self.vsets[&vset];
                let Some(&file) = state.database_runtime.handles.get(&handle) else {
                    Self::database_fail(req, DatabaseError::InvalidHandle, out);
                    return true;
                };
                out.push(Effect::Database(DatabaseReply::FileSize {
                    req,
                    size: Self::file_meta(state, file).size,
                }));
                true
            }
            DatabaseOp::Access { file } => {
                let exists = Self::file_meta(&self.vsets[&vset], file).exists;
                out.push(Effect::Database(DatabaseReply::Access { req, exists }));
                true
            }
            DatabaseOp::Stat { file } => {
                let meta = Self::file_meta(&self.vsets[&vset], file);
                out.push(Effect::Database(DatabaseReply::Stat {
                    req,
                    exists: meta.exists,
                    size: meta.size,
                }));
                true
            }
            DatabaseOp::Delete { file } => {
                let state = self.vsets.get_mut(&vset).expect("validated");
                if state.commit_running || !state.pending_leaves.is_empty() {
                    Self::database_fail(req, DatabaseError::Busy, out);
                    return true;
                }
                state.mutation_seq += 1;
                let sequence = state.mutation_seq;
                Self::prune_file(state, &mut self.cache, vset, file, 0, sequence, out);
                *Self::file_meta_mut(state, file) = DatabaseFileMeta::default();
                out.push(Effect::Database(DatabaseReply::Deleted { req, sequence }));
                true
            }
            DatabaseOp::Sync { handle } => {
                let state = self.vsets.get_mut(&vset).expect("validated");
                if !state.database_runtime.handles.contains_key(&handle) {
                    Self::database_fail(req, DatabaseError::InvalidHandle, out);
                    return true;
                }
                let barrier = state.mutation_seq;
                state.database_runtime.drain_barrier =
                    state.database_runtime.drain_barrier.max(barrier);
                if state.sync_ack_through >= barrier {
                    out.push(Effect::Database(DatabaseReply::Synced {
                        req,
                        sequence: barrier,
                    }));
                } else {
                    state.pending_database_syncs.push((req, barrier));
                    self.maybe_start_commit(vset, mem, out);
                }
                true
            }
            DatabaseOp::Read {
                handle,
                offset,
                len,
            } => {
                let state = &self.vsets[&vset];
                let Some(&file) = state.database_runtime.handles.get(&handle) else {
                    Self::database_fail(req, DatabaseError::InvalidHandle, out);
                    return true;
                };
                let meta = Self::file_meta(state, file);
                if !meta.exists {
                    Self::database_fail(req, DatabaseError::NotFound, out);
                    return true;
                }
                let available = meta.size.saturating_sub(offset).min(u64::from(len));
                let eof = available < u64::from(len);
                self.vsets
                    .get_mut(&vset)
                    .expect("known")
                    .database_runtime
                    .active = Some(Active::Read {
                    req,
                    file,
                    offset,
                    len: usize::try_from(available).expect("bounded"),
                    cursor: 0,
                    output: Vec::with_capacity(usize::try_from(available).expect("bounded")),
                    eof,
                    fetched: None,
                });
                false
            }
            DatabaseOp::Write {
                handle,
                offset,
                bytes,
            } => {
                let state = self.vsets.get_mut(&vset).expect("known");
                let Some(&file) = state.database_runtime.handles.get(&handle) else {
                    Self::database_fail(req, DatabaseError::InvalidHandle, out);
                    return true;
                };
                if !Self::file_meta(state, file).exists {
                    Self::database_fail(req, DatabaseError::NotFound, out);
                    return true;
                }
                if bytes.is_empty() {
                    out.push(Effect::Database(DatabaseReply::Written {
                        req,
                        sequence: state.mutation_seq,
                    }));
                    return true;
                }
                state.mutation_seq += 1;
                let sequence = state.mutation_seq;
                state.database_runtime.active = Some(Active::Write {
                    req,
                    file,
                    offset,
                    bytes,
                    cursor: 0,
                    sequence,
                    fetched: None,
                });
                false
            }
            DatabaseOp::Truncate { handle, size } => {
                let state = self.vsets.get_mut(&vset).expect("known");
                let Some(&file) = state.database_runtime.handles.get(&handle) else {
                    Self::database_fail(req, DatabaseError::InvalidHandle, out);
                    return true;
                };
                let old_size = Self::file_meta(state, file).size;
                if !Self::file_meta(state, file).exists {
                    Self::database_fail(req, DatabaseError::NotFound, out);
                    return true;
                }
                // Truncation prunes every page after the new EOF, so it cannot
                // proceed until the complete durable map is known. Reject
                // before allocating a mutation sequence or touching the tail
                // page; a failed truncate must be observationally atomic.
                if state.commit_running || !state.pending_leaves.is_empty() {
                    Self::database_fail(req, DatabaseError::Busy, out);
                    return true;
                }
                state.mutation_seq += 1;
                let sequence = state.mutation_seq;
                state.database_runtime.active = Some(Active::Truncate {
                    req,
                    file,
                    old_size,
                    size,
                    sequence,
                    fetched: None,
                });
                false
            }
        }
    }

    /// Returns false while parked on I/O or cache pressure.
    #[allow(clippy::too_many_lines)]
    fn advance_database_active(
        &mut self,
        vset: VsetId,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) -> bool {
        let active = self
            .vsets
            .get_mut(&vset)
            .expect("known")
            .database_runtime
            .active
            .take();
        let Some(mut active) = active else {
            return true;
        };
        match &mut active {
            Active::Read {
                req,
                file,
                offset,
                len,
                cursor,
                output,
                eof,
                fetched,
            } => {
                if *cursor == *len {
                    let reply = DatabaseReply::Read {
                        req: *req,
                        bytes: std::mem::take(output),
                        eof: *eof,
                    };
                    self.complete_database_request(vset, *req, reply, out);
                    return true;
                }
                let absolute = *offset + u64::try_from(*cursor).expect("bounded");
                let page_no = u32::try_from(absolute / page_size() as u64).expect("bounded");
                let in_page = usize::try_from(absolute % page_size() as u64).expect("page offset");
                let take = (*len - *cursor).min(page_size() - in_page);
                let page = file.page(vset, page_no);
                let bytes = match self.resolve_database_page(vset, page, fetched, mem, false) {
                    PageResolution::Ready(bytes) => bytes,
                    PageResolution::Fetch => {
                        return self.park_database_active(vset, active, Some(page), out);
                    }
                    PageResolution::Park => {
                        return self.park_database_active(vset, active, None, out);
                    }
                    PageResolution::Dead => {
                        Self::database_fail(*req, DatabaseError::Io, out);
                        return true;
                    }
                };
                output.extend_from_slice(&bytes[in_page..in_page + take]);
                *cursor += take;
                self.vsets
                    .get_mut(&vset)
                    .expect("known")
                    .database_runtime
                    .active = Some(active);
                true
            }
            Active::Write {
                req,
                file,
                offset,
                bytes,
                cursor,
                sequence,
                fetched,
            } => {
                if *cursor == bytes.len() {
                    let end = offset
                        .checked_add(u64::try_from(bytes.len()).expect("bounded"))
                        .expect("validated");
                    let state = self.vsets.get_mut(&vset).expect("known");
                    Self::file_meta_mut(state, *file).size =
                        Self::file_meta(state, *file).size.max(end);
                    let reply = DatabaseReply::Written {
                        req: *req,
                        sequence: *sequence,
                    };
                    self.complete_database_request(vset, *req, reply, out);
                    return true;
                }
                let absolute = *offset + u64::try_from(*cursor).expect("bounded");
                let page_no = u32::try_from(absolute / page_size() as u64).expect("bounded");
                let in_page = usize::try_from(absolute % page_size() as u64).expect("page offset");
                let take = (bytes.len() - *cursor).min(page_size() - in_page);
                let page = file.page(vset, page_no);
                let full = in_page == 0 && take == page_size();
                let mut page_bytes =
                    match self.resolve_database_page(vset, page, fetched, mem, full) {
                        PageResolution::Ready(bytes) => bytes,
                        PageResolution::Fetch => {
                            return self.park_database_active(vset, active, Some(page), out);
                        }
                        PageResolution::Park => {
                            return self.park_database_active(vset, active, None, out);
                        }
                        PageResolution::Dead => {
                            Self::database_fail(*req, DatabaseError::Io, out);
                            return true;
                        }
                    };
                if self.cache.is_resident(page) {
                    self.cache.mark_dirty(page);
                } else {
                    let Some(victim) = self.cache.reserve_slot() else {
                        return self.park_database_active(vset, active, None, out);
                    };
                    if let Some(victim) = victim {
                        out.push(Effect::Evict { page: victim });
                    }
                    self.cache.fill_slot(page, true, false);
                }
                page_bytes[in_page..in_page + take]
                    .copy_from_slice(&bytes[*cursor..*cursor + take]);
                out.push(Effect::DatabaseInstall {
                    page,
                    bytes: page_bytes,
                });
                *cursor += take;
                self.vsets
                    .get_mut(&vset)
                    .expect("known")
                    .database_runtime
                    .active = Some(active);
                true
            }
            Active::Truncate {
                req,
                file,
                old_size,
                size,
                sequence,
                fetched,
            } => {
                if *size < *old_size && *size % page_size() as u64 != 0 {
                    let page_no = u32::try_from(*size / page_size() as u64).expect("bounded");
                    let page = file.page(vset, page_no);
                    let mut page_bytes =
                        match self.resolve_database_page(vset, page, fetched, mem, false) {
                            PageResolution::Ready(bytes) => bytes,
                            PageResolution::Fetch => {
                                return self.park_database_active(vset, active, Some(page), out);
                            }
                            PageResolution::Park => {
                                return self.park_database_active(vset, active, None, out);
                            }
                            PageResolution::Dead => {
                                Self::database_fail(*req, DatabaseError::Io, out);
                                return true;
                            }
                        };
                    let tail = usize::try_from(*size % page_size() as u64).expect("offset");
                    page_bytes[tail..].fill(0);
                    if self.cache.is_resident(page) {
                        self.cache.mark_dirty(page);
                    } else {
                        let Some(victim) = self.cache.reserve_slot() else {
                            return self.park_database_active(vset, active, None, out);
                        };
                        if let Some(victim) = victim {
                            out.push(Effect::Evict { page: victim });
                        }
                        self.cache.fill_slot(page, true, false);
                    }
                    out.push(Effect::DatabaseInstall {
                        page,
                        bytes: page_bytes,
                    });
                }
                let state = self.vsets.get_mut(&vset).expect("known");
                let first_removed = size.div_ceil(page_size() as u64);
                Self::prune_file(
                    state,
                    &mut self.cache,
                    vset,
                    *file,
                    first_removed,
                    *sequence,
                    out,
                );
                Self::file_meta_mut(state, *file).size = *size;
                let reply = DatabaseReply::Truncated {
                    req: *req,
                    sequence: *sequence,
                };
                self.complete_database_request(vset, *req, reply, out);
                true
            }
        }
    }

    fn resolve_database_page(
        &self,
        vset: VsetId,
        page: PageId,
        fetched: &mut Option<(PageId, Vec<u8>)>,
        mem: &dyn HostMap,
        overwrite_full_page: bool,
    ) -> PageResolution {
        let state = &self.vsets[&vset];
        let span = span_of(page);
        if state.pending_leaves.contains_key(&span) {
            return PageResolution::Park;
        }
        if state.dead_spans.contains(&span) {
            return PageResolution::Dead;
        }
        if overwrite_full_page {
            return PageResolution::Ready(vec![0; page_size()]);
        }
        if let Some((got, bytes)) = fetched.take() {
            debug_assert_eq!(got, page);
            return PageResolution::Ready(bytes);
        }
        if self.cache.is_resident(page) {
            return PageResolution::Ready(mem.read_page(page));
        }
        if state.page_locs.contains_key(&page) {
            return PageResolution::Fetch;
        }
        PageResolution::Ready(vec![0; page_size()])
    }

    fn park_database_active(
        &mut self,
        vset: VsetId,
        active: Active,
        fetch: Option<PageId>,
        out: &mut Vec<Effect>,
    ) -> bool {
        self.vsets
            .get_mut(&vset)
            .expect("known")
            .database_runtime
            .active = Some(active);
        if let Some(page) = fetch {
            self.start_database_fetch(vset, page, out);
        }
        false
    }

    fn start_database_fetch(&mut self, vset: VsetId, page: PageId, out: &mut Vec<Effect>) -> bool {
        let Some(victim) = self.cache.reserve_slot() else {
            return false;
        };
        if let Some(victim) = victim {
            out.push(Effect::Evict { page: victim });
        }
        let (generation, loc) = self.vsets[&vset].page_locs[&page];
        let io = self.io();
        self.pending.insert(
            io,
            Pending::DatabaseFetch {
                vset,
                page,
                generation,
                loc,
            },
        );
        out.push(Effect::BlobReadRange {
            io,
            name: crate::layout::segment_blob(vset, loc.fence, loc.seg),
            offset: u64::from(loc.offset),
            len: u64::from(loc.len),
        });
        false
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn database_fetch_done(
        &mut self,
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
        bytes: Option<Vec<u8>>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let bytes = bytes.or_else(|| self.replica_segment_range(vset, loc));
        if let Some(raw) = Self::verify_entry(page, generation, bytes) {
            self.database_page_fetched(vset, page, raw, mem, out);
            return;
        }
        if let Some(source) = self.vsets.get(&vset).and_then(|state| {
            (loc.base == 0 && loc.fence < state.fence)
                .then_some(state.peer_source)
                .flatten()
        }) {
            self.start_database_peer_fetch(vset, page, generation, loc, source, out);
            return;
        }
        self.database_fetch_after_peer(vset, page, generation, loc, mem, out);
    }

    fn start_database_peer_fetch(
        &mut self,
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
        source: crate::types::HostId,
        out: &mut Vec<Effect>,
    ) {
        let io = self.io();
        self.pending.insert(
            io,
            Pending::DatabasePeerFetch {
                vset,
                page,
                generation,
                loc,
            },
        );
        out.push(Effect::PeerSend {
            to: source,
            msg: PeerMsg::FetchRange {
                io,
                vset,
                fence: loc.fence,
                seg: loc.seg,
                offset: loc.offset,
                len: loc.len,
            },
        });
        out.push(Effect::SetTimer {
            timer: TimerId::PeerRetry(io),
            after: super::migrate::PEER_RETRY,
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn database_peer_fetch_done(
        &mut self,
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
        bytes: Option<Vec<u8>>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        if let Some(raw) = Self::verify_entry(page, generation, bytes) {
            self.database_page_fetched(vset, page, raw, mem, out);
        } else {
            self.database_fetch_after_peer(vset, page, generation, loc, mem, out);
        }
    }

    pub(super) fn database_peer_retry(
        &mut self,
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        if let Some(source) = self.vsets.get(&vset).and_then(|state| state.peer_source) {
            self.start_database_peer_fetch(vset, page, generation, loc, source, out);
        } else {
            self.database_fetch_after_peer(vset, page, generation, loc, mem, out);
        }
    }

    fn database_fetch_after_peer(
        &mut self,
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        if self.vsets.contains_key(&vset) {
            let io = self.io();
            self.pending.insert(
                io,
                Pending::DatabaseStoreFetch {
                    vset,
                    page,
                    generation,
                    loc,
                },
            );
            out.push(Effect::StoreGetRange {
                io,
                key: crate::layout::segment_key(vset, loc.fence, loc.seg),
                offset: u64::from(loc.offset),
                len: u64::from(loc.len),
            });
        } else {
            self.database_fetch_failed(vset, mem, out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn database_store_fetch_done(
        &mut self,
        vset: VsetId,
        page: PageId,
        generation: Gen,
        loc: PageLoc,
        result: Result<Option<(u64, Vec<u8>)>, StoreFault>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        if result == Err(StoreFault::Unavailable) {
            if let Some(state) = self.vsets.get_mut(&vset) {
                state.database_runtime.store_retry = Some((page, generation, loc));
            }
            out.push(Effect::SetTimer {
                timer: TimerId::DatabaseRetry(vset),
                after: self.config.backup_retry,
            });
            return;
        }
        let bytes = result.ok().flatten().map(|(_, bytes)| bytes);
        if let Some(raw) = Self::verify_entry(page, generation, bytes) {
            self.database_page_fetched(vset, page, raw, mem, out);
        } else {
            self.database_fetch_failed(vset, mem, out);
        }
    }

    pub(super) fn database_retry(&mut self, vset: VsetId, out: &mut Vec<Effect>) {
        let Some((page, generation, loc)) = self
            .vsets
            .get_mut(&vset)
            .and_then(|state| state.database_runtime.store_retry.take())
        else {
            return;
        };
        let io = self.io();
        self.pending.insert(
            io,
            Pending::DatabaseStoreFetch {
                vset,
                page,
                generation,
                loc,
            },
        );
        out.push(Effect::StoreGetRange {
            io,
            key: crate::layout::segment_key(vset, loc.fence, loc.seg),
            offset: u64::from(loc.offset),
            len: u64::from(loc.len),
        });
    }

    fn database_page_fetched(
        &mut self,
        vset: VsetId,
        page: PageId,
        raw: Vec<u8>,
        mem: &dyn HostMap,
        out: &mut Vec<Effect>,
    ) {
        let state = self.vsets.get_mut(&vset).expect("fetch vset");
        let active = state
            .database_runtime
            .active
            .as_mut()
            .expect("active fetch");
        match active {
            Active::Read { fetched, .. }
            | Active::Write { fetched, .. }
            | Active::Truncate { fetched, .. } => *fetched = Some((page, raw)),
        }
        // Reads install clean. Writes/truncates consume the fetched bytes and
        // install their modified page in the same continuation.
        if matches!(active, Active::Read { .. }) {
            self.cache.fill_slot(page, false, false);
            let bytes = match active {
                Active::Read { fetched, .. } => fetched.as_ref().expect("set").1.clone(),
                _ => unreachable!(),
            };
            out.push(Effect::DatabaseInstall { page, bytes });
        } else {
            self.cache.release_slot();
        }
        self.drive_database(vset, mem, out);
    }

    fn database_fetch_failed(&mut self, vset: VsetId, mem: &dyn HostMap, out: &mut Vec<Effect>) {
        self.cache.release_slot();
        let req = self.vsets.get_mut(&vset).and_then(|state| {
            state
                .database_runtime
                .active
                .take()
                .map(|active| match active {
                    Active::Read { req, .. }
                    | Active::Write { req, .. }
                    | Active::Truncate { req, .. } => req,
                })
        });
        if let Some(req) = req {
            Self::database_fail(req, DatabaseError::Io, out);
        }
        self.drive_database(vset, mem, out);
    }

    fn complete_database_request(
        &self,
        vset: VsetId,
        req: ReqId,
        reply: DatabaseReply,
        out: &mut Vec<Effect>,
    ) {
        if self.vsets.get(&vset).is_some_and(|state| {
            matches!(
                state.database_runtime.phase,
                AttachmentPhase::ForcedDetached(_)
            )
        }) {
            Self::database_fail(req, DatabaseError::StaleAttachment, out);
        } else {
            out.push(Effect::Database(reply));
        }
    }

    fn file_meta(state: &super::Vset, file: DatabaseFile) -> DatabaseFileMeta {
        match file {
            DatabaseFile::Main => state.database.main,
            DatabaseFile::Wal => state.database.wal,
            DatabaseFile::Journal => state.database.journal,
        }
    }

    fn file_meta_mut(state: &mut super::Vset, file: DatabaseFile) -> &mut DatabaseFileMeta {
        match file {
            DatabaseFile::Main => &mut state.database.main,
            DatabaseFile::Wal => &mut state.database.wal,
            DatabaseFile::Journal => &mut state.database.journal,
        }
    }

    fn prune_file(
        state: &mut super::Vset,
        cache: &mut crate::cache::Cache,
        vset: VsetId,
        file: DatabaseFile,
        first_removed: u64,
        sequence: u64,
        out: &mut Vec<Effect>,
    ) {
        let idx = file.volume_index();
        let mut pages: Vec<PageId> = state
            .page_locs
            .keys()
            .copied()
            .filter(|page| page.volume.idx == idx && u64::from(page.page.0) >= first_removed)
            .collect();
        pages.extend(cache.resident_pages_of(vset));
        pages.retain(|page| page.volume.idx == idx && u64::from(page.page.0) >= first_removed);
        pages.sort_unstable();
        pages.dedup();
        for page in pages {
            state.map_remove(page);
            state.overlay.remove(&page);
            let span = span_of(page);
            state
                .database_prune_spans
                .entry(span)
                .and_modify(|op| *op = (*op).max(sequence))
                .or_insert(sequence);
            if cache.remove_page(page) {
                out.push(Effect::Evict { page });
            }
        }
        // Existing leaf spans can contain pages not currently inline. Mark
        // every file span at/after EOF for rebuilding from the serving map.
        let first_page = u32::try_from(first_removed).unwrap_or(u32::MAX);
        let first_span = span_of(file.page(crate::types::VsetId(0), first_page));
        let last_page = state.config.pages_per_volume.saturating_sub(1);
        let last_span = span_of(file.page(crate::types::VsetId(0), last_page));
        for span in first_span..=last_span {
            if state.leaf_table.contains_key(&span) {
                state.database_prune_spans.insert(span, sequence);
            }
        }
        state.rebuild_seg_live();
    }
}
