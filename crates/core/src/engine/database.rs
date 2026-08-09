use std::collections::BTreeSet;
use std::rc::Rc;

use blockd_exec::yield_now;

use super::capture::{shard_map, write_record_copies};
use super::fault::load_page_for_database;
use super::reclaim::cleanup_local;
use super::replica::replicate_latest;
use super::state::{AttachmentPhase, SharedHost};
use crate::database::{
    AttachmentId, DatabaseError, DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest,
    MAX_DATABASE_IO,
};
use crate::journal::{DatabaseFileMeta, JournalRecord, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::{LEAF_SPAN, span_of};
use crate::protocol::{AdminReply, DetachMode, ReqId};
use crate::segment::SegmentBatchBuilder;
use crate::types::{Gen, JournalSeq, PageId, PageNo, SegId, VsetId, page_size};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store};

pub async fn attach_database<W: AdminIo>(
    state: SharedHost,
    world: &W,
    req: ReqId,
    vset: VsetId,
    vm: crate::types::VmId,
) {
    let attachment = {
        let mut host = state.borrow_mut();
        let valid = host.vsets.get(&vset).is_some_and(|vset_state| {
            vset_state.ready
                && vset_state.config.kind == VsetKind::Database
                && vset_state.outbound.is_none()
                && vset_state.database_runtime.phase == AttachmentPhase::Detached
        });
        if valid {
            let attachment = host.allocate_attachment(vm);
            host.vsets
                .get_mut(&vset)
                .expect("validated vset")
                .database_runtime
                .phase = AttachmentPhase::Attached(attachment);
            Some(attachment)
        } else {
            None
        }
    };
    let reply = attachment.map_or(AdminReply::AdminFailed { req }, |attachment| {
        AdminReply::DatabaseAttached {
            req,
            vset,
            attachment,
        }
    });
    AdminIo::reply_admin(world, reply).await;
}

pub async fn begin_detach_database<W>(
    state: SharedHost,
    world: Rc<W>,
    req: ReqId,
    vset: VsetId,
    attachment: AttachmentId,
    mode: DetachMode,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let started = {
        let mut host = state.borrow_mut();
        host.vsets.get_mut(&vset).is_some_and(|vset_state| {
            let runtime = &mut vset_state.database_runtime;
            let current = match runtime.phase {
                AttachmentPhase::Attached(id)
                | AttachmentPhase::Draining(id)
                | AttachmentPhase::Forced(id) => Some(id),
                AttachmentPhase::Detached => None,
            };
            if current != Some(attachment) {
                return false;
            }
            runtime.drain_barrier = vset_state.mutation_seq;
            runtime.phase = if mode == DetachMode::Forced {
                runtime.handles.clear();
                AttachmentPhase::Forced(attachment)
            } else {
                AttachmentPhase::Draining(attachment)
            };
            true
        })
    };
    let reply = if started {
        AdminReply::DatabaseDetachStarted {
            req,
            vset,
            attachment,
            forced: mode == DetachMode::Forced,
        }
    } else {
        AdminReply::AdminFailed { req }
    };
    AdminIo::reply_admin(world.as_ref(), reply).await;
    if !started || mode == DetachMode::Forced {
        return;
    }
    loop {
        let active = state
            .borrow()
            .vsets
            .get(&vset)
            .is_some_and(|vset_state| vset_state.database_runtime.active.is_some());
        if !active {
            break;
        }
        yield_now().await;
    }
    let barrier = {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host.vsets.get_mut(&vset) else {
            return;
        };
        if vset_state.database_runtime.phase != AttachmentPhase::Draining(attachment) {
            return;
        }
        vset_state.database_runtime.drain_barrier = vset_state.mutation_seq;
        vset_state.mutation_seq
    };
    let _ = ensure_database_sync(&state, world, vset, barrier).await;
}

pub async fn finish_detach_database<W: AdminIo>(
    state: SharedHost,
    world: &W,
    req: ReqId,
    vset: VsetId,
    attachment: AttachmentId,
) {
    let detached = {
        let mut host = state.borrow_mut();
        host.vsets.get_mut(&vset).is_some_and(|vset_state| {
            let runtime = &mut vset_state.database_runtime;
            let forced = runtime.phase == AttachmentPhase::Forced(attachment);
            let valid = matches!(
                runtime.phase,
                AttachmentPhase::Draining(id) | AttachmentPhase::Forced(id) if id == attachment
            );
            if !valid
                || !runtime.handles.is_empty()
                || runtime.active.is_some()
                || (!forced && vset_state.sync_ack_through < runtime.drain_barrier)
            {
                return false;
            }
            runtime.phase = AttachmentPhase::Detached;
            true
        })
    };
    let reply = if detached {
        AdminReply::DatabaseDetached {
            req,
            vset,
            attachment,
        }
    } else {
        AdminReply::AdminFailed { req }
    };
    AdminIo::reply_admin(world, reply).await;
}

pub async fn database_source<W>(state: SharedHost, world: Rc<W>)
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    while let Some(request) = AdminIo::next_database(world.as_ref()).await {
        let vset = request.vset;
        let attachment = request.attachment;
        let tracked = {
            let mut host = state.borrow_mut();
            host.vsets.get_mut(&vset).is_some_and(|vset_state| {
                let valid = matches!(
                    vset_state.database_runtime.phase,
                    AttachmentPhase::Attached(id) | AttachmentPhase::Draining(id)
                        if id == attachment
                );
                if valid {
                    vset_state.database_runtime.active = Some(attachment);
                }
                valid
            })
        };
        let reply = database_request(&state, Rc::clone(&world), request).await;
        let retired = if tracked {
            let mut host = state.borrow_mut();
            if let Some(vset_state) = host.vsets.get_mut(&vset) {
                if vset_state.database_runtime.active == Some(attachment) {
                    vset_state.database_runtime.active = None;
                }
                !matches!(
                    vset_state.database_runtime.phase,
                    AttachmentPhase::Attached(id) | AttachmentPhase::Draining(id)
                        if id == attachment
                )
            } else {
                true
            }
        } else {
            false
        };
        let reply = if retired {
            failed(reply.req(), DatabaseError::StaleAttachment)
        } else {
            reply
        };
        AdminIo::reply_database(world.as_ref(), reply).await;
    }
}

#[allow(clippy::too_many_lines)]
async fn database_request<W>(
    state: &SharedHost,
    world: Rc<W>,
    request: DatabaseRequest,
) -> DatabaseReply
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let req = request.req;
    let phase = state
        .borrow()
        .vsets
        .get(&request.vset)
        .filter(|vset| vset.ready && vset.config.kind == VsetKind::Database)
        .map(|vset| vset.database_runtime.phase);
    let Some(phase) = phase else {
        return failed(req, DatabaseError::NotAttached);
    };
    let current = match phase {
        AttachmentPhase::Attached(id)
        | AttachmentPhase::Draining(id)
        | AttachmentPhase::Forced(id) => Some(id),
        AttachmentPhase::Detached => None,
    };
    if current != Some(request.attachment)
        || matches!(
            phase,
            AttachmentPhase::Detached | AttachmentPhase::Forced(_)
        )
    {
        return failed(req, DatabaseError::StaleAttachment);
    }
    if matches!(phase, AttachmentPhase::Draining(_))
        && !matches!(
            request.op,
            DatabaseOp::Close { .. } | DatabaseOp::Sync { .. }
        )
    {
        return failed(req, DatabaseError::Draining);
    }
    if !bounded_request(state, request.vset, &request.op) {
        return failed(req, DatabaseError::TooLarge);
    }
    match request.op {
        DatabaseOp::Open {
            handle,
            file,
            create,
        } => {
            open(
                state,
                world.as_ref(),
                request.vset,
                req,
                handle,
                file,
                create,
                request.attachment,
            )
            .await
        }
        DatabaseOp::Close { handle } => close(state, request.vset, req, handle),
        DatabaseOp::Read {
            handle,
            offset,
            len,
        } => {
            read(
                state,
                world.as_ref(),
                request.vset,
                req,
                handle,
                offset,
                len,
            )
            .await
        }
        DatabaseOp::FileSize { handle } => file_size(state, request.vset, req, handle),
        DatabaseOp::Access { file } => {
            let exists = file_meta(state.borrow().vsets[&request.vset].database, file).exists;
            DatabaseReply::Access { req, exists }
        }
        DatabaseOp::Stat { file } => {
            let meta = file_meta(state.borrow().vsets[&request.vset].database, file);
            DatabaseReply::Stat {
                req,
                exists: meta.exists,
                size: meta.size,
            }
        }
        DatabaseOp::Sync { handle } => {
            sync(state, world, request.vset, req, handle, request.attachment).await
        }
        DatabaseOp::Write {
            handle,
            offset,
            bytes,
        } => {
            write(
                state,
                world.as_ref(),
                request.vset,
                req,
                handle,
                offset,
                bytes,
            )
            .await
        }
        DatabaseOp::Truncate { handle, size } => {
            truncate(state, world.as_ref(), request.vset, req, handle, size).await
        }
        DatabaseOp::Delete { file } => delete(state, world.as_ref(), request.vset, req, file).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn open<W: Blobs + AdminIo>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    req: ReqId,
    handle: u64,
    file: DatabaseFile,
    create: bool,
    attachment: AttachmentId,
) -> DatabaseReply {
    let (exists, database) = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        if vset_state.database_runtime.handles.contains_key(&handle) {
            return failed(req, DatabaseError::AlreadyOpen);
        }
        let meta = file_meta(vset_state.database, file);
        if !meta.exists && !create {
            return failed(req, DatabaseError::NotFound);
        }
        let mut database = vset_state.database;
        if !meta.exists {
            *file_meta_mut(&mut database, file) = DatabaseFileMeta {
                exists: true,
                size: 0,
            };
        }
        (meta.exists, database)
    };
    if !exists
        && persist_database(state, world, vset, Vec::new(), None, database, true, None)
            .await
            .is_err()
    {
        return failed(req, DatabaseError::Io);
    }
    let mut host = state.borrow_mut();
    let Some(vset_state) = host.vsets.get_mut(&vset) else {
        return failed(req, DatabaseError::StaleAttachment);
    };
    if !matches!(
        vset_state.database_runtime.phase,
        AttachmentPhase::Attached(id) | AttachmentPhase::Draining(id) if id == attachment
    ) {
        return failed(req, DatabaseError::StaleAttachment);
    }
    vset_state.database_runtime.handles.insert(handle, file);
    DatabaseReply::Opened { req }
}

fn close(state: &SharedHost, vset: VsetId, req: ReqId, handle: u64) -> DatabaseReply {
    let removed = state
        .borrow_mut()
        .vsets
        .get_mut(&vset)
        .and_then(|vset| vset.database_runtime.handles.remove(&handle));
    removed.map_or_else(
        || failed(req, DatabaseError::InvalidHandle),
        |_| DatabaseReply::Closed { req },
    )
}

fn file_size(state: &SharedHost, vset: VsetId, req: ReqId, handle: u64) -> DatabaseReply {
    let host = state.borrow();
    let vset_state = &host.vsets[&vset];
    let Some(&file) = vset_state.database_runtime.handles.get(&handle) else {
        return failed(req, DatabaseError::InvalidHandle);
    };
    DatabaseReply::FileSize {
        req,
        size: file_meta(vset_state.database, file).size,
    }
}

async fn sync<W>(
    state: &SharedHost,
    world: Rc<W>,
    vset: VsetId,
    req: ReqId,
    handle: u64,
    attachment: AttachmentId,
) -> DatabaseReply
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let barrier = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        if !vset_state.database_runtime.handles.contains_key(&handle) {
            return failed(req, DatabaseError::InvalidHandle);
        }
        vset_state.mutation_seq
    };
    if ensure_database_sync(state, Rc::clone(&world), vset, barrier)
        .await
        .is_err()
    {
        return failed(req, DatabaseError::Io);
    }
    let host = state.borrow();
    let Some(vset_state) = host.vsets.get(&vset) else {
        return failed(req, DatabaseError::StaleAttachment);
    };
    if !matches!(
        vset_state.database_runtime.phase,
        AttachmentPhase::Attached(id) | AttachmentPhase::Draining(id) if id == attachment
    ) {
        return failed(req, DatabaseError::StaleAttachment);
    }
    if vset_state.sync_ack_through < barrier {
        return failed(req, DatabaseError::Io);
    }
    DatabaseReply::Synced {
        req,
        sequence: barrier,
    }
}

async fn ensure_database_sync<W>(
    state: &SharedHost,
    world: Rc<W>,
    vset: VsetId,
    barrier: u64,
) -> Result<(), ()>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let (covered, acknowledged, database, peer_stashed) = {
        let host = state.borrow();
        let vset_state = host.vsets.get(&vset).ok_or(())?;
        (
            vset_state.local_covered_through,
            vset_state.sync_ack_through,
            vset_state.database,
            vset_state.config.durability.requires_peer_sync(),
        )
    };
    if acknowledged < barrier && covered < barrier {
        persist_database(
            state,
            world.as_ref(),
            vset,
            Vec::new(),
            None,
            database,
            false,
            Some(barrier),
        )
        .await?;
    }
    if peer_stashed
        && state
            .borrow()
            .vsets
            .get(&vset)
            .is_some_and(|vset_state| vset_state.sync_ack_through < barrier)
    {
        replicate_latest(Rc::clone(state), world, vset).await;
    }
    state
        .borrow()
        .vsets
        .get(&vset)
        .is_some_and(|vset_state| vset_state.sync_ack_through >= barrier)
        .then_some(())
        .ok_or(())
}

async fn write<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    req: ReqId,
    handle: u64,
    offset: u64,
    bytes: Vec<u8>,
) -> DatabaseReply
where
    W: Blobs + Store + Peers + AdminIo,
{
    let (file, old_size, incarnation, database) = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        let Some(&file) = vset_state.database_runtime.handles.get(&handle) else {
            return failed(req, DatabaseError::InvalidHandle);
        };
        let meta = file_meta(vset_state.database, file);
        if !meta.exists {
            return failed(req, DatabaseError::NotFound);
        }
        (file, meta.size, vset_state.incarnation, vset_state.database)
    };
    if bytes.is_empty() {
        return DatabaseReply::Written {
            req,
            sequence: state.borrow().vsets[&vset].mutation_seq,
        };
    }
    let mut updates = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let absolute = offset + u64::try_from(cursor).expect("bounded request");
        let page_number = u32::try_from(absolute / page_size() as u64).expect("bounded request");
        let in_page = usize::try_from(absolute % page_size() as u64).expect("page offset");
        let take = (bytes.len() - cursor).min(page_size() - in_page);
        let page = file.page(vset, page_number);
        let Some(mut page_bytes) = load_page_for_database(state, world, page, incarnation).await
        else {
            return failed(req, DatabaseError::Io);
        };
        page_bytes[in_page..in_page + take].copy_from_slice(&bytes[cursor..cursor + take]);
        updates.push((page, page_bytes));
        cursor += take;
        if updates.len() % 64 == 0 {
            yield_now().await;
        }
    }
    let mut database = database;
    file_meta_mut(&mut database, file).size = old_size.max(
        offset
            .checked_add(u64::try_from(bytes.len()).expect("bounded request"))
            .expect("validated request"),
    );
    match persist_database(state, world, vset, updates, None, database, true, None).await {
        Ok(sequence) => DatabaseReply::Written { req, sequence },
        Err(()) => failed(req, DatabaseError::Io),
    }
}

async fn truncate<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    req: ReqId,
    handle: u64,
    size: u64,
) -> DatabaseReply
where
    W: Blobs + Store + Peers + AdminIo,
{
    let (file, old_size, incarnation, database) = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        let Some(&file) = vset_state.database_runtime.handles.get(&handle) else {
            return failed(req, DatabaseError::InvalidHandle);
        };
        let meta = file_meta(vset_state.database, file);
        if !meta.exists {
            return failed(req, DatabaseError::NotFound);
        }
        (file, meta.size, vset_state.incarnation, vset_state.database)
    };
    let mut updates = Vec::new();
    if size < old_size && !size.is_multiple_of(page_size() as u64) {
        let page_number = u32::try_from(size / page_size() as u64).expect("bounded request");
        let page = file.page(vset, page_number);
        let Some(mut bytes) = load_page_for_database(state, world, page, incarnation).await else {
            return failed(req, DatabaseError::Io);
        };
        bytes[usize::try_from(size % page_size() as u64).expect("page offset")..].fill(0);
        updates.push((page, bytes));
    }
    let first_removed = size.div_ceil(page_size() as u64);
    if size < old_size
        && hydrate_prune_spans(state, world, vset, file, first_removed, incarnation)
            .await
            .is_err()
    {
        return failed(req, DatabaseError::Io);
    }
    let mut database = database;
    file_meta_mut(&mut database, file).size = size;
    let prune = (size < old_size).then_some((file, first_removed));
    match persist_database(state, world, vset, updates, prune, database, true, None).await {
        Ok(sequence) => DatabaseReply::Truncated { req, sequence },
        Err(()) => failed(req, DatabaseError::Io),
    }
}

async fn delete<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    req: ReqId,
    file: DatabaseFile,
) -> DatabaseReply
where
    W: Blobs + Store + Peers + AdminIo,
{
    let (incarnation, mut database) = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        (vset_state.incarnation, vset_state.database)
    };
    if hydrate_prune_spans(state, world, vset, file, 0, incarnation)
        .await
        .is_err()
    {
        return failed(req, DatabaseError::Io);
    }
    *file_meta_mut(&mut database, file) = DatabaseFileMeta::default();
    match persist_database(
        state,
        world,
        vset,
        Vec::new(),
        Some((file, 0)),
        database,
        true,
        None,
    )
    .await
    {
        Ok(sequence) => DatabaseReply::Deleted { req, sequence },
        Err(()) => failed(req, DatabaseError::Io),
    }
}

fn bounded_request(state: &SharedHost, vset: VsetId, op: &DatabaseOp) -> bool {
    let max = u64::from(state.borrow().vsets[&vset].config.pages_per_volume)
        * u64::try_from(page_size()).expect("page size fits u64");
    let range = |offset: u64, len: usize| {
        len <= MAX_DATABASE_IO
            && offset
                .checked_add(u64::try_from(len).expect("request size fits u64"))
                .is_some_and(|end| end <= max)
    };
    match op {
        DatabaseOp::Read { offset, len, .. } => usize::try_from(*len)
            .ok()
            .is_some_and(|len| range(*offset, len)),
        DatabaseOp::Write { offset, bytes, .. } => range(*offset, bytes.len()),
        DatabaseOp::Truncate { size, .. } => *size <= max,
        _ => true,
    }
}

async fn hydrate_prune_spans<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    file: DatabaseFile,
    first_removed: u64,
    incarnation: u64,
) -> Result<(), ()>
where
    W: Blobs + Store,
{
    let idx = file.volume_index();
    let first_page = u32::try_from(first_removed).unwrap_or(u32::MAX);
    let first_span = span_of(file.page(VsetId(0), first_page));
    let last_span = {
        let host = state.borrow();
        let last_page = host.vsets[&vset].config.pages_per_volume.saturating_sub(1);
        span_of(file.page(VsetId(0), last_page))
    };
    let spans = state.borrow().vsets[&vset]
        .leaf_table
        .keys()
        .copied()
        .filter(|span| *span >= first_span && *span <= last_span)
        .collect::<Vec<_>>();
    for (index, span) in spans.into_iter().enumerate() {
        let key = u64::from(span) * LEAF_SPAN;
        let page = PageId {
            volume: crate::types::VolumeId { vset, idx },
            page: PageNo(u32::try_from(key & 0xffff_ffff).expect("page number")),
        };
        super::hydrate_mapping(state, world, page, incarnation).await?;
        if (index + 1) % 64 == 0 {
            yield_now().await;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn persist_database<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    updates: Vec<(PageId, Vec<u8>)>,
    prune: Option<(DatabaseFile, u64)>,
    database: crate::journal::DatabaseMeta,
    mutation: bool,
    sync_barrier: Option<u64>,
) -> Result<u64, ()>
where
    W: Blobs + AdminIo,
{
    let (incarnation, fence, seq, capture_seq, first_segment, generations) = {
        let mut host = state.borrow_mut();
        let vset_state = host.vsets.get_mut(&vset).ok_or(())?;
        if !vset_state.ready || vset_state.commit_running {
            return Err(());
        }
        vset_state.commit_running = true;
        if mutation {
            vset_state.mutation_seq = vset_state.mutation_seq.checked_add(1).ok_or(())?;
        }
        let capture_seq = vset_state.mutation_seq;
        let seq = JournalSeq(vset_state.next_seq);
        vset_state.next_seq = vset_state.next_seq.checked_add(1).ok_or(())?;
        let first_segment = SegId(vset_state.next_seg);
        let generations = updates
            .iter()
            .map(|_| {
                let generation = Gen(vset_state.next_gen);
                vset_state.next_gen = vset_state
                    .next_gen
                    .checked_add(1)
                    .expect("generation overflow");
                generation
            })
            .collect::<Vec<_>>();
        (
            vset_state.incarnation,
            vset_state.fence,
            seq,
            capture_seq,
            first_segment,
            generations,
        )
    };
    let mut builder = SegmentBatchBuilder::new(vset, fence, first_segment);
    for ((page, bytes), generation) in updates.iter().zip(&generations) {
        builder.add(*page, *generation, bytes);
    }
    let segments = builder.finish();
    for (segment, bytes, _) in &segments {
        let name = layout::segment_blob(vset, fence, *segment);
        if !state
            .borrow_mut()
            .try_reserve_blob(name.clone(), bytes.len() as u64)
        {
            return Err(());
        }
        if Blobs::write(world, name.clone(), bytes.clone())
            .await
            .is_err()
        {
            AdminIo::abort(world, "database segment write failed").await;
            return Err(());
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
    }
    let (record_overlay, record_leaves, leaf_writes, record) = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
            .ok_or(())?;
        vset_state.next_seg = vset_state
            .next_seg
            .checked_add(u64::try_from(segments.len()).expect("segment count fits u64"))
            .expect("segment id overflow");
        for (segment, bytes, entries) in &segments {
            for &(page, generation, location) in entries {
                vset_state.page_locs.insert(page, (generation, location));
                vset_state.overlay.insert(page, (generation, location));
            }
            vset_state
                .segment_blobs
                .push((fence, *segment, bytes.len() as u64));
        }
        if let Some((file, first_removed)) = prune {
            prune_file(vset_state, file, first_removed);
        }
        vset_state.database = database;
        let (overlay, leaves, leaf_writes) = shard_map(vset_state, vset);
        vset_state.overlay.clone_from(&overlay);
        vset_state.leaf_table.clone_from(&leaves);
        let covered = sync_barrier.map_or(vset_state.local_covered_through, |barrier| {
            vset_state.local_covered_through.max(barrier)
        });
        let record = JournalRecord {
            config: vset_state.config,
            seq,
            fence,
            kind: RecordKind::Commit,
            capture_seq,
            sync_covered_through: covered,
            database,
            overlay: overlay.clone(),
            leaves: leaves.clone(),
            migrated_from: vset_state.peer_source,
        };
        (overlay, leaves, leaf_writes, record)
    };
    for (pointer, bytes, segments) in &leaf_writes {
        let name = layout::leaf_blob(vset, pointer.fence, pointer.id);
        if !state
            .borrow_mut()
            .try_reserve_blob(name.clone(), bytes.len() as u64)
        {
            return Err(());
        }
        if Blobs::write(world, name.clone(), bytes.clone())
            .await
            .is_err()
        {
            AdminIo::abort(world, "database map-leaf write failed").await;
            return Err(());
        }
        let mut host = state.borrow_mut();
        host.record_blob(name, bytes.len() as u64);
        let vset_state = host.vsets.get_mut(&vset).ok_or(())?;
        vset_state
            .leaf_blobs
            .insert(*pointer, (bytes.len() as u64, segments.clone()));
        host.counters.leaf_rolls += 1;
    }
    if !write_record_copies(state, world, vset, &record).await {
        AdminIo::abort(world, "database journal write failed").await;
        return Err(());
    }
    {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
            .ok_or(())?;
        vset_state.overlay = record_overlay;
        vset_state.leaf_table = record_leaves;
        vset_state.best_record = Some(record.clone());
        vset_state.local_covered_through = record.sync_covered_through;
        if !vset_state.config.durability.requires_peer_sync() {
            vset_state.sync_ack_through =
                vset_state.sync_ack_through.max(record.sync_covered_through);
        }
        vset_state
            .record_writes
            .insert(seq, (fence, record.sync_covered_through));
        vset_state.commit_running = false;
        host.counters.pages_flushed += updates.len() as u64;
        host.counters.records_written += 1;
    }
    if cleanup_local(Rc::clone(state), world, vset, incarnation)
        .await
        .is_err()
    {
        AdminIo::abort(world, "database local reclaim failed").await;
        return Err(());
    }
    Ok(capture_seq)
}

fn prune_file(state: &mut super::state::VsetState, file: DatabaseFile, first_removed: u64) {
    let idx = file.volume_index();
    let removed = state
        .page_locs
        .keys()
        .copied()
        .filter(|page| page.volume.idx == idx && u64::from(page.page.0) >= first_removed)
        .collect::<Vec<_>>();
    let mut affected = removed
        .iter()
        .map(|page| span_of(*page))
        .collect::<BTreeSet<_>>();
    let first_page = u32::try_from(first_removed).unwrap_or(u32::MAX);
    let first_span = span_of(file.page(VsetId(0), first_page));
    let last_page = state.config.pages_per_volume.saturating_sub(1);
    let last_span = span_of(file.page(VsetId(0), last_page));
    affected.extend(
        state
            .leaf_table
            .keys()
            .copied()
            .filter(|span| *span >= first_span && *span <= last_span),
    );
    for page in removed {
        state.page_locs.remove(&page);
        state.overlay.remove(&page);
    }
    for span in affected {
        state.leaf_table.remove(&span);
        state.overlay.retain(|page, _| span_of(*page) != span);
        let retained = state
            .page_locs
            .iter()
            .filter(|(page, _)| span_of(**page) == span)
            .map(|(&page, &location)| (page, location))
            .collect::<Vec<_>>();
        state.overlay.extend(retained);
        state.hydrated_spans.insert(span);
        state.failed_spans.remove(&span);
    }
}

async fn read<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    req: ReqId,
    handle: u64,
    offset: u64,
    len: u32,
) -> DatabaseReply
where
    W: Blobs + Store + Peers,
{
    let len = len as usize;
    if len > MAX_DATABASE_IO {
        return failed(req, DatabaseError::TooLarge);
    }
    let Some((file, meta, incarnation)) = ({
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        vset_state
            .database_runtime
            .handles
            .get(&handle)
            .map(|&file| {
                (
                    file,
                    file_meta(vset_state.database, file),
                    vset_state.incarnation,
                )
            })
    }) else {
        return failed(req, DatabaseError::InvalidHandle);
    };
    if !meta.exists {
        return failed(req, DatabaseError::NotFound);
    }
    let size = meta.size;
    let eof = size.saturating_sub(offset) < len as u64;
    if offset >= size {
        return DatabaseReply::Read {
            req,
            bytes: Vec::new(),
            eof,
        };
    }
    let end = size.min(offset.saturating_add(len as u64));
    let mut cursor = offset;
    let mut output = Vec::with_capacity(usize::try_from(end - offset).expect("bounded request"));
    while cursor < end {
        let page_number = cursor / page_size() as u64;
        let in_page = usize::try_from(cursor % page_size() as u64).expect("page offset");
        let take = usize::try_from(end - cursor)
            .expect("bounded request")
            .min(page_size() - in_page);
        let page = file.page(vset, u32::try_from(page_number).expect("bounded request"));
        let Some(bytes) = load_page_for_database(state, world, page, incarnation).await else {
            return failed(req, DatabaseError::Io);
        };
        output.extend_from_slice(&bytes[in_page..in_page + take]);
        cursor += take as u64;
    }
    DatabaseReply::Read {
        req,
        bytes: output,
        eof,
    }
}

fn file_meta(database: crate::journal::DatabaseMeta, file: DatabaseFile) -> DatabaseFileMeta {
    match file {
        DatabaseFile::Main => database.main,
        DatabaseFile::Wal => database.wal,
        DatabaseFile::Journal => database.journal,
    }
}

fn file_meta_mut(
    database: &mut crate::journal::DatabaseMeta,
    file: DatabaseFile,
) -> &mut DatabaseFileMeta {
    match file {
        DatabaseFile::Main => &mut database.main,
        DatabaseFile::Wal => &mut database.wal,
        DatabaseFile::Journal => &mut database.journal,
    }
}

const fn failed(req: ReqId, error: DatabaseError) -> DatabaseReply {
    DatabaseReply::Failed { req, error }
}
