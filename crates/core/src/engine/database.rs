use std::collections::BTreeSet;
use std::rc::Rc;

use blockd_exec::channel::{Receiver, unbounded};
use blockd_exec::{Either, OneOf3, TaskSet, delay, select2, select3, yield_now};

use super::capture::{shard_map, write_record_copies};
use super::fault::load_page_for_database;
use super::hydration::HydrationError;
use super::keyed_queue::KeyedQueue;
use super::reclaim::cleanup_local;
use super::replica::replicate_latest;
use super::state::{AttachmentPhase, CommitFlagLease, MutationOwner, SharedHost};
use crate::database::{
    AttachmentId, DatabaseCall, DatabaseError, DatabaseFile, DatabaseOp, DatabaseResult,
    DatabaseSuccess, MAX_DATABASE_IO,
};
use crate::journal::{DatabaseFileMeta, JournalRecord, MigrationSource, RecordKind, VsetKind};
use crate::layout;
use crate::mapleaf::{LEAF_SPAN, span_of};
use crate::protocol::{AdminError, AdminResult, AdminSuccess, DetachMode};
use crate::segment::SegmentBatchBuilder;
use crate::types::{Gen, JournalSeq, PageId, PageNo, SegId, VsetId, page_size};
use crate::world::{AdminIo, Blobs, GuestMem, Peers, Store};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatabasePersistError {
    Stale,
    Busy,
    Overflow,
    Capacity,
    Fatal,
}

const DATABASE_CONCURRENCY: usize = 64;
const DATABASE_QUEUE_CAPACITY: usize = 64;
const DATABASE_INGRESS_BATCH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DetachedDatabaseDrain {
    pub(crate) vset: VsetId,
    pub(crate) attachment: AttachmentId,
}

enum DatabaseWork {
    Request(crate::world::DatabaseActorRequest),
    Drain(DetachedDatabaseDrain),
}

enum DatabaseSourceEvent {
    Completed(Option<VsetId>),
    Request(Option<crate::world::DatabaseActorRequest>),
    Drain(Option<DetachedDatabaseDrain>),
}

pub fn attach_database(state: &SharedHost, vset: VsetId, vm: crate::types::VmId) -> AdminResult {
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
    attachment.map_or(Err(AdminError::Rejected), |attachment| {
        Ok(AdminSuccess::DatabaseAttached { vset, attachment })
    })
}

pub fn begin_detach_database(
    state: &SharedHost,
    vset: VsetId,
    attachment: AttachmentId,
    mode: DetachMode,
) -> (AdminResult, bool) {
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
    let response = if started {
        Ok(AdminSuccess::DatabaseDetachStarted {
            vset,
            attachment,
            forced: mode == DetachMode::Forced,
        })
    } else {
        Err(AdminError::Stale)
    };
    (response, started && mode != DetachMode::Forced)
}

pub async fn drain_detached_database<W>(
    state: SharedHost,
    world: Rc<W>,
    vset: VsetId,
    attachment: AttachmentId,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let barrier = {
        let mut host = state.borrow_mut();
        let Some(vset_state) = host.vsets.get_mut(&vset) else {
            return;
        };
        if vset_state.database_runtime.phase != AttachmentPhase::Draining(attachment)
            || vset_state.database_runtime.active.is_some()
        {
            return;
        }
        vset_state.database_runtime.drain_barrier = vset_state.mutation_seq;
        vset_state.mutation_seq
    };
    let _ = ensure_database_sync(&state, world, vset, barrier).await;
}

pub fn finish_detach_database(
    state: &SharedHost,
    vset: VsetId,
    attachment: AttachmentId,
) -> AdminResult {
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
    if detached {
        Ok(AdminSuccess::DatabaseDetached { vset, attachment })
    } else {
        Err(AdminError::Stale)
    }
}

pub(crate) async fn database_source<W>(
    state: SharedHost,
    world: Rc<W>,
    mut drains: Receiver<DetachedDatabaseDrain>,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut requests = KeyedQueue::new();
    let mut ingress_open = true;
    let mut drains_open = true;
    let mut external_pending = 0usize;
    let mut ingress_batch = 0;
    loop {
        while let Some((vset, work)) = requests.start_next(DATABASE_CONCURRENCY) {
            let external = matches!(work, DatabaseWork::Request(_));
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            let completed = completed.clone();
            actors.spawn(async move {
                match work {
                    DatabaseWork::Request(request) => {
                        handle_database_request(state, world, request).await;
                    }
                    DatabaseWork::Drain(drain) => {
                        drain_detached_database(state, world, drain.vset, drain.attachment).await;
                    }
                }
                let _ = completed.send(vset);
            });
            if external {
                external_pending = external_pending
                    .checked_sub(1)
                    .expect("queued database request started");
            }
        }
        if !ingress_open && !drains_open && requests.is_idle() {
            return;
        }
        let event = match (ingress_open, drains_open) {
            (true, true) => match select3(
                completions.recv(),
                AdminIo::next_database(world.as_ref()),
                drains.recv(),
            )
            .await
            {
                OneOf3::First(value) => DatabaseSourceEvent::Completed(value),
                OneOf3::Second(value) => DatabaseSourceEvent::Request(value),
                OneOf3::Third(value) => DatabaseSourceEvent::Drain(value),
            },
            (true, false) => {
                match select2(completions.recv(), AdminIo::next_database(world.as_ref())).await {
                    Either::First(value) => DatabaseSourceEvent::Completed(value),
                    Either::Second(value) => DatabaseSourceEvent::Request(value),
                }
            }
            (false, true) => match select2(completions.recv(), drains.recv()).await {
                Either::First(value) => DatabaseSourceEvent::Completed(value),
                Either::Second(value) => DatabaseSourceEvent::Drain(value),
            },
            (false, false) => DatabaseSourceEvent::Completed(completions.recv().await),
        };
        match event {
            DatabaseSourceEvent::Completed(Some(vset)) => requests.complete(vset),
            DatabaseSourceEvent::Completed(None) => return,
            DatabaseSourceEvent::Request(Some(request)) => {
                if external_pending >= DATABASE_QUEUE_CAPACITY {
                    let (_, mut reply) = request.into_parts();
                    let _ = reply.send(Err(DatabaseError::Busy));
                } else {
                    requests.push(request.body.vset, DatabaseWork::Request(request));
                    external_pending += 1;
                }
                ingress_batch += 1;
                if ingress_batch == DATABASE_INGRESS_BATCH {
                    ingress_batch = 0;
                    yield_now().await;
                }
            }
            DatabaseSourceEvent::Request(None) => ingress_open = false,
            DatabaseSourceEvent::Drain(Some(drain)) => {
                requests.push(drain.vset, DatabaseWork::Drain(drain));
            }
            DatabaseSourceEvent::Drain(None) => drains_open = false,
        }
    }
}

async fn handle_database_request<W>(
    state: SharedHost,
    world: Rc<W>,
    request: crate::world::DatabaseActorRequest,
) where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let (request, mut reply_target) = request.into_parts();
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
    let result = database_request(&state, Rc::clone(&world), request).await;
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
    let result = if retired {
        Err(DatabaseError::StaleAttachment)
    } else {
        result
    };
    let _ = reply_target.send(result);
}

#[allow(clippy::too_many_lines)]
async fn database_request<W>(
    state: &SharedHost,
    world: Rc<W>,
    request: DatabaseCall,
) -> DatabaseResult
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let phase = state
        .borrow()
        .vsets
        .get(&request.vset)
        .filter(|vset| vset.ready && vset.config.kind == VsetKind::Database)
        .map(|vset| vset.database_runtime.phase);
    let Some(phase) = phase else {
        return Err(DatabaseError::NotAttached);
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
        return Err(DatabaseError::StaleAttachment);
    }
    if matches!(phase, AttachmentPhase::Draining(_))
        && !matches!(
            request.op,
            DatabaseOp::Close { .. } | DatabaseOp::Sync { .. }
        )
    {
        return Err(DatabaseError::Draining);
    }
    if !bounded_request(state, request.vset, &request.op) {
        return Err(DatabaseError::TooLarge);
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
                handle,
                file,
                create,
                request.attachment,
            )
            .await
        }
        DatabaseOp::Close { handle } => close(state, request.vset, handle),
        DatabaseOp::Read {
            handle,
            offset,
            len,
        } => read(state, world.as_ref(), request.vset, handle, offset, len).await,
        DatabaseOp::FileSize { handle } => file_size(state, request.vset, handle),
        DatabaseOp::Access { file } => {
            let exists = file_meta(state.borrow().vsets[&request.vset].database, file).exists;
            Ok(DatabaseSuccess::Access { exists })
        }
        DatabaseOp::Stat { file } => {
            let meta = file_meta(state.borrow().vsets[&request.vset].database, file);
            Ok(DatabaseSuccess::Stat {
                exists: meta.exists,
                size: meta.size,
            })
        }
        DatabaseOp::Sync { handle } => {
            sync(state, world, request.vset, handle, request.attachment).await
        }
        DatabaseOp::Write {
            handle,
            offset,
            bytes,
        } => write(state, world.as_ref(), request.vset, handle, offset, bytes).await,
        DatabaseOp::Truncate { handle, size } => {
            truncate(state, world.as_ref(), request.vset, handle, size).await
        }
        DatabaseOp::Delete { file } => delete(state, world.as_ref(), request.vset, file).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn open<W: Blobs + AdminIo>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    handle: u64,
    file: DatabaseFile,
    create: bool,
    attachment: AttachmentId,
) -> DatabaseResult {
    let (exists, database) = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        if vset_state.database_runtime.handles.contains_key(&handle) {
            return Err(DatabaseError::AlreadyOpen);
        }
        let meta = file_meta(vset_state.database, file);
        if !meta.exists && !create {
            return Err(DatabaseError::NotFound);
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
        return Err(DatabaseError::Io);
    }
    let mut host = state.borrow_mut();
    let Some(vset_state) = host.vsets.get_mut(&vset) else {
        return Err(DatabaseError::StaleAttachment);
    };
    if !matches!(
        vset_state.database_runtime.phase,
        AttachmentPhase::Attached(id) | AttachmentPhase::Draining(id) if id == attachment
    ) {
        return Err(DatabaseError::StaleAttachment);
    }
    vset_state.database_runtime.handles.insert(handle, file);
    Ok(DatabaseSuccess::Opened)
}

fn close(state: &SharedHost, vset: VsetId, handle: u64) -> DatabaseResult {
    let removed = state
        .borrow_mut()
        .vsets
        .get_mut(&vset)
        .and_then(|vset| vset.database_runtime.handles.remove(&handle));
    removed.map_or_else(
        || Err(DatabaseError::InvalidHandle),
        |_| Ok(DatabaseSuccess::Closed),
    )
}

fn file_size(state: &SharedHost, vset: VsetId, handle: u64) -> DatabaseResult {
    let host = state.borrow();
    let vset_state = &host.vsets[&vset];
    let Some(&file) = vset_state.database_runtime.handles.get(&handle) else {
        return Err(DatabaseError::InvalidHandle);
    };
    Ok(DatabaseSuccess::FileSize {
        size: file_meta(vset_state.database, file).size,
    })
}

async fn sync<W>(
    state: &SharedHost,
    world: Rc<W>,
    vset: VsetId,
    handle: u64,
    attachment: AttachmentId,
) -> DatabaseResult
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let barrier = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        if !vset_state.database_runtime.handles.contains_key(&handle) {
            return Err(DatabaseError::InvalidHandle);
        }
        vset_state.mutation_seq
    };
    if ensure_database_sync(state, Rc::clone(&world), vset, barrier)
        .await
        .is_err()
    {
        return Err(DatabaseError::Io);
    }
    let host = state.borrow();
    let Some(vset_state) = host.vsets.get(&vset) else {
        return Err(DatabaseError::StaleAttachment);
    };
    if !matches!(
        vset_state.database_runtime.phase,
        AttachmentPhase::Attached(id) | AttachmentPhase::Draining(id) if id == attachment
    ) {
        return Err(DatabaseError::StaleAttachment);
    }
    if vset_state.sync_ack_through < barrier {
        return Err(DatabaseError::Io);
    }
    Ok(DatabaseSuccess::Synced { sequence: barrier })
}

async fn ensure_database_sync<W>(
    state: &SharedHost,
    world: Rc<W>,
    vset: VsetId,
    barrier: u64,
) -> Result<(), DatabaseError>
where
    W: Blobs + Store + Peers + GuestMem + AdminIo + 'static,
{
    let (covered, acknowledged, database) = {
        let host = state.borrow();
        let vset_state = host
            .vsets
            .get(&vset)
            .ok_or(DatabaseError::StaleAttachment)?;
        (
            vset_state.local_covered_through,
            vset_state.sync_ack_through,
            vset_state.database,
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
        .await
        .map_err(|_| DatabaseError::Io)?;
    }
    loop {
        let retry = {
            let host = state.borrow();
            let vset_state = host
                .vsets
                .get(&vset)
                .ok_or(DatabaseError::StaleAttachment)?;
            if vset_state.sync_ack_through >= barrier {
                return Ok(());
            }
            host.config.backup_retry.max(1)
        };
        replicate_latest(Rc::clone(state), Rc::clone(&world), vset).await;
        if state
            .borrow()
            .vsets
            .get(&vset)
            .is_some_and(|vset_state| vset_state.sync_ack_through >= barrier)
        {
            return Ok(());
        }
        delay(retry).await;
    }
}

async fn write<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    handle: u64,
    offset: u64,
    bytes: Vec<u8>,
) -> DatabaseResult
where
    W: Blobs + Store + Peers + AdminIo,
{
    let (file, old_size, incarnation, database) = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        let Some(&file) = vset_state.database_runtime.handles.get(&handle) else {
            return Err(DatabaseError::InvalidHandle);
        };
        let meta = file_meta(vset_state.database, file);
        if !meta.exists {
            return Err(DatabaseError::NotFound);
        }
        (file, meta.size, vset_state.incarnation, vset_state.database)
    };
    if bytes.is_empty() {
        return Ok(DatabaseSuccess::Written {
            sequence: state.borrow().vsets[&vset].mutation_seq,
        });
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
            return Err(DatabaseError::Io);
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
        Ok(sequence) => Ok(DatabaseSuccess::Written { sequence }),
        Err(_) => Err(DatabaseError::Io),
    }
}

async fn truncate<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    handle: u64,
    size: u64,
) -> DatabaseResult
where
    W: Blobs + Store + Peers + AdminIo,
{
    let (file, old_size, incarnation, database) = {
        let host = state.borrow();
        let vset_state = &host.vsets[&vset];
        let Some(&file) = vset_state.database_runtime.handles.get(&handle) else {
            return Err(DatabaseError::InvalidHandle);
        };
        let meta = file_meta(vset_state.database, file);
        if !meta.exists {
            return Err(DatabaseError::NotFound);
        }
        (file, meta.size, vset_state.incarnation, vset_state.database)
    };
    let mut updates = Vec::new();
    if size < old_size && !size.is_multiple_of(page_size() as u64) {
        let page_number = u32::try_from(size / page_size() as u64).expect("bounded request");
        let page = file.page(vset, page_number);
        let Some(mut bytes) = load_page_for_database(state, world, page, incarnation).await else {
            return Err(DatabaseError::Io);
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
        return Err(DatabaseError::Io);
    }
    let mut database = database;
    file_meta_mut(&mut database, file).size = size;
    let prune = (size < old_size).then_some((file, first_removed));
    match persist_database(state, world, vset, updates, prune, database, true, None).await {
        Ok(sequence) => Ok(DatabaseSuccess::Truncated { sequence }),
        Err(_) => Err(DatabaseError::Io),
    }
}

async fn delete<W>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    file: DatabaseFile,
) -> DatabaseResult
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
        return Err(DatabaseError::Io);
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
        Ok(sequence) => Ok(DatabaseSuccess::Deleted { sequence }),
        Err(_) => Err(DatabaseError::Io),
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
) -> Result<(), HydrationError>
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
) -> Result<u64, DatabasePersistError>
where
    W: Blobs + AdminIo,
{
    let (
        incarnation,
        config,
        fence,
        seq,
        capture_seq,
        first_segment,
        generations,
        mut staged,
        covered,
        peer_source,
        peer_source_offer_fence,
        sequence_after,
        generation_after,
    ) = {
        let host = state.borrow();
        let vset_state = host.vsets.get(&vset).ok_or(DatabasePersistError::Stale)?;
        if !vset_state.ready {
            return Err(DatabasePersistError::Stale);
        }
        if vset_state.operations.mutation_blocked() {
            return Err(DatabasePersistError::Busy);
        }
        let capture_seq = if mutation {
            vset_state
                .mutation_seq
                .checked_add(1)
                .ok_or(DatabasePersistError::Overflow)?
        } else {
            vset_state.mutation_seq
        };
        let seq = JournalSeq(vset_state.next_seq);
        let sequence_after = vset_state
            .next_seq
            .checked_add(1)
            .ok_or(DatabasePersistError::Overflow)?;
        let first_segment = SegId(vset_state.next_seg);
        let generation_count = u64::try_from(updates.len()).expect("update count fits u64");
        let generation_after = vset_state
            .next_gen
            .checked_add(generation_count)
            .ok_or(DatabasePersistError::Overflow)?;
        let generations = (0..updates.len())
            .map(|offset| {
                let offset = u64::try_from(offset).expect("update count fits u64");
                vset_state
                    .next_gen
                    .checked_add(offset)
                    .map(Gen)
                    .ok_or(DatabasePersistError::Overflow)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut staged = super::state::VsetState::fresh(vset_state.config, vset_state.incarnation);
        staged.fence = vset_state.fence;
        staged.page_locs = vset_state.page_locs.clone();
        staged.overlay = vset_state.overlay.clone();
        staged.leaf_table = vset_state.leaf_table.clone();
        staged.next_leaf = vset_state.next_leaf;
        let covered = sync_barrier.map_or(vset_state.local_covered_through, |barrier| {
            vset_state.local_covered_through.max(barrier)
        });
        (
            vset_state.incarnation,
            vset_state.config,
            vset_state.fence,
            seq,
            capture_seq,
            first_segment,
            generations,
            staged,
            covered,
            vset_state.peer_source,
            vset_state.peer_source_offer_fence,
            sequence_after,
            generation_after,
        )
    };
    let mut builder = SegmentBatchBuilder::new(vset, fence, first_segment);
    for ((page, bytes), generation) in updates.iter().zip(&generations) {
        builder
            .try_add(*page, *generation, bytes)
            .map_err(|_| DatabasePersistError::Overflow)?;
    }
    let segments = builder.finish();
    let segment_after = first_segment
        .0
        .checked_add(u64::try_from(segments.len()).expect("segment count fits u64"))
        .ok_or(DatabasePersistError::Overflow)?;
    for (_, _, entries) in &segments {
        for &(page, generation, location) in entries {
            staged.page_locs.insert(page, (generation, location));
            staged.overlay.insert(page, (generation, location));
        }
    }
    if let Some((file, first_removed)) = prune {
        prune_file(&mut staged, file, first_removed);
    }
    let (record_overlay, record_leaves, leaf_writes) = shard_map(&mut staged, vset);
    let record = JournalRecord {
        config,
        seq,
        fence,
        kind: RecordKind::Commit,
        capture_seq,
        sync_covered_through: covered,
        database,
        overlay: record_overlay.clone(),
        leaves: record_leaves.clone(),
        migrated_from: peer_source.map(|host| MigrationSource {
            host,
            offer_fence: peer_source_offer_fence,
        }),
    };
    let reservations = segments
        .iter()
        .map(|(segment, bytes, _)| {
            (
                layout::segment_blob(vset, fence, *segment),
                bytes.len() as u64,
            )
        })
        .chain(leaf_writes.iter().map(|(pointer, bytes, _)| {
            (
                layout::leaf_blob(vset, pointer.fence, pointer.id),
                bytes.len() as u64,
            )
        }))
        .collect::<Vec<_>>();
    {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
            .ok_or(DatabasePersistError::Stale)?;
        if vset_state.operations.mutation_blocked() {
            return Err(DatabasePersistError::Busy);
        }
        assert!(
            vset_state
                .operations
                .try_start_mutation(MutationOwner::Database)
        );
    }
    let lease = CommitFlagLease::new(state, vset, incarnation, MutationOwner::Database);
    if !state.borrow_mut().try_reserve_blobs(&reservations) {
        return Err(DatabasePersistError::Capacity);
    }
    for (segment, bytes, _) in &segments {
        let name = layout::segment_blob(vset, fence, *segment);
        if Blobs::write(world, name.clone(), bytes.clone())
            .await
            .is_err()
        {
            state.borrow_mut().fail("database segment write failed");
            return Err(DatabasePersistError::Fatal);
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
    }
    let mut new_leaf_blobs = Vec::new();
    for (pointer, bytes, segments) in &leaf_writes {
        let name = layout::leaf_blob(vset, pointer.fence, pointer.id);
        if Blobs::write(world, name.clone(), bytes.clone())
            .await
            .is_err()
        {
            state.borrow_mut().fail("database map-leaf write failed");
            return Err(DatabasePersistError::Fatal);
        }
        state.borrow_mut().record_blob(name, bytes.len() as u64);
        new_leaf_blobs.push((*pointer, (bytes.len() as u64, segments.clone())));
    }
    if !write_record_copies(state, world, vset, &record).await {
        state.borrow_mut().fail("database journal write failed");
        return Err(DatabasePersistError::Fatal);
    }
    let mutation_waiters = {
        let mut host = state.borrow_mut();
        let vset_state = host
            .vsets
            .get_mut(&vset)
            .filter(|vset_state| vset_state.incarnation == incarnation)
            .ok_or(DatabasePersistError::Stale)?;
        vset_state.mutation_seq = capture_seq;
        vset_state.next_seq = sequence_after;
        vset_state.next_seg = segment_after;
        vset_state.next_gen = generation_after;
        vset_state.next_leaf = staged.next_leaf;
        vset_state.page_locs = staged.page_locs;
        vset_state.database = database;
        vset_state.segment_blobs.extend(
            segments
                .iter()
                .map(|(segment, bytes, _)| (fence, *segment, bytes.len() as u64)),
        );
        vset_state.leaf_blobs.extend(new_leaf_blobs);
        vset_state.overlay = record_overlay;
        vset_state.leaf_table = record_leaves;
        vset_state.best_record = Some(record.clone());
        vset_state.local_covered_through = record.sync_covered_through;
        vset_state
            .record_writes
            .insert(seq, (fence, record.sync_covered_through));
        vset_state
            .operations
            .finish_mutation(MutationOwner::Database);
        let mutation_waiters = std::mem::take(&mut vset_state.mutation_waiters);
        host.counters.pages_flushed += updates.len() as u64;
        host.counters.records_written += 1;
        host.counters.leaf_rolls += leaf_writes.len() as u64;
        mutation_waiters
    };
    lease.commit();
    for waiter in mutation_waiters {
        let _ = waiter.send(());
    }
    if cleanup_local(Rc::clone(state), world, vset, incarnation)
        .await
        .is_err()
    {
        state.borrow_mut().fail("database local reclaim failed");
        return Err(DatabasePersistError::Fatal);
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
    handle: u64,
    offset: u64,
    len: u32,
) -> DatabaseResult
where
    W: Blobs + Store + Peers,
{
    let len = len as usize;
    if len > MAX_DATABASE_IO {
        return Err(DatabaseError::TooLarge);
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
        return Err(DatabaseError::InvalidHandle);
    };
    if !meta.exists {
        return Err(DatabaseError::NotFound);
    }
    let size = meta.size;
    let eof = size.saturating_sub(offset) < len as u64;
    if offset >= size {
        return Ok(DatabaseSuccess::Read {
            bytes: Vec::new(),
            eof,
        });
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
            return Err(DatabaseError::Io);
        };
        output.extend_from_slice(&bytes[in_page..in_page + take]);
        cursor += take as u64;
    }
    Ok(DatabaseSuccess::Read { bytes: output, eof })
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

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};
    use std::rc::Rc;

    use blockd_exec::{Executor, bridge_request, spawn};

    use super::*;
    use crate::hostmeta::HostConfig;
    use crate::journal::{DatabaseMeta, VsetConfig};
    use crate::protocol::{AdminCall, AdminEvent, AdminResult};
    use crate::types::{HostId, VolumeId, VolumeIdx};
    use crate::world::{BlobEntry, BlobError};

    #[derive(Default)]
    struct TestWorld {
        blobs: RefCell<BTreeMap<String, Vec<u8>>>,
        replies: Rc<RefCell<Vec<AdminResult>>>,
        events: RefCell<Vec<AdminEvent>>,
        admin: RefCell<VecDeque<AdminCall>>,
    }

    impl Blobs for TestWorld {
        async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
            Ok(Vec::new())
        }
        async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            self.blobs.borrow_mut().insert(name, bytes);
            Ok(())
        }
        async fn append(&self, _: String, _: Vec<u8>) -> Result<(), BlobError> {
            unreachable!()
        }
        async fn truncate(&self, _: &str, _: u64) -> Result<(), BlobError> {
            unreachable!()
        }
        async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.blobs.borrow().get(name).cloned())
        }
        async fn read_range(&self, _: &str, _: u64, _: u64) -> Result<Option<Vec<u8>>, BlobError> {
            unreachable!()
        }
        async fn delete(&self, name: &str) -> Result<(), BlobError> {
            self.blobs.borrow_mut().remove(name);
            Ok(())
        }
    }

    impl AdminIo for TestWorld {
        async fn next_admin(&self) -> Option<crate::world::AdminRequest> {
            let command = self.admin.borrow_mut().pop_front()?;
            let (request, receive) = bridge_request(command);
            let replies = Rc::clone(&self.replies);
            spawn(async move {
                if let Ok(result) = receive.await {
                    replies.borrow_mut().push(result);
                }
            })
            .detach();
            Some(request)
        }

        async fn emit_admin_event(&self, event: AdminEvent) {
            self.events.borrow_mut().push(event);
        }
        async fn next_database(&self) -> Option<crate::world::DatabaseActorRequest> {
            None
        }
        async fn host_failed(&self, failure: crate::engine::HostFatal) {
            panic!("unexpected host failure: {}", failure.reason)
        }
    }

    #[test]
    fn failed_capacity_reservation_releases_database_commit_state_for_retry() {
        let vset = VsetId(1);
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(0),
        };
        let mut host = super::super::state::HostState::new(HostConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: Some(1),
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        });
        let incarnation = host.insert_fresh(vset, VsetConfig::database(4));
        host.vsets.get_mut(&vset).expect("vset").ready = true;
        let state = Rc::new(RefCell::new(host));
        let world = Rc::new(TestWorld::default());
        let mut executor = Executor::simulation(1);
        let failed = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                persist_database(
                    &state,
                    world.as_ref(),
                    vset,
                    vec![(page, vec![1; page_size()])],
                    None,
                    DatabaseMeta::default(),
                    true,
                    None,
                )
                .await
            }
        });
        assert_eq!(failed, Err(DatabasePersistError::Capacity));
        {
            let host = state.borrow();
            let vset_state = &host.vsets[&vset];
            assert_eq!(vset_state.incarnation, incarnation);
            assert!(vset_state.operations.mutation_owner().is_none());
            assert_eq!(vset_state.mutation_seq, 0);
            assert_eq!(vset_state.next_seq, 0);
            assert!(vset_state.page_locs.is_empty());
        }
        state.borrow_mut().config.disk_capacity = None;
        let retried = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                persist_database(
                    &state,
                    world.as_ref(),
                    vset,
                    vec![(page, vec![1; page_size()])],
                    None,
                    DatabaseMeta::default(),
                    true,
                    None,
                )
                .await
            }
        });
        assert_eq!(retried, Ok(1));
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn counter_overflow_does_not_claim_database_mutation_ownership() {
        let vset = VsetId(1);
        let pages = [0, 1].map(|page| PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(0),
            },
            page: PageNo(page),
        });
        let mut host = super::super::state::HostState::new(HostConfig {
            archive: Default::default(),
            host: HostId(1),
            cache_pages: 1,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            replica_placement: None,
        });
        host.insert_fresh(vset, VsetConfig::database(4));
        let vset_state = host.vsets.get_mut(&vset).expect("vset");
        vset_state.ready = true;
        vset_state.mutation_seq = u64::MAX;
        let state = Rc::new(RefCell::new(host));
        let world = Rc::new(TestWorld::default());
        let mut executor = Executor::simulation(2);

        let mutation_overflow = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                persist_database(
                    &state,
                    world.as_ref(),
                    vset,
                    Vec::new(),
                    None,
                    DatabaseMeta::default(),
                    true,
                    None,
                )
                .await
            }
        });
        assert_eq!(mutation_overflow, Err(DatabasePersistError::Overflow));
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );

        {
            let mut host = state.borrow_mut();
            let vset_state = host.vsets.get_mut(&vset).expect("vset");
            vset_state.mutation_seq = 0;
            vset_state.next_seq = u64::MAX;
        }
        let sequence_overflow = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                persist_database(
                    &state,
                    world.as_ref(),
                    vset,
                    Vec::new(),
                    None,
                    DatabaseMeta::default(),
                    true,
                    None,
                )
                .await
            }
        });
        assert_eq!(sequence_overflow, Err(DatabasePersistError::Overflow));
        assert!(world.blobs.borrow().is_empty());
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );

        {
            let mut host = state.borrow_mut();
            let vset_state = host.vsets.get_mut(&vset).expect("vset");
            vset_state.next_seq = 0;
            vset_state.next_gen = u64::MAX;
        }
        let generation_overflow = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                persist_database(
                    &state,
                    world.as_ref(),
                    vset,
                    pages.map(|page| (page, vec![1; page_size()])).to_vec(),
                    None,
                    DatabaseMeta::default(),
                    true,
                    None,
                )
                .await
            }
        });
        assert_eq!(generation_overflow, Err(DatabasePersistError::Overflow));
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );

        {
            let mut host = state.borrow_mut();
            let vset_state = host.vsets.get_mut(&vset).expect("vset");
            vset_state.next_gen = 0;
            vset_state.next_seg = u64::MAX;
        }
        let segment_overflow = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                persist_database(
                    &state,
                    world.as_ref(),
                    vset,
                    vec![(pages[0], vec![1; page_size()])],
                    None,
                    DatabaseMeta::default(),
                    true,
                    None,
                )
                .await
            }
        });
        assert_eq!(segment_overflow, Err(DatabasePersistError::Overflow));
        assert!(world.blobs.borrow().is_empty());
        assert!(
            state.borrow().vsets[&vset]
                .operations
                .mutation_owner()
                .is_none()
        );

        state
            .borrow_mut()
            .vsets
            .get_mut(&vset)
            .expect("vset")
            .next_seg = 0;
        let retried = executor.block_on({
            let state = Rc::clone(&state);
            let world = Rc::clone(&world);
            async move {
                persist_database(
                    &state,
                    world.as_ref(),
                    vset,
                    vec![(pages[0], vec![1; page_size()])],
                    None,
                    DatabaseMeta::default(),
                    true,
                    None,
                )
                .await
            }
        });
        assert_eq!(retried, Ok(1));
    }
}
