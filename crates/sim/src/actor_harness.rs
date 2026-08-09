//! Single-host deterministic runs over the async actor core.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::rc::Rc;

use blockd_core::engine::{HostState, host_actor_with_state};
use blockd_core::hostmeta::{Counters, HostConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::protocol::{AdminCmd, AdminReply, ReqId};
use blockd_core::types::{PageId, PageNo, VolumeId, VolumeIdx, VsetId, page_size};
use blockd_exec::rng::Ppm;
use blockd_exec::{Executor, TaskHandle, delay, now, random_u64, spawn};
use blockd_workload::{
    LogicalPage, Operation, Program, WorkloadModel, WorkloadOutcome, WorkloadSpec,
};

use crate::actor_world::{SimNetwork, SimWorld};
use crate::guest::page_pattern;
use crate::world::blobdev::BlobDevConfig;
use crate::world::store::StoreConfig;

#[derive(Clone, Debug)]
pub struct ActorHarnessConfig {
    pub host: HostConfig,
    pub blobs: BlobDevConfig,
    pub store: StoreConfig,
    pub vset_count: u16,
    pub vset: VsetConfig,
    pub horizon: u64,
    pub think: (u64, u64),
    pub sync_share: Option<Ppm>,
    pub hot_pages: Option<(Ppm, u32)>,
    pub checkpoint_interval: Option<u64>,
    pub faults: ActorFaultPlan,
    pub corrupt_fills: bool,
    pub drop_write_protect: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ActorFaultPlan {
    pub crash_mean_interval: u64,
    pub crash_at: Vec<u64>,
    pub restart_delay: (u64, u64),
    pub store_outage: Option<(u64, u64)>,
    pub bitflip_mean_interval: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActorRunReport {
    pub trace_hash: u64,
    pub executor_polls: u64,
    pub violations: Vec<String>,
    pub counters: Counters,
    pub completed_ops: u64,
    pub per_guest_completed: BTreeMap<u64, u64>,
    pub blob_count: usize,
    pub store_keys: Vec<String>,
    pub map_bytes_written: u64,
    pub max_page_reads_in_poll: u64,
    pub max_record_blob_bytes: u64,
    pub seg_bytes_end: u64,
    pub seg_live_bytes_end: u64,
    pub parked_end: usize,
    pub crashes: u64,
    pub resumes: u64,
    pub cold_boots: u64,
    pub unrestorable: u64,
    pub guest_deaths: u64,
    pub bitflips: u64,
}

#[derive(Default)]
struct GuestState {
    completed: Cell<u64>,
    expected: RefCell<BTreeMap<PageId, Vec<u8>>>,
    durable_expected: RefCell<BTreeMap<PageId, Vec<u8>>>,
    written_sequences: RefCell<BTreeMap<PageId, BTreeSet<u64>>>,
    recovering_pages: RefCell<BTreeSet<PageId>>,
    volume_sequences: RefCell<BTreeMap<VolumeId, u64>>,
    violations: RefCell<Vec<String>>,
}

#[derive(Default)]
struct RunEvents {
    crashes: Cell<u64>,
    resumes: Cell<u64>,
    cold_boots: Cell<u64>,
    unrestorable: Cell<u64>,
    guest_deaths: Cell<u64>,
}

type SharedHostState = Rc<RefCell<HostState>>;
type HostSlot = Rc<RefCell<Option<TaskHandle<()>>>>;
type StateSlot = Rc<RefCell<SharedHostState>>;
type GuestSlots = Rc<RefCell<BTreeMap<VsetId, Option<TaskHandle<()>>>>>;

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run(seed: u64, config: ActorHarnessConfig) -> ActorRunReport {
    run_final_blobs(seed, config).0
}

/// Drive one deliberately large dirty set through the actor capture pipeline.
///
/// This is the bare performance harness: all devices are the deterministic
/// model implementations and the report exposes executor polls plus the
/// maximum number of guest-page reads performed by any single poll.
#[allow(clippy::needless_pass_by_value)]
pub fn run_capture_profile(
    seed: u64,
    mut config: ActorHarnessConfig,
    dirty_pages: u32,
) -> ActorRunReport {
    assert!(dirty_pages != 0, "capture profile needs dirty pages");
    config.vset_count = 1;
    config.vset = VsetConfig::compute(1, dirty_pages);

    let network = Rc::new(SimNetwork::default());
    network.set_latency(1, 1);
    let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
    let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
    let vset = VsetId(1);
    world.enqueue_admin(AdminCmd::CreateVset {
        req: ReqId(1),
        vset,
        config: config.vset,
        from_base: None,
    });

    let mut executor = Executor::simulation(seed);
    let mut host = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
    let created = executor.block_on({
        let world = Rc::clone(&world);
        async move { world.next_admin_reply().await }
    });
    assert!(
        matches!(
            created,
            Some(AdminReply::VsetCreated {
                vset: VsetId(1),
                ..
            })
        ),
        "capture-profile vset creation failed: {created:?}"
    );

    for number in 0..dirty_pages {
        let page = PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(1),
            },
            page: PageNo(number),
        };
        let served = executor.block_on({
            let world = Rc::clone(&world);
            async move { world.fault(page, true).await }
        });
        assert!(served, "capture-profile fault failed at {page:?}");
        assert!(
            world.write_resident(page, page_pattern(page, u64::from(number) + 1)),
            "capture-profile write failed at {page:?}"
        );
    }

    let drain_deadline = executor
        .now()
        .saturating_add(config.host.writeback_interval.saturating_mul(4));
    executor.run_until(drain_deadline);
    let blobs = world.durable_blobs();
    let mut report = report_from_state(&executor, &world, &state, &blobs);
    report.completed_ops = u64::from(dirty_pages);
    host.cancel();
    executor.run_ready();
    report
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_final_blobs(
    seed: u64,
    config: ActorHarnessConfig,
) -> (ActorRunReport, Vec<(String, Vec<u8>)>) {
    let network = Rc::new(SimNetwork::default());
    network.set_latency(1_000, 10_000);
    let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
    world.set_corrupt_fills(config.corrupt_fills);
    world.set_drop_write_protect(config.drop_write_protect);
    let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
    let state_slot = Rc::new(RefCell::new(Rc::clone(&state)));
    for number in 1..=config.vset_count {
        let vset = VsetId(u64::from(number));
        world.enqueue_admin(AdminCmd::CreateVset {
            req: ReqId(u64::from(number)),
            vset,
            config: config.vset,
            from_base: None,
        });
    }

    let mut executor = Executor::simulation(seed);
    let host_slot = Rc::new(RefCell::new(Some(
        executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world))),
    )));
    executor.block_on({
        let world = Rc::clone(&world);
        async move {
            let mut remaining = usize::from(config.vset_count);
            while remaining != 0 {
                match world.next_admin_reply().await {
                    Some(AdminReply::VsetCreated { .. }) => remaining -= 1,
                    Some(AdminReply::AdminFailed { req }) => {
                        panic!("vset creation failed for {req:?}")
                    }
                    Some(_) => {}
                    None => panic!("admin reply stream closed during creation"),
                }
            }
        }
    });

    let guest_states = Rc::new(
        (1..=config.vset_count)
            .map(|number| (VsetId(u64::from(number)), Rc::new(GuestState::default())))
            .collect::<BTreeMap<_, _>>(),
    );
    let events = Rc::new(RunEvents::default());
    let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
    for number in 1..=config.vset_count {
        let vset = VsetId(u64::from(number));
        let guest_config = config.clone();
        let guest = executor.spawn(guest_actor(
            Rc::clone(&world),
            Rc::clone(&guest_states[&vset]),
            Rc::clone(&events),
            vset,
            guest_config,
        ));
        guest_slots.borrow_mut().insert(vset, Some(guest));
    }

    let mut supervisor = executor.spawn(recovery_supervisor(
        Rc::clone(&world),
        Rc::clone(&guest_states),
        Rc::clone(&guest_slots),
        Rc::clone(&events),
        config.clone(),
    ));
    let mut fault_actors = Vec::new();
    if !config.faults.crash_at.is_empty() || config.faults.crash_mean_interval != 0 {
        fault_actors.push(executor.spawn(crash_schedule(
            Rc::clone(&world),
            Rc::clone(&host_slot),
            Rc::clone(&state_slot),
            Rc::clone(&guest_slots),
            Rc::clone(&events),
            config.host.clone(),
            config.faults.clone(),
            config.horizon,
        )));
    }
    if let Some(window) = config.faults.store_outage {
        fault_actors.push(executor.spawn(store_outage(Rc::clone(&world), window)));
    }
    if config.faults.bitflip_mean_interval != 0 {
        fault_actors.push(executor.spawn(bitflip_schedule(
            Rc::clone(&world),
            config.faults.bitflip_mean_interval,
            config.horizon,
        )));
    }
    if let Some(interval) = config.checkpoint_interval {
        fault_actors.push(executor.spawn(checkpoint_schedule(
            Rc::clone(&world),
            interval,
            config.horizon,
            config.vset_count,
        )));
    }

    if executor.now() < config.horizon {
        executor.run_until(config.horizon);
    }
    for guest in guest_slots.borrow_mut().values_mut() {
        if let Some(mut guest) = guest.take() {
            guest.cancel();
        }
    }
    supervisor.cancel();
    for mut actor in fault_actors {
        actor.cancel();
    }
    executor.run_ready();
    let drain = executor
        .now()
        .saturating_add(config.host.writeback_interval.saturating_mul(4));
    executor.run_until(drain);

    let blobs = world.durable_blobs();
    let final_state = Rc::clone(&state_slot.borrow());
    let mut report = ActorRunReport {
        trace_hash: executor.trace_hash(),
        executor_polls: executor.polls(),
        counters: final_state.borrow().counters,
        completed_ops: guest_states
            .values()
            .map(|guest| guest.completed.get())
            .sum(),
        per_guest_completed: guest_states
            .iter()
            .map(|(vset, guest)| (vset.0, guest.completed.get()))
            .collect(),
        blob_count: blobs.len(),
        store_keys: world.store_keys(),
        seg_live_bytes_end: final_state.borrow().seg_space().0,
        parked_end: final_state.borrow().stats().parked_faults,
        max_page_reads_in_poll: world.max_page_reads_in_poll(),
        crashes: events.crashes.get(),
        resumes: events.resumes.get(),
        cold_boots: events.cold_boots.get(),
        unrestorable: events.unrestorable.get(),
        guest_deaths: events.guest_deaths.get(),
        bitflips: world.bitflips(),
        ..ActorRunReport::default()
    };
    for guest in guest_states.values() {
        report
            .violations
            .extend(std::mem::take(&mut *guest.violations.borrow_mut()));
    }
    for (name, bytes) in &blobs {
        let extension = Path::new(name).extension().and_then(|value| value.to_str());
        match extension {
            Some("rec" | "recm" | "map") => {
                report.map_bytes_written =
                    report.map_bytes_written.saturating_add(bytes.len() as u64);
                if matches!(extension, Some("rec" | "recm")) {
                    report.max_record_blob_bytes =
                        report.max_record_blob_bytes.max(bytes.len() as u64);
                }
            }
            Some("seg") => {
                report.seg_bytes_end = report.seg_bytes_end.saturating_add(bytes.len() as u64);
            }
            _ => {}
        }
    }
    if let Some(mut host) = host_slot.borrow_mut().take() {
        host.cancel();
    }
    executor.run_ready();
    (report, blobs)
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_workload(
    seed: u64,
    config: ActorHarnessConfig,
    spec: WorkloadSpec,
) -> Result<(ActorRunReport, WorkloadOutcome), String> {
    spec.validate().map_err(|error| error.to_string())?;
    if config.vset_count != 1 {
        return Err("scripted workloads require exactly one vset".to_owned());
    }
    let network = Rc::new(SimNetwork::default());
    network.set_latency(1_000, 10_000);
    let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
    world.set_corrupt_fills(config.corrupt_fills);
    world.set_drop_write_protect(config.drop_write_protect);
    let vset = VsetId(1);
    world.enqueue_admin(AdminCmd::CreateVset {
        req: ReqId(1),
        vset,
        config: config.vset,
        from_base: None,
    });
    let mut executor = Executor::simulation(seed);
    let mut state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
    let mut host = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
    let created = executor.block_on({
        let world = Rc::clone(&world);
        async move { world.next_admin_reply().await }
    });
    if !matches!(
        created,
        Some(AdminReply::VsetCreated {
            vset: VsetId(1),
            ..
        })
    ) {
        return Err(format!("vset creation failed: {created:?}"));
    }

    let mut model = WorkloadModel::new(spec.shape);
    let program = Program::new(spec.clone()).map_err(|error| error.to_string())?;
    let mut next_req = 2_u64;
    let mut crashes = 0_u64;
    let mut resumes = 0_u64;
    let mut cold_boots = 0_u64;
    let unrestorable = 0_u64;
    for operation in program {
        match operation {
            Operation::Create => {}
            Operation::Read { page } => {
                scripted_read(&mut executor, &world, page, model.expected(page))?;
            }
            Operation::Write { page, value } => {
                scripted_write(&mut executor, &world, page, value)?;
            }
            Operation::Sync { volume } => {
                let ok = executor.block_on({
                    let world = Rc::clone(&world);
                    async move {
                        world
                            .sync(blockd_core::world::GuestSync {
                                req: ReqId(next_req),
                                volume: VolumeId {
                                    vset,
                                    idx: VolumeIdx(volume),
                                },
                            })
                            .await
                    }
                });
                next_req = next_req.checked_add(1).expect("script request overflow");
                if !ok {
                    return Err("scripted sync failed".to_owned());
                }
            }
            Operation::Checkpoint => {
                let req = ReqId(next_req);
                next_req = next_req.checked_add(1).expect("script request overflow");
                world.enqueue_admin(AdminCmd::Checkpoint { req, vset });
                let reply = executor.block_on({
                    let world = Rc::clone(&world);
                    async move {
                        loop {
                            match world.next_admin_reply().await {
                                Some(AdminReply::CheckpointDone { req: done, .. })
                                    if done == req =>
                                {
                                    return Some(());
                                }
                                Some(AdminReply::AdminFailed { req: failed }) if failed == req => {
                                    return None;
                                }
                                Some(_) => {}
                                None => return None,
                            }
                        }
                    }
                });
                if reply.is_none() {
                    return Err("scripted checkpoint failed".to_owned());
                }
            }
            Operation::Crash => {
                host.cancel();
                executor.run_ready();
                let _ = world.crash_pending();
                world.crash_guest_io();
                crashes = crashes.saturating_add(1);
                state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
                host = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
            }
            Operation::Restore => {
                let deadline = executor.now().saturating_add(1_000_000_000);
                let verdict = loop {
                    match world.try_next_admin_reply() {
                        Some(AdminReply::VsetRecovered {
                            vset: VsetId(1),
                            verdict,
                        }) => break Some(verdict),
                        Some(_) => {}
                        None if executor.now() >= deadline => break None,
                        None => executor
                            .run_until(deadline.min(executor.now().saturating_add(1_000_000))),
                    }
                };
                match verdict {
                    Some(blockd_core::protocol::Verdict::Resume { .. }) => {
                        resumes = resumes.saturating_add(1);
                    }
                    Some(blockd_core::protocol::Verdict::ColdBoot) => {
                        cold_boots = cold_boots.saturating_add(1);
                    }
                    Some(blockd_core::protocol::Verdict::Unrestorable) => {
                        return Err("scripted recovery was unrestorable".to_owned());
                    }
                    Some(blockd_core::protocol::Verdict::DatabaseReady { .. }) | None => {
                        return Err(format!(
                            "scripted recovery returned no compute verdict (abort={:?})",
                            world.abort_reason()
                        ));
                    }
                }
            }
            Operation::Verify { scope } => {
                for (page, expected) in model.pages(scope) {
                    scripted_read(&mut executor, &world, page, expected)?;
                }
            }
            Operation::Migrate { .. } | Operation::Fork { .. } => {
                return Err("scripted migration/fork is not a single-host operation".to_owned());
            }
        }
        model.complete(operation);
    }
    let blobs = world.durable_blobs();
    let mut report = report_from_state(&executor, &world, &state, &blobs);
    report.crashes = crashes;
    report.resumes = resumes;
    report.cold_boots = cold_boots;
    report.unrestorable = unrestorable;
    host.cancel();
    executor.run_ready();
    Ok((report, model.outcome(&spec.name)))
}

fn scripted_read(
    executor: &mut Executor,
    world: &Rc<SimWorld>,
    logical: LogicalPage,
    expected: u64,
) -> Result<(), String> {
    let page = scripted_page(logical);
    let served = executor.block_on({
        let world = Rc::clone(world);
        async move { world.fault(page, false).await }
    });
    if !served {
        return Err(format!("scripted read fault failed at {logical:?}"));
    }
    let bytes = world
        .page_bytes(page)
        .unwrap_or_else(|| vec![0; page_size()]);
    let actual = crate::guest::claimed_vol_seq(&bytes).saturating_sub(u64::from(expected != 0));
    if actual != expected {
        return Err(format!(
            "scripted read mismatch at {logical:?}: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn scripted_write(
    executor: &mut Executor,
    world: &Rc<SimWorld>,
    logical: LogicalPage,
    value: u64,
) -> Result<(), String> {
    let page = scripted_page(logical);
    let served = executor.block_on({
        let world = Rc::clone(world);
        async move { world.fault(page, true).await }
    });
    if !served || !world.write_resident(page, page_pattern(page, value.saturating_add(1))) {
        return Err(format!("scripted write fault failed at {logical:?}"));
    }
    Ok(())
}

fn scripted_page(logical: LogicalPage) -> PageId {
    PageId {
        volume: VolumeId {
            vset: VsetId(1),
            idx: VolumeIdx(logical.volume),
        },
        page: PageNo(logical.page),
    }
}

fn report_from_state(
    executor: &Executor,
    world: &SimWorld,
    state: &SharedHostState,
    blobs: &[(String, Vec<u8>)],
) -> ActorRunReport {
    let mut report = ActorRunReport {
        trace_hash: executor.trace_hash(),
        executor_polls: executor.polls(),
        counters: state.borrow().counters,
        blob_count: blobs.len(),
        store_keys: world.store_keys(),
        seg_live_bytes_end: state.borrow().seg_space().0,
        parked_end: state.borrow().stats().parked_faults,
        max_page_reads_in_poll: world.max_page_reads_in_poll(),
        bitflips: world.bitflips(),
        ..ActorRunReport::default()
    };
    for (name, bytes) in blobs {
        let extension = Path::new(name).extension().and_then(|value| value.to_str());
        match extension {
            Some("rec" | "recm" | "map") => {
                report.map_bytes_written =
                    report.map_bytes_written.saturating_add(bytes.len() as u64);
                if matches!(extension, Some("rec" | "recm")) {
                    report.max_record_blob_bytes =
                        report.max_record_blob_bytes.max(bytes.len() as u64);
                }
            }
            Some("seg") => {
                report.seg_bytes_end = report.seg_bytes_end.saturating_add(bytes.len() as u64);
            }
            _ => {}
        }
    }
    report
}

async fn recovery_supervisor(
    world: Rc<SimWorld>,
    guest_states: Rc<BTreeMap<VsetId, Rc<GuestState>>>,
    guest_slots: GuestSlots,
    events: Rc<RunEvents>,
    config: ActorHarnessConfig,
) {
    while let Some(reply) = world.next_admin_reply().await {
        let (AdminReply::VsetRecovered { vset, verdict }
        | AdminReply::VsetRestored { vset, verdict, .. }
        | AdminReply::VsetMigratedIn { vset, verdict }
        | AdminReply::VsetForked { vset, verdict, .. }) = reply
        else {
            continue;
        };
        match verdict {
            blockd_core::protocol::Verdict::Resume { vmstate, .. } => {
                events.resumes.set(events.resumes.get().saturating_add(1));
                guest_states[&vset].completed.set(vmstate);
                *guest_states[&vset].expected.borrow_mut() =
                    guest_states[&vset].durable_expected.borrow().clone();
            }
            blockd_core::protocol::Verdict::ColdBoot => {
                events
                    .cold_boots
                    .set(events.cold_boots.get().saturating_add(1));
                *guest_states[&vset].expected.borrow_mut() = guest_states[&vset]
                    .durable_expected
                    .borrow()
                    .iter()
                    .filter(|(page, _)| page.volume.idx.0 != 0)
                    .map(|(page, bytes)| (*page, bytes.clone()))
                    .collect();
            }
            blockd_core::protocol::Verdict::Unrestorable => {
                events
                    .unrestorable
                    .set(events.unrestorable.get().saturating_add(1));
                continue;
            }
            blockd_core::protocol::Verdict::DatabaseReady { .. } => continue,
        }
        *guest_states[&vset].recovering_pages.borrow_mut() = config
            .vset
            .volumes(vset)
            .flat_map(|volume| {
                (0..config.vset.pages_per_volume).map(move |page| PageId {
                    volume,
                    page: PageNo(page),
                })
            })
            .collect();
        let handle = spawn(guest_actor(
            Rc::clone(&world),
            Rc::clone(&guest_states[&vset]),
            Rc::clone(&events),
            vset,
            config.clone(),
        ));
        let previous = guest_slots.borrow_mut().insert(vset, Some(handle));
        assert!(
            previous.is_none_or(|slot| slot.is_none()),
            "recovery started a duplicate guest"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn crash_schedule(
    world: Rc<SimWorld>,
    host_slot: HostSlot,
    state_slot: StateSlot,
    guest_slots: GuestSlots,
    events: Rc<RunEvents>,
    host_config: HostConfig,
    mut faults: ActorFaultPlan,
    horizon: u64,
) {
    if faults.crash_mean_interval != 0 {
        let mut crash_at = random_between(1, faults.crash_mean_interval.saturating_mul(2));
        while crash_at <= horizon {
            faults.crash_at.push(crash_at);
            crash_at = crash_at.saturating_add(random_between(
                1,
                faults.crash_mean_interval.saturating_mul(2),
            ));
        }
    }
    faults.crash_at.sort_unstable();
    faults.crash_at.dedup();
    for crash_at in faults.crash_at {
        if now() < crash_at {
            delay(crash_at - now()).await;
        }
        if let Some(mut host) = host_slot.borrow_mut().take() {
            host.cancel();
        }
        for guest in guest_slots.borrow_mut().values_mut() {
            if let Some(mut guest) = guest.take() {
                guest.cancel();
            }
        }
        let _ = world.crash_pending();
        world.crash_guest_io();
        events.crashes.set(events.crashes.get().saturating_add(1));
        delay(random_between(
            faults.restart_delay.0,
            faults.restart_delay.1,
        ))
        .await;
        world.clear_abort();
        let state = Rc::new(RefCell::new(HostState::new(host_config.clone())));
        *state_slot.borrow_mut() = Rc::clone(&state);
        let handle = spawn(host_actor_with_state(state, Rc::clone(&world)));
        *host_slot.borrow_mut() = Some(handle);
    }
}

async fn store_outage(world: Rc<SimWorld>, window: (u64, u64)) {
    if now() < window.0 {
        delay(window.0 - now()).await;
    }
    world.set_store_outage(true);
    if now() < window.1 {
        delay(window.1 - now()).await;
    }
    world.set_store_outage(false);
}

async fn bitflip_schedule(world: Rc<SimWorld>, mean: u64, horizon: u64) {
    loop {
        delay(random_between(1, mean.saturating_mul(2))).await;
        if now() > horizon {
            return;
        }
        let _ = world.bitflip_segment();
    }
}

async fn checkpoint_schedule(world: Rc<SimWorld>, interval: u64, horizon: u64, vset_count: u16) {
    let mut req = 1_u64 << 63;
    loop {
        delay(interval).await;
        if now() > horizon {
            return;
        }
        for number in 1..=vset_count {
            world.enqueue_admin(AdminCmd::Checkpoint {
                req: ReqId(req),
                vset: VsetId(u64::from(number)),
            });
            req = req.checked_add(1).expect("checkpoint request overflow");
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn guest_actor(
    world: Rc<SimWorld>,
    state: Rc<GuestState>,
    events: Rc<RunEvents>,
    vset: VsetId,
    config: ActorHarnessConfig,
) {
    let mut next_req = (vset.0 << 48) | 1;
    loop {
        let think = random_between(config.think.0, config.think.1);
        delay(think).await;
        if now() > config.horizon {
            return;
        }
        while world.is_paused(vset) {
            delay(1_000).await;
        }
        let sync = config
            .sync_share
            .map_or_else(|| random_u64() % 100 >= 85, hit);
        if sync {
            let volume = VolumeId {
                vset,
                idx: VolumeIdx(
                    1 + u8::try_from(random_u64() % u64::from(config.vset.disk_volumes.max(1)))
                        .expect("volume index fits"),
                ),
            };
            let req = ReqId(next_req);
            next_req = next_req.checked_add(1).expect("guest request overflow");
            if !world
                .sync(blockd_core::world::GuestSync { req, volume })
                .await
            {
                events
                    .guest_deaths
                    .set(events.guest_deaths.get().saturating_add(1));
                return;
            }
            let current = state.expected.borrow();
            let mut durable = state.durable_expected.borrow_mut();
            durable.retain(|page, _| page.volume != volume);
            durable.extend(
                current
                    .iter()
                    .filter(|(page, _)| page.volume == volume)
                    .map(|(page, bytes)| (*page, bytes.clone())),
            );
            finish_operation(&world, &state, vset);
            continue;
        }

        let page = choose_page(&config, vset);
        let write = random_u64() % 100 < 60;
        if !world.fault(page, write).await {
            events
                .guest_deaths
                .set(events.guest_deaths.get().saturating_add(1));
            return;
        }
        if write {
            let sequence = {
                let mut sequences = state.volume_sequences.borrow_mut();
                let sequence = sequences.entry(page.volume).or_default();
                *sequence = sequence.checked_add(1).expect("volume sequence overflow");
                *sequence
            };
            let bytes = page_pattern(page, sequence);
            while !world.write_resident(page, bytes.clone()) {
                if !world.fault(page, true).await {
                    events
                        .guest_deaths
                        .set(events.guest_deaths.get().saturating_add(1));
                    return;
                }
            }
            state.expected.borrow_mut().insert(page, bytes);
            state
                .written_sequences
                .borrow_mut()
                .entry(page)
                .or_default()
                .insert(sequence);
            state.recovering_pages.borrow_mut().remove(&page);
        } else {
            let actual = world
                .page_bytes(page)
                .unwrap_or_else(|| vec![0; page_size()]);
            let expected = state
                .expected
                .borrow()
                .get(&page)
                .cloned()
                .unwrap_or_else(|| vec![0; page_size()]);
            let recovering = state.recovering_pages.borrow_mut().remove(&page);
            let claimed = crate::guest::claimed_vol_seq(&actual);
            let durable_floor = state
                .durable_expected
                .borrow()
                .get(&page)
                .map_or(0, |bytes| crate::guest::claimed_vol_seq(bytes));
            let sequence_is_possible = (claimed == 0
                && !state.durable_expected.borrow().contains_key(&page))
                || state
                    .written_sequences
                    .borrow()
                    .get(&page)
                    .is_some_and(|sequences| sequences.contains(&claimed));
            let valid_recovery = recovering
                && sequence_is_possible
                && claimed >= durable_floor
                && actual == page_pattern(page, claimed);
            if actual != expected && !valid_recovery {
                state
                    .violations
                    .borrow_mut()
                    .push(format!("read returned stale or foreign bytes for {page:?}"));
                return;
            }
            if valid_recovery {
                state.expected.borrow_mut().insert(page, actual);
            }
        }
        finish_operation(&world, &state, vset);
    }
}

fn finish_operation(world: &SimWorld, state: &GuestState, vset: VsetId) {
    let completed = state.completed.get().saturating_add(1);
    state.completed.set(completed);
    world.set_vmstate(vset, completed);
}

fn choose_page(config: &ActorHarnessConfig, vset: VsetId) -> PageId {
    let idx = VolumeIdx(
        u8::try_from(random_u64() % (u64::from(config.vset.disk_volumes) + 1))
            .expect("volume index fits"),
    );
    let pages = config.vset.pages_per_volume;
    let page = match config.hot_pages {
        Some((share, hot)) if hit(share) => random_u64() % u64::from(hot),
        Some((_, hot)) if hot < pages => u64::from(hot) + random_u64() % u64::from(pages - hot),
        _ => random_u64() % u64::from(pages),
    };
    PageId {
        volume: VolumeId { vset, idx },
        page: PageNo(u32::try_from(page).expect("page number fits")),
    }
}

fn random_between(low: u64, high: u64) -> u64 {
    assert!(low <= high);
    low + random_u64() % (high - low + 1)
}

fn hit(probability: Ppm) -> bool {
    random_u64() % 1_000_000 < u64::from(probability.0)
}

#[cfg(test)]
mod tests {
    use blockd_core::types::millis;

    use super::*;

    fn config() -> ActorHarnessConfig {
        ActorHarnessConfig {
            host: HostConfig {
                host: blockd_core::types::HostId(1),
                cache_pages: 24,
                writeback_interval: millis(20),
                backup_retry: millis(5),
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 0,
                replica_placement: None,
            },
            blobs: BlobDevConfig {
                read_latency_min: 100,
                read_latency_max: 500,
                write_latency_min: 100,
                write_latency_max: 1_000,
                ns_per_byte: 0,
            },
            store: StoreConfig {
                latency_min: 1_000,
                latency_max: 5_000,
                ns_per_byte: 0,
            },
            vset_count: 2,
            vset: VsetConfig::compute(2, 16),
            horizon: millis(100),
            think: (50_000, 100_000),
            sync_share: None,
            hot_pages: None,
            checkpoint_interval: None,
            faults: ActorFaultPlan::default(),
            corrupt_fills: false,
            drop_write_protect: false,
        }
    }

    #[test]
    fn quiet_actor_runs_replay_and_make_durable_progress() {
        let first = run(17, config());
        let replay = run(17, config());
        assert_eq!(first, replay);
        assert!(first.violations.is_empty(), "{:?}", first.violations);
        assert!(first.completed_ops > 10);
        assert!(first.counters.records_written > 0);
        assert!(first.counters.syncs_acked > 0);
        assert!(first.blob_count > 0);
        assert!(first.store_keys.iter().any(|key| key.ends_with("/head")));
    }

    #[test]
    fn crash_drops_the_task_tree_and_recovers_on_the_same_executor() {
        let mut crash = config();
        crash.vset_count = 1;
        crash.faults.crash_at = vec![millis(40)];
        crash.faults.restart_delay = (millis(1), millis(1));
        let first = run(23, crash.clone());
        let replay = run(23, crash);
        assert_eq!(first, replay);
        assert_eq!(first.crashes, 1);
        assert_eq!(first.unrestorable, 0);
        assert!(first.cold_boots + first.resumes > 0);
        assert!(first.violations.is_empty(), "{:?}", first.violations);
    }
}
