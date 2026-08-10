//! Single-host deterministic runs over the async actor core.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::rc::Rc;

use blockd_core::engine::{HostState, host_actor_with_state};
use blockd_core::hostmeta::{Counters, HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::{AdminCall, AdminError, AdminEvent, AdminSuccess, ReqId};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, page_size};
use blockd_exec::channel::unbounded;
use blockd_exec::rng::Ppm;
use blockd_exec::{
    Either, Executor, OneOf3, TaskHandle, TaskId, TaskSet, delay, now, random_u64, select2,
    select3, spawn, yield_now,
};
use blockd_workload::{
    LogicalPage, Operation, Program, WorkloadModel, WorkloadOutcome, WorkloadSpec,
};

use crate::actor_world::{OracleSnapshot, SimWorld};
use crate::guest::page_pattern;
use crate::world::blobdev::BlobDevConfig;
use crate::world::store::{StoreConfig, StoreCounters};

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
    pub journal_bitflip_mean_interval: u64,
    pub rot_records_at: Vec<(u64, bool)>,
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
    pub store: StoreCounters,
    pub published_segment_bytes: u64,
    pub published_live_entry_bytes: u64,
    pub published_dead_entry_bytes: u64,
    pub published_segment_overhead_bytes: u64,
    pub map_bytes_written: u64,
    pub max_page_reads_in_poll: u64,
    pub max_pause_ns: u64,
    pub max_record_blob_bytes: u64,
    pub seg_bytes_end: u64,
    pub seg_live_bytes_end: u64,
    pub parked_end: usize,
    pub crashes: u64,
    pub resumes: u64,
    pub cold_boots: u64,
    pub unrestorable: u64,
    pub restores: u64,
    pub guest_deaths: u64,
    pub bitflips: u64,
}

#[derive(Default)]
struct GuestState {
    completed: Cell<u64>,
    total_completed: Cell<u64>,
    expected: Rc<RefCell<BTreeMap<PageId, Vec<u8>>>>,
    durable_expected: RefCell<BTreeMap<PageId, Vec<u8>>>,
    written_sequences: RefCell<BTreeMap<PageId, BTreeSet<u64>>>,
    recovering_pages: Rc<RefCell<BTreeSet<PageId>>>,
    exact_recovery: Cell<bool>,
    exact_candidates: RefCell<Vec<OracleSnapshot>>,
    volume_sequences: RefCell<BTreeMap<VolumeId, u64>>,
    violations: RefCell<Vec<String>>,
}

#[derive(Default)]
struct RunEvents {
    crashes: Cell<u64>,
    resumes: Cell<u64>,
    cold_boots: Cell<u64>,
    unrestorable: Cell<u64>,
    restores: Cell<u64>,
    guest_deaths: Cell<u64>,
    retired_counters: RefCell<Counters>,
}

fn merge_counters(total: &mut Counters, current: &Counters) {
    macro_rules! add {
        ($($field:ident),+ $(,)?) => {
            $(total.$field = total.$field.saturating_add(current.$field);)+
        };
    }
    add!(
        fills,
        zero_fills,
        shared_fills,
        wp_faults,
        guest_pages_dirtied,
        faults_unservable,
        pressure_waits,
        pages_flushed,
        records_written,
        checkpoints_done,
        syncs_acked,
        guest_rejected,
        peer_rejected,
        blobs_deleted,
        manifests_published,
        store_retries,
        fenced,
        assignment_claims,
        assignment_claim_conflicts,
        nvme_reclaims,
        nvme_stalls,
        prefetch_fills,
        hydrate_fills,
        peer_retries,
        cow_captures,
        wedged_guests,
        wedged_hydration,
        wedged_outbound,
        leaf_rolls,
        leaf_fills,
        segs_compacted,
        pages_compacted,
        replica_bytes,
        replica_rejected,
        replica_commits,
        replica_store_bytes,
        replica_unlinks,
        replica_network_bytes,
        replica_logical_bytes,
        replica_nonactive_bytes,
        replica_replacement_bytes,
        replica_cleanup_rewrite_bytes,
        replica_artifact_flushes,
        replica_commit_flushes,
        replica_rotations,
        archive_cycles,
        archive_commits_coalesced,
        replica_capacity_backpressure,
    );
}

type SharedHostState = Rc<RefCell<HostState>>;
type HostSlot = Rc<RefCell<Option<TaskHandle<()>>>>;
type StateSlot = Rc<RefCell<SharedHostState>>;
type GuestSlots = Rc<RefCell<BTreeMap<VsetId, Option<TaskHandle<()>>>>>;
const RECOVERY_CONCURRENCY: usize = 32;
const RECOVERY_QUEUE_CAPACITY: usize = 1_024;
const CHECKPOINT_CONCURRENCY: usize = 32;

struct RecoveryWork {
    event: AdminEvent,
    count_unrestorable: bool,
    generation: u64,
}

struct RecoveryContext {
    world: Rc<SimWorld>,
    guest_states: Rc<BTreeMap<VsetId, Rc<GuestState>>>,
    guest_slots: GuestSlots,
    events: Rc<RunEvents>,
    config: ActorHarnessConfig,
}

enum RecoverySupervisorEvent {
    Completed(Option<(VsetId, u64, Option<RecoveryWork>)>),
    Ingress(Option<(AdminEvent, u64)>),
    RetryReady,
}

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

    let passive_host = HostId(config.host.host.0 ^ u16::MAX);
    config.host.replica_placement = harness_placement(config.host.host, passive_host);
    let (network, [world, passive_world]) =
        SimWorld::pair([config.host.host, passive_host], config.blobs, config.store);
    network.set_latency(1, 1);
    let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
    let passive_state = Rc::new(RefCell::new(HostState::new(harness_passive_config(
        &config.host,
        passive_host,
    ))));
    let vset = VsetId(1);
    let create = world.request_admin(AdminCall::CreateVset {
        vset,
        config: config.vset,
        from_base: None,
    });

    let mut executor = Executor::simulation(seed);
    let mut host = executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world)));
    let mut passive = executor.spawn(host_actor_with_state(
        Rc::clone(&passive_state),
        Rc::clone(&passive_world),
    ));
    let created = executor.block_on({
        let world = Rc::clone(&world);
        let passive_world = Rc::clone(&passive_world);
        async move {
            match select3(create, world.next_abort(), passive_world.next_abort()).await {
                OneOf3::First(reply) => reply.ok(),
                OneOf3::Second(reason) => panic!("primary aborted during creation: {reason:?}"),
                OneOf3::Third(reason) => panic!("passive aborted during creation: {reason:?}"),
            }
        }
    });
    assert!(
        matches!(
            created,
            Some(Ok(AdminSuccess::VsetCreated {
                vset: VsetId(1),
                ..
            }))
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
    merge_counters(&mut report.counters, &passive_state.borrow().counters);
    report.completed_ops = u64::from(dirty_pages);
    host.cancel();
    passive.cancel();
    executor.run_ready();
    report
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_final_blobs(
    seed: u64,
    mut config: ActorHarnessConfig,
) -> (ActorRunReport, Vec<(String, Vec<u8>)>) {
    let passive_host = HostId(config.host.host.0 ^ u16::MAX);
    config.host.replica_placement = harness_placement(config.host.host, passive_host);
    let (network, [world, passive_world]) =
        SimWorld::pair([config.host.host, passive_host], config.blobs, config.store);
    network.set_latency(1_000, 10_000);
    world.set_corrupt_fills(config.corrupt_fills);
    world.set_drop_write_protect(config.drop_write_protect);
    let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
    let state_slot = Rc::new(RefCell::new(Rc::clone(&state)));
    let mut executor = Executor::simulation(seed);
    let passive_state = Rc::new(RefCell::new(HostState::new(harness_passive_config(
        &config.host,
        passive_host,
    ))));
    let mut passive = executor.spawn(host_actor_with_state(
        Rc::clone(&passive_state),
        Rc::clone(&passive_world),
    ));
    let host_slot = Rc::new(RefCell::new(Some(
        executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world))),
    )));
    let creates = (1..=config.vset_count)
        .map(|number| {
            let vset = VsetId(u64::from(number));
            let reply = world.request_admin(AdminCall::CreateVset {
                vset,
                config: config.vset,
                from_base: None,
            });
            (vset, reply)
        })
        .collect::<Vec<_>>();
    executor.block_on({
        let world = Rc::clone(&world);
        let passive_world = Rc::clone(&passive_world);
        async move {
            for (expected_vset, reply) in creates {
                match select3(reply, world.next_abort(), passive_world.next_abort()).await {
                    OneOf3::First(Ok(Ok(AdminSuccess::VsetCreated { vset })))
                        if vset == expected_vset => {}
                    OneOf3::First(Ok(Err(error))) => {
                        panic!("vset creation failed: {error:?}")
                    }
                    OneOf3::First(reply) => {
                        panic!("unexpected vset creation reply: {reply:?}")
                    }
                    OneOf3::Second(reason) => {
                        panic!("primary aborted during creation: {reason:?}")
                    }
                    OneOf3::Third(reason) => {
                        panic!("passive aborted during creation: {reason:?}")
                    }
                }
            }
        }
    });

    let guest_states = Rc::new(
        (1..=config.vset_count)
            .map(|number| (VsetId(u64::from(number)), Rc::new(GuestState::default())))
            .collect::<BTreeMap<_, _>>(),
    );
    for (&vset, guest) in guest_states.iter() {
        world.register_oracle_pages(
            vset,
            Rc::clone(&guest.expected),
            Rc::clone(&guest.recovering_pages),
        );
        world.set_vmstate(vset, 0);
    }
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
    fault_actors.push(executor.spawn(abort_schedule(
        Rc::clone(&world),
        Rc::clone(&host_slot),
        Rc::clone(&state_slot),
        Rc::clone(&guest_slots),
        Rc::clone(&events),
        config.host.clone(),
        config.faults.restart_delay,
    )));
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
    if config.faults.journal_bitflip_mean_interval != 0 {
        fault_actors.push(executor.spawn(record_bitflip_schedule(
            Rc::clone(&world),
            config.faults.journal_bitflip_mean_interval,
            config.horizon,
        )));
    }
    for &(at, mirror) in &config.faults.rot_records_at {
        fault_actors.push(executor.spawn(record_bitflip_at(Rc::clone(&world), at, mirror)));
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
    for mut actor in fault_actors {
        actor.cancel();
    }
    world.set_store_outage(false);
    if host_slot.borrow().is_none() {
        let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
        *state_slot.borrow_mut() = Rc::clone(&state);
        *host_slot.borrow_mut() =
            Some(executor.spawn(host_actor_with_state(state, Rc::clone(&world))));
    }
    executor.run_ready();
    let drain = executor
        .now()
        .saturating_add(config.host.writeback_interval.saturating_mul(4));
    executor.run_until(drain);
    for guest in guest_slots.borrow_mut().values_mut() {
        if let Some(mut guest) = guest.take() {
            guest.cancel();
        }
    }
    supervisor.cancel();
    executor.run_ready();

    let blobs = world.durable_blobs();
    let final_state = Rc::clone(&state_slot.borrow());
    let mut counters = *events.retired_counters.borrow();
    merge_counters(&mut counters, &final_state.borrow().counters);
    merge_counters(&mut counters, &passive_state.borrow().counters);
    let (
        published_segment_bytes,
        published_live_entry_bytes,
        published_dead_entry_bytes,
        published_segment_overhead_bytes,
    ) = world.published_archive_metrics();
    let mut report = ActorRunReport {
        trace_hash: executor.trace_hash(),
        executor_polls: executor.polls(),
        counters,
        completed_ops: guest_states
            .values()
            .map(|guest| guest.total_completed.get())
            .sum(),
        per_guest_completed: guest_states
            .iter()
            .map(|(vset, guest)| (vset.0, guest.total_completed.get()))
            .collect(),
        blob_count: blobs.len(),
        store_keys: world.store_keys(),
        store: world.store_metrics(),
        published_segment_bytes,
        published_live_entry_bytes,
        published_dead_entry_bytes,
        published_segment_overhead_bytes,
        seg_live_bytes_end: final_state.borrow().seg_space().0,
        parked_end: final_state.borrow().stats().parked_faults,
        max_page_reads_in_poll: world.max_page_reads_in_poll(),
        max_pause_ns: world.max_pause_ns(),
        crashes: events.crashes.get(),
        resumes: events.resumes.get(),
        cold_boots: events.cold_boots.get(),
        unrestorable: events.unrestorable.get(),
        restores: events.restores.get(),
        guest_deaths: events.guest_deaths.get(),
        bitflips: world.bitflips(),
        ..ActorRunReport::default()
    };
    for guest in guest_states.values() {
        report
            .violations
            .extend(std::mem::take(&mut *guest.violations.borrow_mut()));
    }
    (report.map_bytes_written, report.max_record_blob_bytes) = world.write_metrics();
    for (name, bytes) in &blobs {
        let extension = Path::new(name).extension().and_then(|value| value.to_str());
        if let Some("seg") = extension {
            report.seg_bytes_end = report.seg_bytes_end.saturating_add(bytes.len() as u64);
        }
    }
    if let Some(mut host) = host_slot.borrow_mut().take() {
        host.cancel();
    }
    passive.cancel();
    executor.run_ready();
    (report, blobs)
}

#[allow(clippy::unnecessary_wraps)]
fn harness_placement(primary: HostId, passive: HostId) -> Option<ReplicaPlacementConfig> {
    Some(ReplicaPlacementConfig {
        membership_epoch: 1,
        local_failure_domain: primary.0,
        roster: vec![
            PeerCandidate {
                host: primary,
                weight: 1,
                failure_domain: primary.0,
                drained: false,
            },
            PeerCandidate {
                host: passive,
                weight: 1,
                failure_domain: passive.0,
                drained: false,
            },
        ],
    })
}

fn harness_passive_config(primary: &HostConfig, passive: HostId) -> HostConfig {
    let mut config = primary.clone();
    config.host = passive;
    config.replica_placement = harness_placement(passive, primary.host);
    config
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_workload(
    seed: u64,
    mut config: ActorHarnessConfig,
    spec: WorkloadSpec,
) -> Result<(ActorRunReport, WorkloadOutcome), String> {
    spec.validate().map_err(|error| error.to_string())?;
    if config.vset_count != 1 {
        return Err("scripted workloads require exactly one vset".to_owned());
    }
    let passive_host = HostId(config.host.host.0 ^ u16::MAX);
    config.host.replica_placement = harness_placement(config.host.host, passive_host);
    let (network, [world, passive_world]) =
        SimWorld::pair([config.host.host, passive_host], config.blobs, config.store);
    network.set_latency(1_000, 10_000);
    world.set_corrupt_fills(config.corrupt_fills);
    world.set_drop_write_protect(config.drop_write_protect);
    let vset = VsetId(1);
    let create = world.request_admin(AdminCall::CreateVset {
        vset,
        config: config.vset,
        from_base: None,
    });
    let mut executor = Executor::simulation(seed);
    let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
    let state_slot = Rc::new(RefCell::new(Rc::clone(&state)));
    let passive_state = Rc::new(RefCell::new(HostState::new(harness_passive_config(
        &config.host,
        passive_host,
    ))));
    let mut passive = executor.spawn(host_actor_with_state(
        Rc::clone(&passive_state),
        Rc::clone(&passive_world),
    ));
    let host_slot = Rc::new(RefCell::new(Some(
        executor.spawn(host_actor_with_state(Rc::clone(&state), Rc::clone(&world))),
    )));
    let created = executor.block_on({
        let world = Rc::clone(&world);
        let passive_world = Rc::clone(&passive_world);
        async move {
            match select3(create, world.next_abort(), passive_world.next_abort()).await {
                OneOf3::First(reply) => reply.ok(),
                OneOf3::Second(reason) => panic!("primary aborted during creation: {reason:?}"),
                OneOf3::Third(reason) => panic!("passive aborted during creation: {reason:?}"),
            }
        }
    });
    if !matches!(
        created,
        Some(Ok(AdminSuccess::VsetCreated {
            vset: VsetId(1),
            ..
        }))
    ) {
        return Err(format!("vset creation failed: {created:?}"));
    }
    world.set_vmstate(vset, 0);

    let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
    let events = Rc::new(RunEvents::default());
    let mut fault_actors = vec![executor.spawn(abort_schedule(
        Rc::clone(&world),
        Rc::clone(&host_slot),
        Rc::clone(&state_slot),
        Rc::clone(&guest_slots),
        Rc::clone(&events),
        config.host.clone(),
        config.faults.restart_delay,
    ))];
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
    if config.faults.journal_bitflip_mean_interval != 0 {
        fault_actors.push(executor.spawn(record_bitflip_schedule(
            Rc::clone(&world),
            config.faults.journal_bitflip_mean_interval,
            config.horizon,
        )));
    }
    for &(at, mirror) in &config.faults.rot_records_at {
        fault_actors.push(executor.spawn(record_bitflip_at(Rc::clone(&world), at, mirror)));
    }
    if let Some(interval) = config.checkpoint_interval {
        fault_actors.push(executor.spawn(checkpoint_schedule(
            Rc::clone(&world),
            interval,
            config.horizon,
            1,
        )));
    }

    let mut model = WorkloadModel::new(spec.shape);
    let program = Program::new(spec.clone()).map_err(|error| error.to_string())?;
    let mut next_req = 2_u64;
    let mut vmstate = 0_u64;
    let mut resumes = 0_u64;
    let mut cold_boots = 0_u64;
    let unrestorable = 0_u64;
    for operation in program {
        let think = config.think;
        executor.block_on(async move {
            delay(random_between(think.0, think.1)).await;
        });
        if executor.now() > config.horizon {
            return Err(format!(
                "workload {} did not finish before the simulation horizon (completed={})",
                spec.name,
                model.outcome(&spec.name).completed
            ));
        }
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
                let reply = world.request_admin(AdminCall::Checkpoint { retry: req, vset });
                let reply = executor.block_on(reply);
                if !matches!(reply, Ok(Ok(AdminSuccess::CheckpointDone { .. }))) {
                    return Err("scripted checkpoint failed".to_owned());
                }
            }
            Operation::Crash => {
                let running = host_slot.borrow_mut().take();
                if let Some(mut host) = running {
                    host.cancel();
                    executor.run_ready();
                    merge_counters(
                        &mut events.retired_counters.borrow_mut(),
                        &state_slot.borrow().borrow().counters,
                    );
                    let _ = world.crash_pending();
                    world.crash_guest_io();
                    events.crashes.set(events.crashes.get().saturating_add(1));
                    let restart_delay = config.faults.restart_delay;
                    let wait = executor
                        .block_on(async move { random_between(restart_delay.0, restart_delay.1) });
                    executor.run_until(executor.now().saturating_add(wait));
                    world.clear_abort();
                    let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
                    *state_slot.borrow_mut() = Rc::clone(&state);
                    *host_slot.borrow_mut() =
                        Some(executor.spawn(host_actor_with_state(state, Rc::clone(&world))));
                }
            }
            Operation::Restore => {
                let deadline = executor.now().saturating_add(1_000_000_000);
                let verdict = loop {
                    match world.try_next_admin_event() {
                        Some(AdminEvent::VsetRecovered {
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
                    Some(blockd_core::protocol::Verdict::Resume {
                        vmstate: recovered, ..
                    }) => {
                        resumes = resumes.saturating_add(1);
                        vmstate = recovered;
                        world.set_vmstate(vset, vmstate);
                    }
                    Some(blockd_core::protocol::Verdict::ColdBoot) => {
                        cold_boots = cold_boots.saturating_add(1);
                        vmstate = 0;
                        world.set_vmstate(vset, vmstate);
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
        if matches!(
            operation,
            Operation::Read { .. }
                | Operation::Write { .. }
                | Operation::Sync { .. }
                | Operation::Verify { .. }
        ) {
            vmstate = vmstate.saturating_add(1);
            world.set_vmstate(vset, vmstate);
        }
    }
    for mut actor in fault_actors {
        actor.cancel();
    }
    world.set_store_outage(false);
    if host_slot.borrow().is_none() {
        let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
        *state_slot.borrow_mut() = Rc::clone(&state);
        *host_slot.borrow_mut() =
            Some(executor.spawn(host_actor_with_state(state, Rc::clone(&world))));
    }
    executor.run_ready();
    let drain = executor
        .now()
        .saturating_add(config.host.writeback_interval.saturating_mul(4));
    executor.run_until(drain);
    let blobs = world.durable_blobs();
    let final_state = Rc::clone(&state_slot.borrow());
    let mut report = report_from_state(&executor, &world, &final_state, &blobs);
    let mut counters = *events.retired_counters.borrow();
    merge_counters(&mut counters, &report.counters);
    merge_counters(&mut counters, &passive_state.borrow().counters);
    report.counters = counters;
    report.crashes = events.crashes.get();
    report.resumes = resumes;
    report.cold_boots = cold_boots;
    report.unrestorable = unrestorable;
    let outcome = model.outcome(&spec.name);
    report.completed_ops = outcome.completed;
    report.per_guest_completed.insert(vset.0, outcome.completed);
    if let Some(mut host) = host_slot.borrow_mut().take() {
        host.cancel();
    }
    passive.cancel();
    executor.run_ready();
    Ok((report, outcome))
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
    let (
        published_segment_bytes,
        published_live_entry_bytes,
        published_dead_entry_bytes,
        published_segment_overhead_bytes,
    ) = world.published_archive_metrics();
    let mut report = ActorRunReport {
        trace_hash: executor.trace_hash(),
        executor_polls: executor.polls(),
        counters: state.borrow().counters,
        blob_count: blobs.len(),
        store_keys: world.store_keys(),
        store: world.store_metrics(),
        published_segment_bytes,
        published_live_entry_bytes,
        published_dead_entry_bytes,
        published_segment_overhead_bytes,
        seg_live_bytes_end: state.borrow().seg_space().0,
        parked_end: state.borrow().stats().parked_faults,
        max_page_reads_in_poll: world.max_page_reads_in_poll(),
        max_pause_ns: world.max_pause_ns(),
        bitflips: world.bitflips(),
        ..ActorRunReport::default()
    };
    (report.map_bytes_written, report.max_record_blob_bytes) = world.write_metrics();
    for (name, bytes) in blobs {
        let extension = Path::new(name).extension().and_then(|value| value.to_str());
        if let Some("seg") = extension {
            report.seg_bytes_end = report.seg_bytes_end.saturating_add(bytes.len() as u64);
        }
    }
    report
}

#[allow(clippy::too_many_lines)]
async fn recovery_supervisor(
    world: Rc<SimWorld>,
    guest_states: Rc<BTreeMap<VsetId, Rc<GuestState>>>,
    guest_slots: GuestSlots,
    events: Rc<RunEvents>,
    config: ActorHarnessConfig,
) {
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = BTreeMap::<VsetId, (u64, TaskId)>::new();
    let mut pending = BTreeMap::<VsetId, (u64, RecoveryWork)>::new();
    let mut ingress_open = true;
    let retry_delay = config.host.backup_retry.max(1);
    loop {
        while active.len() < RECOVERY_CONCURRENCY {
            let Some(vset) = pending.iter().find_map(|(&vset, (ready_at, _))| {
                (*ready_at <= now() && !active.contains_key(&vset)).then_some(vset)
            }) else {
                break;
            };
            let (_, work) = pending.remove(&vset).expect("selected recovery event");
            let completed = completed.clone();
            let context = RecoveryContext {
                world: Rc::clone(&world),
                guest_states: Rc::clone(&guest_states),
                guest_slots: Rc::clone(&guest_slots),
                events: Rc::clone(&events),
                config: config.clone(),
            };
            let generation = work.generation;
            if world.admin_event_generation(vset) != generation {
                yield_now().await;
                continue;
            }
            let task = actors.spawn(async move {
                let retry =
                    handle_recovery_event(context, work.event, work.count_unrestorable, generation)
                        .await;
                let _ = completed.send((vset, generation, retry));
            });
            active.insert(vset, (generation, task));
        }
        if !ingress_open && active.is_empty() && pending.is_empty() {
            return;
        }
        let at_capacity = active.len() + pending.len() >= RECOVERY_QUEUE_CAPACITY;
        let wait_for_retry = if active.len() < RECOVERY_CONCURRENCY {
            pending
                .iter()
                .filter(|(vset, _)| !active.contains_key(vset))
                .map(|(_, (ready_at, _))| ready_at.saturating_sub(now()))
                .min()
                .unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };
        let event = if ingress_open && !at_capacity {
            match select3(
                completions.recv(),
                world.next_admin_event_with_generation(),
                delay(wait_for_retry),
            )
            .await
            {
                OneOf3::First(completion) => RecoverySupervisorEvent::Completed(completion),
                OneOf3::Second(event) => RecoverySupervisorEvent::Ingress(event),
                OneOf3::Third(()) => RecoverySupervisorEvent::RetryReady,
            }
        } else {
            match select2(completions.recv(), delay(wait_for_retry)).await {
                Either::First(completion) => RecoverySupervisorEvent::Completed(completion),
                Either::Second(()) => RecoverySupervisorEvent::RetryReady,
            }
        };
        match event {
            RecoverySupervisorEvent::Completed(Some((vset, generation, retry))) => {
                if active
                    .get(&vset)
                    .is_some_and(|(active_generation, _)| *active_generation == generation)
                {
                    active.remove(&vset);
                    if world.admin_event_generation(vset) == generation
                        && let Some(work) = retry
                    {
                        pending
                            .entry(vset)
                            .or_insert((now().saturating_add(retry_delay), work));
                    }
                }
            }
            RecoverySupervisorEvent::Completed(None) => return,
            RecoverySupervisorEvent::Ingress(Some((event, generation))) => {
                let vset = match event {
                    AdminEvent::VsetRecovered { vset, .. }
                    | AdminEvent::VsetMigratedIn { vset, .. } => vset,
                };
                if let Some((_, task)) = active.remove(&vset) {
                    actors.cancel(task);
                }
                pending.insert(
                    vset,
                    (
                        now(),
                        RecoveryWork {
                            event,
                            count_unrestorable: true,
                            generation,
                        },
                    ),
                );
            }
            RecoverySupervisorEvent::Ingress(None) => ingress_open = false,
            RecoverySupervisorEvent::RetryReady => {}
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_recovery_event(
    context: RecoveryContext,
    event: AdminEvent,
    count_unrestorable: bool,
    generation: u64,
) -> Option<RecoveryWork> {
    let RecoveryContext {
        world,
        guest_states,
        guest_slots,
        events,
        config,
    } = context;
    let (vset, mut verdict, mut local_recovery) = match event {
        AdminEvent::VsetRecovered { vset, verdict } => (vset, verdict, true),
        AdminEvent::VsetMigratedIn { vset, verdict } => (vset, verdict, false),
    };
    if world.admin_event_generation(vset) != generation {
        return None;
    }
    let mut restored = false;
    'verdict: loop {
        if restored {
            events.restores.set(events.restores.get().saturating_add(1));
        }
        match verdict {
            blockd_core::protocol::Verdict::Resume { vmstate, .. } => {
                guest_states[&vset].exact_recovery.set(true);
                events.resumes.set(events.resumes.get().saturating_add(1));
                guest_states[&vset].completed.set(vmstate);
                world.set_vmstate(vset, vmstate);
                let mut candidates = world.checkpoint_snapshots(vset, vmstate);
                if candidates.is_empty() {
                    candidates.push(OracleSnapshot {
                        pages: guest_states[&vset].durable_expected.borrow().clone(),
                        unknown: guest_states[&vset].recovering_pages.borrow().clone(),
                    });
                }
                let expected = candidates
                    .last()
                    .map(|snapshot| snapshot.pages.clone())
                    .unwrap_or_default();
                *guest_states[&vset].exact_candidates.borrow_mut() = candidates;
                guest_states[&vset]
                    .expected
                    .borrow_mut()
                    .clone_from(&expected);
            }
            blockd_core::protocol::Verdict::ColdBoot => {
                guest_states[&vset].exact_recovery.set(false);
                guest_states[&vset].exact_candidates.borrow_mut().clear();
                guest_states[&vset].completed.set(0);
                world.set_vmstate(vset, 0);
                events
                    .cold_boots
                    .set(events.cold_boots.get().saturating_add(1));
                if restored {
                    guest_states[&vset].durable_expected.borrow_mut().clear();
                }
                let cold = guest_states[&vset]
                    .durable_expected
                    .borrow()
                    .iter()
                    .filter(|(page, _)| page.volume.idx.0 != 0)
                    .map(|(page, bytes)| (*page, bytes.clone()))
                    .collect::<BTreeMap<_, _>>();
                guest_states[&vset].expected.borrow_mut().clone_from(&cold);
                *guest_states[&vset].durable_expected.borrow_mut() = cold;
            }
            blockd_core::protocol::Verdict::Unrestorable => {
                if count_unrestorable {
                    events
                        .unrestorable
                        .set(events.unrestorable.get().saturating_add(1));
                }
                if local_recovery {
                    let restored_reply = match select2(
                        world.request_admin(AdminCall::RestoreVset { vset }),
                        world.wait_for_admin_event_generation_change(vset, generation),
                    )
                    .await
                    {
                        Either::First(reply) => reply,
                        Either::Second(()) => return None,
                    };
                    if world.admin_event_generation(vset) != generation {
                        return None;
                    }
                    match restored_reply {
                        Ok(Ok(AdminSuccess::VsetRestored {
                            vset: restored_vset,
                            verdict: restored_verdict,
                            ..
                        })) => {
                            assert_eq!(restored_vset, vset);
                            verdict = restored_verdict;
                            local_recovery = false;
                            restored = true;
                            continue 'verdict;
                        }
                        Ok(Err(AdminError::Busy | AdminError::Stale | AdminError::Unavailable))
                        | Err(_) => {
                            return Some(RecoveryWork {
                                event: AdminEvent::VsetRecovered {
                                    vset,
                                    verdict: blockd_core::protocol::Verdict::Unrestorable,
                                },
                                count_unrestorable: false,
                                generation,
                            });
                        }
                        Ok(Err(AdminError::Rejected | AdminError::NotFound) | Ok(_)) => {
                            return None;
                        }
                    }
                }
                return None;
            }
            blockd_core::protocol::Verdict::DatabaseReady { .. } => return None,
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
        if world.admin_event_generation(vset) != generation {
            return None;
        }
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
        return None;
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
        crash_and_restart(
            &world,
            &host_slot,
            &state_slot,
            &guest_slots,
            &events,
            &host_config,
            faults.restart_delay,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn abort_schedule(
    world: Rc<SimWorld>,
    host_slot: HostSlot,
    state_slot: StateSlot,
    guest_slots: GuestSlots,
    events: Rc<RunEvents>,
    host_config: HostConfig,
    restart_delay: (u64, u64),
) {
    while world.next_abort().await.is_some() {
        crash_and_restart(
            &world,
            &host_slot,
            &state_slot,
            &guest_slots,
            &events,
            &host_config,
            restart_delay,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn crash_and_restart(
    world: &Rc<SimWorld>,
    host_slot: &HostSlot,
    state_slot: &StateSlot,
    guest_slots: &GuestSlots,
    events: &RunEvents,
    host_config: &HostConfig,
    restart_delay: (u64, u64),
) {
    let Some(mut host) = host_slot.borrow_mut().take() else {
        return;
    };
    host.cancel();
    let _ = host.await;
    merge_counters(
        &mut events.retired_counters.borrow_mut(),
        &state_slot.borrow().borrow().counters,
    );
    let guests = guest_slots
        .borrow_mut()
        .values_mut()
        .filter_map(Option::take)
        .collect::<Vec<_>>();
    for mut guest in guests {
        guest.cancel();
        let _ = guest.await;
    }
    let _ = world.crash_pending();
    world.crash_guest_io();
    events.crashes.set(events.crashes.get().saturating_add(1));
    delay(random_between(restart_delay.0, restart_delay.1)).await;
    world.clear_abort();
    let state = Rc::new(RefCell::new(HostState::new(host_config.clone())));
    *state_slot.borrow_mut() = Rc::clone(&state);
    let handle = spawn(host_actor_with_state(state, Rc::clone(world)));
    *host_slot.borrow_mut() = Some(handle);
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

async fn record_bitflip_schedule(world: Rc<SimWorld>, mean: u64, horizon: u64) {
    loop {
        delay(random_between(1, mean.saturating_mul(2))).await;
        if now() > horizon {
            return;
        }
        let _ = world.bitflip_record(None);
    }
}

async fn record_bitflip_at(world: Rc<SimWorld>, at: u64, mirror: bool) {
    if now() < at {
        delay(at - now()).await;
    }
    let _ = world.bitflip_record(Some(mirror));
}

async fn checkpoint_schedule(world: Rc<SimWorld>, interval: u64, horizon: u64, vset_count: u16) {
    let mut req = 1_u64 << 63;
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = BTreeSet::new();
    let mut queued = BTreeSet::new();
    let mut pending = VecDeque::new();
    let interval = interval.max(1);
    let mut next_cadence = now().saturating_add(interval);
    loop {
        if now() >= next_cadence {
            if now() > horizon {
                pending.clear();
                queued.clear();
                while !active.is_empty() {
                    let Some(vset) = completions.recv().await else {
                        return;
                    };
                    active.remove(&vset);
                }
                return;
            }
            for number in 1..=vset_count {
                let vset = VsetId(u64::from(number));
                if world.vmstate_ready(vset) && !active.contains(&vset) && queued.insert(vset) {
                    pending.push_back(vset);
                }
            }
            next_cadence = now().saturating_add(interval);
        }
        while active.len() < CHECKPOINT_CONCURRENCY {
            let Some(vset) = pending.pop_front() else {
                break;
            };
            queued.remove(&vset);
            if !world.vmstate_ready(vset) || !active.insert(vset) {
                continue;
            }
            let reply = world.request_admin(AdminCall::Checkpoint {
                retry: ReqId(req),
                vset,
            });
            let completed = completed.clone();
            actors.spawn(async move {
                let _ = reply.await;
                let _ = completed.send(vset);
            });
            req = req.checked_add(1).expect("checkpoint request overflow");
        }
        match select2(
            completions.recv(),
            delay(next_cadence.saturating_sub(now())),
        )
        .await
        {
            Either::First(Some(vset)) => {
                active.remove(&vset);
            }
            Either::First(None) => return,
            Either::Second(()) => {}
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
            state.expected.borrow_mut().insert(page, bytes.clone());
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
            let exact = state.exact_recovery.get();
            let exact_match = if recovering && exact {
                narrow_exact_candidates(&mut state.exact_candidates.borrow_mut(), page, &actual)
            } else {
                actual == expected
            };
            let recovery = if !recovering {
                RecoveryExpectation::NotRecovering
            } else if exact {
                RecoveryExpectation::Exact
            } else {
                RecoveryExpectation::Crash {
                    sequence_is_possible,
                    durable_floor,
                    pattern_matches: actual == page_pattern(page, claimed),
                }
            };
            let valid_recovery = recovery_read_is_valid(exact_match, recovery, claimed);
            if !valid_recovery {
                let expected_sequence = crate::guest::claimed_vol_seq(&expected);
                state
                    .violations
                    .borrow_mut()
                    .push(format!(
                        "read returned stale or foreign bytes for {page:?}: actual sequence {claimed}, expected sequence {expected_sequence}, durable floor {durable_floor}, recovering {recovering}, exact {}, possible {sequence_is_possible}, vmstate {} at {}",
                        exact,
                        state.completed.get(),
                        now(),
                    ));
                return;
            }
            if actual != expected {
                state.expected.borrow_mut().insert(page, actual);
            }
        }
        finish_operation(&world, &state, vset);
    }
}

#[derive(Clone, Copy)]
enum RecoveryExpectation {
    NotRecovering,
    Exact,
    Crash {
        sequence_is_possible: bool,
        durable_floor: u64,
        pattern_matches: bool,
    },
}

fn recovery_read_is_valid(exact_match: bool, recovery: RecoveryExpectation, claimed: u64) -> bool {
    exact_match
        || matches!(
            recovery,
            RecoveryExpectation::Crash {
                sequence_is_possible: true,
                durable_floor,
                pattern_matches: true,
            } if claimed >= durable_floor
        )
}

fn narrow_exact_candidates(
    candidates: &mut Vec<OracleSnapshot>,
    page: PageId,
    actual: &[u8],
) -> bool {
    let zero = vec![0; page_size()];
    candidates.retain(|snapshot| {
        snapshot.unknown.contains(&page)
            || snapshot.pages.get(&page).unwrap_or(&zero).as_slice() == actual
    });
    !candidates.is_empty()
}

fn finish_operation(world: &SimWorld, state: &GuestState, vset: VsetId) {
    let completed = state.completed.get().saturating_add(1);
    state.completed.set(completed);
    state
        .total_completed
        .set(state.total_completed.get().saturating_add(1));
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
#[allow(clippy::default_trait_access)]
mod tests {
    use blockd_core::types::millis;
    use blockd_core::world::AdminIo;
    use blockd_exec::timeout;

    use super::*;
    use crate::actor_world::SimNetwork;

    fn config() -> ActorHarnessConfig {
        ActorHarnessConfig {
            host: HostConfig {
                archive: Default::default(),
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

    #[test]
    fn backed_local_unrestorable_verdict_requests_store_restore() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        let guest_states = Rc::new(BTreeMap::from([(vset, Rc::new(GuestState::default()))]));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(29);
        let command = executor.block_on({
            let world = Rc::clone(&world);
            async move {
                let supervisor = spawn(recovery_supervisor(
                    Rc::clone(&world),
                    guest_states,
                    guest_slots,
                    events,
                    config,
                ));
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    blockd_core::protocol::AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                let command = AdminIo::next_admin(world.as_ref())
                    .await
                    .map(|request| request.into_parts().0);
                drop(supervisor);
                command
            }
        });
        assert!(matches!(
            command,
            Some(AdminCall::RestoreVset { vset: found }) if found == vset
        ));
    }

    #[test]
    fn recoverable_restore_failure_retries_without_blocking_another_vset() {
        let mut config = config();
        config.host.backup_retry = 1;
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let first = VsetId(1);
        let second = VsetId(2);
        let guest_states = Rc::new(BTreeMap::from([
            (first, Rc::new(GuestState::default())),
            (second, Rc::new(GuestState::default())),
        ]));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(30);
        executor.block_on({
            let world = Rc::clone(&world);
            let guest_slots = Rc::clone(&guest_slots);
            async move {
                let supervisor = spawn(recovery_supervisor(
                    Rc::clone(&world),
                    guest_states,
                    Rc::clone(&guest_slots),
                    events,
                    config,
                ));
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset: first,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset: second,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;

                let request = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("first restore request");
                let (call, mut reply) = request.into_parts();
                assert_eq!(call, AdminCall::RestoreVset { vset: first });
                let _ = reply.send(Err(AdminError::Busy));
                delay(2).await;
                assert!(
                    guest_slots
                        .borrow()
                        .get(&second)
                        .is_some_and(Option::is_some)
                );

                let request = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("retried restore request");
                let (call, _reply) = request.into_parts();
                assert_eq!(call, AdminCall::RestoreVset { vset: first });
                drop(supervisor);
            }
        });
    }

    #[test]
    fn older_restore_success_cannot_replace_newer_recovery_event() {
        let mut config = config();
        config.host.backup_retry = 10;
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        let guest_states = Rc::new(BTreeMap::from([(vset, Rc::new(GuestState::default()))]));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(32);
        executor.block_on({
            let world = Rc::clone(&world);
            let guest_slots = Rc::clone(&guest_slots);
            async move {
                let supervisor = spawn(recovery_supervisor(
                    Rc::clone(&world),
                    guest_states,
                    Rc::clone(&guest_slots),
                    events,
                    config,
                ));
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                let request = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("restore request");
                let (call, mut reply) = request.into_parts();
                assert_eq!(call, AdminCall::RestoreVset { vset });

                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;
                delay(1).await;
                let _ = reply.send(Ok(AdminSuccess::VsetRestored {
                    vset,
                    verdict: blockd_core::protocol::Verdict::ColdBoot,
                }));
                delay(1).await;

                assert!(
                    guest_slots.borrow().get(&vset).is_some_and(Option::is_some),
                    "the newer cold-boot recovery must win over the stale restore"
                );
                match select2(AdminIo::next_admin(world.as_ref()), delay(0)).await {
                    Either::First(Some(request)) => {
                        panic!("unexpected stale request: {:?}", request.body)
                    }
                    Either::First(None) | Either::Second(()) => {}
                }
                drop(supervisor);
            }
        });
    }

    #[test]
    fn newer_recovery_preempts_a_blocked_restore() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        let guest_states = Rc::new(BTreeMap::from([(vset, Rc::new(GuestState::default()))]));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(37);
        executor.block_on({
            let world = Rc::clone(&world);
            let guest_slots = Rc::clone(&guest_slots);
            async move {
                let supervisor = spawn(recovery_supervisor(
                    Rc::clone(&world),
                    guest_states,
                    Rc::clone(&guest_slots),
                    events,
                    config,
                ));
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                let stale = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("stale restore request");
                let (_, mut stale_reply) = stale.into_parts();

                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;
                delay(2).await;

                assert!(
                    guest_slots.borrow().get(&vset).is_some_and(Option::is_some),
                    "the newer recovery must not wait for the stale restore reply"
                );
                assert!(
                    stale_reply.send(Err(AdminError::Busy)).is_err(),
                    "preemption must cancel the stale administrative request"
                );
                drop(supervisor);
            }
        });
    }

    #[test]
    fn generation_change_preempts_restore_before_newer_event_admission() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        let guest_states = Rc::new(BTreeMap::from([(vset, Rc::new(GuestState::default()))]));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(38);
        executor.block_on({
            let world = Rc::clone(&world);
            async move {
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                let (_, generation) = world
                    .next_admin_event_with_generation()
                    .await
                    .expect("initial recovery event");
                let recovery = spawn(handle_recovery_event(
                    RecoveryContext {
                        world: Rc::clone(&world),
                        guest_states,
                        guest_slots,
                        events,
                        config,
                    },
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                    true,
                    generation,
                ));
                let stale = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("restore request");
                let (_, mut stale_reply) = stale.into_parts();

                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;

                assert!(
                    recovery.await.expect("recovery actor completes").is_none(),
                    "a superseded recovery must stop without waiting for supervisor ingress"
                );
                assert!(
                    stale_reply.send(Err(AdminError::Busy)).is_err(),
                    "generation change must cancel the stale restore request"
                );
            }
        });
    }

    #[test]
    fn superseded_recovery_work_issues_no_restore_request() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        let guest_states = Rc::new(BTreeMap::from([(vset, Rc::new(GuestState::default()))]));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let stale_generation = world.admin_event_generation(vset);
        let mut executor = Executor::simulation(34);
        executor.block_on({
            let world = Rc::clone(&world);
            async move {
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;
                let context = RecoveryContext {
                    world: Rc::clone(&world),
                    guest_states,
                    guest_slots,
                    events,
                    config,
                };
                let stale = spawn(handle_recovery_event(
                    context,
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                    false,
                    stale_generation,
                ));
                match select2(AdminIo::next_admin(world.as_ref()), stale).await {
                    Either::First(Some(request)) => {
                        panic!("superseded recovery issued request: {:?}", request.body)
                    }
                    Either::First(None) => panic!("admin ingress closed"),
                    Either::Second(result) => {
                        assert!(result.expect("recovery task completes").is_none());
                    }
                }
            }
        });
    }

    #[test]
    fn stale_recovery_admission_does_not_block_its_newer_event() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        let guest_states = Rc::new(BTreeMap::from([(vset, Rc::new(GuestState::default()))]));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(35);
        executor.block_on({
            let world = Rc::clone(&world);
            let guest_slots = Rc::clone(&guest_slots);
            async move {
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;
                let supervisor = spawn(recovery_supervisor(
                    world,
                    guest_states,
                    Rc::clone(&guest_slots),
                    events,
                    config,
                ));
                delay(2).await;
                assert!(
                    guest_slots.borrow().get(&vset).is_some_and(Option::is_some),
                    "the stale event must not retain the vset's active slot"
                );
                drop(supervisor);
            }
        });
    }

    #[test]
    fn queued_recovery_supersedes_an_active_restore_at_capacity() {
        let mut config = config();
        config.host.backup_retry = 10;
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let first = VsetId(1);
        let guest_states = Rc::new(
            (1..=RECOVERY_QUEUE_CAPACITY)
                .map(|number| {
                    (
                        VsetId(u64::try_from(number).expect("vset fits")),
                        Rc::new(GuestState::default()),
                    )
                })
                .collect(),
        );
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(33);
        executor.block_on({
            let world = Rc::clone(&world);
            let guest_slots = Rc::clone(&guest_slots);
            async move {
                let supervisor = spawn(recovery_supervisor(
                    Rc::clone(&world),
                    guest_states,
                    Rc::clone(&guest_slots),
                    events,
                    config,
                ));
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset: first,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                let request = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("first restore request");
                let (call, mut first_reply) = request.into_parts();
                assert_eq!(call, AdminCall::RestoreVset { vset: first });

                for number in 2..=RECOVERY_QUEUE_CAPACITY {
                    AdminIo::emit_admin_event(
                        world.as_ref(),
                        AdminEvent::VsetRecovered {
                            vset: VsetId(u64::try_from(number).expect("vset fits")),
                            verdict: blockd_core::protocol::Verdict::Unrestorable,
                        },
                    )
                    .await;
                }
                delay(1).await;
                let mut other_replies = Vec::new();
                for _ in 1..RECOVERY_CONCURRENCY {
                    let request = AdminIo::next_admin(world.as_ref())
                        .await
                        .expect("concurrent restore request");
                    let (call, reply) = request.into_parts();
                    assert!(matches!(call, AdminCall::RestoreVset { .. }));
                    other_replies.push(reply);
                }

                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset: first,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;
                let _ = first_reply.send(Ok(AdminSuccess::VsetRestored {
                    vset: first,
                    verdict: blockd_core::protocol::Verdict::ColdBoot,
                }));
                delay(1).await;
                assert!(
                    guest_slots.borrow().get(&first).is_none(),
                    "the superseded restore must not start a guest"
                );

                for mut reply in other_replies {
                    let _ = reply.send(Err(AdminError::Busy));
                }
                delay(1).await;
                assert!(
                    guest_slots
                        .borrow()
                        .get(&first)
                        .is_some_and(Option::is_some),
                    "the queued recovery event must start the replacement guest"
                );
                drop(supervisor);
            }
        });
    }

    #[test]
    fn retrying_restores_release_every_recovery_slot_for_queued_vsets() {
        let mut config = config();
        config.host.backup_retry = 10;
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let queued = VsetId(u64::try_from(RECOVERY_CONCURRENCY + 1).expect("vset fits"));
        let guest_states = Rc::new(
            (1..=queued.0)
                .map(|number| (VsetId(number), Rc::new(GuestState::default())))
                .collect(),
        );
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(31);
        executor.block_on({
            let world = Rc::clone(&world);
            let guest_slots = Rc::clone(&guest_slots);
            async move {
                let supervisor = spawn(recovery_supervisor(
                    Rc::clone(&world),
                    guest_states,
                    Rc::clone(&guest_slots),
                    events,
                    config,
                ));
                for number in 1..=RECOVERY_CONCURRENCY {
                    AdminIo::emit_admin_event(
                        world.as_ref(),
                        AdminEvent::VsetRecovered {
                            vset: VsetId(u64::try_from(number).expect("vset fits")),
                            verdict: blockd_core::protocol::Verdict::Unrestorable,
                        },
                    )
                    .await;
                }
                let mut replies = Vec::new();
                for _ in 0..RECOVERY_CONCURRENCY {
                    let request = AdminIo::next_admin(world.as_ref())
                        .await
                        .expect("restore request");
                    let (call, reply) = request.into_parts();
                    assert!(matches!(call, AdminCall::RestoreVset { .. }));
                    replies.push(reply);
                }
                AdminIo::emit_admin_event(
                    world.as_ref(),
                    AdminEvent::VsetRecovered {
                        vset: queued,
                        verdict: blockd_core::protocol::Verdict::ColdBoot,
                    },
                )
                .await;
                for mut reply in replies {
                    let _ = reply.send(Err(AdminError::Busy));
                }
                delay(1).await;
                assert!(
                    guest_slots
                        .borrow()
                        .get(&queued)
                        .is_some_and(Option::is_some)
                );
                drop(supervisor);
            }
        });
    }

    #[test]
    fn checkpoint_schedule_coalesces_one_outstanding_request_per_vset() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        world.set_vmstate(vset, 1);
        let mut executor = Executor::simulation(31);
        executor.block_on({
            let world = Rc::clone(&world);
            async move {
                let schedule = spawn(checkpoint_schedule(Rc::clone(&world), 1, 100, 1));
                let first = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("first checkpoint request");
                delay(5).await;
                let (_, mut first_reply) = first.into_parts();
                let _ = first_reply.send(Err(AdminError::Busy));

                let second = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("next checkpoint after completion");
                let (_, mut second_reply) = second.into_parts();
                let _ = second_reply.send(Err(AdminError::Busy));
                assert!(
                    timeout(0, AdminIo::next_admin(world.as_ref()))
                        .await
                        .is_err()
                );
                drop(schedule);
            }
        });
    }

    #[test]
    fn checkpoint_schedule_drains_an_admitted_request_past_the_horizon() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset = VsetId(1);
        world.set_vmstate(vset, 1);
        let mut executor = Executor::simulation(36);
        executor.block_on({
            let world = Rc::clone(&world);
            async move {
                let schedule = spawn(checkpoint_schedule(Rc::clone(&world), 1, 2, 1));
                let request = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("checkpoint request before horizon");
                let (_, mut reply) = request.into_parts();
                delay(2).await;
                yield_now().await;
                yield_now().await;
                assert!(
                    reply.send(Err(AdminError::Busy)).is_ok(),
                    "the scheduler must retain the admitted reply while draining"
                );
                schedule.await.expect("checkpoint schedule drains");
            }
        });
    }

    #[test]
    fn checkpoint_schedule_bounds_global_concurrency_and_refills_fairly() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let vset_count = u16::try_from(CHECKPOINT_CONCURRENCY + 8).expect("vset count fits");
        for number in 1..=vset_count {
            world.set_vmstate(VsetId(u64::from(number)), 1);
        }
        let mut executor = Executor::simulation(32);
        executor.block_on({
            let world = Rc::clone(&world);
            async move {
                let schedule = spawn(checkpoint_schedule(Rc::clone(&world), 1, 100, vset_count));
                let mut replies = Vec::new();
                for number in 1..=CHECKPOINT_CONCURRENCY {
                    let request = AdminIo::next_admin(world.as_ref())
                        .await
                        .expect("bounded checkpoint request");
                    let (call, reply) = request.into_parts();
                    assert!(matches!(
                        call,
                        AdminCall::Checkpoint { vset, .. }
                            if vset == VsetId(u64::try_from(number).expect("vset fits"))
                    ));
                    replies.push(reply);
                }
                assert!(
                    timeout(0, AdminIo::next_admin(world.as_ref()))
                        .await
                        .is_err()
                );
                let _ = replies[0].send(Err(AdminError::Busy));
                let next = AdminIo::next_admin(world.as_ref())
                    .await
                    .expect("checkpoint refill request");
                assert!(matches!(
                    next.into_parts().0,
                    AdminCall::Checkpoint { vset, .. }
                        if vset
                            == VsetId(
                                u64::try_from(CHECKPOINT_CONCURRENCY + 1).expect("vset fits")
                            )
                ));
                drop(schedule);
            }
        });
    }

    #[test]
    fn horizon_cleanup_restarts_a_host_cancelled_mid_restart() {
        let mut config = config();
        config.vset_count = 1;
        config.horizon = millis(100);
        config.faults.crash_at = vec![millis(99)];
        config.faults.restart_delay = (millis(50), millis(50));
        let report = run(33, config);
        assert_eq!(report.crashes, 1);
        assert!(report.cold_boots + report.resumes > 0);
        assert!(report.violations.is_empty(), "{:?}", report.violations);
    }

    #[test]
    fn overlapping_restart_requests_create_only_one_replacement_host() {
        let config = config();
        let network = Rc::new(SimNetwork::default());
        let world = SimWorld::new(config.host.host, config.blobs, config.store, &network);
        let state = Rc::new(RefCell::new(HostState::new(config.host.clone())));
        let state_slot = Rc::new(RefCell::new(Rc::clone(&state)));
        let host_slot = Rc::new(RefCell::new(None));
        let guest_slots = Rc::new(RefCell::new(BTreeMap::new()));
        let events = Rc::new(RunEvents::default());
        let mut executor = Executor::simulation(35);
        *host_slot.borrow_mut() =
            Some(executor.spawn(host_actor_with_state(state, Rc::clone(&world))));
        for _ in 0..2 {
            let world = Rc::clone(&world);
            let host_slot = Rc::clone(&host_slot);
            let state_slot = Rc::clone(&state_slot);
            let guest_slots = Rc::clone(&guest_slots);
            let events = Rc::clone(&events);
            let host_config = config.host.clone();
            executor
                .spawn(async move {
                    crash_and_restart(
                        &world,
                        &host_slot,
                        &state_slot,
                        &guest_slots,
                        events.as_ref(),
                        &host_config,
                        (1, 1),
                    )
                    .await;
                })
                .detach();
        }
        executor.run_until(10);
        assert_eq!(events.crashes.get(), 1);
        assert!(host_slot.borrow().is_some());
    }

    #[test]
    fn resume_oracle_rejects_a_different_historical_page_version() {
        assert!(!recovery_read_is_valid(
            false,
            RecoveryExpectation::Exact,
            7,
        ));
        assert!(recovery_read_is_valid(
            false,
            RecoveryExpectation::Crash {
                sequence_is_possible: true,
                durable_floor: 5,
                pattern_matches: true,
            },
            7,
        ));
    }

    #[test]
    fn resume_oracle_accepts_only_pages_unknown_at_the_checkpoint_boundary() {
        let page = PageId {
            volume: VolumeId {
                vset: VsetId(1),
                idx: VolumeIdx(1),
            },
            page: PageNo(2),
        };
        let expected = page_pattern(page, 3);
        let actual = page_pattern(page, 4);
        let known = OracleSnapshot {
            pages: BTreeMap::from([(page, expected.clone())]),
            unknown: BTreeSet::new(),
        };
        let unknown = OracleSnapshot {
            pages: BTreeMap::from([(page, expected)]),
            unknown: BTreeSet::from([page]),
        };

        assert!(!narrow_exact_candidates(&mut vec![known], page, &actual));
        assert!(narrow_exact_candidates(&mut vec![unknown], page, &actual));
    }
}
