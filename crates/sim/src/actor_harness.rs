//! Single-host deterministic runs over the async actor core.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::rc::Rc;

use blockd_core::engine::{HostState, host_actor_with_state};
use blockd_core::hostmeta::{Counters, HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::{AdminCmd, AdminReply, ReqId};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, page_size};
use blockd_exec::rng::Ppm;
use blockd_exec::{Executor, OneOf3, TaskHandle, delay, now, random_u64, select3, spawn};
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
    world.enqueue_admin(AdminCmd::CreateVset {
        req: ReqId(1),
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
            match select3(
                world.next_admin_reply(),
                world.next_abort(),
                passive_world.next_abort(),
            )
            .await
            {
                OneOf3::First(reply) => reply,
                OneOf3::Second(reason) => panic!("primary aborted during creation: {reason:?}"),
                OneOf3::Third(reason) => panic!("passive aborted during creation: {reason:?}"),
            }
        }
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
    for number in 1..=config.vset_count {
        let vset = VsetId(u64::from(number));
        let req = ReqId(u64::from(number));
        world.enqueue_admin(AdminCmd::CreateVset {
            req,
            vset,
            config: config.vset,
            from_base: None,
        });
    }
    executor.block_on({
        let world = Rc::clone(&world);
        let passive_world = Rc::clone(&passive_world);
        async move {
            let mut created = BTreeSet::new();
            while created.len() < usize::from(config.vset_count) {
                match select3(
                    world.next_admin_reply(),
                    world.next_abort(),
                    passive_world.next_abort(),
                )
                .await
                {
                    OneOf3::First(Some(AdminReply::VsetCreated { req, vset }))
                        if req.0 == vset.0
                            && (1..=u64::from(config.vset_count)).contains(&vset.0) =>
                    {
                        created.insert(vset);
                    }
                    OneOf3::First(Some(AdminReply::AdminFailed { req })) => {
                        panic!("vset creation failed for {req:?}")
                    }
                    OneOf3::First(Some(_)) => {}
                    OneOf3::First(None) => {
                        panic!("admin reply stream closed during creation")
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
    for guest in guest_slots.borrow_mut().values_mut() {
        if let Some(mut guest) = guest.take() {
            guest.cancel();
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
    world.enqueue_admin(AdminCmd::CreateVset {
        req: ReqId(1),
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
            match select3(
                world.next_admin_reply(),
                world.next_abort(),
                passive_world.next_abort(),
            )
            .await
            {
                OneOf3::First(reply) => reply,
                OneOf3::Second(reason) => panic!("primary aborted during creation: {reason:?}"),
                OneOf3::Third(reason) => panic!("passive aborted during creation: {reason:?}"),
            }
        }
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

async fn recovery_supervisor(
    world: Rc<SimWorld>,
    guest_states: Rc<BTreeMap<VsetId, Rc<GuestState>>>,
    guest_slots: GuestSlots,
    events: Rc<RunEvents>,
    config: ActorHarnessConfig,
) {
    while let Some(reply) = world.next_admin_reply().await {
        let (vset, verdict, local_recovery, restored) = match reply {
            AdminReply::VsetRecovered { vset, verdict } => (vset, verdict, true, false),
            AdminReply::VsetRestored { vset, verdict, .. } => (vset, verdict, false, true),
            AdminReply::VsetMigratedIn { vset, verdict }
            | AdminReply::VsetForked { vset, verdict, .. } => (vset, verdict, false, false),
            _ => continue,
        };
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
                events
                    .unrestorable
                    .set(events.unrestorable.get().saturating_add(1));
                if local_recovery {
                    world.enqueue_admin(AdminCmd::RestoreVset {
                        req: ReqId(u64::MAX.saturating_sub(vset.0)),
                        vset,
                    });
                }
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
    loop {
        delay(interval).await;
        if now() > horizon {
            return;
        }
        for number in 1..=vset_count {
            let vset = VsetId(u64::from(number));
            if !world.vmstate_ready(vset) {
                continue;
            }
            world.enqueue_admin(AdminCmd::Checkpoint {
                req: ReqId(req),
                vset,
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
                AdminIo::reply_admin(
                    world.as_ref(),
                    AdminReply::VsetRecovered {
                        vset,
                        verdict: blockd_core::protocol::Verdict::Unrestorable,
                    },
                )
                .await;
                let command = AdminIo::next_admin(world.as_ref()).await;
                drop(supervisor);
                command
            }
        });
        assert!(matches!(
            command,
            Some(AdminCmd::RestoreVset { vset: found, .. }) if found == vset
        ));
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
