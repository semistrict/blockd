//! Multi-host deterministic harness over the shared async actor worlds.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::time::Duration;

use blockd_core::engine::{HostState, host_actor_with_state};
use blockd_core::head::HeadRecord;
use blockd_core::hostmeta::{Counters, HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::VolumeConfig;
use blockd_core::layout;
use blockd_core::manifest::Manifest;
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::{AdminCall, AdminEvent, AdminResult, AdminSuccess, ReqId, Verdict};
use blockd_core::replica_recovery::{
    ReplicaResidue, export_replica_recovery, prepare_replica_publication,
    prepare_replica_recovery_claim,
};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, millis, page_size};
use blockd_core::world::{Store, StoreError};
use blockd_exec::channel::{OneReceiver, OneSender, oneshot, unbounded};
use blockd_exec::inject::{Injector, Lane, injector};
use blockd_exec::rng::Ppm;
use blockd_exec::{
    Either, FaultConfig, Response, SimulationContext, TaskHandle, TaskSet, delay, now,
    random_between, random_hit, random_u64, select2, spawn,
};

use crate::guest::page_pattern;
use crate::model::{BlobDevConfig, CrashFate, StoreConfig, StoreCounters};
use crate::peer_transport::{PeerTransport, PeerTransportFaults, PeerTransportStats};
use crate::world::SimWorld;

type SharedState = Rc<RefCell<HostState>>;
type StateSlots = Rc<Vec<RefCell<SharedState>>>;
type GuestSlots = Rc<RefCell<BTreeMap<VolumeId, Option<TaskHandle<()>>>>>;
type HostCommands = Rc<RefCell<VecDeque<HostCommand>>>;
#[cfg(test)]
const CHECKPOINT_CONCURRENCY: usize = crate::checkpoint_schedule::CONCURRENCY;
const CREATION_CONCURRENCY: usize = 32;
const MIGRATION_CONCURRENCY: usize = 8;
const RESTORE_CONCURRENCY: usize = 32;

pub use blockd_exec::FaultPoint;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sabotage {
    EagerHandoffAck,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum PeerKind {
    Offer = 0,
    Accept = 1,
    FetchRange = 2,
    Page = 3,
    Released = 6,
    ReleasedAck = 7,
    ReplicaPut = 8,
    ReplicaPutAck = 9,
    ReplicaCommit = 10,
    ReplicaCommitAck = 11,
    ReplicaStatus = 12,
    ReplicaStatusReply = 13,
    ReplicaRelease = 15,
    ReplicaReleaseAck = 16,
    VnodeAdopt = 18,
    VnodeAdoptAck = 19,
    VnodeFetchClosure = 20,
    VnodeClosure = 21,
    VnodeCommit = 22,
    VnodeCommitAck = 23,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub hosts: u16,
    pub daemon: HostConfig,
    pub bdev: BlobDevConfig,
    pub store: StoreConfig,
    pub volume_count: u16,
    pub volume_config: VolumeConfig,
    pub horizon: u64,
    pub think: (u64, u64),
    pub checkpoint_interval: Option<u64>,
    pub kill_hosts_at: Vec<(u64, u16)>,
    pub crash_hosts_at: Vec<(u64, u16)>,
    pub restart_delay: (u64, u64),
    pub crash_mean_interval: u64,
    pub migrate_mean_interval: u64,
    pub peer_drop: (u64, u64),
    pub peer_dup: (u64, u64),
    pub peer_link_outages: Vec<(u64, u64, u16, u16)>,
    pub fault_points: Vec<FaultPoint>,
    pub store_outage: Option<(u64, u64)>,
    pub drop_peer: Option<(PeerKind, u64, u64)>,
    pub race_restore: bool,
    pub migrate_at: Vec<(u64, VolumeId, u16)>,
    pub sabotage: Option<Sabotage>,
    pub guest_sync_share: Option<Ppm>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClusterReport {
    pub trace_hash: u64,
    pub violations: Vec<String>,
    pub audit_runs: u64,
    pub audited_volumes: u64,
    pub audited_pages: u64,
    pub completed_ops: u64,
    pub restores: u64,
    pub claims_lost: u64,
    pub guest_deaths: u64,
    pub loss_bound_verified: u64,
    pub migrations: u64,
    pub max_restore_ns: u64,
    pub max_migration_pause_ns: u64,
    pub peer_drops: u64,
    pub peer_dups: u64,
    pub peer_link_clogs: u64,
    pub host_crashes: u64,
    pub disk_crash_applied: u64,
    pub disk_crash_dropped: u64,
    pub disk_crash_torn: u64,
    pub disk_bitflips: u64,
    pub store_unavailable: u64,
    pub store_cas_conflicts: u64,
    pub fault_coverage: BTreeMap<FaultPoint, u64>,
    pub sync_samples: u64,
    pub sync_latency_p50_ns: u64,
    pub sync_latency_p95_ns: u64,
    pub sync_latency_p99_ns: u64,
    pub sync_latency_max_ns: u64,
    pub recoveries: u64,
    pub releases: u64,
    pub migrations_refused: u64,
    pub hydrate_fills: u64,
    pub store_retries: u64,
    pub nemesis_drops: u64,
    pub wedged_guests: u64,
    pub wedged_hydration: u64,
    pub wedged_outbound: u64,
    pub parked_end: usize,
    pub hydrating_end: usize,
    pub replica_bytes: u64,
    pub replica_commits: u64,
    pub replica_unlinks: u64,
    pub replica_network_bytes: u64,
    pub replica_logical_bytes: u64,
    pub replica_nonactive_bytes: u64,
    pub replica_replacement_bytes: u64,
    pub replica_artifact_flushes: u64,
    pub replica_commit_flushes: u64,
    pub replica_rotations: u64,
    pub replica_capacity_backpressure: u64,
    pub published_blx_bytes: u64,
    pub published_live_entry_bytes: u64,
    pub published_dead_entry_bytes: u64,
    pub published_blx_overhead_bytes: u64,
    pub blx_files_compacted: u64,
    pub pages_compacted: u64,
    pub store: StoreCounters,
    pub blobs_per_host: Vec<usize>,
    pub primary_blobs_per_host: Vec<usize>,
}

enum HostCommand {
    Crash(HostId, OneSender<()>),
    Bounce(HostId, OneSender<()>),
}

#[derive(Default)]
struct GuestState {
    completed: Cell<u64>,
    total_completed: Cell<u64>,
    expected: RefCell<BTreeMap<PageId, Vec<u8>>>,
    durable: RefCell<BTreeMap<PageId, Vec<u8>>>,
    written: RefCell<BTreeMap<PageId, BTreeSet<u64>>>,
    recovering: RefCell<BTreeSet<PageId>>,
    volume_sequences: RefCell<BTreeMap<VolumeId, u64>>,
    violations: RefCell<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MigrationAttempt {
    from: u16,
    to: u16,
    from_incarnation: u64,
    to_incarnation: u64,
    started: u64,
}

struct Control {
    placement: BTreeMap<VolumeId, u16>,
    guests: GuestSlots,
    guest_state: BTreeMap<VolumeId, Rc<GuestState>>,
    migrations: BTreeMap<VolumeId, MigrationAttempt>,
    uncertain_migrations: BTreeSet<VolumeId>,
    accepted_migrations: BTreeSet<VolumeId>,
    deferred_source_recoveries: BTreeMap<VolumeId, (u16, Verdict)>,
    deferred_destination_recoveries: BTreeMap<VolumeId, (u16, Verdict)>,
    quiescing_guests: BTreeSet<VolumeId>,
    migration_cuts: BTreeSet<VolumeId>,
    live: Vec<bool>,
    up: Vec<bool>,
    workload_end: Cell<u64>,
    report: ClusterReport,
    sync_latencies: Vec<u64>,
    retired_counters: Vec<Counters>,
}

fn migration_pause_ns(attempt: MigrationAttempt, worlds: &[Rc<SimWorld>]) -> u64 {
    worlds[usize::from(attempt.from)]
        .max_pause_ns()
        .max(now().saturating_sub(attempt.started))
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run(seed: u64, mut config: ClusterConfig) -> ClusterReport {
    assert!(config.hosts > 0, "cluster requires at least one host");
    if config.daemon.replica_placement.is_none() {
        config.daemon.replica_placement = Some(ReplicaPlacementConfig {
            membership_epoch: 1,
            local_failure_domain: 0,
            roster: (0..config.hosts)
                .map(|host| PeerCandidate {
                    host: HostId(host),
                    weight: 1,
                    failure_domain: host,
                    drained: false,
                })
                .collect(),
            authority: None,
        });
    }

    let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
    let worlds = Rc::new(worlds);
    if config.sabotage == Some(Sabotage::EagerHandoffAck) {
        for world in worlds.iter() {
            world.set_drop_handoff_writes(true);
        }
    }
    let states = Rc::new(
        (0..config.hosts)
            .map(|host| {
                RefCell::new(Rc::new(RefCell::new(HostState::new(host_config(
                    &config, host,
                )))))
            })
            .collect::<Vec<_>>(),
    );
    let guest_slots: GuestSlots = Rc::new(RefCell::new(BTreeMap::new()));
    let guest_state = (1..=config.volume_count)
        .map(|number| (VolumeId(u64::from(number)), Rc::new(GuestState::default())))
        .collect();
    let control = Rc::new(RefCell::new(Control {
        placement: BTreeMap::new(),
        guests: Rc::clone(&guest_slots),
        guest_state,
        migrations: BTreeMap::new(),
        uncertain_migrations: BTreeSet::new(),
        accepted_migrations: BTreeSet::new(),
        deferred_source_recoveries: BTreeMap::new(),
        deferred_destination_recoveries: BTreeMap::new(),
        quiescing_guests: BTreeSet::new(),
        migration_cuts: BTreeSet::new(),
        live: vec![true; usize::from(config.hosts)],
        up: vec![true; usize::from(config.hosts)],
        workload_end: Cell::new(0),
        report: ClusterReport::default(),
        sync_latencies: Vec::new(),
        retired_counters: Vec::new(),
    }));
    let mut fault_config = FaultConfig::disabled();
    for &point in &config.fault_points {
        fault_config.force(point, [true]);
    }
    let contexts = Rc::new(
        (0..=config.hosts)
            .map(|host| {
                SimulationContext::new(
                    seed.wrapping_add(u64::from(host).wrapping_mul(0x9e37_79b9)),
                    fault_config.clone(),
                )
                .semantic_trace_only()
            })
            .collect::<Vec<_>>(),
    );
    let commands: HostCommands = Rc::new(RefCell::new(VecDeque::new()));
    let peer_stats = Rc::new(PeerTransportStats::default());
    let peer_roster = (0..config.hosts)
        .map(|host| (HostId(host), format!("host-{host}")))
        .collect::<BTreeMap<_, _>>();
    let peer_faults = PeerTransportFaults {
        duplicate_odds: config.peer_dup,
        targeted_drop: config
            .drop_peer
            .map(|(kind, begin, end)| (kind as u8, begin, end)),
        max_frames_per_connection: 1,
    };
    let config = Rc::new(config);

    // One conceptual millisecond per outer step lets lifecycle commands reach
    // Turmoil between host polls without changing the actor clock's nanosecond API.
    let tick = Duration::from_millis(millis(1));
    let duration = Duration::from_millis(config.horizon.saturating_add(5 * millis(1_000)));
    let output = Rc::new(RefCell::new(None));
    let client_output = Rc::clone(&output);
    let fail_rate = if config.peer_drop.1 == 0 {
        0.0
    } else {
        let numerator = u32::try_from(config.peer_drop.0).expect("peer drop numerator fits u32");
        let denominator =
            u32::try_from(config.peer_drop.1).expect("peer drop denominator fits u32");
        f64::from(numerator) / f64::from(denominator) / 16.0
    };
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .simulation_duration(duration)
        .tick_duration(tick)
        // Frame-scoped peer links may have one bounded connection wave from
        // every other host. Turmoil's smaller default backlog panics instead
        // of applying backpressure when that valid wave arrives together.
        .tcp_capacity(
            usize::from(config.hosts).saturating_mul(crate::peer_transport::MAX_IN_FLIGHT),
        )
        .min_message_latency(Duration::from_secs(1))
        .max_message_latency(Duration::from_secs(100))
        .fail_rate(fail_rate);
    let mut sim = builder.build();

    for host in 0..config.hosts {
        let host_name = format!("host-{host}");
        let config = Rc::clone(&config);
        let world = Rc::clone(&worlds[usize::from(host)]);
        let states = Rc::clone(&states);
        let context = contexts[usize::from(host) + 1].clone();
        let roster = peer_roster.clone();
        let transport_stats = Rc::clone(&peer_stats);
        sim.host(host_name, move || {
            let state = Rc::new(RefCell::new(HostState::new(host_config(&config, host))));
            *states[usize::from(host)].borrow_mut() = Rc::clone(&state);
            let world = Rc::clone(&world);
            let context = context.clone();
            let config = Rc::clone(&config);
            let roster = roster.clone();
            let transport_stats = Rc::clone(&transport_stats);
            async move {
                let transport = PeerTransport::start(
                    host_config(&config, host).host,
                    roster,
                    peer_faults,
                    transport_stats,
                )
                .await?;
                let _attachment = world.attach_peer_transport(transport);
                context.scope(host_actor_with_state(state, world)).await;
                Ok(())
            }
        });
    }

    let client_context = contexts[0].clone();
    let client_config = Rc::clone(&config);
    let client_worlds = Rc::clone(&worlds);
    let client_states = Rc::clone(&states);
    let client_control = Rc::clone(&control);
    let client_commands = Rc::clone(&commands);
    let client_contexts = Rc::clone(&contexts);
    let client_peer_stats = Rc::clone(&peer_stats);
    sim.client("cluster-controller", async move {
        let report = client_context
            .scope(run_inner(
                client_config,
                client_worlds,
                client_states,
                client_control,
                client_commands,
                client_contexts,
                client_peer_stats,
            ))
            .await;
        *client_output.borrow_mut() = Some(report);
        Ok(())
    });

    loop {
        let finished = sim
            .step()
            .unwrap_or_else(|error| panic!("Turmoil cluster simulation seed {seed}: {error}"));
        while let Some(command) = commands.borrow_mut().pop_front() {
            match command {
                HostCommand::Crash(host, complete) => {
                    sim.crash(format!("host-{}", host.0));
                    let _ = complete.send(());
                }
                HostCommand::Bounce(host, complete) => {
                    sim.bounce(format!("host-{}", host.0));
                    let _ = complete.send(());
                }
            }
        }
        if finished {
            break;
        }
    }
    output
        .borrow_mut()
        .take()
        .expect("Turmoil cluster client completed")
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
async fn run_inner(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
    contexts: Rc<Vec<SimulationContext>>,
    peer_stats: Rc<PeerTransportStats>,
) -> ClusterReport {
    let guest_slots = Rc::clone(&control.borrow().guests);

    for host in 0..config.hosts {
        spawn(lifecycle_actor(
            host,
            Rc::clone(&worlds[usize::from(host)]),
            Rc::clone(&control),
            Rc::clone(&worlds),
            Rc::clone(&config),
        ))
        .detach();
        spawn(abort_monitor(
            host,
            Rc::clone(&config),
            Rc::clone(&worlds),
            Rc::clone(&states),
            Rc::clone(&control),
            Rc::clone(&commands),
        ))
        .detach();
    }

    let initial_volumes =
        create_initial_volumes(Rc::clone(&config), Rc::clone(&worlds), Rc::clone(&control)).await;
    let workload_end = now().saturating_add(config.horizon);
    control.borrow().workload_end.set(workload_end);
    let simulation_end = workload_end.saturating_add(2 * millis(1_000));

    spawn_schedules(
        &config,
        Rc::clone(&worlds),
        Rc::clone(&states),
        Rc::clone(&control),
        Rc::clone(&commands),
        Rc::clone(&peer_stats),
    );
    let guest_config = Rc::clone(&config);
    let guest_worlds = Rc::clone(&worlds);
    let guest_control = Rc::clone(&control);
    async move {
        for (volume, host) in initial_volumes {
            start_guest(volume, host, &guest_control, &guest_worlds, &guest_config);
        }
    }
    .await;
    delay(simulation_end.saturating_sub(now())).await;

    let audit = audit_cluster(
        Rc::clone(&config),
        Rc::clone(&worlds),
        Rc::clone(&states),
        Rc::clone(&control),
    )
    .await;
    {
        let mut control = control.borrow_mut();
        control.report.audit_runs = 1;
        control.report.audited_volumes = audit.volumes;
        control.report.audited_pages = audit.pages;
        control.report.violations.extend(audit.violations);
    }

    for guest in guest_slots.borrow_mut().values_mut() {
        if let Some(mut guest) = guest.take() {
            guest.cancel();
        }
    }
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    let mut control = control.borrow_mut();
    control.report.trace_hash = contexts.iter().fold(0, |trace, context| {
        trace.rotate_left(7) ^ context.trace_hash()
    });
    for context in contexts.iter() {
        for (point, hits) in context.fault_hits() {
            *control.report.fault_coverage.entry(point).or_default() += hits;
        }
    }
    control.report.completed_ops = control
        .guest_state
        .values()
        .map(|guest| guest.total_completed.get())
        .sum();
    let violations = control
        .guest_state
        .values()
        .flat_map(|guest| std::mem::take(&mut *guest.violations.borrow_mut()))
        .collect::<Vec<_>>();
    control.report.violations.extend(violations);
    let mut counters = control.retired_counters.clone();
    counters.extend(states.iter().map(|state| state.borrow().borrow().counters));
    summarize_counters(&mut control.report, &counters);
    control.report.parked_end = states
        .iter()
        .filter(|state| control.live[usize::from(state.borrow().borrow().config.host.0)])
        .map(|state| state.borrow().borrow().stats().pressure_waiting_faults)
        .sum();
    control.report.hydrating_end = states
        .iter()
        .filter(|state| control.live[usize::from(state.borrow().borrow().config.host.0)])
        .map(|state| {
            state
                .borrow()
                .borrow()
                .stats()
                .volumes
                .iter()
                .filter(|volume| {
                    matches!(volume.role, blockd_core::hostmeta::VolumeRole::Hydrating)
                })
                .count()
        })
        .sum();
    control.sync_latencies.sort_unstable();
    let sync_latencies = control.sync_latencies.clone();
    summarize_latencies(&mut control.report, &sync_latencies);
    let (drops, dups, clogs, targeted, releases) = peer_stats.snapshot();
    control.report.peer_drops = drops;
    control.report.peer_dups = dups;
    control.report.peer_link_clogs = clogs;
    control.report.nemesis_drops = targeted;
    control.report.releases = releases;
    let (unavailable, conflicts) = worlds[0].store_counters();
    control.report.store_unavailable = unavailable;
    control.report.store_cas_conflicts = conflicts;
    control.report.store = worlds[0].store_metrics();
    (
        control.report.published_blx_bytes,
        control.report.published_live_entry_bytes,
        control.report.published_dead_entry_bytes,
        control.report.published_blx_overhead_bytes,
    ) = worlds[0].published_archive_metrics();
    control.report.disk_bitflips = worlds.iter().map(|world| world.bitflips()).sum();
    control.report.blobs_per_host = worlds.iter().map(|world| world.blob_count()).collect();
    control.report.primary_blobs_per_host = worlds
        .iter()
        .map(|world| world.primary_blob_count())
        .collect();
    std::mem::take(&mut control.report)
}

#[derive(Default)]
struct AuditReport {
    volumes: u64,
    pages: u64,
    violations: Vec<String>,
}

#[allow(clippy::too_many_lines)]
async fn audit_cluster(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
) -> AuditReport {
    let mut audit = AuditReport::default();
    let store = worlds[0].store_snapshot();
    for number in 1..=config.volume_count {
        let volume = VolumeId(u64::from(number));
        let Some(placed) = control.borrow().placement.get(&volume).copied() else {
            audit
                .violations
                .push(format!("final audit found no placement for {volume:?}"));
            continue;
        };
        if !control.borrow().live[usize::from(placed)] || !control.borrow().up[usize::from(placed)]
        {
            audit.violations.push(format!(
                "final audit placement for {volume:?} points at unavailable host {placed}"
            ));
            continue;
        }

        let mut authorities = Vec::new();
        for host in 0..config.hosts {
            let live_and_up = {
                let control = control.borrow();
                control.live[usize::from(host)] && control.up[usize::from(host)]
            };
            if !live_and_up {
                continue;
            }
            let state = states[usize::from(host)].borrow();
            let state = state.borrow();
            if state
                .volumes
                .get(&volume)
                .is_some_and(|volume_state| volume_state.ready && volume_state.outbound.is_none())
            {
                authorities.push(host);
            }
        }
        if authorities.len() > 1 {
            audit.violations.push(format!(
                "final audit found multiple authorities for {volume:?}: {authorities:?}"
            ));
            continue;
        }
        let lifecycle_in_progress = {
            let control = control.borrow();
            control.migrations.contains_key(&volume)
                || control.quiescing_guests.contains(&volume)
                || control.migration_cuts.contains(&volume)
                || control.deferred_source_recoveries.contains_key(&volume)
                || control
                    .deferred_destination_recoveries
                    .contains_key(&volume)
        };
        let Some(&authority) = authorities.first() else {
            if !lifecycle_in_progress {
                audit.violations.push(format!(
                    "final audit expected authority {placed} for {volume:?}, found none"
                ));
            }
            continue;
        };
        if authority != placed && !lifecycle_in_progress {
            audit.violations.push(format!(
                "final audit expected authority {placed} for {volume:?}, found {authority}"
            ));
            continue;
        }

        {
            let state = states[usize::from(authority)].borrow();
            let state = state.borrow();
            let Some(volume_state) = state.volumes.get(&volume) else {
                audit.violations.push(format!(
                    "final audit authority {placed} has no state for {volume:?}"
                ));
                continue;
            };
            if volume_state.local_covered_through < volume_state.sync_ack_through {
                audit.violations.push(format!(
                    "final audit found local coverage {} behind acknowledged sync {} for {volume:?}",
                    volume_state.local_covered_through, volume_state.sync_ack_through
                ));
            }
            let archived_through = volume_state
                .backed
                .and_then(|pointer| store.get(&pointer.manifest_key(volume)))
                .and_then(|bytes| Manifest::decode(volume, bytes).ok())
                .map_or(0, |manifest| manifest.sync_covered_through);
            let protected_through = volume_state.peer_committed_through.max(archived_through);
            if protected_through < volume_state.sync_ack_through {
                audit.violations.push(format!(
                    "final audit found protected coverage {protected_through} behind acknowledged sync {} for {volume:?}",
                    volume_state.sync_ack_through
                ));
            }
        }

        if let Some(head_bytes) = store.get(&layout::head_key(volume)) {
            match HeadRecord::decode(volume, head_bytes) {
                Ok(head) => {
                    if head.holder != HostId(placed) {
                        audit.violations.push(format!(
                            "final audit head for {volume:?} names {:?}, placement names {:?}",
                            head.holder,
                            HostId(placed)
                        ));
                    }
                    if let Some(pointer) = head.manifest {
                        let key = pointer.manifest_key(volume);
                        match store
                            .get(&key)
                            .and_then(|bytes| Manifest::decode(volume, bytes).ok())
                        {
                            Some(manifest)
                                if (
                                    manifest.writer_fence,
                                    manifest.archive_seq,
                                    manifest.capture_seq,
                                ) == (pointer.fence, pointer.seq.0, pointer.capture_seq) => {}
                            _ => audit.violations.push(format!(
                                "final audit could not verify head manifest {key} for {volume:?}"
                            )),
                        }
                    }
                }
                Err(_) => audit
                    .violations
                    .push(format!("final audit could not decode head for {volume:?}")),
            }
        }

        let guest = Rc::clone(&control.borrow().guest_state[&volume]);
        let world = &worlds[usize::from(authority)];
        let violations_before = audit.violations.len();
        if config.sabotage == Some(Sabotage::EagerHandoffAck) {
            audit.volumes = audit.volumes.saturating_add(1);
            continue;
        }
        for volume in std::iter::once(volume) {
            for page_number in 0..config.volume_config.pages {
                let page = PageId {
                    volume,
                    page: PageNo(page_number),
                };
                let fault_failure =
                    match select2(world.fault(page, false), delay(millis(2_000))).await {
                        Either::First(true) => None,
                        Either::First(false) => Some("was rejected"),
                        Either::Second(()) => Some("timed out"),
                    };
                world.set_vmstate(volume, guest.completed.get());
                if let Some(reason) = fault_failure {
                    audit.violations.push(format!(
                        "final audit fault {reason} for {page:?} on authority {authority}"
                    ));
                    continue;
                }
                audit.pages = audit.pages.saturating_add(1);
                let actual = world
                    .page_bytes(page)
                    .unwrap_or_else(|| vec![0; page_size()]);
                if let Err(reason) = validate_page_bytes(&guest, page, &actual) {
                    audit.violations.push(format!(
                        "final audit rejected {page:?} on authority {authority}: {reason}"
                    ));
                }
            }
        }
        if audit.violations.len() == violations_before {
            audit.volumes = audit.volumes.saturating_add(1);
        }
    }
    audit
}

#[allow(clippy::too_many_arguments)]
async fn abort_monitor(
    host: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
) {
    while worlds[usize::from(host)].next_abort().await.is_some() {
        crash_host(host, &config, &worlds, &states, &control, &commands).await;
    }
}

fn host_config(config: &ClusterConfig, host: u16) -> HostConfig {
    HostConfig {
        archive: config.daemon.archive,
        host: HostId(host),
        cache_pages: config.daemon.cache_pages,
        writeback_interval: config.daemon.writeback_interval,
        backup_retry: config.daemon.backup_retry,
        disk_capacity: config.daemon.disk_capacity,
        disk_headroom: config.daemon.disk_headroom,
        wedge_ticks: config.daemon.wedge_ticks,
        replica_placement: config.daemon.replica_placement.as_ref().map(|placement| {
            let local_failure_domain = placement
                .roster
                .iter()
                .find(|candidate| candidate.host == HostId(host))
                .map_or(placement.local_failure_domain, |candidate| {
                    candidate.failure_domain
                });
            ReplicaPlacementConfig {
                membership_epoch: placement.membership_epoch,
                local_failure_domain,
                roster: placement.roster.clone(),
                authority: placement.authority,
            }
        }),
    }
}

fn volume_config(config: &ClusterConfig, _volume: VolumeId) -> blockd_core::journal::VolumeConfig {
    config.volume_config
}

async fn create_initial_volumes(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) -> Vec<(VolumeId, u16)> {
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = 0usize;
    let mut next = 1u64;
    let mut created_volumes = Vec::with_capacity(usize::from(config.volume_count));
    while next <= u64::from(config.volume_count) || active != 0 {
        while active < CREATION_CONCURRENCY && next <= u64::from(config.volume_count) {
            let number = u16::try_from(next).expect("volume number fits");
            next += 1;
            let volume = VolumeId(u64::from(number));
            let host = (number - 1) % config.hosts;
            control.borrow_mut().placement.insert(volume, host);
            let reply = worlds[usize::from(host)].request_admin(AdminCall::CreateVolume {
                volume,
                config: volume_config(&config, volume),
                from_base: None,
            });
            let completed = completed.clone();
            actors.spawn(async move {
                let created = matches!(
                    reply.await,
                    Ok(Ok(AdminSuccess::VolumeCreated { volume: created_volume }))
                        if created_volume == volume
                );
                let _ = completed.send((volume, host, created));
            });
            active += 1;
        }
        if active != 0 {
            let (volume, host, created) = completions
                .recv()
                .await
                .expect("initial creation workers remain connected");
            assert!(created, "initial volume creation failed for {volume:?}");
            created_volumes.push((volume, host));
            active -= 1;
        }
    }
    created_volumes.sort_unstable_by_key(|(volume, _)| *volume);
    created_volumes
}

#[allow(clippy::too_many_lines)]
async fn lifecycle_actor(
    host: u16,
    world: Rc<SimWorld>,
    control: Rc<RefCell<Control>>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    config: Rc<ClusterConfig>,
) {
    while let Some(event) = world.next_admin_event().await {
        match event {
            AdminEvent::VolumeMigratedIn { volume, verdict } => {
                let attempt = control
                    .borrow()
                    .migrations
                    .get(&volume)
                    .copied()
                    .filter(|attempt| attempt.to == host);
                if let Some(attempt) = attempt {
                    discard_deferred_source_recovery(volume, attempt, &control);
                }
                let migrated = control.borrow().guest_state[&volume]
                    .expected
                    .borrow()
                    .clone();
                *control.borrow().guest_state[&volume].durable.borrow_mut() = migrated;
                let runnable = prepare_recovered(
                    &control.borrow().guest_state[&volume],
                    volume,
                    &config,
                    world.as_ref(),
                    verdict,
                );
                {
                    let mut control = control.borrow_mut();
                    if let Some(attempt) = attempt {
                        control.migrations.remove(&volume);
                        control.uncertain_migrations.remove(&volume);
                        control.accepted_migrations.remove(&volume);
                        control.deferred_destination_recoveries.remove(&volume);
                        control.report.migrations = control.report.migrations.saturating_add(1);
                        control.report.max_migration_pause_ns = control
                            .report
                            .max_migration_pause_ns
                            .max(migration_pause_ns(attempt, &worlds));
                    }
                    control.placement.insert(volume, host);
                }
                if runnable {
                    start_guest(volume, host, &control, &worlds, &config);
                }
            }
            AdminEvent::VolumeRecovered { volume, verdict } => {
                let pending_migration = {
                    let control = control.borrow();
                    control.migrations.get(&volume).copied().filter(|attempt| {
                        attempt.to == host
                            && world.incarnation() != attempt.to_incarnation
                            && (control.uncertain_migrations.contains(&volume)
                                || control.accepted_migrations.contains(&volume))
                    })
                };
                if let Some(attempt) = pending_migration {
                    finalize_destination_migration(
                        volume, attempt, host, verdict, &control, &worlds, &config,
                    );
                    continue;
                }
                let pending_destination = control
                    .borrow()
                    .migrations
                    .get(&volume)
                    .copied()
                    .is_some_and(|attempt| {
                        attempt.to == host && world.incarnation() != attempt.to_incarnation
                    });
                if pending_destination {
                    control
                        .borrow_mut()
                        .deferred_destination_recoveries
                        .insert(volume, (host, verdict));
                    continue;
                }
                let returned_to_source = {
                    let control = control.borrow();
                    control.migrations.get(&volume).copied().filter(|attempt| {
                        attempt.from == host
                            && world.incarnation() != attempt.from_incarnation
                            && control.uncertain_migrations.contains(&volume)
                    })
                };
                if let Some(attempt) = returned_to_source {
                    let mut control = control.borrow_mut();
                    if control.migrations.get(&volume) == Some(&attempt) {
                        control.migrations.remove(&volume);
                        control.uncertain_migrations.remove(&volume);
                        control.accepted_migrations.remove(&volume);
                        control.deferred_source_recoveries.remove(&volume);
                        control.deferred_destination_recoveries.remove(&volume);
                        control.report.migrations_refused =
                            control.report.migrations_refused.saturating_add(1);
                    }
                }
                let active_migration = control
                    .borrow()
                    .migrations
                    .get(&volume)
                    .copied()
                    .filter(|attempt| attempt.from == host || attempt.to == host);
                if let Some(attempt) = active_migration {
                    // A recovery notification can already be queued when the
                    // simulator starts a migration. The request completion or
                    // migration lifecycle event owns the resulting placement;
                    // this older notification must not start another runner.
                    if attempt.from == host && world.incarnation() != attempt.from_incarnation {
                        control
                            .borrow_mut()
                            .deferred_source_recoveries
                            .insert(volume, (host, verdict));
                    }
                    continue;
                }
                let already_elsewhere =
                    control
                        .borrow()
                        .placement
                        .get(&volume)
                        .is_some_and(|&placed| {
                            placed != host && control.borrow().live[usize::from(placed)]
                        });
                if already_elsewhere {
                    if !matches!(verdict, Verdict::Unrestorable) {
                        control
                            .borrow_mut()
                            .report
                            .violations
                            .push(format!("two runners recovered for {volume:?}"));
                    }
                    continue;
                }
                let runnable = prepare_recovered(
                    &control.borrow().guest_state[&volume],
                    volume,
                    &config,
                    world.as_ref(),
                    verdict,
                );
                {
                    let mut control = control.borrow_mut();
                    control.report.recoveries = control.report.recoveries.saturating_add(1);
                    if runnable {
                        control.placement.insert(volume, host);
                    }
                }
                if runnable {
                    start_guest(volume, host, &control, &worlds, &config);
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct RestoreTarget {
    host: u16,
    incarnation: u64,
}

async fn restore_completion(
    mut target: RestoreTarget,
    volume: VolumeId,
    sent: u64,
    mut reply: Response<AdminResult>,
    control: Rc<RefCell<Control>>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    config: Rc<ClusterConfig>,
) {
    let (target, reply) = loop {
        let result = reply.await;
        let host_is_current = {
            let control = control.borrow();
            control.live[usize::from(target.host)]
                && control.up[usize::from(target.host)]
                && worlds[usize::from(target.host)].incarnation() == target.incarnation
        };
        if host_is_current {
            break (target, result);
        }
        let retry = {
            let control = control.borrow();
            let already_restored = control
                .placement
                .get(&volume)
                .is_some_and(|&host| control.live[usize::from(host)]);
            (!already_restored).then(|| {
                (0..config.hosts)
                    .find(|&host| control.live[usize::from(host)] && control.up[usize::from(host)])
            })
        }
        .flatten();
        let Some(host) = retry else {
            return;
        };
        target = RestoreTarget {
            host,
            incarnation: worlds[usize::from(host)].incarnation(),
        };
        reply = worlds[usize::from(host)].request_admin(AdminCall::RestoreVolume { volume });
    };
    let Ok(reply) = reply else {
        let mut control = control.borrow_mut();
        control.report.claims_lost = control.report.claims_lost.saturating_add(1);
        return;
    };
    let Ok(AdminSuccess::VolumeRestored {
        volume: restored,
        verdict,
        ..
    }) = reply
    else {
        if reply.is_err() {
            let mut control = control.borrow_mut();
            control.report.claims_lost = control.report.claims_lost.saturating_add(1);
        }
        return;
    };
    if restored != volume {
        control.borrow_mut().report.violations.push(format!(
            "restore reply changed volume from {volume:?} to {restored:?}"
        ));
        return;
    }
    let runnable = prepare_recovered(
        &control.borrow().guest_state[&volume],
        volume,
        &config,
        worlds[usize::from(target.host)].as_ref(),
        verdict,
    );
    {
        let mut control = control.borrow_mut();
        control.placement.insert(volume, target.host);
        control.report.restores = control.report.restores.saturating_add(1);
        control.report.loss_bound_verified = control.report.loss_bound_verified.saturating_add(1);
        control.report.max_restore_ns = control
            .report
            .max_restore_ns
            .max(now().saturating_sub(sent));
    }
    if runnable {
        start_guest(volume, target.host, &control, &worlds, &config);
    }
}

struct MigrationCompletionSignal(Option<OneSender<()>>);

impl Drop for MigrationCompletionSignal {
    fn drop(&mut self) {
        if let Some(completed) = self.0.take() {
            let _ = completed.send(());
        }
    }
}

async fn migration_completion(
    volume: VolumeId,
    attempt: MigrationAttempt,
    reply: Response<AdminResult>,
    completed: OneSender<()>,
    control: Rc<RefCell<Control>>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    config: Rc<ClusterConfig>,
) {
    let _completed = MigrationCompletionSignal(Some(completed));
    let result = reply.await;
    let succeeded = matches!(
        &result,
        Ok(Ok(AdminSuccess::MigratedOut { volume: migrated })) if *migrated == volume
    );
    if succeeded {
        let deferred = {
            let mut control = control.borrow_mut();
            if control.migrations.get(&volume) == Some(&attempt) {
                control.accepted_migrations.insert(volume);
                control.deferred_destination_recoveries.remove(&volume)
            } else {
                None
            }
        };
        if let Some((host, verdict)) = deferred {
            finalize_destination_migration(
                volume, attempt, host, verdict, &control, &worlds, &config,
            );
        }
        return;
    }
    if result.is_err() {
        // Source cancellation is ambiguous: the destination may already have
        // durably accepted the handoff. Keep the attempt until recovery tells
        // us which side owns the volume.
        let (deferred_destination, deferred_source) = {
            let mut control = control.borrow_mut();
            if control.migrations.get(&volume) == Some(&attempt) {
                if let Some(destination) = control.deferred_destination_recoveries.remove(&volume) {
                    control.uncertain_migrations.insert(volume);
                    (Some(destination), None)
                } else if let Some(source) = control.deferred_source_recoveries.remove(&volume) {
                    control.migrations.remove(&volume);
                    control.uncertain_migrations.remove(&volume);
                    control.accepted_migrations.remove(&volume);
                    control.report.migrations_refused =
                        control.report.migrations_refused.saturating_add(1);
                    (None, Some(source))
                } else {
                    control.uncertain_migrations.insert(volume);
                    (None, None)
                }
            } else {
                (None, None)
            }
        };
        if let Some((host, verdict)) = deferred_destination {
            finalize_destination_migration(
                volume, attempt, host, verdict, &control, &worlds, &config,
            );
        } else if deferred_source.is_some() {
            restart_source_after_migration_refusal(
                volume,
                attempt,
                deferred_source,
                &control,
                &worlds,
                &config,
            );
        }
        return;
    }
    let (removed, deferred_recovery) = {
        let mut control = control.borrow_mut();
        if control.migrations.get(&volume) == Some(&attempt) {
            control.migrations.remove(&volume);
            control.uncertain_migrations.remove(&volume);
            control.accepted_migrations.remove(&volume);
            control.deferred_destination_recoveries.remove(&volume);
            control.report.migrations_refused = control.report.migrations_refused.saturating_add(1);
            (true, control.deferred_source_recoveries.remove(&volume))
        } else {
            (false, None)
        }
    };
    if removed {
        restart_source_after_migration_refusal(
            volume,
            attempt,
            deferred_recovery,
            &control,
            &worlds,
            &config,
        );
    }
}

fn restart_source_after_migration_refusal(
    volume: VolumeId,
    attempt: MigrationAttempt,
    deferred_recovery: Option<(u16, Verdict)>,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    let source_ready = {
        let control = control.borrow();
        control.placement.get(&volume) == Some(&attempt.from)
            && control.up[usize::from(attempt.from)]
    };
    let source_reconciled = worlds[usize::from(attempt.from)].incarnation()
        == attempt.from_incarnation
        || deferred_recovery.is_some();
    if !source_ready || !source_reconciled {
        return;
    }
    let runnable = deferred_recovery.is_none_or(|(host, verdict)| {
        debug_assert_eq!(host, attempt.from);
        let runnable = prepare_recovered(
            &control.borrow().guest_state[&volume],
            volume,
            config,
            worlds[usize::from(host)].as_ref(),
            verdict,
        );
        let mut control = control.borrow_mut();
        control.report.recoveries = control.report.recoveries.saturating_add(1);
        runnable
    });
    if runnable {
        start_guest(volume, attempt.from, control, worlds, config);
    }
}

fn start_guest(
    volume: VolumeId,
    host: u16,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    cancel_guest(volume, control);
    {
        let mut control = control.borrow_mut();
        control.quiescing_guests.remove(&volume);
        control.migration_cuts.remove(&volume);
    }
    let guest_state = Rc::clone(&control.borrow().guest_state[&volume]);
    worlds[usize::from(host)].set_vmstate(volume, guest_state.completed.get());
    let guest = spawn(guest_actor(
        Rc::clone(&worlds[usize::from(host)]),
        guest_state,
        Rc::clone(control),
        volume,
        Rc::clone(config),
    ));
    control
        .borrow()
        .guests
        .borrow_mut()
        .insert(volume, Some(guest));
}

fn cancel_guest(volume: VolumeId, control: &Rc<RefCell<Control>>) {
    if let Some(Some(mut guest)) = control.borrow().guests.borrow_mut().remove(&volume) {
        guest.cancel();
    }
}

fn apply_deferred_source_recovery(
    volume: VolumeId,
    attempt: MigrationAttempt,
    control: &Rc<RefCell<Control>>,
    worlds: &[Rc<SimWorld>],
    config: &ClusterConfig,
) -> Option<bool> {
    let deferred = control
        .borrow_mut()
        .deferred_source_recoveries
        .remove(&volume)?;
    debug_assert_eq!(deferred.0, attempt.from);
    let runnable = prepare_recovered(
        &control.borrow().guest_state[&volume],
        volume,
        config,
        worlds[usize::from(attempt.from)].as_ref(),
        deferred.1,
    );
    let mut control = control.borrow_mut();
    control.report.recoveries = control.report.recoveries.saturating_add(1);
    Some(runnable)
}

fn discard_deferred_source_recovery(
    volume: VolumeId,
    attempt: MigrationAttempt,
    control: &Rc<RefCell<Control>>,
) {
    let mut control = control.borrow_mut();
    if let Some((host, _)) = control.deferred_source_recoveries.remove(&volume) {
        debug_assert_eq!(host, attempt.from);
        control.report.recoveries = control.report.recoveries.saturating_add(1);
    }
}

fn finalize_destination_migration(
    volume: VolumeId,
    attempt: MigrationAttempt,
    host: u16,
    verdict: Verdict,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    if attempt.to != host || control.borrow().migrations.get(&volume) != Some(&attempt) {
        return;
    }
    discard_deferred_source_recovery(volume, attempt, control);
    let migrated = control.borrow().guest_state[&volume]
        .expected
        .borrow()
        .clone();
    *control.borrow().guest_state[&volume].durable.borrow_mut() = migrated;
    let runnable = prepare_recovered(
        &control.borrow().guest_state[&volume],
        volume,
        config,
        worlds[usize::from(host)].as_ref(),
        verdict,
    );
    {
        let mut control = control.borrow_mut();
        control.migrations.remove(&volume);
        control.uncertain_migrations.remove(&volume);
        control.accepted_migrations.remove(&volume);
        control.deferred_destination_recoveries.remove(&volume);
        control.placement.insert(volume, host);
        control.report.migrations = control.report.migrations.saturating_add(1);
        control.report.max_migration_pause_ns = control
            .report
            .max_migration_pause_ns
            .max(migration_pause_ns(attempt, worlds));
    }
    if runnable {
        start_guest(volume, host, control, worlds, config);
    }
}

fn prepare_recovered(
    state: &GuestState,
    volume: VolumeId,
    config: &ClusterConfig,
    world: &SimWorld,
    verdict: Verdict,
) -> bool {
    world.reset_guest_memory(volume);
    match verdict {
        Verdict::Resume { vmstate, .. } => {
            state.completed.set(vmstate);
            if let Some(snapshot) = world.checkpoint_snapshots(volume, vmstate).last() {
                state.expected.borrow_mut().clone_from(&snapshot.pages);
                *state.durable.borrow_mut() = snapshot
                    .pages
                    .iter()
                    .filter(|(page, _)| !snapshot.unknown.contains(page))
                    .map(|(page, bytes)| (*page, bytes.clone()))
                    .collect();
            } else {
                *state.expected.borrow_mut() = state.durable.borrow().clone();
            }
        }
        Verdict::ColdBoot => {
            state.completed.set(0);
            let cold = if config.volume_config.is_memory() {
                BTreeMap::new()
            } else {
                state
                    .durable
                    .borrow()
                    .iter()
                    .map(|(page, bytes)| (*page, bytes.clone()))
                    .collect::<BTreeMap<_, _>>()
            };
            state.expected.borrow_mut().clone_from(&cold);
            *state.durable.borrow_mut() = cold;
        }
        Verdict::Unrestorable => {
            state
                .violations
                .borrow_mut()
                .push(format!("volume {volume:?} became unrestorable"));
            return false;
        }
    }
    *state.recovering.borrow_mut() = (0..config.volume_config.pages)
        .map(|page| PageId {
            volume,
            page: PageNo(page),
        })
        .collect();
    true
}

#[allow(clippy::too_many_lines)]
async fn guest_actor(
    world: Rc<SimWorld>,
    state: Rc<GuestState>,
    control: Rc<RefCell<Control>>,
    volume: VolumeId,
    config: Rc<ClusterConfig>,
) {
    let mut next_req = (volume.0 << 48) | 1;
    loop {
        if control.borrow().quiescing_guests.contains(&volume) {
            return;
        }
        delay(random_between(config.think.0, config.think.1)).await;
        if control.borrow().quiescing_guests.contains(&volume) {
            return;
        }
        if now() > control.borrow().workload_end.get() {
            return;
        }
        let sync = config
            .guest_sync_share
            .map_or_else(|| random_u64() % 100 >= 85, random_hit);
        if sync {
            let req = ReqId(next_req);
            next_req = next_req.checked_add(1).expect("guest request overflow");
            let started = now();
            if !world
                .sync(blockd_core::world::GuestSync { req, volume })
                .await
            {
                return;
            }
            control
                .borrow_mut()
                .sync_latencies
                .push(now().saturating_sub(started));
            if !config.volume_config.is_memory() {
                let current = state.expected.borrow();
                let mut durable = state.durable.borrow_mut();
                durable.retain(|page, _| page.volume != volume);
                durable.extend(
                    current
                        .iter()
                        .filter(|(page, _)| page.volume == volume)
                        .map(|(page, bytes)| (*page, bytes.clone())),
                );
            }
            finish_operation(&world, &state, &control, volume);
            continue;
        }
        let page = choose_page(&config, volume);
        let write = random_u64() % 100 < 60;
        if !world.fault(page, write).await {
            let mut control = control.borrow_mut();
            control.report.guest_deaths = control.report.guest_deaths.saturating_add(1);
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
                    return;
                }
            }
            state.expected.borrow_mut().insert(page, bytes);
            state
                .written
                .borrow_mut()
                .entry(page)
                .or_default()
                .insert(sequence);
            state.recovering.borrow_mut().remove(&page);
        } else {
            let actual = world
                .page_bytes(page)
                .unwrap_or_else(|| vec![0; page_size()]);
            if let Err(reason) = validate_page_bytes(&state, page, &actual) {
                state
                    .violations
                    .borrow_mut()
                    .push(format!(
                        "read returned stale or foreign bytes on {:?} for {page:?}: {reason}, vmstate {} at {}",
                        world.host_id(),
                        state.completed.get(),
                        now(),
                    ));
                return;
            }
        }
        finish_operation(&world, &state, &control, volume);
    }
}

fn validate_page_bytes(state: &GuestState, page: PageId, actual: &[u8]) -> Result<(), String> {
    let expected = state
        .expected
        .borrow()
        .get(&page)
        .cloned()
        .unwrap_or_else(|| vec![0; page_size()]);
    let expected_claimed = crate::guest::claimed_vol_seq(&expected);
    let recovering = state.recovering.borrow_mut().remove(&page);
    let claimed = crate::guest::claimed_vol_seq(actual);
    let (durable_floor, has_durable) = state
        .durable
        .borrow()
        .get(&page)
        .map_or((0, false), |bytes| {
            (crate::guest::claimed_vol_seq(bytes), true)
        });
    let possible = (claimed == 0 && !has_durable)
        || state
            .written
            .borrow()
            .get(&page)
            .is_some_and(|sequences| sequences.contains(&claimed));
    let valid_recovery =
        recovering && possible && claimed >= durable_floor && actual == page_pattern(page, claimed);
    if actual != expected && !valid_recovery {
        return Err(format!(
            "actual sequence {claimed}, expected {expected_claimed}, durable floor {durable_floor}, recovering {recovering}, possible {possible}"
        ));
    }
    if valid_recovery {
        state.expected.borrow_mut().insert(page, actual.to_vec());
    }
    Ok(())
}

fn finish_operation(
    world: &SimWorld,
    state: &GuestState,
    control: &RefCell<Control>,
    volume: VolumeId,
) {
    if control.borrow().migration_cuts.contains(&volume) {
        state.violations.borrow_mut().push(format!(
            "guest operation completed after the migration cut for {volume:?}"
        ));
    }
    let completed = state.completed.get().saturating_add(1);
    state.completed.set(completed);
    state
        .total_completed
        .set(state.total_completed.get().saturating_add(1));
    world.set_vmstate(volume, completed);
}

fn choose_page(config: &ClusterConfig, volume: VolumeId) -> PageId {
    PageId {
        volume,
        page: PageNo(
            u32::try_from(random_u64() % u64::from(config.volume_config.pages))
                .expect("page number fits"),
        ),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn spawn_schedules(
    config: &Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
    peer_stats: Rc<PeerTransportStats>,
) {
    for &(at, host) in &config.crash_hosts_at {
        spawn(at_crash(
            at,
            host,
            Rc::clone(config),
            Rc::clone(&worlds),
            Rc::clone(&states),
            Rc::clone(&control),
            Rc::clone(&commands),
        ))
        .detach();
    }
    for &(at, host) in &config.kill_hosts_at {
        spawn(at_kill(
            at,
            host,
            Rc::clone(config),
            Rc::clone(&worlds),
            Rc::clone(&states),
            Rc::clone(&control),
            Rc::clone(&commands),
        ))
        .detach();
    }
    for &(at, volume, to) in &config.migrate_at {
        spawn(at_migrate(
            at,
            volume,
            to,
            Rc::clone(config),
            Rc::clone(&worlds),
            Rc::clone(&control),
        ))
        .detach();
    }
    for &(begin, end, from, to) in &config.peer_link_outages {
        let transport_stats = Rc::clone(&peer_stats);
        spawn(async move {
            delay(begin).await;
            turmoil::partition_oneway(format!("host-{from}"), format!("host-{to}"));
            transport_stats.record_clog();
            delay(end.saturating_sub(begin)).await;
            turmoil::repair_oneway(format!("host-{from}"), format!("host-{to}"));
        })
        .detach();
    }
    if let Some((begin, end)) = config.store_outage {
        let world = Rc::clone(&worlds[0]);
        spawn(async move {
            delay(begin).await;
            world.set_store_outage(true);
            delay(end.saturating_sub(begin)).await;
            world.set_store_outage(false);
        })
        .detach();
    }
    if let Some(interval) = config.checkpoint_interval {
        spawn(checkpoint_schedule(
            interval,
            control.borrow().workload_end.get(),
            Rc::clone(&worlds),
            Rc::clone(&control),
        ))
        .detach();
    }
    if config.crash_mean_interval != 0 {
        spawn(random_crashes(
            Rc::clone(config),
            Rc::clone(&worlds),
            Rc::clone(&states),
            Rc::clone(&control),
            Rc::clone(&commands),
        ))
        .detach();
    }
    if config.migrate_mean_interval != 0 {
        spawn(random_migrations(
            Rc::clone(config),
            Rc::clone(&worlds),
            Rc::clone(&control),
        ))
        .detach();
    }
}

#[allow(clippy::too_many_arguments)]
async fn at_crash(
    at: u64,
    host: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
) {
    delay(at).await;
    crash_host(host, &config, &worlds, &states, &control, &commands).await;
}

async fn host_command(commands: &HostCommands, build: impl FnOnce(OneSender<()>) -> HostCommand) {
    let (complete, completed) = oneshot();
    commands.borrow_mut().push_back(build(complete));
    let _ = completed.await;
}

async fn crash_host(
    host: u16,
    config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    states: &StateSlots,
    control: &Rc<RefCell<Control>>,
    commands: &HostCommands,
) {
    if !control.borrow().up[usize::from(host)] {
        return;
    }
    control.borrow_mut().up[usize::from(host)] = false;
    host_command(commands, |complete| {
        HostCommand::Crash(HostId(host), complete)
    })
    .await;
    worlds[usize::from(host)].advance_incarnation();
    let affected = control
        .borrow()
        .placement
        .iter()
        .filter_map(|(&volume, &placed)| (placed == host).then_some(volume))
        .collect::<Vec<_>>();
    for volume in affected {
        cancel_guest(volume, control);
    }
    control
        .borrow_mut()
        .retired_counters
        .push(states[usize::from(host)].borrow().borrow().counters);
    let fates = worlds[usize::from(host)].crash_pending();
    record_fates(&mut control.borrow_mut().report, &fates);
    worlds[usize::from(host)].crash_guest_io();
    {
        let mut control = control.borrow_mut();
        control.report.host_crashes = control.report.host_crashes.saturating_add(1);
    }
    delay(random_between(
        config.restart_delay.0,
        config.restart_delay.1,
    ))
    .await;
    worlds[usize::from(host)].clear_abort();
    host_command(commands, |complete| {
        HostCommand::Bounce(HostId(host), complete)
    })
    .await;
    control.borrow_mut().up[usize::from(host)] = true;
}

#[allow(clippy::too_many_arguments)]
async fn at_kill(
    at: u64,
    host: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
) {
    delay(at).await;
    if !control.borrow().up[usize::from(host)] {
        return;
    }
    control.borrow_mut().up[usize::from(host)] = false;
    host_command(&commands, |complete| {
        HostCommand::Crash(HostId(host), complete)
    })
    .await;
    worlds[usize::from(host)].advance_incarnation();
    control.borrow_mut().live[usize::from(host)] = false;
    control
        .borrow_mut()
        .retired_counters
        .push(states[usize::from(host)].borrow().borrow().counters);
    let affected = control
        .borrow()
        .placement
        .iter()
        .filter_map(|(&volume, &placed)| (placed == host).then_some(volume))
        .collect::<Vec<_>>();
    let orphaned_at = now();
    let mut restore_tasks = TaskSet::new();
    let (restore_completed, mut restore_completions) = unbounded();
    let mut restores_active = 0usize;
    for volume in affected {
        cancel_guest(volume, &control);
        if !promote_orphan(host, volume, &config, &worlds, &control).await {
            control
                .borrow_mut()
                .report
                .violations
                .push(format!("unable to promote orphan {volume:?}"));
            continue;
        }
        prepare_backed_loss(&control.borrow().guest_state[&volume], volume, &worlds[0]);
        let candidates = (0..config.hosts)
            .filter(|&candidate| candidate != host && control.borrow().live[usize::from(candidate)])
            .take(if config.race_restore { 2 } else { 1 })
            .collect::<Vec<_>>();
        for candidate in candidates {
            while restores_active == RESTORE_CONCURRENCY {
                if restore_completions.recv().await.is_none() {
                    return;
                }
                restores_active -= 1;
            }
            let candidate_incarnation = worlds[usize::from(candidate)].incarnation();
            let reply =
                worlds[usize::from(candidate)].request_admin(AdminCall::RestoreVolume { volume });
            let control = Rc::clone(&control);
            let worlds = Rc::clone(&worlds);
            let config = Rc::clone(&config);
            let restore_completed = restore_completed.clone();
            restore_tasks.spawn(async move {
                restore_completion(
                    RestoreTarget {
                        host: candidate,
                        incarnation: candidate_incarnation,
                    },
                    volume,
                    orphaned_at,
                    reply,
                    control,
                    worlds,
                    config,
                )
                .await;
                let _ = restore_completed.send(());
            });
            restores_active += 1;
        }
    }
    while restores_active != 0 {
        if restore_completions.recv().await.is_none() {
            return;
        }
        restores_active -= 1;
    }
}

#[allow(clippy::too_many_lines)]
async fn promote_orphan(
    source: u16,
    volume: VolumeId,
    config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    control: &Rc<RefCell<Control>>,
) -> bool {
    let (observed_version, head) = loop {
        match Store::get(worlds[0].as_ref(), &layout::head_key(volume)).await {
            Ok(Some((version, bytes))) => {
                let Ok(head) = HeadRecord::decode(volume, &bytes) else {
                    return false;
                };
                break (version, head);
            }
            Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                delay(config.daemon.backup_retry).await;
            }
            Ok(None) | Err(StoreError::TooLarge | StoreError::Fault(_)) => return false,
        }
    };
    if head.holder != HostId(source) {
        return false;
    }

    let mut allowed = BTreeSet::new();
    if let Some(stash) = head.stash {
        allowed.insert((stash.active_peer, stash.active_assignment_epoch));
        if let Some(peer) = stash.transition_peer {
            allowed.insert((peer, stash.assignment_epoch));
        }
    }
    allowed.extend(
        head.retired_stashes
            .iter()
            .map(|retired| (retired.peer, retired.assignment_epoch)),
    );
    let live = control.borrow().live.clone();
    let mut owned = Vec::new();
    for (peer, world) in worlds.iter().enumerate() {
        if !live[peer] {
            continue;
        }
        let peer = HostId(u16::try_from(peer).expect("host index fits"));
        let mut generations: BTreeMap<u64, BTreeMap<u64, Vec<u8>>> = BTreeMap::new();
        for (name, bytes) in world.durable_blobs() {
            if let Some(layout::BlobName::ReplicaSpool {
                source: found_source,
                volume: found_volume,
                assignment_epoch,
                generation,
            }) = layout::parse_blob(&name)
                && (found_source, found_volume) == (HostId(source), volume)
                && (allowed.contains(&(peer, assignment_epoch))
                    || head
                        .stash
                        .is_some_and(|stash| assignment_epoch > stash.assignment_epoch))
            {
                generations
                    .entry(assignment_epoch)
                    .or_default()
                    .insert(generation, bytes);
            }
        }
        for (assignment_epoch, generations) in generations {
            owned.push((
                peer,
                assignment_epoch,
                generations.into_values().flatten().collect::<Vec<_>>(),
            ));
        }
    }
    let residues = owned
        .iter()
        .map(|(peer, assignment_epoch, bytes)| ReplicaResidue {
            peer: *peer,
            assignment_epoch: *assignment_epoch,
            bytes,
        })
        .collect::<Vec<_>>();
    let export = if residues.is_empty() {
        None
    } else {
        match export_replica_recovery(
            HostId(source),
            volume,
            observed_version,
            &head,
            &residues,
            &worlds[0].store_snapshot(),
        ) {
            Ok(export) => Some(export),
            Err(_) => return false,
        }
    };
    if export.is_none() && head.manifest.is_none() {
        return false;
    }

    let Some(export) = export else {
        let retired = HeadRecord {
            volume,
            holder: HostId(source),
            fence: head.fence,
            manifest: head.manifest,
            stash: None,
            retired_stashes: Vec::new(),
        };
        return loop {
            match Store::put_cas(
                worlds[0].as_ref(),
                layout::head_key(volume),
                Some(observed_version),
                retired.encode(),
            )
            .await
            {
                Ok(_) => break true,
                Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                    delay(config.daemon.backup_retry).await;
                }
                Err(StoreError::TooLarge | StoreError::Fault(_)) => break false,
            }
        };
    };
    let claim = prepare_replica_recovery_claim(observed_version, &head, HostId(source), &export);
    let writer_fence = loop {
        match Store::put_cas(
            worlds[0].as_ref(),
            layout::head_key(volume),
            Some(claim.expected_version),
            claim.head.encode(),
        )
        .await
        {
            Ok(version) => break version,
            Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                delay(config.daemon.backup_retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(_)) => return false,
        }
    };
    let Ok(publication) =
        prepare_replica_publication(volume, HostId(source), writer_fence, &claim.head, &export)
    else {
        return false;
    };
    for (key, bytes) in &publication.store_objects {
        loop {
            match Store::put(worlds[0].as_ref(), key.clone(), bytes.clone()).await {
                Ok(_) => break,
                Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                    delay(config.daemon.backup_retry).await;
                }
                Err(StoreError::TooLarge | StoreError::Fault(_)) => return false,
            }
        }
    }
    loop {
        match Store::put_cas(
            worlds[0].as_ref(),
            layout::head_key(volume),
            Some(writer_fence),
            publication.head.encode(),
        )
        .await
        {
            Ok(_) => return true,
            Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                delay(config.daemon.backup_retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(_)) => return false,
        }
    }
}

fn prepare_backed_loss(state: &GuestState, volume: VolumeId, world: &SimWorld) {
    let capture_seq = world
        .store_bytes(&blockd_core::layout::head_key(volume))
        .and_then(|bytes| blockd_core::head::HeadRecord::decode(volume, &bytes).ok())
        .and_then(|head| head.manifest)
        .map_or(0, |manifest| manifest.capture_seq);
    if let Some(snapshot) = world.capture_snapshot(volume, capture_seq) {
        state.expected.borrow_mut().clone_from(&snapshot.pages);
        *state.durable.borrow_mut() = snapshot
            .pages
            .iter()
            .filter(|(page, _)| !snapshot.unknown.contains(page))
            .map(|(page, bytes)| (*page, bytes.clone()))
            .collect();
    }
}

async fn at_migrate(
    at: u64,
    volume: VolumeId,
    to: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) {
    delay(at).await;
    start_migration(volume, to, &worlds, &control, &config).await;
}

enum QuiescingCleanup {
    Rollback,
    Disarm,
}

struct QuiescingMigrationGuard {
    cleanup: Injector<QuiescingCleanup>,
    active: bool,
}

impl QuiescingMigrationGuard {
    fn new(
        volume: VolumeId,
        attempt: MigrationAttempt,
        control: &Rc<RefCell<Control>>,
        worlds: &Rc<Vec<Rc<SimWorld>>>,
        config: &Rc<ClusterConfig>,
    ) -> Self {
        let (cleanup, commands) = injector();
        let control = Rc::clone(control);
        let worlds = Rc::clone(worlds);
        let config = Rc::clone(config);
        spawn(async move {
            if matches!(commands.recv().await, Some(QuiescingCleanup::Rollback)) {
                rollback_quiescing_migration(volume, attempt, &control, &worlds, &config);
            }
        })
        .detach();
        Self {
            cleanup,
            active: true,
        }
    }

    fn disarm(mut self) {
        self.active = false;
        let _ = self.cleanup.push(Lane::Critical, QuiescingCleanup::Disarm);
    }
}

impl Drop for QuiescingMigrationGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .cleanup
                .push(Lane::Critical, QuiescingCleanup::Rollback);
        }
    }
}

fn rollback_quiescing_migration(
    volume: VolumeId,
    attempt: MigrationAttempt,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    let (source_up, original_incarnation, deferred_recovery) = {
        let mut control = control.borrow_mut();
        if control.migrations.get(&volume) != Some(&attempt) {
            return;
        }
        control.migrations.remove(&volume);
        control.quiescing_guests.remove(&volume);
        control.uncertain_migrations.remove(&volume);
        control.accepted_migrations.remove(&volume);
        control.deferred_destination_recoveries.remove(&volume);
        (
            control.up[usize::from(attempt.from)]
                && control.placement.get(&volume) == Some(&attempt.from),
            worlds[usize::from(attempt.from)].incarnation() == attempt.from_incarnation,
            control.deferred_source_recoveries.contains_key(&volume),
        )
    };
    if source_up && (original_incarnation || deferred_recovery) {
        let runnable = apply_deferred_source_recovery(volume, attempt, control, worlds, config)
            .unwrap_or(true);
        if runnable {
            start_guest(volume, attempt.from, control, worlds, config);
        }
    }
}

async fn start_migration(
    volume: VolumeId,
    to: u16,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    control: &Rc<RefCell<Control>>,
    config: &Rc<ClusterConfig>,
) -> Option<OneReceiver<()>> {
    let &from = control.borrow().placement.get(&volume)?;
    let unavailable = {
        let control = control.borrow();
        let guest_active = control
            .guests
            .borrow()
            .get(&volume)
            .is_some_and(Option::is_some);
        !control.up[usize::from(from)]
            || !control.live[usize::from(to)]
            || !control.up[usize::from(to)]
            || !guest_active
            || control.migrations.contains_key(&volume)
    };
    if from == to || unavailable {
        return None;
    }
    let attempt = MigrationAttempt {
        from,
        to,
        from_incarnation: worlds[usize::from(from)].incarnation(),
        to_incarnation: worlds[usize::from(to)].incarnation(),
        started: now(),
    };
    {
        let mut control = control.borrow_mut();
        control.quiescing_guests.insert(volume);
        control.uncertain_migrations.remove(&volume);
        control.accepted_migrations.remove(&volume);
        control.deferred_source_recoveries.remove(&volume);
        control.deferred_destination_recoveries.remove(&volume);
        control.migrations.insert(volume, attempt);
    }
    let quiescing = QuiescingMigrationGuard::new(volume, attempt, control, worlds, config);
    let guest = {
        let control = control.borrow();
        let removed = control.guests.borrow_mut().remove(&volume);
        removed.flatten()
    };
    if let Some(guest) = guest {
        let _ = guest.await;
    }
    let can_submit = {
        let control = control.borrow();
        control.up[usize::from(from)]
            && control.up[usize::from(to)]
            && worlds[usize::from(from)].incarnation() == attempt.from_incarnation
            && worlds[usize::from(to)].incarnation() == attempt.to_incarnation
            && control.placement.get(&volume) == Some(&from)
            && control.migrations.get(&volume) == Some(&attempt)
    };
    if !can_submit {
        rollback_quiescing_migration(volume, attempt, control, worlds, config);
        quiescing.disarm();
        return None;
    }
    let guest_state = Rc::clone(&control.borrow().guest_state[&volume]);
    worlds[usize::from(from)].set_vmstate(volume, guest_state.completed.get());
    control.borrow_mut().migration_cuts.insert(volume);
    let reply = worlds[usize::from(from)].request_admin(AdminCall::MigrateOut {
        volume,
        to: HostId(to),
    });
    let (completed, completion) = oneshot();
    spawn(migration_completion(
        volume,
        attempt,
        reply,
        completed,
        Rc::clone(control),
        Rc::clone(worlds),
        Rc::clone(config),
    ))
    .detach();
    quiescing.disarm();
    Some(completion)
}

async fn checkpoint_schedule(
    interval: u64,
    horizon: u64,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) {
    crate::checkpoint_schedule::run(
        interval,
        horizon,
        1,
        || control.borrow().placement.keys().copied().collect(),
        |volume, retry| {
            let host = {
                let control = control.borrow();
                control.placement.get(&volume).copied().filter(|host| {
                    control.up[usize::from(*host)]
                        && control
                            .guests
                            .borrow()
                            .get(&volume)
                            .is_some_and(Option::is_some)
                })
            }?;
            Some(worlds[usize::from(host)].request_admin(AdminCall::Checkpoint { retry, volume }))
        },
    )
    .await;
}

async fn random_crashes(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
) {
    loop {
        delay(random_between(
            1,
            config.crash_mean_interval.saturating_mul(2),
        ))
        .await;
        if now() > control.borrow().workload_end.get() {
            return;
        }
        let host = u16::try_from(random_u64() % u64::from(config.hosts)).expect("host fits");
        crash_host(host, &config, &worlds, &states, &control, &commands).await;
    }
}

async fn random_migrations(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) {
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = BTreeSet::new();
    let mut next_cadence = now().saturating_add(random_between(
        1,
        config.migrate_mean_interval.saturating_mul(2),
    ));
    loop {
        match select2(
            completions.recv(),
            delay(next_cadence.saturating_sub(now())),
        )
        .await
        {
            Either::First(Some(volume)) => {
                active.remove(&volume);
                continue;
            }
            Either::First(None) => return,
            Either::Second(()) => {}
        }
        if now() > control.borrow().workload_end.get() {
            return;
        }
        next_cadence = now().saturating_add(random_between(
            1,
            config.migrate_mean_interval.saturating_mul(2),
        ));
        if active.len() >= MIGRATION_CONCURRENCY {
            continue;
        }
        let candidates = control
            .borrow()
            .placement
            .iter()
            .filter_map(|(&volume, &host)| {
                let control = control.borrow();
                (!active.contains(&volume)
                    && control.up[usize::from(host)]
                    && control
                        .guests
                        .borrow()
                        .get(&volume)
                        .is_some_and(Option::is_some))
                .then_some(volume)
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            continue;
        }
        let volume = candidates
            [usize::try_from(random_u64() % candidates.len() as u64).expect("volume index fits")];
        let from = control.borrow().placement[&volume];
        let destinations = (0..config.hosts)
            .filter(|&host| {
                host != from
                    && control.borrow().live[usize::from(host)]
                    && control.borrow().up[usize::from(host)]
            })
            .collect::<Vec<_>>();
        if destinations.is_empty() {
            continue;
        }
        let to = destinations
            [usize::try_from(random_u64() % destinations.len() as u64).expect("host index fits")];
        assert!(active.insert(volume));
        let worlds = Rc::clone(&worlds);
        let control = Rc::clone(&control);
        let config = Rc::clone(&config);
        let completed = completed.clone();
        actors.spawn(async move {
            if let Some(completion) = start_migration(volume, to, &worlds, &control, &config).await
            {
                let _ = completion.await;
            }
            let _ = completed.send(volume);
        });
    }
}

fn record_fates(report: &mut ClusterReport, fates: &[(String, CrashFate)]) {
    for (_, fate) in fates {
        match fate {
            CrashFate::Applied => report.disk_crash_applied += 1,
            CrashFate::Dropped => report.disk_crash_dropped += 1,
            CrashFate::Torn { .. } => report.disk_crash_torn += 1,
        }
    }
}

fn summarize_counters(report: &mut ClusterReport, counters: &[Counters]) {
    let sum = |read: fn(&Counters) -> u64| counters.iter().map(read).sum();
    report.hydrate_fills = sum(|value| value.hydrate_fills);
    report.store_retries = sum(|value| value.store_retries);
    report.wedged_guests = sum(|value| value.wedged_guests);
    report.wedged_hydration = sum(|value| value.wedged_hydration);
    report.wedged_outbound = sum(|value| value.wedged_outbound);
    report.replica_bytes = sum(|value| value.replica_bytes);
    report.replica_commits = sum(|value| value.replica_commits);
    report.replica_unlinks = sum(|value| value.replica_unlinks);
    report.replica_network_bytes = sum(|value| value.replica_network_bytes);
    report.replica_logical_bytes = sum(|value| value.replica_logical_bytes);
    report.replica_nonactive_bytes = sum(|value| value.replica_nonactive_bytes);
    report.replica_replacement_bytes = sum(|value| value.replica_replacement_bytes);
    report.replica_artifact_flushes = sum(|value| value.replica_artifact_flushes);
    report.replica_commit_flushes = sum(|value| value.replica_commit_flushes);
    report.replica_rotations = sum(|value| value.replica_rotations);
    report.replica_capacity_backpressure = sum(|value| value.replica_capacity_backpressure);
    report.blx_files_compacted = sum(|value| value.blx_files_compacted);
    report.pages_compacted = sum(|value| value.pages_compacted);
}

fn summarize_latencies(report: &mut ClusterReport, latencies: &[u64]) {
    report.sync_samples = latencies.len() as u64;
    let percentile = |pct: usize| {
        if latencies.is_empty() {
            0
        } else {
            latencies[((latencies.len() - 1) * pct) / 100]
        }
    };
    report.sync_latency_p50_ns = percentile(50);
    report.sync_latency_p95_ns = percentile(95);
    report.sync_latency_p99_ns = percentile(99);
    report.sync_latency_max_ns = latencies.last().copied().unwrap_or(0);
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use blockd_core::protocol::{AdminError, AdminEvent};
    use blockd_core::types::Epoch;
    use blockd_core::world::AdminIo;
    use blockd_exec::{delay, request, spawn, timeout, yield_now};

    use super::*;

    macro_rules! simulate {
        ($seed:expr, $future:expr) => {{
            tokio::task::LocalSet::new()
                .run_until(blockd_exec::simulation_scope(
                    $seed,
                    FaultConfig::default(),
                    $future,
                ))
                .await
        }};
    }

    fn control(volume_count: u64, hosts: u16) -> Rc<RefCell<Control>> {
        let guests = Rc::new(RefCell::new(BTreeMap::new()));
        let mut placement = BTreeMap::new();
        let mut guest_state = BTreeMap::new();
        for number in 1..=volume_count {
            let volume = VolumeId(number);
            placement.insert(volume, 0);
            guest_state.insert(volume, Rc::new(GuestState::default()));
            guests
                .borrow_mut()
                .insert(volume, Some(spawn(pending::<()>())));
        }
        Rc::new(RefCell::new(Control {
            placement,
            guests,
            guest_state,
            migrations: BTreeMap::new(),
            uncertain_migrations: BTreeSet::new(),
            accepted_migrations: BTreeSet::new(),
            deferred_source_recoveries: BTreeMap::new(),
            deferred_destination_recoveries: BTreeMap::new(),
            quiescing_guests: BTreeSet::new(),
            migration_cuts: BTreeSet::new(),
            live: vec![true; usize::from(hosts)],
            up: vec![true; usize::from(hosts)],
            workload_end: Cell::new(u64::MAX),
            report: ClusterReport::default(),
            sync_latencies: Vec::new(),
            retired_counters: Vec::new(),
        }))
    }

    fn unplaced_control(volume_count: u64, hosts: u16) -> Rc<RefCell<Control>> {
        Rc::new(RefCell::new(Control {
            placement: BTreeMap::new(),
            guests: Rc::new(RefCell::new(BTreeMap::new())),
            guest_state: (1..=volume_count)
                .map(|number| (VolumeId(number), Rc::new(GuestState::default())))
                .collect(),
            migrations: BTreeMap::new(),
            uncertain_migrations: BTreeSet::new(),
            accepted_migrations: BTreeSet::new(),
            deferred_source_recoveries: BTreeMap::new(),
            deferred_destination_recoveries: BTreeMap::new(),
            quiescing_guests: BTreeSet::new(),
            migration_cuts: BTreeSet::new(),
            live: vec![true; usize::from(hosts)],
            up: vec![true; usize::from(hosts)],
            workload_end: Cell::new(u64::MAX),
            report: ClusterReport::default(),
            sync_latencies: Vec::new(),
            retired_counters: Vec::new(),
        }))
    }

    #[tokio::test(start_paused = true)]
    async fn initial_creation_requests_are_bounded() {
        let mut config = crate::presets::migration_chaos();
        config.hosts = 1;
        config.volume_count = u16::try_from(CREATION_CONCURRENCY + 1).expect("limit fits");
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(107, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let control = Rc::new(RefCell::new(Control {
                    placement: BTreeMap::new(),
                    guests: Rc::new(RefCell::new(BTreeMap::new())),
                    guest_state: (1..=config.volume_count)
                        .map(|number| (VolumeId(u64::from(number)), Rc::new(GuestState::default())))
                        .collect(),
                    migrations: BTreeMap::new(),
                    uncertain_migrations: BTreeSet::new(),
                    accepted_migrations: BTreeSet::new(),
                    deferred_source_recoveries: BTreeMap::new(),
                    deferred_destination_recoveries: BTreeMap::new(),
                    quiescing_guests: BTreeSet::new(),
                    migration_cuts: BTreeSet::new(),
                    live: vec![true; usize::from(config.hosts)],
                    up: vec![true; usize::from(config.hosts)],
                    workload_end: Cell::new(u64::MAX),
                    report: ClusterReport::default(),
                    sync_latencies: Vec::new(),
                    retired_counters: Vec::new(),
                }));
                let schedule = spawn(create_initial_volumes(
                    Rc::clone(&config),
                    Rc::clone(&worlds),
                    control,
                ));
                let mut requests = Vec::new();
                for _ in 0..CREATION_CONCURRENCY {
                    requests.push(
                        timeout(100, AdminIo::next_admin(worlds[0].as_ref()))
                            .await
                            .expect("bounded create request arrives")
                            .expect("admin ingress remains open"),
                    );
                }
                assert!(
                    timeout(0, AdminIo::next_admin(worlds[0].as_ref()))
                        .await
                        .is_err()
                );
                drop(requests);
                drop(schedule);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn initial_creation_waits_for_every_configured_volume() {
        let mut config = crate::presets::migration_chaos();
        config.hosts = 1;
        config.volume_count = u16::try_from(CREATION_CONCURRENCY + 1).expect("limit fits");
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(109, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let control = unplaced_control(u64::from(config.volume_count), config.hosts);
                let responder = spawn({
                    let world = Rc::clone(&worlds[0]);
                    let control = Rc::clone(&control);
                    let count = config.volume_count;
                    async move {
                        for _ in 0..count {
                            let request = AdminIo::next_admin(world.as_ref())
                                .await
                                .expect("create request");
                            let (call, mut reply) = request.into_parts();
                            let AdminCall::CreateVolume { volume, .. } = call else {
                                panic!("unexpected initial request: {call:?}");
                            };
                            assert!(control.borrow().guests.borrow().is_empty());
                            let _ = reply.send(Ok(AdminSuccess::VolumeCreated { volume }));
                        }
                    }
                });
                let created = create_initial_volumes(
                    Rc::clone(&config),
                    Rc::clone(&worlds),
                    Rc::clone(&control),
                )
                .await;
                responder.await.expect("responder completes");
                assert_eq!(
                    control.borrow().placement.len(),
                    usize::from(config.volume_count)
                );
                assert!(control.borrow().guests.borrow().is_empty());
                for (volume, host) in created {
                    start_guest(volume, host, &control, &worlds, &config);
                }
                assert_eq!(
                    control.borrow().guests.borrow().len(),
                    usize::from(config.volume_count)
                );
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn superseded_restore_retries_on_a_current_live_incarnation() {
        let config = Rc::new(crate::presets::migration_chaos());
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let control = unplaced_control(1, config.hosts);
        let volume = VolumeId(1);
        let candidate = 1;
        let (reply, result) = request(());
        reply
            .reply(Ok(AdminSuccess::VolumeRestored {
                volume,
                verdict: Verdict::ColdBoot,
            }))
            .expect("restore completion remains live");
        worlds[usize::from(candidate)].advance_incarnation();
        control.borrow_mut().live[usize::from(candidate)] = false;
        control.borrow_mut().up[usize::from(candidate)] = false;

        simulate!(111, {
            let control = Rc::clone(&control);
            let worlds = Rc::clone(&worlds);
            let config = Rc::clone(&config);
            async move {
                let completion = spawn(restore_completion(
                    RestoreTarget {
                        host: candidate,
                        incarnation: 0,
                    },
                    volume,
                    0,
                    result,
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                ));
                let retry = AdminIo::next_admin(worlds[0].as_ref())
                    .await
                    .expect("restore retried on live host");
                let (call, mut reply) = retry.into_parts();
                assert_eq!(call, AdminCall::RestoreVolume { volume });
                reply
                    .send(Ok(AdminSuccess::VolumeRestored {
                        volume,
                        verdict: Verdict::ColdBoot,
                    }))
                    .expect("retry remains live");
                completion.await.expect("restore completion");
            }
        });

        assert_eq!(control.borrow().placement.get(&volume), Some(&0));
        assert_eq!(control.borrow().report.restores, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_on_a_live_host_replaces_a_dead_placement() {
        let config = Rc::new(crate::presets::migration_chaos());
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let volume = VolumeId(1);
        simulate!(112, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::clone(&config);
            async move {
                let control = control(1, config.hosts);
                control.borrow_mut().live[0] = false;
                control.borrow_mut().up[0] = false;
                let lifecycle = spawn(lifecycle_actor(
                    1,
                    Rc::clone(&worlds[1]),
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    config,
                ));
                AdminIo::emit_admin_event(
                    worlds[1].as_ref(),
                    AdminEvent::VolumeRecovered {
                        volume,
                        verdict: Verdict::ColdBoot,
                    },
                )
                .await;
                delay(1).await;
                assert_eq!(control.borrow().placement.get(&volume), Some(&1));
                drop(lifecycle);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn random_migration_slots_wait_for_request_completion() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 100;
        config.migrate_mean_interval = 1;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(108, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let control = control(
                    u64::try_from(MIGRATION_CONCURRENCY + 1).expect("limit fits"),
                    config.hosts,
                );
                for guest in control.borrow().guests.borrow_mut().values_mut() {
                    *guest = Some(spawn(async {}));
                }
                let schedule = spawn(random_migrations(
                    Rc::clone(&config),
                    Rc::clone(&worlds),
                    control,
                ));
                let mut requests = Vec::new();
                for _ in 0..MIGRATION_CONCURRENCY {
                    requests.push(
                        timeout(100, AdminIo::next_admin(worlds[0].as_ref()))
                            .await
                            .expect("bounded migration request arrives")
                            .expect("admin ingress remains open"),
                    );
                }
                delay(10).await;
                assert!(
                    timeout(0, AdminIo::next_admin(worlds[0].as_ref()))
                        .await
                        .is_err()
                );
                drop(requests);
                drop(schedule);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn slow_guest_drain_does_not_block_other_random_migrations() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 100;
        config.migrate_mean_interval = 1;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(105, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let control = control(2, config.hosts);
                let schedule = spawn(random_migrations(
                    Rc::clone(&config),
                    Rc::clone(&worlds),
                    Rc::clone(&control),
                ));

                delay(10).await;

                assert_eq!(
                    control.borrow().quiescing_guests,
                    BTreeSet::from([VolumeId(1), VolumeId(2)])
                );
                drop(schedule);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_guest_drain_rolls_back_the_pending_migration() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 100;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(106, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let volume = VolumeId(1);
                let control = control(1, config.hosts);
                let mut migration = spawn({
                    let control = Rc::clone(&control);
                    let worlds = Rc::clone(&worlds);
                    let config = Rc::clone(&config);
                    async move { start_migration(volume, 1, &worlds, &control, &config).await }
                });
                delay(1).await;
                assert!(control.borrow().quiescing_guests.contains(&volume));
                assert!(control.borrow().migrations.contains_key(&volume));

                migration.cancel();
                delay(0).await;

                assert!(!control.borrow().quiescing_guests.contains(&volume));
                assert!(!control.borrow().migrations.contains_key(&volume));
                assert_eq!(control.borrow().placement.get(&volume), Some(&0));
                assert!(
                    control
                        .borrow()
                        .guests
                        .borrow()
                        .get(&volume)
                        .is_some_and(Option::is_some)
                );
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn destination_restart_recovery_waits_for_migration_completion() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(101, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let volume = VolumeId(1);
                let page = PageId {
                    volume,
                    page: PageNo(0),
                };
                let bytes = page_pattern(page, 7);
                let control = control(1, config.hosts);
                let attempt = MigrationAttempt {
                    from: 1,
                    to: 0,
                    from_incarnation: worlds[1].incarnation(),
                    to_incarnation: worlds[0].incarnation(),
                    started: 0,
                };
                worlds[usize::from(attempt.from)].advance_incarnation();
                {
                    let state = Rc::clone(&control.borrow().guest_state[&volume]);
                    state.completed.set(7);
                    state.expected.borrow_mut().insert(page, bytes.clone());
                    state.durable.borrow_mut().insert(page, bytes.clone());
                    let mut control = control.borrow_mut();
                    control.placement.insert(volume, 1);
                    control.migrations.insert(volume, attempt);
                    control
                        .deferred_source_recoveries
                        .insert(volume, (1, Verdict::ColdBoot));
                }
                worlds[0].advance_incarnation();
                let lifecycle = spawn(lifecycle_actor(
                    0,
                    Rc::clone(&worlds[0]),
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                ));
                AdminIo::emit_admin_event(
                    worlds[0].as_ref(),
                    AdminEvent::VolumeRecovered {
                        volume,
                        verdict: Verdict::Resume {
                            epoch: Epoch(1),
                            vmstate: 7,
                        },
                    },
                )
                .await;
                delay(1).await;
                assert_eq!(control.borrow().migrations.get(&volume), Some(&attempt));
                assert!(
                    control
                        .borrow()
                        .deferred_destination_recoveries
                        .contains_key(&volume)
                );

                let (reply, result) = request(());
                reply
                    .reply(Ok(AdminSuccess::MigratedOut { volume }))
                    .expect("completion receiver alive");
                let (completed, _completion) = oneshot();
                migration_completion(
                    volume,
                    attempt,
                    result,
                    completed,
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                )
                .await;

                let state = Rc::clone(&control.borrow().guest_state[&volume]);
                assert_eq!(state.completed.get(), 7);
                assert_eq!(state.expected.borrow().get(&page), Some(&bytes));
                assert_eq!(state.durable.borrow().get(&page), Some(&bytes));
                assert_eq!(control.borrow().report.recoveries, 1);
                assert_eq!(control.borrow().report.migrations, 1);
                assert_eq!(control.borrow().placement.get(&volume), Some(&0));
                assert!(!control.borrow().migrations.contains_key(&volume));
                assert!(
                    !control
                        .borrow()
                        .deferred_destination_recoveries
                        .contains_key(&volume)
                );
                drop(lifecycle);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn closed_migration_consumes_a_deferred_source_recovery() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(104, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let volume = VolumeId(1);
                let control = control(1, config.hosts);
                let attempt = MigrationAttempt {
                    from: 1,
                    to: 0,
                    from_incarnation: worlds[1].incarnation(),
                    to_incarnation: worlds[0].incarnation(),
                    started: 0,
                };
                worlds[usize::from(attempt.from)].advance_incarnation();
                {
                    let mut control = control.borrow_mut();
                    control.placement.insert(volume, attempt.from);
                    control.migrations.insert(volume, attempt);
                    control
                        .deferred_source_recoveries
                        .insert(volume, (attempt.from, Verdict::ColdBoot));
                }
                let (reply, result) = request::<_, AdminResult>(());
                drop(reply);
                let (completed, _completion) = oneshot();

                migration_completion(
                    volume,
                    attempt,
                    result,
                    completed,
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                )
                .await;

                assert!(!control.borrow().migrations.contains_key(&volume));
                assert!(!control.borrow().uncertain_migrations.contains(&volume));
                assert!(
                    !control
                        .borrow()
                        .deferred_source_recoveries
                        .contains_key(&volume)
                );
                assert_eq!(control.borrow().report.migrations_refused, 1);
                assert_eq!(control.borrow().report.recoveries, 1);
                assert!(
                    control
                        .borrow()
                        .guests
                        .borrow()
                        .get(&volume)
                        .is_some_and(Option::is_some)
                );
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn stale_source_recovery_cannot_resolve_a_closed_migration() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(109, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let volume = VolumeId(1);
                let control = control(1, config.hosts);
                let attempt = MigrationAttempt {
                    from: 1,
                    to: 0,
                    from_incarnation: worlds[1].incarnation(),
                    to_incarnation: worlds[0].incarnation(),
                    started: 0,
                };
                {
                    let mut control = control.borrow_mut();
                    control.placement.insert(volume, attempt.from);
                    control.migrations.insert(volume, attempt);
                }
                let lifecycle = spawn(lifecycle_actor(
                    attempt.from,
                    Rc::clone(&worlds[usize::from(attempt.from)]),
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                ));
                AdminIo::emit_admin_event(
                    worlds[usize::from(attempt.from)].as_ref(),
                    AdminEvent::VolumeRecovered {
                        volume,
                        verdict: Verdict::ColdBoot,
                    },
                )
                .await;
                delay(1).await;
                assert!(
                    !control
                        .borrow()
                        .deferred_source_recoveries
                        .contains_key(&volume)
                );

                let (reply, result) = request::<_, AdminResult>(());
                drop(reply);
                let (completed, _completion) = oneshot();
                migration_completion(
                    volume,
                    attempt,
                    result,
                    completed,
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                )
                .await;

                assert_eq!(control.borrow().migrations.get(&volume), Some(&attempt));
                assert!(control.borrow().uncertain_migrations.contains(&volume));
                AdminIo::emit_admin_event(
                    worlds[usize::from(attempt.from)].as_ref(),
                    AdminEvent::VolumeRecovered {
                        volume,
                        verdict: Verdict::ColdBoot,
                    },
                )
                .await;
                delay(1).await;
                assert_eq!(control.borrow().migrations.get(&volume), Some(&attempt));
                assert!(control.borrow().uncertain_migrations.contains(&volume));
                drop(lifecycle);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_migration_waits_for_source_recovery_before_guest_restart() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let config = Rc::new(config);
        simulate!(110, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::clone(&config);
            async move {
                let volume = VolumeId(1);
                let control = control(1, config.hosts);
                let attempt = MigrationAttempt {
                    from: 1,
                    to: 0,
                    from_incarnation: worlds[1].incarnation(),
                    to_incarnation: worlds[0].incarnation(),
                    started: 0,
                };
                {
                    let mut control = control.borrow_mut();
                    control.placement.insert(volume, attempt.from);
                    control.up[usize::from(attempt.from)] = false;
                    control.guests.borrow_mut().insert(volume, None);
                }

                restart_source_after_migration_refusal(
                    volume, attempt, None, &control, &worlds, &config,
                );
                assert!(control.borrow().guests.borrow()[&volume].is_none());

                control.borrow_mut().up[usize::from(attempt.from)] = true;
                worlds[usize::from(attempt.from)].advance_incarnation();
                restart_source_after_migration_refusal(
                    volume, attempt, None, &control, &worlds, &config,
                );
                assert!(control.borrow().guests.borrow()[&volume].is_none());

                restart_source_after_migration_refusal(
                    volume,
                    attempt,
                    Some((attempt.from, Verdict::ColdBoot)),
                    &control,
                    &worlds,
                    &config,
                );
                assert!(control.borrow().guests.borrow()[&volume].is_some());
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn source_restart_during_quiescing_applies_deferred_recovery() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(103, {
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let volume = VolumeId(1);
                let control = control(1, config.hosts);
                control
                    .borrow()
                    .guests
                    .borrow_mut()
                    .insert(volume, Some(spawn(delay(10))));
                let migration = spawn({
                    let worlds = Rc::clone(&worlds);
                    let control = Rc::clone(&control);
                    let config = Rc::clone(&config);
                    async move { start_migration(volume, 1, &worlds, &control, &config).await }
                });
                delay(1).await;
                worlds[0].advance_incarnation();
                control.borrow_mut().deferred_source_recoveries.insert(
                    volume,
                    (
                        0,
                        Verdict::Resume {
                            epoch: Epoch(1),
                            vmstate: 0,
                        },
                    ),
                );

                assert!(migration.await.expect("migration task completes").is_none());
                assert!(!control.borrow().quiescing_guests.contains(&volume));
                assert!(
                    control
                        .borrow()
                        .guests
                        .borrow()
                        .get(&volume)
                        .is_some_and(Option::is_some)
                );
                assert!(
                    !control
                        .borrow()
                        .deferred_source_recoveries
                        .contains_key(&volume)
                );
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_checkpoint_schedule_coalesces_each_volume() {
        let config = crate::presets::migration_chaos();
        let worlds = SimWorld::cluster(1, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(102, {
            let worlds = Rc::clone(&worlds);
            async move {
                let control = control(1, 1);
                let schedule = spawn(checkpoint_schedule(1, 100, Rc::clone(&worlds), control));
                let first = AdminIo::next_admin(worlds[0].as_ref())
                    .await
                    .expect("first checkpoint request");
                delay(5).await;
                assert!(
                    timeout(0, AdminIo::next_admin(worlds[0].as_ref()))
                        .await
                        .is_err()
                );
                let (_, mut reply) = first.into_parts();
                let _ = reply.send(Err(AdminError::Busy));
                let second = AdminIo::next_admin(worlds[0].as_ref())
                    .await
                    .expect("checkpoint after completion");
                assert_eq!(
                    second.into_parts().0,
                    AdminCall::Checkpoint {
                        retry: ReqId(2),
                        volume: VolumeId(1),
                    }
                );
                drop(schedule);
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_checkpoint_schedule_drains_an_admitted_request_past_the_horizon() {
        let config = crate::presets::migration_chaos();
        let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(113, {
            let worlds = Rc::clone(&worlds);
            async move {
                let control = control(1, config.hosts);
                let schedule = spawn(checkpoint_schedule(
                    1,
                    2,
                    Rc::clone(&worlds),
                    Rc::clone(&control),
                ));
                let request = AdminIo::next_admin(worlds[0].as_ref())
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

    #[tokio::test(start_paused = true)]
    async fn cluster_checkpoint_schedule_has_a_global_bound() {
        let config = crate::presets::migration_chaos();
        let worlds = SimWorld::cluster(1, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        simulate!(103, {
            let worlds = Rc::clone(&worlds);
            async move {
                let count = u64::try_from(CHECKPOINT_CONCURRENCY + 8).expect("count fits");
                let control = control(count, 1);
                let schedule = spawn(checkpoint_schedule(1, 100, Rc::clone(&worlds), control));
                let mut replies = Vec::new();
                for number in 1..=CHECKPOINT_CONCURRENCY {
                    let request = AdminIo::next_admin(worlds[0].as_ref())
                        .await
                        .expect("bounded checkpoint request");
                    let (call, reply) = request.into_parts();
                    assert!(matches!(
                        call,
                        AdminCall::Checkpoint { volume, .. }
                            if volume == VolumeId(u64::try_from(number).expect("volume fits"))
                    ));
                    replies.push(reply);
                }
                assert!(
                    timeout(0, AdminIo::next_admin(worlds[0].as_ref()))
                        .await
                        .is_err()
                );
                let _ = replies[0].send(Err(AdminError::Busy));
                let next = AdminIo::next_admin(worlds[0].as_ref())
                    .await
                    .expect("checkpoint refill request");
                assert!(matches!(
                    next.into_parts().0,
                    AdminCall::Checkpoint { volume, .. }
                        if volume
                            == VolumeId(
                                u64::try_from(CHECKPOINT_CONCURRENCY + 1).expect("volume fits")
                            )
                ));
                drop(schedule);
            }
        });
    }
}
