//! Multi-host deterministic harness over the shared async actor worlds.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use blockd_core::engine::{HostState, host_actor_with_state};
use blockd_core::head::{HeadRecord, ManifestPtr};
use blockd_core::hostmeta::{Counters, HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::JournalRecord;
use blockd_core::layout;
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::{AdminCall, AdminEvent, AdminResult, AdminSuccess, ReqId, Verdict};
use blockd_core::replica_recovery::{
    ReplicaResidue, export_replica_recovery, refence_replica_export,
};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis, page_size};
use blockd_core::world::{Store, StoreError};
use blockd_exec::channel::{OneReceiver, OneSender, oneshot, unbounded};
use blockd_exec::inject::{Injector, Lane, injector};
use blockd_exec::rng::Ppm;
use blockd_exec::{
    BridgeReceiver, BridgeRecvError, Either, Executor, FaultConfig, TaskHandle, TaskSet, delay,
    now, random_u64, select2, spawn,
};

use crate::actor_world::{SimNetwork, SimWorld};
use crate::cluster::{ClusterConfig, ClusterReport};
use crate::guest::page_pattern;
use crate::world::blobdev::CrashFate;

type SharedState = Rc<RefCell<HostState>>;
type HostSlots = Rc<Vec<RefCell<Option<TaskHandle<()>>>>>;
type StateSlots = Rc<Vec<RefCell<SharedState>>>;
type GuestSlots = Rc<RefCell<BTreeMap<VsetId, Option<TaskHandle<()>>>>>;
const CHECKPOINT_CONCURRENCY: usize = 32;
const CREATION_CONCURRENCY: usize = 32;
const MIGRATION_CONCURRENCY: usize = 8;
const RESTORE_CONCURRENCY: usize = 32;

#[derive(Default)]
struct GuestState {
    completed: Cell<u64>,
    total_completed: Cell<u64>,
    expected: RefCell<BTreeMap<PageId, Vec<u8>>>,
    durable: RefCell<BTreeMap<PageId, Vec<u8>>>,
    written: RefCell<BTreeMap<PageId, BTreeSet<u64>>>,
    recovering: RefCell<BTreeSet<PageId>>,
    volume_sequences: RefCell<BTreeMap<VolumeId, u64>>,
    mutation: Cell<u64>,
    history: RefCell<BTreeMap<u64, BTreeMap<PageId, Vec<u8>>>>,
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
    placement: BTreeMap<VsetId, u16>,
    guests: GuestSlots,
    guest_state: BTreeMap<VsetId, Rc<GuestState>>,
    migrations: BTreeMap<VsetId, MigrationAttempt>,
    uncertain_migrations: BTreeSet<VsetId>,
    accepted_migrations: BTreeSet<VsetId>,
    deferred_source_recoveries: BTreeMap<VsetId, (u16, Verdict)>,
    deferred_destination_recoveries: BTreeMap<VsetId, (u16, Verdict)>,
    quiescing_guests: BTreeSet<VsetId>,
    migration_cuts: BTreeSet<VsetId>,
    next_req: u64,
    live: Vec<bool>,
    up: Vec<bool>,
    workload_end: Cell<u64>,
    report: ClusterReport,
    sync_latencies: Vec<u64>,
    retired_counters: Vec<Counters>,
}

impl Control {
    fn req(&mut self) -> ReqId {
        let req = ReqId(self.next_req);
        self.next_req = self
            .next_req
            .checked_add(1)
            .expect("cluster request overflow");
        req
    }
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub(crate) fn run(seed: u64, mut config: ClusterConfig) -> ClusterReport {
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
    let (network, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
    let worlds = Rc::new(worlds);
    if config.sabotage == Some(crate::harness::Sabotage::EagerHandoffAck) {
        for world in worlds.iter() {
            world.set_drop_handoff_writes(true);
        }
    }
    network.set_latency(1_000, 100_000);
    network.configure_faults(
        config.peer_drop,
        config.peer_dup,
        config
            .peer_link_outages
            .iter()
            .map(|&(begin, end, from, to)| (begin, end, HostId(from), HostId(to)))
            .collect(),
        config
            .drop_peer
            .map(|(kind, begin, end)| (kind as u8, begin, end)),
    );

    let states = Rc::new(
        (0..config.hosts)
            .map(|host| {
                RefCell::new(Rc::new(RefCell::new(HostState::new(host_config(
                    &config, host,
                )))))
            })
            .collect::<Vec<_>>(),
    );
    let slots: HostSlots = Rc::new((0..config.hosts).map(|_| RefCell::new(None)).collect());
    let guest_slots: GuestSlots = Rc::new(RefCell::new(BTreeMap::new()));
    let guest_state = (1..=config.vset_count)
        .map(|number| (VsetId(u64::from(number)), Rc::new(GuestState::default())))
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
        next_req: 1,
        live: vec![true; usize::from(config.hosts)],
        up: vec![true; usize::from(config.hosts)],
        workload_end: Cell::new(0),
        report: ClusterReport::default(),
        sync_latencies: Vec::new(),
        retired_counters: Vec::new(),
    }));

    let config = Rc::new(config);
    let mut executor = Executor::simulation(seed);
    let mut fault_config = FaultConfig::disabled();
    for &point in &config.fault_points {
        fault_config.force(point, [true]);
    }
    executor.set_fault_config(fault_config);

    for host in 0..config.hosts {
        let state = Rc::clone(&states[usize::from(host)].borrow());
        *slots[usize::from(host)].borrow_mut() = Some(executor.spawn(host_actor_with_state(
            state,
            Rc::clone(&worlds[usize::from(host)]),
        )));
        executor
            .spawn(lifecycle_actor(
                host,
                Rc::clone(&worlds[usize::from(host)]),
                Rc::clone(&control),
                Rc::clone(&worlds),
                Rc::clone(&config),
            ))
            .detach();
        executor
            .spawn(abort_monitor(
                host,
                Rc::clone(&config),
                Rc::clone(&worlds),
                Rc::clone(&network),
                Rc::clone(&slots),
                Rc::clone(&states),
                Rc::clone(&control),
            ))
            .detach();
    }

    let initial_vsets = executor.block_on(create_initial_vsets(
        Rc::clone(&config),
        Rc::clone(&worlds),
        Rc::clone(&control),
    ));
    let workload_end = executor.now().saturating_add(config.horizon);
    control.borrow().workload_end.set(workload_end);
    let simulation_end = workload_end.saturating_add(2 * millis(1_000));

    spawn_schedules(
        &mut executor,
        &config,
        Rc::clone(&worlds),
        Rc::clone(&network),
        Rc::clone(&slots),
        Rc::clone(&states),
        Rc::clone(&control),
    );
    let guest_config = Rc::clone(&config);
    let guest_worlds = Rc::clone(&worlds);
    let guest_control = Rc::clone(&control);
    executor.block_on(async move {
        for (vset, host) in initial_vsets {
            start_guest(vset, host, &guest_control, &guest_worlds, &guest_config);
        }
    });
    executor.run_until(simulation_end);

    let audit = executor.block_on(audit_cluster(
        Rc::clone(&config),
        Rc::clone(&worlds),
        Rc::clone(&states),
        Rc::clone(&control),
    ));
    {
        let mut control = control.borrow_mut();
        control.report.audit_runs = 1;
        control.report.audited_vsets = audit.vsets;
        control.report.audited_pages = audit.pages;
        control.report.violations.extend(audit.violations);
    }

    for guest in guest_slots.borrow_mut().values_mut() {
        if let Some(mut guest) = guest.take() {
            guest.cancel();
        }
    }
    for slot in slots.iter() {
        if let Some(mut host) = slot.borrow_mut().take() {
            host.cancel();
        }
    }
    executor.run_ready();

    let mut control = control.borrow_mut();
    control.report.trace_hash = executor.trace_hash();
    control.report.fault_coverage = executor.fault_hits();
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
        .map(|state| state.borrow().borrow().stats().parked_faults)
        .sum();
    control.report.hydrating_end = states
        .iter()
        .filter(|state| control.live[usize::from(state.borrow().borrow().config.host.0)])
        .map(|state| {
            state
                .borrow()
                .borrow()
                .stats()
                .vsets
                .iter()
                .filter(|vset| matches!(vset.role, blockd_core::hostmeta::VsetRole::Hydrating))
                .count()
        })
        .sum();
    control.sync_latencies.sort_unstable();
    let sync_latencies = control.sync_latencies.clone();
    summarize_latencies(&mut control.report, &sync_latencies);
    let (drops, dups, clogs, targeted, releases) = network.counters();
    control.report.peer_drops = drops;
    control.report.peer_dups = dups;
    control.report.peer_link_clogs = clogs;
    control.report.nemesis_drops = targeted;
    control.report.releases = releases;
    let (unavailable, conflicts) = worlds[0].store_counters();
    control.report.store_unavailable = unavailable;
    control.report.store_cas_conflicts = conflicts;
    control.report.disk_bitflips = worlds.iter().map(|world| world.bitflips()).sum();
    control.report.blobs_per_host = worlds.iter().map(|world| world.blob_count()).collect();
    std::mem::take(&mut control.report)
}

#[derive(Default)]
struct AuditReport {
    vsets: u64,
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
    for number in 1..=config.vset_count {
        let vset = VsetId(u64::from(number));
        let Some(placed) = control.borrow().placement.get(&vset).copied() else {
            audit
                .violations
                .push(format!("final audit found no placement for {vset:?}"));
            continue;
        };
        if !control.borrow().live[usize::from(placed)] || !control.borrow().up[usize::from(placed)]
        {
            audit.violations.push(format!(
                "final audit placement for {vset:?} points at unavailable host {placed}"
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
                .vsets
                .get(&vset)
                .is_some_and(|vset_state| vset_state.ready && vset_state.outbound.is_none())
            {
                authorities.push(host);
            }
        }
        if authorities.len() > 1 {
            audit.violations.push(format!(
                "final audit found multiple authorities for {vset:?}: {authorities:?}"
            ));
            continue;
        }
        let lifecycle_in_progress = {
            let control = control.borrow();
            control.migrations.contains_key(&vset)
                || control.quiescing_guests.contains(&vset)
                || control.migration_cuts.contains(&vset)
                || control.deferred_source_recoveries.contains_key(&vset)
                || control.deferred_destination_recoveries.contains_key(&vset)
        };
        let Some(&authority) = authorities.first() else {
            if !lifecycle_in_progress {
                audit.violations.push(format!(
                    "final audit expected authority {placed} for {vset:?}, found none"
                ));
            }
            continue;
        };
        if authority != placed && !lifecycle_in_progress {
            audit.violations.push(format!(
                "final audit expected authority {placed} for {vset:?}, found {authority}"
            ));
            continue;
        }

        {
            let state = states[usize::from(authority)].borrow();
            let state = state.borrow();
            let Some(vset_state) = state.vsets.get(&vset) else {
                audit.violations.push(format!(
                    "final audit authority {placed} has no state for {vset:?}"
                ));
                continue;
            };
            if vset_state.local_covered_through < vset_state.sync_ack_through {
                audit.violations.push(format!(
                    "final audit found local coverage {} behind acknowledged sync {} for {vset:?}",
                    vset_state.local_covered_through, vset_state.sync_ack_through
                ));
            }
            let archived_through = vset_state
                .backed
                .and_then(|pointer| {
                    store.get(&layout::manifest_key(vset, pointer.fence, pointer.seq))
                })
                .and_then(|bytes| JournalRecord::decode(vset, bytes).ok())
                .map_or(0, |record| record.sync_covered_through);
            let protected_through = vset_state.peer_committed_through.max(archived_through);
            if protected_through < vset_state.sync_ack_through {
                audit.violations.push(format!(
                    "final audit found protected coverage {protected_through} behind acknowledged sync {} for {vset:?}",
                    vset_state.sync_ack_through
                ));
            }
        }

        if let Some(head_bytes) = store.get(&layout::head_key(vset)) {
            match HeadRecord::decode(vset, head_bytes) {
                Ok(head) => {
                    if head.holder != HostId(placed) {
                        audit.violations.push(format!(
                            "final audit head for {vset:?} names {:?}, placement names {:?}",
                            head.holder,
                            HostId(placed)
                        ));
                    }
                    if let Some(pointer) = head.manifest {
                        let key = layout::manifest_key(vset, pointer.fence, pointer.seq);
                        match store
                            .get(&key)
                            .and_then(|bytes| JournalRecord::decode(vset, bytes).ok())
                        {
                            Some(record)
                                if (record.fence, record.seq, record.capture_seq)
                                    == (pointer.fence, pointer.seq, pointer.capture_seq) => {}
                            _ => audit.violations.push(format!(
                                "final audit could not verify head manifest {key} for {vset:?}"
                            )),
                        }
                    }
                }
                Err(_) => audit
                    .violations
                    .push(format!("final audit could not decode head for {vset:?}")),
            }
        }

        let guest = Rc::clone(&control.borrow().guest_state[&vset]);
        let world = &worlds[usize::from(authority)];
        let violations_before = audit.violations.len();
        for volume in config.vset_config.volumes(vset) {
            for page_number in 0..config.vset_config.pages_per_volume {
                let page = PageId {
                    volume,
                    page: PageNo(page_number),
                };
                let faulted = match select2(world.fault(page, false), delay(millis(250))).await {
                    Either::First(faulted) => faulted,
                    Either::Second(()) => false,
                };
                world.set_vmstate(vset, guest.completed.get());
                if !faulted {
                    audit.violations.push(format!(
                        "final audit could not fault {page:?} on authority {authority}"
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
            audit.vsets = audit.vsets.saturating_add(1);
        }
    }
    audit
}

#[allow(clippy::too_many_arguments)]
async fn abort_monitor(
    host: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    network: Rc<SimNetwork>,
    slots: HostSlots,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
) {
    while worlds[usize::from(host)].next_abort().await.is_some() {
        crash_host(host, &config, &worlds, &network, &slots, &states, &control).await;
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

fn vset_config(config: &ClusterConfig, _vset: VsetId) -> blockd_core::journal::VsetConfig {
    config.vset_config
}

async fn create_initial_vsets(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) -> Vec<(VsetId, u16)> {
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = 0usize;
    let mut next = 1u64;
    let mut created_vsets = Vec::with_capacity(usize::from(config.vset_count));
    while next <= u64::from(config.vset_count) || active != 0 {
        while active < CREATION_CONCURRENCY && next <= u64::from(config.vset_count) {
            let number = u16::try_from(next).expect("vset number fits");
            next += 1;
            let vset = VsetId(u64::from(number));
            let host = (number - 1) % config.hosts;
            control.borrow_mut().placement.insert(vset, host);
            let reply = worlds[usize::from(host)].request_admin(AdminCall::CreateVset {
                vset,
                config: vset_config(&config, vset),
                from_base: None,
            });
            let completed = completed.clone();
            actors.spawn(async move {
                let created = matches!(
                    reply.await,
                    Ok(Ok(AdminSuccess::VsetCreated { vset: created_vset }))
                        if created_vset == vset
                );
                let _ = completed.send((vset, host, created));
            });
            active += 1;
        }
        if active != 0 {
            let (vset, host, created) = completions
                .recv()
                .await
                .expect("initial creation workers remain connected");
            assert!(created, "initial vset creation failed for {vset:?}");
            created_vsets.push((vset, host));
            active -= 1;
        }
    }
    created_vsets.sort_unstable_by_key(|(vset, _)| *vset);
    created_vsets
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
            AdminEvent::VsetMigratedIn { vset, verdict } => {
                let attempt = control
                    .borrow()
                    .migrations
                    .get(&vset)
                    .copied()
                    .filter(|attempt| attempt.to == host);
                if let Some(attempt) = attempt {
                    discard_deferred_source_recovery(vset, attempt, &control);
                }
                let migrated = control.borrow().guest_state[&vset]
                    .expected
                    .borrow()
                    .clone();
                *control.borrow().guest_state[&vset].durable.borrow_mut() = migrated;
                let runnable = prepare_recovered(
                    &control.borrow().guest_state[&vset],
                    vset,
                    &config,
                    world.as_ref(),
                    verdict,
                );
                {
                    let mut control = control.borrow_mut();
                    if let Some(attempt) = attempt {
                        control.migrations.remove(&vset);
                        control.uncertain_migrations.remove(&vset);
                        control.accepted_migrations.remove(&vset);
                        control.deferred_destination_recoveries.remove(&vset);
                        control.report.migrations = control.report.migrations.saturating_add(1);
                        control.report.max_migration_pause_ns = control
                            .report
                            .max_migration_pause_ns
                            .max(now().saturating_sub(attempt.started));
                    }
                    control.placement.insert(vset, host);
                }
                if runnable {
                    start_guest(vset, host, &control, &worlds, &config);
                }
            }
            AdminEvent::VsetRecovered { vset, verdict } => {
                let pending_migration = {
                    let control = control.borrow();
                    control.migrations.get(&vset).copied().filter(|attempt| {
                        attempt.to == host
                            && world.incarnation() != attempt.to_incarnation
                            && (control.uncertain_migrations.contains(&vset)
                                || control.accepted_migrations.contains(&vset))
                    })
                };
                if let Some(attempt) = pending_migration {
                    finalize_destination_migration(
                        vset, attempt, host, verdict, &control, &worlds, &config,
                    );
                    continue;
                }
                let pending_destination = control
                    .borrow()
                    .migrations
                    .get(&vset)
                    .copied()
                    .is_some_and(|attempt| {
                        attempt.to == host && world.incarnation() != attempt.to_incarnation
                    });
                if pending_destination {
                    control
                        .borrow_mut()
                        .deferred_destination_recoveries
                        .insert(vset, (host, verdict));
                    continue;
                }
                let returned_to_source = {
                    let control = control.borrow();
                    control.migrations.get(&vset).copied().filter(|attempt| {
                        attempt.from == host
                            && world.incarnation() != attempt.from_incarnation
                            && control.uncertain_migrations.contains(&vset)
                    })
                };
                if let Some(attempt) = returned_to_source {
                    let mut control = control.borrow_mut();
                    if control.migrations.get(&vset) == Some(&attempt) {
                        control.migrations.remove(&vset);
                        control.uncertain_migrations.remove(&vset);
                        control.accepted_migrations.remove(&vset);
                        control.deferred_source_recoveries.remove(&vset);
                        control.deferred_destination_recoveries.remove(&vset);
                        control.report.migrations_refused =
                            control.report.migrations_refused.saturating_add(1);
                    }
                }
                let active_migration = control
                    .borrow()
                    .migrations
                    .get(&vset)
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
                            .insert(vset, (host, verdict));
                    }
                    continue;
                }
                let already_elsewhere =
                    control
                        .borrow()
                        .placement
                        .get(&vset)
                        .is_some_and(|&placed| {
                            placed != host && control.borrow().live[usize::from(placed)]
                        });
                if already_elsewhere {
                    if !matches!(verdict, Verdict::Unrestorable) {
                        control
                            .borrow_mut()
                            .report
                            .violations
                            .push(format!("two runners recovered for {vset:?}"));
                    }
                    continue;
                }
                let runnable = prepare_recovered(
                    &control.borrow().guest_state[&vset],
                    vset,
                    &config,
                    world.as_ref(),
                    verdict,
                );
                {
                    let mut control = control.borrow_mut();
                    control.report.recoveries = control.report.recoveries.saturating_add(1);
                    if runnable {
                        control.placement.insert(vset, host);
                    }
                }
                if runnable {
                    start_guest(vset, host, &control, &worlds, &config);
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
    vset: VsetId,
    sent: u64,
    mut reply: BridgeReceiver<AdminResult>,
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
                .get(&vset)
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
        reply = worlds[usize::from(host)].request_admin(AdminCall::RestoreVset { vset });
    };
    let Ok(reply) = reply else {
        let mut control = control.borrow_mut();
        control.report.claims_lost = control.report.claims_lost.saturating_add(1);
        return;
    };
    let Ok(AdminSuccess::VsetRestored {
        vset: restored,
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
    if restored != vset {
        control.borrow_mut().report.violations.push(format!(
            "restore reply changed vset from {vset:?} to {restored:?}"
        ));
        return;
    }
    let runnable = prepare_recovered(
        &control.borrow().guest_state[&vset],
        vset,
        &config,
        worlds[usize::from(target.host)].as_ref(),
        verdict,
    );
    {
        let mut control = control.borrow_mut();
        control.placement.insert(vset, target.host);
        control.report.restores = control.report.restores.saturating_add(1);
        control.report.loss_bound_verified = control.report.loss_bound_verified.saturating_add(1);
        control.report.max_restore_ns = control
            .report
            .max_restore_ns
            .max(now().saturating_sub(sent));
    }
    if runnable {
        start_guest(vset, target.host, &control, &worlds, &config);
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
    vset: VsetId,
    attempt: MigrationAttempt,
    reply: BridgeReceiver<AdminResult>,
    completed: OneSender<()>,
    control: Rc<RefCell<Control>>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    config: Rc<ClusterConfig>,
) {
    let _completed = MigrationCompletionSignal(Some(completed));
    let result = reply.await;
    let succeeded = matches!(
        &result,
        Ok(Ok(AdminSuccess::MigratedOut { vset: migrated })) if *migrated == vset
    );
    if succeeded {
        let deferred = {
            let mut control = control.borrow_mut();
            if control.migrations.get(&vset) == Some(&attempt) {
                control.accepted_migrations.insert(vset);
                control.deferred_destination_recoveries.remove(&vset)
            } else {
                None
            }
        };
        if let Some((host, verdict)) = deferred {
            finalize_destination_migration(
                vset, attempt, host, verdict, &control, &worlds, &config,
            );
        }
        return;
    }
    if matches!(result, Err(BridgeRecvError::Closed)) {
        // Source cancellation is ambiguous: the destination may already have
        // durably accepted the handoff. Keep the attempt until recovery tells
        // us which side owns the vset.
        let (deferred_destination, deferred_source) = {
            let mut control = control.borrow_mut();
            if control.migrations.get(&vset) == Some(&attempt) {
                if let Some(destination) = control.deferred_destination_recoveries.remove(&vset) {
                    control.uncertain_migrations.insert(vset);
                    (Some(destination), None)
                } else if let Some(source) = control.deferred_source_recoveries.remove(&vset) {
                    control.migrations.remove(&vset);
                    control.uncertain_migrations.remove(&vset);
                    control.accepted_migrations.remove(&vset);
                    control.report.migrations_refused =
                        control.report.migrations_refused.saturating_add(1);
                    (None, Some(source))
                } else {
                    control.uncertain_migrations.insert(vset);
                    (None, None)
                }
            } else {
                (None, None)
            }
        };
        if let Some((host, verdict)) = deferred_destination {
            finalize_destination_migration(
                vset, attempt, host, verdict, &control, &worlds, &config,
            );
        } else if deferred_source.is_some() {
            restart_source_after_migration_refusal(
                vset,
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
        if control.migrations.get(&vset) == Some(&attempt) {
            control.migrations.remove(&vset);
            control.uncertain_migrations.remove(&vset);
            control.accepted_migrations.remove(&vset);
            control.deferred_destination_recoveries.remove(&vset);
            control.report.migrations_refused = control.report.migrations_refused.saturating_add(1);
            (true, control.deferred_source_recoveries.remove(&vset))
        } else {
            (false, None)
        }
    };
    if removed {
        restart_source_after_migration_refusal(
            vset,
            attempt,
            deferred_recovery,
            &control,
            &worlds,
            &config,
        );
    }
}

fn restart_source_after_migration_refusal(
    vset: VsetId,
    attempt: MigrationAttempt,
    deferred_recovery: Option<(u16, Verdict)>,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    let source_ready = {
        let control = control.borrow();
        control.placement.get(&vset) == Some(&attempt.from) && control.up[usize::from(attempt.from)]
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
            &control.borrow().guest_state[&vset],
            vset,
            config,
            worlds[usize::from(host)].as_ref(),
            verdict,
        );
        let mut control = control.borrow_mut();
        control.report.recoveries = control.report.recoveries.saturating_add(1);
        runnable
    });
    if runnable {
        start_guest(vset, attempt.from, control, worlds, config);
    }
}

fn start_guest(
    vset: VsetId,
    host: u16,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    cancel_guest(vset, control);
    {
        let mut control = control.borrow_mut();
        control.quiescing_guests.remove(&vset);
        control.migration_cuts.remove(&vset);
    }
    let guest_state = Rc::clone(&control.borrow().guest_state[&vset]);
    worlds[usize::from(host)].set_vmstate(vset, guest_state.completed.get());
    let guest = spawn(guest_actor(
        Rc::clone(&worlds[usize::from(host)]),
        guest_state,
        Rc::clone(control),
        vset,
        Rc::clone(config),
    ));
    control
        .borrow()
        .guests
        .borrow_mut()
        .insert(vset, Some(guest));
}

fn cancel_guest(vset: VsetId, control: &Rc<RefCell<Control>>) {
    if let Some(Some(mut guest)) = control.borrow().guests.borrow_mut().remove(&vset) {
        guest.cancel();
    }
}

fn apply_deferred_source_recovery(
    vset: VsetId,
    attempt: MigrationAttempt,
    control: &Rc<RefCell<Control>>,
    worlds: &[Rc<SimWorld>],
    config: &ClusterConfig,
) -> Option<bool> {
    let deferred = control
        .borrow_mut()
        .deferred_source_recoveries
        .remove(&vset)?;
    debug_assert_eq!(deferred.0, attempt.from);
    let runnable = prepare_recovered(
        &control.borrow().guest_state[&vset],
        vset,
        config,
        worlds[usize::from(attempt.from)].as_ref(),
        deferred.1,
    );
    let mut control = control.borrow_mut();
    control.report.recoveries = control.report.recoveries.saturating_add(1);
    Some(runnable)
}

fn discard_deferred_source_recovery(
    vset: VsetId,
    attempt: MigrationAttempt,
    control: &Rc<RefCell<Control>>,
) {
    let mut control = control.borrow_mut();
    if let Some((host, _)) = control.deferred_source_recoveries.remove(&vset) {
        debug_assert_eq!(host, attempt.from);
        control.report.recoveries = control.report.recoveries.saturating_add(1);
    }
}

fn finalize_destination_migration(
    vset: VsetId,
    attempt: MigrationAttempt,
    host: u16,
    verdict: Verdict,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    if attempt.to != host || control.borrow().migrations.get(&vset) != Some(&attempt) {
        return;
    }
    discard_deferred_source_recovery(vset, attempt, control);
    let migrated = control.borrow().guest_state[&vset]
        .expected
        .borrow()
        .clone();
    *control.borrow().guest_state[&vset].durable.borrow_mut() = migrated;
    let runnable = prepare_recovered(
        &control.borrow().guest_state[&vset],
        vset,
        config,
        worlds[usize::from(host)].as_ref(),
        verdict,
    );
    {
        let mut control = control.borrow_mut();
        control.migrations.remove(&vset);
        control.uncertain_migrations.remove(&vset);
        control.accepted_migrations.remove(&vset);
        control.deferred_destination_recoveries.remove(&vset);
        control.placement.insert(vset, host);
        control.report.migrations = control.report.migrations.saturating_add(1);
        control.report.max_migration_pause_ns = control
            .report
            .max_migration_pause_ns
            .max(now().saturating_sub(attempt.started));
    }
    if runnable {
        start_guest(vset, host, control, worlds, config);
    }
}

fn prepare_recovered(
    state: &GuestState,
    vset: VsetId,
    config: &ClusterConfig,
    _world: &SimWorld,
    verdict: Verdict,
) -> bool {
    match verdict {
        Verdict::Resume { vmstate, .. } => {
            state.completed.set(vmstate);
            *state.expected.borrow_mut() = state.durable.borrow().clone();
        }
        Verdict::ColdBoot => {
            state.completed.set(0);
            let cold = state
                .durable
                .borrow()
                .iter()
                .filter(|(page, _)| page.volume.idx.0 != 0)
                .map(|(page, bytes)| (*page, bytes.clone()))
                .collect::<BTreeMap<_, _>>();
            state.expected.borrow_mut().clone_from(&cold);
            *state.durable.borrow_mut() = cold;
        }
        Verdict::Unrestorable => {
            state
                .violations
                .borrow_mut()
                .push(format!("vset {vset:?} became unrestorable"));
            return false;
        }
        Verdict::DatabaseReady { .. } => return false,
    }
    *state.recovering.borrow_mut() = config
        .vset_config
        .volumes(vset)
        .flat_map(|volume| {
            (0..config.vset_config.pages_per_volume).map(move |page| PageId {
                volume,
                page: PageNo(page),
            })
        })
        .collect();
    true
}

#[allow(clippy::too_many_lines)]
async fn guest_actor(
    world: Rc<SimWorld>,
    state: Rc<GuestState>,
    control: Rc<RefCell<Control>>,
    vset: VsetId,
    config: Rc<ClusterConfig>,
) {
    let mut next_req = (vset.0 << 48) | 1;
    loop {
        if control.borrow().quiescing_guests.contains(&vset) {
            return;
        }
        delay(random_between(config.think.0, config.think.1)).await;
        if control.borrow().quiescing_guests.contains(&vset) {
            return;
        }
        if now() > control.borrow().workload_end.get() {
            return;
        }
        let sync = config
            .guest_sync_share
            .map_or_else(|| random_u64() % 100 >= 85, hit);
        if sync {
            let volume = VolumeId {
                vset,
                idx: VolumeIdx(
                    1 + u8::try_from(
                        random_u64() % u64::from(config.vset_config.disk_volumes.max(1)),
                    )
                    .expect("volume index fits"),
                ),
            };
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
            let current = state.expected.borrow();
            let mut durable = state.durable.borrow_mut();
            durable.retain(|page, _| page.volume != volume);
            durable.extend(
                current
                    .iter()
                    .filter(|(page, _)| page.volume == volume)
                    .map(|(page, bytes)| (*page, bytes.clone())),
            );
            finish_operation(&world, &state, &control, vset);
            continue;
        }
        let page = choose_page(&config, vset);
        let write = random_u64() % 100 < 60;
        let mutation = write && world.write_fault_mutates(page);
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
            if mutation {
                state.mutation.set(state.mutation.get().saturating_add(1));
            }
            state
                .history
                .borrow_mut()
                .insert(state.mutation.get(), state.expected.borrow().clone());
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
        finish_operation(&world, &state, &control, vset);
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
    vset: VsetId,
) {
    if control.borrow().migration_cuts.contains(&vset) {
        state.violations.borrow_mut().push(format!(
            "guest operation completed after the migration cut for {vset:?}"
        ));
    }
    let completed = state.completed.get().saturating_add(1);
    state.completed.set(completed);
    state
        .total_completed
        .set(state.total_completed.get().saturating_add(1));
    world.set_vmstate(vset, completed);
}

fn choose_page(config: &ClusterConfig, vset: VsetId) -> PageId {
    let idx = VolumeIdx(
        u8::try_from(random_u64() % (u64::from(config.vset_config.disk_volumes) + 1))
            .expect("volume index fits"),
    );
    PageId {
        volume: VolumeId { vset, idx },
        page: PageNo(
            u32::try_from(random_u64() % u64::from(config.vset_config.pages_per_volume))
                .expect("page number fits"),
        ),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn spawn_schedules(
    executor: &mut Executor,
    config: &Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    network: Rc<SimNetwork>,
    slots: HostSlots,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
) {
    for &(at, host) in &config.crash_hosts_at {
        executor
            .spawn(at_crash(
                at,
                host,
                Rc::clone(config),
                Rc::clone(&worlds),
                Rc::clone(&network),
                Rc::clone(&slots),
                Rc::clone(&states),
                Rc::clone(&control),
            ))
            .detach();
    }
    for &(at, host) in &config.kill_hosts_at {
        executor
            .spawn(at_kill(
                at,
                host,
                Rc::clone(config),
                Rc::clone(&worlds),
                Rc::clone(&network),
                Rc::clone(&slots),
                Rc::clone(&states),
                Rc::clone(&control),
            ))
            .detach();
    }
    for &(at, vset, to) in &config.migrate_at {
        executor
            .spawn(at_migrate(
                at,
                vset,
                to,
                Rc::clone(config),
                Rc::clone(&worlds),
                Rc::clone(&control),
            ))
            .detach();
    }
    if let Some((begin, end)) = config.store_outage {
        let world = Rc::clone(&worlds[0]);
        executor
            .spawn(async move {
                delay(begin).await;
                world.set_store_outage(true);
                delay(end.saturating_sub(begin)).await;
                world.set_store_outage(false);
            })
            .detach();
    }
    if let Some(at) = config.rot_resume_set_at {
        let world = Rc::clone(&worlds[0]);
        executor
            .spawn(async move {
                delay(at).await;
                let _ = world.rot_store_suffix("/rs");
            })
            .detach();
    }
    if let Some(at) = config.rot_leaves_at {
        let world = Rc::clone(&worlds[0]);
        executor
            .spawn(async move {
                delay(at).await;
                let _ = world.rot_store_leaf();
            })
            .detach();
    }
    if let Some(interval) = config.checkpoint_interval {
        executor
            .spawn(checkpoint_schedule(
                interval,
                control.borrow().workload_end.get(),
                Rc::clone(&worlds),
                Rc::clone(&control),
            ))
            .detach();
    }
    if config.crash_mean_interval != 0 {
        executor
            .spawn(random_crashes(
                Rc::clone(config),
                Rc::clone(&worlds),
                Rc::clone(&network),
                Rc::clone(&slots),
                Rc::clone(&states),
                Rc::clone(&control),
            ))
            .detach();
    }
    if config.migrate_mean_interval != 0 {
        executor
            .spawn(random_migrations(
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
    network: Rc<SimNetwork>,
    slots: HostSlots,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
) {
    delay(at).await;
    crash_host(host, &config, &worlds, &network, &slots, &states, &control).await;
}

async fn crash_host(
    host: u16,
    config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    network: &SimNetwork,
    slots: &HostSlots,
    states: &StateSlots,
    control: &Rc<RefCell<Control>>,
) {
    let Some(mut actor) = slots[usize::from(host)].borrow_mut().take() else {
        return;
    };
    actor.cancel();
    control.borrow_mut().up[usize::from(host)] = false;
    worlds[usize::from(host)].advance_incarnation();
    network.set_host_down(HostId(host), true);
    let affected = control
        .borrow()
        .placement
        .iter()
        .filter_map(|(&vset, &placed)| (placed == host).then_some(vset))
        .collect::<Vec<_>>();
    for vset in affected {
        cancel_guest(vset, control);
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
    let state = Rc::new(RefCell::new(HostState::new(host_config(config, host))));
    *states[usize::from(host)].borrow_mut() = Rc::clone(&state);
    *slots[usize::from(host)].borrow_mut() = Some(spawn(host_actor_with_state(
        state,
        Rc::clone(&worlds[usize::from(host)]),
    )));
    control.borrow_mut().up[usize::from(host)] = true;
    network.set_host_down(HostId(host), false);
}

#[allow(clippy::too_many_arguments)]
async fn at_kill(
    at: u64,
    host: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    network: Rc<SimNetwork>,
    slots: HostSlots,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
) {
    delay(at).await;
    let Some(mut actor) = slots[usize::from(host)].borrow_mut().take() else {
        return;
    };
    actor.cancel();
    control.borrow_mut().up[usize::from(host)] = false;
    worlds[usize::from(host)].advance_incarnation();
    network.set_host_down(HostId(host), true);
    control.borrow_mut().live[usize::from(host)] = false;
    control
        .borrow_mut()
        .retired_counters
        .push(states[usize::from(host)].borrow().borrow().counters);
    let affected = control
        .borrow()
        .placement
        .iter()
        .filter_map(|(&vset, &placed)| (placed == host).then_some(vset))
        .collect::<Vec<_>>();
    let orphaned_at = now();
    let mut restore_tasks = TaskSet::new();
    let (restore_completed, mut restore_completions) = unbounded();
    let mut restores_active = 0usize;
    for vset in affected {
        cancel_guest(vset, &control);
        if !promote_orphan(host, vset, &config, &worlds, &control).await {
            control
                .borrow_mut()
                .report
                .violations
                .push(format!("unable to promote orphan {vset:?}"));
            continue;
        }
        prepare_backed_loss(&control.borrow().guest_state[&vset], vset, &worlds[0]);
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
                worlds[usize::from(candidate)].request_admin(AdminCall::RestoreVset { vset });
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
                    vset,
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
    vset: VsetId,
    config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    control: &Rc<RefCell<Control>>,
) -> bool {
    let (observed_version, head) = loop {
        match Store::get(worlds[0].as_ref(), &layout::head_key(vset)).await {
            Ok(Some((version, bytes))) => {
                let Ok(head) = HeadRecord::decode(vset, &bytes) else {
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
                vset: found_vset,
                assignment_epoch,
                generation,
            }) = layout::parse_blob(&name)
                && (found_source, found_vset) == (HostId(source), vset)
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
            vset,
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

    let retired = HeadRecord {
        vset,
        holder: HostId(source),
        fence: head.fence,
        manifest: head.manifest,
        stash: None,
        retired_stashes: Vec::new(),
    };
    let retired_version = loop {
        match Store::put_cas(
            worlds[0].as_ref(),
            layout::head_key(vset),
            Some(observed_version),
            retired.encode(),
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
    let Some(export) = export else {
        return true;
    };
    let Ok(export) = refence_replica_export(vset, &export, retired_version) else {
        return false;
    };
    let Some(record) = export
        .blobs
        .iter()
        .find_map(|(name, bytes)| {
            matches!(
                layout::parse_blob(name),
                Some(layout::BlobName::Journal { fence, .. }) if fence == retired_version
            )
            .then_some(bytes)
        })
        .and_then(|bytes| blockd_core::journal::JournalRecord::decode(vset, bytes).ok())
    else {
        return false;
    };
    for (name, bytes) in &export.blobs {
        let key = match layout::parse_blob(name) {
            Some(layout::BlobName::Segment { fence, seg, .. }) => {
                Some(layout::segment_key(vset, fence, seg))
            }
            Some(layout::BlobName::Leaf { fence, id, .. }) => {
                Some(layout::leaf_key(vset, fence, id))
            }
            _ => None,
        };
        let Some(key) = key else {
            continue;
        };
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
    let manifest = ManifestPtr {
        fence: retired_version,
        seq: record.seq,
        capture_seq: record.capture_seq,
    };
    loop {
        match Store::put(
            worlds[0].as_ref(),
            layout::manifest_key(vset, manifest.fence, manifest.seq),
            record.encode(vset),
        )
        .await
        {
            Ok(_) => break,
            Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                delay(config.daemon.backup_retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(_)) => return false,
        }
    }
    let promoted = HeadRecord {
        vset,
        holder: HostId(source),
        fence: retired_version,
        manifest: Some(manifest),
        stash: None,
        retired_stashes: Vec::new(),
    };
    loop {
        match Store::put_cas(
            worlds[0].as_ref(),
            layout::head_key(vset),
            Some(retired_version),
            promoted.encode(),
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

fn prepare_backed_loss(state: &GuestState, vset: VsetId, world: &SimWorld) {
    let capture_seq = world
        .store_bytes(&blockd_core::layout::head_key(vset))
        .and_then(|bytes| blockd_core::head::HeadRecord::decode(vset, &bytes).ok())
        .and_then(|head| head.manifest)
        .map_or(0, |manifest| manifest.capture_seq);
    let expected = state
        .history
        .borrow()
        .range(..=capture_seq)
        .next_back()
        .map_or_else(BTreeMap::new, |(_, snapshot)| snapshot.clone());
    state.mutation.set(capture_seq);
    state.expected.borrow_mut().clone_from(&expected);
    state.durable.borrow_mut().clone_from(&expected);
}

async fn at_migrate(
    at: u64,
    vset: VsetId,
    to: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) {
    delay(at).await;
    start_migration(vset, to, &worlds, &control, &config).await;
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
        vset: VsetId,
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
                rollback_quiescing_migration(vset, attempt, &control, &worlds, &config);
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
    vset: VsetId,
    attempt: MigrationAttempt,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    config: &Rc<ClusterConfig>,
) {
    let (source_up, original_incarnation, deferred_recovery) = {
        let mut control = control.borrow_mut();
        if control.migrations.get(&vset) != Some(&attempt) {
            return;
        }
        control.migrations.remove(&vset);
        control.quiescing_guests.remove(&vset);
        control.uncertain_migrations.remove(&vset);
        control.accepted_migrations.remove(&vset);
        control.deferred_destination_recoveries.remove(&vset);
        (
            control.up[usize::from(attempt.from)]
                && control.placement.get(&vset) == Some(&attempt.from),
            worlds[usize::from(attempt.from)].incarnation() == attempt.from_incarnation,
            control.deferred_source_recoveries.contains_key(&vset),
        )
    };
    if source_up && (original_incarnation || deferred_recovery) {
        let runnable =
            apply_deferred_source_recovery(vset, attempt, control, worlds, config).unwrap_or(true);
        if runnable {
            start_guest(vset, attempt.from, control, worlds, config);
        }
    }
}

async fn start_migration(
    vset: VsetId,
    to: u16,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    control: &Rc<RefCell<Control>>,
    config: &Rc<ClusterConfig>,
) -> Option<OneReceiver<()>> {
    let &from = control.borrow().placement.get(&vset)?;
    let unavailable = {
        let control = control.borrow();
        let guest_active = control
            .guests
            .borrow()
            .get(&vset)
            .is_some_and(Option::is_some);
        !control.up[usize::from(from)]
            || !control.live[usize::from(to)]
            || !control.up[usize::from(to)]
            || !guest_active
            || control.migrations.contains_key(&vset)
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
        control.quiescing_guests.insert(vset);
        control.uncertain_migrations.remove(&vset);
        control.accepted_migrations.remove(&vset);
        control.deferred_source_recoveries.remove(&vset);
        control.deferred_destination_recoveries.remove(&vset);
        control.migrations.insert(vset, attempt);
    }
    let quiescing = QuiescingMigrationGuard::new(vset, attempt, control, worlds, config);
    let guest = {
        let control = control.borrow();
        let removed = control.guests.borrow_mut().remove(&vset);
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
            && control.placement.get(&vset) == Some(&from)
            && control.migrations.get(&vset) == Some(&attempt)
    };
    if !can_submit {
        rollback_quiescing_migration(vset, attempt, control, worlds, config);
        quiescing.disarm();
        return None;
    }
    let guest_state = Rc::clone(&control.borrow().guest_state[&vset]);
    worlds[usize::from(from)].set_vmstate(vset, guest_state.completed.get());
    control.borrow_mut().migration_cuts.insert(vset);
    let reply = worlds[usize::from(from)].request_admin(AdminCall::MigrateOut {
        vset,
        to: HostId(to),
    });
    let (completed, completion) = oneshot();
    spawn(migration_completion(
        vset,
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
            for &vset in control.borrow().placement.keys() {
                if !active.contains(&vset) && queued.insert(vset) {
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
            let host = {
                let control = control.borrow();
                control.placement.get(&vset).copied().filter(|host| {
                    control.up[usize::from(*host)]
                        && control
                            .guests
                            .borrow()
                            .get(&vset)
                            .is_some_and(Option::is_some)
                })
            };
            let Some(host) = host else {
                continue;
            };
            assert!(active.insert(vset));
            let req = control.borrow_mut().req();
            let reply =
                worlds[usize::from(host)].request_admin(AdminCall::Checkpoint { retry: req, vset });
            let completed = completed.clone();
            actors.spawn(async move {
                let _ = reply.await;
                let _ = completed.send(vset);
            });
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

async fn random_crashes(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    network: Rc<SimNetwork>,
    slots: HostSlots,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
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
        crash_host(host, &config, &worlds, &network, &slots, &states, &control).await;
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
            Either::First(Some(vset)) => {
                active.remove(&vset);
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
            .filter_map(|(&vset, &host)| {
                let control = control.borrow();
                (!active.contains(&vset)
                    && control.up[usize::from(host)]
                    && control
                        .guests
                        .borrow()
                        .get(&vset)
                        .is_some_and(Option::is_some))
                .then_some(vset)
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            continue;
        }
        let vset = candidates
            [usize::try_from(random_u64() % candidates.len() as u64).expect("vset index fits")];
        let from = control.borrow().placement[&vset];
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
        assert!(active.insert(vset));
        let worlds = Rc::clone(&worlds);
        let control = Rc::clone(&control);
        let config = Rc::clone(&config);
        let completed = completed.clone();
        actors.spawn(async move {
            if let Some(completion) = start_migration(vset, to, &worlds, &control, &config).await {
                let _ = completion.await;
            }
            let _ = completed.send(vset);
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
    report.prefetch_fills = sum(|value| value.prefetch_fills);
    report.hydrate_fills = sum(|value| value.hydrate_fills);
    report.leaf_fills = sum(|value| value.leaf_fills);
    report.store_retries = sum(|value| value.store_retries);
    report.peer_rejected = sum(|value| value.peer_rejected);
    report.wedged_guests = sum(|value| value.wedged_guests);
    report.wedged_hydration = sum(|value| value.wedged_hydration);
    report.wedged_outbound = sum(|value| value.wedged_outbound);
    report.replica_bytes = sum(|value| value.replica_bytes);
    report.replica_commits = sum(|value| value.replica_commits);
    report.replica_store_bytes = sum(|value| value.replica_store_bytes);
    report.replica_unlinks = sum(|value| value.replica_unlinks);
    report.replica_network_bytes = sum(|value| value.replica_network_bytes);
    report.replica_logical_bytes = sum(|value| value.replica_logical_bytes);
    report.replica_nonactive_bytes = sum(|value| value.replica_nonactive_bytes);
    report.replica_replacement_bytes = sum(|value| value.replica_replacement_bytes);
    report.replica_cleanup_rewrite_bytes = sum(|value| value.replica_cleanup_rewrite_bytes);
    report.replica_artifact_flushes = sum(|value| value.replica_artifact_flushes);
    report.replica_commit_flushes = sum(|value| value.replica_commit_flushes);
    report.replica_rotations = sum(|value| value.replica_rotations);
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

fn random_between(low: u64, high: u64) -> u64 {
    assert!(low <= high);
    low + random_u64() % (high - low + 1)
}

fn hit(probability: Ppm) -> bool {
    random_u64() % 1_000_000 < u64::from(probability.0)
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use blockd_core::protocol::{AdminError, AdminEvent};
    use blockd_core::types::{Epoch, VolumeIdx};
    use blockd_core::world::AdminIo;
    use blockd_exec::{Executor, bridge, delay, spawn, timeout, yield_now};

    use super::*;

    fn control(vset_count: u64, hosts: u16) -> Rc<RefCell<Control>> {
        let guests = Rc::new(RefCell::new(BTreeMap::new()));
        let mut placement = BTreeMap::new();
        let mut guest_state = BTreeMap::new();
        for number in 1..=vset_count {
            let vset = VsetId(number);
            placement.insert(vset, 0);
            guest_state.insert(vset, Rc::new(GuestState::default()));
            guests
                .borrow_mut()
                .insert(vset, Some(spawn(pending::<()>())));
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
            next_req: 1,
            live: vec![true; usize::from(hosts)],
            up: vec![true; usize::from(hosts)],
            workload_end: Cell::new(u64::MAX),
            report: ClusterReport::default(),
            sync_latencies: Vec::new(),
            retired_counters: Vec::new(),
        }))
    }

    fn unplaced_control(vset_count: u64, hosts: u16) -> Rc<RefCell<Control>> {
        Rc::new(RefCell::new(Control {
            placement: BTreeMap::new(),
            guests: Rc::new(RefCell::new(BTreeMap::new())),
            guest_state: (1..=vset_count)
                .map(|number| (VsetId(number), Rc::new(GuestState::default())))
                .collect(),
            migrations: BTreeMap::new(),
            uncertain_migrations: BTreeSet::new(),
            accepted_migrations: BTreeSet::new(),
            deferred_source_recoveries: BTreeMap::new(),
            deferred_destination_recoveries: BTreeMap::new(),
            quiescing_guests: BTreeSet::new(),
            migration_cuts: BTreeSet::new(),
            next_req: 1,
            live: vec![true; usize::from(hosts)],
            up: vec![true; usize::from(hosts)],
            workload_end: Cell::new(u64::MAX),
            report: ClusterReport::default(),
            sync_latencies: Vec::new(),
            retired_counters: Vec::new(),
        }))
    }

    #[test]
    fn initial_creation_requests_are_bounded() {
        let mut config = crate::presets::migration_chaos();
        config.hosts = 1;
        config.vset_count = u16::try_from(CREATION_CONCURRENCY + 1).expect("limit fits");
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(107);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let control = Rc::new(RefCell::new(Control {
                    placement: BTreeMap::new(),
                    guests: Rc::new(RefCell::new(BTreeMap::new())),
                    guest_state: (1..=config.vset_count)
                        .map(|number| (VsetId(u64::from(number)), Rc::new(GuestState::default())))
                        .collect(),
                    migrations: BTreeMap::new(),
                    uncertain_migrations: BTreeSet::new(),
                    accepted_migrations: BTreeSet::new(),
                    deferred_source_recoveries: BTreeMap::new(),
                    deferred_destination_recoveries: BTreeMap::new(),
                    quiescing_guests: BTreeSet::new(),
                    migration_cuts: BTreeSet::new(),
                    next_req: 1,
                    live: vec![true; usize::from(config.hosts)],
                    up: vec![true; usize::from(config.hosts)],
                    workload_end: Cell::new(u64::MAX),
                    report: ClusterReport::default(),
                    sync_latencies: Vec::new(),
                    retired_counters: Vec::new(),
                }));
                let schedule = spawn(create_initial_vsets(
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

    #[test]
    fn initial_creation_waits_for_every_configured_vset() {
        let mut config = crate::presets::migration_chaos();
        config.hosts = 1;
        config.vset_count = u16::try_from(CREATION_CONCURRENCY + 1).expect("limit fits");
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(109);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let control = unplaced_control(u64::from(config.vset_count), config.hosts);
                let responder = spawn({
                    let world = Rc::clone(&worlds[0]);
                    let control = Rc::clone(&control);
                    let count = config.vset_count;
                    async move {
                        for _ in 0..count {
                            let request = AdminIo::next_admin(world.as_ref())
                                .await
                                .expect("create request");
                            let (call, mut reply) = request.into_parts();
                            let AdminCall::CreateVset { vset, .. } = call else {
                                panic!("unexpected initial request: {call:?}");
                            };
                            assert!(control.borrow().guests.borrow().is_empty());
                            let _ = reply.send(Ok(AdminSuccess::VsetCreated { vset }));
                        }
                    }
                });
                let created = create_initial_vsets(
                    Rc::clone(&config),
                    Rc::clone(&worlds),
                    Rc::clone(&control),
                )
                .await;
                responder.await.expect("responder completes");
                assert_eq!(
                    control.borrow().placement.len(),
                    usize::from(config.vset_count)
                );
                assert!(control.borrow().guests.borrow().is_empty());
                for (vset, host) in created {
                    start_guest(vset, host, &control, &worlds, &config);
                }
                assert_eq!(
                    control.borrow().guests.borrow().len(),
                    usize::from(config.vset_count)
                );
            }
        });
    }

    #[test]
    fn superseded_restore_retries_on_a_current_live_incarnation() {
        let config = Rc::new(crate::presets::migration_chaos());
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let control = unplaced_control(1, config.hosts);
        let vset = VsetId(1);
        let candidate = 1;
        let (reply, result) = bridge();
        reply
            .send(Ok(AdminSuccess::VsetRestored {
                vset,
                verdict: Verdict::ColdBoot,
            }))
            .expect("restore completion remains live");
        worlds[usize::from(candidate)].advance_incarnation();
        control.borrow_mut().live[usize::from(candidate)] = false;
        control.borrow_mut().up[usize::from(candidate)] = false;

        let mut executor = Executor::simulation(111);
        executor.block_on({
            let control = Rc::clone(&control);
            let worlds = Rc::clone(&worlds);
            let config = Rc::clone(&config);
            async move {
                let completion = spawn(restore_completion(
                    RestoreTarget {
                        host: candidate,
                        incarnation: 0,
                    },
                    vset,
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
                assert_eq!(call, AdminCall::RestoreVset { vset });
                reply
                    .send(Ok(AdminSuccess::VsetRestored {
                        vset,
                        verdict: Verdict::ColdBoot,
                    }))
                    .expect("retry remains live");
                completion.await.expect("restore completion");
            }
        });

        assert_eq!(control.borrow().placement.get(&vset), Some(&0));
        assert_eq!(control.borrow().report.restores, 1);
    }

    #[test]
    fn recovery_on_a_live_host_replaces_a_dead_placement() {
        let config = Rc::new(crate::presets::migration_chaos());
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let vset = VsetId(1);
        let mut executor = Executor::simulation(112);
        executor.block_on({
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
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: Verdict::ColdBoot,
                    },
                )
                .await;
                delay(1).await;
                assert_eq!(control.borrow().placement.get(&vset), Some(&1));
                drop(lifecycle);
            }
        });
    }

    #[test]
    fn random_migration_slots_wait_for_request_completion() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 100;
        config.migrate_mean_interval = 1;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(108);
        executor.block_on({
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

    #[test]
    fn slow_guest_drain_does_not_block_other_random_migrations() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 100;
        config.migrate_mean_interval = 1;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(105);
        executor.block_on({
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
                    BTreeSet::from([VsetId(1), VsetId(2)])
                );
                drop(schedule);
            }
        });
    }

    #[test]
    fn cancelling_guest_drain_rolls_back_the_pending_migration() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 100;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(106);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let vset = VsetId(1);
                let control = control(1, config.hosts);
                let mut migration = spawn({
                    let control = Rc::clone(&control);
                    let worlds = Rc::clone(&worlds);
                    let config = Rc::clone(&config);
                    async move { start_migration(vset, 1, &worlds, &control, &config).await }
                });
                delay(1).await;
                assert!(control.borrow().quiescing_guests.contains(&vset));
                assert!(control.borrow().migrations.contains_key(&vset));

                migration.cancel();
                delay(0).await;

                assert!(!control.borrow().quiescing_guests.contains(&vset));
                assert!(!control.borrow().migrations.contains_key(&vset));
                assert_eq!(control.borrow().placement.get(&vset), Some(&0));
                assert!(
                    control
                        .borrow()
                        .guests
                        .borrow()
                        .get(&vset)
                        .is_some_and(Option::is_some)
                );
            }
        });
    }

    #[test]
    fn destination_restart_recovery_waits_for_migration_completion() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(101);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let vset = VsetId(1);
                let page = PageId {
                    volume: VolumeId {
                        vset,
                        idx: VolumeIdx(0),
                    },
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
                    let state = Rc::clone(&control.borrow().guest_state[&vset]);
                    state.completed.set(7);
                    state.expected.borrow_mut().insert(page, bytes.clone());
                    state.durable.borrow_mut().insert(page, bytes.clone());
                    let mut control = control.borrow_mut();
                    control.placement.insert(vset, 1);
                    control.migrations.insert(vset, attempt);
                    control
                        .deferred_source_recoveries
                        .insert(vset, (1, Verdict::ColdBoot));
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
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: Verdict::Resume {
                            epoch: Epoch(1),
                            vmstate: 7,
                        },
                    },
                )
                .await;
                delay(1).await;
                assert_eq!(control.borrow().migrations.get(&vset), Some(&attempt));
                assert!(
                    control
                        .borrow()
                        .deferred_destination_recoveries
                        .contains_key(&vset)
                );

                let (reply, result) = bridge();
                reply
                    .send(Ok(AdminSuccess::MigratedOut { vset }))
                    .expect("completion receiver alive");
                let (completed, _completion) = oneshot();
                migration_completion(
                    vset,
                    attempt,
                    result,
                    completed,
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                )
                .await;

                let state = Rc::clone(&control.borrow().guest_state[&vset]);
                assert_eq!(state.completed.get(), 7);
                assert_eq!(state.expected.borrow().get(&page), Some(&bytes));
                assert_eq!(state.durable.borrow().get(&page), Some(&bytes));
                assert_eq!(control.borrow().report.recoveries, 1);
                assert_eq!(control.borrow().report.migrations, 1);
                assert_eq!(control.borrow().placement.get(&vset), Some(&0));
                assert!(!control.borrow().migrations.contains_key(&vset));
                assert!(
                    !control
                        .borrow()
                        .deferred_destination_recoveries
                        .contains_key(&vset)
                );
                drop(lifecycle);
            }
        });
    }

    #[test]
    fn closed_migration_consumes_a_deferred_source_recovery() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(104);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let vset = VsetId(1);
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
                    control.placement.insert(vset, attempt.from);
                    control.migrations.insert(vset, attempt);
                    control
                        .deferred_source_recoveries
                        .insert(vset, (attempt.from, Verdict::ColdBoot));
                }
                let (reply, result) = bridge();
                drop(reply);
                let (completed, _completion) = oneshot();

                migration_completion(
                    vset,
                    attempt,
                    result,
                    completed,
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                )
                .await;

                assert!(!control.borrow().migrations.contains_key(&vset));
                assert!(!control.borrow().uncertain_migrations.contains(&vset));
                assert!(
                    !control
                        .borrow()
                        .deferred_source_recoveries
                        .contains_key(&vset)
                );
                assert_eq!(control.borrow().report.migrations_refused, 1);
                assert_eq!(control.borrow().report.recoveries, 1);
                assert!(
                    control
                        .borrow()
                        .guests
                        .borrow()
                        .get(&vset)
                        .is_some_and(Option::is_some)
                );
            }
        });
    }

    #[test]
    fn stale_source_recovery_cannot_resolve_a_closed_migration() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(109);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let vset = VsetId(1);
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
                    control.placement.insert(vset, attempt.from);
                    control.migrations.insert(vset, attempt);
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
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: Verdict::ColdBoot,
                    },
                )
                .await;
                delay(1).await;
                assert!(
                    !control
                        .borrow()
                        .deferred_source_recoveries
                        .contains_key(&vset)
                );

                let (reply, result) = bridge();
                drop(reply);
                let (completed, _completion) = oneshot();
                migration_completion(
                    vset,
                    attempt,
                    result,
                    completed,
                    Rc::clone(&control),
                    Rc::clone(&worlds),
                    Rc::clone(&config),
                )
                .await;

                assert_eq!(control.borrow().migrations.get(&vset), Some(&attempt));
                assert!(control.borrow().uncertain_migrations.contains(&vset));
                AdminIo::emit_admin_event(
                    worlds[usize::from(attempt.from)].as_ref(),
                    AdminEvent::VsetRecovered {
                        vset,
                        verdict: Verdict::ColdBoot,
                    },
                )
                .await;
                delay(1).await;
                assert_eq!(control.borrow().migrations.get(&vset), Some(&attempt));
                assert!(control.borrow().uncertain_migrations.contains(&vset));
                drop(lifecycle);
            }
        });
    }

    #[test]
    fn rejected_migration_waits_for_source_recovery_before_guest_restart() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let config = Rc::new(config);
        let mut executor = Executor::simulation(110);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::clone(&config);
            async move {
                let vset = VsetId(1);
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
                    control.placement.insert(vset, attempt.from);
                    control.up[usize::from(attempt.from)] = false;
                    control.guests.borrow_mut().insert(vset, None);
                }

                restart_source_after_migration_refusal(
                    vset, attempt, None, &control, &worlds, &config,
                );
                assert!(control.borrow().guests.borrow()[&vset].is_none());

                control.borrow_mut().up[usize::from(attempt.from)] = true;
                worlds[usize::from(attempt.from)].advance_incarnation();
                restart_source_after_migration_refusal(
                    vset, attempt, None, &control, &worlds, &config,
                );
                assert!(control.borrow().guests.borrow()[&vset].is_none());

                restart_source_after_migration_refusal(
                    vset,
                    attempt,
                    Some((attempt.from, Verdict::ColdBoot)),
                    &control,
                    &worlds,
                    &config,
                );
                assert!(control.borrow().guests.borrow()[&vset].is_some());
            }
        });
    }

    #[test]
    fn source_restart_during_quiescing_applies_deferred_recovery() {
        let mut config = crate::presets::migration_chaos();
        config.horizon = 0;
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(103);
        executor.block_on({
            let worlds = Rc::clone(&worlds);
            let config = Rc::new(config);
            async move {
                let vset = VsetId(1);
                let control = control(1, config.hosts);
                control
                    .borrow()
                    .guests
                    .borrow_mut()
                    .insert(vset, Some(spawn(delay(10))));
                let migration = spawn({
                    let worlds = Rc::clone(&worlds);
                    let control = Rc::clone(&control);
                    let config = Rc::clone(&config);
                    async move { start_migration(vset, 1, &worlds, &control, &config).await }
                });
                delay(1).await;
                worlds[0].advance_incarnation();
                control.borrow_mut().deferred_source_recoveries.insert(
                    vset,
                    (
                        0,
                        Verdict::Resume {
                            epoch: Epoch(1),
                            vmstate: 0,
                        },
                    ),
                );

                assert!(migration.await.expect("migration task completes").is_none());
                assert!(!control.borrow().quiescing_guests.contains(&vset));
                assert!(
                    control
                        .borrow()
                        .guests
                        .borrow()
                        .get(&vset)
                        .is_some_and(Option::is_some)
                );
                assert!(
                    !control
                        .borrow()
                        .deferred_source_recoveries
                        .contains_key(&vset)
                );
            }
        });
    }

    #[test]
    fn cluster_checkpoint_schedule_coalesces_each_vset() {
        let config = crate::presets::migration_chaos();
        let (_, worlds) = SimWorld::cluster(1, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(102);
        executor.block_on({
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
                        vset: VsetId(1),
                    }
                );
                drop(schedule);
            }
        });
    }

    #[test]
    fn cluster_checkpoint_schedule_drains_an_admitted_request_past_the_horizon() {
        let config = crate::presets::migration_chaos();
        let (_, worlds) = SimWorld::cluster(config.hosts, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(113);
        executor.block_on({
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

    #[test]
    fn cluster_checkpoint_schedule_has_a_global_bound() {
        let config = crate::presets::migration_chaos();
        let (_, worlds) = SimWorld::cluster(1, config.bdev, config.store);
        let worlds = Rc::new(worlds);
        let mut executor = Executor::simulation(103);
        executor.block_on({
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
                        AdminCall::Checkpoint { vset, .. }
                            if vset == VsetId(u64::try_from(number).expect("vset fits"))
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
}
