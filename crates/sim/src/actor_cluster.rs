//! Multi-host deterministic harness over the shared async actor worlds.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use blockd_core::engine::{HostState, host_actor_with_state};
use blockd_core::hostmeta::{Counters, HostConfig, ReplicaPlacementConfig};
use blockd_core::protocol::{AdminCmd, AdminReply, ReqId, Verdict};
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis, page_size};
use blockd_exec::rng::Ppm;
use blockd_exec::{Executor, FaultConfig, TaskHandle, delay, now, random_u64, spawn};

use crate::actor_world::{SimNetwork, SimWorld};
use crate::cluster::{ClusterConfig, ClusterReport};
use crate::guest::page_pattern;
use crate::world::blobdev::CrashFate;

type SharedState = Rc<RefCell<HostState>>;
type HostSlots = Rc<Vec<RefCell<Option<TaskHandle<()>>>>>;
type StateSlots = Rc<Vec<RefCell<SharedState>>>;
type GuestSlots = Rc<RefCell<BTreeMap<VsetId, Option<TaskHandle<()>>>>>;

#[derive(Default)]
struct GuestState {
    completed: Cell<u64>,
    expected: RefCell<BTreeMap<PageId, Vec<u8>>>,
    durable: RefCell<BTreeMap<PageId, Vec<u8>>>,
    written: RefCell<BTreeMap<PageId, BTreeSet<u64>>>,
    recovering: RefCell<BTreeSet<PageId>>,
    volume_sequences: RefCell<BTreeMap<VolumeId, u64>>,
    mutation: Cell<u64>,
    history: RefCell<BTreeMap<u64, BTreeMap<PageId, Vec<u8>>>>,
    violations: RefCell<Vec<String>>,
}

enum Request {
    Create,
    Checkpoint,
    Migrate {
        vset: VsetId,
        from: u16,
        to: u16,
        started: u64,
    },
    Restore {
        sent: u64,
    },
}

struct Control {
    placement: BTreeMap<VsetId, u16>,
    guests: GuestSlots,
    guest_state: BTreeMap<VsetId, Rc<GuestState>>,
    requests: BTreeMap<ReqId, Request>,
    next_req: u64,
    live: Vec<bool>,
    report: ClusterReport,
    sync_latencies: Vec<u64>,
    retired_counters: Vec<Counters>,
}

impl Control {
    fn req(&mut self, request: Request) -> ReqId {
        let req = ReqId(self.next_req);
        self.next_req = self
            .next_req
            .checked_add(1)
            .expect("cluster request overflow");
        self.requests.insert(req, request);
        req
    }
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub(crate) fn run(seed: u64, config: ClusterConfig) -> ClusterReport {
    assert!(config.hosts > 0, "cluster requires at least one host");
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
        requests: BTreeMap::new(),
        next_req: 1,
        live: vec![true; usize::from(config.hosts)],
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
            .spawn(reply_actor(
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

    for number in 1..=config.vset_count {
        let vset = VsetId(u64::from(number));
        let host = (number - 1) % config.hosts;
        let req = {
            let mut control = control.borrow_mut();
            control.placement.insert(vset, host);
            control.req(Request::Create)
        };
        worlds[usize::from(host)].enqueue_admin(AdminCmd::CreateVset {
            req,
            vset,
            config: vset_config(&config, vset),
            from_base: None,
        });
    }

    spawn_schedules(
        &mut executor,
        &config,
        Rc::clone(&worlds),
        Rc::clone(&network),
        Rc::clone(&slots),
        Rc::clone(&states),
        Rc::clone(&control),
    );
    executor.run_until(config.horizon.saturating_add(2 * millis(1_000)));

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
        .map(|guest| guest.completed.get())
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
            }
        }),
    }
}

fn vset_config(config: &ClusterConfig, _vset: VsetId) -> blockd_core::journal::VsetConfig {
    config.vset_config
}

#[allow(clippy::too_many_lines)]
async fn reply_actor(
    host: u16,
    world: Rc<SimWorld>,
    control: Rc<RefCell<Control>>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    config: Rc<ClusterConfig>,
) {
    while let Some(reply) = world.next_admin_reply().await {
        match reply {
            AdminReply::VsetCreated { req, vset } => {
                control.borrow_mut().requests.remove(&req);
                start_guest(vset, host, &control, &worlds, &config);
            }
            AdminReply::CheckpointDone { req, .. } => {
                control.borrow_mut().requests.remove(&req);
            }
            AdminReply::MigratedOut { req, .. } => {
                // Destination activation is the authority-changing reply.
                let _ = control.borrow().requests.get(&req);
            }
            AdminReply::VsetMigratedIn { vset, verdict } => {
                let migrated = control.borrow().guest_state[&vset]
                    .expected
                    .borrow()
                    .clone();
                *control.borrow().guest_state[&vset].durable.borrow_mut() = migrated;
                prepare_recovered(&control.borrow().guest_state[&vset], vset, &config, verdict);
                let started = {
                    let mut control = control.borrow_mut();
                    let request =
                        control
                            .requests
                            .iter()
                            .find_map(|(&req, request)| match request {
                                Request::Migrate {
                                    vset: requested,
                                    to,
                                    started,
                                    ..
                                } if *requested == vset && *to == host => Some((req, *started)),
                                _ => None,
                            });
                    if let Some((req, started)) = request {
                        control.requests.remove(&req);
                        control.report.migrations = control.report.migrations.saturating_add(1);
                        control.report.max_migration_pause_ns = control
                            .report
                            .max_migration_pause_ns
                            .max(now().saturating_sub(started));
                    }
                    control.placement.insert(vset, host);
                    request.map(|(_, started)| started)
                };
                let _ = started;
                start_guest(vset, host, &control, &worlds, &config);
            }
            AdminReply::VsetRecovered { vset, verdict } => {
                let already_elsewhere =
                    control
                        .borrow()
                        .placement
                        .get(&vset)
                        .is_some_and(|&placed| {
                            placed != host && control.borrow().live[usize::from(placed)]
                        });
                if already_elsewhere {
                    control
                        .borrow_mut()
                        .report
                        .violations
                        .push(format!("two runners recovered for {vset:?}"));
                    continue;
                }
                prepare_recovered(&control.borrow().guest_state[&vset], vset, &config, verdict);
                {
                    let mut control = control.borrow_mut();
                    control.report.recoveries = control.report.recoveries.saturating_add(1);
                }
                start_guest(vset, host, &control, &worlds, &config);
            }
            AdminReply::VsetRestored { req, vset, verdict } => {
                prepare_recovered(&control.borrow().guest_state[&vset], vset, &config, verdict);
                let sent = match control.borrow_mut().requests.remove(&req) {
                    Some(Request::Restore { sent }) => sent,
                    _ => now(),
                };
                {
                    let mut control = control.borrow_mut();
                    control.placement.insert(vset, host);
                    control.report.restores = control.report.restores.saturating_add(1);
                    control.report.loss_bound_verified =
                        control.report.loss_bound_verified.saturating_add(1);
                    control.report.max_restore_ns = control
                        .report
                        .max_restore_ns
                        .max(now().saturating_sub(sent));
                }
                start_guest(vset, host, &control, &worlds, &config);
            }
            AdminReply::AdminFailed { req } => {
                let request = control.borrow_mut().requests.remove(&req);
                match request {
                    Some(Request::Migrate { vset, from, .. }) => {
                        {
                            let mut control = control.borrow_mut();
                            control.report.migrations_refused =
                                control.report.migrations_refused.saturating_add(1);
                        }
                        if control.borrow().placement.get(&vset) == Some(&from) {
                            start_guest(vset, from, &control, &worlds, &config);
                        }
                    }
                    Some(Request::Restore { .. }) => {
                        let mut control = control.borrow_mut();
                        control.report.claims_lost = control.report.claims_lost.saturating_add(1);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
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
    let guest_state = Rc::clone(&control.borrow().guest_state[&vset]);
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

fn prepare_recovered(state: &GuestState, vset: VsetId, config: &ClusterConfig, verdict: Verdict) {
    match verdict {
        Verdict::Resume { vmstate, .. } => {
            state.completed.set(vmstate);
            *state.expected.borrow_mut() = state.durable.borrow().clone();
        }
        Verdict::ColdBoot => {
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
        }
        Verdict::DatabaseReady { .. } => return,
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
        delay(random_between(config.think.0, config.think.1)).await;
        if now() > config.horizon {
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
            finish_operation(&world, &state, vset);
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
            let expected = state
                .expected
                .borrow()
                .get(&page)
                .cloned()
                .unwrap_or_else(|| vec![0; page_size()]);
            let expected_claimed = crate::guest::claimed_vol_seq(&expected);
            let recovering = state.recovering.borrow_mut().remove(&page);
            let claimed = crate::guest::claimed_vol_seq(&actual);
            let durable_floor = state
                .durable
                .borrow()
                .get(&page)
                .map_or(0, |bytes| crate::guest::claimed_vol_seq(bytes));
            let possible = (claimed == 0 && !state.durable.borrow().contains_key(&page))
                || state
                    .written
                    .borrow()
                    .get(&page)
                    .is_some_and(|sequences| sequences.contains(&claimed));
            let valid_recovery = recovering
                && possible
                && claimed >= durable_floor
                && actual == page_pattern(page, claimed);
            if actual != expected && !valid_recovery {
                state
                    .violations
                    .borrow_mut()
                    .push(format!(
                        "read returned stale or foreign bytes on {:?} for {page:?}: actual sequence {claimed}, expected {expected_claimed}, durable floor {durable_floor}, recovering {recovering}, possible {possible}, vmstate {} at {}",
                        world.host_id(),
                        state.completed.get(),
                        now(),
                    ));
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
    if let Some((at, vset, to)) = config.migrate_at {
        executor
            .spawn(at_migrate(
                at,
                vset,
                to,
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
                config.horizon,
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
    delay(at.saturating_sub(now())).await;
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
    delay(at.saturating_sub(now())).await;
    let Some(mut actor) = slots[usize::from(host)].borrow_mut().take() else {
        return;
    };
    actor.cancel();
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
    for vset in affected {
        cancel_guest(vset, &control);
        prepare_backed_loss(&control.borrow().guest_state[&vset], vset, &worlds[0]);
        let candidates = (0..config.hosts)
            .filter(|&candidate| candidate != host && control.borrow().live[usize::from(candidate)])
            .take(if config.race_restore { 2 } else { 1 })
            .collect::<Vec<_>>();
        for candidate in candidates {
            let req = control.borrow_mut().req(Request::Restore { sent: now() });
            worlds[usize::from(candidate)].enqueue_admin(AdminCmd::RestoreVset { req, vset });
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
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) {
    delay(at.saturating_sub(now())).await;
    start_migration(vset, to, &worlds, &control);
}

fn start_migration(
    vset: VsetId,
    to: u16,
    worlds: &[Rc<SimWorld>],
    control: &Rc<RefCell<Control>>,
) -> bool {
    let Some(&from) = control.borrow().placement.get(&vset) else {
        return false;
    };
    if from == to
        || !control.borrow().live[usize::from(to)]
        || control.borrow().requests.values().any(
            |request| matches!(request, Request::Migrate { vset: pending, .. } if *pending == vset),
        )
    {
        return false;
    }
    cancel_guest(vset, control);
    let req = control.borrow_mut().req(Request::Migrate {
        vset,
        from,
        to,
        started: now(),
    });
    worlds[usize::from(from)].enqueue_admin(AdminCmd::MigrateOut {
        req,
        vset,
        to: HostId(to),
    });
    true
}

async fn checkpoint_schedule(
    interval: u64,
    horizon: u64,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    control: Rc<RefCell<Control>>,
) {
    loop {
        delay(interval).await;
        if now() > horizon {
            return;
        }
        let placement = control.borrow().placement.clone();
        for (vset, host) in placement {
            let req = control.borrow_mut().req(Request::Checkpoint);
            worlds[usize::from(host)].enqueue_admin(AdminCmd::Checkpoint { req, vset });
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
        if now() > config.horizon {
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
    loop {
        delay(random_between(
            1,
            config.migrate_mean_interval.saturating_mul(2),
        ))
        .await;
        if now() > config.horizon {
            return;
        }
        let candidates = control
            .borrow()
            .placement
            .iter()
            .filter_map(|(&vset, &host)| control.borrow().live[usize::from(host)].then_some(vset))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            continue;
        }
        let vset = candidates
            [usize::try_from(random_u64() % candidates.len() as u64).expect("vset index fits")];
        let from = control.borrow().placement[&vset];
        let destinations = (0..config.hosts)
            .filter(|&host| host != from && control.borrow().live[usize::from(host)])
            .collect::<Vec<_>>();
        if destinations.is_empty() {
            continue;
        }
        let to = destinations
            [usize::try_from(random_u64() % destinations.len() as u64).expect("host index fits")];
        let _ = start_migration(vset, to, &worlds, &control);
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
