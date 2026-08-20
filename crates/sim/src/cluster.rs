//! Multi-host deterministic harness over the shared async actor worlds.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::time::Duration;

use blockd_core::authority::HostSessionRecord;
use blockd_core::engine::{
    HostState, cas_placement, host_actor_with_state, read_host_session, read_placement,
};
use blockd_core::head::HeadRecord;
use blockd_core::hostmeta::{AuthorityHostConfig, ClusterPlacementConfig, Counters, HostConfig};
use blockd_core::journal::VolumeConfig;
use blockd_core::layout;
use blockd_core::manifest::Manifest;
use blockd_core::placement::ClusterPlacement;
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
use crate::peer_transport::{
    PeerAuthorization, PeerMembership, PeerTransport, PeerTransportFaults, PeerTransportStats,
};
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
const FINAL_SETTLE_TIME: u64 = millis(2_000);
const FINAL_LIFECYCLE_DRAIN_TIMEOUT: u64 = millis(60_000);
const FINAL_PAGE_AUDIT_TIMEOUT: u64 = millis(2_000);
const SIMULATION_SCHEDULING_SLACK: u64 = millis(1_000);
const DYNAMIC_AUTHORITY_CLUSTER_ID: u64 = 0x424c_4f43_4b44_5349;
const DYNAMIC_AUTHORITY_POLL_INTERVAL: u64 = millis(50);
const DYNAMIC_AUTHORITY_MAX_STALENESS: u64 = millis(600);
const DYNAMIC_AUTHORITY_CHALLENGE_INTERVAL: u64 = millis(800);

pub use blockd_exec::FaultPoint;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sabotage {
    EagerHandoffAck,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestartClass {
    Fast,
    Slow,
    Rolling,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipEvent {
    Claim {
        at: u64,
        host: u16,
        token: u64,
        commit_response_lost: bool,
    },
    Publish {
        at: u64,
        host: u16,
        lease_duration: u64,
        certificate_generation: u64,
        commit_response_lost: bool,
    },
    Discover {
        at: u64,
        observer: u16,
        reverse_list: bool,
        reverse_gets: bool,
    },
    RotateCertificate {
        at: u64,
        host: u16,
        certificate_generation: u64,
        commit_response_lost: bool,
    },
    Restart {
        at: u64,
        host: u16,
        downtime: u64,
        class: RestartClass,
    },
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
    pub membership_events: Vec<MembershipEvent>,
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
    pub peer_certificate_authorization_drops: u64,
    pub peer_renewed_certificate_frames: u64,
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
    pub membership_transitions: u64,
    pub membership_joins: u64,
    pub membership_leaves: u64,
    pub membership_claims: u64,
    pub membership_claim_retries: u64,
    pub membership_publications: u64,
    pub membership_committed_lost: u64,
    pub membership_lease_expiries: u64,
    pub membership_certificate_rotations: u64,
    pub membership_lists: u64,
    pub membership_gets: u64,
    pub membership_reordered_lists: u64,
    pub membership_reordered_gets: u64,
    pub membership_fast_restarts: u64,
    pub membership_slow_restarts: u64,
    pub membership_rolling_restarts: u64,
    pub membership_lease_preserved_restarts: u64,
    pub durable_placement_writes: u64,
    pub placement_epoch_initial: u64,
    pub placement_epoch_final: u64,
    pub placement_recovered_after_restart: u64,
    pub placement_owner_recovered_after_restart: u64,
    pub placement_owner_first_faults_after_restart: u64,
    pub authority_transfers: u64,
    pub stash_recoveries: u64,
    pub protected_sync_volumes: u64,
    pub continuous_volumes: u64,
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

struct RestartOutcome {
    retired_state: SharedState,
    affected_volumes: Vec<VolumeId>,
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
    post_restart_fault_pending: Cell<bool>,
    post_restart_faults: Cell<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MigrationAttempt {
    from: u16,
    to: u16,
    offer_fence: u64,
    from_run_generation: u64,
    to_run_generation: u64,
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

const MEMBERSHIP_CLAIM_PREFIX: &str = "cluster/membership/claims/";
const MEMBERSHIP_MEMBER_PREFIX: &str = "cluster/membership/members/";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MembershipRecord {
    host: HostId,
    token: u64,
    lease_expires_at: u64,
    certificate_generation: u64,
}

impl MembershipRecord {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(28);
        bytes.extend_from_slice(&self.host.get().to_le_bytes());
        bytes.extend_from_slice(&self.token.to_le_bytes());
        bytes.extend_from_slice(&self.lease_expires_at.to_le_bytes());
        bytes.extend_from_slice(&self.certificate_generation.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let host = HostId::new(u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?));
        let token = u64::from_le_bytes(bytes.get(4..12)?.try_into().ok()?);
        let lease_expires_at = u64::from_le_bytes(bytes.get(12..20)?.try_into().ok()?);
        let certificate_generation = u64::from_le_bytes(bytes.get(20..28)?.try_into().ok()?);
        (bytes.len() == 28 && token != 0 && certificate_generation != 0).then_some(Self {
            host,
            token,
            lease_expires_at,
            certificate_generation,
        })
    }
}

#[derive(Default)]
struct MembershipModel {
    claims: BTreeMap<HostId, u64>,
    member_versions: BTreeMap<HostId, u64>,
    observed_records: BTreeMap<HostId, MembershipRecord>,
    placement_version: Option<u64>,
    placement_epoch: u64,
}

type PlacementSlots = Rc<RefCell<Vec<ClusterPlacementConfig>>>;

fn host_index(host: HostId) -> usize {
    usize::try_from(host.get()).expect("configured host ID fits usize")
}

fn host_id(host: u16) -> HostId {
    HostId::new(u32::from(host))
}

fn dynamic_authority_config() -> AuthorityHostConfig {
    AuthorityHostConfig {
        cluster_id: DYNAMIC_AUTHORITY_CLUSTER_ID,
        poll_interval: DYNAMIC_AUTHORITY_POLL_INTERVAL,
        max_poll_staleness: DYNAMIC_AUTHORITY_MAX_STALENESS,
        challenge_interval: DYNAMIC_AUTHORITY_CHALLENGE_INTERVAL,
    }
}

fn initial_cluster_placement(config: &ClusterConfig) -> ClusterPlacement {
    assert!(
        config.hosts >= 4,
        "dynamic authority requires at least four hosts"
    );
    let mut placement = ClusterPlacement {
        cluster_id: DYNAMIC_AUTHORITY_CLUSTER_ID,
        epoch: config
            .daemon
            .cluster_placement
            .as_ref()
            .expect("cluster placement is initialized")
            .membership_epoch,
        roster: config
            .daemon
            .cluster_placement
            .as_ref()
            .expect("cluster placement is initialized")
            .roster
            .clone(),
    };
    placement.roster.sort_unstable();
    placement
        .validate()
        .expect("valid initial cluster placement");
    placement
}

fn migration_pause_ns(attempt: MigrationAttempt) -> u64 {
    now().saturating_sub(attempt.started)
}

#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run(seed: u64, mut config: ClusterConfig) -> ClusterReport {
    assert!(config.hosts > 0, "cluster requires at least one host");
    if config.daemon.cluster_placement.is_none() {
        config.daemon.cluster_placement = Some(ClusterPlacementConfig {
            membership_epoch: 1,
            roster: (0..config.hosts)
                .map(|host| HostId::new(u32::from(host)))
                .collect(),
            authority: None,
        });
    }
    if !config.membership_events.is_empty() {
        config
            .daemon
            .cluster_placement
            .as_mut()
            .expect("cluster placement is initialized")
            .authority = Some(dynamic_authority_config());
    }
    let worlds = SimWorld::cluster(config.hosts, config.bdev, config.store);
    if !config.membership_events.is_empty() {
        let placement = initial_cluster_placement(&config);
        worlds[0].seed_store_object(layout::placement_key(), placement.encode());
    }
    let initial_identities = config
        .daemon
        .cluster_placement
        .as_ref()
        .expect("cluster placement is initialized")
        .roster
        .clone();
    for world in &worlds {
        world.replace_peer_identities(initial_identities.iter().copied());
    }
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
        report: ClusterReport {
            placement_epoch_initial: config
                .daemon
                .cluster_placement
                .as_ref()
                .map_or(1, |placement| placement.membership_epoch),
            placement_epoch_final: config
                .daemon
                .cluster_placement
                .as_ref()
                .map_or(1, |placement| placement.membership_epoch),
            ..ClusterReport::default()
        },
        sync_latencies: Vec::new(),
        retired_counters: Vec::new(),
    }));
    let contexts = Rc::new(
        (0..=config.hosts)
            .map(|host| {
                SimulationContext::new(
                    seed.wrapping_add(u64::from(host).wrapping_mul(0x9e37_79b9)),
                    FaultConfig::disabled(),
                )
                .semantic_trace_only()
            })
            .collect::<Vec<_>>(),
    );
    let commands: HostCommands = Rc::new(RefCell::new(VecDeque::new()));
    let peer_stats = Rc::new(PeerTransportStats::default());
    let peer_membership: PeerMembership = Rc::new(RefCell::new(
        (0..config.hosts)
            .map(|host| {
                let identity = host_id(host);
                (identity, PeerAuthorization::new(identity, 1))
            })
            .collect(),
    ));
    let placement_slots: PlacementSlots = Rc::new(RefCell::new(
        (0..config.hosts)
            .map(|host| {
                host_config(&config, host)
                    .cluster_placement
                    .expect("cluster placement is initialized")
            })
            .collect(),
    ));
    let membership_model = Rc::new(RefCell::new(MembershipModel {
        placement_version: (!config.membership_events.is_empty()).then_some(1),
        placement_epoch: config
            .daemon
            .cluster_placement
            .as_ref()
            .map_or(1, |placement| placement.membership_epoch),
        ..MembershipModel::default()
    }));
    let peer_roster = (0..config.hosts)
        .map(|host| (host_id(host), format!("host-{host}")))
        .collect::<BTreeMap<_, _>>();
    let peer_faults = PeerTransportFaults {
        duplicate_odds: config.peer_dup,
        targeted_drop: config
            .drop_peer
            .map(|(kind, begin, end)| (kind as u8, begin, end)),
        max_frames_per_connection: 64,
    };
    let config = Rc::new(config);

    // One conceptual millisecond per outer step lets lifecycle commands reach
    // Turmoil between host polls without changing the actor clock's nanosecond API.
    let tick = Duration::from_millis(millis(1));
    // The final audit waits independently for every page. Derive the Turmoil
    // bound from that worst case so alternate page-size profiles cannot turn
    // valid fault timeouts into a harness-duration abort.
    let audit_timeout = u64::from(config.volume_count)
        .saturating_mul(u64::from(config.volume_config.pages))
        .saturating_mul(FINAL_PAGE_AUDIT_TIMEOUT);
    let duration = Duration::from_millis(
        config
            .horizon
            .saturating_add(FINAL_SETTLE_TIME)
            .saturating_add(FINAL_LIFECYCLE_DRAIN_TIMEOUT)
            .saturating_add(audit_timeout)
            .saturating_add(SIMULATION_SCHEDULING_SLACK),
    );
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
        let membership = Rc::clone(&peer_membership);
        sim.host(host_name, move || {
            let states = Rc::clone(&states);
            let world = Rc::clone(&world);
            let context = context.clone();
            let config = Rc::clone(&config);
            let roster = roster.clone();
            let transport_stats = Rc::clone(&transport_stats);
            let membership = Rc::clone(&membership);
            async move {
                context
                    .scope(async move {
                        // Startup placement is a durable input. In particular, a
                        // bounced host must not inherit the controller's in-memory
                        // placement slot: production restart has only the object
                        // store and must install its epoch before recovery begins.
                        let placement = load_startup_placement(&world, &config, host).await;
                        let state = Rc::new(RefCell::new(HostState::new(
                            host_config_with_placement(&config, host, placement),
                        )));
                        *states[usize::from(host)].borrow_mut() = Rc::clone(&state);
                        let transport = PeerTransport::start_with_membership(
                            host_id(host),
                            roster,
                            peer_faults,
                            transport_stats,
                            membership,
                        )
                        .await?;
                        let _attachment = world.attach_peer_transport(transport);
                        host_actor_with_state(state, world).await;
                        Ok(())
                    })
                    .await
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
    let client_peer_membership = Rc::clone(&peer_membership);
    let client_placement_slots = Rc::clone(&placement_slots);
    let client_membership_model = Rc::clone(&membership_model);
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
                client_peer_membership,
                client_placement_slots,
                client_membership_model,
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
                    sim.crash(format!("host-{}", host.get()));
                    let _ = complete.send(());
                }
                HostCommand::Bounce(host, complete) => {
                    sim.bounce(format!("host-{}", host.get()));
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
#[allow(clippy::too_many_arguments)]
async fn run_inner(
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
    contexts: Rc<Vec<SimulationContext>>,
    peer_stats: Rc<PeerTransportStats>,
    peer_membership: PeerMembership,
    placement_slots: PlacementSlots,
    membership_model: Rc<RefCell<MembershipModel>>,
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

    wait_for_initial_authority(&config, &states).await;

    let initial_volumes =
        create_initial_volumes(Rc::clone(&config), Rc::clone(&worlds), Rc::clone(&control)).await;
    // Forced actor failures exercise operating hosts. Creation now includes
    // passive protection, so arming these points earlier would intentionally
    // abort the initial create rather than the runtime behavior under test.
    if !config.fault_points.is_empty() {
        let mut faults = FaultConfig::disabled();
        for &point in &config.fault_points {
            faults.force(point, [true]);
        }
        for context in contexts.iter().skip(1) {
            context.set_fault_config(faults.clone());
        }
    }
    let workload_end = now().saturating_add(config.horizon);
    control.borrow().workload_end.set(workload_end);
    let simulation_end = workload_end.saturating_add(FINAL_SETTLE_TIME);

    spawn_schedules(
        &config,
        Rc::clone(&worlds),
        Rc::clone(&states),
        Rc::clone(&control),
        Rc::clone(&commands),
        Rc::clone(&peer_stats),
        peer_membership,
        Rc::clone(&placement_slots),
        Rc::clone(&membership_model),
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
    if !wait_for_lifecycle_drain(&control, &worlds, &states, &config).await {
        control.borrow_mut().report.violations.push(
            "final audit could not drain in-flight migration lifecycle within its bound".to_owned(),
        );
    }

    verify_membership_convergence(
        &config,
        &states,
        &control,
        &placement_slots,
        &membership_model,
    );

    let audit = audit_cluster(
        Rc::clone(&config),
        Rc::clone(&worlds),
        Rc::clone(&states),
        Rc::clone(&control),
    )
    .await;
    if !config.membership_events.is_empty() {
        audit_dynamic_authority(&config, &worlds, &states, &control).await;
    }
    {
        let mut control = control.borrow_mut();
        control.report.audit_runs = 1;
        control.report.audited_volumes = audit.volumes;
        control.report.audited_pages = audit.pages;
        control.report.protected_sync_volumes = audit.protected_sync_volumes;
        control.report.continuous_volumes = audit.volumes;
        control.report.violations.extend(audit.violations);
    }

    // Freeze every host-owned background actor before yielding to cancelled
    // guests and taking the final trace/counter snapshot. In particular, a
    // host-session monitor records its GET before the simulated store latency;
    // leaving host roots live here made that final maintenance read land on
    // either side of the report boundary depending on unrelated peer wakeups.
    stop_running_hosts_for_report(config.hosts, &control, &commands).await;

    for guest in guest_slots.borrow_mut().values_mut() {
        if let Some(mut guest) = guest.take() {
            guest.cancel();
        }
    }
    let frozen_store = worlds[0].store_metrics();
    let frozen_traces = contexts
        .iter()
        .map(SimulationContext::trace_hash)
        .collect::<Vec<_>>();
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        worlds[0].store_metrics(),
        frozen_store,
        "store activity continued after final host/guest quiescence"
    );
    assert_eq!(
        contexts
            .iter()
            .map(SimulationContext::trace_hash)
            .collect::<Vec<_>>(),
        frozen_traces,
        "semantic trace changed after final host/guest quiescence"
    );

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
    control.report.placement_owner_first_faults_after_restart = control
        .guest_state
        .values()
        .map(|guest| guest.post_restart_faults.get())
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
    control.report.store_retries = control
        .report
        .store_retries
        .saturating_add(control.report.membership_claim_retries);
    control.report.parked_end = states
        .iter()
        .filter(|state| control.live[host_index(state.borrow().borrow().config.host)])
        .map(|state| state.borrow().borrow().stats().pressure_waiting_faults)
        .sum();
    control.report.hydrating_end = states
        .iter()
        .filter(|state| control.live[host_index(state.borrow().borrow().config.host)])
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
    control.report.peer_certificate_authorization_drops =
        peer_stats.certificate_authorization_drops();
    control.report.peer_renewed_certificate_frames = peer_stats.renewed_certificate_frames();
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

async fn stop_running_hosts_for_report(
    hosts: u16,
    control: &Rc<RefCell<Control>>,
    commands: &HostCommands,
) {
    let running = (0..hosts)
        .filter(|host| control.borrow().up[usize::from(*host)])
        .collect::<Vec<_>>();
    let mut completed = Vec::with_capacity(running.len());
    {
        let mut commands = commands.borrow_mut();
        for host in running {
            let (complete, completion) = oneshot();
            commands.push_back(HostCommand::Crash(HostId::new(u32::from(host)), complete));
            completed.push(completion);
        }
    }
    for completion in completed {
        let _ = completion.await;
    }
}

async fn wait_for_initial_authority(config: &ClusterConfig, states: &StateSlots) {
    if config.membership_events.is_empty() {
        return;
    }
    let deadline = now().saturating_add(millis(10_000));
    let expected_epoch = config
        .daemon
        .cluster_placement
        .as_ref()
        .expect("cluster placement is initialized")
        .membership_epoch;
    loop {
        let ready = (1..=config.volume_count).all(|number| {
            let volume = VolumeId(u64::from(number));
            let host = usize::from((number - 1) % config.hosts);
            let state = states[host].borrow();
            let state = state.borrow();
            state.authority_ready()
                && state.authority_placement_epoch() == expected_epoch
                && state.volume_authorized(volume)
        });
        if ready {
            return;
        }
        assert!(
            now() < deadline,
            "initial store-backed authority did not become ready"
        );
        delay(millis(1)).await;
    }
}

#[allow(clippy::too_many_lines)]
async fn audit_dynamic_authority(
    config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    states: &StateSlots,
    control: &Rc<RefCell<Control>>,
) {
    let Ok(Some(placement)) = read_placement(worlds[0].as_ref()).await else {
        control
            .borrow_mut()
            .report
            .violations
            .push("final authority audit found no durable placement".to_owned());
        return;
    };
    let expected_epoch = control.borrow().report.placement_epoch_final;
    if placement.placement.epoch != expected_epoch {
        control.borrow_mut().report.violations.push(format!(
            "authority and actor placement views diverged: authority {}, actor {expected_epoch}",
            placement.placement.epoch,
        ));
    }
    for host in 0..config.hosts {
        let identity = host_id(host);
        let session = read_host_session(worlds[0].as_ref(), identity).await;
        let expected_active = placement.placement.contains(identity);
        match session {
            Ok(Some(session))
                if expected_active
                    && matches!(session.record, HostSessionRecord::Active { .. }) => {}
            Ok(Some(_)) if !expected_active => {}
            other => control.borrow_mut().report.violations.push(format!(
                "final authority session for {identity:?} did not match liveness: {other:?}"
            )),
        }
        if expected_active && control.borrow().up[usize::from(host)] {
            let state = states[usize::from(host)].borrow();
            let state = state.borrow();
            if state.authority_placement_epoch() != placement.placement.epoch {
                control.borrow_mut().report.violations.push(format!(
                    "host {host} cached authority placement epoch {}, expected {}",
                    state.authority_placement_epoch(),
                    placement.placement.epoch
                ));
            }
        }
    }
}

async fn wait_for_lifecycle_drain(
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    states: &StateSlots,
    config: &Rc<ClusterConfig>,
) -> bool {
    let deadline = now().saturating_add(FINAL_LIFECYCLE_DRAIN_TIMEOUT);
    loop {
        let attempts = control
            .borrow()
            .migrations
            .iter()
            .map(|(&volume, &attempt)| (volume, attempt))
            .collect::<Vec<_>>();
        for (volume, attempt) in attempts {
            reconcile_accepted_migration(volume, attempt, control, worlds, states, config);
        }
        let drained = {
            let control = control.borrow();
            control.migrations.is_empty()
                && control.quiescing_guests.is_empty()
                && control.migration_cuts.is_empty()
                && control.deferred_source_recoveries.is_empty()
                && control.deferred_destination_recoveries.is_empty()
        };
        if drained {
            return true;
        }
        if now() >= deadline {
            return false;
        }
        delay(millis(1)).await;
    }
}

#[derive(Default)]
struct AuditReport {
    volumes: u64,
    pages: u64,
    protected_sync_volumes: u64,
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
            } else if volume_state.sync_ack_through > 0 {
                audit.protected_sync_volumes = audit.protected_sync_volumes.saturating_add(1);
            }
        }

        if let Some(head_bytes) = store.get(&layout::head_key(volume)) {
            match HeadRecord::decode(volume, head_bytes) {
                Ok(head) => {
                    if head.holder != host_id(placed) {
                        audit.violations.push(format!(
                            "final audit head for {volume:?} names {:?}, placement names {:?}",
                            head.holder,
                            host_id(placed)
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
                let fault_failure = match select2(
                    world.fault(page, false),
                    delay(FINAL_PAGE_AUDIT_TIMEOUT),
                )
                .await
                {
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
                if guest.post_restart_fault_pending.replace(false) {
                    guest
                        .post_restart_faults
                        .set(guest.post_restart_faults.get().saturating_add(1));
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
    let placement = config
        .daemon
        .cluster_placement
        .as_ref()
        .expect("cluster placement is initialized")
        .clone();
    host_config_with_placement(config, host, placement)
}

async fn load_startup_placement(
    world: &SimWorld,
    config: &ClusterConfig,
    host: u16,
) -> ClusterPlacementConfig {
    let configured = host_config(config, host)
        .cluster_placement
        .expect("cluster placement is initialized");
    if config.membership_events.is_empty() {
        return configured;
    }
    let Some((_, bytes)) =
        retry_get(world, &layout::placement_key(), config.daemon.backup_retry).await
    else {
        return configured;
    };
    let durable = ClusterPlacement::decode(&bytes)
        .filter(|placement| placement.cluster_id == DYNAMIC_AUTHORITY_CLUSTER_ID)
        .expect("durable cluster placement is canonical and belongs to this cluster");
    ClusterPlacementConfig {
        membership_epoch: durable.epoch,
        roster: durable.roster,
        authority: configured.authority,
    }
}

fn host_config_with_placement(
    config: &ClusterConfig,
    host: u16,
    placement: ClusterPlacementConfig,
) -> HostConfig {
    HostConfig {
        archive: config.daemon.archive,
        host: HostId::new(u32::from(host)),
        cache_pages: config.daemon.cache_pages,
        writeback_interval: config.daemon.writeback_interval,
        backup_retry: config.daemon.backup_retry,
        disk_capacity: config.daemon.disk_capacity,
        disk_headroom: config.daemon.disk_headroom,
        wedge_ticks: config.daemon.wedge_ticks,
        cluster_placement: Some(placement),
    }
}

fn verify_membership_convergence(
    config: &ClusterConfig,
    states: &StateSlots,
    control: &Rc<RefCell<Control>>,
    placements: &PlacementSlots,
    model: &Rc<RefCell<MembershipModel>>,
) {
    if config.membership_events.is_empty() {
        return;
    }
    let final_epoch = model.borrow().placement_epoch;
    let active = model
        .borrow()
        .observed_records
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    for host in 0..config.hosts {
        if !active.contains(&HostId::new(u32::from(host)))
            || !control.borrow().up[usize::from(host)]
        {
            continue;
        }
        let slot_epoch = placements.borrow()[usize::from(host)].membership_epoch;
        let state_epoch = states[usize::from(host)]
            .borrow()
            .borrow()
            .config
            .cluster_placement
            .as_ref()
            .map(|placement| placement.membership_epoch);
        if slot_epoch != final_epoch || state_epoch != Some(final_epoch) {
            control.borrow_mut().report.violations.push(format!(
                "membership placement did not converge on host {host}: slot {slot_epoch}, state {state_epoch:?}, expected {final_epoch}"
            ));
        }
    }
    let mut control = control.borrow_mut();
    control.report.placement_epoch_final = final_epoch;
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
                        control.report.authority_transfers =
                            control.report.authority_transfers.saturating_add(1);
                        control.report.max_migration_pause_ns = control
                            .report
                            .max_migration_pause_ns
                            .max(migration_pause_ns(attempt));
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
                            && world.run_generation() != attempt.to_run_generation
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
                        attempt.to == host && world.run_generation() != attempt.to_run_generation
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
                            && world.run_generation() != attempt.from_run_generation
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
                    if attempt.from == host && world.run_generation() != attempt.from_run_generation
                    {
                        control
                            .borrow_mut()
                            .deferred_source_recoveries
                            .insert(volume, (host, verdict));
                    }
                    continue;
                }
                let orphan_recovery = config
                    .daemon
                    .cluster_placement
                    .as_ref()
                    .and_then(|placement| placement.authority)
                    .is_some()
                    && control
                        .borrow()
                        .placement
                        .get(&volume)
                        .is_some_and(|&placed| {
                            placed != host && !control.borrow().live[usize::from(placed)]
                        });
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
                        if orphan_recovery {
                            control.report.restores = control.report.restores.saturating_add(1);
                            if world
                                .store_bytes(&layout::head_key(volume))
                                .and_then(|bytes| HeadRecord::decode(volume, &bytes).ok())
                                .is_some_and(|head| head.stash.is_some())
                            {
                                // Recovery preserves the durable passive assignment.
                                control.report.stash_recoveries =
                                    control.report.stash_recoveries.saturating_add(1);
                            }
                        }
                    }
                }
                let authority_quiesced = config
                    .daemon
                    .cluster_placement
                    .as_ref()
                    .and_then(|placement| placement.authority)
                    .is_some()
                    && control.borrow().quiescing_guests.contains(&volume)
                    && !control.borrow().migrations.contains_key(&volume);
                if runnable && !authority_quiesced {
                    start_guest(volume, host, &control, &worlds, &config);
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct RestoreTarget {
    host: u16,
    run_generation: u64,
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
                && worlds[usize::from(target.host)].run_generation() == target.run_generation
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
            run_generation: worlds[usize::from(host)].run_generation(),
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
        control.report.authority_transfers = control.report.authority_transfers.saturating_add(1);
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

struct MigrationCompletionContext {
    control: Rc<RefCell<Control>>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    config: Rc<ClusterConfig>,
}

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
    context: MigrationCompletionContext,
) {
    let MigrationCompletionContext {
        control,
        worlds,
        config,
    } = context;
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

fn reconcile_accepted_migration(
    volume: VolumeId,
    attempt: MigrationAttempt,
    control: &Rc<RefCell<Control>>,
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    states: &StateSlots,
    config: &Rc<ClusterConfig>,
) {
    if control.borrow().migrations.get(&volume) != Some(&attempt) {
        return;
    }
    let authority = worlds[0]
        .store_bytes(&layout::head_key(volume))
        .and_then(|bytes| HeadRecord::decode(volume, &bytes).ok())
        .and_then(|head| {
            (0..config.hosts).find(|&host| {
                host_id(host) == head.holder
                    && head.holder != host_id(attempt.from)
                    && head.fence != 0
                    && states[usize::from(host)]
                        .borrow()
                        .borrow()
                        .volumes
                        .get(&volume)
                        .is_some_and(|state| state.ready && state.fence == head.fence)
            })
        });
    if let Some(authority) = authority {
        let migrated = control.borrow().guest_state[&volume]
            .expected
            .borrow()
            .clone();
        *control.borrow().guest_state[&volume].durable.borrow_mut() = migrated;
        {
            let mut control = control.borrow_mut();
            if control.migrations.get(&volume) != Some(&attempt) {
                return;
            }
            control.migrations.remove(&volume);
            control.uncertain_migrations.remove(&volume);
            control.accepted_migrations.remove(&volume);
            control.deferred_source_recoveries.remove(&volume);
            control.deferred_destination_recoveries.remove(&volume);
            control.placement.insert(volume, authority);
            control.report.migrations = control.report.migrations.saturating_add(1);
            control.report.authority_transfers =
                control.report.authority_transfers.saturating_add(1);
            control.report.max_migration_pause_ns = control
                .report
                .max_migration_pause_ns
                .max(migration_pause_ns(attempt));
        }
        start_guest(volume, authority, control, worlds, config);
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
    let source_reconciled = worlds[usize::from(attempt.from)].run_generation()
        == attempt.from_run_generation
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
        control.report.authority_transfers = control.report.authority_transfers.saturating_add(1);
        control.report.max_migration_pause_ns = control
            .report
            .max_migration_pause_ns
            .max(migration_pause_ns(attempt));
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
        if state.post_restart_fault_pending.replace(false) {
            state
                .post_restart_faults
                .set(state.post_restart_faults.get().saturating_add(1));
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

fn membership_claim_key(host: HostId) -> String {
    format!("{MEMBERSHIP_CLAIM_PREFIX}{:08x}", host.get())
}

fn membership_member_key(host: HostId) -> String {
    format!("{MEMBERSHIP_MEMBER_PREFIX}{:08x}", host.get())
}

fn encode_claim(host: HostId, token: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(10);
    bytes.extend_from_slice(&host.get().to_le_bytes());
    bytes.extend_from_slice(&token.to_le_bytes());
    bytes
}

fn placement_bytes(epoch: u64, roster: Vec<HostId>) -> Vec<u8> {
    ClusterPlacement {
        cluster_id: DYNAMIC_AUTHORITY_CLUSTER_ID,
        epoch,
        roster,
    }
    .encode()
}

async fn retry_get(world: &SimWorld, key: &str, retry: u64) -> Option<(u64, Vec<u8>)> {
    loop {
        match Store::get(world, key).await {
            Ok(found) => return found,
            Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                delay(retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(_)) => return None,
        }
    }
}

async fn membership_cas(
    world: &SimWorld,
    key: String,
    expected: Option<u64>,
    bytes: Vec<u8>,
    retry: u64,
    commit_response_lost: bool,
    control: &Rc<RefCell<Control>>,
) -> Option<u64> {
    let committed = loop {
        match Store::put_cas(world, key.clone(), expected, bytes.clone()).await {
            Ok(version) => break version,
            Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                {
                    let mut control = control.borrow_mut();
                    control.report.membership_claim_retries =
                        control.report.membership_claim_retries.saturating_add(1);
                }
                delay(retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(_)) => return None,
        }
    };
    if !commit_response_lost {
        return Some(committed);
    }
    {
        let mut control = control.borrow_mut();
        control.report.membership_committed_lost =
            control.report.membership_committed_lost.saturating_add(1);
    }
    match Store::put_cas(world, key.clone(), expected, bytes.clone()).await {
        Err(StoreError::Fault(blockd_core::protocol::StoreFault::CasConflict { .. })) => {
            let mut control = control.borrow_mut();
            control.report.membership_claim_retries =
                control.report.membership_claim_retries.saturating_add(1);
        }
        _ => return None,
    }
    retry_get(world, &key, retry)
        .await
        .filter(|(_, found)| *found == bytes)
        .map(|(version, _)| version)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_membership_event(
    event: MembershipEvent,
    config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    states: &StateSlots,
    control: &Rc<RefCell<Control>>,
    commands: &HostCommands,
    membership: &PeerMembership,
    placements: &PlacementSlots,
    model: &Rc<RefCell<MembershipModel>>,
) {
    let at = match event {
        MembershipEvent::Claim { at, .. }
        | MembershipEvent::Publish { at, .. }
        | MembershipEvent::Discover { at, .. }
        | MembershipEvent::RotateCertificate { at, .. }
        | MembershipEvent::Restart { at, .. } => at,
    };
    delay(at).await;
    match event {
        MembershipEvent::Claim {
            host,
            token,
            commit_response_lost,
            ..
        } => {
            let host = HostId::new(u32::from(host));
            let key = membership_claim_key(host);
            let bytes = encode_claim(host, token);
            if membership_cas(
                worlds[0].as_ref(),
                key,
                None,
                bytes,
                config.daemon.backup_retry,
                commit_response_lost,
                control,
            )
            .await
            .is_some()
            {
                let mut model = model.borrow_mut();
                model.claims.insert(host, token);
                let mut control = control.borrow_mut();
                control.report.membership_claims =
                    control.report.membership_claims.saturating_add(1);
            }
        }
        MembershipEvent::Publish {
            host,
            lease_duration,
            certificate_generation,
            commit_response_lost,
            ..
        } => {
            publish_member(
                HostId::new(u32::from(host)),
                now().saturating_add(lease_duration),
                certificate_generation,
                commit_response_lost,
                config,
                worlds[0].as_ref(),
                control,
                model,
            )
            .await;
        }
        MembershipEvent::RotateCertificate {
            host,
            certificate_generation,
            commit_response_lost,
            ..
        } => {
            let host = HostId::new(u32::from(host));
            send_certificate_probe(host, worlds, control, membership, true);
            let expiry = model.borrow().observed_records.get(&host).map_or_else(
                || now().saturating_add(config.horizon),
                |record| record.lease_expires_at,
            );
            publish_member(
                host,
                expiry,
                certificate_generation,
                commit_response_lost,
                config,
                worlds[0].as_ref(),
                control,
                model,
            )
            .await;
            let mut control = control.borrow_mut();
            control.report.membership_certificate_rotations = control
                .report
                .membership_certificate_rotations
                .saturating_add(1);
        }
        MembershipEvent::Discover {
            observer,
            reverse_list,
            reverse_gets,
            ..
        } => {
            discover_membership(
                observer,
                reverse_list,
                reverse_gets,
                config,
                worlds,
                control,
                membership,
                placements,
                model,
            )
            .await;
        }
        MembershipEvent::Restart {
            host,
            downtime,
            class,
            ..
        } => {
            let restarts_owner = control
                .borrow()
                .placement
                .values()
                .any(|placed| *placed == host);
            // Poison the shortcut used by the old harness. A rolling restart
            // that owns data can succeed only if its new HostState is built
            // from the durable placement object rather than this stale slot.
            let poisoned_slot = (class == RestartClass::Rolling && restarts_owner).then(|| {
                let current = placements.borrow()[usize::from(host)].clone();
                let mut stale = current.clone();
                stale.membership_epoch = stale.membership_epoch.saturating_sub(1).max(1);
                assert_ne!(stale.membership_epoch, current.membership_epoch);
                placements.borrow_mut()[usize::from(host)] = stale;
                current
            });
            let outcome =
                restart_host_for(host, downtime, config, worlds, states, control, commands).await;
            let preserved = model
                .borrow()
                .observed_records
                .get(&HostId::new(u32::from(host)))
                .is_some_and(|record| {
                    record.lease_expires_at > now()
                        && membership
                            .borrow()
                            .contains_key(&HostId::new(u32::from(host)))
                });
            let placement_epoch = model.borrow().placement_epoch;
            let durable_placement = retry_get(
                worlds[0].as_ref(),
                &layout::placement_key(),
                config.daemon.backup_retry,
            )
            .await
            .and_then(|(_, bytes)| ClusterPlacement::decode(&bytes))
            .filter(|placement| {
                placement.cluster_id == DYNAMIC_AUTHORITY_CLUSTER_ID
                    && placement.epoch == placement_epoch
            });
            let recovered_placement = match (outcome.as_ref(), durable_placement.as_ref()) {
                (Some(outcome), Some(durable)) => {
                    wait_for_restarted_placement(
                        states,
                        host,
                        &outcome.retired_state,
                        durable,
                        config.daemon.backup_retry,
                    )
                    .await
                }
                _ => false,
            };
            if let Some(slot) = poisoned_slot {
                placements.borrow_mut()[usize::from(host)] = slot;
            }
            let mut control = control.borrow_mut();
            match class {
                RestartClass::Fast => {
                    control.report.membership_fast_restarts =
                        control.report.membership_fast_restarts.saturating_add(1);
                }
                RestartClass::Slow => {
                    control.report.membership_slow_restarts =
                        control.report.membership_slow_restarts.saturating_add(1);
                }
                RestartClass::Rolling => {
                    control.report.membership_rolling_restarts =
                        control.report.membership_rolling_restarts.saturating_add(1);
                }
            }
            if preserved {
                control.report.membership_lease_preserved_restarts = control
                    .report
                    .membership_lease_preserved_restarts
                    .saturating_add(1);
            }
            if recovered_placement {
                control.report.placement_recovered_after_restart = control
                    .report
                    .placement_recovered_after_restart
                    .saturating_add(1);
                if outcome
                    .as_ref()
                    .is_some_and(|outcome| !outcome.affected_volumes.is_empty())
                {
                    control.report.placement_owner_recovered_after_restart = control
                        .report
                        .placement_owner_recovered_after_restart
                        .saturating_add(1);
                }
            } else {
                control.report.violations.push(format!(
                    "restarted host {host} did not install durable cluster placement epoch {placement_epoch} before actor startup"
                ));
            }
        }
    }
}

async fn wait_for_restarted_placement(
    states: &StateSlots,
    host: u16,
    retired: &SharedState,
    durable: &ClusterPlacement,
    retry: u64,
) -> bool {
    for _ in 0..32 {
        let current = Rc::clone(&states[usize::from(host)].borrow());
        if !Rc::ptr_eq(&current, retired) {
            let state = current.borrow();
            if state
                .config
                .cluster_placement
                .as_ref()
                .is_some_and(|placement| {
                    placement.membership_epoch == durable.epoch
                        && placement.roster == durable.roster
                })
            {
                return true;
            }
        }
        delay(retry).await;
    }
    false
}

fn send_certificate_probe(
    from: HostId,
    worlds: &[Rc<SimWorld>],
    control: &Rc<RefCell<Control>>,
    membership: &PeerMembership,
    delayed: bool,
) {
    if !control
        .borrow()
        .up
        .get(host_index(from))
        .copied()
        .unwrap_or(false)
    {
        return;
    }
    let target = membership
        .borrow()
        .values()
        .find(|authorization| authorization.identity != from)
        .map(|authorization| authorization.identity);
    if let Some(target) = target {
        let world = &worlds[host_index(from)];
        if delayed {
            let _ = world.hold_peer_authentication_probe(target);
        } else {
            let _ = world.release_peer_authentication_probe();
            let _ = world.send_peer_authentication_probe(target);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn publish_member(
    host: HostId,
    lease_expires_at: u64,
    certificate_generation: u64,
    commit_response_lost: bool,
    config: &ClusterConfig,
    world: &SimWorld,
    control: &Rc<RefCell<Control>>,
    model: &Rc<RefCell<MembershipModel>>,
) {
    let Some(token) = model.borrow().claims.get(&host).copied() else {
        control.borrow_mut().report.violations.push(format!(
            "membership publication for {host:?} had no durable claim"
        ));
        return;
    };
    let record = MembershipRecord {
        host,
        token,
        lease_expires_at,
        certificate_generation,
    };
    let expected = model.borrow().member_versions.get(&host).copied();
    if let Some(version) = membership_cas(
        world,
        membership_member_key(host),
        expected,
        record.encode(),
        config.daemon.backup_retry,
        commit_response_lost,
        control,
    )
    .await
    {
        model.borrow_mut().member_versions.insert(host, version);
        let mut control = control.borrow_mut();
        control.report.membership_publications =
            control.report.membership_publications.saturating_add(1);
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn discover_membership(
    observer: u16,
    reverse_list: bool,
    reverse_gets: bool,
    config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    control: &Rc<RefCell<Control>>,
    membership: &PeerMembership,
    placements: &PlacementSlots,
    model: &Rc<RefCell<MembershipModel>>,
) {
    let mut keys = loop {
        match Store::list_prefix(
            worlds[usize::from(observer)].as_ref(),
            MEMBERSHIP_MEMBER_PREFIX,
        )
        .await
        {
            Ok(keys) => break keys,
            Err(StoreError::Fault(blockd_core::protocol::StoreFault::Unavailable)) => {
                delay(config.daemon.backup_retry).await;
            }
            Err(StoreError::TooLarge | StoreError::Fault(_)) => return,
        }
    };
    {
        let mut control = control.borrow_mut();
        control.report.membership_lists = control.report.membership_lists.saturating_add(1);
        if reverse_list {
            control.report.membership_reordered_lists =
                control.report.membership_reordered_lists.saturating_add(1);
        }
        if reverse_gets {
            control.report.membership_reordered_gets =
                control.report.membership_reordered_gets.saturating_add(1);
        }
    }
    if reverse_list {
        keys.reverse();
    }
    if reverse_gets && keys.len() > 1 {
        keys.rotate_left(1);
    }

    let previous_records = model.borrow().observed_records.clone();
    let current_membership = membership.borrow().keys().copied().collect::<BTreeSet<_>>();
    let mut records = BTreeMap::new();
    let mut expired = 0u64;
    for key in keys {
        let Some((version, bytes)) = retry_get(
            worlds[usize::from(observer)].as_ref(),
            &key,
            config.daemon.backup_retry,
        )
        .await
        else {
            continue;
        };
        {
            let mut control = control.borrow_mut();
            control.report.membership_gets = control.report.membership_gets.saturating_add(1);
        }
        let Some(record) = MembershipRecord::decode(&bytes) else {
            control
                .borrow_mut()
                .report
                .violations
                .push(format!("membership discovery decoded corrupt object {key}"));
            continue;
        };
        model
            .borrow_mut()
            .member_versions
            .insert(record.host, version);
        if record.lease_expires_at <= now() {
            if current_membership.contains(&record.host) {
                expired = expired.saturating_add(1);
            }
            continue;
        }
        let claim = retry_get(
            worlds[usize::from(observer)].as_ref(),
            &membership_claim_key(record.host),
            config.daemon.backup_retry,
        )
        .await;
        {
            let mut control = control.borrow_mut();
            control.report.membership_gets = control.report.membership_gets.saturating_add(1);
        }
        if claim
            .as_ref()
            .is_some_and(|(_, bytes)| *bytes == encode_claim(record.host, record.token))
        {
            records.insert(record.host, record);
        }
    }
    let rotated_hosts = records
        .iter()
        .filter_map(|(host, current)| {
            previous_records
                .get(host)
                .is_some_and(|old| current.certificate_generation > old.certificate_generation)
                .then_some(*host)
        })
        .collect::<Vec<_>>();
    let next = records.keys().copied().collect::<BTreeSet<_>>();
    let next_authorizations = records
        .values()
        .map(|record| {
            let identity = record.host;
            (
                identity,
                PeerAuthorization::new(identity, record.certificate_generation),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let previous = current_membership;
    let joins = next.difference(&previous).count() as u64;
    let leaves = previous.difference(&next).count() as u64;
    {
        let mut model = model.borrow_mut();
        model.observed_records = records;
    }
    {
        let mut control = control.borrow_mut();
        control.report.membership_lease_expiries = control
            .report
            .membership_lease_expiries
            .saturating_add(expired);
    }
    if next == previous {
        if *membership.borrow() != next_authorizations {
            membership.borrow_mut().clone_from(&next_authorizations);
            let roster = (0..config.hosts).map(|host| {
                model
                    .borrow()
                    .observed_records
                    .get(&HostId::new(u32::from(host)))
                    .map_or_else(|| host_id(host), |record| record.host)
            });
            let roster = roster.collect::<Vec<_>>();
            for world in worlds {
                world.replace_peer_identities(roster.iter().copied());
            }
            for host in rotated_hosts {
                send_certificate_probe(host, worlds, control, membership, false);
            }
        }
        return;
    }

    let epoch = model
        .borrow()
        .placement_epoch
        .checked_add(1)
        .expect("membership epoch overflow");
    let roster = next.iter().copied().collect::<Vec<_>>();
    let expected = model.borrow().placement_version;
    let Some(version) = membership_cas(
        worlds[usize::from(observer)].as_ref(),
        layout::placement_key(),
        expected,
        placement_bytes(epoch, roster.clone()),
        config.daemon.backup_retry,
        false,
        control,
    )
    .await
    else {
        control
            .borrow_mut()
            .report
            .violations
            .push("durable membership placement CAS failed".to_owned());
        return;
    };
    {
        let mut model = model.borrow_mut();
        model.placement_version = Some(version);
        model.placement_epoch = epoch;
    }
    membership.borrow_mut().clone_from(&next_authorizations);
    for host in &rotated_hosts {
        send_certificate_probe(*host, worlds, control, membership, false);
    }
    for world in worlds {
        world.replace_peer_identities(roster.iter().copied());
    }
    for host in 0..config.hosts {
        let authority = placements.borrow()[usize::from(host)].authority;
        placements.borrow_mut()[usize::from(host)] = ClusterPlacementConfig {
            membership_epoch: epoch,
            roster: roster.clone(),
            authority,
        };
    }
    for host in next.iter().copied() {
        if !control.borrow().up[host_index(host)] {
            continue;
        }
        let placement = placements.borrow()[host_index(host)].clone();
        if !matches!(
            worlds[host_index(host)]
                .request_admin(AdminCall::UpdateClusterPlacement { placement })
                .await,
            Ok(Ok(AdminSuccess::ClusterPlacementUpdated))
        ) {
            control.borrow_mut().report.violations.push(format!(
                "membership placement epoch {epoch} was rejected by host {host}"
            ));
        }
    }
    let rotations = rotated_hosts.len() as u64;
    let mut control = control.borrow_mut();
    control.report.membership_transitions = control.report.membership_transitions.saturating_add(1);
    control.report.membership_joins = control.report.membership_joins.saturating_add(joins);
    control.report.membership_leaves = control.report.membership_leaves.saturating_add(leaves);
    control.report.membership_certificate_rotations = control
        .report
        .membership_certificate_rotations
        .saturating_add(rotations);
    control.report.durable_placement_writes =
        control.report.durable_placement_writes.saturating_add(1);
    control.report.placement_epoch_final = epoch;
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
fn spawn_schedules(
    config: &Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
    peer_stats: Rc<PeerTransportStats>,
    peer_membership: PeerMembership,
    placement_slots: PlacementSlots,
    membership_model: Rc<RefCell<MembershipModel>>,
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
            Rc::clone(&peer_membership),
            Rc::clone(&placement_slots),
            Rc::clone(&membership_model),
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
    for event in config.membership_events.clone() {
        let config = Rc::clone(config);
        let worlds = Rc::clone(&worlds);
        let states = Rc::clone(&states);
        let control = Rc::clone(&control);
        let commands = Rc::clone(&commands);
        let membership = Rc::clone(&peer_membership);
        let placements = Rc::clone(&placement_slots);
        let model = Rc::clone(&membership_model);
        spawn(async move {
            run_membership_event(
                event,
                &config,
                &worlds,
                &states,
                &control,
                &commands,
                &membership,
                &placements,
                &model,
            )
            .await;
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
    let downtime = random_between(config.restart_delay.0, config.restart_delay.1);
    let _ = restart_host_for(host, downtime, config, worlds, states, control, commands).await;
}

async fn restart_host_for(
    host: u16,
    downtime: u64,
    _config: &ClusterConfig,
    worlds: &[Rc<SimWorld>],
    states: &StateSlots,
    control: &Rc<RefCell<Control>>,
    commands: &HostCommands,
) -> Option<RestartOutcome> {
    if !control.borrow().up[usize::from(host)] {
        return None;
    }
    let retired_state = Rc::clone(&states[usize::from(host)].borrow());
    control.borrow_mut().up[usize::from(host)] = false;
    host_command(commands, |complete| {
        HostCommand::Crash(HostId::new(u32::from(host)), complete)
    })
    .await;
    worlds[usize::from(host)].advance_run_generation();
    let affected = control
        .borrow()
        .placement
        .iter()
        .filter_map(|(&volume, &placed)| (placed == host).then_some(volume))
        .collect::<Vec<_>>();
    for &volume in &affected {
        control.borrow().guest_state[&volume]
            .post_restart_fault_pending
            .set(true);
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
    delay(downtime).await;
    worlds[usize::from(host)].clear_abort();
    host_command(commands, |complete| {
        HostCommand::Bounce(HostId::new(u32::from(host)), complete)
    })
    .await;
    control.borrow_mut().up[usize::from(host)] = true;
    Some(RestartOutcome {
        retired_state,
        affected_volumes: affected,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn at_kill(
    at: u64,
    host: u16,
    config: Rc<ClusterConfig>,
    worlds: Rc<Vec<Rc<SimWorld>>>,
    states: StateSlots,
    control: Rc<RefCell<Control>>,
    commands: HostCommands,
    membership: PeerMembership,
    placements: PlacementSlots,
    model: Rc<RefCell<MembershipModel>>,
) {
    delay(at).await;
    if !control.borrow().up[usize::from(host)] {
        return;
    }
    let authority_enabled = config
        .daemon
        .cluster_placement
        .as_ref()
        .and_then(|placement| placement.authority)
        .is_some();
    let affected = control
        .borrow()
        .placement
        .iter()
        .filter_map(|(&volume, &placed)| (placed == host).then_some(volume))
        .collect::<Vec<_>>();
    control.borrow_mut().up[usize::from(host)] = false;
    host_command(&commands, |complete| {
        HostCommand::Crash(HostId::new(u32::from(host)), complete)
    })
    .await;
    worlds[usize::from(host)].advance_run_generation();
    control.borrow_mut().live[usize::from(host)] = false;
    control
        .borrow_mut()
        .retired_counters
        .push(states[usize::from(host)].borrow().borrow().counters);
    let orphaned_at = now();
    let mut promoted = Vec::new();
    for volume in affected {
        cancel_guest(volume, &control);
        let had_stash = worlds[0]
            .store_bytes(&layout::head_key(volume))
            .and_then(|bytes| HeadRecord::decode(volume, &bytes).ok())
            .is_some_and(|head| head.stash.is_some());
        if !promote_orphan(host, volume, &config, &worlds, &control).await {
            control
                .borrow_mut()
                .report
                .violations
                .push(format!("unable to promote orphan {volume:?}"));
            continue;
        }
        if had_stash {
            let mut control = control.borrow_mut();
            control.report.stash_recoveries = control.report.stash_recoveries.saturating_add(1);
        }
        prepare_backed_loss(&control.borrow().guest_state[&volume], volume, &worlds[0]);
        promoted.push(volume);
    }
    let mut restore_tasks = TaskSet::new();
    let (restore_completed, mut restore_completions) = unbounded();
    let mut restores_active = 0usize;
    for volume in promoted {
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
            let candidate_run_generation = worlds[usize::from(candidate)].run_generation();
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
                        run_generation: candidate_run_generation,
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
    if authority_enabled {
        reconfigure_cluster_membership(&worlds, &control, &membership, &placements, &model).await;
    }
}

fn build_cluster_placement(
    _worlds: &[Rc<SimWorld>],
    control: &Rc<RefCell<Control>>,
    membership: &PeerMembership,
    current: &ClusterPlacement,
) -> Result<ClusterPlacement, String> {
    let mut roster = current
        .roster
        .iter()
        .copied()
        .filter(|&candidate| {
            membership.borrow().contains_key(&candidate)
                && control.borrow().live[host_index(candidate)]
        })
        .collect::<Vec<_>>();
    roster.sort_unstable();
    if roster.len() < blockd_core::placement::MIN_PLACEMENT_MEMBERS {
        return Err("cluster placement has fewer than three live members".to_owned());
    }
    let next = ClusterPlacement {
        cluster_id: DYNAMIC_AUTHORITY_CLUSTER_ID,
        epoch: current.epoch.saturating_add(1),
        roster,
    };
    next.validate()
        .ok_or_else(|| "invalid cluster placement".to_owned())?;
    Ok(next)
}

async fn reconfigure_cluster_membership(
    worlds: &Rc<Vec<Rc<SimWorld>>>,
    control: &Rc<RefCell<Control>>,
    membership: &PeerMembership,
    placements: &PlacementSlots,
    model: &Rc<RefCell<MembershipModel>>,
) {
    let Ok(Some(placement)) = read_placement(worlds[0].as_ref()).await else {
        control
            .borrow_mut()
            .report
            .violations
            .push("cluster placement unavailable during membership transition".to_owned());
        return;
    };
    let next = match build_cluster_placement(worlds, control, membership, &placement.placement) {
        Ok(next) => next,
        Err(reason) => {
            control.borrow_mut().report.violations.push(reason);
            return;
        }
    };
    if next.roster == placement.placement.roster {
        return;
    }
    let Ok(committed) = cas_placement(worlds[0].as_ref(), Some(&placement), next.clone()).await
    else {
        control
            .borrow_mut()
            .report
            .violations
            .push("cluster placement CAS failed".to_owned());
        return;
    };
    {
        let mut model = model.borrow_mut();
        model.placement_version = Some(committed.store_version);
        model.placement_epoch = next.epoch;
    }
    let host_count = placements.borrow().len();
    for host in 0..host_count {
        let authority = placements.borrow()[host].authority;
        let config = ClusterPlacementConfig {
            membership_epoch: next.epoch,
            roster: next.roster.clone(),
            authority,
        };
        placements.borrow_mut()[host] = config.clone();
        if control.borrow().up[host] {
            let reply =
                worlds[host].request_admin(AdminCall::UpdateClusterPlacement { placement: config });
            let _ = reply.await;
        }
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
    let source_identity = host_id(source);
    if head.holder != source_identity {
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
        let peer = host_id(u16::try_from(peer).expect("host index fits"));
        let mut generations: BTreeMap<(blockd_core::types::HostId, u64), BTreeMap<u64, Vec<u8>>> =
            BTreeMap::new();
        for (name, bytes) in world.durable_blobs() {
            if let Some(layout::BlobName::ReplicaSpool {
                source: found_source,
                volume: found_volume,
                assignment_epoch,
                generation,
            }) = layout::parse_blob(&name)
                && (found_source, found_volume) == (HostId::new(u32::from(source)), volume)
                && (allowed.contains(&(peer, assignment_epoch))
                    || head
                        .stash
                        .is_some_and(|stash| assignment_epoch > stash.assignment_epoch))
            {
                generations
                    .entry((found_source, assignment_epoch))
                    .or_default()
                    .insert(generation, bytes);
            }
        }
        for ((source_identity, assignment_epoch), generations) in generations {
            owned.push((
                source_identity,
                peer,
                assignment_epoch,
                generations.into_values().flatten().collect::<Vec<_>>(),
            ));
        }
    }
    let residues = owned
        .iter()
        .map(|(source, peer, assignment_epoch, bytes)| ReplicaResidue {
            source: *source,
            peer: *peer,
            assignment_epoch: *assignment_epoch,
            bytes,
        })
        .collect::<Vec<_>>();
    let export = if residues.is_empty() {
        None
    } else {
        match export_replica_recovery(
            source_identity,
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
            holder: source_identity,
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
    let claim = prepare_replica_recovery_claim(observed_version, &head, source_identity, &export);
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
        prepare_replica_publication(volume, source_identity, writer_fence, &claim.head, &export)
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
    let (source_up, original_run_generation, deferred_recovery) = {
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
            worlds[usize::from(attempt.from)].run_generation() == attempt.from_run_generation,
            control.deferred_source_recoveries.contains_key(&volume),
        )
    };
    if source_up && (original_run_generation || deferred_recovery) {
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
        offer_fence: worlds[0]
            .store_bytes(&layout::head_key(volume))
            .and_then(|bytes| HeadRecord::decode(volume, &bytes).ok())
            .filter(|head| head.holder == host_id(from))?
            .fence,
        from_run_generation: worlds[usize::from(from)].run_generation(),
        to_run_generation: worlds[usize::from(to)].run_generation(),
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
            && worlds[usize::from(from)].run_generation() == attempt.from_run_generation
            && worlds[usize::from(to)].run_generation() == attempt.to_run_generation
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
        to: HostId::new(u32::from(to)),
    });
    let (completed, completion) = oneshot();
    spawn(migration_completion(
        volume,
        attempt,
        reply,
        completed,
        MigrationCompletionContext {
            control: Rc::clone(control),
            worlds: Rc::clone(worlds),
            config: Rc::clone(config),
        },
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

    async fn install_test_heads(control: &Rc<RefCell<Control>>, worlds: &Rc<Vec<Rc<SimWorld>>>) {
        let placements = control.borrow().placement.clone();
        for (volume, host) in placements {
            let provisional = HeadRecord {
                volume,
                holder: host_id(host),
                fence: 0,
                manifest: None,
                stash: None,
                retired_stashes: Vec::new(),
            };
            let version = Store::put_cas(
                worlds[0].as_ref(),
                layout::head_key(volume),
                None,
                provisional.encode(),
            )
            .await
            .expect("install provisional test head");
            Store::put_cas(
                worlds[0].as_ref(),
                layout::head_key(volume),
                Some(version),
                HeadRecord {
                    fence: version,
                    ..provisional
                }
                .encode(),
            )
            .await
            .expect("finalize test head");
        }
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
    async fn superseded_restore_retries_on_a_current_live_run_generation() {
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
        worlds[usize::from(candidate)].advance_run_generation();
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
                        run_generation: 1,
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
                install_test_heads(&control, &worlds).await;
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
                install_test_heads(&control, &worlds).await;
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
                install_test_heads(&control, &worlds).await;
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
                    offer_fence: 1,
                    from_run_generation: worlds[1].run_generation(),
                    to_run_generation: worlds[0].run_generation(),
                    started: 0,
                };
                worlds[usize::from(attempt.from)].advance_run_generation();
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
                worlds[0].advance_run_generation();
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
                    MigrationCompletionContext {
                        control: Rc::clone(&control),
                        worlds: Rc::clone(&worlds),
                        config: Rc::clone(&config),
                    },
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
                    offer_fence: 1,
                    from_run_generation: worlds[1].run_generation(),
                    to_run_generation: worlds[0].run_generation(),
                    started: 0,
                };
                worlds[usize::from(attempt.from)].advance_run_generation();
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
                    MigrationCompletionContext {
                        control: Rc::clone(&control),
                        worlds: Rc::clone(&worlds),
                        config: Rc::clone(&config),
                    },
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
                    offer_fence: 1,
                    from_run_generation: worlds[1].run_generation(),
                    to_run_generation: worlds[0].run_generation(),
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
                    MigrationCompletionContext {
                        control: Rc::clone(&control),
                        worlds: Rc::clone(&worlds),
                        config: Rc::clone(&config),
                    },
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
                    offer_fence: 1,
                    from_run_generation: worlds[1].run_generation(),
                    to_run_generation: worlds[0].run_generation(),
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
                worlds[usize::from(attempt.from)].advance_run_generation();
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
                install_test_heads(&control, &worlds).await;
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
                worlds[0].advance_run_generation();
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
