//! The multi-host harness: N real daemons, one shared object store, guests
//! that follow their vset's placement. Host death is permanent here — the
//! recovery story is the head-CAS restore onto a peer (R6.1/R6.3), with the
//! control plane deliberately racing two claimants to keep the
//! exactly-one-runner property under fire. Loss on host death is checked
//! against the head's manifest pointer at the instant of death (R4.3: the
//! archive horizon, nothing more).

use std::collections::{BTreeMap, BTreeSet};

use blockd_core::daemon::{Daemon, DaemonConfig, ReplicaPlacementConfig};
use blockd_core::head::{HeadRecord, ManifestPtr};
use blockd_core::journal::VsetConfig;
use blockd_core::layout;
use blockd_core::placement::PeerCandidate;
use blockd_core::replica_recovery::{
    ReplicaResidue, export_replica_recovery, refence_replica_export,
};
use blockd_core::seam::{AdminCmd, AdminReply, Effect, Event, HostMap, IoId, ReqId, Verdict};
use blockd_core::types::{HostId, PageId, SimTime, VsetId, micros, millis};

use crate::guest::{
    AttemptResult, FillResult, Guest, GuestState, PendingOp, UnparkResult, VsetMem,
};
use crate::harness::{Sabotage, archive_segment_efficiency};
use crate::kernel::Kernel;
use crate::oracle::Oracle;
use crate::world::blobdev::{BdevIo, BlobDev, BlobDevConfig};
use crate::world::store::{ObjectStore, StoreConfig, StoreCounters, Version};

pub use blockd_exec::FaultPoint;

/// One peer message kind, as the wedge nemesis targets them.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerKind {
    Offer,
    Accept,
    FetchRange,
    Page,
    FetchLeaf,
    Leaf,
    Released,
    ReleasedAck,
    ReplicaPut,
    ReplicaPutAck,
    ReplicaCommit,
    ReplicaCommitAck,
    ReplicaArchive,
    ReplicaStatus,
    ReplicaStatusReply,
    ReplicaUploadDone,
    ReplicaRelease,
    ReplicaReleaseAck,
}

impl PeerKind {
    fn of(msg: &blockd_core::seam::PeerMsg) -> PeerKind {
        use blockd_core::seam::PeerMsg;
        match msg {
            PeerMsg::MigrateOffer { .. } => PeerKind::Offer,
            PeerMsg::MigrateAccept { .. } => PeerKind::Accept,
            PeerMsg::FetchRange { .. } => PeerKind::FetchRange,
            PeerMsg::Page { .. } => PeerKind::Page,
            PeerMsg::FetchLeaf { .. } => PeerKind::FetchLeaf,
            PeerMsg::Leaf { .. } => PeerKind::Leaf,
            PeerMsg::Released { .. } => PeerKind::Released,
            PeerMsg::ReleasedAck { .. } => PeerKind::ReleasedAck,
            PeerMsg::ReplicaPut { .. } => PeerKind::ReplicaPut,
            PeerMsg::ReplicaPutAck { .. } => PeerKind::ReplicaPutAck,
            PeerMsg::ReplicaCommit { .. } => PeerKind::ReplicaCommit,
            PeerMsg::ReplicaCommitAck { .. } => PeerKind::ReplicaCommitAck,
            PeerMsg::ReplicaArchive { .. } => PeerKind::ReplicaArchive,
            PeerMsg::ReplicaStatus { .. } => PeerKind::ReplicaStatus,
            PeerMsg::ReplicaStatusReply { .. } => PeerKind::ReplicaStatusReply,
            PeerMsg::ReplicaUploadDone { .. } => PeerKind::ReplicaUploadDone,
            PeerMsg::ReplicaRelease { .. } => PeerKind::ReplicaRelease,
            PeerMsg::ReplicaReleaseAck { .. } => PeerKind::ReplicaReleaseAck,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub hosts: u16,
    pub daemon: DaemonConfig,
    pub bdev: BlobDevConfig,
    pub store: StoreConfig,
    pub vset_count: u16,
    pub vset_config: VsetConfig,
    pub horizon: u64,
    pub think: (u64, u64),
    pub checkpoint_interval: Option<u64>,
    /// Permanently kill hosts at instants.
    pub kill_hosts_at: Vec<(u64, u16)>,
    /// Crash hosts at instants: volatile state is lost and in-flight blob
    /// writes tear, but the host restarts after `restart_delay` and
    /// recovers from its disk.
    pub crash_hosts_at: Vec<(u64, u16)>,
    pub restart_delay: (u64, u64),
    /// Nemesis: mean interval between random host crashes (0 disables).
    pub crash_mean_interval: u64,
    /// Nemesis: mean interval between random migrations (0 disables).
    pub migrate_mean_interval: u64,
    /// Peer-message loss odds as (numerator, denominator); (0, 1) is a
    /// reliable channel. Draws happen only when the numerator is nonzero,
    /// so reliable configs replay byte-identically.
    pub peer_drop: (u64, u64),
    /// Peer-message duplication odds, same convention.
    pub peer_dup: (u64, u64),
    /// Deterministic directional link outages: `(begin, end, from, to)`.
    /// Sends attempted in the half-open window are dropped and counted.
    pub peer_link_outages: Vec<(u64, u64, u16, u16)>,
    /// Stable, explicit rare branches to force in a deterministic run.
    pub fault_points: Vec<FaultPoint>,
    /// Store outage window (R8.3): every store operation fails inside it.
    pub store_outage: Option<(u64, u64)>,
    /// Flip a bit in every stored resume-set object at this instant
    /// (R6.2's prefetch is a bet — a rotten one must cost nothing).
    pub rot_resume_set_at: Option<u64>,
    /// Flip a bit in every stored map-leaf object at this instant. The
    /// affected vsets' next adopter has dead spans: faults into them die
    /// loudly (R8.1) — a sanctioned, injected loss.
    pub rot_leaves_at: Option<u64>,
    /// Wedge nemesis (5b): drop 100% of ONE peer message kind inside the
    /// window `[start, end)` — the targeted outage probabilistic loss
    /// cannot produce, healing on schedule so post-heal convergence is
    /// checkable. The wedge counters must fire DURING the window and the
    /// system must converge after it.
    pub drop_peer: Option<(PeerKind, u64, u64)>,
    /// Send the restore of each orphaned vset to TWO hosts (CAS race).
    pub race_restore: bool,
    /// Migrate a vset to a destination host at an instant (R7).
    pub migrate_at: Option<(u64, VsetId, u16)>,
    /// Deliberately break one protocol rule (negative tests).
    pub sabotage: Option<Sabotage>,
    /// Override the guests' sync share of the op mix (`None` = default).
    pub guest_sync_share: Option<crate::rng::Ppm>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ClusterReport {
    pub trace_hash: u64,
    pub violations: Vec<String>,
    pub completed_ops: u64,
    pub restores: u64,
    /// Restore claims that lost the CAS race (exactly-one-runner, R6.3).
    pub claims_lost: u64,
    pub guest_deaths: u64,
    /// Restored vsets whose recovered point equalled the head's manifest at
    /// the kill instant (the R4.3 loss bound, verified).
    pub loss_bound_verified: u64,
    /// Completed migrations (both sides acknowledged, R7).
    pub migrations: u64,
    /// Slowest restore, `RestoreVset` to `VsetRestored` (the R6.2 budget).
    pub max_restore_ns: u64,
    /// Migration's guest-observed pause: source `PauseGuest` to the
    /// destination's `VsetMigratedIn` (the R7.1 budget).
    pub max_migration_pause_ns: u64,
    /// Resume-set pages prefetched across all live daemons (R6.2).
    pub prefetch_fills: u64,
    /// Peer messages the nemesis dropped / duplicated (fault coverage).
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
    /// Host crash-and-restart recoveries completed.
    pub recoveries: u64,
    /// `Released` deliveries: hydration drained a migrated vset's tail and
    /// freed its source.
    pub releases: u64,
    /// Migration requests the daemon refused (busy/unknown/wrong mode).
    pub migrations_refused: u64,
    /// Tail pages hydrated in the background across all live daemons.
    pub hydrate_fills: u64,
    /// Map leaves hydrated lazily across all live daemons.
    pub leaf_fills: u64,
    /// Store operations deferred by an outage (R8.3) across live daemons —
    /// publishes, claims, and parked demand fills.
    pub store_retries: u64,
    /// Peer messages refused by a protocol guard (R11.1) across live
    /// daemons.
    pub peer_rejected: u64,
    /// Messages the wedge nemesis dropped.
    pub nemesis_drops: u64,
    /// Wedge incidents across live daemons (R9.2 liveness watch).
    pub wedged_guests: u64,
    pub wedged_hydration: u64,
    pub wedged_outbound: u64,
    /// The liveness oracle's end-state: parked fills and still-hydrating
    /// vsets summed over live daemons after the drain. A healed run must
    /// end at (0, 0) — convergence, not just safety.
    pub parked_end: usize,
    pub hydrating_end: usize,
    pub replica_bytes: u64,
    pub replica_commits: u64,
    pub replica_store_bytes: u64,
    pub replica_unlinks: u64,
    pub replica_network_bytes: u64,
    pub replica_logical_bytes: u64,
    pub replica_nonactive_bytes: u64,
    pub replica_replacement_bytes: u64,
    pub replica_cleanup_rewrite_bytes: u64,
    pub replica_artifact_flushes: u64,
    pub replica_commit_flushes: u64,
    pub replica_rotations: u64,
    pub archive_cycles: u64,
    pub archive_commits_coalesced: u64,
    pub replica_capacity_backpressure: u64,
    pub published_segment_bytes: u64,
    pub published_live_entry_bytes: u64,
    pub published_dead_entry_bytes: u64,
    pub published_segment_overhead_bytes: u64,
    pub replica_spool_bytes: u64,
    pub peer_committed_through: u64,
    pub archived_through: u64,
    pub archive_lag_bytes: u64,
    pub max_archive_lag_age_ns: u64,
    pub segs_compacted: u64,
    pub pages_compacted: u64,
    pub store: StoreCounters,
    /// Blobs left on each host's device at the end of the run.
    pub blobs_per_host: Vec<usize>,
    /// Primary journal/segment/leaf blobs, excluding passive replica spools.
    pub primary_blobs_per_host: Vec<usize>,
}

struct MemView<'a> {
    host: u16,
    now: SimTime,
    mems: &'a BTreeMap<VsetId, VsetMem>,
}

impl HostMap for MemView<'_> {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        let mem = self.mems.get(&page.volume.vset).unwrap_or_else(|| {
            panic!(
                "capture read for unmapped vset: {page:?} on host {} at {:?}",
                self.host, self.now
            )
        });
        mem.pages
            .get(&page)
            .unwrap_or_else(|| {
                panic!(
                    "capture read for nonresident page: {page:?} on host {} at {:?}",
                    self.host, self.now
                )
            })
            .clone()
    }

    fn harvest_accessed(&self) -> Vec<PageId> {
        // One-shot: drain every guest's touch record. `mems` is a BTreeMap
        // and each set is ordered, so the result is deterministic.
        self.mems
            .values()
            .flat_map(|mem| mem.accessed.take())
            .collect()
    }
}

struct HostState {
    daemon: Option<Daemon>,
    inc: u32,
    bdev: BlobDev,
    mems: BTreeMap<VsetId, VsetMem>,
    /// The host's shared base-page tier bytes (R5.3).
    shared_base: BTreeMap<(u64, u64, blockd_core::types::SegId, u32), Vec<u8>>,
}

#[derive(Debug)]
enum Ev {
    Daemon {
        host: u16,
        inc: u32,
        event: Event,
    },
    BdevWriteDone {
        host: u16,
        inc: u32,
        bdev_io: BdevIo,
        io: IoId,
    },
    BdevReadDone {
        host: u16,
        inc: u32,
        io: IoId,
        bytes: Option<Vec<u8>>,
    },
    GuestStep {
        vset: VsetId,
    },
    CheckpointTick {
        vset: VsetId,
    },
    KillHost(u16),
    CrashHost(u16),
    RestartHost(u16),
    PromoteOrphan {
        source: u16,
        vset: VsetId,
        claimants: Vec<u16>,
    },
    StoreOutage(bool),
    RotResumeSets,
    RotLeaves,
    /// Nemesis ticks: a random crash / a random migration, self-scheduling.
    CrashNemesis,
    MigrateNemesis,
    MigrateAt {
        vset: VsetId,
        to: u16,
    },
    PeerDeliver {
        from: u16,
        to: u16,
        msg: blockd_core::seam::PeerMsg,
    },
}

struct Cluster {
    config: ClusterConfig,
    kernel: Kernel<Ev>,
    hosts: Vec<HostState>,
    store: ObjectStore,
    placement: BTreeMap<VsetId, u16>,
    guests: BTreeMap<VsetId, Guest>,
    oracle: Oracle,
    next_req: u64,
    sync_reqs: BTreeMap<ReqId, VsetId>,
    sync_started: BTreeMap<ReqId, SimTime>,
    sync_latencies: Vec<u64>,
    admin_reqs: BTreeMap<ReqId, VsetId>,
    /// Head manifest pointer captured at the kill instant, per orphan.
    expected_ptr: BTreeMap<VsetId, Option<ManifestPtr>>,
    /// `RestoreVset` send instants, for the R6.2 latency measurement.
    restore_sent: BTreeMap<ReqId, SimTime>,
    orphan_started: BTreeMap<VsetId, SimTime>,
    /// Last `PauseGuest` instant per vset (the R7.1 pause measurement).
    paused_at: BTreeMap<VsetId, SimTime>,
    /// Migrated-in vsets and the source host still serving their tail.
    migrated_from: BTreeMap<VsetId, u16>,
    /// Offers seen in flight: vset → (source, destination). A destination
    /// that durably accepted and then crashed recovers as the legitimate
    /// owner (R7.2) — this map is how `attach_recovered` tells that
    /// completion apart from a genuine second runner.
    pending_offers: BTreeMap<VsetId, (u16, u16)>,
    /// Vsets whose migration source died mid-drain: unservable pages are
    /// the sanctioned R7.3 loss, not a violation.
    doomed: BTreeSet<VsetId>,
    /// Permanently killed hosts (as opposed to crashed-and-restarting):
    /// they answer peer fetches with `None` so R7.3 fails loudly; a
    /// crashed host stays silent and retries bridge its downtime.
    dead: BTreeSet<u16>,
    /// Requests issued by `MigrateOut` (their failures are refusals, not
    /// lost restore claims).
    migrate_reqs: BTreeSet<ReqId>,
    /// Write ops whose page just installed write-protected: the vCPU's
    /// retry traps again as a WP fault once the current effect batch is
    /// applied (real uffd's double fault under an unsolicited fill).
    refaults: Vec<(u16, PageId)>,
    report: ClusterReport,
}

#[allow(clippy::too_many_lines)]
pub fn run(seed: u64, mut config: ClusterConfig) -> ClusterReport {
    if config.daemon.replica_placement.is_none() {
        config.daemon.replica_placement = Some(ReplicaPlacementConfig {
            membership_epoch: 1,
            local_failure_domain: 1,
            roster: (0..config.hosts)
                .map(|host| PeerCandidate {
                    host: HostId(host),
                    weight: 1,
                    failure_domain: host + 1,
                    drained: false,
                })
                .collect(),
        });
    }
    let kernel = Kernel::new(seed);
    let store = ObjectStore::new(config.store.clone());
    let mut hosts = Vec::new();
    let mut boot_effects = Vec::new();
    for h in 0..config.hosts {
        let mut daemon_config = config.daemon.clone();
        daemon_config.host = HostId(h);
        if let Some(placement) = daemon_config.replica_placement.as_mut()
            && let Some(candidate) = placement
                .roster
                .iter()
                .find(|candidate| candidate.host == HostId(h))
        {
            placement.local_failure_domain = candidate.failure_domain;
        }
        let (daemon, effects) = Daemon::new(daemon_config);
        hosts.push(HostState {
            daemon: Some(daemon),
            inc: 0,
            bdev: BlobDev::new(config.bdev.clone()),
            mems: BTreeMap::new(),
            shared_base: BTreeMap::new(),
        });
        boot_effects.push(effects);
    }
    let mut c = Cluster {
        config,
        kernel,
        hosts,
        store,
        placement: BTreeMap::new(),
        guests: BTreeMap::new(),
        oracle: Oracle::new(),
        next_req: 0,
        sync_reqs: BTreeMap::new(),
        sync_started: BTreeMap::new(),
        sync_latencies: Vec::new(),
        admin_reqs: BTreeMap::new(),
        refaults: Vec::new(),
        pending_offers: BTreeMap::new(),
        expected_ptr: BTreeMap::new(),
        restore_sent: BTreeMap::new(),
        orphan_started: BTreeMap::new(),
        paused_at: BTreeMap::new(),
        migrated_from: BTreeMap::new(),
        doomed: BTreeSet::new(),
        dead: BTreeSet::new(),
        migrate_reqs: BTreeSet::new(),
        report: ClusterReport::default(),
    };
    for (h, effects) in boot_effects.into_iter().enumerate() {
        c.apply_effects(u16::try_from(h).expect("fits"), effects);
    }

    for n in 1..=c.config.vset_count {
        let vset = VsetId(u64::from(n));
        let host = (n - 1) % c.config.hosts;
        c.placement.insert(vset, host);
        let req = c.req();
        c.admin_reqs.insert(req, vset);
        let config = c.vset_config_for(vset);
        c.step_daemon(
            host,
            Event::Admin(AdminCmd::CreateVset {
                req,
                vset,
                config,
                from_base: None,
            }),
        );
    }
    c.schedule_plan();

    let end = SimTime(c.config.horizon + 2 * millis(1000));
    while let Some((at, event)) = c.kernel.pop() {
        if at > end {
            break;
        }
        c.dispatch(event);
    }

    c.report.trace_hash = c.kernel.trace_hash();
    c.report
        .violations
        .extend(std::mem::take(&mut c.oracle.violations));
    c.report.completed_ops = c.guests.values().map(|g| g.completed).sum();
    c.sync_latencies.sort_unstable();
    c.report.sync_samples = c.sync_latencies.len() as u64;
    let percentile = |pct: usize| {
        if c.sync_latencies.is_empty() {
            0
        } else {
            let index = ((c.sync_latencies.len() - 1) * pct) / 100;
            c.sync_latencies[index]
        }
    };
    c.report.sync_latency_p50_ns = percentile(50);
    c.report.sync_latency_p95_ns = percentile(95);
    c.report.sync_latency_p99_ns = percentile(99);
    c.report.sync_latency_max_ns = c.sync_latencies.last().copied().unwrap_or(0);
    let sum = |read: fn(&blockd_core::daemon::Counters) -> u64| -> u64 {
        c.hosts
            .iter()
            .filter_map(|h| h.daemon.as_ref())
            .map(|d| read(&d.counters))
            .sum()
    };
    c.report.prefetch_fills = sum(|k| k.prefetch_fills);
    c.report.hydrate_fills = sum(|k| k.hydrate_fills);
    c.report.leaf_fills = sum(|k| k.leaf_fills);
    c.report.store_retries = sum(|k| k.store_retries);
    c.report.peer_rejected = sum(|k| k.peer_rejected);
    c.report.wedged_guests = sum(|k| k.wedged_guests);
    c.report.wedged_hydration = sum(|k| k.wedged_hydration);
    c.report.wedged_outbound = sum(|k| k.wedged_outbound);
    let live = || c.hosts.iter().filter_map(|h| h.daemon.as_ref());
    c.report.parked_end = live().map(blockd_core::daemon::Daemon::parked_fills).sum();
    c.report.hydrating_end = live()
        .map(blockd_core::daemon::Daemon::hydrating_vsets)
        .sum();
    c.report.replica_bytes = sum(|k| k.replica_bytes);
    c.report.replica_commits = sum(|k| k.replica_commits);
    c.report.replica_store_bytes = sum(|k| k.replica_store_bytes);
    c.report.replica_unlinks = sum(|k| k.replica_unlinks);
    c.report.replica_network_bytes = sum(|k| k.replica_network_bytes);
    c.report.replica_logical_bytes = sum(|k| k.replica_logical_bytes);
    c.report.replica_nonactive_bytes = sum(|k| k.replica_nonactive_bytes);
    c.report.replica_replacement_bytes = sum(|k| k.replica_replacement_bytes);
    c.report.replica_cleanup_rewrite_bytes = sum(|k| k.replica_cleanup_rewrite_bytes);
    c.report.replica_artifact_flushes = sum(|k| k.replica_artifact_flushes);
    c.report.replica_commit_flushes = sum(|k| k.replica_commit_flushes);
    c.report.replica_rotations = sum(|k| k.replica_rotations);
    c.report.archive_cycles = sum(|k| k.archive_cycles);
    c.report.archive_commits_coalesced = sum(|k| k.archive_commits_coalesced);
    c.report.replica_capacity_backpressure = sum(|k| k.replica_capacity_backpressure);
    let vsets: Vec<_> = c.guests.keys().copied().collect();
    let (physical, archive_live, dead, overhead) = archive_segment_efficiency(&c.store, &vsets);
    c.report.published_segment_bytes = physical;
    c.report.published_live_entry_bytes = archive_live;
    c.report.published_dead_entry_bytes = dead;
    c.report.published_segment_overhead_bytes = overhead;
    c.report.replica_spool_bytes = live()
        .flat_map(blockd_core::daemon::Daemon::replica_spool_metrics)
        .map(|metrics| metrics.stored_bytes)
        .sum();
    c.report.max_archive_lag_age_ns = live()
        .flat_map(blockd_core::daemon::Daemon::replica_spool_metrics)
        .map(|metrics| metrics.unarchived_age_ns)
        .max()
        .unwrap_or(0);
    c.report.peer_committed_through = live()
        .flat_map(blockd_core::daemon::Daemon::replica_metrics)
        .map(|metrics| metrics.peer_committed_through)
        .sum();
    c.report.archived_through = live()
        .flat_map(blockd_core::daemon::Daemon::replica_metrics)
        .map(|metrics| metrics.store_published_through)
        .sum();
    c.report.archive_lag_bytes = live()
        .flat_map(|daemon| daemon.stats().vsets)
        .filter_map(|metrics| metrics.archive_lag_bytes)
        .sum();
    c.report.segs_compacted = sum(|k| k.segs_compacted);
    c.report.pages_compacted = sum(|k| k.pages_compacted);
    c.report.disk_crash_applied = c
        .hosts
        .iter()
        .map(|host| host.bdev.counters.crash_applied)
        .sum();
    c.report.disk_crash_dropped = c
        .hosts
        .iter()
        .map(|host| host.bdev.counters.crash_dropped)
        .sum();
    c.report.disk_crash_torn = c
        .hosts
        .iter()
        .map(|host| host.bdev.counters.crash_torn)
        .sum();
    c.report.disk_bitflips = c.hosts.iter().map(|host| host.bdev.counters.bitflips).sum();
    c.report.store_unavailable = c.store.counters.unavailable;
    c.report.store_cas_conflicts = c.store.counters.cas_conflicts;
    c.report.store = c.store.counters;
    c.report.blobs_per_host = c.hosts.iter().map(|h| h.bdev.blob_count()).collect();
    c.report.primary_blobs_per_host = c
        .hosts
        .iter()
        .map(|host| {
            host.bdev
                .scan()
                .filter(|(name, _)| {
                    !matches!(
                        layout::parse_blob(name),
                        Some(layout::BlobName::ReplicaSpool { .. })
                    )
                })
                .count()
        })
        .collect();
    c.report
}

impl Cluster {
    fn fault_enabled(&self, point: FaultPoint) -> bool {
        self.config.fault_points.contains(&point)
    }

    fn hit_fault(&mut self, point: FaultPoint) {
        if self.fault_enabled(point) {
            *self.report.fault_coverage.entry(point).or_default() += 1;
        }
    }

    fn fault_pending(&self, point: FaultPoint) -> bool {
        self.fault_enabled(point)
            && self.report.fault_coverage.get(&point).copied().unwrap_or(0) == 0
    }

    fn req(&mut self) -> ReqId {
        let req = ReqId(self.next_req);
        self.next_req += 1;
        req
    }

    /// Schedule the configured fault plan: kills, crashes, the one-shot
    /// migration, the store outage, resume-set rot, and the nemeses.
    fn schedule_plan(&mut self) {
        for &(at, host) in &self.config.kill_hosts_at {
            self.kernel.schedule_at(SimTime(at), Ev::KillHost(host));
        }
        for &(at, host) in &self.config.crash_hosts_at {
            self.kernel.schedule_at(SimTime(at), Ev::CrashHost(host));
        }
        if let Some((at, vset, to)) = self.config.migrate_at {
            self.kernel
                .schedule_at(SimTime(at), Ev::MigrateAt { vset, to });
        }
        if let Some((begin, end)) = self.config.store_outage {
            self.kernel
                .schedule_at(SimTime(begin), Ev::StoreOutage(true));
            self.kernel
                .schedule_at(SimTime(end), Ev::StoreOutage(false));
        }
        if let Some(at) = self.config.rot_resume_set_at {
            self.kernel.schedule_at(SimTime(at), Ev::RotResumeSets);
        }
        if let Some(at) = self.config.rot_leaves_at {
            self.kernel.schedule_at(SimTime(at), Ev::RotLeaves);
        }
        // First nemesis fire waits a full mean interval: vset creation
        // must finish before hosts start dying under it.
        if self.config.crash_mean_interval > 0 {
            let interval = self.config.crash_mean_interval;
            let at = self.kernel.rng().range(interval, 2 * interval);
            self.kernel.schedule_after(at, Ev::CrashNemesis);
        }
        if self.config.migrate_mean_interval > 0 {
            let interval = self.config.migrate_mean_interval;
            let at = self.kernel.rng().range(interval, 2 * interval);
            self.kernel.schedule_after(at, Ev::MigrateNemesis);
        }
    }

    fn vset_config_for(&self, vset: VsetId) -> VsetConfig {
        let _ = vset;
        self.config.vset_config
    }

    fn step_daemon(&mut self, host: u16, event: Event) {
        let state = &mut self.hosts[usize::from(host)];
        let Some(daemon) = &mut state.daemon else {
            return;
        };
        let effects = daemon.step(
            event,
            &MemView {
                host,
                now: self.kernel.now(),
                mems: &state.mems,
            },
        );
        self.apply_effects(host, effects);
        // Retried writes trap again after the batch (see `refaults`).
        while let Some((host, page)) = self.refaults.pop() {
            self.step_daemon(host, Event::GuestFault { page, write: true });
        }
    }

    #[allow(clippy::too_many_lines)]
    fn apply_effects(&mut self, host: u16, effects: Vec<Effect>) {
        for effect in effects {
            self.kernel.observe(&(host, &effect));
            let inc = self.hosts[usize::from(host)].inc;
            match effect {
                Effect::Fill {
                    page,
                    bytes,
                    writable,
                    share,
                } => {
                    if let Some(key) = share {
                        self.hosts[usize::from(host)]
                            .shared_base
                            .insert(key, bytes.clone());
                    }
                    self.fill(host, page, bytes, writable);
                }
                Effect::FillShared {
                    page,
                    share,
                    writable,
                } => {
                    let bytes = self.hosts[usize::from(host)].shared_base[&share].clone();
                    self.fill(host, page, bytes, writable);
                }
                Effect::FillFailed { page } => self.fill_failed(page),
                Effect::Unprotect { page } => {
                    let mems = &mut self.hosts[usize::from(host)].mems;
                    if let Some(mem) = mems.get_mut(&page.volume.vset) {
                        mem.protected.remove(&page);
                    }
                    self.resolve_write(host, page);
                }
                Effect::WriteProtect { pages } => {
                    let mems = &mut self.hosts[usize::from(host)].mems;
                    for page in pages {
                        if let Some(mem) = mems.get_mut(&page.volume.vset) {
                            mem.protected.insert(page);
                        }
                    }
                }
                Effect::Evict { page } => {
                    let mems = &mut self.hosts[usize::from(host)].mems;
                    if let Some(mem) = mems.get_mut(&page.volume.vset) {
                        mem.pages.remove(&page);
                        mem.protected.remove(&page);
                    }
                }
                Effect::DatabaseInstall { page, bytes } => {
                    let mems = &mut self.hosts[usize::from(host)].mems;
                    let mem = mems.entry(page.volume.vset).or_default();
                    mem.pages.insert(page, bytes);
                    mem.protected.insert(page);
                }
                Effect::Database(_) => {}
                Effect::PauseGuest { vset } => {
                    self.paused_at.insert(vset, self.kernel.now());
                    let guest = self.guests.get_mut(&vset).expect("guest exists");
                    guest.paused = true;
                    let vmstate = guest.applied;
                    let delay = self.kernel.rng().range(micros(20), micros(200));
                    self.kernel.schedule_after(
                        delay,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::GuestPaused { vset, vmstate },
                        },
                    );
                }
                Effect::ResumeGuest { vset } => {
                    let guest = self.guests.get_mut(&vset).expect("guest exists");
                    guest.paused = false;
                    self.unpark(host, vset);
                }
                Effect::SyncOk { req } => {
                    if self.fault_pending(FaultPoint::CrashPrimaryAfterAckBeforeSyncOk) {
                        self.hit_fault(FaultPoint::CrashPrimaryAfterAckBeforeSyncOk);
                        self.crash_host(host);
                        break;
                    }
                    self.sync_done(req, true);
                    if self.fault_pending(FaultPoint::CrashPrimaryAfterSyncOk) {
                        self.hit_fault(FaultPoint::CrashPrimaryAfterSyncOk);
                        self.crash_host(host);
                        break;
                    }
                }
                Effect::SyncFailed { req } => self.sync_done(req, false),
                Effect::BlobWrite { io, name, bytes } => {
                    if self.fault_pending(FaultPoint::CrashPrimaryBeforeClosureCapture)
                        && !self.sync_reqs.is_empty()
                    {
                        self.hit_fault(FaultPoint::CrashPrimaryBeforeClosureCapture);
                        self.crash_host(host);
                        break;
                    }
                    if std::env::var_os("BLOCKD_SIM_DEBUG").is_some() && name.ends_with("/handoff")
                    {
                        eprintln!(
                            "[{:>12}] host {host}: handoff marker write submitted",
                            self.kernel.now().nanos()
                        );
                    }
                    if self.config.sabotage == Some(Sabotage::EagerHandoffAck)
                        && name.ends_with("/handoff")
                    {
                        // SABOTAGE: acknowledge the handoff marker without
                        // persisting a byte — the source acts on a
                        // durability it does not have.
                        let delay = self.kernel.rng().range(micros(20), micros(100));
                        self.kernel.schedule_after(
                            delay,
                            Ev::Daemon {
                                host,
                                inc,
                                event: Event::BlobWriteDone { io },
                            },
                        );
                        continue;
                    }
                    let now = self.kernel.now();
                    let state = &mut self.hosts[usize::from(host)];
                    let (bdev_io, at) =
                        state.bdev.submit_write(now, self.kernel.rng(), name, bytes);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevWriteDone {
                            host,
                            inc,
                            bdev_io,
                            io,
                        },
                    );
                }
                Effect::ReplicaAppend {
                    io,
                    source,
                    vset,
                    assignment_epoch,
                    generation,
                    bytes,
                } => {
                    let now = self.kernel.now();
                    let name = layout::replica_spool_segment_blob(
                        source,
                        vset,
                        assignment_epoch,
                        generation,
                    );
                    let state = &mut self.hosts[usize::from(host)];
                    let (bdev_io, at) =
                        state
                            .bdev
                            .submit_append(now, self.kernel.rng(), name, bytes);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevWriteDone {
                            host,
                            inc,
                            bdev_io,
                            io,
                        },
                    );
                }
                Effect::ReplicaDelete {
                    io,
                    source,
                    vset,
                    assignment_epoch,
                    through_generation,
                } => {
                    for generation in 0..=through_generation {
                        self.hosts[usize::from(host)].bdev.delete(
                            &layout::replica_spool_segment_blob(
                                source,
                                vset,
                                assignment_epoch,
                                generation,
                            ),
                        );
                    }
                    self.kernel.schedule_at(
                        self.kernel.now(),
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::BlobWriteDone { io },
                        },
                    );
                }
                Effect::ReplicaTruncate {
                    io,
                    source,
                    vset,
                    assignment_epoch,
                    generation,
                    len,
                } => {
                    self.hosts[usize::from(host)].bdev.truncate(
                        &layout::replica_spool_segment_blob(
                            source,
                            vset,
                            assignment_epoch,
                            generation,
                        ),
                        usize::try_from(len).expect("fits"),
                    );
                    self.kernel.schedule_at(
                        self.kernel.now(),
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::BlobWriteDone { io },
                        },
                    );
                }
                Effect::BlobRead { io, name } => {
                    let now = self.kernel.now();
                    let state = &mut self.hosts[usize::from(host)];
                    let (at, bytes) = state.bdev.read(now, self.kernel.rng(), &name);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevReadDone {
                            host,
                            inc,
                            io,
                            bytes,
                        },
                    );
                }
                Effect::BlobReadRange {
                    io,
                    name,
                    offset,
                    len,
                } => {
                    let now = self.kernel.now();
                    let state = &mut self.hosts[usize::from(host)];
                    let (at, bytes) =
                        state
                            .bdev
                            .read_range(now, self.kernel.rng(), &name, offset, len);
                    self.kernel.schedule_at(
                        at,
                        Ev::BdevReadDone {
                            host,
                            inc,
                            io,
                            bytes,
                        },
                    );
                }
                Effect::BlobDelete { name } => {
                    self.hosts[usize::from(host)].bdev.delete(&name);
                }
                Effect::SetTimer { timer, after } => {
                    // Zero-delay = "continue once the loop is free"; model
                    // the emitting step's real duration (see the single-
                    // host harness) so other events can interleave.
                    let after = if after == 0 {
                        self.kernel.rng().range(micros(20), micros(200))
                    } else {
                        after
                    };
                    if matches!(timer, blockd_core::seam::TimerId::Replica { .. }) {
                        self.hit_fault(FaultPoint::ReplicaRetryTimer);
                    }
                    self.kernel.schedule_after(
                        after,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::Timer(timer),
                        },
                    );
                }
                Effect::StorePut { io, key, bytes } => {
                    if self.fault_pending(FaultPoint::CrashPeerDuringUpload)
                        && !matches!(layout::parse_key(&key), Some(layout::StoreKey::Head { .. }))
                    {
                        self.hit_fault(FaultPoint::CrashPeerDuringUpload);
                        self.crash_host(host);
                        break;
                    }
                    let now = self.kernel.now();
                    let (at, result) = self.store.put(now, self.kernel.rng(), &key, bytes);
                    let inject_unknown = self.fault_enabled(FaultPoint::StoreUnknownResult)
                        && self
                            .report
                            .fault_coverage
                            .get(&FaultPoint::StoreUnknownResult)
                            .copied()
                            .unwrap_or(0)
                            == 0
                        && result.is_ok();
                    let result = if inject_unknown {
                        self.hit_fault(FaultPoint::StoreUnknownResult);
                        Err(blockd_core::seam::StoreFault::Unavailable)
                    } else {
                        result.map(|v| v.0).map_err(store_fault)
                    };
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StorePutDone { io, result },
                        },
                    );
                }
                Effect::StoreCas {
                    io,
                    key,
                    expected,
                    bytes,
                } => {
                    let now = self.kernel.now();
                    let proposed_head = layout::parse_key(&key)
                        .and_then(|parsed| match parsed {
                            layout::StoreKey::Head { vset } => Some(vset),
                            _ => None,
                        })
                        .and_then(|vset| HeadRecord::decode(vset, &bytes).ok());
                    let proposed_stash = proposed_head.as_ref().and_then(|head| head.stash);
                    let crash_before_transition = self
                        .fault_pending(FaultPoint::CrashPrimaryBeforeTransitionCas)
                        && proposed_stash.is_some_and(|stash| {
                            stash.assignment_epoch > 1 && stash.transition_peer.is_some()
                        });
                    let crash_before_active = self
                        .fault_pending(FaultPoint::CrashPrimaryAfterSeedBeforeActiveCas)
                        && proposed_stash.is_some_and(|stash| {
                            stash.assignment_epoch > 1 && stash.transition_peer.is_none()
                        });
                    if crash_before_transition || crash_before_active {
                        let point = if crash_before_transition {
                            FaultPoint::CrashPrimaryBeforeTransitionCas
                        } else {
                            FaultPoint::CrashPrimaryAfterSeedBeforeActiveCas
                        };
                        self.hit_fault(point);
                        self.crash_host(host);
                        break;
                    }
                    let assignment_race = self.fault_enabled(FaultPoint::AssignmentCasRace)
                        && !self.store.is_out()
                        && self
                            .report
                            .fault_coverage
                            .get(&FaultPoint::AssignmentCasRace)
                            .copied()
                            .unwrap_or(0)
                            == 0
                        && HeadRecord::decode(
                            layout::parse_key(&key)
                                .and_then(|parsed| match parsed {
                                    layout::StoreKey::Head { vset } => Some(vset),
                                    _ => None,
                                })
                                .unwrap_or(VsetId(0)),
                            &bytes,
                        )
                        .ok()
                        .and_then(|head| head.stash)
                        .is_some_and(|stash| stash.transition_peer.is_some());
                    if assignment_race {
                        let _ = self.store.put_cas(
                            now,
                            self.kernel.rng(),
                            &key,
                            expected.map(Version),
                            bytes.clone(),
                        );
                        self.hit_fault(FaultPoint::AssignmentCasRace);
                    }
                    let (at, result) = self.store.put_cas(
                        now,
                        self.kernel.rng(),
                        &key,
                        expected.map(Version),
                        bytes,
                    );
                    let result = result.map(|v| v.0).map_err(store_fault);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StorePutDone { io, result },
                        },
                    );
                }
                Effect::StoreGet { io, key } => {
                    let now = self.kernel.now();
                    let (at, result) = self.store.get(now, self.kernel.rng(), &key);
                    let result = result
                        .map(|found| found.map(|(v, b)| (v.0, b)))
                        .map_err(store_fault);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StoreGetDone { io, result },
                        },
                    );
                }
                Effect::StoreGetRange {
                    io,
                    key,
                    offset,
                    len,
                } => {
                    let now = self.kernel.now();
                    let (at, result) =
                        self.store
                            .get_range(now, self.kernel.rng(), &key, offset, len);
                    let result = result
                        .map(|found| found.map(|(v, b)| (v.0, b)))
                        .map_err(store_fault);
                    self.kernel.schedule_at(
                        at,
                        Ev::Daemon {
                            host,
                            inc,
                            event: Event::StoreGetDone { io, result },
                        },
                    );
                }
                Effect::StoreDelete { key } => {
                    let now = self.kernel.now();
                    let _ = self.store.delete(now, self.kernel.rng(), &key);
                }
                Effect::VsetFenced { vset } => {
                    if self.placement.get(&vset) == Some(&host)
                        && let Some(guest) = self.guests.get_mut(&vset)
                    {
                        guest.state = GuestState::Dead;
                    }
                }
                Effect::VsetUnservable { page } => {
                    if let Some(guest) = self.guests.get_mut(&page.volume.vset) {
                        guest.state = GuestState::Dead;
                    }
                    self.report.guest_deaths += 1;
                }
                Effect::Admin(reply) => self.admin_reply(host, reply),
                Effect::PeerSend { to, msg } => {
                    // The wedge nemesis: a total, targeted outage of one
                    // message kind. Deterministic (no RNG draw), so runs
                    // without it replay byte-identically.
                    if let Some((kind, start, stop)) = self.config.drop_peer {
                        let now = self.kernel.now().nanos();
                        if kind == PeerKind::of(&msg) && (start..stop).contains(&now) {
                            self.report.nemesis_drops += 1;
                            continue;
                        }
                    }
                    let transfer_crash = if self
                        .fault_pending(FaultPoint::CrashPrimaryAfterClosureCapture)
                        && matches!(msg, blockd_core::seam::PeerMsg::ReplicaStatus { .. })
                    {
                        Some(FaultPoint::CrashPrimaryAfterClosureCapture)
                    } else if self.fault_pending(FaultPoint::CrashPrimaryDuringArtifactTransfer)
                        && matches!(msg, blockd_core::seam::PeerMsg::ReplicaPut { .. })
                    {
                        Some(FaultPoint::CrashPrimaryDuringArtifactTransfer)
                    } else if self.fault_pending(FaultPoint::CrashPeerAfterDataFlushBeforeCommit)
                        && matches!(msg, blockd_core::seam::PeerMsg::ReplicaPutAck { .. })
                    {
                        Some(FaultPoint::CrashPeerAfterDataFlushBeforeCommit)
                    } else {
                        None
                    };
                    if let Some(point) = transfer_crash {
                        self.hit_fault(point);
                        self.crash_host(host);
                        break;
                    }
                    if self.fault_pending(FaultPoint::CrashPeerAfterCommitBeforeAck)
                        && matches!(msg, blockd_core::seam::PeerMsg::ReplicaCommitAck { .. })
                    {
                        self.hit_fault(FaultPoint::CrashPeerAfterCommitBeforeAck);
                        self.crash_host(host);
                        break;
                    }
                    // The cluster network: peers reach each other with a
                    // small latency; a dead destination just never answers
                    // (handled at delivery). Loss and duplication draw
                    // from the RNG only when configured, so reliable
                    // configs replay byte-identically.
                    let now = self.kernel.now().nanos();
                    if self
                        .config
                        .peer_link_outages
                        .iter()
                        .any(|&(begin, end, from, target)| {
                            from == host && target == to.0 && begin <= now && now < end
                        })
                    {
                        self.report.peer_link_clogs += 1;
                        continue;
                    }
                    let (drop_n, drop_d) = self.config.peer_drop;
                    if drop_n > 0 && self.kernel.rng().below(drop_d) < drop_n {
                        self.report.peer_drops += 1;
                        continue;
                    }
                    let delay = self.kernel.rng().range(micros(50), micros(500));
                    self.kernel.schedule_after(
                        delay,
                        Ev::PeerDeliver {
                            from: host,
                            to: to.0,
                            msg: msg.clone(),
                        },
                    );
                    if self.fault_pending(FaultPoint::CrashPeerAfterUploadBeforeHead)
                        && matches!(msg, blockd_core::seam::PeerMsg::ReplicaUploadDone { .. })
                    {
                        self.hit_fault(FaultPoint::CrashPeerAfterUploadBeforeHead);
                        self.crash_host(host);
                        break;
                    }
                    if self.fault_pending(FaultPoint::CrashPrimaryAfterHeadBeforeRelease)
                        && matches!(msg, blockd_core::seam::PeerMsg::ReplicaRelease { .. })
                    {
                        self.hit_fault(FaultPoint::CrashPrimaryAfterHeadBeforeRelease);
                        self.crash_host(host);
                        break;
                    }
                    if self.fault_pending(FaultPoint::CrashPrimaryAfterActiveCasBeforeCommit)
                        && matches!(msg, blockd_core::seam::PeerMsg::ReplicaStatus { .. })
                        && self
                            .store
                            .peek(&layout::head_key(VsetId(1)))
                            .and_then(|bytes| HeadRecord::decode(VsetId(1), bytes).ok())
                            .and_then(|head| head.stash)
                            .is_some_and(|stash| {
                                stash.assignment_epoch > 1
                                    && stash.transition_peer.is_none()
                                    && stash.active_peer == to
                            })
                    {
                        self.hit_fault(FaultPoint::CrashPrimaryAfterActiveCasBeforeCommit);
                        self.crash_host(host);
                        break;
                    }
                    if matches!(msg, blockd_core::seam::PeerMsg::ReplicaStatusReply { .. }) {
                        self.hit_fault(FaultPoint::StatusReconciliation);
                    }
                    let forced_duplicate = match msg {
                        blockd_core::seam::PeerMsg::ReplicaPutAck { .. }
                        | blockd_core::seam::PeerMsg::ReplicaCommitAck { .. }
                        | blockd_core::seam::PeerMsg::ReplicaReleaseAck { .. } => {
                            Some(FaultPoint::DuplicateAck)
                        }
                        blockd_core::seam::PeerMsg::ReplicaRelease { .. } => {
                            Some(FaultPoint::ReleaseOverlap)
                        }
                        _ => None,
                    }
                    .filter(|point| self.fault_enabled(*point));
                    if let Some(point) = forced_duplicate {
                        self.hit_fault(point);
                        let delay = self.kernel.rng().range(micros(50), micros(500));
                        self.kernel.schedule_after(
                            delay,
                            Ev::PeerDeliver {
                                from: host,
                                to: to.0,
                                msg: msg.clone(),
                            },
                        );
                    }
                    let (dup_n, dup_d) = self.config.peer_dup;
                    if dup_n > 0 && self.kernel.rng().below(dup_d) < dup_n {
                        self.report.peer_dups += 1;
                        let delay = self.kernel.rng().range(micros(50), micros(500));
                        self.kernel.schedule_after(
                            delay,
                            Ev::PeerDeliver {
                                from: host,
                                to: to.0,
                                msg,
                            },
                        );
                    }
                }
                Effect::Abort { reason } => {
                    self.report
                        .violations
                        .push(format!("daemon {host} aborted: {reason}"));
                }
            }
        }
    }

    fn dispatch(&mut self, event: Ev) {
        match event {
            Ev::Daemon { host, inc, event } => {
                if self.hosts[usize::from(host)].inc == inc {
                    self.step_daemon(host, event);
                }
            }
            Ev::BdevWriteDone {
                host,
                inc,
                bdev_io,
                io,
            } => {
                if self.hosts[usize::from(host)].inc == inc {
                    self.hosts[usize::from(host)].bdev.complete_write(bdev_io);
                    self.step_daemon(host, Event::BlobWriteDone { io });
                }
            }
            Ev::BdevReadDone {
                host,
                inc,
                io,
                bytes,
            } => {
                if self.hosts[usize::from(host)].inc == inc {
                    self.step_daemon(host, Event::BlobReadDone { io, bytes });
                }
            }
            Ev::GuestStep { vset } => self.guest_step(vset),
            Ev::CheckpointTick { vset } => {
                let host = self.placement[&vset];
                if self.hosts[usize::from(host)].daemon.is_some() {
                    let req = self.req();
                    self.admin_reqs.insert(req, vset);
                    self.step_daemon(host, Event::Admin(AdminCmd::Checkpoint { req, vset }));
                }
                if let Some(interval) = self.config.checkpoint_interval
                    && self.kernel.now().nanos() <= self.config.horizon
                {
                    let delay = self.kernel.rng().range(1, 2 * interval);
                    self.kernel
                        .schedule_after(delay, Ev::CheckpointTick { vset });
                }
            }
            Ev::KillHost(host) => self.kill_host(host),
            Ev::CrashHost(host) => self.crash_host(host),
            Ev::RestartHost(host) => self.restart_host(host),
            Ev::PromoteOrphan {
                source,
                vset,
                claimants,
            } => self.promote_orphan(source, vset, claimants),
            Ev::StoreOutage(out) => self.store.set_outage(out),
            Ev::RotResumeSets => self.rot_resume_sets(),
            Ev::RotLeaves => self.rot_leaves(),
            Ev::CrashNemesis => {
                self.random_crash();
                if self.kernel.now().nanos() <= self.config.horizon {
                    let at = self
                        .kernel
                        .rng()
                        .range(1, 2 * self.config.crash_mean_interval);
                    self.kernel.schedule_after(at, Ev::CrashNemesis);
                }
            }
            Ev::MigrateNemesis => {
                self.random_migration();
                if self.kernel.now().nanos() <= self.config.horizon {
                    let at = self
                        .kernel
                        .rng()
                        .range(1, 2 * self.config.migrate_mean_interval);
                    self.kernel.schedule_after(at, Ev::MigrateNemesis);
                }
            }
            Ev::MigrateAt { vset, to } => {
                let host = self.placement[&vset];
                if self.hosts[usize::from(host)].daemon.is_some() {
                    let req = self.req();
                    self.admin_reqs.insert(req, vset);
                    self.migrate_reqs.insert(req);
                    self.step_daemon(
                        host,
                        Event::Admin(AdminCmd::MigrateOut {
                            req,
                            vset,
                            to: HostId(to),
                        }),
                    );
                }
            }
            Ev::PeerDeliver { from, to, msg } => self.peer_deliver(from, to, msg),
        }
    }

    fn peer_deliver(&mut self, from: u16, to: u16, msg: blockd_core::seam::PeerMsg) {
        if std::env::var_os("BLOCKD_SIM_DEBUG").is_some() {
            let mut text = format!("{msg:?}");
            text.truncate(110);
            eprintln!(
                "[{:>12}] peer {from} -> {to}: {text}",
                self.kernel.now().nanos()
            );
        }
        if let blockd_core::seam::PeerMsg::MigrateOffer { vset, .. } = &msg {
            self.pending_offers.insert(*vset, (from, to));
        }
        if self.hosts[usize::from(to)].daemon.is_some() {
            if let blockd_core::seam::PeerMsg::Released { vset } = msg
                && self.migrated_from.remove(&vset).is_some()
            {
                // The tail is drained: the vset no longer depends on its
                // source (its crash costs nothing now).
                self.report.releases += 1;
            }
            self.step_daemon(
                to,
                Event::PeerDelivered {
                    from: HostId(from),
                    msg,
                },
            );
        } else if self.dead.contains(&to)
            && let blockd_core::seam::PeerMsg::FetchRange { io, .. } = msg
        {
            // A dead source answers nothing; the harness surfaces the
            // silence as an explicit miss so the R7.3 failure is loud, not
            // a hang. Crashed-but-restarting hosts stay silent instead:
            // the sender's retries bridge the downtime.
            let delay = self.kernel.rng().range(micros(50), micros(500));
            self.kernel.schedule_after(
                delay,
                Ev::PeerDeliver {
                    from: to,
                    to: from,
                    msg: blockd_core::seam::PeerMsg::Page { io, bytes: None },
                },
            );
        }
    }

    /// Injected store damage: flip a bit in every leaf object; the
    /// losses it causes are sanctioned for the vsets whose leaves rot.
    fn rot_leaves(&mut self) {
        let keys: Vec<String> = self
            .store
            .snapshot()
            .into_iter()
            .map(|(k, _, _)| k)
            .filter(|k| {
                matches!(
                    blockd_core::layout::parse_key(k),
                    Some(blockd_core::layout::StoreKey::Leaf { .. })
                )
            })
            .collect();
        for key in keys {
            if let Some(flipped) = self
                .store
                .flip_random_bit_where(self.kernel.rng(), |k| k == key)
                && let Some(blockd_core::layout::StoreKey::Leaf { vset, .. }) =
                    blockd_core::layout::parse_key(&flipped)
            {
                self.doomed.insert(vset);
            }
        }
    }

    fn rot_resume_sets(&mut self) {
        let keys: Vec<String> = self
            .store
            .snapshot()
            .into_iter()
            .map(|(k, _, _)| k)
            .filter(|k| k.ends_with("/rs"))
            .collect();
        for key in keys {
            self.store
                .flip_random_bit_where(self.kernel.rng(), |k| k == key);
        }
    }

    /// Nemesis: crash a random live host.
    fn random_crash(&mut self) {
        let alive: Vec<u16> = (0..self.config.hosts)
            .filter(|&h| self.hosts[usize::from(h)].daemon.is_some())
            .collect();
        if !alive.is_empty() {
            let host = *self.kernel.rng().pick(&alive);
            self.crash_host(host);
        }
    }

    /// Nemesis: migrate a random vset to a live peer.
    fn random_migration(&mut self) {
        let candidates: Vec<(VsetId, u16)> = self
            .placement
            .iter()
            .filter(|&(&vset, &host)| {
                self.hosts[usize::from(host)].daemon.is_some()
                    && !self.migrated_from.contains_key(&vset)
                    && !self.doomed.contains(&vset)
            })
            .map(|(&vset, &host)| (vset, host))
            .collect();
        if candidates.is_empty() {
            return;
        }
        let (vset, src) = *self.kernel.rng().pick(&candidates);
        let dests: Vec<u16> = (0..self.config.hosts)
            .filter(|&h| {
                h != src && !self.dead.contains(&h) && self.hosts[usize::from(h)].daemon.is_some()
            })
            .collect();
        if dests.is_empty() {
            return;
        }
        let to = *self.kernel.rng().pick(&dests);
        let req = self.req();
        self.admin_reqs.insert(req, vset);
        self.migrate_reqs.insert(req);
        self.step_daemon(
            src,
            Event::Admin(AdminCmd::MigrateOut {
                req,
                vset,
                to: HostId(to),
            }),
        );
    }

    /// Permanent host death (R6.1's premise): volatile state and guests are
    /// gone; the control plane restores each backed-up orphan elsewhere —
    /// racing two claimants when configured.
    fn kill_host(&mut self, host: u16) {
        self.dead.insert(host);
        let state = &mut self.hosts[usize::from(host)];
        if state.daemon.take().is_none() {
            return;
        }
        state.inc += 1;
        state.mems.clear();
        state.shared_base.clear();
        // Migrated-in vsets whose source this was retain the final handoff
        // closure on the destination's durable passive spool (R7.3).
        let orphans: Vec<VsetId> = self
            .placement
            .iter()
            .filter(|&(_, h)| *h == host)
            .map(|(v, _)| *v)
            .collect();
        for vset in orphans {
            self.orphan_started.insert(vset, self.kernel.now());
            if let Some(guest) = self.guests.get_mut(&vset) {
                guest.state = GuestState::Dead;
            }
            let claimants: Vec<u16> = (1..self.config.hosts)
                .map(|offset| (host + offset) % self.config.hosts)
                .filter(|candidate| {
                    !self.dead.contains(candidate)
                        && self.hosts[usize::from(*candidate)].daemon.is_some()
                })
                .take(if self.config.race_restore { 2 } else { 1 })
                .collect();
            if claimants.is_empty() {
                self.report
                    .violations
                    .push(format!("orphan {vset:?} has no live restore claimant"));
                continue;
            }
            self.kernel.schedule_at(
                self.kernel.now(),
                Ev::PromoteOrphan {
                    source: host,
                    vset,
                    claimants,
                },
            );
        }
    }

    /// Inventory the fenced passive, publish its exact committed closure,
    /// retire the dead assignment, then let ordinary restore race onto a
    /// freshly placed passive. Partial object puts are harmless; the head CAS
    /// is the single publication point, so an outage simply retries.
    #[allow(clippy::too_many_lines)]
    fn promote_orphan(&mut self, source: u16, vset: VsetId, claimants: Vec<u16>) {
        let key = layout::head_key(vset);
        let Some((observed_version, head_bytes)) = self.store.peek_versioned(&key) else {
            self.kernel.schedule_after(
                self.config.daemon.backup_retry,
                Ev::PromoteOrphan {
                    source,
                    vset,
                    claimants,
                },
            );
            return;
        };
        let Ok(head) = HeadRecord::decode(vset, head_bytes) else {
            self.report
                .violations
                .push(format!("corrupt orphan head for {vset:?}"));
            return;
        };
        if head.holder != HostId(source) {
            return;
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
        let mut owned: Vec<(HostId, u64, Vec<u8>)> = Vec::new();
        for peer in 0..self.config.hosts {
            if self.dead.contains(&peer) {
                continue;
            }
            let peer_id = HostId(peer);
            let mut generations: BTreeMap<u64, BTreeMap<u64, Vec<u8>>> = BTreeMap::new();
            for (name, bytes) in self.hosts[usize::from(peer)].bdev.scan() {
                if let Some(layout::BlobName::ReplicaSpool {
                    source: got_source,
                    vset: got_vset,
                    assignment_epoch,
                    generation,
                }) = layout::parse_blob(name)
                    && (got_source, got_vset) == (HostId(source), vset)
                    && (allowed.contains(&(peer_id, assignment_epoch))
                        || head
                            .stash
                            .is_some_and(|stash| assignment_epoch > stash.assignment_epoch))
                {
                    generations
                        .entry(assignment_epoch)
                        .or_default()
                        .insert(generation, bytes.clone());
                }
            }
            for (assignment_epoch, generations) in generations {
                let bytes: Vec<u8> = generations.into_values().flatten().collect();
                owned.push((peer_id, assignment_epoch, bytes));
            }
        }
        let residues: Vec<_> = owned
            .iter()
            .map(|(peer, assignment_epoch, bytes)| ReplicaResidue {
                peer: *peer,
                assignment_epoch: *assignment_epoch,
                bytes,
            })
            .collect();
        let store_objects: BTreeMap<_, _> = self
            .store
            .snapshot()
            .into_iter()
            .map(|(key, _, bytes)| (key, bytes))
            .collect();
        let export = if residues.is_empty() {
            None
        } else {
            match export_replica_recovery(
                HostId(source),
                vset,
                observed_version.0,
                &head,
                &residues,
                &store_objects,
            ) {
                Ok(export) => Some(export),
                Err(error) => {
                    self.report
                        .violations
                        .push(format!("passive promotion failed for {vset:?}: {error:?}"));
                    return;
                }
            }
        };
        if export.is_none() && head.manifest.is_none() {
            self.report
                .violations
                .push(format!("orphan {vset:?} has neither passive nor archive"));
            return;
        }

        let new_fence = observed_version.0.saturating_add(1);
        let mut manifest = head.manifest;
        if let Some(export) = export {
            let Ok(export) = refence_replica_export(vset, &export, new_fence) else {
                self.report
                    .violations
                    .push(format!("passive refence failed for {vset:?}"));
                return;
            };
            let record = export
                .blobs
                .iter()
                .find_map(|(name, bytes)| {
                    matches!(
                        layout::parse_blob(name),
                        Some(layout::BlobName::Journal { fence, .. }) if fence == new_fence
                    )
                    .then_some(bytes)
                })
                .and_then(|bytes| blockd_core::journal::JournalRecord::decode(vset, bytes).ok())
                .expect("verified export has its refenced record");
            for (name, bytes) in &export.blobs {
                let object_key = match layout::parse_blob(name) {
                    Some(layout::BlobName::Segment { fence, seg, .. }) => {
                        Some(layout::segment_key(vset, fence, seg))
                    }
                    Some(layout::BlobName::Leaf { fence, id, .. }) => {
                        Some(layout::leaf_key(vset, fence, id))
                    }
                    _ => None,
                };
                if let Some(object_key) = object_key {
                    let (_, result) = self.store.put(
                        self.kernel.now(),
                        self.kernel.rng(),
                        &object_key,
                        bytes.clone(),
                    );
                    if result.is_err() {
                        self.kernel.schedule_after(
                            self.config.daemon.backup_retry,
                            Ev::PromoteOrphan {
                                source,
                                vset,
                                claimants,
                            },
                        );
                        return;
                    }
                }
            }
            let record_bytes = record.encode(vset);
            let (_, result) = self.store.put(
                self.kernel.now(),
                self.kernel.rng(),
                &layout::manifest_key(vset, new_fence, record.seq),
                record_bytes,
            );
            if result.is_err() {
                self.kernel.schedule_after(
                    self.config.daemon.backup_retry,
                    Ev::PromoteOrphan {
                        source,
                        vset,
                        claimants,
                    },
                );
                return;
            }
            manifest = Some(ManifestPtr {
                fence: new_fence,
                seq: record.seq,
                capture_seq: record.capture_seq,
            });
        }
        let promoted = HeadRecord {
            vset,
            holder: HostId(source),
            fence: new_fence,
            manifest,
            stash: None,
            retired_stashes: Vec::new(),
        };
        let (_, result) = self.store.put_cas(
            self.kernel.now(),
            self.kernel.rng(),
            &key,
            Some(observed_version),
            promoted.encode(),
        );
        if result.is_err() {
            self.kernel.schedule_after(
                self.config.daemon.backup_retry,
                Ev::PromoteOrphan {
                    source,
                    vset,
                    claimants,
                },
            );
            return;
        }
        self.expected_ptr.insert(vset, manifest);
        let started = self
            .orphan_started
            .remove(&vset)
            .unwrap_or_else(|| self.kernel.now());
        for claimant in claimants {
            let req = self.req();
            self.admin_reqs.insert(req, vset);
            self.restore_sent.insert(req, started);
            self.step_daemon(claimant, Event::Admin(AdminCmd::RestoreVset { req, vset }));
        }
    }

    /// Transient host crash (R8.2's premise): volatile state and guests
    /// die, in-flight blob writes tear — but the disk survives and the
    /// host restarts shortly.
    fn crash_host(&mut self, host: u16) {
        let state = &mut self.hosts[usize::from(host)];
        if state.daemon.take().is_none() {
            return;
        }
        self.report.host_crashes += 1;
        state.inc += 1;
        state.bdev.crash(self.kernel.rng());
        state.mems.clear();
        state.shared_base.clear();
        // A destination crashing mid-drain recovers the protected closure
        // from its durable passive spool along with ordinary local state.
        let (lo, hi) = self.config.restart_delay;
        let delay = self.kernel.rng().range(lo, hi);
        self.kernel.schedule_after(delay, Ev::RestartHost(host));
    }

    /// Restart a crashed host: recover the daemon from its surviving disk,
    /// exactly as the single-host harness does.
    fn restart_host(&mut self, host: u16) {
        if self.dead.contains(&host) || self.hosts[usize::from(host)].daemon.is_some() {
            return;
        }
        self.hit_fault(FaultPoint::RestartScan);
        let scan: Vec<(String, Vec<u8>)> = self.hosts[usize::from(host)]
            .bdev
            .scan()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();
        let mut daemon_config = self.config.daemon.clone();
        daemon_config.host = HostId(host);
        if let Some(placement) = daemon_config.replica_placement.as_mut()
            && let Some(candidate) = placement
                .roster
                .iter()
                .find(|candidate| candidate.host == HostId(host))
        {
            placement.local_failure_domain = candidate.failure_domain;
        }
        let (daemon, verdicts, effects) = Daemon::recover(
            daemon_config,
            scan.iter().map(|(n, b)| (n.as_str(), b.as_slice())),
        );
        self.hosts[usize::from(host)].daemon = Some(daemon);
        self.report.recoveries += 1;
        self.apply_effects(host, effects);
        for (vset, verdict) in verdicts {
            self.attach_recovered(host, vset, verdict);
        }
    }

    /// A restarted daemon reached a local verdict for a vset. The
    /// placement map is the harness's ground truth of who runs: a
    /// runnable verdict for a vset that runs elsewhere is exactly the
    /// double-run the two-sided handoff and the head CAS exist to prevent.
    fn attach_recovered(&mut self, host: u16, vset: VsetId, verdict: Verdict) {
        if verdict == Verdict::Unrestorable && self.placement.get(&vset) != Some(&host) {
            // Nothing usable here while the vset runs (or is being
            // offered) elsewhere: exactly what a destination crash
            // mid-handshake leaves behind. The verdict claims nothing —
            // ownership stays put, recovery reclaims the wreckage, and
            // the source's re-offers restart the migration from scratch.
            return;
        }
        if self.placement.get(&vset) != Some(&host) {
            // One legitimate mismatch (R7.2): the current holder offered
            // this vset here, the destination durably accepted, then
            // crashed before anyone learned. The accept IS ownership —
            // recovery completes the handshake (the source's re-offers
            // get re-acked), so the placement moves; anything else is a
            // genuine second runner.
            let offered = self
                .pending_offers
                .get(&vset)
                .is_some_and(|&(source, dest)| {
                    dest == host && self.placement.get(&vset) == Some(&source)
                });
            if !offered {
                self.report.violations.push(format!(
                    "two runners: host {host} recovered {vset:?} as runnable, but it runs elsewhere"
                ));
                return;
            }
            self.pending_offers.remove(&vset);
            let source = self.placement.insert(vset, host).expect("was placed");
            self.migrated_from.insert(vset, source);
        }
        self.hosts[usize::from(host)].mems.entry(vset).or_default();
        match verdict {
            Verdict::Resume { vmstate, .. } => {
                self.oracle.on_resume(vset, vmstate);
                let infer = self.oracle.needs_disk_inference(vset);
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.reborn(vmstate, infer);
                self.schedule_guest(vset);
            }
            Verdict::ColdBoot => {
                self.oracle.start_cold_boot(vset);
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.reborn(0, true);
                self.schedule_guest(vset);
            }
            Verdict::DatabaseReady { .. } => {
                unreachable!("compute cluster recovered a database vset")
            }
            Verdict::Unrestorable => {
                // No storage damage is injected in cluster runs: local
                // recovery must always reach a verdict.
                self.report
                    .violations
                    .push(format!("{vset:?} unrestorable without injected damage"));
                self.guests.get_mut(&vset).expect("guest exists").state = GuestState::Dead;
            }
        }
    }

    // ── guests (identical semantics to the single-host harness, routed by
    // placement) ─────────────────────────────────────────────────────────

    fn schedule_guest(&mut self, vset: VsetId) {
        let (lo, hi) = self.config.think;
        let delay = self.kernel.rng().range(lo, hi);
        self.kernel.schedule_after(delay, Ev::GuestStep { vset });
    }

    fn guest_step(&mut self, vset: VsetId) {
        let host = self.placement[&vset];
        if self.hosts[usize::from(host)].daemon.is_none() {
            return;
        }
        let Cluster {
            kernel,
            guests,
            oracle,
            ..
        } = self;
        let Some(guest) = guests.get_mut(&vset) else {
            return;
        };
        if guest.state != GuestState::Idle || guest.paused {
            return;
        }
        match guest.next_op(kernel.rng(), |volume| oracle.next_vol_seq(volume)) {
            Err(volume) => {
                let req = self.req();
                self.sync_reqs.insert(req, vset);
                self.sync_started.insert(req, self.kernel.now());
                self.guests.get_mut(&vset).expect("guest exists").state =
                    GuestState::Syncing { req, volume };
                self.step_daemon(host, Event::GuestSync { req, volume });
            }
            Ok(op) => self.attempt_op(host, vset, op),
        }
    }

    fn attempt_op(&mut self, host: u16, vset: VsetId, op: PendingOp) {
        let result = crate::guest::attempt_op(
            self.hosts[usize::from(host)].mems.entry(vset).or_default(),
            self.guests.get_mut(&vset).expect("guest exists"),
            op,
        );
        match result {
            AttemptResult::Fault { page, write } => {
                self.step_daemon(host, Event::GuestFault { page, write });
            }
            AttemptResult::Complete => {
                // Deferred recovery can warm disk pages before the cold-boot
                // verdict reaches the harness. A resident fsck read must
                // still validate and claim those bytes.
                if let PendingOp::Fsck { page } = op
                    && self.guests[&vset].cold_booting
                {
                    let bytes = self.hosts[usize::from(host)].mems[&vset].pages[&page].clone();
                    self.oracle.check_fill(page, &bytes, true);
                }
                self.complete_op(host, vset, op);
            }
        }
    }

    fn complete_op(&mut self, host: u16, vset: VsetId, op: PendingOp) {
        let fsck_pending = crate::guest::complete_op(
            self.hosts[usize::from(host)].mems.get_mut(&vset),
            self.guests.get_mut(&vset).expect("guest exists"),
            &mut self.oracle,
            vset,
            op,
            |_| {},
        );
        if self.kernel.now().nanos() <= self.config.horizon || fsck_pending {
            self.schedule_guest(vset);
        }
    }

    fn fill(&mut self, host: u16, page: PageId, bytes: Vec<u8>, writable: bool) {
        let vset = page.volume.vset;
        // A migration destination may hydrate its protected tail after its
        // durable accept but before the harness observes VsetMigratedIn and
        // moves the control-plane placement. Those fills belong to the
        // accepted incarnation; only a host outside both roles is stale.
        let accepted_destination = self
            .pending_offers
            .get(&vset)
            .is_some_and(|&(_, destination)| destination == host);
        if self.placement.get(&vset) != Some(&host) && !accepted_destination {
            if std::env::var_os("BLOCKD_SIM_DEBUG").is_some() {
                eprintln!(
                    "[{:>12}] DROPPED fill host {host} {page:?} (placed {:?})",
                    self.kernel.now().nanos(),
                    self.placement.get(&vset)
                );
            }
            return; // fill from a fenced incarnation's tail
        }
        let result = crate::guest::fill(
            self.hosts[usize::from(host)].mems.entry(vset).or_default(),
            self.guests.get_mut(&vset).expect("guest exists"),
            &mut self.oracle,
            page,
            bytes,
            writable,
        );
        match result {
            FillResult::Installed => {}
            FillResult::Refault => self.refaults.push((host, page)),
            FillResult::Complete(op) => self.complete_op(host, vset, op),
        }
    }

    fn resolve_write(&mut self, host: u16, page: PageId) {
        let vset = page.volume.vset;
        if self.placement.get(&vset) != Some(&host) {
            return;
        }
        if let Some(op) = crate::guest::resolve_write(self.guests.get_mut(&vset)) {
            self.complete_op(host, vset, op);
        }
    }

    fn fill_failed(&mut self, page: PageId) {
        let vset = page.volume.vset;
        let Some(guest) = self.guests.get_mut(&vset) else {
            return;
        };
        let GuestState::Faulted { op } = guest.state else {
            return;
        };
        // No damage is injected in cluster runs: an unservable page is a
        // real violation unless an explicit damage nemesis marked the vset.
        self.oracle
            .on_fill_failed(page, self.doomed.contains(&vset));
        if matches!(op, PendingOp::Fsck { .. }) && guest.cold_booting {
            self.oracle.on_fsck_aborted(vset);
        }
        guest.state = GuestState::Dead;
        self.report.guest_deaths += 1;
    }

    fn sync_done(&mut self, req: ReqId, ok: bool) {
        let Some(vset) = self.sync_reqs.remove(&req) else {
            return;
        };
        let guest = self.guests.get_mut(&vset).expect("guest exists");
        let GuestState::Syncing {
            req: waiting,
            volume,
        } = guest.state
        else {
            return;
        };
        if waiting != req || !ok {
            return;
        }
        if let Some(started) = self.sync_started.remove(&req) {
            self.sync_latencies
                .push(self.kernel.now().nanos().saturating_sub(started.nanos()));
        }
        if guest.paused {
            guest.state = GuestState::SyncParked { volume };
            return;
        }
        guest.applied += 1;
        guest.state = GuestState::Idle;
        guest.completed += 1;
        self.oracle.on_sync_ok(volume);
        if self.kernel.now().nanos() <= self.config.horizon {
            self.schedule_guest(vset);
        }
    }

    /// Retire whatever completed while the vCPU was paused.
    fn unpark(&mut self, host: u16, vset: VsetId) {
        let result = crate::guest::unpark(
            self.guests.get_mut(&vset).expect("guest exists"),
            &mut self.oracle,
        );
        match result {
            UnparkResult::Attempt(op) => self.attempt_op(host, vset, op),
            UnparkResult::SyncComplete => {
                if self.kernel.now().nanos() <= self.config.horizon {
                    self.schedule_guest(vset);
                }
            }
            UnparkResult::Schedule => self.schedule_guest(vset),
        }
    }

    /// A restore claim won (R6.1): this host runs the vset now, checked
    /// against the R4.3 loss bound the head promised at the kill instant.
    fn vset_restored(&mut self, host: u16, req: ReqId, vset: VsetId, verdict: Verdict) {
        self.admin_reqs.remove(&req);
        self.report.restores += 1;
        if let Some(sent) = self.restore_sent.remove(&req) {
            let latency = self.kernel.now().nanos() - sent.nanos();
            self.report.max_restore_ns = self.report.max_restore_ns.max(latency);
        }
        self.placement.insert(vset, host);
        self.hosts[usize::from(host)].mems.entry(vset).or_default();
        let restored = self
            .store
            .peek(&layout::head_key(vset))
            .and_then(|bytes| HeadRecord::decode(vset, bytes).ok())
            .and_then(|head| head.manifest);
        match (self.expected_ptr.remove(&vset), restored) {
            (Some(expected), got) if expected == got => {
                self.report.loss_bound_verified += 1;
            }
            (None, _) => {}
            (Some(expected), got) => self.report.violations.push(format!(
                "R4.3: {vset:?} restored to {got:?}, head at death said {expected:?}"
            )),
        }
        match verdict {
            Verdict::Resume { vmstate, .. } => {
                self.oracle.on_resume(vset, vmstate);
                let infer = self.oracle.needs_disk_inference(vset);
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.reborn(vmstate, infer);
            }
            Verdict::ColdBoot | Verdict::Unrestorable => {
                self.oracle.start_cold_boot(vset);
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.reborn(0, true);
            }
            Verdict::DatabaseReady { .. } => {
                unreachable!("compute cluster restored a database vset")
            }
        }
        self.schedule_guest(vset);
    }

    fn admin_reply(&mut self, host: u16, reply: AdminReply) {
        if std::env::var_os("BLOCKD_SIM_DEBUG").is_some() {
            eprintln!("[{:>12}] host {host}: {reply:?}", self.kernel.now().nanos());
        }
        match reply {
            AdminReply::VsetCreated { req, vset } => {
                self.admin_reqs.remove(&req);
                let config = self.vset_config_for(vset);
                self.oracle.register(vset, config);
                self.hosts[usize::from(host)].mems.entry(vset).or_default();
                let mut guest = Guest::new(vset, config);
                guest.sync_share = self.config.guest_sync_share;
                self.guests.insert(vset, guest);
                self.schedule_guest(vset);
                if let Some(interval) = self.config.checkpoint_interval {
                    let delay = self.kernel.rng().range(1, 2 * interval);
                    self.kernel
                        .schedule_after(delay, Ev::CheckpointTick { vset });
                }
            }
            AdminReply::CheckpointDone { req, .. } | AdminReply::AdminFailed { req } => {
                if self.admin_reqs.remove(&req).is_some()
                    && matches!(reply, AdminReply::AdminFailed { .. })
                {
                    if self.migrate_reqs.remove(&req) {
                        // The daemon refused the migration (busy, wrong
                        // mode, mid-drain) — the nemesis just tries later.
                        self.report.migrations_refused += 1;
                    } else {
                        // Restore losers land here: exactly-one-runner
                        // (R6.3).
                        self.report.claims_lost += 1;
                    }
                }
            }
            AdminReply::VsetRestored { req, vset, verdict } => {
                self.vset_restored(host, req, vset, verdict);
            }
            AdminReply::MigratedOut { req, .. } => {
                // Bookkeeping only: the source's ack can be swallowed by a
                // crash after the handoff is already decided, so completed
                // migrations are counted at the DESTINATION's adoption —
                // the ground truth of who runs the vset.
                self.admin_reqs.remove(&req);
                self.migrate_reqs.remove(&req);
            }
            AdminReply::VsetMigratedIn { vset, verdict } => {
                self.report.migrations += 1;
                // R7.1: the guest-observed pause spans the source's pause
                // to the destination coming up ready to serve.
                if let Some(paused) = self.paused_at.get(&vset) {
                    let pause = self.kernel.now().nanos() - paused.nanos();
                    self.report.max_migration_pause_ns =
                        self.report.max_migration_pause_ns.max(pause);
                }
                // The destination's first record is durable: the vset now
                // runs here (R7.2), demand-faulting its tail from the
                // source (R7.1: memory arrives post-copy).
                let source = self.placement.insert(vset, host).expect("was placed");
                self.migrated_from.insert(vset, source);
                if self.config.sabotage == Some(Sabotage::RogueRelease)
                    && let Some(rogue) = (0..self.config.hosts).find(|&h| h != host && h != source)
                {
                    // Mid-drain forged release from a non-destination: the
                    // source's guard must refuse it or the real
                    // destination's tail dies with the reclaimed state.
                    self.kernel.schedule_after(
                        millis(1),
                        Ev::PeerDeliver {
                            from: rogue,
                            to: source,
                            msg: blockd_core::seam::PeerMsg::Released { vset },
                        },
                    );
                }
                self.hosts[usize::from(host)].mems.entry(vset).or_default();
                let Verdict::Resume { vmstate, .. } = verdict else {
                    self.report
                        .violations
                        .push(format!("R7: {vset:?} migrated in without resume state"));
                    return;
                };
                // Migration is lossless: the same on_resume check as a
                // restore, with NO sync-loss allowance (R7.1 vs R4.3).
                self.oracle.on_resume(vset, vmstate);
                let infer = self.oracle.needs_disk_inference(vset);
                let guest = self.guests.get_mut(&vset).expect("guest exists");
                guest.reborn(vmstate, infer);
                self.schedule_guest(vset);
            }
            AdminReply::VsetRecovered { vset, verdict } => {
                // A crashed-and-restarted host finished the deferred backed
                // recovery (head confirmed ownership): reattach its guest —
                // with the same two-runners check as an immediate verdict.
                self.attach_recovered(host, vset, verdict);
            }
            AdminReply::BaseKept { .. }
            | AdminReply::BaseDeleted { .. }
            | AdminReply::VsetForked { .. }
            | AdminReply::DatabaseAttached { .. }
            | AdminReply::DatabaseDetachStarted { .. }
            | AdminReply::DatabaseDetached { .. } => {}
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn store_fault(err: crate::world::store::StoreError) -> blockd_core::seam::StoreFault {
    use crate::world::store::StoreError;
    match err {
        StoreError::Unavailable | StoreError::TooLarge => {
            blockd_core::seam::StoreFault::Unavailable
        }
        StoreError::CasConflict { actual } => blockd_core::seam::StoreFault::CasConflict {
            actual: actual.map(|v| v.0),
        },
    }
}
