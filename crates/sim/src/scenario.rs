//! Checked-in, composable simulation scenarios.
//!
//! A scenario is JSON data embedded in the simulator binary. `extends` composes
//! reusable definitions by recursively merging objects; arrays and scalar
//! values replace their parents. Bounded distributions are realized from an
//! independent, path-keyed RNG stream, so scenario choices never consume or
//! perturb the simulation kernel's RNG. Fixed legacy scenarios therefore keep
//! their byte-for-byte traces while exploratory scenarios can vary topology,
//! workload, nemeses, and operational knobs by seed.

use std::fmt;

use blockd_core::daemon::{ArchivePolicy, DaemonConfig, ReplicaPlacementConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::placement::{PeerCandidate, rank_stash_candidates};
use blockd_core::types::{HostId, VsetId};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::cluster::{ClusterConfig, FaultPoint, PeerKind};
use crate::harness::{FaultPlan, HarnessConfig};
use crate::hash::Fnv64;
use crate::rng::{Pcg64, Ppm};
use crate::world::blobdev::BlobDevConfig;
use crate::world::store::StoreConfig;

const DOCUMENTS: &[(&str, &str)] = &[
    (
        "single-host-base",
        include_str!("../scenarios/single-host-base.json"),
    ),
    ("chaos", include_str!("../scenarios/chaos.json")),
    ("cluster", include_str!("../scenarios/cluster.json")),
    ("migration", include_str!("../scenarios/migration.json")),
    ("peer-stash", include_str!("../scenarios/peer-stash.json")),
    (
        "peer-attrition",
        include_str!("../scenarios/peer-attrition.json"),
    ),
    ("peer-links", include_str!("../scenarios/peer-links.json")),
    ("peer-rare", include_str!("../scenarios/peer-rare.json")),
    (
        "placement-fear",
        include_str!("../scenarios/placement-fear.json"),
    ),
    ("explore", include_str!("../scenarios/explore.json")),
    (
        "cold-restore-outage",
        include_str!("../scenarios/cold-restore-outage.json"),
    ),
    (
        "nvme-pressure-backed",
        include_str!("../scenarios/nvme-pressure-backed.json"),
    ),
    (
        "nvme-pressure-unbacked",
        include_str!("../scenarios/nvme-pressure-unbacked.json"),
    ),
    (
        "migration-release-blackout",
        include_str!("../scenarios/migration-release-blackout.json"),
    ),
    (
        "migration-leaf-blackout",
        include_str!("../scenarios/migration-leaf-blackout.json"),
    ),
    (
        "hot-compaction",
        include_str!("../scenarios/hot-compaction.json"),
    ),
    (
        "resume-set-rot",
        include_str!("../scenarios/resume-set-rot.json"),
    ),
    ("leaf-rot", include_str!("../scenarios/leaf-rot.json")),
    (
        "peer-commit-crashes",
        include_str!("../scenarios/peer-commit-crashes.json"),
    ),
    (
        "peer-transfer-crashes",
        include_str!("../scenarios/peer-transfer-crashes.json"),
    ),
    (
        "peer-transition-before-cas",
        include_str!("../scenarios/peer-transition-before-cas.json"),
    ),
    (
        "peer-transition-after-seed",
        include_str!("../scenarios/peer-transition-after-seed.json"),
    ),
    (
        "peer-transition-after-active",
        include_str!("../scenarios/peer-transition-after-active.json"),
    ),
];

/// Stable names accepted by the sweep runner.
pub const SWEEP_SCENARIOS: &[&str] = &[
    "chaos",
    "cluster",
    "migration",
    "peer-stash",
    "peer-rare",
    "explore",
    "cold-restore-outage",
    "nvme-pressure-backed",
    "nvme-pressure-unbacked",
    "migration-release-blackout",
    "migration-leaf-blackout",
    "hot-compaction",
    "resume-set-rot",
    "leaf-rot",
    "peer-commit-crashes",
    "peer-transfer-crashes",
    "peer-transition-before-cas",
    "peer-transition-after-seed",
    "peer-transition-after-active",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScenarioError(String);

impl ScenarioError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ScenarioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ScenarioError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScenarioKind {
    SingleHost,
    Cluster,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CoverageMetric {
    DaemonCrash,
    BitFlip,
    StoreRetry,
    OrphanRestore,
    RestoreClaimRace,
    HostRecovery,
    CompletedMigration,
    PeerDrop,
    PeerDuplicate,
    PeerFault,
    ReplicaCommit,
    StoreUnavailable,
    NvmeReclaim,
    NvmeStall,
    RecordsWritten,
    PagesFlushed,
    GuestDeath,
    NemesisDrop,
    Wedge,
    WedgeGuest,
    WedgeHydration,
    WedgeOutbound,
    Release,
    LeafFill,
    PrefetchFill,
    ParkedEnd,
    HydratingEnd,
    SpaceAmplificationPpm,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoverageRequirement {
    pub metric: CoverageMetric,
    pub label: String,
    #[serde(default)]
    pub fault_point: Option<FaultPointSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeRequirement {
    pub metric: CoverageMetric,
    pub label: String,
    #[serde(default)]
    pub fault_point: Option<FaultPointSpec>,
    pub min: Option<u64>,
    pub max: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct Scenario {
    spec: ScenarioSpec,
    sources: Vec<String>,
    resolved: String,
}

impl Scenario {
    pub fn name(&self) -> &str {
        &self.spec.name
    }

    pub fn description(&self) -> &str {
        &self.spec.description
    }

    pub fn kind(&self) -> ScenarioKind {
        self.spec.kind
    }

    pub fn coverage(&self) -> &[CoverageRequirement] {
        &self.spec.coverage
    }

    pub fn outcomes(&self) -> &[OutcomeRequirement] {
        &self.spec.outcomes
    }

    pub fn sources(&self) -> &[String] {
        &self.sources
    }

    /// Canonical, fully composed specification retained in replay artifacts.
    pub fn resolved_specification(&self) -> &str {
        &self.resolved
    }

    pub fn realize(&self, seed: u64) -> Result<RealizedScenario, ScenarioError> {
        let realizer = Realizer { seed };
        match self.spec.kind {
            ScenarioKind::SingleHost => self
                .realize_single(&realizer)
                .map(RealizedScenario::SingleHost),
            ScenarioKind::Cluster => self
                .realize_cluster(&realizer)
                .map(RealizedScenario::Cluster),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn common(&self, r: &Realizer) -> Result<Common, ScenarioError> {
        let daemon = &self.spec.daemon;
        let topology = &self.spec.topology;
        let workload = &self.spec.workload;
        let cache_pages = r.usize(&daemon.cache_pages, "daemon.cache-pages")?;
        if cache_pages == 0 {
            return Err(ScenarioError::new("daemon.cache-pages must be positive"));
        }
        let writeback_interval =
            r.duration(&daemon.writeback_interval, "daemon.writeback-interval")?;
        let backup_retry = r.duration(&daemon.backup_retry, "daemon.backup-retry")?;
        if writeback_interval == 0 || backup_retry == 0 {
            return Err(ScenarioError::new(
                "writeback and backup retry durations must be positive",
            ));
        }
        let disk_capacity = daemon
            .disk_capacity_bytes
            .as_ref()
            .map(|v| r.count(v, "daemon.disk-capacity-bytes"))
            .transpose()?;
        let disk_headroom = r.count(&daemon.disk_headroom_bytes, "daemon.disk-headroom-bytes")?;
        if disk_capacity.is_some_and(|capacity| disk_headroom >= capacity) {
            return Err(ScenarioError::new(
                "daemon.disk-headroom-bytes must be smaller than disk capacity",
            ));
        }
        let disk_volumes = r.u8(&topology.disk_volumes, "topology.disk-volumes")?;
        let pages_per_volume = r.u32(&topology.pages_per_volume, "topology.pages-per-volume")?;
        if pages_per_volume == 0 {
            return Err(ScenarioError::new(
                "topology.pages-per-volume must be positive",
            ));
        }
        let vset_config = VsetConfig::compute(disk_volumes, pages_per_volume);
        let vset_count = r.u16(&topology.vset_count, "topology.vset-count")?;
        if vset_count == 0 {
            return Err(ScenarioError::new("topology.vset-count must be positive"));
        }
        let horizon = r.duration(&workload.horizon, "workload.horizon")?;
        let think = r.duration_range(&workload.think, "workload.think")?;
        let checkpoint_interval = workload
            .checkpoint_interval
            .as_ref()
            .map(|v| r.duration(v, "workload.checkpoint-interval"))
            .transpose()?;
        let guest_sync_share = workload
            .sync_share_ppm
            .as_ref()
            .map(|v| r.ppm(v, "workload.sync-share-ppm"))
            .transpose()?;
        let guest_hot_pages = workload
            .hot_pages
            .as_ref()
            .map(|hot| {
                let share = r.ppm(&hot.share_ppm, "workload.hot-pages.share-ppm")?;
                let pages = r.u32(&hot.pages, "workload.hot-pages.pages")?;
                if pages == 0 || pages >= pages_per_volume {
                    return Err(ScenarioError::new(
                        "hot page count must be within 1..pages-per-volume",
                    ));
                }
                Ok((share, pages))
            })
            .transpose()?;
        let bdev = BlobDevConfig {
            read_latency_min: r.duration(
                &self.spec.storage.blob_device.read_latency.min,
                "storage.blob-device.read-latency.min",
            )?,
            read_latency_max: r.duration(
                &self.spec.storage.blob_device.read_latency.max,
                "storage.blob-device.read-latency.max",
            )?,
            write_latency_min: r.duration(
                &self.spec.storage.blob_device.write_latency.min,
                "storage.blob-device.write-latency.min",
            )?,
            write_latency_max: r.duration(
                &self.spec.storage.blob_device.write_latency.max,
                "storage.blob-device.write-latency.max",
            )?,
            ns_per_byte: r.count(
                &self.spec.storage.blob_device.ns_per_byte,
                "storage.blob-device.ns-per-byte",
            )?,
        };
        if bdev.read_latency_min > bdev.read_latency_max
            || bdev.write_latency_min > bdev.write_latency_max
        {
            return Err(ScenarioError::new(
                "blob device latency minima must not exceed maxima",
            ));
        }
        let store = StoreConfig {
            latency_min: r.duration(
                &self.spec.storage.object_store.latency.min,
                "storage.object-store.latency.min",
            )?,
            latency_max: r.duration(
                &self.spec.storage.object_store.latency.max,
                "storage.object-store.latency.max",
            )?,
            ns_per_byte: r.count(
                &self.spec.storage.object_store.ns_per_byte,
                "storage.object-store.ns-per-byte",
            )?,
        };
        if store.latency_min > store.latency_max {
            return Err(ScenarioError::new(
                "object store latency minimum must not exceed maximum",
            ));
        }
        Ok(Common {
            daemon: DaemonConfig {
                archive: ArchivePolicy {
                    interval: blockd_core::types::secs(1),
                    ..ArchivePolicy::default()
                },
                host: HostId(0),
                cache_pages,
                writeback_interval,
                backup_retry,
                disk_capacity,
                disk_headroom,
                wedge_ticks: r.count(&daemon.wedge_ticks, "daemon.wedge-ticks")?,
                replica_placement: None,
            },
            bdev,
            store,
            vset_count,
            vset_config,
            horizon,
            think,
            checkpoint_interval,
            guest_sync_share,
            guest_hot_pages,
        })
    }

    fn realize_single(&self, r: &Realizer) -> Result<HarnessConfig, ScenarioError> {
        let common = self.common(r)?;
        if self.spec.topology.hosts.is_some() {
            return Err(ScenarioError::new(
                "single-host scenarios cannot set cluster topology fields",
            ));
        }
        if self.spec.topology.replica_placement.is_some() {
            return Err(ScenarioError::new(
                "single-host scenarios cannot configure replica placement",
            ));
        }
        let nemeses = &self.spec.nemeses;
        let restart_delay = r.duration_range(
            nemeses
                .restart_delay
                .as_ref()
                .ok_or_else(|| ScenarioError::new("nemeses.restart-delay is required"))?,
            "nemeses.restart-delay",
        )?;
        let faults = FaultPlan {
            crash_mean_interval: r.optional_duration_or_zero(
                nemeses.crash_mean_interval.as_ref(),
                "nemeses.crash-mean-interval",
            )?,
            restart_delay,
            bitflip_mean_interval: r.optional_duration_or_zero(
                nemeses.bitflip_mean_interval.as_ref(),
                "nemeses.bitflip-mean-interval",
            )?,
            journal_bitflip_mean_interval: r.optional_duration_or_zero(
                nemeses.journal_bitflip_mean_interval.as_ref(),
                "nemeses.journal-bitflip-mean-interval",
            )?,
            store_outage: nemeses
                .store_outage
                .as_ref()
                .map(|window| r.window(window, "nemeses.store-outage"))
                .transpose()?,
        };
        let rot_records_at = nemeses
            .rot_records
            .iter()
            .enumerate()
            .map(|(index, item)| {
                Ok((
                    r.duration(&item.at, &format!("nemeses.rot-records.{index}.at"))?,
                    item.mirror,
                ))
            })
            .collect::<Result<Vec<_>, ScenarioError>>()?;
        let crash_at = nemeses
            .crash_at
            .iter()
            .enumerate()
            .map(|(index, at)| r.duration(at, &format!("nemeses.crash-at.{index}")))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(HarnessConfig {
            daemon: common.daemon,
            bdev: common.bdev,
            store: common.store,
            vset_count: common.vset_count,
            vset_config: common.vset_config,
            horizon: common.horizon,
            think: common.think,
            checkpoint_interval: common.checkpoint_interval,
            faults,
            sabotage: None,
            guest_sync_share: common.guest_sync_share,
            guest_hot_pages: common.guest_hot_pages,
            rot_records_at,
            crash_at,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn realize_cluster(&self, r: &Realizer) -> Result<ClusterConfig, ScenarioError> {
        let mut common = self.common(r)?;
        if common.guest_hot_pages.is_some() {
            return Err(ScenarioError::new(
                "cluster scenarios do not yet support workload.hot-pages",
            ));
        }
        let hosts = r.u16(
            self.spec
                .topology
                .hosts
                .as_ref()
                .ok_or_else(|| ScenarioError::new("topology.hosts is required"))?,
            "topology.hosts",
        )?;
        if hosts < 2 {
            return Err(ScenarioError::new(
                "cluster scenarios require at least two hosts",
            ));
        }
        common.daemon.replica_placement = self
            .spec
            .topology
            .replica_placement
            .as_ref()
            .map(|placement| placement.realize(hosts, r))
            .transpose()?;
        let nemeses = &self.spec.nemeses;
        let restart_delay = r.duration_range(
            nemeses
                .restart_delay
                .as_ref()
                .ok_or_else(|| ScenarioError::new("nemeses.restart-delay is required"))?,
            "nemeses.restart-delay",
        )?;
        let mut config = ClusterConfig {
            hosts,
            daemon: common.daemon,
            bdev: common.bdev,
            store: common.store,
            vset_count: common.vset_count,
            vset_config: common.vset_config,
            horizon: common.horizon,
            think: common.think,
            checkpoint_interval: common.checkpoint_interval,
            kill_hosts_at: Vec::new(),
            crash_hosts_at: Vec::new(),
            restart_delay,
            crash_mean_interval: r.optional_duration_or_zero(
                nemeses.crash_mean_interval.as_ref(),
                "nemeses.crash-mean-interval",
            )?,
            migrate_mean_interval: r.optional_duration_or_zero(
                nemeses.migrate_mean_interval.as_ref(),
                "nemeses.migrate-mean-interval",
            )?,
            peer_drop: r.ratio(nemeses.peer_drop.as_ref(), "nemeses.peer-drop")?,
            peer_dup: r.ratio(nemeses.peer_dup.as_ref(), "nemeses.peer-dup")?,
            peer_link_outages: Vec::new(),
            fault_points: nemeses
                .fault_points
                .iter()
                .map(|point| point.to_fault_point())
                .collect(),
            store_outage: nemeses
                .store_outage
                .as_ref()
                .map(|window| r.window(window, "nemeses.store-outage"))
                .transpose()?,
            rot_resume_set_at: nemeses
                .rot_resume_set_at
                .as_ref()
                .map(|v| r.duration(v, "nemeses.rot-resume-set-at"))
                .transpose()?,
            rot_leaves_at: nemeses
                .rot_leaves_at
                .as_ref()
                .map(|v| r.duration(v, "nemeses.rot-leaves-at"))
                .transpose()?,
            drop_peer: nemeses
                .drop_peer
                .as_ref()
                .map(|drop| {
                    let (start, end) = r.window(&drop.window, "nemeses.drop-peer.window")?;
                    Ok((drop.kind.into(), start, end))
                })
                .transpose()?,
            race_restore: nemeses.race_restore,
            migrate_at: None,
            sabotage: None,
            guest_sync_share: common.guest_sync_share,
        };
        config.kill_hosts_at =
            Self::resolve_scheduled_hosts(&config, r, &nemeses.kill_hosts, "nemeses.kill-hosts")?;
        config.crash_hosts_at =
            Self::resolve_scheduled_hosts(&config, r, &nemeses.crash_hosts, "nemeses.crash-hosts")?;
        config.peer_link_outages = nemeses
            .peer_link_outages
            .iter()
            .enumerate()
            .map(|(index, link)| {
                let (start, end) = r.window(
                    &link.window,
                    &format!("nemeses.peer-link-outages.{index}.window"),
                )?;
                Ok((
                    start,
                    end,
                    resolve_host(&config, r, &link.from, &format!("link.{index}.from"))?,
                    resolve_host(&config, r, &link.to, &format!("link.{index}.to"))?,
                ))
            })
            .collect::<Result<Vec<_>, ScenarioError>>()?;
        config.migrate_at = nemeses
            .migrate_at
            .as_ref()
            .map(|migrate| {
                let vset = r.count(&migrate.vset, "nemeses.migrate-at.vset")?;
                if vset == 0 || vset > u64::from(config.vset_count) {
                    return Err(ScenarioError::new(
                        "nemeses.migrate-at.vset is outside the configured vsets",
                    ));
                }
                Ok((
                    r.duration(&migrate.at, "nemeses.migrate-at.at")?,
                    VsetId(vset),
                    resolve_host(&config, r, &migrate.to, "nemeses.migrate-at.to")?,
                ))
            })
            .transpose()?;
        Ok(config)
    }

    fn resolve_scheduled_hosts(
        config: &ClusterConfig,
        r: &Realizer,
        items: &[ScheduledHost],
        path: &str,
    ) -> Result<Vec<(u64, u16)>, ScenarioError> {
        items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                Ok((
                    r.duration(&item.at, &format!("{path}.{index}.at"))?,
                    resolve_host(config, r, &item.host, &format!("{path}.{index}.host"))?,
                ))
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub enum RealizedScenario {
    SingleHost(HarnessConfig),
    Cluster(ClusterConfig),
}

impl RealizedScenario {
    pub fn kind(&self) -> ScenarioKind {
        match self {
            Self::SingleHost(_) => ScenarioKind::SingleHost,
            Self::Cluster(_) => ScenarioKind::Cluster,
        }
    }
}

/// Load and compose one embedded scenario by name.
pub fn load(name: &str) -> Result<Scenario, ScenarioError> {
    let mut stack = Vec::new();
    let mut sources = Vec::new();
    let value = load_value(name, &mut stack, &mut sources)?;
    let spec: ScenarioSpec = serde_json::from_value(value.clone())
        .map_err(|error| ScenarioError::new(format!("scenario {name}: {error}")))?;
    if spec.schema != 1 {
        return Err(ScenarioError::new(format!(
            "scenario {name}: unsupported schema {}",
            spec.schema
        )));
    }
    if spec.name != name {
        return Err(ScenarioError::new(format!(
            "scenario catalog key {name} does not match document name {}",
            spec.name
        )));
    }
    for requirement in &spec.coverage {
        validate_fault_selector(name, requirement.metric, requirement.fault_point)?;
    }
    for requirement in &spec.outcomes {
        validate_fault_selector(name, requirement.metric, requirement.fault_point)?;
        if requirement.min.is_none() && requirement.max.is_none() {
            return Err(ScenarioError::new(format!(
                "scenario {name}: outcome {:?} must set min, max, or both",
                requirement.metric
            )));
        }
        if let (Some(min), Some(max)) = (requirement.min, requirement.max)
            && min > max
        {
            return Err(ScenarioError::new(format!(
                "scenario {name}: outcome {:?} minimum exceeds maximum",
                requirement.metric
            )));
        }
    }
    let resolved = serde_json::to_string_pretty(&value)
        .map_err(|error| ScenarioError::new(error.to_string()))?;
    Ok(Scenario {
        spec,
        sources,
        resolved,
    })
}

fn validate_fault_selector(
    name: &str,
    metric: CoverageMetric,
    fault_point: Option<FaultPointSpec>,
) -> Result<(), ScenarioError> {
    if fault_point.is_some() && metric != CoverageMetric::PeerFault {
        return Err(ScenarioError::new(format!(
            "scenario {name}: fault-point is valid only with the peer-fault metric"
        )));
    }
    Ok(())
}

pub fn names() -> impl Iterator<Item = &'static str> {
    DOCUMENTS.iter().map(|(name, _)| *name)
}

fn document(name: &str) -> Option<&'static str> {
    DOCUMENTS
        .iter()
        .find_map(|(candidate, text)| (*candidate == name).then_some(*text))
}

fn load_value(
    name: &str,
    stack: &mut Vec<String>,
    sources: &mut Vec<String>,
) -> Result<Value, ScenarioError> {
    if stack.iter().any(|item| item == name) {
        stack.push(name.to_owned());
        return Err(ScenarioError::new(format!(
            "scenario inheritance cycle: {}",
            stack.join(" -> ")
        )));
    }
    let text =
        document(name).ok_or_else(|| ScenarioError::new(format!("unknown scenario {name}")))?;
    let mut own: Value = serde_json::from_str(text)
        .map_err(|error| ScenarioError::new(format!("scenario {name}: {error}")))?;
    let object = own
        .as_object_mut()
        .ok_or_else(|| ScenarioError::new(format!("scenario {name} is not an object")))?;
    let parents = match object.remove("extends") {
        None => Vec::new(),
        Some(Value::Array(values)) => values
            .into_iter()
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    ScenarioError::new(format!("scenario {name}: extends entries must be strings"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => {
            return Err(ScenarioError::new(format!(
                "scenario {name}: extends must be an array"
            )));
        }
    };
    stack.push(name.to_owned());
    let mut merged = Value::Object(Map::new());
    for parent in parents {
        let inherited = load_value(&parent, stack, sources)?;
        merge(&mut merged, inherited);
    }
    stack.pop();
    merge(&mut merged, own);
    if !sources.iter().any(|source| source == name) {
        sources.push(name.to_owned());
    }
    Ok(merged)
}

fn merge(base: &mut Value, overlay: Value) {
    match overlay {
        Value::Object(overlay) => {
            if let Some(base) = base.as_object_mut() {
                for (key, value) in overlay {
                    match base.get_mut(&key) {
                        Some(existing) => merge(existing, value),
                        None => {
                            base.insert(key, value);
                        }
                    }
                }
            } else {
                *base = Value::Object(overlay);
            }
        }
        overlay => *base = overlay,
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScenarioSpec {
    schema: u32,
    name: String,
    description: String,
    kind: ScenarioKind,
    daemon: DaemonSpec,
    storage: StorageSpec,
    topology: TopologySpec,
    workload: WorkloadSpec,
    #[serde(default)]
    nemeses: NemesisSpec,
    #[serde(default)]
    coverage: Vec<CoverageRequirement>,
    #[serde(default)]
    outcomes: Vec<OutcomeRequirement>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonSpec {
    cache_pages: CountSpec,
    writeback_interval: DurationSpec,
    backup_retry: DurationSpec,
    disk_capacity_bytes: Option<CountSpec>,
    disk_headroom_bytes: CountSpec,
    wedge_ticks: CountSpec,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageSpec {
    blob_device: BlobDeviceSpec,
    object_store: ObjectStoreSpec,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobDeviceSpec {
    read_latency: DurationRange,
    write_latency: DurationRange,
    ns_per_byte: CountSpec,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectStoreSpec {
    latency: DurationRange,
    ns_per_byte: CountSpec,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TopologySpec {
    hosts: Option<CountSpec>,
    vset_count: CountSpec,
    disk_volumes: CountSpec,
    pages_per_volume: CountSpec,
    replica_placement: Option<PlacementSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlacementSpec {
    mode: PlacementMode,
    membership_epoch: CountSpec,
    local_failure_domain: CountSpec,
}

impl PlacementSpec {
    fn realize(&self, hosts: u16, r: &Realizer) -> Result<ReplicaPlacementConfig, ScenarioError> {
        match self.mode {
            PlacementMode::WeightedHosts => Ok(ReplicaPlacementConfig {
                membership_epoch: r.count(
                    &self.membership_epoch,
                    "topology.replica-placement.membership-epoch",
                )?,
                local_failure_domain: r.u16(
                    &self.local_failure_domain,
                    "topology.replica-placement.local-failure-domain",
                )?,
                roster: (0..hosts)
                    .map(|host| PeerCandidate {
                        host: HostId(host),
                        weight: host + 1,
                        failure_domain: host + 1,
                        drained: false,
                    })
                    .collect(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum PlacementMode {
    WeightedHosts,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadSpec {
    horizon: DurationSpec,
    think: DurationRange,
    checkpoint_interval: Option<DurationSpec>,
    sync_share_ppm: Option<CountSpec>,
    hot_pages: Option<HotPagesSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HotPagesSpec {
    share_ppm: CountSpec,
    pages: CountSpec,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NemesisSpec {
    restart_delay: Option<DurationRange>,
    crash_mean_interval: Option<DurationSpec>,
    migrate_mean_interval: Option<DurationSpec>,
    bitflip_mean_interval: Option<DurationSpec>,
    journal_bitflip_mean_interval: Option<DurationSpec>,
    store_outage: Option<WindowSpec>,
    peer_drop: Option<RatioSpec>,
    peer_dup: Option<RatioSpec>,
    kill_hosts: Vec<ScheduledHost>,
    crash_hosts: Vec<ScheduledHost>,
    peer_link_outages: Vec<LinkOutageSpec>,
    fault_points: Vec<FaultPointSpec>,
    rot_records: Vec<RotRecordSpec>,
    crash_at: Vec<DurationSpec>,
    rot_resume_set_at: Option<DurationSpec>,
    rot_leaves_at: Option<DurationSpec>,
    drop_peer: Option<DropPeerSpec>,
    race_restore: bool,
    migrate_at: Option<MigrateAtSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RatioSpec {
    numerator: CountSpec,
    denominator: CountSpec,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowSpec {
    start: DurationSpec,
    end: DurationSpec,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduledHost {
    at: DurationSpec,
    host: HostSelector,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum HostSelector {
    Id(CountSpec),
    StashRank { stash_rank: CountSpec },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LinkOutageSpec {
    window: WindowSpec,
    from: HostSelector,
    to: HostSelector,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FaultPointSpec {
    ReplicaRetryTimer,
    DuplicateAck,
    StatusReconciliation,
    ReleaseOverlap,
    AssignmentCasRace,
    StoreUnknownResult,
    RestartScan,
    CrashPeerAfterCommitBeforeAck,
    CrashPrimaryAfterAckBeforeSyncOk,
    CrashPrimaryAfterSyncOk,
    CrashPeerAfterUploadBeforeHead,
    CrashPrimaryAfterHeadBeforeRelease,
    CrashPrimaryBeforeTransitionCas,
    CrashPrimaryAfterSeedBeforeActiveCas,
    CrashPrimaryAfterActiveCasBeforeCommit,
    CrashPrimaryBeforeClosureCapture,
    CrashPrimaryAfterClosureCapture,
    CrashPrimaryDuringArtifactTransfer,
    CrashPeerAfterDataFlushBeforeCommit,
    CrashPeerDuringUpload,
}

impl FaultPointSpec {
    pub fn to_fault_point(self) -> FaultPoint {
        match self {
            Self::ReplicaRetryTimer => FaultPoint::ReplicaRetryTimer,
            Self::DuplicateAck => FaultPoint::DuplicateAck,
            Self::StatusReconciliation => FaultPoint::StatusReconciliation,
            Self::ReleaseOverlap => FaultPoint::ReleaseOverlap,
            Self::AssignmentCasRace => FaultPoint::AssignmentCasRace,
            Self::StoreUnknownResult => FaultPoint::StoreUnknownResult,
            Self::RestartScan => FaultPoint::RestartScan,
            Self::CrashPeerAfterCommitBeforeAck => FaultPoint::CrashPeerAfterCommitBeforeAck,
            Self::CrashPrimaryAfterAckBeforeSyncOk => FaultPoint::CrashPrimaryAfterAckBeforeSyncOk,
            Self::CrashPrimaryAfterSyncOk => FaultPoint::CrashPrimaryAfterSyncOk,
            Self::CrashPeerAfterUploadBeforeHead => FaultPoint::CrashPeerAfterUploadBeforeHead,
            Self::CrashPrimaryAfterHeadBeforeRelease => {
                FaultPoint::CrashPrimaryAfterHeadBeforeRelease
            }
            Self::CrashPrimaryBeforeTransitionCas => FaultPoint::CrashPrimaryBeforeTransitionCas,
            Self::CrashPrimaryAfterSeedBeforeActiveCas => {
                FaultPoint::CrashPrimaryAfterSeedBeforeActiveCas
            }
            Self::CrashPrimaryAfterActiveCasBeforeCommit => {
                FaultPoint::CrashPrimaryAfterActiveCasBeforeCommit
            }
            Self::CrashPrimaryBeforeClosureCapture => FaultPoint::CrashPrimaryBeforeClosureCapture,
            Self::CrashPrimaryAfterClosureCapture => FaultPoint::CrashPrimaryAfterClosureCapture,
            Self::CrashPrimaryDuringArtifactTransfer => {
                FaultPoint::CrashPrimaryDuringArtifactTransfer
            }
            Self::CrashPeerAfterDataFlushBeforeCommit => {
                FaultPoint::CrashPeerAfterDataFlushBeforeCommit
            }
            Self::CrashPeerDuringUpload => FaultPoint::CrashPeerDuringUpload,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RotRecordSpec {
    at: DurationSpec,
    mirror: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DropPeerSpec {
    kind: PeerKindSpec,
    window: WindowSpec,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum PeerKindSpec {
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
    ReplicaStatus,
    ReplicaStatusReply,
    ReplicaUploadDone,
    ReplicaRelease,
    ReplicaReleaseAck,
}

impl From<PeerKindSpec> for PeerKind {
    fn from(value: PeerKindSpec) -> Self {
        match value {
            PeerKindSpec::Offer => Self::Offer,
            PeerKindSpec::Accept => Self::Accept,
            PeerKindSpec::FetchRange => Self::FetchRange,
            PeerKindSpec::Page => Self::Page,
            PeerKindSpec::FetchLeaf => Self::FetchLeaf,
            PeerKindSpec::Leaf => Self::Leaf,
            PeerKindSpec::Released => Self::Released,
            PeerKindSpec::ReleasedAck => Self::ReleasedAck,
            PeerKindSpec::ReplicaPut => Self::ReplicaPut,
            PeerKindSpec::ReplicaPutAck => Self::ReplicaPutAck,
            PeerKindSpec::ReplicaCommit => Self::ReplicaCommit,
            PeerKindSpec::ReplicaCommitAck => Self::ReplicaCommitAck,
            PeerKindSpec::ReplicaStatus => Self::ReplicaStatus,
            PeerKindSpec::ReplicaStatusReply => Self::ReplicaStatusReply,
            PeerKindSpec::ReplicaUploadDone => Self::ReplicaUploadDone,
            PeerKindSpec::ReplicaRelease => Self::ReplicaRelease,
            PeerKindSpec::ReplicaReleaseAck => Self::ReplicaReleaseAck,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrateAtSpec {
    at: DurationSpec,
    vset: CountSpec,
    to: HostSelector,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum CountSpec {
    Fixed(u64),
    Distribution { uniform: CountBounds },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CountBounds {
    min: u64,
    max: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum DurationSpec {
    Fixed(String),
    Distribution { uniform: DurationBounds },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurationBounds {
    min: String,
    max: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurationRange {
    min: DurationSpec,
    max: DurationSpec,
}

struct Realizer {
    seed: u64,
}

impl Realizer {
    fn rng(&self, path: &str) -> Pcg64 {
        let mut hash = Fnv64::new();
        hash.write(path.as_bytes());
        Pcg64::new(self.seed, hash.finish())
    }

    fn count(&self, spec: &CountSpec, path: &str) -> Result<u64, ScenarioError> {
        match spec {
            CountSpec::Fixed(value) => Ok(*value),
            CountSpec::Distribution { uniform } => {
                if uniform.min > uniform.max {
                    return Err(ScenarioError::new(format!(
                        "{path}: uniform minimum exceeds maximum"
                    )));
                }
                Ok(self.rng(path).range(uniform.min, uniform.max))
            }
        }
    }

    fn duration(&self, spec: &DurationSpec, path: &str) -> Result<u64, ScenarioError> {
        match spec {
            DurationSpec::Fixed(value) => parse_duration(value)
                .map_err(|error| ScenarioError::new(format!("{path}: {error}"))),
            DurationSpec::Distribution { uniform } => {
                let min = parse_duration(&uniform.min)
                    .map_err(|error| ScenarioError::new(format!("{path}.min: {error}")))?;
                let max = parse_duration(&uniform.max)
                    .map_err(|error| ScenarioError::new(format!("{path}.max: {error}")))?;
                if min > max {
                    return Err(ScenarioError::new(format!(
                        "{path}: uniform minimum exceeds maximum"
                    )));
                }
                Ok(self.rng(path).range(min, max))
            }
        }
    }

    fn duration_range(
        &self,
        range: &DurationRange,
        path: &str,
    ) -> Result<(u64, u64), ScenarioError> {
        let min = self.duration(&range.min, &format!("{path}.min"))?;
        let max = self.duration(&range.max, &format!("{path}.max"))?;
        if min > max {
            return Err(ScenarioError::new(format!(
                "{path}: minimum exceeds maximum"
            )));
        }
        Ok((min, max))
    }

    fn optional_duration_or_zero(
        &self,
        value: Option<&DurationSpec>,
        path: &str,
    ) -> Result<u64, ScenarioError> {
        value.map_or(Ok(0), |value| self.duration(value, path))
    }

    fn window(&self, window: &WindowSpec, path: &str) -> Result<(u64, u64), ScenarioError> {
        let start = self.duration(&window.start, &format!("{path}.start"))?;
        let end = self.duration(&window.end, &format!("{path}.end"))?;
        if start >= end {
            return Err(ScenarioError::new(format!(
                "{path}: window start must be before end"
            )));
        }
        Ok((start, end))
    }

    fn ratio(&self, ratio: Option<&RatioSpec>, path: &str) -> Result<(u64, u64), ScenarioError> {
        let Some(ratio) = ratio else {
            return Ok((0, 1));
        };
        let numerator = self.count(&ratio.numerator, &format!("{path}.numerator"))?;
        let denominator = self.count(&ratio.denominator, &format!("{path}.denominator"))?;
        if denominator == 0 || numerator > denominator {
            return Err(ScenarioError::new(format!(
                "{path}: ratio must satisfy numerator <= denominator and denominator > 0"
            )));
        }
        Ok((numerator, denominator))
    }

    fn ppm(&self, value: &CountSpec, path: &str) -> Result<Ppm, ScenarioError> {
        let value = self.count(value, path)?;
        let value = u32::try_from(value)
            .map_err(|_| ScenarioError::new(format!("{path}: value does not fit u32")))?;
        if value > 1_000_000 {
            return Err(ScenarioError::new(format!(
                "{path}: probability exceeds 1,000,000 ppm"
            )));
        }
        Ok(Ppm(value))
    }

    fn u8(&self, value: &CountSpec, path: &str) -> Result<u8, ScenarioError> {
        u8::try_from(self.count(value, path)?)
            .map_err(|_| ScenarioError::new(format!("{path}: value does not fit u8")))
    }

    fn u16(&self, value: &CountSpec, path: &str) -> Result<u16, ScenarioError> {
        u16::try_from(self.count(value, path)?)
            .map_err(|_| ScenarioError::new(format!("{path}: value does not fit u16")))
    }

    fn u32(&self, value: &CountSpec, path: &str) -> Result<u32, ScenarioError> {
        u32::try_from(self.count(value, path)?)
            .map_err(|_| ScenarioError::new(format!("{path}: value does not fit u32")))
    }

    fn usize(&self, value: &CountSpec, path: &str) -> Result<usize, ScenarioError> {
        usize::try_from(self.count(value, path)?)
            .map_err(|_| ScenarioError::new(format!("{path}: value does not fit usize")))
    }
}

fn parse_duration(value: &str) -> Result<u64, String> {
    let units = [
        ("ns", 1_u64),
        ("us", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
    ];
    for (suffix, multiplier) in units {
        if let Some(number) = value.strip_suffix(suffix) {
            let number = number
                .parse::<u64>()
                .map_err(|_| format!("invalid duration {value:?}"))?;
            return number
                .checked_mul(multiplier)
                .ok_or_else(|| format!("duration {value:?} overflows nanoseconds"));
        }
    }
    Err(format!(
        "invalid duration {value:?}; expected an integer followed by ns, us, ms, or s"
    ))
}

fn resolve_host(
    config: &ClusterConfig,
    r: &Realizer,
    selector: &HostSelector,
    path: &str,
) -> Result<u16, ScenarioError> {
    let host = match selector {
        HostSelector::Id(value) => r.u16(value, path)?,
        HostSelector::StashRank { stash_rank } => {
            let rank = r.usize(stash_rank, &format!("{path}.stash-rank"))?;
            let placement = config.daemon.replica_placement.as_ref().ok_or_else(|| {
                ScenarioError::new(format!("{path}: stash-rank requires replica placement"))
            })?;
            rank_stash_candidates(
                placement.membership_epoch,
                config.daemon.host,
                placement.local_failure_domain,
                VsetId(1),
                &placement.roster,
            )
            .get(rank)
            .ok_or_else(|| ScenarioError::new(format!("{path}: stash rank {rank} is unavailable")))?
            .0
        }
    };
    if host >= config.hosts {
        return Err(ScenarioError::new(format!(
            "{path}: host {host} is outside 0..{}",
            config.hosts
        )));
    }
    Ok(host)
}

struct Common {
    daemon: DaemonConfig,
    bdev: BlobDevConfig,
    store: StoreConfig,
    vset_count: u16,
    vset_config: VsetConfig,
    horizon: u64,
    think: (u64, u64),
    checkpoint_interval: Option<u64>,
    guest_sync_share: Option<Ppm>,
    guest_hot_pages: Option<(Ppm, u32)>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn every_checked_in_scenario_composes_and_realizes() {
        for name in names() {
            let scenario = load(name).unwrap_or_else(|error| panic!("{name}: {error}"));
            let realized = scenario
                .realize(42)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(scenario.kind(), realized.kind());
            assert!(scenario.sources().iter().any(|source| source == name));
        }
    }

    #[test]
    fn composition_keeps_the_complete_source_chain() {
        let scenario = load("peer-rare").expect("peer rare scenario");
        assert_eq!(
            scenario.sources(),
            ["cluster", "peer-stash", "peer-attrition", "peer-rare"]
        );
        assert!(
            scenario
                .resolved_specification()
                .contains("replica-retry-timer")
        );
    }

    #[test]
    fn exploratory_distributions_are_deterministic_bounded_and_path_independent() {
        let scenario = load("explore").expect("explore scenario");
        let RealizedScenario::Cluster(first) = scenario.realize(91).expect("first realization")
        else {
            panic!("cluster scenario expected");
        };
        let RealizedScenario::Cluster(replay) = scenario.realize(91).expect("replay realization")
        else {
            panic!("cluster scenario expected");
        };
        assert_eq!(format!("{first:#?}"), format!("{replay:#?}"));
        assert!((3..=5).contains(&first.hosts));
        assert!((3..=6).contains(&first.vset_count));
        assert!((64..=512).contains(&first.daemon.cache_pages));
        assert!((8..=24).contains(&first.vset_config.pages_per_volume));
        let RealizedScenario::Cluster(other) = scenario.realize(92).expect("other realization")
        else {
            panic!("cluster scenario expected");
        };
        assert_ne!(format!("{first:#?}"), format!("{other:#?}"));

        // A field's draw is keyed by its path; drawing another field first
        // cannot perturb it.
        let r = Realizer { seed: 17 };
        let distributed = CountSpec::Distribution {
            uniform: CountBounds { min: 10, max: 99 },
        };
        let before = r.count(&distributed, "stable.field").expect("draw");
        let _ = r.count(&distributed, "unrelated.field").expect("draw");
        let after = r.count(&distributed, "stable.field").expect("draw");
        assert_eq!(before, after);
    }

    #[test]
    fn duration_units_and_validation_are_strict() {
        assert_eq!(parse_duration("7ns"), Ok(7));
        assert_eq!(parse_duration("7us"), Ok(7_000));
        assert_eq!(parse_duration("7ms"), Ok(7_000_000));
        assert_eq!(parse_duration("7s"), Ok(7_000_000_000));
        assert!(parse_duration("7").is_err());
        assert!(parse_duration("1.5s").is_err());
    }

    #[test]
    fn legacy_scenarios_keep_their_pinned_trace_hashes() {
        let single = [
            ("single-host-base", 31, 0xc66e_fe12_8d12_ae4d),
            ("chaos", 31, 0xdefc_978a_ccfa_8305),
        ];
        for (name, seed, expected) in single {
            let RealizedScenario::SingleHost(config) = load(name)
                .expect("scenario")
                .realize(seed)
                .expect("realization")
            else {
                panic!("single-host scenario expected");
            };
            assert_eq!(
                crate::harness::run(seed, config).trace_hash,
                expected,
                "{name}"
            );
        }
        let cluster = [
            ("cluster", 31, 0xf119_1b3b_8711_7800),
            ("migration", 31, 0x1470_b38b_7804_2d54),
            ("peer-stash", 73, 0x1d6c_a9fa_636c_356f),
            ("peer-attrition", 117, 0x0cd0_49b5_c11e_181c),
            ("peer-links", 119, 0x5fc6_fbda_1c97_c786),
            ("peer-rare", 127, 0xd033_a366_c992_9f0a),
            ("placement-fear", 149, 0xe41b_6399_58c3_6d44),
        ];
        for (name, seed, expected) in cluster {
            let RealizedScenario::Cluster(config) = load(name)
                .expect("scenario")
                .realize(seed)
                .expect("realization")
            else {
                panic!("cluster scenario expected");
            };
            assert_eq!(
                crate::cluster::run(seed, config).trace_hash,
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn sweep_catalog_has_no_duplicates_and_only_known_scenarios() {
        let unique: BTreeSet<_> = SWEEP_SCENARIOS.iter().copied().collect();
        assert_eq!(unique.len(), SWEEP_SCENARIOS.len());
        for name in SWEEP_SCENARIOS {
            assert!(load(name).is_ok(), "unknown sweep scenario {name}");
        }
    }
}
