//! Stable single-host simulation API backed by deterministic async actors.

use std::collections::BTreeMap;

use blockd_core::hostmeta::{Counters, HostConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::types::millis;
use blockd_workload::{WorkloadOutcome, WorkloadSpec};

use crate::world::blobdev::BlobDevConfig;
use crate::world::store::{StoreConfig, StoreCounters};

#[derive(Clone, Debug)]
pub struct FaultPlan {
    pub crash_mean_interval: u64,
    pub restart_delay: (u64, u64),
    pub bitflip_mean_interval: u64,
    pub journal_bitflip_mean_interval: u64,
    pub store_outage: Option<(u64, u64)>,
}

impl FaultPlan {
    pub fn none() -> Self {
        Self {
            crash_mean_interval: 0,
            restart_delay: (millis(10), millis(500)),
            bitflip_mean_interval: 0,
            journal_bitflip_mean_interval: 0,
            store_outage: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sabotage {
    CorruptFill,
    DropWriteProtect,
    EagerHandoffAck,
    RogueRelease,
}

#[derive(Clone, Debug)]
pub struct HarnessConfig {
    pub daemon: HostConfig,
    pub bdev: BlobDevConfig,
    pub store: StoreConfig,
    pub vset_count: u16,
    pub vset_config: VsetConfig,
    pub horizon: u64,
    pub think: (u64, u64),
    pub checkpoint_interval: Option<u64>,
    pub faults: FaultPlan,
    pub sabotage: Option<Sabotage>,
    pub guest_sync_share: Option<crate::rng::Ppm>,
    pub guest_hot_pages: Option<(crate::rng::Ppm, u32)>,
    pub rot_records_at: Vec<(u64, bool)>,
    pub crash_at: Vec<u64>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RunReport {
    pub trace_hash: u64,
    pub violations: Vec<String>,
    pub counters: Counters,
    pub completed_ops: u64,
    pub per_guest_completed: BTreeMap<u64, u64>,
    pub crashes: u64,
    pub resumes: u64,
    pub cold_boots: u64,
    pub unrestorable: u64,
    pub guest_deaths: u64,
    pub bitflips: u64,
    pub blob_count: usize,
    pub max_pause_ns: u64,
    pub restores: u64,
    pub store_keys: Vec<String>,
    /// Submission- and completion-level object-store accounting, including
    /// object kind, CAS outcomes, retries, and size distribution.
    pub store: StoreCounters,
    /// Physical bytes in segment objects reachable from the final archived
    /// heads, split into live entries, dead entries, and framing overhead.
    pub published_segment_bytes: u64,
    pub published_live_entry_bytes: u64,
    pub published_dead_entry_bytes: u64,
    pub published_segment_overhead_bytes: u64,
    /// Final passive-spool occupancy and the two durability frontiers.
    pub replica_spool_bytes: u64,
    pub max_replica_spool_bytes: u64,
    pub peer_committed_through: u64,
    pub archived_through: u64,
    pub archive_lag_bytes: u64,
    /// Total bytes of page-map metadata written locally across the run —
    /// journal records and map leaves. The cost of remembering where pages
    /// live must track the DELTA, not the vset size.
    pub map_bytes_written: u64,
    pub max_step_page_reads: u64,
    pub max_record_blob_bytes: u64,
    pub seg_bytes_end: u64,
    pub seg_live_bytes_end: u64,
    pub parked_end: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct WorkloadRunReport {
    pub simulation: RunReport,
    pub workload: WorkloadOutcome,
}

#[allow(clippy::needless_pass_by_value)]
pub fn run(seed: u64, config: HarnessConfig) -> RunReport {
    run_actor(seed, &config).0
}

#[allow(clippy::needless_pass_by_value)]
pub fn run_workload(
    seed: u64,
    config: HarnessConfig,
    spec: WorkloadSpec,
) -> Result<WorkloadRunReport, String> {
    spec.validate().map_err(|error| error.to_string())?;
    if config.vset_count != 1 {
        return Err("scripted workloads require exactly one vset".to_owned());
    }
    if config.vset_config.disk_volumes != spec.shape.disk_volumes
        || config.vset_config.pages_per_volume != spec.shape.pages_per_volume
    {
        return Err("simulator vset shape does not match workload shape".to_owned());
    }
    let (simulation, workload) =
        crate::actor_harness::run_workload(seed, actor_config(&config), spec)?;
    Ok(WorkloadRunReport {
        simulation: compat_report(simulation),
        workload,
    })
}

#[allow(clippy::needless_pass_by_value)]
pub fn run_final_blobs(seed: u64, config: HarnessConfig) -> (RunReport, Vec<(String, Vec<u8>)>) {
    run_actor(seed, &config)
}

fn run_actor(seed: u64, config: &HarnessConfig) -> (RunReport, Vec<(String, Vec<u8>)>) {
    let (actor, blobs) = crate::actor_harness::run_final_blobs(seed, actor_config(config));
    (compat_report(actor), blobs)
}

fn actor_config(config: &HarnessConfig) -> crate::actor_harness::ActorHarnessConfig {
    crate::actor_harness::ActorHarnessConfig {
        host: config.daemon.clone(),
        blobs: config.bdev,
        store: config.store,
        vset_count: config.vset_count,
        vset: config.vset_config,
        horizon: config.horizon,
        think: config.think,
        sync_share: config.guest_sync_share,
        hot_pages: config.guest_hot_pages,
        checkpoint_interval: config.checkpoint_interval,
        faults: crate::actor_harness::ActorFaultPlan {
            crash_mean_interval: config.faults.crash_mean_interval,
            crash_at: config.crash_at.clone(),
            restart_delay: config.faults.restart_delay,
            store_outage: config.faults.store_outage,
            bitflip_mean_interval: config.faults.bitflip_mean_interval,
            journal_bitflip_mean_interval: config.faults.journal_bitflip_mean_interval,
            rot_records_at: config.rot_records_at.clone(),
        },
        corrupt_fills: config.sabotage == Some(Sabotage::CorruptFill),
        drop_write_protect: config.sabotage == Some(Sabotage::DropWriteProtect),
    }
}

fn compat_report(actor: crate::actor_harness::ActorRunReport) -> RunReport {
    RunReport {
        trace_hash: actor.trace_hash,
        violations: actor.violations,
        counters: actor.counters,
        completed_ops: actor.completed_ops,
        per_guest_completed: actor.per_guest_completed,
        crashes: actor.crashes,
        resumes: actor.resumes,
        cold_boots: actor.cold_boots,
        unrestorable: actor.unrestorable,
        guest_deaths: actor.guest_deaths,
        bitflips: actor.bitflips,
        blob_count: actor.blob_count,
        max_pause_ns: actor.max_pause_ns,
        restores: actor.restores,
        store_keys: actor.store_keys,
        store: StoreCounters::default(),
        published_segment_bytes: 0,
        published_live_entry_bytes: 0,
        published_dead_entry_bytes: 0,
        published_segment_overhead_bytes: 0,
        replica_spool_bytes: 0,
        max_replica_spool_bytes: 0,
        peer_committed_through: 0,
        archived_through: 0,
        archive_lag_bytes: 0,
        map_bytes_written: actor.map_bytes_written,
        max_step_page_reads: actor.max_page_reads_in_poll,
        max_record_blob_bytes: actor.max_record_blob_bytes,
        seg_bytes_end: actor.seg_bytes_end,
        seg_live_bytes_end: actor.seg_live_bytes_end,
        parked_end: actor.parked_end,
    }
}
