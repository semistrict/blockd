use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::journal::VsetConfig;
use blockd_core::protocol::Verdict;
use blockd_core::types::{PageId, PageNo, VolumeId, VolumeIdx, VsetId};
use blockd_runtime::{Runtime, RuntimeConfig, S3Store};
use blockd_workload::{Backend, Capability, LogicalPage, Operation, VerifyScope, WorkloadModel};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeWorkloadMetrics {
    pub creates: u64,
    pub reads: u64,
    pub writes: u64,
    pub syncs: u64,
    pub verifications: u64,
    pub checkpoints: u64,
    pub crashes: u64,
    pub restores: u64,
    pub create_time: Duration,
    pub read_time: Duration,
    pub write_time: Duration,
    pub sync_time: Duration,
    pub verify_time: Duration,
    pub checkpoint_time: Duration,
    pub restore_time: Duration,
}

pub struct RuntimeLifecycleBackend {
    config: RuntimeConfig,
    store: Arc<S3Store>,
    runtime: Option<Runtime>,
    vset: VsetId,
    vset_config: Option<VsetConfig>,
    metrics: RuntimeWorkloadMetrics,
}

impl RuntimeLifecycleBackend {
    pub fn new(config: RuntimeConfig, store: Arc<S3Store>, vset: VsetId) -> Self {
        Self {
            config,
            store,
            runtime: None,
            vset,
            vset_config: None,
            metrics: RuntimeWorkloadMetrics::default(),
        }
    }

    pub fn metrics(&self) -> &RuntimeWorkloadMetrics {
        &self.metrics
    }

    pub fn runtime(&self) -> &Runtime {
        self.runtime.as_ref().expect("runtime is active")
    }

    fn page_id(&self, page: LogicalPage) -> PageId {
        PageId {
            volume: VolumeId {
                vset: self.vset,
                idx: VolumeIdx(page.volume),
            },
            page: PageNo(page.page),
        }
    }

    fn verify_page(&self, page: LogicalPage, expected: u64) -> Result<(), String> {
        verify_runtime_page(
            self.runtime(),
            self.vset,
            self.page_id(page),
            page,
            expected,
        )
    }
}

impl Backend for RuntimeLifecycleBackend {
    type Error = String;

    fn supports(&self, capability: Capability) -> bool {
        matches!(
            capability,
            Capability::Create
                | Capability::Data
                | Capability::Sync
                | Capability::Checkpoint
                | Capability::Crash
                | Capability::Restore
                | Capability::Verify
        )
    }

    #[allow(clippy::too_many_lines)] // one arm per runtime/lifecycle operation
    fn execute(&mut self, operation: Operation, model: &WorkloadModel) -> Result<(), Self::Error> {
        match operation {
            Operation::Create => {
                let started = Instant::now();
                let shape = model.shape();
                let config = VsetConfig::compute(
                    shape.disk_volumes,
                    shape.pages_per_volume,
                    shape.backed_up,
                );
                let runtime = Runtime::new(&self.config, self.store.clone());
                runtime.create_vset(self.vset, config);
                self.vset_config = Some(config);
                self.runtime = Some(runtime);
                self.metrics.create_time += started.elapsed();
                self.metrics.creates += 1;
            }
            Operation::Read { page } => {
                let started = Instant::now();
                self.verify_page(page, model.expected(page))?;
                self.metrics.read_time += started.elapsed();
                self.metrics.reads += 1;
            }
            Operation::Write { page, value } => {
                let started = Instant::now();
                let page_id = self.page_id(page);
                self.runtime().guest_write(self.vset, page_id, value);
                self.metrics.write_time += started.elapsed();
                self.metrics.writes += 1;
            }
            Operation::Sync { volume } => {
                let started = Instant::now();
                if !self.runtime().guest_sync(self.vset, VolumeIdx(volume)) {
                    return Err(format!("sync rejected for volume {volume}"));
                }
                self.metrics.sync_time += started.elapsed();
                self.metrics.syncs += 1;
            }
            Operation::Checkpoint => {
                let started = Instant::now();
                let expected = self.metrics.checkpoints + 1;
                let epoch = self.runtime().checkpoint(self.vset);
                if epoch != expected {
                    return Err(format!("checkpoint epoch {epoch}, expected {expected}"));
                }
                self.metrics.checkpoint_time += started.elapsed();
                self.metrics.checkpoints += 1;
            }
            Operation::Crash => {
                drop(self.runtime.take().expect("runtime is active"));
                self.metrics.crashes += 1;
            }
            Operation::Restore => {
                let started = Instant::now();
                let config = self.vset_config.expect("vset was created");
                let (runtime, verdicts) = Runtime::recover(
                    &self.config,
                    self.store.clone(),
                    &BTreeMap::from([(self.vset, config)]),
                );
                if !matches!(verdicts.get(&self.vset), Some(Verdict::Resume { .. })) {
                    return Err(format!(
                        "restore did not resume the checkpoint: {:?}",
                        verdicts.get(&self.vset)
                    ));
                }
                self.runtime = Some(runtime);
                self.metrics.restore_time += started.elapsed();
                self.metrics.restores += 1;
            }
            Operation::Verify { scope } => {
                let started = Instant::now();
                for (page, expected) in model.pages(scope) {
                    self.verify_page(page, expected)?;
                }
                self.metrics.verify_time += started.elapsed();
                self.metrics.verifications += 1;
            }
            _ => unreachable!("capability checked before execution"),
        }
        Ok(())
    }
}

fn verify_runtime_page(
    runtime: &Runtime,
    vset: VsetId,
    page_id: PageId,
    logical: LogicalPage,
    expected: u64,
) -> Result<(), String> {
    let bytes = runtime.guest_read(vset, page_id);
    let observed = u64::from_le_bytes(
        bytes[0..8]
            .try_into()
            .map_err(|_| format!("short page at {logical:?}"))?,
    );
    if observed != expected {
        return Err(format!(
            "{logical:?}: observed word {observed:#x}, expected {expected:#x}"
        ));
    }
    if bytes[8..].iter().any(|&byte| byte != 0) {
        return Err(format!("{logical:?}: nonzero data after modeled word"));
    }
    Ok(())
}

pub struct RuntimeDataBackend<'a> {
    runtime: &'a Runtime,
    vset: VsetId,
    metrics: RuntimeWorkloadMetrics,
}

impl<'a> RuntimeDataBackend<'a> {
    pub fn new(runtime: &'a Runtime, vset: VsetId) -> Self {
        Self {
            runtime,
            vset,
            metrics: RuntimeWorkloadMetrics::default(),
        }
    }

    pub fn metrics(&self) -> &RuntimeWorkloadMetrics {
        &self.metrics
    }

    fn page_id(&self, page: LogicalPage) -> PageId {
        PageId {
            volume: VolumeId {
                vset: self.vset,
                idx: VolumeIdx(page.volume),
            },
            page: PageNo(page.page),
        }
    }

    fn verify_page(&self, page: LogicalPage, expected: u64) -> Result<(), String> {
        let bytes = self.runtime.guest_read(self.vset, self.page_id(page));
        let observed = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| format!("short page at {page:?}"))?,
        );
        if observed != expected {
            return Err(format!(
                "{page:?}: observed word {observed:#x}, expected {expected:#x}"
            ));
        }
        if bytes[8..].iter().any(|&byte| byte != 0) {
            return Err(format!("{page:?}: nonzero data after modeled word"));
        }
        Ok(())
    }
}

impl Backend for RuntimeDataBackend<'_> {
    type Error = String;

    fn supports(&self, capability: Capability) -> bool {
        matches!(
            capability,
            Capability::Create | Capability::Data | Capability::Sync | Capability::Verify
        )
    }

    fn execute(&mut self, operation: Operation, model: &WorkloadModel) -> Result<(), Self::Error> {
        match operation {
            Operation::Create => {
                let started = Instant::now();
                let shape = model.shape();
                self.runtime.create_vset(
                    self.vset,
                    VsetConfig::compute(
                        shape.disk_volumes,
                        shape.pages_per_volume,
                        shape.backed_up,
                    ),
                );
                self.metrics.create_time += started.elapsed();
                self.metrics.creates += 1;
            }
            Operation::Read { page } => {
                let started = Instant::now();
                self.verify_page(page, model.expected(page))?;
                self.metrics.read_time += started.elapsed();
                self.metrics.reads += 1;
            }
            Operation::Write { page, value } => {
                let started = Instant::now();
                self.runtime
                    .guest_write(self.vset, self.page_id(page), value);
                self.metrics.write_time += started.elapsed();
                self.metrics.writes += 1;
            }
            Operation::Sync { volume } => {
                let started = Instant::now();
                if !self.runtime.guest_sync(self.vset, VolumeIdx(volume)) {
                    return Err(format!("sync rejected for volume {volume}"));
                }
                self.metrics.sync_time += started.elapsed();
                self.metrics.syncs += 1;
            }
            Operation::Verify { scope } => {
                let started = Instant::now();
                self.verify(model, scope)?;
                self.metrics.verify_time += started.elapsed();
                self.metrics.verifications += 1;
            }
            _ => unreachable!("capability checked before execution"),
        }
        Ok(())
    }
}

impl RuntimeDataBackend<'_> {
    fn verify(&self, model: &WorkloadModel, scope: VerifyScope) -> Result<(), String> {
        for (page, expected) in model.pages(scope) {
            self.verify_page(page, expected)?;
        }
        Ok(())
    }
}
