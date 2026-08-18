use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::journal::VolumeConfig;
use blockd_core::protocol::Verdict;
use blockd_core::types::{PageId, PageNo, VolumeId};
use blockd_runtime::{ObjectStore, Runtime, RuntimeConfig};
use blockd_workload::{Backend, Capability, LogicalPage, Operation, VerifyScope, WorkloadModel};
use futures_util::future::join_all;

fn logical_volume(root: VolumeId, index: u8) -> VolumeId {
    VolumeId(root.0 + u64::from(index))
}

fn volume_configs(
    root: VolumeId,
    disk_volumes: u8,
    pages: u32,
) -> BTreeMap<VolumeId, VolumeConfig> {
    (0..=disk_volumes)
        .map(|index| {
            let config = if index == 0 {
                VolumeConfig::memory(pages)
            } else {
                VolumeConfig::data(pages)
            };
            (logical_volume(root, index), config)
        })
        .collect()
}

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
    store: Arc<dyn ObjectStore>,
    runtime: Option<Runtime>,
    passive_runtimes: Vec<Runtime>,
    volume: VolumeId,
    volume_configs: BTreeMap<VolumeId, VolumeConfig>,
    metrics: RuntimeWorkloadMetrics,
}

impl RuntimeLifecycleBackend {
    pub fn new(
        config: RuntimeConfig,
        store: Arc<dyn ObjectStore>,
        passive_runtimes: Vec<Runtime>,
        volume: VolumeId,
    ) -> Self {
        Self {
            config,
            store,
            runtime: None,
            passive_runtimes,
            volume,
            volume_configs: BTreeMap::new(),
            metrics: RuntimeWorkloadMetrics::default(),
        }
    }

    pub fn metrics(&self) -> &RuntimeWorkloadMetrics {
        &self.metrics
    }

    pub fn runtime(&self) -> &Runtime {
        self.runtime.as_ref().expect("runtime is active")
    }

    pub fn passive_runtimes(&self) -> &[Runtime] {
        &self.passive_runtimes
    }

    fn page_id(&self, page: LogicalPage) -> PageId {
        PageId {
            volume: logical_volume(self.volume, page.volume),
            page: PageNo(page.page),
        }
    }

    async fn verify_page(&self, page: LogicalPage, expected: u64) -> Result<(), String> {
        verify_runtime_page(
            self.runtime(),
            self.page_id(page).volume,
            self.page_id(page),
            page,
            expected,
        )
        .await
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
    async fn execute(
        &mut self,
        operation: Operation,
        model: &WorkloadModel,
    ) -> Result<(), Self::Error> {
        match operation {
            Operation::Create => {
                let started = Instant::now();
                let shape = model.shape();
                let configs = volume_configs(self.volume, shape.disk_volumes, shape.pages);
                let runtime = Runtime::new(&self.config, self.store.clone()).await;
                for (&volume, &config) in &configs {
                    runtime.create_volume(volume, config).await;
                }
                self.volume_configs = configs;
                self.runtime = Some(runtime);
                self.metrics.create_time += started.elapsed();
                self.metrics.creates += 1;
            }
            Operation::Read { page } => {
                let started = Instant::now();
                self.verify_page(page, model.expected(page)).await?;
                self.metrics.read_time += started.elapsed();
                self.metrics.reads += 1;
            }
            Operation::Write { page, value } => {
                let started = Instant::now();
                let page_id = self.page_id(page);
                self.runtime()
                    .guest_write(page_id.volume, page_id, value)
                    .await;
                self.metrics.write_time += started.elapsed();
                self.metrics.writes += 1;
            }
            Operation::Sync { volume } => {
                let started = Instant::now();
                if !self
                    .runtime()
                    .guest_sync(logical_volume(self.volume, volume))
                    .await
                {
                    return Err(format!("sync rejected for volume {volume}"));
                }
                self.metrics.sync_time += started.elapsed();
                self.metrics.syncs += 1;
            }
            Operation::Checkpoint => {
                let started = Instant::now();
                let expected = self.metrics.checkpoints + 1;
                let epochs = join_all(
                    self.volume_configs
                        .keys()
                        .copied()
                        .map(|volume| self.runtime().checkpoint(volume)),
                )
                .await;
                if epochs.iter().any(|&epoch| epoch != expected) {
                    return Err(format!("checkpoint epochs {epochs:?}, expected {expected}"));
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
                let (runtime, immediate) =
                    Runtime::recover(&self.config, self.store.clone(), &self.volume_configs).await;
                if !immediate.is_empty() {
                    return Err(format!(
                        "recovery unexpectedly completed synchronously: {immediate:?}"
                    ));
                }
                for (&volume, config) in &self.volume_configs {
                    let verdict = runtime.wait_recovered(volume).await;
                    let expected = if config.is_memory() {
                        matches!(verdict, Verdict::Resume { .. })
                    } else {
                        verdict == Verdict::ColdBoot
                    };
                    if !expected {
                        return Err(format!(
                            "unexpected restore verdict for {volume:?}: {verdict:?}"
                        ));
                    }
                }
                self.runtime = Some(runtime);
                self.metrics.restore_time += started.elapsed();
                self.metrics.restores += 1;
            }
            Operation::Verify { scope } => {
                let started = Instant::now();
                for (page, expected) in model.pages(scope) {
                    self.verify_page(page, expected).await?;
                }
                self.metrics.verify_time += started.elapsed();
                self.metrics.verifications += 1;
            }
            _ => unreachable!("capability checked before execution"),
        }
        Ok(())
    }
}

async fn verify_runtime_page(
    runtime: &Runtime,
    volume: VolumeId,
    page_id: PageId,
    logical: LogicalPage,
    expected: u64,
) -> Result<(), String> {
    let bytes = runtime.guest_read(volume, page_id).await;
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
    volume: VolumeId,
    metrics: RuntimeWorkloadMetrics,
}

impl<'a> RuntimeDataBackend<'a> {
    pub fn new(runtime: &'a Runtime, volume: VolumeId) -> Self {
        Self {
            runtime,
            volume,
            metrics: RuntimeWorkloadMetrics::default(),
        }
    }

    pub fn metrics(&self) -> &RuntimeWorkloadMetrics {
        &self.metrics
    }

    fn page_id(&self, page: LogicalPage) -> PageId {
        PageId {
            volume: logical_volume(self.volume, page.volume),
            page: PageNo(page.page),
        }
    }

    async fn verify_page(&self, page: LogicalPage, expected: u64) -> Result<(), String> {
        let page_id = self.page_id(page);
        let bytes = self.runtime.guest_read(page_id.volume, page_id).await;
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

    async fn execute(
        &mut self,
        operation: Operation,
        model: &WorkloadModel,
    ) -> Result<(), Self::Error> {
        match operation {
            Operation::Create => {
                let started = Instant::now();
                let shape = model.shape();
                for (volume, config) in volume_configs(self.volume, shape.disk_volumes, shape.pages)
                {
                    self.runtime.create_volume(volume, config).await;
                }
                self.metrics.create_time += started.elapsed();
                self.metrics.creates += 1;
            }
            Operation::Read { page } => {
                let started = Instant::now();
                self.verify_page(page, model.expected(page)).await?;
                self.metrics.read_time += started.elapsed();
                self.metrics.reads += 1;
            }
            Operation::Write { page, value } => {
                let started = Instant::now();
                self.runtime
                    .guest_write(self.page_id(page).volume, self.page_id(page), value)
                    .await;
                self.metrics.write_time += started.elapsed();
                self.metrics.writes += 1;
            }
            Operation::Sync { volume } => {
                let started = Instant::now();
                if !self
                    .runtime
                    .guest_sync(logical_volume(self.volume, volume))
                    .await
                {
                    return Err(format!("sync rejected for volume {volume}"));
                }
                self.metrics.sync_time += started.elapsed();
                self.metrics.syncs += 1;
            }
            Operation::Verify { scope } => {
                let started = Instant::now();
                self.verify(model, scope).await?;
                self.metrics.verify_time += started.elapsed();
                self.metrics.verifications += 1;
            }
            _ => unreachable!("capability checked before execution"),
        }
        Ok(())
    }
}

impl RuntimeDataBackend<'_> {
    async fn verify(&self, model: &WorkloadModel, scope: VerifyScope) -> Result<(), String> {
        for (page, expected) in model.pages(scope) {
            self.verify_page(page, expected).await?;
        }
        Ok(())
    }
}
