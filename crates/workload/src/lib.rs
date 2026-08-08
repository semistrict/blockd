//! Pure, declarative workloads shared by modeled and real execution backends.
//!
//! This crate owns no storage, clocks, threads, or metrics. It expands a
//! checked-in specification into a deterministic operation stream and keeps a
//! canonical logical page model. Backends retain their native lifecycle and
//! I/O machinery; comparable correctness comes from the common outcome hash.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

const DOCUMENTS: &[(&str, &str)] = &[
    ("steady-io", include_str!("../specs/steady-io.json")),
    (
        "checkpoint-recovery",
        include_str!("../specs/checkpoint-recovery.json"),
    ),
    ("migration", include_str!("../specs/migration.json")),
    (
        "memory-snapshot",
        include_str!("../specs/memory-snapshot.json"),
    ),
    (
        "decider-throughput",
        include_str!("../specs/decider-throughput.json"),
    ),
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadError(String);

impl WorkloadError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for WorkloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WorkloadError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VolumeSet {
    Memory,
    Disk,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VerifyScope {
    Memory,
    Disk,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VsetShape {
    pub disk_volumes: u8,
    pub pages_per_volume: u32,
    pub backed_up: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HotSet {
    pub share_ppm: u32,
    pub pages: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessPattern {
    pub volumes: VolumeSet,
    pub hot: Option<HotSet>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Phase {
    Create,
    Run {
        operations: u64,
        write_ppm: u32,
        sync_every: Option<u64>,
        access: AccessPattern,
    },
    Checkpoint,
    Migrate {
        to_host: u16,
    },
    Crash,
    Restore,
    Verify {
        scope: VerifyScope,
    },
    Fork {
        copies: u16,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    pub schema: u32,
    pub name: String,
    pub seed: u64,
    pub shape: VsetShape,
    pub phases: Vec<Phase>,
}

impl WorkloadSpec {
    pub fn validate(&self) -> Result<(), WorkloadError> {
        if self.schema != 1 {
            return Err(WorkloadError::new(format!(
                "workload {}: unsupported schema {}",
                self.name, self.schema
            )));
        }
        if self.name.is_empty() {
            return Err(WorkloadError::new("workload name must not be empty"));
        }
        if self.shape.disk_volumes == 0 || self.shape.pages_per_volume == 0 {
            return Err(WorkloadError::new(format!(
                "workload {}: vset shape must contain disk volumes and pages",
                self.name
            )));
        }
        if !matches!(self.phases.first(), Some(Phase::Create)) {
            return Err(WorkloadError::new(format!(
                "workload {}: first phase must be create",
                self.name
            )));
        }
        if self.phases.len() < 2 {
            return Err(WorkloadError::new(format!(
                "workload {}: no work follows create",
                self.name
            )));
        }
        for (index, phase) in self.phases.iter().enumerate() {
            match phase {
                Phase::Create if index != 0 => {
                    return Err(WorkloadError::new(format!(
                        "workload {}: create may appear only once",
                        self.name
                    )));
                }
                Phase::Run {
                    operations,
                    write_ppm,
                    sync_every,
                    access,
                } => {
                    if *operations == 0 || *write_ppm > 1_000_000 {
                        return Err(WorkloadError::new(format!(
                            "workload {}: run {index} has invalid operation count or write share",
                            self.name
                        )));
                    }
                    if sync_every.is_some_and(|every| every == 0) {
                        return Err(WorkloadError::new(format!(
                            "workload {}: run {index} has a zero sync cadence",
                            self.name
                        )));
                    }
                    if let Some(hot) = access.hot
                        && (hot.share_ppm > 1_000_000
                            || hot.pages == 0
                            || hot.pages > self.shape.pages_per_volume
                            || (hot.share_ppm < 1_000_000
                                && hot.pages == self.shape.pages_per_volume))
                    {
                        return Err(WorkloadError::new(format!(
                            "workload {}: run {index} has an invalid hot set",
                            self.name
                        )));
                    }
                }
                Phase::Fork { copies: 0 } => {
                    return Err(WorkloadError::new(format!(
                        "workload {}: fork count must be positive",
                        self.name
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Load one checked-in workload by stable name.
pub fn load(name: &str) -> Result<WorkloadSpec, WorkloadError> {
    let text = DOCUMENTS
        .iter()
        .find_map(|(candidate, text)| (*candidate == name).then_some(*text))
        .ok_or_else(|| WorkloadError::new(format!("unknown workload {name}")))?;
    let spec: WorkloadSpec = serde_json::from_str(text)
        .map_err(|error| WorkloadError::new(format!("workload {name}: {error}")))?;
    if spec.name != name {
        return Err(WorkloadError::new(format!(
            "workload catalog key {name} does not match document name {}",
            spec.name
        )));
    }
    spec.validate()?;
    Ok(spec)
}

pub fn names() -> impl Iterator<Item = &'static str> {
    DOCUMENTS.iter().map(|(name, _)| *name)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogicalPage {
    pub volume: u8,
    pub page: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Create,
    Read { page: LogicalPage },
    Write { page: LogicalPage, value: u64 },
    Sync { volume: u8 },
    Checkpoint,
    Migrate { to_host: u16 },
    Crash,
    Restore,
    Verify { scope: VerifyScope },
    Fork { copies: u16 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    Create,
    Data,
    Sync,
    Checkpoint,
    Migrate,
    Crash,
    Restore,
    Verify,
    Fork,
}

impl Operation {
    pub fn capability(self) -> Capability {
        match self {
            Self::Create => Capability::Create,
            Self::Read { .. } | Self::Write { .. } => Capability::Data,
            Self::Sync { .. } => Capability::Sync,
            Self::Checkpoint => Capability::Checkpoint,
            Self::Migrate { .. } => Capability::Migrate,
            Self::Crash => Capability::Crash,
            Self::Restore => Capability::Restore,
            Self::Verify { .. } => Capability::Verify,
            Self::Fork { .. } => Capability::Fork,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Program {
    spec: WorkloadSpec,
    phase: usize,
    run_index: u64,
    pending_sync: bool,
    writes: u64,
    rng: Lcg,
}

impl Program {
    pub fn new(spec: WorkloadSpec) -> Result<Self, WorkloadError> {
        spec.validate()?;
        Ok(Self {
            rng: Lcg(spec.seed.max(1)),
            spec,
            phase: 0,
            run_index: 0,
            pending_sync: false,
            writes: 0,
        })
    }

    pub fn spec(&self) -> &WorkloadSpec {
        &self.spec
    }

    fn logical_page(&mut self, access: AccessPattern) -> LogicalPage {
        let volume = match access.volumes {
            VolumeSet::Memory => 0,
            VolumeSet::Disk => {
                u8::try_from(self.rng.below(u64::from(self.spec.shape.disk_volumes)) + 1)
                    .expect("disk volume fits u8")
            }
            VolumeSet::All => {
                u8::try_from(self.rng.below(u64::from(self.spec.shape.disk_volumes) + 1))
                    .expect("volume fits u8")
            }
        };
        let page = match access.hot {
            Some(hot) if self.rng.hit(hot.share_ppm) => {
                u32::try_from(self.rng.below(u64::from(hot.pages))).expect("page fits u32")
            }
            Some(hot) => {
                hot.pages
                    + u32::try_from(
                        self.rng
                            .below(u64::from(self.spec.shape.pages_per_volume - hot.pages)),
                    )
                    .expect("page fits u32")
            }
            None => u32::try_from(self.rng.below(u64::from(self.spec.shape.pages_per_volume)))
                .expect("page fits u32"),
        };
        LogicalPage { volume, page }
    }
}

impl Iterator for Program {
    type Item = Operation;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let phase = self.spec.phases.get(self.phase)?.clone();
            match phase {
                Phase::Run {
                    operations,
                    write_ppm,
                    sync_every,
                    access,
                } => {
                    if self.pending_sync {
                        self.pending_sync = false;
                        let volume = u8::try_from(
                            self.rng.below(u64::from(self.spec.shape.disk_volumes)) + 1,
                        )
                        .expect("disk volume fits u8");
                        return Some(Operation::Sync { volume });
                    }
                    if self.run_index == operations {
                        self.phase += 1;
                        self.run_index = 0;
                        continue;
                    }
                    let page = self.logical_page(access);
                    let write = self.rng.hit(write_ppm);
                    self.run_index += 1;
                    if sync_every.is_some_and(|every| self.run_index.is_multiple_of(every)) {
                        self.pending_sync = true;
                    }
                    if write {
                        let value = 0x1000_0000_u64 + self.writes;
                        self.writes += 1;
                        return Some(Operation::Write { page, value });
                    }
                    return Some(Operation::Read { page });
                }
                Phase::Create => {
                    self.phase += 1;
                    return Some(Operation::Create);
                }
                Phase::Checkpoint => {
                    self.phase += 1;
                    return Some(Operation::Checkpoint);
                }
                Phase::Migrate { to_host } => {
                    self.phase += 1;
                    return Some(Operation::Migrate { to_host });
                }
                Phase::Crash => {
                    self.phase += 1;
                    return Some(Operation::Crash);
                }
                Phase::Restore => {
                    self.phase += 1;
                    return Some(Operation::Restore);
                }
                Phase::Verify { scope } => {
                    self.phase += 1;
                    return Some(Operation::Verify { scope });
                }
                Phase::Fork { copies } => {
                    self.phase += 1;
                    return Some(Operation::Fork { copies });
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn below(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }

    fn hit(&mut self, ppm: u32) -> bool {
        self.below(1_000_000) < u64::from(ppm)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadModel {
    shape: VsetShape,
    pages: BTreeMap<LogicalPage, u64>,
    completed: u64,
    reads: u64,
    writes: u64,
    syncs: u64,
    checkpoints: u64,
    migrations: u64,
    crashes: u64,
    restores: u64,
    verifications: u64,
    forks: u64,
}

impl WorkloadModel {
    pub fn new(shape: VsetShape) -> Self {
        Self {
            shape,
            pages: BTreeMap::new(),
            completed: 0,
            reads: 0,
            writes: 0,
            syncs: 0,
            checkpoints: 0,
            migrations: 0,
            crashes: 0,
            restores: 0,
            verifications: 0,
            forks: 0,
        }
    }

    pub fn shape(&self) -> VsetShape {
        self.shape
    }

    pub fn expected(&self, page: LogicalPage) -> u64 {
        self.pages.get(&page).copied().unwrap_or(0)
    }

    pub fn pages(&self, scope: VerifyScope) -> impl Iterator<Item = (LogicalPage, u64)> + '_ {
        let volumes = match scope {
            VerifyScope::Memory => 0..=0,
            VerifyScope::Disk => 1..=self.shape.disk_volumes,
            VerifyScope::All => 0..=self.shape.disk_volumes,
        };
        volumes.flat_map(move |volume| {
            (0..self.shape.pages_per_volume).map(move |page| {
                let logical = LogicalPage { volume, page };
                (logical, self.expected(logical))
            })
        })
    }

    /// Record an operation after a backend has completed it successfully.
    ///
    /// Event-driven adapters use this directly because their operations
    /// complete asynchronously rather than inside [`run`].
    pub fn complete(&mut self, operation: Operation) {
        self.completed += 1;
        match operation {
            Operation::Create => {}
            Operation::Read { .. } => self.reads += 1,
            Operation::Write { page, value } => {
                self.writes += 1;
                self.pages.insert(page, value);
            }
            Operation::Sync { .. } => self.syncs += 1,
            Operation::Checkpoint => self.checkpoints += 1,
            Operation::Migrate { .. } => self.migrations += 1,
            Operation::Crash => self.crashes += 1,
            Operation::Restore => self.restores += 1,
            Operation::Verify { .. } => self.verifications += 1,
            Operation::Fork { copies } => self.forks += u64::from(copies),
        }
    }

    pub fn outcome(&self, name: &str) -> WorkloadOutcome {
        WorkloadOutcome {
            name: name.to_owned(),
            completed: self.completed,
            reads: self.reads,
            writes: self.writes,
            syncs: self.syncs,
            checkpoints: self.checkpoints,
            migrations: self.migrations,
            crashes: self.crashes,
            restores: self.restores,
            verifications: self.verifications,
            forks: self.forks,
            all_pages_hash: self.hash(VerifyScope::All),
            disk_pages_hash: self.hash(VerifyScope::Disk),
        }
    }

    fn hash(&self, scope: VerifyScope) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for (page, value) in self.pages(scope) {
            for number in [u64::from(page.volume), u64::from(page.page), value] {
                hash ^= number;
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        hash
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WorkloadOutcome {
    pub name: String,
    pub completed: u64,
    pub reads: u64,
    pub writes: u64,
    pub syncs: u64,
    pub checkpoints: u64,
    pub migrations: u64,
    pub crashes: u64,
    pub restores: u64,
    pub verifications: u64,
    pub forks: u64,
    pub all_pages_hash: u64,
    pub disk_pages_hash: u64,
}

pub trait Backend {
    type Error;

    fn supports(&self, _capability: Capability) -> bool {
        true
    }

    fn execute(&mut self, operation: Operation, model: &WorkloadModel) -> Result<(), Self::Error>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum RunError<E> {
    Invalid(WorkloadError),
    Unsupported(Capability),
    Backend(E),
}

pub fn run<B: Backend>(
    spec: &WorkloadSpec,
    backend: &mut B,
) -> Result<WorkloadOutcome, RunError<B::Error>> {
    spec.validate().map_err(RunError::Invalid)?;
    let program = Program::new(spec.clone()).map_err(RunError::Invalid)?;
    let mut model = WorkloadModel::new(spec.shape);
    for operation in program {
        let capability = operation.capability();
        if !backend.supports(capability) {
            return Err(RunError::Unsupported(capability));
        }
        backend
            .execute(operation, &model)
            .map_err(RunError::Backend)?;
        model.complete(operation);
    }
    Ok(model.outcome(&spec.name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct ReferenceBackend {
        pages: BTreeMap<LogicalPage, u64>,
    }

    impl Backend for ReferenceBackend {
        type Error = String;

        fn execute(
            &mut self,
            operation: Operation,
            model: &WorkloadModel,
        ) -> Result<(), Self::Error> {
            match operation {
                Operation::Read { page } => {
                    if self.pages.get(&page).copied().unwrap_or(0) != model.expected(page) {
                        return Err(format!("read mismatch at {page:?}"));
                    }
                }
                Operation::Write { page, value } => {
                    self.pages.insert(page, value);
                }
                Operation::Verify { scope } => {
                    for (page, expected) in model.pages(scope) {
                        if self.pages.get(&page).copied().unwrap_or(0) != expected {
                            return Err(format!("verify mismatch at {page:?}"));
                        }
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }

    #[test]
    fn every_checked_in_workload_validates_and_replays() {
        for name in names() {
            let spec = load(name).unwrap_or_else(|error| panic!("{name}: {error}"));
            let first = run(&spec, &mut ReferenceBackend::default()).expect("first run");
            let replay = run(&spec, &mut ReferenceBackend::default()).expect("replay");
            assert_eq!(first, replay, "{name}");
        }
    }

    #[test]
    fn steady_io_operation_stream_and_outcome_are_pinned() {
        let spec = load("steady-io").expect("steady workload");
        let operations: Vec<_> = Program::new(spec.clone())
            .expect("program")
            .take(8)
            .collect();
        assert_eq!(
            operations,
            [
                Operation::Create,
                Operation::Write {
                    page: LogicalPage { volume: 1, page: 1 },
                    value: 0x1000_0000,
                },
                Operation::Write {
                    page: LogicalPage {
                        volume: 2,
                        page: 14
                    },
                    value: 0x1000_0001,
                },
                Operation::Write {
                    page: LogicalPage {
                        volume: 1,
                        page: 15
                    },
                    value: 0x1000_0002,
                },
                Operation::Write {
                    page: LogicalPage { volume: 2, page: 4 },
                    value: 0x1000_0003,
                },
                Operation::Write {
                    page: LogicalPage {
                        volume: 1,
                        page: 13
                    },
                    value: 0x1000_0004,
                },
                Operation::Write {
                    page: LogicalPage {
                        volume: 2,
                        page: 10
                    },
                    value: 0x1000_0005,
                },
                Operation::Write {
                    page: LogicalPage {
                        volume: 1,
                        page: 11
                    },
                    value: 0x1000_0006,
                },
            ]
        );
        let outcome = run(&spec, &mut ReferenceBackend::default()).expect("run");
        assert_eq!(outcome.completed, 206);
        assert_eq!(outcome.syncs, 4);
        assert_eq!(outcome.verifications, 1);
    }

    #[test]
    fn unsupported_capability_stops_before_executing_it() {
        struct DataOnly;
        impl Backend for DataOnly {
            type Error = ();

            fn supports(&self, capability: Capability) -> bool {
                matches!(
                    capability,
                    Capability::Create | Capability::Data | Capability::Sync
                )
            }

            fn execute(
                &mut self,
                _operation: Operation,
                _model: &WorkloadModel,
            ) -> Result<(), Self::Error> {
                Ok(())
            }
        }
        let error = run(&load("steady-io").expect("spec"), &mut DataOnly).expect_err("verify");
        assert_eq!(error, RunError::Unsupported(Capability::Verify));
    }
}
