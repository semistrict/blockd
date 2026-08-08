use blockd_core::journal::VsetConfig;
use blockd_core::types::{micros, millis};
use blockd_sim::harness::{FaultPlan, HarnessConfig, run_workload};
use blockd_workload::{Backend, Operation, WorkloadModel, WorkloadSpec};

struct ReferenceBackend;

impl Backend for ReferenceBackend {
    type Error = std::convert::Infallible;

    fn execute(
        &mut self,
        _operation: Operation,
        _model: &WorkloadModel,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn config_for(spec: &WorkloadSpec) -> HarnessConfig {
    let mut config = blockd_sim::presets::single_host_base();
    config.vset_count = 1;
    config.backed_vsets = u16::from(spec.shape.backed_up);
    config.vset_config = VsetConfig::compute(
        spec.shape.disk_volumes,
        spec.shape.pages_per_volume,
        spec.shape.backed_up,
    );
    config.horizon = millis(10_000);
    config.think = (micros(1), micros(2));
    config.checkpoint_interval = None;
    config.faults = FaultPlan::none();
    config.rot_records_at.clear();
    config.crash_at.clear();
    config
}

#[test]
fn steady_io_matches_the_reference_outcome_and_replays() {
    let spec = blockd_workload::load("steady-io").expect("workload");
    let expected = blockd_workload::run(&spec, &mut ReferenceBackend).expect("reference run");
    let first = run_workload(0x51_51, config_for(&spec), spec.clone()).expect("simulated run");
    let replay = run_workload(0x51_51, config_for(&spec), spec).expect("simulated replay");

    assert!(first.simulation.violations.is_empty());
    assert_eq!(first.workload, expected);
    assert_eq!(replay.workload, expected);
    assert_eq!(first.simulation.trace_hash, replay.simulation.trace_hash);
}

#[test]
fn checkpoint_crash_restore_matches_the_reference_outcome() {
    let spec = blockd_workload::load("checkpoint-recovery").expect("workload");
    let expected = blockd_workload::run(&spec, &mut ReferenceBackend).expect("reference run");
    let actual = run_workload(0x52_52, config_for(&spec), spec).expect("simulated run");

    assert!(actual.simulation.violations.is_empty());
    assert_eq!(actual.workload, expected);
    assert_eq!(actual.simulation.crashes, 1);
    assert_eq!(actual.simulation.resumes, 1);
    assert_eq!(actual.simulation.cold_boots, 0);
}
