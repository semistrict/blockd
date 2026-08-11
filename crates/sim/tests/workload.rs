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
    config.vset_config = VsetConfig::compute(spec.shape.disk_volumes, spec.shape.pages_per_volume);
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
    assert_eq!(first.simulation.completed_ops, first.workload.completed);
    assert_eq!(
        first.simulation.per_guest_completed.get(&1),
        Some(&first.workload.completed)
    );
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

#[test]
fn scripted_workload_honors_fault_schedules_and_horizon() {
    let spec = blockd_workload::load("steady-io").expect("workload");
    let mut scheduled = config_for(&spec);
    scheduled.rot_records_at = vec![(0, false)];
    let report = run_workload(0x54_54, scheduled, spec.clone()).expect("scheduled run");
    assert_eq!(report.simulation.bitflips, 1);

    let mut bounded = config_for(&spec);
    bounded.horizon = millis(1);
    bounded.think = (millis(2), millis(2));
    let error = run_workload(0x55_55, bounded, spec).expect_err("horizon must stop the script");
    assert!(error.contains("did not finish before the simulation horizon"));
}
