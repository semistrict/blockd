//! A stable, actionable summary of host pressure for the control plane.
//!
//! The daemon's individual gauges remain the source of truth for diagnosis.
//! This module turns them into a bounded recommendation: how much new work
//! the host should accept, and which optional background work should pause.
//! Escalation is immediate; recovery is deliberately slower so a value near
//! a threshold cannot make placement flap.

use std::time::Duration;

const RECOVERY_SAMPLES: u8 = 3;
const CONSTRAINED_ADMISSION_PERCENT: u8 = 25;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum CapacityState {
    #[default]
    Normal,
    Constrained,
    Critical,
}

impl CapacityState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Constrained => "constrained",
            Self::Critical => "critical",
        }
    }

    const fn lower(self) -> Self {
        match self {
            Self::Normal | Self::Constrained => Self::Normal,
            Self::Critical => Self::Constrained,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapacityReason {
    CachePressure,
    DirtyBacklog,
    DiskHeadroom,
    LocalIoSaturation,
    BackupLag,
    PeerSpoolCapacity,
    StashReplacement,
    EventLoopOccupancy,
}

impl CapacityReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CachePressure => "cache_pressure",
            Self::DirtyBacklog => "dirty_backlog",
            Self::DiskHeadroom => "disk_headroom",
            Self::LocalIoSaturation => "local_io_saturation",
            Self::BackupLag => "backup_lag",
            Self::PeerSpoolCapacity => "peer_spool_capacity",
            Self::StashReplacement => "stash_replacement",
            Self::EventLoopOccupancy => "event_loop_occupancy",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapacitySignal {
    pub state: CapacityState,
    pub limiting_reason: Option<CapacityReason>,
    /// Recommended share of the host's ordinary placement budget.
    pub admission_percent: u8,
    /// Optional work which should not compete with recovery or foreground I/O.
    pub allow_migrations: bool,
    pub allow_prefetch: bool,
}

impl Default for CapacitySignal {
    fn default() -> Self {
        Self::for_state(CapacityState::Normal, None)
    }
}

impl CapacitySignal {
    const fn for_state(state: CapacityState, limiting_reason: Option<CapacityReason>) -> Self {
        match state {
            CapacityState::Normal => Self {
                state,
                limiting_reason: None,
                admission_percent: 100,
                allow_migrations: true,
                allow_prefetch: true,
            },
            CapacityState::Constrained => Self {
                state,
                limiting_reason,
                admission_percent: CONSTRAINED_ADMISSION_PERCENT,
                allow_migrations: false,
                allow_prefetch: false,
            },
            CapacityState::Critical => Self {
                state,
                limiting_reason,
                admission_percent: 0,
                allow_migrations: false,
                allow_prefetch: false,
            },
        }
    }
}

/// One periodic observation. Cumulative loop times are converted to an
/// interval occupancy by [`CapacityController`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CapacityInputs {
    pub cache_capacity_pages: usize,
    pub cache_used_pages: usize,
    pub dirty_pages: usize,
    pub pressure_waiting_faults: usize,
    pub disk_used_bytes: u64,
    pub disk_capacity_bytes: Option<u64>,
    pub disk_headroom_bytes: u64,
    pub local_io_in_flight: u64,
    pub loop_busy_ns: u64,
    pub loop_idle_ns: u64,
    pub critical_queue_depth: usize,
    pub background_queue_depth: usize,
    pub oldest_backup_lag: Duration,
    pub peer_spool_used_bytes: u64,
    pub peer_spool_capacity_bytes: u64,
    pub stash_missing: bool,
    pub stash_replacement_active: bool,
}

#[derive(Debug, Default)]
pub struct CapacityController {
    signal: CapacitySignal,
    recovery_streak: u8,
    previous_loop_busy_ns: u64,
    previous_loop_idle_ns: u64,
}

impl CapacityController {
    pub fn signal(&self) -> CapacitySignal {
        self.signal
    }

    /// Incorporate one periodic sample. Higher pressure takes effect at once;
    /// lower pressure must persist and then recovers by one state at a time.
    pub fn observe(&mut self, inputs: CapacityInputs) -> CapacitySignal {
        let loop_busy_ns = inputs
            .loop_busy_ns
            .saturating_sub(self.previous_loop_busy_ns);
        let loop_idle_ns = inputs
            .loop_idle_ns
            .saturating_sub(self.previous_loop_idle_ns);
        self.previous_loop_busy_ns = inputs.loop_busy_ns;
        self.previous_loop_idle_ns = inputs.loop_idle_ns;

        let observed = classify(inputs, loop_busy_ns, loop_idle_ns);
        match observed.state.cmp(&self.signal.state) {
            std::cmp::Ordering::Greater => {
                self.signal = CapacitySignal::for_state(observed.state, observed.reason);
                self.recovery_streak = 0;
            }
            std::cmp::Ordering::Equal => {
                self.recovery_streak = 0;
                self.signal = CapacitySignal::for_state(observed.state, observed.reason);
            }
            std::cmp::Ordering::Less => {
                self.recovery_streak = self.recovery_streak.saturating_add(1);
                if self.recovery_streak >= RECOVERY_SAMPLES {
                    let state = self.signal.state.lower().max(observed.state);
                    let reason = if state == observed.state {
                        observed.reason
                    } else {
                        self.signal.limiting_reason
                    };
                    self.signal = CapacitySignal::for_state(state, reason);
                    self.recovery_streak = 0;
                }
            }
        }
        self.signal
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Observation {
    state: CapacityState,
    reason: Option<CapacityReason>,
}

impl Observation {
    fn consider(&mut self, state: CapacityState, reason: CapacityReason) {
        if state > self.state {
            self.state = state;
            self.reason = Some(reason);
        }
    }
}

fn classify(inputs: CapacityInputs, loop_busy_ns: u64, loop_idle_ns: u64) -> Observation {
    let mut observed = Observation::default();

    let cache = ratio_level(
        inputs.cache_used_pages as u128,
        inputs.cache_capacity_pages as u128,
        85,
        95,
    );
    observed.consider(cache, CapacityReason::CachePressure);
    if inputs.pressure_waiting_faults > 0 {
        observed.consider(CapacityState::Critical, CapacityReason::CachePressure);
    }

    let dirty = ratio_level(
        inputs.dirty_pages as u128,
        inputs.cache_capacity_pages as u128,
        50,
        75,
    );
    observed.consider(dirty, CapacityReason::DirtyBacklog);

    if let Some(capacity) = inputs.disk_capacity_bytes {
        let remaining = capacity.saturating_sub(inputs.disk_used_bytes);
        let disk = if remaining <= inputs.disk_headroom_bytes {
            CapacityState::Critical
        } else if inputs.disk_headroom_bytes > 0
            && remaining <= inputs.disk_headroom_bytes.saturating_mul(2)
        {
            CapacityState::Constrained
        } else {
            CapacityState::Normal
        };
        observed.consider(disk, CapacityReason::DiskHeadroom);
    }

    let local_io = match inputs.local_io_in_flight {
        64.. => CapacityState::Critical,
        16.. => CapacityState::Constrained,
        _ => CapacityState::Normal,
    };
    observed.consider(local_io, CapacityReason::LocalIoSaturation);

    let backup = if inputs.oldest_backup_lag >= Duration::from_mins(5) {
        CapacityState::Critical
    } else if inputs.oldest_backup_lag >= Duration::from_secs(30) {
        CapacityState::Constrained
    } else {
        CapacityState::Normal
    };
    observed.consider(backup, CapacityReason::BackupLag);

    let spool = ratio_level(
        inputs.peer_spool_used_bytes.into(),
        inputs.peer_spool_capacity_bytes.into(),
        80,
        95,
    );
    observed.consider(spool, CapacityReason::PeerSpoolCapacity);

    if inputs.stash_missing {
        observed.consider(CapacityState::Critical, CapacityReason::StashReplacement);
    } else if inputs.stash_replacement_active {
        observed.consider(CapacityState::Constrained, CapacityReason::StashReplacement);
    }

    let loop_total_ns = loop_busy_ns.saturating_add(loop_idle_ns);
    let loop_level = ratio_level(loop_busy_ns.into(), loop_total_ns.into(), 80, 95);
    let queue_level = if inputs.critical_queue_depth >= 64 || inputs.background_queue_depth >= 256 {
        CapacityState::Critical
    } else if inputs.critical_queue_depth >= 16 || inputs.background_queue_depth >= 64 {
        CapacityState::Constrained
    } else {
        CapacityState::Normal
    };
    observed.consider(
        loop_level.max(queue_level),
        CapacityReason::EventLoopOccupancy,
    );

    observed
}

fn ratio_level(
    numerator: u128,
    denominator: u128,
    constrained: u128,
    critical: u128,
) -> CapacityState {
    if denominator == 0 {
        return CapacityState::Normal;
    }
    let scaled = numerator.saturating_mul(100);
    if scaled >= denominator.saturating_mul(critical) {
        CapacityState::Critical
    } else if scaled >= denominator.saturating_mul(constrained) {
        CapacityState::Constrained
    } else {
        CapacityState::Normal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(percent: usize) -> CapacityInputs {
        CapacityInputs {
            cache_capacity_pages: 100,
            cache_used_pages: percent,
            ..CapacityInputs::default()
        }
    }

    #[test]
    fn escalates_immediately_and_recovers_with_hysteresis() {
        let mut controller = CapacityController::default();
        assert_eq!(
            controller.observe(cache(85)).state,
            CapacityState::Constrained
        );
        assert_eq!(controller.observe(cache(95)).state, CapacityState::Critical);

        for _ in 0..2 {
            assert_eq!(controller.observe(cache(0)).state, CapacityState::Critical);
        }
        assert_eq!(
            controller.observe(cache(0)).state,
            CapacityState::Constrained
        );
        for _ in 0..2 {
            assert_eq!(
                controller.observe(cache(0)).state,
                CapacityState::Constrained
            );
        }
        assert_eq!(controller.observe(cache(0)), CapacitySignal::default());
    }

    #[test]
    fn renewed_pressure_cancels_recovery() {
        let mut controller = CapacityController::default();
        controller.observe(cache(95));
        controller.observe(cache(0));
        controller.observe(cache(0));
        assert_eq!(controller.observe(cache(95)).state, CapacityState::Critical);
        for _ in 0..2 {
            assert_eq!(controller.observe(cache(0)).state, CapacityState::Critical);
        }
    }

    #[test]
    fn reports_a_deterministic_limiting_reason_and_budget() {
        let mut controller = CapacityController::default();
        let signal = controller.observe(CapacityInputs {
            cache_capacity_pages: 100,
            dirty_pages: 80,
            disk_capacity_bytes: Some(1_000),
            disk_used_bytes: 950,
            disk_headroom_bytes: 100,
            ..CapacityInputs::default()
        });
        assert_eq!(signal.state, CapacityState::Critical);
        assert_eq!(signal.limiting_reason, Some(CapacityReason::DirtyBacklog));
        assert_eq!(signal.admission_percent, 0);
        assert!(!signal.allow_migrations);
        assert!(!signal.allow_prefetch);
    }

    #[test]
    fn classifies_every_external_pressure_source() {
        let cases = [
            (
                CapacityInputs {
                    local_io_in_flight: 16,
                    ..CapacityInputs::default()
                },
                CapacityReason::LocalIoSaturation,
            ),
            (
                CapacityInputs {
                    oldest_backup_lag: Duration::from_secs(30),
                    ..CapacityInputs::default()
                },
                CapacityReason::BackupLag,
            ),
            (
                CapacityInputs {
                    peer_spool_used_bytes: 80,
                    peer_spool_capacity_bytes: 100,
                    ..CapacityInputs::default()
                },
                CapacityReason::PeerSpoolCapacity,
            ),
            (
                CapacityInputs {
                    stash_replacement_active: true,
                    ..CapacityInputs::default()
                },
                CapacityReason::StashReplacement,
            ),
            (
                CapacityInputs {
                    loop_busy_ns: 80,
                    loop_idle_ns: 20,
                    ..CapacityInputs::default()
                },
                CapacityReason::EventLoopOccupancy,
            ),
        ];
        for (input, reason) in cases {
            let mut controller = CapacityController::default();
            let signal = controller.observe(input);
            assert_eq!(signal.state, CapacityState::Constrained);
            assert_eq!(signal.limiting_reason, Some(reason));
        }
    }
}
