//! The nemesis plans faults; the harness applies them. Keeping the planner
//! separate from the wiring means every harness — from a two-host test to the
//! long-haul suite — draws from the same fault vocabulary.

use blockd_core::types::{HostId, SimTime};

use crate::rng::Pcg64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultAction {
    Partition(HostId, HostId),
    Heal(HostId, HostId),
    HealAll,
    CrashHost(HostId),
    RestartHost(HostId),
    StoreOutageBegin,
    StoreOutageEnd,
    CorruptDisk(HostId),
}

/// Relative weights for each fault kind; zero disables a kind.
#[derive(Clone, Copy, Debug)]
pub struct FaultWeights {
    pub partition: u32,
    pub heal: u32,
    pub heal_all: u32,
    pub crash_host: u32,
    pub restart_host: u32,
    pub store_outage: u32,
    pub corrupt_disk: u32,
}

impl FaultWeights {
    pub fn none() -> FaultWeights {
        FaultWeights {
            partition: 0,
            heal: 0,
            heal_all: 0,
            crash_host: 0,
            restart_host: 0,
            store_outage: 0,
            corrupt_disk: 0,
        }
    }

    /// Everything enabled, restarts favored so crashed hosts come back.
    pub fn heavy() -> FaultWeights {
        FaultWeights {
            partition: 3,
            heal: 3,
            heal_all: 1,
            crash_host: 2,
            restart_host: 4,
            store_outage: 2,
            corrupt_disk: 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NemesisConfig {
    pub hosts: u16,
    /// Mean nanoseconds between fault actions (actual gaps are uniform in
    /// `[1, 2 * mean]`).
    pub mean_interval: u64,
    pub weights: FaultWeights,
}

pub struct Nemesis {
    config: NemesisConfig,
    store_out: bool,
}

impl Nemesis {
    pub fn new(config: NemesisConfig) -> Nemesis {
        assert!(config.hosts >= 1);
        assert!(config.mean_interval >= 1);
        Nemesis {
            config,
            store_out: false,
        }
    }

    fn random_host(&self, rng: &mut Pcg64) -> HostId {
        HostId(u16::try_from(rng.below(u64::from(self.config.hosts))).unwrap())
    }

    fn random_pair(&self, rng: &mut Pcg64) -> Option<(HostId, HostId)> {
        if self.config.hosts < 2 {
            return None;
        }
        let a = self.random_host(rng);
        let step = rng.range(1, u64::from(self.config.hosts) - 1);
        let b =
            HostId(u16::try_from((u64::from(a.0) + step) % u64::from(self.config.hosts)).unwrap());
        Some((a, b))
    }

    /// Plan the next fault: when it fires and what it is. The store-outage
    /// weight toggles between begin and end so outages always eventually lift.
    pub fn next(&mut self, now: SimTime, rng: &mut Pcg64) -> (SimTime, FaultAction) {
        let at = now.after(rng.range(1, 2 * self.config.mean_interval));
        let w = self.config.weights;
        let choices: [(u32, u8); 7] = [
            (w.partition, 0),
            (w.heal, 1),
            (w.heal_all, 2),
            (w.crash_host, 3),
            (w.restart_host, 4),
            (w.store_outage, 5),
            (w.corrupt_disk, 6),
        ];
        let total: u64 = choices.iter().map(|(w, _)| u64::from(*w)).sum();
        assert!(total > 0, "nemesis running with all weights zero");
        let mut draw = rng.below(total);
        let kind = choices
            .iter()
            .find(|(w, _)| {
                if draw < u64::from(*w) {
                    true
                } else {
                    draw -= u64::from(*w);
                    false
                }
            })
            .map(|(_, k)| *k)
            .expect("draw < total");
        let action = match kind {
            0 => match self.random_pair(rng) {
                Some((a, b)) => FaultAction::Partition(a, b),
                None => FaultAction::HealAll,
            },
            1 => match self.random_pair(rng) {
                Some((a, b)) => FaultAction::Heal(a, b),
                None => FaultAction::HealAll,
            },
            2 => FaultAction::HealAll,
            3 => FaultAction::CrashHost(self.random_host(rng)),
            4 => FaultAction::RestartHost(self.random_host(rng)),
            5 => {
                self.store_out = !self.store_out;
                if self.store_out {
                    FaultAction::StoreOutageBegin
                } else {
                    FaultAction::StoreOutageEnd
                }
            }
            _ => FaultAction::CorruptDisk(self.random_host(rng)),
        };
        (at, action)
    }
}
