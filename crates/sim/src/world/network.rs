//! The cluster network: random latency (hence reordering), drops, duplicates,
//! and partitions. Generic over the message type; the harness schedules the
//! deliveries this component returns.

use std::collections::BTreeSet;

use blockd_core::types::{HostId, SimTime};

use crate::rng::{Pcg64, Ppm};

#[derive(Clone, Debug)]
pub struct NetworkConfig {
    pub latency_min: u64,
    pub latency_max: u64,
    pub drop: Ppm,
    pub duplicate: Ppm,
}

impl NetworkConfig {
    /// A quiet network: fixed latency, no faults.
    pub fn faultless(latency: u64) -> NetworkConfig {
        NetworkConfig {
            latency_min: latency,
            latency_max: latency,
            drop: Ppm::NEVER,
            duplicate: Ppm::NEVER,
        }
    }
}

/// A message due for delivery at `at`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivery<M> {
    pub at: SimTime,
    pub from: HostId,
    pub to: HostId,
    pub msg: M,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetworkCounters {
    pub sent: u64,
    pub delivered_copies: u64,
    pub dropped: u64,
    pub duplicated: u64,
    pub partitioned: u64,
}

pub struct Network {
    config: NetworkConfig,
    /// Unordered host pairs that cannot currently talk, stored (lo, hi).
    cut: BTreeSet<(HostId, HostId)>,
    pub counters: NetworkCounters,
}

fn pair(a: HostId, b: HostId) -> (HostId, HostId) {
    if a <= b { (a, b) } else { (b, a) }
}

impl Network {
    pub fn new(config: NetworkConfig) -> Network {
        assert!(config.latency_min > 0 && config.latency_min <= config.latency_max);
        Network {
            config,
            cut: BTreeSet::new(),
            counters: NetworkCounters::default(),
        }
    }

    /// Sever the link between two hosts (symmetric).
    pub fn partition(&mut self, a: HostId, b: HostId) {
        self.cut.insert(pair(a, b));
    }

    /// Restore the link between two hosts.
    pub fn heal(&mut self, a: HostId, b: HostId) {
        self.cut.remove(&pair(a, b));
    }

    pub fn heal_all(&mut self) {
        self.cut.clear();
    }

    pub fn is_cut(&self, a: HostId, b: HostId) -> bool {
        self.cut.contains(&pair(a, b))
    }

    /// Send a message: returns the copies to deliver (empty if dropped or
    /// partitioned, two if duplicated). Cloning `M` only happens on duplication.
    pub fn send<M: Clone>(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        from: HostId,
        to: HostId,
        msg: M,
    ) -> Vec<Delivery<M>> {
        self.counters.sent += 1;
        if self.is_cut(from, to) {
            self.counters.partitioned += 1;
            return Vec::new();
        }
        if rng.hit(self.config.drop) {
            self.counters.dropped += 1;
            return Vec::new();
        }
        let copies = if rng.hit(self.config.duplicate) {
            self.counters.duplicated += 1;
            2
        } else {
            1
        };
        let mut out = Vec::with_capacity(copies);
        for _ in 0..copies {
            let latency = rng.range(self.config.latency_min, self.config.latency_max);
            self.counters.delivered_copies += 1;
            out.push(Delivery {
                at: now.after(latency),
                from,
                to,
                msg: msg.clone(),
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockd_core::types::millis;

    const T0: SimTime = SimTime::ZERO;

    fn rng() -> Pcg64 {
        Pcg64::new(0x000b_10cd, 0)
    }

    #[test]
    fn partition_blocks_and_heal_restores() {
        let mut net = Network::new(NetworkConfig::faultless(millis(1)));
        let mut rng = rng();
        let (a, b) = (HostId(0), HostId(1));

        net.partition(a, b);
        assert!(net.is_cut(b, a), "partitions are symmetric");
        assert_eq!(net.send(T0, &mut rng, a, b, "hello"), vec![]);
        assert_eq!(net.counters.partitioned, 1);

        net.heal(a, b);
        let deliveries = net.send(T0, &mut rng, a, b, "hello");
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].at, T0.after(millis(1)));
        assert_eq!(deliveries[0].msg, "hello");
    }

    #[test]
    fn random_latency_reorders_messages() {
        let mut net = Network::new(NetworkConfig {
            latency_min: 1,
            latency_max: millis(10),
            drop: Ppm::NEVER,
            duplicate: Ppm::NEVER,
        });
        let mut rng = rng();
        let times: Vec<SimTime> = (0..20)
            .map(|n| net.send(T0, &mut rng, HostId(0), HostId(1), n)[0].at)
            .collect();
        let mut sorted = times.clone();
        sorted.sort_unstable();
        assert_ne!(times, sorted, "expected at least one reordered delivery");
        assert!(times.iter().all(|t| *t > T0 && *t <= T0.after(millis(10))));
    }

    #[test]
    fn drops_and_duplicates_hit_their_counters() {
        let mut net = Network::new(NetworkConfig {
            latency_min: 1,
            latency_max: millis(1),
            drop: Ppm::percent(20),
            duplicate: Ppm::percent(20),
        });
        let mut rng = rng();
        let copies: usize = (0..1_000)
            .map(|n| net.send(T0, &mut rng, HostId(0), HostId(1), n).len())
            .sum();
        assert_eq!(net.counters.sent, 1_000);
        assert_eq!(net.counters.dropped, 197);
        assert_eq!(net.counters.duplicated, 170);
        assert_eq!(net.counters.delivered_copies, 973);
        assert_eq!(copies, 973);
    }
}
