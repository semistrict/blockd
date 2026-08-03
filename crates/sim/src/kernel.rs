//! The simulation kernel: one virtual clock, one ordered event queue, one
//! seeded RNG, one trace hash. Everything nondeterministic in a run flows
//! through this structure (R10.1); replaying the same seed and configuration
//! reproduces the identical event sequence, asserted via the trace hash.

use std::collections::BTreeMap;
use std::fmt;

use blockd_core::types::SimTime;

use crate::hash::TraceHasher;
use crate::rng::Pcg64;

pub struct Kernel<E> {
    now: SimTime,
    seq: u64,
    queue: BTreeMap<(SimTime, u64), E>,
    rng: Pcg64,
    trace: TraceHasher,
}

impl<E: fmt::Debug> Kernel<E> {
    pub fn new(seed: u64) -> Kernel<E> {
        Kernel {
            now: SimTime::ZERO,
            seq: 0,
            queue: BTreeMap::new(),
            rng: Pcg64::new(seed, 0),
            trace: TraceHasher::new(),
        }
    }

    pub fn now(&self) -> SimTime {
        self.now
    }

    pub fn rng(&mut self) -> &mut Pcg64 {
        &mut self.rng
    }

    /// Schedule an event at an absolute time, which must not be in the past.
    /// Events at the same instant fire in scheduling order.
    pub fn schedule_at(&mut self, at: SimTime, event: E) {
        assert!(
            at >= self.now,
            "scheduling into the past: {at:?} < {:?}",
            self.now
        );
        self.queue.insert((at, self.seq), event);
        self.seq += 1;
    }

    /// Schedule an event `nanos` after now.
    pub fn schedule_after(&mut self, nanos: u64, event: E) {
        self.schedule_at(self.now.after(nanos), event);
    }

    /// Advance the clock to the next event and return it. Every popped event
    /// is folded into the trace hash.
    pub fn pop(&mut self) -> Option<(SimTime, E)> {
        let (&key, _) = self.queue.iter().next()?;
        let event = self.queue.remove(&key).expect("key just observed");
        let (at, _) = key;
        self.now = at;
        self.trace.record(&(at, &event));
        Some((at, event))
    }

    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// Fold an arbitrary observation (an effect, an invariant sample) into
    /// the trace, so divergence is caught where it happens, not downstream.
    pub fn observe(&mut self, record: &dyn fmt::Debug) {
        self.trace.record(record);
    }

    pub fn trace_hash(&self) -> u64 {
        self.trace.finish()
    }

    pub fn trace_records(&self) -> u64 {
        self.trace.records()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Ppm;
    use blockd_core::types::millis;
    use std::collections::BTreeSet;

    #[derive(Debug)]
    enum ToyEvent {
        Message { to: u8, hops: u32 },
    }

    const NODES: u8 = 5;

    /// A gossip storm: each delivered message re-sends to two random peers
    /// with random latency, sometimes dropped, until its hop budget is spent.
    fn run_toy_world(seed: u64) -> (u64, u64) {
        let mut kernel: Kernel<ToyEvent> = Kernel::new(seed);
        for node in 0..NODES {
            kernel.schedule_at(
                SimTime::ZERO,
                ToyEvent::Message {
                    to: (node + 1) % NODES,
                    hops: 12,
                },
            );
        }
        while let Some((_, event)) = kernel.pop() {
            let ToyEvent::Message { to, hops } = event;
            if hops == 0 {
                continue;
            }
            for _ in 0..2 {
                if kernel.rng().hit(Ppm::percent(20)) {
                    continue; // dropped
                }
                let latency = kernel.rng().range(1, millis(5));
                let hop = u8::try_from(kernel.rng().range(1, u64::from(NODES) - 1)).unwrap();
                let next = ToyEvent::Message {
                    to: (to + hop) % NODES,
                    hops: hops - 1,
                };
                kernel.schedule_after(latency, next);
            }
        }
        (kernel.trace_hash(), kernel.trace_records())
    }

    #[test]
    fn same_seed_replays_byte_for_byte() {
        for seed in 0..100 {
            let (hash_a, records_a) = run_toy_world(seed);
            let (hash_b, records_b) = run_toy_world(seed);
            assert_eq!(hash_a, hash_b, "seed {seed} diverged on replay");
            assert_eq!(records_a, records_b, "seed {seed} record count diverged");
        }
    }

    #[test]
    fn distinct_seeds_produce_distinct_traces() {
        let hashes: BTreeSet<u64> = (0..100).map(|seed| run_toy_world(seed).0).collect();
        assert_eq!(hashes.len(), 100);
    }

    #[test]
    fn same_instant_events_fire_in_scheduling_order() {
        let mut kernel: Kernel<u32> = Kernel::new(0);
        let at = SimTime::ZERO.after(millis(1));
        for n in 0..10 {
            kernel.schedule_at(at, n);
        }
        let order: Vec<u32> = std::iter::from_fn(|| kernel.pop().map(|(_, e)| e)).collect();
        assert_eq!(order, (0..10).collect::<Vec<_>>());
        assert_eq!(kernel.now(), at);
    }

    #[test]
    fn pop_advances_the_clock_monotonically() {
        let mut kernel: Kernel<&str> = Kernel::new(9);
        kernel.schedule_after(millis(3), "late");
        kernel.schedule_after(millis(1), "early");
        let (t1, e1) = kernel.pop().unwrap();
        let (t2, e2) = kernel.pop().unwrap();
        assert_eq!((e1, e2), ("early", "late"));
        assert!(t1 < t2);
        assert_eq!(kernel.pending(), 0);
    }

    #[test]
    #[should_panic(expected = "scheduling into the past")]
    fn scheduling_into_the_past_panics() {
        let mut kernel: Kernel<&str> = Kernel::new(0);
        kernel.schedule_after(millis(2), "later");
        kernel.pop();
        kernel.schedule_at(SimTime::ZERO, "too late");
    }

    #[test]
    fn observations_are_part_of_the_trace() {
        let run = |observe: bool| {
            let mut kernel: Kernel<&str> = Kernel::new(4);
            kernel.schedule_after(1, "event");
            kernel.pop();
            if observe {
                kernel.observe(&"effect: wrote page");
            }
            kernel.trace_hash()
        };
        assert_ne!(run(true), run(false));
    }
}
