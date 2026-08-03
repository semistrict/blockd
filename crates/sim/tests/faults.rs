//! Fault coverage: a mini-world wiring every world component to the nemesis,
//! proving that runs actually contain partitions, drops, duplicates, host
//! crashes with all three in-flight-write outcomes, bit rot, store outages
//! and CAS conflicts — so later invariant suites can't pass vacuously.
//! The counter snapshot is exact: the run is deterministic.

use blockd_core::format::seal_frame;
use blockd_core::types::{HostId, SimTime, millis};
use blockd_sim::kernel::Kernel;
use blockd_sim::nemesis::{FaultAction, FaultWeights, Nemesis, NemesisConfig};
use blockd_sim::rng::Ppm;
use blockd_sim::world::blobdev::{BdevIo, BlobDev, BlobDevConfig};
use blockd_sim::world::network::{Delivery, Network, NetworkConfig};
use blockd_sim::world::store::{ObjectStore, StoreConfig, Version};

const HOSTS: u16 = 4;
const TICKS: u32 = 2_000;
const MAGIC: u32 = 0x0B10_C0D1;

// Payload fields feed the trace hash through Debug; dead-code analysis
// deliberately ignores that.
#[allow(dead_code)]
#[derive(Debug)]
enum Ev {
    Deliver(Delivery<u64>),
    BdevDone { host: HostId, inc: u32, io: BdevIo },
    StoreOutcome { ok: bool },
    Fault(FaultAction),
    Tick(u32),
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Coverage {
    partitioned: u64,
    dropped: u64,
    duplicated: u64,
    crash_applied: u64,
    crash_dropped: u64,
    crash_torn: u64,
    bitflips: u64,
    cas_conflicts: u64,
    unavailable: u64,
    crashes: u64,
}

#[allow(clippy::too_many_lines)]
fn run(seed: u64) -> (u64, Coverage) {
    let mut kernel: Kernel<Ev> = Kernel::new(seed);
    let mut net = Network::new(NetworkConfig {
        latency_min: 10_000,
        latency_max: millis(2),
        drop: Ppm::percent(5),
        duplicate: Ppm::percent(5),
    });
    // Slow writes keep ios in flight across crashes, so all three crash
    // outcomes (applied / dropped / torn) actually occur.
    let dev_config = BlobDevConfig {
        read_latency_min: 30_000,
        read_latency_max: 200_000,
        write_latency_min: 100_000,
        write_latency_max: millis(2),
        ns_per_byte: 1,
    };
    let mut devs: Vec<BlobDev> = (0..HOSTS)
        .map(|_| BlobDev::new(dev_config.clone()))
        .collect();
    let mut incarnation = vec![0u32; usize::from(HOSTS)];
    let mut up = vec![true; usize::from(HOSTS)];
    let mut store = ObjectStore::new(StoreConfig::s3());
    let mut nemesis = Nemesis::new(NemesisConfig {
        hosts: HOSTS,
        mean_interval: millis(5),
        weights: FaultWeights::heavy(),
    });
    let mut crashes = 0;

    let horizon = SimTime(u64::from(TICKS) * millis(1));
    kernel.schedule_at(SimTime::ZERO, Ev::Tick(0));
    let (at, fault) = nemesis.next(SimTime::ZERO, kernel.rng());
    kernel.schedule_at(at, Ev::Fault(fault));

    while let Some((now, event)) = kernel.pop() {
        match event {
            // Delivered messages and store outcomes have no receiver yet;
            // they exist to appear in the trace.
            Ev::Deliver(_) | Ev::StoreOutcome { .. } => {}
            Ev::BdevDone { host, inc, io } => {
                if incarnation[usize::from(host.0)] == inc {
                    devs[usize::from(host.0)].complete_write(io);
                }
            }
            Ev::Fault(action) => {
                match action {
                    FaultAction::Partition(a, b) => net.partition(a, b),
                    FaultAction::Heal(a, b) => net.heal(a, b),
                    FaultAction::HealAll => net.heal_all(),
                    FaultAction::CrashHost(h) => {
                        if up[usize::from(h.0)] {
                            up[usize::from(h.0)] = false;
                            incarnation[usize::from(h.0)] += 1;
                            devs[usize::from(h.0)].crash(kernel.rng());
                            crashes += 1;
                        }
                    }
                    FaultAction::RestartHost(h) => up[usize::from(h.0)] = true,
                    FaultAction::StoreOutageBegin => store.set_outage(true),
                    FaultAction::StoreOutageEnd => store.set_outage(false),
                    FaultAction::CorruptDisk(h) => {
                        devs[usize::from(h.0)].flip_random_bit(kernel.rng());
                    }
                }
                if now < horizon {
                    let (fire_at, next_fault) = nemesis.next(now, kernel.rng());
                    kernel.schedule_at(fire_at, Ev::Fault(next_fault));
                }
            }
            Ev::Tick(n) => {
                // One message between random hosts.
                let from = HostId(u16::try_from(kernel.rng().below(u64::from(HOSTS))).unwrap());
                let to = HostId(u16::try_from(kernel.rng().below(u64::from(HOSTS))).unwrap());
                for d in net.send(now, kernel.rng(), from, to, u64::from(n)) {
                    kernel.schedule_at(d.at, Ev::Deliver(d));
                }
                // Framed blob writes on random up hosts, completions
                // scheduled; write-once names.
                for i in 0..4u32 {
                    let h = usize::try_from(kernel.rng().below(u64::from(HOSTS))).unwrap();
                    if up[h] {
                        let name = format!("v/{h}/j/{n:08x}-{i}.rec");
                        let payload = seal_frame(MAGIC, &n.to_le_bytes());
                        let (io, done) = devs[h].submit_write(now, kernel.rng(), name, payload);
                        kernel.schedule_at(
                            done,
                            Ev::BdevDone {
                                host: HostId(u16::try_from(h).unwrap()),
                                inc: incarnation[h],
                                io,
                            },
                        );
                    }
                }
                // One conditional head-record write with a guessed version,
                // so CAS conflicts occur naturally.
                let key = format!("v/{:016x}/head", kernel.rng().below(8));
                let expected = if kernel.rng().hit(Ppm::percent(50)) {
                    None
                } else {
                    Some(Version(kernel.rng().range(1, 4)))
                };
                let (at, outcome) =
                    store.put_cas(now, kernel.rng(), &key, expected, n.to_le_bytes().to_vec());
                kernel.schedule_at(
                    at,
                    Ev::StoreOutcome {
                        ok: outcome.is_ok(),
                    },
                );

                if n + 1 < TICKS {
                    kernel.schedule_at(now.after(millis(1)), Ev::Tick(n + 1));
                }
            }
        }
    }

    let dev_totals = devs.iter().fold((0, 0, 0, 0), |acc, d| {
        (
            acc.0 + d.counters.crash_applied,
            acc.1 + d.counters.crash_dropped,
            acc.2 + d.counters.crash_torn,
            acc.3 + d.counters.bitflips,
        )
    });
    let coverage = Coverage {
        partitioned: net.counters.partitioned,
        dropped: net.counters.dropped,
        duplicated: net.counters.duplicated,
        crash_applied: dev_totals.0,
        crash_dropped: dev_totals.1,
        crash_torn: dev_totals.2,
        bitflips: dev_totals.3,
        cas_conflicts: store.counters.cas_conflicts,
        unavailable: store.counters.unavailable,
        crashes,
    };
    (kernel.trace_hash(), coverage)
}

#[test]
fn heavy_faults_cover_every_injection_kind() {
    let (_, coverage) = run(1);
    assert_eq!(
        coverage,
        Coverage {
            partitioned: 428,
            dropped: 77,
            duplicated: 79,
            crash_applied: 14,
            crash_dropped: 16,
            crash_torn: 11,
            bitflips: 26,
            cas_conflicts: 1224,
            unavailable: 736,
            crashes: 34,
        }
    );
}

#[test]
fn fault_world_replays_byte_for_byte() {
    for seed in [1, 2, 3, 0xdead_beef] {
        let (hash_a, cov_a) = run(seed);
        let (hash_b, cov_b) = run(seed);
        assert_eq!(hash_a, hash_b, "seed {seed} trace diverged");
        assert_eq!(cov_a, cov_b, "seed {seed} coverage diverged");
    }
}
