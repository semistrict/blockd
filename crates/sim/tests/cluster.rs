//! Multi-host assignment protocol (R6.3/R6.4), driven directly: three real
//! daemons sharing one simulated object store, with store operations
//! completed synchronously. This pins the CAS semantics — exactly one
//! runner, structural fencing — without scheduling noise; the full
//! multi-host chaos harness builds on the same calls.

use std::collections::{BTreeMap, VecDeque};

use blockd_core::daemon::{Daemon, DaemonConfig};
use blockd_core::journal::{DurabilityMode, VsetConfig};
use blockd_core::layout;
use blockd_core::seam::{AdminCmd, AdminReply, Effect, Event, HostMap, ReqId, Verdict};
use blockd_core::types::{
    HostId, PageId, PageNo, SegId, SimTime, VolumeId, VolumeIdx, VsetId, page_size,
};
use blockd_sim::rng::Pcg64;
use blockd_sim::world::store::{ObjectStore, StoreConfig, StoreError, Version};

const VSET: VsetId = VsetId(1);

/// Deterministic page content for capture reads.
struct PatternMem;
impl HostMap for PatternMem {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        let mut bytes = vec![0u8; page_size()];
        bytes[0] = 0xA0 ^ page.volume.idx.0;
        bytes[1] = u8::try_from(page.page.0 & 0xFF).expect("fits");
        bytes
    }
}

fn config() -> VsetConfig {
    VsetConfig {
        disk_volumes: 1,
        pages_per_volume: 4,
        durability: DurabilityMode::Backup,
    }
}

fn daemon(host: u16) -> Daemon {
    Daemon::new(DaemonConfig {
        host: HostId(host),
        cache_pages: 16,
        writeback_interval: 1_000_000,
        backup_retry: 1_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: None,
    })
    .0
}

/// Drive one event to completion: every blob write and store operation the
/// daemon issues completes immediately (in order) against the shared store
/// and an always-successful local device. Returns the non-I/O effects.
fn settle(
    daemon: &mut Daemon,
    local: &mut BTreeMap<String, Vec<u8>>,
    store: &mut ObjectStore,
    event: Event,
) -> Vec<Effect> {
    let mut rng = Pcg64::new(9, 9);
    let now = SimTime::ZERO;
    let mut settled = Vec::new();
    let mut queue = VecDeque::from([event]);
    while let Some(event) = queue.pop_front() {
        for effect in daemon.step(event, &PatternMem) {
            match effect {
                Effect::BlobWrite { io, name, bytes } => {
                    local.insert(name, bytes);
                    queue.push_back(Event::BlobWriteDone { io });
                }
                Effect::BlobRead { io, name } => {
                    queue.push_back(Event::BlobReadDone {
                        io,
                        bytes: local.get(&name).cloned(),
                    });
                }
                Effect::BlobReadRange {
                    io,
                    name,
                    offset,
                    len,
                } => {
                    let bytes = local.get(&name).map(|blob| {
                        let start = usize::try_from(offset.min(blob.len() as u64)).expect("fits");
                        let end =
                            usize::try_from((offset + len).min(blob.len() as u64)).expect("fits");
                        blob[start..end].to_vec()
                    });
                    queue.push_back(Event::BlobReadDone { io, bytes });
                }
                Effect::BlobDelete { name } => {
                    local.remove(&name);
                }
                Effect::StorePut { io, key, bytes } => {
                    let (_, result) = store.put(now, &mut rng, &key, bytes);
                    queue.push_back(Event::StorePutDone {
                        io,
                        result: map_put(result),
                    });
                }
                Effect::StoreCas {
                    io,
                    key,
                    expected,
                    bytes,
                } => {
                    let (_, result) =
                        store.put_cas(now, &mut rng, &key, expected.map(Version), bytes);
                    queue.push_back(Event::StorePutDone {
                        io,
                        result: map_put(result),
                    });
                }
                Effect::StoreGet { io, key } => {
                    let (_, result) = store.get(now, &mut rng, &key);
                    let result = result.map(|found| found.map(|(v, b)| (v.0, b)));
                    queue.push_back(Event::StoreGetDone {
                        io,
                        result: result.map_err(|_| blockd_core::seam::StoreFault::Unavailable),
                    });
                }
                Effect::StoreGetRange {
                    io,
                    key,
                    offset,
                    len,
                } => {
                    let (_, result) = store.get_range(now, &mut rng, &key, offset, len);
                    let result = result.map(|found| found.map(|(v, b)| (v.0, b)));
                    queue.push_back(Event::StoreGetDone {
                        io,
                        result: result.map_err(|_| blockd_core::seam::StoreFault::Unavailable),
                    });
                }
                Effect::StoreDelete { key } => {
                    let _ = store.delete(now, &mut rng, &key);
                }
                other => settled.push(other),
            }
        }
    }
    settled
}

#[allow(clippy::needless_pass_by_value)]
fn map_put(result: Result<Version, StoreError>) -> Result<u64, blockd_core::seam::StoreFault> {
    match result {
        Ok(v) => Ok(v.0),
        Err(StoreError::CasConflict { actual }) => {
            Err(blockd_core::seam::StoreFault::CasConflict {
                actual: actual.map(|v| v.0),
            })
        }
        Err(_) => Err(blockd_core::seam::StoreFault::Unavailable),
    }
}

/// Create the vset on host A, dirty one page, checkpoint (so a whole
/// checkpoint manifest lands in the store), and return the settled world.
fn seeded_host_a(local: &mut BTreeMap<String, Vec<u8>>, store: &mut ObjectStore) -> Daemon {
    let mut a = daemon(0);
    let effects = settle(
        &mut a,
        local,
        store,
        Event::Admin(AdminCmd::CreateVset {
            req: ReqId(0),
            vset: VSET,
            config: config(),
            from_base: None,
        }),
    );
    assert_eq!(
        effects,
        [Effect::Admin(AdminReply::VsetCreated {
            req: ReqId(0),
            vset: VSET
        })]
    );
    // Dirty page (1,0): write-intent missing fault, then checkpoint.
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let effects = settle(
        &mut a,
        local,
        store,
        Event::GuestFault { page, write: true },
    );
    assert_eq!(
        effects,
        [Effect::Fill {
            page,
            bytes: vec![0; page_size()],
            writable: true,
            share: None
        }]
    );
    let effects = settle(
        &mut a,
        local,
        store,
        Event::Admin(AdminCmd::Checkpoint {
            req: ReqId(1),
            vset: VSET,
        }),
    );
    assert_eq!(effects, [Effect::PauseGuest { vset: VSET }]);
    let effects = settle(
        &mut a,
        local,
        store,
        Event::GuestPaused {
            vset: VSET,
            vmstate: 7,
        },
    );
    assert!(matches!(effects[0], Effect::WriteProtect { .. }));
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Admin(AdminReply::CheckpointDone { .. })))
    );
    assert_eq!(a.counters.manifests_published, 2, "creation + checkpoint");
    assert_eq!(a.backup_lag(VSET), Some(0));
    a
}

#[test]
#[allow(clippy::too_many_lines)]
fn racing_restores_resolve_to_exactly_one_runner_and_fence_the_old_holder() {
    let mut store = ObjectStore::new(StoreConfig::s3());
    let mut local_a = BTreeMap::new();
    let mut local_b = BTreeMap::new();
    let mut local_c = BTreeMap::new();
    let mut a = seeded_host_a(&mut local_a, &mut store);

    // Hosts B and C race to restore (a wrong liveness guess about A —
    // safe to attempt at any moment, R6.4). C reads the head first; B's
    // claim lands while C's is still in flight — a genuine race.
    let mut b = daemon(1);
    let mut c = daemon(2);
    let mut rng = Pcg64::new(3, 3);
    let now = SimTime::ZERO;
    let c_read = c.step(
        Event::Admin(AdminCmd::RestoreVset {
            req: ReqId(11),
            vset: VSET,
        }),
        &PatternMem,
    );
    let [Effect::StoreGet { io: c_io, key }] = c_read.as_slice() else {
        panic!("restore starts with a head read: {c_read:?}");
    };
    let (_, stale) = store.get(now, &mut rng, key);
    let stale = stale
        .map(|found| found.map(|(v, b)| (v.0, b)))
        .map_err(|_| blockd_core::seam::StoreFault::Unavailable);

    let b_effects = settle(
        &mut b,
        &mut local_b,
        &mut store,
        Event::Admin(AdminCmd::RestoreVset {
            req: ReqId(10),
            vset: VSET,
        }),
    );
    let c_effects = settle(
        &mut c,
        &mut local_c,
        &mut store,
        Event::StoreGetDone {
            io: *c_io,
            result: stale,
        },
    );
    // B claimed the head; C's claim carried the stale expectation and
    // CAS-failed: exactly one runner (R6.3).
    assert_eq!(
        b_effects,
        [
            Effect::Admin(AdminReply::VsetRestored {
                req: ReqId(10),
                vset: VSET,
                verdict: Verdict::Resume {
                    epoch: blockd_core::types::Epoch(1),
                    vmstate: 7
                }
            }),
            // The resume opens its R6.2 recording window (the resume-set
            // fetch itself was settled against the store).
            Effect::SetTimer {
                timer: blockd_core::seam::TimerId::ResumeSet(VSET),
                after: blockd_core::types::millis(1000),
            },
        ]
    );
    assert_eq!(
        c_effects,
        [Effect::Admin(AdminReply::AdminFailed { req: ReqId(11) })]
    );

    // A still runs its zombie guest (bounded double-run window). Its next
    // publish attempt CAS-fails and it is structurally fenced (R6.4).
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(1),
    };
    let _ = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::GuestFault { page, write: true },
    );
    let effects = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::Timer(blockd_core::seam::TimerId::Writeback),
    );
    assert!(
        effects.contains(&Effect::VsetFenced { vset: VSET }),
        "old holder must fence itself on the lost CAS: {effects:?}"
    );
    assert_eq!(a.counters.fenced, 1);
    assert_eq!(a.backup_lag(VSET), None, "the vset is gone from A");

    // B serves the restored page: fill comes from the store (A's segment,
    // verbatim bytes — R8.4/R2.3) after the local miss.
    let restored = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let effects = settle(
        &mut b,
        &mut local_b,
        &mut store,
        Event::GuestFault {
            page: restored,
            write: false,
        },
    );
    let mut expected = vec![0u8; page_size()];
    expected[0] = 0xA0 ^ 1;
    expected[1] = 0;
    assert_eq!(
        effects,
        [Effect::Fill {
            page: restored,
            bytes: expected,
            writable: false,
            share: None
        }]
    );
    assert_eq!(b.counters.fills, 1);

    // The head belongs to B; nothing A writes is reachable (R6.4).
    let now = SimTime::ZERO;
    let mut rng = Pcg64::new(1, 1);
    let (_, head) = store.get(now, &mut rng, &layout::head_key(VSET));
    let (_, bytes) = head.expect("store up").expect("head exists");
    let head = blockd_core::head::HeadRecord::decode(VSET, &bytes).expect("intact");
    assert_eq!(head.holder, HostId(1));
}

#[test]
#[allow(clippy::too_many_lines)]
fn bases_fork_in_o1_and_forks_pay_only_divergence() {
    // R5.1: fork is O(1) metadata. R5.3: the base is stored once; every
    // fork's untouched page reads the base's shared segments; divergence
    // lands in the fork's own namespace.
    let mut store = ObjectStore::new(StoreConfig::s3());
    let mut local_a = BTreeMap::new();
    let mut a = seeded_host_a(&mut local_a, &mut store);

    let effects = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::Admin(AdminCmd::KeepBase {
            req: ReqId(20),
            vset: VSET,
            base: 7,
        }),
    );
    assert_eq!(
        effects,
        [Effect::Admin(AdminReply::BaseKept {
            req: ReqId(20),
            base: 7
        })]
    );

    // Two forks, O(1) each: no segment bytes move (their manifests and
    // heads are metadata, proportional to page count, not data size).
    let mut rng = Pcg64::new(5, 5);
    let now = SimTime::ZERO;
    for (req, fork) in [(21u64, 100u64), (22, 101)] {
        let effects = settle(
            &mut a,
            &mut local_a,
            &mut store,
            Event::Admin(AdminCmd::CreateVset {
                req: ReqId(req),
                vset: VsetId(fork),
                config: config(),
                from_base: Some(7),
            }),
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::Admin(AdminReply::VsetForked {
                    verdict: Verdict::Resume { vmstate: 7, .. },
                    ..
                })
            )),
            "fork of a whole base resumes: {effects:?}"
        );
    }
    for fork_prefix in ["v/0000000000000064/s/", "v/0000000000000065/s/"] {
        let (_, segs) = store.list_prefix(now, &mut rng, fork_prefix);
        assert_eq!(
            segs.expect("store up"),
            Vec::<String>::new(),
            "forking copied zero segments (R5.1)"
        );
    }

    // Fork 100 faults the base page: bytes come from the base's shared
    // segment, identical to what the origin wrote.
    let fork_page = PageId {
        volume: VolumeId {
            vset: VsetId(100),
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let effects = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::GuestFault {
            page: fork_page,
            write: false,
        },
    );
    let mut expected = vec![0u8; page_size()];
    expected[0] = 0xA0 ^ 1;
    expected[1] = 0;
    assert_eq!(
        effects,
        [Effect::Fill {
            page: fork_page,
            bytes: expected,
            writable: false,
            // The base page enters the shared tier (R5.3).
            share: Some((7, 1, SegId(0), 46))
        }]
    );

    // Exactly one copy of the base segment exists, under the base prefix.
    let (_, base_segs) = store.list_prefix(now, &mut rng, "b/0000000000000007/s/");
    assert_eq!(base_segs.expect("store up").len(), 1);

    // Fork 101 writes a page (divergence), and writeback lands it in the
    // fork's own namespace — the base is untouched (immutable, R5.2).
    let diverge = PageId {
        volume: VolumeId {
            vset: VsetId(101),
            idx: VolumeIdx(1),
        },
        page: PageNo(1),
    };
    let _ = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::GuestFault {
            page: diverge,
            write: true,
        },
    );
    let _ = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::Timer(blockd_core::seam::TimerId::Writeback),
    );
    let (_, fork_segs) = store.list_prefix(now, &mut rng, "v/0000000000000065/s/");
    assert_eq!(
        fork_segs.expect("store up").len(),
        1,
        "divergence lives in the fork's namespace"
    );
    let (_, base_segs) = store.list_prefix(now, &mut rng, "b/0000000000000007/s/");
    assert_eq!(base_segs.expect("store up").len(), 1, "base untouched");
}

#[test]
fn two_hundred_forks_hold_one_resident_copy_of_each_base_page() {
    // R5.3, the fork-a-thousand-times contract at model scale: every fork
    // reads the base's pages, and the host holds exactly ONE physical copy
    // of each — forks pay only for what they write.
    let mut store = ObjectStore::new(StoreConfig::s3());
    let mut local_a = BTreeMap::new();
    let mut a = seeded_host_a(&mut local_a, &mut store);
    let effects = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::Admin(AdminCmd::KeepBase {
            req: ReqId(20),
            vset: VSET,
            base: 7,
        }),
    );
    assert_eq!(
        effects,
        [Effect::Admin(AdminReply::BaseKept {
            req: ReqId(20),
            base: 7
        })]
    );

    let fork_config = VsetConfig {
        durability: DurabilityMode::Local, // forks read shared data, write nothing (R4.4)
        ..config()
    };
    for (req, fork) in (100u64..).zip(0..200u64) {
        let effects = settle(
            &mut a,
            &mut local_a,
            &mut store,
            Event::Admin(AdminCmd::CreateVset {
                req: ReqId(req),
                vset: VsetId(1000 + fork),
                config: fork_config,
                from_base: Some(7),
            }),
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::Admin(AdminReply::VsetForked { .. }))),
            "fork {fork}: {effects:?}"
        );
    }

    // Every fork reads the same base page.
    let gets_before = store.counters.gets;
    let fills_before = a.counters.fills;
    for fork in 0..200u64 {
        let page = PageId {
            volume: VolumeId {
                vset: VsetId(1000 + fork),
                idx: VolumeIdx(1),
            },
            page: PageNo(0),
        };
        let effects = settle(
            &mut a,
            &mut local_a,
            &mut store,
            Event::GuestFault { page, write: false },
        );
        assert_eq!(effects.len(), 1, "fork {fork}: {effects:?}");
    }
    // One store fetch, one resident copy, 199 zero-copy shared maps.
    assert_eq!(store.counters.gets - gets_before, 1);
    assert_eq!(a.counters.fills - fills_before, 1);
    assert_eq!(a.counters.shared_fills, 199);
    assert_eq!(a.base_resident_pages(), 1);
    let private_before = a.resident_pages();

    // One fork diverges: a private copy-on-write page, base tier unchanged.
    let diverging = PageId {
        volume: VolumeId {
            vset: VsetId(1000),
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let effects = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::GuestFault {
            page: diverging,
            write: true,
        },
    );
    assert_eq!(
        effects,
        [Effect::FillShared {
            page: diverging,
            share: (7, 1, SegId(0), 46),
            writable: true
        }]
    );
    assert_eq!(a.resident_pages(), private_before + 1);
    assert_eq!(a.base_resident_pages(), 1, "the base copy is untouched");
    assert_eq!(a.counters.shared_fills, 200);
}

#[test]
#[allow(clippy::too_many_lines)]
fn gc_sweeps_only_unrooted_garbage_past_grace() {
    // R9.3: mark from heads and base records; sweep unreferenced objects
    // past the in-flight grace; a base is a root until its explicit delete.
    let mut store = ObjectStore::new(StoreConfig::s3());
    let mut local_a = BTreeMap::new();
    let mut a = seeded_host_a(&mut local_a, &mut store);
    let effects = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::Admin(AdminCmd::KeepBase {
            req: ReqId(20),
            vset: VSET,
            base: 7,
        }),
    );
    assert_eq!(
        effects,
        [Effect::Admin(AdminReply::BaseKept {
            req: ReqId(20),
            base: 7
        })]
    );

    // Inject garbage: a fenced loser's orphan manifest and segment, plus a
    // fresh in-flight segment that the grace must protect.
    let now = SimTime::ZERO;
    let mut rng = Pcg64::new(8, 8);
    let orphan_manifest = "v/0000000000000001/m/00000000000000aa-0000000000000009";
    let orphan_segment = "v/0000000000000001/s/00000000000000aa-0000000000000009";
    assert!(
        store
            .put(now, &mut rng, orphan_manifest, vec![1, 2, 3])
            .1
            .is_ok()
    );
    assert!(
        store
            .put(now, &mut rng, orphan_segment, vec![4, 5, 6])
            .1
            .is_ok()
    );
    let fresh_segment = "v/0000000000000001/s/00000000000000bb-0000000000000001";
    let later = SimTime(blockd_core::types::secs(100));
    assert!(store.put(later, &mut rng, fresh_segment, vec![7]).1.is_ok());

    // GC at t=100s with a 60s grace: the orphans (old) go, the fresh
    // segment (in grace) and everything rooted stays.
    let grace = blockd_core::types::secs(60);
    let doomed = blockd_core::gc::plan(later, grace, &store.snapshot());
    assert_eq!(
        doomed,
        [orphan_manifest.to_owned(), orphan_segment.to_owned()]
    );
    for key in doomed {
        let (_, deleted) = store.delete(later, &mut rng, &key);
        assert_eq!(deleted, Ok(true));
    }

    // Everything still referenced survives a second pass.
    let doomed = blockd_core::gc::plan(later, grace, &store.snapshot());
    assert_eq!(doomed, Vec::<String>::new());

    // Explicit base delete unroots it (R4.5): the admin op deletes the
    // record itself; the next sweep reclaims its segments.
    let effects = settle(
        &mut a,
        &mut local_a,
        &mut store,
        Event::Admin(AdminCmd::DeleteBase {
            req: ReqId(21),
            base: 7,
        }),
    );
    assert_eq!(
        effects,
        [Effect::Admin(AdminReply::BaseDeleted {
            req: ReqId(21),
            base: 7
        })]
    );
    assert!(store.peek("b/0000000000000007/rec").is_none());
    let much_later = SimTime(blockd_core::types::secs(200));
    let doomed = blockd_core::gc::plan(much_later, grace, &store.snapshot());
    assert_eq!(
        doomed,
        [
            // The base's segment, unrooted by the explicit delete…
            "b/0000000000000007/s/0000000000000001-0000000000000000".to_owned(),
            // …and the once-fresh orphan, now past its grace (which was
            // in-flight protection, never retention — R4.5).
            fresh_segment.to_owned(),
        ]
    );
}
