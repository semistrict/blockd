use super::*;
use crate::journal::VsetConfig;
use crate::layout;
use crate::seam::{AdminCmd, AdminReply, Effect, Event, HostMap, ReqId};
use crate::types::{PageNo, VolumeId, VolumeIdx, page_size};

/// Record-only flows never touch the mapping.
struct NoMem;
impl HostMap for NoMem {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        panic!("unexpected mapping read of {page:?}");
    }
}

const VSET: VsetId = VsetId(7);

fn config() -> VsetConfig {
    VsetConfig {
        disk_volumes: 1,
        pages_per_volume: 4,
        backed_up: false,
    }
}

/// Drive one event and complete every blob write it (transitively)
/// issues — the storage layer always succeeds, instantly.
fn step_settled(daemon: &mut Daemon, event: Event) -> Vec<Effect> {
    let mut settled = Vec::new();
    let mut queue = vec![event];
    while let Some(event) = queue.pop() {
        for effect in daemon.step(event, &NoMem) {
            match effect {
                Effect::BlobWrite { io, .. } => queue.push(Event::BlobWriteDone { io }),
                other => settled.push(other),
            }
        }
    }
    settled
}

fn created_daemon() -> Daemon {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        host: crate::types::HostId(0),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
    });
    let effects = step_settled(
        &mut daemon,
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
    daemon
}

#[test]
fn checkpoint_retries_replay_their_outcome() {
    // R3.5: a retried request replays its outcome — same epoch, no new
    // pause, no new capture.
    let mut daemon = created_daemon();
    let effects = step_settled(
        &mut daemon,
        Event::Admin(AdminCmd::Checkpoint {
            req: ReqId(1),
            vset: VSET,
        }),
    );
    assert_eq!(effects, [Effect::PauseGuest { vset: VSET }]);
    let effects = step_settled(
        &mut daemon,
        Event::GuestPaused {
            vset: VSET,
            vmstate: 42,
        },
    );
    let done = Effect::Admin(AdminReply::CheckpointDone {
        req: ReqId(1),
        vset: VSET,
        epoch: Epoch(1),
    });
    assert_eq!(
        effects,
        [
            Effect::ResumeGuest { vset: VSET },
            done.clone(),
            // The creation record is superseded and reclaimed (R4.5),
            // both copies.
            Effect::BlobDelete {
                name: layout::journal_blob(VSET, 1, JournalSeq(0)),
            },
            Effect::BlobDelete {
                name: layout::journal_mirror_blob(VSET, 1, JournalSeq(0)),
            },
        ]
    );
    assert_eq!(daemon.counters.checkpoints_done, 1);

    let retried = step_settled(
        &mut daemon,
        Event::Admin(AdminCmd::Checkpoint {
            req: ReqId(1),
            vset: VSET,
        }),
    );
    assert_eq!(retried, [done]);
    assert_eq!(daemon.counters.checkpoints_done, 1, "no second capture");
}

#[test]
fn adversarial_guests_are_rejected_without_collateral() {
    // R11.2: nothing crossing the guest boundary may influence the
    // daemon beyond that guest's own vset — bad requests fail loudly
    // for the guest, and the daemon keeps serving.
    let mut daemon = created_daemon();
    let foreign = PageId {
        volume: VolumeId {
            vset: VsetId(999),
            idx: VolumeIdx(0),
        },
        page: PageNo(0),
    };
    let out_of_range = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(4),
    };
    let effects = step_settled(
        &mut daemon,
        Event::GuestFault {
            page: foreign,
            write: false,
        },
    );
    assert_eq!(effects, [Effect::FillFailed { page: foreign }]);
    let effects = step_settled(
        &mut daemon,
        Event::GuestFault {
            page: out_of_range,
            write: true,
        },
    );
    assert_eq!(effects, [Effect::FillFailed { page: out_of_range }]);
    let effects = step_settled(
        &mut daemon,
        Event::GuestSync {
            req: ReqId(5),
            volume: VolumeId {
                vset: VSET,
                idx: VolumeIdx::MEMORY,
            },
        },
    );
    assert_eq!(effects, [Effect::SyncFailed { req: ReqId(5) }]);
    assert_eq!(daemon.counters.guest_rejected, 3);

    // The vset itself is unharmed: a legitimate zero-fill still works.
    let good = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let effects = step_settled(
        &mut daemon,
        Event::GuestFault {
            page: good,
            write: false,
        },
    );
    assert_eq!(
        effects,
        [Effect::Fill {
            page: good,
            bytes: vec![0; page_size()],
            writable: false,
            share: None,
        }]
    );
    assert_eq!(daemon.counters.zero_fills, 1);
}

/// Zero page contents for capture reads: content is irrelevant here.
struct ZeroMem;
impl HostMap for ZeroMem {
    fn read_page(&self, _page: PageId) -> Vec<u8> {
        vec![0; page_size()]
    }
}

#[test]
fn checkpoint_cost_scales_with_the_delta_not_the_volume() {
    // R3.3: the first capture of a never-checkpointed volume is whole-
    // written-set sized; every later checkpoint pays only for what changed.
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        host: crate::types::HostId(0),
        cache_pages: 256,
        writeback_interval: 1_000_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
    });
    let config = VsetConfig {
        disk_volumes: 1,
        pages_per_volume: 64,
        backed_up: false,
    };
    let effects = step_settled(
        &mut daemon,
        Event::Admin(AdminCmd::CreateVset {
            req: ReqId(0),
            vset: VSET,
            config,
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
    let page = |n: u32| PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(n),
    };
    let dirty = |daemon: &mut Daemon, n: u32| {
        let mut effects = Vec::new();
        for effect in daemon.step(
            Event::GuestFault {
                page: page(n),
                write: true,
            },
            &ZeroMem,
        ) {
            match effect {
                Effect::Fill { .. } | Effect::Unprotect { .. } => {}
                other => effects.push(other),
            }
        }
        assert_eq!(effects, []);
    };
    let checkpoint = |daemon: &mut Daemon, req: u64| {
        let effects = step_settled(
            daemon,
            Event::Admin(AdminCmd::Checkpoint {
                req: ReqId(req),
                vset: VSET,
            }),
        );
        assert_eq!(effects, [Effect::PauseGuest { vset: VSET }]);
        let mut queue = vec![Event::GuestPaused {
            vset: VSET,
            vmstate: req,
        }];
        while let Some(event) = queue.pop() {
            for effect in daemon.step(event, &ZeroMem) {
                if let Effect::BlobWrite { io, .. } = effect {
                    queue.push(Event::BlobWriteDone { io });
                }
            }
        }
    };

    // Whole written set: all 64 pages dirty; the first checkpoint pays 64.
    for n in 0..64 {
        dirty(&mut daemon, n);
    }
    checkpoint(&mut daemon, 1);
    assert_eq!(daemon.counters.checkpoints_done, 1);
    assert_eq!(daemon.counters.pages_flushed, 64);

    // Delta: 3 pages changed; the next checkpoint pays exactly 3 — never
    // the 64-page volume (R3.3).
    for n in [5, 17, 63] {
        dirty(&mut daemon, n);
    }
    checkpoint(&mut daemon, 2);
    assert_eq!(daemon.counters.checkpoints_done, 2);
    assert_eq!(daemon.counters.pages_flushed, 67);

    // An unchanged vset checkpoints for free: no pages at all.
    checkpoint(&mut daemon, 3);
    assert_eq!(daemon.counters.checkpoints_done, 3);
    assert_eq!(daemon.counters.pages_flushed, 67);
}
