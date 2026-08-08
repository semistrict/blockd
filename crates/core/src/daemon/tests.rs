use super::*;
use crate::database::{
    AttachmentId, DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest, MAX_DATABASE_IO,
};
use crate::head::HeadRecord;
use crate::journal::VsetConfig;
use crate::layout;
use crate::mapleaf::{LeafPtr, span_of};
use crate::placement::{PeerCandidate, rank_stash_candidates};
use crate::seam::{
    AdminCmd, AdminReply, DetachMode, Effect, Event, HostMap, PeerMsg, ReplicaArtifact,
    ReplicaCommitInfo, ReqId, StoreFault, Verdict,
};
use crate::segment::SegmentBuilder;
use crate::types::{HostId, PageNo, VmId, VolumeId, VolumeIdx, page_size};

/// Record-only flows never touch the mapping.
struct NoMem;
impl HostMap for NoMem {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        panic!("unexpected mapping read of {page:?}");
    }
}

const VSET: VsetId = VsetId(7);

fn config() -> VsetConfig {
    VsetConfig::compute(1, 4)
}

fn test_replica_placement() -> ReplicaPlacementConfig {
    ReplicaPlacementConfig {
        membership_epoch: 1,
        local_failure_domain: 1,
        roster: vec![
            PeerCandidate {
                host: HostId(0),
                weight: 1,
                failure_domain: 1,
                drained: false,
            },
            PeerCandidate {
                host: HostId(1),
                weight: 1,
                failure_domain: 2,
                drained: false,
            },
        ],
    }
}

/// Drive one event and complete every blob write it (transitively)
/// issues — the storage layer always succeeds, instantly.
fn step_settled(daemon: &mut Daemon, event: Event) -> Vec<Effect> {
    step_settled_with_mem(daemon, event, &NoMem)
}

fn step_settled_with_mem(daemon: &mut Daemon, event: Event, mem: &dyn HostMap) -> Vec<Effect> {
    let mut settled = Vec::new();
    let mut queue = vec![event];
    while let Some(event) = queue.pop() {
        for effect in daemon.step(event, mem) {
            match effect {
                Effect::BlobWrite { io, .. } => queue.push(Event::BlobWriteDone { io }),
                Effect::StoreCas {
                    io, expected: None, ..
                } => queue.push(Event::StorePutDone { io, result: Ok(1) }),
                Effect::PeerSend {
                    to: HostId(1),
                    msg:
                        PeerMsg::ReplicaStatus {
                            vset,
                            assignment_epoch,
                        },
                } => queue.push(Event::PeerDelivered {
                    from: HostId(1),
                    msg: PeerMsg::ReplicaStatusReply {
                        vset,
                        assignment_epoch,
                        committed: None,
                    },
                }),
                Effect::PeerSend {
                    to: HostId(1),
                    msg:
                        PeerMsg::ReplicaPut {
                            vset,
                            assignment_epoch,
                            artifact,
                            checksum,
                            ..
                        },
                } => queue.push(Event::PeerDelivered {
                    from: HostId(1),
                    msg: PeerMsg::ReplicaPutAck {
                        vset,
                        assignment_epoch,
                        artifact,
                        checksum,
                    },
                }),
                Effect::PeerSend {
                    to: HostId(1),
                    msg:
                        PeerMsg::ReplicaCommit {
                            vset,
                            assignment_epoch,
                            info,
                            ..
                        },
                } => queue.push(Event::PeerDelivered {
                    from: HostId(1),
                    msg: PeerMsg::ReplicaCommitAck {
                        vset,
                        assignment_epoch,
                        info,
                    },
                }),
                Effect::SetTimer {
                    timer:
                        TimerId::Replica { .. }
                        | TimerId::ReplicaUpload { .. }
                        | TimerId::ReplicaRelease(_),
                    ..
                } => {}
                other => settled.push(other),
            }
        }
    }
    settled
}

fn created_daemon() -> Daemon {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: crate::types::HostId(0),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
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
fn operational_snapshot_tracks_dirty_and_parked_state() {
    let mut daemon = created_daemon();
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let effects = daemon.step(Event::GuestFault { page, write: true }, &NoMem);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Fill { writable: true, .. }]
    ));

    let stats = daemon.stats();
    assert_eq!(stats.cache_capacity_pages, 8);
    assert_eq!((stats.resident_pages, stats.dirty_pages), (1, 1));
    assert_eq!(stats.vsets.len(), 1);
    let vset = &stats.vsets[0];
    assert_eq!(vset.role, VsetRole::Serving);
    assert_eq!((vset.dirty_pages, vset.unstable_pages), (1, 1));
    assert_eq!(vset.archive_lag_bytes, Some(0));
    assert_eq!(daemon.counters.guest_pages_dirtied, 1);
}

#[test]
fn hydration_tick_scans_and_issues_bounded_batches() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(0),
        cache_pages: 128,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let mut state = Vset::new(VsetConfig::compute(1, 1024));
    state.ready = true;
    state.fence = 2;
    state.peer_source = Some(HostId(1));
    for page_no in 0..1024 {
        let page = PageId {
            volume: VolumeId {
                vset: VSET,
                idx: VolumeIdx(1),
            },
            page: PageNo(page_no),
        };
        state.page_locs.insert(
            page,
            (
                Gen(u64::from(page_no)),
                crate::segment::PageLoc {
                    base: 0,
                    fence: 1,
                    seg: SegId(0),
                    offset: page_no,
                    len: 1,
                },
            ),
        );
    }
    state.hydration_remaining_pages = state.page_locs.len();
    daemon.vsets.insert(VSET, state);

    let effects = daemon.step(Event::Timer(TimerId::Hydrate(VSET)), &NoMem);
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(
                effect,
                Effect::PeerSend {
                    msg: PeerMsg::FetchRange { .. },
                    ..
                }
            ))
            .count(),
        super::migrate::HYDRATE_BATCH
    );
    assert_eq!(daemon.pending.len(), super::migrate::HYDRATE_BATCH);
    assert_eq!(daemon.vsets[&VSET].hydration_remaining_pages, 1024);
    assert_eq!(
        daemon.vsets[&VSET].hydrate_cursor.map(|page| page.page.0),
        Some(255)
    );
}

#[test]
fn peer_retry_preserves_the_original_request_for_a_late_reply() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(0),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let mut state = Vset::new(config());
    state.ready = true;
    state.fence = 2;
    state.peer_source = Some(HostId(1));
    daemon.vsets.insert(VSET, state);
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(2),
    };
    let loc = crate::segment::PageLoc {
        base: 0,
        fence: 1,
        seg: SegId(3),
        offset: 17,
        len: 4096,
    };
    let io = IoId(41);
    daemon.pending.insert(
        io,
        Pending::PeerFetch {
            page,
            write: false,
            generation: Gen(9),
            loc,
        },
    );

    let effects = daemon.step(Event::Timer(TimerId::PeerRetry(io)), &NoMem);

    assert!(matches!(
        daemon.pending.get(&io),
        Some(Pending::PeerFetch { page: pending, .. }) if *pending == page
    ));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::PeerSend {
            to: HostId(1),
            msg: PeerMsg::FetchRange { io: retried, .. },
        } if *retried == io
    )));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::SetTimer {
            timer: TimerId::PeerRetry(retried),
            ..
        } if *retried == io
    )));
}

#[test]
#[ignore = "performance profile; run explicitly in release mode"]
#[allow(clippy::disallowed_types)]
fn profile_300k_page_hydration_tick() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(0),
        cache_pages: 128,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let mut state = Vset::new(VsetConfig::compute(1, 300_000));
    state.ready = true;
    state.fence = 2;
    state.peer_source = Some(HostId(1));
    for page_no in 0..300_000 {
        state.page_locs.insert(
            PageId {
                volume: VolumeId {
                    vset: VSET,
                    idx: VolumeIdx(1),
                },
                page: PageNo(page_no),
            },
            (
                Gen(u64::from(page_no)),
                crate::segment::PageLoc {
                    base: 0,
                    fence: 1,
                    seg: SegId(0),
                    offset: page_no,
                    len: 1,
                },
            ),
        );
    }
    state.hydration_remaining_pages = state.page_locs.len();
    daemon.vsets.insert(VSET, state);

    let started = std::time::Instant::now();
    let effects = daemon.step(Event::Timer(TimerId::Hydrate(VSET)), &NoMem);
    let elapsed = started.elapsed();
    let issued = effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                Effect::PeerSend {
                    msg: PeerMsg::FetchRange { .. },
                    ..
                }
            )
        })
        .count();
    eprintln!("300k hydration tick: elapsed={elapsed:?}, issued={issued}");
    assert_eq!(issued, super::migrate::HYDRATE_BATCH);
    assert_eq!(
        daemon.vsets[&VSET].hydrate_cursor.map(|page| page.page.0),
        Some(255)
    );
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
fn overlapping_recapture_preserves_running_checkpoint_recovery_kind() {
    let mut daemon = created_daemon();
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let _ = daemon.step(Event::GuestFault { page, write: true }, &ZeroMem);
    assert_eq!(
        daemon.step(
            Event::Admin(AdminCmd::Checkpoint {
                req: ReqId(2),
                vset: VSET,
            }),
            &ZeroMem,
        ),
        [Effect::PauseGuest { vset: VSET }]
    );
    let checkpoint_effects = daemon.step(
        Event::GuestPaused {
            vset: VSET,
            vmstate: 17,
        },
        &ZeroMem,
    );
    assert!(
        checkpoint_effects
            .iter()
            .any(|effect| matches!(effect, Effect::BlobWrite { .. }))
    );

    // A compaction recapture can finish before the checkpoint's own writes.
    // With no intervening mutation it represents the same exact snapshot and
    // must carry the in-flight checkpoint's resume metadata.
    let mut recapture_effects = Vec::new();
    daemon.start_capture(VSET, None, &ZeroMem, &mut recapture_effects);
    while let Some(effect) = recapture_effects.pop() {
        if let Effect::BlobWrite { io, .. } = effect {
            recapture_effects.extend(daemon.step(Event::BlobWriteDone { io }, &ZeroMem));
        }
    }
    assert!(matches!(
        daemon.vsets[&VSET].best_record.as_ref().map(|r| r.kind),
        Some(crate::journal::RecordKind::Checkpoint {
            epoch: Epoch(1),
            vmstate: 17
        })
    ));
}

#[test]
fn same_state_recapture_preserves_checkpoint_recovery_kind() {
    let mut daemon = created_daemon();
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let _ = daemon.step(Event::GuestFault { page, write: true }, &ZeroMem);
    let _ = step_settled_with_mem(
        &mut daemon,
        Event::Admin(AdminCmd::Checkpoint {
            req: ReqId(3),
            vset: VSET,
        }),
        &ZeroMem,
    );
    let _ = step_settled_with_mem(
        &mut daemon,
        Event::GuestPaused {
            vset: VSET,
            vmstate: 23,
        },
        &ZeroMem,
    );
    assert!(matches!(
        daemon.vsets[&VSET].best_record.as_ref().map(|r| r.kind),
        Some(crate::journal::RecordKind::Checkpoint { .. })
    ));

    // Compaction can durably recapture the same logical state under a newer
    // sequence number. It must retain the checkpoint's resume metadata.
    let mut effects = Vec::new();
    daemon.start_capture(VSET, None, &ZeroMem, &mut effects);
    while let Some(effect) = effects.pop() {
        if let Effect::BlobWrite { io, .. } = effect {
            effects.extend(daemon.step(Event::BlobWriteDone { io }, &ZeroMem));
        }
    }
    assert!(matches!(
        daemon.vsets[&VSET].best_record.as_ref().map(|r| r.kind),
        Some(crate::journal::RecordKind::Checkpoint { .. })
    ));
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
fn peer_stashed_sync_never_acks_from_local_record_durability() {
    let mut daemon = created_daemon();
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let _ = daemon.step(Event::GuestFault { page, write: true }, &ZeroMem);

    let mut observed = Vec::new();
    let mut queue = vec![Event::GuestSync {
        req: ReqId(9),
        volume: page.volume,
    }];
    while let Some(event) = queue.pop() {
        for effect in daemon.step(event, &ZeroMem) {
            match effect {
                Effect::BlobWrite { io, .. } => queue.push(Event::BlobWriteDone { io }),
                other => observed.push(other),
            }
        }
    }
    assert!(!observed.contains(&Effect::SyncOk { req: ReqId(9) }));
    let state = &daemon.vsets[&VSET];
    assert!(state.local_covered_through > 0);
    assert_eq!(state.sync_ack_through, 0);
    assert_eq!(
        state.pending_syncs,
        vec![(ReqId(9), state.local_covered_through)]
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn peer_stashed_sync_acks_only_after_exact_peer_commit_and_retries() {
    let mut daemon = created_daemon();
    let state = daemon.vsets.get_mut(&VSET).expect("created vset");
    state.stash_assignment = Some(crate::head::StashAssignment {
        assignment_epoch: 1,
        active_peer: HostId(1),
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: 6,
    });
    state.head_version = Some(1);
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let _ = daemon.step(Event::GuestFault { page, write: true }, &ZeroMem);

    let mut blobs = BTreeMap::new();
    let mut effects = daemon.step(
        Event::GuestSync {
            req: ReqId(91),
            volume: page.volume,
        },
        &ZeroMem,
    );
    let status = loop {
        let mut next = Vec::new();
        let mut found = None;
        for effect in effects {
            match effect {
                Effect::BlobWrite { io, name, bytes } => {
                    blobs.insert(name, bytes);
                    next.extend(daemon.step(Event::BlobWriteDone { io }, &ZeroMem));
                }
                Effect::PeerSend {
                    to,
                    msg: msg @ PeerMsg::ReplicaStatus { .. },
                } => {
                    found = Some((to, msg));
                }
                Effect::SyncOk { .. } => panic!("local durability must not acknowledge sync"),
                _ => {}
            }
        }
        if let Some(found) = found {
            break found;
        }
        assert!(!next.is_empty(), "capture must reach replica status");
        effects = next;
    };
    assert_eq!(status.0, HostId(1));
    assert!(matches!(status.1, PeerMsg::ReplicaStatus { .. }));

    // A lost status request is retried byte-for-byte to only the active peer.
    let retry = daemon.step(
        Event::Timer(TimerId::Replica {
            vset: VSET,
            generation: 1,
        }),
        &ZeroMem,
    );
    assert!(retry.contains(&Effect::PeerSend {
        to: HostId(1),
        msg: status.1.clone(),
    }));

    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(1),
            msg: PeerMsg::ReplicaStatusReply {
                vset: VSET,
                assignment_epoch: 1,
                committed: None,
            },
        },
        &ZeroMem,
    );
    let (read_io, read_name) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::BlobRead { io, name } => Some((*io, name.clone())),
            _ => None,
        })
        .expect("missing artifact must be read locally");
    let effects = daemon.step(
        Event::BlobReadDone {
            io: read_io,
            bytes: Some(blobs[&read_name].clone()),
        },
        &ZeroMem,
    );
    let put = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::PeerSend {
                to: HostId(1),
                msg: msg @ PeerMsg::ReplicaPut { .. },
            } => Some(msg.clone()),
            _ => None,
        })
        .expect("artifact put follows verified read");
    let PeerMsg::ReplicaPut {
        artifact, checksum, ..
    } = put
    else {
        unreachable!()
    };
    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(1),
            msg: PeerMsg::ReplicaPutAck {
                vset: VSET,
                assignment_epoch: 1,
                artifact,
                checksum,
            },
        },
        &ZeroMem,
    );
    let (info, commit) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::PeerSend {
                to: HostId(1),
                msg: msg @ PeerMsg::ReplicaCommit { info, .. },
            } => Some((*info, msg.clone())),
            _ => None,
        })
        .expect("last artifact ACK commits the exact recovery closure");
    assert!(!effects.contains(&Effect::SyncOk { req: ReqId(91) }));

    let retry = daemon.step(
        Event::Timer(TimerId::Replica {
            vset: VSET,
            generation: 4,
        }),
        &ZeroMem,
    );
    assert!(retry.contains(&Effect::PeerSend {
        to: HostId(1),
        msg: commit,
    }));
    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(1),
            msg: PeerMsg::ReplicaCommitAck {
                vset: VSET,
                assignment_epoch: 1,
                info,
            },
        },
        &ZeroMem,
    );
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(effect, Effect::SyncOk { req: ReqId(91) }))
            .count(),
        1
    );
    let archived_record = daemon.vsets[&VSET]
        .peer_committed_record
        .as_ref()
        .expect("committed record")
        .encode(VSET);

    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(1),
            msg: PeerMsg::ReplicaUploadDone {
                vset: VSET,
                assignment_epoch: 1,
                info,
                record: archived_record,
            },
        },
        &NoMem,
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::StorePut { .. })),
        "the primary must never upload peer-stashed page bytes"
    );
    let head_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, .. } => Some(*io),
            _ => None,
        })
        .expect("peer upload evidence permits only the small head CAS");
    let effects = daemon.step(
        Event::StorePutDone {
            io: head_io,
            result: Ok(2),
        },
        &NoMem,
    );
    assert!(effects.contains(&Effect::PeerSend {
        to: HostId(1),
        msg: PeerMsg::ReplicaRelease {
            vset: VSET,
            assignment_epoch: 1,
            through: info,
        },
    }));
}

#[test]
#[allow(clippy::too_many_lines)]
fn completing_older_head_cas_preserves_newer_upload_notice() {
    let mut daemon = created_daemon();
    let assignment = crate::head::StashAssignment {
        assignment_epoch: 1,
        active_peer: HostId(1),
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: 6,
    };
    let record = |seq: u64, capture_seq: u64| JournalRecord {
        config: VsetConfig { ..config() },
        seq: JournalSeq(seq),
        fence: 1,
        kind: crate::journal::RecordKind::Commit,
        capture_seq,
        sync_covered_through: capture_seq,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let older = record(1, 1);
    let newer = record(2, 2);
    let older_info = ReplicaCommitInfo {
        writer_fence: older.fence,
        seq: older.seq,
        sync_covered_through: older.sync_covered_through,
    };
    let newer_info = ReplicaCommitInfo {
        writer_fence: newer.fence,
        seq: newer.seq,
        sync_covered_through: newer.sync_covered_through,
    };
    {
        let state = daemon.vsets.get_mut(&VSET).expect("created vset");
        state.stash_assignment = Some(assignment);
        state.head_version = Some(1);
        state.peer_committed = Some(older_info);
        state.peer_committed_record = Some(older.clone());
    }

    let effects = daemon.step(
        Event::PeerDelivered {
            from: assignment.active_peer,
            msg: PeerMsg::ReplicaUploadDone {
                vset: VSET,
                assignment_epoch: assignment.assignment_epoch,
                info: older_info,
                record: older.encode(VSET),
            },
        },
        &NoMem,
    );
    let older_cas = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, .. } => Some(*io),
            _ => None,
        })
        .expect("older upload starts a head CAS");

    {
        let state = daemon.vsets.get_mut(&VSET).expect("created vset");
        state.peer_committed = Some(newer_info);
        state.peer_committed_record = Some(newer.clone());
    }
    assert!(
        daemon
            .step(
                Event::PeerDelivered {
                    from: assignment.active_peer,
                    msg: PeerMsg::ReplicaUploadDone {
                        vset: VSET,
                        assignment_epoch: assignment.assignment_epoch,
                        info: newer_info,
                        record: newer.encode(VSET),
                    },
                },
                &NoMem,
            )
            .iter()
            .all(|effect| !matches!(effect, Effect::StoreCas { .. })),
        "the newer notice waits behind the in-flight CAS"
    );

    let effects = daemon.step(
        Event::StorePutDone {
            io: older_cas,
            result: Ok(2),
        },
        &NoMem,
    );
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::StoreCas { bytes, .. }
                if HeadRecord::decode(VSET, bytes)
                    .expect("next head")
                    .manifest
                    .is_some_and(|ptr| ptr.seq == newer_info.seq)
        )),
        "completing the older CAS must immediately publish the queued newer notice: {effects:?}"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn head_mutations_are_serialized_per_vset() {
    let mut daemon = created_daemon();
    let current = crate::head::StashAssignment {
        assignment_epoch: 1,
        active_peer: HostId(1),
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: 6,
    };
    let transition = crate::head::StashAssignment {
        assignment_epoch: 2,
        transition_peer: Some(HostId(2)),
        ..current
    };
    let record = JournalRecord {
        config: VsetConfig { ..config() },
        seq: JournalSeq(2),
        fence: 1,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 2,
        sync_covered_through: 2,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let info = Daemon::commit_info(&record);
    {
        let state = daemon.vsets.get_mut(&VSET).expect("created vset");
        state.head_version = Some(1);
        state.stash_assignment = Some(current);
        state.peer_upload_done = Some((current.assignment_epoch, info, record.clone()));
        state.peer_committed_record = Some(record);
        state.replica_assignment_proposal = Some(ReplicaAssignmentProposal {
            assignment: transition,
            activation: None,
        });
    }

    let effects = daemon.step(Event::Timer(TimerId::Backup(VSET)), &NoMem);
    let head_writes: Vec<_> = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::StoreCas {
                io,
                expected,
                bytes,
                ..
            } => Some((
                *io,
                *expected,
                HeadRecord::decode(VSET, bytes).expect("head"),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        head_writes.len(),
        1,
        "only one CAS may target a vset head at a time: {effects:?}"
    );
    assert_eq!(head_writes[0].1, Some(1));
    assert_eq!(
        head_writes[0].2.manifest.expect("publication first").seq,
        info.seq
    );

    let retired = crate::head::RetiredStash {
        peer: HostId(3),
        assignment_epoch: 1,
        through: info,
    };
    {
        let state = daemon.vsets.get_mut(&VSET).expect("created vset");
        state.retired_stashes.push(retired);
        state.replica_release = Some((retired.peer, retired.assignment_epoch, retired.through));
    }
    let effects = daemon.step(
        Event::PeerDelivered {
            from: retired.peer,
            msg: PeerMsg::ReplicaReleaseAck {
                vset: VSET,
                assignment_epoch: retired.assignment_epoch,
                through: retired.through,
            },
        },
        &NoMem,
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::StoreCas { .. })),
        "history cleanup must wait behind the publication CAS: {effects:?}"
    );

    let effects = daemon.step(
        Event::StorePutDone {
            io: head_writes[0].0,
            result: Ok(2),
        },
        &NoMem,
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::StoreCas {
            expected: Some(2),
            bytes,
            ..
        } if HeadRecord::decode(VSET, bytes).expect("transition head").stash == Some(transition)
    )));
}

#[test]
fn store_only_restore_refuses_a_head_with_peer_residue() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(2),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let effects = daemon.step(
        Event::Admin(AdminCmd::RestoreVset {
            req: ReqId(93),
            vset: VSET,
        }),
        &NoMem,
    );
    let head_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreGet { io, .. } => Some(*io),
            _ => None,
        })
        .expect("restore reads the fenced head");
    let head = HeadRecord {
        vset: VSET,
        holder: HostId(0),
        fence: 4,
        manifest: Some(crate::head::ManifestPtr {
            fence: 4,
            seq: JournalSeq(8),
            capture_seq: 11,
        }),
        stash: Some(crate::head::StashAssignment {
            assignment_epoch: 1,
            active_peer: HostId(1),
            active_assignment_epoch: 1,
            transition_peer: None,
            membership_epoch: 6,
        }),
        retired_stashes: Vec::new(),
    };

    assert_eq!(
        daemon.step(
            Event::StoreGetDone {
                io: head_io,
                result: Ok(Some((9, head.encode()))),
            },
            &NoMem,
        ),
        [Effect::Admin(AdminReply::AdminFailed { req: ReqId(93) })],
        "store-only restore must not claim an older point while peer residue may be newer"
    );
}

#[test]
fn peer_stashed_creation_publishes_exactly_one_deterministic_stash() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(0),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 6,
            local_failure_domain: 1,
            roster: vec![
                PeerCandidate {
                    host: HostId(1),
                    weight: 1,
                    failure_domain: 2,
                    drained: false,
                },
                PeerCandidate {
                    host: HostId(2),
                    weight: 3,
                    failure_domain: 3,
                    drained: false,
                },
            ],
        }),
    });
    let effects = daemon.step(
        Event::Admin(AdminCmd::CreateVset {
            req: ReqId(20),
            vset: VSET,
            config: VsetConfig { ..config() },
            from_base: None,
        }),
        &NoMem,
    );
    let [Effect::StoreCas { bytes, .. }] = effects.as_slice() else {
        panic!("creation must first publish one fenced head: {effects:?}");
    };
    let head = HeadRecord::decode(VSET, bytes).expect("head decodes");
    let stash = head.stash.expect("one stash assignment");
    assert!(matches!(stash.active_peer, HostId(1 | 2)));
    assert_eq!(stash.transition_peer, None);
    assert_eq!(stash.membership_epoch, 6);
    assert_eq!(stash.assignment_epoch, 1);
}

#[test]
#[allow(clippy::too_many_lines)]
fn failed_active_peer_rebinds_through_a_fenced_transition_before_sync_ack() {
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
        PeerCandidate {
            host: HostId(2),
            weight: 1,
            failure_domain: 3,
            drained: false,
        },
    ];
    let ranked = rank_stash_candidates(6, HostId(0), 1, VSET, &roster);
    let (active, replacement) = (ranked[0], ranked[1]);
    let mut daemon = created_daemon();
    daemon.config.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 6,
        local_failure_domain: 1,
        roster,
    });
    let record = JournalRecord {
        config: VsetConfig { ..config() },
        seq: JournalSeq(4),
        fence: 1,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 10,
        sync_covered_through: 10,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let state = daemon.vsets.get_mut(&VSET).expect("created");
    state.head_version = Some(5);
    state.stash_assignment = Some(crate::head::StashAssignment {
        assignment_epoch: 1,
        active_peer: active,
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: 6,
    });
    state.best_record = Some(record.clone());
    state.best = Some((10, JournalSeq(4)));
    state.local_covered_through = 10;
    state.pending_syncs = vec![(ReqId(92), 10)];
    state.replica_send = Some(ReplicaSend {
        target: active,
        assignment_epoch: 1,
        record,
        required: Vec::new(),
        todo: Vec::new(),
        awaiting: Some(PeerMsg::ReplicaStatus {
            vset: VSET,
            assignment_epoch: 1,
        }),
        retries: 2,
        timer_generation: 1,
    });

    let effects = daemon.step(
        Event::Timer(TimerId::Replica {
            vset: VSET,
            generation: 1,
        }),
        &NoMem,
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::PeerSend { .. }))
    );
    let (transition_io, transition_head) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, bytes, .. } => {
                Some((*io, HeadRecord::decode(VSET, bytes).expect("head")))
            }
            _ => None,
        })
        .expect("retry threshold proposes a fenced transition");
    assert_eq!(
        transition_head.stash,
        Some(crate::head::StashAssignment {
            assignment_epoch: 2,
            active_peer: active,
            active_assignment_epoch: 1,
            transition_peer: Some(replacement),
            membership_epoch: 6,
        })
    );

    let effects = daemon.step(Event::Timer(TimerId::Backup(VSET)), &NoMem);
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::PeerSend { .. })),
        "an unrelated tick must not restart the old assignment during transition CAS: {effects:?}"
    );

    let effects = daemon.step(
        Event::StorePutDone {
            io: transition_io,
            result: Ok(6),
        },
        &NoMem,
    );
    assert!(effects.contains(&Effect::PeerSend {
        to: replacement,
        msg: PeerMsg::ReplicaStatus {
            vset: VSET,
            assignment_epoch: 2,
        },
    }));
    let effects = daemon.step(
        Event::PeerDelivered {
            from: replacement,
            msg: PeerMsg::ReplicaStatusReply {
                vset: VSET,
                assignment_epoch: 2,
                committed: None,
            },
        },
        &NoMem,
    );
    let (info, commit) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::PeerSend {
                to,
                msg: PeerMsg::ReplicaCommit { info, .. },
            } if *to == replacement => Some((*info, effect.clone())),
            _ => None,
        })
        .expect("replacement receives exactly one complete baseline");
    assert!(matches!(commit, Effect::PeerSend { .. }));
    let effects = daemon.step(
        Event::PeerDelivered {
            from: replacement,
            msg: PeerMsg::ReplicaCommitAck {
                vset: VSET,
                assignment_epoch: 2,
                info,
            },
        },
        &NoMem,
    );
    assert!(!effects.contains(&Effect::SyncOk { req: ReqId(92) }));
    let (activate_io, activate_head) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, bytes, .. } => {
                Some((*io, HeadRecord::decode(VSET, bytes).expect("head")))
            }
            _ => None,
        })
        .expect("baseline commit proposes activation CAS");
    assert_eq!(
        activate_head.stash.expect("assignment").active_peer,
        replacement
    );
    assert_eq!(
        activate_head.stash.expect("assignment").transition_peer,
        None
    );
    assert_eq!(
        activate_head.retired_stashes,
        [crate::head::RetiredStash {
            peer: active,
            assignment_epoch: 1,
            through: info,
        }]
    );
    let effects = daemon.step(Event::Timer(TimerId::Backup(VSET)), &NoMem);
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::PeerSend { .. })),
        "an unrelated tick must not start another send during activation CAS: {effects:?}"
    );
    let effects = daemon.step(
        Event::StorePutDone {
            io: activate_io,
            result: Ok(7),
        },
        &NoMem,
    );
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(effect, Effect::SyncOk { req: ReqId(92) }))
            .count(),
        1
    );
    assert_eq!(
        daemon.vsets[&VSET]
            .stash_assignment
            .expect("active replacement")
            .active_peer,
        replacement
    );

    let effects = daemon.step(
        Event::PeerDelivered {
            from: replacement,
            msg: PeerMsg::ReplicaUploadDone {
                vset: VSET,
                assignment_epoch: 2,
                info,
                record: daemon.vsets[&VSET]
                    .peer_committed_record
                    .as_ref()
                    .expect("replacement commit")
                    .encode(VSET),
            },
        },
        &NoMem,
    );
    let publish_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, .. } => Some(*io),
            _ => None,
        })
        .expect("replacement upload publishes a covering head");
    let effects = daemon.step(
        Event::StorePutDone {
            io: publish_io,
            result: Ok(8),
        },
        &NoMem,
    );
    assert!(effects.contains(&Effect::PeerSend {
        to: replacement,
        msg: PeerMsg::ReplicaRelease {
            vset: VSET,
            assignment_epoch: 2,
            through: info,
        },
    }));
    let effects = daemon.step(
        Event::PeerDelivered {
            from: replacement,
            msg: PeerMsg::ReplicaReleaseAck {
                vset: VSET,
                assignment_epoch: 2,
                through: info,
            },
        },
        &NoMem,
    );
    assert!(effects.contains(&Effect::PeerSend {
        to: active,
        msg: PeerMsg::ReplicaRelease {
            vset: VSET,
            assignment_epoch: 1,
            through: info,
        },
    }));
    let effects = daemon.step(
        Event::PeerDelivered {
            from: active,
            msg: PeerMsg::ReplicaReleaseAck {
                vset: VSET,
                assignment_epoch: 1,
                through: info,
            },
        },
        &NoMem,
    );
    let (history_io, history_head) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, bytes, .. } => {
                Some((*io, HeadRecord::decode(VSET, bytes).expect("head")))
            }
            _ => None,
        })
        .expect("durable old-peer release removes retained authority");
    assert!(history_head.retired_stashes.is_empty());
    daemon.step(
        Event::StorePutDone {
            io: history_io,
            result: Ok(9),
        },
        &NoMem,
    );
    assert!(daemon.vsets[&VSET].retired_stashes.is_empty());
}

#[test]
#[allow(clippy::too_many_lines)]
fn passive_replacement_resumes_sync_during_a_complete_store_outage() {
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
        PeerCandidate {
            host: HostId(2),
            weight: 1,
            failure_domain: 3,
            drained: false,
        },
    ];
    let ranked = rank_stash_candidates(6, HostId(0), 1, VSET, &roster);
    let (failed, replacement) = (ranked[0], ranked[1]);
    let mut daemon = created_daemon();
    daemon.config.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 6,
        local_failure_domain: 1,
        roster,
    });
    let record = JournalRecord {
        config: VsetConfig { ..config() },
        seq: JournalSeq(4),
        fence: 1,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 10,
        sync_covered_through: 10,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let state = daemon.vsets.get_mut(&VSET).expect("created");
    state.head_version = Some(5);
    state.stash_assignment = Some(crate::head::StashAssignment {
        assignment_epoch: 1,
        active_peer: failed,
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: 6,
    });
    state.best_record = Some(record.clone());
    state.best = Some((10, JournalSeq(4)));
    state.local_covered_through = 10;
    state.pending_syncs = vec![(ReqId(92), 10)];
    state.replica_send = Some(ReplicaSend {
        target: failed,
        assignment_epoch: 1,
        record,
        required: Vec::new(),
        todo: Vec::new(),
        awaiting: Some(PeerMsg::ReplicaStatus {
            vset: VSET,
            assignment_epoch: 1,
        }),
        retries: 2,
        timer_generation: 1,
    });

    let effects = daemon.step(
        Event::Timer(TimerId::Replica {
            vset: VSET,
            generation: 1,
        }),
        &NoMem,
    );
    let transition_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, .. } => Some(*io),
            _ => None,
        })
        .expect("failed passive proposes a replacement");
    let effects = daemon.step(
        Event::StorePutDone {
            io: transition_io,
            result: Err(StoreFault::Unavailable),
        },
        &NoMem,
    );
    assert!(effects.contains(&Effect::PeerSend {
        to: replacement,
        msg: PeerMsg::ReplicaStatus {
            vset: VSET,
            assignment_epoch: 2,
        },
    }));

    let effects = daemon.step(
        Event::PeerDelivered {
            from: replacement,
            msg: PeerMsg::ReplicaStatusReply {
                vset: VSET,
                assignment_epoch: 2,
                committed: None,
            },
        },
        &NoMem,
    );
    let info = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::PeerSend {
                to,
                msg: PeerMsg::ReplicaCommit { info, .. },
            } if *to == replacement => Some(*info),
            _ => None,
        })
        .expect("replacement receives a complete baseline");
    let effects = daemon.step(
        Event::PeerDelivered {
            from: replacement,
            msg: PeerMsg::ReplicaCommitAck {
                vset: VSET,
                assignment_epoch: 2,
                info,
            },
        },
        &NoMem,
    );
    assert!(!effects.contains(&Effect::SyncOk { req: ReqId(92) }));
    let activation_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, .. } => Some(*io),
            _ => None,
        })
        .expect("durable replacement proposes activation");
    let effects = daemon.step(
        Event::StorePutDone {
            io: activation_io,
            result: Err(StoreFault::Unavailable),
        },
        &NoMem,
    );
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(effect, Effect::SyncOk { req: ReqId(92) }))
            .count(),
        1,
        "a complete replacement is the durability proof; store age must not gate sync"
    );
    let state = &daemon.vsets[&VSET];
    assert_eq!(
        state
            .stash_assignment
            .expect("provisional active")
            .active_peer,
        replacement
    );
    assert!(state.replica_assignment_proposal.is_some());

    let effects = daemon.step(Event::Timer(TimerId::Backup(VSET)), &NoMem);
    let (retry_io, retry_head) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, bytes, .. } => {
                Some((*io, HeadRecord::decode(VSET, bytes).expect("head")))
            }
            _ => None,
        })
        .expect("assignment publication retries independently");
    assert_eq!(
        retry_head.stash.expect("assignment").active_peer,
        replacement
    );
    assert_eq!(retry_head.retired_stashes[0].peer, failed);
    let effects = daemon.step(
        Event::StorePutDone {
            io: retry_io,
            result: Err(StoreFault::CasConflict { actual: Some(6) }),
        },
        &NoMem,
    );
    let refresh_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreGet { io, .. } => Some(*io),
            _ => None,
        })
        .expect("a conflict rereads assignment authority");
    let old_head = HeadRecord {
        vset: VSET,
        holder: HostId(0),
        fence: 1,
        manifest: None,
        stash: Some(crate::head::StashAssignment {
            assignment_epoch: 1,
            active_peer: failed,
            active_assignment_epoch: 1,
            transition_peer: None,
            membership_epoch: 6,
        }),
        retired_stashes: Vec::new(),
    };
    let effects = daemon.step(
        Event::StoreGetDone {
            io: refresh_io,
            result: Ok(Some((6, old_head.encode()))),
        },
        &NoMem,
    );
    assert_eq!(
        daemon.vsets[&VSET]
            .stash_assignment
            .expect("provisional assignment survives refresh")
            .active_peer,
        replacement
    );
    let reconcile_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, bytes, .. } => {
                let head = HeadRecord::decode(VSET, bytes).expect("head");
                (head.stash.expect("assignment").active_peer == replacement).then_some(*io)
            }
            _ => None,
        })
        .expect("an older head is reconciled to the durable replacement");
    daemon.step(
        Event::StorePutDone {
            io: reconcile_io,
            result: Ok(7),
        },
        &NoMem,
    );
    assert!(daemon.vsets[&VSET].replica_assignment_proposal.is_none());
}

#[test]
#[allow(clippy::too_many_lines)]
fn repeated_passive_failover_cycles_the_roster_without_losing_committed_state() {
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
        PeerCandidate {
            host: HostId(2),
            weight: 1,
            failure_domain: 3,
            drained: false,
        },
        PeerCandidate {
            host: HostId(3),
            weight: 1,
            failure_domain: 4,
            drained: false,
        },
        PeerCandidate {
            host: HostId(4),
            weight: 1,
            failure_domain: 5,
            drained: false,
        },
    ];
    let ranked = rank_stash_candidates(6, HostId(0), 1, VSET, &roster);
    assert_eq!(ranked.len(), 4);
    let mut daemon = created_daemon();
    daemon.config.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 6,
        local_failure_domain: 1,
        roster,
    });
    let mut store_version = 20;
    let mut active = ranked[0];
    daemon.vsets.get_mut(&VSET).expect("created").head_version = Some(store_version);
    daemon
        .vsets
        .get_mut(&VSET)
        .expect("created")
        .stash_assignment = Some(crate::head::StashAssignment {
        assignment_epoch: 1,
        active_peer: active,
        active_assignment_epoch: 1,
        transition_peer: None,
        membership_epoch: 6,
    });

    // Exceed both the number of candidates and the historical eight-entry
    // retired-stash bound. Availability must not have either finite budget.
    for failover in 0_u64..12 {
        let assignment = daemon.vsets[&VSET]
            .stash_assignment
            .expect("active assignment");
        let next_epoch = assignment.assignment_epoch + 1;
        let expected_next = ranked[usize::try_from(next_epoch - 1).unwrap() % ranked.len()];
        assert_ne!(expected_next, active);
        let covered = 100 + failover;
        let record = JournalRecord {
            config: VsetConfig { ..config() },
            seq: JournalSeq(50 + failover),
            fence: 1,
            kind: crate::journal::RecordKind::Commit,
            capture_seq: 70 + failover,
            sync_covered_through: covered,
            database: crate::journal::DatabaseMeta::default(),
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        };
        let req = ReqId(1_000 + failover);
        let timer_generation = 500 + failover;
        {
            let state = daemon.vsets.get_mut(&VSET).expect("created");
            state.best_record = Some(record.clone());
            state.best = Some((record.capture_seq, record.seq));
            state.local_covered_through = covered;
            state.pending_syncs.push((req, covered));
            state.replica_send = Some(ReplicaSend {
                target: active,
                assignment_epoch: assignment.active_assignment_epoch,
                record: record.clone(),
                required: Vec::new(),
                todo: Vec::new(),
                awaiting: Some(PeerMsg::ReplicaStatus {
                    vset: VSET,
                    assignment_epoch: assignment.active_assignment_epoch,
                }),
                retries: 2,
                timer_generation,
            });
        }

        let effects = daemon.step(
            Event::Timer(TimerId::Replica {
                vset: VSET,
                generation: timer_generation,
            }),
            &NoMem,
        );
        let (transition_io, transition) = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::StoreCas { io, bytes, .. } => Some((
                    *io,
                    HeadRecord::decode(VSET, bytes).expect("transition head"),
                )),
                _ => None,
            })
            .expect("failed active must start another transition");
        let transition_assignment = transition.stash.expect("transition assignment");
        assert_eq!(transition_assignment.assignment_epoch, next_epoch);
        assert_eq!(transition_assignment.active_peer, active);
        assert_eq!(transition_assignment.transition_peer, Some(expected_next));
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, Effect::SyncOk { .. }))
        );

        store_version += 1;
        let effects = daemon.step(
            Event::StorePutDone {
                io: transition_io,
                result: Ok(store_version),
            },
            &NoMem,
        );
        assert!(effects.contains(&Effect::PeerSend {
            to: expected_next,
            msg: PeerMsg::ReplicaStatus {
                vset: VSET,
                assignment_epoch: next_epoch,
            },
        }));
        let effects = daemon.step(
            Event::PeerDelivered {
                from: expected_next,
                msg: PeerMsg::ReplicaStatusReply {
                    vset: VSET,
                    assignment_epoch: next_epoch,
                    committed: None,
                },
            },
            &NoMem,
        );
        let info = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::PeerSend {
                    to,
                    msg: PeerMsg::ReplicaCommit { info, .. },
                } if *to == expected_next => Some(*info),
                _ => None,
            })
            .expect("replacement receives a complete commit");
        assert_eq!(info, Daemon::commit_info(&record));

        let effects = daemon.step(
            Event::PeerDelivered {
                from: expected_next,
                msg: PeerMsg::ReplicaCommitAck {
                    vset: VSET,
                    assignment_epoch: next_epoch,
                    info,
                },
            },
            &NoMem,
        );
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, Effect::SyncOk { .. })),
            "commit alone is not authoritative until activation is fenced"
        );
        let (activate_io, activation) = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::StoreCas { io, bytes, .. } => Some((
                    *io,
                    HeadRecord::decode(VSET, bytes).expect("activation head"),
                )),
                _ => None,
            })
            .expect("replacement commit must start activation");
        assert_eq!(activation.stash.expect("active").active_peer, expected_next);
        assert_eq!(activation.retired_stashes.len(), 1);
        assert_eq!(activation.retired_stashes[0].peer, active);

        store_version += 1;
        let effects = daemon.step(
            Event::StorePutDone {
                io: activate_io,
                result: Ok(store_version),
            },
            &NoMem,
        );
        assert_eq!(
            effects
                .iter()
                .filter(|effect| **effect == Effect::SyncOk { req })
                .count(),
            1,
            "the covered sync must be acknowledged exactly once"
        );
        let state = &daemon.vsets[&VSET];
        assert_eq!(state.best_record.as_ref(), Some(&record));
        assert_eq!(state.peer_committed_record.as_ref(), Some(&record));
        assert_eq!(state.sync_ack_through, covered);
        assert_eq!(state.retired_stashes.len(), 1);
        assert_eq!(
            state.stash_assignment.expect("replacement").active_peer,
            expected_next
        );
        let late = daemon.step(
            Event::PeerDelivered {
                from: active,
                msg: PeerMsg::ReplicaCommitAck {
                    vset: VSET,
                    assignment_epoch: assignment.active_assignment_epoch,
                    info,
                },
            },
            &NoMem,
        );
        assert!(late.is_empty(), "a retired peer must not regain authority");
        assert_eq!(daemon.vsets[&VSET].sync_ack_through, covered);
        active = expected_next;
    }
}

#[test]
fn assignment_epochs_cycle_deterministically_without_authorizing_fanout() {
    let roster: Vec<_> = (0..5)
        .map(|host| PeerCandidate {
            host: HostId(host),
            weight: 1,
            failure_domain: host + 1,
            drained: false,
        })
        .collect();
    let ranked = rank_stash_candidates(9, HostId(0), 1, VSET, &roster);
    assert_eq!(ranked.len(), 4);
    let mut daemon = created_daemon();
    daemon.config.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 9,
        local_failure_domain: 1,
        roster,
    });
    for assignment_epoch in 1..=20 {
        let index = usize::try_from(assignment_epoch - 1).expect("test epoch fits");
        let expected = ranked[index % ranked.len()];
        for candidate in &ranked {
            daemon.config.host = *candidate;
            assert_eq!(
                daemon.replica_authorized(HostId(0), VSET, assignment_epoch),
                *candidate == expected,
                "epoch {assignment_epoch} must authorize exactly one cyclic candidate"
            );
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn passive_restart_rebuilds_the_highest_assignment_epoch_fence() {
    let roster: Vec<_> = (0..5)
        .map(|host| PeerCandidate {
            host: HostId(host),
            weight: 1,
            failure_domain: host + 1,
            drained: false,
        })
        .collect();
    let ranked = rank_stash_candidates(9, HostId(0), 1, VSET, &roster);
    let target = ranked[0];
    let target_domain = roster
        .iter()
        .find(|candidate| candidate.host == target)
        .unwrap()
        .failure_domain;
    let daemon_config = DaemonConfig {
        archive: ArchivePolicy::default(),
        host: target,
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 9,
            local_failure_domain: target_domain,
            roster,
        }),
    };
    let seal = |assignment_epoch: u64, seq: u64| {
        let info = ReplicaCommitInfo {
            writer_fence: 4,
            seq: JournalSeq(seq),
            sync_covered_through: seq,
        };
        let record = JournalRecord {
            config: VsetConfig { ..config() },
            seq: info.seq,
            fence: info.writer_fence,
            kind: crate::journal::RecordKind::Commit,
            capture_seq: seq,
            sync_covered_through: seq,
            database: crate::journal::DatabaseMeta::default(),
            overlay: BTreeMap::new(),
            leaves: BTreeMap::new(),
            migrated_from: None,
        }
        .encode(VSET);
        (
            crate::replica_spool::seal_replica_commit(
                HostId(0),
                VSET,
                assignment_epoch,
                info,
                &[],
                &record,
            )
            .unwrap(),
            info,
        )
    };
    let new_epoch = 1 + ranked.len() as u64;
    let (old_spool, _) = seal(1, 1);
    let (new_spool, new_info) = seal(new_epoch, 5);
    let old_name = layout::replica_spool_segment_blob(HostId(0), VSET, 1, 0);
    let new_name = layout::replica_spool_segment_blob(HostId(0), VSET, new_epoch, 0);
    let (mut recovered, _, _) = Daemon::recover(
        daemon_config,
        [
            (old_name.as_str(), old_spool.as_slice()),
            (new_name.as_str(), new_spool.as_slice()),
        ]
        .into_iter(),
    );
    let rejected_before = recovered.counters.replica_rejected;
    assert!(
        recovered
            .step(
                Event::PeerDelivered {
                    from: HostId(0),
                    msg: PeerMsg::ReplicaStatus {
                        vset: VSET,
                        assignment_epoch: 1,
                    },
                },
                &NoMem,
            )
            .is_empty(),
        "restart must not revive a stale cyclic assignment"
    );
    assert_eq!(recovered.counters.replica_rejected, rejected_before + 1);
    assert_eq!(
        recovered.step(
            Event::PeerDelivered {
                from: HostId(0),
                msg: PeerMsg::ReplicaStatus {
                    vset: VSET,
                    assignment_epoch: new_epoch,
                },
            },
            &NoMem,
        ),
        [Effect::PeerSend {
            to: HostId(0),
            msg: PeerMsg::ReplicaStatusReply {
                vset: VSET,
                assignment_epoch: new_epoch,
                committed: Some(new_info),
            },
        }]
    );
}

#[test]
fn assignment_cas_conflict_rereads_authority_without_seeding_a_peer() {
    let mut daemon = created_daemon();
    let assignment = crate::head::StashAssignment {
        assignment_epoch: 2,
        active_peer: HostId(1),
        active_assignment_epoch: 1,
        transition_peer: Some(HostId(2)),
        membership_epoch: 6,
    };
    let mut effects = Vec::new();
    daemon.replica_store_done(
        Pending::ReplicaTransitionCas {
            vset: VSET,
            assignment,
        },
        Err(crate::seam::StoreFault::CasConflict { actual: Some(9) }),
        &mut effects,
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::PeerSend { .. }))
    );
    assert!(matches!(effects.as_slice(), [Effect::StoreGet { .. }]));
}

#[test]
fn claimed_recovery_head_releases_the_store_covered_active_residue() {
    let mut daemon = created_daemon();
    daemon.config.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 6,
        local_failure_domain: 1,
        roster: vec![
            PeerCandidate {
                host: HostId(0),
                weight: 1,
                failure_domain: 1,
                drained: false,
            },
            PeerCandidate {
                host: HostId(1),
                weight: 1,
                failure_domain: 2,
                drained: false,
            },
        ],
    });
    let record = JournalRecord {
        config: VsetConfig { ..config() },
        seq: JournalSeq(3),
        fence: 2,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 4,
        sync_covered_through: 5,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let state = daemon.vsets.get_mut(&VSET).expect("vset");
    state.best_record = Some(record);
    state.best = Some((4, JournalSeq(3)));
    state.head_refreshing = true;
    let head = HeadRecord {
        vset: VSET,
        holder: HostId(0),
        fence: 2,
        manifest: Some(crate::head::ManifestPtr {
            fence: 2,
            seq: JournalSeq(3),
            capture_seq: 4,
        }),
        stash: Some(crate::head::StashAssignment {
            assignment_epoch: 1,
            active_peer: HostId(1),
            active_assignment_epoch: 1,
            transition_peer: None,
            membership_epoch: 6,
        }),
        retired_stashes: Vec::new(),
    };
    let mut effects = Vec::new();
    daemon.head_refresh_done(VSET, Ok(Some((3, head.encode()))), &mut effects);
    assert!(effects.contains(&Effect::PeerSend {
        to: HostId(1),
        msg: PeerMsg::ReplicaRelease {
            vset: VSET,
            assignment_epoch: 1,
            through: ReplicaCommitInfo {
                writer_fence: 2,
                seq: JournalSeq(3),
                sync_covered_through: 5,
            },
        },
    }));
}

#[test]
fn recovered_peer_stash_fences_on_membership_epoch_mismatch() {
    let mut daemon = created_daemon();
    let head = HeadRecord {
        vset: VSET,
        holder: HostId(0),
        fence: 2,
        manifest: None,
        stash: Some(crate::head::StashAssignment {
            assignment_epoch: 1,
            active_peer: HostId(1),
            active_assignment_epoch: 1,
            transition_peer: None,
            membership_epoch: 7,
        }),
        retired_stashes: Vec::new(),
    };
    let mut effects = Vec::new();
    daemon.head_refresh_done(VSET, Ok(Some((3, head.encode()))), &mut effects);
    assert_eq!(effects, vec![Effect::VsetFenced { vset: VSET }]);
    assert!(!daemon.vsets.contains_key(&VSET));
}

#[test]
fn legacy_backup_head_is_upgraded_to_a_passive_assignment_before_recovery_opens() {
    let mut daemon = created_daemon();
    let state = daemon.vsets.get_mut(&VSET).expect("vset");
    state.ready = false;
    state.pending_verdict = Some(Verdict::ColdBoot);
    state.head_refreshing = true;
    state.stash_assignment = None;
    state.replica_assignment_proposal = None;
    state.replica_send = None;
    state.peer_artifacts.clear();
    state.peer_committed = None;
    state.peer_committed_record = None;
    let legacy_head = HeadRecord {
        vset: VSET,
        holder: HostId(0),
        fence: 1,
        manifest: None,
        stash: None,
        retired_stashes: Vec::new(),
    };
    let mut effects = Vec::new();
    daemon.head_refresh_done(VSET, Ok(Some((3, legacy_head.encode()))), &mut effects);
    assert!(daemon.vsets[&VSET].pending_verdict.is_some());
    let (io, upgraded) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas { io, bytes, .. } => {
                Some((*io, HeadRecord::decode(VSET, bytes).expect("head")))
            }
            _ => None,
        })
        .expect("legacy head gets one fenced passive assignment");
    let assignment = upgraded.stash.expect("passive assignment");
    assert_eq!(assignment.assignment_epoch, 1);
    assert_eq!(assignment.transition_peer, None);

    let effects = daemon.step(Event::StorePutDone { io, result: Ok(4) }, &NoMem);
    assert!(effects.contains(&Effect::Admin(AdminReply::VsetRecovered {
        vset: VSET,
        verdict: Verdict::ColdBoot,
    })));
    assert!(effects.contains(&Effect::PeerSend {
        to: assignment.active_peer,
        msg: PeerMsg::ReplicaStatus {
            vset: VSET,
            assignment_epoch: 1,
        },
    }));
    assert!(daemon.vsets[&VSET].ready);
}

#[test]
fn legacy_local_recovery_creates_its_first_head_before_opening() {
    let mut daemon = created_daemon();
    let state = daemon.vsets.get_mut(&VSET).expect("vset");
    state.ready = false;
    state.pending_verdict = Some(Verdict::ColdBoot);
    state.head_refreshing = true;
    state.head_version = None;
    state.stash_assignment = None;
    let mut effects = Vec::new();
    daemon.head_refresh_done(VSET, Ok(None), &mut effects);
    let io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreCas {
                io, expected: None, ..
            } => Some(*io),
            _ => None,
        })
        .expect("legacy local recovery creates a fenced head");
    let effects = daemon.step(Event::StorePutDone { io, result: Ok(1) }, &NoMem);
    assert!(effects.contains(&Effect::Admin(AdminReply::VsetRecovered {
        vset: VSET,
        verdict: Verdict::ColdBoot,
    })));
    assert!(daemon.vsets[&VSET].ready);
    assert!(daemon.vsets[&VSET].stash_assignment.is_some());
}

#[test]
#[allow(clippy::too_many_lines)]
fn release_preserves_acked_artifacts_for_the_next_commit() {
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
    ];
    let target = rank_stash_candidates(6, HostId(0), 1, VSET, &roster)[0];
    let target_domain = roster
        .iter()
        .find(|candidate| candidate.host == target)
        .expect("target in roster")
        .failure_domain;
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: target,
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 6,
            local_failure_domain: target_domain,
            roster,
        }),
    });
    let covered = ReplicaCommitInfo {
        writer_fence: 4,
        seq: JournalSeq(8),
        sync_covered_through: 12,
    };
    daemon.replicas.insert(
        ReplicaKey {
            source: HostId(0),
            vset: VSET,
            assignment_epoch: 1,
        },
        PassiveReplica {
            committed: Some((covered, 1)),
            upload_done: Some(covered),
            ..PassiveReplica::default()
        },
    );

    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let mut builder = SegmentBuilder::new(VSET, 4, SegId(9));
    builder.add(page, Gen(3), &vec![0xA5; page_size()]);
    let (segment, _) = builder.finish();
    let artifact = ReplicaArtifact::Segment {
        fence: 4,
        seg: SegId(9),
    };
    let checksum = crate::format::crc32c(&segment);
    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaPut {
                vset: VSET,
                assignment_epoch: 1,
                artifact,
                checksum,
                bytes: segment,
            },
        },
        &NoMem,
    );
    let append_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaAppend { io, .. } => Some(*io),
            _ => None,
        })
        .expect("new artifact is durably appended");
    assert!(
        daemon
            .step(Event::BlobWriteDone { io: append_io }, &NoMem)
            .contains(&Effect::PeerSend {
                to: HostId(0),
                msg: PeerMsg::ReplicaPutAck {
                    vset: VSET,
                    assignment_epoch: 1,
                    artifact,
                    checksum,
                },
            })
    );

    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaRelease {
                vset: VSET,
                assignment_epoch: 1,
                through: covered,
            },
        },
        &NoMem,
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::ReplicaDelete { .. })),
        "release through X must retain an ACKed artifact awaiting commit Y: {effects:?}"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn passive_replica_appends_idempotently_commits_and_recovers_status() {
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
        PeerCandidate {
            host: HostId(2),
            weight: 1,
            failure_domain: 3,
            drained: false,
        },
    ];
    let target = rank_stash_candidates(6, HostId(0), 1, VSET, &roster)[0];
    let target_domain = roster
        .iter()
        .find(|candidate| candidate.host == target)
        .expect("target in roster")
        .failure_domain;
    let daemon_config = DaemonConfig {
        archive: ArchivePolicy::default(),
        host: target,
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 6,
            local_failure_domain: target_domain,
            roster,
        }),
    };
    let (mut daemon, _) = Daemon::new(daemon_config.clone());
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let mut builder = SegmentBuilder::new(VSET, 4, SegId(3));
    builder.add(page, Gen(2), &vec![0x5A; page_size()]);
    let (segment, locs) = builder.finish();
    let artifact = ReplicaArtifact::Segment {
        fence: 4,
        seg: SegId(3),
    };
    let info = ReplicaCommitInfo {
        writer_fence: 4,
        seq: JournalSeq(8),
        sync_covered_through: 12,
    };
    let record = JournalRecord {
        config: VsetConfig { ..config() },
        seq: info.seq,
        fence: info.writer_fence,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 12,
        sync_covered_through: info.sync_covered_through,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::from([(page, (Gen(2), locs[0].2))]),
        leaves: BTreeMap::new(),
        migrated_from: None,
    }
    .encode(VSET);
    let checksum = crate::format::crc32c(&segment);

    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaPut {
                vset: VSET,
                assignment_epoch: 1,
                artifact,
                checksum,
                bytes: segment.clone(),
            },
        },
        &NoMem,
    );
    let [
        Effect::ReplicaAppend {
            io: put_io,
            bytes: artifact_frame,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("verified put must append exactly once: {effects:?}");
    };
    let artifact_frame = artifact_frame.clone();
    assert_eq!(
        daemon.step(Event::BlobWriteDone { io: *put_io }, &NoMem),
        [Effect::PeerSend {
            to: HostId(0),
            msg: PeerMsg::ReplicaPutAck {
                vset: VSET,
                assignment_epoch: 1,
                artifact,
                checksum,
            },
        }]
    );

    let duplicate = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaPut {
                vset: VSET,
                assignment_epoch: 1,
                artifact,
                checksum,
                bytes: segment.clone(),
            },
        },
        &NoMem,
    );
    assert!(matches!(duplicate.as_slice(), [Effect::PeerSend { .. }]));

    daemon
        .replicas
        .get_mut(&ReplicaKey {
            source: HostId(0),
            vset: VSET,
            assignment_epoch: 1,
        })
        .expect("replica state")
        .current_file_bytes = super::replica::MAX_REPLICA_SPOOL_GENERATION_BYTES;

    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaCommit {
                vset: VSET,
                assignment_epoch: 1,
                info,
                required: vec![artifact],
                record: record.clone(),
            },
        },
        &NoMem,
    );
    let [
        Effect::ReplicaAppend {
            io: commit_io,
            generation: commit_generation,
            bytes: commit_frame,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("complete closure must append one commit footer: {effects:?}");
    };
    assert_eq!(
        *commit_generation, 1,
        "full generation rotates before append"
    );
    let commit_frame = commit_frame.clone();
    let effects = daemon.step(Event::BlobWriteDone { io: *commit_io }, &NoMem);
    assert!(effects.contains(&Effect::PeerSend {
        to: HostId(0),
        msg: PeerMsg::ReplicaCommitAck {
            vset: VSET,
            assignment_epoch: 1,
            info,
        },
    }));
    assert!(
        daemon
            .step(
                Event::PeerDelivered {
                    from: HostId(0),
                    msg: PeerMsg::ReplicaRelease {
                        vset: VSET,
                        assignment_epoch: 1,
                        through: info,
                    },
                },
                &NoMem,
            )
            .iter()
            .all(|effect| !matches!(effect, Effect::ReplicaDelete { .. })),
        "a durable commit remains until its upload is covered"
    );
    let archive_generation = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::SetTimer {
                timer:
                    TimerId::ReplicaUpload {
                        source: HostId(0),
                        vset: VSET,
                        assignment_epoch: 1,
                        generation,
                    },
                ..
            } => Some(*generation),
            _ => None,
        })
        .expect("archive cadence timer");
    let archive_effects = daemon.step(
        Event::Timer(TimerId::ReplicaUpload {
            source: HostId(0),
            vset: VSET,
            assignment_epoch: 1,
            generation: archive_generation,
        }),
        &NoMem,
    );
    let artifact_put = archive_effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StorePut { io, key, bytes } => Some((*io, key.clone(), bytes.clone())),
            _ => None,
        })
        .expect("the cadence packs and uploads the passive cut");
    let archive_fence = u64::MAX - info.seq.0;
    assert_eq!(
        artifact_put.1,
        layout::segment_key(VSET, archive_fence, SegId(0))
    );
    let (_, packed_fence, packed_seg, packed_entries) =
        crate::segment::scan_segment(&artifact_put.2).expect("valid passive pack");
    assert_eq!((packed_fence, packed_seg), (archive_fence, SegId(0)));
    assert_eq!(packed_entries.len(), 1);
    let effects = daemon.step(
        Event::StorePutDone {
            io: artifact_put.0,
            result: Ok(1),
        },
        &NoMem,
    );
    let (manifest_io, packed_record) = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StorePut { io, key, bytes }
                if *key == layout::manifest_key(VSET, 4, JournalSeq(8)) =>
            {
                Some((*io, bytes.clone()))
            }
            _ => None,
        })
        .expect("artifact completion uploads the manifest");
    let effects = daemon.step(
        Event::StorePutDone {
            io: manifest_io,
            result: Ok(1),
        },
        &NoMem,
    );
    assert!(effects.contains(&Effect::PeerSend {
        to: HostId(0),
        msg: PeerMsg::ReplicaUploadDone {
            vset: VSET,
            assignment_epoch: 1,
            info,
            record: packed_record,
        },
    }));
    let awaiting_head_generation = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::SetTimer {
                timer: TimerId::ReplicaUpload { generation, .. },
                ..
            } => Some(*generation),
            _ => None,
        })
        .expect("head publication remains age-tracked");
    let effects = daemon.step(
        Event::Timer(TimerId::ReplicaUpload {
            source: HostId(0),
            vset: VSET,
            assignment_epoch: 1,
            generation: awaiting_head_generation,
        }),
        &NoMem,
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::StorePut { .. }))
    );
    assert_eq!(
        daemon.replica_spool_metrics()[0].unarchived_age_ns,
        daemon.config.archive.interval * 2,
        "manifest upload alone must not advance the archived frontier"
    );

    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaRelease {
                vset: VSET,
                assignment_epoch: 1,
                through: info,
            },
        },
        &NoMem,
    );
    let delete_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaDelete {
                io,
                through_generation,
                ..
            } => {
                assert_eq!(*through_generation, 1);
                Some(*io)
            }
            _ => None,
        })
        .expect("covering release unlinks the sealed spool without rewrite");
    assert!(
        daemon
            .step(Event::ReplicaDeleteFailed { io: delete_io }, &NoMem)
            .is_empty(),
        "a failed unlink must not acknowledge release"
    );
    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaRelease {
                vset: VSET,
                assignment_epoch: 1,
                through: info,
            },
        },
        &NoMem,
    );
    let delete_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaDelete { io, .. } => Some(*io),
            _ => None,
        })
        .expect("the source's release retry retries the durable unlink");
    assert_eq!(
        daemon.step(Event::BlobWriteDone { io: delete_io }, &NoMem),
        [Effect::PeerSend {
            to: HostId(0),
            msg: PeerMsg::ReplicaReleaseAck {
                vset: VSET,
                assignment_epoch: 1,
                through: info,
            },
        }]
    );

    let spool_capacity = daemon.config.archive.spool_capacity_bytes;
    daemon.replicas.insert(
        ReplicaKey {
            source: HostId(0),
            vset: VSET,
            assignment_epoch: 1,
        },
        PassiveReplica {
            stored_bytes: spool_capacity,
            ..PassiveReplica::default()
        },
    );
    assert!(
        daemon
            .step(
                Event::PeerDelivered {
                    from: HostId(0),
                    msg: PeerMsg::ReplicaPut {
                        vset: VSET,
                        assignment_epoch: 1,
                        artifact,
                        checksum,
                        bytes: segment,
                    },
                },
                &NoMem,
            )
            .is_empty(),
        "capacity exhaustion must stall without append, ACK, or eviction"
    );
    let mut horizon_builder = SegmentBuilder::new(VSET, 4, SegId(4));
    horizon_builder.add(page, Gen(3), &vec![0x6B; page_size()]);
    let (horizon_segment, _) = horizon_builder.finish();
    let horizon_artifact = ReplicaArtifact::Segment {
        fence: 4,
        seg: SegId(4),
    };
    {
        let replica = daemon
            .replicas
            .get_mut(&ReplicaKey {
                source: HostId(0),
                vset: VSET,
                assignment_epoch: 1,
            })
            .expect("replica state");
        replica.stored_bytes = 0;
        replica.unarchived_age = u64::MAX;
    }
    let backpressure_before = daemon.counters.replica_capacity_backpressure;
    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaPut {
                vset: VSET,
                assignment_epoch: 1,
                artifact: horizon_artifact,
                checksum: crate::format::crc32c(&horizon_segment),
                bytes: horizon_segment,
            },
        },
        &NoMem,
    );
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReplicaAppend { .. })),
        "arbitrarily old archive lag must not withhold a capacity-safe append"
    );
    assert_eq!(
        daemon.counters.replica_capacity_backpressure, backpressure_before,
        "archive age is observability only, never admission control"
    );

    let mut spool = artifact_frame.clone();
    spool.extend(commit_frame.clone());
    let valid_spool_len = spool.len() as u64;
    spool.extend([0xAA, 0xBB, 0xCC]);
    let spool_name = layout::replica_spool_blob(HostId(0), VSET, 1);
    let (mut recovered, _, recover_effects) = Daemon::recover(
        daemon_config,
        [(spool_name.as_str(), spool.as_slice())].into_iter(),
    );
    assert!(recover_effects.iter().any(|effect| matches!(
        effect,
        Effect::ReplicaTruncate { len, .. } if *len == valid_spool_len
    )));
    assert_eq!(
        recovered.step(
            Event::PeerDelivered {
                from: HostId(0),
                msg: PeerMsg::ReplicaStatus {
                    vset: VSET,
                    assignment_epoch: 1,
                },
            },
            &NoMem,
        ),
        [Effect::PeerSend {
            to: HostId(0),
            msg: PeerMsg::ReplicaStatusReply {
                vset: VSET,
                assignment_epoch: 1,
                committed: Some(info),
            },
        }]
    );

    let generation_zero = layout::replica_spool_blob(HostId(0), VSET, 1);
    let generation_one = layout::replica_spool_segment_blob(HostId(0), VSET, 1, 1);
    let (mut rotated, _, rotate_effects) = Daemon::recover(
        daemon.config.clone(),
        [
            (generation_zero.as_str(), artifact_frame.as_slice()),
            (generation_one.as_str(), commit_frame.as_slice()),
        ]
        .into_iter(),
    );
    assert!(
        !rotate_effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReplicaTruncate { .. }))
    );
    assert_eq!(rotated.counters.replica_rotations, 1);
    assert_eq!(
        rotated.step(
            Event::PeerDelivered {
                from: HostId(0),
                msg: PeerMsg::ReplicaStatus {
                    vset: VSET,
                    assignment_epoch: 1,
                },
            },
            &NoMem,
        ),
        [Effect::PeerSend {
            to: HostId(0),
            msg: PeerMsg::ReplicaStatusReply {
                vset: VSET,
                assignment_epoch: 1,
                committed: Some(info),
            },
        }]
    );
}

fn capacity_test_passive(capacity: u64, headroom: u64) -> (Daemon, HostId) {
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
        PeerCandidate {
            host: HostId(2),
            weight: 1,
            failure_domain: 3,
            drained: false,
        },
    ];
    let source = HostId(0);
    let target = rank_stash_candidates(6, source, 1, VSET, &roster)[0];
    let target_domain = roster
        .iter()
        .find(|candidate| candidate.host == target)
        .expect("target in roster")
        .failure_domain;
    let archive = ArchivePolicy {
        spool_capacity_bytes: capacity,
        spool_headroom_bytes: headroom,
        ..ArchivePolicy::default()
    };
    let (daemon, _) = Daemon::new(DaemonConfig {
        archive,
        host: target,
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 6,
            local_failure_domain: target_domain,
            roster,
        }),
    });
    (daemon, source)
}

fn capacity_test_put(daemon: &mut Daemon, source: HostId, seg: u64) -> Vec<Effect> {
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(u32::try_from(seg).expect("test segment fits")),
    };
    let mut builder = SegmentBuilder::new(VSET, 4, SegId(seg));
    builder.add(page, Gen(seg), &vec![0x5A; page_size()]);
    let (bytes, _) = builder.finish();
    daemon.step(
        Event::PeerDelivered {
            from: source,
            msg: PeerMsg::ReplicaPut {
                vset: VSET,
                assignment_epoch: 1,
                artifact: ReplicaArtifact::Segment {
                    fence: 4,
                    seg: SegId(seg),
                },
                checksum: crate::format::crc32c(&bytes),
                bytes,
            },
        },
        &NoMem,
    )
}

#[test]
fn passive_capacity_is_host_wide_across_source_hosts() {
    let capacity = 1_000_000;
    let (mut daemon, source) = capacity_test_passive(capacity, 100_000);
    daemon.replicas.insert(
        ReplicaKey {
            source: HostId(2),
            vset: VsetId(99),
            assignment_epoch: 1,
        },
        PassiveReplica {
            stored_bytes: capacity - 1,
            ..PassiveReplica::default()
        },
    );

    assert!(
        !capacity_test_put(&mut daemon, source, 31)
            .iter()
            .any(|effect| matches!(effect, Effect::ReplicaAppend { .. })),
        "another source's residue must consume the same physical spool capacity"
    );
}

#[test]
fn passive_capacity_counts_appends_in_flight_on_other_vsets() {
    let capacity = 1_000_000;
    let (mut daemon, source) = capacity_test_passive(capacity, 100_000);
    daemon.pending.insert(
        IoId(99),
        Pending::ReplicaArtifactAppend {
            source,
            vset: VsetId(98),
            assignment_epoch: 1,
            artifact: ReplicaArtifact::Segment {
                fence: 4,
                seg: SegId(98),
            },
            checksum: 0,
            bytes: Vec::new(),
            frame_len: capacity - 1,
        },
    );

    assert!(
        !capacity_test_put(&mut daemon, source, 32)
            .iter()
            .any(|effect| matches!(effect, Effect::ReplicaAppend { .. })),
        "concurrent durable appends must reserve capacity before completion"
    );
}

#[test]
fn an_inflight_closure_can_consume_soft_headroom_until_hard_capacity() {
    let capacity = 1_000_000;
    let headroom = 250_000;
    let (mut daemon, source) = capacity_test_passive(capacity, headroom);
    daemon.replicas.insert(
        ReplicaKey {
            source,
            vset: VSET,
            assignment_epoch: 1,
        },
        PassiveReplica {
            stored_bytes: capacity - headroom,
            uncommitted_artifacts: [ReplicaArtifact::Segment {
                fence: 4,
                seg: SegId(30),
            }]
            .into_iter()
            .collect(),
            ..PassiveReplica::default()
        },
    );

    assert!(
        capacity_test_put(&mut daemon, source, 33)
            .iter()
            .any(|effect| matches!(effect, Effect::ReplicaAppend { .. })),
        "soft headroom must remain available to finish a closure already in progress"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn passive_spool_compaction_is_crash_safe_and_recovers_the_newest_cut_without_store() {
    let roster = vec![
        PeerCandidate {
            host: HostId(0),
            weight: 1,
            failure_domain: 1,
            drained: false,
        },
        PeerCandidate {
            host: HostId(1),
            weight: 1,
            failure_domain: 2,
            drained: false,
        },
    ];
    let target = rank_stash_candidates(6, HostId(0), 1, VSET, &roster)[0];
    let target_domain = roster
        .iter()
        .find(|candidate| candidate.host == target)
        .expect("target")
        .failure_domain;
    let daemon_config = DaemonConfig {
        archive: ArchivePolicy {
            interval: crate::types::secs(100),
            max_unpublished_bytes: u64::MAX,
            ..Default::default()
        },
        host: target,
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 6,
            local_failure_domain: target_domain,
            roster,
        }),
    };
    let (mut daemon, _) = Daemon::new(daemon_config.clone());
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let mut old_log = Vec::new();
    let mut compact_frame = None;
    let mut compact_io = None;
    let mut newest = None;

    for seq in 1_u64..=10 {
        let mut builder = SegmentBuilder::new(VSET, 4, SegId(seq));
        builder.add(
            page,
            Gen(seq),
            &vec![u8::try_from(seq).expect("test sequence fits"); page_size()],
        );
        let (segment, locs) = builder.finish();
        let artifact = ReplicaArtifact::Segment {
            fence: 4,
            seg: SegId(seq),
        };
        let checksum = crate::format::crc32c(&segment);
        let effects = daemon.step(
            Event::PeerDelivered {
                from: HostId(0),
                msg: PeerMsg::ReplicaPut {
                    vset: VSET,
                    assignment_epoch: 1,
                    artifact,
                    checksum,
                    bytes: segment,
                },
            },
            &NoMem,
        );
        let (put_io, put_frame) = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReplicaAppend { io, bytes, .. } => Some((*io, bytes.clone())),
                _ => None,
            })
            .expect("artifact append");
        old_log.extend(put_frame);
        daemon.step(Event::BlobWriteDone { io: put_io }, &NoMem);

        let info = ReplicaCommitInfo {
            writer_fence: 4,
            seq: JournalSeq(seq),
            sync_covered_through: seq,
        };
        let record = JournalRecord {
            config: VsetConfig { ..config() },
            seq: info.seq,
            fence: info.writer_fence,
            kind: crate::journal::RecordKind::Commit,
            capture_seq: seq,
            sync_covered_through: seq,
            database: crate::journal::DatabaseMeta::default(),
            overlay: BTreeMap::from([(page, (Gen(seq), locs[0].2))]),
            leaves: BTreeMap::new(),
            migrated_from: None,
        }
        .encode(VSET);
        let effects = daemon.step(
            Event::PeerDelivered {
                from: HostId(0),
                msg: PeerMsg::ReplicaCommit {
                    vset: VSET,
                    assignment_epoch: 1,
                    info,
                    required: vec![artifact],
                    record,
                },
            },
            &NoMem,
        );
        let (commit_io, commit_frame) = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::ReplicaAppend { io, bytes, .. } => Some((*io, bytes.clone())),
                _ => None,
            })
            .expect("commit append");
        old_log.extend(commit_frame);
        newest = Some(info);
        let effects = daemon.step(Event::BlobWriteDone { io: commit_io }, &NoMem);
        if let Some((io, bytes)) = effects.iter().find_map(|effect| match effect {
            Effect::ReplicaAppend {
                io,
                generation,
                bytes,
                ..
            } if *generation > 0 => Some((*io, bytes.clone())),
            _ => None,
        }) {
            compact_io = Some(io);
            compact_frame = Some(bytes);
            break;
        }
    }

    let newest = newest.expect("committed cuts");
    let compact_io = compact_io.expect("superseded history triggers compaction");
    let compact_frame = compact_frame.expect("fresh compact generation");
    let compact_scan = crate::replica_spool::scan_replica_spool(&compact_frame)
        .expect("compact generation is independently valid");
    assert_eq!(compact_scan.commits.last().expect("commit").info, newest);
    assert_eq!(compact_scan.commits.len(), 1);
    assert_eq!(compact_scan.artifacts.len(), 1);

    let mut crash_between_write_and_delete = old_log;
    crash_between_write_and_delete.extend_from_slice(&compact_frame);
    let crash_scan = crate::replica_spool::scan_replica_spool(&crash_between_write_and_delete)
        .expect("old and new generations coexist safely after a crash");
    assert_eq!(
        crash_scan.commits.last().expect("newest commit").info,
        newest
    );

    let effects = daemon.step(Event::BlobWriteDone { io: compact_io }, &NoMem);
    let delete_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaDelete { io, .. } => Some(*io),
            _ => None,
        })
        .expect("fresh generation fsync precedes old-generation unlink");
    daemon.step(Event::BlobWriteDone { io: delete_io }, &NoMem);
    let metrics = daemon.replica_spool_metrics();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].stored_bytes, compact_frame.len() as u64);
    assert_eq!(
        daemon.counters.replica_cleanup_rewrite_bytes,
        compact_frame.len() as u64
    );

    let compact_artifact = *compact_scan
        .artifacts
        .keys()
        .next()
        .expect("retained artifact");
    let dangling_info = ReplicaCommitInfo {
        writer_fence: 4,
        seq: JournalSeq(newest.seq.0 - 1),
        sync_covered_through: newest.sync_covered_through - 1,
    };
    let dangling_record = JournalRecord {
        config: VsetConfig { ..config() },
        seq: dangling_info.seq,
        fence: dangling_info.writer_fence,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: dangling_info.seq.0,
        sync_covered_through: dangling_info.sync_covered_through,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    }
    .encode(VSET);
    let dangling_old_suffix = crate::replica_spool::seal_replica_commit(
        HostId(0),
        VSET,
        1,
        dangling_info,
        &[compact_artifact],
        &dangling_record,
    )
    .unwrap();
    assert!(
        crate::replica_spool::scan_replica_spool(&dangling_old_suffix).is_err(),
        "partial deletion left a commit whose earlier artifact generation is gone"
    );
    let dangling_name = layout::replica_spool_segment_blob(HostId(0), VSET, 1, 0);
    let compact_name = layout::replica_spool_segment_blob(HostId(0), VSET, 1, 1);
    let (mut recovered, _, _) = Daemon::recover(
        daemon_config,
        [
            (dangling_name.as_str(), dangling_old_suffix.as_slice()),
            (compact_name.as_str(), compact_frame.as_slice()),
        ]
        .into_iter(),
    );
    assert_eq!(
        recovered.step(
            Event::PeerDelivered {
                from: HostId(0),
                msg: PeerMsg::ReplicaStatus {
                    vset: VSET,
                    assignment_epoch: 1,
                },
            },
            &NoMem,
        ),
        [Effect::PeerSend {
            to: HostId(0),
            msg: PeerMsg::ReplicaStatusReply {
                vset: VSET,
                assignment_epoch: 1,
                committed: Some(newest),
            },
        }]
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn newer_cyclic_assignment_commit_durably_reclaims_older_epoch_on_the_same_peer() {
    let roster: Vec<_> = (0..5)
        .map(|host| PeerCandidate {
            host: HostId(host),
            weight: 1,
            failure_domain: host + 1,
            drained: false,
        })
        .collect();
    let ranked = rank_stash_candidates(6, HostId(0), 1, VSET, &roster);
    let target = ranked[0];
    let target_domain = roster
        .iter()
        .find(|candidate| candidate.host == target)
        .unwrap()
        .failure_domain;
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: target,
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(ReplicaPlacementConfig {
            membership_epoch: 6,
            local_failure_domain: target_domain,
            roster,
        }),
    });
    let old_key = ReplicaKey {
        source: HostId(0),
        vset: VSET,
        assignment_epoch: 1,
    };
    daemon.replicas.insert(
        old_key,
        PassiveReplica {
            stored_bytes: 1234,
            current_generation: 2,
            ..PassiveReplica::default()
        },
    );
    let new_epoch = 1 + u64::try_from(ranked.len()).expect("test roster fits");
    let index = usize::try_from(new_epoch - 1).expect("test epoch fits");
    assert_eq!(ranked[index % ranked.len()], target);
    let page = PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        },
        page: PageNo(0),
    };
    let mut builder = SegmentBuilder::new(VSET, 4, SegId(55));
    builder.add(page, Gen(55), &vec![0x55; page_size()]);
    let (segment, locs) = builder.finish();
    let artifact = ReplicaArtifact::Segment {
        fence: 4,
        seg: SegId(55),
    };
    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaPut {
                vset: VSET,
                assignment_epoch: new_epoch,
                artifact,
                checksum: crate::format::crc32c(&segment),
                bytes: segment,
            },
        },
        &NoMem,
    );
    let put_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaAppend { io, .. } => Some(*io),
            _ => None,
        })
        .expect("new epoch artifact");
    daemon.step(Event::BlobWriteDone { io: put_io }, &NoMem);
    let info = ReplicaCommitInfo {
        writer_fence: 4,
        seq: JournalSeq(55),
        sync_covered_through: 55,
    };
    let record = JournalRecord {
        config: VsetConfig { ..config() },
        seq: info.seq,
        fence: info.writer_fence,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 55,
        sync_covered_through: 55,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::from([(page, (Gen(55), locs[0].2))]),
        leaves: BTreeMap::new(),
        migrated_from: None,
    }
    .encode(VSET);
    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(0),
            msg: PeerMsg::ReplicaCommit {
                vset: VSET,
                assignment_epoch: new_epoch,
                info,
                required: vec![artifact],
                record,
            },
        },
        &NoMem,
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::ReplicaDelete { .. })),
        "old epoch remains until the replacement commit is durable"
    );
    let commit_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaAppend { io, .. } => Some(*io),
            _ => None,
        })
        .expect("new epoch commit");
    let effects = daemon.step(Event::BlobWriteDone { io: commit_io }, &NoMem);
    let delete_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::ReplicaDelete {
                io,
                assignment_epoch: 1,
                through_generation: 2,
                ..
            } => Some(*io),
            _ => None,
        })
        .expect("covering replacement retires the old local epoch");
    assert!(daemon.replicas.contains_key(&old_key));
    daemon.step(Event::BlobWriteDone { io: delete_io }, &NoMem);
    assert!(!daemon.replicas.contains_key(&old_key));
    assert!(daemon.replicas.contains_key(&ReplicaKey {
        source: HostId(0),
        vset: VSET,
        assignment_epoch: new_epoch,
    }));
}

#[test]
#[allow(clippy::too_many_lines)]
fn passive_archive_pack_rewrites_leaf_entries_and_is_restart_deterministic() {
    let pages = [
        PageId {
            volume: VolumeId {
                vset: VSET,
                idx: VolumeIdx(1),
            },
            page: PageNo(3),
        },
        PageId {
            volume: VolumeId {
                vset: VSET,
                idx: VolumeIdx(1),
            },
            page: PageNo(7),
        },
    ];
    let mut source_artifacts = BTreeMap::new();
    let mut leaf_entries = Vec::new();
    let mut required = Vec::new();
    for (index, page) in pages.into_iter().enumerate() {
        let seg = SegId(10 + index as u64);
        let generation = Gen(20 + index as u64);
        let mut builder = SegmentBuilder::new(VSET, 4, seg);
        let fill = 0x40 + u8::try_from(index).expect("test index fits");
        builder.add(page, generation, &vec![fill; page_size()]);
        let (bytes, entries) = builder.finish();
        let artifact = ReplicaArtifact::Segment { fence: 4, seg };
        source_artifacts.insert(artifact, (crate::format::crc32c(&bytes), bytes));
        required.push(artifact);
        leaf_entries.push((page.volume.idx, page.page, generation, entries[0].2));
    }
    let leaf = crate::mapleaf::MapLeaf {
        span: crate::mapleaf::span_of(pages[0]),
        entries: leaf_entries,
    };
    let leaf_ptr = crate::mapleaf::LeafPtr {
        base: 0,
        fence: 4,
        id: 6,
    };
    let leaf_bytes = leaf.encode(VSET, leaf_ptr.fence, leaf_ptr.id);
    let leaf_artifact = ReplicaArtifact::Leaf {
        fence: leaf_ptr.fence,
        id: leaf_ptr.id,
    };
    source_artifacts.insert(
        leaf_artifact,
        (crate::format::crc32c(&leaf_bytes), leaf_bytes),
    );
    required.push(leaf_artifact);
    let info = ReplicaCommitInfo {
        writer_fence: 4,
        seq: JournalSeq(12),
        sync_covered_through: 19,
    };
    let record = JournalRecord {
        config: config(),
        seq: info.seq,
        fence: info.writer_fence,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 19,
        sync_covered_through: info.sync_covered_through,
        database: crate::journal::DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::from([(leaf.span, leaf_ptr)]),
        migrated_from: None,
    }
    .encode(VSET);
    let replica = PassiveReplica {
        artifacts: source_artifacts,
        ..PassiveReplica::default()
    };
    let candidate = || ReplicaUpload {
        info,
        todo: required.clone(),
        record: record.clone(),
        derived: BTreeMap::new(),
        inflight: false,
    };
    let first = Daemon::pack_replica_upload(VSET, &replica, candidate());
    let after_restart = Daemon::pack_replica_upload(VSET, &replica, candidate());
    assert_eq!(first.record, after_restart.record);
    assert_eq!(first.todo, after_restart.todo);
    assert_eq!(first.derived, after_restart.derived);

    let archive_fence = u64::MAX - info.seq.0;
    let segment_artifacts: Vec<_> = first
        .derived
        .keys()
        .filter(|artifact| matches!(artifact, ReplicaArtifact::Segment { .. }))
        .copied()
        .collect();
    assert_eq!(
        segment_artifacts,
        [ReplicaArtifact::Segment {
            fence: archive_fence,
            seg: SegId(0),
        }]
    );
    let packed_record = JournalRecord::decode(VSET, &first.record).expect("packed record");
    let packed_ptr = packed_record.leaves[&leaf.span];
    assert_eq!((packed_ptr.fence, packed_ptr.id), (archive_fence, 0));
    let packed_leaf_artifact = ReplicaArtifact::Leaf {
        fence: packed_ptr.fence,
        id: packed_ptr.id,
    };
    let packed_leaf = crate::mapleaf::MapLeaf::decode(
        VSET,
        packed_ptr.fence,
        packed_ptr.id,
        &first.derived[&packed_leaf_artifact],
    )
    .expect("packed leaf");
    assert!(
        packed_leaf
            .entries
            .iter()
            .all(|(_, _, _, loc)| (loc.fence, loc.seg) == (archive_fence, SegId(0)))
    );
}

#[test]
fn passive_upload_backlog_keeps_only_the_latest_complete_commit() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(1),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let key = ReplicaKey {
        source: HostId(0),
        vset: VSET,
        assignment_epoch: 1,
    };
    daemon.replicas.insert(
        key,
        PassiveReplica {
            upload: Some(ReplicaUpload {
                info: ReplicaCommitInfo {
                    writer_fence: 4,
                    seq: JournalSeq(1),
                    sync_covered_through: 1,
                },
                todo: Vec::new(),
                record: vec![1],
                derived: BTreeMap::new(),
                inflight: true,
            }),
            ..PassiveReplica::default()
        },
    );

    for seq in 2..=1_000 {
        let info = ReplicaCommitInfo {
            writer_fence: 4,
            seq: JournalSeq(seq),
            sync_covered_through: seq,
        };
        daemon
            .replicas
            .get_mut(&key)
            .expect("replica")
            .pending_commit = Some(ReplicaPendingCommit {
            info,
            required: Vec::new(),
            record: seq.to_le_bytes().to_vec(),
        });
        let mut effects = Vec::new();
        daemon.replica_append_done(
            Pending::ReplicaCommitAppend {
                source: key.source,
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
                info,
                record_checksum: u32::try_from(seq).expect("test sequence fits checksum field"),
                frame_len: 1,
            },
            &mut effects,
        );
        assert!(effects.contains(&Effect::PeerSend {
            to: key.source,
            msg: PeerMsg::ReplicaCommitAck {
                vset: key.vset,
                assignment_epoch: key.assignment_epoch,
                info,
            },
        }));
    }

    let replica = &daemon.replicas[&key];
    assert_eq!(replica.upload_queue.len(), 1);
    let latest = replica.upload_queue.front().expect("latest queued upload");
    assert_eq!(latest.info.seq, JournalSeq(1_000));
    assert_eq!(latest.info.sync_covered_through, 1_000);
}

#[test]
fn checkpoint_cost_scales_with_the_delta_not_the_volume() {
    // R3.3: the first capture of a never-checkpointed volume is whole-
    // written-set sized; every later checkpoint pays only for what changed.
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: crate::types::HostId(0),
        cache_pages: 256,
        writeback_interval: 1_000_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let config = VsetConfig::compute(1, 64);
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

#[test]
fn large_checkpoint_resumes_after_batch_arm_before_any_page_copy() {
    struct CountingMem(std::cell::Cell<usize>);
    impl HostMap for CountingMem {
        fn read_page(&self, _page: PageId) -> Vec<u8> {
            self.0.set(self.0.get() + 1);
            vec![0; page_size()]
        }
    }

    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(0),
        cache_pages: 80,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let mem = CountingMem(std::cell::Cell::new(0));
    let _ = step_settled_with_mem(
        &mut daemon,
        Event::Admin(AdminCmd::CreateVset {
            req: ReqId(800),
            vset: VSET,
            config: VsetConfig::compute(1, 80),
            from_base: None,
        }),
        &mem,
    );
    for n in 0..65 {
        let _ = daemon.step(
            Event::GuestFault {
                page: PageId {
                    volume: VolumeId {
                        vset: VSET,
                        idx: VolumeIdx(1),
                    },
                    page: PageNo(n),
                },
                write: true,
            },
            &mem,
        );
    }
    let _ = daemon.step(
        Event::Admin(AdminCmd::Checkpoint {
            req: ReqId(801),
            vset: VSET,
        }),
        &mem,
    );
    let paused = daemon.step(
        Event::GuestPaused {
            vset: VSET,
            vmstate: 9,
        },
        &mem,
    );
    assert_eq!(mem.0.get(), 0, "pause event performs no page copies");
    assert!(paused.iter().any(|effect| matches!(
        effect,
        Effect::WriteProtect { pages } if pages.len() == 65
    )));
    assert!(paused.iter().any(|effect| matches!(
        effect,
        Effect::SetTimer {
            timer: TimerId::CaptureStep(VSET),
            ..
        }
    )));
    assert!(
        paused
            .iter()
            .any(|effect| matches!(effect, Effect::ResumeGuest { vset: VSET }))
    );
    assert!(
        !paused
            .iter()
            .any(|effect| matches!(effect, Effect::BlobWrite { .. }))
    );

    let first = daemon.step(Event::Timer(TimerId::CaptureStep(VSET)), &mem);
    assert_eq!(mem.0.get(), 64);
    assert!(first.iter().any(|effect| matches!(
        effect,
        Effect::SetTimer {
            timer: TimerId::CaptureStep(VSET),
            ..
        }
    )));
}

#[derive(Default)]
struct DatabaseMem(
    std::cell::RefCell<BTreeMap<PageId, Vec<u8>>>,
    std::cell::RefCell<BTreeMap<String, Vec<u8>>>,
);

impl HostMap for DatabaseMem {
    fn read_page(&self, page: PageId) -> Vec<u8> {
        self.0.borrow()[&page].clone()
    }
}

fn database_step(daemon: &mut Daemon, mem: &DatabaseMem, event: Event) -> Vec<Effect> {
    let mut settled = Vec::new();
    let mut queue = VecDeque::from([event]);
    while let Some(event) = queue.pop_front() {
        for effect in daemon.step(event, mem) {
            match effect {
                Effect::DatabaseInstall { page, bytes } => {
                    mem.0.borrow_mut().insert(page, bytes);
                }
                Effect::BlobWrite { io, name, bytes } => {
                    mem.1.borrow_mut().insert(name, bytes);
                    queue.push_back(Event::BlobWriteDone { io });
                }
                Effect::BlobRead { io, name } => queue.push_back(Event::BlobReadDone {
                    io,
                    bytes: mem.1.borrow().get(&name).cloned(),
                }),
                Effect::BlobDelete { name } => {
                    mem.1.borrow_mut().remove(&name);
                }
                Effect::StoreCas {
                    io, expected: None, ..
                } => queue.push_back(Event::StorePutDone { io, result: Ok(1) }),
                Effect::SetTimer {
                    timer: TimerId::DatabaseStep(vset),
                    ..
                } => queue.push_back(Event::Timer(TimerId::DatabaseStep(vset))),
                Effect::SetTimer {
                    timer:
                        TimerId::Replica { .. }
                        | TimerId::ReplicaUpload { .. }
                        | TimerId::ReplicaRelease(_),
                    ..
                } => {}
                Effect::PeerSend {
                    to: HostId(1),
                    msg:
                        PeerMsg::ReplicaStatus {
                            vset,
                            assignment_epoch,
                        },
                } => queue.push_back(Event::PeerDelivered {
                    from: HostId(1),
                    msg: PeerMsg::ReplicaStatusReply {
                        vset,
                        assignment_epoch,
                        committed: None,
                    },
                }),
                Effect::PeerSend {
                    to: HostId(1),
                    msg:
                        PeerMsg::ReplicaPut {
                            vset,
                            assignment_epoch,
                            artifact,
                            checksum,
                            ..
                        },
                } => queue.push_back(Event::PeerDelivered {
                    from: HostId(1),
                    msg: PeerMsg::ReplicaPutAck {
                        vset,
                        assignment_epoch,
                        artifact,
                        checksum,
                    },
                }),
                Effect::PeerSend {
                    to: HostId(1),
                    msg:
                        PeerMsg::ReplicaCommit {
                            vset,
                            assignment_epoch,
                            info,
                            ..
                        },
                } => queue.push_back(Event::PeerDelivered {
                    from: HostId(1),
                    msg: PeerMsg::ReplicaCommitAck {
                        vset,
                        assignment_epoch,
                        info,
                    },
                }),
                other => settled.push(other),
            }
        }
    }
    settled
}

fn database_daemon_with_capacity(
    cache_pages: usize,
    database_pages: u32,
) -> (Daemon, DatabaseMem, AttachmentId) {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: crate::types::HostId(0),
        cache_pages,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let mem = DatabaseMem::default();
    let effects = database_step(
        &mut daemon,
        &mem,
        Event::Admin(AdminCmd::CreateVset {
            req: ReqId(100),
            vset: VSET,
            config: VsetConfig::database(database_pages),
            from_base: None,
        }),
    );
    assert_eq!(
        effects,
        [Effect::Admin(AdminReply::VsetCreated {
            req: ReqId(100),
            vset: VSET,
        })]
    );
    let effects = database_step(
        &mut daemon,
        &mem,
        Event::Admin(AdminCmd::AttachDatabase {
            req: ReqId(101),
            vset: VSET,
            vm: VmId(9),
        }),
    );
    let [Effect::Admin(AdminReply::DatabaseAttached { attachment, .. })] = effects.as_slice()
    else {
        panic!("expected attachment, got {effects:?}");
    };
    (daemon, mem, *attachment)
}

fn database_daemon() -> (Daemon, DatabaseMem, AttachmentId) {
    database_daemon_with_capacity(8, 8)
}

fn database_request(
    daemon: &mut Daemon,
    mem: &DatabaseMem,
    attachment: AttachmentId,
    req: u64,
    op: DatabaseOp,
) -> Vec<Effect> {
    database_step(
        daemon,
        mem,
        Event::Database(DatabaseRequest {
            req: ReqId(req),
            vset: VSET,
            attachment,
            op,
        }),
    )
}

#[test]
fn large_database_write_yields_after_a_bounded_page_slice() {
    let (mut daemon, mem, attachment) = database_daemon_with_capacity(32, 32);
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        810,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    let effects = daemon.step(
        Event::Database(DatabaseRequest {
            req: ReqId(811),
            vset: VSET,
            attachment,
            op: DatabaseOp::Write {
                handle: 1,
                offset: 0,
                bytes: vec![0x5a; page_size() * 17],
            },
        }),
        &mem,
    );
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(effect, Effect::DatabaseInstall { .. }))
            .count(),
        16
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::SetTimer {
            timer: TimerId::DatabaseStep(VSET),
            ..
        }
    )));
    assert!(!effects.iter().any(|effect| matches!(
        effect,
        Effect::Database(DatabaseReply::Written {
            req: ReqId(811),
            ..
        })
    )));
}

#[test]
#[ignore = "performance profile; run explicitly in release mode"]
#[allow(clippy::cast_precision_loss, clippy::disallowed_types)]
fn profile_one_mib_database_write_slices() {
    let pages = MAX_DATABASE_IO.div_ceil(page_size());
    let (mut daemon, mem, attachment) =
        database_daemon_with_capacity(pages + 1, u32::try_from(pages + 1).expect("fits"));
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        820,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );

    let mut next = Some(Event::Database(DatabaseRequest {
        req: ReqId(821),
        vset: VSET,
        attachment,
        op: DatabaseOp::Write {
            handle: 1,
            offset: 0,
            bytes: vec![0x5a; MAX_DATABASE_IO],
        },
    }));
    let mut steps = 0u64;
    let mut installs = 0usize;
    let mut max_installs = 0usize;
    let mut total_ns = 0u64;
    let mut worst_ns = 0u64;
    let mut completed = false;
    while let Some(event) = next.take() {
        let started = std::time::Instant::now();
        let effects = daemon.step(event, &mem);
        let elapsed = u64::try_from(started.elapsed().as_nanos()).expect("fits");
        total_ns += elapsed;
        worst_ns = worst_ns.max(elapsed);
        steps += 1;

        let mut step_installs = 0;
        for effect in effects {
            match effect {
                Effect::DatabaseInstall { page, bytes } => {
                    mem.0.borrow_mut().insert(page, bytes);
                    installs += 1;
                    step_installs += 1;
                }
                Effect::SetTimer {
                    timer: TimerId::DatabaseStep(vset),
                    ..
                } => {
                    assert!(next.is_none(), "one continuation per slice");
                    next = Some(Event::Timer(TimerId::DatabaseStep(vset)));
                }
                Effect::Database(DatabaseReply::Written {
                    req: ReqId(821), ..
                }) => completed = true,
                other => panic!("unexpected database profile effect: {other:?}"),
            }
        }
        max_installs = max_installs.max(step_installs);
    }

    assert!(completed, "write never completed");
    assert_eq!(installs, pages);
    assert!(max_installs <= 16, "slice copied {max_installs} pages");
    eprintln!("── PROFILE: one MiB database write ──");
    eprintln!(
        "  {pages} pages in {steps} event-loop slices; at most {max_installs} pages/slice; \
         mean {:.1}µs, worst {:.1}µs, total {:.1}ms",
        total_ns as f64 / steps as f64 / 1_000.0,
        worst_ns as f64 / 1_000.0,
        total_ns as f64 / 1_000_000.0,
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn database_byte_io_sync_truncate_and_recreate_are_exact() {
    let (mut daemon, mem, attachment) = database_daemon();
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            102,
            DatabaseOp::Open {
                handle: 1,
                file: DatabaseFile::Main,
                create: true,
            },
        ),
        [Effect::Database(DatabaseReply::Opened { req: ReqId(102) })]
    );

    let io_len = page_size() + 904;
    let bytes: Vec<u8> = (0..io_len)
        .map(|i| u8::try_from(i % 251).expect("below 251"))
        .collect();
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            103,
            DatabaseOp::Write {
                handle: 1,
                offset: 100,
                bytes: bytes.clone(),
            },
        ),
        [Effect::Database(DatabaseReply::Written {
            req: ReqId(103),
            sequence: 2,
        })]
    );
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            104,
            DatabaseOp::Read {
                handle: 1,
                offset: 100,
                len: u32::try_from(io_len).expect("test I/O fits u32"),
            },
        ),
        [Effect::Database(DatabaseReply::Read {
            req: ReqId(104),
            bytes: bytes.clone(),
            eof: false,
        })]
    );

    let sync = database_request(
        &mut daemon,
        &mem,
        attachment,
        105,
        DatabaseOp::Sync { handle: 1 },
    );
    assert!(
        sync.iter().any(|effect| matches!(
            effect,
            Effect::Database(DatabaseReply::Synced {
                req: ReqId(105),
                sequence: 2,
            })
        )),
        "sync effects: {sync:?}"
    );

    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            106,
            DatabaseOp::Truncate {
                handle: 1,
                size: 200,
            },
        ),
        [
            Effect::Evict {
                page: DatabaseFile::Main.page(VSET, 1),
            },
            Effect::Database(DatabaseReply::Truncated {
                req: ReqId(106),
                sequence: 3,
            }),
        ]
    );
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            107,
            DatabaseOp::Truncate {
                handle: 1,
                size: 1000,
            },
        ),
        [Effect::Database(DatabaseReply::Truncated {
            req: ReqId(107),
            sequence: 4,
        })]
    );
    let expected = bytes[..100]
        .iter()
        .copied()
        .chain(std::iter::repeat_n(0, 800))
        .collect();
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            108,
            DatabaseOp::Read {
                handle: 1,
                offset: 100,
                len: 900,
            },
        ),
        [Effect::Database(DatabaseReply::Read {
            req: ReqId(108),
            bytes: expected,
            eof: false,
        })]
    );

    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            109,
            DatabaseOp::Delete {
                file: DatabaseFile::Main,
            },
        ),
        [
            Effect::Evict {
                page: DatabaseFile::Main.page(VSET, 0),
            },
            Effect::Database(DatabaseReply::Deleted {
                req: ReqId(109),
                sequence: 5,
            }),
        ]
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        110,
        DatabaseOp::Close { handle: 1 },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        111,
        DatabaseOp::Open {
            handle: 2,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            112,
            DatabaseOp::Read {
                handle: 2,
                offset: 0,
                len: 16,
            },
        ),
        [Effect::Database(DatabaseReply::Read {
            req: ReqId(112),
            bytes: Vec::new(),
            eof: true,
        })]
    );
}

#[test]
fn rejected_database_truncate_does_not_mutate_file() {
    let (mut daemon, mem, attachment) = database_daemon();
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            200,
            DatabaseOp::Open {
                handle: 1,
                file: DatabaseFile::Main,
                create: true,
            },
        ),
        [Effect::Database(DatabaseReply::Opened { req: ReqId(200) })]
    );
    let original = vec![0xa5; 512];
    assert!(matches!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            201,
            DatabaseOp::Write {
                handle: 1,
                offset: 0,
                bytes: original.clone(),
            },
        )
        .as_slice(),
        [Effect::Database(DatabaseReply::Written { .. })]
    ));

    // Lazy hydration of an unrelated database-file span makes a truncate
    // temporarily unavailable, but must not permit a partial tail-page write.
    let unrelated = span_of(DatabaseFile::Wal.page(VSET, 0));
    daemon
        .vsets
        .get_mut(&VSET)
        .expect("database vset")
        .pending_leaves
        .insert(
            unrelated,
            LeafPtr {
                base: 0,
                fence: 1,
                id: 99,
            },
        );
    let sequence_before = daemon.vsets[&VSET].mutation_seq;
    let page_before = mem.0.borrow()[&DatabaseFile::Main.page(VSET, 0)].clone();

    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            202,
            DatabaseOp::Truncate {
                handle: 1,
                size: 100,
            },
        ),
        [Effect::Database(DatabaseReply::Failed {
            req: ReqId(202),
            error: crate::database::DatabaseError::Busy,
        })]
    );
    assert_eq!(daemon.vsets[&VSET].mutation_seq, sequence_before);
    assert_eq!(
        mem.0.borrow()[&DatabaseFile::Main.page(VSET, 0)],
        page_before
    );
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            203,
            DatabaseOp::Read {
                handle: 1,
                offset: 0,
                len: u32::try_from(original.len()).expect("small fixture"),
            },
        ),
        [Effect::Database(DatabaseReply::Read {
            req: ReqId(203),
            bytes: original,
            eof: false,
        })]
    );
}

#[test]
fn database_transient_file_can_be_deleted_before_its_first_capture() {
    let (mut daemon, mem, attachment) = database_daemon();
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        113,
        DatabaseOp::Open {
            handle: 3,
            file: DatabaseFile::Journal,
            create: true,
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        114,
        DatabaseOp::Write {
            handle: 3,
            offset: 0,
            bytes: vec![0x5a; 512],
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        115,
        DatabaseOp::Delete {
            file: DatabaseFile::Journal,
        },
    );

    let effects = database_step(&mut daemon, &mem, Event::Timer(TimerId::Writeback));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::SetTimer {
            timer: TimerId::Writeback,
            ..
        }
    )));
}

#[test]
fn database_write_makes_progress_when_dirty_pages_fill_the_cache() {
    let (mut daemon, mem, attachment) = database_daemon_with_capacity(2, 8);
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        710,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );

    let started = database_request(
        &mut daemon,
        &mem,
        attachment,
        711,
        DatabaseOp::Write {
            handle: 1,
            offset: 0,
            bytes: vec![0x5a; page_size() * 3],
        },
    );
    assert!(
        !started.iter().any(|effect| matches!(
            effect,
            Effect::Database(DatabaseReply::Written {
                req: ReqId(711),
                ..
            })
        )),
        "the write must initially park behind cache pressure"
    );

    let capture_started = daemon.step(Event::Timer(TimerId::Writeback), &mem);
    assert_eq!(
        daemon.vsets[&VSET]
            .captures
            .values()
            .next()
            .expect("pressure capture")
            .capture_seq,
        1,
        "a parked write is not part of the capture consistency point"
    );
    let segment_io = capture_started
        .iter()
        .find_map(|effect| match effect {
            Effect::BlobWrite { io, .. } => Some(*io),
            _ => None,
        })
        .expect("pressure capture segment");
    let segment_done = daemon.step(Event::BlobWriteDone { io: segment_io }, &mem);
    assert!(!segment_done.iter().any(|effect| matches!(
        effect,
        Effect::Database(DatabaseReply::Written {
            req: ReqId(711),
            ..
        })
    )));
    assert!(
        !segment_done
            .iter()
            .any(|effect| matches!(effect, Effect::DatabaseInstall { .. })),
        "the parked write cannot resume before the capture record is durable"
    );

    let record_ios: Vec<_> = segment_done
        .iter()
        .filter_map(|effect| match effect {
            Effect::BlobWrite { io, .. } => Some(*io),
            _ => None,
        })
        .collect();
    assert_eq!(record_ios.len(), 2);
    let mut finalized = Vec::new();
    for io in record_ios {
        for effect in daemon.step(Event::BlobWriteDone { io }, &mem) {
            if let Effect::DatabaseInstall { page, bytes } = &effect {
                mem.0.borrow_mut().insert(*page, bytes.clone());
            }
            finalized.push(effect);
        }
    }
    assert!(finalized.iter().any(|effect| matches!(
        effect,
        Effect::Database(DatabaseReply::Written {
            req: ReqId(711),
            ..
        })
    )));
}

#[test]
fn parked_truncate_does_not_resume_until_the_capture_record_is_durable() {
    let (mut daemon, mem, attachment) = database_daemon_with_capacity(2, 8);
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        730,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        731,
        DatabaseOp::Write {
            handle: 1,
            offset: 0,
            bytes: vec![0x41; page_size()],
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        732,
        DatabaseOp::Sync { handle: 1 },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        733,
        DatabaseOp::Write {
            handle: 1,
            offset: page_size() as u64,
            bytes: vec![0x52; page_size() * 2],
        },
    );
    let parked = database_request(
        &mut daemon,
        &mem,
        attachment,
        734,
        DatabaseOp::Truncate { handle: 1, size: 1 },
    );
    assert!(
        !parked
            .iter()
            .any(|effect| matches!(effect, Effect::BlobReadRange { .. })),
        "truncate must initially park behind dirty-cache pressure"
    );

    let capture_started = daemon.step(Event::Timer(TimerId::Writeback), &mem);
    assert_eq!(
        daemon.vsets[&VSET]
            .captures
            .values()
            .next()
            .expect("truncate pressure capture")
            .capture_seq,
        3
    );
    let segment_io = capture_started
        .iter()
        .find_map(|effect| match effect {
            Effect::BlobWrite { io, .. } => Some(*io),
            _ => None,
        })
        .expect("truncate pressure segment");
    let segment_done = daemon.step(Event::BlobWriteDone { io: segment_io }, &mem);
    assert!(
        !segment_done
            .iter()
            .any(|effect| matches!(effect, Effect::BlobReadRange { .. })),
        "truncate cannot resume while the capture record is still pending"
    );

    let record_ios: Vec<_> = segment_done
        .iter()
        .filter_map(|effect| match effect {
            Effect::BlobWrite { io, .. } => Some(*io),
            _ => None,
        })
        .collect();
    let mut finalized = Vec::new();
    for io in record_ios {
        finalized.extend(daemon.step(Event::BlobWriteDone { io }, &mem));
    }
    assert!(
        finalized
            .iter()
            .any(|effect| matches!(effect, Effect::BlobReadRange { .. })),
        "truncate resumes once the capture consistency point is durable"
    );
}

#[test]
fn database_prune_is_rejected_while_an_incremental_capture_is_armed() {
    let (mut daemon, mem, attachment) = database_daemon_with_capacity(80, 80);
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        720,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    let pages_per_write = crate::database::MAX_DATABASE_IO / page_size();
    let mut written_pages = 0usize;
    let mut req = 721;
    while written_pages < 65 {
        let pages = (65 - written_pages).min(pages_per_write);
        let written = database_request(
            &mut daemon,
            &mem,
            attachment,
            req,
            DatabaseOp::Write {
                handle: 1,
                offset: u64::try_from(written_pages * page_size()).expect("bounded"),
                bytes: vec![0x33; page_size() * pages],
            },
        );
        assert!(written.iter().any(|effect| matches!(
            effect,
            Effect::Database(DatabaseReply::Written {
                req: written_req,
                ..
            }) if *written_req == ReqId(req)
        )));
        written_pages += pages;
        req += 1;
    }

    let armed = daemon.step(Event::Timer(TimerId::Writeback), &mem);
    assert!(armed.iter().any(|effect| matches!(
        effect,
        Effect::SetTimer {
            timer: TimerId::CaptureStep(VSET),
            ..
        }
    )));
    assert!(daemon.vsets[&VSET].drain.is_some());

    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            req,
            DatabaseOp::Delete {
                file: DatabaseFile::Main,
            },
        ),
        [Effect::Database(DatabaseReply::Failed {
            req: ReqId(req),
            error: crate::database::DatabaseError::Busy,
        })]
    );
    assert!(daemon.vsets[&VSET].drain.is_some());
}

#[test]
fn database_attachment_generation_fences_old_requests() {
    let (mut daemon, mem, attachment) = database_daemon();
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        120,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    let effects = database_step(
        &mut daemon,
        &mem,
        Event::Admin(AdminCmd::BeginDetachDatabase {
            req: ReqId(121),
            vset: VSET,
            attachment,
            mode: DetachMode::Forced,
        }),
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::Admin(AdminReply::DatabaseDetachStarted {
            forced: true,
            ..
        })]
    ));
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            attachment,
            122,
            DatabaseOp::Access {
                file: DatabaseFile::Main,
            },
        ),
        [Effect::Database(DatabaseReply::Failed {
            req: ReqId(122),
            error: crate::database::DatabaseError::StaleAttachment,
        })]
    );

    assert!(matches!(
        database_step(
            &mut daemon,
            &mem,
            Event::Admin(AdminCmd::FinishDetachDatabase {
                req: ReqId(123),
                vset: VSET,
                attachment,
            }),
        )
        .as_slice(),
        [Effect::Admin(AdminReply::DatabaseDetached { .. })]
    ));

    let effects = database_step(
        &mut daemon,
        &mem,
        Event::Admin(AdminCmd::AttachDatabase {
            req: ReqId(124),
            vset: VSET,
            vm: VmId(10),
        }),
    );
    let [
        Effect::Admin(AdminReply::DatabaseAttached {
            attachment: replacement,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected replacement attachment, got {effects:?}");
    };
    assert!(replacement.generation > attachment.generation);
    assert_ne!(replacement.vm, attachment.vm);
    assert_eq!(
        database_request(
            &mut daemon,
            &mem,
            *replacement,
            125,
            DatabaseOp::Access {
                file: DatabaseFile::Main,
            },
        ),
        [Effect::Database(DatabaseReply::Access {
            req: ReqId(125),
            exists: true,
        })]
    );
}

#[test]
fn forced_database_detach_finishes_after_an_interleaved_record_finalize() {
    let (mut daemon, mem, attachment) = database_daemon();
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        126,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        127,
        DatabaseOp::Write {
            handle: 1,
            offset: 0,
            bytes: b"dirty before forced detach".to_vec(),
        },
    );
    assert!(matches!(
        database_step(
            &mut daemon,
            &mem,
            Event::Admin(AdminCmd::BeginDetachDatabase {
                req: ReqId(128),
                vset: VSET,
                attachment,
                mode: DetachMode::Forced,
            }),
        )
        .as_slice(),
        [Effect::Admin(AdminReply::DatabaseDetachStarted {
            req: ReqId(128),
            forced: true,
            ..
        })]
    ));

    let _ = database_step(&mut daemon, &mem, Event::Timer(TimerId::Writeback));

    assert!(matches!(
        database_step(
            &mut daemon,
            &mem,
            Event::Admin(AdminCmd::FinishDetachDatabase {
                req: ReqId(129),
                vset: VSET,
                attachment,
            }),
        )
        .as_slice(),
        [Effect::Admin(AdminReply::DatabaseDetached {
            req: ReqId(129),
            ..
        })]
    ));
}

#[test]
fn graceful_database_detach_makes_the_final_mutation_durable() {
    let (mut daemon, mem, attachment) = database_daemon();
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        130,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        131,
        DatabaseOp::Write {
            handle: 1,
            offset: 0,
            bytes: b"durable on detach".to_vec(),
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        132,
        DatabaseOp::Close { handle: 1 },
    );
    let started = database_step(
        &mut daemon,
        &mem,
        Event::Admin(AdminCmd::BeginDetachDatabase {
            req: ReqId(133),
            vset: VSET,
            attachment,
            mode: DetachMode::Graceful,
        }),
    );
    assert!(started.iter().any(|effect| matches!(
        effect,
        Effect::Admin(AdminReply::DatabaseDetachStarted {
            req: ReqId(133),
            forced: false,
            ..
        })
    )));
    assert_eq!(
        database_step(
            &mut daemon,
            &mem,
            Event::Admin(AdminCmd::FinishDetachDatabase {
                req: ReqId(134),
                vset: VSET,
                attachment,
            }),
        ),
        [Effect::Admin(AdminReply::DatabaseDetached {
            req: ReqId(134),
            vset: VSET,
            attachment,
        })]
    );
}

#[test]
fn graceful_database_detach_begin_is_idempotent_while_draining() {
    let (mut daemon, mem, attachment) = database_daemon();
    for req in [135, 136] {
        let effects = database_step(
            &mut daemon,
            &mem,
            Event::Admin(AdminCmd::BeginDetachDatabase {
                req: ReqId(req),
                vset: VSET,
                attachment,
                mode: DetachMode::Graceful,
            }),
        );
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Admin(AdminReply::DatabaseDetachStarted {
                req: reply,
                forced: false,
                ..
            }) if *reply == ReqId(req)
        )));
    }
}

#[test]
fn graceful_database_detach_chains_after_an_inflight_record() {
    let (mut daemon, mem, attachment) = database_daemon();
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        140,
        DatabaseOp::Open {
            handle: 1,
            file: DatabaseFile::Main,
            create: true,
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        141,
        DatabaseOp::Write {
            handle: 1,
            offset: 0,
            bytes: b"detach races a record".to_vec(),
        },
    );
    let _ = database_request(
        &mut daemon,
        &mem,
        attachment,
        142,
        DatabaseOp::Close { handle: 1 },
    );

    let capture = daemon.step(Event::Timer(TimerId::Writeback), &mem);
    for effect in &capture {
        if let Effect::BlobWrite { name, bytes, .. } = effect {
            mem.1.borrow_mut().insert(name.clone(), bytes.clone());
        }
    }
    let segment_io = capture
        .iter()
        .find_map(|effect| match effect {
            Effect::BlobWrite { io, .. } => Some(*io),
            _ => None,
        })
        .expect("dirty capture writes a segment");
    let record_writes = daemon.step(Event::BlobWriteDone { io: segment_io }, &mem);
    for effect in &record_writes {
        if let Effect::BlobWrite { name, bytes, .. } = effect {
            mem.1.borrow_mut().insert(name.clone(), bytes.clone());
        }
    }
    let record_ios: Vec<_> = record_writes
        .iter()
        .filter_map(|effect| match effect {
            Effect::BlobWrite { io, .. } => Some(*io),
            _ => None,
        })
        .collect();
    assert_eq!(record_ios.len(), 2, "primary and mirrored record writes");

    let started = database_step(
        &mut daemon,
        &mem,
        Event::Admin(AdminCmd::BeginDetachDatabase {
            req: ReqId(143),
            vset: VSET,
            attachment,
            mode: DetachMode::Graceful,
        }),
    );
    assert!(started.iter().any(|effect| matches!(
        effect,
        Effect::Admin(AdminReply::DatabaseDetachStarted {
            req: ReqId(143),
            ..
        })
    )));

    for io in record_ios {
        let _ = database_step(&mut daemon, &mem, Event::BlobWriteDone { io });
    }
    assert_eq!(
        database_step(
            &mut daemon,
            &mem,
            Event::Admin(AdminCmd::FinishDetachDatabase {
                req: ReqId(144),
                vset: VSET,
                attachment,
            }),
        ),
        [Effect::Admin(AdminReply::DatabaseDetached {
            req: ReqId(144),
            vset: VSET,
            attachment,
        })]
    );
}

#[test]
fn five_hundred_idle_database_vsets_share_one_vm_without_page_residency() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        archive: ArchivePolicy::default(),
        host: crate::types::HostId(0),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    });
    let mem = DatabaseMem::default();
    for id in 1..=500 {
        let vset = VsetId(10_000 + id);
        assert!(
            database_step(
                &mut daemon,
                &mem,
                Event::Admin(AdminCmd::CreateVset {
                    req: ReqId(id),
                    vset,
                    config: VsetConfig::database(8),
                    from_base: None,
                }),
            )
            .iter()
            .any(|effect| matches!(
                effect,
                Effect::Admin(AdminReply::VsetCreated { vset: created, .. }) if *created == vset
            ))
        );
        assert!(matches!(
            database_step(
                &mut daemon,
                &mem,
                Event::Admin(AdminCmd::AttachDatabase {
                    req: ReqId(1_000 + id),
                    vset,
                    vm: VmId(42),
                }),
            )
            .as_slice(),
            [Effect::Admin(AdminReply::DatabaseAttached { vset: attached, .. })]
                if *attached == vset
        ));
    }
    assert!(mem.0.borrow().is_empty());
}

#[allow(clippy::too_many_lines)]
fn database_cluster_step(
    daemons: &mut [Daemon; 2],
    mems: &[DatabaseMem; 2],
    blobs: &mut [BTreeMap<String, Vec<u8>>; 2],
    store: &mut DatabaseTestStore,
    initial: (usize, Event),
) -> Vec<(usize, Effect)> {
    let mut queue = VecDeque::from([initial]);
    let mut settled = Vec::new();
    let mut steps = 0;
    let mut hydration_ticks = BTreeSet::new();
    while let Some((host, event)) = queue.pop_front() {
        steps += 1;
        assert!(steps < 10_000, "database cluster did not settle");
        for effect in daemons[host].step(event, &mems[host]) {
            match effect {
                Effect::DatabaseInstall { page, bytes } => {
                    mems[host].0.borrow_mut().insert(page, bytes);
                }
                Effect::Evict { page } => {
                    mems[host].0.borrow_mut().remove(&page);
                }
                Effect::BlobWrite { io, name, bytes } => {
                    blobs[host].insert(name, bytes);
                    queue.push_back((host, Event::BlobWriteDone { io }));
                }
                Effect::BlobRead { io, name } => {
                    let bytes = blobs[host].get(&name).cloned();
                    queue.push_back((host, Event::BlobReadDone { io, bytes }));
                }
                Effect::BlobReadRange {
                    io,
                    name,
                    offset,
                    len,
                } => {
                    let bytes = blobs[host].get(&name).and_then(|blob| {
                        let start = usize::try_from(offset).ok()?;
                        let amount = usize::try_from(len).ok()?;
                        let end = start.checked_add(amount)?.min(blob.len());
                        (start <= blob.len()).then(|| blob[start..end].to_vec())
                    });
                    queue.push_back((host, Event::BlobReadDone { io, bytes }));
                }
                Effect::BlobDelete { name } => {
                    blobs[host].remove(&name);
                }
                Effect::ReplicaAppend { io, .. }
                | Effect::ReplicaDelete { io, .. }
                | Effect::ReplicaTruncate { io, .. } => {
                    queue.push_back((host, Event::BlobWriteDone { io }));
                }
                Effect::PeerSend { to, msg } => queue.push_back((
                    usize::from(to.0),
                    Event::PeerDelivered {
                        from: HostId(u16::try_from(host).expect("two hosts")),
                        msg,
                    },
                )),
                Effect::SetTimer { timer, .. }
                    if matches!(
                        timer,
                        TimerId::DatabaseMigrate(_)
                            | TimerId::CaptureStep(_)
                            | TimerId::DatabaseStep(_)
                    ) =>
                {
                    queue.push_back((host, Event::Timer(timer)));
                }
                Effect::SetTimer {
                    timer: TimerId::Hydrate(vset),
                    ..
                } if hydration_ticks.insert((host, vset)) => {
                    queue.push_back((host, Event::Timer(TimerId::Hydrate(vset))));
                }
                Effect::SetTimer { .. } => {}
                Effect::Fill { page, .. } => {
                    panic!("database hydration used the compute fill path for {page:?}")
                }
                Effect::StorePut { io, key, bytes } => {
                    let version = store.put(key, bytes);
                    queue.push_back((
                        host,
                        Event::StorePutDone {
                            io,
                            result: Ok(version),
                        },
                    ));
                }
                Effect::StoreCas {
                    io,
                    key,
                    expected,
                    bytes,
                } => {
                    let result = store.cas(key, expected, bytes);
                    queue.push_back((host, Event::StorePutDone { io, result }));
                }
                Effect::StoreGet { io, key } => {
                    let result = Ok(store.objects.get(&key).cloned());
                    queue.push_back((host, Event::StoreGetDone { io, result }));
                }
                Effect::StoreGetRange {
                    io,
                    key,
                    offset,
                    len,
                } => {
                    let result = Ok(store.objects.get(&key).and_then(|(version, bytes)| {
                        let start = usize::try_from(offset).ok()?;
                        let amount = usize::try_from(len).ok()?;
                        let end = start.checked_add(amount)?.min(bytes.len());
                        (start <= bytes.len()).then(|| (*version, bytes[start..end].to_vec()))
                    }));
                    queue.push_back((host, Event::StoreGetDone { io, result }));
                }
                Effect::StoreDelete { key } => {
                    store.objects.remove(&key);
                }
                Effect::Abort { reason } => panic!("daemon aborted: {reason}"),
                other => settled.push((host, other)),
            }
        }
    }
    settled
}

#[derive(Default)]
struct DatabaseTestStore {
    objects: BTreeMap<String, (u64, Vec<u8>)>,
    next_version: u64,
}

impl DatabaseTestStore {
    fn put(&mut self, key: String, bytes: Vec<u8>) -> u64 {
        self.next_version += 1;
        let version = self.next_version;
        self.objects.insert(key, (version, bytes));
        version
    }

    fn cas(
        &mut self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, crate::seam::StoreFault> {
        let actual = self.objects.get(&key).map(|(version, _)| *version);
        if actual != expected {
            return Err(crate::seam::StoreFault::CasConflict { actual });
        }
        Ok(self.put(key, bytes))
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn detached_database_migrates_without_vm_pause_and_reads_tail_from_peer() {
    let config = |host| DaemonConfig {
        archive: ArchivePolicy::default(),
        host: HostId(host),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: Some(test_replica_placement()),
    };
    let (source, _) = Daemon::new(config(0));
    let (destination, _) = Daemon::new(config(1));
    let mut daemons = [source, destination];
    let mems = [DatabaseMem::default(), DatabaseMem::default()];
    let mut blobs = [BTreeMap::new(), BTreeMap::new()];
    let mut store = DatabaseTestStore::default();
    let mut run =
        |daemons: &mut [Daemon; 2], blobs: &mut [BTreeMap<String, Vec<u8>>; 2], initial| {
            database_cluster_step(daemons, &mems, blobs, &mut store, initial)
        };

    let _ = run(
        &mut daemons,
        &mut blobs,
        (
            0,
            Event::Admin(AdminCmd::CreateVset {
                req: ReqId(200),
                vset: VSET,
                config: VsetConfig::database(8),
                from_base: None,
            }),
        ),
    );
    let attached = run(
        &mut daemons,
        &mut blobs,
        (
            0,
            Event::Admin(AdminCmd::AttachDatabase {
                req: ReqId(201),
                vset: VSET,
                vm: VmId(7),
            }),
        ),
    );
    let attachment = attached
        .iter()
        .find_map(|(_, effect)| match effect {
            Effect::Admin(AdminReply::DatabaseAttached { attachment, .. }) => Some(*attachment),
            _ => None,
        })
        .expect("source attachment");
    let request = |req, op| {
        Event::Database(DatabaseRequest {
            req: ReqId(req),
            vset: VSET,
            attachment,
            op,
        })
    };
    let _ = run(
        &mut daemons,
        &mut blobs,
        (
            0,
            request(
                202,
                DatabaseOp::Open {
                    handle: 9,
                    file: DatabaseFile::Main,
                    create: true,
                },
            ),
        ),
    );
    let expected = b"peer-backed sqlite page bytes".to_vec();
    let _ = run(
        &mut daemons,
        &mut blobs,
        (
            0,
            request(
                203,
                DatabaseOp::Write {
                    handle: 9,
                    offset: 211,
                    bytes: expected.clone(),
                },
            ),
        ),
    );
    let _ = run(
        &mut daemons,
        &mut blobs,
        (0, request(204, DatabaseOp::Sync { handle: 9 })),
    );
    let _ = run(
        &mut daemons,
        &mut blobs,
        (0, request(205, DatabaseOp::Close { handle: 9 })),
    );
    let _ = run(
        &mut daemons,
        &mut blobs,
        (
            0,
            Event::Admin(AdminCmd::BeginDetachDatabase {
                req: ReqId(206),
                vset: VSET,
                attachment,
                mode: DetachMode::Graceful,
            }),
        ),
    );
    let _ = run(
        &mut daemons,
        &mut blobs,
        (
            0,
            Event::Admin(AdminCmd::FinishDetachDatabase {
                req: ReqId(207),
                vset: VSET,
                attachment,
            }),
        ),
    );

    let source_state = &daemons[0].vsets[&VSET];
    assert!(
        source_state.ready
            && source_state.outbound.is_none()
            && source_state.migrate.is_none()
            && source_state.peer_source.is_none()
            && source_state.ckpt_pausing.is_none()
            && !source_state.commit_running
            && source_state.database_runtime.is_detached(),
        "source not migration-ready: {source_state:?}"
    );

    let moved = run(
        &mut daemons,
        &mut blobs,
        (
            0,
            Event::Admin(AdminCmd::MigrateOut {
                req: ReqId(208),
                vset: VSET,
                to: HostId(1),
            }),
        ),
    );
    assert!(
        !moved
            .iter()
            .any(|(_, effect)| matches!(effect, Effect::PauseGuest { .. })),
        "database movement must not pause a VM"
    );
    assert!(
        moved.iter().any(|(host, effect)| matches!(
            (host, effect),
            (
                1,
                Effect::Admin(AdminReply::VsetMigratedIn {
                    verdict: Verdict::DatabaseReady { .. },
                    ..
                })
            )
        )),
        "migration effects: {moved:?}"
    );
    let hydrated = DatabaseFile::Main.page(VSET, 0);
    assert_eq!(
        &mems[1].0.borrow()[&hydrated][211..211 + expected.len()],
        expected.as_slice()
    );
    assert_eq!(daemons[1].counters.hydrate_fills, 1);

    let destination_attachment = run(
        &mut daemons,
        &mut blobs,
        (
            1,
            Event::Admin(AdminCmd::AttachDatabase {
                req: ReqId(209),
                vset: VSET,
                vm: VmId(8),
            }),
        ),
    )
    .iter()
    .find_map(|(_, effect)| match effect {
        Effect::Admin(AdminReply::DatabaseAttached { attachment, .. }) => Some(*attachment),
        _ => None,
    })
    .expect("destination attachment");
    let destination_request = |req, op| {
        Event::Database(DatabaseRequest {
            req: ReqId(req),
            vset: VSET,
            attachment: destination_attachment,
            op,
        })
    };
    let _ = run(
        &mut daemons,
        &mut blobs,
        (
            1,
            destination_request(
                210,
                DatabaseOp::Open {
                    handle: 10,
                    file: DatabaseFile::Main,
                    create: false,
                },
            ),
        ),
    );
    let read = run(
        &mut daemons,
        &mut blobs,
        (
            1,
            destination_request(
                211,
                DatabaseOp::Read {
                    handle: 10,
                    offset: 211,
                    len: u32::try_from(expected.len()).expect("small"),
                },
            ),
        ),
    );
    assert!(read.iter().any(|(_, effect)| matches!(
        effect,
        Effect::Database(DatabaseReply::Read { req: ReqId(211), bytes, .. }) if *bytes == expected
    )));
    assert!(store.objects.contains_key(&layout::head_key(VSET)));
    assert!(
        store
            .objects
            .keys()
            .any(|key| key.starts_with("v/0000000000000007/m/"))
    );
}
