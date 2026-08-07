use super::*;
use crate::head::HeadRecord;
use crate::journal::{DurabilityMode, VsetConfig};
use crate::layout;
use crate::placement::{PeerCandidate, rank_stash_candidates};
use crate::seam::{
    AdminCmd, AdminReply, Effect, Event, HostMap, PeerMsg, ReplicaArtifact, ReplicaCommitInfo,
    ReqId,
};
use crate::segment::SegmentBuilder;
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
        durability: DurabilityMode::Local,
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
        wedge_ticks: 25,
        replica_placement: None,
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
    assert_eq!(vset.backup_lag_bytes, None);
    assert_eq!(daemon.counters.guest_pages_dirtied, 1);
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
fn peer_stashed_sync_never_acks_from_local_record_durability() {
    let mut daemon = created_daemon();
    daemon
        .vsets
        .get_mut(&VSET)
        .expect("created vset")
        .config
        .durability = DurabilityMode::PeerStashed;
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
    state.config.durability = DurabilityMode::PeerStashed;
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

    let effects = daemon.step(
        Event::PeerDelivered {
            from: HostId(1),
            msg: PeerMsg::ReplicaUploadDone {
                vset: VSET,
                assignment_epoch: 1,
                info,
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
        config: VsetConfig {
            durability: DurabilityMode::PeerStashed,
            ..config()
        },
        seq: JournalSeq(seq),
        fence: 1,
        kind: crate::journal::RecordKind::Commit,
        capture_seq,
        sync_covered_through: capture_seq,
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
        state.config.durability = DurabilityMode::PeerStashed;
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
        state.peer_committed_record = Some(newer);
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
        config: VsetConfig {
            durability: DurabilityMode::PeerStashed,
            ..config()
        },
        seq: JournalSeq(2),
        fence: 1,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 2,
        sync_covered_through: 2,
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let info = Daemon::commit_info(&record);
    {
        let state = daemon.vsets.get_mut(&VSET).expect("created vset");
        state.config.durability = DurabilityMode::PeerStashed;
        state.head_version = Some(1);
        state.stash_assignment = Some(current);
        state.peer_upload_done = Some((current.assignment_epoch, info));
        state.peer_committed_record = Some(record);
        state.replica_assignment_proposal = Some((transition, None));
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
        host: HostId(2),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: None,
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
            config: VsetConfig {
                durability: DurabilityMode::PeerStashed,
                ..config()
            },
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
fn backup_creation_never_publishes_a_peer_stash() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
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
            roster: vec![PeerCandidate {
                host: HostId(1),
                weight: 1,
                failure_domain: 2,
                drained: false,
            }],
        }),
    });
    let effects = daemon.step(
        Event::Admin(AdminCmd::CreateVset {
            req: ReqId(21),
            vset: VSET,
            config: VsetConfig {
                durability: DurabilityMode::Backup,
                ..config()
            },
            from_base: None,
        }),
        &NoMem,
    );
    let [Effect::StoreCas { bytes, .. }] = effects.as_slice() else {
        panic!("backup creation must first publish one fenced head: {effects:?}");
    };
    assert_eq!(HeadRecord::decode(VSET, bytes).expect("head").stash, None);
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
        config: VsetConfig {
            durability: DurabilityMode::PeerStashed,
            ..config()
        },
        seq: JournalSeq(4),
        fence: 1,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 10,
        sync_covered_through: 10,
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let state = daemon.vsets.get_mut(&VSET).expect("created");
    state.config.durability = DurabilityMode::PeerStashed;
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
        config: VsetConfig {
            durability: DurabilityMode::PeerStashed,
            ..config()
        },
        seq: JournalSeq(3),
        fence: 2,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 4,
        sync_covered_through: 5,
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let state = daemon.vsets.get_mut(&VSET).expect("vset");
    state.config.durability = DurabilityMode::PeerStashed;
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
    daemon.vsets.get_mut(&VSET).expect("vset").config.durability = DurabilityMode::PeerStashed;
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
        config: VsetConfig {
            durability: DurabilityMode::PeerStashed,
            ..config()
        },
        seq: info.seq,
        fence: info.writer_fence,
        kind: crate::journal::RecordKind::Commit,
        capture_seq: 12,
        sync_covered_through: info.sync_covered_through,
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
                record,
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
    let artifact_put = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StorePut { io, key, bytes } => Some((*io, key.clone(), bytes.clone())),
            _ => None,
        })
        .expect("peer uploads the durable artifact directly");
    assert_eq!(artifact_put.1, layout::segment_key(VSET, 4, SegId(3)));
    let effects = daemon.step(
        Event::StorePutDone {
            io: artifact_put.0,
            result: Ok(1),
        },
        &NoMem,
    );
    let manifest_io = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StorePut { io, key, .. }
                if *key == layout::manifest_key(VSET, 4, JournalSeq(8)) =>
            {
                Some(*io)
            }
            _ => None,
        })
        .expect("artifact completion uploads the manifest");
    assert_eq!(
        daemon.step(
            Event::StorePutDone {
                io: manifest_io,
                result: Ok(1),
            },
            &NoMem,
        ),
        [Effect::PeerSend {
            to: HostId(0),
            msg: PeerMsg::ReplicaUploadDone {
                vset: VSET,
                assignment_epoch: 1,
                info,
            },
        }]
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

    daemon.replicas.insert(
        ReplicaKey {
            source: HostId(0),
            vset: VSET,
            assignment_epoch: 1,
        },
        PassiveReplica {
            stored_bytes: super::replica::MAX_REPLICA_SOURCE_BYTES,
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

#[test]
fn passive_upload_backlog_keeps_only_the_latest_complete_commit() {
    let (mut daemon, _) = Daemon::new(DaemonConfig {
        host: HostId(1),
        cache_pages: 8,
        writeback_interval: 1_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: None,
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
        host: crate::types::HostId(0),
        cache_pages: 256,
        writeback_interval: 1_000_000_000,
        backup_retry: 200_000_000,
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 25,
        replica_placement: None,
    });
    let config = VsetConfig {
        disk_volumes: 1,
        pages_per_volume: 64,
        durability: DurabilityMode::Local,
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
