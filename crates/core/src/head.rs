//! The head record: one small object per archived volume at
//! `layout::head_key`, and the system's assignment authority (R6.3). Every
//! update goes through the store's compare-and-swap; the store version that
//! a successful claim returns *is* the claimant's fence — the namespace all
//! of its BLX files and manifests live under. Two hosts racing to restore
//! resolve to exactly one runner by CAS alone, and a fenced former holder's
//! CAS failures make it structurally unable to publish (R6.4).
//!
//! The head is the one non-backup use of the object store (R6.3): small,
//! rare, never on a guest-visible path.

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::protocol::ReplicaCommitInfo;
use crate::types::{HostId, JournalSeq, VolumeId};

pub const MAGIC_HEAD: u32 = u32::from_le_bytes(*b"BHD1");

/// Pointer to the newest backed-up manifest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ManifestPtr {
    pub fence: u64,
    /// The exact journal record represented by the manifest.
    pub journal_seq: JournalSeq,
    /// The independently advancing archive-publication sequence.
    pub seq: JournalSeq,
    /// The manifest's capture instant (restore planning, lag observability).
    pub capture_seq: u64,
    /// Binds the pointer to the exact encoded manifest bytes.
    pub checksum: u64,
}

impl ManifestPtr {
    pub fn manifest_key(self, volume: VolumeId) -> String {
        crate::layout::manifest_key(volume, self.fence, self.seq)
    }

    pub fn pending_key(self, volume: VolumeId) -> String {
        crate::layout::pending_manifest_key(volume, self.fence, self.seq)
    }
}

/// Durable passive-stash placement. Health is deliberately absent: this is
/// assignment authority, not a failure detector (R6.6).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StashAssignment {
    /// Epoch used by new writes (the transition peer during seeding).
    pub assignment_epoch: u64,
    pub active_peer: HostId,
    /// The active peer's actual spool epoch. During a transition this remains
    /// the prior epoch while `assignment_epoch` names the replacement.
    pub active_assignment_epoch: u64,
    pub transition_peer: Option<HostId>,
    pub membership_epoch: u64,
}

pub const MAX_RETIRED_STASHES: usize = 8;

/// The immediately former active peer. It is a redundant recovery source and
/// cleanup target after a covering replacement commit; an older entry is
/// superseded on the next activation so dead peers cannot form a finite
/// failover budget.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RetiredStash {
    pub peer: HostId,
    pub assignment_epoch: u64,
    pub through: ReplicaCommitInfo,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct HeadRecord {
    pub volume: VolumeId,
    /// The host currently assigned to run this volume.
    pub holder: HostId,
    /// The holder's fence (the head version its claim returned).
    pub fence: u64,
    /// Newest backed-up recovery point; `None` until the first backup lands.
    pub manifest: Option<ManifestPtr>,
    /// One active passive stash and at most one replacement being seeded.
    pub stash: Option<StashAssignment>,
    /// Bounded assignment history still relevant to recovery/release.
    pub retired_stashes: Vec<RetiredStash>,
}

impl HeadRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        let version = 4;
        assert!(self.retired_stashes.len() <= MAX_RETIRED_STASHES);
        e.u16(version);
        e.u64(self.volume.0);
        encode_identity(&mut e, self.holder);
        e.u64(self.fence);
        match &self.manifest {
            None => e.u8(0),
            Some(ptr) => {
                e.u8(1);
                e.u64(ptr.fence);
                e.u64(ptr.journal_seq.0);
                e.u64(ptr.seq.0);
                e.u64(ptr.capture_seq);
                e.u64(ptr.checksum);
            }
        }
        match self.stash {
            None => e.u8(0),
            Some(stash) => {
                e.u8(1);
                e.u64(stash.assignment_epoch);
                encode_identity(&mut e, stash.active_peer);
                e.u64(stash.active_assignment_epoch);
                match stash.transition_peer {
                    None => e.u8(0),
                    Some(peer) => {
                        e.u8(1);
                        encode_identity(&mut e, peer);
                    }
                }
                e.u64(stash.membership_epoch);
            }
        }
        e.u8(u8::try_from(self.retired_stashes.len()).expect("bounded history"));
        for retired in &self.retired_stashes {
            encode_identity(&mut e, retired.peer);
            e.u64(retired.assignment_epoch);
            e.u64(retired.through.writer_fence);
            e.u64(retired.through.seq.0);
            e.u64(retired.through.sync_covered_through);
        }
        seal_frame(MAGIC_HEAD, &e.finish())
    }

    /// Verify and decode a head record (R8.1 applies to the store too).
    pub fn decode(volume: VolumeId, bytes: &[u8]) -> Result<HeadRecord, DecodeError> {
        let payload = open_frame(MAGIC_HEAD, bytes)?;
        let mut d = Dec::new(payload);
        let version = d.u16()?;
        if version != 4 {
            return Err(DecodeError);
        }
        if d.u64()? != volume.0 {
            return Err(DecodeError);
        }
        let holder = decode_identity(&mut d)?;
        let fence = d.u64()?;
        let manifest = match d.u8()? {
            0 => None,
            1 => Some(ManifestPtr {
                fence: d.u64()?,
                journal_seq: JournalSeq(d.u64()?),
                seq: JournalSeq(d.u64()?),
                capture_seq: d.u64()?,
                checksum: d.u64()?,
            }),
            _ => return Err(DecodeError),
        };
        let stash = match d.u8()? {
            0 => None,
            1 => {
                let assignment_epoch = d.u64()?;
                let active_peer = decode_identity(&mut d)?;
                let active_assignment_epoch = d.u64()?;
                let transition_peer = match d.u8()? {
                    0 => None,
                    1 => Some(decode_identity(&mut d)?),
                    _ => return Err(DecodeError),
                };
                let membership_epoch = d.u64()?;
                if transition_peer == Some(active_peer) {
                    return Err(DecodeError);
                }
                if transition_peer.is_none() && active_assignment_epoch != assignment_epoch {
                    return Err(DecodeError);
                }
                Some(StashAssignment {
                    assignment_epoch,
                    active_peer,
                    active_assignment_epoch,
                    transition_peer,
                    membership_epoch,
                })
            }
            _ => return Err(DecodeError),
        };
        let count = usize::from(d.u8()?);
        if count > MAX_RETIRED_STASHES {
            return Err(DecodeError);
        }
        let mut retired_stashes = Vec::with_capacity(count);
        for _ in 0..count {
            retired_stashes.push(RetiredStash {
                peer: decode_identity(&mut d)?,
                assignment_epoch: d.u64()?,
                through: ReplicaCommitInfo {
                    writer_fence: d.u64()?,
                    seq: JournalSeq(d.u64()?),
                    sync_covered_through: d.u64()?,
                },
            });
        }
        d.finish()?;
        Ok(HeadRecord {
            volume,
            holder,
            fence,
            manifest,
            stash,
            retired_stashes,
        })
    }
}

fn encode_identity(e: &mut Enc, identity: HostId) {
    e.u32(identity.get());
}

fn decode_identity(d: &mut Dec<'_>) -> Result<HostId, DecodeError> {
    Ok(HostId::new(d.u32()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;

    const fn id(host: u32) -> HostId {
        HostId::new(host)
    }

    fn sample() -> HeadRecord {
        HeadRecord {
            volume: VolumeId(0xA1),
            holder: id(3),
            fence: 4,
            manifest: Some(ManifestPtr {
                fence: 2,
                journal_seq: JournalSeq(11),
                seq: JournalSeq(17),
                capture_seq: 99,
                checksum: 0x1234,
            }),
            stash: Some(StashAssignment {
                assignment_epoch: 8,
                active_peer: id(5),
                active_assignment_epoch: 8,
                transition_peer: Some(id(7)),
                membership_epoch: 3,
            }),
            retired_stashes: Vec::new(),
        }
    }

    #[test]
    fn heads_round_trip_and_are_byte_pinned() {
        let bytes = sample().encode();
        assert_eq!(HeadRecord::decode(VolumeId(0xA1), &bytes), Ok(sample()));
        // Byte pin (R10.2): any change here is a storage format change.
        assert_eq!(bytes.len(), 110);
        assert_eq!(crc32c(&bytes), 0x837F_138A);
    }

    #[test]
    fn heads_reject_any_single_bit_flip() {
        let bytes = sample().encode();
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                HeadRecord::decode(VolumeId(0xA1), &damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn heads_without_manifest_round_trip() {
        let head = HeadRecord {
            volume: VolumeId(0xA1),
            holder: id(0),
            fence: 1,
            manifest: None,
            stash: None,
            retired_stashes: Vec::new(),
        };
        let bytes = head.encode();
        assert_eq!(HeadRecord::decode(VolumeId(0xA1), &bytes), Ok(head));
    }

    #[test]
    fn old_head_versions_are_rejected() {
        let mut e = Enc::new();
        e.u16(1);
        e.u64(0xA1);
        e.u16(3);
        e.u64(4);
        e.u8(0);
        let bytes = seal_frame(MAGIC_HEAD, &e.finish());
        assert!(HeadRecord::decode(VolumeId(0xA1), &bytes).is_err());
    }

    #[test]
    fn version_four_preserves_transition_epoch_and_retired_history() {
        let mut head = sample();
        head.stash.as_mut().expect("stash").active_assignment_epoch = 7;
        head.retired_stashes.push(RetiredStash {
            peer: id(2),
            assignment_epoch: 6,
            through: ReplicaCommitInfo {
                writer_fence: 4,
                seq: JournalSeq(11),
                sync_covered_through: 15,
            },
        });
        assert_eq!(HeadRecord::decode(head.volume, &head.encode()), Ok(head));
    }
}
