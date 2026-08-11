//! The head record: one small object per archived vset at
//! `layout::head_key`, and the system's assignment authority (R6.3). Every
//! update goes through the store's compare-and-swap; the store version that
//! a successful claim returns *is* the claimant's fence — the namespace all
//! of its segments and manifests live under. Two hosts racing to restore
//! resolve to exactly one runner by CAS alone, and a fenced former holder's
//! CAS failures make it structurally unable to publish (R6.4).
//!
//! The head is the one non-backup use of the object store (R4.2): small,
//! rare, never on a guest-visible path.

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::protocol::ReplicaCommitInfo;
use crate::types::{HostId, JournalSeq, VsetId};

pub const MAGIC_HEAD: u32 = u32::from_le_bytes(*b"BHD1");

/// Pointer to the newest backed-up manifest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ManifestPtr {
    pub fence: u64,
    pub seq: JournalSeq,
    /// The manifest's capture instant (restore planning, lag observability).
    pub capture_seq: u64,
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
/// failover budget. The decoder accepts the wider historical bound for rolling
/// compatibility.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RetiredStash {
    pub peer: HostId,
    pub assignment_epoch: u64,
    pub through: ReplicaCommitInfo,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct HeadRecord {
    pub vset: VsetId,
    /// The host currently assigned to run this vset.
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
        let version = if self.retired_stashes.is_empty()
            && self
                .stash
                .is_none_or(|stash| stash.active_assignment_epoch == stash.assignment_epoch)
        {
            2
        } else {
            3
        };
        assert!(self.retired_stashes.len() <= MAX_RETIRED_STASHES);
        e.u16(version);
        e.u64(self.vset.0);
        e.u16(self.holder.0);
        e.u64(self.fence);
        match &self.manifest {
            None => e.u8(0),
            Some(ptr) => {
                e.u8(1);
                e.u64(ptr.fence);
                e.u64(ptr.seq.0);
                e.u64(ptr.capture_seq);
            }
        }
        match self.stash {
            None => e.u8(0),
            Some(stash) => {
                e.u8(1);
                e.u64(stash.assignment_epoch);
                e.u16(stash.active_peer.0);
                if version >= 3 {
                    e.u64(stash.active_assignment_epoch);
                }
                match stash.transition_peer {
                    None => {
                        e.u8(0);
                        e.u16(0);
                    }
                    Some(peer) => {
                        e.u8(1);
                        e.u16(peer.0);
                    }
                }
                e.u64(stash.membership_epoch);
            }
        }
        if version >= 3 {
            e.u8(u8::try_from(self.retired_stashes.len()).expect("bounded history"));
            for retired in &self.retired_stashes {
                e.u16(retired.peer.0);
                e.u64(retired.assignment_epoch);
                e.u64(retired.through.writer_fence);
                e.u64(retired.through.seq.0);
                e.u64(retired.through.sync_covered_through);
            }
        }
        seal_frame(MAGIC_HEAD, &e.finish())
    }

    /// Verify and decode a head record (R8.1 applies to the store too).
    pub fn decode(vset: VsetId, bytes: &[u8]) -> Result<HeadRecord, DecodeError> {
        let payload = open_frame(MAGIC_HEAD, bytes)?;
        let mut d = Dec::new(payload);
        let version = d.u16()?;
        if !matches!(version, 1..=3) {
            return Err(DecodeError);
        }
        if d.u64()? != vset.0 {
            return Err(DecodeError);
        }
        let holder = HostId(d.u16()?);
        let fence = d.u64()?;
        let manifest = match d.u8()? {
            0 => None,
            1 => Some(ManifestPtr {
                fence: d.u64()?,
                seq: JournalSeq(d.u64()?),
                capture_seq: d.u64()?,
            }),
            _ => return Err(DecodeError),
        };
        let stash = if version == 1 {
            None
        } else {
            match d.u8()? {
                0 => None,
                1 => {
                    let assignment_epoch = d.u64()?;
                    let active_peer = HostId(d.u16()?);
                    let active_assignment_epoch = if version >= 3 {
                        d.u64()?
                    } else {
                        assignment_epoch
                    };
                    let transition_peer = match (d.u8()?, d.u16()?) {
                        (0, 0) => None,
                        (1, peer) => Some(HostId(peer)),
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
            }
        };
        let retired_stashes = if version >= 3 {
            let count = usize::from(d.u8()?);
            if count > MAX_RETIRED_STASHES {
                return Err(DecodeError);
            }
            let mut retired = Vec::with_capacity(count);
            for _ in 0..count {
                retired.push(RetiredStash {
                    peer: HostId(d.u16()?),
                    assignment_epoch: d.u64()?,
                    through: ReplicaCommitInfo {
                        writer_fence: d.u64()?,
                        seq: JournalSeq(d.u64()?),
                        sync_covered_through: d.u64()?,
                    },
                });
            }
            retired
        } else {
            Vec::new()
        };
        d.finish()?;
        Ok(HeadRecord {
            vset,
            holder,
            fence,
            manifest,
            stash,
            retired_stashes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;

    fn sample() -> HeadRecord {
        HeadRecord {
            vset: VsetId(0xA1),
            holder: HostId(3),
            fence: 4,
            manifest: Some(ManifestPtr {
                fence: 2,
                seq: JournalSeq(17),
                capture_seq: 99,
            }),
            stash: Some(StashAssignment {
                assignment_epoch: 8,
                active_peer: HostId(5),
                active_assignment_epoch: 8,
                transition_peer: Some(HostId(7)),
                membership_epoch: 3,
            }),
            retired_stashes: Vec::new(),
        }
    }

    #[test]
    fn heads_round_trip_and_are_byte_pinned() {
        let bytes = sample().encode();
        assert_eq!(HeadRecord::decode(VsetId(0xA1), &bytes), Ok(sample()));
        // Byte pin (R10.2): any change here is a storage format change.
        assert_eq!(bytes.len(), 79);
        assert_eq!(crc32c(&bytes), 0xD362_8BF9);
    }

    #[test]
    fn heads_reject_any_single_bit_flip() {
        let bytes = sample().encode();
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                HeadRecord::decode(VsetId(0xA1), &damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn heads_without_manifest_round_trip() {
        let head = HeadRecord {
            vset: VsetId(0xA1),
            holder: HostId(0),
            fence: 1,
            manifest: None,
            stash: None,
            retired_stashes: Vec::new(),
        };
        let bytes = head.encode();
        assert_eq!(HeadRecord::decode(VsetId(0xA1), &bytes), Ok(head));
    }

    #[test]
    fn version_one_heads_decode_without_a_stash_assignment() {
        let mut e = Enc::new();
        e.u16(1);
        e.u64(0xA1);
        e.u16(3);
        e.u64(4);
        e.u8(0);
        let bytes = seal_frame(MAGIC_HEAD, &e.finish());
        assert_eq!(
            HeadRecord::decode(VsetId(0xA1), &bytes),
            Ok(HeadRecord {
                vset: VsetId(0xA1),
                holder: HostId(3),
                fence: 4,
                manifest: None,
                stash: None,
                retired_stashes: Vec::new(),
            })
        );
    }

    #[test]
    fn version_three_preserves_transition_epoch_and_retired_history() {
        let mut head = sample();
        head.stash.as_mut().expect("stash").active_assignment_epoch = 7;
        head.retired_stashes.push(RetiredStash {
            peer: HostId(2),
            assignment_epoch: 6,
            through: ReplicaCommitInfo {
                writer_fence: 4,
                seq: JournalSeq(11),
                sync_covered_through: 15,
            },
        });
        assert_eq!(HeadRecord::decode(head.vset, &head.encode()), Ok(head));
    }
}
