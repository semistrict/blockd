//! Persistent placement, host-session, and vnode-authority records.
//!
//! These records are changed only with object-store conditional writes. A
//! caller presenting an [`AuthorityProof`] is not trusted: a replica must GET
//! the vnode authority object and compare both its store version and decoded
//! bytes before durably adopting the generation.

use crate::format::{Dec, DecodeError, Enc, open_frame, seal_frame};
use crate::types::{HostId, VolumeId};

pub const REPLICATION_FACTOR: usize = 3;
pub const MAGIC_PLACEMENT: u32 = u32::from_le_bytes(*b"BPL1");
pub const MAGIC_HOST_SESSION: u32 = u32::from_le_bytes(*b"BHS1");
pub const MAGIC_VNODE_AUTHORITY: u32 = u32::from_le_bytes(*b"BVA1");
const FORMAT_VERSION: u16 = 1;
const MAX_VNODES: u32 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct VnodeId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VnodePlacement {
    pub vnode: VnodeId,
    pub members: [HostId; REPLICATION_FACTOR],
    pub next_members: Option<[HostId; REPLICATION_FACTOR]>,
}

impl VnodePlacement {
    pub fn voting_sets(&self) -> impl Iterator<Item = [HostId; REPLICATION_FACTOR]> {
        [Some(self.members), self.next_members]
            .into_iter()
            .flatten()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementRecord {
    pub cluster_id: u64,
    pub epoch: u64,
    pub vnode_count: u32,
    pub vnodes: Vec<VnodePlacement>,
}

impl PlacementRecord {
    pub fn new(
        cluster_id: u64,
        epoch: u64,
        vnodes: Vec<VnodePlacement>,
    ) -> Result<Self, DecodeError> {
        let vnode_count = u32::try_from(vnodes.len()).map_err(|_| DecodeError)?;
        let record = Self {
            cluster_id,
            epoch,
            vnode_count,
            vnodes,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), DecodeError> {
        if self.cluster_id == 0
            || self.epoch == 0
            || self.vnode_count == 0
            || self.vnode_count > MAX_VNODES
            || usize::try_from(self.vnode_count).map_err(|_| DecodeError)? != self.vnodes.len()
        {
            return Err(DecodeError);
        }
        for (index, placement) in self.vnodes.iter().enumerate() {
            if usize::try_from(placement.vnode.0).map_err(|_| DecodeError)? != index
                || !distinct_members(placement.members)
                || placement.next_members.is_some_and(|members| {
                    !distinct_members(members) || !quorums_overlap(placement.members, members)
                })
            {
                return Err(DecodeError);
            }
        }
        Ok(())
    }

    pub fn placement(&self, vnode: VnodeId) -> Option<&VnodePlacement> {
        self.vnodes.get(usize::try_from(vnode.0).ok()?)
    }

    pub fn vnode(&self, volume: VolumeId) -> VnodeId {
        vnode_for(volume, self.vnode_count)
    }

    pub fn encode(&self) -> Vec<u8> {
        self.validate().expect("valid placement record");
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u64(self.cluster_id);
        e.u64(self.epoch);
        e.u32(self.vnode_count);
        for placement in &self.vnodes {
            e.u32(placement.vnode.0);
            encode_members(&mut e, placement.members);
            match placement.next_members {
                None => e.u8(0),
                Some(members) => {
                    e.u8(1);
                    encode_members(&mut e, members);
                }
            }
        }
        seal_frame(MAGIC_PLACEMENT, &e.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_PLACEMENT, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION {
            return Err(DecodeError);
        }
        let cluster_id = d.u64()?;
        let epoch = d.u64()?;
        let vnode_count = d.u32()?;
        if vnode_count == 0 || vnode_count > MAX_VNODES {
            return Err(DecodeError);
        }
        let mut vnodes = Vec::with_capacity(usize::try_from(vnode_count).map_err(|_| DecodeError)?);
        for _ in 0..vnode_count {
            let vnode = VnodeId(d.u32()?);
            let members = decode_members(&mut d)?;
            let next_members = match d.u8()? {
                0 => None,
                1 => Some(decode_members(&mut d)?),
                _ => return Err(DecodeError),
            };
            vnodes.push(VnodePlacement {
                vnode,
                members,
                next_members,
            });
        }
        d.finish()?;
        let record = Self {
            cluster_id,
            epoch,
            vnode_count,
            vnodes,
        };
        record.validate()?;
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostSessionRecord {
    Active {
        host: HostId,
        session: u64,
        epoch: u64,
    },
    Challenge {
        host: HostId,
        session: u64,
        epoch: u64,
        nonce: u64,
        challenger: HostId,
        challenged_at: u64,
    },
    Revoked {
        host: HostId,
        old_session: u64,
        epoch: u64,
        nonce: u64,
    },
}

impl HostSessionRecord {
    pub fn initial(host: HostId, session: u64) -> Result<Self, DecodeError> {
        if session == 0 {
            return Err(DecodeError);
        }
        Ok(Self::Active {
            host,
            session,
            epoch: 1,
        })
    }

    pub fn host(self) -> HostId {
        match self {
            Self::Active { host, .. }
            | Self::Challenge { host, .. }
            | Self::Revoked { host, .. } => host,
        }
    }

    pub fn epoch(self) -> u64 {
        match self {
            Self::Active { epoch, .. }
            | Self::Challenge { epoch, .. }
            | Self::Revoked { epoch, .. } => epoch,
        }
    }

    pub fn challenge(
        self,
        challenger: HostId,
        nonce: u64,
        challenged_at: u64,
    ) -> Result<Self, DecodeError> {
        let Self::Active {
            host,
            session,
            epoch,
        } = self
        else {
            return Err(DecodeError);
        };
        if nonce == 0 || challenger == host {
            return Err(DecodeError);
        }
        Ok(Self::Challenge {
            host,
            session,
            epoch,
            nonce,
            challenger,
            challenged_at,
        })
    }

    pub fn defend(self, session: u64, nonce: u64) -> Result<Self, DecodeError> {
        let Self::Challenge {
            host,
            session: challenged_session,
            epoch,
            nonce: challenged_nonce,
            ..
        } = self
        else {
            return Err(DecodeError);
        };
        if session != challenged_session || nonce != challenged_nonce {
            return Err(DecodeError);
        }
        Ok(Self::Active {
            host,
            session,
            epoch,
        })
    }

    pub fn revoke(self, nonce: u64) -> Result<Self, DecodeError> {
        let Self::Challenge {
            host,
            session,
            epoch,
            nonce: challenged_nonce,
            ..
        } = self
        else {
            return Err(DecodeError);
        };
        if nonce != challenged_nonce {
            return Err(DecodeError);
        }
        Ok(Self::Revoked {
            host,
            old_session: session,
            epoch: epoch.checked_add(1).ok_or(DecodeError)?,
            nonce,
        })
    }

    pub fn activate(self, session: u64) -> Result<Self, DecodeError> {
        let Self::Revoked { host, epoch, .. } = self else {
            return Err(DecodeError);
        };
        if session == 0 {
            return Err(DecodeError);
        }
        Ok(Self::Active {
            host,
            session,
            epoch,
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        match self {
            Self::Active {
                host,
                session,
                epoch,
            } => {
                e.u8(0);
                e.u16(host.0);
                e.u64(session);
                e.u64(epoch);
            }
            Self::Challenge {
                host,
                session,
                epoch,
                nonce,
                challenger,
                challenged_at,
            } => {
                e.u8(1);
                e.u16(host.0);
                e.u64(session);
                e.u64(epoch);
                e.u64(nonce);
                e.u16(challenger.0);
                e.u64(challenged_at);
            }
            Self::Revoked {
                host,
                old_session,
                epoch,
                nonce,
            } => {
                e.u8(2);
                e.u16(host.0);
                e.u64(old_session);
                e.u64(epoch);
                e.u64(nonce);
            }
        }
        seal_frame(MAGIC_HOST_SESSION, &e.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_HOST_SESSION, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION {
            return Err(DecodeError);
        }
        let record = match d.u8()? {
            0 => Self::Active {
                host: HostId(d.u16()?),
                session: d.u64()?,
                epoch: d.u64()?,
            },
            1 => Self::Challenge {
                host: HostId(d.u16()?),
                session: d.u64()?,
                epoch: d.u64()?,
                nonce: d.u64()?,
                challenger: HostId(d.u16()?),
                challenged_at: d.u64()?,
            },
            2 => Self::Revoked {
                host: HostId(d.u16()?),
                old_session: d.u64()?,
                epoch: d.u64()?,
                nonce: d.u64()?,
            },
            _ => return Err(DecodeError),
        };
        d.finish()?;
        if record.epoch() == 0
            || match record {
                Self::Active { session, .. } => session == 0,
                Self::Challenge {
                    host,
                    session,
                    nonce,
                    challenger,
                    ..
                } => session == 0 || nonce == 0 || challenger == host,
                Self::Revoked {
                    old_session, nonce, ..
                } => old_session == 0 || nonce == 0,
            }
        {
            return Err(DecodeError);
        }
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VnodeAuthority {
    pub cluster_id: u64,
    pub placement_epoch: u64,
    pub vnode: VnodeId,
    pub generation: u64,
    pub primary: HostId,
    pub primary_session: u64,
    pub primary_host_epoch: u64,
}

impl VnodeAuthority {
    pub fn validate(self, placement: &PlacementRecord) -> Result<(), DecodeError> {
        let members = placement
            .placement(self.vnode)
            .ok_or(DecodeError)?
            .voting_sets()
            .flatten()
            .collect::<Vec<_>>();
        if self.cluster_id != placement.cluster_id
            || self.placement_epoch != placement.epoch
            || self.generation == 0
            || self.primary_session == 0
            || self.primary_host_epoch == 0
            || !members.contains(&self.primary)
        {
            return Err(DecodeError);
        }
        Ok(())
    }

    pub fn advance(
        self,
        primary: HostId,
        primary_session: u64,
        primary_host_epoch: u64,
    ) -> Result<Self, DecodeError> {
        if primary_session == 0 || primary_host_epoch == 0 {
            return Err(DecodeError);
        }
        Ok(Self {
            generation: self.generation.checked_add(1).ok_or(DecodeError)?,
            primary,
            primary_session,
            primary_host_epoch,
            ..self
        })
    }

    pub fn encode(self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u64(self.cluster_id);
        e.u64(self.placement_epoch);
        e.u32(self.vnode.0);
        e.u64(self.generation);
        e.u16(self.primary.0);
        e.u64(self.primary_session);
        e.u64(self.primary_host_epoch);
        seal_frame(MAGIC_VNODE_AUTHORITY, &e.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_VNODE_AUTHORITY, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION {
            return Err(DecodeError);
        }
        let record = Self {
            cluster_id: d.u64()?,
            placement_epoch: d.u64()?,
            vnode: VnodeId(d.u32()?),
            generation: d.u64()?,
            primary: HostId(d.u16()?),
            primary_session: d.u64()?,
            primary_host_epoch: d.u64()?,
        };
        d.finish()?;
        if record.cluster_id == 0
            || record.placement_epoch == 0
            || record.generation == 0
            || record.primary_session == 0
            || record.primary_host_epoch == 0
        {
            return Err(DecodeError);
        }
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityProof {
    pub store_version: u64,
    pub authority: VnodeAuthority,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementProof {
    pub store_version: u64,
    pub placement: PlacementRecord,
}

pub fn valid_placement_transition(
    previous: &PlacementRecord,
    next: &PlacementRecord,
) -> Result<(), DecodeError> {
    previous.validate()?;
    next.validate()?;
    if previous.cluster_id != next.cluster_id
        || previous.vnode_count != next.vnode_count
        || previous.epoch.checked_add(1) != Some(next.epoch)
    {
        return Err(DecodeError);
    }
    for (old, new) in previous.vnodes.iter().zip(&next.vnodes) {
        if old.vnode != new.vnode {
            return Err(DecodeError);
        }
        match (old.next_members, new.next_members) {
            (None, Some(joining))
                if old.members == new.members && quorums_overlap(old.members, joining) => {}
            (Some(joining), None) if new.members == joining => {}
            _ => return Err(DecodeError),
        }
    }
    Ok(())
}

pub fn vnode_for(volume: VolumeId, vnode_count: u32) -> VnodeId {
    assert!(vnode_count > 0, "vnode count must be positive");
    let mut value = volume.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^= value >> 31;
    VnodeId(u32::try_from(value % u64::from(vnode_count)).expect("vnode index fits"))
}

fn distinct_members(members: [HostId; REPLICATION_FACTOR]) -> bool {
    members[0] != members[1] && members[0] != members[2] && members[1] != members[2]
}

fn quorums_overlap(
    left: [HostId; REPLICATION_FACTOR],
    right: [HostId; REPLICATION_FACTOR],
) -> bool {
    left.into_iter().filter(|host| right.contains(host)).count() >= 2
}

fn encode_members(e: &mut Enc, members: [HostId; REPLICATION_FACTOR]) {
    for member in members {
        e.u16(member.0);
    }
}

fn decode_members(d: &mut Dec<'_>) -> Result<[HostId; REPLICATION_FACTOR], DecodeError> {
    Ok([HostId(d.u16()?), HostId(d.u16()?), HostId(d.u16()?)])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement() -> PlacementRecord {
        PlacementRecord::new(
            7,
            3,
            vec![
                VnodePlacement {
                    vnode: VnodeId(0),
                    members: [HostId(1), HostId(2), HostId(3)],
                    next_members: None,
                },
                VnodePlacement {
                    vnode: VnodeId(1),
                    members: [HostId(2), HostId(3), HostId(4)],
                    next_members: Some([HostId(3), HostId(4), HostId(5)]),
                },
            ],
        )
        .expect("placement")
    }

    #[test]
    fn placement_round_trips_and_rejects_non_overlapping_transition() {
        let placement = placement();
        assert_eq!(PlacementRecord::decode(&placement.encode()), Ok(placement));
        assert!(
            PlacementRecord::new(
                7,
                4,
                vec![VnodePlacement {
                    vnode: VnodeId(0),
                    members: [HostId(1), HostId(2), HostId(3)],
                    next_members: Some([HostId(4), HostId(5), HostId(6)]),
                }],
            )
            .is_err()
        );
    }

    #[test]
    fn challenge_defense_and_revocation_require_the_exact_nonce() {
        let active = HostSessionRecord::initial(HostId(2), 99).expect("active");
        let challenge = active.challenge(HostId(1), 44, 1234).expect("challenge");
        assert!(challenge.defend(99, 45).is_err());
        assert!(challenge.revoke(45).is_err());
        assert_eq!(
            challenge.defend(99, 44),
            Ok(HostSessionRecord::Active {
                host: HostId(2),
                session: 99,
                epoch: 1,
            })
        );
        assert_eq!(
            challenge.revoke(44),
            Ok(HostSessionRecord::Revoked {
                host: HostId(2),
                old_session: 99,
                epoch: 2,
                nonce: 44,
            })
        );
    }

    #[test]
    fn every_authority_record_detects_every_single_bit_flip() {
        let records = [
            placement().encode(),
            HostSessionRecord::initial(HostId(1), 9)
                .expect("session")
                .encode(),
            VnodeAuthority {
                cluster_id: 7,
                placement_epoch: 3,
                vnode: VnodeId(1),
                generation: 8,
                primary: HostId(3),
                primary_session: 11,
                primary_host_epoch: 2,
            }
            .encode(),
        ];
        for (record_index, bytes) in records.into_iter().enumerate() {
            for bit in 0..bytes.len() * 8 {
                let mut damaged = bytes.clone();
                damaged[bit / 8] ^= 1 << (bit % 8);
                let rejected = match record_index {
                    0 => PlacementRecord::decode(&damaged).is_err(),
                    1 => HostSessionRecord::decode(&damaged).is_err(),
                    2 => VnodeAuthority::decode(&damaged).is_err(),
                    _ => unreachable!(),
                };
                assert!(rejected, "record {record_index} bit {bit} was accepted");
            }
        }
    }

    #[test]
    fn vnode_mapping_is_stable_and_bounded() {
        assert_eq!(vnode_for(VolumeId(1), 1024), VnodeId(193));
        assert_eq!(vnode_for(VolumeId(2), 1024), VnodeId(718));
        assert!(vnode_for(VolumeId(u64::MAX), 7).0 < 7);
    }

    #[test]
    fn authority_must_name_a_member_of_the_current_or_joint_group() {
        let placement = placement();
        let authority = VnodeAuthority {
            cluster_id: 7,
            placement_epoch: 3,
            vnode: VnodeId(1),
            generation: 8,
            primary: HostId(5),
            primary_session: 11,
            primary_host_epoch: 2,
        };
        assert!(authority.validate(&placement).is_ok());
        assert!(
            VnodeAuthority {
                primary: HostId(8),
                ..authority
            }
            .validate(&placement)
            .is_err()
        );
    }

    #[test]
    fn placement_changes_enter_and_leave_joint_consensus() {
        let old = PlacementRecord::new(
            7,
            3,
            vec![VnodePlacement {
                vnode: VnodeId(0),
                members: [HostId(1), HostId(2), HostId(3)],
                next_members: None,
            }],
        )
        .expect("placement");
        let mut joint = old.clone();
        joint.epoch += 1;
        joint.vnodes[0].next_members = Some([HostId(2), HostId(3), HostId(4)]);
        assert!(valid_placement_transition(&old, &joint).is_ok());

        let mut final_record = joint.clone();
        final_record.epoch += 1;
        for vnode in &mut final_record.vnodes {
            vnode.members = vnode.next_members.take().expect("joint members");
        }
        assert!(valid_placement_transition(&joint, &final_record).is_ok());

        let mut blind = old.clone();
        blind.epoch += 1;
        blind.vnodes[0].members = [HostId(2), HostId(3), HostId(4)];
        assert!(valid_placement_transition(&old, &blind).is_err());
    }
}
