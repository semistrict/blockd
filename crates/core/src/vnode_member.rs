//! Durable vnode-member generation and protected-closure inventory.
//!
//! A member serializes generation adoption and protected commits through this
//! one record. Closure bytes are written first; publishing their reference in
//! the member record is the commit point. A crash may leave an unreachable
//! closure blob, but can never publish a reference to missing bytes.

use std::collections::{BTreeMap, BTreeSet};

use crate::authority::{AuthorityProof, PlacementRecord, VnodeAuthority};
use crate::format::{Dec, DecodeError, Enc, FRAME_HEADER, crc32c, open_frame, seal_frame};
use crate::types::{HostId, VolumeId};
use crate::{
    journal::JournalRecord,
    protocol::ReplicaArtifact,
    replica_spool::verify_replica_artifact,
    replica_wire::{decode_artifact, encode_artifact},
};

pub const MAGIC_VNODE_MEMBER: u32 = u32::from_le_bytes(*b"BVM1");
pub const MAGIC_VNODE_CLOSURE: u32 = u32::from_le_bytes(*b"BVC1");
const FORMAT_VERSION: u16 = 1;
const MAX_CLOSURES: u32 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtectedClosureRef {
    pub volume: VolumeId,
    pub sequence: u64,
    pub checksum: u32,
    pub len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VnodeRecoveryClosure {
    pub record: Vec<u8>,
    pub artifacts: Vec<(ReplicaArtifact, Vec<u8>)>,
}

impl VnodeRecoveryClosure {
    pub fn encode(&self, volume: VolumeId) -> Result<Vec<u8>, DecodeError> {
        self.validate(volume)?;
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u32(u32::try_from(self.record.len()).map_err(|_| DecodeError)?);
        e.bytes(&self.record);
        e.u32(u32::try_from(self.artifacts.len()).map_err(|_| DecodeError)?);
        for (artifact, bytes) in &self.artifacts {
            encode_artifact(&mut e, *artifact);
            e.u32(u32::try_from(bytes.len()).map_err(|_| DecodeError)?);
            e.bytes(bytes);
        }
        Ok(seal_frame(MAGIC_VNODE_CLOSURE, &e.finish()))
    }

    pub fn decode(volume: VolumeId, bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_VNODE_CLOSURE, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION {
            return Err(DecodeError);
        }
        let record_len = usize::try_from(d.u32()?).map_err(|_| DecodeError)?;
        let record = d.bytes(record_len)?.to_vec();
        let count = d.u32()?;
        if count > MAX_CLOSURES {
            return Err(DecodeError);
        }
        let mut artifacts = Vec::with_capacity(usize::try_from(count).map_err(|_| DecodeError)?);
        for _ in 0..count {
            let artifact = decode_artifact(&mut d)?;
            let len = usize::try_from(d.u32()?).map_err(|_| DecodeError)?;
            artifacts.push((artifact, d.bytes(len)?.to_vec()));
        }
        d.finish()?;
        let closure = Self { record, artifacts };
        closure.validate(volume)?;
        Ok(closure)
    }

    fn validate(&self, volume: VolumeId) -> Result<(), DecodeError> {
        JournalRecord::decode(volume, &self.record).map_err(|_| DecodeError)?;
        let mut previous = None;
        for (artifact, bytes) in &self.artifacts {
            if previous.is_some_and(|previous| previous >= *artifact)
                || verify_replica_artifact(volume, *artifact, bytes).is_err()
            {
                return Err(DecodeError);
            }
            previous = Some(*artifact);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VnodeMemberRecord {
    pub authority: VnodeAuthority,
    pub closures: Vec<ProtectedClosureRef>,
}

impl VnodeMemberRecord {
    pub fn new(authority: VnodeAuthority) -> Self {
        Self {
            authority,
            closures: Vec::new(),
        }
    }

    pub fn validate(&self, placement: &PlacementRecord) -> Result<(), DecodeError> {
        self.authority.validate(placement)?;
        if self.closures.len() > usize::try_from(MAX_CLOSURES).expect("limit fits") {
            return Err(DecodeError);
        }
        let mut previous = None;
        for closure in &self.closures {
            if closure.sequence == 0
                || closure.len == 0
                || placement.vnode(closure.volume) != self.authority.vnode
                || previous.is_some_and(|volume| volume >= closure.volume)
            {
                return Err(DecodeError);
            }
            previous = Some(closure.volume);
        }
        Ok(())
    }

    pub fn closure(&self, volume: VolumeId) -> Option<ProtectedClosureRef> {
        self.closures
            .binary_search_by_key(&volume, |closure| closure.volume)
            .ok()
            .map(|index| self.closures[index])
    }

    pub fn commit(
        &mut self,
        placement: &PlacementRecord,
        authority: VnodeAuthority,
        closure: ProtectedClosureRef,
    ) -> Result<(), DecodeError> {
        self.validate(placement)?;
        authority.validate(placement)?;
        if authority != self.authority
            || placement.vnode(closure.volume) != authority.vnode
            || closure.sequence == 0
            || closure.len == 0
        {
            return Err(DecodeError);
        }
        match self
            .closures
            .binary_search_by_key(&closure.volume, |existing| existing.volume)
        {
            Ok(index) => {
                let current = self.closures[index];
                if closure.sequence < current.sequence
                    || (closure.sequence == current.sequence && closure != current)
                {
                    return Err(DecodeError);
                }
                self.closures[index] = closure;
            }
            Err(index) => self.closures.insert(index, closure),
        }
        Ok(())
    }

    pub fn adopt(
        &mut self,
        placement: &PlacementRecord,
        authority: VnodeAuthority,
    ) -> Result<bool, DecodeError> {
        self.validate(placement)?;
        authority.validate(placement)?;
        if authority.cluster_id != self.authority.cluster_id
            || authority.vnode != self.authority.vnode
            || authority.placement_epoch < self.authority.placement_epoch
        {
            return Err(DecodeError);
        }
        if authority == self.authority {
            return Ok(false);
        }
        if authority.generation <= self.authority.generation {
            return Err(DecodeError);
        }
        self.authority = authority;
        Ok(true)
    }

    pub fn encode(&self, placement: &PlacementRecord) -> Vec<u8> {
        self.validate(placement).expect("valid vnode member record");
        let authority = self.authority.encode();
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u32(u32::try_from(authority.len()).expect("authority frame fits"));
        e.bytes(&authority);
        e.u32(u32::try_from(self.closures.len()).expect("closure count fits"));
        for closure in &self.closures {
            e.u64(closure.volume.0);
            e.u64(closure.sequence);
            e.u32(closure.checksum);
            e.u32(closure.len);
        }
        seal_frame(MAGIC_VNODE_MEMBER, &e.finish())
    }

    pub fn decode(bytes: &[u8], placement: &PlacementRecord) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_VNODE_MEMBER, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION {
            return Err(DecodeError);
        }
        let authority_len = usize::try_from(d.u32()?).map_err(|_| DecodeError)?;
        let authority = VnodeAuthority::decode(d.bytes(authority_len)?)?;
        let count = d.u32()?;
        if count > MAX_CLOSURES {
            return Err(DecodeError);
        }
        let mut closures = Vec::with_capacity(usize::try_from(count).map_err(|_| DecodeError)?);
        for _ in 0..count {
            closures.push(ProtectedClosureRef {
                volume: VolumeId(d.u64()?),
                sequence: d.u64()?,
                checksum: d.u32()?,
                len: d.u32()?,
            });
        }
        d.finish()?;
        let record = Self {
            authority,
            closures,
        };
        record.validate(placement)?;
        Ok(record)
    }

    /// Recover the last complete state from the append-only member log. A
    /// partial final frame is a crash tail; damage to a complete frame is
    /// corruption and must not be skipped.
    pub fn decode_log(
        bytes: &[u8],
        placement: &PlacementRecord,
    ) -> Result<Option<Self>, DecodeError> {
        let mut offset = 0usize;
        let mut latest = None;
        while offset < bytes.len() {
            let rest = &bytes[offset..];
            if rest.len() < FRAME_HEADER {
                break;
            }
            let payload_len = u32::from_le_bytes(rest[4..8].try_into().map_err(|_| DecodeError)?);
            let frame_len = FRAME_HEADER
                .checked_add(usize::try_from(payload_len).map_err(|_| DecodeError)?)
                .ok_or(DecodeError)?;
            if frame_len > rest.len() {
                break;
            }
            latest = Some(Self::decode(&rest[..frame_len], placement)?);
            offset = offset.checked_add(frame_len).ok_or(DecodeError)?;
        }
        Ok(latest)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdoptionReceipt {
    pub member: HostId,
    pub proof: AuthorityProof,
    pub closures: Vec<ProtectedClosureRef>,
}

/// Validate that receipts durably cover every voting set and return the
/// highest protected closure reported for each volume.
pub fn adoption_quorum(
    placement: &PlacementRecord,
    proof: AuthorityProof,
    receipts: &[AdoptionReceipt],
) -> Result<BTreeMap<VolumeId, ProtectedClosureRef>, DecodeError> {
    proof.authority.validate(placement)?;
    let vnode = placement
        .placement(proof.authority.vnode)
        .ok_or(DecodeError)?;
    let mut members = BTreeSet::new();
    let mut highest = BTreeMap::new();
    for receipt in receipts {
        if receipt.proof != proof || !members.insert(receipt.member) {
            return Err(DecodeError);
        }
        if !vnode.voting_sets().any(|set| set.contains(&receipt.member)) {
            return Err(DecodeError);
        }
        for closure in &receipt.closures {
            if placement.vnode(closure.volume) != proof.authority.vnode
                || closure.sequence == 0
                || closure.len == 0
            {
                return Err(DecodeError);
            }
            match highest.get(&closure.volume).copied() {
                None => {
                    highest.insert(closure.volume, *closure);
                }
                Some(current) if closure.sequence > current.sequence => {
                    highest.insert(closure.volume, *closure);
                }
                Some(current) if closure.sequence == current.sequence && *closure != current => {
                    return Err(DecodeError);
                }
                Some(_) => {}
            }
        }
    }
    if vnode.voting_sets().any(|set| {
        set.into_iter()
            .filter(|member| members.contains(member))
            .count()
            < 2
    }) {
        return Err(DecodeError);
    }
    Ok(highest)
}

pub fn closure_ref(volume: VolumeId, sequence: u64, bytes: &[u8]) -> Option<ProtectedClosureRef> {
    Some(ProtectedClosureRef {
        volume,
        sequence,
        checksum: crc32c(bytes),
        len: u32::try_from(bytes.len()).ok()?,
    })
    .filter(|closure| closure.sequence > 0 && closure.len > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::{VnodeId, VnodePlacement};

    fn placement(next_members: Option<[HostId; 3]>) -> PlacementRecord {
        PlacementRecord::new(
            8,
            4,
            vec![VnodePlacement {
                vnode: VnodeId(0),
                members: [HostId(1), HostId(2), HostId(3)],
                next_members,
            }],
        )
        .expect("valid placement")
    }

    fn authority(generation: u64, primary: HostId) -> VnodeAuthority {
        VnodeAuthority {
            cluster_id: 8,
            placement_epoch: 4,
            vnode: VnodeId(0),
            generation,
            primary,
            primary_session: generation + 100,
            primary_host_epoch: generation,
        }
    }

    #[test]
    fn member_record_roundtrips_and_rejects_damage() {
        let placement = placement(None);
        let mut record = VnodeMemberRecord::new(authority(1, HostId(1)));
        let closure = closure_ref(VolumeId(7), 9, b"protected").expect("closure");
        record
            .commit(&placement, authority(1, HostId(1)), closure)
            .expect("commit");
        let bytes = record.encode(&placement);
        assert_eq!(
            VnodeMemberRecord::decode(&bytes, &placement),
            Ok(record.clone())
        );
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(VnodeMemberRecord::decode(&damaged, &placement).is_err());
        }

        let mut log = bytes.clone();
        log.extend_from_slice(&bytes[..bytes.len() / 2]);
        assert_eq!(
            VnodeMemberRecord::decode_log(&log, &placement),
            Ok(Some(record))
        );
    }

    #[test]
    fn stale_commit_is_rejected_after_adoption() {
        let placement = placement(None);
        let old = authority(1, HostId(1));
        let mut record = VnodeMemberRecord::new(old);
        record
            .adopt(&placement, authority(2, HostId(2)))
            .expect("adopt");
        assert!(
            record
                .commit(
                    &placement,
                    old,
                    closure_ref(VolumeId(7), 1, b"old").expect("closure")
                )
                .is_err()
        );
    }

    #[test]
    fn joint_placement_requires_both_quorums() {
        let placement = placement(Some([HostId(2), HostId(3), HostId(4)]));
        let proof = AuthorityProof {
            store_version: 11,
            authority: authority(2, HostId(2)),
        };
        let receipt = |member| AdoptionReceipt {
            member,
            proof,
            closures: Vec::new(),
        };
        assert!(
            adoption_quorum(&placement, proof, &[receipt(HostId(1)), receipt(HostId(2))]).is_err()
        );
        assert!(
            adoption_quorum(&placement, proof, &[receipt(HostId(2)), receipt(HostId(3))]).is_ok()
        );
    }
}
