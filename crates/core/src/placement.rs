//! Deterministic passive-stash placement. The output is an ordered candidate
//! list, never a replication set: callers use exactly one active target and
//! advance only through the fenced head assignment (R6.6).

use prost::Message;

use crate::format::{open_frame, seal_frame};
use crate::types::{HostId, VolumeId};

const CLUSTER_PLACEMENT_MAGIC: u32 = u32::from_le_bytes(*b"BCPL");
const CLUSTER_PLACEMENT_VERSION: u32 = 1;
const MAX_CLUSTER_PLACEMENT_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;
pub const MIN_PLACEMENT_MEMBERS: usize = 3;

#[derive(Clone, PartialEq, prost::Message)]
struct ClusterPlacementWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    cluster_id: u64,
    #[prost(uint64, tag = "3")]
    epoch: u64,
    // Field 4 was the removed weighted candidate message and stays unused.
    #[prost(uint32, repeated, packed = "true", tag = "5")]
    roster: Vec<u32>,
}

/// The cluster's single durable membership snapshot. Object-store CAS on this
/// record serializes both authority membership and passive-stash placement.
/// Host IDs are permanent and never recycled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterPlacement {
    pub cluster_id: u64,
    pub epoch: u64,
    pub roster: Vec<HostId>,
}

impl ClusterPlacement {
    pub fn new(cluster_id: u64, epoch: u64, members: Vec<HostId>) -> Option<Self> {
        let placement = Self::from_members(cluster_id, epoch, members);
        placement.validate()?;
        Some(placement)
    }

    pub fn from_members(cluster_id: u64, epoch: u64, mut members: Vec<HostId>) -> Self {
        members.sort_unstable();
        members.dedup();
        Self {
            cluster_id,
            epoch,
            roster: members,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        self.validate().expect("valid cluster placement");
        let wire = ClusterPlacementWire {
            version: CLUSTER_PLACEMENT_VERSION,
            cluster_id: self.cluster_id,
            epoch: self.epoch,
            roster: self.roster.iter().map(|host| host.get()).collect(),
        };
        let payload = wire.encode_to_vec();
        assert!(
            payload.len() <= MAX_CLUSTER_PLACEMENT_PAYLOAD_BYTES,
            "cluster placement payload exceeds the durable format bound"
        );
        seal_frame(CLUSTER_PLACEMENT_MAGIC, &payload)
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let payload = open_frame(CLUSTER_PLACEMENT_MAGIC, bytes).ok()?;
        if payload.len() > MAX_CLUSTER_PLACEMENT_PAYLOAD_BYTES {
            return None;
        }
        let wire = ClusterPlacementWire::decode(payload).ok()?;
        if wire.encode_to_vec() != payload
            || wire.version != CLUSTER_PLACEMENT_VERSION
            || wire.cluster_id == 0
            || wire.epoch == 0
        {
            return None;
        }
        let roster = wire.roster.into_iter().map(HostId::new).collect();
        let placement = Self {
            cluster_id: wire.cluster_id,
            epoch: wire.epoch,
            roster,
        };
        placement.validate()?;
        Some(placement)
    }

    pub fn validate(&self) -> Option<()> {
        (self.cluster_id != 0
            && self.epoch != 0
            && self.roster.len() >= MIN_PLACEMENT_MEMBERS
            && self.roster.windows(2).all(|pair| pair[0] < pair[1]))
        .then_some(())
    }

    pub fn contains(&self, host: HostId) -> bool {
        self.roster.binary_search(&host).is_ok()
    }
}

/// Rank every eligible peer. A caller may try later entries after health
/// failures, but must publish and write to only one selected peer.
pub fn rank_stash_candidates(
    membership_epoch: u64,
    primary: HostId,
    volume: VolumeId,
    roster: &[HostId],
) -> Vec<HostId> {
    let mut ranked: Vec<(u64, HostId)> = roster
        .iter()
        .copied()
        .filter(|&candidate| candidate != primary)
        .map(|candidate| {
            (
                placement_hash(membership_epoch, primary, volume, candidate),
                candidate,
            )
        })
        .collect();
    ranked.sort_unstable_by(|(score_a, host_a), (score_b, host_b)| {
        score_b
            .cmp(score_a)
            .then_with(|| host_a.get().cmp(&host_b.get()))
    });
    ranked.into_iter().map(|(_, host)| host).collect()
}

fn placement_hash(
    membership_epoch: u64,
    primary: HostId,
    volume: VolumeId,
    candidate: HostId,
) -> u64 {
    // FNV-1a over fixed-width little-endian fields followed by SplitMix64.
    // Unlike DefaultHasher this is stable across processes and Rust releases.
    let mut hash = 0xCBF2_9CE4_8422_2325u64;
    for byte in membership_epoch
        .to_le_bytes()
        .into_iter()
        .chain(primary.get().to_le_bytes())
        .chain(volume.0.to_le_bytes())
        .chain(candidate.get().to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    let mut mixed = hash.wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_only_placement_has_one_canonical_roster_and_complete_ranking() {
        let placement = ClusterPlacement::from_members(
            7,
            9,
            vec![HostId::new(3), HostId::new(1), HostId::new(2)],
        );
        assert_eq!(
            open_frame(CLUSTER_PLACEMENT_MAGIC, &placement.encode()).expect("placement frame"),
            &[
                0x08, 0x01, 0x10, 0x07, 0x18, 0x09, 0x2a, 0x03, 0x01, 0x02, 0x03
            ]
        );
        assert_eq!(
            placement.roster,
            vec![HostId::new(1), HostId::new(2), HostId::new(3)]
        );

        let first = rank_stash_candidates(
            placement.epoch,
            HostId::new(1),
            VolumeId(99),
            &placement.roster,
        );
        let second = rank_stash_candidates(
            placement.epoch,
            HostId::new(1),
            VolumeId(99),
            &placement.roster,
        );
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert!(first.contains(&HostId::new(2)));
        assert!(first.contains(&HostId::new(3)));
    }

    #[test]
    fn cluster_placement_round_trips_permanent_host_ids() {
        let placement = ClusterPlacement::from_members(
            7,
            9,
            vec![HostId::new(3), HostId::new(2), HostId::new(1)],
        );
        assert_eq!(
            ClusterPlacement::decode(&placement.encode()),
            Some(placement)
        );
    }

    #[test]
    fn cluster_placement_requires_canonical_protobuf() {
        let placement = ClusterPlacement::from_members(
            7,
            9,
            vec![HostId::new(1), HostId::new(2), HostId::new(3)],
        );
        let encoded = placement.encode();
        let mut payload = open_frame(CLUSTER_PLACEMENT_MAGIC, &encoded)
            .expect("placement frame")
            .to_vec();
        payload.extend_from_slice(&[0xA0, 0x06, 0x01]);
        let with_unknown_field = seal_frame(CLUSTER_PLACEMENT_MAGIC, &payload);
        assert!(ClusterPlacement::decode(&with_unknown_field).is_none());
    }

    #[test]
    fn ranking_is_complete_deterministic_and_filters_ineligible_hosts() {
        let roster = (0..4).map(HostId::new).collect::<Vec<_>>();
        let first = rank_stash_candidates(7, HostId::new(0), VolumeId(99), &roster);
        let second = rank_stash_candidates(7, HostId::new(0), VolumeId(99), &roster);
        assert_eq!(first, second);
        assert_eq!(first.len(), 3);
        assert!(first.contains(&HostId::new(1)));
        assert!(first.contains(&HostId::new(2)));
        assert!(first.contains(&HostId::new(3)));
        assert!(!first.contains(&HostId::new(0)));
    }

    #[test]
    fn removing_a_host_moves_only_its_assignments() {
        let roster = (1..=3).map(HostId::new).collect::<Vec<_>>();
        let without_two = vec![HostId::new(1), HostId::new(3)];
        for volume in 1..=2_000 {
            let before = rank_stash_candidates(5, HostId::new(0), VolumeId(volume), &roster)[0];
            let after = rank_stash_candidates(5, HostId::new(0), VolumeId(volume), &without_two)[0];
            if before != HostId::new(2) {
                assert_eq!(
                    after, before,
                    "unrelated assignment moved for volume {volume}"
                );
            }
        }
    }
}
