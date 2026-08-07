//! Deterministic passive-stash placement. The output is an ordered candidate
//! list, never a replication set: callers use exactly one active target and
//! advance only through the fenced head assignment (R4.8/R6.6).

use crate::types::{HostId, VsetId};

/// Bound configuration work and make a malicious weight unable to create an
/// unbounded placement loop.
pub const MAX_VIRTUAL_TOKENS: u16 = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerCandidate {
    pub host: HostId,
    /// Relative placement weight, represented as virtual tokens.
    pub weight: u16,
    pub failure_domain: u16,
    pub drained: bool,
}

/// Rank every eligible peer. A caller may try later entries after health
/// failures, but must publish and write to only one selected peer.
pub fn rank_stash_candidates(
    membership_epoch: u64,
    primary: HostId,
    primary_failure_domain: u16,
    vset: VsetId,
    roster: &[PeerCandidate],
) -> Vec<HostId> {
    let has_distinct_domain = roster.iter().any(|candidate| {
        candidate.host != primary
            && !candidate.drained
            && candidate.weight > 0
            && candidate.failure_domain != primary_failure_domain
    });

    let mut ranked: Vec<(u64, HostId)> = roster
        .iter()
        .filter(|candidate| {
            candidate.host != primary
                && !candidate.drained
                && candidate.weight > 0
                && (!has_distinct_domain || candidate.failure_domain != primary_failure_domain)
        })
        .map(|candidate| {
            let tokens = candidate.weight.min(MAX_VIRTUAL_TOKENS);
            let score = (0..tokens)
                .map(|token| placement_hash(membership_epoch, primary, vset, candidate.host, token))
                .max()
                .expect("positive token count");
            (score, candidate.host)
        })
        .collect();
    ranked.sort_unstable_by(|(score_a, host_a), (score_b, host_b)| {
        score_b.cmp(score_a).then_with(|| host_a.0.cmp(&host_b.0))
    });
    ranked.into_iter().map(|(_, host)| host).collect()
}

fn placement_hash(
    membership_epoch: u64,
    primary: HostId,
    vset: VsetId,
    candidate: HostId,
    token: u16,
) -> u64 {
    // FNV-1a over fixed-width little-endian fields followed by SplitMix64.
    // Unlike DefaultHasher this is stable across processes and Rust releases.
    let mut hash = 0xCBF2_9CE4_8422_2325u64;
    for byte in membership_epoch
        .to_le_bytes()
        .into_iter()
        .chain(primary.0.to_le_bytes())
        .chain(vset.0.to_le_bytes())
        .chain(candidate.0.to_le_bytes())
        .chain(token.to_le_bytes())
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

    fn peer(host: u16, weight: u16, domain: u16) -> PeerCandidate {
        PeerCandidate {
            host: HostId(host),
            weight,
            failure_domain: domain,
            drained: false,
        }
    }

    #[test]
    fn ranking_is_complete_deterministic_and_filters_ineligible_hosts() {
        let mut roster = vec![peer(0, 10, 1), peer(1, 1, 2), peer(2, 4, 3), peer(3, 0, 4)];
        roster.push(PeerCandidate {
            drained: true,
            ..peer(4, 8, 5)
        });
        let first = rank_stash_candidates(7, HostId(0), 1, VsetId(99), &roster);
        let second = rank_stash_candidates(7, HostId(0), 1, VsetId(99), &roster);
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert!(first.contains(&HostId(1)));
        assert!(first.contains(&HostId(2)));
        assert!(!first.contains(&HostId(0)));
    }

    #[test]
    fn a_distinct_failure_domain_is_preferred_when_available() {
        let roster = vec![peer(1, 1, 9), peer(2, 1, 10), peer(3, 1, 9)];
        assert_eq!(
            rank_stash_candidates(1, HostId(0), 9, VsetId(1), &roster),
            vec![HostId(2)]
        );
    }

    #[test]
    fn removing_a_host_moves_only_its_assignments() {
        let roster = vec![peer(1, 1, 1), peer(2, 1, 2), peer(3, 1, 3)];
        let without_two = vec![peer(1, 1, 1), peer(3, 1, 3)];
        for vset in 1..=2_000 {
            let before = rank_stash_candidates(5, HostId(0), 0, VsetId(vset), &roster)[0];
            let after = rank_stash_candidates(5, HostId(0), 0, VsetId(vset), &without_two)[0];
            if before != HostId(2) {
                assert_eq!(after, before, "unrelated assignment moved for vset {vset}");
            }
        }
    }

    #[test]
    fn virtual_tokens_apply_weight_without_creating_fanout() {
        let roster = vec![peer(1, 1, 1), peer(2, 4, 2)];
        let mut selections = [0usize; 2];
        for vset in 1..=10_000 {
            let ranking = rank_stash_candidates(11, HostId(0), 0, VsetId(vset), &roster);
            assert_eq!(
                ranking.len(),
                2,
                "weights rank candidates; they do not fan out"
            );
            selections[usize::from(ranking[0].0 - 1)] += 1;
        }
        assert!(selections[1] > selections[0] * 2);
        assert!(selections[1] < selections[0] * 6);
    }
}
