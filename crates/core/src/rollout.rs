//! Deterministic, fail-closed rollout policy for peer-stashed durability.
//! The control plane evaluates this before selecting the explicit vset mode;
//! the data path never changes policy by itself.

use crate::types::VsetId;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PeerStashRollout {
    /// The default: no vset may be newly placed in peer-stashed mode.
    #[default]
    Disabled,
    /// First production phase: one explicitly selected failure domain.
    FailureDomain { failure_domain: u64 },
    /// Later phase: a deterministic fraction of vsets in every domain.
    Percentage { basis_points: u16, salt: u64 },
}

impl PeerStashRollout {
    pub fn allows(self, vset: VsetId, failure_domain: u64) -> bool {
        match self {
            Self::Disabled => false,
            Self::FailureDomain {
                failure_domain: allowed,
            } => failure_domain == allowed,
            Self::Percentage { basis_points, salt } => {
                basis_points <= 10_000
                    && rollout_hash(vset.0, failure_domain, salt) % 10_000 < u64::from(basis_points)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerStashRolloutSignals {
    pub false_ack: bool,
    pub recovery_mismatch: bool,
    pub non_active_peer_bytes: u64,
    pub cleanup_rewrite_bytes: u64,
    pub spool_bytes: u64,
    pub spool_capacity_bytes: u64,
}

impl PeerStashRolloutSignals {
    /// Abort rather than expanding when correctness counters move or the
    /// bounded spool reaches the alert threshold. Zero capacity fails closed.
    pub fn abort_required(self) -> bool {
        self.false_ack
            || self.recovery_mismatch
            || self.non_active_peer_bytes != 0
            || self.cleanup_rewrite_bytes != 0
            || self.spool_capacity_bytes == 0
            || self.spool_bytes.saturating_mul(100) >= self.spool_capacity_bytes.saturating_mul(80)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PeerStashGateCheck {
    AuthenticatedEncryptedTransport = 0,
    CapacityAlerting = 1,
    RecoveryDrill = 2,
    Downgrade = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerStashProductionGate(u8);

impl PeerStashProductionGate {
    #[must_use]
    pub const fn verified(mut self, check: PeerStashGateCheck) -> Self {
        self.0 |= 1 << check as u8;
        self
    }

    pub const fn ready(self) -> bool {
        self.0 == 0b1111
    }
}

fn rollout_hash(vset: u64, failure_domain: u64, salt: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in vset
        .to_le_bytes()
        .into_iter()
        .chain(failure_domain.to_le_bytes())
        .chain(salt.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollout_is_disabled_by_default_then_scopes_domain_and_percentage() {
        assert!(!PeerStashRollout::default().allows(VsetId(1), 7));
        let first = PeerStashRollout::FailureDomain { failure_domain: 7 };
        assert!(first.allows(VsetId(1), 7));
        assert!(!first.allows(VsetId(1), 8));

        let percentage = PeerStashRollout::Percentage {
            basis_points: 500,
            salt: 19,
        };
        let selected = (0..10_000)
            .filter(|&vset| percentage.allows(VsetId(vset), 7))
            .count();
        assert!((400..=600).contains(&selected), "selected {selected}");
        assert_eq!(
            percentage.allows(VsetId(91), 7),
            percentage.allows(VsetId(91), 7)
        );
    }

    #[test]
    fn rollout_aborts_on_every_guardrail_and_gate_fails_closed() {
        let healthy = PeerStashRolloutSignals {
            spool_bytes: 79,
            spool_capacity_bytes: 100,
            ..PeerStashRolloutSignals::default()
        };
        assert!(!healthy.abort_required());
        for unhealthy in [
            PeerStashRolloutSignals {
                false_ack: true,
                ..healthy
            },
            PeerStashRolloutSignals {
                recovery_mismatch: true,
                ..healthy
            },
            PeerStashRolloutSignals {
                non_active_peer_bytes: 1,
                ..healthy
            },
            PeerStashRolloutSignals {
                cleanup_rewrite_bytes: 1,
                ..healthy
            },
            PeerStashRolloutSignals {
                spool_bytes: 80,
                ..healthy
            },
        ] {
            assert!(unhealthy.abort_required());
        }
        assert!(!PeerStashProductionGate::default().ready());
        let gate = PeerStashProductionGate::default()
            .verified(PeerStashGateCheck::AuthenticatedEncryptedTransport)
            .verified(PeerStashGateCheck::CapacityAlerting)
            .verified(PeerStashGateCheck::RecoveryDrill)
            .verified(PeerStashGateCheck::Downgrade);
        assert!(gate.ready());
    }
}
