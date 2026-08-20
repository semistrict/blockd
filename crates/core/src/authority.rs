//! Persistent placement and host-session records.
//!
//! These records are changed only with object-store conditional writes.

use prost::Message;

use crate::format::{DecodeError, open_frame, seal_frame};
use crate::placement::ClusterPlacement;

pub const MAGIC_HOST_SESSION: u32 = u32::from_le_bytes(*b"BHS1");
const FORMAT_VERSION: u32 = 1;
const MAX_HOST_SESSION_PAYLOAD_BYTES: usize = 256;

#[derive(Clone, PartialEq, Message)]
struct HostSessionWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(oneof = "host_session_wire::State", tags = "2, 3, 4")]
    state: Option<host_session_wire::State>,
}

mod host_session_wire {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum State {
        #[prost(message, tag = "2")]
        Active(super::ActiveSessionWire),
        #[prost(message, tag = "3")]
        Challenge(super::ChallengeSessionWire),
        #[prost(message, tag = "4")]
        Revoked(super::RevokedSessionWire),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ActiveSessionWire {
    #[prost(uint64, tag = "1")]
    session: u64,
    #[prost(uint64, tag = "2")]
    epoch: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ChallengeSessionWire {
    #[prost(uint64, tag = "1")]
    session: u64,
    #[prost(uint64, tag = "2")]
    epoch: u64,
    #[prost(uint64, tag = "3")]
    nonce: u64,
    #[prost(uint64, tag = "4")]
    challenged_at: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RevokedSessionWire {
    #[prost(uint64, tag = "1")]
    old_session: u64,
    #[prost(uint64, tag = "2")]
    epoch: u64,
    #[prost(uint64, tag = "3")]
    nonce: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostSessionRecord {
    Active {
        session: u64,
        epoch: u64,
    },
    Challenge {
        session: u64,
        epoch: u64,
        nonce: u64,
        challenged_at: u64,
    },
    Revoked {
        old_session: u64,
        epoch: u64,
        nonce: u64,
    },
}

impl HostSessionRecord {
    pub fn initial(session: u64) -> Result<Self, DecodeError> {
        if session == 0 {
            return Err(DecodeError);
        }
        Ok(Self::Active { session, epoch: 1 })
    }

    pub fn epoch(self) -> u64 {
        match self {
            Self::Active { epoch, .. }
            | Self::Challenge { epoch, .. }
            | Self::Revoked { epoch, .. } => epoch,
        }
    }

    pub fn challenge(self, nonce: u64, challenged_at: u64) -> Result<Self, DecodeError> {
        let Self::Active { session, epoch } = self else {
            return Err(DecodeError);
        };
        if nonce == 0 {
            return Err(DecodeError);
        }
        Ok(Self::Challenge {
            session,
            epoch,
            nonce,
            challenged_at,
        })
    }

    pub fn defend(self, session: u64, nonce: u64) -> Result<Self, DecodeError> {
        let Self::Challenge {
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
        Ok(Self::Active { session, epoch })
    }

    /// Voluntarily retire the exact active session during a planned drain.
    /// The enclosing object-store CAS is the exclusion boundary: a stale
    /// process cannot retire a replacement session with a different token.
    pub fn retire(self, session: u64, nonce: u64) -> Result<Self, DecodeError> {
        let Self::Active {
            session: active_session,
            epoch,
        } = self
        else {
            return Err(DecodeError);
        };
        if session != active_session || nonce == 0 {
            return Err(DecodeError);
        }
        Ok(Self::Revoked {
            old_session: session,
            epoch: epoch.checked_add(1).ok_or(DecodeError)?,
            nonce,
        })
    }

    pub fn revoke(self, nonce: u64) -> Result<Self, DecodeError> {
        let Self::Challenge {
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
            old_session: session,
            epoch: epoch.checked_add(1).ok_or(DecodeError)?,
            nonce,
        })
    }

    pub fn activate(self, session: u64) -> Result<Self, DecodeError> {
        let Self::Revoked { epoch, .. } = self else {
            return Err(DecodeError);
        };
        if session == 0 {
            return Err(DecodeError);
        }
        Ok(Self::Active { session, epoch })
    }

    pub fn encode(self) -> Vec<u8> {
        let state = match self {
            Self::Active { session, epoch } => {
                host_session_wire::State::Active(ActiveSessionWire { session, epoch })
            }
            Self::Challenge {
                session,
                epoch,
                nonce,
                challenged_at,
            } => host_session_wire::State::Challenge(ChallengeSessionWire {
                session,
                epoch,
                nonce,
                challenged_at,
            }),
            Self::Revoked {
                old_session,
                epoch,
                nonce,
            } => host_session_wire::State::Revoked(RevokedSessionWire {
                old_session,
                epoch,
                nonce,
            }),
        };
        let payload = HostSessionWire {
            version: FORMAT_VERSION,
            state: Some(state),
        }
        .encode_to_vec();
        seal_frame(MAGIC_HOST_SESSION, &payload)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_HOST_SESSION, bytes)?;
        if payload.len() > MAX_HOST_SESSION_PAYLOAD_BYTES {
            return Err(DecodeError);
        }
        let wire = HostSessionWire::decode(payload).map_err(|_| DecodeError)?;
        if wire.version != FORMAT_VERSION || wire.encode_to_vec() != payload {
            return Err(DecodeError);
        }
        let record = match wire.state.ok_or(DecodeError)? {
            host_session_wire::State::Active(active) => Self::Active {
                session: active.session,
                epoch: active.epoch,
            },
            host_session_wire::State::Challenge(challenge) => Self::Challenge {
                session: challenge.session,
                epoch: challenge.epoch,
                nonce: challenge.nonce,
                challenged_at: challenge.challenged_at,
            },
            host_session_wire::State::Revoked(revoked) => Self::Revoked {
                old_session: revoked.old_session,
                epoch: revoked.epoch,
                nonce: revoked.nonce,
            },
        };
        if record.epoch() == 0
            || match record {
                Self::Active { session, .. } => session == 0,
                Self::Challenge { session, nonce, .. } => session == 0 || nonce == 0,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementProof {
    pub store_version: u64,
    pub placement: ClusterPlacement,
}

pub fn valid_placement_transition(
    previous: &ClusterPlacement,
    next: &ClusterPlacement,
) -> Result<(), DecodeError> {
    previous.validate().ok_or(DecodeError)?;
    next.validate().ok_or(DecodeError)?;
    if previous.cluster_id != next.cluster_id || previous.epoch.checked_add(1) != Some(next.epoch) {
        return Err(DecodeError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::types::HostId;

    const fn id(host: u32) -> HostId {
        HostId::new(host)
    }

    fn placement() -> ClusterPlacement {
        ClusterPlacement::new(7, 3, vec![id(1), id(2), id(3), id(4)]).expect("placement")
    }

    #[derive(Clone, PartialEq, Message)]
    struct SessionWireProbe {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(oneof = "session_wire_probe::State", tags = "2, 3, 4")]
        state: Option<session_wire_probe::State>,
    }

    mod session_wire_probe {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum State {
            #[prost(message, tag = "2")]
            Active(super::ActiveWireProbe),
            #[prost(message, tag = "3")]
            Challenge(super::ChallengeWireProbe),
            #[prost(message, tag = "4")]
            Revoked(super::RevokedWireProbe),
        }
    }

    #[derive(Clone, PartialEq, Message)]
    struct ActiveWireProbe {
        #[prost(uint64, tag = "1")]
        session: u64,
        #[prost(uint64, tag = "2")]
        epoch: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    struct ChallengeWireProbe {
        #[prost(uint64, tag = "1")]
        session: u64,
        #[prost(uint64, tag = "2")]
        epoch: u64,
        #[prost(uint64, tag = "3")]
        nonce: u64,
        #[prost(uint64, tag = "4")]
        challenged_at: u64,
    }

    #[derive(Clone, PartialEq, Message)]
    struct RevokedWireProbe {
        #[prost(uint64, tag = "1")]
        old_session: u64,
        #[prost(uint64, tag = "2")]
        epoch: u64,
        #[prost(uint64, tag = "3")]
        nonce: u64,
    }

    #[test]
    fn host_session_record_is_a_protobuf_oneof() {
        let encoded = HostSessionRecord::Challenge {
            session: 11,
            epoch: 3,
            nonce: 17,
            challenged_at: 19,
        }
        .encode();
        let payload = open_frame(MAGIC_HOST_SESSION, &encoded).expect("session frame");
        let probe = SessionWireProbe::decode(payload).expect("protobuf session payload");

        assert_eq!(probe.version, 1);
        assert!(matches!(
            probe.state,
            Some(session_wire_probe::State::Challenge(ChallengeWireProbe {
                session: 11,
                epoch: 3,
                nonce: 17,
                challenged_at: 19,
            }))
        ));
    }

    #[test]
    fn placement_round_trips_and_allows_store_serialized_roster_replacement() {
        let placement = placement();
        assert_eq!(
            ClusterPlacement::decode(&placement.encode()),
            Some(placement)
        );
        let replacement = ClusterPlacement::new(7, 4, vec![id(4), id(5), id(6)])
            .expect("the object-store CAS, not roster overlap, serializes replacement");
        assert_eq!(
            ClusterPlacement::decode(&replacement.encode()),
            Some(replacement)
        );
    }

    #[test]
    fn challenge_defense_and_revocation_require_the_exact_nonce() {
        let active = HostSessionRecord::initial(99).expect("active");
        assert!(active.retire(100, 44).is_err());
        assert!(active.retire(99, 0).is_err());
        assert_eq!(
            active.retire(99, 43),
            Ok(HostSessionRecord::Revoked {
                old_session: 99,
                epoch: 2,
                nonce: 43,
            })
        );
        let challenge = active.challenge(44, 1234).expect("challenge");
        assert!(challenge.defend(99, 45).is_err());
        assert!(challenge.revoke(45).is_err());
        assert_eq!(
            challenge.defend(99, 44),
            Ok(HostSessionRecord::Active {
                session: 99,
                epoch: 1,
            })
        );
        assert_eq!(
            challenge.revoke(44),
            Ok(HostSessionRecord::Revoked {
                old_session: 99,
                epoch: 2,
                nonce: 44,
            })
        );
    }

    #[test]
    fn revoked_session_reactivates_only_with_a_nonzero_new_token() {
        let active = HostSessionRecord::initial(99).expect("active");
        let challenged = active.challenge(12, 34).expect("challenge");
        assert_eq!(
            challenged.defend(99, 12).expect("defense"),
            HostSessionRecord::Active {
                session: 99,
                epoch: 1,
            }
        );
        let revoked = challenged.revoke(12).expect("revoked");
        assert_eq!(
            HostSessionRecord::decode(&revoked.encode()),
            Ok(revoked),
            "the exact revoked session must survive durable encoding"
        );
        assert!(revoked.activate(0).is_err());
        assert_eq!(
            revoked.activate(100).expect("replacement session active"),
            HostSessionRecord::Active {
                session: 100,
                epoch: 2,
            }
        );
    }

    #[test]
    fn every_authority_record_detects_every_single_bit_flip() {
        let records = [
            placement().encode(),
            HostSessionRecord::initial(9).expect("session").encode(),
        ];
        for (record_index, bytes) in records.into_iter().enumerate() {
            for bit in 0..bytes.len() * 8 {
                let mut damaged = bytes.clone();
                damaged[bit / 8] ^= 1 << (bit % 8);
                let rejected = match record_index {
                    0 => ClusterPlacement::decode(&damaged).is_none(),
                    1 => HostSessionRecord::decode(&damaged).is_err(),
                    _ => unreachable!(),
                };
                assert!(rejected, "record {record_index} bit {bit} was accepted");
            }
        }
    }

    #[test]
    fn placement_transition_is_one_direct_object_store_cas() {
        let old = ClusterPlacement::new(7, 3, vec![id(1), id(2), id(3)]).expect("placement");
        let next = ClusterPlacement::new(7, 4, vec![id(4), id(5), id(6)]).expect("replacement");
        assert!(valid_placement_transition(&old, &next).is_ok());

        let skipped = ClusterPlacement::new(7, 5, vec![id(4), id(5), id(6)]).expect("skipped");
        assert!(valid_placement_transition(&old, &skipped).is_err());
    }
}
