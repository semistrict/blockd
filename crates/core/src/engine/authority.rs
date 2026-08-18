use crate::authority::{
    AuthorityProof, HostSessionRecord, PlacementProof, PlacementRecord, VnodeAuthority,
    valid_placement_transition,
};
use crate::layout;
use crate::types::HostId;
use crate::world::{Store, StoreError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityError {
    Unavailable,
    Conflict,
    Corrupt,
    Fenced,
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionedSession {
    pub store_version: u64,
    pub record: HostSessionRecord,
}

impl VersionedSession {
    async fn replace<W: Store>(
        self,
        world: &W,
        record: HostSessionRecord,
    ) -> Result<Self, AuthorityError> {
        let store_version = Store::put_cas(
            world,
            layout::host_session_key(record.host()),
            Some(self.store_version),
            record.encode(),
        )
        .await
        .map_err(map_store_error)?;
        Ok(Self {
            store_version,
            record,
        })
    }

    async fn defend<W: Store>(
        self,
        world: &W,
        session: u64,
        nonce: u64,
    ) -> Result<Self, AuthorityError> {
        let defended = self
            .record
            .defend(session, nonce)
            .map_err(|_| AuthorityError::Invalid)?;
        self.replace(world, defended).await
    }

    async fn challenge<W: Store>(
        self,
        world: &W,
        challenger: HostId,
        nonce: u64,
        challenged_at: u64,
    ) -> Result<Self, AuthorityError> {
        let next = self
            .record
            .challenge(challenger, nonce, challenged_at)
            .map_err(|_| AuthorityError::Fenced)?;
        self.replace(world, next).await
    }

    async fn revoke<W: Store>(self, world: &W, nonce: u64) -> Result<Self, AuthorityError> {
        let revoked = self
            .record
            .revoke(nonce)
            .map_err(|_| AuthorityError::Invalid)?;
        self.replace(world, revoked).await
    }

    async fn activate<W: Store>(self, world: &W, session: u64) -> Result<Self, AuthorityError> {
        let active = self
            .record
            .activate(session)
            .map_err(|_| AuthorityError::Invalid)?;
        self.replace(world, active).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollSession {
    Active(VersionedSession),
    Defended(VersionedSession),
}

pub async fn read_placement<W: Store>(world: &W) -> Result<Option<PlacementProof>, AuthorityError> {
    let Some((store_version, bytes)) = Store::get(world, &layout::placement_key())
        .await
        .map_err(map_store_error)?
    else {
        return Ok(None);
    };
    let placement = PlacementRecord::decode(&bytes).map_err(|_| AuthorityError::Corrupt)?;
    Ok(Some(PlacementProof {
        store_version,
        placement,
    }))
}

pub async fn cas_placement<W: Store>(
    world: &W,
    expected: Option<&PlacementProof>,
    next: PlacementRecord,
) -> Result<PlacementProof, AuthorityError> {
    next.validate().map_err(|_| AuthorityError::Invalid)?;
    match expected {
        None if next.epoch != 1 => return Err(AuthorityError::Invalid),
        Some(previous) => valid_placement_transition(&previous.placement, &next)
            .map_err(|_| AuthorityError::Invalid)?,
        None => {}
    }
    let store_version = Store::put_cas(
        world,
        layout::placement_key(),
        expected.map(|proof| proof.store_version),
        next.encode(),
    )
    .await
    .map_err(map_store_error)?;
    Ok(PlacementProof {
        store_version,
        placement: next,
    })
}

pub async fn create_host_session<W: Store>(
    world: &W,
    host: HostId,
    session: u64,
) -> Result<VersionedSession, AuthorityError> {
    let record = HostSessionRecord::initial(host, session).map_err(|_| AuthorityError::Invalid)?;
    let store_version =
        Store::put_cas(world, layout::host_session_key(host), None, record.encode())
            .await
            .map_err(map_store_error)?;
    Ok(VersionedSession {
        store_version,
        record,
    })
}

pub async fn read_host_session<W: Store>(
    world: &W,
    host: HostId,
) -> Result<Option<VersionedSession>, AuthorityError> {
    let Some((store_version, bytes)) = Store::get(world, &layout::host_session_key(host))
        .await
        .map_err(map_store_error)?
    else {
        return Ok(None);
    };
    let record = HostSessionRecord::decode(&bytes).map_err(|_| AuthorityError::Corrupt)?;
    if record.host() != host {
        return Err(AuthorityError::Corrupt);
    }
    Ok(Some(VersionedSession {
        store_version,
        record,
    }))
}

pub async fn poll_or_defend_host_session<W: Store>(
    world: &W,
    host: HostId,
    session: u64,
) -> Result<PollSession, AuthorityError> {
    let observed = read_host_session(world, host)
        .await?
        .ok_or(AuthorityError::Fenced)?;
    match observed.record {
        HostSessionRecord::Active {
            host: found,
            session: found_session,
            ..
        } if found == host && found_session == session => Ok(PollSession::Active(observed)),
        HostSessionRecord::Challenge {
            host: found,
            session: found_session,
            nonce,
            ..
        } if found == host && found_session == session => Ok(PollSession::Defended(
            observed.defend(world, session, nonce).await?,
        )),
        _ => Err(AuthorityError::Fenced),
    }
}

pub async fn challenge_host_session<W: Store>(
    world: &W,
    host: HostId,
    challenger: HostId,
    nonce: u64,
    challenged_at: u64,
) -> Result<VersionedSession, AuthorityError> {
    let observed = read_host_session(world, host)
        .await?
        .ok_or(AuthorityError::Fenced)?;
    if matches!(observed.record, HostSessionRecord::Challenge { .. }) {
        return Ok(observed);
    }
    observed
        .challenge(world, challenger, nonce, challenged_at)
        .await
}

pub async fn revoke_host_session<W: Store>(
    world: &W,
    challenged: VersionedSession,
    nonce: u64,
) -> Result<VersionedSession, AuthorityError> {
    challenged.revoke(world, nonce).await
}

pub async fn activate_host_session<W: Store>(
    world: &W,
    revoked: VersionedSession,
    session: u64,
) -> Result<VersionedSession, AuthorityError> {
    revoked.activate(world, session).await
}

pub async fn verify_authority_proof<W: Store>(
    world: &W,
    placement: &PlacementRecord,
    proof: AuthorityProof,
) -> Result<(), AuthorityError> {
    proof
        .authority
        .validate(placement)
        .map_err(|_| AuthorityError::Invalid)?;
    let Some((store_version, bytes)) =
        Store::get(world, &layout::vnode_authority_key(proof.authority.vnode))
            .await
            .map_err(map_store_error)?
    else {
        return Err(AuthorityError::Fenced);
    };
    let observed = VnodeAuthority::decode(&bytes).map_err(|_| AuthorityError::Corrupt)?;
    if store_version != proof.store_version || observed != proof.authority {
        return Err(AuthorityError::Fenced);
    }
    Ok(())
}

pub async fn read_vnode_authority<W: Store>(
    world: &W,
    placement: &PlacementRecord,
    vnode: crate::authority::VnodeId,
) -> Result<Option<AuthorityProof>, AuthorityError> {
    let Some((store_version, bytes)) = Store::get(world, &layout::vnode_authority_key(vnode))
        .await
        .map_err(map_store_error)?
    else {
        return Ok(None);
    };
    let authority = VnodeAuthority::decode(&bytes).map_err(|_| AuthorityError::Corrupt)?;
    authority
        .validate(placement)
        .map_err(|_| AuthorityError::Invalid)?;
    Ok(Some(AuthorityProof {
        store_version,
        authority,
    }))
}

pub async fn cas_vnode_authority<W: Store>(
    world: &W,
    placement: &PlacementRecord,
    expected: Option<AuthorityProof>,
    next: VnodeAuthority,
) -> Result<AuthorityProof, AuthorityError> {
    next.validate(placement)
        .map_err(|_| AuthorityError::Invalid)?;
    let primary_session = read_host_session(world, next.primary)
        .await?
        .ok_or(AuthorityError::Fenced)?;
    if !matches!(
        primary_session.record,
        HostSessionRecord::Active { session, epoch, .. }
            if session == next.primary_session && epoch == next.primary_host_epoch
    ) {
        return Err(AuthorityError::Fenced);
    }
    match expected {
        None if next.generation != 1 => return Err(AuthorityError::Invalid),
        Some(previous)
            if previous.authority.vnode != next.vnode
                || previous.authority.cluster_id != next.cluster_id
                || previous.authority.placement_epoch != next.placement_epoch
                || previous.authority.generation.checked_add(1) != Some(next.generation) =>
        {
            return Err(AuthorityError::Invalid);
        }
        _ => {}
    }
    let store_version = Store::put_cas(
        world,
        layout::vnode_authority_key(next.vnode),
        expected.map(|proof| proof.store_version),
        next.encode(),
    )
    .await
    .map_err(map_store_error)?;
    Ok(AuthorityProof {
        store_version,
        authority: next,
    })
}

fn map_store_error(error: StoreError) -> AuthorityError {
    match error {
        StoreError::Fault(crate::protocol::StoreFault::Unavailable) => AuthorityError::Unavailable,
        StoreError::Fault(crate::protocol::StoreFault::CasConflict { .. }) => {
            AuthorityError::Conflict
        }
        StoreError::TooLarge => AuthorityError::Invalid,
    }
}
