use crate::authority::{AuthorityProof, PlacementRecord, VnodeAuthority, VnodeId};
use crate::format::crc32c;
use crate::layout;
use crate::types::{HostId, VolumeId};
use crate::vnode_member::{
    AdoptionReceipt, ProtectedClosureRef, VnodeMemberRecord, adoption_quorum, closure_ref,
};
use crate::world::{Blobs, Peers, Store};

use super::peer_client::PeerClient;
use super::{
    AuthorityError, SharedHost, cas_vnode_authority, challenge_host_session, read_vnode_authority,
    revoke_host_session, verify_authority_proof,
};

struct VnodeQuorum<'a, W> {
    state: &'a SharedHost,
    world: &'a W,
    placement: PlacementRecord,
    local: HostId,
    client: PeerClient,
}

impl<'a, W> VnodeQuorum<'a, W> {
    fn begin(state: &'a SharedHost, world: &'a W) -> Result<Self, AuthorityError> {
        let host = state.borrow();
        Ok(Self {
            state,
            world,
            placement: host
                .authority
                .placement
                .clone()
                .ok_or(AuthorityError::Invalid)?,
            local: host.config.host,
            client: host.peer_client.clone(),
        })
    }

    fn members(
        &self,
        proof: AuthorityProof,
    ) -> Result<std::collections::BTreeSet<HostId>, AuthorityError> {
        proof
            .authority
            .validate(&self.placement)
            .map_err(|_| AuthorityError::Invalid)?;
        Ok(self
            .placement
            .placement(proof.authority.vnode)
            .ok_or(AuthorityError::Invalid)?
            .voting_sets()
            .flatten()
            .collect())
    }
}

impl<W: Store + Blobs + Peers> VnodeQuorum<'_, W> {
    async fn adopt(
        &self,
        proof: AuthorityProof,
    ) -> Result<std::collections::BTreeMap<VolumeId, ProtectedClosureRef>, AuthorityError> {
        let members = self.members(proof)?;
        let mut receipts = Vec::new();
        for member in members {
            let receipt = if member == self.local {
                adopt_vnode_generation(self.world, &self.placement, member, proof)
                    .await
                    .ok()
            } else {
                self.client
                    .adopt_vnode(self.world, member, proof)
                    .await
                    .ok()
                    .map(|closures| AdoptionReceipt {
                        member,
                        proof,
                        closures,
                    })
            };
            if let Some(receipt) = receipt {
                receipts.push(receipt);
                if let Ok(inventory) = adoption_quorum(&self.placement, proof, &receipts) {
                    self.state.borrow_mut().counters.vnode_adoptions += 1;
                    return Ok(inventory);
                }
            }
        }
        Err(AuthorityError::Unavailable)
    }
}

pub async fn read_vnode_member<W: Blobs>(
    world: &W,
    placement: &PlacementRecord,
    vnode: VnodeId,
) -> Result<Option<VnodeMemberRecord>, AuthorityError> {
    let bytes = Blobs::read(world, &layout::vnode_member_blob(vnode))
        .await
        .map_err(|_| AuthorityError::Unavailable)?;
    match bytes {
        None => Ok(None),
        Some(bytes) => {
            VnodeMemberRecord::decode_log(&bytes, placement).map_err(|_| AuthorityError::Corrupt)
        }
    }
}

/// Verify the object-store proof, then make the generation durable before
/// returning the member's protected-closure inventory.
pub async fn adopt_vnode_generation<W: Store + Blobs>(
    world: &W,
    placement: &PlacementRecord,
    member: HostId,
    proof: AuthorityProof,
) -> Result<AdoptionReceipt, AuthorityError> {
    let vnode = placement
        .placement(proof.authority.vnode)
        .ok_or(AuthorityError::Invalid)?;
    if !vnode.voting_sets().any(|members| members.contains(&member)) {
        return Err(AuthorityError::Invalid);
    }
    verify_authority_proof(world, placement, proof).await?;
    let mut record = match read_vnode_member(world, placement, proof.authority.vnode).await? {
        None => VnodeMemberRecord::new(proof.authority),
        Some(mut current) => {
            current
                .adopt(placement, proof.authority)
                .map_err(|_| AuthorityError::Fenced)?;
            current
        }
    };
    // Re-encoding also validates the retained closure inventory against the
    // current placement before its authority becomes durable.
    let encoded = record.encode(placement);
    Blobs::append(
        world,
        layout::vnode_member_blob(proof.authority.vnode),
        encoded,
    )
    .await
    .map_err(|_| AuthorityError::Unavailable)?;
    Ok(AdoptionReceipt {
        member,
        proof,
        closures: std::mem::take(&mut record.closures),
    })
}

/// Commit a complete recovery closure under an already adopted generation.
/// This path intentionally has no [`Store`] bound: object storage cannot be
/// consulted by a protected sync.
pub async fn commit_vnode_closure<W: Blobs>(
    world: &W,
    placement: &PlacementRecord,
    authority: VnodeAuthority,
    volume: VolumeId,
    sequence: u64,
    bytes: Vec<u8>,
) -> Result<ProtectedClosureRef, AuthorityError> {
    let closure = closure_ref(volume, sequence, &bytes).ok_or(AuthorityError::Invalid)?;
    let mut record = read_vnode_member(world, placement, authority.vnode)
        .await?
        .ok_or(AuthorityError::Fenced)?;
    if let Some(existing) = record.closure(volume) {
        if existing.sequence > sequence {
            return Err(AuthorityError::Fenced);
        }
        if existing.sequence == sequence {
            return (existing == closure)
                .then_some(existing)
                .ok_or(AuthorityError::Corrupt);
        }
    }
    record
        .commit(placement, authority, closure)
        .map_err(|_| AuthorityError::Fenced)?;
    Blobs::write(
        world,
        layout::vnode_closure_blob(authority.vnode, volume, sequence),
        bytes,
    )
    .await
    .map_err(|_| AuthorityError::Unavailable)?;
    Blobs::append(
        world,
        layout::vnode_member_blob(authority.vnode),
        record.encode(placement),
    )
    .await
    .map_err(|_| AuthorityError::Unavailable)?;
    Ok(closure)
}

pub async fn read_vnode_closure<W: Blobs>(
    world: &W,
    vnode: VnodeId,
    closure: ProtectedClosureRef,
) -> Result<Vec<u8>, AuthorityError> {
    let bytes = Blobs::read(
        world,
        &layout::vnode_closure_blob(vnode, closure.volume, closure.sequence),
    )
    .await
    .map_err(|_| AuthorityError::Unavailable)?
    .ok_or(AuthorityError::Corrupt)?;
    if usize::try_from(closure.len).ok() != Some(bytes.len()) || crc32c(&bytes) != closure.checksum
    {
        return Err(AuthorityError::Corrupt);
    }
    Ok(bytes)
}

pub async fn adopt_vnode_quorum<W: Store + Blobs + Peers>(
    state: &SharedHost,
    world: &W,
    proof: AuthorityProof,
) -> Result<std::collections::BTreeMap<VolumeId, ProtectedClosureRef>, AuthorityError> {
    VnodeQuorum::begin(state, world)?.adopt(proof).await
}

/// Challenge and revoke the old host session, publish the next vnode
/// generation, and durably adopt it on an intersecting quorum. The returned
/// inventory is the recovery floor; callers must retrieve it before running.
pub async fn failover_vnode<W: Store + Blobs + Peers>(
    state: &SharedHost,
    world: &W,
    vnode: VnodeId,
    failed_primary: HostId,
    nonce: u64,
) -> Result<
    (
        AuthorityProof,
        std::collections::BTreeMap<VolumeId, ProtectedClosureRef>,
    ),
    AuthorityError,
> {
    let quorum = VnodeQuorum::begin(state, world)?;
    let (session, host_epoch, challenge_interval) = {
        let state = state.borrow();
        let policy = state
            .config
            .replica_placement
            .as_ref()
            .and_then(|placement| placement.authority)
            .ok_or(AuthorityError::Invalid)?;
        (
            state.authority.session.ok_or(AuthorityError::Fenced)?,
            state.authority.host_epoch,
            policy.challenge_interval,
        )
    };
    let candidate = quorum.local;
    if candidate == failed_primary || nonce == 0 {
        return Err(AuthorityError::Invalid);
    }
    let challenged = challenge_host_session(
        quorum.world,
        failed_primary,
        candidate,
        nonce,
        blockd_exec::now(),
    )
    .await?;
    blockd_exec::delay(challenge_interval).await;
    revoke_host_session(quorum.world, challenged, nonce).await?;
    let current = read_vnode_authority(quorum.world, &quorum.placement, vnode)
        .await?
        .ok_or(AuthorityError::Fenced)?;
    if current.authority.primary != failed_primary {
        return Err(AuthorityError::Conflict);
    }
    let next = current
        .authority
        .advance(candidate, session, host_epoch)
        .map_err(|_| AuthorityError::Invalid)?;
    let proof = cas_vnode_authority(quorum.world, &quorum.placement, Some(current), next).await?;
    let inventory = quorum.adopt(proof).await?;
    let vnode_members = quorum
        .placement
        .placement(vnode)
        .ok_or(AuthorityError::Invalid)?
        .voting_sets()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    for closure in inventory.values().copied() {
        let mut bytes = read_vnode_closure(quorum.world, vnode, closure).await.ok();
        if bytes.is_none() {
            for member in &vnode_members {
                if *member == candidate {
                    continue;
                }
                bytes = quorum
                    .client
                    .fetch_vnode_closure(quorum.world, *member, vnode, closure)
                    .await;
                if bytes.as_ref().is_some_and(|bytes| {
                    usize::try_from(closure.len).ok() == Some(bytes.len())
                        && crc32c(bytes) == closure.checksum
                }) {
                    break;
                }
                bytes = None;
            }
        }
        let bytes = bytes.ok_or(AuthorityError::Corrupt)?;
        commit_vnode_closure(
            quorum.world,
            &quorum.placement,
            proof.authority,
            closure.volume,
            closure.sequence,
            bytes,
        )
        .await?;
    }
    quorum
        .state
        .borrow_mut()
        .authority
        .active_vnodes
        .insert(vnode, proof);
    Ok((proof, inventory))
}

pub async fn claim_vnode_authority<W: Store + Blobs + Peers>(
    state: &SharedHost,
    world: &W,
    vnode: VnodeId,
) -> Result<AuthorityProof, AuthorityError> {
    let quorum = VnodeQuorum::begin(state, world)?;
    let (primary_session, primary_host_epoch) = {
        let state = state.borrow();
        (
            state.authority.session.ok_or(AuthorityError::Fenced)?,
            state.authority.host_epoch,
        )
    };
    if read_vnode_authority(quorum.world, &quorum.placement, vnode)
        .await?
        .is_some()
    {
        return Err(AuthorityError::Conflict);
    }
    let authority = VnodeAuthority {
        cluster_id: quorum.placement.cluster_id,
        placement_epoch: quorum.placement.epoch,
        vnode,
        generation: 1,
        primary: quorum.local,
        primary_session,
        primary_host_epoch,
    };
    let proof = cas_vnode_authority(quorum.world, &quorum.placement, None, authority).await?;
    let inventory = quorum.adopt(proof).await?;
    if !inventory.is_empty() {
        return Err(AuthorityError::Corrupt);
    }
    quorum
        .state
        .borrow_mut()
        .authority
        .active_vnodes
        .insert(vnode, proof);
    Ok(proof)
}

/// Durably commit a complete recovery closure on two members of every active
/// voting set. This is the latency-sensitive path and deliberately has no
/// [`Store`] bound.
pub async fn commit_active_vnode_quorum<W: Blobs + Peers>(
    state: &SharedHost,
    world: &W,
    volume: VolumeId,
    sequence: u64,
    bytes: Vec<u8>,
) -> Result<ProtectedClosureRef, AuthorityError> {
    let quorum = VnodeQuorum::begin(state, world)?;
    let proof = {
        let state = state.borrow();
        let vnode = quorum.placement.vnode(volume);
        *state
            .authority
            .active_vnodes
            .get(&vnode)
            .ok_or(AuthorityError::Fenced)?
    };
    let expected = closure_ref(volume, sequence, &bytes).ok_or(AuthorityError::Invalid)?;
    let vnode = quorum
        .placement
        .placement(proof.authority.vnode)
        .ok_or(AuthorityError::Invalid)?;
    let members = vnode
        .voting_sets()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    let mut committed = std::collections::BTreeSet::new();
    for member in members {
        let closure = if member == quorum.local {
            commit_vnode_closure(
                quorum.world,
                &quorum.placement,
                proof.authority,
                volume,
                sequence,
                bytes.clone(),
            )
            .await
            .ok()
        } else {
            quorum
                .client
                .commit_vnode_closure(quorum.world, member, proof, volume, sequence, bytes.clone())
                .await
        };
        if closure == Some(expected) {
            committed.insert(member);
        }
        if vnode.voting_sets().all(|set| {
            set.into_iter()
                .filter(|member| committed.contains(member))
                .count()
                >= 2
        }) {
            return Ok(expected);
        }
    }
    Err(AuthorityError::Unavailable)
}
