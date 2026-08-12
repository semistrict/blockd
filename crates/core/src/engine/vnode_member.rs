use crate::authority::{AuthorityProof, PlacementRecord, VnodeAuthority, VnodeId};
use crate::format::crc32c;
use crate::layout;
use crate::types::{HostId, VsetId};
use crate::vnode_member::{
    AdoptionReceipt, ProtectedClosureRef, VnodeMemberRecord, adoption_quorum, closure_ref,
};
use crate::world::{Blobs, Peers, Store};

use super::{
    AuthorityError, SharedHost, cas_vnode_authority, challenge_host_session, read_vnode_authority,
    revoke_host_session, verify_authority_proof,
};

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
    vset: VsetId,
    sequence: u64,
    bytes: Vec<u8>,
) -> Result<ProtectedClosureRef, AuthorityError> {
    let closure = closure_ref(vset, sequence, &bytes).ok_or(AuthorityError::Invalid)?;
    let mut record = read_vnode_member(world, placement, authority.vnode)
        .await?
        .ok_or(AuthorityError::Fenced)?;
    if let Some(existing) = record.closure(vset) {
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
        layout::vnode_closure_blob(authority.vnode, vset, sequence),
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
        &layout::vnode_closure_blob(vnode, closure.vset, closure.sequence),
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
) -> Result<std::collections::BTreeMap<VsetId, ProtectedClosureRef>, AuthorityError> {
    let (placement, local, retry) = {
        let state = state.borrow();
        (
            state
                .authority_placement
                .clone()
                .ok_or(AuthorityError::Invalid)?,
            state.config.host,
            state.config.backup_retry,
        )
    };
    proof
        .authority
        .validate(&placement)
        .map_err(|_| AuthorityError::Invalid)?;
    let vnode = placement
        .placement(proof.authority.vnode)
        .ok_or(AuthorityError::Invalid)?;
    let members = vnode
        .voting_sets()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    let client = state.borrow().peer_client.clone();
    let mut receipts = Vec::new();
    for member in members {
        let receipt = if member == local {
            adopt_vnode_generation(world, &placement, member, proof)
                .await
                .ok()
        } else {
            client
                .adopt_vnode(world, member, proof, retry)
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
            if let Ok(inventory) = adoption_quorum(&placement, proof, &receipts) {
                state.borrow_mut().counters.vnode_adoptions += 1;
                return Ok(inventory);
            }
        }
    }
    Err(AuthorityError::Unavailable)
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
        std::collections::BTreeMap<VsetId, ProtectedClosureRef>,
    ),
    AuthorityError,
> {
    let (candidate, session, host_epoch, challenge_interval, placement) = {
        let state = state.borrow();
        let policy = state
            .config
            .replica_placement
            .as_ref()
            .and_then(|placement| placement.authority)
            .ok_or(AuthorityError::Invalid)?;
        (
            state.config.host,
            state.authority_session.ok_or(AuthorityError::Fenced)?,
            state.authority_host_epoch,
            policy.challenge_interval,
            state
                .authority_placement
                .clone()
                .ok_or(AuthorityError::Invalid)?,
        )
    };
    if candidate == failed_primary || nonce == 0 {
        return Err(AuthorityError::Invalid);
    }
    let challenged =
        challenge_host_session(world, failed_primary, candidate, nonce, blockd_exec::now()).await?;
    blockd_exec::delay(challenge_interval).await;
    revoke_host_session(world, challenged, nonce).await?;
    let current = read_vnode_authority(world, &placement, vnode)
        .await?
        .ok_or(AuthorityError::Fenced)?;
    if current.authority.primary != failed_primary {
        return Err(AuthorityError::Conflict);
    }
    let next = current
        .authority
        .advance(candidate, session, host_epoch)
        .map_err(|_| AuthorityError::Invalid)?;
    let proof = cas_vnode_authority(world, &placement, Some(current), next).await?;
    let inventory = adopt_vnode_quorum(state, world, proof).await?;
    let vnode_members = placement
        .placement(vnode)
        .ok_or(AuthorityError::Invalid)?
        .voting_sets()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    let client = state.borrow().peer_client.clone();
    let retry = state.borrow().config.backup_retry;
    for closure in inventory.values().copied() {
        let mut bytes = read_vnode_closure(world, vnode, closure).await.ok();
        if bytes.is_none() {
            for member in &vnode_members {
                if *member == candidate {
                    continue;
                }
                bytes = client
                    .fetch_vnode_closure(world, *member, vnode, closure, retry)
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
            world,
            &placement,
            proof.authority,
            closure.vset,
            closure.sequence,
            bytes,
        )
        .await?;
    }
    state.borrow_mut().active_vnodes.insert(vnode, proof);
    Ok((proof, inventory))
}

pub async fn claim_vnode_authority<W: Store + Blobs + Peers>(
    state: &SharedHost,
    world: &W,
    vnode: VnodeId,
) -> Result<AuthorityProof, AuthorityError> {
    let (placement, primary, primary_session, primary_host_epoch) = {
        let state = state.borrow();
        (
            state
                .authority_placement
                .clone()
                .ok_or(AuthorityError::Invalid)?,
            state.config.host,
            state.authority_session.ok_or(AuthorityError::Fenced)?,
            state.authority_host_epoch,
        )
    };
    if read_vnode_authority(world, &placement, vnode)
        .await?
        .is_some()
    {
        return Err(AuthorityError::Conflict);
    }
    let authority = VnodeAuthority {
        cluster_id: placement.cluster_id,
        placement_epoch: placement.epoch,
        vnode,
        generation: 1,
        primary,
        primary_session,
        primary_host_epoch,
    };
    let proof = cas_vnode_authority(world, &placement, None, authority).await?;
    let inventory = adopt_vnode_quorum(state, world, proof).await?;
    if !inventory.is_empty() {
        return Err(AuthorityError::Corrupt);
    }
    state.borrow_mut().active_vnodes.insert(vnode, proof);
    Ok(proof)
}

/// Durably commit a complete recovery closure on two members of every active
/// voting set. This is the latency-sensitive path and deliberately has no
/// [`Store`] bound.
pub async fn commit_active_vnode_quorum<W: Blobs + Peers>(
    state: &SharedHost,
    world: &W,
    vset: VsetId,
    sequence: u64,
    bytes: Vec<u8>,
) -> Result<ProtectedClosureRef, AuthorityError> {
    let (placement, proof, local, retry) = {
        let state = state.borrow();
        let placement = state
            .authority_placement
            .clone()
            .ok_or(AuthorityError::Invalid)?;
        let vnode = placement.vnode(vset);
        (
            placement,
            *state
                .active_vnodes
                .get(&vnode)
                .ok_or(AuthorityError::Fenced)?,
            state.config.host,
            state.config.backup_retry,
        )
    };
    let expected = closure_ref(vset, sequence, &bytes).ok_or(AuthorityError::Invalid)?;
    let vnode = placement
        .placement(proof.authority.vnode)
        .ok_or(AuthorityError::Invalid)?;
    let members = vnode
        .voting_sets()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    let client = state.borrow().peer_client.clone();
    let mut committed = std::collections::BTreeSet::new();
    for member in members {
        let closure = if member == local {
            commit_vnode_closure(
                world,
                &placement,
                proof.authority,
                vset,
                sequence,
                bytes.clone(),
            )
            .await
            .ok()
        } else {
            client
                .commit_vnode_closure(world, member, proof, vset, sequence, bytes.clone(), retry)
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
