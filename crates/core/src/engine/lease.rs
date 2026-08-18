use std::rc::Rc;

use blockd_exec::{delay, now, random_u64};

use crate::authority::HostSessionRecord;
use crate::hostmeta::AuthorityHostConfig;
use crate::world::Store;

use super::authority::{
    AuthorityError, PollSession, activate_host_session, create_host_session,
    poll_or_defend_host_session, read_host_session, read_placement,
};
use super::state::SharedHost;

fn config(state: &SharedHost) -> Option<AuthorityHostConfig> {
    state
        .borrow()
        .config
        .replica_placement
        .as_ref()
        .and_then(|placement| placement.authority)
}

pub(super) async fn bootstrap_host_authority<W: Store>(
    state: &SharedHost,
    world: &W,
) -> Result<(), AuthorityError> {
    let Some(config) = config(state) else {
        return Ok(());
    };
    if config.cluster_id == 0
        || config.poll_interval == 0
        || config.max_poll_staleness < config.poll_interval
        || config.challenge_interval <= config.max_poll_staleness
    {
        return Err(AuthorityError::Invalid);
    }
    let session = random_u64().max(1);
    loop {
        let placement = match read_placement(world).await {
            Ok(Some(proof)) if proof.placement.cluster_id == config.cluster_id => proof.placement,
            Ok(Some(_)) => return Err(AuthorityError::Invalid),
            Ok(None) => return Err(AuthorityError::Fenced),
            Err(AuthorityError::Unavailable) => {
                delay(config.poll_interval).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let host = state.borrow().config.host;
        let versioned = match read_host_session(world, host).await {
            Ok(None) => match create_host_session(world, host, session).await {
                Ok(created) => created,
                Err(AuthorityError::Conflict | AuthorityError::Unavailable) => {
                    delay(config.poll_interval).await;
                    continue;
                }
                Err(error) => return Err(error),
            },
            Ok(Some(
                active @ super::authority::VersionedSession {
                    record: HostSessionRecord::Active { session: found, .. },
                    ..
                },
            )) if found == session => active,
            Ok(Some(
                revoked @ super::authority::VersionedSession {
                    record: HostSessionRecord::Revoked { .. },
                    ..
                },
            )) => match activate_host_session(world, revoked, session).await {
                Ok(active) => active,
                Err(AuthorityError::Conflict | AuthorityError::Unavailable) => {
                    delay(config.poll_interval).await;
                    continue;
                }
                Err(error) => return Err(error),
            },
            Ok(Some(_)) => return Err(AuthorityError::Fenced),
            Err(AuthorityError::Unavailable) => {
                delay(config.poll_interval).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let mut host = state.borrow_mut();
        host.authority.session = Some(session);
        host.authority.host_epoch = versioned.record.epoch();
        host.authority.serving = true;
        host.authority.last_poll = now();
        host.authority.placement = Some(placement);
        return Ok(());
    }
}

pub(super) async fn host_session_monitor<W: Store + 'static>(state: SharedHost, world: Rc<W>) {
    let Some(config) = config(&state) else {
        return;
    };
    loop {
        delay(config.poll_interval).await;
        let (host, session, last_success) = {
            let host = state.borrow();
            (
                host.config.host,
                host.authority.session,
                host.authority.last_poll,
            )
        };
        let Some(session) = session else {
            state.borrow_mut().fail("host authority session missing");
            return;
        };
        state.borrow_mut().counters.lease_gets += 1;
        match read_host_session(world.as_ref(), host).await {
            Ok(Some(
                observed @ super::authority::VersionedSession {
                    record: HostSessionRecord::Active { session: found, .. },
                    ..
                },
            )) if found == session => {
                let mut state = state.borrow_mut();
                state.authority.last_poll = now();
                state.authority.host_epoch = observed.record.epoch();
                state.authority.serving = true;
            }
            Ok(Some(super::authority::VersionedSession {
                record: HostSessionRecord::Challenge { session: found, .. },
                ..
            })) if found == session => {
                {
                    let mut state = state.borrow_mut();
                    state.authority.last_poll = now();
                    state.authority.serving = false;
                    state.counters.lease_challenges += 1;
                }
                match poll_or_defend_host_session(world.as_ref(), host, session).await {
                    Ok(PollSession::Defended(defended) | PollSession::Active(defended)) => {
                        let mut state = state.borrow_mut();
                        state.authority.last_poll = now();
                        state.authority.host_epoch = defended.record.epoch();
                        state.authority.serving = true;
                        state.counters.lease_defenses += 1;
                    }
                    Err(AuthorityError::Unavailable) => {}
                    Err(_) => {
                        self_fence(&state);
                        return;
                    }
                }
            }
            Err(AuthorityError::Unavailable)
                if now().saturating_sub(last_success) < config.max_poll_staleness => {}
            Ok(_) | Err(_) => {
                self_fence(&state);
                return;
            }
        }
    }
}

fn self_fence(state: &SharedHost) {
    let mut state = state.borrow_mut();
    state.authority.serving = false;
    state.counters.lease_self_fences += 1;
    state.fail("host session fenced");
}
