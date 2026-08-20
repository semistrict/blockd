use std::rc::Rc;

use blockd_exec::{Either, delay, now, random_u64, select2};

use crate::authority::HostSessionRecord;
use crate::hostmeta::AuthorityHostConfig;
use crate::world::Store;

use super::authority::{
    AuthorityError, PollSession, activate_host_session, challenge_host_session,
    create_host_session, poll_or_defend_host_session, read_host_session, read_placement,
    revoke_host_session,
};
use super::state::SharedHost;

fn config(state: &SharedHost) -> Option<AuthorityHostConfig> {
    state
        .borrow()
        .config
        .cluster_placement
        .as_ref()
        .and_then(|placement| placement.authority)
}

#[allow(clippy::too_many_lines)]
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
            Ok(Some(super::authority::VersionedSession {
                record: HostSessionRecord::Active { .. },
                ..
            })) => {
                let nonce = random_u64().max(1);
                let challenged = match challenge_host_session(world, host, nonce, now()).await {
                    Ok(challenged) => challenged,
                    Err(AuthorityError::Conflict | AuthorityError::Unavailable) => {
                        delay(config.poll_interval).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let HostSessionRecord::Challenge {
                    session: challenged_session,
                    challenged_at,
                    nonce: challenged_nonce,
                    ..
                } = challenged.record
                else {
                    delay(config.poll_interval).await;
                    continue;
                };
                delay(
                    challenged_at
                        .saturating_add(config.challenge_interval)
                        .saturating_sub(now()),
                )
                .await;
                let observed = match read_host_session(world, host).await {
                    Ok(Some(observed)) => observed,
                    Ok(None) | Err(AuthorityError::Conflict | AuthorityError::Unavailable) => {
                        delay(config.poll_interval).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if !matches!(
                    observed.record,
                    HostSessionRecord::Challenge {
                        session: found,
                        nonce: found_nonce,
                        ..
                    } if found == challenged_session
                        && found_nonce == challenged_nonce
                ) {
                    continue;
                }
                let revoked = match revoke_host_session(world, observed, challenged_nonce).await {
                    Ok(revoked) => revoked,
                    Err(AuthorityError::Conflict | AuthorityError::Unavailable) => continue,
                    Err(error) => return Err(error),
                };
                match activate_host_session(world, revoked, session).await {
                    Ok(active) => active,
                    Err(AuthorityError::Conflict | AuthorityError::Unavailable) => continue,
                    Err(error) => return Err(error),
                }
            }
            Ok(Some(super::authority::VersionedSession {
                record: HostSessionRecord::Challenge { .. },
                ..
            }))
            | Err(AuthorityError::Unavailable) => {
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
        let remaining = last_success
            .saturating_add(config.max_poll_staleness)
            .saturating_sub(now());
        if remaining == 0 {
            self_fence(&state);
            return;
        }
        let observed =
            match select2(read_host_session(world.as_ref(), host), delay(remaining)).await {
                Either::First(observed) => observed,
                Either::Second(()) => {
                    self_fence(&state);
                    return;
                }
            };
        match observed {
            Ok(Some(
                observed @ super::authority::VersionedSession {
                    record: HostSessionRecord::Active { session: found, .. },
                    ..
                },
            )) if found == session => {
                let Ok(Some(placement)) = read_placement(world.as_ref()).await else {
                    continue;
                };
                if placement.placement.cluster_id != config.cluster_id {
                    state
                        .borrow_mut()
                        .fail("host authority placement belongs to another cluster");
                    return;
                }
                let mut state = state.borrow_mut();
                state.authority.last_poll = now();
                state.authority.host_epoch = observed.record.epoch();
                state.authority.serving = true;
                state.authority.placement = Some(placement.placement);
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

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use blockd_exec::{FaultConfig, delay, simulation_scope, spawn};

    use super::*;
    use crate::engine::HostState;
    use crate::hostmeta::{ArchivePolicy, ClusterPlacementConfig, HostConfig};
    use crate::placement::ClusterPlacement;
    use crate::protocol::StoreFault;
    use crate::types::HostId;
    use crate::world::{Store, StoreError};

    #[derive(Default)]
    struct LeaseStore {
        objects: RefCell<BTreeMap<String, (u64, Vec<u8>)>>,
        next: Cell<u64>,
        get_delay: Cell<u64>,
        unavailable: Cell<bool>,
    }

    impl Store for LeaseStore {
        async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
            if self.unavailable.get() {
                return Err(StoreError::Fault(StoreFault::Unavailable));
            }
            let version = self.next.get().saturating_add(1);
            self.next.set(version);
            self.objects.borrow_mut().insert(key, (version, bytes));
            Ok(version)
        }

        async fn put_cas(
            &self,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, StoreError> {
            if self.unavailable.get() {
                return Err(StoreError::Fault(StoreFault::Unavailable));
            }
            let actual = self.objects.borrow().get(&key).map(|(version, _)| *version);
            if actual != expected {
                return Err(StoreError::Fault(StoreFault::CasConflict { actual }));
            }
            self.put(key, bytes).await
        }

        async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            if self.get_delay.get() != 0 {
                delay(self.get_delay.get()).await;
            }
            if self.unavailable.get() {
                return Err(StoreError::Fault(StoreFault::Unavailable));
            }
            Ok(self.objects.borrow().get(key).cloned())
        }

        async fn get_range(
            &self,
            key: &str,
            offset: u64,
            len: u64,
        ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            let found = self.get(key).await?;
            Ok(found.map(|(version, bytes)| {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                let end = start
                    .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
                    .min(bytes.len());
                (version, bytes[start..end].to_vec())
            }))
        }

        async fn delete(&self, key: &str) -> Result<bool, StoreError> {
            Ok(self.objects.borrow_mut().remove(key).is_some())
        }

        async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            Ok(self
                .objects
                .borrow()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    fn config() -> HostConfig {
        let host = HostId::new(1);
        HostConfig {
            archive: ArchivePolicy::default(),
            host,
            cache_pages: 4,
            writeback_interval: 1,
            backup_retry: 1,
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 0,
            cluster_placement: Some(ClusterPlacementConfig {
                membership_epoch: 1,
                roster: [1, 2, 3].into_iter().map(HostId::new).collect(),
                authority: Some(AuthorityHostConfig {
                    cluster_id: 7,
                    poll_interval: 2,
                    max_poll_staleness: 10,
                    challenge_interval: 20,
                }),
            }),
        }
    }

    fn seed(store: &LeaseStore, session: u64) {
        let placement =
            ClusterPlacement::new(7, 1, vec![HostId::new(1), HostId::new(2), HostId::new(3)])
                .expect("placement");
        store
            .objects
            .borrow_mut()
            .insert(crate::layout::placement_key(), (1, placement.encode()));
        store.objects.borrow_mut().insert(
            crate::layout::host_session_key(HostId::new(1)),
            (2, HostSessionRecord::initial(session).unwrap().encode()),
        );
        store.next.set(2);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_session_read_self_fences_at_the_staleness_deadline() {
        tokio::task::LocalSet::new()
            .run_until(simulation_scope(81, FaultConfig::default(), async {
                let store = Rc::new(LeaseStore::default());
                seed(&store, 44);
                store.get_delay.set(50);
                let state = Rc::new(RefCell::new(HostState::new(config())));
                {
                    let mut state = state.borrow_mut();
                    state.authority.session = Some(44);
                    state.authority.serving = true;
                    state.authority.last_poll = 0;
                }
                let monitor = spawn(host_session_monitor(Rc::clone(&state), store));
                blockd_exec::advance_to(11).await;
                assert!(!state.borrow().authority.serving);
                assert_eq!(state.borrow().counters.lease_self_fences, 1);
                drop(monitor);
            }))
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn rapid_restart_reclaims_an_undefended_prior_session_after_the_challenge_bound() {
        tokio::task::LocalSet::new()
            .run_until(simulation_scope(82, FaultConfig::default(), async {
                let store = Rc::new(LeaseStore::default());
                seed(&store, 44);
                let state = Rc::new(RefCell::new(HostState::new(config())));
                let task_state = Rc::clone(&state);
                let task_store = Rc::clone(&store);
                let takeover = spawn(async move {
                    bootstrap_host_authority(&task_state, task_store.as_ref()).await
                });
                blockd_exec::advance_to(25).await;
                assert_eq!(takeover.await, Ok(Ok(())));
                assert!(state.borrow().authority.serving);
                assert_ne!(state.borrow().authority.session, Some(44));
                assert_eq!(state.borrow().authority.host_epoch, 2);
            }))
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn store_partition_self_fences_a_serving_session() {
        tokio::task::LocalSet::new()
            .run_until(simulation_scope(83, FaultConfig::default(), async {
                let store = Rc::new(LeaseStore::default());
                seed(&store, 44);
                let state = Rc::new(RefCell::new(HostState::new(config())));
                {
                    let mut state = state.borrow_mut();
                    state.authority.session = Some(44);
                    state.authority.serving = true;
                    state.authority.last_poll = 0;
                }
                store.unavailable.set(true);
                let monitor = spawn(host_session_monitor(Rc::clone(&state), store));
                blockd_exec::advance_to(11).await;
                assert!(!state.borrow().authority.serving);
                drop(monitor);
            }))
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn resumed_process_rejects_work_before_the_lease_monitor_runs() {
        tokio::task::LocalSet::new()
            .run_until(simulation_scope(84, FaultConfig::default(), async {
                let mut state = HostState::new(config());
                state.authority.session = Some(44);
                state.authority.serving = true;
                state.authority.last_poll = 0;
                let placement = ClusterPlacement::new(
                    7,
                    1,
                    vec![HostId::new(1), HostId::new(2), HostId::new(3)],
                )
                .expect("placement");
                state.authority.placement = Some(placement);
                assert!(state.volume_authorized(crate::types::VolumeId(0)));
                // No monitor task is spawned while time advances: this models a
                // process that was descheduled or stopped by the OS. Admission
                // must re-check monotonic lease age synchronously on resume.
                blockd_exec::advance_to(10).await;
                assert!(!state.volume_authorized(crate::types::VolumeId(0)));
            }))
            .await;
    }
}
