use std::collections::{BTreeSet, VecDeque};

use blockd_core::protocol::{AdminResult, ReqId};
use blockd_core::types::VolumeId;
use blockd_exec::channel::unbounded;
use blockd_exec::{Either, Response, TaskSet, delay, now, select2};

pub const CONCURRENCY: usize = 32;

pub async fn run(
    interval: u64,
    horizon: u64,
    first_request: u64,
    mut candidates: impl FnMut() -> Vec<VolumeId>,
    mut start: impl FnMut(VolumeId, ReqId) -> Option<Response<AdminResult>>,
) {
    let mut req = first_request;
    let mut actors = TaskSet::new();
    let (completed, mut completions) = unbounded();
    let mut active = BTreeSet::new();
    let mut queued = BTreeSet::new();
    let mut pending = VecDeque::new();
    let interval = interval.max(1);
    let mut next_cadence = now().saturating_add(interval);
    loop {
        if now() >= next_cadence {
            if now() > horizon {
                while !active.is_empty() {
                    let Some(volume) = completions.recv().await else {
                        return;
                    };
                    active.remove(&volume);
                }
                return;
            }
            for volume in candidates() {
                if !active.contains(&volume) && queued.insert(volume) {
                    pending.push_back(volume);
                }
            }
            next_cadence = now().saturating_add(interval);
        }
        while active.len() < CONCURRENCY {
            let Some(volume) = pending.pop_front() else {
                break;
            };
            queued.remove(&volume);
            let request = ReqId(req);
            req = req.checked_add(1).expect("checkpoint request overflow");
            let Some(reply) = start(volume, request) else {
                continue;
            };
            assert!(active.insert(volume));
            let completed = completed.clone();
            actors.spawn(async move {
                let _ = reply.await;
                let _ = completed.send(volume);
            });
        }
        match select2(
            completions.recv(),
            delay(next_cadence.saturating_sub(now())),
        )
        .await
        {
            Either::First(Some(volume)) => {
                active.remove(&volume);
            }
            Either::First(None) => return,
            Either::Second(()) => {}
        }
    }
}
