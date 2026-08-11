//! Thread-safe two-lane external event injection.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use crate::TaskId;
use crate::runtime::{Scheduler, WakeSource, current_waiter};

pub const BACKGROUND_SHARE: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Critical,
    Background,
}

struct Waiter {
    task: TaskId,
    scheduler: Weak<Scheduler>,
}

struct State<T> {
    critical: VecDeque<T>,
    background: VecDeque<T>,
    capacity: Option<usize>,
    streak: u32,
    senders: usize,
    receiver_alive: bool,
    waiter: Option<Waiter>,
}

pub struct Injector<T> {
    state: Arc<Mutex<State<T>>>,
}

pub struct Injected<T> {
    state: Arc<Mutex<State<T>>>,
}

pub struct Recv<'a, T> {
    receiver: &'a Injected<T>,
}

pub fn injector<T>() -> (Injector<T>, Injected<T>) {
    injector_with_capacity(None)
}

pub fn bounded_injector<T>(capacity: usize) -> (Injector<T>, Injected<T>) {
    assert!(capacity != 0, "injector capacity must be nonzero");
    injector_with_capacity(Some(capacity))
}

fn injector_with_capacity<T>(capacity: Option<usize>) -> (Injector<T>, Injected<T>) {
    let state = Arc::new(Mutex::new(State {
        critical: VecDeque::new(),
        background: VecDeque::new(),
        capacity,
        streak: 0,
        senders: 1,
        receiver_alive: true,
        waiter: None,
    }));
    (
        Injector {
            state: Arc::clone(&state),
        },
        Injected { state },
    )
}

impl<T> Injector<T> {
    pub fn push(&self, lane: Lane, value: T) -> Result<(), T> {
        let waiter = {
            let mut state = self.state.lock().expect("injector mutex poisoned");
            if !state.receiver_alive {
                return Err(value);
            }
            if state
                .capacity
                .is_some_and(|capacity| state.critical.len() + state.background.len() >= capacity)
            {
                return Err(value);
            }
            match lane {
                Lane::Critical => state.critical.push_back(value),
                Lane::Background => state.background.push_back(value),
            }
            state.waiter.take()
        };
        if let Some(waiter) = waiter.and_then(|waiter| {
            waiter
                .scheduler
                .upgrade()
                .map(|scheduler| (waiter.task, scheduler))
        }) {
            waiter.1.schedule(waiter.0, WakeSource::External);
        }
        Ok(())
    }

    /// Current external backlog by priority lane. This is observational
    /// only; scheduling still applies the fairness valve when values are
    /// received.
    pub fn depths(&self) -> (usize, usize) {
        let state = self.state.lock().expect("injector mutex poisoned");
        (state.critical.len(), state.background.len())
    }
}

impl<T> Clone for Injector<T> {
    fn clone(&self) -> Self {
        self.state.lock().expect("injector mutex poisoned").senders += 1;
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Drop for Injector<T> {
    fn drop(&mut self) {
        let waiter = {
            let mut state = self.state.lock().expect("injector mutex poisoned");
            state.senders -= 1;
            (state.senders == 0).then(|| state.waiter.take()).flatten()
        };
        if let Some(waiter) = waiter.and_then(|waiter| {
            waiter
                .scheduler
                .upgrade()
                .map(|scheduler| (waiter.task, scheduler))
        }) {
            waiter.1.schedule(waiter.0, WakeSource::External);
        }
    }
}

impl<T> Injected<T> {
    pub fn recv(&self) -> Recv<'_, T> {
        Recv { receiver: self }
    }

    pub fn try_recv(&self) -> Option<T> {
        let mut state = self.state.lock().expect("injector mutex poisoned");
        let use_background = !state.background.is_empty()
            && (state.critical.is_empty() || state.streak >= BACKGROUND_SHARE);
        if use_background {
            state.streak = 0;
            state.background.pop_front()
        } else {
            let value = state.critical.pop_front();
            if value.is_some() {
                state.streak = state.streak.saturating_add(1);
            }
            value
        }
    }
}

impl<T> Drop for Injected<T> {
    fn drop(&mut self) {
        self.state
            .lock()
            .expect("injector mutex poisoned")
            .receiver_alive = false;
    }
}

impl<T> Future for Recv<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let current = current_waiter();
        let mut state = self.receiver.state.lock().expect("injector mutex poisoned");
        let use_background = !state.background.is_empty()
            && (state.critical.is_empty() || state.streak >= BACKGROUND_SHARE);
        let value = if use_background {
            state.streak = 0;
            state.background.pop_front()
        } else {
            let value = state.critical.pop_front();
            if value.is_some() {
                state.streak = state.streak.saturating_add(1);
            }
            value
        };
        if let Some(value) = value {
            Poll::Ready(Some(value))
        } else if state.senders == 0 {
            Poll::Ready(None)
        } else {
            state.waiter = Some(Waiter {
                task: current.task,
                scheduler: Arc::downgrade(&current.scheduler),
            });
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Executor;

    use super::{BACKGROUND_SHARE, Lane, bounded_injector, injector};

    #[test]
    fn background_lane_cannot_starve() {
        let (sender, receiver) = injector();
        for value in 0..(BACKGROUND_SHARE + 2) {
            sender.push(Lane::Critical, value).unwrap();
        }
        sender.push(Lane::Background, u32::MAX).unwrap();

        let mut executor = Executor::production();
        let values = executor.block_on(async move {
            let mut values = Vec::new();
            for _ in 0..=BACKGROUND_SHARE {
                values.push(receiver.recv().await.unwrap());
            }
            values
        });
        assert_eq!(values[BACKGROUND_SHARE as usize], u32::MAX);
    }

    #[test]
    fn bounded_injector_rejects_excess_backlog_and_recovers_capacity() {
        let (sender, stream) = bounded_injector(2);
        assert_eq!(sender.push(Lane::Critical, 1), Ok(()));
        assert_eq!(sender.push(Lane::Background, 2), Ok(()));
        assert_eq!(sender.push(Lane::Critical, 3), Err(3));

        let mut executor = Executor::production();
        let task_sender = sender.clone();
        let (item, pushed, depths) = executor.block_on(async move {
            let item = stream.recv().await;
            let pushed = task_sender.push(Lane::Critical, 3);
            (item, pushed, task_sender.depths())
        });
        assert_eq!(item, Some(1));
        assert_eq!(pushed, Ok(()));
        assert_eq!(depths, (1, 1));
    }
}
