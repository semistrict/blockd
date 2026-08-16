//! Thread-safe, two-lane Tokio event injection.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const BACKGROUND_SHARE: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Critical,
    Background,
}

struct Depths {
    capacity: Option<usize>,
    total: AtomicUsize,
    critical: AtomicUsize,
    background: AtomicUsize,
}

struct Receivers<T> {
    critical: tokio::sync::mpsc::UnboundedReceiver<T>,
    background: tokio::sync::mpsc::UnboundedReceiver<T>,
    critical_closed: bool,
    background_closed: bool,
    streak: u32,
}

pub struct Injector<T> {
    critical: tokio::sync::mpsc::UnboundedSender<T>,
    background: tokio::sync::mpsc::UnboundedSender<T>,
    depths: Arc<Depths>,
}

pub struct Injected<T> {
    receivers: tokio::sync::Mutex<Receivers<T>>,
    depths: Arc<Depths>,
}

pub fn injector<T>() -> (Injector<T>, Injected<T>) {
    injector_with_capacity(None)
}

pub fn bounded_injector<T>(capacity: usize) -> (Injector<T>, Injected<T>) {
    assert!(capacity != 0, "injector capacity must be nonzero");
    injector_with_capacity(Some(capacity))
}

fn injector_with_capacity<T>(capacity: Option<usize>) -> (Injector<T>, Injected<T>) {
    let (critical, critical_rx) = tokio::sync::mpsc::unbounded_channel();
    let (background, background_rx) = tokio::sync::mpsc::unbounded_channel();
    let depths = Arc::new(Depths {
        capacity,
        total: AtomicUsize::new(0),
        critical: AtomicUsize::new(0),
        background: AtomicUsize::new(0),
    });
    (
        Injector {
            critical,
            background,
            depths: Arc::clone(&depths),
        },
        Injected {
            receivers: tokio::sync::Mutex::new(Receivers {
                critical: critical_rx,
                background: background_rx,
                critical_closed: false,
                background_closed: false,
                streak: 0,
            }),
            depths,
        },
    )
}

impl Depths {
    fn reserve(&self) -> bool {
        let mut current = self.total.load(Ordering::Acquire);
        loop {
            if self.capacity.is_some_and(|capacity| current >= capacity) {
                return false;
            }
            match self.total.compare_exchange_weak(
                current,
                current.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn sent(&self, lane: Lane) {
        self.lane(lane).fetch_add(1, Ordering::Release);
    }

    fn rollback(&self, lane: Lane) {
        self.lane(lane).fetch_sub(1, Ordering::AcqRel);
        self.total.fetch_sub(1, Ordering::AcqRel);
    }

    fn received(&self, lane: Lane) {
        self.lane(lane).fetch_sub(1, Ordering::AcqRel);
        self.total.fetch_sub(1, Ordering::AcqRel);
    }

    fn lane(&self, lane: Lane) -> &AtomicUsize {
        match lane {
            Lane::Critical => &self.critical,
            Lane::Background => &self.background,
        }
    }
}

impl<T> Injector<T> {
    pub fn push(&self, lane: Lane, value: T) -> Result<(), T> {
        if !self.depths.reserve() {
            return Err(value);
        }
        self.depths.sent(lane);
        let result = match lane {
            Lane::Critical => self.critical.send(value),
            Lane::Background => self.background.send(value),
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.depths.rollback(lane);
                Err(error.0)
            }
        }
    }

    pub fn depths(&self) -> (usize, usize) {
        (
            self.depths.critical.load(Ordering::Acquire),
            self.depths.background.load(Ordering::Acquire),
        )
    }
}

impl<T> Clone for Injector<T> {
    fn clone(&self) -> Self {
        Self {
            critical: self.critical.clone(),
            background: self.background.clone(),
            depths: Arc::clone(&self.depths),
        }
    }
}

impl<T> Injected<T> {
    pub async fn recv(&self) -> Option<T> {
        let mut receivers = self.receivers.lock().await;
        loop {
            let prefer_background = receivers.streak >= BACKGROUND_SHARE;
            let immediate = if prefer_background {
                receivers
                    .background
                    .try_recv()
                    .ok()
                    .map(|value| (Lane::Background, value))
                    .or_else(|| {
                        receivers
                            .critical
                            .try_recv()
                            .ok()
                            .map(|value| (Lane::Critical, value))
                    })
            } else {
                receivers
                    .critical
                    .try_recv()
                    .ok()
                    .map(|value| (Lane::Critical, value))
                    .or_else(|| {
                        receivers
                            .background
                            .try_recv()
                            .ok()
                            .map(|value| (Lane::Background, value))
                    })
            };
            if let Some((lane, value)) = immediate {
                Self::received(&mut receivers, lane);
                self.depths.received(lane);
                return Some(value);
            }
            if receivers.critical_closed && receivers.background_closed {
                return None;
            }

            let critical_open = !receivers.critical_closed;
            let background_open = !receivers.background_closed;
            let Receivers {
                critical,
                background,
                ..
            } = &mut *receivers;
            let received = if prefer_background {
                tokio::select! {
                    biased;
                    value = background.recv(), if background_open => {
                        (Lane::Background, value)
                    }
                    value = critical.recv(), if critical_open => {
                        (Lane::Critical, value)
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    value = critical.recv(), if critical_open => {
                        (Lane::Critical, value)
                    }
                    value = background.recv(), if background_open => {
                        (Lane::Background, value)
                    }
                }
            };
            match received {
                (lane, Some(value)) => {
                    Self::received(&mut receivers, lane);
                    self.depths.received(lane);
                    return Some(value);
                }
                (Lane::Critical, None) => receivers.critical_closed = true,
                (Lane::Background, None) => receivers.background_closed = true,
            }
        }
    }

    fn received(receivers: &mut Receivers<T>, lane: Lane) {
        match lane {
            Lane::Critical => receivers.streak = receivers.streak.saturating_add(1),
            Lane::Background => receivers.streak = 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ProductionContext;

    use super::{BACKGROUND_SHARE, Lane, bounded_injector, injector};

    #[tokio::test]
    async fn background_lane_cannot_starve() {
        let (sender, receiver) = injector();
        for value in 0..(BACKGROUND_SHARE + 2) {
            sender.push(Lane::Critical, value).unwrap();
        }
        sender.push(Lane::Background, u32::MAX).unwrap();

        let values = ProductionContext::new(|_| {})
            .scope(async move {
                let mut values = Vec::new();
                for _ in 0..=BACKGROUND_SHARE {
                    values.push(receiver.recv().await.unwrap());
                }
                values
            })
            .await;
        assert_eq!(values[BACKGROUND_SHARE as usize], u32::MAX);
    }

    #[tokio::test]
    async fn bounded_injector_rejects_excess_backlog_and_recovers_capacity() {
        let (sender, stream) = bounded_injector(2);
        assert_eq!(sender.push(Lane::Critical, 1), Ok(()));
        assert_eq!(sender.push(Lane::Background, 2), Ok(()));
        assert_eq!(sender.push(Lane::Critical, 3), Err(3));

        let task_sender = sender.clone();
        let (item, pushed, depths) = ProductionContext::new(|_| {})
            .scope(async move {
                let item = stream.recv().await;
                let pushed = task_sender.push(Lane::Critical, 3);
                (item, pushed, task_sender.depths())
            })
            .await;
        assert_eq!(item, Some(1));
        assert_eq!(pushed, Ok(()));
        assert_eq!(depths, (1, 1));
    }
}
