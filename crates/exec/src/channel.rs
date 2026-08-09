//! Deterministic single-threaded promises and channels.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::runtime::{Waiter, WakeSource, current_waiter, wake};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Closed;

struct OneState<T> {
    value: Option<T>,
    sender_alive: bool,
    receiver_alive: bool,
    waiter: Option<Waiter>,
}

pub struct OneSender<T> {
    state: Rc<RefCell<OneState<T>>>,
}

pub struct OneReceiver<T> {
    state: Rc<RefCell<OneState<T>>>,
}

pub fn oneshot<T>() -> (OneSender<T>, OneReceiver<T>) {
    let state = Rc::new(RefCell::new(OneState {
        value: None,
        sender_alive: true,
        receiver_alive: true,
        waiter: None,
    }));
    (
        OneSender {
            state: Rc::clone(&state),
        },
        OneReceiver { state },
    )
}

impl<T> OneSender<T> {
    pub fn send(self, value: T) -> Result<(), T> {
        let waiter = {
            let mut state = self.state.borrow_mut();
            state.sender_alive = false;
            if !state.receiver_alive {
                return Err(value);
            }
            state.value = Some(value);
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            wake(&waiter, WakeSource::Oneshot);
        }
        Ok(())
    }
}

impl<T> Drop for OneSender<T> {
    fn drop(&mut self) {
        let waiter = {
            let mut state = self.state.borrow_mut();
            if !state.sender_alive {
                return;
            }
            state.sender_alive = false;
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            wake(&waiter, WakeSource::Oneshot);
        }
    }
}

impl<T> Future for OneReceiver<T> {
    type Output = Result<T, Closed>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        if let Some(value) = state.value.take() {
            state.receiver_alive = false;
            Poll::Ready(Ok(value))
        } else if !state.sender_alive {
            state.receiver_alive = false;
            Poll::Ready(Err(Closed))
        } else {
            state.waiter = Some(current_waiter());
            Poll::Pending
        }
    }
}

impl<T> Drop for OneReceiver<T> {
    fn drop(&mut self) {
        self.state.borrow_mut().receiver_alive = false;
    }
}

struct ChannelState<T> {
    queue: VecDeque<T>,
    capacity: Option<usize>,
    senders: usize,
    receiver_alive: bool,
    receiver_waiter: Option<Waiter>,
    sender_waiters: VecDeque<Waiter>,
}

pub struct Sender<T> {
    state: Rc<RefCell<ChannelState<T>>>,
}

pub struct UnboundedSender<T> {
    inner: Sender<T>,
}

pub struct Receiver<T> {
    state: Rc<RefCell<ChannelState<T>>>,
}

pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
    waiter: Option<Waiter>,
}

impl<T> Unpin for Send<'_, T> {}

pub struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Closed,
}

pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "bounded channel capacity must be positive");
    channel(Some(capacity))
}

pub fn unbounded<T>() -> (UnboundedSender<T>, Receiver<T>) {
    let (sender, receiver) = channel(None);
    (UnboundedSender { inner: sender }, receiver)
}

fn channel<T>(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let state = Rc::new(RefCell::new(ChannelState {
        queue: VecDeque::new(),
        capacity,
        senders: 1,
        receiver_alive: true,
        receiver_waiter: None,
        sender_waiters: VecDeque::new(),
    }));
    (
        Sender {
            state: Rc::clone(&state),
        },
        Receiver { state },
    )
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Send<'_, T> {
        Send {
            sender: self,
            value: Some(value),
            waiter: None,
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.state.borrow_mut().senders += 1;
        Self {
            state: Rc::clone(&self.state),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waiter = {
            let mut state = self.state.borrow_mut();
            state.senders -= 1;
            (state.senders == 0)
                .then(|| state.receiver_waiter.take())
                .flatten()
        };
        if let Some(waiter) = waiter {
            wake(&waiter, WakeSource::Channel);
        }
    }
}

impl<T> UnboundedSender<T> {
    pub fn send(&self, value: T) -> Result<(), T> {
        let waiter = {
            let mut state = self.inner.state.borrow_mut();
            if !state.receiver_alive {
                return Err(value);
            }
            state.queue.push_back(value);
            state.receiver_waiter.take()
        };
        if let Some(waiter) = waiter {
            wake(&waiter, WakeSource::Channel);
        }
        Ok(())
    }

    /// Drop every value that has been sent but not yet received.
    pub fn discard_pending(&self) -> usize {
        let mut state = self.inner.state.borrow_mut();
        let discarded = state.queue.len();
        state.queue.clear();
        discarded
    }
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Receiver<T> {
    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv { receiver: self }
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut state = self.state.borrow_mut();
        if let Some(value) = state.queue.pop_front() {
            if let Some(sender) = state.sender_waiters.pop_front() {
                wake(&sender, WakeSource::Channel);
            }
            Ok(value)
        } else if state.senders == 0 {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.receiver_alive = false;
            std::mem::take(&mut state.sender_waiters)
        };
        for waiter in waiters {
            wake(&waiter, WakeSource::Channel);
        }
    }
}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), T>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let waiter = current_waiter();
        let mut state = this.sender.state.borrow_mut();
        if !state.receiver_alive {
            return Poll::Ready(Err(this
                .value
                .take()
                .expect("send polled after completion")));
        }
        let has_space = state
            .capacity
            .is_none_or(|capacity| state.queue.len() < capacity);
        if has_space {
            let value = this.value.take().expect("send polled after completion");
            state.queue.push_back(value);
            if let Some(waiter) = state.receiver_waiter.take() {
                wake(&waiter, WakeSource::Channel);
            }
            Poll::Ready(Ok(()))
        } else {
            if !state
                .sender_waiters
                .iter()
                .any(|waiting| waiting.task == waiter.task)
            {
                state.sender_waiters.push_back(Waiter {
                    task: waiter.task,
                    scheduler: std::sync::Arc::clone(&waiter.scheduler),
                });
                this.waiter = Some(waiter);
            }
            Poll::Pending
        }
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        let mut state = self.sender.state.borrow_mut();
        if let Some(index) = state
            .sender_waiters
            .iter()
            .position(|waiting| waiting.task == waiter.task)
        {
            state.sender_waiters.remove(index);
        }
    }
}

impl<T> Future for Recv<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.receiver.state.borrow_mut();
        if let Some(value) = state.queue.pop_front() {
            if let Some(sender) = state.sender_waiters.pop_front() {
                wake(&sender, WakeSource::Channel);
            }
            Poll::Ready(Some(value))
        } else if state.senders == 0 {
            Poll::Ready(None)
        } else {
            state.receiver_waiter = Some(current_waiter());
            Poll::Pending
        }
    }
}
