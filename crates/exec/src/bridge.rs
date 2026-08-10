//! A one-shot promise that bridges external threads and local actors.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::inject::{Injector, Lane};
use crate::runtime::{Waiter, WakeSource, current_waiter, wake};

struct State<T> {
    value: Option<T>,
    sender_alive: bool,
    receiver_alive: bool,
    waiter: Option<Waiter>,
    cancel_callback: Option<Box<dyn FnOnce() + Send>>,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    ready: Condvar,
}

pub struct BridgeSender<T> {
    shared: Arc<Shared<T>>,
}

pub struct BridgeReceiver<T> {
    shared: Arc<Shared<T>>,
}

pub struct BridgeRequest<T, R> {
    pub body: T,
    reply: ReplyTarget<R>,
}

pub struct BridgeReply<R>(Option<BridgeSender<R>>);

pub enum ReplyTarget<R> {
    Bridge(BridgeReply<R>),
    Injector(Injector<R>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BridgeRecvError {
    Closed,
    Timeout,
}

pub fn bridge<T>() -> (BridgeSender<T>, BridgeReceiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            value: None,
            sender_alive: true,
            receiver_alive: true,
            waiter: None,
            cancel_callback: None,
        }),
        ready: Condvar::new(),
    });
    (
        BridgeSender {
            shared: Arc::clone(&shared),
        },
        BridgeReceiver { shared },
    )
}

pub fn bridge_request<T, R>(body: T) -> (BridgeRequest<T, R>, BridgeReceiver<R>) {
    let (send, receive) = bridge();
    (
        BridgeRequest {
            body,
            reply: ReplyTarget::Bridge(BridgeReply(Some(send))),
        },
        receive,
    )
}

impl<T, R> BridgeRequest<T, R> {
    pub fn with_reply(body: T, reply: ReplyTarget<R>) -> Self {
        Self { body, reply }
    }

    pub fn into_parts(self) -> (T, ReplyTarget<R>) {
        (self.body, self.reply)
    }

    pub fn on_cancel(&mut self, callback: impl FnOnce() + Send + 'static) {
        self.reply.on_cancel(callback);
    }
}

impl<R> BridgeReply<R> {
    pub fn send(&mut self, value: R) -> Result<(), R> {
        let Some(reply) = self.0.take() else {
            return Err(value);
        };
        reply.send(value)
    }

    fn on_cancel(&mut self, callback: Box<dyn FnOnce() + Send>) {
        if let Some(reply) = self.0.as_mut() {
            reply.on_cancel(callback);
        }
    }

    fn is_cancelled(&self) -> bool {
        self.0.as_ref().is_none_or(BridgeSender::is_cancelled)
    }
}

impl<R> ReplyTarget<R> {
    pub fn injector(injector: Injector<R>) -> Self {
        Self::Injector(injector)
    }

    pub fn send(&mut self, value: R) -> Result<(), R> {
        match self {
            Self::Bridge(reply) => reply.send(value),
            Self::Injector(reply) => reply.push(Lane::Critical, value),
        }
    }

    pub fn on_cancel(&mut self, callback: impl FnOnce() + Send + 'static) {
        let callback = Box::new(callback);
        match self {
            Self::Bridge(reply) => reply.on_cancel(callback),
            Self::Injector(_) => drop(callback),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        match self {
            Self::Bridge(reply) => reply.is_cancelled(),
            Self::Injector(_) => false,
        }
    }
}

impl<T> BridgeSender<T> {
    fn on_cancel(&mut self, callback: Box<dyn FnOnce() + Send>) {
        let callback = {
            let mut state = self.shared.state.lock().expect("bridge mutex poisoned");
            if !state.receiver_alive {
                Some(callback)
            } else if state.sender_alive {
                state.cancel_callback = Some(callback);
                None
            } else {
                None
            }
        };
        if let Some(callback) = callback {
            callback();
        }
    }

    fn is_cancelled(&self) -> bool {
        !self
            .shared
            .state
            .lock()
            .expect("bridge mutex poisoned")
            .receiver_alive
    }

    pub fn send(self, value: T) -> Result<(), T> {
        let waiter = {
            let mut state = self.shared.state.lock().expect("bridge mutex poisoned");
            state.sender_alive = false;
            if !state.receiver_alive {
                return Err(value);
            }
            state.cancel_callback = None;
            state.value = Some(value);
            state.waiter.take()
        };
        self.shared.ready.notify_one();
        if let Some(waiter) = waiter {
            wake(&waiter, WakeSource::Oneshot);
        }
        Ok(())
    }
}

impl<T> Drop for BridgeSender<T> {
    fn drop(&mut self) {
        let waiter = {
            let mut state = self.shared.state.lock().expect("bridge mutex poisoned");
            if !state.sender_alive {
                return;
            }
            state.sender_alive = false;
            state.cancel_callback = None;
            state.waiter.take()
        };
        self.shared.ready.notify_one();
        if let Some(waiter) = waiter {
            wake(&waiter, WakeSource::Oneshot);
        }
    }
}

impl<T> BridgeReceiver<T> {
    pub fn blocking_recv_timeout(self, timeout: Duration) -> Result<T, BridgeRecvError> {
        let mut state = self.shared.state.lock().expect("bridge mutex poisoned");
        let (next, result) = self
            .shared
            .ready
            .wait_timeout_while(state, timeout, |state| {
                state.value.is_none() && state.sender_alive
            })
            .expect("bridge mutex poisoned");
        state = next;
        let outcome = if let Some(value) = state.value.take() {
            Ok(value)
        } else if !state.sender_alive {
            Err(BridgeRecvError::Closed)
        } else if result.timed_out() {
            Err(BridgeRecvError::Timeout)
        } else {
            unreachable!("bridge wait ended without a value, close, or timeout")
        };
        state.receiver_alive = false;
        let callback = state.cancel_callback.take();
        drop(state);
        if let Some(callback) = callback {
            callback();
        }
        outcome
    }
}

impl<T> Future for BridgeReceiver<T> {
    type Output = Result<T, BridgeRecvError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.state.lock().expect("bridge mutex poisoned");
        let outcome = if let Some(value) = state.value.take() {
            state.receiver_alive = false;
            Some(Ok(value))
        } else if !state.sender_alive {
            state.receiver_alive = false;
            Some(Err(BridgeRecvError::Closed))
        } else {
            state.waiter = Some(current_waiter());
            None
        };
        let callback = outcome.as_ref().and_then(|_| state.cancel_callback.take());
        drop(state);
        if let Some(callback) = callback {
            callback();
        }
        match outcome {
            Some(outcome) => Poll::Ready(outcome),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for BridgeReceiver<T> {
    fn drop(&mut self) {
        let callback = {
            let mut state = self.shared.state.lock().expect("bridge mutex poisoned");
            state.receiver_alive = false;
            state.cancel_callback.take()
        };
        if let Some(callback) = callback {
            callback();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::Executor;

    use super::{BridgeRecvError, bridge, bridge_request};

    #[test]
    #[allow(
        clippy::disallowed_methods,
        reason = "the bridge exists specifically to cross the production thread boundary"
    )]
    fn actor_can_await_an_external_reply() {
        let (send, receive) = bridge();
        let sender = thread::spawn(move || send.send(7_u64));
        let mut executor = Executor::production();
        assert_eq!(executor.block_on(receive), Ok(7));
        assert_eq!(sender.join().expect("sender thread"), Ok(()));
    }

    #[test]
    fn external_thread_can_block_for_an_actor_reply() {
        let (send, receive) = bridge();
        let mut executor = Executor::simulation(1);
        executor.block_on(async move {
            assert_eq!(send.send(11_u64), Ok(()));
        });
        assert_eq!(
            receive.blocking_recv_timeout(Duration::from_secs(1)),
            Ok(11)
        );
    }

    #[test]
    fn blocking_wait_reports_timeout_and_cancels_the_sender() {
        let (send, receive) = bridge::<u64>();
        assert_eq!(
            receive.blocking_recv_timeout(Duration::ZERO),
            Err(BridgeRecvError::Timeout)
        );
        assert_eq!(send.send(11), Err(11));
    }

    #[test]
    fn request_owns_a_single_reply_capability() {
        let (request, receive) = bridge_request::<_, u64>(7_u64);
        let (body, mut reply) = request.into_parts();
        assert_eq!(body, 7);
        assert_eq!(reply.send(11), Ok(()));
        assert_eq!(reply.send(13), Err(13));
        assert_eq!(
            receive.blocking_recv_timeout(Duration::from_secs(1)),
            Ok(11)
        );
    }

    #[test]
    fn request_cancellation_notifies_the_reply_owner() {
        let (mut request, receive) = bridge_request::<_, u64>(7_u64);
        let cancelled = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&cancelled);
        request.on_cancel(move || observed.store(true, Ordering::SeqCst));
        drop(receive);
        assert!(cancelled.load(Ordering::SeqCst));
        let (_, reply) = request.into_parts();
        assert!(reply.is_cancelled());
    }
}
