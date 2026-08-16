//! Typed Tokio actor requests with an owned one-shot reply.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::sync::oneshot;

pub use tokio::sync::oneshot::error::TryRecvError;

struct Cancellation {
    receiver_alive: bool,
    callback: Option<Box<dyn FnOnce() + Send>>,
}

/// A request body paired with the only capability that can complete it.
pub struct Request<T, R> {
    pub body: T,
    reply: Reply<R>,
}

/// The reply capability owned by the actor handling a [`Request`].
pub struct Reply<R> {
    sender: Option<oneshot::Sender<R>>,
    cancellation: Arc<Mutex<Cancellation>>,
}

/// The future returned to the request caller.
pub struct Response<R> {
    receiver: oneshot::Receiver<R>,
    cancellation: Arc<Mutex<Cancellation>>,
    completed: bool,
}

/// Construct a request and the future that resolves its reply.
pub fn request<T, R>(body: T) -> (Request<T, R>, Response<R>) {
    let (sender, receiver) = oneshot::channel();
    let cancellation = Arc::new(Mutex::new(Cancellation {
        receiver_alive: true,
        callback: None,
    }));
    (
        Request {
            body,
            reply: Reply {
                sender: Some(sender),
                cancellation: Arc::clone(&cancellation),
            },
        },
        Response {
            receiver,
            cancellation,
            completed: false,
        },
    )
}

impl<T, R> Request<T, R> {
    /// Resolve the request. A returned value means the caller cancelled first.
    pub fn reply(self, value: R) -> Result<(), R> {
        let mut reply = self.reply;
        reply.send(value)
    }

    /// Separate the body from its reply capability for a long-running actor.
    pub fn into_parts(self) -> (T, Reply<R>) {
        (self.body, self.reply)
    }

    /// Run `callback` if the caller drops its response before it is completed.
    pub fn on_cancel(&mut self, callback: impl FnOnce() + Send + 'static) {
        self.reply.on_cancel(callback);
    }
}

impl<R> Reply<R> {
    /// Resolve the request once. A returned value means the caller cancelled.
    pub fn send(&mut self, value: R) -> Result<(), R> {
        let Some(sender) = self.sender.take() else {
            return Err(value);
        };
        self.cancellation
            .lock()
            .expect("request cancellation mutex poisoned")
            .callback = None;
        sender.send(value)
    }

    /// Run `callback` if the caller has cancelled or later cancels the request.
    pub fn on_cancel(&mut self, callback: impl FnOnce() + Send + 'static) {
        let callback = {
            let mut cancellation = self
                .cancellation
                .lock()
                .expect("request cancellation mutex poisoned");
            if cancellation.receiver_alive {
                cancellation.callback = Some(Box::new(callback));
                None
            } else {
                Some(callback)
            }
        };
        if let Some(callback) = callback {
            callback();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        !self
            .cancellation
            .lock()
            .expect("request cancellation mutex poisoned")
            .receiver_alive
    }
}

impl<R> Future for Response<R> {
    type Output = Result<R, oneshot::error::RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(result) => {
                self.completed = true;
                self.cancellation
                    .lock()
                    .expect("request cancellation mutex poisoned")
                    .callback = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R> Response<R> {
    pub fn try_recv(&mut self) -> Result<R, TryRecvError> {
        match self.receiver.try_recv() {
            Ok(value) => {
                self.completed = true;
                self.cancellation
                    .lock()
                    .expect("request cancellation mutex poisoned")
                    .callback = None;
                Ok(value)
            }
            result => result,
        }
    }
}

impl<R> Drop for Response<R> {
    fn drop(&mut self) {
        let callback = {
            let mut cancellation = self
                .cancellation
                .lock()
                .expect("request cancellation mutex poisoned");
            cancellation.receiver_alive = false;
            if self.completed {
                cancellation.callback = None;
                None
            } else {
                cancellation.callback.take()
            }
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

    use super::request;

    #[tokio::test]
    async fn reply_resolves_the_request_future() {
        let (request, response) = request::<_, Result<u64, &'static str>>(7_u64);
        assert_eq!(request.body, 7);
        assert_eq!(request.reply(Ok(11)), Ok(()));
        assert_eq!(response.await, Ok(Ok(11)));
    }

    #[tokio::test]
    async fn dropped_caller_cancels_the_reply_capability() {
        let (request, response) = request::<_, u64>(7_u64);
        drop(response);
        assert_eq!(request.reply(11), Err(11));
    }

    #[test]
    fn cancellation_notifies_the_reply_owner() {
        let (mut request, response) = request::<_, u64>(7_u64);
        let cancelled = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&cancelled);
        request.on_cancel(move || observed.store(true, Ordering::SeqCst));
        drop(response);
        assert!(cancelled.load(Ordering::SeqCst));
        let (_, reply) = request.into_parts();
        assert!(reply.is_cancelled());
    }
}
