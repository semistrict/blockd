//! Typed local actor requests with an owned one-shot reply.

use crate::channel::{OneReceiver, OneSender, oneshot};

/// A request body paired with the only capability that can complete it.
///
/// Transport correlation belongs in `T` only when it is part of the domain
/// protocol. Local callers await the returned [`OneReceiver`] instead of
/// registering an identifier in a shared reply table.
pub struct Request<T, R> {
    pub body: T,
    reply: OneSender<R>,
}

/// Construct a local request and the future that resolves its reply.
pub fn request<T, R>(body: T) -> (Request<T, R>, OneReceiver<R>) {
    let (reply, receive) = oneshot();
    (Request { body, reply }, receive)
}

impl<T, R> Request<T, R> {
    /// Resolve the request. A returned value means the caller cancelled first.
    pub fn reply(self, value: R) -> Result<(), R> {
        self.reply.send(value)
    }

    /// Separate the body from its reply capability for a long-running actor.
    pub fn into_parts(self) -> (T, Reply<R>) {
        (self.body, Reply(self.reply))
    }
}

/// The reply half of a request after its body has been moved elsewhere.
pub struct Reply<R>(OneSender<R>);

impl<R> Reply<R> {
    /// Resolve the request. A returned value means the caller cancelled first.
    pub fn send(self, value: R) -> Result<(), R> {
        self.0.send(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::Executor;

    use super::request;

    #[test]
    fn reply_resolves_the_request_future() {
        let mut executor = Executor::simulation(1);
        executor.block_on(async {
            let (request, reply) = request::<_, Result<u64, &'static str>>(7_u64);
            assert_eq!(request.body, 7);
            assert_eq!(request.reply(Ok(11)), Ok(()));
            assert_eq!(reply.await, Ok(Ok(11)));
        });
    }

    #[test]
    fn dropped_caller_cancels_the_reply_capability() {
        let mut executor = Executor::simulation(1);
        executor.block_on(async {
            let (request, reply) = request::<_, u64>(7_u64);
            drop(reply);
            assert_eq!(request.reply(11), Err(11));
        });
    }
}
