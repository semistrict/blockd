//! Fixed declaration-order selection primitives.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::runtime::delay;

#[derive(Debug, PartialEq, Eq)]
pub enum Either<A, B> {
    First(A),
    Second(B),
}

#[derive(Debug, PartialEq, Eq)]
pub enum OneOf3<A, B, C> {
    First(A),
    Second(B),
    Third(C),
}

pub struct Select2<A: Future, B: Future> {
    first: Pin<Box<A>>,
    second: Pin<Box<B>>,
}

pub struct Select3<A: Future, B: Future, C: Future> {
    first: Pin<Box<A>>,
    second: Pin<Box<B>>,
    third: Pin<Box<C>>,
}

/// Wait for both futures, polling them in declaration order on every wake.
///
/// Unlike [`select2`], completion of one side does not cancel the other. This
/// is the common durable-write shape for actors which must await redundant
/// copies before publishing a result.
pub struct Join2<A: Future, B: Future> {
    first: Option<Pin<Box<A>>>,
    second: Option<Pin<Box<B>>>,
    first_output: Option<A::Output>,
    second_output: Option<B::Output>,
}

pub fn select2<A: Future, B: Future>(first: A, second: B) -> Select2<A, B> {
    Select2 {
        first: Box::pin(first),
        second: Box::pin(second),
    }
}

pub fn select3<A: Future, B: Future, C: Future>(first: A, second: B, third: C) -> Select3<A, B, C> {
    Select3 {
        first: Box::pin(first),
        second: Box::pin(second),
        third: Box::pin(third),
    }
}

pub fn join2<A: Future, B: Future>(first: A, second: B) -> Join2<A, B> {
    Join2 {
        first: Some(Box::pin(first)),
        second: Some(Box::pin(second)),
        first_output: None,
        second_output: None,
    }
}

impl<A: Future, B: Future> Unpin for Join2<A, B> {}

impl<A: Future, B: Future> Future for Join2<A, B> {
    type Output = (A::Output, B::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(first) = this.first.as_mut()
            && let Poll::Ready(value) = first.as_mut().poll(cx)
        {
            this.first = None;
            this.first_output = Some(value);
        }
        if let Some(second) = this.second.as_mut()
            && let Poll::Ready(value) = second.as_mut().poll(cx)
        {
            this.second = None;
            this.second_output = Some(value);
        }
        match (this.first_output.take(), this.second_output.take()) {
            (Some(first), Some(second)) => Poll::Ready((first, second)),
            (first, second) => {
                this.first_output = first;
                this.second_output = second;
                Poll::Pending
            }
        }
    }
}

impl<A: Future, B: Future> Future for Select2<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(value) = self.first.as_mut().poll(cx) {
            return Poll::Ready(Either::First(value));
        }
        self.second.as_mut().poll(cx).map(Either::Second)
    }
}

impl<A: Future, B: Future, C: Future> Future for Select3<A, B, C> {
    type Output = OneOf3<A::Output, B::Output, C::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(value) = self.first.as_mut().poll(cx) {
            return Poll::Ready(OneOf3::First(value));
        }
        if let Poll::Ready(value) = self.second.as_mut().poll(cx) {
            return Poll::Ready(OneOf3::Second(value));
        }
        self.third.as_mut().poll(cx).map(OneOf3::Third)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeout;

pub async fn timeout<T>(nanoseconds: u64, future: impl Future<Output = T>) -> Result<T, Timeout> {
    match select2(future, delay(nanoseconds)).await {
        Either::First(value) => Ok(value),
        Either::Second(()) => Err(Timeout),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::task::Poll;

    use super::join2;

    #[test]
    fn join_waits_for_both_in_declaration_order() {
        let polls = Rc::new(RefCell::new(Vec::new()));
        let first_polls = Rc::clone(&polls);
        let mut first_ready = false;
        let first = std::future::poll_fn(move |cx| {
            first_polls.borrow_mut().push(1);
            if first_ready {
                Poll::Ready("first")
            } else {
                first_ready = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        });
        let second_polls = Rc::clone(&polls);
        let second = std::future::poll_fn(move |_| {
            second_polls.borrow_mut().push(2);
            Poll::Ready("second")
        });

        let mut executor = crate::Executor::simulation(7);
        let result = executor.block_on(join2(first, second));

        assert_eq!(result, ("first", "second"));
        assert_eq!(&*polls.borrow(), &[1, 2, 1]);
    }
}
