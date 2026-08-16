//! Declaration-order Tokio selection primitives.

use std::future::Future;

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

pub async fn select2<A: Future, B: Future>(first: A, second: B) -> Either<A::Output, B::Output> {
    tokio::pin!(first, second);
    tokio::select! {
        biased;
        value = &mut first => Either::First(value),
        value = &mut second => Either::Second(value),
    }
}

pub async fn select3<A: Future, B: Future, C: Future>(
    first: A,
    second: B,
    third: C,
) -> OneOf3<A::Output, B::Output, C::Output> {
    tokio::pin!(first, second, third);
    tokio::select! {
        biased;
        value = &mut first => OneOf3::First(value),
        value = &mut second => OneOf3::Second(value),
        value = &mut third => OneOf3::Third(value),
    }
}

/// Wait for both futures while polling them in declaration order.
pub async fn join2<A: Future, B: Future>(first: A, second: B) -> (A::Output, B::Output) {
    tokio::join!(biased; first, second)
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

    #[tokio::test(start_paused = true)]
    async fn join_waits_for_both_in_declaration_order() {
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

        let result =
            crate::simulation_scope(7, crate::FaultConfig::default(), join2(first, second)).await;

        assert_eq!(result, ("first", "second"));
        assert_eq!(&*polls.borrow(), &[1, 2, 1]);
    }
}
