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
