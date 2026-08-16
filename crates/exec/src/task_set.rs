//! Owned groups of Tokio child actors.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::rc::Rc;

use crate::channel::{UnboundedReceiver, unbounded};
use crate::{TaskId, spawn};

pub struct ActorCollection<E> {
    actors: Rc<RefCell<BTreeMap<TaskId, tokio::task::AbortHandle>>>,
    failures_rx: UnboundedReceiver<E>,
    failures_tx: crate::channel::UnboundedSender<E>,
}

pub type TaskSet = ActorCollection<Infallible>;

impl<E: 'static> Default for ActorCollection<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: 'static> ActorCollection<E> {
    pub fn new() -> Self {
        let (failures_tx, failures_rx) = unbounded();
        Self {
            actors: Rc::new(RefCell::new(BTreeMap::new())),
            failures_rx,
            failures_tx,
        }
    }

    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) -> TaskId {
        self.spawn_inner(async move {
            future.await;
            None
        })
    }

    pub fn spawn_result(
        &mut self,
        future: impl Future<Output = Result<(), E>> + 'static,
    ) -> TaskId {
        self.spawn_inner(async move { future.await.err() })
    }

    fn spawn_inner(&mut self, future: impl Future<Output = Option<E>> + 'static) -> TaskId {
        let actors = Rc::clone(&self.actors);
        let failures = self.failures_tx.clone();
        let task_id = Rc::new(Cell::new(u64::MAX));
        let child_id = Rc::clone(&task_id);
        let handle = spawn(async move {
            let failure = future.await;
            actors.borrow_mut().remove(&child_id.get());
            if let Some(failure) = failure {
                let _ = failures.send(failure);
            }
        });
        let (id, abort) = handle.detach_abort_handle();
        task_id.set(id);
        self.actors.borrow_mut().insert(id, abort);
        id
    }

    pub async fn next_failure(&mut self) -> Option<E> {
        self.failures_rx.recv().await
    }

    pub fn len(&self) -> usize {
        self.actors.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.actors.borrow().is_empty()
    }

    pub fn cancel(&mut self, task: TaskId) -> bool {
        let Some(abort) = self.actors.borrow_mut().remove(&task) else {
            return false;
        };
        abort.abort();
        true
    }
}

impl<E> Drop for ActorCollection<E> {
    fn drop(&mut self) {
        for (_, abort) in self.actors.borrow_mut().split_off(&0) {
            abort.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{ActorCollection, TaskSet};
    use crate::{FaultConfig, delay, simulation_scope, yield_now};

    async fn simulate<T>(seed: u64, future: impl Future<Output = T>) -> T {
        tokio::task::LocalSet::new()
            .run_until(simulation_scope(seed, FaultConfig::default(), future))
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn reaps_completed_children_and_cancels_live_children() {
        let cancelled = Rc::new(Cell::new(false));
        let child_cancelled = Rc::clone(&cancelled);
        simulate(1, async move {
            let mut children = TaskSet::new();
            children.spawn(async {});
            children.spawn(async move {
                struct MarkOnDrop(Rc<Cell<bool>>);
                impl Drop for MarkOnDrop {
                    fn drop(&mut self) {
                        self.0.set(true);
                    }
                }
                let _guard = MarkOnDrop(child_cancelled);
                delay(100).await;
            });
            delay(1).await;
            assert_eq!(children.len(), 1);
            drop(children);
            yield_now().await;
        })
        .await;
        assert!(cancelled.get());
    }

    #[tokio::test(start_paused = true)]
    async fn reaps_completed_children_while_parent_is_idle() {
        simulate(1, async move {
            let mut children = TaskSet::new();
            children.spawn(async {});
            delay(1).await;
            assert!(children.is_empty());
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn reports_child_failures_and_reaps_the_child() {
        simulate(1, async move {
            let mut children = ActorCollection::<&'static str>::new();
            children.spawn_result(async { Err("failed") });
            assert_eq!(children.next_failure().await, Some("failed"));
            assert!(children.is_empty());
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancels_one_child_without_disturbing_its_sibling() {
        let first_cancelled = Rc::new(Cell::new(false));
        let second_completed = Rc::new(Cell::new(false));
        simulate(4, {
            let first_cancelled = Rc::clone(&first_cancelled);
            let second_completed = Rc::clone(&second_completed);
            async move {
                struct MarkOnDrop(Rc<Cell<bool>>);
                impl Drop for MarkOnDrop {
                    fn drop(&mut self) {
                        self.0.set(true);
                    }
                }
                let mut children = TaskSet::new();
                let first = children.spawn(async move {
                    let _guard = MarkOnDrop(first_cancelled);
                    delay(100).await;
                });
                children.spawn(async move {
                    delay(1).await;
                    second_completed.set(true);
                });
                yield_now().await;
                assert!(children.cancel(first));
                assert!(!children.cancel(first));
                delay(2).await;
                assert!(children.is_empty());
            }
        })
        .await;
        assert!(first_cancelled.get());
        assert!(second_completed.get());
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_completion_keeps_the_owned_handle_set_bounded() {
        simulate(9, async move {
            let mut children = TaskSet::new();
            let (completed, mut completions) = crate::channel::unbounded();
            for index in 0..10_000_u64 {
                let completed = completed.clone();
                children.spawn(async move {
                    let _ = completed.send(index);
                });
                assert_eq!(completions.recv().await, Some(index));
                yield_now().await;
                assert!(children.len() <= 1, "completed handles accumulated");
            }
            yield_now().await;
            assert!(children.is_empty());
        })
        .await;
    }
}
