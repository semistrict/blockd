//! Owned groups of child actors.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::rc::Rc;

use crate::channel::{Receiver, UnboundedSender, unbounded};
use crate::{TaskHandle, TaskId, spawn};

/// A cancellation tree node for dynamically spawned child actors.
///
/// Completed children are reaped without cancelling them. Dropping the set
/// drops every remaining handle and therefore cancels the whole subtree.
pub struct ActorCollection<E> {
    actors: Rc<RefCell<BTreeMap<TaskId, TaskHandle<()>>>>,
    completed_tx: UnboundedSender<(TaskId, Option<E>)>,
    failures_rx: Receiver<E>,
    reaper: TaskHandle<()>,
}

/// A child collection whose actors cannot return an error.
pub type TaskSet = ActorCollection<Infallible>;

impl<E: 'static> Default for ActorCollection<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: 'static> ActorCollection<E> {
    pub fn new() -> Self {
        let actors = Rc::new(RefCell::new(BTreeMap::<TaskId, TaskHandle<()>>::new()));
        let reaper_actors = Rc::clone(&actors);
        let (completed_tx, mut completed_rx) = unbounded();
        let (failures_tx, failures_rx) = unbounded();
        let reaper = spawn(async move {
            while let Some((task, failure)) = completed_rx.recv().await {
                if let Some(handle) = reaper_actors.borrow_mut().remove(&task) {
                    handle.detach();
                }
                if let Some(failure) = failure {
                    let _ = failures_tx.send(failure);
                }
            }
        });
        Self {
            actors,
            completed_tx,
            failures_rx,
            reaper,
        }
    }

    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) -> TaskId {
        let completed = self.completed_tx.clone();
        let task_id = Rc::new(Cell::new(u64::MAX));
        let child_id = Rc::clone(&task_id);
        let handle = spawn(async move {
            future.await;
            let _ = completed.send((child_id.get(), None));
        });
        let id = handle.id();
        task_id.set(id);
        self.actors.borrow_mut().insert(id, handle);
        id
    }

    pub fn spawn_result(
        &mut self,
        future: impl Future<Output = Result<(), E>> + 'static,
    ) -> TaskId {
        let completed = self.completed_tx.clone();
        let task_id = Rc::new(Cell::new(u64::MAX));
        let child_id = Rc::clone(&task_id);
        let handle = spawn(async move {
            let failure = future.await.err();
            let _ = completed.send((child_id.get(), failure));
        });
        let id = handle.id();
        task_id.set(id);
        self.actors.borrow_mut().insert(id, handle);
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

    /// Cancel one owned child while leaving the rest of the collection live.
    pub fn cancel(&mut self, task: TaskId) -> bool {
        self.actors.borrow_mut().remove(&task).is_some()
    }
}

impl<E> Drop for ActorCollection<E> {
    fn drop(&mut self) {
        self.reaper.cancel();
        self.actors.borrow_mut().clear();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{ActorCollection, TaskSet};
    use crate::{Executor, delay, yield_now};

    #[test]
    fn reaps_completed_children_and_cancels_live_children() {
        let cancelled = Rc::new(Cell::new(false));
        let child_cancelled = Rc::clone(&cancelled);
        let mut executor = Executor::simulation(1);
        executor.block_on(async move {
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
        });
        assert!(cancelled.get());
    }

    #[test]
    fn reaps_completed_children_while_parent_is_idle() {
        let mut executor = Executor::simulation(1);
        executor.block_on(async move {
            let mut children = TaskSet::new();
            children.spawn(async {});
            delay(1).await;
            assert!(children.is_empty());
        });
    }

    #[test]
    fn reports_child_failures_and_reaps_the_child() {
        let mut executor = Executor::simulation(1);
        executor.block_on(async move {
            let mut children = ActorCollection::<&'static str>::new();
            children.spawn_result(async { Err("failed") });
            assert_eq!(children.next_failure().await, Some("failed"));
            assert!(children.is_empty());
        });
    }

    #[test]
    fn cancels_one_child_without_disturbing_its_sibling() {
        let first_cancelled = Rc::new(Cell::new(false));
        let second_completed = Rc::new(Cell::new(false));
        let mut executor = Executor::simulation(4);
        executor.block_on({
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
                delay(1).await;
                assert!(children.cancel(first));
                assert!(!children.cancel(first));
                delay(2).await;
            }
        });
        assert!(first_cancelled.get());
        assert!(second_completed.get());
    }
}
