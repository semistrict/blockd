//! Owned groups of Tokio child actors.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;

use crate::channel::{TryRecvError, UnboundedReceiver, unbounded};
use crate::{TaskId, spawn};

pub struct TaskSet {
    actors: Rc<RefCell<BTreeMap<TaskId, tokio::task::AbortHandle>>>,
    completed_rx: UnboundedReceiver<TaskId>,
    completed_tx: crate::channel::UnboundedSender<TaskId>,
}

impl Default for TaskSet {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskSet {
    pub fn new() -> Self {
        let (completed_tx, completed_rx) = unbounded();
        Self {
            actors: Rc::new(RefCell::new(BTreeMap::new())),
            completed_rx,
            completed_tx,
        }
    }

    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) -> TaskId {
        let actors = Rc::clone(&self.actors);
        let completed = self.completed_tx.clone();
        let task_id = Rc::new(Cell::new(u64::MAX));
        let child_id = Rc::clone(&task_id);
        let handle = spawn(async move {
            future.await;
            let id = child_id.get();
            actors.borrow_mut().remove(&id);
            let _ = completed.send(id);
        });
        let (id, abort) = handle.detach_abort_handle();
        task_id.set(id);
        self.actors.borrow_mut().insert(id, abort);
        id
    }

    pub async fn next_done(&mut self) -> Option<TaskId> {
        self.completed_rx.recv().await
    }

    pub fn try_next_done(&mut self) -> Result<TaskId, TryRecvError> {
        self.completed_rx.try_recv()
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

impl Drop for TaskSet {
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

    use super::TaskSet;
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
    async fn reports_child_completion_and_reaps_the_child() {
        simulate(1, async move {
            let mut children = TaskSet::new();
            let child = children.spawn(async {});
            assert_eq!(children.next_done().await, Some(child));
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
            for index in 0..10_000_u64 {
                let child = children.spawn(async move {
                    let _ = index;
                });
                assert_eq!(children.next_done().await, Some(child));
                yield_now().await;
                assert!(children.len() <= 1, "completed handles accumulated");
            }
            yield_now().await;
            assert!(children.is_empty());
        })
        .await;
    }
}
