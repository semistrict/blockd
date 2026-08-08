//! Owned groups of child actors.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;

use crate::channel::{Receiver, TryRecvError, UnboundedSender, unbounded};
use crate::{TaskHandle, TaskId, spawn};

/// A cancellation tree node for dynamically spawned child actors.
///
/// Completed children are reaped without cancelling them. Dropping the set
/// drops every remaining handle and therefore cancels the whole subtree.
pub struct TaskSet {
    actors: BTreeMap<TaskId, TaskHandle<()>>,
    completed_tx: UnboundedSender<TaskId>,
    completed_rx: Receiver<TaskId>,
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
            actors: BTreeMap::new(),
            completed_tx,
            completed_rx,
        }
    }

    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) -> TaskId {
        let completed = self.completed_tx.clone();
        let task_id = Rc::new(Cell::new(u64::MAX));
        let child_id = Rc::clone(&task_id);
        let handle = spawn(async move {
            future.await;
            let _ = completed.send(child_id.get());
        });
        let id = handle.id();
        task_id.set(id);
        self.actors.insert(id, handle);
        id
    }

    pub fn reap(&mut self) {
        loop {
            match self.completed_rx.try_recv() {
                Ok(task) => {
                    if let Some(handle) = self.actors.remove(&task) {
                        handle.detach();
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Closed) => return,
            }
        }
    }

    pub fn len(&self) -> usize {
        self.actors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::TaskSet;
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
            yield_now().await;
            yield_now().await;
            children.reap();
            assert_eq!(children.len(), 1);
            drop(children);
            yield_now().await;
        });
        assert!(cancelled.get());
    }
}
