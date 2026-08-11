use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Fair pending work for keys that may have at most one active operation.
pub(super) struct KeyedQueue<K, V> {
    queues: BTreeMap<K, VecDeque<V>>,
    ready: VecDeque<K>,
    active: BTreeSet<K>,
    pending: usize,
}

impl<K: Copy + Ord, V> KeyedQueue<K, V> {
    pub(super) fn new() -> Self {
        Self {
            queues: BTreeMap::new(),
            ready: VecDeque::new(),
            active: BTreeSet::new(),
            pending: 0,
        }
    }

    pub(super) fn push(&mut self, key: K, value: V) {
        let queue = self.queues.entry(key).or_default();
        if queue.is_empty() && !self.active.contains(&key) {
            self.ready.push_back(key);
        }
        queue.push_back(value);
        self.pending += 1;
    }

    pub(super) fn start_next(&mut self, concurrency: usize) -> Option<(K, V)> {
        if self.active.len() >= concurrency {
            return None;
        }
        let key = self.ready.pop_front()?;
        assert!(self.active.insert(key), "key already active");
        let value = self
            .queues
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .expect("ready key has pending work");
        self.pending -= 1;
        Some((key, value))
    }

    pub(super) fn complete(&mut self, key: K) {
        assert!(self.active.remove(&key), "completed key was not active");
        if self.queues.get(&key).is_some_and(VecDeque::is_empty) {
            self.queues.remove(&key);
        } else {
            self.ready.push_back(key);
        }
    }

    pub(super) fn remove_where(
        &mut self,
        key: K,
        mut predicate: impl FnMut(&V) -> bool,
    ) -> Option<V> {
        let queue = self.queues.get_mut(&key)?;
        let index = queue.iter().position(&mut predicate)?;
        let value = queue.remove(index).expect("matching queued value");
        self.pending -= 1;
        if queue.is_empty() && !self.active.contains(&key) {
            self.queues.remove(&key);
            self.ready.retain(|ready| *ready != key);
        }
        Some(value)
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending
    }

    pub(super) fn contains_key(&self, key: K) -> bool {
        self.active.contains(&key) || self.queues.contains_key(&key)
    }

    pub(super) fn is_idle(&self) -> bool {
        self.pending == 0 && self.active.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::KeyedQueue;

    #[test]
    fn serializes_each_key_without_hash_collision_blocking() {
        let mut queue = KeyedQueue::new();
        queue.push(1, "first");
        queue.push(1, "second");
        queue.push(17, "independent");

        assert_eq!(queue.start_next(2), Some((1, "first")));
        assert_eq!(queue.start_next(2), Some((17, "independent")));
        assert_eq!(queue.start_next(2), None);
        queue.complete(1);
        assert_eq!(queue.start_next(2), Some((1, "second")));
        queue.complete(17);
        queue.complete(1);
        assert!(queue.is_idle());
    }

    #[test]
    fn rotates_ready_keys_fairly() {
        let mut queue = KeyedQueue::new();
        queue.push(1, "a");
        queue.push(1, "b");
        queue.push(2, "c");

        assert_eq!(queue.start_next(1), Some((1, "a")));
        queue.complete(1);
        assert_eq!(queue.start_next(1), Some((2, "c")));
        queue.complete(2);
        assert_eq!(queue.start_next(1), Some((1, "b")));
    }

    #[test]
    fn exposes_active_and_pending_keys_for_protocol_coalescing() {
        let mut queue = KeyedQueue::new();
        queue.push(1, "first");
        assert!(queue.contains_key(1));
        assert_eq!(queue.start_next(1), Some((1, "first")));
        assert!(queue.contains_key(1));
        queue.complete(1);
        assert!(!queue.contains_key(1));
    }

    #[test]
    fn removes_cancelled_pending_work_without_disturbing_the_active_key() {
        let mut queue = KeyedQueue::new();
        queue.push(1, (1, "active"));
        queue.push(1, (2, "cancelled"));
        assert_eq!(queue.start_next(1), Some((1, (1, "active"))));
        assert_eq!(
            queue.remove_where(1, |(id, _)| *id == 2),
            Some((2, "cancelled"))
        );
        queue.complete(1);
        assert!(queue.is_idle());
    }
}
