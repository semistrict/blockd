//! Current-thread deterministic task executor.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use crate::channel::{OneReceiver, oneshot};
use crate::fault::{FaultConfig, FaultPoint};
use crate::rng::Pcg64;
use crate::trace::TraceHasher;

pub type TaskId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeSource {
    Spawn,
    Waker,
    Timer,
    Yield,
    Oneshot,
    Channel,
    External,
    Cancellation,
}

#[derive(Clone, Copy, Debug)]
struct Ready {
    task: TaskId,
    source: WakeSource,
}

#[derive(Default)]
struct ReadyState {
    queue: VecDeque<Ready>,
    queued: BTreeSet<TaskId>,
}

pub(crate) struct Scheduler {
    ready: Mutex<ReadyState>,
    changed: Condvar,
}

impl Scheduler {
    fn new() -> Self {
        Self {
            ready: Mutex::new(ReadyState::default()),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn schedule(&self, task: TaskId, source: WakeSource) {
        let mut ready = self.ready.lock().expect("ready mutex poisoned");
        if ready.queued.insert(task) {
            ready.queue.push_back(Ready { task, source });
            self.changed.notify_one();
        }
    }

    fn pop(&self) -> Option<Ready> {
        let mut ready = self.ready.lock().expect("ready mutex poisoned");
        let item = ready.queue.pop_front()?;
        ready.queued.remove(&item.task);
        Some(item)
    }

    fn wait(&self) {
        let ready = self.ready.lock().expect("ready mutex poisoned");
        drop(
            self.changed
                .wait_while(ready, |ready| ready.queue.is_empty())
                .expect("ready mutex poisoned"),
        );
    }

    fn wait_timeout(&self, duration: Duration) {
        let ready = self.ready.lock().expect("ready mutex poisoned");
        drop(
            self.changed
                .wait_timeout_while(ready, duration, |ready| ready.queue.is_empty())
                .expect("ready mutex poisoned"),
        );
    }
}

pub type MonotonicClock = Arc<dyn Fn() -> u64 + Send + Sync>;

struct TaskWake {
    task: TaskId,
    scheduler: Arc<Scheduler>,
}

impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.scheduler.schedule(self.task, WakeSource::Waker);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.scheduler.schedule(self.task, WakeSource::Waker);
    }
}

struct TimerRequest {
    at: u64,
    sequence: u64,
    task: TaskId,
    alive: Arc<AtomicBool>,
}

struct RuntimeContext {
    now: Rc<Cell<u64>>,
    scheduler: Arc<Scheduler>,
    current_task: Rc<Cell<Option<TaskId>>>,
    poll_sequence: Rc<Cell<u64>>,
    timer_sequence: Rc<Cell<u64>>,
    timer_requests: Rc<RefCell<Vec<TimerRequest>>>,
    observations: Rc<RefCell<Vec<String>>>,
    rng: Rc<RefCell<Option<Pcg64>>>,
    faults: Rc<RefCell<FaultConfig>>,
    fault_hits: Rc<RefCell<BTreeMap<FaultPoint, u64>>>,
    clock: Option<MonotonicClock>,
    next_task: Rc<Cell<TaskId>>,
    spawn_requests: Rc<RefCell<Vec<SpawnRequest>>>,
}

impl Clone for RuntimeContext {
    fn clone(&self) -> Self {
        Self {
            now: Rc::clone(&self.now),
            scheduler: Arc::clone(&self.scheduler),
            current_task: Rc::clone(&self.current_task),
            poll_sequence: Rc::clone(&self.poll_sequence),
            timer_sequence: Rc::clone(&self.timer_sequence),
            timer_requests: Rc::clone(&self.timer_requests),
            observations: Rc::clone(&self.observations),
            rng: Rc::clone(&self.rng),
            faults: Rc::clone(&self.faults),
            fault_hits: Rc::clone(&self.fault_hits),
            clock: self.clock.clone(),
            next_task: Rc::clone(&self.next_task),
            spawn_requests: Rc::clone(&self.spawn_requests),
        }
    }
}

thread_local! {
    static CURRENT: RefCell<Vec<RuntimeContext>> = const { RefCell::new(Vec::new()) };
}

struct Entered;

impl Drop for Entered {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current
                .borrow_mut()
                .pop()
                .expect("executor context stack empty");
        });
    }
}

fn enter(context: RuntimeContext) -> Entered {
    CURRENT.with(|current| current.borrow_mut().push(context));
    Entered
}

fn with_context<T>(operation: impl FnOnce(&RuntimeContext) -> T) -> T {
    CURRENT.with(|current| {
        let current = current.borrow();
        operation(
            current
                .last()
                .expect("async primitive used outside executor"),
        )
    })
}

pub fn now() -> u64 {
    with_context(refresh_now)
}

/// Monotonically increasing identity of the task poll currently in progress.
///
/// Simulation worlds use this to account bounded cooperative work without
/// consulting wall time. Calls made during one future poll return the same
/// value; a wake and subsequent poll advances it.
pub fn current_poll() -> u64 {
    with_context(|context| context.poll_sequence.get())
}

fn refresh_now(context: &RuntimeContext) -> u64 {
    if let Some(clock) = &context.clock {
        context.now.set(clock());
    }
    context.now.get()
}

pub(crate) struct Waiter {
    pub(crate) task: TaskId,
    pub(crate) scheduler: Arc<Scheduler>,
}

pub(crate) fn current_waiter() -> Waiter {
    with_context(|context| Waiter {
        task: context
            .current_task
            .get()
            .expect("primitive polled outside a task"),
        scheduler: Arc::clone(&context.scheduler),
    })
}

pub(crate) fn wake(waiter: &Waiter, source: WakeSource) {
    waiter.scheduler.schedule(waiter.task, source);
}

pub fn observe(record: &dyn fmt::Debug) {
    with_context(|context| {
        context
            .observations
            .borrow_mut()
            .push(format!("{record:?}"));
    });
}

pub fn random_u64() -> u64 {
    with_context(|context| {
        let value = context
            .rng
            .borrow_mut()
            .as_mut()
            .expect("randomness is available only in simulation")
            .next_u64();
        context
            .observations
            .borrow_mut()
            .push(format!("Random({value})"));
        value
    })
}

pub fn fault_point(point: FaultPoint) -> bool {
    with_context(|context| {
        let mut rng = context.rng.borrow_mut();
        let Some(rng) = rng.as_mut() else {
            return false;
        };
        let result = {
            let mut faults = context.faults.borrow_mut();
            if let Some(forced) = faults.forced.get_mut(&point).and_then(VecDeque::pop_front) {
                forced
            } else {
                faults.enabled.contains(&point) && rng.hit(faults.probability)
            }
        };
        context
            .observations
            .borrow_mut()
            .push(format!("Fault({point:?}, {result})"));
        if result {
            let mut hits = context.fault_hits.borrow_mut();
            *hits.entry(point).or_default() += 1;
        }
        result
    })
}

pub struct Delay {
    duration: u64,
    deadline: Option<u64>,
    alive: Arc<AtomicBool>,
}

pub fn delay(nanoseconds: u64) -> Delay {
    Delay {
        duration: nanoseconds,
        deadline: None,
        alive: Arc::new(AtomicBool::new(true)),
    }
}

impl Future for Delay {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let current_now = now();
        if self
            .deadline
            .is_some_and(|deadline| current_now >= deadline)
        {
            self.alive.store(false, Ordering::Release);
            return Poll::Ready(());
        }
        if self.deadline.is_none() {
            let task = current_waiter().task;
            with_context(|context| {
                let sequence = context.timer_sequence.get();
                context.timer_sequence.set(sequence + 1);
                let at = current_now.saturating_add(self.duration);
                context.timer_requests.borrow_mut().push(TimerRequest {
                    at,
                    sequence,
                    task,
                    alive: Arc::clone(&self.alive),
                });
                self.deadline = Some(at);
            });
        }
        Poll::Pending
    }
}

impl Drop for Delay {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
    }
}

pub struct YieldNow(bool);

pub fn yield_now() -> YieldNow {
    YieldNow(false)
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            let waiter = current_waiter();
            waiter.scheduler.schedule(waiter.task, WakeSource::Yield);
            Poll::Pending
        }
    }
}

struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
    cancelled: Arc<AtomicBool>,
}

struct SpawnRequest {
    task: TaskId,
    actor: Task,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cancelled;

pub struct TaskHandle<T> {
    task: TaskId,
    result: OneReceiver<T>,
    cancelled: Arc<AtomicBool>,
    scheduler: Arc<Scheduler>,
    cancel_on_drop: bool,
}

impl<T> TaskHandle<T> {
    pub const fn id(&self) -> TaskId {
        self.task
    }

    pub fn cancel(&mut self) {
        if self.cancel_on_drop {
            self.cancelled.store(true, Ordering::Release);
            self.scheduler.schedule(self.task, WakeSource::Cancellation);
            self.cancel_on_drop = false;
        }
    }

    pub fn detach(mut self) -> TaskId {
        self.cancel_on_drop = false;
        self.task
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, Cancelled>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.result).poll(cx) {
            Poll::Ready(Ok(value)) => {
                self.cancel_on_drop = false;
                Poll::Ready(Ok(value))
            }
            Poll::Ready(Err(_)) => {
                self.cancel_on_drop = false;
                Poll::Ready(Err(Cancelled))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for TaskHandle<T> {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Simulation,
    Production,
}

#[derive(Debug)]
#[allow(dead_code)]
struct PollRecord {
    time: u64,
    task: TaskId,
    source: WakeSource,
}

pub struct Executor {
    mode: Mode,
    context: RuntimeContext,
    tasks: BTreeMap<TaskId, Task>,
    timers: BTreeMap<(u64, u64), TimerRequest>,
    trace: TraceHasher,
}

impl Executor {
    pub fn simulation(seed: u64) -> Self {
        Self::new(Mode::Simulation, Some(Pcg64::new(seed, 0)), None)
    }

    pub fn production() -> Self {
        Self::new(Mode::Production, None, None)
    }

    pub fn production_with_clock(clock: MonotonicClock) -> Self {
        Self::new(Mode::Production, None, Some(clock))
    }

    fn new(mode: Mode, rng: Option<Pcg64>, clock: Option<MonotonicClock>) -> Self {
        let scheduler = Arc::new(Scheduler::new());
        Self {
            mode,
            context: RuntimeContext {
                now: Rc::new(Cell::new(0)),
                scheduler,
                current_task: Rc::new(Cell::new(None)),
                poll_sequence: Rc::new(Cell::new(0)),
                timer_sequence: Rc::new(Cell::new(0)),
                timer_requests: Rc::new(RefCell::new(Vec::new())),
                observations: Rc::new(RefCell::new(Vec::new())),
                rng: Rc::new(RefCell::new(rng)),
                faults: Rc::new(RefCell::new(FaultConfig::default())),
                fault_hits: Rc::new(RefCell::new(BTreeMap::new())),
                clock,
                next_task: Rc::new(Cell::new(0)),
                spawn_requests: Rc::new(RefCell::new(Vec::new())),
            },
            tasks: BTreeMap::new(),
            timers: BTreeMap::new(),
            trace: TraceHasher::new(),
        }
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn now(&self) -> u64 {
        refresh_now(&self.context)
    }

    pub fn set_fault_config(&mut self, config: FaultConfig) {
        *self.context.faults.borrow_mut() = config;
    }

    pub fn fault_hits(&self) -> BTreeMap<FaultPoint, u64> {
        self.context.fault_hits.borrow().clone()
    }

    pub fn spawn<F, T>(&mut self, future: F) -> TaskHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let (task, actor, handle) = prepare_spawn(&self.context, future);
        self.tasks.insert(task, actor);
        self.context.scheduler.schedule(task, WakeSource::Spawn);
        handle
    }

    pub fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let output = Rc::new(RefCell::new(None));
        let target = Rc::clone(&output);
        self.spawn(async move {
            *target.borrow_mut() = Some(future.await);
        })
        .detach();
        loop {
            if let Some(value) = output.borrow_mut().take() {
                return value;
            }
            if !self.run_one() {
                assert_eq!(
                    self.mode,
                    Mode::Production,
                    "root actor stalled with no possible wake"
                );
                self.wait_for_wake();
            }
        }
    }

    pub fn run_until_stalled(&mut self) {
        while self.run_one() {}
    }

    pub fn run_until(&mut self, horizon: u64) {
        assert!(horizon >= self.now(), "cannot run backwards");
        loop {
            while self.poll_ready() {}
            let next_timer = self.timers.keys().next().map(|key| key.0);
            if next_timer.is_some_and(|at| at <= horizon) {
                self.fire_next_timers();
            } else {
                break;
            }
        }
        self.context.now.set(horizon);
    }

    pub fn advance_to(&mut self, time: u64) {
        assert!(time >= self.now(), "cannot move clock backwards");
        self.context.now.set(time);
        self.fire_due_timers();
    }

    pub fn run_ready(&mut self) {
        while self.poll_ready() {}
    }

    pub fn wait_for_wake(&self) {
        if let Some((&(deadline, _), _)) = self.timers.first_key_value() {
            let remaining = deadline.saturating_sub(self.now());
            self.context
                .scheduler
                .wait_timeout(Duration::from_nanos(remaining));
        } else {
            self.context.scheduler.wait();
        }
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn trace_hash(&self) -> u64 {
        self.trace.finish()
    }

    pub fn trace_records(&self) -> u64 {
        self.trace.records()
    }

    pub fn polls(&self) -> u64 {
        self.context.poll_sequence.get()
    }

    fn run_one(&mut self) -> bool {
        if self.poll_ready() {
            return true;
        }
        if self.mode == Mode::Production {
            refresh_now(&self.context);
            self.fire_due_timers();
            if self.poll_ready() {
                return true;
            }
        }
        if self.mode == Mode::Simulation && !self.timers.is_empty() {
            self.fire_next_timers();
            return self.poll_ready();
        }
        false
    }

    fn poll_ready(&mut self) -> bool {
        let Some(ready) = self.context.scheduler.pop() else {
            return false;
        };
        let Some(mut task) = self.tasks.remove(&ready.task) else {
            return true;
        };
        if task.cancelled.load(Ordering::Acquire) {
            return true;
        }
        let record = PollRecord {
            time: self.now(),
            task: ready.task,
            source: ready.source,
        };
        self.trace.record(&record);
        self.context
            .poll_sequence
            .set(self.context.poll_sequence.get().saturating_add(1));
        self.context.current_task.set(Some(ready.task));
        let waker = Waker::from(Arc::new(TaskWake {
            task: ready.task,
            scheduler: Arc::clone(&self.context.scheduler),
        }));
        let mut context = Context::from_waker(&waker);
        let entered = enter(self.context.clone());
        let result = task.future.as_mut().poll(&mut context);
        drop(entered);
        self.context.current_task.set(None);
        self.drain_registrations();
        if result.is_pending() && !task.cancelled.load(Ordering::Acquire) {
            self.tasks.insert(ready.task, task);
        }
        true
    }

    fn drain_registrations(&mut self) {
        for request in self.context.spawn_requests.borrow_mut().drain(..) {
            self.tasks.insert(request.task, request.actor);
            self.context
                .scheduler
                .schedule(request.task, WakeSource::Spawn);
        }
        for timer in self.context.timer_requests.borrow_mut().drain(..) {
            self.timers.insert((timer.at, timer.sequence), timer);
        }
        for observation in self.context.observations.borrow_mut().drain(..) {
            self.trace.record(&observation);
        }
    }

    fn fire_next_timers(&mut self) {
        let Some(time) = self.timers.keys().next().map(|key| key.0) else {
            return;
        };
        self.context.now.set(time);
        self.fire_due_timers();
    }

    fn fire_due_timers(&mut self) {
        let now = self.now();
        let keys: Vec<(u64, u64)> = self
            .timers
            .range(..=(now, u64::MAX))
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            let timer = self.timers.remove(&key).expect("timer key observed");
            if timer.alive.swap(false, Ordering::AcqRel) {
                self.context
                    .scheduler
                    .schedule(timer.task, WakeSource::Timer);
            }
        }
    }
}

fn prepare_spawn<F, T>(context: &RuntimeContext, future: F) -> (TaskId, Task, TaskHandle<T>)
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    let task = context.next_task.get();
    context.next_task.set(task + 1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let (sender, result) = oneshot();
    let wrapped = async move {
        let value = future.await;
        let _ = sender.send(value);
    };
    let actor = Task {
        future: Box::pin(wrapped),
        cancelled: Arc::clone(&cancelled),
    };
    let handle = TaskHandle {
        task,
        result,
        cancelled,
        scheduler: Arc::clone(&context.scheduler),
        cancel_on_drop: true,
    };
    (task, actor, handle)
}

/// Spawn a child actor from the currently running actor.
pub fn spawn<F, T>(future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    with_context(|context| {
        let (task, actor, handle) = prepare_spawn(context, future);
        context
            .spawn_requests
            .borrow_mut()
            .push(SpawnRequest { task, actor });
        handle
    })
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeSet;
    use std::rc::Rc;

    use super::{Executor, delay, random_u64, yield_now};
    use crate::channel::{bounded, oneshot, unbounded};
    use crate::select::{Either, select2, timeout};

    #[test]
    fn spawn_and_wake_order_is_fifo() {
        let mut executor = Executor::simulation(1);
        let order = Rc::new(RefCell::new(Vec::new()));
        for value in 0..8 {
            let order = Rc::clone(&order);
            executor
                .spawn(async move {
                    order.borrow_mut().push(value);
                    yield_now().await;
                    order.borrow_mut().push(value + 10);
                })
                .detach();
        }
        executor.run_until_stalled();
        assert_eq!(
            *order.borrow(),
            [0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17]
        );
    }

    #[test]
    fn same_deadline_timers_fire_in_registration_order() {
        let mut executor = Executor::simulation(1);
        let order = Rc::new(RefCell::new(Vec::new()));
        for value in 0..8 {
            let order = Rc::clone(&order);
            executor
                .spawn(async move {
                    delay(10).await;
                    order.borrow_mut().push(value);
                })
                .detach();
        }
        executor.run_until_stalled();
        assert_eq!(*order.borrow(), [0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(executor.now(), 10);
    }

    #[test]
    fn dropping_handle_cancels_actor() {
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let mut executor = Executor::simulation(1);
        let dropped = Rc::new(Cell::new(false));
        let actor_flag = Rc::clone(&dropped);
        let handle = executor.spawn(async move {
            let _flag = DropFlag(actor_flag);
            delay(100).await;
        });
        executor.run_ready();
        drop(handle);
        executor.run_ready();
        assert!(dropped.get());
        assert_eq!(executor.task_count(), 0);
    }

    #[test]
    fn promises_and_bounded_channels_defer_wakes() {
        let mut executor = Executor::simulation(2);
        let (ready, waiting) = oneshot();
        let (sender, mut receiver) = bounded(1);
        executor
            .spawn(async move {
                sender.send(1).await.unwrap();
                ready.send(()).unwrap();
                sender.send(2).await.unwrap();
            })
            .detach();
        let values = executor.block_on(async move {
            waiting.await.unwrap();
            vec![
                receiver.recv().await.unwrap(),
                receiver.recv().await.unwrap(),
            ]
        });
        assert_eq!(values, [1, 2]);
    }

    #[test]
    fn select_is_declaration_ordered_and_timeout_cancels_timer() {
        let mut executor = Executor::simulation(3);
        let selected = executor.block_on(async {
            let result = select2(std::future::ready(1), std::future::ready(2)).await;
            let timed = timeout(10, std::future::ready(3)).await;
            (result, timed)
        });
        assert_eq!(selected, (Either::First(1), Ok(3)));
        assert_eq!(executor.now(), 0);
    }

    fn channel_storm(seed: u64) -> u64 {
        let mut executor = Executor::simulation(seed);
        let (sender, mut receiver) = unbounded();
        for _ in 0..12 {
            let sender = sender.clone();
            executor
                .spawn(async move {
                    for _ in 0..8 {
                        sender.send(random_u64()).unwrap();
                        yield_now().await;
                    }
                })
                .detach();
        }
        drop(sender);
        executor
            .spawn(async move { while receiver.recv().await.is_some() {} })
            .detach();
        executor.run_until_stalled();
        executor.trace_hash()
    }

    #[test]
    fn replay_identity_and_seed_divergence() {
        for seed in 0..100 {
            assert_eq!(channel_storm(seed), channel_storm(seed));
        }
        let hashes: BTreeSet<u64> = (0..100).map(channel_storm).collect();
        assert_eq!(hashes.len(), 100);
    }

    #[test]
    fn actors_can_spawn_owned_children() {
        let mut executor = Executor::simulation(8);
        let values = Rc::new(RefCell::new(Vec::new()));
        let output = Rc::clone(&values);
        executor.block_on(async move {
            let child_output = Rc::clone(&output);
            let child = super::spawn(async move {
                yield_now().await;
                child_output.borrow_mut().push(2);
                7
            });
            output.borrow_mut().push(1);
            assert_eq!(child.await, Ok(7));
            output.borrow_mut().push(3);
        });
        assert_eq!(*values.borrow(), [1, 2, 3]);
    }
}
