//! Tokio-backed current-thread actor support.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant as TokioInstant;

use crate::fault::{FaultConfig, FaultPoint};
use crate::rng::Pcg64;
use crate::trace::TraceHasher;

pub type TaskId = u64;
type DurationObserver = Arc<dyn Fn(u64) + Send + Sync>;

#[derive(Clone)]
struct RuntimeContext {
    mode: Mode,
    epoch: Rc<RefCell<Option<tokio::time::Instant>>>,
    poll_sequence: Rc<Cell<u64>>,
    trace: Rc<RefCell<TraceHasher>>,
    rng: Rc<RefCell<Option<Pcg64>>>,
    faults: Rc<RefCell<FaultConfig>>,
    fault_hits: Rc<RefCell<BTreeMap<FaultPoint, u64>>>,
    poll_observer: Option<DurationObserver>,
    next_task: Rc<Cell<TaskId>>,
    live_tasks: Rc<Cell<usize>>,
    trace_task_polls: Rc<Cell<bool>>,
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
                .expect("actor runtime context stack empty");
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
                .expect("async primitive used outside a Tokio actor scope"),
        )
    })
}

fn context(
    mode: Mode,
    seed: Option<u64>,
    poll_observer: Option<DurationObserver>,
) -> RuntimeContext {
    RuntimeContext {
        mode,
        epoch: Rc::new(RefCell::new(None)),
        poll_sequence: Rc::new(Cell::new(0)),
        trace: Rc::new(RefCell::new(TraceHasher::new())),
        rng: Rc::new(RefCell::new(seed.map(|seed| Pcg64::new(seed, 0)))),
        faults: Rc::new(RefCell::new(FaultConfig::default())),
        fault_hits: Rc::new(RefCell::new(BTreeMap::new())),
        poll_observer,
        next_task: Rc::new(Cell::new(0)),
        live_tasks: Rc::new(Cell::new(0)),
        trace_task_polls: Rc::new(Cell::new(true)),
    }
}

pub fn now() -> u64 {
    with_context(now_for)
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn now_for(context: &RuntimeContext) -> u64 {
    let current = tokio::time::Instant::now();
    let mut epoch = context.epoch.borrow_mut();
    let epoch = epoch.get_or_insert(current);
    let elapsed = current.saturating_duration_since(*epoch);
    match context.mode {
        Mode::Simulation => u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        Mode::Production => nanos(elapsed),
    }
}

/// Monotonically increasing identity of the current Tokio task poll.
pub fn current_poll() -> u64 {
    with_context(|context| context.poll_sequence.get())
}

pub fn observe(record: &dyn fmt::Debug) {
    with_context(|context| context.trace.borrow_mut().record(record));
}

/// Deterministic under simulation; sourced from the operating system and
/// guaranteed nonzero in production for durable session/nonce identities.
pub fn random_u64() -> u64 {
    with_context(|context| {
        let value = match context.mode {
            Mode::Simulation => context
                .rng
                .borrow_mut()
                .as_mut()
                .expect("simulation RNG is seeded")
                .next_u64(),
            Mode::Production => loop {
                let mut bytes = [0_u8; std::mem::size_of::<u64>()];
                getrandom::fill(&mut bytes).expect("operating-system randomness unavailable");
                let value = u64::from_ne_bytes(bytes);
                if value != 0 {
                    break value;
                }
            },
        };
        context
            .trace
            .borrow_mut()
            .record(&format_args!("Random({value})"));
        value
    })
}

pub fn random_between(low: u64, high: u64) -> u64 {
    assert!(low <= high);
    low + random_u64() % (high - low + 1)
}

pub fn random_hit(probability: crate::rng::Ppm) -> bool {
    random_u64() % 1_000_000 < u64::from(probability.0)
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
            .trace
            .borrow_mut()
            .record(&format_args!("Fault({point:?}, {result})"));
        if result {
            *context.fault_hits.borrow_mut().entry(point).or_default() += 1;
        }
        result
    })
}

pub struct Delay(Pin<Box<dyn Future<Output = ()>>>);

pub fn delay(nanoseconds: u64) -> Delay {
    if nanoseconds == 0 {
        return Delay(Box::pin(tokio::task::yield_now()));
    }
    let duration = with_context(|context| match context.mode {
        Mode::Simulation => Duration::from_millis(nanoseconds),
        Mode::Production => Duration::from_nanos(nanoseconds),
    });
    Delay(Box::pin(tokio::time::sleep(duration)))
}

impl Future for Delay {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

pub struct YieldNow(Pin<Box<dyn Future<Output = ()>>>);

pub fn yield_now() -> YieldNow {
    YieldNow(Box::pin(tokio::task::yield_now()))
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cancelled;

pub struct TaskHandle<T> {
    task: TaskId,
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> TaskHandle<T> {
    pub const fn id(&self) -> TaskId {
        self.task
    }

    pub fn cancel(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }

    pub fn detach(mut self) -> TaskId {
        self.handle.take();
        self.task
    }

    pub(crate) fn detach_abort_handle(mut self) -> (TaskId, tokio::task::AbortHandle) {
        let handle = self.handle.take().expect("task handle already consumed");
        let abort = handle.abort_handle();
        drop(handle);
        (self.task, abort)
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, Cancelled>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(handle) = self.handle.as_mut() else {
            return Poll::Ready(Err(Cancelled));
        };
        match Pin::new(handle).poll(cx) {
            Poll::Ready(Ok(value)) => {
                self.handle.take();
                Poll::Ready(Ok(value))
            }
            Poll::Ready(Err(_)) => {
                self.handle.take();
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

struct TaskGuard(Rc<Cell<usize>>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

struct Scoped<F> {
    context: RuntimeContext,
    task: TaskId,
    count_polls: bool,
    trace_polls: bool,
    future: Pin<Box<F>>,
}

struct PollRecord {
    time: u64,
    task: TaskId,
}

impl fmt::Debug for PollRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Poll({}, {})", self.time, self.task)
    }
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let context = self.context.clone();
        let started = context.poll_observer.as_ref().map(|_| TokioInstant::now());
        if self.count_polls {
            let sequence = context.poll_sequence.get();
            context.poll_sequence.set(sequence.saturating_add(1));
        }
        if self.trace_polls {
            context.trace.borrow_mut().record(&PollRecord {
                time: now_for(&context),
                task: self.task,
            });
        }
        let _entered = enter(context);
        let result = self.future.as_mut().poll(cx);
        if let (Some(observer), Some(started)) = (&self.context.poll_observer, started) {
            observer(nanos(started.elapsed()));
        }
        result
    }
}

fn scoped<F: Future>(
    context: RuntimeContext,
    task: TaskId,
    count_polls: bool,
    trace_polls: bool,
    future: F,
) -> Scoped<F> {
    Scoped {
        context,
        task,
        count_polls,
        trace_polls,
        future: Box::pin(future),
    }
}

fn next_task(context: &RuntimeContext) -> TaskId {
    let task = context.next_task.get();
    context.next_task.set(task.saturating_add(1));
    task
}

fn spawn_with<F, T>(context: &RuntimeContext, future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    let task = next_task(context);
    context.live_tasks.set(context.live_tasks.get() + 1);
    let live_tasks = Rc::clone(&context.live_tasks);
    let trace_polls = context.trace_task_polls.get();
    let handle = tokio::task::spawn_local(scoped(
        context.clone(),
        task,
        true,
        trace_polls,
        async move {
            let _guard = TaskGuard(live_tasks);
            future.await
        },
    ));
    TaskHandle {
        task,
        handle: Some(handle),
    }
}

/// Spawn a child actor on the current Tokio `LocalSet`.
pub fn spawn<F, T>(future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    with_context(|context| spawn_with(context, future))
}

/// Install deterministic actor hooks around a future already running on Tokio.
///
/// Turmoil simulations use this entry point so their per-host Tokio runtime is
/// the only scheduler and clock in the process.
pub async fn simulation_scope<F, T>(seed: u64, faults: FaultConfig, future: F) -> T
where
    F: Future<Output = T>,
{
    SimulationContext::new(seed, faults).scope(future).await
}

/// Production actor hooks for futures running on an application-owned Tokio
/// `LocalSet`.
#[derive(Clone)]
pub struct ProductionContext {
    context: RuntimeContext,
}

impl ProductionContext {
    pub fn new(poll_observer: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self {
            context: context(Mode::Production, None, Some(Arc::new(poll_observer))),
        }
    }

    pub async fn scope<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        scoped(
            self.context.clone(),
            next_task(&self.context),
            true,
            false,
            future,
        )
        .await
    }
}

/// Deterministic hooks shared by successive process runs of one simulated host.
///
/// Turmoil destroys a host's Tokio runtime on crash. Keeping these hooks outside
/// that runtime preserves the host's RNG stream, trace, and fault coverage when
/// Turmoil later constructs a fresh process run.
#[derive(Clone)]
pub struct SimulationContext {
    context: RuntimeContext,
}

impl SimulationContext {
    pub fn new(seed: u64, faults: FaultConfig) -> Self {
        let context = context(Mode::Simulation, Some(seed), None);
        *context.faults.borrow_mut() = faults;
        Self { context }
    }

    /// Keep semantic observations, RNG choices, and fault decisions in the
    /// replay hash while excluding Tokio poll order. Turmoil's socket internals
    /// may wake equivalent tasks in a different order across fresh simulations.
    #[must_use]
    pub fn semantic_trace_only(self) -> Self {
        self.context.trace_task_polls.set(false);
        self
    }

    pub async fn scope<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        scoped(
            self.context.clone(),
            next_task(&self.context),
            true,
            self.context.trace_task_polls.get(),
            future,
        )
        .await
    }

    pub fn trace_hash(&self) -> u64 {
        self.context.trace.borrow().finish()
    }

    pub fn fault_hits(&self) -> BTreeMap<FaultPoint, u64> {
        self.context.fault_hits.borrow().clone()
    }

    /// Replace the fault schedule shared by all current and future scopes for
    /// this simulated host.
    pub fn set_fault_config(&self, faults: FaultConfig) {
        *self.context.faults.borrow_mut() = faults;
    }

    pub fn now(&self) -> u64 {
        now_for(&self.context)
    }

    pub fn polls(&self) -> u64 {
        self.context.poll_sequence.get()
    }
}

/// Drain currently runnable local actors without advancing a paused clock.
pub async fn run_ready() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

/// Advance a paused simulation clock to the absolute millisecond horizon.
pub async fn advance_to(horizon: u64) {
    delay(horizon.saturating_sub(now())).await;
    run_ready().await;
}

pub fn simulation_trace_hash() -> u64 {
    with_context(|context| context.trace.borrow().finish())
}

pub fn simulation_polls() -> u64 {
    with_context(|context| context.poll_sequence.get())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Simulation,
    Production,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{ProductionContext, SimulationContext, delay, observe, random_u64};
    use crate::FaultConfig;

    async fn simulation_trace(seed: u64) -> u64 {
        let context = SimulationContext::new(seed, FaultConfig::default());
        context
            .scope(async {
                observe(&"begin");
                let _ = random_u64();
                delay(1).await;
            })
            .await;
        context.trace_hash()
    }

    #[tokio::test(start_paused = true)]
    async fn simulation_trace_is_replayable_and_seeded() {
        assert_eq!(simulation_trace(7).await, simulation_trace(7).await);
        assert_ne!(simulation_trace(7).await, simulation_trace(8).await);
    }

    #[tokio::test]
    async fn production_observer_sees_task_polls_and_has_nonzero_randomness() {
        let polls = Arc::new(AtomicU64::new(0));
        let poll_totals = Arc::clone(&polls);
        let context = ProductionContext::new(move |nanoseconds| {
            poll_totals.fetch_add(nanoseconds.saturating_add(1), Ordering::Relaxed);
        });

        context
            .scope(async {
                assert_ne!(random_u64(), 0);
                delay(1_000_000).await;
            })
            .await;

        assert!(polls.load(Ordering::Relaxed) > 0);
    }
}
