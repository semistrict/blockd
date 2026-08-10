# Async actor migration follow-up

Status legend: `[ ]` pending, `[~]` validated/in progress, `[x]` fixed and verified, `[-]` invalidated with evidence.

## Core correctness

- [x] Publish the initial backed head before acknowledging creation; add crash/recovery coverage.
- [x] Validate the complete segment closure before selecting a local recovery record; add missing-segment coverage.
- [x] Derive recovery coverage only from usable history; add an unusable-newer-record regression.
- [x] Allocate and persist a stash assignment for peer-stashed forks; add fork/sync coverage.
- [x] Validate a fork base before claiming its durable head; add failed-fork retry coverage.
- [x] Serialize replica-head publication; add overlapping-publication coverage.
- [x] Accept migration acknowledgements only from the selected destination; add wrong-peer coverage.
- [x] Bind page and leaf fetch replies to their expected peer; add wrong-peer coverage.
- [x] Release migration state on reservation failure; add retry coverage.
- [x] Release hydration commit state on reservation failure; add retry coverage.
- [x] Bound source-page eviction work per executor turn; add bounded-poll coverage if observable.
- [x] Roll back database commit state and its lease on failed writes/reservations; add retry coverage.
- [x] Keep the newest replica-upload completion under out-of-order delivery; add ordering coverage.

## Storage and runtime

- [x] Make object-store GC memory-bounded instead of loading every payload; add a large-object test.
- [x] Bound peer ingress or apply transport backpressure; add overload coverage.
- [x] Implement complete prefix listing for GCS and S3 stores; add backend unit coverage where practical.

## Simulator and verification

- [x] Restore store-backed sets after local recovery reports unrestorable; add lifecycle coverage.
- [x] Require exact checkpoint state in the recovery oracle; add a mismatched-history regression.
- [x] Run scripted workloads through configured schedules and horizon semantics; add schedule coverage.
- [x] Let stateful fault actors unwind before drain; add outage/crash-at-horizon coverage.
- [x] Coalesce overlapping crash/restart requests; add single-host-actor coverage.
- [x] Record write metrics at write time rather than from final live blobs; add deletion/supersession coverage.
- [x] Target the newest journal copy for scheduled record rot; add deterministic selection coverage.
- [x] Populate scripted workload completion metrics; extend workload report assertions.

## Review fixes

- [x] Remove the retired event/effect interpreter vocabulary from runtime telemetry.
  - Attribute loop time to actor polls and async world operations, including exported metric phase labels.
- [x] Preserve local blobs for unrestorable vsets that have an intact outbound handoff marker.
  - Added recovery coverage for an unusable source plus durable handoff.
- [x] Prevent production timers from starving behind continuously ready actors.
  - Added executor coverage with a self-waking actor and a manually advanced production clock.
- [x] Re-register bounded-channel senders whose previous wake was consumed before capacity was available.
  - Added a deterministic sender wake-race regression.
- [x] Protect object-store artifacts belonging to an in-flight publication from garbage collection.
  - Added a durable pending-manifest root used by direct and replica-backed publishers, plus GC coverage beyond the normal grace period.
- [x] Keep recovery metadata-only for immutable segment payloads.
  - Added recovery coverage that rejects startup payload reads.

## Final verification

- [x] Format and lint changed code.
- [x] Run focused regression tests.
- [x] Run the full workspace test suite and configured integration/Linux lanes.
- [x] Run pre-ship review, resolve findings, commit, push, and open a pull request to `main`.
- [x] Re-run the full workspace suite and pre-ship review for the review fixes above.

## Remaining actor-model migration

These items complete the move from the former event/effect interpreter to
typed async request flows. Work in phase order unless an item explicitly says
it is independent.

### Invariants and boundaries

- [x] Preserve deterministic FIFO wake ordering, declaration-order selection,
  virtual-time behavior, trace hashing, and cancellation-tree semantics in
  `crates/exec`.
- [x] Keep transport and idempotency identifiers only where they are domain
  requirements: peer messages, guest database wire requests, and checkpoint
  retry identity. Do not use those identifiers for in-process reply routing.
- [x] Preserve the synchronous runtime API where the Firecracker/VFS boundary
  requires it; make each synchronous call block on its own completion rather
  than a shared reply stream.
- [x] Preserve the documented at-least-once peer protocol, retry behavior,
  durability ordering, crash cuts, and simulation/production equivalence.
- [x] Do not introduce an OS thread, device, or provisioned-size allocation per
  vset. Validate any per-vset actor/timer design against the 10,000-live-guest
  target in R1.3.

### Phase 0 — Lock down known regressions

- [x] Add a failing-then-fixed concurrency regression for the admin reply
  demultiplexer in `crates/runtime/src/actor_host.rs`.
  - Exercise two simultaneous requests whose replies arrive in the opposite
    order.
  - Assert that both callers receive their own reply without waiting for the
    30-second timeout.
- [x] Restore and test successful migration pause completion.
  - Cover the lifecycle from `expect_pause` through `GuestMem::pause` and a
    successful `MigratedOut` result.
  - Assert that `pause_in_flight` is cleared and the migration pause metric is
    observed exactly once.
- [x] Add characterization coverage for child completion, failure, and
  cancellation before changing `TaskSet`.
  - Include the quiescent case where a burst of children completes and no new
    ingress arrives to trigger manual reaping.
  - Confirm that dropping a parent still cancels every live descendant.
- [x] Add source-isolation tests before changing ingress scheduling.
  - A slow database request on one vset must not block another vset.
  - A slow peer storage operation must not block an unrelated acknowledgement.
  - A slow sync capture must not block ingestion of a sync for another vset.

### Phase 1 — Build typed actor primitives

- [x] Introduce a typed request envelope using the executor's one-shot
  primitive, analogous to a request carrying its own reply promise.
  - Support `Result<T, E>` replies and cancellation when the caller drops.
  - Keep the envelope local to the actor runtime; adapters remain responsible
    for transport identifiers.
- [x] Replace `TaskSet`'s manual `reap()` protocol with a structured actor
  collection that concurrently observes additions, completions, and failures.
  - Do not retain completed child handles while the parent is otherwise idle.
  - Preserve subtree cancellation on drop.
  - Allow child failures to reach the supervising actor.
- [x] Define typed actor errors.
  - Add an administrative error type instead of `AdminFailed` without context.
  - Add a host-fatal error type for failures that terminate the actor tree.
  - Replace `Result<_, ()>` where callers need to distinguish retryable,
    rejected, fenced, stale, unavailable, and fatal outcomes.
- [x] Decide and document the split between request replies and unsolicited
  lifecycle events.
  - Recovery and inbound-migration notifications belong on a dedicated event
    stream.
  - Operation completion belongs to the request's typed reply capability.

### Phase 2 — Remove manual in-process correlation

- [x] Migrate every administrative operation to typed request/reply flow.
  - Cover create, checkpoint, keep/delete base, restore, migrate out, database
    attach, begin detach, and finish detach.
  - Remove all 49 production `AdminIo::reply_admin` call sites from core
    operation bodies.
  - Remove the runtime `admin_backlog`, shared reply receiver, predicate-based
    `wait_admin`, and per-operation `AdminReply` matching.
  - Retain checkpoint retry identity independently of completion routing.
- [x] Separate administrative lifecycle events from administrative replies.
  - Route `VsetRecovered` and `VsetMigratedIn` through the lifecycle stream.
  - Update production supervision and simulation orchestration to consume the
    same event API.
- [x] Migrate database request completion to typed internal replies.
  - Make the core database operation return a typed result.
  - Remove `AdminIo::reply_database` and the runtime `database_waiters` table.
  - Attach and restore the guest's wire `ReqId` only in the runtime transport
    adapter; remove internal `DatabaseReply::with_req` routing.
- [x] Migrate guest sync completion to a request-owned reply capability.
  - Replace `GuestMem::sync_ok`, `GuestMem::sync_failed`, and the raw request ID
    stored in `VsetState::pending_syncs`.
  - Remove production and simulation `sync_waiters` maps.
  - Preserve the mutation barrier and durability-before-ack rules.
- [x] Encapsulate peer RPC correlation in a typed peer-client/broker actor.
  - Move peer request-ID allocation, reply matching, timeout cleanup, retry,
    and source authentication behind typed request futures.
  - Remove `peer_pages`, `peer_leaves`, `migration_accepts`,
    `replica_status_waiters`, `replica_put_waiters`, and
    `replica_commit_waiters` from `HostState`.
  - Preserve correlation IDs on the network wire and preserve idempotent
    handling of duplicated or late replies.
- [x] Replace simulator-side request tables and reply polling with the same
  typed client APIs used by production.
  - Remove the `Request`/`Control.requests` dispatcher in
    `crates/sim/src/actor_cluster.rs`.
  - Remove raw `AdminReply` polling and matching in
    `crates/sim/src/actor_harness.rs` except for dedicated lifecycle events.

### Phase 3 — Propagate failures through the actor tree

- [x] Change core actors that can fail fatally to return typed errors instead
  of invoking `AdminIo::abort`.
  - Remove all 48 production `AdminIo::abort` call sites.
  - Preserve the exact crash/fault-injection cut points used by simulation.
  - Let the root supervisor translate a host-fatal error into production
    termination or deterministic simulated host failure.
- [x] Make spawned child failures observable by their supervising actor.
  - Define which source/supervisor failures terminate the host, fence one
    vset, reject one request, or are retried.
  - Add coverage proving a child cannot fail silently or continue after a
    fatal result.
- [x] Replace generic administrative failure and unit-error paths with typed
  errors while preserving stable guest/database error mappings.
- [x] Review the remaining former output effects, especially `GuestMem::fence`,
  and place terminal policy at the owning supervisor rather than in unrelated
  world callbacks.

### Phase 4 — Give actors explicit scheduling and ownership

- [x] Remove head-of-line blocking from source actors.
  - Route database requests by vset so unrelated databases can progress while
    preserving per-vset mutation order.
  - Dispatch peer work by peer/vset/replica key so storage I/O cannot block
    acknowledgements or unrelated messages.
  - Dispatch sync work without awaiting a full capture in the global source.
- [x] Replace the `begin_detach_database` `yield_now` polling loop with an
  awaited state-change notification.
- [x] Replace the implicit vset operation flag matrix with typed ownership or
  serialized per-vset operations.
  - Eliminate invalid combinations involving `commit_running`,
    `checkpoint_running`, `migration_running`, `publishing`, `replicating`,
    `drain`, and `pending_verdict`.
  - Preserve cancellation cleanup currently provided by capture, commit,
    migration, publication, and replication leases.
- [x] Replace the host-wide periodic polling scheduler.
  - Stop serially awaiting capture and hydration for every vset on every tick.
  - Avoid spawning replication/publication/release retry actors for every
    vset when no work is pending.
  - Drive work from per-vset state changes and bounded timer queues while
    retaining writeback cadence, fairness, wedge accounting, and deterministic
    simulation.
- [x] Parallelize independent backed-vset reconciliation during startup with a
  bounded actor collection; preserve deterministic outcome ordering where it
  is externally observable.

### Phase 5 — Make the async boundary idiomatic and explicit

- [x] Replace `async_trait(?Send)` on the statically dispatched core world
  traits with native async trait methods.
  - Update production, simulation, and test implementations.
  - Remove the core/runtime/simulation dependency where no remaining dynamic
    trait object requires it.
  - Confirm hot fault-path world calls no longer allocate boxed futures.
- [x] Give fallible guest-memory operations an explicit error contract, or
  document and type their process-fatal contract.
  - Remove hidden termination from `fault_response` when the failure can be
    propagated to the host supervisor.
  - Preserve the mandatory fatal behavior for an unservable guest page.
- [x] Keep direct awaited `Blobs` and `Store` operations as the exemplar for
  world-boundary APIs; do not reintroduce I/O IDs or completion callbacks.
- [x] Remove obsolete protocol variants, waiter fields, reply helpers, imports,
  telemetry labels, and dependencies after all call sites have migrated.
- [x] Update `DESIGN.md` to describe typed request envelopes, lifecycle-event
  streams, actor error propagation, peer RPC encapsulation, per-vset ownership,
  and timer scheduling.

### Migration completion gates

#### Expanded pre-ship review

- [-] Hydration creates unbounded fetch fanout.
  - Invalidated: `hydrate_tail` applies `.take(HYDRATE_BATCH)` and
    `HYDRATE_BATCH` is 64, so no poll can create more than 64 fetch children.
- [x] Bound peer page/leaf storage work and defer overload to the caller retry.
  - Fixed-size keyed worker routes replace a task per request; overload
    coverage proves pressure cannot become a false missing-data response.
- [x] Restore the exact published replica commit during backed recovery.
  - Recovery reads the durable manifest when it differs from the newest local
    record; regression coverage preserves the release watermark across restart.
- [x] Bound database routing state and queue growth.
  - A bounded fair keyed queue replaces permanent per-vset actors; overload is
    returned as `DatabaseError::Busy` and hash-colliding vsets remain isolated.
- [x] Bound replica-message routing state and queue growth.
  - Fixed-size keyed worker routes replace permanent per-assignment actors;
    transport retry remains responsible for messages rejected under pressure.

#### Final expanded review

- [-] Capture cancellation re-borrows `HostState` while a mutable borrow lives.
  - Invalidated: `CaptureLease::drop` explicitly calls `drop(host)` before the
    final scheduling borrow; cancellation coverage exercises this cleanup.
- [x] Do not turn transient peer storage-route overload into missing data.
  - Defer the overloaded request so the peer client's existing timeout retries
    it; regression coverage rejects an early `None` response.
- [x] Bound periodic scheduled-work actor creation, not only executor polls.
  - Cap live scheduled children at 64 and leave excess vsets in the ordered
    pending set; exercise a 10,000-vset backlog.
- [x] Bound sync ingress fanout while preserving independent-vset progress.
  - Use a bounded fair keyed queue with one active sync per vset and a global
    concurrency limit; exercise a same-vset burst plus an unrelated sync.
- [x] Preserve database isolation for keys that collide under hash sharding.
  - Replace hash shards with bounded fair keyed dispatch and run the slow-I/O
    isolation regression against formerly colliding vset IDs.
- [x] Let in-flight guest faults finish during simulator horizon drain.
  - Stop new fault generation first, coalesce duplicate page-fault ingress,
    then cancel guests only after the host has resolved pre-horizon operations.
- [x] Rotate bounded scheduled-work admission fairly across vsets.
  - Preserve deterministic order while preventing continuously active low IDs
    from starving pending sync durability work on higher IDs.
- [x] Resume a paused compute guest after every recoverable migration failure.
  - Cover failure after capture/reservation and preserve the intentional paused
    state only once the durable handoff succeeds.
- [x] Schedule hydration before an outbound migration awaits its tail waiter.
  - Ensure an idle/read-only inbound vset can hydrate and release its source
    without requiring unrelated guest activity.
- [x] Bound and serialize peer lifecycle handlers by vset.
  - Route `MigrateOffer` and `Released` through a bounded fair keyed queue,
    coalescing duplicates while one operation for the vset is pending/active.
- [x] Refresh a locally held durable-head claim before accepting a returning
  migration.
  - Allocate a new store-version fence so residue from an interrupted earlier
    residency cannot reuse immutable journal, segment, or leaf names.
- [x] Bound sync requests through the durability acknowledgement, not merely
  through local capture admission.
  - Keep the total admitted sync backlog capped while replica durability is
    unavailable, and resume ingress when replies resolve or are dropped.
- [x] Refill scheduled work as bounded children complete within one cadence.
  - Give every vset present at the cadence boundary one fair admission chance
    without waiting another full writeback interval or blocking other host
    maintenance on the refill loop.
- [x] Release database mutation ownership on counter overflow.
  - Perform fallible counter planning before claiming the mutation slot and
    cover overflow followed by a successful retry.
- [x] Prevent stalled maintenance attempts from monopolizing scheduled-work
  capacity.
  - Ensure an unavailable peer or store cannot occupy all 64 shared slots and
    starve unrelated captures, sync durability, releases, or archive work.
- [x] Release sync admission when the external caller cancels.
  - Observe reply-target cancellation, remove abandoned durability waiters,
    and admit later syncs without waiting for replica recovery.
- [x] Preflight every database sequence and segment successor before I/O.
  - Reject overflow before immutable blobs are written so retry and recovery
    cannot observe a mutation that its caller was told had failed.
- [x] Count only scheduler admissions that actually receive child capacity.
  - A multi-work vset must not cause later IDs to be marked examined and then
    skipped on every cadence after they are reinserted without an actor.
- [x] Make multi-segment ID allocation fallible before an invalid ID is built.
  - Cover rotation at the end of the `u64` segment namespace as well as the
    existing one-segment successor check.
- [x] Include the authenticated target host in every replica RPC waiter key.
  - Concurrent status, put, and commit calls for equivalent replica metadata
    on different peers must retain and resolve independent waiters.
- [x] Apply backpressure instead of dropping replica protocol messages when a
  bounded route is saturated.
- [x] Bound concurrent administrative handlers so stalled operations cannot
  create an actor per ingress request.
- [x] Preserve every concurrent waiter for equivalent replica RPCs to the same
  authenticated peer.
- [x] Preserve scheduler completion wakeups until actor capacity is visibly
  reaped; never sleep with scheduled work and no future notifier.
- [x] Isolate replica-route backpressure with bounded per-shard deferral so a
  busy shard cannot immediately stall unrelated peer replies.
- [x] Keep the global peer ingress non-blocking when a bounded replica shard is
  exhausted; route reply messages inline and make retryable request overload
  explicit.
- [-] Preserve lifecycle messages instead of coalescing or rejecting them at
  the bounded keyed queue.
  - Invalidated: migration offers retry until `MigrateAccept`; hydration sends
    `Released` on every scheduled empty-tail pass; and `release_source`
    acknowledges duplicate releases even after the source vset is removed.
- [x] Bound guest-fault actors and apply backpressure before accepting another
  fault from the runtime.
- [x] Resolve outbound-migration hydration waiters on failed hydration instead
  of leaving the admin call pending until a later successful retry.
- [x] Resume a guest paused for an administrative checkpoint on every capture
  failure before the normal early-resume point.
- [-] Consume scheduler completions continuously after all vsets present at a
  cadence boundary received their fair admission chance.
  - Invalidated: child completion deliberately schedules follow-on
    replication/publication work for the next writeback cadence; the same-
    cadence contract applies to the bounded boundary snapshot, whose remaining
    count is already drained on completion notifications.
- [x] Make paused checkpoint and migration guests cancellation-safe until
  early checkpoint resume or durable outbound cutover disarms cleanup.
- [x] Dispatch simulator recovery events through bounded per-vset actors so a
  slow restore cannot block unrelated vsets.
- [x] Retry recoverable simulator restore failures after the one-shot recovery
  event has been consumed.
- [x] Coalesce scheduled simulator checkpoints to one outstanding request per
  vset and own reply tasks in a bounded actor collection.
- [x] Bound outbound migration offer waiting, complete the admin request after
  one attempt, and retain scheduler-owned durable reoffers until acceptance.
- [x] Bound scheduled-work completion notifications with explicit active-child
  accounting so stale wakeups cannot leak or be lost across cadences.
- [-] Preserve write escalation when simulator page faults are coalesced.
  - Invalidated: every awakened waiter re-checks residency and write
    protection in `SimWorld::fault`; a writer behind a read fill therefore
    submits an unprotecting write fault after the read fill completes.

- [x] Search confirms no production core call sites remain for
  `AdminIo::reply_admin`, `AdminIo::reply_database`, `AdminIo::abort`,
  `GuestMem::sync_ok`, or `GuestMem::sync_failed`.
- [x] Search confirms `HostState` and runtime/simulation adapters contain no
  in-process reply-routing maps keyed by `ReqId` or peer request ID.
- [x] Search confirms core operation functions do not accept a `ReqId` solely
  to select a later callback; retained IDs have a documented wire or
  idempotency purpose.
- [x] No source actor awaits unrelated long-running work inline, and no actor
  uses an unbounded `yield_now` state-polling loop.
- [x] Completed child actors are reaped while ingress is idle, child errors are
  supervised, and dropping the root cancels the complete actor subtree.
- [x] Concurrency regressions cover out-of-order replies, late replies,
  cancellation, timeouts, duplicate peer delivery, and independent-vset
  progress.
- [x] Scale coverage demonstrates bounded work and actor creation per
  writeback interval at the 10,000-live-vset planning target.
- [x] Format and lint the workspace with warnings denied.
- [x] Run focused executor, core actor, runtime, simulator, migration,
  replication, database, checkpoint, and recovery tests.
- [x] Quiesce and fully drain the simulator guest actor before submitting an
  outbound migration, so no in-flight operation can cross the capture cut.
  - Preserve the latest completed-operation VM state at the cut and add a
    regression that rejects any guest completion after quiescing begins.
- [x] Release simulator recovery concurrency slots between retryable restore
  attempts and fairly re-admit delayed retries behind unrelated queued vsets.
- [x] Globally bound simulator checkpoint requests and child actors, retaining
  excess ready vsets in a fair queue refilled by completion notifications.
- [x] Reject migration and checkpoint dispatch to crashed simulator hosts;
  stale queued admin work captured empty post-crash memory under an old VM
  state in seeds 19, 429, 487, 714, 736, 826, 890, 896, and 922. Retain every
  seed as a regression.
- [x] Keep `MigrateOut` pending until the authenticated destination accept is
  observed; cancellation must release admin admission while durable scheduler
  reoffers preserve the already-cut-over source.
- [x] Preserve a newer simulator recovery event when an older restore attempt
  completes with a retryable error for the same vset.
- [-] Cancellation of `CaptureLease` does not double-borrow `HostState`: the
  guard is explicitly dropped before rescheduling, and checkpoint/migration
  cancellation regressions exercise this cleanup path without a panic.
- [x] Hydrate lazy map leaves, not only materialized remote page locations,
  before reserving an outbound migration.
- [x] Discard a deferred source recovery verdict after the destination accepts
  a migration; only apply it when ownership rolls back to the source.
- [x] Bound and coalesce cluster checkpoint requests and completion actors to
  one outstanding request per vset and a fixed global concurrency limit.
- [x] Keep capture admission blocked until cancellation has completed the
  matching guest resume, with generation-safe resume tokens.
- [x] Observe caller cancellation while a checkpoint is running and complete
  its ordered resume cleanup before re-admitting work.
- [-] Replica status is a monotonic durable attestation within one
  `(host, vset, assignment_epoch)`: a late reply can understate progress and
  cause redundant transfer, but cannot overstate durable progress or resolve
  another assignment.
- [x] Finalize an accepted migration when the destination restarts and emits a
  recovery event before its migrated-in event is consumed.
- [x] Make the production guest pause operation cancellation-safe before it
  returns the generation token to the capture guard.
- [x] Defer a destination recovery notification until the matching migration
  request outcome is known, then finalize accepted or uncertain ownership.
- [x] Unprotect every abandoned capture page before cancellation resumes the
  guest, including cancellation after the early checkpoint resume.
- [x] Apply a source restart recovery deferred while migration quiescing is
  still draining the old guest actor.
- [x] Keep host-wide disk reclaim pressure active until total local usage is
  at or below the configured data watermark.
- [x] Consume a deferred source recovery when a migration request closes and
  ownership becomes uncertain.
- [x] Bound concurrent random migration attempts without letting one slow
  guest drain block scheduling for unrelated vsets.
- [x] Wake shared mutation-slot waiters after capture, hydration, database, or
  guest-resume completion.
- [x] Roll back simulated migration state if quiescing is cancelled before the
  source submits the migration request.
- [x] Bound initial vset creation requests and reply waiters at cluster startup.
- [x] Hold each random migration concurrency slot until the administrative
  migration request actually resolves or rolls back.
- [x] Resolve ambiguous migration ownership from source recovery only when the
  recovery belongs to a post-attempt source incarnation.
- [x] Supersede active recovery handlers when a newer event for the same vset
  arrives.
- [x] Bound restore request and completion fanout after a simulated host kill.
- [x] Keep failed writeback cleanup serialized with the vset mutation slot
  until every abandoned write protection is removed.
- [x] Supersede active recovery handlers from emitted lifecycle generations
  even while the bounded recovery work queue is full.
- [x] Authenticate and correlate lazy-hydration release acknowledgements with
  the current source and destination fence.
- [x] Reject inbound migration fences occupied by any surviving local journal,
  segment, or map-leaf artifact.
- [x] Finish every initial vset creation before simulated workloads or fault
  schedules begin.
- [x] Propagate simulated unservable page failures through the same fatal error
  path as production.
- [x] Reject migration admission when the peer protocol cannot complete the
  fenced release handshake.
- [x] Start simulated guest workloads only after every initial vset creation
  has completed.
- [x] Resolve uncertain source ownership only from a post-attempt recovery
  incarnation.
- [x] Restart a rejected migration on its source only while the original
  incarnation is still up or a newer recovery verdict has reconciled it.
- [x] Allocate every inbound migration fence above both the offered fence and
  every surviving local artifact fence, even when the local namespace has
  gaps.
- [x] Roll back simulator pause bookkeeping when a pending pause future is
  cancelled before it returns a pause token.
- [x] Invalidate active simulator recovery generations whenever the host
  incarnation advances.
- [-] Schedule recovered outbound handoffs after startup.
  - Invalidated: `host_work` explicitly spawns `reoffer_outbound` for every
    recovered vset with `outbound.is_some()`, and
    `recovered_handoff_reoffers_without_resuming_the_source` covers the crash
    cut.
- [x] Rebase the simulated workload deadline and every timed schedule from the
  post-creation epoch so blocking initial creation cannot shorten the workload
  or fire elapsed faults immediately.
- [x] Boundedly scan idle archive-capable vsets when host-wide disk pressure
  begins so reclaim does not depend on unrelated per-vset work.
- [x] Preserve an active outbound migration reservation when overlapping
  hydration completes successfully.
- [x] Reject a restore completion from a host that is no longer live in the
  incarnation that accepted the request, and let its later recovery replace
  the dead placement.
- [x] Retry an orphan restore on a current live incarnation when its selected
  candidate is superseded before the restore reply is observed.
- [x] Reject superseded recovery work before it mutates oracle state or issues
  an administrative restore request.
- [x] Release stale recovery admission without leaking a bounded supervisor
  slot when its lifecycle generation has already been superseded.
- [x] Preserve release and acknowledgment for pre-upgrade v1 migrations while
  continuing to reject new migrations to v1 peers.
- [x] Correlate each migration acceptance with the exact offered fence so a
  delayed duplicate from an older same-peer handoff cannot complete a new one.
- [x] Persist the accepted source fence in migration provenance and acknowledge
  a same-source duplicate only when it matches the installed offered cut.
- [x] Introduce a negotiated v3 peer wire shape for fenced migration messages
  while preserving the exact v2 migration and peer-stash rolling-upgrade form.
- [x] Stop admitting checkpoints at the workload horizon while draining every
  already-issued single-host and cluster checkpoint reply to completion.
- [x] Reconsider scheduler work requeued at the concurrency edge within the
  current cadence instead of delaying it by a full writeback interval.
- [x] Rate-limit completed no-progress maintenance to one retry per writeback
  cadence without delaying work deferred only by the concurrency edge.
- [x] Admit recovered outbound handoff reoffers through the bounded startup
  scheduler instead of spawning one unbounded child per recovered vset.
- [x] Move post-reply graceful database drain work out of bounded admin ingress
  slots while keeping it owned by the host actor tree.
- [x] Preempt a blocked per-vset recovery actor when a newer lifecycle
  generation arrives, including cancellation of its stale restore request.
- [x] Accept the zero-fence release acknowledgement produced by an in-flight
  v1/v2 handoff after a rolling upgrade.
- [x] Recognize persisted unfenced migration provenance as legacy after the
  peer itself upgrades to the fenced protocol.
- [x] Default an unannotated production peer to the last rolling-compatible
  protocol version and require explicit capability for fenced migration.
- [x] Fail closed if a successful head CAS does not advance the durable
  ownership fence.
- [x] Express the simulator pressure budget in pages so its reclaim regression
  exercises the same lifecycle cut at 4 KiB and 16 KiB page sizes.
- [x] Replace the removed legacy Linux runtime target with the durable-replica
  end-to-end suite, which needs no separately staged Firecracker artifacts.
- [x] Remove Linux-only `Debug` requirements from closed actor request queues.
- [x] Update Linux-only shared workload support for the mandatory-durability
  vset constructor and asynchronous recovery verdict.
- [x] Restore the Linux durable-replica end-to-end test's actor-host imports
  and reuse the shared authenticated three-host runtime configuration.
- [x] Explicitly negotiate the current peer protocol in Linux test clusters so
  version-3 replica replacement and release messages are not downgraded.
- [x] Preserve control-plane head CAS availability in the Linux replacement
  test while injecting archive-data outage, allowing the replacement epoch to
  become authoritative before its durability is asserted.
- [x] Restore replacement and non-active replica network-byte accounting in
  the awaited peer RPC path, including retry attempts.
- [x] Discover and send store-covered replica releases after startup head
  reconciliation, including externally promoted passive-recovery cuts.
- [x] Run the shared Linux workload differential against authenticated
  three-host durability, retaining passive runtimes across primary crashes.
- [x] Accept both foreground peer faults and background hydration as valid
  post-copy drain progress in the live Linux migration test.
- [x] Run the Linux loop-interference profile with the mandatory authenticated
  passive-durability cluster while preserving its primary-loop measurement.
- [x] Remove obsolete effect-era world-operation telemetry slots that no
  awaited production-world call records.
- [x] Clean Linux-only request capability, terminal callback returns, and
  shared-page key typing left over from the callback runtime.
- [x] Normalize Linux-only runtime configs for strict linting on the hosted
  toolchain.
- [x] Run the full workspace test suite and every configured integration/Linux
  lane.
- [x] Run pre-ship review and resolve every migration-related correctness,
  cancellation, ordering, and performance finding before declaring the
  migration complete.
