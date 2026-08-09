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

## Final verification

- [x] Format and lint changed code.
- [x] Run focused regression tests.
- [~] Run the full workspace test suite and configured integration/Linux lanes.
- [ ] Run pre-ship review, resolve findings, commit, push, and open a pull request to `main`.
