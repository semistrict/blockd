# blockd requirements

This document defines the system contract. [DESIGN.md](DESIGN.md) records the
implementation decisions. Requirements take precedence when the two conflict.

## R1 — The unit of everything is the volume set

- **R1.1** The system manages *volume sets* (vsets), each with exactly one
  memory volume plus one or more disk volumes and maps 1:1 onto a Firecracker
  sandbox. Each vset is captured, restored, forked and migrated as its own
  consistent unit.
- **R1.2** Every whole-compute-vset consistency point covers all of its
  volumes plus the VMM's device/vCPU state (vmstate) atomically. No
  observable compute state may mix two epochs — not across volumes, not
  between memory and vmstate. A partial compute checkpoint (R3.7) declares
  memory invalid and is restorable only by cold boot.
- **R1.3** Plan for bare-metal hosts running up to **10,000 concurrently
  live guests each** — running Firecracker processes, not merely managed
  entries — with the population of non-resident vsets beyond them bounded
  only by storage. Memory volumes up to 64 GB and disk
  volumes up to 256 GB must work; the typical vset is far smaller. Provisioned disk is
  overcommitted **10–100×** against physical NVMe and provisioned memory
  **2–4×** against physical RAM — thin provisioning, proportional sharing
  (R5.3) and writeback-driven eviction (R2.4) are what make the ratios
  safe, degrading per R2.5 when they are exceeded. Per-guest idle overhead
  must price 10,000 mostly-idle live guests into one host: no cost
  proportional to provisioned size, and fixed per-vset state (memory,
  descriptors, tasks) small enough that the fleet's overhead is a rounding
  error next to one busy guest.
- **R1.4** A vset has **exactly one writer** at any instant: the host that
  holds its assignment (R6.3). No volume is ever writable from two places;
  every other party that touches a vset's data — a peer serving a fetch, a
  restore reading the archive, GC — is a reader of immutable state. All
  consistency in the system rests on this.

## R2 — Serving: the host is the page cache

- **R2.1** Compute guests run against demand-paged RAM and virtio-pmem/DAX
  disks: a vset resumes before its bytes are local and every touched page is
  faulted in on first access.
- **R2.3** Fault service targets by source: local NVMe in the ~100 µs class;
  a peer host under ~1 ms; the object store as fallback in the tens of
  milliseconds. The system must prefer sources in that order.
- **R2.4** Host memory is overcommitted across vsets. A dirty page becomes
  evictable once background writeback has made it durable on local NVMe —
  writeback runs continuously, guest-invisibly, and independently of
  checkpoints, so a vset that is never checkpointed evicts exactly as
  freely as one checkpointed constantly. A vset's working set must be
  able to refault after eviction. Eviction is kind-aware: **compute memory
  pages have strictly higher residency affinity than every storage page** —
  compute disks go first, because a guest tolerates storage latency where a
  RAM miss is a stalled vCPU. Kernel swap is forbidden on the host (it would
  bypass the accounting).
- **R2.5** Memory pressure slows vsets down, gradually — pressure never
  kills a vset and never refuses admission (the only sanctioned guest
  deaths in the system are unservable data, R8.1, and daemon death, R8.2).
  As demand exceeds RAM, fault service waits on reclamation
  (writeback-driven, R2.4) and everything
  gets proportionally slower; the host must remain stable under sustained
  overcommit, the kernel OOM killer must never be the mechanism, and
  pressure must be observable (R9.2) so the control plane can relieve it
  by placement or migration — relief is the operator's move, sacrifice is
  not the system's.
- **R2.6** The eviction policy is borrowed, not invented: which pages are
  cold is decided the way the Linux kernel's own page reclaim decides it —
  the LRU algorithm of current kernels (the multi-generational LRU),
  reused where the kernel can simply do the work and faithfully mirrored
  where it cannot. The system adds only what the kernel cannot know: the
  memory-over-disk affinity of R2.4 and the accounting of its own tiers.
- **R2.7** NVMe pressure has the same contract as memory pressure, and it
  is the harder case because memory relief drains **into** disk (R2.4).
  The reclaim ladder has two classes: everything refetchable from the
  object-store archive — hydrated cache and already-archived state alike —
  droppable to a floor; and the irreducible residue — live heads plus state
  not yet archived — genuine occupancy that only archival, migration or
  discard can move. The residue must be observable (R9.2)
  long before the device fills, with enough configured headroom that
  in-flight writeback completes. At exhaustion the coupled degradation is
  explicit: writeback stalls, therefore memory relief stalls, therefore
  vsets slow per R2.5 — slowness and loud pressure, never corruption,
  never a kill, and relief is the control plane's move exactly as in R2.5.

## R3 — Capture: checkpoints

- **R3.1** A compute checkpoint captures a coherent point-in-time of a whole vset
  while the guest keeps running, with a vCPU pause bounded by a caller-stated
  budget (default 250 ms, target < 50 ms). Nothing after the pause —
  encoding, writing, backing up — may block the guest.
- **R3.2** Compute checkpoints happen only on explicit request — there is no
  built-in cadence and never will be a requirement for one — and the
  dependency is forbidden in the other direction too: nothing in the
  system may **rely** on a checkpoint ever arriving. Writeback (R2.4),
  archival, eviction and pressure relief all run in the background
  whether a vset is checkpointed constantly or never; a never-checkpointed
  vset still recovers by cold boot at sync consistency (R3.8, R8.2). A
  checkpoint is an operation the system supports — a coherent
  point-in-time capture of a compute vset — not a mechanism it leans on.
- **R3.3** Checkpoint cost must scale with what changed since the last
  checkpoint, not with volume size — except the first capture of a
  never-checkpointed volume, which is inherently whole-written-set sized.
- **R3.4** Repeated checkpointing forever must not accrue debt: per-vset
  storage and background work must not grow with the number of checkpoints
  taken, only with live data and with explicit branch points. A vset
  checkpointed however often, for however long, costs what its data costs.
- **R3.5** A checkpoint is atomic and idempotent end to end: a crash leaves
  exactly the old or exactly the new epoch; a retried request replays its
  outcome.
- **R3.6** A checkpoint is a **local** operation: it completes — durable and
  restorable on its host — using local resources only. The object store is
  never on a checkpoint's path; copying state there is archival (R4),
  asynchronous and separate.
- **R3.7** In addition to whole-compute-vset checkpoints, the system supports
  **partial checkpoints**, driven by the guest's pmem sync operation: every
  disk volume captured at a state at least as new as its own last
  acknowledged sync — possibly newer — and memory explicitly marked
  **invalid**. A partial checkpoint carries no vmstate and claims no
  cross-volume barrier; it never feigns the coherence of R1.2, which is why
  it restores only by **cold boot** — the guest boots fresh from its disks,
  as after power loss — and the instant-resume target (R6.2) does not apply
  to it.
- **R3.8** Sync ordering is inviolable. A compute pmem sync is acknowledged to the guest
  only once its ordering is locked in: from the ack onward, crash recovery
  can never observe that disk at a state older than the sync point, and
  every captured disk state is a crash-consistent point of that device's
  write history — nothing written after a sync barrier is present unless
  everything before it is. The durability domain is the primary host plus
  its assigned passive peer as specified by R4.1; object-store archival is
  asynchronous and is not on the acknowledgment path. This does not change
  device ordering or checkpoint semantics.

## R4 — Durability: primary and passive first, object storage is the archive

- **R4.1** There is one durability contract. Every guest sync is acknowledged
  only after its recovery closure is durable on the primary and exactly one
  assigned passive peer, or after an equal or newer point is already published
  in the object store. The closure is the compressed immutable artifacts and
  record needed to cold-boot at the barrier; it is neither guest RAM nor every
  provisioned page. The peer is recovery storage, not an owner or runner.
- **R4.3** Loss of either the primary or the active passive alone loses no
  acknowledged sync. The passive holds the newest protected closure; loss of
  that passive pauses new sync acknowledgments only while a replacement is
  selected, seeded with a complete covering closure, and fenced as active.
  Repair has no finite retry, candidate-count, retired-peer, or archive-lag
  budget: repeated passive losses continue through the eligible roster.
  Simultaneous loss of the primary and every peer holding its newest protected
  closure is outside the single-node-loss contract. In that event the newest
  object-store archive may be arbitrarily old; the system neither claims nor
  configures a time-bounded catastrophic RPO. Checkpoints govern the kind of
  recovery, never its existence: recovery uses the newest available point —
  resumed if it is a whole checkpoint, cold-booted at sync consistency
  otherwise (R6.1).
- **R4.4** An object-store outage never limits sync admission by elapsed time
  or archive lag. A healthy active passive continues accepting protected writes
  indefinitely while its durable storage has capacity. Archive lag age and
  bytes remain observable cost and disaster-recovery facts, not correctness
  thresholds. Only actual passive-capacity exhaustion, including the reserved
  space needed to finish or compact an in-flight closure, may stall new
  protected work without acknowledgment. Intermediate cuts may be coalesced,
  but the latest peer-committed closure must remain complete.
- **R4.5** Nothing is ever deleted by age. Every durable artifact lives
  until an explicit delete: a vset until its discard, a base until its
  delete. Superseded state is reclaimed only when the same vset's newer
  committed state replaces it. Grace periods exist only to protect in-flight writes,
  never as retention policy.
- **R4.6** The object store is the only shared durable dependency, and the
  system requires of it only: strong read-after-write consistency,
  conditional writes (compare-and-swap by version), and objects up to
  64 MiB. The GCS client must enforce that contract.
- **R4.7** Protected and archived durability are distinct monotone frontiers.
  The protected frontier advances on passive commit and authorizes sync ACKs;
  the archived frontier advances only after the manifest and all referenced
  objects are durable and a fenced head CAS publishes that cut. Neither may
  advance from an attempted, partial, or unverifiable write.
- **R4.9** Passive retention is defined by recovery roots, not by archive age:
  the newest complete protected cut, any cut selected by the primary for
  archival, and any in-progress replacement cut. Once a newer complete cut is
  durable, artifacts referenced only by superseded cuts may be compacted into
  fresh sealed spool generations even while the object store is unavailable.
  A selected archive cut remains pinned locally and on the passive until its
  attempt finishes or is abandoned for a newer cut.
  Reclamation is explicit and crash-safe; capacity exhaustion stalls new
  protected syncs and is observable rather than evicting any recovery root.

## R5 — Lineage: bases and forks

- **R5.1** Creating a vset from a base is O(1) metadata: no bytes copied, no
  bytes moved, regardless of base size.
- **R5.2** A base is a kept recovery point, created two ways: keeping a
  vset's checkpoint, or importing a raw disk image. It is **whole**
  (memory, vmstate and disks — forks of it resume) or **disk-only** (forks
  of it cold-boot, R3.7); an image import produces the disk-only kind.
  Every base is immutable, forkable from any host, and alive until explicitly
  deleted.
- **R5.3** Sharing is proportional to divergence — in storage **and in
  memory**. Fork one base a thousand times and let every fork modify a
  little of every volume: the host stores the base once, keeps **one
  physical copy of every unmodified base page, RAM included**, and each
  fork pays only for what it changed. Total consumption is the base plus
  the sum of divergences — never the base times the fork count. This holds
  locally (NVMe and host memory) and in the object-store archive.
- **R5.4** Naming (which base is "python-3.13") lives outside the system;
  the storage layer deals in ids only.

## R6 — Restore and placement

- **R6.1** Any vset can be brought back on any host of the
  cluster from the object store alone, at its newest archived recovery
  point (R8.2) — restored if that point is a whole checkpoint, cold-booted
  at sync consistency otherwise: no prior local state, no reachable
  previous host, and no requirement that a checkpoint was ever taken. A
  vset whose newest protected sync is ahead of that point instead
  requires a complete verified closure from its recorded stash assignment, or
  from a higher assignment epoch carrying a commit by the recorded holder's
  current writer fence (R6.6); it must never silently present the older
  object-store point as satisfying the stronger guarantee.
- **R6.2** A compute restore onto a host with none of the vset's bytes reaches the
  guest's first instruction in under 200 ms from a warm object store,
  independent of vset size — by fetching only what the resume touches (a
  recorded resume set prefetched, the rest on demand). ("Restore" always
  means resuming a whole-vset point; booting a disk-only point is a *cold
  boot*, R3.7, and carries no latency target.)
- **R6.3** The system itself is the authority for which host runs a vset —
  no consensus service, no trusted control plane. The instrument is the
  object store's conditional head: two hosts racing to restore one vset
  resolve to exactly one runner by CAS alone. Migration preserves the same
  exclusion by making the handoff durable on both sides before either acts.
- **R6.4** Durable state can never fork: a fenced former holder must be
  structurally unable to publish, and its guest must stop within a bounded,
  configured time. Failover after suspected host death must be safe to
  attempt at any moment — a wrong liveness guess costs a bounded double-run
  window, never divergent durable state. Every timing bound here rests
  only on local monotonic clocks with bounded drift; nothing anywhere
  requires synchronized clocks across hosts.
- **R6.5** The control plane's only obligations are liveness policy (when to
  claim), placement preference, roster and certificates, and never reusing a
  vset id.
- **R6.6** The fenced per-vset head is also the durable authority for the one
  active stash assignment and any in-progress replacement. Health observations
  and deterministic virtual-node rankings are placement inputs, not authority.
  An assignment change is a rare conditional head update and never enters the
  steady-state guest sync path. During a healthy-store replacement the head
  names both the old active peer and the single transition peer. During a
  complete store outage, the existing holder may provisionally advance to a
  higher assignment epoch, seed exactly one deterministic replacement at a time, and
  activate it after a complete commit. That residue is authoritative only when
  its commit and journal carry the holder's current writer fence; recovery may
  inventory such higher epochs and must reject every stale-fence residue. The
  provisional assignment is reconciled by head CAS when the store returns.
  Assignment epochs map cyclically across eligible candidates rather than
  consuming a finite roster, and a covering activation supersedes obsolete
  retired-peer authority so an unreachable former peer cannot exhaust future
  failovers.

## R7 — Migration

- **R7.1** Migration is post-copy: cut over first, fetch the remainder. It
  pauses the guest for under 500 ms; the destination resumes and demand-faults
  the tail.
- **R7.2** Migration is served peer-to-peer while the source lives, with the
  same at-most-one-runner guarantee, transferred explicitly and durably on
  both sides. Its final handoff cut is an explicit archive event.
- **R7.3** Source death mid-migration recovers from the newest complete
  protected or archived cut, never a corrupt or half-migrated survivor.

## R8 — Integrity and failure behavior

- **R8.1** The system never serves bytes it cannot vouch for. Every unit
  read from disk, a peer, or the object store is checksum-verified before a
  guest can observe it; a failed check means try another source, and an
  exhausted plan means one guest fails. The system must not substitute zeros
  or other invented data for a lost page.
- **R8.2** The daemon's failure mode is dying loudly. On unrecoverable local
  faults (journal device failure, invariant violations) the process aborts;
  guests hang on unserved faults and are killed by the node manager. No
  runtime state survives a daemon crash; recovery is by durable state only,
  each vset to its newest committed **recovery point** — a whole-vset
  checkpoint, which resumes, or a disk-only state honoring sync ordering
  (R3.8), which cold-boots like a normal VM after power loss. Continuous
  writeback keeps producing the disk-only kind, so a never-checkpointed
  vset still recovers with at most sync-bounded loss. Each vset gets an
  explicit verdict (restorable / quarantined / unrestorable).
- **R8.3** An object-store outage stalls archival and the cold path, never
  local durability: writeback and checkpoints complete locally (R3.6) and
  archive writes queue,
  with explicit backpressure before any loss of queued work. A guest
  stalls only if it genuinely needs bytes that exist nowhere but the
  store — mid-hydration after a cold restore, or an evicted page with no
  local or peer copy — which is fault-path reality, not a durability
  failure; a vset whose working bytes are on its host or a peer runs
  through the outage untouched, and the source-preference order (R2.3)
  is what keeps that population large. A healthy passive
  lets guest syncs continue through the outage while observable residue is
  compacted around the live recovery roots. Loss of that passive stalls new
  sync acknowledgments only until automatic replacement completes; it never
  permits an optimistic acknowledgment.
- **R8.4** Bytes are stored compressed on local disk and in the object store.
  Primary-to-passive transfer preserves the source `.blx` bytes verbatim;
  an archive cycle may decode selected live entries and recompress them into
  fewer bounded `.blx` files. The current storage format deliberately keeps the
  existing LZ4 entry codec. Zstd, zero-page elision, or another format version
  requires a measured incremental win after temporal packing, plus updated
  byte-pin and bit-flip suites; it is not implied by this contract. The fault
  path still decompresses at most one small entry. Compression is part of what
  makes R1.3's disk overcommit real capacity rather than accounting.

## R9 — Operability

- **R9.1** One daemon per host, one bucket-and-prefix per cluster (the pair
  is cluster identity; distinct clusters may share a bucket). Deployment
  requires a Linux host with userfaultfd (MINOR + write-protect), the
  patched Firecracker, root, and kernel swap off — nothing else stateful.
- **R9.2** The operable minimum of observability: per-vset fault latency by
  source, hydration progress, archive lag, dirty rate, memory and disk
  pressure, assignment claims and fences, and for every vset the
  active/transition peer, assignment epoch, protected-sync lag, spool bytes
  and capacity, stalled syncs, retries, integrity rejects, replacement bytes,
  and cleanup unlinks — as Prometheus series with a fixed, bounded label
  vocabulary. Steady-state bytes sent to non-active peers and bytes rewritten
  by stash cleanup are invariant counters and must remain zero. Pressure
  signals are required because they trigger capacity relief; missing signals
  under pressure are a defect.
- **R9.3** Cluster garbage collection runs as a separate process against the
  bucket alone, deletes only unreferenced objects past the in-flight grace,
  and can never delete a base, a live vset's state, or anything an explicit
  delete has not unrooted.

## R10 — Engineering constraints

- **R10.1** The distributed core must run under deterministic simulation:
  every source of nondeterminism (time, randomness, network, kernel, object
  store, crashes and torn writes) behind one seam, so any failing run
  replays byte-for-byte from its seed. This constrains production code
  continuously; it is not a test harness bolted on.
- **R10.2** All wire and storage formats are fixed-width, little-endian,
  float-free, and byte-pinned by tests, so a format break cannot pass CI
  silently. Two hosts (or two replays) encoding the same state produce
  identical bytes.
- **R10.3** Rust; dependencies OSI-licensed only, verified at adoption.
## R11 — Security and tenancy

- **R11.1** All inter-host traffic is mutually authenticated and encrypted
  (mTLS); host identities and certificates come from the control plane
  (R6.5). A peer serves data only to authenticated members of its own
  cluster.
- **R11.2** Guests are adversarial. Nothing that crosses the guest boundary
  — fault patterns, page contents, sync requests, timing — may influence
  the daemon beyond that guest's own vset, and a guest can never read
  another vset's data, with one deliberate exception: pages of a base its
  own lineage legitimately shares (R5.3). Sharing follows lineage only;
  there is no content-based sharing that could leak bytes across unrelated
  vsets.
- **R11.3** Who may fork which base is the control plane's to arbitrate,
  where naming already lives (R5.4); the storage layer enforces lineage,
  not tenancy policy.
- **R11.4** Encryption at rest is the deployment's layer (encrypted NVMe,
  bucket-side encryption); the system must work unchanged on top of both
  and requires neither. Everything in flight is covered by R11.1 and by
  TLS to the object store.
