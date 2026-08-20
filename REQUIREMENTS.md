# blockd requirements

This document defines the system contract. [DESIGN.md](DESIGN.md) records the
implementation decisions. Requirements take precedence when the two conflict.

## R1 — The unit of everything is one volume

- **R1.1** The system manages independently identified *volumes*. A volume is
  either guest memory or one block device. Every create, capture, restore,
  fork, lease, archive, and migration operation names exactly one volume;
  the storage system has no VM-level grouping identity.
- **R1.2** A memory-volume checkpoint covers that memory volume and the VMM's
  device/vCPU state (vmstate) atomically. A VM orchestrator may pause a VM and
  request snapshots of its memory and block volumes concurrently, but those
  requests remain independent and require no cross-volume epoch or commit.
- **R1.3** Plan for bare-metal hosts running up to **10,000 concurrently
  live guests each** — running Firecracker processes, not merely managed
  entries — with the population of non-resident volumes beyond them bounded
  only by storage. Memory volumes up to 64 GB and disk
  volumes up to 256 GB must work; the typical volume is far smaller. Provisioned disk is
  overcommitted **10–100×** against physical NVMe and provisioned memory
  **2–4×** against physical RAM — thin provisioning, proportional sharing
  (R5.3) and writeback-driven eviction (R2.4) are what make the ratios
  safe, degrading per R2.5 when they are exceeded. Per-guest idle overhead
  must price 10,000 mostly-idle live guests into one host: no cost
  proportional to provisioned size, and fixed per-volume state (memory,
  descriptors, tasks) small enough that the fleet's overhead is a rounding
  error next to one busy guest.
- **R1.4** A volume has **exactly one writer** at any instant: the host that
  holds its assignment (R6.3). No volume is ever writable from two places;
  every other party that touches a volume's data — a peer serving a fetch, a
  restore reading the archive, GC — is a reader of immutable state. All
  consistency in the system rests on this.

## R2 — Serving: the host is the page cache

- **R2.1** Compute guests run against demand-paged memory and virtio-pmem/DAX
  block volumes: a volume becomes usable before its bytes are local and every touched page is
  faulted in on first access.
- **R2.3** Fault service targets by source: local NVMe in the ~100 µs class;
  a peer host under ~1 ms; the object store as fallback in the tens of
  milliseconds. The system must prefer sources in that order.
- **R2.4** Host memory is overcommitted across volumes. A dirty page becomes
  evictable once background writeback has made it durable on local NVMe —
  writeback runs continuously, guest-invisibly, and independently of
  checkpoints, so a volume that is never checkpointed evicts exactly as
  freely as one checkpointed constantly. A volume's working set must be
  able to refault after eviction. Eviction is kind-aware: **memory-volume
  pages have strictly higher residency affinity than every storage page** —
  compute disks go first, because a guest tolerates storage latency where a
  RAM miss is a stalled vCPU. Kernel swap is forbidden on the host (it would
  bypass the accounting).
- **R2.5** Memory pressure slows volumes down, gradually — pressure never
  kills a volume and never refuses admission (the only sanctioned guest
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
  volumes slow per R2.5 — slowness and loud pressure, never corruption,
  never a kill, and relief is the control plane's move exactly as in R2.5.

## R3 — Capture: checkpoints

- **R3.1** A memory checkpoint captures a coherent point-in-time of one memory volume
  while the guest keeps running, with a vCPU pause bounded by a caller-stated
  budget (default 250 ms, target < 50 ms). Nothing after the pause —
  encoding, writing, backing up — may block the guest.
- **R3.2** Compute checkpoints happen only on explicit request — there is no
  built-in cadence and never will be a requirement for one — and the
  dependency is forbidden in the other direction too: nothing in the
  system may **rely** on a checkpoint ever arriving. Writeback (R2.4),
  archival, eviction and pressure relief all run in the background
  whether a volume is checkpointed constantly or never; a never-checkpointed
  volume still recovers by cold boot at sync consistency (R3.8, R8.2). A
  checkpoint is an operation the system supports — a coherent
  point-in-time capture of a memory volume — not a mechanism it leans on.
- **R3.3** Checkpoint cost must scale with what changed since the last
  checkpoint, not with volume size — except the first capture of a
  never-checkpointed volume, which is inherently whole-written-set sized.
- **R3.4** Repeated checkpointing forever must not accrue debt: per-volume
  storage and background work must not grow with the number of checkpoints
  taken, only with live data and with explicit branch points. A volume
  checkpointed however often, for however long, costs what its data costs.
- **R3.5** A checkpoint is atomic and idempotent end to end: a crash leaves
  exactly the old or exactly the new epoch; a retried request replays its
  outcome.
- **R3.6** A checkpoint is a **local** operation: it completes — durable and
  restorable on its host — using local resources only. The object store is
  never on a checkpoint's path; copying state there is archival (R4),
  asynchronous and separate.
- **R3.7** A block-volume snapshot is independent of memory and other block
  volumes. It contains no vmstate and restores as a block device for a
  **cold boot**. A memory-volume checkpoint restores by resume only when its
  own memory and vmstate are intact. Neither kind claims a cross-volume barrier.
- **R3.8** Sync ordering is inviolable. A block-volume sync is acknowledged to the guest
  only once its ordering is locked in: from the ack onward, crash recovery
  can never observe that disk at a state older than the sync point, and
  every captured state is a crash-consistent point of that device's
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
  acknowledged sync. The passive spool holds the newest committed recovery cut; loss of
  that passive pauses new sync acknowledgments only while a replacement is
  selected, seeded with a complete covering closure, and fenced as active.
  Repair has no finite retry, candidate-count, retired-peer, or archive-lag
  budget: repeated passive losses continue through the eligible roster.
  Simultaneous loss of the primary and every peer holding its newest protected
  closure is outside the single-node-loss contract. In that event the newest
  object-store archive may be arbitrarily old; the system neither claims nor
  configures a time-bounded catastrophic RPO. Checkpoints govern the kind of
  recovery, never its existence: recovery uses the newest available point —
  resumed if it is a memory checkpoint, cold-booted with block volumes at sync consistency
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
  until an explicit delete: a volume until its discard, a base until its
  delete. Superseded state is reclaimed only when the same volume's newer
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

- **R5.1** Creating a volume from a base is O(1) metadata: no bytes copied, no
  bytes moved, regardless of base size.
- **R5.2** A base is a kept recovery point for exactly one volume, created by
  keeping that volume's snapshot or importing a raw block image. A memory
  base includes its vmstate and resumes; a block base attaches for cold boot.
  An image import produces the block kind.
  Every base is immutable, forkable from any host, and alive until explicitly
  deleted.
- **R5.3** Sharing is proportional to divergence — in storage **and in
  memory**. Fork one base a thousand times and let every fork modify a
  little: the host stores the base once, keeps **one
  physical copy of every unmodified base page, RAM included**, and each
  fork pays only for what it changed. Total consumption is the base plus
  the sum of divergences — never the base times the fork count. This holds
  locally (NVMe and host memory) and in the object-store archive.
- **R5.4** Naming (which base is "python-3.13") lives outside the system;
  the storage layer deals in ids only.

## R6 — Restore and placement

- **R6.1** Any volume can be brought back on any host of the
  cluster from the object store alone, at its newest archived recovery
  point (R8.2) — resumed if it is a memory checkpoint, attached at sync
  consistency if it is a block volume: no prior local state, no reachable
  previous host, and no requirement that a checkpoint was ever taken. A
  volume whose newest protected sync is ahead of that point instead
  requires a complete verified recovery cut from its recorded replica spool,
  or from a higher assignment epoch carrying a commit by the recorded holder's
  current writer fence (R6.6); it must never silently present the older
  object-store point as satisfying the stronger guarantee.
- **R6.3** The system itself is the authority for which host runs a volume —
  no peer consensus service and no external trusted control plane. The object
  store is the sole shared control-plane authority: every membership,
  placement, session, authority, per-volume head, and replica-assignment
  transition uses a conditional operation against the exact observed object
  version. Two hosts racing to restore one volume resolve to exactly one runner
  by head CAS alone. Migration preserves the same exclusion by making the
  handoff durable on both sides before either acts.
- **R6.4** Durable state can never fork: a fenced former holder must be
  structurally unable to publish, and its guest must stop within a bounded,
  configured time. Failover after suspected host death must be safe to
  attempt at any moment — a wrong liveness guess costs a bounded double-run
  window, never divergent durable state. Every timing bound here rests
  only on local monotonic clocks with bounded drift; nothing anywhere
  requires synchronized clocks across hosts.
- **R6.5** The control plane's only obligations are liveness policy (when to
  claim), placement preference, and never reusing a volume id. Nodes generate
  ephemeral TLS identities at startup and atomically publish their public
  certificates and reachable endpoints in the cluster's object-store prefix,
  then CAS-renew that record as a liveness heartbeat. Every node continuously
  discovers and reconciles connections from live records in that directory;
  write access to the prefix is the authority to join the roster.
- **R6.6** The fenced per-volume head is also the durable authority for the one
  active stash assignment and any in-progress replacement. Health observations
  and deterministic candidate rankings are placement inputs, not authority.
  An assignment change is a rare conditional head update and never enters the
  steady-state guest sync path. During a healthy-store replacement the head
  names both the old active peer and the single transition peer. During a
  complete store outage, the existing assignment remains fixed: the holder may
  continue using that already-authorized peer only while its own authority
  remains valid, but replacement, activation, and every other assignment
  transition wait for the store. Peers may retain fenced transfer residue, but
  it has no control-plane authority until an exact head CAS publishes it.
  Assignment epochs map cyclically across eligible candidates rather than
  consuming a finite roster, and a covering activation supersedes obsolete
  retired-peer authority so an unreachable former peer cannot exhaust future
  failovers.

## R7 — Migration

- **R7.1** Migration is post-copy: cut over first, fetch the remainder. It
  pauses a memory volume's guest for under 500 ms; the destination resumes and
  demand-faults the tail. A block volume migrates independently without a
  guest-memory pause.
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
  each volume to its newest committed **recovery point** — a memory checkpoint,
  which resumes with vmstate, or a block-volume state honoring sync ordering
  (R3.8), which attaches for a normal cold boot. Continuous writeback keeps
  producing block-volume recovery points, so a never-snapshotted block volume
  still recovers with at most sync-bounded loss. Each volume gets an
  explicit verdict (restorable / quarantined / unrestorable).
- **R8.3** An object-store outage stalls archival and the cold path, never
  local durability: writeback and checkpoints complete locally (R3.6) and
  archive writes queue,
  with explicit backpressure before any loss of queued work. A guest
  stalls only if it genuinely needs bytes that exist nowhere but the
  store — mid-hydration after a cold restore, or an evicted page with no
  local or peer copy — which is fault-path reality, not a durability
  failure; a volume whose working bytes are on its host or a peer runs
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
- **R9.2** The operable minimum of observability: per-volume fault latency by
  source, hydration progress, archive lag, dirty rate, memory and disk
  pressure, assignment claims and fences, and for every volume the
  active/transition peer, assignment epoch, protected-sync lag, spool bytes
  and capacity, stalled syncs, retries, integrity rejects, replacement bytes,
  and cleanup unlinks — as Prometheus series with a fixed, bounded label
  vocabulary. Steady-state bytes sent to non-active peers and bytes rewritten
  by stash cleanup are invariant counters and must remain zero. Pressure
  signals are required because they trigger capacity relief; missing signals
  under pressure are a defect.
- **R9.3** Cluster garbage collection runs as a separate process against the
  bucket alone, deletes only unreferenced objects past the in-flight grace,
  and can never delete a base, a live volume's state, or anything an explicit
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
  (mTLS). Each host generates its certificate and private key in memory at
  startup, publishes only the public certificate and reachable endpoint under
  the cluster's object-store prefix, CAS-renews its record, and continuously
  refreshes that directory as its trust set and discovery source (R6.5). A peer
  serves data only to a certificate in a currently live record in its own
  cluster; anyone able to write that directory is authorized to be a node.
- **R11.2** Guests are adversarial. Nothing that crosses the guest boundary
  — fault patterns, page contents, sync requests, timing — may influence
  the daemon beyond that guest's own volume, and a guest can never read
  another volume's data, with one deliberate exception: pages of a base its
  own lineage legitimately shares (R5.3). Sharing follows lineage only;
  there is no content-based sharing that could leak bytes across unrelated
  volumes.
- **R11.3** Who may fork which base is the control plane's to arbitrate,
  where naming already lives (R5.4); the storage layer enforces lineage,
  not tenancy policy.
- **R11.4** Encryption at rest is the deployment's layer (encrypted NVMe,
  bucket-side encryption); the system must work unchanged on top of both
  and requires neither. Everything in flight is covered by R11.1 and by
  TLS to the object store.
