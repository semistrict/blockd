# blockd

Storage backend for the Firecracker sandbox fleet. Replaces JuiceFS.

One daemon per host owns all sandbox state — guest RAM and pmem disks — as
layered, page-granular volumes. Local NVMe is the primary store; S3 is async
backup and cold tier. Every byte a VM touches is served through userfaultfd,
which makes restore-from-S3, restore-from-peer, and host-to-host migration the
same code path: resume immediately, fault pages in from wherever they live.

## Goals

- Fast writes: guest writes hit local NVMe, never synchronous S3.
- Coherent checkpoints: memory + disk captured at one instant, sub-second
  pause, every ~30s, uploaded to S3 in the background. Acceptable loss on
  host death: since the last uploaded checkpoint (seconds–minutes).
- Instant migration: sub-second pause, destination resumes immediately and
  JIT-faults pages from S3 and the source host while the tail drains.
- No guest page cache: disks are virtio-pmem with DAX, so the host page
  cache is the only cache.
- e2b-class targets: 100–200 sandboxes/host, 1–8 GB RAM + 1–10 GB disk each,
  resume-to-first-instruction < 200 ms, migration pause < 500 ms.

## Non-goals

- POSIX filesystem semantics (that was JuiceFS's job; nothing needs it now).
- Live migration of running vCPUs (pre-copy). Pause-and-shift is sufficient.
- Synchronous replication. Durability beyond the last checkpoint is out.

## Architecture

```
┌──────────────────────── host ────────────────────────┐
│  node manager (Go)                                   │
│      │ gRPC over UDS (+ SCM_RIGHTS fd passing)       │
│  blockd (Rust daemon)                                │
│      ├── volume/layer store on NVMe                  │
│      ├── UFFD fault servers (one task per VM)        │
│      ├── checkpoint engine (uffd-wp dirty tracking)  │
│      ├── S3 tier (async upload, range-GET fetch)     │
│      └── peer server (gRPC + mTLS, page/layer fetch) │
│  firecracker ×N                                      │
│      ├── guest RAM     ← memfd owned by blockd       │
│      └── virtio-pmem   ← memfd owned by blockd (DAX) │
└──────────────────────────────────────────────────────┘
```

blockd is a pure storage daemon. The node manager keeps owning Firecracker
lifecycle and the control plane keeps deciding placement/migration; blockd
executes storage operations and serves faults.

### Why a daemon (not a library)

Host-wide shared state: base layers cached once per host and shared by every
sandbox forked from them, one NVMe pool, one S3 upload queue, one peer
endpoint. Hot path (fault serving, io_uring, hashing) belongs in Rust; the Go
node manager talks gRPC. Note UFFD gives no crash isolation either way — if
the fault server dies, its VMs are dead — so the daemon buys ops separation,
not safety.

## Data model

### Volumes and layers

- **Volume**: an ordered chain of layers. Two kinds, same representation:
  `mem` (guest RAM) and `disk` (pmem device). Kind only affects attach.
- **Layer**: immutable, page-granular (4 KiB) sparse diff. Produced by
  checkpoint (dirty pages since previous layer) or template build.
- **Fork**: new volume whose chain is a shared suffix plus a fresh empty
  writable head. O(1), no copying; base layers shared across all forks on
  the host and in S3.
- **Sandbox snapshot**: an atomic record binding {mem volume head, disk
  volume head(s), Firecracker vmstate blob}. This is the unit of coherence:
  restore/migrate always targets a snapshot, never a lone volume.

Both lifecycles fall out of the same model:
- ephemeral CI-style: template base + one dirty head, discarded.
- long-lived dev envs: chains grow one layer per checkpoint → background
  **compaction** merges runs of small layers (newest page wins), bounded
  chain length (target ≤ ~32). Compaction writes a new merged layer, flips
  the chain pointer, GC collects the old ones.

### Layer format

Each layer is a **manifest** plus **data segments**:

- Manifest (small, one S3 object / one local file): layer id, parent id,
  page index (sorted runs of {volume page range → segment, offset}),
  per-segment checksums, resume-set hint (see Restore).
- Data segments: page data, ~4 MiB per segment. On NVMe segments are stored
  **uncompressed** — a 4 K fault must never pay a segment-sized decompress —
  zstd applies only at the S3 and peer-transfer boundary. A 30s checkpoint
  layer is typically one segment; a 10 GB template base is thousands,
  fetched in parallel on cold hydration.

Rationale: page-granular index gives cheap tiny diffs at 30s cadence;
fixed-size segments give parallel cold fetch, ranged partial fetch
(fault → fetch only the segment containing the page), and sane S3 object
counts. No content-addressing: dedup beyond shared base layers isn't worth
per-page hashing on the write path.

### S3 layout

```
s3://<bucket>/layers/<layer-id>/manifest
s3://<bucket>/layers/<layer-id>/seg/<n>
s3://<bucket>/sandboxes/<sandbox-id>/head          # snapshot record, conditional PUT
s3://<bucket>/templates/<name>                     # named snapshot record
```

The head object IS the snapshot record: vmstate blob (small — device state
only) plus, per volume, the **full chain layer-id list**, inline. One GET
starts a cold restore and all manifests fetch in parallel; chain-walking
parent links sequentially would put chain-length × S3-RTT on the resume
path. Compaction rewrites the chain lists at its next head update.

Layer ids are ULIDs (immutable objects, never overwritten). The only mutable
objects are head/template records, updated with S3 conditional writes
(If-Match), which S3 supports natively — **no external metadata database**.
The source of truth for "what is sandbox X" is its head object; blockd's
local state and peer knowledge are caches. Each sandbox's head has a
**single writer**: the blockd currently hosting it. Migration transfers
writership explicitly (see Migration). The control plane tells blockd peer
addresses; blockd never needs consensus.

## Fault-serving mechanism

One mechanism for everything: **shared memfd + UFFD (`MISSING|MINOR|WP`)**,
for both RAM and pmem regions.

- blockd creates a memfd per attached volume (RAM: existing Firecracker
  memfd backend; pmem: the fork's device maps the fd it's given).
- Firecracker registers the mapping with userfaultfd
  (`MISSING|MINOR|WP`) and passes the uffd to blockd over the UDS (the
  existing snapshot-restore UFFD handoff, extended to pmem regions).
  `MISSING` matters: evicted (hole-punched) pages fault as missing, not
  minor.
- **Miss** (missing or minor fault): blockd resolves the page through the
  chain's merged index, `pwrite`s it into the memfd page cache,
  `UFFDIO_CONTINUE` — always with the WP flag set. **Invariant: every page
  entering a mapping arrives write-protected**; otherwise
  repopulated-then-written pages silently escape the next dirty set.
- **Dirty tracking**: wp-faults record the page as dirty and drop its
  protection; the accumulated set is the next layer. No KVM dirty-log
  dependency, identical for RAM and pmem. (Prior art: QEMU's
  background-snapshot uses uffd-wp over guest RAM the same way.)
- memfd means checkpoint reads dirty pages directly from the fd — no copies
  through the guest, and a crashed Firecracker doesn't take page contents
  with it.

Merged chain index lives in blockd memory as an interval map, built at attach
from manifests, updated on checkpoint/compaction.

### Residency: blockd is the page cache

Total attached disk far exceeds host RAM, so pages in the memfds are a
**cache**, not a copy: blockd manages residency like a page cache, with NVMe
as backing store.

This applies to **RAM volumes too**: any page whose content is captured in a
local sealed layer is clean and evictable, and after every checkpoint that
is all of guest RAM except the since-then dirty set. Cold pages of idle
sandboxes drain to NVMe — swap-via-layers — which is what makes 200 × 8 GB
nominal RAM fit on a real host; only hot working sets plus the current
dirty epoch stay resident. RAM and disk get separate watermarks (a RAM-page
refault costs a guest memory stall, so RAM evicts less eagerly).

- **Eviction**: blockd keeps an LRU over resident pages (fault traffic =
  touch). To evict a clean page it `fallocate(PUNCH_HOLE)`s it out of the
  memfd, freeing the RAM; the next guest access is a missing fault served
  from NVMe again. Global watermarks (per-host RAM budgets, RAM vs disk
  volumes) drive background eviction, kswapd-style.
- **Writeback**: a dirty page can't be punched until persisted, and waiting
  for the next checkpoint would pin up to 30 s of dirty data per sandbox.
  So each disk volume has a **writeback log** on NVMe: an unsealed,
  page-granular append log ahead of the head layer. Background writeback
  flushes dirty pages to the log (then re-arms wp and marks them clean →
  evictable). At checkpoint, the log plus remaining dirty pages seal into
  the new layer — the log is never uploaded or shared, it's host-local
  spill.
- **Read path with the log**: fault resolution checks writeback log first,
  then the sealed chain (the log is logically the newest part of the head).
- This is the DAX bargain: the guest gave up its page cache, so the *only*
  cache is this one, host-side, sized and evicted under blockd's control —
  double-caching never returns.

Host kernel: Linux ≥ 6.4 — uffd-wp on shmem (5.19) plus
`UFFD_FEATURE_WP_UNPOPULATED` (6.4), without which hole-punched pages
escape write-protect tracking.
Guest: ext4 `-o dax` on /dev/pmem0 — no guest page cache, guest "flush" is a
no-op by design (durability is checkpoint-based, below).

## Checkpoint

Coherence rule: memory and disk are captured at the same instant, so the
pair is exactly as consistent as a pmem machine losing power — which ext4
with DAX already tolerates. No guest cooperation needed.

1. Pause vCPUs (Firecracker API).
2. Take Firecracker diff-snapshot vmstate (device state only — page data is
   already in blockd's memfds).
3. Freeze the dirty sets (RAM + all disks) accumulated via wp-faults;
   re-arm uffd-wp on those pages. This is the pause-budget risk: 30 s of
   dirt can be GBs across many ranges. Mitigations: batch
   `UFFDIO_WRITEPROTECT` over coalesced ranges, and pre-freeze
   incrementally — start re-protecting cold dirty pages *before* pausing,
   so the in-pause work is only the recently-written residue.
4. Resume vCPUs. Pause ends here — target < 50 ms.
5. Copy-out under write protection: read still-in-memory dirty pages from
   the memfds into the new layer. If the guest writes a page not yet
   copied, the wp-fault handler copies it out first, then un-protects
   (snapshot-while-running).
6. Seal: merge the disk volumes' writeback logs (pages already spilled to
   NVMe since the last checkpoint) with the copy-out set, write manifest +
   segments, fsync, atomically update local head, reset the logs, enqueue
   S3 upload (segments, manifests, then conditional head-record PUT — in
   that order, so S3 never references missing data). The manifest also
   seals the **resume set** observed after this sandbox's most recent
   resume (carried forward unchanged if it hasn't resumed since the last
   checkpoint) — it can't live anywhere else, since manifests are immutable
   and the resume-time fault trace only exists after a restore.

Checkpoint cadence (default 30s) and upload concurrency are policy knobs
owned by the node manager.

## Restore

`Restore(snapshot)` on any host:

1. Fetch the head record — one GET; it inlines vmstate and the full chain
   lists — then all manifests in parallel (local cache, else peer/S3).
   Build merged index.
2. Create memfds, hand fds to Firecracker, load vmstate, resume vCPUs.
   The VM runs immediately; every touched page faults (missing — the
   memfds start empty).
3. Fault resolution order: local NVMe → peer → S3. Any peer known to hold a
   layer resident on NVMe beats S3 — a page RTT to a peer is < 1 ms vs
   20–50 ms for an S3 GET — S3 is the fallback, not the default. Faulted
   segments are written back to NVMe (hydration is also caching).
4. **Resume-set prefetch**: the head layer's manifest carries the ordered
   list of pages faulted in the first few seconds after the sandbox's
   previous resume (sealed at checkpoint; recorded at template build for
   templates). blockd eagerly fetches that set concurrently with step 2,
   hiding S3 latency for the predictable resume path. Full background
   hydration follows at low priority.

Cold-restore first-instruction latency is one head GET (vmstate included) +
parallel manifest GETs + the first resume-set segments — within the 200 ms
target from warm S3; base layers are usually already on-host anyway
(shared templates).

## Migration

Migration = checkpoint + restore with the tail served peer-to-peer:

1. Control plane picks dest, tells both blockds (source addr, sandbox id).
2. Source: micro-checkpoint (pause < 500 ms incl. vmstate transfer), seal
   tail layer locally. S3 upload of the tail proceeds in background as
   usual, ending in the source's **final head PUT**.
3. Dest: `Restore` as above; head record + manifests come directly from
   source (skipping S3 round-trip). Fault sources: dest NVMe (shared bases
   already present) → **source** (< 1 ms away) for every segment it still
   holds — the whole tail plus whatever older segments its cache retains →
   S3 only for segments the source already evicted. Never S3 for a page
   sitting on a peer.
4. Source background-pushes every segment it holds that dest lacks; dest
   writes them to NVMe and fills the remainder from S3.
5. **Writership handoff**: after the final head PUT, source sends dest the
   resulting ETag; from then on dest is the head's single writer and CAS-es
   against that ETag at its first checkpoint. Dest checkpoints before the
   handoff arrives seal locally and queue their uploads behind it.
6. Source releases its copy once (a) dest acks receipt of every segment
   source held that dest lacked and (b) the tail upload + final head PUT
   completed. Until then source remains the serving peer; if dest dies
   mid-migration, the snapshot is still restorable (source + S3 have
   everything).

No special "migration state" in the data model — it's a restore whose layer
locations happen to include a peer.

## Peer protocol

gRPC over TCP with mTLS between blockd daemons (certs provisioned by the
control plane). Four RPCs: `Residency() → versioned bloom filter`,
`HaveLayers(ids) → per-layer segment bitmaps`,
`FetchPages(layer, ranges) → stream`, `PushSegments(layer) → stream`.
10–100 Gbps EC2 networking makes gRPC streaming adequate; revisit only if
fault-service p99 demands raw framing.

### Peer residency discovery

Nobody maintains an exact cluster-wide layer→host map; peer serving is an
optimization and S3 is always the correct fallback. Discovery is two-tier:
an approximate view says *who to ask*, a probe says *what they have*.

- **Bloom gossip (who to ask)**: each blockd maintains a bloom filter over
  the layer ids with any segment resident on its NVMe (~10 bits/layer;
  10⁴–10⁵ layers ≈ 12–125 KB) and exchanges versioned filters with all
  peers every few seconds over the peer gRPC. Layer ids, not segment ids:
  the filter only selects who to probe and the probe reply is
  segment-precise anyway, so segment-granular blooms would be ~10× larger
  (10⁶ segments/host) for no better fetch plans. At fleet scale (tens–hundreds of hosts) this
  is periodic full-mesh, not a real epidemic protocol; the roster comes
  from the control plane, which already provisions the certs. Control-plane
  `peer_hints` (e.g. the migration source) are merged in as
  highest-priority candidates.
- **`HaveLayers` probe (what they have)**: residency is **segment-granular**
  — JIT hydration means a host routinely holds a subset of a layer's
  segments, and a partially-hydrated migration dest can serve what it has.
  At attach/restore, blockd probes the bloom-matched peers with the chain's
  layer ids; replies carry one segment bitmap per layer (~320 B for a
  10 GB layer) from which it builds a per-segment fetch plan. Manifests are
  small and always held whole. Bloom false positives and staleness just
  cost a probe of a peer that answers "none".
- A peer pins the segments it advertised in a probe reply under a short
  renewable lease (~60 s, renewed by fetch traffic), closing the
  probe-then-evict race.
- A stale answer anyway (peer died, lease lapsed) degrades to not-found and
  the fetcher falls through to S3 — never an error.

## NVMe store & GC

- Layout: one directory per layer (`manifest`, `seg.N`), sparse files,
  io_uring with registered buffers for the fault path. Presence is tracked
  per segment (a layer on NVMe is a manifest plus whatever subset of
  segments has been hydrated, pushed, or checkpointed here).
- Cache policy: segments pinned while any attached volume references their
  layer, while not yet in S3, or under a peer lease; otherwise LRU by
  last-fault time. Template bases effectively stay resident by popularity.
- GC (cluster): roots are sandbox head objects and template pointers; any
  layer unreachable from a root and older than a grace window (24 h) is
  deleted from S3. Runs as a blockd subcommand invoked by the control
  plane, listing via S3 inventory. Local GC is just cache eviction.
- Compaction (above) feeds GC: superseded layers become unreachable.

## API (gRPC over UDS)

```
ImportImage(path) → volume                  # raw ext4 image → base layer
CreateTemplate(name, snapshot)              # publish template record
Fork(template|snapshot) → sandbox           # O(1) new writable heads
Attach(sandbox) → {memfd fds, region map}   # then Firecracker passes uffd back
Checkpoint(sandbox) → snapshot              # coherent mem+disk+vmstate
Restore(snapshot, peer_hints[]) → as Attach # hints: hosts w/ chain warm on NVMe
Detach(sandbox, discard|keep)
MigrateOut(sandbox, dest) / MigrateIn(...)  # driven by control plane
Stat/Watch(sandbox)                         # hydration %, layer chain, tiering state
```

fd passing via SCM_RIGHTS on the UDS. All ops idempotent; sandbox/layer ids
are caller-supplied ULIDs so retries are safe.

### Template build

A template starts from a raw ext4 image (built with any standard tooling,
`-o dax`-mountable): `ImportImage` chops it into a single base layer
(manifest + segments) as a disk volume. The node manager boots a sandbox
from it, runs provisioning, lets it settle into the state sandboxes should
wake in, then `Checkpoint` + `CreateTemplate` seal and publish the snapshot
record — including the resume set recorded during a trial restore, so first
forks prefetch well.

## Failure matrix

| Failure | Outcome |
|---|---|
| Firecracker crash | Page data survives in blockd's memfds, but vmstate died with the VMM — no coherent snapshot possible; sandbox restarts from last checkpoint |
| blockd crash | Closing the uffd drops registrations: subsequent guest faults get **zero pages — silent corruption, not a crash**. Invariant: node manager watches blockd and kills every Firecracker on its death, then restarts blockd, which restores each sandbox from its last checkpoint — local NVMe layers make this fast |
| Host loss | Restore everywhere from S3; lose since last uploaded checkpoint |
| Dest dies mid-migration | Source + S3 still hold everything; retry elsewhere |
| Source dies mid-migration | Dest restarts from last S3 checkpoint (tail lost — same loss bound as host death) |
| S3 outage | Sandboxes run and checkpoint locally; uploads queue; only cold restores block |

## Implementation notes

- Rust. Key crates (all OSI-licensed): tokio, tonic (gRPC), io-uring or
  tokio-uring, object_store or aws-sdk-s3, zstd, userfaultfd-rs (verify
  MINOR+WP coverage, else raw ioctls), rustls, ulid, prost; turmoil (MIT) +
  mad-turmoil (MIT) for DST.
- DST constraints bleed into production code by design: no direct system
  time/rng/syscalls outside the `Env` implementations, seeded hashers,
  single-threaded-friendly task structure. This is a feature — it keeps
  the core sans-io and testable.
- Firecracker fork changes needed: virtio-pmem regions included in the UFFD
  handoff message; pmem backed by caller-supplied fd; diff-snapshot vmstate
  without memory file write (blockd owns page data).
- Observability: per-sandbox fault latency histograms (local/peer/S3),
  hydration progress, upload lag (checkpoint-to-S3 durability window),
  dirty rate. Prometheus endpoint.

## Verification plan

### Deterministic simulation testing

blockd's distributed logic runs under FoundationDB/TigerBeetle-style DST:
whole multi-host clusters simulated in one thread, every source of
non-determinism seeded, so any failing run replays exactly from its seed
and simulated hours of cluster time pass in seconds.

This must be designed in, not bolted on. All environment access goes
through one `Env` abstraction with a production and a simulation
implementation:

- **Time**: tokio single-threaded scheduler with paused clock
  (`tokio::time::Instant` only, never system time).
- **Scheduling & network**: turmoil (MIT) hosts — each simulated blockd is
  a turmoil host; the peer gRPC, gossip, and migration protocols run over
  its simulated network with programmable partitions, latency, and drops.
  mad-turmoil's (MIT) madsim-derived libc overrides seed `getrandom` and
  friends if crates sneak past the Env rng.
- **Kernel**: the uffd/memfd/io_uring/PUNCH_HOLE surface is a trait.
  Production implements it with real syscalls; simulation implements it
  in-memory and generates fault events (missing/minor/wp) from simulated
  guest access patterns — a per-host in-process fcsim, sharing its
  workload-generation and self-checking-page logic.
- **Storage**: in-memory S3 (with conditional-PUT semantics and injectable
  latency/errors) and in-memory NVMe with fsync-boundary crash simulation:
  on simulated host crash, un-fsynced writes are torn per a seeded policy.
- **Determinism check**: CI reruns seeds and byte-compares TRACE logs —
  the S2 team found stray non-determinism (hash randomization, timestamps
  in protocol frames) only this way. Seeded hashers everywhere.

What DST covers that nothing else can: migration races (source dies at
every simulated step), gossip/probe/lease staleness, head-record CAS
conflicts, GC-vs-migration interleavings, S3 outage windows, cascading
host failures — each exhaustively fault-injected across seeds
(buggify-style: sim-only code points that misbehave with seeded
probability).

What DST cannot cover — real kernel behavior — is exactly the fcsim and
integration suites' job below. The split: **DST proves the protocol,
fcsim proves the syscalls, integration proves the device model.**

### Firecracker simulator

`fcsim`: an external process that stands in for Firecracker against blockd's
real contract — connects to the UDS, receives memfds, mmaps them, registers
userfaultfd (`MISSING|MINOR|WP`) and hands the fd back, honors pause/resume,
and emits vmstate blobs — so every blockd path (fault serving, dirty tracking,
checkpoint, restore, migration) runs unmodified with no KVM, on any Linux
box or CI runner.

Beyond protocol fidelity, it drives verification:

- Workload generation: seeded, deterministic access patterns (uniform,
  zipfian, sequential scan, dirty-rate-targeted) over RAM and pmem regions.
- Self-checking memory: every page it writes embeds {seed, page index,
  generation}; after any restore/migration it revalidates all touched pages,
  so a wrong/stale/zero page from any fetch path is caught immediately.
- Fault operations: crash on command (mid-write, mid-checkpoint), stall
  fault handling, simulate guest touching pages during copy-out to exercise
  the wp-fault snapshot path.
- Scale: hundreds of fcsim instances per host to test blockd under
  e2b-class sandbox counts without VM overhead.

The simulator is the primary test harness; real-Firecracker integration
tests below then only need to cover what fcsim cannot — the device model,
DAX behavior, and the fork's UFFD handoff.

### Test suites

- Unit: layer index merge, manifest round-trip, compaction equivalence
  (merged chain reads ≡ original chain reads, property-tested).
- DST: seeded cluster simulations — migration/gossip/CAS/GC interleavings
  under crash, partition, and S3-fault injection; nightly long-haul seed
  sweeps; failing seeds land in the repo as regression tests.
- Simulator (fcsim): full-matrix checkpoint/restore/migrate under load,
  crash/fault injection at every step boundary, self-checking-memory
  validation, scale runs.
- Integration (Linux host, real Firecracker fork): boot sandbox on pmem+DAX,
  checkpoint under write load, restore, verify guest fs + process state;
  kill -9 blockd mid-checkpoint and mid-upload, restore, verify coherence.
- Migration: two blockds on one host (or two lnx VMs), migrate under load,
  assert pause duration and data equivalence; kill source/dest mid-flight.
- Performance: fault-latency and resume-latency harness against MinIO
  locally and real S3 on EC2 metal; assert the e2b-class targets.
- Crash-consistency: fio + dm-log-writes-style replay on the pmem volume
  across random checkpoint boundaries.

## Glossary

- **Bloom gossip**: periodic full-mesh exchange of per-host bloom filters
  over resident layer ids; selects which peers to probe.
- **Chain**: a volume's layer sequence, newest first; reads resolve
  top-down through the merged index.
- **Checkpoint**: brief vCPU pause capturing a coherent mem+disk dirty set
  into new head layers plus vmstate.
- **Compaction**: background merge of a run of small layers into one
  (newest page wins) to bound chain length.
- **Control plane**: cluster-level orchestrator deciding placement and
  migration; supplies peer roster, certs, and hints.
- **DAX**: guest ext4 mode mapping pmem directly, bypassing the guest page
  cache.
- **DST**: deterministic simulation testing — seeded single-thread
  simulation of whole blockd clusters (time, network, kernel, S3 all
  simulated) with exact replay from a seed.
- **Env**: the trait boundary through which all code reaches time, rng,
  network, kernel, and object storage; swapped wholesale for simulation
  in DST.
- **fcsim**: Firecracker simulator process implementing blockd's contract
  for KVM-free testing.
- **Fork**: O(1) creation of new volumes sharing a chain suffix with a
  fresh empty writable head.
- **Head layer**: the newest layer of a volume's chain.
- **Head record**: the mutable, single-writer S3 object holding a sandbox's
  current snapshot record; updated by conditional PUT.
- **Hydration**: pulling a layer's segments onto local NVMe (JIT via
  faults, prefetch, or background); doubles as caching.
- **Layer**: immutable page-granular sparse diff = one manifest + its
  segments. Produced by a checkpoint or a template build.
- **Lease**: short renewable pin a peer places on segments it advertised
  in a probe reply, so they aren't evicted mid-fetch.
- **Manifest**: small metadata object of a layer: page index (page ranges →
  segment/offset), checksums, parent link, resume-set hint.
- **memfd**: anonymous shared-memory fd, one per attached volume; backs
  both guest RAM and pmem mappings, owned by blockd.
- **Node manager**: the per-host Go daemon owning Firecracker lifecycle;
  blockd's only local client.
- **Page**: 4 KiB unit of data and of fault serving; the granularity of
  layer indexes and dirty tracking.
- **Residency**: which segments a host currently holds on NVMe;
  segment-granular, advertised approximately via bloom gossip and exactly
  via `HaveLayers`.
- **Resume set**: ordered list of pages faulted shortly after a resume;
  sealed into the next checkpoint's manifest (or at template build) and
  prefetched on the next restore.
- **Sandbox**: one Firecracker microVM = a mem volume + disk volume(s).
- **Segment**: ~4 MiB chunk of a layer's page data; the unit of storage on
  NVMe (uncompressed), of S3 objects and peer transfer (zstd), of fetching,
  and of residency.
- **Snapshot record**: atomic record {per-volume chain lists, vmstate};
  the unit of coherence for restore and migration; stored inline in head
  records and template records.
- **Template**: named snapshot record that sandboxes fork from.
- **UFFD (userfaultfd)**: kernel mechanism blockd uses to serve pages
  (missing + minor faults on memfd) and track writes (uffd-wp).
- **vmstate**: Firecracker device/vCPU state blob; page data excluded
  (blockd owns it).
- **Volume**: ordered chain of layers representing one guest region; kind
  `mem` (guest RAM) or `disk` (virtio-pmem device).
- **Writeback log**: host-local unsealed append log per disk volume; dirty
  pages spill there between checkpoints so they become evictable, and it
  seals into the next layer at checkpoint.
