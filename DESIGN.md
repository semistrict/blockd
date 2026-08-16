# blockd design

[REQUIREMENTS.md](REQUIREMENTS.md) is the contract. This document records
standing design decisions and their rationale. Formats and protocols live in
`crates/core` and are pinned by tests.

## Non-goals

- General-purpose POSIX filesystem semantics. The database export implements
  only the Linux/FUSE file, mapping, synchronization, and locking behavior
  required by stock SQLite.
- Pre-copy migration. Migration is post-copy: cut over first, fault the
  remainder (R7.1).
- Concurrent writable attachment of one SQLite database to multiple VMs.
- Atomic SQLite transactions spanning independently fenced database vsets.

## Why a daemon (not a library)

The daemon owns host-wide state: shared base pages, the NVMe pool, archival
work, and peer connections. The node manager retains Firecracker lifecycle,
and the control plane decides placement. Userfaultfd provides no process-crash
isolation; if the fault server dies, its VMs stop.

## Standing decisions

### One read-only data-file format

[STORAGE_DESIGN.md](STORAGE_DESIGN.md) defines the storage format. Changed
blocks are written into sorted, bounded `.blx` files on the primary. The same
bytes are copied to the passive and may later be uploaded by the primary.
There is no second archive page-data format.

A local journal record commits the files that make up one recovery point and
its monotone `sync_covered_through` watermark. The primary and passive flush
the named files before committing the record. Periodic complete local file
lists bound restart work. Object storage uses one current manifest, one
reusable complete file list, and bounded additions and removals; restore never
walks manifest history. Compaction replaces file references without changing
the saved guest state. Each local commit record is written twice (`.rec` and
`.recm`) so recovery can use either verified copy.

### Fenced namespaces

Assignment authority is the head object's conditional-write version
(R6.3): a claim's returned version IS the claimant's fence, and every
artifact the holder writes — journal records, segments, manifests — lives
under that fence in its names. A fenced former holder's writes dangle
unreachably (R6.4) without any revocation protocol.

### The host owns database page residency

Compute disks are virtio-pmem with DAX. Database files use virtio-fs: eligible
main/WAL pages may be mapped through its shared DAX window, while unsupported
or pressured pages fall back to FUSE reads and writes. This DAX region is
ordinary volatile shared memory, not persistent memory. The backend, rather
than a guest-visible block device, owns durable page residency and eviction.
SQLite's userspace page cache and unavoidable guest virtio-fs metadata/page
state are bounded parts of the data path; correctness never relies on either
surviving cold recovery. Eviction of a clean compute page uses
`MADV_DONTNEED` plus a hole punch; database pages are reclaimed from the DAX
window/backend cache. Refault or a FUSE read reloads from a local `.blx` file.
The eviction structure mirrors the multi-generational LRU, with compute memory
receiving the higher residency affinity of R2.4 and every storage page sharing
the lower class.

### SQLite databases are independent vsets

One SQLite database is a storage-only vset containing its main, WAL and
rollback-journal file pages plus atomic existence and logical-size metadata.
WAL is the only supported steady state; the journal persists SQLite's
crash-safe transition of a new database into WAL. It has no guest RAM or
vmstate. One always-present virtio-fs MMIO device and mount per VM multiplexes
any number of database namespaces, so hotplug changes the exported inode tree
without adding a Firecracker device or mount. A distinct vhost-user connection
binds each export to a trusted VM identity; an attachment generation resolved
from every inode/handle fences delayed traffic after detach. Attachments are
volatile subordinate leases: uncoordinated daemon recovery starts databases
detached.

The external backend implements the FUSE operations, POSIX byte-range locks,
WAL shared memory, and DAX mapping lifecycle expected by the stock Unix VFS.
`FUSE_FSYNC` is the durable door: it completes only after a mirrored local
record covers all accepted file and metadata mutations through its barrier and
the recovery closure is durable on the assigned passive or already archived.
Object-store archival remains asynchronous.

### VM snapshots coordinate attached databases

A memory snapshot pauses vCPUs, freezes and drains the filesystem backend,
synchronizes writable DAX state, and pins an immutable version of every
attached database before RAM and device state are captured. The filesystem
snapshot serializes stable handles, POSIX locks, WAL shared-memory bytes,
VirtIO queue state, and DAX mappings. Restore recreates the same MMIO layout and
guest-physical DAX mappings before any vCPU resumes. Restoring one image more
than once branches every attached database version, so writable authority is
never duplicated. This coordinated path preserves already-open SQLite
connections; cold database recovery remains detached and reconstructs SHM from
WAL.

An ordinary file-backed Firecracker restore maps RAM privately. That is not
compatible with an external vhost-user process: guest VirtIO queue writes can
become private copy-on-write pages that the backend's independent mapping
cannot observe. The patched `FileShared` restore backend first copies the
immutable memory snapshot into an unlinked per-VM working file and maps that
copy shared. The vhost-user backend and Firecracker therefore observe the same
queue memory while the source snapshot stays immutable. This mode is for a
unique restore; restoring the snapshot again still requires the database-vset
branching rule above before either VM can become writable.

### Shared-memory fault handling

Guest memory is shared memory the daemon populates; faults resolve by
populating the backing and letting the mapping find the page — never by
copying into VM-private memory, which would break R5.3's one-physical-copy
sharing. Kernel finding (verified): MINOR-mode uffd alone silently
zero-allocates absent shmem pages; MISSING registration is what makes
first touches and post-reclaim refills trap.

### Forks share by mapping, diverge by copy-on-write

A fork is O(1) metadata over its base's record (R5.1); every unmodified
base page exists once per host regardless of fork count (R5.3), and a
fork's writes go to its own namespace. Under Firecracker this is the
patched `UffdShmem` memory backend
(`patches/firecracker-uffd-shmem.patch`): guest memory maps the
handler-owned shared-memory file MAP_PRIVATE, so clean pages are shared
page-cache pages and divergence is kernel copy-on-write.

### Local capture, peer-protected sync, asynchronous archival

Captures complete on local NVMe (R3.6), while guest sync acknowledgement also
requires a durable passive copy (R4.1). The primary archives protected state
asynchronously, independent of checkpoints. Restore fetches only the head,
manifest, and resume set before the guest's first instruction; pages follow on
demand (R6.2). Fault sources prefer local NVMe, then a peer, then the store
(R2.3).

Peer-stashed durability changes only the acknowledgment point of guest disk
sync. The primary appends the exact compressed recovery closure to one passive
peer and waits for that peer's durable commit footer. It does not wait for the
object store, copy guest RAM, or fan out to the peer's placement candidates.
After protection, the primary independently merges unpublished local records
through a selected protected cut, uploads the resulting archive objects and
full manifest, and publishes the manifest through the fenced head CAS. The
passive never uploads to the object store; it remains the durable recovery copy
until the primary's published archive covers its commit. Cleanup then releases
wholly covered sealed spool files without entering the guest sync path.

The per-vset head remains the global authority. Besides holder and writer
fence, a peer-stashed head records one active stash, an assignment epoch, and
at most one transition stash. Hosts derive an ordered candidate list from a
versioned authenticated roster with deterministic weighted rendezvous hashing;
the list is placement preference, never replication. A failed active stash
puts the vset in degraded mode: new sync replies queue, one replacement is
named by head CAS, only the outstanding closure is seeded there, and a second
head CAS activates it after a covering durable commit. Recovery inventories
the head's active and transition peers plus the object store and accepts only a
complete, verified closure. A stash never gains permission to run the guest.

Peer traffic uses mutually authenticated TLS. The verified leaf certificate
maps to a roster host ID; a frame claiming a different sender closes the
connection.

The recovery drill inventories only peers named by the fenced head, verifies a
complete closure in quarantine, claims ownership by head CAS, refences and
publishes the recovery point, atomically promotes the local directory, starts
the guest, and releases stale peer residue through the normal watermark path.

### Migration is a durable two-sided handoff

Post-copy: a compute source pauses and captures a final whole record; a
database source first retires its attachment and captures a final commit
record. Either source makes an outbound handoff marker durable before offering,
and the destination makes the record durable as its own first journal entry
before resuming a compute guest or declaring a database ready and detached. A
compute offer normally carries the captured VMM bytes directly; if it cannot,
the destination verifies and reads those bytes from the source before resume.
Whichever side crashes, at most one host can own the vset (R6.3/R7.2); a source
recovering with an intact marker serves fetches and never runs or attaches it.

### Failure behavior

The daemon recovers from durable state alone and gives each vset an explicit
verdict. A vset resumes from its newest whole checkpoint or cold-boots at sync
consistency (R8.2). Bytes are verified before use; an unservable page fails its
guest rather than substituting data (R8.1). Memory or NVMe pressure stalls work
and emits signals without discarding state (R2.5/R2.7).

### One actor runtime in simulation and production

The core protocol is expressed as current-thread async actors over the
contracts in `crates/core/src/world.rs`. Fault service, capture, publication,
restore, migration, replication, database I/O, compaction, and store garbage
collection are straight-line futures sharing host state only between await
points. A host owns those children as one cancellation tree: a simulated crash
or production teardown drops the root, then recovery reconstructs state from
durable bytes rather than continuing an in-memory conversation.

`crates/exec` is the scheduler in both environments. Simulation owns virtual
time, seeded randomness, fault-point decisions, and a closed ready queue;
production adds monotonic timers and a thread-safe external-event injector.
FIFO wake ordering and declaration-order selection are fixed. Every simulated
poll folds virtual time, task identity, and wake source into the trace hash, so
R10.1 replay checks the actual actor interleaving rather than a second model of
the protocol. Simulation and production differ only in their world trait
implementations for blobs, object storage, peers, guest memory, and admin I/O.

In-process operations use typed request envelopes. Each administrative,
database, and guest-sync call carries its own one-shot reply promise, so
completion routing has no shared reply stream, request table, or scan for a
matching identifier. The synchronous Firecracker and VFS adapters block on
that call's promise only. Checkpoint keeps a request identity because durable
retry idempotency is part of its protocol; database and peer identifiers
remain only at their wire boundaries. Recovery and inbound migration are not
request completions and therefore arrive on a separate typed lifecycle-event
stream.

Dynamic children live in structured actor collections. Completion reaps a
child even while ingress is idle, dropping an owner cancels the complete
subtree, and child failures propagate to the host supervisor. Core operations
return typed rejection, stale, unavailable, and fatal outcomes. The root alone
turns a host-fatal outcome into process termination in production or a
deterministic host failure in simulation. Fallible guest-memory calls follow
the same rule; mandatory unservable pages are reported to the root rather than
terminating from a world callback.

Each vset has typed ownership slots for mutation, migration, publication, and
replication. A mutation owner is capture (writeback, checkpoint, or migration),
database persistence, or hydration; only capture can own a page-drain state,
and cancellation leases release exactly the owner they acquired. This makes
overlapping invalid flag combinations unrepresentable through the operation
API while retaining orthogonal publication and replication progress.

Writeback cadence uses one host timer plus a deterministic ordered work set.
Faults, lifecycle transitions, peer progress, and operation completion enqueue
only affected vsets; each timer turn drains that set in bounded poll batches.
Idle vsets allocate no actor, timer, device, thread, or provisioned-size
buffer. Startup store reconciliation is separately limited to 32 children and
emits externally visible lifecycle outcomes in vset order. Peer RPC call sites
await a typed client future whose broker owns wire-ID allocation, authenticated
reply matching, timeout cleanup, and retry; duplicated and late replies cannot
complete a different call.
