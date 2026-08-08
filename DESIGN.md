# blockd

Storage backend for the Firecracker sandbox fleet. Replaces JuiceFS.

One daemon per host owns compute state — guest RAM and pmem disks — and
independently attachable SQLite databases as page-granular volume sets. Local
NVMe is the primary store; S3 is async backup and cold tier. Compute bytes use
userfaultfd; database bytes use the stock SQLite Unix VFS over one VM-wide
virtio-fs mount, with an optional DAX shared-memory window. Both converge on
the same verified local/peer/store page path.

[REQUIREMENTS.md](REQUIREMENTS.md) is the contract; this document records
the standing design decisions and their reasons. Formats, protocols and
signatures live in the implementation (`crates/core`) and are byte-pinned
by tests — passages that merely mirrored them have been deleted, per R10.4.

## Non-goals

- General-purpose POSIX filesystem semantics. The database export implements
  only the Linux/FUSE file, mapping, synchronization, and locking behavior
  required by stock SQLite.
- Pre-copy migration. Migration is post-copy: cut over first, fault the
  remainder (R7.1).
- Concurrent writable attachment of one SQLite database to multiple VMs.
- Atomic SQLite transactions spanning independently fenced database vsets.

## Why a daemon (not a library)

Host-wide shared state: base pages cached once per host and shared by every
sandbox forked from them, one NVMe pool, one backup queue, one peer
endpoint. The hot path (fault serving, capture, verification) belongs in
one Rust process; the node manager keeps owning Firecracker lifecycle and
the control plane keeps deciding placement. Note UFFD gives no crash
isolation either way — if the fault server dies, its VMs are dead — so the
daemon buys ops separation, not safety.

## Standing decisions

### Records, not layer chains

Every capture writes one journal record carrying the vset's
page→location map as a **bounded inline overlay plus one pointer per
4096-page span**, whose contents live in write-once leaf blobs — record
size is O(delta), never O(vset), and forks reference base leaves in
place. One write-once segment holds the flushed pages. Reads never walk
a chain; recovery reads the newest record whose leaves are intact — it
never replays a log. Superseded blobs are reclaimed once a newer
committed record replaces them (R4.5), and **minimal compaction** keeps
that honest for long-lived vsets: a segment at least half dead has its
live pages rewritten forward on the writeback cadence, so disk stays
within ~2× live data and each rewritten byte reclaims at least one. The
record is the atomic consistency point (R3.5), and its monotone
`sync_covered_through` watermark is what makes sync ordering survive record
reclamation (R3.8) — a bare covers-sync flag provably loses it; the
simulation found this. Each record is written twice (`.rec` + `.recm`):
the newest record is the sole carrier of its newly-acked syncs, and the
simulation showed one rotten bit rolling acked syncs back silently —
recovery accepts whichever copy decodes.

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
window/backend cache. Refault or a FUSE read reloads from the local segment.
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
record covers all accepted file and metadata mutations through its barrier.
Backup stays asynchronous, so a locally acknowledged transaction may still
fall inside host-loss backup lag.

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

### One fill door

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

### Durability is local; the store is backup; protected sync uses one peer

Captures complete on local NVMe only (R3.6); backup copies locally durable
state to the store continuously as writeback commits it, never gated on
checkpoints (R4.2). Loss on host death is the measured backup lag (R4.3).
Restore fetches a bounded number of small objects (head, manifest, resume
set) before the guest's first instruction; pages follow on demand with a
recorded resume set prefetched (R6.2). Fault sources prefer local NVMe,
then a peer, then the store (R2.3).

Peer-stashed durability changes only the acknowledgment point of guest disk
sync. The primary appends the exact compressed recovery closure to one passive
peer and waits for that peer's durable commit footer. It does not wait for S3,
does not copy guest RAM, and does not fan out to the peer's placement
candidates. The passive peer subsequently uploads those same stored bytes;
the primary performs only the small fenced manifest/head publication. Once the
head covers the peer commit, cleanup unlinks wholly covered sealed spool files
without compaction or data rewrite.

The per-vset head remains the global authority. Besides holder and writer
fence, a peer-stashed head records one active stash, an assignment epoch, and
at most one transition stash. Hosts derive an ordered candidate list from a
versioned authenticated roster with deterministic weighted rendezvous hashing;
the list is placement preference, never replication. A failed active stash
puts the vset in degraded mode: new sync replies queue, one replacement is
named by head CAS, only the outstanding closure is seeded there, and a second
head CAS activates it after a covering durable commit. Recovery inventories
the head's active and transition peers plus S3 and accepts only a complete,
verified closure. A stash never gains permission to run the guest.

Peer traffic is mutually authenticated TLS. The exact verified leaf
certificate maps to a roster host ID, and a frame whose claimed sender differs
from that identity closes the connection. Rollout is a separate fail-closed
control-plane policy: disabled by default, then one failure domain, then a
salted deterministic percentage of vsets. Expansion aborts on any false ACK,
recovery mismatch, non-active-peer byte, cleanup rewrite, or 80% spool-capacity
alert. A production gate separately records completion of transport,
capacity-alert, recovery-drill, and downgrade checks.

The recovery drill inventories only peers named by the fenced head, verifies a
complete closure in quarantine, claims ownership by head CAS, refences and
publishes the recovery point, atomically promotes the local directory, starts
the guest, and releases stale peer residue through the normal watermark path.

### Migration is a durable two-sided handoff

Post-copy: a compute source pauses and captures a final whole record; a
database source first retires its attachment and captures a final commit
record. Either source makes an outbound handoff marker durable before offering,
and the destination makes the record durable as its own first journal entry
before resuming a compute guest or declaring a database ready and detached.
Whichever side crashes, at most one host can own the vset (R6.3/R7.2); a source
recovering with an intact marker serves fetches and never runs or attaches it.

### Failure philosophy

The daemon dies loudly and recovers from durable state alone, giving each
vset an explicit verdict; a resumed vset continues from its newest whole
checkpoint, anything else cold-boots at sync consistency (R8.2). Bytes are
verified before a guest can observe them, and an unservable page kills one
guest loudly — inventing a page is the defining forbidden failure (R8.1).
Pressure — memory or NVMe — slows guests and raises observable signals;
it never kills and never corrupts (R2.5/R2.7).

## Verification

Three tiers, in the repo and green:

- **Deterministic simulation** (`crates/sim`): the whole distributed core —
  multi-host clusters, crashes, torn writes, bit rot, store outages, CAS
  races, unsynchronized clocks — replayable byte-for-byte from a seed
  (R10.1), checked against ghost-history oracles with negative tests
  proving the oracles bite. Correctness workloads compose with independently
  verified network, attrition, disk, store, and placement fault workloads;
  targeted crash grids cover each replica commit, publication, release, and
  replacement boundary before large deterministic seed ensembles run.
- **Real-kernel machinery** (`crates/hostmem`, `crates/runtime`): the same
  daemon state machine driven against real userfaultfd, memfds, O_DIRECT
  disk I/O and an S3-shaped store in a Linux VM, with physical memory
  measured and asserted.
- **Real Firecracker** (`crates/runtime` fc tests, `crates/fc-guest`):
  boot, snapshot, restore, fork-after-work and demand-paged restore of
  real microVMs, the guest itself checksumming its memory; the fork fleet
  proves one-physical-copy sharing through the patched memory backend. The
  database gate additionally runs the current supported SQLite release inside
  a guest through virtio-fs, including WAL, durability, hot detach/attach, and
  snapshot/restore with an open connection.
