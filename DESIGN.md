# blockd

Storage backend for the Firecracker sandbox fleet. Replaces JuiceFS.

One daemon per host owns all sandbox state — guest RAM and pmem disks — as
page-granular volume sets. Local NVMe is the primary store; S3 is async
backup and cold tier. Every byte a VM touches is served through userfaultfd,
which makes restore-from-S3, restore-from-peer, and host-to-host migration
the same code path: resume immediately, fault pages in from wherever they
live.

[REQUIREMENTS.md](REQUIREMENTS.md) is the contract; this document records
the standing design decisions and their reasons. Formats, protocols and
signatures live in the implementation (`crates/core`) and are byte-pinned
by tests — passages that merely mirrored them have been deleted, per R10.4.

## Non-goals

- POSIX filesystem semantics (that was JuiceFS's job; nothing needs it now).
- Pre-copy migration. Migration is post-copy: cut over first, fault the
  remainder (R7.1).

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

Every capture writes one journal record carrying the vset's **full
page→location map** at that instant, plus one write-once segment holding
the flushed pages. Reads never walk a chain; recovery never replays a log;
there is no compaction — superseded blobs are reclaimed once a newer
committed record replaces them (R4.5). The record is the atomic
consistency point (R3.5), and its monotone `synced_through` watermark is
what makes sync ordering survive record reclamation (R3.8) — a bare
covers-sync flag provably loses it; the simulation found this.

### Fenced namespaces

Assignment authority is the head object's conditional-write version
(R6.3): a claim's returned version IS the claimant's fence, and every
artifact the holder writes — journal records, segments, manifests — lives
under that fence in its names. A fenced former holder's writes dangle
unreachably (R6.4) without any revocation protocol.

### The host is the page cache

Disks are virtio-pmem with DAX: the guest gave up its page cache, so the
host-side cache is the only cache (R2.2), sized and evicted under the
daemon's control. Eviction of a clean page is `MADV_DONTNEED` plus a hole
punch of its backing — real RAM comes back — and the next touch is a fault
served from the local segment. Which pages are cold is decided the way the
kernel decides it: the eviction structure mirrors the multi-generational
LRU (generations, accessed-bit aging, readahead placed oldest), with the
memory-over-disk affinity of R2.4 as the fixed balance rule (R2.6).

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

### Durability is local; the store is backup

Captures complete on local NVMe only (R3.6); backup copies locally durable
state to the store continuously as writeback commits it, never gated on
checkpoints (R4.2). Loss on host death is the measured backup lag (R4.3).
Restore fetches a bounded number of small objects (head, manifest, resume
set) before the guest's first instruction; pages follow on demand with a
recorded resume set prefetched (R6.2). Fault sources prefer local NVMe,
then a peer, then the store (R2.3).

### Migration is a durable two-sided handoff

Post-copy: the source pauses, captures a final whole record, and makes an
outbound handoff marker durable before offering; the destination makes the
record durable as its own first journal entry before resuming. Whichever
side crashes, at most one host can ever run the vset (R6.3/R7.2); a source
recovering with an intact marker serves fetches and never runs the guest.

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
  proving the oracles bite.
- **Real-kernel machinery** (`crates/hostmem`, `crates/runtime`): the same
  daemon state machine driven against real userfaultfd, memfds, O_DIRECT
  disk I/O and an S3-shaped store in a Linux VM, with physical memory
  measured and asserted.
- **Real Firecracker** (`crates/runtime` fc tests, `crates/fc-guest`):
  boot, snapshot, restore, fork-after-work and demand-paged restore of
  real microVMs, the guest itself checksumming its memory; the fork fleet
  proves one-physical-copy sharing through the patched memory backend.
