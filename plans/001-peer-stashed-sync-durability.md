# Plan 001: Add peer-stashed sync durability without putting S3 on the guest sync path

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report; do not improvise. When done, update the status row for this plan in
> `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat 7125707..HEAD -- REQUIREMENTS.md DESIGN.md crates/core crates/runtime crates/demod crates/sim`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against live code before proceeding. A semantic
> mismatch is a STOP condition.

## Status

- **Priority**: P1
- **Effort**: L (multi-day protocol and recovery change)
- **Risk**: HIGH (changes the meaning and acknowledgment point of guest sync)
- **Depends on**: none
- **Category**: direction
- **Planned at**: commit `7125707`, 2026-08-06

## Why this matters

Today a successful guest pmem sync survives daemon or operating-system restart
on the same host, but permanent host loss may roll it back by the measured S3
backup lag. The requested mode should acknowledge a guest sync only after its
disk recovery point exists on both the primary host and one passive peer, while
keeping S3 asynchronous. The peer copy is temporary recovery residue: retain it
until S3 has published an equally new recovery point, then reclaim it.

The promise is **recoverability**, not automatic availability. A passive stash
does not own the vset and may never start it. Promoting it automatically would
require a larger fencing and placement protocol. The first version must provide
a verified inventory/export recovery path and must never silently restore an
older S3 point while claiming the stronger sync guarantee.

## Proposed contract

### User-visible semantics

Replace the `backed_up: bool` durability choice with one immutable enum:

```rust
pub enum DurabilityMode {
    Local,
    Backup,
    PeerStashed,
}
```

- `Local` preserves the present non-backed behavior and writes no object-store
  object.
- `Backup` preserves the present asynchronous S3 behavior and host-loss bound.
- `PeerStashed` includes asynchronous S3 backup and additionally gates each
  successful guest sync on one passive peer's stable storage.
- Checkpoints remain local operations. Background writeback and checkpoint
  replies do not wait for the peer. A checkpoint that happens to cover a
  pending guest sync may be mirrored because the sync acknowledgment is gated;
  the checkpoint reply itself is not.
- A successful sync means all disk state needed to cold-boot at that barrier is
  recoverable from `peer stash + already-confirmed S3 objects`. It does not copy
  all provisioned pages, and it does not make guest RAM crash-recoverable.
- If the stash peer or network is unavailable, new syncs wait and retry. They
  are never acknowledged optimistically. Guest execution may continue until
  normal cache/NVMe pressure applies; pressure remains observable and bounded.
- Loss of both primary and stash peer before S3 catches up can still lose data.
  This is the explicit two-machine failure boundary.

### Replica placement

Add one host-level `replica_host` setting selected by the control plane. Keep it
out of `VsetConfig`: the durability mode is immutable per vset, while placement
is a property of the current primary host. The selected host must be a distinct
authenticated cluster member and, operationally, should be in another failure
domain.

A replica-host change is not immediately safe. Permit it only after either:

1. S3 has published through every peer-protected sync watermark on that host,
   or
2. the complete outstanding recovery closure has been copied and committed on
   the new peer.

Until one condition holds, reject the rebind and keep syncs stalled. Do not
silently start sending deltas to an empty new peer.

### Durability equation

For a `PeerStashed` vset, define:

```text
sync_ack_through = max(peer_committed_through, store_published_through)
```

A pending guest sync at barrier `b` may receive `SyncOk` only when:

```text
local_record_covers(b) && sync_ack_through >= b
```

TCP write completion, message delivery, remote page-cache residency, or a
remote temporary file is not an acknowledgment point. `peer_committed_through`
advances only after the peer has validated the artifacts, made them stable on
its own disk, made a commit marker stable, and replied with the matching commit
identity.

### Recovery closure

Do not resend a full disk on every sync. Reuse the existing immutable,
compressed artifacts verbatim:

- segment blobs containing changed pages;
- rolled map leaves needed by the record;
- the exact journal record that covers the sync.

For a record, mirror every own-namespace segment and leaf it references unless
that exact object already has a truthful successful S3 `Put` result. Base
objects already durable in S3 are not copied. The journal record is always
committed to the peer until the S3 head publishes a record with an equal or
higher covered sync watermark.

The peer stores artifacts below a namespace derived only from validated typed
fields:

```text
replicas/<source-host>/<vset>/<writer-fence>/
  segments/<segment-id>
  leaves/<leaf-id>
  records/<journal-seq>.rec
  records/<journal-seq>.recm
```

Never accept a path string from the network. The source host and writer fence
are part of the identity so stale writers cannot collide with current residue.

### Protocol

Extend the fixed peer protocol with typed, idempotent messages. Exact field
widths belong in the implementation and byte-pinned tests, not in design prose.

```text
ReplicaPut          source -> peer  one segment or leaf, exact stored bytes
ReplicaPutAck       peer -> source  artifact is verified and stable
ReplicaCommit       source -> peer  exact journal record after all puts ack
ReplicaCommitAck    peer -> source  both record markers are stable
ReplicaStatus       source -> peer  query committed records after restart
ReplicaStatusReply  peer -> source  highest intact committed identity/watermark
ReplicaRelease      source -> peer  S3 head safely covers records through X
ReplicaReleaseAck   peer -> source  covered residue reclaimed
```

Identity for puts and commits is `(source host, vset, writer fence, journal
sequence, artifact kind/id, content checksum)`. Duplicate identical requests
must re-ack. A request that reuses an identity with different bytes is an
integrity violation: reject it, count it, and do not overwrite the first copy.
Lost messages and acknowledgments are retried by the deterministic state
machine. The current bounded, fire-and-forget TCP queue remains acceptable only
because protocol retries own liveness.

Receiver ordering is:

1. authenticate the sender and verify it is the configured counterparty;
2. decode and checksum-validate the artifact using the existing segment, leaf,
   or journal decoder;
3. durably create-or-verify the typed replica artifact;
4. acknowledge data artifacts;
5. accept `ReplicaCommit` only when all referenced non-S3 artifacts are intact;
6. durably write both small record/commit markers and their parent directory;
7. send `ReplicaCommitAck`.

The sender may issue independent data artifacts concurrently. It sends the
commit only after their acknowledgments. This preserves the current
"data-before-record" consistency point on the second machine.

### Watermarks and restart

The current `synced_through` name conflates "captured locally" with "safe to
acknowledge under the new mode." In journal format v4, split the concepts:

- `sync_covered_through`: highest guest barrier whose disk writes are included
  by this record. It may not yet have been acknowledged.
- runtime `sync_ack_through`: the volatile maximum proven by a peer commit or
  published S3 head.

The journal need not claim whether the guest observed an acknowledgment. Extra
durable state is safe. After daemon restart, a `PeerStashed` vset may resume
from its local record, but new sync acknowledgments remain queued until
`ReplicaStatusReply` or the S3 head reconstructs `sync_ack_through`. If the peer
reports behind, resend the local recovery closure before acknowledging.

Cleanup must retain the local record and artifacts needed by any uncommitted
peer operation. Peer residue may be released only after the S3 head CAS—not a
segment upload alone—publishes a record whose `sync_covered_through` is at least
the released peer commit. Release and release acknowledgment are retried;
duplicates are harmless.

### Host-loss recovery

The initial feature deliberately does not make the peer a runner. Add a
read-only recovery command that:

1. inventories residue for `(source host, vset)`;
2. verifies both record copies and all local replica artifacts;
3. chooses the newest whole record by covered sync watermark, capture sequence,
   and journal sequence;
4. reports which referenced objects are present in the stash and which must be
   fetched from S3;
5. exports a normal local recovery directory only after every reference is
   verified;
6. leaves the result quarantined until an operator obtains a fresh assignment
   fence through the existing S3 head CAS.

No recovery command may start a guest or mutate the S3 head implicitly. A
normal restore of a `PeerStashed` vset after primary-host loss must report
"peer recovery required" when S3 is behind known peer residue; it must not
silently present the older point as satisfying the stronger mode.

### Failure matrix

| Failure | Required behavior |
|---------|-------------------|
| S3 outage, peer healthy | Guest syncs continue; peer residue and lag grow. |
| Peer/network outage | New syncs wait; no optimistic acknowledgment. |
| Primary daemon restart | Resume locally; reconcile peer/S3 before new sync acknowledgments. |
| Primary machine lost, peer alive | Latest acknowledged sync is verifiably exportable from peer plus S3. |
| Peer restarts during a put | Torn/uncommitted data is ignored; source retries idempotently. |
| Commit ack is lost | Duplicate commit is verified and re-acked. |
| Primary dies after peer commit but before guest reply | Extra recoverable state is allowed; no false acknowledgment occurred. |
| Both machines lost before S3 catches up | Data may be lost; surface the declared failure boundary. |
| Replica disk fills | Replica puts stop completing; syncs stall and bounded metrics alert. |

## Current state

- `REQUIREMENTS.md:83-100` defines sync acknowledgment and local-host
  durability. `REQUIREMENTS.md:102-134` permits only a backed/not-backed knob
  and explicitly allows host-loss rollback by backup lag. These clauses must be
  renegotiated for the new third mode rather than contradicted in code.
- `DESIGN.md:64-69` says captures complete on local NVMe and S3 is asynchronous.
  Preserve that for checkpoints and writeback; add the narrower peer gate only
  to successful guest syncs in the new mode.
- `crates/core/src/daemon/guest.rs:201-229` snapshots the vset-global mutation
  barrier and currently short-circuits against local `durable_watermark`.
- `crates/core/src/daemon/capture.rs:420-613` reads unstable pages, writes one
  compressed segment plus any rolled leaves, and then writes the record.
- `crates/core/src/daemon/capture.rs:738-863` marks the record durable and emits
  `SyncOk` immediately after the two local record copies complete. This is the
  acknowledgment point that must become mode-dependent.
- `crates/core/src/daemon/backup.rs` already derives a record's missing S3
  segments/leaves and copies stored bytes verbatim. Use this as the structural
  exemplar for a new `daemon/replica.rs`; do not fold peer durability into the
  object-store state machine.
- `crates/core/src/seam.rs:132-175` defines the peer messages, and
  `crates/core/src/peer.rs` byte-pins their wire encoding. Any new variant is a
  deliberate protocol version change.
- `crates/runtime/src/peer.rs:39-121` has a 128-frame queue and deliberately
  drops on pressure or connection failure. Correctness therefore requires
  source-owned retry and durable receiver acknowledgments.
- `crates/runtime/src/host.rs:829-858` is the existing durable-blob exemplar:
  write the file, sync it, then sync parent directories before emitting
  `BlobWriteDone`.
- `crates/core/src/segment.rs:69-75` stores offsets as `u32`, but capture
  currently has no explicit segment-size cap. Add a cap below the peer frame
  limit and split large captures; do not assume typical dirty sets are small.
- `crates/core/src/daemon/migrate.rs:233-357` demonstrates protocol-level
  counterparty authorization and idempotent peer retries. Match its style, but
  do not reuse migration ownership state.
- `crates/sim/src/cluster.rs`, `crates/sim/src/world/network.rs`, and
  `crates/sim/src/world/blobdev.rs` already model peer loss/duplication and
  independent durable disks. The new guarantee belongs in this simulator.
- `crates/sim/src/oracle.rs:311-316` currently allows acknowledged-sync rollback
  after backed host loss. It must continue doing so for `Backup`, but never for
  `PeerStashed` when one of the promised recovery copies survives.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Core tests | `cargo test -p blockd-core` | exit 0; all format and daemon tests pass |
| Simulation tests | `cargo test -p blockd-sim` | exit 0; no oracle violations |
| Workspace tests | `cargo test --workspace` | exit 0 |
| Lint/format | `./lint.sh` | exit 0; no warnings or format diff |
| Build seed runner | `cargo build --release -p blockd-sim --bin sweep` | exit 0 |
| Chaos corpus | `target/release/sweep chaos 0 2000` | 0 failing seeds |
| Cluster corpus | `target/release/sweep cluster 0 2000` | 0 failing seeds |
| Migration corpus | `target/release/sweep migration 0 2000` | 0 failing seeds |
| Linux peer test | `cargo test -p blockd-runtime --test peer_linux` | exit 0 on Linux |
| Linux replica test | `cargo test -p blockd-runtime --test replica_e2e_linux` | exit 0 on Linux |

## Scope

**In scope**:

- `REQUIREMENTS.md`, `DESIGN.md`
- `crates/core/src/journal.rs`, `crates/core/src/seam.rs`,
  `crates/core/src/peer.rs`, `crates/core/src/layout.rs`
- `crates/core/src/daemon/mod.rs`, `guest.rs`, `capture.rs`, `backup.rs`,
  `recover.rs`, `restore.rs`, and new `replica.rs`
- `crates/core/src/segment.rs` for bounded segment splitting
- `crates/core/tests/format_goldens.rs` and focused core tests
- `crates/runtime/src/host.rs`, `crates/runtime/src/peer.rs`, and a typed durable
  replica storage helper if needed
- `crates/runtime/tests/peer_linux.rs` and new
  `crates/runtime/tests/replica_e2e_linux.rs`
- `crates/demod/src/config.rs` and startup wiring for `replica_host`
- deterministic simulation cluster, network/disk model, oracle, presets, and
  tests required to prove the new guarantee
- bounded metrics for replica lag, bytes, retries, stalled syncs, rejects, and
  recovery residue
- read-only inventory/verify/export recovery tooling

**Out of scope**:

- automatic peer promotion or any new consensus/quorum service
- making guest RAM durable on fsync
- waiting for S3 on the guest sync path
- changing ordinary `Backup` host-loss semantics
- enabling peer-stashed mode without S3 backup
- content-based deduplication or erasure coding
- cross-region replication
- weakening the existing checkpoint pause or local-durability contracts
- shipping production mode over unauthenticated plain TCP

## Git workflow

- Branch: `semistrict/001-peer-stashed-sync-durability`
- Use focused commits matching the repository's subject style, for example
  `core: gate peer-stashed syncs on remote durability`.
- Do not push or open a pull request unless instructed by the operator.
- Never edit generated outputs directly; update their source and run the
  repository's generator when one exists.

## Steps

### Step 1: Amend the contract before changing behavior

Update `REQUIREMENTS.md` and `DESIGN.md` with the Proposed contract above.
Preserve the meaning of the existing modes. Assign new requirement identifiers
for the peer-stashed acknowledgment point, two-machine failure boundary,
failure behavior, observability, and passive recovery. Explicitly distinguish
disk recovery closure from guest RAM and from copying all provisioned pages.

**Verify**: `rg -n "PeerStashed|peer stash|two-machine|recovery closure" REQUIREMENTS.md DESIGN.md`
must show the mode, acknowledgment point, failure boundary, and recovery rule in
both contract and decision prose.

### Step 2: Introduce the durability enum and format v4

Replace `VsetConfig.backed_up` with `VsetConfig.durability`. Add helpers such as
`uses_store()` and `requires_peer_sync()` so call sites do not scatter enum
matches. Replace the record's ambiguous `synced_through` with
`sync_covered_through`, migrate journal encoding to v4, and update all byte
goldens intentionally. Old v3 records must either decode with their exact old
semantics or recovery must reject startup with a clear migration-required
verdict; choose and document one policy. Do not silently reinterpret a v3
boolean as the stronger mode.

**Verify**: `cargo test -p blockd-core journal format_goldens` exits 0 and the
tests include all three durability variants plus the chosen v3 policy.

### Step 3: Bound segment size before adding replica payloads

Teach capture to split a large dirty set into multiple immutable segment blobs
under an explicit limit comfortably below `MAX_PEER_PAYLOAD` and S3's 64 MiB
contract (32 MiB is the recommended ceiling). Preserve one generation and one
`PageLoc` per captured page, and keep record creation gated on every segment and
rolled leaf. Add tests with incompressible page data that cross the cap.

Do not split a page entry and do not change compression or entry framing.

**Verify**: `cargo test -p blockd-core segment capture` exits 0; a test asserts
every produced segment is at or below the cap and the reconstructed map contains
every page exactly once.

### Step 4: Add the typed replica wire protocol

Add the seven message families in Proposed contract to `PeerMsg` and implement
strict encode/decode with a deliberate peer protocol version bump. Keep sender
identity, fixed-width integer rules, frame checksum, payload cap, rejection of
trailers/unknown discriminants, and single-bit corruption coverage. Artifact
paths must be reconstructed from typed fields only.

Add protocol-level authorization: replica puts/commits/releases are accepted
only from the configured source counterpart, and replies/status are accepted
only from the vset's configured replica target. Unauthorized messages increment
a bounded counter and have no storage effect.

**Verify**: `cargo test -p blockd-core peer` exits 0; every new variant is in the
round-trip and byte-pinned sample set, and wrong-counterparty tests observe no
`BlobWrite` or `BlobDelete` effect.

### Step 5: Implement passive, idempotent replica storage

Create a distinct typed replica storage path in the state machine/runtime. A
receiver must verify segment headers/entries, leaf identity, and record identity
before durable storage. Implement create-or-verify behavior so an identical
retry re-acks without rewriting, while conflicting bytes never overwrite the
first durable artifact.

`ReplicaCommit` is the commit point. Store both small record markers only after
required puts are stable, sync their containing directory, then acknowledge.
Incomplete/torn artifacts without an intact commit marker are residue, not a
recovery point. Enforce per-source byte accounting and capacity metrics; on
full disk, stop acknowledging instead of deleting unbacked residue.

**Verify**: focused core and runtime tests cover first put, duplicate put, lost
ack retry, conflicting duplicate, torn file, receiver restart, full disk, and
commit-before-data rejection. All tests assert intended correct behavior; do
not leave a green test whose expectation is the buggy behavior.

### Step 6: Gate only high-durability sync acknowledgments

Add a separate `sync_ack_through` to vset runtime state. Keep local recovery
watermarks and remote/store acknowledgment watermarks distinct. When a locally
durable record advances `sync_covered_through`:

1. ordinary modes follow their existing behavior;
2. `PeerStashed` derives missing recovery artifacts using the same closure
   logic as backup;
3. it sends missing puts, retries until stable, and commits the exact record;
4. only `ReplicaCommitAck` or a sufficiently new successful S3 head CAS advances
   `sync_ack_through` and drains pending `SyncOk` replies.

Keep checkpoint replies tied to local record durability. Pin all artifacts used
by an in-flight peer commit against local cleanup. Coalesce multiple pending
syncs into one covering record when possible, while ensuring a sync arriving
after a record's covered watermark triggers a later capture.

**Verify**: core tests show no `SyncOk` after local record completion alone,
then exactly one `SyncOk` after a matching peer commit; ordinary modes retain
their old acknowledgment behavior.

### Step 7: Reconcile restart state and release only after S3 head publication

On recovery of a `PeerStashed` vset, issue `ReplicaStatus` and reconstruct
`sync_ack_through` before acknowledging new syncs. If the peer is behind the
best local covering record, resend its closure. The guest may resume locally;
only new sync completion waits for reconciliation.

After `PubHeadCas` succeeds, use the published record's covered watermark to
drain syncs if S3 won the race and to send `ReplicaRelease`. Retry release until
acknowledged. The receiver deletes only records/artifacts covered by that
release, preserves newer residue, and treats duplicates idempotently.

**Verify**: tests cover source restart before/after commit ack, peer restart,
S3 winning the race, lost releases, duplicate releases, and a rebind attempt
while residue is outstanding.

### Step 8: Add verified recovery inventory and export

Implement the read-only inventory/verify/export path from Host-loss recovery.
Its normal output must identify source host, vset, fence, record sequence,
covered sync watermark, intact stash objects, required S3 objects, and a final
`complete`/`incomplete` verdict. Export only a complete verified closure and
leave it quarantined. Starting the guest and acquiring a fresh fence remain
separate explicit operations.

Add a machine-readable output option so operations can automate a recovery
drill without parsing prose. Never print page contents or credentials.

**Verify**: a fixture with one stash artifact in S3 exports successfully; a
fixture with one missing/corrupt artifact reports incomplete and writes no
usable recovery directory.

### Step 9: Prove the protocol in deterministic simulation

Extend the cluster model with replica placement, per-host replica residue, peer
disk capacity, and the new messages. Exercise independent primary/peer crashes,
torn writes, bit rot, loss, duplication, reordering, partitions, S3 outages,
and S3/peer completion races.

Change the oracle so host loss permits acknowledged-sync rollback for `Backup`
but not for `PeerStashed` when the declared single-machine failure assumption
holds. Add targeted crash-instant sweeps across data put, commit marker, ack,
guest reply, S3 head CAS, and release. Negative oracle tests may assert that the
oracle detects an injected violation; they must not encode a reproduced bug as
the passing system behavior.

**Verify**: `cargo test -p blockd-sim` exits 0, followed by all three 2,000-seed
commands in Commands you will need reporting zero failures.

### Step 10: Validate the real runtime and security boundary

Wire `replica_host` through daemon configuration and expose bounded metrics.
Add a two-runtime Linux test using separate blob roots and real TCP:

1. delay/block S3;
2. write multiple disk pages and issue guest sync;
3. prove the sync does not finish after primary local fsync alone;
4. let the peer make the commit durable and observe sync completion;
5. destroy the primary blob root;
6. inventory/export from the peer and verify every acknowledged disk page;
7. publish S3 through the same record, release residue, and verify it is gone.

Measure p50/p95/p99 sync latency and bytes copied for at least small (one page),
medium (1 MiB delta), and cap-crossing deltas. Record the baseline and fail the
performance test only on a reviewed, stable threshold.

The runtime currently documents plain TCP as demo-only. Do not enable
`PeerStashed` in production configuration until authenticated peer identity and
encryption are actually enforced; envelope-provided sender ids are not an
authentication mechanism.

**Verify**: Linux peer and replica end-to-end tests exit 0; `./lint.sh` and
`cargo test --workspace` exit 0.

## Test plan

- Unit tests in `crates/core`:
  - three durability modes and v3/v4 record policy;
  - bounded multi-segment capture;
  - every replica wire variant, byte pin, corruption, and hostile fields;
  - closure derivation excludes only truthfully confirmed S3 objects;
  - mode-dependent sync acknowledgment;
  - idempotent puts/commits/status/releases and counterparty guards.
- Deterministic tests in `crates/sim/tests`:
  - S3 outage with healthy peer continues syncs;
  - peer partition stalls sync and recovery resumes it;
  - primary loss after guest ack recovers every acknowledged disk write;
  - primary loss before guest ack may expose extra state but never older state;
  - source/peer crash grids around every protocol commit edge;
  - replica capacity exhaustion stalls without corruption or eviction;
  - release happens only after head CAS and never deletes newer residue;
  - simultaneous primary/peer loss is reported as outside the guarantee.
- Runtime tests:
  - real durable receiver ordering and retry over TCP;
  - primary deletion plus verified peer export;
  - residue reclamation after S3 publication;
  - bounded queues under load do not create false acknowledgments.
- Use `crates/core/src/daemon/tests.rs` for direct effect/event expectations,
  `crates/sim/tests/backup.rs` for S3 outage style, and
  `crates/sim/tests/multihost.rs` for crash-grid and exactly-one-runner style.

## Done criteria

- [ ] Contract prose defines the third mode, exact acknowledgment point,
  failure boundary, recovery scope, and passive-peer rule.
- [ ] `PeerStashed` never emits `SyncOk` from local durability alone.
- [ ] Every successful peer commit has an intact record and complete recovery
  closure across peer residue plus truthfully confirmed S3 objects.
- [ ] Receiver retries are idempotent and conflicting identities cannot
  overwrite durable bytes.
- [ ] Restart reconstructs the peer/S3 acknowledgment watermark before new
  sync acknowledgments.
- [ ] Peer residue is reclaimed only after a covering S3 head CAS.
- [ ] Recovery inventory/export verifies content and never starts a guest or
  changes assignment implicitly.
- [ ] Ordinary `Local` and `Backup` behavior remains covered and unchanged.
- [ ] Core, simulation, workspace, lint, Linux peer, and Linux replica tests
  pass.
- [ ] All three 2,000-seed sweeps report zero failures.
- [ ] No file outside Scope is modified except lockfile changes caused by an
  explicitly reviewed dependency addition.

## STOP conditions

Stop and report rather than improvising if:

- implementing the mode appears to require automatic peer promotion,
  multi-writer state, or weakening the existing S3 fencing authority;
- a record cannot identify the full recovery closure without an unbounded page
  scan or an incompatible storage-format break;
- the selected replica peer can change while unbacked residue exists and no
  safe rebind source is available;
- a payload can exceed the peer frame cap after segment splitting;
- authenticated peer identity cannot be established for production enablement;
- the simulator cannot distinguish primary and replica durable disks or cannot
  crash them independently;
- any test would pass by expecting the known-wrong behavior to reproduce;
- a required change reaches outside Scope or changes guest/VMM sync semantics.

## Maintenance notes

- Reviewers should scrutinize the exact `SyncOk` emission sites, remote commit
  ordering, cleanup pins, restart watermark reconstruction, and wrong-peer
  guards. Those are the safety boundary.
- Future automatic failover should consume the verified recovery export and add
  an explicit fencing design; it must not turn a stash commit into ownership.
- Future replica rebalancing must copy or drain outstanding closure before
  switching targets.
- Capacity planning must include worst-case S3 outage duration multiplied by
  dirty rate. Metrics should alert well before replica disk exhaustion.
- If the peer transport later becomes reliable, retain protocol idempotency and
  retries; delivery guarantees do not replace durable acknowledgment.
