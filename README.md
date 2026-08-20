# blockd

Page-granular storage for Firecracker sandbox fleets. One daemon per host
manages guest memory and durable disk state, with asynchronous archival to
object storage.

## Build and test

```sh
cargo build --workspace
cargo test --workspace
```

## Start a cluster

Build the host daemon once, then run the same command on every Linux machine:

```sh
cargo build --release -p blockd-runtime --bin blockd
sudo target/release/blockd serve gs://MY_BUCKET/MY_CLUSTER \
  --capacity-bytes 1099511627776 \
  --headroom-bytes 107374182400 \
  --firecracker /usr/local/bin/firecracker \
  --firecracker-sha256 "$APPROVED_FIRECRACKER_SHA256"
```

The bucket and prefix are the cluster identity. The daemon uses the machine's
ambient GCE service-account credentials, chooses and durably claims a host ID,
publishes its ephemeral mTLS identity, and continuously derives replica
placement from the live records in that prefix. Concurrent first nodes safely
race to create the cluster metadata and cannot claim the same host ID. The
initial cluster placement is published only after three distinct live hosts
have joined, so a one- or two-node start remains not-ready instead of weakening
the replication contract.

By default, local state is stored in `/var/lib/blockd` and the peer service is
advertised on the private address selected by the metadata-server route at
port 7001. Address discovery follows the private route to the GCE metadata
service and uses the local source address chosen by the kernel; it does not
allocate a new address. On hosts where that route is unavailable or ambiguous,
pass the already configured private address explicitly. Override either when
necessary:

```sh
sudo target/release/blockd serve gs://MY_BUCKET/MY_CLUSTER \
  --data-dir /var/opt/blockd \
  --peer 10.10.0.12:7001 \
  --capacity-bytes 1099511627776 \
  --headroom-bytes 107374182400 \
  --firecracker /usr/local/bin/firecracker \
  --firecracker-sha256 "$APPROVED_FIRECRACKER_SHA256"
```

Set `APPROVED_FIRECRACKER_SHA256` from the release manifest for the patched
Firecracker artifact. Startup hashes the executable and refuses a build whose
identity differs; do not derive the approved value from the host at startup.
The `DATA_DIR/blobs` path must be the exact mount point of one whole XFS
filesystem that is not mounted elsewhere; an XFS ancestor or bind mount is not
accepted. Root, swap, userfaultfd, KVM, capacity, and current headroom checks
also complete before the node publishes membership.

Give each machine read/write/list access to the selected prefix and allow the
advertised peer port between machines. A restarted machine reuses
`node.identity` from its data directory; do not copy that file to another
machine. The identity binds the directory to the exact bucket and prefix;
startup fails before remote access if a different store is configured. It also
fails closed if the cluster metadata or this node's durable host-ID claim was
removed or changed. The current production object-store adapter supports GCS.

The data directory is local, durable node state and must not be shared between
machines or two daemon processes. It is created as `0700`, contains an
owner-only `node.identity`, a process lock, the Unix control socket, and the
`blobs/` tree. Startup takes a nonblocking lifetime lock before reading or
changing identity state. A crash releases the kernel lock, while the retained
owner metadata remains diagnostic only. A fast restart keeps the same HostId
and challenges the prior session; it does not silently serve concurrently with
an old process. An old live process defends the challenge, while an undefended
session is reclaimed after the bounded challenge interval.

The daemon exposes `/live`, `/ready`, and `/metrics` on `127.0.0.1:7002` by
default and an owner-only JSON-lines control socket at
`DATA_DIR/control.sock`. Lifecycle operations include `inventory`, `create`,
`restore`, `quarantines`, and the destructive `discard-quarantine`, which
requires a nonempty operator reason and writes synced intent and completion
audit records. Every 32-bit HostId claim is permanent: neither the daemon nor
the deployment IAM policy can delete or recycle it.

`/live` reports only that the daemon's health server is running, including
while local recovery is still in progress. `/ready` additionally requires
current object-store access and membership renewal, a live roster of at least
three nodes, applied cluster placement, authority ownership, healthy recovery,
the peer listener, every supervised critical task, and an unfenced host. Its
503 response names the missing dependencies. Metrics report the distinct
recovering, joined, ready, draining, and fenced lifecycle states and the R9.2
per-volume series using only fixed labels plus the current volume ID; peer IDs
and assignment epochs are metric values rather than labels.

SIGINT and SIGTERM enter the same bounded drain. The daemon first rejects new
control work, migrates each serviceable volume to its durably protected active
peer and waits for source release, then publishes `drained:true`. It waits until
durable authority placement no longer names the host, retires its exact
authority session, and conditionally removes only the membership generation it
owns. A timeout or replaced membership owner makes shutdown fail, so the
service manager uses its failure restart policy instead of reporting an unsafe
clean exit.

Run archive collection separately with `blockd_gc`; storage hosts never start
it implicitly. The collector examines only the `v/` and `b/` archive
namespaces, renews grace when an object generation changes, and has a dedicated
service account in the example infrastructure. Ordinary host DELETE access is
conditioned to archive and membership namespaces rather than the whole bucket.

Membership refresh uses versioned listings whose ETag is a content-stable
fingerprint for the single-request, Google-managed-encryption membership
objects used by the deployment. A heartbeat advances the object generation and
therefore renews the observed lease, but an unchanged fingerprint reuses the
cached body. Only a new member or a changed endpoint, certificate generation,
or drain flag needs a bounded body GET. A backend that omits the fingerprint
remains correct but conservatively fetches each new generation. Cached
authorization still expires on schedule when generations stop advancing,
including during a store outage.

With the production 5-second refresh and heartbeat intervals and GCS's
1,000-object LIST pages, the steady-state request rates are:

| Nodes (`N`) | LIST/s per node | LIST/s cluster-wide | body GET/s per node | body GET/s cluster-wide | heartbeat PUT/s per node | heartbeat PUT/s cluster-wide |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.2 | 0.2 | 0 | 0 | 0.2 | 0.2 |
| 100 | 0.2 | 20 | 0 | 0 | 0.2 | 20 |
| 1,000 | 0.2 | 200 | 0 | 0 | 0.2 | 200 |
| 10,000 | 2 | 20,000 | 0 | 0 | 0.2 | 2,000 |

In general, LIST rate is `ceil(N/1000)/5` per node and
`N*ceil(N/1000)/5` cluster-wide; heartbeat PUT rate is `N/5`. Bootstrap
requires one body GET per observed member (`N` per joining node, or `N*N` if
all nodes bootstrap concurrently). If `C` member bodies change during one
refresh interval, each node performs at most `C/5` body GETs per second and the
cluster performs at most `N*C/5`; identical-body heartbeats add none.

Linux is required for the userfaultfd and Firecracker integration tests. See
[TESTING.md](TESTING.md) for the full test matrix.

## Documentation

- [REQUIREMENTS.md](REQUIREMENTS.md) — system contract
- [DESIGN.md](DESIGN.md) — design decisions
- [docs/adr/0001-object-store-cas-is-the-control-plane.md](docs/adr/0001-object-store-cas-is-the-control-plane.md) — control-plane authority decision
- [demo/README.md](demo/README.md) — local and GCP demos

For deployments, use XFS on a dedicated data volume. Do not reformat or store
daemon data on the boot disk.
