# blockd

blockd is a page-granular storage backend for Firecracker sandbox fleets.
One daemon per host owns guest RAM and pmem disk state, commits it locally,
and optionally publishes it asynchronously to an object store. See
`REQUIREMENTS.md` for the system contract and `DESIGN.md` for the standing
design decisions.

## Local filesystem recommendation

Use **XFS on a dedicated node data volume** for the daemon's blob directory.
Do not place the blob directory directly on the boot filesystem, and never
reformat the boot disk during provisioning.

XFS is the recommended substrate because blockd already owns the storage
semantics above the filesystem:

- write-once compressed segments contain page data;
- checksummed map leaves locate pages in segments;
- mirrored journal records define atomic recovery points;
- application-level fencing, backup, compaction, and reclamation determine
  reachability.

Filesystem snapshots cannot replace that metadata: they do not contain the
page generation, sync watermark, ownership fence, migration state, remote
backup status, or per-segment live-byte information. XFS therefore provides
the useful lower layer—mature metadata journaling, predictable `fsync`, good
concurrent blob I/O, and a conventional path to direct I/O—without adding a
second snapshot lifecycle or data cache.

Recommended starting configuration:

- a dedicated SSD volume with a 4 KiB sector size;
- XFS V5 defaults;
- `noatime`;
- normal barriers and `fsync` semantics;
- periodic trim;
- no filesystem compression or deduplication, because segment entries are
  already compressed and copied verbatim to remote storage.

The daemon must retain its own conservative capacity, reachability, and
in-flight-write accounting. Filesystem free-space reporting is useful for
reconciliation and alerting, not as a replacement for those correctness
decisions.

## GCP demo storage

The GCP configuration attaches one dedicated SSD Persistent Disk to each VM.
The startup script identifies it through the stable
`/dev/disk/by-id/google-blockd-data` path, verifies that it is not the root
device, formats it as XFS only when it has no filesystem signature, and mounts
it at `/var/opt/blockd/blobs`.

Using a Persistent Disk instead of reformatting the boot disk isolates daemon
data from the operating system and preserves local recovery state across Spot
VM stops. Deleting the infrastructure still deletes the data disks. For a
production deployment requiring physically attached NVMe latency, use Local
SSD or equivalent hardware with the same XFS mount arrangement; its loss must
be treated as host loss and recovered from remote backup.

See `demo/README.md` for local and GCP demo instructions.

## Observability

The demo daemon writes newline-delimited JSON events to stderr. Set `RUST_LOG`
to adjust the filter; the default enables `info` events from the daemon and
runtime. Control API requests and VM lifecycle operations are emitted as
structured spans, and request events include trace and span IDs when trace
export is enabled.

`GET /metrics` on the control API exposes Prometheus text format with bounded
label keys. Prometheus scraping does not require a telemetry collector. The
operator-facing groups include:

- end-to-end page-fault latency by final source (`zero`, shared memory,
  write-protect, local NVMe, peer, object store, or unservable), with aggregate
  histograms plus per-vset counts and cumulative time;
- Firecracker snapshot-memory fault latency, object-store latency, local blob
  I/O latency, synchronous create/checkpoint/restore/migration/sync latency,
  and the guest-visible checkpoint/migration pause;
- event-loop occupancy, time attribution, and critical/background queue depth;
- process CPU, resident/virtual memory, threads, and file-descriptor usage;
- cache capacity, residency, dirty/unstable/reserved pages, parked faults, and
  current memory-pressure waiters;
- NVMe capacity, headroom, total use, live segment bytes, and stored segment
  bytes, plus actual capacity/available bytes for the backing filesystem;
- current per-vset lifecycle, dirty and unstable pages, parked faults,
  hydration remainder, pending syncs and map leaves, backup lag by captures,
  bytes and age, background-operation active age, and segment space;
- assignment claims/conflicts/fences, liveness wedges, per-peer connectivity,
  peer drops, incidents, store failures/retries, transferred bytes, and the
  existing correctness counters.

Classic histograms are aggregated by bounded source/operation labels. Per-vset
fault series expose count and cumulative duration without histogram buckets;
this preserves per-vset diagnosis without multiplying every live vset by every
bucket. Retired vsets disappear from the point-in-time exposition.

The daemon configuration accepts `cache_pages`, `writeback_interval_ms`,
`backup_retry_ms`, `disk_capacity_bytes`, `disk_headroom_bytes`, and
`wedge_ticks`. Defaults preserve the demo profile, but production deployments
should set disk capacity and headroom conservatively for the dedicated blob
volume; filesystem free space remains a separate reconciliation signal.

OTLP trace export is disabled unless `OTEL_EXPORTER_OTLP_ENDPOINT` or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. Export uses OTLP/HTTP protobuf and
the SDK's background batch processor, reusing the daemon's synchronous HTTP
and TLS stack. Standard settings including
`OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_TRACES_SAMPLER`,
`OTEL_EXPORTER_OTLP_HEADERS`, and the signal-specific variants are honored.
Inbound W3C `traceparent` and `tracestate` headers establish the parent of each
control API server span. Set `OTEL_TRACES_EXPORTER=none` or
`OTEL_SDK_DISABLED=true` to force trace export off.
