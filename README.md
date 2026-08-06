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
