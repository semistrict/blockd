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
sudo target/release/blockd serve gs://MY_BUCKET/MY_CLUSTER
```

The bucket and prefix are the cluster identity. The daemon uses the machine's
ambient GCE service-account credentials, chooses and durably claims a host ID,
publishes its ephemeral mTLS identity, and continuously derives replica
placement from the live records in that prefix. Concurrent first nodes safely
race to create the cluster metadata and cannot claim the same host ID.

By default, local state is stored in `/var/lib/blockd` and the peer service is
advertised on the private address selected by the metadata-server route at
port 7001. Override either when necessary:

```sh
sudo target/release/blockd serve gs://MY_BUCKET/MY_CLUSTER \
  --data-dir /var/opt/blockd \
  --peer 10.10.0.12:7001
```

Give each machine read/write/list access to the selected prefix and allow the
advertised peer port between machines. A restarted machine reuses
`node.identity` from its data directory; do not copy that file to another
machine. The identity binds the directory to the exact bucket and prefix;
startup fails before remote access if a different store is configured. It also
fails closed if the cluster metadata or this node's durable host-ID claim was
removed or changed. The current production object-store adapter supports GCS.

Linux is required for the userfaultfd and Firecracker integration tests. See
[TESTING.md](TESTING.md) for the full test matrix.

## Documentation

- [REQUIREMENTS.md](REQUIREMENTS.md) — system contract
- [DESIGN.md](DESIGN.md) — design decisions
- [demo/README.md](demo/README.md) — local and GCP demos

For deployments, use XFS on a dedicated data volume. Do not reformat or store
daemon data on the boot disk.
