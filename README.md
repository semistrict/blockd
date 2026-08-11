# blockd

Page-granular storage for Firecracker sandbox fleets. One daemon per host
manages guest memory, durable disk state, and independently attachable SQLite
vsets, with asynchronous archival to object storage.

## Build and test

```sh
cargo build --workspace
cargo test --workspace
```

Linux is required for the userfaultfd and Firecracker integration tests. See
[TESTING.md](TESTING.md) for the full test matrix.

## Documentation

- [REQUIREMENTS.md](REQUIREMENTS.md) — system contract
- [DESIGN.md](DESIGN.md) — design decisions
- [demo/README.md](demo/README.md) — local and GCP demos

For deployments, use XFS on a dedicated data volume. Do not reformat or store
daemon data on the boot disk.
