# Demo

The demo runs two blockd hosts, an object store, and Firecracker microVMs. It
covers snapshot publication, demand restore, copy-on-write forks, post-copy
migration, and recovery after host loss.

## Local

Run the demo in a Lima VM with Firecracker installed:

```sh
limactl shell default -- bash -c \
  'CARGO_TARGET_DIR=/var/tmp/blockd-target BLOCKD_FC_DIR=/var/tmp/blockd-fc ./demo/smoke-lima.sh'
```

This starts a local GCS-compatible server and two daemon processes.

## GCP production composition

Prerequisites:

- an authenticated `gcloud` CLI;
- a project with the Compute and Storage APIs enabled;
- [OpenTofu](https://opentofu.org).

```sh
tofu -chdir=infra init
tofu -chdir=infra apply -var project=YOUR_PROJECT_ID
```

The `infra/` composition intentionally deploys three production `blockd serve`
hosts plus the separately credentialed archive collector; it does not install
or start `demod`. The interactive demo remains the local Lima composition
above. Each production daemon generates its TLS identity at first startup and
publishes its public certificate and reachable endpoint to GCS. Nodes discover
one another exclusively from those records.

Each VM uses a separate XFS data disk mounted at
`/var/opt/blockd/blobs`. Provisioning does not format the boot disk. Set
`-var data_disk_size_gb=N` to change the data-disk size.

Peer traffic is reachable only inside the VPC. Health endpoints remain bound
to loopback and can be inspected through an IAP SSH tunnel.

Destroy all production-composition resources when finished:

```sh
tofu -chdir=infra destroy -var project=YOUR_PROJECT_ID
```

This deletes the VMs, disks, network, service accounts, bucket, and bucket
contents.
