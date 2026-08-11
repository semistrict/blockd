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

## GCP

Prerequisites:

- an authenticated `gcloud` CLI;
- a project with the Compute and Storage APIs enabled;
- [OpenTofu](https://opentofu.org).

```sh
tofu -chdir=infra init
tofu -chdir=infra apply -var project=YOUR_PROJECT_ID
./demo/run.sh
```

Each VM uses a separate XFS data disk mounted at
`/var/opt/blockd/blobs`. Provisioning does not format the boot disk. Set
`-var data_disk_size_gb=N` to change the data-disk size.

The APIs are reachable only inside the VPC; the demo script connects through
an IAP SSH tunnel.

Destroy all demo resources when finished:

```sh
tofu -chdir=infra destroy -var project=YOUR_PROJECT_ID
```

This deletes the VMs, disks, network, service account, bucket, and bucket
contents.
