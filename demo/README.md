# The blockd demo

Two hosts, one object store, real Firecracker microVMs. The story:

1. **Bake**: boot a template guest, work it, snapshot it, publish the
   snapshot to the store.
2. **Start**: a microVM restores from the store-held snapshot on demand
   (segment-granular fetches with readahead — no full download), paired
   with a blockd **vset** that carries its durable state. Guest work is
   mirrored into the vset and synced: every burst is a durable
   consistency point.
3. **Fork**: snapshot the live VM, start N forks off that one snapshot.
   They share one physical memory copy on the host (kernel-verified:
   ΣPss < ΣRss), diverging copy-on-write.
4. **Migrate**: the vset moves to the other host **live** — post-copy
   over TCP, ~10ms handoff, demand fetches from the source until
   hydration drains and the source reclaims to zero. The microVM itself
   re-restores from its snapshot via the store.
5. **Host death**: kill a host outright. A backed vset restores on the
   survivor **from the bucket alone**, byte-verified.

Stated limitation: VM RAM divergence is Firecracker-level (snapshots),
not daemon-persisted — that needs a `MAP_SHARED`+uffd-wp memory backend,
a later milestone. The vset (the durable state) is what blockd manages.

## Local (Lima, no cloud)

Runs the whole story on one machine: a fake GCS (real HTTP, real
`GcsStore` client), two demod processes, real Firecracker:

```bash
limactl shell default -- bash -c \
  'CARGO_TARGET_DIR=/var/tmp/blockd-target BLOCKD_FC_DIR=/var/tmp/blockd-fc ./demo/smoke-lima.sh'
```

## GCP

Prereqs: `gcloud` authenticated, a project with the Compute and Storage
APIs enabled, [OpenTofu](https://opentofu.org) installed.

```bash
cd infra
tofu init
tofu apply -var project=YOUR_PROJECT_ID     # ~2× n2-standard-4 (Spot) + a bucket
cd ..
./demo/run.sh                               # waits out first-boot builds (~15 min), then the story
```

Teardown (removes the VMs, network, service account, and the bucket
with everything in it):

```bash
tofu -chdir=infra destroy -var project=YOUR_PROJECT_ID
```

Cost while up: two Spot `n2-standard-4` (~$0.10/h each), two 50GB
pd-ssd, cents of GCS. The APIs are VPC-internal only; `run.sh` reaches
them through an IAP SSH tunnel.

Extra validation on a VM (the store adapter against the real bucket):

```bash
gcloud compute ssh blockd-demo-0 --zone ZONE --tunnel-through-iap
sudo su -; cd /opt/blockd
BLOCKD_GCS_TEST_BUCKET=$(curl -sf -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/blockd-bucket) \
  PATH=/opt/cargo/bin:$PATH cargo test -p blockd-runtime --test gcs_store -- --ignored
```
