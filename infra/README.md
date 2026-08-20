# Production infrastructure

This composition requires the exact OpenTofu and provider versions committed
with it. Install the verified runner, initialize without changing the lock, and
provide the immutable blockd commit:

```sh
sudo ./install-tofu.sh /usr/local/bin/tofu
tofu init -lockfile=readonly
tofu plan -var repo_ref=FULL_40_HEX_COMMIT
```

The default base image is one exact dated Ubuntu image. OpenTofu and the
provider are pinned for repeatable plans. Workload inputs—blockd revision,
provisioning script and unit, image, store/prefix, peer address, and disk
size—produce the deployment fingerprint; apt, Rust, Firecracker, and kernel
pins flow through the script hash. Hosts compare that fingerprint plus the
observed data-disk size before taking the provisioned fast path.

HostId claims are permanent. The provisioned runtime and operator policies do
not grant claim deletion, so retiring a machine leaves its numeric identity
reserved forever.
