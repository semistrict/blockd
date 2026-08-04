#!/usr/bin/env bash
# GCE startup script: build the whole blockd demo stack from source on
# first boot — the daemon, the patched Firecracker, the guest initramfs,
# the CI kernel — then run demod under systemd. Progress lands in
# /var/log/blockd-provision.log; /var/opt/blockd/.ready marks completion.
# Idempotent: re-runs (every boot) skip straight to the service.
set -euo pipefail
exec >> /var/log/blockd-provision.log 2>&1

READY=/var/opt/blockd/.ready
if [ -f "$READY" ]; then
  systemctl start blockd-demod || true
  echo "$(date -u) already provisioned"
  exit 0
fi
echo "$(date -u) provisioning starts"

meta() {
  curl -sf -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"
}
HOST_ID=$(meta blockd-host-id)
PEER0=$(meta blockd-peer0-ip)
PEER1=$(meta blockd-peer1-ip)
BUCKET=$(meta blockd-bucket)
REPO=$(meta blockd-repo)
REPO_REF=$(meta blockd-repo-ref)
SELF_IP=$([ "$HOST_ID" = 0 ] && echo "$PEER0" || echo "$PEER1")

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential curl git pkg-config libseccomp-dev cpio

# Rust, system-wide (the toolchain the repo pins).
export RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo
if [ ! -x /opt/cargo/bin/cargo ]; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path
fi
export PATH=/opt/cargo/bin:$PATH

# The repo, at the requested ref.
if [ ! -d /opt/blockd ]; then
  git clone "$REPO" /opt/blockd
fi
cd /opt/blockd
git fetch origin "$REPO_REF" && git checkout FETCH_HEAD
rustup target add x86_64-unknown-linux-musl

echo "$(date -u) building demod"
cargo build --release -p blockd-demod

FC_DIR=/var/opt/blockd/fc
mkdir -p "$FC_DIR"

echo "$(date -u) building the guest initramfs (static musl PID 1)"
cargo build --release -p blockd-fc-guest --target x86_64-unknown-linux-musl
rm -rf "$FC_DIR/initramfs"
mkdir -p "$FC_DIR/initramfs/dev"
cp target/x86_64-unknown-linux-musl/release/blockd-fc-guest "$FC_DIR/initramfs/init"
(cd "$FC_DIR/initramfs" && find . | cpio -o -H newc > "$FC_DIR/initramfs.cpio")

echo "$(date -u) building patched Firecracker (UffdShmem backend)"
FC_COMMIT=f79d660
if [ ! -d /opt/firecracker ]; then
  mkdir -p /opt/firecracker
  cd /opt/firecracker
  git init -q
  git remote add origin https://github.com/firecracker-microvm/firecracker
  git fetch --depth 1 origin "$FC_COMMIT"
  git checkout -q FETCH_HEAD
  git apply /opt/blockd/patches/firecracker-uffd-shmem.patch
fi
cd /opt/firecracker
cargo build --release -p firecracker
cp target/release/firecracker "$FC_DIR/firecracker"

echo "$(date -u) fetching the Firecracker CI guest kernel"
ARCH=x86_64
LATEST=$(curl -sf "http://spec.ccfc.min.s3.amazonaws.com/?prefix=firecracker-ci/v1.13/$ARCH/vmlinux-6.1&list-type=2" \
  | grep -oP "(?<=<Key>)(firecracker-ci/v1.13/$ARCH/vmlinux-6\.1\.\d+)(?=</Key>)" | sort -V | tail -1)
curl -sf "https://s3.amazonaws.com/spec.ccfc.min/$LATEST" -o "$FC_DIR/vmlinux"

# The daemon opens /dev/kvm and /dev/userfaultfd; make both world-usable
# (the demo VM is single-purpose).
cat > /etc/udev/rules.d/99-blockd.rules <<'EOF'
KERNEL=="kvm", MODE="0666"
KERNEL=="userfaultfd", MODE="0666"
EOF
udevadm control --reload-rules && udevadm trigger || true

mkdir -p /var/opt/blockd/blobs /var/opt/blockd/scratch
cat > /var/opt/blockd/demod.conf <<EOF
host = $HOST_ID
api = $SELF_IP:7000
peer_listen = $SELF_IP:7001
peer.0 = $PEER0:7001
peer.1 = $PEER1:7001
gcs_endpoint = https://storage.googleapis.com
gcs_metadata = http://metadata.google.internal
gcs_bucket = $BUCKET
gcs_prefix = blockd/
blob_dir = /var/opt/blockd/blobs
scratch = /var/opt/blockd/scratch
shmem_dir = /dev/shm
fc_dir = $FC_DIR
EOF

cat > /etc/systemd/system/blockd-demod.service <<EOF
[Unit]
Description=blockd demo daemon
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/opt/blockd/target/release/demod /var/opt/blockd/demod.conf
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now blockd-demod

touch "$READY"
echo "$(date -u) provisioning done"
