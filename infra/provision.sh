#!/usr/bin/env bash
# GCE startup script: build the production storage host from source on first
# boot — blockd, the patched Firecracker, the guest initramfs, and the CI
# kernel — then run `blockd serve` under systemd. Progress lands in
# /var/log/blockd-provision.log; /var/opt/blockd/.ready marks completion.
# Idempotent: re-runs (every boot) skip straight to the service.
set -euo pipefail

validate_repo_commit() {
  local commit=$1
  if [[ ! "$commit" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "blockd-repo-ref must be a full 40-hex commit ID" >&2
    return 1
  fi
}

checkout_repo_at_commit() {
  local repository=$1
  local commit=$2
  local destination=$3
  validate_repo_commit "$commit"
  commit=$(printf '%s' "$commit" | tr 'A-F' 'a-f')
  if [ ! -d "$destination/.git" ]; then
    git clone --no-checkout --filter=blob:none "$repository" "$destination"
  fi
  git -C "$destination" remote set-url origin "$repository"
  git -C "$destination" fetch --force --no-tags --depth 1 origin "$commit"
  local resolved
  resolved=$(git -C "$destination" rev-parse --verify 'FETCH_HEAD^{commit}')
  if [ "$resolved" != "$commit" ]; then
    echo "fetched blockd commit does not match blockd-repo-ref" >&2
    return 1
  fi
  git -C "$destination" checkout --detach --force "$resolved"
  git -C "$destination" clean -dffx
  if [ "$(git -C "$destination" rev-parse --verify HEAD)" != "$commit" ] \
      || [ -n "$(git -C "$destination" status --porcelain)" ]; then
    echo "blockd checkout is not the requested clean detached commit" >&2
    return 1
  fi
}

installed_deployment_matches() {
  local ready=$1
  local installed_deployment=$2
  local requested_deployment=$3
  [ -f "$ready" ] \
    && [ -f "$installed_deployment" ] \
    && [ "$(cat "$installed_deployment")" = "$requested_deployment" ]
}

# Keep immutable-ref validation and checkout behavior executable without root
# side effects for provisioning policy tests and operator diagnostics.
if [[ "${1:-}" == "--validate-repo-commit" ]]; then
  validate_repo_commit "${2:-}"
  exit
fi
if [[ "${1:-}" == "--checkout-repo" ]]; then
  checkout_repo_at_commit "${2:-}" "${3:-}" "${4:-}"
  exit
fi
if [[ "${1:-}" == "--ready-for-deployment" ]]; then
  installed_deployment_matches "${2:-}" "${3:-}" "${4:-}"
  exit
fi

exec >> /var/log/blockd-provision.log 2>&1

READY=/var/opt/blockd/.ready
INSTALLED_REVISION=/var/opt/blockd/.installed-revision
INSTALLED_DEPLOYMENT=/var/opt/blockd/.installed-deployment
APT_SNAPSHOT=20250818T000000Z
APT_SOURCE=/etc/apt/sources.list.d/blockd-snapshot.sources
echo "$(date -u) provisioning starts"

export DEBIAN_FRONTEND=noninteractive
APT_UPDATED=0
configure_apt_snapshot() {
  cat > "$APT_SOURCE" <<EOF
Types: deb
URIs: https://snapshot.ubuntu.com/ubuntu/$APT_SNAPSHOT/
Suites: noble noble-updates noble-security
Components: main universe restricted multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
EOF
}
apt_snapshot() {
  apt-get \
    -o "Dir::Etc::sourcelist=$APT_SOURCE" \
    -o 'Dir::Etc::sourceparts=-' \
    "$@"
}
apt_update() {
  if [ "$APT_UPDATED" = 0 ]; then
    configure_apt_snapshot
    apt_snapshot update
    APT_UPDATED=1
  fi
}

# The daemon's dedicated data disk. Never infer this device from enumeration:
# the GCE device name gives it a stable path and prevents the boot disk from
# ever becoming a formatting candidate.
BLOB_DEVICE=/dev/disk/by-id/google-blockd-data
BLOB_MOUNT=/var/opt/blockd/blobs
systemctl stop blockd 2>/dev/null || true
if ! command -v mkfs.xfs >/dev/null 2>&1; then
  apt_update
  apt_snapshot install -y xfsprogs
fi

udevadm settle
for _ in $(seq 1 30); do
  [ -b "$BLOB_DEVICE" ] && break
  sleep 1
done
if [ ! -b "$BLOB_DEVICE" ]; then
  echo "dedicated data disk did not appear at $BLOB_DEVICE"
  exit 1
fi

ROOT_SOURCE=$(findmnt -n -o SOURCE /)
ROOT_REAL=$(readlink -f "$ROOT_SOURCE")
ROOT_PARENT=$(lsblk -nro PKNAME "$ROOT_REAL" | head -n 1)
BLOB_REAL=$(readlink -f "$BLOB_DEVICE")
if [ "$BLOB_REAL" = "$ROOT_REAL" ] || [ "$BLOB_REAL" = "/dev/$ROOT_PARENT" ]; then
  echo "refusing to use root device $ROOT_SOURCE as the blob volume"
  exit 1
fi

BLOB_SIGNATURES=$(wipefs -n --noheadings --output TYPE "$BLOB_DEVICE" | xargs)
BLOB_WAS_BLANK=0
case "$BLOB_SIGNATURES" in
  "")
    echo "$(date -u) formatting blank blob volume as XFS"
    mkfs.xfs -s size=4096 -L blockd-blobs "$BLOB_DEVICE"
    BLOB_WAS_BLANK=1
    ;;
  xfs)
    echo "$(date -u) existing XFS blob volume found"
    ;;
  *)
    echo "refusing to overwrite $BLOB_DEVICE: found signatures $BLOB_SIGNATURES"
    exit 1
    ;;
esac

mkdir -p "$BLOB_MOUNT"
# On an in-place infrastructure upgrade, preserve any blobs that predate the
# dedicated disk. The boot-disk copy is deliberately left untouched beneath
# the mount point until an operator chooses to remove it.
if [ "$BLOB_WAS_BLANK" = 1 ] && ! mountpoint -q "$BLOB_MOUNT" \
    && find "$BLOB_MOUNT" -mindepth 1 -print -quit | grep -q .; then
  BLOB_STAGING=/mnt/blockd-blobs
  mkdir -p "$BLOB_STAGING"
  mount "$BLOB_DEVICE" "$BLOB_STAGING"
  cp -a "$BLOB_MOUNT/." "$BLOB_STAGING/"
  sync -f "$BLOB_STAGING"
  umount "$BLOB_STAGING"
fi
BLOB_UUID=$(blkid -s UUID -o value "$BLOB_DEVICE")
sed -i '\|[[:space:]]/var/opt/blockd/blobs[[:space:]]|d' /etc/fstab
printf 'UUID=%s %s xfs defaults,noatime,nofail 0 2\n' "$BLOB_UUID" "$BLOB_MOUNT" >> /etc/fstab
if ! mountpoint -q "$BLOB_MOUNT"; then
  mount "$BLOB_MOUNT"
fi
if [ "$(findmnt -n -o FSTYPE --target "$BLOB_MOUNT")" != xfs ]; then
  echo "$BLOB_MOUNT is not mounted as XFS"
  exit 1
fi
systemctl enable --now fstrim.timer
BLOB_DEVICE_BYTES=$(blockdev --getsize64 "$BLOB_DEVICE")
# Keep ten percent entirely outside the daemon's budget, then begin reclaim
# another ten percent before that budget is exhausted.
BLOB_CAPACITY_BYTES=$((BLOB_DEVICE_BYTES * 9 / 10))
BLOB_HEADROOM_BYTES=$((BLOB_DEVICE_BYTES / 10))

meta() {
  curl -sf -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"
}
if ! command -v curl >/dev/null 2>&1; then
  apt_update
  apt_snapshot install -y curl
fi
REPO_REF=$(meta blockd-repo-ref)
validate_repo_commit "$REPO_REF"
REQUESTED_DEPLOYMENT_ID=$(meta blockd-deployment-id)
if [[ ! "$REQUESTED_DEPLOYMENT_ID" =~ ^[0-9a-f]{64}$ ]]; then
  echo "blockd-deployment-id must be a lowercase SHA-256 digest"
  exit 1
fi
DEPLOYMENT_ID=$(printf '%s\n%s\n' "$REQUESTED_DEPLOYMENT_ID" "$BLOB_DEVICE_BYTES" | sha256sum | awk '{print $1}')
if installed_deployment_matches "$READY" "$INSTALLED_DEPLOYMENT" "$DEPLOYMENT_ID"; then
  systemctl start blockd || true
  echo "$(date -u) already provisioned at deployment $DEPLOYMENT_ID"
  exit 0
fi
SELF_IP=$(meta blockd-peer-ip)
BUCKET=$(meta blockd-bucket)
PREFIX=$(meta blockd-prefix)
REPO=$(meta blockd-repo)
apt_update
apt_snapshot install -y \
  bc bison build-essential cpio curl flex git libelf-dev libseccomp-dev \
  libssl-dev openssl pkg-config xfsprogs

# Rust, system-wide (the toolchain the repo pins).
export RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo
RUSTUP_INIT=/var/tmp/blockd-rustup-init.$$
RUSTUP_INIT_SHA256=20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c
trap 'rm -f "$RUSTUP_INIT"' EXIT
curl -sSf \
  https://static.rust-lang.org/rustup/archive/1.28.2/x86_64-unknown-linux-gnu/rustup-init \
  -o "$RUSTUP_INIT"
printf '%s  %s\n' "$RUSTUP_INIT_SHA256" "$RUSTUP_INIT" | sha256sum -c -
chmod 0700 "$RUSTUP_INIT"
"$RUSTUP_INIT" -y --no-modify-path --default-toolchain none
rm -f "$RUSTUP_INIT"
trap - EXIT
export PATH=/opt/cargo/bin:$PATH

# The repo, at the requested immutable commit. The exact clean detached tree is
# verified before any root-owned production artifact is built.
checkout_repo_at_commit "$REPO" "$REPO_REF" /opt/blockd
cd /opt/blockd
rustup target add x86_64-unknown-linux-musl

echo "$(date -u) building production daemon"
cargo build --locked --release -p blockd-runtime --bin blockd

FC_DIR=/var/opt/blockd/fc
mkdir -p "$FC_DIR"

echo "$(date -u) building the guest initramfs (static musl PID 1)"
cargo build --locked --release -p blockd-fc-guest --target x86_64-unknown-linux-musl
rm -rf "$FC_DIR/initramfs"
mkdir -p "$FC_DIR/initramfs/dev"
cp target/x86_64-unknown-linux-musl/release/blockd-fc-guest "$FC_DIR/initramfs/init"
(cd "$FC_DIR/initramfs" && find . | cpio -o -H newc > "$FC_DIR/initramfs.cpio")

echo "$(date -u) building patched Firecracker (UffdShmem backend)"
FC_COMMIT=f79d660a379fed936d9234adad3298c0acc9bcd5
checkout_repo_at_commit \
  https://github.com/firecracker-microvm/firecracker \
  "$FC_COMMIT" \
  /opt/firecracker
cd /opt/firecracker
git apply --check /opt/blockd/patches/firecracker-uffd-shmem.patch
git apply /opt/blockd/patches/firecracker-uffd-shmem.patch
git diff --check
cargo build --locked --release -p firecracker
cp build/cargo_target/release/firecracker "$FC_DIR/firecracker"

echo "$(date -u) fetching the pinned Firecracker CI guest kernel"
KERNEL_OBJECT=firecracker-ci/v1.13/x86_64/vmlinux-6.1.141
KERNEL_SHA256=b36a4a1b10f33b9cfdcde3d1a787d9c090556a3edb211cd06d1f3f9a6c7e8724
curl -sf "https://s3.amazonaws.com/spec.ccfc.min/$KERNEL_OBJECT" -o "$FC_DIR/vmlinux.pending"
printf '%s  %s\n' "$KERNEL_SHA256" "$FC_DIR/vmlinux.pending" | sha256sum -c -
mv "$FC_DIR/vmlinux.pending" "$FC_DIR/vmlinux"

# The daemon opens /dev/kvm and /dev/userfaultfd; make both world-usable
# (the demo VM is single-purpose).
cat > /etc/udev/rules.d/99-blockd.rules <<'EOF'
KERNEL=="kvm", MODE="0666"
KERNEL=="userfaultfd", MODE="0666"
EOF
udevadm control --reload-rules && udevadm trigger || true

mkdir -p /var/opt/blockd/scratch
FIRECRACKER_SHA256=$(sha256sum "$FC_DIR/firecracker" | awk '{print $1}')
install -o root -g root -m 0755 /opt/blockd/target/release/blockd /usr/local/bin/blockd
install -o root -g root -m 0644 /opt/blockd/infra/blockd.service /etc/systemd/system/blockd.service
install -d -o root -g root -m 0700 /etc/blockd /var/opt/blockd
cat > /etc/blockd/blockd.env <<EOF
BLOCKD_STORE=gs://$BUCKET/$PREFIX
BLOCKD_CAPACITY_BYTES=$BLOB_CAPACITY_BYTES
BLOCKD_HEADROOM_BYTES=$BLOB_HEADROOM_BYTES
BLOCKD_FIRECRACKER=$FC_DIR/firecracker
BLOCKD_FIRECRACKER_SHA256=$FIRECRACKER_SHA256
BLOCKD_PEER=$SELF_IP:7001
BLOCKD_HEALTH=127.0.0.1:7002
EOF
chown root:root /etc/blockd/blockd.env
chmod 0600 /etc/blockd/blockd.env
systemctl daemon-reload
systemctl enable --now blockd

printf '%s\n' "$REPO_REF" > "$INSTALLED_REVISION.pending"
mv "$INSTALLED_REVISION.pending" "$INSTALLED_REVISION"
printf '%s\n' "$DEPLOYMENT_ID" > "$INSTALLED_DEPLOYMENT.pending"
mv "$INSTALLED_DEPLOYMENT.pending" "$INSTALLED_DEPLOYMENT"
touch "$READY"
sync -f /var/opt/blockd
echo "$(date -u) provisioning done"
