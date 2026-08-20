#!/bin/bash
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive
export RUSTUP_HOME=/opt/rustup
export CARGO_HOME=/opt/cargo
export PATH=/opt/cargo/bin:$PATH
APT_SNAPSHOT=20250818T000000Z
APT_SOURCE=/etc/apt/sources.list.d/blockd-snapshot.sources

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

validate_object_prefix() {
  local prefix=$1
  if [[ -n "$prefix" && ! "$prefix" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*/?$ ]]; then
    echo "invalid blockd object prefix" >&2
    return 1
  fi
}

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

# Keep the production parser directly executable by policy tests and operators.
if [[ "${1:-}" == "--validate-prefix" ]]; then
  validate_object_prefix "${2:-}"
  exit
fi
if [[ "${1:-}" == "--validate-repo-commit" ]]; then
  validate_repo_commit "${2:-}"
  exit
fi
if [[ "${1:-}" == "--checkout-repo" ]]; then
  checkout_repo_at_commit "${2:-}" "${3:-}" "${4:-}"
  exit
fi

meta() {
  curl -sf -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"
}

configure_apt_snapshot
apt_snapshot update
apt_snapshot install -y curl
BUCKET=$(meta blockd-bucket)
PREFIX=$(meta blockd-prefix)
REPO=$(meta blockd-repo)
REPO_REF=$(meta blockd-repo-ref)
validate_object_prefix "$PREFIX"
validate_repo_commit "$REPO_REF"

apt_snapshot install -y build-essential git libssl-dev pkg-config
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
checkout_repo_at_commit "$REPO" "$REPO_REF" /opt/blockd
cd /opt/blockd
cargo build --locked --release -p blockd-runtime --bin blockd_gc

if ! getent group blockd-gc >/dev/null; then
  groupadd --system blockd-gc
fi
if ! id blockd-gc >/dev/null 2>&1; then
  useradd --system --gid blockd-gc --home-dir /nonexistent --shell /usr/sbin/nologin blockd-gc
fi
install -o root -g root -m 0755 target/release/blockd_gc /usr/local/bin/blockd_gc
install -o root -g root -m 0644 infra/blockd-gc.service /etc/systemd/system/blockd-gc.service
install -d -o root -g blockd-gc -m 0750 /etc/blockd
printf '%s\n' \
  "BLOCKD_STORE=gs://$BUCKET/$PREFIX" \
  'BLOCKD_GC_INTERVAL_SECONDS=60' \
  'BLOCKD_GC_GRACE_SECONDS=600' \
  > /etc/blockd/collector.env
chown root:blockd-gc /etc/blockd/collector.env
chmod 0640 /etc/blockd/collector.env
systemctl daemon-reload
systemctl enable --now blockd-gc.service
