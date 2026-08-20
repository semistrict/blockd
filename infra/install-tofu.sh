#!/usr/bin/env bash
set -euo pipefail

VERSION=1.12.5
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  arm64 | aarch64) ARCH=arm64 ;;
  *) echo "unsupported OpenTofu architecture: $(uname -m)" >&2; exit 1 ;;
esac

case "$OS/$ARCH" in
  darwin/amd64) SHA256=1012d8f3d4567bcbcd1f2c7d766feca39a30bced32fb8be47e1887fbbee2456d ;;
  darwin/arm64) SHA256=2ae38150a667f5c0bd57b318d18ad8091d08f93fcca40345f3d88998661de5a9 ;;
  linux/amd64) SHA256=a6894d45ae7a17ce83189cce8fe04b5a65f68cefceb62455b5a6a89fa53ab38f ;;
  linux/arm64) SHA256=e67e9da2b1ddf5050ebee62a584cb826eafe1dfd3827d7ec20899ac62791ed1a ;;
  *) echo "unsupported OpenTofu platform: $OS/$ARCH" >&2; exit 1 ;;
esac

DESTINATION=${1:-/usr/local/bin/tofu}
ARCHIVE="tofu_${VERSION}_${OS}_${ARCH}.tar.gz"
TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/blockd-tofu.XXXXXX")
trap 'rm -rf "$TEMP_DIR"' EXIT
curl -fsSL "https://github.com/opentofu/opentofu/releases/download/v${VERSION}/${ARCHIVE}" \
  -o "$TEMP_DIR/$ARCHIVE"
if command -v sha256sum >/dev/null 2>&1; then
  printf '%s  %s\n' "$SHA256" "$TEMP_DIR/$ARCHIVE" | sha256sum -c -
else
  [ "$(shasum -a 256 "$TEMP_DIR/$ARCHIVE" | awk '{print $1}')" = "$SHA256" ]
fi
tar -xzf "$TEMP_DIR/$ARCHIVE" -C "$TEMP_DIR" tofu
install -m 0755 "$TEMP_DIR/tofu" "$DESTINATION"
"$DESTINATION" version
