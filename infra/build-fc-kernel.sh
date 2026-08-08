#!/bin/sh
set -eu

KERNEL_VERSION=6.1.141
KERNEL_ARCHIVE="linux-${KERNEL_VERSION}.tar.xz"
KERNEL_SHA256=bc3c45faf6f5f0450666c75fa9dad9bc7c0cf7c7cba0dbd94e5cfdc58229c116

OUTPUT_DIR=${1:?usage: build-fc-kernel.sh OUTPUT_DIR [x86_64|aarch64]}
TARGET_ARCH=${2:-$(uname -m)}
JOBS=${JOBS:-$(getconf _NPROCESSORS_ONLN)}

case "$TARGET_ARCH" in
  x86_64)
    MAKE_ARCH=x86_64
    KERNEL_TARGET=bzImage
    KERNEL_IMAGE=arch/x86/boot/bzImage
    ;;
  aarch64|arm64)
    MAKE_ARCH=arm64
    KERNEL_TARGET=Image
    KERNEL_IMAGE=arch/arm64/boot/Image
    ;;
  *)
    echo "unsupported kernel architecture: $TARGET_ARCH" >&2
    exit 1
    ;;
esac

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT HUP INT TERM

if [ -n "${KERNEL_ARCHIVE_PATH:-}" ]; then
  cp "$KERNEL_ARCHIVE_PATH" "$WORK/$KERNEL_ARCHIVE"
else
  curl -fsSL --retry 5 --retry-delay 2 \
    "https://cdn.kernel.org/pub/linux/kernel/v6.x/${KERNEL_ARCHIVE}" \
    -o "$WORK/$KERNEL_ARCHIVE"
fi
printf '%s  %s\n' "$KERNEL_SHA256" "$WORK/$KERNEL_ARCHIVE" | sha256sum -c -
tar -xJf "$WORK/$KERNEL_ARCHIVE" -C "$WORK"
SOURCE="$WORK/linux-${KERNEL_VERSION}"

make -C "$SOURCE" ARCH="$MAKE_ARCH" defconfig
CONFIG="$SOURCE/scripts/config"
for option in \
  BLK_DEV_INITRD \
  DAX \
  DEVTMPFS \
  DEVTMPFS_MOUNT \
  FS_DAX \
  FUSE_DAX \
  FUSE_FS \
  SERIAL_8250 \
  SERIAL_8250_CONSOLE \
  TMPFS \
  VIRTIO \
  VIRTIO_FS \
  VIRTIO_MMIO \
  VIRTIO_VSOCKETS \
  VSOCKETS \
  ZONE_DEVICE
do
  "$CONFIG" --file "$SOURCE/.config" --enable "$option"
done
"$CONFIG" --file "$SOURCE/.config" --disable DEBUG_INFO
"$CONFIG" --file "$SOURCE/.config" --disable DEBUG_INFO_BTF
make -C "$SOURCE" ARCH="$MAKE_ARCH" olddefconfig
for option in DAX FS_DAX FUSE_DAX FUSE_FS VIRTIO_FS VIRTIO_MMIO VIRTIO_VSOCKETS VSOCKETS; do
  grep -q "^CONFIG_${option}=y$" "$SOURCE/.config" || {
    echo "required kernel option CONFIG_${option}=y was not resolved" >&2
    exit 1
  }
done
make -C "$SOURCE" ARCH="$MAKE_ARCH" -j"$JOBS" "$KERNEL_TARGET"

mkdir -p "$OUTPUT_DIR"
cp "$SOURCE/$KERNEL_IMAGE" "$OUTPUT_DIR/vmlinux"
cp "$SOURCE/.config" "$OUTPUT_DIR/kernel.config"
printf '%s\n%s\n' "$KERNEL_VERSION" "$KERNEL_SHA256" > "$OUTPUT_DIR/KERNEL_SOURCE"
