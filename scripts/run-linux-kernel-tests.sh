#!/usr/bin/env bash
# Exercise the real Linux seam without requiring Firecracker artifacts.
set -euo pipefail

if [[ $# -gt 1 ]]; then
    echo "usage: $0 [artifact-dir]" >&2
    exit 2
fi

artifact_dir=${1:-artifacts/linux-kernel}
mkdir -p "$artifact_dir"
log_file="$artifact_dir/linux-kernel.log"

abi_binary="${TMPDIR:-/tmp}/blockd-userfaultfd-abi-$$"
cleanup() {
    status=$?
    trap - EXIT
    rm -f "$abi_binary"
    exec 1>&3 2>&4
    cat "$log_file"
    exit "$status"
}
exec 3>&1 4>&2
trap cleanup EXIT
exec >"$log_file" 2>&1

if revision=$(git rev-parse HEAD 2>/dev/null); then
    echo "revision: $revision"
else
    echo "revision: unavailable"
fi
uname -a
printf 'host page size: '
getconf PAGESIZE
rustc -vV
if [[ -r /proc/sys/vm/unprivileged_userfaultfd ]]; then
    printf 'vm.unprivileged_userfaultfd: '
    cat /proc/sys/vm/unprivileged_userfaultfd
fi

echo "checking userfaultfd constants against Linux headers"
cc -std=c11 -Wall -Wextra -Werror scripts/check-userfaultfd-abi.c -o "$abi_binary"
"$abi_binary"

tests=(
    "blockd-hostmem:uffd_linux"
    "blockd-hostmem:vm_fleet_linux"
    "blockd-runtime:e2e_linux"
    "blockd-runtime:workload_e2e_linux"
    "blockd-runtime:peer_linux"
    "blockd-runtime:migrate_e2e_linux"
    "blockd-runtime:part_fetch_linux"
    "blockd-runtime:loop_interference_linux"
)

for spec in "${tests[@]}"; do
    IFS=: read -r package target <<<"$spec"
    echo "running $package/$target"
    cargo test -p "$package" --test "$target" -- --test-threads=1
done
