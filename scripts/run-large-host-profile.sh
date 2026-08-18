#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 5 ]]; then
    echo "usage: $0 ARTIFACT_DIR VSET_COUNT PROVENANCE [DURATION_SECS] [runtime|firecracker]" >&2
    echo "provenance: independent | star | balanced:N | chain:N | mixed:SEED:ROOT_PPM:MAX_DEPTH" >&2
    exit 2
fi

artifact_dir=$1
vset_count=$2
provenance=$3
duration_secs=${4:-900}
backend=${5:-runtime}
timeout_secs=${BLOCKD_PROFILE_TIMEOUT_SECS:-}

if ! [[ $duration_secs =~ ^[1-9][0-9]*$ ]]; then
    echo "duration must be a positive integer" >&2
    exit 2
fi
if [[ -z $timeout_secs ]]; then
    timeout_secs=$((10#$duration_secs + 300))
fi

if [[ $(uname -s) != Linux ]]; then
    echo "large-host profiles require Linux" >&2
    exit 1
fi
if [[ -e $artifact_dir ]]; then
    echo "artifact directory already exists: $artifact_dir" >&2
    exit 1
fi
artifact_parent=$(dirname "$artifact_dir")
if [[ ! -d $artifact_parent ]]; then
    echo "artifact parent does not exist: $artifact_parent" >&2
    exit 1
fi
artifact_name=$(basename "$artifact_dir")
artifact_parent=$(cd "$artifact_parent" && pwd -P)
artifact_dir=$artifact_parent/$artifact_name
for command in awk cargo cp findmnt git lscpu perf ps rustc stat timeout; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "missing required command: $command" >&2
        exit 1
    fi
done
if ! [[ $timeout_secs =~ ^[1-9][0-9]*$ ]]; then
    echo "BLOCKD_PROFILE_TIMEOUT_SECS must be positive" >&2
    exit 2
fi
if [[ ! -d /var/tmp/blockd-scratch ]]; then
    echo "missing dedicated scratch directory: /var/tmp/blockd-scratch" >&2
    exit 1
fi
scratch_fs=$(stat -f -c %T /var/tmp/blockd-scratch)
if [[ $scratch_fs != xfs ]]; then
    echo "profile scratch must be XFS, found $scratch_fs" >&2
    exit 1
fi
if [[ $(awk 'END { print NR }' /proc/swaps) -ne 1 ]]; then
    echo "swap must be disabled for retained profile runs" >&2
    exit 1
fi
case $backend in
    runtime)
        profile_test=large_host_profile_linux
        profile_name=profile_vset_scale_and_fork_provenance
        ;;
    firecracker)
        profile_test=large_host_fc_profile_linux
        profile_name=profile_firecracker_scale_and_fork_provenance
        if [[ ! -r /dev/kvm || ! -w /dev/kvm ]]; then
            echo "Firecracker profiles require read/write access to /dev/kvm" >&2
            exit 1
        fi
        fc_dir=${BLOCKD_FC_DIR:-/var/tmp/blockd-fc}
        for artifact in firecracker vmlinux initramfs.cpio; do
            if [[ ! -f $fc_dir/$artifact ]]; then
                echo "missing Firecracker artifact: $fc_dir/$artifact" >&2
                exit 1
            fi
        done
        ;;
    *)
        echo "unknown profile backend: $backend" >&2
        exit 2
        ;;
esac

mkdir "$artifact_dir"
mkdir "$artifact_dir/machine"
mkdir "$artifact_dir/system"

git rev-parse HEAD >"$artifact_dir/machine/revision.txt"
git status --porcelain=v1 >"$artifact_dir/machine/worktree.txt"
uname -a >"$artifact_dir/machine/uname.txt"
lscpu --json >"$artifact_dir/machine/lscpu.json"
rustc -Vv >"$artifact_dir/machine/rustc.txt"
cargo -V >"$artifact_dir/machine/cargo.txt"
cp /proc/cmdline "$artifact_dir/machine/cmdline.txt"
cp /proc/swaps "$artifact_dir/machine/swaps.txt"
findmnt --json /var/tmp/blockd-scratch >"$artifact_dir/machine/scratch-mount.json"

revision=$(git rev-parse HEAD)
runtime_artifacts=$artifact_dir/runtime
perf_events=${BLOCKD_PROFILE_PERF_EVENTS:-task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses}
printf '%s\n' "$perf_events" >"$artifact_dir/machine/perf-events.txt"
profile_command=(
    cargo test --release -p blockd-runtime --test "$profile_test"
    "$profile_name" -- --ignored --exact --nocapture
)
if [[ -n ${BLOCKD_PROFILE_CPU_LIST:-} ]]; then
    if ! command -v taskset >/dev/null 2>&1; then
        echo "BLOCKD_PROFILE_CPU_LIST requires taskset" >&2
        exit 1
    fi
    printf '%s\n' "$BLOCKD_PROFILE_CPU_LIST" >"$artifact_dir/machine/cpu-list.txt"
    profile_command=(taskset --cpu-list "$BLOCKD_PROFILE_CPU_LIST" "${profile_command[@]}")
fi

cargo test --release -p blockd-runtime --test "$profile_test" --no-run
if [[ ${BLOCKD_PROFILE_STACKS:-0} == 1 ]]; then
    profile_command=(
        perf record -e cpu-clock -F 99 -g --call-graph "dwarf,16384"
        -o "$artifact_dir/system/perf.data" -- "${profile_command[@]}"
    )
fi
profile_command=(
    timeout --signal=TERM --kill-after=10 "$timeout_secs"
    "${profile_command[@]}"
)

profile_pid=
sampler_pid=
cleanup_background() {
    if [[ -n $sampler_pid ]]; then
        kill "$sampler_pid" 2>/dev/null || true
    fi
    if [[ -n $profile_pid ]]; then
        kill "$profile_pid" 2>/dev/null || true
    fi
}
trap cleanup_background EXIT INT TERM

set +e
BLOCKD_PROFILE_ARTIFACT_DIR=$runtime_artifacts \
BLOCKD_PROFILE_VSET_COUNT=$vset_count \
BLOCKD_PROFILE_PROVENANCE=$provenance \
BLOCKD_PROFILE_DURATION_SECS=$duration_secs \
BLOCKD_PROFILE_REVISION=$revision \
perf stat -x, -e "$perf_events" -o "$artifact_dir/system/perf-stat.csv" -- \
    "${profile_command[@]}" \
    >"$artifact_dir/profile.stdout" 2>"$artifact_dir/profile.stderr" &
profile_pid=$!

(
    while kill -0 "$profile_pid" 2>/dev/null; do
        date --iso-8601=ns
        ps -eLo pid,tid,psr,pcpu,stat,comm
        cat /proc/pressure/cpu
        cat /proc/pressure/io
        cat /proc/pressure/memory
        cat /proc/diskstats
        cat /sys/devices/system/node/node*/numastat 2>/dev/null || true
        cat /proc/meminfo
        sleep 1
    done
) >"$artifact_dir/system/samples.txt" &
sampler_pid=$!

wait "$profile_pid"
profile_status=$?
wait "$sampler_pid"
set -e
trap - EXIT INT TERM

if [[ $profile_status -ne 0 ]]; then
    echo "profile failed with status $profile_status; artifacts retained in $artifact_dir" >&2
    exit "$profile_status"
fi
if [[ -f $artifact_dir/system/perf.data ]]; then
    perf report --stdio --no-children --no-inline --no-source \
        --fields overhead,symbol --percent-limit 0.1 \
        -i "$artifact_dir/system/perf.data" \
        >"$artifact_dir/system/perf-report.txt"
fi

echo "profile artifacts: $artifact_dir"
