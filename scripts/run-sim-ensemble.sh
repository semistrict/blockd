#!/bin/sh
# Run one independently replayable shard of the deterministic simulation.
set -eu

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <chaos|cluster|migration> <start-seed> <count> <artifact-dir>" >&2
    exit 2
fi

preset=$1
start=$2
count=$3
artifact_dir=$4

case "$preset" in
    chaos|cluster|migration) ;;
    *)
        echo "unknown preset: $preset" >&2
        exit 2
        ;;
esac

case "$start" in
    ''|*[!0-9]*)
        echo "start must be a non-negative integer" >&2
        exit 2
        ;;
esac

case "$count" in
    ''|*[!0-9]*|0)
        echo "count must be a positive integer" >&2
        exit 2
        ;;
esac

mkdir -p "$artifact_dir"
{
    echo "preset: $preset"
    echo "start seed: $start"
    echo "count: $count"
    echo "revision: $(git rev-parse HEAD)"
    echo "replay command: scripts/run-sim-ensemble.sh $preset $start $count $artifact_dir"
} >"$artifact_dir/ensemble.txt"

cargo build --release -p blockd-sim --bin sweep

set +e
BLOCKD_SWEEP_ARTIFACT_DIR=$artifact_dir \
BLOCKD_SWEEP_REQUIRE_COVERAGE=1 \
    target/release/sweep "$preset" "$start" "$count" \
    >"$artifact_dir/sweep.log" 2>&1
status=$?
set -e

cat "$artifact_dir/sweep.log"
exit "$status"
