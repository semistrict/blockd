#!/bin/sh
# Run one independently replayable shard of the deterministic simulation.
set -eu

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <scenario> <start-seed> <count> <artifact-dir>" >&2
    exit 2
fi

scenario=$1
start=$2
count=$3
artifact_dir=$4

case "$scenario" in
    chaos|cluster|migration|peer-stash|peer-rare|explore|\
    cold-restore-outage|nvme-pressure-backed|nvme-pressure-unbacked|\
    migration-release-blackout|migration-leaf-blackout|hot-compaction|\
    resume-set-rot|leaf-rot|peer-commit-crashes|peer-transfer-crashes|\
    peer-transition-before-cas|peer-transition-after-seed|\
    peer-transition-after-active) ;;
    *)
        echo "unknown scenario: $scenario" >&2
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
    echo "scenario: $scenario"
    echo "start seed: $start"
    echo "count: $count"
    echo "revision: $(git rev-parse HEAD)"
    echo "replay command: scripts/run-sim-ensemble.sh $scenario $start $count $artifact_dir"
} >"$artifact_dir/ensemble.txt"

cargo build --release -p blockd-sim --bin sweep --features test-page-size

set +e
BLOCKD_SWEEP_ARTIFACT_DIR=$artifact_dir \
BLOCKD_SWEEP_REQUIRE_COVERAGE=1 \
    target/release/sweep "$scenario" "$start" "$count" \
    >"$artifact_dir/sweep.log" 2>&1
status=$?
set -e

cat "$artifact_dir/sweep.log"
exit "$status"
