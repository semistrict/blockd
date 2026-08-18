#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "usage: $0 ARTIFACT_ROOT VOLUME_COUNT PROVENANCE [DURATION_SECS]" >&2
    exit 2
fi

artifact_root=$1
volume_count=$2
provenance=$3
duration_secs=${4:-300}
repetitions=${BLOCKD_PROFILE_INSTRUMENTATION_REPETITIONS:-10}
first=${BLOCKD_PROFILE_DETAILED_FIRST:-0}

if [[ -e $artifact_root ]]; then
    echo "artifact root already exists: $artifact_root" >&2
    exit 1
fi
artifact_parent=$(dirname "$artifact_root")
if [[ ! -d $artifact_parent ]]; then
    echo "artifact parent does not exist: $artifact_parent" >&2
    exit 1
fi
if ! [[ $repetitions =~ ^[1-9][0-9]*$ ]] || ((repetitions < 10 || repetitions % 2 != 0)); then
    echo "BLOCKD_PROFILE_INSTRUMENTATION_REPETITIONS must be an even integer of at least 10" >&2
    exit 2
fi
if [[ $first != 0 && $first != 1 ]]; then
    echo "BLOCKD_PROFILE_DETAILED_FIRST must be 0 or 1" >&2
    exit 2
fi
for command in awk jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "missing required command: $command" >&2
        exit 1
    fi
done

mkdir "$artifact_root"
printf 'volume_count\t%s\nprovenance\t%s\nduration_secs\t%s\nrepetitions\t%s\ndetailed_first\t%s\n' \
    "$volume_count" "$provenance" "$duration_secs" "$repetitions" "$first" \
    >"$artifact_root/config.tsv"
printf 'order\tpair\tdetailed_profile_metrics\tartifact\n' >"$artifact_root/index.tsv"

provenance_tag=${provenance//:/-}
for ((order = 1; order <= repetitions; order++)); do
    detailed=$(((order - 1 + first) % 2))
    pair=$(((order + 1) / 2))
    run_dir=$artifact_root/v${volume_count}-${provenance_tag}-detail${detailed}-n${order}
    printf '%s\t%s\t%s\t%s\n' "$order" "$pair" "$detailed" "$run_dir" \
        >>"$artifact_root/index.tsv"
    BLOCKD_PROFILE_DETAILED_METRICS=$detailed \
    BLOCKD_PROFILE_STACKS=0 \
        scripts/run-large-host-profile.sh \
        "$run_dir" "$volume_count" "$provenance" "$duration_secs" runtime
done

results=$artifact_root/results.tsv
printf 'order\tpair\tdetailed_profile_metrics\toperations\toperations_per_second\terrors\toperation_p50_upper_ns\toperation_p99_upper_ns\toperation_max_ns\ttask_clock_ms\tcontext_switches\tcycles\tinstructions\tcycles_per_operation\tinstructions_per_operation\tartifact\n' \
    >"$results"

perf_value() {
    awk -F, -v event="$2" '$3 == event { print $1; exit }' "$1"
}

tail -n +2 "$artifact_root/index.tsv" | while IFS=$'\t' read -r order pair detailed run_dir; do
    run_summary=$run_dir/runtime/summary.json
    perf_stat=$run_dir/system/perf-stat.csv
    if [[ ! -f $run_summary || ! -f $perf_stat ]]; then
        echo "missing completed artifacts for run: $run_dir" >&2
        exit 1
    fi
    operations=$(jq -r '.operations' "$run_summary")
    operations_per_second=$(jq -r '.operations_per_second' "$run_summary")
    errors=$(jq -r '.errors' "$run_summary")
    operation_p50_upper_ns=$(jq -r '.operation_latency.p50_upper_ns' "$run_summary")
    operation_p99_upper_ns=$(jq -r '.operation_latency.p99_upper_ns' "$run_summary")
    operation_max_ns=$(jq -r '.operation_latency.max_ns' "$run_summary")
    task_clock_ms=$(perf_value "$perf_stat" task-clock)
    context_switches=$(perf_value "$perf_stat" context-switches)
    cycles=$(perf_value "$perf_stat" cycles)
    instructions=$(perf_value "$perf_stat" instructions)
    cycles_per_operation=$(awk -v total="$cycles" -v operations="$operations" \
        'BEGIN { if (operations > 0 && total ~ /^[0-9]+([.][0-9]+)?$/) print total / operations; else print "NA" }')
    instructions_per_operation=$(awk -v total="$instructions" -v operations="$operations" \
        'BEGIN { if (operations > 0 && total ~ /^[0-9]+([.][0-9]+)?$/) print total / operations; else print "NA" }')
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$order" "$pair" "$detailed" "$operations" "$operations_per_second" "$errors" \
        "$operation_p50_upper_ns" "$operation_p99_upper_ns" "$operation_max_ns" \
        "$task_clock_ms" "$context_switches" "$cycles" "$instructions" \
        "$cycles_per_operation" "$instructions_per_operation" "$run_dir" >>"$results"
done

effect=$artifact_root/effect.tsv
awk -F'\t' '
    NR == 1 { next }
    {
        mode = $3
        count[mode]++
        ops[mode] += $5
        if ($14 != "NA") {
            cycles_per_op[mode] += $14
            cycles_count[mode]++
        }
        if ($15 != "NA") {
            instructions_per_op[mode] += $15
            instructions_count[mode]++
        }
    }
    END {
        print "metric\tdetailed_off_mean\tdetailed_on_mean\tpercent_change"
        off = ops[0] / count[0]
        on = ops[1] / count[1]
        print "operations_per_second\t" off "\t" on "\t" 100 * (on / off - 1)
        if (cycles_count[0] > 0 && cycles_count[1] > 0) {
            off = cycles_per_op[0] / cycles_count[0]
            on = cycles_per_op[1] / cycles_count[1]
            print "cycles_per_operation\t" off "\t" on "\t" 100 * (on / off - 1)
        } else {
            print "cycles_per_operation\tNA\tNA\tNA"
        }
        if (instructions_count[0] > 0 && instructions_count[1] > 0) {
            off = instructions_per_op[0] / instructions_count[0]
            on = instructions_per_op[1] / instructions_count[1]
            print "instructions_per_operation\t" off "\t" on "\t" 100 * (on / off - 1)
        } else {
            print "instructions_per_operation\tNA\tNA\tNA"
        }
    }
' "$results" >"$effect"

echo "instrumentation observations: $results"
echo "instrumentation mean effects: $effect"
