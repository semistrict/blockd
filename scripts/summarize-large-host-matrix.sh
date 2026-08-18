#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 MATRIX_ARTIFACT_ROOT" >&2
    exit 2
fi

artifact_root=$1
index=$artifact_root/index.tsv
if [[ ! -f $index ]]; then
    echo "missing matrix index: $index" >&2
    exit 1
fi
for command in awk jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "missing required command: $command" >&2
        exit 1
    fi
done

summary=$artifact_root/summary.tsv
hotspots=$artifact_root/hotspots.tsv
printf 'backend\tvset_count\tprovenance\tcpu_list\trepetition\tstacks\troot_count\tmax_generation\toperations_per_second\terrors\toperation_p50_upper_ns\toperation_p90_upper_ns\toperation_p99_upper_ns\toperation_p999_upper_ns\toperation_max_ns\tartifact\n' >"$summary"
printf 'backend\tvset_count\tprovenance\tcpu_list\trepetition\toverhead\tsymbol\tartifact\n' >"$hotspots"

tail -n +2 "$index" | while IFS=$'\t' read -r backend vset_count provenance cpu_list repetition stacks run_dir; do
    run_summary=$run_dir/runtime/summary.json
    if [[ ! -f $run_summary ]]; then
        echo "missing completed run summary: $run_summary" >&2
        exit 1
    fi
    jq -r \
        --arg backend "$backend" \
        --arg vset_count "$vset_count" \
        --arg provenance "$provenance" \
        --arg cpu_list "$cpu_list" \
        --arg repetition "$repetition" \
        --arg stacks "$stacks" \
        --arg artifact "$run_dir" \
        '[
            $backend,
            $vset_count,
            $provenance,
            $cpu_list,
            $repetition,
            $stacks,
            (.root_count | tostring),
            (.max_generation | tostring),
            (.operations_per_second | tostring),
            (.errors | tostring),
            (.operation_latency.p50_upper_ns | tostring),
            (.operation_latency.p90_upper_ns | tostring),
            (.operation_latency.p99_upper_ns | tostring),
            (.operation_latency.p999_upper_ns | tostring),
            (.operation_latency.max_ns | tostring),
            $artifact
        ] | @tsv' "$run_summary" >>"$summary"

    perf_report=$run_dir/system/perf-report.txt
    if [[ -f $perf_report ]]; then
        awk \
            -v backend="$backend" \
            -v vsets="$vset_count" \
            -v provenance="$provenance" \
            -v cpus="$cpu_list" \
            -v repetition="$repetition" \
            -v artifact="$run_dir" '
            /^[[:space:]]*[0-9]+([.][0-9]+)?%/ {
                overhead=$1
                sub(/^[[:space:]]*[^[:space:]]+[[:space:]]+/, "", $0)
                print backend "\t" vsets "\t" provenance "\t" cpus "\t" repetition "\t" overhead "\t" $0 "\t" artifact
            }
        ' "$perf_report" >>"$hotspots"
    fi
done

echo "matrix summary: $summary"
echo "sampled hotspots: $hotspots"
