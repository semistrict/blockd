#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 ARTIFACT_ROOT" >&2
    exit 2
fi

artifact_root=$1
if [[ ! -d $artifact_root ]]; then
    echo "missing artifact root: $artifact_root" >&2
    exit 1
fi
for command in awk find head jq mktemp sort tail; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "missing required command: $command" >&2
        exit 1
    fi
done

rows=$(mktemp)
trap 'rm -f "$rows"' EXIT
signature=
while IFS= read -r summary; do
    runtime_dir=$(dirname "$summary")
    manifest=$runtime_dir/manifest.json
    if [[ ! -f $manifest ]]; then
        echo "missing manifest beside summary: $summary" >&2
        exit 1
    fi
    if ! jq -e '
        (.available_parallelism | type == "number" and . > 0 and floor == .)
        and (.operations_per_second | type == "number" and . >= 0)
        and (.process_cpu.average_cores | type == "number" and . >= 0)
        and (.operation_latency.p50_upper_ns | type == "number" and . >= 0)
        and (.operation_latency.p99_upper_ns | type == "number" and . >= 0)
        and (.errors | type == "number" and . >= 0 and floor == .)
    ' "$summary" >/dev/null; then
        echo "invalid or missing numeric metric in summary: $summary" >&2
        exit 1
    fi
    current_signature=$(jq -c '{
        volume_count,
        active_volume_count,
        topology: .fork_provenance.topology,
        roots: .fork_provenance.roots,
        runtime_shards,
        prefault_hotset,
        refault_each_access,
        pages_per_volume,
        hot_pages,
        cache_pages_per_volume,
        write_ppm,
        duration_secs,
        latency_sample_rate
    }' "$manifest")
    if [[ -z $signature ]]; then
        signature=$current_signature
    elif [[ $current_signature != "$signature" ]]; then
        echo "incomparable workload manifest: $manifest" >&2
        echo "expected: $signature" >&2
        echo "found:    $current_signature" >&2
        exit 1
    fi
    jq -r --slurpfile manifest "$manifest" --arg artifact "$(dirname "$runtime_dir")" '[
        ($manifest[0].available_parallelism | tostring),
        (.operations_per_second | tostring),
        (.process_cpu.average_cores | tostring),
        (.operation_latency.p50_upper_ns | tostring),
        (.operation_latency.p99_upper_ns | tostring),
        (.errors | tostring),
        $artifact
    ] | @tsv' "$summary" >>"$rows"
done < <(find "$artifact_root" -type f -path '*/runtime/summary.json' | sort)

if [[ ! -s $rows ]]; then
    echo "no completed runtime summaries under $artifact_root" >&2
    exit 1
fi

if [[ $(jq -r '.topology' <<<"$signature") != independent ]]; then
    echo "scaling gate requires independent provenance" >&2
    exit 1
fi
if [[ $(jq -r '.runtime_shards' <<<"$signature") != 1 ]]; then
    echo "scaling gate requires the production-default single runtime lane" >&2
    exit 1
fi
if [[ $(jq -r '.prefault_hotset' <<<"$signature") != true ]] ||
    [[ $(jq -r '.refault_each_access' <<<"$signature") != false ]]; then
    echo "scaling gate requires a prefaulted resident hot set without forced refaults" >&2
    exit 1
fi

median() {
    sort -n | awk '
        { values[NR] = $1 }
        END {
            if (NR == 0) exit 1
            if (NR % 2) print values[(NR + 1) / 2]
            else print (values[NR / 2] + values[NR / 2 + 1]) / 2
        }
    '
}

for cores in 1 2 4 8; do
    repetitions=$(awk -F '\t' -v cores="$cores" '$1 == cores { count++ } END { print count + 0 }' "$rows")
    if (( repetitions < 3 )); then
        echo "need at least three completed ${cores}-core repetitions, found $repetitions" >&2
        exit 1
    fi
    errors=$(awk -F '\t' -v cores="$cores" '$1 == cores { errors += $6 } END { print errors + 0 }' "$rows")
    if (( errors != 0 )); then
        echo "${cores}-core repetitions reported $errors data errors" >&2
        exit 1
    fi
done

base=$(awk -F '\t' '$1 == 1 { print $2 }' "$rows" | median)
base_p99=$(awk -F '\t' '$1 == 1 { print $5 }' "$rows" | median)
failed=0
printf 'cores\trepetitions\tmin_ops_s\tmedian_ops_s\tmax_ops_s\tspeedup\tefficiency_pct\tmedian_cpu_cores\tmedian_p50_ns\tmedian_p99_ns\n'
for cores in 1 2 4 8; do
    repetitions=$(awk -F '\t' -v cores="$cores" '$1 == cores { count++ } END { print count + 0 }' "$rows")
    minimum=$(awk -F '\t' -v cores="$cores" '$1 == cores { print $2 }' "$rows" | sort -n | head -1)
    throughput=$(awk -F '\t' -v cores="$cores" '$1 == cores { print $2 }' "$rows" | median)
    maximum=$(awk -F '\t' -v cores="$cores" '$1 == cores { print $2 }' "$rows" | sort -n | tail -1)
    cpu=$(awk -F '\t' -v cores="$cores" '$1 == cores { print $3 }' "$rows" | median)
    p50=$(awk -F '\t' -v cores="$cores" '$1 == cores { print $4 }' "$rows" | median)
    p99=$(awk -F '\t' -v cores="$cores" '$1 == cores { print $5 }' "$rows" | median)
    speedup=$(awk -v throughput="$throughput" -v base="$base" 'BEGIN { printf "%.3f", throughput / base }')
    efficiency=$(awk -v speedup="$speedup" -v cores="$cores" 'BEGIN { printf "%.1f", 100 * speedup / cores }')
    printf '%s\t%s\t%.0f\t%.0f\t%.0f\t%s\t%s\t%.3f\t%.0f\t%.0f\n' \
        "$cores" "$repetitions" "$minimum" "$throughput" "$maximum" "$speedup" "$efficiency" "$cpu" "$p50" "$p99"
    if (( cores > 1 )) && ! awk -v efficiency="$efficiency" 'BEGIN { exit !(efficiency >= 80) }'; then
        failed=1
    fi
    if ! awk -v p99="$p99" -v base="$base_p99" 'BEGIN { exit !(p99 <= base * 1.20) }'; then
        failed=1
    fi
done

if (( failed != 0 )); then
    echo "independent-volume scaling gate failed" >&2
    exit 1
fi
echo "independent-volume scaling gate passed"
