#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 BASE_RUN_ARTIFACT COMPARISON_RUN_ARTIFACT" >&2
    exit 2
fi

base_run=$1
comparison_run=$2
base_report=$base_run/system/perf-report.txt
comparison_report=$comparison_run/system/perf-report.txt
base_manifest=$base_run/runtime/manifest.json
comparison_manifest=$comparison_run/runtime/manifest.json
for path in "$base_report" "$comparison_report" "$base_manifest" "$comparison_manifest"; do
    if [[ ! -f $path ]]; then
        echo "missing comparison input: $path" >&2
        exit 1
    fi
done
for command in awk jq mktemp sort; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "missing required command: $command" >&2
        exit 1
    fi
done

echo "base: $(jq -c '{profile,volume_count,topology:.fork_provenance.topology,roots:.fork_provenance.roots,max_generation:.fork_provenance.max_generation,cpu_list,available_parallelism}' "$base_manifest")"
echo "comparison: $(jq -c '{profile,volume_count,topology:.fork_provenance.topology,roots:.fork_provenance.roots,max_generation:.fork_provenance.max_generation,cpu_list,available_parallelism}' "$comparison_manifest")"

rows=$(mktemp)
trap 'rm -f "$rows"' EXIT
awk '
    function consume(line, values, overhead, symbol) {
        sub(/^[[:space:]]*/, "", line)
        split(line, values, /[[:space:]]+/)
        overhead = values[1]
        if (overhead !~ /^[0-9]+([.][0-9]+)?%$/) {
            return
        }
        sub(/%$/, "", overhead)
        sub(/^[0-9]+([.][0-9]+)?%[[:space:]]+/, "", line)
        symbol = line
        if (FILENAME == ARGV[1]) {
            base[symbol] += overhead
        } else {
            comparison[symbol] += overhead
        }
        seen[symbol] = 1
    }
    { consume($0) }
    END {
        for (symbol in seen) {
            delta = comparison[symbol] - base[symbol]
            absolute = delta < 0 ? -delta : delta
            printf "%s\t%.3f\t%.3f\t%+.3f\t%.3f\n", symbol, base[symbol], comparison[symbol], delta, absolute
        }
    }
' "$base_report" "$comparison_report" >"$rows"

printf 'symbol\tbase_overhead_pct\tcomparison_overhead_pct\tdelta_percentage_points\tabs_delta_percentage_points\n'
sort -t $'\t' -k5,5nr "$rows"
