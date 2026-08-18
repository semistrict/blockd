#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 ARTIFACT_ROOT runtime|firecracker" >&2
    echo "set BLOCKD_PROFILE_COUNTS and BLOCKD_PROFILE_PROVENANCES to comma-separated matrices" >&2
    exit 2
fi

artifact_root=$1
backend=$2
counts=${BLOCKD_PROFILE_COUNTS:?set BLOCKD_PROFILE_COUNTS, for example 64,256,1024}
provenances=${BLOCKD_PROFILE_PROVENANCES:?set BLOCKD_PROFILE_PROVENANCES, for example independent,star,balanced:4,chain:8,mixed:17:100000:8}
repetitions=${BLOCKD_PROFILE_REPETITIONS:-3}
duration_secs=${BLOCKD_PROFILE_DURATION_SECS:-900}
cpu_lists=${BLOCKD_PROFILE_CPU_LISTS:-all}

if [[ $backend != runtime && $backend != firecracker ]]; then
    echo "backend must be runtime or firecracker" >&2
    exit 2
fi
if [[ -e $artifact_root ]]; then
    echo "artifact root already exists: $artifact_root" >&2
    exit 1
fi
artifact_parent=$(dirname "$artifact_root")
if [[ ! -d $artifact_parent ]]; then
    echo "artifact parent does not exist: $artifact_parent" >&2
    exit 1
fi
if ! [[ $repetitions =~ ^[1-9][0-9]*$ ]]; then
    echo "BLOCKD_PROFILE_REPETITIONS must be positive" >&2
    exit 2
fi

mkdir "$artifact_root"
printf 'backend\t%s\ncounts\t%s\nprovenances\t%s\nrepetitions\t%s\nduration_secs\t%s\ncpu_lists\t%s\n' \
    "$backend" "$counts" "$provenances" "$repetitions" "$duration_secs" "$cpu_lists" \
    >"$artifact_root/matrix.tsv"
printf 'backend\tvset_count\tprovenance\tcpu_list\trepetition\tstacks\tartifact\n' \
    >"$artifact_root/index.tsv"

IFS=',' read -r -a count_values <<<"$counts"
IFS=',' read -r -a provenance_values <<<"$provenances"
IFS=';' read -r -a cpu_values <<<"$cpu_lists"

for vset_count in "${count_values[@]}"; do
    for provenance in "${provenance_values[@]}"; do
        provenance_tag=${provenance//:/-}
        for cpu_list in "${cpu_values[@]}"; do
            cpu_tag=${cpu_list//,/-}
            for ((repetition = 1; repetition <= repetitions; repetition++)); do
                run_dir=$artifact_root/v${vset_count}-${provenance_tag}-cpu${cpu_tag}-r${repetition}
                stacks=0
                if [[ ${BLOCKD_PROFILE_STACKS_FIRST:-1} == 1 && $repetition -eq 1 ]]; then
                    stacks=1
                fi
                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    "$backend" "$vset_count" "$provenance" "$cpu_list" "$repetition" "$stacks" "$run_dir" \
                    >>"$artifact_root/index.tsv"
                if [[ $cpu_list == all ]]; then
                    env -u BLOCKD_PROFILE_CPU_LIST \
                        BLOCKD_PROFILE_STACKS=$stacks \
                        scripts/run-large-host-profile.sh \
                        "$run_dir" "$vset_count" "$provenance" "$duration_secs" "$backend"
                else
                    BLOCKD_PROFILE_CPU_LIST=$cpu_list \
                    BLOCKD_PROFILE_STACKS=$stacks \
                        scripts/run-large-host-profile.sh \
                        "$run_dir" "$vset_count" "$provenance" "$duration_secs" "$backend"
                fi
            done
        done
    done
done
