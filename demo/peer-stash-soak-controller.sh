#!/usr/bin/env bash
set -euo pipefail

control_host="${BLOCKD_SOAK_CONTROL_HOST:-127.0.0.1}"
control_port="${BLOCKD_SOAK_CONTROL_PORT:-20101}"
duration="${BLOCKD_SOAK_SECONDS:-3600}"
value="${BLOCKD_SOAK_START_VALUE:-3}"
started=$SECONDS
writes=0

command() {
  printf '%s\n' "$1" | nc -w 35 "$control_host" "$control_port"
}

check_metrics() {
  local metrics
  metrics="$(command METRICS)"
  case "$metrics" in
    *"nonactive=0"*"cleanup=0"*"incidents=0"*) ;;
    *) printf 'invariant failure: %s\n' "$metrics" >&2; exit 1 ;;
  esac
  printf 'progress writes=%s value=%s %s\n' "$writes" "$value" "$metrics"
}

while (( SECONDS - started < duration )); do
  command "WRITE $value" >/dev/null
  writes=$((writes + 1))
  if (( writes % 100 == 0 )); then
    check_metrics
  fi
  value=$((value + 1))
done

check_metrics
readback="$(command READ)"
expected=$((value - 1))
[[ "$readback" == "VALUE $expected" ]] || {
  printf 'readback mismatch: expected VALUE %s, got %s\n' "$expected" "$readback" >&2
  exit 1
}
printf '{"duration_seconds":%s,"writes":%s,"last_value":%s,"status":"complete"}\n' \
  "$((SECONDS - started))" "$writes" "$expected"
