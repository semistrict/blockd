#!/usr/bin/env bash
# The blockd demo on GCP: same story as smoke-lima.sh, against the two
# provisioned hosts and the real GCS bucket. Requires: `tofu apply` done
# in infra/, gcloud authenticated for the project.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ZONE=$(tofu -chdir="$REPO/infra" output -raw zone)
BUCKET=$(tofu -chdir="$REPO/infra" output -raw bucket)
HOST0=$(tofu -chdir="$REPO/infra" output -json hosts | sed -n 's/.*"0":"\([^"]*\)".*/\1/p')
HOST1=$(tofu -chdir="$REPO/infra" output -json hosts | sed -n 's/.*"1":"\([^"]*\)".*/\1/p')
API0=http://127.0.0.1:7100
API1=http://127.0.0.1:7101

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
show() { printf '   %s\n' "$*"; }
post() { curl -sf -X POST "$1"; }
get()  { curl -sf "$1"; }
field() { sed -n "s/.*\"$2\":\"\{0,1\}\([^,\"}]*\)\"\{0,1\}.*/\1/p" <<<"$1"; }
ssh0() { gcloud compute ssh "$HOST0" --zone "$ZONE" --tunnel-through-iap --command "$1"; }
ssh1() { gcloud compute ssh "$HOST1" --zone "$ZONE" --tunnel-through-iap --command "$1"; }

cleanup() { kill "${TUNNEL_PID:-0}" 2>/dev/null || true; }
trap cleanup EXIT

say "wait for both hosts to finish provisioning (first boot builds everything: ~15 min)"
for host in "$HOST0" "$HOST1"; do
  for _ in $(seq 1 240); do
    if gcloud compute ssh "$host" --zone "$ZONE" --tunnel-through-iap \
        --command 'test -f /var/opt/blockd/.ready' 2>/dev/null; then
      show "$host ready"; break
    fi
    sleep 15
  done
done

say "verify both nodes published startup-generated TLS membership records"
CERT_PREFIX="gs://$BUCKET/blockd/cluster/tls/public-keys"
for _ in $(seq 1 30); do
  CERTS=$(gcloud storage ls "$CERT_PREFIX/*.member" 2>/dev/null || true)
  [ "$(printf '%s\n' "$CERTS" | sed '/^$/d' | wc -l | tr -d ' ')" -eq 2 ] && break
  sleep 1
done
[ "$(printf '%s\n' "$CERTS" | sed '/^$/d' | wc -l | tr -d ' ')" -eq 2 ] || {
  echo "NODES DID NOT PUBLISH TWO TLS MEMBERSHIP RECORDS"
  exit 1
}
show "startup-generated membership records:"
printf '%s\n' "$CERTS" | sed 's/^/   /'

say "open one SSH tunnel to both demo APIs (they are VPC-internal only)"
gcloud compute ssh "$HOST0" --zone "$ZONE" --tunnel-through-iap -- -N \
  -L 7100:10.10.0.10:7000 -L 7101:10.10.0.11:7000 &
TUNNEL_PID=$!
sleep 5

say "bake the base image into GCS (boot, work, snapshot, publish)"
BAKE=$(post "$API0/base")
show "base checksum: $(field "$BAKE" sum)"
show "objects now in the bucket:"
gcloud storage ls "gs://$BUCKET/blockd/base/**" | sed 's/^/   /'

say "start VM on host 0, restored from the GCS-held base"
R=$(post "$API0/vm"); VM=$(field "$R" id)
show "vm $VM running"

say "guest work, mirrored into its blockd volume (3 synced bursts)"
R=$(post "$API0/vm/$VM/work?bursts=3")
show "burst $(field "$R" burst), guest sum $(field "$R" guest_sum)"
R=$(post "$API0/vm/$VM/verify")
show "volume verifies: ok=$(field "$R" ok)"

say "fork: snapshot the live VM, start 3 forks sharing ONE memory copy"
R=$(post "$API0/vm/$VM/fork?n=3")
show "$R"
[ "$(field "$R" pss_sum)" -lt "$(field "$R" rss_sum)" ] || { echo "FORKS NOT SHARING"; exit 1; }

say "live-migrate VM $VM to host 1: volume over TCP, microVM via GCS"
post "$API1/vm/$VM/expect" >/dev/null
R=$(post "$API0/vm/$VM/migrate?to=1")
show "source: snapshot+publish $(field "$R" snapshot_ms)ms, volume handoff $(field "$R" handoff_ms)ms"
for _ in $(seq 1 150); do
  S=$(get "$API1/status")
  grep -q "\"id\":$VM,.*\"state\":\"running\"" <<<"$S" && break
  sleep 0.5
done
grep -q "\"id\":$VM,.*\"state\":\"running\"" <<<"$S" || { echo "MIGRATION NEVER LANDED"; exit 1; }
R=$(post "$API1/vm/$VM/verify")
show "volume verifies ON HOST 1: ok=$(field "$R" ok) at burst $(field "$R" burst)"
post "$API1/vm/$VM/work?bursts=1" >/dev/null
show "and keeps working there"

say "backed VM: durable state survives host death via GCS alone"
R=$(post "$API0/vm?backed=1"); BVM=$(field "$R" id)
post "$API0/vm/$BVM/work?bursts=2" >/dev/null
sleep 2  # let backup publish
show "killing host 0's daemon (SIGKILL, whole control group)"
ssh0 'sudo systemctl kill --signal=KILL blockd-demod' || true
R=$(post "$API1/vm/$BVM/restore")
show "host 1 restored vm $BVM from the bucket: verdict $(field "$R" verdict)"
R=$(post "$API1/vm/$BVM/verify")
show "volume verifies after restore: ok=$(field "$R" ok) at burst $(field "$R" burst)"
[ "$(field "$R" ok)" = "true" ] || { echo "RESTORE VERIFY FAILED"; exit 1; }

say "the bill (host 1's view)"
get "$API1/status"

say "PASS"
