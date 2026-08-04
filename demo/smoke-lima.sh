#!/usr/bin/env bash
# The whole demo story on one machine (Lima/aarch64): a fake GCS store,
# two demod hosts, real Firecracker microVMs, live vset migration over
# TCP, forks sharing one memory copy, and a backed vset surviving host
# death via the store alone. GCP runs the same script shape against the
# real bucket (demo/run.sh).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${CARGO_TARGET_DIR:-$REPO/target}"
FC_DIR="${BLOCKD_FC_DIR:-/var/tmp/blockd-fc}"
WORK=/var/tmp/blockd-demo
API0=http://127.0.0.1:7100
API1=http://127.0.0.1:7101

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
show() { printf '   %s\n' "$*"; }

post() { curl -sf -X POST "$1"; }
get()  { curl -sf "$1"; }
# Extract "field":value / "field":"value" from one-line JSON.
field() { sed -n "s/.*\"$2\":\"\?\([^,\"}]*\)\"\?.*/\1/p" <<<"$1"; }

cleanup() {
  pkill -9 -f 'demod /var/tmp/blockd-demo' 2>/dev/null || true
  pkill -9 -f 'demod fake-gcs' 2>/dev/null || true
  pkill -9 -f "$FC_DIR/firecracker" 2>/dev/null || true
}
trap cleanup EXIT

say "build"
cargo build -p blockd-demod
DEMOD="$TARGET/debug/demod"

say "start: fake GCS + two demod hosts"
rm -rf "$WORK"; mkdir -p "$WORK"/h0 "$WORK"/h1
"$DEMOD" fake-gcs 127.0.0.1:7099 >"$WORK/fake-gcs.log" 2>&1 &
for h in 0 1; do
  cat > "$WORK/h$h.conf" <<EOF
host = $h
api = 127.0.0.1:710$h
peer_listen = 127.0.0.1:700$((h + 1))
peer.0 = 127.0.0.1:7001
peer.1 = 127.0.0.1:7002
gcs_endpoint = http://127.0.0.1:7099
gcs_metadata = http://127.0.0.1:7099
gcs_bucket = demo
gcs_prefix = blockd/
blob_dir = $WORK/h$h/blobs
scratch = $WORK/h$h/scratch
shmem_dir = /dev/shm
fc_dir = $FC_DIR
EOF
  "$DEMOD" "$WORK/h$h.conf" >"$WORK/h$h.log" 2>&1 &
done
sleep 1

say "bake the base image into the store (boot, work, snapshot, publish)"
BAKE=$(post "$API0/base")
show "base checksum: $(field "$BAKE" sum)"

say "start VM 1 on host 0 (restored from the store-held base)"
R=$(post "$API0/vm")
VM=$(field "$R" id)
show "vm $VM running"

say "guest work, mirrored into its blockd vset (3 bursts, each synced)"
R=$(post "$API0/vm/$VM/work?bursts=3")
show "burst $(field "$R" burst), guest sum $(field "$R" guest_sum)"
R=$(post "$API0/vm/$VM/verify")
show "vset verifies: ok=$(field "$R" ok) at burst $(field "$R" burst)"

say "fork: snapshot the live VM, start 3 forks sharing ONE memory copy"
R=$(post "$API0/vm/$VM/fork?n=3")
RSS=$(field "$R" rss_sum); PSS=$(field "$R" pss_sum)
show "forks: $R"
show "kernel accounting: sum(Pss) $PSS < sum(Rss) $RSS -> pages are shared"
[ "$PSS" -lt "$RSS" ] || { echo "FORKS NOT SHARING"; exit 1; }

say "live-migrate VM $VM: vset over TCP, microVM via the store"
post "$API1/vm/$VM/expect" >/dev/null
R=$(post "$API0/vm/$VM/migrate?to=1")
show "source: snapshot+publish $(field "$R" snapshot_ms)ms, vset handoff $(field "$R" handoff_ms)ms"
for _ in $(seq 1 100); do
  S=$(get "$API1/status")
  grep -q "\"id\":$VM,\"state\":\"running\"" <<<"$S" && break
  sleep 0.2
done
grep -q "\"id\":$VM,\"state\":\"running\"" <<<"$S" || { echo "MIGRATION NEVER LANDED"; exit 1; }
R=$(post "$API1/vm/$VM/verify")
show "vset verifies ON HOST 1: ok=$(field "$R" ok) at burst $(field "$R" burst)"
R=$(post "$API1/vm/$VM/work?bursts=1")
show "and keeps working there: burst $(field "$R" burst)"

say "backed VM: durable state survives host death via the store alone"
R=$(post "$API0/vm?backed=1")
BVM=$(field "$R" id)
post "$API0/vm/$BVM/work?bursts=2" >/dev/null
show "vm $BVM worked to burst 2 on host 0 (backed up continuously)"
sleep 1  # let backup publish
say "kill host 0 (the whole daemon, ungracefully)"
pkill -9 -f "demod $WORK/h0.conf"
pkill -9 -f "$WORK/h0/scratch" 2>/dev/null || true
R=$(post "$API1/vm/$BVM/restore")
show "host 1 restored vm $BVM from the bucket: verdict $(field "$R" verdict)"
R=$(post "$API1/vm/$BVM/verify")
show "vset verifies after restore: ok=$(field "$R" ok) at burst $(field "$R" burst)"
[ "$(field "$R" ok)" = "true" ] || { echo "RESTORE VERIFY FAILED"; exit 1; }

say "final status (host 1)"
get "$API1/status"

say "PASS"
