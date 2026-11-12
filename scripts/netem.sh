#!/usr/bin/env bash
# Run a verified workload over a bad network: 5% packet loss and 100 ms extra
# delay on loopback (tc netem). TCP should hide all of it; loadgen -V exits
# non-zero if a single message is lost, duplicated or reordered.
# Needs root for tc.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-build/release}
OUT=${OUT:-results}
LOSS=${LOSS:-5%}
DELAY=${DELAY:-100ms}
mkdir -p "$OUT"

"$BIN/pubsub-server" -b 127.0.0.1 -p 7401 >"$OUT/netem.server.log" 2>&1 &
SRV=$!
cleanup() {
    sudo tc qdisc del dev lo root 2>/dev/null || true
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
}
trap cleanup EXIT
sleep 0.5

sudo tc qdisc add dev lo root netem delay "$DELAY" loss "$LOSS"
echo "lo: $(tc qdisc show dev lo)"

"$BIN/loadgen" -V -p 7401 -s 200 -P 5 -t 10 -r 200 -W 0 -d 20 -D 60 \
    -j "$OUT/netem.json" | tee "$OUT/netem.txt"
