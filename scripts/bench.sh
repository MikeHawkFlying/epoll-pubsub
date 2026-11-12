#!/usr/bin/env bash
# Throughput / latency runs. Server and load generator share one machine, so
# the numbers include the cost of generating the load too.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-build/release}
OUT=${OUT:-results}
mkdir -p "$OUT"
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)"

run() {
    local name=$1
    shift
    "$BIN/pubsub-server" -b 127.0.0.1 -p 7400 -c 20000 >"$OUT/$name.server.log" 2>&1 &
    local srv=$!
    sleep 0.5
    echo "== $name: loadgen $*"
    "$BIN/loadgen" -q -p 7400 -j "$OUT/$name.json" "$@" | tee "$OUT/$name.txt"
    kill "$srv"
    wait "$srv" || true
    sleep 1
}

# 5,000 subscribers over 50 topics => every publish fans out to 100 clients.
run fanout-5k-20k  -s 5000 -P 10 -t 50 -r 200   -W 2 -d 10
run fanout-5k-100k -s 5000 -P 10 -t 50 -r 1000  -W 2 -d 10
run fanout-5k-200k -s 5000 -P 10 -t 50 -r 2000  -W 2 -d 10
run fanout-5k-500k -s 5000 -P 10 -t 50 -r 5000  -W 2 -d 10
# Connection count on its own: 10,000 subscribers, light traffic.
run conns-10k      -s 10000 -P 10 -t 100 -r 200 -W 2 -d 10
# One subscriber per topic (no fan-out), many publishers.
run direct-50k     -s 100 -P 100 -t 100 -r 50000 -W 2 -d 10

python3 scripts/bench_report.py "$OUT" | tee "$OUT/bench.md"
