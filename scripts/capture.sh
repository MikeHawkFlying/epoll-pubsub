#!/usr/bin/env bash
# Capture a short session with tcpdump and decode it with tshark using the
# Lua dissector in tools/, to check the framing on the wire.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-build/release}
OUT=${OUT:-results}
mkdir -p "$OUT"
PCAP="$OUT/pubsub.pcap"

"$BIN/pubsub-server" -b 127.0.0.1 -p 7400 >"$OUT/capture.server.log" 2>&1 &
SRV=$!
sudo tcpdump -i lo -U -s 0 -w "$PCAP" 'tcp port 7400' 2>"$OUT/tcpdump.log" &
DUMP=$!
# $DUMP is the sudo wrapper, which may not pass signals on, so signal
# tcpdump itself.
trap 'sudo pkill -x tcpdump 2>/dev/null || true; kill "$SRV" 2>/dev/null || true' EXIT
sleep 1

"$BIN/loadgen" -q -p 7400 -s 4 -P 2 -t 2 -r 20 -W 0 -d 2 -D 2

sleep 1
sudo pkill -INT -x tcpdump || true
wait "$DUMP" || true

tshark -X lua_script:tools/pubsub.lua -r "$PCAP" -Y pubsub -T fields -e pubsub.type \
    >"$OUT/frame-types.txt"

python3 - "$OUT/frame-types.txt" <<'EOF'
import collections, sys
names = {1: "SUB", 2: "UNSUB", 3: "PUB", 4: "PING", 5: "STATS",
         0x81: "OK", 0x82: "MSG", 0x83: "PONG", 0x84: "STATS_REPLY", 0x85: "ERR"}
counts = collections.Counter()
for line in open(sys.argv[1]):
    for v in line.replace(",", " ").split():
        counts[names.get(int(v, 0), v)] += 1
for k, v in sorted(counts.items()):
    print(f"{k:>12} {v}")
missing = {"SUB", "OK", "PUB", "MSG"} - set(counts)
if missing:
    sys.exit(f"dissector did not find: {', '.join(sorted(missing))}")
EOF
