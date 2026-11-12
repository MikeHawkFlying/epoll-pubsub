#!/usr/bin/env python3
"""Turn the loadgen JSON files in a results dir into a markdown table."""

import json
import os
import platform
import sys

out = sys.argv[1] if len(sys.argv) > 1 else "results"
order = [
    "fanout-5k-20k",
    "fanout-5k-100k",
    "fanout-5k-200k",
    "fanout-5k-500k",
    "conns-10k",
    "direct-50k",
]

cpus = os.cpu_count()
model = ""
try:
    with open("/proc/cpuinfo") as f:
        for line in f:
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
except OSError:
    pass

print(f"Machine: {cpus} vCPU {model} ({platform.system()} {platform.release()})\n")
print("| run | connections | fan-out | target delivered/s | delivered/s | p50 | p99 | p99.9 | lost | drops |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for name in order:
    path = os.path.join(out, f"{name}.json")
    if not os.path.exists(path):
        continue
    with open(path) as f:
        r = json.load(f)
    fanout = r["subscribers"] / r["topics"]
    target = r["target_rate"] * fanout
    print(
        f"| {name} | {r['connections']:,} | {fanout:.0f} | {target:,.0f} | "
        f"{r['delivered_per_sec']:,.0f} | {r['p50_us'] / 1000:.2f} ms | "
        f"{r['p99_us'] / 1000:.2f} ms | {r['p999_us'] / 1000:.2f} ms | "
        f"{r['lost']} | {r['disconnects']} |"
    )
