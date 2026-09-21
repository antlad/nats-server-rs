#!/bin/bash
# Canonical Go-vs-Rust baseline protocol: interleaved rounds of all three bench
# binaries at their default parameters, no affinity, no priority changes.
#
#   export NATS_SERVER_BIN=/home/vlad/apps/nats-server   # or the Rust binary
#   ROUNDS=16 OUT=/tmp/baseline_raw.txt ./benchmarks/run_baseline.sh
#
# See benchmarks/baseline-go.md for the recorded reference numbers and for the
# pinned latency gate, which this script deliberately does not do.
set -u
cd "$(dirname "$0")/.."
export NATS_SERVER_BIN="${NATS_SERVER_BIN:?point it at the nats-server binary under test}"
OUT="${OUT:-/tmp/baseline_raw.txt}"
ROUNDS="${ROUNDS:-5}"

if pgrep -x nats-server >/dev/null; then
  echo "refusing to run: a nats-server is already alive (skews every number)" >&2
  pgrep -ax nats-server >&2
  exit 1
fi

: > "$OUT"
echo "# load at start: $(cat /proc/loadavg)" >> "$OUT"
for i in $(seq 1 "$ROUNDS"); do
  echo "=== round $i $(date +%H:%M:%S) load=$(cut -d' ' -f1-3 /proc/loadavg) ===" >> "$OUT"
  ./target/release/pubsub   >> "$OUT" 2>&1
  ./target/release/latency  >> "$OUT" 2>&1
  ./target/release/fanout   >> "$OUT" 2>&1
done
echo "# load at end: $(cat /proc/loadavg)" >> "$OUT"
grep '^bench' "$OUT"
