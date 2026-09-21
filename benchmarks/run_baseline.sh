#!/bin/bash
# Canonical Go-vs-Rust baseline protocol: interleaved rounds of all three bench
# binaries at their default parameters, no affinity, no priority changes.
#
#   Single binary (Part 1 form):
#     export NATS_SERVER_BIN=/home/vlad/apps/nats-server
#     ROUNDS=16 OUT=/tmp/baseline_raw.txt ./benchmarks/run_baseline.sh
#
#   Paired — both binaries in the same session, so co-tenant drift hits each one
#   equally (the form Task 19 requires once crates/server exists):
#     GO_BIN=/home/vlad/apps/nats-server \
#     RUST_BIN=$PWD/target/release/nats-server-rs \
#     ROUNDS=16 OUT=benchmarks/raw/$(date +%F)-paired.txt ./benchmarks/run_baseline.sh
#
# In paired mode each `bench` line is preceded by a `# bin=` comment naming the
# server under test; the bench lines themselves are unchanged.
#
# See benchmarks/baseline-go.md for the recorded reference numbers and for the
# pinned latency gate, which this script deliberately does not do.
set -u
cd "$(dirname "$0")/.."
GO_BIN="${GO_BIN:-}"
RUST_BIN="${RUST_BIN:-}"
if [ -n "$GO_BIN" ] || [ -n "$RUST_BIN" ]; then
  PAIRED=1
  : "${GO_BIN:?paired mode needs GO_BIN}"
  : "${RUST_BIN:?paired mode needs RUST_BIN}"
else
  PAIRED=0
  export NATS_SERVER_BIN="${NATS_SERVER_BIN:?point it at the nats-server binary under test}"
fi
OUT="${OUT:-/tmp/baseline_raw.txt}"
ROUNDS="${ROUNDS:-5}"

for probe in nats-server nats-server-rs; do
  if pgrep -x "$probe" >/dev/null; then
    echo "refusing to run: a $probe is already alive (skews every number)" >&2
    pgrep -ax "$probe" >&2
    exit 1
  fi
done

: > "$OUT"
echo "# load at start: $(cat /proc/loadavg)" >> "$OUT"
echo "# go: $GO_BIN" >> "$OUT"
echo "# rust: $RUST_BIN" >> "$OUT"

run_round () { # $1 = label, $2 = binary
  echo "=== round $ROUND $1 $(date +%H:%M:%S) load=$(cut -d' ' -f1-3 /proc/loadavg) ===" >> "$OUT"
  echo "# bin=$1" >> "$OUT"
  NATS_SERVER_BIN="$2" ./target/release/pubsub   >> "$OUT" 2>&1
  NATS_SERVER_BIN="$2" ./target/release/latency  >> "$OUT" 2>&1
  NATS_SERVER_BIN="$2" ./target/release/fanout   >> "$OUT" 2>&1
}

for ROUND in $(seq 1 "$ROUNDS"); do
  if [ "$PAIRED" = 1 ]; then
    run_round go "$GO_BIN"
    run_round rust "$RUST_BIN"
  else
    run_round server "$NATS_SERVER_BIN"
  fi
done
echo "# load at end: $(cat /proc/loadavg)" >> "$OUT"
grep -E '^bench|^# bin' "$OUT"
