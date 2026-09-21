#!/bin/bash
# Variant C: the pinned latency comparison. Server on one P-core, client on the
# adjacent P-core, Go and Rust back to back in one session.
#
# This is the only latency protocol that measures the server rather than the
# scheduler on this host — see "Placement sensitivity" in baseline-go.md. The
# default protocol in run_baseline.sh deliberately does not pin; use this one
# when quoting a latency number.
#
#   ROUNDS=12 ./benchmarks/run_variant_c.sh
set -u
cd "$(dirname "$0")/.."
GO_BIN="${GO_BIN:-/home/vlad/apps/nats-server}"
RUST_BIN="${RUST_BIN:-$PWD/target/release/nats-server-rs}"
SERVER_CPU="${SERVER_CPU:-16}"     # a Cortex-X925 performance core
CLIENT_CPU="${CLIENT_CPU:-15}"     # its neighbour: one hop, same cluster
ROUNDS="${ROUNDS:-12}"
OUT="${OUT:-benchmarks/raw/$(date +%F)-variant-c-paired.txt}"
WRAP_DIR=/tmp/nats-variant-c
mkdir -p "$WRAP_DIR"

for probe in nats-server nats-server-rs; do
  if pgrep -x "$probe" >/dev/null; then
    echo "refusing to run: a $probe is already alive (skews every number)" >&2
    exit 1
  fi
done

# The harness only knows how to start a server, so the pin lives in a wrapper.
mk_wrap () { # $1 = label, $2 = binary
  local f="$WRAP_DIR/$1.sh"
  { echo '#!/bin/bash'; echo "exec taskset -c $SERVER_CPU $2 \"\$@\""; } > "$f"
  chmod +x "$f"
  echo "$f"
}
WRAP_GO=$(mk_wrap go "$GO_BIN")
WRAP_RUST=$(mk_wrap rust "$RUST_BIN")

: > "$OUT"
echo "# variant C: client pinned to CPU $CLIENT_CPU, server to CPU $SERVER_CPU" >> "$OUT"
echo "# go: $GO_BIN" >> "$OUT"
echo "# rust: $RUST_BIN" >> "$OUT"
echo "# load at start: $(cat /proc/loadavg)" >> "$OUT"

run_round () { # $1 = label, $2 = wrapper
  echo "=== round $ROUND $1 $(date +%H:%M:%S) load=$(cut -d' ' -f1-3 /proc/loadavg) ===" >> "$OUT"
  echo "# bin=$1" >> "$OUT"
  NATS_SERVER_BIN="$2" taskset -c "$CLIENT_CPU" ./target/release/latency >> "$OUT" 2>&1
}

for ROUND in $(seq 1 "$ROUNDS"); do
  run_round go "$WRAP_GO"
  run_round rust "$WRAP_RUST"
done
echo "# load at end: $(cat /proc/loadavg)" >> "$OUT"
grep -E '^bench|^# bin' "$OUT"
