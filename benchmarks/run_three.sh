#!/bin/bash
# Three-arm paired run: the build *before* a change, the build *after* it, and the
# reference binary — inside the same round, so whatever the machine is doing, it is
# doing it to all three. This is the protocol `specs/perf-notes.md` requires before
# anything is called a keeper: a Rust-only number from a warm session and a
# Rust-only number from a cold one differ by 1.7× on this host with no change to
# the server at all (see "One caveat the paired protocol caught").
#
#   MODE=pubonly ROUNDS=4 PIN=7 CLIENT_PIN=3 \
#     OLD=/tmp/before/target/release/nats-server-rs \
#     NEW=$PWD/target/release/nats-server-rs \
#     ./benchmarks/run_three.sh
#
#   MODE=pubsub  ROUNDS=3 MSGS=3000000 SIZE=256 PIN=7 ./benchmarks/run_three.sh
#
# MODE=pubonly drives crates/bench's floodpub (one publisher, no subscribers);
# MODE=pubsub drives the pubsub bench (one publisher, one subscriber) against an
# already-running server, which is what NATS_BENCH_URL exists for.
set -u
cd "$(dirname "$0")/.."

NEW="${NEW:-$PWD/target/release/nats-server-rs}"
OLD="${OLD:-}"
GO="${GO:-/home/vlad/apps/nats-server}"
MODE="${MODE:-pubonly}"
PORT="${PORT:-4222}"
HOST="127.0.0.1"
MSGS="${MSGS:-20000000}"
SIZE="${SIZE:-128}"
ROUNDS="${ROUNDS:-4}"
PIN="${PIN:-}"
CLIENT_PIN="${CLIENT_PIN:-}"
OUT="${OUT:-/tmp/three-$(date +%F-%H%M).txt}"

if pgrep -x nats-server >/dev/null || pgrep -x nats-server-rs >/dev/null; then
  echo "refusing to run: a server is already alive (skews every number)" >&2
  pgrep -an nats-server nats-server-rs >&2 || true
  exit 1
fi
CLK=$(getconf CLK_TCK)
: > "$OUT"
echo "# load at start: $(cat /proc/loadavg) mode=$MODE msgs=$MSGS size=$SIZE pin=${PIN:-none}" >> "$OUT"

cpu_ticks () { awk '{for(i=14;i<=15;i++)t+=$i}END{print t+0}' /proc/"$1"/task/*/stat; }
rss_kb ()    { awk '/VmRSS/{print $2}' /proc/"$1"/status 2>/dev/null; }

run_arm () { # $1 = label, $2 = binary
  local label="$1" bin="$2" pid t0 t1 rate peak v
  [ -x "$bin" ] || { echo "# $label: no binary ($bin), skipped" >> "$OUT"; return; }
  if [ -n "$PIN" ]; then taskset -c "$PIN" "$bin" -a "$HOST" -p "$PORT" >/dev/null 2>&1 &
  else "$bin" -a "$HOST" -p "$PORT" >/dev/null 2>&1 & fi
  pid=$!
  for _ in $(seq 1 200); do
    timeout 0.2 bash -c "exec 3<>/dev/tcp/$HOST/$PORT" 2>/dev/null && break
    sleep 0.02
  done
  kill -0 "$pid" 2>/dev/null || { echo "# $label: server died (port busy?)" >> "$OUT"; return 1; }
  t0=$(cpu_ticks "$pid")
  if [ "$MODE" = pubsub ]; then
    MSGS="$MSGS" SIZE="$SIZE" NATS_BENCH_URL="nats://$HOST:$PORT" \
      ${CLIENT_PIN:+taskset -c "$CLIENT_PIN"} ./target/release/pubsub >/tmp/three-bench.txt 2>&1 &
    local bp=$!
    peak=0
    while kill -0 $bp 2>/dev/null; do v=$(rss_kb "$pid"); peak=$(( v > peak ? v : peak )); sleep 0.05; done
    wait $bp
    rate=$(grep -oE 'msgs_per_sec=[0-9]+' /tmp/three-bench.txt | tail -1 | cut -d= -f2)
  else
    peak=0
    rate=$(ADDR="$HOST:$PORT" MSGS="$MSGS" SIZE="$SIZE" \
      ${CLIENT_PIN:+taskset -c "$CLIENT_PIN"} ./target/release/floodpub 2>/dev/null \
      | grep -oE 'msgs_per_sec=[0-9]+' | cut -d= -f2)
  fi
  t1=$(cpu_ticks "$pid")
  local cpu=$(( t1 - t0 ))
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  python3 -c "
rate=float('${rate:-0}'); msgs=$MSGS; cpu=$cpu; clk=$CLK
print(f'bench name=$MODE arm=$label msgs={msgs:.0f} size=$SIZE rate={rate:.0f} '
      f'server_us_per_msg={cpu*1e6/clk/msgs:.3f} server_cores={(cpu/clk)/(msgs/rate if rate else 1):.2f} '
      f'peak_rss_mib={$peak/1024:.1f}')" >> "$OUT"
}

ARMS="go:$GO"
[ -n "$OLD" ] && ARMS="old:$OLD $ARMS"
ARMS="$ARMS new:$NEW"
for ROUND in $(seq 1 "$ROUNDS"); do
  echo "# round $ROUND $(date +%H:%M:%S) load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT"
  for a in $ARMS; do run_arm "${a%%:*}" "${a#*:}"; done
done
echo "# load at end: $(cat /proc/loadavg)" >> "$OUT"
grep -E '^bench name|^# ' "$OUT"
