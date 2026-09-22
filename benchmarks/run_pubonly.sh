#!/bin/bash
# Paired Go-vs-Rust run of the publish-only path: one connection, no
# subscribers. This is what `nats bench pub` drives, and it is a different
# server path from `pubsub` — nothing is written back, so it measures
# read + parse + interest-test only.
#
#   CLIENT=flood  (default) the drain probe: pre-encoded frames, one blocking
#                 writer thread, so the rate is the server's, not the client's
#   CLIENT=nats   the real `nats bench pub`, which is what a user sees
#   CLIENT=sub    the delivery path: `nats bench sub` driven by `nats bench pub`,
#                 reporting the subscriber's rate (the user's second number)
#
#   MSGS=20000000 SIZE=128 ROUNDS=4 ./benchmarks/run_pubonly.sh
set -u
cd "$(dirname "$0")/.."

GO_BIN="${GO_BIN:-/home/vlad/apps/nats-server}"
RUST_BIN="${RUST_BIN:-$PWD/target/release/nats-server-rs}"
NATS_CLI="${NATS_CLI:-nats}"
CLIENT="${CLIENT:-flood}"
PORT="${PORT:-4222}"
HOST="127.0.0.1"
MSGS="${MSGS:-20000000}"
SIZE="${SIZE:-128}"
ROUNDS="${ROUNDS:-4}"
PIN="${PIN:-}"          # e.g. "7" to pin the server to one core
CLIENT_PIN="${CLIENT_PIN:-}"  # the driver on a *different* core: unpinned it can land on the
                            # server's core, and the round then measures the scheduler
OUT="${OUT:-/tmp/pubonly-$(date +%F-%H%M).txt}"

for probe in nats-server nats-server-rs; do
  if pgrep -x "$probe" >/dev/null; then
    echo "refusing to run: a $probe is already alive (skews every number)" >&2
    exit 1
  fi
done

CLK=$(getconf CLK_TCK)
: > "$OUT"
echo "# load at start: $(cat /proc/loadavg)  msgs=$MSGS size=$SIZE client=$CLIENT pin=${PIN:-none}" >> "$OUT"

cpu_ticks () { awk '{for(i=14;i<=15;i++)t+=$i}END{print t+0}' /proc/"$1"/task/*/stat; }


run_arm () { # $1 = label, $2 = binary
  local label="$1" bin="$2" pid t0 t1 rate
  if [ -n "$PIN" ]; then taskset -c "$PIN" "$bin" -a "$HOST" -p "$PORT" >/dev/null 2>&1 &
  else "$bin" -a "$HOST" -p "$PORT" >/dev/null 2>&1 & fi
  pid=$!
  for _ in $(seq 1 200); do
    timeout 0.2 bash -c "exec 3<>/dev/tcp/$HOST/$PORT" 2>/dev/null && break
    sleep 0.02
  done
  # A server that could not bind the port exits at once, and the arm would
  # otherwise measure whatever else is listening.
  kill -0 "$pid" 2>/dev/null || { echo "# $label: server died (port $PORT busy?)" >> "$OUT"; return 1; }
  t0=$(cpu_ticks "$pid")
  if [ "$CLIENT" = nats ]; then
    rate=$("$NATS_CLI" bench pub benchsubj --msgs "$MSGS" --size "$SIZE" --no-progress \
            -s "nats://$HOST:$PORT" 2>&1 | grep -oE '[0-9][0-9,]* msgs/sec' \
            | head -1 | awk '{gsub(/,/,"",$1); print $1}')
  elif [ "$CLIENT" = sub ]; then
    "$NATS_CLI" bench sub benchsubj --msgs "$MSGS" --size "$SIZE" --no-progress \
        -s "nats://$HOST:$PORT" > /tmp/subout.$$ 2>&1 &
    sp=$!
    sleep 0.35
    "$NATS_CLI" bench pub benchsubj --msgs "$MSGS" --size "$SIZE" --no-progress \
        -s "nats://$HOST:$PORT" >/dev/null 2>&1
    wait $sp 2>/dev/null
    rate=$(grep -oE '[0-9][0-9,]* msgs/sec' /tmp/subout.$$ | head -1 | awk '{gsub(/,/,"",$1); print $1}')
    mv "$OUT" "$OUT.keep" 2>/dev/null; grep -v "^Finished" "$OUT.keep" > "$OUT"; mv "$OUT.keep" "$OUT"
  else
    rate=$(ADDR="$HOST:$PORT" MSGS="$MSGS" SIZE="$SIZE" ${CLIENT_PIN:+taskset -c "$CLIENT_PIN"} ./target/release/floodpub 2>/dev/null \
            | grep -oE 'msgs_per_sec=[0-9]+' | cut -d= -f2)
  fi
  t1=$(cpu_ticks "$pid")
  local cpu=$(( t1 - t0 ))
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  python3 -c "
rate=float('${rate:-0}'); msgs=$MSGS; cpu=$cpu; clk=$CLK
print(f'bench name=pubonly bin=$label msgs={msgs:.0f} size=$SIZE rate={rate:.0f} ns_per_msg={1e9/rate if rate else 0:.0f} server_us_per_msg={cpu*1e6/clk/msgs:.3f} server_cores={(cpu/clk)/(msgs/rate if rate else 1):.2f}')" >> "$OUT"
}

for ROUND in $(seq 1 "$ROUNDS"); do
  echo "# round $ROUND $(date +%H:%M:%S) load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT"
  run_arm go "$GO_BIN"
  run_arm rust "$RUST_BIN"
done
echo "# load at end: $(cat /proc/loadavg)" >> "$OUT"
grep -E '^bench name|^# ' "$OUT"
