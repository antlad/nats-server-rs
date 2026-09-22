#!/bin/bash
# The allocation census: allocations and bytes per unit of work, read off both
# binaries' own counters. Output is the table in benchmarks/alloc-census.md.
#
#   MSGS=2000000 SIZE=256 PIN=7 ./benchmarks/tools/census.sh
#
# Ours needs `--features allocstats`, the reference needs the instrumented build
# (specs/tools/go-instrument/README.md). Neither binary is used for timing: the
# counters cost a relaxed add each, so the throughput of these runs is not the
# throughput in specs/perf-notes.md.
set -u
cd "$(dirname "$0")/../.."
RS="${BIN_RS:-$PWD/target/release/nats-server-rs}"
GO="${BIN_GO:-/tmp/nats-instr/nats-server-instr}"
MSGS="${MSGS:-2000000}"
SIZE="${SIZE:-256}"
PIN="${PIN:-7}"
CPIN="${CPIN:-3}"
OUT="${OUT:-/tmp/census-$(date +%F-%H%M).txt}"
: > "$OUT"

run() { # $1 label, $2 binary, $3 bench, $4 stats env var
  local label="$1" bin="$2" bench="$3" var="$4" f pid tp out msgs
  f=$(mktemp); : > "$f"
  taskset -c "$PIN" env "$var=$f" "$bin" -a 127.0.0.1 -p 4231 >>"$OUT" 2>&1 &
  tp=$!
  local ready=no
  for _ in $(seq 1 250); do
    timeout 0.2 bash -c "exec 3<>/dev/tcp/127.0.0.1/4231" 2>/dev/null && { ready=yes; break; }
    sleep 0.02
  done
  [ "$ready" = yes ] || { echo "# $label: server did not listen" >> "$OUT"; return 1; }
  pid=$(pgrep -P $tp); [ -z "$pid" ] && pid=$tp
  out=$(MSGS=$MSGS SIZE=$SIZE ADDR=127.0.0.1:4231 STALL_SECS=20 \
    taskset -c $CPIN "./target/release/$bench" 2>&1)
  msgs=$(printf '%s' "$out" | grep -oE 'msgs=[0-9]+' | head -1 | cut -d= -f2)
  kill -TERM $tp 2>/dev/null; kill -TERM $pid 2>/dev/null; wait $tp 2>/dev/null
  python3 - "$label" "$bench" "$f" "${msgs:-$MSGS}" >> "$OUT" <<'PY'
import os, re, sys
label, bench, path, msgs = sys.argv[1], sys.argv[2], sys.argv[3], max(int(sys.argv[4]), 1)
if not os.path.exists(path):
    print('# %s %s: no stats file' % (label, bench)); sys.exit()
text = open(path).read()
tot = [l for l in text.splitlines() if 'allocs=' in l]
hist = [l for l in text.splitlines() if l.startswith('allochist')]
tags = [l for l in text.splitlines() if l.startswith('alloctags')]
def nums(line): return {k: int(v) for k, v in re.findall(r'(\w+)=(\d+)', line)}
a, b = nums(tot[0]), nums(tot[-1])
print('bench name=census arm=%-10s unit=%-12s msgs=%-9d allocs_per_msg=%.3f bytes_per_msg=%.1f'
      % (label, bench, msgs, (b['allocs'] - a['allocs']) / msgs, (b['bytes'] - a['bytes']) / msgs))
if hist:
    h = {int(k): int(v) for k, v in re.findall(r'2\^(\d+)=(\d+)', hist[-1])}
    g = {int(k): int(v) for k, v in re.findall(r'2\^(\d+)=(\d+)', hist[0])} if len(hist) > 1 else {}
    row = ', '.join('2^%d(%.3f/msg)' % (k, (v - g.get(k, 0)) / msgs)
                    for k, v in sorted(h.items()) if v - g.get(k, 0) > 0)
    print('#   sizes: ' + row)
if tags:
    t = nums(tags[-1].replace('alloctags', 'alloctags x=0'))
    print('#   tags: ' + ', '.join('%s=%.3f/msg' % (k, v / msgs)
                                   for k, v in t.items() if k != 'x' and v))
PY
  mv "$f" "/tmp/census-last-$(echo "$label$bench" | tr -dc 'a-z').txt" 2>/dev/null
}

echo "# census $(date) msgs=$MSGS size=$SIZE pin=$PIN load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT"
run ours "$RS" floodpub NATS_RS_STATS_FILE
run reference "$GO" floodpub NATS_GO_STATS_FILE
run ours "$RS" floodpubsub NATS_RS_STATS_FILE
run reference "$GO" floodpubsub NATS_GO_STATS_FILE
grep -E '^bench|^#' "$OUT"
