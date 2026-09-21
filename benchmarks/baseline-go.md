# Baseline: reference Go nats-server

Results recorded so Rust-server runs can be diffed against them. Same harness,
same binaries, same client — only `$NATS_SERVER_BIN` changes.

**This replaces the Apple M1 Max baseline**, which was never valid on this box:
the whole point of the file is a same-machine Go-vs-Rust diff, so numbers from
another host are worse than useless — they are misleading. The macOS figures are
preserved under "Previous host" so the ~10× delta stays explainable.

| | |
|---|---|
| Date | 2026-09-20, 22:12–22:39 (Europe/Moscow) |
| Server under test | `/home/vlad/apps/nats-server` — `v2.16.0-dev`; module `v2.15.1-0.20260918153739-8ad52657d1f3` (commit `8ad5265`, 2026-09-18), go1.26.8 linux/arm64 |
| Machine | NVIDIA GB10-class SoC (DGX Spark): Cortex-X925 ×10 @ 3.90 GHz + Cortex-A725 ×10 @ 2.808 GHz, 20 CPUs, 1 NUMA node, 127.5 GB RAM, Linux 6.17.0-1021-nvidia aarch64 |
| Client | async-nats 0.50, in-process, loopback only |
| Build | `cargo build --release` (rustc 1.98.1, cargo 1.98.1), repo HEAD `4d58e80` |
| Runs | 16 consecutive rounds of all three benches, interleaved, default parameters |
| Suite health | `cargo test --workspace --release` → 25 passed, 0 failed, immediately before the runs |
| Host condition | idle loadavg 1.03 before round 1 (0.95 at 22:09), 1.1–3.1 during; co-resident tenant `VLLM::EngineCore` (~10.4 GB RSS, ~7 % CPU) plus rust-analyzer/VS Code servers; MemAvailable ≈14 GB of 127 GB, 4.5 GB swap in use, non-zero `allocstall_*`/`compact_stall` counters — the box is permanently under memory pressure |
| Not pinned | no `taskset`, no `chrt`, no root changes (passwordless sudo unavailable in this session) |

Core map, from `cpufreq` maxima cross-checked against `lscpu`: **P-cores = 5–9
and 15–19** (X925, 3.9 GHz), **E-cores = 0–4 and 10–14** (A725, 2.808 GHz).
Scaling governor is `performance` everywhere, so clock never sags — but
`cpuidle` (driver `acpi_idle`, governor `menu`) has a state **LPI-3 with 433 µs
exit latency entered 17.1 M times per core**. That asymmetry is the single
biggest factor in every number below.

## Headline baseline — default protocol

What `./target/release/{pubsub,latency,fanout}` gives on this machine as it is
configured today. Compare Rust-server runs against **this** table when using the
same unpinned protocol.

| Bench | Parameters | Metric | Baseline (median of 16) | min – max | IQR | spread |
|---|---|---|---|---|---|---|
| pubsub | MSGS=1_000_000 SIZE=256 | msgs/sec | **225,932** | 154,005 – 361,585 | 201,084–261,798 | 92 % |
| pubsub | MSGS=1_000_000 SIZE=256 | MB/sec | 55.16 | — | — | — |
| pubsub | MSGS=1_000_000 SIZE=256 | wall | 4,429 ms | — | — | — |
| latency | ITERS=20_000 WARMUP=1_000 | p50 | **662 µs** | 375 – 1,921 µs | 623–732 µs | 233 % |
| latency | ITERS=20_000 WARMUP=1_000 | p99 | 2,814 µs | 2,261 – 3,307 µs | — | — |
| latency | ITERS=20_000 WARMUP=1_000 | p999 | 3,514 µs | — | — | — |
| latency | ITERS=20_000 WARMUP=1_000 | max | 4,275 µs | — | — | — |
| fanout | MSGS=100_000 SIZE=256 SUBSCRIBERS=10 | deliveries/sec | **221,420** | 215,954 – 223,507 | — | 3 % |
| fanout | MSGS=100_000 SIZE=256 SUBSCRIBERS=10 | MB/sec | 54.06 | — | — | — |
| fanout | MSGS=100_000 SIZE=256 SUBSCRIBERS=10 | wall | 4,516 ms | — | — | — |
| fanout | per subscriber (÷ 10) | msgs/sec | 22,142 | — | — | — |

### Every run, in order

| # | time | load 1m | pubsub msgs/sec | latency p50 µs | latency p99 µs | fanout deliveries/sec |
|---:|---|---|---:|---:|---:|---:|
| 1 | 22:12:21 | 1.03 | 304,319 | 636 | 2,719 | 219,318 |
| 2 | 22:12:45 | 1.85 | 238,567 | 698 | 2,868 | 223,507 |
| 3 | 22:13:12 | 2.09 | 220,334 | 490 | 2,753 | 222,009 |
| 4 | 22:13:36 | 2.55 | 189,141 | 709 | 2,508 | 221,832 |
| 5 | 22:14:03 | 2.39 | 185,591 | 1,921 | 3,307 | 221,539 |
| 6 | 22:14:51 | 2.65 | 219,471 | 900 | 3,222 | 221,574 |
| 7 | 22:15:27 | 2.80 | 154,005 | 828 | 2,920 | 221,462 |
| 8 | 22:27:52 | 1.12 | 289,944 | 656 | 2,989 | 222,153 |
| 9 | 22:28:17 | 1.47 | 252,416 | 655 | 2,761 | 221,379 |
| 10 | 22:28:42 | 1.87 | 238,796 | 656 | 2,965 | 220,444 |
| 11 | 22:29:08 | 2.33 | 361,585 | 723 | 2,492 | 215,954 |
| 12 | 22:29:33 | 2.70 | 189,417 | 669 | 2,926 | 221,579 |
| 13 | 22:37:46 | 1.66 | 325,313 | 585 | 2,895 | 219,527 |
| 14 | 22:38:10 | 1.48 | 210,566 | 429 | 2,540 | 216,973 |
| 15 | 22:38:31 | 1.64 | 231,529 | 375 | 2,261 | 217,929 |
| 16 | 22:38:52 | 2.12 | 204,973 | 761 | 2,467 | 220,599 |

Rounds 1–7, 8–12 and 13–16 come from three separate runner invocations at 22:12,
22:27 and 22:37 — same protocol, same binary, nothing changed in between except
whatever the co-resident tenants were doing. The round counter in the archived
logs restarts inside each file; the `#` column here is the authoritative index.

## Placement sensitivity — read this before quoting a latency number

Two things dominate measurement variance here, and neither is the server under
test: which core class the Linux scheduler lands the client and the server on,
and whether that core was asleep in LPI-3 (433 µs to wake). A round-trip pays
that exit latency once per hop; a throughput run pipelines around it.

Same binary, same code, only affinity changed. Rows **D** and **P** are medians
over the run counts named in the "How to use" table; rows **C**, **S**, **E**,
**W** are one or two rounds each — enough to locate the effect, not to bound it.

| Variant | client cores | server cores | pubsub msgs/sec | latency p50 µs | latency p99 µs | fanout deliveries/sec |
|---|---|---|---|---|---|---|
| **D** default | free | free (all 20) | 225,932 (16 runs) | 662 (16 runs) | 2,814 | 221,420 (16 runs) |
| **P** disjoint P-clusters | 5–9 | 15–19 | 558,471 (5 runs) | 1,320 | 3,014 | 832,595 (5 runs) |
| **C** one P-core pair | 5 | 6 | 483,441 / 482,346 | **73 / 43** | **86 / 52** | 1,004,832 / 997,334 |
| **S** one core shared | 5 | 5 | not run | 71 / 72 | 83 / 83 | not run |
| **E** P ↔ E-core | 5 | 10 | not run | 140 | 1,008 | not run |
| **W** default, cores denied deep idle | free | free | 127,691 | 159 | 1,474 | 248,480 |

- **C is the protocol-path ceiling.** With client and server on two adjacent
  P-cores and no wake-up penalty, the Go reference server does **p50 43–73 µs,
  p99 52–86 µs** — better than the old M1 Max baseline (83/127 µs) on the same
  code. This is the number to gate Rust regressions on.
- **P is the trap.** Disjoint 5-core sets triple throughput (pubsub 558,471,
  fanout 832,595, i.e. 2.5× and 3.8× the default) while *doubling* latency
  p50 to 1,320 µs: throughput is pipelined and hides cross-cluster handoff,
  the serial ping-pong pays it on every sample.
- **W isolates the C-state effect.** Nothing pinned; 20 `nice 19` spinners just
  keep every core out of LPI-3. p50 falls from 662 µs to 159 µs — ~4× of
  the default-protocol latency is pure idle-exit, not NATS. (pubsub degrades to
  127,691 in W because the spinners eat the idle cycles the client was borrowing;
  treat W as a latency-only probe.)
- **The real ceiling is above all of these.** One round taken straight after
  variant W, while every core was still hot, measured 757,541 msgs/sec pubsub and
  p50 375 µs unpinned. The default-protocol numbers are not this host's
  capability; they are what an unmanaged scheduler hands a 2-thread workload on
  20 cores.

## How to use this baseline

| Signal | Use it as | Threshold on this host |
|---|---|---|
| fanout, default protocol | the workhorse: tightest distribution here | >6 % = real, ≤3 % = noise (3 % peak-to-peak over 16 runs) |
| pubsub, default protocol | coarse throughput check | >25 % = real, ≤10 % = noise (92 % peak-to-peak, but the middle half sits inside ±15 %) |
| latency, default protocol | **not a regression signal** — placement lottery | p50 scatter 233 %; report it, don't act on it |
| latency, variant **C** | the latency gate | two rounds differed by 30 µs at p50 (73 vs 43), so run ≥3 and treat a >30 µs median shift as real |
| anything after a root `state3/disable` | new machine regime — re-record, do not diff against this file | — |

Always record, next to the numbers: `cat /proc/loadavg`, `pgrep -x nats-server`
(must be empty before the run), and whether anything was pinned. Batch drift is
real and unexplained: the three batches in the run table are ~15 minutes apart,
and their pubsub medians are 219,471 / 252,416 / 221,047 (±15 %) while their p50
medians are 709 / 656 / 507 µs (a 202 µs band). Nothing changed in the code or the
parameters, so treat a lone Rust run as uncalibrated until it has been
interleaved with a Go control.

## Reproduce

```sh
export NATS_SERVER_BIN=/home/vlad/apps/nats-server
cd ~/projects/self/nats-server-rs
cargo build --release
pgrep -x nats-server || echo "clean"     # a stray server skews everything
ROUNDS=16 ./benchmarks/run_baseline.sh
```

Runner: `benchmarks/run_baseline.sh`. It appends a
`=== round N hh:mm:ss load=a b c ===` marker and then runs the three benches in a
fixed order, so no bench is always measured at the same point in the co-tenant
cycle; it refuses to start if a `nats-server` is already alive. This is the same
protocol that produced the numbers above, with the stray-server guard added.

Latency gate (variant C — affinity is inherited by the spawned server, so a
wrapper script is the only thing needed):

```sh
cat > /tmp/nats_c.sh <<'WRAP'
#!/bin/bash
exec taskset -c 6 /home/vlad/apps/nats-server "$@"
WRAP
chmod +x /tmp/nats_c.sh
NATS_SERVER_BIN=/tmp/nats_c.sh taskset -c 5 ./target/release/latency
```

Pinned-throughput variant (row P): `NATS_SERVER_BIN=/tmp/nats_pinned.sh
taskset -c 5-9 ./target/release/pubsub`, where the wrapper is
`exec taskset -c 15-19 /home/vlad/apps/nats-server "$@"`.

If root is available, this removes the dominant noise source and makes the
default protocol usable — but it changes the machine, so re-record rather than
diff against this file:

```sh
for f in /sys/devices/system/cpu/cpu*/cpuidle/state2/disable; do echo 1 > "$f"; done
for f in /sys/devices/system/cpu/cpu*/cpuidle/state3/disable; do echo 1 > "$f"; done
```

## Raw output — default protocol (16 rounds)

```
bench name=pubsub msgs=1000000 size=256 duration_ms=3286 msgs_per_sec=304319 mb_per_sec=74.30
bench name=latency samples=20000 p50_us=636 p99_us=2719 p999_us=3256 max_us=4146
bench name=fanout msgs=1000000 size=256 duration_ms=4560 msgs_per_sec=219318 mb_per_sec=53.54
bench name=pubsub msgs=1000000 size=256 duration_ms=4192 msgs_per_sec=238567 mb_per_sec=58.24
bench name=latency samples=20000 p50_us=698 p99_us=2868 p999_us=3521 max_us=3979
bench name=fanout msgs=1000000 size=256 duration_ms=4474 msgs_per_sec=223507 mb_per_sec=54.57
bench name=pubsub msgs=1000000 size=256 duration_ms=4539 msgs_per_sec=220334 mb_per_sec=53.79
bench name=latency samples=20000 p50_us=490 p99_us=2753 p999_us=3619 max_us=4681
bench name=fanout msgs=1000000 size=256 duration_ms=4504 msgs_per_sec=222009 mb_per_sec=54.20
bench name=pubsub msgs=1000000 size=256 duration_ms=5287 msgs_per_sec=189141 mb_per_sec=46.18
bench name=latency samples=20000 p50_us=709 p99_us=2508 p999_us=3273 max_us=4309
bench name=fanout msgs=1000000 size=256 duration_ms=4508 msgs_per_sec=221832 mb_per_sec=54.16
bench name=pubsub msgs=1000000 size=256 duration_ms=5388 msgs_per_sec=185591 mb_per_sec=45.31
bench name=latency samples=20000 p50_us=1921 p99_us=3307 p999_us=3646 max_us=4213
bench name=fanout msgs=1000000 size=256 duration_ms=4514 msgs_per_sec=221539 mb_per_sec=54.09
bench name=pubsub msgs=1000000 size=256 duration_ms=4556 msgs_per_sec=219471 mb_per_sec=53.58
bench name=latency samples=20000 p50_us=900 p99_us=3222 p999_us=3783 max_us=4479
bench name=fanout msgs=1000000 size=256 duration_ms=4513 msgs_per_sec=221574 mb_per_sec=54.10
bench name=pubsub msgs=1000000 size=256 duration_ms=6493 msgs_per_sec=154005 mb_per_sec=37.60
bench name=latency samples=20000 p50_us=828 p99_us=2920 p999_us=3391 max_us=4396
bench name=fanout msgs=1000000 size=256 duration_ms=4515 msgs_per_sec=221462 mb_per_sec=54.07
bench name=pubsub msgs=1000000 size=256 duration_ms=3449 msgs_per_sec=289944 mb_per_sec=70.79
bench name=latency samples=20000 p50_us=656 p99_us=2989 p999_us=4045 max_us=4729
bench name=fanout msgs=1000000 size=256 duration_ms=4501 msgs_per_sec=222153 mb_per_sec=54.24
bench name=pubsub msgs=1000000 size=256 duration_ms=3962 msgs_per_sec=252416 mb_per_sec=61.63
bench name=latency samples=20000 p50_us=655 p99_us=2761 p999_us=3491 max_us=4198
bench name=fanout msgs=1000000 size=256 duration_ms=4517 msgs_per_sec=221379 mb_per_sec=54.05
bench name=pubsub msgs=1000000 size=256 duration_ms=4188 msgs_per_sec=238796 mb_per_sec=58.30
bench name=latency samples=20000 p50_us=656 p99_us=2965 p999_us=3631 max_us=4338
bench name=fanout msgs=1000000 size=256 duration_ms=4536 msgs_per_sec=220444 mb_per_sec=53.82
bench name=pubsub msgs=1000000 size=256 duration_ms=2766 msgs_per_sec=361585 mb_per_sec=88.28
bench name=latency samples=20000 p50_us=723 p99_us=2492 p999_us=3165 max_us=3756
bench name=fanout msgs=1000000 size=256 duration_ms=4631 msgs_per_sec=215954 mb_per_sec=52.72
bench name=pubsub msgs=1000000 size=256 duration_ms=5279 msgs_per_sec=189417 mb_per_sec=46.24
bench name=latency samples=20000 p50_us=669 p99_us=2926 p999_us=3811 max_us=4418
bench name=fanout msgs=1000000 size=256 duration_ms=4513 msgs_per_sec=221579 mb_per_sec=54.10
bench name=pubsub msgs=1000000 size=256 duration_ms=3074 msgs_per_sec=325313 mb_per_sec=79.42
bench name=latency samples=20000 p50_us=585 p99_us=2895 p999_us=3853 max_us=4352
bench name=fanout msgs=1000000 size=256 duration_ms=4555 msgs_per_sec=219527 mb_per_sec=53.60
bench name=pubsub msgs=1000000 size=256 duration_ms=4749 msgs_per_sec=210566 mb_per_sec=51.41
bench name=latency samples=20000 p50_us=429 p99_us=2540 p999_us=3507 max_us=4241
bench name=fanout msgs=1000000 size=256 duration_ms=4609 msgs_per_sec=216973 mb_per_sec=52.97
bench name=pubsub msgs=1000000 size=256 duration_ms=4319 msgs_per_sec=231529 mb_per_sec=56.53
bench name=latency samples=20000 p50_us=375 p99_us=2261 p999_us=2912 max_us=3695
bench name=fanout msgs=1000000 size=256 duration_ms=4589 msgs_per_sec=217929 mb_per_sec=53.21
bench name=pubsub msgs=1000000 size=256 duration_ms=4879 msgs_per_sec=204973 mb_per_sec=50.04
bench name=latency samples=20000 p50_us=761 p99_us=2467 p999_us=3070 max_us=3973
bench name=fanout msgs=1000000 size=256 duration_ms=4533 msgs_per_sec=220599 mb_per_sec=53.86
```

## Raw output — pinned protocol, client 5–9 / server 15–19 (5 rounds)

```
bench name=pubsub msgs=1000000 size=256 duration_ms=1639 msgs_per_sec=610285 mb_per_sec=149.00
bench name=latency samples=20000 p50_us=1103 p99_us=3092 p999_us=3399 max_us=3591
bench name=fanout msgs=1000000 size=256 duration_ms=1284 msgs_per_sec=778864 mb_per_sec=190.15
bench name=pubsub msgs=1000000 size=256 duration_ms=1801 msgs_per_sec=555300 mb_per_sec=135.57
bench name=latency samples=20000 p50_us=1310 p99_us=2846 p999_us=3341 max_us=3606
bench name=fanout msgs=1000000 size=256 duration_ms=1277 msgs_per_sec=783279 mb_per_sec=191.23
bench name=pubsub msgs=1000000 size=256 duration_ms=1586 msgs_per_sec=630665 mb_per_sec=153.97
bench name=latency samples=20000 p50_us=1320 p99_us=2725 p999_us=3177 max_us=4279
bench name=fanout msgs=1000000 size=256 duration_ms=660 msgs_per_sec=1514084 mb_per_sec=369.65
bench name=pubsub msgs=1000000 size=256 duration_ms=1791 msgs_per_sec=558471 mb_per_sec=136.35
bench name=latency samples=20000 p50_us=2023 p99_us=3014 p999_us=3297 max_us=3952
bench name=fanout msgs=1000000 size=256 duration_ms=1201 msgs_per_sec=832595 mb_per_sec=203.27
bench name=pubsub msgs=1000000 size=256 duration_ms=1829 msgs_per_sec=546658 mb_per_sec=133.46
bench name=latency samples=20000 p50_us=2044 p99_us=3030 p999_us=3273 max_us=3482
bench name=fanout msgs=1000000 size=256 duration_ms=1190 msgs_per_sec=840003 mb_per_sec=205.08
```

Byte-for-byte logs, including the `# server:` URLs and per-round load lines
stripped above: `benchmarks/raw/2026-09-20-canonical-A.txt` (rounds 1–7),
`-B.txt` (8–12), `-C.txt` (13–16), `2026-09-20-pinned.txt`.

## Previous host — Apple M1 Max, 10 cores, 32 GB, macOS 26.6.2

Recorded 2026-09-20 against `nats-server` v2.15.0, async-nats 0.50, 3 runs per
bench, same harness, same commands. Historical only.

| Bench | Metric | M1 Max (median of 3) | This host, default (median of 16) | Ratio | This host, best variant |
|---|---|---|---|---|---|
| pubsub | msgs/sec | 2,349,283 | 225,932 | 0.10× | 757,541 (lucky round) / 558,471 (P) |
| pubsub | MB/sec | 573.56 | 55.16 | 0.10× | 136.35 (P), 118.03 (C) |
| latency | p50 | 83 µs | 662 µs | 8.0× slower | **43–73 µs (C)** |
| latency | p99 | 127 µs | 2,814 µs | 22.2× slower | **52–86 µs (C)** |
| latency | p999 | 148 µs | 3,514 µs | 23.7× slower | 101–395 µs (C), 152–155 µs (S) |
| fanout | deliveries/sec | 2,105,592 | 221,420 | 0.11× | ~1,000,000 (C) / 832,595 (P) |
| fanout | MB/sec | 514.06 | 54.06 | 0.11× | 245.32 (C), 203.27 (P) |

The gap is the host, not the server: an ARM big.LITTLE part with a 433 µs
deep-idle exit, 20 cores for a 2-thread workload, and a co-resident LLM
inference process. Hold placement constant (variant C) and the same Go binary
**beats** the M1 on latency while sitting at roughly 20–50 % of its throughput
— which is where a single-threaded NATS server on 3.9 GHz ARM against an
in-process client belongs. The 757,541 msgs/sec entry is one confirmation round
taken at 22:39, straight after variant W had the whole box hot; it is not part
of the 16-run table and stands as an upper bound this protocol does not reach
when placement is left to the scheduler.

## Reading the numbers

- `pubsub` counts publications; `latency` is a serialised request-reply round
  trip (one exchange in flight), so it includes two client round trips through
  the loopback stack plus the echo publish.
- `fanout` reports **total deliveries** (`MSGS * SUBSCRIBERS`), so its msgs/sec
  is aggregate delivery throughput, not publish rate. Divide by SUBSCRIBERS to
  compare with pubsub.
- Percentiles are nearest-rank over the samples; each bench spawns its own
  server on a random loopback port, so no state is shared between runs.
- Both throughput benches are bound by a handful of client tasks on one tokio
  runtime feeding a single server event loop, not by the machine: 20 cores and
  127 GB buy nothing here, which is why affinity moves the answer more than any
  other knob in this file.
- They also hit the same wall: ~4.4–4.5 s and ~54 MB/sec of aggregate delivery,
  even though pubsub moves 256 MB through 1 subscription and fanout moves the
  same 256 MB through 10. The saturated resource is the shared delivery path,
  not the publisher's loop — fanout publishes at only ~22,142 msgs/sec
  against pubsub's 225,932.
