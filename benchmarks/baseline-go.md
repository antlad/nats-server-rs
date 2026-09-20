# Baseline: reference Go nats-server

Results recorded so Rust-server runs can be diffed against them. Same harness,
same binaries, same client — only `$NATS_SERVER_BIN` changes.

| | |
|---|---|
| Date | 2026-09-20 |
| Server under test | `nats-server` v2.15.0 (Go reference binary) |
| Machine | Apple M1 Max, 10 cores, 32 GB, macOS 26.6.2 |
| Client | async-nats 0.50, in-process, loopback only |
| Build | `cargo build --release` |
| Runs | 3 consecutive per bench, default parameters |

Commands:

```sh
export NATS_SERVER_BIN=/path/to/nats-server
cargo build --release
for i in 1 2 3; do ./target/release/pubsub; done
for i in 1 2 3; do ./target/release/latency; done
for i in 1 2 3; do ./target/release/fanout; done
```

## Raw output

```
bench name=pubsub msgs=1000000 size=256 duration_ms=433 msgs_per_sec=2307345 mb_per_sec=563.32
bench name=pubsub msgs=1000000 size=256 duration_ms=426 msgs_per_sec=2349283 mb_per_sec=573.56
bench name=pubsub msgs=1000000 size=256 duration_ms=425 msgs_per_sec=2353994 mb_per_sec=574.71

bench name=latency samples=20000 p50_us=83 p99_us=127 p999_us=154 max_us=257
bench name=latency samples=20000 p50_us=83 p99_us=128 p999_us=148 max_us=328
bench name=latency samples=20000 p50_us=82 p99_us=126 p999_us=147 max_us=276

bench name=fanout msgs=1000000 size=256 duration_ms=512 msgs_per_sec=1953242 mb_per_sec=476.87
bench name=fanout msgs=1000000 size=256 duration_ms=475 msgs_per_sec=2105592 mb_per_sec=514.06
bench name=fanout msgs=1000000 size=256 duration_ms=558 msgs_per_sec=1790994 mb_per_sec=437.25
```

## Summary (median of 3)

| Bench | Parameters | Metric | Baseline |
|---|---|---|---|
| pubsub | MSGS=1_000_000 SIZE=256 | msgs/sec | **2 349 283** |
| pubsub | MSGS=1_000_000 SIZE=256 | MB/sec | 573.56 |
| pubsub | MSGS=1_000_000 SIZE=256 | wall | 426 ms |
| latency | ITERS=20_000 WARMUP=1_000 | p50 | **83 µs** |
| latency | ITERS=20_000 WARMUP=1_000 | p99 | 127 µs |
| latency | ITERS=20_000 WARMUP=1_000 | p999 | 148 µs |
| latency | ITERS=20_000 WARMUP=1_000 | max | 276 µs |
| fanout | MSGS=100_000 SIZE=256 SUBSCRIBERS=10 | deliveries/sec | **2 105 592** |
| fanout | MSGS=100_000 SIZE=256 SUBSCRIBERS=10 | MB/sec | 514.06 |
| fanout | MSGS=100_000 SIZE=256 SUBSCRIBERS=10 | wall | 475 ms |

## Reading the numbers

- `pubsub` counts publications; `latency` is a serialised request-reply round
  trip (one exchange in flight), so it includes two client round trips through
  the loopback stack plus the echo publish.
- `fanout` reports **total deliveries** (`MSGS * SUBSCRIBERS`), so its
  msgs/sec is aggregate delivery throughput, not publish rate. Divide by
  SUBSCRIBERS to compare with pubsub.
- Percentiles are nearest-rank over the samples; each bench spawns its own
  server on a random loopback port, so no state is shared between runs.
- Anything within ~10% of these numbers is noise on this machine. Treat a
  regression larger than that (especially latency p99) as something to explain.
