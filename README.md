# nats-server-rs

A NATS **Core** server in Rust, validated by running the same integration suite
and the same benchmarks against it and against the reference Go binary. The
server is the specification's implementation; the Go binary is the
specification. Where they disagree, the Rust server is wrong until the
difference is written down and measured
([`specs/protocol-contract.md`](specs/protocol-contract.md),
[`specs/parity-log.md`](specs/parity-log.md)).

```
crates/harness    nats-test-harness: spawn / readiness / teardown, config files
crates/core-it    integration tests (wire protocol, verbs, headers, lifecycle, client)
crates/bench      pubsub / latency / fanout / floodpub benchmark binaries
crates/server     the Rust server: bin nats-server-rs + lib for unit tests
specs             measured contract, Go-suite audit, parity log, perf notes
benchmarks        recorded baselines (Go and Rust, same protocol)
```

## Build

```sh
cargo build --release          # → target/release/nats-server-rs
```

## Run

```sh
target/release/nats-server-rs -a 127.0.0.1 -p 4222
target/release/nats-server-rs -p -1 --ports_file_dir /tmp/ports   # ephemeral, announce it
target/release/nats-server-rs -c server.conf                      # config file
```

Flags, in the reference's shape: `-a/--addr/--net`, `-p/--port` (`-1` = random,
`0` = unset → 4222), `-n/--name/--server_name`, `-c/--config`, `-t` (validate
config and exit), `--ports_file_dir`, `-v/--version`, `-h/--help`,
`-D/--debug`, `-V/--trace`. An unknown flag is an error. Options that are not
flags live in the config file, as they do in the reference: `host`, `port`,
`server_name`, `max_payload`, `max_pending`, `max_control_line`,
`max_connections`, `ping_interval`, `write_deadline`, `debug`, `trace`. Anything
else in the file is rejected, loudly.

## Integration tests

```sh
export NATS_SERVER_BIN=/home/vlad/apps/nats-server           # the reference
export NATS_SERVER_BIN=$PWD/target/release/nats-server-rs    # ours
cargo test --workspace --release
```

`NATS_SERVER_BIN` is required — there is no PATH fallback, so every run says
which binary it validated. Each test spawns its own server with
`-a 127.0.0.1 -p -1 --ports_file_dir <tmpdir>`, discovers the port from the
ports file (race-free: the file only appears once the listener accepts), and
kills the process on drop. Tests that need options the flags do not carry use
`Server::start_with_config`.

The suite is binary-agnostic on purpose: 96 tests drive a server from outside and
pass unchanged against both binaries; the 33 in-process tests
(`crates/server/tests/`) cover what a socket cannot see — the parser's arity
table, the subject grammar, the INFO/CONNECT defaults, and the two ordering
promises. 135 in total, plus a 206-case differential corpus
(`specs/tools/difffuzz.py`) that diffs the two binaries directly.

- `wire.rs` — raw TCP bytes: INFO fields and key set, CONNECT/PING/PONG, `+OK`,
  PUB/SUB/UNSUB framing, `max_payload` (including a configured one).
- `verbs.rs` — the error taxonomy per verb: class A (`-ERR` + close), class B
  (`-ERR`, connection stays), class C (close, no bytes), plus the whole corpus.
- `headers.rs` — HPUB/HMSG byte-for-byte, the capability gate, header bytes in
  `max_payload`, the 503 no-responder frame verbatim.
- `lifecycle.rs` — echo/verbose defaults and their second-CONNECT rule,
  auto-unsub limits, keepalive and stale close, backpressure, connection churn.
- `client.rs` — behaviour through async-nats: wildcards, queue groups,
  request-reply, no-responders, fan-out, flush ordering.
- `harness/tests/smoke.rs` — the harness itself: readiness, teardown, an
  unsupported flag never becoming a silent server.

## Benchmarks

```sh
cargo run --release -p nats-bench --bin pubsub     # MSGS, SIZE
cargo run --release -p nats-bench --bin latency    # ITERS, WARMUP
cargo run --release -p nats-bench --bin fanout     # MSGS, SIZE, SUBSCRIBERS
cargo run --release -p nats-bench --bin floodpub   # ADDR, MSGS, SIZE: the drain rate
```

`floodpub` is the odd one: one connection publishing with **no subscribers**,
which is a completely different server path from `pubsub` — nothing is written
back, so it measures read, parse and the interest test and nothing else. It pre-
encodes the frames and stops the clock only when a trailing `PING`/`PONG` proves
the server consumed every byte, so the number is the server's drain rate and not
the client's own cost. Compare it against the reference, paired inside each round:

```sh
MSGS=20000000 ROUNDS=4 PIN=7 CLIENT_PIN=3 ./benchmarks/run_pubonly.sh      # CPU-bound
NATS_CLI=nats CLIENT=nats MSGS=10000000 ROUNDS=3 ./benchmarks/run_pubonly.sh  # what `nats bench pub` sees
CLIENT=sub NATS_CLI=nats ./benchmarks/run_pubonly.sh                      # the delivery path
```

Each prints one stable `bench name=... key=value` line. For a comparison that
means anything, run both binaries **interleaved in one session**:

```sh
GO_BIN=/home/vlad/apps/nats-server RUST_BIN=$PWD/target/release/nats-server-rs \
ROUNDS=16 OUT=benchmarks/raw/$(date +%F)-paired.txt ./benchmarks/run_baseline.sh
```

For the latency gate specifically, pin both sides (client on one P-core, server
on the neighbour) instead — that is variant C, and on this host it is the only
latency run worth quoting:

```sh
ROUNDS=12 OUT=benchmarks/raw/$(date +%F)-variant-c.txt ./benchmarks/run_variant_c.sh
```

Recorded numbers: [`benchmarks/baseline-go.md`](benchmarks/baseline-go.md) (the
reference on this host) and
[`benchmarks/baseline-rust.md`](benchmarks/baseline-rust.md) (ours, same
protocol, with the Go control rounds from the same session). **Read the
"Placement sensitivity" section of the Go file first**: on this machine an
unpinned latency number measures the scheduler, not the server.

Where the time actually goes is in
[`specs/perf-notes.md`](specs/perf-notes.md) — CPU seconds, syscalls and
allocations per message, measured against the reference (`perf` cannot run on
this host: `kernel.perf_event_paranoid = 4`, no `CAP_PERFMON`). Those counters
need a server that outlives the bench run, which is what `NATS_BENCH_URL` is
for: set it and the bench drives an already-running server instead of spawning
one. Leave it unset — every test and every recorded baseline does.

Three probes go with it, because "where does the time go" needs a smaller
question to ask:

```sh
cargo run --release -p nats-server-rs --example parsebench          # the parser alone, no sockets
cargo run --release -p nats-server-rs --features allocstats --example parsebench   # …and allocations per message
python3 specs/tools/memprobe.py target/release/nats-server-rs       # what a wedged subscriber costs the server in RAM
```

## Status and limitations

Implemented: the Core client protocol — CONNECT (verbose, pedantic, echo,
headers, no_responders), SUB with queue groups, UNSUB with auto-unsub, PUB/HPUB
and MSG/HMSG delivery, wildcard matching, PING/PONG with server-initiated
keepalive and stale close, the `+OK`/`-ERR` semantics and the three error
classes, per-connection outbound buffers bounded in bytes with the reference's
75 % stall and 100 % slow-consumer thresholds, the ports file, a config-file
subset, and SIGTERM/SIGINT shutdown.

Not implemented, deliberately (each needs its own plan):

- **Authorization** of any kind — no token, user, nkey or JWT; INFO says nothing
  about it (`auth_required` is absent, not false).
- **TLS**, **JetStream**, **clustering/routes/gateways/leafnodes**, the **HTTP
  monitoring endpoints**, **accounts**.
- `PSUB`/`SSUB`/`A_SUB`/`RMSG`/`MMSG` — the reference build has no PSUB at all
  (`SUB` takes wildcards), so an unknown-protocol close *is* parity.
- Most of the reference's config surface: we parse the keys listed above and
  reject the rest rather than accept and ignore them.
- `server_id` is an opaque 56-character base32 token, the same shape as the
  reference's, but not a verifiable nkey — a server with no auth has no key pair,
  and we do not emit a fake `xkey` either.

Verified behaviour lives in [`specs/protocol-contract.md`](specs/protocol-contract.md);
the Go-suite mapping in [`specs/go-audit.md`](specs/go-audit.md); the differences
we found and fixed, with the test that caught each one, in
[`specs/parity-log.md`](specs/parity-log.md).
