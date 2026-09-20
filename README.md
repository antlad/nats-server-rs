# nats-server-rs

Rust implementation of a NATS server, validated against the reference
nats-server binary by running the real executable.

Everything here is binary-agnostic: the same tests and benchmarks run against
the Go reference server today and against our Rust server when it exists.

## Integration tests

```sh
export NATS_SERVER_BIN=/path/to/nats-server   # binary under test (required)
cargo test --workspace
```

`NATS_SERVER_BIN` must point at the executable under test; there is no PATH
fallback, so a run always says which binary was validated. Each test spawns its
own server with `-a 127.0.0.1 -p -1 --ports_file_dir <tmpdir>`, discovers the
port from the ports file (race-free), and kills the process on drop.

Tests cover NATS Core:

- `crates/core-it/tests/wire.rs` — raw TCP protocol, exact bytes: `INFO`
  fields, `CONNECT`/`PING`/`PONG`, `+OK` in verbose mode, `PUB`/`SUB`/`UNSUB`
  and `MSG` framing, `-ERR` on an unknown verb or a bad subject, `max_payload`
  enforcement, and connection teardown semantics.
- `crates/core-it/tests/client.rs` — behaviour through `async-nats`:
  wildcards (`*`, `>`), queue groups, request-reply, no-responders, fan-out,
  keepalive/ping liveness, flush ordering.
- `crates/harness/tests/smoke.rs` — harness self-test (start, connect, stop).

## Benchmarks

```sh
cargo run --release -p nats-bench --bin pubsub    # MSGS, SIZE
cargo run --release -p nats-bench --bin latency   # ITERS, WARMUP
cargo run --release -p nats-bench --bin fanout    # MSGS, SIZE, SUBSCRIBERS
```

Each prints one stable `bench name=... key=value` line. Baselines for the Go
v2.15.0 reference binary are in [`benchmarks/baseline-go.md`](benchmarks/baseline-go.md);
Rust-server runs are recorded the same way and diffed against them.

## Layout

```
crates/harness    nats-test-harness: spawn / readiness / teardown
crates/core-it    integration tests (wire protocol + client behaviour)
crates/bench      pubsub / latency / fanout benchmark binaries
benchmarks        recorded baselines
```

## Status

- [x] Test harness + integration suite + benchmarks (this repo, validated against the Go binary)
- [ ] Rust server implementing the contract the wire tests enforce: `-a`, `-p -1`,
      `--ports_file_dir`, `INFO` with `proto:1`/`max_payload`/`port`,
      `+OK`/`-ERR`/`PONG`/`MSG` semantics
- [ ] JetStream, clustering, routes, leafnodes, TLS, auth (separate plans)
