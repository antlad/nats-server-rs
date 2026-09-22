# The allocation census

Regenerate with:

```sh
cargo build --release --features allocstats
BIN_RS=$PWD/target/release/nats-server-rs BIN_GO=/tmp/nats-instr/nats-server-instr \
  MSGS=2000000 SIZE=256 PIN=7 bash tools/census.sh
```

Both numbers come out of the processes themselves — our `allocstats` counters and
the reference's `runtime.MemStats`, via `specs/tools/go-instrument/` — never from
reading the code and arguing. `allocs` is allocations, `bytes` is bytes taken from
the allocator, both per unit of the row's work.

## 2026-09-22, after Task 23 (the read buffer) and Task 25 (the frame arena)

| Unit | reference | ours, start of Part 3 | ours, now |
|---|---:|---:|---:|
| published message, nobody subscribed | 0.751 allocs, 3.5 B | 0.03 allocs, 130 B | **0.010 allocs, 1.6 B** |
| delivered message (1 pub → 1 sub, 256 B) | 1.059 allocs, 256 B | 5.02 allocs, 499 B | **0.111 allocs, 333 B** |
| connection, opened and closed | 73.2 allocs, ~13 KiB | not measurable | not measurable (Task 27) |

Read the middle column as a warning, not as a baseline: 5.02 was four allocations
and a half of *plumbing* — a matched-subscription `Vec`, a frame head, a payload
`Bytes`, an `IoSlice` `Vec`, a `Sleep` — and three of them had already been
addressed by changes that were in the tree and not working. `frame_chunk=0` in the
per-site counters is what gave it away: the arena that Part 3 was written to build
had been built, and never once handed out a chunk, because
`BytesMut`'s `BufMut::remaining_mut` answers `usize::MAX - len` rather than
"capacity minus length" (`specs/perf-notes.md`, change 13).

Where the 0.111 on the delivery row comes from, per site: `frame_chunk` 0.034 (one
8 KiB chunk per 29 frames), the chunk's `Vec`→`Arc` promotion 0.035 (bytes takes a
`Shared` and rebuilds the vector the first time a piece is split off a
`BytesMut`), tokio's mpsc block 0.030 (one per 32 queued frames), and the
straddling frame's hoisted copies ≈0.01. None of them is per message.

The bytes column is the same story: 8 KiB per 29 frames of 256 B is 315 B a message
of allocator *traffic*, in buffers that are handed out and freed rather than
retained. Retained memory is the other measurement, and it is `memprobe.py`:
**1.00× `max_pending`, against the reference's 1.98×** — the copy-on-deliver rule
plus per-connection chunks now costs half what the reference costs for a subscriber
that stops reading.

## Where they go, per site

With the feature on, `NATS_RS_STATS_FILE` gets one `alloctags` line per dump. All
of these are per *run*, not per message; the sites are tagged in the code that
reaches for memory, so a new allocation shows up as a new row rather than as a
mystery in the totals.

| Site | Tagged at |
|---|---|
| `read_buf` | `client.rs::read_loop` — the reader's buffer, grown or reserved |
| `events` | `client.rs::read_loop` — the parsed-operation batch, when it grows |
| `frame_chunk` | `arena.rs::tail` — a new chunk of frame bytes, or an oversized frame's own buffer |
| `subscribe` | `proto.rs::subscribe` — a filter's subject/queue/sid copies |
| `control_line` | `client.rs::Frame::err` — the text of an `-ERR` |
| `connection` | `client.rs::spawn` — the `Conn` and everything hanging off it |
| `routing` | `routing.rs::resolve` — the interest cache's own scratch |
