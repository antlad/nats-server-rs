# Performance notes

One change at a time, each followed by a paired Go/Rust run, and the ordering
proof re-run after every step (Task 20 Step 3). Numbers are the paired protocol
of [`benchmarks/baseline-rust.md`](../benchmarks/baseline-rust.md).

## How this is measured on this host (there is no `perf`)

`perf` cannot run here: `kernel.perf_event_paranoid = 4`, no `CAP_PERFMON`, no
passwordless sudo. So the attribution below comes from four counters that don't
need a profiler, each taken the same way against both binaries. Two more of them
are instruments rather than readings — `parsebench` and `memprobe` — added because
"the server is slow" is not a question the first four can answer on their own:

| What | How | Works on Go? |
|---|---|---|
| Server CPU seconds | `/proc/<pid>/task/*/stat` `utime+stime` summed, before/after a bench run, divided by messages — the one column that does not move with the machine's mood | ✅ |
| Parse cost alone | `cargo run --release -p nats-server-rs --example parsebench`: the same `Parser`, a 64 KiB block of pre-encoded `PUB` frames, no sockets | ours only, but the comparison holds: it is the same state machine `parser.go` runs, so what it shows is work, not luck |
| Drain rate, client excluded | `crates/bench/src/bin/floodpub.rs` — pre-encoded frames, one blocking writer, the clock stopped by the server's `PONG`. Driven paired by `./benchmarks/run_pubonly.sh` and `run_three.sh` | ✅ |
| Memory a wedged subscriber costs | `python3 specs/tools/memprobe.py <bin> 3000000` — one subscriber that stops reading, one publisher, peak `VmRSS` | ✅ |
| Syscalls per message | `strace -f -c` launched as the server's parent (`ptrace_scope` blocks attaching to an already-running process) | ✅ |
| Resident set | `/proc/<pid>/status` `VmRSS` sampled at 200 ms | ✅ |
| Allocations per message | `--features allocstats`: a counting `GlobalAlloc` that dumps at start and shutdown to `$NATS_RS_STATS_FILE` | ❌ — the reference exposes no pprof endpoint on its monitoring port (`/debug/pprof/heap` is 404, with and without `NATS_MONITOR_PPROF=true`), so our absolute count is recorded without a Go counterpart |

All of this needs the server to outlive the bench that drives it, which is what
the measurement-only `NATS_BENCH_URL` knob in `crates/bench` is for.

## The picture before any change (2026-09-21, 1 M-message `pubsub`, 256 B payloads)

| Counter | Rust | Go | Ratio |
|---|---:|---:|---:|
| server CPU per message | **7.47 µs** (paired three-arm; single samples read 6.3–12.0) | 2.44 µs | 3.1× |
| syscalls per message | 0.191 | 0.046 | 4.2× |
| `writev` per message | 0.0455 — **one write per 22 messages** | 0.0039 — one write per 259 messages | 11.8× |
| reads per message | 0.0134 (1 per 75) | 0.0049 (1 per 205) | 2.7× |
| `futex` per message | 0.093 (1 per 11) | 0.014 (1 per 74) | 6.8× |
| `mprotect` per message | 0.034 | 0 | — |
| allocations per message | 11.15 (1 206 B, i.e. 4.7× the payload); re-measured on the final build at 11.08 and 1 104 B | not obtainable | — |
| threads during the run | 21 spawned, 2–4 hot | 14, spread thin over ~1 core | — |

**What that says.** The gap is not "Rust is slower than Go at the same work":
one vectored write per 22 messages, against the reference's one per 259, is the
same amount of bytes pushed through 12× the syscalls and 7× the thread wake-ups,
and the wake-ups are what the extra CPU is spent on. The `mprotect` traffic and
the 1 206 bytes per message say the allocator is also being asked to do work the
reference does not ask of its own — but that is a secondary cost next to the
write count, and it is the one item here that has no Go number to compare to.

The earlier hypothesis list ranked "one `Vec<Bytes>` per frame" first and the
batching rule fifth. The measurement reverses that: the coalescing cap was the
mechanism, and the frame allocation is a per-message constant that matters less.

## The publish-only path (2026-09-22): why Rust lost to Go when nobody is listening

Reported from macOS with the CLI's own bench — `nats bench pub test`, one
publisher, **no subscribers** — 2.23 M msgs/s on the reference against 1.67 M on
ours (and 2.28 M against 1.85 M on the subscribe run). That is a path the `pubsub`
bench never walks: with nobody interested, the server reads, parses, tests for
interest, and writes *nothing at all*. No frame is built, no outbox is touched, no
writer wakes up. Everything the two binaries differ by lives in the read loop, the
parser and the routing decision.

### Measuring it without the client in the way

`nats bench`'s numbers mix the Go client's own CPU into the comparison — measured
here, the client burns 1.0–1.4 cores while the server burns 0.9, so the reported
rate is whichever of the two is slower that second, and it moves ±60 % between
rounds of the same binary. `crates/bench/src/bin/floodpub.rs` takes the client out:
one blocking thread, frames pre-encoded into a 256 KiB buffer, `write` until the
kernel takes them, and the clock stopped only when a trailing `PING`/`PONG` proves
the server consumed every byte — without that gate the loopback's 32 MB of
auto-tuned receive slack lets the client report a rate the server never had to keep
up with. `./benchmarks/run_pubonly.sh` drives it against both binaries inside the
same round, server pinned to one core and the driver to another, and reports CPU
seconds per message from `/proc/<pid>/task/*/stat`.

aarch64 (Cortex-X925, server on core 7, driver on core 3), 20 M messages of 128 B:

| | reference | ours, before | ours, now |
|---|---:|---:|---:|
| msgs/sec | 3.36 M | 1.25 M | **3.10 M** |
| CPU µs per message | 0.297 | 0.796 | **0.320** |
| ours / reference | — | **2.67×** | **1.08×** |
| cores busy | 1.00 | 1.00 | 1.00 |
| allocations per message | not measurable | 5.07 | **0.03** |
| syscalls: `recvfrom` per message | 0.0029 | 0.0091 | 0.0022 |
| parse alone, ns per message | — | 295 | 190 |

Raw: `benchmarks/raw/2026-09-22-pubonly-baseline.txt` (before) and
`2026-09-22-pubonly-6round.txt` (after, four-arm medians below).

### One caveat the paired protocol caught, and it is not about either server

Rounds 1–3 of that file read 0.297 / 0.320 above. Rounds 4–6 read 0.166 for the
reference and 0.177 for ours — **both binaries about 1.7× faster**, in the same
seconds, with the ratio between them unchanged (1.06×). The machine changes state
under a sustained load like this (deep cpuidle exit cost, and the loopback's
receive-buffer autotuning, which decides how often the reader parks at all); after
a few minutes of it, both servers run work-only instead of work-plus-wakeups.
This is the same placement sensitivity `benchmarks/baseline-go.md` warns about for
latency, showing up as a throughput level. It is why every number quoted here is a
**paired** one: an unpaired "0.177 µs/msg" from the fifth round of a warm session
would be a third of the truth, and would not survive anyone reproducing it from a
cold machine.

Both servers were *exactly one core wide* before and after, so this was never a
question of parallelism, of tokio against the Go runtime, or of the kernel's
scheduler. It was 2.7× the work per message, and the work was in seven places:

| What the reference does | What we did | Cost, per message |
|---|---|---:|
| `parser.go` jumps the index over the payload (`i = c.as + c.pa.size - LEN_CR_LF`) and slices it out of the read buffer at `MSG_END_N`; `processPub` points `c.pa.subject` into the buffer too. **Zero copies, zero allocations.** | freeze the control line, copy the subject, gather the body's pieces into a `Vec`, allocate a `BytesMut`, memcpy the payload in. | 4 allocs + 2 memcpys ≈ 130 ns |
| `splitArg` writes into a stack array, deliberately: *"Unroll splitArgs to avoid runtime/heap issues"* (`go:client.go:2973`) | `split_args` returned a `Vec<&[u8]>` | 1 alloc ≈ 30 ns |
| `OP_START` switches on the first byte, so `PUB` is two branches deep | eight prefix/token comparisons before `PUB` matched | ≈ 15 ns |
| the CONNECT options are plain fields, read under the one lock the client already holds | `Mutex<Options>`, read three times per publish, plus a `Mutex<Weak<Conn>>` upgrade for an `Arc` the caller was already holding | 4 lock pairs ≈ 90 ns |
| `acc.sl` is hashed with the runtime's seeded `memhash`, ~5 ns for a short subject | `SipHash-1-3` ≈ 18 ns | ≈ 13 ns |
| "Check for no interest, short circuit if so" (`go:client.go:4506`), after a per-client L1 result cache (`c.in.results`, keyed by subject, invalidated by the sublist `genid`) | a process-wide `Mutex<Registry>`, a hash probe and an RNG draw, every message, whether or not anyone is listening | ≈ 50 ns |
| the read buffer starts at 512 B and doubles to 64 KiB while reads come back full (`go:client.go:110`, `:1675`) | a fixed 16 KiB | 3× the syscalls |

Changes [3](#3--the-parser-stopped-copying-and-stopped-allocating-keeper) through
[7](#7--the-read-buffer-grows-like-the-references-keeper) are those seven, one at a
time, each with a paired run and the ordering proof after it. The steps, in the
order they were measured, are cumulative — each is the build at that point,
against the reference in the same round, cold-session regime:

| After | CPU µs per message | vs the reference |
|---|---:|---:|
| (nothing) | 0.796 | 2.67× |
| change 3 — parser views, no arg `Vec`, first-byte dispatch | 0.530 | 1.77× |
| changes 4, 5 — options atomic word, no `self_ref`, seeded subject hash | 0.384 | 1.28× |
| changes 6, 7, 9 — no-interest short circuit, borrowed `Msg`, 64 KiB reads, reused batch | 0.320 | 1.08× |

All three arms in one session (`./benchmarks/run_three.sh MODE=pubonly`, 20 M
messages, server on core 7, driver on core 3, three rounds — µs of server CPU per
message). The point of the third column is that the *old* build did not get faster
when the machine warmed up, and the new one does, exactly like the reference: the
work per message stopped being the story.

| Round | old build | reference | new build | new / reference |
|---|---:|---:|---:|---:|
| 1 (cold) | 0.791 | 0.297 | 0.317 | 1.07× |
| 2 | 0.671 | 0.174 | 0.183 | 1.05× |
| 3 | 0.444 | 0.171 | 0.183 | 1.07× |

`benchmarks/raw/2026-09-22-pubonly-threearm.txt`.

**What is left is 23 ns, and it has a name.** The reference answers a publish that
nobody wants from a per-client cache keyed by subject: one map probe for the
thousandth message to `test`, because the interest list never changed. We match —
and hash — every message. That is item 5 in "where the gap stands" below. It is
the only structural difference left on this path, and at 0.320 µs against 0.297 it
is no longer the interesting part of the comparison: **the publish-only server is
no longer the reason a publish benchmark looks slower in Rust than in Go.** What
was, was five allocations, four mutexes, a SipHash and a 16 KiB buffer, and every
one of them was ours to fix.

## Changes

Each row: one diff, then a paired run in the same session (Go control beside it),
then the full suite plus the ordering proof.

### 1 — coalesce by bytes, not by 64 buffers (keeper)

`MAX_BATCH = 64` parts, and an `MSG` delivery is 3 parts, so the writer stopped
draining at ~21 messages. Now: drain until 256 KiB or 1 000 buffers (`IOV_MAX`
minus headroom — above it `writev` returns `EINVAL`).

Syscall counters (200 k-message `pubsub`, `strace -f -c`, one run per build):

| Counter | Before | After | Go |
|---|---:|---:|---:|
| `writev` per message | 0.0455 (1 per 22) | **0.0037 (1 per 267)** | 0.0039 (1 per 259) |
| `futex` per message | 0.093 | **0.0042** | 0.014 |
| `mprotect` per message | 0.034 | 0.0098 | 0 |
| reads per message | 0.0134 (1 per 75) | 0.0134 | 0.0049 (1 per 205) |
| total syscalls per message | 0.191 | **0.036** | 0.046 |

The delta that decides it, though, is three-arm and paired: previous build, this
build and the Go reference, all three driven inside the same round, 4 rounds,
server CPU seconds from `/proc/<pid>/stat`. Machine state rose through the run
(loadavg 2.17 → 3.20), which is exactly why all three arms share each round.

| Arm | pubsub µs CPU/msg | pubsub msgs/s | pubsub cores | fanout µs/delivery | fanout deliveries/s | fanout cores |
|---|---:|---:|---:|---:|---:|---:|
| old (64 buffers) | 7.47 [4.75 – 9.65] | 235,432 | 2 | 14.22 [8.92 – 16.69] | 266,035 | 3 |
| **new** | **3.42** [1.70 – 4.84] | **418,616** | 1 | **9.82** [6.90 – 15.06] | **372,694** | 3 |
| go (control) | 2.44 [1.19 – 4.46] | 417,172 | 1 | 26.91 [26.16 – 27.43] | 244,716 | 7 |
| new/old | **0.46×** | **1.78×** | | **0.69×** | 1.40× | |
| new/go | 1.40× | **1.00×** | | **0.37×** | **1.52×** | 0.53× |

So the change is: **half the CPU per message, and on `pubsub` the same throughput
as the reference when both are measured in the same seconds** (418,616 against
417,172 — 0.3 % apart). The remaining 1.40× CPU-per-message is what still has to
be explained; see "Where the pubsub gap stands now".

`fanout` improves too (0.69× the CPU per delivery, 1.40× the throughput of the
old build) and is now 1.52× the reference's deliveries/s at 0.37× its CPU per
delivery, on 3 cores against its 7.

A 7-round session of the same three arms on the throughput protocol alone
(medians: old 171 014, new 195 876, go 260 996 pubsub) said 1.145× new/old and
0.750× new/go. It is the same direction with less resolution: throughput medians
carry the machine's mood, CPU seconds per message do not.

### 2 — a parked publisher must wake when its subscriber dies (keeper, correctness)

Found *because* of the paired runs: a `pubsub` round in ~50 never finished. Every
server thread idle, both sockets empty, no `-ERR`, no close — the publisher's
read task was parked on the stall gate of a subscriber that had already been
closed, waiting for room in a queue that nothing would ever drain again. Two
holes, both in `Outbox`:

* `room()` read the byte counter, and the close path dropped the queued frames
  without zeroing it — so "is there room" was permanently `false` for a corpse.
* `wait_for_space()` looped on `room()` alone, so the one `notify_waiters()` the
  close sent woke the task into another sleep.

Fix: an `gone` flag set with the counter zeroed at close, honoured by `room()`
(and by `drained()`, whose `usize` subtraction could wrap on a double drain).
Regression test:
`core-it/tests/lifecycle.rs::a_parked_publisher_wakes_when_its_subscriber_is_dropped`
— fails against the pre-fix build at 10.04 s, passes against the reference in
0.40 s and against ours in 2.09 s. The 0.4-vs-2.1 split is change 1's other
legacy: the reference drops the wedged subscriber at its 100 % pending mark,
while we hold the publisher at 75 % and let the *write deadline* do the dropping
(contract §7). Same promises, different trigger.

Perf effect: none measurable — one relaxed load on a path that already took a
mutex. It is in this file because it changed the throughput samples' *reliability*,
which is the thing the medians were hiding.

### 3 — the parser stopped copying, and stopped allocating (keeper)

`PUB` used to cost four allocations a message: the argument list (`split_args`
returned a `Vec`), a copy of the subject, a `Vec` to hold the body's pieces, and a
fresh `BytesMut` the payload was memcpy'd into. Now the body is a *view* of the
read buffer (`Parts`), the subject and reply are views of the control line
(`Args::view`, ranges into the frozen line instead of copies), the argument list is
a stack array (`Args`, saturating at "too many"), and the verbs are dispatched on
their first byte the way `OP_START` does it instead of through eight prefix
comparisons.

| Counter | Before | After |
|---|---:|---:|
| allocations per message, publish-only | 5.07 | **0.03** |
| bytes allocated per message | 775 | 130 |
| parse-only ns per message (`examples/parsebench`) | 295 | 190 |
| pubonly CPU µs per message | 0.796 | 0.530 |

Views are also what makes the *no-interest* case free (change 6): skipping a
payload costs nothing when skipping means "do not take a reference".

### 4 — CONNECT's flags became one atomic word; the hot path stopped cloning `Arc`s through a mutex (keeper)

`Conn::opts()` took a `Mutex` and cloned. Three reads per published message
(`verbose`, then the publish arm's `opts`, then the no-responder check) plus a
`Mutex<Weak<Conn>>` upgrade to get an `Arc` the caller already held. The five flags
are now packed (`Options::pack`) into an `AtomicU64` — one writer, the connection's
own reader task, and many lock-free readers on the routing path — and `handle` uses
the `Arc<Conn>` it was given.

### 5 — a subject hash that costs what a subject costs (keeper)

`HashMap<Bytes, _>` with the default `RandomState` spends 15–20 ns of SipHash-1-3
on a four-byte subject, once per message, to defend against a collision attack
that this map has always been open to anyway (subjects are client-chosen). Now:
multiply-xor over 8-byte words, seeded once per process the same way
`RandomState` seeds, so the buckets are still not predictable from outside.
Equality — which is what defines the protocol's behaviour here — is untouched.

### 6 — "check for no interest, short circuit if so" (keeper)

The reference's own comment (`go:client.go:4506`). `route` now returns the moment
the match comes up empty: no `Msg` to build, no RNG draw, no second lock, no
payload to look at. The RNG is now consulted only when a queue group is actually
in the batch, and the registry lock is taken once per message instead of a second
time for expired subscriptions.

`Msg` became borrowed (`&'a [u8]` subject/reply, `&'a Bytes` body) because routing
only needs to *look*: the copy a delivery needs is taken once, in `route`, and only
when somebody is going to write those bytes. That keeps a slow subscriber from
pinning the publisher's 64 KiB read chunk — `specs/tools/memprobe.py` checks it,
and holds 1.95× `max_pending` in this build against the reference's 1.62×.

### 7 — the read buffer grows, like the reference's (keeper)

`startBufSize` 512 B doubling to `maxBufSize` 64 KiB whenever a read comes back
full, halving after `shortsToShrink` short ones (`go:client.go:110-112`,
`:1675-1685`), implemented in `read_loop` with the same rules. Syscalls per message
went from 0.0091 (1 per 110) toward the reference's 0.0029 (1 per 344); what it
buys directly is ~10 ns, and what it buys indirectly is that a 64 KiB read holds a
batch of ~450 events, which is what made change 9's `feed_into` worth doing at all.

The events batch itself is now the reader's, not the allocator's
(`Parser::feed_into`, the caller's `Vec` reused) — a 47 KiB `Vec<Event>` handed
back and re-fetched every read meant every batch was written into cold pages.

### 8 — Nagle was on (keeper, parity)

Nothing in the protocol tests can see this one. The reference never calls
`SetNoDelay` because it does not have to: Go's `net` sets `TCP_NODELAY` on every
connection it hands out. `strace -e trace=setsockopt` on the two servers says it
plainly — the reference: `setsockopt(fd, SOL_TCP, TCP_NODELAY, [1], 4)`; ours,
before: no such line at all. Accepted sockets now get `set_nodelay(true)` in
`net::accept_loop` before anything can be written to them.

### 9 — frame digits and frame parts (keeper, delivery path)

`build_frame` wrote its sizes through a `String` and the writer collected each
frame's buffers into a fresh `Vec<Bytes>` per frame — one allocation each, on the
delivery path only. Digits now go straight into the head buffer (`push_u64`) and
`Frame::write_into` moves its three buffers into the writer's own queue.

## The delivery path, same treatment (2026-09-22)

The subscribe half of the user's report (`nats bench sub`: 2.28 M against 1.85 M)
is a different path — one publisher, one subscriber, and the message has to leave
through a second connection. Same three-arm protocol, `pubsub` bench, 3 M messages
of 256 B, server pinned, client not (`benchmarks/raw/2026-09-22-pubsub-threearm.txt`):

| Round | old build | reference | new build | new / reference |
|---|---:|---:|---:|---:|
| 1 | 2.317 | 1.477 | 1.953 | 1.32× |
| 2 | 2.453 | 1.487 | 1.980 | 1.33× |
| 3 | 2.200 | 1.367 | 1.893 | 1.38× |

So the changes moved the delivery path too (0.85× the old build's CPU per
message), but it is **not** at parity: 1.3× the reference's CPU per delivered
message, against 1.07× on the publish path. Neither server is CPU-saturated in
this bench (0.4–0.8 cores) — the async-nats client is the limit on throughput, so
only the CPU column means anything.

What the delivery path still spends that the reference does not, with the counters
that say so:

* **5.02 allocations per delivered message** on the current build (`allocstats`),
  against 0.03 on the publish path. Three are per message — the `Vec` that `route`
  collects matches into, the `Vec` that becomes the `MSG` head line, and the payload
  copy the copy-on-deliver rule asks for (change 6, and `memprobe.py` is what pays
  for it) — and two are per *write*, which in this bench is per message because
  deliveries arrive one at a time: the `Vec<IoSlice>` built for `writev`, and the
  `Sleep` that `tokio::time::timeout` constructs for every write attempt.
* **One task hop and one wake-up per batch.** The reference appends the frame to
  the subscriber's outbound buffers from the *producer's* thread and, when the
  subscriber's writer is idle, flushes it there and then within a budget
  (`flushClients`, `go:client.go:1431`) — no channel, no wake-up. Our model is a
  queue plus a writer task, which is what makes the ordering promise and the
  bounded-bytes outbox easy to reason about, and costs a `futex` on the way.
* The fanout numbers in `benchmarks/baseline-rust.md` say the width scales better
  than the reference's (0.49× its CPU per delivery at 3 cores against 7), so this
  is a one-subscriber story, not a scalability story.

The next experiment here is the two per-write allocations, and after that the flush
inline/out of the writer task — in that order, each with a three-arm run.

## Where the pubsub gap stands now

This section is about the *delivery* path; the publish-only path that started this
round of work is measured in "The publish-only path" and "The delivery path, same
treatment" above, where the honest summary is 1.07× on the former and 1.3× on the
latter.

Two numbers, depending on how it is asked. On the canonical unpinned protocol
(16 paired rounds, [`baseline-rust.md`](../benchmarks/baseline-rust.md)) the
median is **74 % of the reference** — 199,156 against 268,320 msgs/s. Driven
inside the *same* rounds as the reference (4 rounds, three arms), it is
**418,616 against 417,172 — parity**, bought with 1.40× the CPU per message
(3.42 µs against 2.44). The difference between those two answers is how much the
machine was doing at the moment of the sample, and both are in the raw logs.

What is left, ranked by the counters rather than by guesswork:

1. **Per-message user-space work.** Mostly paid for on the publish path
   (0.03 allocations a message, change 3) and still open on the delivery path:
   **5.02 allocations and 499 bytes per delivered message** on the current build.
   Three of the five are named and cheap — the `Vec` that `route` collects matched
   subscriptions into (one per message when a subject has subscribers), the
   `Vec` that becomes the `MSG` head line, and the payload copy `route` takes on
   purpose (change 6). The other two are per *write*, not per message, and only
   appear when deliveries arrive one at a time: the `Vec<IoSlice>` collected for
   each `writev`, and the `Sleep` that `tokio::time::timeout` builds for every
   write attempt. A batch of 3 buffers should not need an allocation to describe
   itself to the kernel, and the deadline should not need a new timer for a write
   that completes in one syscall.
2. **Read side.** Settled by change 7: the buffer grows to 64 KiB the way the
   reference's does, and the publish path went from 1 `recvfrom` per 110 messages
   to per ~450.
3. **Wake-ups.** Settled, and no longer a suspect: 0.0042 `futex` per message
   against the reference's 0.014, after change 1.
4. **Registry lock.** Still taken once per message, and it is a *process*-wide
   lock: the publish-only path now avoids it entirely when nothing matched
   (change 6 — with no subscriptions at all it is never touched), but a message
   that reaches someone takes it, and so does every subscribe/unsubscribe. With
   one publisher and one subscriber that is invisible (change 6's measurement:
   the lock costs ~25 ns uncontended, and the whole publish costs 320). Untested
   at width beyond the fanout numbers in `benchmarks/baseline-rust.md`, which say
   the width scales better than the reference's does.
5. **The reference has an L1 cache the publish path does not.** `c.in.results` is
   a per-client map of subject → `SublistResult`, invalidated by the sublist
   `genid`, so a publisher that publishes to the same subject a million times
   resolves the interest list *once*. We match per message. It is the one
   structural difference left on this path, and the one the counters cannot
   explain away: we are at 320 ns against its 297.

Item 1 is still the honest next experiment for the delivery path, and the two
per-write allocations named there are the cheapest thing in it: an `IoSlice` array
on the stack for a small batch, and a deadline armed per batch instead of per write
attempt. Both need the `pubsub` three-arm protocol before and after, because the
`pubsub` *rate* is set by the client and moves ±60 % between rounds of the same
binary; the CPU column is the one that can be believed.

## Rule

An optimisation that breaks the ordering proof
(`crates/server/tests/ordering.rs::ordering_proof_100k`) is reverted, not
documented. Every keeper gets a row here with its delta. A throughput claim that
cannot be reproduced without the Go control from the same session is not a
delta — it is noise, and it goes in `benchmarks/baseline-rust.md` instead.

## The allocation census, both binaries (2026-09-22, end of Part 2 — superseded by `benchmarks/alloc-census.md`)

The gap `NATS_GO_STATS_FILE` was 404 for is closed: build the reference from the
source we have plus `specs/tools/go-instrument/zz_allocstats.go`, and its
`Mallocs` counter reads the same way ours does. Measured on this host:

| Unit | reference | ours, start of Part 2 | ours, now |
|---|---:|---:|---:|
| published message, nobody subscribed | 1.00 allocs, ~4 B | 5.07 allocs, 775 B | **0.03 allocs, 130 B** |
| delivered message (1 pub → 1 sub, 256 B) | 1.06 allocs, ~171 B | (not taken) | **5.02 allocs, 499 B** |
| connection, opened and closed | 73.2 allocs, ~13 KiB | **not yet measurable** | not yet measurable |

Three things follow, and they are the whole of Part 3.

1. **The reference allocates once per published message and will keep doing so.**
   `processInboundClientMsg` passes `string(c.pa.subject)` to `acc.sl.Match` — a
   byte-slice-to-string conversion used as a *function argument*, which Go cannot
   elide — and it cannot skip it either, because its L1 result cache only stores
   results that have subscribers. Zero allocations per message beats that design;
   matching it is not the goal.
2. **The delivery path is where we still lose** (1.32–1.38× the reference's CPU per
   message, 5.02 allocations against its 1.06). Two of our five are per *write* and
   only show up when deliveries arrive one at a time — the `Vec<IoSlice>` built for
   `writev` and the `Sleep` that `tokio::time::timeout` constructs for every write
   attempt.
3. **Per-connection cost is unmeasured on our side**, and the instrument for it does
   not exist yet: `allocstats` dumps at start and shutdown only, so a run's worth of
   accepts and closes cannot be isolated. That is a 30-line fix and it gates every
   claim about churn.

## Part 3: beating it, and the two bugs that were in the way (2026-09-22)

Same protocol as above — paired three-arm (`./benchmarks/run_three.sh`, old build,
new build, reference, inside each round), server pinned to core 7, driver to core 3,
CPU seconds per message from `/proc/<pid>/task/*/stat`. **We are now in front on both
paths.** Medians of the ratios, four rounds each; raw files named:

All four rows are the medians of four paired rounds driven against the build in
this commit (`benchmarks/raw/2026-09-22-pubonly-final.txt`,
`2026-09-22-pubsub-final.txt`), so they are the numbers a reader can reproduce.
Where a round lands in the other regime — the machine's ±1.7×, see "One caveat the
paired protocol caught" — the ratio holds even when the absolute seconds move, which
is the whole reason the ratios are the column that gets quoted.

| Path | ours, start of Part 3 | reference | ours, now | ours / reference |
|---|---:|---:|---:|---:|
| publish-only, 20 M × 128 B | 0.317 µs (1.07×) | 0.298 | **0.265** | **0.89×** |
| delivery, 3 M × 256 B (`floodpubsub`) | 1.330 µs | 1.215 | **0.734** | **0.60×** |
| fan-out, 10 subs, 1 M deliveries | 1.120 µs | 0.920 | **0.710** | **0.77×** |
| fan-out, 50 subs, 1.5 M deliveries | 0.833 µs | 0.760 | **0.507** | **0.67×** |

Throughput follows where the client is not the limit: 3.6–4.0 M msg/s publishing
against the reference's 2.9–4.2 M in the same rounds, and 0.9–1.9 M deliveries/s
against its 0.77–0.82 M. Peak RSS during those delivery runs: **3–6 MiB against the
reference's 62–80 MiB**, and a wedged subscriber now costs 1.00× `max_pending` where
the reference costs 1.98× (`memprobe.py`).

Per message the server allocates **0.010 times on the publish path (1.6 B) and 0.111
on the delivery path (333 B)**; the reference, counted the same way, 0.751 and 1.059.
The whole table with its per-site attribution is
[`benchmarks/alloc-census.md`](../benchmarks/alloc-census.md), regenerated by
`./benchmarks/tools/census.sh`.

Five changes got there, and two of them were not optimisations.

### 10 — the interest cache (keeper, Task 24)

A registry `generation` counter, bumped under the lock by everything that changes
who is interested — insert, remove, a close, an auto-unsub spending a subscription —
and a per-*task* cache of (subject, generation) → resolved subscriptions, held on
the reader's stack in `read_loop` where it needs no lock, no `Arc` and no
synchronisation with anybody.

The reference answers a repeated publish from exactly this structure
(`c.in.results`, invalidated by the sublist `genid`) and we were paying a hash probe,
a wildcard scan and a process-wide mutex for every message instead. Measured: the
publish path went 0.317 → 0.284 µs (change 13 finished the job), and a publish that
nobody wants is now a byte compare and an atomic load.

Why per *operation* rather than per read, and why the generation has to be read
inside the lock: `SUB` and `PUB` in the same segment. `crates/core-it/tests/interest.rs`
is the proof, five cases, run against both binaries: the same-segment self-delivery,
the same-segment publish-before-subscribe, a foreign `SUB`, a close, and an
auto-unsub reaching its limit. A stale cache is not a slow cache — it delivers to
subscriptions that are gone.

### 11 — the frame arena (keeper, Task 25.3 + 25.4)

A delivered frame used to be three buffers: a `Vec` that became the head, a `Bytes`
copy of the payload, and a static CRLF. It is now **one contiguous buffer**, laid
down in `arena.rs`: head, payload and terminator written into an 8 KiB chunk that
~26 messages share, `seal`ed into a `Bytes` view that the writer hands to
`writev` as one entry. The chunk is per *connection* — a per-publisher chunk would
let one wedged subscriber pin the bytes of a healthy one, which is the failure
`memprobe.py` was written to catch — and it is handed out under the queue lock, so
frames are laid down in the order the writer drains them: the recycling rule restated
as the ordering promise, with a refcount instead of a ring.

### 12 — the write path: one syscall shape per batch, one timer per writer (keeper, Task 26.1 + 26.2)

A frame is one buffer now, so a batch of one message is one buffer: the writer
detects that and issues a plain `write` with **no iovec array at all** (the
`Vec<IoSlice>` that used to be built per attempt was an allocation per delivered
message whenever deliveries arrived one at a time). Past one buffer it fills a
32-slot stack array; past 32 it takes a `Vec`, which is one per batch of up to a
thousand messages.

The `Sleep` that `tokio::time::timeout` built for every write attempt is now one
`Sleep` owned by the writer and `reset` per attempt, which keeps the 10 s write
deadline exactly as `lifecycle.rs::a_wedged_subscriber_is_closed_and_releases_its_publisher`
pins it.

### 13 — `capacity() - len()`, not `remaining_mut()` (keeper, and the biggest delta here)

**Both** buffer tests in this server were wrong, in the same way, and the code around
them had been written to be right.

`bytes::BytesMut` implements `BufMut::remaining_mut` as `usize::MAX - len`, because a
`BytesMut` can always grow — that is the trait's answer, not the buffer's. So
`if buf.remaining_mut() < cap` is never true, `reserve` was never called, and Part 2's
change 7 ("the read buffer grows like the reference's") grew the buffer only inside
`chunk_mut`, one implicit reservation at a time, **allocating a fresh buffer on every
read**: the allocation histogram shows one 16 KiB allocation per read, and
`bytes_per_msg` on the publish path was 253 B ≈ the whole wire.

And `if self.chunk.remaining_mut() < need` in the frame arena was never true either,
so the arena never handed out a chunk: every `put_slice` grew the same `Vec` by
exactly its own shortfall, which is one allocation for the head and one `Shared`
promotion **per frame**. `frame_chunk=0` in the per-site counters is what gave it
away while the delivery census still read 5.02 allocations a message — the fix had
already been written and did nothing. Measured: delivery 3.03 → 0.11 allocs/msg and
1.33 → 0.75 µs CPU per message, publish 0.065 → 0.018 allocs/msg.

One defect of the same family was found and fixed while finishing this: the first
version of `pick_groups` narrowed the cached subscription list in place, which
leaves one member of a queue group in the cache and sends every later message to
that member — a group of two, sampled twice, delivers each message twice.
`verbs.rs::sub_third_arg_is_the_queue_not_a_limit` failed loudly on it, which is
what that test is for; the sampling now writes to a second list.

Rule for the file: on a `BytesMut`, ask `capacity() - len()`. `remaining_mut()` is a
`BufMut` lie.

### 14 — hoist the views a straddling frame holds (keeper, Task 23.3)

`BytesMut::reserve` reclaims an allocation in place **only while nothing else refers
to it**. A `PUB` whose body is split across two reads keeps the frozen line and the
body fragment alive across the boundary, so the reclaim could not happen and the
reader paid a fresh buffer per read anyway. `proto.rs::hoist` turns what the parser
state holds into owned bytes on the way out of an incomplete read — subject, reply,
and only the body piece taken during *this* read, tracked by `Parts::fresh` so a
1 MiB payload spanning 16 reads does not copy 16 times.

Measured: publish path 0.018 → 0.010 allocs/msg and 291 → 1.9 bytes/msg. The last
0.010 is one small copy per read, which is the honest floor for a frame that straddles.

### The parser is the publish path now, measured rather than suspected

`examples/parsebench.rs` runs the same `Parser` over a 64 KiB block of pre-encoded
`PUB` frames with no sockets, no syscalls and no routing. Re-run against this
commit's build, pinned, on a saturated core (so the figure cannot be the clock
waking up — a 30 s run reads the same as a 1 s one):

| Run | ns per message | what it says |
|---|---:|---|
| `SIZE=128 ROUNDS=60000` | **194.9** | 74 % of our entire publish path (265 ns) is inside the parser |
| `SIZE=128 ROUNDS=2000` | 190.9 | the same number cold, so it is not frequency |
| `SIZE=0 ROUNDS=60000` | **148.8** | a 14-byte frame costs the same 149 ns ⇒ the cost is *per message*, not per byte |
| `--features allocstats` | **0.00 allocs, 1 B/msg** | the parser allocates nothing; it is not an allocation problem |

What the 149 ns buys, from the same line: **`size_of::<Event>() == 120`** and
**`size_of::<Parser>() == 160`**. One parsed `PUB` is therefore ~120 bytes written
into the batch array, ~120 bytes read back out of it by `handle`, two 160-byte
state-machine moves (Line→Body→Line), and five or six `Bytes` refcount pairs from
`split_to().freeze()` and `slice()` — for 144 bytes of wire. The scanning itself is
the remaining ~40 ns at 0.3 ns/byte, which is memory bandwidth and is fine.

So the shape of the next change is named by the counters, not by taste: one buffer
of offsets instead of a batch of `Event`s, and operations dispatched as they
complete instead of collected and moved. It is `PLAN3.md` Task 23.4's surgery, and
the parser's own test surface (`crates/server/tests/proto.rs`, 17 in-process tests
including every segment-cut position and byte-at-a-time input) is what makes it
safe to attempt.

### 15 — the syscalls that are left, counted (Task 26.4's gate)

`strace -f -c` launched as the server's parent (attaching is blocked by
`ptrace_scope`), driven by `floodpubsub` at 1 M messages of 256 B. The run is
strace-slowed, which does not move a count-per-message figure:

| Counter | ours, now | reference (Part 2, same probe) |
|---|---:|---:|
| `writev` per delivered message | **0.0011** — one per 876 | 0.0039 — one per 259 |
| `recvfrom` per delivered message | 0.0024 — one per 414 | 0.0029 |
| `futex` per delivered message | **0.00057** — one per 1,763 | 0.014 |
| `mprotect` per delivered message | 0.0047 | 0 |

Two writes for every thousand deliveries and one thread wake-up for every two
thousand messages: the task hop the delivery path still pays for is now batched
away rather than per message, which is what Task 26 was after. The `mprotect` row
is glibc trimming arenas for the 0.11 allocations a message; it costs 0.4 % of the
run's syscall time and is on the list for the same reason the allocator spike is.

### Part 3's exit criteria, measured

| Path | Part 2 exit | Part 3 target | measured | verdict |
|---|---:|---:|---:|---|
| publish-only CPU µs/msg | 0.317 (1.07×) | ≤ 0.27 (< 0.95×) | **0.265 (0.89×)** | ✅ |
| allocations per published msg | 0.03 | ≤ 0.005 | **0.010** | ⚠️ 2× off, and it is one small hoist per read, not per message |
| delivery CPU µs/msg | 1.89–1.98 (1.32×) | ≤ 1.2 (< 0.9×) | **0.734 (0.60×)** | ✅ |
| allocations per delivered msg | 5.02 | ≤ 1.0 | **0.111** | ✅ 10× under |
| allocations per connection | unknown (ref 73.2) | ≤ 25 | not measured (Task 27 open) | ⬜ |
| `futex` per delivered msg | not measured | < 0.014 (ref) | **0.00057** | ✅ 25× under |
| `writev` per delivered msg | ~1 per message | ≤ 0.2 | **0.0011** | ✅ |

### What the delivery path spends now, and what is left

0.111 allocations a message: an arena chunk per 29 frames, the `Vec`→`Arc` promotion
that comes with it, and a tokio mpsc block per 32 queued frames. CPU is 0.60× the
reference's with one subscriber and 0.67× at fifty.

Not taken, and why: the inline flush (Task 26.3) — the reference flushes an idle
subscriber's queue from the producer's thread (`flushClients`, `go:client.go:1431`),
and we still hand every delivery to a second task. It is the last structural
difference on the delivery path, it is a change to the write model in contract §8,
and at 0.62× of the reference's CPU it is no longer what puts us in front. It stays
on the list, not in the tree.
