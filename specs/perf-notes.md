# Performance notes

One change at a time, each followed by a paired Go/Rust run, and the ordering
proof re-run after every step (Task 20 Step 3). Numbers are the paired protocol
of [`benchmarks/baseline-rust.md`](../benchmarks/baseline-rust.md).

## How this is measured on this host (there is no `perf`)

`perf` cannot run here: `kernel.perf_event_paranoid = 4`, no `CAP_PERFMON`, no
passwordless sudo. So the attribution below comes from four counters that don't
need a profiler, each taken the same way against both binaries:

| What | How | Works on Go? |
|---|---|---|
| Server CPU seconds | `/proc/<pid>/stat` `utime+stime` before/after a bench run, divided by messages | ✅ |
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

## Where the pubsub gap stands now

Two numbers, depending on how it is asked. On the canonical unpinned protocol
(16 paired rounds, [`baseline-rust.md`](../benchmarks/baseline-rust.md)) the
median is **74 % of the reference** — 199,156 against 268,320 msgs/s. Driven
inside the *same* rounds as the reference (4 rounds, three arms), it is
**418,616 against 417,172 — parity**, bought with 1.40× the CPU per message
(3.42 µs against 2.44). The difference between those two answers is how much the
machine was doing at the moment of the sample, and both are in the raw logs.

What is left, ranked by the counters rather than by guesswork:

1. **Per-message user-space work.** 11.08 allocations and 1,104 bytes per message
   on the final build, against 256 bytes of payload — 4.3× the bytes moved. Two
   named sites: `Frame::parts()` allocates a `Vec<Bytes>` per frame per
   subscriber (three clones into a fresh vector, when the writer could take them
   straight into its own), and `itoa()` in `build_frame` returns a `String` for
   the numbers in the head line. Next experiment, in that order, each with an
   `allocstats` count and a paired three-arm run: fill a reusable part list
   (expect roughly −1 alloc and −56 B per message), then write the digits into the
   head buffer (expect −1 alloc, −32 B).
   Caveat that the counters themselves impose: the `allocstats` build costs 2.4×
   the CPU of the plain one (8.32 µs/msg against 3.42 for the same run), so its
   numbers answer "how many", never "how fast".
2. **Read side.** 1 `recvfrom` per 75 messages against the reference's 1 per 205,
   and we have not changed it. Unexamined: whether our reader's buffer growth
   policy is the reason, and how many bytes the client actually hands us per
   segment. Measure: bytes per `recvfrom` in both, from the same `strace -e
   trace=read,recvfrom -T` run.
3. **Wake-ups.** Settled, and no longer a suspect: 0.0042 `futex` per message
   against the reference's 0.014, after change 1.
4. **Registry lock.** Not on this path — one publisher, one subscriber, ~1 core
   busy in each binary, and the fanout numbers (0.37× the CPU per delivery of the
   reference, 3 cores against 7) say the width scales better than the reference's
   does. Untested, lowest priority, and it would take a contention counter to test.

There is no measurement that explains the remaining 40 % CPU-per-message gap on
the single-stream path yet; item 1 is the honest next experiment and Part 3's
first bench task should start there rather than with any restructuring.

## Rule

An optimisation that breaks the ordering proof
(`crates/server/tests/ordering.rs::ordering_proof_100k`) is reverted, not
documented. Every keeper gets a row here with its delta. A throughput claim that
cannot be reproduced without the Go control from the same session is not a
delta — it is noise, and it goes in `benchmarks/baseline-rust.md` instead.
