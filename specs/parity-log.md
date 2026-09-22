# Parity log

Every difference found between `crates/server` and the reference binary while
getting the shared suite green, plus the control runs that prove the suite. The
rule under test: *never weaken an assertion to make the Rust server pass* — so
each row ends either in a server fix, or in a test correction that cites the
reference's measured behaviour.

## Control runs

| Run | Binary | Result |
|---|---|---|
| Suite before any server existed (Task 8 gate) | `/home/vlad/apps/nats-server` | 129 passed, 0 failed |
| Suite after Task 20 (135 tests, incl. the four corpus-driven rulings) | `/home/vlad/apps/nats-server` | **5 consecutive full runs, 135 passed, 0 failed**, no orphans |
| Same, 135 tests | `target/release/nats-server-rs` | **5 consecutive full runs, 135 passed, 0 failed**, no orphans, no ports files |
| Differential corpus, 2026-09-21 after row 13 | both | 206 cases, **0 differences** (it read 20 diffs before that evening) |
| Suite after the publish-path rewrite (rows 3–9 of `perf-notes.md`, 135 tests) | `target/release/nats-server-rs` | 135 passed, 0 failed |
| Same suite, same build | `/home/vlad/apps/nats-server` | 135 passed, 0 failed — the suite still describes the reference, not our server |
| Differential corpus, 2026-09-22, after every parser change | both | **206 cases, 0 differences** |
| Suite after `crates/server` | `/home/vlad/apps/nats-server` | **5 consecutive full runs, 129 passed, 0 failed, zero skipped**, no orphans after |
| Same suite, same binary | `target/release/nats-server-rs` | **5 consecutive full runs, 129 passed, 0 failed, zero skipped**, no orphans, no ports files left in /tmp |
| Ordering + queue proofs, in-process client | both | `crates/server/tests/ordering.rs` — passes against Go too, so the tests are not written to our server |

Nothing is skipped, ignored or `#[ignore]`d in either direction.

## Bugs found in the Rust server, and what fixed them

| # | Symptom (test that caught it) | Root cause | Fix | Rule |
|---|---|---|---|---|
| 1 | Every harness test: `server not ready within 10s` | ports file rendered `…:38029]}` — a raw-string brace escape swallowed the quote before `]` | emit `{"nats":["nats://host:port"]}` and pin it in `write_ports_file` | §1 process contract |
| 2 | `failed to listen on 127.0.0.1:-1 — invalid socket address syntax` | `-p -1` was passed straight to bind. The reference's `RANDOM_PORT` sentinel means "let the kernel choose" | sentinel → port 0 in `net::serve`; `0` in a config still means "unset → 4222" | §1 |
| 3 | `info_on_connect_has_core_fields` / INFO json parse panic | hand-rolled INFO forgot the `,` after a numeric field, so `"proto":1"host"` — invalid JSON | separator emitted per field; `info_is_a_single_line_in_the_documented_shape` keeps it honest | §2 |
| 4 | All of `headers.rs` — `HMSG hb 112 14` | the HMSG head line was built without the space before `#hdr` | fixed the field order: `HMSG subject sid [reply] #hdr #total` | §6 |
| 5 | 4–6 of `headers.rs`, only in parallel runs; ~60 % of single runs | the `headers` capability was checked while *parsing* the HPUB control line. A CONNECT and an HPUB coalesced into one read were parsed with the pre-CONNECT capability → silent close. The reference checks it in `processHeaderPub`, at execution | parser emits `hpub: bool`; `client.rs` compares it with the connection's current options | "the operation is decided where the options live" + §3 |
| 6 | `pub_over_max_payload_gets_err` and friends, only under load: `-ERR` never arrived | the writer treated "wake fired" as "no more frames" whenever the sender had been dropped, abandoning queued frames | `rx.recv()` alone decides (it yields None only when closed **and** empty); `biased` so a pending frame wins over a wake | §7 teardown ordering |
| 7 | `verbs_are_case_insensitive`-adjacent: `PING x\r\n` closed the connection | parser required a bare PING; the reference's `OP_PING` state has no default arm and skips to the line end | PING/PONG matched as 4-byte prefixes, remainder ignored (measured: `PINGxyz` answered, connection kept) | §3 |
| 8 | `CONNECTxyz {}` closed as an unknown verb | same class of mistake for CONNECT: `OP_CONNECT` falls through to `CONNECT_ARG` on any byte, so the junk reaches the JSON decoder and dies silently | CONNECT matched as a 7-byte prefix; empty argument stays class C (measured both) | §3 |
| 9 | `a_blocked_subscriber_is_dropped_not_accumulated`: nothing failed, but nothing was provable either | kernel buffers (up to 4 MiB) absorb a small test, so a "slow consumer" never appears | set a small receive buffer on the victim and a small `max_pending` through the config path | §7, measured with both |

| 10 | nothing yet — found by reading `route` against the contract | a queue group whose every member was over the 75 % stall threshold delivered to **none** of them, so `count` was 0 and an unanswered-looking request would have drawn a spurious 503 status frame | probe for a member that has room, and if none has room hand it to the first eligible member anyway and let the publisher wait — which is what the reference's stall channel does | §6 queue groups, §7 stall |
| 11 | nothing failed — Task 17 Step 4's measurement of a **wedged** subscriber | the write deadline had never been observed on the wire. Measured against the reference: the victim gets whatever was already queued to its socket (987 270 bytes in both binaries' trials) and then FIN, **no `-ERR`**; the server and every other connection are untouched; the publisher parked on the stall gate is released within one deadline (2.36 s reference, 2.38 s ours) | no server change — the behaviour matched. It is now pinned by `lifecycle.rs::a_wedged_subscriber_is_closed_and_releases_its_publisher` (green against both) and the residual mechanism difference — the reference closes on the 100 % pending mark at 0.66 s and prints a stray `WriteDeadline` notice for the same cid 1.6 s later, we close on the deadline at 2.0 s — is written into contract §7 instead of being papered over | §7 slow consumers, §9 unknowns (this row retires one of them) |

| 12 | **a `pubsub` round that never ended**, one in ~50 paired rounds: both sockets open and empty, every server thread idle, no `-ERR`, no close | a publisher parked on the 75 % stall gate of a subscriber that then died. Two holes: closing the connection dropped the queued frames without zeroing the byte counter, so `room()` was `false` forever; and `wait_for_space()` looped on `room()`, so the close's one `notify_waiters()` woke the task straight into another sleep. The reference's own write deadline (10 s) is what closes the victim in that bench — the stall never resolves on its own | `Outbox::gone`: set with the counter zeroed at close, honoured by `room()` and by `wait_for_space()`; `drained()` made saturating so a double drain cannot wrap a `usize` into a number no publisher ever gets under | §7 stall gate, §8 write model |

Row 10 has no regression test: reproducing it needs a queue group of wedged
subscribers, and the timing would make the test flakier than the bug is common.
It is recorded here so that a future rewrite of `route` cannot quietly reintroduce
it, and so nobody claims it is covered.

| 13 | the differential corpus, re-run for Task 20 Step 3, reported **206 cases / 20 differences** where this log claimed zero | four rulings the command table never pinned, all read off the reference afterwards: (a) `INFO` from a client is **parsed** — a JSON object is ignored, anything else (including no argument, `5`, `[1,2]`, `{"a":1} x`) is class C, and the verb matches as a prefix so `INFO{...}` is a good line; (b) `-ERR` needs an argument to *be* the verb: `-ERR x` closes silently, bare `-ERR\r\n` is class A; (c) the payload terminator is checked **positionally** — the reference looks at the single byte after the declared payload and reports class A the moment it is not `\r`, rather than buffering a CRLF and waiting forever on a desynced stream; (d) an `HPUB` is rejected **at the control line**, not at the body: without the `headers` capability it is class C whatever the arguments said, which our parser was answering with class A because the body bytes it went on to parse were not a frame | parser: `Event::Info(Bytes)` carrying the argument, `-ERR` requires `has_args`, `State::Body` decides on `src[0]`, and a new `Event::HpubAttempt(HpubOutcome)` that stops the batch so the capability is still the client's decision at execution — row 5's rule, one level deeper. Four tests in `verbs.rs`, each one verified to fail against the pre-fix build and pass against the reference | §3 verbs and error classes, §4 framing, §6 headers — and the "any diff is a bug until proven otherwise" rule |
| 14 | no test failed. Found by *counting syscalls* while chasing a publish-rate difference (`nats bench pub`, one publisher, no subscribers) | **the reference writes with Nagle off and we did not.** `strace -f -e trace=setsockopt` on both servers is the evidence: the reference does `setsockopt(fd, SOL_TCP, TCP_NODELAY, [1], 4)` on the accepted socket — and it has no `SetNoDelay` call anywhere in `server/*.go`, because Go's `net` sets the option on every TCP connection it hands out. Our accept path never asked, so every `MSG`, every `-ERR`, every `PONG` waited for an ACK or for more data to coalesce. Nothing in the protocol suite can see this: the bytes are the same, only their timing is not | `net::accept_loop` calls `set_nodelay(true)` on each accepted socket before it can be written to (`specs/perf-notes.md`, change 8). Verified the same way: the option now appears in our `strace`, once per connection, with the same `[1]` | §8 write model (new: the write model includes the socket options) |

Row 12 is the one bug in this log that no test found: it appeared as a benchmark
that stopped printing a line. The suite is black-box and time-bounded, so a hang
needs a run long enough and a machine loaded enough to lose the wake-up — which
is why `benchmarks/run_baseline.sh` is part of the correctness story, not only the
performance one. The regression test does reproduce it, deterministically: it
fails at 10.04 s against the pre-fix build and passes in 0.40 s against the
reference.

Row 5 is the one worth remembering: a bug that is invisible unless the OS
coalesces two writes, and visible *only* under load. The fix is not the parser
change but the principle — a verb's meaning is decided where the connection's
options live, never where the bytes are counted.

## Corrections on the test side (never a relaxation)

* `lifecycle.rs::auto_unsub_limit_already_reached_unsubscribes_now` sent
  `UNSUB 1 2` and then published from another connection without waiting for the
  UNSUB to be processed. Against the reference the window is small; against our
  server it was reachable. The test now barriers on the *subscriber* connection
  before the next publish, and asserts that nothing arrived while the UNSUB was in
  flight. Same assertion, strictly more deterministic — this is a race in the
  test, and it was there before any Rust server existed.
* `PLAN2.md` claimed `server_id` is a 26-char NUID. It is the server's nkey
  public key: 56 chars, base32 alphabet (measured, `server.go:716`). `wire.rs`
  pins the measured shape; §2 of the contract records that a server with no
  authorization has no key pair and emits an opaque token of the same shape.
* `PLAN2.md` listed "subject containing a space" as a class-C trigger. Not
  expressible — whitespace separates arguments — so it is gone from the contract
  (§9) rather than tested.
* `PLAN2.md` expected an unknown flag to exit non-zero. The reference prints
  usage on stdout, the reason on stderr, and exits **0** (measured). The harness
  test asserts what both binaries give: no readiness, reported as an error.

## Open divergences (deliberate, recorded, not hidden)

* INFO omits `git_commit`, `go`, `api_lvl`, `xkey`. They describe a Go build or a
  key exchange we do not have; all four are `omitempty` in the reference and
  `client.rs` proves async-nats 0.50 connects without them.
* `server_id`/`server_name`: same shape and alphabet, not a verifiable nkey.
* The 75 % stall is a publisher-side wait (the reference opens a stall channel the
  same way); its observable half — victim dropped, everyone else fine — is what
  the suite asserts, because kernel buffers hide the rest.
* Max-connections behaviour is unmeasured (see contract §9); we close the socket
  after accept with no bytes.

## Differential corpus (Task 18 Step 3)

`specs/tools/difffuzz.py` drives both binaries with the same 206 control lines —
every verb in every arity and both cases, payload framings, pre-CONNECT commands,
oversized lines and frames — and diffs `(bytes received, connection closed?)`.

**Result: 206 cases, 0 differences** — but only after row 13. The tool was run
once on 2026-09-21 during Task 18 and reported 0 diffs; re-run that evening for
Task 20 Step 3 it reported **20**, in four distinct behaviours that the corpus
happened not to exercise the first time (the difference is the `HPUB`/`INFO`
argument shapes and one framing case). Both runs are honest; the second one is
the current truth, and the 20 are now fixed and pinned by tests rather than
explained away.

Re-run it after **any** parser change, and treat the count as the assertion it
is: a diff is a bug in the Rust server until written up here with evidence.
`difffuzz.py A B` also compares two builds of *our* server, which is how change 1
and the row-13 fixes were confirmed not to move any other response: old build
against new, 206 cases, 0 diffs.

## Fan-out and stall behaviour at width (Task 17 Step 5)

`SUBSCRIBERS=10` and `SUBSCRIBERS=50`, 20 k messages: both binaries complete, the
bench's 30 s stall timer never fires. At 50 subscribers the Rust server measured
*faster* than the reference (309 k vs 186 k deliveries/s, one round each — a
single sample, recorded so a later run can check it rather than so anyone can
rely on it).
