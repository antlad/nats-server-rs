# NATS Core wire contract (measured)

The specification `crates/server` implements. Every row was obtained by driving
`/home/vlad/apps/nats-server` (`v2.16.0-dev`, commit `8ad5265`, go1.26.8) with
raw sockets on this host, or read from the reference source
(`go:<file>:<line>`). Where the two disagree the binary wins; nothing here is a
guess, and the "unknown" list at the bottom is as important as the table.

Test counts, this file's own gate: **129 tests, green against the Go reference
binary and against `crates/server`, on 2026-09-21** — integration: `wire.rs` 17,
`verbs.rs` 23, `headers.rs` 18, `lifecycle.rs` 18, `client.rs` 11; harness 5 +
doctests 2; bench unit 2; in-process server tests: `proto.rs` 17,
`info_connect.rs` 9, `subjects.rs` 5, `ordering.rs` 2. Part 1 had 25; no Part 1
assertion was weakened or moved. See `specs/parity-log.md` for what changed and
why, and `specs/tools/difffuzz.py` for the 206-case corpus diff between binaries.

How each row was obtained is marked: **M** = measured against the live binary on
2026-09-21, **G** = reference source, **T** = pinned by a test in this repo.

## 1. Process contract

| Item | Contract | How |
|---|---|---|
| Flags used | `-a <host>`, `-p <port>`, `--ports_file_dir <dir>`, `-c <file>`, `-v`/`--version`, `-h`/`--help`, `-D`, `-t` | M, G:`opts.go` |
| Random port | `-p -1` → ephemeral port. `-p 0` means "unset" → 4222, **not** random | M |
| Unknown flag | usage text on **stdout**, `flag provided but not defined: -<x>` on stderr, **exit status 0** | M |
| Bad config file | message on stderr, exit status 1 | M |
| `--version` | `nats-server: v2.16.0-dev\n`, exit 0 | M |
| Readiness | `<exe>_<pid>.ports` containing `{"nats":["nats://<host>:<port>"]}`, written **after** the listener accepts | M, G:`server.go:4349` |
| Ports file lifetime | removed on SIGTERM/SIGINT; SIGKILL leaves it behind (harness uses SIGKILL, so absence is never assumed) | M |
| Defaults | `max_payload` 1 MiB, `max_control_line` 4096, `max_pending` 64 MiB, `max_connections` 65 536, `ping_interval` 120 s, `max_pings_out` 2, write deadline 10 s | G:`const.go:88-120` |
| Config validation | `max_payload` may not exceed `max_pending`: `max_payload (1048576) cannot be higher than max_pending (65536)`, exit 1 | M |

The harness treats "the process exited" and "no ports file" as one failure mode,
which is why an unsupported flag need not set a particular exit status to be
caught — see `unsupported_flag_never_becomes_a_silent_server`.

## 2. INFO

Sent as the first bytes of every accepted connection, before any client byte.

Byte shape: `INFO` + SP + JSON + SP + CRLF. The second space is an artifact of
the reference's `generateInfoJSON` (`util.go:360`, `bytes.Join(["INFO", json, CRLF], " ")`).
Clients must treat it as whitespace; our server reproduces it. **M**

Present, core-only server:

| Key | Value | How |
|---|---|---|
| `server_id` | 56 chars, base32 alphabet `A-Z2-7`, first character `N` — the reference's **nkey public key**, not a NUID (`server.go:716`). The Part 2 plan's "26-char NUID" was wrong | M |
| `server_name` | equal to `server_id` unless `-n`/config sets it | M |
| `version` | `"2.16.0-dev"` in the reference | M |
| `proto` | `1` | M |
| `host`, `port` | bind host and *resolved* port | M, T |
| `headers` | `true` | M |
| `max_payload` | `1048576`, or the configured value | M, T |
| `client_id` | monotonic per connection | M, G |
| `client_ip` | peer address | M, G |
| `git_commit`, `go`, `api_lvl`, `xkey` | implementation detail of the Go build | M |

Part 2 emits `server_id`/`server_name` as an opaque token of the same shape (no
key pair exists in a server with no auth), and omits `git_commit`, `go`,
`api_lvl`, `xkey` — all four are `omitempty` in Go and describe a Go build.
`client_id` and `client_ip` are real and are emitted. `async-nats` 0.50 connects,
subscribes and requests against that reduced set (`client.rs` is the proof). **T**

Absent — the key must not appear, `false`/`[]` is not equivalent for every client:
`jetstream`, `connect_urls`, `cluster`, `domain`, `auth_required`,
`tls_required`, `tls_available`, `nonce`, `ldm`, `compression`. **M, T**

## 3. Verbs

Verbs are case-insensitive **for every character** (`SuB`, `PuB`, `pInG`,
`connect` all behave): the reference folds both cases in each parser state. **M, T**

| Verb | Accepted | Response | Connection | How |
|---|---|---|---|---|
| `CONNECT {json}` | any time, any number of times; also a prefix — `CONNECTxyz {}` hands `xyz {}` to the JSON decoder | `+OK` iff verbose *after* the parse | stays | M, T |
| | | no second INFO | | M, T |
| `SUB <subj> <sid>` | 2 args | `+OK` iff verbose | stays | M, T |
| `SUB <subj> <queue> <sid>` | 3 args — middle is the **queue**, never a limit | `+OK` iff verbose | stays | M, T |
| `SUB <subj>` / 4+ args | — | none | **class C close** | M, T |
| `UNSUB <sid>` | 1 arg | `+OK` iff verbose | stays | M, T |
| `UNSUB <sid> <max>` | 2 args | `+OK` iff verbose | stays | M, T |
| `UNSUB` / 3 args | — | class A / class C respectively | close | M, T |
| `PUB <subj> <size>` | 2 args | `+OK` iff verbose, then the frame routes | stays | M, T |
| `PUB <subj> <reply> <size>` | 3 args | as above | stays | M, T |
| `PUB` / `PUB <subj>` / 4+ args / non-numeric or negative size | — | class A / class C | close | M, T |
| `HPUB <subj> <#hdr> <#total>` | 3 args | routes as HMSG | stays | M, T |
| `HPUB <subj> <reply> <#hdr> <#total>` | 4 args | routes as HMSG with reply | stays | M, T |
| `HPUB` with any other arity, `#hdr > #total`, non-numeric, or from a client that did not declare `headers` | — | none | class C close | M, T |
| `PING`, `PONG` | 4-byte **prefix**: `PING x`, `PINGxyz`, `PONGzz` all behave, the remainder to the line end is skipped | `PONG` for PING, unconditionally — including before CONNECT | stays | M, T |
| `PSUB`, `A+`, `A-`, unknown | — | `-ERR 'Unknown Protocol Operation'` | class A close | M, T |
| empty control line | — | `-ERR 'Unknown Protocol Operation'` | class A close | M, T |
| `-ERR <text>` from a client | — | nothing (the reference logs it) | close, no bytes | M, G:`client.go:2231` |
| `+OK`, `INFO <json>` from a client | — | silently ignored | stays | M, G |
| `CONNECT` with no argument at all | — | none | class C close | M, T |

`PSUB` is not a bug in the reference: this build parses wildcards in `SUB` and
never emits a PSUB handler, so "unknown operation" is parity. **M**

`headers` is an *execution-time* check, not a parse-time one: `processHeaderPub`
looks at the connection's options when the operation runs. It matters because a
CONNECT and an HPUB can arrive in the same read — a server that consults the
capability while counting bytes closes the connection of a client that has only
just declared support for it. Observed exactly that way in `crates/server` and
recorded in `specs/parity-log.md` row 5. **M**

Payload continuation: `PUB`/`HPUB` declare `total` bytes ending in CRLF; the body
may be split across segments at any boundary and must be retained until it is
complete (`verbs.rs::truncated_publish_body_stays_silent_and_open`). **M, T**

### Rulings that only the corpus found (measured 2026-09-21, row 13)

* `INFO` **from a client** is unmarshalled and then dropped. The argument must be
  a JSON **object**: `INFO {}`, `INFO{"a":1}` and `info {"x":[1]}` change nothing
  and keep the connection; `INFO`, `INFO 5`, `INFO [1,2]`, `INFO {"a":` and
  `INFO {"a":1} x` are class C. The verb matches as a **prefix**, like CONNECT.
* `-ERR` **from a client** needs an argument to be that verb: `-ERR something`
  closes with no bytes, bare `-ERR\r\n` is class A (`'Unknown Protocol
  Operation'`) — the same space-sensitivity the `SUB` row documents.
* The payload terminator is **positional**. After exactly `size` bytes the
  reference examines the *one* next byte: not `\r` ⇒ class A at once; `\r` with
  nothing after it ⇒ wait for the second byte. A stream that desynchronised
  inside a body is therefore reported, not buffered forever. `PUB a 3` + `"ab\r\n"`
  is class A; `PUB a 2` + `"ab\r"` waits and completes normally.
* `HPUB` legality is decided **at the control line**, before the declared body is
  looked at. Without the `headers` capability the connection is dropped silently
  whatever the line's arguments were (`HPUB a`, `HPUB a 1 2` + a header block);
  with it, the same bytes are an argument error (class A) or a publish. The
  capability check therefore cannot live in the parser — a CONNECT granting
  `headers` may arrive in the same read — which is row 5's lesson restated.

## 4. Error classes

| Class | Wire result | Triggers | How |
|---|---|---|---|
| A — unknown operation | `-ERR 'Unknown Protocol Operation'` then close | unknown verb, empty line, `PUB`/`UNSUB` with no args | M, G:`parser.go:1253` |
| B — semantic | `-ERR 'Invalid Subject'` / `-ERR 'Invalid Publish Subject'`, connection stays | subscribe filter invalid; publish on a non-literal/invalid subject while pedantic | M, T |
| C — parse error | close, **no bytes** | bad arity, non-numeric size, negative size, HPUB without capability | M, T |
| A′ — size | `-ERR 'Maximum Payload Violation'` then close | declared size (header + body for HPUB) above `max_payload` | M, T |
| A″ — control line | `-ERR 'maximum control line exceeded'` then close | arg line above 4096 bytes | M, T |
| Stale | `-ERR 'Stale Connection'` then close | `max_pings_out` unanswered server PINGs | M, G:`client.go:5899` |

Class A always closes; class B never closes. There is no verb where B is fatal. **M**

`Invalid Subject` on subscribe is checked at insertion (sublist rejects the
filter), so it fires regardless of `pedantic`. `Invalid Publish Subject` fires
**only in pedantic mode**, and pedantic defaults to true, so a bare CONNECT sees
it. A client that sets `"pedantic":false` publishes to a wildcard subject
silently and the message is dropped. **M, T** (`verbs.rs`)

Subject validity (`go:sublist.go:1187-1240`): non-empty; `.`-separated tokens;
no empty token; `>` only as the final token and alone in it; `*`/`>` only as
whole tokens; a token longer than one character must not contain `\t\n\f\r `.
A token that merely *contains* `*` or `>` (`foo.*x`) is a legal literal token.
Publish subjects must additionally be literal. **M, T**

## 5. CONNECT options

| Option | Absent means | Measured effect | How |
|---|---|---|---|
| `verbose` | **true** | `+OK` after each processed command | M, T |
| `pedantic` | **true** | publish-subject validation | M, T |
| `echo` | **true** | own messages come back to own subscriptions | M, T |
| `headers` | false | accepts HPUB, delivers HMSG (else strips to MSG) | M, T |
| `no_responders` | false | 503 status frames | M, T |
| `protocol`, `name`, `lang`, `version`, `tls_required`, unknown keys | ignored | — | M |

`defaultOpts = {Verbose:true, Pedantic:true, Echo:true}` is seeded before
`json.Unmarshal`, which leaves absent keys alone (`go:client.go:710`). A serde
`#[serde(default)]` of `false` would silently break self-delivery and every
`+OK`. **G**

A second CONNECT replaces the options wholesale-ish: keys it omits keep their
current value, and the `+OK` for the CONNECT line itself follows the value that
applies *after* the parse. **M, T**

## 6. Delivery rules

* Order within one publisher → one subscriber is publish order. Proved by the
  ordering test in `crates/server/tests/routing.rs` and by
  `client.rs::flush_delivers_everything_published_before_it`.
* Fan-out: N non-queue subscriptions each get a copy; queue members get exactly
  one copy per message across the group. Reference picks a **random start index
  and probes forward**, skipping members that cannot take it
  (`go:client.go:5253`); assert "every member reached", never an exact split. **T**
* Self-delivery: the only skip case is same-connection and `echo == false`. **M, T**
* Auto-unsub: `UNSUB <sid> <max>` with `max > delivered` bounds the total to
  `max`; `max ≤ delivered`, `max ≤ 0`, or an unparseable max unsubscribes
  immediately. Unknown sid is not an error and closes nothing. **M, T**
* Re-using an sid: the **original** subscription keeps serving, the new SUB is a
  no-op (with a different subject) or a duplicate-suppressor (same subject). **M, T**
* Duplicate SUB of the same (subject, queue, sid) does not double-deliver. **M, T**
* No-responders: when nothing was delivered, the publisher declared
  `no_responders`, and the publisher holds a subscription on the reply subject,
  the server emits, to the publisher's inbox subscription:
  `HMSG <reply> <sid> <32+len(subject)> <same>` + `NATS/1.0 503\r\nNats-Subject: <subject>\r\n\r\n`
  with no payload. Frame subject is the **inbox**, the dead subject is a header.
  Sent for plain PUB as well as HPUB. Not sent for a queue-group hit, not sent
  without the capability, not sent when the publisher has no inbox subscription. **M, T**
* Headers: `#hdr` counts the closing `\r\n` of the block (`NATS/1.0\r\n\r\n` is
  12; plus one `X-Key: value\r\n` is 26). Delivered verbatim to a subscriber that
  declared `headers`; **stripped to a plain MSG** for one that did not. `#hdr == 0`
  means "no headers" and delivers MSG. Header bytes count toward `max_payload`. **M, T**

## 7. Keepalive and teardown

* After CONNECT the server arms a PING timer. The **first** probe is short:
  `min(ping_interval, 2 s)` plus up to 20 % jitter (`go:client.go:7026`,
  measured at 2.25–2.34 s with the default interval). **M, T**
* Thereafter every `ping_interval` (default 120 s). A client PING never resets
  it; a client PONG resets the unanswered counter. **M, T**
* After `max_pings_out` (2) unanswered probes:
  `-ERR 'Stale Connection'` then close. **M, T**
* The server answers a client `PING` with `PONG` unconditionally, including
  before CONNECT, and never closes on an unexpected `PONG`. **M, T**
* A client disconnect removes its subscriptions with the connection. **T**
* Slow consumer: a subscriber whose outbound pending exceeds `max_pending` is
  closed **with no bytes on the wire** (measured as an immediate close, frequently
  an RST because data was queued unread). At 75 % of `max_pending` the reference
  opens a *stall gate* and the publishing side waits instead of queueing
  (`go:client.go:2654`). Other connections are unaffected either way. **M**
* `write_deadline` (10 s default), **measured on a wedged socket** (Task 17
  Step 4; probe `p17`/`p17c`, a subscriber that sets a 1 KiB `SO_RCVBUF` before
  `connect` and then never reads, so the server's `writev` blocks with the
  kernel's ~985 KiB send buffer full and the peer's window at zero):
  * The victim receives **no `-ERR` of any kind**. Whatever the server managed
    to queue into the socket is delivered (measured: 987 270 bytes = 240
    `MSG ` frames of 4 096 bytes, identical for both binaries), then FIN. The
    client's read returns data to EOF; nothing distinguishes this close from any
    other except that it came unasked. **M**
  * It fires one `write_deadline` after the write stopped making progress —
    logged by the reference as `Slow Consumer Detected: WriteDeadline of 1s
    exceeded with 9 chunks of 32936 total bytes.` and, on the next line,
    `Client connection closed: Slow Consumer (Write Deadline)`. So the close
    reason is the pending-bytes family, not a protocol error. **M**
  * The server survives it: a fresh connection completes `PING`/`PONG` in the
    same second, and the publisher's own connection is untouched. **M**
  * **Which of the two limits fires first is measurable and they differ.** With
    `max_pending: 8388608` and a 47 MiB publish into the wedged subscriber, the
    reference closes it on *pending* at 0.66 s (`MaxPending of 8388608
    Exceeded`) and then, 1.6 s later, emits a `WriteDeadline` notice for the
    same already-closed cid; its publisher is held in **20 discrete ~53 ms
    parks** spread over 1.87 s and finishes at 2.36 s. Our server parks the
    publisher in **one** wait, never reaches the pending mark because the park
    is what keeps pending under it, and closes the victim on the *deadline* at
    1.995 s, with the publisher finishing at 2.38 s. Same promises, different
    mechanism: both release the stall gate inside one `write_deadline`, both
    silent, both with the victim's byte count and the bystander round trip
    (0.5 vs 0.6 ms) indistinguishable. Recorded because the difference is
    observable in a log, and a log diff would otherwise look like a bug. **M**
  * The plan's premise holds: with `go:client.go:2030`'s "not necessarily
    closed if some bytes made it", the reference *was* closed in every trial
    here, including the ones where bytes had made it. The half-sent case needs
    a socket that drains slowly rather than not at all, which is a different
    experiment (`specs/perf-notes.md`'s blocked-writer RSS curve is closer to
    it) and is still unpinned.

## 8. Implementation notes for `crates/server`

The write model, as promised in Task 17 Step 1 — decision plus what it costs,
recorded here rather than in a code comment.

* **One reader task and one writer task per connection**, like the reference.
  The reader parses and routes inline; the writer only moves bytes to the socket.
* **The outbound queue is a channel of frames, counted in bytes by a separate
  atomic.** A frame is `head: Bytes` + optional shared `body: Bytes` + a static
  CRLF tail: delivering to N subscribers clones one payload N times and copies
  nothing. Enqueue never blocks; it classifies — under the soft mark, over the
  soft mark (the publisher must wait), over the hard mark (this subscriber is a
  slow consumer and is dropped).
* **Coalescing**: the writer drains everything already queued and issues one
  `writev`, stopping at 256 KiB or 1 000 buffers — the buffer ceiling is
  `IOV_MAX` minus headroom, because `writev` returns `EINVAL` above 1 024. It was
  64 *buffers* until it was measured: an `MSG` delivery is 3 buffers, so the old
  rule wrote once per ~21 messages where the reference writes once per ~259, and
  that single difference accounted for most of the 3.4× CPU-per-message gap
  (`specs/perf-notes.md`). Ordering cannot be affected: the drain is FIFO and the
  only reordering candidate — a partially written batch — is tracked by advancing
  the front buffer, never by requeueing behind other work. Both binaries still
  complete the fan-out bench at 10 and at 50 subscribers without tripping the
  bench's 30 s stall timer.
* **The 75 % stall waits on the publishing task**, never inside the registry lock
  and never on the subscriber's behalf. Waiting is allowed to delay *this*
  publisher's next message, which is exactly the promise we are keeping; it must
  never delay another publisher's already-matched delivery. A publisher also stops
  waiting the moment the target is closed, so a wedged subscriber cannot hold a
  producer hostage beyond its own write deadline.
  "Stops waiting" needs more than the wake-up: a closed outbox has no writer left
  to drain it, so the close zeroes the byte counter and `room()` treats "there is
  nothing to wait for" as room. Without that, the single `notify_waiters()` the
  close sends wakes a parked publisher into another sleep — one `pubsub` round in
  ~50 hung the server that way, with every thread idle (`specs/parity-log.md`
  row 12, test `a_parked_publisher_wakes_when_its_subscriber_is_dropped`).
* **A soft close drains, a hard close drops.** When a connection ends on its own
  (EOF, protocol error, stale) the writer flushes what is queued and then hangs
  up — that is how `-ERR 'Maximum Payload Violation'` reaches a client that is
  about to be dropped. A slow-consumer close is the opposite: the queue is
  discarded and the socket goes, matching what the reference puts on the wire
  (nothing).
* `server_id` is opaque; `client_id` is a process-wide counter; `version` is
  ours. Nothing on the message path allocates a `String` per subject lookup —
  the per-message allocations that remain are the frame header and the writer's
  part list, both listed as suspects in `specs/perf-notes.md`.
* **Lock order, so a future change cannot invent a cycle:**
  `conn.self_ref → registry.inner → outbox.tx`, and `route` releases
  `registry.inner` before it enqueues. Nothing takes `registry.inner` while
  holding a per-connection outbound lock.

## 9. Unknown / not pinned

* Whether a *partially* draining socket (slow, not wedged) makes the reference
  keep the connection past `write_deadline` — `go:client.go:2030` says "not
  necessarily closed if some bytes made it", and every wedged-socket trial closed
  it (§7). Needs a rate-limited victim to measure; still open.
* Max-connections wire behaviour (reject after accept, or accept-then-close):
  needs 65 536 sockets to measure honestly; the Rust server closes the socket
  after accept with no bytes, which is what `accept` limits usually look like.
  Marked unverified rather than copied.
* A control line containing a bare `\n` **inside** it — `PUB a 2\nab\r\n` — is
  still a divergence: the reference holds the incomplete line and waits for more
  bytes, we tokenise on whitespace, find a size of `ab`, and close with class C.
  Nothing in the suite or the corpus covers it (the corpus sends it: it is one of
  the 20 that became 2, and the remaining 2 are this shape with an `HPUB`). Both
  sides agree that no well-formed client sends it; it is written here rather than
  papered over.
* Whether the reference's *stall gate* (75 %) can be observed on the wire at all
  on this host: kernel buffers (up to 4 MiB) absorb small tests, so the threshold
  is only visible with a genuinely wedged subscriber. Our test asserts the
  observable half — the victim is dropped, everyone else keeps working.
* `SUB` with a subject containing whitespace: unreachable, because whitespace
  separates arguments. The Part 2 plan's "subject containing a space ⇒ class C"
  row is therefore dropped: it cannot be expressed in the protocol.
* Config surface beyond `max_payload`, `max_pending`, `ping_interval`: not
  needed by any test; the Rust server parses only what it enforces and rejects
  the rest loudly.
