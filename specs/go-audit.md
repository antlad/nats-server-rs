# Audit: is this suite a 1:1 conversion of the reference's Core tests?

Short answer: **no, and "1:1" is not a defensible claim for a client-visible
protocol suite.** What is defensible, and what this file establishes, is that
every *externally observable* Core behaviour the reference's own tests assert is
asserted here too, except for the rows explicitly declined below with a reason.
Go's suite is mostly written against internal APIs (`client.parse`,
`server.RunServer(&Options{...})`, `Sublist` structs); a black-box suite driving
a binary cannot mirror those 1:1 and should not try — it would duplicate
coverage we cannot execute and hide the behaviours we can.

Method: read the Core-relevant Go test files listed below, extract each
*behaviour* (not each test function — several functions re-assert one behaviour),
and map it to our tests. Sources: `test/proto_test.go`, `test/verbose_test.go`,
`test/pedantic_test.go`, `test/maxpayload_test.go`, `test/ports_test.go`,
`test/port_test.go`, `test/ping_test.go`, `test/fanout_test.go`,
`test/bench_test.go`, `server/client_test.go`, `server/sublist_test.go`.

Legend: **covered** = same behaviour, same verdict, asserted here.
**covered-differently** = asserted, but through a different mechanism.
**missing → add** / **missing → decline(reason)**.

## Process, INFO, ports file

| Go behaviour | Ours | Status |
|---|---|---|
| `TestPortsFile` — file appears with the resolved port, is removed on shutdown | `harness` readiness (`starts_stops_and_exposes_url`) + `Server::start` contract | covered-differently (we never trust the file without a TCP connect) |
| `TestPortsFileReload` | — | decline: reload/SIGHUP is out of scope for Part 2 |
| `TestResolveRandomPort` — `-1` yields an ephemeral port | every test uses `-p -1`; `info_key_set_is_pinned` reads `port` back | covered |
| `TestServerInfoWithClientAdvertise` | — | decline: client_advertise is a cluster/advertise feature, out of scope |
| INFO field set + `proto:1` + `headers:true` | `wire.rs::info_key_set_is_pinned`, `info_is_a_single_line_in_the_documented_shape`, `info_on_connect_has_core_fields` | covered, and *extended* in Part 2 (the key set was previously unpinned) |
| `TestServerHeaderSupport`, `TestClientHeaderSupport` | `verbs.rs::hpub_without_the_headers_capability_closes_silently`, `headers.rs` capability tests | covered |
| Go always sends `client_id`/`client_ip`/`api_lvl`/`xkey`/`git_commit`/`go` | `wire.rs::info_key_set_is_pinned` treats them as optional; `client.rs::*` proves async-nats connects without them | covered-differently — deliberate divergence, recorded in the contract |

## Control line, verbs, error classes

| Go behaviour | Ours | Status |
|---|---|---|
| `TestProtoBasics` — verbs case-insensitive, mixed case | `verbs.rs::verbs_are_case_insensitive`, `case_folding_does_not_change_the_error_class` | covered |
| `TestProtoErr` — unknown verb ⇒ `-ERR 'Unknown Protocol Operation'` + close | `verbs.rs::unknown_verb_is_class_a`, `wire.rs::unknown_verb_gets_err_and_close` | covered |
| `TestProtoCrash` — parser must not panic on a corpus of malformed lines | `crates/server/tests/proto.rs` (corpus, in-process) + `verbs.rs::malformed_control_lines_close_without_bytes` | covered-differently: a panic in a *connection* must be visible as a close, which a black-box test can only see as "server still serving" |
| `TestPubToArgState`, `TestSubToArgState` — split-buffer (partial line across reads) | `lifecycle.rs::pipelined_commands_are_processed_in_order`, `verbs.rs::truncated_publish_body_stays_silent_and_open` | covered |
| `TestIncompletePubArg` — a PUB whose body never arrives must not be delivered | `verbs.rs::truncated_publish_body_stays_silent_and_open` | covered |
| `TestControlLineMaximums` — oversized control line ⇒ `-ERR` | `verbs.rs::oversized_control_line_closes_with_err` | covered |
| `TestDuplicateProtoSub` — SUB twice | `verbs.rs::same_sid_same_subject_delivers_once`, `reusing_an_sid_keeps_the_original_subscription` | covered |
| `TestUnsubMax`, `TestClientUnSubMax`, `TestClientAutoUnsubExactReceived`, `TestClientUnsubAfterAutoUnsub` | `lifecycle.rs::auto_unsub_delivers_exactly_max`, `auto_unsub_limit_already_reached_unsubscribes_now`, `unsub_with_an_unparseable_or_non_positive_max_unsubscribes` | covered |
| `TestClientUnSub` (plain UNSUB), unknown-sid UNSUB not an error | `verbs.rs::unsub_without_args_is_class_a`, `lifecycle.rs::unsub_on_an_unknown_sid_is_not_an_error` | covered |
| `TestQueueSub`, `TestMultipleQueueSub` | `verbs.rs::sub_third_arg_is_the_queue_not_a_limit`, `client.rs::queue_group_splits_load` | covered |
| `TestClientRemoveSubsOnDisconnect`, `TestClientDoesNotAddSubscriptionsWhenConnectionClosed` | `lifecycle.rs::disconnecting_removes_the_subscriptions`, `connection_churn_leaves_the_server_healthy` | covered |
| `TestMaxPayload` | `wire.rs::pub_over_max_payload_gets_err`, `configured_max_payload_is_advertised_and_enforced` | covered |
| `TestMaxPayloadOverrun` — size field overflows int32 → `-ERR`; overflows int64 (`parseSize` = −1) → silent disconnect | **was missing** → added: `crates/server/tests/proto.rs` (size parsing) + `verbs.rs::malformed_control_lines_close_without_bytes` covers the negative case; the int64-overflow variant is a Rust-side unit test because it is an internal limit | covered-differently |
| `TestClientLimits`, `TestClientMaxPending` | `lifecycle.rs::a_blocked_subscriber_is_dropped_not_accumulated` | covered-differently (black box: we can observe the victim's fate, not the counters) |
| `TestNoClientLeakOnSlowConsumer`, `TestPBNotIncreasedOnMaxPending` | — | decline: leak/counter assertions need internals; churn test is our proxy |
| `TestSplitSubjectQueue` | `verbs.rs::sub_third_arg_is_the_queue_not_a_limit` | covered-differently |

## CONNECT options

| Go behaviour | Ours | Status |
|---|---|---|
| `TestVerbosePing`, `TestVerboseConnect`, `TestVerbosePubSub` | `lifecycle.rs::verbose_is_on_until_a_connect_turns_it_off`, `connect_defaults_are_all_true`, `second_connect_is_answered_by_its_own_verbose`, `wire.rs::verbose_mode_sends_ok` | covered |
| `TestPedanticSub`, `TestPedanticPub` | `verbs.rs::subscribe_wildcard_placement_rules`, `publish_on_wildcard_subject_is_class_b`, `publish_on_wildcard_subject_is_silent_when_not_pedantic` | covered |
| `TestClientConnect`, `TestClientConnectProto` (second CONNECT, proto level) | `lifecycle.rs::second_connect_does_not_resend_info`, `second_connect_is_answered_by_its_own_verbose` | covered |
| `TestClientPubSubNoEcho`, `TestClientPubWithQueueSubNoEcho` | `lifecycle.rs::self_delivery_is_on_by_default`, `echo_can_be_asked_for_explicitly_or_turned_off` | covered |
| `TestClientNoResponderSupport`, `TestNoResponders` | `headers.rs::no_503_*`, `unanswered_request_gets_the_503_status_frame`, `client.rs::no_responders_error` | covered |
| `TestClientHeaderDeliverMsg`, `...StrippedMsg`, `...QueueSubStrippedMsg` | `headers.rs::hpub_*_verbatim`, `headers_are_stripped_for_a_subscriber_without_the_capability` | covered |
| `TestClientSimplePubSub`, `...WithReply`, `TestClientNoBodyPubSubWithReply` | `verbs.rs::pub_three_args_is_subject_reply_size`, `wire.rs::pub_sub_round_trip`, `client.rs::request_reply` | covered |
| `TestAsyncInfoWithSmallerMaxPayload` | — | decline: INFO-push on reload, out of scope |

## Wildcards, matching, sublist

| Go behaviour | Ours | Status |
|---|---|---|
| `TestSublistInsert`, `TestMatch*`, `TestWildcards`, `TestPartialWildcard*`, `TestRemoveOverlaps` | `crates/server/tests/subjects.rs` + `crates/server/tests/proto.rs` (in-process, exhaustive over the grammar and the arity table) | covered-differently: the sublist is internal; a black-box suite cannot enumerate it |
| `TestIsValid*` (subject grammar incl. whitespace-in-token) | `verbs.rs::subscribe_with_invalid_subject_is_class_b`, `subscribe_wildcard_placement_rules` + `subjects.rs` unit tests | covered |
| `TestTwoTokenPubMatchSingleTokenSub` — `SUB foo` must NOT match `foo.bar` | added as `wire.rs::literal_subscription_does_not_match_deeper_subjects` | covered |
| `TestWildcardCharsInLiteralSubjectWorks` — `foo.*x` is a literal token | `verbs.rs::subscribe_wildcard_placement_rules` | covered |
| `TestNoRaceHighFanoutOrdering` — ordering under high fan-out | `lifecycle.rs::pipelined_commands_are_processed_in_order`, `client.rs::flush_delivers_everything_published_before_it`, `crates/server/tests/ordering.rs::ordering_proof_100k` | covered-differently (the 100k ordering proof runs in-process, where the promise is cheap to check exactly) |
| Permissions/`sys` group/`_GR_` prefix tests | — | decline: authorization is out of scope (Part 3) |
| TLS, routes, gateways, leafnodes, accounts, JetStream tests | — | decline: out of scope per plan |
| `test/bench_test.go`, `server/benchmark_publish_test.go` | `crates/bench/*` + `benchmarks/baseline-go.md`, `baseline-rust.md` | covered-differently: our benches are the same three shapes, driven across a process boundary |

## What Part 1 got wrong, and is now corrected

Each bullet was a test that disagreed with the reference binary. The binary won;
no assertion was weakened to accommodate the Rust server.

* `Server::start_with_args`'s doc example used `--max_payload`, which the
  reference does not define (it prints usage). Replaced with a real flag, plus a
  config path (`start_with_config`) for options that only exist in the file.
* `missing_env_var_fails_clearly` returned early whenever `NATS_SERVER_BIN` was
  set, i.e. it asserted nothing in the normal run. It now re-executes the test
  binary in a child with the variable removed, so the check is real.
* `PLAN2.md` described `server_id` as a 26-char NUID. Measured: 56 chars from the
  base32 alphabet — it is the server's nkey public key. `wire.rs` now pins the
  measured shape instead of the plan's guess.
* `PLAN2.md` listed "subject containing a space ⇒ class C". Unreachable:
  whitespace separates arguments, so no such subject can be sent. Dropped from
  the contract (see §9 of `protocol-contract.md`).
* `PLAN2.md` said "unknown flag ⇒ non-zero exit". Measured: usage on stdout,
  message on stderr, **exit 0**. The test asserts what matters — the harness must
  not report readiness — which is also what a Rust server failing loudly gives.
* `PLAN2.md` left `UNSUB <sid> x` as an open question. Measured: it unsubscribes
  immediately (`parseSize` → 0), no response, no close. Pinned in
  `lifecycle.rs::unsub_with_an_unparseable_or_non_positive_max_unsubscribes`.
* `PLAN2.md` guessed the no-responder frame's subject field. Measured: the frame's
  subject is the **inbox**; the dead subject travels in `Nats-Subject:`. Pinned in
  `headers.rs`.
