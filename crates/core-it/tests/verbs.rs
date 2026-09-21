//! Verb-by-verb error taxonomy: which control lines are accepted, what comes
//! back, and what happens to the connection.
//!
//! Every assertion here was read off the reference binary first
//! (`specs/protocol-contract.md` carries the measurements with dates). The three
//! classes are:
//!
//! * **A** — the operation is unknown: `-ERR 'Unknown Protocol Operation'` and
//!   the connection closes.
//! * **B** — the operation parsed but means nothing: `-ERR 'Invalid Subject'` /
//!   `-ERR 'Invalid Publish Subject'` and the connection **stays usable**.
//! * **C** — the control line cannot be parsed at all: the connection closes
//!   **with no bytes**.
//!
//! A fourth shape exists for size violations (**A-prime**): `-ERR` plus close.
//! The `max_payload` case lives in `wire.rs`, the control-line case is here.

use core_it::wire::{pub_frame, Conn, Next, CAPS, CLOSED, IO, NO_VERBOSE, QUIET};
use nats_test_harness::Server;

async fn session(srv: &Server) -> Conn {
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    c
}

/// Send `bytes`, then report what arrived during a quiet window and whether the
/// server hung up.
async fn observe(c: &mut Conn, bytes: &[u8]) -> (Vec<u8>, bool) {
    c.send(bytes).await;
    let seen = c.drain(QUIET).await;
    let closed = c.closed(CLOSED).await;
    (seen, closed)
}

// ---------------------------------------------------------------- class A ----

#[tokio::test]
async fn unknown_verb_is_class_a() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"BOGUS\r\n").await;
    assert_eq!(seen, b"-ERR 'Unknown Protocol Operation'\r\n");
    assert!(closed, "class A must close the connection");
}

#[tokio::test]
async fn empty_control_line_is_class_a() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"\r\n").await;
    assert_eq!(seen, b"-ERR 'Unknown Protocol Operation'\r\n");
    assert!(closed);
}

/// This reference build has no PSUB at all -- `SUB` takes wildcards -- so an
/// unknown-protocol close is parity, not a shortcut. See the contract.
#[tokio::test]
async fn psub_variants_are_class_a() {
    let srv = Server::start().unwrap();
    for verb in [
        b"PSUB foo 1\r\n".as_slice(),
        b"psub foo 1\r\n".as_slice(),
        b"A+ foo 1\r\n".as_slice(),
    ] {
        let mut c = session(&srv).await;
        let (seen, closed) = observe(&mut c, verb).await;
        assert_eq!(
            seen, b"-ERR 'Unknown Protocol Operation'\r\n",
            "for {verb:?}"
        );
        assert!(closed, "for {verb:?}");
    }
}

#[tokio::test]
async fn pub_without_args_is_class_a() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"PUB\r\n").await;
    assert_eq!(seen, b"-ERR 'Unknown Protocol Operation'\r\n");
    assert!(closed);
}

#[tokio::test]
async fn unsub_without_args_is_class_a() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"UNSUB\r\n").await;
    assert_eq!(seen, b"-ERR 'Unknown Protocol Operation'\r\n");
    assert!(closed);
}

/// A control line longer than `max_control_line` (4096 by default) gets an -ERR
/// naming the limit, then the connection closes.
#[tokio::test]
async fn oversized_control_line_closes_with_err() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let line = format!("SUB {} 1\r\n", "a".repeat(5000));
    let (seen, closed) = observe(&mut c, line.as_bytes()).await;
    assert!(
        seen.starts_with(b"-ERR '") && seen.windows(12).any(|w| w == b"control line"),
        "got {seen:?}"
    );
    assert!(closed, "oversized control line must close");
}

// ---------------------------------------------------------------- class B ----

#[tokio::test]
async fn subscribe_with_invalid_subject_is_class_b() {
    let srv = Server::start().unwrap();
    for subj in ["foo..bar", ".foo", "foo.", "foo.>.bar"] {
        let mut c = session(&srv).await;
        let (seen, closed) = observe(&mut c, format!("SUB {subj} 1\r\n").as_bytes()).await;
        assert_eq!(seen, b"-ERR 'Invalid Subject'\r\n", "for subject {subj:?}");
        assert!(!closed, "class B must keep the connection for {subj:?}");
        // ...and it must still work.
        c.send(b"PING\r\n").await;
        assert_eq!(c.next(IO).await.line(), b"PONG\r\n");
    }
}

/// `>` is only legal as the last token; `*`/`>` only as whole tokens. A token
/// that merely *contains* a wildcard char is a literal token, not an error.
#[tokio::test]
async fn subscribe_wildcard_placement_rules() {
    let srv = Server::start().unwrap();
    for (subj, expect_err) in [
        ("foo.>", false),
        (">", false),
        ("*", false),
        ("foo.*x", false), // literal token "*x": legal, matches nothing useful
        ("foo.>.bar", true),
        ("foo..bar", true),
    ] {
        let mut c = session(&srv).await;
        let (seen, closed) = observe(&mut c, format!("SUB {subj} 1\r\n").as_bytes()).await;
        if expect_err {
            assert_eq!(seen, b"-ERR 'Invalid Subject'\r\n", "for {subj:?}");
        } else {
            assert_eq!(seen, b"", "for {subj:?}");
        }
        assert!(!closed, "for {subj:?}");
    }
}

#[tokio::test]
async fn publish_on_wildcard_subject_is_class_b() {
    let srv = Server::start().unwrap();
    // Note: `pedantic` defaults to *true* in the reference (absent keys keep the
    // seeded default), so a bare CONNECT still validates publish subjects.
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, &pub_frame("foo.*", None, b"x")).await;
    assert_eq!(seen, b"-ERR 'Invalid Publish Subject'\r\n");
    assert!(!closed);
    c.send(b"PING\r\n").await;
    assert_eq!(c.next(IO).await.line(), b"PONG\r\n");
}

#[tokio::test]
async fn publish_with_invalid_subject_is_class_b() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, &pub_frame("foo..bar", None, b"x")).await;
    assert_eq!(seen, b"-ERR 'Invalid Publish Subject'\r\n");
    assert!(!closed);
}

/// A client that opts out of pedantic mode is not nagged about a wildcard
/// publish subject -- the message is simply dropped, silently.
#[tokio::test]
async fn publish_on_wildcard_subject_is_silent_when_not_pedantic() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(r#"{"verbose":false,"pedantic":false}"#)
        .await;
    c.send(b"SUB other 1\r\n").await;
    let (seen, closed) = observe(&mut c, &pub_frame("foo.*", None, b"x")).await;
    assert_eq!(seen, b"", "pedantic:false must not warn");
    assert!(!closed);
}

// ---------------------------------------------------------------- class C ----

#[tokio::test]
async fn malformed_control_lines_close_without_bytes() {
    let srv = Server::start().unwrap();
    let cases: &[&str] = &[
        "SUB foo\r\n",            // no sid
        "SUB a b 1 9\r\n",        // 4 args
        "UNSUB 1 2 3\r\n",        // 3 args
        "PUB foo\r\n",            // no size
        "PUB foo notanumber\r\n", // size not numeric
        "PUB foo -1\r\n",         // negative size
        "PUB a b c 1\r\n",        // 4 args
    ];
    for case in cases {
        let mut c = session(&srv).await;
        let (seen, closed) = observe(&mut c, case.as_bytes()).await;
        assert_eq!(seen, b"", "for {case:?}: class C sends no bytes");
        assert!(closed, "for {case:?}: class C must close");
    }
}

/// `PUB subj N` where the body never arrives: the server waits (it is mid-frame),
/// so nothing is said and nothing is delivered. A partial frame must never be
/// routed as if it were complete.
#[tokio::test]
async fn truncated_publish_body_stays_silent_and_open() {
    let srv = Server::start().unwrap();
    let mut sub = session(&srv).await;
    sub.send(b"SUB part 1\r\n").await;
    sub.barrier().await;

    let mut pubc = session(&srv).await;
    pubc.send(b"PUB part 6\r\nab").await;
    let seen = pubc.drain(QUIET).await;
    assert_eq!(
        seen, b"",
        "server must not complain about an in-flight frame"
    );
    let seen = sub.drain(QUIET).await;
    assert_eq!(seen, b"", "an incomplete publish must not be delivered");

    // Finishing the frame later makes it a normal message: retained, not dropped.
    pubc.send(b"cdef\r\n").await;
    let got = sub.next(IO).await;
    assert_eq!(got.line(), b"MSG part 1 6\r\n");
    assert_eq!(sub.read_msg_body(6, IO).await, b"abcdef");
}

// ------------------------------------------------------ arity and identity --

/// Three `SUB` args are `subject queue sid` -- the middle term is a queue group,
/// never a message limit. Auto-unsub lives in `UNSUB`.
#[tokio::test]
async fn sub_third_arg_is_the_queue_not_a_limit() {
    let srv = Server::start().unwrap();
    let mut a = session(&srv).await;
    a.send(b"SUB q3 1 3\r\n").await; // subject q3, queue "1", sid "3"
    a.barrier().await;
    let mut b = session(&srv).await;
    b.send(b"SUB q3 1 7\r\n").await; // same queue group, different sid
    b.barrier().await;

    let mut p = session(&srv).await;
    for i in 0..20u8 {
        p.send(&pub_frame("q3", None, &[b'a' + i])).await;
    }
    // A PONG from the publisher proves all 20 PUBs were processed, so the only
    // thing left to wait for is socket delivery.
    p.barrier().await;
    let short = std::time::Duration::from_millis(800);
    let mut got = Vec::new();
    for (name, c) in [("a", &mut a), ("b", &mut b)] {
        while let Next::Line(l) = c.next(short).await {
            assert!(l.starts_with(b"MSG q3 "), "{name} got {l:?}");
            let sid = String::from_utf8_lossy(&l)
                .split(' ')
                .nth(2)
                .unwrap()
                .chars()
                .next()
                .unwrap();
            let body = c.read_msg_body(1, IO).await;
            got.push((name, sid, body[0]));
        }
    }
    assert_eq!(got.len(), 20, "exactly one delivery per message: {got:?}");
    assert!(
        got.iter().filter(|(n, _, _)| *n == "a").count() > 0
            && got.iter().filter(|(n, _, _)| *n == "b").count() > 0,
        "both queue members must be reached: {got:?}"
    );
    for &(_, sid, _) in got.iter().filter(|(n, _, _)| *n == "a") {
        assert_eq!(sid, '3', "a's sid is the third arg");
    }
    for &(_, sid, _) in got.iter().filter(|(n, _, _)| *n == "b") {
        assert_eq!(sid, '7', "b's sid is the third arg");
    }
}

/// `PUB subject reply size` is the three-arg form.
#[tokio::test]
async fn pub_three_args_is_subject_reply_size() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    c.send(b"SUB rr 1\r\n").await;
    c.barrier().await;
    c.send(&pub_frame("rr", Some("reply.me"), b"hi")).await;
    let line = c.next(IO).await;
    assert_eq!(line.line(), b"MSG rr 1 reply.me 2\r\n");
    assert_eq!(c.read_msg_body(2, IO).await, b"hi");
}

/// Re-using an sid replaces nothing: the original subscription keeps serving and
/// the second SUB is ignored (measured twice, see the contract).
#[tokio::test]
async fn reusing_an_sid_keeps_the_original_subscription() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    c.send(b"SUB first 1\r\nSUB second 1\r\n").await;
    c.barrier().await;

    let mut p = session(&srv).await;
    p.send(&pub_frame("first", None, b"F")).await;
    p.send(&pub_frame("second", None, b"S")).await;

    let line = c.next(IO).await;
    assert_eq!(line.line(), b"MSG first 1 1\r\n", "the original wins");
    assert_eq!(c.read_msg_body(1, IO).await, b"F");
    let seen = c.drain(QUIET).await;
    assert_eq!(
        seen, b"",
        "the second subject must not be delivered on that sid"
    );
}

#[tokio::test]
async fn same_sid_same_subject_delivers_once() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    c.send(b"SUB dup 1\r\nSUB dup 1\r\n").await;
    c.barrier().await;
    let mut p = session(&srv).await;
    p.send(&pub_frame("dup", None, b"x")).await;
    let line = c.next(IO).await;
    assert_eq!(line.line(), b"MSG dup 1 1\r\n");
    assert_eq!(c.read_msg_body(1, IO).await, b"x");
    assert_eq!(c.drain(QUIET).await, b"", "no double delivery");
}

// -------------------------------------------------------------- case folding --

/// Verbs are matched case-insensitively on the reference
/// (measured: `SuB`, `PuB`, `PiNg`, `connect` all behave normally).
#[tokio::test]
async fn verbs_are_case_insensitive() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    // A lowercase CONNECT that takes effect proves both halves at once: the verb
    // matched, and its options were applied (verbose:true means it is answered).
    c.send(b"connect {\"verbose\":true}\r\n").await;
    c.next(IO).await.expect_line(b"+OK\r\n");

    c.send(b"PiNg\r\n").await;
    c.next(IO).await.expect_line(b"PONG\r\n");

    c.send(b"SuB mixed 1\r\n").await;
    c.next(IO).await.expect_line(b"+OK\r\n");
    c.send(b"PuB mixed 1\r\nz\r\n").await;
    // verbose means the PUB is acknowledged too; the ack and the delivered frame
    // are separate writes on separate paths, so accept either order.
    let mut saw_ok = false;
    let mut saw_msg = false;
    for _ in 0..2 {
        match c.next(IO).await {
            Next::Line(l) if l == b"+OK\r\n" => saw_ok = true,
            Next::Line(l) if l == b"MSG mixed 1 1\r\n" => saw_msg = true,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(saw_ok && saw_msg, "mixed-case PUB acked and routed");
    c.read_msg_body(1, IO).await;
    c.send(b"UnSuB 1\r\n").await;
    c.next(IO).await.expect_line(b"+OK\r\n");
    assert!(!c.closed(CLOSED).await);
}

/// Case folding does not change the class of an error.
#[tokio::test]
async fn case_folding_does_not_change_the_error_class() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"pUb foo notanumber\r\n").await;
    assert_eq!(seen, b"", "class C, mixed case");
    assert!(closed);

    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"sUB foo..bar 1\r\n").await;
    assert_eq!(seen, b"-ERR 'Invalid Subject'\r\n", "class B, mixed case");
    assert!(!closed);
}

// ------------------------------------------------------- commands pre-CONNECT --

#[tokio::test]
async fn commands_before_connect_are_processed() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    // No CONNECT yet: verbose defaults to true, so each command answers +OK.
    c.send(b"SUB pre 1\r\n").await;
    assert_eq!(c.next(IO).await.line(), b"+OK\r\n");
    c.send(b"PING\r\n").await;
    assert_eq!(c.next(IO).await.line(), b"PONG\r\n");

    let mut p = session(&srv).await;
    p.send(&pub_frame("pre", None, b"Y")).await;
    let line = c.next(IO).await;
    assert_eq!(line.line(), b"MSG pre 1 1\r\n", "pre-CONNECT SUB is live");
    assert_eq!(c.read_msg_body(1, IO).await, b"Y");
}

#[tokio::test]
async fn publishing_before_connect_routes_normally() {
    let srv = Server::start().unwrap();
    let mut sub = session(&srv).await;
    sub.send(b"SUB prepub 1\r\n").await;
    sub.barrier().await;

    let mut p = Conn::connect(&srv.client_addr()).await; // no CONNECT at all
    p.send(&pub_frame("prepub", None, b"P")).await;
    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"MSG prepub 1 1\r\n");
    assert_eq!(sub.read_msg_body(1, IO).await, b"P");
}

// ------------------------------------------------------- the whole corpus ------

/// Every malformed thing this suite knows about, in one run, against one server.
/// None of it may take the server down or leave it unable to accept: a panic in a
/// connection task, a leaked lock, or a registry left half-unregistered all show
/// up here and nowhere else.
#[tokio::test]
async fn the_corpus_leaves_the_server_serving() {
    let srv = Server::start().unwrap();
    let corpus: &[&str] = &[
        "\r\n",
        "BOGUS\r\n",
        "PSUB a.* 1\r\n",
        "A+ foo 1\r\n",
        "SUB\r\n",
        "SUB \r\n",
        "SUB foo\r\n",
        "SUB a b 1 2 3\r\n",
        "SUB .. 1\r\n",
        "SUB foo.>. 1\r\n",
        "UNSUB\r\n",
        "UNSUB 1 2 3\r\n",
        "PUB\r\n",
        "PUB foo\r\n",
        "PUB foo notanumber\r\n",
        "PUB foo -1\r\n",
        "PUB foo 18446744073709551615123\r\n",
        "PUB a b c d 1\r\n",
        "PUB .. 1\r\nx\r\n",
        "PUB foo.* 1\r\nx\r\n",
        "HPUB hb 12\r\n",
        "HPUB hb 20 10\r\nNATS/1.0\r\n\r\nab\r\n",
        "HPUB hb x 14\r\n",
        "CONNECT not json\r\n",
        "CONNECT []\r\n",
        "CONNECTxyz {}\r\n",
        "PING extra\r\n",
        "PONG whatever\r\n",
        "+OK\r\n",
        "INFO {}\r\n",
        "SUB ok 1\r\n",
    ];
    for line in corpus {
        // Each case gets its own connection: the interesting part is what the
        // server does *after* it, to someone else.
        let mut c = Conn::connect(&srv.client_addr()).await;
        c.send(line.as_bytes()).await;
        let _ = c.drain(QUIET).await;
        let _ = c.closed(std::time::Duration::from_millis(150)).await;
    }

    // And the server is still a server: new connection, subscribe, publish,
    // receive, in that order.
    let mut sub = session(&srv).await;
    sub.send(b"SUB alive 1\r\n").await;
    sub.barrier().await;
    let mut p = session(&srv).await;
    p.send(&pub_frame("alive", None, b"yes")).await;
    p.barrier().await;
    let line = sub.next(IO).await;
    assert_eq!(
        line.line(),
        b"MSG alive 1 3\r\n",
        "the corpus broke something it should not have"
    );
    assert_eq!(sub.read_msg_body(3, IO).await, b"yes");
}

/// The same argument one level down: a bad frame must not be able to take a
/// *connection* task with it in a way that leaves the socket half-alive. Every
/// case ends in either a clean close or a usable connection, never silence.
#[tokio::test]
async fn every_error_path_ends_somewhere() {
    let srv = Server::start().unwrap();
    for line in [
        "BOGUS\r\n",
        "SUB foo\r\n",
        "PUB foo x\r\n",
        "PUB foo 1048577\r\n",
        "HPUB a 5 2\r\n", // header larger than the total: class C
        "\r\n",
    ] {
        let mut c = session(&srv).await;
        c.send(line.as_bytes()).await;
        let seen = c.drain(QUIET).await;
        let closed = c.closed(CLOSED).await;
        assert!(
            closed || seen.is_empty(),
            "for {line:?}: a connection that survives must not have said anything, said {seen:?}"
        );
        if !closed {
            c.send(b"PING\r\n").await;
            c.next(IO).await.expect_line(b"PONG\r\n");
        }
    }
}

// ------------------------------------------- the finer rulings, measured ----
//
// These four are the residue of the 2026-09-21 differential corpus
// (`specs/tools/difffuzz.py`, 206 cases): places where the reference's
// byte-oriented parser makes a ruling the command table does not suggest. Each
// was measured against the reference before it was written down here, and each
// one used to be a diff.

/// `INFO` from a client is parsed and then dropped — and *parsed* is the whole
/// difference between silence and a close. The argument must be a JSON object
/// (an empty one is fine, a list or a number or trailing junk is not), the verb
/// is matched as a prefix so `INFO{...}` is a good line, and a line that does not
/// parse is class C: the connection goes with no bytes, exactly like a CONNECT
/// whose options do not decode.
#[tokio::test]
async fn info_from_a_client_is_parsed_and_then_dropped() {
    let srv = Server::start().unwrap();

    let good: [&[u8]; 4] = [
        b"INFO {\"a\":1}\r\n",
        b"INFO{}\r\n",
        b"INFO  {}  \r\n",
        b"info {\"x\":[1]}\r\n",
    ];
    for good in &good {
        let mut c = session(&srv).await;
        let (seen, closed) = observe(&mut c, good).await;
        assert!(seen.is_empty(), "{good:?} answered {seen:?}");
        assert!(!closed, "{good:?} closed a connection with a parseable INFO");
        c.send(b"PING\r\n").await;
        assert!(
            matches!(c.next(IO).await, Next::Line(l) if l == b"PONG\r\n"),
            "{good:?} left the connection unusable"
        );
    }

    let bad: [&[u8]; 6] = [
        b"INFO\r\n",
        b"INFO \r\n",
        b"INFO 5\r\n",
        b"INFO [1,2]\r\n",
        b"INFO {\"a\":\r\n",
        b"INFO {\"a\":1} x\r\n",
    ];
    for bad in &bad {
        let mut c = session(&srv).await;
        let (seen, closed) = observe(&mut c, bad).await;
        assert!(closed, "{bad:?} left the connection open");
        assert!(
            !has(seen.as_slice(), b"-ERR"),
            "{bad:?} complained {seen:?} instead of just hanging up"
        );
    }
}

/// A client's `-ERR` is a close, silently — but only if it looks like the verb.
/// `-ERR something` is that; `-ERR` alone is four bytes at the start of a line,
/// which the reference's parser never matched, so it is class A instead.
#[tokio::test]
async fn a_clients_err_needs_an_argument_to_be_an_err() {
    let silent = {
        let srv = Server::start().unwrap();
        let mut c = session(&srv).await;
        observe(&mut c, b"-ERR I saw you\r\n").await
    };
    assert!(silent.1, "-ERR with an argument left the connection open");
    assert!(
        !has(&silent.0, b"-ERR"),
        "-ERR with an argument replied {:?}",
        silent.0
    );

    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"-ERR\r\n").await;
    assert!(closed, "a bare -ERR left the connection open");
    assert!(
        has(seen.as_slice(), b"-ERR 'Unknown Protocol Operation'"),
        "a bare -ERR is not a verb: {seen:?}"
    );
}

/// The payload's terminator is checked *positionally*: the reference reads the
/// declared number of bytes and then looks at the one byte after them. If that
/// byte is not `\r` the stream has desynchronised and it says so at once, with
/// class A — it does not wait to see whether the second byte would have been a
/// `\n`. A frame that is merely short still waits.
#[tokio::test]
async fn the_payload_terminator_is_decided_on_the_first_byte() {
    // Declared 3, delivered "ab\r\n": the third payload byte is the '\r' and the
    // byte after the payload is '\n' — not a CRLF where one is required.
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, b"PUB a 3\r\nab\r\n").await;
    assert!(closed, "a mis-centred terminator left the connection open");
    assert!(
        has(seen.as_slice(), b"-ERR 'Unknown Protocol Operation'"),
        "the desync went unreported: {seen:?}"
    );

    // Declared 2, delivered "ab": nothing to decide yet. The connection waits,
    // and completing the frame is a normal publish, not an error.
    let mut sub = session(&srv).await;
    sub.send(b"SUB a 1\r\n").await;
    sub.barrier().await;
    let mut c = session(&srv).await;
    c.send(b"PUB a 2\r\nab").await;
    let (seen, closed) = (c.drain(QUIET).await, c.closed(CLOSED).await);
    assert!(!closed, "an incomplete payload closed the connection: {seen:?}");
    assert!(seen.is_empty(), "an incomplete payload complained: {seen:?}");
    c.send(b"\r\n").await;
    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"MSG a 1 2\r\n", "the finished frame never arrived");
    assert_eq!(sub.read_msg_body(2, IO).await, b"ab");
}

/// An HPUB is rejected where the *options* live. A client that never asked for
/// headers is dropped without a word, whatever the line's arguments looked like
/// — including arguments so broken that the same line from a client that did ask
/// gets class A. This is row 5's rule one level deeper: the parser may not answer
/// the question, because a CONNECT granting `headers` can arrive in the same read.
#[tokio::test]
async fn hpub_without_the_capability_is_dropped_whatever_the_line_says() {
    let frame = b"HPUB hb 1 2\r\nNATS/1.0\r\n\r\nab\r\n";

    // No `headers` in CONNECT: class C, even though the arguments are nonsense.
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    let (seen, closed) = observe(&mut c, frame).await;
    assert!(closed, "an HPUB from a header-less client stayed open");
    assert!(
        !has(seen.as_slice(), b"-ERR"),
        "a header-less HPUB complained {seen:?}; the reference says nothing"
    );

    // Same bytes, capability granted: the arguments are now the problem, and that
    // is class A.
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(CAPS).await;
    let (seen, closed) = observe(&mut c, frame).await;
    assert!(closed, "an HPUB with bad sizes stayed open");
    assert!(
        has(seen.as_slice(), b"-ERR"),
        "with the capability the bad sizes must be reported, got {seen:?}"
    );

    // And a well-formed HPUB from a capable client is simply a publish — to a
    // capable subscriber, which is the only kind that sees the header block
    // (headers.rs owns that half; this one is here so the marker cannot swallow
    // a legitimate publish).
    let mut sub = Conn::connect(&srv.client_addr()).await;
    sub.connect_opts(CAPS).await;
    sub.send(b"SUB hb 1\r\n").await;
    sub.barrier().await;
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(CAPS).await;
    c.send(b"HPUB hb 12 14\r\nNATS/1.0\r\n\r\nab\r\n").await;
    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"HMSG hb 1 12 14\r\n", "a well-formed HPUB did not arrive");
    assert_eq!(
        sub.read_msg_body(14, IO).await,
        &b"NATS/1.0\r\n\r\nab"[..],
        "the header block and body must travel together"
    );
}

/// `needle` anywhere in `hay`. The assertions below are about bytes on a socket,
/// and `Vec<u8>::contains` wants a slice of exactly the right shape for that.
fn has(hay: &[u8], needle: &[u8]) -> bool {
    hay.len() >= needle.len() && hay.windows(needle.len()).any(|w| w == needle)
}
