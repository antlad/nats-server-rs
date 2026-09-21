//! Parser tests, in process: the state machine must agree with the contract for
//! every arity, every case, and every way a stream can be cut up.
//!
//! These are the tests a socket cannot run — a black-box test sees a close, never
//! which rule caused it.

use bytes::BytesMut;
use nats_server_rs::config::MAX_CONTROL_LINE;
use nats_server_rs::proto::{Event, HpubOutcome, Limits, Parser, ProtoError, UNKNOWN_OP};

fn feed_all(input: &[u8], lim: &Limits) -> Result<Vec<Event>, ProtoError> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(input);
    Parser::new().feed(&mut buf, lim)
}

fn events(input: &[u8]) -> Vec<Event> {
    feed_all(input, &Limits::default()).expect("should parse")
}

fn error(input: &[u8]) -> ProtoError {
    feed_all(input, &Limits::default()).expect_err("should fail")
}

// ---------------------------------------------------------------- happy path --

#[test]
fn parses_a_session_in_one_segment() {
    let got = events(b"CONNECT {\"verbose\":false}\r\nSUB foo 1\r\nPUB foo 5\r\nhello\r\nPING\r\nPONG\r\n");
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(matches!(got[0], Event::Connect(_)));
    match &got[1] {
        Event::Subscribe {
            subject,
            queue,
            sid,
        } => {
            assert_eq!(&subject[..], b"foo");
            assert!(queue.is_none());
            assert_eq!(&sid[..], b"1");
        }
        other => panic!("expected SUB, got {other:?}"),
    }
    match &got[2] {
        Event::Publish {
            subject,
            reply,
            hdr,
            total,
            body,
            hpub,
        } => {
            assert_eq!(&subject[..], b"foo");
            assert!(reply.is_none());
            assert_eq!(*hdr, 0);
            assert_eq!(*total, 5);
            assert_eq!(&body[..], b"hello");
            assert!(!hpub);
        }
        other => panic!("expected PUB, got {other:?}"),
    }
    assert!(matches!(got[3], Event::Ping));
    assert!(matches!(got[4], Event::Pong));
}

#[test]
fn sub_with_three_args_puts_the_middle_one_in_the_queue() {
    match &events(b"SUB work q1 7\r\n")[0] {
        Event::Subscribe {
            subject,
            queue,
            sid,
        } => {
            assert_eq!(&subject[..], b"work");
            assert_eq!(queue.as_deref(), Some(&b"q1"[..]));
            assert_eq!(&sid[..], b"7");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn argument_runs_of_space_and_tab_collapse() {
    // Measured: `SUB  foo  1 ` and `SUB\tfoo\t1` are ordinary SUB lines.
    for line in [
        b"SUB  foo   1 \r\n".as_slice(),
        b"SUB\tfoo\t1\r\n".as_slice(),
        b"SUB foo 1 \r\n".as_slice(),
    ] {
        match &events(line)[0] {
            Event::Subscribe { subject, sid, .. } => {
                assert_eq!(&subject[..], b"foo", "for {line:?}");
                assert_eq!(&sid[..], b"1", "for {line:?}");
            }
            other => panic!("for {line:?}: {other:?}"),
        }
    }
}

#[test]
fn verbs_are_case_insensitive_everywhere_but_the_class() {
    for line in [
        b"ping\r\n".as_slice(),
        b"PiNg\r\n".as_slice(),
        b"PONG\r\n".as_slice(),
        b"PoNg\r\n".as_slice(),
        b"sUB a 1\r\n".as_slice(),
        b"PuB a 1\r\nx\r\n".as_slice(),
        b"connect {}\r\n".as_slice(),
        b"unSub 1\r\n".as_slice(),
    ] {
        assert!(
            feed_all(line, &Limits::default()).is_ok(),
            "must parse: {line:?}"
        );
    }
    // And folding does not rescue a bad arity.
    assert_eq!(error(b"sUB a\r\n"), ProtoError::Silent);
    assert_eq!(error(b"pUb a x\r\n"), ProtoError::Silent);
}

#[test]
fn hpub_carries_both_sizes_and_the_whole_frame_body() {
    let got = events(b"HPUB hb 12 14\r\nNATS/1.0\r\n\r\nab\r\n");
    // The line announces itself first: whether an HPUB is legal is the
    // connection's business at execution, not the parser's (§3, row 5).
    assert!(
        matches!(got[0], Event::HpubAttempt(HpubOutcome::Ok)),
        "{:?}",
        got
    );
    match &got[1] {
        Event::Publish {
            subject,
            hdr,
            total,
            body,
            hpub,
            ..
        } => {
            assert_eq!(&subject[..], b"hb");
            assert_eq!(*hdr, 12);
            assert_eq!(*total, 14);
            assert_eq!(&body[..], &b"NATS/1.0\r\n\r\nab"[..]);
            assert!(hpub, "the verb matters: the capability gate is per-verb");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_publish_body_may_hold_crlf() {
    // The body is counted, not scanned: an embedded CRLF is payload.
    let got = events(b"PUB crlf 6\r\na\r\n\r\nb\r\n");
    match &got[0] {
        Event::Publish { body, total, .. } => {
            assert_eq!(*total, 6);
            assert_eq!(body, &BytesMut::from(&b"a\r\n\r\nb"[..]).freeze()[..]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn client_side_ok_and_err_are_recognised_not_invented() {
    assert!(matches!(events(b"+OK\r\n")[0], Event::ClientOk));
    assert!(matches!(events(b"-ERR whatever\r\n")[0], Event::ClientErr));
    assert!(matches!(events(b"INFO {} \r\n")[0], Event::Info(_)));
}

// ------------------------------------------------------------------ the table --

/// Every verb's arity, as one table that can be diffed against the contract.
#[test]
fn arity_table() {
    let silent = [
        // Wrong number of arguments: class C, close with no bytes.
        "SUB foo\r\n",
        "SUB a b 1 9\r\n",
        "UNSUB 1 2 3\r\n",
        "PUB foo\r\n",
        "PUB a b c 1\r\n",
        "PUB foo notanumber\r\n",
        "PUB foo -1\r\n",
        "PUB foo 18446744073709551615123\r\n", // longer than parseSize allows
        "SUB \r\n", // verb + space + nothing: a SUB with no arguments at all
    ];
    for line in silent {
        assert_eq!(error(line.as_bytes()), ProtoError::Silent, "for {line:?}");
    }

    // HPUB lines whose arguments describe no publish land in the same class C,
    // but as an event: the reference asks the capability question first, and a
    // CONNECT granting headers can be in the same read (parity-log row 5), so
    // the parser hands the close to the client instead of raising it.
    for line in ["HPUB hb 12\r\n", "HPUB hb 20 10\r\n", "HPUB hb x 14\r\n"] {
        assert!(
            matches!(
                events(line.as_bytes())[0],
                Event::HpubAttempt(HpubOutcome::Args)
            ),
            "for {line:?}: {:?}",
            events(line.as_bytes())
        );
    }

    let fatal = [
        "BOGUS\r\n",
        "\r\n",
        "PSUB a.* 1\r\n",
        "psub a.* 1\r\n",
        "A+ foo 1\r\n",
        "PUB\r\n",
        "UNSUB\r\n",
        "SUB\r\n",   // verb then end-of-line: the reference's parser wants a space
        "PUBFOO 1\r\n",
        "SUBFOO 1\r\n",
        "SUB.a 1\r\n",
    ];
    for line in fatal {
        // `PUB  foo 1` is not in fact fatal — see the note below — so skip those
        // the parser can legitimately read.
        match feed_all(line.as_bytes(), &Limits::default()) {
            Err(ProtoError::Fatal(UNKNOWN_OP)) => {}
            other => panic!("for {line:?}: expected class A, got {other:?}"),
        }
    }
}

/// `CONNECT` is a prefix too: everything after the seven letters is the options
/// object, so junk there is *parsed* as a CONNECT and fails later, in the JSON —
/// while a CONNECT with no argument at all is already a silent close. Measured.
#[test]
fn connect_takes_the_rest_of_the_line_as_its_options() {
    for line in [
        b"CONNECT {\"verbose\":true}\r\n".as_slice(),
        b"CONNECTxyz {}\r\n".as_slice(),
        b"CONNECT not json\r\n".as_slice(),
    ] {
        match feed_all(line, &Limits::default()) {
            Ok(ev) => assert!(matches!(ev[0], Event::Connect(_)), "for {line:?}"),
            Err(e) => panic!("for {line:?}: {e:?}"),
        }
    }
    for line in ["CONNECT \r\n", "CONNECT\r\n"] {
        assert_eq!(error(line.as_bytes()), ProtoError::Silent, "for {line:?}");
    }
}

/// `PING` and `PONG` swallow whatever follows them: the reference's parser states
/// for those two have no default arm, so bytes are skipped to the line end
/// (measured: `PING x` is answered with a PONG and the connection stays).
#[test]
fn ping_and_pong_ignore_trailing_junk() {
    for line in ["PING x\r\n", "PINGxyz\r\n", "PONG whatever\r\n", "ping \r\n"] {
        match feed_all(line.as_bytes(), &Limits::default()) {
            Ok(events) => assert!(
                matches!(events[0], Event::Ping | Event::Pong),
                "for {line:?}: {events:?}"
            ),
            Err(e) => panic!("for {line:?}: {e:?}"),
        }
    }
}

/// `PUB  foo 1` — two spaces after the verb — is *not* an error: the reference's
/// `OP_PUB_SPC` state skips runs of whitespace. Pinned so nobody "tightens" it.
#[test]
fn runs_of_space_after_a_verb_are_skipped() {
    let got = feed_all(b"PUB  foo 1\r\nx\r\n", &Limits::default()).expect("parses");
    match &got[0] {
        Event::Publish { subject, .. } => assert_eq!(&subject[..], b"foo"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn oversize_is_rejected_before_the_body_arrives() {
    // The declared number is enough: a server that waits for the bytes hands an
    // attacker a buffer for free.
    let big = format!(
        "PUB x {}\r\n",
        nats_server_rs::config::MAX_PAYLOAD + 1
    );
    match feed_all(big.as_bytes(), &Limits::default()) {
        Err(ProtoError::Fatal(text)) => assert_eq!(text, "Maximum Payload Violation"),
        other => panic!("expected class A-prime, got {other:?}"),
    }
}

#[test]
fn a_long_control_line_is_class_a() {
    let line = format!("SUB {} 1\r\n", "a".repeat(MAX_CONTROL_LINE * 2));
    match feed_all(line.as_bytes(), &Limits::default()) {
        Err(ProtoError::Fatal(text)) => assert_eq!(text, MAX_CONTROL_LINE_TEXT),
        other => panic!("expected a control-line error, got {other:?}"),
    }
}

const MAX_CONTROL_LINE_TEXT: &str = "maximum control line exceeded";

// ------------------------------------------------------------------- splitting --

/// Feed the same session with every possible first-cut position. Whatever the
/// TCP segment boundaries are, the parser must produce the same events, in the
/// same order, and never lose or invent an operation.
#[test]
fn split_at_every_byte_boundary_of_a_canned_session() {
    let session: &[u8] = b"CONNECT {\"verbose\":false,\"headers\":true}\r\n\
                           SUB a 1\r\nSUB b q 2\r\n\
                           PUB a 3\r\nabc\r\n\
                           HPUB b 12 14\r\nNATS/1.0\r\n\r\nab\r\n\
                           UNSUB 1\r\nPING\r\n";
    let want = events(session);
    // 8, not 7: the HPUB line contributes its capability marker alongside the
    // publish it declares. What this test is about is that the count and the
    // order do not depend on where the segment boundaries fall.
    assert_eq!(want.len(), 8, "the session itself: {want:?}");

    for cut in 0..=session.len() {
        // One buffer, two feeds: the reader keeps its buffer, the parser keeps
        // its state, and only the segment boundary moves.
        let mut parser = Parser::new();
        let mut buf = BytesMut::new();
        let mut got = Vec::new();
        for part in [&session[..cut], &session[cut..]] {
            buf.extend_from_slice(part);
            got.extend(
                parser
                    .feed(&mut buf, &Limits::default())
                    .unwrap_or_else(|e| panic!("cut at {cut}: {e:?}")),
            );
        }
        assert!(buf.is_empty(), "cut at {cut}: {} bytes left over", buf.len());
        assert_eq!(
            names(&got),
            names(&want),
            "cut at byte {cut} changed the session"
        );
    }
}

/// Byte-at-a-time delivery: the extreme case of fragmentation.
#[test]
fn one_byte_at_a_time_produces_the_same_session() {
    let session: &[u8] = b"SUB a 1\r\nPUB a 6\r\nab\r\ncd\r\nPING\r\n";
    let mut parser = Parser::new();
    let mut buf = BytesMut::new();
    let mut got = Vec::new();
    for i in 0..session.len() {
        buf.extend_from_slice(&session[i..i + 1]);
        got.extend(
            parser
                .feed(&mut buf, &Limits::default())
                .unwrap_or_else(|e| panic!("byte {i}: {e:?}")),
        );
    }
    assert!(buf.is_empty(), "the whole session was consumed");
    // The body is "ab\r\ncd" — an embedded CRLF that must not end the frame.
    match &got[1] {
        Event::Publish { body, total, .. } => {
            assert_eq!(*total, 6);
            assert_eq!(body, &BytesMut::from(&b"ab\r\ncd"[..]).freeze()[..]);
        }
        other => panic!("expected the publish, got {other:?}"),
    }
    assert!(matches!(got[2], Event::Ping), "{:?}", names(&got));
}

fn names(events: &[Event]) -> Vec<&'static str> {
    events
        .iter()
        .map(|e| match e {
            Event::Connect(_) => "connect",
            Event::Subscribe { .. } => "sub",
            Event::Unsubscribe { .. } => "unsub",
            Event::Publish { hpub, .. } => {
                if *hpub {
                    "hpub"
                } else {
                    "pub"
                }
            }
            Event::Ping => "ping",
            Event::Pong => "pong",
            Event::Info(_) => "info",
            Event::ClientOk => "ok",
            Event::ClientErr => "err",
            Event::HpubAttempt(_) => "hpub-attempt",
        })
        .collect()
}

// ---------------------------------------------------------------- no panics ----

/// A corpus of hostile input: none of it may panic the parser. A panic in a
/// connection task must not be able to take the process with it, and the only
/// sane response to garbage is an error with a class.
#[test]
fn the_corpus_never_panics() {
    let corpus: &[&[u8]] = &[
        b"",
        b"\r",
        b"\n",
        b"\r\n\r\n\r\n",
        b"X",
        b"X\r\n",
        b"PUB\r\n",
        b"PUB ",
        b"PUB \r\n",
        b"PUB a\r\n",
        b"PUB a \r\n",
        b"PUB a b c d e f\r\n",
        b"PUB a 99999999999999999999\r\n",
        b"PUB a 1\r\n",
        b"PUB a 1\r\nx",
        b"PUB a 1\r\nxx\r\n",
        b"PUB a 1\r\nx\r\nx\r\n",
        b"HPUB\r\n",
        b"HPUB a\r\n",
        b"HPUB a 1\r\n",
        b"HPUB a -1 -1\r\n",
        b"HPUB a 0 0\r\n\r\n",
        b"HPUB a 5 3\r\nabcdef\r\n",
        b"SUB\r\n",
        b"SUB \r\n",
        b"SUB  \r\n",
        b"SUB a\r\n",
        b"SUB a b\r\n",
        b"SUB a b c d\r\n",
        b"SUB \t\r\n",
        b"UNSUB\r\n",
        b"UNSUB \r\n",
        b"UNSUB a b c\r\n",
        b"CONNECT",
        b"CONNECT \r\n",
        b"CONNECT not-json\r\n",
        b"CONNECT []\r\n",
        b"CONNECT {\"verbose\":1}\r\n",
        b"connect\r\n",
        b"PINGPING\r\n",
        b"ping \r\n",
        b"+OK\r\n",
        b"-ERR\r\n",
        b"INFO\r\n",
        b"\x00\x00\r\n",
        b"a b c d e f g h\r\n",
        &b"PUB \xe2\x82\xac 1\r\nx\r\n"[..],
        &b"SUB \xe2\x82\xac 1\r\n"[..],
    ];
    for case in corpus {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(case);
        if let Ok(events) = Parser::new().feed(&mut buf, &Limits::default()) {
                // Truncated input is allowed to produce nothing, never a partial op.
            for ev in &events {
                if let Event::Publish { total, body, .. } = ev {
                    assert_eq!(&body.len(), total, "partial frame in {case:?}");
                }
            }
        }
    }
}

/// The pre-CONNECT ordering rule: commands before a CONNECT are processed in
/// order, and the CONNECT does not jump the queue.
#[tokio::test]
async fn events_keep_their_wire_order() {
    let got = events(b"SUB a 1\r\nCONNECT {\"verbose\":false}\r\nPING\r\n");
    assert_eq!(
        names(&got),
        vec!["sub", "connect", "ping"],
        "a mid-stream CONNECT must not reorder what came before it"
    );
}
