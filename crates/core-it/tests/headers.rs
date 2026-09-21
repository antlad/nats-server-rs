//! Headers: `HPUB`/`HMSG` framing, the capability gate, and the 503
//! no-responder status frame.
//!
//! The byte-for-byte assertions here are the reason `HPUB hb 12 14` is worth
//! writing out in full: `12` is `NATS/1.0\r\n` (10) plus the block terminator
//! `\r\n` (2), and `14` adds the two payload bytes. A server that counts the
//! terminator differently produces the same *shape* of frame and a different
//! number, which no looser test would catch.

use core_it::wire::{
    header_block, hpub_frame, pub_frame, Conn, Next, CAPS, CLOSED, IO, NO_VERBOSE, QUIET,
};
use nats_test_harness::Server;

/// A client that declares both capabilities, as async-nats does.
async fn caps_session(srv: &Server) -> Conn {
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(CAPS).await;
    c
}

async fn plain_session(srv: &Server) -> Conn {
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    c
}

/// Subscribe on `c` and make sure the server has registered it.
async fn subscribe(c: &mut Conn, spec: &str) {
    c.send(format!("SUB {spec}\r\n").as_bytes()).await;
    c.barrier().await;
}

#[tokio::test]
async fn hpub_with_empty_header_block_delivers_hmsg_verbatim() {
    let srv = Server::start().unwrap();
    let mut sub = caps_session(&srv).await;
    subscribe(&mut sub, "hb 1").await;
    let mut p = caps_session(&srv).await;
    p.send(&hpub_frame("hb", None, &header_block(&[]), b"ab"))
        .await;

    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"HMSG hb 1 12 14\r\n");
    assert_eq!(
        sub.read_msg_body(14, IO).await,
        b"NATS/1.0\r\n\r\nab",
        "header block then payload, untouched"
    );
}

#[tokio::test]
async fn hpub_with_header_lines_delivers_hmsg_verbatim() {
    let srv = Server::start().unwrap();
    let mut sub = caps_session(&srv).await;
    subscribe(&mut sub, "hb 1").await;
    let mut p = caps_session(&srv).await;
    let hdr = header_block(&["X-Key: value"]);
    assert_eq!(hdr.len(), 26, "the block length is part of the contract");
    p.send(&hpub_frame("hb", None, &hdr, b"abc")).await;

    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"HMSG hb 1 26 29\r\n");
    assert_eq!(
        sub.read_msg_body(29, IO).await,
        b"NATS/1.0\r\nX-Key: value\r\n\r\nabc"
    );
}

/// Four `HPUB` args are `subject reply #hdr #total`, and the delivered frame
/// carries the reply between the sid and the sizes.
#[tokio::test]
async fn hpub_with_reply_uses_the_six_field_hmsg() {
    let srv = Server::start().unwrap();
    let mut sub = caps_session(&srv).await;
    subscribe(&mut sub, "hpr 1").await;
    let mut p = caps_session(&srv).await;
    p.send(&hpub_frame(
        "hpr",
        Some("reply.me"),
        &header_block(&[]),
        b"ab",
    ))
    .await;

    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"HMSG hpr 1 reply.me 12 14\r\n");
    assert_eq!(sub.read_msg_body(14, IO).await, b"NATS/1.0\r\n\r\nab");
}

/// A header publish from a client that never declared `headers: true` is a parse
/// error (class C): close, no `-ERR`. Being helpful here would be a divergence.
#[tokio::test]
async fn hpub_without_the_headers_capability_closes_silently() {
    let srv = Server::start().unwrap();
    let mut c = plain_session(&srv).await;
    c.send(&hpub_frame("hb", None, &header_block(&[]), b"ab"))
        .await;
    assert_eq!(c.drain(QUIET).await, b"", "class C sends no bytes");
    assert!(c.closed(CLOSED).await);
}

/// A subscriber that does not support headers still gets the message: the server
/// strips the block and delivers a plain `MSG`.
#[tokio::test]
async fn headers_are_stripped_for_a_subscriber_without_the_capability() {
    let srv = Server::start().unwrap();
    let mut sub = plain_session(&srv).await;
    subscribe(&mut sub, "strip 1").await;
    let mut p = caps_session(&srv).await;
    p.send(&hpub_frame(
        "strip",
        None,
        &header_block(&["X-Key: value"]),
        b"abc",
    ))
    .await;

    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"MSG strip 1 3\r\n", "header block removed");
    assert_eq!(sub.read_msg_body(3, IO).await, b"abc");
}

#[tokio::test]
async fn headers_survive_wildcard_matching() {
    let srv = Server::start().unwrap();
    let mut sub = caps_session(&srv).await;
    subscribe(&mut sub, "foo.* 1").await;
    let mut p = caps_session(&srv).await;
    p.send(&hpub_frame("foo.bar", None, &header_block(&[]), b"ab"))
        .await;
    let line = sub.next(IO).await;
    assert_eq!(
        line.line(),
        b"HMSG foo.bar 1 12 14\r\n",
        "the delivered subject is the published one"
    );
    assert_eq!(sub.read_msg_body(14, IO).await, b"NATS/1.0\r\n\r\nab");
}

/// A plain `PUB` never becomes an `HMSG`, whatever the subscriber declared.
#[tokio::test]
async fn plain_publish_is_msg_even_for_a_headers_capable_subscriber() {
    let srv = Server::start().unwrap();
    let mut sub = caps_session(&srv).await;
    subscribe(&mut sub, "plain 1").await;
    let mut p = caps_session(&srv).await;
    p.send(&pub_frame("plain", None, b"abc")).await;
    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"MSG plain 1 3\r\n");
    assert_eq!(sub.read_msg_body(3, IO).await, b"abc");
}

#[tokio::test]
async fn hpub_self_delivery_keeps_headers() {
    let srv = Server::start().unwrap();
    let mut c = caps_session(&srv).await;
    subscribe(&mut c, "se 1").await;
    c.send(&hpub_frame("se", None, &header_block(&[]), b"ab"))
        .await;
    let line = c.next(IO).await;
    assert_eq!(line.line(), b"HMSG se 1 12 14\r\n");
    assert_eq!(c.read_msg_body(14, IO).await, b"NATS/1.0\r\n\r\nab");
}

#[tokio::test]
async fn malformed_hpub_control_lines_close_silently() {
    let srv = Server::start().unwrap();
    for line in [
        "HPUB z 12\r\n",        // one size is not enough
        "HPUB z 20 10\r\n",     // header larger than the total
        "HPUB z x 14\r\n",      // non-numeric
        "HPUB z r 12 14 9\r\n", // five args
    ] {
        let mut c = caps_session(&srv).await;
        subscribe(&mut c, "z 1").await;
        c.send(line.as_bytes()).await;
        c.send(&header_block(&[])).await;
        assert_eq!(c.drain(QUIET).await, b"", "for {line:?}");
        assert!(c.closed(CLOSED).await, "for {line:?}");
    }
}

/// A zero-length header block means "no headers": the frame is delivered as MSG.
#[tokio::test]
async fn hpub_with_zero_header_block_delivers_msg() {
    let srv = Server::start().unwrap();
    let mut sub = caps_session(&srv).await;
    subscribe(&mut sub, "z0 1").await;
    let mut p = caps_session(&srv).await;
    p.send(b"HPUB z0 0 2\r\nab\r\n").await;
    let line = sub.next(IO).await;
    assert_eq!(line.line(), b"MSG z0 1 2\r\n");
    assert_eq!(sub.read_msg_body(2, IO).await, b"ab");
}

// ---------------------------------------------------------- size accounting --

/// The header block counts against `max_payload` together with the payload: the
/// client library enforces the sum, so a server that counts only the body would
/// disagree with every client about which side errors.
#[tokio::test]
async fn header_bytes_count_toward_max_payload() {
    let srv = Server::start_with_config("max_payload: 1024\n").unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    assert_eq!(
        c.info["max_payload"].as_u64(),
        Some(1024),
        "the limit under test must be the configured one"
    );
    c.connect_opts(CAPS).await;

    // 12 (header) + 1012 (body) == 1024: exactly at the limit, must pass.
    c.send(&hpub_frame(
        "fit",
        None,
        &header_block(&[]),
        &vec![b'x'; 1012],
    ))
    .await;
    assert_eq!(c.drain(QUIET).await, b"", "at the limit is not over it");
    assert!(!c.closed(CLOSED).await);

    // One byte more is class A-prime: -ERR then close.
    c.send(&hpub_frame(
        "over",
        None,
        &header_block(&[]),
        &vec![b'x'; 1013],
    ))
    .await;
    let seen = c.drain(QUIET).await;
    assert_eq!(seen, b"-ERR 'Maximum Payload Violation'\r\n");
    assert!(c.closed(CLOSED).await);
}

// ------------------------------------------------------------ no responders --

/// The status frame a `no_responders` publisher gets back, byte for byte. It is
/// delivered *on the reply subject*, so that is the frame's subject; the subject
/// that went unanswered travels in the `Nats-Subject` header. The block is
/// `NATS/1.0 503` + that header + the terminator = 32 + len(published) bytes,
/// with no payload.
async fn expect_no_responder_status(c: &mut Conn, inbox: &str, published: &str, sid: &str) {
    let hdr = format!("NATS/1.0 503\r\nNats-Subject: {published}\r\n\r\n");
    assert_eq!(hdr.len(), 32 + published.len(), "the size promise");
    let n = hdr.len();
    let line = c.next(IO).await;
    assert_eq!(
        line.line(),
        format!("HMSG {inbox} {sid} {n} {n}\r\n").as_bytes(),
        "503 status frame header"
    );
    assert_eq!(c.read_msg_body(n, IO).await, hdr.as_bytes());
}

#[tokio::test]
async fn unanswered_request_gets_the_503_status_frame() {
    let srv = Server::start().unwrap();
    let mut c = caps_session(&srv).await;
    subscribe(&mut c, "inbox.1 1").await;
    c.send(&hpub_frame(
        "svc.echo",
        Some("inbox.1"),
        &header_block(&[]),
        b"hi",
    ))
    .await;
    expect_no_responder_status(&mut c, "inbox.1", "svc.echo", "1").await;
    assert!(!c.closed(CLOSED).await, "the connection must stay usable");

    // A plain (header-less) publish from a no_responders client gets the same
    // treatment: the capability comes from CONNECT, not from HPUB.
    c.send(&pub_frame("svc.echo", Some("inbox.1"), b"hi")).await;
    expect_no_responder_status(&mut c, "inbox.1", "svc.echo", "1").await;
}

#[tokio::test]
async fn no_503_when_a_subscriber_exists() {
    let srv = Server::start().unwrap();
    let mut caller = caps_session(&srv).await;
    subscribe(&mut caller, "inbox.2 1").await;
    let mut responder = caps_session(&srv).await;
    subscribe(&mut responder, "svc.echo 1").await;

    caller
        .send(&hpub_frame(
            "svc.echo",
            Some("inbox.2"),
            &header_block(&[]),
            b"hi",
        ))
        .await;
    let line = responder.next(IO).await;
    assert_eq!(line.line(), b"HMSG svc.echo 1 inbox.2 12 14\r\n");
    responder.read_msg_body(14, IO).await;
    let seen = caller.drain(QUIET).await;
    assert_eq!(seen, b"", "a delivered request must not also get a 503");
}

/// One member of a queue group answering counts as delivered: no 503.
#[tokio::test]
async fn no_503_when_a_queue_group_member_exists() {
    let srv = Server::start().unwrap();
    let mut caller = caps_session(&srv).await;
    subscribe(&mut caller, "inbox.3 1").await;
    let mut worker = caps_session(&srv).await;
    subscribe(&mut worker, "svc.q g1 1").await;

    caller
        .send(&hpub_frame(
            "svc.q",
            Some("inbox.3"),
            &header_block(&[]),
            b"hi",
        ))
        .await;
    let line = worker.next(IO).await;
    assert_eq!(line.line(), b"HMSG svc.q 1 inbox.3 12 14\r\n");
    worker.read_msg_body(14, IO).await;
    assert_eq!(caller.drain(QUIET).await, b"", "queue hit means delivered");
}

/// Without the capability the publisher asked for nothing: silence, not a 503.
#[tokio::test]
async fn no_503_without_the_no_responders_capability() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(r#"{"verbose":false,"headers":true,"no_responders":false}"#)
        .await;
    subscribe(&mut c, "inbox.4 1").await;
    c.send(&hpub_frame(
        "svc.none",
        Some("inbox.4"),
        &header_block(&[]),
        b"hi",
    ))
    .await;
    assert_eq!(c.drain(QUIET).await, b"");
}

/// The status goes to the publisher's *subscription* on the reply subject. With
/// none, there is nowhere to put it, and the server says nothing.
#[tokio::test]
async fn no_503_when_the_publisher_did_not_subscribe_its_inbox() {
    let srv = Server::start().unwrap();
    let mut c = caps_session(&srv).await;
    c.send(&hpub_frame(
        "svc.none",
        Some("inbox.5"),
        &header_block(&[]),
        b"hi",
    ))
    .await;
    assert_eq!(c.drain(QUIET).await, b"");
    assert!(!c.closed(CLOSED).await, "and the connection is fine");
}

/// A request/reply round trip with headers on both legs: what the responder
/// receives, and what the caller gets back.
#[tokio::test]
async fn request_reply_round_trip_with_headers() {
    let srv = Server::start().unwrap();
    let mut caller = caps_session(&srv).await;
    subscribe(&mut caller, "_in.1 1").await;
    let mut responder = caps_session(&srv).await;
    subscribe(&mut responder, "svc.sum 1").await;

    let req_hdr = header_block(&["X-Op: sum"]);
    caller
        .send(&hpub_frame("svc.sum", Some("_in.1"), &req_hdr, b"7"))
        .await;
    let line = responder.next(IO).await;
    assert_eq!(
        line.line(),
        format!(
            "HMSG svc.sum 1 _in.1 {} {}\r\n",
            req_hdr.len(),
            req_hdr.len() + 1
        )
        .as_bytes()
    );
    let mut frame = responder.read_msg_body(req_hdr.len() + 1, IO).await;
    frame.extend_from_slice(&[req_hdr.as_slice(), b"7"].concat());
    assert!(
        frame.starts_with(b"NATS/1.0\r\nX-Op: sum\r\n\r\n"),
        "got {frame:?}"
    );

    // The reply travels as an ordinary publish to the inbox subject.
    let resp_hdr = header_block(&["X-Status: ok"]);
    responder
        .send(&hpub_frame("_in.1", None, &resp_hdr, b"42"))
        .await;
    let line = caller.next(IO).await;
    assert_eq!(
        line.line(),
        format!("HMSG _in.1 1 {} {}\r\n", resp_hdr.len(), resp_hdr.len() + 2).as_bytes()
    );
    assert_eq!(
        caller.read_msg_body(resp_hdr.len() + 2, IO).await,
        [resp_hdr.as_slice(), b"42"].concat()
    );
}

/// Nothing is left half-written on the publisher's connection when a request
/// goes unanswered: after the 503 the connection is clean.
#[tokio::test]
async fn unanswered_request_leaves_no_partial_frame() {
    let srv = Server::start().unwrap();
    let mut c = caps_session(&srv).await;
    subscribe(&mut c, "inbox.6 1").await;
    c.send(&hpub_frame(
        "svc.gone",
        Some("inbox.6"),
        &header_block(&[]),
        b"hi",
    ))
    .await;
    expect_no_responder_status(&mut c, "inbox.6", "svc.gone", "1").await;
    assert_eq!(c.drain(QUIET).await, b"", "the 503 is the whole response");

    // And it still routes normally afterwards.
    c.send(&pub_frame("inbox.6", None, b"tail")).await;
    let line = c.next(IO).await;
    assert!(
        matches!(&line, Next::Line(l) if l.starts_with(b"MSG inbox.6 1 4\r\n")),
        "got {line:?}"
    );
    assert_eq!(c.read_msg_body(4, IO).await, b"tail");
}
