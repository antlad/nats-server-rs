//! Connection lifecycle: what CONNECT switches on, how verbose/auto-unsub/
//! keepalive behave, and how a connection dies.
//!
//! These are the behaviours a client library assumes and never re-checks, so
//! they are exactly the ones a from-scratch server gets wrong quietly.

use core_it::wire::{pub_frame, Conn, Next, CLOSED, IO, NO_VERBOSE, QUIET, SHORT};
use nats_test_harness::Server;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn session(srv: &Server) -> Conn {
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    c
}

/// Publish to a subject this connection subscribes to: the only thing that
/// decides whether it comes back is `echo`.
async fn self_delivery(connect_line: &str) -> Vec<u8> {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(connect_line).await;
    c.send(b"SUB mine 1\r\n").await;
    c.barrier().await;
    c.send(&pub_frame("mine", None, b"x")).await;
    c.drain(QUIET).await
}

#[tokio::test]
async fn self_delivery_is_on_by_default() {
    // No `echo` key at all: the reference seeds echo=true and json.Unmarshal
    // leaves absent fields alone, so the client's own message comes back.
    let got = self_delivery(NO_VERBOSE).await;
    assert_eq!(got, b"MSG mine 1 1\r\nx\r\n", "echo absent means echo on");
}

#[tokio::test]
async fn echo_can_be_asked_for_explicitly_or_turned_off() {
    assert_eq!(
        self_delivery(r#"{"verbose":false,"echo":true}"#).await,
        b"MSG mine 1 1\r\nx\r\n"
    );
    assert_eq!(
        self_delivery(r#"{"verbose":false,"echo":false}"#).await,
        b"",
        "echo:false must suppress self-delivery"
    );
}

#[tokio::test]
async fn verbose_is_on_until_a_connect_turns_it_off() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.send(b"SUB v 1\r\n").await;
    c.next(IO).await.expect_line(b"+OK\r\n");

    // The same connection, now told to be quiet: nothing else is acknowledged.
    c.connect_opts(NO_VERBOSE).await;
    c.send(b"SUB v 2\r\nUNSUB 2\r\n").await;
    assert_eq!(c.drain(QUIET).await, b"", "verbose:false means no +OK");
    assert!(!c.closed(CLOSED).await);
}

/// Every CONNECT option defaults to *true* in the reference (`defaultOpts =
/// {Verbose:true, Pedantic:true, Echo:true}`), so an empty CONNECT is the noisy,
/// self-delivering, strict client — not the silent one a `false` default would
/// produce.
#[tokio::test]
async fn connect_defaults_are_all_true() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts("{}").await;
    c.next(IO).await.expect_line(b"+OK\r\n"); // CONNECT acknowledged
    c.send(b"SUB d 1\r\n").await;
    c.next(IO).await.expect_line(b"+OK\r\n"); // and SUB too
    c.send(&pub_frame("d", None, b"x")).await;
    let mut saw_ok = false;
    let mut saw_msg = false;
    for _ in 0..2 {
        match c.next(IO).await {
            Next::Line(l) if l == b"+OK\r\n" => saw_ok = true,
            Next::Line(l) if l == b"MSG d 1 1\r\n" => saw_msg = true,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(saw_ok && saw_msg, "PUB acked and self-delivered");
    assert_eq!(c.read_msg_body(1, IO).await, b"x");
}

/// A second CONNECT is legal. Its own `verbose` governs its own `+OK`: the value
/// in effect is the one that has just been parsed.
#[tokio::test]
async fn second_connect_is_answered_by_its_own_verbose() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    c.connect_opts(r#"{"verbose":true}"#).await;
    c.next(IO).await.expect_line(b"+OK\r\n");
    c.send(b"SUB s 1\r\n").await;
    c.next(IO).await.expect_line(b"+OK\r\n");

    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(r#"{"verbose":true}"#).await;
    c.next(IO).await.expect_line(b"+OK\r\n");
    c.connect_opts(NO_VERBOSE).await;
    assert_eq!(
        c.drain(QUIET).await,
        b"",
        "the new value is the one that speaks"
    );
}

#[tokio::test]
async fn second_connect_does_not_resend_info() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    assert!(!c.closed(CLOSED).await);
    c.connect_opts(NO_VERBOSE).await;
    let seen = c.drain(QUIET).await;
    assert!(
        !seen.starts_with(b"INFO "),
        "INFO is the first and only first line: {seen:?}"
    );
    assert_eq!(
        seen, b"",
        "a repeat CONNECT says nothing when verbose is off"
    );
    // The connection is still the same usable connection.
    c.send(b"PING\r\n").await;
    c.next(IO).await.expect_line(b"PONG\r\n");
}

// ------------------------------------------------------------------ unsub ----

#[tokio::test]
async fn auto_unsub_delivers_exactly_max() {
    let srv = Server::start().unwrap();
    let mut sub = session(&srv).await;
    sub.send(b"SUB au 1\r\nUNSUB 1 2\r\n").await;
    let mut p = session(&srv).await;
    for i in 0..5u8 {
        p.send(&pub_frame("au", None, &[b'0' + i])).await;
    }
    p.barrier().await;

    for i in 0..2u8 {
        let line = sub.next(IO).await;
        assert_eq!(line.line(), b"MSG au 1 1\r\n");
        assert_eq!(sub.read_msg_body(1, IO).await, [b'0' + i], "in order");
    }
    assert_eq!(
        sub.drain(QUIET).await,
        b"",
        "nothing after the auto-unsub limit"
    );
    assert!(!sub.closed(CLOSED).await, "the connection stays usable");
}

/// The limit is a *total*, not a countdown: setting it once two messages have
/// already arrived removes the subscription immediately.
#[tokio::test]
async fn auto_unsub_limit_already_reached_unsubscribes_now() {
    let srv = Server::start().unwrap();
    let mut sub = session(&srv).await;
    sub.send(b"SUB a2 1\r\n").await;
    let mut p = session(&srv).await;
    for i in 0..2u8 {
        p.send(&pub_frame("a2", None, &[b'0' + i])).await;
    }
    p.barrier().await;
    for i in 0..2u8 {
        assert_eq!(sub.next(IO).await.line(), b"MSG a2 1 1\r\n");
        assert_eq!(sub.read_msg_body(1, IO).await, [b'0' + i]);
    }

    // The UNSUB rides a different connection than the PUBs, so it needs its own
    // barrier: without one, the last publish can be routed before the server has
    // looked at the UNSUB, and that is a racing test, not a racing server.
    sub.send(b"UNSUB 1 2\r\nPING\r\n").await;
    assert_eq!(
        sub.next(IO).await.line(),
        b"PONG\r\n",
        "nothing may be delivered while the UNSUB is being processed"
    );
    p.send(&pub_frame("a2", None, b"2")).await;
    p.barrier().await;
    assert_eq!(sub.drain(QUIET).await, b"", "already at the limit");

    // A larger limit on a subscription that is already over it is a plain unsub:
    // Go only raises `max` when it is strictly above the delivered count.
    sub.send(b"UNSUB 1 9\r\n").await;
    assert!(!sub.closed(CLOSED).await);
}

#[tokio::test]
async fn unsub_with_an_unparseable_or_non_positive_max_unsubscribes() {
    for max in ["0", "-5", "x"] {
        let srv = Server::start().unwrap();
        let mut sub = session(&srv).await;
        sub.send(format!("SUB ux 1\r\nUNSUB 1 {max}\r\n").as_bytes())
            .await;
        let mut p = session(&srv).await;
        p.send(&pub_frame("ux", None, b"x")).await;
        p.barrier().await;
        assert_eq!(sub.drain(QUIET).await, b"", "for UNSUB 1 {max}");
        assert!(!sub.closed(CLOSED).await, "for UNSUB 1 {max}");
    }
}

#[tokio::test]
async fn unsub_on_an_unknown_sid_is_not_an_error() {
    let srv = Server::start().unwrap();
    let mut c = session(&srv).await;
    c.send(b"UNSUB 99\r\n").await;
    c.send(b"PING\r\n").await;
    c.next(IO).await.expect_line(b"PONG\r\n");
    assert!(!c.closed(CLOSED).await);
}

#[tokio::test]
async fn disconnecting_removes_the_subscriptions() {
    let srv = Server::start().unwrap();
    let mut sub = session(&srv).await;
    sub.send(b"SUB gone 1\r\n").await;
    sub.barrier().await;
    drop(sub);

    // A publish after the subscriber is gone must not leave a half-written frame
    // behind: the next subscriber to the same subject gets only its own traffic.
    let mut p = session(&srv).await;
    p.send(&pub_frame("gone", None, b"x")).await;
    p.barrier().await;
    let mut later = session(&srv).await;
    later.send(b"SUB gone 1\r\n").await;
    later.barrier().await;
    p.send(&pub_frame("gone", None, b"y")).await;
    let line = later.next(IO).await;
    assert_eq!(line.line(), b"MSG gone 1 1\r\n");
    assert_eq!(
        later.read_msg_body(1, IO).await,
        b"y",
        "not the earlier one"
    );
}

// ---------------------------------------------------------------- keepalive --

/// The reference sends the first server-initiated PING about two seconds after
/// CONNECT (`firstClientPingInterval`, plus up to 20 % jitter), whatever the
/// configured interval is. A client that never answers is a bug magnet, so pin
/// both halves: the PING arrives, and a PONG keeps the connection alive.
#[tokio::test]
async fn server_initiated_ping_arrives_after_connect() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect_raw(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    match c.next(Duration::from_secs(6)).await {
        Next::Line(l) if l == b"PING\r\n" => {}
        other => panic!("expected a keepalive PING, got {other:?}"),
    }
    c.send(b"PONG\r\n").await;
    // Nothing else is due for the next two seconds: an answered keepalive is not
    // a stale connection.
    assert_eq!(c.drain(Duration::from_secs(2)).await, b"");
    assert!(!c.closed(CLOSED).await);
}

#[tokio::test]
async fn an_unanswered_keepalive_closes_the_connection() {
    let srv = Server::start_with_config("ping_interval: \"1s\"\n").unwrap();
    let mut c = Conn::connect_raw(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;

    let mut pings = 0;
    let mut got_stale = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match c.next(Duration::from_secs(2)).await {
            Next::Line(l) if l == b"PING\r\n" => pings += 1,
            Next::Line(l) if l.starts_with(b"-ERR") => {
                assert_eq!(l, b"-ERR 'Stale Connection'\r\n", "the exact string");
                got_stale = true;
                break;
            }
            Next::Closed => break,
            Next::Timeout => continue,
            other => panic!("unexpected traffic on an idle connection: {other:?}"),
        }
    }
    assert!(got_stale, "only saw {pings} PINGs and no -ERR");
    assert!(
        pings >= 2,
        "max_pings_out is 2: the close follows at least that many probes (saw {pings})"
    );
    assert!(
        c.closed(Duration::from_secs(2)).await,
        "then the socket closes"
    );
}

#[tokio::test]
async fn answering_keepalives_keeps_the_connection_open() {
    let srv = Server::start_with_config("ping_interval: \"1s\"\n").unwrap();
    let mut c = Conn::connect_raw(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    for _ in 0..4 {
        match c.next(Duration::from_secs(3)).await {
            Next::Line(l) if l == b"PING\r\n" => c.send(b"PONG\r\n").await,
            Next::Line(l) if l.starts_with(b"-ERR") => panic!("should not go stale: {l:?}"),
            other => panic!("unexpected {other:?}"),
        }
    }
    // Still alive, still routing.
    c.send(b"SUB alive 1\r\n").await;
    c.send(&pub_frame("alive", None, b"ok")).await;
    let line = c.next(IO).await;
    assert_eq!(line.line(), b"MSG alive 1 2\r\n");
    assert_eq!(c.read_msg_body(2, IO).await, b"ok");
}

#[tokio::test]
async fn ping_before_connect_is_answered() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect_raw(&srv.client_addr()).await;
    c.send(b"PING\r\n").await;
    // verbose is still on, so the SUB below is acknowledged; PING is not a
    // command that gets an +OK, only a PONG.
    c.next(IO).await.expect_line(b"PONG\r\n");
    assert!(!c.closed(CLOSED).await);
}

// -------------------------------------------------------------- sequencing ----

/// Everything in one TCP segment must be processed in order, and a partial op at
/// the end must be retained rather than dropped.
#[tokio::test]
async fn pipelined_commands_are_processed_in_order() {
    let srv = Server::start().unwrap();
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(r#"{"verbose":true,"echo":false}"#).await;
    c.next(IO).await.expect_line(b"+OK\r\n");
    c.send(b"SUB p 1\r\nPUB p 3\r\none\r\nSUB p 2\r\nPING\r\n")
        .await;
    let mut got = Vec::new();
    for _ in 0..4 {
        got.push(c.next(IO).await);
    }
    assert_eq!(got[0].line(), b"+OK\r\n", "SUB 1");
    assert_eq!(got[1].line(), b"+OK\r\n", "PUB");
    assert_eq!(got[2].line(), b"+OK\r\n", "SUB 2");
    assert_eq!(got[3].line(), b"PONG\r\n", "PING last");

    // Split an op down the middle across two writes. A second publisher keeps
    // echo:false from silencing the delivery we are about to check.
    c.connect_opts(r#"{"verbose":false,"echo":false}"#).await;
    c.send(b"SUB split 3\r\n").await;
    c.barrier().await;
    let mut p = Conn::connect(&srv.client_addr()).await;
    p.connect_opts(NO_VERBOSE).await;
    p.send(b"PUB split 6\r\nabc").await;
    assert!(
        c.next(SHORT).await.is_timeout(),
        "half a frame says nothing"
    );
    p.send(b"def\r\n").await;
    assert_eq!(c.next(IO).await.line(), b"MSG split 3 6\r\n");
    assert_eq!(c.read_msg_body(6, IO).await, b"abcdef");
}

/// Open and close a lot of connections: nothing may accumulate that stops the
/// next client from working, and no leftovers may make the server stop answering.
#[tokio::test]
async fn connection_churn_leaves_the_server_healthy() {
    let srv = Server::start().unwrap();
    for i in 0..500u32 {
        let mut c = Conn::connect(&srv.client_addr()).await;
        c.connect_opts(NO_VERBOSE).await;
        c.send(format!("SUB churn{i} 1\r\nPING\r\n").as_bytes())
            .await;
        c.next(IO).await.expect_line(b"PONG\r\n");
        drop(c);
    }
    // A fresh connection still routes to a fresh subscription, and the subject
    // space is not littered with the churn's registrations.
    let mut c = session(&srv).await;
    c.send(b"SUB churn-final 1\r\n").await;
    c.barrier().await;
    let mut p = session(&srv).await;
    p.send(&pub_frame("churn0", None, b"stale")).await;
    p.send(&pub_frame("churn-final", None, b"live")).await;
    p.barrier().await;
    let line = c.next(IO).await;
    assert_eq!(line.line(), b"MSG churn-final 1 4\r\n");
    assert_eq!(c.read_msg_body(4, IO).await, b"live");
    assert_eq!(
        c.drain(QUIET).await,
        b"",
        "no delivery to dropped connections"
    );
}

// --------------------------------------------------------- backpressure ------

/// A subscriber that never reads must not turn into an unbounded queue.
///
/// The victim's socket is opened with a deliberately tiny receive buffer so the
/// server's writes stop draining, and `max_pending` is set to 1 MiB so the limit
/// is reachable without filling RAM. Measured against the reference: the victim
/// is dropped with **no bytes on the wire** (typically a reset, because data was
/// queued to a peer that never read it), the publisher carries on, and every
/// other connection is untouched.
#[tokio::test]
async fn a_blocked_subscriber_is_dropped_not_accumulated() {
    let srv = Server::start_with_config("max_pending: 1048576\n").unwrap();
    let addr: std::net::SocketAddr = srv.client_addr().parse().unwrap();

    // Victim: subscribes, then never reads again.
    let sock = match addr {
        std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4().unwrap(),
        std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6().unwrap(),
    };
    sock.set_recv_buffer_size(4096).unwrap();
    let mut victim = sock.connect(addr).await.unwrap();
    let mut probe = [0u8; 1];
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n") {
        tokio::io::AsyncReadExt::read(&mut victim, &mut probe[..1])
            .await
            .unwrap();
        head.push(probe[0]);
    }
    assert!(head.starts_with(b"INFO "), "got {head:?}");
    tokio::io::AsyncWriteExt::write_all(
        &mut victim,
        b"CONNECT {\"verbose\":false}\r\nSUB blocked 1\r\n",
    )
    .await
    .unwrap();
    victim.set_nodelay(true).unwrap();

    let mut pubc = session(&srv).await;
    let body = vec![b'x'; 1024 * 1024];
    let mut dropped_at = None;
    for i in 0..25 {
        pubc.send(&pub_frame("blocked", None, &body)).await;
        // The victim is gone when a read stops yielding data: EOF if the server
        // closed cleanly, an error if it reset with bytes still queued.
        let gone = matches!(
            tokio::time::timeout(Duration::from_millis(50), victim.read(&mut probe[..1])).await,
            Ok(Ok(0)) | Ok(Err(_))
        );
        if gone {
            dropped_at = Some(i);
            break;
        }
    }

    // Whether the reset arrived or the server is still queueing inside the cap,
    // the promises that hold are the ones about everybody else.
    let mut other = session(&srv).await;
    other.send(b"SUB after 1\r\n").await;
    other.barrier().await;
    pubc.send(&pub_frame("after", None, b"still-alive")).await;
    let line = other.next(IO).await;
    assert_eq!(
        line.line(),
        b"MSG after 1 11\r\n",
        "a wedged subscriber must not stall the server (dropped at {dropped_at:?})"
    );
    assert_eq!(other.read_msg_body(11, IO).await, b"still-alive");
    assert!(
        !pubc.closed(CLOSED).await,
        "the publisher must not be punished for its subscriber (dropped at {dropped_at:?})"
    );
}

/// The other half of a wedged subscriber: it cannot *stay* wedged forever.
///
/// `write_deadline` is the only thing that notices a socket stopped taking
/// data, so it is also what frees a publisher parked behind the stall gate.
/// Measured against the reference with a tiny-rcvbuf victim and a 1 s deadline
/// (`specs/protocol-contract.md` §7): the victim is handed the frames the
/// server managed to queue and then FIN — **no `-ERR`, ever** — and the
/// publisher's parked send completes within a couple of deadlines with its own
/// connection intact. Without the deadline the publisher would sit there until
/// one of them disconnected.
#[tokio::test]
async fn a_wedged_subscriber_is_closed_and_releases_its_publisher() {
    let srv = Server::start_with_config("write_deadline: \"1s\"\nmax_pending: 8388608\n").unwrap();
    let addr: std::net::SocketAddr = srv.client_addr().parse().unwrap();

    // Victim: subscribes, then never reads again. The receive buffer is set
    // before `connect`, because that is the only moment Linux honours it.
    let sock = match addr {
        std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4().unwrap(),
        std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6().unwrap(),
    };
    sock.set_recv_buffer_size(4096).unwrap();
    let mut victim = sock.connect(addr).await.unwrap();
    let mut probe = [0u8; 1];
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n") {
        tokio::io::AsyncReadExt::read(&mut victim, &mut probe[..1])
            .await
            .unwrap();
        head.push(probe[0]);
    }
    assert!(head.starts_with(b"INFO "), "got {head:?}");
    tokio::io::AsyncWriteExt::write_all(
        &mut victim,
        b"CONNECT {\"verbose\":false}\r\nSUB wedged 1\r\n",
    )
    .await
    .unwrap();
    victim.set_nodelay(true).unwrap();

    let mut pubc = session(&srv).await;
    let frame = pub_frame("wedged", None, &vec![b'x'; 4096]);

    // 32 MiB at an 8 MiB cap: the publisher is certainly parked on the stall
    // gate by the time the deadline runs out, and nothing but closing the
    // subscriber can un-park it.
    let sent = tokio::time::timeout(Duration::from_secs(20), async {
        let mut n = 0usize;
        for _ in 0..8_000 {
            pubc.send(&frame).await;
            n += 1;
        }
        n
    })
    .await
    .expect("a wedged subscriber must not hold a publisher past its write deadline");

    // Now look at what the victim got. Draining it is what lets the queued FIN
    // through, so a subscriber the server forgot about shows up here as the
    // timeout instead of as a silent pass.
    let mut seen = Vec::new();
    let mut chunk = vec![0u8; 1 << 16];
    let gone = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match tokio::io::AsyncReadExt::read(&mut victim, &mut chunk).await {
                Ok(0) | Err(_) => break true,
                Ok(n) => seen.extend_from_slice(&chunk[..n]),
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        gone,
        "the wedged subscriber was still open after {sent} frames were published at it"
    );
    assert!(
        saw(&seen, b"MSG wedged 1 4096"),
        "the victim never received what it subscribed for ({} bytes read)",
        seen.len()
    );
    assert!(
        !saw(&seen, b"-ERR"),
        "a write-deadline close is silent (contract §7); victim saw {:?}",
        String::from_utf8_lossy(&seen[..seen.len().min(120)])
    );

    // The publisher is not punished, and the server is unharmed.
    assert!(
        !pubc.closed(CLOSED).await,
        "the publisher must not be punished for its subscriber"
    );
    let mut other = session(&srv).await;
    other.send(b"SUB after 1\r\n").await;
    other.barrier().await;
    pubc.send(&pub_frame("after", None, b"still-alive")).await;
    let line = other.next(IO).await;
    assert_eq!(line.line(), b"MSG after 1 11\r\n", "server must stay healthy after dropping a wedged subscriber");
    assert_eq!(other.read_msg_body(11, IO).await, b"still-alive");
}

/// `needle` anywhere in `hay`, without pulling in a crate for one assertion.
fn saw(hay: &[u8], needle: &[u8]) -> bool {
    hay.len() >= needle.len() && hay.windows(needle.len()).any(|w| w == needle)
}

/// A publisher parked on the stall gate must be woken when the subscriber it is
/// waiting for is dropped as a slow consumer — otherwise both sides sleep
/// forever and the server looks healthy while serving nothing.
///
/// The victim's queue crosses the 75 % mark, the publisher parks, and then the
/// subscriber goes away — by slow-consumer drop or by write deadline. Neither
/// leaves anything behind that can drain the queue the publisher is waiting on,
/// so a wait keyed only on "is there room now?" never ends: the parked publisher
/// sleeps on a corpse and its own connection stops being served. Found running
/// the paired baseline: one `pubsub` round in ~50 never finished, with every
/// server thread idle and both sockets empty.
#[tokio::test]
async fn a_parked_publisher_wakes_when_its_subscriber_is_dropped() {
    // 2 MiB cap: the 75 % mark is 1.5 MiB, so the publishers park quickly, and
    // `write_deadline` is what eventually drops the victim — that close is the
    // event the parked publishers must wake on. (The `max_payload` line is not
    // decoration: a cap below the default payload is rejected at startup by both
    // binaries.)
    let srv = Server::start_with_config(
        "max_payload: 131072\nmax_pending: 2097152\nwrite_deadline: \"2s\"\n",
    )
    .unwrap();
    let addr: std::net::SocketAddr = srv.client_addr().parse().unwrap();

    // Victim: tiny receive buffer, then never reads again.
    let sock = match addr {
        std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4().unwrap(),
        std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6().unwrap(),
    };
    sock.set_recv_buffer_size(4096).unwrap();
    let mut victim = sock.connect(addr).await.unwrap();
    let mut probe = [0u8; 1];
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n") {
        tokio::io::AsyncReadExt::read(&mut victim, &mut probe[..1])
            .await
            .unwrap();
        head.push(probe[0]);
    }
    assert!(head.starts_with(b"INFO "), "got {head:?}");
    tokio::io::AsyncWriteExt::write_all(
        &mut victim,
        b"CONNECT {\"verbose\":false}\r\nSUB parking 1\r\n",
    )
    .await
    .unwrap();
    victim.set_nodelay(true).unwrap();

    let body = vec![b'x'; 64 * 1024];
    let frame = pub_frame("parking", None, &body);
    // One connection, one 12 MiB write: the server reads until it parks on the
    // stall gate and then stops draining this socket.
    let mut flood = Vec::with_capacity(frame.len() * 200);
    for _ in 0..200 {
        flood.extend_from_slice(&frame);
    }

    let flood = tokio::spawn(async move {
        let mut c = match tokio::net::TcpStream::connect(addr).await {
            Ok(c) => c,
            Err(e) => return Err(format!("connect: {e}")),
        };
        c.set_nodelay(true).unwrap();
        let _ = c.writable().await;
        if let Err(e) = c.write_all(b"CONNECT {\"verbose\":false}\r\n").await {
            return Err(format!("connect line: {e}"));
        }
        // The body of the deadlock under test: this call has to finish.
        match tokio::time::timeout(Duration::from_secs(10), c.write_all(&flood)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(format!("write error: {e}")),
            Err(_) => Err("still parked after 10s: nothing woke the publisher".into()),
        }
    });

    // Give the flood time to park the server's reader on the victim's queue.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second publisher: enough volume to cross the victim's 100 % mark, which is
    // what drops it. Bounded too, because the bug under test parks it on the same
    // dead queue -- a test that can hang is not a test.
    let dropper = {
        let buf = frame.clone();
        tokio::spawn(async move {
            let mut c = tokio::net::TcpStream::connect(addr)
                .await
                .map_err(|e| format!("connect: {e}"))?;
            c.set_nodelay(true).unwrap();
            c.write_all(b"CONNECT {\"verbose\":false}\r\n")
                .await
                .map_err(|e| format!("connect line: {e}"))?;
            let mut many = Vec::with_capacity(buf.len() * 64);
            for _ in 0..64 {
                many.extend_from_slice(&buf);
            }
            match tokio::time::timeout(Duration::from_secs(10), c.write_all(&many)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(format!("write error: {e}")),
                Err(_) => Err("the second publisher is parked on the dead queue too".into()),
            }
        })
    };

    flood
        .await
        .expect("flood task")
        .expect("a publisher parked on a subscriber must be released when it is dropped");
    let _ = dropper.await;

    // And the victim really was dropped -- without that, the two publishers above
    // would have finished because nothing ever closed, and the test would have
    // proved nothing.
    let mut chunk = vec![0u8; 1 << 16];
    let gone = tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            match victim.read(&mut chunk).await {
                Ok(0) | Err(_) => break true,
                Ok(_) => {}
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        gone,
        "the wedged subscriber was never closed, so no publisher ever parked"
    );

    let mut pubc = session(&srv).await;
    let mut other = session(&srv).await;
    other.send(b"SUB after 1\r\n").await;
    other.barrier().await;
    pubc.send(&pub_frame("after", None, b"still-alive")).await;
    let line = other.next(IO).await;
    assert_eq!(line.line(), b"MSG after 1 11\r\n", "server must survive the drop");
    assert_eq!(other.read_msg_body(11, IO).await, b"still-alive");
}
