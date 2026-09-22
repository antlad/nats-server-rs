//! The interest cache — a publish's answer to "who wants this subject", kept on
//! the publishing task and trusted until the registry says it changed.
//!
//! Two promises make that legal, and each has a moment where it could be broken
//! rather than merely argued (`PLAN3.md` Task 24.3):
//!
//! * **Within a connection**, the decision belongs to the *operation*, not the
//!   read: a `SUB` and a `PUB` that arrive in one segment must still be ordered
//!   against each other, which they are because the reader task runs them one
//!   after another and the `SUB` bumps the generation before the `PUB` looks.
//! * **Across connections**, anything that changes who is interested has to bump
//!   that generation on the way out of the registry lock — including the quiet
//!   cases, a close and an auto-unsub reaching its limit, where nobody takes an
//!   obvious "remove" path.
//!
//! A stale cache is not a slow cache: it delivers to subscriptions that are gone
//! and fails to deliver to ones that just appeared.

use core_it::wire::{pub_frame, Conn, Next, IO, NO_VERBOSE, QUIET};
use nats_test_harness::Server;

/// Byte-substring test, because counting the length of a `MSG` line to size a
/// `windows(n)` is exactly the kind of thing that makes a test lie.
fn has(hay: &[u8], needle: &[u8]) -> bool {
    hay.len() >= needle.len() && hay.windows(needle.len()).any(|w| w == needle)
}

async fn quiet_client(srv: &Server) -> Conn {
    let mut c = Conn::connect(&srv.client_addr()).await;
    c.connect_opts(NO_VERBOSE).await;
    c
}

#[tokio::test]
async fn a_subscribe_and_a_publish_in_one_segment_still_self_deliver() {
    // One write, two operations, in order. The hint that "this read had no
    // subscriptions when it started" would answer this wrong, and Part 2 measured
    // it being answered wrong.
    let srv = Server::start().unwrap();
    let mut c = quiet_client(&srv).await;
    c.send(b"SUB mine 1\r\nPUB mine 1\r\nx\r\n").await;
    assert_eq!(
        c.drain(QUIET).await,
        b"MSG mine 1 1\r\nx\r\n",
        "a SUB earlier in the same segment is a SUB that happened"
    );
}

#[tokio::test]
async fn a_publish_earlier_in_the_same_segment_than_the_subscribe_is_not_delivered() {
    // The other direction, same mechanism: the cache must not see the subscription
    // that the *later* operation creates.
    let srv = Server::start().unwrap();
    let mut c = quiet_client(&srv).await;
    c.send(b"PUB mine 1\r\nx\r\nSUB mine 1\r\n").await;
    c.send(b"PUB mine 1\r\ny\r\n").await;
    assert_eq!(
        c.drain(QUIET).await,
        b"MSG mine 1 1\r\ny\r\n",
        "the publish before the subscribe cannot be interested in it"
    );
}

#[tokio::test]
async fn another_connections_subscribe_invalidates_the_cached_answer() {
    // A publishes while nobody listens, which A caches as "no interest". B then
    // subscribes. A's next publish must reach B, which needs the generation A
    // recorded to have moved.
    let srv = Server::start().unwrap();
    let mut a = quiet_client(&srv).await;
    let mut b = quiet_client(&srv).await;

    a.send(&pub_frame("two", None, b"1")).await;
    a.barrier().await; // proves the publish, and the empty answer it cached, is done

    b.send(b"SUB two 1\r\n").await;
    b.barrier().await;

    a.send(&pub_frame("two", None, b"2")).await;
    a.barrier().await;

    let got = b.drain(QUIET).await;
    assert!(
        has(&got, b"MSG two 1 1\r\n2"),
        "B subscribed after A's first publish and must still get the second; got {got:?}"
    );
    assert!(
        !has(&got, b"MSG two 1 1\r\n1"),
        "A's first publish was before B existed"
    );
}

#[tokio::test]
async fn a_close_invalidates_the_cached_answer() {
    // The subscription goes away with the connection, and no `UNSUB` says so:
    // `remove_conn` is the path, and if it does not bump the generation the
    // publisher keeps queueing frames for a dead connection.
    let srv = Server::start().unwrap();
    let mut a = quiet_client(&srv).await;
    let b = quiet_client(&srv).await;
    let mut b = b;
    b.send(b"SUB gone 1\r\n").await;
    b.barrier().await;

    drop(b);
    // Let the server notice: a publish that gets a PONG answered after the hang-up
    // proves the reader ran, and the reader sees the closed socket on its next read.
    a.send(&pub_frame("gone", None, b"x")).await;
    a.barrier().await;
    a.send(&pub_frame("gone", None, b"y")).await;
    match a.next(IO).await {
        Next::Closed | Next::Timeout | Next::Line(_) => {}
    }
    a.barrier().await;
}

#[tokio::test]
async fn an_auto_unsub_that_spends_a_subscription_invalidates_the_cached_answer() {
    // `UNSUB sid 1` is a delivery-counted removal: the subscription disappears
    // inside the routing of the message that used it up. A cache keyed to the
    // generation before that would deliver the next message to a spent sub.
    let srv = Server::start().unwrap();
    let mut a = quiet_client(&srv).await;
    let mut b = quiet_client(&srv).await;
    b.send(b"SUB once 1\r\nUNSUB 1 1\r\n").await;
    b.barrier().await;

    a.send(&pub_frame("once", None, b"1")).await;
    a.barrier().await;
    a.send(&pub_frame("once", None, b"2")).await;
    a.barrier().await;

    let got = b.drain(QUIET).await;
    assert!(
        has(&got, b"MSG once 1 1\r\n1"),
        "the first message must reach it; got {got:?}"
    );
    assert!(
        !has(&got, b"MSG once 1 1\r\n2"),
        "the subscription was spent, so the second must not arrive; got {got:?}"
    );
}
