//! Behavioral tests driven through the real async-nats client.
//!
//! These complement tests/wire.rs: the wire tests pin down byte-level protocol,
//! these pin down the semantics a client actually relies on (routing, wildcards,
//! queue groups, request-reply).

use futures::StreamExt;
use nats_test_harness::Server;
use std::time::Duration;

async fn connect(srv: &Server) -> async_nats::Client {
    async_nats::connect(srv.client_addr()).await.unwrap()
}

fn timeout() -> Duration {
    Duration::from_secs(5)
}

#[tokio::test]
async fn basic_pub_sub() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let mut sub = nc.subscribe("foo").await.unwrap();
    nc.publish("foo", "hello".into()).await.unwrap();
    let msg = tokio::time::timeout(timeout(), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.payload[..], b"hello");
}

#[tokio::test]
async fn wildcard_star() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let mut sub = nc.subscribe("foo.*").await.unwrap();
    nc.publish("foo.bar", "1".into()).await.unwrap(); // matches
    nc.publish("foo.bar.baz", "x".into()).await.unwrap(); // too deep, must NOT match
    nc.flush().await.unwrap();
    let msg = tokio::time::timeout(timeout(), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.subject.as_ref(), "foo.bar");
    assert_eq!(&msg.payload[..], b"1");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), sub.next())
            .await
            .is_err(),
        "foo.bar.baz must not match foo.*"
    );
}

#[tokio::test]
async fn wildcard_gt() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let mut sub = nc.subscribe("foo.>").await.unwrap();
    nc.publish("foo.bar.baz", "2".into()).await.unwrap();
    nc.publish("foo", "nope".into()).await.unwrap(); // `>` needs at least one token after foo.
    nc.flush().await.unwrap();
    let msg = tokio::time::timeout(timeout(), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.subject.as_ref(), "foo.bar.baz");
    assert_eq!(&msg.payload[..], b"2");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), sub.next())
            .await
            .is_err(),
        "subject \"foo\" must not match foo.>"
    );
}

#[tokio::test]
async fn queue_group_splits_load() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let mut s1 = nc
        .queue_subscribe("work", "workers".to_string())
        .await
        .unwrap();
    let mut s2 = nc
        .queue_subscribe("work", "workers".to_string())
        .await
        .unwrap();
    // SUB and PUB go through the same connection and the same command channel,
    // so the server sees both SUBs before any PUB: no extra barrier needed.
    nc.flush().await.unwrap();

    let n = 100usize;
    for i in 0..n {
        nc.publish("work", i.to_string().into()).await.unwrap();
    }
    nc.flush().await.unwrap();

    let (mut c1, mut c2) = (0usize, 0usize);
    while c1 + c2 < n {
        tokio::select! {
            Some(_) = s1.next() => { c1 += 1; }
            Some(_) = s2.next() => { c2 += 1; }
            _ = tokio::time::sleep(timeout()) => panic!("stalled after {} ({}:{})", c1 + c2, c1, c2),
        }
    }
    assert!(c1 > 0 && c2 > 0, "both members must receive, got {c1}/{c2}");
    assert_eq!(c1 + c2, n, "exactly-once across queue group");
}

#[tokio::test]
async fn request_reply() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let mut sub = nc.subscribe("svc.echo").await.unwrap();
    let resp_nc = nc.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            if let Some(reply) = msg.reply {
                resp_nc.publish(reply, "pong".into()).await.unwrap();
            }
        }
    });
    let resp = tokio::time::timeout(timeout(), nc.request("svc.echo", "ping".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&resp.payload[..], b"pong");
}

#[tokio::test]
async fn no_responders_error() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let err = tokio::time::timeout(timeout(), nc.request("nobody.home", "x".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(
            err.kind(),
            async_nats::client::RequestErrorKind::NoResponders
        ),
        "expected NoResponders, got {err:?}"
    );
}

#[tokio::test]
async fn ping_liveness() {
    // async-nats has no manual ping in 0.50: it keepalives with PING on
    // ping_interval and fails the connection if PONG never comes. So drive a
    // client with a short interval and verify it is still connected (and still
    // delivering) several intervals later.
    let srv = Server::start().unwrap();
    let nc = async_nats::ConnectOptions::new()
        .ping_interval(Duration::from_millis(100))
        .connect(srv.client_addr())
        .await
        .unwrap();
    let mut sub = nc.subscribe("alive").await.unwrap();
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            matches!(
                nc.connection_state(),
                async_nats::connection::State::Connected
            ),
            "client should still consider the connection alive"
        );
    }
    nc.publish("alive", "still here".into()).await.unwrap();
    let msg = tokio::time::timeout(timeout(), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.payload[..], b"still here");
}

#[tokio::test]
async fn client_sees_server_info() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let info = nc.server_info();
    assert_eq!(info.port as u16, srv.port);
    assert_eq!(info.proto, 1);
    assert!(info.max_payload >= 1_048_576);
    assert_eq!(nc.max_payload(), info.max_payload);
    assert!(!info.server_id.is_empty());
}

#[tokio::test]
async fn fanout_to_many_subscribers() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let mut subs = Vec::new();
    for _ in 0..5 {
        subs.push(nc.subscribe("news").await.unwrap());
    }
    nc.flush().await.unwrap();
    nc.publish("news", "hi".into()).await.unwrap();
    nc.flush().await.unwrap();
    for mut s in subs {
        let msg = tokio::time::timeout(timeout(), s.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&msg.payload[..], b"hi");
    }
}

#[tokio::test]
async fn oversized_publish_fails_client_side() {
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let big = vec![0u8; 2 * 1024 * 1024]; // Go default max_payload is 1 MiB
    let err = nc.publish("foo", big.into()).await;
    assert!(err.is_err(), "publish over max_payload must error");
    // connection stays usable
    let mut sub = nc.subscribe("foo").await.unwrap();
    nc.publish("foo", "ok".into()).await.unwrap();
    let msg = tokio::time::timeout(timeout(), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.payload[..], b"ok");
}

#[tokio::test]
async fn flush_delivers_everything_published_before_it() {
    // Sanity check on flushing semantics: everything published before a
    // successful flush must be delivered.
    let srv = Server::start().unwrap();
    let nc = connect(&srv).await;
    let mut sub = nc.subscribe("drain.me").await.unwrap();
    for i in 0..500 {
        nc.publish("drain.me", i.to_string().into()).await.unwrap();
        if i % 50 == 0 {
            nc.flush().await.unwrap();
        }
    }
    nc.flush().await.unwrap();
    for i in 0..500 {
        let msg = tokio::time::timeout(timeout(), sub.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            msg.payload.as_ref(),
            i.to_string().as_bytes(),
            "order preserved"
        );
    }
}
