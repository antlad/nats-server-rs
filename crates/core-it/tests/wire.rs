//! Raw-TCP wire-protocol tests.
//!
//! These deliberately avoid a client library: the Go server is the spec, so we
//! assert on exact bytes on the wire. Every read is bounded by a timeout so a
//! hung server fails instead of hanging forever.

use nats_test_harness::Server;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect and consume the initial INFO line, returning the stream and parsed INFO.
async fn connect(srv: &Server) -> (TcpStream, serde_json::Value) {
    let mut s = TcpStream::connect(srv.client_addr()).await.unwrap();
    let line = read_line(&mut s).await.expect("server should send INFO");
    assert!(
        line.starts_with("INFO "),
        "first line must be INFO, got: {line:?}"
    );
    let info: serde_json::Value = serde_json::from_str(&line[5..]).unwrap();
    (s, info)
}

/// Read one CRLF-terminated line. None on EOF.
async fn read_line(s: &mut TcpStream) -> Option<String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match tokio::time::timeout(IO_TIMEOUT, s.read(&mut byte)).await {
            Ok(Ok(0)) => return None, // EOF
            Ok(Ok(_)) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n") {
                    return Some(String::from_utf8_lossy(&buf).into_owned());
                }
            }
            Ok(Err(e)) => panic!("read error: {e}"),
            Err(_) => panic!(
                "read_line timed out with partial: {:?}",
                String::from_utf8_lossy(&buf)
            ),
        }
    }
}

/// Read exactly `n` payload bytes plus their trailing CRLF.
async fn read_exact_line(s: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut body = vec![0u8; n + 2]; // payload + CRLF
    tokio::time::timeout(IO_TIMEOUT, s.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&body[n..], b"\r\n", "payload must be CRLF terminated");
    body[..n].to_vec()
}

async fn write_all(s: &mut TcpStream, data: &str) {
    s.write_all(data.as_bytes()).await.unwrap();
    s.flush().await.unwrap();
}

#[tokio::test]
async fn info_on_connect_has_core_fields() {
    let srv = Server::start().unwrap();
    let (_s, info) = connect(&srv).await;
    assert_eq!(
        info["proto"].as_u64(),
        Some(1),
        "client proto level must be 1"
    );
    assert!(info["max_payload"].as_u64().unwrap() >= 1_048_576);
    assert_eq!(info["port"].as_u64(), Some(srv.port as u64));
    assert!(info["server_id"].is_string());
    assert!(info["version"].is_string());
    // JetStream must be advertised off for a core-only server
    match info.get("jetstream") {
        None | Some(serde_json::Value::Bool(false)) => {}
        Some(v) => panic!("jetstream should be absent or false, got {v}"),
    }
    assert!(info["connect_urls"].as_array().is_none_or(|a| a.is_empty()));
}

#[tokio::test]
async fn ping_pong() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(&mut s, "PING\r\n").await;
    assert_eq!(read_line(&mut s).await.as_deref(), Some("PONG\r\n"));
}

#[tokio::test]
async fn verbose_mode_sends_ok() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(&mut s, "CONNECT {\"verbose\":true}\r\nPING\r\n").await;
    assert_eq!(read_line(&mut s).await.as_deref(), Some("+OK\r\n"));
    assert_eq!(read_line(&mut s).await.as_deref(), Some("PONG\r\n"));
}

#[tokio::test]
async fn pub_sub_round_trip() {
    let srv = Server::start().unwrap();
    let (mut sub, _) = connect(&srv).await;
    write_all(
        &mut sub,
        "CONNECT {\"verbose\":false}\r\nSUB foo 1\r\nPING\r\n",
    )
    .await;
    assert_eq!(
        read_line(&mut sub).await.as_deref(),
        Some("PONG\r\n"),
        "PONG is the barrier: the SUB ahead of it must be registered"
    );

    let (mut pub_, _) = connect(&srv).await;
    write_all(
        &mut pub_,
        "CONNECT {\"verbose\":false}\r\nPUB foo 5\r\nhello\r\n",
    )
    .await;

    let header = read_line(&mut sub).await.expect("MSG header");
    assert!(header.starts_with("MSG foo 1 5\r\n"), "got {header:?}");
    assert_eq!(read_exact_line(&mut sub, 5).await, b"hello");
}

#[tokio::test]
async fn unsub_stops_delivery() {
    let srv = Server::start().unwrap();
    let (mut sub, _) = connect(&srv).await;
    write_all(&mut sub, "CONNECT {\"verbose\":false}\r\nSUB foo 1\r\n").await;

    let (mut pub_, _) = connect(&srv).await;
    write_all(&mut pub_, "CONNECT {\"verbose\":false}\r\n").await;

    // Barrier: PONG after SUB means the subscription is live.
    write_all(&mut sub, "PING\r\n").await;
    assert_eq!(read_line(&mut sub).await.as_deref(), Some("PONG\r\n"));

    write_all(&mut pub_, "PUB foo 1\r\na\r\n").await;
    assert!(read_line(&mut sub).await.unwrap().starts_with("MSG foo 1"));
    read_exact_line(&mut sub, 1).await;

    // UNSUB is processed before the PONG on the same connection, so waiting for
    // PONG *before* publishing is what makes this deterministic.
    write_all(&mut sub, "UNSUB 1\r\nPING\r\n").await;
    loop {
        let line = read_line(&mut sub).await.expect("connection closed early");
        assert_eq!(
            line, "PONG\r\n",
            "unexpected traffic while unsubscribing: {line:?}"
        );
        if line == "PONG\r\n" {
            break;
        }
    }

    write_all(&mut pub_, "PUB foo 1\r\nb\r\n").await;
    write_all(&mut sub, "PING\r\n").await;
    let next = read_line(&mut sub).await;
    assert_eq!(
        next.as_deref(),
        Some("PONG\r\n"),
        "expected no MSG after UNSUB, got {next:?}"
    );
}

#[tokio::test]
async fn unknown_verb_gets_err_and_close() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(&mut s, "CONNECT {\"verbose\":false}\r\nBOGUS\r\n").await;
    let line = read_line(&mut s).await.expect("-ERR line");
    assert!(line.starts_with("-ERR"), "got {line:?}");
    assert!(
        line.contains("Unknown Protocol Operation"),
        "reference server reports the verb as unknown, got {line:?}"
    );
    assert!(
        read_line(&mut s).await.is_none(),
        "connection must close after -ERR"
    );
}

#[tokio::test]
async fn pub_over_max_payload_gets_err() {
    let srv = Server::start().unwrap();
    let (mut s, info) = connect(&srv).await;
    write_all(&mut s, "CONNECT {\"verbose\":false}\r\n").await;
    let max = info["max_payload"].as_u64().unwrap();
    write_all(&mut s, &format!("PUB foo {}\r\n", max + 1)).await;
    let line = read_line(&mut s).await.expect("-ERR line");
    assert!(line.starts_with("-ERR"), "got {line:?}");
    assert!(
        read_line(&mut s).await.is_none(),
        "connection must close after -ERR"
    );
}

// Publishing on a wildcard subject is rejected in both normal and pedantic mode
// (the reference server validates publish subjects unconditionally as of v2.15),
// and the error is *non-fatal*: the connection stays open and keeps processing.
#[tokio::test]
async fn pub_on_wildcard_subject_gets_err_and_stays_usable() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(
        &mut s,
        "CONNECT {\"verbose\":false,\"pedantic\":true}\r\nPUB foo.* 1\r\nx\r\nPING\r\n",
    )
    .await;
    let line = read_line(&mut s).await.expect("-ERR line");
    assert!(line.starts_with("-ERR"), "got {line:?}");
    assert!(line.contains("Invalid Publish Subject"), "got {line:?}");
    assert_eq!(
        read_line(&mut s).await.as_deref(),
        Some("PONG\r\n"),
        "connection must stay usable after a bad publish subject"
    );
}

#[tokio::test]
async fn sub_with_invalid_subject_gets_err_and_stays_usable() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(
        &mut s,
        "CONNECT {\"verbose\":false}\r\nSUB foo..bar 1\r\nPING\r\n",
    )
    .await;
    let line = read_line(&mut s).await.expect("-ERR line");
    assert!(line.starts_with("-ERR"), "got {line:?}");
    assert!(line.contains("Invalid Subject"), "got {line:?}");
    assert_eq!(
        read_line(&mut s).await.as_deref(),
        Some("PONG\r\n"),
        "connection must stay usable after a bad subscribe subject"
    );
}

// Parse-level violations (here: a non-numeric payload size) are a different class
// from the semantic errors above: the reference server drops the connection
// without sending -ERR. Asserting this keeps our server from "improving" the spec.
#[tokio::test]
async fn malformed_pub_closes_connection_without_err() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(
        &mut s,
        "CONNECT {\"verbose\":false}\r\nPUB foo notanumber\r\n",
    )
    .await;
    let line = read_line(&mut s).await;
    assert!(
        line.is_none() || line.unwrap().trim().is_empty(),
        "malformed PUB must close without an -ERR line"
    );
}

// --------------------------------------------------------------------------------
// Added in Part 2, Task 8: the INFO key set, pinned against the Go reference. The
// tests above this line are Part 1's and are unchanged.

/// The keys a core-only server must offer, and the ones it must not mention at
/// all. Measured against the reference binary — see specs/protocol-contract.md.
#[tokio::test]
async fn info_key_set_is_pinned() {
    let srv = Server::start().unwrap();
    let (_s, info) = connect(&srv).await;
    let obj = info.as_object().expect("INFO must be a JSON object");

    // Required of every core server, whatever it is written in.
    for key in [
        "server_id",
        "server_name",
        "version",
        "proto",
        "host",
        "port",
        "headers",
        "max_payload",
    ] {
        assert!(obj.contains_key(key), "INFO must carry {key}: {obj:?}");
    }
    assert_eq!(info["headers"].as_bool(), Some(true));

    // Features this server does not have must be *absent*, not false or empty:
    // clients branch on presence.
    for key in [
        "jetstream",
        "connect_urls",
        "cluster",
        "cluster_name",
        "domain",
        "auth_required",
        "tls_required",
        "tls_available",
        "nonce",
        "ldm",
        "compression",
    ] {
        assert!(
            obj.get(key).is_none_or(|v| {
                // The reference's own omitempty rule: a key may appear only with
                // a non-default value. For these it must not appear at all.
                let _ = v;
                false
            }),
            "INFO must not mention {key}: {obj:?}"
        );
    }

    // Implementation detail of the reference build (Go version, commit, x25519
    // key, JetStream API level, per-connection ids). A Rust server may omit them;
    // client.rs proves async-nats connects without them.
    for key in [
        "git_commit",
        "go",
        "api_lvl",
        "xkey",
        "client_id",
        "client_ip",
    ] {
        if let Some(v) = obj.get(key) {
            assert!(
                !v.is_null(),
                "{key} is omitempty in the reference: absent or a value, never null"
            );
        }
    }
}

#[tokio::test]
async fn info_is_a_single_line_in_the_documented_shape() {
    let srv = Server::start().unwrap();
    let mut s = TcpStream::connect(srv.client_addr()).await.unwrap();
    let line = read_line(&mut s).await.expect("INFO");
    // No CR or LF inside the JSON: one line, one protocol operation.
    let body = &line[5..line.len() - 2];
    assert!(!body.contains('\r') && !body.contains('\n'), "got {line:?}");
    // The reference joins the parts with single spaces, which leaves one before
    // the CR-LF (`generateInfoJSON`). Pinned as a shape, not as a byte trap.
    assert!(
        body.ends_with(' ') || body.ends_with('}'),
        "INFO body must be JSON, optionally followed by the reference's \
         join-space, got {body:?}"
    );
    let json = body.trim();
    let parsed: serde_json::Value =
        serde_json::from_str(json).expect("INFO must be one JSON object");
    assert_eq!(parsed["proto"].as_u64(), Some(1));
}

#[tokio::test]
async fn server_id_is_an_opaque_uppercase_token_equal_to_server_name() {
    // Measured: the reference's server_id is 56 characters from the base32
    // alphabet. It is not a NUID -- server.go uses the server's nkey public key
    // ("CreateServer" -> PublicKey), so the shape is a property of nkeys. A core
    // server with no auth has no key pair, and may emit any token of the same
    // shape: nothing on the client side parses it.
    let srv = Server::start().unwrap();
    let (_s, info) = connect(&srv).await;
    let id = info["server_id"].as_str().expect("server_id");
    assert_eq!(
        id.len(),
        56,
        "measured width of the reference id, got {id:?}"
    );
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b)),
        "base32 alphabet (A-Z, 2-7), got {id:?}"
    );
    assert_eq!(
        info["server_name"].as_str(),
        Some(id),
        "an unconfigured server names itself by its id"
    );
    // A second connection sees the same server, and a different id per process
    // run: nothing here may reuse one connection's INFO.
    let (_s2, info2) = connect(&srv).await;
    assert_eq!(
        info2["server_id"].as_str(),
        Some(id),
        "server_id is per-server, not per-connection"
    );
}

/// The config path (Task 8's harness addition) with a limit small enough that an
/// oversized publish is cheap to send.
#[tokio::test]
async fn configured_max_payload_is_advertised_and_enforced() {
    let srv = Server::start_with_config("max_payload: 1024\n").unwrap();
    let (mut s, info) = connect(&srv).await;
    assert_eq!(info["max_payload"].as_u64(), Some(1024));
    write_all(&mut s, "CONNECT {\"verbose\":false}\r\n").await;

    // Under the limit: silently fine.
    write_all(&mut s, "PUB small 1000\r\n").await;
    write_all(&mut s, &"x".repeat(1000)).await;
    write_all(&mut s, "\r\n").await;
    write_all(&mut s, "PING\r\n").await;
    assert_eq!(read_line(&mut s).await.as_deref(), Some("PONG\r\n"));

    write_all(&mut s, "PUB big 1025\r\n").await;
    let line = read_line(&mut s).await.expect("-ERR line");
    assert!(line.starts_with("-ERR"), "got {line:?}");
    assert!(
        line.contains("Maximum Payload Violation"),
        "the reference names the violation, got {line:?}"
    );
    assert!(read_line(&mut s).await.is_none(), "then closes");
}

// --------------------------------------------------------------------------------
// Ported from the reference's own Core suite (see specs/go-audit.md) where the
// behaviour was previously only implied.

/// Matching is token-wise: a literal subscription to `foo` does not cover
/// `foo.bar` (`go:test/client_test.go:TestTwoTokenPubMatchSingleTokenSub`), and
/// `foo.>` does not cover `foo` either.
#[tokio::test]
async fn literal_subscription_does_not_match_deeper_subjects() {
    let srv = Server::start().unwrap();
    let (mut sub, _) = connect(&srv).await;
    write_all(
        &mut sub,
        "CONNECT {\"verbose\":false}\r\nSUB depth 1\r\nPING\r\n",
    )
    .await;
    assert_eq!(read_line(&mut sub).await.as_deref(), Some("PONG\r\n"));

    let (mut p, _) = connect(&srv).await;
    write_all(
        &mut p,
        "CONNECT {\"verbose\":false}\r\nPUB depth.deeper 1\r\nx\r\nPING\r\n",
    )
    .await;
    assert_eq!(
        read_line(&mut p).await.as_deref(),
        Some("PONG\r\n"),
        "the publisher's barrier proves the PUB was processed"
    );
    // The subscriber's own PING, sent after the PUB was processed server-side,
    // is the barrier: anything it should have received would have come first.
    write_all(&mut sub, "PING\r\n").await;
    assert_eq!(
        read_line(&mut sub).await.as_deref(),
        Some("PONG\r\n"),
        "a deeper subject must not reach a one-token subscription"
    );
}

/// A declared size that overflows the reference's parser (`parseSize` returns -1
/// for anything over an int64) is a parse error: disconnect, no `-ERR`
/// (`go:test/maxpayload_test.go:TestMaxPayloadOverrun`).
#[tokio::test]
async fn publish_size_that_overflows_int64_closes_without_err() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(
        &mut s,
        "CONNECT {\"verbose\":false}\r\nPUB foo 18446744073709551615123\r\n",
    )
    .await;
    let line = read_line(&mut s).await;
    assert!(
        line.is_none() || line.unwrap().trim().is_empty(),
        "an unparseable size must close without an -ERR line"
    );
}

/// A size that is merely larger than `max_payload` but still a sane number gets
/// the named error first (`max_payload_test.go:TestMaxPayload`), including the
/// int32-range case the reference calls out.
#[tokio::test]
async fn publish_size_in_int32_range_over_limit_gets_err() {
    let srv = Server::start().unwrap();
    let (mut s, _) = connect(&srv).await;
    write_all(
        &mut s,
        "CONNECT {\"verbose\":false}\r\nPUB foo 199380988\r\n",
    )
    .await;
    let line = read_line(&mut s).await.expect("-ERR line");
    assert!(line.starts_with("-ERR"), "got {line:?}");
    assert!(read_line(&mut s).await.is_none(), "then close");
}
