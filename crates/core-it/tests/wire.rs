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
