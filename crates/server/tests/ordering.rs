//! End-to-end proof of the two promises that later optimisation can silently
//! break: order within one publisher→subscriber pair, and exactly-once per
//! message across a queue group. Both run against the real binary, because the
//! promise is about the wire.

use nats_test_harness::Server;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CONNECT: &[u8] = b"CONNECT {\"verbose\":false}\r\n";

/// A cancel-safe raw client: bytes accumulate in `buf` and a frame is only
/// consumed when it is complete, so a timed-out read never loses traffic. That is
/// what lets the queue-group test wait on several members at once.
struct Client {
    s: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    async fn new(srv: &Server) -> Client {
        let s = TcpStream::connect(srv.client_addr()).await.unwrap();
        let mut c = Client { s, buf: Vec::new() };
        c.until_crlf().await; // INFO
        c.s.write_all(CONNECT).await.unwrap();
        c
    }

    async fn write(&mut self, bytes: &[u8]) {
        self.s.write_all(bytes).await.unwrap();
        self.s.flush().await.unwrap();
    }

    async fn until_crlf(&mut self) {
        let mut byte = [0u8; 1];
        while !self.buf.ends_with(b"\r\n") {
            let n = self.s.read(&mut byte).await.expect("read");
            assert!(n > 0, "server closed the connection");
            self.buf.extend_from_slice(&byte);
        }
    }

    /// The next MSG frame's payload, or None on EOF.
    async fn msg(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some(frame) = self.take_msg() {
                return Some(frame);
            }
            let mut byte = [0u8; 1];
            match self.s.read(&mut byte).await {
                Ok(0) => return None,
                Ok(_) => self.buf.extend_from_slice(&byte),
                Err(e) => panic!("read: {e}"),
            }
        }
    }

    /// Consume a complete frame from the buffer if one is there.
    fn take_msg(&mut self) -> Option<Vec<u8>> {
        let end = self.buf.windows(2).position(|w| w == b"\r\n")?;
        let header = String::from_utf8_lossy(&self.buf[..end]).into_owned();
        let mut parts = header.split(' ');
        let (verb, size_field) = (parts.next()?, parts.next_back()?);
        let n: usize = match verb {
            "MSG" | "HMSG" => size_field.parse().expect("size field"),
            other => panic!("expected a MSG frame, got {other:?} in {header:?}"),
        };
        if self.buf.len() < end + 2 + n + 2 {
            return None; // body still on the wire
        }
        let body = self.buf[end + 2..end + 2 + n].to_vec();
        assert_eq!(&self.buf[end + 2 + n..end + 2 + n + 2], b"\r\n", "terminator");
        self.buf.drain(..end + 2 + n + 2);
        Some(body)
    }

    async fn barrier(&mut self) {
        self.write(b"PING\r\n").await;
        loop {
            self.until_crlf().await;
            let line = std::mem::take(&mut self.buf);
            if line == b"PONG\r\n" {
                return;
            }
            assert!(!line.starts_with(b"-ERR"), "server errored: {line:?}");
        }
    }
}

#[tokio::test]
async fn ordering_proof_100k() {
    const N: usize = 100_000;
    let srv = Server::start().unwrap();
    let mut sub = Client::new(&srv).await;
    sub.write(b"SUB order 1\r\n").await;
    sub.barrier().await;

    let mut pubc = Client::new(&srv).await;
    let reader = tokio::spawn(async move {
        let mut seen = Vec::with_capacity(N);
        for _ in 0..N {
            let body = sub.msg().await.expect("delivery");
            seen.push(String::from_utf8(body).expect("ascii payload"));
        }
        seen
    });

    // Written in batches: a pipeline of frames in one segment is the case most
    // likely to reorder if the delivery path ever grows a second writer.
    let mut batch = Vec::with_capacity(16 * 1024);
    for i in 0..N {
        let payload = i.to_string();
        batch.extend_from_slice(format!("PUB order {}\r\n", payload.len()).as_bytes());
        batch.extend_from_slice(payload.as_bytes());
        batch.extend_from_slice(b"\r\n");
        if batch.len() > 8 * 1024 {
            pubc.write(&batch).await;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        pubc.write(&batch).await;
    }

    let seen = reader.await.expect("reader task");
    assert_eq!(seen.len(), N);
    for (i, got) in seen.iter().enumerate() {
        assert_eq!(got, &i.to_string(), "delivery {i} out of order");
    }
}

/// One message, one delivery per queue group — and every member reached. The
/// reference picks a random member, so no exact split may be asserted.
#[tokio::test]
async fn queue_group_delivers_each_message_once() {
    const N: usize = 6_000;
    let srv = Server::start().unwrap();
    let total = Arc::new(AtomicUsize::new(0));

    let mut members = Vec::new();
    for sid in 0..3u8 {
        let mut s = Client::new(&srv).await;
        s.write(format!("SUB work q1 {sid}\r\n").as_bytes()).await;
        s.barrier().await;
        members.push(s);
    }

    let mut handles = Vec::new();
    for (i, mut member) in members.into_iter().enumerate() {
        let total = total.clone();
        handles.push(tokio::spawn(async move {
            let mut got = Vec::new();
            loop {
                // Stop when the group as a whole has seen everything, but give a
                // member that is mid-delivery the full window to finish.
                if total.load(Ordering::SeqCst) >= N {
                    match tokio::time::timeout(Duration::from_millis(300), member.msg()).await {
                        Ok(None) | Err(_) => break,
                        Ok(Some(body)) => {
                            total.fetch_add(1, Ordering::SeqCst);
                            got.push(body);
                        }
                    }
                } else {
                    match tokio::time::timeout(Duration::from_secs(10), member.msg()).await {
                        Ok(Some(body)) => {
                            total.fetch_add(1, Ordering::SeqCst);
                            got.push(body);
                        }
                        Ok(None) => panic!("member {i} hung up early"),
                        Err(_) => break,
                    }
                }
            }
            got
        }));
    }

    let mut pubc = Client::new(&srv).await;
    let mut batch = Vec::with_capacity(16 * 1024);
    for i in 0..N {
        let payload = i.to_string();
        batch.extend_from_slice(format!("PUB work {}\r\n", payload.len()).as_bytes());
        batch.extend_from_slice(payload.as_bytes());
        batch.extend_from_slice(b"\r\n");
        if batch.len() > 8 * 1024 {
            pubc.write(&batch).await;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        pubc.write(&batch).await;
    }
    pubc.barrier().await;

    let mut all: HashSet<Vec<u8>> = HashSet::new();
    let mut counts = Vec::new();
    for h in handles {
        let got = h.await.expect("member task");
        counts.push(got.len());
        for g in got {
            assert!(all.insert(g), "a message was delivered twice");
        }
    }
    assert_eq!(all.len(), N, "exactly once across the group, counts {counts:?}");
    assert!(
        counts.iter().all(|c| *c > 0),
        "every member must be reached, got {counts:?}"
    );
}
