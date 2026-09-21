//! Minimal raw-protocol client: enough to speak every verb the contract pins,
//! with no library between us and the bytes.
//!
//! A NATS server is specified by what it puts on the wire, so these helpers
//! return *bytes*, never parsed messages. Reads are always bounded -- a server
//! that goes silent must fail the test, not hang it.
//!
//! One behaviour of the reference deserves to be known here: about 2 s after a
//! `CONNECT` the server starts its own keepalive and sends `PING\r\n` on an idle
//! connection (`setFirstPingTimer`, `firstClientPingInterval = 2 s` plus up to
//! 20 % jitter). Every real client answers that with a `PONG`. [`Conn`] does the
//! same by default so a test cannot mistake server traffic for a protocol
//! response -- use [`Conn::connect_raw`] when the keepalive itself is what you
//! are testing.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Long enough for a loopback round trip, short enough that a broken server
/// fails the run instead of stalling it.
pub const IO: Duration = Duration::from_secs(5);
/// How long silence must last before we call a connection quiet.
pub const QUIET: Duration = Duration::from_millis(300);
/// The server is either done talking or hung up; both answer inside this window.
pub const CLOSED: Duration = Duration::from_millis(400);
/// Anything the tests assert on arrives faster than this on loopback.
pub const SHORT: Duration = Duration::from_millis(900);

/// What the server did next.
#[derive(Debug, PartialEq, Eq)]
pub enum Next {
    /// One complete CRLF-terminated control line (including the CR-LF).
    Line(Vec<u8>),
    /// The server closed the connection.
    Closed,
    /// Nothing arrived within the timeout.
    Timeout,
}

impl Next {
    pub fn is_closed(&self) -> bool {
        matches!(self, Next::Closed)
    }
    pub fn is_timeout(&self) -> bool {
        matches!(self, Next::Timeout)
    }
    /// The line as bytes, panicking with the actual event if it is not a line.
    pub fn line(&self) -> &[u8] {
        match self {
            Next::Line(l) => l,
            other => panic!("expected a control line, got {other:?}"),
        }
    }
    /// Assert the server said exactly this.
    pub fn expect_line(&self, want: &[u8]) {
        assert_eq!(self.line(), want, "unexpected server response");
    }
}

pub struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    /// The INFO line the server sent on accept, parsed.
    pub info: serde_json::Value,
    /// Answer the server's own keepalive PINGs (real clients do).
    auto_pong: bool,
}

impl Conn {
    /// Connect, consume INFO, and keep the connection quiet.
    pub async fn connect(addr: &str) -> Conn {
        let mut c = Self::connect_raw(addr).await;
        c.auto_pong = true;
        c
    }

    /// Connect and consume INFO, but leave server-initiated PINGs visible.
    pub async fn connect_raw(addr: &str) -> Conn {
        let mut s = TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("connect to {addr}: {e}"));
        let mut buf = Vec::new();
        let line = match read_line_into(&mut s, &mut buf, IO).await {
            Next::Line(l) => l,
            other => panic!("server sent no INFO line, got {other:?}"),
        };
        assert!(
            line.starts_with(b"INFO "),
            "first line must be INFO, got {line:?}"
        );
        let json = String::from_utf8_lossy(&line[5..]).into_owned();
        let info: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("bad INFO json {json:?}: {e}"));
        Conn {
            s,
            buf,
            info,
            auto_pong: false,
        }
    }

    pub async fn send(&mut self, bytes: &[u8]) {
        self.s
            .write_all(bytes)
            .await
            .unwrap_or_else(|e| panic!("write to server (did it close early?): {e}"));
        self.s.flush().await.expect("flush");
    }

    /// Send a `CONNECT` with the given options JSON.
    pub async fn connect_opts(&mut self, opts: &str) {
        self.send(format!("CONNECT {opts}\r\n").as_bytes()).await;
    }

    /// The next control line, or [`Next::Closed`] / [`Next::Timeout`].
    pub async fn next(&mut self, wait: Duration) -> Next {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Next::Timeout;
            }
            let line = read_line_into(&mut self.s, &mut self.buf, remaining).await;
            if self.absorb_ping(&line).await {
                continue;
            }
            return line;
        }
    }

    /// Wait for a `PONG` -- the barrier that proves every command sent before it
    /// has been processed by the server.
    pub async fn barrier(&mut self) {
        self.send(b"PING\r\n").await;
        for _ in 0..64 {
            match self.next(IO).await {
                Next::Line(l) if l == b"PONG\r\n" => return,
                Next::Line(l) if l.starts_with(b"+OK") => continue,
                other => panic!("expected PONG barrier, got {other:?}"),
            }
        }
        panic!("too much traffic before the PONG barrier");
    }

    /// Everything buffered plus whatever arrives during one quiet window.
    pub async fn drain(&mut self, quiet: Duration) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        let mut deadline = tokio::time::Instant::now() + quiet;
        let mut byte = [0u8; 1];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return out;
            }
            match tokio::time::timeout(remaining, self.s.read(&mut byte)).await {
                Ok(Ok(0)) | Ok(Err(_)) => return out,
                Ok(Ok(_)) => {
                    out.push(byte[0]);
                    if self.absorb_ping_bytes(&mut out).await {
                        // A whole keepalive PING came and went; give the server
                        // another quiet window so we do not end early.
                        deadline = tokio::time::Instant::now() + quiet;
                    }
                }
                Err(_) => return out, // quiet: the server is done talking
            }
        }
    }

    /// True once the server has closed (or closes while we look).
    pub async fn closed(&mut self, wait: Duration) -> bool {
        if !self.buf.is_empty() {
            return false;
        }
        let mut deadline = tokio::time::Instant::now() + wait;
        let mut byte = [0u8; 1];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            match tokio::time::timeout(remaining, self.s.read(&mut byte)).await {
                Ok(Ok(0)) | Ok(Err(_)) => return true,
                Ok(Ok(_)) => {
                    self.buf.push(byte[0]);
                    let mut v = std::mem::take(&mut self.buf);
                    if self.absorb_ping_bytes(&mut v).await {
                        deadline = tokio::time::Instant::now() + wait;
                    }
                    self.buf = v;
                    if !self.buf.is_empty() {
                        return false;
                    }
                }
                Err(_) => return false,
            }
        }
    }

    /// Read the `n` payload bytes of a MSG/HMSG frame, consuming the CRLF that
    /// terminates them (the frame's own bytes, not the caller's to inspect).
    pub async fn read_msg_body(&mut self, n: usize, wait: Duration) -> Vec<u8> {
        let mut body = vec![0u8; n + 2];
        let mut filled = 0;
        let deadline = tokio::time::Instant::now() + wait;
        while filled < body.len() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!remaining.is_zero(), "payload read timed out");
            match tokio::time::timeout(remaining, self.s.read(&mut body[filled..])).await {
                Ok(Ok(0)) => panic!("connection closed mid-payload"),
                Ok(Ok(k)) => filled += k,
                Ok(Err(e)) => panic!("read error: {e}"),
                Err(_) => panic!(
                    "payload read timed out with {filled} of {} bytes",
                    body.len()
                ),
            }
        }
        assert_eq!(
            &body[n..],
            b"\r\n",
            "payload must be CRLF terminated, got {:?}",
            String::from_utf8_lossy(&body[n..])
        );
        body.truncate(n);
        body
    }

    /// If a complete server keepalive PING sits at the end of `bytes`, answer it
    /// and remove it. Returns true when it did.
    async fn absorb_ping_bytes(&mut self, bytes: &mut Vec<u8>) -> bool {
        if !self.auto_pong {
            return false;
        }
        let n = bytes.len();
        if n < 7 || &bytes[n - 7..] != b"PING\r\n" || !bytes[..n - 7].is_empty() {
            return false;
        }
        bytes.truncate(n - 7);
        // Deliberately unchecked: a keepalive can race with the server closing
        // us, and the caller is about to notice that anyway.
        let _ = self.s.write_all(b"PONG\r\n").await;
        true
    }

    async fn absorb_ping(&mut self, line: &Next) -> bool {
        if let Next::Line(l) = line {
            if self.auto_pong && l == b"PING\r\n" {
                let _ = self.s.write_all(b"PONG\r\n").await;
                return true;
            }
        }
        false
    }
}

async fn read_line_into(s: &mut TcpStream, buf: &mut Vec<u8>, wait: Duration) -> Next {
    let mut byte = [0u8; 1];
    loop {
        if let Some(i) = find_crlf(buf) {
            let line = buf[..=i + 1].to_vec();
            buf.drain(..=i + 1);
            return Next::Line(line);
        }
        match tokio::time::timeout(wait, s.read(&mut byte)).await {
            Ok(Ok(0)) => return Next::Closed,
            Ok(Ok(_)) => buf.push(byte[0]),
            Ok(Err(_)) => return Next::Closed,
            Err(_) => return Next::Timeout,
        }
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// `CONNECT {"verbose":false}` -- quiet, and otherwise the reference's own
/// defaults (verbose/pedantic/echo all stay true unless stated).
pub const NO_VERBOSE: &str = r#"{"verbose":false}"#;

/// The `headers`/`no_responders` capability a modern client declares.
pub const CAPS: &str = r#"{"verbose":false,"headers":true,"no_responders":true}"#;

/// A publish, framed exactly as the protocol requires.
pub fn pub_frame(subject: &str, reply: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut out = match reply {
        Some(r) => format!("PUB {subject} {r} {}\r\n", body.len()),
        None => format!("PUB {subject} {}\r\n", body.len()),
    }
    .into_bytes();
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
    out
}

/// A header publish: the control line counts the header block and the total,
/// which is what makes `HPUB hb 12 14` the empty-header form.
pub fn hpub_frame(subject: &str, reply: Option<&str>, hdr: &[u8], body: &[u8]) -> Vec<u8> {
    let mut out = match reply {
        Some(r) => format!(
            "HPUB {subject} {r} {} {}\r\n",
            hdr.len(),
            hdr.len() + body.len()
        ),
        None => format!(
            "HPUB {subject} {} {}\r\n",
            hdr.len(),
            hdr.len() + body.len()
        ),
    }
    .into_bytes();
    out.extend_from_slice(hdr);
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
    out
}

/// `NATS/1.0\r\n` + header lines + the blank line that closes the block.
pub fn header_block(lines: &[&str]) -> Vec<u8> {
    let mut out = b"NATS/1.0\r\n".to_vec();
    for l in lines {
        out.extend_from_slice(l.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}
