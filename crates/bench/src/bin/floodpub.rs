//! Drain rate: how fast one connection can push `PUB` frames at a server.
//!
//! This is the path `nats bench pub` drives — one publisher, **no subscribers**
//! — with the client's own cost taken out: frames are encoded once into a big
//! buffer and handed to the socket with `write_all`, so the publishing thread
//! either memcpy's into the kernel or blocks. The clock stops when the server
//! has *consumed* every byte, which a trailing `PING`/`PONG` proves (a PONG is
//! ordered behind everything the server has read), so the loopback's multi-megabyte
//! receive slack cannot flatter the number.
//!
//! Env: ADDR (127.0.0.1:4222), MSGS (20_000_000), SIZE (128), SUBJECT (test),
//! TARGET_BYTES (256 KiB per write).
//!
//! The CONNECT is the one the nats CLI sends — `verbose:false` (no `+OK` per
//! message), `pedantic:false` (no subject validation per message),
//! `no_responders:true` (the 503 check runs per publish) — measured with
//! `/tmp/sniff.py`, so both servers take the same options path.

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn main() -> anyhow::Result<()> {
    let addr = std::env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:4222".into());
    let msgs = nats_bench::param("MSGS", 20_000_000);
    let size = nats_bench::param("SIZE", 128) as usize;
    let subject = std::env::var("SUBJECT").unwrap_or_else(|_| "test".into());
    let target = std::env::var("TARGET_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256 * 1024);

    let mut sock = TcpStream::connect(&addr)?;
    sock.set_nodelay(true)?;
    let mut info = [0u8; 8192];
    let n = sock.read(&mut info)?;
    anyhow::ensure!(n > 5 && &info[..5] == b"INFO ", "no INFO from {addr}");
    sock.write_all(br#"CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"name":"floodpub","lang":"rust","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}"#)?;
    sock.write_all(b"\r\nPING\r\n")?;

    let mut frame = Vec::with_capacity(size + 64);
    frame.extend_from_slice(format!("PUB {subject} {size}\r\n").as_bytes());
    frame.resize(frame.len() + size, b'x');
    frame.extend_from_slice(b"\r\n");
    let flen = frame.len();
    let per_chunk = (target / flen).max(1);
    let mut chunk = Vec::with_capacity(per_chunk * flen);
    for _ in 0..per_chunk {
        chunk.extend_from_slice(&frame);
    }

    let started = Instant::now();
    let mut left = msgs;
    let mut bytes = 0u64;
    while left > 0 {
        let whole = left.min(per_chunk as u64) as usize;
        let buf = &chunk[..whole * flen];
        let mut off = 0;
        while off < buf.len() {
            match sock.write(&buf[off..]) {
                Ok(0) => anyhow::bail!("socket stopped accepting data"),
                Ok(n) => off += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        bytes += buf.len() as u64;
        left -= whole as u64;
    }
    // Gate the clock on the server having read everything, not on us having
    // handed it to the kernel.
    sock.write_all(b"PING\r\n")?;
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut timed = true;
    sock.set_read_timeout(Some(Duration::from_millis(200)))?;
    while Instant::now() < deadline {
        let mut b = [0u8; 4096];
        match sock.read(&mut b) {
            Ok(0) => break,
            Ok(n) => {
                seen.extend_from_slice(&b[..n]);
                if seen.windows(5).any(|w| w == b"PONG\r") || seen.windows(4).any(|w| w == b"+OK\r")
                {
                    timed = false;
                    break;
                }
                if seen.len() > 1 << 20 {
                    seen.clear();
                }
            }
            Err(ref e)
                if e.kind() == ErrorKind::WouldBlock
                    || e.kind() == ErrorKind::TimedOut
                    || e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    let dur = started.elapsed();
    nats_bench::report("floodpub", msgs, size as u64, dur);
    println!(
        "# floodpub bytes={bytes} frame_len={flen} per_chunk={per_chunk} consumed={}",
        !timed
    );
    anyhow::ensure!(timed == false, "server never consumed the whole stream");
    Ok(())
}
