//! Delivery rate, with the client taken out of the measurement.
//!
//! `pubsub` is the honest *end-to-end* bench and a poor server instrument: the
//! async-nats client burns more CPU than the server does, so its rate is the
//! client's and only the server-CPU column means anything. This is the delivery
//! path's equivalent of `floodpub` — two raw sockets, one publisher thread
//! pushing pre-encoded `PUB` frames, one subscriber thread pulling `MSG` frames
//! off the wire and counting them, neither of them doing anything a NATS client
//! does (no protocol state, no futures, no allocation per message).
//!
//! What is left is a server holding a message from one socket to another, which
//! is the number PLAN3's delivery gate is set against.
//!
//! Env: ADDR / NATS_BENCH_URL (127.0.0.1:4222), MSGS (10_000_000), SIZE (256),
//! SUBJECT (bench), SID (1), TARGET_BYTES (256 KiB per publisher write),
//! READ_BYTES (4 MiB subscriber buffer).
//!
//! The clock starts once the subscription is live on the server — proved by a
//! `PING`/`PONG` on the subscriber's own connection, which is ordered behind the
//! `SUB` — and stops when the subscriber has counted the last message, so the
//! rate is *deliveries* per second. If the subscriber falls behind, the server's
//! own backpressure (75 % stall, 100 % slow consumer) paces the publisher, which
//! is the behaviour under test and not a fault in it.

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const CONNECT: &[u8] =
    br#"CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"name":"floodpubsub","lang":"rust","version":"0.1.0","protocol":1,"echo":true,"headers":true,"no_responders":true}"#;

fn main() -> anyhow::Result<()> {
    let addr = connect_addr()?;
    let msgs = nats_bench::param("MSGS", 10_000_000);
    let size = nats_bench::param("SIZE", 256) as usize;
    let subject = std::env::var("SUBJECT").unwrap_or_else(|_| "bench".into());
    let sid = std::env::var("SID").unwrap_or_else(|_| "1".into());
    let target_bytes = nats_bench::param("TARGET_BYTES", 256 * 1024) as usize;
    let read_bytes = nats_bench::param("READ_BYTES", 4 * 1024 * 1024) as usize;
    let probe_every = probe_every_ms();
    let stall_timeout = nats_bench::param("STALL_SECS", 60);
    let quiesce_secs = nats_bench::param("QUIESCE_SECS", 2);
    let consumed = Arc::new(AtomicU64::new(0));

    let delivered = Arc::new(AtomicU64::new(0));
    let subscribed = Arc::new(AtomicBool::new(false));
    let go = Arc::new(AtomicBool::new(false));
    // Set when the subscriber's socket goes away early: the run is over, and it
    // is not over well.
    let failed = Arc::new(AtomicBool::new(false));
    // Set when the run is over, so a subscriber parked in `read` on a stream that
    // stopped short can be released instead of joined forever.
    let done = Arc::new(AtomicBool::new(false));

    let (s_delivered, s_subscribed, s_go, s_failed, s_done, s_addr, s_subject, s_sid, s_consumed) = (
        delivered.clone(),
        subscribed.clone(),
        go.clone(),
        failed.clone(),
        done.clone(),
        addr.clone(),
        subject.clone(),
        sid.clone(),
        consumed.clone(),
    );
    let sub_thread = thread::Builder::new().name("sub".into()).spawn(move || {
        subscriber(
            &s_addr,
            &s_subject,
            &s_sid,
            msgs,
            read_bytes,
            &s_delivered,
            &s_subscribed,
            &s_go,
            &s_failed,
            &s_done,
            &s_consumed,
        )
    })?;

    let (p_subscribed, p_go, p_addr, p_subject) = (
        subscribed.clone(),
        go.clone(),
        addr.clone(),
        subject.clone(),
    );
    let pub_thread = thread::Builder::new().name("pub".into()).spawn(move || {
        publisher(
            &p_addr,
            &p_subject,
            msgs,
            size,
            target_bytes,
            &p_subscribed,
            &p_go,
        )
    })?;

    // The subscriber's SUB must be live before the publisher's first byte, and
    // both handshakes must be outside the timed window.
    let wait = Instant::now();
    while !subscribed.load(Ordering::Acquire) {
        anyhow::ensure!(wait.elapsed() < Duration::from_secs(20), "subscriber never subscribed");
        anyhow::ensure!(!failed.load(Ordering::Relaxed), "subscriber died subscribing");
        // A thread that has already returned knows why: surface its error instead
        // of timing out on a flag it will never set.
        if sub_thread.is_finished() {
            break;
        }
        thread::sleep(Duration::from_micros(200));
    }
    let t0 = Instant::now();
    go.store(true, Ordering::Release);

    let mut tick = Instant::now();
    while !pub_thread.is_finished() {
        if tick.elapsed() > Duration::from_millis(probe_every) {
            eprintln!(
                "# t={:.1} sent<={} delivered={}",
                t0.elapsed().as_secs_f64(),
                msgs,
                delivered.load(Ordering::Relaxed)
            );
            tick = Instant::now();
        }
        thread::sleep(Duration::from_millis(1));
    }
    let written = pub_thread.join().expect("publisher panicked")?;
    anyhow::ensure!(written == msgs, "publisher wrote {written} of {msgs}");
    // The run is over when the count stops moving, not when it reaches `msgs`: a
    // flooded subscriber can leave the last few thousand messages unwritten on
    // either binary (see `specs/perf-notes.md`, open question), and the number we
    // want is CPU per message *delivered*, which needs the delivered count as the
    // divisor, not the published one.
    let mut last = delivered.load(Ordering::Relaxed);
    let mut moved = Instant::now();
    while delivered.load(Ordering::Relaxed) < msgs {
        if failed.load(Ordering::Relaxed) {
            break;
        }
        let now = delivered.load(Ordering::Relaxed);
        if now != last {
            last = now;
            moved = Instant::now();
        } else if moved.elapsed() > Duration::from_secs(quiesce_secs) {
            break;
        }
        if tick.elapsed() > Duration::from_millis(probe_every) {
            eprintln!("# t={:.1} written=all delivered={now}", t0.elapsed().as_secs_f64());
            tick = Instant::now();
        }
        if t0.elapsed() > Duration::from_secs(stall_timeout) {
            break;
        }
        thread::sleep(Duration::from_micros(200));
    }
    let got = delivered.load(Ordering::Relaxed);

    // The clock stops at the last delivery, not at the detection of silence: the
    // quiesce window is the instrument's patience, not the server's latency.
    let dur = moved - t0;
    let _ = &dur;
    if got != msgs {
        eprintln!("# short: {got} of {msgs} delivered; reporting the delivered count");
    }
    done.store(true, Ordering::Release);
    let sub_err = sub_thread.join();
    nats_bench::report("floodpubsub", got, size as u64, dur);
    println!(
        "# floodpubsub delivered={got} of {msgs} size={size} wall_ms={:.0}",
        dur.as_secs_f64() * 1000.0
    );
    // A subscriber that was closed mid-run (slow consumer, protocol error) would
    // otherwise look like a merely slow server: the count stops, the run ends, and
    // the CPU per message is a lie.
    anyhow::ensure!(
        !failed.load(Ordering::Relaxed),
        "subscriber went away: {}",
        err_of(sub_err)
    );
    Ok(())
}

/// Whatever the subscriber thread came back with, for an error that says why.
fn err_of(r: thread::Result<anyhow::Result<()>>) -> String {
    match r {
        Ok(Ok(())) => "ended cleanly".into(),
        Ok(Err(e)) => e.to_string(),
        Err(_) => "panicked".into(),
    }
}

/// Print a progress line this often while a run is in flight, so a stalled run
/// says where it stalled instead of hanging for a minute and a half.
fn probe_every_ms() -> u64 {
    std::env::var("PROGRESS_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000)
}

fn connect_addr() -> anyhow::Result<String> {
    match std::env::var("NATS_BENCH_URL") {
        Ok(u) if !u.is_empty() => Ok(u.trim_start_matches("nats://").to_string()),
        _ => Ok(std::env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:4222".into())),
    }
}

fn dial(addr: &str) -> anyhow::Result<TcpStream> {
    let mut sock = TcpStream::connect(addr)?;
    sock.set_nodelay(true)?;
    let mut info = [0u8; 8192];
    let n = sock.read(&mut info)?;
    anyhow::ensure!(n > 5 && &info[..5] == b"INFO ", "no INFO from {addr}");
    Ok(sock)
}

/// Encode `msgs` frames and push them, then gate on a `PING`/`PONG` so "written"
/// means "the server has read every byte".
fn publisher(
    addr: &str,
    subject: &str,
    msgs: u64,
    size: usize,
    target_bytes: usize,
    subscribed: &AtomicBool,
    go: &AtomicBool,
) -> anyhow::Result<u64> {
    let mut sock = dial(addr)?;
    sock.write_all(CONNECT)?;
    sock.write_all(b"\r\nPING\r\n")?;
    expect_pong(&mut sock)?;

    let mut frame = Vec::with_capacity(size + 64);
    frame.extend_from_slice(format!("PUB {subject} {size}\r\n").as_bytes());
    frame.resize(frame.len() + size, b'x');
    frame.extend_from_slice(b"\r\n");
    let flen = frame.len();
    let per_chunk = (target_bytes / flen).max(1);
    let mut chunk = Vec::with_capacity(per_chunk * flen);
    for _ in 0..per_chunk {
        chunk.extend_from_slice(&frame);
    }

    while !subscribed.load(Ordering::Acquire) {
        thread::yield_now();
    }
    while !go.load(Ordering::Acquire) {
        thread::yield_now();
    }

    let mut left = msgs;
    let mut sent_bytes = 0u64;
    while left > 0 {
        let whole = left.min(per_chunk as u64) as usize;
        let buf = &chunk[..whole * flen];
        let mut off = 0;
        while off < buf.len() {
            match sock.write(&buf[off..]) {
                Ok(0) => anyhow::bail!("socket stopped accepting data"),
                Ok(n) => { off += n; sent_bytes += n as u64; }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        left -= whole as u64;
    }
    eprintln!(
        "# pub wrote_msgs={} bytes={} expected={} flen={} per_chunk={}",
        msgs - left,
        sent_bytes,
        msgs * flen as u64,
        flen,
        per_chunk
    );
    Ok(msgs - left)
}

fn subscriber(
    addr: &str,
    subject: &str,
    sid: &str,
    msgs: u64,
    read_bytes: usize,
    delivered: &AtomicU64,
    subscribed: &AtomicBool,
    go: &AtomicBool,
    failed: &AtomicBool,
    done: &AtomicBool,
    consumed: &AtomicU64,
) -> anyhow::Result<()> {
    let mut sock = dial(addr)?;
    // Long enough that a live stream is never interrupted, short enough that a
    // subscriber whose run stopped early wakes up, notices, and can be joined.
    sock.set_read_timeout(Some(Duration::from_millis(200)))?;
    sock.write_all(CONNECT)?;
    sock.write_all(b"\r\n")?;
    sock.write_all(format!("SUB {subject} {sid}\r\nPING\r\n").as_bytes())?;
    sock.flush()?;
    expect_pong(&mut sock)?;
    subscribed.store(true, Ordering::Release);
    while !go.load(Ordering::Acquire) {
        thread::yield_now();
    }

    // A compacted buffer rather than a ring: one memmove per buffer-full of
    // thousands of messages is not the cost being measured, and the scanner stays
    // trivial.
    let mut buf = vec![0u8; read_bytes.max(1 << 20)];
    let mut end = 0usize;
    loop {
        if end == buf.len() {
            anyhow::bail!("subscriber buffer filled without a complete frame");
        }
        match sock.read(&mut buf[end..]) {
            Ok(0) => {
                failed.store(true, Ordering::Relaxed);
                return Ok(());
            }
            Ok(n) => end += n,
            Err(e)
                if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut =>
            {
                // Nothing arrived for a whole poll interval: the run is over if
                // the driver says it is.
                if done.load(Ordering::Relaxed) {
                    return Ok(());
                }
                continue;
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
        let mut pos = 0usize;
        loop {
            match next_frame(&buf[pos..end]) {
                Frame::Message(total) => {
                    pos += total;
                    consumed.fetch_add(total as u64, Ordering::Relaxed);
                    delivered.fetch_add(1, Ordering::Relaxed);
                    if delivered.load(Ordering::Relaxed) >= msgs {
                        return Ok(());
                    }
                }
                Frame::Ping => {
                    sock.write_all(b"PONG\r\n")?;
                    pos += 6;
                }
                Frame::Skip(n) => pos += n,
                Frame::Err(line) => {
                    eprintln!("# subscriber got {line}");
                    failed.store(true, Ordering::Relaxed);
                    return Ok(());
                }
                Frame::NeedMore => break,
            }
        }
        if pos > 0 {
            buf.copy_within(pos..end, 0);
            end -= pos;
        }
    }
}

enum Frame {
    /// A delivered message; the value is the frame's whole length, so the caller
    /// advances without re-scanning.
    Message(usize),
    /// `PING\r\n` — answer it, or the server closes us as stale.
    Ping,
    /// `INFO …`, `+OK`, `PONG`: bytes to step over.
    Skip(usize),
    /// No complete frame here yet.
    NeedMore,
    /// `-ERR '…'`, or a line that is not a frame this bench understands.
    Err(String),
}

/// Decode the frame at the front of `src`. The scan covers the control line only;
/// the payload is skipped by arithmetic, the way the server's own parser skips it.
fn next_frame(src: &[u8]) -> Frame {
    let Some(nl) = src.iter().position(|b| *b == b'\n') else {
        return Frame::NeedMore;
    };
    let line = &src[..=nl];
    let body = &line[..nl.saturating_sub(1)]; // without the trailing \r\n
    let verb_len = body.iter().position(|b| *b == b' ').unwrap_or(body.len());
    match &body[..verb_len] {
        b"MSG" | b"HMSG" => {
            let Some(digits) = body[verb_len..].rsplit(|c| *c == b' ').next() else {
                return Frame::Err(String::from_utf8_lossy(line).into_owned());
            };
            let Ok(Ok(total)) = std::str::from_utf8(digits).map(|d| d.trim().parse::<usize>()) else {
                return Frame::Err(String::from_utf8_lossy(line).into_owned());
            };
            let need = nl + 1 + total + 2;
            if src.len() < need {
                return Frame::NeedMore;
            }
            Frame::Message(need)
        }
        b"PING" => Frame::Ping,
        b"-ERR" => Frame::Err(String::from_utf8_lossy(line).into_owned()),
        _ => Frame::Skip(nl + 1),
    }
}

/// Read until a `PONG`, answering any `PING` the server sends in the meantime.
fn expect_pong(sock: &mut TcpStream) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut buf = [0u8; 8192];
    let mut seen = Vec::new();
    while Instant::now() < deadline {
        let n = match sock.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        if n == 0 {
            anyhow::bail!("closed before the PONG that proves the pipeline");
        }
        seen.extend_from_slice(&buf[..n]);
        if seen.windows(4).any(|w| w == b"PONG") {
            return Ok(());
        }
        if seen.windows(5).any(|w| w == b"PING\r") {
            sock.write_all(b"PONG\r\n")?;
        }
        if seen.len() > 1 << 16 {
            seen.clear();
        }
    }
    anyhow::bail!("no PONG")
}
