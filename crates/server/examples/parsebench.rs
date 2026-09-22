//! Where does the per-message time go on the publish-only path?
//!
//! No sockets: this drives the same `Parser` the reader task uses over a buffer
//! of pre-encoded `PUB` frames — parse only, `recvmsg` and routing excluded.
//! Build with `--features allocstats` and it also prints allocations per
//! message, which is how the count in `specs/perf-notes.md` is attributed.
//!
//!   cargo run --release -p nats-server-rs --example parsebench
//!   cargo run --release -p nats-server-rs --features allocstats --example parsebench
//!   SIZE=128 ROUNDS=4000 …          # frame size, refills of the 64 KiB chunk

use std::time::Instant;

use bytes::BytesMut;
use nats_server_rs::proto::{Event, Limits, Parser};

#[cfg(feature = "allocstats")]
#[global_allocator]
static ALLOC: nats_server_rs::allocstats::Counting = nats_server_rs::allocstats::Counting;

/// One 64 KiB block of `PUB test <size>` frames, plus how many it holds.
fn chunk(size: usize, subject: &str, bytes: usize) -> (Vec<u8>, usize) {
    let flen = subject.len() + size + 8;
    let mut out = Vec::with_capacity(bytes + flen);
    let mut n = 0;
    while out.len() + flen + 8 < bytes {
        out.extend_from_slice(format!("PUB {subject} {size}\r\n").as_bytes());
        out.resize(out.len() + size, b'x');
        out.extend_from_slice(b"\r\n");
        n += 1;
    }
    (out, n)
}

fn main() {
    let size: usize = std::env::var("SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);
    let rounds: u64 = std::env::var("ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let (wire, frames) = chunk(size, "test", 1 << 16);
    let lim = Limits::default();

    run(&wire, frames, 32, &lim); // warm up: allocator, frequency

    let t = Instant::now();
    let msgs = run(&wire, frames, rounds, &lim);
    let dt = t.elapsed();
    println!(
        "bench name=parsebench size={size} msgs={msgs} ns_per_msg={:.1} wire_bytes_per_msg={:.1} event_bytes={} parser_bytes={}",
        dt.as_nanos() as f64 / msgs as f64,
        wire.len() as f64 / frames as f64,
        // The two structures the cost is suspected to hide in: what one parsed
        // operation costs to write into the batch, and what the state machine
        // costs to move between its two states.
        std::mem::size_of::<Event>(),
        std::mem::size_of::<Parser>(),
    );
    #[cfg(feature = "allocstats")]
    {
        use std::sync::atomic::Ordering::Relaxed;
        let a = nats_server_rs::allocstats::ALLOCS.load(Relaxed);
        let b = nats_server_rs::allocstats::BYTES.load(Relaxed);
        println!(
            "# allocs_per_msg={:.2} bytes_per_msg={:.0}",
            a as f64 / msgs as f64,
            b as f64 / msgs as f64
        );
    }
}

/// Feed one 64 KiB chunk per round and return once the buffer is drained, which
/// is the steady state of the reader task: a full read, everything complete.
fn run(wire: &[u8], frames: usize, rounds: u64, lim: &Limits) -> u64 {
    let mut buf = BytesMut::with_capacity(wire.len() * 2);
    let mut events: Vec<Event> = Vec::new();
    let mut msgs = 0u64;
    for _ in 0..rounds {
        buf.extend_from_slice(wire);
        let mut parser = Parser::new();
        while !buf.is_empty() {
            events.clear();
            parser.feed_into(&mut buf, lim, &mut events).expect("parse");
            msgs += events.len() as u64;
            for ev in events.drain(..) {
                match ev {
                    Event::Publish { body, .. } => {
                        std::hint::black_box(&body);
                    }
                    _ => unreachable!("only publishes"),
                }
            }
        }
        let _ = frames;
    }
    msgs
}
