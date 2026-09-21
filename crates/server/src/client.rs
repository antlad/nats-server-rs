//! Per-connection state: options, the outbound buffer, and the two tasks that
//! drive one TCP connection.
//!
//! Model, and why (contract §8):
//!
//! * one **reader** task per connection, running the parser state machine and
//!   doing routing work inline. That is what makes per-publisher ordering real
//!   rather than a hope: a client's commands are processed strictly one after
//!   another by the task that owns them.
//! * one **writer** task per connection draining a bounded, byte-counted queue of
//!   frames with vectored writes. Deliveries from other tasks hand a frame to that
//!   queue and never touch the socket.
//! * the queue is counted in **bytes**, with the reference's two thresholds: at
//!   75 % the publishing task stalls until the subscriber drains, at 100 % the
//!   subscriber is closed as a slow consumer and said nothing to.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify};

use crate::config::Config;
use crate::proto::{self, Event, Limits, Parser};
use crate::routing::{Msg, Sub};
use crate::Server;

/// A piece of the outbound queue: control-line bytes, an optional shared payload,
/// and the frame's trailing CRLF.
pub struct Frame {
    pub head: Bytes,
    pub body: Option<Bytes>,
    pub tail: Bytes,
}

/// The frame terminator, shared by every message frame.
pub const CRLF: &[u8] = b"\r\n";

impl Frame {
    /// A line the server says about a command: it carries its own CRLF.
    pub fn line(bytes: impl Into<Bytes>) -> Frame {
        let head = bytes.into();
        Frame {
            head,
            body: None,
            tail: Bytes::from_static(&[]),
        }
    }

    pub fn ok() -> Frame {
        Frame::line(Bytes::from_static(b"+OK\r\n"))
    }

    pub fn err(text: &str) -> Frame {
        Frame::line(Bytes::from(format!("-ERR '{text}'\r\n")))
    }

    pub fn pong() -> Frame {
        Frame::line(Bytes::from_static(b"PONG\r\n"))
    }

    pub fn ping() -> Frame {
        Frame::line(Bytes::from_static(b"PING\r\n"))
    }

    fn len(&self) -> usize {
        self.head.len() + self.body.as_ref().map(|b| b.len()).unwrap_or(0) + self.tail.len()
    }

    fn parts(&self) -> Vec<Bytes> {
        let mut v = Vec::with_capacity(3);
        if !self.head.is_empty() {
            v.push(self.head.clone());
        }
        if let Some(b) = &self.body {
            v.push(b.clone());
        }
        if !self.tail.is_empty() {
            v.push(self.tail.clone());
        }
        v
    }
}

/// The outcome of queueing a frame for a subscriber.
#[derive(Debug, PartialEq, Eq)]
pub enum Enqueue {
    Ok,
    /// Queued, but the subscriber crossed the stall threshold: the publisher waits
    /// for room before its next message.
    SoftLimit,
    /// Queued nothing: the subscriber is over its limit and is going away.
    SlowConsumer,
    /// The connection is closed; the frame was dropped.
    Dropped,
}

/// What CONNECT said. **Absent keys are true**, because that is what the
/// reference seeds before unmarshalling (`defaultOpts`, `go:client.go:710`). A
/// plain `#[serde(default)]` would make them false and quietly break
/// self-delivery and every `+OK`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub verbose: bool,
    pub pedantic: bool,
    pub echo: bool,
    pub headers: bool,
    pub no_responders: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            verbose: true,
            pedantic: true,
            echo: true,
            headers: false,
            no_responders: false,
        }
    }
}

impl Options {
    /// Apply one CONNECT object. Unknown keys (`lang`, `version`, `protocol`,
    /// `name`, …) are ignored, and a key that is absent keeps its current value —
    /// which is what makes a second CONNECT behave the way it was measured to.
    /// `false` means the options could not be read at all, which the reference
    /// treats as a fatal parse error: close, say nothing.
    pub fn apply_connect(&mut self, json: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(json) else {
            return false;
        };
        let Some(obj) = value.as_object() else {
            // Go unmarshals into a struct: anything but an object is a type error.
            return false;
        };
        let obj = obj.clone();
        for key in ["verbose", "pedantic", "echo", "headers", "no_responders"] {
            if let Some(v) = obj.get(key) {
                if let Some(b) = v.as_bool() {
                    match key {
                        "verbose" => self.verbose = b,
                        "pedantic" => self.pedantic = b,
                        "echo" => self.echo = b,
                        "headers" => self.headers = b,
                        _ => self.no_responders = b,
                    }
                }
            }
        }
        true
    }
}

/// Outbound queue for one connection.
pub struct Outbox {
    tx: Mutex<Option<mpsc::UnboundedSender<Frame>>>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<Frame>>>,
    pub pending: AtomicUsize,
    soft: usize,
    hard: usize,
    /// Woken when the queue drains back under the stall threshold.
    space: Notify,
    /// No writer will ever drain this queue again — the connection is closed.
    /// Separate from `tx` being `None` only in that every waiter and every
    /// `pending` check can read it without taking the lock.
    gone: AtomicBool,
}

impl Outbox {
    fn new(cfg: &Config) -> Arc<Outbox> {
        let (tx, rx) = mpsc::unbounded_channel();
        let hard = cfg.max_pending as usize;
        Arc::new(Outbox {
            tx: Mutex::new(Some(tx)),
            rx: Mutex::new(Some(rx)),
            pending: AtomicUsize::new(0),
            // 75 %: `c.out.mp/4*3` in the reference (`go:client.go:2654`).
            soft: hard / 4 * 3,
            hard,
            space: Notify::new(),
            gone: AtomicBool::new(false),
        })
    }

    fn take_rx(&self) -> Option<mpsc::UnboundedReceiver<Frame>> {
        self.rx.lock().unwrap().take()
    }

    fn enqueue(self: &Arc<Outbox>, frame: Frame) -> Enqueue {
        let len = frame.len();
        let guard = self.tx.lock().unwrap();
        let tx = match guard.as_ref() {
            Some(tx) => tx,
            None => return Enqueue::Dropped,
        };
        let before = self.pending.fetch_add(len, Ordering::AcqRel) + len;
        if before > self.hard {
            self.pending.fetch_sub(len, Ordering::AcqRel);
            return Enqueue::SlowConsumer;
        }
        if tx.send(frame).is_err() {
            self.pending.fetch_sub(len, Ordering::AcqRel);
            return Enqueue::Dropped;
        }
        if before > self.soft {
            Enqueue::SoftLimit
        } else {
            Enqueue::Ok
        }
    }

    fn drained(&self, bytes: usize) {
        if self.gone.load(Ordering::Acquire) {
            // Closing already zeroed the counter; subtracting again would wrap a
            // `usize` into a value no publisher ever gets room under.
            return;
        }
        let was = self.pending.fetch_sub(bytes, Ordering::AcqRel);
        if was > self.soft && was.saturating_sub(bytes) <= self.soft {
            self.space.notify_waiters();
        }
    }

    /// Block the calling task until this connection is back under its stall
    /// threshold. The waiter is registered with `enable` *before* the second
    /// check, so a drain that happens in between cannot be missed — the classic
    /// lost-wakeup shape.
    pub async fn wait_for_space(&self) {
        loop {
            if self.room() {
                return;
            }
            let wait = self.space.notified();
            tokio::pin!(wait);
            wait.as_mut().enable();
            if self.room() {
                return;
            }
            wait.await;
        }
    }

    fn has_room(&self) -> bool {
        self.room()
    }

    /// Room to queue another frame — or nothing left to wait for, which counts as
    /// room: the alternative is a publisher parked on a corpse.
    fn room(&self) -> bool {
        self.gone.load(Ordering::Acquire) || self.pending.load(Ordering::Acquire) <= self.soft
    }

    /// Called when the connection goes away: a publisher waiting for room from a
    /// dead subscriber must not wait for a drain that will never come. The queued
    /// frames are gone with the sender, so the byte counter goes with them —
    /// `room()` and the wait loop both key off it.
    fn no_more_writes(&self) {
        self.gone.store(true, Ordering::Release);
        self.pending.store(0, Ordering::Release);
        self.space.notify_waiters();
    }
}

/// One client connection.
pub struct Conn {
    pub cid: u64,
    pub peer: SocketAddr,
    pub server: Arc<Server>,
    pub out: Arc<Outbox>,
    opts: Mutex<Options>,
    subs: Mutex<HashMap<Bytes, Arc<Sub>>>,
    closed: AtomicBool,
    reason: Mutex<Option<String>>,
    /// Server PINGs sent with no PONG back.
    pings_out: AtomicU64,
    /// Wakes the writer when a foreign close says "stop trying to flush".
    wake: Notify,
    hard: AtomicBool,
    self_ref: Mutex<Weak<Conn>>,
}

impl Conn {
    pub fn opts(&self) -> Options {
        self.opts.lock().unwrap().clone()
    }

    pub fn echo(&self) -> bool {
        self.opts.lock().unwrap().echo
    }

    pub fn supports_headers(&self) -> bool {
        self.opts.lock().unwrap().headers
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn is_same(&self, other: &Arc<Conn>) -> bool {
        std::ptr::eq(self, Arc::as_ptr(other))
    }

    pub fn has_room(&self) -> bool {
        self.out.has_room()
    }

    pub fn enqueue(&self, frame: Frame) -> Enqueue {
        if self.is_closed() {
            return Enqueue::Dropped;
        }
        self.out.enqueue(frame)
    }

    /// Say nothing and hang up: that is what the reference does to a subscriber
    /// whose buffer filled (contract §7).
    pub fn close_slow_consumer(&self) {
        self.close_now("slow consumer: max_pending exceeded");
    }

    /// End the connection, letting whatever is already queued go out first. This
    /// is the path a connection takes for itself: protocol error, EOF, PONG-less
    /// staleness (after the `-ERR` has been queued).
    pub fn close(&self, reason: impl Into<String>) {
        self.finish(reason, false);
    }

    /// End the connection *now*: the queue is dropped and the writer stops trying.
    /// Used on a subscriber that filled its buffer, where the reference closes
    /// without saying anything (contract §7).
    pub fn close_now(&self, reason: impl Into<String>) {
        self.finish(reason, true);
    }

    fn finish(&self, reason: impl Into<String>, hard: bool) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let reason = reason.into();
        if self.server.cfg.debug {
            // `-D` only: the close path runs for every connection, and the
            // harness pipes stderr into the void.
            eprintln!(
                "[{}] [{:>5}] cid={} closing: {}",
                self.server.server_name,
                "DEBUG",
                self.cid,
                reason
            );
        }
        *self.reason.lock().unwrap() = Some(reason);
        self.hard.store(hard, Ordering::Release);
        // Unregister first: a closed connection must not keep interest alive.
        if let Some(me) = self.self_ref.lock().unwrap().upgrade() {
            self.server.registry.remove_conn(&me);
        }
        self.subs.lock().unwrap().clear();
        *self.out.tx.lock().unwrap() = None;
        self.out.no_more_writes();
        self.wake.notify_waiters();
    }

    pub fn pings_out(&self) -> u64 {
        self.pings_out.load(Ordering::Relaxed)
    }

    pub fn note_ping_sent(&self) {
        self.pings_out.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_pong(&self) {
        self.pings_out.store(0, Ordering::Relaxed);
    }

}

/// Spawn the two tasks for an accepted connection. The INFO line is queued before
/// anything can be read, so it is always the first byte the client sees.
pub async fn spawn(server: Arc<Server>, socket: TcpStream, cid: u64) {
    let peer = socket
        .peer_addr()
        .unwrap_or_else(|_| ([127, 0, 0, 1], 0).into());
    let (read, write) = socket.into_split();
    let out = Outbox::new(&server.cfg);

    let conn = Arc::new_cyclic(|me: &Weak<Conn>| Conn {
        cid,
        peer,
        server: server.clone(),
        out: out.clone(),
        opts: Mutex::new(Options::default()),
        subs: Mutex::new(HashMap::new()),
        closed: AtomicBool::new(false),
        reason: Mutex::new(None),
        pings_out: AtomicU64::new(0),
        wake: Notify::new(),
        hard: AtomicBool::new(false),
        self_ref: Mutex::new(me.clone()),
    });

    let line = server.info_line(cid, &peer);
    if out.enqueue(Frame::line(Bytes::from(line))) == Enqueue::Dropped {
        return;
    }

    let writer = tokio::spawn(write_loop(out.clone(), write, conn.clone()));
    read_loop(server.clone(), conn.clone(), read).await;
    // Reading is done: no more commands, and everything they queued is in the
    // writer's hands. Closing the sender lets it flush and finish.
    *out.tx.lock().unwrap() = None;
    if tokio::time::timeout(Duration::from_secs(10), writer).await.is_err() {
        // The write deadline is the real timer; this is only the backstop that
        // keeps a wedged socket from leaking the task.
        conn.close_now("write deadline");
    }
}

/// True when `json` is a JSON object — the shape the reference unmarshals a
/// client's `INFO` into, and the reason `INFO 5` and `INFO [1,2]` are fatal to
/// the connection while `INFO {}` is not.
fn info_is_an_object(json: &[u8]) -> bool {
    matches!(
        serde_json::from_slice::<serde_json::Value>(json),
        Ok(v) if v.is_object()
    )
}

/// Is a foreign close asking the writer to stop flushing?
fn hard_closed(conn: &Conn) -> bool {
    conn.hard.load(Ordering::Acquire)
}

// ------------------------------------------------------------------ reader ---

async fn read_loop(server: Arc<Server>, conn: Arc<Conn>, mut read: OwnedReadHalf) {
    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut parser = Parser::new();
    // The first probe is short whatever is configured; afterwards, the interval.
    let first = keepalive_first(server.cfg.ping_interval);
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + first,
        server.cfg.ping_interval,
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if conn.is_closed() {
            break;
        }
        if buf.capacity() - buf.len() < 8 * 1024 {
            buf.reserve(16 * 1024);
        }
        let step = tokio::select! {
            r = read.read_buf(&mut buf) => Some(r),
            _ = conn.wake.notified() => None,
            _ = ticker.tick() => {
                // Server-initiated keepalive. Two unanswered probes and the
                // reference closes with `-ERR 'Stale Connection'` (contract §7).
                if conn.pings_out() >= crate::config::MAX_PINGS_OUT {
                    conn.enqueue(Frame::err(proto::STALE_CONNECTION));
                    conn.close("stale connection");
                    break;
                }
                conn.note_ping_sent();
                conn.enqueue(Frame::ping());
                continue;
            }
        };
        let n = match step {
            Some(Ok(n)) => n,
            Some(Err(e)) => {
                conn.close(format!("read error: {e}"));
                break;
            }
            None => break, // closed by another task
        };
        if n == 0 {
            conn.close("client closed");
            break;
        }

        let lim = limits_of(&server);
        let events = match parser.feed(&mut buf, &lim) {
            Ok(events) => events,
            Err(err) => {
                if let Some(text) = err.err_line() {
                    conn.enqueue(Frame::err(text));
                }
                if err.closes() {
                    conn.close(format!("protocol error: {err:?}"));
                    break;
                }
                // Class B: said its piece, still serving.
                continue;
            }
        };
        for ev in events {
            if !handle(&server, &conn, ev).await {
                conn.close("connection closed by protocol");
                return;
            }
        }
    }
    conn.close("ended");
}

/// `min(ping_interval, 2 s)` plus up to 20 % of jitter, like the reference's
/// `setFirstPingTimer` (measured: 2.25–2.34 s with the default interval).
fn keepalive_first(interval: Duration) -> Duration {
    let cap = crate::config::FIRST_PING_INTERVAL.min(interval);
    let jitter = crate::nuid::Rng::new().below((cap.as_millis() as u64 / 5).max(1));
    cap + Duration::from_millis(jitter)
}

fn limits_of(server: &Server) -> Limits {
    Limits {
        max_control_line: server.cfg.max_control_line,
        max_payload: server.cfg.max_payload,
    }
}

/// Process one parsed operation. Returns false when the connection must go away
/// with no further words — the class-C path, decided in one place.
async fn handle(server: &Arc<Server>, conn: &Arc<Conn>, ev: Event) -> bool {
    let verbose = conn.opts().verbose;
    match ev {
        Event::Connect(json) => {
            let ok = {
                let mut opts = conn.opts.lock().unwrap();
                opts.apply_connect(&json)
            };
            if !ok {
                // Go cannot unmarshal the options: the connection is done, and it
                // says nothing.
                return false;
            }
            // The verbose value that applies *after* the parse governs this line
            // (measured; contract §5).
            if conn.opts().verbose {
                conn.enqueue(Frame::ok());
            }
        }
        Event::Subscribe { subject, queue, sid } => {
            if !crate::subjects::is_valid(&subject)
                || queue.as_ref().is_some_and(|q| !crate::subjects::is_valid(q))
            {
                server.send_err(conn, proto::INVALID_SUBJECT);
                return true;
            }
            let known = conn.subs.lock().unwrap().contains_key(&sid);
            // Re-using an sid keeps the original subscription and ignores the new
            // one, whichever filter it carried (contract §3).
            if !known {
                let me = conn.self_ref.lock().unwrap().upgrade().expect("alive");
                let sub = Sub::new(subject, queue, sid.clone(), me);
                if server.registry.insert(sub.clone()) {
                    conn.subs.lock().unwrap().insert(sid, sub);
                }
            }
            if verbose {
                conn.enqueue(Frame::ok());
            }
        }
        Event::Unsubscribe { sid, max } => {
            let sub = conn.subs.lock().unwrap().get(&sid).cloned();
            if let Some(sub) = sub {
                let gone = sub.set_limit(max);
                if gone {
                    conn.subs.lock().unwrap().remove(&sid);
                    server.registry.remove(&sub);
                }
            }
            // An unknown sid is not an error (contract §3).
            if verbose {
                conn.enqueue(Frame::ok());
            }
        }
        Event::Publish {
            subject,
            reply,
            hdr,
            total,
            hpub,
            body,
        } => {
            debug_assert_eq!(body.len(), total);
            let opts = conn.opts();
            if hpub && !opts.headers {
                // The reference's ErrMsgHeadersNotSupported: a parse error, so the
                // connection goes without a word.
                return false;
            }
            if opts.pedantic && !crate::subjects::is_valid_publish(&subject) {
                server.send_err(conn, proto::INVALID_PUBLISH_SUBJECT);
            }
            if opts.verbose {
                conn.enqueue(Frame::ok());
            }
            if subject.is_empty() {
                // Nothing can match an empty subject and the reference drops it.
                return true;
            }
            let me = conn.self_ref.lock().unwrap().upgrade().expect("alive");
            let msg = Msg {
                subject: subject.clone(),
                reply: reply.clone(),
                hdr,
                body,
            };
            let outcome = server.registry.route(&msg, &me);
            server.no_responder_check(&me, &msg, reply.as_ref(), outcome.count);
            for stalled in outcome.stall {
                // The publisher waits for room on the subscriber it overflowed,
                // and only ever on its own task: that is what keeps ordering while
                // still bounding memory. A closed victim stops the wait at once.
                loop {
                    if stalled.is_closed() {
                        break;
                    }
                    stalled.out.wait_for_space().await;
                    if stalled.out.room() || stalled.is_closed() {
                        break;
                    }
                }
            }
        }
        Event::Ping => {
            conn.enqueue(Frame::pong());
        }
        Event::Pong => conn.note_pong(),
        Event::Info(arg) => {
            // Parsed, then dropped — but only if it parses. A client-sent INFO
            // whose argument is not a JSON object is the same silent close a bad
            // CONNECT gets (contract §3, measured).
            if !info_is_an_object(&arg) {
                return false;
            }
        }
        Event::ClientOk => {}
        Event::HpubAttempt(outcome) => {
            // The capability decides here, not in the parser. Without headers any
            // HPUB goes silent, whether the line was well formed or not; with
            // them a good line is a no-op (its `Publish` follows), a bad argument
            // list is still silence and a broken frame is class A. All four
            // measured against the reference.
            if !conn.opts().headers {
                return false;
            }
            match outcome {
                proto::HpubOutcome::Ok => {}
                proto::HpubOutcome::Args => return false,
                proto::HpubOutcome::Frame(text) => {
                    server.send_err(conn, text);
                    return false;
                }
            }
        }
        Event::ClientErr => return false,
    }
    true
}

// ------------------------------------------------------------------ writer ---

/// Stop collecting frames into one batch at this many bytes.
///
/// The cap exists to bound how long one `writev` can hold the writer task, not
/// to bound memory: the pending-bytes accounting already does that. It used to
/// be 64 *buffers*, which at three parts per `MSG` frame is ~21 messages per
/// syscall — measured (`specs/perf-notes.md`), 11.8× the reference's
/// ~1 writev per 460 messages, and the single largest source of the CPU
/// difference on the pubsub path.
const MAX_BATCH_BYTES: usize = 256 * 1024;

/// Never hand the kernel more buffers than `IOV_MAX`: above it `writev` fails
/// with EINVAL, so this is the real ceiling on a batch of small frames.
const MAX_BATCH_PARTS: usize = 1000;

async fn write_loop(out: Arc<Outbox>, mut write: OwnedWriteHalf, conn: Arc<Conn>) {
    let mut rx = match out.take_rx() {
        Some(rx) => rx,
        None => return,
    };
    let deadline = conn.server.cfg.write_deadline;
    let mut parts: VecDeque<Bytes> = VecDeque::new();
    let mut batch_bytes = 0usize;

    loop {
        if parts.is_empty() {
            // Wait for work, but notice a foreign close. `recv` yields None only
            // when the sender is gone *and* the queue is empty, so queued frames
            // are never abandoned by a close that is not a hard one.
            tokio::select! {
                biased;
                frame = rx.recv() => match frame {
                    Some(frame) => {
                        batch_bytes += frame.len();
                        parts.extend(frame.parts());
                        while let Ok(frame) = rx.try_recv() {
                            batch_bytes += frame.len();
                            parts.extend(frame.parts());
                            if parts.len() >= MAX_BATCH_PARTS || batch_bytes >= MAX_BATCH_BYTES {
                                break;
                            }
                        }
                    }
                    None => break,
                },
                _ = conn.wake.notified() => {
                    if hard_closed(&conn) {
                        return;
                    }
                }
            }
        }
        if parts.is_empty() {
            continue;
        }

        while batch_bytes > 0 {
            if conn.is_closed() && hard_closed(&conn) {
                out.drained(batch_bytes);
                return;
            }
            let slices: Vec<_> = parts.iter().map(|p| std::io::IoSlice::new(p)).collect();
            let written = tokio::select! {
                r = tokio::time::timeout(deadline, write.write_vectored(&slices)) => r,
                _ = conn.wake.notified() => {
                    if hard_closed(&conn) {
                        out.drained(batch_bytes);
                        return;
                    }
                    continue;
                }
            };
            match written {
                Err(_) => {
                    // The write deadline is how the reference notices a socket
                    // that stopped taking data.
                    out.drained(batch_bytes);
                    conn.close_now("write deadline exceeded");
                    return;
                }
                Ok(Ok(0)) => {
                    out.drained(batch_bytes);
                    conn.close("socket stopped accepting data");
                    return;
                }
                Ok(Ok(n)) => {
                    consume(&mut parts, n);
                    batch_bytes -= n;
                    out.drained(n);
                }
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Ok(Err(e)) => {
                    out.drained(batch_bytes);
                    conn.close(format!("write error: {e}"));
                    return;
                }
            }
        }
        parts.clear();
    }
    conn.close("written");
}

/// Drop `n` bytes from the front of `parts`, splitting the buffer that ends
/// mid-way. Keeps the remaining payloads shared rather than copying them.
fn consume(parts: &mut VecDeque<Bytes>, n: usize) {
    let mut left = n;
    while left > 0 {
        let front = match parts.front_mut() {
            Some(f) => f,
            None => return,
        };
        if front.len() <= left {
            left -= front.len();
            parts.pop_front();
        } else {
            front.advance(left);
            left = 0;
        }
    }
}
