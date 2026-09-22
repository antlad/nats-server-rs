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

use crate::allocstats::{tag, Site};
use crate::arena::Arena;
use crate::config::Config;
use crate::proto::{self, Event, Limits, Parser};
use crate::routing::{self, Msg, Sub};
use crate::Server;

/// One frame: everything the writer must put on the wire for one server
/// statement, laid out contiguously in one buffer.
///
/// It used to be three parts (`head`, `body`, `tail`) because the head was built
/// on the publishing task and the payload was copied on its own. Contiguity is
/// what lets a delivery cost one memcpy and one `write` instead of two
/// allocations and a three-buffer iovec, and it is why `Frame` is a newtype now
/// — see [`crate::arena`] for where the bytes come from.
#[derive(Clone)]
pub struct Frame(pub Bytes);

impl Frame {
    /// A line the server says about a command: it carries its own CRLF.
    pub fn line(bytes: impl Into<Bytes>) -> Frame {
        Frame(bytes.into())
    }

    pub fn ok() -> Frame {
        Frame::line(Bytes::from_static(b"+OK\r\n"))
    }

    pub fn err(text: &str) -> Frame {
        tag(Site::ControlLine);
        Frame::line(Bytes::from(format!("-ERR '{text}'\r\n")))
    }

    pub fn pong() -> Frame {
        Frame::line(Bytes::from_static(b"PONG\r\n"))
    }

    pub fn ping() -> Frame {
        Frame::line(Bytes::from_static(b"PING\r\n"))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    /// Hand this frame's buffer to the writer's list, by moving it.
    fn write_into(self, dst: &mut VecDeque<Bytes>) {
        if !self.0.is_empty() {
            dst.push_back(self.0);
        }
    }
}

/// The frame terminator. `build_frame` writes it *into* the frame; it is not a
/// third buffer for the kernel to be told about.
pub const CRLF: &[u8] = b"\r\n";

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// The five flags as one word, because `Conn` keeps them in an atomic: every
    /// other connection reads them while it routes (`echo`, `headers`), and a
    /// `Mutex<Options>` there cost an uncontended lock pair per read — three
    /// reads per published message, which is real time at a million messages a
    /// second. The word is written only by the connection's own reader task.
    fn pack(&self) -> u64 {
        (self.verbose as u64)
            | (self.pedantic as u64) << 1
            | (self.echo as u64) << 2
            | (self.headers as u64) << 3
            | (self.no_responders as u64) << 4
    }

    fn unpack(word: u64) -> Options {
        Options {
            verbose: word & 1 != 0,
            pedantic: word & 2 != 0,
            echo: word & 4 != 0,
            headers: word & 8 != 0,
            no_responders: word & 16 != 0,
        }
    }

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

/// The queue's own half of an outbox: the sender, and the arena that every frame
/// queued through it is laid out in.
///
/// They share one lock deliberately. Frames must be *laid down* in the order the
/// writer will drain them — that is the arena's recycling rule, which is the
/// ordering promise restated — and the queue lock is where that order is decided.
struct Sender {
    tx: mpsc::UnboundedSender<Frame>,
    arena: Arena,
}

/// Outbound queue for one connection.
pub struct Outbox {
    tx: Mutex<Option<Sender>>,
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

/// Account for `frame` and hand it to the writer. The queue lock is held by the
/// caller: `pending` is only compared against the limits consistently while
/// nobody else can be queuing.
fn push(sender: &mut Sender, out: &Outbox, frame: Frame) -> Enqueue {
    let len = frame.len();
    let before = out.pending.fetch_add(len, Ordering::AcqRel) + len;
    if before > out.hard {
        out.pending.fetch_sub(len, Ordering::AcqRel);
        return Enqueue::SlowConsumer;
    }
    if sender.tx.send(frame).is_err() {
        out.pending.fetch_sub(len, Ordering::AcqRel);
        return Enqueue::Dropped;
    }
    if before > out.soft {
        Enqueue::SoftLimit
    } else {
        Enqueue::Ok
    }
}

impl Outbox {
    fn new(cfg: &Config) -> Arc<Outbox> {
        let (tx, rx) = mpsc::unbounded_channel();
        let hard = cfg.max_pending as usize;
        Arc::new(Outbox {
            tx: Mutex::new(Some(Sender {
                tx,
                arena: Arena::new(),
            })),
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
        let mut guard = self.tx.lock().unwrap();
        match guard.as_mut() {
            Some(sender) => push(sender, self, frame),
            None => Enqueue::Dropped,
        }
    }

    /// Queue a delivery. `build` lays the frame out in this connection's arena
    /// and is called with the queue lock held, so the frames in a chunk are in
    /// exactly the order the writer will hand them to the kernel — and a
    /// connection that is already gone costs the call, not the bytes.
    fn queue_with(
        self: &Arc<Outbox>,
        build: impl FnOnce(&mut Arena) -> Frame,
    ) -> Enqueue {
        let mut guard = self.tx.lock().unwrap();
        let sender = match guard.as_mut() {
            Some(sender) => sender,
            None => return Enqueue::Dropped,
        };
        let frame = build(&mut sender.arena);
        push(sender, self, frame)
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
    /// CONNECT's flags, packed; see `Options::pack`.
    flags: AtomicU64,
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
        Options::unpack(self.flags.load(Ordering::Acquire))
    }

    /// Only the connection's own reader task calls this.
    fn set_opts(&self, opts: Options) {
        self.flags.store(opts.pack(), Ordering::Release);
    }

    pub fn echo(&self) -> bool {
        self.flags.load(Ordering::Acquire) & 4 != 0
    }

    pub fn supports_headers(&self) -> bool {
        self.flags.load(Ordering::Acquire) & 8 != 0
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

    /// Queue one subscriber's copy of one message. The frame is built into this
    /// connection's arena, so a delivery is one memcpy and no allocation, and the
    /// `sub`/`msg` bytes are only ever read — nothing here outlives the call.
    pub fn deliver(&self, sub: &Sub, msg: &Msg<'_>) -> Enqueue {
        if self.is_closed() {
            return Enqueue::Dropped;
        }
        self.out
            .queue_with(|arena| routing::build_frame(arena, sub, msg))
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
                self.server.server_name, "DEBUG", self.cid, reason
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

    tag(Site::Connection);
    let conn = Arc::new_cyclic(|me: &Weak<Conn>| Conn {
        cid,
        peer,
        server: server.clone(),
        out: out.clone(),
        flags: AtomicU64::new(Options::default().pack()),
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
    if tokio::time::timeout(Duration::from_secs(10), writer)
        .await
        .is_err()
    {
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

/// The per-connection read buffer, and how it moves.
///
/// The reference starts at 512 B and doubles whenever a read came back full, to
/// 64 KiB (`go:client.go:110-112`, `:1675-1685`), shrinking again when reads stop
/// filling it. It matters because one `recvfrom` per 110 messages is three times
/// as many syscalls as one per 344, and every one of them copies the same bytes
/// a bigger buffer would have taken in a single go.
const START_READ_BUF: usize = 16 * 1024;
const MAX_READ_BUF: usize = 64 * 1024;
/// Consecutive short reads before the buffer halves, as in the reference's
/// `shortsToShrink`.
const SHORTS_TO_SHRINK: u32 = 2;

async fn read_loop(server: Arc<Server>, conn: Arc<Conn>, mut read: OwnedReadHalf) {
    let mut cap = START_READ_BUF;
    let mut short = 0u32;
    tag(Site::ReadBuf);
    let mut buf = BytesMut::with_capacity(cap);
    // The batch this read produced. Reused, so a connection's events live in the
    // same pages read after read (`proto::Parser::feed_into`).
    let mut events: Vec<Event> = Vec::new();
    // What the last publish's subject resolved to, and where the connections
    // worth waiting for go. Both belong to this task, both keep their capacity
    // for the life of the connection, and together they are why a publish costs no
    // allocation and no registry lock once the subject has been seen once.
    let mut interest = routing::Interest::new();
    let mut stall: Vec<Arc<Conn>> = Vec::new();
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
        // Same trap as the arena: `BytesMut` answers `BufMut::remaining_mut`
        // with `usize::MAX - len`, so "is there room" is always yes, `reserve` is
        // never called, and the growth Part 2 measured as change 7 happens instead
        // inside `chunk_mut`, one implicit 64-byte reservation at a time. The real
        // question is capacity left over what is still in the buffer.
        if buf.capacity() - buf.len() < cap {
            // Only a real allocation if `reserve` could not reclaim in place —
            // which needs every view of the old buffer to be dead, and the
            // straddling frame's are not (Task 23.3).
            tag(Site::ReadBuf);
            // `reserve` reuses the allocation in place when nothing still holds a
            // view of it, which — with the batch's views dropped below — is the
            // normal case: the reader's buffer is then one allocation for the
            // life of the connection.
            buf.reserve(cap - (buf.capacity() - buf.len()));
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
        // Grow on a full read, shrink after enough short ones.
        if n >= cap {
            short = 0;
            cap = (cap * 2).min(MAX_READ_BUF);
        } else if n < cap / 2 {
            short += 1;
            if short > SHORTS_TO_SHRINK && cap > START_READ_BUF {
                short = 0;
                cap /= 2;
                if buf.is_empty() {
                    tag(Site::ReadBuf);
                buf = BytesMut::with_capacity(cap);
                }
            }
        }

        let lim = limits_of(&server);
        // Parse the whole segment first, then run its operations in order: a
        // parse error that follows good commands in the same read belongs to the
        // reference's "the connection is gone, say this one thing" class, and
        // what the earlier commands did before the close is measured parity
        // (`specs/parity-log.md`), so the batch is not dispatched as it fills.
        if events.capacity() <= events.len() {
            tag(Site::Events);
        }
        events.clear();
        if let Err(err) = parser.feed_into(&mut buf, &lim, &mut events) {
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
        for ev in events.drain(..) {
            if !handle(&server, &conn, ev, &mut interest, &mut stall).await {
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
///
/// `interest` and `stall` are the reader task's, threaded down to the router: the
/// alternative — a `Vec` built per message to hold the answer — is the allocation
/// the delivery gate is about.
async fn handle(
    server: &Arc<Server>,
    conn: &Arc<Conn>,
    ev: Event,
    interest: &mut routing::Interest,
    stall: &mut Vec<Arc<Conn>>,
) -> bool {
    let verbose = conn.opts().verbose;
    match ev {
        Event::Connect(json) => {
            let mut opts = conn.opts();
            let ok = opts.apply_connect(&json);
            if !ok {
                // Go cannot unmarshal the options: the connection is done, and it
                // says nothing.
                return false;
            }
            // The verbose value that applies *after* the parse governs this line
            // (measured; contract §5).
            conn.set_opts(opts);
            if opts.verbose {
                conn.enqueue(Frame::ok());
            }
        }
        Event::Subscribe {
            subject,
            queue,
            sid,
        } => {
            if !crate::subjects::is_valid(&subject)
                || queue
                    .as_ref()
                    .is_some_and(|q| !crate::subjects::is_valid(q))
            {
                server.send_err(conn, proto::INVALID_SUBJECT);
                return true;
            }
            let known = conn.subs.lock().unwrap().contains_key(&sid);
            // Re-using an sid keeps the original subscription and ignores the new
            // one, whichever filter it carried (contract §3).
            if !known {
                let sub = Sub::new(subject, queue, sid.clone(), conn.clone());
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
            // Borrowed, not cloned: routing looks at these bytes and the only
            // copy worth making is the one it takes when someone takes delivery.
            let msg = Msg {
                subject: &subject,
                reply: reply.as_deref(),
                hdr,
                body: &body,
            };
            let outcome = server.registry.route(&msg, conn, interest, stall);
            server.no_responder_check(conn, &msg, reply.as_deref(), outcome.count);
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
/// with EINVAL, so this is the real ceiling on a batch. A frame is one buffer
/// now, so at 256 B payloads the byte cap is what a full batch runs into first.
const MAX_BATCH_PARTS: usize = 1000;

/// Buffers a batch of this size or smaller is described to the kernel from the
/// stack.
///
/// The point is the common case: a writer that wakes up to find exactly one
/// message waiting gets a plain `write`, no iovec array and no allocation, and
/// one message at a time is what a network that is not saturated looks like. The
/// array covers a burst of a few dozen, and past that the batch is large enough
/// that one `Vec` per *batch* — not per message — is what the kernel wants anyway.
const STACK_IOV: usize = 32;

async fn write_loop(out: Arc<Outbox>, mut write: OwnedWriteHalf, conn: Arc<Conn>) {
    let mut rx = match out.take_rx() {
        Some(rx) => rx,
        None => return,
    };
    let deadline = conn.server.cfg.write_deadline;
    let mut parts: VecDeque<Bytes> = VecDeque::new();
    let mut batch_bytes = 0usize;
    // One timer for the life of the writer, re-armed per attempt. Building the
    // deadline with `tokio::time::timeout` allocates a `Sleep` and inserts a new
    // entry in the timer wheel *for every write attempt*, which at one message per
    // attempt is one allocation per delivered message (`specs/perf-notes.md`).
    let mut timer = Box::pin(tokio::time::sleep(Duration::ZERO));

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
                        frame.write_into(&mut parts);
                        while let Ok(frame) = rx.try_recv() {
                            batch_bytes += frame.len();
                            frame.write_into(&mut parts);
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
            timer
                .as_mut()
                .reset(tokio::time::Instant::now() + deadline);
            let written = tokio::select! {
                biased;
                r = write_batch(&mut write, &mut parts) => r,
                _ = timer.as_mut() => Err(error_at_deadline()),
                _ = conn.wake.notified() => {
                    if hard_closed(&conn) {
                        out.drained(batch_bytes);
                        return;
                    }
                    continue;
                }
            };
            match written {
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    // The write deadline is how the reference notices a socket
                    // that stopped taking data.
                    out.drained(batch_bytes);
                    conn.close_now("write deadline exceeded");
                    return;
                }
                Ok(0) => {
                    out.drained(batch_bytes);
                    conn.close("socket stopped accepting data");
                    return;
                }
                Ok(n) => {
                    consume(&mut parts, n);
                    batch_bytes -= n;
                    out.drained(n);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
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

/// The deadline as an error, so that one `select!` can tell "the socket stopped
/// taking data" from "the socket broke" by its kind.
fn error_at_deadline() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, "write deadline exceeded")
}

/// One write attempt over the whole batch, in the cheapest shape that describes
/// it: `write` for a single frame, a stack array of iovecs for a small batch, a
/// `Vec` of them for a big one.
async fn write_batch(write: &mut OwnedWriteHalf, parts: &mut VecDeque<Bytes>) -> std::io::Result<usize> {
    match parts.len() {
        0 => Ok(0),
        // The one-message-at-a-time case, and the one that must not allocate:
        // take the buffer out of the queue for the duration of the call, and put
        // back only what the kernel did not take.
        1 => {
            let mut buf = parts.pop_front().expect("one buffer");
            match write.write(&buf).await {
                Ok(n) if n < buf.len() => {
                    buf.advance(n);
                    parts.push_front(buf);
                    Ok(n)
                }
                r => {
                    if r.is_err() {
                        parts.push_front(buf);
                    }
                    r
                }
            }
        }
        n if n <= STACK_IOV => {
            // `Bytes::default()` is the empty buffer, and an empty iovec is a
            // no-op to `writev`, so padding a short batch out to a fixed array
            // costs nothing but the stack.
            let slots: [Bytes; STACK_IOV] = std::array::from_fn(|i| {
                parts.get(i).cloned().unwrap_or_default()
            });
            let iov: [std::io::IoSlice; STACK_IOV] =
                std::array::from_fn(|i| std::io::IoSlice::new(&slots[i]));
            write.write_vectored(&iov[..n]).await
        }
        _ => {
            let slices: Vec<std::io::IoSlice> =
                parts.iter().map(|b| std::io::IoSlice::new(b)).collect();
            write.write_vectored(&slices).await
        }
    }
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
