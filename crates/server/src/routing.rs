//! The subscription registry and the delivery decision.
//!
//! One process-wide registry maps subjects to subscriptions. Routing happens on
//! the publishing connection's task, under one lock, because that is what keeps
//! the ordering promise: within one publisher and one subscriber, deliveries
//! happen in publish order (contract §6). Anything that would let a second
//! publisher interleave *between* matching and enqueueing is still correct — the
//! promise is per publisher — but taking the lock once per message is simpler to
//! reason about and costs one lock, not one per subscriber.
//!
//! The shape is deliberately not the reference's partitioned sublist: a literal
//! `HashMap` plus a linear scan of the wildcard filters. The semantics match
//! (`go:sublist.go`), the structure is ours, and `specs/perf-notes.md` carries
//! the measured cost of the scan.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;

use crate::client::{Conn, Enqueue, Frame};
use crate::subjects;
use crate::Rng;

/// One subscription, as the registry sees it.
pub struct Sub {
    /// The filter the client asked for.
    pub subject: Bytes,
    /// Queue group name, if any.
    pub queue: Option<Bytes>,
    /// The client's own id for this subscription — what goes into MSG/HMSG.
    pub sid: Bytes,
    pub conn: Arc<Conn>,
    /// Messages delivered so far, for auto-unsub.
    delivered: AtomicU64,
    /// Total delivery limit; 0 means unlimited. Set by `UNSUB sid max`.
    limit: AtomicU64,
    /// Precomputed so a publish never re-scans the filter for wildcards.
    pub wildcard: bool,
}

impl Sub {
    pub fn new(subject: Bytes, queue: Option<Bytes>, sid: Bytes, conn: Arc<Conn>) -> Arc<Sub> {
        let wildcard = subjects::has_wildcard(&subject);
        Arc::new(Sub {
            subject,
            queue,
            sid,
            conn,
            delivered: AtomicU64::new(0),
            limit: AtomicU64::new(0),
            wildcard,
        })
    }

    /// `UNSUB sid <max>`: raise the limit, or unsubscribe when the limit has
    /// already been reached (`max <= delivered`, or a non-positive/unparseable
    /// max). Mirrors `go:client.go:processUnsub`.
    pub fn set_limit(&self, max: i64) -> bool {
        if max > 0 && max as u64 > self.delivered.load(Ordering::Relaxed) {
            self.limit.store(max as u64, Ordering::Relaxed);
            false
        } else {
            self.limit.store(0, Ordering::Relaxed);
            true
        }
    }

    /// Count a delivery. Returns true when the subscription is spent.
    fn account(&self) -> bool {
        let n = self.delivered.fetch_add(1, Ordering::Relaxed) + 1;
        let max = self.limit.load(Ordering::Relaxed);
        max != 0 && n >= max
    }

    /// A message that must be delivered here: same-connection traffic when the
    /// publisher has echo off is the only thing that is skipped for ordering
    /// reasons.
    fn eligible(&self, from: &Arc<Conn>) -> bool {
        !self.conn.is_closed() && !(self.conn.is_same(from) && !from.echo())
    }
}

/// A message on its way from a publisher to the registry, borrowed from the
/// publish event.
///
/// Routing needs to *look* at these bytes, not own them: the only publisher
/// worth its throughput spends one pass over the wire and none on the payload,
/// and a message nobody subscribes to to goes no further than the batch it was
/// parsed into. `route` copies the body exactly once, and only when somebody is
/// actually going to write it — see [`Registry::route`].
pub struct Msg<'a> {
    pub subject: &'a [u8],
    pub reply: Option<&'a [u8]>,
    /// Header-block length within `body`.
    pub hdr: usize,
    pub body: &'a Bytes,
}

/// What one route call produced.
pub struct Delivered {
    /// How many subscriptions the message reached. Queue groups count once.
    pub count: usize,
    /// Connections the publisher should wait on before its next message: they
    /// crossed the 75 % soft limit while this message was being queued.
    pub stall: Vec<Arc<Conn>>,
}

/// The subject map's hasher: one multiply-xor per 8 bytes of subject, seeded
/// once per process.
///
/// The standard `SipHash-1-3` is designed to survive a hostile key, and a
/// publisher pays 15–20 ns for that on *every message* — at the reference's
/// rate, most of the difference between "found no interest" and "found no
/// interest quickly" (`specs/perf-notes.md`). The seed keeps the map from being
/// an easy target for a client that chooses its own subjects, and equality is
/// untouched: this only decides which bucket a subject lands in.
#[derive(Clone, Copy)]
struct SubjectHasher(u64);

impl Default for SubjectHasher {
    #[inline]
    fn default() -> SubjectHasher {
        SubjectHasher(hash_seed())
    }
}

/// Not `RandomState` itself — that one is SipHash — but seeded the same way, so
/// two processes disagree about bucket order and a client cannot probe its way
/// into a collision.
fn hash_seed() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| RandomState::new().build_hasher().finish())
}

impl SubjectHasher {
    #[inline]
    fn mix(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(11) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

impl Hasher for SubjectHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut b = bytes;
        while b.len() >= 8 {
            self.mix(u64::from_le_bytes(b[..8].try_into().unwrap()));
            b = &b[8..];
        }
        if !b.is_empty() {
            let mut w = [0u8; 8];
            w[..b.len()].copy_from_slice(b);
            // The length goes in with the tail so `foo` and `foo\0` differ.
            self.mix(u64::from_le_bytes(w) ^ (bytes.len() as u64) << 56);
        }
    }

    #[inline]
    fn finish(&self) -> u64 {
        // Final avalanche: cheap, and it spreads the high buckets, which is what
        // the low bits of a short subject would otherwise not do.
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb) ^ (z >> 31)
    }
}

type SubjectMap = HashMap<Bytes, Vec<Arc<Sub>>, BuildHasherDefault<SubjectHasher>>;

struct Inner {
    literal: SubjectMap,
    wildcard: Vec<Arc<Sub>>,
    /// Counting subscriptions makes the 503 check cheap and exact.
    total: usize,
}

pub struct Registry {
    inner: Mutex<Inner>,
    rng: Mutex<Rng>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry {
            inner: Mutex::new(Inner {
                literal: SubjectMap::default(),
                wildcard: Vec::new(),
                total: 0,
            }),
            rng: Mutex::new(Rng::new()),
        }
    }
}

impl Registry {
    pub fn new() -> Arc<Registry> {
        Arc::new(Registry {
            inner: Mutex::new(Inner {
                literal: SubjectMap::default(),
                wildcard: Vec::new(),
                total: 0,
            }),
            rng: Mutex::new(Rng::new()),
        })
    }

    /// Register a subscription. `false` means the filter is not a legal subject,
    /// which is class B: `-ERR 'Invalid Subject'`, connection stays.
    pub fn insert(&self, sub: Arc<Sub>) -> bool {
        if !subjects::is_valid(&sub.subject) {
            return false;
        }
        let mut inner = self.inner.lock().unwrap();
        if sub.wildcard {
            if !inner.wildcard.iter().any(|s| same(s, &sub)) {
                inner.wildcard.push(sub);
                inner.total += 1;
            }
        } else {
            let entry = inner.literal.entry(sub.subject.clone()).or_default();
            if !entry.iter().any(|s| same(s, &sub)) {
                entry.push(sub);
                inner.total += 1;
            }
        }
        true
    }

    pub fn remove(&self, sub: &Arc<Sub>) {
        let mut inner = self.inner.lock().unwrap();
        inner.remove_sub(sub);
    }

    /// Drop everything a connection held. Called once, when it closes.
    pub fn remove_conn(&self, conn: &Arc<Conn>) {
        let mut inner = self.inner.lock().unwrap();
        let mut removed = 0usize;
        for list in inner.literal.values_mut() {
            let before = list.len();
            list.retain(|s| !s.conn.is_same(conn));
            removed += before - list.len();
        }
        inner.literal.retain(|_, l| !l.is_empty());
        let before = inner.wildcard.len();
        inner.wildcard.retain(|s| !s.conn.is_same(conn));
        removed += before - inner.wildcard.len();
        inner.total -= removed;
    }

    pub fn subscription_count(&self) -> usize {
        self.inner.lock().unwrap().total
    }

    /// The subscriptions of `conn` whose filter matches the literal `subject`.
    /// Used by the no-responder path, which has to write to the publisher's own
    /// inbox subscription.
    pub fn matching_of(&self, conn: &Arc<Conn>, subject: &[u8]) -> Vec<Arc<Sub>> {
        let inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        if let Some(list) = inner.literal.get(subject) {
            out.extend(list.iter().filter(|s| s.conn.is_same(conn)).cloned());
        }
        out.extend(
            inner
                .wildcard
                .iter()
                .filter(|s| s.conn.is_same(conn) && subjects::matches(&s.subject, subject))
                .cloned(),
        );
        out
    }

    /// Fan a message out. Returns who received it and which subscribers the
    /// publisher should wait for.
    pub fn route<'a>(&self, msg: &Msg<'a>, from: &Arc<Conn>) -> Delivered {
        let mut stall = Vec::new();
        let mut count = 0usize;
        let mut expired: Vec<Arc<Sub>> = Vec::new();

        // Everything the message can reach, split into plain subscriptions and
        // queue groups, collected under the registry lock.
        let (plain, groups, start) = {
            let inner = self.inner.lock().unwrap();
            let mut plain: Vec<Arc<Sub>> = Vec::new();
            let mut groups: Vec<(Option<Bytes>, Vec<Arc<Sub>>)> = Vec::new();
            let mut consider = |sub: &Arc<Sub>| match &sub.queue {
                None => plain.push(Arc::clone(sub)),
                Some(q) => match groups
                    .iter_mut()
                    .find(|(gq, _)| gq.as_deref() == Some(&q[..]))
                {
                    Some((_, members)) => members.push(Arc::clone(sub)),
                    None => groups.push((Some(q.clone()), vec![Arc::clone(sub)])),
                },
            };
            if let Some(list) = inner.literal.get(&msg.subject[..]) {
                for sub in list {
                    consider(sub);
                }
            }
            for sub in inner.wildcard.iter() {
                if subjects::matches(&sub.subject, &msg.subject) {
                    consider(sub);
                }
            }
            // The reference picks a random start index per message so that a
            // queue group stays even without a shared counter to serialise. With
            // no queue group in the batch there is nothing to be even about, and
            // asking used to take a process-wide lock on every message.
            let start = if groups.is_empty() {
                0
            } else {
                self.rng.lock().unwrap().next_u64()
            };
            (plain, groups, start)
        };

        if plain.is_empty() && groups.is_empty() {
            // No interest, short circuit if so — the reference's own comment for
            // this branch (`go:client.go:4506`), and the reason a publish to a
            // subject nobody watches costs it one hash and no copies. The payload
            // stays a view of the read buffer and dies with the batch.
            return Delivered { count, stall };
        }

        // Somebody will write these bytes, and the buffer they sit in is about to
        // be the publisher's next read. Take one owned copy here, so every frame
        // below can share it and no 64 KiB chunk ends outliving a slow consumer
        // by holding one pending message hostage.
        let body = Bytes::copy_from_slice(msg.body);
        let owned = Msg {
            subject: msg.subject,
            reply: msg.reply,
            hdr: msg.hdr,
            body: &body,
        };

        for sub in &plain {
            if !sub.eligible(from) {
                continue;
            }
            match self.give(sub, &owned, &mut expired) {
                Give::Ok => count += 1,
                Give::Stall(c) => {
                    count += 1;
                    stall.push(c);
                }
                Give::Gone => {}
            }
        }

        // One member per message per group. The reference picks a random start
        // index and probes forward, skipping members that cannot take it, so the
        // distribution is even without a shared counter to serialise.
        for (_, members) in &groups {
            let n = members.len();
            // Probe forward from a random start for a member that can take it
            // now; if every member is over its stall threshold, hand it to the
            // first eligible one anyway and let the publisher wait. Dropping the
            // message here would also make a busy queue group look unanswered and
            // leak a spurious 503.
            let pick = (0..n)
                .map(|k| &members[(start as usize + k) % n])
                .find(|s| s.eligible(from) && s.conn.has_room())
                .or_else(|| {
                    (0..n)
                        .map(|k| &members[(start as usize + k) % n])
                        .find(|s| s.eligible(from))
                });
            let Some(sub) = pick else { continue };
            {
                match self.give(sub, &owned, &mut expired) {
                    Give::Ok => count += 1,
                    Give::Stall(c) => {
                        count += 1;
                        stall.push(c);
                    }
                    Give::Gone => {}
                }
            }
        }

        if !expired.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for sub in &expired {
                inner.remove_sub(sub);
            }
        }

        Delivered { count, stall }
    }

    /// Build the frame, queue it, and book the delivery.
    fn give(&self, sub: &Arc<Sub>, msg: &Msg<'_>, expired: &mut Vec<Arc<Sub>>) -> Give {
        let frame = build_frame(sub, msg);
        match sub.conn.enqueue(frame) {
            Enqueue::Ok => {
                if sub.account() {
                    expired.push(Arc::clone(sub));
                }
                Give::Ok
            }
            Enqueue::SoftLimit => {
                if sub.account() {
                    expired.push(Arc::clone(sub));
                }
                Give::Stall(Arc::clone(&sub.conn))
            }
            Enqueue::SlowConsumer => {
                // The victim is dropped; the publisher is never punished, and the
                // message that triggered it is not counted as delivered.
                sub.conn.close_slow_consumer();
                expired.push(Arc::clone(sub));
                Give::Gone
            }
            Enqueue::Dropped => Give::Gone,
        }
    }
}

enum Give {
    Ok,
    Stall(Arc<Conn>),
    Gone,
}

impl Inner {
    /// Take one subscription out. The identity is (connection, sid): a client
    /// unsubscribing an sid removes what that sid referred to, whichever filter
    /// it turned out to be.
    fn remove_sub(&mut self, sub: &Arc<Sub>) {
        let matches = |s: &Arc<Sub>| s.sid == sub.sid && s.conn.is_same(&sub.conn);
        let removed = if sub.wildcard {
            let before = self.wildcard.len();
            self.wildcard.retain(|s| !matches(s));
            before - self.wildcard.len()
        } else {
            let gone = match self.literal.get_mut(&sub.subject) {
                Some(list) => {
                    let before = list.len();
                    list.retain(|s| !matches(s));
                    before - list.len()
                }
                None => 0,
            };
            if let Some(list) = self.literal.get(&sub.subject) {
                if list.is_empty() {
                    self.literal.remove(&sub.subject);
                }
            }
            gone
        };
        self.total -= removed;
    }
}

pub fn identical(a: &Arc<Sub>, b: &Arc<Sub>) -> bool {
    a.sid == b.sid && a.subject == b.subject && a.queue == b.queue && a.conn.is_same(&b.conn)
}

fn same(a: &Arc<Sub>, b: &Arc<Sub>) -> bool {
    a.sid == b.sid && a.subject == b.subject && a.conn.is_same(&b.conn)
}

/// `MSG <subject> <sid> [reply] <#bytes>\r\n<payload>\r\n`, or the HMSG form.
///
/// A subscriber that did not declare header support gets the stripped form — the
/// reference removes the block rather than the message
/// (`client.rs::TestClientHeaderDeliverStrippedMsg`, headers.rs).
fn build_frame(sub: &Arc<Sub>, msg: &Msg<'_>) -> Frame {
    let headers = msg.hdr > 0 && sub.conn.supports_headers();
    let body: Bytes = if msg.hdr > 0 && !headers {
        msg.body.slice(msg.hdr..)
    } else {
        msg.body.clone()
    };
    let total = body.len();
    // "MSG " / "HMSG ", the subject, the sid, maybe a reply, one or two sizes and
    // a CRLF. 48 covers the separators and digits with room to spare, which is
    // cheaper than counting them.
    let mut head =
        Vec::with_capacity(48 + msg.subject.len() + sub.sid.len() + msg.reply.unwrap_or(&[]).len());
    if headers {
        head.extend_from_slice(b"HMSG ");
    } else {
        head.extend_from_slice(b"MSG ");
    }
    head.extend_from_slice(msg.subject);
    head.push(b' ');
    head.extend_from_slice(&sub.sid);
    if let Some(reply) = msg.reply {
        head.push(b' ');
        head.extend_from_slice(reply);
    }
    head.push(b' ');
    if headers {
        // HMSG subject sid [reply] #hdr #total
        push_u64(&mut head, msg.hdr as u64);
        head.push(b' ');
    }
    push_u64(&mut head, total as u64);
    head.extend_from_slice(b"\r\n");
    Frame {
        head: Bytes::from(head),
        body: Some(body),
        tail: Bytes::from_static(crate::client::CRLF),
    }
}

/// The 503 status frame for an unanswered request (contract §6). Byte-exact:
/// the frame's subject is the *inbox*, the dead subject travels in a header.
pub fn no_responder_frame(reply: &[u8], sub: &Sub, published: &[u8]) -> Frame {
    let mut head = Vec::with_capacity(48 + reply.len() + published.len());
    head.extend_from_slice(b"HMSG ");
    head.extend_from_slice(reply);
    head.push(b' ');
    head.extend_from_slice(&sub.sid);
    let status_len = 32 + published.len();
    head.extend_from_slice(format!(" {status_len} {status_len}\r\n").as_bytes());
    let mut body = Vec::with_capacity(status_len);
    body.extend_from_slice(b"NATS/1.0 503\r\nNats-Subject: ");
    body.extend_from_slice(published);
    body.extend_from_slice(b"\r\n\r\n");
    debug_assert_eq!(body.len(), status_len);
    Frame {
        head: Bytes::from(head),
        body: Some(Bytes::from(body)),
        tail: Bytes::from_static(crate::client::CRLF),
    }
}

/// Decimal digits, straight into the buffer: the `String` this used to build was
/// one allocation per number per delivery, and an `HMSG` line has two.
fn push_u64(dst: &mut Vec<u8>, mut n: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    dst.extend_from_slice(&buf[i..]);
}
