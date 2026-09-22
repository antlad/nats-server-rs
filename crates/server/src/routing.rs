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
//! (`go:sublist.go`), the structure is ours, and `specs/perf-notes.md` carries the
//! measured cost of the scan.
//!
//! What *is* like the reference is the interest cache ([`Interest`]): a per-client
//! map of subject → resolved result, invalidated by the sublist's `genid`
//! (`c.in.results`, `go:sublist.go`). Ours lives on the publishing *task*, so it
//! needs no lock and no synchronisation, and it is what takes a repeated publish
//! to the same subject down to a byte compare and an atomic load.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::{BufMut, Bytes};

use crate::allocstats::{tag, Site};
use crate::arena::Arena;
use crate::client::{Conn, Enqueue, Frame, CRLF};
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
    /// Precomputed from `queue` so the interest cache can tell, without
    /// dereferencing an `Option<Bytes>`, that this subject needs group sampling.
    queued: bool,
}

impl Sub {
    pub fn new(subject: Bytes, queue: Option<Bytes>, sid: Bytes, conn: Arc<Conn>) -> Arc<Sub> {
        let wildcard = subjects::has_wildcard(&subject);
        let queued = queue.is_some();
        Arc::new(Sub {
            subject,
            queue,
            sid,
            conn,
            delivered: AtomicU64::new(0),
            limit: AtomicU64::new(0),
            wildcard,
            queued,
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
/// and a message nobody subscribes to goes no further than the batch it was
/// parsed into. The one copy a delivery takes is per subscriber, into that
/// subscriber's own frame arena — see [`crate::arena`].
pub struct Msg<'a> {
    pub subject: &'a [u8],
    pub reply: Option<&'a [u8]>,
    /// Header-block length within `body`.
    pub hdr: usize,
    pub body: &'a Bytes,
}

/// What one route call produced. `stall` borrows the caller's scratch rather than
/// owning a `Vec`, because an owned one was an allocation for every message that
/// reached a subscriber near its limit.
pub struct Delivered<'a> {
    /// How many subscriptions the message reached. Queue groups count once.
    pub count: usize,
    /// Connections the publisher should wait on before its next message: they
    /// crossed the 75 % soft limit while this message was being queued.
    pub stall: &'a [Arc<Conn>],
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
    /// Bumped by every change to the subscription set, under the same lock that
    /// makes the change. This is what a stale [`Interest`] is measured against.
    generation: AtomicU64,
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
            generation: AtomicU64::new(0),
        }
    }
}

impl Registry {
    pub fn new() -> Arc<Registry> {
        Arc::new(Registry::default())
    }

    /// Register a subscription. `false` means the filter is not a legal subject,
    /// which is class B: `-ERR 'Invalid Subject'`, connection stays.
    pub fn insert(&self, sub: Arc<Sub>) -> bool {
        if !subjects::is_valid(&sub.subject) {
            return false;
        }
        let mut inner = self.inner.lock().unwrap();
        let added = if sub.wildcard {
            if !inner.wildcard.iter().any(|s| same(s, &sub)) {
                inner.wildcard.push(sub);
                inner.total += 1;
                true
            } else {
                false
            }
        } else {
            let entry = inner.literal.entry(sub.subject.clone()).or_default();
            if !entry.iter().any(|s| same(s, &sub)) {
                entry.push(sub);
                inner.total += 1;
                true
            } else {
                false
            }
        };
        if added {
            self.changed();
        }
        true
    }

    pub fn remove(&self, sub: &Arc<Sub>) {
        let mut inner = self.inner.lock().unwrap();
        inner.remove_sub(sub);
        self.changed();
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
        if removed != 0 {
            self.changed();
        }
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

    /// Fan a message out. Returns how many subscriptions it reached and which
    /// subscribers the publisher should wait for.
    ///
    /// `interest` and `stall` belong to the publishing task and are reused across
    /// messages, which is the point of the whole function: in steady state a
    /// publish to a subject this task resolved a moment ago costs one atomic load,
    /// one byte compare, and the deliveries themselves — no lock, no hash, no
    /// wildcard scan, no allocation.
    pub fn route<'a>(
        &self,
        msg: &Msg<'_>,
        from: &Arc<Conn>,
        interest: &mut Interest,
        stall: &'a mut Vec<Arc<Conn>>,
    ) -> Delivered<'a> {
        stall.clear();
        let generation = self.generation.load(Ordering::Acquire);
        if interest.generation != generation || interest.subject != msg.subject {
            self.resolve(msg.subject, interest);
        }
        // A queue group must be *sampled* per message, and the sampling goes to a
        // second list: narrowing the cache in place would leave one member of the
        // group in it, and every later message — resolving nothing, because the
        // generation has not moved — would go to that same member.
        let list: &[Arc<Sub>] = if interest.queue {
            self.pick_groups(&interest.subs, &mut interest.chosen, &mut interest.groups, from);
            &interest.chosen
        } else {
            &interest.subs
        };
        if list.is_empty() {
            // No interest, short circuit if so — the reference's own comment for
            // this branch (`go:client.go:4506`), and the reason a publish nobody
            // watches costs one hash and no copies.
            return Delivered { count: 0, stall };
        }

        let mut count = 0usize;
        let mut expired: Vec<Arc<Sub>> = Vec::new();
        for sub in list {
            if !sub.eligible(from) {
                continue;
            }
            count += match self.give(sub, msg, &mut expired) {
                Give::Ok => 1,
                // The victim is dropped; the publisher is never punished, and the
                // message that triggered it is not counted as delivered.
                Give::Stall(conn) => {
                    stall.push(conn);
                    1
                }
                Give::Gone => 0,
            };
        }
        if !expired.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for sub in &expired {
                inner.remove_sub(sub);
            }
            self.changed();
        }
        Delivered { count, stall }
    }

    /// Resolve `subject` to the subscriptions interested in it, and record the
    /// answer in `interest` for the next message to the same subject.
    ///
    /// The generation is read *inside* the lock: every change to the set is made
    /// under the same lock, so the value recorded here cannot go stale before we
    /// have looked at the state it describes. A change made after we release the
    /// lock bumps it, and the next publish to this subject re-resolves.
    fn resolve(&self, subject: &[u8], interest: &mut Interest) {
        let inner = self.inner.lock().unwrap();
        let mut queue = false;
        if interest.subs.capacity() < 16 {
            tag(Site::Routing);
        }
        interest.subs.clear();
        if let Some(list) = inner.literal.get(subject) {
            for sub in list {
                queue |= sub.queued;
                interest.subs.push(Arc::clone(sub));
            }
        }
        for sub in inner.wildcard.iter() {
            if subjects::matches(&sub.subject, subject) {
                queue |= sub.queued;
                interest.subs.push(Arc::clone(sub));
            }
        }
        let seen = self.generation.load(Ordering::Relaxed);
        drop(inner);
        interest.subject.clear();
        interest.subject.extend_from_slice(subject);
        interest.queue = queue;
        interest.generation = seen;
    }

    /// Narrow `subs` — every subscription that matched `subject` — to one per
    /// queue group, keeping the plain ones. The reference picks a random start
    /// index per message so that a group stays even without a shared counter to
    /// serialise (`go:client.go:processMsgResults`).
    ///
    /// In place, and with one small scratch `Vec` per group: this path only runs
    /// for subjects that have a queue group on them, and a queue group only
    /// exists because somebody asked for load balancing, which costs more than a
    /// `Vec` per message.
    fn pick_groups(
        &self,
        subs: &[Arc<Sub>],
        out: &mut Vec<Arc<Sub>>,
        groups: &mut Vec<Bytes>,
        from: &Arc<Conn>,
    ) {
        let start = self.rng.lock().unwrap().next_u64() as usize;
        out.clear();
        groups.clear();
        for i in 0..subs.len() {
            let Some(group) = subs[i].queue.as_ref() else {
                out.push(Arc::clone(&subs[i]));
                continue;
            };
            // One member per group per message, so a group is sampled once: the
            // indices after the first of its members are the same group, and
            // picking for each of them would deliver the message again.
            if groups.iter().any(|seen| &seen[..] == &group[..]) {
                continue;
            }
            groups.push(Bytes::copy_from_slice(group));
            let mut members: Vec<usize> = Vec::with_capacity(4);
            for (k, sub) in subs.iter().enumerate().skip(i) {
                if sub.queue.as_deref() == Some(&group[..]) {
                    members.push(k);
                }
            }
            let n = members.len();
            // Probe forward from the random start for a member that can take it
            // now; if every member is over its stall threshold, hand it to the
            // first eligible one anyway and let the publisher wait. Dropping the
            // message here would also make a busy group look unanswered and leak
            // a spurious 503.
            let pick = (0..n)
                .map(|k| members[(start + k) % n])
                .find(|&k| subs[k].eligible(from) && subs[k].conn.has_room())
                .or_else(|| {
                    (0..n)
                        .map(|k| members[(start + k) % n])
                        .find(|&k| subs[k].eligible(from))
                });
            match pick {
                Some(pick) => out.push(Arc::clone(&subs[pick])),
                // Nobody in the group can have it: not eligible, not alive. The
                // message reaches no member, as it does for a subject with no
                // subscriptions at all.
                None => {}
            }
        }
    }

    /// Queue one message on one subscription and book the delivery.
    fn give(&self, sub: &Arc<Sub>, msg: &Msg<'_>, expired: &mut Vec<Arc<Sub>>) -> Give {
        match sub.conn.deliver(sub, msg) {
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
                sub.conn.close_slow_consumer();
                expired.push(Arc::clone(sub));
                Give::Gone
            }
            Enqueue::Dropped => Give::Gone,
        }
    }

    /// One change to the subscription set: every interest cache is now stale.
    fn changed(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
}

enum Give {
    Ok,
    Stall(Arc<Conn>),
    Gone,
}

/// What one subject resolved to, kept on the publishing task between publishes.
///
/// Owned by the task, so there is no lock, no `Arc` and no synchronisation in it.
/// That is also what makes the invalidation rule safe: a `SUB` that arrives in the
/// same read as this `PUB` — the self-delivery case, and the one a per-*read* hint
/// gets wrong — bumped the generation before this operation was decided, because
/// operations are decided one after another, in order, on this task.
pub struct Interest {
    /// The generation `subs` was resolved against; `MAX` means "never", and the
    /// registry cannot reach it (`changed` counts subscription operations, and
    /// 1.8e19 of them is 58 000 years at ten million a second).
    generation: u64,
    /// The subject `subs` answers. Compared as bytes, because subjects arrive as
    /// views of different buffers and a pointer compare would miss.
    subject: Vec<u8>,
    /// The subscriptions interested in `subject`.
    subs: Vec<Arc<Sub>>,
    /// `subs` contains a queue group, so the *membership* is reusable (the
    /// generation says whether it is) but the choice among a group's members is
    /// not: that is sampled per message into `chosen`.
    queue: bool,
    /// The queue path's output, kept separate from `subs` so sampling never
    /// destroys what was resolved.
    chosen: Vec<Arc<Sub>>,
    /// The queue names already sampled this message, so a group is picked once.
    groups: Vec<Bytes>,
}

impl Interest {
    pub fn new() -> Interest {
        Interest {
            generation: u64::MAX,
            subject: Vec::new(),
            subs: Vec::new(),
            chosen: Vec::new(),
            groups: Vec::new(),
            queue: false,
        }
    }

    /// The resolved set, for tests and for the queue-group walk.
    pub fn subs(&self) -> &[Arc<Sub>] {
        &self.subs
    }
}

impl Default for Interest {
    fn default() -> Self {
        Interest::new()
    }
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

/// `MSG <subject> <sid> [reply] <#bytes>\r\n<payload>\r\n`, or the HMSG form — as
/// one contiguous buffer taken from `arena`.
///
/// A subscriber that did not declare header support gets the stripped form — the
/// reference removes the block rather than the message
/// (`client.rs::TestClientHeaderDeliverStrippedMsg`, headers.rs).
///
/// This is the copy a delivery is allowed to take: the payload moves out of the
/// publisher's read buffer into the subscriber's own bytes, once, and the head and
/// the terminator are written around it instead of being allocated beside it.
pub(crate) fn build_frame(arena: &mut Arena, sub: &Sub, msg: &Msg<'_>) -> Frame {
    let headers = msg.hdr > 0 && sub.conn.supports_headers();
    let body: &[u8] = if msg.hdr > 0 && !headers {
        &msg.body[msg.hdr..]
    } else {
        msg.body
    };
    let reply = msg.reply.unwrap_or(&[]);
    // "MSG " / "HMSG ", the subject, the sid, maybe a reply, one or two sizes, a
    // CRLF and the terminator. 48 covers the separators and the digits with room
    // to spare, which is cheaper than counting them.
    let need = 48 + msg.subject.len() + sub.sid.len() + reply.len() + body.len() + 2 * CRLF.len();
    {
        let buf = arena.tail(need);
        buf.put_slice(if headers { b"HMSG " } else { b"MSG " });
        buf.put_slice(msg.subject);
        buf.put_u8(b' ');
        buf.put_slice(&sub.sid);
        if !reply.is_empty() {
            buf.put_u8(b' ');
            buf.put_slice(reply);
        }
        buf.put_u8(b' ');
        if headers {
            // HMSG subject sid [reply] #hdr #total
            push_u64(buf, msg.hdr as u64);
            buf.put_u8(b' ');
        }
        push_u64(buf, body.len() as u64);
        buf.put_slice(CRLF);
        buf.put_slice(body);
        buf.put_slice(CRLF);
    }
    Frame(arena.seal())
}

/// The 503 status frame for an unanswered request (contract §6). Byte-exact:
/// the frame's subject is the *inbox*, the dead subject travels in a header.
///
/// Off the throughput path — one per unanswered request — so it builds its own
/// buffer instead of asking the arena for a chunk.
pub fn no_responder_frame(reply: &[u8], sub: &Sub, published: &[u8]) -> Frame {
    let status_len = 32 + published.len();
    let mut buf = Vec::with_capacity(48 + reply.len() + sub.sid.len() + 2 * status_len);
    buf.extend_from_slice(b"HMSG ");
    buf.extend_from_slice(reply);
    buf.push(b' ');
    buf.extend_from_slice(&sub.sid);
    buf.extend_from_slice(format!(" {status_len} {status_len}\r\n").as_bytes());
    buf.extend_from_slice(b"NATS/1.0 503\r\nNats-Subject: ");
    buf.extend_from_slice(published);
    buf.extend_from_slice(b"\r\n\r\n");
    debug_assert_eq!(buf.len(), 48 + reply.len() + sub.sid.len() - 8 + status_len);
    buf.extend_from_slice(CRLF);
    Frame(Bytes::from(buf))
}

/// Decimal digits, straight into the buffer: the `String` this used to build was
/// one allocation per number per delivery, and an `HMSG` line has two.
fn push_u64(dst: &mut impl BufMut, mut n: u64) {
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
    dst.put_slice(&buf[i..]);
}
