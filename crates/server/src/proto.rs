//! The control-line parser: a state machine over the byte stream, and the one
//! place that decides which class a failure belongs to.
//!
//! Classes, from `specs/protocol-contract.md` §4:
//!
//! | Kind | Wire | Raised by |
//! |---|---|---|
//! | [`ProtoError::Fatal`] | `-ERR '<text>'` then close | unknown verb, empty line, oversized control line, oversized body |
//! | [`ProtoError::Silent`] | close, no bytes | bad arity, unparseable size, `HPUB` without the capability |
//! | [`ProtoError::Soft`] | `-ERR '<text>'`, connection stays | invalid subscribe subject, non-literal publish subject (pedantic) |
//!
//! Anything the parser cannot decide alone — subject grammar, which needs the
//! subscription context — is classified in `client.rs`, and both tables carry a
//! reference to the same contract section.
//!
//! The parser is a state machine rather than a line splitter because a `PUB`
//! body is *part of the stream state*: it can be split at any byte, and a partial
//! frame must be held, never routed (`verbs.rs::truncated_publish_body_...`).

use bytes::{Buf, Bytes, BytesMut};

// ---------------------------------------------------------------- error text --

pub const UNKNOWN_OP: &str = "Unknown Protocol Operation";
pub const MAX_PAYLOAD: &str = "Maximum Payload Violation";
pub const MAX_CONTROL_LINE: &str = "maximum control line exceeded";
pub const INVALID_SUBJECT: &str = "Invalid Subject";
pub const INVALID_PUBLISH_SUBJECT: &str = "Invalid Publish Subject";
pub const STALE_CONNECTION: &str = "Stale Connection";
/// Not a protocol error the client sees: a close reason for a subscriber whose
/// outbound buffer ran out of room (contract §7 — the reference sends nothing).
pub const SLOW_CONSUMER: &str = "Slow Consumer";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtoError {
    /// `-ERR <text>` then close. Class A and its size-sibling A′/A″.
    Fatal(&'static str),
    /// `-ERR <text>`, connection stays usable. Class B.
    Soft(&'static str),
    /// Close with no bytes. Class C.
    Silent,
}

impl ProtoError {
    /// The bytes to put on the wire before closing, if any.
    pub fn err_line(&self) -> Option<&'static str> {
        match self {
            ProtoError::Fatal(t) | ProtoError::Soft(t) => Some(t),
            ProtoError::Silent => None,
        }
    }
    pub fn closes(&self) -> bool {
        !matches!(self, ProtoError::Soft(_))
    }
}

/// What the parser needs to know about the connection it is parsing for: the
/// numeric limits, which no client command can change.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_control_line: usize,
    pub max_payload: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_control_line: crate::config::MAX_CONTROL_LINE,
            max_payload: crate::config::MAX_PAYLOAD,
        }
    }
}

/// One completed protocol operation, in the order it appeared on the wire.
#[derive(Debug, Clone)]
pub enum Event {
    /// `CONNECT <json>` — the object is handed over unparsed; options are applied
    /// by `client.rs`, where the "absent means true" rule lives.
    Connect(Bytes),
    Subscribe {
        subject: Bytes,
        queue: Option<Bytes>,
        sid: Bytes,
    },
    /// `max` is `-1` when no second argument was given, otherwise the parsed
    /// value (`0` and anything unparseable arrive as `-1` too, which the
    /// reference treats as "unsubscribe now").
    Unsubscribe {
        sid: Bytes,
        max: i64,
    },
    Publish {
        subject: Bytes,
        reply: Option<Bytes>,
        /// Header-block length inside `body`; 0 for a plain `PUB`.
        hdr: usize,
        /// `body.len()`: the declared total, header block included.
        total: usize,
        body: Bytes,
        /// True for the `HPUB` verb. Whether the client may use it is decided
        /// when the operation is *executed*, not when the line is parsed: a
        /// CONNECT and an HPUB can arrive in one read, and the reference checks
        /// the capability in `processHeaderPub` (`go:client.go:2897`).
        hpub: bool,
    },
    Ping,
    /// A client PONG: never an error, and it does not reset the server's own
    /// keepalive counter on its own (`ping_test.go:TestUnpromptedPong`).
    Pong,
    /// `INFO` from a client: the reference parses the argument as a JSON object
    /// and ignores it for CLIENT kind — but an argument that does not parse (or
    /// is missing) is the same silent close a bad CONNECT gets. The bytes travel
    /// with the event because the check belongs where the options live.
    Info(Bytes),
    /// An `HPUB`, raised at the control line rather than at the body, because
    /// whether it is legal depends on the capability the connection holds *at
    /// that moment*: a CONNECT granting headers can arrive in the same read as
    /// the publish that needs it (contract §3, `specs/parity-log.md` row 5), so
    /// the parser cannot answer it and hands the decision to the client. The
    /// client that does have headers sees `Ok` as a no-op and the frame's own
    /// error as class A; the one that does not is closed without a word either
    /// way (all four shapes measured against the reference).
    HpubAttempt(HpubOutcome),
    /// `-ERR` from a client: the reference logs it and closes, silently.
    ClientErr,
    /// `+OK` from a client: ignored.
    ClientOk,
}

/// What an `HPUB` line said, as far as the grammar can tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HpubOutcome {
    /// A well-formed line: the body it declares follows as a `Publish`.
    Ok,
    /// Arguments that do not describe a publish at all.
    Args,
    /// The line was fine and the bytes that followed were not.
    Frame(&'static str),
}

/// What the parser is waiting for between calls.
enum State {
    /// At the start of a control line.
    Line,
    /// Mid-body of a `PUB`/`HPUB`.
    Body {
        subject: Bytes,
        reply: Option<Bytes>,
        hdr: usize,
        need: usize,
        hpub: bool,
        got: Vec<Bytes>,
        gathered: usize,
    },
}

pub struct Parser {
    state: State,
}

impl Default for Parser {
    fn default() -> Self {
        Parser::new()
    }
}

impl Parser {
    pub fn new() -> Parser {
        Parser { state: State::Line }
    }

    /// Consume complete operations from `src`, leaving anything incomplete.
    /// `src` is advanced as work is done, so the caller keeps one buffer alive
    /// across reads without copying.
    pub fn feed(&mut self, src: &mut BytesMut, lim: &Limits) -> Result<Vec<Event>, ProtoError> {
        let mut out = Vec::new();
        loop {
            match &mut self.state {
                State::Line => {
                    // A bare LF is not whitespace to be skipped at the start of a
                    // line: the reference's OP_START arm finds no verb in it and
                    // closes with the unknown-operation error (measured: `\n`
                    // alone, and `PING\r\n\n`, both class A).
                    if matches!(src.first(), Some(b'\n')) {
                        return Err(ProtoError::Fatal(UNKNOWN_OP));
                    }
                    match find_crlf(src) {
                        None => {
                            // Still no line end. Guard against an unbounded line.
                            if src.len() > lim.max_control_line * 16 {
                                return Err(ProtoError::Fatal(MAX_CONTROL_LINE));
                            }
                            return Ok(out);
                        }
                        Some(at) => {
                            if at > lim.max_control_line {
                                return Err(ProtoError::Fatal(MAX_CONTROL_LINE));
                            }
                            let line = src.split_to(at).freeze();
                            src.advance(2);
                                                        match parse_line(&line, lim)? {
                                Action::Event(ev) => {
                                    let stop = matches!(
                                        ev,
                                        Event::HpubAttempt(HpubOutcome::Args)
                                    );
                                    out.push(ev);
                                    // An HPUB the parser cannot describe ends the
                                    // batch where it stands: the answer belongs to
                                    // the connection's options at execution time, and
                                    // parsing the header bytes as further commands
                                    // would decide it with an error the client never
                                    // gets to overrule (contract §3, corpus cases).
                                    if stop {
                                        return Ok(out);
                                    }
                                }
                                Action::Body {
                                    subject,
                                    reply,
                                    hdr,
                                    need,
                                    hpub,
                                } => {
                                    if hpub {
                                        out.push(Event::HpubAttempt(HpubOutcome::Ok));
                                    }
                                    self.state = State::Body {
                                        subject,
                                        reply,
                                        hdr,
                                        need,
                                        hpub,
                                        got: Vec::new(),
                                        gathered: 0,
                                    };
                                }
                            }
                        }
                    }
                }
                State::Body {
                    subject,
                    reply,
                    hdr,
                    need,
                    hpub,
                    got,
                    gathered,
                } => {
                    let want = *need - *gathered;
                    let take = want.min(src.len());
                    if take > 0 {
                        got.push(src.split_to(take).freeze());
                        *gathered += take;
                    }
                    if *gathered < *need {
                        return Ok(out);
                    }
                    // The terminator is positional, and the reference decides on
                    // the first byte it can see: `size` payload bytes, then a
                    // CRLF. Anything other than '\r' in that slot is a stream
                    // desync it reports at once — it does not buffer the second
                    // byte first (measured: `PUB a 3` + "ab\r\n" is class A, while
                    // `PUB a 2` + "ab\r" waits for the rest of the terminator).
                    // An HPUB is the exception: there the capability question is
                    // asked before any of this, so the failure is deferred.
                    match src.first() {
                        None => return Ok(out),
                        Some(b'\r') if src.len() >= 2 => {
                            if src[1] != b'\n' {
                                return hpub_frame_error(*hpub, out);
                            }
                        }
                        Some(b'\r') => return Ok(out),
                        Some(_) => return hpub_frame_error(*hpub, out),
                    }
                    src.advance(2);
                    let mut body = BytesMut::with_capacity(*need);
                    for part in got.drain(..) {
                        body.extend_from_slice(&part);
                    }
                    let body = body.freeze();
                    debug_assert_eq!(body.len(), *need);
                    out.push(Event::Publish {
                        subject: std::mem::take(subject),
                        reply: reply.take(),
                        hdr: *hdr,
                        total: body.len(),
                        hpub: *hpub,
                        body,
                    });
                    self.state = State::Line;
                }
            }
        }
    }
}

enum Action {
    Event(Event),
    /// The line was a `PUB`/`HPUB`: the body follows.
    Body {
        subject: Bytes,
        reply: Option<Bytes>,
        hdr: usize,
        need: usize,
        hpub: bool,
    },
}

/// Parse one control line (without its CRLF).
fn parse_line(line: &[u8], lim: &Limits) -> Result<Action, ProtoError> {
    if line.is_empty() {
        return Err(ProtoError::Fatal(UNKNOWN_OP));
    }
    // The verb runs to the first space or tab; a name of the wrong length is
    // simply not a verb this server knows.
    let verb_end = line
        .iter()
        .position(|b| *b == b' ' || *b == b'\t')
        .unwrap_or(line.len());
    let verb = &line[..verb_end];
    let rest = &line[verb_end..];
    let has_args = !rest.is_empty();

    // PING and PONG are prefixes, not tokens: the reference's parser states for
    // them have no default arm, so whatever follows the four letters is skipped
    // to the line end (measured: `PING x` and `PINGxyz` are both answered, and
    // `PONGzz` is ignored, with the connection kept either way).
    if line.len() >= 4 && eq_ignore(&line[..4], b"PING") {
        return Ok(Action::Event(Event::Ping));
    }
    if line.len() >= 4 && eq_ignore(&line[..4], b"PONG") {
        return Ok(Action::Event(Event::Pong));
    }
    if line.len() >= 4 && eq_ignore(&line[..4], b"INFO") {
        // A prefix, like CONNECT: `INFO{"a":1}` is INFO with an argument
        // (measured), so the argument is whatever follows the four letters.
        let tail = &line[4..];
        let arg = match tail.iter().position(|b| *b != b' ' && *b != b'\t') {
            Some(i) => &tail[i..],
            None => return Err(ProtoError::Silent),
        };
        return Ok(Action::Event(Event::Info(Bytes::copy_from_slice(arg))));
    }
    if eq_ignore(verb, b"+OK") {
        return Ok(Action::Event(Event::ClientOk));
    }
    if eq_ignore(verb, b"-ERR") {
        if !has_args {
            // `-ERR` with nothing after it never matched the verb: the reference
            // needs the separator, so a bare `-ERR\r\n` is an unknown operation
            // while `-ERR something` is the silent close (both measured).
            return Err(ProtoError::Fatal(UNKNOWN_OP));
        }
        return Ok(Action::Event(Event::ClientErr));
    }
    if line.len() >= 7 && eq_ignore(&line[..7], b"CONNECT") {
        // Also a prefix: the parser's OP_CONNECT state falls through to
        // CONNECT_ARG on any byte, so `CONNECTxyz {}` hands "xyz {}" to the JSON
        // decoder — which fails, and a failed CONNECT is a silent close
        // (measured). The options object is the remainder; it is never
        // argument-split, because it may contain spaces.
        let tail = &line[7..];
        let arg = match tail.iter().position(|b| *b != b' ' && *b != b'\t') {
            Some(i) => &tail[i..],
            None => return Err(ProtoError::Silent),
        };
        return Ok(Action::Event(Event::Connect(Bytes::copy_from_slice(arg))));
    }
    if eq_ignore(verb, b"SUB") {
        if !has_args {
            return Err(ProtoError::Fatal(UNKNOWN_OP));
        }
        let args = split_args(rest);
        return match args.as_slice() {
            [subject, sid] => Ok(Action::Event(subscribe(subject, None, sid))),
            [subject, queue, sid] => Ok(Action::Event(subscribe(subject, Some(queue), sid))),
            _ => Err(ProtoError::Silent),
        };
    }
    if eq_ignore(verb, b"UNSUB") {
        if !has_args {
            return Err(ProtoError::Fatal(UNKNOWN_OP));
        }
        let args = split_args(rest);
        return match args.as_slice() {
            [sid] => Ok(Action::Event(Event::Unsubscribe {
                sid: Bytes::copy_from_slice(sid),
                max: -1,
            })),
            [sid, max] => Ok(Action::Event(Event::Unsubscribe {
                sid: Bytes::copy_from_slice(sid),
                // A missing or unparseable max arrives as -1, which means
                // "unsubscribe now" — measured, contract §3.
                max: parse_size(max),
            })),
            _ => Err(ProtoError::Silent),
        };
    }
    if eq_ignore(verb, b"PUB") || eq_ignore(verb, b"HPUB") {
        let is_h = verb.len() == 4;
        if !has_args {
            return Err(ProtoError::Fatal(UNKNOWN_OP));
        }
        let args = split_args(rest);
        //  PUB   subject size            |  subject reply size
        //  HPUB  subject #hdr #total     |  subject reply #hdr #total
        let (subject, reply, sizes): (&[u8], Option<&[u8]>, [&[u8]; 2]) = match (is_h, args.len()) {
            (false, 2) => (args[0], None, [args[1], args[1]]),
            (false, 3) => (args[0], Some(args[1]), [args[2], args[2]]),
            (true, 3) => (args[0], None, [args[1], args[2]]),
            (true, 4) => (args[0], Some(args[1]), [args[2], args[3]]),
            _ if is_h => {
                return Ok(Action::Event(Event::HpubAttempt(HpubOutcome::Args)));
            }
            _ => return Err(ProtoError::Silent),
        };
        let hdr = if is_h { parse_size(sizes[0]) } else { 0 };
        let total = parse_size(sizes[if is_h { 1 } else { 0 }]);
        if hdr < 0 || total < 0 || hdr > total {
            if is_h {
                return Ok(Action::Event(Event::HpubAttempt(HpubOutcome::Args)));
            }
            return Err(ProtoError::Silent);
        }
        if total as u64 > lim.max_payload {
            // Class A-prime: the *declared* size is enough to reject the frame,
            // which is what keeps an oversized publish cheap.
            return Err(ProtoError::Fatal(MAX_PAYLOAD));
        }
        return Ok(Action::Body {
            subject: Bytes::copy_from_slice(subject),
            reply: reply.map(Bytes::copy_from_slice),
            hdr: hdr as usize,
            need: total as usize,
            hpub: is_h,
        });
    }
    Err(ProtoError::Fatal(UNKNOWN_OP))
}

/// A body that did not end where the frame said it would. For a plain `PUB`
/// that is the stream desync the reference reports at once; for an `HPUB` the
/// capability is decided first, so the outcome goes to the client instead.
fn hpub_frame_error(
    hpub: bool,
    mut out: Vec<Event>,
) -> Result<Vec<Event>, ProtoError> {
    if hpub {
        out.push(Event::HpubAttempt(HpubOutcome::Frame(UNKNOWN_OP)));
        return Ok(out);
    }
    Err(ProtoError::Fatal(UNKNOWN_OP))
}

fn subscribe(subject: &[u8], queue: Option<&[u8]>, sid: &[u8]) -> Event {
    Event::Subscribe {
        subject: Bytes::copy_from_slice(subject),
        queue: queue.map(Bytes::copy_from_slice),
        sid: Bytes::copy_from_slice(sid),
    }
}

/// The reference's `parseSize` (`go:util.go:83`): 1 to 9 ASCII digits, or -1.
/// Anything longer — including a number that would overflow an int64 — is -1,
/// and the callers turn a negative size into a silent disconnect. That is what
/// `PUB foo 18446744073709551615123` does (measured).
pub fn parse_size(d: &[u8]) -> i64 {
    if d.is_empty() || d.len() > 9 {
        return -1;
    }
    let mut n: i64 = 0;
    for b in d {
        if !b.is_ascii_digit() {
            return -1;
        }
        n = n * 10 + (b - b'0') as i64;
    }
    n
}

fn find_crlf(buf: &BytesMut) -> Option<usize> {
    if buf.len() < 2 {
        return None;
    }
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Split an argument string on spaces and tabs, discarding runs of them: the
/// reference's `splitArg`, so `SUB   foo\t1 ` and `SUB foo 1` are the same line.
fn split_args(s: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::with_capacity(5);
    let mut start: Option<usize> = None;
    for (i, b) in s.iter().enumerate() {
        if *b == b' ' || *b == b'\t' {
            if let Some(st) = start.take() {
                out.push(&s[st..i]);
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(st) = start {
        out.push(&s[st..]);
    }
    out
}

/// Case-insensitive byte comparison. The reference folds case in every parser
/// state, so `SuB` and `sUB` are `SUB` (`verbs_are_case_insensitive`).
fn eq_ignore(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.eq_ignore_ascii_case(y))
}
