//! Subject grammar: tokenizing, validation and wildcard matching.
//!
//! The rules are the reference's (`go:sublist.go:1187-1240`), restated in
//! `specs/protocol-contract.md` §4. Nothing here allocates: every function works
//! on byte slices and walks them token by token, because a publish pays this
//! cost once per message.

/// `.` — the token separator.
const SEP: u8 = b'.';
/// `*` — exactly one token.
const PWC: u8 = b'*';
/// `>` — one or more trailing tokens.
const FWC: u8 = b'>';
/// Characters that may not appear inside a multi-character token. The reference
/// rejects `\t\n\f\r `; in the protocol they can only arrive inside a token via
/// nothing at all (they are argument separators), but a subject can also come
/// from a config file or a mapping, so the rule is kept.
const ILLEGAL_IN_TOKEN: &[u8] = b"\t\n\x0c\r ";

/// Is `s` a legal subject or subscription filter?
pub fn is_valid(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut after_fw = false;
    for tok in s.split(|b| *b == SEP) {
        if tok.is_empty() || after_fw {
            return false;
        }
        if tok.len() > 1 {
            if tok.iter().any(|b| ILLEGAL_IN_TOKEN.contains(b)) {
                return false;
            }
            continue;
        }
        if tok[0] == FWC {
            after_fw = true;
        }
    }
    true
}

/// Does `s` contain a wildcard token (`*` or `>`), i.e. is it not a literal?
pub fn has_wildcard(s: &[u8]) -> bool {
    s.split(|b| *b == SEP)
        .any(|tok| tok.len() == 1 && (tok[0] == PWC || tok[0] == FWC))
}

/// A publish subject must be legal *and* literal: wildcards are for subscribing.
pub fn is_valid_publish(s: &[u8]) -> bool {
    is_valid(s) && !has_wildcard(s)
}

/// Does the subscription filter `filter` match the published `subject`?
///
/// Both sides are validated already, so `>` can only be the final token of the
/// filter and `*` only a whole token.
pub fn matches(filter: &[u8], subject: &[u8]) -> bool {
    let mut f = filter.split(|b| *b == SEP);
    let mut s = subject.split(|b| *b == SEP);
    loop {
        match (f.next(), s.next()) {
            (Some(ftok), Some(stok)) => {
                if ftok == [FWC] {
                    // `>` swallows the rest, but the rest must exist: it matched
                    // a token here, so it does.
                    return true;
                }
                if ftok != stok && ftok != [PWC] {
                    return false;
                }
            }
            (Some(_), None) => {
                // The subject ran out while the filter still has tokens. Even a
                // trailing `>` needs at least one token of its own: `foo.>` does
                // not match `foo`.
                return false;
            }
            (None, Some(_)) => return false,
            (None, None) => return true,
        }
    }
}

/// The token count of a subject; used by the routing tests and nothing else.
pub fn tokens(s: &[u8]) -> usize {
    if s.is_empty() {
        return 0;
    }
    s.iter().filter(|b| **b == SEP).count() + 1
}
