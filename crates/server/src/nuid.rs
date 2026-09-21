//! Identifiers.
//!
//! The reference's `server_id` is not a NUID: `server.go:716` uses the server's
//! nkey public key, which is 56 characters of base32 (`A-Z2-7`) starting with the
//! server prefix `N`. A core-only server with no authorization has no key pair,
//! and nothing on either side of the wire parses the value — it is an opaque
//! name. We therefore emit a token of the same *shape* rather than a key we
//! cannot sign with; the divergence is recorded in
//! `specs/protocol-contract.md` §2.

/// The base32 alphabet used by nkey-encoded identifiers.
const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const PREFIX: u8 = b'N';
const LEN: usize = 56;

/// A fresh identifier. `state` is a process-wide generator so the values stay
/// unique within one run without a lock per call.
pub fn server_id(rng: &mut Rng) -> String {
    let mut out = Vec::with_capacity(LEN);
    out.push(PREFIX);
    for _ in 1..LEN {
        let i = (rng.next_u64() % ALPHABET.len() as u64) as usize;
        out.push(ALPHABET[i]);
    }
    // SAFETY: every byte above is ASCII.
    String::from_utf8(out).expect("ascii")
}

/// splitmix64 over a seed taken from the environment: enough entropy to make
/// collisions between two processes on this host improbable, and no dependency.
pub struct Rng {
    state: u64,
}

impl Default for Rng {
    fn default() -> Self {
        Rng::new()
    }
}

impl Rng {
    pub fn new() -> Rng {
        Rng {
            state: seed()
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .rotate_left(17),
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// `0 .. .bound`
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        self.next_u64() % bound
    }
}

fn seed() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    h.write_u64(std::process::id() as u64);
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x5DEE_CE66),
    );
    // A second RandomState draws different randomness of its own.
    h.write_u64(RandomState::new().build_hasher().finish());
    h.finish()
}
