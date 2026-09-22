//! Where a delivered frame's bytes live: chunked bump allocation, not `malloc`.
//!
//! A delivery needs one contiguous `MSG <subject> <sid> [reply] <len>\r\n<payload>\r\n`.
//! Building it out of three separate buffers costs two allocations (the head
//! `Vec`, the payload copy) and forces the writer to describe three iovecs to the
//! kernel per message. Laid down contiguously in a chunk it costs one memcpy, and
//! a chunk of `CHUNK` bytes is one allocation shared by every frame that fits in
//! it — at 256 B payloads that is ~29 messages per allocation, and the chunk dies
//! by refcount when the last of those frames has been written.
//!
//! The chunk is per connection, not per publisher, and it is handed out under that
//! connection's queue lock. Both halves of that matter:
//!
//! * **Order.** Frames must be laid down in the order the writer will drain them,
//!   because "the slot is free once the writer passed it" is the same promise as
//!   "released in publish order". The queue lock is where that order is decided,
//!   so the arena is allocated inside it.
//! * **Memory.** A chunk shared between two subscribers would let a wedged one pin
//!   the bytes of a healthy one — `memprobe.py` exists to catch exactly that, and
//!   a per-publisher arena would fail it at any fan-out wider than one. A chunk
//!   belonging to one connection can only ever be pinned by that connection's own
//!   pending bytes, so the ceiling stays `max_pending` plus one partial chunk.
//!
//! Safe Rust all the way down: `BytesMut` already is a bump allocator whose free
//! list is an atomic refcount, and `split`/`freeze` are the bump and the release.

use bytes::{Bytes, BytesMut};

use crate::allocstats::{tag, Site};

/// Bytes per chunk. Small enough that a connection that goes quiet after one
/// frame wastes very little, big enough that a 256 B delivery costs ~1/29 of an
/// allocation. Measured against 16 KiB and 4 KiB (`specs/perf-notes.md`).
pub const CHUNK: usize = 8 * 1024;

/// A source of contiguous frame bytes.
pub struct Arena {
    /// Room still to be handed out from the current chunk.
    chunk: BytesMut,
    /// What a new chunk costs.
    size: usize,
}

impl Arena {
    pub fn new() -> Arena {
        Arena {
            chunk: BytesMut::new(),
            size: CHUNK,
        }
    }

    /// Room for `need` bytes, in the current chunk if it has that much left.
    /// Everything written through the returned buffer is handed over by the next
    /// [`seal`](Self::seal), so a caller that writes less than `need` is fine —
    /// `need` is an upper bound, not a promise.
    ///
    /// The test is `capacity() - len()`, **not** `remaining_mut()`: `BufMut`
    /// answers `remaining_mut` for a `BytesMut` with `usize::MAX - len`, because it
    /// *can* always grow — so that version of the test is never true, the chunk is
    /// never replaced, and every `put_slice` grows the same `Vec` by exactly its
    /// own shortfall. That is one allocation per frame instead of one per 25, and
    /// it is the whole reason the delivery path counted three allocations a
    /// message where this file promised zero (`specs/perf-notes.md`).
    pub fn tail(&mut self, need: usize) -> &mut BytesMut {
        if self.chunk.capacity() - self.chunk.len() < need {
            // An allocation, always: either a fresh chunk or a dedicated buffer.
            tag(Site::FrameChunk);
            // The old chunk survives whatever still holds a view of it; this
            // cursor moves to a fresh one. A frame larger than a chunk gets a
            // buffer of its own, which is the payload copy the memory rule
            // already asks for.
            self.chunk = BytesMut::with_capacity(need.max(self.size));
            // BytesMut hands out a `Vec` the first time a piece is split off it,
            // so one chunk can cost two allocations; the histogram says which.
            tag(Site::Other);
        }
        &mut self.chunk
    }

    /// Hand over everything written since the last `seal`.
    pub fn seal(&mut self) -> Bytes {
        self.chunk.split().freeze()
    }
}

impl Default for Arena {
    fn default() -> Self {
        Arena::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn frames_share_a_chunk_until_it_is_full() {
        let mut a = Arena::new();
        a.tail(8).put_slice(b"aaaa");
        let first = a.seal();
        a.tail(8).put_slice(b"bbbb");
        let second = a.seal();
        assert_eq!(&first[..], b"aaaa");
        assert_eq!(&second[..], b"bbbb");
        // Same allocation: the two views differ only by their offset into it.
        assert_eq!(
            first.as_ptr() as usize - second.as_ptr() as usize,
            -4isize as usize - 0,
            "consecutive frames should come from one chunk"
        );
    }

    #[test]
    fn an_oversized_frame_takes_its_own_buffer() {
        let mut a = Arena::new();
        let big = vec![b'x'; CHUNK * 3];
        a.tail(big.len()).put_slice(&big);
        let frame = a.seal();
        assert_eq!(frame.len(), big.len());
        // And the cursor moved on: the next frame is a fresh allocation.
        a.tail(4).put_slice(b"yyyy");
        let next = a.seal();
        assert_eq!(&next[..], b"yyyy");
        assert_ne!(
            frame.as_ptr() as usize - frame.len(),
            next.as_ptr() as usize - 4,
            "a dedicated buffer must not be reused as a chunk"
        );
    }

    #[test]
    fn a_view_keeps_the_chunk_alive() {
        let mut a = Arena::new();
        a.tail(4).put_slice(b"abcd");
        let view = a.seal();
        // Hand the rest of the chunk out, then drop our handle on it: the view
        // must still read back what was written into it.
        a.tail(CHUNK).put_slice(b"zz");
        a.seal();
        assert_eq!(&view[..], b"abcd");
    }
}
