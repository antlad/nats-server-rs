//! Counting allocator: what the server allocates per message, when `perf` can't
//! be run.
//!
//! Compiled in only with `--features allocstats`, which nothing in the test or
//! bench protocol uses. Turn it on to answer a specific question — how many
//! allocations and how many bytes does one delivered message cost — and read
//! the two lines written at start and at shutdown.
//!
//! The counters cost two relaxed atomic add per allocation, so the throughput of
//! a `--features allocstats` binary is *not* comparable with a normal run. Only
//! the counts are.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
pub static BYTES: AtomicU64 = AtomicU64::new(0);
pub static LIVE: AtomicU64 = AtomicU64::new(0);

pub struct Counting;

macro_rules! count {
    ($size:expr) => {{
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add($size as u64, Ordering::Relaxed);
        LIVE.fetch_add($size as u64, Ordering::Relaxed);
    }};
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count!(layout.size());
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count!(new_size);
        LIVE.fetch_add(new_size as u64, Ordering::Relaxed);
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count!(layout.size());
        System.alloc_zeroed(layout)
    }
}

/// Snapshot the counters as one line: stderr, and `NATS_RS_STATS_FILE` if set.
///
/// The bench harness sends the server's stderr to the void, so the file is how a
/// run driven by `./target/release/pubsub` gets its numbers out.
pub fn dump(stage: &str) {
    let line = format!(
        "allocstats stage={stage} allocs={} bytes={} live={}\n",
        ALLOCS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
        LIVE.load(Ordering::Relaxed),
    );
    eprint!("{line}");
    if let Ok(path) = std::env::var("NATS_RS_STATS_FILE") {
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = f.write_all(line.as_bytes());
            let _ = f.flush();
        }
    }
}
