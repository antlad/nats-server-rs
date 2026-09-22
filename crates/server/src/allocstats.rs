//! Counting allocator: what the server allocates per message, when `perf` can't
//! be run.
//!
//! Compiled in only with `--features allocstats`, which nothing in the test or
//! bench protocol uses. Turn it on to answer a specific question — how many
//! allocations and how many bytes does one delivered message cost — and read the
//! lines written at start, every `NATS_RS_STATS_EVERY_MS` (100 ms by default) and
//! at shutdown.
//!
//! The counters cost two relaxed add per allocation, so the throughput of a
//! `--features allocstats` binary is *not* comparable with a normal run. Only the
//! counts are.
//!
//! ## Per site
//!
//! [`tag`] is a second instrument for the same question: a named counter at each
//! place the server deliberately takes memory. It is a no-op unless the feature is
//! on — the call sites stay in the shipping binary because the *function* does not
//! exist there — and the dump prints one line per site. That is what turns
//! "0.063 allocations per message" into "one 64 KiB read buffer per read, and
//! whatever the rest are".

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
pub static BYTES: AtomicU64 = AtomicU64::new(0);
pub static LIVE: AtomicU64 = AtomicU64::new(0);

/// Every place the server takes memory on purpose, in the order the dump prints
/// them. Adding a site means adding a variant: the `COUNT` below keeps the array
/// honest, and `sum_matches_total` asserts that no allocation was missed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Site {
    /// The reader's reusable buffer, when it could not be reclaimed in place.
    ReadBuf = 0,
    /// The batch of parsed operations, when it grew.
    Events = 1,
    /// A frame's bytes taken from a connection's arena (one per chunk, not per
    /// frame).
    FrameChunk = 2,
    /// A delivery's payload copy.
    Payload = 3,
    /// A `SUB`'s subject/queue/sid copies.
    Subscribe = 4,
    /// A control line the server says: `-ERR`, `INFO`, and the sizes that go with
    /// them.
    ControlLine = 5,
    /// A per-connection record and everything hanging off it.
    Connection = 6,
    /// The matched-subscription set, and the scratch that carries it.
    Routing = 7,
    /// Anything else the server deliberately allocates.
    Other = 8,
}

impl Site {
    /// How many sites there are: the size of the counter array.
    pub const COUNT: usize = 9;
}

pub const NAMES: [&str; Site::COUNT] = [
    "read_buf",
    "events",
    "frame_chunk",
    "payload",
    "subscribe",
    "control_line",
    "connection",
    "routing",
    "other",
];

#[cfg(feature = "allocstats")]
static COUNTERS: [AtomicU64; Site::COUNT] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Note that the server asked for memory at a named site. Zero cost unless the
/// `allocstats` feature is built in, which is why the sites are worth tagging.
#[inline(always)]
pub fn tag(site: Site) {
    #[cfg(feature = "allocstats")]
    COUNTERS[site as usize].fetch_add(1, Ordering::Relaxed);
    #[cfg(not(feature = "allocstats"))]
    let _ = site;
}

/// The tags, one line: `alloctags name=n count=n …`, in publish order so a diff
/// between two runs reads like a table.
pub fn tag_line() -> String {
    let mut out = String::from("alloctags");
    for (i, name) in NAMES.iter().enumerate() {
        let n = tag_count(i);
        out.push_str(&format!(" {name}={n}"));
    }
    out
}

fn tag_count(i: usize) -> u64 {
    #[cfg(feature = "allocstats")]
    {
        COUNTERS[i].load(Ordering::Relaxed)
    }
    #[cfg(not(feature = "allocstats"))]
    {
        let _ = i;
        0
    }
}

/// How many allocations of each size, in power-of-two buckets: `bucket n` means
/// `n` allocations of size in `[2^(n-1), 2^n)`.
///
/// This is the instrument that answers "what is being allocated" when a site was
/// never tagged, and it needs no call sites at all — the sizes are unmistakable. A
/// 64 KiB bucket filling up on the publish path can only be the reader's buffer
/// failing to reclaim in place (`PLAN3.md` Task 23.3), which is exactly the
/// question this was written to ask.
#[cfg(feature = "allocstats")]
static HIST: [AtomicU64; HIST_BUCKETS] = [const { AtomicU64::new(0) }; HIST_BUCKETS];
#[cfg(feature = "allocstats")]
const HIST_BUCKETS: usize = 28;

/// One line of the histogram, skipping the buckets nobody used.
#[cfg(feature = "allocstats")]
fn hist_line() -> String {
    let mut out = String::from("allochist");
    let mut nonzero = 0usize;
    for (i, slot) in HIST.iter().enumerate() {
        let n = slot.load(Ordering::Relaxed);
        if n != 0 {
            out.push_str(&format!(" 2^{i}={n}"));
            nonzero += 1;
        }
    }
    if nonzero == 0 {
        out.push_str(" (empty)");
    }
    out
}

#[cfg(feature = "allocstats")]
#[inline(always)]
fn hist(size: usize) {
    let bucket = (usize::BITS - size.leading_zeros()).min(HIST_BUCKETS as u32 - 1) as usize;
    HIST[bucket].fetch_add(1, Ordering::Relaxed);
}

/// With the feature off, the `Counting` allocator is never installed, but the
/// impl still has to compile — so the buckets it would fill are a no-op.
#[cfg(not(feature = "allocstats"))]
#[inline(always)]
fn hist(_size: usize) {}

pub struct Counting;

macro_rules! count {
    ($size:expr) => {{
        hist($size);
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
        // `count!` already moves LIVE by `new_size`, so adding it again here is
        // how `live` once reported 545 MB on a run whose RSS never left 2.7 MB:
        // the block being replaced was subtracted, its replacement counted twice.
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        LIVE.fetch_add(new_size as u64, Ordering::Relaxed);
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count!(layout.size());
        System.alloc_zeroed(layout)
    }
}

/// Nanoseconds since the process started, for the `t=` field: a dumper that
/// samples every 100 ms is only useful if the samples can be lined up with the
/// bench that drove them.
#[cfg(feature = "allocstats")]
fn elapsed_s() -> f64 {
    use std::sync::OnceLock;
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START.get_or_init(std::time::Instant::now).elapsed().as_secs_f64()
}

/// Snapshot the counters as two lines — totals and per-site — to stderr and to
/// `NATS_RS_STATS_FILE` if it is set.
///
/// The bench harness sends the server's stderr to the void, so the file is how a
/// run driven by a bench gets its numbers out. With the feature off this is
/// nothing at all, which is why the call sites need no `cfg`.
pub fn dump(stage: &str) {
    #[cfg(feature = "allocstats")]
    {
        let line = format!(
            "allocstats stage={stage} allocs={} bytes={} live={} t={:.3}\n{}\n",
            ALLOCS.load(Ordering::Relaxed),
            BYTES.load(Ordering::Relaxed),
            LIVE.load(Ordering::Relaxed),
            elapsed_s(),
            tag_line(),
        );
        let line = format!("{line}{}\n", hist_line());
        use std::io::Write;
        eprint!("{line}");
        if let Ok(path) = std::env::var("NATS_RS_STATS_FILE") {
            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
                let _ = f.write_all(line.as_bytes());
                let _ = f.flush();
            }
        }
    }
    #[cfg(not(feature = "allocstats"))]
    {
        let _ = stage;
    }
}

/// Dump every `NATS_RS_STATS_EVERY_MS` (100 ms) as well as at the ends, so a run
/// of accepts and closes — or of one bench — can be isolated from the whole life
/// of the process. Task 22.1 of `PLAN3.md`: without this, "allocations per
/// connection" is not measurable at all, and every census number was a difference
/// between a start line and an exit line.
pub fn spawn_dumper() {
    #[cfg(feature = "allocstats")]
    {
        if std::env::var("NATS_RS_STATS_FILE").is_err() {
            return;
        }
        let every = std::env::var("NATS_RS_STATS_EVERY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(100);
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(every));
            dump("t");
        });
    }
    #[cfg(not(feature = "allocstats"))]
    {}
}

/// Assert that the tags add up to what the allocator counted, so a site that was
/// never tagged shows up as a gap rather than as a quiet wrong answer.
pub fn untagged() -> u64 {
    #[cfg(feature = "allocstats")]
    {
        let sum: u64 = (0..Site::COUNT).map(tag_count).sum();
        (ALLOCS.load(Ordering::Relaxed) as i64 - sum as i64).unsigned_abs()
    }
    #[cfg(not(feature = "allocstats"))]
    {
        0
    }
}
