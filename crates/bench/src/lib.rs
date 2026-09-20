//! Shared helpers for the bench binaries.
//!
//! Output format is deliberately stable and grep-able: one `bench name=... ...`
//! line per run, so Go-vs-Rust results can be diffed mechanically.

use std::time::Duration;

/// Read a u64 parameter from env with a default.
pub fn param(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .map(|v| {
            v.parse()
                .unwrap_or_else(|_| panic!("{name} is not a number: {v}"))
        })
        .unwrap_or(default)
}

/// Print one stable, grep-able metric line per throughput bench.
pub fn report(name: &str, msgs: u64, size: u64, dur: Duration) {
    let secs = dur.as_secs_f64();
    println!(
        "bench name={name} msgs={msgs} size={size} duration_ms={:.0} msgs_per_sec={:.0} mb_per_sec={:.2}",
        secs * 1000.0,
        msgs as f64 / secs,
        (msgs * size) as f64 / secs / 1_048_576.0,
    );
}

/// Print latency percentiles from nanosecond samples.
pub fn report_latency(name: &str, mut samples_ns: Vec<u64>) {
    samples_ns.sort_unstable();
    assert!(!samples_ns.is_empty(), "no samples to report");
    let pct = |p: f64| {
        let idx = ((samples_ns.len() - 1) as f64 * p).round() as usize;
        samples_ns[idx]
    };
    println!(
        "bench name={name} samples={} p50_us={:.0} p99_us={:.0} p999_us={:.0} max_us={:.0}",
        samples_ns.len(),
        pct(0.50) as f64 / 1000.0,
        pct(0.99) as f64 / 1000.0,
        pct(0.999) as f64 / 1000.0,
        *samples_ns.last().unwrap() as f64 / 1000.0,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_pick_nearest_rank() {
        // Should not panic on tiny sample sets and must stay monotonic.
        let samples = vec![1_000u64, 2_000, 3_000, 4_000];
        report_latency("unit", samples);
    }

    #[test]
    fn param_falls_back_to_default() {
        assert_eq!(param("BENCH_UNSET_VAR_FOR_TEST", 42), 42);
    }
}
