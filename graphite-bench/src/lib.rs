//! Benchmark harness utilities and comparison scaffolding.

use std::time::{Duration, Instant};

/// Result of a timed benchmark run.
#[derive(Debug, Clone)]
pub struct BenchResult {
    pub name: String,
    pub iterations: u64,
    pub elapsed: Duration,
    pub ops_per_sec: f64,
    pub mb_per_sec: f64,
}

impl BenchResult {
    pub fn record(name: impl Into<String>, iterations: u64, bytes_per_op: u64, f: impl FnOnce()) -> Self {
        let start = Instant::now();
        f();
        let elapsed = start.elapsed();
        let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
        let mb_per_sec = (iterations as f64 * bytes_per_op as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        Self {
            name: name.into(),
            iterations,
            elapsed,
            ops_per_sec,
            mb_per_sec,
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "{}: {:.0} ops/sec, {:.2} MB/s, {:?} total",
            self.name, self.ops_per_sec, self.mb_per_sec, self.elapsed
        )
    }
}

/// Print a comparison table header for cross-DB benchmarks.
pub fn print_comparison_header(workload: &str) {
    println!("=== Graphite benchmark: {} ===", workload);
    println!("{:<20} {:>14} {:>12} {:>10}", "system", "ops/sec", "MB/s", "elapsed");
}

pub fn print_comparison_row(result: &BenchResult) {
    println!(
        "{:<20} {:>14.0} {:>12.2} {:>10.2?}",
        result.name, result.ops_per_sec, result.mb_per_sec, result.elapsed
    );
}
