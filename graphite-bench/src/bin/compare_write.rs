//! Cross-database write throughput comparison.

use graphite::DB;
use graphite_bench::compare::{duckdb_write, influx_write, timescale_write};
use graphite_bench::{print_comparison_header, print_comparison_row};
use tempfile::tempdir;

const NUM_TICKS: u64 = 50_000;
const BYTES_PER_TICK: u64 = 80;

fn main() {
    print_comparison_header("sequential write (50K ticks)");

    let dir = tempdir().expect("tempdir");
    let graphite_result = graphite_bench::compare::graphite_write("graphite", NUM_TICKS, BYTES_PER_TICK, || {
        let db = DB::open(dir.path()).expect("open");
        for i in 0..NUM_TICKS {
            db.insert(
                "AAPL",
                i as i64 * 1_000_000_000,
                150.0,
                151.0,
                149.0,
                150.0,
                1000,
            )
            .expect("insert");
        }
    });
    print_comparison_row(&graphite_result);

    if let Some(duck) = duckdb_write(NUM_TICKS, BYTES_PER_TICK) {
        print_comparison_row(&duck);
    } else {
        println!(
            "{:<20} skipped (cargo run -p graphite-bench --features compare-duckdb --bin compare_write)",
            "duckdb"
        );
    }

    if let Some(influx) = influx_write(NUM_TICKS, BYTES_PER_TICK) {
        print_comparison_row(&influx);
    } else {
        println!("{:<20} skipped (set GRAPHITE_BENCH_INFLUX_* env vars)", "influxdb");
    }

    if let Some(ts) = timescale_write(NUM_TICKS, BYTES_PER_TICK) {
        print_comparison_row(&ts);
    } else {
        println!(
            "{:<20} skipped (set GRAPHITE_BENCH_TIMESCALE_DSN env var)",
            "timescaledb"
        );
    }
}
