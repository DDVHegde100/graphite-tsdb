//! Basic Graphite usage example.

use graphite::DB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DB::open("/tmp/graphite-example")?;

    db.insert("AAPL", 1_700_000_000_000_000_000, 150.0, 151.0, 149.0, 150.5, 10000)?;

    let result = db.query("SELECT * FROM AAPL WHERE timestamp BETWEEN 0 AND 9990000000000000000")?;
    println!("query returned {} rows", result.rows.len());

    let count = db.count_range("AAPL", 0, 999_000_000_000)?;
    println!("streaming count: {}", count);

    let stats = db.stats();
    println!("total rows: {}, WAF: {:.2}", stats.total_rows, stats.write_amplification_factor);

    Ok(())
}
