//! Cross-database comparison runners for benchmarks.

use crate::BenchResult;
use std::process::Command;
use std::time::Instant;

/// Run Graphite write benchmark inline.
pub fn graphite_write<F>(name: &str, iterations: u64, bytes_per_tick: u64, f: F) -> BenchResult
where
    F: FnOnce(),
{
    BenchResult::record(name, iterations, bytes_per_tick, f)
}

/// Run DuckDB write benchmark when `compare-duckdb` feature is enabled.
pub fn duckdb_write(iterations: u64, bytes_per_tick: u64) -> Option<BenchResult> {
    #[cfg(feature = "compare-duckdb")]
    {
        let result = BenchResult::record("duckdb", iterations, bytes_per_tick, || {
            let conn = duckdb::Connection::open_in_memory().expect("duckdb open");
            conn.execute_batch(
                "CREATE TABLE ticks (symbol TEXT, ts BIGINT, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT)",
            )
            .expect("duckdb schema");
            let mut app = conn
                .prepare("INSERT INTO ticks VALUES ('AAPL', ?, ?, ?, ?, ?, ?)")
                .expect("duckdb prepare");
            for i in 0..iterations {
                app.execute([
                    duckdb::types::Value::BigInt(i as i64 * 1_000_000_000),
                    duckdb::types::Value::Double(150.0),
                    duckdb::types::Value::Double(151.0),
                    duckdb::types::Value::Double(149.0),
                    duckdb::types::Value::Double(150.0),
                    duckdb::types::Value::BigInt(1000),
                ])
                .expect("duckdb insert");
            }
        });
        return Some(result);
    }
    #[cfg(not(feature = "compare-duckdb"))]
    {
        let _ = (iterations, bytes_per_tick);
        None
    }
}

/// POST line protocol ticks to InfluxDB v2 write API when URL/token env vars are set.
pub fn influx_write(iterations: u64, bytes_per_tick: u64) -> Option<BenchResult> {
    let url = std::env::var("GRAPHITE_BENCH_INFLUX_URL").ok();
    let token = std::env::var("GRAPHITE_BENCH_INFLUX_TOKEN").ok();
    let org = std::env::var("GRAPHITE_BENCH_INFLUX_ORG").ok();
    let bucket = std::env::var("GRAPHITE_BENCH_INFLUX_BUCKET").ok();

    let (url, token, org, bucket) = match (url, token, org, bucket) {
        (Some(u), Some(t), Some(o), Some(b)) => (u, t, o, b),
        _ => return None,
    };

    let result = BenchResult::record("influxdb", iterations, bytes_per_tick, || {
        let client = reqwest::blocking::Client::new();
        let mut body = String::new();
        for i in 0..iterations {
            body.push_str(&format!(
                "ticks,symbol=AAPL open=150.0,high=151.0,low=149.0,close=150.0,volume=1000i {}\n",
                i * 1_000_000_000
            ));
        }
        let write_url = format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ns",
            url.trim_end_matches('/'),
            org,
            bucket
        );
        client
            .post(&write_url)
            .header("Authorization", format!("Token {}", token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .expect("influx write");
    });

    Some(result)
}

/// Run `psql` COPY against TimescaleDB when connection env is set.
pub fn timescale_write(iterations: u64, bytes_per_tick: u64) -> Option<BenchResult> {
    let dsn = std::env::var("GRAPHITE_BENCH_TIMESCALE_DSN").ok()?;

    let result = BenchResult::record("timescaledb", iterations, bytes_per_tick, || {
        let mut csv = String::from("symbol,ts,open,high,low,close,volume\n");
        for i in 0..iterations {
            csv.push_str(&format!(
                "AAPL,{},{},{},{},{},{}\n",
                i * 1_000_000_000,
                150.0,
                151.0,
                149.0,
                150.0,
                1000
            ));
        }

        let start = Instant::now();
        Command::new("psql")
            .arg(&dsn)
            .arg("-c")
            .arg("CREATE TABLE IF NOT EXISTS ticks (symbol TEXT, ts BIGINT, open DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION, volume BIGINT)")
            .status()
            .expect("psql create");

        let mut child = Command::new("psql")
            .arg(&dsn)
            .arg("-c")
            .arg("COPY ticks FROM STDIN WITH (FORMAT csv, HEADER true)")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .expect("psql copy spawn");

        use std::io::Write;
        child
            .stdin
            .take()
            .expect("psql stdin")
            .write_all(csv.as_bytes())
            .expect("psql stdin write");
        child.wait().expect("psql wait");
        let _ = start;
    });

    Some(result)
}
