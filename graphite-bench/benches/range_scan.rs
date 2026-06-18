//! Range scan benchmark: SELECT * for rows in time range.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use graphite::DB;
use tempfile::tempdir;

const NUM_TICKS: i64 = 1_000_000;

fn setup_db() -> (tempfile::TempDir, DB) {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    for i in 0..NUM_TICKS {
        let ts = i * 1_000_000_000;
        db.insert("AAPL", ts, 150.0, 151.0, 149.0, 150.0, 1000).unwrap();
    }

    (dir, db)
}

fn range_scan(c: &mut Criterion) {
    let (_dir, db) = setup_db();

    c.bench_function("range_scan_1M_rows", |b| {
        b.iter(|| {
            black_box(
                db.query_range("AAPL", 0, NUM_TICKS * 1_000_000_000)
                    .unwrap(),
            );
        });
    });

    c.bench_function("range_scan_100K_rows", |b| {
        b.iter(|| {
            black_box(
                db.query_range("AAPL", 0, 100_000 * 1_000_000_000).unwrap(),
            );
        });
    });
}

criterion_group!(benches, range_scan);
criterion_main!(benches);
