//! Point query latency benchmark (p50/p99).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use graphite::DB;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tempfile::tempdir;

const NUM_TICKS: i64 = 100_000;

fn setup_db() -> (tempfile::TempDir, DB, Vec<i64>) {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();
    let mut timestamps = Vec::new();

    for i in 0..NUM_TICKS {
        let ts = i * 1_000_000_000;
        timestamps.push(ts);
        db.insert("AAPL", ts, 150.0, 151.0, 149.0, 150.0, 1000).unwrap();
    }

    (dir, db, timestamps)
}

fn point_query(c: &mut Criterion) {
    let (_dir, db, timestamps) = setup_db();
    let mut rng = thread_rng();
    let sample: Vec<i64> = timestamps
        .choose_multiple(&mut rng, 1000)
        .copied()
        .collect();

    c.bench_function("point_query_p50", |b| {
        b.iter(|| {
            for &ts in &sample {
                black_box(db.get("AAPL", ts).unwrap());
            }
        });
    });
}

criterion_group!(benches, point_query);
criterion_main!(benches);
