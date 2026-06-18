//! Sequential write throughput benchmark.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphite::DB;
use tempfile::tempdir;

fn write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    for count in [10000, 100000, 1000000].iter() {
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_with_input(
            BenchmarkId::new("graphite", count),
            count,
            |b, &count| {
                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let db = DB::open(dir.path()).unwrap();
                    for i in 0..count {
                        black_box(
                            db.insert(
                                "AAPL",
                                i as i64 * 1_000_000_000,
                                150.0 + (i as f64) * 0.01,
                                151.0 + (i as f64) * 0.01,
                                149.0 + (i as f64) * 0.01,
                                150.0 + (i as f64) * 0.01,
                                1000,
                            )
                            .unwrap(),
                        );
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, write_throughput);
criterion_main!(benches);
