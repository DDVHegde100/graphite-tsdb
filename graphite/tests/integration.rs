//! Integration tests for Graphite DB.

use graphite::DB;
use tempfile::tempdir;

#[test]
fn insert_and_query() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    for i in 0..100 {
        db.insert(
            "AAPL",
            i as i64 * 1_000_000_000,
            150.0,
            151.0,
            149.0,
            150.5,
            1000,
        )
        .unwrap();
    }

    let result = db
        .query("SELECT * FROM AAPL WHERE timestamp BETWEEN 0 AND 99000000000")
        .unwrap();
    assert_eq!(result.rows.len(), 100);
}

#[test]
fn point_lookup() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();
    db.insert("GOOG", 5000, 100.0, 101.0, 99.0, 100.5, 500).unwrap();

    let tick = db.get("GOOG", 5000).unwrap().unwrap();
    assert_eq!(tick.close, 100.5);
}

#[test]
fn gql_explain() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();
    db.insert("AAPL", 1000, 150.0, 151.0, 149.0, 150.0, 100).unwrap();

    let result = db
        .query("EXPLAIN SELECT * FROM AAPL WHERE timestamp BETWEEN 0 AND 2000")
        .unwrap();
    assert!(result.explain_plan.is_some());
    assert!(result.explain_plan.unwrap().contains("BloomFilterPushdown"));
}

#[test]
fn batch_insert_columns() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    let batch = graphite_core::TickBatch {
        timestamps: (0..500).map(|i| i as i64 * 1_000_000).collect(),
        opens: vec![150.0; 500],
        highs: vec![151.0; 500],
        lows: vec![149.0; 500],
        closes: vec![150.5; 500],
        volumes: vec![100; 500],
    };
    db.insert_batch_columns("AAPL", &batch).unwrap();

    let result = db.query_range("AAPL", 0, 499_000_000).unwrap();
    assert_eq!(result.rows.len(), 500);
}

#[test]
fn scan_stream_count() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    for i in 0..200 {
        db.insert("MSFT", i as i64 * 1_000_000, 300.0, 301.0, 299.0, 300.0, 50)
            .unwrap();
    }

    let count = db.count_range("MSFT", 0, 199_000_000).unwrap();
    assert_eq!(count, 200);
}

#[test]
fn compaction_and_stats() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();

    for i in 0..500 {
        db.insert("AAPL", i as i64 * 1_000_000, 150.0, 151.0, 149.0, 150.0, 100)
            .unwrap();
    }

    db.compact().unwrap();
    let stats = db.stats();
    assert!(stats.total_rows >= 500);
}
