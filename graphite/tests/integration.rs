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
