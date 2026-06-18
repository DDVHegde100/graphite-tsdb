//! Replication integration test.

use graphite::{DB, LsmConfig};
use graphite_core::NodeRole;
use tempfile::tempdir;

#[test]
fn primary_replica_wal_replication() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    let primary = DB::open(primary_dir.path()).unwrap();
    primary
        .insert("AAPL", 1000, 150.0, 151.0, 149.0, 150.5, 10000)
        .unwrap();
    primary
        .insert("AAPL", 2000, 151.0, 152.0, 150.0, 151.5, 5000)
        .unwrap();

    let entries = primary.read_wal_for_replication(None, 100).unwrap();
    assert_eq!(entries.len(), 2);

    let replica = DB::open_replica(replica_dir.path(), LsmConfig::default()).unwrap();
    assert_eq!(replica.node_role(), NodeRole::Replica);

    let applied = replica.apply_replication_batch(&entries).unwrap();
    assert_eq!(applied, 2);

    assert!(replica
        .insert("AAPL", 3000, 1.0, 1.0, 1.0, 1.0, 1)
        .is_err());

    let tick = replica.get("AAPL", 1000).unwrap().unwrap();
    assert_eq!(tick.timestamp, 1000);
    assert_eq!(tick.close, 150.5);
}

#[test]
fn wal_read_since_filters_sequence() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).unwrap();
    db.insert("MSFT", 1, 1.0, 1.0, 1.0, 1.0, 1).unwrap();
    db.insert("MSFT", 2, 2.0, 2.0, 2.0, 2.0, 2).unwrap();

    let all = db.read_wal_for_replication(None, 100).unwrap();
    let partial = db
        .read_wal_for_replication(Some(all[0].sequence), 100)
        .unwrap();
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].sequence, all[1].sequence);
}
