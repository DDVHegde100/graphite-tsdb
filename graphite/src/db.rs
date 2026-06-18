//! Graphite database API.

use crate::gql::{Executor, QueryResult, parse};
use graphite_core::{DbStats, LsmConfig, LsmError, LsmTree, ScanStream, SymbolTick, Tick, TickBatch};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("LSM error: {0}")]
    Lsm(#[from] LsmError),
    #[error("Parse error: {0}")]
    Parse(#[from] crate::gql::ParseError),
    #[error("Execution error: {0}")]
    Exec(#[from] crate::gql::executor::ExecError),
    #[error("Database not open")]
    NotOpen,
}

/// Graphite time-series database.
pub struct DB {
    path: PathBuf,
    lsm: Arc<LsmTree>,
}

impl DB {
    /// Open or create a database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_config(path, LsmConfig::default())
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: LsmConfig) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let lsm = LsmTree::open(&path, config)?;
        Ok(Self { path, lsm })
    }

    /// Insert a single tick.
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &self,
        symbol: &str,
        timestamp_ns: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: u64,
    ) -> Result<(), DbError> {
        self.lsm
            .insert_tick(symbol, timestamp_ns, open, high, low, close, volume)?;
        Ok(())
    }

    /// Bulk insert ticks for one symbol.
    pub fn insert_batch(&self, symbol: &str, ticks: &[Tick]) -> Result<(), DbError> {
        for tick in ticks {
            self.lsm.insert(symbol, *tick)?;
        }
        Ok(())
    }

    /// Columnar bulk insert for one symbol (single WAL fsync).
    pub fn insert_batch_columns(
        &self,
        symbol: &str,
        batch: &TickBatch,
    ) -> Result<(), DbError> {
        self.lsm.insert_batch(symbol, batch).map_err(DbError::Lsm)
    }

    /// Bulk insert ticks across multiple symbols (single WAL fsync).
    pub fn insert_multi(&self, ticks: &[SymbolTick]) -> Result<(), DbError> {
        self.lsm.insert_multi(ticks).map_err(DbError::Lsm)
    }

    /// Execute a GQL query string.
    pub fn query(&self, gql: &str) -> Result<QueryResult, DbError> {
        let query = parse(gql)?;
        let executor = Executor::new(&self.lsm);
        executor.execute(&query).map_err(DbError::Exec)
    }

    /// Query a time range for a symbol (convenience method).
    pub fn query_range(
        &self,
        symbol: &str,
        t1: i64,
        t2: i64,
    ) -> Result<QueryResult, DbError> {
        let gql = format!(
            "SELECT * FROM {} WHERE timestamp BETWEEN {} AND {}",
            symbol, t1, t2
        );
        self.query(&gql)
    }

    /// Stream ticks for a symbol/time range without materializing all rows.
    pub fn scan_stream(
        &self,
        symbol: &str,
        t1: i64,
        t2: i64,
    ) -> Result<ScanStream, DbError> {
        self.lsm
            .scan_stream(Some(symbol), t1, t2, &graphite_core::Column::all())
            .map_err(DbError::Lsm)
    }

    /// Count ticks in range via streaming scan (no full materialization).
    pub fn count_range(&self, symbol: &str, t1: i64, t2: i64) -> Result<u64, DbError> {
        Ok(self.scan_stream(symbol, t1, t2)?.count() as u64)
    }

    /// Point lookup by symbol and timestamp.
    pub fn get(&self, symbol: &str, timestamp_ns: i64) -> Result<Option<Tick>, DbError> {
        self.lsm.get(symbol, timestamp_ns).map_err(DbError::Lsm)
    }

    /// Trigger compaction manually.
    pub fn compact(&self) -> Result<(), DbError> {
        self.lsm.compact()?;
        Ok(())
    }

    /// Whether background or manual compaction is recommended.
    pub fn needs_compaction(&self) -> bool {
        self.lsm.needs_compaction()
    }

    /// Get database statistics.
    pub fn stats(&self) -> DbStats {
        self.lsm.stats()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Upload archived SSTables to cold tier object storage (requires `cold-tier` feature and config).
    #[cfg(feature = "cold-tier")]
    pub fn sync_cold_tier(&self) -> Result<usize, DbError> {
        self.lsm.sync_cold_tier().map_err(DbError::Lsm)
    }

    #[cfg(feature = "cold-tier")]
    pub fn cold_tier_synced_count(&self) -> usize {
        self.lsm.cold_tier_synced_count()
    }
}
