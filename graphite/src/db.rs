//! Graphite database API.

use crate::gql::{Executor, QueryResult, parse};
use graphite_core::{DbStats, LsmConfig, LsmError, LsmTree, Tick};
use std::path::{Path, PathBuf};
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
    lsm: LsmTree,
    config: LsmConfig,
}

impl DB {
    /// Open or create a database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_config(path, LsmConfig::default())
    }

  pub fn open_with_config(path: impl AsRef<Path>, config: LsmConfig) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let lsm = LsmTree::open(&path, config.clone())?;
        Ok(Self {
            path,
            lsm,
            config,
        })
    }

    /// Insert a single tick.
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
        self.lsm.insert_tick(symbol, timestamp_ns, open, high, low, close, volume)?;
        Ok(())
    }

    /// Bulk insert ticks.
    pub fn insert_batch(&self, symbol: &str, ticks: &[Tick]) -> Result<(), DbError> {
        for tick in ticks {
            self.lsm.insert(symbol, *tick)?;
        }
        Ok(())
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

    /// Point lookup by symbol and timestamp.
    pub fn get(&self, symbol: &str, timestamp_ns: i64) -> Result<Option<Tick>, DbError> {
        self.lsm.get(symbol, timestamp_ns).map_err(DbError::Lsm)
    }

    /// Trigger compaction.
    pub fn compact(&self) -> Result<(), DbError> {
        self.lsm.compact()?;
        Ok(())
    }

    /// Get database statistics.
    pub fn stats(&self) -> DbStats {
        self.lsm.stats()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
