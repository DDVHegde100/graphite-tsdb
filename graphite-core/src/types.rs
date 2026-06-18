//! Core types for Graphite TSDB.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Nanosecond-precision timestamp.
pub type TimestampNs = i64;

/// Symbol identifier (dictionary-encoded).
pub type SymbolId = u16;

/// OHLCV tick record.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub symbol_id: SymbolId,
    pub timestamp: TimestampNs,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

/// Composite key: symbol_id + timestamp for LSM ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key {
    pub symbol_id: SymbolId,
    pub timestamp: TimestampNs,
}

impl Key {
    pub fn new(symbol_id: SymbolId, timestamp: TimestampNs) -> Self {
        Self {
            symbol_id,
            timestamp,
        }
    }

    /// Encode key as bytes for SSTable storage (big-endian).
    pub fn encode(&self) -> [u8; 10] {
        let mut buf = [0u8; 10];
        buf[0..2].copy_from_slice(&self.symbol_id.to_be_bytes());
        buf[2..10].copy_from_slice(&self.timestamp.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < 10 {
            return None;
        }
        let symbol_id = u16::from_be_bytes([buf[0], buf[1]]);
        let timestamp = i64::from_be_bytes(buf[2..10].try_into().unwrap());
        Some(Self {
            symbol_id,
            timestamp,
        })
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.symbol_id.cmp(&other.symbol_id) {
            Ordering::Equal => self.timestamp.cmp(&other.timestamp),
            ord => ord,
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Column identifiers for projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Column {
    Timestamp,
    Symbol,
    Open,
    High,
    Low,
    Close,
    Volume,
}

impl Column {
    pub fn all() -> [Column; 7] {
        [
            Column::Timestamp,
            Column::Symbol,
            Column::Open,
            Column::High,
            Column::Low,
            Column::Close,
            Column::Volume,
        ]
    }
}

/// Database statistics snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbStats {
    pub level_sizes: Vec<u64>,
    pub bloom_filter_hit_rate: f64,
    pub cache_hit_rate: f64,
    pub write_amplification_factor: f64,
    pub total_rows: u64,
    pub total_sstables: u64,
}

/// SSTable metadata stored in footer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsTableMeta {
    pub min_timestamp: TimestampNs,
    pub max_timestamp: TimestampNs,
    pub min_symbol_id: SymbolId,
    pub max_symbol_id: SymbolId,
    pub row_count: u64,
    pub file_size: u64,
    pub level: u32,
}

/// WAL record types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    Insert(Tick),
    Delete(Key),
    Checkpoint { sequence: u64 },
}

/// Block size for SSTable data pages (4KB).
pub const BLOCK_SIZE: usize = 4096;

/// Sparse index interval (every N keys).
pub const INDEX_INTERVAL: usize = 16;

/// L0 maximum SSTable count before compaction.
pub const L0_MAX_TABLES: usize = 4;

/// Size ratio between compaction levels.
pub const LEVEL_SIZE_RATIO: u64 = 10;

/// Default bloom filter false positive rate target (~1%).
pub const BLOOM_FPR: f64 = 0.01;
