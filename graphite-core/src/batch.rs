//! Multi-symbol tick batch for bulk insert APIs.

use crate::types::Tick;

/// A tick with its symbol string (resolved to symbol_id on insert).
#[derive(Debug, Clone)]
pub struct SymbolTick {
    pub symbol: String,
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

impl SymbolTick {
    pub fn new(
        symbol: impl Into<String>,
        timestamp: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: u64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        }
    }

    pub fn to_tick(&self, symbol_id: u16) -> Tick {
        Tick {
            symbol_id,
            timestamp: self.timestamp,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        }
    }
}

/// Columnar batch for a single symbol (efficient bulk insert).
#[derive(Debug, Clone)]
pub struct TickBatch {
    pub timestamps: Vec<i64>,
    pub opens: Vec<f64>,
    pub highs: Vec<f64>,
    pub lows: Vec<f64>,
    pub closes: Vec<f64>,
    pub volumes: Vec<u64>,
}

impl TickBatch {
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn tick_at(&self, i: usize, symbol_id: u16) -> Tick {
        Tick {
            symbol_id,
            timestamp: self.timestamps[i],
            open: self.opens[i],
            high: self.highs[i],
            low: self.lows[i],
            close: self.closes[i],
            volume: self.volumes[i],
        }
    }
}
