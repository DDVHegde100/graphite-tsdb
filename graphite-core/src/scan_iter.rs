//! Streaming scan iterator — yields ticks without materializing the full range.

use crate::block_cache::SharedBlockCache;
use crate::compaction::CompactionManager;
use crate::compression::dictionary::SymbolDictionary;
use crate::skip_list::SkipList;
use crate::sstable::SsTable;
use crate::types::{Column, SymbolId, Tick, TimestampNs};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::path::PathBuf;

/// Parameters for a streaming scan.
#[derive(Debug, Clone)]
pub struct ScanParams {
    pub symbol_id: Option<SymbolId>,
    pub symbol_name: Option<String>,
    pub t1: TimestampNs,
    pub t2: TimestampNs,
    pub columns: Vec<Column>,
}

/// Lazily loads one SSTable at a time and merges with memtable ticks in order.
pub struct ScanStream {
    memtable_ticks: Vec<Tick>,
    memtable_idx: usize,
    sstable_paths: Vec<PathBuf>,
    sstable_idx: usize,
    current_batch: Vec<Tick>,
    current_batch_idx: usize,
    params: ScanParams,
    cache: SharedBlockCache,
    last_key: Option<(SymbolId, TimestampNs)>,
}

impl ScanStream {
    pub fn new(
        memtable: &SkipList,
        compaction: &CompactionManager,
        symbol_dict: &SymbolDictionary,
        params: ScanParams,
        cache: SharedBlockCache,
    ) -> Self {
        let mut memtable_ticks: Vec<Tick> = memtable
            .iter()
            .filter(|t| t.timestamp >= params.t1 && t.timestamp <= params.t2)
            .filter(|t| match params.symbol_id {
                Some(sid) => t.symbol_id == sid,
                None => true,
            })
            .collect();
        memtable_ticks.sort_by(|a, b| (a.symbol_id, a.timestamp).cmp(&(b.symbol_id, b.timestamp)));

        let mut sstable_paths = Vec::new();
        for level in &compaction.levels {
            for table in &level.tables {
                if !table.overlaps_time_range(params.t1, params.t2) {
                    continue;
                }
                if let Some(sid) = params.symbol_id {
                    let sym = symbol_dict.get_symbol(sid).unwrap_or("UNKNOWN");
                    if !table.may_contain_symbol(sym) || !table.overlaps_symbol(sid) {
                        continue;
                    }
                }
                sstable_paths.push(table.path.clone());
            }
        }

        Self {
            memtable_ticks,
            memtable_idx: 0,
            sstable_paths,
            sstable_idx: 0,
            current_batch: Vec::new(),
            current_batch_idx: 0,
            params,
            cache,
            last_key: None,
        }
    }

    fn load_next_sstable_batch(&mut self) -> bool {
        if self.sstable_idx >= self.sstable_paths.len() {
            return false;
        }
        let path = self.sstable_paths[self.sstable_idx].clone();
        self.sstable_idx += 1;

        if let Ok(mut table) = SsTable::open(&path) {
            let ticks = table.scan(
                self.params.symbol_id,
                self.params.t1,
                self.params.t2,
                &self.params.columns,
                Some(&self.cache),
            );
            self.current_batch = ticks;
            self.current_batch.sort_by(|a, b| (a.symbol_id, a.timestamp).cmp(&(b.symbol_id, b.timestamp)));
            self.current_batch_idx = 0;
            return true;
        }
        false
    }

    fn peek_memtable(&self) -> Option<Tick> {
        self.memtable_ticks.get(self.memtable_idx).copied()
    }

    fn peek_sstable(&self) -> Option<Tick> {
        self.current_batch.get(self.current_batch_idx).copied()
    }

    fn advance_memtable(&mut self) {
        self.memtable_idx += 1;
    }

    fn advance_sstable(&mut self) {
        self.current_batch_idx += 1;
        if self.current_batch_idx >= self.current_batch.len() {
            self.current_batch.clear();
            self.current_batch_idx = 0;
        }
    }

    fn ensure_sstable_batch(&mut self) {
        if self.current_batch_idx >= self.current_batch.len() && self.sstable_idx < self.sstable_paths.len() {
            self.load_next_sstable_batch();
        }
    }

    /// Next tick in sorted order, deduplicated by (symbol_id, timestamp).
    pub fn next_tick(&mut self) -> Option<Tick> {
        loop {
            self.ensure_sstable_batch();

            let mem = self.peek_memtable();
            let sst = self.peek_sstable();

            let next = match (mem, sst) {
                (None, None) => return None,
                (Some(m), None) => {
                    self.advance_memtable();
                    m
                }
                (None, Some(s)) => {
                    self.advance_sstable();
                    s
                }
                (Some(m), Some(s)) => {
                    let cmp = (m.symbol_id, m.timestamp).cmp(&(s.symbol_id, s.timestamp));
                    if cmp == Ordering::Less || cmp == Ordering::Equal {
                        self.advance_memtable();
                        if cmp == Ordering::Equal {
                            self.advance_sstable();
                        }
                        m
                    } else {
                        self.advance_sstable();
                        s
                    }
                }
            };

            let key = (next.symbol_id, next.timestamp);
            if self.last_key == Some(key) {
                continue;
            }
            self.last_key = Some(key);
            return Some(next);
        }
    }
}

impl Iterator for ScanStream {
    type Item = Tick;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_tick()
    }
}

/// Build scan params from optional symbol name.
pub fn scan_params(
    symbol_dict: &RwLock<SymbolDictionary>,
    symbol: Option<&str>,
    t1: TimestampNs,
    t2: TimestampNs,
    columns: &[Column],
) -> Result<ScanParams, crate::LsmError> {
    let symbol_id = if let Some(sym) = symbol {
        let dict = symbol_dict.read();
        Some(
            dict.get_id(sym)
                .ok_or_else(|| crate::LsmError::SymbolNotFound(sym.to_string()))?,
        )
    } else {
        None
    };
    Ok(ScanParams {
        symbol_id,
        symbol_name: symbol.map(|s| s.to_string()),
        t1,
        t2,
        columns: columns.to_vec(),
    })
}
