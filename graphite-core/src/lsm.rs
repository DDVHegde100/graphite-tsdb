//! LSM-tree: MemTable + WAL + SSTables + compaction.

use crate::batch::{SymbolTick, TickBatch};
use crate::scan_iter::{ScanStream, scan_params};
use crate::block_cache::SharedBlockCache;
use crate::compaction::CompactionManager;
use crate::compaction_scheduler::spawn_background_compaction;
use crate::compression::dictionary::SymbolDictionary;
use crate::skip_list::SkipList;
use crate::sstable::SsTable;
use crate::types::{Column, DbStats, Key, Tick, TimestampNs, WalRecord};
use crate::wal::Wal;
#[cfg(feature = "cold-tier")]
use crate::cold_tier::ColdTier;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

const MEMTABLE_FLUSH_THRESHOLD: usize = 10000;

#[derive(Error, Debug)]
pub enum LsmError {
    #[error("WAL error: {0}")]
    Wal(#[from] crate::wal::WalError),
    #[error("SSTable error: {0}")]
    SsTable(#[from] crate::sstable::SsTableError),
    #[error("Compaction error: {0}")]
    Compaction(#[from] crate::compaction::CompactionError),
    #[cfg(feature = "cold-tier")]
    #[error("Cold tier error: {0}")]
    ColdTier(#[from] crate::cold_tier::ColdTierError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Symbol not found: {0}")]
    SymbolNotFound(String),
}

/// Configuration for the LSM-tree.
#[derive(Debug, Clone)]
pub struct LsmConfig {
    pub cache_size_mb: usize,
    pub memtable_flush_threshold: usize,
    /// Run compaction in a background tokio thread when levels are full.
    pub auto_compact: bool,
    /// Interval between background compaction checks (milliseconds).
    pub compact_interval_ms: u64,
    /// Object store URI for cold tier (`s3://bucket/prefix` or `file:///path`). Requires `cold-tier` feature.
    pub cold_tier_uri: Option<String>,
    /// Minimum SSTable level to archive to cold tier (default 2).
    pub cold_tier_min_level: u32,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            cache_size_mb: 64,
            memtable_flush_threshold: MEMTABLE_FLUSH_THRESHOLD,
            auto_compact: true,
            compact_interval_ms: 5000,
            cold_tier_uri: None,
            cold_tier_min_level: 2,
        }
    }
}

/// The core LSM-tree storage engine.
pub struct LsmTree {
    data_dir: PathBuf,
    memtable: RwLock<SkipList>,
    wal: RwLock<Wal>,
    compaction: RwLock<CompactionManager>,
    symbol_dict: RwLock<SymbolDictionary>,
    cache: SharedBlockCache,
    config: LsmConfig,
    bloom_checks: AtomicU64,
    bloom_hits: AtomicU64,
    total_rows: AtomicU64,
    #[cfg(feature = "cold-tier")]
    cold_tier: Option<Arc<ColdTier>>,
}

impl LsmTree {
    pub fn open(path: impl AsRef<Path>, config: LsmConfig) -> Result<Arc<Self>, LsmError> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let wal_path = data_dir.join("wal.log");
        let wal = Wal::open(&wal_path)?;

        let symbol_dict = SymbolDictionary::new();
        let compaction = CompactionManager::new(&data_dir, symbol_dict.clone());
        let cache = SharedBlockCache::new(config.cache_size_mb);

        let cold_tier: Option<Arc<ColdTier>> = {
            #[cfg(feature = "cold-tier")]
            {
                if let Some(uri) = &config.cold_tier_uri {
                    Some(Arc::new(ColdTier::open(
                        data_dir.clone(),
                        uri,
                        config.cold_tier_min_level,
                    )?))
                } else {
                    None
                }
            }
            #[cfg(not(feature = "cold-tier"))]
            {
                None
            }
        };
        let _ = &config.cold_tier_uri;

        let lsm = Arc::new(Self {
            data_dir,
            memtable: RwLock::new(SkipList::new()),
            wal: RwLock::new(wal),
            compaction: RwLock::new(compaction),
            symbol_dict: RwLock::new(symbol_dict),
            cache,
            config,
            bloom_checks: AtomicU64::new(0),
            bloom_hits: AtomicU64::new(0),
            total_rows: AtomicU64::new(0),
            #[cfg(feature = "cold-tier")]
            cold_tier,
        });

        // Replay WAL
        lsm.recover_from_wal()?;

        // Load existing SSTables
        lsm.load_sstables()?;

        if lsm.config.auto_compact {
            spawn_background_compaction(Arc::clone(&lsm), lsm.config.compact_interval_ms);
        }

        Ok(lsm)
    }

    fn open_sstable(&self, path: &Path) -> Result<SsTable, LsmError> {
        #[cfg(feature = "cold-tier")]
        if let Some(cold) = &self.cold_tier {
            if !path.exists() {
                cold.ensure_local(path)?;
            }
        }
        SsTable::open(path).map_err(LsmError::SsTable)
    }

    /// Upload SSTables at or above the cold tier min level to object storage.
    #[cfg(feature = "cold-tier")]
    pub fn sync_cold_tier(&self) -> Result<usize, LsmError> {
        let cold = self.cold_tier.as_ref().ok_or_else(|| {
            LsmError::ColdTier(crate::cold_tier::ColdTierError::Config(
                "cold_tier_uri not configured".into(),
            ))
        })?;
        let compaction = self.compaction.read();
        let tables: Vec<&SsTable> = compaction
            .levels
            .iter()
            .flat_map(|l| l.tables.iter())
            .collect();
        cold.sync_tables(&tables).map_err(LsmError::ColdTier)
    }

    #[cfg(feature = "cold-tier")]
    pub fn cold_tier_synced_count(&self) -> usize {
        self.cold_tier.as_ref().map(|c| c.synced_count()).unwrap_or(0)
    }

    fn recover_from_wal(&self) -> Result<(), LsmError> {
        let wal = self.wal.read();
        let records = wal.replayed_records();
        let mut memtable = self.memtable.write();

        for record in records {
            match record {
                WalRecord::Insert(tick) => {
                    memtable.insert(*tick);
                }
                WalRecord::Delete(key) => {
                    let _ = key;
                }
                WalRecord::Checkpoint { .. } => {}
            }
        }

        Ok(())
    }

    fn load_sstables(&self) -> Result<(), LsmError> {
        let entries = std::fs::read_dir(&self.data_dir)?;
        let mut compaction = self.compaction.write();

        for entry in entries {
            let path = entry?.path();
            if path.extension().map(|e| e == "sst").unwrap_or(false) {
                #[cfg(feature = "cold-tier")]
                if !path.exists() {
                    if let Some(cold) = &self.cold_tier {
                        cold.ensure_local(&path).ok();
                    }
                }
                let table = self.open_sstable(&path)?;
                let level = table.meta.level as usize;
                while compaction.levels.len() <= level {
                    let level_num = compaction.levels.len() as u32;
                    compaction.levels.push(crate::compaction::Level::new(level_num));
                }
                compaction.levels[level].add_table(table);
            }
        }

        Ok(())
    }

    pub fn insert(&self, symbol: &str, tick: Tick) -> Result<(), LsmError> {
        let mut dict = self.symbol_dict.write();
        let symbol_id = dict.get_or_insert(symbol);
        let tick = Tick {
            symbol_id,
            ..tick
        };

        self.wal.write().append(&WalRecord::Insert(tick))?;
        self.memtable.write().insert(tick);
        self.total_rows.fetch_add(1, Ordering::Relaxed);

        let memtable_len = self.memtable.read().len();
        if memtable_len >= self.config.memtable_flush_threshold {
            self.flush_memtable()?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_tick(&self, symbol: &str, timestamp: TimestampNs, open: f64, high: f64, low: f64, close: f64, volume: u64) -> Result<(), LsmError> {
        let tick = Tick {
            symbol_id: 0,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
        };
        self.insert(symbol, tick)
    }

    /// Bulk insert columnar tick batch for one symbol (single WAL fsync).
    pub fn insert_batch(&self, symbol: &str, batch: &TickBatch) -> Result<(), LsmError> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut dict = self.symbol_dict.write();
        let symbol_id = dict.get_or_insert(symbol);

        let mut wal_records = Vec::with_capacity(batch.len());
        let mut memtable = self.memtable.write();

        for i in 0..batch.len() {
            let tick = batch.tick_at(i, symbol_id);
            wal_records.push(WalRecord::Insert(tick));
            memtable.insert(tick);
            self.total_rows.fetch_add(1, Ordering::Relaxed);
        }

        self.wal.write().append_batch(&wal_records)?;

        let memtable_len = memtable.len();
        drop(memtable);
        if memtable_len >= self.config.memtable_flush_threshold {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Bulk insert ticks across multiple symbols (single WAL fsync).
    pub fn insert_multi(&self, ticks: &[SymbolTick]) -> Result<(), LsmError> {
        if ticks.is_empty() {
            return Ok(());
        }

        let mut dict = self.symbol_dict.write();
        let mut wal_records = Vec::with_capacity(ticks.len());
        let mut memtable = self.memtable.write();

        for tick in ticks {
            let symbol_id = dict.get_or_insert(&tick.symbol);
            let stored = tick.to_tick(symbol_id);
            wal_records.push(WalRecord::Insert(stored));
            memtable.insert(stored);
            self.total_rows.fetch_add(1, Ordering::Relaxed);
        }

        self.wal.write().append_batch(&wal_records)?;

        let memtable_len = memtable.len();
        drop(memtable);
        if memtable_len >= self.config.memtable_flush_threshold {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn needs_compaction(&self) -> bool {
        self.compaction.read().needs_compaction()
    }

    pub fn get(&self, symbol: &str, timestamp: TimestampNs) -> Result<Option<Tick>, LsmError> {
        let dict = self.symbol_dict.read();
        let symbol_id = dict
            .get_id(symbol)
            .ok_or_else(|| LsmError::SymbolNotFound(symbol.to_string()))?;
        let key = Key::new(symbol_id, timestamp);

        // Check memtable first
        if let Some(tick) = self.memtable.read().get(&key) {
            return Ok(Some(tick));
        }

        // Check SSTables
        let compaction = self.compaction.read();
        for level in &compaction.levels {
            for table in level.tables.iter().rev() {
                self.bloom_checks.fetch_add(1, Ordering::Relaxed);
                if !table.may_contain_key(&key) {
                    continue;
                }
                self.bloom_hits.fetch_add(1, Ordering::Relaxed);
                if table.overlaps_time_range(timestamp, timestamp)
                    && table.overlaps_symbol(symbol_id)
                {
                    let mut t = self.open_sstable(&table.path)?;
                    let ticks = t.scan(
                        Some(symbol_id),
                        timestamp,
                        timestamp,
                        &[],
                        Some(&self.cache),
                    );
                    if let Some(tick) = ticks.first() {
                        return Ok(Some(*tick));
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn scan(
        &self,
        symbol: Option<&str>,
        t1: TimestampNs,
        t2: TimestampNs,
        columns: &[Column],
    ) -> Result<Vec<Tick>, LsmError> {
        let symbol_id = if let Some(sym) = symbol {
            let dict = self.symbol_dict.read();
            Some(dict.get_id(sym).ok_or_else(|| LsmError::SymbolNotFound(sym.to_string()))?)
        } else {
            None
        };

        let mut result = Vec::new();

        // Scan memtable
        for tick in self.memtable.read().iter() {
            if tick.timestamp >= t1 && tick.timestamp <= t2 {
                if let Some(sid) = symbol_id {
                    if tick.symbol_id != sid {
                        continue;
                    }
                }
                result.push(tick);
            }
        }

        // Scan SSTables with bloom filter pushdown
        let compaction = self.compaction.read();
        for level in &compaction.levels {
            for table in &level.tables {
                if !table.overlaps_time_range(t1, t2) {
                    continue;
                }

                if let Some(sid) = symbol_id {
                    self.bloom_checks.fetch_add(1, Ordering::Relaxed);
                    let sym_name = self
                        .symbol_dict
                        .read()
                        .get_symbol(sid)
                        .unwrap_or("UNKNOWN")
                        .to_string();
                    if !table.may_contain_symbol(&sym_name) {
                        continue;
                    }
                    self.bloom_hits.fetch_add(1, Ordering::Relaxed);
                    if !table.overlaps_symbol(sid) {
                        continue;
                    }
                }

                let mut t = self.open_sstable(&table.path)?;
                let ticks = t.scan(symbol_id, t1, t2, columns, Some(&self.cache));
                result.extend(ticks);
            }
        }

        result.sort_by(|a, b| (a.symbol_id, a.timestamp).cmp(&(b.symbol_id, b.timestamp)));
        result.dedup_by(|a, b| a.symbol_id == b.symbol_id && a.timestamp == b.timestamp);

        Ok(result)
    }

    /// Stream ticks in sorted order without materializing the full result set.
    pub fn scan_stream(
        &self,
        symbol: Option<&str>,
        t1: TimestampNs,
        t2: TimestampNs,
        columns: &[Column],
    ) -> Result<ScanStream, LsmError> {
        let params = scan_params(&self.symbol_dict, symbol, t1, t2, columns)?;
        let compaction = self.compaction.read();
        Ok(ScanStream::new(
            &self.memtable.read(),
            &compaction,
            &self.symbol_dict.read(),
            params,
            self.cache.clone(),
            #[cfg(feature = "cold-tier")]
            self.cold_tier.clone(),
        ))
    }

    pub fn flush_memtable(&self) -> Result<(), LsmError> {
        let ticks = self.memtable.write().drain();
        if ticks.is_empty() {
            return Ok(());
        }

        let filename = format!(
            "L0_{:06}.sst",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = self.data_dir.join(filename);
        let dict = self.symbol_dict.read().clone();
        let table = SsTable::write(&path, &ticks, 0, &dict)?;

        self.compaction.write().levels[0].add_table(table);
        self.wal.write().truncate()?;

        Ok(())
    }

    pub fn compact(&self) -> Result<(), LsmError> {
        let mut compaction = self.compaction.write();
        compaction.compact(Some(&self.cache))?;
        Ok(())
    }

    pub fn stats(&self) -> DbStats {
        let compaction = self.compaction.read();
        let bloom_checks = self.bloom_checks.load(Ordering::Relaxed);
        let bloom_hits = self.bloom_hits.load(Ordering::Relaxed);

        DbStats {
            level_sizes: compaction.level_sizes(),
            bloom_filter_hit_rate: if bloom_checks > 0 {
                bloom_hits as f64 / bloom_checks as f64
            } else {
                0.0
            },
            cache_hit_rate: self.cache.hit_rate(),
            write_amplification_factor: compaction.write_amplification_factor(),
            total_rows: self.total_rows.load(Ordering::Relaxed),
            total_sstables: compaction.total_sstables(),
        }
    }

    pub fn get_symbol(&self, symbol_id: u16) -> Option<String> {
        self.symbol_dict
            .read()
            .get_symbol(symbol_id)
            .map(|s| s.to_string())
    }

    pub fn symbol_dict(&self) -> SymbolDictionary {
        self.symbol_dict.read().clone()
    }
}
