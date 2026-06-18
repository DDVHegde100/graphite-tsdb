//! Leveled compaction strategy for LSM-tree.

use crate::block_cache::SharedBlockCache;
use crate::compression::dictionary::SymbolDictionary;
use crate::sstable::SsTable;
use crate::types::{L0_MAX_TABLES, LEVEL_SIZE_RATIO, Tick};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("SSTable error: {0}")]
    SsTable(#[from] crate::sstable::SsTableError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// A level in the LSM-tree.
pub struct Level {
    pub level_num: u32,
    pub tables: Vec<SsTable>,
}

impl Level {
    pub fn new(level_num: u32) -> Self {
        Self {
            level_num,
            tables: Vec::new(),
        }
    }

    pub fn total_size(&self) -> u64 {
        self.tables.iter().map(|t| t.meta.file_size).sum()
    }

    pub fn add_table(&mut self, table: SsTable) {
        self.tables.push(table);
    }
}

/// Compaction manager for leveled compaction.
pub struct CompactionManager {
    pub levels: Vec<Level>,
    data_dir: PathBuf,
    symbol_dict: SymbolDictionary,
    bytes_written: u64,
    bytes_read: u64,
}

impl CompactionManager {
    pub fn new(data_dir: impl AsRef<Path>, symbol_dict: SymbolDictionary) -> Self {
        Self {
            levels: vec![Level::new(0)],
            data_dir: data_dir.as_ref().to_path_buf(),
            symbol_dict,
            bytes_written: 0,
            bytes_read: 0,
        }
    }

    pub fn needs_compaction(&self) -> bool {
        if self.levels.is_empty() {
            return false;
        }
        let l0 = &self.levels[0];
        if l0.tables.len() >= L0_MAX_TABLES {
            return true;
        }

        for (i, level) in self.levels.iter().enumerate().skip(1) {
            let target_size = LEVEL_SIZE_RATIO.pow(i as u32) * BLOCK_SIZE_TARGET();
            if level.total_size() > target_size {
                return true;
            }
        }

        false
    }

    pub fn compact(&mut self, cache: Option<&SharedBlockCache>) -> Result<(), CompactionError> {
        if !self.needs_compaction() {
            return Ok(());
        }

        // Compact L0 first
        if self.levels[0].tables.len() >= L0_MAX_TABLES {
            self.compact_level(0, cache)?;
        }

        // Check deeper levels
        for i in 1..self.levels.len() {
            let target_size = LEVEL_SIZE_RATIO.pow(i as u32) * BLOCK_SIZE_TARGET();
            if self.levels[i].total_size() > target_size {
                self.compact_level(i, cache)?;
            }
        }

        Ok(())
    }

    fn compact_level(
        &mut self,
        level_idx: usize,
        cache: Option<&SharedBlockCache>,
    ) -> Result<(), CompactionError> {
        let table_paths: Vec<PathBuf> = self.levels[level_idx]
            .tables
            .iter()
            .map(|t| t.path.clone())
            .collect();
        if table_paths.is_empty() {
            return Ok(());
        }

        let mut all_ticks: Vec<Tick> = Vec::new();
        for path in &table_paths {
            let table = SsTable::open(path)?;
            self.bytes_read += table.meta.file_size;
            let mut t = SsTable::open(path)?;
            let ticks = t.scan(None, i64::MIN, i64::MAX, &[], cache);
            all_ticks.extend(ticks);
        }

        // Remove old tables
        for path in &table_paths {
            std::fs::remove_file(path).ok();
        }
        self.levels[level_idx].tables.clear();

        // Sort and deduplicate
        all_ticks.sort_by(|a, b| {
            (a.symbol_id, a.timestamp).cmp(&(b.symbol_id, b.timestamp))
        });
        all_ticks.dedup_by(|a, b| a.symbol_id == b.symbol_id && a.timestamp == b.timestamp);

        // Write merged tables to next level
        let next_level = level_idx + 1;
        while self.levels.len() <= next_level {
            self.levels.push(Level::new(self.levels.len() as u32));
        }

        let chunk_size = 10000;
        for chunk_start in (0..all_ticks.len()).step_by(chunk_size) {
            let chunk_end = (chunk_start + chunk_size).min(all_ticks.len());
            let chunk = &all_ticks[chunk_start..chunk_end];

            let filename = format!(
                "L{}_{:06}.sst",
                next_level,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let path = self.data_dir.join(filename);
            let table = SsTable::write(&path, chunk, next_level as u32, &self.symbol_dict)?;
            self.bytes_written += table.meta.file_size;
            self.levels[next_level].add_table(table);
        }

        Ok(())
    }

    pub fn write_amplification_factor(&self) -> f64 {
        if self.bytes_read == 0 {
            1.0
        } else {
            self.bytes_written as f64 / self.bytes_read as f64
        }
    }

    pub fn level_sizes(&self) -> Vec<u64> {
        self.levels.iter().map(|l| l.total_size()).collect()
    }

    pub fn total_sstables(&self) -> u64 {
        self.levels.iter().map(|l| l.tables.len() as u64).sum()
    }
}

fn BLOCK_SIZE_TARGET() -> u64 {
    4 * 1024 * 1024 // 4MB target per level
}
