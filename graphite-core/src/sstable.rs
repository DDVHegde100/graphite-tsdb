//! SSTable format: columnar data blocks, sparse index, bloom filter, metadata.

use crate::bloom::BloomFilter;
use crate::block_cache::{CacheKey, SharedBlockCache};
use crate::compression::{delta, dictionary::SymbolDictionary, gorilla, rle_lz4};
use crate::types::{
    BLOCK_SIZE, Column, INDEX_INTERVAL, Key, SsTableMeta, SymbolId, Tick, TimestampNs,
};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

const SSTABLE_MAGIC: u32 = 0x53535442; // "SSTB"
const SSTABLE_VERSION: u16 = 1;

static SSTABLE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Error, Debug)]
pub enum SsTableError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Corrupt SSTable: {0}")]
    Corrupt(String),
}

/// Index entry pointing to a data block offset.
#[derive(Debug, Clone)]
struct IndexEntry {
    key: Key,
    block_offset: u64,
    block_size: u32,
}

/// Footer offsets for SSTable sections.
#[derive(Debug, Clone)]
struct Footer {
    data_offset: u64,
    data_size: u64,
    index_offset: u64,
    index_size: u64,
    bloom_offset: u64,
    bloom_size: u64,
    meta_offset: u64,
    meta_size: u64,
}

/// Immutable SSTable on disk.
pub struct SsTable {
    pub id: u64,
    pub path: PathBuf,
    pub meta: SsTableMeta,
    file: File,
    #[allow(dead_code)]
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    symbol_dict: SymbolDictionary,
    footer: Footer,
}

impl SsTable {
    /// Write a new SSTable from sorted ticks.
    pub fn write(
        path: impl AsRef<Path>,
        ticks: &[Tick],
        level: u32,
        symbol_dict: &SymbolDictionary,
    ) -> Result<Self, SsTableError> {
        let path = path.as_ref().to_path_buf();
        let id = SSTABLE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let mut bloom = BloomFilter::new(ticks.len().max(1));
        for tick in ticks {
            let key = Key::new(tick.symbol_id, tick.timestamp);
            bloom.insert(&key.encode());
            let sym = symbol_dict
                .get_symbol(tick.symbol_id)
                .unwrap_or("UNKNOWN");
            bloom.insert(sym.as_bytes());
        }

        // Encode columns separately (columnar layout)
        let timestamps: Vec<i64> = ticks.iter().map(|t| t.timestamp).collect();
        let opens: Vec<f64> = ticks.iter().map(|t| t.open).collect();
        let highs: Vec<f64> = ticks.iter().map(|t| t.high).collect();
        let lows: Vec<f64> = ticks.iter().map(|t| t.low).collect();
        let closes: Vec<f64> = ticks.iter().map(|t| t.close).collect();
        let volumes: Vec<u64> = ticks.iter().map(|t| t.volume).collect();
        let symbol_ids: Vec<u16> = ticks.iter().map(|t| t.symbol_id).collect();

        let ts_col = delta::encode(&timestamps);
        let open_col = gorilla::encode_double_delta(&opens);
        let high_col = gorilla::encode_double_delta(&highs);
        let low_col = gorilla::encode_double_delta(&lows);
        let close_col = gorilla::encode_double_delta(&closes);
        let vol_col = rle_lz4::encode_for_level(&volumes, level);
        let sym_col = symbol_dict.encode_ids(&symbol_ids);

        // Build data blocks with prefix-compressed keys
        let mut data_buf = Vec::new();
        let mut index = Vec::new();
        let mut prev_key: Option<Key> = None;

        for (i, tick) in ticks.iter().enumerate() {
            let key = Key::new(tick.symbol_id, tick.timestamp);

            if i % INDEX_INTERVAL == 0 {
                index.push(IndexEntry {
                    key,
                    block_offset: data_buf.len() as u64,
                    block_size: 0,
                });
            }

            // Prefix-compress key
            let full_key = key.encode();
            let prefix_len = if let Some(prev) = prev_key {
                let prev_bytes = prev.encode();
                full_key
                    .iter()
                    .zip(prev_bytes.iter())
                    .take_while(|(a, b)| a == b)
                    .count()
            } else {
                0
            };

            data_buf.push(prefix_len as u8);
            data_buf.extend_from_slice(&full_key[prefix_len..]);

            // Store row index for columnar lookup
            data_buf.extend_from_slice(&(i as u32).to_be_bytes());

            prev_key = Some(key);

            // Flush block at BLOCK_SIZE
            if data_buf.len() >= BLOCK_SIZE && i < ticks.len() - 1 {
                if let Some(entry) = index.last_mut() {
                    entry.block_size = data_buf.len() as u32 - entry.block_offset as u32;
                }
            }
        }

        if let Some(entry) = index.last_mut() {
            entry.block_size = data_buf.len() as u32 - entry.block_offset as u32;
        }

        // Append column data after key blocks
        let col_offset = data_buf.len() as u64;
        let col_sizes = [
            ts_col.len(),
            open_col.len(),
            high_col.len(),
            low_col.len(),
            close_col.len(),
            vol_col.len(),
            sym_col.len(),
        ];
        for size in &col_sizes {
            data_buf.extend_from_slice(&(*size as u32).to_be_bytes());
        }
        data_buf.extend_from_slice(&ts_col);
        data_buf.extend_from_slice(&open_col);
        data_buf.extend_from_slice(&high_col);
        data_buf.extend_from_slice(&low_col);
        data_buf.extend_from_slice(&close_col);
        data_buf.extend_from_slice(&vol_col);
        data_buf.extend_from_slice(&sym_col);

        let data_offset = 8u64; // after header
        file.write_all(&SSTABLE_MAGIC.to_be_bytes())?;
        file.write_all(&SSTABLE_VERSION.to_be_bytes())?;
        file.write_all(&data_buf)?;
        let data_size = data_buf.len() as u64;

        // Write index block
        let index_offset = data_offset + data_size;
        let mut index_buf = Vec::new();
        index_buf.extend_from_slice(&(index.len() as u32).to_be_bytes());
        for entry in &index {
            index_buf.extend_from_slice(&entry.key.encode());
            index_buf.extend_from_slice(&entry.block_offset.to_be_bytes());
            index_buf.extend_from_slice(&entry.block_size.to_be_bytes());
        }
        file.write_all(&index_buf)?;
        let index_size = index_buf.len() as u64;

        // Write bloom filter
        let bloom_offset = index_offset + index_size;
        let bloom_bytes = bloom.to_bytes();
        file.write_all(&bloom_bytes)?;
        let bloom_size = bloom_bytes.len() as u64;

        // Write metadata
        let meta_offset = bloom_offset + bloom_size;
        let min_ts = ticks.first().map(|t| t.timestamp).unwrap_or(0);
        let max_ts = ticks.last().map(|t| t.timestamp).unwrap_or(0);
        let min_sym = ticks.iter().map(|t| t.symbol_id).min().unwrap_or(0);
        let max_sym = ticks.iter().map(|t| t.symbol_id).max().unwrap_or(0);

        let meta = SsTableMeta {
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            min_symbol_id: min_sym,
            max_symbol_id: max_sym,
            row_count: ticks.len() as u64,
            file_size: 0,
            level,
        };

        let mut meta_buf = Vec::new();
        meta_buf.extend_from_slice(&meta.min_timestamp.to_be_bytes());
        meta_buf.extend_from_slice(&meta.max_timestamp.to_be_bytes());
        meta_buf.extend_from_slice(&meta.min_symbol_id.to_be_bytes());
        meta_buf.extend_from_slice(&meta.max_symbol_id.to_be_bytes());
        meta_buf.extend_from_slice(&meta.row_count.to_be_bytes());
        meta_buf.extend_from_slice(&level.to_be_bytes());
        meta_buf.extend_from_slice(&col_offset.to_be_bytes());
        let dict_meta = symbol_dict.to_metadata();
        meta_buf.extend_from_slice(&(dict_meta.len() as u32).to_be_bytes());
        meta_buf.extend_from_slice(&dict_meta);
        file.write_all(&meta_buf)?;
        let meta_size = meta_buf.len() as u64;

        // Write footer
        let footer = Footer {
            data_offset,
            data_size,
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            meta_offset,
            meta_size,
        };
        write_footer(&mut file, &footer)?;

        let file_size = file.metadata()?.len();
        file.sync_all()?;

        let final_meta = SsTableMeta {
            file_size,
            ..meta
        };

        Ok(Self {
            id,
            path,
            meta: final_meta,
            file,
            index,
            bloom,
            symbol_dict: symbol_dict.clone(),
            footer,
        })
    }

    /// Open an existing SSTable for reading.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SsTableError> {
        let path = path.as_ref().to_path_buf();
        let id = SSTABLE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut file = File::open(&path)?;
        let file_size = file.metadata()?.len();

        let mut magic_buf = [0u8; 4];
        file.read_exact(&mut magic_buf)?;
        if u32::from_be_bytes(magic_buf) != SSTABLE_MAGIC {
            return Err(SsTableError::Corrupt("bad magic".into()));
        }

        let mut version_buf = [0u8; 2];
        file.read_exact(&mut version_buf)?;

        let footer = read_footer(&mut file, file_size)?;
        let meta = read_metadata(&mut file, &footer)?;
        let index = read_index(&mut file, &footer)?;
        let bloom = read_bloom(&mut file, &footer)?;
        let symbol_dict = read_symbol_dict(&mut file, &footer)?;

        Ok(Self {
            id,
            path,
            meta,
            file,
            index,
            bloom,
            symbol_dict,
            footer,
        })
    }

    /// Check bloom filter for symbol predicate pushdown.
    pub fn may_contain_symbol(&self, symbol: &str) -> bool {
        self.bloom.contains(symbol.as_bytes())
    }

    pub fn may_contain_key(&self, key: &Key) -> bool {
        self.bloom.contains(&key.encode())
    }

    /// Check if SSTable overlaps a time range.
    pub fn overlaps_time_range(&self, t1: TimestampNs, t2: TimestampNs) -> bool {
        self.meta.max_timestamp >= t1 && self.meta.min_timestamp <= t2
    }

    /// Check if SSTable overlaps a symbol ID range.
    pub fn overlaps_symbol(&self, symbol_id: SymbolId) -> bool {
        symbol_id >= self.meta.min_symbol_id && symbol_id <= self.meta.max_symbol_id
    }

    /// Read all ticks matching filters, with optional column projection.
    pub fn scan(
        &mut self,
        symbol_id: Option<SymbolId>,
        t1: TimestampNs,
        t2: TimestampNs,
        columns: &[Column],
        cache: Option<&SharedBlockCache>,
    ) -> Vec<Tick> {
        if !self.overlaps_time_range(t1, t2) {
            return Vec::new();
        }
        if let Some(sid) = symbol_id {
            if !self.overlaps_symbol(sid) {
                return Vec::new();
            }
        }

        let cache_key = CacheKey {
            sstable_id: self.id,
            block_offset: self.footer.data_offset,
        };

        let data = if let Some(c) = cache {
            if let Some(d) = c.get(&cache_key) {
                d
            } else {
                let d = self.read_data_block();
                c.insert(cache_key, d.clone());
                d
            }
        } else {
            self.read_data_block()
        };

        self.decode_ticks(&data, symbol_id, t1, t2, columns)
    }

    fn read_data_block(&mut self) -> Vec<u8> {
        self.file
            .seek(SeekFrom::Start(self.footer.data_offset))
            .unwrap();
        let mut buf = vec![0u8; self.footer.data_size as usize];
        self.file.read_exact(&mut buf).unwrap();
        buf
    }

    fn decode_ticks(
        &self,
        data: &[u8],
        symbol_id: Option<SymbolId>,
        t1: TimestampNs,
        t2: TimestampNs,
        _columns: &[Column],
    ) -> Vec<Tick> {
        if data.len() < 28 {
            return Vec::new();
        }

        // Read column sizes from end of key data
        let col_sizes_offset = data.len() - 28;
        let mut col_sizes = [0u32; 7];
        for (i, slot) in col_sizes.iter_mut().enumerate() {
            let start = col_sizes_offset + i * 4;
            *slot = u32::from_be_bytes(data[start..start + 4].try_into().unwrap());
        }

        let col_start = col_sizes_offset + 28;
        let mut offset = col_start;
        let ts_data = &data[offset..offset + col_sizes[0] as usize];
        offset += col_sizes[0] as usize;
        let open_data = &data[offset..offset + col_sizes[1] as usize];
        offset += col_sizes[1] as usize;
        let high_data = &data[offset..offset + col_sizes[2] as usize];
        offset += col_sizes[2] as usize;
        let low_data = &data[offset..offset + col_sizes[3] as usize];
        offset += col_sizes[3] as usize;
        let close_data = &data[offset..offset + col_sizes[4] as usize];
        offset += col_sizes[4] as usize;
        let vol_data = &data[offset..offset + col_sizes[5] as usize];
        offset += col_sizes[5] as usize;
        let sym_data = &data[offset..offset + col_sizes[6] as usize];

        let timestamps = delta::decode(ts_data);
        let opens = gorilla::decode_double_delta(open_data);
        let highs = gorilla::decode_double_delta(high_data);
        let lows = gorilla::decode_double_delta(low_data);
        let closes = gorilla::decode_double_delta(close_data);
        let volumes = rle_lz4::decode(vol_data);
        let symbol_ids = SymbolDictionary::decode_ids(sym_data);

        let count = timestamps.len();
        let mut result = Vec::new();

        for i in 0..count {
            let ts = timestamps.get(i).copied().unwrap_or(0);
            let sid = symbol_ids.get(i).copied().unwrap_or(0);

            if ts < t1 || ts > t2 {
                continue;
            }
            if let Some(filter_sid) = symbol_id {
                if sid != filter_sid {
                    continue;
                }
            }

            result.push(Tick {
                symbol_id: sid,
                timestamp: ts,
                open: opens.get(i).copied().unwrap_or(0.0),
                high: highs.get(i).copied().unwrap_or(0.0),
                low: lows.get(i).copied().unwrap_or(0.0),
                close: closes.get(i).copied().unwrap_or(0.0),
                volume: volumes.get(i).copied().unwrap_or(0),
            });
        }

        result
    }

    pub fn symbol_dict(&self) -> &SymbolDictionary {
        &self.symbol_dict
    }
}

fn write_footer(file: &mut File, footer: &Footer) -> Result<(), io::Error> {
    file.write_all(&footer.data_offset.to_be_bytes())?;
    file.write_all(&footer.data_size.to_be_bytes())?;
    file.write_all(&footer.index_offset.to_be_bytes())?;
    file.write_all(&footer.index_size.to_be_bytes())?;
    file.write_all(&footer.bloom_offset.to_be_bytes())?;
    file.write_all(&footer.bloom_size.to_be_bytes())?;
    file.write_all(&footer.meta_offset.to_be_bytes())?;
    file.write_all(&footer.meta_size.to_be_bytes())?;
    Ok(())
}

fn read_footer(file: &mut File, file_size: u64) -> Result<Footer, SsTableError> {
    let footer_size = 8 * 8;
    file.seek(SeekFrom::Start(file_size - footer_size))?;
    let mut buf = vec![0u8; footer_size as usize];
    file.read_exact(&mut buf)?;

    let read_u64 = |offset: usize| u64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap());

    Ok(Footer {
        data_offset: read_u64(0),
        data_size: read_u64(8),
        index_offset: read_u64(16),
        index_size: read_u64(24),
        bloom_offset: read_u64(32),
        bloom_size: read_u64(40),
        meta_offset: read_u64(48),
        meta_size: read_u64(56),
    })
}

fn read_metadata(file: &mut File, footer: &Footer) -> Result<SsTableMeta, SsTableError> {
    file.seek(SeekFrom::Start(footer.meta_offset))?;
    let mut buf = vec![0u8; footer.meta_size as usize];
    file.read_exact(&mut buf)?;

    let min_ts = i64::from_be_bytes(buf[0..8].try_into().unwrap());
    let max_ts = i64::from_be_bytes(buf[8..16].try_into().unwrap());
    let min_sym = u16::from_be_bytes(buf[16..18].try_into().unwrap());
    let max_sym = u16::from_be_bytes(buf[18..20].try_into().unwrap());
    let row_count = u64::from_be_bytes(buf[20..28].try_into().unwrap());
    let level = u32::from_be_bytes(buf[28..32].try_into().unwrap());

    Ok(SsTableMeta {
        min_timestamp: min_ts,
        max_timestamp: max_ts,
        min_symbol_id: min_sym,
        max_symbol_id: max_sym,
        row_count,
        file_size: 0,
        level,
    })
}

fn read_index(file: &mut File, footer: &Footer) -> Result<Vec<IndexEntry>, SsTableError> {
    file.seek(SeekFrom::Start(footer.index_offset))?;
    let mut buf = vec![0u8; footer.index_size as usize];
    file.read_exact(&mut buf)?;

    let count = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut index = Vec::with_capacity(count);
    let mut offset = 4;

    for _ in 0..count {
        let key = Key::decode(&buf[offset..offset + 10]).unwrap();
        offset += 10;
        let block_offset = u64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let block_size = u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap());
        offset += 4;
        index.push(IndexEntry {
            key,
            block_offset,
            block_size,
        });
    }

    Ok(index)
}

fn read_bloom(file: &mut File, footer: &Footer) -> Result<BloomFilter, SsTableError> {
    file.seek(SeekFrom::Start(footer.bloom_offset))?;
    let mut buf = vec![0u8; footer.bloom_size as usize];
    file.read_exact(&mut buf)?;
    BloomFilter::from_encoded(&buf).ok_or_else(|| SsTableError::Corrupt("bad bloom filter".into()))
}

fn read_symbol_dict(file: &mut File, footer: &Footer) -> Result<SymbolDictionary, SsTableError> {
    file.seek(SeekFrom::Start(footer.meta_offset))?;
    let mut buf = vec![0u8; footer.meta_size as usize];
    file.read_exact(&mut buf)?;

    let dict_len_offset = 32 + 8; // after fixed meta fields + col_offset
    let dict_len = u32::from_be_bytes(
        buf[dict_len_offset..dict_len_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let dict_data = &buf[dict_len_offset + 4..dict_len_offset + 4 + dict_len];
    Ok(SymbolDictionary::from_metadata(dict_data))
}
