//! Cold tier — archive SSTables to object storage (S3 or local file://).

use crate::sstable::SsTable;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use parking_lot::Mutex;

#[cfg(feature = "cold-tier")]
use object_store::path::Path as ObjectPath;
#[cfg(feature = "cold-tier")]
use object_store::{ObjectStore, ObjectStoreExt};

#[derive(Error, Debug)]
pub enum ColdTierError {
    #[cfg(feature = "cold-tier")]
    #[error("object store: {0}")]
    Store(object_store::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("not found in cold tier: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ColdManifest {
    entries: HashMap<String, ColdEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColdEntry {
    object_path: String,
    synced_at: u64,
}

/// Archives SSTables at or above `min_level` to object storage.
pub struct ColdTier {
    data_dir: PathBuf,
    #[cfg(feature = "cold-tier")]
    store: Arc<dyn ObjectStore>,
    #[cfg(feature = "cold-tier")]
    prefix: ObjectPath,
    min_level: u32,
    manifest: Mutex<ColdManifest>,
}

impl ColdTier {
    /// Open cold tier from a store URI (`s3://bucket/prefix` or `file:///path`).
    #[cfg(feature = "cold-tier")]
    pub fn open(data_dir: PathBuf, store_uri: &str, min_level: u32) -> Result<Self, ColdTierError> {
        let url = url::Url::parse(store_uri)
            .map_err(|e| ColdTierError::Config(format!("invalid store URI: {e}")))?;
        let (store, prefix) = object_store::parse_url(&url).map_err(ColdTierError::Store)?;
        let store: Arc<dyn ObjectStore> = Arc::from(store);
        let manifest = load_manifest(&data_dir)?;
        Ok(Self {
            data_dir,
            store,
            prefix,
            min_level,
            manifest: Mutex::new(manifest),
        })
    }

  #[cfg(not(feature = "cold-tier"))]
    pub fn open(data_dir: PathBuf, store_uri: &str, min_level: u32) -> Result<Self, ColdTierError> {
        let _ = store_uri;
        Err(ColdTierError::Config(
            "cold tier requires the graphite-core `cold-tier` feature".into(),
        ))
    }

    pub fn min_level(&self) -> u32 {
        self.min_level
    }

    pub fn synced_count(&self) -> usize {
        self.manifest.lock().entries.len()
    }

    pub fn is_synced(&self, basename: &str) -> bool {
        self.manifest.lock().entries.contains_key(basename)
    }

    /// Upload SSTables at or above min level that are not yet synced. Local copies remain.
    #[cfg(feature = "cold-tier")]
    pub fn sync_tables(&self, tables: &[&SsTable]) -> Result<usize, ColdTierError> {
        let mut synced = 0;
        let mut manifest = self.manifest.lock();
        for table in tables {
            if table.meta.level < self.min_level {
                continue;
            }
            let basename = table
                .path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            if basename.is_empty() || manifest.entries.contains_key(basename) {
                continue;
            }
            if !table.path.exists() {
                continue;
            }
            let bytes = std::fs::read(&table.path)?;
            let object_path = self.prefix.clone().join(basename);
            block_on(self.store.put(&object_path, bytes.into())).map_err(ColdTierError::Store)?;
            manifest.entries.insert(
                basename.to_string(),
                ColdEntry {
                    object_path: object_path.to_string(),
                    synced_at: unix_now(),
                },
            );
            synced += 1;
        }
        if synced > 0 {
            save_manifest(&self.data_dir, &manifest)?;
        }
        Ok(synced)
    }

    #[cfg(not(feature = "cold-tier"))]
    pub fn sync_tables(&self, _tables: &[&SsTable]) -> Result<usize, ColdTierError> {
        Err(ColdTierError::Config(
            "cold tier requires the graphite-core `cold-tier` feature".into(),
        ))
    }

    /// Download an SSTable from cold tier if missing locally.
    #[cfg(feature = "cold-tier")]
    pub fn ensure_local(&self, local_path: &Path) -> Result<PathBuf, ColdTierError> {
        if local_path.exists() {
            return Ok(local_path.to_path_buf());
        }
        let basename = local_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        let manifest = self.manifest.lock();
        let entry = manifest
            .entries
            .get(basename)
            .ok_or_else(|| ColdTierError::NotFound(basename.to_string()))?;
        let object_path = ObjectPath::from(entry.object_path.as_str());
        drop(manifest);
        let data = block_on(self.store.get(&object_path)).map_err(ColdTierError::Store)?;
        let bytes = block_on(data.bytes()).map_err(ColdTierError::Store)?;
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(local_path, &bytes)?;
        Ok(local_path.to_path_buf())
    }

    #[cfg(not(feature = "cold-tier"))]
    pub fn ensure_local(&self, local_path: &Path) -> Result<PathBuf, ColdTierError> {
        if local_path.exists() {
            return Ok(local_path.to_path_buf());
        }
        let basename = local_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        Err(ColdTierError::NotFound(basename.to_string()))
    }
}

fn load_manifest(data_dir: &Path) -> Result<ColdManifest, ColdTierError> {
    let path = data_dir.join("cold_manifest.json");
    if !path.exists() {
        return Ok(ColdManifest::default());
    }
    let text = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&text)?)
}

fn save_manifest(data_dir: &Path, manifest: &ColdManifest) -> Result<(), ColdTierError> {
    let path = data_dir.join("cold_manifest.json");
    std::fs::write(path, serde_json::to_string_pretty(manifest)?)?;
    Ok(())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(feature = "cold-tier")]
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("cold tier runtime")
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::dictionary::SymbolDictionary;
    use crate::sstable::SsTable;
    use crate::types::Tick;
    use tempfile::tempdir;

    #[cfg(feature = "cold-tier")]
    #[test]
    fn sync_and_restore_roundtrip() {
        let data_dir = tempdir().unwrap();
        let archive_dir = tempdir().unwrap();
        let store_uri = format!("file://{}", archive_dir.path().display());

        let ticks: Vec<Tick> = (0..100)
            .map(|i| Tick {
                symbol_id: 1,
                timestamp: i as i64,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000,
            })
            .collect();
        let dict = SymbolDictionary::new();
        let path = data_dir.path().join("L2_000001.sst");
        let table = SsTable::write(&path, &ticks, 2, &dict).unwrap();

        let mut cold = ColdTier::open(data_dir.path().to_path_buf(), &store_uri, 2).unwrap();
        let synced = cold.sync_tables(&[&table]).unwrap();
        assert_eq!(synced, 1);

        std::fs::remove_file(&path).unwrap();
        cold.ensure_local(&path).unwrap();
        assert!(path.exists());
    }
}
