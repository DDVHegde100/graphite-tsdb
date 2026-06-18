//! Write-ahead log: binary append-only log with CRC32 checksums.

use crate::types::WalRecord;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const MAGIC: u32 = 0x47524154; // "GRAT"
const VERSION: u16 = 1;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Corrupt WAL record at offset {offset}: {reason}")]
    Corrupt { offset: u64, reason: String },
    #[error("Serialization error: {0}")]
    Serialize(String),
}

pub struct Wal {
    path: PathBuf,
    file: File,
    offset: u64,
    sequence: u64,
    replayed: Vec<WalRecord>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let offset = file.metadata()?.len();
        let mut wal = Self {
            path,
            file,
            offset,
            sequence: 0,
            replayed: Vec::new(),
        };
        wal.replayed = wal.replay()?;
        Ok(wal)
    }

    /// Append a record and fsync.
    pub fn append(&mut self, record: &WalRecord) -> Result<u64, WalError> {
        let payload = serde_json::to_vec(record)
            .map_err(|e| WalError::Serialize(e.to_string()))?;

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        let header_size = 4 + 2 + 4 + 8 + 4; // magic + version + checksum + seq + length
        let record_size = header_size + payload.len();

        let mut buf = Vec::with_capacity(record_size);
        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.extend_from_slice(&VERSION.to_be_bytes());
        buf.extend_from_slice(&checksum.to_be_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(self.sequence as u64).to_be_bytes());
        buf.extend_from_slice(&payload);

        self.file.write_all(&buf)?;
        self.file.sync_all()?;

        self.offset += record_size as u64;
        self.sequence += 1;
        Ok(self.sequence)
    }

    /// Replay WAL from beginning for crash recovery.
    pub fn replay(&mut self) -> Result<Vec<WalRecord>, WalError> {
        let mut records = Vec::new();
        let mut file = File::open(&self.path)?;
        let mut offset = 0u64;

        loop {
            let mut magic_buf = [0u8; 4];
            if file.read_exact(&mut magic_buf).is_err() {
                break;
            }
            let magic = u32::from_be_bytes(magic_buf);
            if magic != MAGIC {
                return Err(WalError::Corrupt {
                    offset,
                    reason: format!("bad magic: {magic:#x}"),
                });
            }

            let mut version_buf = [0u8; 2];
            file.read_exact(&mut version_buf)?;
            let version = u16::from_be_bytes(version_buf);
            if version != VERSION {
                return Err(WalError::Corrupt {
                    offset,
                    reason: format!("unsupported version: {version}"),
                });
            }

            let mut checksum_buf = [0u8; 4];
            file.read_exact(&mut checksum_buf)?;
            let expected_checksum = u32::from_be_bytes(checksum_buf);

            let mut len_buf = [0u8; 4];
            file.read_exact(&mut len_buf)?;
            let payload_len = u32::from_be_bytes(len_buf) as usize;

            let mut seq_buf = [0u8; 8];
            file.read_exact(&mut seq_buf)?;
            let seq = u64::from_be_bytes(seq_buf);

            let mut payload = vec![0u8; payload_len];
            file.read_exact(&mut payload)?;

            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let actual_checksum = hasher.finalize();
            if actual_checksum != expected_checksum {
                return Err(WalError::Corrupt {
                    offset,
                    reason: "checksum mismatch".into(),
                });
            }

            let record: WalRecord = serde_json::from_slice(&payload)
                .map_err(|e| WalError::Corrupt {
                    offset,
                    reason: e.to_string(),
                })?;

            records.push(record);
            self.sequence = seq + 1;
            offset += 4 + 2 + 4 + 4 + 8 + payload_len as u64;
        }

        Ok(records)
    }

    pub fn truncate(&mut self) -> Result<(), WalError> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        self.offset = 0;
        Ok(())
    }

    pub fn replayed_records(&self) -> &[WalRecord] {
        &self.replayed
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
