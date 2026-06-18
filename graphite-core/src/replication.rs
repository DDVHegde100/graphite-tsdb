//! WAL streaming replication — primary/replica roles with HTTP sync.

use crate::types::WalRecord;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Accepts writes and serves WAL to replicas.
    Primary,
    /// Read-only; applies WAL from primary.
    Replica,
}

impl Default for NodeRole {
    fn default() -> Self {
        Self::Primary
    }
}

/// A WAL entry for replication streaming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    pub sequence: u64,
    pub record: WalRecord,
    /// Symbol name for Insert records (dictionary sync on replicas).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// Replication batch for POST /replication/apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationBatch {
    pub entries: Vec<ReplicationEntry>,
}

/// Node replication status snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub role: NodeRole,
    pub wal_sequence: u64,
    pub last_primary_sequence: u64,
    pub lag_entries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReplicationState {
  #[serde(default)]
  last_primary_sequence: Option<u64>,
}

#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("replica cannot accept writes")]
    NotPrimary,
    #[error("only replicas accept replication batches")]
    NotReplica,
}

pub struct ReplicationTracker {
    state_path: PathBuf,
    state: ReplicationState,
}

impl ReplicationTracker {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, ReplicationError> {
        let state_path = data_dir.as_ref().join("replication.json");
        let state = if state_path.exists() {
            serde_json::from_str(&std::fs::read_to_string(&state_path)?)?
        } else {
            ReplicationState::default()
        };
        Ok(Self { state_path, state })
    }

    pub fn last_primary_sequence(&self) -> u64 {
        self.state.last_primary_sequence.unwrap_or(0)
    }

    pub fn last_applied(&self) -> Option<u64> {
        self.state.last_primary_sequence
    }

    pub fn advance(&mut self, sequence: u64) -> Result<(), ReplicationError> {
        let current = self.state.last_primary_sequence.unwrap_or(0);
        if sequence >= current {
            self.state.last_primary_sequence = Some(sequence);
            std::fs::write(&self.state_path, serde_json::to_string_pretty(&self.state)?)?;
        }
        Ok(())
    }

    pub fn lag(&self, primary_wal_sequence: u64) -> u64 {
        primary_wal_sequence.saturating_sub(self.last_primary_sequence())
    }
}
