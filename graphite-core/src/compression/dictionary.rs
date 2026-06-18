//! Dictionary encoding for symbol column.

use std::collections::HashMap;

/// Symbol dictionary: maps symbol strings to u16 IDs.
#[derive(Debug, Clone, Default)]
pub struct SymbolDictionary {
    symbol_to_id: HashMap<String, u16>,
    id_to_symbol: Vec<String>,
}

impl SymbolDictionary {
    pub fn new() -> Self {
        Self {
            symbol_to_id: HashMap::new(),
            id_to_symbol: Vec::new(),
        }
    }

    pub fn get_or_insert(&mut self, symbol: &str) -> u16 {
        if let Some(&id) = self.symbol_to_id.get(symbol) {
            return id;
        }
        let id = self.id_to_symbol.len() as u16;
        self.symbol_to_id.insert(symbol.to_string(), id);
        self.id_to_symbol.push(symbol.to_string());
        id
    }

    pub fn get_id(&self, symbol: &str) -> Option<u16> {
        self.symbol_to_id.get(symbol).copied()
    }

    pub fn get_symbol(&self, id: u16) -> Option<&str> {
        self.id_to_symbol.get(id as usize).map(|s| s.as_str())
    }

    pub fn encode_ids(&self, ids: &[u16]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + ids.len() * 2);
        buf.extend_from_slice(&(ids.len() as u32).to_be_bytes());
        for &id in ids {
            buf.extend_from_slice(&id.to_be_bytes());
        }
        buf
    }

    pub fn decode_ids(data: &[u8]) -> Vec<u16> {
        if data.len() < 4 {
            return Vec::new();
        }
        let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 4 + i * 2;
            if offset + 2 <= data.len() {
                ids.push(u16::from_be_bytes(data[offset..offset + 2].try_into().unwrap()));
            }
        }
        ids
    }

    pub fn to_metadata(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.id_to_symbol.len() as u32).to_be_bytes());
        for symbol in &self.id_to_symbol {
            let bytes = symbol.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        buf
    }

    pub fn from_metadata(data: &[u8]) -> Self {
        let mut dict = SymbolDictionary::new();
        if data.len() < 4 {
            return dict;
        }
        let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        for _ in 0..count {
            if offset + 2 > data.len() {
                break;
            }
            let len = u16::from_be_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if offset + len > data.len() {
                break;
            }
            let symbol = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
            offset += len;
            dict.get_or_insert(&symbol);
        }
        dict
    }

    pub fn len(&self) -> usize {
        self.id_to_symbol.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_symbol.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_roundtrip() {
        let mut dict = SymbolDictionary::new();
        let id1 = dict.get_or_insert("AAPL");
        let id2 = dict.get_or_insert("GOOG");
        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(dict.get_symbol(0), Some("AAPL"));
    }

    #[test]
    fn metadata_roundtrip() {
        let mut dict = SymbolDictionary::new();
        dict.get_or_insert("AAPL");
        dict.get_or_insert("GOOG");
        let meta = dict.to_metadata();
        let restored = SymbolDictionary::from_metadata(&meta);
        assert_eq!(restored.get_symbol(0), Some("AAPL"));
        assert_eq!(restored.get_symbol(1), Some("GOOG"));
    }
}
