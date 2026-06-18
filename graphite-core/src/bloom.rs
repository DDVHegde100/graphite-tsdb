//! Bloom filter using FNV-1a hash with ~1% false positive rate.

use crate::types::BLOOM_FPR;

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn fnv1a(data: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Per-SSTable bloom filter for predicate pushdown.
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create bloom filter sized for `num_items` at target FPR.
    pub fn new(num_items: usize) -> Self {
        let num_bits = optimal_num_bits(num_items, BLOOM_FPR);
        let num_hashes = optimal_num_hashes(num_bits, num_items);
        Self {
            bits: vec![0u64; num_bits.div_ceil(64)],
            num_bits,
            num_hashes,
        }
    }

    pub fn from_bytes(data: &[u8], num_bits: usize, num_hashes: u32) -> Self {
        let num_words = num_bits.div_ceil(64);
        let mut bits = vec![0u64; num_words];
        for (i, word) in bits.iter_mut().enumerate() {
            let start = i * 8;
            if start + 8 <= data.len() {
                *word = u64::from_be_bytes(data[start..start + 8].try_into().unwrap());
            }
        }
        Self {
            bits,
            num_bits,
            num_hashes,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        let hash = fnv1a(key);
        for i in 0..self.num_hashes {
            let bit_idx = hash_to_index(hash, i, self.num_bits);
            self.set_bit(bit_idx);
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let hash = fnv1a(key);
        for i in 0..self.num_hashes {
            let bit_idx = hash_to_index(hash, i, self.num_bits);
            if !self.get_bit(bit_idx) {
                return false;
            }
        }
        true
    }

    fn set_bit(&mut self, idx: usize) {
        let word = idx / 64;
        let bit = idx % 64;
        self.bits[word] |= 1u64 << bit;
    }

    fn get_bit(&self, idx: usize) -> bool {
        let word = idx / 64;
        let bit = idx % 64;
        self.bits[word] & (1u64 << bit) != 0
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.bits.len() * 8 + 12);
        buf.extend_from_slice(&(self.num_bits as u32).to_be_bytes());
        buf.extend_from_slice(&self.num_hashes.to_be_bytes());
        for word in &self.bits {
            buf.extend_from_slice(&word.to_be_bytes());
        }
        buf
    }

    pub fn from_encoded(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }
        let num_bits = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let num_hashes = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let bits_data = &data[8..];
        Some(Self::from_bytes(bits_data, num_bits, num_hashes))
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }
}

fn optimal_num_bits(num_items: usize, fpr: f64) -> usize {
    let n = num_items as f64;
    let bits = -(n * fpr.ln()) / (2.0 * fpr.ln().powi(2));
    (bits.ceil() as usize).max(64)
}

fn optimal_num_hashes(num_bits: usize, num_items: usize) -> u32 {
    let n = num_items as f64;
    let k = (num_bits as f64 / n) * 2.0 * std::f64::consts::E.ln();
    (k.round() as u32).clamp(1, 16)
}

fn hash_to_index(hash: u64, i: u32, num_bits: usize) -> usize {
    let combined = hash.wrapping_add(i as u64 * 0x9e3779b97f4a7c15);
    (combined as usize) % num_bits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_contains() {
        let mut bf = BloomFilter::new(1000);
        bf.insert(b"AAPL");
        bf.insert(b"GOOG");
        assert!(bf.contains(b"AAPL"));
        assert!(bf.contains(b"GOOG"));
        // May have false positives but unlikely for unrelated keys
    }

    #[test]
    fn serialize_roundtrip() {
        let mut bf = BloomFilter::new(100);
        bf.insert(b"test");
        let bytes = bf.to_bytes();
        let restored = BloomFilter::from_encoded(&bytes).unwrap();
        assert!(restored.contains(b"test"));
    }
}
