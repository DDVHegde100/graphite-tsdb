//! RLE + LZ4 or ZSTD block compression for volume column.

use std::io::Cursor;

const CODE_LZ4: u8 = 0;
const CODE_ZSTD: u8 = 1;

fn rle_encode(volumes: &[u64]) -> Vec<u8> {
    let mut rle = Vec::new();
    rle.extend_from_slice(&(volumes.len() as u32).to_be_bytes());

    let mut i = 0;
    while i < volumes.len() {
        let val = volumes[i];
        let mut run_len = 1u32;
        while i + (run_len as usize) < volumes.len() && volumes[i + run_len as usize] == val {
            run_len += 1;
        }
        rle.extend_from_slice(&run_len.to_be_bytes());
        rle.extend_from_slice(&val.to_be_bytes());
        i += run_len as usize;
    }
    rle
}

fn rle_decode(rle: &[u8]) -> Vec<u64> {
    if rle.len() < 4 {
        return Vec::new();
    }
    let total = u32::from_be_bytes(rle[0..4].try_into().unwrap()) as usize;
    let mut result = Vec::with_capacity(total);
    let mut offset = 4;

    while offset < rle.len() && result.len() < total {
        if offset + 12 > rle.len() {
            break;
        }
        let run_len = u32::from_be_bytes(rle[offset..offset + 4].try_into().unwrap()) as usize;
        let val = u64::from_be_bytes(rle[offset + 4..offset + 12].try_into().unwrap());
        for _ in 0..run_len {
            result.push(val);
        }
        offset += 12;
    }
    result
}

/// Encode volumes with LZ4 (default, used for L0).
pub fn encode(volumes: &[u64]) -> Vec<u8> {
    encode_for_level(volumes, 0)
}

/// Encode volumes; L1+ SSTables use ZSTD for better cold-tier compression.
pub fn encode_for_level(volumes: &[u64], level: u32) -> Vec<u8> {
    if volumes.is_empty() {
        return vec![0, 0, 0, 0];
    }

    let rle = rle_encode(volumes);
    let use_zstd = level >= 1;

    let compressed = if use_zstd {
        zstd::stream::encode_all(Cursor::new(&rle), 3)
            .unwrap_or_else(|_| lz4_flex::compress_prepend_size(&rle))
    } else {
        lz4_flex::compress_prepend_size(&rle)
    };

    let mut buf = Vec::with_capacity(1 + compressed.len());
    buf.push(if use_zstd { CODE_ZSTD } else { CODE_LZ4 });
    buf.extend_from_slice(&compressed);
    buf
}

/// Decode volume column regardless of LZ4 or ZSTD wrapper.
pub fn decode(data: &[u8]) -> Vec<u64> {
    if data.is_empty() {
        return Vec::new();
    }

    let code = data[0];
    let payload = &data[1..];

    let rle = match code {
        CODE_LZ4 => lz4_flex::decompress_size_prepended(payload).unwrap_or_default(),
        CODE_ZSTD => zstd::stream::decode_all(payload).unwrap_or_default(),
        _ => lz4_flex::decompress_size_prepended(data).unwrap_or_default(),
    };

    rle_decode(&rle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let volumes: Vec<u64> = vec![100, 100, 100, 200, 200, 300, 300, 300, 300];
        let encoded = encode(&volumes);
        let decoded = decode(&encoded);
        assert_eq!(volumes, decoded);
    }

    #[test]
    fn zstd_level_roundtrip() {
        let volumes: Vec<u64> = vec![1000; 5000];
        let encoded = encode_for_level(&volumes, 2);
        assert_eq!(encoded[0], CODE_ZSTD);
        let decoded = decode(&encoded);
        assert_eq!(volumes, decoded);
    }

    #[test]
    fn rle_compression() {
        let volumes: Vec<u64> = vec![1000; 10000];
        let encoded = encode(&volumes);
        assert!(encoded.len() < 100);
    }
}
