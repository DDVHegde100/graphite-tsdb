//! RLE + LZ4 block compression for volume column.

/// Encode volumes using run-length encoding then LZ4 compression.
pub fn encode(volumes: &[u64]) -> Vec<u8> {
    if volumes.is_empty() {
        return vec![0, 0, 0, 0];
    }

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

    lz4_flex::compress_prepend_size(&rle)
}

/// Decode RLE + LZ4 encoded volumes.
pub fn decode(data: &[u8]) -> Vec<u64> {
    if data.is_empty() {
        return Vec::new();
    }

    let rle = lz4_flex::decompress_size_prepended(data).unwrap_or_default();
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
    fn rle_compression() {
        let volumes: Vec<u64> = vec![1000; 10000];
        let encoded = encode(&volumes);
        assert!(encoded.len() < 100); // Heavy compression on repeated values
    }
}
