//! Delta encoding + bit-packing for timestamp column.

/// Encode timestamps using delta encoding then bit-packing.
pub fn encode(timestamps: &[i64]) -> Vec<u8> {
    if timestamps.is_empty() {
        return vec![0, 0, 0, 0];
    }

    let mut deltas = Vec::with_capacity(timestamps.len());
    deltas.push(timestamps[0]);
    for i in 1..timestamps.len() {
        deltas.push(timestamps[i] - timestamps[i - 1]);
    }

    let min_delta = *deltas.iter().min().unwrap();
    let max_delta = *deltas.iter().max().unwrap();
    let range = max_delta - min_delta;

    let bits_needed = if range == 0 {
        0
    } else {
        (64 - range.leading_zeros()) as u8
    };

    let mut buf = Vec::new();
    buf.extend_from_slice(&(timestamps.len() as u32).to_be_bytes());
    buf.extend_from_slice(&timestamps[0].to_be_bytes());
    buf.push(bits_needed);

    if bits_needed == 0 {
        return buf;
    }

    let mut bit_buffer = 0u64;
    let mut bit_count = 0u8;

    for &delta in &deltas[1..] {
        let normalized = (delta - min_delta) as u64;
        bit_buffer |= normalized << bit_count;
        bit_count += bits_needed;

        while bit_count >= 8 {
            buf.push(bit_buffer as u8);
            bit_buffer >>= 8;
            bit_count -= 8;
        }
    }

    if bit_count > 0 {
        buf.push(bit_buffer as u8);
    }

    buf.extend_from_slice(&min_delta.to_be_bytes());
    buf
}

/// Decode delta-encoded timestamps.
pub fn decode(data: &[u8]) -> Vec<i64> {
    if data.len() < 13 {
        return Vec::new();
    }

    let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    let first = i64::from_be_bytes(data[4..12].try_into().unwrap());
    let bits_needed = data[12];

    if count == 0 {
        return Vec::new();
    }

    if count == 1 {
        return vec![first];
    }

    if bits_needed == 0 {
        return vec![first; count];
    }

    let min_delta_offset = data.len() - 8;
    let min_delta = i64::from_be_bytes(data[min_delta_offset..].try_into().unwrap());
    let packed_data = &data[13..min_delta_offset];

    let mut result = vec![first];
    let mut bit_buffer = 0u64;
    let mut bit_count = 0u8;
    let mut byte_idx = 0;

    for _ in 1..count {
        while bit_count < bits_needed {
            if byte_idx < packed_data.len() {
                bit_buffer |= (packed_data[byte_idx] as u64) << bit_count;
                bit_count += 8;
                byte_idx += 1;
            } else {
                break;
            }
        }

        let mask = if bits_needed == 64 {
            u64::MAX
        } else {
            (1u64 << bits_needed) - 1
        };
        let normalized = (bit_buffer & mask) as i64;
        let delta = normalized + min_delta;
        result.push(result.last().unwrap() + delta);

        bit_buffer >>= bits_needed;
        bit_count -= bits_needed;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let ts: Vec<i64> = (0..1000).map(|i| i as i64 * 1_000_000_000).collect();
        let encoded = encode(&ts);
        let decoded = decode(&encoded);
        assert_eq!(ts, decoded);
    }
}
