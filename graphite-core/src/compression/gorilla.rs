//! Gorilla XOR encoding for floating-point price columns.
//! Byte-aligned variant of Facebook's Gorilla paper — exploits temporal locality.

/// Encode f64 values using Gorilla-style XOR compression.
pub fn encode(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return vec![0, 0, 0, 0];
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u32).to_be_bytes());

    let mut prev = values[0].to_bits();
    buf.extend_from_slice(&prev.to_be_bytes());

    for &val in &values[1..] {
        let cur = val.to_bits();
        let xor = cur ^ prev;

        if xor == 0 {
            buf.push(0);
        } else {
            let leading = xor.leading_zeros();
            let trailing = xor.trailing_zeros();
            let sig_bits = 64 - leading - trailing;

            if sig_bits <= 48 {
                buf.push(1);
                buf.push(leading as u8);
                buf.push(trailing as u8);
                let shifted = if trailing < 64 {
                    xor >> trailing
                } else {
                    0
                };
                let nbytes = ((sig_bits + 7) / 8) as usize;
                let bytes = shifted.to_be_bytes();
                buf.extend_from_slice(&bytes[8 - nbytes..]);
            } else {
                buf.push(2);
                buf.extend_from_slice(&xor.to_be_bytes());
            }
        }
        prev = cur;
    }

    buf
}

/// Decode Gorilla XOR encoded values.
pub fn decode(data: &[u8]) -> Vec<f64> {
    if data.len() < 12 {
        return Vec::new();
    }

    let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if count == 0 {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(count);
    let mut prev = u64::from_be_bytes(data[4..12].try_into().unwrap());
    result.push(f64::from_bits(prev));

    let mut offset = 12;
    for _ in 1..count {
        if offset >= data.len() {
            break;
        }
        let control = data[offset];
        offset += 1;

        match control {
            0 => {
                result.push(f64::from_bits(prev));
            }
            1 => {
                if offset + 2 > data.len() {
                    break;
                }
                let leading = data[offset] as u32;
                offset += 1;
                let trailing = data[offset] as u32;
                offset += 1;
                let sig_bits = 64 - leading - trailing;
                let nbytes = ((sig_bits + 7) / 8) as usize;
                if offset + nbytes > data.len() {
                    break;
                }
                let mut shifted = 0u64;
                for i in 0..nbytes {
                    shifted = (shifted << 8) | data[offset + i] as u64;
                }
                offset += nbytes;
                let xor = if trailing < 64 {
                    shifted << trailing
                } else {
                    0
                };
                let cur = prev ^ xor;
                result.push(f64::from_bits(cur));
                prev = cur;
            }
            2 => {
                if offset + 8 > data.len() {
                    break;
                }
                let xor = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let cur = prev ^ xor;
                result.push(f64::from_bits(cur));
                prev = cur;
            }
            _ => break,
        }
    }

    result
}

fn compute_double_deltas(values: &[f64]) -> Vec<f64> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut deltas = vec![values[0]];
    for i in 1..values.len() {
        deltas.push(values[i] - values[i - 1]);
    }
    let mut double_deltas = vec![deltas[0]];
    for i in 1..deltas.len() {
        double_deltas.push(deltas[i] - deltas[i - 1]);
    }
    double_deltas
}

fn reconstruct_from_double_deltas(double_deltas: &[f64]) -> Vec<f64> {
    if double_deltas.is_empty() {
        return Vec::new();
    }
    let mut deltas = vec![double_deltas[0]];
    for i in 1..double_deltas.len() {
        deltas.push(deltas[i - 1] + double_deltas[i]);
    }
    let mut values = vec![deltas[0]];
    for i in 1..deltas.len() {
        values.push(values[i - 1] + deltas[i]);
    }
    values
}

/// Double-delta + Gorilla XOR encoding for price columns.
pub fn encode_double_delta(values: &[f64]) -> Vec<u8> {
    encode(&compute_double_deltas(values))
}

pub fn decode_double_delta(data: &[u8]) -> Vec<f64> {
    reconstruct_from_double_deltas(&decode(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gorilla_roundtrip() {
        let prices: Vec<f64> = (0..1000)
            .map(|i| 100.0 + (i as f64) * 0.01)
            .collect();
        let encoded = encode(&prices);
        let decoded = decode(&encoded);
        assert_eq!(prices, decoded);
    }

    #[test]
    fn double_delta_roundtrip() {
        let prices: Vec<f64> = (0..1000)
            .map(|i| 100.0 + (i as f64) * 0.01)
            .collect();
        let encoded = encode_double_delta(&prices);
        let decoded = decode_double_delta(&encoded);
        assert_eq!(prices, decoded);
    }

    #[test]
    fn identical_values_compress() {
        let prices: Vec<f64> = vec![150.25; 1000];
        let encoded = encode(&prices);
        let decoded = decode(&encoded);
        assert_eq!(prices, decoded);
        assert!(encoded.len() < prices.len() * 8);
    }
}
