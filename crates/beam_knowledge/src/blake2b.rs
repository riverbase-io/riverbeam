//! `BLAKE2b` (RFC 7693), unkeyed, for Python's `hashlib.blake2b(digest_size=n)`.
//! Only [`crate::MockEmbedder`] uses it, so it stays in-crate rather than a
//! new dependency.

const IV: [u64; 8] = [
    0x6a09_e667_f3bc_c908,
    0xbb67_ae85_84ca_a73b,
    0x3c6e_f372_fe94_f82b,
    0xa54f_f53a_5f1d_36f1,
    0x510e_527f_ade6_82d1,
    0x9b05_688c_2b3e_6c1f,
    0x1f83_d9ab_fb41_bd6b,
    0x5be0_cd19_137e_2179,
];

const SIGMA: [[usize; 16]; 12] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
];

#[allow(clippy::many_single_char_names)]
fn mix(v: &mut [u64; 16], a: usize, b: usize, c: usize, d: usize, x: u64, y: u64) {
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(x);
    v[d] = (v[d] ^ v[a]).rotate_right(32);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(24);
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(y);
    v[d] = (v[d] ^ v[a]).rotate_right(16);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(63);
}

fn compress(h: &mut [u64; 8], block: &[u8; 128], counter: u128, last: bool) {
    let mut m = [0u64; 16];
    for (i, word) in m.iter_mut().enumerate() {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&block[i * 8..i * 8 + 8]);
        *word = u64::from_le_bytes(bytes);
    }
    let mut v = [0u64; 16];
    v[..8].copy_from_slice(h);
    v[8..].copy_from_slice(&IV);
    #[allow(clippy::cast_possible_truncation)]
    {
        v[12] ^= counter as u64;
        v[13] ^= (counter >> 64) as u64;
    }
    if last {
        v[14] = !v[14];
    }
    for s in &SIGMA {
        mix(&mut v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
        mix(&mut v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
        mix(&mut v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
        mix(&mut v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
        mix(&mut v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
        mix(&mut v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        mix(&mut v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
        mix(&mut v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
    }
    for i in 0..8 {
        h[i] ^= v[i] ^ v[i + 8];
    }
}

/// `BLAKE2b` of `data` with an `out_len`-byte digest (1..=64).
#[must_use]
pub fn blake2b(data: &[u8], out_len: usize) -> Vec<u8> {
    let out_len = out_len.clamp(1, 64);
    let mut h = IV;
    h[0] ^= 0x0101_0000 ^ out_len as u64;
    let mut counter: u128 = 0;
    let mut chunks = data.chunks(128).peekable();
    if data.is_empty() {
        compress(&mut h, &[0u8; 128], 0, true);
    }
    while let Some(chunk) = chunks.next() {
        let mut block = [0u8; 128];
        block[..chunk.len()].copy_from_slice(chunk);
        counter += chunk.len() as u128;
        compress(&mut h, &block, counter, chunks.peek().is_none());
    }
    h.iter()
        .flat_map(|w| w.to_le_bytes())
        .take(out_len)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::blake2b;

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        bytes.iter().fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
    }

    /// Python: `hashlib.blake2b(b"...", digest_size=n).hexdigest()`.
    #[test]
    fn matches_python_hashlib() {
        assert_eq!(hex(&blake2b(b"abc", 64)), "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923");
        assert_eq!(hex(&blake2b(b"invoice", 8)), PY_INVOICE_8);
        assert_eq!(hex(&blake2b(b"", 8)), PY_EMPTY_8);
        assert_eq!(hex(&blake2b(&[b'x'; 300], 8)), PY_X300_8);
        assert_eq!(hex(&blake2b(&[b'x'; 128], 8)), "901ce374e8e12402");
    }

    const PY_INVOICE_8: &str = "5d12d4d238ac776c";
    const PY_EMPTY_8: &str = "e4a6a0577479b2b4";
    const PY_X300_8: &str = "12b539c1cad94615";
}
