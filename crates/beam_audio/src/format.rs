use riverbase_core::RiverbaseResult;

/// Encode mono float32 PCM (~[-1, 1]) as 16-bit PCM WAV bytes.
///
/// Encode without a `wave` or numpy dependency.
#[must_use]
pub fn pcm_f32_to_wav_bytes(pcm: &[f32], sample_rate: u32) -> Vec<u8> {
    let num_samples = pcm.len();
    let data_len = num_samples * 2; // 16-bit mono
    let byte_rate = sample_rate * 2;
    let mut out = Vec::with_capacity(44 + data_len);

    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&((36 + data_len) as u32).to_le_bytes());
    out.extend_from_slice(b"WAVE");

    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes()); // PCM fmt chunk size
    out.extend_from_slice(&1u16.to_le_bytes()); // audio format = PCM
    out.extend_from_slice(&1u16.to_le_bytes()); // channels = mono
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&2u16.to_le_bytes()); // block align
    out.extend_from_slice(&16u16.to_le_bytes()); // bits per sample

    out.extend_from_slice(b"data");
    out.extend_from_slice(&(data_len as u32).to_le_bytes());
    for &sample in pcm {
        let clamped = sample.clamp(-1.0, 1.0);
        let int16 = (clamped * 32767.0) as i16;
        out.extend_from_slice(&int16.to_le_bytes());
    }
    out
}

/// Parse a 16-bit PCM mono WAV produced by [`pcm_f32_to_wav_bytes`] back into
/// float32 samples (used by tests + the mock STT path).
pub fn parse_wav_pcm_f32(bytes: &[u8]) -> RiverbaseResult<(Vec<f32>, u32)> {
    if bytes.len() < 44 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return Err(crate::BEM_350.raise());
    }
    let sample_rate = u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);
    // Locate the `data` chunk.
    let mut pos = 12;
    while pos + 8 <= bytes.len() {
        let chunk_id = &bytes[pos..pos + 4];
        let chunk_size =
            u32::from_le_bytes([bytes[pos + 4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7]])
                as usize;
        let body = pos + 8;
        if chunk_id == b"data" {
            let end = (body + chunk_size).min(bytes.len());
            let samples = bytes[body..end]
                .chunks_exact(2)
                .map(|b| f32::from(i16::from_le_bytes([b[0], b[1]])) / 32768.0)
                .collect();
            return Ok((samples, sample_rate));
        }
        pos = body + chunk_size + (chunk_size & 1);
    }
    Err(crate::BEM_351.raise())
}

/// Linear resample to `to_hz` (nearest sample). Used to land on 16 kHz PCM.
#[must_use]
pub fn resample_pcm(pcm: &[f32], from_hz: u32, to_hz: u32) -> Vec<f32> {
    if from_hz == 0 || from_hz == to_hz || pcm.is_empty() {
        return pcm.to_vec();
    }
    let n = (pcm.len() as u64 * u64::from(to_hz) / u64::from(from_hz)).max(1) as usize;
    (0..n)
        .map(|i| {
            let src = i as f64 * f64::from(from_hz) / f64::from(to_hz);
            pcm.get(src.floor() as usize).copied().unwrap_or(0.0)
        })
        .collect()
}

/// Decode container bytes to mono float32 at 16 kHz.
///
/// WAV is always supported. With `audio-decode`, a WebM/MP4 header is recognized
/// (host may swap in a native decoder); without a native crate it returns a decode error.
pub fn decode_to_pcm_16k(bytes: &[u8]) -> RiverbaseResult<Vec<f32>> {
    if let Ok((pcm, rate)) = parse_wav_pcm_f32(bytes) {
        return Ok(resample_pcm(&pcm, rate, 16_000));
    }
    #[cfg(feature = "audio-decode")]
    {
        if crate::session::is_probable_webm(bytes) {
            return Err(crate::BEM_352.raise());
        }
    }
    Err(crate::BEM_353.raise())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_roundtrip_preserves_samples() {
        let pcm = vec![0.0_f32, 0.5, -0.5, 1.0, -1.0];
        let wav = pcm_f32_to_wav_bytes(&pcm, 16000);
        assert_eq!(&wav[0..4], b"RIFF");
        let (decoded, rate) = parse_wav_pcm_f32(&wav).unwrap();
        assert_eq!(rate, 16000);
        assert_eq!(decoded.len(), pcm.len());
        for (a, b) in pcm.iter().zip(&decoded) {
            assert!((a - b).abs() < 0.001, "{a} vs {b}");
        }
    }

    #[test]
    fn decode_transcodes_8k_wav_fixture_to_16k() {
        let pcm = vec![0.0_f32, 0.25, -0.25, 0.5];
        let wav = pcm_f32_to_wav_bytes(&pcm, 8000);
        let out = decode_to_pcm_16k(&wav).unwrap();
        assert_eq!(out.len(), 8);
    }
}
