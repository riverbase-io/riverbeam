use async_trait::async_trait;

use riverbase_core::RiverbaseResult;
use crate::format::pcm_f32_to_wav_bytes;

/// Speech-to-text backend (`mock` always available; whisper etc. host-bound).
#[async_trait]
pub trait SttBackend: Send + Sync {
    /// Transcribe decoded mono float32 16 kHz PCM into text.
    async fn transcribe(&self, pcm: &[f32]) -> RiverbaseResult<String>;
}

/// Text-to-speech backend (`mock` always available; gTTS/onnx host-bound).
#[async_trait]
pub trait TtsBackend: Send + Sync {
    /// Synthesize text into WAV bytes.
    async fn synthesize(&self, text: &str) -> RiverbaseResult<Vec<u8>>;
}

/// Agent invocation seam for the audio turn (wired to the worker client).
#[async_trait]
pub trait AudioInvoker: Send + Sync {
    async fn invoke(&self, session_id: &str, transcript: &str) -> RiverbaseResult<String>;
}

/// Deterministic mock STT: reports sample count so tests are stable without a model.
pub struct MockStt {
    transcript: String,
}

impl MockStt {
    #[must_use]
    pub fn new(transcript: impl Into<String>) -> Self {
        Self {
            transcript: transcript.into(),
        }
    }
}

impl Default for MockStt {
    fn default() -> Self {
        Self::new("hello from the microphone")
    }
}

#[async_trait]
impl SttBackend for MockStt {
    async fn transcribe(&self, pcm: &[f32]) -> RiverbaseResult<String> {
        if pcm.is_empty() {
            return Ok(String::new());
        }
        Ok(self.transcript.clone())
    }
}

/// Mock TTS: emits a short silent WAV sized from the reply length.
#[derive(Default)]
pub struct MockTts;

#[async_trait]
impl TtsBackend for MockTts {
    async fn synthesize(&self, text: &str) -> RiverbaseResult<Vec<u8>> {
        // 16 kHz of silence, ~10ms per character (bounded), so output is non-empty.
        let samples = (text.chars().count() * 160).clamp(160, 16_000);
        let pcm = vec![0.0_f32; samples];
        Ok(pcm_f32_to_wav_bytes(&pcm, 16000))
    }
}

/// OpenAI-compatible HTTP STT (`POST {base}/audio/transcriptions`). Behind `audio-http-stt`.
#[cfg(feature = "audio-http-stt")]
pub struct HttpStt {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
}

#[cfg(feature = "audio-http-stt")]
impl HttpStt {
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
        }
    }
}

#[cfg(feature = "audio-http-stt")]
#[async_trait]
impl SttBackend for HttpStt {
    async fn transcribe(&self, pcm: &[f32]) -> RiverbaseResult<String> {
        if pcm.is_empty() {
            return Ok(String::new());
        }
        if self.base_url.is_empty() {
            return Err(crate::BEM_355.raise());
        }
        let wav = pcm_f32_to_wav_bytes(pcm, 16_000);
        let url = format!("{}/audio/transcriptions", self.base_url);
        let part = reqwest::multipart::Part::bytes(wav)
            .file_name("audio.wav")
            .mime_str("audio/wav")
            .map_err(|e| crate::BEM_356.with_data(e.to_string()))?;
        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", "whisper-1");
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|e| crate::BEM_357.with_data(e.to_string()))?;
        let status = resp.status();
        let value: serde_json::Value = resp.json().await.map_err(|e| crate::BEM_358.with_data(e.to_string()))?;
        if !status.is_success() {
            return Err(crate::BEM_359.with_data(format!("stt http {status}: {value}")));
        }
        Ok(value
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string())
    }
}

#[cfg(feature = "audio-http-stt")]
#[cfg(test)]
mod http_tests {
    use super::*;

    #[tokio::test]
    async fn http_stt_requires_base_url() {
        let stt = HttpStt::new("", "k");
        let err = stt.transcribe(&[0.1]).await.unwrap_err();
        assert!(err.to_string().contains("base_url"));
    }
}
