//! Speech backends (Python `stream/audio/stt.py`, `tts.py`).
//!
//! Python runs faster-whisper, gTTS and kokoro-onnx in process. Under D8 those
//! become OpenAI-compatible HTTP sidecars ([`HttpStt`], [`HttpTts`], feature
//! `audio-http`); the keys Python accepts select them, so `AUDIO_STT_BACKEND`
//! and `AUDIO_TTS_BACKEND` keep their values. The mocks match Python's.

use std::sync::Arc;

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;

use crate::format::pcm_f32_to_wav_bytes;

/// Speech-to-text backend.
#[async_trait]
pub trait SttBackend: Send + Sync {
    /// Transcribe decoded mono float32 16 kHz PCM into text.
    async fn transcribe(&self, pcm: &[f32]) -> RiverbaseResult<String>;
}

/// Text-to-speech backend.
#[async_trait]
pub trait TtsBackend: Send + Sync {
    /// Synthesize text into WAV bytes.
    async fn synthesize(&self, text: &str) -> RiverbaseResult<Vec<u8>>;
}

/// Agent invocation seam for the audio turn (wired to the worker client).
#[async_trait]
pub trait AudioInvoker: Send + Sync {
    /// Run the agent on `transcript`, with the turn's request facts and
    /// client capabilities.
    async fn invoke(
        &self,
        session_id: &str,
        transcript: &str,
        inputs: &crate::session::TurnInputs,
    ) -> RiverbaseResult<String>;
}

fn env(key: &str) -> Option<String> {
    beam_env::string(key)
}

/// `AUDIO_MOCK_TRANSCRIPT`, or a fixed sentence.
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
        Self::new(
            env("AUDIO_MOCK_TRANSCRIPT")
                .unwrap_or_else(|| "(mock STT) Audio pipeline test.".into()),
        )
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

/// A 220 Hz tone at 22.05 kHz, 0.6–2.5 s long by
/// reply length.
#[derive(Default)]
pub struct MockTts;

impl MockTts {
    const SAMPLE_RATE: u32 = 22_050;
}

#[async_trait]
impl TtsBackend for MockTts {
    async fn synthesize(&self, text: &str) -> RiverbaseResult<Vec<u8>> {
        #[allow(clippy::cast_precision_loss)]
        let chars = text.chars().count() as f64;
        let duration = (chars / 80.0).clamp(0.6, 2.5);
        let rate = f64::from(Self::SAMPLE_RATE);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let samples = (duration * rate) as usize;
        #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
        let pcm: Vec<f32> = (0..samples)
            .map(|i| (0.15 * (2.0 * std::f64::consts::PI * 220.0 * (i as f64 / rate)).sin()) as f32)
            .collect();
        Ok(pcm_f32_to_wav_bytes(&pcm, Self::SAMPLE_RATE))
    }
}

/// What `auto` answers without a
/// working speech-to-text backend.
fn fallback_transcript() -> String {
    env("AUDIO_MOCK_TRANSCRIPT").unwrap_or_else(|| {
        "(STT not configured: set AUDIO_STT_URL to a speech-to-text sidecar, or use AUDIO_STT_BACKEND=mock)".into()
    })
}

/// The sidecar when one is configured, else (or when
/// it fails) the fallback message.
struct AutoStt {
    http: Option<Box<dyn SttBackend>>,
}

#[async_trait]
impl SttBackend for AutoStt {
    async fn transcribe(&self, pcm: &[f32]) -> RiverbaseResult<String> {
        if let Some(http) = &self.http {
            match http.transcribe(pcm).await {
                Ok(text) => return Ok(text),
                Err(e) => tracing::warn!(error = %e, "speech-to-text sidecar failed"),
            }
        }
        Ok(fallback_transcript())
    }
}

/// The sidecar when one is configured, else (or when
/// it fails) the mock tone.
struct AutoTts {
    http: Option<Box<dyn TtsBackend>>,
}

#[async_trait]
impl TtsBackend for AutoTts {
    async fn synthesize(&self, text: &str) -> RiverbaseResult<Vec<u8>> {
        if let Some(http) = &self.http {
            match http.synthesize(text).await {
                Ok(wav) => return Ok(wav),
                Err(e) => tracing::warn!(error = %e, "text-to-speech sidecar failed"),
            }
        }
        MockTts.synthesize(text).await
    }
}

/// `mock`, `auto`, or a sidecar for
/// `faster_whisper` / `http` (`AUDIO_STT_URL`).
///
/// # Errors
/// `BEM-503` for an unknown key, or a sidecar key without `AUDIO_STT_URL`
/// (or without the `audio-http` feature).
pub fn build_stt_backend(key: &str) -> RiverbaseResult<Arc<dyn SttBackend>> {
    match key.trim().to_lowercase().as_str() {
        "mock" => Ok(Arc::new(MockStt::default())),
        "auto" => Ok(Arc::new(AutoStt { http: http_stt() })),
        "faster_whisper" | "http" => http_stt().map(Arc::from).ok_or_else(|| {
            crate::BEM_503.with_data(format!("AUDIO_STT_BACKEND={key} needs AUDIO_STT_URL"))
        }),
        _ => Err(crate::BEM_503.with_data(format!(
            "unknown AUDIO_STT_BACKEND={key}; choose one of: auto, faster_whisper, http, mock"
        ))),
    }
}

/// `mock`, `auto`, or a sidecar for `gtts`,
/// `onnx` / `kokoro_onnx` / `http` (`AUDIO_TTS_URL`).
///
/// # Errors
/// `BEM-503` for an unknown key or a bad `AUDIO_TTS_SPEED`, or a sidecar key
/// without `AUDIO_TTS_URL` (or without the `audio-http` feature).
pub fn build_tts_backend(key: &str) -> RiverbaseResult<Arc<dyn TtsBackend>> {
    match key.trim().to_lowercase().as_str() {
        "mock" => Ok(Arc::new(MockTts)),
        "auto" => Ok(Arc::new(AutoTts { http: http_tts()? })),
        "gtts" | "onnx" | "kokoro_onnx" | "http" => http_tts()?
            .map(Arc::from)
            .ok_or_else(|| crate::BEM_503.with_data(format!("AUDIO_TTS_BACKEND={key} needs AUDIO_TTS_URL"))),
        _ => Err(crate::BEM_503.with_data(format!(
            "unknown AUDIO_TTS_BACKEND={key}; choose one of: auto, gtts, http, kokoro_onnx, mock, onnx"
        ))),
    }
}

/// The backends `AUDIO_STT_BACKEND` and `AUDIO_TTS_BACKEND` name (default
/// `auto`, as in Python's `configure_audio_ws`).
///
/// # Errors
/// See [`build_stt_backend`] and [`build_tts_backend`].
pub fn backends_from_env() -> RiverbaseResult<(Arc<dyn SttBackend>, Arc<dyn TtsBackend>)> {
    let stt = build_stt_backend(&env("AUDIO_STT_BACKEND").unwrap_or_else(|| "auto".into()))?;
    let tts = build_tts_backend(&env("AUDIO_TTS_BACKEND").unwrap_or_else(|| "auto".into()))?;
    Ok((stt, tts))
}

#[cfg(feature = "audio-http")]
fn http_stt() -> Option<Box<dyn SttBackend>> {
    HttpStt::from_env().map(|s| Box::new(s) as Box<dyn SttBackend>)
}

#[cfg(not(feature = "audio-http"))]
fn http_stt() -> Option<Box<dyn SttBackend>> {
    None
}

#[cfg(feature = "audio-http")]
fn http_tts() -> RiverbaseResult<Option<Box<dyn TtsBackend>>> {
    Ok(HttpTts::from_env()?.map(|s| Box::new(s) as Box<dyn TtsBackend>))
}

#[cfg(not(feature = "audio-http"))]
#[allow(clippy::unnecessary_wraps)]
fn http_tts() -> RiverbaseResult<Option<Box<dyn TtsBackend>>> {
    Ok(None)
}

/// OpenAI-compatible HTTP STT (`POST {base}/audio/transcriptions`), the D8
/// sidecar for Python's faster-whisper.
#[cfg(feature = "audio-http")]
pub struct HttpStt {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    language: Option<String>,
}

#[cfg(feature = "audio-http")]
impl HttpStt {
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: "whisper-1".into(),
            language: None,
        }
    }

    #[must_use]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Python `AUDIO_STT_LANGUAGE` (empty: the sidecar detects it).
    #[must_use]
    pub fn with_language(mut self, language: Option<String>) -> Self {
        self.language = language.filter(|l| !l.is_empty());
        self
    }

    /// `AUDIO_STT_URL`, `AUDIO_STT_API_KEY`, `AUDIO_STT_MODEL` and
    /// `AUDIO_STT_LANGUAGE`; `None` without a URL.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let url = env("AUDIO_STT_URL")?;
        let mut stt = Self::new(url, env("AUDIO_STT_API_KEY").unwrap_or_default());
        if let Some(model) = env("AUDIO_STT_MODEL") {
            stt = stt.with_model(model);
        }
        Some(stt.with_language(env("AUDIO_STT_LANGUAGE")))
    }
}

#[cfg(feature = "audio-http")]
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
        let mut form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", self.model.clone());
        if let Some(language) = &self.language {
            form = form.text("language", language.clone());
        }
        let mut request = self.client.post(url).multipart(form);
        if !self.api_key.is_empty() {
            request = request.bearer_auth(&self.api_key);
        }
        let resp = request
            .send()
            .await
            .map_err(|e| crate::BEM_357.with_data(e.to_string()))?;
        let status = resp.status();
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| crate::BEM_358.with_data(e.to_string()))?;
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

/// OpenAI-compatible HTTP TTS (`POST {base}/audio/speech`, WAV out), the D8
/// sidecar for Python's gTTS and kokoro-onnx backends.
#[cfg(feature = "audio-http")]
pub struct HttpTts {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    voice: String,
    speed: f64,
}

#[cfg(feature = "audio-http")]
impl HttpTts {
    /// Python's defaults: voice `af_heart`, speed 1.1; model `kokoro`.
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: "kokoro".into(),
            voice: "af_heart".into(),
            speed: 1.1,
        }
    }

    #[must_use]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// Python `AUDIO_TTS_SPEAKER`.
    #[must_use]
    pub fn with_voice(mut self, voice: impl Into<String>) -> Self {
        self.voice = voice.into();
        self
    }

    /// Python `AUDIO_TTS_SPEED`.
    #[must_use]
    pub fn with_speed(mut self, speed: f64) -> Self {
        self.speed = speed;
        self
    }

    /// `AUDIO_TTS_URL`, `AUDIO_TTS_API_KEY`, `AUDIO_TTS_MODEL`,
    /// `AUDIO_TTS_SPEAKER` and `AUDIO_TTS_SPEED`; `None` without a URL.
    ///
    /// # Errors
    /// `BEM-503` when `AUDIO_TTS_SPEED` is not a number (Python fails the same way).
    pub fn from_env() -> RiverbaseResult<Option<Self>> {
        let Some(url) = env("AUDIO_TTS_URL") else {
            return Ok(None);
        };
        let mut tts = Self::new(url, env("AUDIO_TTS_API_KEY").unwrap_or_default());
        if let Some(model) = env("AUDIO_TTS_MODEL") {
            tts = tts.with_model(model);
        }
        if let Some(voice) = env("AUDIO_TTS_SPEAKER") {
            tts = tts.with_voice(voice);
        }
        if let Some(speed) = env("AUDIO_TTS_SPEED") {
            let speed = speed
                .trim()
                .parse()
                .map_err(|_| crate::BEM_503.with_data("AUDIO_TTS_SPEED is not a number"))?;
            tts = tts.with_speed(speed);
        }
        Ok(Some(tts))
    }
}

#[cfg(feature = "audio-http")]
#[async_trait]
impl TtsBackend for HttpTts {
    async fn synthesize(&self, text: &str) -> RiverbaseResult<Vec<u8>> {
        if self.base_url.is_empty() {
            return Err(crate::BEM_506.raise());
        }
        let body = serde_json::json!({
            "model": self.model,
            "input": text,
            "voice": self.voice,
            "speed": self.speed,
            "response_format": "wav",
        });
        let mut request = self
            .client
            .post(format!("{}/audio/speech", self.base_url))
            .json(&body);
        if !self.api_key.is_empty() {
            request = request.bearer_auth(&self.api_key);
        }
        let resp = request
            .send()
            .await
            .map_err(|e| crate::BEM_504.with_data(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(crate::BEM_505.with_data(format!("tts http {status}")));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| crate::BEM_504.with_data(e.to_string()))?;
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backend_keys_follow_python() {
        assert!(build_stt_backend("MOCK").is_ok());
        assert!(build_tts_backend("auto").is_ok());
        let err = build_stt_backend("whisperx").err().expect("unknown key");
        assert_eq!(err.errcode.as_str(), "BEM-503");
        assert!(build_tts_backend("nope").is_err());
    }

    #[tokio::test]
    async fn mock_tts_matches_python_length() {
        // Python: 0.6 s minimum at 22050 Hz -> 13230 samples, 16-bit mono.
        let wav = MockTts.synthesize("hi").await.unwrap();
        let (pcm, rate) = crate::format::parse_wav_pcm_f32(&wav).unwrap();
        assert_eq!(rate, 22_050);
        assert_eq!(pcm.len(), 13_230);
    }
}

#[cfg(all(test, feature = "audio-http"))]
mod http_tests {
    use super::*;

    #[tokio::test]
    async fn http_backends_require_base_url() {
        let err = HttpStt::new("", "k").transcribe(&[0.1]).await.unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-355");
        let err = HttpTts::new("", "k").synthesize("hi").await.unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-506");
    }
}
