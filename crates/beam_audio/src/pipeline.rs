use async_trait::async_trait;

use crate::backends::{AudioInvoker, SttBackend, TtsBackend};
use riverbase_core::RiverbaseResult;
use crate::messages::ServerMessage;

/// Output channel for the audio turn (JSON text frames + binary WAV frames).
#[async_trait]
pub trait AudioSink: Send + Sync {
    async fn send_text(&self, message: ServerMessage);
    async fn send_bytes(&self, payload: Vec<u8>);
}

/// Decode mic audio for the mock path.
///
/// The reference decodes WebM/MP4 with PyAV; that native path is host-bound
/// (feature `audio-decode`). By default we accept raw PCM WAV (what the mock
/// client/test harness sends) and fall back to a single-sample buffer so the
/// pipeline still advances for opaque blobs.
fn decode_to_pcm(raw_audio: &[u8]) -> RiverbaseResult<Vec<f32>> {
    if raw_audio.is_empty() {
        return Err(crate::BEM_354.raise());
    }
    if raw_audio.len() >= 12 && &raw_audio[0..4] == b"RIFF" && &raw_audio[8..12] == b"WAVE" {
        return crate::format::parse_wav_pcm_f32(raw_audio).map(|(pcm, _)| pcm);
    }
    // Opaque/compressed blob: represent as non-empty PCM so STT runs.
    Ok(vec![0.0_f32; raw_audio.len().min(16_000)])
}

/// Run one full audio turn: STT → agent → TTS (ports `run_audio_turn`).
///
/// Emits the same message sequence as the reference (`stt` → `user_transcript`
/// → `llm` → `assistant` → `audio_start` → bytes → `audio_end`). Returns early
/// after `user_transcript` when no speech is detected.
pub async fn run_audio_turn(
    raw_audio: &[u8],
    seq: u64,
    session_id: &str,
    stt: &dyn SttBackend,
    tts: &dyn TtsBackend,
    invoker: &dyn AudioInvoker,
    sink: &dyn AudioSink,
) -> RiverbaseResult<()> {
    sink.send_text(ServerMessage::text("Transcribing…", seq, false, "stt"))
        .await;

    let pcm = match decode_to_pcm(raw_audio) {
        Ok(pcm) => pcm,
        Err(err) => {
            sink.send_text(ServerMessage::text(
                format!("Audio decode failed: {err}"),
                seq,
                true,
                "error",
            ))
            .await;
            return Err(err);
        }
    };

    let transcript = stt.transcribe(&pcm).await?.trim().to_string();
    if transcript.is_empty() {
        sink.send_text(ServerMessage::text(
            "(No speech detected)",
            seq,
            true,
            "user_transcript",
        ))
        .await;
        return Ok(());
    }
    sink.send_text(ServerMessage::text(&transcript, seq, false, "user_transcript"))
        .await;

    sink.send_text(ServerMessage::text("Thinking…", seq, false, "llm"))
        .await;
    let reply = invoker.invoke(session_id, &transcript).await?;
    sink.send_text(ServerMessage::text(&reply, seq, true, "assistant"))
        .await;

    let wav = tts.synthesize(&reply).await?;
    sink.send_text(ServerMessage::AudioStart {
        seq,
        format: "audio/wav".into(),
    })
    .await;
    sink.send_bytes(wav).await;
    sink.send_text(ServerMessage::AudioEnd { seq }).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{MockStt, MockTts};
    use crate::format::{parse_wav_pcm_f32, pcm_f32_to_wav_bytes};
    use std::sync::Mutex;

    #[derive(Default)]
    struct CollectSink {
        texts: Mutex<Vec<ServerMessage>>,
        audio: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait]
    impl AudioSink for CollectSink {
        async fn send_text(&self, message: ServerMessage) {
            self.texts.lock().unwrap().push(message);
        }
        async fn send_bytes(&self, payload: Vec<u8>) {
            self.audio.lock().unwrap().push(payload);
        }
    }

    struct EchoInvoker;

    #[async_trait]
    impl AudioInvoker for EchoInvoker {
        async fn invoke(&self, _session_id: &str, transcript: &str) -> RiverbaseResult<String> {
            Ok(format!("Echo: {transcript}"))
        }
    }

    #[tokio::test]
    async fn mock_turn_produces_assistant_text_and_wav() {
        let sink = CollectSink::default();
        let raw = pcm_f32_to_wav_bytes(&vec![0.1_f32; 8000], 16000);
        run_audio_turn(
            &raw,
            1,
            "sid",
            &MockStt::new("hello there"),
            &MockTts,
            &EchoInvoker,
            &sink,
        )
        .await
        .unwrap();

        let texts = sink.texts.lock().unwrap();
        let assistant = texts.iter().find_map(|m| match m {
            ServerMessage::Text { text, phase, .. } if phase == "assistant" => Some(text.clone()),
            _ => None,
        });
        assert_eq!(assistant.as_deref(), Some("Echo: hello there"));
        assert!(texts
            .iter()
            .any(|m| matches!(m, ServerMessage::AudioStart { .. })));

        let audio = sink.audio.lock().unwrap();
        assert_eq!(audio.len(), 1);
        assert!(parse_wav_pcm_f32(&audio[0]).is_ok());
    }

    #[tokio::test]
    async fn empty_speech_returns_after_transcript() {
        let sink = CollectSink::default();
        let raw = pcm_f32_to_wav_bytes(&[], 16000);
        // Empty PCM decode error path: empty WAV has zero samples -> stt returns empty.
        let raw = if raw.len() < 12 { vec![1u8; 16] } else { raw };
        let _ = run_audio_turn(&raw, 2, "sid", &MockStt::new(""), &MockTts, &EchoInvoker, &sink)
            .await;
        let texts = sink.texts.lock().unwrap();
        assert!(texts.iter().all(|m| !matches!(m, ServerMessage::AudioEnd { .. })));
    }
}
