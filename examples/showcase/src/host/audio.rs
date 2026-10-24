//! Realtime audio WebSocket (`/ws/audio`, feature `audio`): Python
//! `configure_audio_ws` on `beam_audio::AudioDriver`.
//!
//! The caller must be authenticated with a role that may `invoke`; otherwise
//! the handshake is refused with 403, as when Python closes the socket before
//! accepting it. Mount [`audio_ws_upgrade`] with an `Arc<AudioState>`.
//! `?agent=` (default `AUDIO_DEFAULT_AGENT_NAME`) and `?session_id=` (a UUID,
//! else a new one) pick the conversation. Each turn is the caller's `invoke`
//! command, waited for like the `/invoke` alias. Speech backends
//! come from `AUDIO_STT_BACKEND` and `AUDIO_TTS_BACKEND`.

use std::sync::{Arc, Mutex};

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use beam_audio::{
    AudioConfig, AudioDriver, AudioInvoker, AudioSession, AudioSink, Inbound, ServerMessage,
};
use futures_util::{SinkExt, StreamExt};
use riverbase_core::base::AggregateRoot;
use riverbase_core::{CommandTarget, EngineContext, RiverbaseResult};
use riverbase_http::Principal;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::host::http::engine_context;
use crate::host::runs::{RunEnd, RunWaiter};

/// What every audio connection shares (built once at startup).
pub struct AudioState {
    pub runs: Arc<RunWaiter>,
    pub stt: Arc<dyn beam_audio::SttBackend>,
    pub tts: Arc<dyn beam_audio::TtsBackend>,
    pub config: AudioConfig,
}

impl AudioState {
    /// The backends are built at startup, and an
    /// unknown key fails it.
    ///
    /// # Errors
    /// `BEM-503` for an unknown backend key or a sidecar key without its URL.
    pub fn from_env(runs: Arc<RunWaiter>) -> RiverbaseResult<Self> {
        let (stt, tts) = beam_audio::backends_from_env()?;
        Ok(Self {
            runs,
            stt,
            tts,
            config: AudioConfig::from_env(),
        })
    }
}

/// The reply's text.
fn reply_text(result: &Value) -> String {
    let texts: Vec<&str> = result
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|p| p.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|p| {
            p.get("text")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
        })
        .collect();
    if !texts.is_empty() {
        return texts.join(" ").trim().to_owned();
    }
    ["text_content", "message"]
        .iter()
        .find_map(|k| result.get(*k).and_then(Value::as_str))
        .unwrap_or_default()
        .chars()
        .take(8000)
        .collect()
}

/// The caller's `invoke` command, waited for; a
/// failure becomes the spoken reply.
struct DomainInvoker {
    runs: Arc<RunWaiter>,
    ctx: EngineContext,
    agent_name: String,
}

#[async_trait::async_trait]
impl AudioInvoker for DomainInvoker {
    async fn invoke(
        &self,
        session_id: &str,
        transcript: &str,
        inputs: &beam_audio::TurnInputs,
    ) -> RiverbaseResult<String> {
        let mut payload = json!({
            "session_id": session_id,
            "content": [{"type": "text", "text": transcript}],
        });
        // The facts and client capabilities the voice client sent.
        if let Some(context) = &inputs.context {
            payload["context"] = context.clone();
        }
        if let Some(client) = &inputs.client {
            payload["client"] = client.clone();
        }
        let target = CommandTarget::Object(AggregateRoot::new("agent", self.agent_name.clone()));
        let reply = match self.runs.run(&self.ctx, "invoke", payload, target).await {
            // A run held for an answer the screen must give (a client tool,
            // an approval): say so instead of an empty reply.
            Ok(RunEnd::Done(result))
                if result
                    .get("action_required")
                    .and_then(Value::as_array)
                    .is_some_and(|a| !a.is_empty()) =>
            {
                "The agent is waiting for your answer on screen.".into()
            }
            Ok(RunEnd::Done(result)) => reply_text(&result),
            Ok(RunEnd::Failed(frame)) => format!(
                "[Agent error] {}",
                frame
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("the run failed")
            ),
            Ok(RunEnd::Pending(_)) => "[Agent error] the agent did not answer in time".into(),
            Err(e) => {
                tracing::warn!(error = %e, "audio invoke failed");
                format!(
                    "[Agent error] {}",
                    beam_core::public_errors::stream_error_text(e)
                )
            }
        };
        Ok(if reply.is_empty() {
            "(empty agent reply)".into()
        } else {
            reply
        })
    }
}

/// Frames to the socket writer; `close` also ends the inbound side.
struct ChannelSink {
    tx: mpsc::UnboundedSender<Message>,
    closed: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait::async_trait]
impl AudioSink for ChannelSink {
    async fn send_text(&self, message: ServerMessage) {
        if let Ok(text) = serde_json::to_string(&message) {
            let _ = self.tx.send(Message::Text(text.into()));
        }
    }

    async fn send_bytes(&self, payload: Vec<u8>) {
        let _ = self.tx.send(Message::Binary(payload.into()));
    }

    async fn close(&self, code: u16, reason: &str) {
        let _ = self.tx.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.to_owned().into(),
        })));
        if let Some(closed) = self
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = closed.send(());
        }
    }
}

/// `?agent=` and `?session_id=`.
#[derive(Debug, Default, Deserialize)]
pub struct AudioQuery {
    pub agent: Option<String>,
    pub session_id: Option<String>,
}

/// `GET /ws/audio`: upgrade to the realtime audio socket.
#[allow(clippy::unused_async)] // An axum handler.
pub async fn audio_ws_upgrade(
    State(audio): State<Arc<AudioState>>,
    principal: Option<Extension<Principal>>,
    Query(query): Query<AudioQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    // riverbase's Casbin layer skips this public route: a turn is an
    // `invoke`, so the caller's roles must allow one.
    match principal {
        Some(Extension(principal)) => serve_audio(&audio, &engine_context(&principal), query, ws),
        None => (StatusCode::FORBIDDEN, "Unauthorized").into_response(),
    }
}

/// The audio socket for a caller already authenticated as `ctx`: 403 unless
/// its roles may `invoke`. For hosts that authenticate the upgrade their own
/// way (a browser cannot set headers on a WebSocket).
#[must_use]
pub fn serve_audio(
    audio: &Arc<AudioState>,
    ctx: &EngineContext,
    query: AudioQuery,
    ws: WebSocketUpgrade,
) -> Response {
    if !beam_domain::roles_may_run(&ctx.roles, "invoke") {
        return (StatusCode::FORBIDDEN, "Unauthorized").into_response();
    }
    let agent_name = query
        .agent
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| audio.config.default_agent.clone());
    let session_id = match query.session_id.as_deref().map(uuid::Uuid::parse_str) {
        Some(Ok(id)) => id,
        Some(Err(_)) => {
            tracing::warn!("audio-ws invalid session_id, using a new id");
            uuid::Uuid::new_v4()
        }
        None => uuid::Uuid::new_v4(),
    };
    let invoker = Arc::new(DomainInvoker {
        runs: audio.runs.clone(),
        ctx: ctx.clone(),
        agent_name: agent_name.clone(),
    });
    let (stt, tts, config) = (audio.stt.clone(), audio.tts.clone(), audio.config.clone());
    ws.on_upgrade(move |socket| async move {
        let session = AudioSession::new(agent_name, session_id.to_string());
        handle_socket(socket, |sink| {
            AudioDriver::new(config, session, stt, tts, invoker, sink)
        })
        .await;
    })
}

async fn handle_socket(socket: WebSocket, driver: impl FnOnce(Arc<dyn AudioSink>) -> AudioDriver) {
    let (mut ws_tx, ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let (closed_tx, closed_rx) = oneshot::channel();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let close = matches!(msg, Message::Close(_));
            if ws_tx.send(msg).await.is_err() || close {
                break;
            }
        }
    });
    let sink = Arc::new(ChannelSink {
        tx,
        closed: Mutex::new(Some(closed_tx)),
    });
    let inbound = ws_rx
        .take_while(|m| std::future::ready(matches!(m, Ok(m) if !matches!(m, Message::Close(_)))))
        .filter_map(|m| {
            std::future::ready(match m {
                Ok(Message::Binary(b)) => Some(Inbound::Binary(b.to_vec())),
                Ok(Message::Text(t)) => Some(Inbound::Text(t.to_string())),
                _ => None,
            })
        })
        .take_until(closed_rx);
    driver(sink).run(Box::pin(inbound)).await;
}

#[cfg(test)]
mod tests {
    use super::reply_text;
    use serde_json::json;

    #[test]
    fn reply_text_follows_python() {
        let parts = json!({"content": [{"type": "text", "text": "a"}, {"type": "image"}, {"type": "text", "text": "b"}]});
        assert_eq!(reply_text(&parts), "a b");
        assert_eq!(
            reply_text(&json!({"content": [], "text_content": "t"})),
            "t"
        );
        assert_eq!(reply_text(&json!({})), "");
    }
}
