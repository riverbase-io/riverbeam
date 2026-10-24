//! Run an agent through the domain and wait for it to end.
//!
//! Domain commands only queue a run. The `/invoke` alias and the audio
//! socket still answer with the result, so they wait here, outside the
//! command engine: subscribe to the run's live channel first, then run the
//! command (so no frame is missed), and stop at the first terminal frame
//! (`done`, which also covers interrupted and cancelled runs, or `error`),
//! or after `BEAM_INVOKE_WAIT_SECONDS` (default 300).
//!
//! A repeated idempotency key answers with the first run, which may have
//! finished already: its terminal frame is then read from the durable
//! replay.

use std::sync::Arc;
use std::time::Duration;

use beam_core::RiverBeam;
use beam_domain::BeamDomainBundle;
use beam_stream::{BeamStream, NatsTransport};
use beam_types::{StreamChunk, StreamChunkType};
use futures_util::StreamExt;
use riverbase_core::{CommandTarget, Domain, EngineContext, RiverbaseResult};
use serde_json::{json, Value};
use uuid::Uuid;

/// How a waited run ended.
#[derive(Debug)]
pub enum RunEnd {
    /// The run's result (`AgentResult` JSON).
    Done(Value),
    /// The run's `error` frame.
    Failed(Value),
    /// Still running when the wait ended: the command's acknowledgement.
    Pending(Value),
}

/// Runs agent commands and waits for them.
pub struct RunWaiter {
    domain: Arc<BeamDomainBundle>,
    transport: Arc<NatsTransport>,
    beam: Arc<RiverBeam>,
    wait: Duration,
}

/// `BEAM_INVOKE_WAIT_SECONDS`, default 300.
#[must_use]
pub fn invoke_wait() -> Duration {
    let secs = std::env::var("BEAM_INVOKE_WAIT_SECONDS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300);
    Duration::from_secs(secs.max(1))
}

/// The run's live channel. Ids are written as the worker writes them
/// (canonical UUID text), whatever case the client used.
fn channel(session: &str, request: &str) -> String {
    let canonical =
        |id: &str| Uuid::parse_str(id.trim()).map_or_else(|_| id.to_owned(), |u| u.to_string());
    format!(
        "ws.channel.{}",
        BeamStream::stream_key(&canonical(session), &canonical(request))
    )
}

fn end_of(frame: &StreamChunk) -> Option<RunEnd> {
    match frame.chunk_type {
        StreamChunkType::Done => Some(RunEnd::Done(
            frame
                .result
                .as_ref()
                .and_then(|r| serde_json::to_value(r).ok())
                .unwrap_or(Value::Null),
        )),
        StreamChunkType::Error => Some(RunEnd::Failed(
            serde_json::to_value(frame).unwrap_or_else(|_| json!({"type": "error"})),
        )),
        StreamChunkType::Chunk => None,
    }
}

/// The last terminal frame in a replay's SSE text.
fn terminal_in_replay(text: &str) -> Option<RunEnd> {
    text.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<StreamChunk>(data.trim()).ok())
        .filter_map(|frame| end_of(&frame))
        .next_back()
}

impl RunWaiter {
    #[must_use]
    pub fn new(
        domain: Arc<BeamDomainBundle>,
        transport: Arc<NatsTransport>,
        beam: Arc<RiverBeam>,
    ) -> Self {
        Self {
            domain,
            transport,
            beam,
            wait: invoke_wait(),
        }
    }

    /// Wait at most `wait` for a run's end instead of
    /// `BEAM_INVOKE_WAIT_SECONDS`.
    #[must_use]
    pub fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// Run command `key` (`invoke` or `stream`) for `ctx` on `target` with
    /// `payload`, and wait for the run to end.
    ///
    /// # Errors
    /// The command's errors (ownership, validation, unknown agent), or the
    /// stream subscription's.
    pub async fn run(
        &self,
        ctx: &EngineContext,
        key: &str,
        mut payload: Value,
        target: CommandTarget,
    ) -> RiverbaseResult<RunEnd> {
        let session = payload
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let Some(session) = session else {
            // The command reports the missing session.
            let ack = self
                .domain
                .command_dyn()
                .execute(ctx, key, payload, target)
                .await?;
            return Ok(RunEnd::Pending(ack));
        };
        let request = payload
            .get("request_id")
            .and_then(Value::as_str)
            .map_or_else(|| Uuid::new_v4().to_string(), str::to_owned);
        payload["request_id"] = json!(request);
        // Subscribe before the run is queued: no frame can be missed.
        let mut frames = self
            .transport
            .subscribe(&channel(&session, &request))
            .await?;
        let ack = self
            .domain
            .command_dyn()
            .execute(ctx, key, payload, target)
            .await?;
        if ack["status"] == "existing" {
            let first = ack["request_id"].as_str().unwrap_or_default().to_owned();
            frames = self.transport.subscribe(&channel(&session, &first)).await?;
            if let Some(end) = self.replayed_end(ctx, &session, &first).await {
                return Ok(end);
            }
        }
        let waited = tokio::time::timeout(self.wait, async {
            while let Some(message) = frames.next().await {
                let Ok(frame) = serde_json::from_slice::<StreamChunk>(&message.payload) else {
                    continue;
                };
                if let Some(end) = end_of(&frame) {
                    return Some(end);
                }
            }
            None
        })
        .await;
        Ok(if let Ok(Some(end)) = waited {
            end
        } else {
            let mut ack = ack;
            ack["status"] = json!("running");
            RunEnd::Pending(ack)
        })
    }

    async fn replayed_end(
        &self,
        ctx: &EngineContext,
        session: &str,
        request: &str,
    ) -> Option<RunEnd> {
        let session = Uuid::parse_str(session).ok()?;
        let user = ctx.actor.user_id.map(|u| u.to_string());
        let org = ctx.organization_id.map(|o| o.to_string());
        let text = self
            .beam
            .replay_request(session, request, None, user.as_deref(), org.as_deref())
            .await
            .ok()?;
        terminal_in_replay(&text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_terminal_frame_of_a_replay_ends_the_run() {
        let text = "id: 1\nevent: started\ndata: {\"type\": \"chunk\"}\n\n\
                    id: 2\nevent: done\ndata: {\"type\": \"done\", \"result\": null}\n\n";
        assert!(matches!(terminal_in_replay(text), Some(RunEnd::Done(_))));
        assert!(terminal_in_replay("data: {\"type\": \"chunk\"}\n").is_none());
        let failed = "data: {\"type\": \"error\", \"error\": \"boom\"}\n";
        assert!(
            matches!(terminal_in_replay(failed), Some(RunEnd::Failed(v)) if v["error"] == "boom")
        );
    }
}
