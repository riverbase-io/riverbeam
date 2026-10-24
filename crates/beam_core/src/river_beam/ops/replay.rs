//! Stream authorization and replay (Python `stream/sse.py`).

use crate::river_beam::RiverBeam;
use beam_store::MessageRole;
use beam_stream::channel::{may_replay, may_subscribe, parse_beam_stream_channel, SessionOwner};
use beam_stream::replay::{
    assemble_replay, cursor_sequence, final_ai_message, sse_text, StoredEvent, REPLAY_LIMIT,
};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

impl RiverBeam {
    /// Subscribing
    /// to `ws.channel.{session}:{request}` needs the session's user or profile.
    ///
    /// # Errors
    /// Store errors.
    pub async fn authorize_stream_channel(
        &self,
        channel: &str,
        user_sub: Option<&str>,
        profile_id: Option<&str>,
    ) -> RiverbaseResult<bool> {
        let Some((session_id, _)) = parse_beam_stream_channel(channel) else {
            return Ok(false);
        };
        let session = self.store.get_session(session_id).await?;
        let owner = session.as_ref().map(|s| SessionOwner {
            user_id: s.user_id.as_deref(),
            profile_id: s.profile_id.as_deref(),
            org_id: s.org_id.as_deref(),
        });
        Ok(may_subscribe(owner, user_sub, profile_id))
    }

    /// The durable replay of `GET /api/beam/sse/{session}/{request}` (Python
    /// `stream/sse.py`), as `text/event-stream` text: `ready`, then the
    /// execution's state, final message and durable frames after
    /// `Last-Event-ID`, then `live` or `done`.
    ///
    /// # Errors
    /// `BEM-069` (public `BEM-411`) when the caller may not read the session;
    /// store errors.
    pub async fn replay_stream(
        &self,
        session_id: Uuid,
        request_id: &str,
        last_event_id: Option<&str>,
        claim_user: Option<&str>,
        claim_org: Option<&str>,
    ) -> RiverbaseResult<String> {
        self.replay(
            session_id,
            request_id,
            last_event_id,
            claim_user,
            claim_org,
            false,
        )
        .await
    }

    /// [`Self::replay_stream`] of this request's own run only: without an
    /// execution for `request_id` (the run is still queued), just `ready`,
    /// never the session's last run. For a client that follows the run live
    /// after its replay (`/astream`).
    ///
    /// # Errors
    /// As [`Self::replay_stream`].
    pub async fn replay_request(
        &self,
        session_id: Uuid,
        request_id: &str,
        last_event_id: Option<&str>,
        claim_user: Option<&str>,
        claim_org: Option<&str>,
    ) -> RiverbaseResult<String> {
        self.replay(
            session_id,
            request_id,
            last_event_id,
            claim_user,
            claim_org,
            true,
        )
        .await
    }

    pub(super) async fn replay(
        &self,
        session_id: Uuid,
        request_id: &str,
        last_event_id: Option<&str>,
        claim_user: Option<&str>,
        claim_org: Option<&str>,
        exact: bool,
    ) -> RiverbaseResult<String> {
        let session = self.store.get_session(session_id).await?;
        let owner = session.as_ref().map(|s| SessionOwner {
            user_id: s.user_id.as_deref(),
            profile_id: s.profile_id.as_deref(),
            org_id: s.org_id.as_deref(),
        });
        if !may_replay(owner, claim_user, claim_org) {
            return Err(crate::BEM_069.with_data("forbidden"));
        }
        let executions = self
            .executions
            .records()
            .list_for_session(session_id)
            .await?;
        let by_request = executions
            .iter()
            .find(|e| e.metadata.get("request_id").and_then(Value::as_str) == Some(request_id));
        let messages = self.store.list_messages(session_id).await?;
        let by_message = || {
            let message = messages
                .iter()
                .find(|m| m.request_id.is_some_and(|r| r.to_string() == request_id))?;
            executions
                .iter()
                .find(|e| Some(e.id) == message.execution_id)
        };
        // Python falls back to the session's last run; an exact replay does not.
        let execution =
            by_request
                .or_else(by_message)
                .or_else(|| if exact { None } else { executions.last() });
        let mut frames = vec![
            json!({"type": "ready", "session_id": session_id.to_string(), "request_id": request_id}),
        ];
        let mut complete = false;
        if let Some(execution) = execution {
            let looked_up = match last_event_id.and_then(|id| Uuid::parse_str(id).ok()) {
                Some(event_id) => self.ops.event_sequence(event_id).await.unwrap_or(None),
                None => None,
            };
            let after = cursor_sequence(last_event_id, |_| looked_up);
            let events: Vec<StoredEvent> = self
                .ops
                .events_after(
                    execution.id,
                    after,
                    i64::try_from(REPLAY_LIMIT).unwrap_or(i64::MAX),
                )
                .await?
                .into_iter()
                .map(|e| StoredEvent {
                    id: e.id.to_string(),
                    sequence: e.sequence,
                    event_type: e.event_type,
                    payload: e.payload,
                })
                .collect();
            let finals = messages
                .iter()
                .filter(|m| m.execution_id == Some(execution.id) && m.role == MessageRole::Ai)
                .map(|m| ("ai", &m.content, i64::from(m.sequence)));
            let last = final_ai_message(finals);
            let request = execution
                .metadata
                .get("request_id")
                .and_then(Value::as_str)
                .unwrap_or(request_id);
            let (replayed, terminal) = assemble_replay(
                &execution.id.to_string(),
                execution.status.as_str(),
                &events,
                last.as_ref(),
                after,
                Some(&session_id.to_string()),
                Some(request),
            );
            frames.extend(replayed);
            complete = terminal;
        }
        Ok(sse_text(&frames, complete))
    }

    // --- health ------------------------------------------------------------------
}
