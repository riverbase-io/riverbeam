//! Run frames on the caller's own riverbase RTC channel.
//!
//! A run streams on its own channel, `ws.channel.{session}:{request}`, which
//! a client cannot follow before the session exists. [`RtcMirrorTransport`]
//! also publishes each frame on the session owner's riverbase RTC channel,
//! `ws.channel.{org}.{sub}`, as a riverbase transport message. A client
//! subscribes there once (`/api/rtc/default~ssestream/{org}.{sub}`) before
//! it sends any command, so no frame of a new session's run is missed.
//!
//! Riverbase's own ACL lets any caller subscribe to any `ws.channel.*`: the
//! app must let a caller reach only their own channel (the showcase server's RTC gate).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use beam_store::ExecutionStore;
use beam_stream::StreamTransport;
use beam_types::StreamChunk;
use riverbase_core::command::MessageBus;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

/// The prefix of beam's run channels and of riverbase's RTC channels.
pub const CHANNEL_PREFIX: &str = "ws.channel.";

/// The `type` of a beam frame on an RTC channel.
pub const RTC_MESSAGE_TYPE: &str = "beam.stream";

/// Sessions whose owner channel is remembered; the cache starts over past it.
const OWNER_CACHE_LIMIT: usize = 10_000;

/// `BEAM_RTC_STREAM`: whether run frames also go to the caller's RTC channel
/// (on unless `0`, `false`, `no` or `off`).
#[must_use]
pub fn rtc_stream_enabled() -> bool {
    beam_env::flag("BEAM_RTC_STREAM", true)
}

/// The caller's own channel as riverbase's RTC bridge names it:
/// `ws.channel.{organization_id}.{sub}`, with `_` when there is no org.
#[must_use]
pub fn caller_channel(org_id: Option<&str>, subject: &str) -> String {
    let org = org_id
        .map(str::trim)
        .filter(|o| !o.is_empty())
        .unwrap_or("_");
    format!("{CHANNEL_PREFIX}{org}.{subject}")
}

/// The token subject of a session's owner: its profile (riverbase's real
/// auth makes the profile the subject), else its user (mock auth).
fn owner_subject(session: &beam_store::SessionRow) -> Option<&str> {
    [session.profile_id.as_deref(), session.user_id.as_deref()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|id| !id.is_empty())
}

/// A [`StreamTransport`] that publishes each run frame on `inner` and also,
/// as a riverbase transport message, on the session owner's RTC channel.
pub struct RtcMirrorTransport {
    inner: Arc<dyn StreamTransport>,
    bus: Arc<dyn MessageBus>,
    sessions: Arc<dyn ExecutionStore>,
    /// Session → its owner's channel (`None`: no user to send to).
    owners: Mutex<HashMap<Uuid, Option<String>>>,
}

impl RtcMirrorTransport {
    #[must_use]
    pub fn new(
        inner: Arc<dyn StreamTransport>,
        bus: Arc<dyn MessageBus>,
        sessions: Arc<dyn ExecutionStore>,
    ) -> Self {
        Self {
            inner,
            bus,
            sessions,
            owners: Mutex::new(HashMap::new()),
        }
    }

    /// The owner's channel of `session`, from the store once per session (a
    /// session's owner never changes; a session that is not there yet stays
    /// without one until the cache starts over). A store error is not
    /// remembered.
    async fn owner_channel(&self, session: Uuid) -> Option<String> {
        if let Some(known) = self
            .owners
            .lock()
            .ok()
            .and_then(|o| o.get(&session).cloned())
        {
            return known;
        }
        let row = match self.sessions.get_session(session).await {
            Ok(row) => row,
            Err(e) => {
                warn!(%session, error = %e, "could not read the session owner for its RTC channel");
                return None;
            }
        };
        let channel = row
            .as_ref()
            .and_then(|s| Some(caller_channel(s.org_id.as_deref(), owner_subject(s)?)));
        if let Ok(mut owners) = self.owners.lock() {
            if owners.len() >= OWNER_CACHE_LIMIT {
                owners.clear();
            }
            owners.insert(session, channel.clone());
        }
        channel
    }
}

/// `{session}:{request}` of a run channel, and its session id.
fn run_of(channel: &str) -> Option<(&str, Uuid)> {
    let key = channel.strip_prefix(CHANNEL_PREFIX)?;
    let (session, request) = key.split_once(':')?;
    if request.is_empty() {
        return None;
    }
    Some((key, Uuid::parse_str(session).ok()?))
}

/// The riverbase transport message carrying `frame` (its bridge sends
/// `txid`, `type`, `topic`, `payload` and `timestamp` to the client).
fn rtc_message(target: &str, topic: &str, frame: &StreamChunk) -> RiverbaseResult<Value> {
    let payload =
        serde_json::to_value(frame).map_err(|e| beam_core::BEM_103.with_data(e.to_string()))?;
    Ok(json!({
        "txid": frame.id.clone().unwrap_or_default(),
        "type": RTC_MESSAGE_TYPE,
        "topic": topic,
        "payload": payload,
        "channel": target,
    }))
}

#[async_trait]
impl StreamTransport for RtcMirrorTransport {
    async fn publish(&self, channel: &str, frame: &StreamChunk) -> RiverbaseResult<String> {
        let published = self.inner.publish(channel, frame).await?;
        let Some((topic, session)) = run_of(channel) else {
            return Ok(published);
        };
        let Some(target) = self.owner_channel(session).await else {
            return Ok(published);
        };
        // The run's own channel has the frame; the mirror is best effort.
        match rtc_message(&target, topic, frame) {
            Ok(message) => {
                if let Err(e) = self.bus.publish(&target, message).await {
                    warn!(
                        channel = %target,
                        error = %e,
                        "could not publish a run frame on the caller's RTC channel"
                    );
                }
            }
            Err(e) => {
                warn!(error = %e, "could not encode a run frame for the caller's RTC channel");
            }
        }
        Ok(published)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_store::{InMemoryStore, SessionRow};
    use beam_stream::InMemoryTransport;
    use tokio::sync::Mutex as AsyncMutex;

    #[derive(Default)]
    struct RecordingBus {
        sent: AsyncMutex<Vec<(String, Value)>>,
        down: bool,
    }

    #[async_trait]
    impl MessageBus for RecordingBus {
        async fn publish(&self, topic: &str, payload: Value) -> RiverbaseResult<()> {
            if self.down {
                return Err(beam_core::BEM_104.raise());
            }
            self.sent.lock().await.push((topic.to_owned(), payload));
            Ok(())
        }
    }

    fn session(user: Option<&str>, org: Option<&str>) -> SessionRow {
        SessionRow {
            id: Uuid::new_v4(),
            agent_id: Uuid::new_v4(),
            user_id: user.map(str::to_owned),
            profile_id: None,
            org_id: org.map(str::to_owned),
            name: None,
            last_active_at: None,
        }
    }

    #[test]
    fn the_callers_channel_follows_riverbases_acl() {
        assert_eq!(
            caller_channel(Some("org-1"), "user-1"),
            "ws.channel.org-1.user-1"
        );
        assert_eq!(caller_channel(None, "user-1"), "ws.channel._.user-1");
        assert_eq!(caller_channel(Some(" "), "user-1"), "ws.channel._.user-1");
        assert!(run_of("ws.channel.broadcast").is_none());
        assert!(run_of("ws.channel.not-a-uuid:r").is_none());
    }

    #[tokio::test]
    async fn run_frames_also_reach_the_owners_channel() {
        let store = Arc::new(InMemoryStore::new());
        let owned = session(Some("user-1"), Some("org-1"));
        let orphan = session(None, Some("org-1"));
        store.ensure_session(&owned).await.unwrap();
        store.ensure_session(&orphan).await.unwrap();
        let inner = Arc::new(InMemoryTransport::new());
        let bus = Arc::new(RecordingBus::default());
        let mirror = RtcMirrorTransport::new(inner.clone(), bus.clone(), store);

        let mut frame = StreamChunk::error_frame("boom");
        frame.id = Some("frame-1".into());
        let run = format!("ws.channel.{}:req-1", owned.id);
        mirror.publish(&run, &frame).await.unwrap();
        mirror.publish(&run, &frame).await.unwrap();
        // No user, or not a run channel: the run's channel only.
        mirror
            .publish(&format!("ws.channel.{}:req-2", orphan.id), &frame)
            .await
            .unwrap();
        mirror
            .publish("ws.channel.broadcast", &frame)
            .await
            .unwrap();

        assert_eq!(
            inner.frames().await.len(),
            4,
            "every frame keeps its own channel"
        );
        let sent = bus.sent.lock().await;
        assert_eq!(sent.len(), 2, "{sent:?}");
        let (target, message) = &sent[0];
        assert_eq!(target, "ws.channel.org-1.user-1");
        assert_eq!(message["type"], RTC_MESSAGE_TYPE);
        assert_eq!(message["topic"], format!("{}:req-1", owned.id));
        assert_eq!(message["txid"], "frame-1");
        assert_eq!(message["payload"]["type"], "error");
    }

    #[tokio::test]
    async fn a_profile_is_the_subject_and_a_bus_failure_spares_the_run() {
        let store = Arc::new(InMemoryStore::new());
        let mut profiled = session(Some("user-1"), Some("org-1"));
        profiled.profile_id = Some("profile-1".into());
        store.ensure_session(&profiled).await.unwrap();
        let inner = Arc::new(InMemoryTransport::new());
        let run = format!("ws.channel.{}:req-1", profiled.id);
        let frame = StreamChunk::error_frame("boom");

        let bus = Arc::new(RecordingBus::default());
        let mirror = RtcMirrorTransport::new(inner.clone(), bus.clone(), store.clone());
        mirror.publish(&run, &frame).await.unwrap();
        assert_eq!(bus.sent.lock().await[0].0, "ws.channel.org-1.profile-1");

        let down = Arc::new(RecordingBus {
            down: true,
            ..RecordingBus::default()
        });
        let mirror = RtcMirrorTransport::new(inner.clone(), down, store);
        mirror
            .publish(&run, &frame)
            .await
            .expect("the run's own channel still gets it");
        assert_eq!(inner.frames().await.len(), 2);
    }
}
