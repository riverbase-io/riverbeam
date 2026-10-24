//! The SQL transcript backends against Postgres.
//!
//! Set `BEAM_DATABASE_URL` (or `DATABASE_URL`), e.g. the parity Postgres:
//! `postgres://postgres@127.0.0.1:55432/postgres`. Without it the tests skip.

use std::sync::Arc;

use beam_memory::transcript::{sequence_of, LoadRange};
use beam_memory::{CachedTranscriptStore, SqlTranscriptStore, TranscriptStore};
use beam_store::{
    agent_id_for, AgentRow, BeamStore, CatalogStore, ExecutionStore, PgBeamStore, SessionRow,
};
use beam_types::{AgentMessage, AgentMessageRole, AgentMetadata};
use uuid::Uuid;

async fn store() -> Option<Arc<PgBeamStore>> {
    Some(Arc::new(beam_store::testdb::store("beam_memory").await?))
}

/// A checked-in agent and a session owned by `user` in `org`.
async fn session(store: &PgBeamStore, user: Uuid, org: Uuid) -> (Uuid, Uuid) {
    let name = format!("transcript-{}", Uuid::new_v4());
    let metadata: AgentMetadata =
        serde_json::from_value(serde_json::json!({"name": name})).expect("metadata");
    store
        .checkin_agents(&[AgentRow::active(&metadata)])
        .await
        .expect("agent");
    let agent = agent_id_for(&name);
    let session = Uuid::new_v4();
    store
        .ensure_session(&SessionRow {
            id: session,
            agent_id: agent,
            user_id: Some(user.to_string()),
            profile_id: None,
            org_id: Some(org.to_string()),
            name: None,
            last_active_at: None,
        })
        .await
        .expect("session");
    (agent, session)
}

fn text(messages: &[AgentMessage]) -> Vec<String> {
    messages.iter().map(AgentMessage::text_content).collect()
}

#[tokio::test]
async fn sql_transcript_roundtrip() {
    let Some(store) = store().await else { return };
    let (user, org) = (Uuid::new_v4(), Uuid::new_v4());
    let (agent, session) = session(&store, user, org).await;
    let transcript = SqlTranscriptStore::new(store.clone() as Arc<dyn BeamStore>);
    let mut tool = AgentMessage::text(AgentMessageRole::Tool, "42");
    tool.tool_call_id = Some("call-1".into());
    let mut ai = AgentMessage::text(AgentMessageRole::Ai, "");
    ai.tool_calls = vec![serde_json::json!({"id": "call-1", "name": "calc", "args": {}})];
    let messages = vec![
        AgentMessage::text(AgentMessageRole::Human, "what is 6*7?"),
        ai,
        tool,
    ];
    transcript
        .save(agent, session, &messages, Some(&user.to_string()))
        .await
        .expect("save");
    transcript
        .save(
            agent,
            session,
            &[AgentMessage::text(AgentMessageRole::Ai, "42.")],
            None,
        )
        .await
        .expect("save");

    // Rows carry Python's columns: dense sequences, the session's org, the type.
    let rows = store.list_messages(session).await.expect("rows");
    assert_eq!(
        rows.iter().map(|r| r.sequence).collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert!(rows
        .iter()
        .all(|r| r.org_id.as_deref() == Some(org.to_string().as_str())));
    assert_eq!(rows[2].msg_type, beam_store::MessageType::ToolResult);
    assert_eq!(rows[1].tool_calls[0]["id"], "call-1");

    let all = transcript
        .load(session, LoadRange::default())
        .await
        .expect("load");
    assert_eq!(text(&all), ["what is 6*7?", "", "42", "42."]);
    assert_eq!(all[0].id, messages[0].id);
    let page = transcript
        .load(
            session,
            LoadRange {
                limit: Some(2),
                after: Some(0),
                ..LoadRange::default()
            },
        )
        .await
        .expect("page");
    assert_eq!(page.iter().map(sequence_of).collect::<Vec<_>>(), [2, 3]);
    assert_eq!(transcript.sequence_length(session).await.expect("len"), 4);
    assert_eq!(
        transcript
            .list_sessions(&user.to_string())
            .await
            .expect("sessions"),
        [session.to_string()]
    );
    transcript.delete(session).await.expect("delete");
    assert_eq!(transcript.sequence_length(session).await.expect("len"), 0);
    // Leave one turn behind for `scripts/cross_read.py` to load with Python.
    transcript
        .save(agent, session, &messages, None)
        .await
        .expect("save");
}

#[tokio::test]
async fn sql_cached_consistency() {
    let Some(store) = store().await else { return };
    let (agent, session) = session(&store, Uuid::new_v4(), Uuid::new_v4()).await;
    let canonical: Arc<dyn TranscriptStore> =
        Arc::new(SqlTranscriptStore::new(store.clone() as Arc<dyn BeamStore>));
    let cached = CachedTranscriptStore::new(canonical.clone());
    cached
        .save(
            agent,
            session,
            &[AgentMessage::text(AgentMessageRole::Human, "one")],
            None,
        )
        .await
        .expect("save");
    assert_eq!(
        text(
            &cached
                .load(session, LoadRange::default())
                .await
                .expect("load")
        ),
        ["one"]
    );
    // Another writer (another worker) appends straight to SQL: the cache must
    // not serve the stale page.
    canonical
        .save(
            agent,
            session,
            &[AgentMessage::text(AgentMessageRole::Ai, "two")],
            None,
        )
        .await
        .expect("save");
    assert_eq!(
        text(
            &cached
                .load(session, LoadRange::default())
                .await
                .expect("load")
        ),
        ["one", "two"]
    );
    cached.delete(session).await.expect("delete");
    assert!(cached
        .load(session, LoadRange::default())
        .await
        .expect("load")
        .is_empty());
}

/// Concurrent writers to one session get dense, unique sequences, and each
/// turn's messages stay together.
#[tokio::test]
async fn concurrent_turns_get_dense_sequences() {
    let Some(store) = store().await else { return };
    let (agent, session) = session(&store, Uuid::new_v4(), Uuid::new_v4()).await;
    let transcript = Arc::new(SqlTranscriptStore::new(store.clone() as Arc<dyn BeamStore>));
    let writers = (0..4).map(|w| {
        let transcript = transcript.clone();
        async move {
            let turn: Vec<AgentMessage> = (0..3)
                .map(|i| AgentMessage::text(AgentMessageRole::Human, format!("w{w}-{i}")))
                .collect();
            transcript.save(agent, session, &turn, None).await
        }
    });
    for result in futures_util::future::join_all(writers).await {
        result.expect("save");
    }
    let rows = store.list_messages(session).await.expect("rows");
    assert_eq!(
        rows.iter().map(|r| r.sequence).collect::<Vec<_>>(),
        (0..12).collect::<Vec<_>>()
    );
    for turn in rows.chunks(3) {
        let writer: Vec<String> = turn
            .iter()
            .map(|r| r.content[0]["text"].as_str().unwrap()[..2].to_owned())
            .collect();
        assert!(writer.iter().all(|w| *w == writer[0]), "{writer:?}");
    }
}
