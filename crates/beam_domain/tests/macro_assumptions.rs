//! Plan 006 Phase 0: the riverbase behaviour the macro-mode domain relies
//! on. If one of these fails, later phases are built on a wrong assumption.
//! Needs `BEAM_DATABASE_URL`; skips without it.

mod support;

use std::time::Duration;

use beam_domain::{BeamSql, DomainStore};
use beam_store::tasks::TASK_TOPIC;
use futures_util::StreamExt;
use riverbase_core::transport::StreamBus;
use riverbase_core::Domain;
use serde_json::json;
use support::{ctx, object, store, uuid_of, world};

/// Point 1: a typed handler's `emit_message` reaches a bus subscriber once
/// the command commits (riverbase's outbox delivers it).
#[tokio::test]
async fn a_commands_hint_reaches_a_bus_subscriber() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let mut hints = w.bus.subscribe_values(TASK_TOPIC).await.expect("subscribe");
    let out = w
        .domain
        .command_dyn()
        .execute(
            &w.owner_ctx(),
            "stream",
            json!({"session_id": w.session, "content": [{"type": "text", "text": "hi"}]}),
            w.agent(),
        )
        .await
        .expect("stream");
    let task_id = uuid_of(&out, "task_id");
    let hint = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let hint = hints.next().await.expect("hint stream");
            if hint["task_id"] == json!(task_id) {
                return hint;
            }
        }
    })
    .await
    .expect("the hint arrives");
    assert_eq!(hint["task_id"], json!(task_id));
}

/// Point 2: object commands open their root without a `_tenant` column or
/// a UUID id: the root lookup passes the identifier through, and the
/// handler answers for a missing row (404, not riverbase's 500 `DAT-033`).
#[tokio::test]
async fn object_roots_need_no_tenant_column_or_uuid() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let command = w.domain.command_dyn();
    let err = command
        .execute(
            &w.owner_ctx(),
            "remove-message",
            json!({"message_id": "m-1"}),
            object("agent_session", "not-a-uuid"),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (err.http_status, err.errcode.as_str()),
        (404, "BEM-404"),
        "{err}"
    );
    let out = command
        .execute(
            &w.owner_ctx(),
            "update-session",
            json!({"name": "still mine"}),
            object("agent_session", w.session),
        )
        .await
        .expect("a session row with no _tenant opens");
    assert_eq!(out["name"], "still mine");
}

/// Point 2 (D7): riverbase refuses a command whose caller has no tenant,
/// before any handler runs.
#[tokio::test]
async fn commands_need_a_tenant() {
    let Some(store) = store("beam_domain").await else {
        return;
    };
    let w = world(&store).await;
    let mut no_tenant = ctx(w.owner.0, w.owner.1);
    no_tenant.actor.tenant = None;
    let err = w
        .domain
        .command_dyn()
        .execute(
            &no_tenant,
            "update-session",
            json!({"name": "x"}),
            object("agent_session", w.session),
        )
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "DOM-045", "{err}");
    // Nothing changed.
    let caller = beam_domain::Caller {
        user_id: Some(w.owner.0.to_string()),
        org_id: Some(w.owner.1.to_string()),
        admin: false,
    };
    let session = DomainStore::for_store(&store)
        .owned_session(&w.session.to_string(), &caller)
        .await
        .expect("session");
    assert_eq!(session["name"], "hello");
}
