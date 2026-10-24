//! Rows for store tests, shared by the in-memory and Postgres suites.

use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use crate::rows::{
    ActionRow, ActionStatus, ActionType, AgentRow, AgentStatus, InteractionRow, InteractionStatus,
    MessageRole, MessageRow, MessageType, ModelRow, ModelStatus, RunRow, RunStatus, SessionRow,
    StepRow, StepStatus,
};

/// Fresh ids for one execution tree.
pub struct Ids {
    pub agent: Uuid,
    pub agent_name: String,
    pub session: Uuid,
    pub interaction: Uuid,
    pub run: Uuid,
    pub user: Uuid,
    pub org: Uuid,
}

impl Ids {
    pub fn new() -> Self {
        let agent_name = format!("demo-{}", Uuid::new_v4());
        Self {
            agent: crate::agent_id_for(&agent_name),
            agent_name,
            session: Uuid::new_v4(),
            interaction: Uuid::new_v4(),
            run: Uuid::new_v4(),
            user: Uuid::new_v4(),
            org: Uuid::new_v4(),
        }
    }
}

pub fn agent(ids: &Ids) -> AgentRow {
    AgentRow {
        id: ids.agent,
        name: ids.agent_name.clone(),
        description: Some("demo".into()),
        supported_modalities: vec!["text".into()],
        output_modalities: vec!["text".into()],
        tags: vec![],
        version: Some("1.0.0".into()),
        status: AgentStatus::Active,
    }
}

pub fn model() -> ModelRow {
    ModelRow {
        id: crate::agent_id_for("gpt-test"),
        name: "gpt-test".into(),
        provider: "openai".into(),
        modalities: vec!["text".into()],
        capabilities: vec![],
        cost_per_1k_tokens: 0.15,
        max_tokens: 4096,
        provider_config: json!({}),
        extra: json!({}),
        status: ModelStatus::Active,
    }
}

pub fn session(ids: &Ids) -> SessionRow {
    SessionRow {
        id: ids.session,
        agent_id: ids.agent,
        user_id: Some(ids.user.to_string()),
        profile_id: None,
        org_id: Some(ids.org.to_string()),
        name: Some("sess".into()),
        last_active_at: None,
    }
}

pub fn interaction(ids: &Ids) -> InteractionRow {
    InteractionRow {
        id: ids.interaction,
        session_id: ids.session,
        execution_id: None,
        agent_id: ids.agent,
        agent_name: ids.agent_name.clone(),
        status: InteractionStatus::InProgress,
        started_at: Utc::now(),
        ended_at: None,
    }
}

pub fn run(ids: &Ids) -> RunRow {
    RunRow {
        id: ids.run,
        session_id: ids.session,
        interaction_id: ids.interaction,
        execution_id: None,
        attempt_no: 1,
        agent_id: ids.agent,
        agent_name: ids.agent_name.clone(),
        status: RunStatus::Running,
        started_at: Utc::now(),
        ended_at: None,
        org_id: Some(ids.org.to_string()),
    }
}

pub fn step(ids: &Ids) -> StepRow {
    StepRow {
        id: Uuid::new_v4(),
        session_id: ids.session,
        interaction_id: ids.interaction,
        run_id: ids.run,
        agent_id: ids.agent,
        status: StepStatus::Running,
        lc_step_id: Some("step-1".into()),
        name: "agent".into(),
        input: json!({"q": "2+2"}),
        result: None,
    }
}

pub fn message(ids: &Ids, sequence: i32) -> MessageRow {
    let mut m = MessageRow::new(
        ids.session,
        ids.agent,
        sequence,
        MessageRole::Human,
        MessageType::Message,
        json!([{"type": "text", "text": "hello"}]),
    );
    m.interaction_id = Some(ids.interaction);
    m.run_id = Some(ids.run);
    m.org_id = Some(ids.org.to_string());
    m
}

pub fn action(ids: &Ids) -> ActionRow {
    ActionRow {
        id: Uuid::new_v4(),
        session_id: ids.session,
        execution_id: None,
        interaction_id: ids.interaction,
        run_id: ids.run,
        step_id: None,
        message_id: None,
        agent_id: ids.agent,
        action_type: ActionType::Confirmation,
        status: ActionStatus::Pending,
        lc_action_id: Some("lc-1".into()),
        payload: json!({"question": "Run?"}),
        result: None,
        resolved_at: None,
        org_id: Some(ids.org.to_string()),
        tool_name: None,
        expires_at: None,
        sweep_after: None,
    }
}

/// A pending client-tool action for `ids`, expiring at `expires_at`.
pub fn client_tool_action(ids: &Ids, expires_at: chrono::DateTime<Utc>) -> ActionRow {
    ActionRow {
        action_type: ActionType::ClientTool,
        tool_name: Some("request_input".into()),
        expires_at: Some(expires_at),
        ..action(ids)
    }
}

/// Due client-tool actions: past `expires_at`, and past `sweep_after` when
/// set; a claim holds them back until its `until`. Seeds `ids`' session,
/// interaction and run first, and resolves its rows at the end, so a shared
/// test database does not fill up with due rows.
pub async fn check_due_client_tools<
    S: crate::store::CatalogStore + crate::store::ExecutionStore,
>(
    store: &S,
) {
    let ids = Ids::new();
    store.checkin_agents(&[agent(&ids)]).await.unwrap();
    store.ensure_session(&session(&ids)).await.unwrap();
    store.create_interaction(&interaction(&ids)).await.unwrap();
    store.create_run(&run(&ids)).await.unwrap();
    let now = Utc::now();
    let minutes = chrono::TimeDelta::minutes;
    let due = client_tool_action(&ids, now - minutes(5));
    let later = client_tool_action(&ids, now + minutes(5));
    let held_back = ActionRow {
        sweep_after: Some(now + minutes(5)),
        ..client_tool_action(&ids, now - minutes(9))
    };
    let approval = ActionRow {
        expires_at: Some(now - minutes(5)),
        ..action(&ids)
    };
    let rows = [&due, &later, &held_back, &approval];
    for row in rows {
        store.create_action((*row).clone()).await.unwrap();
    }
    let mine = |rows: Vec<ActionRow>| {
        rows.into_iter()
            .filter(|r| r.session_id == ids.session)
            .map(|r| r.id)
            .collect::<Vec<_>>()
    };
    let until = now + minutes(5);
    let claimed = store
        .claim_due_client_tool_actions(now, until, 1000)
        .await
        .unwrap();
    assert_eq!(mine(claimed), [due.id]);
    let stored = store.get_action(due.id).await.unwrap();
    assert_eq!(stored.tool_name.as_deref(), Some("request_input"));
    assert_eq!(stored.action_type, ActionType::ClientTool);
    assert_eq!(
        stored.sweep_after.map(|t| t.timestamp()),
        Some(until.timestamp())
    );
    // A second sweep before `until` claims nothing of these.
    assert!(mine(
        store
            .claim_due_client_tool_actions(now, until, 1000)
            .await
            .unwrap()
    )
    .is_empty());
    // Past `until`, everything due is claimed again, oldest first.
    let after = now + minutes(6);
    let claimed = store
        .claim_due_client_tool_actions(after, after + minutes(5), 1000)
        .await
        .unwrap();
    assert_eq!(mine(claimed), [held_back.id, due.id, later.id]);
    let mut pending: Vec<_> = store
        .list_pending_actions(ids.interaction)
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.id)
        .collect();
    pending.sort();
    let mut all: Vec<_> = rows.iter().map(|r| r.id).collect();
    all.sort();
    assert_eq!(pending, all);
    for row in rows {
        store.resolve_action(row.id, json!({})).await.unwrap();
    }
    assert!(store
        .list_pending_actions(ids.interaction)
        .await
        .unwrap()
        .is_empty());
}
