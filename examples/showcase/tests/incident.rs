//! The incident desk on an in-memory beam with a scripted model: Kai opens an
//! incident from the checkout alert and investigates; the rollback waits for
//! approval and then marks the incident mitigated; Lin resolves it and has the
//! postmortem committed to a git sandbox. Only the two personas work incidents,
//! one incident is open per service, and only the commander writes the
//! postmortem.

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_knowledge::{HashEmbedder, InMemoryVectorStore, KnowledgeManager};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{ToolCallContext, ToolRegistry, ToolRuntime, ToolService, KNOWLEDGE_SEARCH};
use beam_types::{
    ActionResponseEnvelope, ContentPart, InvokeAuthContext, InvokeRequest, RequestContext,
    RequestFact,
};
use serde_json::{json, Value};
use showcase::incident::agents::INCIDENT_RESPONDER;
use showcase::incident::personas::{KAI, LIN, ORG};
use showcase::incident::postmortem::Postmortems;
use showcase::incident::tools::{
    incident_tools, ADD_TIMELINE_ENTRY, GET_INCIDENT, OPEN_INCIDENT, RESOLVE_INCIDENT,
    RESTART_SERVICE, ROLLBACK_DEPLOY, WRITE_POSTMORTEM,
};
use showcase::incident::{bind_agents, INCIDENT, SHOWCASE};
use showcase::records::{InMemoryRecords, RecordStore};
use uuid::Uuid;

struct Desk {
    beam: RiverBeam,
    incidents: Arc<dyn RecordStore>,
    postmortems: Arc<Postmortems>,
    _sandbox: tempfile::TempDir,
}

async fn desk(turns: Vec<ScriptedTurn>) -> Desk {
    let sandbox = tempfile::tempdir().unwrap();
    let postmortems = Postmortems::open(sandbox.path()).unwrap();
    let incidents = InMemoryRecords::shared();
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(ScriptedModel::new(turns)))
        .knowledge(Arc::new(KnowledgeManager::new(
            Arc::new(HashEmbedder::new(64)),
            Arc::new(InMemoryVectorStore::new()),
        )))
        .tools(Arc::new(ToolService::new(
            Arc::new(ToolRegistry::new()),
            Arc::new(ToolRuntime::default()),
        )))
        .build()
        .unwrap();
    let owned = bind_agents(&beam, &incidents, Some(postmortems.clone()), &[]).await.unwrap();
    assert!(owned.iter().any(|t| t == ROLLBACK_DEPLOY));
    assert!(owned.iter().any(|t| t == WRITE_POSTMORTEM));
    assert!(!owned.iter().any(|t| t == KNOWLEDGE_SEARCH), "shared tools are not owned");
    beam.checkin_catalog().await.unwrap();
    Desk { beam, incidents, postmortems, _sandbox: sandbox }
}

fn turn(user: &str, incident: Option<Uuid>, text: &str) -> InvokeRequest {
    InvokeRequest {
        agent_name: INCIDENT_RESPONDER.into(),
        content: vec![ContentPart::text(text)],
        session_id: Uuid::new_v4(),
        model_override: None,
        metadata: json!({}),
        interaction_id: Some(Uuid::new_v4()),
        execution_id: None,
        request_id: Some(Uuid::new_v4()),
        idempotency_key: None,
        stream: false,
        actions: None,
        auth_context: Some(InvokeAuthContext {
            profile_id: None,
            user_id: Some(user.into()),
            org_id: Some(ORG.into()),
            mcp_token: None,
        }),
        context: incident.map(|id| RequestContext {
            facts: vec![RequestFact {
                key: "incident_id".into(),
                value: json!(id.to_string()),
                expiry: None,
                sensitivity: beam_types::Sensitivity::Internal,
            }],
            ..RequestContext::default()
        }),
        client: None,
    }
}

/// Approve every held action of `result`.
fn approve_all(result: &beam_types::AgentResult) -> Vec<ActionResponseEnvelope> {
    result
        .action_required
        .as_ref()
        .unwrap()
        .iter()
        .map(|action| {
            let request = &action["action_request"];
            serde_json::from_value(json!({
                "action_id": action["action_id"],
                "lc_action_id": action["lc_action_id"],
                "kind": request["kind"],
                "request_id": request["request_id"],
                "status": "submitted",
                "payload": {"approved": true},
            }))
            .unwrap()
        })
        .collect()
}

#[tokio::test]
async fn the_checkout_alert_becomes_an_incident_a_held_rollback_and_a_committed_postmortem() {
    let desk = desk(vec![
        // Kai opens the incident from the alert.
        ScriptedTurn::tool_call(
            "c1",
            OPEN_INCIDENT,
            json!({"service": "checkout-api", "severity": "critical",
                "summary": "checkout-api: p99 latency 2.8s (SLO 800ms) and 5xx rate 6.2% since 14:05 UTC"}),
        ),
        ScriptedTurn::text("Opened the incident."),
        // Kai investigates, then asks for the rollback, which waits for his approval.
        ScriptedTurn::tool_call("c2", GET_INCIDENT, json!({})),
        ScriptedTurn::tool_call(
            "c3",
            ADD_TIMELINE_ENTRY,
            json!({"entry": "v2.14.0 deployed at 14:02 UTC; p99 and 5xx jumped at 14:05 UTC."}),
        ),
        ScriptedTurn::tool_call(
            "c4",
            ROLLBACK_DEPLOY,
            json!({"service": "checkout-api", "to_version": "v2.13.4", "reason": "latency and 5xx started right after v2.14.0"}),
        ),
        ScriptedTurn::text("Rolled back to v2.13.4."),
        // Lin resolves it and has the postmortem written.
        ScriptedTurn::tool_call(
            "c5",
            RESOLVE_INCIDENT,
            json!({"summary": "Connection pool v3 in v2.14.0 exhausted connections; rolled back to v2.13.4."}),
        ),
        ScriptedTurn::tool_call(
            "c6",
            WRITE_POSTMORTEM,
            json!({"summary": "Checkout was slow for 25 minutes after v2.14.0.",
                "impact": "5xx rate reached 6.2% and p99 latency 2.8s.",
                "root_cause": "Connection pool v3 exhausted its connections under load.",
                "what_went_well": "The rollback took five minutes.",
                "action_items": ["Load test the payment client before deploys."]}),
        ),
        ScriptedTurn::text("The postmortem is committed."),
    ])
    .await;

    // Open.
    let done = desk.beam.invoke_result(turn(KAI, None, "Open an incident for checkout-api.")).await.unwrap();
    assert_eq!(done.text_content(), "Opened the incident.");
    let all = desk.incidents.list(SHOWCASE, INCIDENT, None).await.unwrap();
    assert_eq!(all.len(), 1);
    let id = all[0].id;
    assert_eq!(all[0].status, "open");
    assert_eq!(all[0].text("service"), Some("checkout-api"));
    assert_eq!(all[0].data["timeline"][0]["who"], "Kai Sato");

    // Investigate, then the rollback is held.
    let work = turn(KAI, Some(id), "Roll back the bad deploy.");
    let held = desk.beam.invoke_result(work.clone()).await.unwrap();
    assert!(held.interrupted, "the rollback waits: {:?}", held.content);
    let action = &held.action_required.as_ref().unwrap()[0];
    assert_eq!(action["action_request"]["kind"], "beam.tool_approval");
    assert_eq!(action["action_request"]["payload"]["risk"], "high");
    let investigating = desk.incidents.get(id).await.unwrap().unwrap();
    assert_eq!(investigating.status, "investigating", "no mitigation before approval");
    assert_eq!(investigating.data["timeline"].as_array().unwrap().len(), 2);
    assert!(investigating.data["remediation"].is_null());

    // Approved: mitigated, with the action in the timeline.
    let mut approve = work.clone();
    approve.request_id = Some(Uuid::new_v4());
    approve.content = Vec::new();
    approve.actions = Some(approve_all(&held));
    let done = desk.beam.invoke_result(approve).await.unwrap();
    assert_eq!(done.text_content(), "Rolled back to v2.13.4.");
    let mitigated = desk.incidents.get(id).await.unwrap().unwrap();
    assert_eq!(mitigated.status, "mitigated");
    assert_eq!(mitigated.data["remediation"]["action"], "rollback_deploy");
    assert_eq!(mitigated.data["remediation"]["to_version"], "v2.13.4");
    assert_eq!(mitigated.data["remediation"]["simulated"], true);
    let timeline = mitigated.data["timeline"].as_array().unwrap();
    assert_eq!(timeline.len(), 3);
    assert!(timeline[2]["entry"].as_str().unwrap().contains("back to v2.13.4"));

    // The commander resolves and the postmortem is committed.
    let done = desk
        .beam
        .invoke_result(turn(LIN, Some(id), "Resolve it and write the postmortem."))
        .await
        .unwrap();
    assert_eq!(done.text_content(), "The postmortem is committed.");
    let resolved = desk.incidents.get(id).await.unwrap().unwrap();
    assert_eq!(resolved.status, "resolved");
    let path = resolved.data["postmortem"]["path"].as_str().unwrap().to_owned();
    assert!(path.starts_with("postmortems/") && path.ends_with("-checkout-api.md"), "{path}");
    assert_eq!(resolved.data["postmortem"]["commit"].as_str().unwrap().len(), 40);
    let view = desk.postmortems.read(Some(&path), id).await.unwrap();
    let body = view["files"][0]["body"].as_str().unwrap();
    assert!(body.contains("## Root cause") && body.contains("Connection pool v3 exhausted"), "{body}");
    assert!(body.contains("back to v2.13.4"), "the timeline is copied in: {body}");
    assert_eq!(view["log"].as_array().unwrap().len(), 1);
}

fn ctx(user: &str) -> ToolCallContext {
    ToolCallContext {
        auth: Some(InvokeAuthContext {
            profile_id: None,
            user_id: Some(user.into()),
            org_id: Some(ORG.into()),
            mcp_token: None,
        }),
        ..ToolCallContext::default()
    }
}

struct Tools {
    store: Arc<dyn RecordStore>,
    _sandbox: tempfile::TempDir,
    defs: Vec<beam_tools::ToolDefinition>,
}

fn tools() -> Tools {
    let sandbox = tempfile::tempdir().unwrap();
    let store = InMemoryRecords::shared();
    let tools = incident_tools(&store, Some(Postmortems::open(sandbox.path()).unwrap()));
    Tools { store, _sandbox: sandbox, defs: tools }
}

impl Tools {
    async fn call(&self, tool: &str, args: Value, user: &str) -> Result<Value, String> {
        let tool = self.defs.iter().find(|t| t.name == tool).unwrap();
        tool.callable
            .as_ref()
            .unwrap()
            .call(args, &ctx(user))
            .await
            .map_err(|e| e.errcode.as_str().to_owned())
    }

    /// Open the checkout incident as Kai; returns its id.
    async fn open_checkout(&self) -> String {
        let opened = self
            .call(
                OPEN_INCIDENT,
                json!({"service": "checkout-api", "severity": "critical", "summary": "checkout is slow"}),
                KAI,
            )
            .await
            .unwrap();
        opened["incident_id"].as_str().unwrap().to_owned()
    }
}

#[tokio::test]
async fn only_the_team_opens_incidents_and_only_one_per_service() {
    let t = tools();
    let args = json!({"service": "checkout-api", "severity": "critical", "summary": "checkout is slow"});
    let stranger = "99999999-0000-4000-8000-000000000001";
    assert_eq!(t.call(OPEN_INCIDENT, args.clone(), stranger).await.unwrap_err(), "SHC-004");
    assert!(t.store.list(SHOWCASE, INCIDENT, None).await.unwrap().is_empty());

    let id = t.open_checkout().await;
    assert_eq!(t.call(OPEN_INCIDENT, args.clone(), LIN).await.unwrap_err(), "SHC-003", "already open");
    let other = json!({"service": "search-api", "severity": "warning", "summary": "cpu is high"});
    assert!(t.call(OPEN_INCIDENT, other, LIN).await.is_ok(), "another service is fine");
    let bad = json!({"service": "billing-api", "severity": "urgent", "summary": "x"});
    assert_eq!(t.call(OPEN_INCIDENT, bad, KAI).await.unwrap_err(), "SHC-005");
    assert_eq!(t.call(GET_INCIDENT, json!({"incident_id": id}), stranger).await.unwrap_err(), "SHC-004");

    // Resolve it; a new incident for the service may then be opened.
    let restart = json!({"incident_id": id, "service": "checkout-api", "reason": "stuck pool"});
    t.call(RESTART_SERVICE, restart, KAI).await.unwrap();
    t.call(RESOLVE_INCIDENT, json!({"incident_id": id, "summary": "restarted"}), KAI).await.unwrap();
    assert!(t.call(OPEN_INCIDENT, args, KAI).await.is_ok());
}

#[tokio::test]
async fn remediations_and_resolution_follow_the_incident_status() {
    let t = tools();
    let id = t.open_checkout().await;
    let resolve = json!({"incident_id": id, "summary": "done"});
    assert_eq!(t.call(RESOLVE_INCIDENT, resolve.clone(), KAI).await.unwrap_err(), "SHC-003", "not mitigated yet");
    let wrong = json!({"incident_id": id, "service": "search-api", "to_version": "v1", "reason": "x"});
    assert_eq!(t.call(ROLLBACK_DEPLOY, wrong, KAI).await.unwrap_err(), "SHC-005", "another service");
    let rollback = json!({"incident_id": id, "service": "checkout-api", "to_version": "v2.13.4", "reason": "bad deploy"});
    let done = t.call(ROLLBACK_DEPLOY, rollback.clone(), KAI).await.unwrap();
    assert_eq!(done["simulated"], true);
    assert_eq!(done["status"], "mitigated");
    t.call(RESOLVE_INCIDENT, resolve, KAI).await.unwrap();
    assert_eq!(t.call(ROLLBACK_DEPLOY, rollback, KAI).await.unwrap_err(), "SHC-003", "resolved");
    let note = json!({"incident_id": id, "entry": "late note"});
    assert_eq!(t.call(ADD_TIMELINE_ENTRY, note, KAI).await.unwrap_err(), "SHC-003");
}

#[tokio::test]
async fn an_sre_cannot_write_the_postmortem() {
    let t = tools();
    let id = t.open_checkout().await;
    let restart = json!({"incident_id": id, "service": "checkout-api", "reason": "stuck pool"});
    t.call(RESTART_SERVICE, restart, KAI).await.unwrap();
    let postmortem = json!({"incident_id": id, "summary": "s", "impact": "i", "root_cause": "r",
        "what_went_well": "w", "action_items": ["a"]});
    // Not resolved yet: the commander cannot write it either.
    assert_eq!(t.call(WRITE_POSTMORTEM, postmortem.clone(), LIN).await.unwrap_err(), "SHC-003");
    t.call(RESOLVE_INCIDENT, json!({"incident_id": id, "summary": "restarted"}), LIN).await.unwrap();
    assert_eq!(t.call(WRITE_POSTMORTEM, postmortem.clone(), KAI).await.unwrap_err(), "SHC-004");
    let written = t.call(WRITE_POSTMORTEM, postmortem, LIN).await.unwrap();
    assert_eq!(written["committed"].as_str().unwrap().len(), 40);
}
