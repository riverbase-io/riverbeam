//! `underwriting_precheck` on an in-memory beam: an exception the officer
//! decides through a choice, a note and an approved packet commit; a clean
//! file that goes straight to the packet; and a cancel while it waits.

use std::sync::Arc;

use async_trait::async_trait;
use beam_core::{BeamConfig, RiverBeam};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{
    ToolCallContext, ToolDefinition, ToolHandler, ToolRegistry, ToolRuntime, ToolService,
};
use beam_types::{
    ActionResponseEnvelope, AgentResult, ContentPart, InvokeAuthContext, InvokeRequest,
    RequestContext, RequestFact,
};
use showcase::loan_desk::agents::UNDERWRITING_PRECHECK;
use showcase::loan_desk::app::{bind_agents, LoanDesk};
use showcase::loan_desk::data::{Application, ApplicationStatus, ApplicationStore, InMemoryApplications};
use showcase::loan_desk::mcp_stub::credit_report;
use showcase::loan_desk::packet::Packets;
use showcase::loan_desk::personas::{BOB, CAROL, DANA, ORG};
use showcase::loan_desk::precheck::PULL_CREDIT;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

/// The bureau, as the MCP bridge would register it.
struct Bureau;

#[async_trait]
impl ToolHandler for Bureau {
    async fn call(&self, args: Value, _ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        Ok(credit_report(
            args["borrower_id"].as_str().unwrap_or_default(),
        ))
    }
}

struct Desk {
    beam: RiverBeam,
    applications: Arc<InMemoryApplications>,
    packets: Arc<Packets>,
    _dir: tempfile::TempDir,
}

async fn desk(turns: Vec<ScriptedTurn>) -> Desk {
    let registry = Arc::new(ToolRegistry::new());
    registry.register(
        ToolDefinition {
            side_effect: false,
            ..ToolDefinition::new(PULL_CREDIT)
        }
        .with_handler(Arc::new(Bureau)),
    );
    let dir = tempfile::tempdir().unwrap();
    let packets = Packets::open(dir.path()).unwrap();
    let applications = Arc::new(InMemoryApplications::default());
    let store: Arc<dyn ApplicationStore> = applications.clone();
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(ScriptedModel::new(turns)))
        .tools(Arc::new(ToolService::new(
            registry,
            Arc::new(ToolRuntime::default()),
        )))
        .build()
        .unwrap();
    bind_agents(
        &beam,
        &LoanDesk {
            applications: store,
            mcp_tools: Vec::new(),
            packets: Some(packets.clone()),
        },
    )
    .await
    .unwrap();
    beam.checkin_catalog().await.unwrap();
    Desk {
        beam,
        applications,
        packets,
        _dir: dir,
    }
}

fn submitted(owner: &str, program: &str, fields: &Value) -> Application {
    let mut application = Application::new(owner, Some(ORG), program);
    application.fields.clone_from(fields.as_object().unwrap());
    application
        .documents
        .push(json!({"file_name": "stub.png", "document_type": "pay_stub"}));
    application.submit().unwrap();
    application
}

fn officer(application: Uuid) -> InvokeRequest {
    InvokeRequest {
        agent_name: UNDERWRITING_PRECHECK.into(),
        content: vec![ContentPart::text("Run the pre-check.")],
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
            user_id: Some(CAROL.into()),
            org_id: Some(ORG.into()),
            mcp_token: None,
        }),
        context: Some(RequestContext {
            facts: vec![RequestFact {
                key: "application_id".into(),
                value: json!(application.to_string()),
                expiry: None,
                sensitivity: beam_types::Sensitivity::Internal,
            }],
            ..RequestContext::default()
        }),
        client: None,
    }
}

/// Answer the run's one held action with `payload`, as the officer.
async fn answer(
    desk: &Desk,
    req: &InvokeRequest,
    held: &AgentResult,
    payload: Value,
) -> AgentResult {
    let action = &held.action_required.as_ref().expect("a held action")[0];
    let mut next = req.clone();
    next.request_id = Some(Uuid::new_v4());
    next.content = Vec::new();
    next.execution_id = held.execution_id;
    let response: ActionResponseEnvelope = serde_json::from_value(json!({
        "action_id": action["action_id"],
        "lc_action_id": action["lc_action_id"],
        "kind": action["action_request"]["kind"],
        "request_id": action["action_request"]["request_id"],
        "status": "submitted",
        "payload": payload,
    }))
    .unwrap();
    next.actions = Some(vec![response]);
    desk.beam.invoke_result(next).await.unwrap()
}

fn kind(result: &AgentResult) -> String {
    result.action_required.as_ref().unwrap()[0]["action_request"]["kind"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn an_exception_is_decided_noted_and_committed_with_approval() {
    let desk = desk(vec![ScriptedTurn::text(
        "Approved with a compensating factor: five years at one employer.",
    )])
    .await;
    // Bob: FHA, 5% down, a late payment on his credit report.
    let application = submitted(
        BOB,
        "fha",
        &json!({"first_name": "Bob", "last_name": "Tran", "email": "b@x", "phone": "1", "employer": "R",
                "gross_monthly_income": 6200, "monthly_debts": 820, "purchase_price": 320_000, "down_payment": 16_000}),
    );
    desk.applications.insert(&application).await.unwrap();
    let req = officer(application.id);

    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert!(held.interrupted);
    assert_eq!(kind(&held), "beam.choice_question");
    let pulled = desk
        .applications
        .get(application.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pulled.status, ApplicationStatus::Precheck);

    let held = answer(
        &desk,
        &req,
        &held,
        json!({"selected_choices": ["Approve with exception"]}),
    )
    .await;
    assert_eq!(kind(&held), "beam.form_input");
    let held = answer(
        &desk,
        &req,
        &held,
        json!({"values": {"note": "Late payment explained: bank error."}}),
    )
    .await;
    assert_eq!(kind(&held), "beam.tool_approval");
    let args = &held.action_required.as_ref().unwrap()[0]["action_request"]["payload"]["args"];
    assert!(args["decision_markdown"]
        .as_str()
        .unwrap()
        .contains("bank error"));
    assert!(args["decision_markdown"]
        .as_str()
        .unwrap()
        .contains("compensating factor"));
    assert!(
        desk.packets.read(application.id).await.unwrap()["files"] == json!([]),
        "nothing before approval"
    );

    let done = answer(&desk, &req, &held, json!({"approved": true})).await;
    assert!(!done.interrupted);
    assert!(
        done.text_content().contains("committed"),
        "{}",
        done.text_content()
    );
    let decided = desk
        .applications
        .get(application.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decided.status, ApplicationStatus::Approved);
    assert_eq!(decided.decision.as_ref().unwrap()["outcome"], "approve");
    let packet = desk.packets.read(application.id).await.unwrap();
    assert_eq!(packet["files"].as_array().unwrap().len(), 2);
    assert_eq!(packet["log"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn a_clean_file_goes_straight_to_the_packet() {
    let desk = desk(vec![ScriptedTurn::text("Meets every VA guideline.")]).await;
    let application = submitted(
        DANA,
        "va",
        &json!({"first_name": "Dana", "last_name": "Nguyen", "email": "d@x", "phone": "1", "employer": "N",
                "gross_monthly_income": 9000, "monthly_debts": 410, "purchase_price": 400_000, "down_payment": 0}),
    );
    desk.applications.insert(&application).await.unwrap();
    let req = officer(application.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert_eq!(
        kind(&held),
        "beam.tool_approval",
        "no exception, no decision to ask for"
    );
    let denied = answer(
        &desk,
        &req,
        &held,
        json!({"approved": false, "comment": "not yet"}),
    )
    .await;
    assert!(denied.text_content().contains("not written"));
    assert_eq!(
        desk.applications
            .get(application.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ApplicationStatus::Approved
    );
    assert_eq!(
        desk.packets.read(application.id).await.unwrap()["files"],
        json!([])
    );
}

#[tokio::test]
async fn a_draft_cannot_be_prechecked_and_a_waiting_run_can_be_cancelled() {
    let desk = desk(vec![]).await;
    let draft = Application::new(BOB, Some(ORG), "fha");
    desk.applications.insert(&draft).await.unwrap();
    let err = desk
        .beam
        .invoke_result(officer(draft.id))
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "LND-003", "{err}");

    let application = submitted(
        BOB,
        "fha",
        &json!({"first_name": "Bob", "last_name": "Tran", "email": "b@x", "phone": "1", "employer": "R",
                "gross_monthly_income": 6200, "monthly_debts": 820, "purchase_price": 320_000, "down_payment": 16_000}),
    );
    desk.applications.insert(&application).await.unwrap();
    let held = desk
        .beam
        .invoke_result(officer(application.id))
        .await
        .unwrap();
    let execution = held.execution_id.unwrap();
    desk.beam.cancel_execution(execution).await.unwrap();
    let saved = desk
        .beam
        .runtime_states()
        .load(execution)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.status, "cancelled");
}

/// Plan 023 D2: the rationale's nested model call counts toward the run's
/// usage (before, the pre-check reported none) and is recorded as a
/// `rationale` step.
#[tokio::test]
async fn the_rationale_call_is_metered_and_recorded() {
    let desk = desk(vec![ScriptedTurn::text("Meets every VA guideline.")]).await;
    let application = submitted(
        DANA,
        "va",
        &json!({"first_name": "Dana", "last_name": "Nguyen", "email": "d@x", "phone": "1", "employer": "N",
                "gross_monthly_income": 9000, "monthly_debts": 410, "purchase_price": 400_000, "down_payment": 0}),
    );
    desk.applications.insert(&application).await.unwrap();
    let req = officer(application.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    // The scripted model reports 10 + 5 tokens for its one call.
    let usage = held.token_usage.expect("the rationale's tokens");
    assert_eq!(usage.total_tokens, 15, "{usage:?}");
    let messages = desk
        .beam
        .store()
        .list_messages(req.session_id)
        .await
        .unwrap();
    let run_id = messages
        .iter()
        .find_map(|m| m.run_id)
        .expect("the run's messages");
    let steps = desk.beam.store().list_steps(run_id).await.unwrap();
    let names: Vec<&str> = steps.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"rationale"), "{names:?}");
}
