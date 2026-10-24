//! `loan_assistant` end to end on an in-memory beam with a scripted model:
//! the borrower flow (client tools, an in-loop form, saving, an approved
//! submit), cited guidelines and remembered preferences, and ownership.

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_knowledge::{HashEmbedder, InMemoryVectorStore, KnowledgeManager};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{ToolRegistry, ToolRuntime, ToolService, KNOWLEDGE_SEARCH};
use beam_types::{
    ActionResponseEnvelope, ClientCapabilities, ContentPart, InvokeAuthContext, InvokeRequest,
    RequestContext, RequestFact,
};
use showcase::loan_desk::agents::LOAN_ASSISTANT;
use showcase::loan_desk::app::{bind_agents, LoanDesk};
use showcase::loan_desk::data::{Application, ApplicationStatus, ApplicationStore, InMemoryApplications};
use showcase::loan_desk::personas::{BOB, DANA, ORG};
use showcase::loan_desk::tools::{
    CLIENT_TOOLS, FILL_FORM_FIELDS, GET_APPLICATION, GET_BORROWER_PROFILE, REQUEST_INPUT,
    SUBMIT_APPLICATION, UPDATE_APPLICATION,
};
use serde_json::{json, Value};
use uuid::Uuid;

struct Desk {
    beam: RiverBeam,
    applications: Arc<InMemoryApplications>,
    model: ScriptedModel,
}

async fn desk(turns: Vec<ScriptedTurn>) -> Desk {
    let model = ScriptedModel::new(turns);
    let applications = Arc::new(InMemoryApplications::default());
    let store: Arc<dyn ApplicationStore> = applications.clone();
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(model.clone()))
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
    let tools = bind_agents(
        &beam,
        &LoanDesk {
            applications: store,
            mcp_tools: Vec::new(),
            packets: None,
        },
    )
    .await
    .unwrap();
    assert!(tools.iter().any(|t| t == KNOWLEDGE_SEARCH));
    beam.checkin_catalog().await.unwrap();
    Desk {
        beam,
        applications,
        model,
    }
}

fn turn(user: &str, application: Uuid, text: &str) -> InvokeRequest {
    InvokeRequest {
        agent_name: LOAN_ASSISTANT.into(),
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
        context: Some(RequestContext {
            facts: vec![RequestFact {
                key: "application_id".into(),
                value: json!(application.to_string()),
                expiry: None,
                sensitivity: beam_types::Sensitivity::Internal,
            }],
            ..RequestContext::default()
        }),
        client: Some(ClientCapabilities {
            client_id: "tab-1".into(),
            kind: "web".into(),
            tools: CLIENT_TOOLS.iter().map(|t| (*t).to_owned()).collect(),
        }),
    }
}

/// An answer to every held action of `result`: client tools get `outputs`
/// (by tool name), approvals are approved.
fn answers(result: &beam_types::AgentResult, outputs: &Value) -> Vec<ActionResponseEnvelope> {
    result
        .action_required
        .as_ref()
        .unwrap()
        .iter()
        .map(|action| {
            let request = &action["action_request"];
            let payload = if request["kind"] == "beam.client_tool" {
                let tool = request["payload"]["tool_name"].as_str().unwrap();
                json!({"output": outputs.get(tool).cloned().unwrap_or(json!("done"))})
            } else {
                json!({"approved": true})
            };
            serde_json::from_value(json!({
                "action_id": action["action_id"],
                "lc_action_id": action["lc_action_id"],
                "kind": request["kind"],
                "request_id": request["request_id"],
                "status": "submitted",
                "payload": payload,
            }))
            .unwrap()
        })
        .collect()
}

async fn resume(desk: &Desk, req: &InvokeRequest, outputs: Value) -> beam_types::AgentResult {
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert!(held.interrupted, "expected a hold: {:?}", held.content);
    let mut next = req.clone();
    next.request_id = Some(Uuid::new_v4());
    next.content = Vec::new();
    next.actions = Some(answers(&held, &outputs));
    desk.beam.invoke_result(next).await.unwrap()
}

#[tokio::test]
async fn a_borrower_completes_and_submits_with_the_assistant() {
    let mut application = Application::new(BOB, Some(ORG), "fha");
    application
        .documents
        .push(json!({"file_name": "stub.png", "document_type": "pay_stub"}));
    let fields = json!({
        "first_name": "Bob", "last_name": "Tran", "email": "bob@example.com",
        "phone": "+1 555 0142", "employer": "Riverside Logistics",
    });
    let desk = desk(vec![
        // Turn 1: read, fill what is known, then ask for the rest with a form.
        ScriptedTurn::ToolCalls(vec![
            ("c1".into(), GET_BORROWER_PROFILE.into(), json!({})),
            ("c2".into(), GET_APPLICATION.into(), json!({})),
        ]),
        ScriptedTurn::ToolCalls(vec![
            (
                "c3".into(),
                FILL_FORM_FIELDS.into(),
                json!({"fields": fields, "provenance_source": "Borrower profile"}),
            ),
            (
                "c4".into(),
                REQUEST_INPUT.into(),
                json!({"title": "A few numbers", "fields": [
                    {"name": "gross_monthly_income", "label": "Gross monthly income", "type": "number", "required": true}]}),
            ),
        ]),
        ScriptedTurn::tool_call(
            "c5",
            UPDATE_APPLICATION,
            json!({"fields": {
                "first_name": "Bob", "last_name": "Tran", "email": "bob@example.com",
                "phone": "+1 555 0142", "employer": "Riverside Logistics",
                "gross_monthly_income": 6200, "monthly_debts": 820,
                "purchase_price": 320_000, "down_payment": 16_000}}),
        ),
        ScriptedTurn::text("Saved. Ask me to submit when you are ready."),
        // Turn 2: the borrower asks to submit; the call waits for approval.
        ScriptedTurn::tool_call("c6", SUBMIT_APPLICATION, json!({})),
        ScriptedTurn::text("Submitted for underwriting."),
    ])
    .await;
    desk.applications.insert(&application).await.unwrap();

    let req = turn(BOB, application.id, "Help me finish my application.");
    let done = resume(
        &desk,
        &req,
        json!({"request_input": {"gross_monthly_income": 6200, "monthly_debts": 820,
            "purchase_price": 320_000, "down_payment": 16_000}}),
    )
    .await;
    assert_eq!(
        done.text_content(),
        "Saved. Ask me to submit when you are ready."
    );
    let saved = desk
        .applications
        .get(application.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        saved.missing_fields().is_empty(),
        "{:?}",
        saved.missing_fields()
    );
    // The form card's answer reached the model.
    assert!(format!("{:?}", desk.model.seen()[2].prompt).contains("6200"));

    let submit = turn(BOB, application.id, "Please submit it.");
    let held = desk.beam.invoke_result(submit.clone()).await.unwrap();
    let action = &held.action_required.as_ref().unwrap()[0];
    assert_eq!(action["action_request"]["kind"], "beam.tool_approval");
    assert_eq!(action["action_request"]["payload"]["risk"], "high");
    assert_eq!(
        desk.applications
            .get(application.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ApplicationStatus::Draft,
        "nothing is submitted before the borrower approves"
    );
    let mut approve = submit.clone();
    approve.request_id = Some(Uuid::new_v4());
    approve.content = Vec::new();
    approve.actions = Some(answers(&held, &json!({})));
    let done = desk.beam.invoke_result(approve).await.unwrap();
    assert_eq!(done.text_content(), "Submitted for underwriting.");
    assert_eq!(
        desk.applications
            .get(application.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ApplicationStatus::Submitted
    );
}

#[tokio::test]
async fn the_assistant_cites_guidelines_and_cannot_read_another_borrowers_application() {
    let bobs = Application::new(BOB, Some(ORG), "fha");
    let desk = desk(vec![
        ScriptedTurn::tool_call(
            "c1",
            KNOWLEDGE_SEARCH,
            json!({"query": "FHA minimum credit score"}),
        ),
        ScriptedTurn::text("FHA needs a 580 score with 3.5% down [fha.md]."),
        ScriptedTurn::tool_call(
            "c2",
            GET_APPLICATION,
            json!({"application_id": bobs.id.to_string()}),
        ),
        ScriptedTurn::text("I can only see your own application."),
    ])
    .await;
    desk.applications.insert(&bobs).await.unwrap();
    let mine = Application::new(DANA, Some(ORG), "va");
    desk.applications.insert(&mine).await.unwrap();

    let done = desk
        .beam
        .invoke_result(turn(DANA, mine.id, "What credit score does FHA need?"))
        .await
        .unwrap();
    let search = &done.tool_calls[0];
    assert_eq!(search.tool_name, KNOWLEDGE_SEARCH);
    assert!(
        search.result.as_deref().unwrap_or_default().contains("580"),
        "{search:?}"
    );

    let done = desk
        .beam
        .invoke_result(turn(DANA, mine.id, "Show me Bob's application."))
        .await
        .unwrap();
    let read = done.tool_calls[0].result.as_deref().unwrap_or_default();
    assert!(
        read.contains("LND-002") || read.contains("not found"),
        "{read}"
    );
    assert!(!read.contains("Tran"));
}

#[tokio::test]
async fn a_client_tool_is_offered_only_to_a_declaring_client() {
    let application = Application::new(BOB, Some(ORG), "fha");
    let desk = desk(vec![
        ScriptedTurn::text("hi"),
        ScriptedTurn::text("hi again"),
    ])
    .await;
    desk.applications.insert(&application).await.unwrap();
    let mut req = turn(BOB, application.id, "hello");
    desk.beam.invoke_result(req.clone()).await.unwrap();
    req.client = None;
    req.request_id = Some(Uuid::new_v4());
    desk.beam.invoke_result(req).await.unwrap();
    let offered = |turn: usize| -> Vec<String> {
        desk.model.seen()[turn]
            .prepared
            .tools
            .iter()
            .map(|t| t.name.to_string())
            .collect()
    };
    assert!(offered(0).iter().any(|t| t == REQUEST_INPUT));
    assert!(!offered(1)
        .iter()
        .any(|t| CLIENT_TOOLS.contains(&t.as_str())));
    assert!(offered(1).iter().any(|t| t == SUBMIT_APPLICATION));
}
