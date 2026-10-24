//! The support desk on an in-memory beam with a scripted model: a customer's
//! refund request becomes a ticket, the support agent triages it and issues
//! the refund after approving the call; refunds stop at the order total and
//! customers see only their own tickets.

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
use showcase::records::{InMemoryRecords, Record, RecordStore};
use showcase::support::agents::{SUPPORT_ASSISTANT, TICKET_TRIAGE};
use showcase::support::personas::{MIA, OMAR, ORG, SAM};
use showcase::support::tools::{
    order_tools, support_tools, ESCALATE_TICKET, GET_CUSTOMER_PROFILE, GET_ORDER, GET_TICKET,
    ISSUE_REFUND, MY_ORDER, OPEN_TICKET, TICKET_ORDER, TRIAGE_TICKET,
};
use showcase::support::{bind_agents, SHOWCASE, TICKET};
use uuid::Uuid;

struct Desk {
    beam: RiverBeam,
    tickets: Arc<dyn RecordStore>,
}

async fn desk(turns: Vec<ScriptedTurn>) -> Desk {
    let model = ScriptedModel::new(turns);
    let tickets = InMemoryRecords::shared();
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(model))
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
    let owned = bind_agents(&beam, &tickets).await.unwrap();
    assert!(owned.iter().any(|t| t == ISSUE_REFUND));
    assert!(!owned.iter().any(|t| t == KNOWLEDGE_SEARCH), "shared tools are not owned");
    beam.checkin_catalog().await.unwrap();
    Desk { beam, tickets }
}

fn turn(agent: &str, user: &str, ticket: Option<Uuid>, text: &str) -> InvokeRequest {
    InvokeRequest {
        agent_name: agent.into(),
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
        context: ticket.map(|id| RequestContext {
            facts: vec![RequestFact {
                key: "ticket_id".into(),
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
async fn a_refund_request_becomes_a_ticket_and_an_approved_refund() {
    let desk = desk(vec![
        // Mia's turn: her profile, then a ticket for the broken grinder.
        ScriptedTurn::tool_call("c1", GET_CUSTOMER_PROFILE, json!({})),
        ScriptedTurn::tool_call(
            "c2",
            OPEN_TICKET,
            json!({"subject": "Grinder arrived broken", "message": "The hopper is cracked.",
                "order_id": "ord-1002", "category": "damaged"}),
        ),
        ScriptedTurn::text("I opened a ticket; an agent will follow up within one business day."),
        // Sam's turn: triage, then the refund, which waits for his approval.
        ScriptedTurn::tool_call(
            "c3",
            TRIAGE_TICKET,
            json!({"category": "damaged", "priority": "high", "sentiment": "negative",
                "summary": "Grinder arrived with a cracked hopper.",
                "draft_reply": "Sorry about that, Mia. We are refunding the grinder in full."}),
        ),
        ScriptedTurn::tool_call("c4", ISSUE_REFUND, json!({"amount": 89.5, "reason": "Damaged on arrival"})),
        ScriptedTurn::text("Refunded $89.50."),
    ])
    .await;

    let done = desk
        .beam
        .invoke_result(turn(SUPPORT_ASSISTANT, MIA, None, "My grinder arrived broken. Refund please."))
        .await
        .unwrap();
    assert!(done.text_content().contains("ticket"));
    let tickets = desk.tickets.list(SHOWCASE, TICKET, Some(MIA)).await.unwrap();
    assert_eq!(tickets.len(), 1);
    let ticket = &tickets[0];
    assert_eq!(ticket.text("order_id"), Some("ORD-1002"));
    assert_eq!(ticket.status, "open");

    let work = turn(TICKET_TRIAGE, SAM, Some(ticket.id), "Triage this and refund it in full.");
    let held = desk.beam.invoke_result(work.clone()).await.unwrap();
    assert!(held.interrupted, "the refund waits: {:?}", held.content);
    let action = &held.action_required.as_ref().unwrap()[0];
    assert_eq!(action["action_request"]["kind"], "beam.tool_approval");
    assert_eq!(action["action_request"]["payload"]["risk"], "high");
    let triaged = desk.tickets.get(ticket.id).await.unwrap().unwrap();
    assert_eq!(triaged.status, "triaged", "no refund before approval");
    assert_eq!(triaged.data["triage"]["priority"], "high");

    let mut approve = work.clone();
    approve.request_id = Some(Uuid::new_v4());
    approve.content = Vec::new();
    approve.actions = Some(approve_all(&held));
    let done = desk.beam.invoke_result(approve).await.unwrap();
    assert_eq!(done.text_content(), "Refunded $89.50.");
    let refunded = desk.tickets.get(ticket.id).await.unwrap().unwrap();
    assert_eq!(refunded.status, "refunded");
    assert_eq!(refunded.data["refund"]["amount"], 89.5);
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

async fn call(store: &Arc<dyn RecordStore>, tool: &str, args: Value, user: &str) -> Result<Value, String> {
    let tools = support_tools(store);
    let tool = tools.iter().find(|t| t.name == tool).unwrap();
    tool.callable
        .as_ref()
        .unwrap()
        .call(args, &ctx(user))
        .await
        .map_err(|e| e.errcode.as_str().to_owned())
}

#[tokio::test]
async fn refunds_stop_at_the_order_total_and_tickets_stay_private() {
    let store = InMemoryRecords::shared();
    let mut ticket = Record::new(SHOWCASE, TICKET, MIA, Some(ORG), "Late headphones");
    ticket.data.insert("order_id".into(), json!("ORD-1001"));
    store.insert(&ticket).await.unwrap();
    let id = ticket.id.to_string();

    let over = call(&store, ISSUE_REFUND, json!({"ticket_id": id, "amount": 500, "reason": "late"}), SAM).await;
    assert_eq!(over.unwrap_err(), "SHC-005", "over the $129 order total");
    let by_customer = call(&store, ISSUE_REFUND, json!({"ticket_id": id, "amount": 10, "reason": "late"}), MIA).await;
    assert_eq!(by_customer.unwrap_err(), "SHC-004", "only an agent refunds");
    let other = call(&store, GET_TICKET, json!({"ticket_id": id}), OMAR).await;
    assert_eq!(other.unwrap_err(), "SHC-002", "another customer's ticket does not exist for Omar");
    let own = call(&store, GET_TICKET, json!({"ticket_id": id}), MIA).await.unwrap();
    assert_eq!(own["customer"]["email"], "mia@example.com");
    let ok = call(&store, ISSUE_REFUND, json!({"ticket_id": id, "amount": 14.95, "reason": "late express"}), SAM)
        .await
        .unwrap();
    assert_eq!(ok["refund"]["order_id"], "ORD-1001");
}

#[tokio::test]
async fn a_ticket_is_refunded_once_even_after_an_escalation() {
    let store = InMemoryRecords::shared();
    let mut ticket = Record::new(SHOWCASE, TICKET, MIA, Some(ORG), "Broken grinder");
    ticket.data.insert("order_id".into(), json!("ORD-1002"));
    store.insert(&ticket).await.unwrap();
    let id = ticket.id.to_string();
    let refund = json!({"ticket_id": id, "amount": 89.5, "reason": "damaged"});
    call(&store, ISSUE_REFUND, refund.clone(), SAM).await.unwrap();
    let escalate = call(&store, ESCALATE_TICKET, json!({"ticket_id": id, "reason": "angry"}), SAM).await;
    assert_eq!(escalate.unwrap_err(), "SHC-003", "a refunded ticket is not escalated");
    // Even moved back by hand, a ticket that carries a refund is not refunded again.
    let mut moved = store.get(ticket.id).await.unwrap().unwrap();
    moved.status = "escalated".into();
    store.save(&mut moved).await.unwrap();
    assert_eq!(call(&store, ISSUE_REFUND, refund, SAM).await.unwrap_err(), "SHC-003");
}

/// Echoes the arguments the order system was called with.
struct Echo;

#[async_trait::async_trait]
impl beam_tools::ToolHandler for Echo {
    async fn call(&self, args: Value, _ctx: &ToolCallContext) -> riverbase_core::RiverbaseResult<Value> {
        Ok(args)
    }
}

#[tokio::test]
async fn order_lookups_use_the_callers_own_email() {
    let registry = ToolRegistry::new();
    registry.register(beam_tools::ToolDefinition::new(GET_ORDER).with_handler(Arc::new(Echo)));
    let store = InMemoryRecords::shared();
    let mut ticket = Record::new(SHOWCASE, TICKET, OMAR, Some(ORG), "Wrong colour");
    ticket.data.insert("order_id".into(), json!("ORD-2001"));
    store.insert(&ticket).await.unwrap();
    let tools = order_tools(&registry, &store);
    let run = |name: &str, args: Value, user: &str| {
        let tool = tools.iter().find(|t| t.name == name).unwrap().callable.clone().unwrap();
        let ctx = ctx(user);
        async move { tool.call(args, &ctx).await.map_err(|e| e.errcode.as_str().to_owned()) }
    };
    // The model names Omar's email; the lookup still asks for Mia's own.
    let asked = run(MY_ORDER, json!({"order_id": "ORD-2001", "email": "omar@example.com"}), MIA).await.unwrap();
    assert_eq!(asked["email"], "mia@example.com");
    assert_eq!(run(MY_ORDER, json!({"order_id": "ORD-1"}), SAM).await.unwrap_err(), "SHC-004");
    let asked = run(TICKET_ORDER, json!({"ticket_id": ticket.id.to_string()}), SAM).await.unwrap();
    assert_eq!(asked, json!({"order_id": "ORD-2001", "email": "omar@example.com"}));
    let refused = run(TICKET_ORDER, json!({"ticket_id": ticket.id.to_string()}), MIA).await;
    assert_eq!(refused.unwrap_err(), "SHC-004", "only an agent reads a ticket's order");
}
