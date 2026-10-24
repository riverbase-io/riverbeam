//! Support desk: a store's customer support.
//!
//! Customers chat with `support_assistant`, which answers from the help
//! center (RAG), looks orders up in the order system (MCP, verified by the
//! customer's email), remembers their preferences, and opens a ticket for
//! what it cannot settle. Support agents work the ticket queue with
//! `ticket_triage`: it records a triage (category, priority, sentiment,
//! summary, draft reply) and issues refunds, each held for the agent's
//! approval and capped at the order total.

pub mod agents;
pub mod personas;
pub mod stub;
pub mod tools;

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use beam_core::RiverBeam;
use beam_store::PgBeamStore;
use riverbase_http::Principal;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::errors::SHC_002;
use crate::kit::{caller, ingest_org_documents, problem, shared_agent_tools};
use crate::records::{require, PgRecords, Record, RecordStore};
use crate::registry::{BoxError, Persona, RouteContext, Shared, Showcase};
use personas::{AGENT_ROLE, OMAR, ORG, PERSONAS};
use tools::{
    register_support_tools, ticket_view, AGENT_TOOLS, CUSTOMER_TOOLS, MY_ORDER, MY_ORDERS, TICKET_ORDER,
    TRACK_MY_ORDER,
};

/// The showcase's key.
pub const SHOWCASE: &str = "support";
/// Its records' kind.
pub const TICKET: &str = "ticket";

/// The help center, for RAG: `(file, title, text)`.
pub const HELP_CENTER: [(&str, &str, &str); 4] = [
    ("shipping.md", "Shipping and delivery", include_str!("../../knowledge/support/shipping.md")),
    ("returns.md", "Returns and refunds", include_str!("../../knowledge/support/returns.md")),
    ("warranty.md", "Warranty and damaged items", include_str!("../../knowledge/support/warranty.md")),
    ("faq.md", "Customer FAQ", include_str!("../../knowledge/support/faq.md")),
];

/// Register the support tools and bind both agents on `beam`. Returns the
/// tools it registered. Tests call it with an in-memory store.
///
/// # Errors
/// The tool runtime is missing, or the help center cannot be ingested.
pub async fn bind_agents(
    beam: &RiverBeam,
    tickets: &Arc<dyn RecordStore>,
) -> Result<Vec<String>, BoxError> {
    let registry = beam
        .tools()
        .ok_or("the support desk needs the tool runtime")?
        .registry()
        .clone();
    let owned = register_support_tools(&registry, tickets);
    let shared = shared_agent_tools(beam, &registry);
    if let Some(knowledge) = beam.knowledge() {
        ingest_org_documents(knowledge, ORG, &HELP_CENTER).await?;
    }
    // The order lookups exist only when the order system is connected.
    let has = |name: &str| owned.iter().any(|t| t == name);
    let mut assistant: Vec<String> = CUSTOMER_TOOLS.iter().map(|t| (*t).to_owned()).collect();
    assistant.extend(shared.iter().cloned());
    assistant.extend(
        [MY_ORDERS, MY_ORDER, TRACK_MY_ORDER]
            .into_iter()
            .filter(|t| has(t))
            .map(str::to_owned),
    );
    beam.bind_rig_agent(agents::assistant_metadata(), agents::assistant_def(assistant));
    let mut triage: Vec<String> = AGENT_TOOLS.iter().map(|t| (*t).to_owned()).collect();
    triage.extend(shared.into_iter().filter(|t| t == beam_tools::KNOWLEDGE_SEARCH));
    if has(TICKET_ORDER) {
        triage.push(TICKET_ORDER.to_owned());
    }
    beam.bind_rig_agent(agents::triage_metadata(), agents::triage_def(triage));
    Ok(owned)
}

/// The support desk in the showcase server.
pub struct SupportShowcase;

#[async_trait(?Send)]
impl Showcase for SupportShowcase {
    fn key(&self) -> &'static str {
        SHOWCASE
    }

    fn title(&self) -> &'static str {
        "Support desk"
    }

    fn tagline(&self) -> &'static str {
        "Customer support: help-center answers, order lookups, tickets, triage and approved refunds"
    }

    fn org(&self) -> &'static str {
        ORG
    }

    fn personas(&self) -> Vec<Persona> {
        PERSONAS.to_vec()
    }

    fn agents(&self) -> &'static [&'static str] {
        &[agents::SUPPORT_ASSISTANT, agents::TICKET_TRIAGE]
    }

    fn api_prefix(&self) -> &'static str {
        "/api/support/"
    }

    async fn migrate(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        PgRecords::new(store.clone()).migrate().await?;
        Ok(())
    }

    /// Omar's open ticket about a wrong colour, so the queue is not empty.
    async fn seed(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        let tickets = PgRecords::new(store.clone());
        if tickets.list(SHOWCASE, TICKET, Some(OMAR)).await?.is_empty() {
            let mut ticket = Record::new(SHOWCASE, TICKET, OMAR, Some(ORG), "Wrong colour jacket delivered");
            let fields = json!({
                "category": "wrong_item",
                "order_id": "ORD-2001",
                "message": "I ordered the navy Harbor jacket in L and got a bright yellow one. I need it for a trip on the 30th.",
                "channel": "email",
            });
            if let Some(fields) = fields.as_object() {
                ticket.merge(fields);
            }
            tickets.insert(&ticket).await?;
            println!("support: seeded a ticket for Omar");
        }
        Ok(())
    }

    async fn bind(
        &self,
        beam: &RiverBeam,
        store: &PgBeamStore,
        _shared: &Shared,
    ) -> Result<Vec<String>, BoxError> {
        // bind_all registered the order system's MCP tools; the agents get
        // support's wrappers over them, not the tools themselves.
        let tickets: Arc<dyn RecordStore> = PgRecords::new(store.clone());
        bind_agents(beam, &tickets).await
    }

    fn routes(&self, ctx: &RouteContext) -> Router {
        let tickets: Arc<dyn RecordStore> = PgRecords::new(ctx.store.clone());
        Router::new()
            .route("/api/support/tickets", get(list).post(create))
            .route("/api/support/tickets/{id}", get(read))
            .with_state(tickets)
    }
}

/// Every ticket for a support agent; a customer's own otherwise.
async fn list(
    State(tickets): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    let owner = (!who.has(AGENT_ROLE)).then_some(who.user.as_str());
    match tickets.list(SHOWCASE, TICKET, owner).await {
        Ok(rows) => Json(json!(rows)).into_response(),
        Err(e) => problem(&e),
    }
}

async fn read(
    State(tickets): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<Uuid>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    match require(tickets.as_ref(), SHOWCASE, id).await {
        Ok(t) if t.owner == who.user || who.has(AGENT_ROLE) => {
            let mut out = json!(t);
            out["view"] = ticket_view(&t);
            Json(out).into_response()
        }
        Ok(_) => problem(&SHC_002.with_data(format!("ticket {id} was not found"))),
        Err(e) => problem(&e),
    }
}

#[derive(Deserialize)]
struct NewTicket {
    subject: String,
    message: String,
    #[serde(default)]
    order_id: Option<String>,
}

/// A customer opens a ticket from the form, without the assistant.
async fn create(
    State(tickets): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
    Json(body): Json<NewTicket>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    if body.subject.trim().is_empty() || body.message.trim().is_empty() {
        return problem(&crate::errors::SHC_005.with_data("subject and message are required"));
    }
    let mut ticket = Record::new(SHOWCASE, TICKET, &who.user, who.org.as_deref(), body.subject.trim());
    let mut data = Map::new();
    data.insert("category".into(), json!("other"));
    data.insert("message".into(), Value::String(body.message.trim().into()));
    if let Some(order) = body.order_id.filter(|o| !o.trim().is_empty()) {
        data.insert("order_id".into(), json!(order.trim().to_uppercase()));
    }
    data.insert("channel".into(), json!("form"));
    ticket.merge(&data);
    match tickets.insert(&ticket).await {
        Ok(()) => (StatusCode::CREATED, Json(json!(ticket))).into_response(),
        Err(e) => problem(&e),
    }
}
