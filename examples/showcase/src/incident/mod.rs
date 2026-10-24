//! Incident desk: on-call incident response for the platform team of the
//! online store "Northwind".
//!
//! `incident_responder` reads alerts, metrics and deploys from monitoring
//! (MCP), opens an incident from an alert, follows the runbooks (RAG) and
//! keeps the incident's timeline. A rollback or a restart is high-risk: the
//! on-call SRE approves each call (both are simulated; they record the action
//! and mark the incident mitigated). The incident commander has the agent
//! write the postmortem, which is committed to a git working tree.

pub mod agents;
pub mod personas;
pub mod postmortem;
pub mod stub;
pub mod tools;

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use beam_core::RiverBeam;
use beam_store::PgBeamStore;
use chrono::{Duration, SecondsFormat, Utc};
use riverbase_http::Principal;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::errors::{SHC_002, SHC_004};
use crate::kit::{caller, ingest_org_documents, problem, shared_agent_tools};
use crate::records::{require, PgRecords, Record, RecordStore};
use crate::registry::{BoxError, Persona, RouteContext, Shared, Showcase};
use personas::{KAI, ORG, PERSONAS, SRE_ROLE};
use postmortem::Postmortems;
use tools::{incident_view, register_incident_tools, RESPONDER_TOOLS, WRITE_POSTMORTEM};

/// The showcase's key.
pub const SHOWCASE: &str = "incident";
/// Its records' kind.
pub const INCIDENT: &str = "incident";

/// The runbooks and policy, for RAG: `(file, title, text)`.
pub const RUNBOOKS: [(&str, &str, &str); 5] = [
    (
        "high-latency-after-a-deploy.md",
        "Runbook: high latency after a deploy",
        include_str!("../../knowledge/incident/high-latency-after-a-deploy.md"),
    ),
    (
        "elevated-5xx.md",
        "Runbook: elevated 5xx",
        include_str!("../../knowledge/incident/elevated-5xx.md"),
    ),
    (
        "restart-a-service.md",
        "Runbook: restart a service safely",
        include_str!("../../knowledge/incident/restart-a-service.md"),
    ),
    (
        "on-call-policy.md",
        "On-call policy",
        include_str!("../../knowledge/incident/on-call-policy.md"),
    ),
    (
        "postmortem-template.md",
        "Postmortem template",
        include_str!("../../knowledge/incident/postmortem-template.md"),
    ),
];

/// Register the incident tools and bind the responder on `beam`. Returns
/// the tools it registered. `postmortems` is the postmortem repository;
/// without it there is no `write_postmortem`. Tests call it with an
/// in-memory store.
///
/// # Errors
/// The tool runtime is missing, or the runbooks cannot be ingested.
pub async fn bind_agents(
    beam: &RiverBeam,
    incidents: &Arc<dyn RecordStore>,
    postmortems: Option<Arc<Postmortems>>,
    mcp_tools: &[String],
) -> Result<Vec<String>, BoxError> {
    let registry = beam
        .tools()
        .ok_or("the incident desk needs the tool runtime")?
        .registry()
        .clone();
    let owned = register_incident_tools(&registry, incidents, postmortems);
    let shared = shared_agent_tools(beam, &registry);
    if let Some(knowledge) = beam.knowledge() {
        ingest_org_documents(knowledge, ORG, &RUNBOOKS).await?;
    }
    let mut responder: Vec<String> = RESPONDER_TOOLS.iter().map(|t| (*t).to_owned()).collect();
    if owned.iter().any(|t| t == WRITE_POSTMORTEM) {
        responder.push(WRITE_POSTMORTEM.to_owned());
    }
    responder.extend(shared.into_iter().filter(|t| t == beam_tools::KNOWLEDGE_SEARCH));
    responder.extend(mcp_tools.iter().cloned());
    beam.bind_rig_agent(agents::responder_metadata(), agents::responder_def(responder));
    Ok(owned)
}

/// The incident desk in the showcase server.
pub struct IncidentShowcase;

#[async_trait(?Send)]
impl Showcase for IncidentShowcase {
    fn key(&self) -> &'static str {
        SHOWCASE
    }

    fn title(&self) -> &'static str {
        "Incident desk"
    }

    fn tagline(&self) -> &'static str {
        "On-call: alerts and metrics from monitoring, runbooks, approved remediations and a committed postmortem"
    }

    fn org(&self) -> &'static str {
        ORG
    }

    fn personas(&self) -> Vec<Persona> {
        PERSONAS.to_vec()
    }

    fn agents(&self) -> &'static [&'static str] {
        &[agents::INCIDENT_RESPONDER]
    }

    fn api_prefix(&self) -> &'static str {
        "/api/incident/"
    }

    async fn migrate(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        PgRecords::new(store.clone()).migrate().await?;
        Ok(())
    }

    /// A resolved incident from three days ago (a search-api memory leak), so
    /// the history is not empty. The checkout alert is left for Kai to open.
    async fn seed(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        let incidents = PgRecords::new(store.clone());
        if incidents.list(SHOWCASE, INCIDENT, None).await?.is_empty() {
            incidents.insert(&past_incident()).await?;
            println!("incident: seeded a resolved search-api incident");
        }
        Ok(())
    }

    async fn bind(
        &self,
        beam: &RiverBeam,
        store: &PgBeamStore,
        shared: &Shared,
    ) -> Result<Vec<String>, BoxError> {
        let incidents: Arc<dyn RecordStore> = PgRecords::new(store.clone());
        let postmortems = match Postmortems::from_env() {
            Ok(postmortems) => Some(postmortems),
            Err(err) => {
                eprintln!("warning: postmortem repository unavailable ({err}); no postmortems");
                None
            }
        };
        bind_agents(beam, &incidents, postmortems, &shared.mcp_tools_of("monitoring")).await
    }

    fn routes(&self, ctx: &RouteContext) -> Router {
        let postmortems = match Postmortems::from_env() {
            Ok(postmortems) => Some(postmortems),
            Err(err) => {
                eprintln!("warning: postmortem repository unavailable ({err}); postmortems are not shown");
                None
            }
        };
        let state = Arc::new(ApiState {
            incidents: PgRecords::new(ctx.store.clone()),
            postmortems,
        });
        Router::new()
            .route("/api/incident/incidents", get(list))
            .route("/api/incident/incidents/{id}", get(read))
            .route("/api/incident/incidents/{id}/postmortem", get(postmortem_files))
            .with_state(state)
    }
}

/// A resolved incident from three days ago: a search-api memory leak.
fn past_incident() -> Record {
    let opened = Utc::now() - Duration::days(3);
    let at = |minutes: i64| (opened + Duration::minutes(minutes)).to_rfc3339_opts(SecondsFormat::Secs, true);
    let summary = "search-api: memory above 90% and pods restarting every 20 minutes";
    let mut record = Record::new(SHOWCASE, INCIDENT, KAI, Some(ORG), summary);
    record.created_at = opened;
    record.updated_at = opened + Duration::minutes(95);
    record.status = "resolved".into();
    record.data.insert("service".into(), json!("search-api"));
    record.data.insert("severity".into(), json!("high"));
    record.data.insert("summary".into(), json!(summary));
    record.data.insert(
        "timeline".into(),
        json!([
            {"at": at(0), "who": "Kai Sato", "entry": format!("Opened a high incident: {summary}")},
            {"at": at(12), "who": "Kai Sato", "entry": "Memory grows about 2% a minute after each restart; v5.8.1 deployed 3 hours earlier raised the result cache size."},
            {"at": at(40), "who": "Kai Sato", "entry": "Restarted search-api to clear the memory while the cache change is reviewed."},
            {"at": at(95), "who": "Kai Sato", "entry": "Resolved: the v5.8.1 cache never evicted old entries; the cache size was capped in v5.8.2 and memory is flat."},
        ]),
    );
    record.data.insert(
        "remediation".into(),
        json!({"action": "restart_service", "service": "search-api", "reason": "clear the memory while the cache change is reviewed",
            "simulated": true, "at": at(40)}),
    );
    record.data.insert(
        "resolution".into(),
        json!("The v5.8.1 cache never evicted old entries; the cache size was capped in v5.8.2 and memory is flat."),
    );
    record
}

struct ApiState {
    incidents: Arc<dyn RecordStore>,
    postmortems: Option<Arc<Postmortems>>,
}

/// The caller, if they are on call (`sre`); else the response to send.
fn on_call(principal: Option<Extension<Principal>>) -> Result<(), Response> {
    let who = caller(principal)?;
    if who.has(SRE_ROLE) {
        Ok(())
    } else {
        Err(problem(&SHC_004.with_data("only the on-call team sees incidents")))
    }
}

/// Every incident, newest first.
async fn list(State(state): State<Arc<ApiState>>, principal: Option<Extension<Principal>>) -> Response {
    if let Err(response) = on_call(principal) {
        return response;
    }
    match state.incidents.list(SHOWCASE, INCIDENT, None).await {
        Ok(rows) => Json(json!(rows)).into_response(),
        Err(e) => problem(&e),
    }
}

/// The incident `id`, with the view the agent reads.
async fn read(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Err(response) = on_call(principal) {
        return response;
    }
    match require(state.incidents.as_ref(), SHOWCASE, id).await {
        Ok(record) if record.kind == INCIDENT => {
            let mut out = json!(record);
            out["view"] = incident_view(&record);
            Json(out).into_response()
        }
        Ok(_) => problem(&SHC_002.with_data(format!("incident {id} was not found"))),
        Err(e) => problem(&e),
    }
}

/// The incident's committed postmortem and the commits that name it.
async fn postmortem_files(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Err(response) = on_call(principal) {
        return response;
    }
    let record = match require(state.incidents.as_ref(), SHOWCASE, id).await {
        Ok(record) if record.kind == INCIDENT => record,
        Ok(_) => return problem(&SHC_002.with_data(format!("incident {id} was not found"))),
        Err(e) => return problem(&e),
    };
    let Some(postmortems) = &state.postmortems else {
        return Json(json!({"files": [], "log": []})).into_response();
    };
    let path = record
        .field("postmortem")
        .and_then(|p| p["path"].as_str())
        .map(str::to_owned);
    match postmortems.read(path.as_deref(), record.id).await {
        Ok(view) => Json::<Value>(view).into_response(),
        Err(e) => problem(&e),
    }
}
