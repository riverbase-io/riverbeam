//! The loan desk's own HTTP routes, under `/api/loan/…`: what the UI reads
//! and writes besides the beam domain. Each route checks the caller itself
//! (riverbase registers them public, as `crate::host`'s aliases are).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use crate::host::http::{principal_roles, principal_user};
use beam_domain::{BeamSql, DomainStore};
use riverbase_core::RiverbaseError;
use riverbase_http::Principal;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::loan_desk::admin::{smoke_dataset, sync_holds};
use crate::loan_desk::data::{require, Application, ApplicationStore};
use crate::loan_desk::errors::{LND_005, LND_006, LND_007};
use crate::loan_desk::personas::{role_of, Role, ORG, PERSONAS};
use crate::loan_desk::programs::{program, PROGRAMS};
use crate::loan_desk::tools::may_access;

/// What the routes read.
pub struct ApiState {
    pub applications: Arc<dyn ApplicationStore>,
    /// The application packets (sandbox and git), once the pre-check is on.
    pub packets: Option<Arc<crate::loan_desk::packet::Packets>>,
    /// The beam whose hold book mirrors legal holds (the embedded worker's;
    /// a separate worker syncs its own at startup).
    pub beam: Option<Arc<beam_core::RiverBeam>>,
    /// Each persona's usage today (the admin's view of `.usage`).
    pub usage: Option<DomainStore>,
}

/// The routes; every path starts with `/api/loan/`.
pub fn router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/loan/personas", get(personas))
        .route("/api/loan/programs", get(programs))
        .route("/api/loan/applications", get(list).post(create))
        .route("/api/loan/applications/{id}", get(read).patch(update))
        .route("/api/loan/applications/{id}/documents", post(add_document))
        .route("/api/loan/applications/{id}/hold", post(hold))
        .route("/api/loan/applications/{id}/packet", get(packet))
        .route("/api/loan/erase", post(erase))
        .route("/api/loan/evals", get(evals))
        .route("/api/loan/usage", get(usage))
        .with_state(state)
}

fn problem(err: &RiverbaseError) -> Response {
    let status = StatusCode::from_u16(err.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let detail = err.errdata.get("detail").cloned().unwrap_or(Value::Null);
    (
        status,
        Json(json!({"errcode": err.errcode.as_str(), "errmesg": err.errmesg, "detail": detail})),
    )
        .into_response()
}

struct Caller {
    user: String,
    org: Option<String>,
    role: Role,
}

fn caller(principal: Option<Extension<Principal>>) -> Result<Caller, Response> {
    let Some(Extension(principal)) = principal else {
        return Err((StatusCode::UNAUTHORIZED, "sign in first").into_response());
    };
    Ok(Caller {
        user: principal_user(&principal),
        org: principal
            .claims
            .get("organization_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        role: role_of(&principal_roles(&principal)),
    })
}

async fn personas() -> Json<Value> {
    Json(json!({"org": ORG, "personas": PERSONAS}))
}

async fn programs() -> Json<Value> {
    Json(json!(PROGRAMS))
}

async fn list(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    let owner = (who.role == Role::Borrower).then_some(who.user.as_str());
    match state.applications.list(owner).await {
        Ok(rows) => Json(json!(rows.iter().map(view).collect::<Vec<_>>())).into_response(),
        Err(e) => problem(&e),
    }
}

/// An application with what it still misses.
fn view(application: &Application) -> Value {
    let mut out = json!(application);
    out["missing_fields"] = json!(application.missing_fields());
    out["has_pay_stub"] = json!(application.has_pay_stub());
    out
}

#[derive(Deserialize)]
struct NewApplication {
    #[serde(default)]
    program_id: Option<String>,
}

async fn create(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Json(body): Json<NewApplication>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    let program_id = body.program_id.unwrap_or_else(|| "conventional".into());
    if program(&program_id).is_none() {
        return problem(&LND_006.with_data(program_id));
    }
    let application = Application::new(&who.user, who.org.as_deref(), &program_id);
    match state.applications.insert(&application).await {
        Ok(()) => (StatusCode::CREATED, Json(view(&application))).into_response(),
        Err(e) => problem(&e),
    }
}

async fn load(state: &ApiState, who: &Caller, id: &str) -> Result<Application, Response> {
    let id = Uuid::parse_str(id).map_err(|_| problem(&LND_007.with_data("not a UUID")))?;
    let application = require(state.applications.as_ref(), id)
        .await
        .map_err(|e| problem(&e))?;
    if may_access(Some(&who.user), &application) {
        Ok(application)
    } else {
        Err(problem(
            &crate::loan_desk::errors::LND_002.with_data(format!("application {id} was not found")),
        ))
    }
}

async fn read(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    match load(&state, &who, &id).await {
        Ok(application) => Json(view(&application)).into_response(),
        Err(response) => response,
    }
}

#[derive(Deserialize)]
struct Fields {
    fields: Map<String, Value>,
}

async fn update(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<String>,
    Json(body): Json<Fields>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    let mut application = match load(&state, &who, &id).await {
        Ok(application) => application,
        Err(response) => return response,
    };
    if let Err(e) = application.merge_fields(&body.fields) {
        return problem(&e);
    }
    match state.applications.save(&mut application).await {
        Ok(()) => Json(view(&application)).into_response(),
        Err(e) => problem(&e),
    }
}

async fn add_document(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<String>,
    Json(document): Json<Value>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    if !document.is_object() || document.get("file_name").and_then(Value::as_str).is_none() {
        return problem(&LND_007.with_data("a document needs file_name"));
    }
    let mut application = match load(&state, &who, &id).await {
        Ok(application) => application,
        Err(response) => return response,
    };
    application.documents.push(document);
    match state.applications.save(&mut application).await {
        Ok(()) => Json(view(&application)).into_response(),
        Err(e) => problem(&e),
    }
}

#[derive(Deserialize)]
struct Hold {
    held: bool,
}

async fn hold(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<String>,
    Json(body): Json<Hold>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    if who.role != Role::Admin {
        return problem(&LND_005.with_data("a legal hold is an admin's"));
    }
    let mut application = match load(&state, &who, &id).await {
        Ok(application) => application,
        Err(response) => return response,
    };
    application.held = body.held;
    if let Err(e) = state.applications.save(&mut application).await {
        return problem(&e);
    }
    if let Some(beam) = &state.beam {
        let owner = application.owner.clone();
        if let Err(e) = sync_holds(state.applications.as_ref(), beam, Some(&owner)).await {
            return problem(&e);
        }
    }
    Json(view(&application)).into_response()
}

#[derive(Deserialize)]
struct Erase {
    user_id: String,
}

/// Erase a borrower's applications (not held ones). Their beam data goes
/// through the domain's `delete-user-agent-data` command, which the UI sends
/// only when `held_applications` is 0 (the beam refuses a held user too).
async fn erase(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Json(body): Json<Erase>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    if who.role != Role::Admin {
        return problem(&LND_005.with_data("erasing a borrower is an admin's"));
    }
    let erased = match state.applications.erase_owner(&body.user_id).await {
        Ok(erased) => erased,
        Err(e) => return problem(&e),
    };
    match state.applications.list(Some(&body.user_id)).await {
        Ok(kept) => Json(json!({
            "erased_applications": erased,
            "held_applications": kept.iter().filter(|a| a.held).count(),
        }))
        .into_response(),
        Err(e) => problem(&e),
    }
}

/// The eval dataset the admin runs against a revision.
async fn evals(principal: Option<Extension<Principal>>) -> Response {
    match caller(principal) {
        Ok(who) if who.role == Role::Admin => Json(json!([smoke_dataset()])).into_response(),
        Ok(_) => problem(&LND_005.with_data("evaluations are an admin's")),
        Err(response) => response,
    }
}

/// Each persona's usage summary today, as their own `.usage` reports it.
async fn usage(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
) -> Response {
    match caller(principal) {
        Ok(who) if who.role == Role::Admin => {}
        Ok(_) => return problem(&LND_005.with_data("usage of every persona is an admin's")),
        Err(response) => return response,
    }
    let Some(store) = &state.usage else {
        return Json(json!([])).into_response();
    };
    let mut rows = Vec::new();
    let settings = state.beam.as_deref().map_or(
        beam_domain::UsageSummarySettings {
            enabled: true,
            default_plan: Some(crate::host::plans::DEFAULT_PLAN.to_owned()),
        },
        crate::host::http::usage_settings,
    );
    for persona in &PERSONAS {
        match store.usage_summary(persona.user_id, None, &settings).await {
            Ok(summary) => rows.push(json!({"persona": persona, "usage": summary})),
            Err(e) => return problem(&e),
        }
    }
    Json(Value::Array(rows)).into_response()
}

async fn packet(
    State(state): State<Arc<ApiState>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    if who.role == Role::Borrower {
        return problem(&LND_005.with_data("the packet is the officer's"));
    }
    let application = match load(&state, &who, &id).await {
        Ok(application) => application,
        Err(response) => return response,
    };
    let Some(packets) = &state.packets else {
        return Json(json!({"files": [], "log": []})).into_response();
    };
    match packets.read(application.id).await {
        Ok(view) => Json(view).into_response(),
        Err(e) => problem(&e),
    }
}
