//! Claims: auto insurance claims at "Harbor Mutual".
//!
//! Policyholders report a loss to `claims_intake`, which reads their policy,
//! asks for the missing facts and a damage estimate with client tools, saves
//! them on a draft claim and files it. `damage_reader` reads the uploaded
//! photo or body-shop estimate (vision, structured output). Adjusters work
//! the queue with `claim_review`, a step runtime: it applies the coverage
//! rules and fraud signals, asks the adjuster to decide exceptions, takes
//! their note, and pays out only after the adjuster approves `approve_payout`.

pub mod agents;
pub mod personas;
pub mod policies;
pub mod review;
pub mod rules;
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

use crate::errors::{SHC_002, SHC_003, SHC_005};
use crate::kit::{caller, ingest_org_documents, problem, shared_agent_tools, REQUEST_DOCUMENT, REQUEST_INPUT};
use crate::records::{require, PgRecords, Record, RecordStore};
use crate::registry::{BoxError, Persona, RouteContext, Shared, Showcase};
use personas::{ADJUSTER_ROLE, NINA, ORG, PERSONAS};
use policies::policy_of;
use tools::{claim_view, register_claims_tools, INTAKE_TOOLS};

/// The showcase's key.
pub const SHOWCASE: &str = "claims";
/// Its records' kind.
pub const CLAIM: &str = "claim";

/// The policy wording, how a claim works and the FAQ, for RAG:
/// `(file, title, text)`.
pub const POLICY_DOCS: [(&str, &str, &str); 3] = [
    (
        "policy_wording.md",
        "Auto policy wording",
        include_str!("../../knowledge/claims/policy_wording.md"),
    ),
    (
        "how_claims_work.md",
        "How a claim works",
        include_str!("../../knowledge/claims/how_claims_work.md"),
    ),
    ("faq.md", "Claims FAQ", include_str!("../../knowledge/claims/faq.md")),
];

/// Register the claims tools and bind the three agents on `beam`. Returns
/// the tools it registered. Tests call it with an in-memory store.
///
/// # Errors
/// The tool runtime is missing, or the policy documents cannot be ingested.
pub async fn bind_agents(beam: &RiverBeam, claims: &Arc<dyn RecordStore>) -> Result<Vec<String>, BoxError> {
    let registry = beam
        .tools()
        .ok_or("the claims showcase needs the tool runtime")?
        .registry()
        .clone();
    let owned = register_claims_tools(&registry, claims);
    let shared = shared_agent_tools(beam, &registry);
    if let Some(knowledge) = beam.knowledge() {
        ingest_org_documents(knowledge, ORG, &POLICY_DOCS).await?;
    }
    let mut intake: Vec<String> = INTAKE_TOOLS.iter().map(|t| (*t).to_owned()).collect();
    intake.extend([REQUEST_INPUT.to_owned(), REQUEST_DOCUMENT.to_owned()]);
    intake.extend(shared);
    beam.bind_rig_agent(agents::intake_metadata(), agents::intake_def(intake));
    beam.bind_rig_agent(agents::damage_metadata(), agents::damage_def());
    beam.bind_runtime(
        agents::review_metadata(),
        Arc::new(review::Review::new(beam.runtime_states().clone(), claims.clone())),
    );
    Ok(owned)
}

/// The claims showcase in the showcase server.
pub struct ClaimsShowcase;

#[async_trait(?Send)]
impl Showcase for ClaimsShowcase {
    fn key(&self) -> &'static str {
        SHOWCASE
    }

    fn title(&self) -> &'static str {
        "Claims"
    }

    fn tagline(&self) -> &'static str {
        "Auto claims: first notice of loss, damage estimates read by the vision model, a coverage review and approved payouts"
    }

    fn org(&self) -> &'static str {
        ORG
    }

    fn personas(&self) -> Vec<Persona> {
        PERSONAS.to_vec()
    }

    fn agents(&self) -> &'static [&'static str] {
        &[agents::CLAIMS_INTAKE, agents::DAMAGE_READER, agents::CLAIM_REVIEW]
    }

    fn api_prefix(&self) -> &'static str {
        "/api/claims/"
    }

    async fn migrate(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        PgRecords::new(store.clone()).migrate().await?;
        Ok(())
    }

    /// A filed claim for Nina Alvarez (rear-ended at a light), so the
    /// adjuster's queue is not empty.
    async fn seed(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        let claims = PgRecords::new(store.clone());
        if claims.list(SHOWCASE, CLAIM, Some(NINA)).await?.is_empty() {
            claims.insert(&seeded_claim()).await?;
            println!("claims: seeded a filed claim for Nina Alvarez");
        }
        Ok(())
    }

    async fn bind(
        &self,
        beam: &RiverBeam,
        store: &PgBeamStore,
        _shared: &Shared,
    ) -> Result<Vec<String>, BoxError> {
        let claims: Arc<dyn RecordStore> = PgRecords::new(store.clone());
        bind_agents(beam, &claims).await
    }

    fn routes(&self, ctx: &RouteContext) -> Router {
        let claims: Arc<dyn RecordStore> = PgRecords::new(ctx.store.clone());
        Router::new()
            .route("/api/claims/claims", get(list))
            .route("/api/claims/claims/{id}", get(read))
            .route("/api/claims/claims/{id}/documents", axum::routing::post(add_document))
            .route("/api/claims/policies/mine", get(my_policy))
            .with_state(claims)
    }
}

/// Nina's claim: rear-ended at a light, police report present, a $3,850
/// moderate estimate, filed and waiting for an adjuster.
#[must_use]
pub fn seeded_claim() -> Record {
    let mut claim = Record::new(SHOWCASE, CLAIM, NINA, Some(ORG), "Rear-ended at a light");
    claim.status = "filed".into();
    claim.merge(
        json!({
            "policy_number": "AUT-48377",
            "vehicle": "2019 Toyota RAV4",
            "loss_type": "collision",
            "loss_date": "2026-10-09",
            "location": "Market St and 4th Ave",
            "description": "Stopped at a red light and was rear-ended by a car that did not brake in time.",
            "other_party": "Driver of a 2019 Honda Civic",
            "police_report": "PR-2026-88213",
            "drivable": true,
            "estimated_repair_cost": 3850.0,
            "severity": "moderate",
            "documents": [{
                "file_name": "bayside-estimate.png",
                "document_type": "damage_photo",
                "extracted": {
                    "vehicle": "2019 Toyota RAV4",
                    "damaged_parts": [
                        {"part": "rear bumper cover", "action": "replace", "cost": 720.0},
                        {"part": "rear body panel", "action": "repair", "cost": 1180.0},
                        {"part": "tail lamp", "action": "replace", "cost": 340.0},
                        {"part": "paint and materials", "action": "repair", "cost": 760.0}
                    ],
                    "labor_hours": 9.0,
                    "estimated_repair_cost": 3850.0,
                    "drivable": true,
                    "severity": "moderate",
                    "confidence": 0.95
                }
            }],
            "filed_at": "2026-10-10T16:20:00Z",
        })
        .as_object()
        .unwrap_or(&Map::new()),
    );
    claim
}

/// Every claim for an adjuster; a policyholder's own otherwise.
async fn list(
    State(claims): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    let owner = (!who.has(ADJUSTER_ROLE)).then_some(who.user.as_str());
    match claims.list(SHOWCASE, CLAIM, owner).await {
        Ok(rows) => Json(json!(rows)).into_response(),
        Err(e) => problem(&e),
    }
}

async fn read(
    State(claims): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<Uuid>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    match require(claims.as_ref(), SHOWCASE, id).await {
        Ok(c) if c.owner == who.user || who.has(ADJUSTER_ROLE) => {
            let mut out = json!(c);
            out["view"] = claim_view(&c);
            Json(out).into_response()
        }
        Ok(_) => problem(&SHC_002.with_data(format!("claim {id} was not found"))),
        Err(e) => problem(&e),
    }
}

#[derive(Deserialize)]
struct NewDocument {
    file_name: String,
    document_type: String,
    #[serde(default)]
    extracted: Value,
}

/// The owner adds an uploaded document (what the UI's document card read) to
/// their draft claim.
async fn add_document(
    State(claims): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<Uuid>,
    Json(body): Json<NewDocument>,
) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    let mut claim = match require(claims.as_ref(), SHOWCASE, id).await {
        Ok(c) if c.owner == who.user => c,
        Ok(_) => return problem(&SHC_002.with_data(format!("claim {id} was not found"))),
        Err(e) => return problem(&e),
    };
    if claim.status != "draft" {
        return problem(&SHC_003.with_data(format!("the claim is {}; documents go on a draft", claim.status)));
    }
    if body.file_name.trim().is_empty() || body.document_type.trim().is_empty() {
        return problem(&SHC_005.with_data("file_name and document_type are required"));
    }
    let mut documents = claim
        .field("documents")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    documents.push(json!({
        "file_name": body.file_name.trim(),
        "document_type": body.document_type.trim(),
        "extracted": body.extracted,
    }));
    claim.data.insert("documents".into(), Value::Array(documents));
    match claims.save(&mut claim).await {
        Ok(()) => (StatusCode::CREATED, Json(json!(claim))).into_response(),
        Err(e) => problem(&e),
    }
}

/// The caller's own policy.
async fn my_policy(principal: Option<Extension<Principal>>) -> Response {
    let who = match caller(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    match policy_of(&who.user) {
        Some(policy) => Json(policy.view()).into_response(),
        None => problem(&SHC_002.with_data("no policy was found for the caller")),
    }
}
