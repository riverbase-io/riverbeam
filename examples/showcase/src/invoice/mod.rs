//! Accounts payable: invoices read by the vision model, a three-way match
//! against the ERP, and a controller's approval to pay.
//!
//! The AP clerk uploads a vendor invoice; `invoice_reader` reads the image
//! into structured data. `ap_match`, a step runtime, pulls the purchase order,
//! goods receipt and vendor from the ERP (MCP) and applies the AP policy's
//! rules: vendor, quantity received, price within 2%, total within $5, no
//! duplicate. A clean invoice is matched and the clerk approves its payment.
//! An invoice with exceptions goes to the controller, who decides (pay,
//! pay the matched amount, reject, hold) and approves the payment: a clerk
//! cannot approve exceptions.

pub mod agents;
pub mod matching;
pub mod personas;
pub mod runtime;
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
use serde_json::{json, Value};
use uuid::Uuid;

use crate::errors::{SHC_002, SHC_004};
use crate::kit::{caller, ingest_org_documents, problem, Caller};
use crate::records::{require, PgRecords, Record, RecordStore};
use crate::registry::{BoxError, Persona, RouteContext, Shared, Showcase};
use personas::{AP_ROLE, NORA, ORG, PERSONAS};
use tools::{invoice_view, receive_invoice, register_invoice_tools};

/// The showcase's key.
pub const SHOWCASE: &str = "invoice";
/// Its records' kind.
pub const INVOICE: &str = "invoice";

/// The AP policy and notes, for RAG: `(file, title, text)`.
pub const POLICY: [(&str, &str, &str); 3] = [
    ("ap-policy.md", "Accounts payable policy", include_str!("../../knowledge/invoice/ap-policy.md")),
    ("vendors.md", "Vendor onboarding and bank changes", include_str!("../../knowledge/invoice/vendors.md")),
    ("month-end.md", "Month-end close notes", include_str!("../../knowledge/invoice/month-end.md")),
];

/// Delta Office Supply's invoice for PO-4520, which matches cleanly: it
/// waits in the queue as `received`, so there is something to match before
/// anyone uploads.
#[must_use]
pub fn delta_invoice(owner: &str, org: Option<&str>) -> Record {
    let mut invoice = Record::new(SHOWCASE, INVOICE, owner, org, "Delta Office Supply INV-5531");
    invoice.status = "received".into();
    invoice.merge(
        json!({
            "vendor_name": "Delta Office Supply",
            "invoice_number": "INV-5531",
            "invoice_date": "2026-10-12",
            "po_number": "PO-4520",
            "currency": "USD",
            "lines": [
                {"sku": "PP-500", "description": "Copy paper, 500 sheets", "quantity": 40, "unit_price": 4.25, "amount": 170.0},
                {"sku": "PN-12", "description": "Ballpoint pens, box of 12", "quantity": 12, "unit_price": 8.0, "amount": 96.0}
            ],
            "subtotal": 266.0,
            "tax": 21.28,
            "total": 287.28,
            "confidence": 1.0,
            "file_name": "delta-inv-5531.png",
        })
        .as_object()
        .unwrap_or(&serde_json::Map::new()),
    );
    invoice
}

/// Register the accounts payable tools and bind both agents on `beam`.
/// Returns the tools it registered. Tests call it with an in-memory store.
///
/// # Errors
/// The tool runtime is missing, or the policy cannot be ingested.
pub async fn bind_agents(beam: &RiverBeam, invoices: &Arc<dyn RecordStore>) -> Result<Vec<String>, BoxError> {
    let registry = beam
        .tools()
        .ok_or("accounts payable needs the tool runtime")?
        .registry()
        .clone();
    let owned = register_invoice_tools(&registry, invoices);
    if let Some(knowledge) = beam.knowledge() {
        ingest_org_documents(knowledge, ORG, &POLICY).await?;
    }
    beam.bind_rig_agent(agents::reader_metadata(), agents::reader_def());
    beam.bind_runtime(
        agents::match_metadata(),
        Arc::new(runtime::ApMatch::new(beam.runtime_states().clone(), invoices.clone())),
    );
    Ok(owned)
}

/// Accounts payable in the showcase server.
pub struct InvoiceShowcase;

#[async_trait(?Send)]
impl Showcase for InvoiceShowcase {
    fn key(&self) -> &'static str {
        SHOWCASE
    }

    fn title(&self) -> &'static str {
        "Accounts payable"
    }

    fn tagline(&self) -> &'static str {
        "Accounts payable: invoices read by the vision model, a three-way match against the ERP, and a controller's approval to pay"
    }

    fn org(&self) -> &'static str {
        ORG
    }

    fn personas(&self) -> Vec<Persona> {
        PERSONAS.to_vec()
    }

    fn agents(&self) -> &'static [&'static str] {
        &[agents::INVOICE_READER, agents::AP_MATCH]
    }

    fn api_prefix(&self) -> &'static str {
        "/api/invoice/"
    }

    async fn migrate(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        PgRecords::new(store.clone()).migrate().await?;
        Ok(())
    }

    /// Delta Office Supply's clean invoice, received and waiting to be matched.
    async fn seed(&self, store: &PgBeamStore) -> Result<(), BoxError> {
        let invoices = PgRecords::new(store.clone());
        if invoices.list(SHOWCASE, INVOICE, None).await?.is_empty() {
            invoices.insert(&delta_invoice(NORA, Some(ORG))).await?;
            println!("invoice: seeded Delta Office Supply INV-5531 for PO-4520");
        }
        Ok(())
    }

    async fn bind(
        &self,
        beam: &RiverBeam,
        store: &PgBeamStore,
        shared: &Shared,
    ) -> Result<Vec<String>, BoxError> {
        if shared.mcp_tools_of("erp").len() < 3 {
            eprintln!("warning: the ERP's MCP tools are not bridged; ap_match cannot read purchase orders");
        }
        let invoices: Arc<dyn RecordStore> = PgRecords::new(store.clone());
        bind_agents(beam, &invoices).await
    }

    fn routes(&self, ctx: &RouteContext) -> Router {
        let invoices: Arc<dyn RecordStore> = PgRecords::new(ctx.store.clone());
        Router::new()
            .route("/api/invoice/invoices", get(list).post(create))
            .route("/api/invoice/invoices/{id}", get(read))
            .with_state(invoices)
    }
}

/// The caller, if signed in and AP staff; else the response to send.
fn staff(principal: Option<Extension<Principal>>) -> Result<Caller, Response> {
    let who = caller(principal)?;
    if who.has(AP_ROLE) {
        Ok(who)
    } else {
        Err(problem(&SHC_004.with_data("only accounts payable staff reach invoices")))
    }
}

/// Every invoice, for AP staff.
async fn list(
    State(invoices): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
) -> Response {
    if let Err(response) = staff(principal) {
        return response;
    }
    match invoices.list(SHOWCASE, INVOICE, None).await {
        Ok(rows) => Json(json!(rows)).into_response(),
        Err(e) => problem(&e),
    }
}

async fn read(
    State(invoices): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Err(response) = staff(principal) {
        return response;
    }
    match require(invoices.as_ref(), SHOWCASE, id).await {
        Ok(invoice) if invoice.kind == INVOICE => {
            let mut out = json!(invoice);
            out["view"] = invoice_view(&invoice);
            Json(out).into_response()
        }
        Ok(_) => problem(&SHC_002.with_data(format!("invoice {id} was not found"))),
        Err(e) => problem(&e),
    }
}

#[derive(Deserialize)]
struct NewInvoice {
    file_name: String,
    /// What `invoice_reader` returned for the image.
    extracted: Value,
}

/// The clerk's upload: the invoice the reader read becomes a `received`
/// record. The same invoice twice is refused (`SHC-003`).
async fn create(
    State(invoices): State<Arc<dyn RecordStore>>,
    principal: Option<Extension<Principal>>,
    Json(body): Json<NewInvoice>,
) -> Response {
    let who = match staff(principal) {
        Ok(who) => who,
        Err(response) => return response,
    };
    match receive_invoice(invoices.as_ref(), &who.user, who.org.as_deref(), body.file_name.trim(), &body.extracted).await {
        Ok(invoice) => (StatusCode::CREATED, Json(json!(invoice))).into_response(),
        Err(e) => problem(&e),
    }
}
