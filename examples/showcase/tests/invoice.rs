//! Accounts payable on an in-memory beam with a scripted model: a clean
//! invoice is matched and paid after the clerk approves; the Acme invoice has
//! a price and a quantity exception that the clerk cannot approve and the
//! controller decides; duplicates are refused; and each role reaches only
//! what it may.

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{ToolCallContext, ToolRegistry, ToolRuntime, ToolService};
use beam_types::{
    ActionResponseEnvelope, AgentResult, ContentPart, InvokeAuthContext, InvokeRequest,
    RequestContext, RequestFact,
};
use serde_json::{json, Value};
use showcase::errors::SHC_002;
use showcase::invoice::agents::{reader_def, reader_metadata, AP_MATCH, INVOICE_READER};
use showcase::invoice::personas::{NORA, ORG, RAJ};
use showcase::invoice::runtime::{CHOICES, GET_GOODS_RECEIPT, GET_PURCHASE_ORDER, GET_VENDOR};
use showcase::invoice::stub::{goods_receipt, purchase_order, vendor};
use showcase::invoice::tools::{
    invoice_tools, receive_invoice, GET_INVOICE, HOLD_INVOICE, REJECT_INVOICE, SCHEDULE_PAYMENT,
};
use showcase::invoice::{bind_agents, delta_invoice, INVOICE, SHOWCASE};
use showcase::kit::{object, read_tool};
use showcase::records::{InMemoryRecords, Record, RecordStore};
use showcase::support::personas::MIA;
use uuid::Uuid;

struct Desk {
    beam: RiverBeam,
    invoices: Arc<dyn RecordStore>,
}

/// The ERP, as the MCP bridge would register its three tools.
fn erp_tools() -> Vec<beam_tools::ToolDefinition> {
    vec![
        read_tool(
            GET_PURCHASE_ORDER,
            "A purchase order.",
            object(&json!({"po_number": {"type": "string"}}), &["po_number"]),
            |args, _ctx| async move {
                purchase_order(args["po_number"].as_str().unwrap_or_default())
                    .ok_or_else(|| SHC_002.with_data("no such purchase order"))
            },
        ),
        read_tool(
            GET_GOODS_RECEIPT,
            "A goods receipt.",
            object(&json!({"po_number": {"type": "string"}}), &["po_number"]),
            |args, _ctx| async move {
                goods_receipt(args["po_number"].as_str().unwrap_or_default())
                    .ok_or_else(|| SHC_002.with_data("no such goods receipt"))
            },
        ),
        read_tool(
            GET_VENDOR,
            "A vendor.",
            object(&json!({"vendor_id": {"type": "string"}}), &["vendor_id"]),
            |args, _ctx| async move {
                vendor(args["vendor_id"].as_str().unwrap_or_default())
                    .ok_or_else(|| SHC_002.with_data("no such vendor"))
            },
        ),
    ]
}

async fn desk(turns: Vec<ScriptedTurn>) -> Desk {
    let registry = Arc::new(ToolRegistry::new());
    for tool in erp_tools() {
        registry.register(tool);
    }
    let invoices = InMemoryRecords::shared();
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(ScriptedModel::new(turns)))
        .tools(Arc::new(ToolService::new(registry, Arc::new(ToolRuntime::default()))))
        .build()
        .unwrap();
    let owned = bind_agents(&beam, &invoices).await.unwrap();
    assert!(owned.iter().any(|t| t == SCHEDULE_PAYMENT));
    beam.checkin_catalog().await.unwrap();
    Desk { beam, invoices }
}

fn acme() -> Value {
    json!({
        "vendor_name": "Acme Packaging Co.", "invoice_number": "INV-88213", "invoice_date": "2026-10-20",
        "po_number": "PO-4410", "currency": "USD",
        "lines": [
            {"sku": "BX-18", "description": "Shipping boxes", "quantity": 500, "unit_price": 1.20, "amount": 600.00},
            {"sku": "TP-48", "description": "Packing tape", "quantity": 200, "unit_price": 3.65, "amount": 730.00}
        ],
        "subtotal": 1330.00, "tax": 106.40, "total": 1436.40, "confidence": 0.95
    })
}

fn auth(user: &str) -> InvokeAuthContext {
    InvokeAuthContext {
        profile_id: None,
        user_id: Some(user.into()),
        org_id: Some(ORG.into()),
        mcp_token: None,
    }
}

fn run(user: &str, invoice: Uuid) -> InvokeRequest {
    InvokeRequest {
        agent_name: AP_MATCH.into(),
        content: vec![ContentPart::text("Run the three-way match.")],
        session_id: Uuid::new_v4(),
        model_override: None,
        metadata: json!({}),
        interaction_id: Some(Uuid::new_v4()),
        execution_id: None,
        request_id: Some(Uuid::new_v4()),
        idempotency_key: None,
        stream: false,
        actions: None,
        auth_context: Some(auth(user)),
        context: Some(RequestContext {
            facts: vec![RequestFact {
                key: "invoice_id".into(),
                value: json!(invoice.to_string()),
                expiry: None,
                sensitivity: beam_types::Sensitivity::Internal,
            }],
            ..RequestContext::default()
        }),
        client: None,
    }
}

/// Answer the run's one held action with `payload`.
async fn answer(desk: &Desk, req: &InvokeRequest, held: &AgentResult, payload: Value) -> AgentResult {
    try_answer(desk, req, held, payload).await.unwrap()
}

/// [`answer`], keeping the error.
async fn try_answer(
    desk: &Desk,
    req: &InvokeRequest,
    held: &AgentResult,
    payload: Value,
) -> riverbase_core::RiverbaseResult<AgentResult> {
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
    desk.beam.invoke_result(next).await
}

fn kind(result: &AgentResult) -> String {
    result.action_required.as_ref().unwrap()[0]["action_request"]["kind"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn held_args(result: &AgentResult) -> Value {
    result.action_required.as_ref().unwrap()[0]["action_request"]["payload"]["args"].clone()
}

async fn status_of(desk: &Desk, id: Uuid) -> String {
    desk.invoices.get(id).await.unwrap().unwrap().status
}

#[tokio::test]
async fn a_clean_invoice_is_matched_and_paid_after_the_clerk_approves() {
    let desk = desk(vec![]).await;
    let delta = delta_invoice(NORA, Some(ORG));
    desk.invoices.insert(&delta).await.unwrap();
    let req = run(NORA, delta.id);

    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert!(held.interrupted);
    assert_eq!(kind(&held), "beam.tool_approval");
    let action = &held.action_required.as_ref().unwrap()[0]["action_request"];
    assert_eq!(action["payload"]["tool_name"], SCHEDULE_PAYMENT);
    assert_eq!(action["payload"]["risk"], "high");
    let args = held_args(&held);
    assert_eq!(args["amount"].as_f64(), Some(287.28));
    assert_eq!(args["pay_date"], "2026-10-27", "invoice date plus Net 15");
    let matched = desk.invoices.get(delta.id).await.unwrap().unwrap();
    assert_eq!(matched.status, "matched", "no payment before approval");
    assert_eq!(matched.field("exceptions"), Some(&json!(0)));
    assert!(matched.field("payment").is_none());

    let done = answer(&desk, &req, &held, json!({"approved": true})).await;
    assert!(!done.interrupted);
    assert!(done.text_content().contains("Payment scheduled"), "{}", done.text_content());
    let paid = desk.invoices.get(delta.id).await.unwrap().unwrap();
    assert_eq!(paid.status, "scheduled");
    assert_eq!(paid.data["payment"]["pay_date"], "2026-10-27");
    assert_eq!(paid.data["payment"]["scheduled_by"], NORA);
}

#[tokio::test]
async fn a_declined_payment_leaves_the_invoice_matched() {
    let desk = desk(vec![]).await;
    let delta = delta_invoice(NORA, Some(ORG));
    desk.invoices.insert(&delta).await.unwrap();
    let req = run(NORA, delta.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    let done = answer(&desk, &req, &held, json!({"approved": false})).await;
    assert!(done.text_content().contains("No payment was scheduled"));
    assert_eq!(status_of(&desk, delta.id).await, "matched");
}

#[tokio::test]
async fn the_acme_invoice_goes_to_the_controller_who_decides_and_approves_the_payment() {
    let desk = desk(vec![ScriptedTurn::text(
        "Pay the 500 boxes and the 180 rolls received at the PO price; the rest is disputed.",
    )])
    .await;
    let acme = receive_invoice(desk.invoices.as_ref(), NORA, Some(ORG), "acme.png", &acme())
        .await
        .unwrap();
    assert_eq!(acme.status, "received");
    assert_eq!(acme.title, "Acme Packaging Co. INV-88213");

    // The clerk: exceptions are found, and no card is offered to approve them.
    let by_clerk = desk.beam.invoke_result(run(NORA, acme.id)).await.unwrap();
    assert!(!by_clerk.interrupted, "the clerk cannot decide");
    assert!(by_clerk.action_required.is_none());
    let text = by_clerk.text_content();
    assert!(text.contains("controller"), "{text}");
    assert!(text.contains("unit price TP-48"), "{text}");
    let flagged = desk.invoices.get(acme.id).await.unwrap().unwrap();
    assert_eq!(flagged.status, "pending_approval");
    assert_eq!(flagged.field("exceptions"), Some(&json!(2)));
    let findings = flagged.data["findings"].as_array().unwrap();
    let price = findings.iter().find(|f| f["rule"] == "unit price TP-48").unwrap();
    assert_eq!(price["pass"], false);
    assert!(price["value"].as_str().unwrap().contains("+4.3%"));
    let quantity = findings.iter().find(|f| f["rule"] == "quantity TP-48").unwrap();
    assert_eq!(quantity["value"], "200 invoiced, 180 received");
    assert!((flagged.number("matched_amount").unwrap() - 1328.4).abs() < 1e-6);

    // Running it again as the clerk still offers nothing to approve.
    let again = desk.beam.invoke_result(run(NORA, acme.id)).await.unwrap();
    assert!(again.action_required.is_none());
    assert!(again.text_content().contains("pending the controller"));

    // The controller: choice, note, rationale, then the payment approval.
    let req = run(RAJ, acme.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert_eq!(kind(&held), "beam.choice_question");
    let held = answer(&desk, &req, &held, json!({"selected_choices": [CHOICES[1]]})).await;
    assert_eq!(kind(&held), "beam.form_input");
    let held = answer(&desk, &req, &held, json!({"values": {"note": "Dispute the 20 missing rolls."}})).await;
    assert_eq!(kind(&held), "beam.tool_approval");
    let args = held_args(&held);
    assert!((args["amount"].as_f64().unwrap() - 1328.4).abs() < 1e-6, "{args}");
    assert_eq!(args["pay_date"], "2026-11-19", "invoice date plus Net 30");
    let approved = desk.invoices.get(acme.id).await.unwrap().unwrap();
    assert_eq!(approved.status, "approved", "no payment before approval");
    assert_eq!(approved.data["decision"]["outcome"], "pay_matched");
    assert_eq!(approved.data["decision"]["note"], "Dispute the 20 missing rolls.");
    assert_eq!(approved.data["decision"]["decided_by"], RAJ);
    assert!(approved.data["decision"]["rationale"].as_str().unwrap().contains("disputed"));

    let done = answer(&desk, &req, &held, json!({"approved": true})).await;
    assert!(done.text_content().contains("Payment scheduled"), "{}", done.text_content());
    let paid = desk.invoices.get(acme.id).await.unwrap().unwrap();
    assert_eq!(paid.status, "scheduled");
    assert!((paid.data["payment"]["amount"].as_f64().unwrap() - 1328.4).abs() < 1e-6);
    assert_eq!(paid.data["payment"]["scheduled_by"], RAJ);
}

/// `acme()` under another invoice number.
fn acme_numbered(number: &str) -> Value {
    let mut invoice = acme();
    invoice["invoice_number"] = json!(number);
    invoice
}

/// An answer to a held action of `req`, sent as `user`.
fn answer_as(req: &InvokeRequest, held: &AgentResult, user: &str, payload: &Value) -> InvokeRequest {
    let action = &held.action_required.as_ref().unwrap()[0];
    let mut next = req.clone();
    next.auth_context = Some(auth(user));
    next.request_id = Some(Uuid::new_v4());
    next.content = Vec::new();
    next.execution_id = held.execution_id;
    next.actions = Some(vec![serde_json::from_value(json!({
        "action_id": action["action_id"],
        "lc_action_id": action["lc_action_id"],
        "kind": action["action_request"]["kind"],
        "request_id": action["action_request"]["request_id"],
        "status": "submitted",
        "payload": payload,
    }))
    .unwrap()]);
    next
}

#[tokio::test]
async fn a_clerk_cannot_answer_the_controllers_card() {
    let desk = desk(vec![]).await;
    let invoice = receive_invoice(desk.invoices.as_ref(), NORA, Some(ORG), "acme.png", &acme_numbered("INV-1"))
        .await
        .unwrap();
    // The controller may run the match on a received invoice and decide at once.
    let req = run(RAJ, invoice.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert_eq!(kind(&held), "beam.choice_question");
    assert_eq!(status_of(&desk, invoice.id).await, "pending_approval");

    let as_clerk = answer_as(&req, &held, NORA, &json!({"selected_choices": [CHOICES[0]]}));
    // The beam refuses another user's run before the runtime's role check.
    let err = desk.beam.invoke_result(as_clerk).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-069", "{err}");
    assert_eq!(status_of(&desk, invoice.id).await, "pending_approval");
}

#[tokio::test]
async fn the_controller_can_reject_an_invoice_with_exceptions() {
    let desk = desk(vec![ScriptedTurn::text("Rejected: the vendor must correct the price and the quantity.")]).await;
    let invoice = receive_invoice(desk.invoices.as_ref(), NORA, Some(ORG), "acme.png", &acme_numbered("INV-2"))
        .await
        .unwrap();
    let req = run(RAJ, invoice.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    let held = answer(&desk, &req, &held, json!({"selected_choices": [CHOICES[2]]})).await;
    let done = answer(&desk, &req, &held, json!({"values": {"note": "Price and quantity are wrong."}})).await;
    assert!(done.text_content().contains("rejected"), "{}", done.text_content());
    let rejected = desk.invoices.get(invoice.id).await.unwrap().unwrap();
    assert_eq!(rejected.status, "rejected");
    assert_eq!(rejected.data["rejection_reason"], "Price and quantity are wrong.");
    assert_eq!(rejected.data["decision"]["outcome"], "reject");
}

#[tokio::test]
async fn only_ap_staff_run_the_match_and_a_finished_invoice_is_not_matched_again() {
    let desk = desk(vec![]).await;
    let delta = delta_invoice(NORA, Some(ORG));
    desk.invoices.insert(&delta).await.unwrap();
    let err = desk.beam.invoke_result(run(MIA, delta.id)).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-004", "{err}");

    let req = run(NORA, delta.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    answer(&desk, &req, &held, json!({"approved": true})).await;
    let err = desk.beam.invoke_result(run(NORA, delta.id)).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-003", "{err}");
}

#[tokio::test]
async fn the_same_invoice_twice_is_refused_and_a_changed_total_is_flagged_by_the_match() {
    let desk = desk(vec![]).await;
    let store = desk.invoices.as_ref();
    let first = receive_invoice(store, NORA, Some(ORG), "acme.png", &acme()).await.unwrap();
    let err = receive_invoice(store, NORA, Some(ORG), "acme-again.png", &acme()).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-003");
    let mut bare = acme();
    bare["invoice_number"] = json!("");
    let err = receive_invoice(store, NORA, Some(ORG), "x.png", &bare).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-005", "no invoice number");

    // Same vendor and number with another total: recorded, then failed by the match.
    let mut changed = acme();
    changed["total"] = json!(1500.0);
    let second = receive_invoice(store, NORA, Some(ORG), "acme-2.png", &changed).await.unwrap();
    assert_ne!(first.id, second.id);
    desk.beam.invoke_result(run(NORA, second.id)).await.unwrap();
    let flagged = desk.invoices.get(second.id).await.unwrap().unwrap();
    let findings = flagged.data["findings"].as_array().unwrap();
    let duplicate = findings.iter().find(|f| f["rule"] == "duplicate invoice").unwrap();
    assert_eq!(duplicate["pass"], false);
    assert!(duplicate["value"].as_str().unwrap().contains("INV-88213"));
}

fn ctx(user: &str) -> ToolCallContext {
    ToolCallContext {
        auth: Some(auth(user)),
        ..ToolCallContext::default()
    }
}

async fn call(store: &Arc<dyn RecordStore>, tool: &str, args: Value, user: &str) -> Result<Value, String> {
    let tools = invoice_tools(store);
    let tool = tools.iter().find(|t| t.name == tool).unwrap();
    tool.callable
        .as_ref()
        .unwrap()
        .call(args, &ctx(user))
        .await
        .map_err(|e| e.errcode.as_str().to_owned())
}

async fn stored(store: &Arc<dyn RecordStore>, status: &str, extra: Value) -> Record {
    let mut record = Record::new(SHOWCASE, INVOICE, NORA, Some(ORG), "Acme Packaging Co. INV-1");
    record.status = status.into();
    record.data.insert("total".into(), json!(1436.4));
    record.merge(extra.as_object().unwrap());
    store.insert(&record).await.unwrap();
    record
}

#[tokio::test]
async fn separation_of_duties_holds_at_the_tools() {
    let store = showcase::records::InMemoryRecords::shared();
    let flagged = stored(&store, "pending_approval", json!({})).await;
    let id = flagged.id.to_string();
    let pay = json!({"invoice_id": id, "amount": 1436.4, "pay_date": "2026-11-19"});

    // Outsiders see nothing; the clerk reads but cannot reject.
    let outsider = call(&store, GET_INVOICE, json!({"invoice_id": id}), MIA).await;
    assert_eq!(outsider.unwrap_err(), "SHC-004");
    let read = call(&store, GET_INVOICE, json!({"invoice_id": id}), NORA).await.unwrap();
    assert_eq!(read["status"], "pending_approval");
    let reject = call(&store, REJECT_INVOICE, json!({"invoice_id": id, "reason": "no"}), NORA).await;
    assert_eq!(reject.unwrap_err(), "SHC-004", "only the controller rejects");

    // An invoice with exceptions cannot be scheduled before it is decided.
    let early = call(&store, SCHEDULE_PAYMENT, pay.clone(), RAJ).await;
    assert_eq!(early.unwrap_err(), "SHC-003");

    // Once approved, only the controller schedules it, for the approved amount.
    let mut approved = flagged.clone();
    approved.status = "approved".into();
    approved.data.insert("approved_amount".into(), json!(1328.4));
    store.save(&mut approved).await.unwrap();
    let by_clerk = call(&store, SCHEDULE_PAYMENT, json!({"invoice_id": id, "amount": 1328.4, "pay_date": "2026-11-19"}), NORA).await;
    assert_eq!(by_clerk.unwrap_err(), "SHC-004");
    let too_much = call(&store, SCHEDULE_PAYMENT, pay, RAJ).await;
    assert_eq!(too_much.unwrap_err(), "SHC-005", "not the approved amount");
    let bad_date = call(&store, SCHEDULE_PAYMENT, json!({"invoice_id": id, "amount": 1328.4, "pay_date": "soon"}), RAJ).await;
    assert_eq!(bad_date.unwrap_err(), "SHC-005");
    let ok = call(&store, SCHEDULE_PAYMENT, json!({"invoice_id": id, "amount": 1328.4, "pay_date": "2026-11-19"}), RAJ)
        .await
        .unwrap();
    assert_eq!(ok["payment"]["pay_date"], "2026-11-19");
    let after = call(&store, HOLD_INVOICE, json!({"invoice_id": id, "reason": "late"}), NORA).await;
    assert_eq!(after.unwrap_err(), "SHC-003", "a scheduled invoice cannot be held");
}

#[tokio::test]
async fn a_clerk_may_hold_a_matched_invoice_and_schedule_it_only_while_matched() {
    let store = showcase::records::InMemoryRecords::shared();
    let matched = stored(&store, "matched", json!({})).await;
    let id = matched.id.to_string();
    let held = call(&store, HOLD_INVOICE, json!({"invoice_id": id, "reason": "ask the vendor about the freight"}), NORA)
        .await
        .unwrap();
    assert_eq!(held["on_hold"], true);
    let blocked = call(&store, SCHEDULE_PAYMENT, json!({"invoice_id": id, "amount": 1436.4, "pay_date": "2026-11-19"}), NORA).await;
    assert_eq!(blocked.unwrap_err(), "SHC-003", "an invoice on hold is not paid");

    let clean = stored(&store, "matched", json!({})).await;
    let paid = call(
        &store,
        SCHEDULE_PAYMENT,
        json!({"invoice_id": clean.id.to_string(), "amount": 1436.4, "pay_date": "2026-11-19"}),
        NORA,
    )
    .await
    .unwrap();
    assert_eq!(paid["scheduled"], true);
}

/// `invoice_reader`: an image in, the `vision` model alias, and structured
/// output that matches the invoice schema.
#[tokio::test]
async fn an_invoice_image_goes_to_the_vision_model_and_comes_back_structured() {
    let model = ScriptedModel::new([ScriptedTurn::text(acme().to_string())]);
    let catalog = concat!(env!("CARGO_MANIFEST_DIR"), "/models.yaml");
    let models = beam_routing::load_models_from_yaml(catalog).unwrap();
    let aliases = beam_routing::load_aliases_from_yaml(catalog).unwrap();
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(model.clone()))
        .router(
            beam_routing::ModelRouter::new(models, Some("nemotron-lightning".into()))
                .unwrap()
                .with_aliases(aliases),
        )
        .tools(Arc::new(ToolService::new(
            Arc::new(ToolRegistry::new()),
            Arc::new(ToolRuntime::default()),
        )))
        .build()
        .unwrap();
    beam.bind_rig_agent(reader_metadata(), reader_def());
    beam.checkin_catalog().await.unwrap();

    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
    let request = InvokeRequest {
        agent_name: INVOICE_READER.into(),
        content: vec![
            ContentPart::text("Read this invoice."),
            serde_json::from_value(json!({"type": "image_base64", "data": png, "mime_type": "image/png"})).unwrap(),
        ],
        session_id: Uuid::new_v4(),
        model_override: None,
        metadata: json!({}),
        interaction_id: None,
        execution_id: None,
        request_id: Some(Uuid::new_v4()),
        idempotency_key: None,
        stream: false,
        actions: None,
        auth_context: Some(auth(NORA)),
        context: None,
        client: None,
    };
    let done = beam.invoke_result(request).await.unwrap();
    let call = &model.seen()[0];
    assert_eq!(call.model.as_deref(), Some("vision"));
    assert!(format!("{:?}", call.prompt).contains("Image"), "{:?}", call.prompt);
    let answer: Value = serde_json::from_str(&done.text_content()).unwrap();
    for field in ["vendor_name", "invoice_number", "invoice_date", "po_number", "lines", "subtotal", "tax", "total", "confidence"] {
        assert!(answer.get(field).is_some(), "{field} missing from {answer}");
    }
    assert_eq!(answer["lines"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn two_controller_runs_schedule_one_payment() {
    let desk = desk(vec![ScriptedTurn::text("Pay the matched amount."), ScriptedTurn::text("Pay it.")]).await;
    let invoice = receive_invoice(desk.invoices.as_ref(), NORA, Some(ORG), "acme.png", &acme_numbered("INV-9"))
        .await
        .unwrap();
    // Two controller tabs open the same invoice.
    let (first, second) = (run(RAJ, invoice.id), run(RAJ, invoice.id));
    let held_first = desk.beam.invoke_result(first.clone()).await.unwrap();
    let held_second = desk.beam.invoke_result(second.clone()).await.unwrap();

    let held = answer(&desk, &first, &held_first, json!({"selected_choices": [CHOICES[1]]})).await;
    let held = answer(&desk, &first, &held, json!({"values": {"note": "Pay what arrived."}})).await;
    answer(&desk, &first, &held, json!({"approved": true})).await;
    assert_eq!(status_of(&desk, invoice.id).await, "scheduled");

    let held = answer(&desk, &second, &held_second, json!({"selected_choices": [CHOICES[0]]})).await;
    let err = try_answer(&desk, &second, &held, json!({"values": {"note": "Pay it all."}})).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-003", "{err}");
    let paid = desk.invoices.get(invoice.id).await.unwrap().unwrap();
    assert_eq!(paid.status, "scheduled", "the second run does not reopen the invoice");
    assert!((paid.data["payment"]["amount"].as_f64().unwrap() - 1328.4).abs() < 1e-6);
}
