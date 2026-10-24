//! The claims showcase on an in-memory beam with a scripted model: a
//! policyholder's intake (a form card, a damage upload, a filed claim), the
//! adjuster's review runtime (an exception, a decision, a note and an
//! approved payout), the permissions around claims and payouts, and the
//! payout rule (estimate minus deductible).

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_knowledge::{HashEmbedder, InMemoryVectorStore, KnowledgeManager};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{ToolCallContext, ToolRegistry, ToolRuntime, ToolService};
use beam_types::{
    ActionResponseEnvelope, AgentResult, ClientCapabilities, ContentPart, InvokeAuthContext,
    InvokeRequest, RequestContext, RequestFact,
};
use serde_json::{json, Value};
use showcase::claims::agents::{damage_def, damage_metadata, CLAIMS_INTAKE, CLAIM_REVIEW, DAMAGE_READER};
use showcase::claims::personas::{AVA, LEO, NINA, ORG};
use showcase::claims::rules::payout_for;
use showcase::claims::tools::{
    claims_tools, APPROVE_PAYOUT, FILE_CLAIM, GET_CLAIM, GET_MY_POLICY, START_CLAIM, UPDATE_CLAIM,
};
use showcase::claims::{bind_agents, seeded_claim, CLAIM, SHOWCASE};
use showcase::kit::{REQUEST_DOCUMENT, REQUEST_INPUT};
use showcase::records::{InMemoryRecords, Record, RecordStore};
use uuid::Uuid;

struct Desk {
    beam: RiverBeam,
    claims: Arc<dyn RecordStore>,
    model: ScriptedModel,
}

async fn desk(turns: Vec<ScriptedTurn>) -> Desk {
    let model = ScriptedModel::new(turns);
    let claims = InMemoryRecords::shared();
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
    let owned = bind_agents(&beam, &claims).await.unwrap();
    assert!(owned.iter().any(|t| t == APPROVE_PAYOUT));
    beam.checkin_catalog().await.unwrap();
    Desk { beam, claims, model }
}

fn auth(user: &str) -> InvokeAuthContext {
    InvokeAuthContext {
        profile_id: None,
        user_id: Some(user.into()),
        org_id: Some(ORG.into()),
        mcp_token: None,
    }
}

fn turn(agent: &str, user: &str, claim: Option<Uuid>, text: &str) -> InvokeRequest {
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
        auth_context: Some(auth(user)),
        context: claim.map(|id| RequestContext {
            facts: vec![RequestFact {
                key: "claim_id".into(),
                value: json!(id.to_string()),
                expiry: None,
                sensitivity: beam_types::Sensitivity::Internal,
            }],
            ..RequestContext::default()
        }),
        client: Some(ClientCapabilities {
            client_id: "tab-1".into(),
            kind: "web".into(),
            tools: vec![REQUEST_INPUT.into(), REQUEST_DOCUMENT.into()],
        }),
    }
}

/// An answer to every held action of `result`: client tools get `outputs` (by
/// tool name), approvals are approved.
fn answers(result: &AgentResult, outputs: &Value) -> Vec<ActionResponseEnvelope> {
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

/// Answer the review's one held action with `payload`, as the adjuster.
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

fn payload(result: &AgentResult) -> Value {
    result.action_required.as_ref().unwrap()[0]["action_request"]["payload"].clone()
}

/// A filed claim of `owner` with `extra` merged over the basics.
fn filed(owner: &str, policy: &str, extra: &Value) -> Record {
    let mut claim = Record::new(SHOWCASE, CLAIM, owner, Some(ORG), "Rear bumper damage");
    claim.status = "filed".into();
    claim.merge(
        json!({
            "policy_number": policy, "loss_type": "collision", "loss_date": "2026-10-02",
            "location": "Main St", "description": "Rear-ended at a red light", "drivable": true,
            "other_party": "Driver of a Civic",
        })
        .as_object()
        .unwrap(),
    );
    claim.merge(extra.as_object().unwrap());
    claim
}

fn review(claim: Uuid) -> InvokeRequest {
    turn(CLAIM_REVIEW, AVA, Some(claim), "Review this claim.")
}

#[tokio::test]
async fn a_policyholder_reports_a_loss_uploads_the_estimate_and_files_the_claim() {
    let desk = desk(vec![
        ScriptedTurn::ToolCalls(vec![
            ("c1".into(), GET_MY_POLICY.into(), json!({})),
            ("c2".into(), START_CLAIM.into(), json!({})),
        ]),
        ScriptedTurn::ToolCalls(vec![
            (
                "c3".into(),
                REQUEST_INPUT.into(),
                json!({"title": "About the accident", "fields": [
                    {"name": "loss_date", "label": "Date of loss", "type": "text", "required": true},
                    {"name": "location", "label": "Where", "type": "text", "required": true}]}),
            ),
            (
                "c4".into(),
                REQUEST_DOCUMENT.into(),
                json!({"document_type": "damage_photo", "title": "Your damage estimate"}),
            ),
        ]),
        ScriptedTurn::tool_call(
            "c5",
            UPDATE_CLAIM,
            json!({"fields": {
                "loss_date": "2026-10-02", "location": "Main St and 5th", "loss_type": "collision",
                "description": "Rear-ended at a red light", "other_party": "Driver of a Civic",
                "police_report": "PR-77120", "drivable": true,
                "estimated_repair_cost": 3722.5, "severity": "moderate"}}),
        ),
        ScriptedTurn::tool_call("c6", FILE_CLAIM, json!({})),
        ScriptedTurn::text("Your claim is filed. An adjuster reviews it within two business days."),
    ])
    .await;

    let req = turn(CLAIMS_INTAKE, LEO, None, "I was rear-ended this week.");
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert!(held.interrupted, "the form and the upload wait: {:?}", held.content);
    let held_tools: Vec<String> = held
        .action_required
        .as_ref()
        .unwrap()
        .iter()
        .map(|a| a["action_request"]["payload"]["tool_name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(held_tools, [REQUEST_INPUT, REQUEST_DOCUMENT]);
    let drafts = desk.claims.list(SHOWCASE, CLAIM, Some(LEO)).await.unwrap();
    assert_eq!(drafts.len(), 1, "start_claim made a draft");
    assert_eq!(drafts[0].status, "draft");
    assert_eq!(drafts[0].text("policy_number"), Some("AUT-55120"));

    let outputs = json!({
        REQUEST_INPUT: {"loss_date": "2026-10-02", "location": "Main St and 5th"},
        REQUEST_DOCUMENT: {"file_name": "bayside-estimate.png", "document_type": "damage_photo",
            "extracted": {"estimated_repair_cost": 3722.5, "severity": "moderate", "drivable": true}},
    });
    let mut next = req.clone();
    next.request_id = Some(Uuid::new_v4());
    next.content = Vec::new();
    next.actions = Some(answers(&held, &outputs));
    let done = desk.beam.invoke_result(next).await.unwrap();
    assert!(done.text_content().contains("filed"), "{}", done.text_content());
    // What the upload card read reached the model.
    assert!(format!("{:?}", desk.model.seen()[2].prompt).contains("3722.5"));

    let claims = desk.claims.list(SHOWCASE, CLAIM, Some(LEO)).await.unwrap();
    assert_eq!(claims.len(), 1);
    let claim = &claims[0];
    assert_eq!(claim.status, "filed");
    assert_eq!(claim.text("loss_date"), Some("2026-10-02"));
    assert_eq!(claim.field("drivable"), Some(&json!(true)));
    assert_eq!(claim.number("estimated_repair_cost"), Some(3722.5));
    assert!(claim.field("filed_at").is_some());
}

#[tokio::test]
async fn an_exception_is_decided_noted_and_paid_after_the_adjuster_approves() {
    let desk = desk(vec![ScriptedTurn::text(
        "Approved: the adjuster confirmed fault with the other driver, so the missing report is not decisive.",
    )])
    .await;
    // Two parties, no police report: a fraud signal fires.
    let claim = filed(
        LEO,
        "AUT-55120",
        &json!({"estimated_repair_cost": 3722.5, "severity": "moderate"}),
    );
    desk.claims.insert(&claim).await.unwrap();
    let req = review(claim.id);

    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert!(held.interrupted);
    assert_eq!(kind(&held), "beam.choice_question");
    assert!(held.text_content().contains("no police report"), "{}", held.text_content());
    assert_eq!(
        payload(&held)["choices"],
        json!(["Approve the claim", "Approve a partial amount", "Deny the claim", "Send to investigation"])
    );
    assert_eq!(desk.claims.get(claim.id).await.unwrap().unwrap().status, "in_review");

    let held = answer(&desk, &req, &held, json!({"selected_choices": ["Approve the claim"]})).await;
    assert_eq!(kind(&held), "beam.form_input");
    let held = answer(
        &desk,
        &req,
        &held,
        json!({"values": {"note": "Other driver admitted fault on the scene."}}),
    )
    .await;
    assert_eq!(kind(&held), "beam.tool_approval");
    let approval = payload(&held);
    assert_eq!(approval["tool_name"], APPROVE_PAYOUT);
    assert_eq!(approval["risk"], "high");
    assert_eq!(approval["args"]["amount"], json!(payout_for(3722.5, 500.0)));
    assert_eq!(approval["args"]["amount"], json!(3222.5), "estimate minus the $500 deductible");
    let before = desk.claims.get(claim.id).await.unwrap().unwrap();
    assert_eq!(before.status, "in_review", "nothing is paid before the approval");
    assert!(before.field("payout").is_none());

    let done = answer(&desk, &req, &held, json!({"approved": true})).await;
    assert!(!done.interrupted);
    assert!(done.text_content().contains("$3,222.50"), "{}", done.text_content());
    let paid = desk.claims.get(claim.id).await.unwrap().unwrap();
    assert_eq!(paid.status, "paid");
    assert_eq!(paid.data["payout"]["amount"], json!(3222.5));
    assert_eq!(paid.data["decision"]["outcome"], "approve");
    assert!(paid.data["decision"]["note"].as_str().unwrap().contains("admitted fault"));
    assert!(paid.data["decision"]["rationale"].as_str().unwrap().contains("Approved"));
    assert_eq!(paid.data["decision"]["signals"][1]["fired"], json!(true));
}

#[tokio::test]
async fn a_clean_claim_still_asks_for_a_note_and_the_payout_approval() {
    let desk = desk(vec![ScriptedTurn::text("Every rule passes; pay the estimate less the deductible.")]).await;
    let claim = seeded_claim();
    desk.claims.insert(&claim).await.unwrap();
    let req = review(claim.id);

    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert_eq!(kind(&held), "beam.form_input", "no exception, so no choice to make");
    let held = answer(&desk, &req, &held, json!({"values": {"note": "Report checked."}})).await;
    assert_eq!(kind(&held), "beam.tool_approval");
    assert_eq!(payload(&held)["args"]["amount"], json!(3350.0), "$3,850 less the $500 deductible");

    let done = answer(&desk, &req, &held, json!({"approved": false})).await;
    assert!(done.text_content().contains("not issued"), "{}", done.text_content());
    let read = desk.claims.get(claim.id).await.unwrap().unwrap();
    assert_eq!(read.status, "approved", "decided, not paid");
    assert!(read.field("payout").is_none());
}

#[tokio::test]
async fn a_partial_approval_is_capped_and_a_denial_pays_nothing() {
    let desk = desk(vec![
        ScriptedTurn::text("Partly approved: only the bumper is clearly from this loss."),
        ScriptedTurn::text("Denied: the loss falls under the rideshare exclusion."),
    ])
    .await;
    // A rideshare loss fails the exclusion rule.
    let rideshare = filed(
        LEO,
        "AUT-55120",
        &json!({"estimated_repair_cost": 2000, "severity": "minor", "police_report": "PR-2",
            "description": "Hit a pole during a rideshare trip"}),
    );
    desk.claims.insert(&rideshare).await.unwrap();
    let req = review(rideshare.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert_eq!(kind(&held), "beam.choice_question");
    let held = answer(&desk, &req, &held, json!({"selected_choices": ["Approve a partial amount"]})).await;
    assert_eq!(kind(&held), "beam.form_input");
    assert!(
        held.text_content().contains("Decision recorded"),
        "{}",
        held.text_content()
    );
    // $2,000 less $500: no more than $1,500 can be paid.
    let held = answer(
        &desk,
        &req,
        &held,
        json!({"values": {"note": "Bumper only.", "partial_amount": "9999"}}),
    )
    .await;
    assert_eq!(kind(&held), "beam.form_input", "asked again for an amount within the cap");
    let held = answer(
        &desk,
        &req,
        &held,
        json!({"values": {"note": "Bumper only.", "partial_amount": "$1,000"}}),
    )
    .await;
    assert_eq!(kind(&held), "beam.tool_approval");
    assert_eq!(payload(&held)["args"]["amount"], json!(1000.0));
    let done = answer(&desk, &req, &held, json!({"approved": true})).await;
    assert!(done.text_content().contains("$1,000.00"), "{}", done.text_content());
    let paid = desk.claims.get(rideshare.id).await.unwrap().unwrap();
    assert_eq!(paid.status, "paid");
    assert_eq!(paid.data["decision"]["outcome"], "partial");
    assert_eq!(paid.data["payout"]["amount"], json!(1000.0));

    // A claim with no estimate offers only a denial or an investigation.
    let bare = filed(LEO, "AUT-55120", &json!({"police_report": "PR-3"}));
    desk.claims.insert(&bare).await.unwrap();
    let req = review(bare.id);
    let held = desk.beam.invoke_result(req.clone()).await.unwrap();
    assert_eq!(payload(&held)["choices"], json!(["Deny the claim", "Send to investigation"]));
    let held = answer(&desk, &req, &held, json!({"selected_choices": ["Deny the claim"]})).await;
    let done = answer(&desk, &req, &held, json!({"values": {"note": "No estimate was ever sent."}})).await;
    assert!(!done.interrupted, "a denial ends without a payout approval");
    assert!(done.text_content().contains("No payout"), "{}", done.text_content());
    let denied = desk.claims.get(bare.id).await.unwrap().unwrap();
    assert_eq!(denied.status, "denied");
    assert!(denied.field("payout").is_none());
}

#[tokio::test]
async fn only_an_adjuster_reviews_and_only_a_filed_claim_is_reviewed() {
    let desk = desk(vec![]).await;
    let claim = seeded_claim();
    desk.claims.insert(&claim).await.unwrap();
    let by_leo = turn(CLAIM_REVIEW, LEO, Some(claim.id), "Review this claim.");
    let err = desk.beam.invoke_result(by_leo).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-004", "{err}");
    assert_eq!(
        desk.claims.get(claim.id).await.unwrap().unwrap().status,
        "filed",
        "a refused review leaves the claim alone"
    );

    let mut draft = filed(LEO, "AUT-55120", &json!({}));
    draft.status = "draft".into();
    desk.claims.insert(&draft).await.unwrap();
    let err = desk.beam.invoke_result(review(draft.id)).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-003", "{err}");
}

fn ctx(user: &str) -> ToolCallContext {
    ToolCallContext {
        auth: Some(auth(user)),
        ..ToolCallContext::default()
    }
}

async fn call(store: &Arc<dyn RecordStore>, tool: &str, args: Value, user: &str) -> Result<Value, String> {
    let tools = claims_tools(store);
    let tool = tools.iter().find(|t| t.name == tool).unwrap();
    tool.callable
        .as_ref()
        .unwrap()
        .call(args, &ctx(user))
        .await
        .map_err(|e| e.errcode.as_str().to_owned())
}

#[tokio::test]
async fn claims_stay_private_and_payouts_stop_at_estimate_minus_deductible() {
    let store = InMemoryRecords::shared();
    let mut nina = seeded_claim();
    store.insert(&nina).await.unwrap();
    let id = nina.id.to_string();

    // A policyholder cannot read another's claim; an adjuster reads any.
    let other = call(&store, GET_CLAIM, json!({"claim_id": id}), LEO).await;
    assert_eq!(other.unwrap_err(), "SHC-002");
    let read = call(&store, GET_CLAIM, json!({"claim_id": id}), AVA).await.unwrap();
    assert_eq!(read["policy"]["policy_number"], "AUT-48377");
    let own = call(&store, GET_CLAIM, json!({"claim_id": id}), NINA).await.unwrap();
    assert_eq!(own["status"], "filed");

    // A filed claim no longer changes.
    let edit = call(&store, UPDATE_CLAIM, json!({"claim_id": id, "fields": {"location": "Elsewhere"}}), NINA).await;
    assert_eq!(edit.unwrap_err(), "SHC-003");

    // A draft must be complete to be filed, and fields are checked.
    let started = call(&store, START_CLAIM, json!({}), LEO).await.unwrap();
    assert_eq!(started["existing"], json!(false));
    let again = call(&store, START_CLAIM, json!({}), LEO).await.unwrap();
    assert_eq!(again["existing"], json!(true), "one draft at a time");
    assert_eq!(call(&store, FILE_CLAIM, json!({}), LEO).await.unwrap_err(), "SHC-005", "facts are missing");
    let bad = call(&store, UPDATE_CLAIM, json!({"fields": {"loss_date": "last Tuesday"}}), LEO).await;
    assert_eq!(bad.unwrap_err(), "SHC-005");
    let adjuster_starts = call(&store, START_CLAIM, json!({}), AVA).await;
    assert_eq!(adjuster_starts.unwrap_err(), "SHC-004");

    // Payouts: adjusters only, from a claim under review, within the rule.
    let by_owner = call(&store, APPROVE_PAYOUT, json!({"claim_id": id, "amount": 100, "reason": "x"}), NINA).await;
    assert_eq!(by_owner.unwrap_err(), "SHC-004", "the policyholder cannot pay themselves");
    let unreviewed = call(&store, APPROVE_PAYOUT, json!({"claim_id": id, "amount": 100, "reason": "x"}), AVA).await;
    assert_eq!(unreviewed.unwrap_err(), "SHC-003", "a filed claim is not paid before its review");
    nina.status = "approved".into();
    store.save(&mut nina).await.unwrap();
    let over = call(&store, APPROVE_PAYOUT, json!({"claim_id": id, "amount": 3350.01, "reason": "x"}), AVA).await;
    assert_eq!(over.unwrap_err(), "SHC-005", "$3,850 less $500 is $3,350");
    let paid = call(&store, APPROVE_PAYOUT, json!({"claim_id": id, "amount": 3350, "reason": "Full repair"}), AVA)
        .await
        .unwrap();
    assert_eq!(paid["payout"]["amount"], json!(3350.0));
    assert_eq!(paid["payout"]["deductible"], json!(500.0));
    assert_eq!(store.get(nina.id).await.unwrap().unwrap().status, "paid");
    let twice = call(&store, APPROVE_PAYOUT, json!({"claim_id": id, "amount": 1, "reason": "x"}), AVA).await;
    assert_eq!(twice.unwrap_err(), "SHC-003", "a paid claim is not paid again");
}

#[tokio::test]
async fn damage_reader_sends_the_image_to_the_vision_model_and_returns_structured_output() {
    let extracted = json!({
        "vehicle": "2021 Subaru Outback",
        "damaged_parts": [
            {"part": "rear bumper cover", "action": "replace", "cost": 640.0},
            {"part": "tail lamp", "action": "replace", "cost": 310.0}
        ],
        "labor_hours": 9.5,
        "estimated_repair_cost": 3722.5,
        "drivable": true,
        "severity": "moderate",
        "confidence": 0.94
    });
    let model = ScriptedModel::new([ScriptedTurn::text(extracted.to_string())]);
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
    beam.bind_rig_agent(damage_metadata(), damage_def());
    beam.checkin_catalog().await.unwrap();

    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
    let request = InvokeRequest {
        content: vec![
            ContentPart::text("Read this damage estimate."),
            serde_json::from_value(json!({"type": "image_base64", "data": png, "mime_type": "image/png"})).unwrap(),
        ],
        client: None,
        context: None,
        ..turn(DAMAGE_READER, LEO, None, "")
    };
    let done = beam.invoke_result(request).await.unwrap();
    let call = &model.seen()[0];
    assert_eq!(call.model.as_deref(), Some("vision"));
    assert!(format!("{:?}", call.prompt).contains("Image"), "the image reaches the model");
    let answer: Value = serde_json::from_str(&done.text_content()).unwrap();
    for field in ["damaged_parts", "estimated_repair_cost", "drivable", "severity", "confidence"] {
        assert!(answer.get(field).is_some(), "{field} missing from {answer}");
    }
    assert_eq!(answer["estimated_repair_cost"], json!(3722.5));
}

#[tokio::test]
async fn two_reviews_of_one_claim_pay_it_once() {
    let desk = desk(vec![ScriptedTurn::text("Approved."), ScriptedTurn::text("Approved again.")]).await;
    let claim = filed(LEO, "AUT-55120", &json!({"estimated_repair_cost": 3722.5, "severity": "moderate"}));
    desk.claims.insert(&claim).await.unwrap();
    // Two adjuster tabs start a review of the same claim.
    let (first, second) = (review(claim.id), review(claim.id));
    let held_first = desk.beam.invoke_result(first.clone()).await.unwrap();
    let held_second = desk.beam.invoke_result(second.clone()).await.unwrap();

    let held = answer(&desk, &first, &held_first, json!({"selected_choices": ["Approve the claim"]})).await;
    let held = answer(&desk, &first, &held, json!({"values": {"note": "Fault admitted."}})).await;
    answer(&desk, &first, &held, json!({"approved": true})).await;
    assert_eq!(desk.claims.get(claim.id).await.unwrap().unwrap().status, "paid");

    let held = answer(&desk, &second, &held_second, json!({"selected_choices": ["Approve the claim"]})).await;
    let held = answer(&desk, &second, &held, json!({"values": {"note": "Fault admitted."}})).await;
    let err = try_answer(&desk, &second, &held, json!({"approved": true})).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "SHC-003", "{err}");
    let paid = desk.claims.get(claim.id).await.unwrap().unwrap();
    assert_eq!(paid.status, "paid", "the second review does not reopen the claim");
    assert_eq!(paid.data["payout"]["amount"], json!(3222.5));
}
