//! `ap_match`: the three-way match of an invoice, as a step runtime (the
//! loan desk's pre-check has the same shape). Each step is plain Rust; the
//! run is saved in the beam's `RuntimeStateStore` whenever it waits for a
//! person, so any worker can continue it.
//!
//! 1. Load the invoice (AP staff only) and read the ERP through MCP:
//!    `erp__get_purchase_order`, `erp__get_goods_receipt`, `erp__get_vendor`.
//! 2. Apply the rules ([`crate::invoice::matching`]) and the duplicate check,
//!    and record the findings on the invoice.
//! 3. A clean invoice becomes `matched`, and the run asks the person to
//!    approve `schedule_payment`.
//! 4. An invoice with exceptions becomes `pending_approval`. The clerk is
//!    told the controller must decide (a clerk cannot approve exceptions).
//!    When the controller runs it, they get a choice card (approve the
//!    exception and pay the invoiced amount, pay only the matched amount,
//!    reject, hold) and a form card for their note.
//! 5. One model call drafts the rationale. Reject and hold are recorded; a
//!    payment decision sets the invoice `approved` and the run asks the
//!    controller to approve `schedule_payment`, which sets it `scheduled`.

use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use beam_runtime::actions::{
    build_action_request, validate_action_request, ActionRequestArgs, TOOL_APPROVAL_KIND,
};
use beam_runtime::state::{status, RuntimeStateRecord, RuntimeStateStore};
use beam_runtime::{
    run_nested, AgentRuntime, DirectModelRuntime, EventStream, RuntimeCapabilities, RuntimeContext,
    RuntimeEvent, RuntimeEventType, RuntimeExecutionRef, RuntimeResumeRequest, RuntimeStartRequest,
    RuntimeState, ToolInvocation,
};
use beam_types::{
    ActionRequestEnvelope, ActionResponseEnvelope, ActionResponseStatus, AgentInput, AgentMessage,
    AgentMessageRole, AgentMessageType, AgentResult, ContentPart,
};
use chrono::{Duration, Utc};
use futures_util::StreamExt;
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::errors::{SHC_002, SHC_003, SHC_004, SHC_005};
use crate::invoice::matching::{self, dollars, num, Finding};
use crate::invoice::tools::{duplicates_of, is_controller, is_staff, HOLD_INVOICE, REJECT_INVOICE, SCHEDULE_PAYMENT};
use crate::invoice::{INVOICE, SHOWCASE};
use crate::records::{require, Record, RecordStore};

pub const RUNTIME_NAME: &str = "ap-match";
const ADAPTER_VERSION: &str = "1";
const LIBRARY_VERSION: &str = "invoice-1";
/// The status a controller decides an invoice in; the decision is written only from it.
const PENDING_APPROVAL: &str = "pending_approval";
/// The MCP tools of the ERP.
pub const GET_PURCHASE_ORDER: &str = "erp__get_purchase_order";
pub const GET_GOODS_RECEIPT: &str = "erp__get_goods_receipt";
pub const GET_VENDOR: &str = "erp__get_vendor";

/// The controller's choices on an invoice with exceptions.
pub const CHOICES: [&str; 4] = [
    "Approve the exception and pay the invoiced amount",
    "Pay only the matched amount",
    "Reject the invoice",
    "Put the invoice on hold",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage {
    AwaitingDecision,
    AwaitingNote,
    AwaitingPayment,
    Done,
}

/// What a run keeps between its steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    invoice_id: Uuid,
    stage: Stage,
    findings: Vec<Finding>,
    /// `approve_full`, `pay_matched`, `reject` or `hold`.
    decision: Option<String>,
    note: Option<String>,
    /// The amount to pay and the day to pay it.
    amount: f64,
    pay_date: String,
    /// The action the run waits for: `(action_id, request_id)`.
    waiting: Option<(String, String)>,
}

/// The match runtime over the beam's state store and the invoices.
#[derive(Clone)]
pub struct ApMatch {
    states: Arc<dyn RuntimeStateStore>,
    invoices: Arc<dyn RecordStore>,
}

impl ApMatch {
    #[must_use]
    pub fn new(states: Arc<dyn RuntimeStateStore>, invoices: Arc<dyn RecordStore>) -> Self {
        Self { states, invoices }
    }
}

/// Numbers events in order and sends them to the run's stream.
struct Emitter {
    tx: mpsc::UnboundedSender<RiverbaseResult<RuntimeEvent>>,
    execution_id: Uuid,
    run_id: Uuid,
    input: AgentInput,
    sequence: i64,
    /// What this segment said: its result carries it, so the transcript keeps it.
    said: Vec<String>,
}

impl Emitter {
    fn send(&mut self, mut event: RuntimeEvent) {
        self.sequence += 1;
        event.sequence = Some(self.sequence);
        let _ = self.tx.send(Ok(event));
    }

    fn message(&self, message: AgentMessage) -> AgentMessage {
        AgentMessage {
            run_id: Some(self.input.run_id),
            request_id: self.input.request_id,
            interaction_id: self.input.interaction_id,
            execution_id: Some(self.execution_id),
            ..message
        }
    }

    fn say(&mut self, text: &str) {
        self.said.push(text.to_owned());
        let mut event = RuntimeEvent::new(RuntimeEventType::MessageCompleted, self.execution_id, self.run_id);
        event.message = Some(self.message(AgentMessage::text(AgentMessageRole::Ai, text)));
        self.send(event);
    }

    fn tool(&mut self, name: &str, call_id: &str, args: &Value, result: &str) {
        let mut event = RuntimeEvent::new(RuntimeEventType::MessageCompleted, self.execution_id, self.run_id);
        event.message = Some(self.message(AgentMessage {
            role: AgentMessageRole::Tool,
            msg_type: AgentMessageType::ToolResult,
            content: vec![ContentPart::text(result)],
            tool_call_id: Some(call_id.to_owned()),
            tool_name: Some(name.to_owned()),
            tool_args: Some(args.clone()),
            tool_result: Some(Value::String(result.to_owned())),
            ..AgentMessage::default()
        }));
        self.send(event);
    }

    fn result(&self, text: &str) -> AgentResult {
        AgentResult {
            content: if text.is_empty() {
                Vec::new()
            } else {
                vec![ContentPart::text(text)]
            },
            agent_name: self.input.agent_name.clone().unwrap_or_default(),
            model_used: None,
            session_id: Some(self.input.session_id),
            request_id: self.input.request_id,
            interaction_id: self.input.interaction_id,
            execution_id: Some(self.execution_id),
            token_usage: None,
            tool_calls: Vec::new(),
            metadata: json!({}),
            latency_ms: None,
            interrupted: false,
            action_required: None,
            citations: Vec::new(),
        }
    }

    fn finish(&mut self, result: &AgentResult) {
        let kind = if result.interrupted {
            RuntimeEventType::ExecutionInterrupted
        } else {
            RuntimeEventType::ExecutionCompleted
        };
        let mut event = RuntimeEvent::new(kind, self.execution_id, self.run_id);
        event
            .payload
            .insert("result".into(), serde_json::to_value(result).unwrap_or(Value::Null));
        self.send(event);
    }

    fn fail(&mut self, err: RiverbaseError) {
        let _ = self.tx.send(Err(err));
    }
}

/// How a step ended: waiting for a person, or done.
enum Outcome {
    Hold(Box<ActionRequestEnvelope>, String),
    Done(String),
}

fn invoice_id(input: &AgentInput) -> RiverbaseResult<Uuid> {
    input
        .context
        .as_ref()
        .and_then(|c| c.facts.iter().find(|f| f.key == "invoice_id"))
        .and_then(|f| f.value.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| SHC_005.with_data("the match needs the invoice_id fact"))
}

fn findings_table(findings: &[Finding]) -> String {
    let mut out = String::from("| Rule | Value | Limit | |\n|---|---|---|---|\n");
    for f in findings {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            f.rule,
            f.value,
            f.limit,
            if f.pass { "pass" } else { "**exception**" }
        );
    }
    out
}

fn exceptions(findings: &[Finding]) -> usize {
    findings.iter().filter(|f| !f.pass).count()
}

fn entry(request: &ActionRequestEnvelope, action_id: &str) -> Value {
    json!({"lc_action_id": request.request_id, "action_id": action_id, "action_request": request})
}

fn total_of(invoice: &Record) -> f64 {
    invoice.field("total").and_then(num).unwrap_or_default()
}

/// The invoice date plus the vendor's terms; today plus the terms when the
/// invoice date cannot be read.
fn payment_date(invoice: &Record, terms_days: i64) -> String {
    invoice
        .text("invoice_date")
        .and_then(|date| matching::pay_date(date, terms_days))
        .unwrap_or_else(|| (Utc::now() + Duration::days(terms_days)).format("%Y-%m-%d").to_string())
}

/// The reply of an ERP tool as JSON, if it found something.
fn found(reply: &RiverbaseResult<String>) -> Option<Value> {
    let value = serde_json::from_str::<Value>(reply.as_ref().ok()?).ok()?;
    (value.get("error").is_none() && value.is_object()).then_some(value)
}

/// Run `name` through the tool runtime and record the call in the transcript;
/// the reply (or the error) is returned to the caller.
async fn call(
    emit: &mut Emitter,
    ctx: &RuntimeContext,
    name: &str,
    args: &Value,
    approved: bool,
) -> RiverbaseResult<String> {
    let call_id = Uuid::new_v4().to_string();
    let reply = ctx
        .tool_runtime
        .execute(ToolInvocation {
            execution_id: emit.execution_id,
            run_id: emit.run_id,
            call_id: call_id.clone(),
            name: name.into(),
            args: args.clone(),
            input: Some(Arc::new(emit.input.clone())),
            allowed: None,
            dry_run: false,
            approved,
        })
        .await;
    let shown = match &reply {
        Ok(text) => text.clone(),
        Err(e) => json!({"error": e.errmesg}).to_string(),
    };
    emit.tool(name, &call_id, args, &shown);
    reply
}

fn decision_request(count: usize) -> RiverbaseResult<Box<ActionRequestEnvelope>> {
    Ok(Box::new(validate_action_request(build_action_request(ActionRequestArgs {
        kind: "beam.choice_question".into(),
        request_id: Uuid::new_v4().to_string(),
        title: format!("{count} exception(s): your decision"),
        description: "The invoice is outside the AP policy. Only the controller decides.".into(),
        payload: json!({"question": "How should this invoice proceed?", "choices": CHOICES}),
        ..ActionRequestArgs::default()
    }))?))
}

fn payment_request(invoice: &Record, amount: f64, pay_date: &str) -> RiverbaseResult<Box<ActionRequestEnvelope>> {
    let call_id = Uuid::new_v4().to_string();
    let args = json!({"invoice_id": invoice.id.to_string(), "amount": amount, "pay_date": pay_date});
    Ok(Box::new(validate_action_request(build_action_request(ActionRequestArgs {
        kind: TOOL_APPROVAL_KIND.into(),
        request_id: call_id.clone(),
        title: format!("Approve {SCHEDULE_PAYMENT}"),
        description: format!("Schedule {} to be paid on {pay_date} for {}.", dollars(amount), invoice.title),
        payload: json!({
            "tool_name": SCHEDULE_PAYMENT,
            "call_id": call_id,
            "args": args,
            "risk": "high",
            "remember_options": ["once"],
            "decided_by": {"layer": "agent", "reason": "every payment is approved"}
        }),
        metadata: Some(json!({"tool_name": SCHEDULE_PAYMENT, "tool_args": args, "tool_call_id": call_id})),
        ..ActionRequestArgs::default()
    }))?))
}

impl ApMatch {
    fn record(execution_id: Uuid, run_id: Uuid, state: &State, run_status: &str) -> RuntimeStateRecord {
        RuntimeStateRecord {
            state_id: execution_id,
            execution_id,
            run_id,
            runtime_name: RUNTIME_NAME.into(),
            run_format: 1,
            adapter_version: ADAPTER_VERSION.into(),
            library_version: LIBRARY_VERSION.into(),
            status: run_status.into(),
            turn: 0,
            state: serde_json::to_value(state).unwrap_or(Value::Null),
            updated_at: Utc::now(),
        }
    }

    /// Steps 1-4.
    #[allow(clippy::too_many_lines)] // The steps, read top to bottom.
    async fn begin(&self, emit: &mut Emitter, ctx: &RuntimeContext) -> RiverbaseResult<(State, Outcome)> {
        let id = invoice_id(&emit.input)?;
        let user = emit.input.user_id().unwrap_or_default();
        if !is_staff(&user) {
            return Err(SHC_004.with_data("only accounts payable staff match invoices"));
        }
        let mut invoice = require(self.invoices.as_ref(), SHOWCASE, id).await?;
        if invoice.kind != INVOICE {
            return Err(SHC_002.with_data(format!("invoice {id} was not found")));
        }
        let mut state = State {
            invoice_id: id,
            stage: Stage::AwaitingDecision,
            findings: Vec::new(),
            decision: None,
            note: None,
            amount: 0.0,
            pay_date: String::new(),
            waiting: None,
        };
        match invoice.status.as_str() {
            PENDING_APPROVAL => {
                state.findings = invoice
                    .field("findings")
                    .and_then(|f| serde_json::from_value(f.clone()).ok())
                    .unwrap_or_default();
                state.pay_date = invoice.text("pay_date").unwrap_or_default().to_owned();
                if !is_controller(&user) {
                    state.stage = Stage::Done;
                    return Ok((state, Outcome::Done(awaiting_controller(&invoice))));
                }
                emit.say(&format!(
                    "{} is waiting for your decision.\n\n{}",
                    invoice.title,
                    findings_table(&state.findings)
                ));
                let request = decision_request(exceptions(&state.findings))?;
                return Ok((state, Outcome::Hold(request, String::new())));
            }
            "received" | "matched" | "exception" | "on_hold" => {}
            other => {
                return Err(SHC_003.with_data(format!("the invoice is {other}; it cannot be matched again")));
            }
        }

        let po_number = invoice.text("po_number").unwrap_or_default().to_owned();
        let po = found(&call(emit, ctx, GET_PURCHASE_ORDER, &json!({"po_number": po_number}), false).await);
        let receipt = found(&call(emit, ctx, GET_GOODS_RECEIPT, &json!({"po_number": po_number}), false).await);
        let vendor_id = po.as_ref().and_then(|p| p["vendor_id"].as_str()).map(str::to_owned);
        let vendor = match &vendor_id {
            Some(vendor_id) => found(&call(emit, ctx, GET_VENDOR, &json!({"vendor_id": vendor_id}), false).await),
            None => None,
        };
        let twins: Vec<String> = duplicates_of(
            self.invoices.as_ref(),
            invoice.text("vendor_name").unwrap_or_default(),
            invoice.text("invoice_number").unwrap_or_default(),
            Some(id),
        )
        .await?
        .into_iter()
        .map(|r| format!("{} ({})", r.title, r.status))
        .collect();

        let findings = matching::findings(&invoice.data, po.as_ref(), receipt.as_ref(), vendor.as_ref(), &twins);
        let count = exceptions(&findings);
        let terms_days = vendor.as_ref().and_then(|v| v["terms_days"].as_i64()).unwrap_or(30);
        let pay_date = payment_date(&invoice, terms_days);
        let matched = matching::matched_amount(&invoice.data, po.as_ref(), receipt.as_ref());
        let total = total_of(&invoice);

        let mut fields = Map::new();
        fields.insert("findings".into(), serde_json::to_value(&findings).unwrap_or(Value::Null));
        fields.insert("exceptions".into(), json!(count));
        fields.insert("vendor_id".into(), json!(vendor_id));
        fields.insert("terms".into(), vendor.as_ref().map_or(Value::Null, |v| v["terms"].clone()));
        fields.insert("receipt_id".into(), receipt.as_ref().map_or(Value::Null, |r| r["receipt_id"].clone()));
        fields.insert("pay_date".into(), json!(pay_date));
        fields.insert("matched_amount".into(), json!(matched));
        fields.insert("matched_at".into(), json!(Utc::now()));
        invoice.merge(&fields);
        invoice.data.remove("decision");
        emit.say(&format!(
            "Three-way match for {} against {po_number}.\n\n{}",
            invoice.title,
            findings_table(&findings)
        ));
        state.findings = findings;
        state.pay_date = pay_date.clone();

        if count == 0 {
            invoice.status = "matched".into();
            invoice.data.insert("approved_amount".into(), json!(total));
            self.invoices.save(&mut invoice).await?;
            state.stage = Stage::AwaitingPayment;
            state.amount = total;
            let request = payment_request(&invoice, total, &pay_date)?;
            return Ok((state, Outcome::Hold(request, "Every rule passes. The invoice is matched.".into())));
        }
        invoice.status = PENDING_APPROVAL.into();
        invoice.data.remove("approved_amount");
        self.invoices.save(&mut invoice).await?;
        if is_controller(&user) {
            let request = decision_request(count)?;
            return Ok((state, Outcome::Hold(request, String::new())));
        }
        state.stage = Stage::Done;
        Ok((state, Outcome::Done(awaiting_controller(&invoice))))
    }

    /// Continue from the person's answer.
    #[allow(clippy::too_many_lines)] // One arm per stage.
    async fn proceed(
        &self,
        state: &mut State,
        answer: &ActionResponseEnvelope,
        emit: &mut Emitter,
        ctx: &RuntimeContext,
    ) -> RiverbaseResult<Outcome> {
        let user = emit.input.user_id().unwrap_or_default();
        let mut invoice = require(self.invoices.as_ref(), SHOWCASE, state.invoice_id).await?;
        let payload = answer.payload.clone().unwrap_or_else(|| json!({}));
        match state.stage {
            Stage::AwaitingDecision => {
                let choice = payload["selected_choices"][0].as_str().unwrap_or(CHOICES[3]);
                let decision = match choice {
                    c if c == CHOICES[0] => "approve_full",
                    c if c == CHOICES[1] => "pay_matched",
                    c if c == CHOICES[2] => "reject",
                    _ => "hold",
                };
                state.decision = Some(decision.into());
                state.stage = Stage::AwaitingNote;
                let request = validate_action_request(build_action_request(ActionRequestArgs {
                    kind: "beam.form_input".into(),
                    request_id: Uuid::new_v4().to_string(),
                    title: "Your note for the file".into(),
                    description: format!("Why \"{choice}\"? This goes on the invoice."),
                    payload: json!({"title": "Controller's note", "values": {"note": ""}}),
                    ..ActionRequestArgs::default()
                }))?;
                Ok(Outcome::Hold(Box::new(request), format!("Decision recorded: {choice}.")))
            }
            Stage::AwaitingNote => {
                state.note = payload["values"]["note"].as_str().map(str::trim).filter(|n| !n.is_empty()).map(str::to_owned);
                let decision = state.decision.clone().unwrap_or_else(|| "hold".into());
                let total = total_of(&invoice);
                let matched = invoice.number("matched_amount").unwrap_or_default();
                let amount = match decision.as_str() {
                    "approve_full" => total,
                    "pay_matched" => matched,
                    _ => 0.0,
                };
                if matches!(decision.as_str(), "approve_full" | "pay_matched") && amount <= 0.0 {
                    state.stage = Stage::Done;
                    return Ok(Outcome::Done(
                        "Nothing on the invoice matched, so there is no amount to pay. It stays pending approval.".into(),
                    ));
                }
                let prompt = format!(
                    "Write a short rationale (3-4 sentences, plain prose) for the accounts payable file.\n\
                     Invoice: {}, total {}. Matched amount: {}. Controller's decision: {}. \
                     Controller's note: {}.\nFindings:\n{}\n\
                     State the decision and the reasons. Do not invent facts.",
                    invoice.title,
                    dollars(total),
                    dollars(matched),
                    label(&decision),
                    state.note.clone().unwrap_or_else(|| "none".into()),
                    findings_table(&state.findings),
                );
                let rationale = rationale(ctx, emit, &prompt).await;
                emit.say(&rationale);
                invoice.data.insert(
                    "decision".into(),
                    json!({
                        "outcome": decision,
                        "label": label(&decision),
                        "amount": amount,
                        "note": state.note,
                        "rationale": rationale,
                        "decided_by": user,
                        "decided_at": Utc::now(),
                    }),
                );
                if matches!(decision.as_str(), "reject" | "hold") {
                    self.invoices.save_from(&mut invoice, PENDING_APPROVAL).await?;
                    let reason = state.note.clone().unwrap_or(rationale);
                    let (tool, done) = if decision == "reject" {
                        (REJECT_INVOICE, "rejected")
                    } else {
                        (HOLD_INVOICE, "put on hold")
                    };
                    call(emit, ctx, tool, &json!({"invoice_id": invoice.id.to_string(), "reason": reason}), false).await?;
                    state.stage = Stage::Done;
                    return Ok(Outcome::Done(format!("{} is {done}. Nothing will be paid.", invoice.title)));
                }
                invoice.status = "approved".into();
                invoice.data.insert("approved_amount".into(), json!(amount));
                // Only from pending approval: a second run on an invoice
                // another run already decided is refused here.
                self.invoices.save_from(&mut invoice, PENDING_APPROVAL).await?;
                state.amount = amount;
                state.stage = Stage::AwaitingPayment;
                let request = payment_request(&invoice, amount, &state.pay_date)?;
                Ok(Outcome::Hold(request, format!("{} is approved for {}.", invoice.title, dollars(amount))))
            }
            Stage::AwaitingPayment => {
                let approved = answer.status == ActionResponseStatus::Submitted && payload["approved"].as_bool() == Some(true);
                state.stage = Stage::Done;
                if !approved {
                    return Ok(Outcome::Done(format!(
                        "No payment was scheduled. {} stays {}.",
                        invoice.title,
                        invoice.status.replace('_', " ")
                    )));
                }
                let args = json!({"invoice_id": invoice.id.to_string(), "amount": state.amount, "pay_date": state.pay_date});
                let reply = call(emit, ctx, SCHEDULE_PAYMENT, &args, true).await?;
                let reference = serde_json::from_str::<Value>(&reply)
                    .ok()
                    .and_then(|v| v["payment"]["reference"].as_str().map(str::to_owned))
                    .unwrap_or_default();
                Ok(Outcome::Done(format!(
                    "Payment scheduled: {} on {} ({reference}).",
                    dollars(state.amount),
                    state.pay_date
                )))
            }
            Stage::Done => Err(SHC_003.with_data("this match is finished")),
        }
    }

    /// Save the run and end the segment for `outcome`.
    async fn conclude(&self, state: State, outcome: Outcome, emit: &mut Emitter, ctx: &RuntimeContext) -> RiverbaseResult<()> {
        let mut state = state;
        match outcome {
            Outcome::Hold(request, note) => {
                if !note.is_empty() {
                    emit.say(&note);
                }
                let action_id = Uuid::new_v4().to_string();
                state.waiting = Some((action_id.clone(), request.request_id.clone()));
                let record = Self::record(emit.execution_id, emit.run_id, &state, status::INTERRUPTED);
                self.states.save(&record, ctx.lease.as_ref()).await?;
                let entry = entry(&request, &action_id);
                let mut event = RuntimeEvent::new(RuntimeEventType::ActionRequired, emit.execution_id, emit.run_id);
                event.action = Some(entry.clone());
                emit.send(event);
                let mut result = emit.result(&emit.said.join("\n\n"));
                result.interrupted = true;
                result.action_required = Some(vec![entry]);
                emit.finish(&result);
            }
            Outcome::Done(text) => {
                state.waiting = None;
                let record = Self::record(emit.execution_id, emit.run_id, &state, status::COMPLETED);
                self.states.save(&record, ctx.lease.as_ref()).await?;
                emit.say(&text);
                let result = emit.result(&emit.said.join("\n\n"));
                emit.finish(&result);
            }
        }
        Ok(())
    }
}

/// What the clerk is told when an invoice has exceptions.
fn awaiting_controller(invoice: &Record) -> String {
    let failed: Vec<String> = invoice
        .field("findings")
        .and_then(|f| serde_json::from_value::<Vec<Finding>>(f.clone()).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|f| !f.pass)
        .map(|f| format!("{} ({})", f.rule, f.value))
        .collect();
    format!(
        "{} has {} exception(s): {}. It is now pending the controller's approval. A clerk cannot approve \
         exceptions, so the controller must decide whether to pay, pay part, reject or hold it.",
        invoice.title,
        failed.len(),
        failed.join("; ")
    )
}

fn only_controller(user: &str) -> RiverbaseResult<()> {
    if is_controller(user) {
        Ok(())
    } else {
        Err(SHC_004.with_data("only the controller decides an invoice with exceptions"))
    }
}

fn label(decision: &str) -> &'static str {
    match decision {
        "approve_full" => CHOICES[0],
        "pay_matched" => CHOICES[1],
        "reject" => CHOICES[2],
        _ => CHOICES[3],
    }
}

/// One model call through a nested `DirectModelRuntime`; a failure becomes a
/// note in the rationale instead of failing the match. The call is recorded
/// as a `rationale` step and its tokens count toward the run.
async fn rationale(ctx: &RuntimeContext, emit: &mut Emitter, prompt: &str) -> String {
    let runtime = DirectModelRuntime::model(
        Some("You are a careful accounts payable assistant. Write factual, concise prose.".into()),
        Some("fast".into()),
    );
    let mut input = AgentInput::text(prompt);
    input.session_id = emit.input.session_id;
    input.agent_name.clone_from(&emit.input.agent_name);
    input.auth_context.clone_from(&emit.input.auth_context);
    let request = RuntimeStartRequest {
        execution_id: emit.execution_id,
        run_id: emit.run_id,
        input,
        runtime_ref: None,
    };
    match run_nested(&runtime, request, ctx.clone(), "rationale", 1).await {
        Ok((result, events)) => {
            for event in events {
                emit.send(event);
            }
            let text = result.text_content();
            if text.trim().is_empty() {
                "(The model returned no rationale.)".into()
            } else {
                text
            }
        }
        Err(e) => format!("(Rationale unavailable: {})", e.errmesg),
    }
}

fn stream(rx: mpsc::UnboundedReceiver<RiverbaseResult<RuntimeEvent>>) -> EventStream {
    futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|e| (e, rx)) }).boxed()
}

#[async_trait]
impl AgentRuntime for ApMatch {
    fn runtime_name(&self) -> &str {
        RUNTIME_NAME
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming: true,
            durable_execution: true,
            resume: true,
            cancellation: true,
            state_inspection: true,
            checkpointing: true,
            ..RuntimeCapabilities::default()
        }
    }

    fn adapter_version(&self) -> &str {
        ADAPTER_VERSION
    }

    fn library_version(&self) -> &str {
        LIBRARY_VERSION
    }

    fn allocate_ref(&self, execution_id: Uuid) -> RuntimeExecutionRef {
        let mut reference = Map::new();
        reference.insert("state_id".into(), Value::String(execution_id.to_string()));
        RuntimeExecutionRef {
            runtime: RUNTIME_NAME.into(),
            runtime_version: Some(ADAPTER_VERSION.into()),
            reference,
        }
    }

    fn start(&self, request: RuntimeStartRequest, context: RuntimeContext) -> EventStream {
        let (tx, rx) = mpsc::unbounded_channel();
        let this = self.clone();
        tokio::spawn(async move {
            let mut emit = Emitter {
                tx,
                execution_id: request.execution_id,
                run_id: request.run_id,
                input: request.input,
                sequence: 0,
                said: Vec::new(),
            };
            let ran = async {
                let (state, outcome) = this.begin(&mut emit, &context).await?;
                this.conclude(state, outcome, &mut emit, &context).await
            }
            .await;
            if let Err(e) = ran {
                emit.fail(e);
            }
        });
        stream(rx)
    }

    fn resume(&self, request: RuntimeResumeRequest, context: RuntimeContext) -> EventStream {
        let (tx, rx) = mpsc::unbounded_channel();
        let this = self.clone();
        tokio::spawn(async move {
            let mut emit = Emitter {
                tx,
                execution_id: request.execution_id,
                run_id: request.run_id,
                input: request.input.clone(),
                sequence: 0,
                said: Vec::new(),
            };
            let ran = async {
                let record = this
                    .states
                    .load(request.execution_id)
                    .await?
                    .ok_or_else(|| SHC_005.with_data("no saved match for this execution"))?;
                let mut state: State =
                    serde_json::from_value(record.state).map_err(|e| SHC_005.with_data(e.to_string()))?;
                let Some((action_id, request_id)) = state.waiting.clone() else {
                    return Err(SHC_003.with_data("this match is not waiting for an answer"));
                };
                let answer = request
                    .actions
                    .iter()
                    .find(|a| a.action_id == action_id || a.request_id == request_id)
                    .ok_or_else(|| SHC_005.with_data("no answer for the match's question"))?;
                // Only the controller answers the decision and the note; checked
                // before the claim, so a refused answer leaves the run waiting.
                if matches!(state.stage, Stage::AwaitingDecision | Stage::AwaitingNote) {
                    only_controller(&request.input.user_id().unwrap_or_default())?;
                }
                if !this
                    .states
                    .claim(request.execution_id, status::INTERRUPTED, status::RUNNING, request.run_id)
                    .await?
                {
                    return Err(SHC_003.with_data("another worker is already continuing this match"));
                }
                let outcome = this.proceed(&mut state, answer, &mut emit, &context).await?;
                this.conclude(state, outcome, &mut emit, &context).await
            }
            .await;
            if let Err(e) = ran {
                emit.fail(e);
            }
        });
        stream(rx)
    }

    async fn cancel(&self, runtime_ref: &RuntimeExecutionRef, _context: &RuntimeContext) -> RiverbaseResult<()> {
        if let Some(id) = runtime_ref
            .reference
            .get("state_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
        {
            self.states.set_status(id, status::CANCELLED).await?;
        }
        Ok(())
    }

    async fn inspect(
        &self,
        runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<Option<RuntimeState>> {
        let Some(id) = runtime_ref
            .reference
            .get("state_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
        else {
            return Ok(None);
        };
        Ok(self.states.load(id).await?.map(|record| RuntimeState {
            runtime_ref: runtime_ref.clone(),
            status: Some(record.status),
            metadata: record.state.as_object().cloned().unwrap_or_default(),
        }))
    }
}
