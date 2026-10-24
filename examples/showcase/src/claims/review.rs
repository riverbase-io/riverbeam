//! `claim_review`: an adjuster's review of a filed claim, as a step runtime
//! (the loan desk's pre-check shape). Each step is plain Rust; the run is
//! saved in the beam's `RuntimeStateStore` whenever it waits for the
//! adjuster, so any worker can continue it.
//!
//! 1. Load the claim (it must be filed, and the caller an adjuster) and mark
//!    it in review.
//! 2. Apply the coverage rules and the fraud signals ([`crate::claims::rules`]).
//! 3. When a rule fails or a signal fires, ask the adjuster to decide
//!    (`beam.choice_question`: approve, partial approval, deny, investigate).
//! 4. Ask for the adjuster's note (`beam.form_input`; and the amount for a
//!    partial approval).
//! 5. Draft the rationale with one model call (a nested `DirectModelRuntime`
//!    on the `fast` alias).
//! 6. For an approval, ask the adjuster to approve `approve_payout`
//!    (`beam.tool_approval`), then pay and record the decision. A denial or
//!    an investigation ends here without a payout.

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
    ActionResponseEnvelope, ActionResponseStatus, AgentInput, AgentMessage, AgentMessageRole,
    AgentMessageType, AgentResult, ContentPart,
};
use futures_util::StreamExt;
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::claims::personas::ADJUSTER;
use crate::claims::policies::policy;
use crate::claims::rules::{assess, cents, dollars, loss_type, number, Assessment};
use crate::claims::tools::APPROVE_PAYOUT;
use crate::claims::{CLAIM, SHOWCASE};
use crate::errors::{SHC_002, SHC_003, SHC_004, SHC_005};
use crate::records::{require, Record, RecordStore};
use crate::registry::role_in;

pub const RUNTIME_NAME: &str = "claims-review";
const ADAPTER_VERSION: &str = "1";
const LIBRARY_VERSION: &str = "claims-1";
/// The status a claim is reviewed in; the decision is written only from it.
const IN_REVIEW: &str = "in_review";

const APPROVE: &str = "Approve the claim";
const PARTIAL: &str = "Approve a partial amount";
const DENY: &str = "Deny the claim";
const INVESTIGATE: &str = "Send to investigation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage {
    AwaitingDecision,
    AwaitingNote,
    AwaitingPayoutApproval,
    Done,
}

/// What a run keeps between its steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    claim_id: Uuid,
    stage: Stage,
    assessment: Assessment,
    /// `approve`, `partial`, `deny` or `investigate`.
    decision: Option<String>,
    note: Option<String>,
    /// What the adjuster is asked to pay.
    payout: Option<f64>,
    rationale: Option<String>,
    /// The action the run waits for: `(action_id, request_id)`.
    waiting: Option<(String, String)>,
}

impl State {
    fn decision(&self) -> &str {
        self.decision.as_deref().unwrap_or("investigate")
    }

    fn pays(&self) -> bool {
        matches!(self.decision(), "approve" | "partial")
    }
}

/// The claim status each decision leads to (a payout then makes it `paid`).
fn status_for(decision: &str) -> &'static str {
    match decision {
        "approve" => "approved",
        "partial" => "partially_approved",
        "deny" => "denied",
        _ => "investigating",
    }
}

/// What the adjuster is about to do, for the note's prompt.
fn action_of(decision: &str) -> &'static str {
    match decision {
        "approve" => "approve the claim",
        "partial" => "approve a partial amount",
        "deny" => "deny the claim",
        _ => "send the claim to investigation",
    }
}

/// The review runtime over the beam's state store and the claims.
#[derive(Clone)]
pub struct Review {
    states: Arc<dyn RuntimeStateStore>,
    claims: Arc<dyn RecordStore>,
}

impl Review {
    #[must_use]
    pub fn new(states: Arc<dyn RuntimeStateStore>, claims: Arc<dyn RecordStore>) -> Self {
        Self { states, claims }
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
        let mut event = RuntimeEvent::new(
            RuntimeEventType::MessageCompleted,
            self.execution_id,
            self.run_id,
        );
        event.message = Some(self.message(AgentMessage::text(AgentMessageRole::Ai, text)));
        self.send(event);
    }

    fn tool(&mut self, name: &str, call_id: &str, args: &Value, result: &str) {
        let mut event = RuntimeEvent::new(
            RuntimeEventType::MessageCompleted,
            self.execution_id,
            self.run_id,
        );
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
        event.payload.insert(
            "result".into(),
            serde_json::to_value(result).unwrap_or(Value::Null),
        );
        self.send(event);
    }

    fn fail(&mut self, err: RiverbaseError) {
        let _ = self.tx.send(Err(err));
    }
}

/// How a step ended: waiting for the adjuster, or done.
enum Outcome {
    Hold(Box<beam_types::ActionRequestEnvelope>, String),
    Done(String),
}

fn claim_id(input: &AgentInput) -> RiverbaseResult<Uuid> {
    input
        .context
        .as_ref()
        .and_then(|c| c.facts.iter().find(|f| f.key == "claim_id"))
        .and_then(|f| f.value.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| SHC_005.with_data("the review needs the claim_id fact"))
}

fn findings_table(a: &Assessment) -> String {
    let mut out = String::from("| Rule | Value | Limit | |\n|---|---|---|---|\n");
    for f in &a.findings {
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

fn signals_table(a: &Assessment) -> String {
    let mut out = String::from("| Fraud signal | Detail | |\n|---|---|---|\n");
    for s in &a.signals {
        let _ = writeln!(
            out,
            "| {} | {} | {} |",
            s.signal,
            s.detail,
            if s.fired { "**fired**" } else { "clear" }
        );
    }
    out
}

fn report(claim: &Record, a: &Assessment) -> String {
    let money = match a.estimate {
        Some(estimate) => format!(
            "Estimate {}, deductible {}, payable {}.",
            dollars(estimate),
            dollars(a.deductible),
            a.payable.map_or_else(|| "n/a".into(), dollars)
        ),
        None => "There is no repair estimate on the claim.".into(),
    };
    format!(
        "Review of \"{}\" (policy {}, {} loss).\n\n{}\n{}\n{money}",
        claim.title,
        claim.text("policy_number").unwrap_or("unknown"),
        loss_type(claim),
        findings_table(a),
        signals_table(a)
    )
}

fn entry(request: &beam_types::ActionRequestEnvelope, action_id: &str) -> Value {
    json!({"lc_action_id": request.request_id, "action_id": action_id, "action_request": request})
}

/// The choices the adjuster has: without an estimate nothing can be paid.
fn choices(a: &Assessment) -> Vec<&'static str> {
    if a.payable.is_some() {
        vec![APPROVE, PARTIAL, DENY, INVESTIGATE]
    } else {
        vec![DENY, INVESTIGATE]
    }
}

/// The arguments of the payout the adjuster is asked to approve.
fn payout_args(state: &State) -> Value {
    json!({
        "claim_id": state.claim_id.to_string(),
        "amount": state.payout,
        "reason": state.note.clone().unwrap_or_else(|| "Approved by the adjuster".into()),
    })
}

/// The adjuster's note form (and the amount, for a partial approval).
fn note_request(state: &State, hint: &str) -> RiverbaseResult<Box<beam_types::ActionRequestEnvelope>> {
    let mut values = Map::new();
    values.insert("note".into(), json!(""));
    let mut description = format!("Why {}? This goes on the claim.", action_of(state.decision()));
    if state.decision() == "partial" {
        values.insert("partial_amount".into(), json!(""));
        let _ = write!(
            description,
            " Enter the amount to pay: at most {}.",
            dollars(state.assessment.payable.unwrap_or(0.0))
        );
    }
    if !hint.is_empty() {
        let _ = write!(description, " {hint}");
    }
    Ok(Box::new(validate_action_request(build_action_request(ActionRequestArgs {
        kind: "beam.form_input".into(),
        request_id: Uuid::new_v4().to_string(),
        title: "Your note for the file".into(),
        description,
        payload: json!({"title": "Adjuster's note", "values": values}),
        ..ActionRequestArgs::default()
    }))?))
}

impl Review {
    fn record(
        execution_id: Uuid,
        run_id: Uuid,
        state: &State,
        run_status: &str,
    ) -> RuntimeStateRecord {
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
            updated_at: chrono::Utc::now(),
        }
    }

    /// Steps 1–4.
    async fn begin(&self, emit: &mut Emitter) -> RiverbaseResult<(State, Outcome)> {
        let user = emit.input.user_id();
        if role_in(SHOWCASE, user.as_deref()) != Some(ADJUSTER) {
            return Err(SHC_004.with_data("only an adjuster reviews a claim"));
        }
        let id = claim_id(&emit.input)?;
        let mut claim = require(self.claims.as_ref(), SHOWCASE, id).await?;
        if claim.kind != CLAIM {
            return Err(SHC_002.with_data(format!("claim {id} was not found")));
        }
        if !matches!(claim.status.as_str(), "filed" | "in_review") {
            return Err(SHC_003.with_data(format!(
                "a review needs a filed claim; this one is {}",
                claim.status
            )));
        }
        let from = claim.status.clone();
        claim.status = IN_REVIEW.into();
        self.claims.save_from(&mut claim, &from).await?;

        let assessment = assess(&claim, claim.text("policy_number").and_then(policy));
        emit.say(&report(&claim, &assessment));
        let exceptions = assessment.failed() + assessment.fired();
        let mut state = State {
            claim_id: id,
            stage: Stage::AwaitingDecision,
            assessment,
            decision: None,
            note: None,
            payout: None,
            rationale: None,
            waiting: None,
        };
        if exceptions > 0 {
            let request = validate_action_request(build_action_request(ActionRequestArgs {
                kind: "beam.choice_question".into(),
                request_id: Uuid::new_v4().to_string(),
                title: format!("{exceptions} exception(s): your decision"),
                description: "A coverage rule failed or a fraud signal fired. Decide how to proceed."
                    .into(),
                payload: json!({"question": "How should this claim proceed?", "choices": choices(&state.assessment)}),
                ..ActionRequestArgs::default()
            }))?;
            return Ok((state, Outcome::Hold(Box::new(request), String::new())));
        }
        // A clean claim still gets the adjuster's note and payout approval.
        state.decision = Some("approve".into());
        state.stage = Stage::AwaitingNote;
        let request = note_request(&state, "")?;
        Ok((state, Outcome::Hold(request, "No exceptions: the claim passes every rule.".into())))
    }

    /// Step 5–6: the rationale, then the payout approval or the end.
    async fn conclude_note(
        &self,
        state: &mut State,
        emit: &mut Emitter,
        ctx: &RuntimeContext,
        claim: &mut Record,
    ) -> RiverbaseResult<Outcome> {
        let a = &state.assessment;
        let prompt = format!(
            "Write a short claims decision rationale (3-5 sentences, plain prose) for an adjuster's file.\n\
             Decision: {}. Payout: {}. Adjuster's note: {}.\nCoverage rules:\n{}\nFraud signals:\n{}\n\
             State the decision and the reasons. Do not invent facts. A rule that failed or a signal \
             that fired stays so: say the adjuster decided past it, never that it passed or cleared.",
            action_of(state.decision()),
            state.payout.map_or_else(|| "none".into(), dollars),
            state.note.clone().unwrap_or_else(|| "none".into()),
            findings_table(a),
            signals_table(a),
        );
        let rationale = rationale(ctx, emit, &prompt).await;
        emit.say(&rationale);
        state.rationale = Some(rationale);
        if !state.pays() {
            Self::record_decision(claim, state);
            self.claims.save_from(claim, IN_REVIEW).await?;
            state.stage = Stage::Done;
            return Ok(Outcome::Done(format!(
                "Recorded: {}. No payout was issued.",
                status_for(state.decision()).replace('_', " ")
            )));
        }
        state.stage = Stage::AwaitingPayoutApproval;
        let call_id = Uuid::new_v4().to_string();
        let args = payout_args(state);
        let request = validate_action_request(build_action_request(ActionRequestArgs {
            kind: TOOL_APPROVAL_KIND.into(),
            request_id: call_id.clone(),
            title: format!("Approve {APPROVE_PAYOUT}"),
            description: format!(
                "Pay {} on this claim. Money leaves Harbor Mutual.",
                state.payout.map_or_else(|| "n/a".into(), dollars)
            ),
            payload: json!({
                "tool_name": APPROVE_PAYOUT,
                "call_id": call_id,
                "args": args,
                "risk": "high",
                "remember_options": ["once"],
                "decided_by": {"layer": "agent", "reason": "every payout is approved by the adjuster"}
            }),
            metadata: Some(
                json!({"tool_name": APPROVE_PAYOUT, "tool_args": args, "tool_call_id": call_id}),
            ),
            ..ActionRequestArgs::default()
        }))?;
        Ok(Outcome::Hold(Box::new(request), String::new()))
    }

    /// The decision and what it was based on, on the claim.
    fn record_decision(claim: &mut Record, state: &State) {
        let decision = json!({
            "outcome": state.decision(),
            "findings": state.assessment.findings,
            "signals": state.assessment.signals,
            "estimate": state.assessment.estimate,
            "deductible": state.assessment.deductible,
            "payout_amount": state.payout,
            "note": state.note,
            "rationale": state.rationale,
            "decided_at": chrono::Utc::now(),
        });
        claim.data.insert("decision".into(), decision);
        claim.status = status_for(state.decision()).into();
    }

    /// Continue from the adjuster's answer.
    #[allow(clippy::too_many_lines)] // One arm per stage, read top to bottom.
    async fn proceed(
        &self,
        state: &mut State,
        answer: &ActionResponseEnvelope,
        emit: &mut Emitter,
        ctx: &RuntimeContext,
    ) -> RiverbaseResult<Outcome> {
        let mut claim = require(self.claims.as_ref(), SHOWCASE, state.claim_id).await?;
        let payload = answer.payload.clone().unwrap_or_else(|| json!({}));
        match state.stage {
            Stage::AwaitingDecision => {
                let choice = payload["selected_choices"][0].as_str().unwrap_or(INVESTIGATE);
                let allowed = choices(&state.assessment);
                let decision = match choice {
                    c if c == APPROVE && allowed.contains(&APPROVE) => "approve",
                    c if c == PARTIAL && allowed.contains(&PARTIAL) => "partial",
                    c if c == DENY => "deny",
                    _ => "investigate",
                };
                state.decision = Some(decision.into());
                state.stage = Stage::AwaitingNote;
                let request = note_request(state, "")?;
                Ok(Outcome::Hold(request, format!("Decision recorded: {decision}.")))
            }
            Stage::AwaitingNote => {
                state.note = payload["values"]["note"]
                    .as_str()
                    .map(str::trim)
                    .filter(|n| !n.is_empty())
                    .map(str::to_owned);
                let payable = state.assessment.payable;
                let decision = state.decision().to_owned();
                match decision.as_str() {
                    "approve" => state.payout = payable,
                    "partial" => {
                        let most = payable.unwrap_or(0.0);
                        let amount = number(&payload["values"]["partial_amount"])
                            .map(cents)
                            .filter(|a| *a > 0.0 && *a <= cents(most));
                        let Some(amount) = amount else {
                            let hint = format!(
                                "The amount must be more than $0 and at most {}.",
                                dollars(most)
                            );
                            return Ok(Outcome::Hold(note_request(state, &hint)?, String::new()));
                        };
                        state.payout = Some(amount);
                    }
                    _ => state.payout = None,
                }
                self.conclude_note(state, emit, ctx, &mut claim).await
            }
            Stage::AwaitingPayoutApproval => {
                let approved = answer.status == ActionResponseStatus::Submitted
                    && payload["approved"].as_bool() == Some(true);
                // The decision is on the claim before the tool runs: the
                // payout needs an approved claim, and then makes it paid. A
                // second review of the claim that already decided it is
                // refused here.
                Self::record_decision(&mut claim, state);
                self.claims.save_from(&mut claim, IN_REVIEW).await?;
                let mut reference = Value::Null;
                if approved {
                    let call_id = Uuid::new_v4().to_string();
                    let args = payout_args(state);
                    let paid = ctx
                        .tool_runtime
                        .execute(ToolInvocation {
                            execution_id: emit.execution_id,
                            run_id: emit.run_id,
                            call_id: call_id.clone(),
                            name: APPROVE_PAYOUT.into(),
                            args: args.clone(),
                            input: Some(Arc::new(emit.input.clone())),
                            allowed: None,
                            dry_run: false,
                            approved: true,
                        })
                        .await?;
                    emit.tool(APPROVE_PAYOUT, &call_id, &args, &paid);
                    reference = serde_json::from_str::<Value>(&paid)
                        .ok()
                        .map_or(Value::Null, |v| v["payout"]["reference"].clone());
                }
                state.stage = Stage::Done;
                let outcome = status_for(state.decision()).replace('_', " ");
                Ok(Outcome::Done(if approved {
                    format!(
                        "Recorded: {outcome}. Paid {} (reference {}).",
                        state.payout.map_or_else(|| "n/a".into(), dollars),
                        reference.as_str().unwrap_or("?")
                    )
                } else {
                    format!("Recorded: {outcome}. The payout was not issued.")
                }))
            }
            Stage::Done => Err(SHC_003.with_data("this review is finished")),
        }
    }

    /// Save the run and end the segment for `outcome`.
    async fn conclude(
        &self,
        state: State,
        outcome: Outcome,
        emit: &mut Emitter,
        ctx: &RuntimeContext,
    ) -> RiverbaseResult<()> {
        let mut state = state;
        match outcome {
            Outcome::Hold(request, note) => {
                if !note.is_empty() {
                    emit.say(&note);
                }
                let action_id = Uuid::new_v4().to_string();
                state.waiting = Some((action_id.clone(), request.request_id.clone()));
                let record =
                    Self::record(emit.execution_id, emit.run_id, &state, status::INTERRUPTED);
                self.states.save(&record, ctx.lease.as_ref()).await?;
                let entry = entry(&request, &action_id);
                let mut event = RuntimeEvent::new(
                    RuntimeEventType::ActionRequired,
                    emit.execution_id,
                    emit.run_id,
                );
                event.action = Some(entry.clone());
                emit.send(event);
                let mut result = emit.result(&emit.said.join("\n\n"));
                result.interrupted = true;
                result.action_required = Some(vec![entry]);
                emit.finish(&result);
            }
            Outcome::Done(text) => {
                state.waiting = None;
                let record =
                    Self::record(emit.execution_id, emit.run_id, &state, status::COMPLETED);
                self.states.save(&record, ctx.lease.as_ref()).await?;
                emit.say(&text);
                let result = emit.result(&emit.said.join("\n\n"));
                emit.finish(&result);
            }
        }
        Ok(())
    }
}

/// One model call through a nested `DirectModelRuntime`; a failure becomes a
/// note in the rationale instead of failing the review. The call is recorded
/// as a `rationale` step, and its tokens count toward the run.
async fn rationale(ctx: &RuntimeContext, emit: &mut Emitter, prompt: &str) -> String {
    let runtime = DirectModelRuntime::model(
        Some(
            "You are a careful auto claims assistant. Write factual, concise prose."
                .into(),
        ),
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
    futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|e| (e, rx)) })
        .boxed()
}

#[async_trait]
impl AgentRuntime for Review {
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
                let (state, outcome) = this.begin(&mut emit).await?;
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
                    .ok_or_else(|| SHC_005.with_data("no saved review for this execution"))?;
                let mut state: State = serde_json::from_value(record.state)
                    .map_err(|e| SHC_005.with_data(e.to_string()))?;
                let Some((action_id, request_id)) = state.waiting.clone() else {
                    return Err(SHC_003.with_data("this review is not waiting for an answer"));
                };
                let answer = request
                    .actions
                    .iter()
                    .find(|a| a.action_id == action_id || a.request_id == request_id)
                    .ok_or_else(|| SHC_005.with_data("no answer for the review's question"))?;
                if !this
                    .states
                    .claim(
                        request.execution_id,
                        status::INTERRUPTED,
                        status::RUNNING,
                        request.run_id,
                    )
                    .await?
                {
                    return Err(
                        SHC_003.with_data("another worker is already continuing this review")
                    );
                }
                let outcome = this
                    .proceed(&mut state, answer, &mut emit, &context)
                    .await?;
                this.conclude(state, outcome, &mut emit, &context).await
            }
            .await;
            if let Err(e) = ran {
                emit.fail(e);
            }
        });
        stream(rx)
    }

    async fn cancel(
        &self,
        runtime_ref: &RuntimeExecutionRef,
        _context: &RuntimeContext,
    ) -> RiverbaseResult<()> {
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
