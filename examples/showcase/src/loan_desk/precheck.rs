//! `underwriting_precheck`: an officer's pre-check of a submitted
//! application, as a step runtime (plan 005's shape B). Each step is plain
//! Rust; the run is saved in the beam's `RuntimeStateStore` whenever it waits
//! for the officer, so any worker can continue it.
//!
//! 1. Load the application (it must be submitted) and mark it in pre-check.
//! 2. Pull credit through MCP (`credit__pull_report`).
//! 3. Apply the program's rules: score, LTV, DTI, down payment, loan limit.
//! 4. On an exception, ask the officer to decide (`beam.choice_question`),
//!    then for a note (`beam.form_input`).
//! 5. Draft the rationale with one model call (a nested `DirectModelRuntime`
//!    on the `fast` alias).
//! 6. Ask the officer to approve committing the packet (`beam.tool_approval`
//!    for `write_packet`), then commit it and record the decision.

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

use crate::loan_desk::data::{require, Application, ApplicationStatus, ApplicationStore};
use crate::loan_desk::errors::{LND_003, LND_007};
use crate::loan_desk::packet::WRITE_PACKET;
use crate::loan_desk::programs::program;

pub const RUNTIME_NAME: &str = "loan-desk-precheck";
const ADAPTER_VERSION: &str = "1";
const LIBRARY_VERSION: &str = "loan-desk-1";
/// The MCP credit bureau tool.
pub const PULL_CREDIT: &str = "credit__pull_report";

const DECIDE: [&str; 3] = [
    "Approve with exception",
    "Refer to an underwriter",
    "Decline",
];

/// One rule's outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub rule: String,
    pub value: String,
    pub limit: String,
    pub pass: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage {
    AwaitingDecision,
    AwaitingNote,
    AwaitingPacketApproval,
    Done,
}

/// What a run keeps between its steps.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    application_id: Uuid,
    stage: Stage,
    credit: Option<Value>,
    findings: Vec<Finding>,
    /// `approve`, `refer` or `decline`.
    decision: Option<String>,
    note: Option<String>,
    summary: Option<String>,
    decision_markdown: Option<String>,
    /// The action the run waits for: `(action_id, request_id)`.
    waiting: Option<(String, String)>,
}

/// The pre-check runtime over the beam's state store and the applications.
#[derive(Clone)]
pub struct Precheck {
    states: Arc<dyn RuntimeStateStore>,
    applications: Arc<dyn ApplicationStore>,
}

impl Precheck {
    #[must_use]
    pub fn new(
        states: Arc<dyn RuntimeStateStore>,
        applications: Arc<dyn ApplicationStore>,
    ) -> Self {
        Self {
            states,
            applications,
        }
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

/// How a step ended: waiting for the officer, or done.
enum Outcome {
    Hold(Box<beam_types::ActionRequestEnvelope>, String),
    Done(String),
}

fn application_id(input: &AgentInput) -> RiverbaseResult<Uuid> {
    input
        .context
        .as_ref()
        .and_then(|c| c.facts.iter().find(|f| f.key == "application_id"))
        .and_then(|f| f.value.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| LND_007.with_data("the pre-check needs the application_id fact"))
}

fn number(fields: &Map<String, Value>, key: &str) -> f64 {
    fields
        .get(key)
        .and_then(|v| {
            v.as_f64().or_else(|| {
                v.as_str()
                    .and_then(|s| s.replace([',', '$'], "").trim().parse().ok())
            })
        })
        .unwrap_or(0.0)
}

/// The monthly payment on `principal` at 6.5% over 30 years, plus taxes and
/// insurance at 1.2% of the price a year.
fn housing_payment(principal: f64, price: f64) -> f64 {
    let r = 0.065 / 12.0;
    let n = 360.0;
    let pi = if principal > 0.0 {
        principal * r / (1.0 - (1.0 + r).powf(-n))
    } else {
        0.0
    };
    pi + price * 0.012 / 12.0
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn whole(x: f64) -> u64 {
    x.max(0.0).round() as u64
}

/// The program's rules on the application and the credit report.
#[must_use]
pub fn findings(application: &Application, credit: Option<&Value>) -> Vec<Finding> {
    let Some(p) = program(&application.program_id) else {
        return vec![Finding {
            rule: "program".into(),
            value: application.program_id.clone(),
            limit: "a known program".into(),
            pass: false,
        }];
    };
    let f = &application.fields;
    let (price, down, income, debts) = (
        number(f, "purchase_price"),
        number(f, "down_payment"),
        number(f, "gross_monthly_income"),
        number(f, "monthly_debts"),
    );
    let loan = (price - down).max(0.0);
    let ltv = if price > 0.0 {
        loan / price * 100.0
    } else {
        100.0
    };
    let down_pct = if price > 0.0 {
        down / price * 100.0
    } else {
        0.0
    };
    let bureau_debts = credit
        .and_then(|c| c.get("monthly_debts"))
        .and_then(Value::as_f64)
        .unwrap_or(debts)
        .max(debts);
    let dti = if income > 0.0 {
        (bureau_debts + housing_payment(loan, price)) / income * 100.0
    } else {
        100.0
    };
    let score = credit.and_then(|c| c.get("score")).and_then(Value::as_u64);
    let derogatory: Vec<String> = credit
        .and_then(|c| c.get("derogatory"))
        .and_then(Value::as_array)
        .map(|d| {
            d.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    vec![
        Finding {
            rule: "credit score".into(),
            value: score.map_or_else(|| "no score".into(), |s| s.to_string()),
            limit: format!("at least {}", p.min_credit_score),
            pass: score.is_some_and(|s| s >= u64::from(p.min_credit_score)),
        },
        Finding {
            rule: "derogatory marks".into(),
            value: if derogatory.is_empty() {
                "none".into()
            } else {
                derogatory.join("; ")
            },
            limit: "none in 12 months".into(),
            pass: derogatory.is_empty(),
        },
        Finding {
            rule: "loan-to-value".into(),
            value: format!("{ltv:.1}%"),
            limit: format!("at most {}%", p.max_ltv_pct),
            pass: ltv <= f64::from(p.max_ltv_pct),
        },
        Finding {
            rule: "down payment".into(),
            value: format!("{down_pct:.1}%"),
            limit: format!("at least {}%", p.min_down_payment_pct),
            pass: down_pct + 1e-9 >= f64::from(p.min_down_payment_pct),
        },
        Finding {
            rule: "debt-to-income".into(),
            value: format!("{dti:.1}%"),
            limit: format!("at most {}%", p.max_dti_pct),
            pass: dti <= f64::from(p.max_dti_pct),
        },
        Finding {
            rule: "loan amount".into(),
            value: format!("${}", whole(loan)),
            limit: format!("at most ${}", p.max_loan),
            pass: whole(loan) <= p.max_loan,
        },
    ]
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

fn entry(request: &beam_types::ActionRequestEnvelope, action_id: &str) -> Value {
    json!({"lc_action_id": request.request_id, "action_id": action_id, "action_request": request})
}

impl Precheck {
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

    /// Steps 1–4 (and 5–6 for a clean file).
    async fn begin(
        &self,
        emit: &mut Emitter,
        ctx: &RuntimeContext,
    ) -> RiverbaseResult<(State, Outcome)> {
        let id = application_id(&emit.input)?;
        let mut application = require(self.applications.as_ref(), id).await?;
        if !matches!(
            application.status,
            ApplicationStatus::Submitted | ApplicationStatus::Precheck
        ) {
            return Err(LND_003.with_data(format!(
                "a pre-check needs a submitted application; this one is {}",
                application.status.as_str()
            )));
        }
        application.status = ApplicationStatus::Precheck;
        self.applications.save(&mut application).await?;

        let call_id = Uuid::new_v4().to_string();
        let args = json!({"borrower_id": application.owner});
        let pulled = ctx
            .tool_runtime
            .execute(ToolInvocation {
                execution_id: emit.execution_id,
                run_id: emit.run_id,
                call_id: call_id.clone(),
                name: PULL_CREDIT.into(),
                args: args.clone(),
                input: Some(Arc::new(emit.input.clone())),
                allowed: None,
                dry_run: false,
                approved: false,
            })
            .await
            .unwrap_or_else(|e| json!({"error": e.errmesg}).to_string());
        emit.tool(PULL_CREDIT, &call_id, &args, &pulled);
        let credit = serde_json::from_str::<Value>(&pulled)
            .ok()
            .filter(|v| v.get("error").is_none() && v.get("score").is_some());

        let findings = findings(&application, credit.as_ref());
        let exceptions = findings.iter().filter(|f| !f.pass).count();
        emit.say(&format!(
            "Pre-check for application {id} ({}).\n\n{}",
            application.program_id,
            findings_table(&findings)
        ));
        let mut state = State {
            application_id: id,
            stage: Stage::AwaitingDecision,
            credit,
            findings,
            decision: None,
            note: None,
            summary: None,
            decision_markdown: None,
            waiting: None,
        };
        if exceptions > 0 {
            let request = validate_action_request(build_action_request(ActionRequestArgs {
                kind: "beam.choice_question".into(),
                request_id: Uuid::new_v4().to_string(),
                title: format!("{exceptions} exception(s): your decision"),
                description:
                    "The application is outside the program's rules. Decide how to proceed.".into(),
                payload: json!({"question": "How should this application proceed?", "choices": DECIDE}),
                ..ActionRequestArgs::default()
            }))?;
            return Ok((state, Outcome::Hold(Box::new(request), String::new())));
        }
        state.decision = Some("approve".into());
        let outcome = self.draft(&mut state, emit, ctx, &application).await?;
        Ok((state, outcome))
    }

    /// Steps 5–6: the rationale, then the packet approval.
    async fn draft(
        &self,
        state: &mut State,
        emit: &mut Emitter,
        ctx: &RuntimeContext,
        application: &Application,
    ) -> RiverbaseResult<Outcome> {
        let decision = state.decision.clone().unwrap_or_else(|| "refer".into());
        let prompt = format!(
            "Write a short underwriting rationale (4-6 sentences, plain prose) for a loan officer's file.\n\
             Program: {}. Decision: {decision}. Officer's note: {}.\nFindings:\n{}\n\
             Credit report: {}.\nState the decision, the reasons, and any conditions. Do not invent facts.",
            application.program_id,
            state.note.clone().unwrap_or_else(|| "none".into()),
            findings_table(&state.findings),
            state.credit.clone().unwrap_or(json!("unavailable")),
        );
        let rationale = rationale(ctx, emit, &prompt).await;
        emit.say(&rationale);
        let name = |key: &str| {
            application
                .fields
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned()
        };
        let summary = format!(
            "# Application {}\n\nBorrower: {} {}\nProgram: {}\n\n## Findings\n\n{}",
            application.id,
            name("first_name"),
            name("last_name"),
            application.program_id,
            findings_table(&state.findings)
        );
        let decision_markdown = format!(
            "# Decision: {decision}\n\nOfficer's note: {}\n\n## Rationale\n\n{rationale}\n",
            state.note.clone().unwrap_or_else(|| "none".into())
        );
        state.summary = Some(summary.clone());
        state.decision_markdown = Some(decision_markdown.clone());
        state.stage = Stage::AwaitingPacketApproval;
        let call_id = Uuid::new_v4().to_string();
        let args = json!({
            "application_id": application.id.to_string(),
            "summary_markdown": summary,
            "decision_markdown": decision_markdown,
        });
        let request = validate_action_request(build_action_request(ActionRequestArgs {
            kind: TOOL_APPROVAL_KIND.into(),
            request_id: call_id.clone(),
            title: format!("Approve {WRITE_PACKET}"),
            description:
                "Commit the application's packet (summary and decision) to the packet repository."
                    .into(),
            payload: json!({
                "tool_name": WRITE_PACKET,
                "call_id": call_id,
                "args": args,
                "risk": "medium",
                "remember_options": ["once"],
                "decided_by": {"layer": "agent", "reason": "every packet commit is approved"}
            }),
            metadata: Some(
                json!({"tool_name": WRITE_PACKET, "tool_args": args, "tool_call_id": call_id}),
            ),
            ..ActionRequestArgs::default()
        }))?;
        Ok(Outcome::Hold(Box::new(request), String::new()))
    }

    /// Continue from the officer's answer.
    async fn proceed(
        &self,
        state: &mut State,
        answer: &ActionResponseEnvelope,
        emit: &mut Emitter,
        ctx: &RuntimeContext,
    ) -> RiverbaseResult<Outcome> {
        let mut application = require(self.applications.as_ref(), state.application_id).await?;
        let payload = answer.payload.clone().unwrap_or_else(|| json!({}));
        match state.stage {
            Stage::AwaitingDecision => {
                let choice = payload["selected_choices"][0].as_str().unwrap_or(DECIDE[1]);
                let decision = match choice {
                    c if c == DECIDE[0] => "approve",
                    c if c == DECIDE[2] => "decline",
                    _ => "refer",
                };
                state.decision = Some(decision.into());
                state.stage = Stage::AwaitingNote;
                let request = validate_action_request(build_action_request(ActionRequestArgs {
                    kind: "beam.form_input".into(),
                    request_id: Uuid::new_v4().to_string(),
                    title: "Your note for the file".into(),
                    description: format!("Why {decision}? This goes into the packet."),
                    payload: json!({"title": "Officer's note", "values": {"note": ""}}),
                    ..ActionRequestArgs::default()
                }))?;
                Ok(Outcome::Hold(
                    Box::new(request),
                    format!("Decision recorded: {decision}."),
                ))
            }
            Stage::AwaitingNote => {
                state.note = payload["values"]["note"].as_str().map(str::to_owned);
                self.draft(state, emit, ctx, &application).await
            }
            Stage::AwaitingPacketApproval => {
                let approved = answer.status == ActionResponseStatus::Submitted
                    && payload["approved"].as_bool() == Some(true);
                let decision = state.decision.clone().unwrap_or_else(|| "refer".into());
                let mut commit = Value::Null;
                if approved {
                    let call_id = Uuid::new_v4().to_string();
                    let args = json!({
                        "application_id": state.application_id.to_string(),
                        "summary_markdown": state.summary,
                        "decision_markdown": state.decision_markdown,
                    });
                    let written = ctx
                        .tool_runtime
                        .execute(ToolInvocation {
                            execution_id: emit.execution_id,
                            run_id: emit.run_id,
                            call_id: call_id.clone(),
                            name: WRITE_PACKET.into(),
                            args: args.clone(),
                            input: Some(Arc::new(emit.input.clone())),
                            allowed: None,
                            dry_run: false,
                            approved: true,
                        })
                        .await?;
                    emit.tool(WRITE_PACKET, &call_id, &args, &written);
                    commit = serde_json::from_str::<Value>(&written)
                        .ok()
                        .and_then(|v| v.get("committed").cloned())
                        .unwrap_or(Value::Null);
                }
                application.status = match decision.as_str() {
                    "approve" => ApplicationStatus::Approved,
                    "decline" => ApplicationStatus::Declined,
                    _ => ApplicationStatus::Referred,
                };
                application.decision = Some(json!({
                    "outcome": decision,
                    "findings": state.findings,
                    "note": state.note,
                    "packet_commit": commit,
                    "decided_at": chrono::Utc::now(),
                }));
                self.applications.save(&mut application).await?;
                state.stage = Stage::Done;
                Ok(Outcome::Done(if approved {
                    format!(
                        "Recorded: {decision}. The packet is committed ({}).",
                        commit.as_str().unwrap_or("?")
                    )
                } else {
                    format!("Recorded: {decision}. The packet was not written.")
                }))
            }
            Stage::Done => Err(LND_003.with_data("this pre-check is finished")),
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
/// note in the rationale instead of failing the pre-check. The call is
/// recorded as a `rationale` step, and its tokens count toward the run
/// (plan 023 D2).
async fn rationale(ctx: &RuntimeContext, emit: &mut Emitter, prompt: &str) -> String {
    let runtime = DirectModelRuntime::model(
        Some(
            "You are a careful mortgage underwriting assistant. Write factual, concise prose."
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
impl AgentRuntime for Precheck {
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
                    .ok_or_else(|| LND_007.with_data("no saved pre-check for this execution"))?;
                let mut state: State = serde_json::from_value(record.state)
                    .map_err(|e| LND_007.with_data(e.to_string()))?;
                let Some((action_id, request_id)) = state.waiting.clone() else {
                    return Err(LND_003.with_data("this pre-check is not waiting for an answer"));
                };
                let answer = request
                    .actions
                    .iter()
                    .find(|a| a.action_id == action_id || a.request_id == request_id)
                    .ok_or_else(|| LND_007.with_data("no answer for the pre-check's question"))?;
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
                        LND_003.with_data("another worker is already continuing this pre-check")
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
