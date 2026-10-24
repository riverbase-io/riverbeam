//! `ToolRuntime` (Python `tools/executor.py`).
//!
//! `execute` runs in Python's order:
//!
//! 1. the invocation allowlist: an undeclared tool is refused
//! 2. `dry_run`: a side-effecting tool (not allowlisted for dry runs) is skipped
//! 3. a client tool is refused (the client runs it; see [`ToolRuntime::client_result`]);
//!    then the tool policy ([`ToolRuntime::decide`], plan 023 part C): `deny`
//!    refuses, `ask` refuses unless the call was approved, and a rule or grant
//!    that cannot be read refuses ("tool policy unavailable", D8)
//! 4. idempotency: a remembered result is returned with `cached = true`
//! 5. the `BEFORE_TOOL` security stage
//! 6. input-schema validation (Python's checks: required args and simple types)
//! 7. execution with the timeout; failures retry up to `max_retries`, a timeout does not
//! 8. the output is bounded (`bound_tool_output`)
//! 9. the `AFTER_TOOL` security stage
//! 10. a successful side-effecting result is remembered
//!
//! Refusals and failures are results with `error` set, as in Python; only a
//! security block is an error.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use beam_runtime::ClientAnswer;
use beam_security::{SecurityContext, SecurityManagement, SecurityRequest, SecurityStage};
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use beam_store::tool_policy::{ToolGrantStore, ToolRuleStore};
use beam_types::tool_policy::{
    RememberOption, RuleScope, ToolDecision, ToolGrant, ToolMatch, ToolMode, ToolRule,
    ToolRuleRecord, ToolScope, GRANT_TTL_HOURS,
};

use crate::definition::{ToolCallContext, ToolDefinition};
use crate::idempotency::{
    IdempotencyKey, InMemoryToolIdempotencyStore, ToolIdempotencyStore, DEFAULT_TTL,
};
use crate::output::{bound_tool_output, MAX_RESULT_CHARS};
use crate::policy::{resolve_tool_mode, SecurityCeiling, ToolPolicyLayers};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolRuntimeResult {
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub cached: bool,
}

impl ToolRuntimeResult {
    fn error(message: impl Into<String>) -> Self {
        Self {
            error: Some(message.into()),
            ..Self::default()
        }
    }
}

/// One call's identity and invocation settings.
#[derive(Debug, Clone, Default)]
pub struct ExecuteContext {
    pub call: ToolCallContext,
    /// The tool revision a remembered result is keyed under; `default` when
    /// unset. `ToolService` leaves it unset (see `docs/backlog.md`).
    pub tool_revision: Option<String>,
    /// Python `invocation_tool_allowlist`; `None` means no restriction.
    pub allowed: Option<Vec<String>>,
    /// Python `invocation_dry_run`.
    pub dry_run: bool,
    /// A human approved this call.
    pub approved: bool,
}

/// Where tool rules and grants live (plan 023 part C).
#[derive(Clone)]
pub struct ToolPolicyStores {
    pub rules: Arc<dyn ToolRuleStore>,
    pub grants: Arc<dyn ToolGrantStore>,
}

pub struct ToolRuntime {
    store: Arc<dyn ToolIdempotencyStore>,
    security: Option<Arc<dyn SecurityManagement>>,
    /// Tenant and user rules and session grants; without them only the
    /// tool's own mode and the agent's rules apply.
    policy: Option<ToolPolicyStores>,
    /// Dry-run for every call.
    pub dry_run: bool,
    /// Side-effecting tools that still run in a dry run.
    pub dry_run_allowlist: HashSet<String>,
    pub max_result_chars: usize,
}

impl Default for ToolRuntime {
    fn default() -> Self {
        Self::new(Arc::new(InMemoryToolIdempotencyStore::new()))
    }
}

/// Python rc8 `ToolGate.audit` (`TOOL_APPROVAL`): the decision a call got.
/// `beam_tools` has no audit logger, so it is a structured log line.
fn audit_decision(definition: &ToolDefinition, decision: Option<&ToolDecision>, status: &str) {
    tracing::info!(
        target: "beam.audit",
        action = "tool_approval",
        tool = %definition.name,
        mode = decision.map_or("", |d| d.mode.as_str()),
        layer = decision.map_or("", |d| d.layer.as_str()),
        status,
        "tool_approval"
    );
}

/// The error's detail when it has one, else its message.
fn error_text(e: &RiverbaseError) -> String {
    e.errdata
        .get("detail")
        .and_then(Value::as_str)
        .map_or_else(|| e.errmesg.clone(), str::to_owned)
}

/// Required names and simple JSON types only.
fn validate_input(schema: &Value, args: &Map<String, Value>) -> Option<String> {
    let schema = schema.as_object().filter(|s| !s.is_empty())?;
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|n| !args.contains_key(*n))
        .collect();
    if !missing.is_empty() {
        let quoted: Vec<String> = missing.iter().map(|n| format!("'{n}'")).collect();
        return Some(format!("missing required args: [{}]", quoted.join(", ")));
    }
    let properties = schema.get("properties").and_then(Value::as_object)?;
    for (name, spec) in properties {
        let Some(value) = args.get(name) else {
            continue;
        };
        let expected = spec.get("type").and_then(Value::as_str);
        // Python's bool is an int, so an integer check accepts true/false.
        let ok = match expected {
            Some("string") => value.is_string(),
            Some("integer") => value.is_i64() || value.is_u64() || value.is_boolean(),
            Some("number") => value.is_number() || value.is_boolean(),
            Some("boolean") => value.is_boolean(),
            _ => true,
        };
        if !ok {
            return Some(format!(
                "invalid type for {name}: expected {}",
                expected.unwrap_or_default()
            ));
        }
    }
    None
}

impl ToolRuntime {
    #[must_use]
    pub fn new(store: Arc<dyn ToolIdempotencyStore>) -> Self {
        Self {
            store,
            security: None,
            policy: None,
            dry_run: false,
            dry_run_allowlist: HashSet::new(),
            max_result_chars: MAX_RESULT_CHARS,
        }
    }

    #[must_use]
    pub fn with_security(mut self, security: Arc<dyn SecurityManagement>) -> Self {
        self.security = Some(security);
        self
    }

    /// Read tenant and user rules and session grants from these stores.
    #[must_use]
    pub fn with_policy(
        mut self,
        rules: Arc<dyn ToolRuleStore>,
        grants: Arc<dyn ToolGrantStore>,
    ) -> Self {
        self.policy = Some(ToolPolicyStores { rules, grants });
        self
    }

    #[must_use]
    pub fn policy(&self) -> Option<&ToolPolicyStores> {
        self.policy.as_ref()
    }

    /// The layers in `scope`: its agent rules, then the stores' tenant and
    /// user rules and whether the session holds a grant.
    ///
    /// # Errors
    /// A store error.
    pub async fn layers(
        &self,
        definition: &ToolDefinition,
        scope: &ToolScope,
    ) -> RiverbaseResult<(ToolPolicyLayers, bool)> {
        let mut layers = ToolPolicyLayers {
            agent: scope.agent_rules.clone(),
            ..ToolPolicyLayers::default()
        };
        let Some(policy) = &self.policy else {
            return Ok((layers, false));
        };
        let rows = policy
            .rules
            .list_rules(scope.org_id.as_deref(), scope.user_id.as_deref())
            .await?;
        for row in rows {
            match row.scope {
                RuleScope::Tenant => layers.tenant.push(row.rule),
                RuleScope::User => layers.user.push(row.rule),
            }
        }
        let granted = match scope.session_id {
            Some(session) => {
                policy
                    .grants
                    .is_granted(
                        session,
                        &definition.name,
                        scope.org_id.as_deref(),
                        chrono::Utc::now(),
                    )
                    .await?
            }
            None => false,
        };
        Ok((layers, granted))
    }

    /// What a call to `definition` in `scope` does (Python rc8
    /// `ToolGate.decide`). `BEFORE_TOOL` is not a ceiling here: it stays a
    /// hard block that runs on every call.
    ///
    /// # Errors
    /// A rule or grant that cannot be read.
    pub async fn decide(
        &self,
        definition: &ToolDefinition,
        scope: &ToolScope,
    ) -> RiverbaseResult<ToolDecision> {
        let (layers, granted) = self.layers(definition, scope).await?;
        Ok(resolve_tool_mode(
            definition,
            &layers,
            &SecurityCeiling::default(),
            granted,
        ))
    }

    /// Store an approval's `choice` (Python rc8 `ToolGate.remember`):
    /// `session` grants the tool in the conversation for 24 hours, `always`
    /// writes the user's exact-name `auto` rule (one per tool). A choice the
    /// decision does not offer (a locked one offers `once` only) is ignored.
    ///
    /// # Errors
    /// The stores'.
    pub async fn remember(
        &self,
        definition: &ToolDefinition,
        scope: &ToolScope,
        decision: &ToolDecision,
        choice: RememberOption,
    ) -> RiverbaseResult<()> {
        let Some(policy) = &self.policy else {
            return Ok(());
        };
        if !decision.remember_options().contains(&choice) {
            return Ok(());
        }
        match choice {
            RememberOption::Once => Ok(()),
            RememberOption::Session => {
                let Some(session) = scope.session_id else {
                    return Ok(());
                };
                policy
                    .grants
                    .grant(&ToolGrant {
                        session_id: session,
                        tool_name: definition.name.clone(),
                        user_id: scope.user_id.clone(),
                        org_id: scope.org_id.clone(),
                        expires_at: chrono::Utc::now() + chrono::TimeDelta::hours(GRANT_TTL_HOURS),
                    })
                    .await
            }
            RememberOption::Always => {
                let Some(user) = scope.user_id.as_deref() else {
                    return Ok(());
                };
                let org = scope.org_id.as_deref();
                let matcher = ToolMatch::exact(definition.name.clone());
                for row in policy.rules.list_rules(org, Some(user)).await? {
                    if row.scope == RuleScope::User && row.rule.matcher == matcher {
                        if row.rule.mode == ToolMode::Auto {
                            return Ok(());
                        }
                        policy
                            .rules
                            .remove_rule(row.rule_id, org, Some(user))
                            .await?;
                    }
                }
                policy
                    .rules
                    .put_rule(&ToolRuleRecord {
                        rule_id: uuid::Uuid::new_v4(),
                        scope: RuleScope::User,
                        org_id: scope.org_id.clone(),
                        user_id: Some(user.to_owned()),
                        rule: ToolRule {
                            matcher,
                            mode: ToolMode::Auto,
                            locked: false,
                        },
                    })
                    .await
            }
        }
    }

    async fn check_stage(
        &self,
        stage: SecurityStage,
        definition: &ToolDefinition,
        args: &Value,
        call: &ToolCallContext,
    ) -> RiverbaseResult<()> {
        let Some(security) = &self.security else {
            return Ok(());
        };
        let auth = call.auth.as_ref();
        let input = call.input.as_deref();
        let mut payload = Map::new();
        payload.insert("tool_name".into(), Value::String(definition.name.clone()));
        payload.insert("args".into(), args.clone());
        // The run, session and interaction, so the decision (which holds the
        // tool's arguments) is kept and deleted with its session.
        if let Some(input) = input {
            payload.insert("run_id".into(), Value::String(input.run_id.to_string()));
        }
        let request = SecurityRequest::new(stage)
            .with_payload(payload)
            .with_context(SecurityContext {
                agent_id: None,
                tenant_id: auth.and_then(|a| a.org_id.clone()),
                user_id: auth.and_then(|a| a.user_id.clone()),
                interaction_id: input.and_then(|i| i.interaction_id.map(|id| id.to_string())),
                session_id: input.map(|i| i.session_id.to_string()),
            });
        let decision = security.check(&request).await;
        // Python's handlers raise `SecurityBlocked` when enforcing.
        if decision.denies() && security.enforce() {
            let message = decision
                .user_message
                .clone()
                .unwrap_or_else(|| format!("tool {} blocked by security policy", definition.name));
            return Err(crate::BEM_061.with_data(message));
        }
        Ok(())
    }

    /// Run one tool call (see the module docs for the order).
    ///
    /// # Errors
    /// `BEM-061` when a security stage blocks the call; idempotency store errors.
    pub async fn execute(
        &self,
        definition: &ToolDefinition,
        args: Value,
        ctx: &ExecuteContext,
    ) -> RiverbaseResult<ToolRuntimeResult> {
        // Python `ToolRuntime.execute` opens `beam.tool`.
        let mut attributes = Map::new();
        attributes.insert("tool".into(), Value::String(definition.name.clone()));
        attributes.insert(
            "execution_id".into(),
            Value::String(ctx.call.execution_id.to_string()),
        );
        attributes.insert("args".into(), args.clone());
        beam_telemetry::in_span(
            beam_telemetry::SPAN_TOOL,
            None,
            attributes,
            self.execute_in_span(definition, args, ctx),
        )
        .await
    }

    #[allow(clippy::too_many_lines)] // The pipeline's steps, in order (module docs).
    async fn execute_in_span(
        &self,
        definition: &ToolDefinition,
        args: Value,
        ctx: &ExecuteContext,
    ) -> RiverbaseResult<ToolRuntimeResult> {
        let revision = ctx
            .tool_revision
            .clone()
            .unwrap_or_else(|| "default".into());
        let key: IdempotencyKey = (
            ctx.call.execution_id.to_string(),
            ctx.call.call_id.clone(),
            revision,
        );
        if let Some(allowed) = &ctx.allowed {
            if !allowed.contains(&definition.name) {
                return Ok(ToolRuntimeResult::error(format!(
                    "undeclared tool: {}",
                    definition.name
                )));
            }
        }
        let dry = ctx.dry_run || self.dry_run;
        if dry && definition.side_effect && !self.dry_run_allowlist.contains(&definition.name) {
            return Ok(ToolRuntimeResult {
                output: json!({"skipped": true, "reason": "dry_run"}),
                ..ToolRuntimeResult::default()
            });
        }
        if definition.is_client() {
            return Ok(ToolRuntimeResult::error(
                "client tool needs a resumable runtime",
            ));
        }
        // The tool policy, resolved here too, so every runtime's calls get
        // the tenant, user, grant and security layers and none can skip
        // their `ask` (plan 023 C3). The scope has no agent layer: an agent's
        // `approval_required` tools are held by the runtime alone. Only the
        // runtime's hold can approve: an `approved` argument from the model
        // does not (Python rc1 accepted one; rc8 does not).
        let scope = ctx
            .call
            .input
            .as_deref()
            .map(|input| ToolScope::of(input, Vec::new()))
            .unwrap_or_default();
        let decision = match self.decide(definition, &scope).await {
            Ok(decision) => decision,
            Err(e) => {
                tracing::warn!(tool = %definition.name, error = %e, "tool policy unavailable");
                audit_decision(definition, None, "unavailable");
                return Ok(ToolRuntimeResult::error("tool policy unavailable"));
            }
        };
        match decision.mode {
            ToolMode::Deny => {
                audit_decision(definition, Some(&decision), "denied");
                return Ok(ToolRuntimeResult::error(format!(
                    "denied: {}",
                    decision.layer.as_str()
                )));
            }
            ToolMode::Ask if !ctx.approved => {
                audit_decision(definition, Some(&decision), "approval_required");
                return Ok(ToolRuntimeResult::error("approval required"));
            }
            ToolMode::Ask => audit_decision(definition, Some(&decision), "approved"),
            ToolMode::Auto => {}
        }
        if let Some(cached) = self.store.get(&key).await? {
            return Ok(ToolRuntimeResult {
                cached: true,
                ..cached
            });
        }
        self.check_stage(SecurityStage::BeforeTool, definition, &args, &ctx.call)
            .await?;
        let empty = Map::new();
        let arg_map = args.as_object().unwrap_or(&empty);
        if let Some(error) = validate_input(&definition.input_schema, arg_map) {
            return Ok(ToolRuntimeResult::error(error));
        }
        let result = match &definition.callable {
            None => ToolRuntimeResult {
                output: args.clone(),
                ..ToolRuntimeResult::default()
            },
            Some(handler) => {
                let attempts = definition.max_retries.saturating_add(1);
                let timeout = Duration::try_from_secs_f64(definition.timeout_seconds.max(0.0))
                    .unwrap_or(Duration::MAX);
                let mut result = ToolRuntimeResult::error("tool failed");
                for _ in 0..attempts {
                    match tokio::time::timeout(timeout, handler.call(args.clone(), &ctx.call)).await
                    {
                        Ok(Ok(output)) => {
                            result = ToolRuntimeResult {
                                output: bound_tool_output(output, self.max_result_chars),
                                ..ToolRuntimeResult::default()
                            };
                            break;
                        }
                        // Python does not retry a timeout.
                        Err(_) => {
                            result = ToolRuntimeResult::error("tool timeout");
                            break;
                        }
                        Ok(Err(e)) => {
                            let text = error_text(&e);
                            result = ToolRuntimeResult::error(if text.is_empty() {
                                "tool failed".into()
                            } else {
                                text
                            });
                        }
                    }
                }
                result
            }
        };
        self.check_stage(SecurityStage::AfterTool, definition, &args, &ctx.call)
            .await?;
        if definition.side_effect && result.error.is_none() {
            // The side effect happened: a failed write is logged, not reported
            // as a tool failure (the model would retry and repeat it).
            if let Err(e) = self.store.put(&key, &result, DEFAULT_TTL).await {
                tracing::warn!(tool = %definition.name, error = %e, "tool result not remembered");
            }
        }
        Ok(result)
    }

    /// Finish a client tool call with the client's answer (Python rc8
    /// `_client_result`): the same allowlist, `BEFORE_TOOL` and input checks as
    /// [`Self::execute`], then the client's output is checked against the
    /// tool's `output_schema`, bounded, passed through `AFTER_TOOL` and
    /// remembered like a tool's own output.
    ///
    /// # Errors
    /// `BEM-061` when a security stage blocks the call; idempotency store errors.
    pub async fn client_result(
        &self,
        definition: &ToolDefinition,
        args: Value,
        answer: ClientAnswer,
        ctx: &ExecuteContext,
    ) -> RiverbaseResult<ToolRuntimeResult> {
        let revision = ctx
            .tool_revision
            .clone()
            .unwrap_or_else(|| "default".into());
        let key: IdempotencyKey = (
            ctx.call.execution_id.to_string(),
            ctx.call.call_id.clone(),
            revision,
        );
        if !definition.is_client() {
            return Ok(ToolRuntimeResult::error(format!(
                "{} is not a client tool",
                definition.name
            )));
        }
        if let Some(allowed) = &ctx.allowed {
            if !allowed.contains(&definition.name) {
                return Ok(ToolRuntimeResult::error(format!(
                    "undeclared tool: {}",
                    definition.name
                )));
            }
        }
        if let Some(cached) = self.store.get(&key).await? {
            return Ok(ToolRuntimeResult {
                cached: true,
                ..cached
            });
        }
        self.check_stage(SecurityStage::BeforeTool, definition, &args, &ctx.call)
            .await?;
        let empty = Map::new();
        if let Some(error) =
            validate_input(&definition.input_schema, args.as_object().unwrap_or(&empty))
        {
            return Ok(ToolRuntimeResult::error(error));
        }
        if let Some(error) = answer.error {
            return Ok(ToolRuntimeResult::error(error));
        }
        if let (Some(schema), Some(output)) = (&definition.output_schema, answer.output.as_object())
        {
            if let Some(problem) = validate_input(schema, output) {
                return Ok(ToolRuntimeResult::error(format!(
                    "invalid client output: {problem}"
                )));
            }
        }
        let result = ToolRuntimeResult {
            output: bound_tool_output(answer.output, self.max_result_chars),
            ..ToolRuntimeResult::default()
        };
        self.check_stage(SecurityStage::AfterTool, definition, &args, &ctx.call)
            .await?;
        if definition.side_effect {
            if let Err(e) = self.store.put(&key, &result, DEFAULT_TTL).await {
                tracing::warn!(tool = %definition.name, error = %e, "tool result not remembered");
            }
        }
        Ok(result)
    }
}
