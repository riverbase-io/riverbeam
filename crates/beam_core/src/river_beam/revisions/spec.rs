//! Agent spec checks (Python `agents/compiler.py`, `agents/enforcement.py`):
//! free functions, no beam state.

use beam_context::policy::{ContextSpec, FailMode};
use beam_runtime::AgentRuntime;
use beam_types::spec::{
    RuntimePolicy, ToolPolicy, SUPPORTED_ISOLATION, SUPPORTED_KNOWLEDGE_MODES,
    SUPPORTED_MEMORY_BACKENDS,
};
use beam_types::{AgentInput, AgentMetadata, AgentSpec, InvokeRequest, ModelConfig, Sensitivity};
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::Value;
use std::collections::BTreeSet;
use uuid::Uuid;

/// The runtime name Python pins an agent bound without a spec under.
pub(super) const UNBOUND_RUNTIME: &str = "langgraph";

pub(super) fn invalid(message: impl Into<String>) -> RiverbaseError {
    crate::BEM_494.with_data(message.into())
}

/// Python `_validate_capability_combo` and `_validate_spec_truthfulness`.
///
/// # Errors
/// `BEM-494` (public `BEM-400`) with Python's message.
pub fn validate_spec(spec: &AgentSpec) -> RiverbaseResult<()> {
    if spec.runtime.require_resume && !spec.runtime.require_checkpointing {
        return Err(invalid("resume requires checkpointing"));
    }
    let known = beam_context::types::registry_order();
    if let Some(unknown) = spec
        .context
        .providers
        .iter()
        .find(|p| !known.contains(&p.as_str()))
    {
        return Err(invalid(format!("unknown context provider: {unknown}")));
    }
    if !SUPPORTED_KNOWLEDGE_MODES.contains(&spec.knowledge.mode.as_str()) {
        return Err(invalid(format!(
            "unsupported knowledge mode: {}",
            spec.knowledge.mode
        )));
    }
    if !SUPPORTED_MEMORY_BACKENDS.contains(&spec.memory.backend.as_str()) {
        return Err(invalid(format!(
            "unsupported memory backend: {}",
            spec.memory.backend
        )));
    }
    if spec.knowledge.mode == "tool"
        && !spec.tools.allowlist.iter().any(|t| t == "knowledge_search")
    {
        return Err(invalid("knowledge mode tool requires knowledge_search"));
    }
    if spec.knowledge.mode == "off" && spec.context.providers.iter().any(|p| p == "knowledge") {
        return Err(invalid("knowledge provider contradicts knowledge mode off"));
    }
    if !spec.security.enforce {
        return Err(invalid("fail-open security is not supported"));
    }
    if !SUPPORTED_ISOLATION.contains(&spec.security.isolation.as_str()) {
        return Err(invalid(format!(
            "unsupported isolation: {}",
            spec.security.isolation
        )));
    }
    if spec.execution.timeout_s.is_some_and(|t| t <= 0) {
        return Err(invalid("timeout_s must be positive"));
    }
    if spec.execution.max_steps.is_some_and(|s| s <= 0) {
        return Err(invalid("max_steps must be positive"));
    }
    Ok(())
}

/// The bound runtime is the spec's and has the
/// capabilities it requires.
///
/// # Errors
/// `BEM-494`.
pub(super) fn validate_runtime(
    policy: &RuntimePolicy,
    runtime: &dyn AgentRuntime,
) -> RiverbaseResult<()> {
    let name = runtime.runtime_name();
    if !name.is_empty() && name != policy.name {
        return Err(invalid(format!(
            "runtime name mismatch: {name} != {}",
            policy.name
        )));
    }
    let caps = runtime.capabilities();
    let required = [
        (policy.require_streaming, "streaming"),
        (policy.require_resume, "resume"),
        (policy.require_cancellation, "cancellation"),
        (policy.require_checkpointing, "checkpointing"),
        (policy.require_structured_output, "structured_output"),
        (policy.require_native_tools, "native_tools"),
    ];
    if let Some((_, missing)) = required
        .iter()
        .find(|(wanted, cap)| *wanted && !caps.has(cap))
    {
        return Err(invalid(format!("runtime does not support {missing}")));
    }
    Ok(())
}

/// A new revision keeps every
/// required output field and requires no new input field.
///
/// # Errors
/// `BEM-494`.
pub fn assert_public_schemas_compatible(old: &AgentSpec, new: &AgentSpec) -> RiverbaseResult<()> {
    let required = |schema: Option<&serde_json::Map<String, Value>>| -> BTreeSet<String> {
        schema
            .and_then(|s| s.get("required"))
            .and_then(Value::as_array)
            .map(|r| {
                r.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    };
    let (old_out, new_out) = (
        required(old.schemas.output_schema.as_ref()),
        required(new.schemas.output_schema.as_ref()),
    );
    if !old_out.is_empty() && !old_out.is_subset(&new_out) {
        return Err(invalid("incompatible output schema"));
    }
    let (old_in, new_in) = (
        required(old.schemas.input_schema.as_ref()),
        required(new.schemas.input_schema.as_ref()),
    );
    if new_in.difference(&old_in).next().is_some() {
        return Err(invalid("incompatible input schema"));
    }
    Ok(())
}

/// A spec for an agent bound without one.
#[must_use]
pub fn spec_from_registration(metadata: &AgentMetadata, runtime_name: &str) -> AgentSpec {
    AgentSpec {
        version: if metadata.version.is_empty() {
            "1.0.0".into()
        } else {
            metadata.version.clone()
        },
        description: metadata.description.clone(),
        tags: metadata.tags.clone(),
        tools: ToolPolicy {
            allowlist: metadata.tools.clone(),
            ..ToolPolicy::default()
        },
        ..AgentSpec::new(metadata.name.clone(), runtime_name)
    }
}

/// The context policy parts of a spec, as a [`ContextSpec`].
pub(super) fn context_spec(spec: &AgentSpec) -> ContextSpec {
    let sensitivity =
        serde_json::from_value::<Sensitivity>(Value::String(spec.context.max_sensitivity.clone()))
            .ok();
    ContextSpec {
        instructions: Some(spec.prompt.instructions.clone()).filter(|i| !i.is_empty()),
        prompt_revision: spec.prompt.revision.clone(),
        providers: spec.context.providers.clone(),
        token_budget: spec.context.token_budget,
        max_sensitivity: sensitivity,
        source_failure: spec
            .context
            .source_failure
            .iter()
            .filter_map(|(k, v)| {
                serde_json::from_value::<FailMode>(Value::String(v.clone()))
                    .ok()
                    .map(|m| (k.clone(), m))
            })
            .collect(),
        promote_ltm_trust: spec.context.promote_ltm_trust,
        resource_schemes: spec.context.resource_schemes.clone(),
        resource_hosts: spec.context.resource_hosts.clone(),
        max_resource_bytes: Some(spec.context.max_resource_bytes),
        knowledge_mode: Some(spec.knowledge.mode.clone()),
        collections: spec.knowledge.collections.clone(),
        knowledge_revision: spec.knowledge.revision.clone(),
        memory_backend: Some(spec.memory.backend.clone()),
        memory_revision: spec.memory.revision.clone(),
        tool_allowlist: spec.tools.allowlist.clone(),
        tool_revision: spec.tools.revision.clone(),
        security_revision: spec.security.revision.clone(),
    }
}

/// The step limit: the spec's, capped by `default`.
#[must_use]
pub fn narrowed_max_steps(spec: Option<&AgentSpec>, default: u64) -> u64 {
    match spec.and_then(|s| s.execution.max_steps).filter(|s| *s > 0) {
        Some(steps) => u64::try_from(steps).unwrap_or(default).min(default),
        None => default,
    }
}

/// An `AgentInput` as an invoke request (an eval case).
pub(super) fn request_from(input: AgentInput, agent_name: &str) -> InvokeRequest {
    InvokeRequest {
        agent_name: input.agent_name.unwrap_or_else(|| agent_name.to_owned()),
        content: input.content,
        session_id: input.session_id,
        model_override: input.model_override,
        metadata: input.metadata,
        interaction_id: input.interaction_id,
        request_id: input.request_id.or_else(|| Some(Uuid::new_v4())),
        stream: input.stream,
        actions: input.actions,
        auth_context: input.auth_context,
        context: input.context,
        execution_id: input.execution_id,
        idempotency_key: None,
        client: None,
    }
}

pub(super) fn provider_label(model: &ModelConfig) -> String {
    serde_json::to_value(model.provider)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Python's model checks: the route resolves to the spec's model name and
/// provider.
pub(super) fn check_model(
    spec: &AgentSpec,
    pin: &str,
    selected: &ModelConfig,
) -> RiverbaseResult<()> {
    if let Some(name) = &spec.model.name {
        if &selected.name != name {
            return Err(invalid(format!("model route mismatch: {pin} != {name}")));
        }
    }
    if let Some(provider) = &spec.model.provider {
        let actual = provider_label(selected);
        if &actual != provider {
            return Err(invalid(format!(
                "model provider mismatch: {actual} != {provider}"
            )));
        }
    }
    Ok(())
}
