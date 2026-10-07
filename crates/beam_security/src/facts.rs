//! Builds the `Input.*` fact JSON for each stage.
//!
//! All regex scoring and policy-set membership is resolved here in Rust; the GRL
//! documents ([`crate::grl`]) only see precomputed numbers and booleans.

use serde_json::{json, Value};

use crate::enums::SecurityStage;
use crate::patterns::PatternLoader;
use crate::policy::SecurityPolicy;
use crate::request::SecurityRequest;

const MEMORY_SENSITIVE_THRESHOLD: f64 = 0.40;

/// Construct the fact object for `stage` from the request payload + policy.
#[must_use]
pub fn build_facts(stage: SecurityStage, req: &SecurityRequest, policy: &SecurityPolicy) -> Value {
    let p = &req.payload;
    let content = req.str_field("content");

    match stage {
        SecurityStage::BeforeInput | SecurityStage::BeforeContextInjection => json!({
            "injection_score": PatternLoader::score_injection(&content),
            "role_override_score": PatternLoader::score_role_override(&content),
        }),
        SecurityStage::AfterLlm | SecurityStage::BeforeOutput => json!({
            "secrets_score": PatternLoader::score_secrets(&content),
            "pii_score": PatternLoader::score_pii(&content),
        }),
        SecurityStage::BeforeTool => {
            let tool = req.str_field("tool_name");
            let hitl_approved = p.get("hitl_approved").and_then(Value::as_bool).unwrap_or(false);
            let denied_global = policy.global_denied_tools.contains(&tool);
            let denied_allowlist =
                !policy.global_allowed_tools.is_empty() && !policy.global_allowed_tools.contains(&tool);
            let tool_hitl = policy
                .tool_policies
                .get(&tool)
                .is_some_and(|tp| tp.hitl_required);
            let hitl_required = !hitl_approved && (tool_hitl || policy.hitl_config.enabled);
            json!({
                "tool_denied_global": denied_global,
                "tool_denied_allowlist": denied_allowlist,
                "tool_hitl_required": hitl_required,
            })
        }
        SecurityStage::BeforeMemoryWrite => {
            let operation = p.get("operation").and_then(Value::as_str).unwrap_or("save");
            let confidence = p.get("confidence").and_then(Value::as_f64).unwrap_or(0.0);
            let sensitive = operation == "save"
                && !content.is_empty()
                && PatternLoader::score_memory_sensitive(&content) >= MEMORY_SENSITIVE_THRESHOLD;
            json!({ "memory_sensitive": sensitive, "confidence": confidence })
        }
        SecurityStage::Runtime => {
            let step_count = p.get("step_count").and_then(Value::as_u64).unwrap_or(0);
            let token_count = p.get("token_count").and_then(Value::as_u64).unwrap_or(0);
            let recursion_depth = p.get("recursion_depth").and_then(Value::as_u64).unwrap_or(0);
            let loop_detected = p.get("loop_detected").and_then(Value::as_bool).unwrap_or(false);
            let elapsed_seconds = p.get("elapsed_seconds").and_then(Value::as_f64).unwrap_or(0.0);
            let step_approaching =
                step_count <= policy.max_steps && step_count * 100 > policy.max_steps * 80;
            json!({
                "step_count": step_count,
                "max_steps": policy.max_steps,
                "step_approaching": step_approaching,
                "token_count": token_count,
                "max_tokens": policy.max_tokens,
                "recursion_depth": recursion_depth,
                "max_recursion": policy.max_recursion,
                "loop_detected": loop_detected,
                "elapsed_seconds": elapsed_seconds,
                "max_timeout": policy.max_timeout,
            })
        }
        SecurityStage::BeforeLlm
        | SecurityStage::AfterTool
        | SecurityStage::BeforeHitlResume => json!({}),
    }
}
