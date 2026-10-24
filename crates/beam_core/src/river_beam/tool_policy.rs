//! The tool policy as the inspector's Tools tab explains it (rc8
//! `orchestration/tool_policy.py`, plan 023 A3).

use beam_types::tool_policy::ToolScope;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use super::RiverBeam;

impl RiverBeam {
    /// How a call to `tool` by `agent` resolves for the caller (`user_id`
    /// in `org_id`) in `session`: `{mode, layer, locked, reason, rule,
    /// remember_options}`. The caller's ownership of the session is the
    /// report's to check.
    ///
    /// # Errors
    /// `BEM-021` for a tool this beam does not know; a rule or grant store
    /// error.
    pub async fn explain_tool_policy(
        &self,
        user_id: Option<&str>,
        org_id: Option<&str>,
        tool: &str,
        agent: &str,
        session: Option<Uuid>,
    ) -> RiverbaseResult<Value> {
        let service = self
            .tool_source
            .catalog()
            .ok_or_else(|| crate::BEM_021.with_data(format!("unknown tool {tool}")))?;
        let definition = service
            .registry()
            .get(tool)
            .ok_or_else(|| crate::BEM_021.with_data(format!("unknown tool {tool}")))?;
        let scope = ToolScope {
            session_id: session,
            user_id: user_id.map(str::to_owned),
            org_id: org_id.map(str::to_owned),
            agent_rules: self
                .runtimes
                .find(agent)
                .map(|r| r.tool_rules())
                .unwrap_or_default(),
        };
        let decision = service.runtime().decide(&definition, &scope).await?;
        Ok(json!({
            "mode": decision.mode,
            "layer": decision.layer,
            "locked": decision.locked,
            "reason": decision.reason,
            "rule": decision.rule,
            "remember_options": decision.remember_options(),
        }))
    }
}
