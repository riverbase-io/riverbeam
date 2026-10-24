//! Tool rules and grants (rc8 `domain/commands/tool_rules.py`, plan 023 C5):
//! `set-my-tool-rule`, `remove-my-tool-rule` and `revoke-tool-grants` for a
//! `beam-user` on their own rules and sessions; `set-tenant-tool-rule` and
//! `remove-tenant-tool-rule` for `beam-admin` (D7). The org and user always
//! come from the caller, never from the payload. SQL on the command's
//! transaction, on the rows the tool runtime reads (`beam_store::tool_policy`).

use beam_types::tool_policy::{RuleScope, ToolRule};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use super::BeamAggregate;
use crate::commands::{
    RemoveMyToolRulePayload, RemoveTenantToolRulePayload, RevokeToolGrantsPayload,
    SetMyToolRulePayload, SetTenantToolRulePayload,
};
use beam_store::repo::domain::{DomainSql, ToolRuleInsert};

use crate::local::{sql, BeamSql};

impl BeamAggregate {
    /// Insert `rule` with `scope` for the caller's org (and `user`).
    async fn put_tool_rule(
        &mut self,
        scope: RuleScope,
        user: Option<String>,
        rule: ToolRule,
    ) -> RiverbaseResult<Value> {
        rule.matcher
            .check()
            .map_err(|reason| beam_core::BEM_003.with_data(reason))?;
        let org = self.caller().org_id;
        let matcher = serde_json::to_value(&rule.matcher)
            .map_err(|e| beam_core::BEM_001.with_data(e.to_string()))?;
        self.db()
            .insert_tool_rule(ToolRuleInsert {
                rule_id: Uuid::new_v4(),
                scope: scope.as_str().to_owned(),
                org_id: org,
                user_id: user,
                matcher,
                mode: rule.mode.as_str().to_owned(),
                locked: rule.locked,
            })
            .await
            .map_err(sql)?
            .ok_or_else(|| beam_core::BEM_497.with_data("the tool rule was not stored"))
    }

    /// Delete rule `rule_id` of the caller's org with `scope` (and `user`).
    async fn remove_tool_rule(
        &mut self,
        rule_id: Uuid,
        scope: RuleScope,
        user: Option<String>,
    ) -> RiverbaseResult<Value> {
        let org = self.caller().org_id;
        let removed = self
            .db()
            .delete_tool_rule(rule_id, scope.as_str(), org, user)
            .await
            .map_err(sql)?;
        Ok(json!({ "rule_id": rule_id, "removed": removed }))
    }

    /// The caller's user id.
    ///
    /// # Errors
    /// `WEB-001` without one.
    fn caller_user(&self) -> RiverbaseResult<String> {
        self.caller()
            .user_id
            .filter(|u| !u.trim().is_empty())
            .ok_or_else(|| beam_core::BEM_003.with_data("the caller has no user id"))
    }

    /// `set-my-tool-rule`: a rule for the caller only. An `auto` rule needs an
    /// exact tool name, and `locked` is for tenant rules (D6).
    ///
    /// # Errors
    /// `WEB-001` for a refused rule or no user identity; store errors.
    pub async fn set_my_tool_rule(&mut self, p: SetMyToolRulePayload) -> RiverbaseResult<Value> {
        let user = self.caller_user()?;
        let rule = ToolRule {
            matcher: p.matcher,
            mode: p.mode,
            locked: p.locked,
        };
        rule.check_user_rule()
            .map_err(|reason| beam_core::BEM_003.with_data(reason))?;
        self.put_tool_rule(RuleScope::User, Some(user), rule).await
    }

    /// `remove-my-tool-rule`: remove one of the caller's own rules. Another
    /// user's rule is not removed (`removed: false`).
    ///
    /// # Errors
    /// `WEB-001` without a user identity; store errors.
    pub async fn remove_my_tool_rule(
        &mut self,
        p: RemoveMyToolRulePayload,
    ) -> RiverbaseResult<Value> {
        let user = self.caller_user()?;
        self.remove_tool_rule(p.rule_id, RuleScope::User, Some(user))
            .await
    }

    /// `revoke-tool-grants`: end "allow for this conversation" in one of the
    /// caller's sessions (one tool's grant, or all).
    ///
    /// # Errors
    /// `BEM-412` for another user's session; `BEM-404` for an unknown one;
    /// store errors.
    pub async fn revoke_tool_grants(
        &mut self,
        p: RevokeToolGrantsPayload,
    ) -> RiverbaseResult<Value> {
        let caller = self.caller();
        self.db()
            .owned_session(&p.session_id.to_string(), &caller)
            .await?;
        let revoked = self
            .db()
            .revoke_tool_grants(p.session_id, caller.org_id, p.tool_name)
            .await
            .map_err(sql)?;
        Ok(json!({ "session_id": p.session_id, "revoked": revoked }))
    }

    /// `set-tenant-tool-rule`: a rule for every user of the caller's org;
    /// `locked` stops lower layers loosening it.
    ///
    /// # Errors
    /// `WEB-001` for a match no tool can meet; store errors.
    pub async fn set_tenant_tool_rule(
        &mut self,
        p: SetTenantToolRulePayload,
    ) -> RiverbaseResult<Value> {
        let rule = ToolRule {
            matcher: p.matcher,
            mode: p.mode,
            locked: p.locked,
        };
        self.put_tool_rule(RuleScope::Tenant, None, rule).await
    }

    /// `remove-tenant-tool-rule`: remove a tenant rule of the caller's org.
    ///
    /// # Errors
    /// Store errors.
    pub async fn remove_tenant_tool_rule(
        &mut self,
        p: RemoveTenantToolRulePayload,
    ) -> RiverbaseResult<Value> {
        self.remove_tool_rule(p.rule_id, RuleScope::Tenant, None)
            .await
    }
}
