//! An invoke's identity and access: bootstrap, session and execution ownership, attempt rows.

use beam_audit::{AuditAction, AuditEvent};
use beam_execution::concurrency::default_concurrency_key;
use beam_execution::{EnsureRequest, Ensured};
use beam_store::{agent_id_for, InteractionRow, InteractionStatus, RunRow, RunStatus, SessionRow};
use beam_types::AgentInput;
use beam_usage::UsagePlan;
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use riverbase_core::RiverbaseResult;

use super::{InvokeIdentity, RiverBeam};

impl RiverBeam {
    pub(super) async fn bootstrap_invocation(
        &self,
        mut input: AgentInput,
        identity: InvokeIdentity,
    ) -> RiverbaseResult<(AgentInput, Ensured, Option<UsagePlan>)> {
        let agent_name_owned = input.agent_name.clone().unwrap_or_else(|| "unknown".into());
        let agent_name = agent_name_owned.as_str();
        let agent_id = agent_id_for(agent_name);

        // Python `_assert_tenant_scope` runs first: a caller reusing an
        // execution id must own it (its org, and its session's user).
        if let Some(existing) = input.execution_id {
            self.assert_execution_access(&input, existing).await?;
        }

        self.audit
            .log(
                AuditEvent::new(AuditAction::RequestReceived)
                    .agent_name(input.agent_name.clone())
                    .user_id(input.user_id())
                    .session_id(Some(input.session_id))
                    .run_id(Some(input.run_id))
                    .execution_id(input.execution_id)
                    .trace_id(beam_telemetry::current_span().map(|s| s.trace_id)),
            )
            .await;

        // Pre-dispatch quota enforcement.
        let usage_plan = self.resolve_usage_plan(&input).await?;

        // The store's run rows reference execution rows, so both must be on one database.
        if self.store.durable() && !self.executions.records().durable() {
            return Err(crate::BEM_380.with_data(
                "a durable store needs durable execution records; use RiverBeamBuilder::pg_store",
            ));
        }

        let org_id = input.auth_context.as_ref().and_then(|a| a.org_id.clone());
        self.store
            .ensure_session(&SessionRow {
                id: input.session_id,
                agent_id,
                user_id: input.user_id(),
                profile_id: input
                    .auth_context
                    .as_ref()
                    .and_then(|a| a.profile_id.clone()),
                org_id: org_id.clone(),
                name: None,
                last_active_at: None,
            })
            .await?;
        self.assert_session_owner(&input).await?;
        // Durable execution: start-or-get by
        // idempotency key, else create; replay when already terminal.
        let resume = input.actions.as_ref().is_some_and(|a| !a.is_empty());
        // A runtime-bound agent's new execution gets its runtime ref at creation.
        let runtime = self.runtimes.find(agent_name);
        let execution_id = match (&runtime, input.execution_id) {
            (Some(_), None) => Some(Uuid::new_v4()),
            (_, id) => id,
        };
        let ensured = self
            .executions
            .ensure(&EnsureRequest {
                execution_id,
                session_id: input.session_id,
                agent_id,
                agent_name: agent_name.to_owned(),
                org_id: org_id.as_deref().and_then(|o| Uuid::parse_str(o).ok()),
                idempotency: identity.idempotency,
                concurrency_key: default_concurrency_key(input.session_id, agent_name),
                request_id: input.request_id,
                request_snapshot: identity.request_snapshot,
                runtime_name: runtime.as_ref().map(|r| r.runtime_name().to_owned()),
                runtime_ref: runtime
                    .as_ref()
                    .zip(execution_id)
                    .map(|(r, id)| r.allocate_ref(id).to_value()),
                resume,
            })
            .await?;
        input.execution_id = Some(ensured.execution.id);
        let _ = identity.assigned.set(ensured.execution.id);
        Ok((input, ensured, usage_plan))
    }

    /// The caller must own the session it writes to: another org's session is
    /// `BEM-411` (`assert_tenant_access`), another user's `BEM-412` (Python
    /// `SessionOwnershipError`, raised by the domain's `record_session`, which
    /// Rust checks here so a direct invoke is covered too).
    pub(super) async fn assert_session_owner(&self, input: &AgentInput) -> RiverbaseResult<()> {
        let Some(session) = self.store.get_session(input.session_id).await? else {
            return Ok(());
        };
        let auth = input.auth_context.as_ref();
        let caller = beam_security::ownership::Owner {
            org_id: auth.and_then(|a| a.org_id.as_deref()),
            user_id: None,
        };
        let owner = beam_security::ownership::Owner {
            org_id: session.org_id.as_deref(),
            user_id: None,
        };
        beam_security::ownership::assert_tenant_access("session", owner, caller)
            .map_err(|m| crate::BEM_069.with_data(m))?;
        // Compare the caller's user as the store keeps it: a durable store's
        // `user_id` column is a UUID, so any other id was stored as null.
        let mut caller_user =
            beam_execution::identity::normalize_identity(input.user_id().as_deref());
        if self.store.durable() {
            caller_user = caller_user
                .and_then(|u| Uuid::parse_str(&u).ok())
                .map(|u| u.to_string());
        }
        let session_user = beam_execution::identity::normalize_identity(session.user_id.as_deref());
        let same_user = session_user == caller_user;
        if !same_user {
            return Err(crate::BEM_071
                .with_data(serde_json::json!({ "session_id": input.session_id.to_string() })));
        }
        Ok(())
    }

    /// Python `_assert_tenant_scope` (`TenantAccessError`, public `BEM-411`).
    pub(super) async fn assert_execution_access(
        &self,
        input: &AgentInput,
        execution_id: Uuid,
    ) -> RiverbaseResult<()> {
        let Some(row) = self.executions.records().get(execution_id).await? else {
            return Ok(());
        };
        let auth = input.auth_context.as_ref();
        let caller = beam_security::ownership::Owner {
            org_id: auth.and_then(|a| a.org_id.as_deref()),
            user_id: auth.and_then(|a| a.user_id.as_deref()),
        };
        let org = row.org_id.map(|o| o.to_string());
        let owner = beam_security::ownership::Owner {
            org_id: org.as_deref(),
            user_id: None,
        };
        beam_security::ownership::assert_tenant_access("execution", owner, caller)
            .map_err(|m| crate::BEM_069.with_data(m))?;
        let session_user = self
            .store
            .get_session(row.session_id)
            .await?
            .and_then(|s| s.user_id);
        if let (Some(session_user), Some(user)) =
            (session_user, caller.user_id.filter(|u| !u.is_empty()))
        {
            if session_user != user {
                return Err(crate::BEM_069.with_data("execution belongs to another user"));
            }
        }
        Ok(())
    }

    /// The interaction and run rows of a new attempt. Runs after `ensure`, so a
    /// failure here goes through `handle_failure` and frees the execution.
    pub(super) async fn create_attempt_rows(
        &self,
        input: &AgentInput,
        attempt_no: i32,
        runtime_ref: Option<&Value>,
    ) -> RiverbaseResult<()> {
        let agent_name = input.agent_name.clone().unwrap_or_else(|| "unknown".into());
        let agent_id = agent_id_for(&agent_name);
        let org_id = input.auth_context.as_ref().and_then(|a| a.org_id.clone());
        if let Some(interaction_id) = input.interaction_id {
            self.store
                .create_interaction(&InteractionRow {
                    id: interaction_id,
                    session_id: input.session_id,
                    execution_id: input.execution_id,
                    agent_id,
                    agent_name: agent_name.clone(),
                    status: InteractionStatus::InProgress,
                    started_at: Utc::now(),
                    ended_at: None,
                })
                .await?;
        }
        self.store
            .create_run(&RunRow {
                id: input.run_id,
                session_id: input.session_id,
                interaction_id: input.interaction_id.unwrap_or(input.run_id),
                execution_id: input.execution_id,
                attempt_no,
                agent_id,
                agent_name,
                status: RunStatus::Running,
                started_at: Utc::now(),
                ended_at: None,
                org_id,
            })
            .await?;
        if let Some(runtime_ref) = runtime_ref {
            self.store
                .set_run_runtime_ref(input.run_id, runtime_ref)
                .await?;
        }
        Ok(())
    }
}
