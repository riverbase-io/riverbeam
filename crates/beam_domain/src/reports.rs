//! rc8's reports (`domain/reports.py`, plan 023 A3):
//! `tool-policy-explanation`, `usage-summary` and `session-export`, served at
//! `/{namespace}/{name}.rept`. Their inputs come in the request's
//! `user_query` (`{tool_name, agent_name, session_id}`, as agent-app posts
//! them). They answer from the running beam through [`BeamReports`], which
//! the app sets once at startup ([`set_reports`]); unset, a report answers
//! `BEM-380`. Each checks that a session it names is the caller's.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use beam_core::RiverBeam;
use riverbase_core::{EngineContext, RiverbaseResult};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::local::{BeamSql, Caller, DomainStore, UsageSummarySettings};

/// The report names, for the policy rows.
pub const REPORTS: [&str; 3] = ["tool-policy-explanation", "usage-summary", "session-export"];

/// What the reports ask the running beam.
#[async_trait]
pub trait BeamReports: Send + Sync {
    /// How a call to `tool` by `agent` resolves for `caller` in `session`.
    async fn explain_tool_policy(
        &self,
        caller: &Caller,
        tool: &str,
        agent: &str,
        session: Option<Uuid>,
    ) -> RiverbaseResult<Value>;

    /// The caller's usage in `period` (today when `None`).
    async fn usage_summary(&self, caller: &Caller, period: Option<&str>) -> RiverbaseResult<Value>;

    /// The caller's session, exported.
    async fn export_session(&self, caller: &Caller, session: Uuid) -> RiverbaseResult<Value>;
}

static REPORTS_HANDLE: OnceLock<Arc<dyn BeamReports>> = OnceLock::new();

/// Set the process's report handle; `false` when one was set already.
pub fn set_reports(reports: Arc<dyn BeamReports>) -> bool {
    REPORTS_HANDLE.set(reports).is_ok()
}

fn reports() -> RiverbaseResult<&'static Arc<dyn BeamReports>> {
    REPORTS_HANDLE
        .get()
        .ok_or_else(|| beam_core::BEM_380.with_data("the reports need a running beam"))
}

/// [`BeamReports`] over a beam (and its database, for usage).
pub struct RiverBeamReports {
    beam: Arc<RiverBeam>,
    store: Option<DomainStore>,
    usage: UsageSummarySettings,
}

impl RiverBeamReports {
    #[must_use]
    pub fn new(
        beam: Arc<RiverBeam>,
        store: Option<DomainStore>,
        usage: UsageSummarySettings,
    ) -> Self {
        Self { beam, store, usage }
    }

    /// Fail unless `session` exists and is `caller`'s (admins: any).
    async fn owned(&self, caller: &Caller, session: Uuid) -> RiverbaseResult<()> {
        let row = self
            .beam
            .store()
            .get_session(session)
            .await?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"session_id": session})))?;
        if caller.admin {
            return Ok(());
        }
        let normalize = |v: Option<&str>| v.map(|s| s.trim().to_ascii_lowercase());
        if row.org_id.is_some()
            && normalize(row.org_id.as_deref()) != normalize(caller.org_id.as_deref())
        {
            return Err(beam_core::BEM_069.with_data("session belongs to another tenant"));
        }
        if row.user_id.is_some()
            && normalize(row.user_id.as_deref()) != normalize(caller.user_id.as_deref())
        {
            return Err(beam_core::BEM_071.with_data(json!({"entity": "session"})));
        }
        Ok(())
    }
}

#[async_trait]
impl BeamReports for RiverBeamReports {
    async fn explain_tool_policy(
        &self,
        caller: &Caller,
        tool: &str,
        agent: &str,
        session: Option<Uuid>,
    ) -> RiverbaseResult<Value> {
        if let Some(session) = session {
            self.owned(caller, session).await?;
        }
        self.beam
            .explain_tool_policy(
                caller.user_id.as_deref(),
                caller.org_id.as_deref(),
                tool,
                agent,
                session,
            )
            .await
    }

    async fn usage_summary(&self, caller: &Caller, period: Option<&str>) -> RiverbaseResult<Value> {
        let user = caller
            .user_id
            .as_deref()
            .ok_or_else(|| beam_core::BEM_003.with_data("the caller has no user id"))?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| beam_core::BEM_380.with_data("usage needs the beam database"))?;
        store.usage_summary(user, period, &self.usage).await
    }

    async fn export_session(&self, caller: &Caller, session: Uuid) -> RiverbaseResult<Value> {
        self.owned(caller, session).await?;
        self.beam.export_session(session).await
    }
}

/// `user_query[key]` as text.
fn input(request: &riverbase_core::query::QueryRequest, key: &str) -> Option<String> {
    request
        .user_query
        .as_ref()
        .and_then(|q| q.get(key))
        .or_else(|| request.params.as_ref().and_then(|p| p.get(key)))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn session_input(request: &riverbase_core::query::QueryRequest) -> RiverbaseResult<Option<Uuid>> {
    input(request, "session_id")
        .map(|raw| {
            Uuid::parse_str(&raw)
                .map_err(|_| beam_core::BEM_003.with_data("session_id must be a UUID"))
        })
        .transpose()
}

/// `tool-policy-explanation`.
///
/// # Errors
/// `WEB-001` without a tool or agent; the beam's errors.
pub async fn explain(
    ctx: &EngineContext,
    request: &riverbase_core::query::QueryRequest,
) -> RiverbaseResult<riverbase_core::query::ReportOutput> {
    let tool = input(request, "tool_name")
        .ok_or_else(|| beam_core::BEM_003.with_data("tool_name is required"))?;
    let agent = input(request, "agent_name")
        .ok_or_else(|| beam_core::BEM_003.with_data("agent_name is required"))?;
    let session = session_input(request)?;
    let caller = Caller::from_context(ctx);
    let document = reports()?
        .explain_tool_policy(&caller, &tool, &agent, session)
        .await?;
    Ok(riverbase_core::query::ReportOutput::Document(document))
}

/// `usage-summary`.
///
/// # Errors
/// The beam's errors.
pub async fn usage(
    ctx: &EngineContext,
    request: &riverbase_core::query::QueryRequest,
) -> RiverbaseResult<riverbase_core::query::ReportOutput> {
    let caller = Caller::from_context(ctx);
    let period = input(request, "period");
    let document = reports()?.usage_summary(&caller, period.as_deref()).await?;
    Ok(riverbase_core::query::ReportOutput::Document(document))
}

/// `session-export`.
///
/// # Errors
/// `WEB-001` without a session; the beam's errors.
pub async fn export(
    ctx: &EngineContext,
    request: &riverbase_core::query::QueryRequest,
) -> RiverbaseResult<riverbase_core::query::ReportOutput> {
    let session = session_input(request)?
        .ok_or_else(|| beam_core::BEM_003.with_data("session_id is required"))?;
    let caller = Caller::from_context(ctx);
    let document = reports()?.export_session(&caller, session).await?;
    Ok(riverbase_core::query::ReportOutput::Document(document))
}

riverbase_core::report_resource! {
    ToolPolicyExplanationReport name "tool-policy-explanation" {
        meta {
            title: "Tool policy explanation",
            description: "How a tool call resolves for the caller: mode, layer, locked, remember options.",
            default_order: [id.desc],
            scope: none,
        }
        filters {
            field id { preset: Uuid, label: "ID", identifier }
            field tool_name { preset: String, label: "Tool", no_sort }
            field agent_name { preset: String, label: "Agent", no_sort }
            field session_id { preset: String, label: "Session", no_sort }
        }
        params {}
        binding source "agent_session" {
            id => "_id",
        }
        policy: public("the report checks the caller's session itself", "BEAM-PLAN-023"),
        tenant_scope: exempt("beam rows have no _tenant; the report checks the caller's session", "BEAM-PLAN-023"),
        handler (_self, ctx, _session, request, _query) {
            crate::reports::explain(ctx, request).await
        }
    }
}

riverbase_core::report_resource! {
    UsageSummaryReport name "usage-summary" {
        meta {
            title: "Usage summary",
            description: "The caller's usage counters and plan for a period (today by default).",
            default_order: [id.desc],
            scope: none,
        }
        filters {
            field id { preset: Uuid, label: "ID", identifier }
            field period { preset: String, label: "Period", no_sort }
        }
        params {}
        binding source "agent_session" {
            id => "_id",
        }
        policy: public("the report reads the caller's own usage only", "BEAM-PLAN-023"),
        tenant_scope: exempt("beam rows have no _tenant; the report reads the caller's own usage", "BEAM-PLAN-023"),
        handler (_self, ctx, _session, request, _query) {
            crate::reports::usage(ctx, request).await
        }
    }
}

riverbase_core::report_resource! {
    SessionExportReport name "session-export" {
        meta {
            title: "Session export",
            description: "One of the caller's sessions with its messages and executions.",
            default_order: [id.desc],
            scope: none,
        }
        filters {
            field id { preset: Uuid, label: "ID", identifier }
            field session_id { preset: String, label: "Session", no_sort }
        }
        params {}
        binding source "agent_session" {
            id => "_id",
        }
        policy: public("the report checks the caller's session itself", "BEAM-PLAN-023"),
        tenant_scope: exempt("beam rows have no _tenant; the report checks the caller's session", "BEAM-PLAN-023"),
        handler (_self, ctx, _session, request, _query) {
            crate::reports::export(ctx, request).await
        }
    }
}
