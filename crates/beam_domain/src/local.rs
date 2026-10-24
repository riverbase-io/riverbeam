//! The beam domain's own logic over its statements (Python
//! `BeamAggregate`): the agent behind a command URL, session recording,
//! session and message updates, agent profiles, ownership checks, and the
//! `.usage` summary. Every row is checked against the caller first (another
//! org's is `BEM-411`, another user's `BEM-412`).
//!
//! The statements live in `beam_store`
//! ([`beam_store::repo::domain::DomainSql`]) and run on any
//! [`DomainExec`]: [`DomainStore`] on the pool (the HTTP app's own routes),
//! the aggregate's [`beam_store::repo::domain::CommandExec`] on a riverbase
//! command's transaction. [`BeamSql`] adds the ownership rules and answers.

use async_trait::async_trait;
use beam_store::repo::domain::{DomainExec, DomainSql, PlanColumn, Statement};
use beam_store::{Db, DbError, DbResult, PgBeamStore, SchemaName};
use riverbase_core::{RiverbaseError, RiverbaseResult};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use riverbase_core::base::AggregateContext;
use riverbase_core::EngineContext;

/// The caller a command or query runs for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Caller {
    pub user_id: Option<String>,
    pub org_id: Option<String>,
    /// `beam-admin`: every row.
    pub admin: bool,
}

impl Caller {
    #[must_use]
    pub fn from_context(ctx: &EngineContext) -> Self {
        Self {
            user_id: ctx.actor.user_id.map(|u| u.to_string()),
            org_id: ctx.organization_id.map(|o| o.to_string()),
            admin: ctx.roles.iter().any(|r| r == crate::authz::ADMIN_ROLE),
        }
    }

    /// The caller of a riverbase command.
    #[must_use]
    pub fn from_aggregate(ctx: &AggregateContext) -> Self {
        Self {
            user_id: ctx.actor.user_id.map(|u| u.to_string()),
            org_id: ctx.organization_id.map(|o| o.to_string()),
            admin: ctx.roles.iter().any(|r| r == crate::authz::ADMIN_ROLE),
        }
    }
}

/// A statement's failure: a beam error as it is; the database's message is
/// logged, never returned (`BEM-497`).
pub(crate) fn sql(e: DbError) -> RiverbaseError {
    match e {
        DbError::Beam(e) => e,
        other => {
            tracing::error!(error = %other, "beam domain SQL failed");
            beam_core::BEM_497.raise()
        }
    }
}

/// The API's statements on the pool, scoped by the running task's org
/// (optional RLS). Each write is its own transaction.
#[derive(Clone)]
pub struct DomainStore {
    db: Db,
}

impl DomainStore {
    #[must_use]
    pub fn for_store(store: &PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

#[async_trait]
impl DomainExec for DomainStore {
    fn schema(&self) -> &SchemaName {
        self.db.schema()
    }

    async fn rows(&self, statement: Statement) -> DbResult<Vec<Value>> {
        self.db.rows(statement).await
    }

    async fn write(&self, table: &str, id: Uuid, data: Value) -> DbResult<()> {
        self.db.write(table, id, data).await
    }

    async fn soft_delete(&self, table: &str, id: Uuid) -> DbResult<()> {
        self.db.soft_delete(table, id).await
    }
}

/// The `_id` of `row`, a row [`BeamSql::one`] returned.
fn row_id(row: &Value) -> RiverbaseResult<Uuid> {
    row.get("_id")
        .and_then(Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok())
        .ok_or_else(|| beam_core::BEM_497.with_data("row without an _id"))
}

/// What `stream` records for its session.
#[derive(Debug, Clone, Default)]
pub struct SessionRecord {
    pub session_id: Uuid,
    pub agent_id: Uuid,
    pub user_id: Option<String>,
    pub org_id: Option<String>,
    pub profile_id: Option<String>,
    pub name: String,
    pub resource: Option<String>,
    pub resource_id: Option<String>,
}

fn uuid_text(value: Option<&str>) -> Option<Uuid> {
    value.and_then(|v| Uuid::parse_str(v.trim()).ok())
}

/// A user or org id trimmed; blank, `none`, `null` and the nil UUID are none.
fn normalize(value: Option<&str>) -> Option<String> {
    beam_execution::identity::normalize_identity(value)
}

/// `caller` may change a row owned by `org` / `user` through a worker task:
/// it must be the caller's own (a row with no user is refused); admins may
/// change any row.
fn check_strict_owner(row: &Value, caller: &Caller, entity: &str) -> RiverbaseResult<()> {
    if caller.admin {
        return Ok(());
    }
    let field = |k: &str| row.get(k).and_then(Value::as_str).map(str::to_owned);
    if let Some(org) = field("org_id") {
        if caller.org_id.as_deref() != Some(org.as_str()) {
            return Err(beam_core::BEM_069.with_data(format!("{entity} belongs to another tenant")));
        }
    }
    let owner = normalize(field("user_id").as_deref());
    if owner.is_none() || owner != normalize(caller.user_id.as_deref()) {
        return Err(beam_core::BEM_071.with_data(json!({"entity": entity})));
    }
    Ok(())
}

/// `caller` may act on a row owned by `org` / `user`.
fn check_owner(row: &Value, caller: &Caller, entity: &str) -> RiverbaseResult<()> {
    if caller.admin {
        return Ok(());
    }
    let field = |k: &str| row.get(k).and_then(Value::as_str).map(str::to_owned);
    if let Some(org) = field("org_id") {
        if caller.org_id.as_deref() != Some(org.as_str()) {
            return Err(beam_core::BEM_069.with_data(format!("{entity} belongs to another tenant")));
        }
    }
    let owner = normalize(field("user_id").as_deref());
    if owner.is_some() && owner != normalize(caller.user_id.as_deref()) {
        return Err(beam_core::BEM_071.with_data(json!({"entity": entity})));
    }
    Ok(())
}

/// What [`BeamSql::usage_summary`] needs from the deployment: the same
/// switch and default plan the invoke pipeline enforces (`BeamConfig.usage_enabled`
/// and the builder's `usage_plans(.., default_plan)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSummarySettings {
    /// Off: the summary has the counters only.
    pub enabled: bool,
    /// The plan of a user without a profile; `None` reports no plan.
    pub default_plan: Option<String>,
}

/// The beam domain's statements with their ownership rules, over any
/// [`DomainExec`].
#[async_trait]
pub trait BeamSql: DomainExec {
    /// Row `id` of `table` (not soft-deleted), as JSON.
    ///
    /// # Errors
    /// Store errors.
    async fn one(&self, table: &str, id: &str) -> RiverbaseResult<Option<Value>> {
        self.row(table, id).await.map_err(sql)
    }

    /// The agent a command URL names: its `_id`, or its name. No row locks
    /// are taken (Python's DAT-904 fix).
    ///
    /// # Errors
    /// `BEM-024` (public `BEM-404`) for an unknown agent; store errors.
    async fn agent(&self, identifier: &str) -> RiverbaseResult<Value> {
        self.agent_row(identifier)
            .await
            .map_err(sql)?
            .ok_or_else(|| beam_core::BEM_024.with_data(identifier.to_owned()))
    }

    /// The name of the agent a command URL names (see [`Self::agent`]).
    ///
    /// # Errors
    /// `BEM-024` (public `BEM-404`) for an unknown agent; store errors.
    async fn agent_name(&self, identifier: &str) -> RiverbaseResult<String> {
        self.agent(identifier)
            .await?
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| beam_core::BEM_024.with_data(identifier.to_owned()))
    }

    /// The caller's session `id` (`BEM-501` unknown, `BEM-411` / `BEM-412`
    /// another org's or user's).
    ///
    /// # Errors
    /// As above; store errors.
    async fn owned_session(&self, id: &str, caller: &Caller) -> RiverbaseResult<Value> {
        let session = self
            .one("agent_session", id)
            .await?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"session_id": id})))?;
        check_owner(&session, caller, "session")?;
        Ok(session)
    }

    /// Python `record_session` (for `stream` and `invoke`): create the
    /// session, or confirm the caller owns it (`BEM-412` otherwise).
    ///
    /// # Errors
    /// `BEM-411` / `BEM-412`; store errors.
    async fn record_session(
        &self,
        record: &SessionRecord,
        caller: &Caller,
    ) -> RiverbaseResult<Value> {
        // Insert only: an existing session keeps its owner, and the check
        // below refuses another caller's.
        self.insert(
            "agent_session",
            record.session_id,
            json!({
                "agent_id": record.agent_id,
                "user_id": uuid_text(record.user_id.as_deref()),
                "org_id": uuid_text(record.org_id.as_deref()),
                "profile_id": uuid_text(record.profile_id.as_deref()),
                "name": record.name,
                "resource": record.resource,
                "resource_id": uuid_text(record.resource_id.as_deref()),
            }),
        )
        .await
        .map_err(sql)?;
        self.owned_session(&record.session_id.to_string(), caller)
            .await
    }

    /// `update-session`: the session's name.
    ///
    /// # Errors
    /// `BEM-500` with nothing to update; `BEM-501`; `BEM-411` / `BEM-412`.
    async fn update_session(
        &self,
        id: &str,
        payload: &Value,
        caller: &Caller,
    ) -> RiverbaseResult<Value> {
        let name = payload.get("name").and_then(Value::as_str);
        let Some(name) = name else {
            return Err(beam_core::BEM_500.raise());
        };
        let session = self.owned_session(id, caller).await?;
        self.write("agent_session", row_id(&session)?, json!({ "name": name }))
            .await
            .map_err(sql)?;
        self.one("agent_session", id)
            .await?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"session_id": id})))
    }

    /// `remove-session`: the session is removed (soft delete, as Python
    /// invalidates it) and its runtime state purged.
    ///
    /// # Errors
    /// `BEM-501`; `BEM-411` / `BEM-412`; store errors.
    async fn remove_session(&self, id: &str, caller: &Caller) -> RiverbaseResult<Value> {
        let session = self.owned_session(id, caller).await?;
        self.purge_session_runtime_state(id).await.map_err(sql)?;
        self.soft_delete("agent_session", row_id(&session)?)
            .await
            .map_err(sql)?;
        Ok(json!({"status": "removed", "session_id": id}))
    }

    /// `remove-message`: one message of the session (soft delete).
    ///
    /// # Errors
    /// `BEM-501` for an unknown message; `BEM-411` / `BEM-412`.
    async fn remove_message(
        &self,
        session_id: &str,
        message_id: &str,
        caller: &Caller,
    ) -> RiverbaseResult<Value> {
        self.owned_session(session_id, caller).await?;
        let message = self
            .one("agent_message", message_id)
            .await?
            .filter(|m| m.get("session_id").and_then(Value::as_str) == Some(session_id))
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"message_id": message_id})))?;
        self.soft_delete("agent_message", row_id(&message)?)
            .await
            .map_err(sql)?;
        // An edit invalidates the session summary covering it (rc8
        // `invalidate_from`): its coverage is cut before it.
        if let Some(sequence) = message.get("sequence").and_then(Value::as_i64) {
            self.invalidate_summary_from(session_id, sequence)
                .await
                .map_err(sql)?;
        }
        Ok(json!({"status": "removed", "session_id": session_id, "message_id": message_id}))
    }

    /// `create-agent-profile`: one profile per `profile_id`, for the caller.
    /// The payload may repeat the caller's `user_id` and `profile_id`, never
    /// name another (Python takes them from the payload).
    ///
    /// # Errors
    /// `BEM-498` without an identity; `BEM-071` for another user's or
    /// profile's ids; `BEM-502` for a `profile_id` that is not a UUID;
    /// `BEM-499` when it exists; store errors.
    async fn create_profile(
        &self,
        payload: &Value,
        caller: &Caller,
        caller_profile: Option<&str>,
    ) -> RiverbaseResult<Value> {
        let text = |k: &str| payload.get(k).and_then(Value::as_str).map(str::to_owned);
        let same = |a: &str, b: &str| a.trim().eq_ignore_ascii_case(b.trim());
        let Some(user_id) = caller.user_id.clone() else {
            return Err(beam_core::BEM_498.raise());
        };
        if text("user_id").is_some_and(|u| !same(&u, &user_id)) {
            return Err(beam_core::BEM_071.with_data("agent_profile belongs to another user"));
        }
        let profile_id = match (text("profile_id"), caller_profile) {
            (Some(asked), Some(own)) if !same(&asked, own) => {
                return Err(
                    beam_core::BEM_071.with_data("agent_profile belongs to another profile")
                );
            }
            (asked, own) => asked.or_else(|| own.map(str::to_owned)),
        };
        let Some(profile_id) = profile_id else {
            return Err(beam_core::BEM_498.raise());
        };
        if Uuid::parse_str(profile_id.trim()).is_err() {
            return Err(beam_core::BEM_502.with_data("profile_id must be a UUID"));
        }
        let existing = self
            .profiles_with_profile_id(&profile_id)
            .await
            .map_err(sql)?;
        if !existing.is_empty() {
            return Err(beam_core::BEM_499.raise());
        }
        let opt = |k: &str| {
            payload
                .get(k)
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or(Value::Null)
        };
        let id = Uuid::new_v4();
        self.insert(
            "agent_profile",
            id,
            json!({
                "user_id": user_id,
                "profile_id": profile_id,
                "role": opt("role"),
                "rules": opt("rules"),
                "metadata_json": opt("metadata_json"),
            }),
        )
        .await
        .map_err(sql)?;
        self.one("agent_profile", &id.to_string())
            .await?
            .ok_or_else(|| beam_core::BEM_497.with_data("insert returned no row"))
    }

    /// The caller's profile `id` (`BEM-501` unknown, `BEM-412` another's).
    ///
    /// # Errors
    /// As above; store errors.
    async fn owned_profile(&self, id: &str, caller: &Caller) -> RiverbaseResult<Value> {
        let profile = self
            .one("agent_profile", id)
            .await?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"agent_profile_id": id})))?;
        check_owner(&profile, caller, "agent_profile")?;
        Ok(profile)
    }

    /// `update-agent-profile`: the non-null fields of `role`, `rules`,
    /// `metadata_json`.
    ///
    /// # Errors
    /// `BEM-500`; `BEM-501`; `BEM-412`.
    async fn update_profile(
        &self,
        id: &str,
        payload: &Value,
        caller: &Caller,
    ) -> RiverbaseResult<Value> {
        let updates: Map<String, Value> = ["role", "rules", "metadata_json"]
            .iter()
            .filter_map(|k| {
                payload
                    .get(*k)
                    .filter(|v| !v.is_null())
                    .map(|v| ((*k).to_owned(), v.clone()))
            })
            .collect();
        if updates.is_empty() {
            return Err(beam_core::BEM_500.raise());
        }
        let profile = self.owned_profile(id, caller).await?;
        self.write("agent_profile", row_id(&profile)?, Value::Object(updates))
            .await
            .map_err(sql)?;
        self.one("agent_profile", id)
            .await?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"agent_profile_id": id})))
    }

    /// `remove-agent-profile` (soft delete, as Python invalidates it).
    ///
    /// # Errors
    /// `BEM-501`; `BEM-412`.
    async fn remove_profile(&self, id: &str, caller: &Caller) -> RiverbaseResult<Value> {
        let profile = self.owned_profile(id, caller).await?;
        self.soft_delete("agent_profile", row_id(&profile)?)
            .await
            .map_err(sql)?;
        Ok(json!({"status": "removed", "agent_profile_id": id}))
    }

    /// The caller's execution `id` (owned through its session; its own org
    /// counts too), for `cancel`, `retry` and feedback.
    ///
    /// # Errors
    /// `BEM-501` unknown; `BEM-411` / `BEM-412` not the caller's.
    async fn writable_execution(&self, id: &str, caller: &Caller) -> RiverbaseResult<Value> {
        let owner = self
            .execution_owner(id)
            .await
            .map_err(sql)?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"execution_id": id})))?;
        check_strict_owner(&owner, caller, "agent_execution")?;
        // The execution's own org must be the caller's too.
        let row_org = owner.get("row_org_id").and_then(Value::as_str);
        if !caller.admin && row_org.is_some() && row_org != caller.org_id.as_deref() {
            return Err(beam_core::BEM_069.with_data("agent_execution belongs to another tenant"));
        }
        Ok(owner)
    }

    /// The caller's long-term memory `id`, for forget, edit and expire.
    ///
    /// # Errors
    /// `BEM-501` unknown; `BEM-411` / `BEM-412` not the caller's.
    async fn writable_memory(&self, id: &str, caller: &Caller) -> RiverbaseResult<Value> {
        let memory = self
            .one("agent_memory", id)
            .await?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({"memory_id": id})))?;
        check_strict_owner(&memory, caller, "agent_memory")?;
        Ok(memory)
    }

    /// Python `compute_usage_payload` (`GET /api/{namespace}/.usage`): the
    /// caller's request and token counters for `period` (default today), and
    /// when `settings.enabled` the active plan's limits and what remains.
    /// `settings` are the ones enforcement uses, so the summary
    /// names the plan a user is held to.
    ///
    /// # Errors
    /// Store errors: a failed read is an error, never an empty summary.
    async fn usage_summary(
        &self,
        user_id: &str,
        period: Option<&str>,
        settings: &UsageSummarySettings,
    ) -> RiverbaseResult<Value> {
        let period = period.map_or_else(
            || chrono::Utc::now().format("%Y-%m-%d").to_string(),
            str::to_owned,
        );
        let counters = self.usage_counters(user_id, &period).await.map_err(sql)?;
        let counter = |field: &str| {
            counters
                .iter()
                .filter(|r| r.get("field").and_then(Value::as_str) == Some(field))
                .filter_map(|r| r.get("value").and_then(Value::as_i64))
                .next_back()
                .unwrap_or(0)
        };
        let (requests, tokens) = (counter("requests"), counter("tokens"));
        let mut result =
            json!({"total_requests": requests, "total_tokens": tokens, "period": period});
        if !settings.enabled {
            return Ok(result);
        }
        let profile = self.active_usage_profile(user_id).await.map_err(sql)?;
        let plan_key = profile
            .as_ref()
            .and_then(|p| p.get("plan_key").and_then(Value::as_str).map(str::to_owned))
            .or_else(|| settings.default_plan.clone());
        let mut plan = None;
        if let Some(plan_id) = profile
            .as_ref()
            .and_then(|p| p.get("plan_id"))
            .and_then(Value::as_str)
        {
            plan = self
                .active_plan(PlanColumn::Id, plan_id)
                .await
                .map_err(sql)?;
        }
        if let (None, Some(key)) = (&plan, &plan_key) {
            plan = self.active_plan(PlanColumn::Name, key).await.map_err(sql)?;
        }
        let Some(plan) = plan else {
            result["plan"] = json!(plan_key);
            result["plan_active"] = json!(false);
            return Ok(result);
        };
        Ok(with_plan(
            result,
            &plan,
            plan_key.as_deref(),
            requests,
            tokens,
        ))
    }
}

/// `result` with `plan`'s name, limits and what remains of them.
fn with_plan(
    mut result: Value,
    plan: &Value,
    plan_key: Option<&str>,
    requests: i64,
    tokens: i64,
) -> Value {
    result["plan"] = json!(plan_key);
    result["plan_name"] = plan.get("name").cloned().unwrap_or(Value::Null);
    result["plan_active"] = json!(true);
    result["overage_policy"] = plan
        .get("overage_policy")
        .cloned()
        .unwrap_or_else(|| json!("hard_block"));
    let mut limits = Map::new();
    let mut remaining = Map::new();
    for (column, key) in [
        ("limit_max_requests_per_day", "max_requests_per_day"),
        ("limit_max_tokens_per_day", "max_tokens_per_day"),
        ("limit_max_tokens_per_request", "max_tokens_per_request"),
        ("limit_allowed_agents", "allowed_agents"),
        ("limit_allowed_modalities", "allowed_modalities"),
        ("limit_allowed_models", "allowed_models"),
    ] {
        if let Some(value) = plan.get(column).filter(|v| !v.is_null()) {
            limits.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(max) = limits.get("max_requests_per_day").and_then(Value::as_i64) {
        remaining.insert("requests".into(), json!(max - requests));
    }
    if let Some(max) = limits.get("max_tokens_per_day").and_then(Value::as_i64) {
        remaining.insert("tokens".into(), json!(max - tokens));
    }
    result["limits"] = Value::Object(limits);
    result["remaining"] = Value::Object(remaining);
    result
}

impl<T: DomainExec + ?Sized> BeamSql for T {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canned rows per table the statement reads; `down` fails every read.
    #[derive(Default)]
    struct Rows {
        counters: Vec<Value>,
        profiles: Vec<Value>,
        plans: Vec<Value>,
        down: bool,
        schema: SchemaName,
    }

    #[async_trait]
    impl DomainExec for Rows {
        fn schema(&self) -> &SchemaName {
            &self.schema
        }

        async fn rows(&self, statement: Statement) -> DbResult<Vec<Value>> {
            if self.down {
                return Err(DbError::Beam(
                    beam_core::BEM_497.with_data("connection refused"),
                ));
            }
            let text = statement.sql();
            Ok(if text.contains("agent_usage_counter") {
                self.counters.clone()
            } else if text.contains("agent_usage_profile") {
                self.profiles.clone()
            } else {
                self.plans.clone()
            })
        }

        async fn write(&self, _table: &str, _id: Uuid, _data: Value) -> DbResult<()> {
            Ok(())
        }

        async fn soft_delete(&self, _table: &str, _id: Uuid) -> DbResult<()> {
            Ok(())
        }
    }

    fn settings(enabled: bool, default_plan: Option<&str>) -> UsageSummarySettings {
        UsageSummarySettings {
            enabled,
            default_plan: default_plan.map(str::to_owned),
        }
    }

    /// Plan 019: a failed read is an error. Before, the summary answered zero
    /// usage and no plan.
    #[tokio::test]
    async fn a_failed_read_is_an_error_not_an_empty_summary() {
        let rows = Rows {
            down: true,
            ..Rows::default()
        };
        let err = rows
            .usage_summary("u1", Some("2026-10-21"), &settings(true, Some("free")))
            .await
            .unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-497");
    }

    #[tokio::test]
    async fn disabled_usage_reports_the_counters_only() {
        let rows = Rows {
            counters: vec![
                json!({"field": "requests", "value": 3}),
                json!({"field": "tokens", "value": 120}),
            ],
            ..Rows::default()
        };
        let summary = rows
            .usage_summary("u1", Some("2026-10-21"), &settings(false, Some("free")))
            .await
            .unwrap();
        assert_eq!(
            summary,
            json!({"total_requests": 3, "total_tokens": 120, "period": "2026-10-21"})
        );
    }

    #[tokio::test]
    async fn no_profile_and_no_default_plan_reports_no_plan() {
        let summary = Rows::default()
            .usage_summary("u1", Some("2026-10-21"), &settings(true, None))
            .await
            .unwrap();
        assert_eq!(summary["plan"], Value::Null);
        assert_eq!(summary["plan_active"], false);
    }

    #[tokio::test]
    async fn a_profiles_plan_gives_limits_and_what_remains() {
        let rows = Rows {
            counters: vec![json!({"field": "requests", "value": 4})],
            profiles: vec![json!({"plan_key": "pro", "plan_id": null})],
            plans: vec![json!({"name": "pro", "limit_max_requests_per_day": 10})],
            ..Rows::default()
        };
        let summary = rows
            .usage_summary("u1", Some("2026-10-21"), &settings(true, Some("free")))
            .await
            .unwrap();
        assert_eq!(summary["plan"], "pro");
        assert_eq!(summary["plan_active"], true);
        assert_eq!(summary["limits"], json!({"max_requests_per_day": 10}));
        assert_eq!(summary["remaining"], json!({"requests": 6}));
    }
}
