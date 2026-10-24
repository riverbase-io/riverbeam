//! The ops and hold stores, user-data deletion and export, and health
//! (Python `store/ops/health.py`).

use crate::river_beam::RiverBeam;
use beam_store::{LegalHoldStore, OpsStore};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

impl RiverBeam {
    /// The stream outbox and dead letters.
    #[must_use]
    pub fn ops(&self) -> &Arc<dyn OpsStore> {
        &self.ops
    }

    /// Legal holds: Postgres on a Postgres beam, so a hold placed
    /// by any process stops every process's deletion.
    #[must_use]
    pub fn holds(&self) -> &Arc<dyn LegalHoldStore> {
        &self.holds
    }

    /// `export_session`: the session, its messages
    /// and its execution ids, with credentials redacted.
    ///
    /// # Errors
    /// Store errors.
    pub async fn export_session(&self, session_id: Uuid) -> RiverbaseResult<Value> {
        let mut bundle = self.store.export_session(session_id).await?;
        let executions: Vec<Value> = self
            .executions
            .records()
            .list_for_session(session_id)
            .await?
            .into_iter()
            .map(|e| Value::String(e.id.to_string()))
            .collect();
        if let Value::Object(map) = &mut bundle {
            map.insert("executions".into(), Value::Array(executions));
        }
        Ok(beam_telemetry::redact_secrets(&bundle))
    }

    /// `delete_user_agent_data`: a held user
    /// is refused (`BEM-489`, public `BEM-433`); otherwise every row of the
    /// user's sessions and executions, audit events, memories and documents
    /// goes, in one transaction. Usage and cost records stay. The Postgres
    /// store checks the hold again inside its delete transaction.
    ///
    /// # Errors
    /// `BEM-489`; a hold that cannot be read (deletion fails closed); store
    /// errors.
    pub async fn delete_user_agent_data(&self, user_id: &str) -> RiverbaseResult<Value> {
        if self.holds.is_held(user_id).await? {
            return Err(crate::BEM_489.with_data(json!({"user_id": user_id})));
        }
        let counts = self.store.delete_user_data(user_id).await?;
        let n = |table: &str| counts.get(table).copied().unwrap_or(0);
        Ok(json!({
            "purged_refs": n("runtime_state"),
            "sessions": n("agent_session"),
            "media_refs": 0,
            "audit": n("agent_event"),
            "outbox": n("agent_outbox_event"),
            "dead_letters": n("agent_dead_letter"),
            "artifacts": n("agent_artifact"),
            "tables": counts,
        }))
    }

    // --- stream channels and replay ------------------------------------------

    /// `GET /health/live`: the process only.
    #[must_use]
    pub fn liveness() -> Value {
        json!({"status": "ok"})
    }

    /// `GET /health/ready`: the database answers at the
    /// latest beam migration, and the instance is not draining. The queue
    /// transport (NATS) is the host's to check: the app adds it.
    pub async fn readiness(&self) -> Value {
        let sql = if self.store.durable() {
            Some(self.ops.ping().await.is_ok())
        } else {
            None
        };
        let latest = beam_store::migrate::latest_version();
        let version = self.ops.schema_version().await.ok().flatten();
        let schema_status = match (version, sql) {
            // A database without the beam ledger (or unreadable) is not ready.
            (None, Some(_)) => Value::from("missing"),
            (None, None) => Value::Null,
            // `version` is the last migration with none missing before it
            // (plan 020 D10), so a gap reads as behind.
            (Some(v), _) if v >= latest => Value::from("compatible"),
            (Some(_), _) => Value::from("behind"),
        };
        let draining = !self.executions.accepting();
        let schema_ok = schema_status.is_null() || schema_status == "compatible";
        let ready = sql != Some(false) && schema_ok && !draining;
        json!({
            "status": if ready { "ok" } else { "unavailable" },
            "sql": sql,
            // fluvius-beam's marker (rc8's 26 once 0010 is applied);
            // the migration ledger is in `detail`.
            "schema_version": version.map(beam_store::migrate::python_schema_version),
            "schema_status": schema_status,
            "draining": draining,
            "telemetry": "optional",
            "detail": {
                "execution_inflight": self.executions.inflight(),
                "migration": version,
                "latest_migration": latest,
            },
        })
    }
}
