//! Security decisions, `db.security()`: an `agent_security_decision` row
//! and its rule narrations, written in one transaction (Python
//! `SqlSecurityStore`).

use diesel::sql_query;
use diesel::sql_types::{Double, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel_async::RunQueryDsl;
use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, DbError, DbResult};

/// A decision to store; the enum columns are the labels Python writes.
#[derive(Debug, Clone)]
pub struct NewDecision {
    pub id: Uuid,
    pub stage: String,
    pub action: String,
    pub severity: String,
    pub score: f64,
    pub narration: String,
    pub user_message: Option<String>,
    pub violation_type: Option<String>,
    pub payload: Value,
    pub matched_rules: Value,
    pub metadata: Value,
    pub run_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub interaction_id: Option<Uuid>,
    /// The invoke's org (Python leaves it null; optional RLS needs it).
    pub org_id: Option<Uuid>,
    pub narrations: Vec<NewNarration>,
}

/// One rule's narration of a decision.
#[derive(Debug, Clone)]
pub struct NewNarration {
    pub rule: String,
    pub code: i32,
    pub message: String,
    pub ruleset: String,
    pub revision: i32,
}

/// The security decisions on a [`Db`].
pub struct Security<'d> {
    db: &'d Db,
}

impl Db {
    /// Security decisions.
    #[must_use]
    pub fn security(&self) -> Security<'_> {
        Security { db: self }
    }
}

impl Security<'_> {
    /// Store `decision` and its narrations. The row's user is its session's
    /// (the inspector's query resource is scoped by it).
    ///
    /// # Errors
    /// A failed checkout or write (nothing is stored then).
    pub async fn save_decision(&self, decision: NewDecision) -> DbResult<()> {
        let s = self.db.schema().clone();
        self.db
            .transaction::<_, DbError, _>(|tx| {
                Box::pin(async move {
                    sql_query(format!(
                        "INSERT INTO {s}.agent_security_decision \
                         (_id, run_id, stage, action, severity, score, narration, user_message, violation_type, \
                          payload_json, matched_rules, metadata_json, recorded_at, org_id, \
                          session_id, interaction_id, user_id) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(), $13, $14, $15, \
                          (SELECT user_id FROM {s}.agent_session WHERE _id = $14))"
                    ))
                    .bind::<SqlUuid, _>(decision.id)
                    .bind::<Nullable<SqlUuid>, _>(decision.run_id)
                    .bind::<Text, _>(decision.stage)
                    .bind::<Text, _>(decision.action)
                    .bind::<Text, _>(decision.severity)
                    .bind::<Double, _>(decision.score)
                    .bind::<Text, _>(decision.narration)
                    .bind::<Nullable<Text>, _>(decision.user_message)
                    .bind::<Nullable<Text>, _>(decision.violation_type)
                    .bind::<Jsonb, _>(decision.payload)
                    .bind::<Jsonb, _>(decision.matched_rules)
                    .bind::<Jsonb, _>(decision.metadata)
                    .bind::<Nullable<SqlUuid>, _>(decision.org_id)
                    .bind::<Nullable<SqlUuid>, _>(decision.session_id)
                    .bind::<Nullable<SqlUuid>, _>(decision.interaction_id)
                    .execute(tx)
                    .await?;
                    for n in decision.narrations {
                        sql_query(format!(
                            "INSERT INTO {s}.agent_security_rule_narration \
                             (_id, decision_id, rule, code, message, ruleset, revision) \
                             VALUES ($1, $2, $3, $4, $5, $6, $7)"
                        ))
                        .bind::<SqlUuid, _>(Uuid::new_v4())
                        .bind::<SqlUuid, _>(decision.id)
                        .bind::<Text, _>(n.rule)
                        .bind::<Integer, _>(n.code)
                        .bind::<Text, _>(n.message)
                        .bind::<Text, _>(n.ruleset)
                        .bind::<Integer, _>(n.revision)
                        .execute(tx)
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
    }
}
