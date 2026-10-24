//! Security decision persistence (Python `security/stores/`): every evaluated
//! decision and its rule narrations, when a store is configured.

use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::codes::RuleNarration;
use crate::decision::SecurityDecision;

/// One stored decision (an `agent_security_decision` row with its narrations).
#[derive(Debug, Clone)]
pub struct DecisionRecord {
    pub id: Uuid,
    pub stage: String,
    pub run_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub interaction_id: Option<Uuid>,
    pub decision: SecurityDecision,
    pub narrations: Vec<RuleNarration>,
}

/// Where a decision was made: its run, session and interaction (rc8's
/// `SqlSecurityStore` stores all three, and user-data deletion and the
/// inspector find decisions by session).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecisionScope {
    pub run_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub interaction_id: Option<Uuid>,
}

impl DecisionScope {
    /// The scope of ids given as text (unparseable ones are dropped).
    #[must_use]
    pub fn parse(
        run_id: Option<&str>,
        session_id: Option<&str>,
        interaction_id: Option<&str>,
    ) -> Self {
        let id = |raw: Option<&str>| raw.and_then(|r| Uuid::parse_str(r.trim()).ok());
        Self {
            run_id: id(run_id),
            session_id: id(session_id),
            interaction_id: id(interaction_id),
        }
    }
}

/// Where security decisions are recorded.
#[async_trait]
pub trait SecurityStore: Send + Sync {
    /// Store `decision` for `stage`; returns the decision id.
    ///
    /// # Errors
    /// The store's error text.
    async fn save_decision(
        &self,
        decision: &SecurityDecision,
        stage: &str,
        scope: &DecisionScope,
        narrations: &[RuleNarration],
    ) -> Result<Uuid, String>;
}

/// Decisions in process memory (tests, single-process tools).
#[derive(Default)]
pub struct InMemorySecurityStore {
    rows: Mutex<Vec<DecisionRecord>>,
}

impl InMemorySecurityStore {
    #[must_use]
    pub fn records(&self) -> Vec<DecisionRecord> {
        self.rows.lock().map(|r| r.clone()).unwrap_or_default()
    }
}

#[async_trait]
impl SecurityStore for InMemorySecurityStore {
    async fn save_decision(
        &self,
        decision: &SecurityDecision,
        stage: &str,
        scope: &DecisionScope,
        narrations: &[RuleNarration],
    ) -> Result<Uuid, String> {
        let id = Uuid::new_v4();
        self.rows
            .lock()
            .map_err(|e| e.to_string())?
            .push(DecisionRecord {
                id,
                stage: stage.to_owned(),
                run_id: scope.run_id,
                session_id: scope.session_id,
                interaction_id: scope.interaction_id,
                decision: decision.clone(),
                narrations: narrations.to_vec(),
            });
        Ok(id)
    }
}

/// The `agent_security_decision` columns Python writes (`SqlSecurityStore`).
#[must_use]
pub fn decision_columns(
    decision: &SecurityDecision,
) -> (String, String, Option<String>, Value, Value) {
    let label = |v: Value| v.as_str().map(str::to_owned).unwrap_or_default();
    let action = label(serde_json::to_value(decision.action).unwrap_or(Value::Null));
    let severity = label(serde_json::to_value(decision.severity).unwrap_or(Value::Null));
    let violation = decision
        .violation_type
        .and_then(|v| serde_json::to_value(v).ok())
        .and_then(|v| v.as_str().map(str::to_owned));
    let mut metadata = Map::new();
    if let Some(sanitized) = decision
        .sanitized_content
        .as_ref()
        .filter(|s| !s.is_empty())
    {
        metadata.insert("sanitized_content".into(), Value::String(sanitized.clone()));
    }
    (
        action,
        severity,
        violation,
        Value::Object(decision.metadata.clone()),
        Value::Object(metadata),
    )
}

#[cfg(feature = "postgres")]
pub use pg::PgSecurityStore;

#[cfg(feature = "postgres")]
mod pg {
    use async_trait::async_trait;
    use beam_store::repo::security::{NewDecision, NewNarration};
    use beam_store::{Db, PgBeamStore};
    use serde_json::Value;
    use uuid::Uuid;

    use super::{decision_columns, DecisionScope, SecurityStore};
    use crate::codes::RuleNarration;
    use crate::decision::SecurityDecision;

    /// Python `SqlSecurityStore` on `<schema>.agent_security_decision` and
    /// `agent_security_rule_narration`, one transaction per decision.
    #[derive(Clone)]
    pub struct PgSecurityStore {
        db: Db,
    }

    impl PgSecurityStore {
        /// The decisions of `store`'s database and schema.
        #[must_use]
        pub fn for_store(store: &PgBeamStore) -> Self {
            Self {
                db: store.db().clone(),
            }
        }
    }

    #[async_trait]
    impl SecurityStore for PgSecurityStore {
        async fn save_decision(
            &self,
            decision: &SecurityDecision,
            stage: &str,
            scope: &DecisionScope,
            narrations: &[RuleNarration],
        ) -> Result<Uuid, String> {
            let id = Uuid::new_v4();
            let (action, severity, violation_type, payload, metadata) = decision_columns(decision);
            let record = NewDecision {
                id,
                stage: stage.to_owned(),
                action,
                severity,
                score: decision.score,
                narration: decision.narration.clone(),
                user_message: decision.user_message.clone(),
                violation_type,
                payload,
                matched_rules: Value::from(decision.matched_rules.clone()),
                metadata,
                run_id: scope.run_id,
                session_id: scope.session_id,
                interaction_id: scope.interaction_id,
                org_id: beam_store::org_scope::current_org(),
                narrations: narrations
                    .iter()
                    .map(|n| NewNarration {
                        rule: n.rule.clone(),
                        code: n.code,
                        message: n.message.clone(),
                        ruleset: n.ruleset.clone(),
                        revision: n.revision,
                    })
                    .collect(),
            };
            self.db
                .security()
                .save_decision(record)
                .await
                .map_err(|e| e.to_string())?;
            Ok(id)
        }
    }
}
