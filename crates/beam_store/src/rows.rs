//! Row types for the `ref_beam` tables this store reads and writes.
//!
//! Columns follow the fluvius-beam schema (`tests/schema/python_ref_beam.sql`).
//! Status and kind columns are Postgres enums whose labels are the Python enum
//! **member names** (`ACTIVE`, `IN_PROGRESS`, ...), because `SQLAlchemy` stores
//! member names. [`DbEnum::db_label`] gives that label.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A Rust enum stored as a `ref_beam` Postgres enum.
pub trait DbEnum: Sized + Copy {
    /// Postgres enum type name inside the beam schema (e.g. `runstatus`).
    const PG_TYPE: &'static str;
    /// Label stored in Postgres (the Python member name).
    fn db_label(self) -> &'static str;
    /// Parse a stored label.
    fn from_db_label(label: &str) -> Option<Self>;
}

macro_rules! db_enum {
    ($(#[$meta:meta])* $name:ident, $pg:literal { $($variant:ident => $label:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl DbEnum for $name {
            const PG_TYPE: &'static str = $pg;

            fn db_label(self) -> &'static str {
                match self {
                    $(Self::$variant => $label),+
                }
            }

            fn from_db_label(label: &str) -> Option<Self> {
                match label {
                    $($label => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
        }
    };
}

db_enum!(
    /// `agent.status`
    AgentStatus, "agentstatus" {
        Draft => "DRAFT",
        Validating => "VALIDATING",
        Active => "ACTIVE",
        Inactive => "INACTIVE",
        Deprecated => "DEPRECATED",
        Retired => "RETIRED",
    }
);

db_enum!(
    /// `model.status`
    ModelStatus, "modelstatus" {
        Active => "ACTIVE",
        Inactive => "INACTIVE",
        Deprecated => "DEPRECATED",
    }
);

db_enum!(
    /// `agent_interaction.status`
    InteractionStatus, "interactionstatus" {
        InProgress => "IN_PROGRESS",
        Completed => "COMPLETED",
        Failed => "FAILED",
    }
);

db_enum!(
    /// `agent_run.status`
    RunStatus, "runstatus" {
        Running => "RUNNING",
        Interrupted => "INTERRUPTED",
        Completed => "COMPLETED",
        Failed => "FAILED",
    }
);

db_enum!(
    /// `agent_step.status`
    StepStatus, "stepstatus" {
        Pending => "PENDING",
        Running => "RUNNING",
        Completed => "COMPLETED",
        Failed => "FAILED",
        Interrupted => "INTERRUPTED",
        Resumed => "RESUMED",
    }
);

db_enum!(
    /// `agent_message.role`
    MessageRole, "agentmessagerole" {
        Human => "HUMAN",
        Ai => "AI",
        System => "SYSTEM",
        Tool => "TOOL",
    }
);

db_enum!(
    /// `agent_message.type`
    MessageType, "agentmessagetype" {
        Message => "MESSAGE",
        ToolResult => "TOOL_RESULT",
        Action => "ACTION",
        ActionResult => "ACTION_RESULT",
        Step => "STEP",
    }
);

db_enum!(
    /// `agent_message.phase`
    MessagePhase, "agentmessagephase" {
        Final => "FINAL",
        Thinking => "THINKING",
    }
);

db_enum!(
    /// `agent_action.type`
    ActionType, "actiontype" {
        Confirmation => "CONFIRMATION",
        Form => "FORM",
        Approval => "APPROVAL",
        Question => "QUESTION",
        Choice => "CHOICE",
        ToolApproval => "TOOL_APPROVAL",
        ClientTool => "CLIENT_TOOL",
    }
);

db_enum!(
    /// `agent_action.status`
    ActionStatus, "actionstatus" {
        Pending => "PENDING",
        Resolved => "RESOLVED",
        Expired => "EXPIRED",
    }
);

impl ActionType {
    /// Port of Python `coerce_persisted_action_type`: wire values (`"choice"`,
    /// `"CHOICE"`, `"selection"`) and Beam kinds (`"beam.confirmation"`) map to a
    /// stored type; empty means confirmation.
    #[must_use]
    pub fn coerce(raw: Option<&str>) -> Option<Self> {
        let raw = raw.map(str::trim).unwrap_or_default();
        if raw.is_empty() {
            return Some(Self::Confirmation);
        }
        let name = raw.rsplit('.').next().unwrap_or(raw).to_ascii_lowercase();
        match name.as_str() {
            "confirmation" => Some(Self::Confirmation),
            "form" => Some(Self::Form),
            "approval" => Some(Self::Approval),
            "question" => Some(Self::Question),
            "choice" | "selection" => Some(Self::Choice),
            "tool_approval" => Some(Self::ToolApproval),
            "client_tool" => Some(Self::ClientTool),
            _ => None,
        }
    }
}

impl MessageRole {
    /// From the wire role (`beam_types::AgentMessageRole`).
    #[must_use]
    pub fn from_wire(role: beam_types::AgentMessageRole) -> Self {
        match role {
            beam_types::AgentMessageRole::Human => Self::Human,
            beam_types::AgentMessageRole::Ai => Self::Ai,
            beam_types::AgentMessageRole::System => Self::System,
            beam_types::AgentMessageRole::Tool => Self::Tool,
        }
    }
}

/// `agent` catalog row (Python `agents/stores/sql.py` `_AGENT_COLUMNS`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentRow {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub supported_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
    pub tags: Vec<String>,
    pub version: Option<String>,
    pub status: AgentStatus,
}

impl AgentRow {
    /// Catalog row for a registered agent, checked in as `ACTIVE`.
    #[must_use]
    pub fn active(metadata: &beam_types::AgentMetadata) -> Self {
        let modalities = |m: &[beam_types::Modality]| {
            m.iter()
                .map(|x| {
                    serde_json::to_value(x)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_default()
                })
                .collect()
        };
        Self {
            id: crate::agent_id_for(&metadata.name),
            name: metadata.name.clone(),
            description: Some(metadata.description.clone()),
            supported_modalities: modalities(&metadata.supported_modalities),
            output_modalities: modalities(&metadata.output_modalities),
            tags: metadata.tags.clone(),
            version: Some(metadata.version.clone()),
            status: AgentStatus::Active,
        }
    }
}

/// `agent_usage_plan` catalog row. Profiles reference
/// it by `plan_id = UUID_GENF(name)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanRow {
    pub id: Uuid,
    pub name: String,
    /// `hard_block`, `soft_warn` or `throttle`.
    pub overage_policy: String,
    /// `active` on check-in, `inactive` on check-out.
    pub status: String,
    pub max_requests_per_day: Option<i32>,
    pub max_tokens_per_day: Option<i32>,
    pub max_tokens_per_request: Option<i32>,
    pub allowed_agents: Option<Vec<String>>,
    /// Lower-case modality values (`text`, `image`, …).
    pub allowed_modalities: Option<Vec<String>>,
    pub allowed_models: Option<Vec<String>>,
}

/// `model` catalog row (Python `_MODEL_COLUMNS`; secrets are never stored).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelRow {
    pub id: Uuid,
    pub name: String,
    pub provider: String,
    pub modalities: Vec<String>,
    pub capabilities: Vec<String>,
    pub cost_per_1k_tokens: f64,
    pub max_tokens: i32,
    pub provider_config: Value,
    pub extra: Value,
    pub status: ModelStatus,
}

impl ModelRow {
    /// Catalog row for a routable model, checked in as `ACTIVE`. `api_key` and
    /// `credential_ref` are dropped, as in Python.
    #[must_use]
    pub fn active(model: &beam_types::ModelConfig) -> Self {
        let text = |v: serde_json::Result<Value>| {
            v.ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default()
        };
        let object = |v: &Value| {
            if v.is_object() {
                v.clone()
            } else {
                Value::Object(serde_json::Map::new())
            }
        };
        Self {
            id: crate::agent_id_for(&model.name),
            name: model.name.clone(),
            provider: text(serde_json::to_value(model.provider)),
            modalities: model
                .modalities
                .iter()
                .map(|m| text(serde_json::to_value(m)))
                .collect(),
            capabilities: model.capabilities.clone(),
            cost_per_1k_tokens: model.cost_per_1k_tokens,
            max_tokens: i32::try_from(model.max_tokens).unwrap_or(i32::MAX),
            provider_config: object(&model.provider_config),
            extra: object(&model.extra),
            status: ModelStatus::Active,
        }
    }
}

/// `agent_session` row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionRow {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub user_id: Option<String>,
    pub profile_id: Option<String>,
    pub org_id: Option<String>,
    pub name: Option<String>,
    pub last_active_at: Option<DateTime<Utc>>,
}

/// `agent_interaction` row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InteractionRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub execution_id: Option<Uuid>,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub status: InteractionStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

/// `agent_run` row (its lease and retry columns are the execution records', in `beam_execution`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub interaction_id: Uuid,
    pub execution_id: Option<Uuid>,
    pub attempt_no: i32,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub org_id: Option<String>,
}

/// `agent_step` row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StepRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub interaction_id: Uuid,
    pub run_id: Uuid,
    pub agent_id: Uuid,
    pub status: StepStatus,
    pub lc_step_id: Option<String>,
    pub name: String,
    pub input: Value,
    pub result: Option<Value>,
}

/// `agent_message` row (a persisted transcript turn).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageRow {
    pub id: Uuid,
    pub sequence: i32,
    pub session_id: Uuid,
    pub execution_id: Option<Uuid>,
    pub interaction_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub agent_id: Uuid,
    pub step_id: Option<Uuid>,
    pub role: MessageRole,
    #[serde(rename = "type")]
    pub msg_type: MessageType,
    pub phase: MessagePhase,
    /// JSON array of content parts.
    pub content: Value,
    pub name: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
    pub tool_args: Option<Value>,
    pub tool_result: Option<Value>,
    /// JSON array.
    pub tool_calls: Value,
    pub metadata: Value,
    pub request_id: Option<Uuid>,
    pub org_id: Option<String>,
    pub timestamp: DateTime<Utc>,
}

impl MessageRow {
    /// A final-phase message with empty tool fields.
    #[must_use]
    pub fn new(
        session_id: Uuid,
        agent_id: Uuid,
        sequence: i32,
        role: MessageRole,
        msg_type: MessageType,
        content: Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            sequence,
            session_id,
            execution_id: None,
            interaction_id: None,
            run_id: None,
            agent_id,
            step_id: None,
            role,
            msg_type,
            phase: MessagePhase::Final,
            content,
            name: None,
            tool_call_id: None,
            tool_name: None,
            tool_args: None,
            tool_result: None,
            tool_calls: Value::Array(Vec::new()),
            metadata: Value::Object(serde_json::Map::new()),
            request_id: None,
            org_id: None,
            timestamp: Utc::now(),
        }
    }
}

/// `agent_action` row (a HITL interrupt).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionRow {
    pub id: Uuid,
    pub session_id: Uuid,
    pub execution_id: Option<Uuid>,
    pub interaction_id: Uuid,
    pub run_id: Uuid,
    pub step_id: Option<Uuid>,
    pub message_id: Option<Uuid>,
    pub agent_id: Uuid,
    pub action_type: ActionType,
    pub status: ActionStatus,
    pub lc_action_id: Option<String>,
    pub payload: Value,
    pub result: Option<Value>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub org_id: Option<String>,
    /// The tool a `TOOL_APPROVAL` or `CLIENT_TOOL` action is for (Python rc8).
    pub tool_name: Option<String>,
    /// When a `CLIENT_TOOL` action stops waiting for its client.
    pub expires_at: Option<DateTime<Utc>>,
    /// A failed expiry sweep's next try.
    pub sweep_after: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every label Rust writes exists in the golden enum type, and every
    /// upper-case (member-name) golden label has a variant. Python's
    /// `add_revision_columns` also adds lower-case `agentstatus` labels that
    /// Python itself never writes; those are ignored.
    #[test]
    fn labels_match_golden_schema() {
        fn check<E: DbEnum + std::fmt::Debug>(golden: &str, all: &[E]) {
            let head = format!("CREATE TYPE ref_beam.{} AS ENUM (", E::PG_TYPE);
            let start = golden
                .find(&head)
                .unwrap_or_else(|| panic!("{} missing", E::PG_TYPE));
            let body = &golden[start + head.len()..];
            let body = &body[..body.find(");").unwrap()];
            let labels: Vec<&str> = body
                .split(',')
                .map(|l| l.trim().trim_matches('\''))
                .collect();
            let ours: Vec<&str> = all.iter().copied().map(DbEnum::db_label).collect();
            let member_names: Vec<&str> = labels
                .iter()
                .copied()
                .filter(|l| *l == l.to_ascii_uppercase())
                .collect();
            assert_eq!(ours, member_names, "{}", E::PG_TYPE);
            for v in all {
                assert_eq!(
                    E::from_db_label(v.db_label()).map(DbEnum::db_label),
                    Some(v.db_label())
                );
            }
        }
        let golden = include_str!("../../../tests/schema/python_ref_beam.sql");
        check(golden, AgentStatus::ALL);
        check(golden, ModelStatus::ALL);
        check(golden, InteractionStatus::ALL);
        check(golden, RunStatus::ALL);
        check(golden, StepStatus::ALL);
        check(golden, MessageRole::ALL);
        check(golden, MessageType::ALL);
        check(golden, MessagePhase::ALL);
        check(golden, ActionType::ALL);
        check(golden, ActionStatus::ALL);
    }

    #[test]
    fn action_type_coercion_matches_python() {
        assert_eq!(ActionType::coerce(None), Some(ActionType::Confirmation));
        assert_eq!(
            ActionType::coerce(Some("  ")),
            Some(ActionType::Confirmation)
        );
        assert_eq!(
            ActionType::coerce(Some("selection")),
            Some(ActionType::Choice)
        );
        assert_eq!(ActionType::coerce(Some("CHOICE")), Some(ActionType::Choice));
        assert_eq!(
            ActionType::coerce(Some("beam.approval")),
            Some(ActionType::Approval)
        );
        assert_eq!(
            ActionType::coerce(Some("beam.client_tool")),
            Some(ActionType::ClientTool)
        );
        assert_eq!(
            ActionType::coerce(Some("tool_approval")),
            Some(ActionType::ToolApproval)
        );
        assert_eq!(ActionType::coerce(Some("teleport")), None);
    }
}

/// `agent_context_manifest` row: the
/// privacy-safe manifest view, keyed by `operation_id`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManifestRow {
    pub operation_id: String,
    pub execution_id: Option<Uuid>,
    pub operation_kind: String,
    pub revision_id: Option<String>,
    pub policy_revision: Option<String>,
    pub serializer_revision: Option<String>,
    pub tokenizer_revision: Option<String>,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub reserve_tokens: i32,
    pub actual_prompt_tokens: Option<i32>,
    pub estimate_drift: Option<f64>,
    /// JSON array of decisions.
    pub decisions: Value,
    /// `inspect_manifest` view.
    pub view: Value,
}
