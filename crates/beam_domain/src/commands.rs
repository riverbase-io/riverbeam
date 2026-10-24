//! The beam commands: one riverbase `command_engine!`, each
//! command with its meta, its typed payload and a handler that calls one
//! [`BeamAggregate`] method. Mounted as
//! `POST /api/{namespace}/{key}:exec/{resource}/{id}` (and `:post` for
//! `create-agent-profile`).
//!
//! What each command does lives in its aggregate method
//! (`crate::aggregate::*`):
//!
//! - **Runs** (`runs.rs`): `invoke`, `stream`, `resume` record the session
//!   and queue an `invoke_agent` task; `cancel-execution`, `retry-execution`.
//! - **Sessions** (`sessions.rs`) and **profiles** (`profiles.rs`): SQL on
//!   the command's transaction.
//! - **Memory**, **knowledge**, **revisions**, **operations**: check the
//!   caller's ownership, then queue a worker task.
//!
//! No command calls a worker or waits for one.

use std::sync::Arc;

use async_trait::async_trait;
use riverbase_core::base::Engine;
use riverbase_core::command::{
    BatchExecuteResult, CommandMeta, CommandTarget, MessageBus, PreparedCommand,
};
use riverbase_core::datastore::PgDataStore;
use riverbase_core::{DomainCommandEngine, EngineContext, RiverbaseResult};
use serde_json::Value;

use crate::aggregate::BeamAggregate;

/// The resources beam commands target.
pub const AGENT: &str = "agent";
pub const AGENT_SESSION: &str = "agent_session";
pub const AGENT_MEMORY: &str = "agent_memory";
pub const AGENT_KNOWLEDGE_DOCUMENT: &str = "agent_knowledge_document";
pub const AGENT_PROFILE: &str = "agent_profile";
pub const AGENT_USAGE_PLAN: &str = "agent_usage_plan";
pub const AGENT_EVALUATION_DATASET: &str = "agent_evaluation_dataset";

/// The bus the command outbox publishes queue hints to.
pub type BeamMessageBus = Arc<dyn MessageBus>;

riverbase_core::command_engine! {
    engine BeamCommandEngine {
        domain_trait: manual,
        meta {
            actor_pool_size: 8,
        }
        store: PgDataStore,
        aggregate: BeamAggregate,
        component: {
            msgbus: BeamMessageBus,
        },
    }
    commands {
        // --- agent runs ---------------------------------------------------------

        command Invoke {
            meta {
                key: "invoke",
                title: "Invoke agent",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "Message parts, e.g. [{\"type\": \"text\", \"text\": \"hello\"}].")]
                #[schemars(with = "Vec<serde_json::Value>")]
                #[serde(default)]
                content: Vec<beam_types::ContentPart>,
                #[schemars(description = "The run's session; created for the caller when new.")]
                session_id: uuid::Uuid,
                #[serde(default)]
                model_override: Option<String>,
                #[serde(default)]
                metadata: Option<serde_json::Value>,
                #[schemars(description = "The client's id for this request; assigned when absent.")]
                #[serde(default)]
                request_id: Option<uuid::Uuid>,
                #[schemars(description = "A repeat with the same key (per user and agent) returns the first run.")]
                #[serde(default)]
                idempotency_key: Option<String>,
                #[schemars(with = "Option<serde_json::Value>")]
                #[serde(default)]
                context: Option<beam_types::RequestContext>,
                #[schemars(description = "What the calling client can run now: {\"client_id\", \"kind\", \"tools\"}. Client tools it does not list are hidden from the agent.")]
                #[schemars(with = "Option<serde_json::Value>")]
                #[serde(default)]
                client: Option<beam_types::ClientCapabilities>,
                #[schemars(description = "Answers to an interrupted run's pending actions (Python rc8): with any, this run resumes that run, as the resume command does.")]
                #[schemars(with = "Option<Vec<serde_json::Value>>")]
                #[serde(default)]
                actions: Option<Vec<beam_types::ActionResponseEnvelope>>,
                #[schemars(description = "The interrupted interaction the actions answer; found from the actions when absent. Only with actions.")]
                #[serde(default)]
                interaction_id: Option<uuid::Uuid>,
                #[schemars(description = "The interrupted execution; it must be the interaction's. Only with actions.")]
                #[serde(default)]
                execution_id: Option<uuid::Uuid>,
                #[schemars(description = "Ignored: every queued run streams its frames.")]
                #[serde(default)]
                stream: Option<bool>,
            }
            handle (payload, aggregate) {
                aggregate.invoke(payload).await
            }
        }

        command Stream {
            meta {
                key: "stream",
                title: "Stream agent",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            // The same fields as `invoke` (plan 021 D6; Python rc8 shares one
            // `InvokeRequest`); `commands::tests` checks their schemas agree.
            payload {
                #[schemars(description = "Message parts, e.g. [{\"type\": \"text\", \"text\": \"hello\"}].")]
                #[schemars(with = "Vec<serde_json::Value>")]
                #[serde(default)]
                content: Vec<beam_types::ContentPart>,
                #[schemars(description = "The run's session; created for the caller when new.")]
                session_id: uuid::Uuid,
                #[serde(default)]
                model_override: Option<String>,
                #[serde(default)]
                metadata: Option<serde_json::Value>,
                #[schemars(description = "The client's id for this request; assigned when absent.")]
                #[serde(default)]
                request_id: Option<uuid::Uuid>,
                #[schemars(description = "A repeat with the same key (per user and agent) returns the first run.")]
                #[serde(default)]
                idempotency_key: Option<String>,
                #[schemars(with = "Option<serde_json::Value>")]
                #[serde(default)]
                context: Option<beam_types::RequestContext>,
                #[schemars(description = "What the calling client can run now: {\"client_id\", \"kind\", \"tools\"}. Client tools it does not list are hidden from the agent.")]
                #[schemars(with = "Option<serde_json::Value>")]
                #[serde(default)]
                client: Option<beam_types::ClientCapabilities>,
                #[schemars(description = "Answers to an interrupted run's pending actions (Python rc8): with any, this run resumes that run, as the resume command does.")]
                #[schemars(with = "Option<Vec<serde_json::Value>>")]
                #[serde(default)]
                actions: Option<Vec<beam_types::ActionResponseEnvelope>>,
                #[schemars(description = "The interrupted interaction the actions answer; found from the actions when absent. Only with actions.")]
                #[serde(default)]
                interaction_id: Option<uuid::Uuid>,
                #[schemars(description = "The interrupted execution; it must be the interaction's. Only with actions.")]
                #[serde(default)]
                execution_id: Option<uuid::Uuid>,
                #[schemars(description = "Ignored: every queued run streams its frames.")]
                #[serde(default)]
                stream: Option<bool>,
            }
            handle (payload, aggregate) {
                aggregate.stream(payload).await
            }
        }

        command Resume {
            meta {
                key: "resume",
                title: "Resume agent",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                session_id: uuid::Uuid,
                #[schemars(description = "The interrupted interaction; its execution must be waiting for input.")]
                interaction_id: uuid::Uuid,
                #[schemars(with = "Vec<serde_json::Value>")]
                #[serde(default)]
                actions: Vec<beam_types::ActionResponseEnvelope>,
                #[schemars(with = "Vec<serde_json::Value>")]
                #[serde(default)]
                content: Vec<beam_types::ContentPart>,
                #[serde(default)]
                model_override: Option<String>,
                #[serde(default)]
                metadata: Option<serde_json::Value>,
                #[serde(default)]
                request_id: Option<uuid::Uuid>,
                #[schemars(with = "Option<serde_json::Value>")]
                #[serde(default)]
                context: Option<beam_types::RequestContext>,
                #[schemars(description = "What the calling client can run now: {\"client_id\", \"kind\", \"tools\"}. Client tools it does not list are hidden from the agent.")]
                #[schemars(with = "Option<serde_json::Value>")]
                #[serde(default)]
                client: Option<beam_types::ClientCapabilities>,
            }
            handle (payload, aggregate) {
                aggregate.resume(payload).await
            }
        }

        command CancelExecution {
            meta {
                key: "cancel-execution",
                title: "Cancel agent execution",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                execution_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.cancel_execution(payload).await
            }
        }

        command RetryExecution {
            meta {
                key: "retry-execution",
                title: "Retry agent execution",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                execution_id: uuid::Uuid,
                #[serde(default)]
                reason: Option<String>,
            }
            handle (payload, aggregate) {
                aggregate.retry_execution(payload).await
            }
        }

        // --- sessions -------------------------------------------------------------

        command UpdateSession {
            meta {
                key: "update-session",
                title: "Update session",
                kind: object,
                resources: [AGENT_SESSION],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                name: String,
            }
            handle (payload, aggregate) {
                aggregate.update_session(payload).await
            }
        }

        command RemoveSession {
            meta {
                key: "remove-session",
                title: "Remove session",
                kind: object,
                resources: [AGENT_SESSION],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {}
            handle (_payload, aggregate) {
                aggregate.remove_session().await
            }
        }

        command RemoveMessage {
            meta {
                key: "remove-message",
                title: "Remove message",
                kind: object,
                resources: [AGENT_SESSION],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                message_id: String,
            }
            handle (payload, aggregate) {
                aggregate.remove_message(payload).await
            }
        }

        // --- long-term memory -------------------------------------------------------

        command RememberMemory {
            meta {
                key: "remember-memory",
                title: "Remember long-term memory",
                kind: object,
                resources: [AGENT_MEMORY],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "Must be the caller (admins may name anyone).")]
                user_id: String,
                content: String,
                #[serde(default, rename = "type")]
                memory_type: Option<String>,
                #[serde(default)]
                agent_id: Option<String>,
                #[serde(default)]
                importance: Option<i64>,
                #[serde(default)]
                metadata: Option<serde_json::Value>,
            }
            handle (payload, aggregate) {
                aggregate.remember_memory(payload).await
            }
        }

        command ForgetMemory {
            meta {
                key: "forget-memory",
                title: "Forget long-term memory",
                kind: object,
                resources: [AGENT_MEMORY],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                memory_id: String,
            }
            handle (payload, aggregate) {
                aggregate.forget_memory(payload).await
            }
        }

        command EditMemory {
            meta {
                key: "edit-memory",
                title: "Edit long-term memory",
                kind: object,
                resources: [AGENT_MEMORY],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                memory_id: String,
                #[serde(default)]
                content: Option<String>,
            }
            handle (payload, aggregate) {
                aggregate.edit_memory(payload).await
            }
        }

        command ExpireMemory {
            meta {
                key: "expire-memory",
                title: "Expire long-term memory",
                kind: object,
                resources: [AGENT_MEMORY],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                memory_id: String,
                #[schemars(description = "RFC 3339 time.")]
                expires_at: String,
            }
            handle (payload, aggregate) {
                aggregate.expire_memory(payload).await
            }
        }

        // --- revisions, evals and feedback ----------------------------------------

        command SubmitAgentFeedback {
            meta {
                key: "submit-agent-feedback",
                title: "Submit agent feedback",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                execution_id: uuid::Uuid,
                #[serde(default)]
                thumb: Option<String>,
                #[serde(default)]
                rating: Option<i64>,
                #[serde(default)]
                comment: Option<String>,
            }
            handle (payload, aggregate) {
                aggregate.submit_agent_feedback(payload).await
            }
        }

        command RunEvaluation {
            meta {
                key: "run-evaluation",
                title: "Run offline evaluation",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                dataset_id: uuid::Uuid,
                agent_revision_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.run_evaluation(payload).await
            }
        }

        command MarkRevisionValidating {
            meta {
                key: "mark-revision-validating",
                title: "Mark agent revision validating",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                revision_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.mark_revision_validating(payload).await
            }
        }

        command ActivateRevision {
            meta {
                key: "activate-revision",
                title: "Activate agent revision",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                revision_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.activate_revision(payload).await
            }
        }

        command RetireRevision {
            meta {
                key: "retire-revision",
                title: "Retire agent revision",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                revision_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.retire_revision(payload).await
            }
        }

        // --- operations ----------------------------------------------------------

        command ReconcileNow {
            meta {
                key: "reconcile-now",
                title: "Reconcile executions and outbox",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {}
            handle (_payload, aggregate) {
                aggregate.reconcile_now().await
            }
        }

        command RetryDeadLetter {
            meta {
                key: "retry-dead-letter",
                title: "Retry dead letter",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                letter_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.retry_dead_letter(payload).await
            }
        }

        command DiscardDeadLetter {
            meta {
                key: "discard-dead-letter",
                title: "Discard dead letter",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                letter_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.discard_dead_letter(payload).await
            }
        }

        command DeleteUserAgentData {
            meta {
                key: "delete-user-agent-data",
                title: "Delete user agent data",
                kind: object,
                resources: [AGENT_SESSION],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                user_id: String,
            }
            handle (payload, aggregate) {
                aggregate.delete_user_agent_data(payload).await
            }
        }

        command DeleteMyAgentData {
            meta {
                key: "delete-my-agent-data",
                title: "Delete my agent data",
                kind: object,
                resources: [AGENT_SESSION],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {}
            handle (_payload, aggregate) {
                aggregate.delete_my_agent_data().await
            }
        }

        command PlaceLegalHold {
            meta {
                key: "place-legal-hold",
                title: "Place legal hold",
                kind: object,
                resources: [AGENT_SESSION],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "The user whose agent data may not be deleted until the hold is released.")]
                user_id: String,
                #[serde(default)]
                reason: Option<String>,
            }
            handle (payload, aggregate) {
                aggregate.place_legal_hold(payload).await
            }
        }

        command ReleaseLegalHold {
            meta {
                key: "release-legal-hold",
                title: "Release legal hold",
                kind: object,
                resources: [AGENT_SESSION],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                user_id: String,
            }
            handle (payload, aggregate) {
                aggregate.release_legal_hold(payload).await
            }
        }

        // --- knowledge -----------------------------------------------------------

        command IngestKnowledge {
            meta {
                key: "ingest-knowledge",
                title: "Ingest knowledge document",
                kind: object,
                resources: [AGENT_KNOWLEDGE_DOCUMENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                title: String,
                #[serde(default)]
                scope: Option<String>,
                #[serde(default, rename = "type")]
                document_type: Option<String>,
                #[serde(default)]
                agent_id: Option<String>,
                #[serde(default)]
                user_id: Option<String>,
                #[schemars(description = "Assigned when absent.")]
                #[serde(default)]
                document_id: Option<String>,
                #[serde(default)]
                text: Option<String>,
                #[serde(default)]
                media_id: Option<String>,
                #[serde(default)]
                source_url: Option<String>,
                #[serde(default)]
                filename: Option<String>,
                #[serde(default)]
                mime_type: Option<String>,
                #[serde(default)]
                metadata: Option<serde_json::Value>,
            }
            handle (payload, aggregate) {
                aggregate.ingest_knowledge(payload).await
            }
        }

        command ReindexKnowledge {
            meta {
                key: "reindex-knowledge",
                title: "Reindex knowledge document",
                kind: object,
                resources: [AGENT_KNOWLEDGE_DOCUMENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                title: String,
                #[schemars(description = "The document to reindex.")]
                document_id: String,
                #[serde(default)]
                scope: Option<String>,
                #[serde(default, rename = "type")]
                document_type: Option<String>,
                #[serde(default)]
                agent_id: Option<String>,
                #[serde(default)]
                user_id: Option<String>,
                #[serde(default)]
                text: Option<String>,
                #[serde(default)]
                media_id: Option<String>,
                #[serde(default)]
                source_url: Option<String>,
                #[serde(default)]
                filename: Option<String>,
                #[serde(default)]
                mime_type: Option<String>,
                #[serde(default)]
                metadata: Option<serde_json::Value>,
            }
            handle (payload, aggregate) {
                aggregate.reindex_knowledge(payload).await
            }
        }

        command DeleteKnowledge {
            meta {
                key: "delete-knowledge",
                title: "Delete knowledge document",
                kind: object,
                resources: [AGENT_KNOWLEDGE_DOCUMENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                document_id: String,
            }
            handle (payload, aggregate) {
                aggregate.delete_knowledge(payload).await
            }
        }

        // --- usage plans and evaluation datasets (rc8; plan 023 D9) ----------------

        command AssignUsagePlan {
            meta {
                key: "assign-usage-plan",
                title: "Assign usage plan",
                kind: object,
                resources: [AGENT_USAGE_PLAN],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "The user who gets the plan in the URL; their other plans are deactivated.")]
                user_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.assign_usage_plan(payload).await
            }
        }

        command CreateEvaluationDataset {
            meta {
                key: "create-evaluation-dataset",
                title: "Create evaluation dataset",
                kind: collection,
                resources: [AGENT_EVALUATION_DATASET],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                name: String,
                #[serde(default)]
                metadata_json: Option<serde_json::Value>,
            }
            target collection(AGENT_EVALUATION_DATASET);
            handle (payload, aggregate) {
                aggregate.create_evaluation_dataset(payload).await
            }
        }

        command AddEvaluationCase {
            meta {
                key: "add-evaluation-case",
                title: "Add evaluation case",
                kind: collection,
                resources: [AGENT_EVALUATION_DATASET],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "The dataset the case joins.")]
                dataset_id: uuid::Uuid,
                #[schemars(description = "The case's agent input (Python `AgentInput`).")]
                #[schemars(with = "serde_json::Value")]
                input: beam_types::AgentInput,
                #[serde(default)]
                expected: Option<serde_json::Value>,
            }
            target collection(AGENT_EVALUATION_DATASET);
            handle (payload, aggregate) {
                aggregate.add_evaluation_case(payload).await
            }
        }

        // --- tool rules and grants (rc8; plan 023 part C) ------------------------

        command SetMyToolRule {
            meta {
                key: "set-my-tool-rule",
                title: "Set my tool rule",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "Which tools: name (exact or glob), source, min_risk, side_effect, category. A misspelled key is refused.")]
                #[schemars(with = "serde_json::Value")]
                #[serde(rename = "match")]
                matcher: beam_types::tool_policy::ToolMatch,
                #[schemars(description = "auto | ask | deny")]
                #[schemars(with = "String")]
                mode: beam_types::tool_policy::ToolMode,
                #[schemars(description = "Tenant rules only: lower layers may not loosen this rule.")]
                #[serde(default)]
                locked: bool,
            }
            handle (payload, aggregate) {
                aggregate.set_my_tool_rule(payload).await
            }
        }

        command RemoveMyToolRule {
            meta {
                key: "remove-my-tool-rule",
                title: "Remove my tool rule",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                rule_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.remove_my_tool_rule(payload).await
            }
        }

        command RevokeToolGrants {
            meta {
                key: "revoke-tool-grants",
                title: "Revoke allow-for-this-conversation grants",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "The caller's own session.")]
                session_id: uuid::Uuid,
                #[schemars(description = "One tool's grant; every grant of the session when absent.")]
                #[serde(default)]
                tool_name: Option<String>,
            }
            handle (payload, aggregate) {
                aggregate.revoke_tool_grants(payload).await
            }
        }

        command SetTenantToolRule {
            meta {
                key: "set-tenant-tool-rule",
                title: "Set a tenant tool rule",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "Which tools: name (exact or glob), source, min_risk, side_effect, category. A misspelled key is refused.")]
                #[schemars(with = "serde_json::Value")]
                #[serde(rename = "match")]
                matcher: beam_types::tool_policy::ToolMatch,
                #[schemars(description = "auto | ask | deny")]
                #[schemars(with = "String")]
                mode: beam_types::tool_policy::ToolMode,
                #[schemars(description = "Tenant rules only: lower layers may not loosen this rule.")]
                #[serde(default)]
                locked: bool,
            }
            handle (payload, aggregate) {
                aggregate.set_tenant_tool_rule(payload).await
            }
        }

        command RemoveTenantToolRule {
            meta {
                key: "remove-tenant-tool-rule",
                title: "Remove a tenant tool rule",
                kind: object,
                resources: [AGENT],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                rule_id: uuid::Uuid,
            }
            handle (payload, aggregate) {
                aggregate.remove_tenant_tool_rule(payload).await
            }
        }

        // --- agent profiles ------------------------------------------------------

        command CreateAgentProfile {
            meta {
                key: "create-agent-profile",
                title: "Create agent profile",
                kind: collection,
                resources: [AGENT_PROFILE],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[schemars(description = "Must be the caller when given.")]
                #[serde(default)]
                user_id: Option<String>,
                #[schemars(description = "Must be the caller's profile when given.")]
                #[serde(default)]
                profile_id: Option<String>,
                #[serde(default)]
                role: Option<String>,
                #[serde(default)]
                rules: Option<serde_json::Value>,
                #[serde(default)]
                metadata_json: Option<serde_json::Value>,
            }
            target collection(AGENT_PROFILE);
            handle (payload, aggregate) {
                aggregate.create_agent_profile(payload).await
            }
        }

        command UpdateAgentProfile {
            meta {
                key: "update-agent-profile",
                title: "Update agent profile",
                kind: object,
                resources: [AGENT_PROFILE],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {
                #[serde(default)]
                role: Option<String>,
                #[serde(default)]
                rules: Option<serde_json::Value>,
                #[serde(default)]
                metadata_json: Option<serde_json::Value>,
            }
            handle (payload, aggregate) {
                aggregate.update_agent_profile(payload).await
            }
        }

        command RemoveAgentProfile {
            meta {
                key: "remove-agent-profile",
                title: "Remove agent profile",
                kind: object,
                resources: [AGENT_PROFILE],
                tenant_scope: exempt("beam rows have no _tenant; each command checks the caller's rows", "BEAM-PLAN-007"),
            }
            payload {}
            handle (_payload, aggregate) {
                aggregate.remove_agent_profile().await
            }
        }
    }
    methods ()
}

/// Riverbase's engine, with Python's public error codes for callers.
#[async_trait]
impl DomainCommandEngine for BeamCommandEngine {
    fn context(&self) -> &EngineContext {
        &self.ctx
    }

    async fn commands(&self) -> RiverbaseResult<Vec<String>> {
        Engine::items(&self.engine).await
    }

    fn command_meta(&self, cmdkey: &str) -> Option<CommandMeta> {
        self.engine.command_meta(cmdkey)
    }

    fn command_info(&self, cmdkey: &str) -> Option<Value> {
        self.engine.command_info(cmdkey)
    }

    async fn execute(
        &self,
        ctx: &EngineContext,
        cmdkey: &str,
        payload: Value,
        target: CommandTarget,
    ) -> RiverbaseResult<Value> {
        self.engine
            .execute(ctx, cmdkey, payload, target)
            .await
            // Callers see the public codes; the site code stays in `errdata`.
            .map_err(beam_core::public_errors::to_public)
    }

    async fn execute_prepared_batch(
        &self,
        ctx: &EngineContext,
        items: Vec<PreparedCommand>,
    ) -> RiverbaseResult<BatchExecuteResult> {
        self.engine.execute_prepared_batch(ctx, items).await
    }
}

/// Every command key with its resource, for the Casbin policy and tests.
pub const COMMAND_KEYS: [(&str, &str); 38] = [
    ("invoke", AGENT),
    ("stream", AGENT),
    ("resume", AGENT),
    ("cancel-execution", AGENT),
    ("retry-execution", AGENT),
    ("update-session", AGENT_SESSION),
    ("remove-session", AGENT_SESSION),
    ("remove-message", AGENT_SESSION),
    ("remember-memory", AGENT_MEMORY),
    ("forget-memory", AGENT_MEMORY),
    ("edit-memory", AGENT_MEMORY),
    ("expire-memory", AGENT_MEMORY),
    ("submit-agent-feedback", AGENT),
    ("run-evaluation", AGENT),
    ("mark-revision-validating", AGENT),
    ("activate-revision", AGENT),
    ("retire-revision", AGENT),
    ("reconcile-now", AGENT),
    ("retry-dead-letter", AGENT),
    ("discard-dead-letter", AGENT),
    ("delete-user-agent-data", AGENT_SESSION),
    ("delete-my-agent-data", AGENT_SESSION),
    ("place-legal-hold", AGENT_SESSION),
    ("release-legal-hold", AGENT_SESSION),
    ("ingest-knowledge", AGENT_KNOWLEDGE_DOCUMENT),
    ("reindex-knowledge", AGENT_KNOWLEDGE_DOCUMENT),
    ("delete-knowledge", AGENT_KNOWLEDGE_DOCUMENT),
    ("set-my-tool-rule", AGENT),
    ("remove-my-tool-rule", AGENT),
    ("revoke-tool-grants", AGENT),
    ("set-tenant-tool-rule", AGENT),
    ("remove-tenant-tool-rule", AGENT),
    ("create-agent-profile", AGENT_PROFILE),
    ("update-agent-profile", AGENT_PROFILE),
    ("remove-agent-profile", AGENT_PROFILE),
    ("assign-usage-plan", AGENT_USAGE_PLAN),
    ("create-evaluation-dataset", AGENT_EVALUATION_DATASET),
    ("add-evaluation-case", AGENT_EVALUATION_DATASET),
];

/// The resource command `key` acts on.
#[must_use]
pub fn command_resource(key: &str) -> Option<&'static str> {
    COMMAND_KEYS
        .iter()
        .find(|(k, _)| *k == key)
        .map(|(_, r)| *r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riverbase_core::command::TypedCommandHandler;

    #[test]
    fn thirty_eight_unique_commands() {
        let keys: std::collections::BTreeSet<&str> = COMMAND_KEYS.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys.len(), 38);
        // rc8 dropped `set-canary` and `export-session`.
        for gone in [
            "list-revisions",
            "list-dead-letters",
            "research-references",
            "set-canary",
            "export-session",
        ] {
            assert!(command_resource(gone).is_none(), "{gone}");
        }
    }

    #[test]
    fn handlers_carry_their_meta() {
        let meta = InvokeHandler.meta();
        assert_eq!(meta.key, "invoke");
        assert_eq!(meta.resources, vec![AGENT.to_owned()]);
        assert!(meta.tenant_scope_exempt());
        assert!(
            CreateAgentProfileHandler.meta().resource_init(),
            "create-agent-profile is a collection command"
        );
        // Every handler's meta matches its `COMMAND_KEYS` row.
        macro_rules! metas {
            ($($handler:ident),* $(,)?) => { vec![$($handler.meta()),*] };
        }
        let metas = metas![
            InvokeHandler,
            StreamHandler,
            ResumeHandler,
            CancelExecutionHandler,
            RetryExecutionHandler,
            UpdateSessionHandler,
            RemoveSessionHandler,
            RemoveMessageHandler,
            RememberMemoryHandler,
            ForgetMemoryHandler,
            EditMemoryHandler,
            ExpireMemoryHandler,
            SubmitAgentFeedbackHandler,
            RunEvaluationHandler,
            MarkRevisionValidatingHandler,
            ActivateRevisionHandler,
            RetireRevisionHandler,
            ReconcileNowHandler,
            RetryDeadLetterHandler,
            DiscardDeadLetterHandler,
            DeleteUserAgentDataHandler,
            DeleteMyAgentDataHandler,
            PlaceLegalHoldHandler,
            ReleaseLegalHoldHandler,
            IngestKnowledgeHandler,
            ReindexKnowledgeHandler,
            DeleteKnowledgeHandler,
            SetMyToolRuleHandler,
            RemoveMyToolRuleHandler,
            RevokeToolGrantsHandler,
            SetTenantToolRuleHandler,
            RemoveTenantToolRuleHandler,
            CreateAgentProfileHandler,
            UpdateAgentProfileHandler,
            RemoveAgentProfileHandler,
            AssignUsagePlanHandler,
            CreateEvaluationDatasetHandler,
            AddEvaluationCaseHandler,
        ];
        assert_eq!(metas.len(), COMMAND_KEYS.len());
        for (meta, (key, resource)) in metas.iter().zip(COMMAND_KEYS) {
            assert_eq!(meta.key, key);
            assert_eq!(meta.resources, vec![resource.to_owned()], "{key}");
            assert!(meta.tenant_scope_exempt(), "{key}");
        }
    }

    #[test]
    fn payload_schemas_are_typed() {
        let schema = riverbase_core::command::payload_json_schema::<InvokePayload>();
        assert!(
            schema.pointer("/properties/session_id").is_some(),
            "{schema}"
        );
        let schema = riverbase_core::command::payload_json_schema::<StreamPayload>();
        assert!(schema.pointer("/properties/actions").is_some(), "{schema}");
        let schema = riverbase_core::command::payload_json_schema::<RememberMemoryPayload>();
        assert!(schema.pointer("/properties/type").is_some(), "{schema}");
    }

    /// Plan 021 D6: `stream` takes `invoke`'s fields, and the typed run
    /// fields keep their untyped (`Value`) schema.
    #[test]
    fn stream_and_invoke_share_one_payload_schema() {
        let invoke = riverbase_core::command::payload_json_schema::<InvokePayload>();
        let stream = riverbase_core::command::payload_json_schema::<StreamPayload>();
        assert_eq!(invoke.get("properties"), stream.get("properties"));
        assert_eq!(invoke.get("required"), stream.get("required"));
        let content = invoke.pointer("/properties/content").expect("content");
        assert_eq!(content["type"], "array", "{content}");
        assert!(
            content.pointer("/items/$ref").is_none(),
            "content stays a list of untyped parts: {content}"
        );
    }
}
