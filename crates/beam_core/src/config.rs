//! The beam's settings. Every crate reads its own environment keys through
//! `beam_env`, in a config struct whose `Default` reads nothing
//! and whose `from_env()` reads its keys:
//!
//! | Keys | Owner |
//! |---|---|
//! | `MEMORY_*`, `KNOWLEDGE_ENABLED`, `SECURITY_*`, `BEAM_SECURITY_*`, `AUDIT_*` | [`BeamConfig`] (this file) |
//! | `EXECUTION_TIMEOUT_SECONDS`, `LEASE_*`, `BEAM_WORKER_ID` | `beam_execution::ExecutionConfig` |
//! | `BEAM_STREAM_*` | `beam_stream::StreamConfig` |
//! | `KNOWLEDGE_*` (the store), `BEAM_API_*`, `KNOWLEDGE_EMBED_*` | `beam_knowledge::KnowledgeBuildRequest`, `EmbedBatching` |
//! | `BEAM_WORKER_CONCURRENCY`, `BEAM_WORKER_BATCH_CONCURRENCY`, `BEAM_QUEUE_*`, `BEAM_TASK_*`, `BEAM_REVISION_RELOAD_SECS`, `HOSTNAME` | `beam_worker::QueueConfig` |
//! | `BEAM_RTC_STREAM` | `beam_worker::rtc_stream_enabled` |
//! | `BEAM_MCP_REQUIRE_ALLOWLIST`, `MCP_TOKEN_HEADER` | `beam_tools::McpSettings` |
//! | `MCP_SECRET_KEY`, `MCP_TOKEN_*` | `beam_tools::McpTokenMinter` |
//! | `AUDIO_AI_TIMEOUT_SEC`, `AUDIO_DEFAULT_AGENT_NAME`, `AUDIO_*` backends | `beam_audio::AudioConfig`, its backends |
//! | `NVIDIA_NIM_BASE_URL`, the provider key variables | `beam_gateway` providers |
//! | `BEAM_SQL_SCHEMA` | `beam_store::SchemaName` |
//! | `BEAM_DOMAIN_NAMESPACE` | `beam_domain::beam_namespace` |
//!
//! Read lazily on purpose, each with a `set_*` override: `BEAM_RLS_ENABLED`
//! (`beam_store`), `BEAM_LOG_JSON` and `BEAM_TRACE_PRIVACY`
//! (`beam_telemetry`).

/// Runtime configuration (ports riverbeam `_meta/defaults.py` subset).
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // Feature switches, as in Python's settings.
pub struct BeamConfig {
    pub worker_namespace: String,
    pub knowledge_enabled: bool,
    pub knowledge_embedding_dim: u32,
    pub knowledge_max_context_tokens: u32,
    pub default_model: Option<String>,
    /// `SECURITY_ENABLED` (Python default true): a beam builds an enforcing
    /// security manager unless this is off.
    pub security_enabled: bool,
    /// `SECURITY_ENFORCE` (default true): when off, verdicts are evaluated
    /// and recorded but never block.
    pub security_enforce: bool,
    /// `BEAM_SECURITY_STREAM_HOLDBACK` (default 256): the characters of
    /// streamed model text held back until scanned (plan 030 D2). 0 turns the
    /// stream guard off. Below 64 (`beam_security::detect::ECHO_SPAN`) it
    /// cannot catch an echo of the instructions, and a secret longer than
    /// this can be partly released before it matches.
    pub security_stream_holdback: usize,
    /// `BEAM_SECURITY_SPOTLIGHT` (default on): untrusted context items and
    /// tool results reach the model inside `untrusted` tags, with a system
    /// line saying they are data (plan 030). Off for a model that handles the
    /// tags badly.
    pub security_spotlight: bool,
    pub usage_enabled: bool,
    /// `AUDIT_ENABLED` (default true).
    pub audit_enabled: bool,
    /// `AUDIT_SINKS` (default `["stdout"]`): `stdout`, `file`, `sql`.
    pub audit_sinks: Vec<String>,
    /// `AUDIT_FILE_PATH` for the `file` sink.
    pub audit_file_path: String,
    /// `MEMORY_BACKEND` (default `sql`): the session transcript's backend.
    /// Rust always kept `agent_message` rows in the configured store (in
    /// memory, or Postgres), so the default keeps doing that; `in_memory` is
    /// Python's default (no message rows). Python's `redis` reads as
    /// `sql_cached` (no Redis, plan 005 D4).
    pub memory_backend: String,
    /// `MEMORY_MAX_STORED_MESSAGES_PER_SESSION` (default 0: no cap).
    pub max_stored_messages: usize,
    /// `MEMORY_LONGTERM_TOP_K` (default 5): the `ltm` context source's budget.
    pub longterm_top_k: u64,
    /// `MEMORY_LONGTERM_ENABLED` (default off): the `ltm` context source.
    pub longterm_enabled: bool,
    /// `MEMORY_LONGTERM_AUTO_REMEMBER` (default off).
    pub auto_remember: bool,
    /// `MEMORY_LONGTERM_MIN_IMPORTANCE` (default 1).
    pub min_importance: i64,
    /// Execution timing and worker identity (`EXECUTION_TIMEOUT_SECONDS`,
    /// `LEASE_TTL_SECONDS`, `LEASE_HEARTBEAT_SECONDS`, `BEAM_WORKER_ID`).
    pub execution: beam_execution::ExecutionConfig,
}

impl Default for BeamConfig {
    fn default() -> Self {
        Self {
            worker_namespace: "riverbeam-worker".into(),
            knowledge_enabled: true,
            knowledge_embedding_dim: 1536,
            knowledge_max_context_tokens: 2000,
            default_model: None,
            security_enabled: true,
            security_enforce: true,
            security_stream_holdback: 256,
            security_spotlight: true,
            usage_enabled: true,
            audit_enabled: true,
            audit_sinks: vec!["stdout".into()],
            audit_file_path: "./data/audit.jsonl".into(),
            memory_backend: "sql".into(),
            max_stored_messages: 0,
            longterm_top_k: 5,
            longterm_enabled: false,
            auto_remember: false,
            min_importance: 1,
            execution: beam_execution::ExecutionConfig::default(),
        }
    }
}

/// `MEMORY_BACKEND`, with Python's `redis` read as `sql_cached`.
fn memory_backend(default: String) -> String {
    let backend = beam_env::string("MEMORY_BACKEND").unwrap_or(default);
    // D4: no Redis. Python's `redis` backend is a cache over nothing durable;
    // the nearest Rust backend is SQL behind the in-process cache.
    if backend == "redis" {
        tracing::warn!("MEMORY_BACKEND=redis is not supported (no Redis, D4); using sql_cached");
        return "sql_cached".into();
    }
    backend
}

impl BeamConfig {
    /// The defaults, with Python's environment keys applied. This is the only
    /// place a beam reads the environment ([`Self::default`] reads none).
    #[must_use]
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            memory_backend: memory_backend(d.memory_backend.clone()),
            max_stored_messages: beam_env::number("MEMORY_MAX_STORED_MESSAGES_PER_SESSION")
                .unwrap_or(d.max_stored_messages),
            longterm_top_k: beam_env::number("MEMORY_LONGTERM_TOP_K").unwrap_or(d.longterm_top_k),
            longterm_enabled: beam_env::flag("MEMORY_LONGTERM_ENABLED", d.longterm_enabled),
            auto_remember: beam_env::flag("MEMORY_LONGTERM_AUTO_REMEMBER", d.auto_remember),
            // Read as an unsigned count, as before (a negative value is ignored).
            min_importance: i64::try_from(
                beam_env::number::<usize>("MEMORY_LONGTERM_MIN_IMPORTANCE").unwrap_or(1),
            )
            .unwrap_or(d.min_importance),
            execution: beam_execution::ExecutionConfig::from_env(),
            knowledge_enabled: beam_env::flag("KNOWLEDGE_ENABLED", d.knowledge_enabled),
            security_enabled: beam_env::flag("SECURITY_ENABLED", d.security_enabled),
            security_enforce: beam_env::flag("SECURITY_ENFORCE", d.security_enforce),
            security_stream_holdback: beam_env::number("BEAM_SECURITY_STREAM_HOLDBACK")
                .unwrap_or(d.security_stream_holdback),
            security_spotlight: beam_env::flag("BEAM_SECURITY_SPOTLIGHT", d.security_spotlight),
            audit_enabled: beam_env::flag("AUDIT_ENABLED", d.audit_enabled),
            audit_sinks: beam_env::list("AUDIT_SINKS").unwrap_or(d.audit_sinks),
            audit_file_path: beam_env::string("AUDIT_FILE_PATH").unwrap_or(d.audit_file_path),
            ..d
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_accept_json_and_commas() {
        assert_eq!(
            serde_json::from_str::<Vec<String>>(r#"["stdout","sql"]"#).unwrap(),
            ["stdout", "sql"]
        );
        let d = BeamConfig::default();
        assert!(d.security_enabled && d.security_enforce && d.audit_enabled);
        assert_eq!(d.audit_sinks, ["stdout"]);
    }

    /// Plan 016 D4: `default()` reads no environment; `from_env()` reads the
    /// memory keys `RiverBeam::new` used to read itself.
    #[test]
    fn default_config_reads_no_environment() {
        // Only `from_env` reads these, and no other test in this crate calls it.
        std::env::set_var("MEMORY_LONGTERM_ENABLED", "true");
        std::env::set_var("MEMORY_BACKEND", "in_memory");
        std::env::set_var("MEMORY_LONGTERM_TOP_K", "9");
        // Plan 021 D2: garbage in a default-true key keeps the default.
        std::env::set_var("SECURITY_ENABLED", "garbage");
        let d = BeamConfig::default();
        assert!(!d.longterm_enabled);
        assert_eq!((d.memory_backend.as_str(), d.longterm_top_k), ("sql", 5));
        let e = BeamConfig::from_env();
        assert!(e.longterm_enabled);
        assert_eq!(
            (e.memory_backend.as_str(), e.longterm_top_k),
            ("in_memory", 9)
        );
        assert!(e.security_enabled, "garbage reads as the default (true)");
        for key in [
            "SECURITY_ENABLED",
            "MEMORY_LONGTERM_ENABLED",
            "MEMORY_BACKEND",
            "MEMORY_LONGTERM_TOP_K",
        ] {
            std::env::remove_var(key);
        }
    }
}
