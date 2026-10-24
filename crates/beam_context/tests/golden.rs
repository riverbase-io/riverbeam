//! Golden parity with Python's `ContextBuilder`.
//!
//! Each fixture in `tests/context_compat/fixtures/` was written by
//! `scripts/gen_context_fixtures.py` from the Python stack: the inputs, and
//! the policy revision, manifest decisions and envelope Python produced. The
//! Rust builder must produce the same for the same inputs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use beam_context::builder::{assemble_build_request, BuildOptions};
use beam_context::services::{
    ContextServices, KnowledgeRetriever, LongTermMemoryReader, MemoryReader, RetrievalScope,
    SummaryReader, ToolCatalog, ToolSchema,
};
use beam_context::ContextBuilder;
use beam_knowledge::Hit;
use beam_memory::SessionSummary;
use beam_types::{AgentInput, AgentMessage, ModelConfig};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

struct Memory(Vec<AgentMessage>);

#[async_trait]
impl MemoryReader for Memory {
    async fn load_history(
        &self,
        _session: Uuid,
        limit: Option<usize>,
    ) -> RiverbaseResult<Vec<AgentMessage>> {
        let rows = self.0.clone();
        Ok(match limit {
            Some(l) if l > 0 && rows.len() > l => rows[rows.len() - l..].to_vec(),
            _ => rows,
        })
    }
}

struct Hits(Vec<Hit>);

#[async_trait]
impl KnowledgeRetriever for Hits {
    async fn retrieve(
        &self,
        _query: &str,
        _k: usize,
        _scope: &RetrievalScope,
    ) -> RiverbaseResult<Vec<Hit>> {
        Ok(self.0.clone())
    }
}

#[async_trait]
impl LongTermMemoryReader for Hits {
    async fn recall(
        &self,
        _query: &str,
        _k: usize,
        _scope: &RetrievalScope,
    ) -> RiverbaseResult<Vec<Hit>> {
        Ok(self.0.clone())
    }
}

struct Tools(Vec<Value>);

impl ToolCatalog for Tools {
    fn get_definition(&self, name: &str) -> Option<ToolSchema> {
        self.0
            .iter()
            .find(|s| s["name"] == name)
            .map(|s| ToolSchema {
                name: s["name"].as_str().unwrap_or_default().to_owned(),
                description: s["description"].as_str().unwrap_or_default().to_owned(),
                parameters: s["parameters"].clone(),
            })
    }
}

struct Summary(Option<String>);

#[async_trait::async_trait]
impl SummaryReader for Summary {
    async fn get(&self, session_id: &str) -> Option<SessionSummary> {
        let text = self.0.clone()?;
        serde_json::from_value(json!({
            "session_id": session_id,
            "text": text,
            "created_at": "2026-01-01T00:00:00Z",
        }))
        .ok()
    }
}

fn fixtures() -> Vec<(String, Value)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/context_compat/fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("fixture dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no context fixtures");
    paths
        .into_iter()
        .map(|p| {
            let text = std::fs::read_to_string(&p).expect("read fixture");
            (
                p.display().to_string(),
                serde_json::from_str(&text).expect("fixture json"),
            )
        })
        .collect()
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> T {
    serde_json::from_value(value.clone()).expect("decode fixture input")
}

async fn run(input: &Value) -> RiverbaseResult<Value> {
    let agent_input: AgentInput = decode(&input["agent_input"]);
    let model: ModelConfig = decode(&input["model"]);
    let memory_enabled = input.get("memory").and_then(Value::as_bool).unwrap_or(true);
    let history: Vec<AgentMessage> = input.get("history").map(decode).unwrap_or_default();
    let services = ContextServices {
        memory: memory_enabled.then(|| Arc::new(Memory(history)) as Arc<dyn MemoryReader>),
        knowledge: input
            .get("hits")
            .map(|h| Arc::new(Hits(decode(h))) as Arc<dyn KnowledgeRetriever>),
        long_term_memory: input
            .get("ltm_hits")
            .map(|h| Arc::new(Hits(decode(h))) as Arc<dyn LongTermMemoryReader>),
        tools: Some(Arc::new(Tools(
            input.get("tool_schemas").map(decode).unwrap_or_default(),
        ))),
        summaries: Some(Arc::new(Summary(
            input
                .get("summary")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ))),
        ..ContextServices::default()
    };
    let request = assemble_build_request(
        &agent_input,
        &model,
        services,
        BuildOptions {
            spec: input.get("spec").map(decode),
            tool_names: input.get("tool_names").map(decode).unwrap_or_default(),
            source_budgets: HashMap::from([("ltm".to_owned(), 5)]),
            ..BuildOptions::default()
        },
    );
    let revision = request.policy.revision.clone();
    let enable = request.policy.enable.clone();
    let result = ContextBuilder::default().build(&request).await?;
    let envelope = &result.envelope;
    Ok(json!({
        "policy_revision": revision,
        "enable": enable,
        "decisions": result.manifest.decisions,
        "messages": envelope.messages.iter().map(|m| json!({
            "role": m.role,
            "text": m.text_content(),
            "tool_call_id": m.tool_call_id,
            "tool_calls": m.tool_calls,
        })).collect::<Vec<_>>(),
        "tool_schemas": envelope.tool_schemas,
        "input_tokens": envelope.input_tokens,
        "output_tokens": envelope.output_tokens,
        "reserve_tokens": envelope.reserve_tokens,
        "window_tokens": envelope.window_tokens,
        "counter_method": envelope.counter_method,
        "cache_prefix_tokens": envelope.cache_prefix_tokens,
        "selected_untrusted": result.selected_untrusted.iter().map(|i| i.id.clone()).collect::<Vec<_>>(),
        "tools": result.tools(),
        "citations": result.citations,
    }))
}

#[tokio::test]
async fn builder_matches_python_fixtures() {
    for (path, fixture) in fixtures() {
        let expected = &fixture["expected"];
        match run(&fixture["input"]).await {
            Err(e) => {
                // Python's ContextOverflowError (BEM-432) is site BEM-480 here.
                assert_eq!(expected["error"], "BEM-432", "{path}: unexpected error {e}");
                assert_eq!(e.errcode.as_str(), "BEM-480", "{path}");
            }
            Ok(actual) => {
                assert!(
                    expected.get("error").is_none(),
                    "{path}: Python failed, Rust did not"
                );
                for key in expected.as_object().expect("expected object").keys() {
                    assert_eq!(actual[key], expected[key], "{path}: {key}");
                }
            }
        }
    }
}

/// Manifests round-trip through `agent_context_manifest` (Python
/// `SqlManifestStore`). Needs `BEAM_DATABASE_URL`; skips otherwise. The row is
/// left for `scripts/cross_read.py` to load with Python.
#[tokio::test]
async fn sql_manifest_store_roundtrip() {
    use beam_context::{ManifestStore, SqlManifestStore};
    use beam_store::{BeamStore, PgBeamStore};

    let Some(pool) = beam_store::testdb::pool("beam_context").await else {
        return;
    };
    let schema = beam_store::testdb::schema();
    let store =
        SqlManifestStore::new(Arc::new(PgBeamStore::new(pool, schema)) as Arc<dyn BeamStore>);

    let (_, fixture) = fixtures()
        .into_iter()
        .find(|(p, _)| p.ends_with("budget_split.json"))
        .expect("fixture");
    let agent_input: AgentInput = decode(&fixture["input"]["agent_input"]);
    let model: ModelConfig = decode(&fixture["input"]["model"]);
    let request = assemble_build_request(
        &agent_input,
        &model,
        ContextServices::default(),
        BuildOptions::default(),
    );
    let mut manifest = ContextBuilder::default()
        .build(&request)
        .await
        .expect("build")
        .manifest;
    let operation = Uuid::new_v4().to_string();
    let execution = Uuid::new_v4().to_string();
    manifest.parent_execution_id = Some(execution.clone());
    manifest.source_timings.clear();

    store.put(&operation, &manifest).await.expect("put");
    let stored = store.get(&operation).await.expect("get").expect("row");
    assert_eq!(stored.operation_id.as_deref(), Some(operation.as_str()));
    assert_eq!(stored.decisions, manifest.decisions);
    assert_eq!(stored.input_tokens, manifest.input_tokens);
    assert_eq!(stored.window_tokens, manifest.window_tokens);
    let reconciled = store
        .reconcile(&operation, manifest.input_tokens * 2)
        .await
        .expect("reconcile")
        .expect("row");
    assert_eq!(reconciled.estimate_drift, Some(1.0));
    // A second reconcile keeps the first measurement.
    let again = store
        .reconcile(&operation, 1)
        .await
        .expect("reconcile")
        .expect("row");
    assert_eq!(again.actual_prompt_tokens, Some(manifest.input_tokens * 2));
    assert_eq!(
        store
            .list_for_execution(&execution)
            .await
            .expect("list")
            .len(),
        1
    );
}
