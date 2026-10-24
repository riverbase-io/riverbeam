//! Building an agent's context for a turn.

use std::sync::Arc;

use beam_context::builder::{assemble_build_request, BuildOptions};
use beam_context::services::{KnowledgeRetriever, MemoryReader, SummaryReader, ToolCatalog};
use beam_context::ContextServices;
use beam_types::{AgentContext, AgentInput};
use serde_json::Value;

use riverbase_core::RiverbaseResult;

use super::{history_before_current, PreparedContext, RiverBeam};

impl RiverBeam {
    /// Compile the context policy, run
    /// the sources, compose the envelope, store the manifest, then check the
    /// selected untrusted items (`BEFORE_CONTEXT_INJECTION`).
    #[allow(clippy::too_many_lines)]
    pub(super) async fn build_agent_context(
        &self,
        input: &AgentInput,
    ) -> RiverbaseResult<PreparedContext> {
        let agent_name = input
            .agent_name
            .as_deref()
            .ok_or_else(|| crate::BEM_022.raise())?;
        let metadata = self.registry.get_metadata(agent_name)?;
        // A pinned spec narrows the model, the
        // tools, the context policy and the deadline.
        let spec = self.pinned_spec(input);
        let model =
            self.apply_model_policy(self.router.resolve(input, Some(&metadata))?, spec.as_ref())?;
        // Declared tools that are registered.
        let mut tool_names: Vec<String> =
            self.tool_source.catalog().map_or_else(Vec::new, |tools| {
                let registered = tools.registry().list_tool_names();
                metadata
                    .tools
                    .iter()
                    .filter(|name| registered.contains(name))
                    .cloned()
                    .collect()
            });
        let mut context_spec = None;
        let mut deadline = None;
        if let Some(spec) = &spec {
            let (narrowed, tools) = self.spec_context(spec);
            context_spec = Some(narrowed);
            tool_names = tools;
            // The spec's, capped by the default.
            let default = self.executions.config().timeout;
            deadline = spec
                .execution
                .timeout_s
                .filter(|t| *t > 0)
                .map(|t| {
                    std::time::Duration::from_secs(u64::try_from(t).unwrap_or(u64::MAX))
                        .min(default)
                })
                .and_then(|d| chrono::Duration::from_std(d).ok())
                .map(|d| chrono::Utc::now() + d);
        }
        // Python rc8 `split_client_tools`: a client tool the calling client did
        // not declare is neither advertised nor charged to the context.
        if let Some(tools) = self.tool_source.catalog() {
            tool_names.retain(|name| {
                !tools.registry().get(name).is_some_and(|t| t.is_client())
                    || input.client.as_ref().is_some_and(|c| c.runs(name))
            });
        }
        let services = ContextServices {
            memory: Some(self.transcript.clone() as Arc<dyn MemoryReader>),
            knowledge: self
                .knowledge
                .clone()
                .map(|k| k as Arc<dyn KnowledgeRetriever>),
            // Python reads long-term memory through the knowledge manager when
            // `MEMORY_LONGTERM_ENABLED`; the policy still decides (`ltm`).
            long_term_memory: self
                .knowledge
                .clone()
                .filter(|_| self.config.longterm_enabled)
                .map(|k| k as Arc<dyn beam_context::LongTermMemoryReader>),
            tools: self
                .tool_source
                .catalog()
                .map(|t| t.registry().clone() as Arc<dyn ToolCatalog>),
            profiles: self.profile_resolver.clone(),
            resources: self.resource_resolver.clone(),
            summaries: Some(Arc::new(beam_context::services::StoredSummaries(
                self.transcript.summaries().clone(),
            )) as Arc<dyn SummaryReader>),
        };
        let request = assemble_build_request(
            input,
            &model,
            services,
            BuildOptions {
                spec: context_spec,
                tool_names,
                deadline,
                source_budgets: std::collections::HashMap::from([(
                    "ltm".to_owned(),
                    self.config.longterm_top_k,
                )]),
                ..BuildOptions::default()
            },
        );
        let mut result = self.context_builder.build(&request).await?;
        // Python `apply_context_result` and `persist_context_evidence`.
        let operation_id = result
            .manifest
            .operation_id
            .clone()
            .unwrap_or_else(|| input.run_id.to_string());
        result.manifest.operation_id = Some(operation_id.clone());
        if result.manifest.parent_execution_id.is_none() {
            result.manifest.parent_execution_id = input.execution_id.map(|e| e.to_string());
        }
        self.manifests.put(&operation_id, &result.manifest).await?;
        self.check_selected_untrusted(&result.selected_untrusted, input)
            .await?;

        let mut agent_metadata = input.metadata.clone();
        if !result.citations.is_empty() {
            if agent_metadata.is_null() {
                agent_metadata = Value::Object(serde_json::Map::new());
            }
            if let Some(map) = agent_metadata.as_object_mut() {
                map.insert("citations".into(), Value::Array(result.citations.clone()));
            }
        }
        Ok(PreparedContext {
            agent: AgentContext {
                session_id: input.session_id.to_string(),
                user_id: input.user_id(),
                conversation_history: history_before_current(&result, input),
                model_config_resolved: Some(model),
                available_tools: result.tools().to_vec(),
                metadata: agent_metadata,
            },
            envelope: result.envelope,
            manifest: result.manifest,
            citations: result.citations,
        })
    }
}
