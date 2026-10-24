//! With `BEAM_TRACE_PRIVACY=redacted`, no
//! message text reaches span attributes or audit events. Its own test binary,
//! since the privacy mode is process-wide.

use std::sync::Arc;

use async_trait::async_trait;
use beam_audit::{AuditLogger, InMemorySink};
use beam_core::{BeamConfig, RiverBeam, RiverbaseResult};
use beam_runtime::{RigAgentDef, ScriptedModel, ScriptedTurn};
use beam_types::{AgentMetadata, ContentPart, InvokeAuthContext, InvokeRequest, Modality};
use serde_json::Value;
use uuid::Uuid;

const SECRET: &str = "the-launch-code-is-4471";

struct Echo;

#[async_trait]
impl beam_tools::ToolHandler for Echo {
    async fn call(
        &self,
        args: Value,
        _ctx: &beam_tools::ToolCallContext,
    ) -> RiverbaseResult<Value> {
        Ok(args)
    }
}

#[tokio::test]
async fn redacted_mode_keeps_message_text_out_of_spans_and_audit() {
    beam_telemetry::set_privacy_mode(Some(beam_telemetry::PrivacyMode::Redacted));
    let registry = Arc::new(beam_tools::ToolRegistry::new());
    registry.register(beam_tools::ToolDefinition::new("lookup").with_handler(Arc::new(Echo)));
    let model = ScriptedModel::new([
        ScriptedTurn::tool_call("c1", "lookup", serde_json::json!({ "query": SECRET })),
        ScriptedTurn::text(format!("you said {SECRET}")),
    ]);
    let sink = Arc::new(InMemorySink::new());
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(model))
        .audit(Arc::new(AuditLogger::new(vec![sink.clone()])))
        .tools(Arc::new(beam_tools::ToolService::new(
            registry,
            Arc::new(beam_tools::ToolRuntime::default()),
        )))
        .build()
        .unwrap();
    beam.bind_rig_agent(
        AgentMetadata {
            name: "finder".into(),
            description: "runtime agent".into(),
            supported_modalities: vec![Modality::Text],
            output_modalities: vec![Modality::Text],
            tags: vec![],
            version: "1.0.0".into(),
            final_nodes: Vec::new(),
            output_contract: None,
            tools: Vec::new(),
        },
        RigAgentDef {
            tools: vec!["lookup".into()],
            ..RigAgentDef::new("finder")
        },
    );
    let trace_id = Uuid::new_v4().simple().to_string();
    let request = InvokeRequest {
        agent_name: "finder".into(),
        content: vec![ContentPart::text(SECRET)],
        session_id: Uuid::new_v4(),
        model_override: None,
        metadata: serde_json::json!({ "traceparent": format!("00-{trace_id}-00f067aa0ba902b7-01") }),
        interaction_id: None,
        request_id: Some(Uuid::new_v4()),
        stream: false,
        actions: None,
        auth_context: Some(InvokeAuthContext {
            profile_id: None,
            user_id: Some("tester".into()),
            org_id: None,
            mcp_token: None,
        }),
        idempotency_key: None,
        execution_id: None,
        context: None,
        client: None,
    };
    beam.invoke_result(request).await.expect("invoke");

    let spans = beam_telemetry::recorded_spans(&trace_id);
    assert!(
        spans.iter().any(|s| s.name == "beam.tool"),
        "the run made a tool call"
    );
    for span in &spans {
        let attributes = Value::Object(span.attributes.clone()).to_string();
        assert!(
            !attributes.contains(SECRET),
            "{} keeps message text",
            span.name
        );
    }
    let events = sink.events();
    assert!(!events.is_empty());
    for event in events {
        let line = serde_json::to_string(&event).expect("event json");
        assert!(
            !line.contains(SECRET),
            "{:?} keeps message text",
            event.action
        );
    }
}
