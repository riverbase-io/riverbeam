//! rc8's reports (plan 023 A3) through the domain's query engine. Its own
//! test binary: the report handle is set once per process. Needs
//! `BEAM_DATABASE_URL`; skips without it.

mod support;

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_domain::{DomainStore, RiverBeamReports, UsageSummarySettings};
use beam_runtime::RigAgentDef;
use beam_tools::{ToolDefinition, ToolRegistry, ToolRisk, ToolRuntime, ToolService};
use riverbase_core::query::{QueryAccess, QueryRequest};
use riverbase_core::Domain;
use serde_json::{json, Value};
use support::{ctx, store, world};
use uuid::Uuid;

fn public(e: riverbase_core::RiverbaseError) -> String {
    beam_core::public_errors::to_public(e).errcode.to_string()
}

#[tokio::test]
async fn the_tool_policy_explanation_answers_for_the_callers_session() {
    let Some(store) = store("beam_domain_reports").await else {
        return;
    };
    let w = world(&store).await;
    // A beam on the same database, with a catalog and a rig agent.
    let registry = Arc::new(ToolRegistry::new());
    registry.register(ToolDefinition {
        risk: ToolRisk::High,
        ..ToolDefinition::new("wire")
    });
    registry.register(ToolDefinition::new("pay"));
    let beam = RiverBeam::builder(BeamConfig::default())
        .pg_store(store.clone())
        .tools(Arc::new(ToolService::new(
            registry,
            Arc::new(ToolRuntime::default()),
        )))
        .build()
        .unwrap();
    beam.bind_rig_agent(
        serde_json::from_value(json!({"name": "payer"})).unwrap(),
        RigAgentDef {
            tools: vec!["wire".into(), "pay".into()],
            approval_required: vec!["pay".into()],
            ..RigAgentDef::new("payer")
        },
    );
    beam_domain::set_reports(Arc::new(RiverBeamReports::new(
        Arc::new(beam),
        Some(DomainStore::for_store(&store)),
        UsageSummarySettings {
            enabled: false,
            default_plan: None,
        },
    )));
    let query = w.domain.query_dyn();
    let explain = |caller: riverbase_core::EngineContext, tool: &str| {
        let query = query.clone();
        let request = QueryRequest {
            user_query: Some(json!({
                "tool_name": tool,
                "agent_name": "payer",
                "session_id": w.session.to_string(),
            })),
            ..QueryRequest::default()
        };
        async move {
            query
                .execute(
                    &caller,
                    "tool-policy-explanation",
                    QueryAccess::Report,
                    request,
                    None,
                )
                .await
        }
    };
    let high: Value = explain(w.owner_ctx(), "wire").await.expect("wire")["data"].clone();
    assert_eq!(
        (high["mode"].as_str(), high["layer"].as_str()),
        (Some("ask"), Some("default")),
        "{high}"
    );
    assert_eq!(
        high["remember_options"],
        json!(["once", "session", "always"])
    );
    let agent: Value = explain(w.owner_ctx(), "pay").await.expect("pay")["data"].clone();
    assert_eq!(
        (agent["mode"].as_str(), agent["layer"].as_str()),
        (Some("ask"), Some("agent")),
        "{agent}"
    );

    let unknown = explain(w.owner_ctx(), "nope").await.unwrap_err();
    assert_eq!(public(unknown), "BEM-404");
    let stranger = ctx(Uuid::new_v4(), w.owner.1);
    let refused = explain(stranger, "wire").await.unwrap_err();
    assert_eq!(public(refused), "BEM-412");
}
