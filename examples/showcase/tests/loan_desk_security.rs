//! The security engine on the loan desk: sensitive data a borrower types is
//! refused before it is written to the transcript or reaches the model.

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{ToolRegistry, ToolRuntime, ToolService};
use beam_types::{ContentPart, InvokeAuthContext, InvokeRequest};
use showcase::loan_desk::agents::LOAN_ASSISTANT;
use showcase::loan_desk::app::{bind_agents, LoanDesk};
use showcase::loan_desk::data::{ApplicationStore, InMemoryApplications};
use showcase::loan_desk::personas::{BOB, ORG};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn a_card_number_is_refused_before_the_transcript_and_the_model() {
    let model = ScriptedModel::new([ScriptedTurn::text("should not be reached")]);
    let applications: Arc<dyn ApplicationStore> = Arc::new(InMemoryApplications::default());
    let beam = RiverBeam::builder(BeamConfig {
        security_enabled: true,
        security_enforce: true,
        ..BeamConfig::default()
    })
    .model_caller(Arc::new(model.clone()))
    .tools(Arc::new(ToolService::new(
        Arc::new(ToolRegistry::new()),
        Arc::new(ToolRuntime::default()),
    )))
    .build()
    .unwrap();
    bind_agents(
        &beam,
        &LoanDesk {
            applications,
            mcp_tools: Vec::new(),
            packets: None,
        },
    )
    .await
    .unwrap();
    beam.checkin_catalog().await.unwrap();

    let request = InvokeRequest {
        agent_name: LOAN_ASSISTANT.into(),
        content: vec![ContentPart::text(
            "For the down payment, here is my card: credit card number: 4111 1111 1111 1111",
        )],
        session_id: Uuid::new_v4(),
        model_override: None,
        metadata: json!({}),
        interaction_id: None,
        execution_id: None,
        request_id: Some(Uuid::new_v4()),
        idempotency_key: None,
        stream: false,
        actions: None,
        auth_context: Some(InvokeAuthContext {
            profile_id: None,
            user_id: Some(BOB.into()),
            org_id: Some(ORG.into()),
            mcp_token: None,
        }),
        context: None,
        client: None,
    };
    let err = beam.invoke_result(request).await.unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-061", "{err}");
    assert_eq!(model.calls(), 0, "the model never saw it");
}
