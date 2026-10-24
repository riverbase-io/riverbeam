//! `paystub_reader`: an image in, the `vision` model alias, and structured
//! output that matches the pay stub schema.

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{ToolRegistry, ToolRuntime, ToolService};
use beam_types::{ContentPart, InvokeAuthContext, InvokeRequest};
use showcase::loan_desk::agents::{paystub_def, paystub_metadata, PAYSTUB_READER};
use showcase::loan_desk::personas::{BOB, ORG};
use serde_json::{json, Value};
use uuid::Uuid;

#[tokio::test]
async fn a_pay_stub_image_goes_to_the_vision_model_and_comes_back_structured() {
    let extracted = json!({
        "employer": "Riverside Logistics Inc.",
        "pay_period_end": "2026-10-11",
        "pay_frequency": "biweekly",
        "gross_pay": 2861.54,
        "gross_monthly_income": 6200.0,
        "confidence": 0.93
    });
    let model = ScriptedModel::new([ScriptedTurn::text(extracted.to_string())]);
    // The loan desk's catalog: an image needs a model that takes images.
    let catalog = concat!(env!("CARGO_MANIFEST_DIR"), "/models.yaml");
    let models = beam_routing::load_models_from_yaml(catalog).unwrap();
    let aliases = beam_routing::load_aliases_from_yaml(catalog).unwrap();
    let beam = RiverBeam::builder(BeamConfig::default())
        .model_caller(Arc::new(model.clone()))
        .router(
            beam_routing::ModelRouter::new(models, Some("nemotron-lightning".into()))
                .unwrap()
                .with_aliases(aliases),
        )
        .tools(Arc::new(ToolService::new(
            Arc::new(ToolRegistry::new()),
            Arc::new(ToolRuntime::default()),
        )))
        .build()
        .unwrap();
    beam.bind_rig_agent(paystub_metadata(), paystub_def());
    beam.checkin_catalog().await.unwrap();

    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
    let request = InvokeRequest {
        agent_name: PAYSTUB_READER.into(),
        content: vec![
            ContentPart::text("Read this pay stub."),
            serde_json::from_value(
                json!({"type": "image_base64", "data": png, "mime_type": "image/png"}),
            )
            .unwrap(),
        ],
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
    let done = beam.invoke_result(request).await.unwrap();
    let call = &model.seen()[0];
    assert_eq!(call.model.as_deref(), Some("vision"));
    assert!(
        format!("{:?}", call.prompt).contains("Image"),
        "the image reaches the model: {:?}",
        call.prompt
    );
    let answer: Value = serde_json::from_str(&done.text_content()).unwrap();
    for field in [
        "employer",
        "gross_pay",
        "pay_frequency",
        "gross_monthly_income",
        "confidence",
    ] {
        assert!(answer.get(field).is_some(), "{field} missing from {answer}");
    }
    assert_eq!(answer["gross_monthly_income"], json!(6200.0));
}
