//! The admin's levers on an in-memory beam: `loan_assistant` revision 2 takes
//! a canary share with its own instructions; the smoke eval gates its
//! activation; erasing a borrower waits for their legal hold.

use std::sync::Arc;

use beam_core::{BeamConfig, RiverBeam};
use beam_runtime::{ScriptedModel, ScriptedTurn};
use beam_tools::{ToolRegistry, ToolRuntime, ToolService};
use beam_types::{ContentPart, InvokeAuthContext, InvokeRequest, RevisionStatus};
use showcase::loan_desk::admin::{sync_holds, ASSISTANT_V2_VERSION, SMOKE_DATASET_ID};
use showcase::loan_desk::agents::LOAN_ASSISTANT;
use showcase::loan_desk::app::{bind_agents, LoanDesk};
use showcase::loan_desk::data::{Application, ApplicationStore, InMemoryApplications};
use showcase::loan_desk::personas::{BOB, ORG};
use serde_json::json;
use uuid::Uuid;

/// A line only revision 2's instructions carry.
const V2_MARK: &str = "Ready to submit";

async fn desk(model: &ScriptedModel, applications: Arc<InMemoryApplications>) -> RiverBeam {
    let beam = RiverBeam::builder(BeamConfig::default())
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
    beam
}

fn bob(text: &str) -> InvokeRequest {
    InvokeRequest {
        agent_name: LOAN_ASSISTANT.into(),
        content: vec![ContentPart::text(text)],
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
    }
}

async fn revision_two(beam: &RiverBeam) -> Uuid {
    let revisions = beam.list_revisions(LOAN_ASSISTANT).await.unwrap();
    assert_eq!(revisions.len(), 2, "{revisions:?}");
    let v2 = revisions
        .iter()
        .find(|r| r.version == ASSISTANT_V2_VERSION)
        .expect("revision 2 is published");
    assert_eq!(v2.status, RevisionStatus::Draft);
    v2.id
}

async fn pinned(beam: &RiverBeam, execution: Option<Uuid>) -> Option<Uuid> {
    beam.executions()
        .records()
        .get(execution.unwrap())
        .await
        .unwrap()
        .unwrap()
        .agent_revision_id
}

#[tokio::test]
async fn revision_two_takes_the_canary_share_with_its_instructions() {
    let model = ScriptedModel::new([ScriptedTurn::text("one"), ScriptedTurn::text("two")]);
    let beam = desk(&model, Arc::default()).await;
    let v1 = beam.revisions().current_for(LOAN_ASSISTANT).unwrap().id;
    let v2 = revision_two(&beam).await;
    assert_ne!(v1, v2);

    beam.set_canary(LOAN_ASSISTANT, v2, 100).await.unwrap();
    let canaried = beam.invoke_result(bob("What is missing?")).await.unwrap();
    assert_eq!(pinned(&beam, canaried.execution_id).await, Some(v2));
    assert!(format!("{:?}", model.seen()[0]).contains(V2_MARK));

    beam.set_canary(LOAN_ASSISTANT, v2, 0).await.unwrap();
    let primary = beam.invoke_result(bob("And now?")).await.unwrap();
    assert_eq!(pinned(&beam, primary.execution_id).await, Some(v1));
    assert!(!format!("{:?}", model.seen()[1]).contains(V2_MARK));
}

/// `run-evaluation` runs each case on the revision it names, with no canary:
/// revision 1 keeps all the traffic.
#[tokio::test]
async fn the_smoke_eval_gates_activating_revision_two() {
    let model = ScriptedModel::new([
        ScriptedTurn::text("I am not sure."),
        ScriptedTurn::text("Ask your officer."),
        ScriptedTurn::text("Conventional, FHA, VA and Jumbo."),
        ScriptedTurn::text("FHA needs 3.5% down."),
    ]);
    let beam = Arc::new(desk(&model, Arc::default()).await);
    let v1 = beam.revisions().current_for(LOAN_ASSISTANT).unwrap().id;
    let v2 = revision_two(&beam).await;
    beam.mark_revision_validating(v2).await.unwrap();

    let failed = beam.run_evaluation(SMOKE_DATASET_ID, v2).await.unwrap();
    assert_eq!(failed.results.len(), 2);
    assert!(!failed.passed());
    assert!(
        beam.activate_revision(v2, true, None).await.is_err(),
        "a failed eval keeps it validating"
    );

    let passed = beam.run_evaluation(SMOKE_DATASET_ID, v2).await.unwrap();
    assert!(passed.passed(), "{passed:?}");
    assert!(
        model
            .seen()
            .iter()
            .all(|call| format!("{call:?}").contains(V2_MARK)),
        "every case ran on revision 2"
    );
    assert_eq!(
        beam.revisions().pick_for_new(LOAN_ASSISTANT, None),
        Some(v1),
        "the traffic stayed on revision 1"
    );
    let active = beam.activate_revision(v2, true, None).await.unwrap();
    assert_eq!(active.status, RevisionStatus::Active);
    assert_eq!(beam.revisions().current_for(LOAN_ASSISTANT).unwrap().id, v2);
}

#[tokio::test]
async fn erasing_a_borrower_waits_for_their_legal_hold() {
    let model = ScriptedModel::new([ScriptedTurn::text("Hello Bob.")]);
    let applications = Arc::new(InMemoryApplications::default());
    let beam = desk(&model, applications.clone()).await;
    beam.invoke_result(bob("Hi")).await.unwrap();
    let mut held = Application {
        held: true,
        ..Application::new(BOB, Some(ORG), "fha")
    };
    applications.insert(&held).await.unwrap();
    applications
        .insert(&Application::new(BOB, Some(ORG), "va"))
        .await
        .unwrap();

    assert_eq!(
        sync_holds(applications.as_ref(), &beam, None)
            .await
            .unwrap(),
        1
    );
    let refused = beam.delete_user_agent_data(BOB).await.unwrap_err();
    assert_eq!(refused.errcode.as_str(), "BEM-489", "{refused}");
    assert_eq!(applications.erase_owner(BOB).await.unwrap(), 1);
    assert_eq!(applications.list(Some(BOB)).await.unwrap(), [held.clone()]);

    held.held = false;
    applications.save(&mut held).await.unwrap();
    assert_eq!(
        sync_holds(applications.as_ref(), &beam, Some(BOB))
            .await
            .unwrap(),
        0
    );
    let erased = beam.delete_user_agent_data(BOB).await.unwrap();
    assert_eq!(erased["sessions"], 1, "{erased}");
    assert_eq!(applications.erase_owner(BOB).await.unwrap(), 1);
}
