//! What the admin manages: `loan_assistant` revision 2 (the checklist reply
//! style the admin canaries and activates), the `loan-desk-smoke` eval
//! dataset, and legal holds mirrored into the beam's hold book.

use std::sync::Arc;

use beam_core::RiverBeam;
use beam_evals::{EvaluationCase, EvaluationDataset};
use beam_execution::revisions::AgentRevision;
use beam_runtime::AgentRuntime;
use beam_types::{AgentInput, AgentMetadata, InvokeAuthContext};
use riverbase_core::RiverbaseResult;
use serde_json::json;
use uuid::Uuid;

use crate::loan_desk::agents::{ASSISTANT_INSTRUCTIONS_V2, LOAN_ASSISTANT};
use crate::loan_desk::data::ApplicationStore;
use crate::loan_desk::personas::{JOHN, ORG};

/// The smoke dataset's id: fixed, so the UI and every worker name the same one.
pub const SMOKE_DATASET_ID: Uuid = Uuid::from_u128(0x5e0e_0000_0000_4000_8000_0000_0000_0012);
pub const SMOKE_DATASET: &str = "loan-desk-smoke";
/// `loan_assistant` revision 2's version and prompt revision.
pub const ASSISTANT_V2_VERSION: &str = "2.0.0";
pub const ASSISTANT_V2_PROMPT: &str = "checklist";

/// `loan-desk-smoke`: questions any assistant revision must answer, scored
/// by "the reply contains" (the eval runner's default). Tools run in dry run.
#[must_use]
pub fn smoke_dataset() -> EvaluationDataset {
    let cases = [
        (1, "Which loan programs can I choose from?", "FHA"),
        (
            2,
            "What is the minimum down payment for an FHA loan?",
            "3.5",
        ),
    ];
    EvaluationDataset {
        id: SMOKE_DATASET_ID,
        name: SMOKE_DATASET.into(),
        cases: cases
            .into_iter()
            .map(|(n, question, expected)| EvaluationCase {
                id: Uuid::from_u128(SMOKE_DATASET_ID.as_u128() + n),
                dataset_id: SMOKE_DATASET_ID,
                input: AgentInput {
                    agent_name: Some(LOAN_ASSISTANT.into()),
                    auth_context: Some(InvokeAuthContext {
                        profile_id: None,
                        user_id: Some(JOHN.into()),
                        org_id: Some(ORG.into()),
                        mcp_token: None,
                    }),
                    ..AgentInput::text(question)
                },
                expected: Some(json!({"text": expected})),
            })
            .collect(),
    }
}

/// Publish `loan_assistant` revision 2 on `runtime` (the assistant's own):
/// revision 1's spec plus [`ASSISTANT_INSTRUCTIONS_V2`]. The first publish is
/// a draft; publishing the same spec again returns the stored revision.
///
/// # Errors
/// The spec does not compile (an unknown tool); store errors.
pub async fn publish_assistant_v2(
    beam: &RiverBeam,
    metadata: &AgentMetadata,
    runtime: Arc<dyn AgentRuntime>,
) -> RiverbaseResult<AgentRevision> {
    let mut spec = beam_core::spec_from_registration(metadata, runtime.runtime_name());
    spec.version = ASSISTANT_V2_VERSION.into();
    spec.prompt.instructions = ASSISTANT_INSTRUCTIONS_V2.into();
    spec.prompt.revision = Some(ASSISTANT_V2_PROMPT.into());
    beam.publish_agent(spec, runtime).await
}

/// Store the smoke dataset in this beam's eval store. On Postgres every
/// worker reads it; saving again replaces its cases.
///
/// # Errors
/// Store errors.
pub async fn save_smoke_dataset(beam: &RiverBeam) -> RiverbaseResult<()> {
    beam.eval_store().save_dataset(&smoke_dataset()).await
}

/// Who loan desk's holds are placed by (`agent_legal_hold.placed_by`).
pub const LOAN_DESK_HOLD: &str = "loan_desk";

/// Mirror the applications' legal holds into the beam's holds: a borrower
/// with a held application keeps their agent data (`delete-user-agent-data`
/// is refused), one without has loan desk's hold released. On Postgres the
/// holds are durable, so they also stop the worker's deletion and
/// survive a restart until released here. A hold an admin placed with
/// `place-legal-hold` is never released or rewritten here.
///
/// # Errors
/// Store errors, including a hold that cannot be read or written.
pub async fn sync_holds(
    applications: &dyn ApplicationStore,
    beam: &RiverBeam,
    owner: Option<&str>,
) -> RiverbaseResult<usize> {
    let rows = applications.list(owner).await?;
    let held: std::collections::BTreeSet<&str> = rows
        .iter()
        .filter(|a| a.held)
        .map(|a| a.owner.as_str())
        .collect();
    // Only loan desk's own holds: an admin's `place-legal-hold` stays.
    if let Some(owner) = owner.filter(|o| !held.contains(o)) {
        beam.holds()
            .release_placed_by(owner, LOAN_DESK_HOLD)
            .await?;
    }
    for owner in &held {
        // An existing hold (loan desk's, or an admin's) keeps its reason.
        if !beam.holds().is_held(owner).await? {
            beam.holds()
                .place(
                    owner,
                    None,
                    Some("held loan application"),
                    Some(LOAN_DESK_HOLD),
                )
                .await?;
        }
    }
    Ok(held.len())
}
