//! A runtime run nested inside another (plan 023 D2): a custom runtime that
//! asks another runtime for one answer forwards what the outer run should
//! record, so the nested call leaves a step row. Its tokens are metered by
//! the beam's traced model caller, which the nested run shares.

use beam_types::{AgentResult, AgentStep};
use riverbase_core::RiverbaseResult;
use serde_json::json;

use crate::contract::{AgentRuntime, RuntimeContext, RuntimeStartRequest};
use crate::events::{collect_events, RuntimeEvent, RuntimeEventType};

/// Run `runtime` to its end inside the outer run `request` names. Returns
/// its result, and the events the outer run forwards: the nested run's
/// `usage` events, then a `step_completed` for the call, whose `lc_step_id`
/// is `{run_id}:{step_name}:{n}` (`n` tells a run's calls of one step apart).
///
/// # Errors
/// The nested run's errors.
pub async fn run_nested(
    runtime: &dyn AgentRuntime,
    request: RuntimeStartRequest,
    context: RuntimeContext,
    step_name: &str,
    n: u32,
) -> RiverbaseResult<(AgentResult, Vec<RuntimeEvent>)> {
    let (execution_id, run_id) = (request.execution_id, request.run_id);
    let prompt = request.input.content.clone();
    let (events, result) = collect_events(runtime.start(request, context)).await?;
    let mut forwarded: Vec<RuntimeEvent> = events
        .into_iter()
        .filter(|e| e.event_type == RuntimeEventType::Usage)
        .collect();
    let mut step = RuntimeEvent::new(RuntimeEventType::StepCompleted, execution_id, run_id);
    step.step = Some(AgentStep {
        step_id: None,
        lc_step_id: format!("{run_id}:{step_name}:{n}"),
        step_name: step_name.to_owned(),
        status: "completed".into(),
        input: json!({"runtime": runtime.runtime_name(), "content": prompt}),
        result: Some(json!({
            "text": result.text_content(),
            "model_used": result.model_used,
            "token_usage": result.token_usage,
        })),
    });
    step.usage.clone_from(&result.token_usage);
    forwarded.push(step);
    Ok((result, forwarded))
}

#[cfg(test)]
mod tests {
    use beam_types::AgentInput;
    use uuid::Uuid;

    use super::*;
    use crate::EchoRuntime;

    #[tokio::test]
    async fn a_nested_run_is_recorded_as_one_step() {
        let input = AgentInput::text("why");
        let request = RuntimeStartRequest {
            execution_id: Uuid::new_v4(),
            run_id: input.run_id,
            input,
            runtime_ref: None,
        };
        let run_id = request.run_id;
        let (result, events) = run_nested(
            &EchoRuntime::new(),
            request,
            RuntimeContext::default(),
            "rationale",
            1,
        )
        .await
        .expect("nested run");
        assert_eq!(result.text_content(), "Echo: why");
        let step = events.last().and_then(|e| e.step.clone()).expect("a step");
        assert_eq!(step.lc_step_id, format!("{run_id}:rationale:1"));
        assert_eq!(step.status, "completed");
    }
}
