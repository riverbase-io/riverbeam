//! HITL action contracts (Python `runtime/actions.py`, payload models from
//! `kernel/types/action.py`).

use beam_types::{ActionRequestEnvelope, ActionResponseEnvelope, ActionType};
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfirmationRequest {
    pub question: String,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfirmationResponse {
    pub approved: bool,
    #[serde(default)]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FormInputRequest {
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub values: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FormInputResponse {
    #[serde(default)]
    pub values: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChoiceQuestionRequest {
    pub question: String,
    pub choices: Vec<String>,
    #[serde(default)]
    pub allow_multiple: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChoiceQuestionResponse {
    pub selected_choices: Vec<String>,
    #[serde(default)]
    pub comment: Option<String>,
}

/// How long a tool decision is remembered (Python rc8 `RememberChoice`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RememberChoice {
    #[default]
    Once,
    Session,
    Always,
}

fn remember_once() -> Vec<RememberChoice> {
    vec![RememberChoice::Once]
}

fn medium() -> String {
    "medium".into()
}

fn yes() -> bool {
    true
}

/// `beam.tool_approval` request (Python rc8 `ToolApprovalActionRequestPayload`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolApprovalRequest {
    pub tool_name: String,
    pub call_id: String,
    #[serde(default)]
    pub args: Map<String, Value>,
    #[serde(default = "medium")]
    pub risk: String,
    #[serde(default)]
    pub decided_by: Map<String, Value>,
    #[serde(default = "remember_once")]
    pub remember_options: Vec<RememberChoice>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolApprovalResponse {
    pub approved: bool,
    #[serde(default)]
    pub remember: RememberChoice,
    #[serde(default)]
    pub edited_args: Option<Map<String, Value>>,
    #[serde(default)]
    pub comment: Option<String>,
}

/// `beam.client_tool` request (Python rc8 `ClientToolActionRequestPayload`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientToolRequest {
    pub tool_name: String,
    pub call_id: String,
    #[serde(default)]
    pub args: Map<String, Value>,
    #[serde(default = "medium")]
    pub risk: String,
    #[serde(default)]
    pub requires_confirmation: bool,
    #[serde(default = "remember_once")]
    pub remember_options: Vec<RememberChoice>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// The client's answer to a `beam.client_tool` action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientToolResponse {
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default = "yes")]
    pub approved: bool,
    #[serde(default)]
    pub remember: RememberChoice,
}

/// One registered action contract: its kind and action type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentContract {
    pub kind: &'static str,
    pub action_type: ActionType,
}

/// Python's built-in contracts.
pub const CONTRACTS: &[AgentContract] = &[
    AgentContract {
        kind: "beam.confirmation",
        action_type: ActionType::Confirmation,
    },
    AgentContract {
        kind: "beam.approval",
        action_type: ActionType::Approval,
    },
    AgentContract {
        kind: "beam.form_input",
        action_type: ActionType::Form,
    },
    AgentContract {
        kind: "beam.choice_question",
        action_type: ActionType::Choice,
    },
    AgentContract {
        kind: TOOL_APPROVAL_KIND,
        action_type: ActionType::ToolApproval,
    },
    AgentContract {
        kind: CLIENT_TOOL_KIND,
        action_type: ActionType::ClientTool,
    },
];

/// The action a tool call that waits for a human raises.
pub const TOOL_APPROVAL_KIND: &str = "beam.tool_approval";
/// The action a call to a client-run tool raises.
pub const CLIENT_TOOL_KIND: &str = "beam.client_tool";

/// # Errors
/// `BEM-398` when no contract is registered for `kind`.
pub fn contract(kind: &str) -> RiverbaseResult<AgentContract> {
    CONTRACTS
        .iter()
        .copied()
        .find(|c| c.kind == kind)
        .ok_or_else(|| {
            crate::BEM_398.with_data(format!("No action contract registered for {kind}"))
        })
}

fn parse<T: for<'de> Deserialize<'de> + Serialize>(
    payload: &Value,
    site: &riverbase_core::base::ErrorSpec,
) -> RiverbaseResult<Value> {
    let typed: T =
        serde_json::from_value(payload.clone()).map_err(|e| site.with_data(e.to_string()))?;
    serde_json::to_value(typed).map_err(|e| site.with_data(e.to_string()))
}

fn validate_request_payload(action_type: ActionType, payload: &Value) -> RiverbaseResult<Value> {
    let site = &crate::BEM_398;
    match action_type {
        ActionType::Confirmation | ActionType::Approval => {
            parse::<ConfirmationRequest>(payload, site)
        }
        ActionType::Form => parse::<FormInputRequest>(payload, site),
        ActionType::Choice => {
            let value = parse::<ChoiceQuestionRequest>(payload, site)?;
            if value["choices"].as_array().map_or(0, Vec::len) < 2 {
                return Err(site.with_data("choices: at least 2 items are required"));
            }
            Ok(value)
        }
        ActionType::ToolApproval => parse::<ToolApprovalRequest>(payload, site),
        ActionType::ClientTool => parse::<ClientToolRequest>(payload, site),
    }
}

fn remember_schema() -> Value {
    json!({"default": "once", "enum": ["once", "session", "always"], "title": "Remember", "type": "string"})
}

/// The JSON schema of a contract's response payload.
#[must_use]
pub fn response_schema(action_type: ActionType) -> Value {
    match action_type {
        ActionType::Confirmation | ActionType::Approval => json!({
            "properties": {
                "approved": {"title": "Approved", "type": "boolean"},
                "comment": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null, "title": "Comment"},
            },
            "required": ["approved"],
            "title": "ConfirmationActionResponsePayload",
            "type": "object",
        }),
        ActionType::Form => json!({
            "properties": {
                "values": {"additionalProperties": true, "title": "Values", "type": "object"},
            },
            "title": "FormInputActionResponsePayload",
            "type": "object",
        }),
        ActionType::Choice => json!({
            "properties": {
                "selected_choices": {"items": {"type": "string"}, "minItems": 1, "title": "Selected Choices", "type": "array"},
                "comment": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null, "title": "Comment"},
            },
            "required": ["selected_choices"],
            "title": "ChoiceQuestionActionResponsePayload",
            "type": "object",
        }),
        ActionType::ToolApproval => json!({
            "properties": {
                "approved": {"title": "Approved", "type": "boolean"},
                "remember": remember_schema(),
                "edited_args": {"anyOf": [{"additionalProperties": true, "type": "object"}, {"type": "null"}], "default": null, "title": "Edited Args"},
                "comment": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null, "title": "Comment"},
            },
            "required": ["approved"],
            "title": "ToolApprovalActionResponsePayload",
            "type": "object",
        }),
        ActionType::ClientTool => json!({
            "properties": {
                "output": {"default": null, "title": "Output"},
                "error": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null, "title": "Error"},
                "approved": {"default": true, "title": "Approved", "type": "boolean"},
                "remember": remember_schema(),
            },
            "title": "ClientToolActionResponsePayload",
            "type": "object",
        }),
    }
}

/// Validate an action request before the run stops for it (Python
/// `validate_action_request`): the payload is checked against the kind's
/// request model, and a missing `action_type` and `response_schema` are filled in.
///
/// # Errors
/// `BEM-398` for an unknown kind, a type mismatch, or an invalid payload.
pub fn validate_action_request(
    envelope: ActionRequestEnvelope,
) -> RiverbaseResult<ActionRequestEnvelope> {
    let contract = contract(&envelope.kind)?;
    if let Some(given) = envelope.action_type {
        if given != contract.action_type {
            return Err(crate::BEM_398.with_data(format!(
                "Action type mismatch for {}: expected {:?}, got {given:?}",
                envelope.kind, contract.action_type
            )));
        }
    }
    let payload = validate_request_payload(contract.action_type, &envelope.payload)?;
    Ok(ActionRequestEnvelope {
        payload,
        action_type: Some(contract.action_type),
        response_schema: envelope
            .response_schema
            .or_else(|| Some(response_schema(contract.action_type))),
        ..envelope
    })
}

/// Validate a response against its kind's response model (Python
/// `AgentContract.validate_response`). A missing payload stays `None`.
///
/// # Errors
/// `BEM-399` for an unknown kind or an invalid payload.
pub fn validate_action_response(
    response: &ActionResponseEnvelope,
) -> RiverbaseResult<Option<Value>> {
    let contract = contract(&response.kind).map_err(|e| crate::BEM_399.with_data(e.errmesg))?;
    let Some(payload) = &response.payload else {
        return Ok(None);
    };
    let site = &crate::BEM_399;
    let value = match contract.action_type {
        ActionType::Confirmation | ActionType::Approval => {
            parse::<ConfirmationResponse>(payload, site)?
        }
        ActionType::Form => parse::<FormInputResponse>(payload, site)?,
        ActionType::Choice => {
            let value = parse::<ChoiceQuestionResponse>(payload, site)?;
            if value["selected_choices"]
                .as_array()
                .is_none_or(Vec::is_empty)
            {
                return Err(site.with_data("selected_choices: at least 1 item is required"));
            }
            value
        }
        ActionType::ToolApproval => parse::<ToolApprovalResponse>(payload, site)?,
        ActionType::ClientTool => parse::<ClientToolResponse>(payload, site)?,
    };
    Ok(Some(value))
}

/// Named arguments for [`build_action_request`].
#[derive(Debug, Clone, Default)]
pub struct ActionRequestArgs {
    pub kind: String,
    pub request_id: String,
    pub title: String,
    pub description: String,
    pub payload: Value,
    pub action_type: Option<ActionType>,
    pub submit_label: Option<String>,
    pub response_schema: Option<Value>,
    pub metadata: Option<Value>,
}

/// An action request envelope from its parts.
#[must_use]
pub fn build_action_request(args: ActionRequestArgs) -> ActionRequestEnvelope {
    ActionRequestEnvelope {
        kind: args.kind,
        request_id: args.request_id,
        title: args.title,
        description: args.description,
        action_type: args.action_type,
        submit_label: args.submit_label,
        payload: args.payload,
        response_schema: args.response_schema,
        metadata: args.metadata.unwrap_or_else(|| json!({})),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approval(payload: Value) -> ActionRequestEnvelope {
        build_action_request(ActionRequestArgs {
            kind: "beam.approval".into(),
            request_id: "r1".into(),
            title: "Approve".into(),
            description: "d".into(),
            payload,
            ..ActionRequestArgs::default()
        })
    }

    #[test]
    fn validation_fills_type_and_schema() {
        let valid = validate_action_request(approval(json!({"question": "Pay?"}))).unwrap();
        assert_eq!(valid.action_type, Some(ActionType::Approval));
        assert_eq!(valid.payload, json!({"question": "Pay?", "summary": null}));
        assert_eq!(
            valid.response_schema.unwrap()["required"],
            json!(["approved"])
        );
    }

    #[test]
    fn invalid_requests_are_refused() {
        let missing = validate_action_request(approval(json!({}))).unwrap_err();
        assert_eq!(missing.errcode.as_str(), "BEM-398");
        let mut mismatch = approval(json!({"question": "q"}));
        mismatch.action_type = Some(ActionType::Form);
        assert!(validate_action_request(mismatch).is_err());
        let mut unknown = approval(json!({"question": "q"}));
        unknown.kind = "beam.unknown".into();
        assert!(validate_action_request(unknown).is_err());
        let mut one_choice = approval(json!({"question": "q", "choices": ["a"]}));
        one_choice.kind = "beam.choice_question".into();
        assert!(validate_action_request(one_choice).is_err());
    }

    #[test]
    fn tool_kinds_follow_rc8() {
        let mut client = approval(json!({
            "tool_name": "request_input", "call_id": "c1", "args": {"title": "Income"},
            "expires_at": "2026-10-20T10:00:00+00:00",
        }));
        client.kind = CLIENT_TOOL_KIND.into();
        let valid = validate_action_request(client.clone()).unwrap();
        assert_eq!(valid.action_type, Some(ActionType::ClientTool));
        assert_eq!(valid.payload["remember_options"], json!(["once"]));
        assert_eq!(valid.payload["requires_confirmation"], json!(false));
        client.payload.as_object_mut().unwrap().remove("expires_at");
        assert!(validate_action_request(client).is_err());

        let mut tool = approval(json!({"tool_name": "pay", "call_id": "c2"}));
        tool.kind = TOOL_APPROVAL_KIND.into();
        assert_eq!(
            validate_action_request(tool).unwrap().action_type,
            Some(ActionType::ToolApproval)
        );

        let answer = |kind: &str, payload: Value| ActionResponseEnvelope {
            action_id: "a".into(),
            lc_action_id: None,
            kind: kind.into(),
            request_id: "r".into(),
            status: beam_types::ActionResponseStatus::Submitted,
            payload: Some(payload),
            errors: Vec::new(),
            responder_id: None,
            responded_at: chrono::Utc::now(),
        };
        let output =
            validate_action_response(&answer(CLIENT_TOOL_KIND, json!({"output": {"income": 5}})))
                .unwrap()
                .unwrap();
        assert_eq!(output["approved"], json!(true));
        assert!(validate_action_response(&answer(TOOL_APPROVAL_KIND, json!({}))).is_err());
    }
}
