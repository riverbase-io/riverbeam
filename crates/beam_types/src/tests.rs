#[cfg(test)]
mod tests {
    use super::super::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn invoke_request_roundtrip_minimal() {
        let session_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let req = InvokeRequest {
            agent_name: "agent-calculator".into(),
            content: vec![ContentPart::text("hello")],
            session_id,
            model_override: None,
            metadata: json!({}),
            interaction_id: None,
            request_id: None,
            stream: false,
            actions: None,
            auth_context: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        let back: InvokeRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back.agent_name, "agent-calculator");
        assert_eq!(back.session_id, session_id);
    }

    #[test]
    fn invoke_auth_context_accepts_organization_id_alias() {
        let raw = json!({
            "user_id": "u1",
            "organization_id": "org-9"
        });
        let ctx: InvokeAuthContext = serde_json::from_value(raw).unwrap();
        assert_eq!(ctx.org_id.as_deref(), Some("org-9"));
    }

    #[test]
    fn fixture_invoke_request_minimal_deserializes() {
        let raw = include_str!("../../../tests/wire_compat/fixtures/invoke_request_minimal.json");
        let req: InvokeRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.agent_name, "agent-calculator");
        assert_eq!(req.content.len(), 1);
        assert_eq!(req.content[0].text.as_deref(), Some("What is 2+2?"));
    }

    #[test]
    fn stream_chunk_done_shape() {
        let chunk = StreamChunk {
            chunk_type: StreamChunkType::Done,
            data: None,
            result: Some(AgentResult {
                content: vec![ContentPart::text("ok")],
                agent_name: "a".into(),
                model_used: None,
                session_id: None,
                request_id: None,
                interaction_id: None,
                token_usage: None,
                tool_calls: vec![],
                metadata: json!({}),
                latency_ms: None,
                interrupted: false,
                action_required: None,
            }),
            error: None,
            id: None,
            agent_id: None,
            agent_name: None,
            session_id: None,
            interaction_id: None,
            run_id: None,
            step_id: None,
            message_id: None,
            timestamp: None,
        };
        let v = serde_json::to_value(&chunk).unwrap();
        assert_eq!(v.get("type").and_then(|t| t.as_str()), Some("done"));
        assert!(v.get("result").is_some());
    }
}
