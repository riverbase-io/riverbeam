//! Python's stream contracts (`tests/stream_compat/fixtures.json`, written by
//! `scripts/gen_stream_fixtures.py`): durable frame types, channel parsing,
//! and replay frames with their SSE text, byte for byte.

use beam_stream::channel::parse_beam_stream_channel;
use beam_stream::durable::durable_event_type;
use beam_stream::replay::{assemble_replay, final_ai_message, sse_text, StoredEvent};
use serde_json::Value;

fn fixtures() -> Value {
    let raw = include_str!("../../../tests/stream_compat/fixtures.json");
    serde_json::from_str(raw).expect("fixtures")
}

#[test]
fn durable_types_match_python() {
    for case in fixtures()["durable"].as_array().expect("durable") {
        let expected = case["expected"].as_str();
        assert_eq!(
            durable_event_type(&case["frame"]),
            expected,
            "{}",
            case["frame"]
        );
    }
}

#[test]
fn channels_parse_like_python() {
    for case in fixtures()["channels"].as_array().expect("channels") {
        let channel = case["channel"].as_str().expect("channel");
        let got =
            parse_beam_stream_channel(channel).map(|(s, r)| vec![s.to_string(), r.to_string()]);
        let expected = case["expected"].as_array().map(|ids| {
            ids.iter()
                .map(|id| id.as_str().expect("id").to_lowercase())
                .collect::<Vec<_>>()
        });
        assert_eq!(got, expected, "{channel}");
    }
}

#[test]
fn replays_match_python_byte_for_byte() {
    for case in fixtures()["replays"].as_array().expect("replays") {
        let events: Vec<StoredEvent> = case["events"]
            .as_array()
            .expect("events")
            .iter()
            .map(|e| StoredEvent {
                id: e["_id"].as_str().expect("id").to_owned(),
                sequence: e["sequence"].as_i64().expect("sequence"),
                event_type: e["event_type"].as_str().expect("type").to_owned(),
                payload: e["payload"].clone(),
            })
            .collect();
        let messages = case["messages"].as_array().expect("messages");
        let last = final_ai_message(messages.iter().map(|m| {
            (
                m["role"].as_str().expect("role"),
                &m["content"],
                m["sequence"].as_i64().expect("sequence"),
            )
        }));
        let (frames, terminal) = assemble_replay(
            case["execution_id"].as_str().expect("execution"),
            case["status"].as_str().expect("status"),
            &events,
            last.as_ref(),
            case["after_sequence"].as_i64().expect("after"),
            case["session_id"].as_str(),
            case["request_id"].as_str(),
        );
        assert_eq!(terminal, case["terminal"].as_bool().expect("terminal"));
        assert_eq!(
            Value::Array(frames.clone()),
            case["frames"],
            "{}",
            case["execution_id"]
        );
        assert_eq!(
            sse_text(&frames, terminal),
            case["sse"].as_str().expect("sse")
        );
    }
}
