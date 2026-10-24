//! Durable SSE replay (Python `stream/sse.py` and `stream/replay.py`): the
//! frames a reconnecting client gets after its `Last-Event-ID`, and their SSE
//! text. The HTTP route is the app's; this module is the wire contract.

use beam_types::pyjson::dumps;
use serde_json::{Map, Value};

/// Durable frames per replay.
pub const REPLAY_LIMIT: usize = 500;
const TERMINAL_STATUSES: [&str; 4] = ["completed", "failed", "cancelled", "timed_out"];

/// One `agent_execution_event` row.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredEvent {
    pub id: String,
    pub sequence: i64,
    pub event_type: String,
    pub payload: Value,
}

/// A numeric `Last-Event-ID` is the sequence;
/// otherwise it names an event row, whose sequence `lookup` gives. Unknown
/// ids replay from the start.
#[must_use]
pub fn cursor_sequence(
    last_event_id: Option<&str>,
    lookup: impl FnOnce(&str) -> Option<i64>,
) -> i64 {
    match last_event_id {
        None | Some("" | "0") => 0,
        Some(raw) if raw.bytes().all(|b| b.is_ascii_digit()) => raw.parse().unwrap_or(0),
        Some(raw) => lookup(raw).unwrap_or(0),
    }
}

/// The last AI message of the execution, for the replay's `message` frame.
#[derive(Debug, Clone, PartialEq)]
pub struct FinalMessage {
    pub content: Value,
    pub sequence: i64,
}

/// Python's pick of the replay message: the AI message with the highest
/// sequence (the first one on a tie), from `(role, content, sequence)` rows.
#[must_use]
pub fn final_ai_message<'a>(
    messages: impl IntoIterator<Item = (&'a str, &'a Value, i64)>,
) -> Option<FinalMessage> {
    let mut best: Option<FinalMessage> = None;
    for (role, content, sequence) in messages {
        if !role.eq_ignore_ascii_case("ai") {
            continue;
        }
        if best.as_ref().is_none_or(|b| sequence > b.sequence) {
            best = Some(FinalMessage {
                content: content.clone(),
                sequence,
            });
        }
    }
    best
}

/// A `state` frame, the final AI message, the
/// durable events after the cursor (at most [`REPLAY_LIMIT`]), then a `live`
/// hint while the execution still runs. Returns the frames and whether the
/// execution is terminal.
#[must_use]
pub fn assemble_replay(
    execution_id: &str,
    status: &str,
    events: &[StoredEvent],
    final_message: Option<&FinalMessage>,
    after_sequence: i64,
    session_id: Option<&str>,
    request_id: Option<&str>,
) -> (Vec<Value>, bool) {
    let status = status.to_lowercase();
    let mut frames = vec![object([
        ("type", Value::from("state")),
        ("status", Value::from(status.clone())),
        ("execution_id", Value::from(execution_id)),
    ])];
    if let Some(message) = final_message {
        frames.push(object([
            ("type", Value::from("message")),
            ("role", Value::from("ai")),
            ("content", message.content.clone()),
            ("sequence", Value::from(message.sequence)),
            ("execution_id", Value::from(execution_id)),
        ]));
    }
    let mut durable: Vec<(i64, Value)> = events
        .iter()
        .filter(|e| e.sequence > after_sequence)
        .map(|e| {
            let mut payload = e.payload.as_object().cloned().unwrap_or_default();
            payload
                .entry("type")
                .or_insert_with(|| Value::from(e.event_type.clone()));
            payload.insert("sequence".into(), Value::from(e.sequence));
            payload.insert("id".into(), Value::from(e.id.clone()));
            payload
                .entry("execution_id")
                .or_insert_with(|| Value::from(execution_id));
            (e.sequence, Value::Object(payload))
        })
        .collect();
    durable.sort_by_key(|(sequence, _)| *sequence);
    frames.extend(
        durable
            .into_iter()
            .take(REPLAY_LIMIT)
            .map(|(_, frame)| frame),
    );
    let terminal = TERMINAL_STATUSES.contains(&status.as_str());
    if !terminal {
        let opt = |v: Option<&str>| v.map_or(Value::Null, Value::from);
        frames.push(object([
            ("type", Value::from("live")),
            ("execution_id", Value::from(execution_id)),
            ("session_id", opt(session_id)),
            ("request_id", opt(request_id)),
        ]));
    }
    (frames, terminal)
}

fn object<const N: usize>(entries: [(&str, Value); N]) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect::<Map<_, _>>(),
    )
}

/// The replay as `text/event-stream`, byte for byte.
#[must_use]
pub fn sse_text(frames: &[Value], complete: bool) -> String {
    let mut lines = vec![": beam-replay".to_owned()];
    for frame in frames {
        // Python `frame.get("id") or frame.get("sequence") or ""`.
        let id = [frame.get("id"), frame.get("sequence")]
            .into_iter()
            .flatten()
            .find(|v| python_truthy(v))
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        lines.push(format!("id: {id}"));
        lines.push("event: beam.stream".to_owned());
        lines.push(format!("data: {}", dumps(frame)));
        lines.push(String::new());
    }
    if complete {
        lines.push("event: done".to_owned());
        lines.push(r#"data: {"type": "done"}"#.to_owned());
        lines.push(String::new());
    }
    lines.join("\n") + "\n"
}

fn python_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(sequence: i64, event_type: &str, payload: Value) -> StoredEvent {
        StoredEvent {
            id: format!("e{sequence}"),
            sequence,
            event_type: event_type.into(),
            payload,
        }
    }

    #[test]
    fn the_cursor_is_a_sequence_or_an_event_id() {
        assert_eq!(cursor_sequence(None, |_| Some(9)), 0);
        assert_eq!(cursor_sequence(Some("0"), |_| Some(9)), 0);
        assert_eq!(cursor_sequence(Some("5"), |_| Some(9)), 5);
        assert_eq!(
            cursor_sequence(Some("e7"), |id| (id == "e7").then_some(7)),
            7
        );
        assert_eq!(cursor_sequence(Some("unknown"), |_| None), 0);
    }

    /// A client at sequence 5 gets 6.. in order,
    /// once each, then the live hint.
    #[test]
    fn replays_after_the_cursor_in_order() {
        let events: Vec<StoredEvent> = (1..=8)
            .rev()
            .map(|s| event(s, "tool_result", json!({})))
            .collect();
        let (frames, terminal) =
            assemble_replay("x", "RUNNING", &events, None, 5, Some("s"), Some("r"));
        assert!(!terminal);
        let sequences: Vec<i64> = frames
            .iter()
            .filter_map(|f| f.get("sequence")?.as_i64())
            .collect();
        assert_eq!(sequences, [6, 7, 8]);
        assert_eq!(frames.first().unwrap()["status"], "running");
        assert_eq!(frames.last().unwrap()["type"], "live");
    }
}
