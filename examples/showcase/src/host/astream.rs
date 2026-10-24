//! The deprecated `/astream/{session}/{request}` stream: the
//! run's durable replay, then its live frames until `done` or `error`.
//!
//! [`read_replay`] says whether the replay already ends the stream and which
//! frames it carried; [`live_until_end`] turns the live NATS frames into SSE
//! events, without the ones the replay sent.

use std::convert::Infallible;

use axum::body::Bytes;
use beam_types::{StreamChunk, StreamChunkType};
use futures_util::{Stream, StreamExt};
use serde_json::Value;

/// Execution states after which a run sends nothing more on this request.
/// `waiting_for_input` resumes as a new request.
const ENDED: [&str; 5] = [
    "completed",
    "failed",
    "cancelled",
    "timed_out",
    "waiting_for_input",
];

/// What a durable replay (`RiverBeam::replay_request`) says about the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayEnd {
    /// The run is over: the stream ends with the replay.
    pub complete: bool,
    /// The highest frame sequence the replay sent; live frames up to it are
    /// duplicates.
    pub last_sequence: Option<i64>,
}

/// Read a replay's SSE text: complete when it closes with `event: done`,
/// replays a `done` or `error` frame, or shows an ended execution state.
#[must_use]
pub fn read_replay(text: &str) -> ReplayEnd {
    let mut end = ReplayEnd {
        complete: false,
        last_sequence: None,
    };
    for line in text.lines() {
        if line == "event: done" {
            end.complete = true;
            continue;
        }
        let Some(data) = line.strip_prefix("data: ") else {
            continue;
        };
        let Ok(frame) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        if let Some(sequence) = frame.get("sequence").and_then(Value::as_i64) {
            end.last_sequence = Some(end.last_sequence.map_or(sequence, |s| s.max(sequence)));
        }
        let kind = frame
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let status = frame
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if matches!(kind, "done" | "error") || (kind == "state" && ENDED.contains(&status)) {
            end.complete = true;
        }
    }
    end
}

/// Live frames as SSE `data:` events, up to and including the run's `done`
/// or `error` frame. A frame whose sequence is at most `after` was in the
/// replay and is skipped.
pub fn live_until_end<S>(
    live: S,
    after: Option<i64>,
) -> impl Stream<Item = Result<Bytes, Infallible>>
where
    S: Stream<Item = Bytes> + Unpin,
{
    futures_util::stream::unfold(Some(live), move |live| async move {
        let mut live = live?;
        loop {
            let payload = live.next().await?;
            let Ok(frame) = serde_json::from_slice::<StreamChunk>(&payload) else {
                continue;
            };
            if frame
                .sequence
                .zip(after)
                .is_some_and(|(seq, last)| seq <= last)
            {
                continue;
            }
            let Ok(data) = serde_json::to_string(&frame) else {
                continue;
            };
            let last = matches!(
                frame.chunk_type,
                StreamChunkType::Done | StreamChunkType::Error
            );
            let event = Bytes::from(format!("data: {data}\n\n"));
            return Some((Ok(event), (!last).then_some(live)));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_stream::replay::sse_text;
    use serde_json::json;

    #[test]
    fn a_replay_ends_the_stream_only_when_its_run_is_over() {
        let ready = json!({"type": "ready", "session_id": "s", "request_id": "r"});
        // Still queued (no execution for the request): follow live.
        let queued = read_replay(&sse_text(std::slice::from_ref(&ready), false));
        assert_eq!(
            queued,
            ReplayEnd {
                complete: false,
                last_sequence: None
            }
        );
        // Running: follow live, after the replayed frames.
        let running = read_replay(&sse_text(
            &[
                ready.clone(),
                json!({"type": "state", "status": "running"}),
                json!({"type": "started", "sequence": 1}),
                json!({"type": "live"}),
            ],
            false,
        ));
        assert_eq!(
            running,
            ReplayEnd {
                complete: false,
                last_sequence: Some(1)
            }
        );
        // Finished, with the trailer, or a durable done before the state
        // caught up, or waiting for input: the replay is the whole stream.
        let ends = |frame: Value, trailer: bool| read_replay(&sse_text(&[frame], trailer)).complete;
        assert!(ends(json!({"type": "state", "status": "completed"}), true));
        assert!(ends(json!({"type": "done", "sequence": 2}), false));
        assert!(ends(
            json!({"type": "state", "status": "waiting_for_input"}),
            false
        ));
    }

    fn frame(kind: &str, sequence: Option<i64>, text: &str) -> Bytes {
        let mut frame = json!({"type": kind, "error": text});
        if let Some(sequence) = sequence {
            frame["sequence"] = json!(sequence);
        }
        Bytes::from(frame.to_string())
    }

    #[tokio::test]
    async fn live_frames_skip_the_replayed_ones_and_stop_at_done() {
        let live = futures_util::stream::iter(vec![
            frame("chunk", None, "a"),
            frame("chunk", Some(1), "replayed"),
            Bytes::from_static(b"not json"),
            frame("done", Some(2), "end"),
            frame("chunk", None, "after the end"),
        ]);
        let events: Vec<String> = live_until_end(live, Some(1))
            .map(|e| String::from_utf8(e.unwrap().to_vec()).unwrap())
            .collect()
            .await;
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(events[0].starts_with("data: ") && events[0].contains("\"a\""));
        assert!(events[1].contains("\"done\""), "{events:?}");
    }
}
