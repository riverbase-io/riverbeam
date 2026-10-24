//! In-process span recorder (Python `telemetry/tracing.py`).
//!
//! The span tree is `beam.command` (the domain layer) → the worker extracts the
//! parent → `beam.execution` → `beam.runtime`, `beam.model`, `beam.tool`. The
//! current span is a `tokio` task-local, like Python's `ContextVar`; work run
//! inside [`in_span`] sees it.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Mutex, OnceLock};

use serde_json::{Map, Value};

use crate::privacy::{apply_privacy, privacy_mode};
use crate::propagation::TraceContext;

pub const SPAN_COMMAND: &str = "beam.command";
pub const SPAN_WORKER: &str = "beam.worker";
pub const SPAN_EXECUTION: &str = "beam.execution";
pub const SPAN_RUNTIME: &str = "beam.runtime";
pub const SPAN_MODEL: &str = "beam.model";
pub const SPAN_TOOL: &str = "beam.tool";
pub const SPAN_RAG: &str = "beam.rag";

/// Spans kept in process (oldest dropped first); Python keeps every span.
const KEPT_SPANS: usize = 10_000;

/// A finished span, kept for tests and export.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanRecord {
    pub name: String,
    pub context: TraceContext,
    pub attributes: Map<String, Value>,
}

tokio::task_local! {
    static CURRENT: TraceContext;
}

fn recorder() -> &'static Mutex<VecDeque<SpanRecord>> {
    static SPANS: OnceLock<Mutex<VecDeque<SpanRecord>>> = OnceLock::new();
    SPANS.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// The span of the running task, if any.
#[must_use]
pub fn current_span() -> Option<TraceContext> {
    CURRENT.try_with(Clone::clone).ok()
}

fn new_trace_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn new_span_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_owned()
}

/// A child of `parent` (else the current span, else a new
/// trace), recorded with privacy-filtered attributes.
#[must_use]
pub fn start_span(
    name: &str,
    parent: Option<&TraceContext>,
    attributes: &Map<String, Value>,
) -> TraceContext {
    let parent = parent.cloned().or_else(current_span);
    let context = TraceContext {
        trace_id: parent
            .as_ref()
            .map_or_else(new_trace_id, |p| p.trace_id.clone()),
        span_id: new_span_id(),
        parent_span_id: parent.map(|p| p.span_id),
    };
    let attributes = apply_privacy(attributes, privacy_mode());
    tracing::debug!(span = name, trace_id = %context.trace_id, span_id = %context.span_id, "span started");
    if let Ok(mut spans) = recorder().lock() {
        spans.push_back(SpanRecord {
            name: name.to_owned(),
            context: context.clone(),
            attributes,
        });
        while spans.len() > KEPT_SPANS {
            spans.pop_front();
        }
    }
    context
}

/// Run `fut` inside a new span (the `with start_span(...)` block).
pub async fn in_span<F: Future>(
    name: &str,
    parent: Option<&TraceContext>,
    attributes: Map<String, Value>,
    fut: F,
) -> F::Output {
    let context = start_span(name, parent, &attributes);
    CURRENT.scope(context, fut).await
}

/// Run `fut` with `parent` as the current span,
/// opening no span.
pub async fn with_parent<F: Future>(parent: Option<TraceContext>, fut: F) -> F::Output {
    match parent {
        Some(parent) => CURRENT.scope(parent, fut).await,
        None => fut.await,
    }
}

/// Recorded spans of one trace, oldest first.
#[must_use]
pub fn recorded_spans(trace_id: &str) -> Vec<SpanRecord> {
    recorder()
        .lock()
        .map(|spans| {
            spans
                .iter()
                .filter(|s| s.context.trace_id == trace_id)
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn children_share_the_trace_and_name_their_parent() {
        let (root, child) = in_span(SPAN_EXECUTION, None, Map::new(), async {
            let root = current_span().unwrap();
            let child = in_span(SPAN_MODEL, None, Map::new(), async {
                current_span().unwrap()
            })
            .await;
            (root, child)
        })
        .await;
        assert_eq!(child.trace_id, root.trace_id);
        assert_eq!(child.parent_span_id.as_deref(), Some(root.span_id.as_str()));
        assert!(current_span().is_none(), "the span ends with its block");
        let names: Vec<String> = recorded_spans(&root.trace_id)
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(names, [SPAN_EXECUTION, SPAN_MODEL]);
        assert_eq!(root.trace_id.len(), 32);
        assert_eq!(root.span_id.len(), 16);
    }

    #[tokio::test]
    async fn an_extracted_parent_continues_the_trace() {
        let parent = TraceContext {
            trace_id: "a".repeat(32),
            span_id: "b".repeat(16),
            parent_span_id: None,
        };
        let child = with_parent(Some(parent.clone()), async {
            in_span(SPAN_EXECUTION, None, Map::new(), async {
                current_span().unwrap()
            })
            .await
        })
        .await;
        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
    }
}
