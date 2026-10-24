//! Telemetry for riverbeam (ported from fluvius-beam `telemetry/` and
//! `kernel/redaction.py`).
//!
//! - [`propagation`]: W3C `traceparent` in an invoke's metadata.
//! - [`span`]: an in-process span tree with Python's span names; the current
//!   span follows the task (a `tokio` task-local).
//! - [`privacy`]: `metadata_only` (default), `redacted`, `full_debug`.
//! - [`metrics`]: counters and gauges with a bounded Prometheus text render.
//! - [`logging`]: one structured line per failed invoke (`BEAM_LOG_JSON`).
//! - [`redaction`]: strip credentials from dumps that leave the process.

pub mod logging;
pub mod metrics;
pub mod privacy;
pub mod propagation;
pub mod redaction;
pub mod span;

pub use metrics::{metrics, render_prometheus, InMemoryMetrics};
pub use privacy::{apply_privacy, privacy_mode, set_privacy_mode, PrivacyMode};
pub use propagation::{extract, inject, TraceContext};
pub use redaction::redact_secrets;
pub use span::{
    current_span, in_span, recorded_spans, start_span, with_parent, SpanRecord, SPAN_COMMAND,
    SPAN_EXECUTION, SPAN_MODEL, SPAN_RAG, SPAN_RUNTIME, SPAN_TOOL, SPAN_WORKER,
};
