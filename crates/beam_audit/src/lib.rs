//! Structured audit events and pluggable sinks.
//!
//! [`AuditAction`]/[`AuditEvent`], an [`AuditSink`] trait with a tracing-style
//! sink and an in-memory sink for tests, and an [`AuditLogger`] fan-out.

mod events;
mod logger;
mod sink;

pub use events::{AuditAction, AuditEvent};
pub use logger::{apply_preview_privacy, build_audit_sinks, AuditLogger, AUDIT_SINK_KEYS};
#[cfg(feature = "postgres")]
pub use sink::SqlSink;
pub use sink::{AuditSink, FileSink, InMemorySink, StdoutSink};
