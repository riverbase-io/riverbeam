//! Structured audit events and pluggable sinks.
//!
//! [`AuditAction`]/[`AuditEvent`], an [`AuditSink`] trait with a tracing-style
//! sink and an in-memory sink for tests, and an [`AuditLogger`] fan-out.

mod events;
mod logger;
mod sink;

pub use events::{AuditAction, AuditEvent};
pub use logger::AuditLogger;
pub use sink::{AuditSink, InMemorySink, StdoutSink};
