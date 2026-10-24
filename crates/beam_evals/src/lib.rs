//! Offline evals for agent revisions (ported from fluvius-beam `evals/`).
//!
//! - [`dataset`] holds the cases.
//! - [`evaluators`] holds the nine deterministic evaluators.
//! - [`runner`] runs a dataset through an invoke function (tools in dry
//!   run) and compares runs.
//! - [`feedback`] and [`store`] hold feedback on executions and the eval store.
//!
//! rc8 dropped the suite, metrics, qualification, online sampling and
//! release modules; riverbeam removed them too.

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

pub mod dataset;
pub mod evaluators;
pub mod feedback;
pub mod runner;
pub mod store;

pub use dataset::{EvaluationCase, EvaluationDataset};
pub use evaluators::{EvaluationScore, Evaluator};
pub use feedback::AgentFeedback;
pub use runner::{ComparisonReport, EvalRunner, EvaluationResult, EvaluationRun};
pub use store::{EvaluationStore, InMemoryEvaluationStore, PgEvaluationStore};
