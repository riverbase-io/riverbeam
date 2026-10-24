//! The deterministic evaluators (Python `evals/evaluators.py`), verbatim:
//! exact match, task quality, structured output, citation faithfulness, tool
//! correctness, safety, latency, cost and context utilization.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use async_trait::async_trait;
use beam_types::AgentResult;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::dataset::EvaluationCase;

/// One evaluator's verdict on one case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationScore {
    pub passed: bool,
    #[serde(default)]
    pub score: Option<f64>,
    #[serde(default)]
    pub details: Map<String, Value>,
}

impl EvaluationScore {
    fn of(passed: bool) -> Self {
        Self {
            passed,
            score: Some(if passed { 1.0 } else { 0.0 }),
            details: Map::new(),
        }
    }

    fn with(mut self, key: &str, value: Value) -> Self {
        self.details.insert(key.to_owned(), value);
        self
    }
}

/// Scores one case's result.
#[async_trait]
pub trait Evaluator: Send + Sync {
    async fn evaluate(&self, case: &EvaluationCase, result: &AgentResult) -> EvaluationScore;
}

fn expected<'a>(case: &'a EvaluationCase, key: &str) -> Option<&'a Value> {
    case.expected.as_ref()?.get(key).filter(|v| !v.is_null())
}

/// Python `ExactMatchEvaluator` (and `TaskQualityEvaluator`, the same rule):
/// the expected text appears in the answer.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExactMatchEvaluator;

#[async_trait]
impl Evaluator for ExactMatchEvaluator {
    async fn evaluate(&self, case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        let passed = match expected(case, "text") {
            None => true,
            Some(Value::String(want)) => result.text_content().contains(want.as_str()),
            Some(other) => result.text_content().contains(&other.to_string()),
        };
        EvaluationScore::of(passed)
    }
}

/// The same rule as [`ExactMatchEvaluator`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TaskQualityEvaluator;

#[async_trait]
impl Evaluator for TaskQualityEvaluator {
    async fn evaluate(&self, case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        ExactMatchEvaluator.evaluate(case, result).await
    }
}

/// The answer is JSON with the expected keys.
#[derive(Debug, Clone, Copy, Default)]
pub struct StructuredOutputEvaluator;

#[async_trait]
impl Evaluator for StructuredOutputEvaluator {
    async fn evaluate(&self, case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        let Some(Value::Object(wanted)) = expected(case, "json") else {
            return EvaluationScore::of(true);
        };
        let Ok(payload) = serde_json::from_str::<Value>(&result.text_content()) else {
            return EvaluationScore::of(false).with("reason", json!("not_json"));
        };
        let passed = wanted.iter().all(|(k, v)| payload.get(k) == Some(v));
        EvaluationScore::of(passed)
    }
}

/// Every citation is an expected source.
#[derive(Debug, Clone, Copy, Default)]
pub struct CitationFaithfulnessEvaluator;

#[async_trait]
impl Evaluator for CitationFaithfulnessEvaluator {
    async fn evaluate(&self, case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        let sources: BTreeSet<String> = expected(case, "sources")
            .and_then(Value::as_array)
            .map(|s| {
                s.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        if sources.is_empty() {
            return EvaluationScore::of(true);
        }
        // Python `item.get("id") or item.get("source")`.
        let cited: BTreeSet<Option<String>> = result
            .citations
            .iter()
            .map(|c| {
                ["id", "source"]
                    .into_iter()
                    .filter_map(|k| c.get(k))
                    .find(|v| !v.is_null() && v.as_str() != Some(""))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect();
        let passed = !cited.is_empty()
            && cited
                .iter()
                .all(|c| c.as_ref().is_some_and(|c| sources.contains(c)));
        let listed: Vec<Value> = cited
            .into_iter()
            .map(|c| c.map_or(Value::Null, Value::String))
            .collect();
        EvaluationScore::of(passed).with("cited", Value::Array(listed))
    }
}

/// The tools used, in order.
#[derive(Debug, Clone, Copy, Default)]
pub struct ToolCorrectnessEvaluator;

#[async_trait]
impl Evaluator for ToolCorrectnessEvaluator {
    async fn evaluate(&self, case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        let wanted: Vec<&str> = expected(case, "tools")
            .and_then(Value::as_array)
            .map(|t| t.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if wanted.is_empty() {
            return EvaluationScore::of(true);
        }
        let used: Vec<&str> = result
            .tool_calls
            .iter()
            .map(|t| t.tool_name.as_str())
            .collect();
        EvaluationScore::of(used == wanted).with("used", json!(used))
    }
}

fn unsafe_pattern() -> &'static Regex {
    static UNSAFE: OnceLock<Regex> = OnceLock::new();
    UNSAFE.get_or_init(|| {
        Regex::new(r"(?i)(api_key\s*=|nvapi-|password\s*=|sk-[A-Za-z0-9]{8,})")
            .expect("safety pattern")
    })
}

/// No credential-looking text in the answer.
#[derive(Debug, Clone, Copy, Default)]
pub struct SafetyEvaluator;

#[async_trait]
impl Evaluator for SafetyEvaluator {
    async fn evaluate(&self, _case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        EvaluationScore::of(!unsafe_pattern().is_match(&result.text_content()))
    }
}

/// Passes when the run took at most the limit (default 500 ms).
#[derive(Debug, Clone, Copy)]
pub struct LatencyEvaluator {
    pub max_total_ms: f64,
}

impl Default for LatencyEvaluator {
    fn default() -> Self {
        Self {
            max_total_ms: 500.0,
        }
    }
}

#[async_trait]
impl Evaluator for LatencyEvaluator {
    async fn evaluate(&self, _case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        let latency = result.latency_ms.unwrap_or(0.0);
        EvaluationScore::of(latency <= self.max_total_ms).with("latency_ms", json!(latency))
    }
}

/// Python `CostEvaluator` (default 1.0): `metadata.cost`, else Python's
/// token price estimate.
#[derive(Debug, Clone, Copy)]
pub struct CostEvaluator {
    pub max_cost: f64,
}

impl Default for CostEvaluator {
    fn default() -> Self {
        Self { max_cost: 1.0 }
    }
}

#[async_trait]
impl Evaluator for CostEvaluator {
    async fn evaluate(&self, _case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        let mut cost = result
            .metadata
            .get("cost")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        if let (Some(usage), true) = (&result.token_usage, cost == 0.0) {
            cost = f64::from(usage.prompt_tokens) * 0.00002
                + f64::from(usage.completion_tokens) * 0.00006;
        }
        EvaluationScore::of(cost <= self.max_cost).with("cost", json!(cost))
    }
}

/// Passes when the context used at most a share of the window (default 0.9).
#[derive(Debug, Clone, Copy)]
pub struct ContextUtilizationEvaluator {
    pub max_input_ratio: f64,
}

impl Default for ContextUtilizationEvaluator {
    fn default() -> Self {
        Self {
            max_input_ratio: 0.9,
        }
    }
}

#[async_trait]
impl Evaluator for ContextUtilizationEvaluator {
    async fn evaluate(&self, _case: &EvaluationCase, result: &AgentResult) -> EvaluationScore {
        let manifest = result.metadata.get("manifest");
        let number = |k: &str| {
            manifest
                .and_then(|m| m.get(k))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        };
        let (window, used) = (number("window_tokens"), number("input_tokens"));
        let ratio = if window == 0.0 { 0.0 } else { used / window };
        EvaluationScore::of(ratio <= self.max_input_ratio).with("ratio", json!(ratio))
    }
}

/// An evaluator by its suite name (Python's class names, snake case).
#[must_use]
pub fn by_name(name: &str) -> Option<Box<dyn Evaluator>> {
    Some(match name {
        "exact_match" => Box::new(ExactMatchEvaluator),
        "task_quality" => Box::new(TaskQualityEvaluator),
        "structured_output" => Box::new(StructuredOutputEvaluator),
        "citation_faithfulness" => Box::new(CitationFaithfulnessEvaluator),
        "tool_correctness" => Box::new(ToolCorrectnessEvaluator),
        "safety" => Box::new(SafetyEvaluator),
        "latency" => Box::new(LatencyEvaluator::default()),
        "cost" => Box::new(CostEvaluator::default()),
        "context_utilization" => Box::new(ContextUtilizationEvaluator::default()),
        _ => return None,
    })
}
