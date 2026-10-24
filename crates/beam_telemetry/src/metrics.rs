//! In-process metrics (Python `telemetry/metrics.py`).

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

/// Counters, gauges and histograms in process memory.
#[derive(Debug, Default)]
pub struct InMemoryMetrics {
    counters: Mutex<BTreeMap<String, u64>>,
    gauges: Mutex<BTreeMap<String, f64>>,
    histograms: Mutex<BTreeMap<String, Vec<f64>>>,
}

/// Labels that would make cardinality unbounded.
pub const FORBIDDEN_LABELS: [&str; 11] = [
    "user",
    "user_id",
    "session",
    "session_id",
    "execution",
    "execution_id",
    "request",
    "request_id",
    "prompt",
    "tool_arguments",
    "content",
];

impl InMemoryMetrics {
    pub fn increment(&self, name: &str, amount: u64) {
        if let Ok(mut counters) = self.counters.lock() {
            *counters.entry(name.to_owned()).or_default() += amount;
        }
    }

    pub fn set_gauge(&self, name: &str, value: f64) {
        if let Ok(mut gauges) = self.gauges.lock() {
            gauges.insert(name.to_owned(), value);
        }
    }

    pub fn observe(&self, name: &str, value: f64) {
        if let Ok(mut histograms) = self.histograms.lock() {
            histograms.entry(name.to_owned()).or_default().push(value);
        }
    }

    #[must_use]
    pub fn counter(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .map_or(0, |c| c.get(name).copied().unwrap_or(0))
    }

    pub fn clear(&self) {
        for map in [&self.counters] {
            if let Ok(mut m) = map.lock() {
                m.clear();
            }
        }
        if let Ok(mut m) = self.gauges.lock() {
            m.clear();
        }
        if let Ok(mut m) = self.histograms.lock() {
            m.clear();
        }
    }
}

/// The process metrics.
#[must_use]
pub fn metrics() -> &'static InMemoryMetrics {
    static METRICS: OnceLock<InMemoryMetrics> = OnceLock::new();
    METRICS.get_or_init(InMemoryMetrics::default)
}

/// Bounded-cardinality text export.
#[must_use]
pub fn render_prometheus(source: &InMemoryMetrics) -> String {
    let mut lines = vec![
        "# TYPE beam_info gauge".to_owned(),
        "beam_info{component=\"control_plane\"} 1".to_owned(),
    ];
    if let Ok(counters) = source.counters.lock() {
        for (name, value) in counters.iter() {
            lines.push(format!("beam_{}_total {value}", name.replace('.', "_")));
        }
    }
    if let Ok(gauges) = source.gauges.lock() {
        for (name, value) in gauges.iter() {
            let safe = name.replace('.', "_");
            lines.push(format!("# TYPE beam_{safe} gauge"));
            lines.push(format!("beam_{safe} {}", py_float(*value)));
        }
    }
    lines.join("\n") + "\n"
}

/// Python `str(float)`.
fn py_float(value: f64) -> String {
    let text = value.to_string();
    if value.is_finite() && !text.contains(['.', 'e', 'E']) {
        format!("{text}.0")
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_like_python() {
        let m = InMemoryMetrics::default();
        m.increment("request_count", 1);
        m.increment("request_count", 2);
        m.increment("execution.failed", 1);
        m.set_gauge("queue.depth", 4.0);
        assert_eq!(
            render_prometheus(&m),
            "# TYPE beam_info gauge\nbeam_info{component=\"control_plane\"} 1\nbeam_execution_failed_total 1\n\
             beam_request_count_total 3\n# TYPE beam_queue_depth gauge\nbeam_queue_depth 4.0\n"
        );
    }
}
