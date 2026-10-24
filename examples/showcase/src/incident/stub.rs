//! The platform team's monitoring as an MCP server (`/monitoring/mcp`):
//! firing alerts, metric series and recent deploys. The data is fixed, so
//! runs are repeatable: at 14:02 UTC checkout-api v2.14.0 is deployed, and
//! from 14:05 its latency and error rate are far above their baseline.

use serde_json::{json, Value};

use crate::mcp_stub::{required, StubResult, StubServer, StubTool};

/// The times of a metric series, every five minutes (UTC).
const TIMES: [&str; 12] = [
    "13:30", "13:35", "13:40", "13:45", "13:50", "13:55", "14:00", "14:05", "14:10", "14:15", "14:20", "14:25",
];
/// How many leading points of a series are before the 14:02 deploy.
const BEFORE_DEPLOY: usize = 7;

/// The metrics a series exists for.
pub const METRICS: [&str; 4] = ["latency_p99", "error_rate", "cpu", "rps"];

/// Every alert firing now.
#[must_use]
pub fn alerts_table() -> Vec<Value> {
    vec![
        json!({"alert_id": "ALR-4411", "service": "checkout-api", "severity": "critical",
            "summary": "checkout-api: p99 latency 2.8s (SLO 800ms) and 5xx rate 6.2% since 14:05 UTC",
            "firing_since": "14:05 UTC"}),
        json!({"alert_id": "ALR-4412", "service": "search-api", "severity": "warning",
            "summary": "search-api: CPU above 80% on the search pool since 14:15 UTC",
            "firing_since": "14:15 UTC"}),
    ]
}

/// The baseline of a metric and its five values from 14:05 on.
fn shape(service: &str, metric: &str) -> Option<(f64, [f64; 5])> {
    let flat = |v: f64| Some((v, [v; 5]));
    match (service, metric) {
        ("checkout-api", "latency_p99") => Some((310.0, [2400.0, 2800.0, 2850.0, 2800.0, 2790.0])),
        ("checkout-api", "error_rate") => Some((0.2, [5.8, 6.2, 6.4, 6.2, 6.1])),
        ("checkout-api", "cpu") => Some((38.0, [45.0, 52.0, 55.0, 56.0, 55.0])),
        ("checkout-api", "rps") => Some((820.0, [790.0, 700.0, 640.0, 630.0, 640.0])),
        ("search-api", "cpu") => Some((55.0, [66.0, 74.0, 81.0, 86.0, 88.0])),
        ("search-api", "latency_p99") => flat(120.0),
        ("search-api", "error_rate") => flat(0.1),
        ("search-api", "rps") => flat(1500.0),
        _ => None,
    }
}

/// The metric series of `service`: `(time, value)` for each point.
#[must_use]
pub fn series(service: &str, metric: &str) -> Option<Vec<(&'static str, f64)>> {
    let (baseline, after) = shape(service, metric)?;
    Some(
        TIMES
            .iter()
            .enumerate()
            .map(|(i, at)| (*at, if i < BEFORE_DEPLOY { baseline } else { after[i - BEFORE_DEPLOY] }))
            .collect(),
    )
}

/// The deploys of `service`, newest first.
#[must_use]
pub fn deploys_of(service: &str) -> Option<Vec<Value>> {
    match service {
        "checkout-api" => Some(vec![
            json!({"version": "v2.14.0", "deployed_at": "today 14:02 UTC", "by": "ci-pipeline",
                "change": "switch payment client to connection pool v3"}),
            json!({"version": "v2.13.4", "deployed_at": "yesterday 16:20 UTC", "by": "ci-pipeline",
                "change": "fix rounding of discounted totals"}),
        ]),
        "search-api" => Some(vec![json!({"version": "v5.8.1", "deployed_at": "3 days ago 10:10 UTC",
            "by": "ci-pipeline", "change": "raise the result cache size"})]),
        _ => None,
    }
}

#[allow(clippy::unnecessary_wraps)] // A stub tool's signature.
fn get_alerts(args: &Value) -> StubResult {
    let service = args["service"].as_str().map(str::trim).filter(|s| !s.is_empty());
    let alerts: Vec<Value> = alerts_table()
        .into_iter()
        .filter(|a| service.is_none_or(|s| a["service"].as_str() == Some(s)))
        .collect();
    Ok(json!({"alerts": alerts}))
}

/// The number of minutes in a window such as `30m` or `1h`.
fn window_minutes(window: &str) -> Option<usize> {
    if let Some(m) = window.strip_suffix('m') {
        m.parse().ok()
    } else {
        window.strip_suffix('h')?.parse::<usize>().ok().map(|h| h * 60)
    }
}

fn query_metrics(args: &Value) -> StubResult {
    let service = required(args, "service")?;
    let metric = required(args, "metric")?;
    let window = args["window"].as_str().map(str::trim).filter(|w| !w.is_empty()).unwrap_or("1h");
    let minutes = window_minutes(window).ok_or_else(|| format!("window {window} is not like 30m or 1h"))?;
    let all = series(service, metric).ok_or_else(|| {
        format!("no metric {metric} for {service}; metrics are {}", METRICS.join(", "))
    })?;
    let keep = (minutes / 5).clamp(1, all.len());
    let points: Vec<Value> = all[all.len() - keep..]
        .iter()
        .map(|(at, value)| json!({"at": format!("{at} UTC"), "value": value}))
        .collect();
    let unit = match metric {
        "latency_p99" => "ms",
        "error_rate" | "cpu" => "percent",
        _ => "requests/s",
    };
    Ok(json!({"service": service, "metric": metric, "unit": unit, "window": window, "points": points}))
}

fn recent_deploys(args: &Value) -> StubResult {
    let service = required(args, "service")?;
    let deploys = deploys_of(service).ok_or_else(|| format!("no service {service}"))?;
    Ok(json!({"service": service, "deploys": deploys}))
}

/// The monitoring tools.
#[must_use]
pub fn monitoring() -> StubServer {
    StubServer {
        name: "monitoring",
        tools: vec![
            StubTool {
                name: "get_alerts",
                description: "The alerts firing now (id, service, severity, summary, since when); optionally for one service.",
                input_schema: json!({"type": "object", "properties": {"service": {"type": "string"}}}),
                call: get_alerts,
            },
            StubTool {
                name: "query_metrics",
                description: "A service's metric series, every five minutes in UTC. metric is latency_p99 (ms), \
                              error_rate (percent), cpu (percent) or rps; window is like 30m or 1h.",
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "service": {"type": "string"},
                        "metric": {"type": "string", "enum": METRICS},
                        "window": {"type": "string"}
                    },
                    "required": ["service", "metric"]
                }),
                call: query_metrics,
            },
            StubTool {
                name: "recent_deploys",
                description: "A service's recent deploys, newest first: version, time, who, and the change.",
                input_schema: json!({"type": "object", "properties": {"service": {"type": "string"}}, "required": ["service"]}),
                call: recent_deploys,
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkout_is_firing_and_search_warns() {
        let all = get_alerts(&json!({})).unwrap();
        assert_eq!(all["alerts"].as_array().unwrap().len(), 2);
        let one = get_alerts(&json!({"service": "checkout-api"})).unwrap();
        let alert = &one["alerts"][0];
        assert_eq!(alert["severity"], "critical");
        assert!(alert["summary"].as_str().unwrap().contains("p99 latency 2.8s (SLO 800ms) and 5xx rate 6.2%"));
        assert_eq!(get_alerts(&json!({"service": "search-api"})).unwrap()["alerts"][0]["severity"], "warning");
    }

    #[test]
    fn checkout_metrics_are_flat_before_the_deploy_and_spike_after() {
        for metric in ["latency_p99", "error_rate"] {
            let points = series("checkout-api", metric).unwrap();
            let (before, after) = points.split_at(BEFORE_DEPLOY);
            assert!(before.iter().all(|(_, v)| (*v - before[0].1).abs() < f64::EPSILON), "{metric} is flat");
            assert!(after.iter().all(|(_, v)| *v > before[0].1 * 5.0), "{metric} spikes");
        }
        let answer = query_metrics(&json!({"service": "checkout-api", "metric": "latency_p99", "window": "15m"})).unwrap();
        assert_eq!(answer["points"].as_array().unwrap().len(), 3);
        assert_eq!(answer["unit"], "ms");
    }

    #[test]
    fn a_bad_query_names_what_is_wrong() {
        let err = query_metrics(&json!({"service": "checkout-api", "metric": "disk"})).unwrap_err();
        assert!(err.contains("no metric disk"), "{err}");
        let err = query_metrics(&json!({"service": "checkout-api", "metric": "cpu", "window": "soon"})).unwrap_err();
        assert!(err.contains("window"), "{err}");
        assert!(recent_deploys(&json!({"service": "billing"})).is_err());
    }

    #[test]
    fn the_bad_deploy_is_listed_first() {
        let deploys = recent_deploys(&json!({"service": "checkout-api"})).unwrap();
        assert_eq!(deploys["deploys"][0]["version"], "v2.14.0");
        assert_eq!(deploys["deploys"][0]["deployed_at"], "today 14:02 UTC");
        assert_eq!(deploys["deploys"][0]["by"], "ci-pipeline");
        assert!(deploys["deploys"][0]["change"].as_str().unwrap().contains("connection pool v3"));
        assert_eq!(deploys["deploys"][1]["version"], "v2.13.4");
    }
}
