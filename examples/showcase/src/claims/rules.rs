//! The claim review's rules, as pure functions: coverage rules, fraud
//! signals and the payout. `claim_review` ([`crate::claims::review`]) and the
//! tools use these; nothing here touches a store.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::claims::policies::{parse_date, Policy};
use crate::records::Record;

/// The severities a damage estimate is graded in.
pub const SEVERITIES: [&str; 4] = ["minor", "moderate", "severe", "total_loss"];
/// The kinds of loss a claim can be for.
pub const LOSS_TYPES: [&str; 2] = ["collision", "comprehensive"];
/// A loss this soon after the policy started is a fraud signal.
pub const NEW_POLICY_DAYS: i64 = 30;
/// An estimate over this multiple of the typical cost is a fraud signal.
pub const TYPICAL_COST_FACTOR: f64 = 1.5;

/// One coverage rule's outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub rule: String,
    pub value: String,
    pub limit: String,
    pub pass: bool,
}

/// One fraud signal: `fired` means it needs a person's look.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signal {
    pub signal: String,
    pub detail: String,
    pub fired: bool,
}

/// The repair estimate on a claim.
#[derive(Debug, Clone, PartialEq)]
pub struct Estimate {
    pub amount: f64,
    pub severity: Option<String>,
}

/// What the rules found on a claim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assessment {
    pub findings: Vec<Finding>,
    pub signals: Vec<Signal>,
    pub estimate: Option<f64>,
    pub deductible: f64,
    pub limit: f64,
    /// Estimate minus deductible, capped at the limit; `None` with no
    /// estimate.
    pub payable: Option<f64>,
}

impl Assessment {
    /// Rules that failed.
    #[must_use]
    pub fn failed(&self) -> usize {
        self.findings.iter().filter(|f| !f.pass).count()
    }

    /// Signals that fired.
    #[must_use]
    pub fn fired(&self) -> usize {
        self.signals.iter().filter(|s| s.fired).count()
    }

    /// Whether a rule failed or a signal fired.
    #[must_use]
    pub fn has_exceptions(&self) -> bool {
        self.failed() + self.fired() > 0
    }
}

/// The typical repair cost for a severity.
#[must_use]
pub fn typical_cost(severity: &str) -> Option<f64> {
    match severity {
        "minor" => Some(1_500.0),
        "moderate" => Some(4_000.0),
        "severe" => Some(9_000.0),
        "total_loss" => Some(25_000.0),
        _ => None,
    }
}

/// `x` rounded to whole cents.
#[must_use]
pub fn cents(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// `$1,234.50`.
#[must_use]
pub fn dollars(x: f64) -> String {
    let cents = (x.abs() * 100.0).round();
    let whole = format!("{:.0}", (cents / 100.0).floor());
    let mut grouped = String::new();
    for (i, c) in whole.chars().enumerate() {
        if i > 0 && (whole.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    let frac = cents % 100.0;
    let sign = if x < 0.0 { "-" } else { "" };
    format!("{sign}${grouped}.{frac:02.0}")
}

/// A number from JSON that may be text like `"$3,850"`.
#[must_use]
pub fn number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.replace([',', '$'], "").trim().parse().ok()))
}

/// The kind of loss; `collision` when the claim does not say.
#[must_use]
pub fn loss_type(claim: &Record) -> &str {
    claim
        .text("loss_type")
        .filter(|t| LOSS_TYPES.contains(t))
        .unwrap_or("collision")
}

/// The repair estimate: the claim's own `estimated_repair_cost` and
/// `severity`, else what the newest damage document was read as.
#[must_use]
pub fn estimate_of(claim: &Record) -> Option<Estimate> {
    let positive = |v: Option<&Value>| v.and_then(number).filter(|a| *a > 0.0);
    let severity = |v: Option<&Value>| {
        v.and_then(Value::as_str)
            .filter(|s| SEVERITIES.contains(s))
            .map(str::to_owned)
    };
    if let Some(amount) = positive(claim.field("estimated_repair_cost")) {
        return Some(Estimate {
            amount,
            severity: severity(claim.field("severity")),
        });
    }
    claim
        .field("documents")?
        .as_array()?
        .iter()
        .rev()
        .find_map(|d| {
            let read = d.get("extracted")?;
            Some(Estimate {
                amount: positive(read.get("estimated_repair_cost"))?,
                severity: severity(claim.field("severity")).or_else(|| severity(read.get("severity"))),
            })
        })
}

/// What the insurer pays on an estimate: the estimate minus the deductible,
/// never below zero.
#[must_use]
pub fn payout_for(estimate: f64, deductible: f64) -> f64 {
    (estimate - deductible).max(0.0)
}

/// Whether the claim names another party (so a police report is expected).
fn two_party(claim: &Record) -> bool {
    claim.text("other_party").is_some_and(|p| !blank(p))
}

/// Text that says nothing: empty, "none", "n/a".
fn blank(text: &str) -> bool {
    matches!(
        text.trim().to_lowercase().as_str(),
        "" | "none" | "no" | "n/a" | "na" | "not filed" | "single vehicle" | "no other party"
    )
}

/// Whether the claim has a police report number.
#[must_use]
pub fn has_police_report(claim: &Record) -> bool {
    claim.text("police_report").is_some_and(|r| !blank(r))
}

/// Apply the coverage rules and the fraud signals to `claim` under `policy`.
#[must_use]
pub fn assess(claim: &Record, policy: Option<&Policy>) -> Assessment {
    let Some(policy) = policy else {
        return Assessment {
            findings: vec![Finding {
                rule: "policy on file".into(),
                value: claim.text("policy_number").unwrap_or("none").into(),
                limit: "a Harbor Mutual policy".into(),
                pass: false,
            }],
            signals: Vec::new(),
            estimate: estimate_of(claim).map(|e| e.amount),
            deductible: 0.0,
            limit: 0.0,
            payable: None,
        };
    };
    let kind = loss_type(claim);
    let (deductible, limit) = (policy.deductible(kind), policy.limit(kind));
    let estimate = estimate_of(claim);
    let loss = claim.text("loss_date").and_then(parse_date);
    let term = policy.term();
    let description = claim.text("description").unwrap_or_default();
    let excluded = policy.excluded_by(description);
    let payout = estimate.as_ref().map(|e| payout_for(e.amount, deductible));

    let active = match (loss, term) {
        (Some(loss), Some((start, end))) => loss >= start && loss <= end,
        _ => false,
    };
    let findings = vec![
        Finding {
            rule: "policy active on the loss date".into(),
            value: claim.text("loss_date").unwrap_or("no loss date").into(),
            limit: format!("{} to {}", policy.start, policy.end),
            pass: active,
        },
        Finding {
            rule: "loss type covered".into(),
            value: kind.into(),
            limit: policy.coverages.join(" or "),
            pass: policy.coverages.contains(&kind),
        },
        Finding {
            rule: "cause not excluded".into(),
            value: excluded.map_or_else(|| "none found".into(), |e| e.name.into()),
            limit: "no excluded cause".into(),
            pass: excluded.is_none(),
        },
        Finding {
            rule: "estimate on file".into(),
            value: estimate.as_ref().map_or_else(|| "none".into(), |e| dollars(e.amount)),
            limit: "a repair estimate".into(),
            pass: estimate.is_some(),
        },
        Finding {
            rule: "estimate minus deductible within the limit".into(),
            value: payout.map_or_else(|| "n/a".into(), dollars),
            limit: format!("at most {} (deductible {})", dollars(limit), dollars(deductible)),
            pass: payout.is_some_and(|p| p <= limit),
        },
    ];

    let days = loss.zip(term).map(|(loss, (start, _))| (loss - start).num_days());
    let typical = estimate
        .as_ref()
        .and_then(|e| e.severity.as_deref())
        .and_then(|s| typical_cost(s).map(|t| (s, t)));
    let signals = vec![
        Signal {
            signal: format!("loss within {NEW_POLICY_DAYS} days of the policy start"),
            detail: days.map_or_else(|| "no loss date".into(), |d| format!("{d} days after the start")),
            fired: days.is_some_and(|d| (0..=NEW_POLICY_DAYS).contains(&d)),
        },
        Signal {
            signal: "no police report for a two-party collision".into(),
            detail: if !two_party(claim) {
                "no other party named".into()
            } else if has_police_report(claim) {
                format!("report {}", claim.text("police_report").unwrap_or_default())
            } else {
                format!("{} named, no report", claim.text("other_party").unwrap_or_default())
            },
            fired: kind == "collision" && two_party(claim) && !has_police_report(claim),
        },
        Signal {
            signal: format!("estimate over {TYPICAL_COST_FACTOR}x the typical cost"),
            detail: match (&estimate, typical) {
                (Some(e), Some((severity, t))) => {
                    format!("{} against {} typical for {severity}", dollars(e.amount), dollars(t))
                }
                _ => "no severity to compare".into(),
            },
            fired: matches!((&estimate, typical), (Some(e), Some((_, t))) if e.amount > t * TYPICAL_COST_FACTOR),
        },
    ];
    Assessment {
        findings,
        signals,
        estimate: estimate.map(|e| e.amount),
        deductible,
        limit,
        payable: payout.map(|p| p.min(limit)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claims::personas::LEO;
    use crate::claims::policies::policy_of;
    use serde_json::json;

    fn claim(data: &Value) -> Record {
        let mut claim = Record::new("claims", "claim", LEO, None, "Rear bumper");
        claim.merge(data.as_object().unwrap());
        claim
    }

    fn clean() -> Value {
        json!({
            "policy_number": "AUT-55120", "loss_type": "collision", "loss_date": "2026-10-02",
            "description": "Rear-ended at a red light", "other_party": "Driver of a Civic",
            "police_report": "PR-1", "estimated_repair_cost": 3722.5, "severity": "moderate",
        })
    }

    #[test]
    fn payout_is_the_estimate_minus_the_deductible() {
        assert!((payout_for(3722.5, 500.0) - 3222.5).abs() < f64::EPSILON);
        assert!(payout_for(300.0, 500.0).abs() < f64::EPSILON, "never below zero");
        assert_eq!(dollars(3222.5), "$3,222.50");
        assert_eq!(dollars(25_000.0), "$25,000.00");
    }

    #[test]
    fn a_clean_claim_has_no_exceptions_and_a_payable_amount() {
        let a = assess(&claim(&clean()), policy_of(LEO));
        assert!(!a.has_exceptions(), "{a:?}");
        assert_eq!(a.payable, Some(3222.5));
    }

    #[test]
    fn each_rule_and_signal_fires_on_its_own_case() {
        let policy = policy_of(LEO);
        let mut data = clean();
        data["loss_date"] = json!("2025-12-01");
        assert!(!assess(&claim(&data), policy).findings[0].pass, "before the term");

        let mut data = clean();
        data["description"] = json!("Hit a wall during a street race");
        assert!(!assess(&claim(&data), policy).findings[2].pass, "excluded cause");

        let mut data = clean();
        data["estimated_repair_cost"] = json!(0);
        let a = assess(&claim(&data), policy);
        assert!(!a.findings[3].pass && a.payable.is_none(), "no estimate");

        let mut data = clean();
        data["estimated_repair_cost"] = json!(26_000);
        data["severity"] = json!("total_loss");
        assert!(!assess(&claim(&data), policy).findings[4].pass, "over the limit");

        let mut data = clean();
        data["loss_date"] = json!("2026-02-01");
        assert!(assess(&claim(&data), policy).signals[0].fired, "17 days after the start");

        let mut data = clean();
        data["police_report"] = json!("");
        assert!(assess(&claim(&data), policy).signals[1].fired, "two parties, no report");
        data["other_party"] = json!("none");
        assert!(!assess(&claim(&data), policy).signals[1].fired, "single vehicle");

        let mut data = clean();
        data["estimated_repair_cost"] = json!(6_500);
        assert!(assess(&claim(&data), policy).signals[2].fired, "over 1.5 x $4,000");
    }

    #[test]
    fn the_estimate_falls_back_to_the_newest_document() {
        let c = claim(&json!({"documents": [
            {"document_type": "damage_photo", "extracted": {"estimated_repair_cost": 900, "severity": "minor"}},
            {"document_type": "damage_photo", "extracted": {"estimated_repair_cost": "$3,722.50", "severity": "moderate"}},
        ]}));
        let e = estimate_of(&c).unwrap();
        assert!((e.amount - 3722.5).abs() < f64::EPSILON);
        assert_eq!(e.severity.as_deref(), Some("moderate"));
    }
}
