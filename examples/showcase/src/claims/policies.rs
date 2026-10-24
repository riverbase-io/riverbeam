//! The policies Harbor Mutual holds, as in-module data (the way the loan
//! desk keeps its programs): one for Leo and one for Nina, whose claim is
//! seeded.

use chrono::NaiveDate;
use serde_json::{json, Value};

use crate::claims::personas::{LEO, NINA};

/// A cause of loss a policy does not cover, and the words that signal it in
/// a claim's description.
#[derive(Debug, Clone, Copy)]
pub struct Exclusion {
    pub name: &'static str,
    pub keywords: &'static [&'static str],
}

/// One auto policy.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    pub number: &'static str,
    /// The policyholder's user id.
    pub holder: &'static str,
    pub holder_name: &'static str,
    pub vehicle: &'static str,
    /// First and last day of the term, `YYYY-MM-DD`.
    pub start: &'static str,
    pub end: &'static str,
    /// `collision`, `comprehensive`.
    pub coverages: &'static [&'static str],
    pub collision_deductible: f64,
    pub collision_limit: f64,
    pub comprehensive_deductible: f64,
    pub comprehensive_limit: f64,
    pub rental_per_day: f64,
    pub rental_max_days: u32,
    pub exclusions: &'static [Exclusion],
}

/// What every Harbor Mutual auto policy excludes.
pub const EXCLUSIONS: [Exclusion; 4] = [
    Exclusion {
        name: "racing",
        keywords: &["racing", "race track", "street race", "drag race", "time trial"],
    },
    Exclusion {
        name: "commercial or rideshare use",
        keywords: &["rideshare", "uber", "lyft", "delivery job", "food delivery", "commercial", "taxi"],
    },
    Exclusion {
        name: "intentional damage",
        keywords: &["intentional", "on purpose", "deliberate"],
    },
    Exclusion {
        name: "wear and tear",
        keywords: &["wear and tear", "worn out", "rusted", "corrosion", "gradual"],
    },
];

pub const POLICIES: [Policy; 2] = [
    Policy {
        number: "AUT-55120",
        holder: LEO,
        holder_name: "Leo Park",
        vehicle: "2021 Subaru Outback",
        start: "2026-01-15",
        end: "2027-01-15",
        coverages: &["collision", "comprehensive"],
        collision_deductible: 500.0,
        collision_limit: 25_000.0,
        comprehensive_deductible: 250.0,
        comprehensive_limit: 25_000.0,
        rental_per_day: 40.0,
        rental_max_days: 10,
        exclusions: &EXCLUSIONS,
    },
    Policy {
        number: "AUT-48377",
        holder: NINA,
        holder_name: "Nina Alvarez",
        vehicle: "2019 Toyota RAV4",
        start: "2024-06-15",
        end: "2027-06-15",
        coverages: &["collision", "comprehensive"],
        collision_deductible: 500.0,
        collision_limit: 20_000.0,
        comprehensive_deductible: 250.0,
        comprehensive_limit: 20_000.0,
        rental_per_day: 35.0,
        rental_max_days: 14,
        exclusions: &EXCLUSIONS,
    },
];

impl Policy {
    /// The deductible for `loss_type` (`collision` or `comprehensive`).
    #[must_use]
    pub fn deductible(&self, loss_type: &str) -> f64 {
        if loss_type == "comprehensive" {
            self.comprehensive_deductible
        } else {
            self.collision_deductible
        }
    }

    /// The most the policy pays for `loss_type`.
    #[must_use]
    pub fn limit(&self, loss_type: &str) -> f64 {
        if loss_type == "comprehensive" {
            self.comprehensive_limit
        } else {
            self.collision_limit
        }
    }

    /// The term as dates; `None` when the data is malformed.
    #[must_use]
    pub fn term(&self) -> Option<(NaiveDate, NaiveDate)> {
        Some((parse_date(self.start)?, parse_date(self.end)?))
    }

    /// The exclusion whose words appear in `description`, if any.
    #[must_use]
    pub fn excluded_by(&self, description: &str) -> Option<&'static Exclusion> {
        let text = description.to_lowercase();
        self.exclusions
            .iter()
            .find(|e| e.keywords.iter().any(|k| text.contains(k)))
    }

    /// The policy as the agents and the UI read it.
    #[must_use]
    pub fn view(&self) -> Value {
        json!({
            "policy_number": self.number,
            "holder": self.holder_name,
            "vehicle": self.vehicle,
            "term": {"start": self.start, "end": self.end},
            "coverages": self.coverages,
            "collision": {"deductible": self.collision_deductible, "limit": self.collision_limit},
            "comprehensive": {"deductible": self.comprehensive_deductible, "limit": self.comprehensive_limit},
            "rental": {"per_day": self.rental_per_day, "max_days": self.rental_max_days},
            "exclusions": self.exclusions.iter().map(|e| e.name).collect::<Vec<_>>(),
        })
    }
}

/// `YYYY-MM-DD` as a date.
#[must_use]
pub fn parse_date(text: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d").ok()
}

/// The policy numbered `number`.
#[must_use]
pub fn policy(number: &str) -> Option<&'static Policy> {
    POLICIES.iter().find(|p| p.number.eq_ignore_ascii_case(number.trim()))
}

/// The policy held by `user_id`.
#[must_use]
pub fn policy_of(user_id: &str) -> Option<&'static Policy> {
    POLICIES.iter().find(|p| p.holder == user_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leos_policy_has_the_documented_terms() {
        let p = policy_of(LEO).unwrap();
        assert_eq!(p.number, "AUT-55120");
        assert!((p.collision_deductible - 500.0).abs() < f64::EPSILON);
        assert!((p.collision_limit - 25_000.0).abs() < f64::EPSILON);
        assert_eq!(p.rental_max_days, 10);
        assert!(p.term().is_some());
        assert_eq!(policy("aut-48377").unwrap().holder, NINA);
    }

    #[test]
    fn exclusions_match_on_words_in_the_description() {
        let p = policy_of(LEO).unwrap();
        assert_eq!(p.excluded_by("Hit while on a rideshare trip").unwrap().name, "commercial or rideshare use");
        assert!(p.excluded_by("Rear-ended at a light").is_none());
    }
}
