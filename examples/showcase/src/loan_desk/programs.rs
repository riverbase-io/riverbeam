//! Loan programs and the seeded borrower profiles (mock data, stable ids).

use serde::Serialize;
use serde_json::{json, Value};

/// One loan program's factual criteria.
#[derive(Debug, Clone, Serialize)]
pub struct Program {
    pub id: &'static str,
    pub name: &'static str,
    pub summary: &'static str,
    pub min_credit_score: u32,
    /// Largest loan-to-value, in percent.
    pub max_ltv_pct: u32,
    /// Largest debt-to-income, in percent.
    pub max_dti_pct: u32,
    pub min_down_payment_pct: u32,
    pub max_loan: u64,
}

/// The programs this lender offers.
pub const PROGRAMS: [Program; 4] = [
    Program {
        id: "conventional",
        name: "Conventional 30-year fixed",
        summary:
            "For borrowers with steady income and good credit; mortgage insurance below 20% down.",
        min_credit_score: 620,
        max_ltv_pct: 97,
        max_dti_pct: 45,
        min_down_payment_pct: 3,
        max_loan: 766_550,
    },
    Program {
        id: "fha",
        name: "FHA 30-year fixed",
        summary:
            "Government-insured; lower credit and down payment, with upfront and annual insurance.",
        min_credit_score: 580,
        max_ltv_pct: 96,
        max_dti_pct: 50,
        min_down_payment_pct: 4,
        max_loan: 498_257,
    },
    Program {
        id: "va",
        name: "VA 30-year fixed",
        summary:
            "For eligible veterans and service members; no down payment and no mortgage insurance.",
        min_credit_score: 600,
        max_ltv_pct: 100,
        max_dti_pct: 41,
        min_down_payment_pct: 0,
        max_loan: 1_500_000,
    },
    Program {
        id: "jumbo",
        name: "Jumbo 30-year fixed",
        summary: "For loans above the conforming limit; strong credit and reserves.",
        min_credit_score: 700,
        max_ltv_pct: 80,
        max_dti_pct: 43,
        min_down_payment_pct: 20,
        max_loan: 3_000_000,
    },
];

/// The program called `id`.
#[must_use]
pub fn program(id: &str) -> Option<&'static Program> {
    PROGRAMS.iter().find(|p| p.id == id)
}

/// Fields an application needs before it can be submitted.
pub const REQUIRED_FIELDS: [&str; 9] = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "employer",
    "gross_monthly_income",
    "monthly_debts",
    "purchase_price",
    "down_payment",
];

/// The borrower profile on file for `user_id` (what the lender already knows).
#[must_use]
pub fn borrower_profile(user_id: &str) -> Option<Value> {
    match user_id {
        crate::loan_desk::personas::BOB => Some(json!({
            "first_name": "Bob",
            "last_name": "Tran",
            "email": "bob@example.com",
            "phone": "+1 555 0142",
            "employer": "Riverside Logistics",
            "employment_years": 4,
            "residence": "renting",
            "veteran": false,
        })),
        crate::loan_desk::personas::DANA => Some(json!({
            "first_name": "Dana",
            "last_name": "Nguyen",
            "email": "dana@example.com",
            "phone": "+1 555 0177",
            "employer": "US Navy (retired)",
            "employment_years": 12,
            "residence": "owner",
            "veteran": true,
        })),
        _ => None,
    }
}
