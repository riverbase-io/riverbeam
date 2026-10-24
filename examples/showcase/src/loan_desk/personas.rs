//! Dev personas: fixed mock users, their role and usage plan. The UI signs
//! requests as one of them with riverbase's mock auth.

use serde::Serialize;

/// The lender's organization; every persona belongs to it.
pub const ORG: &str = "1c0a7e52-6d1f-4c3b-9a2e-5b8d7f6e4a10";
/// Borrower on the free plan.
pub const BOB: &str = "b0b00000-0000-4000-8000-000000000002";
/// Veteran borrower on the pro plan.
pub const DANA: &str = "da7a0000-0000-4000-8000-000000000004";
/// Loan officer.
pub const CAROL: &str = "ca201000-0000-4000-8000-000000000003";
/// Admin.
pub const JOHN: &str = "44d2f8cb-0d46-4323-95b9-c5b4bdbf6205";

/// What a persona may do in the loan desk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Borrower,
    Officer,
    Admin,
}

/// The riverbase role an officer carries besides `beam-user`.
pub const OFFICER_ROLE: &str = "loan-officer";

#[derive(Debug, Clone, Serialize)]
pub struct Persona {
    pub key: &'static str,
    pub user_id: &'static str,
    pub name: &'static str,
    pub role: Role,
    /// The usage plan (`plans`).
    pub plan: &'static str,
    /// The riverbase roles the UI signs with.
    pub roles: &'static [&'static str],
}

pub const PERSONAS: [Persona; 4] = [
    Persona {
        key: "bob",
        user_id: BOB,
        name: "Bob Tran",
        role: Role::Borrower,
        plan: "free",
        roles: &["beam-user"],
    },
    Persona {
        key: "dana",
        user_id: DANA,
        name: "Dana Nguyen",
        role: Role::Borrower,
        plan: "pro",
        roles: &["beam-user"],
    },
    Persona {
        key: "carol",
        user_id: CAROL,
        name: "Carol Le",
        role: Role::Officer,
        plan: "pro",
        roles: &["beam-user", OFFICER_ROLE],
    },
    Persona {
        key: "john",
        user_id: JOHN,
        name: "John Doe",
        role: Role::Admin,
        plan: "enterprise",
        roles: &["beam-user", "beam-admin"],
    },
];

/// The role riverbase roles grant: admin over officer over borrower.
#[must_use]
pub fn role_of(roles: &[String]) -> Role {
    if roles.iter().any(|r| r == "beam-admin") {
        Role::Admin
    } else if roles.iter().any(|r| r == OFFICER_ROLE) {
        Role::Officer
    } else {
        Role::Borrower
    }
}

/// The persona with this user id.
#[must_use]
pub fn persona(user_id: &str) -> Option<&'static Persona> {
    PERSONAS.iter().find(|p| p.user_id == user_id)
}
