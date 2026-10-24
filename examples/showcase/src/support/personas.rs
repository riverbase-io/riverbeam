//! The support desk's personas: two customers and a support agent of the
//! store "Northwind Goods".

use crate::registry::Persona;

/// The store's organization.
pub const ORG: &str = "5c0b5a1e-8f2d-4d3a-9b1e-2a7c6d4e5f01";
/// Customer on the free plan: a delayed parcel and a broken grinder.
pub const MIA: &str = "51a00000-0000-4000-8000-000000000011";
/// Customer on the pro plan: a wrong-colour jacket.
pub const OMAR: &str = "0a400000-0000-4000-8000-000000000013";
/// Support agent.
pub const SAM: &str = "5a300000-0000-4000-8000-000000000012";

/// The riverbase role a support agent carries besides `beam-user`.
pub const AGENT_ROLE: &str = "support-agent";

/// A customer.
pub const CUSTOMER: &str = "customer";
/// A support agent.
pub const AGENT: &str = "agent";

pub const PERSONAS: [Persona; 3] = [
    Persona {
        key: "mia",
        user_id: MIA,
        name: "Mia Chen",
        role: CUSTOMER,
        plan: "free",
        roles: &["beam-user"],
    },
    Persona {
        key: "omar",
        user_id: OMAR,
        name: "Omar Haddad",
        role: CUSTOMER,
        plan: "pro",
        roles: &["beam-user"],
    },
    Persona {
        key: "sam",
        user_id: SAM,
        name: "Sam Rivera",
        role: AGENT,
        plan: "pro",
        roles: &["beam-user", AGENT_ROLE],
    },
];

/// What the store knows about a customer: name and email (the order system
/// looks orders up by email).
#[must_use]
pub fn customer_profile(user_id: &str) -> Option<serde_json::Value> {
    match user_id {
        MIA => Some(serde_json::json!({"name": "Mia Chen", "email": "mia@example.com", "member_since": "2023-04-02"})),
        OMAR => Some(serde_json::json!({"name": "Omar Haddad", "email": "omar@example.com", "member_since": "2021-11-19"})),
        _ => None,
    }
}
