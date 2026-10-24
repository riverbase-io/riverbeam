//! The claims showcase's personas: a policyholder and an adjuster of the
//! insurer "Harbor Mutual".

use crate::registry::Persona;

/// The insurer's organization.
pub const ORG: &str = "c1a10000-0000-4000-8000-0000000000c0";
/// Policyholder on the free plan: a 2021 Subaru Outback.
pub const LEO: &str = "1e000000-0000-4000-8000-0000000000c1";
/// Claims adjuster.
pub const AVA: &str = "a7a00000-0000-4000-8000-0000000000c2";
/// A policyholder who is not a persona: the seeded claim is hers.
pub const NINA: &str = "0a1ba000-0000-4000-8000-0000000000c3";

/// The riverbase role an adjuster carries besides `beam-user`.
pub const ADJUSTER_ROLE: &str = "claims-adjuster";

/// A policyholder.
pub const POLICYHOLDER: &str = "policyholder";
/// A claims adjuster.
pub const ADJUSTER: &str = "adjuster";

pub const PERSONAS: [Persona; 2] = [
    Persona {
        key: "leo",
        user_id: LEO,
        name: "Leo Park",
        role: POLICYHOLDER,
        plan: "free",
        roles: &["beam-user"],
    },
    Persona {
        key: "ava",
        user_id: AVA,
        name: "Ava Brooks",
        role: ADJUSTER,
        plan: "pro",
        roles: &["beam-user", ADJUSTER_ROLE],
    },
];

/// A person's name as the adjuster's queue shows it (policyholders only).
#[must_use]
pub fn holder_name(user_id: &str) -> Option<&'static str> {
    match user_id {
        LEO => Some("Leo Park"),
        NINA => Some("Nina Alvarez"),
        _ => None,
    }
}
