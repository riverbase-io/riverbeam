//! The incident desk's personas: the on-call SRE and the incident commander
//! of the "Northwind" platform team.

use crate::registry::Persona;

/// The platform team's organization.
pub const ORG: &str = "1c1de000-0000-4000-8000-0000000000d0";
/// On-call SRE.
pub const KAI: &str = "ca100000-0000-4000-8000-0000000000d1";
/// Incident commander.
pub const LIN: &str = "11a00000-0000-4000-8000-0000000000d2";

/// The riverbase role every on-call engineer carries besides `beam-user`.
pub const SRE_ROLE: &str = "sre";
/// The riverbase role of the incident commander.
pub const COMMANDER_ROLE: &str = "incident-commander";

/// An on-call SRE.
pub const SRE: &str = "sre";
/// An incident commander.
pub const COMMANDER: &str = "commander";

pub const PERSONAS: [Persona; 2] = [
    Persona {
        key: "kai",
        user_id: KAI,
        name: "Kai Sato",
        role: SRE,
        plan: "pro",
        roles: &["beam-user", SRE_ROLE],
    },
    Persona {
        key: "lin",
        user_id: LIN,
        name: "Lin Wei",
        role: COMMANDER,
        plan: "enterprise",
        roles: &["beam-user", SRE_ROLE, COMMANDER_ROLE],
    },
];

/// The persona's display name, or the user id when it is not a persona.
#[must_use]
pub fn display_name(user_id: &str) -> String {
    PERSONAS
        .iter()
        .find(|p| p.user_id == user_id)
        .map_or_else(|| user_id.to_owned(), |p| p.name.to_owned())
}
