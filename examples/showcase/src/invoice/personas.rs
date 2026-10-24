//! Accounts payable's personas: an AP clerk and the controller of the store
//! "Northwind Goods".

use crate::registry::Persona;

/// The store's accounts payable organization.
pub const ORG: &str = "1a7c0000-0000-4000-8000-0000000000e0";
/// AP clerk on the pro plan: uploads and matches invoices.
pub const NORA: &str = "a0aa0000-0000-4000-8000-0000000000e1";
/// Controller on the enterprise plan: decides exceptions and approves
/// payments the clerk may not.
pub const RAJ: &str = "4a100000-0000-4000-8000-0000000000e2";

/// The riverbase role every AP person carries besides `beam-user`.
pub const AP_ROLE: &str = "ap-clerk";
/// The riverbase role only the controller carries.
pub const CONTROLLER_ROLE: &str = "controller";

/// An AP clerk.
pub const CLERK: &str = "clerk";
/// The controller.
pub const CONTROLLER: &str = "controller";

pub const PERSONAS: [Persona; 2] = [
    Persona {
        key: "nora",
        user_id: NORA,
        name: "Nora Diaz",
        role: CLERK,
        plan: "pro",
        roles: &["beam-user", AP_ROLE],
    },
    Persona {
        key: "raj",
        user_id: RAJ,
        name: "Raj Mehta",
        role: CONTROLLER,
        plan: "enterprise",
        roles: &["beam-user", AP_ROLE, CONTROLLER_ROLE],
    },
];
