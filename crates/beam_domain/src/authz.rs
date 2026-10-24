//! Policy authorization for the beam domain.
//!
//! The same CSV policy governs riverbase's HTTP Casbin layer and the
//! aliases riverbase's layer skips (`roles_may_run`, `roles_may_query`). Activities follow
//! riverbase (`{command}.execute`, `{resource}.query`); denials raise
//! [`beam_core::BEM_067`] (HTTP 403).
//!
//! `riverbase_http::casbin` now requires a Postgres adapter, so this crate
//! evaluates the bundled policy in-process (same `p,` / `g,` rows).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;

use crate::domain::BEAM_NAMESPACE;

/// Resource name beam commands authorize against.
pub const BEAM_RESOURCE: &str = "agent";

/// The role that runs every command, on any row.
pub const ADMIN_ROLE: &str = "beam-admin";
/// The role that runs [`USER_COMMANDS`] (on its own rows) and every query.
pub const USER_ROLE: &str = "beam-user";

/// Whether `roles` may run `command` under the bundled policy
/// ([`beam_policy_csv`]). For routes riverbase's Casbin layer does not see
/// (the deprecated aliases, the audio socket).
#[must_use]
pub fn roles_may_run(roles: &[String], command: &str) -> bool {
    roles.iter().any(|r| r == ADMIN_ROLE)
        || (roles.iter().any(|r| r == USER_ROLE) && USER_COMMANDS.contains(&command))
}

/// Whether `roles` may query under the bundled policy (see [`roles_may_run`]).
#[must_use]
pub fn roles_may_query(roles: &[String]) -> bool {
    roles.iter().any(|r| r == ADMIN_ROLE || r == USER_ROLE)
}

/// Commands a `beam-user` may run (on the rows the domain lets them reach);
/// every other command needs `beam-admin`. Python requires no policy
/// (`policy_required = False`); Rust always does.
pub const USER_COMMANDS: [&str; 20] = [
    "invoke",
    "stream",
    "resume",
    "cancel-execution",
    "retry-execution",
    "update-session",
    "remove-session",
    "remove-message",
    "remember-memory",
    "forget-memory",
    "edit-memory",
    "expire-memory",
    "submit-agent-feedback",
    "delete-my-agent-data",
    "set-my-tool-rule",
    "remove-my-tool-rule",
    "revoke-tool-grants",
    "create-agent-profile",
    "update-agent-profile",
    "remove-agent-profile",
];

/// The beam policy (riverbase activity rows: `{command}.execute` on the
/// command's resource, `{resource}.query`) for `namespace`: `beam-user`
/// runs [`USER_COMMANDS`] and every query but `agent_dead_letter`;
/// `beam-admin` everything.
#[must_use]
pub fn beam_policy_csv(namespace: &str) -> String {
    use std::fmt::Write as _;
    let mut csv = String::new();
    for (key, resource) in crate::commands::COMMAND_KEYS {
        if USER_COMMANDS.contains(&key) {
            let _ = writeln!(
                csv,
                "p, {USER_ROLE}, {namespace}, {key}.execute, {resource}, *, allow"
            );
        }
    }
    // Dead letters hold other users' requests: admins only.
    let admin_only = crate::query::resources::ADMIN_QUERY_RESOURCES;
    for name in crate::query::resources::QUERY_RESOURCES
        .iter()
        .filter(|n| !admin_only.contains(n))
    {
        let _ = writeln!(
            csv,
            "p, {USER_ROLE}, {namespace}, {name}.query, {name}, *, allow"
        );
    }
    // rc8's reports: each answers only for the caller's own rows.
    for name in crate::reports::REPORTS {
        let _ = writeln!(
            csv,
            "p, {USER_ROLE}, {namespace}, {name}.query, {name}, *, allow"
        );
    }
    let _ = writeln!(csv, "p, {ADMIN_ROLE}, {namespace}, *, *, *, allow");
    csv
}

/// [`beam_policy_csv`] for [`BEAM_NAMESPACE`].
pub static BEAM_POLICY_CSV: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| beam_policy_csv(BEAM_NAMESPACE));

/// A domain activity (command type + resource) used for policy checks.
#[derive(Debug, Clone)]
pub struct DomainActivity {
    /// Namespace (`flrs.beam`).
    pub namespace: String,
    /// Activity type (`beam.invoke`, …).
    pub activity_type: String,
    /// Resource name (`agent`).
    pub resource: String,
    /// Object id (agent name).
    pub object_id: String,
}

impl DomainActivity {
    /// Construct a new value.
    #[must_use]
    pub fn new(namespace: impl Into<String>, activity_type: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            activity_type: activity_type.into(),
            resource: String::new(),
            object_id: String::new(),
        }
    }

    /// Set resource and return self.
    #[must_use]
    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = resource.into();
        self
    }

    /// Set object id and return self.
    #[must_use]
    pub fn with_object_id(mut self, object_id: impl Into<String>) -> Self {
        self.object_id = object_id.into();
        self
    }
}

/// Authorization check input (subject + activity).
#[derive(Debug, Clone)]
pub struct ActivityRequest {
    /// Subject (user id or role name).
    pub subject: String,
    /// Activity.
    pub activity: DomainActivity,
}

impl ActivityRequest {
    /// Construct a new value.
    #[must_use]
    pub fn new(subject: impl Into<String>, activity: DomainActivity) -> Self {
        Self {
            subject: subject.into(),
            activity,
        }
    }
}

/// Authorize domain activities before command execution.
#[async_trait]
pub trait ActivityAuthorizer: Send + Sync {
    /// Enforce the policy for `request`.
    async fn enforce(&self, request: &ActivityRequest) -> RiverbaseResult<()>;
}

#[derive(Debug, Clone)]
struct PolicyRow {
    subject: String,
    namespace: String,
    activity_type: String,
    resource: String,
    object_id: String,
}

/// In-process evaluator for [`BEAM_POLICY_CSV`].
#[derive(Debug, Clone)]
pub struct BeamPolicyAuthorizer {
    policies: Vec<PolicyRow>,
    roles: HashMap<String, Vec<String>>,
}

/// Compatibility alias: the example HTTP server historically stored a Casbin authorizer.
pub type CasbinActivityAuthorizer = BeamPolicyAuthorizer;

impl BeamPolicyAuthorizer {
    /// Parse Casbin-style `p,` / `g,` rows.
    #[must_use]
    pub fn from_csv(csv: &str) -> Self {
        let mut policies = Vec::new();
        let mut roles: HashMap<String, Vec<String>> = HashMap::new();
        for raw in csv.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let cols: Vec<&str> = line.split(',').map(str::trim).collect();
            match cols.first().copied() {
                Some("p") if cols.len() >= 6 => {
                    let effect = cols.get(6).copied().unwrap_or("allow");
                    if effect.eq_ignore_ascii_case("allow") {
                        policies.push(PolicyRow {
                            subject: cols[1].to_string(),
                            namespace: cols[2].to_string(),
                            activity_type: cols[3].to_string(),
                            resource: cols[4].to_string(),
                            object_id: cols[5].to_string(),
                        });
                    }
                }
                Some("g") if cols.len() >= 3 => {
                    roles
                        .entry(cols[1].to_string())
                        .or_default()
                        .push(cols[2].to_string());
                }
                _ => {}
            }
        }
        Self { policies, roles }
    }

    fn allows(&self, request: &ActivityRequest) -> bool {
        let mut names = vec![request.subject.clone()];
        if let Some(roles) = self.roles.get(&request.subject) {
            names.extend(roles.iter().cloned());
        }
        names.iter().any(|name| {
            self.policies.iter().any(|p| {
                wildcard(&p.subject, name)
                    && wildcard(&p.namespace, &request.activity.namespace)
                    && wildcard(&p.activity_type, &request.activity.activity_type)
                    && wildcard(&p.resource, &request.activity.resource)
                    && wildcard(&p.object_id, &request.activity.object_id)
            })
        })
    }

    /// Enforce the bundled policy for `request`.
    ///
    /// # Errors
    /// Returns [`beam_core::BEM_067`] when the subject has no matching allow rule.
    pub fn enforce_sync(&self, request: &ActivityRequest) -> RiverbaseResult<()> {
        if self.allows(request) {
            Ok(())
        } else {
            Err(beam_core::BEM_067.with_data(request.subject.clone()))
        }
    }
}

fn wildcard(pat: &str, value: &str) -> bool {
    pat == "*" || pat == value
}

#[async_trait]
impl ActivityAuthorizer for BeamPolicyAuthorizer {
    async fn enforce(&self, request: &ActivityRequest) -> RiverbaseResult<()> {
        self.enforce_sync(request)
    }
}

/// A command's activity (riverbase's `{command}.execute` on its resource).
#[must_use]
pub fn activity_for_command(namespace: &str, cmdkey: &str) -> DomainActivity {
    let resource = crate::commands::command_resource(cmdkey).unwrap_or(BEAM_RESOURCE);
    DomainActivity::new(namespace, format!("{cmdkey}.execute")).with_resource(resource)
}

/// Build an authorizer from [`BEAM_POLICY_CSV`].
///
/// # Errors
/// Never fails for the bundled policy; `RiverbaseResult` keeps the call site uniform.
pub async fn build_beam_authorizer() -> RiverbaseResult<Arc<BeamPolicyAuthorizer>> {
    Ok(Arc::new(BeamPolicyAuthorizer::from_csv(&BEAM_POLICY_CSV)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_follow_the_bundled_policy() {
        let roles = |r: &[&str]| r.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        assert!(roles_may_run(&roles(&[USER_ROLE]), "invoke"));
        assert!(!roles_may_run(&roles(&[USER_ROLE]), "reconcile-now"));
        assert!(roles_may_run(&roles(&[ADMIN_ROLE]), "reconcile-now"));
        assert!(!roles_may_run(&roles(&["viewer"]), "invoke"));
        assert!(roles_may_query(&roles(&[USER_ROLE])));
        assert!(!roles_may_query(&[]));
    }

    #[test]
    fn users_run_their_commands_and_admins_everything() {
        let auth = BeamPolicyAuthorizer::from_csv(&format!(
            "{}g, u1, beam-user\ng, a1, beam-admin\n",
            *BEAM_POLICY_CSV
        ));
        let run = |who: &str, cmd: &str| {
            auth.enforce_sync(&ActivityRequest::new(
                who,
                activity_for_command(BEAM_NAMESPACE, cmd).with_object_id("x"),
            ))
        };
        assert!(run("u1", "invoke").is_ok());
        assert!(run("u1", "reconcile-now").is_err());
        assert!(run("a1", "reconcile-now").is_ok());
        // Legal holds are admin-only.
        for hold in ["place-legal-hold", "release-legal-hold"] {
            assert!(run("u1", hold).is_err(), "{hold}");
            assert!(run("a1", hold).is_ok(), "{hold}");
        }
        assert!(run("nobody", "invoke").is_err());
        let query = DomainActivity::new(BEAM_NAMESPACE, "agent_session.query")
            .with_resource("agent_session")
            .with_object_id("*");
        assert!(auth
            .enforce_sync(&ActivityRequest::new("u1", query))
            .is_ok());
        let dead = DomainActivity::new(BEAM_NAMESPACE, "agent_dead_letter.query")
            .with_resource("agent_dead_letter")
            .with_object_id("*");
        assert!(auth
            .enforce_sync(&ActivityRequest::new("u1", dead.clone()))
            .is_err());
        assert!(auth.enforce_sync(&ActivityRequest::new("a1", dead)).is_ok());
    }
}
