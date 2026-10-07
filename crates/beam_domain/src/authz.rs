//! Policy authorization for the beam domain.
//!
//! The same CSV policy governs the HTTP endpoint layer and the per-command
//! layer in [`crate::BeamCommandEngine`]. Commands map to `beam.*` activity
//! types; denials raise [`beam_core::BEM_067`] (HTTP 403).
//!
//! `riverbase_http::casbin` now requires a Postgres adapter, so this crate
//! evaluates the bundled policy in-process (same `p,` / `g,` rows).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;
use serde_json::Value;

use crate::domain::BEAM_NAMESPACE;

/// Resource name beam commands authorize against.
pub const BEAM_RESOURCE: &str = "agent";

/// Subject used when a request carries no authenticated user.
pub const ANONYMOUS_SUBJECT: &str = "anonymous";

/// Casbin-style policy CSV for the beam domain (`p,` / `g,` rows).
///
/// `beam-user` may invoke/stream/resume agents; `beam-admin` may do anything.
/// Role assignments (`g,` rows) are illustrative — production deployments load
/// them from the principal's claims at the HTTP boundary.
pub const BEAM_POLICY_CSV: &str = r"p, beam-user, flrs.beam, beam.invoke, agent, *, allow
p, beam-user, flrs.beam, beam.stream, agent, *, allow
p, beam-user, flrs.beam, beam.resume, agent, *, allow
p, beam-admin, flrs.beam, *, *, *, allow
g, alice, beam-admin
";

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

/// Map a beam command key to its domain activity type (audit + policy).
#[must_use]
pub fn activity_for_command(namespace: &str, cmdkey: &str) -> DomainActivity {
    let activity_type = match cmdkey {
        "invoke" => "beam.invoke",
        "stream" => "beam.stream",
        "resume" => "beam.resume",
        other => other,
    };
    DomainActivity::new(namespace, activity_type).with_resource(BEAM_RESOURCE)
}

/// Extract the policy subject (user id) from a command payload's auth context.
#[must_use]
pub fn subject_from_payload(payload: &Value) -> String {
    payload
        .get("auth_context")
        .and_then(|a| a.get("user_id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(ANONYMOUS_SUBJECT)
        .to_string()
}

/// The agent name a command targets (used as the policy object id).
#[must_use]
pub fn object_from_payload(payload: &Value) -> String {
    payload
        .get("agent_name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Build a beam activity from a command + payload, ready for enforcement.
#[must_use]
pub fn command_activity(cmdkey: &str, payload: &Value) -> DomainActivity {
    activity_for_command(BEAM_NAMESPACE, cmdkey).with_object_id(object_from_payload(payload))
}

/// Build an authorizer from [`BEAM_POLICY_CSV`].
///
/// # Errors
/// Never fails for the bundled policy; `RiverbaseResult` keeps the call site uniform.
pub async fn build_beam_authorizer() -> RiverbaseResult<Arc<BeamPolicyAuthorizer>> {
    Ok(Arc::new(BeamPolicyAuthorizer::from_csv(BEAM_POLICY_CSV)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn subject_and_object_extraction() {
        let payload = json!({
            "agent_name": "demo",
            "auth_context": {"user_id": "alice"},
        });
        assert_eq!(subject_from_payload(&payload), "alice");
        assert_eq!(object_from_payload(&payload), "demo");
        assert_eq!(subject_from_payload(&json!({})), ANONYMOUS_SUBJECT);
    }

    #[tokio::test]
    async fn admin_allowed_user_scoped_and_anon_denied() {
        let auth = build_beam_authorizer().await.expect("authorizer");
        let payload = json!({"agent_name": "demo", "auth_context": {"user_id": "alice"}});

        let activity = command_activity("invoke", &payload);
        auth.enforce(&ActivityRequest::new(subject_from_payload(&payload), activity))
            .await
            .expect("admin invoke allowed");

        let activity = command_activity("invoke", &payload);
        auth.enforce(&ActivityRequest::new("beam-user", activity))
            .await
            .expect("beam-user invoke allowed");

        let activity = command_activity("invoke", &payload);
        assert!(auth
            .enforce(&ActivityRequest::new(ANONYMOUS_SUBJECT, activity))
            .await
            .is_err());
    }
}
