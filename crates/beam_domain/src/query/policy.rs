//! Who sees which rows: the scope policies the generated query resources
//! call. Each returns the filter riverbase applies before the client's own,
//! on the row's owner columns, `user_id` and `org_id` (session-owned
//! rows carry copies of their session's). `beam-admin` sees every row (`None`).
//!
//! Reads see the caller's rows and shared ones (no user, no org); the work
//! queue's rows are the caller's alone.

use riverbase_core::datastore::dsl::{Expr, PredicateOp};
use riverbase_core::{EngineContext, RiverbaseResult};
use serde_json::Value;

fn is_admin(ctx: &EngineContext) -> bool {
    ctx.roles.iter().any(|r| r == crate::authz::ADMIN_ROLE)
}

fn user(ctx: &EngineContext) -> Option<String> {
    ctx.actor.user_id.map(|u| u.to_string())
}

fn org(ctx: &EngineContext) -> Option<String> {
    ctx.organization_id.map(|o| o.to_string())
}

/// `column` is NULL (a shared row) or `value`; only NULL without a value.
fn mine_or_shared(column: &str, value: Option<String>) -> Expr {
    let shared = Expr::field(column, PredicateOp::Eq, Value::Null);
    match value {
        Some(value) => Expr::Or(vec![shared, Expr::eq(column, value)]),
        None => shared,
    }
}

/// Rows with a `user_id`: the caller's and shared ones (`agent_profile`,
/// `agent_usage_profile`).
///
/// # Errors
/// Never; riverbase's scope-policy signature.
pub fn user_rows(ctx: &EngineContext) -> RiverbaseResult<Option<Expr>> {
    if is_admin(ctx) {
        return Ok(None);
    }
    Ok(Some(mine_or_shared("user_id", user(ctx))))
}

/// Rows owned by a user and an org: sessions, memory, knowledge, events, and
/// the rows of a session (messages, runs, steps, executions, feedback), which
/// carry their session's owner.
///
/// # Errors
/// Never; riverbase's scope-policy signature.
pub fn user_org_rows(ctx: &EngineContext) -> RiverbaseResult<Option<Expr>> {
    if is_admin(ctx) {
        return Ok(None);
    }
    Ok(Some(Expr::And(vec![
        mine_or_shared("user_id", user(ctx)),
        mine_or_shared("org_id", org(ctx)),
    ])))
}

/// The caller's own rows in their org or shared orgs, never a row without
/// a user (`agent_feedback`: one user's ratings and comments; feedback whose
/// execution is not stored has no owner and is admins' only).
///
/// # Errors
/// Never; riverbase's scope-policy signature.
pub fn own_rows(ctx: &EngineContext) -> RiverbaseResult<Option<Expr>> {
    if is_admin(ctx) {
        return Ok(None);
    }
    Ok(Some(Expr::And(vec![
        Expr::eq("user_id", user(ctx).unwrap_or_default()),
        mine_or_shared("org_id", org(ctx)),
    ])))
}

/// The caller's own rows only, never shared ones (`agent_task`: a task's
/// payload is its owner's request).
///
/// # Errors
/// Never; riverbase's scope-policy signature.
pub fn private_rows(ctx: &EngineContext) -> RiverbaseResult<Option<Expr>> {
    if is_admin(ctx) {
        return Ok(None);
    }
    // No user matches no row: an empty id is never a UUID's text.
    Ok(Some(Expr::eq("user_id", user(ctx).unwrap_or_default())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn ctx(roles: &[&str]) -> EngineContext {
        let mut ctx = EngineContext::new("fluvius-beam");
        ctx.actor.user_id = Some(Uuid::nil());
        ctx.roles = roles.iter().map(|r| (*r).to_owned()).collect();
        ctx
    }

    #[test]
    fn admins_see_every_row() {
        let admin = ctx(&[crate::authz::ADMIN_ROLE]);
        for policy in [user_rows, user_org_rows, own_rows, private_rows] {
            assert!(policy(&admin).unwrap().is_none());
        }
    }

    #[test]
    fn users_see_their_own_and_shared_rows() {
        let user = ctx(&[crate::authz::USER_ROLE]);
        let Some(Expr::Or(parts)) = user_rows(&user).unwrap() else {
            panic!("an OR of shared and own");
        };
        assert_eq!(parts.len(), 2);
        // The work queue shares nothing; feedback is never shared by user.
        assert!(matches!(
            private_rows(&user).unwrap(),
            Some(Expr::Field { .. })
        ));
        let Some(Expr::And(parts)) = own_rows(&user).unwrap() else {
            panic!("an AND");
        };
        assert!(matches!(
            &parts[0],
            Expr::Field {
                value: Value::String(_),
                ..
            }
        ));
        // Without an org, only org-less rows.
        let Some(Expr::And(parts)) = user_org_rows(&user).unwrap() else {
            panic!("an AND");
        };
        assert!(matches!(
            &parts[1],
            Expr::Field {
                value: Value::Null,
                ..
            }
        ));
    }
}
