//! Tool rules and grants (Python rc8 `tools/stores/`, plan 023 part C):
//! `ref_beam.agent_tool_rule` and `ref_beam.agent_tool_grant` (migration
//! 0010), and in-memory books for beams without a database.
//!
//! Rows are keyed by org: a rule or grant of one org never applies in
//! another, and a missing org matches only rows without one. A user id or
//! org that is not a UUID matches nothing.

use std::sync::Mutex;

use async_trait::async_trait;
use beam_types::tool_policy::{
    RuleScope, ToolGrant, ToolMatch, ToolMode, ToolRule, ToolRuleRecord,
};
use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Bool, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::repo::rows::Found;
use crate::store::StoreResult;

/// Rules tenants and users set.
#[async_trait]
pub trait ToolRuleStore: Send + Sync {
    /// The tenant rules of `org_id`, and `user_id`'s own rules in it.
    async fn list_rules(
        &self,
        org_id: Option<&str>,
        user_id: Option<&str>,
    ) -> StoreResult<Vec<ToolRuleRecord>>;

    async fn put_rule(&self, record: &ToolRuleRecord) -> StoreResult<()>;

    /// Remove rule `rule_id` of `org_id`; with `user_id`, only that user's
    /// own rule. Returns whether it went.
    async fn remove_rule(
        &self,
        rule_id: Uuid,
        org_id: Option<&str>,
        user_id: Option<&str>,
    ) -> StoreResult<bool>;
}

/// "Allow for this conversation" grants.
#[async_trait]
pub trait ToolGrantStore: Send + Sync {
    /// Whether `tool_name` is granted in `session_id` of `org_id` at `now`.
    async fn is_granted(
        &self,
        session_id: Uuid,
        tool_name: &str,
        org_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> StoreResult<bool>;

    /// Grant, replacing the session's grant for the tool.
    async fn grant(&self, grant: &ToolGrant) -> StoreResult<()>;

    /// Revoke `session_id`'s grants in `org_id` (one tool's, or all; with
    /// `user_id`, only that user's). Returns how many went.
    async fn revoke(
        &self,
        session_id: Uuid,
        tool_name: Option<&str>,
        user_id: Option<&str>,
        org_id: Option<&str>,
    ) -> StoreResult<u64>;
}

/// An id as stored: `Ok(None)` for none, `Err(())` for one that is not a UUID.
fn id(raw: Option<&str>) -> Result<Option<Uuid>, ()> {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| ())
}

fn same(a: Option<&str>, b: Option<&str>) -> bool {
    id(a).ok().flatten() == id(b).ok().flatten() && id(a).is_ok() && id(b).is_ok()
}

/// The in-memory rule book.
#[derive(Debug, Default)]
pub struct ToolRules(Mutex<Vec<ToolRuleRecord>>);

impl ToolRules {
    fn book(&self) -> StoreResult<std::sync::MutexGuard<'_, Vec<ToolRuleRecord>>> {
        self.0
            .lock()
            .map_err(|_| crate::BEM_139.with_data("tool rule book poisoned"))
    }
}

#[async_trait]
impl ToolRuleStore for ToolRules {
    async fn list_rules(
        &self,
        org_id: Option<&str>,
        user_id: Option<&str>,
    ) -> StoreResult<Vec<ToolRuleRecord>> {
        Ok(self
            .book()?
            .iter()
            .filter(|r| same(r.org_id.as_deref(), org_id))
            .filter(|r| match r.scope {
                RuleScope::Tenant => true,
                RuleScope::User => user_id.is_some() && same(r.user_id.as_deref(), user_id),
            })
            .cloned()
            .collect())
    }

    async fn put_rule(&self, record: &ToolRuleRecord) -> StoreResult<()> {
        let mut book = self.book()?;
        book.retain(|r| r.rule_id != record.rule_id);
        book.push(record.clone());
        Ok(())
    }

    async fn remove_rule(
        &self,
        rule_id: Uuid,
        org_id: Option<&str>,
        user_id: Option<&str>,
    ) -> StoreResult<bool> {
        let mut book = self.book()?;
        let before = book.len();
        book.retain(|r| {
            let target = r.rule_id == rule_id
                && same(r.org_id.as_deref(), org_id)
                && user_id.is_none_or(|u| {
                    r.scope == RuleScope::User && same(r.user_id.as_deref(), Some(u))
                });
            !target
        });
        Ok(book.len() < before)
    }
}

/// The in-memory grant book.
#[derive(Debug, Default)]
pub struct ToolGrants(Mutex<Vec<ToolGrant>>);

impl ToolGrants {
    fn book(&self) -> StoreResult<std::sync::MutexGuard<'_, Vec<ToolGrant>>> {
        self.0
            .lock()
            .map_err(|_| crate::BEM_139.with_data("tool grant book poisoned"))
    }
}

#[async_trait]
impl ToolGrantStore for ToolGrants {
    async fn is_granted(
        &self,
        session_id: Uuid,
        tool_name: &str,
        org_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        Ok(self.book()?.iter().any(|g| {
            g.session_id == session_id
                && g.tool_name == tool_name
                && same(g.org_id.as_deref(), org_id)
                && g.expires_at > now
        }))
    }

    async fn grant(&self, grant: &ToolGrant) -> StoreResult<()> {
        let mut book = self.book()?;
        book.retain(|g| !(g.session_id == grant.session_id && g.tool_name == grant.tool_name));
        book.push(grant.clone());
        Ok(())
    }

    async fn revoke(
        &self,
        session_id: Uuid,
        tool_name: Option<&str>,
        user_id: Option<&str>,
        org_id: Option<&str>,
    ) -> StoreResult<u64> {
        let mut book = self.book()?;
        let before = book.len();
        book.retain(|g| {
            let target = g.session_id == session_id
                && same(g.org_id.as_deref(), org_id)
                && tool_name.is_none_or(|t| g.tool_name == t)
                && user_id.is_none_or(|u| same(g.user_id.as_deref(), Some(u)));
            !target
        });
        Ok(u64::try_from(before - book.len()).unwrap_or(u64::MAX))
    }
}

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    crate::BEM_139.with_data(e.to_string())
}

/// [`ToolRuleStore`] and [`ToolGrantStore`] over `agent_tool_rule` and
/// `agent_tool_grant`.
#[derive(Clone)]
pub struct PgToolPolicy {
    db: crate::Db,
}

impl PgToolPolicy {
    #[must_use]
    pub fn for_store(store: &crate::PgBeamStore) -> Self {
        Self {
            db: store.db().clone(),
        }
    }
}

#[derive(QueryableByName)]
struct RuleRow {
    #[diesel(sql_type = SqlUuid, column_name = _id)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    scope: String,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    org_id: Option<Uuid>,
    #[diesel(sql_type = Nullable<SqlUuid>)]
    user_id: Option<Uuid>,
    #[diesel(sql_type = Jsonb)]
    matcher: serde_json::Value,
    #[diesel(sql_type = Text)]
    mode: String,
    #[diesel(sql_type = Bool)]
    locked: bool,
}

impl RuleRow {
    /// The record. A row this build cannot read is an error, not a skipped
    /// rule: skipping a `deny` would allow what it refuses.
    fn record(self) -> StoreResult<ToolRuleRecord> {
        let unreadable = |what: &str| {
            crate::BEM_139.with_data(format!("tool rule {}: unreadable {what}", self.id))
        };
        let scope = match self.scope.as_str() {
            "tenant" => RuleScope::Tenant,
            "user" => RuleScope::User,
            _ => return Err(unreadable("scope")),
        };
        let mode = match self.mode.as_str() {
            "auto" => ToolMode::Auto,
            "ask" => ToolMode::Ask,
            "deny" => ToolMode::Deny,
            _ => return Err(unreadable("mode")),
        };
        let matcher: ToolMatch =
            serde_json::from_value(self.matcher).map_err(|_| unreadable("match"))?;
        matcher.check().map_err(|_| unreadable("match"))?;
        Ok(ToolRuleRecord {
            rule_id: self.id,
            scope,
            org_id: self.org_id.map(|o| o.to_string()),
            user_id: self.user_id.map(|u| u.to_string()),
            rule: ToolRule {
                matcher,
                mode,
                locked: self.locked,
            },
        })
    }
}

#[async_trait]
impl ToolRuleStore for PgToolPolicy {
    async fn list_rules(
        &self,
        org_id: Option<&str>,
        user_id: Option<&str>,
    ) -> StoreResult<Vec<ToolRuleRecord>> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    // Rules are stored for UUID orgs and users only: a caller with
                    // another org has no rules, and one with another user id has no
                    // user rules but still gets the tenant's.
                    let Ok(org) = id(org_id) else {
                        return Ok(Vec::new());
                    };
                    let user = id(user_id).ok().flatten();
                    let rows: Vec<RuleRow> = sql_query(format!(
                        "SELECT _id, scope, org_id, user_id, \"match\" AS matcher, mode, locked \
                         FROM {}.agent_tool_rule WHERE _deleted IS NULL AND org_id IS NOT DISTINCT FROM $1 \
                         AND (scope = 'tenant' OR (scope = 'user' AND $2::uuid IS NOT NULL AND user_id = $2)) \
                         ORDER BY _created",
                        self.db.schema()
                    ))
                    .bind::<Nullable<SqlUuid>, _>(org)
                    .bind::<Nullable<SqlUuid>, _>(user)
                    .load(&mut *conn)
                    .await
                    .map_err(db)?;
                    rows.into_iter().map(RuleRow::record).collect()
                }
                .scope_boxed()
            })
            .await
    }

    async fn put_rule(&self, record: &ToolRuleRecord) -> StoreResult<()> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let (Ok(org), Ok(user)) = (id(record.org_id.as_deref()), id(record.user_id.as_deref()))
                    else {
                        return Err(crate::BEM_139.with_data("a tool rule's org and user must be UUIDs"));
                    };
                    let matcher = serde_json::to_value(&record.rule.matcher).map_err(db)?;
                    sql_query(format!(
                        "INSERT INTO {}.agent_tool_rule (_id, scope, org_id, user_id, \"match\", mode, locked) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7) \
                         ON CONFLICT (_id) DO UPDATE SET \"match\" = EXCLUDED.\"match\", mode = EXCLUDED.mode, \
                         locked = EXCLUDED.locked, _updated = now()",
                        self.db.schema()
                    ))
                    .bind::<SqlUuid, _>(record.rule_id)
                    .bind::<Text, _>(record.scope.as_str())
                    .bind::<Nullable<SqlUuid>, _>(org)
                    .bind::<Nullable<SqlUuid>, _>(user)
                    .bind::<Jsonb, _>(matcher)
                    .bind::<Text, _>(record.rule.mode.as_str())
                    .bind::<Bool, _>(record.rule.locked)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn remove_rule(
        &self,
        rule_id: Uuid,
        org_id: Option<&str>,
        user_id: Option<&str>,
    ) -> StoreResult<bool> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let (Ok(org), Ok(user)) = (id(org_id), id(user_id)) else {
                        return Ok(false);
                    };
                    let deleted = sql_query(format!(
                        "DELETE FROM {}.agent_tool_rule WHERE _id = $1 AND org_id IS NOT DISTINCT FROM $2 \
                         AND ($3::boolean IS FALSE OR (scope = 'user' AND user_id = $4))",
                        self.db.schema()
                    ))
                    .bind::<SqlUuid, _>(rule_id)
                    .bind::<Nullable<SqlUuid>, _>(org)
                    .bind::<Bool, _>(user_id.is_some())
                    .bind::<Nullable<SqlUuid>, _>(user)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(deleted > 0)
                }
                .scope_boxed()
            })
            .await
    }
}

#[async_trait]
impl ToolGrantStore for PgToolPolicy {
    async fn is_granted(
        &self,
        session_id: Uuid,
        tool_name: &str,
        org_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let Ok(org) = id(org_id) else {
                        return Ok(false);
                    };
                    let rows: Vec<Found> = sql_query(format!(
                        "SELECT EXISTS (SELECT 1 FROM {}.agent_tool_grant WHERE session_id = $1 \
                     AND tool_name = $2 AND org_id IS NOT DISTINCT FROM $3 AND expires_at > $4 \
                     AND _deleted IS NULL) AS found",
                        self.db.schema()
                    ))
                    .bind::<SqlUuid, _>(session_id)
                    .bind::<Text, _>(tool_name)
                    .bind::<Nullable<SqlUuid>, _>(org)
                    .bind::<Timestamptz, _>(now)
                    .load(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(<[Found]>::first(&rows).is_some_and(|r| r.found))
                }
                .scope_boxed()
            })
            .await
    }

    async fn grant(&self, grant: &ToolGrant) -> StoreResult<()> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let (Ok(org), Ok(user)) = (id(grant.org_id.as_deref()), id(grant.user_id.as_deref()))
                    else {
                        return Err(crate::BEM_139.with_data("a tool grant's org and user must be UUIDs"));
                    };
                    sql_query(format!(
                        "INSERT INTO {}.agent_tool_grant (_id, session_id, tool_name, user_id, org_id, expires_at) \
                         VALUES ($1, $2, $3, $4, $5, $6) \
                         ON CONFLICT (session_id, tool_name) DO UPDATE SET user_id = EXCLUDED.user_id, \
                         org_id = EXCLUDED.org_id, expires_at = EXCLUDED.expires_at, _deleted = NULL, \
                         _updated = now()",
                        self.db.schema()
                    ))
                    .bind::<SqlUuid, _>(Uuid::new_v4())
                    .bind::<SqlUuid, _>(grant.session_id)
                    .bind::<Text, _>(&grant.tool_name)
                    .bind::<Nullable<SqlUuid>, _>(user)
                    .bind::<Nullable<SqlUuid>, _>(org)
                    .bind::<Timestamptz, _>(grant.expires_at)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn revoke(
        &self,
        session_id: Uuid,
        tool_name: Option<&str>,
        user_id: Option<&str>,
        org_id: Option<&str>,
    ) -> StoreResult<u64> {
        self.db
            .at(crate::BEM_139)
            .scoped(|conn| {
                async move {
                    let (Ok(org), Ok(user)) = (id(org_id), id(user_id)) else {
                        return Ok(0);
                    };
                    let deleted = sql_query(format!(
                        "DELETE FROM {}.agent_tool_grant WHERE session_id = $1 \
                     AND org_id IS NOT DISTINCT FROM $2 AND ($3::text IS NULL OR tool_name = $3) \
                     AND ($4::boolean IS FALSE OR user_id = $5)",
                        self.db.schema()
                    ))
                    .bind::<SqlUuid, _>(session_id)
                    .bind::<Nullable<SqlUuid>, _>(org)
                    .bind::<Nullable<Text>, _>(tool_name)
                    .bind::<Bool, _>(user_id.is_some())
                    .bind::<Nullable<SqlUuid>, _>(user)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(u64::try_from(deleted).unwrap_or(0))
                }
                .scope_boxed()
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(scope: RuleScope, org: Option<&str>, user: Option<&str>) -> ToolRuleRecord {
        ToolRuleRecord {
            rule_id: Uuid::new_v4(),
            scope,
            org_id: org.map(str::to_owned),
            user_id: user.map(str::to_owned),
            rule: ToolRule {
                matcher: ToolMatch::exact("pay"),
                mode: ToolMode::Ask,
                locked: false,
            },
        }
    }

    #[tokio::test]
    async fn rules_are_kept_per_org_and_owner() {
        let (org, a, b) = (
            Uuid::new_v4().to_string(),
            Uuid::new_v4().to_string(),
            Uuid::new_v4().to_string(),
        );
        let book = ToolRules::default();
        let tenant = rule(RuleScope::Tenant, Some(&org), None);
        let mine = rule(RuleScope::User, Some(&org), Some(&a));
        book.put_rule(&tenant).await.unwrap();
        book.put_rule(&mine).await.unwrap();
        assert_eq!(
            book.list_rules(Some(&org), Some(&a)).await.unwrap().len(),
            2
        );
        assert_eq!(
            book.list_rules(Some(&org), Some(&b)).await.unwrap().len(),
            1
        );
        assert!(book.list_rules(None, Some(&a)).await.unwrap().is_empty());
        // B cannot remove A's rule.
        assert!(!book
            .remove_rule(mine.rule_id, Some(&org), Some(&b))
            .await
            .unwrap());
        assert!(book
            .remove_rule(mine.rule_id, Some(&org), Some(&a))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn a_grant_lasts_until_it_expires() {
        let session = Uuid::new_v4();
        let now = Utc::now();
        let book = ToolGrants::default();
        book.grant(&ToolGrant {
            session_id: session,
            tool_name: "pay".into(),
            user_id: None,
            org_id: None,
            expires_at: now + chrono::TimeDelta::hours(24),
        })
        .await
        .unwrap();
        assert!(book.is_granted(session, "pay", None, now).await.unwrap());
        let later = now + chrono::TimeDelta::hours(25);
        assert!(!book.is_granted(session, "pay", None, later).await.unwrap());
        assert!(!book
            .is_granted(Uuid::new_v4(), "pay", None, now)
            .await
            .unwrap());
        assert_eq!(book.revoke(session, None, None, None).await.unwrap(), 1);
    }
}
