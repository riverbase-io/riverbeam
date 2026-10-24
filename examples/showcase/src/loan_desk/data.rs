//! The loan desk's own data: applications in `{schema}.loan_application`,
//! beside the beam tables (the app owns the schema). The in-memory store is
//! for tests.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use beam_store::{PgBeamStore, SchemaName};
use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Bool, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::loan_desk::errors::{LND_001, LND_002, LND_003, LND_004};
use crate::loan_desk::programs::REQUIRED_FIELDS;

/// Where an application is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationStatus {
    /// The borrower is still filling it in.
    Draft,
    /// Submitted; waits for an officer's pre-check.
    Submitted,
    /// An officer's pre-check is running or waiting for the officer.
    Precheck,
    Approved,
    /// Sent to a human underwriter.
    Referred,
    Declined,
}

impl ApplicationStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Submitted => "submitted",
            Self::Precheck => "precheck",
            Self::Approved => "approved",
            Self::Referred => "referred",
            Self::Declined => "declined",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        [
            Self::Draft,
            Self::Submitted,
            Self::Precheck,
            Self::Approved,
            Self::Referred,
            Self::Declined,
        ]
        .into_iter()
        .find(|s| s.as_str() == raw)
    }
}

/// One loan application.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Application {
    pub id: Uuid,
    /// The borrower's user id.
    pub owner: String,
    pub org: Option<String>,
    pub program_id: String,
    pub status: ApplicationStatus,
    /// The form, as the borrower and the assistant filled it.
    pub fields: Map<String, Value>,
    /// Uploaded documents: `{file_name, document_type, extracted?}`.
    pub documents: Vec<Value>,
    /// The pre-check's outcome, once there is one.
    pub decision: Option<Value>,
    /// A legal hold: erasing the borrower's data keeps it.
    pub held: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Application {
    #[must_use]
    pub fn new(owner: &str, org: Option<&str>, program_id: &str) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            owner: owner.to_owned(),
            org: org.map(str::to_owned),
            program_id: program_id.to_owned(),
            status: ApplicationStatus::Draft,
            fields: Map::new(),
            documents: Vec::new(),
            decision: None,
            held: false,
            created_at: now,
            updated_at: now,
        }
    }

    /// Required fields that are still empty.
    #[must_use]
    pub fn missing_fields(&self) -> Vec<&'static str> {
        REQUIRED_FIELDS
            .into_iter()
            .filter(|name| {
                self.fields
                    .get(*name)
                    .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()))
            })
            .collect()
    }

    /// Whether a pay stub is on file.
    #[must_use]
    pub fn has_pay_stub(&self) -> bool {
        self.documents
            .iter()
            .any(|d| d.get("document_type").and_then(Value::as_str) == Some("pay_stub"))
    }

    /// Merge `fields` into the form; only a draft changes.
    ///
    /// # Errors
    /// `LND-003` when the application is no longer a draft.
    pub fn merge_fields(&mut self, fields: &Map<String, Value>) -> RiverbaseResult<()> {
        if self.status != ApplicationStatus::Draft {
            return Err(LND_003.with_data(format!("the application is {}", self.status.as_str())));
        }
        for (key, value) in fields {
            if key == "program_id" {
                if let Some(id) = value.as_str() {
                    id.clone_into(&mut self.program_id);
                }
                continue;
            }
            self.fields.insert(key.clone(), value.clone());
        }
        Ok(())
    }

    /// Submit a complete draft.
    ///
    /// # Errors
    /// `LND-003` for an application that is not a draft; `LND-004` when it
    /// misses required fields or a pay stub.
    pub fn submit(&mut self) -> RiverbaseResult<()> {
        if self.status != ApplicationStatus::Draft {
            return Err(LND_003.with_data(format!("the application is {}", self.status.as_str())));
        }
        let missing = self.missing_fields();
        if !missing.is_empty() {
            return Err(LND_004.with_data(format!("missing: {}", missing.join(", "))));
        }
        if !self.has_pay_stub() {
            return Err(LND_004.with_data("missing: a pay stub"));
        }
        self.status = ApplicationStatus::Submitted;
        Ok(())
    }
}

/// Where applications are kept.
#[async_trait]
pub trait ApplicationStore: Send + Sync {
    /// # Errors
    /// `LND-001` on a store failure.
    async fn insert(&self, application: &Application) -> RiverbaseResult<()>;
    /// # Errors
    /// `LND-001` on a store failure.
    async fn get(&self, id: Uuid) -> RiverbaseResult<Option<Application>>;
    /// The owner's applications, or every one with `None`, newest first.
    ///
    /// # Errors
    /// `LND-001` on a store failure.
    async fn list(&self, owner: Option<&str>) -> RiverbaseResult<Vec<Application>>;
    /// Write the whole row and stamp `updated_at`.
    ///
    /// # Errors
    /// `LND-001` on a store failure; `LND-002` when the row is gone.
    async fn save(&self, application: &mut Application) -> RiverbaseResult<()>;
    /// Erase the owner's applications that are not held; returns how many.
    ///
    /// # Errors
    /// `LND-001` on a store failure.
    async fn erase_owner(&self, owner: &str) -> RiverbaseResult<usize>;
}

/// [`ApplicationStore::get`], failing with `LND-002` when there is none.
///
/// # Errors
/// `LND-002` for an unknown id; store errors.
pub async fn require(store: &dyn ApplicationStore, id: Uuid) -> RiverbaseResult<Application> {
    store
        .get(id)
        .await?
        .ok_or_else(|| LND_002.with_data(format!("application {id} was not found")))
}

/// Applications in memory (tests).
#[derive(Default)]
pub struct InMemoryApplications {
    rows: RwLock<HashMap<Uuid, Application>>,
}

fn poisoned() -> riverbase_core::RiverbaseError {
    LND_001.with_data("lock poisoned")
}

#[async_trait]
impl ApplicationStore for InMemoryApplications {
    async fn insert(&self, application: &Application) -> RiverbaseResult<()> {
        self.rows
            .write()
            .map_err(|_| poisoned())?
            .insert(application.id, application.clone());
        Ok(())
    }

    async fn get(&self, id: Uuid) -> RiverbaseResult<Option<Application>> {
        Ok(self.rows.read().map_err(|_| poisoned())?.get(&id).cloned())
    }

    async fn list(&self, owner: Option<&str>) -> RiverbaseResult<Vec<Application>> {
        let mut rows: Vec<Application> = self
            .rows
            .read()
            .map_err(|_| poisoned())?
            .values()
            .filter(|a| owner.is_none_or(|o| a.owner == o))
            .cloned()
            .collect();
        rows.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(rows)
    }

    async fn save(&self, application: &mut Application) -> RiverbaseResult<()> {
        let mut rows = self.rows.write().map_err(|_| poisoned())?;
        if !rows.contains_key(&application.id) {
            return Err(LND_002.with_data(format!("application {} was not found", application.id)));
        }
        application.updated_at = Utc::now();
        rows.insert(application.id, application.clone());
        Ok(())
    }

    async fn erase_owner(&self, owner: &str) -> RiverbaseResult<usize> {
        let mut rows = self.rows.write().map_err(|_| poisoned())?;
        let before = rows.len();
        rows.retain(|_, a| a.owner != owner || a.held);
        Ok(before - rows.len())
    }
}

/// Applications in Postgres, in the beam store's schema and pool.
pub struct PgApplications {
    store: PgBeamStore,
}

impl PgApplications {
    #[must_use]
    pub fn new(store: PgBeamStore) -> Arc<Self> {
        Arc::new(Self { store })
    }

    fn schema(&self) -> &SchemaName {
        self.store.schema()
    }

    /// Create the table if it is missing (`showcase-worker migrate`).
    ///
    /// # Errors
    /// `LND-001` when the DDL fails.
    pub async fn migrate(&self) -> RiverbaseResult<()> {
        self.store
            .db()
            .at(LND_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    conn.batch_execute(&format!(
                        "CREATE TABLE IF NOT EXISTS {s}.loan_application (
                            id uuid PRIMARY KEY,
                            owner text NOT NULL,
                            org text,
                            program_id text NOT NULL,
                            status text NOT NULL DEFAULT 'draft',
                            fields jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                            documents jsonb NOT NULL DEFAULT '[]'::jsonb,
                            decision jsonb,
                            held boolean NOT NULL DEFAULT false,
                            created_at timestamptz NOT NULL DEFAULT now(),
                            updated_at timestamptz NOT NULL DEFAULT now()
                        );
                        CREATE INDEX IF NOT EXISTS loan_application_owner_idx ON {s}.loan_application (owner);"
                    ))
                    .await
                    .map_err(|e| LND_001.with_data(e.to_string()))
                }
                .scope_boxed()
            })
            .await
    }
}

#[derive(QueryableByName)]
struct Row {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    owner: String,
    #[diesel(sql_type = Nullable<Text>)]
    org: Option<String>,
    #[diesel(sql_type = Text)]
    program_id: String,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Jsonb)]
    fields: Value,
    #[diesel(sql_type = Jsonb)]
    documents: Value,
    #[diesel(sql_type = Nullable<Jsonb>)]
    decision: Option<Value>,
    #[diesel(sql_type = Bool)]
    held: bool,
    #[diesel(sql_type = Timestamptz)]
    created_at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    updated_at: DateTime<Utc>,
}

impl From<Row> for Application {
    fn from(r: Row) -> Self {
        Self {
            id: r.id,
            owner: r.owner,
            org: r.org,
            program_id: r.program_id,
            status: ApplicationStatus::parse(&r.status).unwrap_or(ApplicationStatus::Draft),
            fields: match r.fields {
                Value::Object(map) => map,
                _ => Map::new(),
            },
            documents: match r.documents {
                Value::Array(docs) => docs,
                _ => Vec::new(),
            },
            decision: r.decision,
            held: r.held,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

const COLUMNS: &str =
    "id, owner, org, program_id, status, fields, documents, decision, held, created_at, updated_at";

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    LND_001.with_data(e.to_string())
}

#[async_trait]
impl ApplicationStore for PgApplications {
    async fn insert(&self, a: &Application) -> RiverbaseResult<()> {
        self.store
            .db()
            .at(LND_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    sql_query(format!(
                        "INSERT INTO {s}.loan_application ({COLUMNS}) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (id) DO NOTHING"
                    ))
                    .bind::<SqlUuid, _>(a.id)
                    .bind::<Text, _>(&a.owner)
                    .bind::<Nullable<Text>, _>(a.org.clone())
                    .bind::<Text, _>(&a.program_id)
                    .bind::<Text, _>(a.status.as_str())
                    .bind::<Jsonb, _>(Value::Object(a.fields.clone()))
                    .bind::<Jsonb, _>(Value::Array(a.documents.clone()))
                    .bind::<Nullable<Jsonb>, _>(a.decision.clone())
                    .bind::<Bool, _>(a.held)
                    .bind::<Timestamptz, _>(a.created_at)
                    .bind::<Timestamptz, _>(a.updated_at)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn get(&self, id: Uuid) -> RiverbaseResult<Option<Application>> {
        self.store
            .db()
            .at(LND_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    let rows: Vec<Row> = sql_query(format!(
                        "SELECT {COLUMNS} FROM {s}.loan_application WHERE id = $1"
                    ))
                    .bind::<SqlUuid, _>(id)
                    .load(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(rows.into_iter().next().map(Application::from))
                }
                .scope_boxed()
            })
            .await
    }

    async fn list(&self, owner: Option<&str>) -> RiverbaseResult<Vec<Application>> {
        self.store
            .db()
            .at(LND_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    let rows: Vec<Row> = sql_query(format!(
                        "SELECT {COLUMNS} FROM {s}.loan_application \
                     WHERE ($1::text IS NULL OR owner = $1) ORDER BY created_at DESC LIMIT 200"
                    ))
                    .bind::<Nullable<Text>, _>(owner)
                    .load(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(rows.into_iter().map(Application::from).collect())
                }
                .scope_boxed()
            })
            .await
    }

    async fn save(&self, a: &mut Application) -> RiverbaseResult<()> {
        self.store
            .db()
            .at(LND_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    a.updated_at = Utc::now();
                    let n = sql_query(format!(
                    "UPDATE {s}.loan_application SET program_id = $2, status = $3, fields = $4, \
                     documents = $5, decision = $6, held = $7, updated_at = $8 WHERE id = $1"
                ))
                    .bind::<SqlUuid, _>(a.id)
                    .bind::<Text, _>(&a.program_id)
                    .bind::<Text, _>(a.status.as_str())
                    .bind::<Jsonb, _>(Value::Object(a.fields.clone()))
                    .bind::<Jsonb, _>(Value::Array(a.documents.clone()))
                    .bind::<Nullable<Jsonb>, _>(a.decision.clone())
                    .bind::<Bool, _>(a.held)
                    .bind::<Timestamptz, _>(a.updated_at)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    if n == 0 {
                        return Err(
                            LND_002.with_data(format!("application {} was not found", a.id))
                        );
                    }
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn erase_owner(&self, owner: &str) -> RiverbaseResult<usize> {
        self.store
            .db()
            .at(LND_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    sql_query(format!(
                        "DELETE FROM {s}.loan_application WHERE owner = $1 AND NOT held"
                    ))
                    .bind::<Text, _>(owner)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)
                }
                .scope_boxed()
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn complete() -> Application {
        let mut a = Application::new(crate::loan_desk::personas::BOB, None, "conventional");
        for name in REQUIRED_FIELDS {
            a.fields.insert(name.into(), json!("x"));
        }
        a
    }

    #[test]
    fn submit_needs_every_field_and_a_pay_stub() {
        let mut a = Application::new(crate::loan_desk::personas::BOB, None, "fha");
        let err = a.submit().unwrap_err();
        assert_eq!(err.errcode.as_str(), "LND-004");
        let mut a = complete();
        assert_eq!(
            a.submit().unwrap_err().errcode.as_str(),
            "LND-004",
            "no pay stub yet"
        );
        a.documents
            .push(json!({"file_name": "stub.png", "document_type": "pay_stub"}));
        a.submit().unwrap();
        assert_eq!(a.status, ApplicationStatus::Submitted);
        let err = a.merge_fields(&Map::new()).unwrap_err();
        assert_eq!(
            err.errcode.as_str(),
            "LND-003",
            "a submitted application is frozen"
        );
    }

    #[tokio::test]
    async fn erasing_a_borrower_keeps_held_applications() {
        let store = InMemoryApplications::default();
        let kept = Application {
            held: true,
            ..Application::new("u1", None, "va")
        };
        store.insert(&kept).await.unwrap();
        store
            .insert(&Application::new("u1", None, "va"))
            .await
            .unwrap();
        store
            .insert(&Application::new("u2", None, "va"))
            .await
            .unwrap();
        assert_eq!(store.erase_owner("u1").await.unwrap(), 1);
        assert_eq!(store.list(Some("u1")).await.unwrap(), [kept]);
        assert_eq!(store.list(None).await.unwrap().len(), 2);
    }
}
