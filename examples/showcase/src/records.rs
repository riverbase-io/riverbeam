//! The new showcases' records: tickets, claims, incidents and invoices in one
//! table, `{schema}.showcase_record`, beside the beam tables (the showcases
//! own the schema). Each record has a kind, an owner, a status, a title and
//! its fields as JSON. The in-memory store is for tests.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use beam_store::{PgBeamStore, SchemaName};
use chrono::{DateTime, Utc};
use diesel::sql_query;
use diesel::sql_types::{Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid};
use diesel::QueryableByName;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::errors::{SHC_001, SHC_002, SHC_003};

/// One record of a showcase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub id: Uuid,
    /// The showcase's key (`support`, `claims`, …).
    pub showcase: String,
    /// What it is within the showcase (`ticket`, `claim`, …).
    pub kind: String,
    /// The user it belongs to.
    pub owner: String,
    pub org: Option<String>,
    pub status: String,
    pub title: String,
    pub data: Map<String, Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Record {
    /// A new record with status `open` and no data.
    #[must_use]
    pub fn new(showcase: &str, kind: &str, owner: &str, org: Option<&str>, title: &str) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            showcase: showcase.into(),
            kind: kind.into(),
            owner: owner.into(),
            org: org.map(str::to_owned),
            status: "open".into(),
            title: title.into(),
            data: Map::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// The field `key` of its data.
    #[must_use]
    pub fn field(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    /// The field `key` as a number.
    #[must_use]
    pub fn number(&self, key: &str) -> Option<f64> {
        self.data.get(key).and_then(Value::as_f64)
    }

    /// The field `key` as text.
    #[must_use]
    pub fn text(&self, key: &str) -> Option<&str> {
        self.data.get(key).and_then(Value::as_str)
    }

    /// Merge `fields` into its data.
    pub fn merge(&mut self, fields: &Map<String, Value>) {
        for (key, value) in fields {
            self.data.insert(key.clone(), value.clone());
        }
    }
}

/// Where records live.
#[async_trait]
pub trait RecordStore: Send + Sync {
    /// Add a record (a record with the same id is left as it is).
    async fn insert(&self, record: &Record) -> RiverbaseResult<()>;
    async fn get(&self, id: Uuid) -> RiverbaseResult<Option<Record>>;
    /// A showcase's records of one kind, newest first; only `owner`'s when
    /// given.
    async fn list(
        &self,
        showcase: &str,
        kind: &str,
        owner: Option<&str>,
    ) -> RiverbaseResult<Vec<Record>>;
    /// Write the record back and set `updated_at` (`SHC-002` when it is
    /// gone).
    async fn save(&self, record: &mut Record) -> RiverbaseResult<()>;
    /// [`Self::save`] only while the stored status is still `from`: a
    /// transition that moves money must not overwrite another writer's
    /// (`SHC-003` when the status moved or the record is gone).
    async fn save_from(&self, record: &mut Record, from: &str) -> RiverbaseResult<()>;
}

fn moved(record: &Record, from: &str) -> riverbase_core::RiverbaseError {
    SHC_003.with_data(format!("record {} is no longer {from}; read it again", record.id))
}

/// The record `id` of `showcase`, or `SHC-002`.
///
/// # Errors
/// `SHC-002` when there is no such record in that showcase.
pub async fn require(store: &dyn RecordStore, showcase: &str, id: Uuid) -> RiverbaseResult<Record> {
    store
        .get(id)
        .await?
        .filter(|r| r.showcase == showcase)
        .ok_or_else(|| SHC_002.with_data(format!("record {id} was not found")))
}

/// Records in memory (tests).
#[derive(Default)]
pub struct InMemoryRecords {
    rows: RwLock<HashMap<Uuid, Record>>,
}

impl InMemoryRecords {
    #[must_use]
    pub fn shared() -> Arc<dyn RecordStore> {
        Arc::new(Self::default())
    }
}

fn poisoned() -> riverbase_core::RiverbaseError {
    SHC_001.with_data("lock poisoned")
}

#[async_trait]
impl RecordStore for InMemoryRecords {
    async fn insert(&self, record: &Record) -> RiverbaseResult<()> {
        self.rows
            .write()
            .map_err(|_| poisoned())?
            .entry(record.id)
            .or_insert_with(|| record.clone());
        Ok(())
    }

    async fn get(&self, id: Uuid) -> RiverbaseResult<Option<Record>> {
        Ok(self.rows.read().map_err(|_| poisoned())?.get(&id).cloned())
    }

    async fn list(
        &self,
        showcase: &str,
        kind: &str,
        owner: Option<&str>,
    ) -> RiverbaseResult<Vec<Record>> {
        let mut rows: Vec<Record> = self
            .rows
            .read()
            .map_err(|_| poisoned())?
            .values()
            .filter(|r| r.showcase == showcase && r.kind == kind)
            .filter(|r| owner.is_none_or(|o| r.owner == o))
            .cloned()
            .collect();
        rows.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        Ok(rows)
    }

    async fn save(&self, record: &mut Record) -> RiverbaseResult<()> {
        let mut rows = self.rows.write().map_err(|_| poisoned())?;
        if !rows.contains_key(&record.id) {
            return Err(SHC_002.with_data(format!("record {} was not found", record.id)));
        }
        record.updated_at = Utc::now();
        rows.insert(record.id, record.clone());
        Ok(())
    }

    async fn save_from(&self, record: &mut Record, from: &str) -> RiverbaseResult<()> {
        let mut rows = self.rows.write().map_err(|_| poisoned())?;
        if rows.get(&record.id).is_none_or(|stored| stored.status != from) {
            return Err(moved(record, from));
        }
        record.updated_at = Utc::now();
        rows.insert(record.id, record.clone());
        Ok(())
    }
}

/// Records in Postgres, in the beam store's schema and pool.
pub struct PgRecords {
    store: PgBeamStore,
}

impl PgRecords {
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
    /// `SHC-001` when the DDL fails.
    pub async fn migrate(&self) -> RiverbaseResult<()> {
        self.store
            .db()
            .at(SHC_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    conn.batch_execute(&format!(
                        "CREATE TABLE IF NOT EXISTS {s}.showcase_record (
                            id uuid PRIMARY KEY,
                            showcase text NOT NULL,
                            kind text NOT NULL,
                            owner text NOT NULL,
                            org text,
                            status text NOT NULL DEFAULT 'open',
                            title text NOT NULL DEFAULT '',
                            data jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                            created_at timestamptz NOT NULL DEFAULT now(),
                            updated_at timestamptz NOT NULL DEFAULT now()
                        );
                        CREATE INDEX IF NOT EXISTS showcase_record_kind_idx
                            ON {s}.showcase_record (showcase, kind, owner);"
                    ))
                    .await
                    .map_err(db)
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
    showcase: String,
    #[diesel(sql_type = Text)]
    kind: String,
    #[diesel(sql_type = Text)]
    owner: String,
    #[diesel(sql_type = Nullable<Text>)]
    org: Option<String>,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Jsonb)]
    data: Value,
    #[diesel(sql_type = Timestamptz)]
    created_at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    updated_at: DateTime<Utc>,
}

impl From<Row> for Record {
    fn from(r: Row) -> Self {
        Self {
            id: r.id,
            showcase: r.showcase,
            kind: r.kind,
            owner: r.owner,
            org: r.org,
            status: r.status,
            title: r.title,
            data: match r.data {
                Value::Object(map) => map,
                _ => Map::new(),
            },
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

const COLUMNS: &str = "id, showcase, kind, owner, org, status, title, data, created_at, updated_at";

fn db(e: impl std::fmt::Display) -> riverbase_core::RiverbaseError {
    SHC_001.with_data(e.to_string())
}

#[async_trait]
impl RecordStore for PgRecords {
    async fn insert(&self, r: &Record) -> RiverbaseResult<()> {
        self.store
            .db()
            .at(SHC_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    sql_query(format!(
                        "INSERT INTO {s}.showcase_record ({COLUMNS}) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO NOTHING"
                    ))
                    .bind::<SqlUuid, _>(r.id)
                    .bind::<Text, _>(&r.showcase)
                    .bind::<Text, _>(&r.kind)
                    .bind::<Text, _>(&r.owner)
                    .bind::<Nullable<Text>, _>(r.org.clone())
                    .bind::<Text, _>(&r.status)
                    .bind::<Text, _>(&r.title)
                    .bind::<Jsonb, _>(Value::Object(r.data.clone()))
                    .bind::<Timestamptz, _>(r.created_at)
                    .bind::<Timestamptz, _>(r.updated_at)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn get(&self, id: Uuid) -> RiverbaseResult<Option<Record>> {
        self.store
            .db()
            .at(SHC_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    let rows: Vec<Row> = sql_query(format!(
                        "SELECT {COLUMNS} FROM {s}.showcase_record WHERE id = $1"
                    ))
                    .bind::<SqlUuid, _>(id)
                    .load(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(rows.into_iter().next().map(Record::from))
                }
                .scope_boxed()
            })
            .await
    }

    async fn list(
        &self,
        showcase: &str,
        kind: &str,
        owner: Option<&str>,
    ) -> RiverbaseResult<Vec<Record>> {
        self.store
            .db()
            .at(SHC_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    let rows: Vec<Row> = sql_query(format!(
                        "SELECT {COLUMNS} FROM {s}.showcase_record \
                         WHERE showcase = $1 AND kind = $2 AND ($3::text IS NULL OR owner = $3) \
                         ORDER BY created_at DESC LIMIT 200"
                    ))
                    .bind::<Text, _>(showcase)
                    .bind::<Text, _>(kind)
                    .bind::<Nullable<Text>, _>(owner)
                    .load(&mut *conn)
                    .await
                    .map_err(db)?;
                    Ok(rows.into_iter().map(Record::from).collect())
                }
                .scope_boxed()
            })
            .await
    }

    async fn save(&self, r: &mut Record) -> RiverbaseResult<()> {
        self.store
            .db()
            .at(SHC_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    r.updated_at = Utc::now();
                    let n = sql_query(format!(
                        "UPDATE {s}.showcase_record SET status = $2, title = $3, data = $4, \
                         updated_at = $5 WHERE id = $1"
                    ))
                    .bind::<SqlUuid, _>(r.id)
                    .bind::<Text, _>(&r.status)
                    .bind::<Text, _>(&r.title)
                    .bind::<Jsonb, _>(Value::Object(r.data.clone()))
                    .bind::<Timestamptz, _>(r.updated_at)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    if n == 0 {
                        return Err(SHC_002.with_data(format!("record {} was not found", r.id)));
                    }
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    async fn save_from(&self, r: &mut Record, from: &str) -> RiverbaseResult<()> {
        self.store
            .db()
            .at(SHC_001)
            .app_connection(|conn| {
                async move {
                    let s = self.schema();
                    let updated_at = Utc::now();
                    let n = sql_query(format!(
                        "UPDATE {s}.showcase_record SET status = $2, title = $3, data = $4, \
                         updated_at = $5 WHERE id = $1 AND status = $6"
                    ))
                    .bind::<SqlUuid, _>(r.id)
                    .bind::<Text, _>(&r.status)
                    .bind::<Text, _>(&r.title)
                    .bind::<Jsonb, _>(Value::Object(r.data.clone()))
                    .bind::<Timestamptz, _>(updated_at)
                    .bind::<Text, _>(from)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                    if n == 0 {
                        return Err(moved(r, from));
                    }
                    r.updated_at = updated_at;
                    Ok(())
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

    #[tokio::test]
    async fn records_list_by_showcase_kind_and_owner() {
        let store = InMemoryRecords::default();
        let mine = Record::new("support", "ticket", "u1", None, "Late parcel");
        store.insert(&mine).await.unwrap();
        store
            .insert(&Record::new("support", "ticket", "u2", None, "Refund"))
            .await
            .unwrap();
        store
            .insert(&Record::new("claims", "claim", "u1", None, "Bumper"))
            .await
            .unwrap();
        assert_eq!(store.list("support", "ticket", Some("u1")).await.unwrap(), std::slice::from_ref(&mine));
        assert_eq!(store.list("support", "ticket", None).await.unwrap().len(), 2);
        let err = require(&store, "claims", mine.id).await.unwrap_err();
        assert_eq!(err.errcode.as_str(), "SHC-002", "another showcase's record");
    }

    #[tokio::test]
    async fn save_merges_and_needs_the_record() {
        let store = InMemoryRecords::default();
        let mut record = Record::new("invoice", "invoice", "u1", None, "INV-1");
        let err = store.save(&mut record).await.unwrap_err();
        assert_eq!(err.errcode.as_str(), "SHC-002");
        store.insert(&record).await.unwrap();
        record.merge(json!({"total": 12.5}).as_object().unwrap());
        record.status = "matched".into();
        store.save(&mut record).await.unwrap();
        let read = store.get(record.id).await.unwrap().unwrap();
        assert_eq!(read.number("total"), Some(12.5));
        assert_eq!(read.status, "matched");
    }

    #[tokio::test]
    async fn save_from_refuses_a_record_another_writer_moved() {
        let store = InMemoryRecords::default();
        let mut first = Record::new("claims", "claim", "u1", None, "Bumper");
        first.status = "in_review".into();
        store.insert(&first).await.unwrap();
        let mut second = first.clone();
        first.status = "paid".into();
        store.save_from(&mut first, "in_review").await.unwrap();
        second.status = "approved".into();
        let err = store.save_from(&mut second, "in_review").await.unwrap_err();
        assert_eq!(err.errcode.as_str(), "SHC-003");
        assert_eq!(store.get(first.id).await.unwrap().unwrap().status, "paid");
    }
}
