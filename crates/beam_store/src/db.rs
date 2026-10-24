//! The one way beam reaches Postgres (plan 025).
//!
//! Every connection and transaction is checked out through a [`Db`]. With
//! the optional RLS on (`BEAM_RLS_ENABLED`), a scoped checkout runs in a
//! transaction whose first statement sets `beam.org_id` to the running
//! task's org (`org_scope::with_org`) for that transaction only, so no
//! pooled connection carries an org to its next borrower. With RLS off no
//! SQL is added.
//!
//! - [`Db::scoped`]: org-scoped work. With RLS on it is a short transaction
//!   that commits whatever the work returns; a failed statement aborts it, so
//!   Postgres rolls back the work's earlier statements too (with RLS off each
//!   statement commits on its own).
//! - [`Db::transaction`]: always a transaction, scoped first, rolled back
//!   when the work fails.
//! - [`Db::cross_org`]: work across orgs (the queue, reconcile, retention,
//!   migrations). It sets no org: under RLS its role must bypass RLS.
//! - [`Db::unit_of_work`]: a riverbase unit of work for entity writes.
//! - [`Site::app_connection`]: a table an app owns beside the beam's.
//!
//! [`Db::at`] names the error a failed checkout raises, so each store keeps
//! its own code.

use std::sync::Arc;

use diesel_async::pooled_connection::deadpool::Object;
use diesel_async::scoped_futures::{ScopedBoxFuture, ScopedFutureExt};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use riverbase_core::base::ErrorSpec;
use riverbase_core::datastore::{establish_dbpool, PgDataStore, PgPool};
use riverbase_core::RiverbaseError;

use crate::entity::write::{self, UnitOfWork};
use crate::org_scope;
use crate::schema::SchemaName;
use crate::store::StoreResult;

/// A repository failure (plan 025 D5): a statement failed (`Query`, with
/// the database's message; `Conflict` when it broke a unique constraint), or
/// a beam error passed through (`Beam`: a failed checkout, a malformed
/// value, a refusal). Each store maps it to the code
/// its site raises with [`DbError::or`], so public errors stay as they were.
#[derive(Debug)]
pub enum DbError {
    Query(String),
    Conflict(String),
    /// No connection could be had.
    Checkout(String),
    Beam(RiverbaseError),
}

/// A repository result.
pub type DbResult<T> = Result<T, DbError>;

impl DbError {
    /// The error a store raises: `spec` with the database's message for a
    /// failed statement, the beam error itself otherwise.
    #[must_use]
    pub fn or(self, spec: ErrorSpec) -> RiverbaseError {
        match self {
            Self::Query(message) | Self::Conflict(message) | Self::Checkout(message) => {
                spec.with_data(message)
            }
            Self::Beam(e) => e,
        }
    }
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(message) | Self::Conflict(message) | Self::Checkout(message) => {
                f.write_str(message)
            }
            Self::Beam(e) => write!(f, "{e}"),
        }
    }
}

impl From<diesel::result::Error> for DbError {
    fn from(e: diesel::result::Error) -> Self {
        use diesel::result::{DatabaseErrorKind, Error};
        match e {
            Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                Self::Conflict(e.to_string())
            }
            e => Self::Query(e.to_string()),
        }
    }
}

impl From<RiverbaseError> for DbError {
    fn from(e: RiverbaseError) -> Self {
        Self::Beam(e)
    }
}

/// A failed checkout: the site's code (as a beam error) or, for a
/// repository, [`DbError::Checkout`], which its store maps to its own code.
#[derive(Debug)]
pub struct Checkout {
    spec: ErrorSpec,
    message: String,
}

impl From<Checkout> for RiverbaseError {
    fn from(c: Checkout) -> Self {
        c.spec.with_data(c.message)
    }
}

impl From<Checkout> for DbError {
    fn from(c: Checkout) -> Self {
        Self::Checkout(c.message)
    }
}

/// A failure of the transaction [`Site::scoped`] opens with RLS on.
enum ScopeError {
    /// The org could not be applied.
    Apply(diesel::result::Error),
    /// Beginning or committing failed.
    Transaction(diesel::result::Error),
}

impl From<diesel::result::Error> for ScopeError {
    fn from(e: diesel::result::Error) -> Self {
        Self::Transaction(e)
    }
}

impl From<DbError> for RiverbaseError {
    /// Without a site code: `BEM-139` for a failed statement.
    fn from(e: DbError) -> Self {
        e.or(crate::BEM_139)
    }
}

/// The beam database: its pool, schema and riverbase datastore. Cheap to clone.
#[derive(Clone)]
pub struct Db {
    pool: PgPool,
    schema: SchemaName,
    /// The one datastore on this pool (plan 013 D3): the entities the query
    /// and command engines use, and the writes that go through them.
    datastore: Arc<PgDataStore>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl Db {
    /// The database on `pool`, whose tables are in `schema`. The pool must
    /// set the schema's `search_path` ([`SchemaName::pool_url`]): the
    /// entities name tables and enum types without a schema.
    #[must_use]
    pub fn new(pool: PgPool, schema: SchemaName) -> Self {
        let datastore = Arc::new(crate::entity::datastore(pool.clone()));
        Self {
            pool,
            schema,
            datastore,
        }
    }

    /// Connect to `database_url`; the tables are in `BEAM_SQL_SCHEMA`
    /// (default `riverbeam`). No DDL runs; see [`crate::migrate`].
    ///
    /// # Errors
    /// `BEM-110` when the pool cannot be established.
    pub async fn connect(database_url: &str) -> StoreResult<Self> {
        let schema = SchemaName::from_env()?;
        let pool = establish_dbpool(&schema.pool_url(database_url))
            .await
            .map_err(|e| crate::BEM_110.with_data(e.to_string()))?;
        Ok(Self::new(pool, schema))
    }

    #[must_use]
    pub fn schema(&self) -> &SchemaName {
        &self.schema
    }

    /// The riverbase datastore, for the command and query engines.
    #[must_use]
    pub fn datastore(&self) -> &Arc<PgDataStore> {
        &self.datastore
    }

    /// The pool, for this crate and for tests (plan 025 D7).
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The pool, for this crate's migrations and readiness.
    #[cfg(not(any(test, feature = "testing")))]
    #[must_use]
    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Checkouts that raise `pool_error` when no connection can be had.
    #[must_use]
    pub fn at(&self, pool_error: ErrorSpec) -> Site<'_> {
        Site {
            db: self,
            pool_error,
        }
    }

    /// [`Site::scoped`], with `BEM-113` for a failed checkout.
    ///
    /// # Errors
    /// As [`Site::scoped`].
    pub async fn scoped<'a, T, E, F>(&self, work: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        T: Send + 'a,
        E: From<RiverbaseError> + From<Checkout> + From<diesel::result::Error> + Send + 'a,
    {
        self.at(crate::BEM_113).scoped(work).await
    }

    /// [`Site::transaction`], with `BEM-113` for a failed checkout.
    ///
    /// # Errors
    /// As [`Site::transaction`].
    pub async fn transaction<'a, T, E, F>(&self, work: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        T: Send + 'a,
        E: From<RiverbaseError> + From<Checkout> + From<diesel::result::Error> + Send + 'a,
    {
        self.at(crate::BEM_113).transaction(work).await
    }

    /// [`Site::cross_org`], with `BEM-113` for a failed checkout.
    ///
    /// # Errors
    /// As [`Site::cross_org`].
    pub async fn cross_org<'a, T, E, F>(&self, work: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        T: Send + 'a,
        E: From<RiverbaseError> + From<Checkout> + From<diesel::result::Error> + Send + 'a,
    {
        self.at(crate::BEM_113).cross_org(work).await
    }

    /// A unit of work on the datastore, its org applied for the transaction.
    ///
    /// # Errors
    /// When the transaction cannot begin or the org cannot be applied.
    pub async fn unit_of_work(&self) -> StoreResult<UnitOfWork> {
        write::begin(&self.datastore).await
    }

    /// Whether the connecting role bypasses RLS (a superuser, or
    /// `BYPASSRLS`). Cross-org work needs it once RLS is on.
    ///
    /// # Errors
    /// `BEM-113` when the role cannot be read.
    pub async fn role_bypasses_rls(&self) -> StoreResult<bool> {
        #[derive(diesel::QueryableByName)]
        struct Bypass {
            #[diesel(sql_type = diesel::sql_types::Bool)]
            bypass: bool,
        }
        self.cross_org(|conn| {
            async move {
                let rows: Vec<Bypass> = diesel::sql_query(
                    "SELECT (rolsuper OR rolbypassrls) AS bypass FROM pg_roles \
                     WHERE rolname = current_user",
                )
                .load(conn)
                .await
                .map_err(|e| crate::BEM_113.with_data(e.to_string()))?;
                Ok(rows.into_iter().next().is_some_and(|r| r.bypass))
            }
            .scope_boxed()
        })
        .await
    }

    /// A connection taken out of the pool for good, with no org: for a
    /// session-level advisory lock held across calls (reconcile). Closing
    /// the connection releases the lock.
    pub(crate) async fn detached(&self, pool_error: ErrorSpec) -> StoreResult<AsyncPgConnection> {
        let pooled = self
            .pool
            .get()
            .await
            .map_err(|e| pool_error.with_data(e.to_string()))?;
        Ok(Object::take(pooled))
    }
}

/// Checkouts on a [`Db`] that raise one store's error when the pool fails.
pub struct Site<'d> {
    db: &'d Db,
    pool_error: ErrorSpec,
}

impl Site<'_> {
    async fn checkout<E: From<Checkout>>(&self) -> Result<Object<AsyncPgConnection>, E> {
        self.db.pool.get().await.map_err(|e| {
            E::from(Checkout {
                spec: self.pool_error,
                message: e.to_string(),
            })
        })
    }

    /// Run `work` as the running task's org. With RLS on it runs in a
    /// transaction that applies the org first and commits whatever `work`
    /// returns; with RLS off it runs on a plain connection.
    ///
    /// # Errors
    /// The site's error when no connection can be had; `BEM-137` when the
    /// org cannot be applied; a failed begin or commit; `work`'s errors.
    pub async fn scoped<'a, T, E, F>(&self, work: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        T: Send + 'a,
        E: From<RiverbaseError> + From<Checkout> + From<diesel::result::Error> + Send + 'a,
    {
        let mut pooled = self.checkout::<E>().await?;
        let conn: &mut AsyncPgConnection = &mut pooled;
        if !org_scope::rls_enabled() {
            return work(conn).await;
        }
        let org = org_scope::current_org();
        let done: Result<Result<T, E>, ScopeError> = conn
            .transaction(move |tx| {
                async move {
                    org_scope::apply(tx, org).await.map_err(ScopeError::Apply)?;
                    Ok(work(tx).await)
                }
                .scope_boxed()
            })
            .await;
        match done {
            Ok(result) => result,
            Err(ScopeError::Apply(e)) => Err(E::from(crate::BEM_137.with_data(e.to_string()))),
            Err(ScopeError::Transaction(e)) => Err(E::from(e)),
        }
    }

    /// Run `work` in a transaction as the running task's org (applied first
    /// when RLS is on). It commits when `work` succeeds and rolls back when
    /// it fails.
    ///
    /// # Errors
    /// The site's error when no connection can be had; `BEM-137` when the
    /// org cannot be applied; `work`'s errors.
    pub async fn transaction<'a, T, E, F>(&self, work: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        T: Send + 'a,
        E: From<RiverbaseError> + From<Checkout> + From<diesel::result::Error> + Send + 'a,
    {
        let mut pooled = self.checkout::<E>().await?;
        let conn: &mut AsyncPgConnection = &mut pooled;
        let org = org_scope::current_org();
        conn.transaction(move |tx| {
            async move {
                org_scope::apply(tx, org)
                    .await
                    .map_err(|e| E::from(crate::BEM_137.with_data(e.to_string())))?;
                work(tx).await
            }
            .scope_boxed()
        })
        .await
    }

    /// Run `work` on a plain connection that carries no org: work across
    /// orgs (the queue, reconcile, retention, migrations). Under RLS the
    /// role must bypass RLS, or RLS tables show no rows.
    ///
    /// # Errors
    /// The site's error when no connection can be had; `work`'s errors.
    pub async fn cross_org<'a, T, E, F>(&self, work: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        T: Send + 'a,
        E: From<RiverbaseError> + From<Checkout> + From<diesel::result::Error> + Send + 'a,
    {
        let mut pooled = self.checkout::<E>().await?;
        let conn: &mut AsyncPgConnection = &mut pooled;
        work(conn).await
    }

    /// Run `work` on a plain connection, for a table an app owns beside the
    /// beam's (loan desk's `loan_application`). Beam tables go through a
    /// repository instead.
    ///
    /// # Errors
    /// The site's error when no connection can be had; `work`'s errors.
    pub async fn app_connection<'a, T, E, F>(&self, work: F) -> Result<T, E>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> ScopedBoxFuture<'a, 'r, Result<T, E>>
            + Send
            + 'a,
        T: Send + 'a,
        E: From<RiverbaseError> + From<Checkout> + From<diesel::result::Error> + Send + 'a,
    {
        self.cross_org(work).await
    }
}
