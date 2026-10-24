//! The work queue, `db.tasks()`: [`crate::tasks`]' statements on the
//! gateway. Workers claim, renew and finish tasks across orgs (a worker
//! serves every org), so these run without an org: under RLS the worker's
//! role must bypass RLS.

use std::time::Duration;

use serde_json::Value;
use uuid::Uuid;

use crate::db::{Db, Site};
use crate::store::StoreResult;
use crate::tasks::{self, NewTask, TaskRow};

/// The work queue on a [`Db`].
pub struct Tasks<'d> {
    db: &'d Db,
}

impl Db {
    /// The work queue.
    #[must_use]
    pub fn tasks(&self) -> Tasks<'_> {
        Tasks { db: self }
    }
}

impl Tasks<'_> {
    fn site(&self) -> Site<'_> {
        self.db.at(crate::BEM_507)
    }

    /// Insert `task` as `QUEUED` ([`tasks::enqueue`]).
    ///
    /// # Errors
    /// `BEM-507` when the insert fails.
    pub async fn enqueue(&self, task: &NewTask) -> StoreResult<()> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| Box::pin(async move { tasks::enqueue(conn, schema, task).await }))
            .await
    }

    /// One task by id.
    ///
    /// # Errors
    /// `BEM-507` when the read fails.
    pub async fn get(&self, id: Uuid) -> StoreResult<Option<TaskRow>> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| Box::pin(async move { tasks::get(conn, schema, id).await }))
            .await
    }

    /// Claim task `id` for `owner` ([`tasks::claim_by_id`]).
    ///
    /// # Errors
    /// `BEM-507` when the claim fails.
    pub async fn claim_by_id(
        &self,
        id: Uuid,
        owner: &str,
        lease: Duration,
    ) -> StoreResult<Option<TaskRow>> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move { tasks::claim_by_id(conn, schema, id, owner, lease).await })
            })
            .await
    }

    /// Claim the oldest due task for `owner` ([`tasks::claim_next`]).
    ///
    /// # Errors
    /// `BEM-507` when the claim fails.
    pub async fn claim_next(&self, owner: &str, lease: Duration) -> StoreResult<Option<TaskRow>> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move { tasks::claim_next(conn, schema, owner, lease).await })
            })
            .await
    }

    /// Claim the oldest due task named in `names` ([`tasks::claim_next_of`]).
    ///
    /// # Errors
    /// `BEM-507` when the claim fails.
    pub async fn claim_next_of(
        &self,
        owner: &str,
        lease: Duration,
        names: &[&str],
    ) -> StoreResult<Option<TaskRow>> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(
                    async move { tasks::claim_next_of(conn, schema, owner, lease, names).await },
                )
            })
            .await
    }

    /// Renew `owner`'s lease on task `id`; `false` when it is no longer
    /// `owner`'s ([`tasks::renew`]).
    ///
    /// # Errors
    /// `BEM-507` when the update fails.
    pub async fn renew(&self, id: Uuid, owner: &str, lease: Duration) -> StoreResult<bool> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move { tasks::renew(conn, schema, id, owner, lease).await })
            })
            .await
    }

    /// Record task `id` done ([`tasks::complete`]).
    ///
    /// # Errors
    /// `BEM-507` when the update fails.
    pub async fn complete(
        &self,
        id: Uuid,
        owner: &str,
        result: &Value,
        payload: &Value,
    ) -> StoreResult<bool> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(
                    async move { tasks::complete(conn, schema, id, owner, result, payload).await },
                )
            })
            .await
    }

    /// Record a failure of task `id` ([`tasks::fail`]).
    ///
    /// # Errors
    /// `BEM-507` when the update fails.
    pub async fn fail(
        &self,
        id: Uuid,
        owner: &str,
        error: &Value,
        payload: &Value,
        retry_in: Option<Duration>,
    ) -> StoreResult<bool> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move {
                    tasks::fail(conn, schema, id, owner, error, payload, retry_in).await
                })
            })
            .await
    }

    /// Put task `id` back to wait for a busy session ([`tasks::wait_busy`]).
    ///
    /// # Errors
    /// `BEM-507` when the update fails.
    pub async fn wait_busy(
        &self,
        id: Uuid,
        owner: &str,
        error: &Value,
        max_delay: Duration,
    ) -> StoreResult<bool> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move {
                    tasks::wait_busy(conn, schema, id, owner, error, max_delay).await
                })
            })
            .await
    }

    /// Requeue tasks whose lease expired ([`tasks::sweep_expired`]).
    ///
    /// # Errors
    /// `BEM-507` when the update fails.
    pub async fn sweep_expired(&self) -> StoreResult<Vec<Uuid>> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| Box::pin(async move { tasks::sweep_expired(conn, schema).await }))
            .await
    }

    /// Fail tasks queued longer than `ttl` ([`tasks::expire_queued`]).
    ///
    /// # Errors
    /// `BEM-507` when the update fails.
    pub async fn expire_queued(&self, ttl: Duration) -> StoreResult<Vec<TaskRow>> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move { tasks::expire_queued(conn, schema, ttl).await })
            })
            .await
    }

    /// Delete finished tasks older than `age` ([`tasks::purge_finished`]).
    ///
    /// # Errors
    /// `BEM-507` when the delete fails.
    pub async fn purge_finished(&self, age: Duration) -> StoreResult<usize> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move { tasks::purge_finished(conn, schema, age).await })
            })
            .await
    }

    /// Whether an invoke of `execution_id` is queued or running
    /// ([`tasks::has_active_invoke`]).
    ///
    /// # Errors
    /// `BEM-507` when the read fails.
    pub async fn has_active_invoke(&self, execution_id: Uuid) -> StoreResult<bool> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move { tasks::has_active_invoke(conn, schema, execution_id).await })
            })
            .await
    }

    /// Whether the slot of `session_id` and `agent_id` is held by an
    /// execution waiting for input ([`tasks::slot_waits_for_input`]).
    ///
    /// # Errors
    /// `BEM-507` when the read fails.
    pub async fn slot_waits_for_input(
        &self,
        session_id: Uuid,
        agent_id: Uuid,
    ) -> StoreResult<bool> {
        let schema = self.db.schema();
        self.site()
            .cross_org(|conn| {
                Box::pin(async move {
                    tasks::slot_waits_for_input(conn, schema, session_id, agent_id).await
                })
            })
            .await
    }
}
