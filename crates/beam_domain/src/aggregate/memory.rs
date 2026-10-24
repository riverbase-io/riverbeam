//! Long-term memory: `remember-memory`, `forget-memory`, `edit-memory`,
//! `expire-memory`. Each checks the caller, then queues a worker task (the
//! worker embeds and stores).

use beam_store::tasks::NewTask;
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use super::{queued, BeamAggregate};
use crate::commands::{
    EditMemoryPayload, ExpireMemoryPayload, ForgetMemoryPayload, RememberMemoryPayload,
};
use crate::local::BeamSql;

impl BeamAggregate {
    /// `remember-memory`: a memory for the caller (admins may name any user).
    /// The response carries the memory's id, assigned here.
    ///
    /// # Errors
    /// `BEM-412` when `user_id` is not the caller.
    pub async fn remember_memory(&mut self, p: RememberMemoryPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        let named = p.user_id.trim();
        if !caller.admin
            && caller
                .user_id
                .as_deref()
                .is_none_or(|own| !own.eq_ignore_ascii_case(named))
        {
            return Err(beam_core::BEM_071.with_data("user_id must be the caller"));
        }
        let memory_id = Uuid::new_v4().to_string();
        let task = NewTask::new(
            "remember_memory",
            json!({
                "memory_id": memory_id,
                "user_id": named,
                "content": p.content,
                "type": p.memory_type,
                "agent_id": p.agent_id,
                "importance": p.importance,
                "metadata": p.metadata,
                "org_id": caller.org_id,
            }),
        );
        let task_id = self.enqueue(task).await?;
        let mut out = queued(task_id, "remember_memory");
        out["memory_id"] = json!(memory_id);
        Ok(out)
    }

    /// `forget-memory`: delete one of the caller's memories.
    ///
    /// # Errors
    /// `BEM-404` unknown; `BEM-411` / `BEM-412` not the caller's.
    pub async fn forget_memory(&mut self, p: ForgetMemoryPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        self.db().writable_memory(&p.memory_id, &caller).await?;
        let task = NewTask::new(
            "forget_memory",
            json!({ "memory_id": p.memory_id, "org_id": caller.org_id }),
        );
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "forget_memory"))
    }

    /// `edit-memory`: new content for one of the caller's memories.
    ///
    /// # Errors
    /// `BEM-404` unknown; `BEM-411` / `BEM-412` not the caller's.
    pub async fn edit_memory(&mut self, p: EditMemoryPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        self.db().writable_memory(&p.memory_id, &caller).await?;
        let task = NewTask::new(
            "edit_memory",
            json!({ "memory_id": p.memory_id, "content": p.content, "org_id": caller.org_id }),
        );
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "edit_memory"))
    }

    /// `expire-memory`: an expiry time for one of the caller's memories.
    ///
    /// # Errors
    /// `BEM-404` unknown; `BEM-411` / `BEM-412` not the caller's.
    pub async fn expire_memory(&mut self, p: ExpireMemoryPayload) -> RiverbaseResult<Value> {
        let caller = self.caller();
        self.db().writable_memory(&p.memory_id, &caller).await?;
        let task = NewTask::new(
            "expire_memory",
            json!({ "memory_id": p.memory_id, "expires_at": p.expires_at, "org_id": caller.org_id }),
        );
        let task_id = self.enqueue(task).await?;
        Ok(queued(task_id, "expire_memory"))
    }
}
