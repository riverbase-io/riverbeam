//! Durable tool idempotency on a migrated test database, `beam_test_beam_tools`. Runs when
//! `BEAM_DATABASE_URL` points at the parity database.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use beam_tools::{
    ExecuteContext, PgToolIdempotencyStore, ToolCallContext, ToolDefinition, ToolHandler,
    ToolRuntime,
};
use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

struct Charge(Arc<AtomicUsize>);

#[async_trait]
impl ToolHandler for Charge {
    async fn call(&self, _args: Value, _ctx: &ToolCallContext) -> RiverbaseResult<Value> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"charged": true}))
    }
}

#[tokio::test]
async fn idempotent_side_effect_survives_restart() {
    let Some(store) = beam_store::testdb::store("beam_tools").await else {
        return;
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = ToolDefinition::new("charge").with_handler(Arc::new(Charge(calls.clone())));
    let ctx = ExecuteContext {
        call: ToolCallContext {
            execution_id: Uuid::new_v4(),
            call_id: "call-1".into(),
            ..ToolCallContext::default()
        },
        ..ExecuteContext::default()
    };
    let worker_a = ToolRuntime::new(Arc::new(PgToolIdempotencyStore::for_store(&store)));
    let first = worker_a.execute(&tool, json!({}), &ctx).await.unwrap();
    drop(worker_a);
    // A new worker, with no memory of the first call.
    let worker_b = ToolRuntime::new(Arc::new(PgToolIdempotencyStore::for_store(&store)));
    let again = worker_b.execute(&tool, json!({}), &ctx).await.unwrap();
    assert_eq!((first.cached, again.cached), (false, true));
    assert_eq!(again.output, json!({"charged": true}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
