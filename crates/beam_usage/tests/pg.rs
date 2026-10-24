//! Postgres usage stores on a migrated test database, `beam_test_beam_usage`. They run when
//! `BEAM_DATABASE_URL` points at the parity database.

use std::sync::Arc;

use beam_store::{CatalogStore, PgBeamStore};
use beam_types::{Modality, TokenUsage};
use beam_usage::UsageEvent;
use beam_usage::{
    cost_event, CostDims, CostStore, OveragePolicy, PgCostStore, PgReservationStore, PgUsageStore,
    UsageGuard, UsageLimits, UsagePlan, UsageStore, UsageTracker,
};
use uuid::Uuid;

async fn store() -> Option<PgBeamStore> {
    beam_store::testdb::store("beam_usage").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_counters_events_and_profiles() {
    let Some(store) = store().await else { return };
    let usage = Arc::new(PgUsageStore::for_store(&store));
    let user = Uuid::new_v4().to_string();
    let tracker = UsageTracker::new(usage.clone());
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let tracker_store = usage.clone();
        let user = user.clone();
        tasks.push(tokio::spawn(async move {
            UsageTracker::new(tracker_store)
                .record(
                    &user,
                    &UsageEvent {
                        tokens: 7,
                        requests: 1,
                        model_used: Some("m".into()),
                        modalities: vec![Modality::Text],
                    },
                )
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let summary = tracker.get_usage(&user).await.unwrap();
    assert_eq!(
        (summary.total_requests, summary.total_tokens),
        (10, 70),
        "no lost or duplicated counter rows"
    );
    assert_eq!(tracker.get_usage_profile(&user).await.unwrap(), None);
    // Profiles reference a checked-in plan, as in Python.
    let pro = UsagePlan {
        name: "pro".into(),
        limits: UsageLimits {
            allowed_modalities: Some(vec![Modality::Text, Modality::Image]),
            max_tokens_per_day: Some(5000),
            ..UsageLimits::default()
        },
        overage_policy: OveragePolicy::SoftWarn,
    };
    store.checkin_plans(&[pro.to_row("active")]).await.unwrap();
    tracker.set_usage_profile(&user, "pro").await.unwrap();
    assert_eq!(
        tracker.get_usage_profile(&user).await.unwrap().as_deref(),
        Some("pro")
    );
    usage.reset(&user, &usage.current_period()).await.unwrap();
    assert_eq!(tracker.get_usage(&user).await.unwrap().total_requests, 0);
    let err = usage
        .increment("not-a-uuid", "p", "f", 1)
        .await
        .unwrap_err();
    assert_eq!(err.errcode.as_str(), "BEM-364");
}

#[tokio::test]
async fn pg_reserve_settle_release() {
    let Some(store) = store().await else { return };
    let tracker = Arc::new(UsageTracker::new(Arc::new(PgUsageStore::for_store(&store))));
    let guard =
        UsageGuard::new(tracker).with_reservations(Arc::new(PgReservationStore::for_store(&store)));
    let plan = UsagePlan {
        name: "daily".into(),
        limits: UsageLimits {
            max_tokens_per_day: Some(1000),
            ..UsageLimits::default()
        },
        overage_policy: OveragePolicy::HardBlock,
    };
    let user = Uuid::new_v4().to_string();
    let first = guard.reserve(&user, &plan, 600, None, None).await.unwrap();
    // A second worker's guard sees the first hold in the database.
    let other = UsageGuard::new(Arc::new(UsageTracker::new(Arc::new(
        PgUsageStore::for_store(&store),
    ))))
    .with_reservations(Arc::new(PgReservationStore::for_store(&store)));
    let denied = other
        .reserve(&user, &plan, 600, None, None)
        .await
        .unwrap_err();
    assert_eq!(denied.errcode.as_str(), "BEM-362");
    guard.settle(first.id, 123).await.unwrap();
    let second = other.reserve(&user, &plan, 600, None, None).await.unwrap();
    other.release(second.id).await.unwrap();
    other.settle(second.id, 1).await.unwrap();
    let third = guard.reserve(&user, &plan, 900, None, None).await.unwrap();
    guard.release(third.id).await.unwrap();
}

#[tokio::test]
async fn pg_cost_event_is_written() {
    let Some(store) = store().await else { return };
    let costs = PgCostStore::for_store(&store);
    let usage = TokenUsage {
        prompt_tokens: 1200,
        completion_tokens: 345,
        total_tokens: 1545,
        ..TokenUsage::default()
    };
    let event = cost_event(
        "gpt",
        &usage,
        0.002,
        CostDims {
            execution_id: Some(Uuid::new_v4()),
            agent_name: Some("a".into()),
            provider: Some("openai".into()),
            org_id: Some("not-a-uuid".into()),
        },
    );
    costs.save(&event).await.unwrap();
}
