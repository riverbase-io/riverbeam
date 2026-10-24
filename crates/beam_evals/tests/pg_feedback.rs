//! Feedback rows on Postgres (`agent_feedback`, read by Python's feedback
//! query and `scripts/cross_read.py`). Needs `BEAM_DATABASE_URL`.

use beam_evals::{AgentFeedback, EvaluationStore, PgEvaluationStore};
use uuid::Uuid;

#[tokio::test]
async fn feedback_roundtrips() {
    let Some(pool) = beam_store::testdb::pool("beam_evals").await else {
        return;
    };
    let schema = beam_store::testdb::schema();
    let store = PgEvaluationStore::for_store(&beam_store::PgBeamStore::new(pool, schema));
    let execution_id = Uuid::new_v4();
    let feedback = AgentFeedback {
        id: Uuid::new_v4(),
        execution_id,
        agent_revision_id: Some(Uuid::new_v4()),
        rating: Some(4),
        thumb: Some("up".into()),
        comment: Some("good ✓".into()),
    };
    store.save_feedback(&feedback).await.expect("save");
    assert_eq!(
        store.list_feedback(execution_id).await.expect("list"),
        vec![feedback]
    );
}
