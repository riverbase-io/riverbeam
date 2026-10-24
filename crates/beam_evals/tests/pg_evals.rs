//! Eval datasets and runs on Postgres: what one store saves,
//! another store on the same database reads, as another worker would.
//! Before, datasets and runs lived in the saving process's memory. Needs
//! `BEAM_DATABASE_URL`.

use beam_evals::{
    EvaluationDataset, EvaluationResult, EvaluationRun, EvaluationScore, EvaluationStore,
    PgEvaluationStore,
};
use beam_types::AgentInput;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn datasets_and_runs_are_shared_by_stores() {
    let Some(pool) = beam_store::testdb::pool("beam_evals").await else {
        return;
    };
    let a = PgEvaluationStore::for_store(&beam_store::PgBeamStore::new(
        pool.clone(),
        beam_store::testdb::schema(),
    ));
    let b = PgEvaluationStore::for_store(&beam_store::PgBeamStore::new(
        pool,
        beam_store::testdb::schema(),
    ));

    let mut dataset = EvaluationDataset::new(format!("smoke-{}", Uuid::new_v4().simple()));
    let first = dataset.push(AgentInput::text("hello"), Some(json!({"text": "ok"})));
    let second = dataset.push(AgentInput::text("again"), None);
    a.save_dataset(&dataset).await.expect("save");
    let read = b
        .get_dataset(dataset.id)
        .await
        .expect("read")
        .expect("shared");
    assert_eq!(read.name, dataset.name);
    let ids: Vec<Uuid> = read.cases.iter().map(|c| c.id).collect();
    assert_eq!(ids, vec![first, second], "the cases keep their order");
    assert_eq!(read.cases[0].expected, Some(json!({"text": "ok"})));

    // Saving again replaces the cases: the one dropped from the dataset goes.
    dataset.cases.truncate(1);
    a.save_dataset(&dataset).await.expect("save again");
    assert_eq!(
        b.get_dataset(dataset.id)
            .await
            .unwrap()
            .unwrap()
            .cases
            .len(),
        1
    );

    let revision = Uuid::new_v4();
    let run = EvaluationRun {
        id: Uuid::new_v4(),
        dataset_id: dataset.id,
        agent_revision_id: revision,
        results: vec![EvaluationResult {
            case_id: first,
            score: EvaluationScore {
                passed: true,
                score: Some(1.0),
                details: serde_json::Map::new(),
            },
        }],
    };
    a.save_run(&run).await.expect("save run");
    let last = b
        .last_run_for_revision(revision)
        .await
        .expect("read")
        .expect("another store sees the run");
    assert_eq!(last, run);
    assert!(last.passed());
    assert_eq!(b.get_run(run.id).await.unwrap(), Some(run));
}
