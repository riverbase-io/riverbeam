//! Evaluation datasets: `create-evaluation-dataset` and
//! `add-evaluation-case` (rc8; plan 023 D9). The rows are the ones the
//! worker's eval store reads. SQL on the command's transaction;
//! the Casbin policy keeps them to `beam-admin`.

use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use super::BeamAggregate;
use crate::commands::{AddEvaluationCasePayload, CreateEvaluationDatasetPayload};
use beam_store::repo::domain::DomainSql;

use crate::local::sql;

impl BeamAggregate {
    /// `create-evaluation-dataset`: a new, empty dataset.
    ///
    /// # Errors
    /// `WEB-001` for a blank name; store errors.
    pub async fn create_evaluation_dataset(
        &mut self,
        p: CreateEvaluationDatasetPayload,
    ) -> RiverbaseResult<Value> {
        let name = p.name.trim().to_owned();
        if name.is_empty() {
            return Err(beam_core::BEM_003.with_data("name is required"));
        }
        let metadata = p.metadata_json.unwrap_or_else(|| json!({}));
        self.db()
            .insert_dataset(Uuid::new_v4(), &name, metadata)
            .await
            .map_err(sql)?
            .ok_or_else(|| beam_core::BEM_497.with_data("the dataset was not created"))
    }

    /// `add-evaluation-case`: a case in dataset `dataset_id` (no dataset
    /// entity is registered, so the dataset is in the payload, not the URL).
    ///
    /// # Errors
    /// `BEM-501` (public `BEM-404`) for an unknown dataset; store errors.
    pub async fn add_evaluation_case(
        &mut self,
        p: AddEvaluationCasePayload,
    ) -> RiverbaseResult<Value> {
        let dataset = p.dataset_id;
        let input = serde_json::to_value(&p.input)
            .map_err(|e| beam_core::BEM_001.with_data(e.to_string()))?;
        self.db()
            .insert_case(Uuid::new_v4(), dataset, input, p.expected)
            .await
            .map_err(sql)?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({ "dataset_id": dataset })))
    }
}
