//! Usage plans: `assign-usage-plan` (rc8; plan 023 D9). SQL on the
//! command's transaction; the Casbin policy keeps it to `beam-admin`.

use riverbase_core::RiverbaseResult;
use serde_json::{json, Value};
use uuid::Uuid;

use super::BeamAggregate;
use crate::commands::AssignUsagePlanPayload;
use beam_store::repo::domain::DomainSql;

use crate::local::sql;

impl BeamAggregate {
    /// `assign-usage-plan`: give `user_id` the plan in the URL. The user's
    /// other profiles are deactivated and this plan's profile is upserted
    /// active, in one statement, so the guard reads exactly one active plan.
    ///
    /// # Errors
    /// `BEM-501` (public `BEM-404`) for an unknown plan; store errors.
    pub async fn assign_usage_plan(&mut self, p: AssignUsagePlanPayload) -> RiverbaseResult<Value> {
        let plan = self.identifier();
        let org = self.caller().org_id;
        self.db()
            .assign_usage_plan(&plan, p.user_id, Uuid::new_v4(), org)
            .await
            .map_err(sql)?
            .ok_or_else(|| beam_core::BEM_501.with_data(json!({ "plan_id": plan })))
    }
}
