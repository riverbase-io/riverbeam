//! Seeds the security knowledge base into a `river_reed` store.

use std::collections::HashMap;

use river_reed::{EngineKind, RuleManager, RuleResult};
use uuid::Uuid;

use crate::grl;

/// Resolved knowledge-base + per-rule-set identifiers, keyed by rule set name.
pub struct SeededRules {
    pub knowledge_base_id: Uuid,
    pub rule_sets: HashMap<&'static str, Uuid>,
}

/// Create one `Grl` knowledge base with one rule set per security domain.
pub async fn seed(manager: &RuleManager) -> RuleResult<SeededRules> {
    let kb = manager
        .create_knowledge_base("beam_security", EngineKind::Grl, None)
        .await?;

    let mut rule_sets = HashMap::new();
    for (name, rules) in grl::rulesets() {
        let rs = manager.create_rule_set(kb.id, name, None, None).await?;
        for (rule_name, statement, priority) in rules {
            manager
                .create_rule(rs.id, rule_name, statement, priority, None)
                .await?;
        }
        rule_sets.insert(name, rs.id);
    }

    Ok(SeededRules {
        knowledge_base_id: kb.id,
        rule_sets,
    })
}
