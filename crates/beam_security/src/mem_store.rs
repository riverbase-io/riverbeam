//! In-process [`RuleStore`] — `river_reed` no longer ships one.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use river_reed::{
    KnowledgeBase, Rule, RuleBatch, RuleError, RuleEval, RuleNarration, RuleResult, RuleSet,
    RuleStore,
};
use uuid::Uuid;

#[derive(Default)]
struct Inner {
    kbs: HashMap<Uuid, KnowledgeBase>,
    rule_sets: HashMap<Uuid, RuleSet>,
    rules: HashMap<Uuid, Rule>,
    batches: HashMap<Uuid, RuleBatch>,
    evals: HashMap<Uuid, RuleEval>,
    narrations: HashMap<Uuid, RuleNarration>,
}

pub struct InMemoryRuleStore {
    inner: Mutex<Inner>,
}

impl InMemoryRuleStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl Default for InMemoryRuleStore {
    fn default() -> Self {
        Self::new()
    }
}

fn lock(store: &InMemoryRuleStore) -> RuleResult<std::sync::MutexGuard<'_, Inner>> {
    store.inner.lock().map_err(|e| {
        RuleError::storage("RUL-500", "in-memory rule store poisoned", e.to_string())
    })
}

#[async_trait]
impl RuleStore for InMemoryRuleStore {
    async fn create_knowledge_base(&self, kb: KnowledgeBase) -> RuleResult<KnowledgeBase> {
        let mut g = lock(self)?;
        g.kbs.insert(kb.id, kb.clone());
        Ok(kb)
    }
    async fn get_knowledge_base(&self, id: Uuid) -> RuleResult<KnowledgeBase> {
        lock(self)?
            .kbs
            .get(&id)
            .cloned()
            .ok_or_else(|| RuleError::not_found("RUL-404", "knowledge_base not found", id.to_string()))
    }
    async fn list_knowledge_bases(&self) -> RuleResult<Vec<KnowledgeBase>> {
        Ok(lock(self)?.kbs.values().cloned().collect())
    }
    async fn update_knowledge_base(&self, kb: KnowledgeBase) -> RuleResult<KnowledgeBase> {
        let mut g = lock(self)?;
        g.kbs.insert(kb.id, kb.clone());
        Ok(kb)
    }

    async fn create_rule_set(&self, rule_set: RuleSet) -> RuleResult<RuleSet> {
        let mut g = lock(self)?;
        g.rule_sets.insert(rule_set.id, rule_set.clone());
        Ok(rule_set)
    }
    async fn get_rule_set(&self, id: Uuid) -> RuleResult<RuleSet> {
        lock(self)?
            .rule_sets
            .get(&id)
            .cloned()
            .ok_or_else(|| RuleError::not_found("RUL-404", "rule_set not found", id.to_string()))
    }
    async fn list_rule_sets(&self, knowledge_base_id: Uuid) -> RuleResult<Vec<RuleSet>> {
        Ok(lock(self)?
            .rule_sets
            .values()
            .filter(|rs| rs.knowledge_base_id == knowledge_base_id)
            .cloned()
            .collect())
    }
    async fn update_rule_set(&self, rule_set: RuleSet) -> RuleResult<RuleSet> {
        let mut g = lock(self)?;
        g.rule_sets.insert(rule_set.id, rule_set.clone());
        Ok(rule_set)
    }

    async fn create_rule(&self, rule: Rule) -> RuleResult<Rule> {
        let mut g = lock(self)?;
        g.rules.insert(rule.id, rule.clone());
        Ok(rule)
    }
    async fn get_rule(&self, id: Uuid) -> RuleResult<Rule> {
        lock(self)?
            .rules
            .get(&id)
            .cloned()
            .ok_or_else(|| RuleError::not_found("RUL-404", "rule not found", id.to_string()))
    }
    async fn list_rules(&self, rule_set_id: Uuid, only_active: bool) -> RuleResult<Vec<Rule>> {
        Ok(lock(self)?
            .rules
            .values()
            .filter(|r| r.rule_set_id == rule_set_id && (!only_active || r.active))
            .cloned()
            .collect())
    }
    async fn update_rule(&self, rule: Rule) -> RuleResult<Rule> {
        let mut g = lock(self)?;
        g.rules.insert(rule.id, rule.clone());
        Ok(rule)
    }

    async fn create_batch(&self, batch: RuleBatch) -> RuleResult<RuleBatch> {
        let mut g = lock(self)?;
        g.batches.insert(batch.id, batch.clone());
        Ok(batch)
    }
    async fn get_batch(&self, id: Uuid) -> RuleResult<RuleBatch> {
        lock(self)?
            .batches
            .get(&id)
            .cloned()
            .ok_or_else(|| RuleError::not_found("RUL-404", "batch not found", id.to_string()))
    }
    async fn update_batch(&self, batch: RuleBatch) -> RuleResult<RuleBatch> {
        let mut g = lock(self)?;
        g.batches.insert(batch.id, batch.clone());
        Ok(batch)
    }
    async fn list_batches_by_key(
        &self,
        resource_name: &str,
        knowledge_base_id: Uuid,
        collector_name: &str,
        resource_id: &str,
    ) -> RuleResult<Vec<RuleBatch>> {
        Ok(lock(self)?
            .batches
            .values()
            .filter(|b| {
                b.resource_name == resource_name
                    && b.knowledge_base_id == knowledge_base_id
                    && b.collector_name == collector_name
                    && b.resource_id == resource_id
            })
            .cloned()
            .collect())
    }

    async fn create_eval(&self, eval: RuleEval) -> RuleResult<RuleEval> {
        let mut g = lock(self)?;
        g.evals.insert(eval.id, eval.clone());
        Ok(eval)
    }
    async fn update_eval(&self, eval: RuleEval) -> RuleResult<RuleEval> {
        let mut g = lock(self)?;
        g.evals.insert(eval.id, eval.clone());
        Ok(eval)
    }
    async fn list_evals(&self, batch_id: Uuid) -> RuleResult<Vec<RuleEval>> {
        Ok(lock(self)?
            .evals
            .values()
            .filter(|e| e.batch_id == batch_id)
            .cloned()
            .collect())
    }

    async fn create_narration(&self, narration: RuleNarration) -> RuleResult<RuleNarration> {
        let mut g = lock(self)?;
        g.narrations.insert(narration.id, narration.clone());
        Ok(narration)
    }
    async fn list_narrations(&self, rule_eval_id: Uuid) -> RuleResult<Vec<RuleNarration>> {
        Ok(lock(self)?
            .narrations
            .values()
            .filter(|n| n.rule_eval_id == rule_eval_id)
            .cloned()
            .collect())
    }
}
