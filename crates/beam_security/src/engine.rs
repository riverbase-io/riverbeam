//! GRL-backed security engine.
//!
//! `river_reed`'s [`RuleManager`] is not `Send` (its `EvaluatorRegistry`
//! holds `Arc<dyn RuleEvaluator>`, a non-`Send` trait object), so it cannot be
//! held inside the `Send` async invoke pipeline. We isolate it on a dedicated
//! OS thread running a current-thread Tokio runtime: the seeded manager lives
//! there for the engine's lifetime and only JSON facts / coded narrations
//! cross the channel boundary, keeping [`SecurityEngine::evaluate`] `Send`.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use river_reed::{CollectorRegistry, EvaluatorRegistry, RuleManager};

use crate::mem_store::InMemoryRuleStore;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::codes::RuleNarration;
use crate::enums::SecurityStage;
use crate::grl::{self, narration_from_wm};
use crate::seed;

type EvalResult = Result<Vec<RuleNarration>, String>;

struct Job {
    knowledge_base_id: Uuid,
    rule_set_id: Uuid,
    facts: Value,
    reply: oneshot::Sender<EvalResult>,
}

/// Owns the seeded GRL rule manager on a dedicated thread.
pub struct SecurityEngine {
    tx: mpsc::UnboundedSender<Job>,
    stage_rule_sets: HashMap<SecurityStage, (Uuid, Uuid)>,
    ready: bool,
    _worker: JoinHandle<()>,
}

impl SecurityEngine {
    /// Spawn the GRL worker thread and seed the knowledge base.
    ///
    /// Seeding happens synchronously (via a oneshot) so the resulting stage map
    /// is available immediately; the worker then serves evaluation jobs.
    #[must_use]
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<HashMap<&'static str, Uuid>>();
        let (kb_tx, kb_rx) = std::sync::mpsc::channel::<Uuid>();

        let worker = std::thread::Builder::new()
            .name("beam-security-grl".to_string())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread().build() {
                    Ok(rt) => rt,
                    Err(_) => {
                        drop(ready_tx);
                        return;
                    }
                };
                rt.block_on(async move {
                    let store = Arc::new(InMemoryRuleStore::new());
                    let manager =
                        RuleManager::new(store, EvaluatorRegistry::default(), CollectorRegistry::new());

                    let seeded = match seed::seed(&manager).await {
                        Ok(s) => s,
                        Err(_) => {
                            drop(ready_tx);
                            return;
                        }
                    };
                    let _ = kb_tx.send(seeded.knowledge_base_id);
                    if ready_tx.send(seeded.rule_sets).is_err() {
                        return;
                    }

                    while let Some(job) = rx.recv().await {
                        let res = manager
                            .evaluate_rule_set(job.knowledge_base_id, job.rule_set_id, job.facts)
                            .await
                            .map(|hm| narrations_from_eval(&hm))
                            .map_err(|e| e.to_string());
                        let _ = job.reply.send(res);
                    }
                });
            })
            .expect("spawn beam-security-grl thread");

        let (stage_rule_sets, ready) = match (ready_rx.recv(), kb_rx.recv()) {
            (Ok(rule_sets), Ok(kb_id)) => (build_stage_map(kb_id, &rule_sets), true),
            _ => (HashMap::new(), false),
        };

        Self {
            tx,
            stage_rule_sets,
            ready,
            _worker: worker,
        }
    }

    /// `true` once the knowledge base seeded successfully.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// Evaluate `facts` for `stage`, returning the coded narrations the rules
    /// produced. Passthrough / unrouted stages yield an empty narration set.
    pub async fn evaluate(&self, stage: SecurityStage, facts: Value) -> EvalResult {
        let Some(&(kb_id, rs_id)) = self.stage_rule_sets.get(&stage) else {
            return Ok(Vec::new());
        };
        let (reply, reply_rx) = oneshot::channel();
        self.tx
            .send(Job {
                knowledge_base_id: kb_id,
                rule_set_id: rs_id,
                facts,
                reply,
            })
            .map_err(|_| "security engine worker stopped".to_string())?;
        reply_rx
            .await
            .map_err(|_| "security engine dropped reply".to_string())?
    }
}

impl Default for SecurityEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn build_stage_map(
    kb_id: Uuid,
    rule_sets: &HashMap<&'static str, Uuid>,
) -> HashMap<SecurityStage, (Uuid, Uuid)> {
    const STAGES: &[SecurityStage] = &[
        SecurityStage::BeforeInput,
        SecurityStage::BeforeContextInjection,
        SecurityStage::AfterLlm,
        SecurityStage::BeforeOutput,
        SecurityStage::BeforeTool,
        SecurityStage::BeforeMemoryWrite,
        SecurityStage::Runtime,
    ];
    let mut map = HashMap::new();
    for &stage in STAGES {
        if let Some(rs_name) = grl::ruleset_for_stage(stage) {
            if let Some(&rs_id) = rule_sets.get(rs_name) {
                map.insert(stage, (kb_id, rs_id));
            }
        }
    }
    map
}

/// Adapter: `{rule_name -> working_memory}` -> coded narrations.
#[must_use]
pub fn narrations_from_eval(results: &HashMap<String, Value>) -> Vec<RuleNarration> {
    results.values().filter_map(narration_from_wm).collect()
}
