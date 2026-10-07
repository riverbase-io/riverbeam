/// Runtime configuration (ports riverbeam `_meta/defaults.py` subset).
#[derive(Debug, Clone)]
pub struct BeamConfig {
    pub worker_namespace: String,
    pub sql_schema: String,
    pub knowledge_enabled: bool,
    pub knowledge_embedding_dim: u32,
    pub knowledge_max_context_tokens: u32,
    pub default_model: Option<String>,
    pub security_enabled: bool,
    pub usage_enabled: bool,
}

impl Default for BeamConfig {
    fn default() -> Self {
        Self {
            worker_namespace: "riverbeam-worker".into(),
            sql_schema: "ref_beam".into(),
            knowledge_enabled: true,
            knowledge_embedding_dim: 1536,
            knowledge_max_context_tokens: 2000,
            default_model: None,
            security_enabled: true,
            usage_enabled: true,
        }
    }
}
