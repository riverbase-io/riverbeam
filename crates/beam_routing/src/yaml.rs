use beam_types::ModelConfig;
use riverbase_core::RiverbaseResult;

#[derive(serde::Deserialize)]
struct ModelsFile {
    models: Vec<ModelConfig>,
}

pub fn load_models_from_yaml(path: &str) -> RiverbaseResult<Vec<ModelConfig>> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| crate::BEM_043.with_data(format!("read {path}: {e}")))?;
    let parsed: ModelsFile =
        serde_yaml::from_str(&raw).map_err(|e| crate::BEM_044.with_data(e.to_string()))?;
    Ok(parsed.models)
}
