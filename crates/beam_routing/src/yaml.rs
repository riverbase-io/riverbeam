use std::collections::HashMap;

use beam_types::ModelConfig;
use riverbase_core::RiverbaseResult;

use crate::aliases::ModelAliasMap;

#[derive(serde::Deserialize)]
struct ModelsFile {
    #[serde(default)]
    models: Vec<ModelConfig>,
    #[serde(default)]
    aliases: HashMap<String, String>,
}

fn load(path: &str) -> RiverbaseResult<ModelsFile> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| crate::BEM_043.with_data(format!("read {path}: {e}")))?;
    serde_yaml::from_str(&raw).map_err(|e| crate::BEM_044.with_data(e.to_string()))
}

/// The `models` list of a catalog file.
///
/// # Errors
/// `BEM-043` when the file cannot be read; `BEM-044` when it does not parse.
pub fn load_models_from_yaml(path: &str) -> RiverbaseResult<Vec<ModelConfig>> {
    Ok(load(path)?.models)
}

/// The optional `aliases` map of a catalog file (`chat.default: gpt-x`).
///
/// # Errors
/// `BEM-043` when the file cannot be read; `BEM-044` when it does not parse.
pub fn load_aliases_from_yaml(path: &str) -> RiverbaseResult<ModelAliasMap> {
    Ok(ModelAliasMap::new(load(path)?.aliases))
}
