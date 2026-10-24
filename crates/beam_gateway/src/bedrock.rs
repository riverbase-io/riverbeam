//! AWS Bedrock (Converse) through rig-bedrock.
//!
//! Python's Bedrock provider takes `provider_config.region_name` and passes the
//! resolved key as a Bedrock API key (`AWS_BEARER_TOKEN_BEDROCK`). Rust does the
//! same; without a key it falls back to the AWS SDK's default chain (IAM role,
//! profile, environment).

use aws_sdk_bedrockruntime::config::{BehaviorVersion, Region, Token};
use aws_smithy_runtime_api::client::auth::http::HTTP_BEARER_AUTH_SCHEME_ID;
use beam_types::ModelConfig;
use rig_bedrock::client::BedrockRuntime;
use riverbase_core::RiverbaseResult;

use crate::credentials::{model_id, resolved_api_key};
use crate::providers::RigModelProvider;

fn config_str<'a>(model: &'a ModelConfig, key: &str) -> Option<&'a str> {
    model
        .provider_config
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

pub(crate) fn provider(model: &ModelConfig) -> RiverbaseResult<RigModelProvider> {
    let region = config_str(model, "region_name").ok_or_else(|| {
        crate::BEM_461.with_data(format!(
            "Bedrock provider requires provider_config.region_name (model {})",
            model.name
        ))
    })?;
    let runtime = match resolved_api_key(model) {
        Some(key) => {
            let mut builder = aws_sdk_bedrockruntime::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new(region.to_owned()))
                .bearer_token(Token::new(key, None))
                .auth_scheme_preference([HTTP_BEARER_AUTH_SCHEME_ID]);
            // A private endpoint (or a test double).
            if let Some(url) = config_str(model, "endpoint_url") {
                builder = builder.endpoint_url(url);
            }
            BedrockRuntime::from(aws_sdk_bedrockruntime::Client::from_conf(builder.build()))
        }
        None => BedrockRuntime::builder().region(region).build(),
    };
    Ok(RigModelProvider::new(
        model.name.clone(),
        runtime.completion(model_id(model)).erase(),
    ))
}
