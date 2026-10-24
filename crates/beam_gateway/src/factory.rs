//! Per-invoke gateways from the catalog (Python builds a `ModelGateway` for
//! each invoke from the router's chain: the routed model plus two failovers).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beam_routing::ModelAliasMap;
use beam_runtime::{ModelCall, ModelCaller, ModelStream};
use beam_types::ModelConfig;
use riverbase_core::RiverbaseResult;

use crate::gateway::{GatewaySettings, ModelGateway, ModelProvider};
use crate::providers::ProviderFactory;

/// Failover models after the routed one.
pub const FAILOVER_MODELS: usize = 2;

/// Holds the catalog and one provider per model, and builds the gateway for
/// each invoke.
pub struct GatewayFactory {
    models: HashMap<String, ModelConfig>,
    aliases: ModelAliasMap,
    providers: Arc<dyn ProviderFactory>,
    built: Mutex<HashMap<String, Arc<dyn ModelProvider>>>,
    settings: GatewaySettings,
}

impl GatewayFactory {
    #[must_use]
    pub fn new(models: Vec<ModelConfig>, providers: Arc<dyn ProviderFactory>) -> Self {
        Self {
            models: models.into_iter().map(|m| (m.name.clone(), m)).collect(),
            aliases: ModelAliasMap::default(),
            providers,
            built: Mutex::new(HashMap::new()),
            settings: GatewaySettings::default(),
        }
    }

    #[must_use]
    pub fn with_aliases(mut self, aliases: ModelAliasMap) -> Self {
        self.aliases = aliases;
        self
    }

    #[must_use]
    pub fn with_settings(mut self, settings: GatewaySettings) -> Self {
        self.settings = settings;
        self
    }

    /// The catalog model for a name or alias.
    ///
    /// # Errors
    /// `BEM-041` for an unknown dotted alias.
    pub fn model(&self, name: &str) -> RiverbaseResult<Option<&ModelConfig>> {
        if let Some(model) = self.models.get(name) {
            return Ok(Some(model));
        }
        Ok(self.models.get(self.aliases.resolve(name)?))
    }

    fn provider(&self, model: &ModelConfig) -> RiverbaseResult<Arc<dyn ModelProvider>> {
        if let Some(found) = self
            .built
            .lock()
            .ok()
            .and_then(|b| b.get(&model.name).cloned())
        {
            return Ok(found);
        }
        let provider = self.providers.build(model)?;
        if let Ok(mut built) = self.built.lock() {
            built.insert(model.name.clone(), provider.clone());
        }
        Ok(provider)
    }

    /// The gateway for one invoke over the router's `chain`: its first model
    /// plus up to [`FAILOVER_MODELS`] more. A failover model whose provider
    /// cannot be built is skipped with a warning; the first one must build.
    ///
    /// # Errors
    /// `BEM-466` for an empty chain; the first model's build error.
    pub fn gateway(self: &Arc<Self>, chain: &[ModelConfig]) -> RiverbaseResult<RoutedGateway> {
        let Some((first, rest)) = chain.split_first() else {
            return Err(crate::BEM_466.raise());
        };
        let mut providers = vec![self.provider(first)?];
        for model in rest.iter().take(FAILOVER_MODELS) {
            match self.provider(model) {
                Ok(p) => providers.push(p),
                Err(e) => tracing::warn!(model = %model.name, error = %e, "failover model skipped"),
            }
        }
        Ok(RoutedGateway {
            factory: self.clone(),
            gateway: ModelGateway::new(providers, self.settings),
        })
    }
}

/// An invoke's gateway. A call that names another model (an agent's own
/// model alias) puts that model first, with the invoke's chain as failover.
pub struct RoutedGateway {
    factory: Arc<GatewayFactory>,
    gateway: ModelGateway,
}

impl RoutedGateway {
    #[must_use]
    pub fn inner(&self) -> &ModelGateway {
        &self.gateway
    }
}

impl ModelCaller for RoutedGateway {
    fn stream(&self, call: ModelCall) -> ModelStream {
        let wanted = call
            .model
            .as_deref()
            .filter(|m| self.gateway.chain().first().map(String::as_str) != Some(*m));
        let Some(wanted) = wanted else {
            return self.gateway.stream(call);
        };
        let chosen = match self.factory.model(wanted) {
            Ok(Some(model)) => model.clone(),
            Ok(None) => {
                let err = crate::BEM_466.with_data(format!("unknown model: {wanted}"));
                return futures_util::StreamExt::boxed(futures_util::stream::once(async move {
                    Err(err)
                }));
            }
            Err(err) => {
                return futures_util::StreamExt::boxed(futures_util::stream::once(async move {
                    Err(err)
                }));
            }
        };
        // An alias of the routed model keeps this gateway (and its circuits).
        if self.gateway.chain().first() == Some(&chosen.name) {
            return self.gateway.stream(call);
        }
        let mut names = vec![chosen.name.clone()];
        names.extend(
            self.gateway
                .chain()
                .into_iter()
                .filter(|n| *n != chosen.name),
        );
        let chain: Vec<ModelConfig> = names
            .iter()
            .filter_map(|n| self.factory.models.get(n).cloned())
            .collect();
        match self.factory.gateway(&chain) {
            Ok(routed) => routed.gateway.stream(call),
            Err(err) => {
                futures_util::StreamExt::boxed(futures_util::stream::once(async move { Err(err) }))
            }
        }
    }
}
