//! `ModelGateway`: retry, timeout, circuit breaker, failover and streaming
//! (Python `routing/gateway.py`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam_runtime::{ModelCall, ModelCaller, ModelStream, ModelStreamItem};
use futures_util::StreamExt;
use riverbase_core::RiverbaseError;

/// One model the gateway can call. Every provider streams; a provider without
/// streaming yields its whole answer as the reply.
pub trait ModelProvider: Send + Sync {
    /// The catalog model name: the circuit key and the reply's `model_used`.
    fn name(&self) -> &str;
    fn stream(&self, call: &ModelCall) -> ModelStream;
}

/// Python's `ModelGateway` defaults (not configurable in Python either).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewaySettings {
    /// Extra attempts on the first model; failover models get `max(1, retries)`.
    pub retries: u32,
    /// Python times out a whole `complete` call. Rust streams every call, so
    /// this bounds the wait for each next stream item.
    pub timeout: Duration,
    /// Consecutive failures that open a model's circuit.
    pub circuit_threshold: u32,
    /// Retry a stream even after the caller saw tokens.
    pub allow_stream_retry: bool,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            retries: 2,
            timeout: Duration::from_secs(30),
            circuit_threshold: 3,
            allow_stream_retry: false,
        }
    }
}

/// Python `CircuitBreaker`, one per model. An open circuit stays open until a
/// success, and an open model is skipped, so the scope of a breaker is the
/// scope of its gateway: one invoke, as in Python.
#[derive(Debug, Default)]
pub struct Circuits {
    state: Mutex<HashMap<String, (u32, bool)>>,
}

impl Circuits {
    fn is_open(&self, model: &str) -> bool {
        self.state
            .lock()
            .is_ok_and(|s| s.get(model).is_some_and(|(_, open)| *open))
    }

    fn record_failure(&self, model: &str, threshold: u32) {
        if let Ok(mut s) = self.state.lock() {
            let entry = s.entry(model.to_owned()).or_insert((0, false));
            entry.0 += 1;
            if entry.0 >= threshold {
                entry.1 = true;
            }
        }
    }

    fn record_success(&self, model: &str) {
        if let Ok(mut s) = self.state.lock() {
            s.insert(model.to_owned(), (0, false));
        }
    }

    /// Consecutive failures recorded for `model`.
    #[must_use]
    pub fn failures(&self, model: &str) -> u32 {
        self.state
            .lock()
            .map_or(0, |s| s.get(model).map_or(0, |(n, _)| *n))
    }
}

/// The models of one invoke, in failover order, behind one [`ModelCaller`].
pub struct ModelGateway {
    chain: Vec<Arc<dyn ModelProvider>>,
    settings: GatewaySettings,
    circuits: Arc<Circuits>,
}

impl ModelGateway {
    #[must_use]
    pub fn new(chain: Vec<Arc<dyn ModelProvider>>, settings: GatewaySettings) -> Self {
        Self {
            chain,
            settings,
            circuits: Arc::new(Circuits::default()),
        }
    }

    #[must_use]
    pub fn circuits(&self) -> &Arc<Circuits> {
        &self.circuits
    }

    /// The model names in failover order.
    #[must_use]
    pub fn chain(&self) -> Vec<String> {
        self.chain.iter().map(|p| p.name().to_owned()).collect()
    }
}

enum Attempt {
    /// The reply arrived; stop.
    Done,
    /// Failed before the caller saw any text.
    Failed(RiverbaseError),
    /// Failed after the caller saw text.
    FailedVisible(RiverbaseError),
}

impl ModelCaller for ModelGateway {
    fn stream(&self, call: ModelCall) -> ModelStream {
        let chain = self.chain.clone();
        let settings = self.settings;
        let circuits = self.circuits.clone();
        async_stream::stream! {
            if chain.is_empty() {
                yield Err(crate::BEM_466.raise());
                return;
            }
            let mut last: Option<RiverbaseError> = None;
            for (index, provider) in chain.iter().enumerate() {
                let name = provider.name().to_owned();
                if circuits.is_open(&name) {
                    last = Some(crate::BEM_460.with_data(format!("model circuit open: {name}")));
                    continue;
                }
                let attempts = if index == 0 { settings.retries + 1 } else { settings.retries.max(1) };
                for _ in 0..attempts {
                    let mut stream = provider.stream(&call);
                    let mut visible = false;
                    let outcome = loop {
                        match tokio::time::timeout(settings.timeout, stream.next()).await {
                            Err(_) => {
                                let err = crate::BEM_465.with_data(format!("{name}: no reply in {:?}", settings.timeout));
                                break if visible { Attempt::FailedVisible(err) } else { Attempt::Failed(err) };
                            }
                            Ok(None) => {
                                let err = crate::BEM_464.with_data(format!("{name}: stream ended without a reply"));
                                break if visible { Attempt::FailedVisible(err) } else { Attempt::Failed(err) };
                            }
                            Ok(Some(Err(err))) => {
                                break if visible { Attempt::FailedVisible(err) } else { Attempt::Failed(err) };
                            }
                            Ok(Some(Ok(ModelStreamItem::TextDelta(text)))) => {
                                if !text.is_empty() {
                                    visible = true;
                                }
                                yield Ok(ModelStreamItem::TextDelta(text));
                            }
                            Ok(Some(Ok(ModelStreamItem::Completed(mut reply)))) => {
                                circuits.record_success(&name);
                                if reply.model_used.is_none() {
                                    reply.model_used = Some(name.clone());
                                }
                                yield Ok(ModelStreamItem::Completed(reply));
                                break Attempt::Done;
                            }
                        }
                    };
                    match outcome {
                        Attempt::Done => return,
                        // The model cannot take this prompt (an image for a
                        // text-only model, or a prompt over its window, plan
                        // 020): not a fault, so no retry and no circuit
                        // failure. An earlier model's error wins.
                        Attempt::Failed(err)
                            if err.errcode.as_str() == crate::BEM_481.errcode
                                || err.errcode.as_str() == crate::BEM_484.errcode =>
                        {
                            last = last.or(Some(err));
                            break;
                        }
                        Attempt::Failed(err) => {
                            circuits.record_failure(&name, settings.circuit_threshold);
                            tracing::warn!(model = %name, error = %err, "model call failed");
                            last = Some(err);
                        }
                        Attempt::FailedVisible(err) => {
                            circuits.record_failure(&name, settings.circuit_threshold);
                            if !settings.allow_stream_retry {
                                // The caller already showed tokens.
                                yield Err(crate::BEM_463.with_data(format!("{name}: {}", err.errmesg)));
                                return;
                            }
                            last = Some(err);
                        }
                    }
                }
            }
            yield Err(last.unwrap_or_else(|| crate::BEM_466.raise()));
        }
        .boxed()
    }
}
