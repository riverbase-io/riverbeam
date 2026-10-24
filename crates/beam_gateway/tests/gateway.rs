//! Gateway retry, circuit, failover and stream-visible rules with scripted providers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam_gateway::{GatewayFactory, GatewaySettings, ModelGateway, ModelProvider, ProviderFactory};
use beam_runtime::{ModelCall, ModelCaller, ModelReply, ModelStream, ModelStreamItem};
use beam_types::ModelConfig;
use futures_util::stream::{self, StreamExt};
use rig_agent::run::{prepare_request, RunSpec};
use rig_core::completion::{Message, ProviderCapabilities, Usage};
use rig_core::message::AssistantContent;
use riverbase_core::RiverbaseResult;
use serde_json::json;
use uuid::Uuid;

#[derive(Clone)]
enum Step {
    Fail,
    Hang,
    Text(&'static str),
    /// A delta, then a failure.
    Partial(&'static str),
    /// The model cannot take the prompt (`BEM-481`).
    Refuse,
}

/// A provider that plays one step per attempt and counts attempts.
struct Scripted {
    name: String,
    steps: Mutex<VecDeque<Step>>,
    attempts: AtomicUsize,
}

impl Scripted {
    fn new(name: &str, steps: impl IntoIterator<Item = Step>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            steps: Mutex::new(steps.into_iter().collect()),
            attempts: AtomicUsize::new(0),
        })
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[allow(clippy::unnecessary_wraps)] // An item of a stream of results.
fn reply(text: &str) -> RiverbaseResult<ModelStreamItem> {
    Ok(ModelStreamItem::Completed(ModelReply {
        choice: vec![AssistantContent::text(text)],
        usage: Usage::new(),
        finish_reason: None,
        raw: json!({}),
        model_used: None,
    }))
}

const DOWN: riverbase_core::base::ErrorSpec = riverbase_core::base::ErrorSpec {
    error_type_base: "https://riverbase.io/~/rs/error/",
    http_status: 502,
    http_title: "Bad Gateway",
    errcode: "TST-001",
    errmesg: "provider down",
    errhint: None,
};

fn fail() -> RiverbaseResult<ModelStreamItem> {
    Err(DOWN.raise())
}

const REFUSED: riverbase_core::base::ErrorSpec = riverbase_core::base::ErrorSpec {
    error_type_base: "https://riverbase.io/~/rs/error/",
    http_status: 400,
    http_title: "Bad Request",
    errcode: "BEM-481",
    errmesg: "The input uses a modality the model does not support.",
    errhint: None,
};

impl ModelProvider for Scripted {
    fn name(&self) -> &str {
        &self.name
    }

    fn stream(&self, _call: &ModelCall) -> ModelStream {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let step = self.steps.lock().unwrap().pop_front().unwrap_or(Step::Fail);
        match step {
            Step::Fail => stream::iter([fail()]).boxed(),
            Step::Hang => stream::pending().boxed(),
            Step::Text(t) => {
                stream::iter([Ok(ModelStreamItem::TextDelta(t.into())), reply(t)]).boxed()
            }
            Step::Partial(t) => {
                stream::iter([Ok(ModelStreamItem::TextDelta(t.into())), fail()]).boxed()
            }
            Step::Refuse => stream::iter([Err(REFUSED.raise())]).boxed(),
        }
    }
}

fn call(model: Option<&str>) -> ModelCall {
    ModelCall {
        execution_id: Uuid::new_v4(),
        run_id: Uuid::new_v4(),
        turn: 1,
        model: model.map(str::to_owned),
        prompt: Message::user("hi"),
        prepared: prepare_request(
            &RunSpec::new(),
            &ProviderCapabilities::default(),
            &[],
            vec![],
            None,
            None,
        )
        .unwrap(),
    }
}

/// Text deltas and the reply's model, or the error code.
async fn run(caller: &dyn ModelCaller, call: ModelCall) -> Result<(String, String), String> {
    let mut stream = caller.stream(call);
    let mut text = String::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(ModelStreamItem::TextDelta(t)) => text.push_str(&t),
            Ok(ModelStreamItem::Completed(r)) => {
                return Ok((text, r.model_used.unwrap_or_default()))
            }
            Err(e) => return Err(e.errcode.as_str().to_owned()),
        }
    }
    Err("no reply".into())
}

fn settings() -> GatewaySettings {
    GatewaySettings {
        timeout: Duration::from_millis(200),
        ..GatewaySettings::default()
    }
}

#[tokio::test]
async fn two_failures_then_success_takes_three_attempts() {
    let primary = Scripted::new("main", [Step::Fail, Step::Fail, Step::Text("ok")]);
    let gateway = ModelGateway::new(vec![primary.clone()], settings());
    assert_eq!(
        run(&gateway, call(None)).await,
        Ok(("ok".into(), "main".into()))
    );
    assert_eq!(primary.attempts(), 3);
    assert_eq!(
        gateway.circuits().failures("main"),
        0,
        "a success closes the circuit"
    );
}

#[tokio::test]
async fn an_open_circuit_fails_over_to_the_next_model() {
    // retries = 2: three failures on the first model open its circuit.
    let primary = Scripted::new("main", [Step::Fail, Step::Fail, Step::Fail]);
    let backup = Scripted::new("backup", [Step::Text("from backup")]);
    let gateway = ModelGateway::new(vec![primary.clone(), backup.clone()], settings());
    assert_eq!(
        run(&gateway, call(None)).await,
        Ok(("from backup".into(), "backup".into()))
    );
    assert_eq!((primary.attempts(), backup.attempts()), (3, 1));
    // The next call skips the open circuit.
    let gateway_backup_again = backup.clone();
    gateway_backup_again
        .steps
        .lock()
        .unwrap()
        .push_back(Step::Text("again"));
    assert_eq!(
        run(&gateway, call(None)).await,
        Ok(("again".into(), "backup".into()))
    );
    assert_eq!(primary.attempts(), 3);
}

#[tokio::test]
async fn a_failure_after_visible_tokens_is_not_retried() {
    let primary = Scripted::new("main", [Step::Partial("half"), Step::Text("never")]);
    let backup = Scripted::new("backup", [Step::Text("never")]);
    let gateway = ModelGateway::new(vec![primary.clone(), backup.clone()], settings());
    assert_eq!(run(&gateway, call(None)).await, Err("BEM-463".into()));
    assert_eq!((primary.attempts(), backup.attempts()), (1, 0));
}

#[tokio::test]
async fn a_silent_provider_times_out_and_is_retried() {
    let primary = Scripted::new("main", [Step::Hang, Step::Text("ok")]);
    let gateway = ModelGateway::new(vec![primary.clone()], settings());
    assert_eq!(
        run(&gateway, call(None)).await,
        Ok(("ok".into(), "main".into()))
    );
    assert_eq!(primary.attempts(), 2);
}

#[tokio::test]
async fn every_model_failing_returns_the_last_error() {
    let primary = Scripted::new("main", []);
    let backup = Scripted::new("backup", []);
    let gateway = ModelGateway::new(vec![primary.clone(), backup.clone()], settings());
    assert_eq!(run(&gateway, call(None)).await, Err("TST-001".into()));
    // Failover models get max(1, retries) attempts, as in Python.
    assert_eq!((primary.attempts(), backup.attempts()), (3, 2));
    assert_eq!(
        run(&ModelGateway::new(vec![], settings()), call(None)).await,
        Err("BEM-466".into())
    );
}

#[tokio::test]
async fn a_model_that_cannot_take_the_prompt_is_skipped_without_a_failure() {
    let primary = Scripted::new("text-only", [Step::Refuse]);
    let backup = Scripted::new("vision", [Step::Text("a cat")]);
    let gateway = ModelGateway::new(vec![primary.clone(), backup.clone()], settings());
    assert_eq!(
        run(&gateway, call(None)).await,
        Ok(("a cat".into(), "vision".into()))
    );
    assert_eq!((primary.attempts(), backup.attempts()), (1, 1));
    assert_eq!(gateway.circuits().failures("text-only"), 0);

    // An earlier model's real failure is the error the caller sees.
    let down = Scripted::new("main", []);
    let refuses = Scripted::new("text-only", [Step::Refuse]);
    let gateway = ModelGateway::new(vec![down, refuses.clone()], settings());
    assert_eq!(run(&gateway, call(None)).await, Err("TST-001".into()));
    assert_eq!(refuses.attempts(), 1);
}

fn model(name: &str) -> ModelConfig {
    serde_json::from_value(json!({"name": name, "provider": "openai"})).unwrap()
}

/// Builds the scripted providers by name.
struct Providers(Vec<Arc<Scripted>>);

impl ProviderFactory for Providers {
    fn build(&self, model: &ModelConfig) -> RiverbaseResult<Arc<dyn ModelProvider>> {
        let found = self
            .0
            .iter()
            .find(|p| p.name == model.name)
            .cloned()
            .unwrap();
        Ok(found)
    }
}

#[tokio::test]
async fn an_agent_model_alias_goes_first() {
    let main = Scripted::new("main", [Step::Text("main")]);
    let big = Scripted::new("big", [Step::Text("big")]);
    let factory = Arc::new(
        GatewayFactory::new(
            vec![model("main"), model("big")],
            Arc::new(Providers(vec![main.clone(), big.clone()])),
        )
        .with_aliases(beam_routing::ModelAliasMap::new(
            [("agent.smart".to_owned(), "big".to_owned())].into(),
        )),
    );
    let routed = factory.gateway(&[model("main"), model("big")]).unwrap();
    assert_eq!(routed.inner().chain(), ["main", "big"]);
    assert_eq!(
        run(&routed, call(Some("agent.smart"))).await,
        Ok(("big".into(), "big".into()))
    );
    assert_eq!(
        run(&routed, call(None)).await,
        Ok(("main".into(), "main".into()))
    );
    assert_eq!(
        run(&routed, call(Some("agent.unknown"))).await,
        Err("BEM-041".into())
    );
}
