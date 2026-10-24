//! Agent name to runtime bindings (Python `runtime/registry.py`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use riverbase_core::RiverbaseResult;

use crate::contract::{
    ensure_capability, AgentRuntime, EventStream, RuntimeContext, RuntimeExecutionRef,
    RuntimeResumeRequest, RuntimeStartRequest, RuntimeState,
};

/// Maps agent names to runtime adapters. Names that are not bound fall back to
/// the agent registry in `beam_core`, as in Python.
#[derive(Default)]
pub struct RuntimeRegistry {
    runtimes: RwLock<HashMap<String, Arc<dyn AgentRuntime>>>,
}

impl RuntimeRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(&self, agent_name: impl Into<String>, runtime: Arc<dyn AgentRuntime>) {
        if let Ok(mut map) = self.runtimes.write() {
            map.insert(agent_name.into(), runtime);
        }
    }

    pub fn unbind(&self, agent_name: &str) {
        if let Ok(mut map) = self.runtimes.write() {
            map.remove(agent_name);
        }
    }

    #[must_use]
    pub fn has(&self, agent_name: &str) -> bool {
        self.runtimes
            .read()
            .is_ok_and(|m| m.contains_key(agent_name))
    }

    #[must_use]
    pub fn bound_names(&self) -> Vec<String> {
        self.runtimes
            .read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// The runtime bound to `agent_name`, if any.
    #[must_use]
    pub fn find(&self, agent_name: &str) -> Option<Arc<dyn AgentRuntime>> {
        self.runtimes
            .read()
            .ok()
            .and_then(|m| m.get(agent_name).cloned())
    }

    /// # Errors
    /// `BEM-021` (public `BEM-404`) when nothing is bound to `agent_name`.
    pub fn get(&self, agent_name: &str) -> RiverbaseResult<Arc<dyn AgentRuntime>> {
        self.find(agent_name).ok_or_else(|| {
            crate::BEM_021.with_data(format!("No runtime bound for agent '{agent_name}'"))
        })
    }

    /// # Errors
    /// `BEM-021` when unbound.
    pub fn start(
        &self,
        agent_name: &str,
        request: RuntimeStartRequest,
        context: RuntimeContext,
    ) -> RiverbaseResult<EventStream> {
        Ok(self.get(agent_name)?.start(request, context))
    }

    /// # Errors
    /// `BEM-021` when unbound; `BEM-391` when the runtime cannot resume.
    pub fn resume(
        &self,
        agent_name: &str,
        request: RuntimeResumeRequest,
        context: RuntimeContext,
    ) -> RiverbaseResult<EventStream> {
        let runtime = self.get(agent_name)?;
        ensure_capability(runtime.as_ref(), "resume")?;
        Ok(runtime.resume(request, context))
    }

    /// # Errors
    /// `BEM-021` when unbound; `BEM-391` when the runtime cannot cancel.
    pub async fn cancel(
        &self,
        agent_name: &str,
        runtime_ref: &RuntimeExecutionRef,
        context: &RuntimeContext,
    ) -> RiverbaseResult<()> {
        let runtime = self.get(agent_name)?;
        ensure_capability(runtime.as_ref(), "cancellation")?;
        runtime.cancel(runtime_ref, context).await
    }

    /// # Errors
    /// `BEM-021` when unbound; `BEM-391` when the runtime cannot inspect.
    pub async fn inspect(
        &self,
        agent_name: &str,
        runtime_ref: &RuntimeExecutionRef,
        context: &RuntimeContext,
    ) -> RiverbaseResult<Option<RuntimeState>> {
        let runtime = self.get(agent_name)?;
        ensure_capability(runtime.as_ref(), "state_inspection")?;
        runtime.inspect(runtime_ref, context).await
    }
}
