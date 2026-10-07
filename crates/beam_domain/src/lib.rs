//! Command/query boundary between the API process and the worker.
//!
//! The `invoke` / `stream` / `resume` commands build an [`InvokeRequest`] and
//! hand it to a [`BeamWorkerClient`] (the Redis/NATS RPC boundary). `request`
//! is the synchronous round-trip used by `invoke`; `send` is fire-and-forget
//! used by the streaming commands, whose chunks arrive over the SSE bridge.

mod authz;
mod catalog;
mod client;
mod domain;
mod nats;

pub use authz::{
    activity_for_command, build_beam_authorizer, command_activity, object_from_payload,
    subject_from_payload, ActivityAuthorizer, ActivityRequest, BeamPolicyAuthorizer,
    CasbinActivityAuthorizer, DomainActivity, BEAM_POLICY_CSV, BEAM_RESOURCE,
};
pub use catalog::{AgentInfo, Catalog, ModelInfo, SessionInfo};
pub use client::{BeamWorkerClient, LocalWorkerClient};
pub use domain::{
    BeamCommandEngine, BeamDomain, BeamDomainBundle, BeamQueryEngine, InvokePayload, ResumePayload,
    StreamAck, BEAM_NAMESPACE,
};
pub use nats::{
    decode_reply, encode_err, encode_ok, task_subject, NatsWorkerClient, DEFAULT_TASK_PREFIX,
};
