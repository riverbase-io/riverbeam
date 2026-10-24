//! Durable stream frames, dead letters, reconcile, retention, replay and
//! health (ported from fluvius-beam `orchestration/invocation/dispatch.py`,
//! `store/ops/relay.py`, `orchestration/tasks/{reconcile,retention}.py`,
//! `stream/sse.py`, `store/ops/health.py`).
//!
//! Split by concern: delivery, the outbox relay and dead
//! letters, reconcile, replay, and the data and health operations.

mod data;
mod delivery;
mod outbox;
mod reconcile;
mod replay;
#[cfg(test)]
mod tests;

pub(super) use delivery::PendingDone;
