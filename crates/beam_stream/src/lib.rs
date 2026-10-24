//! Agent stream transport with chunk buffering.
//!
//! The transport is abstracted behind [`StreamTransport`] so the wire binding
//! (NATS / Redis / SSE bridge) is pluggable; tests use [`InMemoryTransport`].

#[allow(clippy::wildcard_imports)] // The error catalog (`beam_errors`).
pub(crate) use beam_errors::*;

mod broadcast;
pub mod channel;
pub mod durable;
mod nats;
pub mod replay;
mod stream;
mod transport;

pub use broadcast::BroadcastTransport;
pub use nats::{subject_for_channel, NatsTransport, DEFAULT_STREAM_PREFIX};
pub use stream::{BeamStream, StreamConfig};
pub use transport::{InMemoryTransport, PublishedFrame, StreamTransport};
