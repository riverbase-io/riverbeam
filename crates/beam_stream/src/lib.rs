//! Agent stream transport with chunk buffering.
//!
//! The transport is abstracted behind [`StreamTransport`] so the wire binding
//! (NATS / Redis / SSE bridge) is pluggable; tests use [`InMemoryTransport`].

mod errors;
#[allow(unused_imports)]
pub(crate) use errors::*;

mod broadcast;
mod nats;
mod stream;
mod transport;

pub use broadcast::BroadcastTransport;
pub use nats::{subject_for_channel, NatsTransport, DEFAULT_STREAM_PREFIX};
pub use stream::{BeamStream, StreamConfig};
pub use transport::{InMemoryTransport, PublishedFrame, StreamTransport};
