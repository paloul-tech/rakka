#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Remote envelopes, typed transport errors, and payload serialization registry.

pub mod endpoint;
pub mod envelope;
pub mod error;
pub mod network;
mod proto;
pub mod registry;
pub mod request;
pub mod transport;

use rakka_core::Subsystem;

pub use endpoint::{
    RemoteEndpoint, RemoteEndpointError, RemoteEndpointResult, RemoteEnvelopeHandler,
};
pub use envelope::{
    EncodedPayload, ProtobufEnvelopeCodec, RemoteDestination, RemoteEnvelope,
    RemoteEnvelopeMetadata,
};
pub use error::{RemoteError, RemoteResult};
pub use network::{
    TcpRemoteConnectionLifecycle, TcpRemoteHandshake, TcpRemotePeerSnapshot, TcpRemoteTransport,
    TcpRemoteTransportConfig, TcpRemoteTransportError, TcpRemoteTransportResult,
    TcpRemoteTransportSnapshot, DEFAULT_REMOTE_ENVELOPE_VERSION,
    METRIC_TCP_REMOTE_CONNECTION_STATE, METRIC_TCP_REMOTE_RECEIVES, METRIC_TCP_REMOTE_RECONNECTS,
    METRIC_TCP_REMOTE_SENDS,
};
pub use registry::{
    CodecKey, PayloadCodec, ProtobufMessage, ProtobufPayloadCodec, SchemaCompatibilityPolicy,
    SerializationRegistry,
};
pub use request::{
    RemotePendingReply, RemoteRequestError, RemoteRequestRegistry, RemoteRequestResult,
};
pub use transport::{
    InMemoryRemoteTransport, RemoteTransport, RemoteTransportError, RemoteTransportResult,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-remote";

/// Default codec id for remote payload messages.
pub const DEFAULT_CODEC_ID: &str = "protobuf";

/// Subsystem associated with remoting.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Remote
}
