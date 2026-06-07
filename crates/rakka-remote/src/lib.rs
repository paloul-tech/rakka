#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Remote envelopes, typed transport errors, and payload serialization registry.

pub mod endpoint;
pub mod envelope;
pub mod error;
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
