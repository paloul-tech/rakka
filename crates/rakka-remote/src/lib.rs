#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Remote envelopes, typed transport errors, and payload serialization registry.

pub mod envelope;
pub mod error;
mod proto;
pub mod registry;

use rakka_core::Subsystem;

pub use envelope::{
    EncodedPayload, ProtobufEnvelopeCodec, RemoteDestination, RemoteEnvelope,
    RemoteEnvelopeMetadata,
};
pub use error::{RemoteError, RemoteResult};
pub use registry::{
    CodecKey, PayloadCodec, ProtobufMessage, ProtobufPayloadCodec, SerializationRegistry,
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
