#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Remote transport and serialization foundation.

use prost::Message;
use rakka_core::Subsystem;
use serde::{Deserialize, Serialize};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-remote";

/// Default codec id for remote messages.
pub const DEFAULT_CODEC_ID: &str = "protobuf";

/// Subsystem associated with remoting.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Remote
}

/// Metadata carried by remote message envelopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteEnvelopeMetadata {
    /// Stable message type identifier.
    pub message_type_id: String,
    /// Version of the serialized message schema.
    pub schema_version: u32,
    /// Codec selected from the serialization registry.
    pub codec_id: String,
}

impl RemoteEnvelopeMetadata {
    /// Creates metadata for a Protobuf encoded remote message.
    #[must_use]
    pub fn protobuf(message_type_id: impl Into<String>, schema_version: u32) -> Self {
        Self {
            message_type_id: message_type_id.into(),
            schema_version,
            codec_id: DEFAULT_CODEC_ID.to_string(),
        }
    }
}

/// Marker trait for messages that can use the default Protobuf codec.
pub trait ProtobufMessage: Message + Default + Send + Sync + 'static {}

impl<T> ProtobufMessage for T where T: Message + Default + Send + Sync + 'static {}
