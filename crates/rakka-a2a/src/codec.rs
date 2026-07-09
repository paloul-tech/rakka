//! JSON payload codec registration for the crate-owned remote protocol.
//!
//! This codec is deliberately scoped to the adapter-owned inter-node owner
//! protocol, not public A2A request/response bodies. The codec id and the
//! registered message type ids are compatibility commitments across rolling
//! updates: nodes on adjacent versions must agree on them to interoperate.

use std::marker::PhantomData;

use rakka_remote::{PayloadCodec, RemoteError, RemoteResult, SerializationRegistry};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::protocol::{
    A2ARunRequest, A2ARunResponse, A2A_RUN_REMOTE_SCHEMA_VERSION, A2A_RUN_REQUEST_TYPE_ID,
    A2A_RUN_RESPONSE_TYPE_ID,
};

/// Stable codec id registered for the A2A run owner protocol.
pub const A2A_RUN_CODEC_ID: &str = "rakka-a2a-json";

/// JSON payload codec for one stable remote message type.
#[derive(Debug, Clone)]
pub struct JsonPayloadCodec<T> {
    message_type_id: &'static str,
    schema_version: u32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> JsonPayloadCodec<T> {
    /// Creates a JSON codec for one stable remote message type.
    #[must_use]
    pub const fn new(message_type_id: &'static str, schema_version: u32) -> Self {
        Self {
            message_type_id,
            schema_version,
            _marker: PhantomData,
        }
    }
}

impl<T> PayloadCodec<T> for JsonPayloadCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn codec_id(&self) -> &str {
        A2A_RUN_CODEC_ID
    }

    fn message_type_id(&self) -> &str {
        self.message_type_id
    }

    fn schema_version(&self) -> u32 {
        self.schema_version
    }

    fn encode(&self, message: &T) -> RemoteResult<Vec<u8>> {
        serde_json::to_vec(message).map_err(|error| RemoteError::Encode {
            codec_id: A2A_RUN_CODEC_ID.to_string(),
            message: error.to_string(),
        })
    }

    fn decode(&self, payload: &[u8]) -> RemoteResult<T> {
        serde_json::from_slice(payload).map_err(|error| RemoteError::Decode {
            codec_id: A2A_RUN_CODEC_ID.to_string(),
            message: error.to_string(),
        })
    }
}

/// Registers the remote-safe A2A run request/response payloads.
///
/// Call this on every node that participates in A2A owner routing, alongside
/// the application's own codec registrations.
pub fn register_a2a_run_codecs(registry: &mut SerializationRegistry) -> RemoteResult<()> {
    registry.register::<A2ARunRequest, _>(JsonPayloadCodec::<A2ARunRequest>::new(
        A2A_RUN_REQUEST_TYPE_ID,
        A2A_RUN_REMOTE_SCHEMA_VERSION,
    ))?;
    registry.register::<A2ARunResponse, _>(JsonPayloadCodec::<A2ARunResponse>::new(
        A2A_RUN_RESPONSE_TYPE_ID,
        A2A_RUN_REMOTE_SCHEMA_VERSION,
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        A2AProjectionHints, A2ARunCommandMetadata, A2ARunRequestKind, A2ATimeoutPolicy,
    };
    use rakka_remote::{EncodedPayload, RemoteEnvelopeMetadata};

    fn registry() -> SerializationRegistry {
        let mut registry = SerializationRegistry::new();
        register_a2a_run_codecs(&mut registry).expect("register codecs");
        registry
    }

    #[test]
    fn run_payloads_round_trip_through_registry() {
        let registry = registry();
        let request = A2ARunRequest::new(
            "task-1",
            Some("tenant-a".to_string()),
            A2ARunCommandMetadata::query(),
            A2AProjectionHints::default(),
            A2ATimeoutPolicy {
                ask_timeout_millis: 500,
            },
            A2ARunRequestKind::QueryTaskSnapshot,
        );

        let encoded = registry.encode(&request).unwrap();
        assert_eq!(encoded.metadata.message_type_id, A2A_RUN_REQUEST_TYPE_ID);
        let decoded = registry.decode::<A2ARunRequest>(&encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn unknown_payload_type_is_rejected() {
        let registry = registry();
        let encoded = EncodedPayload::new(
            RemoteEnvelopeMetadata {
                message_type_id: "rakka.a2a.Unknown".to_string(),
                schema_version: A2A_RUN_REMOTE_SCHEMA_VERSION,
                codec_id: A2A_RUN_CODEC_ID.to_string(),
            },
            Vec::new(),
        );

        let error = registry.decode::<A2ARunRequest>(&encoded).unwrap_err();
        assert!(matches!(error, RemoteError::UnknownCodec { .. }));
    }

    #[test]
    fn schema_version_mismatch_is_rejected() {
        let registry = registry();
        let encoded = EncodedPayload::new(
            RemoteEnvelopeMetadata {
                message_type_id: A2A_RUN_REQUEST_TYPE_ID.to_string(),
                schema_version: A2A_RUN_REMOTE_SCHEMA_VERSION + 1,
                codec_id: A2A_RUN_CODEC_ID.to_string(),
            },
            Vec::new(),
        );

        let error = registry.decode::<A2ARunRequest>(&encoded).unwrap_err();
        assert!(matches!(error, RemoteError::UnknownCodec { .. }));
    }

    #[test]
    fn malformed_payload_is_decode_error() {
        let registry = registry();
        let encoded = EncodedPayload::new(
            RemoteEnvelopeMetadata {
                message_type_id: A2A_RUN_REQUEST_TYPE_ID.to_string(),
                schema_version: A2A_RUN_REMOTE_SCHEMA_VERSION,
                codec_id: A2A_RUN_CODEC_ID.to_string(),
            },
            b"not-json".to_vec(),
        );

        let error = registry.decode::<A2ARunRequest>(&encoded).unwrap_err();
        assert!(matches!(error, RemoteError::Decode { .. }));
    }
}
