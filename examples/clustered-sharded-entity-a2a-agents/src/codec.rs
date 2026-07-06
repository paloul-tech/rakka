//! JSON payload codec for A2A run owner remoting.
//!
//! This codec is deliberately scoped to the adapter-owned inter-node protocol,
//! not public A2A request/response bodies.

use std::marker::PhantomData;

use rakka::remote::{PayloadCodec, RemoteError, RemoteResult, SerializationRegistry};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::protocol::{
    A2ARunRequest, A2ARunResponse, A2A_RUN_REMOTE_SCHEMA_VERSION, A2A_RUN_REQUEST_TYPE_ID,
    A2A_RUN_RESPONSE_TYPE_ID,
};

const CODEC_ID: &str = "example-json";

/// JSON payload codec used by this example's trusted node-to-node remoting.
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
        CODEC_ID
    }

    fn message_type_id(&self) -> &str {
        self.message_type_id
    }

    fn schema_version(&self) -> u32 {
        self.schema_version
    }

    fn encode(&self, message: &T) -> RemoteResult<Vec<u8>> {
        serde_json::to_vec(message).map_err(|error| RemoteError::Encode {
            codec_id: CODEC_ID.to_string(),
            message: error.to_string(),
        })
    }

    fn decode(&self, payload: &[u8]) -> RemoteResult<T> {
        serde_json::from_slice(payload).map_err(|error| RemoteError::Decode {
            codec_id: CODEC_ID.to_string(),
            message: error.to_string(),
        })
    }
}

/// Builds the serialization registry used by every example node runtime.
pub fn serialization_registry() -> RemoteResult<SerializationRegistry> {
    let mut registry = SerializationRegistry::new();
    register_a2a_run_codecs(&mut registry)?;
    Ok(registry)
}

/// Registers the remote-safe A2A run request/response payloads.
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
    use rakka::remote::{EncodedPayload, RemoteEnvelopeMetadata};

    #[test]
    fn run_payloads_round_trip_through_registry() {
        let registry = serialization_registry().unwrap();
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
        let decoded = registry.decode::<A2ARunRequest>(&encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn unknown_payload_type_is_rejected() {
        let registry = serialization_registry().unwrap();
        let encoded = EncodedPayload::new(
            RemoteEnvelopeMetadata {
                message_type_id: "rakka.examples.a2a.Unknown".to_string(),
                schema_version: A2A_RUN_REMOTE_SCHEMA_VERSION,
                codec_id: CODEC_ID.to_string(),
            },
            Vec::new(),
        );

        let error = registry.decode::<A2ARunRequest>(&encoded).unwrap_err();
        assert!(matches!(error, RemoteError::UnknownCodec { .. }));
    }

    #[test]
    fn schema_version_mismatch_is_rejected() {
        let registry = serialization_registry().unwrap();
        let encoded = EncodedPayload::new(
            RemoteEnvelopeMetadata {
                message_type_id: A2A_RUN_REQUEST_TYPE_ID.to_string(),
                schema_version: A2A_RUN_REMOTE_SCHEMA_VERSION + 1,
                codec_id: CODEC_ID.to_string(),
            },
            Vec::new(),
        );

        let error = registry.decode::<A2ARunRequest>(&encoded).unwrap_err();
        assert!(matches!(error, RemoteError::UnknownCodec { .. }));
    }

    #[test]
    fn malformed_payload_is_decode_error() {
        let registry = serialization_registry().unwrap();
        let encoded = EncodedPayload::new(
            RemoteEnvelopeMetadata {
                message_type_id: A2A_RUN_REQUEST_TYPE_ID.to_string(),
                schema_version: A2A_RUN_REMOTE_SCHEMA_VERSION,
                codec_id: CODEC_ID.to_string(),
            },
            b"not-json".to_vec(),
        );

        let error = registry.decode::<A2ARunRequest>(&encoded).unwrap_err();
        assert!(matches!(error, RemoteError::Decode { .. }));
    }
}
