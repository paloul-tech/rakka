//! JSON payload codec for the agent entity's remote command surface.
//!
//! `init_agent_entity_remote_sharding` documents its own precondition: the
//! serializable [`rakka_agent::AgentEntityCommand`] crosses the wire paired with
//! a node-local reply channel on the owner, and *the application* registers the
//! payload codecs for the command and its reply. Registering only the exchange
//! codecs leaves the agent class's remote arm unable to encode a command — the
//! one of the five remote registrations this harness could not otherwise reach,
//! because every other class is addressed by exchange envelope rather than by
//! command.
//!
//! The shape follows `examples/clustered-counter-http`'s codec; there is no
//! shared JSON payload codec in the workspace outside `rakka-a2a`, and taking a
//! dependency on that crate for forty lines would be the larger coupling.

use std::marker::PhantomData;

use rakka_remote::{PayloadCodec, RemoteError, RemoteResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Encodes a serde type as JSON for Rakka remoting.
#[derive(Debug, Clone)]
pub struct JsonPayloadCodec<T> {
    message_type_id: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T> JsonPayloadCodec<T> {
    /// A codec publishing `message_type_id` as its wire type name.
    #[must_use]
    pub const fn new(message_type_id: &'static str) -> Self {
        Self {
            message_type_id,
            marker: PhantomData,
        }
    }
}

impl<T> PayloadCodec<T> for JsonPayloadCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn codec_id(&self) -> &str {
        "multi-pod-json"
    }

    fn message_type_id(&self) -> &str {
        self.message_type_id
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn encode(&self, message: &T) -> RemoteResult<Vec<u8>> {
        serde_json::to_vec(message).map_err(|error| RemoteError::Encode {
            codec_id: self.codec_id().to_string(),
            message: error.to_string(),
        })
    }

    fn decode(&self, payload: &[u8]) -> RemoteResult<T> {
        serde_json::from_slice(payload).map_err(|error| RemoteError::Decode {
            codec_id: self.codec_id().to_string(),
            message: error.to_string(),
        })
    }
}
