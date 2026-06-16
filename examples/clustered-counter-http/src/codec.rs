//! JSON payload codec for Rakka remoting.

use std::marker::PhantomData;

use rakka::remote::{PayloadCodec, RemoteError, RemoteResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct JsonPayloadCodec<T> {
    message_type_id: &'static str,
    _marker: PhantomData<fn() -> T>,
}

impl<T> JsonPayloadCodec<T> {
    pub fn new(message_type_id: &'static str) -> Self {
        Self {
            message_type_id,
            _marker: PhantomData,
        }
    }
}

impl<T> PayloadCodec<T> for JsonPayloadCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    // This codec is for Rakka's internal TCP remoting, not for Axum request
    // bodies. Keeping it small makes the REST and remoting boundaries distinct.
    fn codec_id(&self) -> &str {
        "example-json"
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
