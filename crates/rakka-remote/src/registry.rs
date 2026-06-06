//! Pluggable payload serialization registry.

use std::any::{type_name, Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;

use crate::envelope::{EncodedPayload, RemoteEnvelope};
use crate::error::{RemoteError, RemoteResult};
use crate::DEFAULT_CODEC_ID;

/// Marker trait for messages that can use the default Protobuf payload codec.
pub trait ProtobufMessage: Message + Default + Send + Sync + 'static {}

impl<T> ProtobufMessage for T where T: Message + Default + Send + Sync + 'static {}

/// Registry key for one codec/message/schema tuple.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CodecKey {
    /// Codec id.
    pub codec_id: String,
    /// Stable message type id.
    pub message_type_id: String,
    /// Message schema version.
    pub schema_version: u32,
}

impl CodecKey {
    /// Creates a new codec key.
    #[must_use]
    pub fn new(
        codec_id: impl Into<String>,
        message_type_id: impl Into<String>,
        schema_version: u32,
    ) -> Self {
        Self {
            codec_id: codec_id.into(),
            message_type_id: message_type_id.into(),
            schema_version,
        }
    }
}

/// Typed payload codec for one Rust message type.
pub trait PayloadCodec<M>: Send + Sync + 'static
where
    M: Send + Sync + 'static,
{
    /// Codec id used in remote metadata.
    fn codec_id(&self) -> &str;

    /// Stable message type id used in remote metadata.
    fn message_type_id(&self) -> &str;

    /// Message schema version used in remote metadata.
    fn schema_version(&self) -> u32;

    /// Encodes a typed message into payload bytes.
    fn encode(&self, message: &M) -> RemoteResult<Vec<u8>>;

    /// Decodes a typed message from payload bytes.
    fn decode(&self, payload: &[u8]) -> RemoteResult<M>;
}

/// Default Protobuf payload codec for one message type.
#[derive(Debug)]
pub struct ProtobufPayloadCodec<M>
where
    M: ProtobufMessage,
{
    message_type_id: String,
    schema_version: u32,
    _message: PhantomData<fn() -> M>,
}

impl<M> ProtobufPayloadCodec<M>
where
    M: ProtobufMessage,
{
    /// Creates a Protobuf payload codec.
    #[must_use]
    pub fn new(message_type_id: impl Into<String>, schema_version: u32) -> Self {
        Self {
            message_type_id: message_type_id.into(),
            schema_version,
            _message: PhantomData,
        }
    }
}

impl<M> Clone for ProtobufPayloadCodec<M>
where
    M: ProtobufMessage,
{
    fn clone(&self) -> Self {
        Self {
            message_type_id: self.message_type_id.clone(),
            schema_version: self.schema_version,
            _message: PhantomData,
        }
    }
}

impl<M> PayloadCodec<M> for ProtobufPayloadCodec<M>
where
    M: ProtobufMessage,
{
    fn codec_id(&self) -> &str {
        DEFAULT_CODEC_ID
    }

    fn message_type_id(&self) -> &str {
        &self.message_type_id
    }

    fn schema_version(&self) -> u32 {
        self.schema_version
    }

    fn encode(&self, message: &M) -> RemoteResult<Vec<u8>> {
        let mut bytes = Vec::with_capacity(message.encoded_len());
        message
            .encode(&mut bytes)
            .map_err(|error| RemoteError::Encode {
                codec_id: DEFAULT_CODEC_ID.to_string(),
                message: error.to_string(),
            })?;
        Ok(bytes)
    }

    fn decode(&self, payload: &[u8]) -> RemoteResult<M> {
        M::decode(payload).map_err(|error| RemoteError::Decode {
            codec_id: DEFAULT_CODEC_ID.to_string(),
            message: error.to_string(),
        })
    }
}

/// Payload serialization registry.
#[derive(Default, Clone)]
pub struct SerializationRegistry {
    codecs: HashMap<CodecKey, Arc<dyn ErasedPayloadCodec>>,
    defaults: HashMap<TypeId, CodecKey>,
}

impl SerializationRegistry {
    /// Creates an empty serialization registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a typed payload codec.
    pub fn register<M, C>(&mut self, codec: C) -> RemoteResult<()>
    where
        M: Send + Sync + 'static,
        C: PayloadCodec<M>,
    {
        let key = CodecKey::new(
            codec.codec_id(),
            codec.message_type_id(),
            codec.schema_version(),
        );

        if self.codecs.contains_key(&key) {
            return Err(RemoteError::DuplicateCodec {
                codec_id: key.codec_id,
                message_type_id: key.message_type_id,
                schema_version: key.schema_version,
            });
        }

        self.defaults.insert(TypeId::of::<M>(), key.clone());
        self.codecs
            .insert(key, Arc::new(CodecEntry::<M, C>::new(codec)));
        Ok(())
    }

    /// Registers the default Protobuf payload codec for a message type.
    pub fn register_protobuf<M>(
        &mut self,
        message_type_id: impl Into<String>,
        schema_version: u32,
    ) -> RemoteResult<()>
    where
        M: ProtobufMessage,
    {
        self.register::<M, _>(ProtobufPayloadCodec::<M>::new(
            message_type_id,
            schema_version,
        ))
    }

    /// Encodes a typed message using that Rust type's default registered codec.
    pub fn encode<M>(&self, message: &M) -> RemoteResult<EncodedPayload>
    where
        M: Send + Sync + 'static,
    {
        let key = self.defaults.get(&TypeId::of::<M>()).ok_or_else(|| {
            RemoteError::UnknownMessageType {
                rust_type: type_name::<M>(),
            }
        })?;
        let codec = self.codec_for_key(key)?;
        Ok(EncodedPayload::new(
            codec.metadata(),
            codec.encode_any(message as &dyn Any)?,
        ))
    }

    /// Decodes a typed message from encoded payload metadata and bytes.
    pub fn decode<M>(&self, encoded_payload: &EncodedPayload) -> RemoteResult<M>
    where
        M: Send + Sync + 'static,
    {
        let key = encoded_payload.metadata.codec_key();
        let codec = self.codec_for_key(&key)?;
        if codec.rust_type_id() != TypeId::of::<M>() {
            return Err(RemoteError::CodecTypeMismatch {
                message_type_id: key.message_type_id,
                expected: type_name::<M>(),
            });
        }

        let message = codec.decode_any(&encoded_payload.payload)?;
        message
            .downcast::<M>()
            .map(|message| *message)
            .map_err(|_message| RemoteError::CodecTypeMismatch {
                message_type_id: encoded_payload.metadata.message_type_id.clone(),
                expected: type_name::<M>(),
            })
    }

    /// Decodes a typed message from a remote envelope.
    pub fn decode_envelope<M>(&self, envelope: &RemoteEnvelope) -> RemoteResult<M>
    where
        M: Send + Sync + 'static,
    {
        self.decode(&envelope.encoded_payload())
    }

    fn codec_for_key(&self, key: &CodecKey) -> RemoteResult<&Arc<dyn ErasedPayloadCodec>> {
        self.codecs
            .get(key)
            .ok_or_else(|| RemoteError::UnknownCodec {
                codec_id: key.codec_id.clone(),
                message_type_id: key.message_type_id.clone(),
                schema_version: key.schema_version,
            })
    }
}

trait ErasedPayloadCodec: Send + Sync {
    fn rust_type_id(&self) -> TypeId;

    fn metadata(&self) -> crate::RemoteEnvelopeMetadata;

    fn encode_any(&self, message: &dyn Any) -> RemoteResult<Vec<u8>>;

    fn decode_any(&self, payload: &[u8]) -> RemoteResult<Box<dyn Any + Send + Sync>>;
}

struct CodecEntry<M, C>
where
    M: Send + Sync + 'static,
    C: PayloadCodec<M>,
{
    codec: C,
    _message: PhantomData<fn() -> M>,
}

impl<M, C> CodecEntry<M, C>
where
    M: Send + Sync + 'static,
    C: PayloadCodec<M>,
{
    fn new(codec: C) -> Self {
        Self {
            codec,
            _message: PhantomData,
        }
    }
}

impl<M, C> ErasedPayloadCodec for CodecEntry<M, C>
where
    M: Send + Sync + 'static,
    C: PayloadCodec<M>,
{
    fn rust_type_id(&self) -> TypeId {
        TypeId::of::<M>()
    }

    fn metadata(&self) -> crate::RemoteEnvelopeMetadata {
        crate::RemoteEnvelopeMetadata {
            message_type_id: self.codec.message_type_id().to_string(),
            schema_version: self.codec.schema_version(),
            codec_id: self.codec.codec_id().to_string(),
        }
    }

    fn encode_any(&self, message: &dyn Any) -> RemoteResult<Vec<u8>> {
        let message =
            message
                .downcast_ref::<M>()
                .ok_or_else(|| RemoteError::CodecTypeMismatch {
                    message_type_id: self.codec.message_type_id().to_string(),
                    expected: type_name::<M>(),
                })?;
        self.codec.encode(message)
    }

    fn decode_any(&self, payload: &[u8]) -> RemoteResult<Box<dyn Any + Send + Sync>> {
        self.codec
            .decode(payload)
            .map(|message| Box::new(message) as Box<dyn Any + Send + Sync>)
    }
}
