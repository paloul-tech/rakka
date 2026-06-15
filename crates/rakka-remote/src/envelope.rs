//! Remote envelope types and Protobuf envelope codec.

use std::str::FromStr;

use rakka_cluster::NodeId;
use rakka_core::{ActorPath, ActorUid, SerializedActorRef};
use serde::{Deserialize, Serialize};

use crate::error::{RemoteError, RemoteResult};
use crate::proto::{ProtoDestinationKind, ProtoRemoteDestination, ProtoRemoteEnvelope};
use crate::registry::CodecKey;
use crate::DEFAULT_CODEC_ID;

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

    /// Returns this metadata as a registry key.
    #[must_use]
    pub fn codec_key(&self) -> CodecKey {
        CodecKey::new(
            self.codec_id.clone(),
            self.message_type_id.clone(),
            self.schema_version,
        )
    }
}

/// Encoded payload with metadata needed to decode it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedPayload {
    /// Payload metadata.
    pub metadata: RemoteEnvelopeMetadata,
    /// Serialized payload bytes.
    pub payload: Vec<u8>,
}

impl EncodedPayload {
    /// Creates a new encoded payload.
    #[must_use]
    pub fn new(metadata: RemoteEnvelopeMetadata, payload: Vec<u8>) -> Self {
        Self { metadata, payload }
    }
}

/// Remote message destination.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum RemoteDestination {
    /// Route to a concrete actor path.
    ActorPath {
        /// Logical actor path.
        path: String,
    },
    /// Route to a sharded entity.
    Entity {
        /// Entity type name.
        entity_type: String,
        /// Entity id within the entity type.
        entity_id: String,
    },
    /// Route to a discoverable service key.
    Service {
        /// Service key.
        service_key: String,
    },
    /// Route by a generic route key.
    RouteKey {
        /// Route key.
        route_key: String,
    },
    /// Route a reply to a pending remote request.
    Reply {
        /// Request id being completed.
        request_id: String,
    },
    /// Route to a concrete actor incarnation.
    ActorRef {
        /// Remote actor incarnation descriptor.
        actor_ref: Box<RemoteActorRef>,
    },
}

impl RemoteDestination {
    /// Creates a destination for one concrete actor incarnation.
    #[must_use]
    pub fn actor_ref(actor_ref: RemoteActorRef) -> Self {
        Self::ActorRef {
            actor_ref: Box::new(actor_ref),
        }
    }
}

/// Transport-serializable descriptor for one concrete actor incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RemoteActorRef {
    node_id: NodeId,
    system_name: String,
    path: ActorPath,
    uid: ActorUid,
    message_type: String,
}

impl RemoteActorRef {
    /// Creates a remote actor-reference descriptor.
    pub fn new(
        node_id: NodeId,
        system_name: impl Into<String>,
        path: ActorPath,
        uid: ActorUid,
        message_type: impl Into<String>,
    ) -> RemoteResult<Self> {
        validate_node_id(&node_id)?;
        let system_name = require_non_empty("actor_system_name", system_name.into())?;
        let path = ActorPath::new(require_non_empty("path", path.to_string())?);
        let uid = require_non_zero("actor_uid", uid.value()).map(ActorUid::new)?;
        let message_type = require_non_empty("actor_message_type", message_type.into())?;

        Ok(Self {
            node_id,
            system_name,
            path,
            uid,
            message_type,
        })
    }

    /// Creates a remote actor-reference descriptor from a local serialized ref.
    pub fn from_serialized(node_id: NodeId, serialized: &SerializedActorRef) -> RemoteResult<Self> {
        Self::new(
            node_id,
            serialized.system_name(),
            serialized.path().clone(),
            serialized.uid(),
            serialized.message_type(),
        )
    }

    /// Node that owns this actor incarnation.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Actor system name that owns this actor.
    #[must_use]
    pub fn system_name(&self) -> &str {
        &self.system_name
    }

    /// Logical actor path.
    #[must_use]
    pub const fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Actor incarnation uid.
    #[must_use]
    pub const fn uid(&self) -> ActorUid {
        self.uid
    }

    /// Rust message type associated with this actor ref.
    #[must_use]
    pub fn message_type(&self) -> &str {
        &self.message_type
    }

    /// Converts this descriptor back to a local serialized actor ref.
    #[must_use]
    pub fn to_serialized_ref(&self) -> SerializedActorRef {
        SerializedActorRef::new(
            self.system_name.clone(),
            self.path.clone(),
            self.uid,
            self.message_type.clone(),
        )
    }
}

/// Wire envelope for remote messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteEnvelope {
    /// Optional source actor path or endpoint id.
    pub source: Option<String>,
    /// Destination to route this message to.
    pub destination: RemoteDestination,
    /// Payload metadata.
    pub metadata: RemoteEnvelopeMetadata,
    /// Optional tracing context encoded by a higher layer.
    pub trace_context: Option<String>,
    /// Optional request id used by ask/reply flows.
    pub request_id: Option<String>,
    /// Serialized payload bytes.
    pub payload: Vec<u8>,
}

impl RemoteEnvelope {
    /// Creates a new remote envelope.
    #[must_use]
    pub fn new(destination: RemoteDestination, encoded_payload: EncodedPayload) -> Self {
        Self {
            source: None,
            destination,
            metadata: encoded_payload.metadata,
            trace_context: None,
            request_id: None,
            payload: encoded_payload.payload,
        }
    }

    /// Sets the source endpoint or actor path.
    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Sets trace context metadata.
    #[must_use]
    pub fn with_trace_context(mut self, trace_context: impl Into<String>) -> Self {
        self.trace_context = Some(trace_context.into());
        self
    }

    /// Sets request id metadata.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Returns this envelope payload and metadata.
    #[must_use]
    pub fn encoded_payload(&self) -> EncodedPayload {
        EncodedPayload::new(self.metadata.clone(), self.payload.clone())
    }
}

/// Protobuf codec for remote envelopes.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProtobufEnvelopeCodec;

impl ProtobufEnvelopeCodec {
    /// Encodes a remote envelope into Protobuf bytes.
    pub fn encode(envelope: &RemoteEnvelope) -> RemoteResult<Vec<u8>> {
        let proto = ProtoRemoteEnvelope::from(envelope.clone());
        let mut bytes = Vec::with_capacity(prost::Message::encoded_len(&proto));
        prost::Message::encode(&proto, &mut bytes).map_err(|error| RemoteError::Encode {
            codec_id: DEFAULT_CODEC_ID.to_string(),
            message: error.to_string(),
        })?;
        Ok(bytes)
    }

    /// Decodes a remote envelope from Protobuf bytes.
    pub fn decode(bytes: &[u8]) -> RemoteResult<RemoteEnvelope> {
        let proto = <ProtoRemoteEnvelope as prost::Message>::decode(bytes).map_err(|error| {
            RemoteError::Decode {
                codec_id: DEFAULT_CODEC_ID.to_string(),
                message: error.to_string(),
            }
        })?;
        RemoteEnvelope::try_from(proto)
    }
}

impl From<RemoteEnvelope> for ProtoRemoteEnvelope {
    fn from(envelope: RemoteEnvelope) -> Self {
        Self {
            source: envelope.source,
            destination: Some(ProtoRemoteDestination::from(envelope.destination)),
            message_type_id: envelope.metadata.message_type_id,
            schema_version: envelope.metadata.schema_version,
            codec_id: envelope.metadata.codec_id,
            trace_context: envelope.trace_context,
            request_id: envelope.request_id,
            payload: envelope.payload,
        }
    }
}

impl TryFrom<ProtoRemoteEnvelope> for RemoteEnvelope {
    type Error = RemoteError;

    fn try_from(proto: ProtoRemoteEnvelope) -> Result<Self, Self::Error> {
        let destination = proto
            .destination
            .ok_or_else(|| RemoteError::InvalidEnvelope {
                message: "missing destination".to_string(),
            })
            .and_then(RemoteDestination::try_from)?;

        if proto.message_type_id.is_empty() {
            return Err(RemoteError::InvalidEnvelope {
                message: "missing message_type_id".to_string(),
            });
        }

        if proto.codec_id.is_empty() {
            return Err(RemoteError::InvalidEnvelope {
                message: "missing codec_id".to_string(),
            });
        }

        Ok(Self {
            source: proto.source,
            destination,
            metadata: RemoteEnvelopeMetadata {
                message_type_id: proto.message_type_id,
                schema_version: proto.schema_version,
                codec_id: proto.codec_id,
            },
            trace_context: proto.trace_context,
            request_id: proto.request_id,
            payload: proto.payload,
        })
    }
}

impl From<RemoteDestination> for ProtoRemoteDestination {
    fn from(destination: RemoteDestination) -> Self {
        match destination {
            RemoteDestination::ActorPath { path } => Self {
                kind: ProtoDestinationKind::ActorPath as i32,
                path,
                entity_type: String::new(),
                entity_id: String::new(),
                service_key: String::new(),
                route_key: String::new(),
                request_id: String::new(),
                actor_node_id: String::new(),
                actor_system_name: String::new(),
                actor_uid: 0,
                actor_message_type: String::new(),
            },
            RemoteDestination::Entity {
                entity_type,
                entity_id,
            } => Self {
                kind: ProtoDestinationKind::Entity as i32,
                path: String::new(),
                entity_type,
                entity_id,
                service_key: String::new(),
                route_key: String::new(),
                request_id: String::new(),
                actor_node_id: String::new(),
                actor_system_name: String::new(),
                actor_uid: 0,
                actor_message_type: String::new(),
            },
            RemoteDestination::Service { service_key } => Self {
                kind: ProtoDestinationKind::Service as i32,
                path: String::new(),
                entity_type: String::new(),
                entity_id: String::new(),
                service_key,
                route_key: String::new(),
                request_id: String::new(),
                actor_node_id: String::new(),
                actor_system_name: String::new(),
                actor_uid: 0,
                actor_message_type: String::new(),
            },
            RemoteDestination::RouteKey { route_key } => Self {
                kind: ProtoDestinationKind::RouteKey as i32,
                path: String::new(),
                entity_type: String::new(),
                entity_id: String::new(),
                service_key: String::new(),
                route_key,
                request_id: String::new(),
                actor_node_id: String::new(),
                actor_system_name: String::new(),
                actor_uid: 0,
                actor_message_type: String::new(),
            },
            RemoteDestination::Reply { request_id } => Self {
                kind: ProtoDestinationKind::Reply as i32,
                path: String::new(),
                entity_type: String::new(),
                entity_id: String::new(),
                service_key: String::new(),
                route_key: String::new(),
                request_id,
                actor_node_id: String::new(),
                actor_system_name: String::new(),
                actor_uid: 0,
                actor_message_type: String::new(),
            },
            RemoteDestination::ActorRef { actor_ref } => {
                let actor_ref = *actor_ref;
                Self {
                    kind: ProtoDestinationKind::ActorRef as i32,
                    path: actor_ref.path.to_string(),
                    entity_type: String::new(),
                    entity_id: String::new(),
                    service_key: String::new(),
                    route_key: String::new(),
                    request_id: String::new(),
                    actor_node_id: actor_ref.node_id.to_string(),
                    actor_system_name: actor_ref.system_name,
                    actor_uid: actor_ref.uid.value(),
                    actor_message_type: actor_ref.message_type,
                }
            }
        }
    }
}

impl TryFrom<ProtoRemoteDestination> for RemoteDestination {
    type Error = RemoteError;

    fn try_from(proto: ProtoRemoteDestination) -> Result<Self, Self::Error> {
        match ProtoDestinationKind::try_from(proto.kind).map_err(|_unknown| {
            RemoteError::InvalidEnvelope {
                message: format!("unknown destination kind {}", proto.kind),
            }
        })? {
            ProtoDestinationKind::Unspecified => Err(RemoteError::InvalidEnvelope {
                message: "unspecified destination kind".to_string(),
            }),
            ProtoDestinationKind::ActorPath => {
                require_non_empty("path", proto.path).map(|path| Self::ActorPath { path })
            }
            ProtoDestinationKind::Entity => {
                let entity_type = require_non_empty("entity_type", proto.entity_type)?;
                let entity_id = require_non_empty("entity_id", proto.entity_id)?;
                Ok(Self::Entity {
                    entity_type,
                    entity_id,
                })
            }
            ProtoDestinationKind::Service => require_non_empty("service_key", proto.service_key)
                .map(|service_key| Self::Service { service_key }),
            ProtoDestinationKind::RouteKey => require_non_empty("route_key", proto.route_key)
                .map(|route_key| Self::RouteKey { route_key }),
            ProtoDestinationKind::Reply => require_non_empty("request_id", proto.request_id)
                .map(|request_id| Self::Reply { request_id }),
            ProtoDestinationKind::ActorRef => {
                let node_id =
                    parse_node_id(require_non_empty("actor_node_id", proto.actor_node_id)?)?;
                RemoteActorRef::new(
                    node_id,
                    proto.actor_system_name,
                    ActorPath::new(proto.path),
                    ActorUid::new(proto.actor_uid),
                    proto.actor_message_type,
                )
                .map(Self::actor_ref)
            }
        }
    }
}

fn require_non_empty(field: &str, value: String) -> RemoteResult<String> {
    if value.is_empty() {
        Err(RemoteError::InvalidEnvelope {
            message: format!("missing {field}"),
        })
    } else {
        Ok(value)
    }
}

fn require_non_zero(field: &str, value: u64) -> RemoteResult<u64> {
    if value == 0 {
        Err(RemoteError::InvalidEnvelope {
            message: format!("missing {field}"),
        })
    } else {
        Ok(value)
    }
}

fn parse_node_id(value: String) -> RemoteResult<NodeId> {
    NodeId::from_str(&value).map_err(|error| RemoteError::InvalidEnvelope {
        message: error.to_string(),
    })
}

fn validate_node_id(node_id: &NodeId) -> RemoteResult<()> {
    let _parsed = parse_node_id(node_id.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_ref_destination_rejects_missing_required_fields() {
        assert_missing_actor_ref_field(actor_ref_proto_with(|proto| {
            proto.actor_node_id.clear();
        }));
        assert_missing_actor_ref_field(actor_ref_proto_with(|proto| {
            proto.actor_system_name.clear();
        }));
        assert_missing_actor_ref_field(actor_ref_proto_with(|proto| {
            proto.path.clear();
        }));
        assert_missing_actor_ref_field(actor_ref_proto_with(|proto| {
            proto.actor_uid = 0;
        }));
        assert_missing_actor_ref_field(actor_ref_proto_with(|proto| {
            proto.actor_message_type.clear();
        }));
    }

    #[test]
    fn remote_actor_ref_constructor_rejects_missing_required_fields() {
        assert!(RemoteActorRef::new(
            NodeId::new("", ""),
            "system",
            ActorPath::new("/user/worker"),
            ActorUid::new(1),
            "rakka.test.Ping",
        )
        .is_err());
        assert!(RemoteActorRef::new(
            NodeId::new("rakka-0", "uid-a"),
            "",
            ActorPath::new("/user/worker"),
            ActorUid::new(1),
            "rakka.test.Ping",
        )
        .is_err());
        assert!(RemoteActorRef::new(
            NodeId::new("rakka-0", "uid-a"),
            "system",
            ActorPath::new(""),
            ActorUid::new(1),
            "rakka.test.Ping",
        )
        .is_err());
        assert!(RemoteActorRef::new(
            NodeId::new("rakka-0", "uid-a"),
            "system",
            ActorPath::new("/user/worker"),
            ActorUid::new(0),
            "rakka.test.Ping",
        )
        .is_err());
        assert!(RemoteActorRef::new(
            NodeId::new("rakka-0", "uid-a"),
            "system",
            ActorPath::new("/user/worker"),
            ActorUid::new(1),
            "",
        )
        .is_err());
    }

    fn assert_missing_actor_ref_field(proto: ProtoRemoteDestination) {
        assert!(matches!(
            RemoteDestination::try_from(proto),
            Err(RemoteError::InvalidEnvelope { .. })
        ));
    }

    fn actor_ref_proto_with(
        mutate: impl FnOnce(&mut ProtoRemoteDestination),
    ) -> ProtoRemoteDestination {
        let mut proto = ProtoRemoteDestination {
            kind: ProtoDestinationKind::ActorRef as i32,
            path: "rakka://local/example/user/worker".to_string(),
            entity_type: String::new(),
            entity_id: String::new(),
            service_key: String::new(),
            route_key: String::new(),
            request_id: String::new(),
            actor_node_id: "rakka-0#uid-a".to_string(),
            actor_system_name: "example".to_string(),
            actor_uid: 42,
            actor_message_type: "rakka.test.Ping".to_string(),
        };
        mutate(&mut proto);
        proto
    }
}
