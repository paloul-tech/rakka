//! Remote actor-ref inbound delivery.

use std::any::type_name;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::marker::PhantomData;

use rakka_cluster::NodeId;
use rakka_core::{ActorSystem, Message, TellError};

use crate::{
    RemoteDestination, RemoteEndpointError, RemoteEnvelope, RemoteEnvelopeHandler, RemoteError,
    SerializationRegistry,
};

/// Failure returned while accepting an inbound remote actor-ref envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteActorRefInboundError {
    /// Envelope was addressed to a destination kind other than an actor ref.
    UnexpectedDestination {
        /// Destination carried by the remote envelope.
        destination: RemoteDestination,
    },
    /// Envelope was addressed to a different cluster node.
    NodeMismatch {
        /// Local node expected by this handler.
        expected: NodeId,
        /// Node id carried by the actor-ref descriptor.
        actual: NodeId,
    },
    /// The actor-ref descriptor could not be resolved in the local actor system.
    Resolve {
        /// Failure detail returned by the actor-ref resolver.
        message: String,
    },
    /// Envelope payload could not be decoded into this actor protocol.
    Decode {
        /// Decode failure reported by the serialization registry.
        error: RemoteError,
    },
    /// The resolved actor mailbox was full.
    MailboxFull,
    /// The resolved actor mailbox was closed.
    MailboxClosed,
}

impl Display for RemoteActorRefInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedDestination { destination } => {
                write!(
                    f,
                    "remote envelope is not addressed to an actor ref: {destination:?}"
                )
            }
            Self::NodeMismatch { expected, actual } => {
                write!(
                    f,
                    "remote actor-ref envelope for node {actual} cannot be handled by local node {expected}"
                )
            }
            Self::Resolve { message } => write!(f, "remote actor-ref resolve failed: {message}"),
            Self::Decode { error } => write!(f, "remote actor-ref decode failed: {error}"),
            Self::MailboxFull => f.write_str("remote actor-ref delivery mailbox was full"),
            Self::MailboxClosed => f.write_str("remote actor-ref delivery mailbox was closed"),
        }
    }
}

impl Error for RemoteActorRefInboundError {}

/// Inbound handler that decodes remote actor-ref envelopes and delivers them to local actors.
pub struct RemoteActorRefInbound<M>
where
    M: Message + Sync,
{
    local_node_id: NodeId,
    system: ActorSystem,
    registry: SerializationRegistry,
    _message: PhantomData<fn() -> M>,
}

impl<M> RemoteActorRefInbound<M>
where
    M: Message + Sync,
{
    /// Creates an inbound remote actor-ref handler.
    #[must_use]
    pub fn new(
        local_node_id: NodeId,
        system: ActorSystem,
        registry: SerializationRegistry,
    ) -> Self {
        Self {
            local_node_id,
            system,
            registry,
            _message: PhantomData,
        }
    }

    /// Local node served by this handler.
    #[must_use]
    pub const fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Actor system used for actor-ref resolution.
    #[must_use]
    pub const fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Serialization registry used for payload decoding.
    #[must_use]
    pub const fn registry(&self) -> &SerializationRegistry {
        &self.registry
    }

    /// Rust message type name this handler accepts.
    #[must_use]
    pub fn message_type(&self) -> &'static str {
        type_name::<M>()
    }

    /// Decodes and delivers one remote actor-ref envelope.
    pub fn handle(&self, envelope: RemoteEnvelope) -> Result<(), RemoteActorRefInboundError> {
        let actor_ref = match &envelope.destination {
            RemoteDestination::ActorRef { actor_ref } => actor_ref,
            destination => {
                return Err(RemoteActorRefInboundError::UnexpectedDestination {
                    destination: destination.clone(),
                });
            }
        };

        if actor_ref.node_id() != &self.local_node_id {
            return Err(RemoteActorRefInboundError::NodeMismatch {
                expected: self.local_node_id.clone(),
                actual: actor_ref.node_id().clone(),
            });
        }

        let local_ref = self
            .system
            .actor_ref_resolver()
            .resolve::<M>(&actor_ref.to_serialized_ref())
            .map_err(|error| RemoteActorRefInboundError::Resolve {
                message: error.to_string(),
            })?;
        let message = self
            .registry
            .decode_envelope(&envelope)
            .map_err(|error| RemoteActorRefInboundError::Decode { error })?;

        local_ref.tell(message).map_err(|error| match error {
            TellError::Full(_message) => RemoteActorRefInboundError::MailboxFull,
            TellError::Closed(_message) => RemoteActorRefInboundError::MailboxClosed,
        })
    }
}

impl<M> Clone for RemoteActorRefInbound<M>
where
    M: Message + Sync,
{
    fn clone(&self) -> Self {
        Self {
            local_node_id: self.local_node_id.clone(),
            system: self.system.clone(),
            registry: self.registry.clone(),
            _message: PhantomData,
        }
    }
}

impl<M> Debug for RemoteActorRefInbound<M>
where
    M: Message + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteActorRefInbound")
            .field("local_node_id", &self.local_node_id)
            .field("system", &self.system.name())
            .field("message_type", &self.message_type())
            .finish_non_exhaustive()
    }
}

impl<M> RemoteEnvelopeHandler for RemoteActorRefInbound<M>
where
    M: Message + Sync,
{
    fn handle(&self, envelope: RemoteEnvelope) -> Result<(), RemoteEndpointError> {
        let destination = envelope.destination.clone();
        RemoteActorRefInbound::handle(self, envelope).map_err(|error| {
            RemoteEndpointError::HandlerRejected {
                destination,
                message: error.to_string(),
            }
        })
    }
}
