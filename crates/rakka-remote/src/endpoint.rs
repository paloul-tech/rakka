//! Remote endpoint routing for inbound envelopes.

use std::any::type_name;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};

use rakka_cluster::NodeId;
use rakka_core::Message;

use crate::{ProtobufEnvelopeCodec, RemoteDestination, RemoteEnvelope, RemoteError};

/// Convenient result alias for endpoint dispatch operations.
pub type RemoteEndpointResult<T> = Result<T, RemoteEndpointError>;

/// Failure returned while dispatching an inbound remote envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEndpointError {
    /// Wire bytes could not be decoded into a remote envelope.
    Decode {
        /// Decode failure reported by the envelope codec.
        error: RemoteError,
    },
    /// Envelope was addressed to a destination kind this endpoint does not handle yet.
    UnexpectedDestination {
        /// Destination carried by the remote envelope.
        destination: RemoteDestination,
    },
    /// No handler is registered for the requested entity type.
    UnregisteredEntityType {
        /// Entity type carried by the remote envelope.
        entity_type: String,
    },
    /// No handler is registered for the requested actor-ref message type.
    UnregisteredActorRefHandler {
        /// Rust message type carried by the actor-ref descriptor.
        message_type: String,
    },
    /// No handler is registered for the requested service key.
    UnregisteredServiceHandler {
        /// Receptionist service key carried by the remote envelope.
        service_key: String,
    },
    /// No handler is registered for remote replies.
    UnregisteredReplyHandler {
        /// Request id carried by the reply destination.
        request_id: String,
    },
    /// An entity handler is already registered for this entity type.
    DuplicateEntityHandler {
        /// Entity type that already has a handler.
        entity_type: String,
    },
    /// An actor-ref handler is already registered for this message type.
    DuplicateActorRefHandler {
        /// Rust message type that already has a handler.
        message_type: String,
    },
    /// A service handler is already registered for this service key.
    DuplicateServiceHandler {
        /// Receptionist service key that already has a handler.
        service_key: String,
    },
    /// A registered handler rejected the envelope.
    HandlerRejected {
        /// Destination carried by the rejected envelope.
        destination: RemoteDestination,
        /// Failure detail returned by the registered handler.
        message: String,
    },
}

impl Display for RemoteEndpointError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { error } => write!(f, "remote endpoint decode failed: {error}"),
            Self::UnexpectedDestination { destination } => {
                write!(
                    f,
                    "remote endpoint has no route for destination {destination:?}"
                )
            }
            Self::UnregisteredEntityType { entity_type } => {
                write!(f, "remote endpoint has no entity handler for {entity_type}")
            }
            Self::UnregisteredActorRefHandler { message_type } => {
                write!(
                    f,
                    "remote endpoint has no actor-ref handler for {message_type}"
                )
            }
            Self::UnregisteredServiceHandler { service_key } => {
                write!(
                    f,
                    "remote endpoint has no service handler for {service_key}"
                )
            }
            Self::UnregisteredReplyHandler { request_id } => {
                write!(
                    f,
                    "remote endpoint has no reply handler for request {request_id}"
                )
            }
            Self::DuplicateEntityHandler { entity_type } => {
                write!(
                    f,
                    "remote endpoint already has an entity handler for {entity_type}"
                )
            }
            Self::DuplicateActorRefHandler { message_type } => {
                write!(
                    f,
                    "remote endpoint already has an actor-ref handler for {message_type}"
                )
            }
            Self::DuplicateServiceHandler { service_key } => {
                write!(
                    f,
                    "remote endpoint already has a service handler for {service_key}"
                )
            }
            Self::HandlerRejected {
                destination,
                message,
            } => write!(
                f,
                "remote endpoint handler rejected {destination:?}: {message}"
            ),
        }
    }
}

impl Error for RemoteEndpointError {}

/// Handler for one inbound remote envelope destination.
pub trait RemoteEnvelopeHandler: Send + Sync + 'static {
    /// Handles one decoded remote envelope.
    fn handle(&self, envelope: RemoteEnvelope) -> RemoteEndpointResult<()>;
}

impl<F> RemoteEnvelopeHandler for F
where
    F: Fn(RemoteEnvelope) -> RemoteEndpointResult<()> + Send + Sync + 'static,
{
    fn handle(&self, envelope: RemoteEnvelope) -> RemoteEndpointResult<()> {
        self(envelope)
    }
}

/// Inbound endpoint that dispatches remote envelopes to registered handlers.
#[derive(Clone)]
pub struct RemoteEndpoint {
    node_id: NodeId,
    handlers: Arc<Mutex<RemoteEndpointHandlers>>,
}

impl RemoteEndpoint {
    /// Creates a remote endpoint for one cluster node.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            handlers: Arc::new(Mutex::new(RemoteEndpointHandlers::default())),
        }
    }

    /// Returns the node id served by this endpoint.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Registers a handler for a sharded entity type.
    pub fn register_entity_handler(
        &self,
        entity_type: impl Into<String>,
        handler: impl RemoteEnvelopeHandler,
    ) -> RemoteEndpointResult<()> {
        let entity_type = entity_type.into();
        let mut handlers = self
            .handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned");
        if handlers.entities.contains_key(&entity_type) {
            return Err(RemoteEndpointError::DuplicateEntityHandler { entity_type });
        }

        handlers.entities.insert(entity_type, Arc::new(handler));
        Ok(())
    }

    /// Registers a handler for concrete actor-ref envelopes with message type `M`.
    pub fn register_actor_ref_handler<M>(
        &self,
        handler: impl RemoteEnvelopeHandler,
    ) -> RemoteEndpointResult<()>
    where
        M: Message,
    {
        let message_type = type_name::<M>().to_string();
        let mut handlers = self
            .handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned");
        if handlers.actor_refs.contains_key(&message_type) {
            return Err(RemoteEndpointError::DuplicateActorRefHandler { message_type });
        }

        handlers.actor_refs.insert(message_type, Arc::new(handler));
        Ok(())
    }

    /// Registers a handler for service-key envelopes.
    pub fn register_service_handler(
        &self,
        service_key: impl Into<String>,
        handler: impl RemoteEnvelopeHandler,
    ) -> RemoteEndpointResult<()> {
        let service_key = service_key.into();
        let mut handlers = self
            .handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned");
        if handlers.services.contains_key(&service_key) {
            return Err(RemoteEndpointError::DuplicateServiceHandler { service_key });
        }

        handlers.services.insert(service_key, Arc::new(handler));
        Ok(())
    }

    /// Registers the handler for reply envelopes.
    pub fn register_reply_handler(&self, handler: impl RemoteEnvelopeHandler) {
        self.handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned")
            .reply = Some(Arc::new(handler));
    }

    /// Receives encoded envelope bytes and dispatches the decoded envelope.
    pub fn receive_wire(&self, bytes: &[u8]) -> RemoteEndpointResult<()> {
        let envelope = ProtobufEnvelopeCodec::decode(bytes)
            .map_err(|error| RemoteEndpointError::Decode { error })?;
        self.receive_envelope(envelope)
    }

    /// Receives a decoded envelope and dispatches it by destination.
    pub fn receive_envelope(&self, envelope: RemoteEnvelope) -> RemoteEndpointResult<()> {
        match &envelope.destination {
            RemoteDestination::Entity { entity_type, .. } => {
                let handler = self.entity_handler(entity_type)?;
                handler.handle(envelope)
            }
            RemoteDestination::ActorRef { actor_ref } => {
                let handler = self.actor_ref_handler(actor_ref.message_type())?;
                handler.handle(envelope)
            }
            RemoteDestination::Service { service_key } => {
                let handler = self.service_handler(service_key)?;
                handler.handle(envelope)
            }
            RemoteDestination::Reply { request_id } => {
                let handler = self.reply_handler(request_id)?;
                handler.handle(envelope)
            }
            destination => Err(RemoteEndpointError::UnexpectedDestination {
                destination: destination.clone(),
            }),
        }
    }

    fn entity_handler(
        &self,
        entity_type: &str,
    ) -> RemoteEndpointResult<Arc<dyn RemoteEnvelopeHandler>> {
        self.handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned")
            .entities
            .get(entity_type)
            .cloned()
            .ok_or_else(|| RemoteEndpointError::UnregisteredEntityType {
                entity_type: entity_type.to_string(),
            })
    }

    fn actor_ref_handler(
        &self,
        message_type: &str,
    ) -> RemoteEndpointResult<Arc<dyn RemoteEnvelopeHandler>> {
        self.handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned")
            .actor_refs
            .get(message_type)
            .cloned()
            .ok_or_else(|| RemoteEndpointError::UnregisteredActorRefHandler {
                message_type: message_type.to_string(),
            })
    }

    fn service_handler(
        &self,
        service_key: &str,
    ) -> RemoteEndpointResult<Arc<dyn RemoteEnvelopeHandler>> {
        self.handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned")
            .services
            .get(service_key)
            .cloned()
            .ok_or_else(|| RemoteEndpointError::UnregisteredServiceHandler {
                service_key: service_key.to_string(),
            })
    }

    fn reply_handler(
        &self,
        request_id: &str,
    ) -> RemoteEndpointResult<Arc<dyn RemoteEnvelopeHandler>> {
        self.handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned")
            .reply
            .clone()
            .ok_or_else(|| RemoteEndpointError::UnregisteredReplyHandler {
                request_id: request_id.to_string(),
            })
    }
}

impl Debug for RemoteEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let handlers = self
            .handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned");
        f.debug_struct("RemoteEndpoint")
            .field("node_id", &self.node_id)
            .field("entity_handler_count", &handlers.entities.len())
            .field("actor_ref_handler_count", &handlers.actor_refs.len())
            .field("service_handler_count", &handlers.services.len())
            .field("has_reply_handler", &handlers.reply.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct RemoteEndpointHandlers {
    entities: BTreeMap<String, Arc<dyn RemoteEnvelopeHandler>>,
    actor_refs: BTreeMap<String, Arc<dyn RemoteEnvelopeHandler>>,
    services: BTreeMap<String, Arc<dyn RemoteEnvelopeHandler>>,
    reply: Option<Arc<dyn RemoteEnvelopeHandler>>,
}
