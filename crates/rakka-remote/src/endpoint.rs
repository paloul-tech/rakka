//! Remote endpoint routing for inbound envelopes.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};

use rakka_cluster::NodeId;

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
    /// An entity handler is already registered for this entity type.
    DuplicateEntityHandler {
        /// Entity type that already has a handler.
        entity_type: String,
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
            Self::DuplicateEntityHandler { entity_type } => {
                write!(
                    f,
                    "remote endpoint already has an entity handler for {entity_type}"
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
}

impl Debug for RemoteEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let entity_handler_count = self
            .handlers
            .lock()
            .expect("remote endpoint handler mutex poisoned")
            .entities
            .len();
        f.debug_struct("RemoteEndpoint")
            .field("node_id", &self.node_id)
            .field("entity_handler_count", &entity_handler_count)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct RemoteEndpointHandlers {
    entities: BTreeMap<String, Arc<dyn RemoteEnvelopeHandler>>,
}
