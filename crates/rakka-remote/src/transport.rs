//! Remote transport abstractions and deterministic in-memory transport.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};

use rakka_cluster::NodeId;

use crate::{
    ProtobufEnvelopeCodec, RemoteEndpoint, RemoteEndpointError, RemoteEnvelope, RemoteError,
};

/// Convenient result alias for remote transport operations.
pub type RemoteTransportResult<T> = Result<T, RemoteTransportError>;

/// Failure returned by a remote envelope transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteTransportError {
    /// A destination node was not registered with the transport.
    UnknownNode {
        /// Destination node id.
        node_id: NodeId,
    },
    /// A node endpoint was already registered.
    DuplicateNode {
        /// Duplicate node id.
        node_id: NodeId,
    },
    /// A bounded outbound transport queue was full.
    QueueFull {
        /// Destination node id.
        node_id: NodeId,
        /// Configured queue capacity.
        capacity: usize,
    },
    /// A destination node is draining and rejects new sends.
    Draining {
        /// Destination node id.
        node_id: NodeId,
    },
    /// A destination node connection or worker was closed.
    Closed {
        /// Destination node id.
        node_id: NodeId,
    },
    /// Envelope encoding failed before transport delivery.
    Encode {
        /// Destination node id.
        node_id: NodeId,
        /// Encode failure reported by the envelope codec.
        error: RemoteError,
    },
    /// The destination endpoint rejected the envelope.
    Endpoint {
        /// Destination node id.
        node_id: NodeId,
        /// Endpoint dispatch failure.
        error: RemoteEndpointError,
    },
}

impl Display for RemoteTransportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode { node_id } => {
                write!(f, "remote transport has no endpoint for node {node_id}")
            }
            Self::DuplicateNode { node_id } => {
                write!(
                    f,
                    "remote transport already has an endpoint for node {node_id}"
                )
            }
            Self::QueueFull { node_id, capacity } => {
                write!(
                    f,
                    "remote transport queue to node {node_id} is full at capacity {capacity}"
                )
            }
            Self::Draining { node_id } => {
                write!(f, "remote transport to node {node_id} is draining")
            }
            Self::Closed { node_id } => {
                write!(f, "remote transport to node {node_id} is closed")
            }
            Self::Encode { node_id, error } => {
                write!(
                    f,
                    "remote transport encode to node {node_id} failed: {error}"
                )
            }
            Self::Endpoint { node_id, error } => {
                write!(f, "remote endpoint {node_id} rejected envelope: {error}")
            }
        }
    }
}

impl Error for RemoteTransportError {}

/// Transport for sending remote envelopes to cluster nodes.
pub trait RemoteTransport: Send + Sync + 'static {
    /// Sends one remote envelope to the destination node.
    fn send(&self, destination: &NodeId, envelope: RemoteEnvelope) -> RemoteTransportResult<()>;
}

/// Deterministic in-memory transport for multi-node tests.
#[derive(Clone, Default)]
pub struct InMemoryRemoteTransport {
    endpoints: Arc<Mutex<BTreeMap<NodeId, RemoteEndpoint>>>,
}

impl InMemoryRemoteTransport {
    /// Creates an empty in-memory remote transport.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an endpoint as reachable by this transport.
    pub fn register_endpoint(&self, endpoint: RemoteEndpoint) -> RemoteTransportResult<()> {
        let node_id = endpoint.node_id().clone();
        let mut endpoints = self
            .endpoints
            .lock()
            .expect("in-memory remote transport mutex poisoned");
        if endpoints.contains_key(&node_id) {
            return Err(RemoteTransportError::DuplicateNode { node_id });
        }

        endpoints.insert(node_id, endpoint);
        Ok(())
    }

    /// Removes an endpoint from this transport.
    pub fn unregister_endpoint(&self, node_id: &NodeId) -> Option<RemoteEndpoint> {
        self.endpoints
            .lock()
            .expect("in-memory remote transport mutex poisoned")
            .remove(node_id)
    }

    /// Returns a registered endpoint.
    #[must_use]
    pub fn endpoint(&self, node_id: &NodeId) -> Option<RemoteEndpoint> {
        self.endpoints
            .lock()
            .expect("in-memory remote transport mutex poisoned")
            .get(node_id)
            .cloned()
    }

    /// Returns the number of registered endpoints.
    #[must_use]
    pub fn endpoint_count(&self) -> usize {
        self.endpoints
            .lock()
            .expect("in-memory remote transport mutex poisoned")
            .len()
    }
}

impl RemoteTransport for InMemoryRemoteTransport {
    fn send(&self, destination: &NodeId, envelope: RemoteEnvelope) -> RemoteTransportResult<()> {
        let endpoint =
            self.endpoint(destination)
                .ok_or_else(|| RemoteTransportError::UnknownNode {
                    node_id: destination.clone(),
                })?;
        let wire = ProtobufEnvelopeCodec::encode(&envelope).map_err(|error| {
            RemoteTransportError::Encode {
                node_id: destination.clone(),
                error,
            }
        })?;
        endpoint
            .receive_wire(&wire)
            .map_err(|error| RemoteTransportError::Endpoint {
                node_id: destination.clone(),
                error,
            })
    }
}

impl Debug for InMemoryRemoteTransport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryRemoteTransport")
            .field("endpoint_count", &self.endpoint_count())
            .finish_non_exhaustive()
    }
}
