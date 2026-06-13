//! Remote clustered receptionist wire-listing descriptors.

use std::str::FromStr;

use rakka_cluster::NodeId;
use rakka_core::{ActorRef, ActorRefResolver, Listing, Message};
use serde::{Deserialize, Serialize};

use crate::{RemoteActorRef, RemoteError, RemoteResult};

/// Transport-facing descriptor for one service routee in a remote receptionist listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteServiceRoutee {
    actor_ref: RemoteActorRef,
}

impl RemoteServiceRoutee {
    /// Creates a remote service routee from a concrete remote actor reference.
    #[must_use]
    pub const fn new(actor_ref: RemoteActorRef) -> Self {
        Self { actor_ref }
    }

    /// Creates a routee descriptor from a local actor ref.
    pub fn from_actor_ref<M>(
        source_node: NodeId,
        resolver: &ActorRefResolver,
        actor_ref: &ActorRef<M>,
    ) -> RemoteResult<Self>
    where
        M: Message,
    {
        let serialized = resolver.to_serialized_ref(actor_ref);
        RemoteActorRef::from_serialized(source_node, &serialized).map(Self::new)
    }

    /// Concrete actor incarnation descriptor for this service routee.
    #[must_use]
    pub const fn actor_ref(&self) -> &RemoteActorRef {
        &self.actor_ref
    }

    /// Rust message type associated with this service routee.
    #[must_use]
    pub fn message_type(&self) -> &str {
        self.actor_ref.message_type()
    }
}

/// Transport-facing receptionist listing for one service on one source node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteReceptionistListing {
    source_node: NodeId,
    service_id: String,
    service_message_type: String,
    routees: Vec<RemoteServiceRoutee>,
    version: u64,
    observed_at_millis: u64,
}

impl RemoteReceptionistListing {
    /// Creates a remote receptionist listing.
    pub fn new(
        source_node: NodeId,
        service_id: impl Into<String>,
        service_message_type: impl Into<String>,
        routees: Vec<RemoteServiceRoutee>,
        version: u64,
        observed_at_millis: u64,
    ) -> RemoteResult<Self> {
        Self::new_with_max_routees(
            source_node,
            service_id,
            service_message_type,
            routees,
            version,
            observed_at_millis,
            None,
        )
    }

    /// Creates a remote receptionist listing and enforces a routee-count limit.
    pub fn new_with_max_routees(
        source_node: NodeId,
        service_id: impl Into<String>,
        service_message_type: impl Into<String>,
        routees: Vec<RemoteServiceRoutee>,
        version: u64,
        observed_at_millis: u64,
        max_routees: Option<usize>,
    ) -> RemoteResult<Self> {
        validate_node_id(&source_node)?;
        let service_id = require_non_empty("service_id", service_id.into())?;
        let service_message_type =
            require_non_empty("service_message_type", service_message_type.into())?;
        if let Some(max) = max_routees {
            if routees.len() > max {
                return Err(RemoteError::InvalidEnvelope {
                    message: format!(
                        "remote receptionist listing for service {service_id:?} has {} routees, max {max}",
                        routees.len()
                    ),
                });
            }
        }
        for routee in &routees {
            if routee.actor_ref().node_id() != &source_node {
                return Err(RemoteError::InvalidEnvelope {
                    message: format!(
                        "routee node {} does not match listing source node {source_node}",
                        routee.actor_ref().node_id()
                    ),
                });
            }
            if routee.message_type() != service_message_type {
                return Err(RemoteError::InvalidEnvelope {
                    message: format!(
                        "routee message type {} does not match service message type {service_message_type}",
                        routee.message_type()
                    ),
                });
            }
        }

        Ok(Self {
            source_node,
            service_id,
            service_message_type,
            routees,
            version,
            observed_at_millis,
        })
    }

    /// Converts a local typed receptionist listing into a remote wire listing.
    pub fn from_listing<M>(
        source_node: NodeId,
        resolver: &ActorRefResolver,
        listing: &Listing<M>,
        observed_at_millis: u64,
    ) -> RemoteResult<Self>
    where
        M: Message,
    {
        Self::from_listing_with_max_routees(
            source_node,
            resolver,
            listing,
            observed_at_millis,
            None,
        )
    }

    /// Converts a local typed listing and enforces a routee-count limit.
    pub fn from_listing_with_max_routees<M>(
        source_node: NodeId,
        resolver: &ActorRefResolver,
        listing: &Listing<M>,
        observed_at_millis: u64,
        max_routees: Option<usize>,
    ) -> RemoteResult<Self>
    where
        M: Message,
    {
        let routees = listing
            .routees()
            .iter()
            .map(|routee| {
                RemoteServiceRoutee::from_actor_ref(source_node.clone(), resolver, routee)
            })
            .collect::<RemoteResult<Vec<_>>>()?;

        Self::new_with_max_routees(
            source_node,
            listing.key().id(),
            listing.key().message_type(),
            routees,
            listing.revision(),
            observed_at_millis,
            max_routees,
        )
    }

    /// Source cluster node that published this listing.
    #[must_use]
    pub const fn source_node(&self) -> &NodeId {
        &self.source_node
    }

    /// Receptionist service id.
    #[must_use]
    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// Rust message type associated with the service key.
    #[must_use]
    pub fn service_message_type(&self) -> &str {
        &self.service_message_type
    }

    /// Routees published by the source node.
    #[must_use]
    pub fn routees(&self) -> &[RemoteServiceRoutee] {
        &self.routees
    }

    /// Monotonic source receptionist revision.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Source observation time in milliseconds.
    #[must_use]
    pub const fn observed_at_millis(&self) -> u64 {
        self.observed_at_millis
    }

    /// Number of routees in this listing.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routees.len()
    }

    /// Returns true when the listing contains no routees.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routees.is_empty()
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

fn validate_node_id(node_id: &NodeId) -> RemoteResult<()> {
    NodeId::from_str(&node_id.to_string())
        .map(|_node_id| ())
        .map_err(|error| RemoteError::InvalidEnvelope {
            message: error.to_string(),
        })
}
