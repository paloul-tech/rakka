//! Typed cluster membership and discovery errors.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{RakkaError, ReceptionistError, Subsystem};

use crate::membership::MembershipState;
use crate::node::{ClusterProtocol, NodeId};

/// Convenient result alias for cluster operations.
pub type ClusterResult<T> = Result<T, ClusterError>;

/// Cluster membership, compatibility, or discovery failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterError {
    /// Membership operation referenced a node that is not known.
    UnknownNode {
        /// Unknown node id.
        node_id: NodeId,
    },
    /// A membership state transition is not allowed.
    InvalidTransition {
        /// Node id.
        node_id: NodeId,
        /// Current state.
        from: MembershipState,
        /// Requested state.
        to: MembershipState,
    },
    /// A discovered node cannot safely coexist with the local node protocol.
    IncompatibleNode {
        /// Discovered node id.
        node_id: NodeId,
        /// Local node protocol compatibility advertisement.
        local: ClusterProtocol,
        /// Remote node protocol compatibility advertisement.
        remote: ClusterProtocol,
    },
    /// Discovery provider failed.
    Discovery {
        /// Provider name.
        provider: String,
        /// Failure detail.
        message: String,
    },
    /// Clustered receptionist operation failed.
    Receptionist {
        /// Failure detail.
        message: String,
    },
    /// A propagated receptionist listing exceeded the configured routee limit.
    ReceptionistListingTooLarge {
        /// Service id.
        service_id: String,
        /// Actual routee count.
        actual: usize,
        /// Configured maximum routee count.
        max: usize,
    },
}

impl ClusterError {
    /// Converts a core receptionist error into a cluster error.
    #[must_use]
    pub fn from_receptionist(error: ReceptionistError) -> Self {
        Self::Receptionist {
            message: error.to_string(),
        }
    }

    /// Converts this error to a core framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Cluster, self.code(), self.to_string())
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnknownNode { .. } => "unknown-node",
            Self::InvalidTransition { .. } => "invalid-transition",
            Self::IncompatibleNode { .. } => "incompatible-node",
            Self::Discovery { .. } => "discovery-error",
            Self::Receptionist { .. } => "receptionist-error",
            Self::ReceptionistListingTooLarge { .. } => "receptionist-listing-too-large",
        }
    }
}

impl Display for ClusterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownNode { node_id } => write!(f, "unknown cluster node {node_id}"),
            Self::InvalidTransition { node_id, from, to } => write!(
                f,
                "invalid cluster membership transition for {node_id}: {from:?} -> {to:?}"
            ),
            Self::IncompatibleNode {
                node_id,
                local,
                remote,
            } => write!(
                f,
                "cluster node {node_id} advertises incompatible protocol {remote}; local is {local}"
            ),
            Self::Discovery { provider, message } => {
                write!(f, "cluster discovery provider {provider} failed: {message}")
            }
            Self::Receptionist { message } => {
                write!(f, "clustered receptionist failed: {message}")
            }
            Self::ReceptionistListingTooLarge {
                service_id,
                actual,
                max,
            } => write!(
                f,
                "clustered receptionist listing for '{service_id}' has {actual} routees, above configured maximum {max}"
            ),
        }
    }
}

impl Error for ClusterError {}
