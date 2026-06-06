//! Typed cluster membership and discovery errors.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{RakkaError, Subsystem};

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
}

impl ClusterError {
    /// Converts this error to a core framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Cluster, self.code(), self.to_string())
    }

    fn code(&self) -> &'static str {
        match self {
            Self::UnknownNode { .. } => "unknown-node",
            Self::InvalidTransition { .. } => "invalid-transition",
            Self::IncompatibleNode { .. } => "incompatible-node",
            Self::Discovery { .. } => "discovery-error",
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
        }
    }
}

impl Error for ClusterError {}
