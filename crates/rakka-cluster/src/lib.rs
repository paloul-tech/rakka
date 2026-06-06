#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Cluster membership, discovery, and node lifecycle foundation.

pub mod discovery;
pub mod error;
pub mod membership;
pub mod node;

use rakka_core::Subsystem;

pub use discovery::{DiscoveryProvider, DiscoverySnapshot, LocalDiscovery, StaticDiscovery};
pub use error::{ClusterError, ClusterResult};
pub use membership::{
    ClusterMembership, MemberRecord, MembershipConfig, MembershipEvent, MembershipSnapshot,
    MembershipState,
};
pub use node::{
    ClusterNode, ClusterProtocol, CompatibilityRange, NodeAddress, NodeId, NodeRole,
    ProtocolVersion,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-cluster";

/// Subsystem associated with cluster membership.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Cluster
}
