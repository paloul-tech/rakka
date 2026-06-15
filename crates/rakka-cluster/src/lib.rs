#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Cluster membership, discovery, and node lifecycle foundation.

pub mod discovery;
pub mod error;
pub mod facade;
pub mod membership;
pub mod node;
pub mod receptionist;
pub mod shutdown;

use rakka_core::Subsystem;

pub use discovery::{DiscoveryProvider, DiscoverySnapshot, LocalDiscovery, StaticDiscovery};
pub use error::{ClusterError, ClusterResult};
pub use facade::{
    Cluster, ClusterEvent, ClusterManager, ClusterRuntime, ClusterSettings, ClusterState,
    ClusterSubscription, ClusterSubscriptionError, ClusterSubscriptionReplay, ClusterSubscriptions,
    ClusterUpdate, DowningStrategy, FailureDetector, NoDowningStrategy, SelfMember,
    TimeoutDowningStrategy, TimeoutFailureDetector,
};
pub use membership::{
    ClusterMembership, ClusterMembershipOperationalSnapshot, MemberRecord, MembershipConfig,
    MembershipEvent, MembershipSnapshot, MembershipState, MembershipStateCount,
};
pub use node::{
    ClusterNode, ClusterProtocol, CompatibilityRange, NodeAddress, NodeId, NodeRole,
    ProtocolVersion,
};
pub use receptionist::{
    ClusteredReceptionist, ClusteredReceptionistListing, ClusteredReceptionistSettings,
};
pub use shutdown::{
    register_cluster_down_self_task, register_cluster_leave_task,
    register_clustered_receptionist_prune_task,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-cluster";

/// Subsystem associated with cluster membership.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Cluster
}
