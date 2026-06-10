#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Cluster sharding and entity routing foundation.

pub mod coordinator;
pub mod error;
pub mod handoff;
pub mod identity;
pub mod local;
pub mod node_runtime;
pub mod remote;
pub mod routing;
pub mod runtime;

use rakka_core::Subsystem;

pub use coordinator::{
    ShardAssignment, ShardCoordinator, ShardDecision, ShardMoveReason, ShardOwnerCount,
    ShardOwnershipSnapshot, ShardRebalancePlan,
};
pub use error::{ShardingError, ShardingResult};
pub use handoff::{ShardHandoff, ShardHandoffState};
pub use identity::{EntityId, EntityRef, EntityType, ShardId, ShardKey, ShardingConfig};
pub use local::{LocalEntityContext, LocalEntityRoute};
pub use node_runtime::{
    ClusterNodeRuntime, ClusterNodeRuntimeBuilder, ClusterNodeRuntimeError,
    ClusterNodeRuntimeResult, ClusterNodeRuntimeUpdate,
};
pub use remote::{
    RemoteEntityAskClient, RemoteEntityAskError, RemoteEntityAskInbound,
    RemoteEntityAskInboundError, RemoteEntityInbound, RemoteEntityInboundError,
    RemoteEntityOutbound, RemoteEntityRoute, RemoteEntitySendFailure,
    RemoteTransportEntityOutbound,
};
pub use routing::{
    EntityAskError, EntityDeliveryFailure, EntityRoute, EntityTellError, RoutedEntityMessage,
    ShardOwnerCache, ShardRegion,
};
pub use runtime::{
    ClusterShardingError, ClusterShardingResult, ClusterShardingRuntime, ClusterShardingUpdate,
    EntityShardRebalance,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-sharding";

/// Subsystem associated with sharding.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Sharding
}
