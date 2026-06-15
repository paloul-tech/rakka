#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Cluster sharding and entity routing foundation.

pub mod allocation;
pub mod coordinator;
pub mod coordinator_lease;
pub mod coordinator_store;
pub mod error;
pub mod facade;
pub mod handoff;
pub mod identity;
pub mod local;
pub mod node_runtime;
pub mod remembered_entities;
pub mod remote;
pub mod routing;
pub mod runtime;
pub mod shutdown;

use rakka_core::Subsystem;

pub use allocation::{
    DeterministicModuloShardAllocationStrategy, LeastShardAllocationStrategy,
    ShardAllocationContext, ShardAllocationStrategy, ShardReassignment, ShardRebalanceContext,
};
pub use coordinator::{
    ShardAssignment, ShardCoordinator, ShardDecision, ShardMoveReason, ShardOwnerCount,
    ShardOwnershipSnapshot, ShardRebalancePlan,
};
pub use coordinator_lease::{
    CoordinatorLeaseFuture, InMemoryShardCoordinatorLease, LeaseToken, ShardCoordinatorLease,
};
pub use coordinator_store::{
    AsyncShardCoordinatorStore, CoordinatorStoreFuture, InMemoryShardCoordinatorStore,
    PersistedShardCoordinatorState, ShardCoordinatorStore,
};
pub use error::{ShardingError, ShardingResult};
pub use facade::{
    ClusterSharding, ClusterShardingState, Entity, EntityContext, EntityTypeKey,
    EntityTypeRegistration, EntityTypeRegistrationState, Passivate, ShardedEntityRef,
};
pub use handoff::{ShardHandoff, ShardHandoffState};
pub use identity::{EntityId, EntityRef, EntityType, ShardId, ShardKey, ShardingConfig};
pub use local::{LocalEntityContext, LocalEntityRoute};
pub use node_runtime::{
    ClusterNodeRuntime, ClusterNodeRuntimeBuilder, ClusterNodeRuntimeError,
    ClusterNodeRuntimeResult, ClusterNodeRuntimeUpdate,
};
pub use remembered_entities::{
    InMemoryRememberedEntityStore, RememberedEntities, RememberedEntityReplay,
    RememberedEntityReplaySettings, RememberedEntityStore, RememberedStoreFuture,
};
pub use remote::{
    RemoteEntityAskClient, RemoteEntityAskError, RemoteEntityAskInbound,
    RemoteEntityAskInboundError, RemoteEntityInbound, RemoteEntityInboundError,
    RemoteEntityOutbound, RemoteEntityRoute, RemoteEntitySendFailure,
    RemoteTransportEntityOutbound,
};
pub use routing::{
    EntityAskError, EntityDeliveryFailure, EntityRoute, EntityTellError, RoutedEntityMessage,
    ShardBufferConfig, ShardBufferOverflow, ShardOwnerCache, ShardRegion,
};
pub use runtime::{
    ClusterShardingError, ClusterShardingResult, ClusterShardingRuntime, ClusterShardingUpdate,
    EntityShardRebalance,
};
pub use shutdown::{
    register_async_cluster_node_leave_task, register_async_cluster_sharding_leave_task,
    register_cluster_node_leave_task, register_cluster_sharding_leave_task,
    AsyncClusterNodeShutdownHandle, AsyncClusterShardingShutdownHandle, ClusterNodeShutdownHandle,
    ClusterShardingShutdownHandle,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-sharding";

/// Subsystem associated with sharding.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Sharding
}
