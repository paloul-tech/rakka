#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Cluster sharding and entity routing foundation.

pub mod coordinator;
pub mod error;
pub mod identity;
pub mod local;
pub mod remote;
pub mod routing;

use rakka_core::Subsystem;

pub use coordinator::{
    ShardAssignment, ShardCoordinator, ShardDecision, ShardMoveReason, ShardOwnershipSnapshot,
    ShardRebalancePlan,
};
pub use error::{ShardingError, ShardingResult};
pub use identity::{EntityId, EntityRef, EntityType, ShardId, ShardKey, ShardingConfig};
pub use local::{LocalEntityContext, LocalEntityRoute};
pub use remote::{
    RemoteEntityInbound, RemoteEntityInboundError, RemoteEntityOutbound, RemoteEntityRoute,
    RemoteEntitySendFailure,
};
pub use routing::{
    EntityAskError, EntityDeliveryFailure, EntityRoute, EntityTellError, RoutedEntityMessage,
    ShardOwnerCache, ShardRegion,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-sharding";

/// Subsystem associated with sharding.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Sharding
}
