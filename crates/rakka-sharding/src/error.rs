//! Typed errors for shard identity and coordination.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{RakkaError, Subsystem};

use crate::identity::{EntityId, EntityType, ShardId};

/// Convenient result alias for sharding operations.
pub type ShardingResult<T> = Result<T, ShardingError>;

/// Shard routing, ownership, or configuration failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingError {
    /// A sharding config requested zero shards.
    InvalidShardCount,
    /// The shard id is outside the configured shard range.
    UnknownShard {
        /// Unknown shard id.
        shard_id: ShardId,
        /// Configured number of shards.
        number_of_shards: u32,
    },
    /// The coordinator has no owner for the requested shard.
    NoShardOwner {
        /// Entity type.
        entity_type: EntityType,
        /// Shard id.
        shard_id: ShardId,
    },
    /// The coordinator has no owner for the requested entity.
    NoEntityOwner {
        /// Entity type.
        entity_type: EntityType,
        /// Entity id.
        entity_id: EntityId,
        /// Shard id.
        shard_id: ShardId,
    },
    /// Entity reference was used with a region for a different entity type.
    EntityTypeMismatch {
        /// Entity type expected by the region.
        expected: EntityType,
        /// Entity type carried by the entity reference.
        actual: EntityType,
    },
    /// Ownership snapshot did not match the region configuration.
    OwnershipSnapshotMismatch {
        /// Entity type expected by the region.
        expected_entity_type: EntityType,
        /// Entity type carried by the ownership snapshot.
        actual_entity_type: EntityType,
        /// Shard count expected by the region.
        expected_shards: u32,
        /// Shard count carried by the ownership snapshot.
        actual_shards: u32,
    },
}

impl ShardingError {
    /// Converts this error to a core framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Sharding, self.code(), self.to_string())
    }

    fn code(&self) -> &'static str {
        match self {
            Self::InvalidShardCount => "invalid-shard-count",
            Self::UnknownShard { .. } => "unknown-shard",
            Self::NoShardOwner { .. } => "no-shard-owner",
            Self::NoEntityOwner { .. } => "no-entity-owner",
            Self::EntityTypeMismatch { .. } => "entity-type-mismatch",
            Self::OwnershipSnapshotMismatch { .. } => "ownership-snapshot-mismatch",
        }
    }
}

impl Display for ShardingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShardCount => write!(f, "sharding config must contain at least one shard"),
            Self::UnknownShard {
                shard_id,
                number_of_shards,
            } => write!(
                f,
                "unknown shard {shard_id}; configured shard range is 0..{number_of_shards}"
            ),
            Self::NoShardOwner {
                entity_type,
                shard_id,
            } => write!(f, "no owner for {entity_type} shard {shard_id}"),
            Self::NoEntityOwner {
                entity_type,
                entity_id,
                shard_id,
            } => write!(
                f,
                "no owner for {entity_type}/{entity_id} routed to shard {shard_id}"
            ),
            Self::EntityTypeMismatch { expected, actual } => write!(
                f,
                "entity ref type {actual} cannot be routed through region for {expected}"
            ),
            Self::OwnershipSnapshotMismatch {
                expected_entity_type,
                actual_entity_type,
                expected_shards,
                actual_shards,
            } => write!(
                f,
                "ownership snapshot {actual_entity_type}/{actual_shards} does not match region {expected_entity_type}/{expected_shards}"
            ),
        }
    }
}

impl Error for ShardingError {}
