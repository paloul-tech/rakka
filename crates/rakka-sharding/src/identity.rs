//! Sharded entity identity and deterministic shard mapping.

use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::error::{ShardingError, ShardingResult};

/// Named actor type for sharded entities.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntityType(String);

impl EntityType {
    /// Creates a new entity type name.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the entity type as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for EntityType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Domain id for one sharded entity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntityId(String);

impl EntityId {
    /// Creates a new entity id.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the entity id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Numeric shard id within an entity type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardId(u32);

impl ShardId {
    /// Creates a shard id from a zero-based index.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the shard id as a zero-based index.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Derives a deterministic shard id from entity type and entity id.
    #[must_use]
    pub fn for_entity(
        entity_type: &EntityType,
        entity_id: &EntityId,
        config: &ShardingConfig,
    ) -> Self {
        let hash = stable_hash(entity_type.as_str(), entity_id.as_str());
        Self((hash % u64::from(config.number_of_shards())) as u32)
    }
}

impl Display for ShardId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A shard qualified by entity type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    entity_type: EntityType,
    shard_id: ShardId,
}

impl ShardKey {
    /// Creates a shard key.
    #[must_use]
    pub fn new(entity_type: EntityType, shard_id: ShardId) -> Self {
        Self {
            entity_type,
            shard_id,
        }
    }

    /// Entity type that owns this shard namespace.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Shard id within the entity type.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

impl Display for ShardKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.entity_type, self.shard_id)
    }
}

/// Sharding configuration for one entity type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardingConfig {
    number_of_shards: u32,
}

impl ShardingConfig {
    /// Creates a sharding configuration.
    pub fn new(number_of_shards: u32) -> ShardingResult<Self> {
        if number_of_shards == 0 {
            return Err(ShardingError::InvalidShardCount);
        }

        Ok(Self { number_of_shards })
    }

    /// Number of shards in the entity type namespace.
    #[must_use]
    pub const fn number_of_shards(&self) -> u32 {
        self.number_of_shards
    }

    /// Returns true when this shard belongs to the configured shard range.
    #[must_use]
    pub const fn contains_shard(&self, shard_id: ShardId) -> bool {
        shard_id.as_u32() < self.number_of_shards
    }
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            number_of_shards: 128,
        }
    }
}

/// Typed logical reference for a sharded entity.
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityRef<M> {
    entity_type: EntityType,
    entity_id: EntityId,
    #[serde(skip)]
    _message: PhantomData<fn(M)>,
}

impl<M> EntityRef<M> {
    /// Creates a typed logical entity reference.
    #[must_use]
    pub fn new(entity_type: EntityType, entity_id: EntityId) -> Self {
        Self {
            entity_type,
            entity_id,
            _message: PhantomData,
        }
    }

    /// Entity type.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Entity id.
    #[must_use]
    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    /// Computes the shard id for this entity ref.
    #[must_use]
    pub fn shard_id(&self, config: &ShardingConfig) -> ShardId {
        ShardId::for_entity(&self.entity_type, &self.entity_id, config)
    }

    /// Computes the shard key for this entity ref.
    #[must_use]
    pub fn shard_key(&self, config: &ShardingConfig) -> ShardKey {
        ShardKey::new(self.entity_type.clone(), self.shard_id(config))
    }
}

impl<M> Clone for EntityRef<M> {
    fn clone(&self) -> Self {
        Self {
            entity_type: self.entity_type.clone(),
            entity_id: self.entity_id.clone(),
            _message: PhantomData,
        }
    }
}

fn stable_hash(entity_type: &str, entity_id: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in entity_type
        .as_bytes()
        .iter()
        .copied()
        .chain(std::iter::once(0xff))
        .chain(entity_id.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
