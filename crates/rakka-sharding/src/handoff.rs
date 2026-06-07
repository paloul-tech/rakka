//! Graceful shard handoff state model.

use std::fmt::{self, Display, Formatter};

use rakka_cluster::NodeId;
use serde::{Deserialize, Serialize};

use crate::{ShardKey, ShardMoveReason};

/// Local lifecycle state for a shard during ownership handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShardHandoffState {
    /// Shard is locally owned and can activate entities.
    Owning,
    /// Shard is refusing new local deliveries while the old owner drains.
    Draining,
    /// Shard has stopped local entities and is waiting for ownership transfer to publish.
    Transferring,
    /// Shard ownership has been acquired by the new owner and can activate entities.
    Acquired,
}

impl Display for ShardHandoffState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owning => f.write_str("owning"),
            Self::Draining => f.write_str("draining"),
            Self::Transferring => f.write_str("transferring"),
            Self::Acquired => f.write_str("acquired"),
        }
    }
}

/// One deterministic step in a graceful shard handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardHandoff {
    shard: ShardKey,
    from: NodeId,
    to: NodeId,
    reason: ShardMoveReason,
    state: ShardHandoffState,
    stopped_entities: usize,
}

impl ShardHandoff {
    /// Creates a shard handoff step.
    #[must_use]
    pub fn new(
        shard: ShardKey,
        from: NodeId,
        to: NodeId,
        reason: ShardMoveReason,
        state: ShardHandoffState,
        stopped_entities: usize,
    ) -> Self {
        Self {
            shard,
            from,
            to,
            reason,
            state,
            stopped_entities,
        }
    }

    /// Shard being handed off.
    #[must_use]
    pub fn shard(&self) -> &ShardKey {
        &self.shard
    }

    /// Previous shard owner.
    #[must_use]
    pub fn from(&self) -> &NodeId {
        &self.from
    }

    /// New shard owner.
    #[must_use]
    pub fn to(&self) -> &NodeId {
        &self.to
    }

    /// Coordinator reason that triggered the handoff.
    #[must_use]
    pub const fn reason(&self) -> ShardMoveReason {
        self.reason
    }

    /// Handoff lifecycle state represented by this step.
    #[must_use]
    pub const fn state(&self) -> ShardHandoffState {
        self.state
    }

    /// Number of local entity actors stopped during this step.
    #[must_use]
    pub const fn stopped_entities(&self) -> usize {
        self.stopped_entities
    }
}
