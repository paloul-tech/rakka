#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Cluster membership and node lifecycle foundation.

use rakka_core::Subsystem;
use serde::{Deserialize, Serialize};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-cluster";

/// Subsystem associated with cluster membership.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Cluster
}

/// Lifecycle state for a Rakka cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MembershipState {
    /// Node has started but is not yet a cluster member.
    Joining,
    /// Node is an active cluster member.
    Up,
    /// Node is gracefully leaving the cluster.
    Leaving,
    /// Node is suspected unreachable.
    Unreachable,
    /// Node has been downed.
    Down,
    /// Node has been removed from membership.
    Removed,
}
