//! Discovery snapshots and local/static discovery providers.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::error::{ClusterError, ClusterResult};
use crate::node::{ClusterNode, NodeId};

/// Snapshot produced by a discovery provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverySnapshot {
    provider: String,
    observed_at_millis: u64,
    nodes: Vec<ClusterNode>,
}

impl DiscoverySnapshot {
    /// Creates a discovery snapshot, de-duplicating nodes by incarnation id.
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        observed_at_millis: u64,
        nodes: impl IntoIterator<Item = ClusterNode>,
    ) -> Self {
        let nodes = nodes
            .into_iter()
            .map(|node| (node.id().clone(), node))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect();

        Self {
            provider: provider.into(),
            observed_at_millis,
            nodes,
        }
    }

    /// Discovery provider name.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Observation timestamp in milliseconds, supplied by the caller.
    #[must_use]
    pub const fn observed_at_millis(&self) -> u64 {
        self.observed_at_millis
    }

    /// Discovered cluster nodes.
    #[must_use]
    pub fn nodes(&self) -> &[ClusterNode] {
        &self.nodes
    }

    pub(crate) fn into_nodes(self) -> Vec<ClusterNode> {
        self.nodes
    }
}

/// Synchronous discovery provider abstraction.
///
/// The caller supplies time, which keeps discovery deterministic and lets a
/// Tokio task drive polling without coupling this crate to one scheduler API.
pub trait DiscoveryProvider {
    /// Provider name used in diagnostics.
    fn provider_name(&self) -> &str;

    /// Discovers cluster nodes observed at the supplied timestamp.
    fn discover(&self, observed_at_millis: u64) -> ClusterResult<DiscoverySnapshot>;
}

/// Discovery provider backed by a fixed list of cluster nodes.
#[derive(Debug, Clone)]
pub struct StaticDiscovery {
    provider: String,
    nodes: Vec<ClusterNode>,
}

impl StaticDiscovery {
    /// Creates a static discovery provider named `static`.
    #[must_use]
    pub fn new(nodes: impl IntoIterator<Item = ClusterNode>) -> Self {
        Self {
            provider: "static".to_string(),
            nodes: nodes.into_iter().collect(),
        }
    }

    /// Renames this discovery provider for diagnostics.
    #[must_use]
    pub fn with_provider_name(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }
}

impl DiscoveryProvider for StaticDiscovery {
    fn provider_name(&self) -> &str {
        &self.provider
    }

    fn discover(&self, observed_at_millis: u64) -> ClusterResult<DiscoverySnapshot> {
        Ok(DiscoverySnapshot::new(
            self.provider.clone(),
            observed_at_millis,
            self.nodes.clone(),
        ))
    }
}

/// In-memory discovery registry useful for local tests and single-process simulations.
#[derive(Debug, Clone)]
pub struct LocalDiscovery {
    provider: String,
    nodes: Arc<RwLock<BTreeMap<NodeId, ClusterNode>>>,
}

impl LocalDiscovery {
    /// Creates an empty local discovery registry named `local`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            provider: "local".to_string(),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Renames this discovery provider for diagnostics.
    #[must_use]
    pub fn with_provider_name(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }

    /// Registers or replaces a node in the local discovery registry.
    pub fn register(&self, node: ClusterNode) -> ClusterResult<()> {
        let mut nodes = self.write_nodes()?;
        nodes.insert(node.id().clone(), node);
        Ok(())
    }

    /// Removes a node from the local discovery registry.
    pub fn unregister(&self, node_id: &NodeId) -> ClusterResult<Option<ClusterNode>> {
        let mut nodes = self.write_nodes()?;
        Ok(nodes.remove(node_id))
    }

    fn write_nodes(
        &self,
    ) -> ClusterResult<std::sync::RwLockWriteGuard<'_, BTreeMap<NodeId, ClusterNode>>> {
        self.nodes.write().map_err(|error| ClusterError::Discovery {
            provider: self.provider.clone(),
            message: error.to_string(),
        })
    }

    fn read_nodes(
        &self,
    ) -> ClusterResult<std::sync::RwLockReadGuard<'_, BTreeMap<NodeId, ClusterNode>>> {
        self.nodes.read().map_err(|error| ClusterError::Discovery {
            provider: self.provider.clone(),
            message: error.to_string(),
        })
    }
}

impl Default for LocalDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

impl DiscoveryProvider for LocalDiscovery {
    fn provider_name(&self) -> &str {
        &self.provider
    }

    fn discover(&self, observed_at_millis: u64) -> ClusterResult<DiscoverySnapshot> {
        let nodes = self.read_nodes()?.values().cloned().collect::<Vec<_>>();
        Ok(DiscoverySnapshot::new(
            self.provider.clone(),
            observed_at_millis,
            nodes,
        ))
    }
}
