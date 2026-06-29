#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! etcd-backed external-arbiter discovery for Rakka clusters.
//!
//! This adapter promotes dynamic, autoscaling-friendly cluster membership to a
//! supported provider. Each node registers itself in etcd under a leased key
//! (`<prefix><node-id>`) and keeps the lease alive; peers are learned by ranging
//! the prefix. A scaled-in or crashed node's key disappears when its lease lapses
//! (or is revoked on graceful shutdown), so membership grows and shrinks at
//! runtime with no configured replica count.
//!
//! etcd is the strongly-consistent **membership arbiter**: every node converges on
//! the same up-set, which — with `DeterministicModuloShardAllocationStrategy` —
//! yields the same shard ownership without any internal consensus. See
//! `docs/rakka-cluster-coordination-strategy.md` and
//! `docs/rakka-v1-reliability-boundaries.md`.
//!
//! # Shape
//!
//! [`connect`] returns a cache-backed [`EtcdDiscovery`] (a synchronous
//! [`DiscoveryProvider`] that reads the latest snapshot) and an
//! [`EtcdDiscoverySession`] that performs the async I/O. Drive the session with
//! [`EtcdDiscoverySession::run`], or call [`EtcdDiscoverySession::refresh`]
//! yourself; feed the provider into the cluster runtime's discovery polling as
//! usual.
//!
//! [`EtcdDiscoverySession::leave`] revokes the lease so peers drop this node
//! immediately — the **actuator** for peer-reachability self-fencing
//! (`rakka_cluster::SelfFenceDetector` decides; this revokes).

use std::sync::{Arc, RwLock};
use std::time::Duration;

use etcd_client::{Client, GetOptions, PutOptions};
use rakka_cluster::{
    ClusterError, ClusterNode, ClusterResult, DiscoveryProvider, DiscoverySnapshot,
};
use tokio::sync::Notify;

/// Provider label used in snapshots and diagnostics.
const PROVIDER: &str = "etcd";

type MemberCache = Arc<RwLock<Vec<ClusterNode>>>;

/// Configuration for [`connect`].
#[derive(Debug, Clone)]
pub struct EtcdDiscoveryConfig {
    endpoints: Vec<String>,
    prefix: String,
    lease_ttl_seconds: i64,
    provider_name: String,
}

impl EtcdDiscoveryConfig {
    /// Default key prefix members register under.
    pub const DEFAULT_PREFIX: &'static str = "/rakka/members/";
    /// Default lease TTL (seconds) for a member registration.
    pub const DEFAULT_LEASE_TTL_SECONDS: i64 = 10;

    /// Creates a configuration from one or more etcd endpoints.
    #[must_use]
    pub fn new(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            endpoints: endpoints.into_iter().map(Into::into).collect(),
            prefix: Self::DEFAULT_PREFIX.to_string(),
            lease_ttl_seconds: Self::DEFAULT_LEASE_TTL_SECONDS,
            provider_name: PROVIDER.to_string(),
        }
    }

    /// Sets the key prefix members register under (must be cluster-unique).
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Sets the member-registration lease TTL in seconds.
    #[must_use]
    pub const fn with_lease_ttl_seconds(mut self, lease_ttl_seconds: i64) -> Self {
        self.lease_ttl_seconds = lease_ttl_seconds;
        self
    }

    /// Renames the provider for diagnostics (the snapshot's provider label).
    #[must_use]
    pub fn with_provider_name(mut self, provider_name: impl Into<String>) -> Self {
        self.provider_name = provider_name.into();
        self
    }

    /// Configured etcd endpoints.
    #[must_use]
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Key prefix members register under.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Member-registration lease TTL in seconds.
    #[must_use]
    pub const fn lease_ttl_seconds(&self) -> i64 {
        self.lease_ttl_seconds
    }

    /// Provider label used in snapshots.
    #[must_use]
    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }
}

/// Cache-backed [`DiscoveryProvider`] reading the latest etcd membership snapshot.
///
/// Cloneable and cheap to read; the backing snapshot is refreshed by the paired
/// [`EtcdDiscoverySession`]. `discover` never performs I/O.
#[derive(Clone)]
pub struct EtcdDiscovery {
    provider_name: Arc<str>,
    cache: MemberCache,
}

impl EtcdDiscovery {
    fn from_parts(provider_name: &str, cache: MemberCache) -> Self {
        Self {
            provider_name: Arc::from(provider_name),
            cache,
        }
    }

    /// Returns the most recently observed members without performing I/O.
    #[must_use]
    pub fn cached_members(&self) -> Vec<ClusterNode> {
        self.cache
            .read()
            .map(|members| members.clone())
            .unwrap_or_default()
    }
}

impl std::fmt::Debug for EtcdDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdDiscovery")
            .field("provider_name", &self.provider_name)
            .finish_non_exhaustive()
    }
}

impl DiscoveryProvider for EtcdDiscovery {
    fn provider_name(&self) -> &str {
        &self.provider_name
    }

    fn discover(&self, observed_at_millis: u64) -> ClusterResult<DiscoverySnapshot> {
        let members = self
            .cache
            .read()
            .map_err(|error| discovery_error(format!("membership cache poisoned: {error}")))?
            .clone();
        Ok(DiscoverySnapshot::new(
            self.provider_name.to_string(),
            observed_at_millis,
            members,
        ))
    }
}

/// Async driver that registers this node, keeps its lease alive, and refreshes
/// the membership snapshot read by [`EtcdDiscovery`].
pub struct EtcdDiscoverySession {
    client: Client,
    lease_id: i64,
    key: String,
    value: Vec<u8>,
    prefix: String,
    lease_ttl_seconds: i64,
    cache: MemberCache,
}

impl EtcdDiscoverySession {
    /// Renews the lease and refreshes the cached membership snapshot.
    pub async fn refresh(&mut self) -> ClusterResult<()> {
        self.renew_lease().await?;
        let members = range_members(&mut self.client, &self.prefix).await?;
        *self
            .cache
            .write()
            .map_err(|error| discovery_error(format!("membership cache poisoned: {error}")))? =
            members;
        Ok(())
    }

    /// Revokes this node's lease so peers drop it immediately.
    ///
    /// This is the actuator for peer-reachability self-fencing and for graceful
    /// shutdown. After leaving, the session should be dropped.
    pub async fn leave(&mut self) -> ClusterResult<()> {
        self.client
            .lease_revoke(self.lease_id)
            .await
            .map_err(|error| discovery_error(format!("lease revoke failed: {error}")))?;
        Ok(())
    }

    /// Runs the discovery loop until `shutdown` is signalled, refreshing every
    /// `poll_interval`, then revokes the lease.
    ///
    /// Transient refresh errors are tolerated (the snapshot stays last-known until
    /// the next tick); call [`refresh`](Self::refresh) directly if you need error
    /// visibility.
    pub async fn run(mut self, poll_interval: Duration, shutdown: Arc<Notify>) {
        let mut interval = tokio::time::interval(poll_interval);
        loop {
            tokio::select! {
                () = shutdown.notified() => {
                    let _ = self.leave().await;
                    break;
                }
                _ = interval.tick() => {
                    let _ = self.refresh().await;
                }
            }
        }
    }

    async fn renew_lease(&mut self) -> ClusterResult<()> {
        // Try a keepalive first; if the lease has lapsed, grant a new one and
        // re-register so this node stays in membership.
        if let Ok((mut keeper, _stream)) = self.client.lease_keep_alive(self.lease_id).await {
            if keeper.keep_alive().await.is_ok() {
                return Ok(());
            }
        }
        let grant = self
            .client
            .lease_grant(self.lease_ttl_seconds, None)
            .await
            .map_err(|error| discovery_error(format!("lease grant failed: {error}")))?;
        self.lease_id = grant.id();
        self.client
            .put(
                self.key.clone(),
                self.value.clone(),
                Some(PutOptions::new().with_lease(self.lease_id)),
            )
            .await
            .map_err(|error| discovery_error(format!("member re-register failed: {error}")))?;
        Ok(())
    }
}

impl std::fmt::Debug for EtcdDiscoverySession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdDiscoverySession")
            .field("key", &self.key)
            .field("lease_id", &self.lease_id)
            .finish_non_exhaustive()
    }
}

/// Connects to etcd, registers `local_node` under a fresh lease, reads the first
/// membership snapshot, and returns the provider plus its async session.
pub async fn connect(
    config: EtcdDiscoveryConfig,
    local_node: &ClusterNode,
) -> ClusterResult<(EtcdDiscovery, EtcdDiscoverySession)> {
    let mut client = Client::connect(&config.endpoints, None)
        .await
        .map_err(|error| discovery_error(format!("connect failed: {error}")))?;
    let grant = client
        .lease_grant(config.lease_ttl_seconds, None)
        .await
        .map_err(|error| discovery_error(format!("lease grant failed: {error}")))?;
    let lease_id = grant.id();
    let key = member_key(&config.prefix, local_node);
    let value = serde_json::to_vec(local_node)
        .map_err(|error| discovery_error(format!("encode member failed: {error}")))?;
    client
        .put(
            key.clone(),
            value.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        )
        .await
        .map_err(|error| discovery_error(format!("member register failed: {error}")))?;

    let members = range_members(&mut client, &config.prefix).await?;
    let cache: MemberCache = Arc::new(RwLock::new(members));
    let discovery = EtcdDiscovery::from_parts(&config.provider_name, cache.clone());
    let session = EtcdDiscoverySession {
        client,
        lease_id,
        key,
        value,
        prefix: config.prefix,
        lease_ttl_seconds: config.lease_ttl_seconds,
        cache,
    };
    Ok((discovery, session))
}

async fn range_members(client: &mut Client, prefix: &str) -> ClusterResult<Vec<ClusterNode>> {
    let response = client
        .get(
            prefix.as_bytes().to_vec(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(|error| discovery_error(format!("range members failed: {error}")))?;
    Ok(response
        .kvs()
        .iter()
        .filter_map(|kv| decode_member(kv.value()))
        .collect())
}

fn decode_member(value: &[u8]) -> Option<ClusterNode> {
    serde_json::from_slice(value).ok()
}

fn member_key(prefix: &str, local_node: &ClusterNode) -> String {
    format!("{prefix}{}", local_node.id())
}

fn discovery_error(message: String) -> ClusterError {
    ClusterError::Discovery {
        provider: PROVIDER.to_string(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_cluster::{ClusterNode, NodeAddress, NodeId};

    fn node(logical: &str) -> ClusterNode {
        ClusterNode::new(
            NodeId::new(logical, format!("uid-{logical}")),
            NodeAddress::new(format!("{logical}.rakka.svc"), 2552),
        )
    }

    #[test]
    fn provider_reads_cached_snapshot() {
        let cache: MemberCache = Arc::new(RwLock::new(vec![node("a"), node("b")]));
        let discovery = EtcdDiscovery::from_parts("etcd", cache.clone());

        assert_eq!(discovery.provider_name(), "etcd");
        let snapshot = discovery.discover(42).unwrap();
        assert_eq!(snapshot.provider(), "etcd");
        assert_eq!(snapshot.observed_at_millis(), 42);
        assert_eq!(snapshot.nodes().len(), 2);

        // The provider tracks later refreshes through the shared cache.
        *cache.write().unwrap() = vec![node("a")];
        assert_eq!(discovery.discover(43).unwrap().nodes().len(), 1);
    }

    #[test]
    fn member_value_round_trips_through_etcd_encoding() {
        let original = node("rakka-0");
        let encoded = serde_json::to_vec(&original).unwrap();
        let decoded = decode_member(&encoded).expect("valid member value decodes");
        assert_eq!(decoded.id(), original.id());
        // Non-member bytes are ignored rather than failing the whole range.
        assert!(decode_member(b"not json").is_none());
    }

    #[test]
    fn member_key_is_prefixed_node_id() {
        let key = member_key("/rakka/members/", &node("rakka-7"));
        assert!(key.starts_with("/rakka/members/"));
        assert!(key.contains("rakka-7"));
    }
}
